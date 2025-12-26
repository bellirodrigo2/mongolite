/*
 * mongolite_txn.c - Transaction management
 *
 * Handles:
 * - Transaction helpers (_get_write_txn, _get_read_txn, etc.)
 * - Public transaction API (begin, commit, rollback)
 * - Sync operations
 * - Doc count updates (transactional)
 */

#include "mongolite_internal.h"
#include "macros.h"
#include <stdlib.h>
#include <string.h>

#define MONGOLITE_LIB "mongolite"

/* ============================================================
 * Transaction Helpers (wtree3)
 * ============================================================ */

MONGOLITE_HOT
wtree3_txn_t* _mongolite_get_write_txn(mongolite_db_t *db, gerror_t *error) {
    if (MONGOLITE_UNLIKELY(!db)) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Database is NULL");
        return NULL;
    }
    if (MONGOLITE_UNLIKELY(db->in_transaction && db->current_txn)) {
        return db->current_txn;
    }

    /*
     * IMPORTANT: Invalidate the pooled read transaction before writing.
     * A reset (but not aborted) read txn holds a slot in LMDB's reader table,
     * which can cause issues with write transactions.
     */
    if (db->read_txn_pool) {
        wtree3_txn_abort(db->read_txn_pool);
        db->read_txn_pool = NULL;
    }

    return wtree3_txn_begin(db->wdb, true, error);
}

/*
 * Get a read transaction, using pooling for better performance.
 *
 * Optimization: Instead of creating a new transaction each time,
 * we reuse a cached transaction via wtree3_txn_renew() which only
 * acquires a new LMDB snapshot (much faster than full txn_begin).
 */
MONGOLITE_HOT
wtree3_txn_t* _mongolite_get_read_txn(mongolite_db_t *db, gerror_t *error) {
    if (MONGOLITE_UNLIKELY(!db)) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Database is NULL");
        return NULL;
    }

    /* If in explicit transaction, use that */
    if (MONGOLITE_UNLIKELY(db->in_transaction && db->current_txn)) {
        return db->current_txn;
    }

    /* Try to reuse pooled read transaction */
    if (MONGOLITE_LIKELY(db->read_txn_pool != NULL)) {
        int rc = wtree3_txn_renew(db->read_txn_pool, error);
        if (MONGOLITE_LIKELY(rc == 0)) {
            return db->read_txn_pool;
        }
        /* Renew failed - abort and create new */
        wtree3_txn_abort(db->read_txn_pool);
        db->read_txn_pool = NULL;
    }

    /* Create new read transaction and cache it */
    wtree3_txn_t *txn = wtree3_txn_begin(db->wdb, false, error);
    if (MONGOLITE_LIKELY(txn != NULL)) {
        db->read_txn_pool = txn;
    }
    return txn;
}

/*
 * Release a read transaction back to the pool.
 * Uses reset instead of abort to keep the handle for reuse.
 */
MONGOLITE_HOT
void _mongolite_release_read_txn(mongolite_db_t *db, wtree3_txn_t *txn) {
    if (MONGOLITE_UNLIKELY(!db || !txn)) return;

    /* Don't touch explicit transactions */
    if (MONGOLITE_UNLIKELY(db->in_transaction)) return;

    /* If this is our pooled transaction, just reset it */
    if (MONGOLITE_LIKELY(txn == db->read_txn_pool)) {
        wtree3_txn_reset(txn);
    } else {
        /* Not our pooled txn (shouldn't happen normally) - just abort */
        wtree3_txn_abort(txn);
    }
}

int _mongolite_commit_if_auto(mongolite_db_t *db, wtree3_txn_t *txn, gerror_t *error) {
    if (MONGOLITE_UNLIKELY(!db || !txn)) return MONGOLITE_EINVAL;
    /* Only commit if not in explicit transaction */
    if (MONGOLITE_LIKELY(!db->in_transaction)) {
        return wtree3_txn_commit(txn, error);
    }
    return MONGOLITE_OK;
}

void _mongolite_abort_if_auto(mongolite_db_t *db, wtree3_txn_t *txn) {
    if (MONGOLITE_UNLIKELY(!db || !txn)) return;
    /* Only abort if not in explicit transaction */
    if (MONGOLITE_LIKELY(!db->in_transaction)) {
        wtree3_txn_abort(txn);
        /* Clear the pool reference if we just aborted the pooled txn */
        if (txn == db->read_txn_pool) {
            db->read_txn_pool = NULL;
        }
    }
}

/* ============================================================
 * Doc Count Update (within transaction)
 *
 * NOTE: With wtree3, doc_count is maintained internally by the tree.
 * This function now just updates the schema for persistence.
 * The cached count comes from wtree3_tree_count().
 * ============================================================ */

int _mongolite_update_doc_count_txn(mongolite_db_t *db, wtree3_txn_t *txn,
                                     const char *collection, int64_t delta,
                                     gerror_t *error) {
    if (!db || !txn || !collection) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return MONGOLITE_EINVAL;
    }

    /* Read schema entry using provided transaction */
    const void *value;
    size_t value_size;
    int rc = wtree3_get_txn(txn, db->schema_tree, collection, strlen(collection),
                           &value, &value_size, error);
    if (rc != 0) {
        return rc;
    }

    /* Parse the schema entry */
    bson_t doc;
    if (!bson_init_static(&doc, value, value_size)) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_ERROR, "Invalid BSON in schema");
        return MONGOLITE_ERROR;
    }

    mongolite_schema_entry_t entry = {0};
    rc = _mongolite_schema_entry_from_bson(&doc, &entry, error);
    if (rc != 0) {
        return rc;
    }

    /* Get the current count from wtree3 (source of truth) */
    wtree3_tree_t *tree = _mongolite_tree_cache_get(db, collection);
    if (tree) {
        entry.doc_count = wtree3_tree_count(tree);
    } else {
        /* Fallback if tree not cached */
        entry.doc_count += delta;
        if (entry.doc_count < 0) entry.doc_count = 0;
    }
    entry.modified_at = _mongolite_now_ms();

    /* Serialize and write back using provided transaction */
    bson_t *new_doc = _mongolite_schema_entry_to_bson(&entry);
    if (!new_doc) {
        _mongolite_schema_entry_free(&entry);
        set_error(error, "system", MONGOLITE_ENOMEM, "Failed to serialize schema entry");
        return MONGOLITE_ENOMEM;
    }

    rc = wtree3_update_txn(txn, db->schema_tree,
                          entry.name, strlen(entry.name),
                          bson_get_data(new_doc), new_doc->len, error);

    bson_destroy(new_doc);
    _mongolite_schema_entry_free(&entry);

    return rc;
}

/* ============================================================
 * Transaction Support (Public API)
 * ============================================================ */

int mongolite_begin_transaction(mongolite_db_t *db) {
    if (!db) return MONGOLITE_EINVAL;

    _mongolite_lock(db);

    if (db->in_transaction) {
        _mongolite_unlock(db);
        /* Note: No gerror parameter in this function, so can't set error */
        return MONGOLITE_ERROR;  /* Already in transaction */
    }

    gerror_t local_error = {0};
    db->current_txn = wtree3_txn_begin(db->wdb, true, &local_error);
    if (!db->current_txn) {
        _mongolite_unlock(db);
        /* Note: error was set in local_error but we can't return it (no gerror param) */
        return MONGOLITE_ERROR;
    }

    db->in_transaction = true;
    _mongolite_unlock(db);
    return MONGOLITE_OK;
}

int mongolite_commit(mongolite_db_t *db) {
    if (!db) return MONGOLITE_EINVAL;

    _mongolite_lock(db);

    if (!db->in_transaction || !db->current_txn) {
        _mongolite_unlock(db);
        /* Note: No gerror parameter in this function, so can't set error */
        return MONGOLITE_ERROR;  /* Not in transaction */
    }

    gerror_t local_error = {0};
    int rc = wtree3_txn_commit(db->current_txn, &local_error);

    db->current_txn = NULL;
    db->in_transaction = false;

    _mongolite_unlock(db);
    return rc;
}

int mongolite_rollback(mongolite_db_t *db) {
    if (!db) return MONGOLITE_EINVAL;

    _mongolite_lock(db);

    if (!db->in_transaction || !db->current_txn) {
        _mongolite_unlock(db);
        /* Note: No gerror parameter in this function, so can't set error */
        return MONGOLITE_ERROR;  /* Not in transaction */
    }

    wtree3_txn_abort(db->current_txn);

    db->current_txn = NULL;
    db->in_transaction = false;

    _mongolite_unlock(db);
    return MONGOLITE_OK;
}

/* ============================================================
 * Sync
 * ============================================================ */

int mongolite_sync(mongolite_db_t *db, bool force, gerror_t *error) {
    if (!db || !db->wdb) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Database is NULL");
        return MONGOLITE_EINVAL;
    }
    return wtree3_db_sync(db->wdb, force, error);
}
