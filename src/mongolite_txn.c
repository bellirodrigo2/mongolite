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
#include <stdlib.h>
#include <string.h>

#define MONGOLITE_LIB "mongolite"

/* ============================================================
 * Transaction Helpers
 * ============================================================ */

wtree_txn_t* _mongolite_get_write_txn(mongolite_db_t *db, gerror_t *error) {
    if (!db) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Database is NULL");
        return NULL;
    }
    if (db->in_transaction && db->current_txn) {
        return db->current_txn;
    }
    return wtree_txn_begin(db->wdb, true, error);
}

wtree_txn_t* _mongolite_get_read_txn(mongolite_db_t *db, gerror_t *error) {
    if (!db) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Database is NULL");
        return NULL;
    }
    if (db->in_transaction && db->current_txn) {
        return db->current_txn;
    }
    return wtree_txn_begin(db->wdb, false, error);
}

int _mongolite_commit_if_auto(mongolite_db_t *db, wtree_txn_t *txn, gerror_t *error) {
    if (!db || !txn) return MONGOLITE_EINVAL;
    /* Only commit if not in explicit transaction */
    if (!db->in_transaction) {
        return wtree_txn_commit(txn, error);
    }
    return MONGOLITE_OK;
}

void _mongolite_abort_if_auto(mongolite_db_t *db, wtree_txn_t *txn) {
    if (!db || !txn) return;
    /* Only abort if not in explicit transaction */
    if (!db->in_transaction) {
        wtree_txn_abort(txn);
    }
}

/* ============================================================
 * Doc Count Update (within transaction)
 *
 * This is the unified version - used by both insert and delete.
 * Updates doc_count atomically within an existing transaction.
 * ============================================================ */

int _mongolite_update_doc_count_txn(mongolite_db_t *db, wtree_txn_t *txn,
                                     const char *collection, int64_t delta,
                                     gerror_t *error) {
    if (!db || !txn || !collection) {
        return MONGOLITE_EINVAL;
    }

    /* Read schema entry using provided transaction */
    const void *value;
    size_t value_size;
    int rc = wtree_get_txn(txn, db->schema_tree, collection, strlen(collection),
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

    /* Update count */
    entry.doc_count += delta;
    if (entry.doc_count < 0) entry.doc_count = 0;
    entry.modified_at = _mongolite_now_ms();

    /* Serialize and write back using provided transaction */
    bson_t *new_doc = _mongolite_schema_entry_to_bson(&entry);
    if (!new_doc) {
        _mongolite_schema_entry_free(&entry);
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM, "Failed to serialize schema entry");
        return MONGOLITE_ENOMEM;
    }

    rc = wtree_update_txn(txn, db->schema_tree,
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
        return MONGOLITE_ERROR;  /* Already in transaction */
    }

    gerror_t error = {0};
    db->current_txn = wtree_txn_begin(db->wdb, true, &error);
    if (!db->current_txn) {
        _mongolite_unlock(db);
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
        return MONGOLITE_ERROR;  /* Not in transaction */
    }

    gerror_t error = {0};
    int rc = wtree_txn_commit(db->current_txn, &error);

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
        return MONGOLITE_ERROR;  /* Not in transaction */
    }

    wtree_txn_abort(db->current_txn);

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
    return wtree_db_sync(db->wdb, force, error);
}
