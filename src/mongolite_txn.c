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

/* Note: Doc count now managed automatically by wtree3_tree_count() */

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
