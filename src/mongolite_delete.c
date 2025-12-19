/*
 * mongolite_delete.c - Delete operations (Phase 7)
 *
 * Functions:
 * - mongolite_delete_one() - Delete first matching document
 * - mongolite_delete_many() - Delete all matching documents
 */

#include "mongolite_internal.h"
#include <string.h>
#include <stdlib.h>

#define MONGOLITE_LIB "mongolite"

/* ============================================================
 * Delete one document
 * ============================================================ */

int mongolite_delete_one(mongolite_db_t *db, const char *collection,
                         const bson_t *filter, gerror_t *error) {
    if (!db || !collection) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return -1;
    }

    /* OPTIMIZATION: Check for direct _id lookup */
    bson_oid_t doc_id;
    bool found_by_id = false;

    if (_mongolite_is_id_query(filter, &doc_id)) {
        /* Fast path: we already have the _id, just verify it exists */
        _mongolite_lock(db);
        wtree_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
        if (tree) {
            bson_t *existing = _mongolite_find_by_id(db, tree, &doc_id, error);
            if (existing) {
                bson_destroy(existing);
                found_by_id = true;
            }
        }
        /* Keep lock for delete operation below */
    } else {
        /* Slow path: full scan to find document */
        bson_t *existing = mongolite_find_one(db, collection, filter, NULL, error);
        if (!existing) {
            /* No match found - not an error */
            return 0;
        }

        /* Extract _id */
        bson_iter_t id_iter;
        if (!bson_iter_init_find(&id_iter, existing, "_id")) {
            bson_destroy(existing);
            set_error(error, MONGOLITE_LIB, MONGOLITE_ERROR, "Document missing _id");
            return -1;
        }
        bson_oid_copy(bson_iter_oid(&id_iter), &doc_id);
        bson_destroy(existing);

        /* Lock database for delete */
        _mongolite_lock(db);
    }

    /* If fast path didn't find document, unlock and return */
    if (_mongolite_is_id_query(filter, NULL) && !found_by_id) {
        _mongolite_unlock(db);
        return 0;  /* No match found - not an error */
    }

    /* Get collection tree */
    wtree_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (!tree) {
        _mongolite_unlock(db);
        return -1;
    }

    /* Begin transaction */
    wtree_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (!txn) {
        _mongolite_unlock(db);
        return -1;
    }

    /* Delete document */
    bool deleted = false;
    int rc = wtree_delete_one_txn(txn, tree,
                                   doc_id.bytes, sizeof(doc_id.bytes),
                                   &deleted, error);
    if (rc != 0) {
        _mongolite_abort_if_auto(db, txn);
        _mongolite_unlock(db);
        return -1;
    }

    /* Update doc count within the same transaction for atomicity */
    if (deleted) {
        rc = _mongolite_update_doc_count_txn(db, txn, collection, -1, error);
        if (rc != 0) {
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return -1;
        }
    }

    /* Commit */
    rc = _mongolite_commit_if_auto(db, txn, error);
    if (rc != 0) {
        _mongolite_unlock(db);
        return -1;
    }

    /* Update changes counter */
    if (deleted) {
        db->changes = 1;
    } else {
        db->changes = 0;
    }

    _mongolite_unlock(db);
    return 0;
}

/* ============================================================
 * Delete many documents
 * ============================================================ */

int mongolite_delete_many(mongolite_db_t *db, const char *collection,
                          const bson_t *filter, int64_t *deleted_count,
                          gerror_t *error) {
    if (!db || !collection) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return -1;
    }

    if (deleted_count) {
        *deleted_count = 0;
    }

    /* Lock database */
    _mongolite_lock(db);

    /* Get collection tree */
    wtree_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (!tree) {
        _mongolite_unlock(db);
        return -1;
    }

    /* Begin transaction */
    wtree_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (!txn) {
        _mongolite_unlock(db);
        return -1;
    }

    /* Create cursor using the existing transaction (avoids deadlock) */
    mongolite_cursor_t *cursor = _mongolite_cursor_create_with_txn(db, tree, collection,
                                                                    txn, filter, error);
    if (!cursor) {
        _mongolite_abort_if_auto(db, txn);
        _mongolite_unlock(db);
        return -1;
    }

    /* Collect all _ids to delete (can't delete while iterating) */
    bson_oid_t *ids = NULL;
    size_t n_ids = 0;
    size_t ids_capacity = 16;

    ids = malloc(sizeof(bson_oid_t) * ids_capacity);
    if (!ids) {
        mongolite_cursor_destroy(cursor);
        _mongolite_abort_if_auto(db, txn);
        _mongolite_unlock(db);
        set_error(error, "system", MONGOLITE_ENOMEM, "Out of memory");
        return -1;
    }

    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        /* Extract _id */
        bson_iter_t id_iter;
        if (!bson_iter_init_find(&id_iter, doc, "_id")) {
            continue;
        }

        /* Expand array if needed */
        if (n_ids >= ids_capacity) {
            ids_capacity *= 2;
            bson_oid_t *new_ids = realloc(ids, sizeof(bson_oid_t) * ids_capacity);
            if (!new_ids) {
                free(ids);
                mongolite_cursor_destroy(cursor);
                _mongolite_abort_if_auto(db, txn);
                _mongolite_unlock(db);
                set_error(error, "system", MONGOLITE_ENOMEM, "Out of memory");
                return -1;
            }
            ids = new_ids;
        }

        bson_oid_copy(bson_iter_oid(&id_iter), &ids[n_ids++]);
    }

    mongolite_cursor_destroy(cursor);

    /* Now delete all collected _ids */
    int64_t count = 0;
    for (size_t i = 0; i < n_ids; i++) {
        bool deleted = false;
        int rc = wtree_delete_one_txn(txn, tree,
                                       ids[i].bytes, sizeof(ids[i].bytes),
                                       &deleted, error);
        if (rc != 0) {
            free(ids);
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return -1;
        }

        if (deleted) {
            count++;
        }
    }

    free(ids);

    /* Update doc count within the same transaction for atomicity */
    int rc = 0;
    if (count > 0) {
        rc = _mongolite_update_doc_count_txn(db, txn, collection, -count, error);
        if (rc != 0) {
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return -1;
        }
    }

    /* Commit */
    rc = _mongolite_commit_if_auto(db, txn, error);
    if (rc != 0) {
        _mongolite_unlock(db);
        return -1;
    }

    if (deleted_count) {
        *deleted_count = count;
    }
    db->changes = (int)count;

    _mongolite_unlock(db);
    return 0;
}
