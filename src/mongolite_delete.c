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
 * Helper: Update document count in schema within an existing transaction
 *
 * This ensures doc_count is updated atomically with the delete
 * operation, preventing inconsistency on crash or failure.
 * ============================================================ */

static int _update_doc_count_txn(mongolite_db_t *db, wtree_txn_t *txn,
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
 * Delete one document
 * ============================================================ */

int mongolite_delete_one(mongolite_db_t *db, const char *collection,
                         const bson_t *filter, gerror_t *error) {
    if (!db || !collection) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return -1;
    }

    /* Find the first matching document */
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
    bson_oid_t doc_id;
    bson_oid_copy(bson_iter_oid(&id_iter), &doc_id);
    bson_destroy(existing);

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
        rc = _update_doc_count_txn(db, txn, collection, -1, error);
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

    /* Find all matching documents and collect their _ids */
    mongolite_cursor_t *cursor = mongolite_find(db, collection, filter, NULL, error);
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
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM, "Out of memory");
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
                set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM, "Out of memory");
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
        rc = _update_doc_count_txn(db, txn, collection, -count, error);
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
