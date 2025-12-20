/*
 * mongolite_update.c - Update and replace operations (Phase 6)
 *
 * Functions:
 * - mongolite_update_one() - Update first matching document
 * - mongolite_update_many() - Update all matching documents
 * - mongolite_replace_one() - Replace entire document
 * - JSON wrappers for all operations
 *
 * Update operators are implemented in bson_update.c
 */

#include "mongolite_internal.h"
#include "bson_update.h"
#include <string.h>
#include <stdlib.h>

#define MONGOLITE_LIB "mongolite"

/* ============================================================
 * Update one document
 * ============================================================ */

int mongolite_update_one(mongolite_db_t *db, const char *collection,
                         const bson_t *filter, const bson_t *update,
                         bool upsert, gerror_t *error) {
    if (!db || !collection || !update) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return -1;
    }

    /* OPTIMIZATION: Check for direct _id lookup */
    bson_oid_t oid;
    bson_t *existing = NULL;

    if (_mongolite_is_id_query(filter, &oid)) {
        /* Fast path: direct _id lookup */
        _mongolite_lock(db);
        wtree_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
        if (tree) {
            existing = _mongolite_find_by_id(db, tree, &oid, error);
        }
        _mongolite_unlock(db);
    } else {
        /* Slow path: full scan */
        existing = mongolite_find_one(db, collection, filter, NULL, error);
    }

    if (!existing) {
        /* No match found */
        if (upsert) {
            /* TODO: Implement upsert - create new document */
            set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Upsert not yet implemented");
            return -1;
        }
        return 0;  /* No error, just no match */
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

    /* Apply update operators */
    bson_t *updated = bson_update_apply(existing, update, error);
    bson_destroy(existing);

    if (!updated) {
        return -1;
    }

    /* Lock database */
    _mongolite_lock(db);

    /* Get collection tree */
    wtree_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (!tree) {
        bson_destroy(updated);
        _mongolite_unlock(db);
        return -1;
    }

    /* Begin transaction */
    wtree_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (!txn) {
        bson_destroy(updated);
        _mongolite_unlock(db);
        return -1;
    }

    /* Update document (overwrites existing) */
    int rc = wtree_update_txn(txn, tree,
                              doc_id.bytes, sizeof(doc_id.bytes),
                              bson_get_data(updated), updated->len,
                              error);
    if (rc != 0) {
        _mongolite_abort_if_auto(db, txn);
        bson_destroy(updated);
        _mongolite_unlock(db);
        return -1;
    }

    rc = _mongolite_commit_if_auto(db, txn, error);
    if (rc != 0) {
        bson_destroy(updated);
        _mongolite_unlock(db);
        return -1;
    }

    bson_destroy(updated);
    db->changes = 1;
    _mongolite_unlock(db);
    return 0;
}

/* ============================================================
 * Update many documents
 * ============================================================ */

int mongolite_update_many(mongolite_db_t *db, const char *collection,
                         const bson_t *filter, const bson_t *update,
                         bool upsert, int64_t *modified_count, gerror_t *error) {
    if (!db || !collection || !update) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return -1;
    }

    if (modified_count) {
        *modified_count = 0;
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

    /* Create cursor using existing transaction (avoids deadlock) */
    mongolite_cursor_t *cursor = _mongolite_cursor_create_with_txn(db, tree, collection,
                                                                    txn, filter, error);
    if (!cursor) {
        _mongolite_abort_if_auto(db, txn);
        _mongolite_unlock(db);
        return -1;
    }

    int64_t count = 0;
    const bson_t *doc;

    while (mongolite_cursor_next(cursor, &doc)) {
        /* Extract _id */
        bson_iter_t id_iter;
        if (!bson_iter_init_find(&id_iter, doc, "_id")) {
            continue;
        }
        bson_oid_t doc_id;
        bson_oid_copy(bson_iter_oid(&id_iter), &doc_id);

        /* Apply update operators */
        bson_t *updated = bson_update_apply(doc, update, error);
        if (!updated) {
            mongolite_cursor_destroy(cursor);
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return -1;
        }

        /* Update document */
        int rc = wtree_update_txn(txn, tree,
                                  doc_id.bytes, sizeof(doc_id.bytes),
                                  bson_get_data(updated), updated->len,
                                  error);
        bson_destroy(updated);

        if (rc != 0) {
            mongolite_cursor_destroy(cursor);
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return -1;
        }

        count++;
    }

    mongolite_cursor_destroy(cursor);

    /* Handle upsert if no matches */
    if (count == 0 && upsert) {
        /* TODO: Implement upsert */
        _mongolite_abort_if_auto(db, txn);
        _mongolite_unlock(db);
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Upsert not yet implemented");
        return -1;
    }

    int rc = _mongolite_commit_if_auto(db, txn, error);
    if (rc != 0) {
        _mongolite_unlock(db);
        return -1;
    }

    if (modified_count) {
        *modified_count = count;
    }
    db->changes = (int)count;
    _mongolite_unlock(db);
    return 0;
}

/* ============================================================
 * Replace one document
 * ============================================================ */

int mongolite_replace_one(mongolite_db_t *db, const char *collection,
                         const bson_t *filter, const bson_t *replacement,
                         bool upsert, gerror_t *error) {
    if (!db || !collection || !replacement) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return -1;
    }

    /* Replacement must not contain update operators */
    bson_iter_t iter;
    bson_iter_init(&iter, replacement);
    while (bson_iter_next(&iter)) {
        const char *key = bson_iter_key(&iter);
        if (key[0] == '$') {
            set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Replacement cannot contain operators");
            return -1;
        }
    }

    /* Find the first matching document */
    bson_t *existing = mongolite_find_one(db, collection, filter, NULL, error);

    if (!existing) {
        /* No match found */
        if (upsert) {
            /* TODO: Implement upsert */
            set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Upsert not yet implemented");
            return -1;
        }
        return 0;
    }

    /* Extract _id from existing document */
    bson_iter_t id_iter;
    if (!bson_iter_init_find(&id_iter, existing, "_id")) {
        bson_destroy(existing);
        set_error(error, MONGOLITE_LIB, MONGOLITE_ERROR, "Document missing _id");
        return -1;
    }
    bson_oid_t doc_id;
    bson_oid_copy(bson_iter_oid(&id_iter), &doc_id);
    bson_destroy(existing);

    /* Create new document with _id preserved */
    bson_t *new_doc = bson_new();
    BSON_APPEND_OID(new_doc, "_id", &doc_id);

    /* Copy all fields from replacement */
    bson_iter_init(&iter, replacement);
    while (bson_iter_next(&iter)) {
        const char *key = bson_iter_key(&iter);
        if (strcmp(key, "_id") != 0) {  /* Skip _id if present in replacement */
            bson_append_iter(new_doc, key, -1, &iter);
        }
    }

    /* Lock database */
    _mongolite_lock(db);

    /* Get collection tree */
    wtree_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (!tree) {
        bson_destroy(new_doc);
        _mongolite_unlock(db);
        return -1;
    }

    /* Begin transaction */
    wtree_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (!txn) {
        bson_destroy(new_doc);
        _mongolite_unlock(db);
        return -1;
    }

    /* Replace document */
    int rc = wtree_update_txn(txn, tree,
                              doc_id.bytes, sizeof(doc_id.bytes),
                              bson_get_data(new_doc), new_doc->len,
                              error);
    if (rc != 0) {
        _mongolite_abort_if_auto(db, txn);
        bson_destroy(new_doc);
        _mongolite_unlock(db);
        return -1;
    }

    rc = _mongolite_commit_if_auto(db, txn, error);
    bson_destroy(new_doc);

    if (rc != 0) {
        _mongolite_unlock(db);
        return -1;
    }

    db->changes = 1;
    _mongolite_unlock(db);
    return 0;
}

/* ============================================================
 * JSON Wrappers
 * ============================================================ */

int mongolite_update_one_json(mongolite_db_t *db, const char *collection,
                              const char *filter_json, const char *update_json,
                              bool upsert, gerror_t *error) {
    bson_error_t bson_err;

    bson_t *filter = filter_json ? bson_new_from_json((const uint8_t*)filter_json, -1, &bson_err) : NULL;
    if (filter_json && !filter) {
        set_error(error, "libbson", MONGOLITE_EINVAL, "Invalid filter JSON: %s", bson_err.message);
        return -1;
    }

    bson_t *update = bson_new_from_json((const uint8_t*)update_json, -1, &bson_err);
    if (!update) {
        if (filter) bson_destroy(filter);
        set_error(error, "libbson", MONGOLITE_EINVAL, "Invalid update JSON: %s", bson_err.message);
        return -1;
    }

    int rc = mongolite_update_one(db, collection, filter, update, upsert, error);

    if (filter) bson_destroy(filter);
    bson_destroy(update);
    return rc;
}

int mongolite_update_many_json(mongolite_db_t *db, const char *collection,
                               const char *filter_json, const char *update_json,
                               bool upsert, int64_t *modified_count, gerror_t *error) {
    bson_error_t bson_err;

    bson_t *filter = filter_json ? bson_new_from_json((const uint8_t*)filter_json, -1, &bson_err) : NULL;
    if (filter_json && !filter) {
        set_error(error, "libbson", MONGOLITE_EINVAL, "Invalid filter JSON: %s", bson_err.message);
        return -1;
    }

    bson_t *update = bson_new_from_json((const uint8_t*)update_json, -1, &bson_err);
    if (!update) {
        if (filter) bson_destroy(filter);
        set_error(error, "libbson", MONGOLITE_EINVAL, "Invalid update JSON: %s", bson_err.message);
        return -1;
    }

    int rc = mongolite_update_many(db, collection, filter, update, upsert, modified_count, error);

    if (filter) bson_destroy(filter);
    bson_destroy(update);
    return rc;
}

int mongolite_replace_one_json(mongolite_db_t *db, const char *collection,
                               const char *filter_json, const char *replacement_json,
                               bool upsert, gerror_t *error) {
    bson_error_t bson_err;

    bson_t *filter = filter_json ? bson_new_from_json((const uint8_t*)filter_json, -1, &bson_err) : NULL;
    if (filter_json && !filter) {
        set_error(error, "libbson", MONGOLITE_EINVAL, "Invalid filter JSON: %s", bson_err.message);
        return -1;
    }

    bson_t *replacement = bson_new_from_json((const uint8_t*)replacement_json, -1, &bson_err);
    if (!replacement) {
        if (filter) bson_destroy(filter);
        set_error(error, "libbson", MONGOLITE_EINVAL, "Invalid replacement JSON: %s", bson_err.message);
        return -1;
    }

    int rc = mongolite_replace_one(db, collection, filter, replacement, upsert, error);

    if (filter) bson_destroy(filter);
    bson_destroy(replacement);
    return rc;
}
