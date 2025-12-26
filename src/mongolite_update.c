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
#include "macros.h"
#include <string.h>
#include <stdlib.h>

#define MONGOLITE_LIB "mongolite"

/* ============================================================
 * Update one document
 * ============================================================ */

MONGOLITE_HOT
int mongolite_update_one(mongolite_db_t *db, const char *collection,
                         const bson_t *filter, const bson_t *update,
                         bool upsert, gerror_t *error) {
    VALIDATE_DB_COLLECTION_UPDATE(db, collection, update, error, -1);

    /* OPTIMIZATION: Check for direct _id lookup */
    bson_oid_t oid;
    bson_t *existing = NULL;

    if (MONGOLITE_LIKELY(_mongolite_is_id_query(filter, &oid))) {
        /* Fast path: direct _id lookup */
        _mongolite_lock(db);
        wtree3_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
        if (MONGOLITE_LIKELY(tree)) {
            existing = _mongolite_find_by_id(db, tree, &oid, error);
        }
        _mongolite_unlock(db);
    } else {
        /* Slow path: full scan */
        existing = mongolite_find_one(db, collection, filter, NULL, error);
    }

    if (MONGOLITE_UNLIKELY(!existing)) {
        /* No match found */
        if (upsert) {
            /* Build base document from filter equality conditions */
            bson_t *base = bson_upsert_build_base(filter);
            if (MONGOLITE_UNLIKELY(!base)) {
                set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM, "Failed to build upsert base");
                return -1;
            }

            /* Apply update operators to base document */
            bson_t *new_doc = bson_update_apply(base, update, error);
            bson_destroy(base);
            if (MONGOLITE_UNLIKELY(!new_doc)) {
                return -1;
            }

            /* Generate _id if not present */
            bson_oid_t new_oid;
            bson_iter_t id_iter;
            if (!bson_iter_init_find(&id_iter, new_doc, "_id")) {
                bson_oid_init(&new_oid, NULL);
                bson_t *with_id = bson_new();
                if (MONGOLITE_UNLIKELY(!with_id)) {
                    bson_destroy(new_doc);
                    set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM, "Failed to allocate document");
                    return -1;
                }
                BSON_APPEND_OID(with_id, "_id", &new_oid);
                /* Copy all fields from new_doc */
                bson_iter_t iter;
                if (bson_iter_init(&iter, new_doc)) {
                    while (bson_iter_next(&iter)) {
                        bson_append_iter(with_id, bson_iter_key(&iter), -1, &iter);
                    }
                }
                bson_destroy(new_doc);
                new_doc = with_id;
            }

            /* Insert using mongolite_insert_one */
            bson_oid_t inserted_id;
            int rc = mongolite_insert_one(db, collection, new_doc, &inserted_id, error);
            bson_destroy(new_doc);
            return rc;
        }
        return 0;  /* No error, just no match */
    }

    /* Extract _id */
    bson_oid_t doc_id;
    if (MONGOLITE_UNLIKELY(!extract_doc_oid_with_error(existing, &doc_id, error))) {
        bson_destroy(existing);
        return -1;
    }

    /* Apply update operators */
    bson_t *updated = bson_update_apply(existing, update, error);
    if (MONGOLITE_UNLIKELY(!updated)) {
        bson_destroy(existing);
        return -1;
    }

    /* Lock database */
    _mongolite_lock(db);

    /* Get collection tree (wtree3 - indexes maintained automatically) */
    wtree3_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (MONGOLITE_UNLIKELY(!tree)) {
        bson_destroy(existing);
        bson_destroy(updated);
        _mongolite_unlock(db);
        return -1;
    }

    /* Begin transaction */
    wtree3_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (MONGOLITE_UNLIKELY(!txn)) {
        bson_destroy(existing);
        bson_destroy(updated);
        _mongolite_unlock(db);
        return -1;
    }

    bson_destroy(existing);

    /* Update document via wtree3 (indexes maintained automatically) */
    int rc = wtree3_update_txn(txn, tree,
                                doc_id.bytes, sizeof(doc_id.bytes),
                                bson_get_data(updated), updated->len,
                                error);
    if (MONGOLITE_UNLIKELY(rc != 0)) {
        _mongolite_abort_if_auto(db, txn);
        bson_destroy(updated);
        _mongolite_unlock(db);
        return _mongolite_translate_wtree3_error(rc);
    }

    rc = _mongolite_commit_if_auto(db, txn, error);
    if (MONGOLITE_UNLIKELY(rc != 0)) {
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

/* Helper struct to store update info */
typedef struct {
    bson_oid_t id;
    bson_t *old_doc;
    bson_t *new_doc;
} update_info_t;

MONGOLITE_HOT
int mongolite_update_many(mongolite_db_t *db, const char *collection,
                         const bson_t *filter, const bson_t *update,
                         bool upsert, int64_t *modified_count, gerror_t *error) {
    VALIDATE_DB_COLLECTION_UPDATE(db, collection, update, error, -1);

    if (modified_count) {
        *modified_count = 0;
    }

    /* Lock database */
    _mongolite_lock(db);

    /* Get collection tree (wtree3) */
    wtree3_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (MONGOLITE_UNLIKELY(!tree)) {
        _mongolite_unlock(db);
        return -1;
    }

    /* Begin transaction */
    wtree3_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (MONGOLITE_UNLIKELY(!txn)) {
        _mongolite_unlock(db);
        return -1;
    }

    /* Create cursor using existing transaction (avoids deadlock) */
    mongolite_cursor_t *cursor = _mongolite_cursor_create_with_txn(db, tree, collection,
                                                                    txn, filter, error);
    if (MONGOLITE_UNLIKELY(!cursor)) {
        _mongolite_abort_if_auto(db, txn);
        _mongolite_unlock(db);
        return -1;
    }

    /* Collect all documents and their updates first (can't modify while iterating) */
    update_info_t *updates = NULL;
    size_t n_updates = 0;
    size_t updates_capacity = 16;

    updates = malloc(sizeof(update_info_t) * updates_capacity);
    if (MONGOLITE_UNLIKELY(!updates)) {
        mongolite_cursor_destroy(cursor);
        _mongolite_abort_if_auto(db, txn);
        _mongolite_unlock(db);
        set_error(error, "system", MONGOLITE_ENOMEM, "Out of memory");
        return -1;
    }

    const bson_t *doc;
    bson_oid_t oid;
    int rc = 0;

    while (MONGOLITE_LIKELY(mongolite_cursor_next(cursor, &doc))) {
        /* Extract _id - skip documents without valid OID */
        EXTRACT_OID_OR_CONTINUE(doc, oid);

        /* Expand array if needed */
        if (MONGOLITE_UNLIKELY(n_updates >= updates_capacity)) {
            updates_capacity *= 2;
            update_info_t *new_updates = realloc(updates, sizeof(update_info_t) * updates_capacity);
            if (MONGOLITE_UNLIKELY(!new_updates)) {
                for (size_t j = 0; j < n_updates; j++) {
                    bson_destroy(updates[j].old_doc);
                    bson_destroy(updates[j].new_doc);
                }
                free(updates);
                mongolite_cursor_destroy(cursor);
                _mongolite_abort_if_auto(db, txn);
                _mongolite_unlock(db);
                set_error(error, "system", MONGOLITE_ENOMEM, "Out of memory");
                return -1;
            }
            updates = new_updates;
        }

        bson_oid_copy(&oid, &updates[n_updates].id);
        updates[n_updates].old_doc = bson_copy(doc);

        /* Apply update operators */
        updates[n_updates].new_doc = bson_update_apply(doc, update, error);

        if (MONGOLITE_UNLIKELY(!updates[n_updates].old_doc || !updates[n_updates].new_doc)) {
            if (updates[n_updates].old_doc) bson_destroy(updates[n_updates].old_doc);
            if (updates[n_updates].new_doc) bson_destroy(updates[n_updates].new_doc);
            for (size_t j = 0; j < n_updates; j++) {
                bson_destroy(updates[j].old_doc);
                bson_destroy(updates[j].new_doc);
            }
            free(updates);
            mongolite_cursor_destroy(cursor);
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return -1;
        }

        n_updates++;
    }

    mongolite_cursor_destroy(cursor);

    /* Now apply all updates */
    int64_t count = 0;

    for (size_t i = 0; i < n_updates; i++) {
        /* Update document via wtree3 (indexes maintained automatically) */
        rc = wtree3_update_txn(txn, tree,
                              updates[i].id.bytes, sizeof(updates[i].id.bytes),
                              bson_get_data(updates[i].new_doc), updates[i].new_doc->len,
                              error);
        if (MONGOLITE_UNLIKELY(rc != 0)) {
            int translated_rc = _mongolite_translate_wtree3_error(rc);
            for (size_t j = 0; j < n_updates; j++) {
                bson_destroy(updates[j].old_doc);
                bson_destroy(updates[j].new_doc);
            }
            free(updates);
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return translated_rc;
        }

        count++;
    }

    /* Free update info */
    for (size_t i = 0; i < n_updates; i++) {
        bson_destroy(updates[i].old_doc);
        bson_destroy(updates[i].new_doc);
    }
    free(updates);

    /* Handle upsert if no matches */
    if (count == 0 && upsert) {
        /* Build base document from filter equality conditions */
        bson_t *base = bson_upsert_build_base(filter);
        if (MONGOLITE_UNLIKELY(!base)) {
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM, "Failed to build upsert base");
            return -1;
        }

        /* Apply update operators to base document */
        bson_t *new_doc = bson_update_apply(base, update, error);
        bson_destroy(base);
        if (MONGOLITE_UNLIKELY(!new_doc)) {
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return -1;
        }

        /* Generate _id if not present */
        bson_oid_t new_oid;
        bson_iter_t id_iter;
        if (!bson_iter_init_find(&id_iter, new_doc, "_id")) {
            bson_oid_init(&new_oid, NULL);
            bson_t *with_id = bson_new();
            if (MONGOLITE_UNLIKELY(!with_id)) {
                bson_destroy(new_doc);
                _mongolite_abort_if_auto(db, txn);
                _mongolite_unlock(db);
                set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM, "Failed to allocate document");
                return -1;
            }
            BSON_APPEND_OID(with_id, "_id", &new_oid);
            bson_iter_t iter;
            if (bson_iter_init(&iter, new_doc)) {
                while (bson_iter_next(&iter)) {
                    bson_append_iter(with_id, bson_iter_key(&iter), -1, &iter);
                }
            }
            bson_destroy(new_doc);
            new_doc = with_id;
        } else if (BSON_ITER_HOLDS_OID(&id_iter)) {
            bson_oid_copy(bson_iter_oid(&id_iter), &new_oid);
        } else {
            bson_oid_init(&new_oid, NULL);
        }

        /* Insert into collection via wtree3 (indexes maintained automatically) */
        rc = wtree3_insert_one_txn(txn, tree,
                                    new_oid.bytes, sizeof(new_oid.bytes),
                                    bson_get_data(new_doc), new_doc->len,
                                    error);
        if (MONGOLITE_UNLIKELY(rc != 0)) {
            bson_destroy(new_doc);
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return rc;
        }

        /* Update doc count */
        rc = _mongolite_update_doc_count_txn(db, txn, collection, 1, error);
        if (MONGOLITE_UNLIKELY(rc != 0)) {
            bson_destroy(new_doc);
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return rc;
        }

        bson_destroy(new_doc);
        count = 1;  /* We inserted one document */
    }

    rc = _mongolite_commit_if_auto(db, txn, error);
    if (MONGOLITE_UNLIKELY(rc != 0)) {
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
    VALIDATE_PARAMS(db && collection && replacement, error, "Database, collection, and replacement are required", -1);

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

    if (MONGOLITE_UNLIKELY(!existing)) {
        /* No match found */
        if (upsert) {
            /* For replace, the new document is the replacement itself */
            /* Build base from filter, then merge with replacement */
            bson_t *base = bson_upsert_build_base(filter);
            if (MONGOLITE_UNLIKELY(!base)) {
                set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM, "Failed to build upsert base");
                return -1;
            }

            /* Merge: replacement fields override base fields */
            bson_t *new_doc = bson_new();
            if (MONGOLITE_UNLIKELY(!new_doc)) {
                bson_destroy(base);
                set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM, "Failed to allocate document");
                return -1;
            }

            /* Generate _id if neither filter nor replacement has it */
            bson_oid_t new_oid;
            bson_iter_t id_iter;
            bool has_id = false;

            /* Check if replacement has _id */
            if (bson_iter_init_find(&id_iter, replacement, "_id")) {
                BSON_APPEND_VALUE(new_doc, "_id", bson_iter_value(&id_iter));
                has_id = true;
            } else if (bson_iter_init_find(&id_iter, base, "_id")) {
                /* Check if filter had _id */
                BSON_APPEND_VALUE(new_doc, "_id", bson_iter_value(&id_iter));
                has_id = true;
            }

            if (!has_id) {
                bson_oid_init(&new_oid, NULL);
                BSON_APPEND_OID(new_doc, "_id", &new_oid);
            }

            /* Copy base fields (except _id) */
            if (bson_iter_init(&id_iter, base)) {
                while (bson_iter_next(&id_iter)) {
                    const char *key = bson_iter_key(&id_iter);
                    if (strcmp(key, "_id") != 0) {
                        /* Only add if not in replacement */
                        bson_iter_t repl_iter;
                        if (!bson_iter_init_find(&repl_iter, replacement, key)) {
                            bson_append_iter(new_doc, key, -1, &id_iter);
                        }
                    }
                }
            }

            /* Copy all replacement fields (except _id) */
            if (bson_iter_init(&id_iter, replacement)) {
                while (bson_iter_next(&id_iter)) {
                    const char *key = bson_iter_key(&id_iter);
                    if (strcmp(key, "_id") != 0) {
                        bson_append_iter(new_doc, key, -1, &id_iter);
                    }
                }
            }

            bson_destroy(base);

            /* Insert using mongolite_insert_one */
            bson_oid_t inserted_id;
            int rc = mongolite_insert_one(db, collection, new_doc, &inserted_id, error);
            bson_destroy(new_doc);
            return rc;
        }
        return 0;
    }

    /* Extract _id from existing document */
    bson_oid_t doc_id;
    if (MONGOLITE_UNLIKELY(!extract_doc_oid_with_error(existing, &doc_id, error))) {
        bson_destroy(existing);
        return -1;
    }

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

    /* Get collection tree (wtree3 - indexes maintained automatically) */
    wtree3_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (MONGOLITE_UNLIKELY(!tree)) {
        bson_destroy(existing);
        bson_destroy(new_doc);
        _mongolite_unlock(db);
        return -1;
    }

    /* Begin transaction */
    wtree3_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (MONGOLITE_UNLIKELY(!txn)) {
        bson_destroy(existing);
        bson_destroy(new_doc);
        _mongolite_unlock(db);
        return -1;
    }

    bson_destroy(existing);

    /* Replace document via wtree3 (indexes maintained automatically) */
    int rc = wtree3_update_txn(txn, tree,
                          doc_id.bytes, sizeof(doc_id.bytes),
                          bson_get_data(new_doc), new_doc->len,
                          error);
    if (MONGOLITE_UNLIKELY(rc != 0)) {
        _mongolite_abort_if_auto(db, txn);
        bson_destroy(new_doc);
        _mongolite_unlock(db);
        return _mongolite_translate_wtree3_error(rc);
    }

    rc = _mongolite_commit_if_auto(db, txn, error);
    bson_destroy(new_doc);

    if (MONGOLITE_UNLIKELY(rc != 0)) {
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
    bson_t *filter = parse_optional_json_to_bson(filter_json, error);
    if (filter_json && !filter) {
        return -1;  /* Error already set by parse function */
    }

    bson_t *update = parse_json_to_bson(update_json, error);
    if (!update) {
        if (filter) bson_destroy(filter);
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
    bson_t *filter = parse_optional_json_to_bson(filter_json, error);
    if (filter_json && !filter) {
        return -1;
    }

    bson_t *update = parse_json_to_bson(update_json, error);
    if (!update) {
        if (filter) bson_destroy(filter);
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
    bson_t *filter = parse_optional_json_to_bson(filter_json, error);
    if (filter_json && !filter) {
        return -1;
    }

    bson_t *replacement = parse_json_to_bson(replacement_json, error);
    if (!replacement) {
        if (filter) bson_destroy(filter);
        return -1;
    }

    int rc = mongolite_replace_one(db, collection, filter, replacement, upsert, error);

    if (filter) bson_destroy(filter);
    bson_destroy(replacement);
    return rc;
}
