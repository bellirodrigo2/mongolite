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
#include "mongoc-matcher.h"
#include "macros.h"
#include <string.h>
#include <stdlib.h>

#define MONGOLITE_LIB "mongolite"

/* ============================================================
 * BSON Merge Function for wtree3 Upsert
 * ============================================================ */

/**
 * Context for BSON merge during upsert operations.
 * Contains the update operators and filter for building base document.
 */
typedef struct {
    const bson_t *update;  /* Update operators ($set, $inc, etc.) */
    const bson_t *filter;  /* Query filter for upsert base (may be NULL) */
} bson_merge_ctx_t;

/**
 * Merge function for wtree3_upsert_txn().
 * Applies MongoDB update operators to existing document or creates new document.
 *
 * @param existing_value Existing BSON document (NULL for insert)
 * @param existing_len   Length of existing document
 * @param new_value      Not used (update operators are in user_data)
 * @param new_len        Not used
 * @param user_data      Pointer to bson_merge_ctx_t
 * @param out_len        Output: length of merged document
 * @return               malloc'd BSON document, or NULL on error
 */
static void* bson_merge_for_upsert(const void *existing_value, size_t existing_len,
                                    const void *new_value, size_t new_len,
                                    void *user_data, size_t *out_len) {
    bson_merge_ctx_t *ctx = (bson_merge_ctx_t *)user_data;
    bson_t *result = NULL;

    if (existing_value) {
        /* Update existing document */
        bson_t existing;
        bson_init_static(&existing, existing_value, existing_len);

        /* Apply update operators */
        result = bson_update_apply(&existing, ctx->update, NULL);
    } else {
        /* Insert new document - build base from filter */
        bson_t *base = bson_upsert_build_base(ctx->filter);
        if (!base) return NULL;

        /* Apply update operators to base */
        result = bson_update_apply(base, ctx->update, NULL);
        bson_destroy(base);
    }

    if (!result) return NULL;

    /* Return malloc'd copy of BSON data */
    *out_len = result->len;
    void *data = malloc(*out_len);
    if (data) {
        memcpy(data, bson_get_data(result), *out_len);
    }
    bson_destroy(result);
    return data;
}

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
    bool has_id = _mongolite_is_id_query(filter, &oid);

    if (!has_id) {
        /* No _id in filter: need to find document first (slow path) */
        bson_t *existing = mongolite_find_one(db, collection, filter, NULL, error);
        if (existing) {
            /* Extract _id from found document */
            if (MONGOLITE_UNLIKELY(!extract_doc_oid_with_error(existing, &oid, error))) {
                bson_destroy(existing);
                return -1;
            }
            bson_destroy(existing);
            has_id = true;
        } else if (!upsert) {
            /* No match and not upsert - nothing to do */
            return 0;
        }
        /* If upsert and no match, has_id remains false - will insert new doc */
    }

    /* Lock database */
    _mongolite_lock(db);

    /* Get collection tree */
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

    int rc;

    if (has_id) {
        /* Fast path: direct _id update/upsert */
        if (upsert) {
            /* Use wtree3_upsert_txn with merge function */
            /* First, build the new document for insert case */
            bson_t *base = bson_upsert_build_base(filter);
            if (MONGOLITE_UNLIKELY(!base)) {
                _mongolite_abort_if_auto(db, txn);
                _mongolite_unlock(db);
                set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM, "Failed to build upsert base");
                return -1;
            }

            bson_t *new_doc = bson_update_apply(base, update, error);
            bson_destroy(base);
            if (MONGOLITE_UNLIKELY(!new_doc)) {
                _mongolite_abort_if_auto(db, txn);
                _mongolite_unlock(db);
                return -1;
            }

            /* Ensure _id is set */
            bson_iter_t id_iter;
            if (!bson_iter_init_find(&id_iter, new_doc, "_id")) {
                bson_t *with_id = bson_new();
                if (MONGOLITE_UNLIKELY(!with_id)) {
                    bson_destroy(new_doc);
                    _mongolite_abort_if_auto(db, txn);
                    _mongolite_unlock(db);
                    set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM, "Failed to allocate document");
                    return -1;
                }
                BSON_APPEND_OID(with_id, "_id", &oid);
                bson_iter_t iter;
                if (bson_iter_init(&iter, new_doc)) {
                    while (bson_iter_next(&iter)) {
                        bson_append_iter(with_id, bson_iter_key(&iter), -1, &iter);
                    }
                }
                bson_destroy(new_doc);
                new_doc = with_id;
            }

            /* Setup merge context for update case */
            bson_merge_ctx_t merge_ctx = { .update = update, .filter = filter };
            wtree3_tree_set_merge_fn(tree, (wtree3_merge_fn)bson_merge_for_upsert, &merge_ctx);

            /* Upsert: insert new_doc if key doesn't exist, or merge if it does */
            rc = wtree3_upsert_txn(txn, tree,
                                   oid.bytes, sizeof(oid.bytes),
                                   bson_get_data(new_doc), new_doc->len,
                                   error);

            wtree3_tree_set_merge_fn(tree, NULL, NULL);
            bson_destroy(new_doc);

            if (MONGOLITE_UNLIKELY(rc != 0)) {
                _mongolite_abort_if_auto(db, txn);
                _mongolite_unlock(db);
                return _mongolite_translate_wtree3_error(rc);
            }

            /* For upsert, we may have inserted - update count if needed */
            /* This is a simplification - ideally we'd track if insert happened */
            /* For now, we accept slight inaccuracy in doc count during upserts */
        } else {
            /* Simple update - check if document exists first */
            const void *existing_value;
            size_t existing_len;
            rc = wtree3_get_txn(txn, tree, oid.bytes, sizeof(oid.bytes),
                                &existing_value, &existing_len, NULL);

            if (rc != 0) {
                /* Document doesn't exist - nothing to update */
                _mongolite_abort_if_auto(db, txn);
                _mongolite_unlock(db);
                return 0;
            }

            /* Document exists - apply update */
            bson_t existing;
            bson_init_static(&existing, existing_value, existing_len);

            bson_t *updated = bson_update_apply(&existing, update, error);
            if (MONGOLITE_UNLIKELY(!updated)) {
                _mongolite_abort_if_auto(db, txn);
                _mongolite_unlock(db);
                return -1;
            }

            rc = wtree3_update_txn(txn, tree,
                                   oid.bytes, sizeof(oid.bytes),
                                   bson_get_data(updated), updated->len,
                                   error);
            bson_destroy(updated);

            if (MONGOLITE_UNLIKELY(rc != 0)) {
                _mongolite_abort_if_auto(db, txn);
                _mongolite_unlock(db);
                return _mongolite_translate_wtree3_error(rc);
            }
        }
    } else {
        /* Upsert without _id: create new document with generated _id */
        bson_t *base = bson_upsert_build_base(filter);
        if (MONGOLITE_UNLIKELY(!base)) {
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM, "Failed to build upsert base");
            return -1;
        }

        /* Apply update operators */
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
        }

        /* Insert new document */
        rc = wtree3_insert_one_txn(txn, tree,
                                    bson_get_data(new_doc), sizeof(bson_oid_t),
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
        bson_destroy(new_doc);
        if (MONGOLITE_UNLIKELY(rc != 0)) {
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return rc;
        }
    }

    rc = _mongolite_commit_if_auto(db, txn, error);
    if (MONGOLITE_UNLIKELY(rc != 0)) {
        _mongolite_unlock(db);
        return -1;
    }

    db->changes = 1;
    _mongolite_unlock(db);
    return 0;
}

/* ============================================================
 * Update many documents
 * ============================================================ */

/* Context for collecting keys that match filter */
typedef struct {
    mongoc_matcher_t *matcher;
    bson_oid_t *keys;      /* Array of matching keys */
    size_t count;          /* Number of collected keys */
    size_t capacity;       /* Allocated capacity */
} collect_keys_ctx_t;

/* Scan callback to collect matching document keys */
static bool _collect_matching_keys_cb(const void *key, size_t key_len,
                                       const void *value, size_t value_len,
                                       void *user_data) {
    collect_keys_ctx_t *ctx = (collect_keys_ctx_t *)user_data;

    /* Parse document */
    bson_t doc;
    bson_init_static(&doc, value, value_len);

    /* Apply filter if specified */
    if (ctx->matcher && !mongoc_matcher_match(ctx->matcher, &doc)) {
        return true;  /* Continue scanning */
    }

    /* Grow array if needed */
    if (ctx->count >= ctx->capacity) {
        size_t new_cap = ctx->capacity == 0 ? 16 : ctx->capacity * 2;
        bson_oid_t *new_keys = realloc(ctx->keys, new_cap * sizeof(bson_oid_t));
        if (!new_keys) {
            return false;  /* Stop on allocation failure */
        }
        ctx->keys = new_keys;
        ctx->capacity = new_cap;
    }

    /* Store key (must be OID size) */
    if (key_len == sizeof(bson_oid_t)) {
        memcpy(&ctx->keys[ctx->count], key, sizeof(bson_oid_t));
        ctx->count++;
    }

    return true;  /* Continue scanning */
}

MONGOLITE_HOT
int mongolite_update_many(mongolite_db_t *db, const char *collection,
                         const bson_t *filter, const bson_t *update,
                         bool upsert, int64_t *modified_count, gerror_t *error) {
    VALIDATE_DB_COLLECTION_UPDATE(db, collection, update, error, -1);

    if (modified_count) {
        *modified_count = 0;
    }

    /* Compile matcher if filter provided */
    mongoc_matcher_t *matcher = NULL;
    bson_error_t bson_err = {0};
    if (filter && !bson_empty(filter)) {
        matcher = mongoc_matcher_new(filter, &bson_err);
        if (!matcher) {
            set_error(error, "bsonmatch", MONGOLITE_EQUERY,
                     "Failed to compile filter: %s", bson_err.message);
            return -1;
        }
    }

    /* Lock database */
    _mongolite_lock(db);

    /* Get collection tree */
    wtree3_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (MONGOLITE_UNLIKELY(!tree)) {
        if (matcher) mongoc_matcher_destroy(matcher);
        _mongolite_unlock(db);
        return -1;
    }

    /* Begin transaction */
    wtree3_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (MONGOLITE_UNLIKELY(!txn)) {
        if (matcher) mongoc_matcher_destroy(matcher);
        _mongolite_unlock(db);
        return -1;
    }

    /* Phase 1: Collect all matching document keys
     * This avoids cursor invalidation issues when modifying during iteration */
    collect_keys_ctx_t collect_ctx = {
        .matcher = matcher,
        .keys = NULL,
        .count = 0,
        .capacity = 0
    };

    int rc = wtree3_scan_range_txn(txn, tree,
                                    NULL, 0,  /* Start from beginning */
                                    NULL, 0,  /* Scan to end */
                                    _collect_matching_keys_cb, &collect_ctx,
                                    error);

    if (matcher) {
        mongoc_matcher_destroy(matcher);
    }

    if (rc != 0) {
        free(collect_ctx.keys);
        _mongolite_abort_if_auto(db, txn);
        _mongolite_unlock(db);
        return -1;
    }

    /* Phase 2: Apply updates to all collected keys */
    int64_t updated_count = 0;
    for (size_t i = 0; i < collect_ctx.count; i++) {
        /* Get current document */
        const void *value;
        size_t value_len;
        rc = wtree3_get_txn(txn, tree, collect_ctx.keys[i].bytes, sizeof(bson_oid_t),
                            &value, &value_len, error);
        if (rc != 0) {
            continue;  /* Document may have been deleted, skip */
        }

        bson_t doc;
        bson_init_static(&doc, value, value_len);

        /* Apply update operators */
        bson_t *updated_doc = bson_update_apply(&doc, update, error);
        if (!updated_doc) {
            free(collect_ctx.keys);
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return -1;
        }

        /* Update document */
        rc = wtree3_update_txn(txn, tree,
                               collect_ctx.keys[i].bytes, sizeof(bson_oid_t),
                               bson_get_data(updated_doc), updated_doc->len,
                               error);
        bson_destroy(updated_doc);

        if (rc != 0) {
            free(collect_ctx.keys);
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return -1;
        }

        updated_count++;
    }

    free(collect_ctx.keys);

    /* Handle upsert if no matches */
    if (updated_count == 0 && upsert) {
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
        updated_count = 1;  /* We inserted one document */
    }

    rc = _mongolite_commit_if_auto(db, txn, error);
    if (MONGOLITE_UNLIKELY(rc != 0)) {
        _mongolite_unlock(db);
        return -1;
    }

    if (modified_count) {
        *modified_count = updated_count;
    }
    db->changes = (int)updated_count;
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
 * Find and Modify (Atomic Operations)
 * ============================================================ */

/**
 * Context for wtree3_modify_txn callback
 */
typedef struct {
    const bson_t *update;
    const bson_t *filter;
    bool upsert;
    bool return_new;
    bson_t *old_doc;  /* Copy of document before modification */
    gerror_t *error;
} find_modify_ctx_t;

/**
 * Modify callback for wtree3_modify_txn
 * Atomically updates a document and returns the result
 */
static void* _find_and_modify_cb(const void *existing_value, size_t existing_len,
                                  void *user_data, size_t *out_len) {
    find_modify_ctx_t *ctx = (find_modify_ctx_t *)user_data;

    if (existing_value) {
        /* Document exists - apply update */
        bson_t existing;
        bson_init_static(&existing, existing_value, existing_len);

        /* Save old document if return_new is false */
        if (!ctx->return_new) {
            ctx->old_doc = bson_copy(&existing);
        }

        /* Apply update operators */
        bson_t *updated = bson_update_apply(&existing, ctx->update, ctx->error);
        if (!updated) {
            return NULL;
        }

        /* Save new document if return_new is true */
        if (ctx->return_new) {
            ctx->old_doc = bson_copy(updated);
        }

        /* Return malloc'd copy for wtree3 */
        *out_len = updated->len;
        void *data = malloc(*out_len);
        if (data) {
            memcpy(data, bson_get_data(updated), *out_len);
        }
        bson_destroy(updated);
        return data;
    } else if (ctx->upsert) {
        /* Document doesn't exist - create new one (upsert) */
        bson_t *base = bson_upsert_build_base(ctx->filter);
        if (!base) {
            set_error(ctx->error, MONGOLITE_LIB, MONGOLITE_ENOMEM, "Failed to build upsert base");
            return NULL;
        }

        bson_t *new_doc = bson_update_apply(base, ctx->update, ctx->error);
        bson_destroy(base);
        if (!new_doc) {
            return NULL;
        }

        /* For upsert insert, old_doc is NULL (no old document to return) */
        if (ctx->return_new) {
            ctx->old_doc = bson_copy(new_doc);
        }

        /* Return malloc'd copy */
        *out_len = new_doc->len;
        void *data = malloc(*out_len);
        if (data) {
            memcpy(data, bson_get_data(new_doc), *out_len);
        }
        bson_destroy(new_doc);
        return data;
    }

    /* Document doesn't exist and not upsert - return NULL (no modification) */
    return NULL;
}

bson_t* mongolite_find_and_modify(mongolite_db_t *db, const char *collection,
                                  const bson_t *filter, const bson_t *update,
                                  bool return_new, bool upsert, gerror_t *error) {
    VALIDATE_DB_COLLECTION_UPDATE(db, collection, update, error, NULL);

    /* Check for direct _id lookup */
    bson_oid_t oid;
    bool has_id = _mongolite_is_id_query(filter, &oid);

    if (!has_id && !upsert) {
        /* Need to find document first to get _id */
        bson_t *existing = mongolite_find_one(db, collection, filter, NULL, error);
        if (!existing) {
            return NULL;  /* No match */
        }

        if (MONGOLITE_UNLIKELY(!extract_doc_oid_with_error(existing, &oid, error))) {
            bson_destroy(existing);
            return NULL;
        }
        bson_destroy(existing);
        has_id = true;
    }

    if (!has_id && !upsert) {
        /* No _id and not upsert - nothing to do */
        return NULL;
    }

    /* Lock database */
    _mongolite_lock(db);

    /* Get collection tree */
    wtree3_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (MONGOLITE_UNLIKELY(!tree)) {
        _mongolite_unlock(db);
        return NULL;
    }

    /* Begin transaction */
    wtree3_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (MONGOLITE_UNLIKELY(!txn)) {
        _mongolite_unlock(db);
        return NULL;
    }

    bson_t *result = NULL;

    if (has_id) {
        /* Use wtree3_modify_txn for atomic find-and-modify */
        find_modify_ctx_t ctx = {
            .update = update,
            .filter = filter,
            .upsert = upsert,
            .return_new = return_new,
            .old_doc = NULL,
            .error = error
        };

        int rc = wtree3_modify_txn(txn, tree,
                                    oid.bytes, sizeof(oid.bytes),
                                    _find_and_modify_cb, &ctx,
                                    error);

        if (rc != 0) {
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return NULL;
        }

        result = ctx.old_doc;
    } else {
        /* Upsert without _id: generate new _id and insert */
        bson_t *base = bson_upsert_build_base(filter);
        if (MONGOLITE_UNLIKELY(!base)) {
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM, "Failed to build upsert base");
            return NULL;
        }

        bson_t *new_doc = bson_update_apply(base, update, error);
        bson_destroy(base);
        if (MONGOLITE_UNLIKELY(!new_doc)) {
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return NULL;
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
                return NULL;
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
        }

        /* Insert new document */
        int rc = wtree3_insert_one_txn(txn, tree,
                                        bson_get_data(new_doc), sizeof(bson_oid_t),
                                        bson_get_data(new_doc), new_doc->len,
                                        error);
        if (MONGOLITE_UNLIKELY(rc != 0)) {
            bson_destroy(new_doc);
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return NULL;
        }

        /* Update doc count */
        rc = _mongolite_update_doc_count_txn(db, txn, collection, 1, error);
        if (MONGOLITE_UNLIKELY(rc != 0)) {
            bson_destroy(new_doc);
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return NULL;
        }

        /* Return new document if requested, otherwise NULL (no old document) */
        result = return_new ? new_doc : NULL;
        if (!return_new) {
            bson_destroy(new_doc);
        }
    }

    int rc = _mongolite_commit_if_auto(db, txn, error);
    if (MONGOLITE_UNLIKELY(rc != 0)) {
        if (result) bson_destroy(result);
        _mongolite_unlock(db);
        return NULL;
    }

    db->changes = 1;
    _mongolite_unlock(db);
    return result;
}

bson_t* mongolite_find_and_modify_json(mongolite_db_t *db, const char *collection,
                                       const char *filter_json, const char *update_json,
                                       bool return_new, bool upsert, gerror_t *error) {
    bson_t *filter = parse_optional_json_to_bson(filter_json, error);
    if (filter_json && !filter) {
        return NULL;
    }

    bson_t *update = parse_json_to_bson(update_json, error);
    if (!update) {
        if (filter) bson_destroy(filter);
        return NULL;
    }

    bson_t *result = mongolite_find_and_modify(db, collection, filter, update,
                                                return_new, upsert, error);

    if (filter) bson_destroy(filter);
    bson_destroy(update);
    return result;
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
