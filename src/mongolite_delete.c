/*
 * mongolite_delete.c - Delete operations (Phase 7)
 *
 * Functions:
 * - mongolite_delete_one() - Delete first matching document
 * - mongolite_delete_many() - Delete all matching documents
 */

#include "mongolite_internal.h"
#include "macros.h"
#include <string.h>
#include <stdlib.h>

#include "mongoc-matcher.h"
#define MONGOLITE_LIB "mongolite"

/* ============================================================
 * Delete one document
 * ============================================================ */

MONGOLITE_HOT
int mongolite_delete_one(mongolite_db_t *db, const char *collection,
                         const bson_t *filter, gerror_t *error) {
    VALIDATE_DB_COLLECTION(db, collection, error, -1);

    /* We need to find the document first (for index maintenance) */
    bson_oid_t doc_id;
    bson_t *doc_to_delete = NULL;

    /* Lock database FIRST to avoid race conditions */
    _mongolite_lock(db);

    /* Get collection tree */
    wtree3_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (MONGOLITE_UNLIKELY(!tree)) {
        _mongolite_unlock(db);
        return -1;
    }

    if (MONGOLITE_LIKELY(_mongolite_is_id_query(filter, &doc_id))) {
        /* Fast path: direct _id lookup */
        doc_to_delete = _mongolite_find_by_id(db, tree, &doc_id, error);
    } else {
        /* Slow path: full scan to find document (under lock) */
        doc_to_delete = _mongolite_find_one_scan(db, tree, collection, filter, error);
        if (MONGOLITE_UNLIKELY(!doc_to_delete)) {
            /* No match found - not an error */
            _mongolite_unlock(db);
            return 0;
        }

        /* Extract _id */
        if (MONGOLITE_UNLIKELY(!extract_doc_oid_with_error(doc_to_delete, &doc_id, error))) {
            bson_destroy(doc_to_delete);
            _mongolite_unlock(db);
            return -1;
        }
    }

    /* If document wasn't found, unlock and return */
    if (!doc_to_delete) {
        _mongolite_unlock(db);
        return 0;  /* No match found - not an error */
    }

    /* Begin transaction */
    wtree3_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (MONGOLITE_UNLIKELY(!txn)) {
        bson_destroy(doc_to_delete);
        _mongolite_unlock(db);
        return -1;
    }

    bson_destroy(doc_to_delete);

    /* Delete document via wtree3 (indexes maintained automatically) */
    bool deleted = false;
    int rc = wtree3_delete_one_txn(txn, tree,
                                    doc_id.bytes, sizeof(doc_id.bytes),
                                    &deleted, error);
    if (MONGOLITE_UNLIKELY(rc != 0)) {
        _mongolite_abort_if_auto(db, txn);
        _mongolite_unlock(db);
        return -1;
    }

    /* Note: Doc count is maintained by wtree3 internally */

    /* Commit */
    rc = _mongolite_commit_if_auto(db, txn, error);
    if (MONGOLITE_UNLIKELY(rc != 0)) {
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

/* Helper struct to store document id for deletion */
typedef struct {
    bson_oid_t id;
} delete_info_t;

/* ============================================================
 * Delete many documents - Using wtree3_delete_if_txn
 * ============================================================ */

/* Context for delete predicate callback */
typedef struct {
    mongoc_matcher_t *matcher;
} delete_many_ctx_t;

/* Predicate callback: returns true to delete matching documents */
static bool _delete_many_predicate(const void *key, size_t key_len,
                                   const void *value, size_t value_len,
                                   void *user_data) {
    (void)key; (void)key_len;  /* Unused */
    delete_many_ctx_t *ctx = (delete_many_ctx_t*)user_data;

    /* Parse document */
    bson_t doc;
    if (MONGOLITE_UNLIKELY(!bson_init_static(&doc, value, value_len))) {
        return false;  /* Don't delete on parse error */
    }

    /* Check if matches filter */
    if (ctx->matcher) {
        return mongoc_matcher_match(ctx->matcher, &doc);
    }

    return true;  /* No filter = delete all */
}

MONGOLITE_HOT
int mongolite_delete_many(mongolite_db_t *db, const char *collection,
                          const bson_t *filter, int64_t *deleted_count,
                          gerror_t *error) {
    VALIDATE_DB_COLLECTION(db, collection, error, -1);

    if (deleted_count) {
        *deleted_count = 0;
    }

    /* Lock database */
    _mongolite_lock(db);

    /* Get collection tree (wtree3 - indexes maintained automatically) */
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

    /* Create matcher if we have a filter */
    mongoc_matcher_t *matcher = NULL;
    if (filter && !bson_empty(filter)) {
        bson_error_t bson_err;
        matcher = mongoc_matcher_new(filter, &bson_err);
        if (MONGOLITE_UNLIKELY(!matcher)) {
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            set_error(error, "bsonmatch", MONGOLITE_EQUERY,
                     "Invalid query: %s", bson_err.message);
            return -1;
        }
    }

    /* Single-pass delete using wtree3_delete_if_txn - indexes maintained automatically */
    delete_many_ctx_t ctx = { .matcher = matcher };
    size_t count = 0;
    int rc = wtree3_delete_if_txn(txn, tree, NULL, 0, NULL, 0,
                                   _delete_many_predicate, &ctx, &count, error);

    if (matcher) {
        mongoc_matcher_destroy(matcher);
    }

    /* Note: Doc count is maintained by wtree3 internally */

    /* Commit */
    rc = _mongolite_commit_if_auto(db, txn, error);
    if (MONGOLITE_UNLIKELY(rc != 0)) {
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
