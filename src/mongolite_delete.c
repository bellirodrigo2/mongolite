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

    if (MONGOLITE_LIKELY(_mongolite_is_id_query(filter, &doc_id))) {
        /* Fast path: direct _id lookup */
        _mongolite_lock(db);
        wtree_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
        if (MONGOLITE_LIKELY(tree)) {
            doc_to_delete = _mongolite_find_by_id(db, tree, &doc_id, error);
        }
        /* Keep lock for delete operation below */
    } else {
        /* Slow path: full scan to find document */
        doc_to_delete = mongolite_find_one(db, collection, filter, NULL, error);
        if (MONGOLITE_UNLIKELY(!doc_to_delete)) {
            /* No match found - not an error */
            return 0;
        }

        /* Extract _id */
        if (MONGOLITE_UNLIKELY(!extract_doc_oid_with_error(doc_to_delete, &doc_id, error))) {
            bson_destroy(doc_to_delete);
            return -1;
        }

        /* Lock database for delete */
        _mongolite_lock(db);
    }

    /* If document wasn't found, unlock and return */
    if (!doc_to_delete) {
        _mongolite_unlock(db);
        return 0;  /* No match found - not an error */
    }

    /* Get collection tree */
    wtree_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (MONGOLITE_UNLIKELY(!tree)) {
        bson_destroy(doc_to_delete);
        _mongolite_unlock(db);
        return -1;
    }

    /* Pre-load index cache before starting transaction (avoids read txn inside write txn) */
    size_t index_count = 0;
    (void)_mongolite_get_cached_indexes(db, collection, &index_count, error);

    /* Begin transaction */
    wtree_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (MONGOLITE_UNLIKELY(!txn)) {
        bson_destroy(doc_to_delete);
        _mongolite_unlock(db);
        return -1;
    }

    /* Maintain secondary indexes (delete from indexes before deleting doc) */
    int rc = _mongolite_index_delete(db, txn, collection, doc_to_delete, error);
    if (MONGOLITE_UNLIKELY(rc != 0)) {
        bson_destroy(doc_to_delete);
        _mongolite_abort_if_auto(db, txn);
        _mongolite_unlock(db);
        return -1;
    }

    /* Delete document */
    bool deleted = false;
    rc = wtree_delete_one_txn(txn, tree,
                               doc_id.bytes, sizeof(doc_id.bytes),
                               &deleted, error);
    if (MONGOLITE_UNLIKELY(rc != 0)) {
        bson_destroy(doc_to_delete);
        _mongolite_abort_if_auto(db, txn);
        _mongolite_unlock(db);
        return -1;
    }

    bson_destroy(doc_to_delete);

    /* Update doc count within the same transaction for atomicity */
    if (deleted) {
        rc = _mongolite_update_doc_count_txn(db, txn, collection, -1, error);
        if (MONGOLITE_UNLIKELY(rc != 0)) {
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return -1;
        }
    }

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

/* Helper struct to store document info for deletion */
typedef struct {
    bson_oid_t id;
    bson_t *doc;  /* Full document (for index maintenance) */
} delete_info_t;

/* ============================================================
 * Delete many documents
 * ============================================================ */

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

    /* Get collection tree */
    wtree_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (MONGOLITE_UNLIKELY(!tree)) {
        _mongolite_unlock(db);
        return -1;
    }

    /* Pre-load index cache before starting transaction (avoids read txn inside write txn) */
    size_t index_count = 0;
    (void)_mongolite_get_cached_indexes(db, collection, &index_count, error);

    /* Begin transaction */
    wtree_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (MONGOLITE_UNLIKELY(!txn)) {
        _mongolite_unlock(db);
        return -1;
    }

    /* Create cursor using the existing transaction (avoids deadlock) */
    mongolite_cursor_t *cursor = _mongolite_cursor_create_with_txn(db, tree, collection,
                                                                    txn, filter, error);
    if (MONGOLITE_UNLIKELY(!cursor)) {
        _mongolite_abort_if_auto(db, txn);
        _mongolite_unlock(db);
        return -1;
    }

    /* Collect all documents to delete (need full docs for index maintenance) */
    delete_info_t *docs = NULL;
    size_t n_docs = 0;
    size_t docs_capacity = 16;

    docs = malloc(sizeof(delete_info_t) * docs_capacity);
    if (MONGOLITE_UNLIKELY(!docs)) {
        mongolite_cursor_destroy(cursor);
        _mongolite_abort_if_auto(db, txn);
        _mongolite_unlock(db);
        set_error(error, "system", MONGOLITE_ENOMEM, "Out of memory");
        return -1;
    }

    const bson_t *doc;
    bson_oid_t oid;
    while (MONGOLITE_LIKELY(mongolite_cursor_next(cursor, &doc))) {
        /* Extract _id - skip documents without valid OID */
        EXTRACT_OID_OR_CONTINUE(doc, oid);

        /* Expand array if needed */
        if (MONGOLITE_UNLIKELY(n_docs >= docs_capacity)) {
            docs_capacity *= 2;
            delete_info_t *new_docs = realloc(docs, sizeof(delete_info_t) * docs_capacity);
            if (MONGOLITE_UNLIKELY(!new_docs)) {
                /* Free already collected docs */
                for (size_t j = 0; j < n_docs; j++) {
                    bson_destroy(docs[j].doc);
                }
                free(docs);
                mongolite_cursor_destroy(cursor);
                _mongolite_abort_if_auto(db, txn);
                _mongolite_unlock(db);
                set_error(error, "system", MONGOLITE_ENOMEM, "Out of memory");
                return -1;
            }
            docs = new_docs;
        }

        bson_oid_copy(&oid, &docs[n_docs].id);
        docs[n_docs].doc = bson_copy(doc);
        if (MONGOLITE_UNLIKELY(!docs[n_docs].doc)) {
            for (size_t j = 0; j < n_docs; j++) {
                bson_destroy(docs[j].doc);
            }
            free(docs);
            mongolite_cursor_destroy(cursor);
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            set_error(error, "system", MONGOLITE_ENOMEM, "Out of memory");
            return -1;
        }
        n_docs++;
    }

    mongolite_cursor_destroy(cursor);

    /* Now delete all collected documents */
    int64_t count = 0;
    int rc = 0;

    for (size_t i = 0; i < n_docs; i++) {
        /* Maintain secondary indexes first */
        rc = _mongolite_index_delete(db, txn, collection, docs[i].doc, error);
        if (MONGOLITE_UNLIKELY(rc != 0)) {
            for (size_t j = 0; j < n_docs; j++) {
                bson_destroy(docs[j].doc);
            }
            free(docs);
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return -1;
        }

        /* Delete the document */
        bool deleted = false;
        rc = wtree_delete_one_txn(txn, tree,
                                   docs[i].id.bytes, sizeof(docs[i].id.bytes),
                                   &deleted, error);
        if (MONGOLITE_UNLIKELY(rc != 0)) {
            for (size_t j = 0; j < n_docs; j++) {
                bson_destroy(docs[j].doc);
            }
            free(docs);
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return -1;
        }

        if (deleted) {
            count++;
        }
    }

    /* Free collected documents */
    for (size_t i = 0; i < n_docs; i++) {
        bson_destroy(docs[i].doc);
    }
    free(docs);

    /* Update doc count within the same transaction for atomicity */
    if (MONGOLITE_LIKELY(count > 0)) {
        rc = _mongolite_update_doc_count_txn(db, txn, collection, -count, error);
        if (MONGOLITE_UNLIKELY(rc != 0)) {
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return -1;
        }
    }

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
