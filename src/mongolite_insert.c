/*
 * mongolite_insert.c - Insert operations
 *
 * Handles:
 * - insert_one / insert_many
 * - JSON wrappers
 * - _id generation
 * - doc_count updates
 *
 * Note: With wtree2, index maintenance is automatic.
 */

#include "mongolite_internal.h"
#include "macros.h"
#include <stdlib.h>
#include <string.h>

#define MONGOLITE_LIB "mongolite"

/* Maximum resize attempts to prevent infinite loops */
#define MONGOLITE_MAX_RESIZE_ATTEMPTS 3

/*
 * Optional compile-time limit for maximum database size after auto-resize.
 * Define MONGOLITE_MAX_DB_SIZE to a byte value to enforce a limit.
 * If not defined or set to 0, only overflow is checked (no artificial limit).
 *
 * Example: -DMONGOLITE_MAX_DB_SIZE=1099511627776ULL  (1TB)
 */
#ifndef MONGOLITE_MAX_DB_SIZE
#define MONGOLITE_MAX_DB_SIZE 0
#endif

/* ============================================================
 * Internal: Try to resize database on MDB_MAP_FULL
 *
 * Doubles the current mapsize. Returns 0 on success.
 * Should only be called when no transactions are active.
 * ============================================================ */
int _mongolite_try_resize(mongolite_db_t *db, gerror_t *error) {
    if (!db || !db->wdb) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "Invalid database handle");
        return MONGOLITE_EINVAL;
    }

    size_t current_size = wtree2_db_get_mapsize(db->wdb);
    size_t new_size = current_size * 2;

    /* Check for overflow */
    if (new_size < current_size) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_ERROR,
                 "Database size overflow");
        return MONGOLITE_ERROR;
    }

#if MONGOLITE_MAX_DB_SIZE > 0
    /* Check compile-time size limit */
    if (new_size > (size_t)MONGOLITE_MAX_DB_SIZE) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_ERROR,
                 "Database would exceed maximum size limit");
        return MONGOLITE_ERROR;
    }
#endif

    int rc = wtree2_db_resize(db->wdb, new_size, error);
    if (rc == 0) {
        db->max_bytes = new_size;
    }

    return rc;
}

/* ============================================================
 * Internal: Ensure document has _id field
 *
 * If doc has _id, copies it to out_id and returns the doc as-is.
 * If doc lacks _id, creates a new doc with generated _id prepended.
 * Returns: the document to use (may be original or new copy)
 *          Caller must check if returned doc != input doc and destroy if so.
 * ============================================================ */

MONGOLITE_HOT
static bson_t* _ensure_id(const bson_t *doc, bson_oid_t *out_id, bool *was_generated) {
    bson_iter_t iter;

    *was_generated = false;

    /* Check if _id exists */
    if (MONGOLITE_LIKELY(bson_iter_init_find(&iter, doc, "_id"))) {
        /* Extract existing _id */
        if (MONGOLITE_LIKELY(BSON_ITER_HOLDS_OID(&iter))) {
            bson_oid_copy(bson_iter_oid(&iter), out_id);
        } else {
            /* Non-OID _id - generate a hash or use as-is */
            /* For simplicity, generate new OID for non-OID _id tracking */
            bson_oid_init(out_id, NULL);
        }
        return (bson_t*)doc;  /* Use original */
    }

    /* No _id - generate one and prepend */
    *was_generated = true;
    bson_oid_init(out_id, NULL);

    bson_t *new_doc = bson_new();
    if (MONGOLITE_UNLIKELY(!new_doc)) return NULL;

    /* Prepend _id */
    BSON_APPEND_OID(new_doc, "_id", out_id);

    /* Copy all fields from original */
    bson_iter_init(&iter, doc);
    while (bson_iter_next(&iter)) {
        const bson_value_t *value = bson_iter_value(&iter);
        bson_append_value(new_doc, bson_iter_key(&iter), -1, value);
    }

    return new_doc;
}

/* ============================================================
 * Internal: Check if error is MAP_FULL (needs resize)
 * ============================================================ */
static inline bool _is_map_full_error(int rc) {
    return (rc == WTREE_MAP_FULL || rc == WTREE2_MAP_FULL || rc == MDB_MAP_FULL);
}

/* ============================================================
 * Insert One
 *
 * With wtree2, indexes are maintained automatically.
 * ============================================================ */

MONGOLITE_HOT
int mongolite_insert_one(mongolite_db_t *db, const char *collection,
                          const bson_t *doc, bson_oid_t *inserted_id,
                          gerror_t *error) {
    VALIDATE_DB_COLLECTION_DOC(db, collection, doc, error, MONGOLITE_EINVAL);

    _mongolite_lock(db);

    /* Get collection tree (wtree2 - handles indexes automatically) */
    wtree2_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (MONGOLITE_UNLIKELY(!tree)) {
        _mongolite_unlock(db);
        return MONGOLITE_ERROR;
    }

    /* Ensure document has _id */
    bson_oid_t oid;
    bool id_generated = false;
    bson_t *final_doc = _ensure_id(doc, &oid, &id_generated);
    if (MONGOLITE_UNLIKELY(!final_doc)) {
        _mongolite_unlock(db);
        set_error(error, "system", MONGOLITE_ENOMEM,
                 "Failed to prepare document");
        return MONGOLITE_ENOMEM;
    }

    int rc = MONGOLITE_OK;
    int resize_attempts = 0;

retry_insert:
    /* Begin transaction */
    {
        wtree2_txn_t *txn = _mongolite_get_write_txn(db, error);
        if (MONGOLITE_UNLIKELY(!txn)) {
            if (id_generated) bson_destroy(final_doc);
            _mongolite_unlock(db);
            return MONGOLITE_ERROR;
        }

        /* Insert: key = OID (12 bytes), value = BSON document
         * wtree2 automatically maintains indexes */
        rc = wtree2_insert_one_txn(txn, tree,
                                    oid.bytes, sizeof(oid.bytes),
                                    bson_get_data(final_doc), final_doc->len,
                                    error);

        if (MONGOLITE_UNLIKELY(rc != 0)) {
            _mongolite_abort_if_auto(db, txn);

            /* Check if we can retry with a resize */
            if (_is_map_full_error(rc) && resize_attempts < MONGOLITE_MAX_RESIZE_ATTEMPTS) {
                resize_attempts++;
                gerror_t resize_error = {0};
                if (_mongolite_try_resize(db, &resize_error) == 0) {
                    /* Clear the previous error and retry */
                    if (error) {
                        error->code = 0;
                        error->message[0] = '\0';
                    }
                    goto retry_insert;
                }
                /* Resize failed - keep original error */
            }

            if (id_generated) bson_destroy(final_doc);
            _mongolite_unlock(db);
            return _mongolite_translate_wtree2_error(rc);
        }

        /* Note: Index maintenance is automatic with wtree2_insert_one_txn */
        /* Note: Doc count is maintained by wtree2 internally */

        /* Update schema doc count for persistence */
        rc = _mongolite_update_doc_count_txn(db, txn, collection, 1, error);
        if (MONGOLITE_UNLIKELY(rc != 0)) {
            _mongolite_abort_if_auto(db, txn);
            if (id_generated) bson_destroy(final_doc);
            _mongolite_unlock(db);
            return rc;
        }

        /* Commit */
        rc = _mongolite_commit_if_auto(db, txn, error);
        if (MONGOLITE_UNLIKELY(rc != 0)) {
            /* Check if commit failed due to MAP_FULL */
            if (_is_map_full_error(rc) && resize_attempts < MONGOLITE_MAX_RESIZE_ATTEMPTS) {
                resize_attempts++;
                gerror_t resize_error = {0};
                if (_mongolite_try_resize(db, &resize_error) == 0) {
                    if (error) {
                        error->code = 0;
                        error->message[0] = '\0';
                    }
                    goto retry_insert;
                }
            }

            if (id_generated) bson_destroy(final_doc);
            _mongolite_unlock(db);
            return rc;
        }
    }

    /* Return inserted _id */
    if (inserted_id) {
        bson_oid_copy(&oid, inserted_id);
    }

    /* Update db state */
    db->last_insert_rowid = _mongolite_oid_to_rowid(&oid);
    db->changes = 1;

    if (id_generated) bson_destroy(final_doc);
    _mongolite_unlock(db);

    return MONGOLITE_OK;
}

/* ============================================================
 * Insert Many
 * ============================================================ */

MONGOLITE_HOT
int mongolite_insert_many(mongolite_db_t *db, const char *collection,
                           const bson_t **docs, size_t n_docs,
                           bson_oid_t **inserted_ids, gerror_t *error) {
    VALIDATE_PARAMS(db && collection && docs && n_docs > 0, error,
                   "Database, collection, and documents are required", MONGOLITE_EINVAL);

    _mongolite_lock(db);

    /* Get collection tree (wtree2 - handles indexes automatically) */
    wtree2_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (MONGOLITE_UNLIKELY(!tree)) {
        _mongolite_unlock(db);
        return MONGOLITE_ERROR;
    }

    /* Allocate array for OIDs if requested */
    bson_oid_t *oids = NULL;
    if (inserted_ids) {
        oids = calloc(n_docs, sizeof(bson_oid_t));
        if (MONGOLITE_UNLIKELY(!oids)) {
            _mongolite_unlock(db);
            set_error(error, "system", MONGOLITE_ENOMEM,
                     "Failed to allocate OID array");
            return MONGOLITE_ENOMEM;
        }
    }

    int rc = MONGOLITE_OK;
    int resize_attempts = 0;

retry_insert_many:
    {
        /* Begin transaction */
        wtree2_txn_t *txn = _mongolite_get_write_txn(db, error);
        if (MONGOLITE_UNLIKELY(!txn)) {
            free(oids);
            _mongolite_unlock(db);
            return MONGOLITE_ERROR;
        }

        /* Track documents that need cleanup */
        bson_t **generated_docs = calloc(n_docs, sizeof(bson_t*));
        if (MONGOLITE_UNLIKELY(!generated_docs)) {
            _mongolite_abort_if_auto(db, txn);
            free(oids);
            _mongolite_unlock(db);
            set_error(error, "system", MONGOLITE_ENOMEM,
                     "Failed to allocate tracking array");
            return MONGOLITE_ENOMEM;
        }

        size_t inserted = 0;
        rc = MONGOLITE_OK;

        for (size_t i = 0; i < n_docs; i++) {
            if (MONGOLITE_UNLIKELY(!docs[i])) continue;

            bson_oid_t oid;
            bool id_generated = false;
            bson_t *final_doc = _ensure_id(docs[i], &oid, &id_generated);

            if (MONGOLITE_UNLIKELY(!final_doc)) {
                rc = MONGOLITE_ENOMEM;
                set_error(error, "system", MONGOLITE_ENOMEM,
                         "Failed to prepare document %zu", i);
                break;
            }

            if (id_generated) {
                generated_docs[i] = final_doc;
            }

            /* Insert via wtree2 - indexes maintained automatically */
            rc = wtree2_insert_one_txn(txn, tree,
                                        oid.bytes, sizeof(oid.bytes),
                                        bson_get_data(final_doc), final_doc->len,
                                        error);

            if (MONGOLITE_UNLIKELY(rc != 0)) {
                break;
            }

            if (oids) {
                bson_oid_copy(&oid, &oids[i]);
            }

            inserted++;
        }

        /* Cleanup generated docs */
        for (size_t i = 0; i < n_docs; i++) {
            if (generated_docs[i]) {
                bson_destroy(generated_docs[i]);
            }
        }
        free(generated_docs);

        if (MONGOLITE_UNLIKELY(rc != 0)) {
            _mongolite_abort_if_auto(db, txn);

            /* Check if we can retry with a resize */
            if (_is_map_full_error(rc) && resize_attempts < MONGOLITE_MAX_RESIZE_ATTEMPTS) {
                resize_attempts++;
                gerror_t resize_error = {0};
                if (_mongolite_try_resize(db, &resize_error) == 0) {
                    /* Clear error and retry from beginning */
                    if (error) {
                        error->code = 0;
                        error->message[0] = '\0';
                    }
                    /* Reset oids array for retry */
                    if (oids) {
                        memset(oids, 0, n_docs * sizeof(bson_oid_t));
                    }
                    goto retry_insert_many;
                }
            }

            free(oids);
            _mongolite_unlock(db);
            return _mongolite_translate_wtree2_error(rc);
        }

        /* Update schema doc count for persistence */
        rc = _mongolite_update_doc_count_txn(db, txn, collection, (int64_t)inserted, error);
        if (MONGOLITE_UNLIKELY(rc != 0)) {
            _mongolite_abort_if_auto(db, txn);
            free(oids);
            _mongolite_unlock(db);
            return rc;
        }

        /* Commit */
        rc = _mongolite_commit_if_auto(db, txn, error);
        if (MONGOLITE_UNLIKELY(rc != 0)) {
            /* Check if commit failed due to MAP_FULL */
            if (_is_map_full_error(rc) && resize_attempts < MONGOLITE_MAX_RESIZE_ATTEMPTS) {
                resize_attempts++;
                gerror_t resize_error = {0};
                if (_mongolite_try_resize(db, &resize_error) == 0) {
                    if (error) {
                        error->code = 0;
                        error->message[0] = '\0';
                    }
                    if (oids) {
                        memset(oids, 0, n_docs * sizeof(bson_oid_t));
                    }
                    goto retry_insert_many;
                }
            }

            free(oids);
            _mongolite_unlock(db);
            return rc;
        }

        /* Return OIDs */
        if (inserted_ids) {
            *inserted_ids = oids;
        } else {
            free(oids);
        }

        /* Update db state */
        db->changes = (int)inserted;
    }

    _mongolite_unlock(db);
    return MONGOLITE_OK;
}

/* ============================================================
 * Insert One JSON
 * ============================================================ */

int mongolite_insert_one_json(mongolite_db_t *db, const char *collection,
                               const char *json_str, bson_oid_t *inserted_id,
                               gerror_t *error) {
    VALIDATE_PARAMS(db && collection && json_str, error,
                   "Database, collection, and JSON string are required", MONGOLITE_EINVAL);

    bson_t *doc = parse_json_to_bson(json_str, error);
    if (MONGOLITE_UNLIKELY(!doc)) {
        return MONGOLITE_EINVAL;
    }

    int rc = mongolite_insert_one(db, collection, doc, inserted_id, error);
    bson_destroy(doc);

    return rc;
}

/* ============================================================
 * Insert Many JSON
 * ============================================================ */

int mongolite_insert_many_json(mongolite_db_t *db, const char *collection,
                                const char **json_strs, size_t n_docs,
                                bson_oid_t **inserted_ids, gerror_t *error) {
    VALIDATE_PARAMS(db && collection && json_strs && n_docs > 0, error,
                   "Database, collection, and JSON strings are required", MONGOLITE_EINVAL);

    /* Parse all JSON strings */
    bson_t **docs = calloc(n_docs, sizeof(bson_t*));
    if (MONGOLITE_UNLIKELY(!docs)) {
        set_error(error, "system", MONGOLITE_ENOMEM,
                 "Failed to allocate document array");
        return MONGOLITE_ENOMEM;
    }

    int rc = MONGOLITE_OK;

    for (size_t i = 0; i < n_docs; i++) {
        if (MONGOLITE_UNLIKELY(!json_strs[i])) continue;

        docs[i] = parse_json_to_bson(json_strs[i], error);
        if (MONGOLITE_UNLIKELY(!docs[i])) {
            rc = MONGOLITE_EINVAL;
            break;
        }
    }

    if (rc == MONGOLITE_OK) {
        rc = mongolite_insert_many(db, collection, (const bson_t**)docs, n_docs,
                                   inserted_ids, error);
    }

    /* Cleanup */
    for (size_t i = 0; i < n_docs; i++) {
        if (docs[i]) bson_destroy(docs[i]);
    }
    free(docs);

    return rc;
}
