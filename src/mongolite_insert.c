/*
 * mongolite_insert.c - Insert operations
 *
 * Handles:
 * - insert_one / insert_many
 * - JSON wrappers
 * - _id generation
 * - doc_count updates
 */

#include "mongolite_internal.h"
#include <stdlib.h>
#include <string.h>

#define MONGOLITE_LIB "mongolite"

/* ============================================================
 * Internal: Update schema doc_count within an existing transaction
 *
 * This ensures doc_count is updated atomically with the insert/delete
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
 * Internal: Ensure document has _id field
 *
 * If doc has _id, copies it to out_id and returns the doc as-is.
 * If doc lacks _id, creates a new doc with generated _id prepended.
 * Returns: the document to use (may be original or new copy)
 *          Caller must check if returned doc != input doc and destroy if so.
 * ============================================================ */

static bson_t* _ensure_id(const bson_t *doc, bson_oid_t *out_id, bool *was_generated) {
    bson_iter_t iter;

    *was_generated = false;

    /* Check if _id exists */
    if (bson_iter_init_find(&iter, doc, "_id")) {
        /* Extract existing _id */
        if (BSON_ITER_HOLDS_OID(&iter)) {
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
    if (!new_doc) return NULL;

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
 * Insert One
 * ============================================================ */

int mongolite_insert_one(mongolite_db_t *db, const char *collection,
                          const bson_t *doc, bson_oid_t *inserted_id,
                          gerror_t *error) {
    if (!db || !collection || !doc) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "Database, collection, and document are required");
        return MONGOLITE_EINVAL;
    }

    _mongolite_lock(db);

    /* Get collection tree */
    wtree_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (!tree) {
        _mongolite_unlock(db);
        return MONGOLITE_ENOTFOUND;
    }

    /* Ensure document has _id */
    bson_oid_t oid;
    bool id_generated = false;
    bson_t *final_doc = _ensure_id(doc, &oid, &id_generated);
    if (!final_doc) {
        _mongolite_unlock(db);
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM,
                 "Failed to prepare document");
        return MONGOLITE_ENOMEM;
    }

    /* Begin transaction */
    wtree_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (!txn) {
        if (id_generated) bson_destroy(final_doc);
        _mongolite_unlock(db);
        return MONGOLITE_ERROR;
    }

    /* Insert: key = OID (12 bytes), value = BSON document */
    int rc = wtree_insert_one_txn(txn, tree,
                                   oid.bytes, sizeof(oid.bytes),
                                   bson_get_data(final_doc), final_doc->len,
                                   error);

    if (rc != 0) {
        _mongolite_abort_if_auto(db, txn);
        if (id_generated) bson_destroy(final_doc);
        _mongolite_unlock(db);
        return rc;
    }

    /* Update doc count within the same transaction for atomicity */
    rc = _update_doc_count_txn(db, txn, collection, 1, error);
    if (rc != 0) {
        _mongolite_abort_if_auto(db, txn);
        if (id_generated) bson_destroy(final_doc);
        _mongolite_unlock(db);
        return rc;
    }

    /* Commit */
    rc = _mongolite_commit_if_auto(db, txn, error);
    if (rc != 0) {
        if (id_generated) bson_destroy(final_doc);
        _mongolite_unlock(db);
        return rc;
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

int mongolite_insert_many(mongolite_db_t *db, const char *collection,
                           const bson_t **docs, size_t n_docs,
                           bson_oid_t **inserted_ids, gerror_t *error) {
    if (!db || !collection || !docs || n_docs == 0) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "Database, collection, and documents are required");
        return MONGOLITE_EINVAL;
    }

    _mongolite_lock(db);

    /* Get collection tree */
    wtree_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (!tree) {
        _mongolite_unlock(db);
        return MONGOLITE_ENOTFOUND;
    }

    /* Allocate array for OIDs if requested */
    bson_oid_t *oids = NULL;
    if (inserted_ids) {
        oids = calloc(n_docs, sizeof(bson_oid_t));
        if (!oids) {
            _mongolite_unlock(db);
            set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM,
                     "Failed to allocate OID array");
            return MONGOLITE_ENOMEM;
        }
    }

    /* Begin transaction */
    wtree_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (!txn) {
        free(oids);
        _mongolite_unlock(db);
        return MONGOLITE_ERROR;
    }

    /* Track documents that need cleanup */
    bson_t **generated_docs = calloc(n_docs, sizeof(bson_t*));
    if (!generated_docs) {
        _mongolite_abort_if_auto(db, txn);
        free(oids);
        _mongolite_unlock(db);
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM,
                 "Failed to allocate tracking array");
        return MONGOLITE_ENOMEM;
    }

    size_t inserted = 0;
    int rc = MONGOLITE_OK;

    for (size_t i = 0; i < n_docs; i++) {
        if (!docs[i]) continue;

        bson_oid_t oid;
        bool id_generated = false;
        bson_t *final_doc = _ensure_id(docs[i], &oid, &id_generated);

        if (!final_doc) {
            rc = MONGOLITE_ENOMEM;
            set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM,
                     "Failed to prepare document %zu", i);
            break;
        }

        if (id_generated) {
            generated_docs[i] = final_doc;
        }

        rc = wtree_insert_one_txn(txn, tree,
                                   oid.bytes, sizeof(oid.bytes),
                                   bson_get_data(final_doc), final_doc->len,
                                   error);

        if (rc != 0) {
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

    if (rc != 0) {
        _mongolite_abort_if_auto(db, txn);
        free(oids);
        _mongolite_unlock(db);
        return rc;
    }

    /* Update doc count within the same transaction for atomicity */
    rc = _update_doc_count_txn(db, txn, collection, (int64_t)inserted, error);
    if (rc != 0) {
        _mongolite_abort_if_auto(db, txn);
        free(oids);
        _mongolite_unlock(db);
        return rc;
    }

    /* Commit */
    rc = _mongolite_commit_if_auto(db, txn, error);
    if (rc != 0) {
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

    _mongolite_unlock(db);
    return MONGOLITE_OK;
}

/* ============================================================
 * Insert One JSON
 * ============================================================ */

int mongolite_insert_one_json(mongolite_db_t *db, const char *collection,
                               const char *json_str, bson_oid_t *inserted_id,
                               gerror_t *error) {
    if (!db || !collection || !json_str) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "Database, collection, and JSON string are required");
        return MONGOLITE_EINVAL;
    }

    bson_error_t bson_err;
    bson_t *doc = bson_new_from_json((const uint8_t*)json_str, -1, &bson_err);
    if (!doc) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "Invalid JSON: %s", bson_err.message);
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
    if (!db || !collection || !json_strs || n_docs == 0) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "Database, collection, and JSON strings are required");
        return MONGOLITE_EINVAL;
    }

    /* Parse all JSON strings */
    bson_t **docs = calloc(n_docs, sizeof(bson_t*));
    if (!docs) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM,
                 "Failed to allocate document array");
        return MONGOLITE_ENOMEM;
    }

    bson_error_t bson_err;
    int rc = MONGOLITE_OK;

    for (size_t i = 0; i < n_docs; i++) {
        if (!json_strs[i]) continue;

        docs[i] = bson_new_from_json((const uint8_t*)json_strs[i], -1, &bson_err);
        if (!docs[i]) {
            set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                     "Invalid JSON at index %zu: %s", i, bson_err.message);
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
