/*
 * mongolite_schema.c - Schema management
 *
 * Handles:
 * - Schema entry operations (to_bson, from_bson, free)
 * - Schema CRUD operations (init, get, put, delete, list)
 */

#include "mongolite_internal.h"
#include <stdlib.h>
#include <string.h>

#define MONGOLITE_LIB "mongolite"

/* ============================================================
 * Schema Entry Operations
 * ============================================================ */

void _mongolite_schema_entry_free(mongolite_schema_entry_t *entry) {
    if (!entry) return;
    free(entry->name);
    free(entry->tree_name);
    free(entry->type);
    if (entry->options) bson_destroy(entry->options);
    if (entry->metadata) bson_destroy(entry->metadata);
    /* Note: index fields removed - managed by wtree3 */
    memset(entry, 0, sizeof(*entry));
}

bson_t* _mongolite_schema_entry_to_bson(const mongolite_schema_entry_t *entry) {
    if (!entry) return NULL;

    bson_t *doc = bson_new();
    if (!doc) return NULL;

    /* Required fields */
    BSON_APPEND_OID(doc, SCHEMA_FIELD_ID, &entry->oid);
    if (entry->name) {
        BSON_APPEND_UTF8(doc, SCHEMA_FIELD_NAME, entry->name);
    }
    if (entry->tree_name) {
        BSON_APPEND_UTF8(doc, SCHEMA_FIELD_TREE_NAME, entry->tree_name);
    }
    if (entry->type) {
        BSON_APPEND_UTF8(doc, SCHEMA_FIELD_TYPE, entry->type);
    }

    /* Timestamps */
    BSON_APPEND_DATE_TIME(doc, SCHEMA_FIELD_CREATED_AT, entry->created_at);
    BSON_APPEND_DATE_TIME(doc, SCHEMA_FIELD_MODIFIED_AT, entry->modified_at);

    /* Collection fields (all entries are collections now) */
    BSON_APPEND_INT64(doc, SCHEMA_FIELD_DOC_COUNT, entry->doc_count);

    /* Optional metadata */
    if (entry->options) {
        BSON_APPEND_DOCUMENT(doc, SCHEMA_FIELD_OPTIONS, entry->options);
    }
    if (entry->metadata) {
        BSON_APPEND_DOCUMENT(doc, SCHEMA_FIELD_METADATA, entry->metadata);
    }

    /* Note: Index-specific fields removed - wtree3 handles index persistence */

    return doc;
}

int _mongolite_schema_entry_from_bson(const bson_t *doc,
                                       mongolite_schema_entry_t *entry,
                                       gerror_t *error) {
    if (!doc || !entry) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return MONGOLITE_EINVAL;
    }

    memset(entry, 0, sizeof(*entry));
    bson_iter_t iter;

    if (bson_iter_init_find(&iter, doc, SCHEMA_FIELD_ID) &&
        BSON_ITER_HOLDS_OID(&iter)) {
        bson_oid_copy(bson_iter_oid(&iter), &entry->oid);
    }

    if (bson_iter_init_find(&iter, doc, SCHEMA_FIELD_NAME) &&
        BSON_ITER_HOLDS_UTF8(&iter)) {
        entry->name = strdup(bson_iter_utf8(&iter, NULL));
    }

    if (bson_iter_init_find(&iter, doc, SCHEMA_FIELD_TREE_NAME) &&
        BSON_ITER_HOLDS_UTF8(&iter)) {
        entry->tree_name = strdup(bson_iter_utf8(&iter, NULL));
    }

    if (bson_iter_init_find(&iter, doc, SCHEMA_FIELD_TYPE) &&
        BSON_ITER_HOLDS_UTF8(&iter)) {
        entry->type = strdup(bson_iter_utf8(&iter, NULL));
    }

    if (bson_iter_init_find(&iter, doc, SCHEMA_FIELD_CREATED_AT) &&
        BSON_ITER_HOLDS_DATE_TIME(&iter)) {
        entry->created_at = bson_iter_date_time(&iter);
    }

    if (bson_iter_init_find(&iter, doc, SCHEMA_FIELD_MODIFIED_AT) &&
        BSON_ITER_HOLDS_DATE_TIME(&iter)) {
        entry->modified_at = bson_iter_date_time(&iter);
    }

    if (bson_iter_init_find(&iter, doc, SCHEMA_FIELD_DOC_COUNT) &&
        BSON_ITER_HOLDS_INT64(&iter)) {
        entry->doc_count = bson_iter_int64(&iter);
    }

    /* Options document */
    if (bson_iter_init_find(&iter, doc, SCHEMA_FIELD_OPTIONS) &&
        BSON_ITER_HOLDS_DOCUMENT(&iter)) {
        uint32_t len;
        const uint8_t *data;
        bson_iter_document(&iter, &len, &data);
        entry->options = bson_new_from_data(data, len);
    }

    /* Metadata document */
    if (bson_iter_init_find(&iter, doc, SCHEMA_FIELD_METADATA) &&
        BSON_ITER_HOLDS_DOCUMENT(&iter)) {
        uint32_t len;
        const uint8_t *data;
        bson_iter_document(&iter, &len, &data);
        entry->metadata = bson_new_from_data(data, len);
    }

    /* Note: Index fields removed - wtree3 handles index persistence */
    /* Ignore legacy "indexes" array if present in old databases */

    return MONGOLITE_OK;
}

/* ============================================================
 * Schema CRUD Operations
 * ============================================================ */

int _mongolite_schema_init(mongolite_db_t *db, gerror_t *error) {
    if (!db || !db->wdb) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Database not initialized");
        return MONGOLITE_EINVAL;
    }

    /* Create or open the schema tree */
    db->schema_tree = wtree3_tree_open(db->wdb, MONGOLITE_SCHEMA_TREE, 0, -1, error);
    if (!db->schema_tree) {
        return MONGOLITE_ERROR;
    }

    return MONGOLITE_OK;
}

int _mongolite_schema_get(mongolite_db_t *db, const char *name,
                          mongolite_schema_entry_t *entry, gerror_t *error) {
    if (!db || !name || !entry) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return MONGOLITE_EINVAL;
    }

    wtree3_txn_t *txn = _mongolite_get_read_txn(db, error);
    if (!txn) return MONGOLITE_ERROR;

    const void *value;
    size_t value_size;
    int rc = wtree3_get_txn(txn, db->schema_tree, name, strlen(name), &value, &value_size, error);

    if (rc != 0) {
        _mongolite_release_read_txn(db, txn);
        if (rc == WTREE3_NOT_FOUND) {
            set_error(error, MONGOLITE_LIB, MONGOLITE_ENOTFOUND,
                     "Collection or index not found: %s", name);
            return MONGOLITE_ENOTFOUND;
        }
        return rc;
    }

    /* Parse BSON document */
    bson_t doc;
    if (!bson_init_static(&doc, value, value_size)) {
        _mongolite_release_read_txn(db, txn);
        set_error(error, MONGOLITE_LIB, MONGOLITE_ERROR, "Invalid BSON in schema");
        return MONGOLITE_ERROR;
    }

    rc = _mongolite_schema_entry_from_bson(&doc, entry, error);
    _mongolite_release_read_txn(db, txn);

    return rc;
}

int _mongolite_schema_put(mongolite_db_t *db, const mongolite_schema_entry_t *entry,
                          gerror_t *error) {
    if (!db || !entry || !entry->name) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return MONGOLITE_EINVAL;
    }

    bson_t *doc = _mongolite_schema_entry_to_bson(entry);
    if (!doc) {
        set_error(error, "system", MONGOLITE_ENOMEM, "Failed to create BSON document");
        return MONGOLITE_ENOMEM;
    }

    wtree3_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (!txn) {
        bson_destroy(doc);
        return MONGOLITE_ERROR;
    }

    /* Use update (overwrite) instead of insert to allow updates */
    int rc = wtree3_update_txn(txn, db->schema_tree,
                              entry->name, strlen(entry->name),
                              bson_get_data(doc), doc->len, error);

    bson_destroy(doc);

    if (rc != 0) {
        _mongolite_abort_if_auto(db, txn);
        return rc;
    }

    return _mongolite_commit_if_auto(db, txn, error);
}

int _mongolite_schema_delete(mongolite_db_t *db, const char *name, gerror_t *error) {
    if (!db || !name) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return MONGOLITE_EINVAL;
    }

    wtree3_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (!txn) return MONGOLITE_ERROR;

    bool deleted = false;
    int rc = wtree3_delete_one_txn(txn, db->schema_tree,
                                   name, strlen(name), &deleted, error);

    if (rc != 0) {
        _mongolite_abort_if_auto(db, txn);
        return rc;
    }

    if (!deleted) {
        _mongolite_abort_if_auto(db, txn);
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOTFOUND,
                 "Schema entry not found: %s", name);
        return MONGOLITE_ENOTFOUND;
    }

    return _mongolite_commit_if_auto(db, txn, error);
}

int _mongolite_schema_list(mongolite_db_t *db, char ***names, size_t *count,
                           const char *type_filter, gerror_t *error) {
    if (!db || !names || !count) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return MONGOLITE_EINVAL;
    }

    *names = NULL;
    *count = 0;

    wtree3_txn_t *txn = _mongolite_get_read_txn(db, error);
    if (!txn) return MONGOLITE_ERROR;

    wtree3_iterator_t *iter = wtree3_iterator_create_with_txn(db->schema_tree, txn, error);
    if (!iter) {
        _mongolite_release_read_txn(db, txn);
        return MONGOLITE_ERROR;
    }

    /* First pass: count entries */
    size_t total = 0;
    if (wtree3_iterator_first(iter)) {
        do {
            if (type_filter) {
                const void *value;
                size_t value_size;
                if (wtree3_iterator_value(iter, &value, &value_size)) {
                    bson_t doc;
                    if (bson_init_static(&doc, value, value_size)) {
                        bson_iter_t it;
                        if (bson_iter_init_find(&it, &doc, SCHEMA_FIELD_TYPE) &&
                            BSON_ITER_HOLDS_UTF8(&it)) {
                            if (strcmp(bson_iter_utf8(&it, NULL), type_filter) == 0) {
                                total++;
                            }
                        }
                    }
                }
            } else {
                total++;
            }
        } while (wtree3_iterator_next(iter));
    }

    if (total == 0) {
        wtree3_iterator_close(iter);
        _mongolite_release_read_txn(db, txn);
        return MONGOLITE_OK;
    }

    /* Allocate array */
    *names = calloc(total, sizeof(char*));
    if (!*names) {
        wtree3_iterator_close(iter);
        _mongolite_release_read_txn(db, txn);
        set_error(error, "system", MONGOLITE_ENOMEM, "Failed to allocate names array");
        return MONGOLITE_ENOMEM;
    }

    /* Second pass: collect names */
    size_t idx = 0;
    if (wtree3_iterator_first(iter)) {
        do {
            const void *key;
            size_t key_size;
            const void *value;
            size_t value_size;

            if (!wtree3_iterator_key(iter, &key, &key_size)) continue;
            if (!wtree3_iterator_value(iter, &value, &value_size)) continue;

            bool include = true;
            if (type_filter) {
                include = false;
                bson_t doc;
                if (bson_init_static(&doc, value, value_size)) {
                    bson_iter_t it;
                    if (bson_iter_init_find(&it, &doc, SCHEMA_FIELD_TYPE) &&
                        BSON_ITER_HOLDS_UTF8(&it)) {
                        if (strcmp(bson_iter_utf8(&it, NULL), type_filter) == 0) {
                            include = true;
                        }
                    }
                }
            }

            if (include && idx < total) {
                (*names)[idx] = _mongolite_strndup((const char*)key, key_size);
                idx++;
            }
        } while (wtree3_iterator_next(iter));
    }

    *count = idx;
    wtree3_iterator_close(iter);
    _mongolite_release_read_txn(db, txn);

    return MONGOLITE_OK;
}
