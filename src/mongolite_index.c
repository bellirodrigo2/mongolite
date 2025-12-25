/*
 * mongolite_index.c - Index infrastructure and helper functions
 *
 * Phase 1: Index Storage Infrastructure
 * - Index tree naming: idx:<collection>:<index_name>
 * - Index key building: extracted fields + _id for uniqueness
 * - Index name generation from key spec
 * - Index key comparison using bson_compare_docs
 */

#include "mongolite_internal.h"
#include "key_compare.h"
#include "macros.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define MONGOLITE_LIB "mongolite"

/* ============================================================
 * Index Name Generation
 *
 * Generates a default index name from key specification.
 * Format: field1_dir1_field2_dir2_...
 * Examples:
 *   {"email": 1}           -> "email_1"
 *   {"name": 1, "age": -1} -> "name_1_age_-1"
 *   {"a.b.c": 1}           -> "a.b.c_1"
 * ============================================================ */

char* _index_name_from_spec(const bson_t *keys) {
    if (!keys) return NULL;

    bson_iter_t iter;
    if (!bson_iter_init(&iter, keys)) {
        return NULL;
    }

    /* Calculate required length */
    size_t total_len = 0;
    int field_count = 0;

    while (bson_iter_next(&iter)) {
        const char *field = bson_iter_key(&iter);
        total_len += strlen(field);
        total_len += 3;  /* "_1" or "_-1" + separator "_" */
        field_count++;
    }

    if (field_count == 0) {
        return NULL;
    }

    /* Allocate buffer */
    char *name = malloc(total_len + 1);
    if (!name) return NULL;

    /* Build name */
    char *p = name;
    bson_iter_init(&iter, keys);

    while (bson_iter_next(&iter)) {
        const char *field = bson_iter_key(&iter);
        int direction = 1;

        /* Get direction from value */
        if (BSON_ITER_HOLDS_INT32(&iter)) {
            direction = bson_iter_int32(&iter);
        } else if (BSON_ITER_HOLDS_INT64(&iter)) {
            direction = (int)bson_iter_int64(&iter);
        } else if (BSON_ITER_HOLDS_DOUBLE(&iter)) {
            direction = (int)bson_iter_double(&iter);
        }

        /* Add separator between fields */
        if (p != name) {
            *p++ = '_';
        }

        /* Add field name */
        size_t field_len = strlen(field);
        memcpy(p, field, field_len);
        p += field_len;

        /* Add direction */
        if (direction >= 0) {
            *p++ = '_';
            *p++ = '1';
        } else {
            *p++ = '_';
            *p++ = '-';
            *p++ = '1';
        }
    }

    *p = '\0';
    return name;
}

/* ============================================================
 * Index Key Building
 *
 * Creates an index key from a document.
 * With MDB_DUPSORT, the key contains only the indexed fields.
 * The document _id is stored as the value (for duplicate handling).
 *
 * For non-unique indexes, multiple documents can have the same key,
 * stored as multiple values under one key (via MDB_DUPSORT).
 *
 * The include_id parameter is kept for backward compatibility but
 * is now ignored - keys never include _id.
 * ============================================================ */

bson_t* _build_index_key(const bson_t *doc, const bson_t *keys, bool include_id) {
    (void)include_id;  /* No longer used with DUPSORT - kept for API compat */

    if (!doc || !keys) return NULL;

    /* Extract fields according to index spec - no _id appended */
    return bson_extract_index_key(doc, keys);
}

/* ============================================================
 * Index Key Comparison
 *
 * Compares two serialized index keys for tree ordering.
 * Uses bson_compare_docs for MongoDB-compatible ordering.
 *
 * This function is designed to be used as a wtree comparator.
 * ============================================================ */

int _index_key_compare(const void *key1, size_t key1_len,
                       const void *key2, size_t key2_len,
                       void *user_data) {
    (void)user_data;  /* Reserved for future use (e.g., sort direction) */

    /* Parse BSON keys */
    bson_t b1, b2;

    if (!bson_init_static(&b1, (const uint8_t*)key1, key1_len)) {
        return -1;  /* Invalid key1 sorts first */
    }

    if (!bson_init_static(&b2, (const uint8_t*)key2, key2_len)) {
        return 1;   /* Invalid key2 sorts last */
    }

    return bson_compare_docs(&b1, &b2);
}

/* ============================================================
 * Unique Key Check
 *
 * For unique indexes, we need to check if a key already exists.
 * With DUPSORT, the key never includes _id, so this is just
 * building the standard index key.
 * ============================================================ */

inline bson_t* _build_unique_check_key(const bson_t *doc, const bson_t *keys) {
    return _build_index_key(doc, keys, false);
}

/* ============================================================
 * Index Key Serialization Helpers
 *
 * For storage in wtree, we serialize the BSON key.
 * ============================================================ */

uint8_t* _index_key_serialize(const bson_t *key, size_t *out_len) {
    if (!key || !out_len) return NULL;

    uint32_t len = key->len;
    uint8_t *data = malloc(len);
    if (!data) return NULL;

    memcpy(data, bson_get_data(key), len);
    *out_len = len;
    return data;
}

bson_t* _index_key_deserialize(const uint8_t *data, size_t len) {
    if (!data || len == 0) return NULL;

    bson_t *key = bson_new_from_data(data, len);
    return key;
}

/* ============================================================
 * Index Value (Document ID) Serialization
 *
 * Index entries store just the _id value as the tree value.
 * This allows us to fetch the full document from the collection.
 * ============================================================ */

uint8_t* _index_value_from_doc(const bson_t *doc, size_t *out_len) {
    if (!doc || !out_len) return NULL;

    bson_iter_t iter;
    if (!bson_iter_init_find(&iter, doc, "_id")) {
        return NULL;  /* Document has no _id */
    }

    /* Optimization: For OID type (most common), store just the 12 bytes */
    if (BSON_ITER_HOLDS_OID(&iter)) {
        const bson_oid_t *oid = bson_iter_oid(&iter);
        *out_len = sizeof(bson_oid_t);
        uint8_t *data = malloc(*out_len);
        if (data) {
            memcpy(data, oid->bytes, *out_len);
        }
        return data;
    }

    /* Fallback for non-OID _id: Create a small BSON with just _id */
    bson_t *id_doc = bson_new();
    if (!id_doc) return NULL;

    const bson_value_t *value = bson_iter_value(&iter);
    bson_append_value(id_doc, "_id", 3, value);

    *out_len = id_doc->len;
    uint8_t *data = malloc(*out_len);
    if (data) {
        memcpy(data, bson_get_data(id_doc), *out_len);
    }

    bson_destroy(id_doc);
    return data;
}

/* ============================================================
 * Extract _id from Index Value
 *
 * Given an index value (BSON with _id), extract the _id
 * for fetching the full document.
 * ============================================================ */

bool _index_value_get_oid(const uint8_t *data, size_t len, bson_oid_t *out_oid) {
    if (!data || !out_oid || len == 0) return false;

    /* Optimization: Check for raw OID format (12 bytes) first */
    if (len == sizeof(bson_oid_t)) {
        memcpy(out_oid->bytes, data, sizeof(bson_oid_t));
        return true;
    }

    /* Fallback: Parse as BSON document with _id field */
    bson_t doc;
    if (!bson_init_static(&doc, data, len)) {
        return false;
    }

    bson_iter_t iter;
    if (!bson_iter_init_find(&iter, &doc, "_id")) {
        return false;
    }

    if (!BSON_ITER_HOLDS_OID(&iter)) {
        return false;  /* Non-OID _id not supported yet */
    }

    const bson_oid_t *oid = bson_iter_oid(&iter);
    memcpy(out_oid, oid, sizeof(bson_oid_t));
    return true;
}

/* ============================================================
 * Index Metadata Helpers
 *
 * These functions help manage index metadata in the schema.
 * ============================================================ */

bson_t* _index_spec_to_bson(const char *name, const bson_t *keys,
                            const index_config_t *config) {
    if (!name || !keys) return NULL;

    bson_t *spec = bson_new();
    if (!spec) return NULL;

    bson_append_utf8(spec, "name", 4, name, -1);

    /* Append key specification */
    bson_append_document(spec, "key", 3, keys);

    /* Append options */
    if (config) {
        if (config->unique) {
            bson_append_bool(spec, "unique", 6, true);
        }
        if (config->sparse) {
            bson_append_bool(spec, "sparse", 6, true);
        }
        if (config->expire_after_seconds > 0) {
            bson_append_int64(spec, "expireAfterSeconds", 18, config->expire_after_seconds);
        }
    }

    return spec;
}

/* Parse index spec from schema */
int _index_spec_from_bson(const bson_t *spec, char **out_name,
                          bson_t **out_keys, index_config_t *out_config) {
    if (!spec) return MONGOLITE_EINVAL;

    bson_iter_t iter;

    /* Get name */
    if (out_name) {
        *out_name = NULL;
        if (bson_iter_init_find(&iter, spec, "name") && BSON_ITER_HOLDS_UTF8(&iter)) {
            *out_name = strdup(bson_iter_utf8(&iter, NULL));
        }
    }

    /* Get keys */
    if (out_keys) {
        *out_keys = NULL;
        if (bson_iter_init_find(&iter, spec, "key") && BSON_ITER_HOLDS_DOCUMENT(&iter)) {
            uint32_t len;
            const uint8_t *data;
            bson_iter_document(&iter, &len, &data);
            *out_keys = bson_new_from_data(data, len);
        }
    }

    /* Get config options */
    if (out_config) {
        memset(out_config, 0, sizeof(index_config_t));

        if (bson_iter_init_find(&iter, spec, "unique") && BSON_ITER_HOLDS_BOOL(&iter)) {
            out_config->unique = bson_iter_bool(&iter);
        }
        if (bson_iter_init_find(&iter, spec, "sparse") && BSON_ITER_HOLDS_BOOL(&iter)) {
            out_config->sparse = bson_iter_bool(&iter);
        }
        if (bson_iter_init_find(&iter, spec, "expireAfterSeconds") && BSON_ITER_HOLDS_INT64(&iter)) {
            out_config->expire_after_seconds = bson_iter_int64(&iter);
        }
    }

    return MONGOLITE_OK;
}

/* ============================================================
 * Check if Document Should Be Indexed (sparse index handling)
 *
 * For sparse indexes, skip documents where indexed field is
 * missing or null.
 * ============================================================ */

bool _should_index_document(const bson_t *doc, const bson_t *keys, bool sparse) {
    if (!doc || !keys) return false;

    /* Non-sparse indexes: always index */
    if (!sparse) return true;

    /* Sparse indexes: check if any indexed field exists and is non-null */
    bson_iter_t keys_iter;
    if (!bson_iter_init(&keys_iter, keys)) return false;

    while (bson_iter_next(&keys_iter)) {
        const char *field = bson_iter_key(&keys_iter);
        bson_iter_t doc_iter, descendant;

        bool found = false;
        if (bson_iter_init_find(&doc_iter, doc, field)) {
            found = true;
        } else if (strchr(field, '.') != NULL) {
            if (bson_iter_init(&doc_iter, doc) &&
                bson_iter_find_descendant(&doc_iter, field, &descendant)) {
                doc_iter = descendant;
                found = true;
            }
        }

        if (found && !BSON_ITER_HOLDS_NULL(&doc_iter)) {
            return true;  /* At least one non-null indexed field exists */
        }
    }

    return false;  /* All indexed fields are missing or null */
}

/* ============================================================
 * Phase 2: Index Creation and Deletion
 * ============================================================ */

/*
 * LMDB comparator wrapper for index keys.
 * MDB_cmp_func signature: int (*)(const MDB_val *a, const MDB_val *b)
 * Made non-static so it can be used when loading cached indexes.
 */
int _mongolite_index_compare(const MDB_val *a, const MDB_val *b) {
    return _index_key_compare(a->mv_data, a->mv_size, b->mv_data, b->mv_size, NULL);
}

/*
 * Check if an index with given name already exists on collection
 */
static bool _index_exists(const bson_t *indexes, const char *name) {
    if (!indexes || !name) return false;

    bson_iter_t iter;
    if (!bson_iter_init(&iter, indexes)) return false;

    while (bson_iter_next(&iter)) {
        if (BSON_ITER_HOLDS_DOCUMENT(&iter)) {
            bson_iter_t child;
            if (bson_iter_recurse(&iter, &child) &&
                bson_iter_find(&child, "name") &&
                BSON_ITER_HOLDS_UTF8(&child)) {
                if (strcmp(bson_iter_utf8(&child, NULL), name) == 0) {
                    return true;
                }
            }
        }
    }
    return false;
}

/*
 * Add index spec to collection's indexes array
 */
static bson_t* _add_index_to_array(const bson_t *existing, const bson_t *new_spec) {
    bson_t *result = bson_new();
    if (!result) return NULL;

    int idx = 0;
    char idx_str[16];

    /* Copy existing indexes */
    if (existing) {
        bson_iter_t iter;
        if (bson_iter_init(&iter, existing)) {
            while (bson_iter_next(&iter)) {
                if (BSON_ITER_HOLDS_DOCUMENT(&iter)) {
                    uint32_t len;
                    const uint8_t *data;
                    bson_iter_document(&iter, &len, &data);
                    bson_t child;
                    bson_init_static(&child, data, len);

                    snprintf(idx_str, sizeof(idx_str), "%d", idx++);
                    bson_append_document(result, idx_str, -1, &child);
                }
            }
        }
    }

    /* Add new index */
    snprintf(idx_str, sizeof(idx_str), "%d", idx);
    bson_append_document(result, idx_str, -1, new_spec);

    return result;
}

/*
 * Remove index from collection's indexes array by name
 */
static bson_t* _remove_index_from_array(const bson_t *existing, const char *name) {
    if (!existing || !name) return NULL;

    bson_t *result = bson_new();
    if (!result) return NULL;

    int idx = 0;
    char idx_str[16];

    bson_iter_t iter;
    if (bson_iter_init(&iter, existing)) {
        while (bson_iter_next(&iter)) {
            if (BSON_ITER_HOLDS_DOCUMENT(&iter)) {
                uint32_t len;
                const uint8_t *data;
                bson_iter_document(&iter, &len, &data);
                bson_t child;
                bson_init_static(&child, data, len);

                /* Check if this is the index to remove */
                bson_iter_t name_iter;
                if (bson_iter_init_find(&name_iter, &child, "name") &&
                    BSON_ITER_HOLDS_UTF8(&name_iter)) {
                    if (strcmp(bson_iter_utf8(&name_iter, NULL), name) == 0) {
                        continue;  /* Skip this index */
                    }
                }

                snprintf(idx_str, sizeof(idx_str), "%d", idx++);
                bson_append_document(result, idx_str, -1, &child);
            }
        }
    }

    return result;
}

/*
 * Helper: Populate index tree from existing documents in collection
 *
 * Scans all documents and inserts them into the index.
 * With DUPSORT: key = indexed fields, value = document _id (raw OID)
 *
 * Returns: MONGOLITE_OK on success, error code on failure
 */
static int _populate_index_from_collection(
    mongolite_db_t *db,
    wtree_tree_t *col_tree,
    wtree_tree_t *index_tree,
    const bson_t *keys,
    bool is_unique,
    bool is_sparse,
    const char *index_name,
    gerror_t *error)
{
    wtree_txn_t *txn = wtree_txn_begin(db->wdb, true, error);
    if (!txn) {
        return MONGOLITE_ERROR;
    }

    wtree_iterator_t *iter = wtree_iterator_create_with_txn(col_tree, txn, error);
    if (!iter) {
        wtree_txn_abort(txn);
        return MONGOLITE_ERROR;
    }

    int rc = MONGOLITE_OK;

    if (wtree_iterator_first(iter)) {
        do {
            const void *doc_data;
            size_t doc_len;
            if (!wtree_iterator_value(iter, &doc_data, &doc_len)) continue;

            bson_t doc;
            if (!bson_init_static(&doc, doc_data, doc_len)) continue;

            /* Check if document should be indexed (sparse handling) */
            if (!_should_index_document(&doc, keys, is_sparse)) continue;

            /* Build index key (just indexed fields, no _id - DUPSORT handles duplicates) */
            bson_t *idx_key = _build_index_key(&doc, keys, false);
            if (!idx_key) continue;

            /* For unique indexes, check if key already exists */
            if (is_unique) {
                const void *existing_val;
                size_t existing_len;
                int get_rc = wtree_get_txn(txn, index_tree,
                                           bson_get_data(idx_key), idx_key->len,
                                           &existing_val, &existing_len, NULL);
                if (get_rc == 0) {
                    /* Key exists - duplicate violation */
                    bson_destroy(idx_key);
                    wtree_iterator_close(iter);
                    wtree_txn_abort(txn);
                    set_error(error, MONGOLITE_LIB, MONGOLITE_EINDEX,
                             "Duplicate key violation for unique index '%s'", index_name);
                    return MONGOLITE_EINDEX;
                }
            }

            /* Build index value (just the _id for document lookup) */
            size_t value_len;
            uint8_t *value_data = _index_value_from_doc(&doc, &value_len);
            if (!value_data) {
                bson_destroy(idx_key);
                continue;
            }

            /* Insert into index tree (DUPSORT allows multiple values per key) */
            rc = wtree_insert_one_txn(txn, index_tree,
                                      bson_get_data(idx_key), idx_key->len,
                                      value_data, value_len, error);

            free(value_data);
            bson_destroy(idx_key);

            if (rc != 0) {
                wtree_iterator_close(iter);
                wtree_txn_abort(txn);
                return rc;
            }
        } while (wtree_iterator_next(iter));
    }

    wtree_iterator_close(iter);

    /* Commit the index population */
    rc = wtree_txn_commit(txn, error);
    return rc;
}

/*
 * Helper: Cleanup index creation resources on failure
 */
static void _cleanup_failed_index(mongolite_db_t *db, wtree_tree_t *tree, const char *tree_name) {
    if (tree) {
        wtree_tree_close(tree);
    }
    if (tree_name) {
        wtree_tree_delete(db->wdb, tree_name, NULL);
    }
}

/*
 * mongolite_create_index - Create an index on a collection
 *
 * Steps:
 * 1. Validate parameters
 * 2. Generate index name if not provided
 * 3. Check collection exists and index doesn't exist
 * 4. Create index tree with custom comparator
 * 5. Scan all documents and populate index
 * 6. Update collection schema with new index
 */
int mongolite_create_index(mongolite_db_t *db, const char *collection,
                           const bson_t *keys, const char *name,
                           index_config_t *config, gerror_t *error) {
    VALIDATE_PARAMS(db && collection && keys, error,
                   "Database, collection and keys are required", MONGOLITE_EINVAL);

    /* Check keys is not empty */
    if (bson_empty(keys)) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "Index keys cannot be empty");
        return MONGOLITE_EINVAL;
    }

    int rc = MONGOLITE_OK;
    char *index_name = NULL;
    char *tree_name = NULL;
    wtree_tree_t *index_tree = NULL;
    bson_t *index_spec = NULL;
    bson_t *new_indexes = NULL;

    _mongolite_lock(db);

    /* Generate index name if not provided */
    if (name && strlen(name) > 0) {
        index_name = strdup(name);
    } else {
        index_name = _index_name_from_spec(keys);
    }
    if (!index_name) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM,
                 "Failed to generate index name");
        rc = MONGOLITE_ENOMEM;
        goto cleanup;
    }

    /* Get collection schema */
    mongolite_schema_entry_t col_entry = {0};
    rc = _mongolite_schema_get(db, collection, &col_entry, error);
    if (rc != 0) {
        goto cleanup;
    }

    /* Verify it's a collection */
    if (!col_entry.type || strcmp(col_entry.type, SCHEMA_TYPE_COLLECTION) != 0) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "'%s' is not a collection", collection);
        rc = MONGOLITE_EINVAL;
        _mongolite_schema_entry_free(&col_entry);
        goto cleanup;
    }

    /* Check if index already exists */
    if (_index_exists(col_entry.indexes, index_name)) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EEXISTS,
                 "Index '%s' already exists on collection '%s'", index_name, collection);
        rc = MONGOLITE_EEXISTS;
        _mongolite_schema_entry_free(&col_entry);
        goto cleanup;
    }

    /* Create tree name: idx:<collection>:<index_name> */
    tree_name = _mongolite_index_tree_name(collection, index_name);
    if (!tree_name) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM,
                 "Failed to create tree name");
        rc = MONGOLITE_ENOMEM;
        _mongolite_schema_entry_free(&col_entry);
        goto cleanup;
    }

    /* Create the index tree with MDB_DUPSORT for efficient multi-value handling
     * Key: extracted index fields (BSON)
     * Value: document _id (raw OID bytes - 12 bytes)
     * Multiple documents with same index key = multiple values under one key
     */
    index_tree = wtree_tree_create(db->wdb, tree_name, MDB_DUPSORT, error);
    if (!index_tree) {
        rc = MONGOLITE_ERROR;
        _mongolite_schema_entry_free(&col_entry);
        goto cleanup;
    }

    /* Set custom comparator for MongoDB-style key ordering */
    rc = wtree_tree_set_compare(index_tree, _mongolite_index_compare, error);
    if (rc != 0) {
        _cleanup_failed_index(db, index_tree, tree_name);
        _mongolite_schema_entry_free(&col_entry);
        goto cleanup;
    }

    /* Note: MDB_DUPSORT uses default memcmp for values, which is perfect for
     * fixed 12-byte OIDs. No custom dupsort comparator needed. */

    /* Get collection tree to scan documents */
    wtree_tree_t *col_tree = _mongolite_get_collection_tree(db, collection, error);
    if (!col_tree) {
        _cleanup_failed_index(db, index_tree, tree_name);
        rc = MONGOLITE_ERROR;
        _mongolite_schema_entry_free(&col_entry);
        goto cleanup;
    }

    /* Populate index from existing documents */
    bool is_unique = config && config->unique;
    bool is_sparse = config && config->sparse;

    rc = _populate_index_from_collection(db, col_tree, index_tree, keys,
                                          is_unique, is_sparse, index_name, error);
    if (rc != 0) {
        _cleanup_failed_index(db, index_tree, tree_name);
        _mongolite_schema_entry_free(&col_entry);
        goto cleanup;
    }

    /* Create index spec for schema */
    index_spec = _index_spec_to_bson(index_name, keys, config);
    if (!index_spec) {
        _cleanup_failed_index(db, index_tree, tree_name);
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM,
                 "Failed to create index spec");
        rc = MONGOLITE_ENOMEM;
        _mongolite_schema_entry_free(&col_entry);
        goto cleanup;
    }

    /* Add index to collection's indexes array */
    new_indexes = _add_index_to_array(col_entry.indexes, index_spec);
    if (!new_indexes) {
        _cleanup_failed_index(db, index_tree, tree_name);
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM,
                 "Failed to update indexes array");
        rc = MONGOLITE_ENOMEM;
        _mongolite_schema_entry_free(&col_entry);
        goto cleanup;
    }

    /* Update collection schema */
    if (col_entry.indexes) {
        bson_destroy(col_entry.indexes);
    }
    col_entry.indexes = new_indexes;
    new_indexes = NULL;  /* Ownership transferred */
    col_entry.modified_at = _mongolite_now_ms();

    rc = _mongolite_schema_put(db, &col_entry, error);
    if (rc != 0) {
        _cleanup_failed_index(db, index_tree, tree_name);
        _mongolite_schema_entry_free(&col_entry);
        goto cleanup;
    }

    _mongolite_schema_entry_free(&col_entry);

    /* Close the index tree handle (can be reopened later) */
    wtree_tree_close(index_tree);

    /* Invalidate index cache so it gets reloaded with new index */
    _mongolite_invalidate_index_cache(db, collection);

    rc = MONGOLITE_OK;

cleanup:
    free(index_name);
    free(tree_name);
    if (index_spec) bson_destroy(index_spec);
    if (new_indexes) bson_destroy(new_indexes);
    _mongolite_unlock(db);
    return rc;
}

/*
 * mongolite_drop_index - Drop an index from a collection
 *
 * Steps:
 * 1. Validate parameters
 * 2. Check collection exists and index exists
 * 3. Prevent dropping _id index
 * 4. Delete index tree
 * 5. Update collection schema
 */
int mongolite_drop_index(mongolite_db_t *db, const char *collection,
                         const char *index_name, gerror_t *error) {
    VALIDATE_PARAMS(db && collection && index_name, error,
                   "Database, collection and index_name are required", MONGOLITE_EINVAL);

    /* Prevent dropping _id index */
    if (strcmp(index_name, "_id_") == 0) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "Cannot drop the _id index");
        return MONGOLITE_EINVAL;
    }

    int rc = MONGOLITE_OK;
    char *tree_name = NULL;
    bson_t *new_indexes = NULL;

    _mongolite_lock(db);

    /* Get collection schema */
    mongolite_schema_entry_t col_entry = {0};
    rc = _mongolite_schema_get(db, collection, &col_entry, error);
    if (rc != 0) {
        _mongolite_unlock(db);
        return rc;
    }

    /* Verify it's a collection */
    if (!col_entry.type || strcmp(col_entry.type, SCHEMA_TYPE_COLLECTION) != 0) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "'%s' is not a collection", collection);
        _mongolite_schema_entry_free(&col_entry);
        _mongolite_unlock(db);
        return MONGOLITE_EINVAL;
    }

    /* Check if index exists */
    if (!_index_exists(col_entry.indexes, index_name)) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOTFOUND,
                 "Index '%s' not found on collection '%s'", index_name, collection);
        _mongolite_schema_entry_free(&col_entry);
        _mongolite_unlock(db);
        return MONGOLITE_ENOTFOUND;
    }

    /* Build tree name */
    tree_name = _mongolite_index_tree_name(collection, index_name);
    if (!tree_name) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM,
                 "Failed to create tree name");
        _mongolite_schema_entry_free(&col_entry);
        _mongolite_unlock(db);
        return MONGOLITE_ENOMEM;
    }

    /* Remove index from tree cache if cached */
    _mongolite_tree_cache_remove(db, tree_name);

    /* Delete the index tree */
    rc = wtree_tree_delete(db->wdb, tree_name, error);
    if (rc != 0 && rc != WTREE_KEY_NOT_FOUND) {
        free(tree_name);
        _mongolite_schema_entry_free(&col_entry);
        _mongolite_unlock(db);
        return rc;
    }

    /* Remove index from collection's indexes array */
    new_indexes = _remove_index_from_array(col_entry.indexes, index_name);
    if (!new_indexes) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM,
                 "Failed to update indexes array");
        free(tree_name);
        _mongolite_schema_entry_free(&col_entry);
        _mongolite_unlock(db);
        return MONGOLITE_ENOMEM;
    }

    /* Update collection schema */
    if (col_entry.indexes) {
        bson_destroy(col_entry.indexes);
    }
    col_entry.indexes = new_indexes;
    col_entry.modified_at = _mongolite_now_ms();

    rc = _mongolite_schema_put(db, &col_entry, error);

    /* Invalidate index cache so it gets reloaded without dropped index */
    _mongolite_invalidate_index_cache(db, collection);

    free(tree_name);
    _mongolite_schema_entry_free(&col_entry);
    _mongolite_unlock(db);

    return rc;
}

/* ============================================================
 * Phase 3: Index Maintenance on CRUD
 * ============================================================ */

/*
 * _mongolite_index_insert - Maintain indexes after document insert
 *
 * Called within an existing transaction after document is inserted.
 * For each index on the collection:
 *   1. Check if document should be indexed (sparse handling)
 *   2. Check unique constraint if applicable (just key existence with DUPSORT)
 *   3. Insert index entry
 *
 * With DUPSORT:
 * - Key = extracted index fields (no _id)
 * - Value = document _id (raw 12-byte OID)
 * - Multiple docs with same key = multiple values under one key
 */
int _mongolite_index_insert(mongolite_db_t *db, wtree_txn_t *txn,
                             const char *collection, const bson_t *doc,
                             gerror_t *error) {
    if (!db || !txn || !collection || !doc) {
        return MONGOLITE_OK;  /* Nothing to do */
    }

    /* Get cached indexes for this collection */
    size_t index_count = 0;
    mongolite_cached_index_t *indexes = _mongolite_get_cached_indexes(db, collection, &index_count, error);

    if (index_count == 0 || !indexes) {
        return MONGOLITE_OK;  /* No secondary indexes */
    }

    /* OPTIMIZATION: Pre-extract _id once for all indexes */
    bson_iter_t id_iter;
    if (MONGOLITE_UNLIKELY(!bson_iter_init_find(&id_iter, doc, "_id"))) {
        return MONGOLITE_OK;  /* No _id, can't index */
    }

    /* For OID type, prepare raw bytes (most common case) */
    bool is_oid = BSON_ITER_HOLDS_OID(&id_iter);
    const bson_oid_t *doc_oid = is_oid ? bson_iter_oid(&id_iter) : NULL;

    int rc = MONGOLITE_OK;

    for (size_t i = 0; i < index_count; i++) {
        mongolite_cached_index_t *idx = &indexes[i];

        if (!idx->tree || !idx->keys) continue;

        /* Check if document should be indexed (sparse handling) */
        if (!_should_index_document(doc, idx->keys, idx->sparse)) {
            continue;
        }

        /* Build index key (just indexed fields - DUPSORT handles duplicates via value) */
        bson_t *idx_key = bson_extract_index_key(doc, idx->keys);
        if (!idx_key) continue;

        /* For unique indexes, check if key already exists */
        if (idx->unique) {
            const void *existing_val;
            size_t existing_len;
            int get_rc = wtree_get_txn(txn, idx->tree,
                                       bson_get_data(idx_key), idx_key->len,
                                       &existing_val, &existing_len, NULL);
            if (get_rc == 0) {
                /* Key exists - duplicate violation */
                bson_destroy(idx_key);
                set_error(error, MONGOLITE_LIB, MONGOLITE_EINDEX,
                         "Duplicate key error on index '%s'", idx->name);
                return MONGOLITE_EINDEX;
            }
        }

        /* OPTIMIZATION: Use raw OID bytes directly if possible */
        if (is_oid) {
            rc = wtree_insert_one_txn(txn, idx->tree,
                                      bson_get_data(idx_key), idx_key->len,
                                      doc_oid->bytes, sizeof(bson_oid_t), error);
        } else {
            /* Fallback for non-OID _id */
            size_t value_len;
            uint8_t *value_data = _index_value_from_doc(doc, &value_len);
            if (value_data) {
                rc = wtree_insert_one_txn(txn, idx->tree,
                                          bson_get_data(idx_key), idx_key->len,
                                          value_data, value_len, error);
                free(value_data);
            }
        }

        bson_destroy(idx_key);

        if (rc != 0) {
            return rc;
        }
    }

    return MONGOLITE_OK;
}

/*
 * _mongolite_index_delete - Maintain indexes after document delete
 *
 * Called within an existing transaction before/after document is deleted.
 * With DUPSORT, we need to delete the specific key+value pair where
 * value = document _id (raw OID bytes).
 */
int _mongolite_index_delete(mongolite_db_t *db, wtree_txn_t *txn,
                             const char *collection, const bson_t *doc,
                             gerror_t *error) {
    if (!db || !txn || !collection || !doc) {
        return MONGOLITE_OK;  /* Nothing to do */
    }

    /* Get cached indexes for this collection */
    size_t index_count = 0;
    mongolite_cached_index_t *indexes = _mongolite_get_cached_indexes(db, collection, &index_count, error);

    if (index_count == 0 || !indexes) {
        return MONGOLITE_OK;  /* No secondary indexes */
    }

    /* Pre-extract _id for deletion value */
    bson_iter_t id_iter;
    if (!bson_iter_init_find(&id_iter, doc, "_id")) {
        return MONGOLITE_OK;  /* No _id, can't have been indexed */
    }

    bool is_oid = BSON_ITER_HOLDS_OID(&id_iter);
    const bson_oid_t *doc_oid = is_oid ? bson_iter_oid(&id_iter) : NULL;

    int rc = MONGOLITE_OK;

    for (size_t i = 0; i < index_count; i++) {
        mongolite_cached_index_t *idx = &indexes[i];

        if (!idx->tree || !idx->keys) continue;

        /* Check if document was indexed (sparse handling) */
        if (!_should_index_document(doc, idx->keys, idx->sparse)) {
            continue;
        }

        /* Build index key (just indexed fields - no _id with DUPSORT) */
        bson_t *idx_key = _build_index_key(doc, idx->keys, false);
        if (!idx_key) continue;

        /* Delete specific key+value pair from DUPSORT tree */
        bool deleted = false;
        if (is_oid) {
            rc = wtree_delete_dup_txn(txn, idx->tree,
                                       bson_get_data(idx_key), idx_key->len,
                                       doc_oid->bytes, sizeof(bson_oid_t),
                                       &deleted, error);
        } else {
            /* Fallback for non-OID _id */
            size_t value_len;
            uint8_t *value_data = _index_value_from_doc(doc, &value_len);
            if (value_data) {
                rc = wtree_delete_dup_txn(txn, idx->tree,
                                           bson_get_data(idx_key), idx_key->len,
                                           value_data, value_len,
                                           &deleted, error);
                free(value_data);
            }
        }

        bson_destroy(idx_key);

        if (rc != 0) {
            return rc;
        }
        /* Note: We don't fail if pair wasn't found - document might not have been indexed */
    }

    return MONGOLITE_OK;
}

/*
 * _mongolite_index_update - Maintain indexes after document update
 *
 * Called within an existing transaction.
 * With DUPSORT, we delete the old key+value pair and insert the new one.
 * Key = indexed fields, Value = document _id (raw OID).
 */
int _mongolite_index_update(mongolite_db_t *db, wtree_txn_t *txn,
                             const char *collection,
                             const bson_t *old_doc, const bson_t *new_doc,
                             gerror_t *error) {
    if (!db || !txn || !collection || !old_doc || !new_doc) {
        return MONGOLITE_OK;  /* Nothing to do */
    }

    /* Get cached indexes for this collection */
    size_t index_count = 0;
    mongolite_cached_index_t *indexes = _mongolite_get_cached_indexes(db, collection, &index_count, error);

    if (index_count == 0 || !indexes) {
        return MONGOLITE_OK;  /* No secondary indexes */
    }

    /* Pre-extract _id from old doc (for deletion) */
    bson_iter_t old_id_iter;
    bool old_has_id = bson_iter_init_find(&old_id_iter, old_doc, "_id");
    bool old_is_oid = old_has_id && BSON_ITER_HOLDS_OID(&old_id_iter);
    const bson_oid_t *old_oid = old_is_oid ? bson_iter_oid(&old_id_iter) : NULL;

    /* Pre-extract _id from new doc (for insertion) */
    bson_iter_t new_id_iter;
    bool new_has_id = bson_iter_init_find(&new_id_iter, new_doc, "_id");
    bool new_is_oid = new_has_id && BSON_ITER_HOLDS_OID(&new_id_iter);
    const bson_oid_t *new_oid = new_is_oid ? bson_iter_oid(&new_id_iter) : NULL;

    int rc = MONGOLITE_OK;

    for (size_t i = 0; i < index_count; i++) {
        mongolite_cached_index_t *idx = &indexes[i];

        if (!idx->tree || !idx->keys) continue;

        bool old_indexed = _should_index_document(old_doc, idx->keys, idx->sparse);
        bool new_indexed = _should_index_document(new_doc, idx->keys, idx->sparse);

        /* Build keys for comparison (no _id with DUPSORT) */
        bson_t *old_key = old_indexed ? _build_index_key(old_doc, idx->keys, false) : NULL;
        bson_t *new_key = new_indexed ? _build_index_key(new_doc, idx->keys, false) : NULL;

        /* Check if keys are the same (no update needed) */
        bool keys_equal = false;
        if (old_key && new_key) {
            keys_equal = (bson_compare(old_key, new_key) == 0);
        }

        if (keys_equal) {
            /* No change in index key */
            if (old_key) bson_destroy(old_key);
            if (new_key) bson_destroy(new_key);
            continue;
        }

        /* Delete old entry if it existed */
        if (old_key && old_has_id) {
            bool deleted = false;
            if (old_is_oid) {
                rc = wtree_delete_dup_txn(txn, idx->tree,
                                           bson_get_data(old_key), old_key->len,
                                           old_oid->bytes, sizeof(bson_oid_t),
                                           &deleted, error);
            } else {
                size_t value_len;
                uint8_t *value_data = _index_value_from_doc(old_doc, &value_len);
                if (value_data) {
                    rc = wtree_delete_dup_txn(txn, idx->tree,
                                               bson_get_data(old_key), old_key->len,
                                               value_data, value_len,
                                               &deleted, error);
                    free(value_data);
                }
            }
            bson_destroy(old_key);
            if (rc != 0) {
                if (new_key) bson_destroy(new_key);
                return rc;
            }
        } else if (old_key) {
            bson_destroy(old_key);
        }

        /* Insert new entry if document should be indexed */
        if (new_key && new_has_id) {
            /* Check unique constraint */
            if (idx->unique) {
                const void *existing_val;
                size_t existing_len;
                int get_rc = wtree_get_txn(txn, idx->tree,
                                           bson_get_data(new_key), new_key->len,
                                           &existing_val, &existing_len, NULL);
                if (get_rc == 0) {
                    /* Key exists - duplicate violation */
                    bson_destroy(new_key);
                    set_error(error, MONGOLITE_LIB, MONGOLITE_EINDEX,
                             "Duplicate key error on index '%s'", idx->name);
                    return MONGOLITE_EINDEX;
                }
            }

            /* Insert with raw OID if possible */
            if (new_is_oid) {
                rc = wtree_insert_one_txn(txn, idx->tree,
                                          bson_get_data(new_key), new_key->len,
                                          new_oid->bytes, sizeof(bson_oid_t), error);
            } else {
                size_t value_len;
                uint8_t *value_data = _index_value_from_doc(new_doc, &value_len);
                if (value_data) {
                    rc = wtree_insert_one_txn(txn, idx->tree,
                                              bson_get_data(new_key), new_key->len,
                                              value_data, value_len, error);
                    free(value_data);
                }
            }

            bson_destroy(new_key);

            if (rc != 0) {
                return rc;
            }
        } else if (new_key) {
            bson_destroy(new_key);
        }
    }

    return MONGOLITE_OK;
}

/* ============================================================
 * Phase 4: Query Optimization
 * ============================================================ */

/*
 * _analyze_query_for_index - Analyze a query filter for index usage
 *
 * Identifies simple equality conditions that can use an index.
 * Currently supports:
 *   - Simple field equality: {"field": value}
 *   - Multiple equality conditions: {"a": 1, "b": 2}
 *
 * Does NOT support (yet):
 *   - Operators like $gt, $lt, $in, etc.
 *   - Nested documents
 *   - Array queries
 */
query_analysis_t* _analyze_query_for_index(const bson_t *filter) {
    if (!filter || bson_empty(filter)) {
        return NULL;
    }

    query_analysis_t *analysis = calloc(1, sizeof(query_analysis_t));
    if (!analysis) return NULL;

    /* Count fields first */
    size_t count = 0;
    bson_iter_t iter;
    if (bson_iter_init(&iter, filter)) {
        while (bson_iter_next(&iter)) {
            const char *key = bson_iter_key(&iter);
            /* Skip _id - already has dedicated optimization */
            if (strcmp(key, "_id") == 0) continue;

            /* Check if this is a simple value (not an operator or document) */
            bson_type_t type = bson_iter_type(&iter);
            if (type == BSON_TYPE_DOCUMENT) {
                /* Could be an operator like {$gt: 5} - check first key */
                bson_iter_t child;
                if (bson_iter_recurse(&iter, &child) && bson_iter_next(&child)) {
                    const char *child_key = bson_iter_key(&child);
                    if (child_key[0] == '$') {
                        /* It's an operator - not simple equality */
                        continue;
                    }
                }
                /* Nested document equality - skip for now */
                continue;
            }
            count++;
        }
    }

    if (count == 0) {
        free(analysis);
        return NULL;
    }

    /* Allocate fields array */
    analysis->equality_fields = calloc(count, sizeof(char*));
    if (!analysis->equality_fields) {
        free(analysis);
        return NULL;
    }

    /* Populate fields */
    if (bson_iter_init(&iter, filter)) {
        while (bson_iter_next(&iter)) {
            const char *key = bson_iter_key(&iter);
            if (strcmp(key, "_id") == 0) continue;

            bson_type_t type = bson_iter_type(&iter);
            if (type == BSON_TYPE_DOCUMENT) {
                bson_iter_t child;
                if (bson_iter_recurse(&iter, &child) && bson_iter_next(&child)) {
                    const char *child_key = bson_iter_key(&child);
                    if (child_key[0] == '$') continue;
                }
                continue;
            }

            analysis->equality_fields[analysis->equality_count] = strdup(key);
            if (!analysis->equality_fields[analysis->equality_count]) {
                _free_query_analysis(analysis);
                return NULL;
            }
            analysis->equality_count++;
        }
    }

    /* Check if all conditions are simple equality */
    size_t total_fields = 0;
    if (bson_iter_init(&iter, filter)) {
        while (bson_iter_next(&iter)) total_fields++;
    }

    /* is_simple_equality = all fields are equality (including _id) */
    analysis->is_simple_equality = (analysis->equality_count > 0) &&
        (analysis->equality_count == total_fields ||
         (analysis->equality_count + 1 == total_fields));  /* +1 for _id */

    return analysis;
}

void _free_query_analysis(query_analysis_t *analysis) {
    if (!analysis) return;
    if (analysis->equality_fields) {
        for (size_t i = 0; i < analysis->equality_count; i++) {
            free(analysis->equality_fields[i]);
        }
        free(analysis->equality_fields);
    }
    free(analysis);
}

/*
 * _find_best_index - Find the best index for a query
 *
 * Strategy:
 *   1. Get all indexes for the collection
 *   2. For each index, check if first field matches a query field
 *   3. Prefer index that matches more query fields (prefix matching)
 *   4. Return best match or NULL if no suitable index
 */
mongolite_cached_index_t* _find_best_index(mongolite_db_t *db, const char *collection,
                                            const query_analysis_t *analysis,
                                            gerror_t *error) {
    if (!db || !collection || !analysis || analysis->equality_count == 0) {
        return NULL;
    }

    size_t index_count = 0;
    mongolite_cached_index_t *indexes = _mongolite_get_cached_indexes(db, collection,
                                                                       &index_count, error);
    if (!indexes || index_count == 0) {
        return NULL;
    }

    mongolite_cached_index_t *best = NULL;
    size_t best_match_count = 0;

    for (size_t i = 0; i < index_count; i++) {
        mongolite_cached_index_t *idx = &indexes[i];
        if (!idx->keys) continue;

        /* Check how many leading fields of this index match query fields */
        size_t match_count = 0;
        bson_iter_t key_iter;
        if (bson_iter_init(&key_iter, idx->keys)) {
            while (bson_iter_next(&key_iter)) {
                const char *idx_field = bson_iter_key(&key_iter);
                bool found = false;

                /* Check if this index field is in the query */
                for (size_t j = 0; j < analysis->equality_count; j++) {
                    if (strcmp(analysis->equality_fields[j], idx_field) == 0) {
                        found = true;
                        break;
                    }
                }

                if (found) {
                    match_count++;
                } else {
                    /* Prefix matching: stop at first non-matching field */
                    break;
                }
            }
        }

        /* Update best if this index matches more fields */
        if (match_count > 0 && match_count > best_match_count) {
            best = idx;
            best_match_count = match_count;
        }
    }

    return best;
}

/*
 * _find_one_with_index - Use index to find a single document
 *
 * Strategy:
 *   1. Build lookup key from filter values
 *   2. Seek in index tree
 *   3. Get document _id from index value
 *   4. Fetch document by _id from collection
 */
bson_t* _find_one_with_index(mongolite_db_t *db, const char *collection,
                              wtree_tree_t *col_tree,
                              mongolite_cached_index_t *index,
                              const bson_t *filter, gerror_t *error) {
    if (!db || !collection || !col_tree || !index || !filter) {
        return NULL;
    }

    /* Build lookup key from filter using index key spec */
    bson_t *lookup_key = bson_extract_index_key(filter, index->keys);
    if (!lookup_key) {
        return NULL;
    }

    /* Get read transaction */
    wtree_txn_t *txn = _mongolite_get_read_txn(db, error);
    if (!txn) {
        bson_destroy(lookup_key);
        return NULL;
    }

    bson_t *result = NULL;

    /* Create iterator and seek to key */
    wtree_iterator_t *iter = wtree_iterator_create_with_txn(index->tree, txn, error);
    if (!iter) {
        bson_destroy(lookup_key);
        _mongolite_release_read_txn(db, txn);
        return NULL;
    }

    /* Seek to first matching key
     * With DUPSORT, key = indexed fields only (no _id), value = raw OID
     */
    if (wtree_iterator_seek_range(iter, bson_get_data(lookup_key), lookup_key->len)) {
        /* Found something - verify it's an exact match for the lookup key */
        const void *found_key;
        size_t found_len;
        if (wtree_iterator_key(iter, &found_key, &found_len)) {
            bson_t found_bson;
            if (bson_init_static(&found_bson, found_key, found_len)) {
                /* With DUPSORT, key IS just the indexed fields - compare directly */
                if (bson_compare_docs(lookup_key, &found_bson) == 0) {
                    /* Match! Get the document _id from index value (raw OID) */
                    const void *value;
                    size_t value_len;
                    if (wtree_iterator_value(iter, &value, &value_len)) {
                        bson_oid_t doc_oid;
                        if (_index_value_get_oid(value, value_len, &doc_oid)) {
                            /* Fetch document by _id */
                            const void *doc_data;
                            size_t doc_len;
                            int rc = wtree_get_txn(txn, col_tree,
                                                   doc_oid.bytes, sizeof(doc_oid.bytes),
                                                   &doc_data, &doc_len, error);
                            if (rc == 0) {
                                result = bson_new_from_data(doc_data, doc_len);
                            }
                        }
                    }
                }
            }
        }
    }

    wtree_iterator_close(iter);
    bson_destroy(lookup_key);
    _mongolite_release_read_txn(db, txn);

    return result;
}
