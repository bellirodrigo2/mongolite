/*
 * mongolite_collection.c - Collection management operations
 *
 * Handles:
 * - Collection create/drop
 * - Collection list/exists
 * - Collection count
 * - Collection metadata
 */

#include "mongolite_internal.h"
#include "key_compare.h"
#include <stdlib.h>
#include <string.h>

#define MONGOLITE_LIB "mongolite"

/* ============================================================
 * Collection Create
 * ============================================================ */

int mongolite_collection_create(mongolite_db_t *db, const char *name,
                                 col_config_t *config, gerror_t *error) {
    if (!db || !name || strlen(name) == 0) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "Database and collection name are required");
        return MONGOLITE_EINVAL;
    }

    _mongolite_lock(db);

    /* Check if collection already exists (inside lock to avoid race condition) */
    mongolite_schema_entry_t existing = {0};
    int rc = _mongolite_schema_get(db, name, &existing, NULL);
    if (rc == 0) {
        _mongolite_schema_entry_free(&existing);
        _mongolite_unlock(db);
        set_error(error, MONGOLITE_LIB, MONGOLITE_EEXISTS,
                 "Collection already exists: %s", name);
        return MONGOLITE_EEXISTS;
    }

    /* Build tree name: col:<collection_name> */
    char *tree_name = _mongolite_collection_tree_name(name);
    if (!tree_name) {
        _mongolite_unlock(db);
        set_error(error, "system", MONGOLITE_ENOMEM,
                 "Failed to allocate tree name");
        return MONGOLITE_ENOMEM;
    }

    /* Create the wtree2 tree for this collection (with index support) */
    wtree2_tree_t *tree = wtree2_tree_create(db->wdb, tree_name, 0, 0, error);
    if (!tree) {
        free(tree_name);
        _mongolite_unlock(db);
        return MONGOLITE_ERROR;
    }

    /* Create schema entry */
    mongolite_schema_entry_t entry = {0};
    bson_oid_init(&entry.oid, NULL);
    entry.name = strdup(name);
    entry.tree_name = tree_name;  /* Transfer ownership */
    entry.type = strdup(SCHEMA_TYPE_COLLECTION);
    entry.created_at = _mongolite_now_ms();
    entry.modified_at = entry.created_at;
    entry.doc_count = 0;

    /* Create default _id index info */
    entry.indexes = bson_new();
    bson_t idx_doc;
    BSON_APPEND_DOCUMENT_BEGIN(entry.indexes, "0", &idx_doc);
    BSON_APPEND_UTF8(&idx_doc, "name", "_id_");
    bson_t keys;
    BSON_APPEND_DOCUMENT_BEGIN(&idx_doc, "keys", &keys);
    BSON_APPEND_INT32(&keys, "_id", 1);
    bson_append_document_end(&idx_doc, &keys);
    BSON_APPEND_BOOL(&idx_doc, "unique", true);
    bson_append_document_end(entry.indexes, &idx_doc);

    /* Copy options from config */
    if (config) {
        entry.options = bson_new();
        BSON_APPEND_BOOL(entry.options, "capped", config->capped);
        if (config->capped) {
            BSON_APPEND_INT64(entry.options, "max_docs", config->max_docs);
            BSON_APPEND_INT64(entry.options, "max_bytes", config->max_bytes);
        }
        if (config->validator) {
            BSON_APPEND_DOCUMENT(entry.options, "validator", config->validator);
        }
        if (config->metadata) {
            entry.metadata = bson_copy(config->metadata);
        }
    }

    /* Store schema entry */
    rc = _mongolite_schema_put(db, &entry, error);
    if (rc != 0) {
        /* Delete the tree to avoid orphaned DBI - close handle first, then delete */
        wtree2_tree_close(tree);
        wtree2_tree_delete(db->wdb, entry.tree_name, NULL);
        _mongolite_schema_entry_free(&entry);
        _mongolite_unlock(db);
        return rc;
    }

    /* Cache the tree handle */
    _mongolite_tree_cache_put(db, name, entry.tree_name, &entry.oid, tree);

    _mongolite_schema_entry_free(&entry);
    _mongolite_unlock(db);

    return MONGOLITE_OK;
}

/* ============================================================
 * Collection Drop
 * ============================================================ */

int mongolite_collection_drop(mongolite_db_t *db, const char *name, gerror_t *error) {
    if (!db || !name) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "Database and collection name are required");
        return MONGOLITE_EINVAL;
    }

    _mongolite_lock(db);

    /* Get schema entry to find tree name */
    mongolite_schema_entry_t entry = {0};
    int rc = _mongolite_schema_get(db, name, &entry, error);
    if (rc != 0) {
        _mongolite_unlock(db);
        return rc;
    }

    /* Verify it's a collection, not an index */
    if (!entry.type || strcmp(entry.type, SCHEMA_TYPE_COLLECTION) != 0) {
        _mongolite_schema_entry_free(&entry);
        _mongolite_unlock(db);
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "'%s' is not a collection", name);
        return MONGOLITE_EINVAL;
    }

    /* Remove from tree cache first (also invalidates index cache) */
    _mongolite_tree_cache_remove(db, name);

    /* Delete the wtree2 tree (this also deletes its internal index trees) */
    rc = wtree2_tree_delete(db->wdb, entry.tree_name, error);
    if (rc != 0 && rc != WTREE2_ENOTFOUND) {
        _mongolite_schema_entry_free(&entry);
        _mongolite_unlock(db);
        return rc;
    }

    /* Note: wtree2 now manages index trees internally.
     * We still clean up the old-style index trees for backward compatibility */
    if (entry.indexes) {
        bson_iter_t iter;
        if (bson_iter_init(&iter, entry.indexes)) {
            while (bson_iter_next(&iter)) {
                if (BSON_ITER_HOLDS_DOCUMENT(&iter)) {
                    bson_iter_t child;
                    if (bson_iter_recurse(&iter, &child) &&
                        bson_iter_find(&child, "name") &&
                        BSON_ITER_HOLDS_UTF8(&child)) {
                        const char *index_name = bson_iter_utf8(&child, NULL);
                        /* Skip _id index (no separate tree) */
                        if (strcmp(index_name, "_id_") != 0) {
                            char *index_tree_name = _mongolite_index_tree_name(name, index_name);
                            if (index_tree_name) {
                                /* Try to delete any old-style index trees */
                                wtree_db_t *wdb = wtree2_db_get_wtree(db->wdb);
                                wtree_tree_delete(wdb, index_tree_name, NULL);
                                free(index_tree_name);
                            }
                        }
                    }
                }
            }
        }
    }

    /* Delete schema entry */
    rc = _mongolite_schema_delete(db, name, error);

    _mongolite_schema_entry_free(&entry);
    _mongolite_unlock(db);

    return rc;
}

/* ============================================================
 * Collection Exists
 * ============================================================ */

bool mongolite_collection_exists(mongolite_db_t *db, const char *name, gerror_t *error) {
    if (!db || !name) {
        if (error) {
            set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                     "Database and collection name are required");
        }
        return false;
    }

    mongolite_schema_entry_t entry = {0};
    int rc = _mongolite_schema_get(db, name, &entry, NULL);

    if (rc == 0) {
        /* Found - check if it's a collection */
        bool is_collection = entry.type && strcmp(entry.type, SCHEMA_TYPE_COLLECTION) == 0;
        _mongolite_schema_entry_free(&entry);
        return is_collection;
    }

    return false;
}

/* ============================================================
 * Collection List
 * ============================================================ */

char** mongolite_collection_list(mongolite_db_t *db, size_t *count, gerror_t *error) {
    if (!db || !count) {
        if (error) {
            set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                     "Database and count pointer are required");
        }
        return NULL;
    }

    char **names = NULL;
    int rc = _mongolite_schema_list(db, &names, count, SCHEMA_TYPE_COLLECTION, error);

    if (rc != 0) {
        return NULL;
    }

    return names;
}

void mongolite_collection_list_free(char **list, size_t count) {
    if (!list) return;
    for (size_t i = 0; i < count; i++) {
        free(list[i]);
    }
    free(list);
}

/* ============================================================
 * Collection Count
 * ============================================================ */

int64_t mongolite_collection_count(mongolite_db_t *db, const char *collection,
                                    const bson_t *filter, gerror_t *error) {
    if (!db || !collection) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "Database and collection name are required");
        return -1;
    }

    /* If no filter, return count from wtree2 (fast path) */
    if (!filter || bson_empty(filter)) {
        /* Get tree and return wtree2's count */
        wtree2_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
        if (tree) {
            return wtree2_tree_count(tree);
        }
        /* Fallback to schema if tree not accessible */
        mongolite_schema_entry_t entry = {0};
        int rc = _mongolite_schema_get(db, collection, &entry, error);
        if (rc != 0) {
            return -1;
        }
        int64_t count = entry.doc_count;
        _mongolite_schema_entry_free(&entry);
        return count;
    }

    /* With filter: iterate and count matches using cursor */
    mongolite_cursor_t *cursor = mongolite_find(db, collection, filter, NULL, error);
    if (!cursor) {
        return -1;
    }

    int64_t count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }

    mongolite_cursor_destroy(cursor);
    return count;
}

/* ============================================================
 * Collection Metadata
 * ============================================================ */

const bson_t* mongolite_collection_metadata(mongolite_db_t *db, const char *collection,
                                             gerror_t *error) {
    if (!db || !collection) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "Database and collection name are required");
        return NULL;
    }

    /* Note: This returns a copy that the caller must manage */
    mongolite_schema_entry_t entry = {0};
    int rc = _mongolite_schema_get(db, collection, &entry, error);
    if (rc != 0) {
        return NULL;
    }

    bson_t *metadata = entry.metadata ? bson_copy(entry.metadata) : NULL;
    _mongolite_schema_entry_free(&entry);

    return metadata;
}

int mongolite_collection_set_metadata(mongolite_db_t *db, const char *collection,
                                       const bson_t *metadata, gerror_t *error) {
    if (!db || !collection) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "Database and collection name are required");
        return MONGOLITE_EINVAL;
    }

    _mongolite_lock(db);

    /* Get current schema entry */
    mongolite_schema_entry_t entry = {0};
    int rc = _mongolite_schema_get(db, collection, &entry, error);
    if (rc != 0) {
        _mongolite_unlock(db);
        return rc;
    }

    /* Update metadata */
    if (entry.metadata) {
        bson_destroy(entry.metadata);
    }
    entry.metadata = metadata ? bson_copy(metadata) : NULL;
    entry.modified_at = _mongolite_now_ms();

    /* Save back */
    rc = _mongolite_schema_put(db, &entry, error);

    _mongolite_schema_entry_free(&entry);
    _mongolite_unlock(db);

    return rc;
}

/* ============================================================
 * Internal: Load indexes from schema into wtree2 tree
 *
 * Called when opening a tree from cache miss to register all
 * existing indexes with wtree2 for automatic maintenance.
 * ============================================================ */

static int _load_indexes_from_schema(wtree2_tree_t *tree, const bson_t *indexes,
                                      gerror_t *error) {
    if (!indexes || bson_empty(indexes)) {
        return MONGOLITE_OK;  /* No indexes to load */
    }

    bson_iter_t iter;
    if (!bson_iter_init(&iter, indexes)) {
        return MONGOLITE_OK;
    }

    /* Iterate over all indexes in the schema */
    while (bson_iter_next(&iter)) {
        if (!BSON_ITER_HOLDS_DOCUMENT(&iter)) continue;

        /* Get index spec document */
        uint32_t len;
        const uint8_t *data;
        bson_iter_document(&iter, &len, &data);
        bson_t spec;
        if (!bson_init_static(&spec, data, len)) continue;

        /* Parse the index spec */
        char *idx_name = NULL;
        bson_t *idx_keys = NULL;
        index_config_t idx_config = {0};

        int rc = _index_spec_from_bson(&spec, &idx_name, &idx_keys, &idx_config);
        if (rc != MONGOLITE_OK || !idx_name || !idx_keys) {
            if (idx_name) free(idx_name);
            if (idx_keys) bson_destroy(idx_keys);
            continue;
        }

        /* Skip the default _id index - it's implicit */
        if (strcmp(idx_name, "_id_") == 0) {
            free(idx_name);
            bson_destroy(idx_keys);
            continue;
        }

        /* Copy keys for use as user_data (wtree2 stores this reference) */
        bson_t *keys_copy = bson_copy(idx_keys);
        bson_destroy(idx_keys);
        if (!keys_copy) {
            free(idx_name);
            continue;
        }

        /* Configure index for wtree2 */
        wtree2_index_config_t wtree2_config = {
            .name = idx_name,
            .key_fn = idx_config.sparse ? bson_index_key_extractor_sparse : bson_index_key_extractor,
            .user_data = keys_copy,
            .unique = idx_config.unique,
            .sparse = idx_config.sparse,
            .compare = _mongolite_index_compare
        };

        /* Register index with wtree2 (no population - data already exists) */
        rc = wtree2_tree_add_index(tree, &wtree2_config, error);

        free(idx_name);

        if (rc != WTREE2_OK && rc != WTREE2_EEXISTS) {
            /* Index registration failed - free the keys copy we made */
            bson_destroy(keys_copy);
            /* Continue trying other indexes - log error but don't fail */
        }
        /* Note: keys_copy ownership transferred to wtree2 on success */
    }

    return MONGOLITE_OK;
}

/* ============================================================
 * Internal: Get or Open Collection Tree
 * ============================================================ */

wtree2_tree_t* _mongolite_get_collection_tree(mongolite_db_t *db, const char *name,
                                               gerror_t *error) {
    if (!db || !name) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return NULL;
    }

    /* Check cache first */
    wtree2_tree_t *tree = _mongolite_tree_cache_get(db, name);
    if (tree) {
        return tree;
    }

    /* Not cached - get from schema and open */
    mongolite_schema_entry_t entry = {0};
    int rc = _mongolite_schema_get(db, name, &entry, error);
    if (rc != 0) {
        return NULL;
    }

    /* Open the tree with wtree2 (index-aware), using doc_count from schema */
    tree = wtree2_tree_create(db->wdb, entry.tree_name, 0, entry.doc_count, error);
    if (!tree) {
        _mongolite_schema_entry_free(&entry);
        return NULL;
    }

    /* Load indexes from schema into wtree2 for automatic maintenance */
    rc = _load_indexes_from_schema(tree, entry.indexes, error);
    if (rc != MONGOLITE_OK) {
        /* Index loading failed - but tree is still usable */
        /* Just log and continue (indexes won't be maintained) */
    }

    /* Cache it */
    _mongolite_tree_cache_put(db, name, entry.tree_name, &entry.oid, tree);

    _mongolite_schema_entry_free(&entry);
    return tree;
}
