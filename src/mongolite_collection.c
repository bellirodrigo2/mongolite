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

    /* Create the LMDB tree for this collection */
    wtree_tree_t *tree = wtree_tree_create(db->wdb, tree_name, 0, error);
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
        wtree_tree_close(tree);
        wtree_tree_delete(db->wdb, entry.tree_name, NULL);
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

    /* Remove from tree cache first */
    _mongolite_tree_cache_remove(db, name);

    /* Delete the LMDB tree */
    rc = wtree_tree_delete(db->wdb, entry.tree_name, error);
    if (rc != 0 && rc != WTREE_KEY_NOT_FOUND) {
        _mongolite_schema_entry_free(&entry);
        _mongolite_unlock(db);
        return rc;
    }

    /* TODO: Drop all indexes for this collection */

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

    /* If no filter, return cached count from schema */
    if (!filter || bson_empty(filter)) {
        mongolite_schema_entry_t entry = {0};
        int rc = _mongolite_schema_get(db, collection, &entry, error);
        if (rc != 0) {
            return -1;
        }
        int64_t count = entry.doc_count;
        _mongolite_schema_entry_free(&entry);
        return count;
    }

    /* With filter: need to iterate and count matches */
    /* TODO: Implement filtered count using cursor */
    set_error(error, MONGOLITE_LIB, MONGOLITE_ERROR,
             "Filtered count not yet implemented");
    return -1;
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
 * Internal: Get or Open Collection Tree
 * ============================================================ */

wtree_tree_t* _mongolite_get_collection_tree(mongolite_db_t *db, const char *name,
                                              gerror_t *error) {
    if (!db || !name) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return NULL;
    }

    /* Check cache first */
    wtree_tree_t *tree = _mongolite_tree_cache_get(db, name);
    if (tree) {
        return tree;
    }

    /* Not cached - get from schema and open */
    mongolite_schema_entry_t entry = {0};
    int rc = _mongolite_schema_get(db, name, &entry, error);
    if (rc != 0) {
        return NULL;
    }

    /* Open the tree */
    tree = wtree_tree_create(db->wdb, entry.tree_name, 0, error);
    if (!tree) {
        _mongolite_schema_entry_free(&entry);
        return NULL;
    }

    /* Cache it */
    _mongolite_tree_cache_put(db, name, entry.tree_name, &entry.oid, tree);

    _mongolite_schema_entry_free(&entry);
    return tree;
}
