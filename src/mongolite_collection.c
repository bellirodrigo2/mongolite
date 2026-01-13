/*
 * mongolite_collection.c - Collection management operations
 *
 * Handles:
 * - Collection create/drop
 * - Collection list/exists
 * - Collection count
 *
 * Note: Schema system removed - collections are simply wtree3 trees with "col:" prefix.
 * Metadata support removed for simplicity (low-level embedded DB like SQLite).
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
    (void)config;  /* Config options (capped, metadata) no longer supported */

    if (!db || !name || strlen(name) == 0) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "Database and collection name are required");
        return MONGOLITE_EINVAL;
    }

    _mongolite_lock(db);

    /* Check if collection already exists in cache */
    wtree3_tree_t *existing = _mongolite_tree_cache_get(db, name);
    if (existing) {
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

    /* Check if tree already exists in database */
    int exists = wtree3_tree_exists(db->wdb, tree_name, NULL);
    if (exists == 1) {
        free(tree_name);
        _mongolite_unlock(db);
        set_error(error, MONGOLITE_LIB, MONGOLITE_EEXISTS,
                 "Collection already exists: %s", name);
        return MONGOLITE_EEXISTS;
    }

    /* Create the tree (wtree3_tree_open always creates with MDB_CREATE) */
    wtree3_tree_t *tree = wtree3_tree_open(db->wdb, tree_name, 0, 0, error);
    if (!tree) {
        free(tree_name);
        _mongolite_unlock(db);
        return MONGOLITE_ERROR;
    }

    /* Generate a unique OID for the cache entry */
    bson_oid_t oid;
    bson_oid_init(&oid, NULL);

    /* Cache the tree handle */
    _mongolite_tree_cache_put(db, name, tree_name, &oid, tree);

    free(tree_name);
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

    /* Build tree name */
    char *tree_name = _mongolite_collection_tree_name(name);
    if (!tree_name) {
        _mongolite_unlock(db);
        set_error(error, "system", MONGOLITE_ENOMEM, "Failed to allocate tree name");
        return MONGOLITE_ENOMEM;
    }

    /* Remove from tree cache first (also closes the handle) */
    _mongolite_tree_cache_remove(db, name);

    /* Delete the wtree3 tree (this also deletes its internal index trees) */
    int rc = wtree3_tree_delete(db->wdb, tree_name, error);
    free(tree_name);

    if (rc != 0 && rc != WTREE3_NOT_FOUND) {
        _mongolite_unlock(db);
        return rc;
    }

    if (rc == WTREE3_NOT_FOUND) {
        _mongolite_unlock(db);
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOTFOUND,
                 "Collection not found: %s", name);
        return MONGOLITE_ENOTFOUND;
    }

    _mongolite_unlock(db);
    return MONGOLITE_OK;
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

    /* Check cache first */
    wtree3_tree_t *tree = _mongolite_tree_cache_get(db, name);
    if (tree) {
        return true;
    }

    /* Build tree name and check if it exists */
    char *tree_name = _mongolite_collection_tree_name(name);
    if (!tree_name) {
        return false;
    }

    /* Use wtree3_tree_exists to check without creating */
    int exists = wtree3_tree_exists(db->wdb, tree_name, NULL);
    free(tree_name);

    return exists == 1;
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

    *count = 0;

    /* Get list of all tree names from wtree3 */
    char **tree_names = NULL;
    size_t tree_count = 0;
    int rc = wtree3_db_list_trees(db->wdb, &tree_names, &tree_count, error);
    if (rc != 0) {
        return NULL;
    }

    if (tree_count == 0) {
        return NULL;  /* No trees at all */
    }

    /* Count trees with "col:" prefix */
    size_t col_prefix_len = strlen(MONGOLITE_COL_PREFIX);
    size_t collection_count = 0;
    for (size_t i = 0; i < tree_count; i++) {
        if (tree_names[i] && strncmp(tree_names[i], MONGOLITE_COL_PREFIX, col_prefix_len) == 0) {
            collection_count++;
        }
    }

    if (collection_count == 0) {
        /* Free tree names and return */
        for (size_t i = 0; i < tree_count; i++) {
            free(tree_names[i]);
        }
        free(tree_names);
        return NULL;
    }

    /* Allocate result array */
    char **names = calloc(collection_count, sizeof(char*));
    if (!names) {
        for (size_t i = 0; i < tree_count; i++) {
            free(tree_names[i]);
        }
        free(tree_names);
        set_error(error, "system", MONGOLITE_ENOMEM, "Failed to allocate names array");
        return NULL;
    }

    /* Extract collection names (strip "col:" prefix) */
    size_t idx = 0;
    for (size_t i = 0; i < tree_count && idx < collection_count; i++) {
        if (tree_names[i] && strncmp(tree_names[i], MONGOLITE_COL_PREFIX, col_prefix_len) == 0) {
            names[idx] = strdup(tree_names[i] + col_prefix_len);
            if (!names[idx]) {
                /* Cleanup on allocation failure */
                for (size_t j = 0; j < idx; j++) {
                    free(names[j]);
                }
                free(names);
                for (size_t j = 0; j < tree_count; j++) {
                    free(tree_names[j]);
                }
                free(tree_names);
                set_error(error, "system", MONGOLITE_ENOMEM, "Failed to allocate name");
                return NULL;
            }
            idx++;
        }
    }

    /* Free original tree names */
    for (size_t i = 0; i < tree_count; i++) {
        free(tree_names[i]);
    }
    free(tree_names);

    *count = idx;
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

    /* Get tree */
    wtree3_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (!tree) {
        return -1;
    }

    /* If no filter, return count from wtree3 (fast path) */
    if (!filter || bson_empty(filter)) {
        return wtree3_tree_count(tree);
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
 * Internal: Get or Open Collection Tree
 * ============================================================ */

wtree3_tree_t* _mongolite_get_collection_tree(mongolite_db_t *db, const char *name,
                                               gerror_t *error) {
    if (!db || !name) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return NULL;
    }

    /* Check cache first */
    wtree3_tree_t *tree = _mongolite_tree_cache_get(db, name);
    if (tree) {
        return tree;
    }

    /* Not cached - build tree name */
    char *tree_name = _mongolite_collection_tree_name(name);
    if (!tree_name) {
        set_error(error, "system", MONGOLITE_ENOMEM, "Failed to allocate tree name");
        return NULL;
    }

    /* Check if tree exists before opening (wtree3_tree_open always creates) */
    int exists = wtree3_tree_exists(db->wdb, tree_name, NULL);
    if (exists != 1) {
        free(tree_name);
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOTFOUND,
                 "Collection not found: %s", name);
        return NULL;
    }

    /* Open the tree with wtree3 (index-aware) */
    /* Note: wtree3 automatically loads all indexes from its metadata database */
    tree = wtree3_tree_open(db->wdb, tree_name, 0, -1, error);
    if (!tree) {
        free(tree_name);
        return NULL;
    }

    /* Generate OID for cache entry */
    bson_oid_t oid;
    bson_oid_init(&oid, NULL);

    /* Cache it */
    _mongolite_tree_cache_put(db, name, tree_name, &oid, tree);

    free(tree_name);
    return tree;
}
