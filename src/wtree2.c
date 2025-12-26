/*
 * wtree2.c - Index-aware storage layer implementation
 */

#include "wtree2.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define WTREE2_LIB "wtree2"
#define WTREE2_INDEX_PREFIX "idx:"

/* ============================================================
 * Internal Structures
 * ============================================================ */

/* Single index entry */
typedef struct wtree2_index {
    char *name;                     /* Index name */
    char *tree_name;                /* Full tree name (idx:tree:name) */
    wtree_tree_t *tree;             /* Index tree handle */
    wtree2_index_key_fn key_fn;     /* Key extraction callback */
    void *user_data;                /* Callback user data */
    bool unique;                    /* Unique constraint */
    bool sparse;                    /* Sparse index */
    MDB_cmp_func *compare;          /* Custom comparator */
} wtree2_index_t;

/* Tree with index support */
struct wtree2_tree_t {
    char *name;                     /* Tree name */
    wtree_tree_t *main_tree;        /* Main data tree */
    wtree2_db_t *db;                /* Parent database */

    /* Indexes */
    wtree2_index_t *indexes;        /* Array of indexes */
    size_t index_count;             /* Number of indexes */
    size_t index_capacity;          /* Allocated capacity */

    /* Entry tracking */
    int64_t entry_count;            /* Current entry count */
};

/* Database wrapper */
struct wtree2_db_t {
    wtree_db_t *wdb;                /* Underlying wtree database */
    char *path;                     /* Database path */
};

/* Transaction wrapper */
struct wtree2_txn_t {
    wtree_txn_t *wtxn;              /* Underlying wtree transaction */
    wtree2_db_t *db;                /* Parent database */
    bool is_write;                  /* Write transaction? */
};

/* Iterator wrapper */
struct wtree2_iterator_t {
    wtree_iterator_t *witer;        /* Underlying wtree iterator */
    wtree2_tree_t *tree;            /* Parent tree */
    wtree2_txn_t *txn;              /* Transaction (if owned) */
    bool owns_txn;                  /* Did we create the txn? */
    bool is_index;                  /* Iterating an index tree? */
};

/* ============================================================
 * Helper Functions
 * ============================================================ */

static int translate_wtree_error(int wtree_rc) {
    switch (wtree_rc) {
        case 0:
            return WTREE2_OK;
        case WTREE_MAP_FULL:
            return WTREE2_MAP_FULL;
        case WTREE_KEY_NOT_FOUND:
            return WTREE2_ENOTFOUND;
        case WTREE_KEY_EXISTS:
            return WTREE2_EEXISTS;
        default:
            return WTREE2_ERROR;
    }
}

/* Build index tree name: idx:<tree_name>:<index_name> */
static char* build_index_tree_name(const char *tree_name, const char *index_name) {
    size_t len = strlen(WTREE2_INDEX_PREFIX) + strlen(tree_name) + 1 + strlen(index_name) + 1;
    char *name = malloc(len);
    if (name) {
        snprintf(name, len, "%s%s:%s", WTREE2_INDEX_PREFIX, tree_name, index_name);
    }
    return name;
}

/* Find index by name */
static wtree2_index_t* find_index(wtree2_tree_t *tree, const char *name) {
    for (size_t i = 0; i < tree->index_count; i++) {
        if (strcmp(tree->indexes[i].name, name) == 0) {
            return &tree->indexes[i];
        }
    }
    return NULL;
}

/* Free a single index entry */
static void free_index(wtree2_index_t *idx) {
    if (!idx) return;
    free(idx->name);
    free(idx->tree_name);
    if (idx->tree) {
        wtree_tree_close(idx->tree);
    }
}

/* ============================================================
 * Database Operations
 * ============================================================ */

wtree2_db_t* wtree2_db_create(const char *path, size_t mapsize,
                               unsigned int max_dbs, unsigned int flags,
                               gerror_t *error) {
    if (!path) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Path cannot be NULL");
        return NULL;
    }

    wtree2_db_t *db = calloc(1, sizeof(wtree2_db_t));
    if (!db) {
        set_error(error, WTREE2_LIB, WTREE2_ENOMEM, "Failed to allocate database");
        return NULL;
    }

    db->wdb = wtree_db_create(path, mapsize, max_dbs, flags, error);
    if (!db->wdb) {
        free(db);
        return NULL;
    }

    db->path = strdup(path);
    return db;
}

void wtree2_db_close(wtree2_db_t *db) {
    if (!db) return;
    if (db->wdb) {
        wtree_db_close(db->wdb);
    }
    free(db->path);
    free(db);
}

int wtree2_db_sync(wtree2_db_t *db, bool force, gerror_t *error) {
    if (!db) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Database cannot be NULL");
        return WTREE2_EINVAL;
    }
    return wtree_db_sync(db->wdb, force, error);
}

int wtree2_db_resize(wtree2_db_t *db, size_t new_mapsize, gerror_t *error) {
    if (!db) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Database cannot be NULL");
        return WTREE2_EINVAL;
    }
    return wtree_db_resize(db->wdb, new_mapsize, error);
}

size_t wtree2_db_get_mapsize(wtree2_db_t *db) {
    return db ? wtree_db_get_mapsize(db->wdb) : 0;
}

wtree_db_t* wtree2_db_get_wtree(wtree2_db_t *db) {
    return db ? db->wdb : NULL;
}

/* ============================================================
 * Transaction Operations
 * ============================================================ */

wtree2_txn_t* wtree2_txn_begin(wtree2_db_t *db, bool write, gerror_t *error) {
    if (!db) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Database cannot be NULL");
        return NULL;
    }

    wtree2_txn_t *txn = calloc(1, sizeof(wtree2_txn_t));
    if (!txn) {
        set_error(error, WTREE2_LIB, WTREE2_ENOMEM, "Failed to allocate transaction");
        return NULL;
    }

    txn->wtxn = wtree_txn_begin(db->wdb, write, error);
    if (!txn->wtxn) {
        free(txn);
        return NULL;
    }

    txn->db = db;
    txn->is_write = write;
    return txn;
}

int wtree2_txn_commit(wtree2_txn_t *txn, gerror_t *error) {
    if (!txn) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Transaction cannot be NULL");
        return WTREE2_EINVAL;
    }

    int rc = wtree_txn_commit(txn->wtxn, error);
    free(txn);
    return translate_wtree_error(rc);
}

void wtree2_txn_abort(wtree2_txn_t *txn) {
    if (!txn) return;
    wtree_txn_abort(txn->wtxn);
    free(txn);
}

bool wtree2_txn_is_readonly(wtree2_txn_t *txn) {
    return txn && !txn->is_write;
}

wtree_txn_t* wtree2_txn_get_wtree(wtree2_txn_t *txn) {
    return txn ? txn->wtxn : NULL;
}

/* ============================================================
 * Tree Operations
 * ============================================================ */

wtree2_tree_t* wtree2_tree_create(wtree2_db_t *db, const char *name,
                                   unsigned int flags, int64_t entry_count,
                                   gerror_t *error) {
    if (!db || !name) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Database and name are required");
        return NULL;
    }

    wtree2_tree_t *tree = calloc(1, sizeof(wtree2_tree_t));
    if (!tree) {
        set_error(error, WTREE2_LIB, WTREE2_ENOMEM, "Failed to allocate tree");
        return NULL;
    }

    tree->main_tree = wtree_tree_create(db->wdb, name, flags, error);
    if (!tree->main_tree) {
        free(tree);
        return NULL;
    }

    tree->name = strdup(name);
    tree->db = db;
    tree->entry_count = entry_count;
    tree->index_capacity = 4;  /* Initial capacity */
    tree->indexes = calloc(tree->index_capacity, sizeof(wtree2_index_t));

    if (!tree->name || !tree->indexes) {
        wtree_tree_close(tree->main_tree);
        free(tree->name);
        free(tree->indexes);
        free(tree);
        set_error(error, WTREE2_LIB, WTREE2_ENOMEM, "Failed to allocate tree resources");
        return NULL;
    }

    return tree;
}

void wtree2_tree_close(wtree2_tree_t *tree) {
    if (!tree) return;

    /* Close all indexes */
    for (size_t i = 0; i < tree->index_count; i++) {
        free_index(&tree->indexes[i]);
    }
    free(tree->indexes);

    /* Close main tree */
    if (tree->main_tree) {
        wtree_tree_close(tree->main_tree);
    }

    free(tree->name);
    free(tree);
}

int wtree2_tree_delete(wtree2_db_t *db, const char *name, gerror_t *error) {
    if (!db || !name) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Database and name are required");
        return WTREE2_EINVAL;
    }

    /* Note: This only deletes the main tree.
     * Caller should drop indexes first using wtree2_tree_drop_index().
     * For full cleanup, the tree should be opened first. */
    int rc = wtree_tree_delete(db->wdb, name, error);
    return translate_wtree_error(rc);
}

const char* wtree2_tree_name(wtree2_tree_t *tree) {
    return tree ? tree->name : NULL;
}

int64_t wtree2_tree_count(wtree2_tree_t *tree) {
    return tree ? tree->entry_count : 0;
}

wtree_tree_t* wtree2_tree_get_wtree(wtree2_tree_t *tree) {
    return tree ? tree->main_tree : NULL;
}

wtree2_db_t* wtree2_tree_get_db(wtree2_tree_t *tree) {
    return tree ? tree->db : NULL;
}

/* ============================================================
 * Index Management
 * ============================================================ */

int wtree2_tree_add_index(wtree2_tree_t *tree,
                           const wtree2_index_config_t *config,
                           gerror_t *error) {
    if (!tree || !config || !config->name || !config->key_fn) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Invalid parameters");
        return WTREE2_EINVAL;
    }

    /* Check if index already exists */
    if (find_index(tree, config->name)) {
        set_error(error, WTREE2_LIB, WTREE2_EEXISTS,
                 "Index '%s' already exists", config->name);
        return WTREE2_EEXISTS;
    }

    /* Expand array if needed */
    if (tree->index_count >= tree->index_capacity) {
        size_t new_capacity = tree->index_capacity * 2;
        wtree2_index_t *new_indexes = realloc(tree->indexes,
                                               new_capacity * sizeof(wtree2_index_t));
        if (!new_indexes) {
            set_error(error, WTREE2_LIB, WTREE2_ENOMEM, "Failed to expand index array");
            return WTREE2_ENOMEM;
        }
        tree->indexes = new_indexes;
        tree->index_capacity = new_capacity;
    }

    /* Build index tree name */
    char *tree_name = build_index_tree_name(tree->name, config->name);
    if (!tree_name) {
        set_error(error, WTREE2_LIB, WTREE2_ENOMEM, "Failed to allocate tree name");
        return WTREE2_ENOMEM;
    }

    /* Create index tree with MDB_DUPSORT for multiple entries per key */
    wtree_tree_t *idx_tree = wtree_tree_create(tree->db->wdb, tree_name,
                                                MDB_DUPSORT, error);
    if (!idx_tree) {
        free(tree_name);
        return WTREE2_ERROR;
    }

    /* Set custom comparator if provided */
    if (config->compare) {
        int rc = wtree_tree_set_compare(idx_tree, config->compare, error);
        if (rc != 0) {
            wtree_tree_close(idx_tree);
            wtree_tree_delete(tree->db->wdb, tree_name, NULL);
            free(tree_name);
            return WTREE2_ERROR;
        }
    }

    /* Add to index array */
    wtree2_index_t *idx = &tree->indexes[tree->index_count];
    idx->name = strdup(config->name);
    idx->tree_name = tree_name;
    idx->tree = idx_tree;
    idx->key_fn = config->key_fn;
    idx->user_data = config->user_data;
    idx->unique = config->unique;
    idx->sparse = config->sparse;
    idx->compare = config->compare;

    if (!idx->name) {
        wtree_tree_close(idx_tree);
        wtree_tree_delete(tree->db->wdb, tree_name, NULL);
        free(tree_name);
        set_error(error, WTREE2_LIB, WTREE2_ENOMEM, "Failed to allocate index name");
        return WTREE2_ENOMEM;
    }

    tree->index_count++;
    return WTREE2_OK;
}

int wtree2_tree_populate_index(wtree2_tree_t *tree,
                                const char *index_name,
                                gerror_t *error) {
    if (!tree || !index_name) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Invalid parameters");
        return WTREE2_EINVAL;
    }

    wtree2_index_t *idx = find_index(tree, index_name);
    if (!idx) {
        set_error(error, WTREE2_LIB, WTREE2_ENOTFOUND,
                 "Index '%s' not found", index_name);
        return WTREE2_ENOTFOUND;
    }

    /* Begin write transaction */
    wtree_txn_t *txn = wtree_txn_begin(tree->db->wdb, true, error);
    if (!txn) {
        return WTREE2_ERROR;
    }

    /* Create iterator for main tree */
    wtree_iterator_t *iter = wtree_iterator_create_with_txn(tree->main_tree, txn, error);
    if (!iter) {
        wtree_txn_abort(txn);
        return WTREE2_ERROR;
    }

    int rc = WTREE2_OK;

    /* Scan all entries */
    if (wtree_iterator_first(iter)) {
        do {
            const void *key, *value;
            size_t key_size, value_size;

            if (!wtree_iterator_key(iter, &key, &key_size) ||
                !wtree_iterator_value(iter, &value, &value_size)) {
                continue;
            }

            /* Extract index key */
            void *idx_key = NULL;
            size_t idx_key_size = 0;
            bool should_index = idx->key_fn(value, value_size, idx->user_data,
                                            &idx_key, &idx_key_size);

            if (!should_index) {
                continue;  /* Sparse: skip this entry */
            }

            if (!idx_key) {
                continue;  /* Key extraction failed */
            }

            /* For unique indexes, check if key already exists */
            if (idx->unique) {
                const void *existing;
                size_t existing_size;
                int get_rc = wtree_get_txn(txn, idx->tree, idx_key, idx_key_size,
                                           &existing, &existing_size, NULL);
                if (get_rc == 0) {
                    /* Duplicate found */
                    free(idx_key);
                    wtree_iterator_close(iter);
                    wtree_txn_abort(txn);
                    set_error(error, WTREE2_LIB, WTREE2_EINDEX,
                             "Duplicate key for unique index '%s'", index_name);
                    return WTREE2_EINDEX;
                }
            }

            /* Insert into index: key = index_key, value = main_key */
            rc = wtree_insert_one_txn(txn, idx->tree, idx_key, idx_key_size,
                                       key, key_size, error);
            free(idx_key);

            if (rc != 0) {
                wtree_iterator_close(iter);
                wtree_txn_abort(txn);
                return translate_wtree_error(rc);
            }
        } while (wtree_iterator_next(iter));
    }

    wtree_iterator_close(iter);

    /* Commit */
    rc = wtree_txn_commit(txn, error);
    return translate_wtree_error(rc);
}

int wtree2_tree_drop_index(wtree2_tree_t *tree,
                            const char *index_name,
                            gerror_t *error) {
    if (!tree || !index_name) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Invalid parameters");
        return WTREE2_EINVAL;
    }

    /* Find index */
    size_t idx_pos = 0;
    wtree2_index_t *idx = NULL;
    for (size_t i = 0; i < tree->index_count; i++) {
        if (strcmp(tree->indexes[i].name, index_name) == 0) {
            idx = &tree->indexes[i];
            idx_pos = i;
            break;
        }
    }

    if (!idx) {
        set_error(error, WTREE2_LIB, WTREE2_ENOTFOUND,
                 "Index '%s' not found", index_name);
        return WTREE2_ENOTFOUND;
    }

    /* Delete the index tree from LMDB */
    char *tree_name = idx->tree_name;
    idx->tree_name = NULL;  /* Prevent double-free */

    if (idx->tree) {
        wtree_tree_close(idx->tree);
        idx->tree = NULL;
    }

    int rc = wtree_tree_delete(tree->db->wdb, tree_name, error);
    free(tree_name);

    if (rc != 0 && rc != WTREE_KEY_NOT_FOUND) {
        return translate_wtree_error(rc);
    }

    /* Free index entry */
    free(idx->name);

    /* Remove from array by shifting */
    for (size_t i = idx_pos; i < tree->index_count - 1; i++) {
        tree->indexes[i] = tree->indexes[i + 1];
    }
    tree->index_count--;

    return WTREE2_OK;
}

bool wtree2_tree_has_index(wtree2_tree_t *tree, const char *index_name) {
    return tree && index_name && find_index(tree, index_name) != NULL;
}

size_t wtree2_tree_index_count(wtree2_tree_t *tree) {
    return tree ? tree->index_count : 0;
}

/* ============================================================
 * Index Maintenance Helpers (Internal)
 * ============================================================ */

/*
 * Insert entry into all indexes
 * Returns: 0 on success, error code on failure
 */
static int indexes_insert(wtree2_tree_t *tree, wtree_txn_t *txn,
                          const void *key, size_t key_size,
                          const void *value, size_t value_size,
                          gerror_t *error) {
    for (size_t i = 0; i < tree->index_count; i++) {
        wtree2_index_t *idx = &tree->indexes[i];

        /* Extract index key */
        void *idx_key = NULL;
        size_t idx_key_size = 0;
        bool should_index = idx->key_fn(value, value_size, idx->user_data,
                                        &idx_key, &idx_key_size);

        if (!should_index) {
            continue;  /* Sparse: skip */
        }

        if (!idx_key) {
            set_error(error, WTREE2_LIB, WTREE2_ERROR,
                     "Index key extraction failed for '%s'", idx->name);
            return WTREE2_ERROR;
        }

        /* Check unique constraint */
        if (idx->unique) {
            const void *existing;
            size_t existing_size;
            int get_rc = wtree_get_txn(txn, idx->tree, idx_key, idx_key_size,
                                       &existing, &existing_size, NULL);
            if (get_rc == 0) {
                free(idx_key);
                set_error(error, WTREE2_LIB, WTREE2_EINDEX,
                         "Duplicate key for unique index '%s'", idx->name);
                return WTREE2_EINDEX;
            }
        }

        /* Insert: index_key -> main_key */
        int rc = wtree_insert_one_txn(txn, idx->tree, idx_key, idx_key_size,
                                       key, key_size, error);
        free(idx_key);

        if (rc != 0) {
            return translate_wtree_error(rc);
        }
    }

    return WTREE2_OK;
}

/*
 * Delete entry from all indexes
 * Returns: 0 on success, error code on failure
 */
static int indexes_delete(wtree2_tree_t *tree, wtree_txn_t *txn,
                          const void *key, size_t key_size,
                          const void *value, size_t value_size,
                          gerror_t *error) {
    for (size_t i = 0; i < tree->index_count; i++) {
        wtree2_index_t *idx = &tree->indexes[i];

        /* Extract index key */
        void *idx_key = NULL;
        size_t idx_key_size = 0;
        bool should_index = idx->key_fn(value, value_size, idx->user_data,
                                        &idx_key, &idx_key_size);

        if (!should_index) {
            continue;  /* Was never indexed */
        }

        if (!idx_key) {
            continue;  /* Can't build key, skip */
        }

        /* Delete from DUPSORT tree: specific key+value pair */
        bool deleted = false;
        int rc = wtree_delete_dup_txn(txn, idx->tree, idx_key, idx_key_size,
                                       key, key_size, &deleted, error);
        free(idx_key);

        if (rc != 0) {
            return translate_wtree_error(rc);
        }
        /* Note: We don't fail if pair wasn't found */
    }

    return WTREE2_OK;
}

/* ============================================================
 * Data Operations (With Transaction)
 * ============================================================ */

int wtree2_insert_one_txn(wtree2_txn_t *txn, wtree2_tree_t *tree,
                           const void *key, size_t key_size,
                           const void *value, size_t value_size,
                           gerror_t *error) {
    if (!txn || !tree || !key || !value) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Invalid parameters");
        return WTREE2_EINVAL;
    }

    if (!txn->is_write) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Write operation requires write transaction");
        return WTREE2_EINVAL;
    }

    /* Insert into indexes first (to check unique constraints) */
    int rc = indexes_insert(tree, txn->wtxn, key, key_size, value, value_size, error);
    if (rc != 0) {
        return rc;
    }

    /* Insert into main tree */
    rc = wtree_insert_one_txn(txn->wtxn, tree->main_tree,
                               key, key_size, value, value_size, error);
    if (rc != 0) {
        /* TODO: Rollback index inserts? Transaction will be aborted anyway. */
        return translate_wtree_error(rc);
    }

    /* Increment entry count */
    tree->entry_count++;

    return WTREE2_OK;
}

int wtree2_update_txn(wtree2_txn_t *txn, wtree2_tree_t *tree,
                       const void *key, size_t key_size,
                       const void *value, size_t value_size,
                       gerror_t *error) {
    if (!txn || !tree || !key || !value) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Invalid parameters");
        return WTREE2_EINVAL;
    }

    if (!txn->is_write) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Write operation requires write transaction");
        return WTREE2_EINVAL;
    }

    /* Get old value for index maintenance */
    const void *old_value;
    size_t old_value_size;
    int rc = wtree_get_txn(txn->wtxn, tree->main_tree, key, key_size,
                           &old_value, &old_value_size, error);
    if (rc != 0) {
        if (rc == WTREE_KEY_NOT_FOUND) {
            set_error(error, WTREE2_LIB, WTREE2_ENOTFOUND, "Key not found");
            return WTREE2_ENOTFOUND;
        }
        return translate_wtree_error(rc);
    }

    /* Delete from indexes using old value */
    rc = indexes_delete(tree, txn->wtxn, key, key_size, old_value, old_value_size, error);
    if (rc != 0) {
        return rc;
    }

    /* Insert into indexes with new value */
    rc = indexes_insert(tree, txn->wtxn, key, key_size, value, value_size, error);
    if (rc != 0) {
        /* TODO: Restore old index entries? Transaction will be aborted. */
        return rc;
    }

    /* Update main tree */
    rc = wtree_update_txn(txn->wtxn, tree->main_tree, key, key_size,
                          value, value_size, error);
    if (rc != 0) {
        return translate_wtree_error(rc);
    }

    /* Note: entry_count unchanged (update, not insert) */

    return WTREE2_OK;
}

int wtree2_delete_one_txn(wtree2_txn_t *txn, wtree2_tree_t *tree,
                           const void *key, size_t key_size,
                           bool *deleted,
                           gerror_t *error) {
    if (!txn || !tree || !key) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Invalid parameters");
        return WTREE2_EINVAL;
    }

    if (!txn->is_write) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Write operation requires write transaction");
        return WTREE2_EINVAL;
    }

    if (deleted) *deleted = false;

    /* Get value for index maintenance */
    const void *value;
    size_t value_size;
    int rc = wtree_get_txn(txn->wtxn, tree->main_tree, key, key_size,
                           &value, &value_size, error);
    if (rc != 0) {
        if (rc == WTREE_KEY_NOT_FOUND) {
            /* Key doesn't exist - not an error */
            return WTREE2_OK;
        }
        return translate_wtree_error(rc);
    }

    /* Delete from indexes */
    rc = indexes_delete(tree, txn->wtxn, key, key_size, value, value_size, error);
    if (rc != 0) {
        return rc;
    }

    /* Delete from main tree */
    bool was_deleted = false;
    rc = wtree_delete_one_txn(txn->wtxn, tree->main_tree, key, key_size,
                               &was_deleted, error);
    if (rc != 0) {
        return translate_wtree_error(rc);
    }

    if (was_deleted) {
        tree->entry_count--;
        if (deleted) *deleted = true;
    }

    return WTREE2_OK;
}

int wtree2_get_txn(wtree2_txn_t *txn, wtree2_tree_t *tree,
                    const void *key, size_t key_size,
                    const void **value, size_t *value_size,
                    gerror_t *error) {
    if (!txn || !tree || !key || !value || !value_size) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Invalid parameters");
        return WTREE2_EINVAL;
    }

    int rc = wtree_get_txn(txn->wtxn, tree->main_tree, key, key_size,
                           value, value_size, error);
    return translate_wtree_error(rc);
}

bool wtree2_exists_txn(wtree2_txn_t *txn, wtree2_tree_t *tree,
                        const void *key, size_t key_size,
                        gerror_t *error) {
    if (!txn || !tree || !key) {
        return false;
    }
    return wtree_exists_txn(txn->wtxn, tree->main_tree, key, key_size, error);
}

/* ============================================================
 * Data Operations (Auto-transaction)
 * ============================================================ */

int wtree2_insert_one(wtree2_tree_t *tree,
                       const void *key, size_t key_size,
                       const void *value, size_t value_size,
                       gerror_t *error) {
    if (!tree) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Tree cannot be NULL");
        return WTREE2_EINVAL;
    }

    wtree2_txn_t *txn = wtree2_txn_begin(tree->db, true, error);
    if (!txn) return WTREE2_ERROR;

    int rc = wtree2_insert_one_txn(txn, tree, key, key_size, value, value_size, error);

    if (rc == 0) {
        rc = wtree2_txn_commit(txn, error);
    } else {
        wtree2_txn_abort(txn);
    }

    return rc;
}

int wtree2_update(wtree2_tree_t *tree,
                   const void *key, size_t key_size,
                   const void *value, size_t value_size,
                   gerror_t *error) {
    if (!tree) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Tree cannot be NULL");
        return WTREE2_EINVAL;
    }

    wtree2_txn_t *txn = wtree2_txn_begin(tree->db, true, error);
    if (!txn) return WTREE2_ERROR;

    int rc = wtree2_update_txn(txn, tree, key, key_size, value, value_size, error);

    if (rc == 0) {
        rc = wtree2_txn_commit(txn, error);
    } else {
        wtree2_txn_abort(txn);
    }

    return rc;
}

int wtree2_delete_one(wtree2_tree_t *tree,
                       const void *key, size_t key_size,
                       bool *deleted,
                       gerror_t *error) {
    if (!tree) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Tree cannot be NULL");
        return WTREE2_EINVAL;
    }

    wtree2_txn_t *txn = wtree2_txn_begin(tree->db, true, error);
    if (!txn) return WTREE2_ERROR;

    int rc = wtree2_delete_one_txn(txn, tree, key, key_size, deleted, error);

    if (rc == 0) {
        rc = wtree2_txn_commit(txn, error);
    } else {
        wtree2_txn_abort(txn);
    }

    return rc;
}

int wtree2_get(wtree2_tree_t *tree,
                const void *key, size_t key_size,
                void **value, size_t *value_size,
                gerror_t *error) {
    if (!tree || !key || !value || !value_size) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Invalid parameters");
        return WTREE2_EINVAL;
    }

    wtree2_txn_t *txn = wtree2_txn_begin(tree->db, false, error);
    if (!txn) return WTREE2_ERROR;

    const void *tmp_value;
    size_t tmp_size;

    int rc = wtree2_get_txn(txn, tree, key, key_size, &tmp_value, &tmp_size, error);

    if (rc == 0) {
        /* Copy data before closing transaction */
        *value = malloc(tmp_size);
        if (!*value) {
            wtree2_txn_abort(txn);
            set_error(error, WTREE2_LIB, WTREE2_ENOMEM, "Failed to allocate value");
            return WTREE2_ENOMEM;
        }
        memcpy(*value, tmp_value, tmp_size);
        *value_size = tmp_size;
    }

    wtree2_txn_abort(txn);  /* Read-only, just abort */
    return rc;
}

bool wtree2_exists(wtree2_tree_t *tree,
                    const void *key, size_t key_size,
                    gerror_t *error) {
    if (!tree || !key) return false;

    wtree2_txn_t *txn = wtree2_txn_begin(tree->db, false, error);
    if (!txn) return false;

    bool exists = wtree2_exists_txn(txn, tree, key, key_size, error);
    wtree2_txn_abort(txn);
    return exists;
}

/* ============================================================
 * Iterator Operations
 * ============================================================ */

wtree2_iterator_t* wtree2_iterator_create(wtree2_tree_t *tree, gerror_t *error) {
    if (!tree) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Tree cannot be NULL");
        return NULL;
    }

    wtree2_iterator_t *iter = calloc(1, sizeof(wtree2_iterator_t));
    if (!iter) {
        set_error(error, WTREE2_LIB, WTREE2_ENOMEM, "Failed to allocate iterator");
        return NULL;
    }

    iter->witer = wtree_iterator_create(tree->main_tree, error);
    if (!iter->witer) {
        free(iter);
        return NULL;
    }

    iter->tree = tree;
    iter->owns_txn = false;  /* wtree_iterator owns its txn */
    iter->is_index = false;

    return iter;
}

wtree2_iterator_t* wtree2_iterator_create_with_txn(wtree2_tree_t *tree,
                                                    wtree2_txn_t *txn,
                                                    gerror_t *error) {
    if (!tree || !txn) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Invalid parameters");
        return NULL;
    }

    wtree2_iterator_t *iter = calloc(1, sizeof(wtree2_iterator_t));
    if (!iter) {
        set_error(error, WTREE2_LIB, WTREE2_ENOMEM, "Failed to allocate iterator");
        return NULL;
    }

    iter->witer = wtree_iterator_create_with_txn(tree->main_tree, txn->wtxn, error);
    if (!iter->witer) {
        free(iter);
        return NULL;
    }

    iter->tree = tree;
    iter->txn = txn;
    iter->owns_txn = false;
    iter->is_index = false;

    return iter;
}

bool wtree2_iterator_first(wtree2_iterator_t *iter) {
    return iter && wtree_iterator_first(iter->witer);
}

bool wtree2_iterator_last(wtree2_iterator_t *iter) {
    return iter && wtree_iterator_last(iter->witer);
}

bool wtree2_iterator_next(wtree2_iterator_t *iter) {
    return iter && wtree_iterator_next(iter->witer);
}

bool wtree2_iterator_prev(wtree2_iterator_t *iter) {
    return iter && wtree_iterator_prev(iter->witer);
}

bool wtree2_iterator_seek(wtree2_iterator_t *iter, const void *key, size_t key_size) {
    return iter && wtree_iterator_seek(iter->witer, key, key_size);
}

bool wtree2_iterator_seek_range(wtree2_iterator_t *iter, const void *key, size_t key_size) {
    return iter && wtree_iterator_seek_range(iter->witer, key, key_size);
}

bool wtree2_iterator_key(wtree2_iterator_t *iter, const void **key, size_t *key_size) {
    return iter && wtree_iterator_key(iter->witer, key, key_size);
}

bool wtree2_iterator_value(wtree2_iterator_t *iter, const void **value, size_t *value_size) {
    return iter && wtree_iterator_value(iter->witer, value, value_size);
}

bool wtree2_iterator_valid(wtree2_iterator_t *iter) {
    return iter && wtree_iterator_valid(iter->witer);
}

void wtree2_iterator_close(wtree2_iterator_t *iter) {
    if (!iter) return;

    if (iter->witer) {
        wtree_iterator_close(iter->witer);
    }

    if (iter->owns_txn && iter->txn) {
        wtree2_txn_abort(iter->txn);
    }

    free(iter);
}

/* ============================================================
 * Index Query Operations
 * ============================================================ */

static wtree2_iterator_t* index_seek_internal(wtree2_tree_t *tree,
                                               const char *index_name,
                                               const void *key, size_t key_size,
                                               bool range,
                                               gerror_t *error) {
    if (!tree || !index_name) {
        set_error(error, WTREE2_LIB, WTREE2_EINVAL, "Invalid parameters");
        return NULL;
    }

    wtree2_index_t *idx = find_index(tree, index_name);
    if (!idx) {
        set_error(error, WTREE2_LIB, WTREE2_ENOTFOUND,
                 "Index '%s' not found", index_name);
        return NULL;
    }

    wtree2_iterator_t *iter = calloc(1, sizeof(wtree2_iterator_t));
    if (!iter) {
        set_error(error, WTREE2_LIB, WTREE2_ENOMEM, "Failed to allocate iterator");
        return NULL;
    }

    /* Create iterator on index tree */
    iter->witer = wtree_iterator_create(idx->tree, error);
    if (!iter->witer) {
        free(iter);
        return NULL;
    }

    iter->tree = tree;
    iter->owns_txn = false;
    iter->is_index = true;

    /* Seek to key */
    if (key && key_size > 0) {
        bool found;
        if (range) {
            found = wtree_iterator_seek_range(iter->witer, key, key_size);
        } else {
            found = wtree_iterator_seek(iter->witer, key, key_size);
        }
        (void)found;  /* Iterator will be invalid if not found */
    }

    return iter;
}

wtree2_iterator_t* wtree2_index_seek(wtree2_tree_t *tree,
                                      const char *index_name,
                                      const void *key, size_t key_size,
                                      gerror_t *error) {
    return index_seek_internal(tree, index_name, key, key_size, false, error);
}

wtree2_iterator_t* wtree2_index_seek_range(wtree2_tree_t *tree,
                                            const char *index_name,
                                            const void *key, size_t key_size,
                                            gerror_t *error) {
    return index_seek_internal(tree, index_name, key, key_size, true, error);
}

bool wtree2_index_iterator_main_key(wtree2_iterator_t *iter,
                                     const void **main_key,
                                     size_t *main_key_size) {
    /* In index iterator, "value" is the main tree key */
    return iter && iter->is_index &&
           wtree_iterator_value(iter->witer, main_key, main_key_size);
}

wtree_txn_t* wtree2_iterator_get_txn(wtree2_iterator_t *iter) {
    if (!iter || !iter->witer) {
        return NULL;
    }
    /* Get the transaction from the underlying wtree iterator */
    return wtree_iterator_get_txn(iter->witer);
}

/* ============================================================
 * Utility Functions
 * ============================================================ */

const char* wtree2_strerror(int error_code) {
    switch (error_code) {
        case WTREE2_OK:
            return "Success";
        case WTREE2_ERROR:
            return "Generic error";
        case WTREE2_EINVAL:
            return "Invalid argument";
        case WTREE2_ENOMEM:
            return "Out of memory";
        case WTREE2_EEXISTS:
            return "Already exists";
        case WTREE2_ENOTFOUND:
            return "Not found";
        case WTREE2_EINDEX:
            return "Index error (duplicate key violation)";
        case WTREE2_MAP_FULL:
            return "Database map is full, resize needed";
        default:
            return wtree_strerror(error_code);
    }
}

bool wtree2_error_recoverable(int error_code) {
    return error_code == WTREE2_MAP_FULL || wtree_error_recoverable(error_code);
}
