#include "wtree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>

// Windows compatibility for S_ISDIR
#ifndef S_ISDIR
#define S_ISDIR(mode) (((mode) & S_IFMT) == S_IFDIR)
#endif

#define WTREE_LIB "wtree"

// ============= Internal Structures =============

struct wtree_db_t {
    MDB_env *env;
    char *path;
    size_t mapsize;
    unsigned int max_dbs;
    unsigned int flags;
};

struct wtree_txn_t {
    MDB_txn *txn;
    wtree_db_t *db;
    bool is_write;
    bool is_nested;
};

struct wtree_tree_t {
    MDB_dbi dbi;
    char *name;
    wtree_db_t *db;
    MDB_cmp_func *cmp_func;
    MDB_cmp_func *dup_cmp_func;
};

struct wtree_iterator_t {
    MDB_cursor *cursor;
    wtree_txn_t *txn;
    wtree_tree_t *tree;
    MDB_val current_key;
    MDB_val current_val;
    bool valid;
    bool owns_txn;  // Se o iterator criou sua própria transação
};

// ============= Helper Functions =============

static int translate_mdb_error(int mdb_rc, gerror_t *error) {
    switch (mdb_rc) {
        case MDB_MAP_FULL:
            set_error(error, WTREE_LIB, WTREE_MAP_FULL, 
                     "Database map is full, resize needed");
            return WTREE_MAP_FULL;
        case MDB_TXN_FULL:
            set_error(error, WTREE_LIB, WTREE_TXN_FULL, 
                     "Transaction has too many dirty pages");
            return WTREE_TXN_FULL;
        case MDB_NOTFOUND:
            set_error(error, WTREE_LIB, WTREE_KEY_NOT_FOUND, 
                     "Key not found");
            return WTREE_KEY_NOT_FOUND;
        default:
            set_error(error, WTREE_LIB, mdb_rc, "%s", mdb_strerror(mdb_rc));
            return mdb_rc;
    }
}

// ============= Database Operations =============

wtree_db_t* wtree_db_create(const char *path, size_t mapsize, unsigned int max_dbs, 
                            unsigned int flags, gerror_t *error) {
    if (!path) {
        set_error(error, WTREE_LIB, EINVAL, "Database path cannot be NULL");
        return NULL;
    }
    
    // Check if directory exists (don't create it)
    struct stat st = {0};
    if (stat(path, &st) == -1) {
        set_error(error, WTREE_LIB, ENOENT, 
                 "Directory does not exist: %s", path);
        return NULL;
    }
    
    if (!S_ISDIR(st.st_mode)) {
        set_error(error, WTREE_LIB, ENOTDIR, 
                 "Path is not a directory: %s", path);
        return NULL;
    }
    
    wtree_db_t *db = calloc(1, sizeof(wtree_db_t));
    if (!db) {
        set_error(error, WTREE_LIB, ENOMEM, "Failed to allocate memory for database");
        return NULL;
    }
    
    int rc;
    
    // Create environment
    rc = mdb_env_create(&db->env);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to create environment: %s", mdb_strerror(rc));
        free(db);
        return NULL;
    }
    
    // Set map size
    if (mapsize == 0) {
        mapsize = 1024UL * 1024 * 1024; // Default 1GB
    }
    rc = mdb_env_set_mapsize(db->env, mapsize);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to set map size: %s", mdb_strerror(rc));
        mdb_env_close(db->env);
        free(db);
        return NULL;
    }
    
    // Set max databases
    if (max_dbs == 0) {
        max_dbs = 128; // Default
    }
    rc = mdb_env_set_maxdbs(db->env, max_dbs);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to set max databases: %s", mdb_strerror(rc));
        mdb_env_close(db->env);
        free(db);
        return NULL;
    }
    
    // Open environment
    rc = mdb_env_open(db->env, path, flags, 0664);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to open environment: %s", mdb_strerror(rc));
        mdb_env_close(db->env);
        free(db);
        return NULL;
    }
    
    db->path = strdup(path);
    db->mapsize = mapsize;
    db->max_dbs = max_dbs;
    db->flags = flags;
    
    return db;
}

void wtree_db_close(wtree_db_t *db) {
    if (!db) return;
    
    if (db->env) {
        mdb_env_close(db->env);
    }
    
    free(db->path);
    free(db);
}

int wtree_db_stats(wtree_db_t *db, MDB_stat *stat, gerror_t *error) {
    if (!db || !stat) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return -1;
    }
    
    int rc = mdb_env_stat(db->env, stat);
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }
    
    return 0;
}

int wtree_db_sync(wtree_db_t *db, bool force, gerror_t *error) {
    if (!db) {
        set_error(error, WTREE_LIB, EINVAL, "Database cannot be NULL");
        return -1;
    }
    
    int rc = mdb_env_sync(db->env, force ? 1 : 0);
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }
    
    return 0;
}

int wtree_db_resize(wtree_db_t *db, size_t new_mapsize, gerror_t *error) {
    if (!db) {
        set_error(error, WTREE_LIB, EINVAL, "Database cannot be NULL");
        return -1;
    }
    
    int rc = mdb_env_set_mapsize(db->env, new_mapsize);
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }
    
    db->mapsize = new_mapsize;
    return 0;
}

size_t wtree_db_get_mapsize(wtree_db_t *db) {
    return db ? db->mapsize : 0;
}

// ============= Tree Operations =============

wtree_tree_t* wtree_tree_create(wtree_db_t *db, const char *name, unsigned int flags, gerror_t *error) {
    if (!db) {
        set_error(error, WTREE_LIB, EINVAL, "Database cannot be NULL");
        return NULL;
    }
    
    wtree_tree_t *tree = calloc(1, sizeof(wtree_tree_t));
    if (!tree) {
        set_error(error, WTREE_LIB, ENOMEM, "Failed to allocate memory for tree");
        return NULL;
    }
    
    MDB_txn *txn;
    int rc = mdb_txn_begin(db->env, NULL, 0, &txn);
    if (rc != 0) {
        translate_mdb_error(rc, error);
        free(tree);
        return NULL;
    }
    
    rc = mdb_dbi_open(txn, name, MDB_CREATE | flags, &tree->dbi);
    if (rc != 0) {
        translate_mdb_error(rc, error);
        mdb_txn_abort(txn);
        free(tree);
        return NULL;
    }
    
    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        translate_mdb_error(rc, error);
        free(tree);
        return NULL;
    }
    
    tree->name = name ? strdup(name) : NULL;
    tree->db = db;
    tree->cmp_func = NULL;
    tree->dup_cmp_func = NULL;
    
    return tree;
}

int wtree_tree_set_compare(wtree_tree_t *tree, MDB_cmp_func *cmp, gerror_t *error) {
    if (!tree || !cmp) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return -1;
    }
    
    MDB_txn *txn;
    int rc = mdb_txn_begin(tree->db->env, NULL, 0, &txn);
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }
    
    rc = mdb_set_compare(txn, tree->dbi, cmp);
    if (rc != 0) {
        mdb_txn_abort(txn);
        return translate_mdb_error(rc, error);
    }
    
    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }
    
    tree->cmp_func = cmp;
    return 0;
}

int wtree_tree_set_dupsort(wtree_tree_t *tree, MDB_cmp_func *cmp, gerror_t *error) {
    if (!tree || !cmp) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return -1;
    }
    
    MDB_txn *txn;
    int rc = mdb_txn_begin(tree->db->env, NULL, 0, &txn);
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }
    
    rc = mdb_set_dupsort(txn, tree->dbi, cmp);
    if (rc != 0) {
        mdb_txn_abort(txn);
        return translate_mdb_error(rc, error);
    }
    
    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }
    
    tree->dup_cmp_func = cmp;
    return 0;
}

int wtree_tree_delete(wtree_db_t *db, const char *name, gerror_t *error) {
    if (!db || !name) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return -1;
    }
    
    MDB_txn *txn;
    int rc = mdb_txn_begin(db->env, NULL, 0, &txn);
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }
    
    MDB_dbi dbi;
    rc = mdb_dbi_open(txn, name, 0, &dbi);
    if (rc != 0) {
        mdb_txn_abort(txn);
        return translate_mdb_error(rc, error);
    }
    
    rc = mdb_drop(txn, dbi, 1);  // 1 = delete DB
    if (rc != 0) {
        mdb_txn_abort(txn);
        return translate_mdb_error(rc, error);
    }
    
    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }
    
    return 0;
}

void wtree_tree_close(wtree_tree_t *tree) {
    if (!tree) return;
    
    // Note: mdb_dbi_close is usually not needed with LMDB
    // DBIs are closed automatically when the environment is closed
    
    free(tree->name);
    free(tree);
}

// ============= Transaction Operations =============

wtree_txn_t* wtree_txn_begin(wtree_db_t *db, bool write, gerror_t *error) {
    if (!db) {
        set_error(error, WTREE_LIB, EINVAL, "Database cannot be NULL");
        return NULL;
    }
    
    wtree_txn_t *txn = calloc(1, sizeof(wtree_txn_t));
    if (!txn) {
        set_error(error, WTREE_LIB, ENOMEM, "Failed to allocate memory for transaction");
        return NULL;
    }
    
    unsigned int flags = write ? 0 : MDB_RDONLY;
    int rc = mdb_txn_begin(db->env, NULL, flags, &txn->txn);
    if (rc != 0) {
        translate_mdb_error(rc, error);
        free(txn);
        return NULL;
    }
    
    txn->db = db;
    txn->is_write = write;
    txn->is_nested = false;
    
    return txn;
}

wtree_txn_t* wtree_txn_begin_nested(wtree_txn_t *parent, gerror_t *error) {
    if (!parent) {
        set_error(error, WTREE_LIB, EINVAL, "Parent transaction cannot be NULL");
        return NULL;
    }
    
    if (!parent->is_write) {
        set_error(error, WTREE_LIB, EINVAL, "Nested transactions require write parent");
        return NULL;
    }
    
    wtree_txn_t *txn = calloc(1, sizeof(wtree_txn_t));
    if (!txn) {
        set_error(error, WTREE_LIB, ENOMEM, "Failed to allocate memory for transaction");
        return NULL;
    }
    
    int rc = mdb_txn_begin(parent->db->env, parent->txn, 0, &txn->txn);
    if (rc != 0) {
        translate_mdb_error(rc, error);
        free(txn);
        return NULL;
    }
    
    txn->db = parent->db;
    txn->is_write = true;
    txn->is_nested = true;
    
    return txn;
}

int wtree_txn_commit(wtree_txn_t *txn, gerror_t *error) {
    if (!txn) {
        set_error(error, WTREE_LIB, EINVAL, "Transaction cannot be NULL");
        return -1;
    }
    
    int rc = mdb_txn_commit(txn->txn);
    if (rc != 0) {
        free(txn);
        return translate_mdb_error(rc, error);
    }
    
    free(txn);
    return 0;
}

void wtree_txn_abort(wtree_txn_t *txn) {
    if (!txn) return;

    mdb_txn_abort(txn->txn);
    free(txn);
}

void wtree_txn_reset(wtree_txn_t *txn) {
    if (!txn || txn->is_write) return;  // Only valid for read-only txn
    mdb_txn_reset(txn->txn);
}

int wtree_txn_renew(wtree_txn_t *txn, gerror_t *error) {
    if (!txn) {
        set_error(error, WTREE_LIB, EINVAL, "Transaction is NULL");
        return -1;
    }
    if (txn->is_write) {
        set_error(error, WTREE_LIB, EINVAL, "Cannot renew write transaction");
        return -1;
    }

    int rc = mdb_txn_renew(txn->txn);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "mdb_txn_renew failed: %s", mdb_strerror(rc));
        return -1;
    }
    return 0;
}

bool wtree_txn_is_readonly(wtree_txn_t *txn) {
    return txn && !txn->is_write;
}

// ============= Data Operations (With Transaction) =============

int wtree_insert_one_txn(wtree_txn_t *txn, wtree_tree_t *tree, 
                        const void *key, size_t key_size,
                        const void *value, size_t value_size, gerror_t *error) {
    if (!txn || !tree || !key || !value) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return -1;
    }
    
    if (!txn->is_write) {
        set_error(error, WTREE_LIB, EINVAL, "Write operation requires write transaction");
        return -1;
    }
    
    MDB_val mkey = {.mv_size = key_size, .mv_data = (void*)key};
    MDB_val mval = {.mv_size = value_size, .mv_data = (void*)value};
    
    int rc = mdb_put(txn->txn, tree->dbi, &mkey, &mval, MDB_NOOVERWRITE);
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }
    
    return 0;
}

int wtree_insert_many_txn(wtree_txn_t *txn, wtree_tree_t *tree, 
                         const wtree_kv_t *kvs, size_t count, gerror_t *error) {
    if (!txn || !tree || !kvs || count == 0) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return -1;
    }
    
    if (!txn->is_write) {
        set_error(error, WTREE_LIB, EINVAL, "Write operation requires write transaction");
        return -1;
    }
    
    for (size_t i = 0; i < count; i++) {
        MDB_val mkey = {.mv_size = kvs[i].key_size, .mv_data = (void*)kvs[i].key};
        MDB_val mval = {.mv_size = kvs[i].value_size, .mv_data = (void*)kvs[i].value};
        
        int rc = mdb_put(txn->txn, tree->dbi, &mkey, &mval, MDB_NOOVERWRITE);
        if (rc != 0 && rc != MDB_KEYEXIST) {
            return translate_mdb_error(rc, error);
        }
    }
    
    return 0;
}

int wtree_update_txn(wtree_txn_t *txn, wtree_tree_t *tree,
                    const void *key, size_t key_size,
                    const void *value, size_t value_size, gerror_t *error) {
    if (!txn || !tree || !key || !value) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return -1;
    }
    
    if (!txn->is_write) {
        set_error(error, WTREE_LIB, EINVAL, "Write operation requires write transaction");
        return -1;
    }
    
    MDB_val mkey = {.mv_size = key_size, .mv_data = (void*)key};
    MDB_val mval = {.mv_size = value_size, .mv_data = (void*)value};
    
    int rc = mdb_put(txn->txn, tree->dbi, &mkey, &mval, 0);
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }
    
    return 0;
}

int wtree_delete_one_txn(wtree_txn_t *txn, wtree_tree_t *tree,
                         const void *key, size_t key_size, 
                         bool *deleted, gerror_t *error) {
    if (!txn || !tree || !key) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return -1;
    }
    
    if (!txn->is_write) {
        set_error(error, WTREE_LIB, EINVAL, "Write operation requires write transaction");
        return -1;
    }
    
    MDB_val mkey = {.mv_size = key_size, .mv_data = (void*)key};
    
    int rc = mdb_del(txn->txn, tree->dbi, &mkey, NULL);
    
    if (rc == MDB_NOTFOUND) {
        // Not an error - key just doesn't exist
        if (deleted) *deleted = false;
        return 0;
    }
    
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }
    
    if (deleted) *deleted = true;
    return 0;
}

int wtree_delete_many_txn(wtree_txn_t *txn, wtree_tree_t *tree,
                          const void **keys, const size_t *key_sizes, 
                          size_t count, size_t *deleted_count, gerror_t *error) {
    if (!txn || !tree || !keys || !key_sizes || count == 0) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return -1;
    }
    
    if (!txn->is_write) {
        set_error(error, WTREE_LIB, EINVAL, "Write operation requires write transaction");
        return -1;
    }
    
    size_t deleted = 0;
    
    for (size_t i = 0; i < count; i++) {
        MDB_val mkey = {.mv_size = key_sizes[i], .mv_data = (void*)keys[i]};
        int rc = mdb_del(txn->txn, tree->dbi, &mkey, NULL);
        
        if (rc == 0) {
            deleted++;
        } else if (rc != MDB_NOTFOUND) {
            // Only fail on real errors, not on missing keys
            if (deleted_count) *deleted_count = deleted;
            return translate_mdb_error(rc, error);
        }
    }
    
    if (deleted_count) *deleted_count = deleted;
    return 0;
}

int wtree_get_txn(wtree_txn_t *txn, wtree_tree_t *tree,
                 const void *key, size_t key_size,
                 const void **value, size_t *value_size, gerror_t *error) {
    if (!txn || !tree || !key || !value || !value_size) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return -1;
    }
    
    MDB_val mkey = {.mv_size = key_size, .mv_data = (void*)key};
    MDB_val mval;
    
    int rc = mdb_get(txn->txn, tree->dbi, &mkey, &mval);
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }
    
    // Zero-copy return - data is valid only during transaction
    *value = mval.mv_data;
    *value_size = mval.mv_size;
    
    return 0;
}

bool wtree_exists_txn(wtree_txn_t *txn, wtree_tree_t *tree,
                     const void *key, size_t key_size, gerror_t *error) {
    if (!txn || !tree || !key) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return false;
    }
    
    MDB_val mkey = {.mv_size = key_size, .mv_data = (void*)key};
    MDB_val mval;
    
    int rc = mdb_get(txn->txn, tree->dbi, &mkey, &mval);
    return (rc == 0);
}

// ============= Data Operations (Auto-transaction) =============

int wtree_insert_one(wtree_tree_t *tree, const void *key, size_t key_size, 
                     const void *value, size_t value_size, gerror_t *error) {
    wtree_txn_t *txn = wtree_txn_begin(tree->db, true, error);
    if (!txn) return -1;
    
    int rc = wtree_insert_one_txn(txn, tree, key, key_size, value, value_size, error);
    
    if (rc == 0) {
        rc = wtree_txn_commit(txn, error);
    } else {
        wtree_txn_abort(txn);
    }
    
    return rc;
}

int wtree_update(wtree_tree_t *tree, const void *key, size_t key_size,
                const void *value, size_t value_size, gerror_t *error) {
    wtree_txn_t *txn = wtree_txn_begin(tree->db, true, error);
    if (!txn) return -1;
    
    int rc = wtree_update_txn(txn, tree, key, key_size, value, value_size, error);
    
    if (rc == 0) {
        rc = wtree_txn_commit(txn, error);
    } else {
        wtree_txn_abort(txn);
    }
    
    return rc;
}

int wtree_delete_one(wtree_tree_t *tree, const void *key, size_t key_size, 
                     bool *deleted, gerror_t *error) {
    wtree_txn_t *txn = wtree_txn_begin(tree->db, true, error);
    if (!txn) return -1;
    
    int rc = wtree_delete_one_txn(txn, tree, key, key_size, deleted, error);
    
    if (rc == 0) {
        rc = wtree_txn_commit(txn, error);
    } else {
        wtree_txn_abort(txn);
    }
    
    return rc;
}

int wtree_get(wtree_tree_t *tree, const void *key, size_t key_size,
             void **value, size_t *value_size, gerror_t *error) {
    if (!tree || !key || !value || !value_size) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return -1;
    }
    
    wtree_txn_t *txn = wtree_txn_begin(tree->db, false, error);
    if (!txn) return -1;
    
    const void *tmp_value;
    size_t tmp_size;
    
    int rc = wtree_get_txn(txn, tree, key, key_size, &tmp_value, &tmp_size, error);
    
    if (rc == 0) {
        // Copy data before closing transaction
        *value = malloc(tmp_size);
        if (!*value) {
            set_error(error, WTREE_LIB, ENOMEM, "Failed to allocate memory for value");
            wtree_txn_abort(txn);
            return -1;
        }
        memcpy(*value, tmp_value, tmp_size);
        *value_size = tmp_size;
    }
    
    wtree_txn_abort(txn);  // Read-only transactions can be aborted
    return rc;
}

bool wtree_exists(wtree_tree_t *tree, const void *key, size_t key_size, gerror_t *error) {
    if (!tree || !key) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return false;
    }
    
    wtree_txn_t *txn = wtree_txn_begin(tree->db, false, error);
    if (!txn) return false;
    
    bool exists = wtree_exists_txn(txn, tree, key, key_size, error);
    
    wtree_txn_abort(txn);  // Read-only transactions can be aborted
    return exists;
}

// ============= Iterator Operations =============

wtree_iterator_t* wtree_iterator_create(wtree_tree_t *tree, gerror_t *error) {
    if (!tree) {
        set_error(error, WTREE_LIB, EINVAL, "Tree cannot be NULL");
        return NULL;
    }
    
    // Create a read-only transaction for the iterator
    wtree_txn_t *txn = wtree_txn_begin(tree->db, false, error);
    if (!txn) {
        return NULL;
    }
    
    wtree_iterator_t *iter = calloc(1, sizeof(wtree_iterator_t));
    if (!iter) {
        set_error(error, WTREE_LIB, ENOMEM, "Failed to allocate memory for iterator");
        wtree_txn_abort(txn);
        return NULL;
    }
    
    // Open cursor with the transaction
    int rc = mdb_cursor_open(txn->txn, tree->dbi, &iter->cursor);
    if (rc != 0) {
        translate_mdb_error(rc, error);
        free(iter);
        wtree_txn_abort(txn);
        return NULL;
    }
    
    iter->txn = txn;
    iter->tree = tree;
    iter->valid = false;
    iter->owns_txn = true;
    
    // Initialize MDB_val structures
    iter->current_key.mv_size = 0;
    iter->current_key.mv_data = NULL;
    iter->current_val.mv_size = 0;
    iter->current_val.mv_data = NULL;
    
    return iter;
}

wtree_iterator_t* wtree_iterator_create_with_txn(wtree_tree_t *tree, wtree_txn_t *txn, gerror_t *error) {
    if (!tree || !txn) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return NULL;
    }
    
    wtree_iterator_t *iter = calloc(1, sizeof(wtree_iterator_t));
    if (!iter) {
        set_error(error, WTREE_LIB, ENOMEM, "Failed to allocate memory for iterator");
        return NULL;
    }
    
    int rc = mdb_cursor_open(txn->txn, tree->dbi, &iter->cursor);
    if (rc != 0) {
        translate_mdb_error(rc, error);
        free(iter);
        return NULL;
    }
    
    iter->txn = txn;
    iter->tree = tree;
    iter->valid = false;
    iter->owns_txn = false;
    
    // Initialize MDB_val structures
    iter->current_key.mv_size = 0;
    iter->current_key.mv_data = NULL;
    iter->current_val.mv_size = 0;
    iter->current_val.mv_data = NULL;
    
    return iter;
}

bool wtree_iterator_first(wtree_iterator_t *iter) {
    if (!iter || !iter->cursor) return false;
    
    // Reset the MDB_val structures
    iter->current_key.mv_size = 0;
    iter->current_key.mv_data = NULL;
    iter->current_val.mv_size = 0;
    iter->current_val.mv_data = NULL;
    
    int rc = mdb_cursor_get(iter->cursor, &iter->current_key, &iter->current_val, MDB_FIRST);
    iter->valid = (rc == 0);
    return iter->valid;
}

bool wtree_iterator_last(wtree_iterator_t *iter) {
    if (!iter || !iter->cursor) return false;
    
    // Reset the MDB_val structures
    iter->current_key.mv_size = 0;
    iter->current_key.mv_data = NULL;
    iter->current_val.mv_size = 0;
    iter->current_val.mv_data = NULL;
    
    int rc = mdb_cursor_get(iter->cursor, &iter->current_key, &iter->current_val, MDB_LAST);
    iter->valid = (rc == 0);
    return iter->valid;
}

bool wtree_iterator_next(wtree_iterator_t *iter) {
    if (!iter || !iter->cursor) return false;
    
    // Don't reset MDB_val here - LMDB needs current position
    int rc = mdb_cursor_get(iter->cursor, &iter->current_key, &iter->current_val, MDB_NEXT);
    iter->valid = (rc == 0);
    return iter->valid;
}

bool wtree_iterator_prev(wtree_iterator_t *iter) {
    if (!iter || !iter->cursor) return false;
    
    // Don't reset MDB_val here - LMDB needs current position
    int rc = mdb_cursor_get(iter->cursor, &iter->current_key, &iter->current_val, MDB_PREV);
    iter->valid = (rc == 0);
    return iter->valid;
}

bool wtree_iterator_seek(wtree_iterator_t *iter, const void *key, size_t key_size) {
    if (!iter || !iter->cursor || !key) return false;
    
    // Create a local MDB_val for the search key
    MDB_val search_key = {.mv_size = key_size, .mv_data = (void*)key};
    MDB_val found_val = {0};
    
    // MDB_SET will modify search_key to contain the actual found key
    int rc = mdb_cursor_get(iter->cursor, &search_key, &found_val, MDB_SET);
    
    if (rc == 0) {
        // Copy the returned values to our iterator's storage
        iter->current_key = search_key;  // Now contains the found key
        iter->current_val = found_val;   // Contains the found value
        iter->valid = true;
    } else {
        iter->valid = false;
    }
    
    return iter->valid;
}

bool wtree_iterator_seek_range(wtree_iterator_t *iter, const void *key, size_t key_size) {
    if (!iter || !iter->cursor || !key) return false;
    
    // Create a local MDB_val for the search key
    MDB_val search_key = {.mv_size = key_size, .mv_data = (void*)key};
    MDB_val found_val = {0};
    
    // MDB_SET_RANGE will find key >= search_key
    int rc = mdb_cursor_get(iter->cursor, &search_key, &found_val, MDB_SET_RANGE);
    
    if (rc == 0) {
        // Copy the returned values to our iterator's storage
        iter->current_key = search_key;  // Now contains the found key (>= original)
        iter->current_val = found_val;   // Contains the found value
        iter->valid = true;
    } else {
        iter->valid = false;
    }
    
    return iter->valid;
}

bool wtree_iterator_key(wtree_iterator_t *iter, const void **key, size_t *key_size) {
    if (!iter || !iter->valid || !key || !key_size) return false;
    
    *key = iter->current_key.mv_data;
    *key_size = iter->current_key.mv_size;
    return true;
}

bool wtree_iterator_value(wtree_iterator_t *iter, const void **value, size_t *value_size) {
    if (!iter || !iter->valid || !value || !value_size) return false;
    
    *value = iter->current_val.mv_data;
    *value_size = iter->current_val.mv_size;
    return true;
}

bool wtree_iterator_key_copy(wtree_iterator_t *iter, void **key, size_t *key_size) {
    if (!iter || !iter->valid || !key || !key_size) return false;
    
    *key_size = iter->current_key.mv_size;
    *key = malloc(*key_size);
    if (!*key) return false;
    
    memcpy(*key, iter->current_key.mv_data, *key_size);
    return true;
}

bool wtree_iterator_value_copy(wtree_iterator_t *iter, void **value, size_t *value_size) {
    if (!iter || !iter->valid || !value || !value_size) return false;
    
    *value_size = iter->current_val.mv_size;
    *value = malloc(*value_size);
    if (!*value) return false;
    
    memcpy(*value, iter->current_val.mv_data, *value_size);
    return true;
}

bool wtree_iterator_valid(wtree_iterator_t *iter) {
    return iter && iter->valid;
}

int wtree_iterator_delete(wtree_iterator_t *iter, gerror_t *error) {
    if (!iter || !iter->cursor) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid iterator");
        return -1;
    }
    
    if (!iter->valid) {
        set_error(error, WTREE_LIB, EINVAL, "Iterator not positioned on a valid entry");
        return -1;
    }
    
    if (!iter->txn->is_write) {
        set_error(error, WTREE_LIB, EINVAL, "Delete requires write transaction");
        return -1;
    }
    
    int rc = mdb_cursor_del(iter->cursor, 0);
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }
    
    // After delete, cursor position is undefined, try to move to next
    iter->valid = false;
    rc = mdb_cursor_get(iter->cursor, &iter->current_key, &iter->current_val, MDB_GET_CURRENT);
    if (rc == MDB_NOTFOUND) {
        // Current was deleted, try next
        rc = mdb_cursor_get(iter->cursor, &iter->current_key, &iter->current_val, MDB_NEXT);
    }
    iter->valid = (rc == 0);
    
    return 0;
}

void wtree_iterator_close(wtree_iterator_t *iter) {
    if (!iter) return;
    
    if (iter->cursor) {
        mdb_cursor_close(iter->cursor);
    }
    
    // If iterator owns the transaction, clean it up
    if (iter->owns_txn && iter->txn) {
        wtree_txn_abort(iter->txn);
    }
    
    free(iter);
}

// ============= Utility Functions =============

const char* wtree_strerror(int error_code) {
    switch (error_code) {
        case WTREE_MAP_FULL:
            return "Database map is full, resize needed";
        case WTREE_TXN_FULL:
            return "Transaction has too many dirty pages";
        case WTREE_KEY_NOT_FOUND:
            return "Key not found";
        default:
            return mdb_strerror(error_code);
    }
}

bool wtree_error_recoverable(int error_code) {
    return (error_code == WTREE_MAP_FULL || 
            error_code == WTREE_TXN_FULL ||
            error_code == MDB_MAP_RESIZED);
}