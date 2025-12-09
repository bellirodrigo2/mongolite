#include "wtree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include <errno.h>


#ifdef _WIN32
    #include <direct.h>   // _mkdir, _rmdir
    #include <io.h>       // _unlink
    #define wtree_mkdir(path) _mkdir(path)
    #define wtree_unlink(path) _unlink(path)
    #define wtree_rmdir(path) _rmdir(path)
#else
    #include <unistd.h>  // unlink, rmdir
    #define wtree_mkdir(path) mkdir(path, 0755)
    #define wtree_unlink(path) unlink(path)
    #define wtree_rmdir(path) rmdir(path)
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

struct wtree_kv_t {
    void *key;
    size_t key_size;
    void *value;
    size_t value_size;
};

// ============= Database Operations =============

wtree_db_t* wtree_db_create(const char *path, size_t mapsize, unsigned int max_dbs, gerror_t *error) {
    if (!path) {
        set_error(error, WTREE_LIB, EINVAL, "Database path cannot be NULL");
        return NULL;
    }
    
    wtree_db_t *db = calloc(1, sizeof(wtree_db_t));
    if (!db) {
        set_error(error, WTREE_LIB, ENOMEM, "Failed to allocate memory for database");
        return NULL;
    }
    
    // Create directory if it doesn't exist
    struct stat st = {0};
    if (stat(path, &st) == -1) {
        if (wtree_mkdir(path) != 0) {
            set_error(error, WTREE_LIB, errno, "Failed to create directory: %s", strerror(errno));
            free(db);
            return NULL;
        }
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
    rc = mdb_env_open(db->env, path, MDB_NOTLS, 0664);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to open environment: %s", mdb_strerror(rc));
        mdb_env_close(db->env);
        free(db);
        return NULL;
    }
    
    db->path = strdup(path);
    db->mapsize = mapsize;
    db->max_dbs = max_dbs;
    db->flags = MDB_NOTLS;
    
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

int wtree_db_delete(const char *path, gerror_t *error) {
    if (!path) {
        set_error(error, WTREE_LIB, EINVAL, "Database path cannot be NULL");
        return -1;
    }
    
    // Remove all LMDB files
    char filepath[512];
    
    snprintf(filepath, sizeof(filepath), "%s/data.mdb", path);
    wtree_unlink(filepath);
    
    snprintf(filepath, sizeof(filepath), "%s/lock.mdb", path);
    wtree_unlink(filepath);
    
    // Remove directory
    if (wtree_rmdir(path) != 0) {
        set_error(error, WTREE_LIB, errno, "Failed to remove directory: %s", strerror(errno));
        return -1;
    }
    
    return 0;
}

int wtree_db_stats(wtree_db_t *db, MDB_stat *stat, gerror_t *error) {
    if (!db || !stat) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return -1;
    }
    
    int rc = mdb_env_stat(db->env, stat);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to get stats: %s", mdb_strerror(rc));
        return -1;
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
        set_error(error, WTREE_LIB, rc, "Failed to sync: %s", mdb_strerror(rc));
        return -1;
    }
    
    return 0;
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
        set_error(error, WTREE_LIB, rc, "Failed to begin transaction: %s", mdb_strerror(rc));
        free(tree);
        return NULL;
    }
    
    rc = mdb_dbi_open(txn, name, MDB_CREATE | flags, &tree->dbi);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to open database: %s", mdb_strerror(rc));
        mdb_txn_abort(txn);
        free(tree);
        return NULL;
    }
    
    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to commit transaction: %s", mdb_strerror(rc));
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
        set_error(error, WTREE_LIB, rc, "Failed to begin transaction: %s", mdb_strerror(rc));
        return -1;
    }
    
    rc = mdb_set_compare(txn, tree->dbi, cmp);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to set compare function: %s", mdb_strerror(rc));
        mdb_txn_abort(txn);
        return -1;
    }
    
    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to commit transaction: %s", mdb_strerror(rc));
        return -1;
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
        set_error(error, WTREE_LIB, rc, "Failed to begin transaction: %s", mdb_strerror(rc));
        return -1;
    }
    
    rc = mdb_set_dupsort(txn, tree->dbi, cmp);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to set dupsort function: %s", mdb_strerror(rc));
        mdb_txn_abort(txn);
        return -1;
    }
    
    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to commit transaction: %s", mdb_strerror(rc));
        return -1;
    }
    
    tree->dup_cmp_func = cmp;
    return 0;
}

char** wtree_tree_list(wtree_db_t *db, size_t *count, gerror_t *error) {
    if (!db || !count) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return NULL;
    }
    
    *count = 0;
    
    MDB_txn *txn;
    int rc = mdb_txn_begin(db->env, NULL, MDB_RDONLY, &txn);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to begin transaction: %s", mdb_strerror(rc));
        return NULL;
    }
    
    // Open main database
    MDB_dbi dbi;
    rc = mdb_dbi_open(txn, NULL, 0, &dbi);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to open main database: %s", mdb_strerror(rc));
        mdb_txn_abort(txn);
        return NULL;
    }
    
    // Count entries
    MDB_stat stat;
    rc = mdb_stat(txn, dbi, &stat);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to get stats: %s", mdb_strerror(rc));
        mdb_txn_abort(txn);
        return NULL;
    }
    
    if (stat.ms_entries == 0) {
        mdb_txn_abort(txn);
        return NULL;
    }
    
    // Allocate array
    char **list = calloc(stat.ms_entries + 1, sizeof(char*));
    if (!list) {
        set_error(error, WTREE_LIB, ENOMEM, "Failed to allocate memory for list");
        mdb_txn_abort(txn);
        return NULL;
    }
    
    // Iterate through databases
    MDB_cursor *cursor;
    rc = mdb_cursor_open(txn, dbi, &cursor);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to open cursor: %s", mdb_strerror(rc));
        free(list);
        mdb_txn_abort(txn);
        return NULL;
    }
    
    MDB_val key, data;
    size_t idx = 0;
    
    while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
        if (idx >= stat.ms_entries) break;
        
        list[idx] = malloc(key.mv_size + 1);
        if (list[idx]) {
            memcpy(list[idx], key.mv_data, key.mv_size);
            list[idx][key.mv_size] = '\0';
            idx++;
        }
    }
    
    *count = idx;
    
    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
    
    return list;
}

void wtree_tree_list_free(char **list, size_t count) {
    if (!list) return;
    
    for (size_t i = 0; i < count; i++) {
        free(list[i]);
    }
    free(list);
}

int wtree_tree_delete(wtree_db_t *db, const char *name, gerror_t *error) {
    if (!db || !name) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return -1;
    }
    
    MDB_txn *txn;
    int rc = mdb_txn_begin(db->env, NULL, 0, &txn);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to begin transaction: %s", mdb_strerror(rc));
        return -1;
    }
    
    MDB_dbi dbi;
    rc = mdb_dbi_open(txn, name, 0, &dbi);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to open database: %s", mdb_strerror(rc));
        mdb_txn_abort(txn);
        return -1;
    }
    
    rc = mdb_drop(txn, dbi, 1); // 1 = delete database
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to drop database: %s", mdb_strerror(rc));
        mdb_txn_abort(txn);
        return -1;
    }
    
    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to commit transaction: %s", mdb_strerror(rc));
        return -1;
    }
    
    return 0;
}

void wtree_tree_close(wtree_tree_t *tree) {
    if (!tree) return;
    
    if (tree->db && tree->db->env) {
        mdb_dbi_close(tree->db->env, tree->dbi);
    }
    
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
        set_error(error, WTREE_LIB, rc, "Failed to begin transaction: %s", mdb_strerror(rc));
        free(txn);
        return NULL;
    }
    
    txn->db = db;
    txn->is_write = write;
    
    return txn;
}

int wtree_txn_commit(wtree_txn_t *txn, gerror_t *error) {
    if (!txn) {
        set_error(error, WTREE_LIB, EINVAL, "Transaction cannot be NULL");
        return -1;
    }
    
    int rc = mdb_txn_commit(txn->txn);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to commit transaction: %s", mdb_strerror(rc));
        free(txn);
        return -1;
    }
    
    free(txn);
    return 0;
}

void wtree_txn_abort(wtree_txn_t *txn) {
    if (!txn) return;
    
    mdb_txn_abort(txn->txn);
    free(txn);
}

// ============= Data Operations =============

int wtree_insert_one(wtree_tree_t *tree, const void *key, size_t key_size,
                     const void *value, size_t value_size, gerror_t *error) {
    if (!tree || !key || !value) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return -1;
    }
    
    MDB_txn *txn;
    int rc = mdb_txn_begin(tree->db->env, NULL, 0, &txn);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to begin transaction: %s", mdb_strerror(rc));
        return -1;
    }
    
    MDB_val mkey = {.mv_size = key_size, .mv_data = (void*)key};
    MDB_val mval = {.mv_size = value_size, .mv_data = (void*)value};
    
    rc = mdb_put(txn, tree->dbi, &mkey, &mval, MDB_NOOVERWRITE);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to insert: %s", mdb_strerror(rc));
        mdb_txn_abort(txn);
        return -1;
    }
    
    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to commit: %s", mdb_strerror(rc));
        return -1;
    }
    
    return 0;
}

int wtree_insert_many(wtree_tree_t *tree, wtree_kv_t *kvs, size_t count, gerror_t *error) {
    if (!tree || !kvs || count == 0) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return -1;
    }
    
    MDB_txn *txn;
    int rc = mdb_txn_begin(tree->db->env, NULL, 0, &txn);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to begin transaction: %s", mdb_strerror(rc));
        return -1;
    }
    
    for (size_t i = 0; i < count; i++) {
        MDB_val mkey = {.mv_size = kvs[i].key_size, .mv_data = kvs[i].key};
        MDB_val mval = {.mv_size = kvs[i].value_size, .mv_data = kvs[i].value};
        
        rc = mdb_put(txn, tree->dbi, &mkey, &mval, MDB_NOOVERWRITE);
        if (rc != 0 && rc != MDB_KEYEXIST) {
            set_error(error, WTREE_LIB, rc, "Failed to insert key at index %zu: %s", i, mdb_strerror(rc));
            mdb_txn_abort(txn);
            return -1;
        }
    }
    
    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to commit: %s", mdb_strerror(rc));
        return -1;
    }
    
    return 0;
}

int wtree_update(wtree_tree_t *tree, const void *key, size_t key_size,
                const void *value, size_t value_size, gerror_t *error) {
    if (!tree || !key || !value) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return -1;
    }
    
    MDB_txn *txn;
    int rc = mdb_txn_begin(tree->db->env, NULL, 0, &txn);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to begin transaction: %s", mdb_strerror(rc));
        return -1;
    }
    
    MDB_val mkey = {.mv_size = key_size, .mv_data = (void*)key};
    MDB_val mval = {.mv_size = value_size, .mv_data = (void*)value};
    
    rc = mdb_put(txn, tree->dbi, &mkey, &mval, 0);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to update: %s", mdb_strerror(rc));
        mdb_txn_abort(txn);
        return -1;
    }
    
    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to commit: %s", mdb_strerror(rc));
        return -1;
    }
    
    return 0;
}

int wtree_delete_one(wtree_tree_t *tree, const void *key, size_t key_size, gerror_t *error) {
    if (!tree || !key) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return -1;
    }
    
    MDB_txn *txn;
    int rc = mdb_txn_begin(tree->db->env, NULL, 0, &txn);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to begin transaction: %s", mdb_strerror(rc));
        return -1;
    }
    
    MDB_val mkey = {.mv_size = key_size, .mv_data = (void*)key};
    
    rc = mdb_del(txn, tree->dbi, &mkey, NULL);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to delete: %s", mdb_strerror(rc));
        mdb_txn_abort(txn);
        return -1;
    }
    
    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to commit: %s", mdb_strerror(rc));
        return -1;
    }
    
    return 0;
}

int wtree_get(wtree_tree_t *tree, const void *key, size_t key_size,
             void **value, size_t *value_size, gerror_t *error) {
    if (!tree || !key || !value || !value_size) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return -1;
    }
    
    MDB_txn *txn;
    int rc = mdb_txn_begin(tree->db->env, NULL, MDB_RDONLY, &txn);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to begin transaction: %s", mdb_strerror(rc));
        return -1;
    }
    
    MDB_val mkey = {.mv_size = key_size, .mv_data = (void*)key};
    MDB_val mval;
    
    rc = mdb_get(txn, tree->dbi, &mkey, &mval);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to get: %s", mdb_strerror(rc));
        mdb_txn_abort(txn);
        return -1;
    }
    
    *value = malloc(mval.mv_size);
    if (!*value) {
        set_error(error, WTREE_LIB, ENOMEM, "Failed to allocate memory for value");
        mdb_txn_abort(txn);
        return -1;
    }
    
    memcpy(*value, mval.mv_data, mval.mv_size);
    *value_size = mval.mv_size;
    
    mdb_txn_abort(txn);
    return 0;
}

bool wtree_exists(wtree_tree_t *tree, const void *key, size_t key_size, gerror_t *error) {
    if (!tree || !key) {
        set_error(error, WTREE_LIB, EINVAL, "Invalid parameters");
        return false;
    }
    
    MDB_txn *txn;
    int rc = mdb_txn_begin(tree->db->env, NULL, MDB_RDONLY, &txn);
    if (rc != 0) {
        set_error(error, WTREE_LIB, rc, "Failed to begin transaction: %s", mdb_strerror(rc));
        return false;
    }
    
    MDB_val mkey = {.mv_size = key_size, .mv_data = (void*)key};
    MDB_val mval;
    
    rc = mdb_get(txn, tree->dbi, &mkey, &mval);
    mdb_txn_abort(txn);
    
    return (rc == 0);
}

// ============= Iterator Operations =============

wtree_iterator_t* wtree_iterator_create(wtree_tree_t *tree, gerror_t *error) {
    if (!tree) {
        set_error(error, WTREE_LIB, EINVAL, "Tree cannot be NULL");
        return NULL;
    }
    
    wtree_txn_t *txn = wtree_txn_begin(tree->db, false, error);
    if (!txn) {
        return NULL;
    }
    
    wtree_iterator_t *iter = wtree_iterator_create_with_txn(tree, txn, error);
    if (!iter) {
        wtree_txn_abort(txn);
        return NULL;
    }
    
    iter->owns_txn = true;  // Iterator owns this transaction
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
        set_error(error, WTREE_LIB, rc, "Failed to open cursor: %s", mdb_strerror(rc));
        free(iter);
        return NULL;
    }
    
    iter->txn = txn;
    iter->tree = tree;
    iter->valid = false;
    iter->owns_txn = false;  // Using external transaction
    
    return iter;
}

bool wtree_iterator_first(wtree_iterator_t *iter) {
    if (!iter || !iter->cursor) return false;
    
    int rc = mdb_cursor_get(iter->cursor, &iter->current_key, &iter->current_val, MDB_FIRST);
    iter->valid = (rc == 0);
    return iter->valid;
}

bool wtree_iterator_last(wtree_iterator_t *iter) {
    if (!iter || !iter->cursor) return false;
    
    int rc = mdb_cursor_get(iter->cursor, &iter->current_key, &iter->current_val, MDB_LAST);
    iter->valid = (rc == 0);
    return iter->valid;
}

bool wtree_iterator_next(wtree_iterator_t *iter) {
    if (!iter || !iter->cursor) return false;
    
    int rc = mdb_cursor_get(iter->cursor, &iter->current_key, &iter->current_val, MDB_NEXT);
    iter->valid = (rc == 0);
    return iter->valid;
}

bool wtree_iterator_prev(wtree_iterator_t *iter) {
    if (!iter || !iter->cursor) return false;
    
    int rc = mdb_cursor_get(iter->cursor, &iter->current_key, &iter->current_val, MDB_PREV);
    iter->valid = (rc == 0);
    return iter->valid;
}

bool wtree_iterator_seek(wtree_iterator_t *iter, const void *key, size_t key_size) {
    if (!iter || !iter->cursor || !key) return false;
    
    MDB_val mkey = {.mv_size = key_size, .mv_data = (void*)key};
    int rc = mdb_cursor_get(iter->cursor, &mkey, &iter->current_val, MDB_SET);
    
    if (rc == 0) {
        iter->current_key = mkey;
        iter->valid = true;
    } else {
        iter->valid = false;
    }
    
    return iter->valid;
}

bool wtree_iterator_seek_range(wtree_iterator_t *iter, const void *key, size_t key_size) {
    if (!iter || !iter->cursor || !key) return false;
    
    MDB_val mkey = {.mv_size = key_size, .mv_data = (void*)key};
    int rc = mdb_cursor_get(iter->cursor, &mkey, &iter->current_val, MDB_SET_RANGE);
    
    if (rc == 0) {
        iter->current_key = mkey;
        iter->valid = true;
    } else {
        iter->valid = false;
    }
    
    return iter->valid;
}

bool wtree_iterator_key(wtree_iterator_t *iter, void **key, size_t *key_size) {
    if (!iter || !iter->valid || !key || !key_size) return false;
    
    *key = iter->current_key.mv_data;
    *key_size = iter->current_key.mv_size;
    return true;
}

bool wtree_iterator_value(wtree_iterator_t *iter, void **value, size_t *value_size) {
    if (!iter || !iter->valid || !value || !value_size) return false;
    
    *value = iter->current_val.mv_data;
    *value_size = iter->current_val.mv_size;
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
        set_error(error, WTREE_LIB, rc, "Failed to delete: %s", mdb_strerror(rc));
        return -1;
    }
    
    // Move to next after delete
    iter->valid = false;
    wtree_iterator_next(iter);
    
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
