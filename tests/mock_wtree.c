/**
 * mock_wtree.c - Mock implementation of wtree for unit testing
 *
 * This file provides mock implementations of all wtree functions.
 * It uses a simple in-memory store and tracks call counts for verification.
 */

#include "mock_wtree.h"
#include "wtree.h"
#include <stdlib.h>
#include <string.h>

/* Global mock state */
mock_wtree_state_t g_mock_wtree_state = {0};

/* ============================================================
 * Mock Control Functions
 * ============================================================ */

void mock_wtree_reset(void) {
    mock_wtree_store_clear();
    memset(&g_mock_wtree_state, 0, sizeof(g_mock_wtree_state));
}

void mock_wtree_store_put_for_tree(void *tree, const void *key, size_t key_size,
                                   const void *value, size_t value_size) {
    /* Grow store if needed */
    if (g_mock_wtree_state.store_count >= g_mock_wtree_state.store_capacity) {
        size_t new_cap = g_mock_wtree_state.store_capacity == 0 ? 16 : g_mock_wtree_state.store_capacity * 2;
        g_mock_wtree_state.store = realloc(g_mock_wtree_state.store,
                                            new_cap * sizeof(g_mock_wtree_state.store[0]));
        g_mock_wtree_state.store_capacity = new_cap;
    }

    /* Check if key exists for this tree (update) */
    for (size_t i = 0; i < g_mock_wtree_state.store_count; i++) {
        if (g_mock_wtree_state.store[i].tree == tree &&
            g_mock_wtree_state.store[i].key_size == key_size &&
            memcmp(g_mock_wtree_state.store[i].key, key, key_size) == 0) {
            /* Update existing */
            free(g_mock_wtree_state.store[i].value);
            g_mock_wtree_state.store[i].value = malloc(value_size);
            memcpy(g_mock_wtree_state.store[i].value, value, value_size);
            g_mock_wtree_state.store[i].value_size = value_size;
            return;
        }
    }

    /* Insert new */
    size_t idx = g_mock_wtree_state.store_count++;
    g_mock_wtree_state.store[idx].key = malloc(key_size);
    memcpy(g_mock_wtree_state.store[idx].key, key, key_size);
    g_mock_wtree_state.store[idx].key_size = key_size;
    g_mock_wtree_state.store[idx].value = malloc(value_size);
    memcpy(g_mock_wtree_state.store[idx].value, value, value_size);
    g_mock_wtree_state.store[idx].value_size = value_size;
    g_mock_wtree_state.store[idx].tree = tree;
}

void mock_wtree_store_put(const void *key, size_t key_size,
                          const void *value, size_t value_size) {
    mock_wtree_store_put_for_tree(NULL, key, key_size, value, value_size);
}

static bool mock_wtree_store_get_for_tree(void *tree, const void *key, size_t key_size,
                                          void **value, size_t *value_size) {
    for (size_t i = 0; i < g_mock_wtree_state.store_count; i++) {
        if (g_mock_wtree_state.store[i].tree == tree &&
            g_mock_wtree_state.store[i].key_size == key_size &&
            memcmp(g_mock_wtree_state.store[i].key, key, key_size) == 0) {
            if (value) *value = g_mock_wtree_state.store[i].value;
            if (value_size) *value_size = g_mock_wtree_state.store[i].value_size;
            return true;
        }
    }
    return false;
}

bool mock_wtree_store_get(const void *key, size_t key_size,
                          void **value, size_t *value_size) {
    /* Legacy: search across all trees */
    for (size_t i = 0; i < g_mock_wtree_state.store_count; i++) {
        if (g_mock_wtree_state.store[i].key_size == key_size &&
            memcmp(g_mock_wtree_state.store[i].key, key, key_size) == 0) {
            if (value) *value = g_mock_wtree_state.store[i].value;
            if (value_size) *value_size = g_mock_wtree_state.store[i].value_size;
            return true;
        }
    }
    return false;
}

static bool mock_wtree_store_delete_for_tree(void *tree, const void *key, size_t key_size) {
    for (size_t i = 0; i < g_mock_wtree_state.store_count; i++) {
        if (g_mock_wtree_state.store[i].tree == tree &&
            g_mock_wtree_state.store[i].key_size == key_size &&
            memcmp(g_mock_wtree_state.store[i].key, key, key_size) == 0) {
            free(g_mock_wtree_state.store[i].key);
            free(g_mock_wtree_state.store[i].value);
            /* Move last element to this position */
            if (i < g_mock_wtree_state.store_count - 1) {
                g_mock_wtree_state.store[i] = g_mock_wtree_state.store[g_mock_wtree_state.store_count - 1];
            }
            g_mock_wtree_state.store_count--;
            return true;
        }
    }
    return false;
}

bool mock_wtree_store_delete(const void *key, size_t key_size) {
    /* Legacy: search across all trees */
    for (size_t i = 0; i < g_mock_wtree_state.store_count; i++) {
        if (g_mock_wtree_state.store[i].key_size == key_size &&
            memcmp(g_mock_wtree_state.store[i].key, key, key_size) == 0) {
            free(g_mock_wtree_state.store[i].key);
            free(g_mock_wtree_state.store[i].value);
            /* Move last element to this position */
            if (i < g_mock_wtree_state.store_count - 1) {
                g_mock_wtree_state.store[i] = g_mock_wtree_state.store[g_mock_wtree_state.store_count - 1];
            }
            g_mock_wtree_state.store_count--;
            return true;
        }
    }
    return false;
}

void mock_wtree_store_clear(void) {
    for (size_t i = 0; i < g_mock_wtree_state.store_count; i++) {
        free(g_mock_wtree_state.store[i].key);
        free(g_mock_wtree_state.store[i].value);
    }
    free(g_mock_wtree_state.store);
    g_mock_wtree_state.store = NULL;
    g_mock_wtree_state.store_count = 0;
    g_mock_wtree_state.store_capacity = 0;
}

void mock_wtree_inject_error(int error_code) {
    g_mock_wtree_state.error_code_to_inject = error_code;
}

void mock_wtree_fail_next_txn_begin(int error_code) {
    g_mock_wtree_state.fail_next_txn_begin = true;
    g_mock_wtree_state.error_code_to_inject = error_code;
}

void mock_wtree_fail_next_txn_commit(int error_code) {
    g_mock_wtree_state.fail_next_txn_commit = true;
    g_mock_wtree_state.error_code_to_inject = error_code;
}

void mock_wtree_fail_next_insert(int error_code) {
    g_mock_wtree_state.fail_next_insert = true;
    g_mock_wtree_state.error_code_to_inject = error_code;
}

void mock_wtree_fail_next_get(int error_code) {
    g_mock_wtree_state.fail_next_get = true;
    g_mock_wtree_state.error_code_to_inject = error_code;
}

/* ============================================================
 * Mock Handle Creators
 * ============================================================ */

mock_wtree_db_t* mock_wtree_create_db_handle(const char *path, size_t mapsize) {
    mock_wtree_db_t *db = calloc(1, sizeof(mock_wtree_db_t));
    db->path = strdup(path);
    db->mapsize = mapsize;
    db->max_dbs = 128;
    return db;
}

mock_wtree_tree_t* mock_wtree_create_tree_handle(mock_wtree_db_t *db, const char *name) {
    mock_wtree_tree_t *tree = calloc(1, sizeof(mock_wtree_tree_t));
    tree->db = db;
    tree->name = strdup(name);
    return tree;
}

mock_wtree_txn_t* mock_wtree_create_txn_handle(mock_wtree_db_t *db, bool write) {
    mock_wtree_txn_t *txn = calloc(1, sizeof(mock_wtree_txn_t));
    txn->db = db;
    txn->write = write;
    return txn;
}

mock_wtree_iterator_t* mock_wtree_create_iterator_handle(mock_wtree_tree_t *tree) {
    mock_wtree_iterator_t *iter = calloc(1, sizeof(mock_wtree_iterator_t));
    iter->tree = tree;
    iter->position = -1;
    return iter;
}

void mock_wtree_free_db_handle(mock_wtree_db_t *db) {
    if (db) {
        free(db->path);
        free(db);
    }
}

void mock_wtree_free_tree_handle(mock_wtree_tree_t *tree) {
    if (tree) {
        free(tree->name);
        free(tree);
    }
}

void mock_wtree_free_txn_handle(mock_wtree_txn_t *txn) {
    free(txn);
}

void mock_wtree_free_iterator_handle(mock_wtree_iterator_t *iter) {
    free(iter);
}

/* ============================================================
 * Mock wtree API Implementation
 * ============================================================ */

/* Database Operations */

wtree_db_t* wtree_db_create(const char *path, size_t mapsize, unsigned int max_dbs,
                            unsigned int flags, gerror_t *error) {
    g_mock_wtree_state.db_create_count++;

    mock_wtree_db_t *db = mock_wtree_create_db_handle(path, mapsize);
    db->max_dbs = max_dbs;
    db->flags = flags;

    return (wtree_db_t*)db;
}

void wtree_db_close(wtree_db_t *db) {
    g_mock_wtree_state.db_close_count++;
    mock_wtree_db_t *mdb = (mock_wtree_db_t*)db;
    if (mdb) {
        mdb->closed = true;
        mock_wtree_free_db_handle(mdb);
    }
}

int wtree_db_stats(wtree_db_t *db, MDB_stat *stat, gerror_t *error) {
    (void)db;
    if (stat) {
        memset(stat, 0, sizeof(MDB_stat));
        stat->ms_entries = g_mock_wtree_state.store_count;
    }
    return 0;
}

int wtree_db_sync(wtree_db_t *db, bool force, gerror_t *error) {
    (void)db;
    (void)force;
    (void)error;
    return 0;
}

int wtree_db_resize(wtree_db_t *db, size_t new_mapsize, gerror_t *error) {
    mock_wtree_db_t *mdb = (mock_wtree_db_t*)db;
    if (mdb) {
        mdb->mapsize = new_mapsize;
    }
    return 0;
}

size_t wtree_db_get_mapsize(wtree_db_t *db) {
    mock_wtree_db_t *mdb = (mock_wtree_db_t*)db;
    return mdb ? mdb->mapsize : 0;
}

/* Tree Operations */

wtree_tree_t* wtree_tree_create(wtree_db_t *db, const char *name, unsigned int flags, gerror_t *error) {
    g_mock_wtree_state.tree_create_count++;

    mock_wtree_db_t *mdb = (mock_wtree_db_t*)db;
    mock_wtree_tree_t *tree = mock_wtree_create_tree_handle(mdb, name);
    tree->flags = flags;

    return (wtree_tree_t*)tree;
}

int wtree_tree_set_compare(wtree_tree_t *tree, MDB_cmp_func *cmp, gerror_t *error) {
    mock_wtree_tree_t *mtree = (mock_wtree_tree_t*)tree;
    if (mtree) {
        mtree->compare_fn = cmp;
    }
    return 0;
}

int wtree_tree_set_dupsort(wtree_tree_t *tree, MDB_cmp_func *cmp, gerror_t *error) {
    mock_wtree_tree_t *mtree = (mock_wtree_tree_t*)tree;
    if (mtree) {
        mtree->dupsort_fn = cmp;
    }
    return 0;
}

int wtree_tree_delete(wtree_db_t *db, const char *name, gerror_t *error) {
    (void)db;
    (void)name;
    (void)error;
    return 0;
}

void wtree_tree_close(wtree_tree_t *tree) {
    g_mock_wtree_state.tree_close_count++;
    mock_wtree_tree_t *mtree = (mock_wtree_tree_t*)tree;
    if (mtree) {
        mtree->closed = true;
        mock_wtree_free_tree_handle(mtree);
    }
}

/* Transaction Operations */

wtree_txn_t* wtree_txn_begin(wtree_db_t *db, bool write, gerror_t *error) {
    g_mock_wtree_state.txn_begin_count++;

    if (g_mock_wtree_state.fail_next_txn_begin) {
        g_mock_wtree_state.fail_next_txn_begin = false;
        if (error) {
            set_error(error, "wtree", g_mock_wtree_state.error_code_to_inject,
                      "Mock: txn_begin failed");
        }
        return NULL;
    }

    mock_wtree_db_t *mdb = (mock_wtree_db_t*)db;
    return (wtree_txn_t*)mock_wtree_create_txn_handle(mdb, write);
}

wtree_txn_t* wtree_txn_begin_nested(wtree_txn_t *parent, gerror_t *error) {
    g_mock_wtree_state.txn_begin_count++;

    mock_wtree_txn_t *mparent = (mock_wtree_txn_t*)parent;
    mock_wtree_txn_t *txn = mock_wtree_create_txn_handle(mparent->db, mparent->write);
    txn->parent = mparent;

    return (wtree_txn_t*)txn;
}

int wtree_txn_commit(wtree_txn_t *txn, gerror_t *error) {
    g_mock_wtree_state.txn_commit_count++;

    if (g_mock_wtree_state.fail_next_txn_commit) {
        g_mock_wtree_state.fail_next_txn_commit = false;
        if (error) {
            set_error(error, "wtree", g_mock_wtree_state.error_code_to_inject,
                      "Mock: txn_commit failed");
        }
        return g_mock_wtree_state.error_code_to_inject;
    }

    mock_wtree_txn_t *mtxn = (mock_wtree_txn_t*)txn;
    if (mtxn) {
        mtxn->committed = true;
        mock_wtree_free_txn_handle(mtxn);
    }
    return 0;
}

void wtree_txn_abort(wtree_txn_t *txn) {
    g_mock_wtree_state.txn_abort_count++;

    mock_wtree_txn_t *mtxn = (mock_wtree_txn_t*)txn;
    if (mtxn) {
        mtxn->aborted = true;
        mock_wtree_free_txn_handle(mtxn);
    }
}

void wtree_txn_reset(wtree_txn_t *txn) {
    mock_wtree_txn_t *mtxn = (mock_wtree_txn_t*)txn;
    if (mtxn) {
        mtxn->reset = true;
    }
}

int wtree_txn_renew(wtree_txn_t *txn, gerror_t *error) {
    mock_wtree_txn_t *mtxn = (mock_wtree_txn_t*)txn;
    if (mtxn) {
        mtxn->reset = false;
    }
    return 0;
}

bool wtree_txn_is_readonly(wtree_txn_t *txn) {
    mock_wtree_txn_t *mtxn = (mock_wtree_txn_t*)txn;
    return mtxn ? !mtxn->write : true;
}

/* Data Operations (Auto-transaction) */

int wtree_insert_one(wtree_tree_t *tree, const void *key, size_t key_size,
                     const void *value, size_t value_size, gerror_t *error) {
    g_mock_wtree_state.insert_count++;

    if (g_mock_wtree_state.fail_next_insert) {
        g_mock_wtree_state.fail_next_insert = false;
        if (error) {
            set_error(error, "wtree", g_mock_wtree_state.error_code_to_inject,
                      "Mock: insert failed");
        }
        return g_mock_wtree_state.error_code_to_inject;
    }

    /* Check for duplicate in this tree */
    if (mock_wtree_store_get_for_tree(tree, key, key_size, NULL, NULL)) {
        if (error) {
            set_error(error, "wtree", MDB_KEYEXIST, "Key already exists");
        }
        return MDB_KEYEXIST;
    }

    mock_wtree_store_put_for_tree(tree, key, key_size, value, value_size);
    return 0;
}

int wtree_update(wtree_tree_t *tree, const void *key, size_t key_size,
                const void *value, size_t value_size, gerror_t *error) {
    g_mock_wtree_state.update_count++;
    mock_wtree_store_put_for_tree(tree, key, key_size, value, value_size);
    return 0;
}

int wtree_delete_one(wtree_tree_t *tree, const void *key, size_t key_size,
                     bool *deleted, gerror_t *error) {
    g_mock_wtree_state.delete_count++;
    bool found = mock_wtree_store_delete_for_tree(tree, key, key_size);
    if (deleted) *deleted = found;
    return 0;
}

int wtree_get(wtree_tree_t *tree, const void *key, size_t key_size,
             void **value, size_t *value_size, gerror_t *error) {
    g_mock_wtree_state.get_count++;

    if (g_mock_wtree_state.fail_next_get) {
        g_mock_wtree_state.fail_next_get = false;
        if (error) {
            set_error(error, "wtree", g_mock_wtree_state.error_code_to_inject,
                      "Mock: get failed");
        }
        return g_mock_wtree_state.error_code_to_inject;
    }

    void *found_value = NULL;
    size_t found_size = 0;
    if (mock_wtree_store_get_for_tree(tree, key, key_size, &found_value, &found_size)) {
        /* Return a copy */
        if (value) {
            *value = malloc(found_size);
            memcpy(*value, found_value, found_size);
        }
        if (value_size) *value_size = found_size;
        return 0;
    }

    return WTREE_KEY_NOT_FOUND;
}

bool wtree_exists(wtree_tree_t *tree, const void *key, size_t key_size, gerror_t *error) {
    return mock_wtree_store_get_for_tree(tree, key, key_size, NULL, NULL);
}

/* Data Operations (With Transaction) */

int wtree_insert_one_txn(wtree_txn_t *txn, wtree_tree_t *tree,
                        const void *key, size_t key_size,
                        const void *value, size_t value_size, gerror_t *error) {
    return wtree_insert_one(tree, key, key_size, value, value_size, error);
}

int wtree_insert_many_txn(wtree_txn_t *txn, wtree_tree_t *tree,
                         const wtree_kv_t *kvs, size_t count, gerror_t *error) {
    for (size_t i = 0; i < count; i++) {
        int rc = wtree_insert_one(tree, kvs[i].key, kvs[i].key_size,
                                  kvs[i].value, kvs[i].value_size, error);
        if (rc != 0) return rc;
    }
    return 0;
}

int wtree_update_txn(wtree_txn_t *txn, wtree_tree_t *tree,
                    const void *key, size_t key_size,
                    const void *value, size_t value_size, gerror_t *error) {
    return wtree_update(tree, key, key_size, value, value_size, error);
}

int wtree_delete_one_txn(wtree_txn_t *txn, wtree_tree_t *tree,
                         const void *key, size_t key_size,
                         bool *deleted, gerror_t *error) {
    return wtree_delete_one(tree, key, key_size, deleted, error);
}

int wtree_delete_many_txn(wtree_txn_t *txn, wtree_tree_t *tree,
                          const void **keys, const size_t *key_sizes,
                          size_t count, size_t *deleted_count, gerror_t *error) {
    size_t del = 0;
    for (size_t i = 0; i < count; i++) {
        bool deleted = false;
        wtree_delete_one(tree, keys[i], key_sizes[i], &deleted, error);
        if (deleted) del++;
    }
    if (deleted_count) *deleted_count = del;
    return 0;
}

int wtree_get_txn(wtree_txn_t *txn, wtree_tree_t *tree,
                 const void *key, size_t key_size,
                 const void **value, size_t *value_size, gerror_t *error) {
    g_mock_wtree_state.get_count++;

    if (g_mock_wtree_state.fail_next_get) {
        g_mock_wtree_state.fail_next_get = false;
        if (error) {
            set_error(error, "wtree", g_mock_wtree_state.error_code_to_inject,
                      "Mock: get failed");
        }
        return g_mock_wtree_state.error_code_to_inject;
    }

    if (mock_wtree_store_get_for_tree(tree, key, key_size, (void**)value, value_size)) {
        return 0;
    }
    return WTREE_KEY_NOT_FOUND;
}

bool wtree_exists_txn(wtree_txn_t *txn, wtree_tree_t *tree,
                     const void *key, size_t key_size, gerror_t *error) {
    return wtree_exists(tree, key, key_size, error);
}

/* Iterator Operations */

/* Helper: Find next store index matching this iterator's tree, starting from 'start' */
static int find_next_tree_entry(mock_wtree_iterator_t *miter, int start) {
    void *target_tree = miter->tree;
    for (int i = start; i < (int)g_mock_wtree_state.store_count; i++) {
        if (g_mock_wtree_state.store[i].tree == target_tree) {
            return i;
        }
    }
    return -1;  /* Not found */
}

/* Helper: Find previous store index matching this iterator's tree, starting from 'start' */
static int find_prev_tree_entry(mock_wtree_iterator_t *miter, int start) {
    void *target_tree = miter->tree;
    for (int i = start; i >= 0; i--) {
        if (g_mock_wtree_state.store[i].tree == target_tree) {
            return i;
        }
    }
    return -1;  /* Not found */
}

/* Helper: Find last store index matching this iterator's tree */
static int find_last_tree_entry(mock_wtree_iterator_t *miter) {
    return find_prev_tree_entry(miter, (int)g_mock_wtree_state.store_count - 1);
}

wtree_iterator_t* wtree_iterator_create(wtree_tree_t *tree, gerror_t *error) {
    g_mock_wtree_state.iterator_create_count++;

    mock_wtree_tree_t *mtree = (mock_wtree_tree_t*)tree;
    mock_wtree_iterator_t *iter = mock_wtree_create_iterator_handle(mtree);
    iter->owns_txn = true;

    return (wtree_iterator_t*)iter;
}

wtree_iterator_t* wtree_iterator_create_with_txn(wtree_tree_t *tree, wtree_txn_t *txn, gerror_t *error) {
    g_mock_wtree_state.iterator_create_count++;

    mock_wtree_tree_t *mtree = (mock_wtree_tree_t*)tree;
    mock_wtree_iterator_t *iter = mock_wtree_create_iterator_handle(mtree);
    iter->txn = (mock_wtree_txn_t*)txn;
    iter->owns_txn = false;

    return (wtree_iterator_t*)iter;
}

bool wtree_iterator_first(wtree_iterator_t *iter) {
    mock_wtree_iterator_t *miter = (mock_wtree_iterator_t*)iter;
    int pos = find_next_tree_entry(miter, 0);
    if (pos >= 0) {
        miter->position = pos;
        miter->valid = true;
        return true;
    }
    miter->valid = false;
    return false;
}

bool wtree_iterator_last(wtree_iterator_t *iter) {
    mock_wtree_iterator_t *miter = (mock_wtree_iterator_t*)iter;
    int pos = find_last_tree_entry(miter);
    if (pos >= 0) {
        miter->position = pos;
        miter->valid = true;
        return true;
    }
    miter->valid = false;
    return false;
}

bool wtree_iterator_next(wtree_iterator_t *iter) {
    mock_wtree_iterator_t *miter = (mock_wtree_iterator_t*)iter;
    int pos = find_next_tree_entry(miter, miter->position + 1);
    if (pos >= 0) {
        miter->position = pos;
        miter->valid = true;
        return true;
    }
    miter->valid = false;
    return false;
}

bool wtree_iterator_prev(wtree_iterator_t *iter) {
    mock_wtree_iterator_t *miter = (mock_wtree_iterator_t*)iter;
    int pos = find_prev_tree_entry(miter, miter->position - 1);
    if (pos >= 0) {
        miter->position = pos;
        miter->valid = true;
        return true;
    }
    miter->valid = false;
    return false;
}

bool wtree_iterator_seek(wtree_iterator_t *iter, const void *key, size_t key_size) {
    mock_wtree_iterator_t *miter = (mock_wtree_iterator_t*)iter;
    void *target_tree = miter->tree;
    for (size_t i = 0; i < g_mock_wtree_state.store_count; i++) {
        if (g_mock_wtree_state.store[i].tree == target_tree &&
            g_mock_wtree_state.store[i].key_size == key_size &&
            memcmp(g_mock_wtree_state.store[i].key, key, key_size) == 0) {
            miter->position = (int)i;
            miter->valid = true;
            return true;
        }
    }
    miter->valid = false;
    return false;
}

bool wtree_iterator_seek_range(wtree_iterator_t *iter, const void *key, size_t key_size) {
    /* For mock purposes, just use exact seek */
    return wtree_iterator_seek(iter, key, key_size);
}

bool wtree_iterator_key(wtree_iterator_t *iter, const void **key, size_t *key_size) {
    mock_wtree_iterator_t *miter = (mock_wtree_iterator_t*)iter;
    if (!miter->valid || miter->position < 0 ||
        miter->position >= (int)g_mock_wtree_state.store_count) {
        return false;
    }

    if (key) *key = g_mock_wtree_state.store[miter->position].key;
    if (key_size) *key_size = g_mock_wtree_state.store[miter->position].key_size;
    return true;
}

bool wtree_iterator_value(wtree_iterator_t *iter, const void **value, size_t *value_size) {
    mock_wtree_iterator_t *miter = (mock_wtree_iterator_t*)iter;
    if (!miter->valid || miter->position < 0 ||
        miter->position >= (int)g_mock_wtree_state.store_count) {
        return false;
    }

    if (value) *value = g_mock_wtree_state.store[miter->position].value;
    if (value_size) *value_size = g_mock_wtree_state.store[miter->position].value_size;
    return true;
}

bool wtree_iterator_key_copy(wtree_iterator_t *iter, void **key, size_t *key_size) {
    const void *k = NULL;
    size_t ks = 0;
    if (!wtree_iterator_key(iter, &k, &ks)) {
        return false;
    }

    if (key) {
        *key = malloc(ks);
        memcpy(*key, k, ks);
    }
    if (key_size) *key_size = ks;
    return true;
}

bool wtree_iterator_value_copy(wtree_iterator_t *iter, void **value, size_t *value_size) {
    const void *v = NULL;
    size_t vs = 0;
    if (!wtree_iterator_value(iter, &v, &vs)) {
        return false;
    }

    if (value) {
        *value = malloc(vs);
        memcpy(*value, v, vs);
    }
    if (value_size) *value_size = vs;
    return true;
}

bool wtree_iterator_valid(wtree_iterator_t *iter) {
    mock_wtree_iterator_t *miter = (mock_wtree_iterator_t*)iter;
    return miter->valid;
}

int wtree_iterator_delete(wtree_iterator_t *iter, gerror_t *error) {
    mock_wtree_iterator_t *miter = (mock_wtree_iterator_t*)iter;
    if (!miter->valid || miter->position < 0 ||
        miter->position >= (int)g_mock_wtree_state.store_count) {
        return -1;
    }

    /* Delete current entry */
    free(g_mock_wtree_state.store[miter->position].key);
    free(g_mock_wtree_state.store[miter->position].value);

    /* Move remaining entries */
    for (size_t i = miter->position; i < g_mock_wtree_state.store_count - 1; i++) {
        g_mock_wtree_state.store[i] = g_mock_wtree_state.store[i + 1];
    }
    g_mock_wtree_state.store_count--;

    /* Iterator now invalid until next move */
    miter->valid = false;
    return 0;
}

void wtree_iterator_close(wtree_iterator_t *iter) {
    g_mock_wtree_state.iterator_close_count++;

    mock_wtree_iterator_t *miter = (mock_wtree_iterator_t*)iter;
    if (miter) {
        miter->closed = true;
        mock_wtree_free_iterator_handle(miter);
    }
}

/* Utility Functions */

const char* wtree_strerror(int error_code) {
    switch (error_code) {
        case 0: return "Success";
        case WTREE_MAP_FULL: return "Database map is full";
        case WTREE_TXN_FULL: return "Transaction is full";
        case WTREE_KEY_NOT_FOUND: return "Key not found";
        case MDB_KEYEXIST: return "Key already exists";
        default: return "Unknown error";
    }
}

bool wtree_error_recoverable(int error_code) {
    return error_code == WTREE_MAP_FULL;
}
