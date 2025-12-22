/**
 * mock_wtree.h - Mock wtree layer for unit testing
 *
 * Uses cmocka's mocking infrastructure to simulate wtree behavior.
 * This enables isolated testing of mongolite modules without filesystem I/O.
 *
 * Usage:
 *   1. Link test with mock_wtree.c instead of wtree.c
 *   2. Use will_return() to set expected return values
 *   3. Use expect_*() to verify function arguments
 *   4. Use mock_wtree_*() helpers for common patterns
 */

#ifndef MOCK_WTREE_H
#define MOCK_WTREE_H

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdbool.h>
#include <lmdb.h>
#include "gerror.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Mock Handle Types
 *
 * These are simple structures that can be allocated/tracked
 * without requiring real LMDB resources.
 * ============================================================ */

typedef struct mock_wtree_db {
    char *path;
    size_t mapsize;
    unsigned int max_dbs;
    unsigned int flags;
    bool closed;
} mock_wtree_db_t;

typedef struct mock_wtree_tree {
    mock_wtree_db_t *db;
    char *name;
    unsigned int flags;
    MDB_cmp_func *compare_fn;
    MDB_cmp_func *dupsort_fn;
    bool closed;
} mock_wtree_tree_t;

typedef struct mock_wtree_txn {
    mock_wtree_db_t *db;
    struct mock_wtree_txn *parent;
    bool write;
    bool committed;
    bool aborted;
    bool reset;
} mock_wtree_txn_t;

typedef struct mock_wtree_iterator {
    mock_wtree_tree_t *tree;
    mock_wtree_txn_t *txn;
    bool owns_txn;
    bool valid;
    int position;  /* For simulating iteration */
    bool closed;
} mock_wtree_iterator_t;

/* ============================================================
 * Mock Configuration
 *
 * These globals control mock behavior across tests.
 * Reset with mock_wtree_reset() between tests.
 * ============================================================ */

/* Global mock state */
typedef struct mock_wtree_state {
    /* Counters for tracking calls */
    int db_create_count;
    int db_close_count;
    int tree_create_count;
    int tree_close_count;
    int txn_begin_count;
    int txn_commit_count;
    int txn_abort_count;
    int insert_count;
    int update_count;
    int delete_count;
    int get_count;
    int iterator_create_count;
    int iterator_close_count;

    /* Simulated data store (key -> value pairs, per-tree) */
    struct {
        void *key;
        size_t key_size;
        void *value;
        size_t value_size;
        void *tree;  /* Which tree this entry belongs to */
    } *store;
    size_t store_count;
    size_t store_capacity;

    /* Error injection */
    bool fail_next_txn_begin;
    bool fail_next_txn_commit;
    bool fail_next_insert;
    bool fail_next_get;
    int error_code_to_inject;

} mock_wtree_state_t;

extern mock_wtree_state_t g_mock_wtree_state;

/* ============================================================
 * Mock Control Functions
 * ============================================================ */

/**
 * Reset all mock state (call in test setup/teardown)
 */
void mock_wtree_reset(void);

/**
 * Add a key-value pair to the simulated store for a specific tree
 */
void mock_wtree_store_put_for_tree(void *tree, const void *key, size_t key_size,
                                   const void *value, size_t value_size);

/**
 * Add a key-value pair to the simulated store (legacy, tree=NULL)
 */
void mock_wtree_store_put(const void *key, size_t key_size,
                          const void *value, size_t value_size);

/**
 * Get a value from the simulated store
 * Returns true if found
 */
bool mock_wtree_store_get(const void *key, size_t key_size,
                          void **value, size_t *value_size);

/**
 * Remove a key from the simulated store
 * Returns true if found and removed
 */
bool mock_wtree_store_delete(const void *key, size_t key_size);

/**
 * Clear all data from the simulated store
 */
void mock_wtree_store_clear(void);

/**
 * Inject an error for the next operation
 */
void mock_wtree_inject_error(int error_code);

/**
 * Make next txn_begin fail
 */
void mock_wtree_fail_next_txn_begin(int error_code);

/**
 * Make next txn_commit fail
 */
void mock_wtree_fail_next_txn_commit(int error_code);

/**
 * Make next insert fail
 */
void mock_wtree_fail_next_insert(int error_code);

/**
 * Make next get fail
 */
void mock_wtree_fail_next_get(int error_code);

/* ============================================================
 * Mock Handle Creators (for tests that need specific handles)
 * ============================================================ */

mock_wtree_db_t* mock_wtree_create_db_handle(const char *path, size_t mapsize);
mock_wtree_tree_t* mock_wtree_create_tree_handle(mock_wtree_db_t *db, const char *name);
mock_wtree_txn_t* mock_wtree_create_txn_handle(mock_wtree_db_t *db, bool write);
mock_wtree_iterator_t* mock_wtree_create_iterator_handle(mock_wtree_tree_t *tree);

void mock_wtree_free_db_handle(mock_wtree_db_t *db);
void mock_wtree_free_tree_handle(mock_wtree_tree_t *tree);
void mock_wtree_free_txn_handle(mock_wtree_txn_t *txn);
void mock_wtree_free_iterator_handle(mock_wtree_iterator_t *iter);

/* ============================================================
 * Test Assertion Helpers
 * ============================================================ */

/**
 * Assert that a specific number of transactions were begun
 */
#define assert_txn_begin_count(expected) \
    assert_int_equal((expected), g_mock_wtree_state.txn_begin_count)

/**
 * Assert that all transactions were properly closed
 */
#define assert_txn_commit_count(expected) \
    assert_int_equal((expected), g_mock_wtree_state.txn_commit_count)

/**
 * Assert insert count
 */
#define assert_insert_count(expected) \
    assert_int_equal((expected), g_mock_wtree_state.insert_count)

/**
 * Assert no resources were leaked
 */
#define assert_no_leaks() do { \
    assert_int_equal(g_mock_wtree_state.db_create_count, g_mock_wtree_state.db_close_count); \
    assert_int_equal(g_mock_wtree_state.tree_create_count, g_mock_wtree_state.tree_close_count); \
    assert_int_equal(g_mock_wtree_state.iterator_create_count, g_mock_wtree_state.iterator_close_count); \
} while(0)

#ifdef __cplusplus
}
#endif

#endif /* MOCK_WTREE_H */
