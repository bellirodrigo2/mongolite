#ifndef WTREE_H
#define WTREE_H

#include <lmdb.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Error handling
typedef struct wtree_error_t wtree_error_t;

// Database handle
typedef struct wtree_db_t wtree_db_t;

// Transaction handle
typedef struct wtree_txn_t wtree_txn_t;

// Tree handle (DBI - Database Instance)
typedef struct wtree_tree_t wtree_tree_t;

// Iterator handle
typedef struct wtree_iterator_t wtree_iterator_t;

// Key-Value pair
typedef struct wtree_kv_t wtree_kv_t;

// ============= Database Operations =============

// Create/Open database environment
wtree_db_t* wtree_db_create(const char *path, size_t mapsize, unsigned int max_dbs, wtree_error_t *error);

// Close database environment
void wtree_db_close(wtree_db_t *db);

// Delete database (remove all files)
int wtree_db_delete(const char *path, wtree_error_t *error);

// Get database statistics
int wtree_db_stats(wtree_db_t *db, MDB_stat *stat, wtree_error_t *error);

// Sync database to disk
int wtree_db_sync(wtree_db_t *db, bool force, wtree_error_t *error);

// ============= Tree Operations =============

// Create/Open a tree (named database)
wtree_tree_t* wtree_tree_create(wtree_db_t *db, const char *name, unsigned int flags, wtree_error_t *error);

// Set custom key comparison function
int wtree_tree_set_compare(wtree_tree_t *tree, MDB_cmp_func *cmp, wtree_error_t *error);

// List all trees in database
char** wtree_tree_list(wtree_db_t *db, size_t *count, wtree_error_t *error);

// Free tree list
void wtree_tree_list_free(char **list, size_t count);

// Delete a tree
int wtree_tree_delete(wtree_db_t *db, const char *name, wtree_error_t *error);

// Close tree handle
void wtree_tree_close(wtree_tree_t *tree);

// ============= Transaction Operations =============

// Begin transaction
wtree_txn_t* wtree_txn_begin(wtree_db_t *db, bool write, wtree_error_t *error);

// Commit transaction
int wtree_txn_commit(wtree_txn_t *txn, wtree_error_t *error);

// Abort transaction
void wtree_txn_abort(wtree_txn_t *txn);

// ============= Data Operations =============

// Insert single key-value pair (ACID)
int wtree_insert_one(wtree_tree_t *tree, const void *key, size_t key_size, 
                     const void *value, size_t value_size, wtree_error_t *error);

// Insert multiple key-value pairs (ACID batch)
int wtree_insert_many(wtree_tree_t *tree, wtree_kv_t *kvs, size_t count, wtree_error_t *error);

// Update existing key
int wtree_update(wtree_tree_t *tree, const void *key, size_t key_size,
                const void *value, size_t value_size, wtree_error_t *error);

// Delete single key
int wtree_delete_one(wtree_tree_t *tree, const void *key, size_t key_size, wtree_error_t *error);

// Get value by key
int wtree_get(wtree_tree_t *tree, const void *key, size_t key_size,
             void **value, size_t *value_size, wtree_error_t *error);

// Check if key exists
bool wtree_exists(wtree_tree_t *tree, const void *key, size_t key_size, wtree_error_t *error);

// ============= Iterator Operations =============

// Create iterator
wtree_iterator_t* wtree_iterator_create(wtree_tree_t *tree, wtree_error_t *error);

// Create iterator with custom transaction
wtree_iterator_t* wtree_iterator_create_with_txn(wtree_tree_t *tree, wtree_txn_t *txn, wtree_error_t *error);

// Move to first entry
bool wtree_iterator_first(wtree_iterator_t *iter);

// Move to last entry
bool wtree_iterator_last(wtree_iterator_t *iter);

// Move to next entry
bool wtree_iterator_next(wtree_iterator_t *iter);

// Move to previous entry
bool wtree_iterator_prev(wtree_iterator_t *iter);

// Seek to specific key
bool wtree_iterator_seek(wtree_iterator_t *iter, const void *key, size_t key_size);

// Seek to key or next greater key
bool wtree_iterator_seek_range(wtree_iterator_t *iter, const void *key, size_t key_size);

// Get current key
bool wtree_iterator_key(wtree_iterator_t *iter, void **key, size_t *key_size);

// Get current value
bool wtree_iterator_value(wtree_iterator_t *iter, void **value, size_t *value_size);

// Check if iterator is valid
bool wtree_iterator_valid(wtree_iterator_t *iter);

// Delete current entry (requires write transaction)
int wtree_iterator_delete(wtree_iterator_t *iter, wtree_error_t *error);

// Close iterator
void wtree_iterator_close(wtree_iterator_t *iter);

// ============= Utility Functions =============

// Get last error message
const char* wtree_error_message(wtree_error_t *error);

// Clear error
void wtree_error_clear(wtree_error_t *error);


#ifdef __cplusplus
}
#endif

#endif // WTREE_H