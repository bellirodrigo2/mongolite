#ifndef WTREE_H
#define WTREE_H

#include <lmdb.h>
#include <stdbool.h>
#include <stddef.h>
#include "gerror.h"

#ifdef __cplusplus
extern "C" {
#endif

// Error codes específicos do wtree
#define WTREE_MAP_FULL      1001  // Database map is full, needs resizing
#define WTREE_TXN_FULL      1002  // Transaction is full
#define WTREE_KEY_NOT_FOUND 1003  // Key not found (not necessarily an error)

// Database handle
typedef struct wtree_db_t wtree_db_t;

// Transaction handle
typedef struct wtree_txn_t wtree_txn_t;

// Tree handle (DBI - Database Instance)
typedef struct wtree_tree_t wtree_tree_t;

// Iterator handle
typedef struct wtree_iterator_t wtree_iterator_t;

// Key-Value pair for batch operations
typedef struct wtree_kv_t {
    const void *key;
    size_t key_size;
    const void *value;
    size_t value_size;
} wtree_kv_t;

// ============= Database Operations =============

// Create/Open database environment
// Note: Directory must exist. Does not create directories.
// flags = MDB_NOTLS, MDB_RDONLY, MDB_WRITEMAP, MDB_NOMETASYNC, MDB_NOSYNC, MDB_MAPASYNC
wtree_db_t* wtree_db_create(const char *path, size_t mapsize, unsigned int max_dbs, unsigned int flags, gerror_t *error);

// Close database environment
void wtree_db_close(wtree_db_t *db);

// Get database statistics
int wtree_db_stats(wtree_db_t *db, MDB_stat *stat, gerror_t *error);

// Sync database to disk
int wtree_db_sync(wtree_db_t *db, bool force, gerror_t *error);

// Resize the database (requires closing all transactions first)
int wtree_db_resize(wtree_db_t *db, size_t new_mapsize, gerror_t *error);

// Get current mapsize
size_t wtree_db_get_mapsize(wtree_db_t *db);

// ============= Tree Operations =============

// Create/Open a tree (named database)
wtree_tree_t* wtree_tree_create(wtree_db_t *db, const char *name, unsigned int flags, gerror_t *error);

// Set custom key comparison function
int wtree_tree_set_compare(wtree_tree_t *tree, MDB_cmp_func *cmp, gerror_t *error);

int wtree_tree_set_dupsort(wtree_tree_t *tree, MDB_cmp_func *cmp, gerror_t *error);

// Delete a tree
int wtree_tree_delete(wtree_db_t *db, const char *name, gerror_t *error);

// Close tree handle
void wtree_tree_close(wtree_tree_t *tree);

// ============= Transaction Operations =============

// Begin transaction
wtree_txn_t* wtree_txn_begin(wtree_db_t *db, bool write, gerror_t *error);

// Begin nested transaction
wtree_txn_t* wtree_txn_begin_nested(wtree_txn_t *parent, gerror_t *error);

// Commit transaction
int wtree_txn_commit(wtree_txn_t *txn, gerror_t *error);

// Abort transaction
void wtree_txn_abort(wtree_txn_t *txn);

// Reset read-only transaction (release snapshot but keep handle for reuse)
// Only valid for read-only transactions. After reset, must call renew before use.
void wtree_txn_reset(wtree_txn_t *txn);

// Renew a reset read-only transaction (acquire new snapshot)
// Much faster than creating a new transaction.
int wtree_txn_renew(wtree_txn_t *txn, gerror_t *error);

// Check if transaction is read-only
bool wtree_txn_is_readonly(wtree_txn_t *txn);

// ============= Data Operations (Auto-transaction) =============
// These create their own transactions for convenience

// Insert single key-value pair (creates own transaction)
int wtree_insert_one(wtree_tree_t *tree, const void *key, size_t key_size, 
                     const void *value, size_t value_size, gerror_t *error);

// Update existing key (creates own transaction)
int wtree_update(wtree_tree_t *tree, const void *key, size_t key_size,
                const void *value, size_t value_size, gerror_t *error);

// Delete single key (creates own transaction, returns 0 even if key doesn't exist)
int wtree_delete_one(wtree_tree_t *tree, const void *key, size_t key_size, 
                     bool *deleted, gerror_t *error);

// Get value by key (creates own read transaction)
int wtree_get(wtree_tree_t *tree, const void *key, size_t key_size,
             void **value, size_t *value_size, gerror_t *error);

// Check if key exists (creates own read transaction)
bool wtree_exists(wtree_tree_t *tree, const void *key, size_t key_size, gerror_t *error);

// ============= Data Operations (With Transaction) =============
// These use an existing transaction for better performance

// Insert with existing transaction
int wtree_insert_one_txn(wtree_txn_t *txn, wtree_tree_t *tree, 
                        const void *key, size_t key_size,
                        const void *value, size_t value_size, gerror_t *error);

// Insert multiple key-value pairs in one transaction (efficient batch)
int wtree_insert_many_txn(wtree_txn_t *txn, wtree_tree_t *tree, 
                         const wtree_kv_t *kvs, size_t count, gerror_t *error);

// Update with existing transaction
int wtree_update_txn(wtree_txn_t *txn, wtree_tree_t *tree,
                    const void *key, size_t key_size,
                    const void *value, size_t value_size, gerror_t *error);

// Delete with existing transaction (returns 0 even if key doesn't exist)
int wtree_delete_one_txn(wtree_txn_t *txn, wtree_tree_t *tree,
                         const void *key, size_t key_size,
                         bool *deleted, gerror_t *error);

// Delete a specific key+value pair from a DUPSORT tree
int wtree_delete_dup_txn(wtree_txn_t *txn, wtree_tree_t *tree,
                         const void *key, size_t key_size,
                         const void *value, size_t value_size,
                         bool *deleted, gerror_t *error);

// Delete multiple keys in one transaction
int wtree_delete_many_txn(wtree_txn_t *txn, wtree_tree_t *tree,
                          const void **keys, const size_t *key_sizes,
                          size_t count, size_t *deleted_count, gerror_t *error);

// Get with existing transaction (zero-copy, valid only during transaction)
int wtree_get_txn(wtree_txn_t *txn, wtree_tree_t *tree,
                 const void *key, size_t key_size,
                 const void **value, size_t *value_size, gerror_t *error);

// Check existence with existing transaction
bool wtree_exists_txn(wtree_txn_t *txn, wtree_tree_t *tree,
                     const void *key, size_t key_size, gerror_t *error);

// ============= Iterator Operations =============

// Create iterator (creates its own read transaction)
wtree_iterator_t* wtree_iterator_create(wtree_tree_t *tree, gerror_t *error);

// Create iterator with custom transaction
wtree_iterator_t* wtree_iterator_create_with_txn(wtree_tree_t *tree, wtree_txn_t *txn, gerror_t *error);

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

// Get current key (zero-copy, valid only while iterator is valid)
bool wtree_iterator_key(wtree_iterator_t *iter, const void **key, size_t *key_size);

// Get current value (zero-copy, valid only while iterator is valid)
bool wtree_iterator_value(wtree_iterator_t *iter, const void **value, size_t *value_size);

// Get current key with copy (safe to use after iterator is closed)
bool wtree_iterator_key_copy(wtree_iterator_t *iter, void **key, size_t *key_size);

// Get current value with copy (safe to use after iterator is closed)  
bool wtree_iterator_value_copy(wtree_iterator_t *iter, void **value, size_t *value_size);

// Check if iterator is valid
bool wtree_iterator_valid(wtree_iterator_t *iter);

// Delete current entry (requires write transaction)
int wtree_iterator_delete(wtree_iterator_t *iter, gerror_t *error);

// Close iterator
void wtree_iterator_close(wtree_iterator_t *iter);

// ============= Utility Functions =============

// Convert LMDB error code to string
const char* wtree_strerror(int error_code);

// Check if error is recoverable (like MAP_FULL)
bool wtree_error_recoverable(int error_code);

#ifdef __cplusplus
}
#endif

#endif // WTREE_H
