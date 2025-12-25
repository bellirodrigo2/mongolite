/*
 * wtree2.h - Index-aware storage layer
 *
 * Extends wtree with built-in secondary index support:
 * - Automatic index maintenance on insert/update/delete
 * - Callback-based index key extraction
 * - Entry count tracking
 * - Decoupled from BSON/mongolite (works with raw bytes)
 *
 * Usage:
 *   1. Create tree with wtree2_tree_create()
 *   2. Register indexes with wtree2_tree_add_index()
 *   3. Populate existing data with wtree2_tree_populate_index()
 *   4. Use wtree2_insert/update/delete - indexes maintained automatically
 */

#ifndef WTREE2_H
#define WTREE2_H

#include "wtree.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Error Codes
 * ============================================================ */

#define WTREE2_OK            0
#define WTREE2_ERROR        -2000   /* Generic error */
#define WTREE2_EINVAL       -2001   /* Invalid argument */
#define WTREE2_ENOMEM       -2002   /* Out of memory */
#define WTREE2_EEXISTS      -2003   /* Already exists */
#define WTREE2_ENOTFOUND    -2004   /* Not found */
#define WTREE2_EINDEX       -2005   /* Index error (e.g., unique violation) */
#define WTREE2_MAP_FULL     -2006   /* Database map is full */

/* ============================================================
 * Types
 * ============================================================ */

/* Opaque handles */
typedef struct wtree2_db_t wtree2_db_t;
typedef struct wtree2_tree_t wtree2_tree_t;
typedef struct wtree2_txn_t wtree2_txn_t;
typedef struct wtree2_iterator_t wtree2_iterator_t;

/*
 * Index key extraction callback
 *
 * Called during insert/update/delete to extract index key from value.
 *
 * Parameters:
 *   value     - Raw value bytes (e.g., BSON document)
 *   value_len - Value length in bytes
 *   user_data - User context passed during index registration
 *   out_key   - Output: allocated key data (caller frees with free())
 *   out_len   - Output: key length
 *
 * Returns:
 *   true  - Key extracted successfully, document should be indexed
 *   false - Document should NOT be indexed (sparse index behavior)
 *
 * Note: For sparse indexes, return false when indexed field is missing/null.
 *       The callback is responsible for allocating out_key with malloc().
 */
typedef bool (*wtree2_index_key_fn)(
    const void *value,
    size_t value_len,
    void *user_data,
    void **out_key,
    size_t *out_len
);

/*
 * Index configuration
 */
typedef struct wtree2_index_config {
    const char *name;           /* Index name (e.g., "email_1") */
    wtree2_index_key_fn key_fn; /* Key extraction callback */
    void *user_data;            /* User context for callback */
    bool unique;                /* Unique constraint */
    bool sparse;                /* Skip entries where key_fn returns false */
    MDB_cmp_func *compare;      /* Custom key comparator (NULL for default) */
} wtree2_index_config_t;

/* ============================================================
 * Database Operations
 * ============================================================ */

/*
 * Create/open a database environment
 *
 * Wraps wtree_db_create with the same semantics.
 */
wtree2_db_t* wtree2_db_create(
    const char *path,
    size_t mapsize,
    unsigned int max_dbs,
    unsigned int flags,
    gerror_t *error
);

/* Close database environment */
void wtree2_db_close(wtree2_db_t *db);

/* Sync database to disk */
int wtree2_db_sync(wtree2_db_t *db, bool force, gerror_t *error);

/* Resize the database map */
int wtree2_db_resize(wtree2_db_t *db, size_t new_mapsize, gerror_t *error);

/* Get current mapsize */
size_t wtree2_db_get_mapsize(wtree2_db_t *db);

/* Get underlying wtree_db for advanced operations */
wtree_db_t* wtree2_db_get_wtree(wtree2_db_t *db);

/* ============================================================
 * Transaction Operations
 * ============================================================ */

/* Begin a transaction */
wtree2_txn_t* wtree2_txn_begin(wtree2_db_t *db, bool write, gerror_t *error);

/* Commit transaction */
int wtree2_txn_commit(wtree2_txn_t *txn, gerror_t *error);

/* Abort transaction */
void wtree2_txn_abort(wtree2_txn_t *txn);

/* Check if transaction is read-only */
bool wtree2_txn_is_readonly(wtree2_txn_t *txn);

/* Get underlying wtree_txn for advanced operations */
wtree_txn_t* wtree2_txn_get_wtree(wtree2_txn_t *txn);

/* ============================================================
 * Tree Operations
 * ============================================================ */

/*
 * Create/open a tree with index support
 *
 * Parameters:
 *   db          - Database handle
 *   name        - Tree name (e.g., "users")
 *   flags       - LMDB flags (usually 0)
 *   entry_count - Initial entry count (use 0 for new trees, or pass
 *                 persisted value when reopening existing tree)
 *   error       - Error output
 *
 * The tree starts with no indexes. Use wtree2_tree_add_index() to add them.
 */
wtree2_tree_t* wtree2_tree_create(
    wtree2_db_t *db,
    const char *name,
    unsigned int flags,
    int64_t entry_count,
    gerror_t *error
);

/* Close tree handle (does not delete data) */
void wtree2_tree_close(wtree2_tree_t *tree);

/* Delete a tree and all its indexes */
int wtree2_tree_delete(wtree2_db_t *db, const char *name, gerror_t *error);

/* Get tree name */
const char* wtree2_tree_name(wtree2_tree_t *tree);

/* Get current entry count */
int64_t wtree2_tree_count(wtree2_tree_t *tree);

/* Get underlying wtree_tree for advanced operations */
wtree_tree_t* wtree2_tree_get_wtree(wtree2_tree_t *tree);

/* Get parent database from tree */
wtree2_db_t* wtree2_tree_get_db(wtree2_tree_t *tree);

/* ============================================================
 * Index Management
 * ============================================================ */

/*
 * Add an index to a tree
 *
 * Creates the index tree but does NOT populate it with existing data.
 * Call wtree2_tree_populate_index() after adding to build from existing entries.
 *
 * Index tree naming: idx:<tree_name>:<index_name>
 *
 * Parameters:
 *   tree   - Tree handle
 *   config - Index configuration
 *   error  - Error output
 *
 * Returns: 0 on success, error code on failure
 */
int wtree2_tree_add_index(
    wtree2_tree_t *tree,
    const wtree2_index_config_t *config,
    gerror_t *error
);

/*
 * Populate an index from existing tree entries
 *
 * Scans all entries in the main tree and builds the index.
 * Use after wtree2_tree_add_index() for trees that already have data.
 *
 * Returns: 0 on success, WTREE2_EINDEX on unique violation
 */
int wtree2_tree_populate_index(
    wtree2_tree_t *tree,
    const char *index_name,
    gerror_t *error
);

/*
 * Drop an index from a tree
 *
 * Deletes the index tree and removes it from the tree's index list.
 */
int wtree2_tree_drop_index(
    wtree2_tree_t *tree,
    const char *index_name,
    gerror_t *error
);

/*
 * Check if an index exists
 */
bool wtree2_tree_has_index(wtree2_tree_t *tree, const char *index_name);

/*
 * Get index count
 */
size_t wtree2_tree_index_count(wtree2_tree_t *tree);

/* ============================================================
 * Data Operations (With Transaction)
 *
 * These operations automatically maintain all registered indexes.
 * ============================================================ */

/*
 * Insert a key-value pair
 *
 * Automatically:
 * - Checks unique constraints on all indexes
 * - Inserts into all index trees
 * - Increments entry count
 *
 * Returns: 0 on success, WTREE2_EINDEX on unique violation
 */
int wtree2_insert_one_txn(
    wtree2_txn_t *txn,
    wtree2_tree_t *tree,
    const void *key, size_t key_size,
    const void *value, size_t value_size,
    gerror_t *error
);

/*
 * Update an existing key's value
 *
 * Automatically:
 * - Removes old index entries
 * - Checks unique constraints for new index keys
 * - Inserts new index entries
 *
 * Note: Does NOT change entry count (key already exists)
 *
 * Returns: 0 on success, WTREE2_ENOTFOUND if key doesn't exist
 */
int wtree2_update_txn(
    wtree2_txn_t *txn,
    wtree2_tree_t *tree,
    const void *key, size_t key_size,
    const void *value, size_t value_size,
    gerror_t *error
);

/*
 * Delete a key-value pair
 *
 * Automatically:
 * - Removes from all index trees
 * - Decrements entry count
 *
 * Parameters:
 *   deleted - Output: true if key existed and was deleted
 *
 * Returns: 0 on success (even if key didn't exist)
 */
int wtree2_delete_one_txn(
    wtree2_txn_t *txn,
    wtree2_tree_t *tree,
    const void *key, size_t key_size,
    bool *deleted,
    gerror_t *error
);

/*
 * Get value by key (read-only, no index maintenance)
 *
 * Zero-copy: returned value is valid only during transaction.
 */
int wtree2_get_txn(
    wtree2_txn_t *txn,
    wtree2_tree_t *tree,
    const void *key, size_t key_size,
    const void **value, size_t *value_size,
    gerror_t *error
);

/*
 * Check if key exists
 */
bool wtree2_exists_txn(
    wtree2_txn_t *txn,
    wtree2_tree_t *tree,
    const void *key, size_t key_size,
    gerror_t *error
);

/* ============================================================
 * Data Operations (Auto-transaction)
 *
 * Convenience wrappers that create their own transaction.
 * ============================================================ */

int wtree2_insert_one(
    wtree2_tree_t *tree,
    const void *key, size_t key_size,
    const void *value, size_t value_size,
    gerror_t *error
);

int wtree2_update(
    wtree2_tree_t *tree,
    const void *key, size_t key_size,
    const void *value, size_t value_size,
    gerror_t *error
);

int wtree2_delete_one(
    wtree2_tree_t *tree,
    const void *key, size_t key_size,
    bool *deleted,
    gerror_t *error
);

int wtree2_get(
    wtree2_tree_t *tree,
    const void *key, size_t key_size,
    void **value, size_t *value_size,
    gerror_t *error
);

bool wtree2_exists(
    wtree2_tree_t *tree,
    const void *key, size_t key_size,
    gerror_t *error
);

/* ============================================================
 * Iterator Operations (Main Tree)
 * ============================================================ */

/* Create iterator for main tree */
wtree2_iterator_t* wtree2_iterator_create(wtree2_tree_t *tree, gerror_t *error);

/* Create iterator with existing transaction */
wtree2_iterator_t* wtree2_iterator_create_with_txn(
    wtree2_tree_t *tree,
    wtree2_txn_t *txn,
    gerror_t *error
);

/* Navigation */
bool wtree2_iterator_first(wtree2_iterator_t *iter);
bool wtree2_iterator_last(wtree2_iterator_t *iter);
bool wtree2_iterator_next(wtree2_iterator_t *iter);
bool wtree2_iterator_prev(wtree2_iterator_t *iter);
bool wtree2_iterator_seek(wtree2_iterator_t *iter, const void *key, size_t key_size);
bool wtree2_iterator_seek_range(wtree2_iterator_t *iter, const void *key, size_t key_size);

/* Access current entry (zero-copy) */
bool wtree2_iterator_key(wtree2_iterator_t *iter, const void **key, size_t *key_size);
bool wtree2_iterator_value(wtree2_iterator_t *iter, const void **value, size_t *value_size);

/* Check validity */
bool wtree2_iterator_valid(wtree2_iterator_t *iter);

/* Close iterator */
void wtree2_iterator_close(wtree2_iterator_t *iter);

/* ============================================================
 * Index Query Operations
 *
 * Query using an index, then fetch from main tree.
 * ============================================================ */

/*
 * Create iterator positioned at index key
 *
 * The iterator yields main tree keys that match the index key.
 * Use wtree2_get_txn() to fetch the actual values.
 *
 * Parameters:
 *   tree       - Tree handle
 *   index_name - Name of index to query
 *   key        - Index key to seek
 *   key_size   - Key size
 *   error      - Error output
 *
 * Returns: Iterator or NULL on error
 */
wtree2_iterator_t* wtree2_index_seek(
    wtree2_tree_t *tree,
    const char *index_name,
    const void *key, size_t key_size,
    gerror_t *error
);

/*
 * Create iterator positioned at index key range
 *
 * Like wtree2_index_seek but uses range seek (key or next greater).
 */
wtree2_iterator_t* wtree2_index_seek_range(
    wtree2_tree_t *tree,
    const char *index_name,
    const void *key, size_t key_size,
    gerror_t *error
);

/*
 * Get main tree key from index iterator
 *
 * When iterating an index, the "value" is the main tree key.
 * This is a convenience alias for wtree2_iterator_value().
 */
bool wtree2_index_iterator_main_key(
    wtree2_iterator_t *iter,
    const void **main_key,
    size_t *main_key_size
);

/* ============================================================
 * Utility Functions
 * ============================================================ */

/* Convert error code to string */
const char* wtree2_strerror(int error_code);

/* Check if error is recoverable (e.g., map full) */
bool wtree2_error_recoverable(int error_code);

#ifdef __cplusplus
}
#endif

#endif /* WTREE2_H */
