/*
** MongoLite - Core Interface
** MongoDB-compatible document database using SQLite storage + BSON documents
** 
** Architecture:
** - SQLite provides ACID transactions, indexing, and persistence
** - BSON documents stored as BLOBs in SQLite tables
** - MongoDB-style collections and operations
** - Zero-overhead header-only design for performance
*/
#ifndef MONGOLITE_H
#define MONGOLITE_H

#include <sqlite3.h>
#include <stdbool.h>
#include <stdint.h>
#include "mongolite_bson.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
** Core MongoLite Types
*/
typedef struct mongolite_db mongolite_db_t;
typedef struct mongolite_collection mongolite_collection_t;
typedef struct mongolite_cursor mongolite_cursor_t;

/*
** Error handling
*/
typedef enum {
    MONGOLITE_OK = 0,
    MONGOLITE_ERROR_INVALID_ARGUMENT,
    MONGOLITE_ERROR_OUT_OF_MEMORY,
    MONGOLITE_ERROR_DATABASE_ERROR,
    MONGOLITE_ERROR_COLLECTION_NOT_FOUND,
    MONGOLITE_ERROR_DOCUMENT_NOT_FOUND,
    MONGOLITE_ERROR_INVALID_BSON,
    MONGOLITE_ERROR_TRANSACTION_FAILED
} mongolite_error_t;

typedef struct {
    mongolite_error_t code;
    char message[256];
    int sqlite_error;
} mongolite_result_t;

/*
** Database Operations
*/

// Open/close database
static inline mongolite_db_t* mongolite_open(const char *path, mongolite_result_t *result);
static inline void mongolite_close(mongolite_db_t *db);

// Collection access
static inline mongolite_collection_t* mongolite_get_collection(mongolite_db_t *db, const char *name, mongolite_result_t *result);
static inline bool mongolite_drop_collection(mongolite_db_t *db, const char *name, mongolite_result_t *result);
static inline char** mongolite_list_collections(mongolite_db_t *db, size_t *count, mongolite_result_t *result);

/*
** Collection Operations (MongoDB-style CRUD)
*/

// Insert operations
static inline bool mongolite_insert_one(mongolite_collection_t *coll, const mongolite_bson_t *doc, mongolite_result_t *result);
static inline bool mongolite_insert_many(mongolite_collection_t *coll, const mongolite_bson_t **docs, size_t count, mongolite_result_t *result);

// Find operations  
static inline mongolite_cursor_t* mongolite_find(mongolite_collection_t *coll, const mongolite_bson_t *filter, mongolite_result_t *result);
static inline mongolite_bson_t* mongolite_find_one(mongolite_collection_t *coll, const mongolite_bson_t *filter, mongolite_result_t *result);
static inline int64_t mongolite_count_documents(mongolite_collection_t *coll, const mongolite_bson_t *filter, mongolite_result_t *result);

// Update operations
static inline bool mongolite_update_one(mongolite_collection_t *coll, const mongolite_bson_t *filter, const mongolite_bson_t *update, mongolite_result_t *result);
static inline bool mongolite_update_many(mongolite_collection_t *coll, const mongolite_bson_t *filter, const mongolite_bson_t *update, mongolite_result_t *result);
static inline bool mongolite_replace_one(mongolite_collection_t *coll, const mongolite_bson_t *filter, const mongolite_bson_t *replacement, mongolite_result_t *result);

// Delete operations
static inline bool mongolite_delete_one(mongolite_collection_t *coll, const mongolite_bson_t *filter, mongolite_result_t *result);
static inline bool mongolite_delete_many(mongolite_collection_t *coll, const mongolite_bson_t *filter, mongolite_result_t *result);

/*
** Cursor Operations (for iteration)
*/
static inline bool mongolite_cursor_next(mongolite_cursor_t *cursor, mongolite_bson_t **doc);
static inline void mongolite_cursor_destroy(mongolite_cursor_t *cursor);
static inline int64_t mongolite_cursor_count(mongolite_cursor_t *cursor);

/*
** Transaction Support
*/
static inline bool mongolite_begin_transaction(mongolite_db_t *db, mongolite_result_t *result);
static inline bool mongolite_commit_transaction(mongolite_db_t *db, mongolite_result_t *result);
static inline bool mongolite_rollback_transaction(mongolite_db_t *db, mongolite_result_t *result);

/*
** Indexing (MongoDB-style)
*/
static inline bool mongolite_create_index(mongolite_collection_t *coll, const mongolite_bson_t *keys, mongolite_result_t *result);
static inline bool mongolite_drop_index(mongolite_collection_t *coll, const char *name, mongolite_result_t *result);
static inline char** mongolite_list_indexes(mongolite_collection_t *coll, size_t *count, mongolite_result_t *result);

/*
** Utility Functions
*/
static inline const char* mongolite_get_version(void);
static inline const char* mongolite_error_string(mongolite_error_t error);
static inline void mongolite_result_clear(mongolite_result_t *result);

/*
** Internal Schema
** 
** MongoLite uses this SQLite schema:
** 
** Collections table (metadata):
** CREATE TABLE _mongolite_collections (
**     name TEXT PRIMARY KEY,
**     created_at INTEGER,
**     document_count INTEGER DEFAULT 0,
**     indexes TEXT  -- JSON array of index definitions
** );
** 
** Per-collection document tables:
** CREATE TABLE collection_{name} (
**     _id TEXT PRIMARY KEY,        -- BSON ObjectId as string  
**     document BLOB NOT NULL,      -- BSON document as binary
**     created_at INTEGER,
**     updated_at INTEGER,
**     -- Index columns are added dynamically based on created indexes
** );
**
** Index tables (for complex queries):
** CREATE INDEX idx_{collection}_{field} ON collection_{name} (json_extract(document, '$.field'));
*/

#ifdef __cplusplus
}
#endif

#endif /* MONGOLITE_H */