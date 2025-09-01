#ifndef MONGOLITE_API_H
#define MONGOLITE_API_H

#include <stddef.h>
#include <stdbool.h>
#include <bson/bson.h>

#ifdef __cplusplus
extern "C" {
#endif

// Forward declarations
typedef struct mlite_db mlite_db_t;
typedef struct mlite_cursor mlite_cursor_t;

// Open flags (similar to sqlite3)
#define MLITE_OPEN_READWRITE    0x00000002
#define MLITE_OPEN_CREATE       0x00000004
#define MLITE_OPEN_READONLY     0x00000001

// Database operations (similar to sqlite3_open/close)
int mlite_open(const char *filename, mlite_db_t **db);
int mlite_open_v2(const char *filename, mlite_db_t **db, int flags);
int mlite_close(mlite_db_t *db);

// Collection operations
int mlite_collection_create(mlite_db_t *db, const char *collection_name);
int mlite_collection_drop(mlite_db_t *db, const char *collection_name);
bool mlite_collection_exists(mlite_db_t *db, const char *collection_name);

// Document operations
int mlite_insert_one(mlite_db_t *db, const char *collection_name, 
                     const bson_t *doc, bson_error_t *error);

int mlite_insert_many(mlite_db_t *db, const char *collection_name,
                      const bson_t **docs, size_t n_docs, 
                      bson_error_t *error);

int mlite_update_one(mlite_db_t *db, const char *collection_name,
                     const bson_t *filter, const bson_t *update,
                     bson_error_t *error);

int mlite_update_many(mlite_db_t *db, const char *collection_name,
                      const bson_t *filter, const bson_t *update,
                      bson_error_t *error);

int mlite_replace_one(mlite_db_t *db, const char *collection_name,
                      const bson_t *filter, const bson_t *replacement,
                      bson_error_t *error);

int mlite_delete_one(mlite_db_t *db, const char *collection_name,
                     const bson_t *filter, bson_error_t *error);

int mlite_delete_many(mlite_db_t *db, const char *collection_name,
                      const bson_t *filter, bson_error_t *error);

// Query operations
mlite_cursor_t* mlite_find(mlite_db_t *db, const char *collection_name,
                          const bson_t *filter, const bson_t *opts);

// Cursor operations  
bool mlite_cursor_next(mlite_cursor_t *cursor, const bson_t **doc);
void mlite_cursor_destroy(mlite_cursor_t *cursor);
bool mlite_cursor_error(mlite_cursor_t *cursor, bson_error_t *error);

// Count operations
int64_t mlite_count_documents(mlite_db_t *db, const char *collection_name,
                             const bson_t *filter, bson_error_t *error);

// Index operations
int mlite_create_index(mlite_db_t *db, const char *collection_name,
                       const bson_t *keys, const bson_t *opts,
                       bson_error_t *error);

int mlite_drop_index(mlite_db_t *db, const char *collection_name,
                     const char *index_name, bson_error_t *error);

// Utility functions
const char* mlite_errmsg(mlite_db_t *db);
int mlite_errcode(mlite_db_t *db);

#ifdef __cplusplus
}
#endif

#endif // MONGOLITE_API_H