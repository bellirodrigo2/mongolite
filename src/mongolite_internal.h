#ifndef MONGOLITE_INTERNAL_H
#define MONGOLITE_INTERNAL_H

#include "mongolite.h"
#include <bson/bson.h>
#include <sqlite3.h>

// Internal database structure
struct mlite_db {
    sqlite3 *sqlite_db;
    char *filename;
    int flags;
    char *errmsg;
    int errcode;
};

// Internal cursor structure
struct mlite_cursor {
    mlite_db_t *db;
    sqlite3_stmt *stmt;
    char *collection_name;
    bson_t *filter;
    bson_t *opts;
    bson_t *current_doc;
    bool has_error;
    bson_error_t error;
    bool finished;
};

// Query evaluation functions (implemented in mongolite_query.c)
bool evaluate_query_operator(const bson_iter_t *doc_iter, const bson_t *query_expr);
bool document_matches_filter(const bson_t *doc, const bson_t *filter);
int mongodb_value_compare(const bson_iter_t *iter1, const bson_iter_t *iter2);
int get_mongodb_type_precedence(bson_type_t type);
bool bson_iter_values_equal(const bson_iter_t *iter1, const bson_iter_t *iter2);
int bson_iter_mongodb_compare(const bson_iter_t *iter1, const bson_iter_t *iter2);

// Find operations (implemented in mongolite_query.c)
mlite_cursor_t* mlite_find(mlite_db_t *db, const char *collection_name,
                          const bson_t *filter, const bson_t *opts);
bson_t* mlite_find_one(mlite_db_t *db, const char *collection_name,
                       const bson_t *filter, const bson_t *opts);
int64_t mlite_count_documents(mlite_db_t *db, const char *collection_name,
                              const bson_t *filter, bson_error_t *error);

// Cursor operations (implemented in mongolite_query.c)
bool mlite_cursor_next(mlite_cursor_t *cursor, const bson_t **doc);
bool mlite_cursor_error(mlite_cursor_t *cursor, bson_error_t *error);
void mlite_cursor_destroy(mlite_cursor_t *cursor);

// SQL abstraction layer (implemented in mongolite_sql.c)
// Schema operations
int mlite_sql_init_schema(sqlite3 *db);

// Collection operations  
int mlite_sql_create_collection_table(sqlite3 *db, const char *collection_name);
int mlite_sql_add_collection_metadata(sqlite3 *db, const char *collection_name);
int mlite_sql_drop_collection_table(sqlite3 *db, const char *collection_name);
int mlite_sql_remove_collection_metadata(sqlite3 *db, const char *collection_name);
bool mlite_sql_collection_exists(sqlite3 *db, const char *collection_name);

// Document operations
int mlite_sql_prepare_document_insert(sqlite3 *db, const char *collection_name, sqlite3_stmt **stmt);
int mlite_sql_insert_document(sqlite3_stmt *stmt, const char *oid_str, const uint8_t *bson_data, uint32_t bson_len);

// Transaction operations
int mlite_sql_begin_transaction(sqlite3 *db);
int mlite_sql_commit_transaction(sqlite3 *db);
int mlite_sql_rollback_transaction(sqlite3 *db);

// Query operations
int mlite_sql_prepare_collection_query(sqlite3 *db, const char *collection_name, sqlite3_stmt **stmt);
int mlite_sql_query_step(sqlite3_stmt *stmt, const char **oid_str, const void **document_data, int *document_len);

#endif // MONGOLITE_INTERNAL_H