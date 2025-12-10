#ifndef MONGOLITE_API_H
#define MONGOLITE_API_H

#include <stdbool.h>
#include <stdint.h>
#include "wbson.h"
#include "gerror.h"

#ifdef __cplusplus
extern "C" {
#endif

// Forward declarations
typedef struct mongolite_db mongolite_db_t;
typedef struct mongolite_cursor mongolite_cursor_t;
typedef struct mongolite_options mongolite_options_t;

// Open flags (similar to SQLite)
#define MONGOLITE_OPEN_READONLY    0x00000001
#define MONGOLITE_OPEN_READWRITE   0x00000002
#define MONGOLITE_OPEN_CREATE      0x00000004
#define MONGOLITE_OPEN_NOMUTEX     0x00008000
#define MONGOLITE_OPEN_FULLMUTEX   0x00010000

// ============= Database Operations =============

// Open/Close (SQLite-style)
int mongolite_open(const char *filename, mongolite_db_t **db);
int mongolite_open_v2(const char *filename, mongolite_db_t **db, int flags, const char *vfs);
int mongolite_close(mongolite_db_t *db);

// Database info
const char* mongolite_db_filename(mongolite_db_t *db);
int64_t mongolite_last_insert_rowid(mongolite_db_t *db);  // Para _id auto-generated
int mongolite_changes(mongolite_db_t *db);  // Docs affected by last operation

// ============= Collection Operations =============

int mongolite_collection_create(mongolite_db_t *db, const char *name, gerror_t *error);
int mongolite_collection_drop(mongolite_db_t *db, const char *name, gerror_t *error);
bool mongolite_collection_exists(mongolite_db_t *db, const char *name, gerror_t *error);

// List collections
char** mongolite_collection_list(mongolite_db_t *db, size_t *count, gerror_t *error);
void mongolite_collection_list_free(char **list, size_t count);

// Collection stats
int64_t mongolite_collection_count(mongolite_db_t *db, const char *collection,
                                   const wbson_t *filter, gerror_t *error);

// ============= Document Operations =============

// Insert
int mongolite_insert_one(mongolite_db_t *db, const char *collection, 
                        const wbson_t *doc, wbson_oid_t *inserted_id, gerror_t *error);

int mongolite_insert_many(mongolite_db_t *db, const char *collection,
                         const wbson_t **docs, size_t n_docs, 
                         wbson_oid_t **inserted_ids, gerror_t *error);
// Find
wbson_t* mongolite_find_one(mongolite_db_t *db, const char *collection,
                           const wbson_t *filter, const wbson_t *projection,
                           gerror_t *error);

mongolite_cursor_t* mongolite_find(mongolite_db_t *db, const char *collection,
                                   const wbson_t *filter, const wbson_t *projection,
                                   gerror_t *error);

// Update
int mongolite_update_one(mongolite_db_t *db, const char *collection,
                         const wbson_t *filter, const wbson_t *update,
                         bool upsert, gerror_t *error);

int mongolite_update_many(mongolite_db_t *db, const char *collection,
                         const wbson_t *filter, const wbson_t *update,
                         bool upsert, int64_t *modified_count, gerror_t *error);

int mongolite_replace_one(mongolite_db_t *db, const char *collection,
                         const wbson_t *filter, const wbson_t *replacement,
                         bool upsert, gerror_t *error);

// Delete
int mongolite_delete_one(mongolite_db_t *db, const char *collection,
                         const wbson_t *filter, gerror_t *error);

int mongolite_delete_many(mongolite_db_t *db, const char *collection,
                         const wbson_t *filter, int64_t *deleted_count, 
                         gerror_t *error);

// ============= Cursor Operations =============

bool mongolite_cursor_next(mongolite_cursor_t *cursor, const wbson_t **doc);
bool mongolite_cursor_more(mongolite_cursor_t *cursor);
void mongolite_cursor_destroy(mongolite_cursor_t *cursor);

// Cursor modifiers (before iteration)
int mongolite_cursor_set_limit(mongolite_cursor_t *cursor, int64_t limit);
int mongolite_cursor_set_skip(mongolite_cursor_t *cursor, int64_t skip);
int mongolite_cursor_set_sort(mongolite_cursor_t *cursor, const wbson_t *sort);

// ============= Index Operations =============

int mongolite_create_index(mongolite_db_t *db, const char *collection,
                          const wbson_t *keys, const char *name,
                          bool unique, gerror_t *error);

int mongolite_drop_index(mongolite_db_t *db, const char *collection,
                        const char *index_name, gerror_t *error);

// ============= Transaction Support (Optional) =============

int mongolite_begin_transaction(mongolite_db_t *db);
int mongolite_commit(mongolite_db_t *db);
int mongolite_rollback(mongolite_db_t *db);

// ============= Aggregation Pipeline (Future) =============

mongolite_cursor_t* mongolite_aggregate(mongolite_db_t *db, const char *collection,
                                       const wbson_t *pipeline, gerror_t *error);

// ============= Utility =============

const char* mongolite_version(void);
const char* mongolite_errstr(int error_code);

// BSON helpers specific to mongolite
wbson_t* mongolite_matcher_regex(const char *field, const char *pattern, const char *options);
wbson_t* mongolite_matcher_in(const char *field, const wbson_t *values);
wbson_t* mongolite_matcher_exists(const char *field, bool exists);

#ifdef __cplusplus
}
#endif

#endif // MONGOLITE_API_H