#ifndef MONGOLITE_API_H
#define MONGOLITE_API_H

#include <stdbool.h>
#include <stdint.h>
#include "gerror.h"

#include <bson/bson.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Forward Declarations (opaque types)
 * ============================================================ */

typedef struct mongolite_db mongolite_db_t;
typedef struct mongolite_cursor mongolite_cursor_t;

/* ============================================================
 * Configuration Structures
 *
 * All fields are optional - zero/NULL values use defaults.
 * Structures are designed for extensibility (add new fields at end).
 * ============================================================ */

/*
 * Database configuration (passed to mongolite_open)
 *
 * Example usage:
 *   db_config_t config = {0};  // Zero-init for defaults
 *   config.max_bytes = 10ULL * 1024 * 1024 * 1024;  // 10GB
 *   config.metadata = my_app_metadata_bson;
 *   mongolite_open("./mydb", &db, &config, &error);
 */
typedef struct db_config {
    /* LMDB backend settings */
    size_t max_bytes;           /* Max database size (default: 1GB) */
    unsigned int max_dbs;       /* Max named trees (default: 256) */
    unsigned int lmdb_flags;    /* LMDB flags: MDB_NOSYNC, MDB_WRITEMAP, etc. */

    /* Mongolite limits */
    size_t max_collections;     /* Soft limit on collections (default: 128) */

    /* Document cache settings (0 = disabled) */
    size_t cache_max_items;     /* Max cached documents */
    int64_t cache_max_bytes;    /* Max cache memory in bytes */
    uint64_t cache_ttl_ms;      /* Default cache TTL in milliseconds */

    /* User-defined metadata (stored in DB, retrievable via mongolite_db_metadata) */
    const bson_t *metadata;     /* Optional BSON - will be copied */

    /* Reserved for future expansion */
    void *_reserved[4];
} db_config_t;

/*
 * Collection configuration (passed to mongolite_collection_create)
 *
 * Example:
 *   col_config_t config = {0};
 *   config.capped = true;
 *   config.max_docs = 10000;
 *   mongolite_collection_create(db, "logs", &config, &error);
 */
typedef struct col_config {
    /* Capped collection settings */
    bool capped;                /* Is this a capped collection? */
    int64_t max_docs;           /* Max documents (capped only) */
    int64_t max_bytes;          /* Max size in bytes (capped only) */

    /* Validation (future) */
    const bson_t *validator;    /* JSON Schema validator document */

    /* User-defined metadata */
    const bson_t *metadata;     /* Optional BSON - will be copied */

    /* Reserved for future expansion */
    void *_reserved[4];
} col_config_t;

/*
 * Index configuration (passed to mongolite_create_index)
 *
 * Example:
 *   index_config_t config = {0};
 *   config.unique = true;
 *   mongolite_create_index(db, "users", keys, "email_unique", &config, &error);
 */
typedef struct index_config {
    bool unique;                /* Enforce uniqueness */
    bool sparse;                /* Skip docs without indexed fields */
    bool background;            /* Build in background (future) */

    /* TTL index */
    int64_t expire_after_seconds;  /* Auto-delete after N seconds (0 = disabled) */

    /* Partial filter (future) */
    const bson_t *partial_filter;

    /* User-defined metadata */
    const bson_t *metadata;     /* Optional BSON - will be copied */

    /* Reserved for future expansion */
    void *_reserved[4];
} index_config_t;



// Open flags (similar to SQLite)
#define MONGOLITE_OPEN_READONLY    0x00000001
#define MONGOLITE_OPEN_READWRITE   0x00000002
#define MONGOLITE_OPEN_CREATE      0x00000004
#define MONGOLITE_OPEN_NOMUTEX     0x00008000
#define MONGOLITE_OPEN_FULLMUTEX   0x00010000

// ============= Database Operations =============

// Open/Close (SQLite-style)
int mongolite_open(const char *filename, mongolite_db_t **db, db_config_t *config, gerror_t *error);
int mongolite_close(mongolite_db_t *db);

// Database info
const char* mongolite_db_filename(mongolite_db_t *db);
int64_t mongolite_last_insert_rowid(mongolite_db_t *db);  // Para _id auto-generated
int mongolite_changes(mongolite_db_t *db);  // Docs affected by last operation

// ============= Collection Operations =============

int mongolite_collection_create(mongolite_db_t *db, const char *name, col_config_t *config, gerror_t *error);
int mongolite_collection_drop(mongolite_db_t *db, const char *name, gerror_t *error);
bool mongolite_collection_exists(mongolite_db_t *db, const char *name, gerror_t *error);

// List collections
char** mongolite_collection_list(mongolite_db_t *db, size_t *count, gerror_t *error);
void mongolite_collection_list_free(char **list, size_t count);

// Collection stats
int64_t mongolite_collection_count(mongolite_db_t *db, const char *collection,
                                   const bson_t *filter, gerror_t *error);

// ============= Document Operations =============

// Insert
int mongolite_insert_one(mongolite_db_t *db, const char *collection, 
                        const bson_t *doc, bson_oid_t *inserted_id, gerror_t *error);

int mongolite_insert_many(mongolite_db_t *db, const char *collection,
                         const bson_t **docs, size_t n_docs, 
                         bson_oid_t **inserted_ids, gerror_t *error);
int mongolite_insert_one_json(mongolite_db_t *db, const char *collection, 
                        const char *json_str, bson_oid_t *inserted_id, gerror_t *error);

int mongolite_insert_many_json(mongolite_db_t *db, const char *collection,
                         const char **json_strs, size_t n_docs, 
                         bson_oid_t **inserted_ids, gerror_t *error);
// Find
bson_t* mongolite_find_one(mongolite_db_t *db, const char *collection,
                           const bson_t *filter, const bson_t *projection,
                           gerror_t *error);

const char* mongolite_find_one_json(mongolite_db_t *db, const char *collection,
                           const char *filter_json, const char *projection_json,
                           gerror_t *error);

mongolite_cursor_t* mongolite_find(mongolite_db_t *db, const char *collection,
                                   const bson_t *filter, const bson_t *projection,
                                   gerror_t *error);
char** mongolite_find_json(mongolite_db_t *db, const char *collection,
                                   const char *filter_json, const char *projection_json,
                                   gerror_t *error);

// Update
int mongolite_update_one(mongolite_db_t *db, const char *collection,
                         const bson_t *filter, const bson_t *update,
                         bool upsert, gerror_t *error);
int mongolite_update_one_json(mongolite_db_t *db, const char *collection,
                         const char *filter_json, const char *update_json,
                         bool upsert, gerror_t *error);

int mongolite_update_many(mongolite_db_t *db, const char *collection,
                         const bson_t *filter, const bson_t *update,
                         bool upsert, int64_t *modified_count, gerror_t *error);
int mongolite_update_many_json(mongolite_db_t *db, const char *collection,
                         const char *filter_json, const char *update_json,
                         bool upsert, int64_t *modified_count, gerror_t *error);

int mongolite_replace_one(mongolite_db_t *db, const char *collection,
                         const bson_t *filter, const bson_t *replacement,
                         bool upsert, gerror_t *error);
int mongolite_replace_one_json(mongolite_db_t *db, const char *collection,
                         const char *filter_json, const char *replacement_json,
                         bool upsert, gerror_t *error);

// Find and modify (atomic operations)
bson_t* mongolite_find_and_modify(mongolite_db_t *db, const char *collection,
                                  const bson_t *filter, const bson_t *update,
                                  bool return_new, bool upsert, gerror_t *error);
bson_t* mongolite_find_and_modify_json(mongolite_db_t *db, const char *collection,
                                       const char *filter_json, const char *update_json,
                                       bool return_new, bool upsert, gerror_t *error);

// Delete
int mongolite_delete_one(mongolite_db_t *db, const char *collection,
                         const bson_t *filter, gerror_t *error);

int mongolite_delete_many(mongolite_db_t *db, const char *collection,
                         const bson_t *filter, int64_t *deleted_count, 
                         gerror_t *error);

// ============= Cursor Operations =============

bool mongolite_cursor_next(mongolite_cursor_t *cursor, const bson_t **doc);
bool mongolite_cursor_more(mongolite_cursor_t *cursor);
void mongolite_cursor_destroy(mongolite_cursor_t *cursor);

// Cursor modifiers (before iteration)
int mongolite_cursor_set_limit(mongolite_cursor_t *cursor, int64_t limit);
int mongolite_cursor_set_skip(mongolite_cursor_t *cursor, int64_t skip);
int mongolite_cursor_set_sort(mongolite_cursor_t *cursor, const bson_t *sort);

// ============= Index Operations =============

int mongolite_create_index(mongolite_db_t *db, const char *collection,
                          const bson_t *keys, const char *name,
                          index_config_t *config, gerror_t *error);

int mongolite_drop_index(mongolite_db_t *db, const char *collection,
                        const char *index_name, gerror_t *error);

// ============= Transaction Support (Optional) =============

int mongolite_begin_transaction(mongolite_db_t *db);
int mongolite_commit(mongolite_db_t *db);
int mongolite_rollback(mongolite_db_t *db);

// ============= Aggregation Pipeline (Future) =============

mongolite_cursor_t* mongolite_aggregate(mongolite_db_t *db, const char *collection,
                                       const bson_t *pipeline, gerror_t *error);

// ============= Utility =============

const char* mongolite_version(void);
const char* mongolite_errstr(int error_code);

// Database metadata (user-defined, from db_config)
const bson_t* mongolite_db_metadata(mongolite_db_t *db);
int mongolite_db_set_metadata(mongolite_db_t *db, const bson_t *metadata, gerror_t *error);

// Collection metadata
const bson_t* mongolite_collection_metadata(mongolite_db_t *db, const char *collection, gerror_t *error);
int mongolite_collection_set_metadata(mongolite_db_t *db, const char *collection,
                                      const bson_t *metadata, gerror_t *error);

// Database sync (flush to disk)
int mongolite_sync(mongolite_db_t *db, bool force, gerror_t *error);

// BSON helpers specific to mongolite
bson_t* mongolite_matcher_regex(const char *field, const char *pattern, const char *options);
bson_t* mongolite_matcher_in(const char *field, const bson_t *values);
bson_t* mongolite_matcher_exists(const char *field, bool exists);

#ifdef __cplusplus
}
#endif

#endif // MONGOLITE_API_H