#ifndef MONGOLITE_INTERNAL_H
#define MONGOLITE_INTERNAL_H

#include "mongolite.h"
#include "wtree.h"
#include <bson/bson.h>

/* Forward declaration for bsonmatch (to avoid including full header) */
typedef struct _mongoc_matcher_t mongoc_matcher_t;

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Constants
 * ============================================================ */

#define MONGOLITE_VERSION "0.1.0"

/* Tree naming conventions */
#define MONGOLITE_SCHEMA_TREE   "_mongolite_schema"
#define MONGOLITE_COL_PREFIX    "col:"
#define MONGOLITE_IDX_PREFIX    "idx:"

/* Default limits */
#define MONGOLITE_DEFAULT_MAPSIZE     (1024ULL * 1024 * 1024)  /* 1GB */
#define MONGOLITE_DEFAULT_MAX_DBS     256
#define MONGOLITE_DEFAULT_MAX_COLLECTIONS 128

/* Schema document field names */
#define SCHEMA_FIELD_ID           "_id"
#define SCHEMA_FIELD_NAME         "name"
#define SCHEMA_FIELD_TREE_NAME    "tree_name"
#define SCHEMA_FIELD_TYPE         "type"
#define SCHEMA_FIELD_CREATED_AT   "created_at"
#define SCHEMA_FIELD_MODIFIED_AT  "modified_at"
#define SCHEMA_FIELD_DOC_COUNT    "doc_count"
#define SCHEMA_FIELD_INDEXES      "indexes"
#define SCHEMA_FIELD_OPTIONS      "options"
#define SCHEMA_FIELD_METADATA     "metadata"

/* Schema type values */
#define SCHEMA_TYPE_COLLECTION    "collection"
#define SCHEMA_TYPE_INDEX         "index"

/* ============================================================
 * Error Codes
 *
 * To avoid overlap with LMDB and libbson errors, mongolite uses
 * a dedicated range: -1000 to -1999
 *
 * Error handling strategy:
 * - LMDB errors: passed through as-is (typically MDB_* codes)
 * - libbson errors: use bson_error_t separately
 * - Mongolite errors: use MONGOLITE_E* codes below
 * - gerror_t.lib field identifies the source ("mongolite", "wtree", etc.)
 * ============================================================ */

#define MONGOLITE_OK            0
#define MONGOLITE_ERROR        -1000   /* Generic error */
#define MONGOLITE_ENOTFOUND    -1001   /* Collection/document not found */
#define MONGOLITE_EEXISTS      -1002   /* Already exists */
#define MONGOLITE_EINVAL       -1003   /* Invalid argument */
#define MONGOLITE_ENOMEM       -1004   /* Out of memory */
#define MONGOLITE_EIO          -1005   /* I/O error */
#define MONGOLITE_ESCHEMA      -1006   /* Schema error */
#define MONGOLITE_ETXN         -1007   /* Transaction error */
#define MONGOLITE_EQUERY       -1008   /* Query/filter error */
#define MONGOLITE_EINDEX       -1009   /* Index error */
#define MONGOLITE_ECAPPED      -1010   /* Capped collection error */
#define MONGOLITE_EVALIDATION  -1011   /* Validation error */

/* Check if error code is from mongolite range */
#define MONGOLITE_IS_ERROR(code) ((code) <= -1000 && (code) >= -1999)

/* ============================================================
 * Internal Structures
 * ============================================================ */

/*
 * Cached tree handle (for open collection/index trees)
 */
typedef struct mongolite_tree_cache_entry {
    bson_oid_t oid;             /* Tree's unique identifier */
    char *name;                 /* Collection/index name */
    char *tree_name;            /* Full LMDB tree name (col:xxx or idx:xxx) */
    wtree_tree_t *tree;         /* Open tree handle */
    struct mongolite_tree_cache_entry *next;
} mongolite_tree_cache_entry_t;

/*
 * Main database handle
 */
struct mongolite_db {
    /* LMDB backend */
    wtree_db_t *wdb;                    /* LMDB environment */
    wtree_tree_t *schema_tree;          /* _mongolite_schema tree */

    /* Configuration (copied from open) */
    char *path;                         /* Database directory path */
    int open_flags;                     /* MONGOLITE_OPEN_* flags */
    size_t max_bytes;
    unsigned int max_dbs;

    /* State */
    int64_t last_insert_rowid;          /* Last generated _id as int64 */
    int changes;                        /* Docs affected by last operation */
    bool in_transaction;                /* Explicit transaction active */
    wtree_txn_t *current_txn;           /* Current explicit transaction */

    /* Tree cache (simple linked list for now) */
    mongolite_tree_cache_entry_t *tree_cache;
    size_t tree_cache_count;

    /* Database metadata (user-defined, from config) */
    bson_t *db_metadata;

    /* Thread safety (if FULLMUTEX) */
#ifdef _WIN32
    void *mutex;                        /* CRITICAL_SECTION* on Windows */
#else
    pthread_mutex_t *mutex;
#endif
};

/*
 * Cursor for iterating query results
 */
struct mongolite_cursor {
    mongolite_db_t *db;
    char *collection_name;

    /* Iteration state */
    wtree_txn_t *txn;                   /* Read transaction */
    wtree_iterator_t *iter;             /* Tree iterator */
    bool owns_txn;                      /* Did we create the transaction? */

    /* Query */
    mongoc_matcher_t *matcher;          /* Filter (from bsonmatch) */
    bson_t *projection;                 /* Field projection */
    bson_t *sort;                       /* Sort specification */

    /* Pagination */
    int64_t limit;                      /* Max results (0 = unlimited) */
    int64_t skip;                       /* Skip count */
    int64_t position;                   /* Current position */
    int64_t returned;                   /* Documents returned so far */

    /* Current document */
    bson_t *current_doc;                /* Current document (owned) */
    bool exhausted;                     /* No more results */

    /* Sort buffer (if sorting required) */
    bson_t **sort_buffer;               /* Buffered docs for sorting */
    size_t sort_buffer_size;
    size_t sort_buffer_pos;
};

/* ============================================================
 * Internal Schema Operations
 * ============================================================ */

/*
 * Schema entry - represents a collection or index in _mongolite_schema
 */
typedef struct mongolite_schema_entry {
    bson_oid_t oid;                     /* Unique identifier */
    char *name;                         /* Collection/index name */
    char *tree_name;                    /* LMDB tree name */
    char *type;                         /* "collection" or "index" */
    int64_t created_at;                 /* Creation timestamp (ms) */
    int64_t modified_at;                /* Last modification (ms) */
    int64_t doc_count;                  /* Document count (collections) */
    bson_t *indexes;                    /* Index definitions (collections) */
    bson_t *options;                    /* Creation options */
    bson_t *metadata;                   /* User metadata */

    /* For indexes */
    char *collection_name;              /* Parent collection (if index) */
    bson_t *keys;                       /* Index keys (if index) */
    bool unique;
    bool sparse;
} mongolite_schema_entry_t;

/* Schema operations (internal) */
int _mongolite_schema_init(mongolite_db_t *db, gerror_t *error);
int _mongolite_schema_get(mongolite_db_t *db, const char *name,
                          mongolite_schema_entry_t *entry, gerror_t *error);
int _mongolite_schema_put(mongolite_db_t *db, const mongolite_schema_entry_t *entry,
                          gerror_t *error);
int _mongolite_schema_delete(mongolite_db_t *db, const char *name, gerror_t *error);
int _mongolite_schema_list(mongolite_db_t *db, char ***names, size_t *count,
                           const char *type_filter, gerror_t *error);
void _mongolite_schema_entry_free(mongolite_schema_entry_t *entry);

/* Schema serialization */
bson_t* _mongolite_schema_entry_to_bson(const mongolite_schema_entry_t *entry);
int _mongolite_schema_entry_from_bson(const bson_t *doc,
                                       mongolite_schema_entry_t *entry,
                                       gerror_t *error);

/* ============================================================
 * Internal Tree Cache Operations
 * ============================================================ */

wtree_tree_t* _mongolite_tree_cache_get(mongolite_db_t *db, const char *name);
int _mongolite_tree_cache_put(mongolite_db_t *db, const char *name,
                              const char *tree_name, const bson_oid_t *oid,
                              wtree_tree_t *tree);
void _mongolite_tree_cache_remove(mongolite_db_t *db, const char *name);
void _mongolite_tree_cache_clear(mongolite_db_t *db);

/* ============================================================
 * Internal Utilities
 * ============================================================ */

/* Build tree names */
char* _mongolite_collection_tree_name(const char *collection_name);
char* _mongolite_index_tree_name(const char *collection_name, const char *index_name);

/* Timestamp helpers */
int64_t _mongolite_now_ms(void);

/* OID helpers */
int64_t _mongolite_oid_to_rowid(const bson_oid_t *oid);

/* Lock helpers */
void _mongolite_lock(mongolite_db_t *db);
void _mongolite_unlock(mongolite_db_t *db);

/* Transaction helpers */
wtree_txn_t* _mongolite_get_write_txn(mongolite_db_t *db, gerror_t *error);
wtree_txn_t* _mongolite_get_read_txn(mongolite_db_t *db, gerror_t *error);
int _mongolite_commit_if_auto(mongolite_db_t *db, wtree_txn_t *txn, gerror_t *error);
void _mongolite_abort_if_auto(mongolite_db_t *db, wtree_txn_t *txn);

/* ============================================================
 * Internal Collection Operations
 * ============================================================ */

/* Get or open a collection's tree handle (uses cache) */
wtree_tree_t* _mongolite_get_collection_tree(mongolite_db_t *db, const char *name,
                                              gerror_t *error);

#ifdef __cplusplus
}
#endif

#endif /* MONGOLITE_INTERNAL_H */
