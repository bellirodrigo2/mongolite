#ifndef MONGOLITE_INTERNAL_H
#define MONGOLITE_INTERNAL_H

#include "mongolite.h"
#include "wtree3/wtree3.h"
#include <bson/bson.h>
#include "mongolite_helpers.h"

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
/* Note: SCHEMA_TYPE_INDEX removed - indexes now fully managed by wtree3 */

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

/* Translate wtree3 error codes to mongolite error codes */
static inline int _mongolite_translate_wtree3_error(int wtree3_rc) {
    switch (wtree3_rc) {
        case WTREE3_OK:          return MONGOLITE_OK;
        case WTREE3_NOT_FOUND:   return MONGOLITE_ENOTFOUND;
        case WTREE3_KEY_EXISTS:  return MONGOLITE_EEXISTS;
        case WTREE3_EINVAL:      return MONGOLITE_EINVAL;
        case WTREE3_ENOMEM:      return MONGOLITE_ENOMEM;
        case WTREE3_INDEX_ERROR: return MONGOLITE_EINDEX;
        case WTREE3_MAP_FULL:    return WTREE3_MAP_FULL;  /* Pass through for resize */
        case WTREE3_TXN_FULL:    return WTREE3_TXN_FULL;  /* Pass through for resize */
        default:                 return MONGOLITE_ERROR;
    }
}

/* ============================================================
 * Internal Structures
 * ============================================================ */

/*
 * Cached index info for a collection (used for query optimization)
 * Note: Index trees are now managed internally by wtree2
 */
typedef struct mongolite_cached_index {
    char *name;                 /* Index name (e.g., "email_1") */
    bson_t *keys;               /* Index key spec (e.g., {"email": 1}) */
    bool unique;
    bool sparse;
    MDB_dbi dbi;                /* Index DBI handle (from wtree3) */
} mongolite_cached_index_t;

/*
 * Cached tree handle (for open collection trees)
 * Note: Index trees are now managed internally by wtree3
 */
typedef struct mongolite_tree_cache_entry {
    bson_oid_t oid;             /* Tree's unique identifier */
    char *name;                 /* Collection name */
    char *tree_name;            /* Full LMDB tree name (col:xxx) */
    wtree3_tree_t *tree;        /* Open tree handle (wtree3 - manages indexes) */

    /* Cached index specs for query optimization (not tree handles) */
    mongolite_cached_index_t *indexes;  /* Array of cached index specs */
    size_t index_count;                 /* Number of indexes (excluding _id) */
    bool indexes_loaded;                /* true if index specs have been loaded */

    struct mongolite_tree_cache_entry *next;
} mongolite_tree_cache_entry_t;

/*
 * Main database handle
 */
struct mongolite_db {
    /* LMDB backend (wtree3 for unified index-aware operations) */
    wtree3_db_t *wdb;                   /* LMDB environment (wtree3) */
    wtree3_tree_t *schema_tree;         /* _mongolite_schema tree (wtree3) */

    /* Configuration (copied from open) */
    char *path;                         /* Database directory path */
    int open_flags;                     /* MONGOLITE_OPEN_* flags */
    size_t max_bytes;
    unsigned int max_dbs;
    uint32_t version;                   /* Schema version for extractors */

    /* State */
    int64_t last_insert_rowid;          /* Last generated _id as int64 */
    int changes;                        /* Docs affected by last operation */
    bool in_transaction;                /* Explicit transaction active */
    wtree3_txn_t *current_txn;          /* Current explicit transaction (wtree3) */

    /* Read transaction pool (optimization: reuse via reset/renew) */
    wtree3_txn_t *read_txn_pool;        /* Cached read transaction (wtree3) */

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
    wtree3_txn_t *txn;                  /* Read transaction (wtree3) */
    wtree3_iterator_t *iter;            /* Tree iterator (wtree3) */
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
 * Schema entry - represents a collection in _mongolite_schema
 * Note: Index metadata now stored entirely in wtree3's index persistence system
 */
typedef struct mongolite_schema_entry {
    bson_oid_t oid;                     /* Unique identifier */
    char *name;                         /* Collection name */
    char *tree_name;                    /* LMDB tree name (e.g., "col:users") */
    char *type;                         /* Always "collection" now */
    int64_t created_at;                 /* Creation timestamp (ms) */
    int64_t modified_at;                /* Last modification (ms) */
    int64_t doc_count;                  /* Document count */
    bson_t *options;                    /* Creation options (capped, validators, etc.) */
    bson_t *metadata;                   /* User-defined metadata */

    /* REMOVED: indexes, collection_name, keys, unique, sparse */
    /* These are now managed by wtree3's index persistence system */
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

wtree3_tree_t* _mongolite_tree_cache_get(mongolite_db_t *db, const char *name);
int _mongolite_tree_cache_put(mongolite_db_t *db, const char *name,
                              const char *tree_name, const bson_oid_t *oid,
                              wtree3_tree_t *tree);
void _mongolite_tree_cache_remove(mongolite_db_t *db, const char *name);
void _mongolite_tree_cache_clear(mongolite_db_t *db);

/* Index specs cache operations (for query optimization) */
mongolite_cached_index_t* _mongolite_get_cached_indexes(mongolite_db_t *db,
                                                         const char *collection,
                                                         size_t *out_count,
                                                         gerror_t *error);
void _mongolite_invalidate_index_cache(mongolite_db_t *db, const char *collection);

/* ============================================================
 * Internal Utilities
 * ============================================================ */

/* Build tree names */
char* _mongolite_collection_tree_name(const char *collection_name);

/* Timestamp helpers */
int64_t _mongolite_now_ms(void);

/* OID helpers */
int64_t _mongolite_oid_to_rowid(const bson_oid_t *oid);

/* Lock helpers */
int _mongolite_lock_init(mongolite_db_t *db);
void _mongolite_lock_free(mongolite_db_t *db);
void _mongolite_lock(mongolite_db_t *db);
void _mongolite_unlock(mongolite_db_t *db);

/* Platform helpers */
char* _mongolite_strndup(const char *s, size_t n);

/* Transaction helpers (wtree3) */
wtree3_txn_t* _mongolite_get_write_txn(mongolite_db_t *db, gerror_t *error);
wtree3_txn_t* _mongolite_get_read_txn(mongolite_db_t *db, gerror_t *error);
void _mongolite_release_read_txn(mongolite_db_t *db, wtree3_txn_t *txn);
int _mongolite_commit_if_auto(mongolite_db_t *db, wtree3_txn_t *txn, gerror_t *error);
void _mongolite_abort_if_auto(mongolite_db_t *db, wtree3_txn_t *txn);

/* Doc count update (within existing transaction) - may be removed when fully migrated */
int _mongolite_update_doc_count_txn(mongolite_db_t *db, wtree3_txn_t *txn,
                                     const char *collection, int64_t delta,
                                     gerror_t *error);

/* Auto-resize database on MDB_MAP_FULL (doubles mapsize) */
int _mongolite_try_resize(mongolite_db_t *db, gerror_t *error);

/* Query optimization helpers */
bool _mongolite_is_id_query(const bson_t *filter, bson_oid_t *out_oid);
bson_t* _mongolite_find_by_id(mongolite_db_t *db, wtree3_tree_t *tree,
                               const bson_oid_t *oid, gerror_t *error);

/* ============================================================
 * Internal Index Operations (Phase 1 Infrastructure)
 * ============================================================ */

/* Generate default index name from key spec (e.g., "email_1", "name_1_age_-1")
 * Returns: allocated string (caller must free), or NULL on error */
char* _index_name_from_spec(const bson_t *keys);

/* Build index key from document
 * include_id: true to append document's _id (for tree uniqueness)
 * Returns: allocated bson_t (caller must destroy), or NULL on error */
bson_t* _build_index_key(const bson_t *doc, const bson_t *keys, bool include_id);

/* Build key for unique constraint checking (without _id) */
bson_t* _build_unique_check_key(const bson_t *doc, const bson_t *keys);

/* Index key comparator for wtree (uses bson_compare_docs) */
int _index_key_compare(const void *key1, size_t key1_len,
                       const void *key2, size_t key2_len,
                       void *user_data);

/* LMDB-style index comparator wrapper for wtree3 */
int _mongolite_index_compare(const MDB_val *a, const MDB_val *b);

/* Serialize/deserialize index keys */
uint8_t* _index_key_serialize(const bson_t *key, size_t *out_len);
bson_t* _index_key_deserialize(const uint8_t *data, size_t len);

/* Index value helpers (stores _id for document lookup) */
uint8_t* _index_value_from_doc(const bson_t *doc, size_t *out_len);
bool _index_value_get_oid(const uint8_t *data, size_t len, bson_oid_t *out_oid);

/* Index metadata serialization for schema storage */
bson_t* _index_spec_to_bson(const char *name, const bson_t *keys,
                            const index_config_t *config);
int _index_spec_from_bson(const bson_t *spec, char **out_name,
                          bson_t **out_keys, index_config_t *out_config);

/* Check if document should be indexed (sparse index handling) */
bool _should_index_document(const bson_t *doc, const bson_t *keys, bool sparse);

/* ============================================================
 * Internal Query Optimization (Phase 4)
 * ============================================================ */

/*
 * Query analysis result - identifies fields that can use an index
 */
typedef struct {
    char **equality_fields;     /* Fields with simple equality (e.g., {"email": "x"}) */
    size_t equality_count;
    bool is_simple_equality;    /* true if query is only simple equality conditions */
} query_analysis_t;

/* Analyze a query filter for index usage potential */
query_analysis_t* _analyze_query_for_index(const bson_t *filter);
void _free_query_analysis(query_analysis_t *analysis);

/* Find the best index for a query (returns NULL if no suitable index) */
mongolite_cached_index_t* _find_best_index(mongolite_db_t *db, const char *collection,
                                            const query_analysis_t *analysis,
                                            gerror_t *error);

/* Use index to find documents matching a simple equality query */
bson_t* _find_one_with_index(mongolite_db_t *db, const char *collection,
                              wtree3_tree_t *col_tree,
                              mongolite_cached_index_t *index,
                              const bson_t *filter, gerror_t *error);

/* ============================================================
 * Internal Collection Operations
 * ============================================================ */

/* Get or open a collection's tree handle (uses cache) */
wtree3_tree_t* _mongolite_get_collection_tree(mongolite_db_t *db, const char *name,
                                               gerror_t *error);

/* ============================================================
 * Internal Cursor Operations
 * ============================================================ */

/*
 * Create a cursor using an existing transaction.
 * IMPORTANT: Caller must already hold _mongolite_lock(db).
 *            Caller is responsible for the transaction lifecycle.
 *            The cursor will NOT own the transaction (owns_txn = false).
 */
mongolite_cursor_t* _mongolite_cursor_create_with_txn(mongolite_db_t *db,
                                                       wtree3_tree_t *tree,
                                                       const char *collection,
                                                       wtree3_txn_t *txn,
                                                       const bson_t *filter,
                                                       gerror_t *error);

#ifdef __cplusplus
}
#endif

#endif /* MONGOLITE_INTERNAL_H */
