/*
 * mongolite_db.c - Database core operations
 *
 * Handles:
 * - Database open/close
 * - Schema tree management (_mongolite_schema)
 * - Tree cache
 * - Transaction helpers
 * - Utility functions
 */

#include "mongolite_internal.h"
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>

#ifdef _WIN32
#include <windows.h>
#include <direct.h>
#define mkdir(path, mode) _mkdir(path)
/* strndup not available on Windows */
static char* strndup(const char *s, size_t n) {
    size_t len = strlen(s);
    if (len > n) len = n;
    char *result = malloc(len + 1);
    if (result) {
        memcpy(result, s, len);
        result[len] = '\0';
    }
    return result;
}
#else
#include <pthread.h>
#include <sys/time.h>
#endif

#define MONGOLITE_LIB "mongolite"

/* ============================================================
 * Timestamp Helpers
 * ============================================================ */

int64_t _mongolite_now_ms(void) {
#ifdef _WIN32
    FILETIME ft;
    ULARGE_INTEGER uli;
    GetSystemTimeAsFileTime(&ft);
    uli.LowPart = ft.dwLowDateTime;
    uli.HighPart = ft.dwHighDateTime;
    /* Convert from 100-nanosecond intervals since 1601 to ms since 1970 */
    return (int64_t)((uli.QuadPart - 116444736000000000ULL) / 10000);
#else
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
#endif
}

/* ============================================================
 * OID Helpers
 * ============================================================ */

int64_t _mongolite_oid_to_rowid(const bson_oid_t *oid) {
    if (!oid) return 0;
    /* Use last 8 bytes of OID as a pseudo row-id */
    int64_t rowid = 0;
    memcpy(&rowid, oid->bytes + 4, 8);
    return rowid;
}

/* ============================================================
 * Lock Helpers
 * ============================================================ */

void _mongolite_lock(mongolite_db_t *db) {
    if (!db || !db->mutex) return;
#ifdef _WIN32
    EnterCriticalSection((CRITICAL_SECTION*)db->mutex);
#else
    pthread_mutex_lock(db->mutex);
#endif
}

void _mongolite_unlock(mongolite_db_t *db) {
    if (!db || !db->mutex) return;
#ifdef _WIN32
    LeaveCriticalSection((CRITICAL_SECTION*)db->mutex);
#else
    pthread_mutex_unlock(db->mutex);
#endif
}

/* ============================================================
 * Tree Name Builders
 * ============================================================ */

char* _mongolite_collection_tree_name(const char *collection_name) {
    if (!collection_name) return NULL;
    size_t prefix_len = strlen(MONGOLITE_COL_PREFIX);
    size_t name_len = strlen(collection_name);
    char *tree_name = malloc(prefix_len + name_len + 1);
    if (!tree_name) return NULL;
    memcpy(tree_name, MONGOLITE_COL_PREFIX, prefix_len);
    memcpy(tree_name + prefix_len, collection_name, name_len + 1);
    return tree_name;
}

char* _mongolite_index_tree_name(const char *collection_name, const char *index_name) {
    if (!collection_name || !index_name) return NULL;
    size_t prefix_len = strlen(MONGOLITE_IDX_PREFIX);
    size_t col_len = strlen(collection_name);
    size_t idx_len = strlen(index_name);
    /* Format: idx:collection:index_name */
    char *tree_name = malloc(prefix_len + col_len + 1 + idx_len + 1);
    if (!tree_name) return NULL;
    sprintf(tree_name, "%s%s:%s", MONGOLITE_IDX_PREFIX, collection_name, index_name);
    return tree_name;
}

/* ============================================================
 * Tree Cache Operations
 * ============================================================ */

wtree_tree_t* _mongolite_tree_cache_get(mongolite_db_t *db, const char *name) {
    if (!db || !name) return NULL;
    mongolite_tree_cache_entry_t *entry = db->tree_cache;
    while (entry) {
        if (strcmp(entry->name, name) == 0) {
            return entry->tree;
        }
        entry = entry->next;
    }
    return NULL;
}

int _mongolite_tree_cache_put(mongolite_db_t *db, const char *name,
                              const char *tree_name, const bson_oid_t *oid,
                              wtree_tree_t *tree) {
    if (!db || !name || !tree_name || !tree) return MONGOLITE_EINVAL;

    /* Check if already exists */
    if (_mongolite_tree_cache_get(db, name)) {
        return MONGOLITE_EEXISTS;
    }

    mongolite_tree_cache_entry_t *entry = calloc(1, sizeof(mongolite_tree_cache_entry_t));
    if (!entry) return MONGOLITE_ENOMEM;

    entry->name = strdup(name);
    entry->tree_name = strdup(tree_name);
    entry->tree = tree;
    if (oid) {
        memcpy(&entry->oid, oid, sizeof(bson_oid_t));
    }

    /* Add to front of list */
    entry->next = db->tree_cache;
    db->tree_cache = entry;
    db->tree_cache_count++;

    return MONGOLITE_OK;
}

void _mongolite_tree_cache_remove(mongolite_db_t *db, const char *name) {
    if (!db || !name) return;

    mongolite_tree_cache_entry_t **pp = &db->tree_cache;
    while (*pp) {
        if (strcmp((*pp)->name, name) == 0) {
            mongolite_tree_cache_entry_t *to_remove = *pp;
            *pp = to_remove->next;
            wtree_tree_close(to_remove->tree);
            free(to_remove->name);
            free(to_remove->tree_name);
            free(to_remove);
            db->tree_cache_count--;
            return;
        }
        pp = &(*pp)->next;
    }
}

void _mongolite_tree_cache_clear(mongolite_db_t *db) {
    if (!db) return;
    while (db->tree_cache) {
        mongolite_tree_cache_entry_t *next = db->tree_cache->next;
        wtree_tree_close(db->tree_cache->tree);
        free(db->tree_cache->name);
        free(db->tree_cache->tree_name);
        free(db->tree_cache);
        db->tree_cache = next;
    }
    db->tree_cache_count = 0;
}

/* ============================================================
 * Transaction Helpers
 * ============================================================ */

wtree_txn_t* _mongolite_get_write_txn(mongolite_db_t *db, gerror_t *error) {
    if (!db) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Database is NULL");
        return NULL;
    }
    if (db->in_transaction && db->current_txn) {
        return db->current_txn;
    }
    return wtree_txn_begin(db->wdb, true, error);
}

wtree_txn_t* _mongolite_get_read_txn(mongolite_db_t *db, gerror_t *error) {
    if (!db) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Database is NULL");
        return NULL;
    }
    if (db->in_transaction && db->current_txn) {
        return db->current_txn;
    }
    return wtree_txn_begin(db->wdb, false, error);
}

int _mongolite_commit_if_auto(mongolite_db_t *db, wtree_txn_t *txn, gerror_t *error) {
    if (!db || !txn) return MONGOLITE_EINVAL;
    /* Only commit if not in explicit transaction */
    if (!db->in_transaction) {
        return wtree_txn_commit(txn, error);
    }
    return MONGOLITE_OK;
}

void _mongolite_abort_if_auto(mongolite_db_t *db, wtree_txn_t *txn) {
    if (!db || !txn) return;
    /* Only abort if not in explicit transaction */
    if (!db->in_transaction) {
        wtree_txn_abort(txn);
    }
}

/* ============================================================
 * Schema Entry Operations
 * ============================================================ */

void _mongolite_schema_entry_free(mongolite_schema_entry_t *entry) {
    if (!entry) return;
    free(entry->name);
    free(entry->tree_name);
    free(entry->type);
    free(entry->collection_name);
    if (entry->indexes) bson_destroy(entry->indexes);
    if (entry->options) bson_destroy(entry->options);
    if (entry->metadata) bson_destroy(entry->metadata);
    if (entry->keys) bson_destroy(entry->keys);
    memset(entry, 0, sizeof(*entry));
}

bson_t* _mongolite_schema_entry_to_bson(const mongolite_schema_entry_t *entry) {
    if (!entry) return NULL;

    bson_t *doc = bson_new();
    if (!doc) return NULL;

    /* Required fields */
    BSON_APPEND_OID(doc, SCHEMA_FIELD_ID, &entry->oid);
    if (entry->name) {
        BSON_APPEND_UTF8(doc, SCHEMA_FIELD_NAME, entry->name);
    }
    if (entry->tree_name) {
        BSON_APPEND_UTF8(doc, SCHEMA_FIELD_TREE_NAME, entry->tree_name);
    }
    if (entry->type) {
        BSON_APPEND_UTF8(doc, SCHEMA_FIELD_TYPE, entry->type);
    }

    /* Timestamps */
    BSON_APPEND_DATE_TIME(doc, SCHEMA_FIELD_CREATED_AT, entry->created_at);
    BSON_APPEND_DATE_TIME(doc, SCHEMA_FIELD_MODIFIED_AT, entry->modified_at);

    /* Collection-specific */
    if (entry->type && strcmp(entry->type, SCHEMA_TYPE_COLLECTION) == 0) {
        BSON_APPEND_INT64(doc, SCHEMA_FIELD_DOC_COUNT, entry->doc_count);
        if (entry->indexes) {
            BSON_APPEND_ARRAY(doc, SCHEMA_FIELD_INDEXES, entry->indexes);
        }
    }

    /* Index-specific */
    if (entry->type && strcmp(entry->type, SCHEMA_TYPE_INDEX) == 0) {
        if (entry->collection_name) {
            BSON_APPEND_UTF8(doc, "collection", entry->collection_name);
        }
        if (entry->keys) {
            BSON_APPEND_DOCUMENT(doc, "keys", entry->keys);
        }
        BSON_APPEND_BOOL(doc, "unique", entry->unique);
        BSON_APPEND_BOOL(doc, "sparse", entry->sparse);
    }

    /* Optional metadata */
    if (entry->options) {
        BSON_APPEND_DOCUMENT(doc, SCHEMA_FIELD_OPTIONS, entry->options);
    }
    if (entry->metadata) {
        BSON_APPEND_DOCUMENT(doc, SCHEMA_FIELD_METADATA, entry->metadata);
    }

    return doc;
}

int _mongolite_schema_entry_from_bson(const bson_t *doc,
                                       mongolite_schema_entry_t *entry,
                                       gerror_t *error) {
    if (!doc || !entry) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return MONGOLITE_EINVAL;
    }

    memset(entry, 0, sizeof(*entry));
    bson_iter_t iter;

    if (bson_iter_init_find(&iter, doc, SCHEMA_FIELD_ID) &&
        BSON_ITER_HOLDS_OID(&iter)) {
        bson_oid_copy(bson_iter_oid(&iter), &entry->oid);
    }

    if (bson_iter_init_find(&iter, doc, SCHEMA_FIELD_NAME) &&
        BSON_ITER_HOLDS_UTF8(&iter)) {
        entry->name = strdup(bson_iter_utf8(&iter, NULL));
    }

    if (bson_iter_init_find(&iter, doc, SCHEMA_FIELD_TREE_NAME) &&
        BSON_ITER_HOLDS_UTF8(&iter)) {
        entry->tree_name = strdup(bson_iter_utf8(&iter, NULL));
    }

    if (bson_iter_init_find(&iter, doc, SCHEMA_FIELD_TYPE) &&
        BSON_ITER_HOLDS_UTF8(&iter)) {
        entry->type = strdup(bson_iter_utf8(&iter, NULL));
    }

    if (bson_iter_init_find(&iter, doc, SCHEMA_FIELD_CREATED_AT) &&
        BSON_ITER_HOLDS_DATE_TIME(&iter)) {
        entry->created_at = bson_iter_date_time(&iter);
    }

    if (bson_iter_init_find(&iter, doc, SCHEMA_FIELD_MODIFIED_AT) &&
        BSON_ITER_HOLDS_DATE_TIME(&iter)) {
        entry->modified_at = bson_iter_date_time(&iter);
    }

    if (bson_iter_init_find(&iter, doc, SCHEMA_FIELD_DOC_COUNT) &&
        BSON_ITER_HOLDS_INT64(&iter)) {
        entry->doc_count = bson_iter_int64(&iter);
    }

    /* Index array for collections */
    if (bson_iter_init_find(&iter, doc, SCHEMA_FIELD_INDEXES) &&
        BSON_ITER_HOLDS_ARRAY(&iter)) {
        uint32_t len;
        const uint8_t *data;
        bson_iter_array(&iter, &len, &data);
        entry->indexes = bson_new_from_data(data, len);
    }

    /* Options document */
    if (bson_iter_init_find(&iter, doc, SCHEMA_FIELD_OPTIONS) &&
        BSON_ITER_HOLDS_DOCUMENT(&iter)) {
        uint32_t len;
        const uint8_t *data;
        bson_iter_document(&iter, &len, &data);
        entry->options = bson_new_from_data(data, len);
    }

    /* Metadata document */
    if (bson_iter_init_find(&iter, doc, SCHEMA_FIELD_METADATA) &&
        BSON_ITER_HOLDS_DOCUMENT(&iter)) {
        uint32_t len;
        const uint8_t *data;
        bson_iter_document(&iter, &len, &data);
        entry->metadata = bson_new_from_data(data, len);
    }

    /* Index-specific fields */
    if (bson_iter_init_find(&iter, doc, "collection") &&
        BSON_ITER_HOLDS_UTF8(&iter)) {
        entry->collection_name = strdup(bson_iter_utf8(&iter, NULL));
    }

    if (bson_iter_init_find(&iter, doc, "keys") &&
        BSON_ITER_HOLDS_DOCUMENT(&iter)) {
        uint32_t len;
        const uint8_t *data;
        bson_iter_document(&iter, &len, &data);
        entry->keys = bson_new_from_data(data, len);
    }

    if (bson_iter_init_find(&iter, doc, "unique") &&
        BSON_ITER_HOLDS_BOOL(&iter)) {
        entry->unique = bson_iter_bool(&iter);
    }

    if (bson_iter_init_find(&iter, doc, "sparse") &&
        BSON_ITER_HOLDS_BOOL(&iter)) {
        entry->sparse = bson_iter_bool(&iter);
    }

    return MONGOLITE_OK;
}

/* ============================================================
 * Schema CRUD Operations
 * ============================================================ */

int _mongolite_schema_init(mongolite_db_t *db, gerror_t *error) {
    if (!db || !db->wdb) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Database not initialized");
        return MONGOLITE_EINVAL;
    }

    /* Create or open the schema tree */
    db->schema_tree = wtree_tree_create(db->wdb, MONGOLITE_SCHEMA_TREE, 0, error);
    if (!db->schema_tree) {
        return MONGOLITE_ERROR;
    }

    return MONGOLITE_OK;
}

int _mongolite_schema_get(mongolite_db_t *db, const char *name,
                          mongolite_schema_entry_t *entry, gerror_t *error) {
    if (!db || !name || !entry) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return MONGOLITE_EINVAL;
    }

    wtree_txn_t *txn = _mongolite_get_read_txn(db, error);
    if (!txn) return MONGOLITE_ERROR;

    const void *value;
    size_t value_size;
    int rc = wtree_get_txn(txn, db->schema_tree, name, strlen(name), &value, &value_size, error);

    if (rc != 0) {
        _mongolite_abort_if_auto(db, txn);
        if (rc == WTREE_KEY_NOT_FOUND) {
            set_error(error, MONGOLITE_LIB, MONGOLITE_ENOTFOUND,
                     "Collection or index not found: %s", name);
            return MONGOLITE_ENOTFOUND;
        }
        return rc;
    }

    /* Parse BSON document */
    bson_t doc;
    if (!bson_init_static(&doc, value, value_size)) {
        _mongolite_abort_if_auto(db, txn);
        set_error(error, MONGOLITE_LIB, MONGOLITE_ERROR, "Invalid BSON in schema");
        return MONGOLITE_ERROR;
    }

    rc = _mongolite_schema_entry_from_bson(&doc, entry, error);
    _mongolite_abort_if_auto(db, txn);

    return rc;
}

int _mongolite_schema_put(mongolite_db_t *db, const mongolite_schema_entry_t *entry,
                          gerror_t *error) {
    if (!db || !entry || !entry->name) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return MONGOLITE_EINVAL;
    }

    bson_t *doc = _mongolite_schema_entry_to_bson(entry);
    if (!doc) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM, "Failed to create BSON document");
        return MONGOLITE_ENOMEM;
    }

    wtree_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (!txn) {
        bson_destroy(doc);
        return MONGOLITE_ERROR;
    }

    /* Use update (overwrite) instead of insert to allow updates */
    int rc = wtree_update_txn(txn, db->schema_tree,
                              entry->name, strlen(entry->name),
                              bson_get_data(doc), doc->len, error);

    bson_destroy(doc);

    if (rc != 0) {
        _mongolite_abort_if_auto(db, txn);
        return rc;
    }

    return _mongolite_commit_if_auto(db, txn, error);
}

int _mongolite_schema_delete(mongolite_db_t *db, const char *name, gerror_t *error) {
    if (!db || !name) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return MONGOLITE_EINVAL;
    }

    wtree_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (!txn) return MONGOLITE_ERROR;

    bool deleted = false;
    int rc = wtree_delete_one_txn(txn, db->schema_tree,
                                   name, strlen(name), &deleted, error);

    if (rc != 0) {
        _mongolite_abort_if_auto(db, txn);
        return rc;
    }

    if (!deleted) {
        _mongolite_abort_if_auto(db, txn);
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOTFOUND,
                 "Schema entry not found: %s", name);
        return MONGOLITE_ENOTFOUND;
    }

    return _mongolite_commit_if_auto(db, txn, error);
}

int _mongolite_schema_list(mongolite_db_t *db, char ***names, size_t *count,
                           const char *type_filter, gerror_t *error) {
    if (!db || !names || !count) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return MONGOLITE_EINVAL;
    }

    *names = NULL;
    *count = 0;

    wtree_txn_t *txn = _mongolite_get_read_txn(db, error);
    if (!txn) return MONGOLITE_ERROR;

    wtree_iterator_t *iter = wtree_iterator_create_with_txn(db->schema_tree, txn, error);
    if (!iter) {
        _mongolite_abort_if_auto(db, txn);
        return MONGOLITE_ERROR;
    }

    /* First pass: count entries */
    size_t total = 0;
    if (wtree_iterator_first(iter)) {
        do {
            if (type_filter) {
                const void *value;
                size_t value_size;
                if (wtree_iterator_value(iter, &value, &value_size)) {
                    bson_t doc;
                    if (bson_init_static(&doc, value, value_size)) {
                        bson_iter_t it;
                        if (bson_iter_init_find(&it, &doc, SCHEMA_FIELD_TYPE) &&
                            BSON_ITER_HOLDS_UTF8(&it)) {
                            if (strcmp(bson_iter_utf8(&it, NULL), type_filter) == 0) {
                                total++;
                            }
                        }
                    }
                }
            } else {
                total++;
            }
        } while (wtree_iterator_next(iter));
    }

    if (total == 0) {
        wtree_iterator_close(iter);
        _mongolite_abort_if_auto(db, txn);
        return MONGOLITE_OK;
    }

    /* Allocate array */
    *names = calloc(total, sizeof(char*));
    if (!*names) {
        wtree_iterator_close(iter);
        _mongolite_abort_if_auto(db, txn);
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM, "Failed to allocate names array");
        return MONGOLITE_ENOMEM;
    }

    /* Second pass: collect names */
    size_t idx = 0;
    if (wtree_iterator_first(iter)) {
        do {
            const void *key;
            size_t key_size;
            const void *value;
            size_t value_size;

            if (!wtree_iterator_key(iter, &key, &key_size)) continue;
            if (!wtree_iterator_value(iter, &value, &value_size)) continue;

            bool include = true;
            if (type_filter) {
                include = false;
                bson_t doc;
                if (bson_init_static(&doc, value, value_size)) {
                    bson_iter_t it;
                    if (bson_iter_init_find(&it, &doc, SCHEMA_FIELD_TYPE) &&
                        BSON_ITER_HOLDS_UTF8(&it)) {
                        if (strcmp(bson_iter_utf8(&it, NULL), type_filter) == 0) {
                            include = true;
                        }
                    }
                }
            }

            if (include && idx < total) {
                (*names)[idx] = strndup((const char*)key, key_size);
                idx++;
            }
        } while (wtree_iterator_next(iter));
    }

    *count = idx;
    wtree_iterator_close(iter);
    _mongolite_abort_if_auto(db, txn);

    return MONGOLITE_OK;
}

/* ============================================================
 * Database Open / Close
 * ============================================================ */

int mongolite_open(const char *filename, mongolite_db_t **db,
                   db_config_t *config, gerror_t *error) {
    if (!filename || !db) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "filename and db pointer are required");
        return MONGOLITE_EINVAL;
    }

    *db = NULL;

    /* Check/create directory */
    struct stat st = {0};
    if (stat(filename, &st) == -1) {
        /* Directory doesn't exist, try to create */
        if (mkdir(filename, 0755) != 0) {
            set_error(error, MONGOLITE_LIB, errno,
                     "Failed to create database directory: %s", filename);
            return MONGOLITE_EIO;
        }
    } else if (!S_ISDIR(st.st_mode)) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "Path exists but is not a directory: %s", filename);
        return MONGOLITE_EINVAL;
    }

    /* Allocate database structure */
    mongolite_db_t *new_db = calloc(1, sizeof(mongolite_db_t));
    if (!new_db) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM,
                 "Failed to allocate database structure");
        return MONGOLITE_ENOMEM;
    }

    /* Apply configuration */
    size_t max_bytes = config ? config->max_bytes : 0;
    if (max_bytes == 0) max_bytes = MONGOLITE_DEFAULT_MAPSIZE;

    unsigned int max_dbs = config ? config->max_dbs : 0;
    if (max_dbs == 0) max_dbs = MONGOLITE_DEFAULT_MAX_DBS;

    unsigned int lmdb_flags = config ? config->lmdb_flags : 0;

    /* Open LMDB environment */
    new_db->wdb = wtree_db_create(filename, max_bytes, max_dbs, lmdb_flags, error);
    if (!new_db->wdb) {
        free(new_db);
        return MONGOLITE_ERROR;
    }

    new_db->path = strdup(filename);
    new_db->max_bytes = max_bytes;
    new_db->max_dbs = max_dbs;

    /* Initialize schema tree */
    int rc = _mongolite_schema_init(new_db, error);
    if (rc != 0) {
        wtree_db_close(new_db->wdb);
        free(new_db->path);
        free(new_db);
        return rc;
    }

    /* Copy user metadata if provided */
    if (config && config->metadata) {
        new_db->db_metadata = bson_copy(config->metadata);
    }

    *db = new_db;
    return MONGOLITE_OK;
}

int mongolite_close(mongolite_db_t *db) {
    if (!db) return MONGOLITE_OK;

    /* Abort any pending transaction */
    if (db->in_transaction && db->current_txn) {
        wtree_txn_abort(db->current_txn);
        db->current_txn = NULL;
        db->in_transaction = false;
    }

    /* Clear tree cache */
    _mongolite_tree_cache_clear(db);

    /* Close schema tree */
    if (db->schema_tree) {
        wtree_tree_close(db->schema_tree);
    }

    /* Close LMDB environment */
    if (db->wdb) {
        wtree_db_close(db->wdb);
    }

    /* Free metadata */
    if (db->db_metadata) {
        bson_destroy(db->db_metadata);
    }

    /* Free mutex */
    if (db->mutex) {
#ifdef _WIN32
        DeleteCriticalSection((CRITICAL_SECTION*)db->mutex);
        free(db->mutex);
#else
        pthread_mutex_destroy(db->mutex);
        free(db->mutex);
#endif
    }

    free(db->path);
    free(db);

    return MONGOLITE_OK;
}

/* ============================================================
 * Database Info
 * ============================================================ */

const char* mongolite_db_filename(mongolite_db_t *db) {
    return db ? db->path : NULL;
}

int64_t mongolite_last_insert_rowid(mongolite_db_t *db) {
    return db ? db->last_insert_rowid : 0;
}

int mongolite_changes(mongolite_db_t *db) {
    return db ? db->changes : 0;
}

/* ============================================================
 * Database Metadata
 * ============================================================ */

const bson_t* mongolite_db_metadata(mongolite_db_t *db) {
    return db ? db->db_metadata : NULL;
}

int mongolite_db_set_metadata(mongolite_db_t *db, const bson_t *metadata, gerror_t *error) {
    if (!db) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Database is NULL");
        return MONGOLITE_EINVAL;
    }

    _mongolite_lock(db);

    if (db->db_metadata) {
        bson_destroy(db->db_metadata);
        db->db_metadata = NULL;
    }

    if (metadata) {
        db->db_metadata = bson_copy(metadata);
        if (!db->db_metadata) {
            _mongolite_unlock(db);
            set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM, "Failed to copy metadata");
            return MONGOLITE_ENOMEM;
        }
    }

    _mongolite_unlock(db);
    return MONGOLITE_OK;
}

/* ============================================================
 * Transaction Support
 * ============================================================ */

int mongolite_begin_transaction(mongolite_db_t *db) {
    if (!db) return MONGOLITE_EINVAL;

    _mongolite_lock(db);

    if (db->in_transaction) {
        _mongolite_unlock(db);
        return MONGOLITE_ERROR;  /* Already in transaction */
    }

    gerror_t error = {0};
    db->current_txn = wtree_txn_begin(db->wdb, true, &error);
    if (!db->current_txn) {
        _mongolite_unlock(db);
        return MONGOLITE_ERROR;
    }

    db->in_transaction = true;
    _mongolite_unlock(db);
    return MONGOLITE_OK;
}

int mongolite_commit(mongolite_db_t *db) {
    if (!db) return MONGOLITE_EINVAL;

    _mongolite_lock(db);

    if (!db->in_transaction || !db->current_txn) {
        _mongolite_unlock(db);
        return MONGOLITE_ERROR;  /* Not in transaction */
    }

    gerror_t error = {0};
    int rc = wtree_txn_commit(db->current_txn, &error);

    db->current_txn = NULL;
    db->in_transaction = false;

    _mongolite_unlock(db);
    return rc;
}

int mongolite_rollback(mongolite_db_t *db) {
    if (!db) return MONGOLITE_EINVAL;

    _mongolite_lock(db);

    if (!db->in_transaction || !db->current_txn) {
        _mongolite_unlock(db);
        return MONGOLITE_ERROR;  /* Not in transaction */
    }

    wtree_txn_abort(db->current_txn);

    db->current_txn = NULL;
    db->in_transaction = false;

    _mongolite_unlock(db);
    return MONGOLITE_OK;
}

/* ============================================================
 * Sync
 * ============================================================ */

int mongolite_sync(mongolite_db_t *db, bool force, gerror_t *error) {
    if (!db || !db->wdb) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Database is NULL");
        return MONGOLITE_EINVAL;
    }
    return wtree_db_sync(db->wdb, force, error);
}

/* ============================================================
 * Utility
 * ============================================================ */

const char* mongolite_version(void) {
    return MONGOLITE_VERSION;
}

const char* mongolite_errstr(int error_code) {
    switch (error_code) {
        case MONGOLITE_OK:        return "Success";
        case MONGOLITE_ERROR:     return "Generic error";
        case MONGOLITE_ENOTFOUND: return "Not found";
        case MONGOLITE_EEXISTS:   return "Already exists";
        case MONGOLITE_EINVAL:    return "Invalid argument";
        case MONGOLITE_ENOMEM:    return "Out of memory";
        case MONGOLITE_EIO:       return "I/O error";
        default:                  return wtree_strerror(error_code);
    }
}
