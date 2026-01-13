/*
 * mongolite_util.c - Utility functions
 *
 * Handles:
 * - Timestamp helpers
 * - OID helpers
 * - Lock helpers
 * - Tree name builders
 * - Tree cache operations
 * - Version and error strings
 */

#include "mongolite_internal.h"
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>
#include <sys/time.h>
#endif

#define MONGOLITE_LIB "mongolite"

/* ============================================================
 * Platform-specific helpers
 * ============================================================ */

char* _mongolite_strndup(const char *s, size_t n) {
#ifdef _WIN32
    size_t len = strlen(s);
    if (len > n) len = n;
    char *result = malloc(len + 1);
    if (result) {
        memcpy(result, s, len);
        result[len] = '\0';
    }
    return result;
#else
    return strndup(s, n);
#endif
}

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
 * Document _id Helpers
 * ============================================================ */

/**
 * Ensure a BSON document has an _id field.
 * If missing, generates a new OID and prepends it.
 *
 * @param doc           The document to check/modify
 * @param out_oid       Output: the _id OID (existing or generated)
 * @param was_generated Output: true if _id was generated (optional, may be NULL)
 * @param error         Error output (optional, may be NULL)
 * @return              Document with _id (may be original or new copy).
 *                      Caller must check if result != doc and destroy if so.
 *                      Returns NULL on allocation error.
 */
bson_t* _mongolite_ensure_doc_id(const bson_t *doc, bson_oid_t *out_oid,
                                  bool *was_generated, gerror_t *error) {
    bson_iter_t iter;

    if (was_generated) *was_generated = false;

    /* Check if _id exists */
    if (bson_iter_init_find(&iter, doc, "_id")) {
        /* Extract existing _id */
        if (BSON_ITER_HOLDS_OID(&iter)) {
            bson_oid_copy(bson_iter_oid(&iter), out_oid);
        } else {
            /* Non-OID _id - generate OID for internal key but keep original _id */
            bson_oid_init(out_oid, NULL);
        }
        return (bson_t*)doc;  /* Use original */
    }

    /* No _id - generate one and prepend */
    if (was_generated) *was_generated = true;
    bson_oid_init(out_oid, NULL);

    bson_t *new_doc = bson_new();
    if (!new_doc) {
        if (error) {
            set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM, "Failed to allocate document");
        }
        return NULL;
    }

    /* Prepend _id */
    BSON_APPEND_OID(new_doc, "_id", out_oid);

    /* Copy all fields from original */
    if (bson_iter_init(&iter, doc)) {
        while (bson_iter_next(&iter)) {
            const bson_value_t *value = bson_iter_value(&iter);
            bson_append_value(new_doc, bson_iter_key(&iter), -1, value);
        }
    }

    return new_doc;
}

/* ============================================================
 * Lock Helpers
 * ============================================================ */

int _mongolite_lock_init(mongolite_db_t *db) {
    if (!db) return MONGOLITE_EINVAL;

#ifdef _WIN32
    CRITICAL_SECTION *cs = malloc(sizeof(CRITICAL_SECTION));
    if (!cs) return MONGOLITE_ENOMEM;
    InitializeCriticalSection(cs);
    db->mutex = cs;
#else
    pthread_mutex_t *mtx = malloc(sizeof(pthread_mutex_t));
    if (!mtx) return MONGOLITE_ENOMEM;
    if (pthread_mutex_init(mtx, NULL) != 0) {
        free(mtx);
        return MONGOLITE_ERROR;
    }
    db->mutex = mtx;
#endif

    return MONGOLITE_OK;
}

void _mongolite_lock_free(mongolite_db_t *db) {
    if (!db || !db->mutex) return;

#ifdef _WIN32
    DeleteCriticalSection((CRITICAL_SECTION*)db->mutex);
    free(db->mutex);
#else
    pthread_mutex_destroy(db->mutex);
    free(db->mutex);
#endif
    db->mutex = NULL;
}

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
    /* Format: idx:<collection>:<index> */
    char *tree_name = malloc(prefix_len + col_len + 1 + idx_len + 1);
    if (!tree_name) return NULL;
    memcpy(tree_name, MONGOLITE_IDX_PREFIX, prefix_len);
    memcpy(tree_name + prefix_len, collection_name, col_len);
    tree_name[prefix_len + col_len] = ':';
    memcpy(tree_name + prefix_len + col_len + 1, index_name, idx_len + 1);
    return tree_name;
}

/* ============================================================
 * Tree Cache Operations
 * ============================================================ */

wtree3_tree_t* _mongolite_tree_cache_get(mongolite_db_t *db, const char *name) {
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
                              wtree3_tree_t *tree) {
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
    /* Note: doc_count is now managed by wtree3 internally via wtree3_tree_count() */
    if (oid) {
        memcpy(&entry->oid, oid, sizeof(bson_oid_t));
    }

    /* Add to front of list */
    entry->next = db->tree_cache;
    db->tree_cache = entry;
    db->tree_cache_count++;

    return MONGOLITE_OK;
}

/* Helper to free cached index specs array */
static void _free_cached_indexes(mongolite_cached_index_t *indexes, size_t count) {
    if (!indexes) return;
    for (size_t i = 0; i < count; i++) {
        free(indexes[i].name);
        if (indexes[i].keys) bson_destroy(indexes[i].keys);
        /* Note: Index trees are now managed by wtree3 internally */
    }
    free(indexes);
}

void _mongolite_tree_cache_remove(mongolite_db_t *db, const char *name) {
    if (!db || !name) return;

    mongolite_tree_cache_entry_t **pp = &db->tree_cache;
    while (*pp) {
        if (strcmp((*pp)->name, name) == 0) {
            mongolite_tree_cache_entry_t *to_remove = *pp;
            *pp = to_remove->next;
            wtree3_tree_close(to_remove->tree);
            free(to_remove->name);
            free(to_remove->tree_name);
            _free_cached_indexes(to_remove->indexes, to_remove->index_count);
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
        wtree3_tree_close(db->tree_cache->tree);
        free(db->tree_cache->name);
        free(db->tree_cache->tree_name);
        _free_cached_indexes(db->tree_cache->indexes, db->tree_cache->index_count);
        free(db->tree_cache);
        db->tree_cache = next;
    }
    db->tree_cache_count = 0;
}

/*
 * Get doc count for a collection from wtree3.
 * Note: wtree3 now manages the count internally.
 */
int64_t mongolite_get_collection_doc_count(mongolite_db_t *db, const char *name) {
    if (!db || !name) return -1;
    wtree3_tree_t *tree = _mongolite_tree_cache_get(db, name);
    if (!tree) {
        /* Try to open the collection tree */
        tree = _mongolite_get_collection_tree(db, name, NULL);
        if (!tree) return -1;
    }
    return wtree3_tree_count(tree);
}

/* ============================================================
 * Index Specs Cache Operations (for query optimization)
 *
 * Note: Index trees are now managed by wtree3 internally.
 * This cache only stores index specs for query optimization.
 * ============================================================ */

/* Helper to find cache entry by name */
static mongolite_tree_cache_entry_t* _find_cache_entry(mongolite_db_t *db, const char *name) {
    mongolite_tree_cache_entry_t *entry = db->tree_cache;
    while (entry) {
        if (strcmp(entry->name, name) == 0) {
            return entry;
        }
        entry = entry->next;
    }
    return NULL;
}

/*
 * Get cached index specs for a collection.
 * Loads from schema on first access, returns cached array on subsequent calls.
 * Returns pointer to internal array (do not free).
 *
 * Note: Index trees are managed by wtree3 - this just caches specs for query optimization.
 */
mongolite_cached_index_t* _mongolite_get_cached_indexes(mongolite_db_t *db,
                                                         const char *collection,
                                                         size_t *out_count,
                                                         gerror_t *error) {
    if (!db || !collection || !out_count) {
        if (out_count) *out_count = 0;
        return NULL;
    }

    mongolite_tree_cache_entry_t *entry = _find_cache_entry(db, collection);
    if (!entry) {
        /* Collection not in cache - need to open it first */
        wtree3_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
        if (!tree) {
            *out_count = 0;
            return NULL;
        }
        entry = _find_cache_entry(db, collection);
        if (!entry) {
            *out_count = 0;
            return NULL;
        }
    }

    /* If indexes already loaded, return them */
    if (entry->indexes_loaded) {
        *out_count = entry->index_count;
        return entry->indexes;
    }

    /* Load indexes from wtree3 */
    wtree3_index_info_t *wtree_indexes = NULL;
    size_t wtree_count = 0;
    
    if (wtree3_tree_list_indexes(entry->tree, &wtree_indexes, &wtree_count, error) != 0) {
        *out_count = 0;
        return NULL;
    }

    if (wtree_count == 0) {
        entry->indexes = NULL;
        entry->index_count = 0;
        entry->indexes_loaded = true;
        *out_count = 0;
        return NULL;
    }

    /* Allocate cache array */
    mongolite_cached_index_t *cached = calloc(wtree_count, sizeof(mongolite_cached_index_t));
    if (!cached) {
        /* Free wtree indexes */
        for (size_t i = 0; i < wtree_count; i++) {
            free(wtree_indexes[i].name);
            free(wtree_indexes[i].user_data);
        }
        free(wtree_indexes);
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM, "Failed to allocate index cache");
        *out_count = 0;
        return NULL;
    }

    /* Convert wtree3_index_info_t to mongolite_cached_index_t */
    for (size_t i = 0; i < wtree_count; i++) {
        cached[i].name = wtree_indexes[i].name;  /* Transfer ownership */
        cached[i].unique = wtree_indexes[i].unique;
        cached[i].sparse = wtree_indexes[i].sparse;
        cached[i].dbi = wtree_indexes[i].dbi;

        /* Parse BSON keys from user_data */
        if (wtree_indexes[i].user_data && wtree_indexes[i].user_data_len > 0) {
            bson_t bson_keys;
            if (bson_init_static(&bson_keys, wtree_indexes[i].user_data, wtree_indexes[i].user_data_len)) {
                cached[i].keys = bson_copy(&bson_keys);
            } else {
                cached[i].keys = NULL;
            }
        } else {
            cached[i].keys = NULL;
        }

        free(wtree_indexes[i].user_data);  /* Free user_data, we copied it */
    }
    free(wtree_indexes);

    entry->indexes = cached;
    entry->index_count = wtree_count;
    entry->indexes_loaded = true;

    *out_count = wtree_count;
    return cached;
}

/*
 * Invalidate the index cache for a collection.
 * Called when indexes are created or dropped.
 */
void _mongolite_invalidate_index_cache(mongolite_db_t *db, const char *collection) {
    if (!db || !collection) return;

    mongolite_tree_cache_entry_t *entry = _find_cache_entry(db, collection);
    if (!entry) return;

    _free_cached_indexes(entry->indexes, entry->index_count);
    entry->indexes = NULL;
    entry->index_count = 0;
    entry->indexes_loaded = false;
}

/* ============================================================
 * Utility Functions
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
        default:                  return wtree3_strerror(error_code);
    }
}
