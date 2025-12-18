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
    /* Format: idx:collection:index_name */
    size_t total_len = prefix_len + col_len + 1 + idx_len + 1;
    char *tree_name = malloc(total_len);
    if (!tree_name) return NULL;
    snprintf(tree_name, total_len, "%s%s:%s", MONGOLITE_IDX_PREFIX, collection_name, index_name);
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
        default:                  return wtree_strerror(error_code);
    }
}
