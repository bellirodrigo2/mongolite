/*
 * wregex.c - PCRE2 wrapper with global cache
 *
 * Thread-safe regex compilation cache with on-demand match_data allocation.
 * Designed for use with bsonmatch (MongoDB-like query matcher).
 */

/* MUST be defined BEFORE including pcre2.h */
#define PCRE2_CODE_UNIT_WIDTH 8

#include "wregex.h"
#include <pcre2.h>
#include <uthash.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ------------------------------------------------------------------
 * Thread-safety configuration
 * ------------------------------------------------------------------ */

#ifdef _WIN32
    /* Windows: use Critical Section */
    #include <windows.h>
    static CRITICAL_SECTION g_lock;
    static volatile LONG g_lock_initialized = 0;

    static void ensure_lock_init(void) {
        if (InterlockedCompareExchange(&g_lock_initialized, 1, 0) == 0) {
            InitializeCriticalSection(&g_lock);
        }
    }
    #define LOCK()   do { ensure_lock_init(); EnterCriticalSection(&g_lock); } while(0)
    #define UNLOCK() LeaveCriticalSection(&g_lock)

#elif defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L && !defined(__STDC_NO_THREADS__)
    /* C11 threads */
    #include <threads.h>
    static mtx_t g_lock;
    static once_flag g_lock_flag = ONCE_FLAG_INIT;

    static void init_lock(void) {
        mtx_init(&g_lock, mtx_plain);
    }
    #define LOCK()   do { call_once(&g_lock_flag, init_lock); mtx_lock(&g_lock); } while(0)
    #define UNLOCK() mtx_unlock(&g_lock)

#elif defined(_POSIX_VERSION)
    /* POSIX threads */
    #include <pthread.h>
    static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;
    #define LOCK()   pthread_mutex_lock(&g_lock)
    #define UNLOCK() pthread_mutex_unlock(&g_lock)

#else
    /* No threading support - no locking */
    #define LOCK()
    #define UNLOCK()
#endif

/* ------------------------------------------------------------------
 * Internal structures
 * ------------------------------------------------------------------ */

typedef struct cache_entry {
    char *pattern;              /* hash key */
    unsigned int options;       /* compilation options (part of key) */
    pcre2_code *code;          /* compiled regex */
    UT_hash_handle hh;
} cache_entry_t;

/* The wregex_t is just a pointer to the cached pcre2_code */
struct wregex {
    pcre2_code *code;
};

/* Global cache */
static cache_entry_t *g_cache = NULL;

/* ------------------------------------------------------------------
 * Helper: create composite key from pattern + options
 * ------------------------------------------------------------------ */

static char *make_cache_key(const char *pattern, unsigned int options, size_t *key_len)
{
    size_t plen = strlen(pattern);
    /* key = pattern + '\0' + options_as_hex */
    size_t total = plen + 1 + 8 + 1;  /* pattern + null + 8 hex digits + null */
    char *key = malloc(total);
    if (!key) return NULL;
    
    memcpy(key, pattern, plen + 1);
    snprintf(key + plen + 1, 9, "%08x", options);
    
    *key_len = plen + 1 + 8;  /* exclude final null for hash key length */
    return key;
}

/* ------------------------------------------------------------------
 * wregex_compile - Compile regex with caching
 * ------------------------------------------------------------------ */

wregex_t *wregex_compile(const char *pattern, unsigned int options)
{
    if (!pattern) return NULL;

    cache_entry_t *entry = NULL;
    size_t key_len;
    char *key = make_cache_key(pattern, options, &key_len);
    if (!key) return NULL;

    LOCK();

    /* Check cache */
    HASH_FIND(hh, g_cache, key, key_len, entry);
    if (entry) {
        free(key);
        UNLOCK();
        
        /* Return wrapper pointing to cached code */
        wregex_t *re = malloc(sizeof(*re));
        if (re) re->code = entry->code;
        return re;
    }

    /* Not in cache - compile */
    int errorcode;
    PCRE2_SIZE erroroffset;
    
    pcre2_code *code = pcre2_compile(
        (PCRE2_SPTR)pattern,
        PCRE2_ZERO_TERMINATED,
        options,
        &errorcode,
        &erroroffset,
        NULL
    );

    if (!code) {
        free(key);
        UNLOCK();
        return NULL;
    }

    /* Try JIT compilation (ignore errors - falls back to interpreter) */
    pcre2_jit_compile(code, PCRE2_JIT_COMPLETE);

    /* Add to cache */
    entry = malloc(sizeof(*entry));
    if (!entry) {
        pcre2_code_free(code);
        free(key);
        UNLOCK();
        return NULL;
    }

    entry->pattern = key;  /* transfer ownership */
    entry->options = options;
    entry->code = code;

    HASH_ADD_KEYPTR(hh, g_cache, entry->pattern, key_len, entry);

    UNLOCK();

    /* Return wrapper */
    wregex_t *re = malloc(sizeof(*re));
    if (re) re->code = code;
    return re;
}

/* ------------------------------------------------------------------
 * wregex_match - Execute match (thread-safe)
 * ------------------------------------------------------------------ */

bool wregex_match(wregex_t *re, const char *subject, size_t len)
{
    if (!re || !re->code || !subject) return false;

    /* Handle zero-terminated string */
    if (len == WREGEX_ZERO_TERMINATED) {
        len = strlen(subject);
    }

    /* Allocate match_data per call (thread-safe) */
    pcre2_match_data *match_data = pcre2_match_data_create_from_pattern(re->code, NULL);
    if (!match_data) return false;

    int rc = pcre2_match(
        re->code,
        (PCRE2_SPTR)subject,
        len,
        0,              /* start offset */
        0,              /* options */
        match_data,
        NULL            /* match context */
    );

    pcre2_match_data_free(match_data);

    return rc >= 0;
}

/* ------------------------------------------------------------------
 * wregex_free - Free wrapper (does NOT free cached code)
 * ------------------------------------------------------------------ */

void wregex_free(wregex_t *re)
{
    free(re);
}

/* ------------------------------------------------------------------
 * wregex_cache_destroy - Free all cached regexes
 * ------------------------------------------------------------------ */

void wregex_cache_destroy(void)
{
    cache_entry_t *entry, *tmp;

    LOCK();

    HASH_ITER(hh, g_cache, entry, tmp) {
        HASH_DEL(g_cache, entry);
        pcre2_code_free(entry->code);
        free(entry->pattern);
        free(entry);
    }
    g_cache = NULL;

    UNLOCK();
}

/* ------------------------------------------------------------------
 * wregex_cache_stats - Print cache statistics
 * ------------------------------------------------------------------ */

void wregex_cache_stats(void)
{
    cache_entry_t *entry, *tmp;
    unsigned int count = 0;
    size_t total_pattern_len = 0;

    LOCK();

    HASH_ITER(hh, g_cache, entry, tmp) {
        count++;
        total_pattern_len += strlen(entry->pattern);
    }

    UNLOCK();

    printf("wregex cache stats:\n");
    printf("  Entries: %u\n", count);
    if (count > 0) {
        printf("  Avg pattern length: %.1f\n", (double)total_pattern_len / count);
    }
}
