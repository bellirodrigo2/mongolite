#ifndef FXCACHE_H
#define FXCACHE_H

#include <stddef.h>
#include <stdint.h>

#include <bson/bson.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 *  Opaque types
 * ============================================================ */
typedef struct fxcache fxcache;
typedef struct fxcache_random_policy fxcache_random_policy;

/* ============================================================
 *  Key mode (runtime)
 * ============================================================ */
typedef enum {
    /* Key is treated as a BSON OID (12 bytes). Cache stores the 12 bytes inline. */
    FLEXCACHE_KEY_OID = 1,

    /* Key is arbitrary bytes (e.g., string/blob). Cache can optionally copy/free them. */
    FLEXCACHE_KEY_BYTES = 2,

    /* Key is a 64-bit integer. Cache stores 8 bytes inline (no allocations). */
    FLEXCACHE_KEY_INT64 = 3
} fxcache_key_mode;

/* ============================================================
 *  Callbacks
 * ============================================================ */
typedef uint64_t (*fxcache_now_fn)(void *user_ctx);

typedef void *(*fxcache_copy_fn)(const void *ptr, size_t len, void *user_ctx);
typedef void (*fxcache_free_fn)(void *ptr, void *user_ctx);

typedef void (*fxcache_ondelete_fn)(
    const void   *key,
    size_t        key_len,
    void         *value,
    int64_t       byte_size,
    void         *user_ctx
);

/* RNG used by random policy */
typedef uint32_t (*fxcache_rng_fn)(void *rng_ctx);

/* ============================================================
 *  Init / create / destroy
 * ============================================================ */
int fxcache_init(
    fxcache *fc,
    fxcache_key_mode key_mode,
    fxcache_now_fn now_fn,
    size_t item_max,
    int64_t byte_max,
    uint64_t scan_interval_ms,
    fxcache_copy_fn key_copy,
    fxcache_free_fn key_free,
    fxcache_copy_fn value_copy,
    fxcache_free_fn value_free,
    fxcache_ondelete_fn ondelete,
    void *user_ctx
);

fxcache *fxcache_create(
    fxcache_key_mode key_mode,
    fxcache_now_fn now_fn,
    size_t item_max,
    int64_t byte_max,
    uint64_t scan_interval_ms,
    fxcache_copy_fn key_copy,
    fxcache_free_fn key_free,
    fxcache_copy_fn value_copy,
    fxcache_free_fn value_free,
    fxcache_ondelete_fn ondelete,
    void *user_ctx
);

void fxcache_destroy(fxcache *fc);

/* ============================================================
 *  Core API
 * ============================================================ */
int fxcache_insert(
    fxcache *fc,
    const void *key,
    size_t key_len,
    const void *value,
    size_t value_len,
    int64_t byte_size,
    uint64_t ttl_ms,
    uint64_t expires_at_ms
);

void *fxcache_get(fxcache *fc, const void *key, size_t key_len);

int fxcache_delete(fxcache *fc, const void *key, size_t key_len);

/* ============================================================
 *  Convenience API for OID mode
 * ============================================================ */
int   fxcache_insert_oid(
    fxcache *fc,
    const bson_oid_t *oid,
    const void *value,
    size_t value_len,
    int64_t byte_size,
    uint64_t ttl_ms,
    uint64_t expires_at_ms
);

void *fxcache_get_oid(fxcache *fc, const bson_oid_t *oid);
int   fxcache_delete_oid(fxcache *fc, const bson_oid_t *oid);


/* ============================================================
 *  Convenience API for INT64 mode
 * ============================================================ */
int   fxcache_insert_int64(
    fxcache *fc,
    int64_t key,
    const void *value,
    size_t value_len,
    int64_t byte_size,
    uint64_t ttl_ms,
    uint64_t expires_at_ms
);

void *fxcache_get_int64(fxcache *fc, int64_t key);
int   fxcache_delete_int64(fxcache *fc, int64_t key);

/* ============================================================
 *  Maintenance
 * ============================================================ */
void fxcache_scan_and_clean(fxcache *fc);

size_t  fxcache_item_count(const fxcache *fc);

/* ============================================================
 *  Replacement policy selection
 * ============================================================ */
void fxcache_policy_fifo_init(fxcache *fc);

/* Random eviction policy */
fxcache_random_policy *fxcache_policy_random_create(
    fxcache_rng_fn rng_fn,
    void *rng_ctx
);

void fxcache_policy_random_destroy(
    fxcache_random_policy *policy
);

void fxcache_policy_random_init(
    fxcache *fc,
    fxcache_random_policy *policy
);

#ifdef __cplusplus
}
#endif

#endif /* FXCACHE_H */
