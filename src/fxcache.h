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
    FLEXCACHE_KEY_BYTES = 2
} fxcache_key_mode;

/* ============================================================
 *  Time source
 * ============================================================ */
typedef uint64_t (*fxcache_now_fn)(void *user_ctx);

/* ============================================================
 *  Memory management callbacks (keys and values)
 * ============================================================ */
typedef void *(*fxcache_copy_fn)(const void *ptr, size_t len, void *user_ctx);
typedef void  (*fxcache_free_fn)(void *ptr, void *user_ctx);

/* ============================================================
 *  Deletion hook
 * ============================================================ */
typedef void (*fxcache_ondelete_fn)(
    const void   *key,
    size_t        key_len,
    void         *value,
    int64_t       byte_size,
    void         *user_ctx
);

/* ============================================================
 *  Random policy RNG
 * ============================================================ */
typedef uint32_t (*fxcache_rng_fn)(void *rng_ctx);

/* ============================================================
 *  Lifecycle
 * ============================================================ */

/**
 * Initialize a fxcache instance.
 *
 * key_mode:
 *  - FLEXCACHE_KEY_OID: key callbacks are ignored; OID bytes are copied inline.
 *  - FLEXCACHE_KEY_BYTES: if key_copy is non-NULL, keys are copied and later freed with key_free.
 *                         if key_copy is NULL, keys are stored by pointer and MUST remain valid.
 */
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
void fxcache_free(fxcache *fc);

/* ============================================================
 *  Operations (generic)
 * ============================================================ */

/**
 * Insert an item.
 *
 * For FLEXCACHE_KEY_OID, key_len must be 12 and key must point to bson_oid_t bytes.
 * For FLEXCACHE_KEY_BYTES, key/key_len are arbitrary (e.g., string bytes).
 *
 * ttl_ms > 0 has priority over expires_at_ms.
 *
 * Returns: 0 success, -1 duplicate key, -2 allocation error / invalid args.
 */
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
int   fxcache_delete(fxcache *fc, const void *key, size_t key_len);

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
 *  Maintenance
 * ============================================================ */
void fxcache_scan_and_clean(fxcache *fc);
void fxcache_maybe_scan_and_clean(fxcache *fc);

/* ============================================================
 *  Statistics
 * ============================================================ */
size_t  fxcache_item_count(const fxcache *fc);
int64_t fxcache_total_bytes(const fxcache *fc);

/* ============================================================
 *  Eviction policies
 * ============================================================ */
void fxcache_policy_fifo_init(fxcache *fc);
void fxcache_policy_lru_init(fxcache *fc);

fxcache_random_policy *
fxcache_policy_random_create(
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
