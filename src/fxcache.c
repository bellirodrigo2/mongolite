#include "fxcache.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "uthash.h"

/* ============================================================
 *  Internal helpers / key packing
 * ============================================================
 *
 * For OID mode, we store 12 bytes inline (BSON OID).
 * For INT64 mode, we store 8 bytes inline (int64_t).
 * For BYTES mode, we store:
 *   - pointer bytes (sizeof(void*))
 *   - 32-bit length (uint32_t)
 *   The key bytes themselves live at ptr (either user memory or owned copy).
 */

#define FLEXCACHE_OID_INLINE_SIZE 12
#define FLEXCACHE_PTRLEN_INLINE_SIZE (sizeof(void*) + sizeof(uint32_t))
#define FLEXCACHE_INLINE_KEY_SIZE \
    ((FLEXCACHE_PTRLEN_INLINE_SIZE > FLEXCACHE_OID_INLINE_SIZE) ? \
     FLEXCACHE_PTRLEN_INLINE_SIZE : FLEXCACHE_OID_INLINE_SIZE)

/* C99-compatible static assert */
#define FLEXCACHE_STATIC_ASSERT(cond, msg) \
    typedef char flexcache_static_assert_##msg[(cond) ? 1 : -1]

FLEXCACHE_STATIC_ASSERT(FLEXCACHE_INLINE_KEY_SIZE >= 12, inline_key_size_must_be_at_least_12);

static inline void
pack_ptr_len(uint8_t out[FLEXCACHE_INLINE_KEY_SIZE], const void *p, uint32_t len)
{
    /* store pointer bytes first */
    memset(out, 0, FLEXCACHE_INLINE_KEY_SIZE);
    memcpy(out, &p, sizeof(void*));
    memcpy(out + sizeof(void*), &len, sizeof(uint32_t));
}

static inline const void *
unpack_ptr(const uint8_t in[FLEXCACHE_INLINE_KEY_SIZE])
{
    const void *p = NULL;
    memcpy(&p, in, sizeof(void*));
    return p;
}

static inline uint32_t
unpack_len(const uint8_t in[FLEXCACHE_INLINE_KEY_SIZE])
{
    uint32_t len = 0;
    memcpy(&len, in + sizeof(void*), sizeof(uint32_t));
    return len;
}

/* ============================================================
 *  Cache node / base store
 * ============================================================ */
typedef struct bcache_node {
    uint8_t key_storage[FLEXCACHE_INLINE_KEY_SIZE];

    void   *value;      /* fxcache_entry* */
    int64_t byte_size;

    UT_hash_handle hh;

    struct bcache_node *prev;
    struct bcache_node *next;
} bcache_node;

typedef struct bcache {
    bcache_node *map;   /* uthash table head */
    bcache_node *head;  /* policy list head (FIFO/LRU-ish) */
    bcache_node *tail;

    size_t  item_count;
    int64_t total_bytes;
} bcache;

typedef struct fxcache_entry {
    void    *user_value;
    uint64_t expires_at_ms;
} fxcache_entry;

struct fxcache {
    bcache base;

    fxcache_key_mode key_mode;

    fxcache_now_fn now_fn;

    size_t  item_max;
    int64_t byte_max;
    uint64_t scan_interval_ms;
    uint64_t last_scan_ms;

    fxcache_copy_fn key_copy;
    fxcache_free_fn key_free;
    fxcache_copy_fn value_copy;
    fxcache_free_fn value_free;

    fxcache_ondelete_fn ondelete;
    void *user_ctx;

    /* policy hooks */
    void (*touch_fn)(bcache *b, bcache_node *n, void *policy_ctx);
    bcache_node *(*pop_fn)(bcache *b, void *policy_ctx);
    void *policy_ctx;
};

/* ============================================================
 *  List helpers (policy list)
 * ============================================================ */
static inline void
list_remove(bcache *b, bcache_node *n)
{
    if (!n) return;
    if (n->prev) n->prev->next = n->next;
    else         b->head = n->next;

    if (n->next) n->next->prev = n->prev;
    else         b->tail = n->prev;

    n->prev = n->next = NULL;
}

static inline void
list_push_tail(bcache *b, bcache_node *n)
{
    n->prev = b->tail;
    n->next = NULL;
    if (b->tail) b->tail->next = n;
    else         b->head = n;
    b->tail = n;
}

/* ============================================================
 *  uthash wrappers
 * ============================================================ */
static inline bcache_node *
bcache_get(bcache *b, const void *keyptr, unsigned keylen)
{
    bcache_node *n = NULL;
    HASH_FIND(hh, b->map, keyptr, keylen, n);
    return n;
}

static inline int
bcache_insert_keyptr(bcache *b, bcache_node *n, const void *keyptr, unsigned keylen)
{
    bcache_node *existing = NULL;
    HASH_FIND(hh, b->map, keyptr, keylen, existing);
    if (existing) return -1;

    HASH_ADD_KEYPTR(hh, b->map, keyptr, keylen, n);
    b->item_count++;
    b->total_bytes += n->byte_size;
    return 0;
}

static inline void
bcache_remove_keyptr(bcache *b, bcache_node *n)
{
    if (!n) return;
    HASH_DEL(b->map, n);
    b->item_count--;
    b->total_bytes -= n->byte_size;
}

/* ============================================================
 *  Eviction / delete helpers
 * ============================================================ */
static void
uthash_free(bcache_node *n, size_t sz)
{
    (void)sz;
    free(n);
}

static bcache_node *
bcache_node_new(void *value, int64_t byte_size)
{
    bcache_node *n = (bcache_node *)calloc(1, sizeof(*n));
    if (!n) return NULL;
    n->value = value;
    n->byte_size = byte_size;
    return n;
}

static inline uint64_t
safe_expiration(uint64_t now_ms, uint64_t ttl_ms)
{
    uint64_t max = UINT64_MAX;
    if (ttl_ms > max - now_ms) return max;
    return now_ms + ttl_ms;
}

static void
delete_node(fxcache *fc, bcache_node *n)
{
    if (!fc || !n) return;

    /* unlink from policy list and hash table */
    list_remove(&fc->base, n);
    bcache_remove_keyptr(&fc->base, n);

    /* pull entry */
    fxcache_entry *e = (fxcache_entry *)n->value;
    void *user_val = e ? e->user_value : NULL;

    /* Fire ondelete before freeing (key/value are valid during callback only) */
    if (fc->ondelete) {
        if (fc->key_mode == FLEXCACHE_KEY_OID) {
            fc->ondelete(n->key_storage, 12, user_val, n->byte_size, fc->user_ctx);
        } else if (fc->key_mode == FLEXCACHE_KEY_INT64) {
            fc->ondelete(n->key_storage, sizeof(int64_t), user_val, n->byte_size, fc->user_ctx);
        } else {
            const void *kptr = unpack_ptr(n->key_storage);
            uint32_t klen = unpack_len(n->key_storage);
            fc->ondelete(kptr, (size_t)klen, user_val, n->byte_size, fc->user_ctx);
        }
    }

    /* free key if owned (BYTES mode only) */
    if (fc->key_mode == FLEXCACHE_KEY_BYTES && fc->key_copy && fc->key_free) {
        void *kptr = (void *)unpack_ptr(n->key_storage);
        if (kptr) fc->key_free(kptr, fc->user_ctx);
    }

    /* free value if owned */
    if (fc->value_free && user_val) {
        fc->value_free(user_val, fc->user_ctx);
    }

    free(e);
    uthash_free(n, sizeof(*n));
}

static void
enforce_limits(fxcache *fc)
{
    if (!fc) return;

    /* Evict until within limits */
    while ((fc->item_max > 0 && fc->base.item_count > fc->item_max) ||
           (fc->byte_max > 0 && fc->base.total_bytes > fc->byte_max))
    {
        bcache_node *victim = NULL;
        if (fc->pop_fn) victim = fc->pop_fn(&fc->base, fc->policy_ctx);
        if (!victim) break;
        delete_node(fc, victim);
    }
}

static void
remove_expired(fxcache *fc, uint64_t now_ms)
{
    if (!fc) return;

    bcache_node *n, *tmp;
    HASH_ITER(hh, fc->base.map, n, tmp) {
        fxcache_entry *e = (fxcache_entry *)n->value;
        if (e && e->expires_at_ms && e->expires_at_ms <= now_ms) {
            delete_node(fc, n);
        }
    }
}

/* ============================================================
 *  Policies: FIFO (default)
 * ============================================================ */
static void
fifo_touch(bcache *b, bcache_node *n, void *policy_ctx)
{
    (void)policy_ctx;
    if (!b || !n) return;
    /* FIFO doesn't move on touch */
}

static bcache_node *
fifo_pop(bcache *b, void *policy_ctx)
{
    (void)policy_ctx;
    if (!b) return NULL;
    /* evict from head (oldest) */
    return b->head;
}

/* ============================================================
 *  Policy: RANDOM
 * ============================================================ */
struct fxcache_random_policy {
    fxcache_rng_fn rng_fn;
    void *rng_ctx;
};

static bcache_node *
random_pop(bcache *b, void *policy_ctx)
{
    fxcache_random_policy *p = (fxcache_random_policy *)policy_ctx;
    if (!b || !p || !p->rng_fn || b->item_count == 0) return NULL;

    /* Choose an index in [0, item_count-1] */
    uint32_t r = p->rng_fn(p->rng_ctx);
    size_t idx = (size_t)(r % (uint32_t)b->item_count);

    bcache_node *n = b->head;
    while (n && idx--) n = n->next;
    return n ? n : b->head;
}

fxcache_random_policy *
fxcache_policy_random_create(fxcache_rng_fn rng_fn, void *rng_ctx)
{
    if (!rng_fn) return NULL;
    fxcache_random_policy *p = (fxcache_random_policy *)calloc(1, sizeof(*p));
    if (!p) return NULL;
    p->rng_fn = rng_fn;
    p->rng_ctx = rng_ctx;
    return p;
}

void
fxcache_policy_random_destroy(fxcache_random_policy *policy)
{
    free(policy);
}

void
fxcache_policy_random_init(fxcache *fc, fxcache_random_policy *policy)
{
    if (!fc || !policy) return;
    fc->touch_fn = NULL; /* no touch behavior needed */
    fc->pop_fn   = random_pop;
    fc->policy_ctx = policy;
}

/* ============================================================
 *  Init / create / destroy
 * ============================================================ */
int
fxcache_init(
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
)
{
    if (!fc || !now_fn) return -1;
    if (key_mode != FLEXCACHE_KEY_OID && key_mode != FLEXCACHE_KEY_BYTES && key_mode != FLEXCACHE_KEY_INT64) return -1;

    memset(fc, 0, sizeof(*fc));

    fc->key_mode = key_mode;
    fc->now_fn = now_fn;

    fc->item_max = item_max;
    fc->byte_max = byte_max;
    fc->scan_interval_ms = scan_interval_ms;
    fc->last_scan_ms = 0;

    fc->key_copy = key_copy;
    fc->key_free = key_free;
    fc->value_copy = value_copy;
    fc->value_free = value_free;
    fc->ondelete = ondelete;
    fc->user_ctx = user_ctx;

    /* default policy FIFO */
    fc->touch_fn = fifo_touch;
    fc->pop_fn = fifo_pop;
    fc->policy_ctx = NULL;

    return 0;
}

fxcache *
fxcache_create(
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
)
{
    fxcache *fc = (fxcache *)calloc(1, sizeof(*fc));
    if (!fc) return NULL;

    if (fxcache_init(fc, key_mode, now_fn, item_max, byte_max,
                     scan_interval_ms, key_copy, key_free,
                     value_copy, value_free, ondelete, user_ctx) != 0)
    {
        free(fc);
        return NULL;
    }
    return fc;
}

void
fxcache_destroy(fxcache *fc)
{
    if (!fc) return;

    /* delete all nodes */
    bcache_node *n, *tmp;
    HASH_ITER(hh, fc->base.map, n, tmp) {
        delete_node(fc, n);
    }
}

void
fxcache_policy_fifo_init(fxcache *fc)
{
    if (!fc) return;
    fc->touch_fn = fifo_touch;
    fc->pop_fn = fifo_pop;
    fc->policy_ctx = NULL;
}

/* ============================================================
 *  Scan scheduling
 * ============================================================ */
void
fxcache_maybe_scan_and_clean(fxcache *fc)
{
    if (!fc) return;
    uint64_t now_ms = fc->now_fn(fc->user_ctx);

    if (fc->scan_interval_ms == 0 ||
        fc->last_scan_ms == 0 ||
        (now_ms - fc->last_scan_ms) >= fc->scan_interval_ms)
    {
        fc->last_scan_ms = now_ms;
        remove_expired(fc, now_ms);
        enforce_limits(fc);
    }
}

/* ============================================================
 *  Public API
 * ============================================================ */
int
fxcache_insert(
    fxcache *fc,
    const void *key,
    size_t key_len,
    const void *value,
    size_t value_len,
    int64_t byte_size,
    uint64_t ttl_ms,
    uint64_t expires_at_ms
)
{
    if (!fc || !key || key_len == 0 || byte_size < 0) return -2;

    if (fc->key_mode == FLEXCACHE_KEY_OID && key_len != 12) {
        return -2;
    }
    if (fc->key_mode == FLEXCACHE_KEY_INT64 && key_len != sizeof(int64_t)) {
        return -2;
    }

    fxcache_maybe_scan_and_clean(fc);

    /* Copy or store value */
    void *v = fc->value_copy ? fc->value_copy(value, value_len, fc->user_ctx) : (void *)value;
    if (fc->value_copy && !v) return -2;

    fxcache_entry *e = (fxcache_entry *)malloc(sizeof(*e));
    if (!e) {
        if (fc->value_free && v) fc->value_free(v, fc->user_ctx);
        return -2;
    }
    e->user_value = v;

    uint64_t now_ms = fc->now_fn(fc->user_ctx);
    if (ttl_ms > 0) {
        e->expires_at_ms = safe_expiration(now_ms, ttl_ms);
    } else {
        e->expires_at_ms = expires_at_ms;
    }

    bcache_node *n = bcache_node_new(e, byte_size);
    if (!n) {
        free(e);
        if (fc->value_free && v) fc->value_free(v, fc->user_ctx);
        return -2;
    }

    const void *keyptr = NULL;
    unsigned keylen_u = (unsigned)key_len;

    if (fc->key_mode == FLEXCACHE_KEY_OID) {
        /* store inline OID bytes */
        memcpy(n->key_storage, key, 12);
        keyptr = n->key_storage;
        keylen_u = 12u;
    } else if (fc->key_mode == FLEXCACHE_KEY_INT64) {
        /* store inline int64 bytes (no allocations) */
        memcpy(n->key_storage, key, sizeof(int64_t));
        keyptr = n->key_storage;
        keylen_u = (unsigned)sizeof(int64_t);
    } else {
        /* BYTES: store a pointer to key bytes + store len in the extra 4 bytes */
        const void *stored = key;
        if (fc->key_copy) {
            stored = fc->key_copy(key, key_len, fc->user_ctx);
            if (!stored) {
                uthash_free(n, sizeof(*n));
                free(e);
                if (fc->value_free && v) fc->value_free(v, fc->user_ctx);
                return -2;
            }
        }
        pack_ptr_len(n->key_storage, stored, (uint32_t)key_len);
        keyptr = stored;
        keylen_u = (unsigned)key_len;
    }

    if (bcache_insert_keyptr(&fc->base, n, keyptr, keylen_u) != 0) {
        /* duplicate */
        if (fc->key_mode == FLEXCACHE_KEY_BYTES && fc->key_copy && fc->key_free) {
            void *kptr = (void *)unpack_ptr(n->key_storage);
            if (kptr) fc->key_free(kptr, fc->user_ctx);
        }
        if (fc->value_free && v) fc->value_free(v, fc->user_ctx);
        free(e);
        uthash_free(n, sizeof(*n));
        return -1;
    }

    /* add to policy list tail */
    list_push_tail(&fc->base, n);

    enforce_limits(fc);
    return 0;
}

void *
fxcache_get(fxcache *fc, const void *key, size_t key_len)
{
    if (!fc || !key || key_len == 0) return NULL;
    if (fc->key_mode == FLEXCACHE_KEY_OID && key_len != 12) return NULL;
    if (fc->key_mode == FLEXCACHE_KEY_INT64 && key_len != sizeof(int64_t)) return NULL;

    fxcache_maybe_scan_and_clean(fc);

    bcache_node *n = bcache_get(&fc->base, key, (unsigned)key_len);
    if (!n) return NULL;

    fxcache_entry *e = (fxcache_entry *)n->value;
    uint64_t now_ms = fc->now_fn(fc->user_ctx);

    if (e && e->expires_at_ms && e->expires_at_ms <= now_ms) {
        delete_node(fc, n);
        return NULL;
    }

    if (fc->touch_fn) fc->touch_fn(&fc->base, n, fc->policy_ctx);
    return e ? e->user_value : NULL;
}

int
fxcache_delete(fxcache *fc, const void *key, size_t key_len)
{
    if (!fc || !key || key_len == 0) return -1;
    if (fc->key_mode == FLEXCACHE_KEY_OID && key_len != 12) return -1;
    if (fc->key_mode == FLEXCACHE_KEY_INT64 && key_len != sizeof(int64_t)) return -1;

    bcache_node *n = bcache_get(&fc->base, key, (unsigned)key_len);
    if (!n) return -1;

    delete_node(fc, n);
    return 0;
}

/* Convenience API for OID mode */
inline int
fxcache_insert_oid(
    fxcache *fc,
    const bson_oid_t *oid,
    const void *value,
    size_t value_len,
    int64_t byte_size,
    uint64_t ttl_ms,
    uint64_t expires_at_ms
)
{
    return fxcache_insert(fc, oid, 12, value, value_len, byte_size, ttl_ms, expires_at_ms);
}

inline void *
fxcache_get_oid(fxcache *fc, const bson_oid_t *oid)
{
    return fxcache_get(fc, oid, 12);
}

inline int
fxcache_delete_oid(fxcache *fc, const bson_oid_t *oid)
{
    return fxcache_delete(fc, oid, 12);
}


/* Convenience API for INT64 mode */
inline int
fxcache_insert_int64(
    fxcache *fc,
    int64_t key,
    const void *value,
    size_t value_len,
    int64_t byte_size,
    uint64_t ttl_ms,
    uint64_t expires_at_ms
)
{
    return fxcache_insert(fc, &key, sizeof(key), value, value_len, byte_size, ttl_ms, expires_at_ms);
}

inline void *
fxcache_get_int64(fxcache *fc, int64_t key)
{
    return fxcache_get(fc, &key, sizeof(key));
}

inline int
fxcache_delete_int64(fxcache *fc, int64_t key)
{
    return fxcache_delete(fc, &key, sizeof(key));
}

void
fxcache_scan_and_clean(fxcache *fc)
{
    if (!fc) return;
    uint64_t now_ms = fc->now_fn(fc->user_ctx);
    remove_expired(fc, now_ms);
    enforce_limits(fc);
}

size_t
fxcache_item_count(const fxcache *fc)
{
    if (!fc) return 0;
    return fc->base.item_count;
}
