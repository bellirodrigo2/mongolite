#include "fxcache.h"

#include <stdlib.h>
#include <string.h>
#include <limits.h>

#include "uthash.h"
#include "utlist.h"

/* ============================================================
 *  Internal key storage (always 12 bytes)
 * ============================================================
 *  - OID mode: stores 12 OID bytes (inline).
 *  - BYTES mode: stores { void* ptr ; uint32_t len } packed into 12 bytes.
 *
 *  Note: key bytes themselves live at ptr (either user memory or owned copy).
 */
#define FLEXCACHE_INLINE_KEY_SIZE 12

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
 *  Internal bcache (hidden)
 * ============================================================ */
typedef struct bcache_node {
    uint8_t key_storage[FLEXCACHE_INLINE_KEY_SIZE];

    void   *value;      /* fxcache_entry* */
    int64_t byte_size;

    UT_hash_handle hh;

    struct bcache_node *prev, *next;
} bcache_node;

typedef struct bcache {
    bcache_node *hashmap;
    bcache_node *list;
    size_t  item_count;
    int64_t total_bytes;
} bcache;

static void
bcache_init(bcache *c)
{
    memset(c, 0, sizeof(*c));
}

static bcache_node *
bcache_node_new(void *value, int64_t byte_size)
{
    if (byte_size < 0) return NULL;
    bcache_node *n = (bcache_node *)uthash_malloc(sizeof *n);
    if (!n) return NULL;
    memset(n->key_storage, 0, sizeof(n->key_storage));
    n->value = value;
    n->byte_size = byte_size;
    n->prev = n->next = NULL;
    return n;
}

static int
bcache_insert_keyptr(bcache *c, bcache_node *n, const void *keyptr, unsigned keylen)
{
    if (!c || !n || !keyptr || keylen == 0) return -1;

    bcache_node *existing = NULL;
    HASH_FIND(hh, c->hashmap, keyptr, keylen, existing);
    if (existing) return -1;

    HASH_ADD_KEYPTR(hh, c->hashmap, keyptr, keylen, n);
    DL_APPEND(c->list, n);

    c->item_count++;
    c->total_bytes += n->byte_size;
    return 0;
}

static bcache_node *
bcache_get(bcache *c, const void *keyptr, unsigned keylen)
{
    bcache_node *n = NULL;
    HASH_FIND(hh, c->hashmap, keyptr, keylen, n);
    return n;
}

static void
bcache_remove_node(bcache *c, bcache_node *n)
{
    if (!c || !n) return;
    HASH_DEL(c->hashmap, n);
    DL_DELETE(c->list, n);
    c->item_count--;
    c->total_bytes -= n->byte_size;
    uthash_free(n, sizeof *n);
}

/* list reorder helpers (for LRU) */
static void
bcache_move_back(bcache *c, bcache_node *n)
{
    if (!c || !n) return;
    DL_DELETE(c->list, n);
    DL_APPEND(c->list, n);
}

/* ============================================================
 *  fxcache internals
 * ============================================================ */
typedef struct fxcache_entry {
    void    *user_value;
    uint64_t expires_at_ms;
} fxcache_entry;

struct fxcache {
    bcache base;

    fxcache_key_mode key_mode;
    fxcache_copy_fn key_copy;
    fxcache_free_fn key_free;

    fxcache_now_fn now_fn;

    fxcache_copy_fn value_copy;
    fxcache_free_fn value_free;

    fxcache_ondelete_fn ondelete;

    void (*touch_fn)(bcache *, bcache_node *, void *);
    bcache_node *(*pop_fn)(bcache *, void *);

    size_t  item_max;
    int64_t byte_max;

    uint64_t scan_interval_ms;
    uint64_t last_scan_ms;

    void *user_ctx;
    void *policy_ctx;
};

/* ============================================================
 *  Helpers
 * ============================================================ */
static inline uint64_t
safe_expiration(uint64_t now_ms, uint64_t ttl_ms)
{
    if (ttl_ms == 0) return 0;
    if (now_ms > UINT64_MAX - ttl_ms) return UINT64_MAX;
    return now_ms + ttl_ms;
}

static inline const void *
node_keyptr(const fxcache *fc, const bcache_node *n, unsigned *out_len)
{
    if (fc->key_mode == FLEXCACHE_KEY_OID) {
        if (out_len) *out_len = 12u;
        return n->key_storage;
    }

    /* BYTES */
    const void *p = unpack_ptr(n->key_storage);
    uint32_t len = unpack_len(n->key_storage);
    if (out_len) *out_len = (unsigned)len;
    return p;
}

static void
delete_node(fxcache *fc, bcache_node *n)
{
    if (!fc || !n) return;

    fxcache_entry *e = (fxcache_entry *)n->value;
    void *user_value = e ? e->user_value : NULL;

    /* Compute key view before removing node */
    unsigned klen_u = 0;
    const void *kptr = node_keyptr(fc, n, &klen_u);

    if (fc->ondelete) {
        fc->ondelete(kptr, (size_t)klen_u, user_value, n->byte_size, fc->user_ctx);
    }

    /* Remove from hash/list and free node memory */
    bcache_remove_node(&fc->base, n);

    /* Free key bytes if we own them (BYTES mode + key_copy provided) */
    if (fc->key_mode == FLEXCACHE_KEY_BYTES && fc->key_copy && fc->key_free && kptr) {
        fc->key_free((void *)kptr, fc->user_ctx);
    }

    /* Free value if we own it */
    if (fc->value_free && user_value) {
        fc->value_free(user_value, fc->user_ctx);
    }

    free(e);
}

static void
remove_expired(fxcache *fc, uint64_t now_ms)
{
    bcache_node *n, *tmp;
    HASH_ITER(hh, fc->base.hashmap, n, tmp) {
        fxcache_entry *e = (fxcache_entry *)n->value;
        if (e && e->expires_at_ms && e->expires_at_ms <= now_ms) {
            delete_node(fc, n);
        }
    }
}

static void
enforce_limits(fxcache *fc)
{
    for (;;) {
        int over_items = (fc->item_max != 0 && fc->base.item_count > fc->item_max);
        int over_bytes = (fc->byte_max != 0 && fc->base.total_bytes > fc->byte_max);
        if (!over_items && !over_bytes) break;

        bcache_node *victim = fc->pop_fn ? fc->pop_fn(&fc->base, fc->policy_ctx) : NULL;
        if (!victim) break;

        delete_node(fc, victim);
    }
}

/* ============================================================
 *  Policy hook setter (internal)
 * ============================================================ */
static void
fxcache_set_policy_hooks(
    fxcache *fc,
    void (*touch_fn)(bcache *, bcache_node *, void *),
    bcache_node *(*pop_fn)(bcache *, void *),
    void *policy_ctx
)
{
    if (!fc) return;
    fc->touch_fn = touch_fn;
    fc->pop_fn = pop_fn;
    fc->policy_ctx = policy_ctx;
}

/* ============================================================
 *  Public API
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
    if (key_mode != FLEXCACHE_KEY_OID && key_mode != FLEXCACHE_KEY_BYTES) return -1;

    bcache_init(&fc->base);

    fc->key_mode = key_mode;
    fc->key_copy = (key_mode == FLEXCACHE_KEY_BYTES) ? key_copy : NULL;
    fc->key_free = (key_mode == FLEXCACHE_KEY_BYTES) ? key_free : NULL;

    /* If copying keys, freeing must exist too (otherwise leak) */
    if (fc->key_copy && !fc->key_free) return -1;

    fc->now_fn = now_fn;

    fc->value_copy = value_copy;
    fc->value_free = value_free;

    fc->ondelete = ondelete;

    fc->item_max = item_max;
    fc->byte_max = byte_max;

    fc->scan_interval_ms = scan_interval_ms;
    fc->last_scan_ms = 0;

    fc->user_ctx = user_ctx;
    fc->policy_ctx = NULL;

    fc->touch_fn = NULL;
    fc->pop_fn = NULL;

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
    fxcache *fc = (fxcache *)malloc(sizeof(*fc));
    if (!fc) return NULL;

    if (fxcache_init(fc, key_mode, now_fn, item_max, byte_max, scan_interval_ms,
                      key_copy, key_free, value_copy, value_free, ondelete, user_ctx) != 0) {
        free(fc);
        return NULL;
    }
    return fc;
}

void
fxcache_destroy(fxcache *fc)
{
    if (!fc) return;
    while (fc->base.list) {
        delete_node(fc, fc->base.list);
    }
}

void
fxcache_free(fxcache *fc)
{
    if (!fc) return;
    fxcache_destroy(fc);
    free(fc);
}

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
            const void *p = unpack_ptr(n->key_storage);
            if (p) fc->key_free((void *)p, fc->user_ctx);
        }
        uthash_free(n, sizeof(*n));
        free(e);
        if (fc->value_free && v) fc->value_free(v, fc->user_ctx);
        return -1;
    }

    enforce_limits(fc);
    return 0;
}

void *
fxcache_get(fxcache *fc, const void *key, size_t key_len)
{
    if (!fc || !key || key_len == 0) return NULL;
    if (fc->key_mode == FLEXCACHE_KEY_OID && key_len != 12) return NULL;

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

void
fxcache_scan_and_clean(fxcache *fc)
{
    if (!fc) return;
    uint64_t now_ms = fc->now_fn(fc->user_ctx);
    remove_expired(fc, now_ms);
    enforce_limits(fc);
}

void
fxcache_maybe_scan_and_clean(fxcache *fc)
{
    if (!fc) return;
    uint64_t now_ms = fc->now_fn(fc->user_ctx);

    if (fc->scan_interval_ms == 0 ||
        fc->last_scan_ms == 0 ||
        (now_ms - fc->last_scan_ms) >= fc->scan_interval_ms) {
        fc->last_scan_ms = now_ms;
        remove_expired(fc, now_ms);
        enforce_limits(fc);
    }
}

size_t
fxcache_item_count(const fxcache *fc)
{
    return fc ? fc->base.item_count : 0;
}

int64_t
fxcache_total_bytes(const fxcache *fc)
{
    return fc ? fc->base.total_bytes : 0;
}

/* ============================================================
 *  FIFO policy
 * ============================================================ */
static void
fifo_touch(bcache *base, bcache_node *node, void *ctx)
{
    (void)base; (void)node; (void)ctx;
}

static bcache_node *
fifo_pop(bcache *base, void *ctx)
{
    (void)ctx;
    return base->list; /* oldest */
}

void
fxcache_policy_fifo_init(fxcache *fc)
{
    if (!fc) return;
    fxcache_set_policy_hooks(fc, fifo_touch, fifo_pop, NULL);
}

/* ============================================================
 *  LRU policy
 * ============================================================ */
static void
lru_touch(bcache *base, bcache_node *node, void *ctx)
{
    (void)ctx;
    bcache_move_back(base, node);
}

static bcache_node *
lru_pop(bcache *base, void *ctx)
{
    (void)ctx;
    return base->list; /* LRU */
}

void
fxcache_policy_lru_init(fxcache *fc)
{
    if (!fc) return;
    fxcache_set_policy_hooks(fc, lru_touch, lru_pop, NULL);
}

/* ============================================================
 *  RANDOM policy
 * ============================================================ */
struct fxcache_random_policy {
    fxcache_rng_fn rng_fn;
    void *rng_ctx;
};

static void
random_touch(bcache *base, bcache_node *node, void *ctx)
{
    (void)base; (void)node; (void)ctx;
}

static bcache_node *
random_pop(bcache *base, void *ctx)
{
    struct fxcache_random_policy *p = (struct fxcache_random_policy *)ctx;
    if (!base || !p || base->item_count == 0) return NULL;

    size_t idx = (size_t)(p->rng_fn(p->rng_ctx) % base->item_count);
    bcache_node *n = base->list;
    while (idx > 0 && n) {
        n = n->next;
        idx--;
    }
    return n;
}

fxcache_random_policy *
fxcache_policy_random_create(fxcache_rng_fn rng_fn, void *rng_ctx)
{
    if (!rng_fn) return NULL;
    struct fxcache_random_policy *p =
        (struct fxcache_random_policy *)malloc(sizeof(*p));
    if (!p) return NULL;
    p->rng_fn = rng_fn;
    p->rng_ctx = rng_ctx;
    return p;
}

void
fxcache_policy_random_destroy(fxcache_random_policy *policy)
{
    if (policy) free(policy);
}

void
fxcache_policy_random_init(fxcache *fc, fxcache_random_policy *policy)
{
    if (!fc || !policy) return;
    fxcache_set_policy_hooks(fc, random_touch, random_pop, policy);
}
