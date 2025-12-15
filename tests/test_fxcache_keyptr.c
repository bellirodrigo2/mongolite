#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include "fxcache.h"

#include <string.h>
#include <stdlib.h>

/* ============================================================
 *  Globals / fakes
 * ============================================================ */
static uint64_t fake_now_ms;

static uint64_t fake_now(void *ctx) {
    (void)ctx;
    return fake_now_ms;
}

static int key_copies, key_frees;
static int value_copies, value_frees;
static int ondelete_calls;

static void reset_counters(void) {
    key_copies = key_frees = 0;
    value_copies = value_frees = 0;
    ondelete_calls = 0;
}

/* ============================================================
 *  Callbacks
 * ============================================================ */
static void *key_copy(const void *p, size_t len, void *ctx) {
    (void)ctx;
    key_copies++;
    void *m = malloc(len);
    memcpy(m, p, len);
    return m;
}

static void key_free(void *p, void *ctx) {
    (void)ctx;
    key_frees++;
    free(p);
}

static void *value_copy(const void *p, size_t len, void *ctx) {
    (void)ctx;
    value_copies++;
    void *m = malloc(len);
    memcpy(m, p, len);
    return m;
}

static void value_free(void *p, void *ctx) {
    (void)ctx;
    value_frees++;
    free(p);
}

static void ondelete(
    const void *key,
    size_t key_len,
    void *value,
    int64_t byte_size,
    void *ctx
) {
    (void)key;
    (void)value;
    (void)byte_size;
    (void)ctx;
    assert_true(key_len > 0);
    ondelete_calls++;
}

/* ============================================================
 *  Tests
 * ============================================================ */

static void test_insert_get_delete(void **state) {
    (void)state;
    reset_counters();
    fake_now_ms = 1000;

    fxcache *fc = fxcache_create(
        FLEXCACHE_KEY_BYTES,
        fake_now,
        0, 0, 0,
        key_copy, key_free,
        value_copy, value_free,
        ondelete,
        NULL
    );
    assert_non_null(fc);

    const char *key = "abc";
    const char *val = "value";

    assert_int_equal(
        fxcache_insert(fc, key, strlen(key), val, strlen(val), 1, 0, 0),
        0
    );

    char *out = fxcache_get(fc, key, strlen(key));
    assert_non_null(out);
    assert_string_equal(out, val);

    assert_int_equal(fxcache_delete(fc, key, strlen(key)), 0);
    assert_null(fxcache_get(fc, key, strlen(key)));

    assert_int_equal(ondelete_calls, 1);
    assert_int_equal(key_copies, 1);
    assert_int_equal(key_frees, 1);
    assert_int_equal(value_copies, 1);
    assert_int_equal(value_frees, 1);

    fxcache_destroy(fc);
}

static void test_duplicate_key(void **state) {
    (void)state;
    reset_counters();
    fake_now_ms = 0;

    fxcache *fc = fxcache_create(
        FLEXCACHE_KEY_BYTES,
        fake_now,
        0, 0, 0,
        key_copy, key_free,
        value_copy, value_free,
        ondelete,
        NULL
    );

    const char *key = "dup";
    const char *v1 = "v1";
    const char *v2 = "v2";

    assert_int_equal(fxcache_insert(fc, key, 3, v1, 2, 1, 0, 0), 0);
    assert_int_equal(fxcache_insert(fc, key, 3, v2, 2, 1, 0, 0), -1);

    assert_int_equal(key_copies, 2);
    assert_int_equal(key_frees, 1); /* second freed */
    assert_int_equal(value_frees, 1);

    fxcache_destroy(fc);
}

static void test_ttl_expiration(void **state) {
    (void)state;
    reset_counters();

    fxcache *fc = fxcache_create(
        FLEXCACHE_KEY_BYTES,
        fake_now,
        0, 0, 0,
        key_copy, key_free,
        value_copy, value_free,
        ondelete,
        NULL
    );

    fake_now_ms = 1000;
    assert_int_equal(
        fxcache_insert(fc, "k", 1, "v", 1, 1, 500, 0),
        0
    );

    assert_non_null(fxcache_get(fc, "k", 1));

    fake_now_ms = 1600;
    assert_null(fxcache_get(fc, "k", 1));
    assert_int_equal(ondelete_calls, 1);

    fxcache_destroy(fc);
}

static void test_item_max_eviction(void **state) {
    (void)state;
    reset_counters();
    fake_now_ms = 0;

    fxcache *fc = fxcache_create(
        FLEXCACHE_KEY_BYTES,
        fake_now,
        2, 0, 0,
        key_copy, key_free,
        value_copy, value_free,
        ondelete,
        NULL
    );

    fxcache_insert(fc, "a", 1, "v", 1, 1, 0, 0);
    fxcache_insert(fc, "b", 1, "v", 1, 1, 0, 0);
    fxcache_insert(fc, "c", 1, "v", 1, 1, 0, 0);

    assert_null(fxcache_get(fc, "a", 1));
    assert_non_null(fxcache_get(fc, "b", 1));
    assert_non_null(fxcache_get(fc, "c", 1));

    fxcache_destroy(fc);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_insert_get_delete),
        cmocka_unit_test(test_duplicate_key),
        cmocka_unit_test(test_ttl_expiration),
        cmocka_unit_test(test_item_max_eviction),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
