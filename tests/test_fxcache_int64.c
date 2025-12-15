#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include "fxcache.h"

static uint64_t fake_now_ms;
static uint64_t fake_now(void *ctx) {
    (void)ctx;
    return fake_now_ms;
}

static int ondelete_calls;

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
    assert_int_equal(key_len, sizeof(int64_t));
    ondelete_calls++;
}

static void test_int64_basic(void **state) {
    (void)state;
    fake_now_ms = 0;
    ondelete_calls = 0;

    fxcache *fc = fxcache_create(
        FLEXCACHE_KEY_INT64,
        fake_now,
        0, 0, 0,
        NULL, NULL,
        NULL, NULL,
        ondelete,
        NULL
    );

    int64_t key = 42;
    int value = 99;

    assert_int_equal(
        fxcache_insert_int64(fc, key, &value, sizeof(value), 1, 0, 0),
        0
    );

    int *out = fxcache_get_int64(fc, key);
    assert_non_null(out);
    assert_int_equal(*out, 99);

    fxcache_delete_int64(fc, key);
    assert_int_equal(ondelete_calls, 1);

    fxcache_destroy(fc);
}

static void test_int64_ttl(void **state) {
    (void)state;
    fake_now_ms = 1000;
    ondelete_calls = 0;

    fxcache *fc = fxcache_create(
        FLEXCACHE_KEY_INT64,
        fake_now,
        0, 0, 0,
        NULL, NULL,
        NULL, NULL,
        ondelete,
        NULL
    );

    int v = 1;
    fxcache_insert_int64(fc, 7, &v, sizeof(v), 1, 500, 0);

    assert_non_null(fxcache_get_int64(fc, 7));

    fake_now_ms = 2000;
    assert_null(fxcache_get_int64(fc, 7));
    assert_int_equal(ondelete_calls, 1);

    fxcache_destroy(fc);
}

static void test_int64_eviction(void **state) {
    (void)state;
    fake_now_ms = 0;

    fxcache *fc = fxcache_create(
        FLEXCACHE_KEY_INT64,
        fake_now,
        2, 0, 0,
        NULL, NULL,
        NULL, NULL,
        NULL,
        NULL
    );

    int v = 1;
    fxcache_insert_int64(fc, 1, &v, sizeof(v), 1, 0, 0);
    fxcache_insert_int64(fc, 2, &v, sizeof(v), 1, 0, 0);
    fxcache_insert_int64(fc, 3, &v, sizeof(v), 1, 0, 0);

    assert_null(fxcache_get_int64(fc, 1));
    assert_non_null(fxcache_get_int64(fc, 2));
    assert_non_null(fxcache_get_int64(fc, 3));

    fxcache_destroy(fc);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_int64_basic),
        cmocka_unit_test(test_int64_ttl),
        cmocka_unit_test(test_int64_eviction),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
