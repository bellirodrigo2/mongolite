#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include "fxcache.h"
#include <bson/bson.h>

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
    assert_int_equal(key_len, 12);
    ondelete_calls++;
}

static void test_oid_basic(void **state) {
    (void)state;
    fake_now_ms = 0;
    ondelete_calls = 0;

    fxcache *fc = fxcache_create(
        FLEXCACHE_KEY_OID,
        fake_now,
        0, 0, 0,
        NULL, NULL,
        NULL, NULL,
        ondelete,
        NULL
    );

    bson_oid_t oid;
    bson_oid_init(&oid, NULL);

    int value = 123;
    assert_int_equal(
        fxcache_insert_oid(fc, &oid, &value, sizeof(value), 1, 0, 0),
        0
    );

    int *out = fxcache_get_oid(fc, &oid);
    assert_non_null(out);
    assert_int_equal(*out, 123);

    fxcache_delete_oid(fc, &oid);
    assert_int_equal(ondelete_calls, 1);

    fxcache_destroy(fc);
}

static void test_oid_duplicate(void **state) {
    (void)state;
    fake_now_ms = 0;

    fxcache *fc = fxcache_create(
        FLEXCACHE_KEY_OID,
        fake_now,
        0, 0, 0,
        NULL, NULL,
        NULL, NULL,
        NULL,
        NULL
    );

    bson_oid_t oid;
    bson_oid_init(&oid, NULL);

    int v = 1;
    assert_int_equal(fxcache_insert_oid(fc, &oid, &v, sizeof(v), 1, 0, 0), 0);
    assert_int_equal(fxcache_insert_oid(fc, &oid, &v, sizeof(v), 1, 0, 0), -1);

    fxcache_destroy(fc);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_oid_basic),
        cmocka_unit_test(test_oid_duplicate),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
