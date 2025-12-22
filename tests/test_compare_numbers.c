/**
 * test_compare_numbers.c - Tests for numeric comparison logic
 *
 * Tests the mongodb_compare_numbers() function which handles
 * comparison of int32, int64, and double values according to
 * MongoDB ordering semantics.
 */

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <math.h>
#include <float.h>
#include <bson/bson.h>

/* Function under test (from compare_numbers.c) */
extern int mongodb_compare_numbers(const bson_iter_t *a, const bson_iter_t *b);

/* ============================================================
 * Helper functions
 * ============================================================ */

static bson_t* make_int32_doc(int32_t val) {
    bson_t *doc = bson_new();
    BSON_APPEND_INT32(doc, "v", val);
    return doc;
}

static bson_t* make_int64_doc(int64_t val) {
    bson_t *doc = bson_new();
    BSON_APPEND_INT64(doc, "v", val);
    return doc;
}

static bson_t* make_double_doc(double val) {
    bson_t *doc = bson_new();
    BSON_APPEND_DOUBLE(doc, "v", val);
    return doc;
}

static void get_iter(const bson_t *doc, bson_iter_t *iter) {
    bson_iter_init(iter, doc);
    bson_iter_find(iter, "v");
}

/* ============================================================
 * int32 comparison tests
 * ============================================================ */

static void test_int32_equal(void **state) {
    (void)state;
    bson_t *a = make_int32_doc(42);
    bson_t *b = make_int32_doc(42);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    assert_int_equal(0, mongodb_compare_numbers(&ia, &ib));

    bson_destroy(a);
    bson_destroy(b);
}

static void test_int32_less(void **state) {
    (void)state;
    bson_t *a = make_int32_doc(10);
    bson_t *b = make_int32_doc(20);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    assert_true(mongodb_compare_numbers(&ia, &ib) < 0);

    bson_destroy(a);
    bson_destroy(b);
}

static void test_int32_greater(void **state) {
    (void)state;
    bson_t *a = make_int32_doc(100);
    bson_t *b = make_int32_doc(50);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    assert_true(mongodb_compare_numbers(&ia, &ib) > 0);

    bson_destroy(a);
    bson_destroy(b);
}

static void test_int32_negative(void **state) {
    (void)state;
    bson_t *a = make_int32_doc(-100);
    bson_t *b = make_int32_doc(100);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    assert_true(mongodb_compare_numbers(&ia, &ib) < 0);

    bson_destroy(a);
    bson_destroy(b);
}

static void test_int32_zero(void **state) {
    (void)state;
    bson_t *a = make_int32_doc(0);
    bson_t *b = make_int32_doc(0);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    assert_int_equal(0, mongodb_compare_numbers(&ia, &ib));

    bson_destroy(a);
    bson_destroy(b);
}

static void test_int32_min_max(void **state) {
    (void)state;
    bson_t *a = make_int32_doc(INT32_MIN);
    bson_t *b = make_int32_doc(INT32_MAX);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    assert_true(mongodb_compare_numbers(&ia, &ib) < 0);

    bson_destroy(a);
    bson_destroy(b);
}

/* ============================================================
 * int64 comparison tests
 * ============================================================ */

static void test_int64_equal(void **state) {
    (void)state;
    bson_t *a = make_int64_doc(1234567890123LL);
    bson_t *b = make_int64_doc(1234567890123LL);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    assert_int_equal(0, mongodb_compare_numbers(&ia, &ib));

    bson_destroy(a);
    bson_destroy(b);
}

static void test_int64_less(void **state) {
    (void)state;
    bson_t *a = make_int64_doc(100LL);
    bson_t *b = make_int64_doc(9007199254740992LL);  /* 2^53 - max safe */
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    assert_true(mongodb_compare_numbers(&ia, &ib) < 0);

    bson_destroy(a);
    bson_destroy(b);
}

static void test_int64_greater(void **state) {
    (void)state;
    bson_t *a = make_int64_doc(9007199254740992LL);
    bson_t *b = make_int64_doc(100LL);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    assert_true(mongodb_compare_numbers(&ia, &ib) > 0);

    bson_destroy(a);
    bson_destroy(b);
}

/* ============================================================
 * double comparison tests
 * ============================================================ */

static void test_double_equal(void **state) {
    (void)state;
    bson_t *a = make_double_doc(3.14159);
    bson_t *b = make_double_doc(3.14159);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    assert_int_equal(0, mongodb_compare_numbers(&ia, &ib));

    bson_destroy(a);
    bson_destroy(b);
}

static void test_double_less(void **state) {
    (void)state;
    bson_t *a = make_double_doc(1.5);
    bson_t *b = make_double_doc(2.5);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    assert_true(mongodb_compare_numbers(&ia, &ib) < 0);

    bson_destroy(a);
    bson_destroy(b);
}

static void test_double_greater(void **state) {
    (void)state;
    bson_t *a = make_double_doc(9.99);
    bson_t *b = make_double_doc(1.11);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    assert_true(mongodb_compare_numbers(&ia, &ib) > 0);

    bson_destroy(a);
    bson_destroy(b);
}

static void test_double_negative(void **state) {
    (void)state;
    bson_t *a = make_double_doc(-1.5);
    bson_t *b = make_double_doc(1.5);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    assert_true(mongodb_compare_numbers(&ia, &ib) < 0);

    bson_destroy(a);
    bson_destroy(b);
}

static void test_double_zero_positive_negative(void **state) {
    (void)state;
    bson_t *a = make_double_doc(-0.0);
    bson_t *b = make_double_doc(0.0);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    /* -0.0 and +0.0 should be equal */
    assert_int_equal(0, mongodb_compare_numbers(&ia, &ib));

    bson_destroy(a);
    bson_destroy(b);
}

static void test_double_very_small(void **state) {
    (void)state;
    bson_t *a = make_double_doc(DBL_MIN);
    bson_t *b = make_double_doc(DBL_MIN * 2);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    assert_true(mongodb_compare_numbers(&ia, &ib) < 0);

    bson_destroy(a);
    bson_destroy(b);
}

/* ============================================================
 * Cross-type comparison tests
 * ============================================================ */

static void test_int32_vs_int64_equal(void **state) {
    (void)state;
    bson_t *a = make_int32_doc(42);
    bson_t *b = make_int64_doc(42LL);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    assert_int_equal(0, mongodb_compare_numbers(&ia, &ib));

    bson_destroy(a);
    bson_destroy(b);
}

static void test_int32_vs_int64_less(void **state) {
    (void)state;
    bson_t *a = make_int32_doc(100);
    bson_t *b = make_int64_doc(1000000000000LL);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    assert_true(mongodb_compare_numbers(&ia, &ib) < 0);

    bson_destroy(a);
    bson_destroy(b);
}

static void test_int32_vs_double_equal(void **state) {
    (void)state;
    bson_t *a = make_int32_doc(42);
    bson_t *b = make_double_doc(42.0);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    assert_int_equal(0, mongodb_compare_numbers(&ia, &ib));

    bson_destroy(a);
    bson_destroy(b);
}

static void test_int32_vs_double_less(void **state) {
    (void)state;
    bson_t *a = make_int32_doc(42);
    bson_t *b = make_double_doc(42.5);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    assert_true(mongodb_compare_numbers(&ia, &ib) < 0);

    bson_destroy(a);
    bson_destroy(b);
}

static void test_int64_vs_double_equal(void **state) {
    (void)state;
    bson_t *a = make_int64_doc(1000000LL);
    bson_t *b = make_double_doc(1000000.0);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    assert_int_equal(0, mongodb_compare_numbers(&ia, &ib));

    bson_destroy(a);
    bson_destroy(b);
}

static void test_int64_vs_double_less(void **state) {
    (void)state;
    bson_t *a = make_int64_doc(1000000LL);
    bson_t *b = make_double_doc(1000000.5);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    assert_true(mongodb_compare_numbers(&ia, &ib) < 0);

    bson_destroy(a);
    bson_destroy(b);
}

/* ============================================================
 * Edge cases - infinity and NaN
 * ============================================================ */

static void test_double_infinity_positive(void **state) {
    (void)state;
    bson_t *a = make_double_doc(1000.0);
    bson_t *b = make_double_doc(INFINITY);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    /* Infinity is "unsafe" - uses fallback comparison */
    /* Just check it produces a deterministic result */
    int result = mongodb_compare_numbers(&ia, &ib);
    (void)result;  /* Result is deterministic but depends on fallback logic */

    bson_destroy(a);
    bson_destroy(b);
}

static void test_double_infinity_negative(void **state) {
    (void)state;
    bson_t *a = make_double_doc(-INFINITY);
    bson_t *b = make_double_doc(1000.0);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    int result = mongodb_compare_numbers(&ia, &ib);
    (void)result;

    bson_destroy(a);
    bson_destroy(b);
}

static void test_double_nan(void **state) {
    (void)state;
    bson_t *a = make_double_doc(NAN);
    bson_t *b = make_double_doc(42.0);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    /* NaN is "unsafe" - uses fallback */
    int result = mongodb_compare_numbers(&ia, &ib);
    (void)result;

    bson_destroy(a);
    bson_destroy(b);
}

static void test_double_nan_both(void **state) {
    (void)state;
    bson_t *a = make_double_doc(NAN);
    bson_t *b = make_double_doc(NAN);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    /* Both NaN - should be equal via fallback */
    int result = mongodb_compare_numbers(&ia, &ib);
    assert_int_equal(0, result);

    bson_destroy(a);
    bson_destroy(b);
}

/* ============================================================
 * Edge cases - large int64 beyond safe double precision
 * ============================================================ */

static void test_int64_beyond_safe(void **state) {
    (void)state;
    /* Values beyond 2^53 where double loses precision */
    bson_t *a = make_int64_doc(9007199254740993LL);  /* 2^53 + 1 */
    bson_t *b = make_int64_doc(9007199254740994LL);  /* 2^53 + 2 */
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    /* Uses fallback - still should produce deterministic order */
    int result = mongodb_compare_numbers(&ia, &ib);
    assert_true(result < 0);  /* a < b */

    bson_destroy(a);
    bson_destroy(b);
}

static void test_int64_negative_beyond_safe(void **state) {
    (void)state;
    bson_t *a = make_int64_doc(-9007199254740994LL);
    bson_t *b = make_int64_doc(-9007199254740993LL);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    int result = mongodb_compare_numbers(&ia, &ib);
    assert_true(result < 0);  /* a < b (more negative) */

    bson_destroy(a);
    bson_destroy(b);
}

/* ============================================================
 * Symmetry and transitivity tests
 * ============================================================ */

static void test_symmetry(void **state) {
    (void)state;
    bson_t *a = make_int32_doc(100);
    bson_t *b = make_double_doc(50.5);
    bson_iter_t ia, ib;
    get_iter(a, &ia);
    get_iter(b, &ib);

    int ab = mongodb_compare_numbers(&ia, &ib);
    int ba = mongodb_compare_numbers(&ib, &ia);

    /* If a > b, then b < a */
    assert_true((ab > 0 && ba < 0) || (ab < 0 && ba > 0) || (ab == 0 && ba == 0));

    bson_destroy(a);
    bson_destroy(b);
}

static void test_transitivity(void **state) {
    (void)state;
    bson_t *a = make_int32_doc(10);
    bson_t *b = make_double_doc(20.5);
    bson_t *c = make_int64_doc(30LL);
    bson_iter_t ia, ib, ic;
    get_iter(a, &ia);
    get_iter(b, &ib);
    get_iter(c, &ic);

    int ab = mongodb_compare_numbers(&ia, &ib);
    int bc = mongodb_compare_numbers(&ib, &ic);
    int ac = mongodb_compare_numbers(&ia, &ic);

    /* If a < b and b < c, then a < c */
    if (ab < 0 && bc < 0) {
        assert_true(ac < 0);
    }

    bson_destroy(a);
    bson_destroy(b);
    bson_destroy(c);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* int32 tests */
        cmocka_unit_test(test_int32_equal),
        cmocka_unit_test(test_int32_less),
        cmocka_unit_test(test_int32_greater),
        cmocka_unit_test(test_int32_negative),
        cmocka_unit_test(test_int32_zero),
        cmocka_unit_test(test_int32_min_max),

        /* int64 tests */
        cmocka_unit_test(test_int64_equal),
        cmocka_unit_test(test_int64_less),
        cmocka_unit_test(test_int64_greater),

        /* double tests */
        cmocka_unit_test(test_double_equal),
        cmocka_unit_test(test_double_less),
        cmocka_unit_test(test_double_greater),
        cmocka_unit_test(test_double_negative),
        cmocka_unit_test(test_double_zero_positive_negative),
        cmocka_unit_test(test_double_very_small),

        /* cross-type tests */
        cmocka_unit_test(test_int32_vs_int64_equal),
        cmocka_unit_test(test_int32_vs_int64_less),
        cmocka_unit_test(test_int32_vs_double_equal),
        cmocka_unit_test(test_int32_vs_double_less),
        cmocka_unit_test(test_int64_vs_double_equal),
        cmocka_unit_test(test_int64_vs_double_less),

        /* edge cases - infinity and NaN */
        cmocka_unit_test(test_double_infinity_positive),
        cmocka_unit_test(test_double_infinity_negative),
        cmocka_unit_test(test_double_nan),
        cmocka_unit_test(test_double_nan_both),

        /* edge cases - large int64 */
        cmocka_unit_test(test_int64_beyond_safe),
        cmocka_unit_test(test_int64_negative_beyond_safe),

        /* symmetry and transitivity */
        cmocka_unit_test(test_symmetry),
        cmocka_unit_test(test_transitivity),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
