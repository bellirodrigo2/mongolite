// test_key_compare.c - Unit tests for key_compare module (cmocka)

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <math.h>
#include <float.h>
#include <bson/bson.h>

#include "key_compare.h"

/* ============================================================
 * HELPERS
 * ============================================================ */

static bson_t *make_doc_int32(const char *key, int32_t val) {
    bson_t *doc = bson_new();
    BSON_APPEND_INT32(doc, key, val);
    return doc;
}

static bson_t *make_doc_int64(const char *key, int64_t val) {
    bson_t *doc = bson_new();
    BSON_APPEND_INT64(doc, key, val);
    return doc;
}

static bson_t *make_doc_double(const char *key, double val) {
    bson_t *doc = bson_new();
    BSON_APPEND_DOUBLE(doc, key, val);
    return doc;
}

static bson_t *make_doc_utf8(const char *key, const char *val) {
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, key, val);
    return doc;
}

static bson_t *make_doc_bool(const char *key, bool val) {
    bson_t *doc = bson_new();
    BSON_APPEND_BOOL(doc, key, val);
    return doc;
}

static bson_t *make_doc_null(const char *key) {
    bson_t *doc = bson_new();
    BSON_APPEND_NULL(doc, key);
    return doc;
}

static bson_t *make_doc_minkey(const char *key) {
    bson_t *doc = bson_new();
    BSON_APPEND_MINKEY(doc, key);
    return doc;
}

static bson_t *make_doc_maxkey(const char *key) {
    bson_t *doc = bson_new();
    BSON_APPEND_MAXKEY(doc, key);
    return doc;
}

/* ============================================================
 * TESTS: Type Precedence
 * ============================================================ */

static void test_type_minkey_less_than_null(void **state) {
    (void)state;
    bson_t *a = make_doc_minkey("x");
    bson_t *b = make_doc_null("x");
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_type_null_less_than_number(void **state) {
    (void)state;
    bson_t *a = make_doc_null("x");
    bson_t *b = make_doc_int32("x", 0);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_type_number_less_than_string(void **state) {
    (void)state;
    bson_t *a = make_doc_int32("x", 999);
    bson_t *b = make_doc_utf8("x", "a");
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_type_bool_less_than_datetime(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_BOOL(a, "x", true);
    BSON_APPEND_DATE_TIME(b, "x", 0);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_type_datetime_less_than_timestamp(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_DATE_TIME(a, "x", 9999999999999LL);
    BSON_APPEND_TIMESTAMP(b, "x", 0, 0);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

/* ============================================================
 * TESTS: Numeric Comparison - Same Type
 * ============================================================ */

static void test_int32_equal(void **state) {
    (void)state;
    bson_t *a = make_doc_int32("n", 42);
    bson_t *b = make_doc_int32("n", 42);
    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

static void test_int32_less(void **state) {
    (void)state;
    bson_t *a = make_doc_int32("n", 10);
    bson_t *b = make_doc_int32("n", 20);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_int32_greater(void **state) {
    (void)state;
    bson_t *a = make_doc_int32("n", 100);
    bson_t *b = make_doc_int32("n", 50);
    assert_true(bson_compare_docs(a, b) > 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_int64_equal(void **state) {
    (void)state;
    bson_t *a = make_doc_int64("n", 1000000000000LL);
    bson_t *b = make_doc_int64("n", 1000000000000LL);
    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

static void test_int64_less(void **state) {
    (void)state;
    bson_t *a = make_doc_int64("n", 999999999999LL);
    bson_t *b = make_doc_int64("n", 1000000000000LL);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_double_equal(void **state) {
    (void)state;
    bson_t *a = make_doc_double("n", 3.14159);
    bson_t *b = make_doc_double("n", 3.14159);
    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

static void test_double_less(void **state) {
    (void)state;
    bson_t *a = make_doc_double("n", 3.14);
    bson_t *b = make_doc_double("n", 3.15);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_decimal128_less(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    bson_decimal128_t dec_a, dec_b;
    bson_decimal128_from_string("123.456", &dec_a);
    bson_decimal128_from_string("123.457", &dec_b);
    BSON_APPEND_DECIMAL128(a, "n", &dec_a);
    BSON_APPEND_DECIMAL128(b, "n", &dec_b);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

/* ============================================================
 * TESTS: Strings
 * ============================================================ */

static void test_strings_equal(void **state) {
    (void)state;
    bson_t *a = make_doc_utf8("s", "hello");
    bson_t *b = make_doc_utf8("s", "hello");
    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

static void test_strings_lexicographic(void **state) {
    (void)state;
    bson_t *a = make_doc_utf8("s", "abc");
    bson_t *b = make_doc_utf8("s", "abd");
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_strings_prefix_shorter_is_less(void **state) {
    (void)state;
    bson_t *a = make_doc_utf8("s", "abc");
    bson_t *b = make_doc_utf8("s", "abcd");
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_strings_empty(void **state) {
    (void)state;
    bson_t *a = make_doc_utf8("s", "");
    bson_t *b = make_doc_utf8("s", "a");
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

/* ============================================================
 * TESTS: Boolean
 * ============================================================ */

static void test_bool_false_less_than_true(void **state) {
    (void)state;
    bson_t *a = make_doc_bool("b", false);
    bson_t *b = make_doc_bool("b", true);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_bool_equal_true(void **state) {
    (void)state;
    bson_t *a = make_doc_bool("b", true);
    bson_t *b = make_doc_bool("b", true);
    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

static void test_bool_equal_false(void **state) {
    (void)state;
    bson_t *a = make_doc_bool("b", false);
    bson_t *b = make_doc_bool("b", false);
    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

/* ============================================================
 * TESTS: ObjectId
 * ============================================================ */

static void test_oid_less(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    bson_oid_t oid_a, oid_b;
    bson_oid_init_from_string(&oid_a, "000000000000000000000001");
    bson_oid_init_from_string(&oid_b, "000000000000000000000002");
    BSON_APPEND_OID(a, "id", &oid_a);
    BSON_APPEND_OID(b, "id", &oid_b);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_oid_equal(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    bson_oid_t oid;
    bson_oid_init_from_string(&oid, "507f1f77bcf86cd799439011");
    BSON_APPEND_OID(a, "id", &oid);
    BSON_APPEND_OID(b, "id", &oid);
    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

/* ============================================================
 * TESTS: DateTime
 * ============================================================ */

static void test_datetime_less(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_DATE_TIME(a, "d", 1000);
    BSON_APPEND_DATE_TIME(b, "d", 2000);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_datetime_equal(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_DATE_TIME(a, "d", 1702300800000LL);
    BSON_APPEND_DATE_TIME(b, "d", 1702300800000LL);
    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

/* ============================================================
 * TESTS: Timestamp
 * ============================================================ */

static void test_timestamp_by_ts(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_TIMESTAMP(a, "t", 100, 1);
    BSON_APPEND_TIMESTAMP(b, "t", 200, 1);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_timestamp_by_inc(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_TIMESTAMP(a, "t", 100, 1);
    BSON_APPEND_TIMESTAMP(b, "t", 100, 2);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_timestamp_equal(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_TIMESTAMP(a, "t", 100, 5);
    BSON_APPEND_TIMESTAMP(b, "t", 100, 5);
    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

/* ============================================================
 * TESTS: Binary
 * ============================================================ */

static void test_binary_by_length(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    uint8_t data_a[] = {1, 2};
    uint8_t data_b[] = {1, 2, 3};
    BSON_APPEND_BINARY(a, "bin", BSON_SUBTYPE_BINARY, data_a, 2);
    BSON_APPEND_BINARY(b, "bin", BSON_SUBTYPE_BINARY, data_b, 3);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_binary_by_subtype(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    uint8_t data[] = {1, 2, 3};
    BSON_APPEND_BINARY(a, "bin", BSON_SUBTYPE_BINARY, data, 3);
    BSON_APPEND_BINARY(b, "bin", BSON_SUBTYPE_UUID, data, 3);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_binary_by_content(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    uint8_t data_a[] = {1, 2, 3};
    uint8_t data_b[] = {1, 2, 4};
    BSON_APPEND_BINARY(a, "bin", BSON_SUBTYPE_BINARY, data_a, 3);
    BSON_APPEND_BINARY(b, "bin", BSON_SUBTYPE_BINARY, data_b, 3);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

/* ============================================================
 * TESTS: Regex
 * ============================================================ */

static void test_regex_by_pattern(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_REGEX(a, "r", "abc", "i");
    BSON_APPEND_REGEX(b, "r", "abd", "i");
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_regex_by_options(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_REGEX(a, "r", "abc", "i");
    BSON_APPEND_REGEX(b, "r", "abc", "m");
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_regex_equal(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_REGEX(a, "r", "^test$", "im");
    BSON_APPEND_REGEX(b, "r", "^test$", "im");
    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

/* ============================================================
 * TESTS: Nested Documents
 * ============================================================ */

static void test_nested_doc_less(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    bson_t child_a, child_b;

    bson_append_document_begin(a, "nested", -1, &child_a);
    BSON_APPEND_INT32(&child_a, "x", 1);
    bson_append_document_end(a, &child_a);

    bson_append_document_begin(b, "nested", -1, &child_b);
    BSON_APPEND_INT32(&child_b, "x", 2);
    bson_append_document_end(b, &child_b);

    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_nested_doc_equal(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    bson_t child_a, child_b;

    bson_append_document_begin(a, "nested", -1, &child_a);
    BSON_APPEND_UTF8(&child_a, "name", "test");
    BSON_APPEND_INT32(&child_a, "val", 42);
    bson_append_document_end(a, &child_a);

    bson_append_document_begin(b, "nested", -1, &child_b);
    BSON_APPEND_UTF8(&child_b, "name", "test");
    BSON_APPEND_INT32(&child_b, "val", 42);
    bson_append_document_end(b, &child_b);

    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

/* ============================================================
 * TESTS: Keys and Doc Size
 * ============================================================ */

static void test_key_order_matters(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_INT32(a, "aaa", 1);
    BSON_APPEND_INT32(b, "bbb", 1);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_more_fields_is_greater(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_INT32(a, "x", 1);
    BSON_APPEND_INT32(b, "x", 1);
    BSON_APPEND_INT32(b, "y", 2);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_empty_docs_equal(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

/* ============================================================
 * TESTS: MinKey/MaxKey/Null Equality
 * ============================================================ */

static void test_minkey_equal(void **state) {
    (void)state;
    bson_t *a = make_doc_minkey("x");
    bson_t *b = make_doc_minkey("x");
    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

static void test_maxkey_equal(void **state) {
    (void)state;
    bson_t *a = make_doc_maxkey("x");
    bson_t *b = make_doc_maxkey("x");
    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

static void test_null_equal(void **state) {
    (void)state;
    bson_t *a = make_doc_null("x");
    bson_t *b = make_doc_null("x");
    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

/* ============================================================
 * TESTS: Multiple Fields
 * ============================================================ */

static void test_multi_field_first_differs(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_INT32(a, "a", 1);
    BSON_APPEND_INT32(a, "b", 100);
    BSON_APPEND_INT32(b, "a", 2);
    BSON_APPEND_INT32(b, "b", 1);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_multi_field_second_differs(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_INT32(a, "a", 1);
    BSON_APPEND_INT32(a, "b", 10);
    BSON_APPEND_INT32(b, "a", 1);
    BSON_APPEND_INT32(b, "b", 20);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

/* ============================================================
 * TESTS: bson_extract_index_key
 * ============================================================ */

static bool bson_docs_equal(const bson_t *a, const bson_t *b) {
    if (!a || !b) return a == b;
    if (a->len != b->len) return false;
    return memcmp(bson_get_data(a), bson_get_data(b), a->len) == 0;
}

static bson_t *make_keys_1(const char *f1) {
    bson_t *keys = bson_new();
    BSON_APPEND_INT32(keys, f1, 1);
    return keys;
}

static bson_t *make_keys_2(const char *f1, const char *f2) {
    bson_t *keys = bson_new();
    BSON_APPEND_INT32(keys, f1, 1);
    BSON_APPEND_INT32(keys, f2, 1);
    return keys;
}

static bson_t *make_keys_3(const char *f1, const char *f2, const char *f3) {
    bson_t *keys = bson_new();
    BSON_APPEND_INT32(keys, f1, 1);
    BSON_APPEND_INT32(keys, f2, 1);
    BSON_APPEND_INT32(keys, f3, 1);
    return keys;
}

static void test_extract_null_doc(void **state) {
    (void)state;
    bson_t *keys = make_keys_1("name");
    bson_t *result = bson_extract_index_key(NULL, keys);
    assert_null(result);
    bson_destroy(keys);
}

static void test_extract_null_keys(void **state) {
    (void)state;
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "test");
    bson_t *result = bson_extract_index_key(doc, NULL);
    assert_null(result);
    bson_destroy(doc);
}

static void test_extract_single_field_string(void **state) {
    (void)state;
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "Alice");
    BSON_APPEND_INT32(doc, "age", 30);
    BSON_APPEND_UTF8(doc, "city", "NYC");

    bson_t *keys = make_keys_1("name");
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    bson_t *expected = bson_new();
    BSON_APPEND_UTF8(expected, "name", "Alice");

    assert_true(bson_docs_equal(result, expected));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    bson_destroy(expected);
}

static void test_extract_single_field_int32(void **state) {
    (void)state;
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "Bob");
    BSON_APPEND_INT32(doc, "age", 25);

    bson_t *keys = make_keys_1("age");
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    bson_t *expected = bson_new();
    BSON_APPEND_INT32(expected, "age", 25);

    assert_true(bson_docs_equal(result, expected));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    bson_destroy(expected);
}

static void test_extract_multiple_fields(void **state) {
    (void)state;
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "Charlie");
    BSON_APPEND_INT32(doc, "age", 35);
    BSON_APPEND_UTF8(doc, "city", "LA");
    BSON_APPEND_DOUBLE(doc, "score", 95.5);

    bson_t *keys = make_keys_2("name", "age");
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    bson_t *expected = bson_new();
    BSON_APPEND_UTF8(expected, "name", "Charlie");
    BSON_APPEND_INT32(expected, "age", 35);

    assert_true(bson_docs_equal(result, expected));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    bson_destroy(expected);
}

static void test_extract_missing_field_becomes_null(void **state) {
    (void)state;
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "Dave");

    bson_t *keys = make_keys_1("nonexistent");
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    bson_t *expected = bson_new();
    BSON_APPEND_NULL(expected, "nonexistent");

    assert_true(bson_docs_equal(result, expected));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    bson_destroy(expected);
}

static void test_extract_dotted_field(void **state) {
    (void)state;
    bson_t *doc = bson_new();
    bson_t address;

    BSON_APPEND_UTF8(doc, "name", "Frank");
    bson_append_document_begin(doc, "address", -1, &address);
    BSON_APPEND_UTF8(&address, "city", "Boston");
    BSON_APPEND_UTF8(&address, "zip", "02101");
    bson_append_document_end(doc, &address);

    bson_t *keys = make_keys_1("address.city");
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, result, "address.city"));
    assert_int_equal(BSON_TYPE_UTF8, bson_iter_type(&iter));
    assert_string_equal("Boston", bson_iter_utf8(&iter, NULL));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
}

static void test_extract_empty_doc(void **state) {
    (void)state;
    bson_t *doc = bson_new();
    bson_t *keys = make_keys_2("name", "age");
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    bson_t *expected = bson_new();
    BSON_APPEND_NULL(expected, "name");
    BSON_APPEND_NULL(expected, "age");

    assert_true(bson_docs_equal(result, expected));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    bson_destroy(expected);
}

static void test_extract_preserves_key_order(void **state) {
    (void)state;
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "z", "last");
    BSON_APPEND_UTF8(doc, "a", "first");
    BSON_APPEND_UTF8(doc, "m", "middle");

    bson_t *keys = make_keys_3("m", "z", "a");
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    bson_iter_t iter;
    assert_true(bson_iter_init(&iter, result));

    assert_true(bson_iter_next(&iter));
    assert_string_equal("m", bson_iter_key(&iter));

    assert_true(bson_iter_next(&iter));
    assert_string_equal("z", bson_iter_key(&iter));

    assert_true(bson_iter_next(&iter));
    assert_string_equal("a", bson_iter_key(&iter));

    assert_false(bson_iter_next(&iter));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
}

static void test_array_simple_less(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    bson_t arr_a, arr_b;

    bson_append_array_begin(a, "x", -1, &arr_a);
    BSON_APPEND_INT32(&arr_a, "0", 1);
    bson_append_array_end(a, &arr_a);

    bson_append_array_begin(b, "x", -1, &arr_b);
    BSON_APPEND_INT32(&arr_b, "0", 2);
    bson_append_array_end(b, &arr_b);

    assert_true(bson_compare_docs(a, b) < 0);

    bson_destroy(a);
    bson_destroy(b);
}

static void test_array_length_matters(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    bson_t arr;

    bson_append_array_begin(a, "x", -1, &arr);
    BSON_APPEND_INT32(&arr, "0", 1);
    bson_append_array_end(a, &arr);

    bson_append_array_begin(b, "x", -1, &arr);
    BSON_APPEND_INT32(&arr, "0", 1);
    BSON_APPEND_INT32(&arr, "1", 2);
    bson_append_array_end(b, &arr);

    assert_true(bson_compare_docs(a, b) < 0);

    bson_destroy(a);
    bson_destroy(b);
}

/* ============================================================
 * TESTS: Advanced Numeric Comparison (edge cases)
 * ============================================================ */

static void test_int32_negative(void **state) {
    (void)state;
    bson_t *a = make_doc_int32("n", -100);
    bson_t *b = make_doc_int32("n", 100);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_int32_zero(void **state) {
    (void)state;
    bson_t *a = make_doc_int32("n", 0);
    bson_t *b = make_doc_int32("n", 0);
    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

static void test_int32_min_max(void **state) {
    (void)state;
    bson_t *a = make_doc_int32("n", INT32_MIN);
    bson_t *b = make_doc_int32("n", INT32_MAX);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_int64_greater(void **state) {
    (void)state;
    bson_t *a = make_doc_int64("n", 9007199254740992LL);
    bson_t *b = make_doc_int64("n", 100LL);
    assert_true(bson_compare_docs(a, b) > 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_double_greater(void **state) {
    (void)state;
    bson_t *a = make_doc_double("n", 9.99);
    bson_t *b = make_doc_double("n", 1.11);
    assert_true(bson_compare_docs(a, b) > 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_double_negative(void **state) {
    (void)state;
    bson_t *a = make_doc_double("n", -1.5);
    bson_t *b = make_doc_double("n", 1.5);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_double_zero_positive_negative(void **state) {
    (void)state;
    bson_t *a = make_doc_double("n", -0.0);
    bson_t *b = make_doc_double("n", 0.0);
    /* -0.0 and +0.0 should be equal */
    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

static void test_double_very_small(void **state) {
    (void)state;
    bson_t *a = make_doc_double("n", DBL_MIN);
    bson_t *b = make_doc_double("n", DBL_MIN * 2);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

/* Cross-type numeric comparison */

static void test_int32_vs_int64_equal(void **state) {
    (void)state;
    bson_t *a = make_doc_int32("n", 42);
    bson_t *b = make_doc_int64("n", 42LL);
    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

static void test_int32_vs_int64_less(void **state) {
    (void)state;
    bson_t *a = make_doc_int32("n", 100);
    bson_t *b = make_doc_int64("n", 1000000000000LL);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_int32_vs_double_equal(void **state) {
    (void)state;
    bson_t *a = make_doc_int32("n", 42);
    bson_t *b = make_doc_double("n", 42.0);
    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

static void test_int32_vs_double_less(void **state) {
    (void)state;
    bson_t *a = make_doc_int32("n", 42);
    bson_t *b = make_doc_double("n", 42.5);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_int64_vs_double_equal(void **state) {
    (void)state;
    bson_t *a = make_doc_int64("n", 1000000LL);
    bson_t *b = make_doc_double("n", 1000000.0);
    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

static void test_int64_vs_double_less(void **state) {
    (void)state;
    bson_t *a = make_doc_int64("n", 1000000LL);
    bson_t *b = make_doc_double("n", 1000000.5);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

/* Infinity and NaN edge cases */

static void test_double_infinity_positive(void **state) {
    (void)state;
    bson_t *a = make_doc_double("n", 1000.0);
    bson_t *b = make_doc_double("n", INFINITY);
    /* Normal value < Infinity */
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_double_infinity_negative(void **state) {
    (void)state;
    bson_t *a = make_doc_double("n", -INFINITY);
    bson_t *b = make_doc_double("n", 1000.0);
    /* -Infinity < normal value */
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_double_nan(void **state) {
    (void)state;
    bson_t *a = make_doc_double("n", NAN);
    bson_t *b = make_doc_double("n", 42.0);
    /* NaN is "less" than normal values in fallback ordering */
    int result = bson_compare_docs(a, b);
    assert_true(result < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_double_nan_both(void **state) {
    (void)state;
    bson_t *a = make_doc_double("n", NAN);
    bson_t *b = make_doc_double("n", NAN);
    /* Both NaN should be equal */
    assert_int_equal(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
}

/* Large int64 beyond safe double precision */

static void test_int64_beyond_safe(void **state) {
    (void)state;
    /* Values beyond 2^53 where double loses precision */
    bson_t *a = make_doc_int64("n", 9007199254740993LL);  /* 2^53 + 1 */
    bson_t *b = make_doc_int64("n", 9007199254740994LL);  /* 2^53 + 2 */
    /* Uses fallback - still produces deterministic order */
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

static void test_int64_negative_beyond_safe(void **state) {
    (void)state;
    bson_t *a = make_doc_int64("n", -9007199254740994LL);
    bson_t *b = make_doc_int64("n", -9007199254740993LL);
    assert_true(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
}

/* Symmetry and transitivity tests */

static void test_numeric_symmetry(void **state) {
    (void)state;
    bson_t *a = make_doc_int32("n", 100);
    bson_t *b = make_doc_double("n", 50.5);

    int ab = bson_compare_docs(a, b);
    int ba = bson_compare_docs(b, a);

    /* If a > b, then b < a */
    assert_true((ab > 0 && ba < 0) || (ab < 0 && ba > 0) || (ab == 0 && ba == 0));

    bson_destroy(a); bson_destroy(b);
}

static void test_numeric_transitivity(void **state) {
    (void)state;
    bson_t *a = make_doc_int32("n", 10);
    bson_t *b = make_doc_double("n", 20.5);
    bson_t *c = make_doc_int64("n", 30LL);

    int ab = bson_compare_docs(a, b);
    int bc = bson_compare_docs(b, c);
    int ac = bson_compare_docs(a, c);

    /* If a < b and b < c, then a < c */
    if (ab < 0 && bc < 0) {
        assert_true(ac < 0);
    }

    bson_destroy(a); bson_destroy(b); bson_destroy(c);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        // Type precedence
        cmocka_unit_test(test_type_minkey_less_than_null),
        cmocka_unit_test(test_type_null_less_than_number),
        cmocka_unit_test(test_type_number_less_than_string),
        cmocka_unit_test(test_type_bool_less_than_datetime),
        cmocka_unit_test(test_type_datetime_less_than_timestamp),
        // Numeric - same type
        cmocka_unit_test(test_int32_equal),
        cmocka_unit_test(test_int32_less),
        cmocka_unit_test(test_int32_greater),
        cmocka_unit_test(test_int64_equal),
        cmocka_unit_test(test_int64_less),
        cmocka_unit_test(test_double_equal),
        cmocka_unit_test(test_double_less),
        cmocka_unit_test(test_decimal128_less),
        // Strings
        cmocka_unit_test(test_strings_equal),
        cmocka_unit_test(test_strings_lexicographic),
        cmocka_unit_test(test_strings_prefix_shorter_is_less),
        cmocka_unit_test(test_strings_empty),
        // Boolean
        cmocka_unit_test(test_bool_false_less_than_true),
        cmocka_unit_test(test_bool_equal_true),
        cmocka_unit_test(test_bool_equal_false),
        // ObjectId
        cmocka_unit_test(test_oid_less),
        cmocka_unit_test(test_oid_equal),
        // DateTime
        cmocka_unit_test(test_datetime_less),
        cmocka_unit_test(test_datetime_equal),
        // Timestamp
        cmocka_unit_test(test_timestamp_by_ts),
        cmocka_unit_test(test_timestamp_by_inc),
        cmocka_unit_test(test_timestamp_equal),
        // Binary
        cmocka_unit_test(test_binary_by_length),
        cmocka_unit_test(test_binary_by_subtype),
        cmocka_unit_test(test_binary_by_content),
        // Regex
        cmocka_unit_test(test_regex_by_pattern),
        cmocka_unit_test(test_regex_by_options),
        cmocka_unit_test(test_regex_equal),
        // Nested documents
        cmocka_unit_test(test_nested_doc_less),
        cmocka_unit_test(test_nested_doc_equal),
        // Keys and doc size
        cmocka_unit_test(test_key_order_matters),
        cmocka_unit_test(test_more_fields_is_greater),
        cmocka_unit_test(test_empty_docs_equal),
        // MinKey/MaxKey/Null
        cmocka_unit_test(test_minkey_equal),
        cmocka_unit_test(test_maxkey_equal),
        cmocka_unit_test(test_null_equal),
        // Multiple fields
        cmocka_unit_test(test_multi_field_first_differs),
        cmocka_unit_test(test_multi_field_second_differs),
        // Extract index key
        cmocka_unit_test(test_extract_null_doc),
        cmocka_unit_test(test_extract_null_keys),
        cmocka_unit_test(test_extract_single_field_string),
        cmocka_unit_test(test_extract_single_field_int32),
        cmocka_unit_test(test_extract_multiple_fields),
        cmocka_unit_test(test_extract_missing_field_becomes_null),
        cmocka_unit_test(test_extract_dotted_field),
        cmocka_unit_test(test_extract_empty_doc),
        cmocka_unit_test(test_extract_preserves_key_order),
        // Arrays
        cmocka_unit_test(test_array_simple_less),
        cmocka_unit_test(test_array_length_matters),
        // Advanced numeric comparison (edge cases)
        cmocka_unit_test(test_int32_negative),
        cmocka_unit_test(test_int32_zero),
        cmocka_unit_test(test_int32_min_max),
        cmocka_unit_test(test_int64_greater),
        cmocka_unit_test(test_double_greater),
        cmocka_unit_test(test_double_negative),
        cmocka_unit_test(test_double_zero_positive_negative),
        cmocka_unit_test(test_double_very_small),
        // Cross-type numeric comparison
        cmocka_unit_test(test_int32_vs_int64_equal),
        cmocka_unit_test(test_int32_vs_int64_less),
        cmocka_unit_test(test_int32_vs_double_equal),
        cmocka_unit_test(test_int32_vs_double_less),
        cmocka_unit_test(test_int64_vs_double_equal),
        cmocka_unit_test(test_int64_vs_double_less),
        // Infinity and NaN edge cases
        cmocka_unit_test(test_double_infinity_positive),
        cmocka_unit_test(test_double_infinity_negative),
        cmocka_unit_test(test_double_nan),
        cmocka_unit_test(test_double_nan_both),
        // Large int64 beyond safe double precision
        cmocka_unit_test(test_int64_beyond_safe),
        cmocka_unit_test(test_int64_negative_beyond_safe),
        // Symmetry and transitivity
        cmocka_unit_test(test_numeric_symmetry),
        cmocka_unit_test(test_numeric_transitivity),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
