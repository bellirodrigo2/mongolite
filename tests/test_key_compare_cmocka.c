// test_key_compare_cmocka.c - Unit tests for key comparison using CMocka

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <stdint.h>
#include <cmocka.h>
#include <string.h>
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

/* Helper: verifica se dois BSON são iguais (byte a byte) */
static bool bson_docs_equal(const bson_t *a, const bson_t *b) {
    if (!a || !b) return a == b;
    if (a->len != b->len) return false;
    return memcmp(bson_get_data(a), bson_get_data(b), a->len) == 0;
}

/* Helper: cria keys spec {"field": 1} */
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

/* ============================================================
 * TESTES: PRECEDÊNCIA DE TIPOS
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
 * TESTES: COMPARAÇÃO NUMÉRICA - MESMO TIPO
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
 * TESTES: STRINGS
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
 * TESTES: BOOLEAN
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
 * TESTES: OBJECTID
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
 * TESTES: DATE_TIME
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
 * TESTES: TIMESTAMP
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
 * TESTES: BINARY
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
 * TESTES: REGEX
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
 * TESTES: DOCUMENTOS ANINHADOS
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
 * TESTES: CHAVES E TAMANHO DE DOC
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
 * TESTES: MINKEY/MAXKEY/NULL EQUALITY
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
 * TESTES: MÚLTIPLOS CAMPOS
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
 * TESTES: bson_extract_index_key
 * ============================================================ */

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

static void test_extract_three_fields(void **state) {
    (void)state;
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "a", "val_a");
    BSON_APPEND_INT32(doc, "b", 100);
    BSON_APPEND_DOUBLE(doc, "c", 3.14);
    BSON_APPEND_BOOL(doc, "d", true);

    bson_t *keys = make_keys_3("a", "b", "c");
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    bson_t *expected = bson_new();
    BSON_APPEND_UTF8(expected, "a", "val_a");
    BSON_APPEND_INT32(expected, "b", 100);
    BSON_APPEND_DOUBLE(expected, "c", 3.14);

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

static void test_extract_partial_fields_exist(void **state) {
    (void)state;
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "Eve");
    BSON_APPEND_INT32(doc, "age", 28);

    bson_t *keys = make_keys_3("name", "missing", "age");
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    bson_t *expected = bson_new();
    BSON_APPEND_UTF8(expected, "name", "Eve");
    BSON_APPEND_NULL(expected, "missing");
    BSON_APPEND_INT32(expected, "age", 28);

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

    /* Resultado deve ter "address.city": "Boston" */
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, result, "address.city"));
    assert_int_equal(BSON_TYPE_UTF8, bson_iter_type(&iter));
    assert_string_equal("Boston", bson_iter_utf8(&iter, NULL));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
}

static void test_extract_dotted_missing(void **state) {
    (void)state;
    bson_t *doc = bson_new();
    bson_t address;

    BSON_APPEND_UTF8(doc, "name", "Grace");
    bson_append_document_begin(doc, "address", -1, &address);
    BSON_APPEND_UTF8(&address, "city", "Chicago");
    bson_append_document_end(doc, &address);

    bson_t *keys = make_keys_1("address.country");
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    /* Campo não existe, deve ser null */
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, result, "address.country"));
    assert_int_equal(BSON_TYPE_NULL, bson_iter_type(&iter));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
}

static void test_extract_deep_dotted(void **state) {
    (void)state;
    bson_t *doc = bson_new();
    bson_t level1, level2;

    bson_append_document_begin(doc, "a", -1, &level1);
    bson_append_document_begin(&level1, "b", -1, &level2);
    BSON_APPEND_INT32(&level2, "c", 42);
    bson_append_document_end(&level1, &level2);
    bson_append_document_end(doc, &level1);

    bson_t *keys = make_keys_1("a.b.c");
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, result, "a.b.c"));
    assert_int_equal(BSON_TYPE_INT32, bson_iter_type(&iter));
    assert_int_equal(42, bson_iter_int32(&iter));

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

static void test_extract_empty_keys(void **state) {
    (void)state;
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "Henry");

    bson_t *keys = bson_new(); /* empty */
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    bson_t *expected = bson_new(); /* empty */
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

    /* Keys em ordem diferente do doc */
    bson_t *keys = make_keys_3("m", "z", "a");
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    /* Verificar ordem: m, z, a */
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

static void test_extract_oid_field(void **state) {
    (void)state;
    bson_t *doc = bson_new();
    bson_oid_t oid;
    bson_oid_init_from_string(&oid, "507f1f77bcf86cd799439011");
    BSON_APPEND_OID(doc, "_id", &oid);
    BSON_APPEND_UTF8(doc, "name", "Ivy");

    bson_t *keys = make_keys_1("_id");
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, result, "_id"));
    assert_int_equal(BSON_TYPE_OID, bson_iter_type(&iter));

    const bson_oid_t *result_oid = bson_iter_oid(&iter);
    assert_int_equal(0, bson_oid_compare(&oid, result_oid));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
}

static void test_extract_bool_field(void **state) {
    (void)state;
    bson_t *doc = bson_new();
    BSON_APPEND_BOOL(doc, "active", true);
    BSON_APPEND_BOOL(doc, "verified", false);

    bson_t *keys = make_keys_2("active", "verified");
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, result, "active"));
    assert_true(bson_iter_bool(&iter));

    assert_true(bson_iter_init_find(&iter, result, "verified"));
    assert_false(bson_iter_bool(&iter));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
}

static void test_extract_datetime_field(void **state) {
    (void)state;
    bson_t *doc = bson_new();
    int64_t ts = 1702300800000LL;
    BSON_APPEND_DATE_TIME(doc, "created", ts);

    bson_t *keys = make_keys_1("created");
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, result, "created"));
    assert_int_equal(BSON_TYPE_DATE_TIME, bson_iter_type(&iter));
    assert_int_equal(ts, bson_iter_date_time(&iter));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
}

static void test_extract_double_field(void **state) {
    (void)state;
    bson_t *doc = bson_new();
    BSON_APPEND_DOUBLE(doc, "price", 19.99);
    BSON_APPEND_DOUBLE(doc, "tax", 1.50);

    bson_t *keys = make_keys_1("price");
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, result, "price"));
    assert_int_equal(BSON_TYPE_DOUBLE, bson_iter_type(&iter));
    /* Double comparison with tolerance */
    double diff = bson_iter_double(&iter) - 19.99;
    assert_true(diff < 0.001 && diff > -0.001);

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
}

static void test_extract_array_field(void **state) {
    (void)state;
    bson_t *doc = bson_new();
    bson_t arr;

    BSON_APPEND_UTF8(doc, "name", "Jack");
    bson_append_array_begin(doc, "tags", -1, &arr);
    BSON_APPEND_UTF8(&arr, "0", "red");
    BSON_APPEND_UTF8(&arr, "1", "blue");
    bson_append_array_end(doc, &arr);

    bson_t *keys = make_keys_1("tags");
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, result, "tags"));
    assert_int_equal(BSON_TYPE_ARRAY, bson_iter_type(&iter));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
}

static void test_extract_nested_doc_field(void **state) {
    (void)state;
    bson_t *doc = bson_new();
    bson_t nested;

    BSON_APPEND_UTF8(doc, "name", "Kate");
    bson_append_document_begin(doc, "meta", -1, &nested);
    BSON_APPEND_INT32(&nested, "version", 1);
    BSON_APPEND_BOOL(&nested, "active", true);
    bson_append_document_end(doc, &nested);

    bson_t *keys = make_keys_1("meta");
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, result, "meta"));
    assert_int_equal(BSON_TYPE_DOCUMENT, bson_iter_type(&iter));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
}

static void test_extract_binary_field(void **state) {
    (void)state;
    bson_t *doc = bson_new();
    uint8_t data[] = {0xDE, 0xAD, 0xBE, 0xEF};
    BSON_APPEND_BINARY(doc, "data", BSON_SUBTYPE_BINARY, data, 4);

    bson_t *keys = make_keys_1("data");
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, result, "data"));
    assert_int_equal(BSON_TYPE_BINARY, bson_iter_type(&iter));

    bson_subtype_t subtype;
    uint32_t len;
    const uint8_t *bin;
    bson_iter_binary(&iter, &subtype, &len, &bin);
    assert_int_equal(4, len);
    assert_memory_equal(data, bin, 4);

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
}

static void test_extract_compound_index_realistic(void **state) {
    (void)state;
    /* Simula índice composto: {lastName: 1, firstName: 1, age: 1} */
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "firstName", "John");
    BSON_APPEND_UTF8(doc, "lastName", "Doe");
    BSON_APPEND_INT32(doc, "age", 30);
    BSON_APPEND_UTF8(doc, "email", "john@example.com");
    BSON_APPEND_BOOL(doc, "active", true);

    bson_t *keys = make_keys_3("lastName", "firstName", "age");
    bson_t *result = bson_extract_index_key(doc, keys);

    assert_non_null(result);

    /* Verificar ordem e valores */
    bson_iter_t iter;
    assert_true(bson_iter_init(&iter, result));

    assert_true(bson_iter_next(&iter));
    assert_string_equal("lastName", bson_iter_key(&iter));
    assert_string_equal("Doe", bson_iter_utf8(&iter, NULL));

    assert_true(bson_iter_next(&iter));
    assert_string_equal("firstName", bson_iter_key(&iter));
    assert_string_equal("John", bson_iter_utf8(&iter, NULL));

    assert_true(bson_iter_next(&iter));
    assert_string_equal("age", bson_iter_key(&iter));
    assert_int_equal(30, bson_iter_int32(&iter));

    assert_false(bson_iter_next(&iter));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
}

static void test_single_field_doc_equal_strict(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();

    BSON_APPEND_INT32(a, "x", 1);
    BSON_APPEND_INT32(b, "x", 1);

    /* Deve ser exatamente igual */
    assert_int_equal(0, bson_compare_docs(a, b));

    bson_destroy(a);
    bson_destroy(b);
}

static void test_empty_vs_single_field(void **state) {
    (void)state;
    bson_t *empty = bson_new();
    bson_t *nonempty = bson_new();

    BSON_APPEND_INT32(nonempty, "x", 1);

    /* Empty deve ser menor */
    assert_true(bson_compare_docs(empty, nonempty) < 0);

    bson_destroy(empty);
    bson_destroy(nonempty);
}

static void test_first_field_must_decide(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();

    BSON_APPEND_INT32(a, "a", 1);
    BSON_APPEND_INT32(a, "z", 100);

    BSON_APPEND_INT32(b, "b", 0);
    BSON_APPEND_INT32(b, "a", 1);

    /* "a" < "b", então a < b */
    assert_true(bson_compare_docs(a, b) < 0);

    bson_destroy(a);
    bson_destroy(b);
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

static void test_nested_array_compare(void **state) {
    (void)state;
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    bson_t arr;

    bson_append_array_begin(a, "arr", -1, &arr);
    BSON_APPEND_UTF8(&arr, "0", "a");
    bson_append_array_end(a, &arr);

    bson_append_array_begin(b, "arr", -1, &arr);
    BSON_APPEND_UTF8(&arr, "0", "b");
    bson_append_array_end(b, &arr);

    assert_true(bson_compare_docs(a, b) < 0);

    bson_destroy(a);
    bson_destroy(b);
}

/* ============================================================
 * MAIN
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Precedência de tipos */
        cmocka_unit_test(test_type_minkey_less_than_null),
        cmocka_unit_test(test_type_null_less_than_number),
        cmocka_unit_test(test_type_number_less_than_string),
        cmocka_unit_test(test_type_bool_less_than_datetime),
        cmocka_unit_test(test_type_datetime_less_than_timestamp),

        /* Numéricos - mesmo tipo */
        cmocka_unit_test(test_int32_equal),
        cmocka_unit_test(test_int32_less),
        cmocka_unit_test(test_int32_greater),
        cmocka_unit_test(test_int64_equal),
        cmocka_unit_test(test_int64_less),
        cmocka_unit_test(test_double_equal),
        cmocka_unit_test(test_double_less),
        cmocka_unit_test(test_decimal128_less),

        /* Strings */
        cmocka_unit_test(test_strings_equal),
        cmocka_unit_test(test_strings_lexicographic),
        cmocka_unit_test(test_strings_prefix_shorter_is_less),
        cmocka_unit_test(test_strings_empty),

        /* Boolean */
        cmocka_unit_test(test_bool_false_less_than_true),
        cmocka_unit_test(test_bool_equal_true),
        cmocka_unit_test(test_bool_equal_false),

        /* ObjectId */
        cmocka_unit_test(test_oid_less),
        cmocka_unit_test(test_oid_equal),

        /* DateTime */
        cmocka_unit_test(test_datetime_less),
        cmocka_unit_test(test_datetime_equal),

        /* Timestamp */
        cmocka_unit_test(test_timestamp_by_ts),
        cmocka_unit_test(test_timestamp_by_inc),
        cmocka_unit_test(test_timestamp_equal),

        /* Binary */
        cmocka_unit_test(test_binary_by_length),
        cmocka_unit_test(test_binary_by_subtype),
        cmocka_unit_test(test_binary_by_content),

        /* Regex */
        cmocka_unit_test(test_regex_by_pattern),
        cmocka_unit_test(test_regex_by_options),
        cmocka_unit_test(test_regex_equal),

        /* Documentos aninhados */
        cmocka_unit_test(test_nested_doc_less),
        cmocka_unit_test(test_nested_doc_equal),

        /* Chaves e tamanho */
        cmocka_unit_test(test_key_order_matters),
        cmocka_unit_test(test_more_fields_is_greater),
        cmocka_unit_test(test_empty_docs_equal),

        /* MinKey/MaxKey/Null equality */
        cmocka_unit_test(test_minkey_equal),
        cmocka_unit_test(test_maxkey_equal),
        cmocka_unit_test(test_null_equal),

        /* Múltiplos campos */
        cmocka_unit_test(test_multi_field_first_differs),
        cmocka_unit_test(test_multi_field_second_differs),

        /* bson_extract_index_key */
        cmocka_unit_test(test_extract_null_doc),
        cmocka_unit_test(test_extract_null_keys),
        cmocka_unit_test(test_extract_single_field_string),
        cmocka_unit_test(test_extract_single_field_int32),
        cmocka_unit_test(test_extract_multiple_fields),
        cmocka_unit_test(test_extract_three_fields),
        cmocka_unit_test(test_extract_missing_field_becomes_null),
        cmocka_unit_test(test_extract_partial_fields_exist),
        cmocka_unit_test(test_extract_dotted_field),
        cmocka_unit_test(test_extract_dotted_missing),
        cmocka_unit_test(test_extract_deep_dotted),
        cmocka_unit_test(test_extract_empty_doc),
        cmocka_unit_test(test_extract_empty_keys),
        cmocka_unit_test(test_extract_preserves_key_order),
        cmocka_unit_test(test_extract_oid_field),
        cmocka_unit_test(test_extract_bool_field),
        cmocka_unit_test(test_extract_datetime_field),
        cmocka_unit_test(test_extract_double_field),
        cmocka_unit_test(test_extract_array_field),
        cmocka_unit_test(test_extract_nested_doc_field),
        cmocka_unit_test(test_extract_binary_field),
        cmocka_unit_test(test_extract_compound_index_realistic),

        /* bug fixes */
        cmocka_unit_test(test_single_field_doc_equal_strict),
        cmocka_unit_test(test_empty_vs_single_field),
        cmocka_unit_test(test_first_field_must_decide),
        cmocka_unit_test(test_array_simple_less),
        cmocka_unit_test(test_array_length_matters),
        cmocka_unit_test(test_nested_array_compare),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
