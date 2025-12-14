#include "test_runner.h"
#include "bson_compare.h"

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
 * TESTES: PRECEDÊNCIA DE TIPOS
 * ============================================================ */

TEST(type_minkey_less_than_null) {
    bson_t *a = make_doc_minkey("x");
    bson_t *b = make_doc_null("x");
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(type_null_less_than_number) {
    bson_t *a = make_doc_null("x");
    bson_t *b = make_doc_int32("x", 0);
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(type_number_less_than_string) {
    bson_t *a = make_doc_int32("x", 999);
    bson_t *b = make_doc_utf8("x", "a");
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(type_bool_less_than_datetime) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_BOOL(a, "x", true);
    BSON_APPEND_DATE_TIME(b, "x", 0);
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(type_datetime_less_than_timestamp) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_DATE_TIME(a, "x", 9999999999999LL);
    BSON_APPEND_TIMESTAMP(b, "x", 0, 0);
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

/* ============================================================
 * TESTES: COMPARAÇÃO NUMÉRICA - MESMO TIPO
 * ============================================================ */

TEST(int32_equal) {
    bson_t *a = make_doc_int32("n", 42);
    bson_t *b = make_doc_int32("n", 42);
    TEST_ASSERT_EQUAL(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(int32_less) {
    bson_t *a = make_doc_int32("n", 10);
    bson_t *b = make_doc_int32("n", 20);
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(int32_greater) {
    bson_t *a = make_doc_int32("n", 100);
    bson_t *b = make_doc_int32("n", 50);
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) > 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(int64_equal) {
    bson_t *a = make_doc_int64("n", 1000000000000LL);
    bson_t *b = make_doc_int64("n", 1000000000000LL);
    TEST_ASSERT_EQUAL(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(int64_less) {
    bson_t *a = make_doc_int64("n", 999999999999LL);
    bson_t *b = make_doc_int64("n", 1000000000000LL);
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(double_equal) {
    bson_t *a = make_doc_double("n", 3.14159);
    bson_t *b = make_doc_double("n", 3.14159);
    TEST_ASSERT_EQUAL(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(double_less) {
    bson_t *a = make_doc_double("n", 3.14);
    bson_t *b = make_doc_double("n", 3.15);
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(decimal128_less) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    bson_decimal128_t dec_a, dec_b;
    bson_decimal128_from_string("123.456", &dec_a);
    bson_decimal128_from_string("123.457", &dec_b);
    BSON_APPEND_DECIMAL128(a, "n", &dec_a);
    BSON_APPEND_DECIMAL128(b, "n", &dec_b);
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

/* ============================================================
 * TESTES: STRINGS
 * ============================================================ */

TEST(strings_equal) {
    bson_t *a = make_doc_utf8("s", "hello");
    bson_t *b = make_doc_utf8("s", "hello");
    TEST_ASSERT_EQUAL(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(strings_lexicographic) {
    bson_t *a = make_doc_utf8("s", "abc");
    bson_t *b = make_doc_utf8("s", "abd");
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(strings_prefix_shorter_is_less) {
    bson_t *a = make_doc_utf8("s", "abc");
    bson_t *b = make_doc_utf8("s", "abcd");
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(strings_empty) {
    bson_t *a = make_doc_utf8("s", "");
    bson_t *b = make_doc_utf8("s", "a");
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

/* ============================================================
 * TESTES: BOOLEAN
 * ============================================================ */

TEST(bool_false_less_than_true) {
    bson_t *a = make_doc_bool("b", false);
    bson_t *b = make_doc_bool("b", true);
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(bool_equal_true) {
    bson_t *a = make_doc_bool("b", true);
    bson_t *b = make_doc_bool("b", true);
    TEST_ASSERT_EQUAL(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(bool_equal_false) {
    bson_t *a = make_doc_bool("b", false);
    bson_t *b = make_doc_bool("b", false);
    TEST_ASSERT_EQUAL(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
    return 0;
}

/* ============================================================
 * TESTES: OBJECTID
 * ============================================================ */

TEST(oid_less) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    bson_oid_t oid_a, oid_b;
    bson_oid_init_from_string(&oid_a, "000000000000000000000001");
    bson_oid_init_from_string(&oid_b, "000000000000000000000002");
    BSON_APPEND_OID(a, "id", &oid_a);
    BSON_APPEND_OID(b, "id", &oid_b);
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(oid_equal) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    bson_oid_t oid;
    bson_oid_init_from_string(&oid, "507f1f77bcf86cd799439011");
    BSON_APPEND_OID(a, "id", &oid);
    BSON_APPEND_OID(b, "id", &oid);
    TEST_ASSERT_EQUAL(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
    return 0;
}

/* ============================================================
 * TESTES: DATE_TIME
 * ============================================================ */

TEST(datetime_less) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_DATE_TIME(a, "d", 1000);
    BSON_APPEND_DATE_TIME(b, "d", 2000);
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(datetime_equal) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_DATE_TIME(a, "d", 1702300800000LL);
    BSON_APPEND_DATE_TIME(b, "d", 1702300800000LL);
    TEST_ASSERT_EQUAL(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
    return 0;
}

/* ============================================================
 * TESTES: TIMESTAMP
 * ============================================================ */

TEST(timestamp_by_ts) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_TIMESTAMP(a, "t", 100, 1);
    BSON_APPEND_TIMESTAMP(b, "t", 200, 1);
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(timestamp_by_inc) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_TIMESTAMP(a, "t", 100, 1);
    BSON_APPEND_TIMESTAMP(b, "t", 100, 2);
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(timestamp_equal) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_TIMESTAMP(a, "t", 100, 5);
    BSON_APPEND_TIMESTAMP(b, "t", 100, 5);
    TEST_ASSERT_EQUAL(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
    return 0;
}

/* ============================================================
 * TESTES: BINARY
 * ============================================================ */

TEST(binary_by_length) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    uint8_t data_a[] = {1, 2};
    uint8_t data_b[] = {1, 2, 3};
    BSON_APPEND_BINARY(a, "bin", BSON_SUBTYPE_BINARY, data_a, 2);
    BSON_APPEND_BINARY(b, "bin", BSON_SUBTYPE_BINARY, data_b, 3);
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(binary_by_subtype) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    uint8_t data[] = {1, 2, 3};
    BSON_APPEND_BINARY(a, "bin", BSON_SUBTYPE_BINARY, data, 3);
    BSON_APPEND_BINARY(b, "bin", BSON_SUBTYPE_UUID, data, 3);
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(binary_by_content) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    uint8_t data_a[] = {1, 2, 3};
    uint8_t data_b[] = {1, 2, 4};
    BSON_APPEND_BINARY(a, "bin", BSON_SUBTYPE_BINARY, data_a, 3);
    BSON_APPEND_BINARY(b, "bin", BSON_SUBTYPE_BINARY, data_b, 3);
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

/* ============================================================
 * TESTES: REGEX
 * ============================================================ */

TEST(regex_by_pattern) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_REGEX(a, "r", "abc", "i");
    BSON_APPEND_REGEX(b, "r", "abd", "i");
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(regex_by_options) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_REGEX(a, "r", "abc", "i");
    BSON_APPEND_REGEX(b, "r", "abc", "m");
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(regex_equal) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_REGEX(a, "r", "^test$", "im");
    BSON_APPEND_REGEX(b, "r", "^test$", "im");
    TEST_ASSERT_EQUAL(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
    return 0;
}

/* ============================================================
 * TESTES: DOCUMENTOS ANINHADOS
 * ============================================================ */

TEST(nested_doc_less) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    bson_t child_a, child_b;

    bson_append_document_begin(a, "nested", -1, &child_a);
    BSON_APPEND_INT32(&child_a, "x", 1);
    bson_append_document_end(a, &child_a);

    bson_append_document_begin(b, "nested", -1, &child_b);
    BSON_APPEND_INT32(&child_b, "x", 2);
    bson_append_document_end(b, &child_b);

    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(nested_doc_equal) {
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

    TEST_ASSERT_EQUAL(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
    return 0;
}

/* ============================================================
 * TESTES: CHAVES E TAMANHO DE DOC
 * ============================================================ */

TEST(key_order_matters) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_INT32(a, "aaa", 1);
    BSON_APPEND_INT32(b, "bbb", 1);
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(more_fields_is_greater) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_INT32(a, "x", 1);
    BSON_APPEND_INT32(b, "x", 1);
    BSON_APPEND_INT32(b, "y", 2);
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(empty_docs_equal) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    TEST_ASSERT_EQUAL(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
    return 0;
}

/* ============================================================
 * TESTES: MINKEY/MAXKEY/NULL EQUALITY
 * ============================================================ */

TEST(minkey_equal) {
    bson_t *a = make_doc_minkey("x");
    bson_t *b = make_doc_minkey("x");
    TEST_ASSERT_EQUAL(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(maxkey_equal) {
    bson_t *a = make_doc_maxkey("x");
    bson_t *b = make_doc_maxkey("x");
    TEST_ASSERT_EQUAL(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(null_equal) {
    bson_t *a = make_doc_null("x");
    bson_t *b = make_doc_null("x");
    TEST_ASSERT_EQUAL(0, bson_compare_docs(a, b));
    bson_destroy(a); bson_destroy(b);
    return 0;
}

/* ============================================================
 * TESTES: MÚLTIPLOS CAMPOS
 * ============================================================ */

TEST(multi_field_first_differs) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_INT32(a, "a", 1);
    BSON_APPEND_INT32(a, "b", 100);
    BSON_APPEND_INT32(b, "a", 2);
    BSON_APPEND_INT32(b, "b", 1);
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

TEST(multi_field_second_differs) {
    bson_t *a = bson_new();
    bson_t *b = bson_new();
    BSON_APPEND_INT32(a, "a", 1);
    BSON_APPEND_INT32(a, "b", 10);
    BSON_APPEND_INT32(b, "a", 1);
    BSON_APPEND_INT32(b, "b", 20);
    TEST_ASSERT_TRUE(bson_compare_docs(a, b) < 0);
    bson_destroy(a); bson_destroy(b);
    return 0;
}

/* ============================================================
 * TESTES: bson_extract_index_key
 * ============================================================ */

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

TEST(extract_null_doc) {
    bson_t *keys = make_keys_1("name");
    bson_t *result = bson_extract_index_key(NULL, keys);
    TEST_ASSERT_NULL(result);
    bson_destroy(keys);
    return 0;
}

TEST(extract_null_keys) {
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "test");
    bson_t *result = bson_extract_index_key(doc, NULL);
    TEST_ASSERT_NULL(result);
    bson_destroy(doc);
    return 0;
}

TEST(extract_single_field_string) {
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "Alice");
    BSON_APPEND_INT32(doc, "age", 30);
    BSON_APPEND_UTF8(doc, "city", "NYC");

    bson_t *keys = make_keys_1("name");
    bson_t *result = bson_extract_index_key(doc, keys);

    TEST_ASSERT_NOT_NULL(result);

    bson_t *expected = bson_new();
    BSON_APPEND_UTF8(expected, "name", "Alice");

    TEST_ASSERT_TRUE(bson_docs_equal(result, expected));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    bson_destroy(expected);
    return 0;
}

TEST(extract_single_field_int32) {
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "Bob");
    BSON_APPEND_INT32(doc, "age", 25);

    bson_t *keys = make_keys_1("age");
    bson_t *result = bson_extract_index_key(doc, keys);

    TEST_ASSERT_NOT_NULL(result);

    bson_t *expected = bson_new();
    BSON_APPEND_INT32(expected, "age", 25);

    TEST_ASSERT_TRUE(bson_docs_equal(result, expected));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    bson_destroy(expected);
    return 0;
}

TEST(extract_multiple_fields) {
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "Charlie");
    BSON_APPEND_INT32(doc, "age", 35);
    BSON_APPEND_UTF8(doc, "city", "LA");
    BSON_APPEND_DOUBLE(doc, "score", 95.5);

    bson_t *keys = make_keys_2("name", "age");
    bson_t *result = bson_extract_index_key(doc, keys);

    TEST_ASSERT_NOT_NULL(result);

    bson_t *expected = bson_new();
    BSON_APPEND_UTF8(expected, "name", "Charlie");
    BSON_APPEND_INT32(expected, "age", 35);

    TEST_ASSERT_TRUE(bson_docs_equal(result, expected));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    bson_destroy(expected);
    return 0;
}

TEST(extract_three_fields) {
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "a", "val_a");
    BSON_APPEND_INT32(doc, "b", 100);
    BSON_APPEND_DOUBLE(doc, "c", 3.14);
    BSON_APPEND_BOOL(doc, "d", true);

    bson_t *keys = make_keys_3("a", "b", "c");
    bson_t *result = bson_extract_index_key(doc, keys);

    TEST_ASSERT_NOT_NULL(result);

    bson_t *expected = bson_new();
    BSON_APPEND_UTF8(expected, "a", "val_a");
    BSON_APPEND_INT32(expected, "b", 100);
    BSON_APPEND_DOUBLE(expected, "c", 3.14);

    TEST_ASSERT_TRUE(bson_docs_equal(result, expected));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    bson_destroy(expected);
    return 0;
}

TEST(extract_missing_field_becomes_null) {
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "Dave");

    bson_t *keys = make_keys_1("nonexistent");
    bson_t *result = bson_extract_index_key(doc, keys);

    TEST_ASSERT_NOT_NULL(result);

    bson_t *expected = bson_new();
    BSON_APPEND_NULL(expected, "nonexistent");

    TEST_ASSERT_TRUE(bson_docs_equal(result, expected));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    bson_destroy(expected);
    return 0;
}

TEST(extract_partial_fields_exist) {
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "Eve");
    BSON_APPEND_INT32(doc, "age", 28);

    bson_t *keys = make_keys_3("name", "missing", "age");
    bson_t *result = bson_extract_index_key(doc, keys);

    TEST_ASSERT_NOT_NULL(result);

    bson_t *expected = bson_new();
    BSON_APPEND_UTF8(expected, "name", "Eve");
    BSON_APPEND_NULL(expected, "missing");
    BSON_APPEND_INT32(expected, "age", 28);

    TEST_ASSERT_TRUE(bson_docs_equal(result, expected));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    bson_destroy(expected);
    return 0;
}

TEST(extract_dotted_field) {
    bson_t *doc = bson_new();
    bson_t address;

    BSON_APPEND_UTF8(doc, "name", "Frank");
    bson_append_document_begin(doc, "address", -1, &address);
    BSON_APPEND_UTF8(&address, "city", "Boston");
    BSON_APPEND_UTF8(&address, "zip", "02101");
    bson_append_document_end(doc, &address);

    bson_t *keys = make_keys_1("address.city");
    bson_t *result = bson_extract_index_key(doc, keys);

    TEST_ASSERT_NOT_NULL(result);

    /* Resultado deve ter "address.city": "Boston" */
    bson_iter_t iter;
    TEST_ASSERT_TRUE(bson_iter_init_find(&iter, result, "address.city"));
    TEST_ASSERT_EQUAL(BSON_TYPE_UTF8, bson_iter_type(&iter));
    TEST_ASSERT_EQUAL_STRING("Boston", bson_iter_utf8(&iter, NULL));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    return 0;
}

TEST(extract_dotted_missing) {
    bson_t *doc = bson_new();
    bson_t address;

    BSON_APPEND_UTF8(doc, "name", "Grace");
    bson_append_document_begin(doc, "address", -1, &address);
    BSON_APPEND_UTF8(&address, "city", "Chicago");
    bson_append_document_end(doc, &address);

    bson_t *keys = make_keys_1("address.country");
    bson_t *result = bson_extract_index_key(doc, keys);

    TEST_ASSERT_NOT_NULL(result);

    /* Campo não existe, deve ser null */
    bson_iter_t iter;
    TEST_ASSERT_TRUE(bson_iter_init_find(&iter, result, "address.country"));
    TEST_ASSERT_EQUAL(BSON_TYPE_NULL, bson_iter_type(&iter));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    return 0;
}

TEST(extract_deep_dotted) {
    bson_t *doc = bson_new();
    bson_t level1, level2;

    bson_append_document_begin(doc, "a", -1, &level1);
    bson_append_document_begin(&level1, "b", -1, &level2);
    BSON_APPEND_INT32(&level2, "c", 42);
    bson_append_document_end(&level1, &level2);
    bson_append_document_end(doc, &level1);

    bson_t *keys = make_keys_1("a.b.c");
    bson_t *result = bson_extract_index_key(doc, keys);

    TEST_ASSERT_NOT_NULL(result);

    bson_iter_t iter;
    TEST_ASSERT_TRUE(bson_iter_init_find(&iter, result, "a.b.c"));
    TEST_ASSERT_EQUAL(BSON_TYPE_INT32, bson_iter_type(&iter));
    TEST_ASSERT_EQUAL(42, bson_iter_int32(&iter));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    return 0;
}

TEST(extract_empty_doc) {
    bson_t *doc = bson_new();
    bson_t *keys = make_keys_2("name", "age");
    bson_t *result = bson_extract_index_key(doc, keys);

    TEST_ASSERT_NOT_NULL(result);

    bson_t *expected = bson_new();
    BSON_APPEND_NULL(expected, "name");
    BSON_APPEND_NULL(expected, "age");

    TEST_ASSERT_TRUE(bson_docs_equal(result, expected));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    bson_destroy(expected);
    return 0;
}

TEST(extract_empty_keys) {
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "Henry");

    bson_t *keys = bson_new(); /* empty */
    bson_t *result = bson_extract_index_key(doc, keys);

    TEST_ASSERT_NOT_NULL(result);

    bson_t *expected = bson_new(); /* empty */
    TEST_ASSERT_TRUE(bson_docs_equal(result, expected));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    bson_destroy(expected);
    return 0;
}

TEST(extract_preserves_key_order) {
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "z", "last");
    BSON_APPEND_UTF8(doc, "a", "first");
    BSON_APPEND_UTF8(doc, "m", "middle");

    /* Keys em ordem diferente do doc */
    bson_t *keys = make_keys_3("m", "z", "a");
    bson_t *result = bson_extract_index_key(doc, keys);

    TEST_ASSERT_NOT_NULL(result);

    /* Verificar ordem: m, z, a */
    bson_iter_t iter;
    TEST_ASSERT_TRUE(bson_iter_init(&iter, result));

    TEST_ASSERT_TRUE(bson_iter_next(&iter));
    TEST_ASSERT_EQUAL_STRING("m", bson_iter_key(&iter));

    TEST_ASSERT_TRUE(bson_iter_next(&iter));
    TEST_ASSERT_EQUAL_STRING("z", bson_iter_key(&iter));

    TEST_ASSERT_TRUE(bson_iter_next(&iter));
    TEST_ASSERT_EQUAL_STRING("a", bson_iter_key(&iter));

    TEST_ASSERT_FALSE(bson_iter_next(&iter));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    return 0;
}

TEST(extract_oid_field) {
    bson_t *doc = bson_new();
    bson_oid_t oid;
    bson_oid_init_from_string(&oid, "507f1f77bcf86cd799439011");
    BSON_APPEND_OID(doc, "_id", &oid);
    BSON_APPEND_UTF8(doc, "name", "Ivy");

    bson_t *keys = make_keys_1("_id");
    bson_t *result = bson_extract_index_key(doc, keys);

    TEST_ASSERT_NOT_NULL(result);

    bson_iter_t iter;
    TEST_ASSERT_TRUE(bson_iter_init_find(&iter, result, "_id"));
    TEST_ASSERT_EQUAL(BSON_TYPE_OID, bson_iter_type(&iter));

    const bson_oid_t *result_oid = bson_iter_oid(&iter);
    TEST_ASSERT_EQUAL(0, bson_oid_compare(&oid, result_oid));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    return 0;
}

TEST(extract_bool_field) {
    bson_t *doc = bson_new();
    BSON_APPEND_BOOL(doc, "active", true);
    BSON_APPEND_BOOL(doc, "verified", false);

    bson_t *keys = make_keys_2("active", "verified");
    bson_t *result = bson_extract_index_key(doc, keys);

    TEST_ASSERT_NOT_NULL(result);

    bson_iter_t iter;
    TEST_ASSERT_TRUE(bson_iter_init_find(&iter, result, "active"));
    TEST_ASSERT_TRUE(bson_iter_bool(&iter));

    TEST_ASSERT_TRUE(bson_iter_init_find(&iter, result, "verified"));
    TEST_ASSERT_FALSE(bson_iter_bool(&iter));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    return 0;
}

TEST(extract_datetime_field) {
    bson_t *doc = bson_new();
    int64_t ts = 1702300800000LL;
    BSON_APPEND_DATE_TIME(doc, "created", ts);

    bson_t *keys = make_keys_1("created");
    bson_t *result = bson_extract_index_key(doc, keys);

    TEST_ASSERT_NOT_NULL(result);

    bson_iter_t iter;
    TEST_ASSERT_TRUE(bson_iter_init_find(&iter, result, "created"));
    TEST_ASSERT_EQUAL(BSON_TYPE_DATE_TIME, bson_iter_type(&iter));
    TEST_ASSERT_EQUAL(ts, bson_iter_date_time(&iter));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    return 0;
}

TEST(extract_double_field) {
    bson_t *doc = bson_new();
    BSON_APPEND_DOUBLE(doc, "price", 19.99);
    BSON_APPEND_DOUBLE(doc, "tax", 1.50);

    bson_t *keys = make_keys_1("price");
    bson_t *result = bson_extract_index_key(doc, keys);

    TEST_ASSERT_NOT_NULL(result);

    bson_iter_t iter;
    TEST_ASSERT_TRUE(bson_iter_init_find(&iter, result, "price"));
    TEST_ASSERT_EQUAL(BSON_TYPE_DOUBLE, bson_iter_type(&iter));
    /* Double comparison with tolerance */
    double diff = bson_iter_double(&iter) - 19.99;
    TEST_ASSERT_TRUE(diff < 0.001 && diff > -0.001);

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    return 0;
}

TEST(extract_array_field) {
    bson_t *doc = bson_new();
    bson_t arr;

    BSON_APPEND_UTF8(doc, "name", "Jack");
    bson_append_array_begin(doc, "tags", -1, &arr);
    BSON_APPEND_UTF8(&arr, "0", "red");
    BSON_APPEND_UTF8(&arr, "1", "blue");
    bson_append_array_end(doc, &arr);

    bson_t *keys = make_keys_1("tags");
    bson_t *result = bson_extract_index_key(doc, keys);

    TEST_ASSERT_NOT_NULL(result);

    bson_iter_t iter;
    TEST_ASSERT_TRUE(bson_iter_init_find(&iter, result, "tags"));
    TEST_ASSERT_EQUAL(BSON_TYPE_ARRAY, bson_iter_type(&iter));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    return 0;
}

TEST(extract_nested_doc_field) {
    bson_t *doc = bson_new();
    bson_t nested;

    BSON_APPEND_UTF8(doc, "name", "Kate");
    bson_append_document_begin(doc, "meta", -1, &nested);
    BSON_APPEND_INT32(&nested, "version", 1);
    BSON_APPEND_BOOL(&nested, "active", true);
    bson_append_document_end(doc, &nested);

    bson_t *keys = make_keys_1("meta");
    bson_t *result = bson_extract_index_key(doc, keys);

    TEST_ASSERT_NOT_NULL(result);

    bson_iter_t iter;
    TEST_ASSERT_TRUE(bson_iter_init_find(&iter, result, "meta"));
    TEST_ASSERT_EQUAL(BSON_TYPE_DOCUMENT, bson_iter_type(&iter));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    return 0;
}

TEST(extract_binary_field) {
    bson_t *doc = bson_new();
    uint8_t data[] = {0xDE, 0xAD, 0xBE, 0xEF};
    BSON_APPEND_BINARY(doc, "data", BSON_SUBTYPE_BINARY, data, 4);

    bson_t *keys = make_keys_1("data");
    bson_t *result = bson_extract_index_key(doc, keys);

    TEST_ASSERT_NOT_NULL(result);

    bson_iter_t iter;
    TEST_ASSERT_TRUE(bson_iter_init_find(&iter, result, "data"));
    TEST_ASSERT_EQUAL(BSON_TYPE_BINARY, bson_iter_type(&iter));

    bson_subtype_t subtype;
    uint32_t len;
    const uint8_t *bin;
    bson_iter_binary(&iter, &subtype, &len, &bin);
    TEST_ASSERT_EQUAL(4, len);
    TEST_ASSERT_EQUAL_MEMORY(data, bin, 4);

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    return 0;
}

TEST(extract_compound_index_realistic) {
    /* Simula índice composto: {lastName: 1, firstName: 1, age: 1} */
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "firstName", "John");
    BSON_APPEND_UTF8(doc, "lastName", "Doe");
    BSON_APPEND_INT32(doc, "age", 30);
    BSON_APPEND_UTF8(doc, "email", "john@example.com");
    BSON_APPEND_BOOL(doc, "active", true);

    bson_t *keys = make_keys_3("lastName", "firstName", "age");
    bson_t *result = bson_extract_index_key(doc, keys);

    TEST_ASSERT_NOT_NULL(result);

    /* Verificar ordem e valores */
    bson_iter_t iter;
    TEST_ASSERT_TRUE(bson_iter_init(&iter, result));

    TEST_ASSERT_TRUE(bson_iter_next(&iter));
    TEST_ASSERT_EQUAL_STRING("lastName", bson_iter_key(&iter));
    TEST_ASSERT_EQUAL_STRING("Doe", bson_iter_utf8(&iter, NULL));

    TEST_ASSERT_TRUE(bson_iter_next(&iter));
    TEST_ASSERT_EQUAL_STRING("firstName", bson_iter_key(&iter));
    TEST_ASSERT_EQUAL_STRING("John", bson_iter_utf8(&iter, NULL));

    TEST_ASSERT_TRUE(bson_iter_next(&iter));
    TEST_ASSERT_EQUAL_STRING("age", bson_iter_key(&iter));
    TEST_ASSERT_EQUAL(30, bson_iter_int32(&iter));

    TEST_ASSERT_FALSE(bson_iter_next(&iter));

    bson_destroy(doc);
    bson_destroy(keys);
    bson_destroy(result);
    return 0;
}

/* ============================================================
 * MAIN
 * ============================================================ */

TEST_SUITE_BEGIN("bson_compare tests")

    /* Precedência de tipos */
    RUN_TEST(test_type_minkey_less_than_null);
    RUN_TEST(test_type_null_less_than_number);
    RUN_TEST(test_type_number_less_than_string);
    RUN_TEST(test_type_bool_less_than_datetime);
    RUN_TEST(test_type_datetime_less_than_timestamp);

    /* Numéricos - mesmo tipo */
    RUN_TEST(test_int32_equal);
    RUN_TEST(test_int32_less);
    RUN_TEST(test_int32_greater);
    RUN_TEST(test_int64_equal);
    RUN_TEST(test_int64_less);
    RUN_TEST(test_double_equal);
    RUN_TEST(test_double_less);
    RUN_TEST(test_decimal128_less);

    /* Strings */
    RUN_TEST(test_strings_equal);
    RUN_TEST(test_strings_lexicographic);
    RUN_TEST(test_strings_prefix_shorter_is_less);
    RUN_TEST(test_strings_empty);

    /* Boolean */
    RUN_TEST(test_bool_false_less_than_true);
    RUN_TEST(test_bool_equal_true);
    RUN_TEST(test_bool_equal_false);

    /* ObjectId */
    RUN_TEST(test_oid_less);
    RUN_TEST(test_oid_equal);

    /* DateTime */
    RUN_TEST(test_datetime_less);
    RUN_TEST(test_datetime_equal);

    /* Timestamp */
    RUN_TEST(test_timestamp_by_ts);
    RUN_TEST(test_timestamp_by_inc);
    RUN_TEST(test_timestamp_equal);

    /* Binary */
    RUN_TEST(test_binary_by_length);
    RUN_TEST(test_binary_by_subtype);
    RUN_TEST(test_binary_by_content);

    /* Regex */
    RUN_TEST(test_regex_by_pattern);
    RUN_TEST(test_regex_by_options);
    RUN_TEST(test_regex_equal);

    /* Documentos aninhados */
    RUN_TEST(test_nested_doc_less);
    RUN_TEST(test_nested_doc_equal);

    /* Chaves e tamanho */
    RUN_TEST(test_key_order_matters);
    RUN_TEST(test_more_fields_is_greater);
    RUN_TEST(test_empty_docs_equal);

    /* MinKey/MaxKey/Null equality */
    RUN_TEST(test_minkey_equal);
    RUN_TEST(test_maxkey_equal);
    RUN_TEST(test_null_equal);

    /* Múltiplos campos */
    RUN_TEST(test_multi_field_first_differs);
    RUN_TEST(test_multi_field_second_differs);

    /* bson_extract_index_key */
    RUN_TEST(test_extract_null_doc);
    RUN_TEST(test_extract_null_keys);
    RUN_TEST(test_extract_single_field_string);
    RUN_TEST(test_extract_single_field_int32);
    RUN_TEST(test_extract_multiple_fields);
    RUN_TEST(test_extract_three_fields);
    RUN_TEST(test_extract_missing_field_becomes_null);
    RUN_TEST(test_extract_partial_fields_exist);
    RUN_TEST(test_extract_dotted_field);
    RUN_TEST(test_extract_dotted_missing);
    RUN_TEST(test_extract_deep_dotted);
    RUN_TEST(test_extract_empty_doc);
    RUN_TEST(test_extract_empty_keys);
    RUN_TEST(test_extract_preserves_key_order);
    RUN_TEST(test_extract_oid_field);
    RUN_TEST(test_extract_bool_field);
    RUN_TEST(test_extract_datetime_field);
    RUN_TEST(test_extract_double_field);
    RUN_TEST(test_extract_array_field);
    RUN_TEST(test_extract_nested_doc_field);
    RUN_TEST(test_extract_binary_field);
    RUN_TEST(test_extract_compound_index_realistic);

TEST_SUITE_END()