/**
 * test_helpers.c - Tests for mongolite_helpers.h inline functions
 *
 * Tests helper functions:
 * - extract_doc_oid()
 * - extract_doc_oid_with_error()
 * - parse_json_to_bson()
 * - parse_optional_json_to_bson()
 * - cleanup_bson_array()
 */

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>

#include "mongolite_helpers.h"

/* ============================================================
 * extract_doc_oid Tests
 * ============================================================ */

static void test_extract_doc_oid_success(void **state) {
    (void)state;

    bson_oid_t original_oid;
    bson_oid_init(&original_oid, NULL);

    bson_t *doc = bson_new();
    BSON_APPEND_OID(doc, "_id", &original_oid);
    BSON_APPEND_INT32(doc, "value", 42);

    bson_oid_t extracted_oid;
    bool result = extract_doc_oid(doc, &extracted_oid);

    assert_true(result);
    assert_memory_equal(original_oid.bytes, extracted_oid.bytes, sizeof(bson_oid_t));

    bson_destroy(doc);
}

static void test_extract_doc_oid_missing_id(void **state) {
    (void)state;

    /* Document without _id field */
    bson_t *doc = bson_new();
    BSON_APPEND_INT32(doc, "value", 42);
    BSON_APPEND_UTF8(doc, "name", "test");

    bson_oid_t extracted_oid;
    bool result = extract_doc_oid(doc, &extracted_oid);

    assert_false(result);

    bson_destroy(doc);
}

static void test_extract_doc_oid_wrong_type(void **state) {
    (void)state;

    /* Document with _id that's not an OID */
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "_id", "string_id");  /* String instead of OID */
    BSON_APPEND_INT32(doc, "value", 42);

    bson_oid_t extracted_oid;
    bool result = extract_doc_oid(doc, &extracted_oid);

    assert_false(result);

    bson_destroy(doc);
}

static void test_extract_doc_oid_int_id(void **state) {
    (void)state;

    /* Document with _id as integer */
    bson_t *doc = bson_new();
    BSON_APPEND_INT32(doc, "_id", 12345);
    BSON_APPEND_UTF8(doc, "name", "test");

    bson_oid_t extracted_oid;
    bool result = extract_doc_oid(doc, &extracted_oid);

    assert_false(result);

    bson_destroy(doc);
}

/* ============================================================
 * extract_doc_oid_with_error Tests
 * ============================================================ */

static void test_extract_doc_oid_with_error_success(void **state) {
    (void)state;

    bson_oid_t original_oid;
    bson_oid_init(&original_oid, NULL);

    bson_t *doc = bson_new();
    BSON_APPEND_OID(doc, "_id", &original_oid);

    bson_oid_t extracted_oid;
    gerror_t error = {0};

    bool result = extract_doc_oid_with_error(doc, &extracted_oid, &error);

    assert_true(result);
    assert_int_equal(0, error.code);
    assert_memory_equal(original_oid.bytes, extracted_oid.bytes, sizeof(bson_oid_t));

    bson_destroy(doc);
}

static void test_extract_doc_oid_with_error_missing_id(void **state) {
    (void)state;

    bson_t *doc = bson_new();
    BSON_APPEND_INT32(doc, "value", 42);

    bson_oid_t extracted_oid;
    gerror_t error = {0};

    bool result = extract_doc_oid_with_error(doc, &extracted_oid, &error);

    assert_false(result);
    assert_int_equal(-1000, error.code);
    assert_true(strlen(error.message) > 0);
    assert_string_equal("mongolite", error.lib);

    bson_destroy(doc);
}

static void test_extract_doc_oid_with_error_null_error(void **state) {
    (void)state;

    /* Test that passing NULL error doesn't crash */
    bson_t *doc = bson_new();
    BSON_APPEND_INT32(doc, "value", 42);

    bson_oid_t extracted_oid;
    bool result = extract_doc_oid_with_error(doc, &extracted_oid, NULL);

    assert_false(result);

    bson_destroy(doc);
}

static void test_extract_doc_oid_with_error_wrong_type(void **state) {
    (void)state;

    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "_id", "not_an_oid");

    bson_oid_t extracted_oid;
    gerror_t error = {0};

    bool result = extract_doc_oid_with_error(doc, &extracted_oid, &error);

    assert_false(result);
    assert_int_equal(-1000, error.code);

    bson_destroy(doc);
}

/* ============================================================
 * parse_json_to_bson Tests
 * ============================================================ */

static void test_parse_json_to_bson_success(void **state) {
    (void)state;

    gerror_t error = {0};
    bson_t *doc = parse_json_to_bson("{\"name\": \"Alice\", \"age\": 30}", &error);

    assert_non_null(doc);
    assert_int_equal(0, error.code);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, doc, "name"));
    assert_string_equal("Alice", bson_iter_utf8(&iter, NULL));

    assert_true(bson_iter_init_find(&iter, doc, "age"));
    assert_int_equal(30, bson_iter_int32(&iter));

    bson_destroy(doc);
}

static void test_parse_json_to_bson_null_string(void **state) {
    (void)state;

    gerror_t error = {0};
    bson_t *doc = parse_json_to_bson(NULL, &error);

    assert_null(doc);
    assert_int_equal(-1003, error.code);
    assert_true(strlen(error.message) > 0);
}

static void test_parse_json_to_bson_invalid_json(void **state) {
    (void)state;

    gerror_t error = {0};
    bson_t *doc = parse_json_to_bson("{\"name\": invalid}", &error);

    assert_null(doc);
    assert_int_not_equal(0, error.code);
}

static void test_parse_json_to_bson_null_error(void **state) {
    (void)state;

    /* Test that passing NULL error doesn't crash */
    bson_t *doc = parse_json_to_bson("{\"valid\": true}", NULL);

    assert_non_null(doc);
    bson_destroy(doc);

    /* Invalid JSON with NULL error should also not crash */
    doc = parse_json_to_bson("{invalid}", NULL);
    assert_null(doc);

    /* NULL JSON with NULL error */
    doc = parse_json_to_bson(NULL, NULL);
    assert_null(doc);
}

static void test_parse_json_to_bson_empty_object(void **state) {
    (void)state;

    gerror_t error = {0};
    bson_t *doc = parse_json_to_bson("{}", &error);

    assert_non_null(doc);
    assert_int_equal(0, error.code);

    bson_destroy(doc);
}

/* ============================================================
 * parse_optional_json_to_bson Tests
 * ============================================================ */

static void test_parse_optional_json_null_returns_null(void **state) {
    (void)state;

    gerror_t error = {0};
    bson_t *doc = parse_optional_json_to_bson(NULL, &error);

    /* NULL input should return NULL without error (it's optional) */
    assert_null(doc);
    assert_int_equal(0, error.code);
}

static void test_parse_optional_json_valid(void **state) {
    (void)state;

    gerror_t error = {0};
    bson_t *doc = parse_optional_json_to_bson("{\"key\": \"value\"}", &error);

    assert_non_null(doc);
    assert_int_equal(0, error.code);

    bson_destroy(doc);
}

static void test_parse_optional_json_invalid(void **state) {
    (void)state;

    gerror_t error = {0};
    bson_t *doc = parse_optional_json_to_bson("{invalid json}", &error);

    assert_null(doc);
    assert_int_not_equal(0, error.code);
}

/* ============================================================
 * cleanup_bson_array Tests
 * ============================================================ */

static void test_cleanup_bson_array_valid(void **state) {
    (void)state;

    /* Create array of BSON documents */
    bson_t **arr = malloc(sizeof(bson_t*) * 3);
    arr[0] = bson_new();
    BSON_APPEND_INT32(arr[0], "a", 1);
    arr[1] = bson_new();
    BSON_APPEND_INT32(arr[1], "b", 2);
    arr[2] = bson_new();
    BSON_APPEND_INT32(arr[2], "c", 3);

    /* This should free all documents and the array itself */
    cleanup_bson_array(arr, 3);

    /* If we get here without crashing, the test passes */
}

static void test_cleanup_bson_array_with_nulls(void **state) {
    (void)state;

    /* Array with some NULL entries */
    bson_t **arr = malloc(sizeof(bson_t*) * 4);
    arr[0] = bson_new();
    arr[1] = NULL;  /* NULL entry */
    arr[2] = bson_new();
    arr[3] = NULL;

    /* Should handle NULL entries gracefully */
    cleanup_bson_array(arr, 4);
}

static void test_cleanup_bson_array_null_array(void **state) {
    (void)state;

    /* NULL array should not crash */
    cleanup_bson_array(NULL, 0);
    cleanup_bson_array(NULL, 100);
}

static void test_cleanup_bson_array_zero_count(void **state) {
    (void)state;

    bson_t **arr = malloc(sizeof(bson_t*) * 1);
    arr[0] = bson_new();

    /* Count 0 - should just free the array, not the document */
    /* Note: This creates a memory leak for arr[0], but tests the edge case */
    cleanup_bson_array(arr, 0);

    /* To avoid the leak, let's test differently */
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* extract_doc_oid tests */
        cmocka_unit_test(test_extract_doc_oid_success),
        cmocka_unit_test(test_extract_doc_oid_missing_id),
        cmocka_unit_test(test_extract_doc_oid_wrong_type),
        cmocka_unit_test(test_extract_doc_oid_int_id),

        /* extract_doc_oid_with_error tests */
        cmocka_unit_test(test_extract_doc_oid_with_error_success),
        cmocka_unit_test(test_extract_doc_oid_with_error_missing_id),
        cmocka_unit_test(test_extract_doc_oid_with_error_null_error),
        cmocka_unit_test(test_extract_doc_oid_with_error_wrong_type),

        /* parse_json_to_bson tests */
        cmocka_unit_test(test_parse_json_to_bson_success),
        cmocka_unit_test(test_parse_json_to_bson_null_string),
        cmocka_unit_test(test_parse_json_to_bson_invalid_json),
        cmocka_unit_test(test_parse_json_to_bson_null_error),
        cmocka_unit_test(test_parse_json_to_bson_empty_object),

        /* parse_optional_json_to_bson tests */
        cmocka_unit_test(test_parse_optional_json_null_returns_null),
        cmocka_unit_test(test_parse_optional_json_valid),
        cmocka_unit_test(test_parse_optional_json_invalid),

        /* cleanup_bson_array tests */
        cmocka_unit_test(test_cleanup_bson_array_valid),
        cmocka_unit_test(test_cleanup_bson_array_with_nulls),
        cmocka_unit_test(test_cleanup_bson_array_null_array),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
