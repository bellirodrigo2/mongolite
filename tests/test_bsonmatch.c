// test_bsonmatch.c - Unit tests for bsonmatch (cmocka)

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <bson/bson.h>

#include "mongoc-matcher.h"
#include "bsoncompare.h"

/* ============================================================
 * Test: Basic matcher creation and match
 * ============================================================ */

static void test_matcher_create_destroy(void **state) {
    (void)state;
    bson_error_t error;
    bson_t *query = BCON_NEW("name", BCON_UTF8("Alice"));

    mongoc_matcher_t *matcher = mongoc_matcher_new(query, &error);
    assert_non_null(matcher);

    mongoc_matcher_destroy(matcher);
    bson_destroy(query);
}

static void test_matcher_simple_match(void **state) {
    (void)state;
    bson_error_t error;
    bson_t *query = BCON_NEW("name", BCON_UTF8("Alice"));
    bson_t *doc_match = BCON_NEW("name", BCON_UTF8("Alice"), "age", BCON_INT32(30));
    bson_t *doc_nomatch = BCON_NEW("name", BCON_UTF8("Bob"), "age", BCON_INT32(25));

    mongoc_matcher_t *matcher = mongoc_matcher_new(query, &error);
    assert_non_null(matcher);

    assert_true(mongoc_matcher_match(matcher, doc_match));
    assert_false(mongoc_matcher_match(matcher, doc_nomatch));

    mongoc_matcher_destroy(matcher);
    bson_destroy(query);
    bson_destroy(doc_match);
    bson_destroy(doc_nomatch);
}

static void test_matcher_gt_operator(void **state) {
    (void)state;
    bson_error_t error;
    bson_t *query = BCON_NEW("age", "{", "$gt", BCON_INT32(18), "}");
    bson_t *doc_match = BCON_NEW("name", BCON_UTF8("Alice"), "age", BCON_INT32(30));
    bson_t *doc_nomatch = BCON_NEW("name", BCON_UTF8("Kid"), "age", BCON_INT32(10));

    mongoc_matcher_t *matcher = mongoc_matcher_new(query, &error);
    assert_non_null(matcher);

    assert_true(mongoc_matcher_match(matcher, doc_match));
    assert_false(mongoc_matcher_match(matcher, doc_nomatch));

    mongoc_matcher_destroy(matcher);
    bson_destroy(query);
    bson_destroy(doc_match);
    bson_destroy(doc_nomatch);
}

static void test_matcher_in_operator(void **state) {
    (void)state;
    bson_error_t error;
    bson_t *query = BCON_NEW("status", "{", "$in", "[",
        BCON_UTF8("active"), BCON_UTF8("pending"), "]", "}");

    bson_t *doc_match = BCON_NEW("status", BCON_UTF8("active"));
    bson_t *doc_nomatch = BCON_NEW("status", BCON_UTF8("deleted"));

    mongoc_matcher_t *matcher = mongoc_matcher_new(query, &error);
    assert_non_null(matcher);

    assert_true(mongoc_matcher_match(matcher, doc_match));
    assert_false(mongoc_matcher_match(matcher, doc_nomatch));

    mongoc_matcher_destroy(matcher);
    bson_destroy(query);
    bson_destroy(doc_match);
    bson_destroy(doc_nomatch);
}

static void test_matcher_and_operator(void **state) {
    (void)state;
    bson_error_t error;
    bson_t *query = BCON_NEW(
        "$and", "[",
            "{", "age", "{", "$gte", BCON_INT32(18), "}", "}",
            "{", "active", BCON_BOOL(true), "}",
        "]"
    );

    bson_t *doc_match = BCON_NEW("age", BCON_INT32(25), "active", BCON_BOOL(true));
    bson_t *doc_nomatch1 = BCON_NEW("age", BCON_INT32(15), "active", BCON_BOOL(true));
    bson_t *doc_nomatch2 = BCON_NEW("age", BCON_INT32(25), "active", BCON_BOOL(false));

    mongoc_matcher_t *matcher = mongoc_matcher_new(query, &error);
    assert_non_null(matcher);

    assert_true(mongoc_matcher_match(matcher, doc_match));
    assert_false(mongoc_matcher_match(matcher, doc_nomatch1));
    assert_false(mongoc_matcher_match(matcher, doc_nomatch2));

    mongoc_matcher_destroy(matcher);
    bson_destroy(query);
    bson_destroy(doc_match);
    bson_destroy(doc_nomatch1);
    bson_destroy(doc_nomatch2);
}

static void test_matcher_regex(void **state) {
    (void)state;
    bson_error_t error;

    bson_t query = BSON_INITIALIZER;
    bson_append_regex(&query, "email", -1, "@example\\.com$", "");

    bson_t doc_match = BSON_INITIALIZER;
    bson_append_utf8(&doc_match, "email", -1, "user@example.com", -1);

    bson_t doc_nomatch = BSON_INITIALIZER;
    bson_append_utf8(&doc_nomatch, "email", -1, "user@other.com", -1);

    mongoc_matcher_t *matcher = mongoc_matcher_new(&query, &error);
    assert_non_null(matcher);

    assert_true(mongoc_matcher_match(matcher, &doc_match));
    assert_false(mongoc_matcher_match(matcher, &doc_nomatch));

    mongoc_matcher_destroy(matcher);
    bson_destroy(&query);
    bson_destroy(&doc_match);
    bson_destroy(&doc_nomatch);
}

static void test_matcher_regex_case_insensitive(void **state) {
    (void)state;
    bson_error_t error;

    bson_t query = BSON_INITIALIZER;
    bson_append_regex(&query, "name", -1, "john", "i");

    bson_t doc_match = BSON_INITIALIZER;
    bson_append_utf8(&doc_match, "name", -1, "John Doe", -1);

    bson_t doc_nomatch = BSON_INITIALIZER;
    bson_append_utf8(&doc_nomatch, "name", -1, "Jane Doe", -1);

    mongoc_matcher_t *matcher = mongoc_matcher_new(&query, &error);
    assert_non_null(matcher);

    assert_true(mongoc_matcher_match(matcher, &doc_match));
    assert_false(mongoc_matcher_match(matcher, &doc_nomatch));

    mongoc_matcher_destroy(matcher);
    bson_destroy(&query);
    bson_destroy(&doc_match);
    bson_destroy(&doc_nomatch);
}

/* ============================================================
 * Tests using compare() from bsoncompare.h
 * ============================================================ */

static void test_compare_regex_json_style(void **state) {
    (void)state;
    bson_error_t error;
    const char *spec_json = "{\"hello\": {\"$regex\": \"world\", \"$options\": \"\"}}";
    const char *doc_json = "{\"hello\": \"hello world\"}";
    const char *doc_nomatch_json = "{\"hello\": \"goodbye\"}";

    bson_t *spec = bson_new_from_json((const uint8_t*)spec_json, -1, &error);
    bson_t *doc_match = bson_new_from_json((const uint8_t*)doc_json, -1, &error);
    bson_t *doc_nomatch = bson_new_from_json((const uint8_t*)doc_nomatch_json, -1, &error);

    assert_non_null(spec);
    assert_non_null(doc_match);
    assert_non_null(doc_nomatch);

    int result_match = compare(
        bson_get_data(spec), spec->len,
        bson_get_data(doc_match), doc_match->len
    );
    int result_nomatch = compare(
        bson_get_data(spec), spec->len,
        bson_get_data(doc_nomatch), doc_nomatch->len
    );

    assert_int_equal(1, result_match);
    assert_int_equal(0, result_nomatch);

    bson_destroy(spec);
    bson_destroy(doc_match);
    bson_destroy(doc_nomatch);
}

static void test_compare_regex_case_insensitive(void **state) {
    (void)state;
    bson_error_t error;
    const char *spec_json = "{\"name\": {\"$regex\": \"JOHN\", \"$options\": \"i\"}}";
    const char *doc_json = "{\"name\": \"john doe\"}";

    bson_t *spec = bson_new_from_json((const uint8_t*)spec_json, -1, &error);
    bson_t *doc = bson_new_from_json((const uint8_t*)doc_json, -1, &error);

    assert_non_null(spec);
    assert_non_null(doc);

    int result = compare(
        bson_get_data(spec), spec->len,
        bson_get_data(doc), doc->len
    );

    assert_int_equal(1, result);

    bson_destroy(spec);
    bson_destroy(doc);
}

static void test_matcher_nested_field(void **state) {
    (void)state;
    bson_error_t error;
    bson_t *query = BCON_NEW("address.city", BCON_UTF8("NYC"));

    bson_t *doc_match = BCON_NEW(
        "name", BCON_UTF8("Alice"),
        "address", "{",
            "city", BCON_UTF8("NYC"),
            "zip", BCON_UTF8("10001"),
        "}"
    );
    bson_t *doc_nomatch = BCON_NEW(
        "name", BCON_UTF8("Bob"),
        "address", "{",
            "city", BCON_UTF8("LA"),
            "zip", BCON_UTF8("90001"),
        "}"
    );

    mongoc_matcher_t *matcher = mongoc_matcher_new(query, &error);
    assert_non_null(matcher);

    assert_true(mongoc_matcher_match(matcher, doc_match));
    assert_false(mongoc_matcher_match(matcher, doc_nomatch));

    mongoc_matcher_destroy(matcher);
    bson_destroy(query);
    bson_destroy(doc_match);
    bson_destroy(doc_nomatch);
}

/* Cleanup function - declared in bsoncompare.h */
extern int regex_destroy(void);

static int suite_teardown(void **state) {
    (void)state;
    regex_destroy();
    return 0;
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_matcher_create_destroy),
        cmocka_unit_test(test_matcher_simple_match),
        cmocka_unit_test(test_matcher_gt_operator),
        cmocka_unit_test(test_matcher_in_operator),
        cmocka_unit_test(test_matcher_and_operator),
        cmocka_unit_test(test_matcher_regex),
        cmocka_unit_test(test_matcher_regex_case_insensitive),
        cmocka_unit_test(test_matcher_nested_field),
        cmocka_unit_test(test_compare_regex_json_style),
        cmocka_unit_test(test_compare_regex_case_insensitive),
    };

    return cmocka_run_group_tests(tests, NULL, suite_teardown);
}
