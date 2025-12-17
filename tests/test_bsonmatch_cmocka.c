// test_bsonmatch_cmocka.c - Unit tests for bsonmatch using CMocka

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <stdint.h>
#include <cmocka.h>

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
    /* Use BCON_REGEX to create native BSON_TYPE_REGEX */
    bson_t *query = BCON_NEW("email", BCON_REGEX("@example\\.com$", ""));

    bson_t *doc_match = BCON_NEW("email", BCON_UTF8("user@example.com"));
    bson_t *doc_nomatch = BCON_NEW("email", BCON_UTF8("user@other.com"));

    mongoc_matcher_t *matcher = mongoc_matcher_new(query, &error);
    assert_non_null(matcher);

    assert_true(mongoc_matcher_match(matcher, doc_match));
    assert_false(mongoc_matcher_match(matcher, doc_nomatch));

    mongoc_matcher_destroy(matcher);
    bson_destroy(query);
    bson_destroy(doc_match);
    bson_destroy(doc_nomatch);
}

static void test_matcher_regex_case_insensitive(void **state) {
    (void)state;
    bson_error_t error;
    /* "i" option for case insensitive */
    bson_t *query = BCON_NEW("name", BCON_REGEX("john", "i"));

    bson_t *doc_match = BCON_NEW("name", BCON_UTF8("John Doe"));
    bson_t *doc_nomatch = BCON_NEW("name", BCON_UTF8("Jane Doe"));

    mongoc_matcher_t *matcher = mongoc_matcher_new(query, &error);
    assert_non_null(matcher);

    assert_true(mongoc_matcher_match(matcher, doc_match));
    assert_false(mongoc_matcher_match(matcher, doc_nomatch));

    mongoc_matcher_destroy(matcher);
    bson_destroy(query);
    bson_destroy(doc_match);
    bson_destroy(doc_nomatch);
}

/* ============================================================
 * Tests using compare() from bsoncompare.h
 * ============================================================ */

static void test_compare_regex_json_style(void **state) {
    (void)state;
    /* Using JSON-style $regex/$options (Extended JSON) */
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

/* ============================================================
 * MAIN
 * ============================================================ */

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

        /* Tests using compare() with JSON-style $regex */
        cmocka_unit_test(test_compare_regex_json_style),
        cmocka_unit_test(test_compare_regex_case_insensitive),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
