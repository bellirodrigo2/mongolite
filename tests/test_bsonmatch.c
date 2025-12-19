#include "test_runner.h"
#include "mongoc-matcher.h"
#include "bsoncompare.h"

/* ============================================================
 * Test: Basic matcher creation and match
 * ============================================================ */

TEST(matcher_create_destroy) {
    bson_error_t error;
    bson_t *query = BCON_NEW("name", BCON_UTF8("Alice"));

    mongoc_matcher_t *matcher = mongoc_matcher_new(query, &error);
    TEST_ASSERT_NOT_NULL(matcher);

    mongoc_matcher_destroy(matcher);
    bson_destroy(query);
    return 0;
}

TEST(matcher_simple_match) {
    bson_error_t error;
    bson_t *query = BCON_NEW("name", BCON_UTF8("Alice"));
    bson_t *doc_match = BCON_NEW("name", BCON_UTF8("Alice"), "age", BCON_INT32(30));
    bson_t *doc_nomatch = BCON_NEW("name", BCON_UTF8("Bob"), "age", BCON_INT32(25));

    mongoc_matcher_t *matcher = mongoc_matcher_new(query, &error);
    TEST_ASSERT_NOT_NULL(matcher);

    TEST_ASSERT_TRUE(mongoc_matcher_match(matcher, doc_match));
    TEST_ASSERT_FALSE(mongoc_matcher_match(matcher, doc_nomatch));

    mongoc_matcher_destroy(matcher);
    bson_destroy(query);
    bson_destroy(doc_match);
    bson_destroy(doc_nomatch);
    return 0;
}

TEST(matcher_gt_operator) {
    bson_error_t error;
    bson_t *query = BCON_NEW("age", "{", "$gt", BCON_INT32(18), "}");
    bson_t *doc_match = BCON_NEW("name", BCON_UTF8("Alice"), "age", BCON_INT32(30));
    bson_t *doc_nomatch = BCON_NEW("name", BCON_UTF8("Kid"), "age", BCON_INT32(10));

    mongoc_matcher_t *matcher = mongoc_matcher_new(query, &error);
    TEST_ASSERT_NOT_NULL(matcher);

    TEST_ASSERT_TRUE(mongoc_matcher_match(matcher, doc_match));
    TEST_ASSERT_FALSE(mongoc_matcher_match(matcher, doc_nomatch));

    mongoc_matcher_destroy(matcher);
    bson_destroy(query);
    bson_destroy(doc_match);
    bson_destroy(doc_nomatch);
    return 0;
}

TEST(matcher_in_operator) {
    bson_error_t error;
    bson_t *query = BCON_NEW("status", "{", "$in", "[",
        BCON_UTF8("active"), BCON_UTF8("pending"), "]", "}");

    bson_t *doc_match = BCON_NEW("status", BCON_UTF8("active"));
    bson_t *doc_nomatch = BCON_NEW("status", BCON_UTF8("deleted"));

    mongoc_matcher_t *matcher = mongoc_matcher_new(query, &error);
    TEST_ASSERT_NOT_NULL(matcher);

    TEST_ASSERT_TRUE(mongoc_matcher_match(matcher, doc_match));
    TEST_ASSERT_FALSE(mongoc_matcher_match(matcher, doc_nomatch));

    mongoc_matcher_destroy(matcher);
    bson_destroy(query);
    bson_destroy(doc_match);
    bson_destroy(doc_nomatch);
    return 0;
}

TEST(matcher_and_operator) {
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
    TEST_ASSERT_NOT_NULL(matcher);

    TEST_ASSERT_TRUE(mongoc_matcher_match(matcher, doc_match));
    TEST_ASSERT_FALSE(mongoc_matcher_match(matcher, doc_nomatch1));
    TEST_ASSERT_FALSE(mongoc_matcher_match(matcher, doc_nomatch2));

    mongoc_matcher_destroy(matcher);
    bson_destroy(query);
    bson_destroy(doc_match);
    bson_destroy(doc_nomatch1);
    bson_destroy(doc_nomatch2);
    return 0;
}

TEST(matcher_regex) {
    bson_error_t error;

    /* Zero-initialize stack-allocated bson_t to avoid valgrind warnings about uninitialized padding */
    bson_t query = BSON_INITIALIZER;
    bson_append_regex(&query, "email", -1, "@example\\.com$", "");

    bson_t doc_match = BSON_INITIALIZER;
    bson_append_utf8(&doc_match, "email", -1, "user@example.com", -1);

    bson_t doc_nomatch = BSON_INITIALIZER;
    bson_append_utf8(&doc_nomatch, "email", -1, "user@other.com", -1);

    mongoc_matcher_t *matcher = mongoc_matcher_new(&query, &error);
    TEST_ASSERT_NOT_NULL(matcher);

    TEST_ASSERT_TRUE(mongoc_matcher_match(matcher, &doc_match));
    TEST_ASSERT_FALSE(mongoc_matcher_match(matcher, &doc_nomatch));

    mongoc_matcher_destroy(matcher);
    bson_destroy(&query);
    bson_destroy(&doc_match);
    bson_destroy(&doc_nomatch);
    return 0;
}

TEST(matcher_regex_case_insensitive) {
    bson_error_t error;

    /* Zero-initialize stack-allocated bson_t to avoid valgrind warnings about uninitialized padding */
    bson_t query = BSON_INITIALIZER;
    bson_append_regex(&query, "name", -1, "john", "i");

    bson_t doc_match = BSON_INITIALIZER;
    bson_append_utf8(&doc_match, "name", -1, "John Doe", -1);

    bson_t doc_nomatch = BSON_INITIALIZER;
    bson_append_utf8(&doc_nomatch, "name", -1, "Jane Doe", -1);

    mongoc_matcher_t *matcher = mongoc_matcher_new(&query, &error);
    TEST_ASSERT_NOT_NULL(matcher);

    TEST_ASSERT_TRUE(mongoc_matcher_match(matcher, &doc_match));
    TEST_ASSERT_FALSE(mongoc_matcher_match(matcher, &doc_nomatch));

    mongoc_matcher_destroy(matcher);
    bson_destroy(&query);
    bson_destroy(&doc_match);
    bson_destroy(&doc_nomatch);
    return 0;
}

/* ============================================================
 * Tests using compare() from bsoncompare.h
 * ============================================================ */

TEST(compare_regex_json_style) {
    /* Using JSON-style $regex/$options (Extended JSON) */
    bson_error_t error;
    const char *spec_json = "{\"hello\": {\"$regex\": \"world\", \"$options\": \"\"}}";
    const char *doc_json = "{\"hello\": \"hello world\"}";
    const char *doc_nomatch_json = "{\"hello\": \"goodbye\"}";

    bson_t *spec = bson_new_from_json((const uint8_t*)spec_json, -1, &error);
    bson_t *doc_match = bson_new_from_json((const uint8_t*)doc_json, -1, &error);
    bson_t *doc_nomatch = bson_new_from_json((const uint8_t*)doc_nomatch_json, -1, &error);

    TEST_ASSERT_NOT_NULL(spec);
    TEST_ASSERT_NOT_NULL(doc_match);
    TEST_ASSERT_NOT_NULL(doc_nomatch);

    int result_match = compare(
        bson_get_data(spec), spec->len,
        bson_get_data(doc_match), doc_match->len
    );
    int result_nomatch = compare(
        bson_get_data(spec), spec->len,
        bson_get_data(doc_nomatch), doc_nomatch->len
    );

    TEST_ASSERT_EQUAL(1, result_match);
    TEST_ASSERT_EQUAL(0, result_nomatch);

    bson_destroy(spec);
    bson_destroy(doc_match);
    bson_destroy(doc_nomatch);
    return 0;
}

TEST(compare_regex_case_insensitive) {
    bson_error_t error;
    const char *spec_json = "{\"name\": {\"$regex\": \"JOHN\", \"$options\": \"i\"}}";
    const char *doc_json = "{\"name\": \"john doe\"}";

    bson_t *spec = bson_new_from_json((const uint8_t*)spec_json, -1, &error);
    bson_t *doc = bson_new_from_json((const uint8_t*)doc_json, -1, &error);

    TEST_ASSERT_NOT_NULL(spec);
    TEST_ASSERT_NOT_NULL(doc);

    int result = compare(
        bson_get_data(spec), spec->len,
        bson_get_data(doc), doc->len
    );

    TEST_ASSERT_EQUAL(1, result);

    bson_destroy(spec);
    bson_destroy(doc);
    return 0;
}

TEST(matcher_nested_field) {
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
    TEST_ASSERT_NOT_NULL(matcher);

    TEST_ASSERT_TRUE(mongoc_matcher_match(matcher, doc_match));
    TEST_ASSERT_FALSE(mongoc_matcher_match(matcher, doc_nomatch));

    mongoc_matcher_destroy(matcher);
    bson_destroy(query);
    bson_destroy(doc_match);
    bson_destroy(doc_nomatch);
    return 0;
}

/* ============================================================
 * MAIN
 * ============================================================ */

/* Cleanup function - declared in bsoncompare.h */
extern int regex_destroy(void);

TEST_SUITE_BEGIN("bsonmatch tests")

    RUN_TEST(test_matcher_create_destroy);
    RUN_TEST(test_matcher_simple_match);
    RUN_TEST(test_matcher_gt_operator);
    RUN_TEST(test_matcher_in_operator);
    RUN_TEST(test_matcher_and_operator);
    RUN_TEST(test_matcher_regex);
    RUN_TEST(test_matcher_regex_case_insensitive);
    RUN_TEST(test_matcher_nested_field);

    /* Tests using compare() with JSON-style $regex */
    RUN_TEST(test_compare_regex_json_style);
    RUN_TEST(test_compare_regex_case_insensitive);

    /* Cleanup global regex cache to avoid memory leak */
    regex_destroy();

TEST_SUITE_END()
