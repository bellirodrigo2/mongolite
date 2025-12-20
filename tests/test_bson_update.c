/**
 * test_bson_update.c - Unit tests for BSON update operators
 *
 * Tests each update operator in isolation for correctness.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <bson/bson.h>
#include "bson_update.h"
#include "gerror.h"

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) static void test_##name(void)
#define RUN_TEST(name) do { \
    printf("  Testing %s... ", #name); \
    tests_run++; \
    test_##name(); \
    tests_passed++; \
    printf("PASSED\n"); \
} while(0)

/* ============================================================
 * Helper: Create document from JSON
 * ============================================================ */

static bson_t* doc_from_json(const char *json) {
    bson_error_t err;
    bson_t *doc = bson_new_from_json((const uint8_t*)json, -1, &err);
    if (!doc) {
        fprintf(stderr, "Failed to parse JSON: %s\n", err.message);
        abort();
    }
    return doc;
}

/* ============================================================
 * Helper: Check if field has expected value
 * ============================================================ */

static bool has_int32_field(const bson_t *doc, const char *field, int32_t expected) {
    bson_iter_t iter;
    if (!bson_iter_init_find(&iter, doc, field)) return false;
    if (!BSON_ITER_HOLDS_INT32(&iter)) return false;
    return bson_iter_int32(&iter) == expected;
}

static bool has_utf8_field(const bson_t *doc, const char *field, const char *expected) {
    bson_iter_t iter;
    if (!bson_iter_init_find(&iter, doc, field)) return false;
    if (!BSON_ITER_HOLDS_UTF8(&iter)) return false;
    return strcmp(bson_iter_utf8(&iter, NULL), expected) == 0;
}

static bool has_field(const bson_t *doc, const char *field) {
    bson_iter_t iter;
    return bson_iter_init_find(&iter, doc, field);
}

/* ============================================================
 * $set tests
 * ============================================================ */

TEST(set_new_field) {
    bson_t *doc = doc_from_json("{\"name\": \"test\"}");
    bson_t *update = doc_from_json("{\"$set\": {\"age\": 25}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$set");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_set(doc, &iter, &error);

    assert(result != NULL);
    assert(has_utf8_field(result, "name", "test"));
    assert(has_int32_field(result, "age", 25));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

TEST(set_existing_field) {
    bson_t *doc = doc_from_json("{\"name\": \"old\", \"count\": 10}");
    bson_t *update = doc_from_json("{\"$set\": {\"name\": \"new\"}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$set");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_set(doc, &iter, &error);

    assert(result != NULL);
    assert(has_utf8_field(result, "name", "new"));
    assert(has_int32_field(result, "count", 10));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

TEST(set_multiple_fields) {
    bson_t *doc = doc_from_json("{\"a\": 1}");
    bson_t *update = doc_from_json("{\"$set\": {\"b\": 2, \"c\": 3}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$set");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_set(doc, &iter, &error);

    assert(result != NULL);
    assert(has_int32_field(result, "a", 1));
    assert(has_int32_field(result, "b", 2));
    assert(has_int32_field(result, "c", 3));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

/* ============================================================
 * $unset tests
 * ============================================================ */

TEST(unset_field) {
    bson_t *doc = doc_from_json("{\"a\": 1, \"b\": 2, \"c\": 3}");
    bson_t *update = doc_from_json("{\"$unset\": {\"b\": 1}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$unset");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_unset(doc, &iter, &error);

    assert(result != NULL);
    assert(has_int32_field(result, "a", 1));
    assert(!has_field(result, "b"));
    assert(has_int32_field(result, "c", 3));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

TEST(unset_multiple_fields) {
    bson_t *doc = doc_from_json("{\"a\": 1, \"b\": 2, \"c\": 3, \"d\": 4}");
    bson_t *update = doc_from_json("{\"$unset\": {\"b\": 1, \"d\": 1}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$unset");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_unset(doc, &iter, &error);

    assert(result != NULL);
    assert(has_int32_field(result, "a", 1));
    assert(!has_field(result, "b"));
    assert(has_int32_field(result, "c", 3));
    assert(!has_field(result, "d"));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

TEST(unset_nonexistent) {
    bson_t *doc = doc_from_json("{\"a\": 1}");
    bson_t *update = doc_from_json("{\"$unset\": {\"z\": 1}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$unset");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_unset(doc, &iter, &error);

    assert(result != NULL);
    assert(has_int32_field(result, "a", 1));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

/* ============================================================
 * $inc tests
 * ============================================================ */

TEST(inc_existing_field) {
    bson_t *doc = doc_from_json("{\"count\": 10}");
    bson_t *update = doc_from_json("{\"$inc\": {\"count\": 5}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$inc");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_inc(doc, &iter, &error);

    assert(result != NULL);
    assert(has_int32_field(result, "count", 15));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

TEST(inc_new_field) {
    bson_t *doc = doc_from_json("{\"a\": 1}");
    bson_t *update = doc_from_json("{\"$inc\": {\"count\": 5}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$inc");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_inc(doc, &iter, &error);

    assert(result != NULL);
    assert(has_int32_field(result, "a", 1));
    assert(has_int32_field(result, "count", 5));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

TEST(inc_negative) {
    bson_t *doc = doc_from_json("{\"count\": 10}");
    bson_t *update = doc_from_json("{\"$inc\": {\"count\": -3}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$inc");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_inc(doc, &iter, &error);

    assert(result != NULL);
    assert(has_int32_field(result, "count", 7));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

/* ============================================================
 * $rename tests
 * ============================================================ */

TEST(rename_field) {
    bson_t *doc = doc_from_json("{\"old_name\": \"value\", \"other\": 1}");
    bson_t *update = doc_from_json("{\"$rename\": {\"old_name\": \"new_name\"}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$rename");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_rename(doc, &iter, &error);

    assert(result != NULL);
    assert(!has_field(result, "old_name"));
    assert(has_utf8_field(result, "new_name", "value"));
    assert(has_int32_field(result, "other", 1));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

TEST(rename_nonexistent) {
    bson_t *doc = doc_from_json("{\"a\": 1}");
    bson_t *update = doc_from_json("{\"$rename\": {\"z\": \"new_z\"}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$rename");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_rename(doc, &iter, &error);

    assert(result != NULL);
    assert(has_int32_field(result, "a", 1));
    assert(!has_field(result, "z"));
    assert(!has_field(result, "new_z"));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

/* ============================================================
 * $push tests
 * ============================================================ */

TEST(push_to_existing_array) {
    bson_t *doc = doc_from_json("{\"items\": [1, 2]}");
    bson_t *update = doc_from_json("{\"$push\": {\"items\": 3}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$push");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_push(doc, &iter, &error);

    assert(result != NULL);
    assert(has_field(result, "items"));

    /* Check array has 3 elements by counting */
    bson_iter_t arr_iter, child_iter;
    bson_iter_init_find(&arr_iter, result, "items");
    bson_iter_recurse(&arr_iter, &child_iter);
    int count = 0;
    while (bson_iter_next(&child_iter)) count++;
    assert(count == 3);

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

TEST(push_to_new_field) {
    bson_t *doc = doc_from_json("{\"a\": 1}");
    bson_t *update = doc_from_json("{\"$push\": {\"items\": 1}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$push");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_push(doc, &iter, &error);

    assert(result != NULL);
    assert(has_int32_field(result, "a", 1));
    assert(has_field(result, "items"));

    /* Check array has 1 element by counting */
    bson_iter_t arr_iter, child_iter;
    bson_iter_init_find(&arr_iter, result, "items");
    bson_iter_recurse(&arr_iter, &child_iter);
    int count = 0;
    while (bson_iter_next(&child_iter)) count++;
    assert(count == 1);

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

/* ============================================================
 * $pull tests
 * ============================================================ */

TEST(pull_from_array) {
    bson_t *doc = doc_from_json("{\"items\": [1, 2, 3, 2, 4]}");
    bson_t *update = doc_from_json("{\"$pull\": {\"items\": 2}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$pull");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_pull(doc, &iter, &error);

    assert(result != NULL);
    assert(has_field(result, "items"));

    /* Check array has 3 elements (both 2s removed) by counting */
    bson_iter_t arr_iter, child_iter;
    bson_iter_init_find(&arr_iter, result, "items");
    bson_iter_recurse(&arr_iter, &child_iter);
    int count = 0;
    while (bson_iter_next(&child_iter)) count++;
    assert(count == 3);

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

/* ============================================================
 * Combined update tests
 * ============================================================ */

TEST(apply_combined_update) {
    bson_t *doc = doc_from_json("{\"name\": \"test\", \"count\": 10, \"remove_me\": 1}");
    bson_t *update = doc_from_json("{\"$set\": {\"name\": \"updated\"}, \"$inc\": {\"count\": 5}, \"$unset\": {\"remove_me\": 1}}");

    gerror_t error = {0};
    bson_t *result = bson_update_apply(doc, update, &error);

    assert(result != NULL);
    assert(has_utf8_field(result, "name", "updated"));
    assert(has_int32_field(result, "count", 15));
    assert(!has_field(result, "remove_me"));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

/* ============================================================
 * Utility function tests
 * ============================================================ */

TEST(is_update_spec_true) {
    bson_t *update = doc_from_json("{\"$set\": {\"a\": 1}}");
    assert(bson_update_is_update_spec(update) == true);
    bson_destroy(update);
}

TEST(is_update_spec_false) {
    bson_t *doc = doc_from_json("{\"name\": \"test\"}");
    assert(bson_update_is_update_spec(doc) == false);
    bson_destroy(doc);
}

TEST(is_update_spec_mixed) {
    bson_t *doc = doc_from_json("{\"$set\": {\"a\": 1}, \"b\": 2}");
    assert(bson_update_is_update_spec(doc) == false);
    bson_destroy(doc);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    printf("Running bson_update tests...\n\n");

    printf("$set operator:\n");
    RUN_TEST(set_new_field);
    RUN_TEST(set_existing_field);
    RUN_TEST(set_multiple_fields);

    printf("\n$unset operator:\n");
    RUN_TEST(unset_field);
    RUN_TEST(unset_multiple_fields);
    RUN_TEST(unset_nonexistent);

    printf("\n$inc operator:\n");
    RUN_TEST(inc_existing_field);
    RUN_TEST(inc_new_field);
    RUN_TEST(inc_negative);

    printf("\n$rename operator:\n");
    RUN_TEST(rename_field);
    RUN_TEST(rename_nonexistent);

    printf("\n$push operator:\n");
    RUN_TEST(push_to_existing_array);
    RUN_TEST(push_to_new_field);

    printf("\n$pull operator:\n");
    RUN_TEST(pull_from_array);

    printf("\nCombined updates:\n");
    RUN_TEST(apply_combined_update);

    printf("\nUtility functions:\n");
    RUN_TEST(is_update_spec_true);
    RUN_TEST(is_update_spec_false);
    RUN_TEST(is_update_spec_mixed);

    printf("\n========================================\n");
    printf("Tests: %d/%d passed\n", tests_passed, tests_run);

    return tests_passed == tests_run ? 0 : 1;
}
