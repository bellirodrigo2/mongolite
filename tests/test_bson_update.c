// test_bson_update.c - Unit tests for BSON update operators (cmocka)

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <stdlib.h>
#include <bson/bson.h>

#include "bson_update.h"
#include "gerror.h"

/* ============================================================
 * Helper: Create document from JSON
 * ============================================================ */

static bson_t* doc_from_json(const char *json) {
    bson_error_t err;
    bson_t *doc = bson_new_from_json((const uint8_t*)json, -1, &err);
    assert_non_null(doc);
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

static void test_set_new_field(void **state) {
    (void)state;
    bson_t *doc = doc_from_json("{\"name\": \"test\"}");
    bson_t *update = doc_from_json("{\"$set\": {\"age\": 25}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$set");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_set(doc, &iter, &error);

    assert_non_null(result);
    assert_true(has_utf8_field(result, "name", "test"));
    assert_true(has_int32_field(result, "age", 25));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

static void test_set_existing_field(void **state) {
    (void)state;
    bson_t *doc = doc_from_json("{\"name\": \"old\", \"count\": 10}");
    bson_t *update = doc_from_json("{\"$set\": {\"name\": \"new\"}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$set");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_set(doc, &iter, &error);

    assert_non_null(result);
    assert_true(has_utf8_field(result, "name", "new"));
    assert_true(has_int32_field(result, "count", 10));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

static void test_set_multiple_fields(void **state) {
    (void)state;
    bson_t *doc = doc_from_json("{\"a\": 1}");
    bson_t *update = doc_from_json("{\"$set\": {\"b\": 2, \"c\": 3}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$set");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_set(doc, &iter, &error);

    assert_non_null(result);
    assert_true(has_int32_field(result, "a", 1));
    assert_true(has_int32_field(result, "b", 2));
    assert_true(has_int32_field(result, "c", 3));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

/* ============================================================
 * $unset tests
 * ============================================================ */

static void test_unset_field(void **state) {
    (void)state;
    bson_t *doc = doc_from_json("{\"a\": 1, \"b\": 2, \"c\": 3}");
    bson_t *update = doc_from_json("{\"$unset\": {\"b\": 1}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$unset");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_unset(doc, &iter, &error);

    assert_non_null(result);
    assert_true(has_int32_field(result, "a", 1));
    assert_false(has_field(result, "b"));
    assert_true(has_int32_field(result, "c", 3));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

static void test_unset_multiple_fields(void **state) {
    (void)state;
    bson_t *doc = doc_from_json("{\"a\": 1, \"b\": 2, \"c\": 3, \"d\": 4}");
    bson_t *update = doc_from_json("{\"$unset\": {\"b\": 1, \"d\": 1}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$unset");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_unset(doc, &iter, &error);

    assert_non_null(result);
    assert_true(has_int32_field(result, "a", 1));
    assert_false(has_field(result, "b"));
    assert_true(has_int32_field(result, "c", 3));
    assert_false(has_field(result, "d"));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

static void test_unset_nonexistent(void **state) {
    (void)state;
    bson_t *doc = doc_from_json("{\"a\": 1}");
    bson_t *update = doc_from_json("{\"$unset\": {\"z\": 1}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$unset");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_unset(doc, &iter, &error);

    assert_non_null(result);
    assert_true(has_int32_field(result, "a", 1));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

/* ============================================================
 * $inc tests
 * ============================================================ */

static void test_inc_existing_field(void **state) {
    (void)state;
    bson_t *doc = doc_from_json("{\"count\": 10}");
    bson_t *update = doc_from_json("{\"$inc\": {\"count\": 5}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$inc");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_inc(doc, &iter, &error);

    assert_non_null(result);
    assert_true(has_int32_field(result, "count", 15));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

static void test_inc_new_field(void **state) {
    (void)state;
    bson_t *doc = doc_from_json("{\"a\": 1}");
    bson_t *update = doc_from_json("{\"$inc\": {\"count\": 5}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$inc");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_inc(doc, &iter, &error);

    assert_non_null(result);
    assert_true(has_int32_field(result, "a", 1));
    assert_true(has_int32_field(result, "count", 5));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

static void test_inc_negative(void **state) {
    (void)state;
    bson_t *doc = doc_from_json("{\"count\": 10}");
    bson_t *update = doc_from_json("{\"$inc\": {\"count\": -3}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$inc");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_inc(doc, &iter, &error);

    assert_non_null(result);
    assert_true(has_int32_field(result, "count", 7));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

/* ============================================================
 * $rename tests
 * ============================================================ */

static void test_rename_field(void **state) {
    (void)state;
    bson_t *doc = doc_from_json("{\"old_name\": \"value\", \"other\": 1}");
    bson_t *update = doc_from_json("{\"$rename\": {\"old_name\": \"new_name\"}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$rename");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_rename(doc, &iter, &error);

    assert_non_null(result);
    assert_false(has_field(result, "old_name"));
    assert_true(has_utf8_field(result, "new_name", "value"));
    assert_true(has_int32_field(result, "other", 1));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

static void test_rename_nonexistent(void **state) {
    (void)state;
    bson_t *doc = doc_from_json("{\"a\": 1}");
    bson_t *update = doc_from_json("{\"$rename\": {\"z\": \"new_z\"}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$rename");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_rename(doc, &iter, &error);

    assert_non_null(result);
    assert_true(has_int32_field(result, "a", 1));
    assert_false(has_field(result, "z"));
    assert_false(has_field(result, "new_z"));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

/* ============================================================
 * $push tests
 * ============================================================ */

static void test_push_to_existing_array(void **state) {
    (void)state;
    bson_t *doc = doc_from_json("{\"items\": [1, 2]}");
    bson_t *update = doc_from_json("{\"$push\": {\"items\": 3}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$push");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_push(doc, &iter, &error);

    assert_non_null(result);
    assert_true(has_field(result, "items"));

    /* Check array has 3 elements by counting */
    bson_iter_t arr_iter, child_iter;
    bson_iter_init_find(&arr_iter, result, "items");
    bson_iter_recurse(&arr_iter, &child_iter);
    int count = 0;
    while (bson_iter_next(&child_iter)) count++;
    assert_int_equal(3, count);

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

static void test_push_to_new_field(void **state) {
    (void)state;
    bson_t *doc = doc_from_json("{\"a\": 1}");
    bson_t *update = doc_from_json("{\"$push\": {\"items\": 1}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$push");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_push(doc, &iter, &error);

    assert_non_null(result);
    assert_true(has_int32_field(result, "a", 1));
    assert_true(has_field(result, "items"));

    /* Check array has 1 element by counting */
    bson_iter_t arr_iter, child_iter;
    bson_iter_init_find(&arr_iter, result, "items");
    bson_iter_recurse(&arr_iter, &child_iter);
    int count = 0;
    while (bson_iter_next(&child_iter)) count++;
    assert_int_equal(1, count);

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

/* ============================================================
 * $pull tests
 * ============================================================ */

static void test_pull_from_array(void **state) {
    (void)state;
    bson_t *doc = doc_from_json("{\"items\": [1, 2, 3, 2, 4]}");
    bson_t *update = doc_from_json("{\"$pull\": {\"items\": 2}}");

    bson_iter_t iter;
    bson_iter_init_find(&iter, update, "$pull");

    gerror_t error = {0};
    bson_t *result = bson_update_apply_pull(doc, &iter, &error);

    assert_non_null(result);
    assert_true(has_field(result, "items"));

    /* Check array has 3 elements (both 2s removed) by counting */
    bson_iter_t arr_iter, child_iter;
    bson_iter_init_find(&arr_iter, result, "items");
    bson_iter_recurse(&arr_iter, &child_iter);
    int count = 0;
    while (bson_iter_next(&child_iter)) count++;
    assert_int_equal(3, count);

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

/* ============================================================
 * Combined update tests
 * ============================================================ */

static void test_apply_combined_update(void **state) {
    (void)state;
    bson_t *doc = doc_from_json("{\"name\": \"test\", \"count\": 10, \"remove_me\": 1}");
    bson_t *update = doc_from_json("{\"$set\": {\"name\": \"updated\"}, \"$inc\": {\"count\": 5}, \"$unset\": {\"remove_me\": 1}}");

    gerror_t error = {0};
    bson_t *result = bson_update_apply(doc, update, &error);

    assert_non_null(result);
    assert_true(has_utf8_field(result, "name", "updated"));
    assert_true(has_int32_field(result, "count", 15));
    assert_false(has_field(result, "remove_me"));

    bson_destroy(result);
    bson_destroy(update);
    bson_destroy(doc);
}

/* ============================================================
 * Utility function tests
 * ============================================================ */

static void test_is_update_spec_true(void **state) {
    (void)state;
    bson_t *update = doc_from_json("{\"$set\": {\"a\": 1}}");
    assert_true(bson_update_is_update_spec(update));
    bson_destroy(update);
}

static void test_is_update_spec_false(void **state) {
    (void)state;
    bson_t *doc = doc_from_json("{\"name\": \"test\"}");
    assert_false(bson_update_is_update_spec(doc));
    bson_destroy(doc);
}

static void test_is_update_spec_mixed(void **state) {
    (void)state;
    bson_t *doc = doc_from_json("{\"$set\": {\"a\": 1}, \"b\": 2}");
    assert_false(bson_update_is_update_spec(doc));
    bson_destroy(doc);
}

/* ============================================================
 * ID preservation test
 * ============================================================ */

static void test_id_preserved(void **state) {
    (void)state;
    bson_oid_t oid;
    bson_oid_init(&oid, NULL);

    bson_t *doc = bson_new();
    BSON_APPEND_OID(doc, "_id", &oid);
    BSON_APPEND_UTF8(doc, "name", "Alice");

    bson_t *update = doc_from_json("{\"$set\": {\"name\": \"Bob\"}}");

    gerror_t error = {0};
    bson_t *result = bson_update_apply(doc, update, &error);

    assert_non_null(result);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, result, "_id"));

    const bson_oid_t *result_oid = bson_iter_oid(&iter);
    assert_true(bson_oid_equal(&oid, result_oid));

    bson_iter_t name_iter;
    assert_true(bson_iter_init_find(&name_iter, result, "name"));
    assert_string_equal("Bob", bson_iter_utf8(&name_iter, NULL));

    bson_destroy(doc);
    bson_destroy(update);
    bson_destroy(result);
}

/* ============================================================
 * Empty update test
 * ============================================================ */

static void test_empty_update(void **state) {
    (void)state;
    bson_t *doc = doc_from_json("{\"name\": \"Alice\"}");
    bson_t *update = bson_new();

    gerror_t error = {0};
    bson_t *result = bson_update_apply(doc, update, &error);

    assert_non_null(result);
    assert_true(has_utf8_field(result, "name", "Alice"));

    bson_destroy(doc);
    bson_destroy(update);
    bson_destroy(result);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        // $set operator
        cmocka_unit_test(test_set_new_field),
        cmocka_unit_test(test_set_existing_field),
        cmocka_unit_test(test_set_multiple_fields),
        // $unset operator
        cmocka_unit_test(test_unset_field),
        cmocka_unit_test(test_unset_multiple_fields),
        cmocka_unit_test(test_unset_nonexistent),
        // $inc operator
        cmocka_unit_test(test_inc_existing_field),
        cmocka_unit_test(test_inc_new_field),
        cmocka_unit_test(test_inc_negative),
        // $rename operator
        cmocka_unit_test(test_rename_field),
        cmocka_unit_test(test_rename_nonexistent),
        // $push operator
        cmocka_unit_test(test_push_to_existing_array),
        cmocka_unit_test(test_push_to_new_field),
        // $pull operator
        cmocka_unit_test(test_pull_from_array),
        // Combined updates
        cmocka_unit_test(test_apply_combined_update),
        // Utility functions
        cmocka_unit_test(test_is_update_spec_true),
        cmocka_unit_test(test_is_update_spec_false),
        cmocka_unit_test(test_is_update_spec_mixed),
        // ID preservation
        cmocka_unit_test(test_id_preserved),
        // Empty update
        cmocka_unit_test(test_empty_update),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
