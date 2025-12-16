/*
 * test_mongolite_update.c - Tests for Phase 6: Update operations
 *
 * Tests:
 * - $set operator
 * - $unset operator
 * - $inc operator
 * - $push operator
 * - $pull operator
 * - $rename operator
 * - update_one
 * - update_many
 * - replace_one
 * - JSON wrappers
 */

#include "test_runner.h"
#include "mongolite_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

/* Test database path */
#define TEST_DB_PATH "./test_mongolite_update"

/* ============================================================
 * Helper: Remove test database directory
 * ============================================================ */

static void cleanup_test_db(void) {
#ifdef _WIN32
    system("rmdir /s /q " TEST_DB_PATH " 2>nul");
#else
    system("rm -rf " TEST_DB_PATH);
#endif
}

/* ============================================================
 * Helper: Setup test database with sample data
 * ============================================================ */

static mongolite_db_t* setup_test_db(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    if (mongolite_open(TEST_DB_PATH, &db, NULL, &error) != 0) {
        printf("Failed to open db: %s\n", error.message);
        return NULL;
    }

    if (mongolite_collection_create(db, "users", NULL, &error) != 0) {
        printf("Failed to create collection: %s\n", error.message);
        mongolite_close(db);
        return NULL;
    }

    return db;
}

/* ============================================================
 * Test: $set operator
 * ============================================================ */

static int test_set_operator(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Insert test document */
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users",
        "{\"name\": \"Alice\", \"age\": 30}", &id, &error);
    TEST_ASSERT(rc == 0, "insert should succeed");

    /* Update with $set */
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *update = bson_new();
    bson_t set_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
    BSON_APPEND_INT32(&set_doc, "age", 31);
    BSON_APPEND_UTF8(&set_doc, "city", "NYC");
    bson_append_document_end(update, &set_doc);

    rc = mongolite_update_one(db, "users", filter, update, false, &error);
    TEST_ASSERT(rc == 0, "update should succeed: %s", error.message);

    bson_destroy(filter);
    bson_destroy(update);

    /* Verify changes */
    filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    TEST_ASSERT(found != NULL, "should find document");

    bson_iter_t iter;
    TEST_ASSERT(bson_iter_init_find(&iter, found, "age"), "should have age");
    TEST_ASSERT(bson_iter_int32(&iter) == 31, "age should be 31");

    TEST_ASSERT(bson_iter_init_find(&iter, found, "city"), "should have city");
    TEST_ASSERT(strcmp(bson_iter_utf8(&iter, NULL), "NYC") == 0, "city should be NYC");

    printf("  Set operator verified: age=31, city=NYC\n");

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: $unset operator
 * ============================================================ */

static int test_unset_operator(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Insert test document */
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users",
        "{\"name\": \"Bob\", \"age\": 25, \"city\": \"LA\"}", &id, &error);
    TEST_ASSERT(rc == 0, "insert should succeed");

    /* Update with $unset */
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *update = bson_new();
    bson_t unset_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$unset", &unset_doc);
    BSON_APPEND_INT32(&unset_doc, "city", 1);  /* Value doesn't matter */
    bson_append_document_end(update, &unset_doc);

    rc = mongolite_update_one(db, "users", filter, update, false, &error);
    TEST_ASSERT(rc == 0, "update should succeed");

    bson_destroy(filter);
    bson_destroy(update);

    /* Verify city is removed */
    filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    TEST_ASSERT(found != NULL, "should find document");

    bson_iter_t iter;
    TEST_ASSERT(!bson_iter_init_find(&iter, found, "city"), "city should be removed");
    TEST_ASSERT(bson_iter_init_find(&iter, found, "name"), "name should still exist");
    TEST_ASSERT(bson_iter_init_find(&iter, found, "age"), "age should still exist");

    printf("  Unset operator verified: city removed\n");

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: $inc operator
 * ============================================================ */

static int test_inc_operator(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Insert test document */
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users",
        "{\"name\": \"Charlie\", \"age\": 35, \"score\": 100}", &id, &error);
    TEST_ASSERT(rc == 0, "insert should succeed");

    /* Update with $inc */
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *update = bson_new();
    bson_t inc_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$inc", &inc_doc);
    BSON_APPEND_INT32(&inc_doc, "age", 1);
    BSON_APPEND_INT32(&inc_doc, "score", 50);
    bson_append_document_end(update, &inc_doc);

    rc = mongolite_update_one(db, "users", filter, update, false, &error);
    TEST_ASSERT(rc == 0, "update should succeed");

    bson_destroy(filter);
    bson_destroy(update);

    /* Verify increments */
    filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    TEST_ASSERT(found != NULL, "should find document");

    bson_iter_t iter;
    TEST_ASSERT(bson_iter_init_find(&iter, found, "age"), "should have age");
    TEST_ASSERT(bson_iter_int32(&iter) == 36, "age should be 36");

    TEST_ASSERT(bson_iter_init_find(&iter, found, "score"), "should have score");
    TEST_ASSERT(bson_iter_int32(&iter) == 150, "score should be 150");

    printf("  Inc operator verified: age=36, score=150\n");

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: $push operator
 * ============================================================ */

static int test_push_operator(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Insert test document with array */
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users",
        "{\"name\": \"Diana\", \"tags\": [\"developer\", \"admin\"]}", &id, &error);
    TEST_ASSERT(rc == 0, "insert should succeed");

    /* Update with $push */
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *update = bson_new();
    bson_t push_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$push", &push_doc);
    BSON_APPEND_UTF8(&push_doc, "tags", "user");
    bson_append_document_end(update, &push_doc);

    rc = mongolite_update_one(db, "users", filter, update, false, &error);
    TEST_ASSERT(rc == 0, "update should succeed: %s", error.message);

    bson_destroy(filter);
    bson_destroy(update);

    /* Verify new element added */
    filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    TEST_ASSERT(found != NULL, "should find document");

    bson_iter_t iter, array_iter;
    TEST_ASSERT(bson_iter_init_find(&iter, found, "tags"), "should have tags");
    TEST_ASSERT(BSON_ITER_HOLDS_ARRAY(&iter), "tags should be array");

    bson_iter_recurse(&iter, &array_iter);
    int count = 0;
    while (bson_iter_next(&array_iter)) {
        count++;
    }
    TEST_ASSERT(count == 3, "tags should have 3 elements, got %d", count);

    printf("  Push operator verified: 3 tags\n");

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: $pull operator
 * ============================================================ */

static int test_pull_operator(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Insert test document with array */
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users",
        "{\"name\": \"Eve\", \"tags\": [\"developer\", \"admin\", \"user\"]}", &id, &error);
    TEST_ASSERT(rc == 0, "insert should succeed");

    /* Update with $pull */
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *update = bson_new();
    bson_t pull_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$pull", &pull_doc);
    BSON_APPEND_UTF8(&pull_doc, "tags", "admin");
    bson_append_document_end(update, &pull_doc);

    rc = mongolite_update_one(db, "users", filter, update, false, &error);
    TEST_ASSERT(rc == 0, "update should succeed: %s", error.message);

    bson_destroy(filter);
    bson_destroy(update);

    /* Verify element removed */
    filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    TEST_ASSERT(found != NULL, "should find document");

    bson_iter_t iter, array_iter;
    TEST_ASSERT(bson_iter_init_find(&iter, found, "tags"), "should have tags");

    bson_iter_recurse(&iter, &array_iter);
    int count = 0;
    bool has_admin = false;
    while (bson_iter_next(&array_iter)) {
        count++;
        if (strcmp(bson_iter_utf8(&array_iter, NULL), "admin") == 0) {
            has_admin = true;
        }
    }

    TEST_ASSERT(count == 2, "tags should have 2 elements");
    TEST_ASSERT(!has_admin, "admin should be removed");

    printf("  Pull operator verified: admin removed, 2 tags remain\n");

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: $rename operator
 * ============================================================ */

static int test_rename_operator(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Insert test document */
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users",
        "{\"name\": \"Frank\", \"old_field\": \"test_value\"}", &id, &error);
    TEST_ASSERT(rc == 0, "insert should succeed");

    /* Update with $rename */
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *update = bson_new();
    bson_t rename_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$rename", &rename_doc);
    BSON_APPEND_UTF8(&rename_doc, "old_field", "new_field");
    bson_append_document_end(update, &rename_doc);

    rc = mongolite_update_one(db, "users", filter, update, false, &error);
    TEST_ASSERT(rc == 0, "update should succeed: %s", error.message);

    bson_destroy(filter);
    bson_destroy(update);

    /* Verify field renamed */
    filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    TEST_ASSERT(found != NULL, "should find document");

    bson_iter_t iter;
    TEST_ASSERT(!bson_iter_init_find(&iter, found, "old_field"), "old_field should not exist");
    TEST_ASSERT(bson_iter_init_find(&iter, found, "new_field"), "new_field should exist");
    TEST_ASSERT(strcmp(bson_iter_utf8(&iter, NULL), "test_value") == 0, "value should match");

    printf("  Rename operator verified: old_field -> new_field\n");

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: update_one
 * ============================================================ */

static int test_update_one(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Insert multiple documents */
    for (int i = 0; i < 3; i++) {
        int rc = mongolite_insert_one_json(db, "users",
            "{\"name\": \"User\", \"value\": 10}", NULL, &error);
        TEST_ASSERT(rc == 0, "insert should succeed");
    }

    /* Update only first match */
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "name", "User");

    bson_t *update = bson_new();
    bson_t set_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
    BSON_APPEND_INT32(&set_doc, "value", 99);
    bson_append_document_end(update, &set_doc);

    int rc = mongolite_update_one(db, "users", filter, update, false, &error);
    TEST_ASSERT(rc == 0, "update_one should succeed");

    bson_destroy(filter);
    bson_destroy(update);

    /* Count documents with value=99 */
    filter = bson_new();
    BSON_APPEND_INT32(filter, "value", 99);

    int64_t count = mongolite_collection_count(db, "users", filter, &error);
    TEST_ASSERT(count == 1, "only one document should be updated, got %lld", (long long)count);

    printf("  update_one verified: only 1 of 3 updated\n");

    bson_destroy(filter);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: update_many
 * ============================================================ */

static int test_update_many(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Insert multiple documents */
    for (int i = 0; i < 5; i++) {
        int rc = mongolite_insert_one_json(db, "users",
            "{\"category\": \"test\", \"value\": 10}", NULL, &error);
        TEST_ASSERT(rc == 0, "insert should succeed");
    }

    /* Update all matches */
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "category", "test");

    bson_t *update = bson_new();
    bson_t inc_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$inc", &inc_doc);
    BSON_APPEND_INT32(&inc_doc, "value", 5);
    bson_append_document_end(update, &inc_doc);

    int64_t modified_count = 0;
    int rc = mongolite_update_many(db, "users", filter, update, false, &modified_count, &error);
    TEST_ASSERT(rc == 0, "update_many should succeed: %s", error.message);
    TEST_ASSERT(modified_count == 5, "should modify 5 docs, got %lld", (long long)modified_count);

    bson_destroy(filter);
    bson_destroy(update);

    /* Verify all updated */
    filter = bson_new();
    BSON_APPEND_INT32(filter, "value", 15);

    int64_t count = mongolite_collection_count(db, "users", filter, &error);
    TEST_ASSERT(count == 5, "all 5 should have value=15, got %lld", (long long)count);

    printf("  update_many verified: all 5 updated\n");

    bson_destroy(filter);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: replace_one
 * ============================================================ */

static int test_replace_one(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Insert test document */
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users",
        "{\"name\": \"Grace\", \"age\": 27, \"city\": \"NYC\"}", &id, &error);
    TEST_ASSERT(rc == 0, "insert should succeed");

    /* Replace entire document */
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *replacement = bson_new();
    BSON_APPEND_UTF8(replacement, "name", "Grace Updated");
    BSON_APPEND_INT32(replacement, "status", 1);

    rc = mongolite_replace_one(db, "users", filter, replacement, false, &error);
    TEST_ASSERT(rc == 0, "replace_one should succeed: %s", error.message);

    bson_destroy(filter);
    bson_destroy(replacement);

    /* Verify replacement */
    filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    TEST_ASSERT(found != NULL, "should find document");

    bson_iter_t iter;
    TEST_ASSERT(bson_iter_init_find(&iter, found, "name"), "should have name");
    TEST_ASSERT(strcmp(bson_iter_utf8(&iter, NULL), "Grace Updated") == 0, "name should be updated");

    TEST_ASSERT(bson_iter_init_find(&iter, found, "status"), "should have status");
    TEST_ASSERT(bson_iter_int32(&iter) == 1, "status should be 1");

    TEST_ASSERT(!bson_iter_init_find(&iter, found, "age"), "age should not exist");
    TEST_ASSERT(!bson_iter_init_find(&iter, found, "city"), "city should not exist");

    /* _id should be preserved */
    TEST_ASSERT(bson_iter_init_find(&iter, found, "_id"), "should have _id");
    bson_oid_t found_id;
    bson_oid_copy(bson_iter_oid(&iter), &found_id);
    TEST_ASSERT(bson_oid_equal(&id, &found_id), "_id should be preserved");

    printf("  replace_one verified: document replaced, _id preserved\n");

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: JSON wrappers
 * ============================================================ */

static int test_json_wrappers(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Insert test document */
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users",
        "{\"name\": \"Henry\", \"age\": 45}", &id, &error);
    TEST_ASSERT(rc == 0, "insert should succeed");

    /* Update using JSON */
    char oid_str[25];
    bson_oid_to_string(&id, oid_str);

    char filter_json[256];
    snprintf(filter_json, sizeof(filter_json), "{\"_id\": {\"$oid\": \"%s\"}}", oid_str);

    rc = mongolite_update_one_json(db, "users", filter_json,
        "{\"$set\": {\"age\": 46}}", false, &error);
    TEST_ASSERT(rc == 0, "update_one_json should succeed: %s", error.message);

    /* Verify */
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    TEST_ASSERT(found != NULL, "should find document");

    bson_iter_t iter;
    TEST_ASSERT(bson_iter_init_find(&iter, found, "age"), "should have age");
    TEST_ASSERT(bson_iter_int32(&iter) == 46, "age should be 46");

    printf("  JSON wrappers verified\n");

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Combined operators
 * ============================================================ */

static int test_combined_operators(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Insert test document */
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users",
        "{\"name\": \"Test\", \"age\": 30, \"score\": 100, \"old_field\": \"x\"}", &id, &error);
    TEST_ASSERT(rc == 0, "insert should succeed");

    /* Apply multiple operators */
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *update = bson_new();

    /* $set */
    bson_t set_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
    BSON_APPEND_UTF8(&set_doc, "name", "Test Updated");
    bson_append_document_end(update, &set_doc);

    /* $inc */
    bson_t inc_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$inc", &inc_doc);
    BSON_APPEND_INT32(&inc_doc, "age", 1);
    BSON_APPEND_INT32(&inc_doc, "score", 50);
    bson_append_document_end(update, &inc_doc);

    /* $unset */
    bson_t unset_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$unset", &unset_doc);
    BSON_APPEND_INT32(&unset_doc, "old_field", 1);
    bson_append_document_end(update, &unset_doc);

    rc = mongolite_update_one(db, "users", filter, update, false, &error);
    TEST_ASSERT(rc == 0, "combined update should succeed: %s", error.message);

    bson_destroy(filter);
    bson_destroy(update);

    /* Verify all changes */
    filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    TEST_ASSERT(found != NULL, "should find document");

    bson_iter_t iter;
    TEST_ASSERT(bson_iter_init_find(&iter, found, "name"), "should have name");
    TEST_ASSERT(strcmp(bson_iter_utf8(&iter, NULL), "Test Updated") == 0, "name updated");

    TEST_ASSERT(bson_iter_init_find(&iter, found, "age"), "should have age");
    TEST_ASSERT(bson_iter_int32(&iter) == 31, "age incremented");

    TEST_ASSERT(bson_iter_init_find(&iter, found, "score"), "should have score");
    TEST_ASSERT(bson_iter_int32(&iter) == 150, "score incremented");

    TEST_ASSERT(!bson_iter_init_find(&iter, found, "old_field"), "old_field removed");

    printf("  Combined operators verified\n");

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    printf("=== Mongolite Update Tests (Phase 6) ===\n\n");

    RUN_TEST(test_set_operator);
    RUN_TEST(test_unset_operator);
    RUN_TEST(test_inc_operator);
    RUN_TEST(test_push_operator);
    RUN_TEST(test_pull_operator);
    RUN_TEST(test_rename_operator);
    RUN_TEST(test_update_one);
    RUN_TEST(test_update_many);
    RUN_TEST(test_replace_one);
    RUN_TEST(test_json_wrappers);
    RUN_TEST(test_combined_operators);

    printf("\n=== All tests passed! ===\n");
    return 0;
}
