/*
 * test_mongolite_insert.c - Tests for Phase 3: Insert operations
 *
 * Tests:
 * - insert_one with/without _id
 * - insert_many
 * - JSON inserts
 * - doc_count updates
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
#define TEST_DB_PATH "./test_mongolite_insert"

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
 * Test: Insert one without _id (auto-generated)
 * ============================================================ */

static int test_insert_one_auto_id(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "open should succeed: %s", error.message);

    rc = mongolite_collection_create(db, "users", NULL, &error);
    TEST_ASSERT(rc == 0, "create collection should succeed");

    /* Create document without _id */
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "Alice");
    BSON_APPEND_INT32(doc, "age", 30);

    bson_oid_t inserted_id;
    rc = mongolite_insert_one(db, "users", doc, &inserted_id, &error);
    TEST_ASSERT(rc == 0, "insert_one should succeed: %s", error.message);

    /* Verify _id was generated */
    char oid_str[25];
    bson_oid_to_string(&inserted_id, oid_str);
    printf("  Inserted _id: %s\n", oid_str);

    /* Verify doc count */
    int64_t count = mongolite_collection_count(db, "users", NULL, &error);
    TEST_ASSERT(count == 1, "count should be 1, got %lld", (long long)count);

    /* Verify changes */
    TEST_ASSERT(mongolite_changes(db) == 1, "changes should be 1");

    bson_destroy(doc);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Insert one with existing _id
 * ============================================================ */

static int test_insert_one_with_id(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    rc = mongolite_collection_create(db, "users", NULL, &error);
    TEST_ASSERT(rc == 0, "create collection should succeed");

    /* Create document with specific _id */
    bson_oid_t my_id;
    bson_oid_init(&my_id, NULL);

    bson_t *doc = bson_new();
    BSON_APPEND_OID(doc, "_id", &my_id);
    BSON_APPEND_UTF8(doc, "name", "Bob");

    bson_oid_t inserted_id;
    rc = mongolite_insert_one(db, "users", doc, &inserted_id, &error);
    TEST_ASSERT(rc == 0, "insert_one should succeed: %s", error.message);

    /* Verify returned _id matches our _id */
    TEST_ASSERT(bson_oid_compare(&inserted_id, &my_id) == 0,
                "returned _id should match provided _id");

    bson_destroy(doc);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Insert duplicate _id should fail
 * ============================================================ */

static int test_insert_duplicate_id(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    rc = mongolite_collection_create(db, "users", NULL, &error);
    TEST_ASSERT(rc == 0, "create collection should succeed");

    /* Create document with specific _id */
    bson_oid_t my_id;
    bson_oid_init(&my_id, NULL);

    bson_t *doc1 = bson_new();
    BSON_APPEND_OID(doc1, "_id", &my_id);
    BSON_APPEND_UTF8(doc1, "name", "First");

    bson_t *doc2 = bson_new();
    BSON_APPEND_OID(doc2, "_id", &my_id);  /* Same _id */
    BSON_APPEND_UTF8(doc2, "name", "Second");

    /* First insert should succeed */
    rc = mongolite_insert_one(db, "users", doc1, NULL, &error);
    TEST_ASSERT(rc == 0, "first insert should succeed");

    /* Second insert should fail (duplicate key) */
    rc = mongolite_insert_one(db, "users", doc2, NULL, &error);
    TEST_ASSERT(rc != 0, "duplicate insert should fail");

    /* Count should still be 1 */
    int64_t count = mongolite_collection_count(db, "users", NULL, &error);
    TEST_ASSERT(count == 1, "count should be 1 after failed duplicate");

    bson_destroy(doc1);
    bson_destroy(doc2);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Insert many
 * ============================================================ */

static int test_insert_many(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    rc = mongolite_collection_create(db, "products", NULL, &error);
    TEST_ASSERT(rc == 0, "create collection should succeed");

    /* Create array of documents */
    bson_t *docs[3];
    docs[0] = bson_new();
    BSON_APPEND_UTF8(docs[0], "name", "Apple");
    BSON_APPEND_DOUBLE(docs[0], "price", 1.50);

    docs[1] = bson_new();
    BSON_APPEND_UTF8(docs[1], "name", "Banana");
    BSON_APPEND_DOUBLE(docs[1], "price", 0.75);

    docs[2] = bson_new();
    BSON_APPEND_UTF8(docs[2], "name", "Cherry");
    BSON_APPEND_DOUBLE(docs[2], "price", 3.00);

    bson_oid_t *inserted_ids = NULL;
    rc = mongolite_insert_many(db, "products", (const bson_t**)docs, 3,
                                &inserted_ids, &error);
    TEST_ASSERT(rc == 0, "insert_many should succeed: %s", error.message);
    TEST_ASSERT(inserted_ids != NULL, "should return inserted_ids");

    /* Print inserted IDs */
    printf("  Inserted IDs: ");
    for (int i = 0; i < 3; i++) {
        char oid_str[25];
        bson_oid_to_string(&inserted_ids[i], oid_str);
        printf("%s ", oid_str);
    }
    printf("\n");

    /* Verify count */
    int64_t count = mongolite_collection_count(db, "products", NULL, &error);
    TEST_ASSERT(count == 3, "count should be 3, got %lld", (long long)count);

    /* Verify changes */
    TEST_ASSERT(mongolite_changes(db) == 3, "changes should be 3");

    /* Cleanup */
    free(inserted_ids);
    for (int i = 0; i < 3; i++) {
        bson_destroy(docs[i]);
    }
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Insert one JSON
 * ============================================================ */

static int test_insert_one_json(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    rc = mongolite_collection_create(db, "events", NULL, &error);
    TEST_ASSERT(rc == 0, "create collection should succeed");

    /* Insert using JSON string */
    const char *json = "{\"event\": \"click\", \"x\": 100, \"y\": 200}";

    bson_oid_t inserted_id;
    rc = mongolite_insert_one_json(db, "events", json, &inserted_id, &error);
    TEST_ASSERT(rc == 0, "insert_one_json should succeed: %s", error.message);

    /* Verify count */
    int64_t count = mongolite_collection_count(db, "events", NULL, &error);
    TEST_ASSERT(count == 1, "count should be 1");

    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Insert many JSON
 * ============================================================ */

static int test_insert_many_json(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    rc = mongolite_collection_create(db, "logs", NULL, &error);
    TEST_ASSERT(rc == 0, "create collection should succeed");

    /* Insert using JSON strings */
    const char *jsons[] = {
        "{\"level\": \"INFO\", \"msg\": \"Started\"}",
        "{\"level\": \"DEBUG\", \"msg\": \"Processing\"}",
        "{\"level\": \"INFO\", \"msg\": \"Completed\"}"
    };

    rc = mongolite_insert_many_json(db, "logs", jsons, 3, NULL, &error);
    TEST_ASSERT(rc == 0, "insert_many_json should succeed: %s", error.message);

    /* Verify count */
    int64_t count = mongolite_collection_count(db, "logs", NULL, &error);
    TEST_ASSERT(count == 3, "count should be 3, got %lld", (long long)count);

    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Insert invalid JSON should fail
 * ============================================================ */

static int test_insert_invalid_json(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    rc = mongolite_collection_create(db, "test", NULL, &error);
    TEST_ASSERT(rc == 0, "create collection should succeed");

    /* Try to insert invalid JSON */
    const char *bad_json = "{invalid json}";
    rc = mongolite_insert_one_json(db, "test", bad_json, NULL, &error);
    TEST_ASSERT(rc != 0, "invalid JSON should fail");
    printf("  Expected error: %s\n", error.message);

    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Insert into non-existent collection should fail
 * ============================================================ */

static int test_insert_no_collection(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    /* Try to insert into non-existent collection */
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "test", "value");

    rc = mongolite_insert_one(db, "nonexistent", doc, NULL, &error);
    TEST_ASSERT(rc != 0, "insert to nonexistent collection should fail");

    bson_destroy(doc);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Large batch insert
 * ============================================================ */

static int test_insert_large_batch(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    rc = mongolite_collection_create(db, "batch", NULL, &error);
    TEST_ASSERT(rc == 0, "create collection should succeed");

    /* Create 100 documents */
    const size_t N = 100;
    bson_t **docs = calloc(N, sizeof(bson_t*));
    TEST_ASSERT(docs != NULL, "alloc should succeed");

    for (size_t i = 0; i < N; i++) {
        docs[i] = bson_new();
        BSON_APPEND_INT32(docs[i], "index", (int32_t)i);
        BSON_APPEND_UTF8(docs[i], "data", "test data for batch insert");
    }

    rc = mongolite_insert_many(db, "batch", (const bson_t**)docs, N, NULL, &error);
    TEST_ASSERT(rc == 0, "insert_many should succeed: %s", error.message);

    /* Verify count */
    int64_t count = mongolite_collection_count(db, "batch", NULL, &error);
    TEST_ASSERT(count == (int64_t)N, "count should be %zu, got %lld", N, (long long)count);

    printf("  Inserted %zu documents\n", N);

    /* Cleanup */
    for (size_t i = 0; i < N; i++) {
        bson_destroy(docs[i]);
    }
    free(docs);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    printf("=== Mongolite Insert Tests (Phase 3) ===\n\n");

    RUN_TEST(test_insert_one_auto_id);
    RUN_TEST(test_insert_one_with_id);
    RUN_TEST(test_insert_duplicate_id);
    RUN_TEST(test_insert_many);
    RUN_TEST(test_insert_one_json);
    RUN_TEST(test_insert_many_json);
    RUN_TEST(test_insert_invalid_json);
    RUN_TEST(test_insert_no_collection);
    RUN_TEST(test_insert_large_batch);

    printf("\n=== All tests passed! ===\n");
    return 0;
}
