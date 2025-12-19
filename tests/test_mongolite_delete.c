/*
 * test_mongolite_delete.c - Tests for Phase 7: Delete operations
 *
 * Tests:
 * - delete_one by _id
 * - delete_one with filter
 * - delete_one not found
 * - delete_many with filter
 * - delete_many all documents
 * - delete_many with count
 * - doc_count updates after delete
 * - delete from empty collection
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
#define TEST_DB_PATH "./test_mongolite_delete"

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
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;  /* 32MB */
    if (mongolite_open(TEST_DB_PATH, &db, &config, &error) != 0) {
        printf("Failed to open db: %s\n", error.message);
        return NULL;
    }

    if (mongolite_collection_create(db, "users", NULL, &error) != 0) {
        printf("Failed to create collection: %s\n", error.message);
        mongolite_close(db);
        return NULL;
    }

    /* Insert test data */
    const char *users[] = {
        "{\"name\": \"Alice\", \"age\": 30, \"city\": \"NYC\"}",
        "{\"name\": \"Bob\", \"age\": 25, \"city\": \"LA\"}",
        "{\"name\": \"Charlie\", \"age\": 35, \"city\": \"NYC\"}",
        "{\"name\": \"Diana\", \"age\": 28, \"city\": \"Chicago\"}",
        "{\"name\": \"Eve\", \"age\": 30, \"city\": \"LA\"}"
    };

    for (int i = 0; i < 5; i++) {
        if (mongolite_insert_one_json(db, "users", users[i], NULL, &error) != 0) {
            printf("Failed to insert: %s\n", error.message);
            mongolite_close(db);
            return NULL;
        }
    }

    return db;
}

/* ============================================================
 * Test: Delete one by _id
 * ============================================================ */

static int test_delete_one_by_id(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Insert a document */
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users",
        "{\"name\": \"ToDelete\", \"age\": 99}", &id, &error);
    TEST_ASSERT(rc == 0, "insert should succeed");

    /* Verify it exists */
    int64_t count_before = mongolite_collection_count(db, "users", NULL, &error);
    TEST_ASSERT(count_before == 6, "should have 6 documents");

    /* Delete by _id */
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    rc = mongolite_delete_one(db, "users", filter, &error);
    TEST_ASSERT(rc == 0, "delete_one should succeed: %s", error.message);

    bson_destroy(filter);

    /* Verify it's gone */
    int64_t count_after = mongolite_collection_count(db, "users", NULL, &error);
    TEST_ASSERT(count_after == 5, "should have 5 documents after delete");

    filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);
    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    TEST_ASSERT(found == NULL, "deleted document should not be found");

    printf("  Deleted document by _id, count: %lld -> %lld\n",
           (long long)count_before, (long long)count_after);

    bson_destroy(filter);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Delete one with filter
 * ============================================================ */

static int test_delete_one_with_filter(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Delete user with age 35 */
    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "age", 35);

    int rc = mongolite_delete_one(db, "users", filter, &error);
    TEST_ASSERT(rc == 0, "delete_one should succeed");

    bson_destroy(filter);

    /* Verify count */
    int64_t count = mongolite_collection_count(db, "users", NULL, &error);
    TEST_ASSERT(count == 4, "should have 4 documents after delete");

    /* Verify Charlie is gone */
    filter = bson_new();
    BSON_APPEND_UTF8(filter, "name", "Charlie");

    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    TEST_ASSERT(found == NULL, "Charlie should be deleted");

    printf("  Deleted one by filter (age=35)\n");

    bson_destroy(filter);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Delete one not found
 * ============================================================ */

static int test_delete_one_not_found(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    int64_t count_before = mongolite_collection_count(db, "users", NULL, &error);

    /* Try to delete non-existent document */
    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "age", 999);

    int rc = mongolite_delete_one(db, "users", filter, &error);
    TEST_ASSERT(rc == 0, "delete_one should return 0 even if not found");

    bson_destroy(filter);

    /* Verify count unchanged */
    int64_t count_after = mongolite_collection_count(db, "users", NULL, &error);
    TEST_ASSERT(count_before == count_after, "count should be unchanged");

    printf("  Delete one not found - no error, count unchanged\n");

    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Delete many with filter
 * ============================================================ */

static int test_delete_many_with_filter(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Delete all users with age 30 */
    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "age", 30);

    int64_t deleted_count = 0;
    int rc = mongolite_delete_many(db, "users", filter, &deleted_count, &error);
    TEST_ASSERT(rc == 0, "delete_many should succeed: %s", error.message);
    TEST_ASSERT(deleted_count == 2, "should delete 2 documents (Alice, Eve)");

    bson_destroy(filter);

    /* Verify count */
    int64_t count = mongolite_collection_count(db, "users", NULL, &error);
    TEST_ASSERT(count == 3, "should have 3 documents remaining");

    /* Verify Alice and Eve are gone */
    filter = bson_new();
    BSON_APPEND_INT32(filter, "age", 30);

    mongolite_cursor_t *cursor = mongolite_find(db, "users", filter, NULL, &error);
    TEST_ASSERT(cursor != NULL, "find should work");

    const bson_t *doc;
    int found_count = 0;
    while (mongolite_cursor_next(cursor, &doc)) {
        found_count++;
    }
    TEST_ASSERT(found_count == 0, "no users with age 30 should remain");

    printf("  Deleted %lld documents with filter (age=30)\n", (long long)deleted_count);

    bson_destroy(filter);
    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Delete many all documents
 * ============================================================ */

static int test_delete_many_all(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Delete all documents (NULL filter) */
    int64_t deleted_count = 0;
    int rc = mongolite_delete_many(db, "users", NULL, &deleted_count, &error);
    TEST_ASSERT(rc == 0, "delete_many should succeed");
    TEST_ASSERT(deleted_count == 5, "should delete all 5 documents");

    /* Verify count is 0 */
    int64_t count = mongolite_collection_count(db, "users", NULL, &error);
    TEST_ASSERT(count == 0, "should have 0 documents after deleting all");

    printf("  Deleted all %lld documents\n", (long long)deleted_count);

    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Delete many from NYC
 * ============================================================ */

static int test_delete_many_nyc(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Delete all NYC users */
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "city", "NYC");

    int64_t deleted_count = 0;
    int rc = mongolite_delete_many(db, "users", filter, &deleted_count, &error);
    TEST_ASSERT(rc == 0, "delete_many should succeed");
    TEST_ASSERT(deleted_count == 2, "should delete 2 NYC users");

    bson_destroy(filter);

    /* Verify remaining users */
    int64_t count = mongolite_collection_count(db, "users", NULL, &error);
    TEST_ASSERT(count == 3, "should have 3 documents remaining");

    /* Verify no NYC users remain */
    filter = bson_new();
    BSON_APPEND_UTF8(filter, "city", "NYC");

    int64_t nyc_count = mongolite_collection_count(db, "users", filter, &error);
    TEST_ASSERT(nyc_count == 0, "no NYC users should remain");

    printf("  Deleted %lld NYC users\n", (long long)deleted_count);

    bson_destroy(filter);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Delete from empty collection
 * ============================================================ */

static int test_delete_from_empty(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;  /* 32MB */
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    rc = mongolite_collection_create(db, "empty", NULL, &error);
    TEST_ASSERT(rc == 0, "create should succeed");

    /* Try to delete from empty collection */
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "name", "Nobody");

    rc = mongolite_delete_one(db, "empty", filter, &error);
    TEST_ASSERT(rc == 0, "delete_one from empty should succeed");

    int64_t deleted_count = 0;
    rc = mongolite_delete_many(db, "empty", filter, &deleted_count, &error);
    TEST_ASSERT(rc == 0, "delete_many from empty should succeed");
    TEST_ASSERT(deleted_count == 0, "deleted count should be 0");

    printf("  Delete from empty collection - no error\n");

    bson_destroy(filter);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: changes() counter after delete
 * ============================================================ */

static int test_delete_changes_counter(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Delete one */
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "name", "Bob");

    int rc = mongolite_delete_one(db, "users", filter, &error);
    TEST_ASSERT(rc == 0, "delete_one should succeed");

    int changes = mongolite_changes(db);
    TEST_ASSERT(changes == 1, "changes should be 1 after delete_one");

    bson_destroy(filter);

    /* Delete many */
    filter = bson_new();
    BSON_APPEND_INT32(filter, "age", 30);

    int64_t deleted_count = 0;
    rc = mongolite_delete_many(db, "users", filter, &deleted_count, &error);
    TEST_ASSERT(rc == 0, "delete_many should succeed");

    changes = mongolite_changes(db);
    TEST_ASSERT(changes == 2, "changes should be 2 after deleting 2 docs");

    printf("  changes() counter verified: 1, then 2\n");

    bson_destroy(filter);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Delete with complex filter
 * ============================================================ */

static int test_delete_complex_filter(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Delete users with age > 28 */
    bson_t *filter = bson_new();
    bson_t child;
    BSON_APPEND_DOCUMENT_BEGIN(filter, "age", &child);
    BSON_APPEND_INT32(&child, "$gt", 28);
    bson_append_document_end(filter, &child);

    int64_t deleted_count = 0;
    int rc = mongolite_delete_many(db, "users", filter, &deleted_count, &error);
    TEST_ASSERT(rc == 0, "delete_many should succeed: %s", error.message);
    TEST_ASSERT(deleted_count == 3, "should delete 3 docs (Alice 30, Charlie 35, Eve 30)");

    bson_destroy(filter);

    /* Verify remaining users */
    int64_t count = mongolite_collection_count(db, "users", NULL, &error);
    TEST_ASSERT(count == 2, "should have 2 documents remaining (Bob 25, Diana 28)");

    printf("  Deleted %lld documents with complex filter (age > 28)\n",
           (long long)deleted_count);

    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Delete and verify data integrity
 * ============================================================ */

static int test_delete_data_integrity(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;  /* 32MB */
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    rc = mongolite_collection_create(db, "test", NULL, &error);
    TEST_ASSERT(rc == 0, "create should succeed");

    /* Insert 10 documents */
    bson_oid_t ids[10];
    for (int i = 0; i < 10; i++) {
        bson_t *doc = bson_new();
        BSON_APPEND_INT32(doc, "index", i);
        BSON_APPEND_INT32(doc, "category", i % 3);  /* 0, 1, or 2 */

        rc = mongolite_insert_one(db, "test", doc, &ids[i], &error);
        TEST_ASSERT(rc == 0, "insert should succeed");
        bson_destroy(doc);
    }

    /* Delete category 1 (indices 1, 4, 7) */
    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "category", 1);

    int64_t deleted_count = 0;
    rc = mongolite_delete_many(db, "test", filter, &deleted_count, &error);
    TEST_ASSERT(rc == 0, "delete_many should succeed");
    TEST_ASSERT(deleted_count == 3, "should delete 3 documents");

    bson_destroy(filter);

    /* Verify remaining documents */
    mongolite_cursor_t *cursor = mongolite_find(db, "test", NULL, NULL, &error);
    TEST_ASSERT(cursor != NULL, "find should work");

    int found_count = 0;
    const bson_t *doc;
    printf("  Remaining indices: ");
    while (mongolite_cursor_next(cursor, &doc)) {
        bson_iter_t iter;
        if (bson_iter_init_find(&iter, doc, "index")) {
            int idx = bson_iter_int32(&iter);
            printf("%d ", idx);
            TEST_ASSERT(idx % 3 != 1, "deleted indices should not be present");
        }
        found_count++;
    }
    printf("\n");

    TEST_ASSERT(found_count == 7, "should have 7 documents remaining");

    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    printf("=== Mongolite Delete Tests (Phase 7) ===\n\n");

    RUN_TEST(test_delete_one_by_id);
    RUN_TEST(test_delete_one_with_filter);
    RUN_TEST(test_delete_one_not_found);
    RUN_TEST(test_delete_many_with_filter);
    RUN_TEST(test_delete_many_all);
    RUN_TEST(test_delete_many_nyc);
    RUN_TEST(test_delete_from_empty);
    RUN_TEST(test_delete_changes_counter);
    RUN_TEST(test_delete_complex_filter);
    RUN_TEST(test_delete_data_integrity);

    printf("\n=== All tests passed! ===\n");
    return 0;
}
