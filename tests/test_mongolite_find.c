/*
 * test_mongolite_find.c - Tests for Phase 4: Find operations
 *
 * Tests:
 * - find_one by _id
 * - find_one with filter
 * - find with cursor
 * - JSON wrappers
 * - limit/skip
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
#define TEST_DB_PATH "./test_mongolite_find"

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
 * Test: Find one without filter (returns first doc)
 * ============================================================ */

static int test_find_one_no_filter(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Find any document */
    bson_t *doc = mongolite_find_one(db, "users", NULL, NULL, &error);
    TEST_ASSERT(doc != NULL, "find_one should return a document");

    /* Should have a name field */
    bson_iter_t iter;
    TEST_ASSERT(bson_iter_init_find(&iter, doc, "name"), "doc should have name");
    printf("  Found: %s\n", bson_iter_utf8(&iter, NULL));

    bson_destroy(doc);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Find one by _id (direct lookup)
 * ============================================================ */

static int test_find_one_by_id(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    rc = mongolite_collection_create(db, "items", NULL, &error);
    TEST_ASSERT(rc == 0, "create should succeed");

    /* Insert with known _id */
    bson_oid_t my_id;
    bson_oid_init(&my_id, NULL);

    bson_t *insert_doc = bson_new();
    BSON_APPEND_OID(insert_doc, "_id", &my_id);
    BSON_APPEND_UTF8(insert_doc, "value", "test_value");

    rc = mongolite_insert_one(db, "items", insert_doc, NULL, &error);
    TEST_ASSERT(rc == 0, "insert should succeed");
    bson_destroy(insert_doc);

    /* Find by _id */
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &my_id);

    bson_t *found = mongolite_find_one(db, "items", filter, NULL, &error);
    TEST_ASSERT(found != NULL, "find_one by _id should find doc: %s", error.message);

    /* Verify it's the right document */
    bson_iter_t iter;
    TEST_ASSERT(bson_iter_init_find(&iter, found, "value"), "should have value");
    TEST_ASSERT(strcmp(bson_iter_utf8(&iter, NULL), "test_value") == 0,
                "value should match");

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Find one with filter (scan)
 * ============================================================ */

static int test_find_one_with_filter(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Find user with age 35 */
    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "age", 35);

    bson_t *doc = mongolite_find_one(db, "users", filter, NULL, &error);
    TEST_ASSERT(doc != NULL, "find_one should find Charlie: %s", error.message);

    bson_iter_t iter;
    TEST_ASSERT(bson_iter_init_find(&iter, doc, "name"), "should have name");
    TEST_ASSERT(strcmp(bson_iter_utf8(&iter, NULL), "Charlie") == 0,
                "should find Charlie, got %s", bson_iter_utf8(&iter, NULL));

    bson_destroy(filter);
    bson_destroy(doc);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Find one with complex filter
 * ============================================================ */

static int test_find_one_complex_filter(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Find user in NYC with age 30 */
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "city", "NYC");
    BSON_APPEND_INT32(filter, "age", 30);

    bson_t *doc = mongolite_find_one(db, "users", filter, NULL, &error);
    TEST_ASSERT(doc != NULL, "find_one should find Alice: %s", error.message);

    bson_iter_t iter;
    TEST_ASSERT(bson_iter_init_find(&iter, doc, "name"), "should have name");
    TEST_ASSERT(strcmp(bson_iter_utf8(&iter, NULL), "Alice") == 0,
                "should find Alice");

    bson_destroy(filter);
    bson_destroy(doc);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Find one not found
 * ============================================================ */

static int test_find_one_not_found(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Find user with age 99 (doesn't exist) */
    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "age", 99);

    bson_t *doc = mongolite_find_one(db, "users", filter, NULL, &error);
    TEST_ASSERT(doc == NULL, "find_one should return NULL for no match");

    bson_destroy(filter);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Find with cursor (iterate all)
 * ============================================================ */

static int test_find_cursor_all(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Find all documents */
    mongolite_cursor_t *cursor = mongolite_find(db, "users", NULL, NULL, &error);
    TEST_ASSERT(cursor != NULL, "find should return cursor: %s", error.message);

    int count = 0;
    const bson_t *doc;
    printf("  Documents: ");
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
        bson_iter_t iter;
        if (bson_iter_init_find(&iter, doc, "name")) {
            printf("%s ", bson_iter_utf8(&iter, NULL));
        }
    }
    printf("\n");

    TEST_ASSERT(count == 5, "should find 5 documents, got %d", count);

    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Find with cursor and filter
 * ============================================================ */

static int test_find_cursor_filtered(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Find users with age 30 */
    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "age", 30);

    mongolite_cursor_t *cursor = mongolite_find(db, "users", filter, NULL, &error);
    TEST_ASSERT(cursor != NULL, "find should return cursor: %s", error.message);

    int count = 0;
    const bson_t *doc;
    printf("  Age 30: ");
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
        bson_iter_t iter;
        if (bson_iter_init_find(&iter, doc, "name")) {
            printf("%s ", bson_iter_utf8(&iter, NULL));
        }
    }
    printf("\n");

    TEST_ASSERT(count == 2, "should find 2 documents with age 30, got %d", count);

    bson_destroy(filter);
    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Cursor with limit
 * ============================================================ */

static int test_cursor_limit(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    mongolite_cursor_t *cursor = mongolite_find(db, "users", NULL, NULL, &error);
    TEST_ASSERT(cursor != NULL, "find should return cursor");

    /* Set limit to 2 */
    int rc = mongolite_cursor_set_limit(cursor, 2);
    TEST_ASSERT(rc == 0, "set_limit should succeed");

    int count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }

    TEST_ASSERT(count == 2, "should return only 2 documents, got %d", count);

    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Find one JSON
 * ============================================================ */

static int test_find_one_json(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Find by JSON filter */
    const char *result = mongolite_find_one_json(db, "users",
                                                  "{\"name\": \"Bob\"}",
                                                  NULL, &error);
    TEST_ASSERT(result != NULL, "find_one_json should return result: %s", error.message);

    printf("  JSON result: %s\n", result);
    TEST_ASSERT(strstr(result, "Bob") != NULL, "result should contain Bob");

    bson_free((void*)result);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Find JSON (array)
 * ============================================================ */

static int test_find_json_array(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Find all from NYC */
    char **results = mongolite_find_json(db, "users",
                                          "{\"city\": \"NYC\"}",
                                          NULL, &error);
    TEST_ASSERT(results != NULL, "find_json should return results: %s", error.message);

    int count = 0;
    printf("  NYC users:\n");
    for (int i = 0; results[i] != NULL; i++) {
        printf("    %s\n", results[i]);
        bson_free(results[i]);
        count++;
    }
    free(results);

    TEST_ASSERT(count == 2, "should find 2 NYC users, got %d", count);

    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Find with $gt operator
 * ============================================================ */

static int test_find_gt_operator(void) {
    mongolite_db_t *db = setup_test_db();
    TEST_ASSERT(db != NULL, "setup should succeed");

    gerror_t error = {0};

    /* Find users with age > 28 */
    bson_t *filter = bson_new();
    bson_t child;
    BSON_APPEND_DOCUMENT_BEGIN(filter, "age", &child);
    BSON_APPEND_INT32(&child, "$gt", 28);
    bson_append_document_end(filter, &child);

    mongolite_cursor_t *cursor = mongolite_find(db, "users", filter, NULL, &error);
    TEST_ASSERT(cursor != NULL, "find should return cursor: %s", error.message);

    int count = 0;
    const bson_t *doc;
    printf("  Age > 28: ");
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
        bson_iter_t iter;
        if (bson_iter_init_find(&iter, doc, "name")) {
            printf("%s ", bson_iter_utf8(&iter, NULL));
        }
    }
    printf("\n");

    TEST_ASSERT(count == 3, "should find 3 documents (Alice, Charlie, Eve), got %d", count);

    bson_destroy(filter);
    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Verify insert data integrity via find
 * ============================================================ */

static int test_insert_find_integrity(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    rc = mongolite_collection_create(db, "test", NULL, &error);
    TEST_ASSERT(rc == 0, "create should succeed");

    /* Insert complex document */
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "string", "hello world");
    BSON_APPEND_INT32(doc, "int32", 42);
    BSON_APPEND_INT64(doc, "int64", 9876543210LL);
    BSON_APPEND_DOUBLE(doc, "double", 3.14159);
    BSON_APPEND_BOOL(doc, "bool_true", true);
    BSON_APPEND_BOOL(doc, "bool_false", false);

    bson_oid_t inserted_id;
    rc = mongolite_insert_one(db, "test", doc, &inserted_id, &error);
    TEST_ASSERT(rc == 0, "insert should succeed");
    bson_destroy(doc);

    /* Find by _id */
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &inserted_id);

    bson_t *found = mongolite_find_one(db, "test", filter, NULL, &error);
    TEST_ASSERT(found != NULL, "should find document");

    /* Verify all fields */
    bson_iter_t iter;

    TEST_ASSERT(bson_iter_init_find(&iter, found, "string"), "should have string");
    TEST_ASSERT(strcmp(bson_iter_utf8(&iter, NULL), "hello world") == 0, "string should match");

    TEST_ASSERT(bson_iter_init_find(&iter, found, "int32"), "should have int32");
    TEST_ASSERT(bson_iter_int32(&iter) == 42, "int32 should match");

    TEST_ASSERT(bson_iter_init_find(&iter, found, "int64"), "should have int64");
    TEST_ASSERT(bson_iter_int64(&iter) == 9876543210LL, "int64 should match");

    TEST_ASSERT(bson_iter_init_find(&iter, found, "double"), "should have double");
    TEST_ASSERT(bson_iter_double(&iter) > 3.14 && bson_iter_double(&iter) < 3.15,
                "double should match");

    TEST_ASSERT(bson_iter_init_find(&iter, found, "bool_true"), "should have bool_true");
    TEST_ASSERT(bson_iter_bool(&iter) == true, "bool_true should be true");

    TEST_ASSERT(bson_iter_init_find(&iter, found, "bool_false"), "should have bool_false");
    TEST_ASSERT(bson_iter_bool(&iter) == false, "bool_false should be false");

    printf("  All data types verified!\n");

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
    printf("=== Mongolite Find Tests (Phase 4) ===\n\n");

    RUN_TEST(test_find_one_no_filter);
    RUN_TEST(test_find_one_by_id);
    RUN_TEST(test_find_one_with_filter);
    RUN_TEST(test_find_one_complex_filter);
    RUN_TEST(test_find_one_not_found);
    RUN_TEST(test_find_cursor_all);
    RUN_TEST(test_find_cursor_filtered);
    RUN_TEST(test_cursor_limit);
    RUN_TEST(test_find_one_json);
    RUN_TEST(test_find_json_array);
    RUN_TEST(test_find_gt_operator);
    RUN_TEST(test_insert_find_integrity);

    printf("\n=== All tests passed! ===\n");
    return 0;
}
