/*
 * test_mongolite_collection.c - Tests for Phase 2: Collection operations
 *
 * Tests:
 * - Collection create/drop
 * - Collection list/exists
 * - Collection metadata
 */

#include "test_runner.h"
#include "mongolite_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#include <direct.h>
#define rmdir _rmdir
#else
#include <unistd.h>
#include <sys/stat.h>
#endif

/* Test database path */
#define TEST_DB_PATH "./test_mongolite_col"

/* ============================================================
 * Helper: Remove test database directory
 * ============================================================ */

static void cleanup_test_db(void) {
#ifdef _WIN32
    system("rmdir /s /q " TEST_DB_PATH " 2>nul");
#else
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DB_PATH);
    system(cmd);
#endif
}

/* ============================================================
 * Test: Create collection
 * ============================================================ */

static int test_collection_create(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "open should succeed: %s", error.message);

    /* Create a simple collection */
    rc = mongolite_collection_create(db, "users", NULL, &error);
    TEST_ASSERT(rc == 0, "collection_create should succeed: %s", error.message);

    /* Verify it exists */
    bool exists = mongolite_collection_exists(db, "users", &error);
    TEST_ASSERT(exists, "collection should exist after create");

    /* Try to create again - should fail */
    rc = mongolite_collection_create(db, "users", NULL, &error);
    TEST_ASSERT(rc == MONGOLITE_EEXISTS, "duplicate create should fail with EEXISTS");

    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Create collection with config
 * ============================================================ */

static int test_collection_create_with_config(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    /* Create with custom metadata */
    col_config_t config = {0};
    bson_t *metadata = bson_new();
    BSON_APPEND_UTF8(metadata, "description", "User accounts");
    BSON_APPEND_INT32(metadata, "version", 1);
    config.metadata = metadata;

    rc = mongolite_collection_create(db, "users", &config, &error);
    TEST_ASSERT(rc == 0, "collection_create with config should succeed: %s", error.message);

    /* Verify metadata was stored */
    const bson_t *stored = mongolite_collection_metadata(db, "users", &error);
    TEST_ASSERT(stored != NULL, "metadata should be retrievable");

    bson_iter_t iter;
    TEST_ASSERT(bson_iter_init_find(&iter, stored, "description"),
                "metadata should contain description");
    TEST_ASSERT(strcmp(bson_iter_utf8(&iter, NULL), "User accounts") == 0,
                "description should match");

    bson_destroy((bson_t*)stored);
    bson_destroy(metadata);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Drop collection
 * ============================================================ */

static int test_collection_drop(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    /* Create collection */
    rc = mongolite_collection_create(db, "temp", NULL, &error);
    TEST_ASSERT(rc == 0, "create should succeed");
    TEST_ASSERT(mongolite_collection_exists(db, "temp", NULL), "should exist after create");

    /* Drop it */
    rc = mongolite_collection_drop(db, "temp", &error);
    TEST_ASSERT(rc == 0, "drop should succeed: %s", error.message);

    /* Verify it's gone */
    TEST_ASSERT(!mongolite_collection_exists(db, "temp", NULL), "should not exist after drop");

    /* Drop non-existent - should fail */
    rc = mongolite_collection_drop(db, "nonexistent", &error);
    TEST_ASSERT(rc != 0, "drop nonexistent should fail");

    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: List collections
 * ============================================================ */

static int test_collection_list(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    /* Empty database should have no collections */
    size_t count = 0;
    char **list = mongolite_collection_list(db, &count, &error);
    TEST_ASSERT(count == 0, "empty db should have 0 collections");
    mongolite_collection_list_free(list, count);

    /* Create some collections */
    rc = mongolite_collection_create(db, "users", NULL, &error);
    TEST_ASSERT(rc == 0, "create users should succeed");

    rc = mongolite_collection_create(db, "products", NULL, &error);
    TEST_ASSERT(rc == 0, "create products should succeed");

    rc = mongolite_collection_create(db, "orders", NULL, &error);
    TEST_ASSERT(rc == 0, "create orders should succeed");

    /* List should show 3 */
    list = mongolite_collection_list(db, &count, &error);
    TEST_ASSERT(count == 3, "should have 3 collections, got %zu", count);

    printf("  Collections: ");
    for (size_t i = 0; i < count; i++) {
        printf("%s ", list[i]);
    }
    printf("\n");

    mongolite_collection_list_free(list, count);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Collection exists
 * ============================================================ */

static int test_collection_exists(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    /* Non-existent */
    TEST_ASSERT(!mongolite_collection_exists(db, "nope", NULL),
                "nonexistent collection should return false");

    /* Create and check */
    rc = mongolite_collection_create(db, "test", NULL, &error);
    TEST_ASSERT(rc == 0, "create should succeed");
    TEST_ASSERT(mongolite_collection_exists(db, "test", NULL),
                "existing collection should return true");

    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Collection count (empty)
 * ============================================================ */

static int test_collection_count_empty(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    rc = mongolite_collection_create(db, "empty", NULL, &error);
    TEST_ASSERT(rc == 0, "create should succeed");

    /* Empty collection should have count 0 */
    int64_t count = mongolite_collection_count(db, "empty", NULL, &error);
    TEST_ASSERT(count == 0, "empty collection should have count 0, got %lld", (long long)count);

    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Collection metadata operations
 * ============================================================ */

static int test_collection_metadata(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    rc = mongolite_collection_create(db, "meta_test", NULL, &error);
    TEST_ASSERT(rc == 0, "create should succeed");

    /* Initially no metadata */
    const bson_t *meta = mongolite_collection_metadata(db, "meta_test", &error);
    TEST_ASSERT(meta == NULL, "initially should have no metadata");

    /* Set metadata */
    bson_t *new_meta = bson_new();
    BSON_APPEND_UTF8(new_meta, "owner", "admin");
    BSON_APPEND_INT32(new_meta, "priority", 5);

    rc = mongolite_collection_set_metadata(db, "meta_test", new_meta, &error);
    TEST_ASSERT(rc == 0, "set_metadata should succeed: %s", error.message);

    /* Retrieve and verify */
    meta = mongolite_collection_metadata(db, "meta_test", &error);
    TEST_ASSERT(meta != NULL, "metadata should be retrievable");

    bson_iter_t iter;
    TEST_ASSERT(bson_iter_init_find(&iter, meta, "owner"), "should have owner field");
    TEST_ASSERT(strcmp(bson_iter_utf8(&iter, NULL), "admin") == 0, "owner should be admin");

    bson_destroy((bson_t*)meta);
    bson_destroy(new_meta);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Persistence across reopen
 * ============================================================ */

static int test_collection_persistence(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    /* First session: create collections */
    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "first open should succeed");

    rc = mongolite_collection_create(db, "persistent1", NULL, &error);
    TEST_ASSERT(rc == 0, "create persistent1 should succeed");

    rc = mongolite_collection_create(db, "persistent2", NULL, &error);
    TEST_ASSERT(rc == 0, "create persistent2 should succeed");

    mongolite_close(db);

    /* Second session: verify persistence */
    db = NULL;
    rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "reopen should succeed: %s", error.message);

    TEST_ASSERT(mongolite_collection_exists(db, "persistent1", NULL),
                "persistent1 should exist after reopen");
    TEST_ASSERT(mongolite_collection_exists(db, "persistent2", NULL),
                "persistent2 should exist after reopen");

    size_t count = 0;
    char **list = mongolite_collection_list(db, &count, &error);
    TEST_ASSERT(count == 2, "should have 2 collections after reopen");
    mongolite_collection_list_free(list, count);

    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    printf("=== Mongolite Collection Tests (Phase 2) ===\n\n");

    RUN_TEST(test_collection_create);
    RUN_TEST(test_collection_create_with_config);
    RUN_TEST(test_collection_drop);
    RUN_TEST(test_collection_list);
    RUN_TEST(test_collection_exists);
    RUN_TEST(test_collection_count_empty);
    RUN_TEST(test_collection_metadata);
    RUN_TEST(test_collection_persistence);

    printf("\n=== All tests passed! ===\n");
    return 0;
}
