/*
 * test_mongolite_db.c - Tests for Phase 1: Database core operations
 *
 * Tests:
 * - Database open/close
 * - Schema operations
 * - Metadata handling
 * - Transaction basics
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
#include <dirent.h>
#include <sys/stat.h>
#endif

/* Test database path */
#define TEST_DB_PATH "./test_mongolite_db"

/* ============================================================
 * Helper: Remove test database directory
 * ============================================================ */

static void cleanup_test_db(void) {
#ifdef _WIN32
    /* Windows: use shell command for simplicity */
    system("rmdir /s /q " TEST_DB_PATH " 2>nul");
#else
    /* Unix: recursively remove directory */
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DB_PATH);
    system(cmd);
#endif
}

/* ============================================================
 * Test: Basic open/close
 * ============================================================ */

static int test_open_close(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    /* Open with default config */
    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    TEST_ASSERT(rc == 0, "mongolite_open should succeed: %s", error.message);
    TEST_ASSERT(db != NULL, "db should not be NULL");

    /* Check filename */
    const char *filename = mongolite_db_filename(db);
    TEST_ASSERT(filename != NULL, "filename should not be NULL");
    TEST_ASSERT(strcmp(filename, TEST_DB_PATH) == 0, "filename should match");

    /* Check version */
    const char *version = mongolite_version();
    TEST_ASSERT(version != NULL, "version should not be NULL");
    printf("  mongolite version: %s\n", version);

    /* Close */
    rc = mongolite_close(db);
    TEST_ASSERT(rc == 0, "mongolite_close should succeed");

    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Open with custom configuration
 * ============================================================ */

static int test_open_with_config(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    /* Create config with custom settings */
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;  /* 32MB */
    config.max_dbs = 64;

    /* Create user metadata */
    bson_t *metadata = bson_new();
    BSON_APPEND_UTF8(metadata, "app_name", "test_app");
    BSON_APPEND_UTF8(metadata, "version", "1.0.0");
    BSON_APPEND_INT32(metadata, "schema_version", 1);
    config.metadata = metadata;

    /* Open database */
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    TEST_ASSERT(rc == 0, "mongolite_open should succeed: %s", error.message);

    /* Verify metadata is accessible */
    const bson_t *stored_metadata = mongolite_db_metadata(db);
    TEST_ASSERT(stored_metadata != NULL, "metadata should not be NULL");

    /* Check metadata content */
    bson_iter_t iter;
    TEST_ASSERT(bson_iter_init_find(&iter, stored_metadata, "app_name"),
                "metadata should contain app_name");
    TEST_ASSERT(strcmp(bson_iter_utf8(&iter, NULL), "test_app") == 0,
                "app_name should match");

    bson_destroy(metadata);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Reopen existing database
 * ============================================================ */

static int test_reopen_database(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;  /* 32MB */
    /* First open - create database */
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    TEST_ASSERT(rc == 0, "first open should succeed: %s", error.message);

    /* Set some metadata */
    bson_t *metadata = bson_new();
    BSON_APPEND_INT32(metadata, "counter", 42);
    rc = mongolite_db_set_metadata(db, metadata, &error);
    TEST_ASSERT(rc == 0, "set_metadata should succeed: %s", error.message);

    bson_destroy(metadata);
    mongolite_close(db);

    /* Reopen */
    db = NULL;
    rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    TEST_ASSERT(rc == 0, "reopen should succeed: %s", error.message);
    TEST_ASSERT(db != NULL, "db should not be NULL after reopen");

    /* Close */
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Schema entry operations
 * ============================================================ */

static int test_schema_operations(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;  /* 32MB */

    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    TEST_ASSERT(rc == 0, "open should succeed: %s", error.message);

    /* Create a schema entry for a collection */
    mongolite_schema_entry_t entry = {0};
    bson_oid_init(&entry.oid, NULL);
    entry.name = strdup("test_collection");
    entry.tree_name = _mongolite_collection_tree_name("test_collection");
    entry.type = strdup(SCHEMA_TYPE_COLLECTION);
    entry.created_at = _mongolite_now_ms();
    entry.modified_at = entry.created_at;
    entry.doc_count = 0;

    /* Add options */
    entry.options = bson_new();
    BSON_APPEND_BOOL(entry.options, "capped", false);

    /* Add user metadata */
    entry.metadata = bson_new();
    BSON_APPEND_UTF8(entry.metadata, "description", "A test collection");

    /* Store in schema */
    rc = _mongolite_schema_put(db, &entry, &error);
    TEST_ASSERT(rc == 0, "schema_put should succeed: %s", error.message);

    /* Read it back */
    mongolite_schema_entry_t read_entry = {0};
    rc = _mongolite_schema_get(db, "test_collection", &read_entry, &error);
    TEST_ASSERT(rc == 0, "schema_get should succeed: %s", error.message);

    /* Verify fields */
    TEST_ASSERT(read_entry.name != NULL, "name should not be NULL");
    TEST_ASSERT(strcmp(read_entry.name, "test_collection") == 0, "name should match");
    TEST_ASSERT(strcmp(read_entry.type, SCHEMA_TYPE_COLLECTION) == 0, "type should match");
    TEST_ASSERT(bson_oid_compare(&read_entry.oid, &entry.oid) == 0, "oid should match");

    /* Check metadata was preserved */
    TEST_ASSERT(read_entry.metadata != NULL, "metadata should not be NULL");
    bson_iter_t iter;
    TEST_ASSERT(bson_iter_init_find(&iter, read_entry.metadata, "description"),
                "metadata should contain description");

    /* Update the entry */
    entry.doc_count = 100;
    entry.modified_at = _mongolite_now_ms();
    rc = _mongolite_schema_put(db, &entry, &error);
    TEST_ASSERT(rc == 0, "schema_put (update) should succeed: %s", error.message);

    /* Read again and verify update */
    _mongolite_schema_entry_free(&read_entry);
    rc = _mongolite_schema_get(db, "test_collection", &read_entry, &error);
    TEST_ASSERT(rc == 0, "schema_get after update should succeed");
    TEST_ASSERT(read_entry.doc_count == 100, "doc_count should be updated");

    /* Delete the entry */
    rc = _mongolite_schema_delete(db, "test_collection", &error);
    TEST_ASSERT(rc == 0, "schema_delete should succeed: %s", error.message);

    /* Verify it's gone */
    _mongolite_schema_entry_free(&read_entry);
    rc = _mongolite_schema_get(db, "test_collection", &read_entry, &error);
    TEST_ASSERT(rc != 0, "schema_get after delete should fail");

    /* Cleanup */
    _mongolite_schema_entry_free(&entry);
    _mongolite_schema_entry_free(&read_entry);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Schema list operation
 * ============================================================ */

static int test_schema_list(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;  /* 32MB */

    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    TEST_ASSERT(rc == 0, "open should succeed: %s", error.message);

    /* Create multiple schema entries */
    const char *names[] = {"users", "products", "orders"};
    for (int i = 0; i < 3; i++) {
        mongolite_schema_entry_t entry = {0};
        bson_oid_init(&entry.oid, NULL);
        entry.name = strdup(names[i]);
        entry.tree_name = _mongolite_collection_tree_name(names[i]);
        entry.type = strdup(SCHEMA_TYPE_COLLECTION);
        entry.created_at = _mongolite_now_ms();
        entry.modified_at = entry.created_at;

        rc = _mongolite_schema_put(db, &entry, &error);
        TEST_ASSERT(rc == 0, "schema_put should succeed for %s: %s", names[i], error.message);

        _mongolite_schema_entry_free(&entry);
    }

    /* List all collections */
    char **list = NULL;
    size_t count = 0;
    rc = _mongolite_schema_list(db, &list, &count, SCHEMA_TYPE_COLLECTION, &error);
    TEST_ASSERT(rc == 0, "schema_list should succeed: %s", error.message);
    TEST_ASSERT(count == 3, "should have 3 collections, got %zu", count);

    printf("  Collections: ");
    for (size_t i = 0; i < count; i++) {
        printf("%s ", list[i]);
        free(list[i]);
    }
    printf("\n");
    free(list);

    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Transaction operations
 * ============================================================ */

static int test_transactions(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;  /* 32MB */
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    TEST_ASSERT(rc == 0, "open should succeed: %s", error.message);

    /* Begin transaction */
    rc = mongolite_begin_transaction(db);
    TEST_ASSERT(rc == 0, "begin_transaction should succeed");

    /* Create a schema entry within transaction */
    mongolite_schema_entry_t entry = {0};
    bson_oid_init(&entry.oid, NULL);
    entry.name = strdup("txn_test");
    entry.tree_name = _mongolite_collection_tree_name("txn_test");
    entry.type = strdup(SCHEMA_TYPE_COLLECTION);
    entry.created_at = _mongolite_now_ms();
    entry.modified_at = entry.created_at;

    rc = _mongolite_schema_put(db, &entry, &error);
    TEST_ASSERT(rc == 0, "schema_put in transaction should succeed: %s", error.message);

    /* Commit */
    rc = mongolite_commit(db);
    TEST_ASSERT(rc == 0, "commit should succeed");

    /* Verify entry exists after commit */
    mongolite_schema_entry_t read_entry = {0};
    rc = _mongolite_schema_get(db, "txn_test", &read_entry, &error);
    TEST_ASSERT(rc == 0, "entry should exist after commit");
    _mongolite_schema_entry_free(&read_entry);

    /* Test rollback */
    rc = mongolite_begin_transaction(db);
    TEST_ASSERT(rc == 0, "begin second transaction should succeed");

    /* Delete entry in transaction */
    rc = _mongolite_schema_delete(db, "txn_test", &error);
    TEST_ASSERT(rc == 0, "delete in transaction should succeed");

    /* Rollback */
    rc = mongolite_rollback(db);
    TEST_ASSERT(rc == 0, "rollback should succeed");

    /* Verify entry still exists after rollback */
    rc = _mongolite_schema_get(db, "txn_test", &read_entry, &error);
    TEST_ASSERT(rc == 0, "entry should still exist after rollback: %s", error.message);

    _mongolite_schema_entry_free(&entry);
    _mongolite_schema_entry_free(&read_entry);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Tree name builders
 * ============================================================ */

static int test_tree_name_builders(void) {
    /* Collection tree name */
    char *col_tree = _mongolite_collection_tree_name("users");
    TEST_ASSERT(col_tree != NULL, "collection tree name should not be NULL");
    TEST_ASSERT(strcmp(col_tree, "col:users") == 0, "should be 'col:users', got '%s'", col_tree);
    free(col_tree);

    /* Index tree name */
    char *idx_tree = _mongolite_index_tree_name("users", "email_1");
    TEST_ASSERT(idx_tree != NULL, "index tree name should not be NULL");
    TEST_ASSERT(strcmp(idx_tree, "idx:users:email_1") == 0,
                "should be 'idx:users:email_1', got '%s'", idx_tree);
    free(idx_tree);

    return 0;
}

/* ============================================================
 * Test: Error handling
 * ============================================================ */

static int test_error_handling(void) {
    gerror_t error = {0};

    /* Test opening invalid path */
    mongolite_db_t *db = NULL;

    /* Test NULL parameters */
    int rc = mongolite_open(NULL, &db, NULL, &error);
    TEST_ASSERT(rc != 0, "open with NULL path should fail");

    rc = mongolite_open(TEST_DB_PATH, NULL, NULL, &error);
    TEST_ASSERT(rc != 0, "open with NULL db pointer should fail");

    /* Test errstr */
    const char *errstr = mongolite_errstr(0);
    TEST_ASSERT(errstr != NULL, "errstr should not be NULL");
    TEST_ASSERT(strcmp(errstr, "Success") == 0, "errstr(0) should be 'Success'");

    errstr = mongolite_errstr(-4);  /* MONGOLITE_EINVAL */
    TEST_ASSERT(errstr != NULL, "errstr for EINVAL should not be NULL");
    printf("  EINVAL message: %s\n", errstr);

    return 0;
}

/* ============================================================
 * Test: Sync operation
 * ============================================================ */

static int test_sync(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;  /* 32MB */

    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    TEST_ASSERT(rc == 0, "open should succeed: %s", error.message);

    /* Create some data */
    mongolite_schema_entry_t entry = {0};
    bson_oid_init(&entry.oid, NULL);
    entry.name = strdup("sync_test");
    entry.tree_name = _mongolite_collection_tree_name("sync_test");
    entry.type = strdup(SCHEMA_TYPE_COLLECTION);
    entry.created_at = _mongolite_now_ms();
    entry.modified_at = entry.created_at;

    rc = _mongolite_schema_put(db, &entry, &error);
    TEST_ASSERT(rc == 0, "schema_put should succeed");

    /* Force sync */
    rc = mongolite_sync(db, true, &error);
    TEST_ASSERT(rc == 0, "sync should succeed: %s", error.message);

    _mongolite_schema_entry_free(&entry);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: OID to rowid conversion
 * ============================================================ */

static int test_oid_to_rowid(void) {
    bson_oid_t oid1, oid2;
    bson_oid_init(&oid1, NULL);
    bson_oid_init(&oid2, NULL);

    int64_t rowid1 = _mongolite_oid_to_rowid(&oid1);
    int64_t rowid2 = _mongolite_oid_to_rowid(&oid2);

    /* Different OIDs should produce different rowids (with high probability) */
    TEST_ASSERT(rowid1 != rowid2, "different OIDs should have different rowids");

    /* Same OID should produce same rowid */
    int64_t rowid1_again = _mongolite_oid_to_rowid(&oid1);
    TEST_ASSERT(rowid1 == rowid1_again, "same OID should produce same rowid");

    /* NULL should return 0 */
    int64_t null_rowid = _mongolite_oid_to_rowid(NULL);
    TEST_ASSERT(null_rowid == 0, "NULL OID should return 0");

    return 0;
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    printf("=== Mongolite Database Tests (Phase 1) ===\n\n");

    RUN_TEST(test_open_close);
    RUN_TEST(test_open_with_config);
    RUN_TEST(test_reopen_database);
    RUN_TEST(test_schema_operations);
    RUN_TEST(test_schema_list);
    RUN_TEST(test_transactions);
    RUN_TEST(test_tree_name_builders);
    RUN_TEST(test_error_handling);
    RUN_TEST(test_sync);
    RUN_TEST(test_oid_to_rowid);

    printf("\n=== All tests passed! ===\n");
    return 0;
}
