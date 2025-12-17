/*
 * test_mongolite_db_cmocka.c - Tests for Phase 1: Database core operations using CMocka
 *
 * Tests:
 * - Database open/close
 * - Schema operations
 * - Metadata handling
 * - Transaction basics
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <stdint.h>
#include <cmocka.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mongolite_internal.h"

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
#define TEST_DB_PATH "./test_mongolite_db_cmocka"

/* ============================================================
 * Helper: Remove test database directory
 * ============================================================ */

static void cleanup_test_db(void) {
#ifdef _WIN32
    /* Windows: use shell command for simplicity */
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "cmd /c if exist \"%s\" rmdir /s /q \"%s\" 2>nul", TEST_DB_PATH, TEST_DB_PATH);
    system(cmd);
    /* Small delay to ensure files are released */
    Sleep(100);
#else
    /* Unix: recursively remove directory */
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DB_PATH);
    system(cmd);
    usleep(100000);
#endif
}

/* ============================================================
 * Setup/Teardown functions for CMocka
 * ============================================================ */

static int setup(void **state) {
    (void)state;
    cleanup_test_db();
    return 0;
}

static int teardown(void **state) {
    (void)state;
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Basic open/close
 * ============================================================ */

static void test_open_close(void **state) {
    (void)state;

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    /* Open with default config */
    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    assert_int_equal(rc, 0);
    assert_non_null(db);

    /* Check filename */
    const char *filename = mongolite_db_filename(db);
    assert_non_null(filename);
    assert_string_equal(filename, TEST_DB_PATH);

    /* Check version */
    const char *version = mongolite_version();
    assert_non_null(version);

    /* Close */
    rc = mongolite_close(db);
    assert_int_equal(rc, 0);
}

/* ============================================================
 * Test: Open with custom configuration
 * ============================================================ */

static void test_open_with_config(void **state) {
    (void)state;

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    /* Create config with custom settings */
    db_config_t config = {0};
    config.max_bytes = 512ULL * 1024 * 1024;  /* 512MB */
    config.max_dbs = 64;

    /* Create user metadata */
    bson_t *metadata = bson_new();
    BSON_APPEND_UTF8(metadata, "app_name", "test_app");
    BSON_APPEND_UTF8(metadata, "version", "1.0.0");
    BSON_APPEND_INT32(metadata, "schema_version", 1);
    config.metadata = metadata;

    /* Open database */
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(rc, 0);

    /* Verify metadata is accessible */
    const bson_t *stored_metadata = mongolite_db_metadata(db);
    assert_non_null(stored_metadata);

    /* Check metadata content */
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, stored_metadata, "app_name"));
    assert_string_equal(bson_iter_utf8(&iter, NULL), "test_app");

    bson_destroy(metadata);
    mongolite_close(db);
}

/* ============================================================
 * Test: Reopen existing database
 * ============================================================ */

static void test_reopen_database(void **state) {
    (void)state;

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    /* First open - create database */
    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    assert_int_equal(rc, 0);

    /* Set some metadata */
    bson_t *metadata = bson_new();
    BSON_APPEND_INT32(metadata, "counter", 42);
    rc = mongolite_db_set_metadata(db, metadata, &error);
    assert_int_equal(rc, 0);

    bson_destroy(metadata);
    mongolite_close(db);

    /* Reopen */
    db = NULL;
    rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    assert_int_equal(rc, 0);
    assert_non_null(db);

    /* Close */
    mongolite_close(db);
}

/* ============================================================
 * Test: Schema entry operations
 * ============================================================ */

static void test_schema_operations(void **state) {
    (void)state;

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    assert_int_equal(rc, 0);

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
    assert_int_equal(rc, 0);

    /* Read it back */
    mongolite_schema_entry_t read_entry = {0};
    rc = _mongolite_schema_get(db, "test_collection", &read_entry, &error);
    assert_int_equal(rc, 0);

    /* Verify fields */
    assert_non_null(read_entry.name);
    assert_string_equal(read_entry.name, "test_collection");
    assert_string_equal(read_entry.type, SCHEMA_TYPE_COLLECTION);
    assert_int_equal(bson_oid_compare(&read_entry.oid, &entry.oid), 0);

    /* Check metadata was preserved */
    assert_non_null(read_entry.metadata);
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, read_entry.metadata, "description"));

    /* Update the entry */
    entry.doc_count = 100;
    entry.modified_at = _mongolite_now_ms();
    rc = _mongolite_schema_put(db, &entry, &error);
    assert_int_equal(rc, 0);

    /* Read again and verify update */
    _mongolite_schema_entry_free(&read_entry);
    rc = _mongolite_schema_get(db, "test_collection", &read_entry, &error);
    assert_int_equal(rc, 0);
    assert_int_equal(read_entry.doc_count, 100);

    /* Delete the entry */
    rc = _mongolite_schema_delete(db, "test_collection", &error);
    assert_int_equal(rc, 0);

    /* Verify it's gone */
    _mongolite_schema_entry_free(&read_entry);
    rc = _mongolite_schema_get(db, "test_collection", &read_entry, &error);
    assert_true(rc != 0);

    /* Cleanup */
    _mongolite_schema_entry_free(&entry);
    _mongolite_schema_entry_free(&read_entry);
    mongolite_close(db);
}

/* ============================================================
 * Test: Schema list operation
 * ============================================================ */

static void test_schema_list(void **state) {
    (void)state;

    /* Ensure clean database - explicit cleanup in addition to setup */
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    assert_int_equal(rc, 0);

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
        assert_int_equal(rc, 0);

        _mongolite_schema_entry_free(&entry);
    }

    /* List all collections */
    char **list = NULL;
    size_t count = 0;
    rc = _mongolite_schema_list(db, &list, &count, SCHEMA_TYPE_COLLECTION, &error);
    assert_int_equal(rc, 0);

    /* Debug: print collections if count doesn't match */
    if (count != 3) {
        printf("\nExpected 3 collections but found %zu:\n", count);
        for (size_t i = 0; i < count; i++) {
            printf("  - %s\n", list[i]);
        }
    }

    assert_int_equal(count, 3);

    for (size_t i = 0; i < count; i++) {
        free(list[i]);
    }
    free(list);

    mongolite_close(db);
}

/* ============================================================
 * Test: Transaction operations
 * ============================================================ */

static void test_transactions(void **state) {
    (void)state;

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    assert_int_equal(rc, 0);

    /* Begin transaction */
    rc = mongolite_begin_transaction(db);
    assert_int_equal(rc, 0);

    /* Create a schema entry within transaction */
    mongolite_schema_entry_t entry = {0};
    bson_oid_init(&entry.oid, NULL);
    entry.name = strdup("txn_test");
    entry.tree_name = _mongolite_collection_tree_name("txn_test");
    entry.type = strdup(SCHEMA_TYPE_COLLECTION);
    entry.created_at = _mongolite_now_ms();
    entry.modified_at = entry.created_at;

    rc = _mongolite_schema_put(db, &entry, &error);
    assert_int_equal(rc, 0);

    /* Commit */
    rc = mongolite_commit(db);
    assert_int_equal(rc, 0);

    /* Verify entry exists after commit */
    mongolite_schema_entry_t read_entry = {0};
    rc = _mongolite_schema_get(db, "txn_test", &read_entry, &error);
    assert_int_equal(rc, 0);
    _mongolite_schema_entry_free(&read_entry);

    /* Test rollback */
    rc = mongolite_begin_transaction(db);
    assert_int_equal(rc, 0);

    /* Delete entry in transaction */
    rc = _mongolite_schema_delete(db, "txn_test", &error);
    assert_int_equal(rc, 0);

    /* Rollback */
    rc = mongolite_rollback(db);
    assert_int_equal(rc, 0);

    /* Verify entry still exists after rollback */
    rc = _mongolite_schema_get(db, "txn_test", &read_entry, &error);
    assert_int_equal(rc, 0);

    _mongolite_schema_entry_free(&entry);
    _mongolite_schema_entry_free(&read_entry);
    mongolite_close(db);
}

/* ============================================================
 * Test: Tree name builders
 * ============================================================ */

static void test_tree_name_builders(void **state) {
    (void)state;

    /* Collection tree name */
    char *col_tree = _mongolite_collection_tree_name("users");
    assert_non_null(col_tree);
    assert_string_equal(col_tree, "col:users");
    free(col_tree);

    /* Index tree name */
    char *idx_tree = _mongolite_index_tree_name("users", "email_1");
    assert_non_null(idx_tree);
    assert_string_equal(idx_tree, "idx:users:email_1");
    free(idx_tree);
}

/* ============================================================
 * Test: Error handling
 * ============================================================ */

static void test_error_handling(void **state) {
    (void)state;

    gerror_t error = {0};

    /* Test opening invalid path */
    mongolite_db_t *db = NULL;

    /* Test NULL parameters */
    int rc = mongolite_open(NULL, &db, NULL, &error);
    assert_true(rc != 0);

    rc = mongolite_open(TEST_DB_PATH, NULL, NULL, &error);
    assert_true(rc != 0);

    /* Test errstr */
    const char *errstr = mongolite_errstr(0);
    assert_non_null(errstr);
    assert_string_equal(errstr, "Success");

    errstr = mongolite_errstr(-1003);  /* MONGOLITE_EINVAL */
    assert_non_null(errstr);
    assert_string_equal(errstr, "Invalid argument");
}

/* ============================================================
 * Test: Sync operation
 * ============================================================ */

static void test_sync(void **state) {
    (void)state;

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    assert_int_equal(rc, 0);

    /* Create some data */
    mongolite_schema_entry_t entry = {0};
    bson_oid_init(&entry.oid, NULL);
    entry.name = strdup("sync_test");
    entry.tree_name = _mongolite_collection_tree_name("sync_test");
    entry.type = strdup(SCHEMA_TYPE_COLLECTION);
    entry.created_at = _mongolite_now_ms();
    entry.modified_at = entry.created_at;

    rc = _mongolite_schema_put(db, &entry, &error);
    assert_int_equal(rc, 0);

    /* Force sync */
    rc = mongolite_sync(db, true, &error);
    assert_int_equal(rc, 0);

    _mongolite_schema_entry_free(&entry);
    mongolite_close(db);
}

/* ============================================================
 * Test: OID to rowid conversion
 * ============================================================ */

static void test_oid_to_rowid(void **state) {
    (void)state;

    bson_oid_t oid1, oid2;
    bson_oid_init(&oid1, NULL);
    bson_oid_init(&oid2, NULL);

    int64_t rowid1 = _mongolite_oid_to_rowid(&oid1);
    int64_t rowid2 = _mongolite_oid_to_rowid(&oid2);

    /* Different OIDs should produce different rowids (with high probability) */
    assert_true(rowid1 != rowid2);

    /* Same OID should produce same rowid */
    int64_t rowid1_again = _mongolite_oid_to_rowid(&oid1);
    assert_int_equal(rowid1, rowid1_again);

    /* NULL should return 0 */
    int64_t null_rowid = _mongolite_oid_to_rowid(NULL);
    assert_int_equal(null_rowid, 0);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_open_close, setup, teardown),
        cmocka_unit_test_setup_teardown(test_open_with_config, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reopen_database, setup, teardown),
        cmocka_unit_test_setup_teardown(test_schema_operations, setup, teardown),
        cmocka_unit_test_setup_teardown(test_schema_list, setup, teardown),
        cmocka_unit_test_setup_teardown(test_transactions, setup, teardown),
        cmocka_unit_test(test_tree_name_builders),
        cmocka_unit_test(test_error_handling),
        cmocka_unit_test_setup_teardown(test_sync, setup, teardown),
        cmocka_unit_test(test_oid_to_rowid),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
