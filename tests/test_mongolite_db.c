// test_mongolite_db.c - Tests for database core operations (cmocka)
// Note: Schema-related tests removed - schema system eliminated

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <stdlib.h>

#include "mongolite_internal.h"

#define TEST_DB_PATH "./test_mongolite_db"

static void cleanup_test_db(void) {
    system("rm -rf " TEST_DB_PATH);
}

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

static void test_open_close(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    assert_int_equal(0, rc);
    assert_non_null(db);

    const char *filename = mongolite_db_filename(db);
    assert_non_null(filename);
    assert_string_equal(TEST_DB_PATH, filename);

    const char *version = mongolite_version();
    assert_non_null(version);

    rc = mongolite_close(db);
    assert_int_equal(0, rc);
}

static void test_open_with_config(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    config.max_dbs = 64;

    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    assert_non_null(db);

    mongolite_close(db);
}

static void test_reopen_database(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;

    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);

    /* Create a collection to verify persistence */
    rc = mongolite_collection_create(db, "reopen_test", NULL, &error);
    assert_int_equal(0, rc);

    mongolite_close(db);

    /* Reopen */
    db = NULL;
    rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    assert_non_null(db);

    /* Verify collection persists */
    assert_true(mongolite_collection_exists(db, "reopen_test", NULL));

    mongolite_close(db);
}

static void test_transactions(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);

    /* Create a collection for transaction tests */
    rc = mongolite_collection_create(db, "txn_collection", NULL, &error);
    assert_int_equal(0, rc);

    /* Begin transaction and insert */
    rc = mongolite_begin_transaction(db);
    assert_int_equal(0, rc);

    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "txn_test");
    rc = mongolite_insert_one(db, "txn_collection", doc, NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc);

    rc = mongolite_commit(db);
    assert_int_equal(0, rc);

    /* Verify committed */
    int64_t count = mongolite_collection_count(db, "txn_collection", NULL, &error);
    assert_int_equal(1, count);

    /* Begin transaction and insert, then rollback */
    rc = mongolite_begin_transaction(db);
    assert_int_equal(0, rc);

    doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "rollback_test");
    rc = mongolite_insert_one(db, "txn_collection", doc, NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc);

    rc = mongolite_rollback(db);
    assert_int_equal(0, rc);

    /* Verify rollback - still 1 document */
    count = mongolite_collection_count(db, "txn_collection", NULL, &error);
    assert_int_equal(1, count);

    mongolite_close(db);
}

static void test_tree_name_builders(void **state) {
    (void)state;

    char *col_tree = _mongolite_collection_tree_name("users");
    assert_non_null(col_tree);
    assert_string_equal("col:users", col_tree);
    free(col_tree);

    char *idx_tree = _mongolite_index_tree_name("users", "email_1");
    assert_non_null(idx_tree);
    assert_string_equal("idx:users:email_1", idx_tree);
    free(idx_tree);
}

static void test_error_handling(void **state) {
    (void)state;
    gerror_t error = {0};
    mongolite_db_t *db = NULL;

    int rc = mongolite_open(NULL, &db, NULL, &error);
    assert_int_not_equal(0, rc);

    rc = mongolite_open(TEST_DB_PATH, NULL, NULL, &error);
    assert_int_not_equal(0, rc);

    const char *errstr = mongolite_errstr(0);
    assert_non_null(errstr);
    assert_string_equal("Success", errstr);

    errstr = mongolite_errstr(-4);
    assert_non_null(errstr);
}

static void test_sync(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;

    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);

    /* Create a collection */
    rc = mongolite_collection_create(db, "sync_test", NULL, &error);
    assert_int_equal(0, rc);

    /* Sync to disk */
    rc = mongolite_sync(db, true, &error);
    assert_int_equal(0, rc);

    mongolite_close(db);
}

static void test_oid_to_rowid(void **state) {
    (void)state;
    bson_oid_t oid1, oid2;
    bson_oid_init(&oid1, NULL);
    bson_oid_init(&oid2, NULL);

    int64_t rowid1 = _mongolite_oid_to_rowid(&oid1);
    int64_t rowid2 = _mongolite_oid_to_rowid(&oid2);

    assert_int_not_equal(rowid1, rowid2);

    int64_t rowid1_again = _mongolite_oid_to_rowid(&oid1);
    assert_int_equal(rowid1, rowid1_again);

    int64_t null_rowid = _mongolite_oid_to_rowid(NULL);
    assert_int_equal(0, null_rowid);
}

static void test_last_insert_rowid(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;

    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);

    /* Create a collection */
    rc = mongolite_collection_create(db, "rowid_test", NULL, &error);
    assert_int_equal(0, rc);

    /* Insert a document */
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "test");
    bson_oid_t oid;
    rc = mongolite_insert_one(db, "rowid_test", doc, &oid, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc);

    /* Check last_insert_rowid */
    int64_t rowid = mongolite_last_insert_rowid(db);
    assert_true(rowid != 0);

    /* Test with NULL db */
    rowid = mongolite_last_insert_rowid(NULL);
    assert_int_equal(0, rowid);

    mongolite_close(db);
}

static void test_changes_counter(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;

    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);

    rc = mongolite_collection_create(db, "changes_test", NULL, &error);
    assert_int_equal(0, rc);

    /* Initial changes should be 0 */
    int changes = mongolite_changes(db);
    assert_int_equal(0, changes);

    /* Insert should set changes to 1 */
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "test");
    rc = mongolite_insert_one(db, "changes_test", doc, NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc);

    changes = mongolite_changes(db);
    assert_int_equal(1, changes);

    /* Test with NULL */
    changes = mongolite_changes(NULL);
    assert_int_equal(0, changes);

    mongolite_close(db);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_open_close, setup, teardown),
        cmocka_unit_test_setup_teardown(test_open_with_config, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reopen_database, setup, teardown),
        /* test_schema_operations removed - schema system eliminated */
        /* test_schema_list removed - schema system eliminated */
        cmocka_unit_test_setup_teardown(test_transactions, setup, teardown),
        cmocka_unit_test(test_tree_name_builders),
        cmocka_unit_test_setup_teardown(test_error_handling, setup, teardown),
        cmocka_unit_test_setup_teardown(test_sync, setup, teardown),
        cmocka_unit_test(test_oid_to_rowid),
        cmocka_unit_test_setup_teardown(test_last_insert_rowid, setup, teardown),
        /* test_set_metadata removed - schema system eliminated */
        cmocka_unit_test_setup_teardown(test_changes_counter, setup, teardown),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
