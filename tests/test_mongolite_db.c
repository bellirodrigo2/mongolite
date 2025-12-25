// test_mongolite_db.c - Tests for database core operations (cmocka)

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
    
    bson_t *metadata = bson_new();
    BSON_APPEND_UTF8(metadata, "app_name", "test_app");
    BSON_APPEND_UTF8(metadata, "version", "1.0.0");
    BSON_APPEND_INT32(metadata, "schema_version", 1);
    config.metadata = metadata;
    
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    const bson_t *stored_metadata = mongolite_db_metadata(db);
    assert_non_null(stored_metadata);
    
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, stored_metadata, "app_name"));
    assert_string_equal("test_app", bson_iter_utf8(&iter, NULL));
    
    bson_destroy(metadata);
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
    
    bson_t *metadata = bson_new();
    BSON_APPEND_INT32(metadata, "counter", 42);
    rc = mongolite_db_set_metadata(db, metadata, &error);
    assert_int_equal(0, rc);
    
    bson_destroy(metadata);
    mongolite_close(db);
    
    db = NULL;
    rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    assert_non_null(db);
    
    mongolite_close(db);
}

static void test_schema_operations(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    mongolite_schema_entry_t entry = {0};
    bson_oid_init(&entry.oid, NULL);
    entry.name = strdup("test_collection");
    entry.tree_name = _mongolite_collection_tree_name("test_collection");
    entry.type = strdup(SCHEMA_TYPE_COLLECTION);
    entry.created_at = _mongolite_now_ms();
    entry.modified_at = entry.created_at;
    entry.doc_count = 0;
    
    entry.options = bson_new();
    BSON_APPEND_BOOL(entry.options, "capped", false);
    
    entry.metadata = bson_new();
    BSON_APPEND_UTF8(entry.metadata, "description", "A test collection");
    
    rc = _mongolite_schema_put(db, &entry, &error);
    assert_int_equal(0, rc);
    
    mongolite_schema_entry_t read_entry = {0};
    rc = _mongolite_schema_get(db, "test_collection", &read_entry, &error);
    assert_int_equal(0, rc);
    
    assert_non_null(read_entry.name);
    assert_string_equal("test_collection", read_entry.name);
    assert_string_equal(SCHEMA_TYPE_COLLECTION, read_entry.type);
    assert_int_equal(0, bson_oid_compare(&read_entry.oid, &entry.oid));
    
    assert_non_null(read_entry.metadata);
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, read_entry.metadata, "description"));
    
    entry.doc_count = 100;
    entry.modified_at = _mongolite_now_ms();
    rc = _mongolite_schema_put(db, &entry, &error);
    assert_int_equal(0, rc);
    
    _mongolite_schema_entry_free(&read_entry);
    rc = _mongolite_schema_get(db, "test_collection", &read_entry, &error);
    assert_int_equal(0, rc);
    assert_int_equal(100, read_entry.doc_count);
    
    rc = _mongolite_schema_delete(db, "test_collection", &error);
    assert_int_equal(0, rc);
    
    _mongolite_schema_entry_free(&read_entry);
    rc = _mongolite_schema_get(db, "test_collection", &read_entry, &error);
    assert_int_not_equal(0, rc);
    
    _mongolite_schema_entry_free(&entry);
    _mongolite_schema_entry_free(&read_entry);
    mongolite_close(db);
}

static void test_schema_list(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
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
        assert_int_equal(0, rc);
        
        _mongolite_schema_entry_free(&entry);
    }
    
    char **list = NULL;
    size_t count = 0;
    rc = _mongolite_schema_list(db, &list, &count, SCHEMA_TYPE_COLLECTION, &error);
    assert_int_equal(0, rc);
    assert_int_equal(3, count);
    
    for (size_t i = 0; i < count; i++) {
        free(list[i]);
    }
    free(list);
    
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
    
    rc = mongolite_begin_transaction(db);
    assert_int_equal(0, rc);
    
    mongolite_schema_entry_t entry = {0};
    bson_oid_init(&entry.oid, NULL);
    entry.name = strdup("txn_test");
    entry.tree_name = _mongolite_collection_tree_name("txn_test");
    entry.type = strdup(SCHEMA_TYPE_COLLECTION);
    entry.created_at = _mongolite_now_ms();
    entry.modified_at = entry.created_at;
    
    rc = _mongolite_schema_put(db, &entry, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_commit(db);
    assert_int_equal(0, rc);
    
    mongolite_schema_entry_t read_entry = {0};
    rc = _mongolite_schema_get(db, "txn_test", &read_entry, &error);
    assert_int_equal(0, rc);
    _mongolite_schema_entry_free(&read_entry);
    
    rc = mongolite_begin_transaction(db);
    assert_int_equal(0, rc);
    
    rc = _mongolite_schema_delete(db, "txn_test", &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_rollback(db);
    assert_int_equal(0, rc);
    
    rc = _mongolite_schema_get(db, "txn_test", &read_entry, &error);
    assert_int_equal(0, rc);
    
    _mongolite_schema_entry_free(&entry);
    _mongolite_schema_entry_free(&read_entry);
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
    
    mongolite_schema_entry_t entry = {0};
    bson_oid_init(&entry.oid, NULL);
    entry.name = strdup("sync_test");
    entry.tree_name = _mongolite_collection_tree_name("sync_test");
    entry.type = strdup(SCHEMA_TYPE_COLLECTION);
    entry.created_at = _mongolite_now_ms();
    entry.modified_at = entry.created_at;
    
    rc = _mongolite_schema_put(db, &entry, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_sync(db, true, &error);
    assert_int_equal(0, rc);
    
    _mongolite_schema_entry_free(&entry);
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

static void test_set_metadata(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;

    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);

    /* Set metadata */
    bson_t *metadata = bson_new();
    BSON_APPEND_UTF8(metadata, "app", "test_app");
    BSON_APPEND_INT32(metadata, "version", 1);

    rc = mongolite_db_set_metadata(db, metadata, &error);
    assert_int_equal(0, rc);

    /* Read back and verify */
    const bson_t *stored = mongolite_db_metadata(db);
    assert_non_null(stored);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, stored, "app"));
    assert_string_equal("test_app", bson_iter_utf8(&iter, NULL));

    assert_true(bson_iter_init_find(&iter, stored, "version"));
    assert_int_equal(1, bson_iter_int32(&iter));

    bson_destroy(metadata);

    /* Update metadata */
    metadata = bson_new();
    BSON_APPEND_UTF8(metadata, "app", "updated_app");
    BSON_APPEND_INT32(metadata, "version", 2);

    rc = mongolite_db_set_metadata(db, metadata, &error);
    assert_int_equal(0, rc);

    stored = mongolite_db_metadata(db);
    assert_true(bson_iter_init_find(&iter, stored, "version"));
    assert_int_equal(2, bson_iter_int32(&iter));

    bson_destroy(metadata);

    /* Test with NULL db */
    error.code = 0;
    rc = mongolite_db_set_metadata(NULL, metadata, &error);
    assert_int_not_equal(0, rc);

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
        cmocka_unit_test_setup_teardown(test_schema_operations, setup, teardown),
        cmocka_unit_test_setup_teardown(test_schema_list, setup, teardown),
        cmocka_unit_test_setup_teardown(test_transactions, setup, teardown),
        cmocka_unit_test(test_tree_name_builders),
        cmocka_unit_test_setup_teardown(test_error_handling, setup, teardown),
        cmocka_unit_test_setup_teardown(test_sync, setup, teardown),
        cmocka_unit_test(test_oid_to_rowid),
        cmocka_unit_test_setup_teardown(test_last_insert_rowid, setup, teardown),
        cmocka_unit_test_setup_teardown(test_set_metadata, setup, teardown),
        cmocka_unit_test_setup_teardown(test_changes_counter, setup, teardown),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
