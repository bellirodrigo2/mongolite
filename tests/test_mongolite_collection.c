// test_mongolite_collection.c - Tests for collection operations (cmocka)

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <stdlib.h>

#include "mongolite_internal.h"

#define TEST_DB_PATH "./test_mongolite_col"

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

static void test_collection_create(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "users", NULL, &error);
    assert_int_equal(0, rc);
    
    bool exists = mongolite_collection_exists(db, "users", &error);
    assert_true(exists);
    
    rc = mongolite_collection_create(db, "users", NULL, &error);
    assert_int_equal(MONGOLITE_EEXISTS, rc);
    
    mongolite_close(db);
}

static void test_collection_create_with_config(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t dbconfig = {0};
    dbconfig.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &dbconfig, &error);
    assert_int_equal(0, rc);
    
    col_config_t config = {0};
    bson_t *metadata = bson_new();
    BSON_APPEND_UTF8(metadata, "description", "User accounts");
    BSON_APPEND_INT32(metadata, "version", 1);
    config.metadata = metadata;
    
    rc = mongolite_collection_create(db, "users", &config, &error);
    assert_int_equal(0, rc);
    
    const bson_t *stored = mongolite_collection_metadata(db, "users", &error);
    assert_non_null(stored);
    
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, stored, "description"));
    assert_string_equal("User accounts", bson_iter_utf8(&iter, NULL));
    
    bson_destroy((bson_t*)stored);
    bson_destroy(metadata);
    mongolite_close(db);
}

static void test_collection_drop(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "temp", NULL, &error);
    assert_int_equal(0, rc);
    assert_true(mongolite_collection_exists(db, "temp", NULL));
    
    rc = mongolite_collection_drop(db, "temp", &error);
    assert_int_equal(0, rc);
    
    assert_false(mongolite_collection_exists(db, "temp", NULL));
    
    rc = mongolite_collection_drop(db, "nonexistent", &error);
    assert_int_not_equal(0, rc);
    
    mongolite_close(db);
}

static void test_collection_list(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    size_t count = 0;
    char **list = mongolite_collection_list(db, &count, &error);
    assert_int_equal(0, count);
    mongolite_collection_list_free(list, count);
    
    rc = mongolite_collection_create(db, "users", NULL, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "products", NULL, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "orders", NULL, &error);
    assert_int_equal(0, rc);
    
    list = mongolite_collection_list(db, &count, &error);
    assert_int_equal(3, count);
    
    mongolite_collection_list_free(list, count);
    mongolite_close(db);
}

static void test_collection_exists(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    assert_false(mongolite_collection_exists(db, "nope", NULL));
    
    rc = mongolite_collection_create(db, "test", NULL, &error);
    assert_int_equal(0, rc);
    assert_true(mongolite_collection_exists(db, "test", NULL));
    
    mongolite_close(db);
}

static void test_collection_count_empty(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "empty", NULL, &error);
    assert_int_equal(0, rc);
    
    int64_t count = mongolite_collection_count(db, "empty", NULL, &error);
    assert_int_equal(0, count);
    
    mongolite_close(db);
}

static void test_collection_metadata(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "meta_test", NULL, &error);
    assert_int_equal(0, rc);
    
    const bson_t *meta = mongolite_collection_metadata(db, "meta_test", &error);
    assert_null(meta);
    
    bson_t *new_meta = bson_new();
    BSON_APPEND_UTF8(new_meta, "owner", "admin");
    BSON_APPEND_INT32(new_meta, "priority", 5);
    
    rc = mongolite_collection_set_metadata(db, "meta_test", new_meta, &error);
    assert_int_equal(0, rc);
    
    meta = mongolite_collection_metadata(db, "meta_test", &error);
    assert_non_null(meta);
    
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, meta, "owner"));
    assert_string_equal("admin", bson_iter_utf8(&iter, NULL));
    
    bson_destroy((bson_t*)meta);
    bson_destroy(new_meta);
    mongolite_close(db);
}

static void test_collection_persistence(void **state) {
    (void)state;
    gerror_t error = {0};
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    
    // First session
    {
        mongolite_db_t *db = NULL;
        int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
        assert_int_equal(0, rc);
        
        rc = mongolite_collection_create(db, "persistent1", NULL, &error);
        assert_int_equal(0, rc);
        
        rc = mongolite_collection_create(db, "persistent2", NULL, &error);
        assert_int_equal(0, rc);
        
        mongolite_close(db);
    }
    
    // Second session
    {
        mongolite_db_t *db = NULL;
        int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
        assert_int_equal(0, rc);
        
        assert_true(mongolite_collection_exists(db, "persistent1", NULL));
        assert_true(mongolite_collection_exists(db, "persistent2", NULL));
        
        size_t count = 0;
        char **list = mongolite_collection_list(db, &count, &error);
        assert_int_equal(2, count);
        mongolite_collection_list_free(list, count);
        
        mongolite_close(db);
    }
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_collection_create, setup, teardown),
        cmocka_unit_test_setup_teardown(test_collection_create_with_config, setup, teardown),
        cmocka_unit_test_setup_teardown(test_collection_drop, setup, teardown),
        cmocka_unit_test_setup_teardown(test_collection_list, setup, teardown),
        cmocka_unit_test_setup_teardown(test_collection_exists, setup, teardown),
        cmocka_unit_test_setup_teardown(test_collection_count_empty, setup, teardown),
        cmocka_unit_test_setup_teardown(test_collection_metadata, setup, teardown),
        cmocka_unit_test_setup_teardown(test_collection_persistence, setup, teardown),
    };
    
    return cmocka_run_group_tests(tests, NULL, NULL);
}
