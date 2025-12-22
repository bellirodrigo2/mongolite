// test_mongolite_delete.c - Tests for delete operations (cmocka)

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <stdlib.h>

#include "mongolite_internal.h"

#define TEST_DB_PATH "./test_mongolite_delete"

static void cleanup_test_db(void) {
    system("rm -rf " TEST_DB_PATH);
}

static mongolite_db_t* setup_test_db(void) {
    cleanup_test_db();
    
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    if (mongolite_open(TEST_DB_PATH, &db, &config, &error) != 0) {
        return NULL;
    }
    
    if (mongolite_collection_create(db, "users", NULL, &error) != 0) {
        mongolite_close(db);
        return NULL;
    }
    
    const char *users[] = {
        "{\"name\": \"Alice\", \"age\": 30, \"city\": \"NYC\"}",
        "{\"name\": \"Bob\", \"age\": 25, \"city\": \"LA\"}",
        "{\"name\": \"Charlie\", \"age\": 35, \"city\": \"NYC\"}",
        "{\"name\": \"Diana\", \"age\": 28, \"city\": \"Chicago\"}",
        "{\"name\": \"Eve\", \"age\": 30, \"city\": \"LA\"}"
    };
    
    for (int i = 0; i < 5; i++) {
        if (mongolite_insert_one_json(db, "users", users[i], NULL, &error) != 0) {
            mongolite_close(db);
            return NULL;
        }
    }
    
    return db;
}

static int teardown(void **state) {
    (void)state;
    cleanup_test_db();
    return 0;
}

static void test_delete_one_by_id(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users", "{\"name\": \"ToDelete\", \"age\": 99}", &id, &error);
    assert_int_equal(0, rc);
    
    int64_t count_before = mongolite_collection_count(db, "users", NULL, &error);
    assert_int_equal(6, count_before);
    
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);
    
    rc = mongolite_delete_one(db, "users", filter, &error);
    assert_int_equal(0, rc);
    
    bson_destroy(filter);
    
    int64_t count_after = mongolite_collection_count(db, "users", NULL, &error);
    assert_int_equal(5, count_after);
    
    filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);
    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_null(found);
    
    bson_destroy(filter);
    mongolite_close(db);
}

static void test_delete_one_with_filter(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "age", 35);
    
    int rc = mongolite_delete_one(db, "users", filter, &error);
    assert_int_equal(0, rc);
    
    bson_destroy(filter);
    
    int64_t count = mongolite_collection_count(db, "users", NULL, &error);
    assert_int_equal(4, count);
    
    filter = bson_new();
    BSON_APPEND_UTF8(filter, "name", "Charlie");
    
    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_null(found);
    
    bson_destroy(filter);
    mongolite_close(db);
}

static void test_delete_one_not_found(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    int64_t count_before = mongolite_collection_count(db, "users", NULL, &error);
    
    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "age", 999);
    
    int rc = mongolite_delete_one(db, "users", filter, &error);
    assert_int_equal(0, rc);
    
    bson_destroy(filter);
    
    int64_t count_after = mongolite_collection_count(db, "users", NULL, &error);
    assert_int_equal(count_before, count_after);
    
    mongolite_close(db);
}

static void test_delete_many_with_filter(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "age", 30);
    
    int64_t deleted_count = 0;
    int rc = mongolite_delete_many(db, "users", filter, &deleted_count, &error);
    assert_int_equal(0, rc);
    assert_int_equal(2, deleted_count);
    
    bson_destroy(filter);
    
    int64_t count = mongolite_collection_count(db, "users", NULL, &error);
    assert_int_equal(3, count);
    
    filter = bson_new();
    BSON_APPEND_INT32(filter, "age", 30);
    
    mongolite_cursor_t *cursor = mongolite_find(db, "users", filter, NULL, &error);
    assert_non_null(cursor);
    
    const bson_t *doc;
    int found_count = 0;
    while (mongolite_cursor_next(cursor, &doc)) {
        found_count++;
    }
    assert_int_equal(0, found_count);
    
    bson_destroy(filter);
    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

static void test_delete_many_all(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    int64_t deleted_count = 0;
    int rc = mongolite_delete_many(db, "users", NULL, &deleted_count, &error);
    assert_int_equal(0, rc);
    assert_int_equal(5, deleted_count);
    
    int64_t count = mongolite_collection_count(db, "users", NULL, &error);
    assert_int_equal(0, count);
    
    mongolite_close(db);
}

static void test_delete_many_nyc(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "city", "NYC");
    
    int64_t deleted_count = 0;
    int rc = mongolite_delete_many(db, "users", filter, &deleted_count, &error);
    assert_int_equal(0, rc);
    assert_int_equal(2, deleted_count);
    
    bson_destroy(filter);
    
    int64_t count = mongolite_collection_count(db, "users", NULL, &error);
    assert_int_equal(3, count);
    
    filter = bson_new();
    BSON_APPEND_UTF8(filter, "city", "NYC");
    
    int64_t nyc_count = mongolite_collection_count(db, "users", filter, &error);
    assert_int_equal(0, nyc_count);
    
    bson_destroy(filter);
    mongolite_close(db);
}

static void test_delete_from_empty(void **state) {
    (void)state;
    cleanup_test_db();
    
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "empty", NULL, &error);
    assert_int_equal(0, rc);
    
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "name", "Nobody");
    
    rc = mongolite_delete_one(db, "empty", filter, &error);
    assert_int_equal(0, rc);
    
    int64_t deleted_count = 0;
    rc = mongolite_delete_many(db, "empty", filter, &deleted_count, &error);
    assert_int_equal(0, rc);
    assert_int_equal(0, deleted_count);
    
    bson_destroy(filter);
    mongolite_close(db);
}

static void test_delete_changes_counter(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "name", "Bob");
    
    int rc = mongolite_delete_one(db, "users", filter, &error);
    assert_int_equal(0, rc);
    
    int changes = mongolite_changes(db);
    assert_int_equal(1, changes);
    
    bson_destroy(filter);
    
    filter = bson_new();
    BSON_APPEND_INT32(filter, "age", 30);
    
    int64_t deleted_count = 0;
    rc = mongolite_delete_many(db, "users", filter, &deleted_count, &error);
    assert_int_equal(0, rc);
    
    changes = mongolite_changes(db);
    assert_int_equal(2, changes);
    
    bson_destroy(filter);
    mongolite_close(db);
}

static void test_delete_complex_filter(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    bson_t *filter = bson_new();
    bson_t child;
    BSON_APPEND_DOCUMENT_BEGIN(filter, "age", &child);
    BSON_APPEND_INT32(&child, "$gt", 28);
    bson_append_document_end(filter, &child);
    
    int64_t deleted_count = 0;
    int rc = mongolite_delete_many(db, "users", filter, &deleted_count, &error);
    assert_int_equal(0, rc);
    assert_int_equal(3, deleted_count);
    
    bson_destroy(filter);
    
    int64_t count = mongolite_collection_count(db, "users", NULL, &error);
    assert_int_equal(2, count);
    
    mongolite_close(db);
}

static void test_delete_data_integrity(void **state) {
    (void)state;
    cleanup_test_db();
    
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "test", NULL, &error);
    assert_int_equal(0, rc);
    
    bson_oid_t ids[10];
    for (int i = 0; i < 10; i++) {
        bson_t *doc = bson_new();
        BSON_APPEND_INT32(doc, "index", i);
        BSON_APPEND_INT32(doc, "category", i % 3);
        
        rc = mongolite_insert_one(db, "test", doc, &ids[i], &error);
        assert_int_equal(0, rc);
        bson_destroy(doc);
    }
    
    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "category", 1);
    
    int64_t deleted_count = 0;
    rc = mongolite_delete_many(db, "test", filter, &deleted_count, &error);
    assert_int_equal(0, rc);
    assert_int_equal(3, deleted_count);
    
    bson_destroy(filter);
    
    mongolite_cursor_t *cursor = mongolite_find(db, "test", NULL, NULL, &error);
    assert_non_null(cursor);
    
    int found_count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        bson_iter_t iter;
        if (bson_iter_init_find(&iter, doc, "index")) {
            int idx = bson_iter_int32(&iter);
            assert_int_not_equal(1, idx % 3);
        }
        found_count++;
    }
    
    assert_int_equal(7, found_count);
    
    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_teardown(test_delete_one_by_id, teardown),
        cmocka_unit_test_teardown(test_delete_one_with_filter, teardown),
        cmocka_unit_test_teardown(test_delete_one_not_found, teardown),
        cmocka_unit_test_teardown(test_delete_many_with_filter, teardown),
        cmocka_unit_test_teardown(test_delete_many_all, teardown),
        cmocka_unit_test_teardown(test_delete_many_nyc, teardown),
        cmocka_unit_test_teardown(test_delete_from_empty, teardown),
        cmocka_unit_test_teardown(test_delete_changes_counter, teardown),
        cmocka_unit_test_teardown(test_delete_complex_filter, teardown),
        cmocka_unit_test_teardown(test_delete_data_integrity, teardown),
    };
    
    return cmocka_run_group_tests(tests, NULL, NULL);
}
