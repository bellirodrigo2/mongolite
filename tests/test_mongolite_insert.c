// test_mongolite_insert.c - Tests for insert operations (cmocka)

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <stdlib.h>

#include "mongolite_internal.h"

#define TEST_DB_PATH "./test_mongolite_insert"

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

static void test_insert_one_auto_id(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "users", NULL, &error);
    assert_int_equal(0, rc);
    
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "Alice");
    BSON_APPEND_INT32(doc, "age", 30);
    
    bson_oid_t inserted_id;
    rc = mongolite_insert_one(db, "users", doc, &inserted_id, &error);
    assert_int_equal(0, rc);
    
    int64_t count = mongolite_collection_count(db, "users", NULL, &error);
    assert_int_equal(1, count);
    
    assert_int_equal(1, mongolite_changes(db));
    
    bson_destroy(doc);
    mongolite_close(db);
}

static void test_insert_one_with_id(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "users", NULL, &error);
    assert_int_equal(0, rc);
    
    bson_oid_t my_id;
    bson_oid_init(&my_id, NULL);
    
    bson_t *doc = bson_new();
    BSON_APPEND_OID(doc, "_id", &my_id);
    BSON_APPEND_UTF8(doc, "name", "Bob");
    
    bson_oid_t inserted_id;
    rc = mongolite_insert_one(db, "users", doc, &inserted_id, &error);
    assert_int_equal(0, rc);
    
    assert_int_equal(0, bson_oid_compare(&inserted_id, &my_id));
    
    bson_destroy(doc);
    mongolite_close(db);
}

static void test_insert_duplicate_id(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "users", NULL, &error);
    assert_int_equal(0, rc);
    
    bson_oid_t my_id;
    bson_oid_init(&my_id, NULL);
    
    bson_t *doc1 = bson_new();
    BSON_APPEND_OID(doc1, "_id", &my_id);
    BSON_APPEND_UTF8(doc1, "name", "First");
    
    bson_t *doc2 = bson_new();
    BSON_APPEND_OID(doc2, "_id", &my_id);
    BSON_APPEND_UTF8(doc2, "name", "Second");
    
    rc = mongolite_insert_one(db, "users", doc1, NULL, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_insert_one(db, "users", doc2, NULL, &error);
    assert_int_not_equal(0, rc);
    
    int64_t count = mongolite_collection_count(db, "users", NULL, &error);
    assert_int_equal(1, count);
    
    bson_destroy(doc1);
    bson_destroy(doc2);
    mongolite_close(db);
}

static void test_insert_many(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "products", NULL, &error);
    assert_int_equal(0, rc);
    
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
    rc = mongolite_insert_many(db, "products", (const bson_t**)docs, 3, &inserted_ids, &error);
    assert_int_equal(0, rc);
    assert_non_null(inserted_ids);
    
    int64_t count = mongolite_collection_count(db, "products", NULL, &error);
    assert_int_equal(3, count);
    
    assert_int_equal(3, mongolite_changes(db));
    
    free(inserted_ids);
    for (int i = 0; i < 3; i++) {
        bson_destroy(docs[i]);
    }
    mongolite_close(db);
}

static void test_insert_one_json(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "events", NULL, &error);
    assert_int_equal(0, rc);
    
    const char *json = "{\"event\": \"click\", \"x\": 100, \"y\": 200}";
    
    bson_oid_t inserted_id;
    rc = mongolite_insert_one_json(db, "events", json, &inserted_id, &error);
    assert_int_equal(0, rc);
    
    int64_t count = mongolite_collection_count(db, "events", NULL, &error);
    assert_int_equal(1, count);
    
    mongolite_close(db);
}

static void test_insert_many_json(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "logs", NULL, &error);
    assert_int_equal(0, rc);
    
    const char *jsons[] = {
        "{\"level\": \"INFO\", \"msg\": \"Started\"}",
        "{\"level\": \"DEBUG\", \"msg\": \"Processing\"}",
        "{\"level\": \"INFO\", \"msg\": \"Completed\"}"
    };
    
    rc = mongolite_insert_many_json(db, "logs", jsons, 3, NULL, &error);
    assert_int_equal(0, rc);
    
    int64_t count = mongolite_collection_count(db, "logs", NULL, &error);
    assert_int_equal(3, count);
    
    mongolite_close(db);
}

static void test_insert_invalid_json(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "test", NULL, &error);
    assert_int_equal(0, rc);
    
    const char *bad_json = "{invalid json}";
    rc = mongolite_insert_one_json(db, "test", bad_json, NULL, &error);
    assert_int_not_equal(0, rc);
    
    mongolite_close(db);
}

static void test_insert_no_collection(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "test", "value");
    
    rc = mongolite_insert_one(db, "nonexistent", doc, NULL, &error);
    assert_int_not_equal(0, rc);
    
    bson_destroy(doc);
    mongolite_close(db);
}

static void test_insert_large_batch(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "batch", NULL, &error);
    assert_int_equal(0, rc);
    
    const size_t N = 100;
    bson_t **docs = calloc(N, sizeof(bson_t*));
    assert_non_null(docs);
    
    for (size_t i = 0; i < N; i++) {
        docs[i] = bson_new();
        BSON_APPEND_INT32(docs[i], "index", (int32_t)i);
        BSON_APPEND_UTF8(docs[i], "data", "test data for batch insert");
    }
    
    rc = mongolite_insert_many(db, "batch", (const bson_t**)docs, N, NULL, &error);
    assert_int_equal(0, rc);
    
    int64_t count = mongolite_collection_count(db, "batch", NULL, &error);
    assert_int_equal((int64_t)N, count);
    
    for (size_t i = 0; i < N; i++) {
        bson_destroy(docs[i]);
    }
    free(docs);
    mongolite_close(db);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_insert_one_auto_id, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_one_with_id, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_duplicate_id, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_many, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_one_json, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_many_json, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_invalid_json, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_no_collection, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_large_batch, setup, teardown),
    };
    
    return cmocka_run_group_tests(tests, NULL, NULL);
}
