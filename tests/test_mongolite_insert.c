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

/* ============================================================
 * Additional Coverage Tests
 * ============================================================ */

static void test_insert_null_params(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);

    rc = mongolite_collection_create(db, "test", NULL, &error);
    assert_int_equal(0, rc);

    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "test");

    /* NULL db */
    rc = mongolite_insert_one(NULL, "test", doc, NULL, &error);
    assert_int_not_equal(0, rc);

    /* NULL collection */
    error.code = 0;
    rc = mongolite_insert_one(db, NULL, doc, NULL, &error);
    assert_int_not_equal(0, rc);

    /* NULL doc */
    error.code = 0;
    rc = mongolite_insert_one(db, "test", NULL, NULL, &error);
    assert_int_not_equal(0, rc);

    bson_destroy(doc);
    mongolite_close(db);
}

static void test_insert_many_null_params(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);

    rc = mongolite_collection_create(db, "test", NULL, &error);
    assert_int_equal(0, rc);

    bson_t *doc = bson_new();
    const bson_t *docs[] = { doc };

    /* NULL db */
    rc = mongolite_insert_many(NULL, "test", docs, 1, NULL, &error);
    assert_int_not_equal(0, rc);

    /* NULL collection */
    error.code = 0;
    rc = mongolite_insert_many(db, NULL, docs, 1, NULL, &error);
    assert_int_not_equal(0, rc);

    /* NULL docs */
    error.code = 0;
    rc = mongolite_insert_many(db, "test", NULL, 1, NULL, &error);
    assert_int_not_equal(0, rc);

    /* Zero count */
    error.code = 0;
    rc = mongolite_insert_many(db, "test", docs, 0, NULL, &error);
    assert_int_not_equal(0, rc);

    bson_destroy(doc);
    mongolite_close(db);
}

static void test_insert_with_non_oid_id(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);

    rc = mongolite_collection_create(db, "test", NULL, &error);
    assert_int_equal(0, rc);

    /* Document with string _id instead of OID */
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "_id", "my_string_id");
    BSON_APPEND_UTF8(doc, "name", "Test");

    bson_oid_t returned_id;
    rc = mongolite_insert_one(db, "test", doc, &returned_id, &error);
    assert_int_equal(0, rc);  /* Should succeed - generates internal OID */

    bson_destroy(doc);
    mongolite_close(db);
}

static void test_insert_with_int_id(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);

    rc = mongolite_collection_create(db, "test", NULL, &error);
    assert_int_equal(0, rc);

    /* Document with integer _id */
    bson_t *doc = bson_new();
    BSON_APPEND_INT32(doc, "_id", 12345);
    BSON_APPEND_UTF8(doc, "name", "Integer ID Test");

    bson_oid_t returned_id;
    rc = mongolite_insert_one(db, "test", doc, &returned_id, &error);
    assert_int_equal(0, rc);

    bson_destroy(doc);
    mongolite_close(db);
}

static void test_insert_json_null_params(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);

    rc = mongolite_collection_create(db, "test", NULL, &error);
    assert_int_equal(0, rc);

    /* NULL db */
    rc = mongolite_insert_one_json(NULL, "test", "{\"a\":1}", NULL, &error);
    assert_int_not_equal(0, rc);

    /* NULL collection */
    error.code = 0;
    rc = mongolite_insert_one_json(db, NULL, "{\"a\":1}", NULL, &error);
    assert_int_not_equal(0, rc);

    /* NULL json */
    error.code = 0;
    rc = mongolite_insert_one_json(db, "test", NULL, NULL, &error);
    assert_int_not_equal(0, rc);

    mongolite_close(db);
}

static void test_insert_many_json_null_params(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);

    rc = mongolite_collection_create(db, "test", NULL, &error);
    assert_int_equal(0, rc);

    const char *json_docs[] = { "{\"a\":1}" };

    /* NULL db */
    rc = mongolite_insert_many_json(NULL, "test", json_docs, 1, NULL, &error);
    assert_int_not_equal(0, rc);

    /* NULL collection */
    error.code = 0;
    rc = mongolite_insert_many_json(db, NULL, json_docs, 1, NULL, &error);
    assert_int_not_equal(0, rc);

    /* NULL json_docs */
    error.code = 0;
    rc = mongolite_insert_many_json(db, "test", NULL, 1, NULL, &error);
    assert_int_not_equal(0, rc);

    /* Zero count */
    error.code = 0;
    rc = mongolite_insert_many_json(db, "test", json_docs, 0, NULL, &error);
    assert_int_not_equal(0, rc);

    mongolite_close(db);
}

static void test_insert_many_with_ids(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);

    rc = mongolite_collection_create(db, "test", NULL, &error);
    assert_int_equal(0, rc);

    /* Create docs, some with _id, some without */
    bson_t *docs[3];
    bson_oid_t preset_id;
    bson_oid_init(&preset_id, NULL);

    docs[0] = bson_new();
    BSON_APPEND_OID(docs[0], "_id", &preset_id);
    BSON_APPEND_INT32(docs[0], "value", 1);

    docs[1] = bson_new();
    BSON_APPEND_INT32(docs[1], "value", 2);  /* No _id */

    docs[2] = bson_new();
    BSON_APPEND_INT32(docs[2], "value", 3);  /* No _id */

    bson_oid_t returned_ids_arr[3];
    bson_oid_t *returned_ids = returned_ids_arr;
    rc = mongolite_insert_many(db, "test", (const bson_t**)docs, 3, &returned_ids, &error);
    assert_int_equal(0, rc);

    /* First returned ID should match preset_id */
    assert_int_equal(0, bson_oid_compare(&returned_ids[0], &preset_id));

    for (int i = 0; i < 3; i++) {
        bson_destroy(docs[i]);
    }
    mongolite_close(db);
}

static void test_insert_empty_doc(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);

    rc = mongolite_collection_create(db, "test", NULL, &error);
    assert_int_equal(0, rc);

    /* Empty document (just {}) - should get _id added */
    bson_t *doc = bson_new();

    bson_oid_t inserted_id;
    rc = mongolite_insert_one(db, "test", doc, &inserted_id, &error);
    assert_int_equal(0, rc);

    /* Verify document was inserted */
    int64_t count = mongolite_collection_count(db, "test", NULL, &error);
    assert_int_equal(1, count);

    bson_destroy(doc);
    mongolite_close(db);
}

static void test_insert_complex_document(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);

    rc = mongolite_collection_create(db, "test", NULL, &error);
    assert_int_equal(0, rc);

    /* Complex nested document */
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "Complex");
    BSON_APPEND_INT64(doc, "big_number", 9876543210123LL);
    BSON_APPEND_DOUBLE(doc, "float_val", 3.14159265);
    BSON_APPEND_BOOL(doc, "active", true);
    BSON_APPEND_NULL(doc, "nullable");

    /* Nested object */
    bson_t nested;
    BSON_APPEND_DOCUMENT_BEGIN(doc, "nested", &nested);
    BSON_APPEND_UTF8(&nested, "key", "value");
    BSON_APPEND_INT32(&nested, "num", 42);
    bson_append_document_end(doc, &nested);

    /* Array */
    bson_t arr;
    BSON_APPEND_ARRAY_BEGIN(doc, "tags", &arr);
    BSON_APPEND_UTF8(&arr, "0", "tag1");
    BSON_APPEND_UTF8(&arr, "1", "tag2");
    BSON_APPEND_UTF8(&arr, "2", "tag3");
    bson_append_array_end(doc, &arr);

    bson_oid_t inserted_id;
    rc = mongolite_insert_one(db, "test", doc, &inserted_id, &error);
    assert_int_equal(0, rc);

    /* Retrieve and verify */
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &inserted_id);

    bson_t *found = mongolite_find_one(db, "test", filter, NULL, &error);
    assert_non_null(found);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found, "name"));
    assert_string_equal("Complex", bson_iter_utf8(&iter, NULL));

    assert_true(bson_iter_init_find(&iter, found, "big_number"));
    assert_true(bson_iter_int64(&iter) == 9876543210123LL);

    bson_destroy(filter);
    bson_destroy(found);
    bson_destroy(doc);
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
        /* Additional coverage tests */
        cmocka_unit_test_setup_teardown(test_insert_null_params, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_many_null_params, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_with_non_oid_id, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_with_int_id, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_json_null_params, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_many_json_null_params, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_many_with_ids, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_empty_doc, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_complex_document, setup, teardown),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
