// test_mongolite_integration.c - Full-cycle integration tests (cmocka)

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <stdlib.h>

#include "mongolite_internal.h"

#define TEST_DB_PATH "./test_mongolite_integration"

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

static void test_basic_full_cycle(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "products", NULL, &error);
    assert_int_equal(0, rc);
    
    bson_oid_t ids[3];
    
    bson_t *doc1 = bson_new();
    BSON_APPEND_UTF8(doc1, "name", "Laptop");
    BSON_APPEND_DOUBLE(doc1, "price", 999.99);
    BSON_APPEND_INT32(doc1, "stock", 50);
    rc = mongolite_insert_one(db, "products", doc1, &ids[0], &error);
    assert_int_equal(0, rc);
    bson_destroy(doc1);
    
    bson_t *doc2 = bson_new();
    BSON_APPEND_UTF8(doc2, "name", "Mouse");
    BSON_APPEND_DOUBLE(doc2, "price", 29.99);
    BSON_APPEND_INT32(doc2, "stock", 200);
    rc = mongolite_insert_one(db, "products", doc2, &ids[1], &error);
    assert_int_equal(0, rc);
    bson_destroy(doc2);
    
    bson_t *doc3 = bson_new();
    BSON_APPEND_UTF8(doc3, "name", "Keyboard");
    BSON_APPEND_DOUBLE(doc3, "price", 79.99);
    BSON_APPEND_INT32(doc3, "stock", 100);
    rc = mongolite_insert_one(db, "products", doc3, &ids[2], &error);
    assert_int_equal(0, rc);
    bson_destroy(doc3);
    
    int64_t count = mongolite_collection_count(db, "products", NULL, &error);
    assert_int_equal(3, count);
    
    for (int i = 0; i < 3; i++) {
        bson_t *filter = bson_new();
        BSON_APPEND_OID(filter, "_id", &ids[i]);
        
        bson_t *found = mongolite_find_one(db, "products", filter, NULL, &error);
        assert_non_null(found);
        
        bson_iter_t iter;
        assert_true(bson_iter_init_find(&iter, found, "name"));
        
        bson_destroy(filter);
        bson_destroy(found);
    }
    
    mongolite_cursor_t *cursor = mongolite_find(db, "products", NULL, NULL, &error);
    assert_non_null(cursor);
    
    int found_count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        found_count++;
    }
    assert_int_equal(3, found_count);
    
    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

static void test_multiple_collections(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "users", NULL, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "orders", NULL, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "products", NULL, &error);
    assert_int_equal(0, rc);
    
    bson_oid_t user_id;
    bson_t *user = bson_new();
    BSON_APPEND_UTF8(user, "name", "John Doe");
    BSON_APPEND_UTF8(user, "email", "john@example.com");
    rc = mongolite_insert_one(db, "users", user, &user_id, &error);
    assert_int_equal(0, rc);
    bson_destroy(user);
    
    bson_oid_t product_id;
    bson_t *product = bson_new();
    BSON_APPEND_UTF8(product, "name", "Widget");
    BSON_APPEND_DOUBLE(product, "price", 19.99);
    rc = mongolite_insert_one(db, "products", product, &product_id, &error);
    assert_int_equal(0, rc);
    bson_destroy(product);
    
    bson_t *order = bson_new();
    BSON_APPEND_OID(order, "user_id", &user_id);
    BSON_APPEND_OID(order, "product_id", &product_id);
    BSON_APPEND_INT32(order, "quantity", 2);
    BSON_APPEND_DOUBLE(order, "total", 39.98);
    rc = mongolite_insert_one(db, "orders", order, NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(order);
    
    assert_int_equal(1, mongolite_collection_count(db, "users", NULL, &error));
    assert_int_equal(1, mongolite_collection_count(db, "products", NULL, &error));
    assert_int_equal(1, mongolite_collection_count(db, "orders", NULL, &error));
    
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "user_id", &user_id);
    
    bson_t *found_order = mongolite_find_one(db, "orders", filter, NULL, &error);
    assert_non_null(found_order);
    
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found_order, "quantity"));
    assert_int_equal(2, bson_iter_int32(&iter));
    
    bson_destroy(filter);
    bson_destroy(found_order);
    mongolite_close(db);
}

static void test_data_persistence(void **state) {
    (void)state;
    bson_oid_t saved_id;
    gerror_t error = {0};
    
    // Phase 1: Create and insert
    {
        mongolite_db_t *db = NULL;
        db_config_t config = {0};
        config.max_bytes = 32ULL * 1024 * 1024;
        int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
        assert_int_equal(0, rc);
        
        rc = mongolite_collection_create(db, "persistent", NULL, &error);
        assert_int_equal(0, rc);
        
        bson_t *doc = bson_new();
        BSON_APPEND_UTF8(doc, "message", "Hello, persistence!");
        BSON_APPEND_INT32(doc, "magic", 42);
        
        rc = mongolite_insert_one(db, "persistent", doc, &saved_id, &error);
        assert_int_equal(0, rc);
        
        bson_destroy(doc);
        mongolite_close(db);
    }
    
    // Phase 2: Reopen and verify
    {
        mongolite_db_t *db = NULL;
        db_config_t config = {0};
        config.max_bytes = 32ULL * 1024 * 1024;
        int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
        assert_int_equal(0, rc);
        
        assert_true(mongolite_collection_exists(db, "persistent", &error));
        
        int64_t count = mongolite_collection_count(db, "persistent", NULL, &error);
        assert_int_equal(1, count);
        
        bson_t *filter = bson_new();
        BSON_APPEND_OID(filter, "_id", &saved_id);
        
        bson_t *found = mongolite_find_one(db, "persistent", filter, NULL, &error);
        assert_non_null(found);
        
        bson_iter_t iter;
        assert_true(bson_iter_init_find(&iter, found, "message"));
        assert_string_equal("Hello, persistence!", bson_iter_utf8(&iter, NULL));
        
        assert_true(bson_iter_init_find(&iter, found, "magic"));
        assert_int_equal(42, bson_iter_int32(&iter));
        
        bson_destroy(filter);
        bson_destroy(found);
        mongolite_close(db);
    }
}

static void test_large_dataset(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "large", NULL, &error);
    assert_int_equal(0, rc);
    
    const int N = 1000;
    
    bson_t **docs = calloc(N, sizeof(bson_t*));
    for (int i = 0; i < N; i++) {
        docs[i] = bson_new();
        BSON_APPEND_INT32(docs[i], "index", i);
        BSON_APPEND_INT32(docs[i], "category", i % 10);
        BSON_APPEND_UTF8(docs[i], "data", "Lorem ipsum dolor sit amet");
    }
    
    rc = mongolite_insert_many(db, "large", (const bson_t**)docs, N, NULL, &error);
    assert_int_equal(0, rc);
    
    for (int i = 0; i < N; i++) {
        bson_destroy(docs[i]);
    }
    free(docs);
    
    int64_t count = mongolite_collection_count(db, "large", NULL, &error);
    assert_int_equal(N, count);
    
    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "category", 5);
    
    mongolite_cursor_t *cursor = mongolite_find(db, "large", filter, NULL, &error);
    assert_non_null(cursor);
    
    int category_count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        category_count++;
    }
    
    assert_int_equal(N/10, category_count);
    
    bson_destroy(filter);
    mongolite_cursor_destroy(cursor);
    
    cursor = mongolite_find(db, "large", NULL, NULL, &error);
    assert_non_null(cursor);
    mongolite_cursor_set_limit(cursor, 50);
    
    int limited_count = 0;
    while (mongolite_cursor_next(cursor, &doc)) {
        limited_count++;
    }
    assert_int_equal(50, limited_count);
    
    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

static void test_complex_queries(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "employees", NULL, &error);
    assert_int_equal(0, rc);
    
    const char *employees[] = {
        "{\"name\": \"Alice\", \"age\": 30, \"department\": \"Engineering\", \"salary\": 80000}",
        "{\"name\": \"Bob\", \"age\": 25, \"department\": \"Sales\", \"salary\": 60000}",
        "{\"name\": \"Charlie\", \"age\": 35, \"department\": \"Engineering\", \"salary\": 95000}",
        "{\"name\": \"Diana\", \"age\": 28, \"department\": \"Marketing\", \"salary\": 65000}",
        "{\"name\": \"Eve\", \"age\": 40, \"department\": \"Engineering\", \"salary\": 110000}",
        "{\"name\": \"Frank\", \"age\": 32, \"department\": \"Sales\", \"salary\": 70000}",
        "{\"name\": \"Grace\", \"age\": 27, \"department\": \"Engineering\", \"salary\": 75000}",
        "{\"name\": \"Henry\", \"age\": 45, \"department\": \"Management\", \"salary\": 120000}"
    };
    
    for (int i = 0; i < 8; i++) {
        rc = mongolite_insert_one_json(db, "employees", employees[i], NULL, &error);
        assert_int_equal(0, rc);
    }
    
    // Test $gt
    {
        bson_t *filter = bson_new();
        bson_t child;
        BSON_APPEND_DOCUMENT_BEGIN(filter, "salary", &child);
        BSON_APPEND_INT32(&child, "$gt", 75000);
        bson_append_document_end(filter, &child);
        
        mongolite_cursor_t *cursor = mongolite_find(db, "employees", filter, NULL, &error);
        assert_non_null(cursor);
        
        int count = 0;
        const bson_t *doc;
        while (mongolite_cursor_next(cursor, &doc)) {
            count++;
        }
        assert_int_equal(4, count);
        
        bson_destroy(filter);
        mongolite_cursor_destroy(cursor);
    }
    
    // Test $lt
    {
        bson_t *filter = bson_new();
        bson_t child;
        BSON_APPEND_DOCUMENT_BEGIN(filter, "age", &child);
        BSON_APPEND_INT32(&child, "$lt", 30);
        bson_append_document_end(filter, &child);
        
        mongolite_cursor_t *cursor = mongolite_find(db, "employees", filter, NULL, &error);
        assert_non_null(cursor);
        
        int count = 0;
        const bson_t *doc;
        while (mongolite_cursor_next(cursor, &doc)) {
            count++;
        }
        assert_int_equal(3, count);
        
        bson_destroy(filter);
        mongolite_cursor_destroy(cursor);
    }
    
    // Test range
    {
        bson_t *filter = bson_new();
        bson_t child;
        BSON_APPEND_DOCUMENT_BEGIN(filter, "age", &child);
        BSON_APPEND_INT32(&child, "$gte", 25);
        BSON_APPEND_INT32(&child, "$lte", 35);
        bson_append_document_end(filter, &child);
        
        mongolite_cursor_t *cursor = mongolite_find(db, "employees", filter, NULL, &error);
        assert_non_null(cursor);
        
        int count = 0;
        const bson_t *doc;
        while (mongolite_cursor_next(cursor, &doc)) {
            count++;
        }
        assert_int_equal(6, count);
        
        bson_destroy(filter);
        mongolite_cursor_destroy(cursor);
    }
    
    // Test $ne
    {
        bson_t *filter = bson_new();
        bson_t child;
        BSON_APPEND_DOCUMENT_BEGIN(filter, "department", &child);
        BSON_APPEND_UTF8(&child, "$ne", "Engineering");
        bson_append_document_end(filter, &child);
        
        mongolite_cursor_t *cursor = mongolite_find(db, "employees", filter, NULL, &error);
        assert_non_null(cursor);
        
        int count = 0;
        const bson_t *doc;
        while (mongolite_cursor_next(cursor, &doc)) {
            count++;
        }
        assert_int_equal(4, count);
        
        bson_destroy(filter);
        mongolite_cursor_destroy(cursor);
    }
    
    mongolite_close(db);
}

static void test_nested_documents(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "nested", NULL, &error);
    assert_int_equal(0, rc);
    
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "Test User");
    
    bson_t address;
    BSON_APPEND_DOCUMENT_BEGIN(doc, "address", &address);
    BSON_APPEND_UTF8(&address, "street", "123 Main St");
    BSON_APPEND_UTF8(&address, "city", "New York");
    BSON_APPEND_UTF8(&address, "zip", "10001");
    bson_append_document_end(doc, &address);
    
    bson_t tags;
    BSON_APPEND_ARRAY_BEGIN(doc, "tags", &tags);
    BSON_APPEND_UTF8(&tags, "0", "developer");
    BSON_APPEND_UTF8(&tags, "1", "admin");
    BSON_APPEND_UTF8(&tags, "2", "user");
    bson_append_array_end(doc, &tags);
    
    bson_oid_t id;
    rc = mongolite_insert_one(db, "nested", doc, &id, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc);
    
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);
    
    bson_t *found = mongolite_find_one(db, "nested", filter, NULL, &error);
    assert_non_null(found);
    
    bson_iter_t iter, child;
    assert_true(bson_iter_init_find(&iter, found, "address"));
    assert_true(BSON_ITER_HOLDS_DOCUMENT(&iter));
    
    bson_iter_recurse(&iter, &child);
    assert_true(bson_iter_find(&child, "city"));
    assert_string_equal("New York", bson_iter_utf8(&child, NULL));
    
    assert_true(bson_iter_init_find(&iter, found, "tags"));
    assert_true(BSON_ITER_HOLDS_ARRAY(&iter));
    
    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

static void test_empty_results(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "empty", NULL, &error);
    assert_int_equal(0, rc);
    
    bson_t *found = mongolite_find_one(db, "empty", NULL, NULL, &error);
    assert_null(found);
    
    mongolite_cursor_t *cursor = mongolite_find(db, "empty", NULL, NULL, &error);
    assert_non_null(cursor);
    
    int count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }
    assert_int_equal(0, count);
    
    mongolite_cursor_destroy(cursor);
    
    rc = mongolite_insert_one_json(db, "empty", "{\"x\": 1}", NULL, &error);
    assert_int_equal(0, rc);
    
    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "x", 999);
    
    found = mongolite_find_one(db, "empty", filter, NULL, &error);
    assert_null(found);
    
    cursor = mongolite_find(db, "empty", filter, NULL, &error);
    assert_non_null(cursor);
    
    count = 0;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }
    assert_int_equal(0, count);
    
    bson_destroy(filter);
    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

static void test_special_characters(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "unicode", NULL, &error);
    assert_int_equal(0, rc);
    
    const char *docs[] = {
        "{\"text\": \"Hello, World!\"}",
        "{\"text\": \"Caf\\u00e9\"}",
        "{\"text\": \"\\u4e2d\\u6587\"}",
        "{\"text\": \"Special: \\\"quotes\\\" and \\\\backslash\"}",
        "{\"text\": \"Newline\\nTab\\t\"}"
    };
    
    for (int i = 0; i < 5; i++) {
        rc = mongolite_insert_one_json(db, "unicode", docs[i], NULL, &error);
        assert_int_equal(0, rc);
    }
    
    int64_t count = mongolite_collection_count(db, "unicode", NULL, &error);
    assert_int_equal(5, count);
    
    mongolite_cursor_t *cursor = mongolite_find(db, "unicode", NULL, NULL, &error);
    assert_non_null(cursor);
    
    const bson_t *doc;
    int found_count = 0;
    while (mongolite_cursor_next(cursor, &doc)) {
        found_count++;
    }
    assert_int_equal(5, found_count);
    
    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

static void test_cursor_exhausted(void **state) {
    (void)state;
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "cursor_test", NULL, &error);
    assert_int_equal(0, rc);
    
    for (int i = 0; i < 5; i++) {
        bson_t *doc = bson_new();
        BSON_APPEND_INT32(doc, "i", i);
        rc = mongolite_insert_one(db, "cursor_test", doc, NULL, &error);
        assert_int_equal(0, rc);
        bson_destroy(doc);
    }
    
    mongolite_cursor_t *cursor = mongolite_find(db, "cursor_test", NULL, NULL, &error);
    assert_non_null(cursor);
    
    int count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }
    assert_int_equal(5, count);
    
    assert_false(mongolite_cursor_more(cursor));
    
    assert_false(mongolite_cursor_next(cursor, &doc));
    assert_null(doc);
    
    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_basic_full_cycle, setup, teardown),
        cmocka_unit_test_setup_teardown(test_multiple_collections, setup, teardown),
        cmocka_unit_test_setup_teardown(test_data_persistence, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_dataset, setup, teardown),
        cmocka_unit_test_setup_teardown(test_complex_queries, setup, teardown),
        cmocka_unit_test_setup_teardown(test_nested_documents, setup, teardown),
        cmocka_unit_test_setup_teardown(test_empty_results, setup, teardown),
        cmocka_unit_test_setup_teardown(test_special_characters, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cursor_exhausted, setup, teardown),
    };
    
    return cmocka_run_group_tests(tests, NULL, NULL);
}
