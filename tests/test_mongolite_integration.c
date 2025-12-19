/*
 * test_mongolite_integration.c - Full-cycle integration tests
 *
 * Tests complete workflows:
 * - Create -> Insert -> Find -> Verify
 * - Multiple collections
 * - Large datasets
 * - Concurrent operations
 * - Data persistence (close/reopen)
 * - Edge cases
 */

#include "test_runner.h"
#include "mongolite_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

/* Test database path */
#define TEST_DB_PATH "./test_mongolite_integration"

/* ============================================================
 * Helper: Remove test database directory
 * ============================================================ */

static void cleanup_test_db(void) {
#ifdef _WIN32
    system("rmdir /s /q " TEST_DB_PATH " 2>nul");
#else
    system("rm -rf " TEST_DB_PATH);
#endif
}

/* ============================================================
 * Test: Basic full cycle (create, insert, find)
 * ============================================================ */

static int test_basic_full_cycle(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    /* 1. Open database */
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;  /* 32MB */
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    TEST_ASSERT(rc == 0, "open should succeed: %s", error.message);

    /* 2. Create collection */
    rc = mongolite_collection_create(db, "products", NULL, &error);
    TEST_ASSERT(rc == 0, "create collection should succeed");

    /* 3. Insert documents */
    bson_oid_t ids[3];

    bson_t *doc1 = bson_new();
    BSON_APPEND_UTF8(doc1, "name", "Laptop");
    BSON_APPEND_DOUBLE(doc1, "price", 999.99);
    BSON_APPEND_INT32(doc1, "stock", 50);
    rc = mongolite_insert_one(db, "products", doc1, &ids[0], &error);
    TEST_ASSERT(rc == 0, "insert 1 should succeed");
    bson_destroy(doc1);

    bson_t *doc2 = bson_new();
    BSON_APPEND_UTF8(doc2, "name", "Mouse");
    BSON_APPEND_DOUBLE(doc2, "price", 29.99);
    BSON_APPEND_INT32(doc2, "stock", 200);
    rc = mongolite_insert_one(db, "products", doc2, &ids[1], &error);
    TEST_ASSERT(rc == 0, "insert 2 should succeed");
    bson_destroy(doc2);

    bson_t *doc3 = bson_new();
    BSON_APPEND_UTF8(doc3, "name", "Keyboard");
    BSON_APPEND_DOUBLE(doc3, "price", 79.99);
    BSON_APPEND_INT32(doc3, "stock", 100);
    rc = mongolite_insert_one(db, "products", doc3, &ids[2], &error);
    TEST_ASSERT(rc == 0, "insert 3 should succeed");
    bson_destroy(doc3);

    /* 4. Verify count */
    int64_t count = mongolite_collection_count(db, "products", NULL, &error);
    TEST_ASSERT(count == 3, "count should be 3, got %lld", (long long)count);

    /* 5. Find each by _id */
    for (int i = 0; i < 3; i++) {
        bson_t *filter = bson_new();
        BSON_APPEND_OID(filter, "_id", &ids[i]);

        bson_t *found = mongolite_find_one(db, "products", filter, NULL, &error);
        TEST_ASSERT(found != NULL, "should find document %d", i);

        bson_iter_t iter;
        TEST_ASSERT(bson_iter_init_find(&iter, found, "name"), "should have name");
        printf("  Found: %s\n", bson_iter_utf8(&iter, NULL));

        bson_destroy(filter);
        bson_destroy(found);
    }

    /* 6. Find all via cursor */
    mongolite_cursor_t *cursor = mongolite_find(db, "products", NULL, NULL, &error);
    TEST_ASSERT(cursor != NULL, "find should return cursor");

    int found_count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        found_count++;
    }
    TEST_ASSERT(found_count == 3, "cursor should iterate 3 docs, got %d", found_count);

    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Multiple collections
 * ============================================================ */

static int test_multiple_collections(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;  /* 32MB */
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    /* Create multiple collections */
    rc = mongolite_collection_create(db, "users", NULL, &error);
    TEST_ASSERT(rc == 0, "create users should succeed");

    rc = mongolite_collection_create(db, "orders", NULL, &error);
    TEST_ASSERT(rc == 0, "create orders should succeed");

    rc = mongolite_collection_create(db, "products", NULL, &error);
    TEST_ASSERT(rc == 0, "create products should succeed");

    /* Insert into users */
    bson_oid_t user_id;
    bson_t *user = bson_new();
    BSON_APPEND_UTF8(user, "name", "John Doe");
    BSON_APPEND_UTF8(user, "email", "john@example.com");
    rc = mongolite_insert_one(db, "users", user, &user_id, &error);
    TEST_ASSERT(rc == 0, "insert user should succeed");
    bson_destroy(user);

    /* Insert into products */
    bson_oid_t product_id;
    bson_t *product = bson_new();
    BSON_APPEND_UTF8(product, "name", "Widget");
    BSON_APPEND_DOUBLE(product, "price", 19.99);
    rc = mongolite_insert_one(db, "products", product, &product_id, &error);
    TEST_ASSERT(rc == 0, "insert product should succeed");
    bson_destroy(product);

    /* Insert into orders (referencing user and product) */
    bson_t *order = bson_new();
    BSON_APPEND_OID(order, "user_id", &user_id);
    BSON_APPEND_OID(order, "product_id", &product_id);
    BSON_APPEND_INT32(order, "quantity", 2);
    BSON_APPEND_DOUBLE(order, "total", 39.98);
    rc = mongolite_insert_one(db, "orders", order, NULL, &error);
    TEST_ASSERT(rc == 0, "insert order should succeed");
    bson_destroy(order);

    /* Verify counts */
    TEST_ASSERT(mongolite_collection_count(db, "users", NULL, &error) == 1, "users count");
    TEST_ASSERT(mongolite_collection_count(db, "products", NULL, &error) == 1, "products count");
    TEST_ASSERT(mongolite_collection_count(db, "orders", NULL, &error) == 1, "orders count");

    /* Find order by user_id */
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "user_id", &user_id);

    bson_t *found_order = mongolite_find_one(db, "orders", filter, NULL, &error);
    TEST_ASSERT(found_order != NULL, "should find order by user_id");

    bson_iter_t iter;
    TEST_ASSERT(bson_iter_init_find(&iter, found_order, "quantity"), "should have quantity");
    TEST_ASSERT(bson_iter_int32(&iter) == 2, "quantity should be 2");

    printf("  Multi-collection relationships verified!\n");

    bson_destroy(filter);
    bson_destroy(found_order);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Data persistence (close/reopen)
 * ============================================================ */

static int test_data_persistence(void) {
    cleanup_test_db();

    bson_oid_t saved_id;
    gerror_t error = {0};

    /* Phase 1: Create and insert */
    {
        mongolite_db_t *db = NULL;
        
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;  /* 32MB */
        int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
        TEST_ASSERT(rc == 0, "open should succeed");

        rc = mongolite_collection_create(db, "persistent", NULL, &error);
        TEST_ASSERT(rc == 0, "create should succeed");

        bson_t *doc = bson_new();
        BSON_APPEND_UTF8(doc, "message", "Hello, persistence!");
        BSON_APPEND_INT32(doc, "magic", 42);

        rc = mongolite_insert_one(db, "persistent", doc, &saved_id, &error);
        TEST_ASSERT(rc == 0, "insert should succeed");

        bson_destroy(doc);
        mongolite_close(db);
        printf("  Phase 1: Data written and database closed\n");
    }

    /* Phase 2: Reopen and verify */
    {
        mongolite_db_t *db = NULL;
        
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;  /* 32MB */
        int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
        TEST_ASSERT(rc == 0, "reopen should succeed");

        /* Verify collection exists */
        TEST_ASSERT(mongolite_collection_exists(db, "persistent", &error),
                    "collection should still exist");

        /* Verify count */
        int64_t count = mongolite_collection_count(db, "persistent", NULL, &error);
        TEST_ASSERT(count == 1, "count should be 1 after reopen");

        /* Find the document */
        bson_t *filter = bson_new();
        BSON_APPEND_OID(filter, "_id", &saved_id);

        bson_t *found = mongolite_find_one(db, "persistent", filter, NULL, &error);
        TEST_ASSERT(found != NULL, "should find document after reopen");

        bson_iter_t iter;
        TEST_ASSERT(bson_iter_init_find(&iter, found, "message"), "should have message");
        TEST_ASSERT(strcmp(bson_iter_utf8(&iter, NULL), "Hello, persistence!") == 0,
                    "message should match");

        TEST_ASSERT(bson_iter_init_find(&iter, found, "magic"), "should have magic");
        TEST_ASSERT(bson_iter_int32(&iter) == 42, "magic should be 42");

        printf("  Phase 2: Data verified after reopen!\n");

        bson_destroy(filter);
        bson_destroy(found);
        mongolite_close(db);
    }

    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Large dataset
 * ============================================================ */

static int test_large_dataset(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;  /* 32MB */
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    rc = mongolite_collection_create(db, "large", NULL, &error);
    TEST_ASSERT(rc == 0, "create should succeed");

    const int N = 1000;
    printf("  Inserting %d documents...\n", N);

    /* Batch insert */
    bson_t **docs = calloc(N, sizeof(bson_t*));
    for (int i = 0; i < N; i++) {
        docs[i] = bson_new();
        BSON_APPEND_INT32(docs[i], "index", i);
        BSON_APPEND_INT32(docs[i], "category", i % 10);  /* 10 categories */
        BSON_APPEND_UTF8(docs[i], "data", "Lorem ipsum dolor sit amet");
    }

    rc = mongolite_insert_many(db, "large", (const bson_t**)docs, N, NULL, &error);
    TEST_ASSERT(rc == 0, "insert_many should succeed: %s", error.message);

    for (int i = 0; i < N; i++) {
        bson_destroy(docs[i]);
    }
    free(docs);

    /* Verify count */
    int64_t count = mongolite_collection_count(db, "large", NULL, &error);
    TEST_ASSERT(count == N, "count should be %d, got %lld", N, (long long)count);
    printf("  Count verified: %lld\n", (long long)count);

    /* Find by category (should return ~100 each) */
    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "category", 5);

    mongolite_cursor_t *cursor = mongolite_find(db, "large", filter, NULL, &error);
    TEST_ASSERT(cursor != NULL, "find should return cursor");

    int category_count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        category_count++;
    }

    TEST_ASSERT(category_count == N/10, "category 5 should have %d docs, got %d",
                N/10, category_count);
    printf("  Category filter verified: %d docs\n", category_count);

    bson_destroy(filter);
    mongolite_cursor_destroy(cursor);

    /* Test with limit */
    cursor = mongolite_find(db, "large", NULL, NULL, &error);
    TEST_ASSERT(cursor != NULL, "find should return cursor");
    mongolite_cursor_set_limit(cursor, 50);

    int limited_count = 0;
    while (mongolite_cursor_next(cursor, &doc)) {
        limited_count++;
    }
    TEST_ASSERT(limited_count == 50, "limit should return 50, got %d", limited_count);
    printf("  Limit verified: %d docs\n", limited_count);

    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Complex queries with bsonmatch operators
 * ============================================================ */

static int test_complex_queries(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;  /* 32MB */
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    rc = mongolite_collection_create(db, "employees", NULL, &error);
    TEST_ASSERT(rc == 0, "create should succeed");

    /* Insert test data */
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
        TEST_ASSERT(rc == 0, "insert %d should succeed", i);
    }

    /* Test $gt: salary > 75000 */
    {
        bson_t *filter = bson_new();
        bson_t child;
        BSON_APPEND_DOCUMENT_BEGIN(filter, "salary", &child);
        BSON_APPEND_INT32(&child, "$gt", 75000);
        bson_append_document_end(filter, &child);

        mongolite_cursor_t *cursor = mongolite_find(db, "employees", filter, NULL, &error);
        TEST_ASSERT(cursor != NULL, "find should work");

        int count = 0;
        const bson_t *doc;
        printf("  Salary > 75000: ");
        while (mongolite_cursor_next(cursor, &doc)) {
            count++;
            bson_iter_t iter;
            if (bson_iter_init_find(&iter, doc, "name")) {
                printf("%s ", bson_iter_utf8(&iter, NULL));
            }
        }
        printf("(%d)\n", count);
        TEST_ASSERT(count == 4, "should find 4 with salary > 75000");

        bson_destroy(filter);
        mongolite_cursor_destroy(cursor);
    }

    /* Test $lt: age < 30 */
    {
        bson_t *filter = bson_new();
        bson_t child;
        BSON_APPEND_DOCUMENT_BEGIN(filter, "age", &child);
        BSON_APPEND_INT32(&child, "$lt", 30);
        bson_append_document_end(filter, &child);

        mongolite_cursor_t *cursor = mongolite_find(db, "employees", filter, NULL, &error);
        TEST_ASSERT(cursor != NULL, "find should work");

        int count = 0;
        const bson_t *doc;
        printf("  Age < 30: ");
        while (mongolite_cursor_next(cursor, &doc)) {
            count++;
            bson_iter_t iter;
            if (bson_iter_init_find(&iter, doc, "name")) {
                printf("%s ", bson_iter_utf8(&iter, NULL));
            }
        }
        printf("(%d)\n", count);
        TEST_ASSERT(count == 3, "should find 3 with age < 30");

        bson_destroy(filter);
        mongolite_cursor_destroy(cursor);
    }

    /* Test $gte and $lte combined: 25 <= age <= 35 */
    {
        bson_t *filter = bson_new();
        bson_t child;
        BSON_APPEND_DOCUMENT_BEGIN(filter, "age", &child);
        BSON_APPEND_INT32(&child, "$gte", 25);
        BSON_APPEND_INT32(&child, "$lte", 35);
        bson_append_document_end(filter, &child);

        mongolite_cursor_t *cursor = mongolite_find(db, "employees", filter, NULL, &error);
        TEST_ASSERT(cursor != NULL, "find should work");

        int count = 0;
        const bson_t *doc;
        printf("  25 <= Age <= 35: ");
        while (mongolite_cursor_next(cursor, &doc)) {
            count++;
            bson_iter_t iter;
            if (bson_iter_init_find(&iter, doc, "name")) {
                printf("%s ", bson_iter_utf8(&iter, NULL));
            }
        }
        printf("(%d)\n", count);
        TEST_ASSERT(count == 6, "should find 6 with 25 <= age <= 35");

        bson_destroy(filter);
        mongolite_cursor_destroy(cursor);
    }

    /* Test $ne: department != Engineering */
    {
        bson_t *filter = bson_new();
        bson_t child;
        BSON_APPEND_DOCUMENT_BEGIN(filter, "department", &child);
        BSON_APPEND_UTF8(&child, "$ne", "Engineering");
        bson_append_document_end(filter, &child);

        mongolite_cursor_t *cursor = mongolite_find(db, "employees", filter, NULL, &error);
        TEST_ASSERT(cursor != NULL, "find should work");

        int count = 0;
        const bson_t *doc;
        printf("  Dept != Engineering: ");
        while (mongolite_cursor_next(cursor, &doc)) {
            count++;
            bson_iter_t iter;
            if (bson_iter_init_find(&iter, doc, "name")) {
                printf("%s ", bson_iter_utf8(&iter, NULL));
            }
        }
        printf("(%d)\n", count);
        TEST_ASSERT(count == 4, "should find 4 not in Engineering");

        bson_destroy(filter);
        mongolite_cursor_destroy(cursor);
    }

    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Nested documents
 * ============================================================ */

static int test_nested_documents(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;  /* 32MB */
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    rc = mongolite_collection_create(db, "nested", NULL, &error);
    TEST_ASSERT(rc == 0, "create should succeed");

    /* Insert document with nested structure */
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "name", "Test User");

    /* Nested address */
    bson_t address;
    BSON_APPEND_DOCUMENT_BEGIN(doc, "address", &address);
    BSON_APPEND_UTF8(&address, "street", "123 Main St");
    BSON_APPEND_UTF8(&address, "city", "New York");
    BSON_APPEND_UTF8(&address, "zip", "10001");
    bson_append_document_end(doc, &address);

    /* Nested array of tags */
    bson_t tags;
    BSON_APPEND_ARRAY_BEGIN(doc, "tags", &tags);
    BSON_APPEND_UTF8(&tags, "0", "developer");
    BSON_APPEND_UTF8(&tags, "1", "admin");
    BSON_APPEND_UTF8(&tags, "2", "user");
    bson_append_array_end(doc, &tags);

    bson_oid_t id;
    rc = mongolite_insert_one(db, "nested", doc, &id, &error);
    TEST_ASSERT(rc == 0, "insert should succeed");
    bson_destroy(doc);

    /* Find and verify */
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *found = mongolite_find_one(db, "nested", filter, NULL, &error);
    TEST_ASSERT(found != NULL, "should find document");

    /* Verify nested address */
    bson_iter_t iter, child;
    TEST_ASSERT(bson_iter_init_find(&iter, found, "address"), "should have address");
    TEST_ASSERT(BSON_ITER_HOLDS_DOCUMENT(&iter), "address should be document");

    bson_iter_recurse(&iter, &child);
    TEST_ASSERT(bson_iter_find(&child, "city"), "should have city");
    TEST_ASSERT(strcmp(bson_iter_utf8(&child, NULL), "New York") == 0, "city should match");

    /* Verify array */
    TEST_ASSERT(bson_iter_init_find(&iter, found, "tags"), "should have tags");
    TEST_ASSERT(BSON_ITER_HOLDS_ARRAY(&iter), "tags should be array");

    printf("  Nested document structure verified!\n");

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Empty results
 * ============================================================ */

static int test_empty_results(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;  /* 32MB */
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    rc = mongolite_collection_create(db, "empty", NULL, &error);
    TEST_ASSERT(rc == 0, "create should succeed");

    /* Empty collection - find_one should return NULL */
    bson_t *found = mongolite_find_one(db, "empty", NULL, NULL, &error);
    TEST_ASSERT(found == NULL, "find_one on empty should return NULL");

    /* Empty collection - cursor should be empty */
    mongolite_cursor_t *cursor = mongolite_find(db, "empty", NULL, NULL, &error);
    TEST_ASSERT(cursor != NULL, "find should return cursor even for empty");

    int count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }
    TEST_ASSERT(count == 0, "cursor should be empty");

    mongolite_cursor_destroy(cursor);

    /* Insert one, then search for non-existent */
    rc = mongolite_insert_one_json(db, "empty", "{\"x\": 1}", NULL, &error);
    TEST_ASSERT(rc == 0, "insert should succeed");

    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "x", 999);

    found = mongolite_find_one(db, "empty", filter, NULL, &error);
    TEST_ASSERT(found == NULL, "find_one for non-match should return NULL");

    cursor = mongolite_find(db, "empty", filter, NULL, &error);
    TEST_ASSERT(cursor != NULL, "find should return cursor");

    count = 0;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }
    TEST_ASSERT(count == 0, "cursor for non-match should be empty");

    printf("  Empty results handled correctly!\n");

    bson_destroy(filter);
    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Special characters and unicode
 * ============================================================ */

static int test_special_characters(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;  /* 32MB */
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    rc = mongolite_collection_create(db, "unicode", NULL, &error);
    TEST_ASSERT(rc == 0, "create should succeed");

    /* Insert documents with special characters */
    const char *docs[] = {
        "{\"text\": \"Hello, World!\"}",
        "{\"text\": \"Caf\\u00e9\"}",                    /* Café */
        "{\"text\": \"\\u4e2d\\u6587\"}",              /* Chinese */
        "{\"text\": \"Special: \\\"quotes\\\" and \\\\backslash\"}",
        "{\"text\": \"Newline\\nTab\\t\"}"
    };

    for (int i = 0; i < 5; i++) {
        rc = mongolite_insert_one_json(db, "unicode", docs[i], NULL, &error);
        TEST_ASSERT(rc == 0, "insert %d should succeed: %s", i, error.message);
    }

    /* Find and verify count */
    int64_t count = mongolite_collection_count(db, "unicode", NULL, &error);
    TEST_ASSERT(count == 5, "count should be 5");

    /* Iterate and print */
    mongolite_cursor_t *cursor = mongolite_find(db, "unicode", NULL, NULL, &error);
    TEST_ASSERT(cursor != NULL, "find should work");

    printf("  Unicode documents:\n");
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        bson_iter_t iter;
        if (bson_iter_init_find(&iter, doc, "text")) {
            printf("    - %s\n", bson_iter_utf8(&iter, NULL));
        }
    }

    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Test: Cursor reuse prevention
 * ============================================================ */

static int test_cursor_exhausted(void) {
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;  /* 32MB */
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    TEST_ASSERT(rc == 0, "open should succeed");

    rc = mongolite_collection_create(db, "cursor_test", NULL, &error);
    TEST_ASSERT(rc == 0, "create should succeed");

    /* Insert some docs */
    for (int i = 0; i < 5; i++) {
        bson_t *doc = bson_new();
        BSON_APPEND_INT32(doc, "i", i);
        rc = mongolite_insert_one(db, "cursor_test", doc, NULL, &error);
        TEST_ASSERT(rc == 0, "insert should succeed");
        bson_destroy(doc);
    }

    /* Get cursor and exhaust it */
    mongolite_cursor_t *cursor = mongolite_find(db, "cursor_test", NULL, NULL, &error);
    TEST_ASSERT(cursor != NULL, "find should work");

    int count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }
    TEST_ASSERT(count == 5, "should find 5 docs");

    /* Cursor should be exhausted - cursor_more should return false */
    TEST_ASSERT(!mongolite_cursor_more(cursor), "cursor should be exhausted");

    /* Additional next calls should return false */
    TEST_ASSERT(!mongolite_cursor_next(cursor, &doc), "exhausted cursor should return false");
    TEST_ASSERT(doc == NULL, "doc should be NULL after exhaustion");

    printf("  Cursor exhaustion handled correctly!\n");

    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    printf("=== Mongolite Integration Tests ===\n\n");

    RUN_TEST(test_basic_full_cycle);
    RUN_TEST(test_multiple_collections);
    RUN_TEST(test_data_persistence);
    RUN_TEST(test_large_dataset);
    RUN_TEST(test_complex_queries);
    RUN_TEST(test_nested_documents);
    RUN_TEST(test_empty_results);
    RUN_TEST(test_special_characters);
    RUN_TEST(test_cursor_exhausted);

    printf("\n=== All integration tests passed! ===\n");
    return 0;
}
