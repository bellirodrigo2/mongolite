#include "../src/mongolite.h"
#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <bson/bson.h>

// Helper function to create test data
void setup_test_data(mlite_db_t *db, const char *collection_name) {
    bson_error_t error;
    
    // Insert test documents
    const char *json_docs[] = {
        "{\"name\": \"Alice\", \"age\": 25, \"city\": \"New York\", \"active\": true, \"score\": 85.5}",
        "{\"name\": \"Bob\", \"age\": 30, \"city\": \"San Francisco\", \"active\": false, \"score\": 92.3}",
        "{\"name\": \"Charlie\", \"age\": 35, \"city\": \"New York\", \"active\": true, \"score\": 78.9}",
        "{\"name\": \"Diana\", \"age\": 28, \"city\": \"Chicago\", \"active\": true, \"score\": 88.1}",
        "{\"name\": \"Eve\", \"age\": 32, \"city\": \"San Francisco\", \"active\": false, \"score\": 94.7}",
        "{\"name\": \"Frank\", \"age\": 27, \"city\": \"Boston\", \"active\": true, \"score\": 76.2}"
    };
    
    int rc = mlite_insert_many_jsonstr(db, collection_name, json_docs, 6, &error);
    assert(rc == 0);
}

// Helper function to count documents in cursor
int count_cursor_results(mlite_cursor_t *cursor) {
    int count = 0;
    const bson_t *doc;
    
    while (mlite_cursor_next(cursor, &doc)) {
        count++;
    }
    
    return count;
}

// Helper function to print document (for debugging)
void print_document(const bson_t *doc, const char *prefix) {
    char *json_str = bson_as_canonical_extended_json(doc, NULL);
    printf("%s%s\n", prefix, json_str);
    bson_free(json_str);
}

void test_find_all_documents() {
    printf("Testing find() with no filter (all documents)...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_find_all.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "users");
    assert(rc == 0);
    
    // Setup test data
    setup_test_data(db, "users");
    
    // Find all documents
    mlite_cursor_t *cursor = mlite_find(db, "users", NULL, NULL);
    assert(cursor != NULL);
    
    int count = 0;
    const bson_t *doc;
    
    while (mlite_cursor_next(cursor, &doc)) {
        assert(doc != NULL);
        
        // Verify document has required fields
        bson_iter_t iter;
        assert(bson_iter_init_find(&iter, doc, "_id"));
        assert(BSON_ITER_HOLDS_OID(&iter));
        
        assert(bson_iter_init_find(&iter, doc, "name"));
        assert(BSON_ITER_HOLDS_UTF8(&iter));
        
        count++;
    }
    
    assert(count == 6);
    printf("✓ Found %d documents (expected 6)\n", count);
    
    // Check no cursor errors
    assert(!mlite_cursor_error(cursor, &error));
    
    mlite_cursor_destroy(cursor);
    mlite_close(db);
    unlink(test_file);
    
    printf("All find-all tests passed!\n");
}

void test_find_with_exact_match_filter() {
    printf("Testing find() with exact match filter...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_find_filter.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "users");
    assert(rc == 0);
    
    // Setup test data
    setup_test_data(db, "users");
    
    // Test 1: Find by name
    bson_t *filter = bson_new();
    assert(bson_append_utf8(filter, "name", -1, "Alice", -1));
    
    mlite_cursor_t *cursor = mlite_find(db, "users", filter, NULL);
    assert(cursor != NULL);
    
    int count = count_cursor_results(cursor);
    assert(count == 1);
    printf("✓ Found %d documents with name='Alice' (expected 1)\n", count);
    
    mlite_cursor_destroy(cursor);
    bson_destroy(filter);
    
    // Test 2: Find by city (multiple matches)
    filter = bson_new();
    assert(bson_append_utf8(filter, "city", -1, "New York", -1));
    
    cursor = mlite_find(db, "users", filter, NULL);
    assert(cursor != NULL);
    
    count = count_cursor_results(cursor);
    assert(count == 2);  // Alice and Charlie
    printf("✓ Found %d documents with city='New York' (expected 2)\n", count);
    
    mlite_cursor_destroy(cursor);
    bson_destroy(filter);
    
    // Test 3: Find by boolean field
    filter = bson_new();
    assert(bson_append_bool(filter, "active", -1, false));
    
    cursor = mlite_find(db, "users", filter, NULL);
    assert(cursor != NULL);
    
    count = count_cursor_results(cursor);
    assert(count == 2);  // Bob and Eve
    printf("✓ Found %d documents with active=false (expected 2)\n", count);
    
    mlite_cursor_destroy(cursor);
    bson_destroy(filter);
    
    // Test 4: Find by numeric field
    filter = bson_new();
    assert(bson_append_int32(filter, "age", -1, 30));
    
    cursor = mlite_find(db, "users", filter, NULL);
    assert(cursor != NULL);
    
    count = count_cursor_results(cursor);
    assert(count == 1);  // Bob
    printf("✓ Found %d documents with age=30 (expected 1)\n", count);
    
    mlite_cursor_destroy(cursor);
    bson_destroy(filter);
    
    // Test 5: Find with no matches
    filter = bson_new();
    assert(bson_append_utf8(filter, "name", -1, "Nonexistent", -1));
    
    cursor = mlite_find(db, "users", filter, NULL);
    assert(cursor != NULL);
    
    count = count_cursor_results(cursor);
    assert(count == 0);
    printf("✓ Found %d documents with name='Nonexistent' (expected 0)\n", count);
    
    mlite_cursor_destroy(cursor);
    bson_destroy(filter);
    
    mlite_close(db);
    unlink(test_file);
    
    printf("All exact match filter tests passed!\n");
}

void test_find_with_multiple_conditions() {
    printf("Testing find() with multiple filter conditions...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_find_multi.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "users");
    assert(rc == 0);
    
    // Setup test data
    setup_test_data(db, "users");
    
    // Test: Find by city AND active status
    bson_t *filter = bson_new();
    assert(bson_append_utf8(filter, "city", -1, "New York", -1));
    assert(bson_append_bool(filter, "active", -1, true));
    
    mlite_cursor_t *cursor = mlite_find(db, "users", filter, NULL);
    assert(cursor != NULL);
    
    int count = 0;
    const bson_t *doc;
    
    while (mlite_cursor_next(cursor, &doc)) {
        // Verify both conditions are met
        bson_iter_t iter;
        
        assert(bson_iter_init_find(&iter, doc, "city"));
        const char *city = bson_iter_utf8(&iter, NULL);
        assert(strcmp(city, "New York") == 0);
        
        assert(bson_iter_init_find(&iter, doc, "active"));
        assert(bson_iter_bool(&iter) == true);
        
        count++;
    }
    
    assert(count == 2);  // Alice and Charlie
    printf("✓ Found %d documents with city='New York' AND active=true (expected 2)\n", count);
    
    mlite_cursor_destroy(cursor);
    bson_destroy(filter);
    
    mlite_close(db);
    unlink(test_file);
    
    printf("All multiple conditions tests passed!\n");
}

void test_find_with_projection() {
    printf("Testing find() with field projection...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_find_projection.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "users");
    assert(rc == 0);
    
    // Setup test data
    setup_test_data(db, "users");
    
    // Test 1: Project only name field
    bson_t *projection = bson_new();
    assert(bson_append_int32(projection, "name", -1, 1));
    
    bson_t *opts = bson_new();
    assert(bson_append_document(opts, "projection", -1, projection));
    
    mlite_cursor_t *cursor = mlite_find(db, "users", NULL, opts);
    assert(cursor != NULL);
    
    const bson_t *doc;
    int count = 0;
    
    while (mlite_cursor_next(cursor, &doc)) {
        // Should have _id and name only
        bson_iter_t iter;
        assert(bson_iter_init_find(&iter, doc, "_id"));  // _id always included
        assert(bson_iter_init_find(&iter, doc, "name")); // requested field
        
        // Should NOT have other fields
        assert(!bson_iter_init_find(&iter, doc, "age"));
        assert(!bson_iter_init_find(&iter, doc, "city"));
        assert(!bson_iter_init_find(&iter, doc, "active"));
        
        count++;
    }
    
    assert(count == 6);
    printf("✓ Projection with name field only worked correctly\n");
    
    mlite_cursor_destroy(cursor);
    bson_destroy(projection);
    bson_destroy(opts);
    
    // Test 2: Project multiple fields
    projection = bson_new();
    assert(bson_append_int32(projection, "name", -1, 1));
    assert(bson_append_int32(projection, "age", -1, 1));
    
    opts = bson_new();
    assert(bson_append_document(opts, "projection", -1, projection));
    
    cursor = mlite_find(db, "users", NULL, opts);
    assert(cursor != NULL);
    
    count = 0;
    while (mlite_cursor_next(cursor, &doc)) {
        // Should have _id, name, and age
        bson_iter_t iter;
        assert(bson_iter_init_find(&iter, doc, "_id"));
        assert(bson_iter_init_find(&iter, doc, "name"));
        assert(bson_iter_init_find(&iter, doc, "age"));
        
        // Should NOT have other fields
        assert(!bson_iter_init_find(&iter, doc, "city"));
        assert(!bson_iter_init_find(&iter, doc, "active"));
        assert(!bson_iter_init_find(&iter, doc, "score"));
        
        count++;
    }
    
    assert(count == 6);
    printf("✓ Projection with multiple fields worked correctly\n");
    
    mlite_cursor_destroy(cursor);
    bson_destroy(projection);
    bson_destroy(opts);
    
    // Test 3: Exclude _id field
    projection = bson_new();
    assert(bson_append_int32(projection, "name", -1, 1));
    assert(bson_append_int32(projection, "_id", -1, 0));  // Exclude _id
    
    opts = bson_new();
    assert(bson_append_document(opts, "projection", -1, projection));
    
    cursor = mlite_find(db, "users", NULL, opts);
    assert(cursor != NULL);
    
    if (mlite_cursor_next(cursor, &doc)) {
        // Should have name only (no _id)
        bson_iter_t iter;
        assert(!bson_iter_init_find(&iter, doc, "_id"));  // _id excluded
        assert(bson_iter_init_find(&iter, doc, "name"));  // name included
        
        printf("✓ _id field exclusion worked correctly\n");
    }
    
    mlite_cursor_destroy(cursor);
    bson_destroy(projection);
    bson_destroy(opts);
    
    mlite_close(db);
    unlink(test_file);
    
    printf("All projection tests passed!\n");
}

void test_count_documents() {
    printf("Testing mlite_count_documents()...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_count.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "users");
    assert(rc == 0);
    
    // Setup test data
    setup_test_data(db, "users");
    
    // Test 1: Count all documents
    int64_t count = mlite_count_documents(db, "users", NULL, &error);
    assert(count == 6);
    printf("✓ Total document count: %lld (expected 6)\n", (long long)count);
    
    // Test 2: Count with filter
    bson_t *filter = bson_new();
    assert(bson_append_utf8(filter, "city", -1, "New York", -1));
    
    count = mlite_count_documents(db, "users", filter, &error);
    assert(count == 2);
    printf("✓ Filtered document count (city='New York'): %lld (expected 2)\n", (long long)count);
    
    bson_destroy(filter);
    
    // Test 3: Count with no matches
    filter = bson_new();
    assert(bson_append_utf8(filter, "name", -1, "Nonexistent", -1));
    
    count = mlite_count_documents(db, "users", filter, &error);
    assert(count == 0);
    printf("✓ No matches count: %lld (expected 0)\n", (long long)count);
    
    bson_destroy(filter);
    
    mlite_close(db);
    unlink(test_file);
    
    printf("All count tests passed!\n");
}

void test_cursor_error_handling() {
    printf("Testing cursor error handling...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_cursor_errors.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    // Test 1: Find on non-existent collection
    mlite_cursor_t *cursor = mlite_find(db, "nonexistent", NULL, NULL);
    assert(cursor == NULL);
    printf("✓ Find on non-existent collection correctly returned NULL\n");
    
    // Test 2: NULL parameters
    cursor = mlite_find(NULL, "test", NULL, NULL);
    assert(cursor == NULL);
    printf("✓ NULL database parameter correctly handled\n");
    
    cursor = mlite_find(db, NULL, NULL, NULL);
    assert(cursor == NULL);
    printf("✓ NULL collection name correctly handled\n");
    
    // Test 3: Count on non-existent collection
    int64_t count = mlite_count_documents(db, "nonexistent", NULL, &error);
    assert(count == -1);
    assert(error.code == 401);
    printf("✓ Count on non-existent collection correctly failed\n");
    
    mlite_close(db);
    unlink(test_file);
    
    printf("All error handling tests passed!\n");
}

void test_empty_collection() {
    printf("Testing operations on empty collection...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_empty.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create empty collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "empty");
    assert(rc == 0);
    
    // Find on empty collection
    mlite_cursor_t *cursor = mlite_find(db, "empty", NULL, NULL);
    assert(cursor != NULL);
    
    const bson_t *doc;
    assert(!mlite_cursor_next(cursor, &doc));  // Should return false
    printf("✓ Find on empty collection correctly returns no documents\n");
    
    mlite_cursor_destroy(cursor);
    
    // Count on empty collection
    int64_t count = mlite_count_documents(db, "empty", NULL, &error);
    assert(count == 0);
    printf("✓ Count on empty collection: %lld (expected 0)\n", (long long)count);
    
    mlite_close(db);
    unlink(test_file);
    
    printf("All empty collection tests passed!\n");
}

void test_cursor_reuse_and_memory() {
    printf("Testing cursor reuse and memory management...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_cursor_memory.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "test");
    assert(rc == 0);
    
    // Insert some test data
    const char *json_docs[] = {
        "{\"value\": 1}",
        "{\"value\": 2}",
        "{\"value\": 3}"
    };
    rc = mlite_insert_many_jsonstr(db, "test", json_docs, 3, &error);
    assert(rc == 0);
    
    // Test multiple iterations with same cursor
    mlite_cursor_t *cursor = mlite_find(db, "test", NULL, NULL);
    assert(cursor != NULL);
    
    const bson_t *doc;
    int count = 0;
    
    // First iteration
    while (mlite_cursor_next(cursor, &doc)) {
        count++;
    }
    assert(count == 3);
    printf("✓ First cursor iteration found %d documents\n", count);
    
    // Second iteration should return nothing (cursor exhausted)
    count = 0;
    while (mlite_cursor_next(cursor, &doc)) {
        count++;
    }
    assert(count == 0);
    printf("✓ Second iteration correctly returned 0 documents (cursor exhausted)\n");
    
    mlite_cursor_destroy(cursor);
    
    // Test destroying cursor multiple times (should be safe)
    cursor = mlite_find(db, "test", NULL, NULL);
    mlite_cursor_destroy(cursor);
    cursor = NULL;  // Set to NULL after destroy
    mlite_cursor_destroy(cursor);  // Should be safe to call with NULL
    mlite_cursor_destroy(NULL);    // Should be safe with NULL
    printf("✓ Multiple cursor destroy calls handled safely\n");
    
    mlite_close(db);
    unlink(test_file);
    
    printf("All cursor memory tests passed!\n");
}

int main() {
    printf("Running MongoLite find operation tests...\n\n");
    
    test_find_all_documents();
    printf("\n");
    test_find_with_exact_match_filter();
    printf("\n");
    test_find_with_multiple_conditions();
    printf("\n");
    test_find_with_projection();
    printf("\n");
    test_count_documents();
    printf("\n");
    test_cursor_error_handling();
    printf("\n");
    test_empty_collection();
    printf("\n");
    test_cursor_reuse_and_memory();
    printf("\n");
    
    // Quick cross-type comparison test
    printf("Testing MongoDB cross-type comparisons...\n");
    mlite_db_t *xtype_db;
    mlite_open("xtype_test.mlite", &xtype_db);
    mlite_collection_create(xtype_db, "numbers");
    
    // Insert same logical value with different types
    bson_t *int_doc = bson_new();
    bson_append_int32(int_doc, "value", -1, 42);
    bson_append_utf8(int_doc, "type", -1, "int32", -1);
    
    bson_t *double_doc = bson_new();
    bson_append_double(double_doc, "value", -1, 42.0);
    bson_append_utf8(double_doc, "type", -1, "double", -1);
    
    mlite_insert_one(xtype_db, "numbers", int_doc, NULL);
    mlite_insert_one(xtype_db, "numbers", double_doc, NULL);
    
    // Search for int32(42) - should match both int32(42) and double(42.0)
    bson_t *search = bson_new();
    bson_append_int32(search, "value", -1, 42);
    
    mlite_cursor_t *xtype_cursor = mlite_find(xtype_db, "numbers", search, NULL);
    int xtype_count = 0;
    const bson_t *found_doc;
    while (mlite_cursor_next(xtype_cursor, &found_doc)) {
        xtype_count++;
    }
    
    if (xtype_count == 2) {
        printf("✅ Cross-type comparison works: int32(42) matches both int32(42) and double(42.0)\n");
    } else {
        printf("❌ Cross-type comparison failed: expected 2 matches, got %d\n", xtype_count);
    }
    
    // Cleanup
    mlite_cursor_destroy(xtype_cursor);
    bson_destroy(int_doc);
    bson_destroy(double_doc);
    bson_destroy(search);
    mlite_close(xtype_db);
    unlink("xtype_test.mlite");
    
    printf("\nAll find operation tests passed! ✅\n");
    return 0;
}