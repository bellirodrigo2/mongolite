#include "../src/mongolite.h"
#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <bson/bson.h>

void test_insert_one_valid_with_id() {
    printf("Testing insert_one with valid BSON document containing _id...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_insert_with_id.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "users");
    assert(rc == 0);
    
    // Create BSON document with _id
    bson_t *doc = bson_new();
    bson_oid_t oid;
    bson_oid_init(&oid, NULL);
    
    assert(bson_append_oid(doc, "_id", -1, &oid));
    assert(bson_append_utf8(doc, "name", -1, "John Doe", -1));
    assert(bson_append_int32(doc, "age", -1, 30));
    
    // Insert document
    rc = mlite_insert_one(db, "users", doc, &error);
    assert(rc == 0);
    printf("✓ Document with _id inserted successfully\n");
    
    // Try to insert same document again (should fail due to duplicate _id)
    rc = mlite_insert_one(db, "users", doc, &error);
    assert(rc != 0);
    assert(error.code == 12);  // Duplicate _id error code
    printf("✓ Duplicate _id correctly rejected\n");
    
    // Clean up
    bson_destroy(doc);
    mlite_close(db);
    unlink(test_file);
    
    printf("All insert_one with _id tests passed!\n");
}

void test_insert_one_valid_without_id() {
    printf("Testing insert_one with valid BSON document without _id...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_insert_without_id.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "products");
    assert(rc == 0);
    
    // Create BSON document without _id
    bson_t *doc = bson_new();
    assert(bson_append_utf8(doc, "name", -1, "Laptop", -1));
    assert(bson_append_double(doc, "price", -1, 999.99));
    assert(bson_append_bool(doc, "available", -1, true));
    
    // Verify document doesn't have _id initially
    assert(!bson_has_field(doc, "_id"));
    
    // Insert document (should auto-generate _id)
    rc = mlite_insert_one(db, "products", doc, &error);
    assert(rc == 0);
    printf("✓ Document without _id inserted successfully (auto-generated _id)\n");
    
    // Insert another document without _id (should get different _id)
    bson_t *doc2 = bson_new();
    assert(bson_append_utf8(doc2, "name", -1, "Phone", -1));
    assert(bson_append_double(doc2, "price", -1, 599.99));
    
    rc = mlite_insert_one(db, "products", doc2, &error);
    assert(rc == 0);
    printf("✓ Multiple documents without _id inserted successfully\n");
    
    // Clean up
    bson_destroy(doc);
    bson_destroy(doc2);
    mlite_close(db);
    unlink(test_file);
    
    printf("All insert_one without _id tests passed!\n");
}

void test_insert_one_invalid_bson() {
    printf("Testing insert_one with invalid BSON document...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_insert_invalid.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "test");
    assert(rc == 0);
    
    // Create invalid BSON document (manually corrupted)
    bson_t doc;
    uint8_t invalid_data[] = {0x05, 0x00, 0x00, 0x00, 0xFF};  // Invalid BSON
    bson_init_static(&doc, invalid_data, sizeof(invalid_data));
    
    // Try to insert invalid document
    rc = mlite_insert_one(db, "test", &doc, &error);
    assert(rc != 0);
    assert(error.code == 2);  // Invalid BSON error code
    printf("✓ Invalid BSON document correctly rejected\n");
    
    // Clean up
    mlite_close(db);
    unlink(test_file);
    
    printf("All invalid BSON tests passed!\n");
}

void test_insert_one_nonexistent_collection() {
    printf("Testing insert_one with non-existent collection...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_insert_no_collection.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database but don't create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    // Create valid BSON document
    bson_t *doc = bson_new();
    assert(bson_append_utf8(doc, "name", -1, "Test", -1));
    
    // Try to insert into non-existent collection
    rc = mlite_insert_one(db, "nonexistent", doc, &error);
    assert(rc != 0);
    assert(error.code == 3);  // Collection doesn't exist error code
    printf("✓ Non-existent collection correctly rejected\n");
    
    // Clean up
    bson_destroy(doc);
    mlite_close(db);
    unlink(test_file);
    
    printf("All non-existent collection tests passed!\n");
}

void test_insert_one_invalid_id_types() {
    printf("Testing insert_one with invalid _id types...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_insert_invalid_id.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "test");
    assert(rc == 0);
    
    // Test with string _id (should fail - must be ObjectId)
    bson_t *doc1 = bson_new();
    assert(bson_append_utf8(doc1, "_id", -1, "string_id", -1));
    assert(bson_append_utf8(doc1, "name", -1, "Test", -1));
    
    rc = mlite_insert_one(db, "test", doc1, &error);
    assert(rc != 0);
    assert(error.code == 6);  // Invalid _id type error code
    printf("✓ String _id correctly rejected\n");
    
    // Test with int32 _id (should fail - must be ObjectId)
    bson_t *doc2 = bson_new();
    assert(bson_append_int32(doc2, "_id", -1, 123));
    assert(bson_append_utf8(doc2, "name", -1, "Test2", -1));
    
    rc = mlite_insert_one(db, "test", doc2, &error);
    assert(rc != 0);
    assert(error.code == 6);  // Invalid _id type error code
    printf("✓ Integer _id correctly rejected\n");
    
    // Clean up
    bson_destroy(doc1);
    bson_destroy(doc2);
    mlite_close(db);
    unlink(test_file);
    
    printf("All invalid _id type tests passed!\n");
}

void test_insert_one_parameter_validation() {
    printf("Testing insert_one parameter validation...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_insert_params.mlite";
    bson_error_t error;
    bson_t *doc = bson_new();
    
    // Open database
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    // Test NULL database
    rc = mlite_insert_one(NULL, "test", doc, &error);
    assert(rc != 0);
    assert(error.code == 1);  // Invalid parameters error code
    printf("✓ NULL database correctly rejected\n");
    
    // Test NULL collection name
    rc = mlite_insert_one(db, NULL, doc, &error);
    assert(rc != 0);
    assert(error.code == 1);  // Invalid parameters error code
    printf("✓ NULL collection name correctly rejected\n");
    
    // Test NULL document
    rc = mlite_insert_one(db, "test", NULL, &error);
    assert(rc != 0);
    assert(error.code == 1);  // Invalid parameters error code
    printf("✓ NULL document correctly rejected\n");
    
    // Test with NULL error parameter (should still work)
    rc = mlite_collection_create(db, "test");
    assert(rc == 0);
    assert(bson_append_utf8(doc, "name", -1, "Test", -1));
    
    rc = mlite_insert_one(db, "test", doc, NULL);
    assert(rc == 0);  // Should work even with NULL error parameter
    printf("✓ NULL error parameter handled correctly\n");
    
    // Clean up
    bson_destroy(doc);
    mlite_close(db);
    unlink(test_file);
    
    printf("All parameter validation tests passed!\n");
}

void test_insert_one_edge_cases() {
    printf("Testing insert_one edge cases...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_insert_edge.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "edge");
    assert(rc == 0);
    
    // Test empty document
    bson_t *empty_doc = bson_new();
    rc = mlite_insert_one(db, "edge", empty_doc, &error);
    assert(rc == 0);  // Should work - will get auto-generated _id
    printf("✓ Empty document inserted successfully\n");
    
    // Test document with only _id
    bson_t *id_only_doc = bson_new();
    bson_oid_t oid;
    bson_oid_init(&oid, NULL);
    assert(bson_append_oid(id_only_doc, "_id", -1, &oid));
    
    rc = mlite_insert_one(db, "edge", id_only_doc, &error);
    assert(rc == 0);
    printf("✓ Document with only _id inserted successfully\n");
    
    // Test document with nested subdocuments
    bson_t *nested_doc = bson_new();
    bson_t *sub_doc = bson_new();
    assert(bson_append_utf8(sub_doc, "street", -1, "123 Main St", -1));
    assert(bson_append_utf8(sub_doc, "city", -1, "Anytown", -1));
    
    assert(bson_append_utf8(nested_doc, "name", -1, "John", -1));
    assert(bson_append_document(nested_doc, "address", -1, sub_doc));
    
    rc = mlite_insert_one(db, "edge", nested_doc, &error);
    assert(rc == 0);
    printf("✓ Document with nested subdocument inserted successfully\n");
    
    // Test document with array
    bson_t *array_doc = bson_new();
    bson_t *array = bson_new();
    assert(bson_append_utf8(array, "0", -1, "apple", -1));
    assert(bson_append_utf8(array, "1", -1, "banana", -1));
    assert(bson_append_utf8(array, "2", -1, "cherry", -1));
    
    assert(bson_append_utf8(array_doc, "name", -1, "Fruit List", -1));
    assert(bson_append_array(array_doc, "fruits", -1, array));
    
    rc = mlite_insert_one(db, "edge", array_doc, &error);
    assert(rc == 0);
    printf("✓ Document with array inserted successfully\n");
    
    // Clean up
    bson_destroy(empty_doc);
    bson_destroy(id_only_doc);
    bson_destroy(nested_doc);
    bson_destroy(sub_doc);
    bson_destroy(array_doc);
    bson_destroy(array);
    mlite_close(db);
    unlink(test_file);
    
    printf("All edge case tests passed!\n");
}

int main() {
    printf("Running MongoLite insert_one tests...\n\n");
    
    test_insert_one_valid_with_id();
    printf("\n");
    test_insert_one_valid_without_id();
    printf("\n");
    test_insert_one_invalid_bson();
    printf("\n");
    test_insert_one_nonexistent_collection();
    printf("\n");
    test_insert_one_invalid_id_types();
    printf("\n");
    test_insert_one_parameter_validation();
    printf("\n");
    test_insert_one_edge_cases();
    
    printf("\nAll insert_one tests passed! ✅\n");
    return 0;
}