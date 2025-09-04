#include "mongolite.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

// Test cross-type BSON comparisons following MongoDB rules
int main() {
    mlite_db_t *db;
    bson_error_t error;
    const char *test_file = "test_cross_type.mlite";
    
    printf("Testing MongoDB cross-type comparisons...\n");
    
    // Clean up any existing test file
    unlink(test_file);
    
    // Open database
    if (mlite_open(test_file, &db) != 0) {
        printf("❌ Failed to open database\n");
        return 1;
    }
    
    // Create test collection
    if (mlite_collection_create(db, "numbers") != 0) {
        printf("❌ Failed to create collection\n");
        mlite_close(db);
        unlink(test_file);
        return 1;
    }
    
    // Insert documents with same logical value but different BSON types
    bson_t *doc1 = bson_new();
    bson_append_int32(doc1, "value", -1, 42);  // int32
    bson_append_utf8(doc1, "type", -1, "int32", -1);
    
    bson_t *doc2 = bson_new();
    bson_append_int64(doc2, "value", -1, 42);  // int64
    bson_append_utf8(doc2, "type", -1, "int64", -1);
    
    bson_t *doc3 = bson_new();
    bson_append_double(doc3, "value", -1, 42.0);  // double
    bson_append_utf8(doc3, "type", -1, "double", -1);
    
    bson_t *doc4 = bson_new();
    bson_append_double(doc4, "value", -1, 42.5);  // different double
    bson_append_utf8(doc4, "type", -1, "double_different", -1);
    
    // Insert all documents
    mlite_insert_one(db, "numbers", doc1, &error);
    mlite_insert_one(db, "numbers", doc2, &error);
    mlite_insert_one(db, "numbers", doc3, &error);
    mlite_insert_one(db, "numbers", doc4, &error);
    
    // Test 1: Search for int32(42) - should match int32, int64, and double(42.0)
    printf("\nTesting cross-type equality for value 42...\n");
    bson_t *filter_int32 = bson_new();
    bson_append_int32(filter_int32, "value", -1, 42);
    
    mlite_cursor_t *cursor = mlite_find(db, "numbers", filter_int32, NULL);
    int count = 0;
    const bson_t *doc;
    
    while (mlite_cursor_next(cursor, &doc)) {
        bson_iter_t iter;
        if (bson_iter_init_find(&iter, doc, "type")) {
            const char *type_str = bson_iter_utf8(&iter, NULL);
            printf("✓ Found match: %s\n", type_str);
            count++;
        }
    }
    
    printf("Found %d matches for int32(42)\n", count);
    if (count == 3) {  // Should match int32, int64, and double(42.0)
        printf("✅ Cross-type equality works correctly!\n");
    } else {
        printf("❌ Expected 3 matches, got %d\n", count);
    }
    
    mlite_cursor_destroy(cursor);
    
    // Test 2: Search for double(42.5) - should only match double(42.5)
    printf("\nTesting specific double value 42.5...\n");
    bson_t *filter_double = bson_new();
    bson_append_double(filter_double, "value", -1, 42.5);
    
    cursor = mlite_find(db, "numbers", filter_double, NULL);
    count = 0;
    
    while (mlite_cursor_next(cursor, &doc)) {
        bson_iter_t iter;
        if (bson_iter_init_find(&iter, doc, "type")) {
            const char *type_str = bson_iter_utf8(&iter, NULL);
            printf("✓ Found match: %s\n", type_str);
            count++;
        }
    }
    
    printf("Found %d matches for double(42.5)\n", count);
    if (count == 1) {  // Should only match the 42.5 double
        printf("✅ Precise double comparison works!\n");
    } else {
        printf("❌ Expected 1 match, got %d\n", count);
    }
    
    mlite_cursor_destroy(cursor);
    
    // Cleanup
    bson_destroy(doc1);
    bson_destroy(doc2);
    bson_destroy(doc3);
    bson_destroy(doc4);
    bson_destroy(filter_int32);
    bson_destroy(filter_double);
    
    mlite_close(db);
    unlink(test_file);
    
    printf("\nCross-type comparison tests completed!\n");
    return 0;
}