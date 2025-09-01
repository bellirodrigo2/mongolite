#include "../src/mongolite.h"
#include <stdio.h>
#include <assert.h>
#include <unistd.h>

void test_open_close() {
    printf("Testing mlite_open/close...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_mongolite.mlite";
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Test mlite_open
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    assert(db != NULL);
    printf("✓ Database opened successfully\n");
    
    // Test mlite_close
    rc = mlite_close(db);
    assert(rc == 0);
    printf("✓ Database closed successfully\n");
    
    // Test opening with specific flags
    rc = mlite_open_v2(test_file, &db, MLITE_OPEN_READWRITE);
    assert(rc == 0);
    assert(db != NULL);
    printf("✓ Database opened with flags successfully\n");
    
    rc = mlite_close(db);
    assert(rc == 0);
    
    // Test opening non-existent file without CREATE flag (should fail)
    unlink(test_file);
    rc = mlite_open_v2("nonexistent.mlite", &db, MLITE_OPEN_READWRITE);
    assert(rc != 0);
    printf("✓ Failed to open non-existent file without CREATE flag (expected)\n");
    
    // Cleanup
    unlink(test_file);
    
    printf("All open/close tests passed!\n");
}

void test_error_handling() {
    printf("Testing error handling...\n");
    
    mlite_db_t *db = NULL;
    
    // Test invalid parameters
    int rc = mlite_open(NULL, &db);
    assert(rc != 0);
    
    rc = mlite_open("test.mlite", NULL);
    assert(rc != 0);
    
    rc = mlite_close(NULL);
    assert(rc != 0);
    
    printf("✓ Error handling tests passed!\n");
}

void test_collection_operations() {
    printf("Testing collection operations...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_collections.mlite";
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    assert(db != NULL);
    
    // Test collection doesn't exist initially
    assert(mlite_collection_exists(db, "users") == false);
    printf("✓ Collection doesn't exist initially\n");
    
    // Create collection
    rc = mlite_collection_create(db, "users");
    assert(rc == 0);
    printf("✓ Collection created successfully\n");
    
    // Check collection now exists
    assert(mlite_collection_exists(db, "users") == true);
    printf("✓ Collection exists after creation\n");
    
    // Creating same collection again should succeed (idempotent)
    rc = mlite_collection_create(db, "users");
    assert(rc == 0);
    printf("✓ Creating existing collection is idempotent\n");
    
    // Create another collection
    rc = mlite_collection_create(db, "products");
    assert(rc == 0);
    assert(mlite_collection_exists(db, "products") == true);
    printf("✓ Multiple collections can be created\n");
    
    // Drop collection
    rc = mlite_collection_drop(db, "products");
    assert(rc == 0);
    assert(mlite_collection_exists(db, "products") == false);
    printf("✓ Collection dropped successfully\n");
    
    // Dropping non-existent collection should succeed (idempotent)
    rc = mlite_collection_drop(db, "nonexistent");
    assert(rc == 0);
    printf("✓ Dropping non-existent collection is idempotent\n");
    
    // users collection should still exist
    assert(mlite_collection_exists(db, "users") == true);
    printf("✓ Other collections unaffected by drop\n");
    
    // Clean up
    mlite_close(db);
    unlink(test_file);
    
    printf("All collection tests passed!\n");
}

void test_collection_error_handling() {
    printf("Testing collection error handling...\n");
    
    mlite_db_t *db = NULL;
    
    // Test invalid parameters
    int rc = mlite_collection_create(NULL, "test");
    assert(rc != 0);
    
    rc = mlite_collection_create(db, NULL);
    assert(rc != 0);
    
    rc = mlite_collection_drop(NULL, "test");
    assert(rc != 0);
    
    rc = mlite_collection_drop(db, NULL);
    assert(rc != 0);
    
    assert(mlite_collection_exists(NULL, "test") == false);
    assert(mlite_collection_exists(db, NULL) == false);
    
    printf("✓ Collection error handling tests passed!\n");
}

int main() {
    printf("Running MongoLite tests...\n\n");
    
    test_open_close();
    printf("\n");
    test_error_handling();
    printf("\n");
    test_collection_operations();
    printf("\n");
    test_collection_error_handling();
    
    printf("\nAll tests passed! ✅\n");
    return 0;
}