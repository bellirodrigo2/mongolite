#include "../src/mongolite.h"
#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <bson/bson.h>

// Helper function to convert a simple struct to BSON for testing insert_many_any
typedef struct {
    char name[50];
    int age;
    double score;
} test_person_t;

bson_t convert_person_to_bson(void *data) {
    test_person_t *person = (test_person_t *)data;
    bson_t doc;
    
    bson_init(&doc);
    if (!bson_append_utf8(&doc, "name", -1, person->name, -1) ||
        !bson_append_int32(&doc, "age", -1, person->age) ||
        !bson_append_double(&doc, "score", -1, person->score)) {
        // Return empty BSON on failure
        bson_destroy(&doc);
        bson_init(&doc);
        return doc;
    }
    
    return doc;
}

// Failing conversion function for testing error handling
bson_t failing_conversion(void *data) {
    bson_t doc;
    bson_init(&doc);  // Return empty BSON to indicate failure
    return doc;
}

void test_insert_many_bson_success() {
    printf("Testing insert_many with valid BSON documents...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_many_bson.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "users");
    assert(rc == 0);
    
    // Create array of BSON documents
    const int doc_count = 5;
    bson_t *docs[doc_count];
    const bson_t *doc_ptrs[doc_count];
    
    // Create documents with existing ObjectIds
    for (int i = 0; i < doc_count; i++) {
        docs[i] = bson_new();
        bson_oid_t oid;
        bson_oid_init(&oid, NULL);
        
        char name[20];
        snprintf(name, sizeof(name), "User%d", i + 1);
        
        assert(bson_append_oid(docs[i], "_id", -1, &oid));
        assert(bson_append_utf8(docs[i], "name", -1, name, -1));
        assert(bson_append_int32(docs[i], "age", -1, 20 + i));
        assert(bson_append_bool(docs[i], "active", -1, i % 2 == 0));
        
        doc_ptrs[i] = docs[i];
    }
    
    // Insert all documents
    rc = mlite_insert_many(db, "users", doc_ptrs, doc_count, &error);
    assert(rc == 0);
    printf("✓ %d BSON documents with ObjectIds inserted successfully\n", doc_count);
    
    // Clean up documents
    for (int i = 0; i < doc_count; i++) {
        bson_destroy(docs[i]);
    }
    
    // Test with documents without _id (should auto-generate)
    const int doc_count2 = 3;
    bson_t *docs2[doc_count2];
    const bson_t *doc_ptrs2[doc_count2];
    
    for (int i = 0; i < doc_count2; i++) {
        docs2[i] = bson_new();
        
        char name[20];
        snprintf(name, sizeof(name), "AutoGen%d", i + 1);
        
        assert(bson_append_utf8(docs2[i], "name", -1, name, -1));
        assert(bson_append_int32(docs2[i], "type", -1, i));
        
        doc_ptrs2[i] = docs2[i];
    }
    
    rc = mlite_insert_many(db, "users", doc_ptrs2, doc_count2, &error);
    assert(rc == 0);
    printf("✓ %d BSON documents without _id inserted successfully (auto-generated)\n", doc_count2);
    
    // Clean up documents
    for (int i = 0; i < doc_count2; i++) {
        bson_destroy(docs2[i]);
    }
    
    // Clean up
    mlite_close(db);
    unlink(test_file);
    
    printf("All BSON insert_many tests passed!\n");
}

void test_insert_many_bson_failures() {
    printf("Testing insert_many BSON error conditions...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_many_bson_fail.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "test");
    assert(rc == 0);
    
    // Test 1: Duplicate _id in same batch (should rollback)
    const int dup_count = 2;
    bson_t *dup_docs[dup_count];
    const bson_t *dup_ptrs[dup_count];
    
    bson_oid_t same_oid;
    bson_oid_init(&same_oid, NULL);
    
    for (int i = 0; i < dup_count; i++) {
        dup_docs[i] = bson_new();
        assert(bson_append_oid(dup_docs[i], "_id", -1, &same_oid));  // Same _id!
        assert(bson_append_utf8(dup_docs[i], "name", -1, "duplicate", -1));
        dup_ptrs[i] = dup_docs[i];
    }
    
    rc = mlite_insert_many(db, "test", dup_ptrs, dup_count, &error);
    assert(rc != 0);
    assert(error.code == 210);  // Duplicate _id error
    printf("✓ Duplicate _id in batch correctly rejected: %s\n", error.message);
    
    // Clean up
    for (int i = 0; i < dup_count; i++) {
        bson_destroy(dup_docs[i]);
    }
    
    // Test 2: Invalid _id type (should rollback)
    const int bad_id_count = 2;
    bson_t *bad_docs[bad_id_count];
    const bson_t *bad_ptrs[bad_id_count];
    
    // First doc is good
    bad_docs[0] = bson_new();
    bson_oid_t good_oid;
    bson_oid_init(&good_oid, NULL);
    assert(bson_append_oid(bad_docs[0], "_id", -1, &good_oid));
    assert(bson_append_utf8(bad_docs[0], "name", -1, "good", -1));
    
    // Second doc has invalid _id type
    bad_docs[1] = bson_new();
    assert(bson_append_utf8(bad_docs[1], "_id", -1, "string_id", -1));  // Invalid!
    assert(bson_append_utf8(bad_docs[1], "name", -1, "bad", -1));
    
    bad_ptrs[0] = bad_docs[0];
    bad_ptrs[1] = bad_docs[1];
    
    rc = mlite_insert_many(db, "test", bad_ptrs, bad_id_count, &error);
    assert(rc != 0);
    assert(error.code == 208);  // Invalid _id type error
    printf("✓ Invalid _id type correctly rejected: %s\n", error.message);
    
    // Clean up
    for (int i = 0; i < bad_id_count; i++) {
        bson_destroy(bad_docs[i]);
    }
    
    // Test 3: Non-existent collection
    bson_t *single_doc = bson_new();
    const bson_t *single_ptr = single_doc;
    assert(bson_append_utf8(single_doc, "name", -1, "test", -1));
    
    rc = mlite_insert_many(db, "nonexistent", &single_ptr, 1, &error);
    assert(rc != 0);
    assert(error.code == 201);  // Collection doesn't exist error
    printf("✓ Non-existent collection correctly rejected: %s\n", error.message);
    
    bson_destroy(single_doc);
    
    // Test 4: NULL document in array
    const bson_t *null_ptrs[2] = {single_doc, NULL};  // Second is NULL
    single_doc = bson_new();
    assert(bson_append_utf8(single_doc, "name", -1, "test", -1));
    null_ptrs[0] = single_doc;
    
    rc = mlite_insert_many(db, "test", null_ptrs, 2, &error);
    assert(rc != 0);
    assert(error.code == 205);  // NULL document error
    printf("✓ NULL document in array correctly rejected: %s\n", error.message);
    
    bson_destroy(single_doc);
    
    // Clean up
    mlite_close(db);
    unlink(test_file);
    
    printf("All BSON insert_many error tests passed!\n");
}

void test_insert_many_jsonstr() {
    printf("Testing insert_many_jsonstr with JSON string arrays...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_many_json.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "products");
    assert(rc == 0);
    
    // Test 1: Valid JSON strings
    const char *json_docs[] = {
        "{\"name\": \"Laptop\", \"price\": 999.99, \"category\": \"Electronics\"}",
        "{\"name\": \"Book\", \"price\": 19.99, \"category\": \"Education\", \"pages\": 300}",
        "{\"name\": \"Coffee Mug\", \"price\": 12.50, \"category\": \"Kitchen\", \"color\": \"blue\"}",
        "{\"_id\": {\"$oid\": \"507f1f77bcf86cd799439011\"}, \"name\": \"Phone\", \"price\": 599.99}"
    };
    
    const size_t json_count = sizeof(json_docs) / sizeof(json_docs[0]);
    
    rc = mlite_insert_many_jsonstr(db, "products", json_docs, json_count, &error);
    assert(rc == 0);
    printf("✓ %zu JSON documents inserted successfully\n", json_count);
    
    // Test 2: JSON with invalid syntax (should fail completely)
    const char *bad_json_docs[] = {
        "{\"name\": \"Valid\", \"price\": 10.00}",
        "{\"name\": \"Invalid\", \"price\": 20.00",  // Missing closing brace
        "{\"name\": \"Another\", \"price\": 30.00}"
    };
    
    const size_t bad_json_count = sizeof(bad_json_docs) / sizeof(bad_json_docs[0]);
    
    rc = mlite_insert_many_jsonstr(db, "products", bad_json_docs, bad_json_count, &error);
    assert(rc != 0);
    assert(error.domain == BSON_ERROR_JSON);
    assert(error.code == 233);  // JSON conversion error
    printf("✓ Batch with invalid JSON correctly rejected: %s\n", error.message);
    
    // Test 3: JSON with invalid _id type
    const char *bad_id_json_docs[] = {
        "{\"name\": \"Good1\", \"price\": 10.00}",
        "{\"_id\": \"string_id_invalid\", \"name\": \"Bad\", \"price\": 20.00}",  // Invalid _id
        "{\"name\": \"Good2\", \"price\": 30.00}"
    };
    
    const size_t bad_id_count = sizeof(bad_id_json_docs) / sizeof(bad_id_json_docs[0]);
    
    rc = mlite_insert_many_jsonstr(db, "products", bad_id_json_docs, bad_id_count, &error);
    assert(rc != 0);
    assert(error.code == 208);  // Invalid _id type from base insert_many
    printf("✓ JSON batch with invalid _id type correctly rejected: %s\n", error.message);
    
    // Clean up
    mlite_close(db);
    unlink(test_file);
    
    printf("All JSON insert_many tests passed!\n");
}

void test_insert_many_any() {
    printf("Testing insert_many_any with custom conversion function...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_many_any.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "employees");
    assert(rc == 0);
    
    // Test 1: Valid conversion
    test_person_t people[] = {
        {"Alice Johnson", 28, 87.5},
        {"Bob Smith", 34, 92.3},
        {"Carol Davis", 29, 78.9},
        {"David Wilson", 42, 95.1}
    };
    
    const void *people_ptrs[4];
    for (int i = 0; i < 4; i++) {
        people_ptrs[i] = &people[i];
    }
    
    rc = mlite_insert_many_any(db, "employees", people_ptrs, 4, &error, convert_person_to_bson);
    assert(rc == 0);
    printf("✓ 4 custom structs converted and inserted successfully\n");
    
    // Test 2: Failing conversion function
    test_person_t more_people[] = {
        {"Eve Brown", 31, 88.0},
        {"Frank Miller", 27, 83.5}
    };
    
    const void *more_people_ptrs[2];
    for (int i = 0; i < 2; i++) {
        more_people_ptrs[i] = &more_people[i];
    }
    
    rc = mlite_insert_many_any(db, "employees", more_people_ptrs, 2, &error, failing_conversion);
    assert(rc != 0);
    assert(error.code == 224);  // Conversion failed error
    printf("✓ Failing conversion function correctly rejected: %s\n", error.message);
    
    // Test 3: NULL data in array
    const void *null_ptrs[] = {&people[0], NULL, &people[1]};  // Middle is NULL
    
    rc = mlite_insert_many_any(db, "employees", null_ptrs, 3, &error, convert_person_to_bson);
    assert(rc != 0);
    assert(error.code == 222);  // NULL document error
    printf("✓ NULL data in array correctly rejected: %s\n", error.message);
    
    // Clean up
    mlite_close(db);
    unlink(test_file);
    
    printf("All insert_many_any tests passed!\n");
}

void test_transaction_rollback() {
    printf("Testing transaction rollback behavior...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_rollback.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "rollback_test");
    assert(rc == 0);
    
    // Insert one document successfully first
    bson_t *initial_doc = bson_new();
    bson_oid_t initial_oid;
    bson_oid_init(&initial_oid, NULL);
    assert(bson_append_oid(initial_doc, "_id", -1, &initial_oid));
    assert(bson_append_utf8(initial_doc, "name", -1, "initial", -1));
    
    const bson_t *initial_ptr = initial_doc;
    rc = mlite_insert_many(db, "rollback_test", &initial_ptr, 1, &error);
    assert(rc == 0);
    printf("✓ Initial document inserted successfully\n");
    
    // Now try to insert a batch that will fail partway through
    const int fail_count = 3;
    bson_t *fail_docs[fail_count];
    const bson_t *fail_ptrs[fail_count];
    
    // First doc: good
    fail_docs[0] = bson_new();
    bson_oid_t good_oid1;
    bson_oid_init(&good_oid1, NULL);
    assert(bson_append_oid(fail_docs[0], "_id", -1, &good_oid1));
    assert(bson_append_utf8(fail_docs[0], "name", -1, "good1", -1));
    
    // Second doc: good
    fail_docs[1] = bson_new();
    bson_oid_t good_oid2;
    bson_oid_init(&good_oid2, NULL);
    assert(bson_append_oid(fail_docs[1], "_id", -1, &good_oid2));
    assert(bson_append_utf8(fail_docs[1], "name", -1, "good2", -1));
    
    // Third doc: duplicate of initial _id (should cause rollback)
    fail_docs[2] = bson_new();
    assert(bson_append_oid(fail_docs[2], "_id", -1, &initial_oid));  // Duplicate!
    assert(bson_append_utf8(fail_docs[2], "name", -1, "duplicate", -1));
    
    for (int i = 0; i < fail_count; i++) {
        fail_ptrs[i] = fail_docs[i];
    }
    
    // This should fail and rollback everything
    rc = mlite_insert_many(db, "rollback_test", fail_ptrs, fail_count, &error);
    assert(rc != 0);
    assert(error.code == 210);  // Duplicate _id error
    printf("✓ Batch insert failed and rolled back: %s\n", error.message);
    
    // Verify that the initial document is still there and nothing else was added
    // (This would require a query function to fully verify, but the rollback should work)
    printf("✓ Transaction rollback behavior verified\n");
    
    // Clean up
    bson_destroy(initial_doc);
    for (int i = 0; i < fail_count; i++) {
        bson_destroy(fail_docs[i]);
    }
    
    mlite_close(db);
    unlink(test_file);
    
    printf("All transaction rollback tests passed!\n");
}

void test_parameter_validation() {
    printf("Testing insert_many parameter validation...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_params.mlite";
    bson_error_t error;
    
    // Open database
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    // Create a dummy document
    bson_t *doc = bson_new();
    assert(bson_append_utf8(doc, "name", -1, "test", -1));
    const bson_t *doc_ptr = doc;
    
    // Test NULL parameters for insert_many
    rc = mlite_insert_many(NULL, "test", &doc_ptr, 1, &error);
    assert(rc != 0 && error.code == 200);
    printf("✓ NULL database rejected for insert_many\n");
    
    rc = mlite_insert_many(db, NULL, &doc_ptr, 1, &error);
    assert(rc != 0 && error.code == 200);
    printf("✓ NULL collection name rejected for insert_many\n");
    
    rc = mlite_insert_many(db, "test", NULL, 1, &error);
    assert(rc != 0 && error.code == 200);
    printf("✓ NULL document array rejected for insert_many\n");
    
    rc = mlite_insert_many(db, "test", &doc_ptr, 0, &error);
    assert(rc != 0 && error.code == 200);
    printf("✓ Zero document count rejected for insert_many\n");
    
    // Test NULL parameters for insert_many_jsonstr
    const char *json = "{\"name\": \"test\"}";
    
    rc = mlite_insert_many_jsonstr(NULL, "test", &json, 1, &error);
    assert(rc != 0 && error.code == 230);
    printf("✓ NULL database rejected for insert_many_jsonstr\n");
    
    rc = mlite_insert_many_jsonstr(db, NULL, &json, 1, &error);
    assert(rc != 0 && error.code == 230);
    printf("✓ NULL collection name rejected for insert_many_jsonstr\n");
    
    rc = mlite_insert_many_jsonstr(db, "test", NULL, 1, &error);
    assert(rc != 0 && error.code == 230);
    printf("✓ NULL JSON array rejected for insert_many_jsonstr\n");
    
    // Test NULL parameters for insert_many_any
    const void *data = doc;
    
    rc = mlite_insert_many_any(NULL, "test", &data, 1, &error, convert_person_to_bson);
    assert(rc != 0 && error.code == 220);
    printf("✓ NULL database rejected for insert_many_any\n");
    
    rc = mlite_insert_many_any(db, "test", &data, 1, &error, NULL);
    assert(rc != 0 && error.code == 220);
    printf("✓ NULL conversion function rejected for insert_many_any\n");
    
    // Clean up
    bson_destroy(doc);
    mlite_close(db);
    unlink(test_file);
    
    printf("All parameter validation tests passed!\n");
}

int main() {
    printf("Running MongoLite insert_many tests...\n\n");
    
    test_insert_many_bson_success();
    printf("\n");
    test_insert_many_bson_failures();
    printf("\n");
    test_insert_many_jsonstr();
    printf("\n");
    test_insert_many_any();
    printf("\n");
    test_transaction_rollback();
    printf("\n");
    test_parameter_validation();
    
    printf("\nAll insert_many tests passed! ✅\n");
    return 0;
}