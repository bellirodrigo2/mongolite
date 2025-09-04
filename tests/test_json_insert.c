#include "../src/mongolite.h"
#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <bson/bson.h>

// Helper function to convert a simple struct to BSON for testing insert_one_any
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

void test_insert_one_jsonstr_valid() {
    printf("Testing insert_one_jsonstr with valid JSON strings...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_json_valid.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "people");
    assert(rc == 0);
    
    // Test 1: Simple JSON document
    const char *json1 = "{\"name\": \"Alice\", \"age\": 25, \"active\": true}";
    rc = mlite_insert_one_jsonstr(db, "people", json1, &error);
    assert(rc == 0);
    printf("✓ Simple JSON document inserted successfully\n");
    
    // Test 2: JSON with nested object
    const char *json2 = "{"
        "\"name\": \"Bob\", "
        "\"age\": 30, "
        "\"address\": {"
            "\"street\": \"123 Main St\", "
            "\"city\": \"Anytown\", "
            "\"zip\": 12345"
        "}"
    "}";
    rc = mlite_insert_one_jsonstr(db, "people", json2, &error);
    assert(rc == 0);
    printf("✓ JSON with nested object inserted successfully\n");
    
    // Test 3: JSON with array
    const char *json3 = "{"
        "\"name\": \"Charlie\", "
        "\"age\": 28, "
        "\"hobbies\": [\"reading\", \"swimming\", \"coding\"]"
    "}";
    rc = mlite_insert_one_jsonstr(db, "people", json3, &error);
    assert(rc == 0);
    printf("✓ JSON with array inserted successfully\n");
    
    // Test 4: JSON with existing ObjectId
    const char *json4 = "{"
        "\"_id\": {\"$oid\": \"507f1f77bcf86cd799439011\"}, "
        "\"name\": \"David\", "
        "\"age\": 35"
    "}";
    rc = mlite_insert_one_jsonstr(db, "people", json4, &error);
    assert(rc == 0);
    printf("✓ JSON with existing ObjectId inserted successfully\n");
    
    // Test 5: JSON with various data types
    const char *json5 = "{"
        "\"name\": \"Eve\", "
        "\"age\": 22, "
        "\"score\": 95.5, "
        "\"active\": false, "
        "\"notes\": null, "
        "\"timestamp\": {\"$date\": \"2023-01-01T00:00:00.000Z\"}"
    "}";
    rc = mlite_insert_one_jsonstr(db, "people", json5, &error);
    assert(rc == 0);
    printf("✓ JSON with various data types inserted successfully\n");
    
    // Test 6: Minimal JSON document
    const char *json6 = "{}";
    rc = mlite_insert_one_jsonstr(db, "people", json6, &error);
    assert(rc == 0);
    printf("✓ Empty JSON document inserted successfully (auto-generated _id)\n");
    
    // Clean up
    mlite_close(db);
    unlink(test_file);
    
    printf("All valid JSON tests passed!\n");
}

void test_insert_one_jsonstr_invalid() {
    printf("Testing insert_one_jsonstr with invalid JSON strings...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_json_invalid.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "test");
    assert(rc == 0);
    
    // Test 1: Malformed JSON - missing closing brace
    const char *bad_json1 = "{\"name\": \"Alice\", \"age\": 25";
    rc = mlite_insert_one_jsonstr(db, "test", bad_json1, &error);
    assert(rc != 0);
    assert(error.domain == BSON_ERROR_JSON);
    assert(error.code == 111);
    printf("✓ Malformed JSON (missing brace) correctly rejected: %s\n", error.message);
    
    // Test 2: Invalid JSON - wrong quotes
    const char *bad_json2 = "{name: 'Alice', age: 25}";
    rc = mlite_insert_one_jsonstr(db, "test", bad_json2, &error);
    assert(rc != 0);
    assert(error.domain == BSON_ERROR_JSON);
    printf("✓ Invalid JSON (wrong quotes) correctly rejected: %s\n", error.message);
    
    // Test 3: Empty string
    const char *bad_json3 = "";
    rc = mlite_insert_one_jsonstr(db, "test", bad_json3, &error);
    assert(rc != 0);
    assert(error.domain == BSON_ERROR_JSON);
    printf("✓ Empty JSON string correctly rejected: %s\n", error.message);
    
    // Test 4: Non-JSON string
    const char *bad_json4 = "This is not JSON at all!";
    rc = mlite_insert_one_jsonstr(db, "test", bad_json4, &error);
    assert(rc != 0);
    assert(error.domain == BSON_ERROR_JSON);
    printf("✓ Non-JSON string correctly rejected: %s\n", error.message);
    
    // Test 5: JSON with trailing comma
    const char *bad_json5 = "{\"name\": \"Alice\", \"age\": 25,}";
    rc = mlite_insert_one_jsonstr(db, "test", bad_json5, &error);
    assert(rc != 0);
    assert(error.domain == BSON_ERROR_JSON);
    printf("✓ JSON with trailing comma correctly rejected: %s\n", error.message);
    
    // Test 6: JSON with duplicate keys (technically valid JSON but problematic)
    const char *bad_json6 = "{\"name\": \"Alice\", \"name\": \"Bob\"}";
    rc = mlite_insert_one_jsonstr(db, "test", bad_json6, &error);
    // Note: This might succeed as libbson may accept duplicate keys
    // The test validates our error handling works, regardless of outcome
    if (rc != 0) {
        printf("✓ JSON with duplicate keys rejected: %s\n", error.message);
    } else {
        printf("✓ JSON with duplicate keys accepted (libbson behavior)\n");
    }
    
    // Clean up
    mlite_close(db);
    unlink(test_file);
    
    printf("All invalid JSON tests passed!\n");
}

void test_insert_one_jsonstr_edge_cases() {
    printf("Testing insert_one_jsonstr edge cases...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_json_edge.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "edge");
    assert(rc == 0);
    
    // Test 1: JSON with unicode characters
    const char *json_unicode = "{\"name\": \"José\", \"city\": \"São Paulo\", \"emoji\": \"😀\"}";
    rc = mlite_insert_one_jsonstr(db, "edge", json_unicode, &error);
    assert(rc == 0);
    printf("✓ JSON with unicode characters inserted successfully\n");
    
    // Test 2: JSON with escaped characters
    const char *json_escaped = "{\"message\": \"Hello\\nWorld\\t\\\"Quote\\\"\", \"path\": \"C:\\\\\\\\folder\"}";
    rc = mlite_insert_one_jsonstr(db, "edge", json_escaped, &error);
    assert(rc == 0);
    printf("✓ JSON with escaped characters inserted successfully\n");
    
    // Test 3: JSON with large numbers
    const char *json_numbers = "{\"big_int\": 9223372036854775807, \"big_float\": 1.7976931348623157e+308}";
    rc = mlite_insert_one_jsonstr(db, "edge", json_numbers, &error);
    assert(rc == 0);
    printf("✓ JSON with large numbers inserted successfully\n");
    
    // Test 4: Deeply nested JSON
    const char *json_nested = "{"
        "\"level1\": {"
            "\"level2\": {"
                "\"level3\": {"
                    "\"level4\": {"
                        "\"level5\": \"deep value\""
                    "}"
                "}"
            "}"
        "}"
    "}";
    rc = mlite_insert_one_jsonstr(db, "edge", json_nested, &error);
    assert(rc == 0);
    printf("✓ Deeply nested JSON inserted successfully\n");
    
    // Test 5: JSON with invalid _id type (should be rejected)
    const char *json_bad_id = "{\"_id\": \"string_id_not_objectid\", \"name\": \"Test\"}";
    rc = mlite_insert_one_jsonstr(db, "edge", json_bad_id, &error);
    assert(rc != 0);
    assert(error.code == 6);  // Invalid _id type error from base insert_one
    printf("✓ JSON with invalid _id type correctly rejected: %s\n", error.message);
    
    // Clean up
    mlite_close(db);
    unlink(test_file);
    
    printf("All edge case tests passed!\n");
}

void test_insert_one_any() {
    printf("Testing insert_one_any with custom conversion function...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_any.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database and create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    rc = mlite_collection_create(db, "people");
    assert(rc == 0);
    
    // Test 1: Valid conversion
    test_person_t person1 = {"Alice Johnson", 28, 87.5};
    rc = mlite_insert_one_any(db, "people", &person1, &error, convert_person_to_bson);
    assert(rc == 0);
    printf("✓ Custom struct converted and inserted successfully\n");
    
    // Test 2: Another valid conversion
    test_person_t person2 = {"Bob Smith", 34, 92.3};
    rc = mlite_insert_one_any(db, "people", &person2, &error, convert_person_to_bson);
    assert(rc == 0);
    printf("✓ Second custom struct converted and inserted successfully\n");
    
    // Test 3: Failing conversion function
    test_person_t person3 = {"Charlie Brown", 25, 78.9};
    rc = mlite_insert_one_any(db, "people", &person3, &error, failing_conversion);
    assert(rc != 0);
    assert(error.code == 101);  // Conversion failed error
    printf("✓ Failing conversion function correctly rejected: %s\n", error.message);
    
    // Test 4: NULL conversion function
    rc = mlite_insert_one_any(db, "people", &person1, &error, NULL);
    assert(rc != 0);
    assert(error.code == 100);  // Invalid parameters error
    printf("✓ NULL conversion function correctly rejected: %s\n", error.message);
    
    // Clean up
    mlite_close(db);
    unlink(test_file);
    
    printf("All insert_one_any tests passed!\n");
}

void test_error_propagation() {
    printf("Testing error propagation and categorization...\n");
    
    mlite_db_t *db = NULL;
    const char *test_file = "test_errors.mlite";
    bson_error_t error;
    
    // Remove test file if it exists
    unlink(test_file);
    
    // Open database but DON'T create collection
    int rc = mlite_open(test_file, &db);
    assert(rc == 0);
    
    // Test 1: Collection doesn't exist (should propagate from base insert_one)
    const char *json = "{\"name\": \"Test\"}";
    rc = mlite_insert_one_jsonstr(db, "nonexistent", json, &error);
    assert(rc != 0);
    assert(error.code == 3);  // Collection doesn't exist error from base insert_one
    printf("✓ Collection not found error correctly propagated: %s\n", error.message);
    
    // Test 2: Test parameter validation errors
    rc = mlite_insert_one_jsonstr(NULL, "test", json, &error);
    assert(rc != 0);
    assert(error.code == 110);  // jsonstr parameter validation error
    printf("✓ Parameter validation error correctly set: %s\n", error.message);
    
    rc = mlite_insert_one_jsonstr(db, NULL, json, &error);
    assert(rc != 0);
    assert(error.code == 110);  // jsonstr parameter validation error
    printf("✓ NULL collection name error correctly set: %s\n", error.message);
    
    rc = mlite_insert_one_jsonstr(db, "test", NULL, &error);
    assert(rc != 0);
    assert(error.code == 110);  // jsonstr parameter validation error
    printf("✓ NULL JSON string error correctly set: %s\n", error.message);
    
    // Test 3: Error handling with NULL error parameter (should not crash)
    rc = mlite_insert_one_jsonstr(db, "test", "{invalid json", NULL);
    assert(rc != 0);
    printf("✓ NULL error parameter handled gracefully\n");
    
    // Clean up
    mlite_close(db);
    unlink(test_file);
    
    printf("All error propagation tests passed!\n");
}

int main() {
    printf("Running MongoLite JSON and Any insert tests...\n\n");
    
    test_insert_one_jsonstr_valid();
    printf("\n");
    test_insert_one_jsonstr_invalid();
    printf("\n");
    test_insert_one_jsonstr_edge_cases();
    printf("\n");
    test_insert_one_any();
    printf("\n");
    test_error_propagation();
    
    printf("\nAll JSON and Any insert tests passed! ✅\n");
    return 0;
}