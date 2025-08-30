/*
** Test MongoLite BSON Interface - Header-Only Version
** Verify our zero-cost inline BSON wrapper works correctly
*/

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "../mongolite/mongolite_bson.h"

void test_basic_creation(void) {
    printf("Testing basic BSON creation...\n");
    
    mongolite_bson_t *doc = mongolite_bson_new();
    assert(doc != NULL);
    assert(mongolite_bson_empty(doc));
    assert(mongolite_bson_count_keys(doc) == 0);
    
    mongolite_bson_destroy(doc);
    printf("✓ Basic creation test passed\n");
}

void test_json_conversion(void) {
    printf("Testing JSON conversion...\n");
    
    const char *json = "{\"name\":\"John\",\"age\":30,\"active\":true}";
    
    mongolite_bson_error_t error;
    mongolite_bson_t *doc = mongolite_bson_new_from_json(json, &error);
    assert(doc != NULL);
    assert(!mongolite_bson_empty(doc));
    assert(mongolite_bson_count_keys(doc) == 3);
    
    // Convert back to JSON
    size_t json_len;
    char *json_out = mongolite_bson_as_json(doc, &json_len);
    assert(json_out != NULL);
    assert(json_len > 0);
    printf("JSON out: %s\n", json_out);
    
    bson_free(json_out);  // Use libbson's free for JSON strings
    mongolite_bson_destroy(doc);
    printf("✓ JSON conversion test passed\n");
}

void test_field_access(void) {
    printf("Testing field access...\n");
    
    const char *json = "{\"name\":\"Alice\",\"age\":25,\"score\":95.5,\"active\":true}";
    
    mongolite_bson_error_t error;
    mongolite_bson_t *doc = mongolite_bson_new_from_json(json, &error);
    assert(doc != NULL);
    
    // Test field existence
    assert(mongolite_bson_has_field(doc, "name"));
    assert(mongolite_bson_has_field(doc, "age"));
    assert(mongolite_bson_has_field(doc, "score"));
    assert(mongolite_bson_has_field(doc, "active"));
    assert(!mongolite_bson_has_field(doc, "missing"));
    
    // Test field values
    const char *name;
    assert(mongolite_bson_get_utf8(doc, "name", &name));
    assert(strcmp(name, "Alice") == 0);
    
    int32_t age;
    assert(mongolite_bson_get_int32(doc, "age", &age));
    assert(age == 25);
    
    double score;
    assert(mongolite_bson_get_double(doc, "score", &score));
    assert(score == 95.5);
    
    bool active;
    assert(mongolite_bson_get_bool(doc, "active", &active));
    assert(active == true);
    
    mongolite_bson_destroy(doc);
    printf("✓ Field access test passed\n");
}

void test_document_building(void) {
    printf("Testing document building...\n");
    
    mongolite_bson_t *doc = mongolite_bson_new();
    assert(doc != NULL);
    
    // Build document programmatically
    assert(mongolite_bson_append_utf8(doc, "username", "testuser"));
    assert(mongolite_bson_append_int32(doc, "user_id", 12345));
    assert(mongolite_bson_append_double(doc, "balance", 1000.50));
    assert(mongolite_bson_append_bool(doc, "verified", false));
    assert(mongolite_bson_append_null(doc, "last_login"));
    
    assert(!mongolite_bson_empty(doc));
    assert(mongolite_bson_count_keys(doc) == 5);
    
    // Verify the built document
    const char *username;
    assert(mongolite_bson_get_utf8(doc, "username", &username));
    assert(strcmp(username, "testuser") == 0);
    
    int32_t user_id;
    assert(mongolite_bson_get_int32(doc, "user_id", &user_id));
    assert(user_id == 12345);
    
    double balance;
    assert(mongolite_bson_get_double(doc, "balance", &balance));
    assert(balance == 1000.50);
    
    bool verified;
    assert(mongolite_bson_get_bool(doc, "verified", &verified));
    assert(verified == false);
    
    assert(mongolite_bson_has_field(doc, "last_login"));
    
    // Convert to JSON to see the result
    size_t json_len;
    char *json = mongolite_bson_as_json(doc, &json_len);
    printf("Built document: %s\n", json);
    bson_free(json);
    
    mongolite_bson_destroy(doc);
    printf("✓ Document building test passed\n");
}

void test_iteration(void) {
    printf("Testing document iteration...\n");
    
    const char *json = "{\"a\":1,\"b\":\"hello\",\"c\":true}";
    
    mongolite_bson_error_t error;
    mongolite_bson_t *doc = mongolite_bson_new_from_json(json, &error);
    assert(doc != NULL);
    
    mongolite_bson_iter_t iter;
    assert(mongolite_bson_iter_init(&iter, doc));
    
    int field_count = 0;
    while (mongolite_bson_iter_next(&iter)) {
        const char *key = mongolite_bson_iter_key(&iter);
        mongolite_bson_type_t type = mongolite_bson_iter_type(&iter);
        
        printf("Field: %s, Type: %d\n", key, (int)type);
        field_count++;
    }
    
    assert(field_count == 3);
    
    mongolite_bson_destroy(doc);
    printf("✓ Document iteration test passed\n");
}

void test_binary_data(void) {
    printf("Testing binary data handling...\n");
    
    // Create a BSON document
    mongolite_bson_t *doc = mongolite_bson_new();
    assert(mongolite_bson_append_utf8(doc, "type", "binary_test"));
    assert(mongolite_bson_append_int32(doc, "version", 1));
    
    // Get binary representation
    uint32_t data_len;
    const uint8_t *data = mongolite_bson_get_data(doc, &data_len);
    assert(data != NULL);
    assert(data_len > 0);
    
    printf("BSON binary size: %u bytes\n", data_len);
    
    // Create new document from binary data
    mongolite_bson_t *doc2 = mongolite_bson_new_from_data(data, data_len);
    assert(doc2 != NULL);
    
    // Verify the reconstructed document
    const char *type;
    assert(mongolite_bson_get_utf8(doc2, "type", &type));
    assert(strcmp(type, "binary_test") == 0);
    
    int32_t version;
    assert(mongolite_bson_get_int32(doc2, "version", &version));
    assert(version == 1);
    
    mongolite_bson_destroy(doc);
    mongolite_bson_destroy(doc2);
    printf("✓ Binary data test passed\n");
}

void test_arrays(void) {
    printf("Testing array operations...\n");
    
    // Create array
    mongolite_bson_t *array = mongolite_bson_array_new();
    assert(mongolite_bson_array_append_utf8(array, "first"));
    assert(mongolite_bson_array_append_utf8(array, "second"));
    assert(mongolite_bson_array_append_int32(array, 42));
    
    assert(mongolite_bson_array_get_length(array) == 3);
    
    // Create document with array
    mongolite_bson_t *doc = mongolite_bson_new();
    assert(mongolite_bson_append_utf8(doc, "name", "test"));
    assert(mongolite_bson_append_array(doc, "items", array));
    
    // Convert to JSON to verify
    size_t json_len;
    char *json = mongolite_bson_as_json(doc, &json_len);
    printf("Array document: %s\n", json);
    bson_free(json);
    
    mongolite_bson_destroy(array);
    mongolite_bson_destroy(doc);
    printf("✓ Array operations test passed\n");
}

int main(void) {
    printf("MongoLite BSON Interface Tests (Header-Only Version)\n");
    printf("====================================================\n");
    
    test_basic_creation();
    test_json_conversion();
    test_field_access();
    test_document_building();
    test_iteration();
    test_binary_data();
    test_arrays();
    
    printf("\n✅ All tests passed! MongoLite BSON header-only interface is working.\n");
    
    const char *version = mongolite_bson_get_version();
    printf("Version: %s\n", version);
    
    return 0;
}