/* Simple test to verify index-based queries work */

#include "src/mongolite.h"
#include <stdio.h>
#include <stdlib.h>

int main(void) {
    gerror_t error = {0};

    /* Create database */
    printf("Creating database...\n");
    mongolite_db_t *db = mongolite_db_open("test_index_query.db", 0, &error);
    if (!db) {
        fprintf(stderr, "Failed to open database: %s\n", error.message);
        return 1;
    }

    const char *collection = "users";

    /* Insert test documents */
    printf("Inserting test documents...\n");
    for (int i = 0; i < 10; i++) {
        bson_t *doc = bson_new();
        bson_append_int32(doc, "age", -1, 20 + i);
        bson_append_utf8(doc, "name", -1, (i % 2 == 0) ? "Alice" : "Bob", -1);

        if (mongolite_insert_one(db, collection, doc, NULL, &error) != 0) {
            fprintf(stderr, "Failed to insert: %s\n", error.message);
            bson_destroy(doc);
            mongolite_db_close(db, NULL);
            return 1;
        }
        bson_destroy(doc);
    }
    printf("Inserted 10 documents\n");

    /* Create index on age field */
    printf("Creating index on 'age' field...\n");
    bson_t *keys = bson_new();
    bson_append_int32(keys, "age", -1, 1);

    if (mongolite_create_index(db, collection, "age_idx", keys, false, false, &error) != 0) {
        fprintf(stderr, "Failed to create index: %s\n", error.message);
        bson_destroy(keys);
        mongolite_db_close(db, NULL);
        return 1;
    }
    bson_destroy(keys);
    printf("Index created successfully\n");

    /* Query using the index */
    printf("Querying with index (age = 25)...\n");
    bson_t *filter = bson_new();
    bson_append_int32(filter, "age", -1, 25);

    bson_t *result = mongolite_find_one(db, collection, filter, NULL, &error);
    if (!result) {
        fprintf(stderr, "Query failed: %s\n", error.message);
        bson_destroy(filter);
        mongolite_db_close(db, NULL);
        return 1;
    }

    /* Print result */
    char *str = bson_as_json(result, NULL);
    printf("Found document: %s\n", str);
    bson_free(str);
    bson_destroy(result);
    bson_destroy(filter);

    /* Query without index (name = "Alice") */
    printf("\nQuerying without index (name = Alice)...\n");
    filter = bson_new();
    bson_append_utf8(filter, "name", -1, "Alice", -1);

    result = mongolite_find_one(db, collection, filter, NULL, &error);
    if (!result) {
        fprintf(stderr, "Query failed: %s\n", error.message);
        bson_destroy(filter);
        mongolite_db_close(db, NULL);
        return 1;
    }

    str = bson_as_json(result, NULL);
    printf("Found document: %s\n", str);
    bson_free(str);
    bson_destroy(result);
    bson_destroy(filter);

    /* Cleanup */
    mongolite_db_close(db, NULL);
    printf("\nTest completed successfully!\n");

    return 0;
}
