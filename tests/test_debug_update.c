/*
 * test_debug_update.c - Debug test for combined $set + $inc
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mongolite.h"

#ifdef _WIN32
#include <windows.h>
#endif

#define TEST_DB_PATH "./test_debug_update_db"

static void cleanup(void) {
#ifdef _WIN32
    system("rmdir /s /q " TEST_DB_PATH " 2>nul");
#else
    system("rm -rf " TEST_DB_PATH);
#endif
}

int main(void) {
    cleanup();

    printf("=== Debug: Combined $set + $inc ===\n\n");

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    printf("1. Opening database...\n");
    int rc = mongolite_open(TEST_DB_PATH, &db, NULL, &error);
    if (rc != 0) {
        printf("FAILED to open db: %s\n", error.message);
        return 1;
    }
    printf("   OK\n");

    printf("2. Creating collection...\n");
    rc = mongolite_collection_create(db, "test", NULL, &error);
    if (rc != 0) {
        printf("FAILED to create collection: %s\n", error.message);
        mongolite_close(db);
        return 1;
    }
    printf("   OK\n");

    printf("3. Inserting document...\n");
    bson_oid_t id;
    rc = mongolite_insert_one_json(db, "test",
        "{\"name\": \"Test\", \"age\": 30, \"score\": 100.0, \"active\": false, \"department\": \"eng\"}",
        &id, &error);
    if (rc != 0) {
        printf("FAILED to insert: %s\n", error.message);
        mongolite_close(db);
        return 1;
    }
    char oid_str[25];
    bson_oid_to_string(&id, oid_str);
    printf("   OK - _id: %s\n", oid_str);

    printf("4. Testing $set only...\n");
    {
        bson_t *filter = bson_new();
        BSON_APPEND_OID(filter, "_id", &id);

        bson_t *update = bson_new();
        bson_t set_doc;
        BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
        BSON_APPEND_BOOL(&set_doc, "active", true);
        bson_append_document_end(update, &set_doc);

        rc = mongolite_update_one(db, "test", filter, update, false, &error);
        bson_destroy(filter);
        bson_destroy(update);

        if (rc < 0) {
            printf("FAILED: %s\n", error.message);
        } else {
            printf("   OK\n");
        }
    }

    printf("5. Testing $inc only...\n");
    {
        bson_t *filter = bson_new();
        BSON_APPEND_OID(filter, "_id", &id);

        bson_t *update = bson_new();
        bson_t inc_doc;
        BSON_APPEND_DOCUMENT_BEGIN(update, "$inc", &inc_doc);
        BSON_APPEND_INT32(&inc_doc, "age", 1);
        bson_append_document_end(update, &inc_doc);

        rc = mongolite_update_one(db, "test", filter, update, false, &error);
        bson_destroy(filter);
        bson_destroy(update);

        if (rc < 0) {
            printf("FAILED: %s\n", error.message);
        } else {
            printf("   OK\n");
        }
    }

    printf("6. Verifying document before combined update...\n");
    {
        bson_t *filter = bson_new();
        BSON_APPEND_OID(filter, "_id", &id);

        bson_t *found = mongolite_find_one(db, "test", filter, NULL, &error);
        bson_destroy(filter);

        if (found) {
            char *json = bson_as_canonical_extended_json(found, NULL);
            printf("   Doc: %s\n", json);
            bson_free(json);
            bson_destroy(found);
        } else {
            printf("   NOT FOUND\n");
        }
    }

    printf("7. Testing $set + $inc combined...\n");
    {
        bson_t *filter = bson_new();
        BSON_APPEND_OID(filter, "_id", &id);

        bson_t *update = bson_new();

        /* $set */
        bson_t set_doc;
        BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
        BSON_APPEND_BOOL(&set_doc, "active", true);
        BSON_APPEND_UTF8(&set_doc, "department", "updated");
        bson_append_document_end(update, &set_doc);

        /* $inc */
        bson_t inc_doc;
        BSON_APPEND_DOCUMENT_BEGIN(update, "$inc", &inc_doc);
        BSON_APPEND_INT32(&inc_doc, "age", 1);
        BSON_APPEND_DOUBLE(&inc_doc, "score", 0.5);
        bson_append_document_end(update, &inc_doc);

        printf("   Calling mongolite_update_one...\n");
        rc = mongolite_update_one(db, "test", filter, update, false, &error);
        printf("   Returned: %d\n", rc);

        bson_destroy(filter);
        bson_destroy(update);

        if (rc < 0) {
            printf("FAILED: %s\n", error.message);
        } else {
            printf("   OK\n");
        }
    }

    printf("8. Verifying final document...\n");
    {
        bson_t *filter = bson_new();
        BSON_APPEND_OID(filter, "_id", &id);

        bson_t *found = mongolite_find_one(db, "test", filter, NULL, &error);
        bson_destroy(filter);

        if (found) {
            char *json = bson_as_canonical_extended_json(found, NULL);
            printf("   Final: %s\n", json);
            bson_free(json);
            bson_destroy(found);
        } else {
            printf("   NOT FOUND\n");
        }
    }

    printf("9. Loop test: repeated $set + $inc (10 iterations)...\n");
    for (int i = 0; i < 10; i++) {
        bson_t *filter = bson_new();
        BSON_APPEND_OID(filter, "_id", &id);

        bson_t *update = bson_new();

        bson_t set_doc;
        BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
        BSON_APPEND_BOOL(&set_doc, "active", (i % 2) == 0);
        bson_append_document_end(update, &set_doc);

        bson_t inc_doc;
        BSON_APPEND_DOCUMENT_BEGIN(update, "$inc", &inc_doc);
        BSON_APPEND_INT32(&inc_doc, "age", 1);
        bson_append_document_end(update, &inc_doc);

        rc = mongolite_update_one(db, "test", filter, update, false, &error);
        bson_destroy(filter);
        bson_destroy(update);

        if (rc < 0) {
            printf("   FAILED at iteration %d: %s\n", i, error.message);
            break;
        }
        printf("   Iteration %d OK\n", i);
    }

    printf("\n10. Closing database...\n");
    mongolite_close(db);
    cleanup();

    printf("\n=== All debug tests completed ===\n");
    return 0;
}
