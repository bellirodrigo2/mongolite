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

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;  /* 32MB */
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    if (rc != 0) {
        printf("FAILED to open db: %s\n", error.message);
        return 1;
    }
    rc = mongolite_collection_create(db, "test", NULL, &error);
    if (rc != 0) {
        printf("FAILED to create collection: %s\n", error.message);
        mongolite_close(db);
        return 1;
    }
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
        } 
    }
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
        }
    }

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

        rc = mongolite_update_one(db, "test", filter, update, false, &error);

        bson_destroy(filter);
        bson_destroy(update);

        if (rc < 0) {
            printf("FAILED: %s\n", error.message);
        }
    }
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
    }
    for (int i = 0; i < 2000; i++) {
        bson_t *filter = bson_new();
        BSON_APPEND_OID(filter, "_id", &id);

        bson_t *update = bson_new();

        bson_t set_doc;
        BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
        BSON_APPEND_BOOL(&set_doc, "active", (i % 2) == 0);
        BSON_APPEND_UTF8(&set_doc, "department", "stress");
        bson_append_document_end(update, &set_doc);

        bson_t inc_doc;
        BSON_APPEND_DOCUMENT_BEGIN(update, "$inc", &inc_doc);
        BSON_APPEND_INT32(&inc_doc, "age", 1);
        BSON_APPEND_DOUBLE(&inc_doc, "score", 0.5);
        bson_append_document_end(update, &inc_doc);

        rc = mongolite_update_one(db, "test", filter, update, false, &error);
        bson_destroy(filter);
        bson_destroy(update);

        if (rc < 0) {
            printf("   FAILED at iteration %d: %s\n", i, error.message);
            break;
        }
        
    }
    mongolite_close(db);
    cleanup();
    return 0;
}
