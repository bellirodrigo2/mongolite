// test_mongolite_update.c - Tests for update operations (cmocka)

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <stdlib.h>

#include "mongolite_internal.h"

#define TEST_DB_PATH "./test_mongolite_update"

static void cleanup_test_db(void) {
    system("rm -rf " TEST_DB_PATH);
}

static mongolite_db_t* setup_test_db(void) {
    cleanup_test_db();
    
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    if (mongolite_open(TEST_DB_PATH, &db, &config, &error) != 0) {
        return NULL;
    }
    
    if (mongolite_collection_create(db, "users", NULL, &error) != 0) {
        mongolite_close(db);
        return NULL;
    }
    
    return db;
}

static int teardown(void **state) {
    (void)state;
    cleanup_test_db();
    return 0;
}

static void test_set_operator(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users", "{\"name\": \"Alice\", \"age\": 30}", &id, &error);
    assert_int_equal(0, rc);
    
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);
    
    bson_t *update = bson_new();
    bson_t set_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
    BSON_APPEND_INT32(&set_doc, "age", 31);
    BSON_APPEND_UTF8(&set_doc, "city", "NYC");
    bson_append_document_end(update, &set_doc);
    
    rc = mongolite_update_one(db, "users", filter, update, false, &error);
    assert_int_equal(0, rc);
    
    bson_destroy(update);
    
    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(found);
    
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found, "age"));
    assert_int_equal(31, bson_iter_int32(&iter));
    
    assert_true(bson_iter_init_find(&iter, found, "city"));
    assert_string_equal("NYC", bson_iter_utf8(&iter, NULL));
    
    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

static void test_unset_operator(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users", "{\"name\": \"Bob\", \"age\": 25, \"city\": \"LA\"}", &id, &error);
    assert_int_equal(0, rc);
    
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);
    
    bson_t *update = bson_new();
    bson_t unset_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$unset", &unset_doc);
    BSON_APPEND_INT32(&unset_doc, "city", 1);
    bson_append_document_end(update, &unset_doc);
    
    rc = mongolite_update_one(db, "users", filter, update, false, &error);
    assert_int_equal(0, rc);
    
    bson_destroy(update);
    
    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(found);
    
    bson_iter_t iter;
    assert_false(bson_iter_init_find(&iter, found, "city"));
    assert_true(bson_iter_init_find(&iter, found, "name"));
    assert_true(bson_iter_init_find(&iter, found, "age"));
    
    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

static void test_inc_operator(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users", "{\"name\": \"Charlie\", \"age\": 35, \"score\": 100}", &id, &error);
    assert_int_equal(0, rc);
    
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);
    
    bson_t *update = bson_new();
    bson_t inc_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$inc", &inc_doc);
    BSON_APPEND_INT32(&inc_doc, "age", 1);
    BSON_APPEND_INT32(&inc_doc, "score", 50);
    bson_append_document_end(update, &inc_doc);
    
    rc = mongolite_update_one(db, "users", filter, update, false, &error);
    assert_int_equal(0, rc);
    
    bson_destroy(update);
    
    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(found);
    
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found, "age"));
    assert_int_equal(36, bson_iter_int32(&iter));
    
    assert_true(bson_iter_init_find(&iter, found, "score"));
    assert_int_equal(150, bson_iter_int32(&iter));
    
    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

static void test_push_operator(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users", "{\"name\": \"Diana\", \"tags\": [\"developer\", \"admin\"]}", &id, &error);
    assert_int_equal(0, rc);
    
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);
    
    bson_t *update = bson_new();
    bson_t push_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$push", &push_doc);
    BSON_APPEND_UTF8(&push_doc, "tags", "user");
    bson_append_document_end(update, &push_doc);
    
    rc = mongolite_update_one(db, "users", filter, update, false, &error);
    assert_int_equal(0, rc);
    
    bson_destroy(update);
    
    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(found);
    
    bson_iter_t iter, array_iter;
    assert_true(bson_iter_init_find(&iter, found, "tags"));
    assert_true(BSON_ITER_HOLDS_ARRAY(&iter));
    
    bson_iter_recurse(&iter, &array_iter);
    int count = 0;
    while (bson_iter_next(&array_iter)) count++;
    assert_int_equal(3, count);
    
    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

static void test_pull_operator(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users", "{\"name\": \"Eve\", \"tags\": [\"developer\", \"admin\", \"user\"]}", &id, &error);
    assert_int_equal(0, rc);
    
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);
    
    bson_t *update = bson_new();
    bson_t pull_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$pull", &pull_doc);
    BSON_APPEND_UTF8(&pull_doc, "tags", "admin");
    bson_append_document_end(update, &pull_doc);
    
    rc = mongolite_update_one(db, "users", filter, update, false, &error);
    assert_int_equal(0, rc);
    
    bson_destroy(update);
    
    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(found);
    
    bson_iter_t iter, array_iter;
    assert_true(bson_iter_init_find(&iter, found, "tags"));
    
    bson_iter_recurse(&iter, &array_iter);
    int count = 0;
    bool has_admin = false;
    while (bson_iter_next(&array_iter)) {
        count++;
        if (strcmp(bson_iter_utf8(&array_iter, NULL), "admin") == 0) {
            has_admin = true;
        }
    }
    
    assert_int_equal(2, count);
    assert_false(has_admin);
    
    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

static void test_rename_operator(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users", "{\"name\": \"Frank\", \"old_field\": \"test_value\"}", &id, &error);
    assert_int_equal(0, rc);
    
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);
    
    bson_t *update = bson_new();
    bson_t rename_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$rename", &rename_doc);
    BSON_APPEND_UTF8(&rename_doc, "old_field", "new_field");
    bson_append_document_end(update, &rename_doc);
    
    rc = mongolite_update_one(db, "users", filter, update, false, &error);
    assert_int_equal(0, rc);
    
    bson_destroy(update);
    
    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(found);
    
    bson_iter_t iter;
    assert_false(bson_iter_init_find(&iter, found, "old_field"));
    assert_true(bson_iter_init_find(&iter, found, "new_field"));
    assert_string_equal("test_value", bson_iter_utf8(&iter, NULL));
    
    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

static void test_update_one(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    for (int i = 0; i < 3; i++) {
        int rc = mongolite_insert_one_json(db, "users", "{\"name\": \"User\", \"value\": 10}", NULL, &error);
        assert_int_equal(0, rc);
    }
    
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "name", "User");
    
    bson_t *update = bson_new();
    bson_t set_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
    BSON_APPEND_INT32(&set_doc, "value", 99);
    bson_append_document_end(update, &set_doc);
    
    int rc = mongolite_update_one(db, "users", filter, update, false, &error);
    assert_int_equal(0, rc);
    
    bson_destroy(filter);
    bson_destroy(update);
    
    filter = bson_new();
    BSON_APPEND_INT32(filter, "value", 99);
    
    int64_t count = mongolite_collection_count(db, "users", filter, &error);
    assert_int_equal(1, count);
    
    bson_destroy(filter);
    mongolite_close(db);
}

static void test_update_many(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    for (int i = 0; i < 5; i++) {
        int rc = mongolite_insert_one_json(db, "users", "{\"category\": \"test\", \"value\": 10}", NULL, &error);
        assert_int_equal(0, rc);
    }
    
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "category", "test");
    
    bson_t *update = bson_new();
    bson_t inc_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$inc", &inc_doc);
    BSON_APPEND_INT32(&inc_doc, "value", 5);
    bson_append_document_end(update, &inc_doc);
    
    int64_t modified_count = 0;
    int rc = mongolite_update_many(db, "users", filter, update, false, &modified_count, &error);
    assert_int_equal(0, rc);
    assert_int_equal(5, modified_count);
    
    bson_destroy(filter);
    bson_destroy(update);
    
    filter = bson_new();
    BSON_APPEND_INT32(filter, "value", 15);
    
    int64_t count = mongolite_collection_count(db, "users", filter, &error);
    assert_int_equal(5, count);
    
    bson_destroy(filter);
    mongolite_close(db);
}

static void test_replace_one(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users", "{\"name\": \"Grace\", \"age\": 27, \"city\": \"NYC\"}", &id, &error);
    assert_int_equal(0, rc);
    
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);
    
    bson_t *replacement = bson_new();
    BSON_APPEND_UTF8(replacement, "name", "Grace Updated");
    BSON_APPEND_INT32(replacement, "status", 1);
    
    rc = mongolite_replace_one(db, "users", filter, replacement, false, &error);
    assert_int_equal(0, rc);
    
    bson_destroy(replacement);
    
    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(found);
    
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found, "name"));
    assert_string_equal("Grace Updated", bson_iter_utf8(&iter, NULL));
    
    assert_true(bson_iter_init_find(&iter, found, "status"));
    assert_int_equal(1, bson_iter_int32(&iter));
    
    assert_false(bson_iter_init_find(&iter, found, "age"));
    assert_false(bson_iter_init_find(&iter, found, "city"));
    
    assert_true(bson_iter_init_find(&iter, found, "_id"));
    bson_oid_t found_id;
    bson_oid_copy(bson_iter_oid(&iter), &found_id);
    assert_true(bson_oid_equal(&id, &found_id));
    
    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

static void test_json_wrappers(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users", "{\"name\": \"Henry\", \"age\": 45}", &id, &error);
    assert_int_equal(0, rc);
    
    char oid_str[25];
    bson_oid_to_string(&id, oid_str);
    
    char filter_json[256];
    snprintf(filter_json, sizeof(filter_json), "{\"_id\": {\"$oid\": \"%s\"}}", oid_str);
    
    rc = mongolite_update_one_json(db, "users", filter_json, "{\"$set\": {\"age\": 46}}", false, &error);
    assert_int_equal(0, rc);
    
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);
    
    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(found);
    
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found, "age"));
    assert_int_equal(46, bson_iter_int32(&iter));
    
    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

static void test_combined_operators(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users", "{\"name\": \"Test\", \"age\": 30, \"score\": 100, \"old_field\": \"x\"}", &id, &error);
    assert_int_equal(0, rc);

    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *update = bson_new();

    bson_t set_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
    BSON_APPEND_UTF8(&set_doc, "name", "Test Updated");
    bson_append_document_end(update, &set_doc);

    bson_t inc_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$inc", &inc_doc);
    BSON_APPEND_INT32(&inc_doc, "age", 1);
    BSON_APPEND_INT32(&inc_doc, "score", 50);
    bson_append_document_end(update, &inc_doc);

    bson_t unset_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$unset", &unset_doc);
    BSON_APPEND_INT32(&unset_doc, "old_field", 1);
    bson_append_document_end(update, &unset_doc);

    rc = mongolite_update_one(db, "users", filter, update, false, &error);
    assert_int_equal(0, rc);

    bson_destroy(update);

    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(found);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found, "name"));
    assert_string_equal("Test Updated", bson_iter_utf8(&iter, NULL));

    assert_true(bson_iter_init_find(&iter, found, "age"));
    assert_int_equal(31, bson_iter_int32(&iter));

    assert_true(bson_iter_init_find(&iter, found, "score"));
    assert_int_equal(150, bson_iter_int32(&iter));

    assert_false(bson_iter_init_find(&iter, found, "old_field"));

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

static void test_inc_double(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users", "{\"name\": \"Test\", \"score\": 100.0}", &id, &error);
    assert_int_equal(0, rc);

    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *update = bson_new();
    bson_t inc_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$inc", &inc_doc);
    BSON_APPEND_DOUBLE(&inc_doc, "score", 0.5);
    bson_append_document_end(update, &inc_doc);

    rc = mongolite_update_one(db, "users", filter, update, false, &error);
    assert_int_equal(0, rc);

    bson_destroy(update);

    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(found);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found, "score"));
    double score = bson_iter_double(&iter);
    assert_true(score > 100.4 && score < 100.6);

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

static void test_repeated_updates(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users",
        "{\"name\": \"Test\", \"age\": 30, \"score\": 100.0, \"active\": false}", &id, &error);
    assert_int_equal(0, rc);

    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    // Perform 100 repeated updates with combined $set + $inc
    const int N = 100;
    for (int i = 0; i < N; i++) {
        bson_t *update = bson_new();

        bson_t set_doc;
        BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
        BSON_APPEND_BOOL(&set_doc, "active", (i % 2) == 0);
        bson_append_document_end(update, &set_doc);

        bson_t inc_doc;
        BSON_APPEND_DOCUMENT_BEGIN(update, "$inc", &inc_doc);
        BSON_APPEND_INT32(&inc_doc, "age", 1);
        BSON_APPEND_DOUBLE(&inc_doc, "score", 0.5);
        bson_append_document_end(update, &inc_doc);

        rc = mongolite_update_one(db, "users", filter, update, false, &error);
        bson_destroy(update);
        assert_int_equal(0, rc);
    }

    // Verify final values
    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(found);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found, "age"));
    assert_int_equal(30 + N, bson_iter_int32(&iter));

    assert_true(bson_iter_init_find(&iter, found, "score"));
    double expected_score = 100.0 + (N * 0.5);
    double actual_score = bson_iter_double(&iter);
    assert_true(actual_score > expected_score - 0.1 && actual_score < expected_score + 0.1);

    assert_true(bson_iter_init_find(&iter, found, "active"));
    // N=100 is even, last iteration i=99, (99 % 2) == 1, so active = false
    assert_false(bson_iter_bool(&iter));

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

/* ============================================================
 * Upsert Tests
 * ============================================================ */

static void test_upsert_update_one_insert(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    // Upsert when no document matches - should insert
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "email", "new@example.com");

    bson_t *update = bson_new();
    bson_t set_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
    BSON_APPEND_UTF8(&set_doc, "name", "New User");
    BSON_APPEND_INT32(&set_doc, "age", 25);
    bson_append_document_end(update, &set_doc);

    int rc = mongolite_update_one(db, "users", filter, update, true, &error);
    assert_int_equal(0, rc);

    bson_destroy(update);

    // Verify document was inserted with email from filter
    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(found);

    bson_iter_t iter;
    // Should have email from filter
    assert_true(bson_iter_init_find(&iter, found, "email"));
    assert_string_equal("new@example.com", bson_iter_utf8(&iter, NULL));
    // Should have name from update
    assert_true(bson_iter_init_find(&iter, found, "name"));
    assert_string_equal("New User", bson_iter_utf8(&iter, NULL));
    // Should have age from update
    assert_true(bson_iter_init_find(&iter, found, "age"));
    assert_int_equal(25, bson_iter_int32(&iter));
    // Should have auto-generated _id
    assert_true(bson_iter_init_find(&iter, found, "_id"));

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

static void test_upsert_update_one_update(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    // Insert a document first
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users", "{\"email\": \"existing@example.com\", \"name\": \"Existing\"}", &id, &error);
    assert_int_equal(0, rc);

    // Upsert with matching filter - should update, not insert
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "email", "existing@example.com");

    bson_t *update = bson_new();
    bson_t set_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
    BSON_APPEND_UTF8(&set_doc, "name", "Updated Name");
    bson_append_document_end(update, &set_doc);

    rc = mongolite_update_one(db, "users", filter, update, true, &error);
    assert_int_equal(0, rc);

    bson_destroy(update);

    // Should still have only 1 document
    int64_t count = mongolite_collection_count(db, "users", NULL, &error);
    assert_int_equal(1, count);

    // Verify it was updated
    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(found);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found, "name"));
    assert_string_equal("Updated Name", bson_iter_utf8(&iter, NULL));

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

static void test_upsert_update_many_insert(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    // Upsert with update_many when no documents match
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "category", "nonexistent");

    bson_t *update = bson_new();
    bson_t set_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
    BSON_APPEND_UTF8(&set_doc, "status", "active");
    BSON_APPEND_INT32(&set_doc, "count", 0);
    bson_append_document_end(update, &set_doc);

    int64_t modified_count = 0;
    int rc = mongolite_update_many(db, "users", filter, update, true, &modified_count, &error);
    assert_int_equal(0, rc);
    assert_int_equal(1, modified_count);  // One document inserted

    bson_destroy(update);

    // Verify document was inserted
    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(found);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found, "category"));
    assert_string_equal("nonexistent", bson_iter_utf8(&iter, NULL));
    assert_true(bson_iter_init_find(&iter, found, "status"));
    assert_string_equal("active", bson_iter_utf8(&iter, NULL));

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

static void test_upsert_replace_one_insert(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    // Upsert with replace_one when no document matches
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "username", "newuser");

    bson_t *replacement = bson_new();
    BSON_APPEND_UTF8(replacement, "name", "New User");
    BSON_APPEND_INT32(replacement, "level", 1);

    int rc = mongolite_replace_one(db, "users", filter, replacement, true, &error);
    assert_int_equal(0, rc);

    bson_destroy(replacement);

    // Verify document was inserted with username from filter
    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(found);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found, "username"));
    assert_string_equal("newuser", bson_iter_utf8(&iter, NULL));
    assert_true(bson_iter_init_find(&iter, found, "name"));
    assert_string_equal("New User", bson_iter_utf8(&iter, NULL));
    assert_true(bson_iter_init_find(&iter, found, "level"));
    assert_int_equal(1, bson_iter_int32(&iter));

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

static void test_upsert_with_id_in_filter(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    // Upsert with specific _id in filter
    bson_oid_t specified_id;
    bson_oid_init(&specified_id, NULL);

    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &specified_id);

    bson_t *update = bson_new();
    bson_t set_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
    BSON_APPEND_UTF8(&set_doc, "name", "With Specific ID");
    bson_append_document_end(update, &set_doc);

    int rc = mongolite_update_one(db, "users", filter, update, true, &error);
    assert_int_equal(0, rc);

    bson_destroy(update);

    // Verify document has the specified _id
    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(found);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found, "_id"));
    assert_true(bson_oid_equal(&specified_id, bson_iter_oid(&iter)));

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

static void test_upsert_with_empty_filter(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    // Upsert with empty filter (should create empty base doc)
    bson_t *filter = bson_new();  // Empty filter

    bson_t *update = bson_new();
    bson_t set_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
    BSON_APPEND_UTF8(&set_doc, "name", "From Empty Filter");
    bson_append_document_end(update, &set_doc);

    int rc = mongolite_update_one(db, "users", filter, update, true, &error);
    assert_int_equal(0, rc);

    bson_destroy(update);
    bson_destroy(filter);

    // Verify document was created
    int64_t count = mongolite_collection_count(db, "users", NULL, &error);
    assert_int_equal(1, count);

    mongolite_close(db);
}

static void test_upsert_filter_with_operators(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    // Upsert with filter containing operators (operators should NOT be in base doc)
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "name", "Test");  // Equality - should be in base
    bson_t gt_doc;
    BSON_APPEND_DOCUMENT_BEGIN(filter, "age", &gt_doc);
    BSON_APPEND_INT32(&gt_doc, "$gt", 18);
    bson_append_document_end(filter, &gt_doc);  // Operator - should NOT be in base

    bson_t *update = bson_new();
    bson_t set_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
    BSON_APPEND_INT32(&set_doc, "score", 100);
    bson_append_document_end(update, &set_doc);

    int rc = mongolite_update_one(db, "users", filter, update, true, &error);
    assert_int_equal(0, rc);

    bson_destroy(filter);
    bson_destroy(update);

    // Verify: should have name but NOT age (since age was an operator condition)
    filter = bson_new();
    BSON_APPEND_UTF8(filter, "name", "Test");
    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(found);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found, "name"));
    assert_string_equal("Test", bson_iter_utf8(&iter, NULL));
    assert_false(bson_iter_init_find(&iter, found, "age"));  // Should NOT have age
    assert_true(bson_iter_init_find(&iter, found, "score"));
    assert_int_equal(100, bson_iter_int32(&iter));

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

/* ============================================================
 * Edge Case Tests
 * ============================================================ */

static void test_update_no_match_no_upsert(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    // Update with no matching document and upsert=false
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "email", "nonexistent@example.com");

    bson_t *update = bson_new();
    bson_t set_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
    BSON_APPEND_UTF8(&set_doc, "name", "Should Not Exist");
    bson_append_document_end(update, &set_doc);

    int rc = mongolite_update_one(db, "users", filter, update, false, &error);
    assert_int_equal(0, rc);  // Should succeed (no-op)

    bson_destroy(update);

    // Verify no document was created
    int64_t count = mongolite_collection_count(db, "users", NULL, &error);
    assert_int_equal(0, count);

    bson_destroy(filter);
    mongolite_close(db);
}

static void test_replace_one_invalid_operators(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    // Insert a document
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users", "{\"name\": \"Test\"}", &id, &error);
    assert_int_equal(0, rc);

    // Try to replace with a document containing operators - should fail
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);

    bson_t *replacement = bson_new();
    bson_t set_doc;
    BSON_APPEND_DOCUMENT_BEGIN(replacement, "$set", &set_doc);  // Invalid!
    BSON_APPEND_UTF8(&set_doc, "name", "Bad");
    bson_append_document_end(replacement, &set_doc);

    rc = mongolite_replace_one(db, "users", filter, replacement, false, &error);
    assert_int_not_equal(0, rc);  // Should fail

    bson_destroy(filter);
    bson_destroy(replacement);
    mongolite_close(db);
}

static void test_update_many_no_match_no_upsert(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    // Insert some documents
    for (int i = 0; i < 3; i++) {
        int rc = mongolite_insert_one_json(db, "users", "{\"category\": \"A\"}", NULL, &error);
        assert_int_equal(0, rc);
    }

    // Update with non-matching filter, no upsert
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "category", "Z");

    bson_t *update = bson_new();
    bson_t set_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
    BSON_APPEND_INT32(&set_doc, "value", 999);
    bson_append_document_end(update, &set_doc);

    int64_t modified_count = -1;
    int rc = mongolite_update_many(db, "users", filter, update, false, &modified_count, &error);
    assert_int_equal(0, rc);
    assert_int_equal(0, modified_count);  // No documents modified

    bson_destroy(filter);
    bson_destroy(update);
    mongolite_close(db);
}

static void test_replace_one_no_match_no_upsert(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    // Replace with no matching document and upsert=false
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "name", "nonexistent");

    bson_t *replacement = bson_new();
    BSON_APPEND_UTF8(replacement, "name", "Should Not Exist");

    int rc = mongolite_replace_one(db, "users", filter, replacement, false, &error);
    assert_int_equal(0, rc);  // Should succeed (no-op)

    // Verify no document was created
    int64_t count = mongolite_collection_count(db, "users", NULL, &error);
    assert_int_equal(0, count);

    bson_destroy(filter);
    bson_destroy(replacement);
    mongolite_close(db);
}

static void test_update_with_null_filter(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    // Insert documents
    for (int i = 0; i < 3; i++) {
        char json[64];
        snprintf(json, sizeof(json), "{\"value\": %d}", i);
        int rc = mongolite_insert_one_json(db, "users", json, NULL, &error);
        assert_int_equal(0, rc);
    }

    // Update with NULL filter (should match first document)
    bson_t *update = bson_new();
    bson_t set_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
    BSON_APPEND_INT32(&set_doc, "updated", 1);
    bson_append_document_end(update, &set_doc);

    int rc = mongolite_update_one(db, "users", NULL, update, false, &error);
    assert_int_equal(0, rc);

    bson_destroy(update);

    // Count documents with "updated" field
    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "updated", 1);
    int64_t count = mongolite_collection_count(db, "users", filter, &error);
    assert_int_equal(1, count);

    bson_destroy(filter);
    mongolite_close(db);
}

static void test_update_many_with_null_filter(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    // Insert documents
    for (int i = 0; i < 5; i++) {
        char json[64];
        snprintf(json, sizeof(json), "{\"value\": %d}", i);
        int rc = mongolite_insert_one_json(db, "users", json, NULL, &error);
        assert_int_equal(0, rc);
    }

    // Update many with NULL filter (should match all)
    bson_t *update = bson_new();
    bson_t set_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
    BSON_APPEND_INT32(&set_doc, "updated", 1);
    bson_append_document_end(update, &set_doc);

    int64_t modified_count = 0;
    int rc = mongolite_update_many(db, "users", NULL, update, false, &modified_count, &error);
    assert_int_equal(0, rc);
    assert_int_equal(5, modified_count);

    bson_destroy(update);
    mongolite_close(db);
}

static void test_upsert_replace_with_id_in_replacement(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    // Upsert replace with _id in the replacement document
    bson_oid_t custom_id;
    bson_oid_init(&custom_id, NULL);

    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "name", "test");

    bson_t *replacement = bson_new();
    BSON_APPEND_OID(replacement, "_id", &custom_id);
    BSON_APPEND_UTF8(replacement, "name", "With Custom ID");

    int rc = mongolite_replace_one(db, "users", filter, replacement, true, &error);
    assert_int_equal(0, rc);

    bson_destroy(filter);
    bson_destroy(replacement);

    // Verify the custom _id was used
    filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &custom_id);
    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(found);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found, "_id"));
    assert_true(bson_oid_equal(&custom_id, bson_iter_oid(&iter)));

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

static void test_update_many_json_wrapper(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    // Insert documents
    for (int i = 0; i < 3; i++) {
        int rc = mongolite_insert_one_json(db, "users", "{\"type\": \"test\"}", NULL, &error);
        assert_int_equal(0, rc);
    }

    // Use JSON wrapper
    int64_t modified = 0;
    int rc = mongolite_update_many_json(db, "users",
        "{\"type\": \"test\"}",
        "{\"$set\": {\"updated\": true}}",
        false, &modified, &error);
    assert_int_equal(0, rc);
    assert_int_equal(3, modified);

    mongolite_close(db);
}

static void test_update_many_large_batch(void **state) {
    (void)state;
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 64ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);

    rc = mongolite_collection_create(db, "batch", NULL, &error);
    assert_int_equal(0, rc);

    // Insert more than initial capacity (16) to test array growth
    const int NUM_DOCS = 50;
    for (int i = 0; i < NUM_DOCS; i++) {
        bson_t *doc = bson_new();
        BSON_APPEND_INT32(doc, "batch", 1);
        BSON_APPEND_INT32(doc, "value", i);
        rc = mongolite_insert_one(db, "batch", doc, NULL, &error);
        assert_int_equal(0, rc);
        bson_destroy(doc);
    }

    // Update all documents with batch=1 - this forces array growth in update_many
    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "batch", 1);

    bson_t *update = bson_new();
    bson_t set_doc;
    BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
    BSON_APPEND_UTF8(&set_doc, "status", "updated");
    bson_append_document_end(update, &set_doc);

    int64_t modified_count = 0;
    rc = mongolite_update_many(db, "batch", filter, update, false, &modified_count, &error);
    assert_int_equal(0, rc);
    assert_int_equal(NUM_DOCS, modified_count);

    bson_destroy(filter);
    bson_destroy(update);

    // Verify all were updated
    filter = bson_new();
    BSON_APPEND_UTF8(filter, "status", "updated");
    int64_t count = mongolite_collection_count(db, "batch", filter, &error);
    assert_int_equal(NUM_DOCS, count);

    bson_destroy(filter);
    mongolite_close(db);
}

static void test_replace_one_json_wrapper(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    // Insert a document
    bson_oid_t id;
    int rc = mongolite_insert_one_json(db, "users", "{\"name\": \"Original\"}", &id, &error);
    assert_int_equal(0, rc);

    // Use JSON wrapper for replace
    char oid_str[25];
    bson_oid_to_string(&id, oid_str);
    char filter_json[256];
    snprintf(filter_json, sizeof(filter_json), "{\"_id\": {\"$oid\": \"%s\"}}", oid_str);

    rc = mongolite_replace_one_json(db, "users", filter_json,
        "{\"name\": \"Replaced\", \"status\": \"done\"}", false, &error);
    assert_int_equal(0, rc);

    // Verify
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &id);
    bson_t *found = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(found);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found, "name"));
    assert_string_equal("Replaced", bson_iter_utf8(&iter, NULL));

    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_teardown(test_set_operator, teardown),
        cmocka_unit_test_teardown(test_unset_operator, teardown),
        cmocka_unit_test_teardown(test_inc_operator, teardown),
        cmocka_unit_test_teardown(test_push_operator, teardown),
        cmocka_unit_test_teardown(test_pull_operator, teardown),
        cmocka_unit_test_teardown(test_rename_operator, teardown),
        cmocka_unit_test_teardown(test_update_one, teardown),
        cmocka_unit_test_teardown(test_update_many, teardown),
        cmocka_unit_test_teardown(test_replace_one, teardown),
        cmocka_unit_test_teardown(test_json_wrappers, teardown),
        cmocka_unit_test_teardown(test_combined_operators, teardown),
        cmocka_unit_test_teardown(test_inc_double, teardown),
        cmocka_unit_test_teardown(test_repeated_updates, teardown),
        /* Upsert tests */
        cmocka_unit_test_teardown(test_upsert_update_one_insert, teardown),
        cmocka_unit_test_teardown(test_upsert_update_one_update, teardown),
        cmocka_unit_test_teardown(test_upsert_update_many_insert, teardown),
        cmocka_unit_test_teardown(test_upsert_replace_one_insert, teardown),
        cmocka_unit_test_teardown(test_upsert_with_id_in_filter, teardown),
        cmocka_unit_test_teardown(test_upsert_with_empty_filter, teardown),
        cmocka_unit_test_teardown(test_upsert_filter_with_operators, teardown),
        /* Edge case tests */
        cmocka_unit_test_teardown(test_update_no_match_no_upsert, teardown),
        cmocka_unit_test_teardown(test_replace_one_invalid_operators, teardown),
        cmocka_unit_test_teardown(test_update_many_no_match_no_upsert, teardown),
        cmocka_unit_test_teardown(test_replace_one_no_match_no_upsert, teardown),
        cmocka_unit_test_teardown(test_update_with_null_filter, teardown),
        cmocka_unit_test_teardown(test_update_many_with_null_filter, teardown),
        cmocka_unit_test_teardown(test_upsert_replace_with_id_in_replacement, teardown),
        cmocka_unit_test_teardown(test_update_many_json_wrapper, teardown),
        cmocka_unit_test_teardown(test_replace_one_json_wrapper, teardown),
        cmocka_unit_test_teardown(test_update_many_large_batch, teardown),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
