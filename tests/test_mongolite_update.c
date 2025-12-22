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
    };
    
    return cmocka_run_group_tests(tests, NULL, NULL);
}
