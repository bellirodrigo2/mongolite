// test_mongolite_find.c - Tests for find operations (cmocka)

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <stdlib.h>

#include "mongolite_internal.h"

#define TEST_DB_PATH "./test_mongolite_find"

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
    
    const char *users[] = {
        "{\"name\": \"Alice\", \"age\": 30, \"city\": \"NYC\"}",
        "{\"name\": \"Bob\", \"age\": 25, \"city\": \"LA\"}",
        "{\"name\": \"Charlie\", \"age\": 35, \"city\": \"NYC\"}",
        "{\"name\": \"Diana\", \"age\": 28, \"city\": \"Chicago\"}",
        "{\"name\": \"Eve\", \"age\": 30, \"city\": \"LA\"}"
    };
    
    for (int i = 0; i < 5; i++) {
        if (mongolite_insert_one_json(db, "users", users[i], NULL, &error) != 0) {
            mongolite_close(db);
            return NULL;
        }
    }
    
    return db;
}

static int teardown(void **state) {
    (void)state;
    cleanup_test_db();
    return 0;
}

static void test_find_one_no_filter(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    bson_t *doc = mongolite_find_one(db, "users", NULL, NULL, &error);
    assert_non_null(doc);
    
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, doc, "name"));
    
    bson_destroy(doc);
    mongolite_close(db);
}

static void test_find_one_by_id(void **state) {
    (void)state;
    cleanup_test_db();
    
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "items", NULL, &error);
    assert_int_equal(0, rc);
    
    bson_oid_t my_id;
    bson_oid_init(&my_id, NULL);
    
    bson_t *insert_doc = bson_new();
    BSON_APPEND_OID(insert_doc, "_id", &my_id);
    BSON_APPEND_UTF8(insert_doc, "value", "test_value");
    
    rc = mongolite_insert_one(db, "items", insert_doc, NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(insert_doc);
    
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &my_id);
    
    bson_t *found = mongolite_find_one(db, "items", filter, NULL, &error);
    assert_non_null(found);
    
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found, "value"));
    assert_string_equal("test_value", bson_iter_utf8(&iter, NULL));
    
    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

static void test_find_one_with_filter(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "age", 35);
    
    bson_t *doc = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(doc);
    
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, doc, "name"));
    assert_string_equal("Charlie", bson_iter_utf8(&iter, NULL));
    
    bson_destroy(filter);
    bson_destroy(doc);
    mongolite_close(db);
}

static void test_find_one_complex_filter(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "city", "NYC");
    BSON_APPEND_INT32(filter, "age", 30);
    
    bson_t *doc = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_non_null(doc);
    
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, doc, "name"));
    assert_string_equal("Alice", bson_iter_utf8(&iter, NULL));
    
    bson_destroy(filter);
    bson_destroy(doc);
    mongolite_close(db);
}

static void test_find_one_not_found(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "age", 99);
    
    bson_t *doc = mongolite_find_one(db, "users", filter, NULL, &error);
    assert_null(doc);
    
    bson_destroy(filter);
    mongolite_close(db);
}

static void test_find_cursor_all(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    mongolite_cursor_t *cursor = mongolite_find(db, "users", NULL, NULL, &error);
    assert_non_null(cursor);
    
    int count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }
    
    assert_int_equal(5, count);
    
    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

static void test_find_cursor_filtered(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "age", 30);
    
    mongolite_cursor_t *cursor = mongolite_find(db, "users", filter, NULL, &error);
    assert_non_null(cursor);
    
    int count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }
    
    assert_int_equal(2, count);
    
    bson_destroy(filter);
    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

static void test_cursor_limit(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    mongolite_cursor_t *cursor = mongolite_find(db, "users", NULL, NULL, &error);
    assert_non_null(cursor);
    
    int rc = mongolite_cursor_set_limit(cursor, 2);
    assert_int_equal(0, rc);
    
    int count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }
    
    assert_int_equal(2, count);
    
    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

static void test_find_one_json(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    const char *result = mongolite_find_one_json(db, "users", "{\"name\": \"Bob\"}", NULL, &error);
    assert_non_null(result);
    assert_non_null(strstr(result, "Bob"));
    
    bson_free((void*)result);
    mongolite_close(db);
}

static void test_find_json_array(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    char **results = mongolite_find_json(db, "users", "{\"city\": \"NYC\"}", NULL, &error);
    assert_non_null(results);
    
    int count = 0;
    for (int i = 0; results[i] != NULL; i++) {
        bson_free(results[i]);
        count++;
    }
    free(results);
    
    assert_int_equal(2, count);
    
    mongolite_close(db);
}

static void test_find_gt_operator(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);
    
    gerror_t error = {0};
    
    bson_t *filter = bson_new();
    bson_t child;
    BSON_APPEND_DOCUMENT_BEGIN(filter, "age", &child);
    BSON_APPEND_INT32(&child, "$gt", 28);
    bson_append_document_end(filter, &child);
    
    mongolite_cursor_t *cursor = mongolite_find(db, "users", filter, NULL, &error);
    assert_non_null(cursor);
    
    int count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }
    
    assert_int_equal(3, count);
    
    bson_destroy(filter);
    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

static void test_insert_find_integrity(void **state) {
    (void)state;
    cleanup_test_db();
    
    mongolite_db_t *db = NULL;
    gerror_t error = {0};
    
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);
    
    rc = mongolite_collection_create(db, "test", NULL, &error);
    assert_int_equal(0, rc);
    
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "string", "hello world");
    BSON_APPEND_INT32(doc, "int32", 42);
    BSON_APPEND_INT64(doc, "int64", 9876543210LL);
    BSON_APPEND_DOUBLE(doc, "double", 3.14159);
    BSON_APPEND_BOOL(doc, "bool_true", true);
    BSON_APPEND_BOOL(doc, "bool_false", false);
    
    bson_oid_t inserted_id;
    rc = mongolite_insert_one(db, "test", doc, &inserted_id, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc);
    
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &inserted_id);
    
    bson_t *found = mongolite_find_one(db, "test", filter, NULL, &error);
    assert_non_null(found);
    
    bson_iter_t iter;
    
    assert_true(bson_iter_init_find(&iter, found, "string"));
    assert_string_equal("hello world", bson_iter_utf8(&iter, NULL));
    
    assert_true(bson_iter_init_find(&iter, found, "int32"));
    assert_int_equal(42, bson_iter_int32(&iter));
    
    assert_true(bson_iter_init_find(&iter, found, "int64"));
    assert_true(bson_iter_int64(&iter) == 9876543210LL);
    
    assert_true(bson_iter_init_find(&iter, found, "double"));
    assert_true(bson_iter_double(&iter) > 3.14 && bson_iter_double(&iter) < 3.15);
    
    assert_true(bson_iter_init_find(&iter, found, "bool_true"));
    assert_true(bson_iter_bool(&iter) == true);
    
    assert_true(bson_iter_init_find(&iter, found, "bool_false"));
    assert_true(bson_iter_bool(&iter) == false);
    
    bson_destroy(filter);
    bson_destroy(found);
    mongolite_close(db);
}

/* ============================================================
 * Additional Coverage Tests
 * ============================================================ */

static void test_find_null_params(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    /* NULL db */
    bson_t *doc = mongolite_find_one(NULL, "users", NULL, NULL, &error);
    assert_null(doc);
    assert_int_not_equal(0, error.code);

    /* NULL collection */
    error.code = 0;
    doc = mongolite_find_one(db, NULL, NULL, NULL, &error);
    assert_null(doc);
    assert_int_not_equal(0, error.code);

    mongolite_close(db);
}

static void test_find_cursor_null_params(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    /* NULL db */
    mongolite_cursor_t *cursor = mongolite_find(NULL, "users", NULL, NULL, &error);
    assert_null(cursor);

    /* NULL collection */
    error.code = 0;
    cursor = mongolite_find(db, NULL, NULL, NULL, &error);
    assert_null(cursor);

    mongolite_close(db);
}

static void test_find_with_id_not_oid(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    /* Filter with _id that's a string, not OID - should use slow path */
    bson_t *filter = bson_new();
    BSON_APPEND_UTF8(filter, "_id", "string_id");

    bson_t *doc = mongolite_find_one(db, "users", filter, NULL, &error);
    /* Should not find anything since no docs have string _id */
    assert_null(doc);

    bson_destroy(filter);
    mongolite_close(db);
}

static void test_find_with_multi_field_id_filter(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    /* Filter with _id plus other fields - not a simple _id query */
    bson_oid_t oid;
    bson_oid_init(&oid, NULL);

    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &oid);
    BSON_APPEND_UTF8(filter, "name", "Alice");

    /* This should use slow path (filter has more than just _id) */
    bson_t *doc = mongolite_find_one(db, "users", filter, NULL, &error);
    /* Should not find matching doc */
    assert_null(doc);

    bson_destroy(filter);
    mongolite_close(db);
}

static void test_find_one_json_null_params(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    /* NULL db */
    char *json = mongolite_find_one_json(NULL, "users", NULL, NULL, &error);
    assert_null(json);

    /* NULL collection */
    error.code = 0;
    json = mongolite_find_one_json(db, NULL, NULL, NULL, &error);
    assert_null(json);

    mongolite_close(db);
}

static void test_find_one_json_invalid_filter(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    /* Invalid JSON filter */
    char *json = mongolite_find_one_json(db, "users", "{invalid json}", NULL, &error);
    assert_null(json);
    assert_int_not_equal(0, error.code);

    mongolite_close(db);
}

static void test_find_json_array_null_params(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    /* NULL db */
    char *json = mongolite_find_json(NULL, "users", NULL, NULL, &error);
    assert_null(json);

    /* NULL collection */
    error.code = 0;
    json = mongolite_find_json(db, NULL, NULL, NULL, &error);
    assert_null(json);

    mongolite_close(db);
}

static void test_find_json_array_invalid_filter(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    /* Invalid JSON filter */
    char *json = mongolite_find_json(db, "users", "{invalid}", NULL, &error);
    assert_null(json);
    assert_int_not_equal(0, error.code);

    mongolite_close(db);
}

static void test_find_with_projection(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    /* Find with projection - projection is stored but not yet applied (TODO in code) */
    bson_t *projection = bson_new();
    BSON_APPEND_INT32(projection, "name", 1);

    bson_t *doc = mongolite_find_one(db, "users", NULL, projection, &error);
    assert_non_null(doc);

    bson_iter_t iter;
    /* name should be present */
    assert_true(bson_iter_init_find(&iter, doc, "name"));
    /* Note: projection not yet implemented, so all fields are returned */

    bson_destroy(projection);
    bson_destroy(doc);
    mongolite_close(db);
}

static void test_find_cursor_with_projection(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    /* Projection is stored in cursor but not yet applied (TODO in code) */
    bson_t *projection = bson_new();
    BSON_APPEND_INT32(projection, "city", 1);

    mongolite_cursor_t *cursor = mongolite_find(db, "users", NULL, projection, &error);
    assert_non_null(cursor);

    const bson_t *doc;
    int count = 0;
    while (mongolite_cursor_next(cursor, &doc)) {
        bson_iter_t iter;
        /* city should be present */
        assert_true(bson_iter_init_find(&iter, doc, "city"));
        /* Note: projection not yet implemented, name is also present */
        count++;
    }

    assert_int_equal(5, count);

    bson_destroy(projection);
    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

static void test_find_empty_collection(void **state) {
    (void)state;
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);

    rc = mongolite_collection_create(db, "empty", NULL, &error);
    assert_int_equal(0, rc);

    /* Find one in empty collection */
    bson_t *doc = mongolite_find_one(db, "empty", NULL, NULL, &error);
    assert_null(doc);  /* No documents */

    /* Find cursor in empty collection */
    mongolite_cursor_t *cursor = mongolite_find(db, "empty", NULL, NULL, &error);
    assert_non_null(cursor);

    const bson_t *cursor_doc;
    assert_false(mongolite_cursor_next(cursor, &cursor_doc));

    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

static void test_find_nonexistent_collection(void **state) {
    (void)state;
    cleanup_test_db();

    mongolite_db_t *db = NULL;
    gerror_t error = {0};

    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;
    int rc = mongolite_open(TEST_DB_PATH, &db, &config, &error);
    assert_int_equal(0, rc);

    /* Find in nonexistent collection */
    bson_t *doc = mongolite_find_one(db, "nonexistent", NULL, NULL, &error);
    assert_null(doc);

    mongolite_close(db);
}

/* ============================================================
 * Cursor Skip/Sort Tests (Coverage improvement)
 * ============================================================ */

static void test_cursor_skip(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    mongolite_cursor_t *cursor = mongolite_find(db, "users", NULL, NULL, &error);
    assert_non_null(cursor);

    /* Set skip to 2 - should skip first 2 documents */
    int rc = mongolite_cursor_set_skip(cursor, 2);
    assert_int_equal(0, rc);

    int count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }

    /* 5 total docs - 2 skipped = 3 returned */
    assert_int_equal(3, count);

    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

static void test_cursor_skip_null(void **state) {
    (void)state;

    /* Skip with NULL cursor should return error */
    int rc = mongolite_cursor_set_skip(NULL, 2);
    assert_int_equal(MONGOLITE_EINVAL, rc);
}

static void test_cursor_sort(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    mongolite_cursor_t *cursor = mongolite_find(db, "users", NULL, NULL, &error);
    assert_non_null(cursor);

    /* Set sort by age ascending */
    bson_t *sort = bson_new();
    BSON_APPEND_INT32(sort, "age", 1);

    int rc = mongolite_cursor_set_sort(cursor, sort);
    assert_int_equal(0, rc);

    /* Note: Sort is stored but not yet implemented in cursor_next */
    int count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }

    assert_int_equal(5, count);

    bson_destroy(sort);
    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

static void test_cursor_sort_null(void **state) {
    (void)state;

    /* Sort with NULL cursor or NULL sort should return error */
    int rc = mongolite_cursor_set_sort(NULL, NULL);
    assert_int_equal(MONGOLITE_EINVAL, rc);
}

static void test_cursor_skip_after_iteration(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    mongolite_cursor_t *cursor = mongolite_find(db, "users", NULL, NULL, &error);
    assert_non_null(cursor);

    /* Iterate once */
    const bson_t *doc;
    mongolite_cursor_next(cursor, &doc);

    /* Try to set skip after iteration started - should fail */
    int rc = mongolite_cursor_set_skip(cursor, 2);
    assert_int_equal(MONGOLITE_ERROR, rc);

    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

static void test_cursor_sort_after_iteration(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    mongolite_cursor_t *cursor = mongolite_find(db, "users", NULL, NULL, &error);
    assert_non_null(cursor);

    /* Iterate once */
    const bson_t *doc;
    mongolite_cursor_next(cursor, &doc);

    /* Try to set sort after iteration started - should fail */
    bson_t *sort = bson_new();
    BSON_APPEND_INT32(sort, "age", 1);

    int rc = mongolite_cursor_set_sort(cursor, sort);
    assert_int_equal(MONGOLITE_ERROR, rc);

    bson_destroy(sort);
    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

static void test_cursor_limit_null(void **state) {
    (void)state;

    /* Limit with NULL cursor should return error */
    int rc = mongolite_cursor_set_limit(NULL, 5);
    assert_int_equal(MONGOLITE_EINVAL, rc);
}

static void test_cursor_limit_after_iteration(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    mongolite_cursor_t *cursor = mongolite_find(db, "users", NULL, NULL, &error);
    assert_non_null(cursor);

    /* Iterate once */
    const bson_t *doc;
    mongolite_cursor_next(cursor, &doc);

    /* Try to set limit after iteration started - should fail */
    int rc = mongolite_cursor_set_limit(cursor, 2);
    assert_int_equal(MONGOLITE_ERROR, rc);

    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

static void test_cursor_more(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    mongolite_cursor_t *cursor = mongolite_find(db, "users", NULL, NULL, &error);
    assert_non_null(cursor);

    /* Before iteration, cursor_more should be true */
    assert_true(mongolite_cursor_more(cursor));

    /* Iterate through all documents */
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        /* Do nothing */
    }

    /* After exhaustion, cursor_more should be false */
    assert_false(mongolite_cursor_more(cursor));

    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

static void test_cursor_more_null(void **state) {
    (void)state;

    /* cursor_more with NULL should return false */
    assert_false(mongolite_cursor_more(NULL));
}

static void test_cursor_next_exhausted(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    mongolite_cursor_t *cursor = mongolite_find(db, "users", NULL, NULL, &error);
    assert_non_null(cursor);

    /* Set limit to 1 */
    mongolite_cursor_set_limit(cursor, 1);

    /* First call should succeed */
    const bson_t *doc;
    assert_true(mongolite_cursor_next(cursor, &doc));
    assert_non_null(doc);

    /* Second call should fail (limit reached) */
    assert_false(mongolite_cursor_next(cursor, &doc));

    /* Third call on exhausted cursor should still fail */
    assert_false(mongolite_cursor_next(cursor, &doc));

    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

static void test_cursor_skip_and_limit(void **state) {
    (void)state;
    mongolite_db_t *db = setup_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    mongolite_cursor_t *cursor = mongolite_find(db, "users", NULL, NULL, &error);
    assert_non_null(cursor);

    /* Skip 1, limit 2 */
    int rc = mongolite_cursor_set_skip(cursor, 1);
    assert_int_equal(0, rc);
    rc = mongolite_cursor_set_limit(cursor, 2);
    assert_int_equal(0, rc);

    int count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }

    /* Should get 2 documents (skip 1, limit to 2) */
    assert_int_equal(2, count);

    mongolite_cursor_destroy(cursor);
    mongolite_close(db);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_teardown(test_find_one_no_filter, teardown),
        cmocka_unit_test_teardown(test_find_one_by_id, teardown),
        cmocka_unit_test_teardown(test_find_one_with_filter, teardown),
        cmocka_unit_test_teardown(test_find_one_complex_filter, teardown),
        cmocka_unit_test_teardown(test_find_one_not_found, teardown),
        cmocka_unit_test_teardown(test_find_cursor_all, teardown),
        cmocka_unit_test_teardown(test_find_cursor_filtered, teardown),
        cmocka_unit_test_teardown(test_cursor_limit, teardown),
        cmocka_unit_test_teardown(test_find_one_json, teardown),
        cmocka_unit_test_teardown(test_find_json_array, teardown),
        cmocka_unit_test_teardown(test_find_gt_operator, teardown),
        cmocka_unit_test_teardown(test_insert_find_integrity, teardown),
        /* Additional coverage tests */
        cmocka_unit_test_teardown(test_find_null_params, teardown),
        cmocka_unit_test_teardown(test_find_cursor_null_params, teardown),
        cmocka_unit_test_teardown(test_find_with_id_not_oid, teardown),
        cmocka_unit_test_teardown(test_find_with_multi_field_id_filter, teardown),
        cmocka_unit_test_teardown(test_find_one_json_null_params, teardown),
        cmocka_unit_test_teardown(test_find_one_json_invalid_filter, teardown),
        cmocka_unit_test_teardown(test_find_json_array_null_params, teardown),
        cmocka_unit_test_teardown(test_find_json_array_invalid_filter, teardown),
        cmocka_unit_test_teardown(test_find_with_projection, teardown),
        cmocka_unit_test_teardown(test_find_cursor_with_projection, teardown),
        cmocka_unit_test_teardown(test_find_empty_collection, teardown),
        cmocka_unit_test_teardown(test_find_nonexistent_collection, teardown),
        /* Cursor skip/sort/limit coverage tests */
        cmocka_unit_test_teardown(test_cursor_skip, teardown),
        cmocka_unit_test(test_cursor_skip_null),
        cmocka_unit_test_teardown(test_cursor_sort, teardown),
        cmocka_unit_test(test_cursor_sort_null),
        cmocka_unit_test_teardown(test_cursor_skip_after_iteration, teardown),
        cmocka_unit_test_teardown(test_cursor_sort_after_iteration, teardown),
        cmocka_unit_test(test_cursor_limit_null),
        cmocka_unit_test_teardown(test_cursor_limit_after_iteration, teardown),
        cmocka_unit_test_teardown(test_cursor_more, teardown),
        cmocka_unit_test(test_cursor_more_null),
        cmocka_unit_test_teardown(test_cursor_next_exhausted, teardown),
        cmocka_unit_test_teardown(test_cursor_skip_and_limit, teardown),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
