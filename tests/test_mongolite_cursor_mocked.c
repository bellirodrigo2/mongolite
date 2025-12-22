/**
 * test_mongolite_cursor_mocked.c - Unit tests for cursor operations with mocking
 *
 * Tests cursor functionality using mock_wtree to:
 * - Test cursor iteration with controlled data
 * - Verify limit/skip behavior
 * - Test edge cases and error paths
 * - Verify resource cleanup
 */

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <stdlib.h>

#include "mock_wtree.h"
#include "mongolite_internal.h"

/* ============================================================
 * Test Setup/Teardown
 * ============================================================ */

static mongolite_db_t *g_db = NULL;

static int setup(void **state) {
    (void)state;
    mock_wtree_reset();

    gerror_t error = {0};
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;

    int rc = mongolite_open("./test_cursor_mocked", &g_db, &config, &error);
    if (rc != 0) {
        return -1;
    }

    /* Create test collection */
    rc = mongolite_collection_create(g_db, "test", NULL, &error);
    if (rc != 0 && rc != MONGOLITE_EEXISTS) {
        mongolite_close(g_db);
        g_db = NULL;
        return -1;
    }

    return 0;
}

static int teardown(void **state) {
    (void)state;
    if (g_db) {
        mongolite_close(g_db);
        g_db = NULL;
    }
    mock_wtree_reset();
    return 0;
}

/* ============================================================
 * Helper: Insert test documents
 * ============================================================ */

static void insert_test_docs(int count) {
    gerror_t error = {0};
    for (int i = 0; i < count; i++) {
        bson_t *doc = bson_new();
        BSON_APPEND_INT32(doc, "index", i);
        BSON_APPEND_UTF8(doc, "name", "test");
        BSON_APPEND_INT32(doc, "value", i * 10);
        mongolite_insert_one(g_db, "test", doc, NULL, &error);
        bson_destroy(doc);
    }
}

/* ============================================================
 * Basic Cursor Tests
 * ============================================================ */

static void test_cursor_empty_collection(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Find on empty collection */
    mongolite_cursor_t *cursor = mongolite_find(g_db, "test", NULL, NULL, &error);
    assert_non_null(cursor);

    const bson_t *doc;
    bool has_next = mongolite_cursor_next(cursor, &doc);
    assert_false(has_next);
    assert_null(doc);

    assert_false(mongolite_cursor_more(cursor));

    mongolite_cursor_destroy(cursor);
}

static void test_cursor_single_doc(void **state) {
    (void)state;
    gerror_t error = {0};

    insert_test_docs(1);

    mongolite_cursor_t *cursor = mongolite_find(g_db, "test", NULL, NULL, &error);
    assert_non_null(cursor);

    const bson_t *doc;
    bool has_next = mongolite_cursor_next(cursor, &doc);
    assert_true(has_next);
    assert_non_null(doc);

    /* Verify document content */
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, doc, "index"));
    assert_int_equal(0, bson_iter_int32(&iter));

    /* No more docs */
    has_next = mongolite_cursor_next(cursor, &doc);
    assert_false(has_next);

    mongolite_cursor_destroy(cursor);
}

static void test_cursor_multiple_docs(void **state) {
    (void)state;
    gerror_t error = {0};

    insert_test_docs(5);

    mongolite_cursor_t *cursor = mongolite_find(g_db, "test", NULL, NULL, &error);
    assert_non_null(cursor);

    int count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        assert_non_null(doc);
        count++;
    }
    assert_int_equal(5, count);

    mongolite_cursor_destroy(cursor);
}

/* ============================================================
 * Limit Tests
 * ============================================================ */

static void test_cursor_limit(void **state) {
    (void)state;
    gerror_t error = {0};

    insert_test_docs(10);

    mongolite_cursor_t *cursor = mongolite_find(g_db, "test", NULL, NULL, &error);
    assert_non_null(cursor);

    int rc = mongolite_cursor_set_limit(cursor, 3);
    assert_int_equal(0, rc);

    int count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }
    assert_int_equal(3, count);

    mongolite_cursor_destroy(cursor);
}

static void test_cursor_limit_zero_means_unlimited(void **state) {
    (void)state;
    gerror_t error = {0};

    insert_test_docs(5);

    mongolite_cursor_t *cursor = mongolite_find(g_db, "test", NULL, NULL, &error);
    assert_non_null(cursor);

    int rc = mongolite_cursor_set_limit(cursor, 0);
    assert_int_equal(0, rc);

    int count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }
    assert_int_equal(5, count);  /* All docs returned */

    mongolite_cursor_destroy(cursor);
}

static void test_cursor_limit_greater_than_count(void **state) {
    (void)state;
    gerror_t error = {0};

    insert_test_docs(3);

    mongolite_cursor_t *cursor = mongolite_find(g_db, "test", NULL, NULL, &error);
    assert_non_null(cursor);

    int rc = mongolite_cursor_set_limit(cursor, 100);
    assert_int_equal(0, rc);

    int count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }
    assert_int_equal(3, count);  /* Only 3 docs exist */

    mongolite_cursor_destroy(cursor);
}

static void test_cursor_limit_after_iteration_fails(void **state) {
    (void)state;
    gerror_t error = {0};

    insert_test_docs(5);

    mongolite_cursor_t *cursor = mongolite_find(g_db, "test", NULL, NULL, &error);
    assert_non_null(cursor);

    /* Start iteration */
    const bson_t *doc;
    mongolite_cursor_next(cursor, &doc);

    /* Now try to set limit - should fail */
    int rc = mongolite_cursor_set_limit(cursor, 1);
    assert_int_equal(MONGOLITE_ERROR, rc);

    mongolite_cursor_destroy(cursor);
}

/* ============================================================
 * Skip Tests
 * ============================================================ */

static void test_cursor_skip(void **state) {
    (void)state;
    gerror_t error = {0};

    insert_test_docs(10);

    mongolite_cursor_t *cursor = mongolite_find(g_db, "test", NULL, NULL, &error);
    assert_non_null(cursor);

    int rc = mongolite_cursor_set_skip(cursor, 5);
    assert_int_equal(0, rc);

    int count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }
    assert_int_equal(5, count);  /* 10 - 5 = 5 */

    mongolite_cursor_destroy(cursor);
}

static void test_cursor_skip_all(void **state) {
    (void)state;
    gerror_t error = {0};

    insert_test_docs(5);

    mongolite_cursor_t *cursor = mongolite_find(g_db, "test", NULL, NULL, &error);
    assert_non_null(cursor);

    int rc = mongolite_cursor_set_skip(cursor, 10);  /* Skip more than exist */
    assert_int_equal(0, rc);

    int count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }
    assert_int_equal(0, count);

    mongolite_cursor_destroy(cursor);
}

static void test_cursor_skip_after_iteration_fails(void **state) {
    (void)state;
    gerror_t error = {0};

    insert_test_docs(5);

    mongolite_cursor_t *cursor = mongolite_find(g_db, "test", NULL, NULL, &error);
    assert_non_null(cursor);

    /* Start iteration */
    const bson_t *doc;
    mongolite_cursor_next(cursor, &doc);

    /* Now try to set skip - should fail */
    int rc = mongolite_cursor_set_skip(cursor, 1);
    assert_int_equal(MONGOLITE_ERROR, rc);

    mongolite_cursor_destroy(cursor);
}

/* ============================================================
 * Skip + Limit Combined Tests
 * ============================================================ */

static void test_cursor_skip_and_limit(void **state) {
    (void)state;
    gerror_t error = {0};

    insert_test_docs(10);

    mongolite_cursor_t *cursor = mongolite_find(g_db, "test", NULL, NULL, &error);
    assert_non_null(cursor);

    mongolite_cursor_set_skip(cursor, 3);
    mongolite_cursor_set_limit(cursor, 4);

    int count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        count++;
    }
    assert_int_equal(4, count);  /* Skip 3, take 4 */

    mongolite_cursor_destroy(cursor);
}

/* ============================================================
 * Sort Tests
 * ============================================================ */

static void test_cursor_set_sort(void **state) {
    (void)state;
    gerror_t error = {0};

    insert_test_docs(5);

    mongolite_cursor_t *cursor = mongolite_find(g_db, "test", NULL, NULL, &error);
    assert_non_null(cursor);

    bson_t *sort = bson_new();
    BSON_APPEND_INT32(sort, "index", -1);

    int rc = mongolite_cursor_set_sort(cursor, sort);
    assert_int_equal(0, rc);
    assert_non_null(cursor->sort);

    bson_destroy(sort);
    mongolite_cursor_destroy(cursor);
}

static void test_cursor_set_sort_after_iteration_fails(void **state) {
    (void)state;
    gerror_t error = {0};

    insert_test_docs(5);

    mongolite_cursor_t *cursor = mongolite_find(g_db, "test", NULL, NULL, &error);
    assert_non_null(cursor);

    /* Start iteration */
    const bson_t *doc;
    mongolite_cursor_next(cursor, &doc);

    /* Now try to set sort - should fail */
    bson_t *sort = bson_new();
    BSON_APPEND_INT32(sort, "index", -1);

    int rc = mongolite_cursor_set_sort(cursor, sort);
    assert_int_equal(MONGOLITE_ERROR, rc);

    bson_destroy(sort);
    mongolite_cursor_destroy(cursor);
}

/* ============================================================
 * Null Parameter Tests
 * ============================================================ */

static void test_cursor_null_param(void **state) {
    (void)state;

    /* cursor_next with NULL */
    const bson_t *doc;
    bool result = mongolite_cursor_next(NULL, &doc);
    assert_false(result);
    assert_null(doc);

    /* cursor_more with NULL */
    result = mongolite_cursor_more(NULL);
    assert_false(result);

    /* cursor_destroy with NULL - should not crash */
    mongolite_cursor_destroy(NULL);

    /* cursor_set_limit with NULL */
    int rc = mongolite_cursor_set_limit(NULL, 10);
    assert_int_equal(MONGOLITE_EINVAL, rc);

    /* cursor_set_skip with NULL */
    rc = mongolite_cursor_set_skip(NULL, 10);
    assert_int_equal(MONGOLITE_EINVAL, rc);

    /* cursor_set_sort with NULL cursor */
    bson_t *sort = bson_new();
    rc = mongolite_cursor_set_sort(NULL, sort);
    assert_int_equal(MONGOLITE_EINVAL, rc);
    bson_destroy(sort);
}

/* ============================================================
 * Filter Tests
 * ============================================================ */

static void test_cursor_with_filter(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Insert docs with different values */
    for (int i = 0; i < 10; i++) {
        bson_t *doc = bson_new();
        BSON_APPEND_INT32(doc, "value", i % 3);  /* 0, 1, 2, 0, 1, 2, ... */
        mongolite_insert_one(g_db, "test", doc, NULL, &error);
        bson_destroy(doc);
    }

    /* Filter for value == 1 */
    bson_t *filter = bson_new();
    BSON_APPEND_INT32(filter, "value", 1);

    mongolite_cursor_t *cursor = mongolite_find(g_db, "test", filter, NULL, &error);
    assert_non_null(cursor);

    int count = 0;
    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        bson_iter_t iter;
        assert_true(bson_iter_init_find(&iter, doc, "value"));
        assert_int_equal(1, bson_iter_int32(&iter));
        count++;
    }
    /* Should have 3 or 4 docs with value==1 (indices 1, 4, 7) */
    assert_true(count >= 3);

    bson_destroy(filter);
    mongolite_cursor_destroy(cursor);
}

/* ============================================================
 * Exhausted Cursor Tests
 * ============================================================ */

static void test_cursor_exhausted_returns_false(void **state) {
    (void)state;
    gerror_t error = {0};

    insert_test_docs(2);

    mongolite_cursor_t *cursor = mongolite_find(g_db, "test", NULL, NULL, &error);
    assert_non_null(cursor);

    const bson_t *doc;

    /* Exhaust cursor */
    while (mongolite_cursor_next(cursor, &doc)) {}

    /* Additional calls should return false */
    assert_false(mongolite_cursor_next(cursor, &doc));
    assert_null(doc);
    assert_false(mongolite_cursor_next(cursor, &doc));
    assert_false(mongolite_cursor_more(cursor));

    mongolite_cursor_destroy(cursor);
}

/* ============================================================
 * Resource Cleanup Tests
 * ============================================================ */

static void test_cursor_cleanup_on_destroy(void **state) {
    (void)state;
    gerror_t error = {0};

    insert_test_docs(5);

    int initial_iter_count = g_mock_wtree_state.iterator_create_count;
    int initial_iter_close = g_mock_wtree_state.iterator_close_count;

    mongolite_cursor_t *cursor = mongolite_find(g_db, "test", NULL, NULL, &error);
    assert_non_null(cursor);

    /* Iterate partially */
    const bson_t *doc;
    mongolite_cursor_next(cursor, &doc);
    mongolite_cursor_next(cursor, &doc);

    mongolite_cursor_destroy(cursor);

    /* Verify iterator was closed */
    assert_int_equal(initial_iter_count + 1, g_mock_wtree_state.iterator_create_count);
    assert_int_equal(initial_iter_close + 1, g_mock_wtree_state.iterator_close_count);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Basic tests */
        cmocka_unit_test_setup_teardown(test_cursor_empty_collection, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cursor_single_doc, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cursor_multiple_docs, setup, teardown),

        /* Limit tests */
        cmocka_unit_test_setup_teardown(test_cursor_limit, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cursor_limit_zero_means_unlimited, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cursor_limit_greater_than_count, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cursor_limit_after_iteration_fails, setup, teardown),

        /* Skip tests */
        cmocka_unit_test_setup_teardown(test_cursor_skip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cursor_skip_all, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cursor_skip_after_iteration_fails, setup, teardown),

        /* Combined tests */
        cmocka_unit_test_setup_teardown(test_cursor_skip_and_limit, setup, teardown),

        /* Sort tests */
        cmocka_unit_test_setup_teardown(test_cursor_set_sort, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cursor_set_sort_after_iteration_fails, setup, teardown),

        /* Null parameter tests */
        cmocka_unit_test_setup_teardown(test_cursor_null_param, setup, teardown),

        /* Filter tests */
        cmocka_unit_test_setup_teardown(test_cursor_with_filter, setup, teardown),

        /* Exhausted cursor tests */
        cmocka_unit_test_setup_teardown(test_cursor_exhausted_returns_false, setup, teardown),

        /* Resource cleanup tests */
        cmocka_unit_test_setup_teardown(test_cursor_cleanup_on_destroy, setup, teardown),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
