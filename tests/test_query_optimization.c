/**
 * test_query_optimization.c - Tests for index-based query optimization (Phase 4)
 *
 * Tests:
 * - Query analysis for simple equality
 * - Index selection for matching queries
 * - find_one using secondary index
 * - Fallback to collection scan when no index
 */

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>

#include "mongolite.h"
#include "mongolite_internal.h"

/* ============================================================
 * Test Setup/Teardown
 * ============================================================ */

static mongolite_db_t *g_db = NULL;
static const char *DB_PATH = "./test_query_opt_db";
static gerror_t error = {0};

static void cleanup_db_path(void) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", DB_PATH);
    system(cmd);
}

static int global_setup(void **state) {
    (void)state;
    cleanup_db_path();

    db_config_t config = {0};
    config.max_bytes = 64ULL * 1024 * 1024;

    int rc = mongolite_open(DB_PATH, &g_db, &config, &error);
    if (rc != 0) {
        return -1;
    }

    return 0;
}

static int global_teardown(void **state) {
    (void)state;
    if (g_db) {
        mongolite_close(g_db);
        g_db = NULL;
    }

    cleanup_db_path();
    return 0;
}

/* ============================================================
 * Tests: Query Analysis
 * ============================================================ */

static void test_analyze_simple_equality(void **state) {
    (void)state;

    /* Simple single-field equality */
    bson_t *filter = BCON_NEW("email", BCON_UTF8("test@example.com"));
    query_analysis_t *analysis = _analyze_query_for_index(filter);

    assert_non_null(analysis);
    assert_int_equal(1, analysis->equality_count);
    assert_string_equal("email", analysis->equality_fields[0]);
    assert_true(analysis->is_simple_equality);

    _free_query_analysis(analysis);
    bson_destroy(filter);
}

static void test_analyze_multiple_equality(void **state) {
    (void)state;

    /* Multiple field equality */
    bson_t *filter = BCON_NEW("name", BCON_UTF8("John"), "age", BCON_INT32(30));
    query_analysis_t *analysis = _analyze_query_for_index(filter);

    assert_non_null(analysis);
    assert_int_equal(2, analysis->equality_count);
    assert_true(analysis->is_simple_equality);

    _free_query_analysis(analysis);
    bson_destroy(filter);
}

static void test_analyze_with_operators_not_simple(void **state) {
    (void)state;

    /* Query with $gt operator - not simple equality */
    bson_t *filter = BCON_NEW("age", "{", "$gt", BCON_INT32(25), "}");
    query_analysis_t *analysis = _analyze_query_for_index(filter);

    /* Should return NULL or empty since it's not simple equality */
    assert_null(analysis);

    bson_destroy(filter);
}

static void test_analyze_empty_filter(void **state) {
    (void)state;

    bson_t *filter = bson_new();
    query_analysis_t *analysis = _analyze_query_for_index(filter);

    assert_null(analysis);

    bson_destroy(filter);
}

static void test_analyze_id_only_skipped(void **state) {
    (void)state;

    /* _id-only query should return NULL (already has dedicated optimization) */
    bson_oid_t oid;
    bson_oid_init(&oid, NULL);
    bson_t *filter = BCON_NEW("_id", BCON_OID(&oid));

    query_analysis_t *analysis = _analyze_query_for_index(filter);
    assert_null(analysis);

    bson_destroy(filter);
}

/* ============================================================
 * Tests: Index Selection
 * ============================================================ */

static void test_find_best_index_single_field(void **state) {
    (void)state;

    /* Create collection and index */
    int rc = mongolite_collection_create(g_db, "idx_test", NULL, &error);
    assert_int_equal(0, rc);

    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    rc = mongolite_create_index(g_db, "idx_test", keys, "email_1", NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(keys);

    /* Analyze query */
    bson_t *filter = BCON_NEW("email", BCON_UTF8("test@example.com"));
    query_analysis_t *analysis = _analyze_query_for_index(filter);
    assert_non_null(analysis);

    /* Find best index */
    _mongolite_lock(g_db);
    mongolite_cached_index_t *idx = _find_best_index(g_db, "idx_test", analysis, &error);
    _mongolite_unlock(g_db);

    assert_non_null(idx);
    assert_string_equal("email_1", idx->name);

    _free_query_analysis(analysis);
    bson_destroy(filter);
    mongolite_collection_drop(g_db, "idx_test", NULL);
}

static void test_find_best_index_no_match(void **state) {
    (void)state;

    /* Create collection with index on email */
    int rc = mongolite_collection_create(g_db, "idx_test2", NULL, &error);
    assert_int_equal(0, rc);

    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    rc = mongolite_create_index(g_db, "idx_test2", keys, "email_1", NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(keys);

    /* Query on different field */
    bson_t *filter = BCON_NEW("name", BCON_UTF8("John"));
    query_analysis_t *analysis = _analyze_query_for_index(filter);
    assert_non_null(analysis);

    /* Should not find a matching index */
    _mongolite_lock(g_db);
    mongolite_cached_index_t *idx = _find_best_index(g_db, "idx_test2", analysis, &error);
    _mongolite_unlock(g_db);

    assert_null(idx);

    _free_query_analysis(analysis);
    bson_destroy(filter);
    mongolite_collection_drop(g_db, "idx_test2", NULL);
}

/* ============================================================
 * Tests: find_one with Index
 * ============================================================ */

static void test_find_one_uses_index(void **state) {
    (void)state;

    /* Create collection */
    int rc = mongolite_collection_create(g_db, "users", NULL, &error);
    assert_int_equal(0, rc);

    /* Create index on email */
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    rc = mongolite_create_index(g_db, "users", keys, "email_1", NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(keys);

    /* Insert test documents */
    for (int i = 0; i < 100; i++) {
        char email[64];
        snprintf(email, sizeof(email), "user%d@example.com", i);
        bson_t *doc = BCON_NEW("email", BCON_UTF8(email), "name", BCON_UTF8("User"), "index", BCON_INT32(i));
        rc = mongolite_insert_one(g_db, "users", doc, NULL, &error);
        assert_int_equal(0, rc);
        bson_destroy(doc);
    }

    /* Find by email (should use index) */
    bson_t *filter = BCON_NEW("email", BCON_UTF8("user50@example.com"));
    bson_t *found = mongolite_find_one(g_db, "users", filter, NULL, &error);
    assert_non_null(found);

    /* Verify correct document was found */
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found, "index"));
    assert_int_equal(50, bson_iter_int32(&iter));

    bson_destroy(found);
    bson_destroy(filter);
    mongolite_collection_drop(g_db, "users", NULL);
}

static void test_find_one_not_found_with_index(void **state) {
    (void)state;

    /* Create collection */
    int rc = mongolite_collection_create(g_db, "users2", NULL, &error);
    assert_int_equal(0, rc);

    /* Create index on email */
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    rc = mongolite_create_index(g_db, "users2", keys, "email_1", NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(keys);

    /* Insert a document */
    bson_t *doc = BCON_NEW("email", BCON_UTF8("exists@example.com"), "name", BCON_UTF8("Exists"));
    rc = mongolite_insert_one(g_db, "users2", doc, NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc);

    /* Search for non-existent email */
    bson_t *filter = BCON_NEW("email", BCON_UTF8("notfound@example.com"));
    bson_t *found = mongolite_find_one(g_db, "users2", filter, NULL, &error);

    /* Should not find anything */
    assert_null(found);

    bson_destroy(filter);
    mongolite_collection_drop(g_db, "users2", NULL);
}

static void test_find_one_falls_back_to_scan(void **state) {
    (void)state;

    /* Create collection without index on queried field */
    int rc = mongolite_collection_create(g_db, "no_idx", NULL, &error);
    assert_int_equal(0, rc);

    /* Insert documents */
    for (int i = 0; i < 10; i++) {
        bson_t *doc = BCON_NEW("name", BCON_UTF8("TestUser"), "seq", BCON_INT32(i));
        rc = mongolite_insert_one(g_db, "no_idx", doc, NULL, &error);
        assert_int_equal(0, rc);
        bson_destroy(doc);
    }

    /* Find by name (no index - should fall back to scan) */
    bson_t *filter = BCON_NEW("name", BCON_UTF8("TestUser"));
    bson_t *found = mongolite_find_one(g_db, "no_idx", filter, NULL, &error);

    /* Should find one of the documents */
    assert_non_null(found);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found, "name"));
    assert_string_equal("TestUser", bson_iter_utf8(&iter, NULL));

    bson_destroy(found);
    bson_destroy(filter);
    mongolite_collection_drop(g_db, "no_idx", NULL);
}

static void test_find_one_compound_index(void **state) {
    (void)state;

    /* Create collection */
    int rc = mongolite_collection_create(g_db, "compound", NULL, &error);
    assert_int_equal(0, rc);

    /* Create compound index on (category, status) */
    bson_t *keys = BCON_NEW("category", BCON_INT32(1), "status", BCON_INT32(1));
    rc = mongolite_create_index(g_db, "compound", keys, "cat_status_1", NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(keys);

    /* Insert documents */
    bson_t *doc1 = BCON_NEW("category", BCON_UTF8("A"), "status", BCON_UTF8("active"), "val", BCON_INT32(1));
    bson_t *doc2 = BCON_NEW("category", BCON_UTF8("A"), "status", BCON_UTF8("inactive"), "val", BCON_INT32(2));
    bson_t *doc3 = BCON_NEW("category", BCON_UTF8("B"), "status", BCON_UTF8("active"), "val", BCON_INT32(3));
    rc = mongolite_insert_one(g_db, "compound", doc1, NULL, &error);
    assert_int_equal(0, rc);
    rc = mongolite_insert_one(g_db, "compound", doc2, NULL, &error);
    assert_int_equal(0, rc);
    rc = mongolite_insert_one(g_db, "compound", doc3, NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc1);
    bson_destroy(doc2);
    bson_destroy(doc3);

    /* Query on both fields of compound index */
    bson_t *filter = BCON_NEW("category", BCON_UTF8("A"), "status", BCON_UTF8("inactive"));
    bson_t *found = mongolite_find_one(g_db, "compound", filter, NULL, &error);

    assert_non_null(found);
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found, "val"));
    assert_int_equal(2, bson_iter_int32(&iter));

    bson_destroy(found);
    bson_destroy(filter);
    mongolite_collection_drop(g_db, "compound", NULL);
}

/* ============================================================
 * Test Runner
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Query Analysis */
        cmocka_unit_test(test_analyze_simple_equality),
        cmocka_unit_test(test_analyze_multiple_equality),
        cmocka_unit_test(test_analyze_with_operators_not_simple),
        cmocka_unit_test(test_analyze_empty_filter),
        cmocka_unit_test(test_analyze_id_only_skipped),

        /* Index Selection */
        cmocka_unit_test(test_find_best_index_single_field),
        cmocka_unit_test(test_find_best_index_no_match),

        /* find_one with Index */
        cmocka_unit_test(test_find_one_uses_index),
        cmocka_unit_test(test_find_one_not_found_with_index),
        cmocka_unit_test(test_find_one_falls_back_to_scan),
        cmocka_unit_test(test_find_one_compound_index),
    };

    int rc = cmocka_run_group_tests_name("tests", tests, global_setup, global_teardown);

    return rc;
}
