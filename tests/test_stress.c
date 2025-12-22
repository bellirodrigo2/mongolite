/**
 * test_stress.c - Stress tests for mongolite
 *
 * Tests high-volume operations:
 * - Bulk inserts
 * - Rapid updates
 * - Many deletes
 * - Concurrent-like patterns
 * - Large documents
 * - Memory pressure scenarios
 */

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include "mongolite.h"
#include "mongolite_internal.h"

#define TEST_DB_PATH "./test_stress_db"

/* ============================================================
 * Test Setup/Teardown
 * ============================================================ */

static mongolite_db_t *g_db = NULL;

static int global_setup(void **state) {
    (void)state;

    /* Clean up any existing test database */
    system("rm -rf " TEST_DB_PATH);

    gerror_t error = {0};
    db_config_t config = {0};
    config.max_bytes = 256ULL * 1024 * 1024;  /* 256MB for stress tests */

    int rc = mongolite_open(TEST_DB_PATH, &g_db, &config, &error);
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
    system("rm -rf " TEST_DB_PATH);
    return 0;
}

static int collection_setup(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Create fresh collection for each test */
    mongolite_collection_drop(g_db, "stress", &error);
    int rc = mongolite_collection_create(g_db, "stress", NULL, &error);
    if (rc != 0 && rc != MONGOLITE_EEXISTS) {
        return -1;
    }
    return 0;
}

static int collection_teardown(void **state) {
    (void)state;
    gerror_t error = {0};
    mongolite_collection_drop(g_db, "stress", &error);
    return 0;
}

/* ============================================================
 * Bulk Insert Tests
 * ============================================================ */

static void test_bulk_insert_1000(void **state) {
    (void)state;
    gerror_t error = {0};

    for (int i = 0; i < 1000; i++) {
        bson_t *doc = bson_new();
        BSON_APPEND_INT32(doc, "index", i);
        BSON_APPEND_UTF8(doc, "type", "bulk");
        BSON_APPEND_INT32(doc, "value", i * 10);

        int rc = mongolite_insert_one(g_db, "stress", doc, NULL, &error);
        assert_int_equal(0, rc);
        bson_destroy(doc);
    }

    /* Verify count */
    int64_t count = mongolite_collection_count(g_db, "stress", NULL, &error);
    assert_int_equal(1000, count);
}

static void test_bulk_insert_transaction(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Insert 500 docs in a single transaction */
    int rc = mongolite_begin_transaction(g_db);
    assert_int_equal(0, rc);

    for (int i = 0; i < 500; i++) {
        bson_t *doc = bson_new();
        BSON_APPEND_INT32(doc, "batch", 1);
        BSON_APPEND_INT32(doc, "index", i);

        rc = mongolite_insert_one(g_db, "stress", doc, NULL, &error);
        assert_int_equal(0, rc);
        bson_destroy(doc);
    }

    rc = mongolite_commit(g_db);
    assert_int_equal(0, rc);

    /* Verify count */
    int64_t count = mongolite_collection_count(g_db, "stress", NULL, &error);
    assert_int_equal(500, count);
}

/* ============================================================
 * Rapid Update Tests
 * ============================================================ */

static void test_rapid_updates_same_doc(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Insert one document */
    bson_t *doc = bson_new();
    BSON_APPEND_INT32(doc, "counter", 0);  /* Let mongolite generate _id */
    bson_oid_t oid;
    int rc = mongolite_insert_one(g_db, "stress", doc, &oid, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc);

    /* Use the OID for updates */
    char oid_str[25];
    bson_oid_to_string(&oid, oid_str);

    /* Update it 100 times */
    for (int i = 1; i <= 100; i++) {
        bson_t *filter = bson_new();
        BSON_APPEND_OID(filter, "_id", &oid);

        bson_t *update = BCON_NEW("$set", "{", "counter", BCON_INT32(i), "}");

        rc = mongolite_update_one(g_db, "stress", filter, update, false, &error);
        assert_int_equal(0, rc);

        bson_destroy(filter);
        bson_destroy(update);
    }

    /* Verify final value */
    bson_t *filter = bson_new();
    BSON_APPEND_OID(filter, "_id", &oid);

    mongolite_cursor_t *cursor = mongolite_find(g_db, "stress", filter, NULL, &error);
    assert_non_null(cursor);

    const bson_t *result;
    assert_true(mongolite_cursor_next(cursor, &result));

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, result, "counter"));
    assert_int_equal(100, bson_iter_int32(&iter));

    mongolite_cursor_destroy(cursor);
    bson_destroy(filter);
}

static void test_rapid_updates_many_docs(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Insert 100 documents */
    for (int i = 0; i < 100; i++) {
        bson_t *doc = bson_new();
        BSON_APPEND_INT32(doc, "group", i % 10);
        BSON_APPEND_INT32(doc, "value", 0);
        int rc = mongolite_insert_one(g_db, "stress", doc, NULL, &error);
        assert_int_equal(0, rc);
        bson_destroy(doc);
    }

    /* Update all docs in group 5 (10 docs) 20 times each */
    for (int round = 1; round <= 20; round++) {
        bson_t *filter = bson_new();
        BSON_APPEND_INT32(filter, "group", 5);

        bson_t *update = BCON_NEW("$set", "{", "value", BCON_INT32(round), "}");

        int64_t modified = 0;
        int rc = mongolite_update_many(g_db, "stress", filter, update, false, &modified, &error);
        assert_int_equal(0, rc);
        assert_int_equal(10, modified);

        bson_destroy(filter);
        bson_destroy(update);
    }
}

/* ============================================================
 * Delete Stress Tests
 * ============================================================ */

static void test_delete_many_stress(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Insert 500 documents */
    for (int i = 0; i < 500; i++) {
        bson_t *doc = bson_new();
        BSON_APPEND_INT32(doc, "batch", i / 100);  /* 5 batches of 100 */
        BSON_APPEND_INT32(doc, "index", i);
        int rc = mongolite_insert_one(g_db, "stress", doc, NULL, &error);
        assert_int_equal(0, rc);
        bson_destroy(doc);
    }

    /* Delete each batch */
    for (int batch = 0; batch < 5; batch++) {
        bson_t *filter = bson_new();
        BSON_APPEND_INT32(filter, "batch", batch);

        int64_t deleted = 0;
        int rc = mongolite_delete_many(g_db, "stress", filter, &deleted, &error);
        assert_int_equal(0, rc);
        assert_int_equal(100, deleted);

        bson_destroy(filter);
    }

    /* Verify empty */
    int64_t count = mongolite_collection_count(g_db, "stress", NULL, &error);
    assert_int_equal(0, count);
}

/* ============================================================
 * Large Document Tests
 * ============================================================ */

static void test_large_documents(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Create a large string (10KB) */
    char *large_string = malloc(10 * 1024 + 1);
    memset(large_string, 'x', 10 * 1024);
    large_string[10 * 1024] = '\0';

    /* Insert 100 documents with large payload */
    for (int i = 0; i < 100; i++) {
        bson_t *doc = bson_new();
        BSON_APPEND_INT32(doc, "index", i);
        BSON_APPEND_UTF8(doc, "payload", large_string);
        int rc = mongolite_insert_one(g_db, "stress", doc, NULL, &error);
        assert_int_equal(0, rc);
        bson_destroy(doc);
    }

    free(large_string);

    /* Verify count */
    int64_t count = mongolite_collection_count(g_db, "stress", NULL, &error);
    assert_int_equal(100, count);
}

/* ============================================================
 * Mixed Operations Test
 * ============================================================ */

static void test_mixed_operations(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Interleave inserts, updates, deletes, and finds */
    for (int round = 0; round < 50; round++) {
        /* Insert 10 docs */
        for (int i = 0; i < 10; i++) {
            bson_t *doc = bson_new();
            BSON_APPEND_INT32(doc, "round", round);
            BSON_APPEND_INT32(doc, "index", i);
            BSON_APPEND_INT32(doc, "value", 0);
            int rc = mongolite_insert_one(g_db, "stress", doc, NULL, &error);
            assert_int_equal(0, rc);
            bson_destroy(doc);
        }

        /* Update half of them */
        bson_t *filter = BCON_NEW("round", BCON_INT32(round), "index", "{", "$lt", BCON_INT32(5), "}");
        bson_t *update = BCON_NEW("$set", "{", "value", BCON_INT32(100), "}");
        int64_t modified = 0;
        int rc = mongolite_update_many(g_db, "stress", filter, update, false, &modified, &error);
        assert_int_equal(0, rc);
        bson_destroy(filter);
        bson_destroy(update);

        /* Delete a few */
        if (round % 5 == 0 && round > 0) {
            filter = bson_new();
            BSON_APPEND_INT32(filter, "round", round - 5);
            int64_t deleted = 0;
            rc = mongolite_delete_many(g_db, "stress", filter, &deleted, &error);
            assert_int_equal(0, rc);
            bson_destroy(filter);
        }

        /* Find and count */
        filter = bson_new();
        BSON_APPEND_INT32(filter, "round", round);
        mongolite_cursor_t *cursor = mongolite_find(g_db, "stress", filter, NULL, &error);
        assert_non_null(cursor);
        int found = 0;
        const bson_t *doc;
        while (mongolite_cursor_next(cursor, &doc)) {
            found++;
        }
        assert_int_equal(10, found);
        mongolite_cursor_destroy(cursor);
        bson_destroy(filter);
    }
}

/* ============================================================
 * Cursor Stress Test
 * ============================================================ */

static void test_cursor_many_iterations(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Insert 500 documents */
    for (int i = 0; i < 500; i++) {
        bson_t *doc = bson_new();
        BSON_APPEND_INT32(doc, "index", i);
        int rc = mongolite_insert_one(g_db, "stress", doc, NULL, &error);
        assert_int_equal(0, rc);
        bson_destroy(doc);
    }

    /* Iterate through all docs multiple times */
    for (int iter = 0; iter < 10; iter++) {
        mongolite_cursor_t *cursor = mongolite_find(g_db, "stress", NULL, NULL, &error);
        assert_non_null(cursor);

        int count = 0;
        const bson_t *doc;
        while (mongolite_cursor_next(cursor, &doc)) {
            count++;
        }
        assert_int_equal(500, count);

        mongolite_cursor_destroy(cursor);
    }
}

/* ============================================================
 * Transaction Stress Test
 * ============================================================ */

static void test_transaction_rollback_stress(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Insert some initial data */
    for (int i = 0; i < 100; i++) {
        bson_t *doc = bson_new();
        BSON_APPEND_INT32(doc, "permanent", i);
        int rc = mongolite_insert_one(g_db, "stress", doc, NULL, &error);
        assert_int_equal(0, rc);
        bson_destroy(doc);
    }

    /* Do many transaction begin/rollback cycles */
    for (int round = 0; round < 20; round++) {
        int rc = mongolite_begin_transaction(g_db);
        assert_int_equal(0, rc);

        /* Rollback immediately */
        rc = mongolite_rollback(g_db);
        assert_int_equal(0, rc);
    }

    /* Verify permanent docs still exist */
    int64_t count = mongolite_collection_count(g_db, "stress", NULL, &error);
    assert_int_equal(100, count);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Bulk insert tests */
        cmocka_unit_test_setup_teardown(test_bulk_insert_1000, collection_setup, collection_teardown),
        cmocka_unit_test_setup_teardown(test_bulk_insert_transaction, collection_setup, collection_teardown),

        /* Rapid update tests */
        cmocka_unit_test_setup_teardown(test_rapid_updates_same_doc, collection_setup, collection_teardown),
        cmocka_unit_test_setup_teardown(test_rapid_updates_many_docs, collection_setup, collection_teardown),

        /* Delete stress tests */
        cmocka_unit_test_setup_teardown(test_delete_many_stress, collection_setup, collection_teardown),

        /* Large document tests */
        cmocka_unit_test_setup_teardown(test_large_documents, collection_setup, collection_teardown),

        /* Mixed operations */
        cmocka_unit_test_setup_teardown(test_mixed_operations, collection_setup, collection_teardown),

        /* Cursor stress */
        cmocka_unit_test_setup_teardown(test_cursor_many_iterations, collection_setup, collection_teardown),

        /* Transaction stress */
        cmocka_unit_test_setup_teardown(test_transaction_rollback_stress, collection_setup, collection_teardown),
    };

    return cmocka_run_group_tests(tests, global_setup, global_teardown);
}
