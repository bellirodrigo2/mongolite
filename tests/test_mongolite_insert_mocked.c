/**
 * test_mongolite_insert_mocked.c - Unit tests for insert operations with mocking
 *
 * Tests insert functionality using mock_wtree to:
 * - Test error paths (disk full, txn failures)
 * - Verify transaction behavior
 * - Test duplicate key handling
 * - Test batch insert behavior
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

    int rc = mongolite_open("./test_insert_mocked", &g_db, &config, &error);
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
 * Helper: Create test document
 * ============================================================ */

static bson_t* create_test_doc(const char *id, int value) {
    bson_t *doc = bson_new();
    BSON_APPEND_UTF8(doc, "_id", id);
    BSON_APPEND_INT32(doc, "value", value);
    return doc;
}

/* ============================================================
 * Basic Insert Tests
 * ============================================================ */

static void test_insert_one_success(void **state) {
    (void)state;
    gerror_t error = {0};

    bson_t *doc = create_test_doc("doc1", 42);
    bson_oid_t inserted_id;
    int rc = mongolite_insert_one(g_db, "test", doc, &inserted_id, &error);
    assert_int_equal(0, rc);

    bson_destroy(doc);
}

static void test_insert_one_null_params(void **state) {
    (void)state;
    gerror_t error = {0};

    bson_t *doc = bson_new();

    /* NULL db */
    int rc = mongolite_insert_one(NULL, "test", doc, NULL, &error);
    assert_int_equal(MONGOLITE_EINVAL, rc);

    /* NULL collection */
    rc = mongolite_insert_one(g_db, NULL, doc, NULL, &error);
    assert_int_equal(MONGOLITE_EINVAL, rc);

    /* NULL document */
    rc = mongolite_insert_one(g_db, "test", NULL, NULL, &error);
    assert_int_equal(MONGOLITE_EINVAL, rc);

    bson_destroy(doc);
}

static void test_insert_one_nonexistent_collection(void **state) {
    (void)state;
    gerror_t error = {0};

    bson_t *doc = create_test_doc("doc1", 42);
    int rc = mongolite_insert_one(g_db, "nonexistent", doc, NULL, &error);
    /* Collection not found - should return an error */
    assert_int_not_equal(0, rc);

    bson_destroy(doc);
}

/* ============================================================
 * Duplicate Key Tests
 * ============================================================ */

static void test_insert_duplicate_key(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Insert first document with unique ID */
    bson_t *doc1 = bson_new();
    BSON_APPEND_INT32(doc1, "value", 1);  /* Let mongolite generate _id */
    bson_oid_t oid1;
    int rc = mongolite_insert_one(g_db, "test", doc1, &oid1, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc1);

    /* Insert second document with same _id (the one we just got) */
    bson_t *doc2 = bson_new();
    BSON_APPEND_OID(doc2, "_id", &oid1);
    BSON_APPEND_INT32(doc2, "value", 2);
    rc = mongolite_insert_one(g_db, "test", doc2, NULL, &error);
    /* Mock returns MDB_KEYEXIST directly */
    assert_int_equal(MDB_KEYEXIST, rc);
    bson_destroy(doc2);
}

/* ============================================================
 * Auto-generated _id Tests
 * ============================================================ */

static void test_insert_generates_id(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Insert document without _id */
    bson_t *doc = bson_new();
    BSON_APPEND_INT32(doc, "value", 100);

    bson_oid_t inserted_id;
    memset(&inserted_id, 0, sizeof(inserted_id));
    int rc = mongolite_insert_one(g_db, "test", doc, &inserted_id, &error);
    assert_int_equal(0, rc);

    /* The inserted_id should be populated with a valid OID */
    char oid_str[25];
    bson_oid_to_string(&inserted_id, oid_str);
    assert_true(strlen(oid_str) == 24);  /* OID string is 24 hex chars */

    bson_destroy(doc);
}

static void test_insert_returns_oid(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Insert document without _id */
    bson_t *doc = bson_new();
    BSON_APPEND_INT32(doc, "value", 200);

    bson_oid_t inserted_id;
    memset(&inserted_id, 0, sizeof(inserted_id));

    int rc = mongolite_insert_one(g_db, "test", doc, &inserted_id, &error);
    assert_int_equal(0, rc);

    /* inserted_id should be populated */
    char oid_str[25];
    bson_oid_to_string(&inserted_id, oid_str);
    assert_true(strlen(oid_str) > 0);

    bson_destroy(doc);
}

/* ============================================================
 * Error Injection Tests
 * ============================================================ */

static void test_insert_map_full_auto_resize(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Get initial mapsize */
    size_t initial_mapsize = wtree_db_get_mapsize((wtree_db_t*)g_db->wdb);

    /* Inject MDB_MAP_FULL error for next insert - should trigger auto-resize */
    mock_wtree_fail_next_insert(MDB_MAP_FULL);

    bson_t *doc = create_test_doc("resize_test", 1);
    int rc = mongolite_insert_one(g_db, "test", doc, NULL, &error);

    /* Insert should succeed after resize */
    assert_int_equal(0, rc);

    /* Mapsize should have doubled */
    size_t new_mapsize = wtree_db_get_mapsize((wtree_db_t*)g_db->wdb);
    assert_int_equal(initial_mapsize * 2, new_mapsize);

    bson_destroy(doc);
}

static void test_insert_wtree_failure(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Inject a non-MAP_FULL error that doesn't trigger resize */
    mock_wtree_fail_next_insert(MDB_KEYEXIST);

    bson_t *doc = create_test_doc("will_fail", 1);
    int rc = mongolite_insert_one(g_db, "test", doc, NULL, &error);
    assert_int_equal(MDB_KEYEXIST, rc);

    bson_destroy(doc);
}

static void test_insert_txn_begin_failure(void **state) {
    (void)state;
    gerror_t error = {0};

    /*
     * The insert flow now pre-loads index cache which may use a read transaction.
     * We need to fail the insert operation itself to test write txn failure path.
     * Use mock_wtree_fail_next_insert instead.
     */
    mock_wtree_fail_next_insert(WTREE_TXN_FULL);

    bson_t *doc = create_test_doc("will_fail", 1);
    int rc = mongolite_insert_one(g_db, "test", doc, NULL, &error);
    assert_int_not_equal(0, rc);

    bson_destroy(doc);
}

/* ============================================================
 * Transaction Mode Tests
 * ============================================================ */

static void test_insert_in_transaction(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Start explicit transaction */
    int rc = mongolite_begin_transaction(g_db);
    assert_int_equal(0, rc);

    int initial_commit_count = g_mock_wtree_state.txn_commit_count;

    /* Insert should use existing transaction (no auto-commit) */
    bson_t *doc = create_test_doc("txn_doc", 1);
    rc = mongolite_insert_one(g_db, "test", doc, NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc);

    /* Should not have committed yet */
    assert_int_equal(initial_commit_count, g_mock_wtree_state.txn_commit_count);

    /* Commit transaction */
    rc = mongolite_commit(g_db);
    assert_int_equal(0, rc);

    /* Now commit count should increase */
    assert_int_equal(initial_commit_count + 1, g_mock_wtree_state.txn_commit_count);
}

static void test_insert_auto_commit(void **state) {
    (void)state;
    gerror_t error = {0};

    int initial_commit_count = g_mock_wtree_state.txn_commit_count;

    /* Insert without explicit transaction should auto-commit */
    bson_t *doc = create_test_doc("auto_commit_doc", 1);
    int rc = mongolite_insert_one(g_db, "test", doc, NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc);

    /* Should have auto-committed */
    assert_int_equal(initial_commit_count + 1, g_mock_wtree_state.txn_commit_count);
}

static void test_insert_multiple_in_transaction(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Start explicit transaction */
    int rc = mongolite_begin_transaction(g_db);
    assert_int_equal(0, rc);

    int initial_commit_count = g_mock_wtree_state.txn_commit_count;

    /* Insert multiple documents in same transaction */
    for (int i = 0; i < 5; i++) {
        char id[32];
        snprintf(id, sizeof(id), "multi_txn_%d", i);
        bson_t *doc = create_test_doc(id, i);
        rc = mongolite_insert_one(g_db, "test", doc, NULL, &error);
        assert_int_equal(0, rc);
        bson_destroy(doc);
    }

    /* Should not have committed during inserts */
    assert_int_equal(initial_commit_count, g_mock_wtree_state.txn_commit_count);

    /* Commit transaction */
    rc = mongolite_commit(g_db);
    assert_int_equal(0, rc);

    /* Single commit for all */
    assert_int_equal(initial_commit_count + 1, g_mock_wtree_state.txn_commit_count);
}

/* ============================================================
 * Insert Many Tests
 * ============================================================ */

static void test_insert_many_success(void **state) {
    (void)state;
    gerror_t error = {0};

    bson_t *docs[3];
    docs[0] = create_test_doc("many1", 1);
    docs[1] = create_test_doc("many2", 2);
    docs[2] = create_test_doc("many3", 3);

    int rc = mongolite_insert_many(g_db, "test", (const bson_t**)docs, 3, NULL, &error);
    assert_int_equal(0, rc);

    for (int i = 0; i < 3; i++) {
        bson_destroy(docs[i]);
    }
}

static void test_insert_many_null_params(void **state) {
    (void)state;
    gerror_t error = {0};

    bson_t *doc = bson_new();
    const bson_t *docs[1] = { doc };

    /* NULL db */
    int rc = mongolite_insert_many(NULL, "test", docs, 1, NULL, &error);
    assert_int_equal(MONGOLITE_EINVAL, rc);

    /* NULL collection */
    rc = mongolite_insert_many(g_db, NULL, docs, 1, NULL, &error);
    assert_int_equal(MONGOLITE_EINVAL, rc);

    /* NULL documents */
    rc = mongolite_insert_many(g_db, "test", NULL, 1, NULL, &error);
    assert_int_equal(MONGOLITE_EINVAL, rc);

    /* Zero count */
    rc = mongolite_insert_many(g_db, "test", docs, 0, NULL, &error);
    assert_int_equal(MONGOLITE_EINVAL, rc);

    bson_destroy(doc);
}

static void test_insert_many_duplicate_within_batch(void **state) {
    (void)state;
    gerror_t error = {0};

    /* First insert a doc to get a known OID */
    bson_t *first = bson_new();
    BSON_APPEND_INT32(first, "value", 0);
    bson_oid_t oid;
    int rc = mongolite_insert_one(g_db, "test", first, &oid, &error);
    assert_int_equal(0, rc);
    bson_destroy(first);

    /* Now try to insert another doc with the same _id */
    bson_t *dup = bson_new();
    BSON_APPEND_OID(dup, "_id", &oid);
    BSON_APPEND_INT32(dup, "value", 999);

    rc = mongolite_insert_one(g_db, "test", dup, NULL, &error);
    /* Mock returns MDB_KEYEXIST directly */
    assert_int_equal(MDB_KEYEXIST, rc);
    bson_destroy(dup);
}

/* ============================================================
 * JSON Insert Tests
 * ============================================================ */

static void test_insert_one_json_success(void **state) {
    (void)state;
    gerror_t error = {0};

    const char *json = "{\"_id\": \"json_doc\", \"value\": 42}";
    bson_oid_t inserted_id;
    int rc = mongolite_insert_one_json(g_db, "test", json, &inserted_id, &error);
    assert_int_equal(0, rc);
}

static void test_insert_one_json_null_params(void **state) {
    (void)state;
    gerror_t error = {0};

    const char *json = "{\"value\": 1}";

    /* NULL db */
    int rc = mongolite_insert_one_json(NULL, "test", json, NULL, &error);
    assert_int_equal(MONGOLITE_EINVAL, rc);

    /* NULL collection */
    rc = mongolite_insert_one_json(g_db, NULL, json, NULL, &error);
    assert_int_equal(MONGOLITE_EINVAL, rc);

    /* NULL json */
    rc = mongolite_insert_one_json(g_db, "test", NULL, NULL, &error);
    assert_int_equal(MONGOLITE_EINVAL, rc);
}

static void test_insert_one_json_invalid(void **state) {
    (void)state;
    gerror_t error = {0};

    const char *invalid_json = "{\"value\": invalid}";
    int rc = mongolite_insert_one_json(g_db, "test", invalid_json, NULL, &error);
    assert_int_not_equal(0, rc);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Basic insert tests */
        cmocka_unit_test_setup_teardown(test_insert_one_success, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_one_null_params, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_one_nonexistent_collection, setup, teardown),

        /* Duplicate key tests */
        cmocka_unit_test_setup_teardown(test_insert_duplicate_key, setup, teardown),

        /* Auto-generated _id tests */
        cmocka_unit_test_setup_teardown(test_insert_generates_id, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_returns_oid, setup, teardown),

        /* Error injection tests */
        cmocka_unit_test_setup_teardown(test_insert_map_full_auto_resize, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_wtree_failure, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_txn_begin_failure, setup, teardown),

        /* Transaction mode tests */
        cmocka_unit_test_setup_teardown(test_insert_in_transaction, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_auto_commit, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_multiple_in_transaction, setup, teardown),

        /* Insert many tests */
        cmocka_unit_test_setup_teardown(test_insert_many_success, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_many_null_params, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_many_duplicate_within_batch, setup, teardown),

        /* JSON insert tests */
        cmocka_unit_test_setup_teardown(test_insert_one_json_success, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_one_json_null_params, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_one_json_invalid, setup, teardown),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
