/**
 * test_mongolite_txn_mocked.c - Unit tests for transaction management with mocking
 *
 * Tests transaction helpers and error paths using mock_wtree.
 * This enables testing scenarios that are hard to trigger with real LMDB:
 * - Transaction begin failures
 * - Commit failures
 * - Transaction reuse/pooling
 * - Error recovery paths
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

static int setup(void **state) {
    (void)state;
    mock_wtree_reset();
    return 0;
}

static int teardown(void **state) {
    (void)state;
    mock_wtree_reset();
    return 0;
}

/* ============================================================
 * Helper: Create a minimal db structure for testing
 * ============================================================ */

static mongolite_db_t* create_test_db(void) {
    gerror_t error = {0};

    /* Use mongolite_open which will use our mocked wtree */
    mongolite_db_t *db = NULL;
    db_config_t config = {0};
    config.max_bytes = 32ULL * 1024 * 1024;

    int rc = mongolite_open("./test_txn_mocked", &db, &config, &error);
    if (rc != 0) {
        return NULL;
    }
    return db;
}

/* ============================================================
 * Basic Transaction Tests
 * ============================================================ */

static void test_begin_commit_transaction(void **state) {
    (void)state;
    mongolite_db_t *db = create_test_db();
    assert_non_null(db);

    /* Begin transaction */
    int rc = mongolite_begin_transaction(db);
    assert_int_equal(0, rc);
    assert_true(db->in_transaction);
    assert_non_null(db->current_txn);

    /* Commit transaction */
    rc = mongolite_commit(db);
    assert_int_equal(0, rc);
    assert_false(db->in_transaction);
    assert_null(db->current_txn);

    mongolite_close(db);
}

static void test_begin_rollback_transaction(void **state) {
    (void)state;
    mongolite_db_t *db = create_test_db();
    assert_non_null(db);

    /* Begin transaction */
    int rc = mongolite_begin_transaction(db);
    assert_int_equal(0, rc);

    /* Rollback transaction */
    rc = mongolite_rollback(db);
    assert_int_equal(0, rc);
    assert_false(db->in_transaction);
    assert_null(db->current_txn);

    mongolite_close(db);
}

static void test_double_begin_fails(void **state) {
    (void)state;
    mongolite_db_t *db = create_test_db();
    assert_non_null(db);

    /* First begin succeeds */
    int rc = mongolite_begin_transaction(db);
    assert_int_equal(0, rc);

    /* Second begin fails (already in transaction) */
    rc = mongolite_begin_transaction(db);
    assert_int_equal(MONGOLITE_ERROR, rc);

    /* Clean up */
    mongolite_rollback(db);
    mongolite_close(db);
}

static void test_commit_without_begin_fails(void **state) {
    (void)state;
    mongolite_db_t *db = create_test_db();
    assert_non_null(db);

    /* Commit without begin should fail */
    int rc = mongolite_commit(db);
    assert_int_equal(MONGOLITE_ERROR, rc);

    mongolite_close(db);
}

static void test_rollback_without_begin_fails(void **state) {
    (void)state;
    mongolite_db_t *db = create_test_db();
    assert_non_null(db);

    /* Rollback without begin should fail */
    int rc = mongolite_rollback(db);
    assert_int_equal(MONGOLITE_ERROR, rc);

    mongolite_close(db);
}

/* ============================================================
 * Null Parameter Tests
 * ============================================================ */

static void test_begin_null_db(void **state) {
    (void)state;
    int rc = mongolite_begin_transaction(NULL);
    assert_int_equal(MONGOLITE_EINVAL, rc);
}

static void test_commit_null_db(void **state) {
    (void)state;
    int rc = mongolite_commit(NULL);
    assert_int_equal(MONGOLITE_EINVAL, rc);
}

static void test_rollback_null_db(void **state) {
    (void)state;
    int rc = mongolite_rollback(NULL);
    assert_int_equal(MONGOLITE_EINVAL, rc);
}

/* ============================================================
 * Read Transaction Pooling Tests
 * ============================================================ */

static void test_read_txn_pooling(void **state) {
    (void)state;
    mongolite_db_t *db = create_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    /* First read txn should create new */
    wtree_txn_t *txn1 = _mongolite_get_read_txn(db, &error);
    assert_non_null(txn1);
    assert_non_null(db->read_txn_pool);

    /* Release it - should go back to pool */
    _mongolite_release_read_txn(db, txn1);

    /* Second read txn should reuse pooled */
    wtree_txn_t *txn2 = _mongolite_get_read_txn(db, &error);
    assert_non_null(txn2);
    assert_ptr_equal(txn1, txn2);  /* Same handle reused */

    _mongolite_release_read_txn(db, txn2);
    mongolite_close(db);
}

static void test_write_txn_invalidates_read_pool(void **state) {
    (void)state;
    mongolite_db_t *db = create_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    /* Create a pooled read transaction */
    wtree_txn_t *read_txn = _mongolite_get_read_txn(db, &error);
    assert_non_null(read_txn);
    _mongolite_release_read_txn(db, read_txn);
    assert_non_null(db->read_txn_pool);

    /* Getting write txn should invalidate the pool */
    wtree_txn_t *write_txn = _mongolite_get_write_txn(db, &error);
    assert_non_null(write_txn);
    assert_null(db->read_txn_pool);  /* Pool was cleared */

    wtree_txn_commit(write_txn, &error);
    mongolite_close(db);
}

/* ============================================================
 * Auto-commit/abort Helper Tests
 * ============================================================ */

static void test_commit_if_auto_commits_when_not_in_txn(void **state) {
    (void)state;
    mongolite_db_t *db = create_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    wtree_txn_t *txn = _mongolite_get_write_txn(db, &error);
    assert_non_null(txn);

    int initial_commit_count = g_mock_wtree_state.txn_commit_count;

    /* Should commit since we're not in explicit transaction */
    int rc = _mongolite_commit_if_auto(db, txn, &error);
    assert_int_equal(0, rc);
    assert_int_equal(initial_commit_count + 1, g_mock_wtree_state.txn_commit_count);

    mongolite_close(db);
}

static void test_commit_if_auto_skips_when_in_txn(void **state) {
    (void)state;
    mongolite_db_t *db = create_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    /* Start explicit transaction */
    int rc = mongolite_begin_transaction(db);
    assert_int_equal(0, rc);

    int initial_commit_count = g_mock_wtree_state.txn_commit_count;

    /* Should NOT commit since we're in explicit transaction */
    rc = _mongolite_commit_if_auto(db, db->current_txn, &error);
    assert_int_equal(0, rc);
    assert_int_equal(initial_commit_count, g_mock_wtree_state.txn_commit_count);

    mongolite_rollback(db);
    mongolite_close(db);
}

static void test_abort_if_auto_aborts_when_not_in_txn(void **state) {
    (void)state;
    mongolite_db_t *db = create_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    wtree_txn_t *txn = _mongolite_get_write_txn(db, &error);
    assert_non_null(txn);

    int initial_abort_count = g_mock_wtree_state.txn_abort_count;

    /* Should abort since we're not in explicit transaction */
    _mongolite_abort_if_auto(db, txn);
    assert_int_equal(initial_abort_count + 1, g_mock_wtree_state.txn_abort_count);

    mongolite_close(db);
}

/* ============================================================
 * Sync Tests
 * ============================================================ */

static void test_sync_basic(void **state) {
    (void)state;
    mongolite_db_t *db = create_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    int rc = mongolite_sync(db, false, &error);
    assert_int_equal(0, rc);

    rc = mongolite_sync(db, true, &error);  /* force sync */
    assert_int_equal(0, rc);

    mongolite_close(db);
}

static void test_sync_null_db(void **state) {
    (void)state;
    gerror_t error = {0};

    int rc = mongolite_sync(NULL, false, &error);
    assert_int_equal(MONGOLITE_EINVAL, rc);
}

/* ============================================================
 * Error Injection Tests
 * ============================================================ */

static void test_begin_txn_failure(void **state) {
    (void)state;
    mongolite_db_t *db = create_test_db();
    assert_non_null(db);

    /* Inject error for next txn_begin */
    mock_wtree_fail_next_txn_begin(WTREE_TXN_FULL);

    /* Begin should fail */
    int rc = mongolite_begin_transaction(db);
    assert_int_equal(MONGOLITE_ERROR, rc);
    assert_false(db->in_transaction);

    mongolite_close(db);
}

static void test_get_write_txn_null_db(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree_txn_t *txn = _mongolite_get_write_txn(NULL, &error);
    assert_null(txn);
    assert_int_equal(MONGOLITE_EINVAL, error.code);
}

static void test_get_read_txn_null_db(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree_txn_t *txn = _mongolite_get_read_txn(NULL, &error);
    assert_null(txn);
    assert_int_equal(MONGOLITE_EINVAL, error.code);
}

/* ============================================================
 * Transaction Reuse in Explicit Mode
 * ============================================================ */

static void test_get_write_txn_reuses_explicit(void **state) {
    (void)state;
    mongolite_db_t *db = create_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    /* Start explicit transaction */
    int rc = mongolite_begin_transaction(db);
    assert_int_equal(0, rc);
    wtree_txn_t *explicit_txn = db->current_txn;

    /* Getting write txn should return the explicit one */
    wtree_txn_t *txn = _mongolite_get_write_txn(db, &error);
    assert_ptr_equal(explicit_txn, txn);

    mongolite_rollback(db);
    mongolite_close(db);
}

static void test_get_read_txn_reuses_explicit(void **state) {
    (void)state;
    mongolite_db_t *db = create_test_db();
    assert_non_null(db);

    gerror_t error = {0};

    /* Start explicit transaction */
    int rc = mongolite_begin_transaction(db);
    assert_int_equal(0, rc);
    wtree_txn_t *explicit_txn = db->current_txn;

    /* Getting read txn should return the explicit one (write txn can read) */
    wtree_txn_t *txn = _mongolite_get_read_txn(db, &error);
    assert_ptr_equal(explicit_txn, txn);

    mongolite_rollback(db);
    mongolite_close(db);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Basic transaction tests */
        cmocka_unit_test_setup_teardown(test_begin_commit_transaction, setup, teardown),
        cmocka_unit_test_setup_teardown(test_begin_rollback_transaction, setup, teardown),
        cmocka_unit_test_setup_teardown(test_double_begin_fails, setup, teardown),
        cmocka_unit_test_setup_teardown(test_commit_without_begin_fails, setup, teardown),
        cmocka_unit_test_setup_teardown(test_rollback_without_begin_fails, setup, teardown),

        /* Null parameter tests */
        cmocka_unit_test_setup_teardown(test_begin_null_db, setup, teardown),
        cmocka_unit_test_setup_teardown(test_commit_null_db, setup, teardown),
        cmocka_unit_test_setup_teardown(test_rollback_null_db, setup, teardown),

        /* Read transaction pooling */
        cmocka_unit_test_setup_teardown(test_read_txn_pooling, setup, teardown),
        cmocka_unit_test_setup_teardown(test_write_txn_invalidates_read_pool, setup, teardown),

        /* Auto-commit/abort helpers */
        cmocka_unit_test_setup_teardown(test_commit_if_auto_commits_when_not_in_txn, setup, teardown),
        cmocka_unit_test_setup_teardown(test_commit_if_auto_skips_when_in_txn, setup, teardown),
        cmocka_unit_test_setup_teardown(test_abort_if_auto_aborts_when_not_in_txn, setup, teardown),

        /* Sync tests */
        cmocka_unit_test_setup_teardown(test_sync_basic, setup, teardown),
        cmocka_unit_test_setup_teardown(test_sync_null_db, setup, teardown),

        /* Error injection tests */
        cmocka_unit_test_setup_teardown(test_begin_txn_failure, setup, teardown),
        cmocka_unit_test_setup_teardown(test_get_write_txn_null_db, setup, teardown),
        cmocka_unit_test_setup_teardown(test_get_read_txn_null_db, setup, teardown),

        /* Transaction reuse */
        cmocka_unit_test_setup_teardown(test_get_write_txn_reuses_explicit, setup, teardown),
        cmocka_unit_test_setup_teardown(test_get_read_txn_reuses_explicit, setup, teardown),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
