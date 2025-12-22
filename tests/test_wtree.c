// test_wtree.c - Unit tests for wtree module (cmocka)

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <errno.h>

#include "wtree.h"
#include "gerror.h"

#ifdef _WIN32
    #include <direct.h>
    #include <windows.h>
    #define mkdir_portable(path) _mkdir(path)
    #define sleep_ms(ms) Sleep(ms)
#else
    #include <dirent.h>
    #define mkdir_portable(path) mkdir(path, 0755)
    #define sleep_ms(ms) usleep((ms) * 1000)
#endif

static const char *TEST_DB_PATH = "./tests/test_wtree_db";

// Helper to create test directory structure
static void create_test_dir(void) {
    mkdir_portable("./tests");
    mkdir_portable(TEST_DB_PATH);
}

// Helper to recursively remove directory and contents
static void remove_directory(const char *path) {
#ifdef _WIN32
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rmdir /s /q \"%s\" 2>nul", path);
    system(cmd);
#else
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf \"%s\" 2>/dev/null", path);
    system(cmd);
#endif
}

// Helper to clean test database
static void cleanup_test_db(void) {
    remove_directory(TEST_DB_PATH);
}

static int setup(void **state) {
    (void)state;
    cleanup_test_db();
    create_test_dir();
#ifdef _WIN32
    sleep_ms(50);
#endif
    return 0;
}

static int teardown(void **state) {
    (void)state;
    cleanup_test_db();
#ifdef _WIN32
    sleep_ms(50);
#endif
    return 0;
}

// ============= Database Tests =============

static void test_db_create_and_close(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    assert_non_null(db);
    assert_int_equal(0, error.code);

    // Check database files exist
    struct stat st;
    char path[256];
    snprintf(path, sizeof(path), "%s/data.mdb", TEST_DB_PATH);
    assert_int_equal(0, stat(path, &st));

    wtree_db_close(db);
}

static void test_db_create_directory_not_exist(void **state) {
    (void)state;
    gerror_t error = {0};

    // Should fail when directory doesn't exist
    wtree_db_t *db = wtree_db_create("./nonexistent_dir", 0, 0, 0, &error);
    assert_null(db);
    assert_int_not_equal(0, error.code);
}

static void test_db_create_invalid_path(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(NULL, 0, 0, 0, &error);
    assert_null(db);
    assert_int_not_equal(0, error.code);
    assert_string_equal("wtree", error.lib);
}

static void test_db_stats(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    assert_non_null(db);

    MDB_stat stat;
    int rc = wtree_db_stats(db, &stat, &error);
    assert_int_equal(0, rc);
    assert_true(stat.ms_psize > 0); // Page size should be positive

    wtree_db_close(db);
}

static void test_db_sync(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    assert_non_null(db);

    int rc = wtree_db_sync(db, false, &error);
    assert_int_equal(0, rc);

    rc = wtree_db_sync(db, true, &error); // Force sync
    assert_int_equal(0, rc);

    wtree_db_close(db);
}

static void test_db_mapsize(void **state) {
    (void)state;
    gerror_t error = {0};
    size_t custom_size = 10 * 1024 * 1024; // 10MB

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, custom_size, 0, 0, &error);
    assert_non_null(db);

    size_t size = wtree_db_get_mapsize(db);
    assert_int_equal(custom_size, size);

    wtree_db_close(db);
}

// ============= Tree Tests =============

static void test_tree_create_and_close(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    assert_non_null(db);

    wtree_tree_t *tree = wtree_tree_create(db, "test_tree", 0, &error);
    assert_non_null(tree);
    assert_int_equal(0, error.code);

    wtree_tree_close(tree);
    wtree_db_close(db);
}

static void test_tree_create_unnamed(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error); // Main DB
    assert_non_null(tree);

    wtree_tree_close(tree);
    wtree_db_close(db);
}

// ============= Basic Operations Tests =============

static void test_insert_and_get(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, "test", 0, &error);

    // Insert
    const char *key = "test_key";
    const char *value = "test_value";
    int rc = wtree_insert_one(tree, key, strlen(key), value, strlen(value) + 1, &error);
    assert_int_equal(0, rc);
    assert_int_equal(0, error.code);

    // Get
    void *got_value;
    size_t got_size;
    rc = wtree_get(tree, key, strlen(key), &got_value, &got_size, &error);
    assert_int_equal(0, rc);
    assert_string_equal(value, (char*)got_value);
    assert_int_equal(strlen(value) + 1, got_size);

    free(got_value);
    wtree_tree_close(tree);
    wtree_db_close(db);
}

static void test_insert_duplicate(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);

    const char *key = "dup_key";
    const char *value1 = "value1";
    const char *value2 = "value2";

    // First insert should succeed
    int rc = wtree_insert_one(tree, key, strlen(key), value1, strlen(value1) + 1, &error);
    assert_int_equal(0, rc);

    // Second insert with same key should fail
    rc = wtree_insert_one(tree, key, strlen(key), value2, strlen(value2) + 1, &error);
    assert_int_not_equal(0, rc);

    wtree_tree_close(tree);
    wtree_db_close(db);
}

static void test_exists_check(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);

    const char *key = "exists_key";
    const char *value = "exists_value";

    // Check non-existent key
    bool exists = wtree_exists(tree, key, strlen(key), &error);
    assert_false(exists);

    // Insert and check again
    wtree_insert_one(tree, key, strlen(key), value, strlen(value) + 1, &error);
    exists = wtree_exists(tree, key, strlen(key), &error);
    assert_true(exists);

    wtree_tree_close(tree);
    wtree_db_close(db);
}

static void test_update_value(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);

    const char *key = "update_key";
    const char *value1 = "original";
    const char *value2 = "updated_longer_value";

    // Insert original
    wtree_insert_one(tree, key, strlen(key), value1, strlen(value1) + 1, &error);

    // Update
    int rc = wtree_update(tree, key, strlen(key), value2, strlen(value2) + 1, &error);
    assert_int_equal(0, rc);

    // Verify update
    void *got_value;
    size_t got_size;
    wtree_get(tree, key, strlen(key), &got_value, &got_size, &error);
    assert_string_equal(value2, (char*)got_value);
    assert_int_equal(strlen(value2) + 1, got_size);

    free(got_value);
    wtree_tree_close(tree);
    wtree_db_close(db);
}

static void test_delete_key(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);

    const char *key = "delete_key";
    const char *value = "delete_value";

    // Insert
    wtree_insert_one(tree, key, strlen(key), value, strlen(value) + 1, &error);
    assert_true(wtree_exists(tree, key, strlen(key), &error));

    // Delete
    bool deleted = false;
    int rc = wtree_delete_one(tree, key, strlen(key), &deleted, &error);
    assert_int_equal(0, rc);
    assert_true(deleted);

    // Verify deletion
    assert_false(wtree_exists(tree, key, strlen(key), &error));

    // Delete non-existent key should succeed but deleted=false
    deleted = false;
    rc = wtree_delete_one(tree, key, strlen(key), &deleted, &error);
    assert_int_equal(0, rc);
    assert_false(deleted);

    wtree_tree_close(tree);
    wtree_db_close(db);
}

// ============= Transaction Tests =============

static void test_transaction_basic(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);

    // Begin write transaction
    wtree_txn_t *txn = wtree_txn_begin(db, true, &error);
    assert_non_null(txn);
    assert_false(wtree_txn_is_readonly(txn));

    // Insert with transaction
    const char *key = "txn_key";
    const char *value = "txn_value";
    int rc = wtree_insert_one_txn(txn, tree, key, strlen(key),
                                  value, strlen(value) + 1, &error);
    assert_int_equal(0, rc);

    // Commit
    rc = wtree_txn_commit(txn, &error);
    assert_int_equal(0, rc);

    // Verify data persisted
    assert_true(wtree_exists(tree, key, strlen(key), &error));

    wtree_tree_close(tree);
    wtree_db_close(db);
}

static void test_transaction_abort(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);

    // Insert something first
    const char *key = "abort_key";
    const char *value1 = "initial";
    wtree_insert_one(tree, key, strlen(key), value1, strlen(value1) + 1, &error);

    // Begin transaction and update
    wtree_txn_t *txn = wtree_txn_begin(db, true, &error);
    const char *value2 = "updated_but_aborted";
    wtree_update_txn(txn, tree, key, strlen(key),
                    value2, strlen(value2) + 1, &error);

    // Abort transaction
    wtree_txn_abort(txn);

    // Verify original value remains
    void *got_value;
    size_t got_size;
    wtree_get(tree, key, strlen(key), &got_value, &got_size, &error);
    assert_string_equal(value1, (char*)got_value);

    free(got_value);
    wtree_tree_close(tree);
    wtree_db_close(db);
}

static void test_transaction_readonly(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);

    // Insert test data
    const char *key = "ro_key";
    const char *value = "ro_value";
    wtree_insert_one(tree, key, strlen(key), value, strlen(value) + 1, &error);

    // Begin read-only transaction
    wtree_txn_t *txn = wtree_txn_begin(db, false, &error);
    assert_non_null(txn);
    assert_true(wtree_txn_is_readonly(txn));

    // Read with transaction (zero-copy)
    const void *got_value;
    size_t got_size;
    int rc = wtree_get_txn(txn, tree, key, strlen(key), &got_value, &got_size, &error);
    assert_int_equal(0, rc);
    assert_string_equal(value, (char*)got_value);

    // Check exists with transaction
    assert_true(wtree_exists_txn(txn, tree, key, strlen(key), &error));

    wtree_txn_abort(txn);  // Read-only can be aborted
    wtree_tree_close(tree);
    wtree_db_close(db);
}

static void test_transaction_batch_insert(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);

    // Prepare batch data
    wtree_kv_t kvs[] = {
        {.key = "batch1", .key_size = 6, .value = "val1", .value_size = 5},
        {.key = "batch2", .key_size = 6, .value = "val2", .value_size = 5},
        {.key = "batch3", .key_size = 6, .value = "val3", .value_size = 5},
        {.key = "batch4", .key_size = 6, .value = "val4", .value_size = 5}
    };

    // Insert batch in single transaction
    wtree_txn_t *txn = wtree_txn_begin(db, true, &error);
    int rc = wtree_insert_many_txn(txn, tree, kvs, 4, &error);
    assert_int_equal(0, rc);
    rc = wtree_txn_commit(txn, &error);
    assert_int_equal(0, rc);

    // Verify all inserted
    assert_true(wtree_exists(tree, "batch1", 6, &error));
    assert_true(wtree_exists(tree, "batch2", 6, &error));
    assert_true(wtree_exists(tree, "batch3", 6, &error));
    assert_true(wtree_exists(tree, "batch4", 6, &error));

    wtree_tree_close(tree);
    wtree_db_close(db);
}

static void test_transaction_batch_delete(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);

    // Insert test data
    wtree_insert_one(tree, "del1", 4, "v1", 3, &error);
    wtree_insert_one(tree, "del2", 4, "v2", 3, &error);
    wtree_insert_one(tree, "del3", 4, "v3", 3, &error);

    // Delete batch
    const void *keys[] = {"del1", "del2", "del3", "nonexistent"};
    size_t key_sizes[] = {4, 4, 4, 11};
    size_t deleted_count = 0;

    wtree_txn_t *txn = wtree_txn_begin(db, true, &error);
    int rc = wtree_delete_many_txn(txn, tree, keys, key_sizes, 4, &deleted_count, &error);
    assert_int_equal(0, rc);
    assert_int_equal(3, deleted_count); // Only 3 existed
    rc = wtree_txn_commit(txn, &error);
    assert_int_equal(0, rc);

    // Verify deletion
    assert_false(wtree_exists(tree, "del1", 4, &error));
    assert_false(wtree_exists(tree, "del2", 4, &error));
    assert_false(wtree_exists(tree, "del3", 4, &error));

    wtree_tree_close(tree);
    wtree_db_close(db);
}

// ============= Iterator Tests =============

static void test_iterator_basic(void **state) {
    (void)state;
    gerror_t error = {0};

    // Extra cleanup for this specific test
    cleanup_test_db();
    create_test_dir();
#ifdef _WIN32
    sleep_ms(100);
#endif

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);

    // Insert multiple items
    wtree_insert_one(tree, "key1", 4, "val1", 5, &error);
    wtree_insert_one(tree, "key2", 4, "val2", 5, &error);
    wtree_insert_one(tree, "key3", 4, "val3", 5, &error);

    // Create iterator
    wtree_iterator_t *iter = wtree_iterator_create(tree, &error);
    assert_non_null(iter);

    // Count items forward
    int count = 0;
    for (bool ok = wtree_iterator_first(iter); ok; ok = wtree_iterator_next(iter)) {
        assert_true(wtree_iterator_valid(iter));
        count++;
    }
    assert_int_equal(3, count);

    // Count items backward
    count = 0;
    for (bool ok = wtree_iterator_last(iter); ok; ok = wtree_iterator_prev(iter)) {
        count++;
    }
    assert_int_equal(3, count);

    wtree_iterator_close(iter);
    wtree_tree_close(tree);
    wtree_db_close(db);
}

static void test_iterator_seek(void **state) {
    (void)state;
    gerror_t error = {0};

    // Extra cleanup for this specific test
    cleanup_test_db();
    create_test_dir();
#ifdef _WIN32
    sleep_ms(100);
#endif

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);

    wtree_insert_one(tree, "aaa", 3, "1", 2, &error);
    wtree_insert_one(tree, "bbb", 3, "2", 2, &error);
    wtree_insert_one(tree, "ccc", 3, "3", 2, &error);
    wtree_insert_one(tree, "ddd", 3, "4", 2, &error);

    wtree_iterator_t *iter = wtree_iterator_create(tree, &error);

    // Seek exact
    assert_true(wtree_iterator_seek(iter, "bbb", 3));
    const void *key;
    size_t key_size;
    wtree_iterator_key(iter, &key, &key_size);
    assert_memory_equal("bbb", key, 3);

    // Seek non-existent exact
    assert_false(wtree_iterator_seek(iter, "xyz", 3));

    // Seek range (between aaa and bbb)
    assert_true(wtree_iterator_seek_range(iter, "aab", 3));
    wtree_iterator_key(iter, &key, &key_size);
    assert_memory_equal("bbb", key, 3);

    // Seek range (after all)
    assert_false(wtree_iterator_seek_range(iter, "zzz", 3));

    wtree_iterator_close(iter);
    wtree_tree_close(tree);
    wtree_db_close(db);
}

static void test_iterator_get_copy(void **state) {
    (void)state;
    gerror_t error = {0};

    // Extra cleanup for this specific test
    cleanup_test_db();
    create_test_dir();
#ifdef _WIN32
    sleep_ms(100);
#endif

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);

    const char *test_key = "test_key";
    const char *test_val = "test_value";
    wtree_insert_one(tree, test_key, strlen(test_key),
                    test_val, strlen(test_val) + 1, &error);

    wtree_iterator_t *iter = wtree_iterator_create(tree, &error);
    assert_true(wtree_iterator_first(iter));

    // Get copies (safe after iterator close)
    void *key_copy, *val_copy;
    size_t key_size, val_size;
    assert_true(wtree_iterator_key_copy(iter, &key_copy, &key_size));
    assert_true(wtree_iterator_value_copy(iter, &val_copy, &val_size));

    // Close iterator
    wtree_iterator_close(iter);

    // Copies should still be valid
    assert_memory_equal(test_key, key_copy, strlen(test_key));
    assert_string_equal(test_val, (char*)val_copy);

    free(key_copy);
    free(val_copy);
    wtree_tree_close(tree);
    wtree_db_close(db);
}

static void test_iterator_with_txn(void **state) {
    (void)state;
    gerror_t error = {0};

    // Extra cleanup for this specific test
    cleanup_test_db();
    create_test_dir();
#ifdef _WIN32
    sleep_ms(100);
#endif

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);

    // Insert initial data
    wtree_insert_one(tree, "a", 1, "1", 2, &error);
    wtree_insert_one(tree, "b", 1, "2", 2, &error);

    // Create read transaction and iterator
    wtree_txn_t *txn = wtree_txn_begin(db, false, &error);
    wtree_iterator_t *iter = wtree_iterator_create_with_txn(tree, txn, &error);
    assert_non_null(iter);

    // Count items
    int count = 0;
    for (bool ok = wtree_iterator_first(iter); ok; ok = wtree_iterator_next(iter)) {
        count++;
    }
    assert_int_equal(2, count);

    // Iterator doesn't own transaction
    wtree_iterator_close(iter);
    wtree_txn_abort(txn);

    wtree_tree_close(tree);
    wtree_db_close(db);
}

// ============= Error Handling Tests =============

static void test_error_handling(void **state) {
    (void)state;

    // Test recoverable error detection
    assert_true(wtree_error_recoverable(WTREE_MAP_FULL));
    assert_true(wtree_error_recoverable(WTREE_TXN_FULL));
    assert_false(wtree_error_recoverable(EINVAL));

    // Test error strings
    const char *msg = wtree_strerror(WTREE_MAP_FULL);
    assert_non_null(msg);
    assert_true(strlen(msg) > 0);
}

// ============= Binary Data Test =============

static void test_binary_data(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);

    // Binary key and value with null bytes
    unsigned char binary_key[] = {0x00, 0x01, 0x02, 0x03, 0x04};
    unsigned char binary_val[] = {0xFF, 0xFE, 0x00, 0x00, 0xAB, 0xCD};

    // Insert binary data
    int rc = wtree_insert_one(tree, binary_key, sizeof(binary_key),
                             binary_val, sizeof(binary_val), &error);
    assert_int_equal(0, rc);

    // Retrieve binary data
    void *got_val;
    size_t got_size;
    rc = wtree_get(tree, binary_key, sizeof(binary_key), &got_val, &got_size, &error);
    assert_int_equal(0, rc);
    assert_int_equal(sizeof(binary_val), got_size);
    assert_memory_equal(binary_val, got_val, sizeof(binary_val));

    free(got_val);
    wtree_tree_close(tree);
    wtree_db_close(db);
}

static void test_txn_reset_and_renew_readonly(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    assert_non_null(db);

    // read-only txn
    wtree_txn_t *txn = wtree_txn_begin(db, false, &error);
    assert_non_null(txn);

    // reset should be safe
    wtree_txn_reset(txn);

    // renew should work
    int rc = wtree_txn_renew(txn, &error);
    assert_int_equal(0, rc);

    wtree_txn_abort(txn);
    wtree_db_close(db);
}

// ============= Main Test Suite =============

int main(void) {
    const struct CMUnitTest tests[] = {
        // Database tests
        cmocka_unit_test_setup_teardown(test_db_create_and_close, setup, teardown),
        cmocka_unit_test_setup_teardown(test_db_create_directory_not_exist, setup, teardown),
        cmocka_unit_test_setup_teardown(test_db_create_invalid_path, setup, teardown),
        cmocka_unit_test_setup_teardown(test_db_stats, setup, teardown),
        cmocka_unit_test_setup_teardown(test_db_sync, setup, teardown),
        cmocka_unit_test_setup_teardown(test_db_mapsize, setup, teardown),

        // Tree tests
        cmocka_unit_test_setup_teardown(test_tree_create_and_close, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tree_create_unnamed, setup, teardown),

        // Basic operations
        cmocka_unit_test_setup_teardown(test_insert_and_get, setup, teardown),
        cmocka_unit_test_setup_teardown(test_insert_duplicate, setup, teardown),
        cmocka_unit_test_setup_teardown(test_exists_check, setup, teardown),
        cmocka_unit_test_setup_teardown(test_update_value, setup, teardown),
        cmocka_unit_test_setup_teardown(test_delete_key, setup, teardown),

        // Transaction tests
        cmocka_unit_test_setup_teardown(test_transaction_basic, setup, teardown),
        cmocka_unit_test_setup_teardown(test_transaction_abort, setup, teardown),
        cmocka_unit_test_setup_teardown(test_transaction_readonly, setup, teardown),
        cmocka_unit_test_setup_teardown(test_transaction_batch_insert, setup, teardown),
        cmocka_unit_test_setup_teardown(test_transaction_batch_delete, setup, teardown),

        // Iterator tests
        cmocka_unit_test_setup_teardown(test_iterator_basic, setup, teardown),
        cmocka_unit_test_setup_teardown(test_iterator_seek, setup, teardown),
        cmocka_unit_test_setup_teardown(test_iterator_get_copy, setup, teardown),
        cmocka_unit_test_setup_teardown(test_iterator_with_txn, setup, teardown),

        // Other tests
        cmocka_unit_test(test_error_handling),
        cmocka_unit_test_setup_teardown(test_binary_data, setup, teardown),
        cmocka_unit_test_setup_teardown(test_txn_reset_and_renew_readonly, setup, teardown),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
