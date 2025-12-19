// test_wtree.c - Unit tests for wtree module

#include "test_runner.h"
#include "wtree.h"
#include "gerror.h"
#include <sys/stat.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

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
    // First ensure tests directory exists
    mkdir_portable("./tests");
    // Then create the test database directory
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

// Setup before tests
static void setUp(void) {
    cleanup_test_db();
    create_test_dir();
    
    // Give OS time to release resources
#ifdef _WIN32
    sleep_ms(50);
#endif
}

// Teardown after tests
static void tearDown(void) {
    cleanup_test_db();
    
    // Give OS time to release resources
#ifdef _WIN32
    sleep_ms(50);
#endif
}

// ============= Database Tests =============

TEST(db_create_and_close) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    TEST_ASSERT_NOT_NULL(db);
    TEST_ASSERT_EQUAL(0, error.code);
    
    // Check database files exist
    struct stat st;
    char path[256];
    snprintf(path, sizeof(path), "%s/data.mdb", TEST_DB_PATH);
    TEST_ASSERT_EQUAL(0, stat(path, &st));
    
    wtree_db_close(db);
    return 0;
}

TEST(db_create_directory_not_exist) {
    gerror_t error = {0};
    
    // Should fail when directory doesn't exist
    wtree_db_t *db = wtree_db_create("./nonexistent_dir", 0, 0, 0, &error);
    TEST_ASSERT_NULL(db);
    TEST_ASSERT_NOT_EQUAL(0, error.code);
    
    return 0;
}

TEST(db_create_invalid_path) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(NULL, 0, 0, 0, &error);
    TEST_ASSERT_NULL(db);
    TEST_ASSERT_NOT_EQUAL(0, error.code);
    TEST_ASSERT_EQUAL_STRING("wtree", error.lib);
    
    return 0;
}

TEST(db_stats) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    TEST_ASSERT_NOT_NULL(db);
    
    MDB_stat stat;
    int rc = wtree_db_stats(db, &stat, &error);
    TEST_ASSERT_EQUAL(0, rc);
    TEST_ASSERT_TRUE(stat.ms_psize > 0); // Page size should be positive
    
    wtree_db_close(db);
    return 0;
}

TEST(db_sync) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    TEST_ASSERT_NOT_NULL(db);
    
    int rc = wtree_db_sync(db, false, &error);
    TEST_ASSERT_EQUAL(0, rc);
    
    rc = wtree_db_sync(db, true, &error); // Force sync
    TEST_ASSERT_EQUAL(0, rc);
    
    wtree_db_close(db);
    return 0;
}

TEST(db_mapsize) {
    gerror_t error = {0};
    size_t custom_size = 10 * 1024 * 1024; // 10MB
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, custom_size, 0, 0, &error);
    TEST_ASSERT_NOT_NULL(db);
    
    size_t size = wtree_db_get_mapsize(db);
    TEST_ASSERT_EQUAL(custom_size, size);
    
    wtree_db_close(db);
    return 0;
}

// ============= Tree Tests =============

TEST(tree_create_and_close) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    TEST_ASSERT_NOT_NULL(db);
    
    wtree_tree_t *tree = wtree_tree_create(db, "test_tree", 0, &error);
    TEST_ASSERT_NOT_NULL(tree);
    TEST_ASSERT_EQUAL(0, error.code);
    
    wtree_tree_close(tree);
    wtree_db_close(db);
    return 0;
}

TEST(tree_create_unnamed) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error); // Main DB
    TEST_ASSERT_NOT_NULL(tree);
    
    wtree_tree_close(tree);
    wtree_db_close(db);
    return 0;
}

// ============= Basic Operations Tests =============

TEST(insert_and_get) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, "test", 0, &error);
    
    // Insert
    const char *key = "test_key";
    const char *value = "test_value";
    int rc = wtree_insert_one(tree, key, strlen(key), value, strlen(value) + 1, &error);
    TEST_ASSERT_EQUAL(0, rc);
    TEST_ASSERT_EQUAL(0, error.code);
    
    // Get
    void *got_value;
    size_t got_size;
    rc = wtree_get(tree, key, strlen(key), &got_value, &got_size, &error);
    TEST_ASSERT_EQUAL(0, rc);
    TEST_ASSERT_EQUAL_STRING(value, (char*)got_value);
    TEST_ASSERT_EQUAL(strlen(value) + 1, got_size);
    
    free(got_value);
    wtree_tree_close(tree);
    wtree_db_close(db);
    return 0;
}

TEST(insert_duplicate) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);
    
    const char *key = "dup_key";
    const char *value1 = "value1";
    const char *value2 = "value2";
    
    // First insert should succeed
    int rc = wtree_insert_one(tree, key, strlen(key), value1, strlen(value1) + 1, &error);
    TEST_ASSERT_EQUAL(0, rc);
    
    // Second insert with same key should fail
    rc = wtree_insert_one(tree, key, strlen(key), value2, strlen(value2) + 1, &error);
    TEST_ASSERT_NOT_EQUAL(0, rc);
    
    wtree_tree_close(tree);
    wtree_db_close(db);
    return 0;
}

TEST(exists_check) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);
    
    const char *key = "exists_key";
    const char *value = "exists_value";
    
    // Check non-existent key
    bool exists = wtree_exists(tree, key, strlen(key), &error);
    TEST_ASSERT_FALSE(exists);
    
    // Insert and check again
    wtree_insert_one(tree, key, strlen(key), value, strlen(value) + 1, &error);
    exists = wtree_exists(tree, key, strlen(key), &error);
    TEST_ASSERT_TRUE(exists);
    
    wtree_tree_close(tree);
    wtree_db_close(db);
    return 0;
}

TEST(update_value) {
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
    TEST_ASSERT_EQUAL(0, rc);
    
    // Verify update
    void *got_value;
    size_t got_size;
    wtree_get(tree, key, strlen(key), &got_value, &got_size, &error);
    TEST_ASSERT_EQUAL_STRING(value2, (char*)got_value);
    TEST_ASSERT_EQUAL(strlen(value2) + 1, got_size);
    
    free(got_value);
    wtree_tree_close(tree);
    wtree_db_close(db);
    return 0;
}

TEST(delete_key) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);
    
    const char *key = "delete_key";
    const char *value = "delete_value";
    
    // Insert
    wtree_insert_one(tree, key, strlen(key), value, strlen(value) + 1, &error);
    TEST_ASSERT_TRUE(wtree_exists(tree, key, strlen(key), &error));
    
    // Delete
    bool deleted = false;
    int rc = wtree_delete_one(tree, key, strlen(key), &deleted, &error);
    TEST_ASSERT_EQUAL(0, rc);
    TEST_ASSERT_TRUE(deleted);
    
    // Verify deletion
    TEST_ASSERT_FALSE(wtree_exists(tree, key, strlen(key), &error));
    
    // Delete non-existent key should succeed but deleted=false
    deleted = false;
    rc = wtree_delete_one(tree, key, strlen(key), &deleted, &error);
    TEST_ASSERT_EQUAL(0, rc);
    TEST_ASSERT_FALSE(deleted);
    
    wtree_tree_close(tree);
    wtree_db_close(db);
    return 0;
}

// ============= Transaction Tests =============

TEST(transaction_basic) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);
    
    // Begin write transaction
    wtree_txn_t *txn = wtree_txn_begin(db, true, &error);
    TEST_ASSERT_NOT_NULL(txn);
    TEST_ASSERT_FALSE(wtree_txn_is_readonly(txn));
    
    // Insert with transaction
    const char *key = "txn_key";
    const char *value = "txn_value";
    int rc = wtree_insert_one_txn(txn, tree, key, strlen(key), 
                                  value, strlen(value) + 1, &error);
    TEST_ASSERT_EQUAL(0, rc);
    
    // Commit
    rc = wtree_txn_commit(txn, &error);
    TEST_ASSERT_EQUAL(0, rc);
    
    // Verify data persisted
    TEST_ASSERT_TRUE(wtree_exists(tree, key, strlen(key), &error));
    
    wtree_tree_close(tree);
    wtree_db_close(db);
    return 0;
}

TEST(transaction_abort) {
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
    TEST_ASSERT_EQUAL_STRING(value1, (char*)got_value);
    
    free(got_value);
    wtree_tree_close(tree);
    wtree_db_close(db);
    return 0;
}

TEST(transaction_readonly) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);
    
    // Insert test data
    const char *key = "ro_key";
    const char *value = "ro_value";
    wtree_insert_one(tree, key, strlen(key), value, strlen(value) + 1, &error);
    
    // Begin read-only transaction
    wtree_txn_t *txn = wtree_txn_begin(db, false, &error);
    TEST_ASSERT_NOT_NULL(txn);
    TEST_ASSERT_TRUE(wtree_txn_is_readonly(txn));
    
    // Read with transaction (zero-copy)
    const void *got_value;
    size_t got_size;
    int rc = wtree_get_txn(txn, tree, key, strlen(key), &got_value, &got_size, &error);
    TEST_ASSERT_EQUAL(0, rc);
    TEST_ASSERT_EQUAL_STRING(value, (char*)got_value);
    
    // Check exists with transaction
    TEST_ASSERT_TRUE(wtree_exists_txn(txn, tree, key, strlen(key), &error));
    
    wtree_txn_abort(txn);  // Read-only can be aborted
    wtree_tree_close(tree);
    wtree_db_close(db);
    return 0;
}

TEST(transaction_batch_insert) {
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
    TEST_ASSERT_EQUAL(0, rc);
    rc = wtree_txn_commit(txn, &error);
    TEST_ASSERT_EQUAL(0, rc);
    
    // Verify all inserted
    TEST_ASSERT_TRUE(wtree_exists(tree, "batch1", 6, &error));
    TEST_ASSERT_TRUE(wtree_exists(tree, "batch2", 6, &error));
    TEST_ASSERT_TRUE(wtree_exists(tree, "batch3", 6, &error));
    TEST_ASSERT_TRUE(wtree_exists(tree, "batch4", 6, &error));
    
    wtree_tree_close(tree);
    wtree_db_close(db);
    return 0;
}

TEST(transaction_batch_delete) {
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
    TEST_ASSERT_EQUAL(0, rc);
    TEST_ASSERT_EQUAL(3, deleted_count); // Only 3 existed
    rc = wtree_txn_commit(txn, &error);
    TEST_ASSERT_EQUAL(0, rc);
    
    // Verify deletion
    TEST_ASSERT_FALSE(wtree_exists(tree, "del1", 4, &error));
    TEST_ASSERT_FALSE(wtree_exists(tree, "del2", 4, &error));
    TEST_ASSERT_FALSE(wtree_exists(tree, "del3", 4, &error));
    
    wtree_tree_close(tree);
    wtree_db_close(db);
    return 0;
}

// ============= Iterator Tests =============

TEST(iterator_basic) {
    gerror_t error = {0};
    
    // Extra cleanup for this specific test
    cleanup_test_db();
    create_test_dir();
#ifdef _WIN32
    sleep_ms(100);  // Windows needs more time to release locks
#endif
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);
    
    // Insert multiple items
    wtree_insert_one(tree, "key1", 4, "val1", 5, &error);
    wtree_insert_one(tree, "key2", 4, "val2", 5, &error);
    wtree_insert_one(tree, "key3", 4, "val3", 5, &error);
    
    // Create iterator
    wtree_iterator_t *iter = wtree_iterator_create(tree, &error);
    TEST_ASSERT_NOT_NULL(iter);
    
    // Count items forward
    int count = 0;
    for (bool ok = wtree_iterator_first(iter); ok; ok = wtree_iterator_next(iter)) {
        TEST_ASSERT_TRUE(wtree_iterator_valid(iter));
        count++;
    }
    TEST_ASSERT_EQUAL(3, count);
    
    // Count items backward
    count = 0;
    for (bool ok = wtree_iterator_last(iter); ok; ok = wtree_iterator_prev(iter)) {
        count++;
    }
    TEST_ASSERT_EQUAL(3, count);
    
    wtree_iterator_close(iter);
    wtree_tree_close(tree);
    wtree_db_close(db);
    return 0;
}

TEST(iterator_seek) {
    gerror_t error = {0};
    
    // Extra cleanup for this specific test
    cleanup_test_db();
    create_test_dir();
#ifdef _WIN32
    sleep_ms(100);  // Windows needs more time to release locks
#endif
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);
    
    wtree_insert_one(tree, "aaa", 3, "1", 2, &error);
    wtree_insert_one(tree, "bbb", 3, "2", 2, &error);
    wtree_insert_one(tree, "ccc", 3, "3", 2, &error);
    wtree_insert_one(tree, "ddd", 3, "4", 2, &error);
    
    wtree_iterator_t *iter = wtree_iterator_create(tree, &error);
    
    // Seek exact
    TEST_ASSERT_TRUE(wtree_iterator_seek(iter, "bbb", 3));
    const void *key;
    size_t key_size;
    wtree_iterator_key(iter, &key, &key_size);
    TEST_ASSERT_EQUAL_MEMORY("bbb", key, 3);
    
    // Seek non-existent exact
    TEST_ASSERT_FALSE(wtree_iterator_seek(iter, "xyz", 3));
    
    // Seek range (between aaa and bbb)
    TEST_ASSERT_TRUE(wtree_iterator_seek_range(iter, "aab", 3));
    wtree_iterator_key(iter, &key, &key_size);
    TEST_ASSERT_EQUAL_MEMORY("bbb", key, 3);
    
    // Seek range (after all)
    TEST_ASSERT_FALSE(wtree_iterator_seek_range(iter, "zzz", 3));
    
    wtree_iterator_close(iter);
    wtree_tree_close(tree);
    wtree_db_close(db);
    return 0;
}

TEST(iterator_get_copy) {
    gerror_t error = {0};
    
    // Extra cleanup for this specific test
    cleanup_test_db();
    create_test_dir();
#ifdef _WIN32
    sleep_ms(100);  // Windows needs more time to release locks
#endif
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);
    
    const char *test_key = "test_key";
    const char *test_val = "test_value";
    wtree_insert_one(tree, test_key, strlen(test_key), 
                    test_val, strlen(test_val) + 1, &error);
    
    wtree_iterator_t *iter = wtree_iterator_create(tree, &error);
    TEST_ASSERT_TRUE(wtree_iterator_first(iter));
    
    // Get copies (safe after iterator close)
    void *key_copy, *val_copy;
    size_t key_size, val_size;
    TEST_ASSERT_TRUE(wtree_iterator_key_copy(iter, &key_copy, &key_size));
    TEST_ASSERT_TRUE(wtree_iterator_value_copy(iter, &val_copy, &val_size));
    
    // Close iterator
    wtree_iterator_close(iter);
    
    // Copies should still be valid
    TEST_ASSERT_EQUAL_MEMORY(test_key, key_copy, strlen(test_key));
    TEST_ASSERT_EQUAL_STRING(test_val, (char*)val_copy);
    
    free(key_copy);
    free(val_copy);
    wtree_tree_close(tree);
    wtree_db_close(db);
    return 0;
}

TEST(iterator_with_txn) {
    gerror_t error = {0};
    
    // Extra cleanup for this specific test
    cleanup_test_db();
    create_test_dir();
#ifdef _WIN32
    sleep_ms(100);  // Windows needs more time to release locks
#endif
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);
    
    // Insert initial data
    wtree_insert_one(tree, "a", 1, "1", 2, &error);
    wtree_insert_one(tree, "b", 1, "2", 2, &error);
    
    // Create read transaction and iterator
    wtree_txn_t *txn = wtree_txn_begin(db, false, &error);
    wtree_iterator_t *iter = wtree_iterator_create_with_txn(tree, txn, &error);
    TEST_ASSERT_NOT_NULL(iter);
    
    // Count items
    int count = 0;
    for (bool ok = wtree_iterator_first(iter); ok; ok = wtree_iterator_next(iter)) {
        count++;
    }
    TEST_ASSERT_EQUAL(2, count);
    
    // Iterator doesn't own transaction
    wtree_iterator_close(iter);
    wtree_txn_abort(txn);
    
    wtree_tree_close(tree);
    wtree_db_close(db);
    return 0;
}

// ============= Error Handling Tests =============

TEST(error_handling) {
    // Test recoverable error detection
    TEST_ASSERT_TRUE(wtree_error_recoverable(WTREE_MAP_FULL));
    TEST_ASSERT_TRUE(wtree_error_recoverable(WTREE_TXN_FULL));
    TEST_ASSERT_FALSE(wtree_error_recoverable(EINVAL));
    
    // Test error strings
    const char *msg = wtree_strerror(WTREE_MAP_FULL);
    TEST_ASSERT_NOT_NULL(msg);
    TEST_ASSERT_TRUE(strlen(msg) > 0);
    
    return 0;
}

// ============= Binary Data Test =============

TEST(binary_data) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);
    
    // Binary key and value with null bytes
    unsigned char binary_key[] = {0x00, 0x01, 0x02, 0x03, 0x04};
    unsigned char binary_val[] = {0xFF, 0xFE, 0x00, 0x00, 0xAB, 0xCD};
    
    // Insert binary data
    int rc = wtree_insert_one(tree, binary_key, sizeof(binary_key), 
                             binary_val, sizeof(binary_val), &error);
    TEST_ASSERT_EQUAL(0, rc);
    
    // Retrieve binary data
    void *got_val;
    size_t got_size;
    rc = wtree_get(tree, binary_key, sizeof(binary_key), &got_val, &got_size, &error);
    TEST_ASSERT_EQUAL(0, rc);
    TEST_ASSERT_EQUAL(sizeof(binary_val), got_size);
    TEST_ASSERT_EQUAL_MEMORY(binary_val, got_val, sizeof(binary_val));
    
    free(got_val);
    wtree_tree_close(tree);
    wtree_db_close(db);
    return 0;
}
TEST(txn_reset_and_renew_readonly) {
    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, 0, &error);
    TEST_ASSERT_NOT_NULL(db);

    // read-only txn
    wtree_txn_t *txn = wtree_txn_begin(db, false, &error);
    TEST_ASSERT_NOT_NULL(txn);

    // reset deve ser seguro
    wtree_txn_reset(txn);

    // renew deve funcionar
    int rc = wtree_txn_renew(txn, &error);
    TEST_ASSERT_EQUAL(0, rc);

    wtree_txn_abort(txn);
    wtree_db_close(db);
    return 0;
}


// ============= Main Test Suite =============

static void final_cleanup(void) {
    // Ensure all test artifacts are removed
    cleanup_test_db();
    
    // Also try to remove the tests directory if it's empty
#ifndef _WIN32
    rmdir("./tests");  // Will fail silently if not empty
#endif
}

TEST_SUITE_BEGIN("wtree tests")
    setUp();
    
    // Database tests
    RUN_TEST(test_db_create_and_close);
    RUN_TEST(test_db_create_directory_not_exist);
    RUN_TEST(test_db_create_invalid_path);
    RUN_TEST(test_db_stats);
    RUN_TEST(test_db_sync);
    RUN_TEST(test_db_mapsize);
    
    // Tree tests
    RUN_TEST(test_tree_create_and_close);
    RUN_TEST(test_tree_create_unnamed);
    
    // Basic operations
    RUN_TEST(test_insert_and_get);
    RUN_TEST(test_insert_duplicate);
    RUN_TEST(test_exists_check);
    RUN_TEST(test_update_value);
    RUN_TEST(test_delete_key);
    
    // Transaction tests
    RUN_TEST(test_transaction_basic);
    RUN_TEST(test_transaction_abort);
    RUN_TEST(test_transaction_readonly);
    RUN_TEST(test_transaction_batch_insert);
    RUN_TEST(test_transaction_batch_delete);
    
    // Iterator tests
    RUN_TEST(test_iterator_basic);
    RUN_TEST(test_iterator_seek);
    RUN_TEST(test_iterator_get_copy);
    RUN_TEST(test_iterator_with_txn);
    
    // Other tests
    RUN_TEST(test_error_handling);
    RUN_TEST(test_binary_data);

    // Transaction reset and renew test
    RUN_TEST(test_txn_reset_and_renew_readonly);
    RUN_TEST(test_iterator_with_txn);
    
    tearDown();
    final_cleanup();  // Final cleanup
TEST_SUITE_END()
