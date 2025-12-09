// test_wtree.c - Unit tests for wtree module

#include "test_runner.h"
#include "wtree.h"
#include "gerror.h"
// #include <unistd.h>
#include <sys/stat.h>

static const char *TEST_DB_PATH = "./test_wtree_db";

// Helper to clean test database
static void cleanup_test_db(void) {
    wtree_db_delete(TEST_DB_PATH, NULL);
}

// Setup before tests
TEST_SETUP() {
    cleanup_test_db();
}

// Teardown after tests
TEST_TEARDOWN() {
    cleanup_test_db();
}

TEST(db_create_and_close) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, &error);
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

TEST(db_create_invalid_path) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(NULL, 0, 0, &error);
    TEST_ASSERT_NULL(db);
    TEST_ASSERT_NOT_EQUAL(0, error.code);
    TEST_ASSERT_EQUAL_STRING("wtree", error.lib);
    
    return 0;
}

TEST(tree_create_and_close) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, &error);
    TEST_ASSERT_NOT_NULL(db);
    
    wtree_tree_t *tree = wtree_tree_create(db, "test_tree", 0, &error);
    TEST_ASSERT_NOT_NULL(tree);
    TEST_ASSERT_EQUAL(0, error.code);
    
    wtree_tree_close(tree);
    wtree_db_close(db);
    
    return 0;
}

TEST(insert_and_get) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, &error);
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
    
    free(got_value);
    wtree_tree_close(tree);
    wtree_db_close(db);
    
    return 0;
}

TEST(exists_check) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, &error);
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
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);
    
    const char *key = "update_key";
    const char *value1 = "original";
    const char *value2 = "updated";
    
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
    
    free(got_value);
    wtree_tree_close(tree);
    wtree_db_close(db);
    
    return 0;
}

TEST(delete_key) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);
    
    const char *key = "delete_key";
    const char *value = "delete_value";
    
    // Insert
    wtree_insert_one(tree, key, strlen(key), value, strlen(value) + 1, &error);
    TEST_ASSERT_TRUE(wtree_exists(tree, key, strlen(key), &error));
    
    // Delete
    int rc = wtree_delete_one(tree, key, strlen(key), &error);
    TEST_ASSERT_EQUAL(0, rc);
    
    // Verify deletion
    TEST_ASSERT_FALSE(wtree_exists(tree, key, strlen(key), &error));
    
    wtree_tree_close(tree);
    wtree_db_close(db);
    
    return 0;
}

TEST(iterator_basic) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);
    
    // Insert multiple items
    wtree_insert_one(tree, "key1", 4, "val1", 5, &error);
    wtree_insert_one(tree, "key2", 4, "val2", 5, &error);
    wtree_insert_one(tree, "key3", 4, "val3", 5, &error);
    
    // Create iterator
    wtree_iterator_t *iter = wtree_iterator_create(tree, &error);
    TEST_ASSERT_NOT_NULL(iter);
    
    // Count items
    int count = 0;
    for (wtree_iterator_first(iter); wtree_iterator_valid(iter); wtree_iterator_next(iter)) {
        count++;
    }
    TEST_ASSERT_EQUAL(3, count);
    
    // Test last and prev
    TEST_ASSERT_TRUE(wtree_iterator_last(iter));
    TEST_ASSERT_TRUE(wtree_iterator_prev(iter));
    
    wtree_iterator_close(iter);
    wtree_tree_close(tree);
    wtree_db_close(db);
    
    return 0;
}

TEST(iterator_seek) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);
    
    wtree_insert_one(tree, "aaa", 3, "1", 2, &error);
    wtree_insert_one(tree, "bbb", 3, "2", 2, &error);
    wtree_insert_one(tree, "ccc", 3, "3", 2, &error);
    
    wtree_iterator_t *iter = wtree_iterator_create(tree, &error);
    
    // Seek exact
    TEST_ASSERT_TRUE(wtree_iterator_seek(iter, "bbb", 3));
    void *key, *value;
    size_t key_size, value_size;
    wtree_iterator_key(iter, &key, &key_size);
    TEST_ASSERT_EQUAL_MEMORY("bbb", key, 3);
    
    // Seek range
    TEST_ASSERT_TRUE(wtree_iterator_seek_range(iter, "aab", 3));
    wtree_iterator_key(iter, &key, &key_size);
    TEST_ASSERT_EQUAL_MEMORY("bbb", key, 3);
    
    wtree_iterator_close(iter);
    wtree_tree_close(tree);
    wtree_db_close(db);
    
    return 0;
}

TEST(transaction_commit) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);
    
    // Manual transaction
    wtree_txn_t *txn = wtree_txn_begin(db, true, &error);
    TEST_ASSERT_NOT_NULL(txn);
    
    // Operations would go here in real usage
    
    int rc = wtree_txn_commit(txn, &error);
    TEST_ASSERT_EQUAL(0, rc);
    
    wtree_tree_close(tree);
    wtree_db_close(db);
    
    return 0;
}

TEST(transaction_abort) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, &error);
    
    wtree_txn_t *txn = wtree_txn_begin(db, true, &error);
    TEST_ASSERT_NOT_NULL(txn);
    
    // Abort should not crash
    wtree_txn_abort(txn);
    TEST_ASSERT_TRUE(1); // Success if we get here
    
    wtree_db_close(db);
    
    return 0;
}

TEST(db_stats) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, &error);
    
    MDB_stat stat;
    int rc = wtree_db_stats(db, &stat, &error);
    TEST_ASSERT_EQUAL(0, rc);
    TEST_ASSERT_TRUE(stat.ms_psize > 0); // Page size should be positive
    
    wtree_db_close(db);
    
    return 0;
}

TEST(tree_list) {
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(TEST_DB_PATH, 0, 0, &error);
    
    // Create multiple named trees
    wtree_tree_t *tree1 = wtree_tree_create(db, "tree1", 0, &error);
    wtree_tree_t *tree2 = wtree_tree_create(db, "tree2", 0, &error);
    
    size_t count;
    char **list = wtree_tree_list(db, &count, &error);
    
    if (list) {
        TEST_ASSERT_TRUE(count >= 2); // At least our two trees
        wtree_tree_list_free(list, count);
    }
    
    wtree_tree_close(tree1);
    wtree_tree_close(tree2);
    wtree_db_close(db);
    
    return 0;
}

TEST_SUITE_BEGIN("wtree tests")
    setUp();
    
    RUN_TEST(test_db_create_and_close);
    RUN_TEST(test_db_create_invalid_path);
    RUN_TEST(test_tree_create_and_close);
    RUN_TEST(test_insert_and_get);
    RUN_TEST(test_exists_check);
    RUN_TEST(test_update_value);
    RUN_TEST(test_delete_key);
    RUN_TEST(test_iterator_basic);
    RUN_TEST(test_iterator_seek);
    RUN_TEST(test_transaction_commit);
    RUN_TEST(test_transaction_abort);
    RUN_TEST(test_db_stats);
    RUN_TEST(test_tree_list);
    
    tearDown();
TEST_SUITE_END()