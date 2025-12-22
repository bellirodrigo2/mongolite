// test_wtree_iter.c - Isolated iterator tests for wtree module (cmocka)

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "wtree.h"
#include "gerror.h"

#ifdef _WIN32
    #include <direct.h>
    #define mkdir_portable(path) _mkdir(path)
#else
    #include <sys/stat.h>
    #define mkdir_portable(path) mkdir(path, 0755)
#endif

static void cleanup_and_setup(const char *db_path) {
    // Clean up
#ifdef _WIN32
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rmdir /s /q \"%s\" 2>nul", db_path);
    system(cmd);
#else
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf \"%s\" 2>/dev/null", db_path);
    system(cmd);
#endif

    // Create fresh
    mkdir_portable("./tests");
    mkdir_portable(db_path);
}

static void test_iterator_basic_isolated(void **state) {
    (void)state;

    const char *db_path = "./tests/test_iter_basic";
    cleanup_and_setup(db_path);

    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(db_path, 0, 0, 0, &error);
    assert_non_null(db);

    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);
    assert_non_null(tree);

    // Insert exactly what the test expects
    int rc;
    rc = wtree_insert_one(tree, "key1", 4, "val1", 5, &error);
    assert_int_equal(0, rc);

    rc = wtree_insert_one(tree, "key2", 4, "val2", 5, &error);
    assert_int_equal(0, rc);

    rc = wtree_insert_one(tree, "key3", 4, "val3", 5, &error);
    assert_int_equal(0, rc);

    // Create iterator
    wtree_iterator_t *iter = wtree_iterator_create(tree, &error);
    assert_non_null(iter);

    // Count items forward (exactly as in the test)
    int count = 0;
    for (bool ok = wtree_iterator_first(iter); ok; ok = wtree_iterator_next(iter)) {
        assert_true(wtree_iterator_valid(iter));
        count++;
    }
    assert_int_equal(3, count);

    // Count items backward (exactly as in the test)
    count = 0;
    for (bool ok = wtree_iterator_last(iter); ok; ok = wtree_iterator_prev(iter)) {
        count++;
    }
    assert_int_equal(3, count);

    wtree_iterator_close(iter);
    wtree_tree_close(tree);
    wtree_db_close(db);
}

static void test_iterator_seek_isolated(void **state) {
    (void)state;

    const char *db_path = "./tests/test_iter_seek";
    cleanup_and_setup(db_path);

    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(db_path, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);

    // Insert test data
    wtree_insert_one(tree, "aaa", 3, "1", 2, &error);
    wtree_insert_one(tree, "bbb", 3, "2", 2, &error);
    wtree_insert_one(tree, "ccc", 3, "3", 2, &error);
    wtree_insert_one(tree, "ddd", 3, "4", 2, &error);

    wtree_iterator_t *iter = wtree_iterator_create(tree, &error);

    // Test exact seek
    bool ok = wtree_iterator_seek(iter, "bbb", 3);
    assert_true(ok);

    const void *key;
    size_t key_size;
    wtree_iterator_key(iter, &key, &key_size);
    assert_int_equal(3, key_size);
    assert_memory_equal("bbb", key, 3);

    // Test seek non-existent
    ok = wtree_iterator_seek(iter, "xyz", 3);
    assert_false(ok);

    // Test seek range
    ok = wtree_iterator_seek_range(iter, "aab", 3);
    assert_true(ok);

    wtree_iterator_key(iter, &key, &key_size);
    assert_int_equal(3, key_size);
    assert_memory_equal("bbb", key, 3);

    wtree_iterator_close(iter);
    wtree_tree_close(tree);
    wtree_db_close(db);
}

static void test_iterator_copy_isolated(void **state) {
    (void)state;

    const char *db_path = "./tests/test_iter_copy";
    cleanup_and_setup(db_path);

    gerror_t error = {0};

    wtree_db_t *db = wtree_db_create(db_path, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);

    const char *test_key = "test_key";
    const char *test_val = "test_value";

    wtree_insert_one(tree, test_key, strlen(test_key),
                    test_val, strlen(test_val) + 1, &error);

    wtree_iterator_t *iter = wtree_iterator_create(tree, &error);

    bool ok = wtree_iterator_first(iter);
    assert_true(ok);

    // Test copy functions
    void *key_copy, *val_copy;
    size_t key_size, val_size;

    ok = wtree_iterator_key_copy(iter, &key_copy, &key_size);
    assert_true(ok);
    assert_int_equal(strlen(test_key), key_size);
    assert_memory_equal(test_key, key_copy, key_size);

    ok = wtree_iterator_value_copy(iter, &val_copy, &val_size);
    assert_true(ok);
    assert_string_equal(test_val, (char*)val_copy);

    // Close iterator
    wtree_iterator_close(iter);

    // Verify copies are still valid after iterator close
    assert_memory_equal(test_key, key_copy, key_size);
    assert_string_equal(test_val, (char*)val_copy);

    free(key_copy);
    free(val_copy);

    wtree_tree_close(tree);
    wtree_db_close(db);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_iterator_basic_isolated),
        cmocka_unit_test(test_iterator_seek_isolated),
        cmocka_unit_test(test_iterator_copy_isolated),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
