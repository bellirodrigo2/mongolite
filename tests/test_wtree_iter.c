// test_iterator_isolated.c - Isolated test to debug iterator issues

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
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

static void test_iterator_basic_isolated() {
    printf("\n=== Test: Iterator Basic (Isolated) ===\n");
    
    const char *db_path = "./tests/test_iter_basic";
    cleanup_and_setup(db_path);
    
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(db_path, 0, 0, 0, &error);
    if (!db) {
        printf("ERROR: Failed to create database: %s\n", error.message);
        return;
    }
    
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);
    if (!tree) {
        printf("ERROR: Failed to create tree: %s\n", error.message);
        wtree_db_close(db);
        return;
    }
    
    // Insert exactly what the test expects
    printf("Inserting test data...\n");
    int rc;
    rc = wtree_insert_one(tree, "key1", 4, "val1", 5, &error);
    printf("  Insert key1: %s (rc=%d)\n", rc == 0 ? "OK" : error.message, rc);
    
    rc = wtree_insert_one(tree, "key2", 4, "val2", 5, &error);
    printf("  Insert key2: %s (rc=%d)\n", rc == 0 ? "OK" : error.message, rc);
    
    rc = wtree_insert_one(tree, "key3", 4, "val3", 5, &error);
    printf("  Insert key3: %s (rc=%d)\n", rc == 0 ? "OK" : error.message, rc);
    
    // Create iterator
    printf("\nCreating iterator...\n");
    wtree_iterator_t *iter = wtree_iterator_create(tree, &error);
    if (!iter) {
        printf("ERROR: Failed to create iterator: %s\n", error.message);
        wtree_tree_close(tree);
        wtree_db_close(db);
        return;
    }
    
    // Count items forward (exactly as in the test)
    printf("\nCounting forward with first/next...\n");
    int count = 0;
    for (bool ok = wtree_iterator_first(iter); ok; ok = wtree_iterator_next(iter)) {
        if (!wtree_iterator_valid(iter)) {
            printf("  ERROR: Iterator reports invalid at count=%d\n", count);
            break;
        }
        
        const void *key, *val;
        size_t key_size, val_size;
        
        if (wtree_iterator_key(iter, &key, &key_size)) {
            printf("  Item %d key: '", count + 1);
            fwrite(key, 1, key_size, stdout);
            printf("' (size=%zu)\n", key_size);
        }
        
        if (wtree_iterator_value(iter, &val, &val_size)) {
            printf("  Item %d val: '", count + 1);
            fwrite(val, 1, val_size, stdout);
            printf("' (size=%zu)\n", val_size);
        }
        
        count++;
    }
    printf("Forward count: %d (expected: 3)\n", count);
    assert(count == 3);
    
    // Count items backward (exactly as in the test)
    printf("\nCounting backward with last/prev...\n");
    count = 0;
    for (bool ok = wtree_iterator_last(iter); ok; ok = wtree_iterator_prev(iter)) {
        count++;
        printf("  Item %d found\n", count);
    }
    printf("Backward count: %d (expected: 3)\n", count);
    assert(count == 3);
    
    printf("✓ Test passed!\n");
    
    wtree_iterator_close(iter);
    wtree_tree_close(tree);
    wtree_db_close(db);
}

static void test_iterator_seek_isolated() {
    printf("\n=== Test: Iterator Seek (Isolated) ===\n");
    
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
    printf("\nTesting seek to 'bbb'...\n");
    bool ok = wtree_iterator_seek(iter, "bbb", 3);
    printf("  seek('bbb') returned: %s\n", ok ? "true" : "false");
    
    if (ok) {
        const void *key;
        size_t key_size;
        wtree_iterator_key(iter, &key, &key_size);
        
        printf("  Found key: '");
        fwrite(key, 1, key_size, stdout);
        printf("' (size=%zu)\n", key_size);
        
        if (key_size == 3 && memcmp(key, "bbb", 3) == 0) {
            printf("  ✓ Key matches 'bbb'\n");
        } else {
            printf("  ✗ Key does NOT match 'bbb'\n");
            printf("  Expected bytes: 62 62 62\n");
            printf("  Got bytes: ");
            for (size_t i = 0; i < key_size; i++) {
                printf("%02x ", ((unsigned char*)key)[i]);
            }
            printf("\n");
            assert(0);
        }
    } else {
        assert(0);
    }
    
    // Test seek non-existent
    printf("\nTesting seek to 'xyz' (should fail)...\n");
    ok = wtree_iterator_seek(iter, "xyz", 3);
    printf("  seek('xyz') returned: %s\n", ok ? "true" : "false");
    assert(!ok);
    
    // Test seek range
    printf("\nTesting seek_range to 'aab'...\n");
    ok = wtree_iterator_seek_range(iter, "aab", 3);
    printf("  seek_range('aab') returned: %s\n", ok ? "true" : "false");
    
    if (ok) {
        const void *key;
        size_t key_size;
        wtree_iterator_key(iter, &key, &key_size);
        
        printf("  Found key: '");
        fwrite(key, 1, key_size, stdout);
        printf("' (size=%zu)\n", key_size);
        
        if (key_size == 3 && memcmp(key, "bbb", 3) == 0) {
            printf("  ✓ Key matches 'bbb' (next key >= 'aab')\n");
        } else {
            printf("  ✗ Key does NOT match expected 'bbb'\n");
            assert(0);
        }
    }
    
    printf("✓ Test passed!\n");
    
    wtree_iterator_close(iter);
    wtree_tree_close(tree);
    wtree_db_close(db);
}

static void test_iterator_copy_isolated() {
    printf("\n=== Test: Iterator Copy (Isolated) ===\n");
    
    const char *db_path = "./tests/test_iter_copy";
    cleanup_and_setup(db_path);
    
    gerror_t error = {0};
    
    wtree_db_t *db = wtree_db_create(db_path, 0, 0, 0, &error);
    wtree_tree_t *tree = wtree_tree_create(db, NULL, 0, &error);
    
    const char *test_key = "test_key";
    const char *test_val = "test_value";
    
    printf("Inserting: '%s' -> '%s'\n", test_key, test_val);
    wtree_insert_one(tree, test_key, strlen(test_key), 
                    test_val, strlen(test_val) + 1, &error);
    
    wtree_iterator_t *iter = wtree_iterator_create(tree, &error);
    
    printf("Moving to first item...\n");
    // bool ok = wtree_iterator_first(iter);
    // assert(ok);
    
    // Test copy functions
    void *key_copy, *val_copy;
    size_t key_size, val_size;
    
    printf("Testing key_copy...\n");
    ok = wtree_iterator_key_copy(iter, &key_copy, &key_size);
    assert(ok);
    printf("  Copied key: '");
    fwrite(key_copy, 1, key_size, stdout);
    printf("' (size=%zu)\n", key_size);
    
    if (key_size == strlen(test_key) && memcmp(key_copy, test_key, key_size) == 0) {
        printf("  ✓ Key copy matches\n");
    } else {
        printf("  ✗ Key copy does NOT match\n");
        printf("  Expected: '%s' (size=%zu)\n", test_key, strlen(test_key));
        assert(0);
    }
    
    printf("Testing value_copy...\n");
    ok = wtree_iterator_value_copy(iter, &val_copy, &val_size);
    assert(ok);
    printf("  Copied val: '%s' (size=%zu)\n", (char*)val_copy, val_size);
    
    if (strcmp((char*)val_copy, test_val) == 0) {
        printf("  ✓ Value copy matches\n");
    } else {
        printf("  ✗ Value copy does NOT match\n");
        assert(0);
    }
    
    // Close iterator
    wtree_iterator_close(iter);
    
    // Verify copies are still valid after iterator close
    printf("\nVerifying copies after iterator close...\n");
    printf("  Key still valid: '");
    fwrite(key_copy, 1, key_size, stdout);
    printf("'\n");
    printf("  Val still valid: '%s'\n", (char*)val_copy);
    
    free(key_copy);
    free(val_copy);
    
    printf("✓ Test passed!\n");
    
    wtree_tree_close(tree);
    wtree_db_close(db);
}

int main() {
    printf("=== Running Isolated Iterator Tests ===\n");
    
    test_iterator_basic_isolated();
    test_iterator_seek_isolated();
    test_iterator_copy_isolated();
    
    printf("\n=== All isolated tests passed! ===\n");
    return 0;
}
