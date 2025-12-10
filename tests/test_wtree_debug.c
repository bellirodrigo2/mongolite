// test_wtree_debug.c - Debug test for wtree iterator issues

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "wtree.h"
#include "gerror.h"

#ifdef _WIN32
    #include <direct.h>
    #define mkdir_portable(path) _mkdir(path)
#else
    #include <sys/stat.h>
    #define mkdir_portable(path) mkdir(path, 0755)
#endif

static void debug_iterator_test() {
    printf("\n=== DEBUG: Iterator Test ===\n");
    
    gerror_t error = {0};
    
    // Setup
    mkdir_portable("./tests");
    mkdir_portable("./tests/debug_db");
    
    wtree_db_t *db = wtree_db_create("./tests/debug_db", 0, 0, 0, &error);
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
    
    // Insert test data
    printf("Inserting test data...\n");
    struct {
        const char *key;
        const char *val;
    } test_data[] = {
        {"key1", "val1"},
        {"key2", "val2"},
        {"key3", "val3"}
    };
    
    for (int i = 0; i < 3; i++) {
        int rc = wtree_insert_one(tree, 
                                 test_data[i].key, strlen(test_data[i].key),
                                 test_data[i].val, strlen(test_data[i].val) + 1,
                                 &error);
        if (rc != 0) {
            printf("ERROR: Failed to insert %s: %s\n", test_data[i].key, error.message);
        } else {
            printf("  Inserted: %s -> %s\n", test_data[i].key, test_data[i].val);
        }
    }
    
    // Test iterator
    printf("\nCreating iterator...\n");
    wtree_iterator_t *iter = wtree_iterator_create(tree, &error);
    if (!iter) {
        printf("ERROR: Failed to create iterator: %s\n", error.message);
        wtree_tree_close(tree);
        wtree_db_close(db);
        return;
    }
    
    // Test first/next
    printf("\nIterating with first/next...\n");
    int count = 0;
    bool ok = wtree_iterator_first(iter);
    printf("  first() returned: %s\n", ok ? "true" : "false");
    
    while (ok) {
        const void *key, *val;
        size_t key_size, val_size;
        
        if (wtree_iterator_key(iter, &key, &key_size)) {
            printf("  Key %d: '", count + 1);
            fwrite(key, 1, key_size, stdout);
            printf("' (size=%zu)\n", key_size);
        } else {
            printf("  Failed to get key %d\n", count + 1);
        }
        
        if (wtree_iterator_value(iter, &val, &val_size)) {
            printf("  Val %d: '%s' (size=%zu)\n", count + 1, (char*)val, val_size);
        } else {
            printf("  Failed to get value %d\n", count + 1);
        }
        
        count++;
        ok = wtree_iterator_next(iter);
        printf("  next() returned: %s\n", ok ? "true" : "false");
    }
    
    printf("Total items counted: %d (expected 3)\n", count);
    
    // Test seek
    printf("\nTesting seek to 'key2'...\n");
    ok = wtree_iterator_seek(iter, "key2", 4);
    printf("  seek() returned: %s\n", ok ? "true" : "false");
    
    if (ok) {
        const void *key;
        size_t key_size;
        
        if (wtree_iterator_key(iter, &key, &key_size)) {
            printf("  Found key: '");
            fwrite(key, 1, key_size, stdout);
            printf("' (size=%zu)\n", key_size);
            
            // Compare with expected
            if (key_size == 4 && memcmp(key, "key2", 4) == 0) {
                printf("  ✓ Key matches 'key2'\n");
            } else {
                printf("  ✗ Key does NOT match 'key2'\n");
                printf("  Expected: 'key2' (4 bytes)\n");
                printf("  Got: '");
                for (size_t i = 0; i < key_size; i++) {
                    printf("%02x ", ((unsigned char*)key)[i]);
                }
                printf("'\n");
            }
        }
    }
    
    // Cleanup
    wtree_iterator_close(iter);
    wtree_tree_close(tree);
    wtree_db_close(db);
    
    // Remove test directory
#ifdef _WIN32
    system("rmdir /s /q .\\tests\\debug_db 2>nul");
#else
    system("rm -rf ./tests/debug_db");
#endif
}

int main() {
    debug_iterator_test();
    return 0;
}
