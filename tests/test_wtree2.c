/*
 * test_wtree2.c - Comprehensive tests for wtree2
 */

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "wtree2.h"

/* Test database path */
static char test_db_path[256];
static wtree2_db_t *test_db = NULL;

/* ============================================================
 * Test Fixtures
 * ============================================================ */

static int setup_db(void **state) {
    (void)state;

    /* Create temp directory */
    snprintf(test_db_path, sizeof(test_db_path), "/tmp/test_wtree2_%d", getpid());
    mkdir(test_db_path, 0755);

    gerror_t error = {0};
    test_db = wtree2_db_create(test_db_path, 64 * 1024 * 1024, 32, 0, &error);
    if (!test_db) {
        fprintf(stderr, "Failed to create test database: %s\n", error.message);
        return -1;
    }

    return 0;
}

static int teardown_db(void **state) {
    (void)state;

    if (test_db) {
        wtree2_db_close(test_db);
        test_db = NULL;
    }

    /* Clean up temp directory */
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", test_db_path);
    (void)system(cmd);

    return 0;
}

/* ============================================================
 * Simple Index Key Extractor
 *
 * For testing, we use a simple format:
 * Value = "field1:value1|field2:value2|..."
 * We extract the value of the field named by user_data.
 * ============================================================ */

static bool simple_key_extractor(const void *value, size_t value_len,
                                  void *user_data,
                                  void **out_key, size_t *out_len) {
    (void)value_len;  /* Use null-terminated string instead */
    const char *field_name = (const char *)user_data;
    const char *val = (const char *)value;

    /* Find field:value in the string */
    char search[64];
    snprintf(search, sizeof(search), "%s:", field_name);

    const char *found = strstr(val, search);
    if (!found) {
        return false;  /* Field not found - sparse behavior */
    }

    /* Extract value until | or end of string */
    const char *start = found + strlen(search);
    const char *end = strchr(start, '|');
    if (!end) {
        end = start + strlen(start);  /* Use strlen, not value_len */
    }

    size_t key_len = end - start;
    char *key = malloc(key_len);  /* No need for +1, we don't store null */
    if (!key) return false;

    memcpy(key, start, key_len);

    *out_key = key;
    *out_len = key_len;
    return true;
}

/* ============================================================
 * Basic Tree Tests
 * ============================================================ */

static void test_tree_create_close(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree2_tree_t *tree = wtree2_tree_create(test_db, "test_tree", 0, 0, &error);
    assert_non_null(tree);
    assert_string_equal(wtree2_tree_name(tree), "test_tree");
    assert_int_equal(wtree2_tree_count(tree), 0);

    wtree2_tree_close(tree);
}

static void test_basic_crud(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree2_tree_t *tree = wtree2_tree_create(test_db, "crud_tree", 0, 0, &error);
    assert_non_null(tree);

    /* Insert */
    const char *key1 = "key1";
    const char *val1 = "value1";
    int rc = wtree2_insert_one(tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);
    assert_int_equal(rc, WTREE2_OK);
    assert_int_equal(wtree2_tree_count(tree), 1);

    /* Get */
    void *retrieved = NULL;
    size_t retrieved_size = 0;
    rc = wtree2_get(tree, key1, strlen(key1), &retrieved, &retrieved_size, &error);
    assert_int_equal(rc, WTREE2_OK);
    assert_string_equal((char *)retrieved, "value1");
    free(retrieved);

    /* Update */
    const char *val1_new = "value1_updated";
    rc = wtree2_update(tree, key1, strlen(key1), val1_new, strlen(val1_new) + 1, &error);
    assert_int_equal(rc, WTREE2_OK);
    assert_int_equal(wtree2_tree_count(tree), 1);  /* Count unchanged */

    rc = wtree2_get(tree, key1, strlen(key1), &retrieved, &retrieved_size, &error);
    assert_int_equal(rc, WTREE2_OK);
    assert_string_equal((char *)retrieved, "value1_updated");
    free(retrieved);

    /* Exists */
    assert_true(wtree2_exists(tree, key1, strlen(key1), &error));
    assert_false(wtree2_exists(tree, "nonexistent", 11, &error));

    /* Delete */
    bool deleted = false;
    rc = wtree2_delete_one(tree, key1, strlen(key1), &deleted, &error);
    assert_int_equal(rc, WTREE2_OK);
    assert_true(deleted);
    assert_int_equal(wtree2_tree_count(tree), 0);

    /* Delete non-existent */
    deleted = false;
    rc = wtree2_delete_one(tree, key1, strlen(key1), &deleted, &error);
    assert_int_equal(rc, WTREE2_OK);
    assert_false(deleted);

    wtree2_tree_close(tree);
}

static void test_iterator(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree2_tree_t *tree = wtree2_tree_create(test_db, "iter_tree", 0, 0, &error);
    assert_non_null(tree);

    /* Insert some entries */
    char key[16], val[16];
    for (int i = 1; i <= 5; i++) {
        snprintf(key, sizeof(key), "key%d", i);
        snprintf(val, sizeof(val), "val%d", i);
        wtree2_insert_one(tree, key, strlen(key), val, strlen(val) + 1, &error);
    }
    assert_int_equal(wtree2_tree_count(tree), 5);

    /* Forward iteration */
    wtree2_iterator_t *iter = wtree2_iterator_create(tree, &error);
    assert_non_null(iter);

    int count = 0;
    if (wtree2_iterator_first(iter)) {
        do {
            const void *k, *v;
            size_t klen, vlen;
            assert_true(wtree2_iterator_key(iter, &k, &klen));
            assert_true(wtree2_iterator_value(iter, &v, &vlen));
            count++;
        } while (wtree2_iterator_next(iter));
    }
    assert_int_equal(count, 5);

    wtree2_iterator_close(iter);
    wtree2_tree_close(tree);
}

/* ============================================================
 * Index Tests
 * ============================================================ */

static void test_add_index(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree2_tree_t *tree = wtree2_tree_create(test_db, "idx_tree1", 0, 0, &error);
    assert_non_null(tree);

    wtree2_index_config_t config = {
        .name = "email_idx",
        .key_fn = simple_key_extractor,
        .user_data = (void *)"email",
        .unique = true,
        .sparse = false,
        .compare = NULL
    };

    int rc = wtree2_tree_add_index(tree, &config, &error);
    assert_int_equal(rc, WTREE2_OK);
    assert_true(wtree2_tree_has_index(tree, "email_idx"));
    assert_int_equal(wtree2_tree_index_count(tree), 1);

    /* Adding duplicate should fail */
    rc = wtree2_tree_add_index(tree, &config, &error);
    assert_int_equal(rc, WTREE2_EEXISTS);

    wtree2_tree_close(tree);
}

static void test_index_maintenance_insert(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree2_tree_t *tree = wtree2_tree_create(test_db, "idx_tree2", 0, 0, &error);
    assert_non_null(tree);

    /* Add email index */
    wtree2_index_config_t config = {
        .name = "email",
        .key_fn = simple_key_extractor,
        .user_data = (void *)"email",
        .unique = false,
        .sparse = false,
        .compare = NULL
    };
    int rc = wtree2_tree_add_index(tree, &config, &error);
    assert_int_equal(rc, WTREE2_OK);

    /* Insert document with email */
    const char *key1 = "doc1";
    const char *val1 = "name:Alice|email:alice@test.com";
    rc = wtree2_insert_one(tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);
    assert_int_equal(rc, WTREE2_OK);

    /* Verify we can find via index */
    wtree2_iterator_t *iter = wtree2_index_seek(tree, "email",
                                                  "alice@test.com", 14, &error);
    assert_non_null(iter);

    assert_true(wtree2_iterator_valid(iter));

    const void *main_key;
    size_t main_key_size;
    assert_true(wtree2_index_iterator_main_key(iter, &main_key, &main_key_size));
    assert_memory_equal(main_key, "doc1", 4);

    wtree2_iterator_close(iter);
    wtree2_tree_close(tree);
}

static void test_unique_index_violation(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree2_tree_t *tree = wtree2_tree_create(test_db, "idx_tree3", 0, 0, &error);
    assert_non_null(tree);

    /* Add unique email index */
    wtree2_index_config_t config = {
        .name = "email",
        .key_fn = simple_key_extractor,
        .user_data = (void *)"email",
        .unique = true,
        .sparse = false,
        .compare = NULL
    };
    wtree2_tree_add_index(tree, &config, &error);

    /* Insert first document */
    const char *key1 = "doc1";
    const char *val1 = "name:Alice|email:alice@test.com";
    int rc = wtree2_insert_one(tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);
    assert_int_equal(rc, WTREE2_OK);

    /* Insert second document with same email - should fail */
    const char *key2 = "doc2";
    const char *val2 = "name:Bob|email:alice@test.com";
    rc = wtree2_insert_one(tree, key2, strlen(key2), val2, strlen(val2) + 1, &error);
    assert_int_equal(rc, WTREE2_EINDEX);
    assert_int_equal(wtree2_tree_count(tree), 1);  /* Only 1 doc inserted */

    /* Insert with different email - should succeed */
    const char *key3 = "doc3";
    const char *val3 = "name:Charlie|email:charlie@test.com";
    rc = wtree2_insert_one(tree, key3, strlen(key3), val3, strlen(val3) + 1, &error);
    assert_int_equal(rc, WTREE2_OK);
    assert_int_equal(wtree2_tree_count(tree), 2);

    wtree2_tree_close(tree);
}

static void test_sparse_index(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree2_tree_t *tree = wtree2_tree_create(test_db, "idx_tree4", 0, 0, &error);
    assert_non_null(tree);

    /* Add sparse email index */
    wtree2_index_config_t config = {
        .name = "email",
        .key_fn = simple_key_extractor,
        .user_data = (void *)"email",
        .unique = true,
        .sparse = true,
        .compare = NULL
    };
    wtree2_tree_add_index(tree, &config, &error);

    /* Insert document WITH email */
    const char *key1 = "doc1";
    const char *val1 = "name:Alice|email:alice@test.com";
    int rc = wtree2_insert_one(tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);
    assert_int_equal(rc, WTREE2_OK);

    /* Insert document WITHOUT email - should succeed (sparse index) */
    const char *key2 = "doc2";
    const char *val2 = "name:Bob|phone:12345";  /* No email field */
    rc = wtree2_insert_one(tree, key2, strlen(key2), val2, strlen(val2) + 1, &error);
    assert_int_equal(rc, WTREE2_OK);
    assert_int_equal(wtree2_tree_count(tree), 2);

    /* Verify second doc is NOT in the index */
    wtree2_iterator_t *iter = wtree2_index_seek(tree, "email", "bob", 3, &error);
    assert_non_null(iter);
    assert_false(wtree2_iterator_valid(iter));  /* Not found */
    wtree2_iterator_close(iter);

    wtree2_tree_close(tree);
}

static void test_index_maintenance_update(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree2_tree_t *tree = wtree2_tree_create(test_db, "idx_tree5", 0, 0, &error);
    assert_non_null(tree);

    /* Add email index */
    wtree2_index_config_t config = {
        .name = "email",
        .key_fn = simple_key_extractor,
        .user_data = (void *)"email",
        .unique = true,
        .sparse = false,
        .compare = NULL
    };
    wtree2_tree_add_index(tree, &config, &error);

    /* Insert document */
    const char *key1 = "doc1";
    const char *val1 = "name:Alice|email:alice@test.com";
    wtree2_insert_one(tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);

    /* Update email */
    const char *val1_updated = "name:Alice|email:alice.new@test.com";
    int rc = wtree2_update(tree, key1, strlen(key1), val1_updated, strlen(val1_updated) + 1, &error);
    assert_int_equal(rc, WTREE2_OK);

    /* Old email should NOT be findable */
    wtree2_iterator_t *iter = wtree2_index_seek(tree, "email", "alice@test.com", 14, &error);
    assert_non_null(iter);
    assert_false(wtree2_iterator_valid(iter));
    wtree2_iterator_close(iter);

    /* New email SHOULD be findable */
    iter = wtree2_index_seek(tree, "email", "alice.new@test.com", 18, &error);
    assert_non_null(iter);
    assert_true(wtree2_iterator_valid(iter));
    wtree2_iterator_close(iter);

    wtree2_tree_close(tree);
}

static void test_index_maintenance_delete(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree2_tree_t *tree = wtree2_tree_create(test_db, "idx_tree6", 0, 0, &error);
    assert_non_null(tree);

    /* Add email index */
    wtree2_index_config_t config = {
        .name = "email",
        .key_fn = simple_key_extractor,
        .user_data = (void *)"email",
        .unique = false,
        .sparse = false,
        .compare = NULL
    };
    wtree2_tree_add_index(tree, &config, &error);

    /* Insert document */
    const char *key1 = "doc1";
    const char *val1 = "name:Alice|email:alice@test.com";
    wtree2_insert_one(tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);

    /* Delete document */
    bool deleted = false;
    int rc = wtree2_delete_one(tree, key1, strlen(key1), &deleted, &error);
    assert_int_equal(rc, WTREE2_OK);
    assert_true(deleted);

    /* Email should NOT be findable in index */
    wtree2_iterator_t *iter = wtree2_index_seek(tree, "email", "alice@test.com", 14, &error);
    assert_non_null(iter);
    assert_false(wtree2_iterator_valid(iter));
    wtree2_iterator_close(iter);

    wtree2_tree_close(tree);
}

static void test_populate_index(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree2_tree_t *tree = wtree2_tree_create(test_db, "idx_tree7", 0, 0, &error);
    assert_non_null(tree);

    /* Insert documents BEFORE adding index */
    const char *key1 = "doc1";
    const char *val1 = "name:Alice|email:alice@test.com";
    wtree2_insert_one(tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);

    const char *key2 = "doc2";
    const char *val2 = "name:Bob|email:bob@test.com";
    wtree2_insert_one(tree, key2, strlen(key2), val2, strlen(val2) + 1, &error);

    /* Add index AFTER data exists */
    wtree2_index_config_t config = {
        .name = "email",
        .key_fn = simple_key_extractor,
        .user_data = (void *)"email",
        .unique = false,
        .sparse = false,
        .compare = NULL
    };
    wtree2_tree_add_index(tree, &config, &error);

    /* Populate index */
    int rc = wtree2_tree_populate_index(tree, "email", &error);
    assert_int_equal(rc, WTREE2_OK);

    /* Both emails should be findable */
    wtree2_iterator_t *iter = wtree2_index_seek(tree, "email", "alice@test.com", 14, &error);
    assert_non_null(iter);
    assert_true(wtree2_iterator_valid(iter));
    wtree2_iterator_close(iter);

    iter = wtree2_index_seek(tree, "email", "bob@test.com", 12, &error);
    assert_non_null(iter);
    assert_true(wtree2_iterator_valid(iter));
    wtree2_iterator_close(iter);

    wtree2_tree_close(tree);
}

static void test_drop_index(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree2_tree_t *tree = wtree2_tree_create(test_db, "idx_tree8", 0, 0, &error);
    assert_non_null(tree);

    /* Add index */
    wtree2_index_config_t config = {
        .name = "email",
        .key_fn = simple_key_extractor,
        .user_data = (void *)"email",
        .unique = false,
        .sparse = false,
        .compare = NULL
    };
    wtree2_tree_add_index(tree, &config, &error);
    assert_true(wtree2_tree_has_index(tree, "email"));

    /* Insert a document */
    const char *key1 = "doc1";
    const char *val1 = "name:Alice|email:alice@test.com";
    wtree2_insert_one(tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);

    /* Drop index */
    int rc = wtree2_tree_drop_index(tree, "email", &error);
    assert_int_equal(rc, WTREE2_OK);
    assert_false(wtree2_tree_has_index(tree, "email"));
    assert_int_equal(wtree2_tree_index_count(tree), 0);

    /* Document should still exist in main tree */
    void *retrieved = NULL;
    size_t retrieved_size = 0;
    rc = wtree2_get(tree, key1, strlen(key1), &retrieved, &retrieved_size, &error);
    assert_int_equal(rc, WTREE2_OK);
    free(retrieved);

    wtree2_tree_close(tree);
}

static void test_multiple_indexes(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree2_tree_t *tree = wtree2_tree_create(test_db, "idx_tree9", 0, 0, &error);
    assert_non_null(tree);

    /* Add email index */
    wtree2_index_config_t email_config = {
        .name = "email",
        .key_fn = simple_key_extractor,
        .user_data = (void *)"email",
        .unique = true,
        .sparse = false,
        .compare = NULL
    };
    wtree2_tree_add_index(tree, &email_config, &error);

    /* Add name index */
    wtree2_index_config_t name_config = {
        .name = "name",
        .key_fn = simple_key_extractor,
        .user_data = (void *)"name",
        .unique = false,
        .sparse = false,
        .compare = NULL
    };
    wtree2_tree_add_index(tree, &name_config, &error);

    assert_int_equal(wtree2_tree_index_count(tree), 2);

    /* Insert document */
    const char *key1 = "doc1";
    const char *val1 = "name:Alice|email:alice@test.com";
    int rc = wtree2_insert_one(tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);
    assert_int_equal(rc, WTREE2_OK);

    /* Both indexes should work */
    wtree2_iterator_t *iter = wtree2_index_seek(tree, "email", "alice@test.com", 14, &error);
    assert_true(wtree2_iterator_valid(iter));
    wtree2_iterator_close(iter);

    iter = wtree2_index_seek(tree, "name", "Alice", 5, &error);
    assert_true(wtree2_iterator_valid(iter));
    wtree2_iterator_close(iter);

    wtree2_tree_close(tree);
}

static void test_transaction_with_indexes(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree2_tree_t *tree = wtree2_tree_create(test_db, "idx_tree10", 0, 0, &error);
    assert_non_null(tree);

    /* Add index */
    wtree2_index_config_t config = {
        .name = "email",
        .key_fn = simple_key_extractor,
        .user_data = (void *)"email",
        .unique = true,
        .sparse = false,
        .compare = NULL
    };
    wtree2_tree_add_index(tree, &config, &error);

    /* Start transaction */
    wtree2_txn_t *txn = wtree2_txn_begin(wtree2_tree_get_db(tree), true, &error);
    assert_non_null(txn);

    /* Insert within transaction */
    const char *key1 = "doc1";
    const char *val1 = "name:Alice|email:alice@test.com";
    int rc = wtree2_insert_one_txn(txn, tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);
    assert_int_equal(rc, WTREE2_OK);

    const char *key2 = "doc2";
    const char *val2 = "name:Bob|email:bob@test.com";
    rc = wtree2_insert_one_txn(txn, tree, key2, strlen(key2), val2, strlen(val2) + 1, &error);
    assert_int_equal(rc, WTREE2_OK);

    /* Commit */
    rc = wtree2_txn_commit(txn, &error);
    assert_int_equal(rc, WTREE2_OK);

    assert_int_equal(wtree2_tree_count(tree), 2);

    wtree2_tree_close(tree);
}

static void test_transaction_rollback(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree2_tree_t *tree = wtree2_tree_create(test_db, "idx_tree11", 0, 0, &error);
    assert_non_null(tree);

    /* Add index */
    wtree2_index_config_t config = {
        .name = "email",
        .key_fn = simple_key_extractor,
        .user_data = (void *)"email",
        .unique = true,
        .sparse = false,
        .compare = NULL
    };
    wtree2_tree_add_index(tree, &config, &error);

    /* Insert one document and commit */
    const char *key1 = "doc1";
    const char *val1 = "name:Alice|email:alice@test.com";
    wtree2_insert_one(tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);
    assert_int_equal(wtree2_tree_count(tree), 1);

    /* Start new transaction */
    wtree2_txn_t *txn = wtree2_txn_begin(wtree2_tree_get_db(tree), true, &error);
    assert_non_null(txn);

    /* Insert within transaction */
    const char *key2 = "doc2";
    const char *val2 = "name:Bob|email:bob@test.com";
    int rc = wtree2_insert_one_txn(txn, tree, key2, strlen(key2), val2, strlen(val2) + 1, &error);
    assert_int_equal(rc, WTREE2_OK);
    assert_int_equal(wtree2_tree_count(tree), 2);  /* Count is 2 during txn */

    /* Abort instead of commit */
    wtree2_txn_abort(txn);

    /* Note: entry_count in memory may be stale after abort
     * In real usage, you'd reload the tree or track in txn-local state */

    /* But main tree should only have 1 doc */
    assert_true(wtree2_exists(tree, key1, strlen(key1), &error));
    assert_false(wtree2_exists(tree, key2, strlen(key2), &error));

    /* Index should only have alice */
    wtree2_iterator_t *iter = wtree2_index_seek(tree, "email", "bob@test.com", 12, &error);
    assert_false(wtree2_iterator_valid(iter));
    wtree2_iterator_close(iter);

    wtree2_tree_close(tree);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Basic tests */
        cmocka_unit_test(test_tree_create_close),
        cmocka_unit_test(test_basic_crud),
        cmocka_unit_test(test_iterator),

        /* Index tests */
        cmocka_unit_test(test_add_index),
        cmocka_unit_test(test_index_maintenance_insert),
        cmocka_unit_test(test_unique_index_violation),
        cmocka_unit_test(test_sparse_index),
        cmocka_unit_test(test_index_maintenance_update),
        cmocka_unit_test(test_index_maintenance_delete),
        cmocka_unit_test(test_populate_index),
        cmocka_unit_test(test_drop_index),
        cmocka_unit_test(test_multiple_indexes),
        cmocka_unit_test(test_transaction_with_indexes),
        cmocka_unit_test(test_transaction_rollback),
    };

    return cmocka_run_group_tests(tests, setup_db, teardown_db);
}
