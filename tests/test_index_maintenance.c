/*
 * test_index_maintenance.c - Tests for index maintenance on CRUD operations
 *
 * Tests that indexes are correctly maintained when documents are:
 * - Inserted (index entries added)
 * - Deleted (index entries removed)
 * - Updated (index entries updated)
 *
 * Also tests:
 * - Unique constraint enforcement on insert/update
 * - Sparse index behavior
 */

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <bson/bson.h>
#include <lmdb.h>

#include "mongolite.h"
#include "mongolite_internal.h"

static mongolite_db_t *g_db = NULL;
static char g_db_path[256];
static gerror_t error = {0};

/* ============================================================
 * Setup/Teardown
 * ============================================================ */

static void cleanup_db_path(void) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", g_db_path);
    system(cmd);
}

static int setup(void **state) {
    (void)state;

    snprintf(g_db_path, sizeof(g_db_path), "/tmp/test_index_maint_%d", getpid());
    cleanup_db_path();

    char cmd[512];
    snprintf(cmd, sizeof(cmd), "mkdir -p %s", g_db_path);
    system(cmd);

    db_config_t config = {0};
    config.max_bytes = 64 * 1024 * 1024;
    config.max_dbs = 64;

    int rc = mongolite_open(g_db_path, &g_db, &config, &error);
    assert_int_equal(0, rc);
    assert_non_null(g_db);

    return 0;
}

static int teardown(void **state) {
    (void)state;

    if (g_db) {
        mongolite_close(g_db);
        g_db = NULL;
    }

    cleanup_db_path();
    return 0;
}

/* ============================================================
 * Helper: Count index entries
 *
 * Uses a dedicated read transaction to avoid conflicts.
 * ============================================================ */

static int count_index_entries(const char *collection, const char *index_name) {
    /* wtree2 uses the format: idx:<collection_tree>:<index_name>
     * where collection_tree is col:<collection> */
    char *col_tree = _mongolite_collection_tree_name(collection);
    if (!col_tree) return -1;

    /* Build wtree2-style index tree name: idx:col:<collection>:<index_name> */
    size_t len = 4 + strlen(col_tree) + 1 + strlen(index_name) + 1;  /* "idx:" + tree + ":" + index + '\0' */
    char *tree_name = malloc(len);
    if (!tree_name) {
        free(col_tree);
        return -1;
    }
    snprintf(tree_name, len, "idx:%s:%s", col_tree, index_name);
    free(col_tree);

    /* Open the index tree using wtree3 API (MDB_DUPSORT for index trees) */
    wtree3_tree_t *tree = wtree3_tree_open(g_db->wdb, tree_name, MDB_DUPSORT, -1, NULL);
    free(tree_name);
    if (!tree) return -1;

    /* Create a dedicated read transaction */
    wtree3_txn_t *txn = wtree3_txn_begin(g_db->wdb, false, NULL);
    if (!txn) {
        wtree3_tree_close(tree);
        return -1;
    }

    int count = 0;
    wtree3_iterator_t *iter = wtree3_iterator_create_with_txn(tree, txn, NULL);
    if (iter) {
        if (wtree3_iterator_first(iter)) {
            do {
                count++;
            } while (wtree3_iterator_next(iter));
        }
        wtree3_iterator_close(iter);
    }

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
    return count;
}

/* ============================================================
 * Tests: Insert maintains index
 * ============================================================ */

static void test_insert_adds_to_index(void **state) {
    (void)state;

    /* Create collection */
    int rc = mongolite_collection_create(g_db, "users", NULL, &error);
    assert_int_equal(0, rc);

    /* Create index */
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    rc = mongolite_create_index(g_db, "users", keys, "email_1", NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(keys);

    /* Index should be empty */
    int count = count_index_entries("users", "email_1");
    assert_int_equal(0, count);

    /* Insert a document */
    bson_t *doc = BCON_NEW("email", BCON_UTF8("test@example.com"), "name", BCON_UTF8("Test"));
    rc = mongolite_insert_one(g_db, "users", doc, NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc);

    /* Index should have 1 entry */
    count = count_index_entries("users", "email_1");
    assert_int_equal(1, count);

    /* Insert another document */
    doc = BCON_NEW("email", BCON_UTF8("test2@example.com"), "name", BCON_UTF8("Test2"));
    rc = mongolite_insert_one(g_db, "users", doc, NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc);

    /* Index should have 2 entries */
    count = count_index_entries("users", "email_1");
    assert_int_equal(2, count);

    mongolite_collection_drop(g_db, "users", NULL);
}

static void test_insert_unique_violation(void **state) {
    (void)state;

    /* Create collection */
    int rc = mongolite_collection_create(g_db, "users_uniq", NULL, &error);
    assert_int_equal(0, rc);

    /* Create unique index */
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    index_config_t config = {0};
    config.unique = true;
    rc = mongolite_create_index(g_db, "users_uniq", keys, "email_1", &config, &error);
    assert_int_equal(0, rc);
    bson_destroy(keys);

    /* Insert first document */
    bson_t *doc1 = BCON_NEW("email", BCON_UTF8("test@example.com"), "name", BCON_UTF8("First"));
    rc = mongolite_insert_one(g_db, "users_uniq", doc1, NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc1);

    /* Try to insert duplicate - should fail */
    bson_t *doc2 = BCON_NEW("email", BCON_UTF8("test@example.com"), "name", BCON_UTF8("Second"));
    rc = mongolite_insert_one(g_db, "users_uniq", doc2, NULL, &error);
    assert_int_equal(MONGOLITE_EINDEX, rc);
    bson_destroy(doc2);

    /* Only 1 document should exist */
    int64_t doc_count = mongolite_collection_count(g_db, "users_uniq", NULL, &error);
    assert_int_equal(1, doc_count);

    mongolite_collection_drop(g_db, "users_uniq", NULL);
}

static void test_insert_sparse_skips_null(void **state) {
    (void)state;

    /* Create collection */
    int rc = mongolite_collection_create(g_db, "users_sparse", NULL, &error);
    assert_int_equal(0, rc);

    /* Create sparse index */
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    index_config_t config = {0};
    config.sparse = true;
    rc = mongolite_create_index(g_db, "users_sparse", keys, "email_1", &config, &error);
    assert_int_equal(0, rc);
    bson_destroy(keys);

    /* Insert document with email */
    bson_t *doc1 = BCON_NEW("email", BCON_UTF8("test@example.com"), "name", BCON_UTF8("Has Email"));
    rc = mongolite_insert_one(g_db, "users_sparse", doc1, NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc1);

    /* Insert document without email */
    bson_t *doc2 = BCON_NEW("name", BCON_UTF8("No Email"));
    rc = mongolite_insert_one(g_db, "users_sparse", doc2, NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc2);

    /* Insert document with null email */
    bson_t *doc3 = BCON_NEW("email", BCON_NULL, "name", BCON_UTF8("Null Email"));
    rc = mongolite_insert_one(g_db, "users_sparse", doc3, NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc3);

    /* Index should only have 1 entry (only the one with email) */
    int count = count_index_entries("users_sparse", "email_1");
    assert_int_equal(1, count);

    /* But we should have 3 documents */
    int64_t doc_count = mongolite_collection_count(g_db, "users_sparse", NULL, &error);
    assert_int_equal(3, doc_count);

    mongolite_collection_drop(g_db, "users_sparse", NULL);
}

/* ============================================================
 * Tests: Delete removes from index
 * ============================================================ */

static void test_delete_removes_from_index(void **state) {
    (void)state;

    /* Create collection */
    int rc = mongolite_collection_create(g_db, "del_test", NULL, &error);
    assert_int_equal(0, rc);

    /* Create index */
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    rc = mongolite_create_index(g_db, "del_test", keys, "email_1", NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(keys);

    /* Insert documents */
    bson_t *doc1 = BCON_NEW("email", BCON_UTF8("user1@example.com"));
    bson_t *doc2 = BCON_NEW("email", BCON_UTF8("user2@example.com"));
    bson_t *doc3 = BCON_NEW("email", BCON_UTF8("user3@example.com"));

    rc = mongolite_insert_one(g_db, "del_test", doc1, NULL, &error);
    assert_int_equal(0, rc);
    rc = mongolite_insert_one(g_db, "del_test", doc2, NULL, &error);
    assert_int_equal(0, rc);
    rc = mongolite_insert_one(g_db, "del_test", doc3, NULL, &error);
    assert_int_equal(0, rc);

    bson_destroy(doc1);
    bson_destroy(doc2);
    bson_destroy(doc3);

    /* Index should have 3 entries */
    int count = count_index_entries("del_test", "email_1");
    assert_int_equal(3, count);

    /* Delete one document */
    bson_t *filter = BCON_NEW("email", BCON_UTF8("user2@example.com"));
    rc = mongolite_delete_one(g_db, "del_test", filter, &error);
    assert_int_equal(0, rc);
    bson_destroy(filter);

    /* Index should have 2 entries */
    count = count_index_entries("del_test", "email_1");
    assert_int_equal(2, count);

    mongolite_collection_drop(g_db, "del_test", NULL);
}

static void test_delete_many_removes_from_index(void **state) {
    (void)state;

    /* Create collection */
    int rc = mongolite_collection_create(g_db, "delmany_test", NULL, &error);
    assert_int_equal(0, rc);

    /* Create index */
    bson_t *keys = BCON_NEW("age", BCON_INT32(1));
    rc = mongolite_create_index(g_db, "delmany_test", keys, "age_1", NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(keys);

    /* Insert documents */
    for (int i = 0; i < 10; i++) {
        bson_t *doc = BCON_NEW("name", BCON_UTF8("User"), "age", BCON_INT32(20 + (i % 3)));
        rc = mongolite_insert_one(g_db, "delmany_test", doc, NULL, &error);
        assert_int_equal(0, rc);
        bson_destroy(doc);
    }

    /* Index should have 10 entries */
    int count = count_index_entries("delmany_test", "age_1");
    assert_int_equal(10, count);

    /* Delete all documents with age=21 (should be 3) */
    bson_t *filter = BCON_NEW("age", BCON_INT32(21));
    int64_t deleted;
    rc = mongolite_delete_many(g_db, "delmany_test", filter, &deleted, &error);
    assert_int_equal(0, rc);
    assert_int_equal(3, deleted);
    bson_destroy(filter);

    /* Index should have 7 entries */
    count = count_index_entries("delmany_test", "age_1");
    assert_int_equal(7, count);

    mongolite_collection_drop(g_db, "delmany_test", NULL);
}

/* ============================================================
 * Tests: Update maintains index
 * ============================================================ */

static void test_update_updates_index(void **state) {
    (void)state;

    /* Create collection */
    int rc = mongolite_collection_create(g_db, "upd_test", NULL, &error);
    assert_int_equal(0, rc);

    /* Create index */
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    rc = mongolite_create_index(g_db, "upd_test", keys, "email_1", NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(keys);

    /* Insert document */
    bson_t *doc = BCON_NEW("email", BCON_UTF8("old@example.com"), "name", BCON_UTF8("Test"));
    rc = mongolite_insert_one(g_db, "upd_test", doc, NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc);

    /* Index should have 1 entry */
    int count = count_index_entries("upd_test", "email_1");
    assert_int_equal(1, count);

    /* Update the email field */
    bson_t *filter = BCON_NEW("email", BCON_UTF8("old@example.com"));
    bson_t *update = BCON_NEW("$set", "{", "email", BCON_UTF8("new@example.com"), "}");
    rc = mongolite_update_one(g_db, "upd_test", filter, update, false, &error);
    assert_int_equal(0, rc);
    bson_destroy(filter);
    bson_destroy(update);

    /* Index should still have 1 entry (updated) */
    count = count_index_entries("upd_test", "email_1");
    assert_int_equal(1, count);

    /* Verify the update - old email should not find anything */
    filter = BCON_NEW("email", BCON_UTF8("old@example.com"));
    bson_t *found = mongolite_find_one(g_db, "upd_test", filter, NULL, &error);
    assert_null(found);
    bson_destroy(filter);

    /* New email should find the document */
    filter = BCON_NEW("email", BCON_UTF8("new@example.com"));
    found = mongolite_find_one(g_db, "upd_test", filter, NULL, &error);
    assert_non_null(found);
    bson_destroy(found);
    bson_destroy(filter);

    mongolite_collection_drop(g_db, "upd_test", NULL);
}

static void test_update_unique_violation(void **state) {
    (void)state;

    /* Create collection */
    int rc = mongolite_collection_create(g_db, "upd_uniq", NULL, &error);
    assert_int_equal(0, rc);

    /* Create unique index */
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    index_config_t config = {0};
    config.unique = true;
    rc = mongolite_create_index(g_db, "upd_uniq", keys, "email_1", &config, &error);
    assert_int_equal(0, rc);
    bson_destroy(keys);

    /* Insert two documents */
    bson_t *doc1 = BCON_NEW("email", BCON_UTF8("user1@example.com"), "name", BCON_UTF8("User1"));
    bson_t *doc2 = BCON_NEW("email", BCON_UTF8("user2@example.com"), "name", BCON_UTF8("User2"));
    rc = mongolite_insert_one(g_db, "upd_uniq", doc1, NULL, &error);
    assert_int_equal(0, rc);
    rc = mongolite_insert_one(g_db, "upd_uniq", doc2, NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc1);
    bson_destroy(doc2);

    /* Try to update user2's email to user1's email - should fail */
    bson_t *filter = BCON_NEW("email", BCON_UTF8("user2@example.com"));
    bson_t *update = BCON_NEW("$set", "{", "email", BCON_UTF8("user1@example.com"), "}");
    rc = mongolite_update_one(g_db, "upd_uniq", filter, update, false, &error);
    assert_int_equal(MONGOLITE_EINDEX, rc);
    bson_destroy(filter);
    bson_destroy(update);

    /* user2 should still have their original email */
    filter = BCON_NEW("name", BCON_UTF8("User2"));
    bson_t *found = mongolite_find_one(g_db, "upd_uniq", filter, NULL, &error);
    assert_non_null(found);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, found, "email"));
    assert_string_equal("user2@example.com", bson_iter_utf8(&iter, NULL));

    bson_destroy(found);
    bson_destroy(filter);

    mongolite_collection_drop(g_db, "upd_uniq", NULL);
}

static void test_replace_updates_index(void **state) {
    (void)state;

    /* Create collection */
    int rc = mongolite_collection_create(g_db, "repl_test", NULL, &error);
    assert_int_equal(0, rc);

    /* Create index */
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    rc = mongolite_create_index(g_db, "repl_test", keys, "email_1", NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(keys);

    /* Insert document */
    bson_t *doc = BCON_NEW("email", BCON_UTF8("old@example.com"), "name", BCON_UTF8("Old Name"));
    rc = mongolite_insert_one(g_db, "repl_test", doc, NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(doc);

    /* Replace the document */
    bson_t *filter = BCON_NEW("email", BCON_UTF8("old@example.com"));
    bson_t *replacement = BCON_NEW("email", BCON_UTF8("replaced@example.com"), "name", BCON_UTF8("New Name"));
    rc = mongolite_replace_one(g_db, "repl_test", filter, replacement, false, &error);
    assert_int_equal(0, rc);
    bson_destroy(filter);
    bson_destroy(replacement);

    /* Index should still have 1 entry */
    int count = count_index_entries("repl_test", "email_1");
    assert_int_equal(1, count);

    /* Verify the replacement */
    filter = BCON_NEW("email", BCON_UTF8("replaced@example.com"));
    bson_t *found = mongolite_find_one(g_db, "repl_test", filter, NULL, &error);
    assert_non_null(found);
    bson_destroy(found);
    bson_destroy(filter);

    mongolite_collection_drop(g_db, "repl_test", NULL);
}

/* ============================================================
 * Tests: Multiple indexes
 * ============================================================ */

static void test_multiple_indexes_maintained(void **state) {
    (void)state;

    /* Create collection */
    int rc = mongolite_collection_create(g_db, "multi_idx", NULL, &error);
    assert_int_equal(0, rc);

    /* Create two indexes */
    bson_t *keys1 = BCON_NEW("email", BCON_INT32(1));
    bson_t *keys2 = BCON_NEW("age", BCON_INT32(1));

    rc = mongolite_create_index(g_db, "multi_idx", keys1, "email_1", NULL, &error);
    assert_int_equal(0, rc);
    rc = mongolite_create_index(g_db, "multi_idx", keys2, "age_1", NULL, &error);
    assert_int_equal(0, rc);

    bson_destroy(keys1);
    bson_destroy(keys2);

    /* Insert documents */
    for (int i = 0; i < 5; i++) {
        char email[64];
        snprintf(email, sizeof(email), "user%d@example.com", i);
        bson_t *doc = BCON_NEW("email", BCON_UTF8(email), "age", BCON_INT32(20 + i));
        rc = mongolite_insert_one(g_db, "multi_idx", doc, NULL, &error);
        assert_int_equal(0, rc);
        bson_destroy(doc);
    }

    /* Both indexes should have 5 entries */
    assert_int_equal(5, count_index_entries("multi_idx", "email_1"));
    assert_int_equal(5, count_index_entries("multi_idx", "age_1"));

    /* Delete one document */
    bson_t *filter = BCON_NEW("email", BCON_UTF8("user2@example.com"));
    rc = mongolite_delete_one(g_db, "multi_idx", filter, &error);
    assert_int_equal(0, rc);
    bson_destroy(filter);

    /* Both indexes should have 4 entries */
    assert_int_equal(4, count_index_entries("multi_idx", "email_1"));
    assert_int_equal(4, count_index_entries("multi_idx", "age_1"));

    mongolite_collection_drop(g_db, "multi_idx", NULL);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Insert tests */
        cmocka_unit_test(test_insert_adds_to_index),
        cmocka_unit_test(test_insert_unique_violation),
        cmocka_unit_test(test_insert_sparse_skips_null),

        /* Delete tests */
        cmocka_unit_test(test_delete_removes_from_index),
        cmocka_unit_test(test_delete_many_removes_from_index),

        /* Update tests */
        cmocka_unit_test(test_update_updates_index),
        cmocka_unit_test(test_update_unique_violation),
        cmocka_unit_test(test_replace_updates_index),

        /* Multiple indexes */
        cmocka_unit_test(test_multiple_indexes_maintained),
    };

    return cmocka_run_group_tests(tests, setup, teardown);
}
