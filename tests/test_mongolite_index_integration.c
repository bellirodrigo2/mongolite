/*
 * test_mongolite_index_integration.c - Integration tests for index creation/deletion
 *
 * Tests:
 * - Basic index creation
 * - Index creation on existing documents
 * - Unique index creation and violation
 * - Sparse index creation
 * - Compound index creation
 * - Index deletion
 * - Error cases
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

    snprintf(g_db_path, sizeof(g_db_path), "/tmp/test_mongolite_index_%d", getpid());
    cleanup_db_path();

    char cmd[512];
    snprintf(cmd, sizeof(cmd), "mkdir -p %s", g_db_path);
    system(cmd);

    db_config_t config = {0};
    config.max_bytes = 64 * 1024 * 1024;  /* 64MB */
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
 * Helper: Insert test documents
 * ============================================================ */

static void insert_test_docs(const char *collection, int count) {
    for (int i = 0; i < count; i++) {
        char email[64];
        snprintf(email, sizeof(email), "user%d@example.com", i);

        bson_t *doc = BCON_NEW(
            "name", BCON_UTF8("User"),
            "email", BCON_UTF8(email),
            "age", BCON_INT32(20 + i),
            "score", BCON_DOUBLE(75.5 + i)
        );

        int rc = mongolite_insert_one(g_db, collection, doc, NULL, &error);
        assert_int_equal(0, rc);
        bson_destroy(doc);
    }
}

/* ============================================================
 * Tests: Basic Index Creation
 * ============================================================ */

static void test_create_simple_index(void **state) {
    (void)state;

    /* Create collection */
    int rc = mongolite_collection_create(g_db, "users", NULL, &error);
    assert_int_equal(0, rc);

    /* Create index */
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    rc = mongolite_create_index(g_db, "users", keys, NULL, NULL, &error);
    assert_int_equal(0, rc);

    bson_destroy(keys);

    /* Cleanup */
    mongolite_collection_drop(g_db, "users", NULL);
}

static void test_create_index_with_name(void **state) {
    (void)state;

    int rc = mongolite_collection_create(g_db, "users2", NULL, &error);
    assert_int_equal(0, rc);

    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    rc = mongolite_create_index(g_db, "users2", keys, "email_unique_idx", NULL, &error);
    assert_int_equal(0, rc);

    bson_destroy(keys);
    mongolite_collection_drop(g_db, "users2", NULL);
}

static void test_create_index_on_existing_documents(void **state) {
    (void)state;

    int rc = mongolite_collection_create(g_db, "indexed_col", NULL, &error);
    assert_int_equal(0, rc);

    /* Insert documents first */
    insert_test_docs("indexed_col", 10);

    /* Then create index */
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    rc = mongolite_create_index(g_db, "indexed_col", keys, NULL, NULL, &error);
    assert_int_equal(0, rc);

    bson_destroy(keys);
    mongolite_collection_drop(g_db, "indexed_col", NULL);
}

/* ============================================================
 * Tests: Unique Index
 * ============================================================ */

static void test_create_unique_index(void **state) {
    (void)state;

    int rc = mongolite_collection_create(g_db, "unique_test", NULL, &error);
    assert_int_equal(0, rc);

    /* Insert unique documents */
    insert_test_docs("unique_test", 5);

    /* Create unique index */
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    index_config_t config = {0};
    config.unique = true;

    rc = mongolite_create_index(g_db, "unique_test", keys, NULL, &config, &error);
    assert_int_equal(0, rc);

    bson_destroy(keys);
    mongolite_collection_drop(g_db, "unique_test", NULL);
}

static void test_unique_index_duplicate_violation(void **state) {
    (void)state;

    int rc = mongolite_collection_create(g_db, "dup_test", NULL, &error);
    assert_int_equal(0, rc);

    /* Insert documents with duplicate email */
    bson_t *doc1 = BCON_NEW("name", BCON_UTF8("User1"), "email", BCON_UTF8("same@example.com"));
    bson_t *doc2 = BCON_NEW("name", BCON_UTF8("User2"), "email", BCON_UTF8("same@example.com"));

    rc = mongolite_insert_one(g_db, "dup_test", doc1, NULL, &error);
    assert_int_equal(0, rc);
    rc = mongolite_insert_one(g_db, "dup_test", doc2, NULL, &error);
    assert_int_equal(0, rc);

    /* Try to create unique index - should fail */
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    index_config_t config = {0};
    config.unique = true;

    rc = mongolite_create_index(g_db, "dup_test", keys, NULL, &config, &error);
    assert_int_equal(MONGOLITE_EINDEX, rc);

    bson_destroy(keys);
    bson_destroy(doc1);
    bson_destroy(doc2);
    mongolite_collection_drop(g_db, "dup_test", NULL);
}

/* ============================================================
 * Tests: Sparse Index
 * ============================================================ */

static void test_create_sparse_index(void **state) {
    (void)state;

    int rc = mongolite_collection_create(g_db, "sparse_test", NULL, &error);
    assert_int_equal(0, rc);

    /* Insert some docs with email, some without */
    bson_t *doc1 = BCON_NEW("name", BCON_UTF8("User1"), "email", BCON_UTF8("user1@example.com"));
    bson_t *doc2 = BCON_NEW("name", BCON_UTF8("User2"));  /* No email */
    bson_t *doc3 = BCON_NEW("name", BCON_UTF8("User3"), "email", BCON_NULL);  /* email is null */

    rc = mongolite_insert_one(g_db, "sparse_test", doc1, NULL, &error);
    assert_int_equal(0, rc);
    rc = mongolite_insert_one(g_db, "sparse_test", doc2, NULL, &error);
    assert_int_equal(0, rc);
    rc = mongolite_insert_one(g_db, "sparse_test", doc3, NULL, &error);
    assert_int_equal(0, rc);

    /* Create sparse index */
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    index_config_t config = {0};
    config.sparse = true;

    rc = mongolite_create_index(g_db, "sparse_test", keys, NULL, &config, &error);
    assert_int_equal(0, rc);

    bson_destroy(keys);
    bson_destroy(doc1);
    bson_destroy(doc2);
    bson_destroy(doc3);
    mongolite_collection_drop(g_db, "sparse_test", NULL);
}

/* ============================================================
 * Tests: Compound Index
 * ============================================================ */

static void test_create_compound_index(void **state) {
    (void)state;

    int rc = mongolite_collection_create(g_db, "compound_test", NULL, &error);
    assert_int_equal(0, rc);

    insert_test_docs("compound_test", 5);

    /* Create compound index */
    bson_t *keys = BCON_NEW("name", BCON_INT32(1), "age", BCON_INT32(-1));

    rc = mongolite_create_index(g_db, "compound_test", keys, NULL, NULL, &error);
    assert_int_equal(0, rc);

    bson_destroy(keys);
    mongolite_collection_drop(g_db, "compound_test", NULL);
}

/* ============================================================
 * Tests: Index Deletion
 * ============================================================ */

static void test_drop_index(void **state) {
    (void)state;

    int rc = mongolite_collection_create(g_db, "drop_test", NULL, &error);
    assert_int_equal(0, rc);

    /* Create index */
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    rc = mongolite_create_index(g_db, "drop_test", keys, "email_1", NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(keys);

    /* Drop it */
    rc = mongolite_drop_index(g_db, "drop_test", "email_1", &error);
    assert_int_equal(0, rc);

    mongolite_collection_drop(g_db, "drop_test", NULL);
}

static void test_cannot_drop_id_index(void **state) {
    (void)state;

    int rc = mongolite_collection_create(g_db, "nodrop_test", NULL, &error);
    assert_int_equal(0, rc);

    /* Try to drop _id index - should fail */
    rc = mongolite_drop_index(g_db, "nodrop_test", "_id_", &error);
    assert_int_equal(MONGOLITE_EINVAL, rc);

    mongolite_collection_drop(g_db, "nodrop_test", NULL);
}

static void test_drop_nonexistent_index(void **state) {
    (void)state;

    int rc = mongolite_collection_create(g_db, "noindex_test", NULL, &error);
    assert_int_equal(0, rc);

    rc = mongolite_drop_index(g_db, "noindex_test", "nonexistent_1", &error);
    assert_int_equal(MONGOLITE_ENOTFOUND, rc);

    mongolite_collection_drop(g_db, "noindex_test", NULL);
}

/* ============================================================
 * Tests: Error Cases
 * ============================================================ */

static void test_create_index_on_nonexistent_collection(void **state) {
    (void)state;

    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    int rc = mongolite_create_index(g_db, "nonexistent", keys, NULL, NULL, &error);
    assert_int_equal(MONGOLITE_ENOTFOUND, rc);

    bson_destroy(keys);
}

static void test_create_duplicate_index(void **state) {
    (void)state;

    int rc = mongolite_collection_create(g_db, "dup_idx_test", NULL, &error);
    assert_int_equal(0, rc);

    bson_t *keys = BCON_NEW("email", BCON_INT32(1));

    /* Create first time */
    rc = mongolite_create_index(g_db, "dup_idx_test", keys, "email_1", NULL, &error);
    assert_int_equal(0, rc);

    /* Try to create again - should fail */
    rc = mongolite_create_index(g_db, "dup_idx_test", keys, "email_1", NULL, &error);
    assert_int_equal(MONGOLITE_EEXISTS, rc);

    bson_destroy(keys);
    mongolite_collection_drop(g_db, "dup_idx_test", NULL);
}

static void test_create_index_empty_keys(void **state) {
    (void)state;

    int rc = mongolite_collection_create(g_db, "empty_keys_test", NULL, &error);
    assert_int_equal(0, rc);

    bson_t *keys = bson_new();  /* Empty */
    rc = mongolite_create_index(g_db, "empty_keys_test", keys, NULL, NULL, &error);
    assert_int_equal(MONGOLITE_EINVAL, rc);

    bson_destroy(keys);
    mongolite_collection_drop(g_db, "empty_keys_test", NULL);
}

/* ============================================================
 * Tests: Index after Insert/Delete
 * ============================================================ */

static void test_index_survives_reopen(void **state) {
    (void)state;

    /* Create collection and index */
    int rc = mongolite_collection_create(g_db, "persist_test", NULL, &error);
    assert_int_equal(0, rc);

    insert_test_docs("persist_test", 3);

    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    rc = mongolite_create_index(g_db, "persist_test", keys, "email_1", NULL, &error);
    assert_int_equal(0, rc);
    bson_destroy(keys);

    /* Close and reopen database */
    mongolite_close(g_db);

    db_config_t config = {0};
    config.max_bytes = 64 * 1024 * 1024;
    config.max_dbs = 64;

    rc = mongolite_open(g_db_path, &g_db, &config, &error);
    assert_int_equal(0, rc);

    /* Try to create same index again - should fail because it exists */
    keys = BCON_NEW("email", BCON_INT32(1));
    rc = mongolite_create_index(g_db, "persist_test", keys, "email_1", NULL, &error);
    assert_int_equal(MONGOLITE_EEXISTS, rc);

    bson_destroy(keys);
    mongolite_collection_drop(g_db, "persist_test", NULL);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Basic index creation */
        cmocka_unit_test(test_create_simple_index),
        cmocka_unit_test(test_create_index_with_name),
        cmocka_unit_test(test_create_index_on_existing_documents),

        /* Unique index */
        cmocka_unit_test(test_create_unique_index),
        cmocka_unit_test(test_unique_index_duplicate_violation),

        /* Sparse index */
        cmocka_unit_test(test_create_sparse_index),

        /* Compound index */
        cmocka_unit_test(test_create_compound_index),

        /* Index deletion */
        cmocka_unit_test(test_drop_index),
        cmocka_unit_test(test_cannot_drop_id_index),
        cmocka_unit_test(test_drop_nonexistent_index),

        /* Error cases */
        cmocka_unit_test(test_create_index_on_nonexistent_collection),
        cmocka_unit_test(test_create_duplicate_index),
        cmocka_unit_test(test_create_index_empty_keys),

        /* Persistence */
        cmocka_unit_test(test_index_survives_reopen),
    };

    return cmocka_run_group_tests(tests, setup, teardown);
}
