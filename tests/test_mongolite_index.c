/*
 * test_mongolite_index.c - Unit tests for index infrastructure (Phase 1)
 *
 * Tests:
 * - _index_name_from_spec(): Index name generation
 * - _build_index_key(): Index key building
 * - _index_key_compare(): Key comparison
 * - _should_index_document(): Sparse index handling
 * - Serialization/deserialization helpers
 */

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>
#include <bson/bson.h>

#include "mongolite_internal.h"
#include "key_compare.h"

/* ============================================================
 * Tests: _index_name_from_spec
 * ============================================================ */

static void test_index_name_single_field_asc(void **state) {
    (void)state;
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    char *name = _index_name_from_spec(keys);

    assert_non_null(name);
    assert_string_equal(name, "email_1");

    free(name);
    bson_destroy(keys);
}

static void test_index_name_single_field_desc(void **state) {
    (void)state;
    bson_t *keys = BCON_NEW("email", BCON_INT32(-1));
    char *name = _index_name_from_spec(keys);

    assert_non_null(name);
    assert_string_equal(name, "email_-1");

    free(name);
    bson_destroy(keys);
}

static void test_index_name_compound(void **state) {
    (void)state;
    bson_t *keys = BCON_NEW("name", BCON_INT32(1), "age", BCON_INT32(-1));
    char *name = _index_name_from_spec(keys);

    assert_non_null(name);
    assert_string_equal(name, "name_1_age_-1");

    free(name);
    bson_destroy(keys);
}

static void test_index_name_dotted_field(void **state) {
    (void)state;
    bson_t *keys = BCON_NEW("address.city", BCON_INT32(1));
    char *name = _index_name_from_spec(keys);

    assert_non_null(name);
    assert_string_equal(name, "address.city_1");

    free(name);
    bson_destroy(keys);
}

static void test_index_name_three_fields(void **state) {
    (void)state;
    bson_t *keys = BCON_NEW("a", BCON_INT32(1), "b", BCON_INT32(-1), "c", BCON_INT32(1));
    char *name = _index_name_from_spec(keys);

    assert_non_null(name);
    assert_string_equal(name, "a_1_b_-1_c_1");

    free(name);
    bson_destroy(keys);
}

static void test_index_name_null_keys(void **state) {
    (void)state;
    char *name = _index_name_from_spec(NULL);
    assert_null(name);
}

static void test_index_name_empty_keys(void **state) {
    (void)state;
    bson_t *keys = bson_new();
    char *name = _index_name_from_spec(keys);
    assert_null(name);  /* Empty keys should return NULL */
    bson_destroy(keys);
}

/* ============================================================
 * Tests: _build_index_key
 * ============================================================ */

static void test_build_index_key_single_field(void **state) {
    (void)state;
    bson_oid_t oid;
    bson_oid_init(&oid, NULL);

    bson_t *doc = BCON_NEW(
        "_id", BCON_OID(&oid),
        "email", BCON_UTF8("test@example.com"),
        "name", BCON_UTF8("John")
    );
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));

    /* Without _id */
    bson_t *key = _build_index_key(doc, keys, false);
    assert_non_null(key);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, key, "email"));
    assert_string_equal(bson_iter_utf8(&iter, NULL), "test@example.com");
    /* With DUPSORT optimization, _id is never in the key - it's the value */
    assert_false(bson_iter_init_find(&iter, key, "_id"));

    bson_destroy(key);

    /* include_id parameter is now ignored with DUPSORT - key never has _id */
    key = _build_index_key(doc, keys, true);
    assert_non_null(key);

    assert_true(bson_iter_init_find(&iter, key, "email"));
    assert_false(bson_iter_init_find(&iter, key, "_id"));  /* No _id with DUPSORT */

    bson_destroy(key);
    bson_destroy(doc);
    bson_destroy(keys);
}

static void test_build_index_key_compound(void **state) {
    (void)state;
    bson_oid_t oid;
    bson_oid_init(&oid, NULL);

    bson_t *doc = BCON_NEW(
        "_id", BCON_OID(&oid),
        "name", BCON_UTF8("John"),
        "age", BCON_INT32(30),
        "city", BCON_UTF8("NYC")
    );
    bson_t *keys = BCON_NEW("name", BCON_INT32(1), "age", BCON_INT32(-1));

    /* With DUPSORT, include_id is ignored - _id is stored as value, not in key */
    bson_t *key = _build_index_key(doc, keys, true);
    assert_non_null(key);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, key, "name"));
    assert_string_equal(bson_iter_utf8(&iter, NULL), "John");
    assert_true(bson_iter_init_find(&iter, key, "age"));
    assert_int_equal(bson_iter_int32(&iter), 30);
    assert_false(bson_iter_init_find(&iter, key, "_id"));  /* No _id with DUPSORT */

    /* Should NOT have city (not in index spec) */
    assert_false(bson_iter_init_find(&iter, key, "city"));

    bson_destroy(key);
    bson_destroy(doc);
    bson_destroy(keys);
}

static void test_build_index_key_missing_field(void **state) {
    (void)state;
    bson_oid_t oid;
    bson_oid_init(&oid, NULL);

    bson_t *doc = BCON_NEW(
        "_id", BCON_OID(&oid),
        "name", BCON_UTF8("John")
        /* missing "email" field */
    );
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));

    /* With DUPSORT, include_id is ignored - _id is stored as value, not in key */
    bson_t *key = _build_index_key(doc, keys, true);
    assert_non_null(key);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, key, "email"));
    assert_true(BSON_ITER_HOLDS_NULL(&iter));  /* Missing field becomes null */
    assert_false(bson_iter_init_find(&iter, key, "_id"));  /* No _id with DUPSORT */

    bson_destroy(key);
    bson_destroy(doc);
    bson_destroy(keys);
}

static void test_build_index_key_dotted_path(void **state) {
    (void)state;
    bson_oid_t oid;
    bson_oid_init(&oid, NULL);

    bson_t *doc = BCON_NEW(
        "_id", BCON_OID(&oid),
        "address", "{",
            "city", BCON_UTF8("NYC"),
            "zip", BCON_UTF8("10001"),
        "}"
    );
    bson_t *keys = BCON_NEW("address.city", BCON_INT32(1));

    bson_t *key = _build_index_key(doc, keys, false);
    assert_non_null(key);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, key, "address.city"));
    assert_string_equal(bson_iter_utf8(&iter, NULL), "NYC");

    bson_destroy(key);
    bson_destroy(doc);
    bson_destroy(keys);
}

/* ============================================================
 * Tests: _index_key_compare
 * ============================================================ */

static void test_index_key_compare_equal(void **state) {
    (void)state;
    bson_t *k1 = BCON_NEW("email", BCON_UTF8("a@b.com"));
    bson_t *k2 = BCON_NEW("email", BCON_UTF8("a@b.com"));

    int cmp = _index_key_compare(
        bson_get_data(k1), k1->len,
        bson_get_data(k2), k2->len,
        NULL
    );
    assert_int_equal(cmp, 0);

    bson_destroy(k1);
    bson_destroy(k2);
}

static void test_index_key_compare_less(void **state) {
    (void)state;
    bson_t *k1 = BCON_NEW("email", BCON_UTF8("aaa@b.com"));
    bson_t *k2 = BCON_NEW("email", BCON_UTF8("zzz@b.com"));

    int cmp = _index_key_compare(
        bson_get_data(k1), k1->len,
        bson_get_data(k2), k2->len,
        NULL
    );
    assert_true(cmp < 0);

    bson_destroy(k1);
    bson_destroy(k2);
}

static void test_index_key_compare_greater(void **state) {
    (void)state;
    bson_t *k1 = BCON_NEW("age", BCON_INT32(50));
    bson_t *k2 = BCON_NEW("age", BCON_INT32(25));

    int cmp = _index_key_compare(
        bson_get_data(k1), k1->len,
        bson_get_data(k2), k2->len,
        NULL
    );
    assert_true(cmp > 0);

    bson_destroy(k1);
    bson_destroy(k2);
}

static void test_index_key_compare_compound(void **state) {
    (void)state;
    /* Same first field, different second */
    bson_t *k1 = BCON_NEW("name", BCON_UTF8("John"), "age", BCON_INT32(25));
    bson_t *k2 = BCON_NEW("name", BCON_UTF8("John"), "age", BCON_INT32(30));

    int cmp = _index_key_compare(
        bson_get_data(k1), k1->len,
        bson_get_data(k2), k2->len,
        NULL
    );
    assert_true(cmp < 0);  /* 25 < 30 */

    bson_destroy(k1);
    bson_destroy(k2);
}

static void test_index_key_compare_null_handling(void **state) {
    (void)state;
    bson_t *k1 = BCON_NEW("email", BCON_NULL);
    bson_t *k2 = BCON_NEW("email", BCON_UTF8("a@b.com"));

    int cmp = _index_key_compare(
        bson_get_data(k1), k1->len,
        bson_get_data(k2), k2->len,
        NULL
    );
    /* null < string in MongoDB ordering */
    assert_true(cmp < 0);

    bson_destroy(k1);
    bson_destroy(k2);
}

/* ============================================================
 * Tests: _should_index_document (sparse index)
 * ============================================================ */

static void test_should_index_non_sparse(void **state) {
    (void)state;
    bson_t *doc = BCON_NEW("name", BCON_UTF8("John"));
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));

    /* Non-sparse: always index even if field missing */
    assert_true(_should_index_document(doc, keys, false));

    bson_destroy(doc);
    bson_destroy(keys);
}

static void test_should_index_sparse_field_exists(void **state) {
    (void)state;
    bson_t *doc = BCON_NEW("email", BCON_UTF8("a@b.com"));
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));

    /* Sparse: index because field exists */
    assert_true(_should_index_document(doc, keys, true));

    bson_destroy(doc);
    bson_destroy(keys);
}

static void test_should_index_sparse_field_missing(void **state) {
    (void)state;
    bson_t *doc = BCON_NEW("name", BCON_UTF8("John"));
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));

    /* Sparse: don't index because field missing */
    assert_false(_should_index_document(doc, keys, true));

    bson_destroy(doc);
    bson_destroy(keys);
}

static void test_should_index_sparse_field_null(void **state) {
    (void)state;
    bson_t *doc = BCON_NEW("email", BCON_NULL);
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));

    /* Sparse: don't index because field is null */
    assert_false(_should_index_document(doc, keys, true));

    bson_destroy(doc);
    bson_destroy(keys);
}

static void test_should_index_sparse_compound_one_exists(void **state) {
    (void)state;
    bson_t *doc = BCON_NEW("name", BCON_UTF8("John"));
    bson_t *keys = BCON_NEW("name", BCON_INT32(1), "email", BCON_INT32(1));

    /* Sparse compound: index because at least one field exists */
    assert_true(_should_index_document(doc, keys, true));

    bson_destroy(doc);
    bson_destroy(keys);
}

static void test_should_index_sparse_dotted_field(void **state) {
    (void)state;
    bson_t *doc = BCON_NEW(
        "address", "{",
            "city", BCON_UTF8("NYC"),
        "}"
    );
    bson_t *keys = BCON_NEW("address.city", BCON_INT32(1));

    /* Sparse: index because nested field exists */
    assert_true(_should_index_document(doc, keys, true));

    bson_destroy(doc);
    bson_destroy(keys);
}

/* ============================================================
 * Tests: Serialization helpers
 * ============================================================ */

static void test_index_key_serialize_deserialize(void **state) {
    (void)state;
    bson_t *key = BCON_NEW("email", BCON_UTF8("test@example.com"), "age", BCON_INT32(25));

    size_t len;
    uint8_t *data = _index_key_serialize(key, &len);
    assert_non_null(data);
    assert_int_equal(len, key->len);

    bson_t *deserialized = _index_key_deserialize(data, len);
    assert_non_null(deserialized);

    /* Compare */
    int cmp = bson_compare_docs(key, deserialized);
    assert_int_equal(cmp, 0);

    free(data);
    bson_destroy(key);
    bson_destroy(deserialized);
}

static void test_index_value_roundtrip(void **state) {
    (void)state;
    bson_oid_t oid;
    bson_oid_init(&oid, NULL);

    bson_t *doc = BCON_NEW("_id", BCON_OID(&oid), "name", BCON_UTF8("John"));

    size_t len;
    uint8_t *data = _index_value_from_doc(doc, &len);
    assert_non_null(data);

    bson_oid_t extracted;
    bool ok = _index_value_get_oid(data, len, &extracted);
    assert_true(ok);
    assert_int_equal(bson_oid_compare(&oid, &extracted), 0);

    free(data);
    bson_destroy(doc);
}

/* ============================================================
 * Tests: Index spec BSON serialization
 * ============================================================ */

static void test_index_spec_to_bson(void **state) {
    (void)state;
    bson_t *keys = BCON_NEW("email", BCON_INT32(1));
    index_config_t config = {0};
    config.unique = true;
    config.sparse = false;

    bson_t *spec = _index_spec_to_bson("email_1", keys, &config);
    assert_non_null(spec);

    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, spec, "name"));
    assert_string_equal(bson_iter_utf8(&iter, NULL), "email_1");

    assert_true(bson_iter_init_find(&iter, spec, "unique"));
    assert_true(bson_iter_bool(&iter));

    /* sparse should not be present (false) */
    assert_false(bson_iter_init_find(&iter, spec, "sparse"));

    bson_destroy(spec);
    bson_destroy(keys);
}

static void test_index_spec_from_bson(void **state) {
    (void)state;
    bson_t *spec = BCON_NEW(
        "name", BCON_UTF8("email_unique"),
        "key", "{", "email", BCON_INT32(1), "}",
        "unique", BCON_BOOL(true),
        "sparse", BCON_BOOL(true),
        "expireAfterSeconds", BCON_INT64(3600)
    );

    char *name = NULL;
    bson_t *keys = NULL;
    index_config_t config = {0};

    int rc = _index_spec_from_bson(spec, &name, &keys, &config);
    assert_int_equal(rc, MONGOLITE_OK);

    assert_non_null(name);
    assert_string_equal(name, "email_unique");

    assert_non_null(keys);
    bson_iter_t iter;
    assert_true(bson_iter_init_find(&iter, keys, "email"));

    assert_true(config.unique);
    assert_true(config.sparse);
    assert_int_equal(config.expire_after_seconds, 3600);

    free(name);
    bson_destroy(keys);
    bson_destroy(spec);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* _index_name_from_spec */
        cmocka_unit_test(test_index_name_single_field_asc),
        cmocka_unit_test(test_index_name_single_field_desc),
        cmocka_unit_test(test_index_name_compound),
        cmocka_unit_test(test_index_name_dotted_field),
        cmocka_unit_test(test_index_name_three_fields),
        cmocka_unit_test(test_index_name_null_keys),
        cmocka_unit_test(test_index_name_empty_keys),

        /* _build_index_key */
        cmocka_unit_test(test_build_index_key_single_field),
        cmocka_unit_test(test_build_index_key_compound),
        cmocka_unit_test(test_build_index_key_missing_field),
        cmocka_unit_test(test_build_index_key_dotted_path),

        /* _index_key_compare */
        cmocka_unit_test(test_index_key_compare_equal),
        cmocka_unit_test(test_index_key_compare_less),
        cmocka_unit_test(test_index_key_compare_greater),
        cmocka_unit_test(test_index_key_compare_compound),
        cmocka_unit_test(test_index_key_compare_null_handling),

        /* _should_index_document */
        cmocka_unit_test(test_should_index_non_sparse),
        cmocka_unit_test(test_should_index_sparse_field_exists),
        cmocka_unit_test(test_should_index_sparse_field_missing),
        cmocka_unit_test(test_should_index_sparse_field_null),
        cmocka_unit_test(test_should_index_sparse_compound_one_exists),
        cmocka_unit_test(test_should_index_sparse_dotted_field),

        /* Serialization */
        cmocka_unit_test(test_index_key_serialize_deserialize),
        cmocka_unit_test(test_index_value_roundtrip),

        /* Index spec BSON */
        cmocka_unit_test(test_index_spec_to_bson),
        cmocka_unit_test(test_index_spec_from_bson),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
