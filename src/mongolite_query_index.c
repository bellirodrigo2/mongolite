/*
 * mongolite_query_index.c - Index-based query optimization
 *
 * Functions:
 * - _analyze_query_for_index() - Analyze query filter for index use
 * - _free_query_analysis() - Free query analysis
 * - _find_best_index() - Find best index for query
 * - _find_one_with_index() - Execute index-based query
 */

#include "mongolite_internal.h"
#include "mongoc-matcher.h"
#include "macros.h"
#include <string.h>
#include <stdlib.h>

#define MONGOLITE_LIB "mongolite"

/* ============================================================
 * Query Analysis
 * ============================================================ */

query_analysis_t* _analyze_query_for_index(const bson_t *filter) {
    if (!filter || bson_empty(filter)) {
        return NULL;
    }

    query_analysis_t *analysis = calloc(1, sizeof(query_analysis_t));
    if (!analysis) return NULL;

    analysis->is_simple_equality = true;
    analysis->equality_fields = NULL;
    analysis->equality_count = 0;

    bson_iter_t iter;
    if (!bson_iter_init(&iter, filter)) {
        free(analysis);
        return NULL;
    }

    /* Count fields and check for simple equality */
    size_t field_count = 0;
    while (bson_iter_next(&iter)) {
        field_count++;
    }

    if (field_count == 0) {
        free(analysis);
        return NULL;
    }

    /* Allocate array for equality field names */
    analysis->equality_fields = calloc(field_count, sizeof(char*));
    if (!analysis->equality_fields) {
        free(analysis);
        return NULL;
    }

    /* Reset iterator and extract equality fields */
    if (!bson_iter_init(&iter, filter)) {
        free(analysis->equality_fields);
        free(analysis);
        return NULL;
    }

    bool has_non_id_field = false;

    while (bson_iter_next(&iter)) {
        const char *key = bson_iter_key(&iter);

        /* If key starts with '$', it's an operator - not simple equality */
        if (key[0] == '$') {
            analysis->is_simple_equality = false;
            continue;
        }

        /* Skip _id field - it has dedicated optimization */
        if (strcmp(key, "_id") == 0) {
            continue;
        }

        has_non_id_field = true;

        /* Check for nested operators (e.g., {"age": {"$gt": 25}}) */
        if (BSON_ITER_HOLDS_DOCUMENT(&iter)) {
            uint32_t subdoc_len;
            const uint8_t *subdoc_data;
            bson_t subdoc;
            bson_iter_t subiter;

            bson_iter_document(&iter, &subdoc_len, &subdoc_data);
            if (bson_init_static(&subdoc, subdoc_data, subdoc_len)) {
                if (bson_iter_init(&subiter, &subdoc)) {
                    while (bson_iter_next(&subiter)) {
                        if (bson_iter_key(&subiter)[0] == '$') {
                            /* Has operator - not simple equality, cleanup and return NULL */
                            for (size_t i = 0; i < analysis->equality_count; i++) {
                                free(analysis->equality_fields[i]);
                            }
                            free(analysis->equality_fields);
                            free(analysis);
                            return NULL;
                        }
                    }
                }
            }
        }

        /* Add to equality fields */
        analysis->equality_fields[analysis->equality_count] = strdup(key);
        if (!analysis->equality_fields[analysis->equality_count]) {
            /* Allocation failed - cleanup */
            for (size_t i = 0; i < analysis->equality_count; i++) {
                free(analysis->equality_fields[i]);
            }
            free(analysis->equality_fields);
            free(analysis);
            return NULL;
        }
        analysis->equality_count++;
    }

    /* If only _id field, return NULL (dedicated optimization handles it) */
    if (!has_non_id_field || analysis->equality_count == 0) {
        free(analysis->equality_fields);
        free(analysis);
        return NULL;
    }

    return analysis;
}

void _free_query_analysis(query_analysis_t *analysis) {
    if (!analysis) return;

    if (analysis->equality_fields) {
        for (size_t i = 0; i < analysis->equality_count; i++) {
            free(analysis->equality_fields[i]);
        }
        free(analysis->equality_fields);
    }
    free(analysis);
}

/* ============================================================
 * Index Selection
 * ============================================================ */

mongolite_cached_index_t* _find_best_index(mongolite_db_t *db,
                                            const char *collection,
                                            const query_analysis_t *analysis,
                                            gerror_t *error) {
    if (!analysis || !analysis->is_simple_equality || analysis->equality_count == 0) {
        return NULL;
    }

    /* Get cached indexes */
    size_t index_count = 0;
    mongolite_cached_index_t *indexes = _mongolite_get_cached_indexes(db, collection, &index_count, error);
    if (!indexes || index_count == 0) {
        return NULL;
    }

    /* Find index that matches query fields */
    for (size_t i = 0; i < index_count; i++) {
        if (!indexes[i].keys) continue;

        /* Check if index keys match query fields */
        bson_iter_t idx_iter;
        if (!bson_iter_init(&idx_iter, indexes[i].keys)) {
            continue;
        }

        bool matches = true;
        size_t matched_count = 0;

        /* Check if all index fields are in the query */
        while (bson_iter_next(&idx_iter)) {
            const char *idx_field = bson_iter_key(&idx_iter);
            bool found = false;

            for (size_t j = 0; j < analysis->equality_count; j++) {
                if (strcmp(idx_field, analysis->equality_fields[j]) == 0) {
                    found = true;
                    matched_count++;
                    break;
                }
            }

            if (!found) {
                matches = false;
                break;
            }
        }

        /* If all index fields are in the query, use this index */
        if (matches && matched_count > 0) {
            return &indexes[i];
        }
    }

    return NULL;
}

/* ============================================================
 * Index-based Query Execution
 * ============================================================ */

static void* _build_index_key_from_filter(const bson_t *filter,
                                          const bson_t *index_keys,
                                          size_t *out_key_len) {
    if (!filter || !index_keys || !out_key_len) {
        return NULL;
    }

    bson_t key_doc;
    bson_init(&key_doc);

    /* Iterate index keys (e.g., {"field1": 1, "field2": -1}) */
    bson_iter_t idx_iter;
    if (!bson_iter_init(&idx_iter, index_keys)) {
        bson_destroy(&key_doc);
        return NULL;
    }

    while (bson_iter_next(&idx_iter)) {
        const char *idx_field = bson_iter_key(&idx_iter);

        /* Look for this field in the filter */
        bson_iter_t filter_iter;
        if (bson_iter_init_find(&filter_iter, filter, idx_field)) {
            /* Append the value to our key document */
            if (!bson_append_iter(&key_doc, idx_field, -1, &filter_iter)) {
                bson_destroy(&key_doc);
                return NULL;
            }
        } else {
            /* Field not in filter - can't use this index */
            bson_destroy(&key_doc);
            return NULL;
        }
    }

    /* Serialize to buffer */
    uint32_t key_len = key_doc.len;
    void *key_buf = malloc(key_len);
    if (!key_buf) {
        bson_destroy(&key_doc);
        return NULL;
    }

    memcpy(key_buf, bson_get_data(&key_doc), key_len);
    bson_destroy(&key_doc);

    *out_key_len = key_len;
    return key_buf;
}

bson_t* _find_one_with_index(mongolite_db_t *db,
                              const char *collection,
                              wtree3_tree_t *col_tree,
                              mongolite_cached_index_t *index,
                              const bson_t *filter,
                              gerror_t *error) {
    (void)collection;  /* Unused */

    /* Build index key from filter */
    size_t index_key_len = 0;
    void *index_key = _build_index_key_from_filter(filter, index->keys, &index_key_len);
    if (!index_key) {
        return NULL;  /* Can't use this index */
    }

    /* Create matcher for validation */
    bson_error_t bson_err;
    mongoc_matcher_t *matcher = mongoc_matcher_new(filter, &bson_err);
    if (!matcher) {
        free(index_key);
        set_error(error, "bsonmatch", MONGOLITE_EQUERY,
                 "Invalid query: %s", bson_err.message);
        return NULL;
    }

    /* Get read transaction */
    wtree3_txn_t *txn = _mongolite_get_read_txn(db, error);
    if (MONGOLITE_UNLIKELY(!txn)) {
        mongoc_matcher_destroy(matcher);
        free(index_key);
        return NULL;
    }

    MDB_txn *mdb_txn = wtree3_txn_get_mdb(txn);
    if (MONGOLITE_UNLIKELY(!mdb_txn)) {
        _mongolite_release_read_txn(db, txn);
        mongoc_matcher_destroy(matcher);
        free(index_key);
        set_error(error, MONGOLITE_LIB, MONGOLITE_ERROR, "Failed to get MDB transaction");
        return NULL;
    }

    /* Open cursor on index DBI */
    MDB_cursor *cursor = NULL;
    int rc = mdb_cursor_open(mdb_txn, index->dbi, &cursor);
    if (MONGOLITE_UNLIKELY(rc != MDB_SUCCESS)) {
        _mongolite_release_read_txn(db, txn);
        mongoc_matcher_destroy(matcher);
        free(index_key);
        set_error(error, "lmdb", rc, "Failed to open cursor: %s", mdb_strerror(rc));
        return NULL;
    }

    /* Position cursor at the index key */
    MDB_val key = {.mv_size = index_key_len, .mv_data = index_key};
    MDB_val val;
    rc = mdb_cursor_get(cursor, &key, &val, MDB_SET_KEY);

    bson_t *result = NULL;

    if (rc == MDB_SUCCESS) {
        /* Found matching index entry - val contains document _id */
        if (val.mv_size == 12) {  /* OID size */
            bson_oid_t doc_oid;
            memcpy(doc_oid.bytes, val.mv_data, 12);

            /* Fetch document from main tree using SAME transaction */
            const void *doc_data;
            size_t doc_len;
            int get_rc = wtree3_get_txn(txn, col_tree, doc_oid.bytes, sizeof(doc_oid.bytes),
                                        &doc_data, &doc_len, error);
            if (get_rc == 0) {
                bson_t *doc = bson_new_from_data(doc_data, doc_len);
                if (doc) {
                    /* Validate with matcher (handles sparse indexes) */
                    if (mongoc_matcher_match(matcher, doc)) {
                        result = doc;
                    } else {
                        bson_destroy(doc);

                        /* For non-unique indexes, try next duplicate */
                        if (!index->unique) {
                            while (mdb_cursor_get(cursor, &key, &val, MDB_NEXT_DUP) == MDB_SUCCESS) {
                                if (val.mv_size == 12) {
                                    memcpy(doc_oid.bytes, val.mv_data, 12);
                                    get_rc = wtree3_get_txn(txn, col_tree, doc_oid.bytes, sizeof(doc_oid.bytes),
                                                            &doc_data, &doc_len, NULL);
                                    if (get_rc == 0) {
                                        doc = bson_new_from_data(doc_data, doc_len);
                                        if (doc && mongoc_matcher_match(matcher, doc)) {
                                            result = doc;
                                            break;
                                        }
                                        if (doc) bson_destroy(doc);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    mdb_cursor_close(cursor);
    _mongolite_release_read_txn(db, txn);
    mongoc_matcher_destroy(matcher);
    free(index_key);
    return result;
}
