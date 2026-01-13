/*
 * mongolite_find.c - Find/query operations
 *
 * Handles:
 * - find_one / find
 * - JSON wrappers
 * - _id optimization
 * - bsonmatch integration for filtering
 */

#include "mongolite_internal.h"
#include "mongoc-matcher.h"
#include "macros.h"
#include <stdlib.h>
#include <string.h>

#define MONGOLITE_LIB "mongolite"

/* ============================================================
 * Internal: Check if filter is a simple _id query
 *
 * Returns true if filter is { "_id": <oid> }
 * Extracts the OID if found.
 * ============================================================ */

MONGOLITE_HOT
bool _mongolite_is_id_query(const bson_t *filter, bson_oid_t *out_oid) {
    if (MONGOLITE_UNLIKELY(!filter || bson_empty(filter))) {
        return false;
    }

    /* Check if filter has exactly one field "_id" */
    bson_iter_t iter;
    if (MONGOLITE_UNLIKELY(!bson_iter_init(&iter, filter))) {
        return false;
    }

    int field_count = 0;
    bool has_id = false;

    while (bson_iter_next(&iter)) {
        field_count++;
        if (strcmp(bson_iter_key(&iter), "_id") == 0) {
            has_id = true;
            if (MONGOLITE_LIKELY(BSON_ITER_HOLDS_OID(&iter) && out_oid)) {
                bson_oid_copy(bson_iter_oid(&iter), out_oid);
            } else {
                /* _id is not an OID - can't optimize */
                return false;
            }
        }
    }

    return (field_count == 1 && has_id);
}

/* ============================================================
 * Internal: Get document by _id (direct lookup)
 * ============================================================ */

MONGOLITE_HOT
bson_t* _mongolite_find_by_id(mongolite_db_t *db, wtree3_tree_t *tree,
                               const bson_oid_t *oid, gerror_t *error) {
    wtree3_txn_t *txn = _mongolite_get_read_txn(db, error);
    if (MONGOLITE_UNLIKELY(!txn)) return NULL;

    const void *value;
    size_t value_size;
    int rc = wtree3_get_txn(txn, tree, oid->bytes, sizeof(oid->bytes),
                            &value, &value_size, error);

    if (MONGOLITE_UNLIKELY(rc != 0)) {
        _mongolite_release_read_txn(db, txn);
        return NULL;
    }

    /* Copy BSON document */
    bson_t *doc = bson_new_from_data(value, value_size);

    _mongolite_release_read_txn(db, txn);
    return doc;
}

/* _mongolite_find_one_scan is now in mongolite_util.c */

/* ============================================================
 * Find One
 * ============================================================ */

bson_t* mongolite_find_one(mongolite_db_t *db, const char *collection,
                            const bson_t *filter, const bson_t *projection,
                            gerror_t *error) {
    if (!db || !collection) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "Database and collection are required");
        return NULL;
    }

    _mongolite_lock(db);

    /* Get collection tree (wtree3) */
    wtree3_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (!tree) {
        _mongolite_unlock(db);
        return NULL;
    }

    bson_t *result = NULL;

    /* Optimization 1: direct _id lookup */
    bson_oid_t oid;
    if (_mongolite_is_id_query(filter, &oid)) {
        result = _mongolite_find_by_id(db, tree, &oid, error);
        _mongolite_unlock(db);
        /* TODO: Apply projection if specified */
        (void)projection;
        return result;
    }

    /* Optimization 2: try to use secondary index */
    query_analysis_t *analysis = _analyze_query_for_index(filter);
    if (analysis) {
        mongolite_cached_index_t *idx = _find_best_index(db, collection, analysis, error);
        if (idx && analysis->is_simple_equality) {
            /* Use index for lookup */
            result = _find_one_with_index(db, collection, tree, idx, filter, error);
            _free_query_analysis(analysis);
            _mongolite_unlock(db);
            /* TODO: Apply projection if specified */
            (void)projection;
            return result;
        }
        _free_query_analysis(analysis);
    }

    /* Fallback: Full scan with filter */
    result = _mongolite_find_one_scan(db, tree, collection, filter, error);

    _mongolite_unlock(db);

    /* TODO: Apply projection if specified */
    (void)projection;

    return result;
}

/* ============================================================
 * Find One JSON
 * ============================================================ */

const char* mongolite_find_one_json(mongolite_db_t *db, const char *collection,
                                     const char *filter_json,
                                     const char *projection_json,
                                     gerror_t *error) {
    if (!db || !collection) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "Database and collection are required");
        return NULL;
    }

    /* Parse filter */
    bson_t *filter = NULL;
    if (filter_json && strlen(filter_json) > 0) {
        bson_error_t bson_err;
        filter = bson_new_from_json((const uint8_t*)filter_json, -1, &bson_err);
        if (!filter) {
            set_error(error, "libbson", MONGOLITE_EINVAL,
                     "Invalid filter JSON: %s", bson_err.message);
            return NULL;
        }
    }

    /* Parse projection */
    bson_t *projection = NULL;
    if (projection_json && strlen(projection_json) > 0) {
        bson_error_t bson_err;
        projection = bson_new_from_json((const uint8_t*)projection_json, -1, &bson_err);
        if (!projection) {
            if (filter) bson_destroy(filter);
            set_error(error, "libbson", MONGOLITE_EINVAL,
                     "Invalid projection JSON: %s", bson_err.message);
            return NULL;
        }
    }

    bson_t *doc = mongolite_find_one(db, collection, filter, projection, error);

    if (filter) bson_destroy(filter);
    if (projection) bson_destroy(projection);

    if (!doc) {
        return NULL;
    }

    /* Convert to JSON - caller must free */
    char *json = bson_as_canonical_extended_json(doc, NULL);
    bson_destroy(doc);

    return json;
}

/* ============================================================
 * Find (returns cursor)
 * ============================================================ */

mongolite_cursor_t* mongolite_find(mongolite_db_t *db, const char *collection,
                                    const bson_t *filter, const bson_t *projection,
                                    gerror_t *error) {
    if (!db || !collection) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "Database and collection are required");
        return NULL;
    }

    _mongolite_lock(db);

    /* Get collection tree (wtree3) */
    wtree3_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (!tree) {
        _mongolite_unlock(db);
        return NULL;
    }

    /* Create read transaction */
    wtree3_txn_t *txn = wtree3_txn_begin(db->wdb, false, error);
    if (!txn) {
        _mongolite_unlock(db);
        return NULL;
    }

    /* Create cursor using internal helper */
    mongolite_cursor_t *cursor = _mongolite_cursor_create_with_txn(
        db, tree, collection, txn, filter, error);
    if (!cursor) {
        wtree3_txn_abort(txn);
        _mongolite_unlock(db);
        return NULL;
    }

    /* Take ownership of the transaction */
    cursor->owns_txn = true;

    /* Copy projection if provided */
    if (projection && !bson_empty(projection)) {
        cursor->projection = bson_copy(projection);
    }

    _mongolite_unlock(db);
    return cursor;
}

/* ============================================================
 * Find JSON (returns array of JSON strings)
 * ============================================================ */

char** mongolite_find_json(mongolite_db_t *db, const char *collection,
                            const char *filter_json, const char *projection_json,
                            gerror_t *error) {
    if (!db || !collection) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "Database and collection are required");
        return NULL;
    }

    /* Parse filter */
    bson_t *filter = NULL;
    if (filter_json && strlen(filter_json) > 0) {
        bson_error_t bson_err;
        filter = bson_new_from_json((const uint8_t*)filter_json, -1, &bson_err);
        if (!filter) {
            set_error(error, "libbson", MONGOLITE_EINVAL,
                     "Invalid filter JSON: %s", bson_err.message);
            return NULL;
        }
    }

    /* Parse projection */
    bson_t *projection = NULL;
    if (projection_json && strlen(projection_json) > 0) {
        bson_error_t bson_err;
        projection = bson_new_from_json((const uint8_t*)projection_json, -1, &bson_err);
        if (!projection) {
            if (filter) bson_destroy(filter);
            set_error(error, "libbson", MONGOLITE_EINVAL,
                     "Invalid projection JSON: %s", bson_err.message);
            return NULL;
        }
    }

    /* Get cursor */
    mongolite_cursor_t *cursor = mongolite_find(db, collection, filter, projection, error);

    if (filter) bson_destroy(filter);
    if (projection) bson_destroy(projection);

    if (!cursor) {
        return NULL;
    }

    /* Collect results into array */
    size_t capacity = 16;
    size_t count = 0;
    char **results = calloc(capacity + 1, sizeof(char*));  /* +1 for NULL terminator */

    if (!results) {
        mongolite_cursor_destroy(cursor);
        set_error(error, "system", MONGOLITE_ENOMEM,
                 "Failed to allocate results array");
        return NULL;
    }

    const bson_t *doc;
    while (mongolite_cursor_next(cursor, &doc)) {
        if (count >= capacity) {
            capacity *= 2;
            char **new_results = realloc(results, (capacity + 1) * sizeof(char*));
            if (!new_results) {
                /* Cleanup on failure */
                for (size_t i = 0; i < count; i++) {
                    bson_free(results[i]);
                }
                free(results);
                mongolite_cursor_destroy(cursor);
                set_error(error, "system", MONGOLITE_ENOMEM,
                         "Failed to grow results array");
                return NULL;
            }
            results = new_results;
        }

        results[count] = bson_as_canonical_extended_json(doc, NULL);
        count++;
    }

    results[count] = NULL;  /* NULL terminator */

    mongolite_cursor_destroy(cursor);
    return results;
}
