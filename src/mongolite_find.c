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
#include <stdlib.h>
#include <string.h>

#define MONGOLITE_LIB "mongolite"

/* ============================================================
 * Internal: Check if filter is a simple _id query
 *
 * Returns true if filter is { "_id": <oid> }
 * Extracts the OID if found.
 * ============================================================ */

static bool _is_id_query(const bson_t *filter, bson_oid_t *out_oid) {
    if (!filter || bson_empty(filter)) {
        return false;
    }

    /* Check if filter has exactly one field "_id" */
    bson_iter_t iter;
    if (!bson_iter_init(&iter, filter)) {
        return false;
    }

    int field_count = 0;
    bool has_id = false;

    while (bson_iter_next(&iter)) {
        field_count++;
        if (strcmp(bson_iter_key(&iter), "_id") == 0) {
            has_id = true;
            if (BSON_ITER_HOLDS_OID(&iter) && out_oid) {
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

static bson_t* _find_by_id(mongolite_db_t *db, wtree_tree_t *tree,
                           const bson_oid_t *oid, gerror_t *error) {
    wtree_txn_t *txn = _mongolite_get_read_txn(db, error);
    if (!txn) return NULL;

    const void *value;
    size_t value_size;
    int rc = wtree_get_txn(txn, tree, oid->bytes, sizeof(oid->bytes),
                           &value, &value_size, error);

    if (rc != 0) {
        _mongolite_abort_if_auto(db, txn);
        return NULL;
    }

    /* Copy BSON document */
    bson_t *doc = bson_new_from_data(value, value_size);

    _mongolite_abort_if_auto(db, txn);
    return doc;
}

/* ============================================================
 * Internal: Full scan to find first matching document
 * ============================================================ */

static bson_t* _find_one_scan(mongolite_db_t *db, wtree_tree_t *tree,
                               const bson_t *filter, gerror_t *error) {
    /* Create matcher if we have a filter */
    mongoc_matcher_t *matcher = NULL;
    if (filter && !bson_empty(filter)) {
        bson_error_t bson_err;
        matcher = mongoc_matcher_new(filter, &bson_err);
        if (!matcher) {
            set_error(error, MONGOLITE_LIB, MONGOLITE_EQUERY,
                     "Invalid query: %s", bson_err.message);
            return NULL;
        }
    }

    wtree_txn_t *txn = _mongolite_get_read_txn(db, error);
    if (!txn) {
        if (matcher) mongoc_matcher_destroy(matcher);
        return NULL;
    }

    wtree_iterator_t *iter = wtree_iterator_create_with_txn(tree, txn, error);
    if (!iter) {
        _mongolite_abort_if_auto(db, txn);
        if (matcher) mongoc_matcher_destroy(matcher);
        return NULL;
    }

    bson_t *result = NULL;

    if (wtree_iterator_first(iter)) {
        do {
            const void *value;
            size_t value_size;

            if (!wtree_iterator_value(iter, &value, &value_size)) {
                continue;
            }

            /* Parse document */
            bson_t doc;
            if (!bson_init_static(&doc, value, value_size)) {
                continue;
            }

            /* Check if matches */
            bool matches = true;
            if (matcher) {
                matches = mongoc_matcher_match(matcher, &doc);
            }

            if (matches) {
                /* Found a match - copy it */
                result = bson_copy(&doc);
                break;
            }
        } while (wtree_iterator_next(iter));
    }

    wtree_iterator_close(iter);
    _mongolite_abort_if_auto(db, txn);
    if (matcher) mongoc_matcher_destroy(matcher);

    return result;
}

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

    /* Get collection tree */
    wtree_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (!tree) {
        _mongolite_unlock(db);
        return NULL;
    }

    bson_t *result = NULL;

    /* Optimize: direct _id lookup */
    bson_oid_t oid;
    if (_is_id_query(filter, &oid)) {
        result = _find_by_id(db, tree, &oid, error);
    } else {
        /* Full scan with filter */
        result = _find_one_scan(db, tree, filter, error);
    }

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
            set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
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
            set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
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

    /* Get collection tree */
    wtree_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (!tree) {
        _mongolite_unlock(db);
        return NULL;
    }

    /* Create cursor */
    mongolite_cursor_t *cursor = calloc(1, sizeof(mongolite_cursor_t));
    if (!cursor) {
        _mongolite_unlock(db);
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM,
                 "Failed to allocate cursor");
        return NULL;
    }

    cursor->db = db;
    cursor->collection_name = strdup(collection);

    /* Create read transaction */
    cursor->txn = wtree_txn_begin(db->wdb, false, error);
    if (!cursor->txn) {
        free(cursor->collection_name);
        free(cursor);
        _mongolite_unlock(db);
        return NULL;
    }
    cursor->owns_txn = true;

    /* Create iterator */
    cursor->iter = wtree_iterator_create_with_txn(tree, cursor->txn, error);
    if (!cursor->iter) {
        wtree_txn_abort(cursor->txn);
        free(cursor->collection_name);
        free(cursor);
        _mongolite_unlock(db);
        return NULL;
    }

    /* Create matcher if we have a filter */
    if (filter && !bson_empty(filter)) {
        bson_error_t bson_err;
        cursor->matcher = mongoc_matcher_new(filter, &bson_err);
        if (!cursor->matcher) {
            wtree_iterator_close(cursor->iter);
            wtree_txn_abort(cursor->txn);
            free(cursor->collection_name);
            free(cursor);
            _mongolite_unlock(db);
            set_error(error, MONGOLITE_LIB, MONGOLITE_EQUERY,
                     "Invalid query: %s", bson_err.message);
            return NULL;
        }
    }

    /* Copy projection if provided */
    if (projection && !bson_empty(projection)) {
        cursor->projection = bson_copy(projection);
    }

    /* Initialize position */
    cursor->limit = 0;  /* No limit */
    cursor->skip = 0;
    cursor->position = 0;
    cursor->returned = 0;
    cursor->exhausted = false;
    cursor->current_doc = NULL;

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
            set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
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
            set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
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
        set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM,
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
                set_error(error, MONGOLITE_LIB, MONGOLITE_ENOMEM,
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
