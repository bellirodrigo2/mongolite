/*
 * mongolite_cursor.c - Cursor operations for iterating query results
 *
 * Handles:
 * - cursor_next / cursor_more
 * - cursor_destroy
 * - limit / skip / sort modifiers
 */

#include "mongolite_internal.h"
#include "mongoc-matcher.h"
#include <stdlib.h>
#include <string.h>

#define MONGOLITE_LIB "mongolite"

/* ============================================================
 * Cursor Next
 *
 * Advances cursor and returns next matching document.
 * Returns true if a document was found, false if exhausted.
 * ============================================================ */

bool mongolite_cursor_next(mongolite_cursor_t *cursor, const bson_t **doc) {
    if (!cursor || cursor->exhausted) {
        if (doc) *doc = NULL;
        return false;
    }

    /* Free previous document */
    if (cursor->current_doc) {
        bson_destroy(cursor->current_doc);
        cursor->current_doc = NULL;
    }

    /* Check limit */
    if (cursor->limit > 0 && cursor->returned >= cursor->limit) {
        cursor->exhausted = true;
        if (doc) *doc = NULL;
        return false;
    }

    /* Start iteration if not started */
    bool has_entry;
    if (cursor->position == 0) {
        has_entry = wtree_iterator_first(cursor->iter);
    } else {
        has_entry = wtree_iterator_next(cursor->iter);
    }

    while (has_entry) {
        cursor->position++;

        const void *value;
        size_t value_size;

        if (!wtree_iterator_value(cursor->iter, &value, &value_size)) {
            has_entry = wtree_iterator_next(cursor->iter);
            continue;
        }

        /* Parse document */
        bson_t temp_doc;
        if (!bson_init_static(&temp_doc, value, value_size)) {
            has_entry = wtree_iterator_next(cursor->iter);
            continue;
        }

        /* Check if matches filter */
        bool matches = true;
        if (cursor->matcher) {
            matches = mongoc_matcher_match(cursor->matcher, &temp_doc);
        }

        if (!matches) {
            has_entry = wtree_iterator_next(cursor->iter);
            continue;
        }

        /* Handle skip */
        if (cursor->skip > 0 && (cursor->returned + cursor->skip) > cursor->position - 1) {
            has_entry = wtree_iterator_next(cursor->iter);
            continue;
        }

        /* Found a matching document */
        cursor->current_doc = bson_copy(&temp_doc);
        cursor->returned++;

        /* TODO: Apply projection */

        if (doc) *doc = cursor->current_doc;
        return true;
    }

    /* No more documents */
    cursor->exhausted = true;
    if (doc) *doc = NULL;
    return false;
}

/* ============================================================
 * Cursor More
 *
 * Returns true if there might be more documents.
 * Note: This is a hint, not a guarantee.
 * ============================================================ */

bool mongolite_cursor_more(mongolite_cursor_t *cursor) {
    if (!cursor) return false;
    return !cursor->exhausted;
}

/* ============================================================
 * Cursor Destroy
 * ============================================================ */

void mongolite_cursor_destroy(mongolite_cursor_t *cursor) {
    if (!cursor) return;

    /* Free current document */
    if (cursor->current_doc) {
        bson_destroy(cursor->current_doc);
    }

    /* Free matcher */
    if (cursor->matcher) {
        mongoc_matcher_destroy(cursor->matcher);
    }

    /* Free projection */
    if (cursor->projection) {
        bson_destroy(cursor->projection);
    }

    /* Free sort */
    if (cursor->sort) {
        bson_destroy(cursor->sort);
    }

    /* Close iterator */
    if (cursor->iter) {
        wtree_iterator_close(cursor->iter);
    }

    /* Abort transaction if we own it */
    if (cursor->owns_txn && cursor->txn) {
        wtree_txn_abort(cursor->txn);
    }

    /* Free sort buffer if any */
    if (cursor->sort_buffer) {
        for (size_t i = 0; i < cursor->sort_buffer_size; i++) {
            if (cursor->sort_buffer[i]) {
                bson_destroy(cursor->sort_buffer[i]);
            }
        }
        free(cursor->sort_buffer);
    }

    /* Free collection name */
    free(cursor->collection_name);

    /* Free cursor */
    free(cursor);
}

/* ============================================================
 * Cursor Set Limit
 * ============================================================ */

int mongolite_cursor_set_limit(mongolite_cursor_t *cursor, int64_t limit) {
    if (!cursor) return MONGOLITE_EINVAL;

    /* Can only set before iteration starts */
    if (cursor->position > 0) {
        return MONGOLITE_ERROR;
    }

    cursor->limit = limit;
    return MONGOLITE_OK;
}

/* ============================================================
 * Cursor Set Skip
 * ============================================================ */

int mongolite_cursor_set_skip(mongolite_cursor_t *cursor, int64_t skip) {
    if (!cursor) return MONGOLITE_EINVAL;

    /* Can only set before iteration starts */
    if (cursor->position > 0) {
        return MONGOLITE_ERROR;
    }

    cursor->skip = skip;
    return MONGOLITE_OK;
}

/* ============================================================
 * Cursor Set Sort
 *
 * Note: Sort requires buffering all matching documents, which
 * can be expensive for large result sets.
 * ============================================================ */

int mongolite_cursor_set_sort(mongolite_cursor_t *cursor, const bson_t *sort) {
    if (!cursor || !sort) return MONGOLITE_EINVAL;

    /* Can only set before iteration starts */
    if (cursor->position > 0) {
        return MONGOLITE_ERROR;
    }

    if (cursor->sort) {
        bson_destroy(cursor->sort);
    }

    cursor->sort = bson_copy(sort);

    /* TODO: Implement sort by buffering results */
    /* For now, sort is stored but not applied */

    return MONGOLITE_OK;
}
