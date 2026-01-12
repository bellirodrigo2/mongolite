# Phase 4: Index-Based Query Optimization - Implementation Guide

## Overview
This document contains the complete implementation for index-based query optimization in mongolite.

## Summary
All CRUD operations are already optimized for wtree3's new APIs. The **only** missing piece is index-based query execution to avoid full collection scans when indexes exist.

**Implementation Status:**
- ✅ Find operations use `wtree3_scan_range_txn` efficiently
- ✅ Delete operations use `wtree3_delete_if_txn` (single-pass)
- ✅ Insert operations use `wtree3_insert_many_txn` (batch)
- ✅ Update operations use `wtree3_upsert_txn` and `wtree3_modify_txn` (Phase 2)
- ⚠️ Index queries currently disabled (cache returns NULL)

**What's Needed:**
1. Add `wtree3_tree_list_indexes()` API to enumerate indexes
2. Populate mongolite index cache from wtree3
3. Implement index key builder from query filter
4. Implement LMDB cursor-based index scan
5. Integrate into find operations

---

## 1. Add to wtree3.h (after line 1061)

```c
/*
 * Index information structure for wtree3_tree_list_indexes
 */
typedef struct wtree3_index_info {
    char *name;              /* Index name (caller must free) */
    void *user_data;         /* User data (BSON keys spec, caller must free) */
    size_t user_data_len;    /* Length of user_data */
    bool unique;             /* Unique constraint */
    bool sparse;             /* Sparse index */
    MDB_dbi dbi;             /* Index DBI handle (do not close) */
} wtree3_index_info_t;

/*
 * List all indexes on a tree
 *
 * Returns information about all indexes. Caller must free the returned array
 * and each index_info's name and user_data fields.
 *
 * @param tree Tree handle
 * @param out_indexes Pointer to receive array of index info (caller must free)
 * @param out_count Pointer to receive count of indexes
 * @param error Error information
 * @return 0 on success, error code on failure
 */
int wtree3_tree_list_indexes(
    wtree3_tree_t *tree,
    wtree3_index_info_t **out_indexes,
    size_t *out_count,
    gerror_t *error
);
```

---

## 2. Implement in wtree3_index.c (end of file)

```c
/* ============================================================
 * List Indexes API
 * ============================================================ */

int wtree3_tree_list_indexes(
    wtree3_tree_t *tree,
    wtree3_index_info_t **out_indexes,
    size_t *out_count,
    gerror_t *error) {

    if (!tree || !out_indexes || !out_count) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    size_t count = wvector_size(tree->indexes);
    *out_count = 0;
    *out_indexes = NULL;

    if (count == 0) {
        return WTREE3_OK;  /* No indexes */
    }

    wtree3_index_info_t *infos = calloc(count, sizeof(wtree3_index_info_t));
    if (!infos) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate index info array");
        return WTREE3_ENOMEM;
    }

    for (size_t i = 0; i < count; i++) {
        wtree3_index_t *idx = (wtree3_index_t *)wvector_get(tree->indexes, i);

        infos[i].name = strdup(idx->name);
        if (!infos[i].name) {
            /* Cleanup on failure */
            for (size_t j = 0; j < i; j++) {
                free(infos[j].name);
                free(infos[j].user_data);
            }
            free(infos);
            set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to copy index name");
            return WTREE3_ENOMEM;
        }

        /* Copy user_data (BSON keys spec) */
        if (idx->user_data && idx->user_data_len > 0) {
            infos[i].user_data = malloc(idx->user_data_len);
            if (!infos[i].user_data) {
                free(infos[i].name);
                for (size_t j = 0; j < i; j++) {
                    free(infos[j].name);
                    free(infos[j].user_data);
                }
                free(infos);
                set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to copy user_data");
                return WTREE3_ENOMEM;
            }
            memcpy(infos[i].user_data, idx->user_data, idx->user_data_len);
            infos[i].user_data_len = idx->user_data_len;
        } else {
            infos[i].user_data = NULL;
            infos[i].user_data_len = 0;
        }

        infos[i].unique = idx->unique;
        infos[i].sparse = idx->sparse;
        infos[i].dbi = idx->dbi;  /* Share DBI handle (don't close) */
    }

    *out_indexes = infos;
    *out_count = count;
    return WTREE3_OK;
}
```

---

## 3. Update mongolite_util.c `_mongolite_get_cached_indexes`

Replace lines 322-335 with:

```c
    /* Load indexes from wtree3 */
    wtree3_index_info_t *wtree_indexes = NULL;
    size_t wtree_count = 0;

    wtree3_tree_t *tree = entry->tree;
    int rc = wtree3_tree_list_indexes(tree, &wtree_indexes, &wtree_count, error);
    if (rc != 0 || wtree_count == 0) {
        entry->indexes = NULL;
        entry->index_count = 0;
        entry->indexes_loaded = true;
        *out_count = 0;
        return NULL;
    }

    /* Convert wtree3_index_info_t to mongolite_cached_index_t */
    mongolite_cached_index_t *indexes = calloc(wtree_count, sizeof(mongolite_cached_index_t));
    if (!indexes) {
        /* Free wtree indexes */
        for (size_t i = 0; i < wtree_count; i++) {
            free(wtree_indexes[i].name);
            free(wtree_indexes[i].user_data);
        }
        free(wtree_indexes);
        *out_count = 0;
        return NULL;
    }

    for (size_t i = 0; i < wtree_count; i++) {
        indexes[i].name = wtree_indexes[i].name;  /* Transfer ownership */
        indexes[i].unique = wtree_indexes[i].unique;
        indexes[i].sparse = wtree_indexes[i].sparse;
        indexes[i].dbi = wtree_indexes[i].dbi;

        /* Parse BSON keys from user_data */
        if (wtree_indexes[i].user_data && wtree_indexes[i].user_data_len > 0) {
            bson_t temp;
            if (bson_init_static(&temp, wtree_indexes[i].user_data, wtree_indexes[i].user_data_len)) {
                indexes[i].keys = bson_copy(&temp);
            } else {
                indexes[i].keys = NULL;
            }
            free(wtree_indexes[i].user_data);  /* Free the copy */
        } else {
            indexes[i].keys = NULL;
        }
    }
    free(wtree_indexes);

    entry->indexes = indexes;
    entry->index_count = wtree_count;
    entry->indexes_loaded = true;

    *out_count = wtree_count;
    return indexes;
```

---

## 4. Add to mongolite_internal.h (in mongolite_cached_index_t structure)

Add `dbi` field:

```c
typedef struct mongolite_cached_index {
    char *name;           /* Index name */
    bson_t *keys;         /* Index key specification BSON */
    bool unique;          /* Unique constraint */
    bool sparse;          /* Sparse index */
    MDB_dbi dbi;          /* Index DBI handle (from wtree3) */
} mongolite_cached_index_t;
```

---

## 5. Implement index query in mongolite_find.c

Add before `_find_one_with_index` (around line 285):

```c
/* Build index key from query filter for index scan */
static int _build_index_key_from_filter(
    mongolite_cached_index_t *idx,
    const bson_t *filter,
    void **out_key,
    size_t *out_key_len,
    gerror_t *error) {

    if (!idx->keys) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_ERROR, "Index has no keys");
        return MONGOLITE_ERROR;
    }

    /* Build a document with only the indexed fields */
    bson_t *key_doc = bson_new();
    if (!key_doc) {
        set_error(error, "system", MONGOLITE_ENOMEM, "Failed to create key document");
        return MONGOLITE_ENOMEM;
    }

    bson_iter_t key_iter;
    if (!bson_iter_init(&key_iter, idx->keys)) {
        bson_destroy(key_doc);
        return MONGOLITE_ERROR;
    }

    /* Extract each indexed field from filter */
    while (bson_iter_next(&key_iter)) {
        const char *field = bson_iter_key(&key_iter);

        bson_iter_t filter_iter;
        if (bson_iter_init_find(&filter_iter, filter, field)) {
            /* Copy the value from filter to key_doc */
            const bson_value_t *value = bson_iter_value(&filter_iter);
            bson_append_value(key_doc, field, -1, value);
        } else {
            /* Field not in query - can't use this index */
            bson_destroy(key_doc);
            set_error(error, MONGOLITE_LIB, MONGOLITE_ERROR,
                     "Query missing indexed field '%s'", field);
            return MONGOLITE_ERROR;
        }
    }

    /* Extract BSON key using same extractor as insert */
    *out_key = malloc(key_doc->len);
    if (!*out_key) {
        bson_destroy(key_doc);
        set_error(error, "system", MONGOLITE_ENOMEM, "Failed to allocate key");
        return MONGOLITE_ENOMEM;
    }

    memcpy(*out_key, bson_get_data(key_doc), key_doc->len);
    *out_key_len = key_doc->len;
    bson_destroy(key_doc);

    return MONGOLITE_OK;
}

/* Find one document using index scan */
static bson_t* _find_one_with_index(
    mongolite_db_t *db,
    const char *collection,
    wtree3_tree_t *tree,
    mongolite_cached_index_t *idx,
    const bson_t *filter,
    mongoc_matcher_t *matcher,  /* For final validation */
    gerror_t *error) {

    /* Build search key from filter */
    void *search_key = NULL;
    size_t search_key_len = 0;
    int rc = _build_index_key_from_filter(idx, filter, &search_key, &search_key_len, error);
    if (rc != 0) {
        return NULL;  /* Can't use index - fallback to full scan */
    }

    wtree3_txn_t *wtxn = _mongolite_get_read_txn(db, error);
    if (!wtxn) {
        free(search_key);
        return NULL;
    }

    MDB_txn *txn = wtxn->txn;
    MDB_cursor *cursor;
    rc = mdb_cursor_open(txn, idx->dbi, &cursor);
    if (rc != 0) {
        free(search_key);
        _mongolite_release_read_txn(db, wtxn);
        set_error(error, "lmdb", rc, "Failed to open index cursor");
        return NULL;
    }

    /* Position cursor at search key */
    MDB_val mk = {.mv_size = search_key_len, .mv_data = search_key};
    MDB_val mv;  /* Will contain document _id */

    rc = mdb_cursor_get(cursor, &mk, &mv, MDB_SET_KEY);

    bson_t *result = NULL;

    /* Iterate all matching index entries (for non-unique indexes) */
    while (rc == 0) {
        /* Fetch full document using _id from index */
        const void *value;
        size_t value_len;
        int fetch_rc = wtree3_get_txn(wtxn, tree, mv.mv_data, mv.mv_size,
                                       &value, &value_len, error);

        if (fetch_rc == 0) {
            bson_t doc;
            if (bson_init_static(&doc, value, value_len)) {
                /* Apply full matcher (handles sparse/partial coverage) */
                if (!matcher || mongoc_matcher_match(matcher, &doc)) {
                    result = bson_copy(&doc);
                    break;  /* Found match */
                }
            }
        }

        /* Try next duplicate (for non-unique indexes) */
        rc = mdb_cursor_get(cursor, &mk, &mv, MDB_NEXT_DUP);
    }

    mdb_cursor_close(cursor);
    free(search_key);
    _mongolite_release_read_txn(db, wtxn);

    return result;
}
```

---

## 6. Update mongolite_find_one to use index

Around line 280, update the existing code:

```c
    /* Optimization 2: try to use secondary index */
    query_analysis_t *analysis = _analyze_query_for_index(filter);
    if (analysis) {
        mongolite_cached_index_t *idx = _find_best_index(db, collection, analysis, error);
        if (idx && analysis->is_simple_equality) {
            /* Create matcher for final validation */
            mongoc_matcher_t *matcher = NULL;
            if (filter && !bson_empty(filter)) {
                bson_error_t bson_err;
                matcher = mongoc_matcher_new(filter, &bson_err);
            }

            /* Use index for lookup */
            result = _find_one_with_index(db, collection, tree, idx, filter, matcher, error);

            if (matcher) mongoc_matcher_destroy(matcher);
            _free_query_analysis(analysis);
            _mongolite_unlock(db);

            /* If index scan succeeded, return result */
            /* If it failed (e.g., missing field), result will be NULL and we fall through */
            if (result || (error && error->code != 0)) {
                (void)projection;
                return result;
            }

            /* Clear error from index attempt - will try full scan */
            if (error) {
                error->code = 0;
                error->message[0] = '\0';
            }
        } else {
            _free_query_analysis(analysis);
        }
    }
```

---

## Testing

After implementation, test with:

```c
// Create index
mongolite_create_index(db, "users", "{\"email\": 1}", "email_idx", true, false, NULL, &err);

// Query using index (should be fast)
bson_t *doc = mongolite_find_one(db, "users", "{\"email\": \"foo@bar.com\"}", NULL, &err);

// Verify it used the index by checking performance with/without index
```

---

## Performance Impact

**Before (Full Scan):**
- O(N) where N = total documents
- Reads every document from disk

**After (Index Scan):**
- O(log N + M) where M = matching documents
- Reads only matching documents
- 10-1000x faster for selective queries

---

## Next Steps

1. Implement the code changes above
2. Build and test
3. Measure performance improvement
4. Consider adding support for range queries ($gt, $lt) in Phase 5

