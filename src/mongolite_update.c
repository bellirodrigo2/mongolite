/*
 * mongolite_update.c - Update and replace operations (Phase 6)
 *
 * Functions:
 * - mongolite_update_one() - Update first matching document
 * - mongolite_update_many() - Update all matching documents
 * - mongolite_replace_one() - Replace entire document
 * - JSON wrappers for all operations
 *
 * Update operators supported:
 * - $set - Set field values
 * - $unset - Remove fields
 * - $inc - Increment numeric values
 * - $push - Append to array
 * - $pull - Remove from array
 * - $rename - Rename field
 */

#include "mongolite_internal.h"
#include "key_compare.h"
#include <string.h>
#include <stdlib.h>

#define MONGOLITE_LIB "mongolite"

/* ============================================================
 * Helper: Apply $set operator
 * Returns a NEW document (caller must free), or NULL on error.
 * ============================================================ */

static bson_t* _apply_set(const bson_t *doc, bson_iter_t *set_iter, gerror_t *error) {
    bson_iter_t field_iter;

    if (!bson_iter_recurse(set_iter, &field_iter)) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "$set requires a document");
        return NULL;
    }

    /* Start with a copy of the input document */
    bson_t *result = bson_copy(doc);
    if (!result) {
        set_error(error, "system", MONGOLITE_ENOMEM, "Failed to copy document");
        return NULL;
    }

    while (bson_iter_next(&field_iter)) {
        const char *field_name = bson_iter_key(&field_iter);

        /* Build new document: copy all fields except this one, then add new value */
        bson_t *tmp = bson_new();
        if (!tmp) {
            bson_destroy(result);
            set_error(error, "system", MONGOLITE_ENOMEM, "Out of memory");
            return NULL;
        }

        /* Copy all fields except the one we're setting */
        bson_iter_t result_iter;
        bson_iter_init(&result_iter, result);
        while (bson_iter_next(&result_iter)) {
            const char *key = bson_iter_key(&result_iter);
            if (strcmp(key, field_name) != 0) {
                bson_append_iter(tmp, key, -1, &result_iter);
            }
        }

        /* Add new field value */
        if (!bson_append_iter(tmp, field_name, -1, &field_iter)) {
            bson_destroy(tmp);
            bson_destroy(result);
            set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Failed to append field: %s", field_name);
            return NULL;
        }

        /* Replace result with tmp */
        bson_destroy(result);
        result = tmp;
    }

    return result;
}

/* ============================================================
 * Helper: Apply $unset operator
 * Returns a NEW document (caller must free), or NULL on error.
 * ============================================================ */

static bson_t* _apply_unset(const bson_t *doc, bson_iter_t *unset_iter, gerror_t *error) {
    bson_iter_t field_iter;

    if (!bson_iter_recurse(unset_iter, &field_iter)) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "$unset requires a document");
        return NULL;
    }

    /* Collect field names to remove */
    char **fields = NULL;
    size_t n_fields = 0;

    while (bson_iter_next(&field_iter)) {
        const char *field_name = bson_iter_key(&field_iter);
        fields = realloc(fields, sizeof(char*) * (n_fields + 1));
        if (!fields) {
            set_error(error, "system", MONGOLITE_ENOMEM, "Out of memory");
            return NULL;
        }
        fields[n_fields++] = (char*)field_name;
    }

    /* Create new document without those fields */
    bson_t *result = bson_new();
    if (!result) {
        free(fields);
        set_error(error, "system", MONGOLITE_ENOMEM, "Out of memory");
        return NULL;
    }

    /* Copy all fields except the ones in the unset list */
    bson_iter_t doc_iter;
    bson_iter_init(&doc_iter, doc);

    while (bson_iter_next(&doc_iter)) {
        const char *key = bson_iter_key(&doc_iter);
        bool skip = false;

        for (size_t i = 0; i < n_fields; i++) {
            if (strcmp(key, fields[i]) == 0) {
                skip = true;
                break;
            }
        }

        if (!skip) {
            bson_append_iter(result, key, -1, &doc_iter);
        }
    }

    free(fields);
    return result;
}

/* ============================================================
 * Helper: Apply $inc operator
 * Returns a NEW document (caller must free), or NULL on error.
 * ============================================================ */

static bson_t* _apply_inc(const bson_t *doc, bson_iter_t *inc_iter, gerror_t *error) {
    bson_iter_t field_iter;

    if (!bson_iter_recurse(inc_iter, &field_iter)) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "$inc requires a document");
        return NULL;
    }

    /* Start with a copy of the input document */
    bson_t *result = bson_copy(doc);
    if (!result) {
        set_error(error, "system", MONGOLITE_ENOMEM, "Failed to copy document");
        return NULL;
    }

    while (bson_iter_next(&field_iter)) {
        const char *field_name = bson_iter_key(&field_iter);

        /* Get increment value */
        double inc_value = 0;
        bson_type_t inc_type = bson_iter_type(&field_iter);

        if (inc_type == BSON_TYPE_INT32) {
            inc_value = bson_iter_int32(&field_iter);
        } else if (inc_type == BSON_TYPE_INT64) {
            inc_value = (double)bson_iter_int64(&field_iter);
        } else if (inc_type == BSON_TYPE_DOUBLE) {
            inc_value = bson_iter_double(&field_iter);
        } else {
            bson_destroy(result);
            set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "$inc value must be numeric");
            return NULL;
        }

        /* Find existing field */
        bson_iter_t doc_field;
        double new_value;
        bson_type_t result_type = inc_type;

        if (bson_iter_init_find(&doc_field, result, field_name)) {
            /* Field exists - increment it */
            double current_value = 0;
            bson_type_t field_type = bson_iter_type(&doc_field);

            if (field_type == BSON_TYPE_INT32) {
                current_value = bson_iter_int32(&doc_field);
            } else if (field_type == BSON_TYPE_INT64) {
                current_value = (double)bson_iter_int64(&doc_field);
            } else if (field_type == BSON_TYPE_DOUBLE) {
                current_value = bson_iter_double(&doc_field);
            } else {
                bson_destroy(result);
                set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "$inc field must be numeric");
                return NULL;
            }

            new_value = current_value + inc_value;

            /* Determine result type */
            if (field_type == BSON_TYPE_INT32 && inc_type == BSON_TYPE_INT32) {
                result_type = BSON_TYPE_INT32;
            } else if (field_type == BSON_TYPE_INT64 || inc_type == BSON_TYPE_INT64) {
                result_type = BSON_TYPE_INT64;
            } else {
                result_type = BSON_TYPE_DOUBLE;
            }
        } else {
            /* Field doesn't exist - use increment value directly */
            new_value = inc_value;
        }

        /* Build new document: copy all fields except this one, then add new value */
        bson_t *tmp = bson_new();
        if (!tmp) {
            bson_destroy(result);
            set_error(error, "system", MONGOLITE_ENOMEM, "Out of memory");
            return NULL;
        }

        /* Copy all fields except the one we're incrementing */
        bson_iter_t result_iter;
        bson_iter_init(&result_iter, result);
        while (bson_iter_next(&result_iter)) {
            const char *key = bson_iter_key(&result_iter);
            if (strcmp(key, field_name) != 0) {
                bson_append_iter(tmp, key, -1, &result_iter);
            }
        }

        /* Add the incremented value */
        if (result_type == BSON_TYPE_INT32) {
            BSON_APPEND_INT32(tmp, field_name, (int32_t)new_value);
        } else if (result_type == BSON_TYPE_INT64) {
            BSON_APPEND_INT64(tmp, field_name, (int64_t)new_value);
        } else {
            BSON_APPEND_DOUBLE(tmp, field_name, new_value);
        }

        /* Replace result with tmp */
        bson_destroy(result);
        result = tmp;
    }

    return result;
}

/* ============================================================
 * Helper: Apply $push operator
 * Returns a NEW document (caller must free), or NULL on error.
 * ============================================================ */

static bson_t* _apply_push(const bson_t *doc, bson_iter_t *push_iter, gerror_t *error) {
    bson_iter_t field_iter;

    if (!bson_iter_recurse(push_iter, &field_iter)) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "$push requires a document");
        return NULL;
    }

    /* Start with a copy of the input document */
    bson_t *result = bson_copy(doc);
    if (!result) {
        set_error(error, "system", MONGOLITE_ENOMEM, "Failed to copy document");
        return NULL;
    }

    while (bson_iter_next(&field_iter)) {
        const char *field_name = bson_iter_key(&field_iter);

        /* Check if field exists and is array */
        bson_iter_t doc_field;
        bson_t new_array;
        bson_init(&new_array);

        if (bson_iter_init_find(&doc_field, result, field_name)) {
            if (!BSON_ITER_HOLDS_ARRAY(&doc_field)) {
                bson_destroy(&new_array);
                bson_destroy(result);
                set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "$push field must be array");
                return NULL;
            }

            /* Copy existing array elements */
            bson_iter_t array_iter;
            bson_iter_recurse(&doc_field, &array_iter);

            while (bson_iter_next(&array_iter)) {
                bson_append_iter(&new_array, bson_iter_key(&array_iter), -1, &array_iter);
            }

            /* Append new value */
            char index_str[16];
            uint32_t array_len = bson_count_keys(&new_array);
            snprintf(index_str, sizeof(index_str), "%u", array_len);
            bson_append_iter(&new_array, index_str, -1, &field_iter);

        } else {
            /* Field doesn't exist - create new array with one element */
            bson_append_iter(&new_array, "0", -1, &field_iter);
        }

        /* Build new document: copy all fields except this one, then add new array */
        bson_t *tmp = bson_new();
        if (!tmp) {
            bson_destroy(&new_array);
            bson_destroy(result);
            set_error(error, "system", MONGOLITE_ENOMEM, "Out of memory");
            return NULL;
        }

        /* Copy all fields except the one we're pushing to */
        bson_iter_t result_iter;
        bson_iter_init(&result_iter, result);
        while (bson_iter_next(&result_iter)) {
            const char *key = bson_iter_key(&result_iter);
            if (strcmp(key, field_name) != 0) {
                bson_append_iter(tmp, key, -1, &result_iter);
            }
        }

        /* Add the new array */
        BSON_APPEND_ARRAY(tmp, field_name, &new_array);

        bson_destroy(&new_array);
        bson_destroy(result);
        result = tmp;
    }

    return result;
}

/* ============================================================
 * Helper: Apply $pull operator
 * Returns a NEW document (caller must free), or NULL on error.
 * ============================================================ */

static bson_t* _apply_pull(const bson_t *doc, bson_iter_t *pull_iter, gerror_t *error) {
    bson_iter_t field_iter;

    if (!bson_iter_recurse(pull_iter, &field_iter)) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "$pull requires a document");
        return NULL;
    }

    /* Start with a copy of the input document */
    bson_t *result = bson_copy(doc);
    if (!result) {
        set_error(error, "system", MONGOLITE_ENOMEM, "Failed to copy document");
        return NULL;
    }

    while (bson_iter_next(&field_iter)) {
        const char *field_name = bson_iter_key(&field_iter);

        /* Get the value to pull */
        bson_iter_t pull_value_iter = field_iter;

        /* Check if field exists and is array */
        bson_iter_t doc_field;
        if (bson_iter_init_find(&doc_field, result, field_name)) {
            if (!BSON_ITER_HOLDS_ARRAY(&doc_field)) {
                bson_destroy(result);
                set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "$pull field must be array");
                return NULL;
            }

            /* Create new array without matching elements */
            bson_t new_array;
            bson_init(&new_array);

            bson_iter_t array_iter;
            bson_iter_recurse(&doc_field, &array_iter);

            uint32_t new_index = 0;
            while (bson_iter_next(&array_iter)) {
                /* Compare value - if not equal, keep it */
                if (mongodb_compare_iter(&array_iter, &pull_value_iter) != 0) {
                    char index_str[16];
                    snprintf(index_str, sizeof(index_str), "%u", new_index++);
                    bson_append_iter(&new_array, index_str, -1, &array_iter);
                }
            }

            /* Build new document: copy all fields except this one, then add new array */
            bson_t *tmp = bson_new();
            if (!tmp) {
                bson_destroy(&new_array);
                bson_destroy(result);
                set_error(error, "system", MONGOLITE_ENOMEM, "Out of memory");
                return NULL;
            }

            /* Copy all fields except the one we're pulling from */
            bson_iter_t result_iter;
            bson_iter_init(&result_iter, result);
            while (bson_iter_next(&result_iter)) {
                const char *key = bson_iter_key(&result_iter);
                if (strcmp(key, field_name) != 0) {
                    bson_append_iter(tmp, key, -1, &result_iter);
                }
            }

            /* Add the new array */
            BSON_APPEND_ARRAY(tmp, field_name, &new_array);

            bson_destroy(&new_array);
            bson_destroy(result);
            result = tmp;
        }
    }

    return result;
}

/* ============================================================
 * Helper: Apply $rename operator
 * Returns a NEW document (caller must free), or NULL on error.
 * ============================================================ */

static bson_t* _apply_rename(const bson_t *doc, bson_iter_t *rename_iter, gerror_t *error) {
    bson_iter_t field_iter;

    if (!bson_iter_recurse(rename_iter, &field_iter)) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "$rename requires a document");
        return NULL;
    }

    /* Start with a copy of the input document */
    bson_t *result = bson_copy(doc);
    if (!result) {
        set_error(error, "system", MONGOLITE_ENOMEM, "Failed to copy document");
        return NULL;
    }

    while (bson_iter_next(&field_iter)) {
        const char *old_name = bson_iter_key(&field_iter);

        if (!BSON_ITER_HOLDS_UTF8(&field_iter)) {
            bson_destroy(result);
            set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "$rename new name must be string");
            return NULL;
        }

        const char *new_name = bson_iter_utf8(&field_iter, NULL);

        /* Find old field */
        bson_iter_t doc_field;
        if (bson_iter_init_find(&doc_field, result, old_name)) {
            /* Build new document: copy all fields except old, add with new name */
            bson_t *tmp = bson_new();
            if (!tmp) {
                bson_destroy(result);
                set_error(error, "system", MONGOLITE_ENOMEM, "Out of memory");
                return NULL;
            }

            /* Copy all fields except the one being renamed */
            bson_iter_t result_iter;
            bson_iter_init(&result_iter, result);
            while (bson_iter_next(&result_iter)) {
                const char *key = bson_iter_key(&result_iter);
                if (strcmp(key, old_name) != 0) {
                    bson_append_iter(tmp, key, -1, &result_iter);
                }
            }

            /* Add field with new name (doc_field still points to valid result data) */
            bson_append_iter(tmp, new_name, -1, &doc_field);

            bson_destroy(result);
            result = tmp;
        }
    }

    return result;
}

/* ============================================================
 * Helper: Apply update operators to document
 * ============================================================ */

static bson_t* _apply_update(const bson_t *original, const bson_t *update, gerror_t *error) {
    bson_t *doc = bson_copy(original);
    if (!doc) {
        set_error(error, "system", MONGOLITE_ENOMEM, "Failed to copy document");
        return NULL;
    }

    bson_iter_t iter;
    bson_iter_init(&iter, update);

    while (bson_iter_next(&iter)) {
        const char *op = bson_iter_key(&iter);
        bson_t *new_doc = NULL;

        if (strcmp(op, "$set") == 0) {
            new_doc = _apply_set(doc, &iter, error);
        } else if (strcmp(op, "$unset") == 0) {
            new_doc = _apply_unset(doc, &iter, error);
        } else if (strcmp(op, "$inc") == 0) {
            new_doc = _apply_inc(doc, &iter, error);
        } else if (strcmp(op, "$push") == 0) {
            new_doc = _apply_push(doc, &iter, error);
        } else if (strcmp(op, "$pull") == 0) {
            new_doc = _apply_pull(doc, &iter, error);
        } else if (strcmp(op, "$rename") == 0) {
            new_doc = _apply_rename(doc, &iter, error);
        } else {
            bson_destroy(doc);
            set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Unknown update operator: %s", op);
            return NULL;
        }

        if (!new_doc) {
            bson_destroy(doc);
            return NULL;
        }

        /* Replace old document with new one */
        bson_destroy(doc);
        doc = new_doc;
    }

    return doc;
}

/* ============================================================
 * Update one document
 * ============================================================ */

int mongolite_update_one(mongolite_db_t *db, const char *collection,
                         const bson_t *filter, const bson_t *update,
                         bool upsert, gerror_t *error) {
    if (!db || !collection || !update) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return -1;
    }

    /* OPTIMIZATION: Check for direct _id lookup */
    bson_oid_t oid;
    bson_t *existing = NULL;

    if (_mongolite_is_id_query(filter, &oid)) {
        /* Fast path: direct _id lookup */
        _mongolite_lock(db);
        wtree_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
        if (tree) {
            existing = _mongolite_find_by_id(db, tree, &oid, error);
        }
        _mongolite_unlock(db);
    } else {
        /* Slow path: full scan */
        existing = mongolite_find_one(db, collection, filter, NULL, error);
    }

    if (!existing) {
        /* No match found */
        if (upsert) {
            /* TODO: Implement upsert - create new document */
            set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Upsert not yet implemented");
            return -1;
        }
        return 0;  /* No error, just no match */
    }

    /* Extract _id */
    bson_iter_t id_iter;
    if (!bson_iter_init_find(&id_iter, existing, "_id")) {
        bson_destroy(existing);
        set_error(error, MONGOLITE_LIB, MONGOLITE_ERROR, "Document missing _id");
        return -1;
    }
    bson_oid_t doc_id;
    bson_oid_copy(bson_iter_oid(&id_iter), &doc_id);

    /* Apply update operators */
    bson_t *updated = _apply_update(existing, update, error);
    bson_destroy(existing);

    if (!updated) {
        return -1;
    }

    /* Lock database */
    _mongolite_lock(db);

    /* Get collection tree */
    wtree_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (!tree) {
        bson_destroy(updated);
        _mongolite_unlock(db);
        return -1;
    }

    /* Begin transaction */
    wtree_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (!txn) {
        bson_destroy(updated);
        _mongolite_unlock(db);
        return -1;
    }

    /* Update document (overwrites existing) */
    int rc = wtree_update_txn(txn, tree,
                              doc_id.bytes, sizeof(doc_id.bytes),
                              bson_get_data(updated), updated->len,
                              error);
    if (rc != 0) {
        _mongolite_abort_if_auto(db, txn);
        bson_destroy(updated);
        _mongolite_unlock(db);
        return -1;
    }

    rc = _mongolite_commit_if_auto(db, txn, error);
    if (rc != 0) {
        bson_destroy(updated);
        _mongolite_unlock(db);
        return -1;
    }

    bson_destroy(updated);
    db->changes = 1;
    _mongolite_unlock(db);
    return 0;
}

/* ============================================================
 * Update many documents
 * ============================================================ */

int mongolite_update_many(mongolite_db_t *db, const char *collection,
                         const bson_t *filter, const bson_t *update,
                         bool upsert, int64_t *modified_count, gerror_t *error) {
    if (!db || !collection || !update) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return -1;
    }

    if (modified_count) {
        *modified_count = 0;
    }

    /* Lock database */
    _mongolite_lock(db);

    /* Get collection tree */
    wtree_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (!tree) {
        _mongolite_unlock(db);
        return -1;
    }

    /* Begin transaction */
    wtree_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (!txn) {
        _mongolite_unlock(db);
        return -1;
    }

    /* Create cursor using existing transaction (avoids deadlock) */
    mongolite_cursor_t *cursor = _mongolite_cursor_create_with_txn(db, tree, collection,
                                                                    txn, filter, error);
    if (!cursor) {
        _mongolite_abort_if_auto(db, txn);
        _mongolite_unlock(db);
        return -1;
    }

    int64_t count = 0;
    const bson_t *doc;

    while (mongolite_cursor_next(cursor, &doc)) {
        /* Extract _id */
        bson_iter_t id_iter;
        if (!bson_iter_init_find(&id_iter, doc, "_id")) {
            continue;
        }
        bson_oid_t doc_id;
        bson_oid_copy(bson_iter_oid(&id_iter), &doc_id);

        /* Apply update operators */
        bson_t *updated = _apply_update(doc, update, error);
        if (!updated) {
            mongolite_cursor_destroy(cursor);
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return -1;
        }

        /* Update document */
        int rc = wtree_update_txn(txn, tree,
                                  doc_id.bytes, sizeof(doc_id.bytes),
                                  bson_get_data(updated), updated->len,
                                  error);
        bson_destroy(updated);

        if (rc != 0) {
            mongolite_cursor_destroy(cursor);
            _mongolite_abort_if_auto(db, txn);
            _mongolite_unlock(db);
            return -1;
        }

        count++;
    }

    mongolite_cursor_destroy(cursor);

    /* Handle upsert if no matches */
    if (count == 0 && upsert) {
        /* TODO: Implement upsert */
        _mongolite_abort_if_auto(db, txn);
        _mongolite_unlock(db);
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Upsert not yet implemented");
        return -1;
    }

    int rc = _mongolite_commit_if_auto(db, txn, error);
    if (rc != 0) {
        _mongolite_unlock(db);
        return -1;
    }

    if (modified_count) {
        *modified_count = count;
    }
    db->changes = (int)count;
    _mongolite_unlock(db);
    return 0;
}

/* ============================================================
 * Replace one document
 * ============================================================ */

int mongolite_replace_one(mongolite_db_t *db, const char *collection,
                         const bson_t *filter, const bson_t *replacement,
                         bool upsert, gerror_t *error) {
    if (!db || !collection || !replacement) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Invalid parameters");
        return -1;
    }

    /* Replacement must not contain update operators */
    bson_iter_t iter;
    bson_iter_init(&iter, replacement);
    while (bson_iter_next(&iter)) {
        const char *key = bson_iter_key(&iter);
        if (key[0] == '$') {
            set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Replacement cannot contain operators");
            return -1;
        }
    }

    /* Find the first matching document */
    bson_t *existing = mongolite_find_one(db, collection, filter, NULL, error);

    if (!existing) {
        /* No match found */
        if (upsert) {
            /* TODO: Implement upsert */
            set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Upsert not yet implemented");
            return -1;
        }
        return 0;
    }

    /* Extract _id from existing document */
    bson_iter_t id_iter;
    if (!bson_iter_init_find(&id_iter, existing, "_id")) {
        bson_destroy(existing);
        set_error(error, MONGOLITE_LIB, MONGOLITE_ERROR, "Document missing _id");
        return -1;
    }
    bson_oid_t doc_id;
    bson_oid_copy(bson_iter_oid(&id_iter), &doc_id);
    bson_destroy(existing);

    /* Create new document with _id preserved */
    bson_t *new_doc = bson_new();
    BSON_APPEND_OID(new_doc, "_id", &doc_id);

    /* Copy all fields from replacement */
    bson_iter_init(&iter, replacement);
    while (bson_iter_next(&iter)) {
        const char *key = bson_iter_key(&iter);
        if (strcmp(key, "_id") != 0) {  /* Skip _id if present in replacement */
            bson_append_iter(new_doc, key, -1, &iter);
        }
    }

    /* Lock database */
    _mongolite_lock(db);

    /* Get collection tree */
    wtree_tree_t *tree = _mongolite_get_collection_tree(db, collection, error);
    if (!tree) {
        bson_destroy(new_doc);
        _mongolite_unlock(db);
        return -1;
    }

    /* Begin transaction */
    wtree_txn_t *txn = _mongolite_get_write_txn(db, error);
    if (!txn) {
        bson_destroy(new_doc);
        _mongolite_unlock(db);
        return -1;
    }

    /* Replace document */
    int rc = wtree_update_txn(txn, tree,
                              doc_id.bytes, sizeof(doc_id.bytes),
                              bson_get_data(new_doc), new_doc->len,
                              error);
    if (rc != 0) {
        _mongolite_abort_if_auto(db, txn);
        bson_destroy(new_doc);
        _mongolite_unlock(db);
        return -1;
    }

    rc = _mongolite_commit_if_auto(db, txn, error);
    bson_destroy(new_doc);

    if (rc != 0) {
        _mongolite_unlock(db);
        return -1;
    }

    db->changes = 1;
    _mongolite_unlock(db);
    return 0;
}

/* ============================================================
 * JSON Wrappers
 * ============================================================ */

int mongolite_update_one_json(mongolite_db_t *db, const char *collection,
                              const char *filter_json, const char *update_json,
                              bool upsert, gerror_t *error) {
    bson_error_t bson_err;

    bson_t *filter = filter_json ? bson_new_from_json((const uint8_t*)filter_json, -1, &bson_err) : NULL;
    if (filter_json && !filter) {
        set_error(error, "libbson", MONGOLITE_EINVAL, "Invalid filter JSON: %s", bson_err.message);
        return -1;
    }

    bson_t *update = bson_new_from_json((const uint8_t*)update_json, -1, &bson_err);
    if (!update) {
        if (filter) bson_destroy(filter);
        set_error(error, "libbson", MONGOLITE_EINVAL, "Invalid update JSON: %s", bson_err.message);
        return -1;
    }

    int rc = mongolite_update_one(db, collection, filter, update, upsert, error);

    if (filter) bson_destroy(filter);
    bson_destroy(update);
    return rc;
}

int mongolite_update_many_json(mongolite_db_t *db, const char *collection,
                               const char *filter_json, const char *update_json,
                               bool upsert, int64_t *modified_count, gerror_t *error) {
    bson_error_t bson_err;

    bson_t *filter = filter_json ? bson_new_from_json((const uint8_t*)filter_json, -1, &bson_err) : NULL;
    if (filter_json && !filter) {
        set_error(error, "libbson", MONGOLITE_EINVAL, "Invalid filter JSON: %s", bson_err.message);
        return -1;
    }

    bson_t *update = bson_new_from_json((const uint8_t*)update_json, -1, &bson_err);
    if (!update) {
        if (filter) bson_destroy(filter);
        set_error(error, "libbson", MONGOLITE_EINVAL, "Invalid update JSON: %s", bson_err.message);
        return -1;
    }

    int rc = mongolite_update_many(db, collection, filter, update, upsert, modified_count, error);

    if (filter) bson_destroy(filter);
    bson_destroy(update);
    return rc;
}

int mongolite_replace_one_json(mongolite_db_t *db, const char *collection,
                               const char *filter_json, const char *replacement_json,
                               bool upsert, gerror_t *error) {
    bson_error_t bson_err;

    bson_t *filter = filter_json ? bson_new_from_json((const uint8_t*)filter_json, -1, &bson_err) : NULL;
    if (filter_json && !filter) {
        set_error(error, "libbson", MONGOLITE_EINVAL, "Invalid filter JSON: %s", bson_err.message);
        return -1;
    }

    bson_t *replacement = bson_new_from_json((const uint8_t*)replacement_json, -1, &bson_err);
    if (!replacement) {
        if (filter) bson_destroy(filter);
        set_error(error, "libbson", MONGOLITE_EINVAL, "Invalid replacement JSON: %s", bson_err.message);
        return -1;
    }

    int rc = mongolite_replace_one(db, collection, filter, replacement, upsert, error);

    if (filter) bson_destroy(filter);
    bson_destroy(replacement);
    return rc;
}
