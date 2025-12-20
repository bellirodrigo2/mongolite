/**
 * bson_update.c - BSON Update Operators Implementation
 *
 * Pure functions for applying MongoDB-style update operators.
 * Extracted from mongolite_update.c for reuse and testing.
 */

#include "bson_update.h"
#include "key_compare.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#define BSON_UPDATE_LIB "bson_update"

/* ============================================================
 * Helper: Copy document excluding specified field
 * ============================================================ */

MONGOLITE_ALWAYS_INLINE
static bool _copy_except_field(bson_t *dest, const bson_t *src, const char *exclude_field) {
    bson_iter_t iter;
    if (MONGOLITE_UNLIKELY(!bson_iter_init(&iter, src))) {
        return false;
    }

    while (bson_iter_next(&iter)) {
        const char *key = bson_iter_key(&iter);
        if (strcmp(key, exclude_field) != 0) {
            if (MONGOLITE_UNLIKELY(!bson_append_iter(dest, key, -1, &iter))) {
                return false;
            }
        }
    }
    return true;
}

/* ============================================================
 * $set operator
 * ============================================================ */

MONGOLITE_HOT MONGOLITE_WARN_UNUSED
bson_t* bson_update_apply_set(const bson_t *doc, bson_iter_t *set_iter, gerror_t *error) {
    bson_iter_t field_iter;

    if (MONGOLITE_UNLIKELY(!bson_iter_recurse(set_iter, &field_iter))) {
        set_error(error, BSON_UPDATE_LIB, -1, "$set requires a document");
        return NULL;
    }

    /* Start with a copy of the input document */
    bson_t *result = bson_copy(doc);
    if (MONGOLITE_UNLIKELY(!result)) {
        set_error(error, "system", -1, "Failed to copy document");
        return NULL;
    }

    while (bson_iter_next(&field_iter)) {
        const char *field_name = bson_iter_key(&field_iter);

        /* Build new document: copy all fields except this one, then add new value */
        bson_t *tmp = bson_new();
        if (MONGOLITE_UNLIKELY(!tmp)) {
            bson_destroy(result);
            set_error(error, "system", -1, "Out of memory");
            return NULL;
        }

        /* Copy all fields except the one we're setting */
        if (MONGOLITE_UNLIKELY(!_copy_except_field(tmp, result, field_name))) {
            bson_destroy(tmp);
            bson_destroy(result);
            set_error(error, BSON_UPDATE_LIB, -1, "Failed to copy fields");
            return NULL;
        }

        /* Add new field value */
        if (MONGOLITE_UNLIKELY(!bson_append_iter(tmp, field_name, -1, &field_iter))) {
            bson_destroy(tmp);
            bson_destroy(result);
            set_error(error, BSON_UPDATE_LIB, -1, "Failed to append field: %s", field_name);
            return NULL;
        }

        /* Replace result with tmp */
        bson_destroy(result);
        result = tmp;
    }

    return result;
}

/* ============================================================
 * $unset operator
 * ============================================================ */

MONGOLITE_WARN_UNUSED
bson_t* bson_update_apply_unset(const bson_t *doc, bson_iter_t *unset_iter, gerror_t *error) {
    bson_iter_t field_iter;

    if (MONGOLITE_UNLIKELY(!bson_iter_recurse(unset_iter, &field_iter))) {
        set_error(error, BSON_UPDATE_LIB, -1, "$unset requires a document");
        return NULL;
    }

    /* Collect field names to remove */
    char **fields = NULL;
    size_t n_fields = 0;

    while (bson_iter_next(&field_iter)) {
        const char *field_name = bson_iter_key(&field_iter);
        char **new_fields = realloc(fields, sizeof(char*) * (n_fields + 1));
        if (MONGOLITE_UNLIKELY(!new_fields)) {
            free(fields);
            set_error(error, "system", -1, "Out of memory");
            return NULL;
        }
        fields = new_fields;
        fields[n_fields++] = (char*)field_name;
    }

    /* Create new document without those fields */
    bson_t *result = bson_new();
    if (MONGOLITE_UNLIKELY(!result)) {
        free(fields);
        set_error(error, "system", -1, "Out of memory");
        return NULL;
    }

    /* Copy all fields except the ones in the unset list */
    bson_iter_t doc_iter;
    if (MONGOLITE_LIKELY(bson_iter_init(&doc_iter, doc))) {
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
    }

    free(fields);
    return result;
}

/* ============================================================
 * $inc operator
 * ============================================================ */

MONGOLITE_HOT MONGOLITE_WARN_UNUSED
bson_t* bson_update_apply_inc(const bson_t *doc, bson_iter_t *inc_iter, gerror_t *error) {
    bson_iter_t field_iter;

    if (MONGOLITE_UNLIKELY(!bson_iter_recurse(inc_iter, &field_iter))) {
        set_error(error, BSON_UPDATE_LIB, -1, "$inc requires a document");
        return NULL;
    }

    /* Start with a copy of the input document */
    bson_t *result = bson_copy(doc);
    if (MONGOLITE_UNLIKELY(!result)) {
        set_error(error, "system", -1, "Failed to copy document");
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
            set_error(error, BSON_UPDATE_LIB, -1, "$inc value must be numeric");
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
                set_error(error, BSON_UPDATE_LIB, -1, "$inc field must be numeric");
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
        if (MONGOLITE_UNLIKELY(!tmp)) {
            bson_destroy(result);
            set_error(error, "system", -1, "Out of memory");
            return NULL;
        }

        /* Copy all fields except the one we're incrementing */
        if (MONGOLITE_UNLIKELY(!_copy_except_field(tmp, result, field_name))) {
            bson_destroy(tmp);
            bson_destroy(result);
            set_error(error, BSON_UPDATE_LIB, -1, "Failed to copy fields");
            return NULL;
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
 * $push operator
 * ============================================================ */

MONGOLITE_WARN_UNUSED
bson_t* bson_update_apply_push(const bson_t *doc, bson_iter_t *push_iter, gerror_t *error) {
    bson_iter_t field_iter;

    if (MONGOLITE_UNLIKELY(!bson_iter_recurse(push_iter, &field_iter))) {
        set_error(error, BSON_UPDATE_LIB, -1, "$push requires a document");
        return NULL;
    }

    /* Start with a copy of the input document */
    bson_t *result = bson_copy(doc);
    if (MONGOLITE_UNLIKELY(!result)) {
        set_error(error, "system", -1, "Failed to copy document");
        return NULL;
    }

    while (bson_iter_next(&field_iter)) {
        const char *field_name = bson_iter_key(&field_iter);

        /* Check if field exists and is array */
        bson_iter_t doc_field;
        bson_t new_array;
        bson_init(&new_array);

        if (bson_iter_init_find(&doc_field, result, field_name)) {
            if (MONGOLITE_UNLIKELY(!BSON_ITER_HOLDS_ARRAY(&doc_field))) {
                bson_destroy(&new_array);
                bson_destroy(result);
                set_error(error, BSON_UPDATE_LIB, -1, "$push field must be array");
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
        if (MONGOLITE_UNLIKELY(!tmp)) {
            bson_destroy(&new_array);
            bson_destroy(result);
            set_error(error, "system", -1, "Out of memory");
            return NULL;
        }

        /* Copy all fields except the one we're pushing to */
        if (MONGOLITE_UNLIKELY(!_copy_except_field(tmp, result, field_name))) {
            bson_destroy(&new_array);
            bson_destroy(tmp);
            bson_destroy(result);
            set_error(error, BSON_UPDATE_LIB, -1, "Failed to copy fields");
            return NULL;
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
 * $pull operator
 * ============================================================ */

MONGOLITE_WARN_UNUSED
bson_t* bson_update_apply_pull(const bson_t *doc, bson_iter_t *pull_iter, gerror_t *error) {
    bson_iter_t field_iter;

    if (MONGOLITE_UNLIKELY(!bson_iter_recurse(pull_iter, &field_iter))) {
        set_error(error, BSON_UPDATE_LIB, -1, "$pull requires a document");
        return NULL;
    }

    /* Start with a copy of the input document */
    bson_t *result = bson_copy(doc);
    if (MONGOLITE_UNLIKELY(!result)) {
        set_error(error, "system", -1, "Failed to copy document");
        return NULL;
    }

    while (bson_iter_next(&field_iter)) {
        const char *field_name = bson_iter_key(&field_iter);

        /* Get the value to pull */
        bson_iter_t pull_value_iter = field_iter;

        /* Check if field exists and is array */
        bson_iter_t doc_field;
        if (bson_iter_init_find(&doc_field, result, field_name)) {
            if (MONGOLITE_UNLIKELY(!BSON_ITER_HOLDS_ARRAY(&doc_field))) {
                bson_destroy(result);
                set_error(error, BSON_UPDATE_LIB, -1, "$pull field must be array");
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
            if (MONGOLITE_UNLIKELY(!tmp)) {
                bson_destroy(&new_array);
                bson_destroy(result);
                set_error(error, "system", -1, "Out of memory");
                return NULL;
            }

            /* Copy all fields except the one we're pulling from */
            if (MONGOLITE_UNLIKELY(!_copy_except_field(tmp, result, field_name))) {
                bson_destroy(&new_array);
                bson_destroy(tmp);
                bson_destroy(result);
                set_error(error, BSON_UPDATE_LIB, -1, "Failed to copy fields");
                return NULL;
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
 * $rename operator
 * ============================================================ */

MONGOLITE_WARN_UNUSED
bson_t* bson_update_apply_rename(const bson_t *doc, bson_iter_t *rename_iter, gerror_t *error) {
    bson_iter_t field_iter;

    if (MONGOLITE_UNLIKELY(!bson_iter_recurse(rename_iter, &field_iter))) {
        set_error(error, BSON_UPDATE_LIB, -1, "$rename requires a document");
        return NULL;
    }

    /* Start with a copy of the input document */
    bson_t *result = bson_copy(doc);
    if (MONGOLITE_UNLIKELY(!result)) {
        set_error(error, "system", -1, "Failed to copy document");
        return NULL;
    }

    while (bson_iter_next(&field_iter)) {
        const char *old_name = bson_iter_key(&field_iter);

        if (MONGOLITE_UNLIKELY(!BSON_ITER_HOLDS_UTF8(&field_iter))) {
            bson_destroy(result);
            set_error(error, BSON_UPDATE_LIB, -1, "$rename new name must be string");
            return NULL;
        }

        const char *new_name = bson_iter_utf8(&field_iter, NULL);

        /* Find old field */
        bson_iter_t doc_field;
        if (bson_iter_init_find(&doc_field, result, old_name)) {
            /* Build new document: copy all fields except old, add with new name */
            bson_t *tmp = bson_new();
            if (MONGOLITE_UNLIKELY(!tmp)) {
                bson_destroy(result);
                set_error(error, "system", -1, "Out of memory");
                return NULL;
            }

            /* Copy all fields except the one being renamed */
            if (MONGOLITE_UNLIKELY(!_copy_except_field(tmp, result, old_name))) {
                bson_destroy(tmp);
                bson_destroy(result);
                set_error(error, BSON_UPDATE_LIB, -1, "Failed to copy fields");
                return NULL;
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
 * High-level update function
 * ============================================================ */

MONGOLITE_HOT MONGOLITE_WARN_UNUSED
bson_t* bson_update_apply(const bson_t *original, const bson_t *update, gerror_t *error) {
    if (MONGOLITE_UNLIKELY(!original || !update)) {
        set_error(error, BSON_UPDATE_LIB, -1, "NULL document");
        return NULL;
    }

    bson_t *doc = bson_copy(original);
    if (MONGOLITE_UNLIKELY(!doc)) {
        set_error(error, "system", -1, "Failed to copy document");
        return NULL;
    }

    bson_iter_t iter;
    if (MONGOLITE_UNLIKELY(!bson_iter_init(&iter, update))) {
        bson_destroy(doc);
        set_error(error, BSON_UPDATE_LIB, -1, "Invalid update document");
        return NULL;
    }

    while (bson_iter_next(&iter)) {
        const char *op = bson_iter_key(&iter);
        bson_t *new_doc = NULL;

        if (strcmp(op, "$set") == 0) {
            new_doc = bson_update_apply_set(doc, &iter, error);
        } else if (strcmp(op, "$unset") == 0) {
            new_doc = bson_update_apply_unset(doc, &iter, error);
        } else if (strcmp(op, "$inc") == 0) {
            new_doc = bson_update_apply_inc(doc, &iter, error);
        } else if (strcmp(op, "$push") == 0) {
            new_doc = bson_update_apply_push(doc, &iter, error);
        } else if (strcmp(op, "$pull") == 0) {
            new_doc = bson_update_apply_pull(doc, &iter, error);
        } else if (strcmp(op, "$rename") == 0) {
            new_doc = bson_update_apply_rename(doc, &iter, error);
        } else {
            bson_destroy(doc);
            set_error(error, BSON_UPDATE_LIB, -1, "Unknown update operator: %s", op);
            return NULL;
        }

        if (MONGOLITE_UNLIKELY(!new_doc)) {
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
 * Utility functions
 * ============================================================ */

MONGOLITE_PURE
bool bson_update_is_update_spec(const bson_t *update) {
    if (!update || bson_empty(update)) {
        return false;
    }

    bson_iter_t iter;
    if (!bson_iter_init(&iter, update)) {
        return false;
    }

    while (bson_iter_next(&iter)) {
        const char *key = bson_iter_key(&iter);
        if (key[0] != '$') {
            return false;  /* Not an operator key */
        }
    }

    return true;
}
