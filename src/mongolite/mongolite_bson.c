/*
** MongoLite BSON Implementation
** Wrapper around libbson providing clean MongoLite-specific interface
*/

#include "mongolite_bson.h"
#include <stdlib.h>
#include <string.h>

/* Include libbson headers */
#include <bson/bson.h>

/*
** Internal structures
** Hide libbson implementation details behind our interface
*/
struct mongolite_bson {
    bson_t *bson_doc;     /* libbson document */
    bool owns_data;       /* Whether we own the underlying data */
};

struct mongolite_bson_iter {
    bson_iter_t iter;     /* libbson iterator */
    const mongolite_bson_t *bson;  /* Parent document */
};

/*
** Document Creation and Destruction
*/
mongolite_bson_t* mongolite_bson_new(void) {
    mongolite_bson_t *mb = calloc(1, sizeof(mongolite_bson_t));
    if (!mb) return NULL;
    
    mb->bson_doc = bson_new();
    if (!mb->bson_doc) {
        free(mb);
        return NULL;
    }
    
    mb->owns_data = true;
    return mb;
}

mongolite_bson_t* mongolite_bson_new_from_json(const char *json, mongolite_bson_error_t *error) {
    if (!json) {
        if (error) *error = MONGOLITE_BSON_ERROR_INVALID_ARGUMENT;
        return NULL;
    }
    
    mongolite_bson_t *mb = calloc(1, sizeof(mongolite_bson_t));
    if (!mb) {
        if (error) *error = MONGOLITE_BSON_ERROR_OUT_OF_MEMORY;
        return NULL;
    }
    
    bson_error_t bson_err;
    mb->bson_doc = bson_new_from_json((const uint8_t*)json, -1, &bson_err);
    if (!mb->bson_doc) {
        free(mb);
        if (error) *error = MONGOLITE_BSON_ERROR_INVALID_JSON;
        return NULL;
    }
    
    mb->owns_data = true;
    if (error) *error = MONGOLITE_BSON_OK;
    return mb;
}

mongolite_bson_t* mongolite_bson_new_from_data(const uint8_t *data, size_t length, mongolite_bson_error_t *error) {
    if (!data || length == 0) {
        if (error) *error = MONGOLITE_BSON_ERROR_INVALID_ARGUMENT;
        return NULL;
    }
    
    mongolite_bson_t *mb = calloc(1, sizeof(mongolite_bson_t));
    if (!mb) {
        if (error) *error = MONGOLITE_BSON_ERROR_OUT_OF_MEMORY;
        return NULL;
    }
    
    mb->bson_doc = bson_new_from_data(data, length);
    if (!mb->bson_doc) {
        free(mb);
        if (error) *error = MONGOLITE_BSON_ERROR_INVALID_BSON;
        return NULL;
    }
    
    mb->owns_data = true;
    if (error) *error = MONGOLITE_BSON_OK;
    return mb;
}

mongolite_bson_t* mongolite_bson_copy(const mongolite_bson_t *bson) {
    if (!bson || !bson->bson_doc) return NULL;
    
    mongolite_bson_t *mb = calloc(1, sizeof(mongolite_bson_t));
    if (!mb) return NULL;
    
    mb->bson_doc = bson_copy(bson->bson_doc);
    if (!mb->bson_doc) {
        free(mb);
        return NULL;
    }
    
    mb->owns_data = true;
    return mb;
}

void mongolite_bson_destroy(mongolite_bson_t *bson) {
    if (!bson) return;
    
    if (bson->bson_doc && bson->owns_data) {
        bson_destroy(bson->bson_doc);
    }
    
    free(bson);
}

/*
** Document Properties
*/
bool mongolite_bson_empty(const mongolite_bson_t *bson) {
    if (!bson || !bson->bson_doc) return true;
    return bson_empty(bson->bson_doc);
}

uint32_t mongolite_bson_count_keys(const mongolite_bson_t *bson) {
    if (!bson || !bson->bson_doc) return 0;
    return bson_count_keys(bson->bson_doc);
}

const uint8_t* mongolite_bson_get_data(const mongolite_bson_t *bson, uint32_t *length) {
    if (!bson || !bson->bson_doc) {
        if (length) *length = 0;
        return NULL;
    }
    
    return bson_get_data(bson->bson_doc, length);
}

/*
** JSON Conversion
*/
char* mongolite_bson_as_canonical_extended_json(const mongolite_bson_t *bson, size_t *length) {
    if (!bson || !bson->bson_doc) {
        if (length) *length = 0;
        return NULL;
    }
    
    return bson_as_canonical_extended_json(bson->bson_doc, length);
}

char* mongolite_bson_as_relaxed_extended_json(const mongolite_bson_t *bson, size_t *length) {
    if (!bson || !bson->bson_doc) {
        if (length) *length = 0;
        return NULL;
    }
    
    return bson_as_relaxed_extended_json(bson->bson_doc, length);
}

/*
** Document Building (Append Operations)
*/
bool mongolite_bson_append_utf8(mongolite_bson_t *bson, const char *key, const char *value) {
    if (!bson || !bson->bson_doc || !key || !value) return false;
    return bson_append_utf8(bson->bson_doc, key, -1, value, -1);
}

bool mongolite_bson_append_int32(mongolite_bson_t *bson, const char *key, int32_t value) {
    if (!bson || !bson->bson_doc || !key) return false;
    return bson_append_int32(bson->bson_doc, key, -1, value);
}

bool mongolite_bson_append_int64(mongolite_bson_t *bson, const char *key, int64_t value) {
    if (!bson || !bson->bson_doc || !key) return false;
    return bson_append_int64(bson->bson_doc, key, -1, value);
}

bool mongolite_bson_append_double(mongolite_bson_t *bson, const char *key, double value) {
    if (!bson || !bson->bson_doc || !key) return false;
    return bson_append_double(bson->bson_doc, key, -1, value);
}

bool mongolite_bson_append_bool(mongolite_bson_t *bson, const char *key, bool value) {
    if (!bson || !bson->bson_doc || !key) return false;
    return bson_append_bool(bson->bson_doc, key, -1, value);
}

bool mongolite_bson_append_null(mongolite_bson_t *bson, const char *key) {
    if (!bson || !bson->bson_doc || !key) return false;
    return bson_append_null(bson->bson_doc, key, -1);
}

/*
** Document Iteration
*/
mongolite_bson_iter_t* mongolite_bson_iter_new(const mongolite_bson_t *bson) {
    if (!bson || !bson->bson_doc) return NULL;
    
    mongolite_bson_iter_t *iter = calloc(1, sizeof(mongolite_bson_iter_t));
    if (!iter) return NULL;
    
    if (!bson_iter_init(&iter->iter, bson->bson_doc)) {
        free(iter);
        return NULL;
    }
    
    iter->bson = bson;
    return iter;
}

void mongolite_bson_iter_destroy(mongolite_bson_iter_t *iter) {
    if (iter) {
        free(iter);
    }
}

bool mongolite_bson_iter_next(mongolite_bson_iter_t *iter) {
    if (!iter) return false;
    return bson_iter_next(&iter->iter);
}

const char* mongolite_bson_iter_key(const mongolite_bson_iter_t *iter) {
    if (!iter) return NULL;
    return bson_iter_key(&iter->iter);
}

mongolite_bson_type_t mongolite_bson_iter_type(const mongolite_bson_iter_t *iter) {
    if (!iter) return MONGOLITE_BSON_TYPE_EOD;
    return (mongolite_bson_type_t)bson_iter_type(&iter->iter);
}

/*
** Document Querying
*/
bool mongolite_bson_has_field(const mongolite_bson_t *bson, const char *key) {
    if (!bson || !bson->bson_doc || !key) return false;
    return bson_has_field(bson->bson_doc, key);
}

bool mongolite_bson_get_utf8(const mongolite_bson_t *bson, const char *key, const char **value) {
    if (!bson || !bson->bson_doc || !key || !value) return false;
    
    bson_iter_t iter;
    if (!bson_iter_init_find(&iter, bson->bson_doc, key)) return false;
    if (!BSON_ITER_HOLDS_UTF8(&iter)) return false;
    
    *value = bson_iter_utf8(&iter, NULL);
    return true;
}

bool mongolite_bson_get_int32(const mongolite_bson_t *bson, const char *key, int32_t *value) {
    if (!bson || !bson->bson_doc || !key || !value) return false;
    
    bson_iter_t iter;
    if (!bson_iter_init_find(&iter, bson->bson_doc, key)) return false;
    if (!BSON_ITER_HOLDS_INT32(&iter)) return false;
    
    *value = bson_iter_int32(&iter);
    return true;
}

bool mongolite_bson_get_int64(const mongolite_bson_t *bson, const char *key, int64_t *value) {
    if (!bson || !bson->bson_doc || !key || !value) return false;
    
    bson_iter_t iter;
    if (!bson_iter_init_find(&iter, bson->bson_doc, key)) return false;
    if (!BSON_ITER_HOLDS_INT64(&iter)) return false;
    
    *value = bson_iter_int64(&iter);
    return true;
}

bool mongolite_bson_get_double(const mongolite_bson_t *bson, const char *key, double *value) {
    if (!bson || !bson->bson_doc || !key || !value) return false;
    
    bson_iter_t iter;
    if (!bson_iter_init_find(&iter, bson->bson_doc, key)) return false;
    if (!BSON_ITER_HOLDS_DOUBLE(&iter)) return false;
    
    *value = bson_iter_double(&iter);
    return true;
}

bool mongolite_bson_get_bool(const mongolite_bson_t *bson, const char *key, bool *value) {
    if (!bson || !bson->bson_doc || !key || !value) return false;
    
    bson_iter_t iter;
    if (!bson_iter_init_find(&iter, bson->bson_doc, key)) return false;
    if (!BSON_ITER_HOLDS_BOOL(&iter)) return false;
    
    *value = bson_iter_bool(&iter);
    return true;
}

/*
** Validation
*/
bool mongolite_bson_validate(const uint8_t *data, size_t length, mongolite_bson_error_t *error) {
    if (!data || length == 0) {
        if (error) *error = MONGOLITE_BSON_ERROR_INVALID_ARGUMENT;
        return false;
    }
    
    bson_validate_flags_t flags = BSON_VALIDATE_UTF8 | BSON_VALIDATE_DOLLAR_KEYS | BSON_VALIDATE_DOT_KEYS;
    size_t max_len = 0;
    bson_error_t bson_err;
    
    bool result = bson_validate_with_error_and_size(data, length, flags, &max_len, &bson_err);
    
    if (!result && error) {
        *error = MONGOLITE_BSON_ERROR_INVALID_BSON;
    } else if (error) {
        *error = MONGOLITE_BSON_OK;
    }
    
    return result;
}

/*
** Library Information
*/
const char* mongolite_bson_get_version(void) {
    return "MongoLite BSON 1.0 (libbson backend)";
}

bool mongolite_bson_check_version(int major, int minor, int micro) {
    /* For now, just return true for version 1.x.x */
    return major == 1;
}