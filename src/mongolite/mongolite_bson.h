/*
** MongoLite BSON Interface - Header-Only Implementation
** Zero-cost inline wrappers around libbson
** 
** Provides clean MongoLite API with direct libbson performance
*/
#ifndef MONGOLITE_BSON_H
#define MONGOLITE_BSON_H

#include <bson/bson.h>
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
** Type aliases for clean MongoLite interface
** Direct mappings to libbson types for zero overhead
*/
typedef bson_t mongolite_bson_t;
typedef bson_iter_t mongolite_bson_iter_t;
typedef bson_error_t mongolite_bson_error_t;
typedef bson_type_t mongolite_bson_type_t;
typedef bson_value_t mongolite_bson_value_t;

/*
** MongoLite error codes (mapped to bson errors)
*/
#define MONGOLITE_BSON_OK BSON_ERROR_NONE
#define MONGOLITE_BSON_ERROR_INVALID_JSON BSON_ERROR_JSON_INVALID_ELEMENT
#define MONGOLITE_BSON_ERROR_INVALID_BSON BSON_ERROR_INVALID
#define MONGOLITE_BSON_ERROR_OUT_OF_MEMORY BSON_ERROR_OTHER

/*
** BSON types (direct mapping)
*/
#define MONGOLITE_BSON_TYPE_EOD        BSON_TYPE_EOD
#define MONGOLITE_BSON_TYPE_DOUBLE     BSON_TYPE_DOUBLE
#define MONGOLITE_BSON_TYPE_UTF8       BSON_TYPE_UTF8
#define MONGOLITE_BSON_TYPE_DOCUMENT   BSON_TYPE_DOCUMENT
#define MONGOLITE_BSON_TYPE_ARRAY      BSON_TYPE_ARRAY
#define MONGOLITE_BSON_TYPE_BINARY     BSON_TYPE_BINARY
#define MONGOLITE_BSON_TYPE_OID        BSON_TYPE_OID
#define MONGOLITE_BSON_TYPE_BOOL       BSON_TYPE_BOOL
#define MONGOLITE_BSON_TYPE_DATE_TIME  BSON_TYPE_DATE_TIME
#define MONGOLITE_BSON_TYPE_NULL       BSON_TYPE_NULL
#define MONGOLITE_BSON_TYPE_INT32      BSON_TYPE_INT32
#define MONGOLITE_BSON_TYPE_INT64      BSON_TYPE_INT64

/*
** Document Creation and Destruction
*/
static inline mongolite_bson_t* mongolite_bson_new(void) {
    return bson_new();
}

static inline mongolite_bson_t* mongolite_bson_new_from_json(const char *json, mongolite_bson_error_t *error) {
    if (!json) {
        if (error) *error = MONGOLITE_BSON_ERROR_INVALID_JSON;
        return NULL;
    }
    mongolite_bson_t *doc = bson_new_from_json((const uint8_t*)json, -1, error);
    return doc;
}

static inline mongolite_bson_t* mongolite_bson_new_from_data(const uint8_t *data, size_t length) {
    if (!data || length == 0) return NULL;
    return bson_new_from_data(data, length);
}

static inline mongolite_bson_t* mongolite_bson_copy(const mongolite_bson_t *bson) {
    return bson_copy(bson);
}

static inline void mongolite_bson_destroy(mongolite_bson_t *bson) {
    if (bson) bson_destroy(bson);
}

/*
** Document Properties
*/
static inline bool mongolite_bson_empty(const mongolite_bson_t *bson) {
    return bson ? bson_empty(bson) : true;
}

static inline uint32_t mongolite_bson_count_keys(const mongolite_bson_t *bson) {
    return bson ? bson_count_keys(bson) : 0;
}

static inline const uint8_t* mongolite_bson_get_data(const mongolite_bson_t *bson, uint32_t *length) {
    return bson ? bson_get_data(bson, length) : NULL;
}

/*
** JSON Conversion
*/
static inline char* mongolite_bson_as_canonical_extended_json(const mongolite_bson_t *bson, size_t *length) {
    return bson ? bson_as_canonical_extended_json(bson, length) : NULL;
}

static inline char* mongolite_bson_as_relaxed_extended_json(const mongolite_bson_t *bson, size_t *length) {
    return bson ? bson_as_relaxed_extended_json(bson, length) : NULL;
}

static inline char* mongolite_bson_as_json(const mongolite_bson_t *bson, size_t *length) {
    return bson ? bson_as_relaxed_extended_json(bson, length) : NULL;
}

/*
** Document Building (Append Operations)
*/
static inline bool mongolite_bson_append_utf8(mongolite_bson_t *bson, const char *key, const char *value) {
    return bson && key && value ? bson_append_utf8(bson, key, -1, value, -1) : false;
}

static inline bool mongolite_bson_append_int32(mongolite_bson_t *bson, const char *key, int32_t value) {
    return bson && key ? bson_append_int32(bson, key, -1, value) : false;
}

static inline bool mongolite_bson_append_int64(mongolite_bson_t *bson, const char *key, int64_t value) {
    return bson && key ? bson_append_int64(bson, key, -1, value) : false;
}

static inline bool mongolite_bson_append_double(mongolite_bson_t *bson, const char *key, double value) {
    return bson && key ? bson_append_double(bson, key, -1, value) : false;
}

static inline bool mongolite_bson_append_bool(mongolite_bson_t *bson, const char *key, bool value) {
    return bson && key ? bson_append_bool(bson, key, -1, value) : false;
}

static inline bool mongolite_bson_append_null(mongolite_bson_t *bson, const char *key) {
    return bson && key ? bson_append_null(bson, key, -1) : false;
}

static inline bool mongolite_bson_append_document(mongolite_bson_t *bson, const char *key, const mongolite_bson_t *value) {
    return bson && key && value ? bson_append_document(bson, key, -1, value) : false;
}

static inline bool mongolite_bson_append_array(mongolite_bson_t *bson, const char *key, const mongolite_bson_t *array) {
    return bson && key && array ? bson_append_array(bson, key, -1, array) : false;
}

static inline bool mongolite_bson_append_binary(mongolite_bson_t *bson, const char *key, const uint8_t *binary, uint32_t length) {
    return bson && key && binary ? bson_append_binary(bson, key, -1, BSON_SUBTYPE_BINARY, binary, length) : false;
}

static inline bool mongolite_bson_append_datetime(mongolite_bson_t *bson, const char *key, int64_t msec_since_epoch) {
    return bson && key ? bson_append_date_time(bson, key, -1, msec_since_epoch) : false;
}

/*
** Document Iteration
*/
static inline bool mongolite_bson_iter_init(mongolite_bson_iter_t *iter, const mongolite_bson_t *bson) {
    return iter && bson ? bson_iter_init(iter, bson) : false;
}

static inline bool mongolite_bson_iter_next(mongolite_bson_iter_t *iter) {
    return iter ? bson_iter_next(iter) : false;
}

static inline const char* mongolite_bson_iter_key(const mongolite_bson_iter_t *iter) {
    return iter ? bson_iter_key(iter) : NULL;
}

static inline mongolite_bson_type_t mongolite_bson_iter_type(const mongolite_bson_iter_t *iter) {
    return iter ? bson_iter_type(iter) : BSON_TYPE_EOD;
}

static inline const mongolite_bson_value_t* mongolite_bson_iter_value(const mongolite_bson_iter_t *iter) {
    return iter ? bson_iter_value(iter) : NULL;
}

static inline const char* mongolite_bson_iter_utf8(const mongolite_bson_iter_t *iter, uint32_t *length) {
    return iter ? bson_iter_utf8(iter, length) : NULL;
}

static inline int32_t mongolite_bson_iter_int32(const mongolite_bson_iter_t *iter) {
    return iter ? bson_iter_int32(iter) : 0;
}

static inline int64_t mongolite_bson_iter_int64(const mongolite_bson_iter_t *iter) {
    return iter ? bson_iter_int64(iter) : 0;
}

static inline double mongolite_bson_iter_double(const mongolite_bson_iter_t *iter) {
    return iter ? bson_iter_double(iter) : 0.0;
}

static inline bool mongolite_bson_iter_bool(const mongolite_bson_iter_t *iter) {
    return iter ? bson_iter_bool(iter) : false;
}

/*
** Document Querying (MongoDB-style field access)
*/
static inline bool mongolite_bson_has_field(const mongolite_bson_t *bson, const char *key) {
    return bson && key ? bson_has_field(bson, key) : false;
}

static inline bool mongolite_bson_iter_init_find(mongolite_bson_iter_t *iter, const mongolite_bson_t *bson, const char *key) {
    return iter && bson && key ? bson_iter_init_find(iter, bson, key) : false;
}

static inline bool mongolite_bson_get_utf8(const mongolite_bson_t *bson, const char *key, const char **value) {
    if (!bson || !key || !value) return false;
    mongolite_bson_iter_t iter;
    if (!mongolite_bson_iter_init_find(&iter, bson, key)) return false;
    if (mongolite_bson_iter_type(&iter) != BSON_TYPE_UTF8) return false;
    *value = mongolite_bson_iter_utf8(&iter, NULL);
    return true;
}

static inline bool mongolite_bson_get_int32(const mongolite_bson_t *bson, const char *key, int32_t *value) {
    if (!bson || !key || !value) return false;
    mongolite_bson_iter_t iter;
    if (!mongolite_bson_iter_init_find(&iter, bson, key)) return false;
    if (mongolite_bson_iter_type(&iter) != BSON_TYPE_INT32) return false;
    *value = mongolite_bson_iter_int32(&iter);
    return true;
}

static inline bool mongolite_bson_get_int64(const mongolite_bson_t *bson, const char *key, int64_t *value) {
    if (!bson || !key || !value) return false;
    mongolite_bson_iter_t iter;
    if (!mongolite_bson_iter_init_find(&iter, bson, key)) return false;
    if (mongolite_bson_iter_type(&iter) != BSON_TYPE_INT64) return false;
    *value = mongolite_bson_iter_int64(&iter);
    return true;
}

static inline bool mongolite_bson_get_double(const mongolite_bson_t *bson, const char *key, double *value) {
    if (!bson || !key || !value) return false;
    mongolite_bson_iter_t iter;
    if (!mongolite_bson_iter_init_find(&iter, bson, key)) return false;
    if (mongolite_bson_iter_type(&iter) != BSON_TYPE_DOUBLE) return false;
    *value = mongolite_bson_iter_double(&iter);
    return true;
}

static inline bool mongolite_bson_get_bool(const mongolite_bson_t *bson, const char *key, bool *value) {
    if (!bson || !key || !value) return false;
    mongolite_bson_iter_t iter;
    if (!mongolite_bson_iter_init_find(&iter, bson, key)) return false;
    if (mongolite_bson_iter_type(&iter) != BSON_TYPE_BOOL) return false;
    *value = mongolite_bson_iter_bool(&iter);
    return true;
}

/*
** Validation and Utility
*/
static inline bool mongolite_bson_validate(const uint8_t *data, size_t length) {
    return data && length > 0 ? bson_validate(data, length, BSON_VALIDATE_UTF8) : false;
}

static inline bool mongolite_bson_equal(const mongolite_bson_t *bson1, const mongolite_bson_t *bson2) {
    return bson1 && bson2 ? bson_equal(bson1, bson2) : false;
}

/*
** Array Operations
*/
static inline mongolite_bson_t* mongolite_bson_array_new(void) {
    return bson_new();
}

static inline bool mongolite_bson_array_append_utf8(mongolite_bson_t *array, const char *value) {
    if (!array || !value) return false;
    char key[16];
    uint32_t index = bson_count_keys(array);
    bson_uint32_to_string(index, &key, key, sizeof(key));
    return bson_append_utf8(array, key, -1, value, -1);
}

static inline bool mongolite_bson_array_append_int32(mongolite_bson_t *array, int32_t value) {
    if (!array) return false;
    char key[16];
    uint32_t index = bson_count_keys(array);
    bson_uint32_to_string(index, &key, key, sizeof(key));
    return bson_append_int32(array, key, -1, value);
}

static inline bool mongolite_bson_array_append_document(mongolite_bson_t *array, const mongolite_bson_t *doc) {
    if (!array || !doc) return false;
    char key[16];
    uint32_t index = bson_count_keys(array);
    bson_uint32_to_string(index, &key, key, sizeof(key));
    return bson_append_document(array, key, -1, doc);
}

static inline uint32_t mongolite_bson_array_get_length(const mongolite_bson_t *array) {
    return array ? bson_count_keys(array) : 0;
}

/*
** Library Information
*/
static inline const char* mongolite_bson_get_version(void) {
    return "MongoLite BSON 1.0 (libbson " BSON_VERSION_S " backend)";
}

#ifdef __cplusplus
}
#endif

#endif /* MONGOLITE_BSON_H */