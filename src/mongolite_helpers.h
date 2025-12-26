/**
 * mongolite_helpers.h - Common helper macros and inline functions
 *
 * This file consolidates frequently-used patterns across mongolite:
 * - Parameter validation
 * - OID extraction from documents
 * - JSON parsing with error handling
 * - Lock + tree get patterns
 * - Cleanup helpers
 *
 * These helpers reduce code duplication and improve maintainability.
 */

#ifndef MONGOLITE_HELPERS_H
#define MONGOLITE_HELPERS_H

#include "macros.h"
#include "gerror.h"
#include <bson/bson.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

/* Forward declarations to avoid circular includes */
struct mongolite_db;
typedef struct mongolite_db mongolite_db_t;

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Parameter Validation Helpers
 *
 * These macros provide consistent validation across all functions.
 * They set appropriate error messages and return early on failure.
 * ============================================================ */

/**
 * VALIDATE_DB_COLLECTION - Validate db and collection parameters
 *
 * Usage:
 *   VALIDATE_DB_COLLECTION(db, collection, error, -1);
 *
 * Expands to: if (!db || !collection) { set_error(...); return ret; }
 */
#define VALIDATE_DB_COLLECTION(db, collection, error, ret_val) \
    do { \
        if (MONGOLITE_UNLIKELY(!(db) || !(collection))) { \
            set_error((error), "mongolite", -1003, "Database and collection are required"); \
            return (ret_val); \
        } \
    } while (0)

/**
 * VALIDATE_DB_COLLECTION_DOC - Validate db, collection, and document parameters
 */
#define VALIDATE_DB_COLLECTION_DOC(db, collection, doc, error, ret_val) \
    do { \
        if (MONGOLITE_UNLIKELY(!(db) || !(collection) || !(doc))) { \
            set_error((error), "mongolite", -1003, "Database, collection, and document are required"); \
            return (ret_val); \
        } \
    } while (0)

/**
 * VALIDATE_DB_COLLECTION_FILTER - Validate db, collection, and filter parameters
 * Note: filter can be NULL (means match all), so we don't validate it
 */
#define VALIDATE_DB_COLLECTION_FILTER(db, collection, error, ret_val) \
    VALIDATE_DB_COLLECTION(db, collection, error, ret_val)

/**
 * VALIDATE_DB_COLLECTION_UPDATE - Validate db, collection, and update parameters
 */
#define VALIDATE_DB_COLLECTION_UPDATE(db, collection, update, error, ret_val) \
    do { \
        if (MONGOLITE_UNLIKELY(!(db) || !(collection) || !(update))) { \
            set_error((error), "mongolite", -1003, "Database, collection, and update are required"); \
            return (ret_val); \
        } \
    } while (0)

/**
 * VALIDATE_PARAMS - Generic validation macro
 *
 * Usage:
 *   VALIDATE_PARAMS(db && collection && doc, error, "Custom message", -1);
 */
#define VALIDATE_PARAMS(condition, error, message, ret_val) \
    do { \
        if (MONGOLITE_UNLIKELY(!(condition))) { \
            set_error((error), "mongolite", -1003, (message)); \
            return (ret_val); \
        } \
    } while (0)

/* ============================================================
 * OID Extraction Helpers
 *
 * Extract _id OID from BSON documents with consistent error handling.
 * ============================================================ */

/**
 * Extract OID from a document's _id field
 *
 * @param doc       The BSON document
 * @param out_oid   Output: the extracted OID
 * @return          true if successful, false if _id is missing or not an OID
 */
MONGOLITE_ALWAYS_INLINE
static inline bool extract_doc_oid(const bson_t *doc, bson_oid_t *out_oid) {
    bson_iter_t iter;
    if (MONGOLITE_UNLIKELY(!bson_iter_init_find(&iter, doc, "_id"))) {
        return false;
    }
    if (MONGOLITE_UNLIKELY(!BSON_ITER_HOLDS_OID(&iter))) {
        return false;
    }
    bson_oid_copy(bson_iter_oid(&iter), out_oid);
    return true;
}

/**
 * Extract OID from document, setting error on failure
 *
 * @param doc       The BSON document
 * @param out_oid   Output: the extracted OID
 * @param error     Error struct to populate on failure
 * @return          true if successful, false on error
 */
MONGOLITE_ALWAYS_INLINE
static inline bool extract_doc_oid_with_error(const bson_t *doc, bson_oid_t *out_oid,
                                               gerror_t *error) {
    if (MONGOLITE_UNLIKELY(!extract_doc_oid(doc, out_oid))) {
        if (error) {
            error->code = -1000;  /* MONGOLITE_ERROR */
            snprintf(error->message, sizeof(error->message), "Document missing or has invalid _id");
            strncpy(error->lib, "mongolite", sizeof(error->lib) - 1);
            error->lib[sizeof(error->lib) - 1] = '\0';
        }
        return false;
    }
    return true;
}

/**
 * EXTRACT_OID_OR_CONTINUE - Extract OID or skip to next iteration
 *
 * For use in loops where we want to skip documents without valid _id
 */
#define EXTRACT_OID_OR_CONTINUE(doc, out_oid) \
    do { \
        if (MONGOLITE_UNLIKELY(!extract_doc_oid((doc), &(out_oid)))) { \
            continue; \
        } \
    } while (0)

/**
 * EXTRACT_OID_OR_FAIL - Extract OID or return with error
 *
 * For use when _id is required and missing is an error
 */
#define EXTRACT_OID_OR_FAIL(doc, out_oid, error, ret_val) \
    do { \
        if (MONGOLITE_UNLIKELY(!extract_doc_oid_with_error((doc), &(out_oid), (error)))) { \
            return (ret_val); \
        } \
    } while (0)

/* ============================================================
 * JSON Parsing Helpers
 *
 * Parse JSON strings to BSON with consistent error handling.
 * ============================================================ */

/**
 * Parse JSON string to BSON document
 *
 * @param json_str  The JSON string to parse
 * @param error     Error struct to populate on failure
 * @return          Parsed BSON document (caller owns), or NULL on error
 */
MONGOLITE_ALWAYS_INLINE
static inline bson_t* parse_json_to_bson(const char *json_str, gerror_t *error) {
    if (MONGOLITE_UNLIKELY(!json_str)) {
        if (error) {
            error->code = -1003;  /* MONGOLITE_EINVAL */
            snprintf(error->message, sizeof(error->message), "JSON string is NULL");
            strncpy(error->lib, "mongolite", sizeof(error->lib) - 1);
            error->lib[sizeof(error->lib) - 1] = '\0';
        }
        return NULL;
    }

    bson_error_t bson_err;
    bson_t *doc = bson_new_from_json((const uint8_t*)json_str, -1, &bson_err);
    if (MONGOLITE_UNLIKELY(!doc)) {
        if (error) {
            error->code = -1003;  /* MONGOLITE_EINVAL */
            snprintf(error->message, sizeof(error->message), "Invalid JSON: %s", bson_err.message);
            strncpy(error->lib, "libbson", sizeof(error->lib) - 1);
            error->lib[sizeof(error->lib) - 1] = '\0';
        }
        return NULL;
    }

    return doc;
}

/**
 * Parse optional JSON string to BSON document
 * Returns NULL without error if json_str is NULL
 *
 * @param json_str  The JSON string to parse (may be NULL)
 * @param error     Error struct to populate on parse failure
 * @return          Parsed BSON document, or NULL (check error for failure)
 */
MONGOLITE_ALWAYS_INLINE
static inline bson_t* parse_optional_json_to_bson(const char *json_str, gerror_t *error) {
    if (!json_str) {
        return NULL;  /* Not an error - explicitly optional */
    }
    return parse_json_to_bson(json_str, error);
}

/* ============================================================
 * Dynamic Array Helpers
 *
 * Common pattern: grow an array when capacity is reached
 * ============================================================ */

/**
 * GROW_ARRAY - Grow a dynamic array when capacity is reached
 *
 * Usage:
 *   GROW_ARRAY(items, n_items, capacity, item_type, cleanup_block);
 *
 * @param arr       The array pointer
 * @param count     Current item count
 * @param cap       Current capacity
 * @param type      Type of array elements
 * @param cleanup   Block of cleanup code to run on allocation failure
 */
#define GROW_ARRAY(arr, count, cap, type, cleanup) \
    do { \
        if (MONGOLITE_UNLIKELY((count) >= (cap))) { \
            (cap) *= 2; \
            type *_new_arr = realloc((arr), sizeof(type) * (cap)); \
            if (MONGOLITE_UNLIKELY(!_new_arr)) { \
                cleanup; \
            } \
            (arr) = _new_arr; \
        } \
    } while (0)

/**
 * INIT_DYNAMIC_ARRAY - Initialize a dynamic array with default capacity
 *
 * @param arr       Array pointer to initialize
 * @param cap       Capacity variable
 * @param type      Type of array elements
 * @param init_cap  Initial capacity
 * @param on_fail   Code to execute on failure
 */
#define INIT_DYNAMIC_ARRAY(arr, cap, type, init_cap, on_fail) \
    do { \
        (cap) = (init_cap); \
        (arr) = malloc(sizeof(type) * (cap)); \
        if (MONGOLITE_UNLIKELY(!(arr))) { \
            on_fail; \
        } \
    } while (0)

/* ============================================================
 * Cleanup Helpers
 *
 * Common cleanup patterns for early returns
 * ============================================================ */

/**
 * CLEANUP_AND_RETURN - Execute cleanup code and return
 *
 * Usage:
 *   CLEANUP_AND_RETURN({ bson_destroy(doc); free(ptr); }, -1);
 */
#define CLEANUP_AND_RETURN(cleanup, ret_val) \
    do { \
        cleanup; \
        return (ret_val); \
    } while (0)

/**
 * CLEANUP_BSON_ARRAY - Destroy an array of BSON documents
 *
 * @param arr       Array of bson_t* pointers
 * @param count     Number of elements
 */
MONGOLITE_ALWAYS_INLINE
static inline void cleanup_bson_array(bson_t **arr, size_t count) {
    if (arr) {
        for (size_t i = 0; i < count; i++) {
            if (arr[i]) {
                bson_destroy(arr[i]);
            }
        }
        free(arr);
    }
}

/* ============================================================
 * Transaction Cleanup Pattern
 *
 * Common pattern: abort transaction, unlock, return
 * ============================================================ */

/**
 * Operation context for cleanup helpers
 * Groups common resources that need cleanup on failure
 */
typedef struct {
    mongolite_db_t *db;
    void *txn;              /* wtree3_txn_t* - void* to avoid include dependency */
    bson_t *doc1;           /* First document to cleanup */
    bson_t *doc2;           /* Second document to cleanup */
    void *array;            /* Array to free */
    size_t array_count;     /* For BSON arrays */
    bool array_is_bson;     /* If true, array contains bson_t* to destroy */
    bool locked;            /* If true, unlock db */
    bool has_txn;           /* If true, abort transaction */
} cleanup_ctx_t;

/**
 * Initialize cleanup context with defaults
 */
#define CLEANUP_CTX_INIT(ctx, database) \
    do { \
        memset(&(ctx), 0, sizeof(cleanup_ctx_t)); \
        (ctx).db = (database); \
    } while (0)

#ifdef __cplusplus
}
#endif

#endif /* MONGOLITE_HELPERS_H */
