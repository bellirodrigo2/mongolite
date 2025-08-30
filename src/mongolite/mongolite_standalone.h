/*
** MongoLite - Standalone Core Interface
** MongoDB-compatible document database using SQLite storage
** 
** This version is standalone for testing without full libbson build
*/
#ifndef MONGOLITE_STANDALONE_H
#define MONGOLITE_STANDALONE_H

#include <sqlite3.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
** Minimal BSON type for testing (will be replaced by full libbson interface)
*/
typedef struct {
    uint8_t *data;
    uint32_t length;
} mongolite_bson_t;

/*
** Core MongoLite Types
*/
typedef struct mongolite_db mongolite_db_t;
typedef struct mongolite_collection mongolite_collection_t;
typedef struct mongolite_cursor mongolite_cursor_t;

/*
** Error handling
*/
typedef enum {
    MONGOLITE_OK = 0,
    MONGOLITE_ERROR_INVALID_ARGUMENT,
    MONGOLITE_ERROR_OUT_OF_MEMORY,
    MONGOLITE_ERROR_DATABASE_ERROR,
    MONGOLITE_ERROR_COLLECTION_NOT_FOUND,
    MONGOLITE_ERROR_DOCUMENT_NOT_FOUND,
    MONGOLITE_ERROR_INVALID_BSON,
    MONGOLITE_ERROR_TRANSACTION_FAILED
} mongolite_error_t;

typedef struct {
    mongolite_error_t code;
    char message[256];
    int sqlite_error;
} mongolite_result_t;

/*
** Database Operations
*/
mongolite_db_t* mongolite_open(const char *path, mongolite_result_t *result);
void mongolite_close(mongolite_db_t *db);
mongolite_collection_t* mongolite_get_collection(mongolite_db_t *db, const char *name, mongolite_result_t *result);

/*
** Collection Operations (MongoDB-style CRUD)
*/
bool mongolite_insert_one(mongolite_collection_t *coll, const mongolite_bson_t *doc, mongolite_result_t *result);
mongolite_bson_t* mongolite_find_one(mongolite_collection_t *coll, const mongolite_bson_t *filter, mongolite_result_t *result);

/*
** Utility Functions
*/
const char* mongolite_get_version(void);
const char* mongolite_error_string(mongolite_error_t error);
void mongolite_result_clear(mongolite_result_t *result);

/*
** Minimal BSON interface for testing
*/
static inline mongolite_bson_t* mongolite_bson_new_from_data(const uint8_t *data, uint32_t length) {
    if (!data || length == 0) return NULL;
    
    mongolite_bson_t *bson = malloc(sizeof(mongolite_bson_t));
    if (!bson) return NULL;
    
    bson->data = malloc(length);
    if (!bson->data) {
        free(bson);
        return NULL;
    }
    
    memcpy(bson->data, data, length);
    bson->length = length;
    return bson;
}

static inline const uint8_t* mongolite_bson_get_data(const mongolite_bson_t *bson, uint32_t *length) {
    if (!bson) {
        if (length) *length = 0;
        return NULL;
    }
    if (length) *length = bson->length;
    return bson->data;
}

static inline void mongolite_bson_destroy(mongolite_bson_t *bson) {
    if (bson) {
        free(bson->data);
        free(bson);
    }
}

#ifdef __cplusplus
}
#endif

#endif /* MONGOLITE_STANDALONE_H */