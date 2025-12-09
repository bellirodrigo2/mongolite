#ifndef MONGOLITE_API_H
#define MONGOLITE_API_H

#include <bson/bson.h>  // BSON support

#ifdef __cplusplus
extern "C" {
#endif

// Database handle
typedef struct mongolite_db mongolite_db_t;

// Database operations (similar to sqlite3_open/close)
int mongolite_open(const char *filename, mongolite_db_t **db);
int mongolite_open_v2(const char *filename, mongolite_db_t **db, int flags);
int mongolite_close(mongolite_db_t *db);

// Collection operations
int mongolite_collection_create(mongolite_db_t *db, const char *collection_name);
int mongolite_collection_drop(mongolite_db_t *db, const char *collection_name);
bool mongolite_collection_exists(mongolite_db_t *db, const char *collection_name);

// Document operations
int mongolite_insert_one(mongolite_db_t *db, const char *collection_name, 
                    const bson_t *doc, bson_error_t *error);
int mongolite_insert_many(mongolite_db_t *db, const char *collection_name,
                    const bson_t **docs, size_t n_docs, 
                    bson_error_t *error);
bson_t* mongolite_find_one(mongolite_db_t *db, const char *collection_name,
                       const bson_t *matcher);
int mongolite_delete_one(mongolite_db_t *db, const char *collection_name,
                    const bson_t *filter, bson_error_t *error);
int mongolite_delete_many(mongolite_db_t *db, const char *collection_name,
                    const bson_t *filter, bson_error_t *error);

#ifdef __cplusplus
}
#endif

#endif // MONGOLITE_API_H