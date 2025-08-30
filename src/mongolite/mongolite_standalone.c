/*
** MongoLite Standalone Implementation
** SQLite document database without external BSON dependencies
*/

#include "mongolite_standalone.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/*
** Internal Structures
*/
struct mongolite_db {
    sqlite3 *db;
    bool in_transaction;
    char path[512];
};

struct mongolite_collection {
    mongolite_db_t *db;
    char name[128];
    char table_name[150]; // "collection_" + name
};

/*
** Schema Setup
*/
static const char *SCHEMA_SQL = 
    "CREATE TABLE IF NOT EXISTS _mongolite_collections ("
    "  name TEXT PRIMARY KEY,"
    "  created_at INTEGER,"
    "  document_count INTEGER DEFAULT 0,"
    "  indexes TEXT DEFAULT '[]'"
    ");";

/*
** Utility Functions
*/
static void mongolite_result_set_error(mongolite_result_t *result, mongolite_error_t code, const char *msg, int sqlite_error) {
    if (!result) return;
    result->code = code;
    result->sqlite_error = sqlite_error;
    strncpy(result->message, msg ? msg : "", sizeof(result->message) - 1);
    result->message[sizeof(result->message) - 1] = '\0';
}

static void mongolite_result_set_ok(mongolite_result_t *result) {
    if (!result) return;
    result->code = MONGOLITE_OK;
    result->sqlite_error = SQLITE_OK;
    result->message[0] = '\0';
}

static bool is_valid_collection_name(const char *name) {
    if (!name || strlen(name) == 0 || strlen(name) > 100) return false;
    // MongoDB collection name rules (simplified)
    for (size_t i = 0; name[i]; i++) {
        char c = name[i];
        if (!(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && 
            !(c >= '0' && c <= '9') && c != '_' && c != '-') {
            return false;
        }
    }
    return true;
}

/*
** Database Operations Implementation
*/
mongolite_db_t* mongolite_open(const char *path, mongolite_result_t *result) {
    if (!path) {
        mongolite_result_set_error(result, MONGOLITE_ERROR_INVALID_ARGUMENT, "Path cannot be null", 0);
        return NULL;
    }

    mongolite_db_t *db = calloc(1, sizeof(mongolite_db_t));
    if (!db) {
        mongolite_result_set_error(result, MONGOLITE_ERROR_OUT_OF_MEMORY, "Failed to allocate database structure", 0);
        return NULL;
    }

    // Open SQLite database
    int rc = sqlite3_open_v2(path, &db->db, 
                            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, 
                            NULL);
    
    if (rc != SQLITE_OK) {
        const char *err = sqlite3_errmsg(db->db);
        mongolite_result_set_error(result, MONGOLITE_ERROR_DATABASE_ERROR, err, rc);
        if (db->db) sqlite3_close(db->db);
        free(db);
        return NULL;
    }

    strncpy(db->path, path, sizeof(db->path) - 1);
    db->in_transaction = false;

    // Set up MongoLite schema
    char *err_msg = NULL;
    rc = sqlite3_exec(db->db, SCHEMA_SQL, NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        mongolite_result_set_error(result, MONGOLITE_ERROR_DATABASE_ERROR, err_msg, rc);
        sqlite3_free(err_msg);
        sqlite3_close(db->db);
        free(db);
        return NULL;
    }

    mongolite_result_set_ok(result);
    return db;
}

void mongolite_close(mongolite_db_t *db) {
    if (!db) return;
    
    if (db->db) {
        sqlite3_close(db->db);
    }
    free(db);
}

mongolite_collection_t* mongolite_get_collection(mongolite_db_t *db, const char *name, mongolite_result_t *result) {
    if (!db || !name || !is_valid_collection_name(name)) {
        mongolite_result_set_error(result, MONGOLITE_ERROR_INVALID_ARGUMENT, "Invalid database or collection name", 0);
        return NULL;
    }

    mongolite_collection_t *coll = calloc(1, sizeof(mongolite_collection_t));
    if (!coll) {
        mongolite_result_set_error(result, MONGOLITE_ERROR_OUT_OF_MEMORY, "Failed to allocate collection", 0);
        return NULL;
    }

    coll->db = db;
    strncpy(coll->name, name, sizeof(coll->name) - 1);
    snprintf(coll->table_name, sizeof(coll->table_name), "collection_%s", name);

    // Create collection table if it doesn't exist
    char sql[512];
    snprintf(sql, sizeof(sql), 
             "CREATE TABLE IF NOT EXISTS %s ("
             "_id TEXT PRIMARY KEY,"
             "document BLOB NOT NULL,"
             "created_at INTEGER,"
             "updated_at INTEGER"
             ");", coll->table_name);

    char *err_msg = NULL;
    int rc = sqlite3_exec(db->db, sql, NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        mongolite_result_set_error(result, MONGOLITE_ERROR_DATABASE_ERROR, err_msg, rc);
        sqlite3_free(err_msg);
        free(coll);
        return NULL;
    }

    // Register collection in metadata table
    snprintf(sql, sizeof(sql), 
             "INSERT OR IGNORE INTO _mongolite_collections (name, created_at) VALUES (?, ?);");
    
    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db->db, sql, -1, &stmt, NULL);
    if (rc == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, name, -1, SQLITE_STATIC);
        sqlite3_bind_int64(stmt, 2, time(NULL));
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }

    mongolite_result_set_ok(result);
    return coll;
}

/*
** Insert Operations
*/
bool mongolite_insert_one(mongolite_collection_t *coll, const mongolite_bson_t *doc, mongolite_result_t *result) {
    if (!coll || !doc) {
        mongolite_result_set_error(result, MONGOLITE_ERROR_INVALID_ARGUMENT, "Invalid collection or document", 0);
        return false;
    }

    // Get BSON binary data
    uint32_t doc_len;
    const uint8_t *doc_data = mongolite_bson_get_data(doc, &doc_len);
    if (!doc_data) {
        mongolite_result_set_error(result, MONGOLITE_ERROR_INVALID_BSON, "Failed to get BSON data", 0);
        return false;
    }

    // Generate _id (simplified: use timestamp + random)
    char id_str[64];
    snprintf(id_str, sizeof(id_str), "%ld_%d", time(NULL), rand() % 10000);

    // Insert document
    char sql[256];
    snprintf(sql, sizeof(sql), 
             "INSERT INTO %s (_id, document, created_at, updated_at) VALUES (?, ?, ?, ?);",
             coll->table_name);

    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(coll->db->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        mongolite_result_set_error(result, MONGOLITE_ERROR_DATABASE_ERROR, sqlite3_errmsg(coll->db->db), rc);
        return false;
    }

    int64_t now = time(NULL);
    sqlite3_bind_text(stmt, 1, id_str, -1, SQLITE_TRANSIENT);
    sqlite3_bind_blob(stmt, 2, doc_data, doc_len, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 3, now);
    sqlite3_bind_int64(stmt, 4, now);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        mongolite_result_set_error(result, MONGOLITE_ERROR_DATABASE_ERROR, sqlite3_errmsg(coll->db->db), rc);
        return false;
    }

    mongolite_result_set_ok(result);
    return true;
}

/*
** Find Operations  
*/
mongolite_bson_t* mongolite_find_one(mongolite_collection_t *coll, const mongolite_bson_t *filter, mongolite_result_t *result) {
    (void)filter; // Suppress unused parameter warning
    if (!coll) {
        mongolite_result_set_error(result, MONGOLITE_ERROR_INVALID_ARGUMENT, "Invalid collection", 0);
        return NULL;
    }

    char sql[256];
    snprintf(sql, sizeof(sql), "SELECT document FROM %s LIMIT 1;", coll->table_name);

    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(coll->db->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        mongolite_result_set_error(result, MONGOLITE_ERROR_DATABASE_ERROR, sqlite3_errmsg(coll->db->db), rc);
        return NULL;
    }

    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        // Get BSON data from BLOB
        const void *blob_data = sqlite3_column_blob(stmt, 0);
        int blob_size = sqlite3_column_bytes(stmt, 0);
        
        if (blob_data && blob_size > 0) {
            mongolite_bson_t *doc = mongolite_bson_new_from_data((const uint8_t*)blob_data, blob_size);
            sqlite3_finalize(stmt);
            
            if (doc) {
                mongolite_result_set_ok(result);
                return doc;
            }
        }
    }
    
    sqlite3_finalize(stmt);
    
    if (rc == SQLITE_DONE) {
        mongolite_result_set_error(result, MONGOLITE_ERROR_DOCUMENT_NOT_FOUND, "No documents found", 0);
    } else {
        mongolite_result_set_error(result, MONGOLITE_ERROR_DATABASE_ERROR, sqlite3_errmsg(coll->db->db), rc);
    }
    
    return NULL;
}

/*
** Utility Functions Implementation
*/
const char* mongolite_get_version(void) {
    return "MongoLite 1.0 (SQLite " SQLITE_VERSION " + Standalone BSON)";
}

const char* mongolite_error_string(mongolite_error_t error) {
    switch (error) {
        case MONGOLITE_OK: return "Success";
        case MONGOLITE_ERROR_INVALID_ARGUMENT: return "Invalid argument";
        case MONGOLITE_ERROR_OUT_OF_MEMORY: return "Out of memory";
        case MONGOLITE_ERROR_DATABASE_ERROR: return "Database error";
        case MONGOLITE_ERROR_COLLECTION_NOT_FOUND: return "Collection not found";
        case MONGOLITE_ERROR_DOCUMENT_NOT_FOUND: return "Document not found";
        case MONGOLITE_ERROR_INVALID_BSON: return "Invalid BSON";
        case MONGOLITE_ERROR_TRANSACTION_FAILED: return "Transaction failed";
        default: return "Unknown error";
    }
}

void mongolite_result_clear(mongolite_result_t *result) {
    if (result) {
        memset(result, 0, sizeof(mongolite_result_t));
    }
}