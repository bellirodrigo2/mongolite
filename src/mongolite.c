#include "mongolite.h"
#include "mongolite_internal.h"
#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <bson/bson.h>


// SQLite result codes mapping to mongolite codes
#define MLITE_OK           0
#define MLITE_ERROR        1
#define MLITE_CANTOPEN     14
#define MLITE_NOMEM        7

// Database operations
int mlite_open(const char *filename, mlite_db_t **db) {
    return mlite_open_v2(filename, db, MLITE_OPEN_READWRITE | MLITE_OPEN_CREATE);
}

int mlite_open_v2(const char *filename, mlite_db_t **db, int flags) {
    if (!filename || !db) {
        return MLITE_ERROR;
    }

    // Allocate database structure
    mlite_db_t *database = (mlite_db_t*)malloc(sizeof(mlite_db_t));
    if (!database) {
        return MLITE_NOMEM;
    }

    // Initialize structure
    memset(database, 0, sizeof(mlite_db_t));
    database->flags = flags;
    
    // Copy filename
    database->filename = strdup(filename);
    if (!database->filename) {
        free(database);
        return MLITE_NOMEM;
    }

    // Open SQLite database
    int sqlite_flags = 0;
    if (flags & MLITE_OPEN_READONLY) {
        sqlite_flags |= SQLITE_OPEN_READONLY;
    } else {
        sqlite_flags |= SQLITE_OPEN_READWRITE;
    }
    if (flags & MLITE_OPEN_CREATE) {
        sqlite_flags |= SQLITE_OPEN_CREATE;
    }

    int rc = sqlite3_open_v2(filename, &database->sqlite_db, sqlite_flags, NULL);
    if (rc != SQLITE_OK) {
        database->errcode = rc;
        if (database->sqlite_db) {
            database->errmsg = strdup(sqlite3_errmsg(database->sqlite_db));
            sqlite3_close(database->sqlite_db);
        }
        free(database->filename);
        free(database);
        return (rc == SQLITE_CANTOPEN) ? MLITE_CANTOPEN : MLITE_ERROR;
    }

    // Create metadata table for collections if it doesn't exist
    const char *create_metadata_sql = 
        "CREATE TABLE IF NOT EXISTS _mlite_collections ("
        "name TEXT PRIMARY KEY, "
        "created_at INTEGER DEFAULT (strftime('%s','now'))"
        ")";
    
    rc = sqlite3_exec(database->sqlite_db, create_metadata_sql, NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        database->errcode = rc;
        database->errmsg = strdup(sqlite3_errmsg(database->sqlite_db));
        sqlite3_close(database->sqlite_db);
        free(database->filename);
        free(database);
        return MLITE_ERROR;
    }

    *db = database;
    return MLITE_OK;
}

int mlite_close(mlite_db_t *db) {
    if (!db) {
        return MLITE_ERROR;
    }

    int rc = MLITE_OK;
    
    // Close SQLite database
    if (db->sqlite_db) {
        int sqlite_rc = sqlite3_close(db->sqlite_db);
        if (sqlite_rc != SQLITE_OK) {
            rc = MLITE_ERROR;
        }
    }

    // Free allocated memory
    if (db->filename) {
        free(db->filename);
    }
    if (db->errmsg) {
        free(db->errmsg);
    }
    
    free(db);
    return rc;
}

// Utility functions
const char* mlite_errmsg(mlite_db_t *db) {
    if (!db) {
        return "Invalid database handle";
    }
    return db->errmsg ? db->errmsg : "No error";
}

int mlite_errcode(mlite_db_t *db) {
    if (!db) {
        return MLITE_ERROR;
    }
    return db->errcode;
}

// Collection operations
int mlite_collection_create(mlite_db_t *db, const char *collection_name) {
    if (!db || !collection_name) {
        return MLITE_ERROR;
    }

    // Clear previous error
    if (db->errmsg) {
        free(db->errmsg);
        db->errmsg = NULL;
    }
    db->errcode = MLITE_OK;

    // Check if collection already exists
    if (mlite_collection_exists(db, collection_name)) {
        return MLITE_OK; // Collection already exists, return success
    }

    // Create collection table: collection_<name> (_id TEXT PRIMARY KEY, document BLOB)
    char *sql = NULL;
    int sql_len = snprintf(NULL, 0, 
        "CREATE TABLE collection_%s (_id TEXT PRIMARY KEY, document BLOB NOT NULL)", 
        collection_name);
    
    sql = malloc(sql_len + 1);
    if (!sql) {
        db->errcode = MLITE_NOMEM;
        return MLITE_NOMEM;
    }
    
    snprintf(sql, sql_len + 1, 
        "CREATE TABLE collection_%s (_id TEXT PRIMARY KEY, document BLOB NOT NULL)", 
        collection_name);

    int rc = sqlite3_exec(db->sqlite_db, sql, NULL, NULL, NULL);
    free(sql);

    if (rc != SQLITE_OK) {
        db->errcode = rc;
        db->errmsg = strdup(sqlite3_errmsg(db->sqlite_db));
        return MLITE_ERROR;
    }

    // Add entry to metadata table
    const char *insert_metadata_sql = 
        "INSERT INTO _mlite_collections (name, created_at) VALUES (?, strftime('%s','now'))";
    
    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db->sqlite_db, insert_metadata_sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        db->errcode = rc;
        db->errmsg = strdup(sqlite3_errmsg(db->sqlite_db));
        return MLITE_ERROR;
    }

    rc = sqlite3_bind_text(stmt, 1, collection_name, -1, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(stmt);
        db->errcode = rc;
        db->errmsg = strdup(sqlite3_errmsg(db->sqlite_db));
        return MLITE_ERROR;
    }

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        db->errcode = rc;
        db->errmsg = strdup(sqlite3_errmsg(db->sqlite_db));
        return MLITE_ERROR;
    }

    return MLITE_OK;
}

int mlite_collection_drop(mlite_db_t *db, const char *collection_name) {
    if (!db || !collection_name) {
        return MLITE_ERROR;
    }

    // Clear previous error
    if (db->errmsg) {
        free(db->errmsg);
        db->errmsg = NULL;
    }
    db->errcode = MLITE_OK;

    // Check if collection exists
    if (!mlite_collection_exists(db, collection_name)) {
        return MLITE_OK; // Collection doesn't exist, return success
    }

    // Drop collection table
    char *sql = NULL;
    int sql_len = snprintf(NULL, 0, "DROP TABLE collection_%s", collection_name);
    
    sql = malloc(sql_len + 1);
    if (!sql) {
        db->errcode = MLITE_NOMEM;
        return MLITE_NOMEM;
    }
    
    snprintf(sql, sql_len + 1, "DROP TABLE collection_%s", collection_name);

    int rc = sqlite3_exec(db->sqlite_db, sql, NULL, NULL, NULL);
    free(sql);

    if (rc != SQLITE_OK) {
        db->errcode = rc;
        db->errmsg = strdup(sqlite3_errmsg(db->sqlite_db));
        return MLITE_ERROR;
    }

    // Remove entry from metadata table
    const char *delete_metadata_sql = "DELETE FROM _mlite_collections WHERE name = ?";
    
    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db->sqlite_db, delete_metadata_sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        db->errcode = rc;
        db->errmsg = strdup(sqlite3_errmsg(db->sqlite_db));
        return MLITE_ERROR;
    }

    rc = sqlite3_bind_text(stmt, 1, collection_name, -1, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(stmt);
        db->errcode = rc;
        db->errmsg = strdup(sqlite3_errmsg(db->sqlite_db));
        return MLITE_ERROR;
    }

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        db->errcode = rc;
        db->errmsg = strdup(sqlite3_errmsg(db->sqlite_db));
        return MLITE_ERROR;
    }

    return MLITE_OK;
}

bool mlite_collection_exists(mlite_db_t *db, const char *collection_name) {
    if (!db || !collection_name) {
        return false;
    }

    const char *check_sql = "SELECT 1 FROM _mlite_collections WHERE name = ? LIMIT 1";
    
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(db->sqlite_db, check_sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        return false;
    }

    rc = sqlite3_bind_text(stmt, 1, collection_name, -1, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(stmt);
        return false;
    }

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    return (rc == SQLITE_ROW);
}

// Document operations
int mlite_insert_one(mlite_db_t *db, const char *collection_name, 
                     const bson_t *doc, bson_error_t *error) {
    if (!db || !collection_name || !doc) {
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 1, "Invalid parameters");
        }
        return MLITE_ERROR;
    }

    // Clear previous error
    if (db->errmsg) {
        free(db->errmsg);
        db->errmsg = NULL;
    }
    db->errcode = MLITE_OK;

    // Validate BSON document
    size_t offset;
    if (!bson_validate(doc, BSON_VALIDATE_NONE, &offset)) {
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 2, "Invalid BSON document at offset %zu", offset);
        }
        db->errcode = MLITE_ERROR;
        return MLITE_ERROR;
    }

    // Check if collection exists
    if (!mlite_collection_exists(db, collection_name)) {
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 3, "Collection '%s' does not exist", collection_name);
        }
        db->errcode = MLITE_ERROR;
        return MLITE_ERROR;
    }

    // Handle _id field - check if document has one
    bson_t *doc_to_insert = NULL;
    const bson_t *final_doc = doc;
    
    if (!bson_has_field(doc, "_id")) {
        // Generate new ObjectId and create new document with _id
        bson_oid_t oid;
        bson_oid_init(&oid, NULL);  // Use default context
        
        // Create a copy of the original document
        doc_to_insert = bson_copy(doc);
        if (!doc_to_insert) {
            if (error) {
                bson_set_error(error, BSON_ERROR_INVALID, 4, "Failed to copy document");
            }
            db->errcode = MLITE_NOMEM;
            return MLITE_NOMEM;
        }
        
        // Append _id to the copy
        if (!bson_append_oid(doc_to_insert, "_id", -1, &oid)) {
            if (error) {
                bson_set_error(error, BSON_ERROR_INVALID, 5, "Failed to append _id field");
            }
            bson_destroy(doc_to_insert);
            db->errcode = MLITE_ERROR;
            return MLITE_ERROR;
        }
        
        final_doc = doc_to_insert;
    } else {
        // Validate _id field is ObjectId
        bson_iter_t iter;
        if (bson_iter_init(&iter, doc) && bson_iter_find(&iter, "_id")) {
            if (!BSON_ITER_HOLDS_OID(&iter)) {
                if (error) {
                    bson_set_error(error, BSON_ERROR_INVALID, 6, "_id field must be ObjectId");
                }
                db->errcode = MLITE_ERROR;
                return MLITE_ERROR;
            }
        }
    }

    // Prepare SQL statement for insertion
    char *sql = NULL;
    int sql_len = snprintf(NULL, 0, "INSERT INTO collection_%s (_id, document) VALUES (?, ?)", collection_name);
    sql = malloc(sql_len + 1);
    if (!sql) {
        if (doc_to_insert) {
            bson_destroy(doc_to_insert);
        }
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 7, "Memory allocation failed");
        }
        db->errcode = MLITE_NOMEM;
        return MLITE_NOMEM;
    }
    
    snprintf(sql, sql_len + 1, "INSERT INTO collection_%s (_id, document) VALUES (?, ?)", collection_name);

    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(db->sqlite_db, sql, -1, &stmt, NULL);
    free(sql);
    
    if (rc != SQLITE_OK) {
        if (doc_to_insert) {
            bson_destroy(doc_to_insert);
        }
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 8, "Failed to prepare statement: %s", sqlite3_errmsg(db->sqlite_db));
        }
        db->errcode = rc;
        db->errmsg = strdup(sqlite3_errmsg(db->sqlite_db));
        return MLITE_ERROR;
    }

    // Extract _id as string for the key
    bson_iter_t iter;
    char oid_str[25];
    if (bson_iter_init(&iter, final_doc) && bson_iter_find(&iter, "_id")) {
        const bson_oid_t *oid = bson_iter_oid(&iter);
        bson_oid_to_string(oid, oid_str);
    } else {
        // This should never happen since we ensured _id exists above
        sqlite3_finalize(stmt);
        if (doc_to_insert) {
            bson_destroy(doc_to_insert);
        }
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 9, "Failed to extract _id field");
        }
        db->errcode = MLITE_ERROR;
        return MLITE_ERROR;
    }

    // Bind _id string
    rc = sqlite3_bind_text(stmt, 1, oid_str, -1, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(stmt);
        if (doc_to_insert) {
            bson_destroy(doc_to_insert);
        }
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 10, "Failed to bind _id: %s", sqlite3_errmsg(db->sqlite_db));
        }
        db->errcode = rc;
        db->errmsg = strdup(sqlite3_errmsg(db->sqlite_db));
        return MLITE_ERROR;
    }

    // Bind BSON document as BLOB
    const uint8_t *bson_data = bson_get_data(final_doc);
    uint32_t bson_len = final_doc->len;
    
    rc = sqlite3_bind_blob(stmt, 2, bson_data, bson_len, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(stmt);
        if (doc_to_insert) {
            bson_destroy(doc_to_insert);
        }
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 11, "Failed to bind document: %s", sqlite3_errmsg(db->sqlite_db));
        }
        db->errcode = rc;
        db->errmsg = strdup(sqlite3_errmsg(db->sqlite_db));
        return MLITE_ERROR;
    }

    // Execute the statement
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    // Clean up document copy if created
    if (doc_to_insert) {
        bson_destroy(doc_to_insert);
    }

    if (rc != SQLITE_DONE) {
        if (error) {
            if (rc == SQLITE_CONSTRAINT) {
                bson_set_error(error, BSON_ERROR_INVALID, 12, "Document with this _id already exists");
            } else {
                bson_set_error(error, BSON_ERROR_INVALID, 13, "Failed to insert document: %s", sqlite3_errmsg(db->sqlite_db));
            }
        }
        db->errcode = rc;
        db->errmsg = strdup(sqlite3_errmsg(db->sqlite_db));
        return MLITE_ERROR;
    }

    return MLITE_OK;
}
// Generic insert_one function with custom conversion
int mlite_insert_one_any(mlite_db_t *db, const char *collection_name, 
                         const void *doc, bson_error_t *error, convert_to_bson func) {
    if (!db || !collection_name || !doc || !func) {
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 100, "Invalid parameters for insert_one_any");
        }
        return MLITE_ERROR;
    }

    // Call the conversion function to convert input to BSON
    bson_t bson_doc = func((void *)doc);
    
    // Check if conversion was successful (empty BSON indicates failure)
    if (bson_empty(&bson_doc)) {
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 101, "Conversion function failed to create valid BSON");
        }
        return MLITE_ERROR;
    }

    // Call the core insert_one function
    int result = mlite_insert_one(db, collection_name, &bson_doc, error);
    
    // Clean up the converted BSON document
    bson_destroy(&bson_doc);
    
    return result;
}

// JSON string to BSON conversion function
int mlite_insert_one_jsonstr(mlite_db_t *db, const char *collection_name, 
                             const char *json_doc, bson_error_t *error) {
    if (!db || !collection_name || !json_doc) {
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 110, "Invalid parameters for insert_one_jsonstr");
        }
        return MLITE_ERROR;
    }

    // Convert JSON string to BSON
    bson_error_t conversion_error;
    bson_t *bson_doc = bson_new_from_json((const uint8_t *)json_doc, -1, &conversion_error);
    
    if (!bson_doc) {
        // JSON conversion failed - propagate the conversion error
        if (error) {
            bson_set_error(error, BSON_ERROR_JSON, 111, 
                          "JSON to BSON conversion failed: %s (domain: %u, code: %u)", 
                          conversion_error.message, conversion_error.domain, conversion_error.code);
        }
        return MLITE_ERROR;
    }

    // Validate the converted BSON
    size_t offset;
    if (!bson_validate(bson_doc, BSON_VALIDATE_NONE, &offset)) {
        if (error) {
            bson_set_error(error, BSON_ERROR_JSON, 112, 
                          "Converted BSON is invalid at offset %zu", offset);
        }
        bson_destroy(bson_doc);
        return MLITE_ERROR;
    }

    // Call the core insert_one function
    int result = mlite_insert_one(db, collection_name, bson_doc, error);
    
    // Clean up the converted BSON document
    bson_destroy(bson_doc);
    
    return result;
}

// Bulk insert BSON documents with transaction support
int mlite_insert_many(mlite_db_t *db, const char *collection_name,
                      const bson_t **docs, size_t n_docs, bson_error_t *error) {
    if (!db || !collection_name || !docs || n_docs == 0) {
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 200, "Invalid parameters for insert_many");
        }
        return MLITE_ERROR;
    }

    // Clear previous error
    if (db->errmsg) {
        free(db->errmsg);
        db->errmsg = NULL;
    }
    db->errcode = MLITE_OK;

    // Check if collection exists
    if (!mlite_collection_exists(db, collection_name)) {
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 201, "Collection '%s' does not exist", collection_name);
        }
        db->errcode = MLITE_ERROR;
        return MLITE_ERROR;
    }

    // Start transaction for bulk operation
    int rc = sqlite3_exec(db->sqlite_db, "BEGIN TRANSACTION", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 202, "Failed to begin transaction: %s", sqlite3_errmsg(db->sqlite_db));
        }
        db->errcode = rc;
        db->errmsg = strdup(sqlite3_errmsg(db->sqlite_db));
        return MLITE_ERROR;
    }

    // Prepare SQL statement once for better performance
    char *sql = NULL;
    int sql_len = snprintf(NULL, 0, "INSERT INTO collection_%s (_id, document) VALUES (?, ?)", collection_name);
    sql = malloc(sql_len + 1);
    if (!sql) {
        sqlite3_exec(db->sqlite_db, "ROLLBACK", NULL, NULL, NULL);
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 203, "Memory allocation failed");
        }
        db->errcode = MLITE_NOMEM;
        return MLITE_NOMEM;
    }
    snprintf(sql, sql_len + 1, "INSERT INTO collection_%s (_id, document) VALUES (?, ?)", collection_name);

    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db->sqlite_db, sql, -1, &stmt, NULL);
    free(sql);
    
    if (rc != SQLITE_OK) {
        sqlite3_exec(db->sqlite_db, "ROLLBACK", NULL, NULL, NULL);
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 204, "Failed to prepare statement: %s", sqlite3_errmsg(db->sqlite_db));
        }
        db->errcode = rc;
        db->errmsg = strdup(sqlite3_errmsg(db->sqlite_db));
        return MLITE_ERROR;
    }

    // Insert each document
    for (size_t i = 0; i < n_docs; i++) {
        const bson_t *doc = docs[i];
        
        if (!doc) {
            sqlite3_finalize(stmt);
            sqlite3_exec(db->sqlite_db, "ROLLBACK", NULL, NULL, NULL);
            if (error) {
                bson_set_error(error, BSON_ERROR_INVALID, 205, "Document at index %zu is NULL", i);
            }
            db->errcode = MLITE_ERROR;
            return MLITE_ERROR;
        }

        // Validate BSON document
        size_t offset;
        if (!bson_validate(doc, BSON_VALIDATE_NONE, &offset)) {
            sqlite3_finalize(stmt);
            sqlite3_exec(db->sqlite_db, "ROLLBACK", NULL, NULL, NULL);
            if (error) {
                bson_set_error(error, BSON_ERROR_INVALID, 206, "Invalid BSON document at index %zu, offset %zu", i, offset);
            }
            db->errcode = MLITE_ERROR;
            return MLITE_ERROR;
        }

        // Handle _id field - create document copy if needed
        bson_t *doc_to_insert = NULL;
        const bson_t *final_doc = doc;
        
        if (!bson_has_field(doc, "_id")) {
            // Generate new ObjectId
            bson_oid_t oid;
            bson_oid_init(&oid, NULL);
            
            doc_to_insert = bson_copy(doc);
            if (!doc_to_insert || !bson_append_oid(doc_to_insert, "_id", -1, &oid)) {
                if (doc_to_insert) bson_destroy(doc_to_insert);
                sqlite3_finalize(stmt);
                sqlite3_exec(db->sqlite_db, "ROLLBACK", NULL, NULL, NULL);
                if (error) {
                    bson_set_error(error, BSON_ERROR_INVALID, 207, "Failed to generate _id for document at index %zu", i);
                }
                db->errcode = MLITE_ERROR;
                return MLITE_ERROR;
            }
            final_doc = doc_to_insert;
        } else {
            // Validate _id field is ObjectId
            bson_iter_t iter;
            if (bson_iter_init(&iter, doc) && bson_iter_find(&iter, "_id")) {
                if (!BSON_ITER_HOLDS_OID(&iter)) {
                    sqlite3_finalize(stmt);
                    sqlite3_exec(db->sqlite_db, "ROLLBACK", NULL, NULL, NULL);
                    if (error) {
                        bson_set_error(error, BSON_ERROR_INVALID, 208, "_id field must be ObjectId in document at index %zu", i);
                    }
                    db->errcode = MLITE_ERROR;
                    return MLITE_ERROR;
                }
            }
        }

        // Extract _id as string
        bson_iter_t iter;
        char oid_str[25];
        if (bson_iter_init(&iter, final_doc) && bson_iter_find(&iter, "_id")) {
            const bson_oid_t *oid = bson_iter_oid(&iter);
            bson_oid_to_string(oid, oid_str);
        } else {
            if (doc_to_insert) bson_destroy(doc_to_insert);
            sqlite3_finalize(stmt);
            sqlite3_exec(db->sqlite_db, "ROLLBACK", NULL, NULL, NULL);
            if (error) {
                bson_set_error(error, BSON_ERROR_INVALID, 209, "Failed to extract _id from document at index %zu", i);
            }
            db->errcode = MLITE_ERROR;
            return MLITE_ERROR;
        }

        // Bind parameters
        sqlite3_reset(stmt);
        sqlite3_bind_text(stmt, 1, oid_str, -1, SQLITE_STATIC);
        
        const uint8_t *bson_data = bson_get_data(final_doc);
        uint32_t bson_len = final_doc->len;
        sqlite3_bind_blob(stmt, 2, bson_data, bson_len, SQLITE_STATIC);

        // Execute insert
        rc = sqlite3_step(stmt);
        
        // Clean up document copy if created
        if (doc_to_insert) {
            bson_destroy(doc_to_insert);
            doc_to_insert = NULL;
        }

        if (rc != SQLITE_DONE) {
            sqlite3_finalize(stmt);
            sqlite3_exec(db->sqlite_db, "ROLLBACK", NULL, NULL, NULL);
            if (error) {
                if (rc == SQLITE_CONSTRAINT) {
                    bson_set_error(error, BSON_ERROR_INVALID, 210, "Duplicate _id in document at index %zu", i);
                } else {
                    bson_set_error(error, BSON_ERROR_INVALID, 211, "Failed to insert document at index %zu: %s", i, sqlite3_errmsg(db->sqlite_db));
                }
            }
            db->errcode = rc;
            db->errmsg = strdup(sqlite3_errmsg(db->sqlite_db));
            return MLITE_ERROR;
        }
    }

    // Clean up statement
    sqlite3_finalize(stmt);

    // Commit transaction
    rc = sqlite3_exec(db->sqlite_db, "COMMIT", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_exec(db->sqlite_db, "ROLLBACK", NULL, NULL, NULL);
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 212, "Failed to commit transaction: %s", sqlite3_errmsg(db->sqlite_db));
        }
        db->errcode = rc;
        db->errmsg = strdup(sqlite3_errmsg(db->sqlite_db));
        return MLITE_ERROR;
    }

    return MLITE_OK;
}

// Bulk insert with custom conversion function
int mlite_insert_many_any(mlite_db_t *db, const char *collection_name,
                          const void **docs, size_t n_docs, 
                          bson_error_t *error, convert_to_bson func) {
    if (!db || !collection_name || !docs || !func || n_docs == 0) {
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 220, "Invalid parameters for insert_many_any");
        }
        return MLITE_ERROR;
    }

    // Convert all documents first
    const bson_t **bson_docs = malloc(n_docs * sizeof(bson_t *));
    if (!bson_docs) {
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 221, "Memory allocation failed for document array");
        }
        return MLITE_ERROR;
    }

    // Convert each document using the provided function
    for (size_t i = 0; i < n_docs; i++) {
        if (!docs[i]) {
            // Clean up previously converted docs
            for (size_t j = 0; j < i; j++) {
                bson_destroy((bson_t *)bson_docs[j]);
                free((bson_t *)bson_docs[j]);
            }
            free(bson_docs);
            if (error) {
                bson_set_error(error, BSON_ERROR_INVALID, 222, "Document at index %zu is NULL", i);
            }
            return MLITE_ERROR;
        }

        // Allocate memory for converted BSON
        bson_t *converted_doc = malloc(sizeof(bson_t));
        if (!converted_doc) {
            // Clean up previously converted docs
            for (size_t j = 0; j < i; j++) {
                bson_destroy((bson_t *)bson_docs[j]);
                free((bson_t *)bson_docs[j]);
            }
            free(bson_docs);
            if (error) {
                bson_set_error(error, BSON_ERROR_INVALID, 223, "Memory allocation failed for document at index %zu", i);
            }
            return MLITE_ERROR;
        }

        // Convert using provided function
        *converted_doc = func((void *)docs[i]);
        
        if (bson_empty(converted_doc)) {
            bson_destroy(converted_doc);
            free(converted_doc);
            // Clean up previously converted docs
            for (size_t j = 0; j < i; j++) {
                bson_destroy((bson_t *)bson_docs[j]);
                free((bson_t *)bson_docs[j]);
            }
            free(bson_docs);
            if (error) {
                bson_set_error(error, BSON_ERROR_INVALID, 224, "Conversion function failed for document at index %zu", i);
            }
            return MLITE_ERROR;
        }

        bson_docs[i] = converted_doc;
    }

    // Call the core insert_many function
    int result = mlite_insert_many(db, collection_name, bson_docs, n_docs, error);

    // Clean up all converted documents
    for (size_t i = 0; i < n_docs; i++) {
        bson_destroy((bson_t *)bson_docs[i]);
        free((bson_t *)bson_docs[i]);
    }
    free(bson_docs);

    return result;
}

// Bulk insert JSON strings
int mlite_insert_many_jsonstr(mlite_db_t *db, const char *collection_name,
                              const char **json_docs, size_t n_docs,
                              bson_error_t *error) {
    if (!db || !collection_name || !json_docs || n_docs == 0) {
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 230, "Invalid parameters for insert_many_jsonstr");
        }
        return MLITE_ERROR;
    }

    // Convert all JSON strings to BSON first
    const bson_t **bson_docs = malloc(n_docs * sizeof(bson_t *));
    if (!bson_docs) {
        if (error) {
            bson_set_error(error, BSON_ERROR_INVALID, 231, "Memory allocation failed for document array");
        }
        return MLITE_ERROR;
    }

    // Convert each JSON string to BSON
    for (size_t i = 0; i < n_docs; i++) {
        if (!json_docs[i]) {
            // Clean up previously converted docs
            for (size_t j = 0; j < i; j++) {
                bson_destroy((bson_t *)bson_docs[j]);
            }
            free(bson_docs);
            if (error) {
                bson_set_error(error, BSON_ERROR_INVALID, 232, "JSON document at index %zu is NULL", i);
            }
            return MLITE_ERROR;
        }

        // Convert JSON to BSON
        bson_error_t conversion_error;
        bson_t *bson_doc = bson_new_from_json((const uint8_t *)json_docs[i], -1, &conversion_error);
        
        if (!bson_doc) {
            // Clean up previously converted docs
            for (size_t j = 0; j < i; j++) {
                bson_destroy((bson_t *)bson_docs[j]);
            }
            free(bson_docs);
            if (error) {
                bson_set_error(error, BSON_ERROR_JSON, 233, 
                              "JSON to BSON conversion failed at index %zu: %s (domain: %u, code: %u)",
                              i, conversion_error.message, conversion_error.domain, conversion_error.code);
            }
            return MLITE_ERROR;
        }

        // Validate converted BSON
        size_t offset;
        if (!bson_validate(bson_doc, BSON_VALIDATE_NONE, &offset)) {
            bson_destroy(bson_doc);
            // Clean up previously converted docs
            for (size_t j = 0; j < i; j++) {
                bson_destroy((bson_t *)bson_docs[j]);
            }
            free(bson_docs);
            if (error) {
                bson_set_error(error, BSON_ERROR_JSON, 234,
                              "Converted BSON is invalid at index %zu, offset %zu", i, offset);
            }
            return MLITE_ERROR;
        }

        bson_docs[i] = bson_doc;
    }

    // Call the core insert_many function
    int result = mlite_insert_many(db, collection_name, bson_docs, n_docs, error);

    // Clean up all converted documents
    for (size_t i = 0; i < n_docs; i++) {
        bson_destroy((bson_t *)bson_docs[i]);
    }
    free(bson_docs);

    return result;
}



