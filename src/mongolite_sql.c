/*
 * mongolite_sql.c - SQL abstraction layer for MongoLite
 * 
 * This module encapsulates all SQLite SQL statement operations to make
 * future migration to direct B-tree API easier. All SQL operations are
 * isolated here for clean separation.
 */

#include "mongolite_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Initialize database schema
int mlite_sql_init_schema(sqlite3 *db) {
    const char *create_metadata_sql = 
        "CREATE TABLE IF NOT EXISTS _mlite_collections ("
        "name TEXT PRIMARY KEY, "
        "created_at INTEGER DEFAULT (strftime('%s','now'))"
        ")";
    
    return sqlite3_exec(db, create_metadata_sql, NULL, NULL, NULL);
}

// Create collection table
int mlite_sql_create_collection_table(sqlite3 *db, const char *collection_name) {
    char *sql = NULL;
    int sql_len = snprintf(NULL, 0, 
        "CREATE TABLE collection_%s (_id TEXT PRIMARY KEY, document BLOB NOT NULL)", 
        collection_name);
    
    sql = malloc(sql_len + 1);
    if (!sql) {
        return SQLITE_NOMEM;
    }
    
    snprintf(sql, sql_len + 1, 
        "CREATE TABLE collection_%s (_id TEXT PRIMARY KEY, document BLOB NOT NULL)", 
        collection_name);

    int rc = sqlite3_exec(db, sql, NULL, NULL, NULL);
    free(sql);
    
    return rc;
}

// Add collection to metadata table
int mlite_sql_add_collection_metadata(sqlite3 *db, const char *collection_name) {
    const char *insert_metadata_sql = 
        "INSERT INTO _mlite_collections (name, created_at) VALUES (?, strftime('%s','now'))";
    
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(db, insert_metadata_sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        return rc;
    }

    rc = sqlite3_bind_text(stmt, 1, collection_name, -1, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(stmt);
        return rc;
    }

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    
    return (rc == SQLITE_DONE) ? SQLITE_OK : rc;
}

// Drop collection table
int mlite_sql_drop_collection_table(sqlite3 *db, const char *collection_name) {
    char *sql = NULL;
    int sql_len = snprintf(NULL, 0, "DROP TABLE collection_%s", collection_name);
    
    sql = malloc(sql_len + 1);
    if (!sql) {
        return SQLITE_NOMEM;
    }
    
    snprintf(sql, sql_len + 1, "DROP TABLE collection_%s", collection_name);

    int rc = sqlite3_exec(db, sql, NULL, NULL, NULL);
    free(sql);
    
    return rc;
}

// Remove collection from metadata table
int mlite_sql_remove_collection_metadata(sqlite3 *db, const char *collection_name) {
    const char *delete_metadata_sql = "DELETE FROM _mlite_collections WHERE name = ?";
    
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(db, delete_metadata_sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        return rc;
    }

    rc = sqlite3_bind_text(stmt, 1, collection_name, -1, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(stmt);
        return rc;
    }

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    
    return (rc == SQLITE_DONE) ? SQLITE_OK : rc;
}

// Check if collection exists in metadata
bool mlite_sql_collection_exists(sqlite3 *db, const char *collection_name) {
    const char *check_sql = "SELECT 1 FROM _mlite_collections WHERE name = ? LIMIT 1";
    
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(db, check_sql, -1, &stmt, NULL);
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

// Prepare insert statement for document
int mlite_sql_prepare_document_insert(sqlite3 *db, const char *collection_name, sqlite3_stmt **stmt) {
    char *sql = NULL;
    int sql_len = snprintf(NULL, 0, "INSERT INTO collection_%s (_id, document) VALUES (?, ?)", collection_name);
    sql = malloc(sql_len + 1);
    if (!sql) {
        return SQLITE_NOMEM;
    }
    
    snprintf(sql, sql_len + 1, "INSERT INTO collection_%s (_id, document) VALUES (?, ?)", collection_name);

    int rc = sqlite3_prepare_v2(db, sql, -1, stmt, NULL);
    free(sql);
    
    return rc;
}

// Execute single document insert
int mlite_sql_insert_document(sqlite3_stmt *stmt, const char *oid_str, const uint8_t *bson_data, uint32_t bson_len) {
    // Reset statement for reuse
    sqlite3_reset(stmt);
    
    // Bind _id string
    int rc = sqlite3_bind_text(stmt, 1, oid_str, -1, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        return rc;
    }
    
    // Bind BSON document
    rc = sqlite3_bind_blob(stmt, 2, bson_data, bson_len, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        return rc;
    }

    // Execute the statement
    rc = sqlite3_step(stmt);
    return rc;
}

// Transaction operations
int mlite_sql_begin_transaction(sqlite3 *db) {
    return sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);
}

int mlite_sql_commit_transaction(sqlite3 *db) {
    return sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
}

int mlite_sql_rollback_transaction(sqlite3 *db) {
    return sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
}

// Prepare query statement for collection
int mlite_sql_prepare_collection_query(sqlite3 *db, const char *collection_name, sqlite3_stmt **stmt) {
    char *sql = NULL;
    int sql_len = snprintf(NULL, 0, "SELECT _id, document FROM collection_%s", collection_name);
    sql = malloc(sql_len + 1);
    if (!sql) {
        return SQLITE_NOMEM;
    }
    
    snprintf(sql, sql_len + 1, "SELECT _id, document FROM collection_%s", collection_name);
    
    int rc = sqlite3_prepare_v2(db, sql, -1, stmt, NULL);
    free(sql);
    
    return rc;
}

// Execute query step and get document data
int mlite_sql_query_step(sqlite3_stmt *stmt, const char **oid_str, const void **document_data, int *document_len) {
    int rc = sqlite3_step(stmt);
    
    if (rc == SQLITE_ROW) {
        *oid_str = (const char*)sqlite3_column_text(stmt, 0);
        *document_data = sqlite3_column_blob(stmt, 1);
        *document_len = sqlite3_column_bytes(stmt, 1);
    }
    
    return rc;
}