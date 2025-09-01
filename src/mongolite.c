#include "mongolite.h"
#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// Internal database structure
struct mlite_db {
    sqlite3 *sqlite_db;
    char *filename;
    int flags;
    char *errmsg;
    int errcode;
};

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