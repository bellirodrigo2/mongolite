#include <stdio.h>
#include <stdlib.h>
#include "sqlite3.h"

int main() {
    sqlite3 *db;
    char *err_msg = 0;
    int rc;
    
    // Test 1: Check SQLite version
    printf("SQLite version: %s\n", sqlite3_version);
    
    // Test 2: Open in-memory database
    rc = sqlite3_open(":memory:", &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }
    printf("✓ Successfully opened in-memory SQLite database\n");
    
    // Test 3: Create a simple table
    const char *sql_create = "CREATE TABLE test(id INTEGER PRIMARY KEY, name TEXT);";
    rc = sqlite3_exec(db, sql_create, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(db);
        return 1;
    }
    printf("✓ Successfully created table\n");
    
    // Test 4: Insert data
    const char *sql_insert = "INSERT INTO test (name) VALUES ('Hello'), ('World');";
    rc = sqlite3_exec(db, sql_insert, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(db);
        return 1;
    }
    printf("✓ Successfully inserted data\n");
    
    // Test 5: Query data
    sqlite3_stmt *stmt;
    const char *sql_select = "SELECT id, name FROM test;";
    rc = sqlite3_prepare_v2(db, sql_select, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }
    
    printf("✓ Query results:\n");
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        int id = sqlite3_column_int(stmt, 0);
        const char *name = (const char*)sqlite3_column_text(stmt, 1);
        printf("  ID: %d, Name: %s\n", id, name);
    }
    
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    
    printf("✓ All SQLite tests passed successfully!\n");
    return 0;
}