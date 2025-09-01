#include <stdio.h>
#include <sqlite3.h>
#include <assert.h>

int main() {
    printf("Running basic SQLite tests...\n");
    
    sqlite3 *db;
    int rc = sqlite3_open(":memory:", &db);
    assert(rc == SQLITE_OK);
    printf("✓ SQLite database opened successfully\n");
    
    const char *sql = "CREATE TABLE test(id INTEGER, name TEXT);";
    rc = sqlite3_exec(db, sql, NULL, NULL, NULL);
    assert(rc == SQLITE_OK);
    printf("✓ Table created successfully\n");
    
    sql = "INSERT INTO test VALUES(1, 'Hello World');";
    rc = sqlite3_exec(db, sql, NULL, NULL, NULL);
    assert(rc == SQLITE_OK);
    printf("✓ Data inserted successfully\n");
    
    sqlite3_close(db);
    printf("✓ Database closed successfully\n");
    printf("All tests passed!\n");
    return 0;
}