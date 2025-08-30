/*
** Test file to check SQLite core compilation
** This will help identify missing dependencies
*/
#include "sqlite.h"
#include "sqliteInt.h"
#include "btree.h"
#include "pager.h"

int main() {
    sqlite3 *db;
    // Just test basic compilation - don't actually run
    return 0;
}