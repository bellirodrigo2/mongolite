# SQLite B-tree API Guide

This guide documents how to use SQLite's internal B-tree API for direct data structure manipulation without SQL statements.

## Overview

SQLite's B-tree API provides low-level access to the underlying B-tree data structures that store all SQLite data. This allows for:

- Direct key-value operations
- Custom storage layouts
- Performance optimizations
- Learning B-tree internals

## Key Structures

### Btree
The main B-tree handle representing a database file:
```c
Btree *pBt = db->aDb[0].pBt;  // Get from database connection
```

### BtCursor
A cursor for navigating and modifying B-tree entries:
```c
BtCursor *pCur = sqlite3MallocZero(sqlite3BtreeCursorSize());
```

### BtreePayload
Structure for insert/update operations:
```c
typedef struct BtreePayload {
    const void *pKey;        // Key content for indexes (NULL for tables)
    sqlite3_int64 nKey;      // Key size for indexes, rowid for tables
    const void *pData;       // Data content
    sqlite3_value *aMem;     // Unpacked key values
    u16 nMem;               // Number of aMem values
    int nData;              // Data size
    int nZero;              // Extra zero bytes
} BtreePayload;
```

## Essential Functions

### Transaction Management

```c
// Begin write transaction
int pSchemaVersion;
int rc = sqlite3BtreeBeginTrans(pBt, 2, &pSchemaVersion);  // 2 = TRANS_WRITE

// Commit transaction
rc = sqlite3BtreeCommit(pBt);

// Rollback transaction
rc = sqlite3BtreeRollback(pBt, SQLITE_OK, 0);
```

### Table Operations

```c
// Create new B-tree table
int rootPage;
rc = sqlite3BtreeCreateTable(pBt, &rootPage, BTREE_INTKEY);
```

### Cursor Operations

```c
// Open cursor for writing
rc = sqlite3BtreeCursor(pBt, rootPage, BTREE_WRCSR, 0, pCur);

// Close cursor
sqlite3BtreeCloseCursor(pCur);
```

### Data Operations

```c
// Insert data
BtreePayload payload;
memset(&payload, 0, sizeof(payload));
payload.nKey = rowid;        // For INTKEY tables
payload.pData = data;        // Data pointer
payload.nData = dataLen;     // Data length

rc = sqlite3BtreeInsert(pCur, &payload, 0, 0);
```

### Navigation

```c
// Move to first entry
int isEmpty;
rc = sqlite3BtreeFirst(pCur, &isEmpty);

// Move to next entry
rc = sqlite3BtreeNext(pCur, 0);

// Check if at end
int isEof = sqlite3BtreeEof(pCur);

// Search for specific key
int found;
rc = sqlite3BtreeTableMoveto(pCur, searchKey, 0, &found);
```

### Data Reading

```c
// Get integer key (for INTKEY tables)
i64 key = sqlite3BtreeIntegerKey(pCur);

// Get payload size
u32 payloadSize = sqlite3BtreePayloadSize(pCur);

// Read payload data
char buffer[256];
rc = sqlite3BtreePayload(pCur, 0, payloadSize, buffer);
```

## Complete Example

Here's a complete example from our test file:

```c
#include <sqlite3.h>
#include "sqliteInt.h"
#include "btreeInt.h"

int main() {
    sqlite3 *db;
    int rc = sqlite3_open(":memory:", &db);
    
    // Get B-tree handle
    Btree *pBt = db->aDb[0].pBt;
    
    // Enter mutex for thread safety
    sqlite3_mutex_enter(sqlite3_db_mutex(db));
    
    // Begin write transaction
    int pSchemaVersion;
    rc = sqlite3BtreeBeginTrans(pBt, 2, &pSchemaVersion);
    
    // Create table
    int rootPage;
    rc = sqlite3BtreeCreateTable(pBt, &rootPage, BTREE_INTKEY);
    
    // Open cursor
    BtCursor *pCur = sqlite3MallocZero(sqlite3BtreeCursorSize());
    rc = sqlite3BtreeCursor(pBt, rootPage, BTREE_WRCSR, 0, pCur);
    
    // Insert data
    const char *data = "Hello, B-tree!";
    BtreePayload payload;
    memset(&payload, 0, sizeof(payload));
    payload.nKey = 1;                    // Row ID
    payload.pData = data;                // Data
    payload.nData = strlen(data);        // Data length
    
    rc = sqlite3BtreeInsert(pCur, &payload, 0, 0);
    
    // Read back
    int isEmpty;
    rc = sqlite3BtreeFirst(pCur, &isEmpty);
    if (!isEmpty) {
        i64 key = sqlite3BtreeIntegerKey(pCur);
        u32 size = sqlite3BtreePayloadSize(pCur);
        
        char buffer[256];
        rc = sqlite3BtreePayload(pCur, 0, size, buffer);
        buffer[size] = '\0';
        
        printf("Key: %lld, Data: %s\n", key, buffer);
    }
    
    // Cleanup
    sqlite3BtreeCloseCursor(pCur);
    sqlite3_free(pCur);
    sqlite3BtreeCommit(pBt);
    sqlite3_mutex_leave(sqlite3_db_mutex(db));
    sqlite3_close(db);
    
    return 0;
}
```

## Key Constants

### Table Types
- `BTREE_INTKEY` - Table with integer primary key (rowid)
- `BTREE_BLOBKEY` - Index table with blob keys

### Cursor Flags
- `BTREE_WRCSR` - Write cursor
- `BTREE_FORDELETE` - Cursor for deletion

### Insert Flags
- `BTREE_SAVEPOSITION` - Save cursor position
- `BTREE_APPEND` - Hint that insert is likely an append

## Thread Safety

Always use proper mutex handling:

```c
// Enter database mutex before B-tree operations
sqlite3_mutex_enter(sqlite3_db_mutex(db));

// ... B-tree operations ...

// Leave mutex when done
sqlite3_mutex_leave(sqlite3_db_mutex(db));
```

## Error Handling

All B-tree functions return SQLite result codes:
- `SQLITE_OK` (0) - Success
- `SQLITE_ERROR` - General error
- `SQLITE_CORRUPT` - Database corruption
- `SQLITE_NOMEM` - Out of memory
- `SQLITE_BUSY` - Database locked

Always check return codes:

```c
int rc = sqlite3BtreeInsert(pCur, &payload, 0, 0);
if (rc != SQLITE_OK) {
    fprintf(stderr, "Insert failed: %d\n", rc);
    // Handle error...
}
```

## Performance Notes

1. **Batch Operations**: Use transactions for multiple operations
2. **Cursor Reuse**: Reuse cursors when possible
3. **Memory Management**: Always free allocated cursors
4. **Page Cache**: B-tree operations benefit from larger page cache

## Limitations

1. **Internal API**: Subject to change between SQLite versions
2. **No SQL Features**: No triggers, constraints, or SQL functions
3. **Manual Schema**: Must manage table structure manually
4. **Thread Safety**: Requires explicit mutex management

## References

- SQLite source code: `src/btree.c`, `src/btreeInt.h`
- Our test implementation: `tests/test_btree.c`
- Build documentation: `sqlite-source-build.md`