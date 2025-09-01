#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>

// Include SQLite internal headers - now available with source build
#include "sqliteInt.h"
#include "btreeInt.h"

int main() {
    printf("SQLite B-tree Internal API Test\n");
    printf("===============================\n");
    
    sqlite3 *db;
    int rc = sqlite3_open(":memory:", &db);
    if (rc != SQLITE_OK) {
        printf("❌ Failed to open database: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    printf("✓ SQLite database opened\n");
    
    // Get the main database B-tree
    Btree *pBt = db->aDb[0].pBt;
    if (!pBt) {
        printf("❌ Could not get B-tree handle\n");
        sqlite3_close(db);
        return 1;
    }
    printf("✓ B-tree handle obtained: %p\n", (void*)pBt);
    
    // Enter the database mutex for thread safety
    sqlite3_mutex_enter(sqlite3_db_mutex(db));
    
    // Begin a write transaction
    int pSchemaVersion;
    rc = sqlite3BtreeBeginTrans(pBt, 2, &pSchemaVersion); // TRANS_WRITE = 2
    if (rc != SQLITE_OK) {
        printf("❌ Failed to begin write transaction: %d\n", rc);
        sqlite3_close(db);
        return 1;
    }
    printf("✓ Write transaction started\n");
    
    // Create a new table (B-tree)
    int rootPage;
    rc = sqlite3BtreeCreateTable(pBt, &rootPage, BTREE_INTKEY);
    if (rc != SQLITE_OK) {
        printf("❌ Failed to create B-tree table: %d\n", rc);
        sqlite3BtreeRollback(pBt, SQLITE_OK, 0);
        sqlite3_close(db);
        return 1;
    }
    printf("✓ B-tree table created with root page: %d\n", rootPage);
    
    // Allocate cursor
    BtCursor *pCur = sqlite3MallocZero(sqlite3BtreeCursorSize());
    if (!pCur) {
        printf("❌ Failed to allocate cursor\n");
        sqlite3BtreeRollback(pBt, SQLITE_OK, 0);
        sqlite3_close(db);
        return 1;
    }
    
    // Open cursor on the new table
    rc = sqlite3BtreeCursor(pBt, rootPage, BTREE_WRCSR, 0, pCur);
    if (rc != SQLITE_OK) {
        printf("❌ Failed to open cursor: %d\n", rc);
        sqlite3_free(pCur);
        sqlite3BtreeRollback(pBt, SQLITE_OK, 0);
        sqlite3_close(db);
        return 1;
    }
    printf("✓ B-tree cursor opened\n");
    
    // Insert strings using B-tree API
    const char *strings[] = {"apple", "banana", "cherry", "date", "elderberry"};
    int num_strings = sizeof(strings) / sizeof(strings[0]);
    
    printf("\n--- Inserting into B-tree ---\n");
    for (int i = 0; i < num_strings; i++) {
        const char *str = strings[i];
        int len = strlen(str);
        i64 key = i + 1;  // Use integer key
        
        // Create BtreePayload structure
        BtreePayload payload;
        memset(&payload, 0, sizeof(payload));
        payload.nKey = key;          // For INTKEY table, this is the rowid
        payload.pData = str;         // Data content
        payload.nData = len;         // Data length
        
        rc = sqlite3BtreeInsert(pCur, &payload, 0, 0);
        if (rc != SQLITE_OK) {
            printf("❌ Failed to insert '%s': %d\n", str, rc);
        } else {
            printf("✓ Inserted B-tree entry: key=%lld, data='%s'\n", (long long)key, str);
        }
    }
    
    // Read back from B-tree
    printf("\n--- Reading from B-tree ---\n");
    int isEmpty;
    rc = sqlite3BtreeFirst(pCur, &isEmpty);
    if (rc != SQLITE_OK) {
        printf("❌ Failed to move to first: %d\n", rc);
    } else if (isEmpty) {
        printf("B-tree is empty\n");
    } else {
        do {
            // For INTKEY table, get the key directly
            i64 key = sqlite3BtreeIntegerKey(pCur);
            
            // Get payload size
            u32 payloadSize = sqlite3BtreePayloadSize(pCur);
            
            // Read data
            char dataBuf[256];
            if (payloadSize < sizeof(dataBuf)) {
                rc = sqlite3BtreePayload(pCur, 0, payloadSize, dataBuf);
                if (rc == SQLITE_OK) {
                    dataBuf[payloadSize] = '\0';
                    printf("→ B-tree entry: key=%lld, data='%s' (%u bytes)\n", 
                           (long long)key, dataBuf, payloadSize);
                }
            }
            
            // Move to next
            rc = sqlite3BtreeNext(pCur, 0);
            if (rc != SQLITE_OK) break;
            
        } while (!sqlite3BtreeEof(pCur));
    }
    
    // Search for specific key
    printf("\n--- B-tree key search ---\n");
    i64 searchKey = 3; // Look for "cherry"
    
    // Move to the key we're looking for
    UnpackedRecord *pUnpacked = 0;
    int found;
    rc = sqlite3BtreeTableMoveto(pCur, searchKey, 0, &found);
    if (rc == SQLITE_OK && found == 0) {
        // Key found exactly
        u32 payloadSize = sqlite3BtreePayloadSize(pCur);
        char dataBuf[256];
        if (payloadSize < sizeof(dataBuf)) {
            rc = sqlite3BtreePayload(pCur, 0, payloadSize, dataBuf);
            if (rc == SQLITE_OK) {
                dataBuf[payloadSize] = '\0';
                printf("✓ Found key %lld: '%s'\n", (long long)searchKey, dataBuf);
            }
        }
    } else {
        printf("Key %lld not found (rc=%d, found=%d)\n", (long long)searchKey, rc, found);
    }
    
    // Cleanup
    sqlite3BtreeCloseCursor(pCur);
    sqlite3_free(pCur);
    
    // Commit transaction
    rc = sqlite3BtreeCommit(pBt);
    if (rc != SQLITE_OK) {
        printf("❌ Failed to commit: %d\n", rc);
    } else {
        printf("✓ Transaction committed\n");
    }
    
    // Leave the database mutex
    sqlite3_mutex_leave(sqlite3_db_mutex(db));
    
    sqlite3_close(db);
    printf("✓ Direct B-tree API operations completed successfully!\n");
    
    return 0;
}