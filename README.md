# MongoLite 

A MongoDB-like document database using SQLite's storage engine, designed for embedded systems and applications requiring minimal footprint.

## 🎯 Project Goals

- **MongoDB-Compatible API**: Familiar document operations without MongoDB server overhead
- **SQLite B-tree Storage**: Leverage SQLite's reliability WITHOUT SQL compilation  
- **Embedded-First**: Optimized for IoT, mobile, and resource-constrained environments
- **Single File Database**: No server, no configuration, just a `.mlite` file
- **Minimal Binary Size**: Target 75% smaller than full SQLite (200-400KB vs 1.5MB+)

## ✅ Current Progress (Phase 1)

### Core Infrastructure - COMPLETE ✅
- [x] **Database Operations**: `mlite_open()`, `mlite_close()`, `mlite_open_v2()`
- [x] **Collection Management**: `mlite_collection_create()`, `mlite_collection_drop()`, `mlite_collection_exists()`
- [x] **Error Handling**: `mlite_errmsg()`, `mlite_errcode()` with proper memory management
- [x] **Build System**: CMake integration with SQLite source build
- [x] **Test Suite**: Comprehensive tests for all implemented functionality

### What Works Now
```c
// Open database
mlite_db_t *db;
mlite_open("myapp.mlite", &db);

// Create collections  
mlite_collection_create(db, "users");
mlite_collection_create(db, "products");

// Check if collection exists
if (mlite_collection_exists(db, "users")) {
    printf("Users collection ready!\n");
}

// Drop collections
mlite_collection_drop(db, "products");

// Clean close
mlite_close(db);
```

### Database Schema (Phase 1)
```sql
-- Each collection becomes a table
CREATE TABLE collection_users (
    _id TEXT PRIMARY KEY,
    document BLOB NOT NULL
);

-- Metadata tracking
CREATE TABLE _mlite_collections (
    name TEXT PRIMARY KEY,
    created_at INTEGER
);
```

## 🚧 Next Steps (Phase 2)

### Document Operations (Next Priority)
- [ ] `mlite_insert_one()` - Insert single BSON document
- [ ] `mlite_insert_many()` - Batch document insertion  
- [ ] `mlite_find()` - Query with cursor-based iteration
- [ ] `mlite_update_one()`, `mlite_delete_one()` - Basic CRUD operations

### BSON Integration
- [ ] Integrate MongoDB's libbson library
- [ ] Automatic ObjectId generation for documents without `_id`
- [ ] BSON document validation and serialization

## 🎯 Architecture Strategy

### Phase 1: SQL-based Prototype (Current) ⚠️ TEMPORARY
- Uses standard SQLite API for rapid development
- SQL statements for collection management
- Foundation for Phase 2 migration

### Phase 2: B-tree Direct Access (Production Target) 🎯
- **Eliminate SQL engine entirely** - Direct `sqlite3Btree*()` API usage
- **75% binary size reduction** - No SQL parser/compiler/tokenizer
- **Better performance** - No SQL compilation overhead
- **Custom storage layouts** - Optimized for BSON document access

### Phase 3: Advanced Features
- Secondary B-tree indexes for field queries
- MongoDB-style compound indexes
- Query optimization and caching

## 🏗️ Technical Implementation

### Memory Management
- **Standard malloc/free** - No conflicts with SQLite/libbson internal allocators
- **Clean separation** - Each library manages its own memory
- **Error-safe cleanup** - Proper resource deallocation on failures

### Storage Design
```
Collections → SQLite Tables (Phase 1) → Direct B-trees (Phase 2)
Documents → BLOB storage → Optimized BSON layout
Indexes → SQLite indexes → Secondary B-trees
```

## 🧪 Testing

```bash
# Build and test
mkdir build && cd build
cmake ..
make test_mongolite
./test_mongolite

# Run all tests
ctest -R mongolite_test
```

**All tests currently passing** ✅
- Database open/close operations
- Collection creation, existence, and deletion
- Error handling and edge cases
- Memory management and cleanup

## 📦 Dependencies

### Current (Development)
- **SQLite 3 Source** - Full SQLite for prototyping (temporary)
- **Standard C Libraries** - malloc, stdio, string handling

### Future (Production)
- **SQLite B-tree Engine Only** - Minimal SQLite components
- **MongoDB libbson** - BSON document handling
- **~75% size reduction** from eliminating SQL engine

## 🎖️ Key Features Implemented

- ✅ **File-based storage** - Single `.mlite` database files
- ✅ **Collection management** - Create, drop, check existence
- ✅ **Idempotent operations** - Safe to call create/drop multiple times
- ✅ **Error handling** - Proper error codes and messages
- ✅ **Memory safety** - No leaks, proper cleanup
- ✅ **SQLite integration** - Leverages proven storage engine

## 📋 Usage Example (Current)

```c
#include "mongolite.h"

int main() {
    mlite_db_t *db;
    
    // Open database file
    if (mlite_open("app.mlite", &db) != 0) {
        printf("Failed to open database\n");
        return 1;
    }
    
    // Create user collection
    if (mlite_collection_create(db, "users") != 0) {
        printf("Error: %s\n", mlite_errmsg(db));
        mlite_close(db);
        return 1;
    }
    
    // Verify collection exists
    if (mlite_collection_exists(db, "users")) {
        printf("Users collection ready for documents!\n");
    }
    
    // Close database
    mlite_close(db);
    return 0;
}
```

## 🔮 Roadmap

1. **Phase 2A**: BSON document operations (insert, find, update, delete)
2. **Phase 2B**: Query filtering and cursor-based iteration  
3. **Phase 3A**: Migration to direct B-tree API (eliminate SQL)
4. **Phase 3B**: Field indexing and query optimization
5. **Phase 4**: Performance benchmarking and embedded optimization

---

**MongoLite** - Bringing MongoDB's document model to embedded systems with SQLite's reliability and minimal footprint.