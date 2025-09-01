# MongoLite Project Overview

## What We Will Accomplish

MongoLite is a MongoDB-like document database that uses SQLite as its storage backend. It provides a familiar MongoDB API for document operations while leveraging SQLite's proven reliability, ACID compliance, and file-based architecture.

### Key Goals

1. **MongoDB-Compatible API**: Provide a subset of MongoDB's most commonly used operations
2. **SQLite B-tree Storage**: Use SQLite's B-tree engine WITHOUT SQL compilation
3. **File-Based Architecture**: Single file database like SQLite, no server required
4. **BSON Document Support**: Native BSON document handling using MongoDB's libbson
5. **Embedded Usage**: Optimized for embedded systems with minimal footprint
6. **No SQL Engine**: Eliminate SQL parser/compiler to reduce binary size and improve performance

### Target Use Cases

- **Embedded Systems**: Document storage without SQL engine overhead (smaller binary size)
- **IoT Devices**: MongoDB-style operations with minimal memory footprint
- **Mobile Applications**: Local document storage with familiar MongoDB API
- **Performance-Critical Apps**: Avoid SQL compilation overhead, direct B-tree access
- **Resource-Constrained Environments**: Full SQL engine too heavy, need document storage

## Technical Approach

### Architecture Overview

```
Phase 1: Simple Approach
Application → MongoLite API → BSON Processing → SQLite SQL API → Database File

Phase 2+: Optimized Approach  
Application → MongoLite API → BSON Processing → SQLite B-tree API → Database File
```

### Storage Strategy (Phased Approach)

#### Phase 1: SQL-based Prototype (Development Only)
**TEMPORARY** approach using SQL for rapid prototyping:
```sql
-- Development-only table structure
CREATE TABLE collection_<name> (
    _id TEXT PRIMARY KEY,
    document BLOB NOT NULL
);
```
⚠️ **Important**: This SQL usage is ONLY for initial development. The end goal is to eliminate SQL entirely.

#### Phase 2: Direct B-tree Implementation (Production Target)
**PRIMARY GOAL** - Replace all SQL with direct B-tree operations:
- **Collections as B-trees**: Each collection = separate B-tree structure
- **No SQL Compilation**: Use only `sqlite3Btree*()` functions
- **Custom Key Encoding**: Optimized _id storage without SQL overhead
- **Binary Size Reduction**: Eliminate SQL parser, tokenizer, and compiler

#### Phase 3: Advanced B-tree Features (Performance Optimization)
- **Secondary B-trees**: Field indexes using separate B-tree structures
- **Compound Indexes**: Multi-field indexes like MongoDB
- **BSON-optimized Storage**: Custom layouts to offset BSON parsing costs

### Key Technical Components

#### 1. Database Management (Phase 1)
- **SQL Operations**: Use standard SQLite API for table operations
- **Collection Management**: CREATE/DROP TABLE for collections
- **Simple Schema**: TEXT _id + BLOB document storage

#### 2. Document Processing
- **BSON Integration**: Use MongoDB's libbson for document parsing/serialization  
- **ID Generation**: Automatic ObjectId generation for documents without `_id`
- **Blob Storage**: Store complete BSON documents as SQLite BLOBs

#### 3. Query Engine (Phase 1)
- **SQL-based Queries**: Use SELECT statements for _id lookups
- **In-memory Filtering**: Parse BSON and filter documents in application layer
- **Cursor Implementation**: SQLite result sets wrapped as MongoLite cursors

#### 4. CRUD Operations (Phase 1)
- **Insert**: `INSERT INTO collection_name (_id, document) VALUES (?, ?)`
- **Update**: Retrieve document, modify BSON in memory, UPDATE blob
- **Delete**: `DELETE FROM collection_name WHERE _id = ?`  
- **Find**: `SELECT * FROM collection_name` + in-memory BSON filtering

#### 5. Future Optimization (Phase 2+)
- **B-tree Direct Access**: Bypass SQL for performance-critical operations
- **Field Indexes**: Extract BSON fields into separate B-tree structures
- **Custom Storage**: Optimized document layout and key encoding

### Implementation Phases

#### Phase 1: Core SQL-based Infrastructure
- [x] API design and header file
- [ ] Database open/close using standard SQLite API
- [ ] Collection table creation (`CREATE TABLE collection_name`)
- [ ] Basic insert operations with SQL (`INSERT INTO`)

#### Phase 2: Basic CRUD Operations  
- [ ] Document insertion with SQL prepared statements
- [ ] _id-based lookups with `SELECT WHERE _id = ?`
- [ ] Update operations (fetch BSON, modify, UPDATE)
- [ ] Delete operations with `DELETE WHERE _id = ?`

#### Phase 3: Query and Indexing
- [ ] Find operations with in-memory BSON filtering
- [ ] Cursor implementation wrapping SQLite result sets  
- [ ] Count operations with `SELECT COUNT(*)`
- [ ] Basic field indexing exploration

#### Phase 4: B-tree Optimization Migration
- [ ] Benchmark SQL vs B-tree approaches
- [ ] Migrate hot paths to direct B-tree API
- [ ] Implement field-based secondary indexes
- [ ] Advanced query optimization

### Technical Challenges

#### Phase 1 Challenges
1. **BSON-SQL Integration**: Efficiently storing/retrieving BSON as SQLite BLOBs
2. **ID Management**: Handling MongoDB ObjectId vs SQLite TEXT PRIMARY KEY  
3. **Query Translation**: Converting MongoDB queries to in-memory BSON filtering
4. **Performance**: Ensuring acceptable performance with SQL + in-memory filtering

#### Production Phase Challenges (Critical for Embedded Systems)
5. **SQL Engine Elimination**: Complete removal of SQL parser/compiler from final binary
6. **Binary Size Optimization**: Minimize footprint for embedded/IoT deployment  
7. **Performance vs Size Trade-off**: BSON parsing overhead vs SQL compilation elimination
8. **B-tree Direct Access**: Implementing all operations without SQL layer
9. **Field Index Design**: MongoDB-style compound indexes using only B-tree operations
10. **Memory Efficiency**: Optimal BSON handling to offset parsing costs in constrained environments

### Dependencies

#### Production Build (Final Target)
- **SQLite 3 B-tree Engine ONLY**: Internal B-tree API without SQL engine compilation
- **libbson**: MongoDB's BSON library for document handling  
- **Standard C Libraries**: For system operations and memory management

#### Development Build (Temporary)
- **SQLite 3 Source**: Full SQLite for rapid prototyping (will be eliminated)

### Binary Size Impact
- **With SQL Engine**: ~1.5MB+ (full SQLite)
- **B-tree Only Target**: ~200-400KB (estimated, B-tree + pager + OS interface only)
- **Size Reduction**: ~75% smaller binary for embedded deployment

### Key SQLite APIs Used

#### Phase 1: Standard SQL API (Development Only - WILL BE REMOVED)
⚠️ **TEMPORARY USAGE** - These will be eliminated in production:
- `sqlite3_open()` / `sqlite3_close()` - Database management
- `sqlite3_exec()` - DDL operations (CREATE TABLE, etc.)  
- `sqlite3_prepare_v2()` / `sqlite3_step()` - Prepared statements
- `sqlite3_bind_text()` / `sqlite3_bind_blob()` - Parameter binding

#### Production Target: B-tree API ONLY (Final Implementation)
🎯 **PRODUCTION IMPLEMENTATION** - Only these APIs in final binary:
- `sqlite3BtreeOpen()` / `sqlite3BtreeClose()` - Database file management
- `sqlite3BtreeBeginTrans()` / `sqlite3BtreeCommit()` - Transaction control
- `sqlite3BtreeCreateTable()` - B-tree creation (collections)
- `sqlite3BtreeCursor()` - Cursor creation for navigation
- `sqlite3BtreeInsert()` / `sqlite3BtreeDelete()` - Document operations
- `sqlite3BtreeMoveto()` / `sqlite3BtreeNext()` - Key seeking and iteration

**Result**: No SQL parser, no compiler, no tokenizer = Much smaller binary + Better performance

### File Structure

```
mongolite/
├── src/
│   ├── mongolite.h          # Public API
│   ├── mongolite.c          # Implementation
│   └── internal/            # Internal headers
├── deps/
│   ├── sqlite-src/          # SQLite source
│   └── mongo-c-driver/      # MongoDB C driver (for libbson)
├── tests/                   # Test suite
├── docs/
│   └── dev/                 # Development documentation
└── CMakeLists.txt          # Build configuration
```

This approach gives us the best of both worlds: MongoDB's intuitive document API with SQLite's reliability and simplicity.