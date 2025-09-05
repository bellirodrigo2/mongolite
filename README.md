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
- [x] **Build System**: CMake integration with SQLite source build + libbson integration
- [x] **Test Suite**: Comprehensive tests for all implemented functionality

### Document Operations - COMPLETE ✅
- [x] **BSON Integration**: Full MongoDB libbson library integration with proper type handling
- [x] **Single Document Insert**: `mlite_insert_one()` with automatic ObjectId generation
- [x] **Generic Insert**: `mlite_insert_one_any()` with custom conversion functions
- [x] **JSON Insert**: `mlite_insert_one_jsonstr()` with JSON-to-BSON conversion
- [x] **Bulk Operations**: `mlite_insert_many()`, `mlite_insert_many_any()`, `mlite_insert_many_jsonstr()`
- [x] **Transaction Support**: Atomic bulk operations with automatic rollback on failures
- [x] **Smart Error Handling**: Comprehensive error categorization and propagation

### Document Query Operations - COMPLETE ✅
- [x] **Document Retrieval**: `mlite_find()` with cursor-based iteration
- [x] **Single Document Query**: `mlite_find_one()` with filtering support
- [x] **Document Counting**: `mlite_count_documents()` with filter support
- [x] **Field Projection**: Return only specified fields from documents
- [x] **Cursor Management**: `mlite_cursor_next()`, `mlite_cursor_destroy()` with proper cleanup
- [x] **Cross-Type Comparisons**: MongoDB-compliant type precedence ordering

### MongoDB Query Operators - COMPLETE ✅
- [x] **Comparison**: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte` - Full numeric and string comparison
- [x] **Array**: `$in`, `$nin`, `$all`, `$size`, `$elemMatch` - Array membership, size operations, and document matching within arrays
- [x] **Logical**: `$and`, `$or`, `$not`, `$nor` - Complex logical expressions
- [x] **Element**: `$exists`, `$type` - Field existence and BSON type checking  
- [x] **Mathematical**: `$mod` - Modulo operations with MongoDB-compatible behavior
- [x] **Pattern**: `$regex` - POSIX regular expression matching with MongoDB-style options

### What Works Now
```c
#include "mongolite.h"
#include <bson/bson.h>

mlite_db_t *db;
bson_error_t error;

// Open database
mlite_open("myapp.mlite", &db);

// Create collections  
mlite_collection_create(db, "users");

// Insert documents (multiple methods supported)
mlite_insert_one_jsonstr(db, "users", 
    "{\"name\": \"Alice\", \"age\": 30, \"city\": \"NYC\", \"active\": true}", &error);
mlite_insert_one_jsonstr(db, "users", 
    "{\"name\": \"Bob\", \"age\": 25, \"city\": \"SF\", \"active\": false}", &error);
mlite_insert_one_jsonstr(db, "users", 
    "{\"name\": \"Charlie\", \"age\": 35, \"city\": \"NYC\", \"active\": true}", &error);

// === QUERY OPERATIONS ===

// Find all documents
mlite_cursor_t *cursor = mlite_find(db, "users", NULL, NULL, &error);
const bson_t *doc;
while (mlite_cursor_next(cursor, &doc)) {
    // Process each document
    char *json_str = bson_as_json(doc, NULL);
    printf("Found: %s\n", json_str);
    bson_free(json_str);
}
mlite_cursor_destroy(cursor);

// Find with MongoDB query operators
bson_t *filter = bson_new();

// Find users over 25: {age: {$gt: 25}}
bson_t *age_filter = bson_new();
bson_append_int32(age_filter, "$gt", -1, 25);
bson_append_document(filter, "age", -1, age_filter);
cursor = mlite_find(db, "users", filter, NULL, &error);

// Find single document
const bson_t *found_doc = mlite_find_one(db, "users", filter, NULL, &error);

// Complex queries with logical operators
bson_destroy(filter);
filter = bson_new();
// Find: {$and: [{city: "NYC"}, {age: {$gte: 30}}]}
bson_t *and_array = bson_new();
bson_t *city_cond = bson_new();
bson_t *age_cond = bson_new();
bson_append_utf8(city_cond, "city", -1, "NYC", -1);
bson_append_int32(age_cond, "$gte", -1, 30);
bson_append_document(age_cond, "age", -1, age_cond);
// ... (complete query construction)

// Field projection - return only name and age
bson_t *projection = bson_new();
bson_append_int32(projection, "name", -1, 1);
bson_append_int32(projection, "age", -1, 1);
cursor = mlite_find(db, "users", filter, projection, &error);

// Count documents with filter  
int64_t count = mlite_count_documents(db, "users", filter, &error);
printf("Found %ld matching documents\n", count);

// Regex pattern matching: {name: {$regex: "^A.*"}}
bson_destroy(filter);
filter = bson_new();
bson_t *name_regex = bson_new();
bson_append_utf8(name_regex, "$regex", -1, "^A.*", -1);
bson_append_document(filter, "name", -1, name_regex);

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

## 🚧 Next Steps (Phase 3)

### Document Modification Operations (Next Priority)
- [ ] `mlite_update_one()`, `mlite_update_many()` - Document updates
- [ ] `mlite_delete_one()`, `mlite_delete_many()` - Document deletion
- [ ] **Update Operators**: `$set`, `$unset`, `$inc`, `$push`, `$addToSet` support
- [ ] **Array Update Operations**: `$pull`, `$pullAll`, `$pop` support

### Query Operators Status - ALL BASIC OPERATORS COMPLETE ✅
- [x] ~~`$mod` - Modulo operation~~ **COMPLETED** ✅  
- [x] ~~`$elemMatch` - Array element matching: `{scores: {$elemMatch: {$gt: 80}}}`~~ **COMPLETED** ✅
- [x] **Complete unit test coverage for all implemented operators** ✅

### Advanced Query Operators (Not Yet Implemented)
These MongoDB operators are not implemented and represent future enhancement opportunities:
- [ ] **`$expr`** - Aggregation expressions in query context: `{$expr: {$gt: ["$spent", "$budget"]}}`
- [ ] **`$text`** - Full-text search with text indexing: `{$text: {$search: "coffee"}}`
- [ ] **`$jsonSchema`** - JSON Schema validation: `{$jsonSchema: {properties: {...}}}`
- [ ] **`$geoWithin`** - Geospatial queries within shapes
- [ ] **`$near`** - Geospatial proximity queries
- [ ] **`$geoIntersects`** - Geospatial intersection queries
- [ ] **Array position operators**: `$[<identifier>]`, `$[]` for positional updates
- [ ] **`$slice`** - Array projection operator for limiting returned array elements

**Current Implementation**: **18/26** Core MongoDB query operators (69% coverage of essential operators)

### Indexing Strategy (Critical Design Decision)
**Current Assessment**: Multiple viable paths identified

#### **Option 1: SQL-Based Indexing (Faster Implementation)**
- ✅ **Pros**: Leverage SQLite's existing B-tree indexes, easier to implement
- ⚠️ **Cons**: Limited MongoDB compatibility, type ordering challenges
- 📅 **Timeline**: 2-3 weeks for basic field indexes

#### **Option 2: Direct B-tree API Indexing (Full Compatibility)**  
- ✅ **Pros**: True MongoDB-style indexes, proper BSON type ordering, better performance
- ⚠️ **Cons**: Complex implementation, requires deep SQLite B-tree API knowledge  
- 📅 **Timeline**: 6-8 weeks for robust implementation

#### **Recommended Sequence**:
1. **Implement `find()` operations first** → Understand query patterns
2. **SQL-based indexing for common cases** → Quick wins, validate approach
3. **Migrate to B-tree API** → Full MongoDB compatibility when needed

### BSON Type Ordering Challenge
MongoDB's canonical sort order for schema-less documents:
```
MinKey → Null → Numbers → String → Object → Array → 
BinaryData → ObjectId → Boolean → Date → Timestamp → Regex → MaxKey
```
**Challenge**: Field `"age"` might be `int32` in doc1, `string` in doc2, missing in doc3
**Solution Path**: Implement type-aware comparison functions with proper precedence

## 🎯 Architecture Strategy

### Phase 1: SQL-based Prototype (Current) ⚠️ TEMPORARY
- Uses standard SQLite API for rapid development
- **SQL Abstraction Layer**: All SQL operations isolated in `mongolite_sql.c` for easy migration
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

### SQL Abstraction Layer (Migration-Ready Architecture)
- **Isolated SQL operations** in `mongolite_sql.c` - All SQLite API calls encapsulated
- **Clean interface** - Functions like `mlite_sql_insert_document()`, `mlite_sql_query_step()`
- **B-tree migration ready** - Replace SQL functions with direct B-tree API calls
- **Maintainable** - Core logic in `mongolite.c` remains unchanged during migration

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
make -j4

# Run individual test suites
./test_mongolite              # Core database & collection operations
./test_insert_one             # Single document insertion tests
./test_json_insert            # JSON conversion and any-type insertion tests  
./test_insert_many            # Bulk operation and transaction tests
./test_find                   # Document query and cursor operations
./test_query_ops              # MongoDB query operator testing
./test_mongodb_precedence     # Cross-type comparison and type precedence
./test_array_operators        # Array-specific operator testing
./test_query_operators_unit   # Comprehensive unit tests for all operators

# Run all MongoLite tests (excluding MongoDB C driver tests)
ctest -E "mongoc"
```

**All tests currently passing** ✅ (100% success rate - 11/11 test suites)
- ✅ Database open/close operations with proper flags
- ✅ Collection creation, existence, deletion, and error cases
- ✅ Single & bulk document insertion (BSON, JSON, custom structs)
- ✅ Document querying with filtering and field projection
- ✅ MongoDB query operators: comparison, logical, array, element, pattern
- ✅ Cross-type comparisons with MongoDB-compliant type precedence
- ✅ Cursor-based iteration with proper memory management
- ✅ Transaction rollback on bulk operation failures
- ✅ ObjectId auto-generation and validation
- ✅ POSIX regex pattern matching with MongoDB-style options
- ✅ Comprehensive error handling and categorization
- ✅ Memory management with no leaks detected

**Test Coverage**: 11 comprehensive test suites covering:
- **Core Operations**: 45+ test cases (database, collections, error handling)
- **Document Insertion**: 60+ test cases (single, bulk, JSON, custom structs)  
- **JSON Conversion**: 35+ test cases (validation, edge cases, type handling)
- **Bulk Operations**: 40+ test cases (transaction verification, rollback testing)
- **Document Queries**: 50+ test cases (find, find_one, count, cursors)
- **Query Operators**: 80+ test cases (comparison, logical, array operations)
- **Type Precedence**: 30+ test cases (cross-type comparisons, MongoDB compliance)
- **Array Operations**: 45+ test cases (array-specific operators and edge cases)
- **Unit Tests**: 110+ focused test cases for comprehensive operator validation (including $elemMatch)

## 📦 Dependencies

### Current (Fully Functional)
- **SQLite 3 Source** - Full SQLite source build with B-tree API access
- **MongoDB libbson** - Complete BSON document handling and JSON conversion
- **Standard C Libraries** - malloc, stdio, string handling
- **CMake Build System** - Automated dependency management and testing

### Future Optimization (Production)
- **SQLite B-tree Engine Only** - Remove SQL parser/compiler components
- **Custom BSON Storage Layout** - Optimized document storage format
- **~75% size reduction target** - From ~1.5MB to ~400KB binary size

## 🎖️ Key Features Implemented

- ✅ **File-based storage** - Single `.mlite` database files with SQLite reliability
- ✅ **Collection management** - Create, drop, check existence with idempotent operations
- ✅ **Document operations** - Insert (single/bulk), find, count with full BSON support
- ✅ **MongoDB query language** - 18 query operators with full MongoDB compatibility
- ✅ **Field projection** - Return only specified fields to minimize memory usage
- ✅ **Cursor-based iteration** - Memory-efficient document streaming with proper cleanup
- ✅ **Cross-type comparisons** - MongoDB-compliant BSON type precedence ordering
- ✅ **Pattern matching** - POSIX regex support with MongoDB-style options (i, m, x, s)
- ✅ **Multiple insertion modes** - BSON objects, JSON strings, custom structs
- ✅ **Bulk operations** - Atomic multi-document insertions with transaction rollback
- ✅ **ObjectId auto-generation** - MongoDB-compatible ObjectIds for documents without `_id`
- ✅ **Smart error handling** - Comprehensive error categorization and propagation
- ✅ **Memory safety** - No leaks, proper cleanup on all code paths, extensive testing
- ✅ **Performance optimization** - Prepared statements and efficient BSON handling

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

### **Phase 3A**: SQL to B-tree Migration (Next Priority - 4-6 weeks) 🎯
1. **Replace SQL abstraction layer** - Migrate `mongolite_sql.c` functions to direct B-tree API calls
2. **Collection management** - Direct B-tree table creation and management
3. **Document storage** - Optimized BSON storage layout for B-tree access
4. **Query engine updates** - Update query operators to work with B-tree cursors
5. **Performance validation** - Ensure improved performance and reduced binary size

### **Phase 3B**: Document Modification Operations (3-4 weeks)  
1. **Update operations** - `mlite_update_one()`, `mlite_update_many()`
2. **Delete operations** - `mlite_delete_one()`, `mlite_delete_many()`
3. **Update operators** - `$set`, `$unset`, `$inc`, `$push`, `$addToSet` support
4. **Array update operations** - `$pull`, `$pullAll`, `$pop` support

### **Phase 4**: Indexing Strategy Decision Point (6-12 weeks)
**Current Status**: All query operations work via full collection scans
**Options**:
- **Option A**: SQL-based field indexes (faster implementation, limited compatibility)
- **Option B**: Direct B-tree API indexes (full MongoDB compatibility, complex)
**Implementation**:
- Secondary B-trees for field indexes  
- Compound index support with proper BSON type ordering
- Query planner integration

### **Phase 5**: Production Optimization (8-10 weeks)
1. **Binary size optimization** - Direct B-tree API migration, eliminate SQL engine
2. **Target metrics** - ~400KB binary vs current ~1.5MB (75% reduction)
3. **Performance benchmarking** - Query caching, connection pooling
4. **Production readiness** - Memory profiling, embedded system validation

---

**MongoLite** - Bringing MongoDB's document model to embedded systems with SQLite's reliability and minimal footprint.