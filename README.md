# MongoLite

A MongoDB-compatible document database built on SQLite for embedded and lightweight applications.

## Overview

MongoLite provides MongoDB-style document storage and querying capabilities using SQLite as the storage engine. It offers:

- **MongoDB-compatible API** for familiar development experience
- **SQLite reliability** with ACID transactions and proven stability  
- **Single-file deployment** with no external dependencies
- **Embedded-friendly** design for applications and IoT devices

## Project Status

🟢 **SQLite Integration**: ✅ Complete (v3.46.1)  
🟢 **BSON Support**: ✅ Complete (Header-only interface + Standalone implementation)  
🟢 **MongoDB API Layer**: ✅ Core Complete (insert_one, find_one, collections)  
🟢 **Document Storage**: ✅ Complete (BSON docs in SQLite BLOBs)  
🟡 **Query Translation**: 🔧 Next Phase (filtering, indexing)  
🟡 **Production libbson**: 🔧 Next Phase (replace standalone BSON)  

## Quick Start

### Build

```bash
cd src
make all
```

### Test

```bash
make test
# Runs: SQLite backend, BSON interface, MongoLite integration
```

### Example Usage

```c
#include "mongolite/mongolite_standalone.h"

// Open database
mongolite_result_t result;
mongolite_db_t *db = mongolite_open("myapp.db", &result);

// Get collection  
mongolite_collection_t *users = mongolite_get_collection(db, "users", &result);

// Create and insert document
mongolite_bson_t *doc = mongolite_bson_new_from_data(bson_data, data_len);
mongolite_insert_one(users, doc, &result);

// Find document
mongolite_bson_t *found = mongolite_find_one(users, NULL, &result);

// Cleanup
mongolite_bson_destroy(doc);
mongolite_bson_destroy(found);
mongolite_close(db);
```

### SQLite Shell

```bash
cd src/sqlite
./sqlite3
```

## Project Structure

```
mongolite/
├── src/                        # Source code
│   ├── sqlite/                # SQLite 3.46.1 amalgamation ✅
│   ├── mongolite/             # MongoLite core implementation ✅  
│   │   ├── mongolite_standalone.h/.c  # Working document database ✅
│   │   ├── mongolite_bson.h          # Header-only BSON interface ✅
│   │   └── mongolite.h/.c            # Full libbson integration (ready)
│   ├── tests/                 # Test programs ✅
│   │   ├── test_sqlite.c             # SQLite backend tests ✅
│   │   ├── test_bson_minimal.c       # BSON interface tests ✅  
│   │   └── test_mongolite_integration.c # Full integration tests ✅
│   ├── examples/              # Example applications (planned)
│   └── Makefile               # Build system ✅
├── docs/                      # Documentation
├── BSON_STRATEGY.md          # BSON implementation strategy ✅
├── LIBBSON_ANALYSIS.md       # libbson size analysis ✅  
├── PHASE3_COMPLETE.md        # Current completion status ✅
└── README.md                 # This file
```

## Architecture

```
┌─────────────────┐
│  MongoDB API    │  ← insert_one, find_one, collections ✅
├─────────────────┤
│  BSON Interface │  ← Header-only + standalone BSON ✅ 
├─────────────────┤
│  Document Store │  ← BSON docs as SQLite BLOBs ✅
├─────────────────┤
│  Query Engine   │  ← Basic ops ✅, filtering (next phase)
├─────────────────┤
│  SQLite 3.46.1  │  ← ACID storage engine ✅
└─────────────────┘
```

**Current State**: Functional document database with MongoDB-compatible API

## Development

### Current Status ✅ PHASE 3 COMPLETE 
1. ✅ SQLite integration and build system
2. ✅ BSON document storage layer (standalone + header-only interface)
3. ✅ MongoDB query API implementation (core operations)
4. ✅ Document database working end-to-end

### Next Phase Options
1. **Production libbson Integration**: Replace standalone BSON with full libbson
2. **Advanced Queries**: Filtering, sorting, indexing, aggregation  
3. **Performance**: Bulk operations, connection pooling, optimization
4. **Extended API**: Update operations, transactions, schema validation

### Contributing
This project is in early development. See the `/src` directory for current implementation status.

## License

[To be determined]

---

**Built with SQLite 3.46.1** - The world's most deployed database engine.