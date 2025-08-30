# SQLite Core - Next Steps for Compilation

## Current Status
✅ **COMPLETED**: Successfully copied core SQLite files to `src/sqlite-core/`
✅ **COMPLETED**: Created comprehensive documentation in `SQLITE_CORE_ANALYSIS.md`
✅ **COMPLETED**: Identified 42 core files needed for storage engine functionality

## Files Successfully Copied (42 files)
### Core Engine Files
- `btree.c`, `btree.h`, `btreeInt.h` - B-tree storage engine
- `pager.c`, `pager.h` - Page cache and transaction management  
- `os.c`, `os.h`, `os_*.c` files - Cross-platform file I/O
- `pcache.c`, `pcache.h`, `pcache1.c` - Page caching system
- `sqlite.h`, `sqliteInt.h` - Main API and internal definitions

### Supporting Files
- Memory management: `malloc.c`, `mem*.c` files
- Threading: `mutex.c`, `mutex_*.c` files
- Utilities: `hash.c`, `util.c`, `printf.c`, `utf.c`, etc.

## IMMEDIATE NEXT STEPS (for session with compiler)

### 1. Identify and Create Missing Generated Headers
**PRIORITY**: These files are generated during SQLite build process and are missing:
- `opcodes.h` - Required by `global.c` 
- `parse.h` - SQL parser definitions (may not be needed for MongoLite)
- `keywordhash.h` - Keyword hash table (may not be needed)

**Action**: 
```bash
# Find these files in SQLite build directory or generate them
find .deps/sqlite/ -name "opcodes.h" -o -name "parse.h" 
# If not found, check tool/ directory for generators:
# - mkopcodes.tcl generates opcodes.h
# - lemon generates parse.h from parse.y
```

### 2. Create Minimal Compilation Test
**File**: `test_compile.c` (already created)
**Command**: 
```bash
cd src/sqlite-core
gcc -DSQLITE_CORE -DSQLITE_OMIT_LOAD_EXTENSION -c test_compile.c
```

### 3. Resolve Compilation Issues Systematically
Expected issues and solutions:

#### Missing Headers
- Copy or generate missing `.h` files from SQLite build
- May need to run SQLite's configure/build process once to generate headers

#### Unused Features to Disable
Add these defines to minimize dependencies:
```c
#define SQLITE_OMIT_LOAD_EXTENSION
#define SQLITE_OMIT_SHARED_CACHE  
#define SQLITE_OMIT_WAL
#define SQLITE_OMIT_AUTOVACUUM
#define SQLITE_OMIT_JSON
#define SQLITE_THREADSAFE=0  // If single-threaded
```

#### Platform-Specific Code
- Use appropriate `os_*.c` file based on target platform
- May need to adjust `#ifdef` conditions in `os_setup.h`

### 4. Create Minimal MongoLite API Bridge
Once compilation works, create:
- `mongolite.h` - Public API header
- `mongolite.c` - Bridge between MongoDB-style API and SQLite B-tree
- Focus on basic operations: create collection, insert document, find document

## COMPILATION STRATEGY

### Phase 1: Core Storage Only
Compile minimal set for storage engine:
```bash
gcc -DSQLITE_CORE -c \
  btree.c pager.c os.c pcache.c pcache1.c \
  mutex.c mutex_unix.c hash.c malloc.c \
  mem1.c util.c random.c global.c \
  -o sqlite-core.o
```

### Phase 2: Add Platform Support  
Add appropriate OS layer:
- Unix: `os_unix.c`
- Windows: `os_win.c`

### Phase 3: Integration Test
Create simple test that:
1. Opens a B-tree database file
2. Inserts key-value pairs (simulate BSON documents)
3. Retrieves data by key
4. Closes database

### Phase 4: BSON Integration
Add libbson integration from `.deps/mongo-c-driver/`

## FILE STRUCTURE READY FOR NEXT SESSION
```
src/sqlite-core/
├── SQLITE_CORE_ANALYSIS.md     # Complete file documentation
├── NEXT_STEPS.md                # This file - compilation roadmap
├── test_compile.c               # Test compilation file
├── [42 SQLite core files]       # All needed source files
└── [Missing generated headers]   # Need to resolve in next session
```

## KEY SUCCESS INDICATORS
1. ✅ All 42 core files copied and documented
2. 🔄 Compilation test passes (need compiler)
3. ⏭️ Basic B-tree operations work
4. ⏭️ BSON document storage/retrieval functional
5. ⏭️ MongoDB-like API layer created

## NOTES FOR NEXT SESSION
- This is a **storage engine extraction**, not full SQLite rebuild
- Focus on **B-tree + Pager + OS abstraction** as core components
- **Minimize dependencies** - omit SQL parsing, virtual tables, etc.
- **Goal**: Functional C library for document storage with MongoDB API
- **Architecture**: SQLite storage + BSON serialization + MongoDB API

## COMPILATION COMMAND REFERENCE
```bash
# Basic test compilation
gcc -DSQLITE_CORE -DSQLITE_OMIT_LOAD_EXTENSION -I. -c test_compile.c

# Full minimal build (once headers resolved)
gcc -DSQLITE_CORE -DSQLITE_OMIT_LOAD_EXTENSION \
    -DSQLITE_OMIT_SHARED_CACHE -DSQLITE_THREADSAFE=0 \
    -I. -c btree.c pager.c os.c os_unix.c pcache.c pcache1.c \
    mutex.c mutex_noop.c hash.c malloc.c mem1.c util.c \
    random.c global.c bitvec.c status.c printf.c utf.c
```

**Status**: Ready for compilation phase with proper C development environment.