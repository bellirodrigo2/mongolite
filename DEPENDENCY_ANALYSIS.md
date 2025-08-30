# SQLite Core - Detailed Dependency Analysis

## Static Analysis Results

Based on static code analysis, here are the detailed dependencies between SQLite core files:

## Layer 1: Foundation (No Dependencies)
These files are at the bottom of the dependency chain:

### Platform Headers & Setup
- `vxworks.h` - VxWorks RTOS definitions
- `msvc.h` - MSVC compiler definitions  
- `hwtime.h` - Hardware timing macros
- `os_setup.h` - Platform detection macros
- `os_common.h` - Common OS definitions
- `os_win.h` - Windows-specific definitions

### Interface Definitions
- `mutex.h` - Mutex interface (no implementation)
- `hash.h` - Hash table interface
- `pcache.h` - Page cache interface

## Layer 2: Core Utilities (Minimal Dependencies)
These depend only on `sqliteInt.h` and system headers:

### Memory Management
- **`mem0.c`** - Memory allocator interface selector
  - Dependencies: `sqliteInt.h`
  - Exports: `sqlite3MemSetDefault()`

- **`mem1.c`** - Standard malloc() wrapper
  - Dependencies: `sqliteInt.h`, `<sys/sysctl.h>`, `<malloc/malloc.h>`
  - Exports: `sqlite3MemMalloc()`, `sqlite3MemFree()`

- **`mem2.c`** - Debug memory allocator
  - Dependencies: `sqliteInt.h`, `<stdio.h>`
  - Used for: Memory leak detection

- **`mem3.c`** - Emergency memory allocator
  - Dependencies: `sqliteInt.h`
  - Used for: Low-memory situations

- **`mem5.c`** - Power-of-2 memory allocator
  - Dependencies: `sqliteInt.h`
  - Used for: Embedded systems

### Basic Utilities
- **`random.c`** - Random number generation
  - Dependencies: `sqliteInt.h`
  - Exports: `sqlite3_randomness()`

- **`printf.c`** - Custom printf implementation
  - Dependencies: `sqliteInt.h`
  - Exports: `sqlite3_snprintf()`, `sqlite3_vmprintf()`

- **`utf.c`** - UTF-8/16 conversion
  - Dependencies: `sqliteInt.h`
  - Exports: `sqlite3Utf8To8()`, `sqlite3Utf16to8()`

## Layer 3: Core Infrastructure (Foundation + Layer 2)

### Memory Management Coordinator
- **`malloc.c`** - SQLite memory wrapper
  - Dependencies: `sqliteInt.h`, `<stdarg.h>`
  - Calls: `sqlite3MemMalloc()`, `sqlite3MemFree()` from mem*.c
  - Exports: `sqlite3Malloc()`, `sqlite3DbMalloc()`, `sqlite3_malloc()`

### Threading Support
- **`mutex_noop.c`** - No-op mutex (single-threaded)
  - Dependencies: `sqliteInt.h`
  - Exports: Stub mutex functions

- **`mutex_unix.c`** - POSIX pthread mutex
  - Dependencies: `sqliteInt.h`, `<pthread.h>`
  - Exports: `sqlite3_mutex_alloc()`, etc.

- **`mutex_w32.c`** - Windows mutex
  - Dependencies: `sqliteInt.h`, `os_common.h`, `os_win.h`
  - Exports: Windows-specific mutex functions

- **`mutex.c`** - Mutex abstraction layer
  - Dependencies: `sqliteInt.h`
  - Calls: Platform-specific mutex functions
  - Exports: `sqlite3MutexInit()`, `sqlite3_mutex_enter()`

### Data Structures
- **`hash.c`** - Hash table implementation
  - Dependencies: `sqliteInt.h`, `hash.h`, `<assert.h>`
  - Exports: `sqlite3HashInit()`, `sqlite3HashInsert()`
  - Calls: `sqlite3Malloc()`, `sqlite3_malloc()`

- **`util.c`** - String and utility functions
  - Dependencies: `sqliteInt.h`
  - Exports: `sqlite3StrICmp()`, `sqlite3HashNoCase()`, `sqlite3GetVarint()`
  - Calls: `sqlite3Malloc()`, memory allocation functions

## Layer 4: System Abstraction

### Operating System Interface
- **`os.c`** - OS abstraction layer
  - Dependencies: `sqliteInt.h`
  - Exports: `sqlite3_vfs_find()`, `sqlite3_vfs_register()`
  - Calls: Platform-specific VFS functions

- **`os_unix.c`** - Unix/Linux implementation (285KB)
  - Dependencies: `sqliteInt.h`, `os_common.h`, many system headers
  - Exports: Unix VFS implementation
  - Calls: `sqlite3_malloc()`, `sqlite3Malloc()`
  - System calls: `open()`, `read()`, `write()`, `fsync()`, `mmap()`

- **`os_win.c`** - Windows implementation (218KB)
  - Dependencies: `sqliteInt.h`, `os_common.h`, `os_win.h`, `windows.h`
  - Exports: Windows VFS implementation
  - Calls: `sqlite3_malloc()`, Windows API functions

### Status and Monitoring
- **`status.c`** - Status tracking
  - Dependencies: `sqliteInt.h`
  - Exports: `sqlite3_status()`, `sqlite3StatusValue()`
  - Calls: Memory allocation functions

- **`bitvec.c`** - Bit vector operations
  - Dependencies: `sqliteInt.h`
  - Exports: `sqlite3BitvecCreate()`, `sqlite3BitvecSet()`
  - Calls: `sqlite3Malloc()`

## Layer 5: Page Management

### Page Cache System
- **`pcache.c`** - Page cache abstraction
  - Dependencies: `sqliteInt.h`, `pcache.h`
  - Exports: `sqlite3PcacheInitialize()`, `sqlite3PcacheOpen()`
  - Calls: Implementation-specific cache functions

- **`pcache1.c`** - Default LRU page cache implementation
  - Dependencies: `sqliteInt.h`, `pcache.h`
  - Exports: `sqlite3PCacheSetDefault()`
  - Calls: `sqlite3_malloc()`, `sqlite3StatusValue()`

### Configuration and Globals
- **`global.c`** - Global variables and configuration
  - Dependencies: `sqliteInt.h`, `opcodes.h` (MISSING!)
  - Exports: Global SQLite configuration variables
  - Calls: Various initialization functions

## Layer 6: Storage Engine Core

### Pager - Transaction and File Management
- **`pager.c`** (302KB) - Core pager implementation
  - **Key Dependencies**:
    - `sqliteInt.h` - Core definitions
    - OS Layer: `sqlite3OsOpen()`, `sqlite3OsRead()`, `sqlite3OsWrite()`, `sqlite3OsSync()`
    - Page Cache: `sqlite3PcacheOpen()`, `sqlite3PcacheGet()`, `sqlite3PcacheMakeDirty()`
    - Memory: `sqlite3Malloc()`, `sqlite3DbMalloc()`
    - Mutex: `sqlite3_mutex_enter()`, `sqlite3_mutex_leave()`
  - **Key Exports**:
    - `sqlite3PagerOpen()` - Open database file
    - `sqlite3PagerGet()` - Get page from cache
    - `sqlite3PagerWrite()` - Mark page for writing
    - `sqlite3PagerBegin()`, `sqlite3PagerCommit()`, `sqlite3PagerRollback()` - Transactions

### B-Tree - Database Structure
- **`btree.c`** (403KB) - B-tree implementation
  - **Key Dependencies**:
    - `btreeInt.h` - Internal B-tree definitions
    - Pager: `sqlite3PagerGet()`, `sqlite3PagerWrite()`, `sqlite3PagerUnref()`
    - Memory: `sqlite3DbMalloc()`, `sqlite3DbFree()`
    - Mutex: `sqlite3_mutex_enter()`, `sqlite3_mutex_leave()`
    - Utilities: `sqlite3GetVarint()`, `sqlite3PutVarint()`
  - **Key Exports**:
    - `sqlite3BtreeOpen()` - Open B-tree database
    - `sqlite3BtreeCursor()` - Create cursor for iteration
    - `sqlite3BtreeInsert()` - Insert key-value pair
    - `sqlite3BtreeDelete()` - Delete entry
    - `sqlite3BtreeNext()`, `sqlite3BtreePrevious()` - Cursor navigation

## Critical Dependency Chains

### For Basic B-tree Operations:
```
Application Code
    ↓
btree.c (B-tree operations)
    ↓
pager.c (Page management)
    ↓
pcache1.c (Page caching)
    ↓
os_unix.c/os_win.c (File I/O)
    ↓
malloc.c → mem1.c (Memory allocation)
    ↓
mutex_unix.c/mutex_noop.c (Threading)
```

## Missing Components for Compilation

### Generated Headers (CRITICAL)
1. **`opcodes.h`** - Required by `global.c`
   - Contains SQLite opcode definitions
   - Generated by `tool/mkopcodes.tcl`

### Optional/SQL-Related (Can be stubbed for MongoLite)
2. **`parse.h`** - SQL parser definitions (not needed for document storage)
3. **`keywordhash.h`** - SQL keyword hash (not needed for document storage)

## Minimum Viable Compilation Units

### Phase 1: Memory + OS Layer
```c
// Compile order for basic infrastructure
mem0.c, mem1.c          // Memory allocation
mutex_noop.c            // Single-threaded (or mutex_unix.c for multi-threaded)
malloc.c                // Memory management wrapper
random.c, printf.c      // Basic utilities
os.c, os_unix.c         // File I/O (or os_win.c for Windows)
```

### Phase 2: Utilities + Caching
```c
hash.c                  // Hash tables
util.c                  // String utilities
status.c                // Status tracking
pcache.c, pcache1.c     // Page caching
bitvec.c                // Bit vectors
```

### Phase 3: Storage Engine
```c
global.c                // Global config (needs opcodes.h stub)
pager.c                 // Page management
btree.c                 // B-tree storage
```

## Build Strategy for MongoLite

1. **Create stub `opcodes.h`** with minimal definitions
2. **Start with single-threaded** build using `mutex_noop.c`
3. **Use standard allocator** with `mem1.c`
4. **Platform-specific** OS layer (`os_unix.c` or `os_win.c`)
5. **Build incrementally** following the layer dependencies

This analysis shows that SQLite's architecture is well-layered, making it suitable for extracting just the storage engine components for MongoLite.