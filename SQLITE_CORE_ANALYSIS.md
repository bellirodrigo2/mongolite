# SQLite Core Module Analysis

This document provides an analysis of the SQLite core files copied to `src/sqlite-core/` and their roles in the MongoLite project.

## Core Storage Engine Files

### B-Tree Layer (Database Structure)
- **`btree.c`** (403KB) - Main B-tree implementation for database storage
  - Implements the B-tree database file format
  - Handles page allocation, splitting, and merging
  - Dependencies: `btreeInt.h`, `pager.h`, `sqliteInt.h`
  
- **`btree.h`** - B-tree public interface definitions
  - Defines B-tree API functions and structures
  - Used by: Applications interfacing with B-tree storage
  
- **`btreeInt.h`** - B-tree internal definitions and structures
  - Internal B-tree data structures and constants
  - Used by: `btree.c` and other B-tree related code

### Pager Layer (Page Management & Transactions)
- **`pager.c`** (302KB) - Page cache and transaction management
  - Implements atomic commit/rollback through journaling
  - Handles file locking and concurrent access
  - Dependencies: `sqliteInt.h`, `os.h`, `pcache.h`
  
- **`pager.h`** - Pager public interface
  - Page cache API and type definitions
  - Used by: `btree.c` and other storage components

### Operating System Abstraction Layer
- **`os.c`** - OS abstraction layer implementation
  - Cross-platform file I/O operations
  - Dependencies: `sqliteInt.h`, `os.h`
  
- **`os.h`** - OS abstraction layer interface
  - Defines VFS (Virtual File System) interface
  - Platform detection and setup macros
  
- **`os_unix.c`** (285KB) - Unix/Linux specific implementations
  - POSIX file operations, locking, memory mapping
  - Used when building on Unix-like systems
  
- **`os_win.c`** (218KB) - Windows specific implementations  
  - Win32 file operations and locking
  - Used when building on Windows systems
  
- **`os_win.h`** - Windows specific definitions
- **`os_common.h`** - Common OS abstraction definitions
- **`os_setup.h`** - Platform detection and setup

### Page Cache System
- **`pcache.c`** - Page cache abstraction layer
  - Interface between pager and page cache implementations
  - Dependencies: `sqliteInt.h`, `pcache.h`
  
- **`pcache.h`** - Page cache interface definitions
  
- **`pcache1.c`** - Default page cache implementation
  - LRU page replacement algorithm
  - Dependencies: `sqliteInt.h`, `pcache.h`

### Memory Management
- **`malloc.c`** - SQLite memory allocation wrapper
  - Debugging and tracking capabilities
  - Dependencies: `sqliteInt.h`
  
- **`mem0.c`** - Memory allocator interface
- **`mem1.c`** - Standard malloc() wrapper
- **`mem2.c`** - Debug memory allocator  
- **`mem3.c`** - Emergency memory allocator
- **`mem5.c`** - Power-of-two memory allocator

### Threading & Synchronization
- **`mutex.c`** - Mutex abstraction layer
  - Cross-platform mutex interface
  - Dependencies: `sqliteInt.h`, `mutex.h`
  
- **`mutex.h`** - Mutex interface definitions
- **`mutex_unix.c`** - Unix pthread mutex implementation
- **`mutex_w32.c`** - Windows mutex implementation  
- **`mutex_noop.c`** - No-op mutex for single-threaded builds

### Utility Components
- **`hash.c`** - Hash table implementation
  - Generic hash table for internal use
  - Dependencies: `sqliteInt.h`, `hash.h`
  
- **`hash.h`** - Hash table interface
  
- **`util.c`** - Utility functions
  - String manipulation, comparison, conversion utilities
  - Dependencies: `sqliteInt.h`
  
- **`printf.c`** - Custom printf implementation
  - SQLite's custom printf with additional format specifiers
  - Dependencies: `sqliteInt.h`
  
- **`utf.c`** - UTF-8/UTF-16 string handling
  - Unicode string conversion and validation
  - Dependencies: `sqliteInt.h`
  
- **`bitvec.c`** - Bit vector implementation
  - Used for tracking page usage in auto-vacuum
  - Dependencies: `sqliteInt.h`
  
- **`random.c`** - Random number generation
  - Cryptographically secure random numbers
  - Dependencies: `sqliteInt.h`
  
- **`global.c`** - Global variables and configuration
  - SQLite global state and configuration
  - Dependencies: `sqliteInt.h`
  
- **`status.c`** - Status and statistics tracking
  - Runtime statistics and status information
  - Dependencies: `sqliteInt.h`

### Header Files
- **`sqlite.h`** - Main SQLite public API header
  - All public SQLite API functions and constants
  - This is the primary header applications include
  
- **`sqliteInt.h`** (256KB) - Internal SQLite definitions
  - Core internal structures, macros, and function declarations
  - Used by: Nearly all SQLite source files
  
- **`sqliteLimit.h`** - SQLite compile-time limits
  - Maximum values and limits for various SQLite features
  
- **`hwtime.h`** - Hardware timing definitions
- **`msvc.h`** - Microsoft Visual C++ specific definitions
- **`vxworks.h`** - VxWorks RTOS specific definitions

## File Dependencies Overview

### Core Dependency Chain:
1. **`sqliteInt.h`** - Included by almost all files
2. **`os.h`** - Included by I/O related files  
3. **Platform-specific files** (`os_unix.c`, `os_win.c`, etc.) - Conditionally compiled
4. **`btreeInt.h`** - Used by B-tree implementation
5. **`pager.h`**, **`pcache.h`**, **`mutex.h`**, **`hash.h`** - Component interfaces

### Key Relationships:
- **B-tree** depends on **Pager** for page management
- **Pager** depends on **OS layer** for file I/O
- **Pager** depends on **Page Cache** for memory management
- **OS layer** depends on platform-specific implementations
- **All components** depend on **Memory Management** and **Threading**

## Architecture for MongoLite Integration

This SQLite core provides:
1. **File I/O and Paging** - Reliable disk storage with ACID properties
2. **B-tree Storage** - Efficient key-value storage with indexing
3. **Memory Management** - Pluggable memory allocators
4. **Cross-platform Support** - Works on Unix, Windows, and embedded systems
5. **Thread Safety** - Proper synchronization primitives

For MongoLite, we will:
- Use B-tree storage for document collections
- Replace SQL parsing/execution with BSON document handling
- Maintain SQLite's proven file format and transaction system
- Add MongoDB-style query operators on top of B-tree cursors