# SQLite Core - Build Order and Compilation Units

## Dependency Matrix and Build Order

Based on the static analysis, here's the precise build order for SQLite core components:

## Phase 1: Foundation Layer (No Internal Dependencies)
These can be compiled in any order as they only depend on system headers:

```makefile
# Memory allocators (choose one based on needs)
FOUNDATION_MEM = mem0.c mem1.c  # Standard allocator
# FOUNDATION_MEM = mem0.c mem2.c  # Debug allocator  
# FOUNDATION_MEM = mem0.c mem3.c  # Emergency allocator
# FOUNDATION_MEM = mem0.c mem5.c  # Power-of-2 allocator

# Threading support (choose one based on platform)
FOUNDATION_MUTEX = mutex_noop.c   # Single-threaded
# FOUNDATION_MUTEX = mutex_unix.c  # POSIX threads
# FOUNDATION_MUTEX = mutex_w32.c   # Windows threads

# Basic utilities
FOUNDATION_UTILS = random.c printf.c utf.c

# Platform OS layer (choose one)
FOUNDATION_OS = os.c os_unix.c    # Unix/Linux
# FOUNDATION_OS = os.c os_win.c   # Windows

PHASE1_OBJECTS = $(FOUNDATION_MEM:.c=.o) $(FOUNDATION_MUTEX:.c=.o) \
                 $(FOUNDATION_UTILS:.c=.o) $(FOUNDATION_OS:.c=.o)
```

## Phase 2: Infrastructure Layer (Depends on Phase 1)
These depend on foundation components:

```makefile
PHASE2_SOURCES = malloc.c hash.c util.c status.c bitvec.c
PHASE2_OBJECTS = $(PHASE2_SOURCES:.c=.o)

# Dependencies:
# malloc.c    <- mem*.c (memory allocators)
# hash.c      <- malloc.c (for memory allocation)
# util.c      <- malloc.c (for memory allocation)
# status.c    <- malloc.c (for memory allocation)
# bitvec.c    <- malloc.c (for memory allocation)
```

## Phase 3: Subsystem Layer (Depends on Phase 1+2)
Higher-level subsystems:

```makefile
PHASE3_SOURCES = mutex.c pcache.c pcache1.c global.c
PHASE3_OBJECTS = $(PHASE3_SOURCES:.c=.o)

# Dependencies:
# mutex.c     <- mutex_*.c (platform-specific mutexes)
# pcache.c    <- malloc.c, status.c
# pcache1.c   <- pcache.c, malloc.c, status.c
# global.c    <- opcodes.h (now provided as stub)
```

## Phase 4: Storage Engine (Depends on All Previous)
Core storage components:

```makefile
PHASE4_SOURCES = pager.c btree.c
PHASE4_OBJECTS = $(PHASE4_SOURCES:.c=.o)

# Dependencies:
# pager.c     <- os.c, pcache.c, mutex.c, malloc.c, util.c
# btree.c     <- pager.c, mutex.c, malloc.c, util.c
```

## Complete Build Configuration

### Makefile Template
```makefile
# SQLite Core for MongoLite
CC = gcc
CFLAGS = -DSQLITE_CORE -DSQLITE_OMIT_LOAD_EXTENSION -DSQLITE_THREADSAFE=0 \
         -DSQLITE_OMIT_WAL -DSQLITE_OMIT_SHARED_CACHE -I.

# Choose configuration
MEMORY_CONFIG = mem0.c mem1.c        # Standard malloc
THREAD_CONFIG = mutex_noop.c         # Single threaded
PLATFORM_CONFIG = os.c os_unix.c     # Unix/Linux

# Build phases
PHASE1_SRC = $(MEMORY_CONFIG) $(THREAD_CONFIG) $(PLATFORM_CONFIG) \
             random.c printf.c utf.c
PHASE2_SRC = malloc.c hash.c util.c status.c bitvec.c  
PHASE3_SRC = mutex.c pcache.c pcache1.c global.c
PHASE4_SRC = pager.c btree.c

ALL_SRC = $(PHASE1_SRC) $(PHASE2_SRC) $(PHASE3_SRC) $(PHASE4_SRC)
ALL_OBJ = $(ALL_SRC:.c=.o)

# Build targets
sqlite-core.a: $(ALL_OBJ)
	ar rcs $@ $(ALL_OBJ)

# Test compilation
test: test_compile.c sqlite-core.a
	$(CC) $(CFLAGS) -o test_compile test_compile.c sqlite-core.a

clean:
	rm -f *.o sqlite-core.a test_compile

# Individual phase targets for debugging
phase1: $(PHASE1_SRC:.c=.o)
phase2: phase1 $(PHASE2_SRC:.c=.o)  
phase3: phase2 $(PHASE3_SRC:.c=.o)
phase4: phase3 $(PHASE4_SRC:.c=.o)

.PHONY: test clean phase1 phase2 phase3 phase4
```

## Compilation Commands for Testing

### Test Each Phase Individually
```bash
# Phase 1 - Foundation
gcc -DSQLITE_CORE -DSQLITE_THREADSAFE=0 -I. -c mem0.c mem1.c mutex_noop.c \
    random.c printf.c utf.c os.c os_unix.c

# Phase 2 - Infrastructure  
gcc -DSQLITE_CORE -DSQLITE_THREADSAFE=0 -I. -c malloc.c hash.c util.c \
    status.c bitvec.c

# Phase 3 - Subsystems
gcc -DSQLITE_CORE -DSQLITE_THREADSAFE=0 -I. -c mutex.c pcache.c pcache1.c \
    global.c

# Phase 4 - Storage Engine
gcc -DSQLITE_CORE -DSQLITE_THREADSAFE=0 -I. -c pager.c btree.c

# Link all together
ar rcs sqlite-core.a *.o

# Test final compilation
gcc -DSQLITE_CORE -DSQLITE_THREADSAFE=0 -I. -o test test_compile.c sqlite-core.a
```

## Minimal Configuration Flags

### Essential Defines
```c
#define SQLITE_CORE                    // Building core library
#define SQLITE_THREADSAFE 0            // Single-threaded for simplicity
#define SQLITE_OMIT_LOAD_EXTENSION     // No extension loading
#define SQLITE_OMIT_SHARED_CACHE       // No shared cache
#define SQLITE_OMIT_WAL                // No write-ahead logging
#define SQLITE_OMIT_AUTOVACUUM         // No auto-vacuum
#define SQLITE_OMIT_AUTHORIZATION      // No authorization callbacks
```

### Platform Specific
```c
// Unix/Linux
#define SQLITE_OS_UNIX 1

// Windows  
#define SQLITE_OS_WIN 1
```

## Troubleshooting Build Issues

### Missing Includes
If compilation fails with missing includes:
1. Check that `opcodes.h` stub is present
2. Verify all required `.h` files are copied
3. Add `-I.` to include current directory

### Undefined Symbols  
If linking fails with undefined symbols:
1. Ensure all phases are compiled in order
2. Check that platform-specific files match target OS
3. Verify mutex implementation matches threading config

### Feature Dependencies
If functions are missing:
1. Add appropriate `SQLITE_OMIT_*` defines to disable unused features
2. Check for conditionally compiled code blocks
3. May need to provide stub implementations for omitted features

## File Count Summary
- **Phase 1**: 8 files (foundation)
- **Phase 2**: 5 files (infrastructure)  
- **Phase 3**: 4 files (subsystems)
- **Phase 4**: 2 files (storage engine)
- **Total**: 19 source files + headers

## Success Criteria
1. ✅ All source files compile without errors
2. 🔄 Static library `sqlite-core.a` links successfully
3. ⏭️ Test program runs basic B-tree operations
4. ⏭️ Ready for BSON integration and MongoDB API layer