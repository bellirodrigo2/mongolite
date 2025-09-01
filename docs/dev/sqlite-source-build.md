# SQLite Source Build Process

This document describes how we build SQLite from non-amalgamation source to access internal B-tree APIs.

## Overview

The project uses SQLite's non-amalgamation source code to gain access to internal B-tree APIs that are normally hidden. This allows direct manipulation of SQLite's B-tree data structures without using SQL statements.

## Build Process

### 1. Source Download

- **URL**: `https://www.sqlite.org/2024/sqlite-src-3460100.zip`
- **Version**: SQLite 3.46.1
- **Type**: Non-amalgamation source code
- **Destination**: `deps/sqlite-src/`

### 2. Configure and Build

The build process runs SQLite's configure script and make to generate all required headers:

```bash
cd deps/sqlite-src
./configure --enable-debug --disable-shared
make -j4
```

### 3. Generated Files

The build process creates essential files:
- `sqlite3.c` - Generated amalgamation file
- `sqlite3.h` - Public API header
- `parse.h` - Generated parser definitions
- `opcodes.h` - VDBE opcode definitions
- `tsrc/` - Directory with processed source files
- `tsrc/sqliteInt.h` - Internal APIs header
- `tsrc/btreeInt.h` - B-tree internal definitions

### 4. CMake Integration

Our CMakeLists.txt automatically handles the build:

```cmake
# Check if SQLite is already built, if not, build it
if(NOT EXISTS "${SQLITE_BUILD_DIR}/sqlite3.c")
    message(STATUS "Building SQLite from source...")
    execute_process(
        COMMAND ./configure --enable-debug --disable-shared
        WORKING_DIRECTORY ${SQLITE_BUILD_DIR}
        RESULT_VARIABLE CONFIGURE_RESULT
    )
    execute_process(
        COMMAND make -j4
        WORKING_DIRECTORY ${SQLITE_BUILD_DIR}
        RESULT_VARIABLE MAKE_RESULT
    )
endif()
```

## Internal API Access

### Compiler Definitions

To expose internal APIs, we use these definitions:

```cmake
target_compile_definitions(sqlite3 PRIVATE
    SQLITE_ENABLE_API_ARMOR=0    # Disable API protection
    SQLITE_DEBUG=1               # Enable debug features
    SQLITE_PRIVATE=              # Make internal functions public
    SQLITE_CORE=1               # Core SQLite functionality
)
```

### Include Directories

We provide access to all header locations:

```cmake
target_include_directories(sqlite3 PUBLIC
    ${SQLITE_BUILD_DIR}          # Generated amalgamation headers
    ${SQLITE_BUILD_DIR}/tsrc     # Processed source files
    ${SQLITE_BUILD_DIR}/src      # Original source headers
)
```

## Key Benefits

1. **Direct B-tree Access**: Can use `sqlite3BtreeOpen()`, `sqlite3BtreeInsert()`, etc.
2. **No SQL Overhead**: Direct data structure manipulation
3. **Full Control**: Access to internal cursors, transactions, and pages
4. **Debug Features**: Enhanced debugging capabilities

## Files Created

After successful build:
- `deps/sqlite-src/sqlite3.c` - Main amalgamation file (8MB+)
- `deps/sqlite-src/sqlite3.h` - Public header (644KB)
- `deps/sqlite-src/parse.h` - Parser definitions (8KB)
- `deps/sqlite-src/opcodes.h` - VDBE opcodes (15KB)
- `deps/sqlite-src/tsrc/` - Directory with all processed sources

## Dependencies

- **CMake 3.14+**
- **GCC/Clang compiler**
- **Make build system**
- **Tcl 8.6** (for build tools)
- **Standard UNIX tools** (sed, grep, etc.)

## Troubleshooting

### Common Issues

1. **Missing `parse.h`**: Build process didn't complete - check make output
2. **Tcl not found**: Install tcl8.6-dev package
3. **Permission errors**: Ensure write access to deps/ directory
4. **Build failures**: Check for missing development packages

### Clean Rebuild

To force a complete rebuild:

```bash
rm -rf deps/sqlite-src
rm -rf build
make build
```

This will re-download, re-configure, and rebuild everything.