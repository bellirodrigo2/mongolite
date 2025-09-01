# Mongolite Development Documentation

This directory contains development documentation for the Mongolite project.

## Project Overview

Mongolite is a project that demonstrates direct access to SQLite's internal B-tree APIs without using SQL statements. It builds SQLite from non-amalgamation source code to expose internal data structures and functions.

## Key Achievements

✅ **SQLite Source Build**: Successfully builds SQLite 3.46.1 from non-amalgamation source  
✅ **Internal API Access**: Exposes SQLite's internal B-tree APIs using compile flags  
✅ **Direct B-tree Operations**: Insert, read, and search operations without SQL  
✅ **Header Generation**: Generates all required internal headers (`parse.h`, `opcodes.h`, etc.)  
✅ **CMake Integration**: Automated build process with dependency management  
✅ **Working Examples**: Complete test suite demonstrating B-tree API usage  

## Documentation

- **[SQLite Source Build](sqlite-source-build.md)** - How we build SQLite from source
- **[B-tree API Guide](btree-api-guide.md)** - Complete guide to using B-tree APIs

## Project Structure

```
mongolite/
├── CMakeLists.txt          # Build configuration
├── Makefile               # Build shortcuts
├── tests/
│   ├── test.c            # Basic SQLite test
│   └── test_btree.c      # B-tree API test
├── docs/dev/             # Development documentation
├── deps/                 # Downloaded dependencies
│   └── sqlite-src/       # SQLite source code
└── build/                # Build output
```

## Quick Start

```bash
# Build the project
make build

# Run basic SQLite test
make run-test

# Run B-tree API test  
make run-btree

# Run all tests
make test
```

## Build Requirements

- CMake 3.14+
- GCC/Clang compiler
- Make build system
- Tcl 8.6 (for SQLite build tools)
- Standard UNIX development tools

## Key Technologies

- **SQLite 3.46.1** - Database engine (built from source)
- **CMake** - Build system  
- **FetchContent** - Dependency management
- **C11** - Programming language standard

## Test Results

Our B-tree API test successfully demonstrates:

```
SQLite B-tree Internal API Test
===============================
✓ SQLite database opened
✓ B-tree handle obtained: 0x57a3cbf1d888
✓ Write transaction started
✓ B-tree table created with root page: 2
✓ B-tree cursor opened

--- Inserting into B-tree ---
✓ Inserted B-tree entry: key=1, data='apple'
✓ Inserted B-tree entry: key=2, data='banana'
✓ Inserted B-tree entry: key=3, data='cherry'
✓ Inserted B-tree entry: key=4, data='date'
✓ Inserted B-tree entry: key=5, data='elderberry'

--- Reading from B-tree ---
→ B-tree entry: key=1, data='apple' (5 bytes)
→ B-tree entry: key=2, data='banana' (6 bytes)
→ B-tree entry: key=3, data='cherry' (6 bytes)
→ B-tree entry: key=4, data='date' (4 bytes)
→ B-tree entry: key=5, data='elderberry' (10 bytes)

--- B-tree key search ---
✓ Found key 3: 'cherry'
✓ Transaction committed
✓ Direct B-tree API operations completed successfully!
```

## What Makes This Special

This project demonstrates **true statement-free B-tree operations** using SQLite's internal APIs:

1. **No SQL Statements**: Direct B-tree manipulation without any SQL
2. **Internal API Access**: Uses SQLite's private functions normally hidden from users
3. **Custom Build Process**: Builds SQLite from source with special configuration
4. **Complete B-tree Operations**: Create, insert, read, search, and navigate B-trees
5. **Proper Integration**: CMake handles the complex build process automatically

## Contributing

When working on this project:

1. Understand the SQLite source build process
2. Be familiar with B-tree data structures
3. Always test both basic SQLite and B-tree operations
4. Update documentation when adding features
5. Use proper mutex handling for thread safety

## Future Directions

Potential enhancements:
- Additional B-tree operations (delete, update)
- Index B-tree support (BTREE_BLOBKEY)
- Page-level operations
- Performance benchmarking
- Custom storage backends