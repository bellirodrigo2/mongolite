# MongoLite Source Code Structure

This directory contains the complete source code for MongoLite, a MongoDB-compatible document database built on SQLite.

## Directory Structure

```
src/
├── sqlite/           # Complete SQLite 3.46.1 amalgamation
│   ├── sqlite3.c     # SQLite source code (9MB single file)
│   ├── sqlite3.h     # SQLite API header
│   ├── sqlite3ext.h  # SQLite extension interface
│   ├── shell.c       # SQLite command-line shell
│   └── libsqlite3.a  # Compiled static library
├── mongolite/        # MongoLite core implementation
├── tests/            # Test programs and unit tests
├── examples/         # Example applications
├── docs/             # Source code documentation
└── Makefile          # Build system

```

## Building

### Prerequisites
- GCC compiler
- Make
- pthread, dl, and math libraries

### Build Commands

```bash
# Build everything
make all

# Build just SQLite library
make sqlite-lib

# Build SQLite shell
make sqlite-shell

# Build and run tests
make test

# Clean build artifacts
make clean
```

## SQLite Information

- **Version**: 3.46.1 (2024-08-13)
- **Type**: Amalgamation (single-file distribution)
- **Size**: 9MB source, 1.5MB compiled
- **Features**: Complete SQLite functionality including B-trees, SQL parser, VDBE

## Integration Status

✅ **Complete**: SQLite storage engine
⏳ **Planned**: MongoDB API layer
⏳ **Planned**: BSON document support
⏳ **Planned**: MongoDB query language

## Testing

The `tests/test_sqlite.c` program verifies basic SQLite functionality:
- Database creation/connection
- Table creation
- Data insertion and querying  
- Memory management

Run with: `make test`