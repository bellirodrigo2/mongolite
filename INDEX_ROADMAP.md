# Index Implementation Roadmap

## Current State Analysis

The codebase already has significant infrastructure prepared:

**API defined in mongolite.h:**
- `mongolite_create_index()` and `mongolite_drop_index()` signatures exist
- `index_config_t` structure with `unique`, `sparse`, `background`, `expire_after_seconds`

**Key comparison ready in key_compare.c:**
- `bson_extract_index_key()` - extracts fields from document based on index spec
- `bson_compare_docs()` - compares extracted keys for ordering

**Schema storage in mongolite_collection.c:**
- Index metadata stored in `entry.indexes` BSON array
- Default `_id` index already created on collection creation

---

## Phase 1: Index Storage Infrastructure ✅ COMPLETED

**Files created:**
- `src/mongolite_index.c` - Index helper functions
- `tests/test_mongolite_index.c` - 26 unit tests

**Functions implemented:**

| Function | Description |
|----------|-------------|
| `_index_name_from_spec()` | Generate index name from key spec (e.g., `"email_1"`) |
| `_build_index_key()` | Build composite key from document (with/without `_id`) |
| `_build_unique_check_key()` | Build key for unique constraint checking |
| `_index_key_compare()` | Comparator for wtree using `bson_compare_docs()` |
| `_should_index_document()` | Sparse index handling |
| `_index_key_serialize()` | Serialize index key to bytes |
| `_index_key_deserialize()` | Deserialize bytes to index key |
| `_index_value_from_doc()` | Extract `_id` for index value storage |
| `_index_value_get_oid()` | Get OID from index value |
| `_index_spec_to_bson()` | Serialize index spec for schema |
| `_index_spec_from_bson()` | Parse index spec from schema |

**Already existing:**
- `_mongolite_index_tree_name()` in `mongolite_util.c` - generates `idx:<collection>:<index_name>`

---

## Phase 2: Index Creation & Deletion ✅ COMPLETED

**Functions implemented in `mongolite_index.c`:**

| Function | Description |
|----------|-------------|
| `mongolite_create_index()` | Create index with full collection scan, supports unique/sparse |
| `mongolite_drop_index()` | Drop index by name (protects `_id_` index) |
| `_lmdb_index_compare()` | LMDB comparator wrapper for MongoDB-style ordering |
| `_index_exists()` | Check if index exists in collection schema |
| `_add_index_to_array()` | Add index spec to collection's indexes array |
| `_remove_index_from_array()` | Remove index from collection's indexes array |

**Tests in `test_mongolite_index_integration.c` (14 tests):**

- Basic index creation (simple, with name, on existing docs)
- Unique index creation and duplicate key violation detection
- Sparse index creation (skips null/missing fields)
- Compound index creation
- Index deletion and error cases
- Index persistence across database reopen

### 2.1 Implementation Steps ✅

1. ✅ Validate index specification (fields, options)
2. ✅ Generate index name if not provided
3. ✅ Check if index already exists in schema
4. ✅ Create new wtree tree with custom BSON comparator
5. ✅ Scan all existing documents in collection
6. ✅ For each document, extract index key and insert into index tree
7. ✅ Update schema with new index metadata
8. ✅ Handle unique constraint violations during build

### 2.2 Error Handling ✅

- Duplicate key on unique index → abort, return `MONGOLITE_EINDEX`
- Transaction rollback on any failure
- Cannot drop `_id_` index → return `MONGOLITE_EINVAL`

---

## Phase 3: Index Maintenance on CRUD ✅ COMPLETED

**Key optimization: Index cache in tree cache**
- `mongolite_cached_index_t` structure caches index name, keys, tree handle, unique/sparse flags
- Added to `mongolite_tree_cache_entry_t` for fast access without schema reads
- Lazy loading on first CRUD operation, invalidated on index create/drop

**Functions implemented:**

| Function | Description |
|----------|-------------|
| `_mongolite_get_cached_indexes()` | Get/load cached indexes for collection |
| `_mongolite_invalidate_index_cache()` | Invalidate cache on index create/drop |
| `_mongolite_index_insert()` | Maintain indexes after document insert |
| `_mongolite_index_delete()` | Maintain indexes after document delete |
| `_mongolite_index_update()` | Maintain indexes after document update |

**Modified files:**
- `mongolite_internal.h` - Added cached index structures and function declarations
- `mongolite_util.c` - Added index cache operations
- `mongolite_index.c` - Added index maintenance functions, cache invalidation
- `mongolite_insert.c` - Calls `_mongolite_index_insert()` after insert
- `mongolite_delete.c` - Calls `_mongolite_index_delete()` before delete
- `mongolite_update.c` - Calls `_mongolite_index_update()` for update/replace

**Tests in `test_index_maintenance.c` (9 tests):**
- Insert adds entries to secondary indexes
- Insert unique violation detection
- Insert sparse index skips null/missing fields
- Delete removes entries from indexes
- Delete many removes entries from indexes
- Update modifies index entries when indexed fields change
- Update unique violation detection
- Replace updates index entries
- Multiple indexes maintained together

### 3.1 Implementation Details ✅

1. ✅ Pre-load index cache before write transaction (avoids LMDB nested txn deadlock)
2. ✅ Check sparse index handling (skip null/missing fields)
3. ✅ Check unique constraint before insert/update
4. ✅ Properly handle update: delete old key, insert new key
5. ✅ Return proper error codes (MONGOLITE_EINDEX for violations)

---

## Phase 4: Query Optimization (`mongolite_find.c`) ✅ COMPLETED

**Functions implemented in `mongolite_index.c`:**

| Function | Description |
|----------|-------------|
| `_analyze_query_for_index()` | Parse filter to extract simple equality fields |
| `_free_query_analysis()` | Free query analysis structure |
| `_find_best_index()` | Select best matching index using prefix matching |
| `_find_one_with_index()` | Use index seek to find document by equality lookup |

**Modified files:**
- `mongolite_internal.h` - Added `query_analysis_t` structure and function declarations
- `mongolite_index.c` - Added query analysis and index selection functions
- `mongolite_find.c` - Modified `mongolite_find_one()` to use index optimization

**Tests in `test_query_optimization.c` (11 tests):**
- Query analysis for simple equality conditions
- Query analysis for multiple equality fields
- Query analysis detects operators (returns NULL for non-simple)
- Query analysis handles empty filter
- Query analysis skips `_id`-only queries (already optimized)
- Index selection for single field match
- Index selection returns NULL when no matching index
- find_one uses index for equality query
- find_one returns NULL when document not found via index
- find_one falls back to collection scan when no index
- find_one works with compound indexes

### 4.1 Implementation Details ✅

1. ✅ Parse query filter for simple equality conditions (no operators like `$gt`, `$in`)
2. ✅ Skip `_id`-only queries (already have dedicated optimization)
3. ✅ Select best index using prefix matching strategy
4. ✅ Use index seek for exact match lookup
5. ✅ Fetch document by `_id` from collection after index lookup
6. ✅ Fall back to collection scan when no suitable index

### 4.2 Index Scan Types

- **Exact match**: ✅ Implemented for `find_one` with simple equality
- **Range scan**: For `$gt`, `$lt`, `$gte`, `$lte` (future enhancement)
- **Prefix scan**: For compound indexes (future enhancement)

### 4.3 Covered Queries

If all requested fields are in the index, return directly from index without fetching document. (Future enhancement)

---

## Implementation Order

| Step | Component | Status |
|------|-----------|--------|
| 1 | `mongolite_index.c` (Phase 1) | ✅ Complete |
| 2 | `mongolite_create_index()` (Phase 2) | ✅ Complete |
| 3 | `mongolite_drop_index()` (Phase 2) | ✅ Complete |
| 4 | Modify `mongolite_insert.c` (Phase 3) | ✅ Complete |
| 5 | Modify `mongolite_delete.c` (Phase 3) | ✅ Complete |
| 6 | Modify `mongolite_update.c` (Phase 3) | ✅ Complete |
| 7 | Modify `mongolite_find.c` (Phase 4) | ✅ Complete |

---

## Key Design Decisions

1. **Index key format**: Use `bson_extract_index_key()` output serialized as BSON for consistent comparison via `bson_compare_docs()`

2. **Unique indexes**: Append `_id` to all index keys to ensure uniqueness in the tree; for unique indexes, first check for existing key without `_id` suffix

3. **Sparse indexes**: Skip documents where indexed field is missing/null

4. **Compound indexes**: Already supported by `bson_extract_index_key()` which handles multiple fields

5. **Index tree comparison**: Use custom comparator wrapping `bson_compare_docs()` for proper MongoDB-style ordering
