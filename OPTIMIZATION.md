# Mongolite Performance Optimization Guide

## Benchmark Summary (December 2024)

**Test Environment:** Windows, 8 cores @ 2.2GHz, 6MB L3 cache

### Insert Operations

| Operation | Throughput | Latency | Notes |
|-----------|------------|---------|-------|
| InsertOne | 3.1k/s | 322μs | Single document |
| InsertMany/10 | 14.6k/s | 68μs/doc | Batch of 10 |
| InsertMany/100 | 54.6k/s | 18μs/doc | Batch of 100 |
| InsertMany/1000 | 62.6k/s | 16μs/doc | Batch of 1000 |
| InsertOneJson | 3.0k/s | 331μs | JSON parsing overhead |
| InsertAtScale | ~58/s | 17ms | With 100K existing docs |

**Key Insight:** Batching provides **20x throughput improvement** (3.1k → 62.6k docs/s).

### Find Operations

| Operation | Throughput | Latency | Notes |
|-----------|------------|---------|-------|
| FindOneById | 704k/s | 1.4μs | Direct LMDB key lookup |
| FindOneByRefId | 78.7k/s | 12.7μs | Full scan with filter |
| FindOneByRange | 457k/s | 2.2μs | Early match in scan |
| FindManyCursor/100 | 32.8k/s | 30μs | Cursor iteration |
| FindManyCursor/10000 | 375/s | 2.7ms | Full collection scan |
| FindWithSort/1000 | 3.9k/s | 256μs | In-memory sort |
| FindPagination/page50 | 4.9k/s | 203μs | Skip 5000 docs |

**Key Insight:** `_id` lookup is **9x faster** than field scan (704k vs 78k/s).

### Update Operations

| Operation | Throughput | Latency | Notes |
|-----------|------------|---------|-------|
| UpdateOneSetById | 3.3k/s | 307μs | By _id |
| UpdateOneSetByField | 2.3k/s | 429μs | By field (scan) |
| UpdateOneInc | 2.9k/s | 340μs | $inc operator |
| UpdateMany (~1.2k docs) | 24-32/s | 35-42ms | Bulk update |

**Key Insight:** Update = Find + Modify + Write. Each step adds latency.

### Delete Operations

| Operation | Throughput | Latency | Notes |
|-----------|------------|---------|-------|
| DeleteOneById | 2.7k/s | 375μs | By _id |
| DeleteOneByField | 2.5k/s | 406μs | By field (scan) |
| DeleteMany (~1.3k) | 27/s | 37ms | Bulk delete |
| DeleteAndReinsert | 2.5k/s | 795μs | Churn simulation |

---

## Optimization Opportunities

### Priority 1: High Impact, Low Difficulty

#### 1.1 Read Transaction Pooling (NEW - wtree_txn_reset/renew)
**Operation:** Find (all read operations)
**Difficulty:** ★★☆☆☆
**Expected Gain:** 20-40% for read-heavy workloads

LMDB's `mdb_txn_begin` is expensive (memory allocation + snapshot).
`mdb_txn_renew` only acquires a snapshot (much faster).

**Now available in wtree:**
```c
// wtree.h - NEW functions
void wtree_txn_reset(wtree_txn_t *txn);   // Release snapshot, keep handle
int wtree_txn_renew(wtree_txn_t *txn, gerror_t *error);  // Acquire new snapshot
```

**Implementation in mongolite:**
```c
// In mongolite_db structure, add:
struct mongolite_db {
    // ... existing fields ...
    wtree_txn_t *read_txn_pool;  // Cached read transaction
};

// In _mongolite_get_read_txn():
wtree_txn_t* _mongolite_get_read_txn(mongolite_db_t *db, gerror_t *error) {
    if (db->explicit_txn) return db->explicit_txn;

    // Reuse pooled transaction if available
    if (db->read_txn_pool) {
        if (wtree_txn_renew(db->read_txn_pool, error) == 0) {
            return db->read_txn_pool;
        }
        // Renew failed, abort and create new
        wtree_txn_abort(db->read_txn_pool);
        db->read_txn_pool = NULL;
    }

    // Create new transaction
    wtree_txn_t *txn = wtree_txn_begin(db->wtree_db, false, error);
    db->read_txn_pool = txn;  // Cache for reuse
    return txn;
}

// In _mongolite_release_read_txn():
void _mongolite_release_read_txn(mongolite_db_t *db, wtree_txn_t *txn) {
    if (db->explicit_txn) return;  // Don't touch explicit transactions

    // Reset instead of abort - keeps handle for reuse
    wtree_txn_reset(txn);
}
```

**Benchmark comparison:**
| Operation | Before | After (estimated) |
|-----------|--------|-------------------|
| FindOneById | 1.4μs | ~1.0μs |
| FindOneByRefId | 12.7μs | ~10μs |

---

#### 1.2 Tree Cache Optimization
**Operation:** All
**Difficulty:** ★☆☆☆☆
**Expected Gain:** 10-20%

Currently `_mongolite_get_collection_tree()` does a schema lookup on every operation.

```c
// Current: Linear search in linked list
wtree_tree_t* _mongolite_tree_cache_get(mongolite_db_t *db, const char *name);

// Optimization: Use hash map for O(1) lookup
// Consider uthash (already in project) or simple fixed-size hash table
```

#### 1.2 Reduce BSON Allocations in Update
**Operation:** Update
**Difficulty:** ★★☆☆☆
**Expected Gain:** 15-25%

`_apply_set`, `_apply_inc` create temporary BSON documents for each field modification.


#### 1.3 Direct _id Lookup for Update/Delete
**Operation:** Update, Delete
**Difficulty:** ★☆☆☆☆
**Expected Gain:** 20-30% for _id queries

Currently `mongolite_update_one` calls `mongolite_find_one` which handles all filter types. For `_id`-only filters, bypass the query engine.

```c
// Add fast path in mongolite_update_one:
if (_is_id_only_filter(filter)) {
    existing = _find_by_id_direct(db, collection, filter, error);
} else {
    existing = mongolite_find_one(...);
}
```

---

### Priority 2: Medium Impact, Medium Difficulty

#### 2.1 Batch Transaction Support
**Operation:** Insert, Update, Delete
**Difficulty:** ★★★☆☆
**Expected Gain:** 30-50% for bulk operations

`insert_many` already batches internally. Extend to update/delete.

```c
// New API:
int mongolite_update_many_batch(
    mongolite_db_t *db,
    const char *collection,
    const mongolite_update_op_t *ops,  // Array of {filter, update} pairs
    size_t n_ops,
    gerror_t *error
);
```

#### 2.2 Projection at Storage Level
**Operation:** Find
**Difficulty:** ★★☆☆☆
**Expected Gain:** 10-20% for large documents

Currently loads full document, then applies projection. For large documents with small projections, this wastes memory bandwidth.

```c
// Current flow:
doc = wtree_get_txn(...)           // Load full BSON
result = apply_projection(doc, projection)  // Filter fields

// Optimization: Parse BSON once, copy only needed fields
result = wtree_get_projected(tree, key, projection_fields, n_fields)
```

#### 2.3 Cursor-Based Pagination
**Operation:** Find with skip/limit
**Difficulty:** ★★☆☆☆
**Expected Gain:** 10x for deep pagination

`skip(5000)` currently scans and discards 5000 documents.

```c
// Current: O(skip + limit)
cursor->skip = 5000;  // Must iterate through 5000 docs

// Optimization: Keyset pagination
// Client provides last seen _id, query starts from there
cursor = mongolite_find_after(db, collection, last_id, filter, limit);
```

---

### Priority 3: High Impact, High Difficulty

#### 3.1 Secondary Indexes
**Operation:** Find, Update, Delete
**Difficulty:** ★★★★★
**Expected Gain:** 100x for indexed queries

Field scans are O(n). Indexes make them O(log n).

```c
// New API:
mongolite_create_index(db, "users", "{\"email\": 1}", &config, &error);

// Index storage: Separate LMDB tree per index
// Key: field_value + _id
// Value: (empty or _id reference)
```

**Implementation phases:**
1. Single-field ascending indexes
2. Compound indexes
3. Unique constraint enforcement
4. Index selection in query planner

#### 3.2 Query Plan Caching
**Operation:** Find
**Difficulty:** ★★★★☆
**Expected Gain:** 5-10%

For repeated queries with same structure, cache the execution plan.

```c
// Hash query structure (not values)
uint64_t plan_key = hash_query_structure(filter);
query_plan_t *plan = plan_cache_get(plan_key);
if (!plan) {
    plan = create_query_plan(filter, indexes);
    plan_cache_put(plan_key, plan);
}
```

#### 3.3 Write-Ahead Log (WAL) Mode
**Operation:** Insert, Update, Delete
**Difficulty:** ★★★★☆
**Expected Gain:** 50-100% for writes

LMDB is already ACID, but adding an explicit WAL could batch fsync calls.

```c
// Current: Each transaction fsyncs
// With WAL: Batch multiple operations, fsync periodically
```

---

### Priority 4: Future Considerations (Not Recommended Now)

#### 4.1 Multi-threaded Operations
**Difficulty:** ★★★★★
**Risk:** High complexity, potential for deadlocks

LMDB supports concurrent readers but single writer. Would require:
- Reader/writer lock redesign
- Per-collection locking
- Connection pooling

**Recommendation:** Defer until single-threaded performance is optimized.

#### 4.2 Memory-Mapped Document Cache
**Difficulty:** ★★★★☆

Cache frequently accessed documents in memory. Complex invalidation logic.

#### 4.3 Compression
**Difficulty:** ★★★☆☆

BSON is already compact, but could add optional zstd compression for large documents.

---

## Recommended Optimization Order

| Phase | Optimizations | Effort | Expected Total Gain |
|-------|--------------|--------|---------------------|
| **1** | 1.1 Tree Cache + 1.3 Direct _id | 2-3 days | 15-30% |
| **2** | 1.2 BSON Allocations + 2.3 Cursor Pagination | 3-5 days | 20-40% |
| **3** | 2.1 Batch Transactions + 2.2 Projection | 5-7 days | 30-50% |
| **4** | 3.1 Secondary Indexes | 2-3 weeks | 10-100x for indexed queries |

---

## Benchmark Comparison Targets

| Operation | Current | Target (Phase 2) | SQLite (reference) |
|-----------|---------|------------------|-------------------|
| InsertOne | 3.1k/s | 5k/s | 10-50k/s |
| FindById | 704k/s | 800k/s | 500k-1M/s |
| FindByField | 78k/s | 100k/s | 200k/s (indexed) |
| UpdateById | 3.3k/s | 5k/s | 10-30k/s |
| DeleteById | 2.7k/s | 4k/s | 10-30k/s |

---

## Compiler Optimizations

### Build Flags (CMakeLists.txt)

| Mode | Flags | Notes |
|------|-------|-------|
| Debug | `-g -O0` | Full debug info, no optimization |
| Release | `-O3 -march=native -flto -DNDEBUG` | Max optimization + LTO |

**Important:** `NDEBUG` must be defined for Google Benchmark to detect release mode.

### Compiler Hints (`macros.h`)

Include in source files:
```c
#include "macros.h"
```

#### Branch Prediction

```c
// Error paths (rarely taken)
MONGOLITE_CHECK(ptr != NULL) {
    return -1;
}

// Or explicit:
if (MONGOLITE_UNLIKELY(error)) {
    handle_error();
}

if (MONGOLITE_LIKELY(cache_hit)) {
    return cached_value;  // Fast path
}
```

#### Function Attributes

```c
// Pure function - no side effects, only reads
MONGOLITE_PURE
int bson_compare_docs(const bson_t *a, const bson_t *b);

// Returns newly allocated memory
MONGOLITE_MALLOC MONGOLITE_WARN_UNUSED
bson_t* mongolite_find_one(...);

// Hot path - optimize for speed
MONGOLITE_HOT
static int _find_by_id(mongolite_db_t *db, ...);

// Cold path - optimize for size
MONGOLITE_COLD MONGOLITE_NOINLINE
static void _handle_error(gerror_t *error, const char *msg);

// Critical inner loop
MONGOLITE_FLATTEN
static int _apply_operators(bson_t *doc, const bson_t *update);

// All pointer args non-null
MONGOLITE_NONNULL_ALL
int mongolite_insert_one(mongolite_db_t *db, const char *collection, ...);
```

#### Restrict Pointers

```c
// Tell compiler src and dst don't overlap
void copy_data(char * MONGOLITE_RESTRICT dst,
               const char * MONGOLITE_RESTRICT src,
               size_t len);
```

#### Prefetch

```c
// Prefetch next iteration's data
for (int i = 0; i < n; i++) {
    MONGOLITE_PREFETCH(&data[i + 4], 0, 3);  // Read, high locality
    process(data[i]);
}
```

### Where to Apply Hints

| Location | Hint | Reason |
|----------|------|--------|
| `_find_by_id` | `MONGOLITE_HOT` | Called frequently |
| `_is_id_query` | `MONGOLITE_PURE` | No side effects |
| Error paths | `MONGOLITE_UNLIKELY` | Rarely taken |
| `set_error` | `MONGOLITE_COLD` | Only on errors |
| `bson_compare_*` | `MONGOLITE_PURE` | Read-only comparison |
| `_mongolite_tree_cache_get` | `MONGOLITE_HOT` | Called every operation |

---

## Appendix: Raw Benchmark Data

See `benchmarks/` directory for full benchmark source code.

Run benchmarks:
```bash
make benchmark           # All benchmarks
make bench-insert        # Insert only
make bench-find          # Find only
make bench-update        # Update only
make bench-delete        # Delete only
```
