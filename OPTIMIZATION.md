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

#### 1.1 Read Transaction Pooling ✅ COMPLETED
**Operation:** Find (all read operations)
**Difficulty:** ★★☆☆☆
**Actual Gain:** 42% for FindOneById, 27% for FindOneByRange

Implemented using `wtree_txn_reset/renew` to pool read transactions.

**Implementation:** `src/mongolite_txn.c` - `_mongolite_get_read_txn()`, `_mongolite_release_read_txn()`

**Benchmark Results (Linux, 12 cores @ 4.5GHz):**
| Operation | Before | After | Improvement |
|-----------|--------|-------|-------------|
| FindOneById | 0.708μs | 0.410μs | **42% faster** |
| FindOneByRange | 1.09μs | 0.794μs | **27% faster** |

---

#### 1.1b Doc Count Caching ✅ COMPLETED
**Operation:** Insert, Delete, Collection Count
**Difficulty:** ★☆☆☆☆
**Expected Gain:** Reduces BSON parsing overhead on every insert/delete

Cached `doc_count` in tree cache entry to avoid reading/parsing schema on every doc count update.

**Implementation:**
- `mongolite_tree_cache_entry_t.doc_count` - cached count
- `_mongolite_tree_cache_update_doc_count()` - delta update
- `_mongolite_tree_cache_get_doc_count()` - fast retrieval
- `mongolite_collection_count()` - uses cache first
- `_mongolite_update_doc_count_txn()` - updates cache + schema

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

#### 1.2 BSON Update Module Extraction ✅ COMPLETED
**Operation:** Update
**Difficulty:** ★★☆☆☆
**Actual Gain:** Code organization + testability (no perf change expected)

Extracted update operators into standalone `bson_update.c` module for:
- Independent unit testing
- Reusability outside mongolite
- Isolated benchmarking of operator performance

**Implementation:**
- `src/bson_update.h` - Public API with compiler hints
- `src/bson_update.c` - Pure functions for $set, $unset, $inc, $push, $pull, $rename
- `tests/test_bson_update.c` - 18 unit tests
- `benchmarks/bench_bson_update.c` - Standalone operator benchmarks

**Operator Benchmark Results (Linux, 12 cores @ 4.5GHz, O(n) optimized):**
| Operator | Throughput | Latency |
|----------|------------|---------|
| $set (single field) | 2.35M/s | 425 ns |
| $set (3 fields) | 2.34M/s | 428 ns |
| $inc | 2.64M/s | 378 ns |
| $unset | 2.84M/s | 353 ns |
| $push (10-elem) | 881k/s | 1.14 μs |
| $pull (10-elem) | 611k/s | 1.64 μs |
| $rename | 2.02M/s | 494 ns |
| Combined | 847k/s | 1.18 μs |

**Document Size Scaling (single field $set):**
| Doc Size | Latency |
|----------|---------|
| 5 fields | 442 ns |
| 20 fields | 1.40 μs |
| 50 fields | 3.32 μs |
| 100 fields | 6.24 μs |

**Multi-field $set Scaling (O(n) optimized):**
| Update | Latency |
|--------|---------|
| 5/50 fields | 3.90 μs |
| 10/50 fields | 4.93 μs |
| 10/100 fields | 8.85 μs |

---

#### 1.2b Reduce BSON Allocations in Update ✅ COMPLETED
**Operation:** Update
**Difficulty:** ★★★☆☆
**Actual Gain:** 2.5-7x faster for multi-field updates

Replaced O(n*m) per-field document rebuilding with O(n) single-pass rebuilding.

**Before:** For `$set {a:1, b:2, c:3}` on 50-field doc = 4 complete document copies
**After:** Single pass through document, replacing/adding fields in one allocation

**Implementation:**
- `bson_update_apply_set()` - Single-pass rebuild with field array
- `bson_update_apply_inc()` - Single-pass rebuild with increment array
- Stack-allocated field arrays (max 64 fields) to avoid heap allocation

**Benchmark Results:**
| Test Case | Before | After | Speedup |
|-----------|--------|-------|---------|
| $set 3 fields (5-field doc) | 1,206 ns | 428 ns | **2.8x faster** |
| $set 5/50 fields | 17,990 ns | 3,900 ns | **4.6x faster** |
| $set 10/50 fields | 32,972 ns | 4,927 ns | **6.7x faster** |
| $set 10/100 fields | 61,421 ns | 8,845 ns | **6.9x faster** |

---

#### 1.3 Direct _id Lookup for Update/Delete ✅ COMPLETED
**Operation:** Update, Delete
**Difficulty:** ★☆☆☆☆
**Actual Gain:** ~18% for UpdateOneById, ~15% for DeleteOneById

Optimized `mongolite_update_one` and `mongolite_delete_one` to use direct _id lookup via `_mongolite_is_id_query()` and `_mongolite_find_by_id()`.

**Implementation:**
- `src/mongolite_find.c` - Exposed `_mongolite_is_id_query()` and `_mongolite_find_by_id()`
- `src/mongolite_update.c` - Fast path for _id queries
- `src/mongolite_delete.c` - Fast path for _id queries

**Benchmark Results:**
| Operation | Before | After | Improvement |
|-----------|--------|-------|-------------|
| UpdateOneById | 307μs | ~260μs | **~18% faster** |
| DeleteOneById | 375μs | ~320μs | **~15% faster** |

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

**Files with macros.h included:**
- `src/mongolite_find.c` ✅
- `src/mongolite_txn.c` ✅
- `src/bson_update.c` ✅ (via bson_update.h)
- `src/mongolite_update.c` ✅
- `src/mongolite_delete.c` ✅
- `src/mongolite_insert.c` ✅

| Location | Hint | Status |
|----------|------|--------|
| `_find_by_id` | `MONGOLITE_HOT` | ✅ Applied |
| `_is_id_query` | `MONGOLITE_HOT` | ✅ Applied |
| `_mongolite_get_read_txn` | `MONGOLITE_HOT` | ✅ Applied |
| `_mongolite_get_write_txn` | `MONGOLITE_HOT` | ✅ Applied |
| `_mongolite_release_read_txn` | `MONGOLITE_HOT` | ✅ Applied |
| `bson_update_apply` | `MONGOLITE_HOT` | ✅ Applied |
| `bson_update_apply_set` | `MONGOLITE_HOT` | ✅ Applied |
| `bson_update_apply_inc` | `MONGOLITE_HOT` | ✅ Applied |
| `mongolite_update_one` | `MONGOLITE_HOT` | ✅ Applied |
| `mongolite_update_many` | `MONGOLITE_HOT` | ✅ Applied |
| `mongolite_delete_one` | `MONGOLITE_HOT` | ✅ Applied |
| `mongolite_delete_many` | `MONGOLITE_HOT` | ✅ Applied |
| `mongolite_insert_one` | `MONGOLITE_HOT` | ✅ Applied |
| `mongolite_insert_many` | `MONGOLITE_HOT` | ✅ Applied |
| `_ensure_id` (insert) | `MONGOLITE_HOT` | ✅ Applied |
| Error paths | `MONGOLITE_UNLIKELY` | ✅ Applied |
| `_mongolite_tree_cache_get` | `MONGOLITE_HOT` | Pending |
| `set_error` | `MONGOLITE_COLD` | Pending |

---

### Phase 1.4: Extend Macros to Other mongolite_*.c Files ✅ PARTIALLY COMPLETED

**High priority files completed:**

| File | Priority | Macros Added | Status |
|------|----------|--------------|--------|
| `mongolite_update.c` | High | `MONGOLITE_HOT`, `LIKELY/UNLIKELY` | ✅ Completed |
| `mongolite_delete.c` | High | `MONGOLITE_HOT`, `LIKELY/UNLIKELY` | ✅ Completed |
| `mongolite_insert.c` | High | `MONGOLITE_HOT`, `LIKELY/UNLIKELY` | ✅ Completed |
| `mongolite_cursor.c` | Medium | `LIKELY/UNLIKELY` | Pending |
| `mongolite_collection.c` | Medium | `UNLIKELY` | Pending |
| `mongolite_db.c` | Low | `UNLIKELY` | Pending |
| `mongolite_schema.c` | Low | `UNLIKELY` | Pending |
| `mongolite_util.c` | Low | None needed | N/A |

**Macros applied to high-priority files:**
- `MONGOLITE_HOT` on main API functions (`mongolite_update_one`, `mongolite_update_many`, `mongolite_delete_one`, `mongolite_delete_many`, `mongolite_insert_one`, `mongolite_insert_many`)
- `MONGOLITE_UNLIKELY` on all error paths (NULL checks, allocation failures, transaction errors)
- `MONGOLITE_LIKELY` on success paths (cursor iteration, _id lookups, successful operations)

**Conservative Macro Guidelines:**

1. **SAFE to use liberally:**
   - `MONGOLITE_LIKELY/UNLIKELY` - Branch hints, no correctness impact
   - `MONGOLITE_HOT` - Optimization hint only
   - `MONGOLITE_COLD` - Optimization hint only

2. **USE WITH CAUTION:**
   - `MONGOLITE_WARN_UNUSED` - Safe but may cause compiler warnings
   - `MONGOLITE_ALWAYS_INLINE` - Can increase code size, use sparingly

3. **AVOID unless certain:**
   - `MONGOLITE_PURE` - Compiler may cache results; function must have NO side effects
   - `MONGOLITE_NONNULL` - Compiler may optimize away NULL checks; dangerous if caller might pass NULL
   - `MONGOLITE_RESTRICT` - Undefined behavior if pointers overlap

**Pattern for applying macros:**

```c
// Error paths - always unlikely
if (MONGOLITE_UNLIKELY(!ptr)) {
    set_error(error, ...);
    return -1;
}

// Success paths - usually likely
if (MONGOLITE_LIKELY(result == 0)) {
    return success;
}

// Hot functions - add to declaration
MONGOLITE_HOT
static int _inner_loop_function(...);
```

---

## Future Optimizations

### Phase 2: System Memory Optimizations (Linux-specific)

Low-level memory optimizations using Linux system calls. These should be wrapped with `#ifdef __linux__` for portability.

#### 2.1 Memory Advise (`madvise`)

Hint to the kernel about memory access patterns:

```c
#ifdef __linux__
#include <sys/mman.h>

// For sequential scan (cursor iteration)
madvise(data, size, MADV_SEQUENTIAL);

// For random access (B-tree lookups)
madvise(data, size, MADV_RANDOM);

// Pre-fault pages before heavy use
madvise(data, size, MADV_WILLNEED);

// Release pages after batch operations
madvise(data, size, MADV_DONTNEED);
#endif
```

**Where to apply:**
| Location | Hint | Rationale |
|----------|------|-----------|
| Cursor full scan | `MADV_SEQUENTIAL` | Linear iteration through documents |
| B-tree node access | `MADV_RANDOM` | Random key lookups |
| Database open | `MADV_WILLNEED` | Pre-fault hot pages |
| After bulk delete | `MADV_DONTNEED` | Release freed pages |

#### 2.2 Memory Locking (`mlock`)

Pin critical data in RAM to prevent swapping:

```c
#ifdef __linux__
#include <sys/mman.h>

// Lock B-tree root nodes in memory
mlock(root_node, node_size);

// Lock frequently accessed metadata
mlock(metadata, sizeof(metadata));

// Unlock when done
munlock(data, size);
#endif
```

**Candidates for mlock:**
- B-tree root nodes (prevent swap during lookups)
- Collection metadata cache
- Hot document cache entries

**Caution:** `mlock` requires `CAP_IPC_LOCK` or appropriate `ulimit`. Consider making this opt-in via configuration.

#### 2.3 Huge Pages (`madvise` + `MAP_HUGETLB`)

Reduce TLB misses for large datasets:

```c
#ifdef __linux__
// Advise kernel to use transparent huge pages
madvise(data, size, MADV_HUGEPAGE);

// Or allocate with explicit huge pages (requires setup)
void *ptr = mmap(NULL, size, PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
#endif
```

**Where to apply:**
- Large LMDB memory maps (>2MB)
- Document cache buffers

#### 2.4 Memory Prefetching (`__builtin_prefetch`)

Already in `macros.h` as `MONGOLITE_PREFETCH`. Use for:

```c
// Prefetch next B-tree node during traversal
MONGOLITE_PREFETCH(next_node, 0, 3);  // read, high locality

// Prefetch document data before processing
MONGOLITE_PREFETCH(doc_data, 0, 1);   // read, low locality
```

#### 2.5 `posix_fadvise` for File I/O

Hint to kernel about file access patterns (LMDB already uses this internally):

```c
#ifdef __linux__
#include <fcntl.h>

// Sequential read
posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);

// Random access
posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM);

// Will need soon
posix_fadvise(fd, offset, length, POSIX_FADV_WILLNEED);
#endif
```

#### 2.6 Implementation Strategy

1. **Create `src/mem_hints.h`** with portable wrappers:

```c
#ifndef MEM_HINTS_H
#define MEM_HINTS_H

#ifdef __linux__
#include <sys/mman.h>

#define MEM_HINT_SEQUENTIAL(p, sz)  madvise((p), (sz), MADV_SEQUENTIAL)
#define MEM_HINT_RANDOM(p, sz)      madvise((p), (sz), MADV_RANDOM)
#define MEM_HINT_WILLNEED(p, sz)    madvise((p), (sz), MADV_WILLNEED)
#define MEM_HINT_DONTNEED(p, sz)    madvise((p), (sz), MADV_DONTNEED)
#define MEM_LOCK(p, sz)             mlock((p), (sz))
#define MEM_UNLOCK(p, sz)           munlock((p), (sz))

#else
// No-op on other platforms
#define MEM_HINT_SEQUENTIAL(p, sz)  ((void)0)
#define MEM_HINT_RANDOM(p, sz)      ((void)0)
#define MEM_HINT_WILLNEED(p, sz)    ((void)0)
#define MEM_HINT_DONTNEED(p, sz)    ((void)0)
#define MEM_LOCK(p, sz)             ((void)0)
#define MEM_UNLOCK(p, sz)           ((void)0)

#endif

#endif /* MEM_HINTS_H */
```

2. **Add CMake option:**

```cmake
option(ENABLE_MEM_HINTS "Enable Linux memory hints (madvise/mlock)" ON)

if(ENABLE_MEM_HINTS AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_compile_definitions(MONGOLITE_MEM_HINTS)
endif()
```

#### 2.7 Priority Order

| Optimization | Impact | Complexity | Priority |
|--------------|--------|------------|----------|
| `MADV_SEQUENTIAL` for cursors | Medium | Low | High |
| `MADV_WILLNEED` on db open | Medium | Low | High |
| `__builtin_prefetch` in loops | Low-Medium | Low | Medium |
| `mlock` for hot pages | Medium | Medium | Medium |
| Huge pages | High (large data) | High | Low |

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
make bench-bson          # BSON update operators (standalone C)
```
