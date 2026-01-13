# Mongolite Production Readiness Plan

This document tracks the issues and improvements needed to make mongolite production-ready.

## Critical Bugs

### 1. [CRITICAL] Upsert Key Bug in `mongolite_update.c`

**Location:** Lines 279-282, 907-910

**Issue:** When upserting without `_id` in filter, the code uses `bson_get_data(new_doc)` as the key instead of the OID bytes:

```c
// WRONG - uses BSON data pointer as key
rc = wtree3_insert_one_txn(txn, tree,
                            bson_get_data(new_doc), sizeof(bson_oid_t),  // BUG!
                            bson_get_data(new_doc), new_doc->len,
                            error);

// CORRECT - should use OID bytes
rc = wtree3_insert_one_txn(txn, tree,
                            new_oid.bytes, sizeof(new_oid.bytes),
                            bson_get_data(new_doc), new_doc->len,
                            error);
```

**Impact:** Upsert operations without `_id` in filter will insert documents with corrupted keys.

**Fix:** Use `new_oid.bytes` as the key parameter.

**Status:** [x] FIXED (2026-01-13)

**Fix Applied:**
- Created `_ensure_doc_has_id()` helper function to consolidate _id handling
- Refactored `mongolite_update_one()` to use `_mongolite_update_merge` callback with `wtree3_upsert_txn()`
- Fixed `mongolite_find_and_modify()` to use `new_oid.bytes` as key instead of `bson_get_data(new_doc)`
- All 37 update tests pass

---

### 2. [CRITICAL] Race Condition in `mongolite_delete.c`

**Location:** Line 40

**Issue:** `mongolite_find_one()` called without holding the database lock:

```c
} else {
    doc_to_delete = mongolite_find_one(...);  // NO LOCK HELD!
    _mongolite_lock(db);                       // Lock acquired after
}
```

**Impact:** Document could be modified/deleted between find and delete operations.

**Fix:** Acquire lock before find_one or use atomic find-and-delete.

**Status:** [x] FIXED (2026-01-13)

**Fix Applied:**
- Moved `_mongolite_lock(db)` to beginning of function
- Created `_delete_find_one_scan()` helper that operates under existing lock
- All find-then-delete operations now happen atomically under lock
- All 17 delete tests pass

---

### 3. [CRITICAL] Non-OID `_id` Not Supported in `mongolite_find.c`

**Location:** Line 97

**Issue:** When `_id` is not an OID (e.g., string, int), the code generates a random OID:

```c
if (MONGOLITE_LIKELY(BSON_ITER_HOLDS_OID(&iter) && out_oid)) {
    bson_oid_copy(bson_iter_oid(&iter), out_oid);
} else {
    /* Non-OID _id - generate a hash or use as-is */
    bson_oid_init(out_oid, NULL);  // WRONG - generates random OID
}
```

**Impact:** Documents with non-OID `_id` cannot be found by `_id` lookup.

**Fix:** Support non-OID `_id` values or document the limitation clearly.

**Status:** [ ] Not Started

---

### 4. [HIGH] Transaction API Missing Error Parameter

**Location:** `mongolite_txn.c` lines 130-192

**Issue:** Public transaction functions lack `gerror_t` parameter:

```c
int mongolite_begin_transaction(mongolite_db_t *db);  // No error output
int mongolite_commit(mongolite_db_t *db);             // No error output
int mongolite_rollback(mongolite_db_t *db);           // No error output
```

**Impact:** Callers cannot diagnose transaction failures.

**Fix:** Add `gerror_t *error` parameter to all transaction functions.

**Status:** [ ] Not Started

---

## Update/Upsert Refactoring

### Current Problems

The update code has several issues:

1. **Code duplication** - Similar patterns repeated for:
   - `mongolite_update_one()` (lines 87-303)
   - `mongolite_update_many()` (lines 353-540)
   - `mongolite_find_and_modify()` (lines 793-937)

2. **Merge function usage is inconsistent** - `bson_merge_for_upsert()` is defined but only used in one place (line 178).

3. **Complex branching** - Multiple code paths for:
   - Has `_id` vs no `_id`
   - Upsert vs no upsert
   - Document exists vs doesn't exist

### Proposed Refactoring

wtree3 already provides excellent merge support via `wtree3_tree_set_merge_fn()` and `wtree3_upsert_txn()`. We should leverage this more consistently.

#### Step 1: Create unified BSON merge callback

```c
/**
 * Generic BSON document merge for MongoDB-style updates.
 * Handles: $set, $inc, $unset, etc. via bson_update_apply()
 */
typedef struct {
    const bson_t *update;      // Update operators
    const bson_t *filter;      // For building upsert base
    bool is_replace;           // true for replace_one (no operators)
    gerror_t *error;           // Error output
} mongolite_merge_ctx_t;

static void* _mongolite_bson_merge(
    const void *existing_value, size_t existing_len,
    const void *new_value, size_t new_len,
    void *user_data, size_t *out_len);
```

#### Step 2: Simplify update functions

Use wtree3's built-in upsert with merge:

```c
int mongolite_update_one(db, collection, filter, update, upsert, error) {
    // 1. Get _id (from filter or find)
    // 2. Set merge function on tree
    // 3. Call wtree3_upsert_txn() - handles insert/update automatically
    // 4. Clear merge function
}
```

#### Step 3: Remove duplicate code

Consolidate the "build document with _id" pattern that appears in 6+ places.

### Files to Modify

- [ ] `mongolite_update.c` - Main refactoring
- [ ] `mongolite_internal.h` - Add merge context structure
- [ ] `mongolite_helpers.c` - Add helper for "ensure _id in document"

---

## Missing Features

### 1. [HIGH] Projections Not Implemented

**Locations:**
- `mongolite_find.c:150, 164, 176` - "TODO: Apply projection"
- `mongolite_cursor.c:90` - "TODO: Apply projection"

**Current behavior:** Projection parameter accepted but ignored.

**Fix:** Implement `_apply_projection(bson_t *doc, const bson_t *projection)` helper.

**Status:** [ ] Not Started

---

### 2. [HIGH] Sort Not Applied

**Location:** `mongolite_cursor.c:292`

**Current behavior:** `mongolite_cursor_set_sort()` stores sort spec but never applies it.

**Fix:** Buffer results and sort before returning (note: memory implications for large result sets).

**Status:** [ ] Not Started

---

## Thread Safety Issues

### 1. Race Conditions in Update Operations

**Location:** Multiple functions in `mongolite_update.c`

**Issues:**
- `mongolite_update_one()`: Called `mongolite_find_one()` without lock when filter had no `_id`
- `mongolite_replace_one()`: Called `mongolite_find_one()` without lock
- `mongolite_find_and_modify()`: Called `mongolite_find_one()` without lock

**Impact:** Documents could be modified/deleted between find and update operations.

**Fix:** Moved lock acquisition to beginning of each function, created `_update_find_one_scan()` helper.

**Status:** [x] FIXED (2026-01-13)

**Fix Applied:**
- Created `_update_find_one_scan()` helper in `mongolite_update.c`
- Refactored `mongolite_update_one()` to acquire lock before any database operations
- Refactored `mongolite_replace_one()` to acquire lock at start and use `_mongolite_find_by_id()` or `_update_find_one_scan()` under lock
- Refactored `mongolite_find_and_modify()` to acquire lock at start
- All 37 update tests pass

---

### 2. Read Transaction Pool

**Location:** `mongolite_txn.c:53-81`

**Issue:** `read_txn_pool` accessed without guaranteed lock protection.

**Status:** [ ] Not Started

---

### 3. Index Cache Invalidation

**Location:** `mongolite_util.c:390-400`

**Issue:** Cache access patterns may have races.

**Status:** [ ] Not Started

---

## Code Quality Improvements

### 1. Standardize Error Library Names

Current inconsistency:
- "mongolite" - main library
- "bsonmatch" - should be "mongoc-matcher" or "libbson"
- "system" - for memory errors
- "lmdb" - for LMDB errors

### 2. Add Missing Tests

- [ ] Update with non-OID `_id`
- [ ] Concurrent update/delete operations
- [ ] Upsert without `_id` in filter
- [ ] Large result set sorting
- [ ] Projection edge cases

---

## Progress Tracking

| Category | Total | Fixed | Remaining |
|----------|-------|-------|-----------|
| Critical Bugs | 4 | 2 | 2 |
| High Priority | 3 | 0 | 3 |
| Thread Safety | 3 | 2 | 1 |
| Code Quality | 2 | 0 | 2 |

**Overall Status:** NOT PRODUCTION READY (but significant progress made)

---

## Next Steps

1. ~~Fix critical upsert key bug~~ ✓
2. ~~Fix delete race condition~~ ✓
3. ~~Fix update/replace/find_and_modify race conditions~~ ✓
4. Add `gerror_t` to transaction API
5. Implement projections
6. Implement sort
7. Handle non-OID `_id` values

---

---

## Change Log

### 2026-01-13
- **FIXED** Critical upsert key bug in `mongolite_update.c`
  - Created `_mongolite_ensure_doc_id()` helper in `mongolite_util.c`
  - Refactored `mongolite_update_one()` to use merge callback
  - Fixed `mongolite_find_and_modify()` key parameter
  - All 37 update tests pass

- **REFACTORED** Consolidated `_ensure_id` functions
  - Moved shared function to `mongolite_util.c` as `_mongolite_ensure_doc_id()`
  - Updated `mongolite_insert.c` and `mongolite_update.c` to use shared function
  - Reduces code duplication, improves maintainability

- **FIXED** Race condition in `mongolite_delete.c`
  - Moved lock acquisition to beginning of `mongolite_delete_one()`
  - Created `_delete_find_one_scan()` helper for atomic find-delete
  - All 17 delete tests pass

- **FIXED** Race conditions in update operations
  - Created `_update_find_one_scan()` helper in `mongolite_update.c`
  - Fixed `mongolite_update_one()` - lock now acquired before find
  - Fixed `mongolite_replace_one()` - lock now acquired before find
  - Fixed `mongolite_find_and_modify()` - lock now acquired before find
  - All operations now atomic under database lock
  - All 37 update tests pass

*Last Updated: 2026-01-13*
