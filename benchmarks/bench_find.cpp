/**
 * bench_find.cpp - Find/Query operation benchmarks for mongolite
 *
 * Benchmarks:
 * - BM_FindOneById: Direct _id lookup (optimized path)
 * - BM_FindOneByField: Field scan with filter
 * - BM_FindOneScan: Full collection scan
 * - BM_FindMany: Cursor iteration with varying result sizes
 * - BM_FindWithProjection: Find with field projection
 * - BM_FindWithSort: Find with sorting (requires buffering)
 * - BM_FindWithSkipLimit: Pagination patterns
 */

#include <benchmark/benchmark.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <string>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

extern "C" {
#include "mongolite.h"
}

#include "bench_docs.h"

// ============================================================
// Helper: Remove directory recursively
// ============================================================

static void remove_directory(const char* path) {
#ifdef _WIN32
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rmdir /s /q \"%s\" 2>nul", path);
    system(cmd);
#else
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf \"%s\" 2>/dev/null", path);
    system(cmd);
#endif
}

// ============================================================
// Fixture: Pre-populated database for find benchmarks
// ============================================================

class FindFixture : public benchmark::Fixture {
public:
    mongolite_db_t* db = nullptr;
    bench::DocumentGenerator generator;
    std::string db_path;
    gerror_t error;

    // Store some known IDs for lookup benchmarks
    std::vector<bson_oid_t> known_ids;
    std::vector<int64_t> known_ref_ids;

    static constexpr size_t COLLECTION_SIZE = 10000;

    void SetUp(const benchmark::State& state) override {
        (void)state;
        memset(&error, 0, sizeof(error));

        db_path = "./bench_find_db_" + std::to_string(rand());
        remove_directory(db_path.c_str());

        db_config_t config = {0};
        config.max_bytes = 1ULL * 1024 * 1024 * 1024;

        int rc = mongolite_open(db_path.c_str(), &db, &config, &error);
        if (rc != 0) {
            fprintf(stderr, "Failed to open database: %s\n", error.message);
            return;
        }

        rc = mongolite_collection_create(db, "bench", nullptr, &error);
        if (rc != 0 && rc != -1) {
            fprintf(stderr, "Failed to create collection: %s\n", error.message);
            return;
        }

        // Pre-populate collection
        generator.reset(42);
        known_ids.clear();
        known_ref_ids.clear();

        const size_t batch = 1000;
        for (size_t i = 0; i < COLLECTION_SIZE; i += batch) {
            size_t to_insert = std::min(batch, COLLECTION_SIZE - i);
            std::vector<bench::BenchDocument> docs = generator.generate_batch(to_insert);
            std::vector<bson_t*> bson_docs;

            for (const auto& doc : docs) {
                bson_docs.push_back(bench::bench_doc_to_bson(doc));
                // Store some ref_ids for lookup
                if (known_ref_ids.size() < 100) {
                    known_ref_ids.push_back(doc.id);
                }
            }

            bson_oid_t* ids = nullptr;
            rc = mongolite_insert_many(db, "bench",
                                       const_cast<const bson_t**>(bson_docs.data()),
                                       to_insert, &ids, &error);

            // Store some OIDs for _id lookup
            if (ids && known_ids.size() < 100) {
                for (size_t j = 0; j < to_insert && known_ids.size() < 100; ++j) {
                    known_ids.push_back(ids[j]);
                }
            }

            for (auto* b : bson_docs) bson_destroy(b);
            if (ids) free(ids);
        }

        // Reset generator for any additional doc generation
        generator.reset(12345);
    }

    void TearDown(const benchmark::State& state) override {
        (void)state;
        if (db) {
            mongolite_close(db);
            db = nullptr;
        }
        remove_directory(db_path.c_str());
    }
};

// ============================================================
// Benchmark: Find One by _id (optimized direct lookup)
// ============================================================

BENCHMARK_DEFINE_F(FindFixture, BM_FindOneById)(benchmark::State& state) {
    size_t idx = 0;

    for (auto _ : state) {
        // Cycle through known IDs
        const bson_oid_t& oid = known_ids[idx % known_ids.size()];
        idx++;

        // Build filter: {"_id": <oid>}
        bson_t* filter = bson_new();
        BSON_APPEND_OID(filter, "_id", &oid);

        bson_t* result = mongolite_find_one(db, "bench", filter, nullptr, &error);

        bson_destroy(filter);
        if (result) {
            bson_destroy(result);
        } else {
            state.SkipWithError("Find by _id returned null");
            break;
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(FindFixture, BM_FindOneById)
    ->Unit(benchmark::kMicrosecond);

// ============================================================
// Benchmark: Find One by indexed-like field (ref_id)
// ============================================================

BENCHMARK_DEFINE_F(FindFixture, BM_FindOneByRefId)(benchmark::State& state) {
    size_t idx = 0;

    for (auto _ : state) {
        int64_t ref_id = known_ref_ids[idx % known_ref_ids.size()];
        idx++;

        // Build filter: {"ref_id": <value>}
        bson_t* filter = bson_new();
        BSON_APPEND_INT64(filter, "ref_id", ref_id);

        bson_t* result = mongolite_find_one(db, "bench", filter, nullptr, &error);

        bson_destroy(filter);
        if (result) {
            bson_destroy(result);
        } else {
            state.SkipWithError("Find by ref_id returned null");
            break;
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(FindFixture, BM_FindOneByRefId)
    ->Unit(benchmark::kMicrosecond);

// ============================================================
// Benchmark: Find One with range filter (requires scan)
// ============================================================

BENCHMARK_DEFINE_F(FindFixture, BM_FindOneByRange)(benchmark::State& state) {
    int32_t age_threshold = 25;

    for (auto _ : state) {
        // Build filter: {"age": {"$gte": 25}}
        bson_t* filter = bson_new();
        bson_t child;
        BSON_APPEND_DOCUMENT_BEGIN(filter, "age", &child);
        BSON_APPEND_INT32(&child, "$gte", age_threshold);
        bson_append_document_end(filter, &child);

        bson_t* result = mongolite_find_one(db, "bench", filter, nullptr, &error);

        bson_destroy(filter);
        if (result) {
            bson_destroy(result);
        }
        // May be null if no match - that's ok for benchmark
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(FindFixture, BM_FindOneByRange)
    ->Unit(benchmark::kMicrosecond);

// ============================================================
// Benchmark: Find Many with cursor iteration
// ============================================================

BENCHMARK_DEFINE_F(FindFixture, BM_FindManyCursor)(benchmark::State& state) {
    const int64_t limit = state.range(0);

    for (auto _ : state) {
        // Find all with limit
        bson_t* filter = bson_new(); // empty = match all

        mongolite_cursor_t* cursor = mongolite_find(db, "bench", filter, nullptr, &error);
        bson_destroy(filter);

        if (!cursor) {
            state.SkipWithError("Find returned null cursor");
            break;
        }

        if (limit > 0) {
            mongolite_cursor_set_limit(cursor, limit);
        }

        // Iterate through results
        const bson_t* doc;
        int64_t count = 0;
        while (mongolite_cursor_next(cursor, &doc)) {
            count++;
            benchmark::DoNotOptimize(doc);
        }

        mongolite_cursor_destroy(cursor);

        state.counters["docs_read"] = static_cast<double>(count);
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(FindFixture, BM_FindManyCursor)
    ->Unit(benchmark::kMillisecond)
    ->Arg(100)    // 100 docs
    ->Arg(1000)   // 1000 docs
    ->Arg(10000); // all docs

// ============================================================
// Benchmark: Find with filter + cursor (varying selectivity)
// ============================================================

BENCHMARK_DEFINE_F(FindFixture, BM_FindWithFilter)(benchmark::State& state) {
    // Selectivity: 0=match ~12%, 1=match ~50%, 2=match ~100%
    const int selectivity = static_cast<int>(state.range(0));

    int32_t age_threshold;
    switch (selectivity) {
        case 0: age_threshold = 70; break;  // ~12% match (age 70-80)
        case 1: age_threshold = 50; break;  // ~50% match (age 50-80)
        default: age_threshold = 18; break; // ~100% match (age 18-80)
    }

    for (auto _ : state) {
        bson_t* filter = bson_new();
        bson_t child;
        BSON_APPEND_DOCUMENT_BEGIN(filter, "age", &child);
        BSON_APPEND_INT32(&child, "$gte", age_threshold);
        bson_append_document_end(filter, &child);

        mongolite_cursor_t* cursor = mongolite_find(db, "bench", filter, nullptr, &error);
        bson_destroy(filter);

        if (!cursor) {
            state.SkipWithError("Find with filter returned null cursor");
            break;
        }

        mongolite_cursor_set_limit(cursor, 100); // Limit to 100 results

        const bson_t* doc;
        int64_t count = 0;
        while (mongolite_cursor_next(cursor, &doc)) {
            count++;
            benchmark::DoNotOptimize(doc);
        }

        mongolite_cursor_destroy(cursor);
        state.counters["matches"] = static_cast<double>(count);
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(FindFixture, BM_FindWithFilter)
    ->Unit(benchmark::kMicrosecond)
    ->Arg(0)   // low selectivity (~12%)
    ->Arg(1)   // medium selectivity (~50%)
    ->Arg(2);  // high selectivity (~100%)

// ============================================================
// Benchmark: Find with projection (return fewer fields)
// ============================================================

BENCHMARK_DEFINE_F(FindFixture, BM_FindWithProjection)(benchmark::State& state) {
    for (auto _ : state) {
        bson_t* filter = bson_new();

        // Projection: only return name and email
        bson_t* projection = bson_new();
        BSON_APPEND_INT32(projection, "name", 1);
        BSON_APPEND_INT32(projection, "email", 1);

        mongolite_cursor_t* cursor = mongolite_find(db, "bench", filter, projection, &error);
        bson_destroy(filter);
        bson_destroy(projection);

        if (!cursor) {
            state.SkipWithError("Find with projection returned null cursor");
            break;
        }

        mongolite_cursor_set_limit(cursor, 100);

        const bson_t* doc;
        while (mongolite_cursor_next(cursor, &doc)) {
            benchmark::DoNotOptimize(doc);
        }

        mongolite_cursor_destroy(cursor);
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(FindFixture, BM_FindWithProjection)
    ->Unit(benchmark::kMicrosecond);

// ============================================================
// Benchmark: Find with sort (requires buffering all matches)
// ============================================================

BENCHMARK_DEFINE_F(FindFixture, BM_FindWithSort)(benchmark::State& state) {
    const int64_t limit = state.range(0);

    for (auto _ : state) {
        bson_t* filter = bson_new();

        // Sort by score descending
        bson_t* sort = bson_new();
        BSON_APPEND_INT32(sort, "score", -1);

        mongolite_cursor_t* cursor = mongolite_find(db, "bench", filter, nullptr, &error);
        bson_destroy(filter);

        if (!cursor) {
            bson_destroy(sort);
            state.SkipWithError("Find for sort returned null cursor");
            break;
        }

        mongolite_cursor_set_sort(cursor, sort);
        mongolite_cursor_set_limit(cursor, limit);
        bson_destroy(sort);

        const bson_t* doc;
        int64_t count = 0;
        while (mongolite_cursor_next(cursor, &doc)) {
            count++;
            benchmark::DoNotOptimize(doc);
        }

        mongolite_cursor_destroy(cursor);
        state.counters["sorted_docs"] = static_cast<double>(count);
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(FindFixture, BM_FindWithSort)
    ->Unit(benchmark::kMillisecond)
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000);

// ============================================================
// Benchmark: Find with skip/limit (pagination)
// ============================================================

BENCHMARK_DEFINE_F(FindFixture, BM_FindPagination)(benchmark::State& state) {
    const int64_t page_size = 100;
    const int64_t page_num = state.range(0);  // 0, 10, 50 = page number
    const int64_t skip = page_num * page_size;

    for (auto _ : state) {
        bson_t* filter = bson_new();

        mongolite_cursor_t* cursor = mongolite_find(db, "bench", filter, nullptr, &error);
        bson_destroy(filter);

        if (!cursor) {
            state.SkipWithError("Find for pagination returned null cursor");
            break;
        }

        mongolite_cursor_set_skip(cursor, skip);
        mongolite_cursor_set_limit(cursor, page_size);

        const bson_t* doc;
        int64_t count = 0;
        while (mongolite_cursor_next(cursor, &doc)) {
            count++;
            benchmark::DoNotOptimize(doc);
        }

        mongolite_cursor_destroy(cursor);
        state.counters["page"] = static_cast<double>(page_num);
        state.counters["docs"] = static_cast<double>(count);
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(FindFixture, BM_FindPagination)
    ->Unit(benchmark::kMicrosecond)
    ->Arg(0)    // first page
    ->Arg(10)   // page 10 (skip 1000)
    ->Arg(50);  // page 50 (skip 5000)

// ============================================================
// Benchmark: Find One JSON API
// ============================================================

BENCHMARK_DEFINE_F(FindFixture, BM_FindOneJson)(benchmark::State& state) {
    size_t idx = 0;

    for (auto _ : state) {
        int64_t ref_id = known_ref_ids[idx % known_ref_ids.size()];
        idx++;

        char filter_json[128];
        snprintf(filter_json, sizeof(filter_json), "{\"ref_id\": %" PRId64 "}", ref_id);

        const char* result = mongolite_find_one_json(db, "bench", filter_json, nullptr, &error);

        if (result) {
            benchmark::DoNotOptimize(result);
            // Note: result is owned by mongolite, don't free
        } else {
            state.SkipWithError("Find one JSON returned null");
            break;
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(FindFixture, BM_FindOneJson)
    ->Unit(benchmark::kMicrosecond);

// ============================================================
// Fixture: Pre-populated database with secondary index
// ============================================================

class IndexedFindFixture : public benchmark::Fixture {
public:
    mongolite_db_t* db = nullptr;
    bench::DocumentGenerator generator;
    std::string db_path;
    gerror_t error;

    std::vector<int64_t> known_ref_ids;
    std::vector<std::string> known_emails;

    static constexpr size_t COLLECTION_SIZE = 10000;

    void SetUp(const benchmark::State& state) override {
        (void)state;
        memset(&error, 0, sizeof(error));

        db_path = "./bench_indexed_find_db_" + std::to_string(rand());
        remove_directory(db_path.c_str());

        db_config_t config = {0};
        config.max_bytes = 1ULL * 1024 * 1024 * 1024;

        int rc = mongolite_open(db_path.c_str(), &db, &config, &error);
        if (rc != 0) {
            fprintf(stderr, "Failed to open database: %s\n", error.message);
            return;
        }

        rc = mongolite_collection_create(db, "bench", nullptr, &error);
        if (rc != 0 && rc != -1) {
            fprintf(stderr, "Failed to create collection: %s\n", error.message);
            return;
        }

        // Pre-populate collection
        generator.reset(42);
        known_ref_ids.clear();
        known_emails.clear();

        const size_t batch = 1000;
        for (size_t i = 0; i < COLLECTION_SIZE; i += batch) {
            size_t to_insert = std::min(batch, COLLECTION_SIZE - i);
            std::vector<bench::BenchDocument> docs = generator.generate_batch(to_insert);
            std::vector<bson_t*> bson_docs;

            for (const auto& doc : docs) {
                bson_docs.push_back(bench::bench_doc_to_bson(doc));
                if (known_ref_ids.size() < 100) {
                    known_ref_ids.push_back(doc.id);
                    known_emails.push_back(doc.email);
                }
            }

            bson_oid_t* ids = nullptr;
            mongolite_insert_many(db, "bench",
                                  const_cast<const bson_t**>(bson_docs.data()),
                                  to_insert, &ids, &error);

            for (auto* b : bson_docs) bson_destroy(b);
            if (ids) free(ids);
        }

        generator.reset(12345);
    }

    void TearDown(const benchmark::State& state) override {
        (void)state;
        if (db) {
            mongolite_close(db);
            db = nullptr;
        }
        remove_directory(db_path.c_str());
    }
};

// ============================================================
// Fixture: Pre-populated database with index on ref_id
// ============================================================

class IndexedRefIdFixture : public benchmark::Fixture {
public:
    mongolite_db_t* db = nullptr;
    bench::DocumentGenerator generator;
    std::string db_path;
    gerror_t error;
    std::vector<int64_t> known_ref_ids;

    static constexpr size_t COLLECTION_SIZE = 10000;

    void SetUp(const benchmark::State& state) override {
        (void)state;
        memset(&error, 0, sizeof(error));

        db_path = "./bench_indexed_refid_db_" + std::to_string(rand());
        remove_directory(db_path.c_str());

        db_config_t config = {0};
        config.max_bytes = 1ULL * 1024 * 1024 * 1024;

        int rc = mongolite_open(db_path.c_str(), &db, &config, &error);
        if (rc != 0) return;

        mongolite_collection_create(db, "bench", nullptr, &error);

        // Create index FIRST (empty collection is fast)
        bson_t* keys = bson_new();
        BSON_APPEND_INT32(keys, "ref_id", 1);
        mongolite_create_index(db, "bench", keys, "ref_id_1", nullptr, &error);
        bson_destroy(keys);

        // Then populate
        generator.reset(42);
        known_ref_ids.clear();

        const size_t batch = 1000;
        for (size_t i = 0; i < COLLECTION_SIZE; i += batch) {
            size_t to_insert = std::min(batch, COLLECTION_SIZE - i);
            std::vector<bench::BenchDocument> docs = generator.generate_batch(to_insert);
            std::vector<bson_t*> bson_docs;

            for (const auto& doc : docs) {
                bson_docs.push_back(bench::bench_doc_to_bson(doc));
                if (known_ref_ids.size() < 100) {
                    known_ref_ids.push_back(doc.id);
                }
            }

            bson_oid_t* ids = nullptr;
            mongolite_insert_many(db, "bench",
                                  const_cast<const bson_t**>(bson_docs.data()),
                                  to_insert, &ids, &error);

            for (auto* b : bson_docs) bson_destroy(b);
            if (ids) free(ids);
        }
    }

    void TearDown(const benchmark::State& state) override {
        (void)state;
        if (db) {
            mongolite_close(db);
            db = nullptr;
        }
        remove_directory(db_path.c_str());
    }
};

// ============================================================
// Benchmark: Find One by ref_id WITH index
// ============================================================

BENCHMARK_DEFINE_F(IndexedRefIdFixture, BM_FindOneByRefIdWithIndex)(benchmark::State& state) {
    size_t idx = 0;
    for (auto _ : state) {
        int64_t ref_id = known_ref_ids[idx % known_ref_ids.size()];
        idx++;

        bson_t* filter = bson_new();
        BSON_APPEND_INT64(filter, "ref_id", ref_id);

        bson_t* result = mongolite_find_one(db, "bench", filter, nullptr, &error);

        bson_destroy(filter);
        if (result) {
            bson_destroy(result);
        } else {
            state.SkipWithError("Find by ref_id with index returned null");
            break;
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(IndexedRefIdFixture, BM_FindOneByRefIdWithIndex)
    ->Unit(benchmark::kMicrosecond);

// ============================================================
// Benchmark: Find One by ref_id WITHOUT index (scan baseline)
// Uses IndexedFindFixture which has no index
// ============================================================

BENCHMARK_DEFINE_F(IndexedFindFixture, BM_FindOneByRefIdNoIndex)(benchmark::State& state) {
    size_t idx = 0;
    for (auto _ : state) {
        int64_t ref_id = known_ref_ids[idx % known_ref_ids.size()];
        idx++;

        bson_t* filter = bson_new();
        BSON_APPEND_INT64(filter, "ref_id", ref_id);

        bson_t* result = mongolite_find_one(db, "bench", filter, nullptr, &error);

        bson_destroy(filter);
        if (result) {
            bson_destroy(result);
        } else {
            state.SkipWithError("Find by ref_id (scan) returned null");
            break;
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(IndexedFindFixture, BM_FindOneByRefIdNoIndex)
    ->Unit(benchmark::kMicrosecond);

// ============================================================
// Benchmark: Index vs Scan at different collection sizes
// ============================================================

class ScaleFindFixture : public benchmark::Fixture {
public:
    mongolite_db_t* db = nullptr;
    bench::DocumentGenerator generator;
    std::string db_path;
    gerror_t error;
    std::vector<int64_t> known_ref_ids;

    void SetUp(const benchmark::State& state) override {
        memset(&error, 0, sizeof(error));
        db_path = "./bench_scale_find_db_" + std::to_string(rand());
        remove_directory(db_path.c_str());

        db_config_t config = {0};
        config.max_bytes = 1ULL * 1024 * 1024 * 1024;

        mongolite_open(db_path.c_str(), &db, &config, &error);
        mongolite_collection_create(db, "bench", nullptr, &error);
        generator.reset(42);
        known_ref_ids.clear();
    }

    void TearDown(const benchmark::State& state) override {
        (void)state;
        if (db) {
            mongolite_close(db);
            db = nullptr;
        }
        remove_directory(db_path.c_str());
    }

    void populate(size_t count) {
        const size_t batch = 1000;
        for (size_t i = 0; i < count; i += batch) {
            size_t to_insert = std::min(batch, count - i);
            std::vector<bench::BenchDocument> docs = generator.generate_batch(to_insert);
            std::vector<bson_t*> bson_docs;

            for (const auto& doc : docs) {
                bson_docs.push_back(bench::bench_doc_to_bson(doc));
                if (known_ref_ids.size() < 100) {
                    known_ref_ids.push_back(doc.id);
                }
            }

            bson_oid_t* ids = nullptr;
            mongolite_insert_many(db, "bench",
                                  const_cast<const bson_t**>(bson_docs.data()),
                                  to_insert, &ids, &error);
            for (auto* b : bson_docs) bson_destroy(b);
            if (ids) free(ids);
        }
    }
};

BENCHMARK_DEFINE_F(ScaleFindFixture, BM_FindIndexVsScanAtScale)(benchmark::State& state) {
    const size_t collection_size = static_cast<size_t>(state.range(0));
    const bool use_index = static_cast<bool>(state.range(1));

    // Create index FIRST on empty collection (fast), then populate
    if (use_index) {
        bson_t* keys = bson_new();
        BSON_APPEND_INT32(keys, "ref_id", 1);
        mongolite_create_index(db, "bench", keys, "ref_id_1", nullptr, &error);
        bson_destroy(keys);
    }

    populate(collection_size);

    size_t idx = 0;
    for (auto _ : state) {
        int64_t ref_id = known_ref_ids[idx % known_ref_ids.size()];
        idx++;

        bson_t* filter = bson_new();
        BSON_APPEND_INT64(filter, "ref_id", ref_id);

        bson_t* result = mongolite_find_one(db, "bench", filter, nullptr, &error);

        bson_destroy(filter);
        if (result) {
            bson_destroy(result);
        }
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["collection_size"] = static_cast<double>(collection_size);
    state.counters["indexed"] = use_index ? 1.0 : 0.0;
}

BENCHMARK_REGISTER_F(ScaleFindFixture, BM_FindIndexVsScanAtScale)
    ->Unit(benchmark::kMicrosecond)
    ->Args({1000, 0})    // 1K docs, no index (scan)
    ->Args({1000, 1})    // 1K docs, with index
    ->Args({10000, 0})   // 10K docs, no index (scan)
    ->Args({10000, 1})   // 10K docs, with index
    ->Args({50000, 0})   // 50K docs, no index (scan)
    ->Args({50000, 1})   // 50K docs, with index
    ->Iterations(500);

// ============================================================
// Main
// ============================================================

BENCHMARK_MAIN();
