/**
 * bench_delete.cpp - Delete operation benchmarks for mongolite
 *
 * Benchmarks:
 * - BM_DeleteOneById: Delete by _id (direct lookup)
 * - BM_DeleteOneByField: Delete by field (requires scan)
 * - BM_DeleteMany: Delete multiple documents
 * - BM_DeleteAndReinsert: Delete/insert cycle (simulates churn)
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
// Fixture: Pre-populated database for delete benchmarks
// Each iteration needs fresh data, so we repopulate between runs
// ============================================================

class DeleteFixture : public benchmark::Fixture {
public:
    mongolite_db_t* db = nullptr;
    bench::DocumentGenerator generator;
    std::string db_path;
    gerror_t error;

    std::vector<bson_oid_t> known_ids;
    std::vector<int64_t> known_ref_ids;

    size_t delete_index = 0;
    static constexpr size_t COLLECTION_SIZE = 10000;

    void SetUp(const benchmark::State& state) override {
        (void)state;
        memset(&error, 0, sizeof(error));

        db_path = "./bench_delete_db_" + std::to_string(rand());
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

        populate_collection();
    }

    void TearDown(const benchmark::State& state) override {
        (void)state;
        if (db) {
            mongolite_close(db);
            db = nullptr;
        }
        remove_directory(db_path.c_str());
    }

    void populate_collection() {
        generator.reset(42);
        known_ids.clear();
        known_ref_ids.clear();
        delete_index = 0;

        const size_t batch = 1000;
        for (size_t i = 0; i < COLLECTION_SIZE; i += batch) {
            size_t to_insert = std::min(batch, COLLECTION_SIZE - i);
            std::vector<bench::BenchDocument> docs = generator.generate_batch(to_insert);
            std::vector<bson_t*> bson_docs;

            for (const auto& doc : docs) {
                bson_docs.push_back(bench::bench_doc_to_bson(doc));
                known_ref_ids.push_back(doc.id);
            }

            bson_oid_t* ids = nullptr;
            int rc = mongolite_insert_many(db, "bench",
                                           const_cast<const bson_t**>(bson_docs.data()),
                                           to_insert, &ids, &error);

            if (ids) {
                for (size_t j = 0; j < to_insert; ++j) {
                    known_ids.push_back(ids[j]);
                }
                free(ids);
            }

            for (auto* b : bson_docs) bson_destroy(b);

            if (rc < 0) break;
        }

        generator.reset(99999);
    }

    // Get next ID to delete (cycles through, repopulates when exhausted)
    bool get_next_delete_target(bson_oid_t* oid, int64_t* ref_id) {
        if (delete_index >= known_ids.size()) {
            // Exhausted all IDs - this shouldn't happen in a well-sized benchmark
            return false;
        }
        *oid = known_ids[delete_index];
        *ref_id = known_ref_ids[delete_index];
        delete_index++;
        return true;
    }
};

// ============================================================
// Benchmark: Delete One by _id
// ============================================================

BENCHMARK_DEFINE_F(DeleteFixture, BM_DeleteOneById)(benchmark::State& state) {
    for (auto _ : state) {
        bson_oid_t oid;
        int64_t ref_id;

        if (!get_next_delete_target(&oid, &ref_id)) {
            state.SkipWithError("Ran out of documents to delete");
            break;
        }

        bson_t* filter = bson_new();
        BSON_APPEND_OID(filter, "_id", &oid);

        int rc = mongolite_delete_one(db, "bench", filter, &error);

        bson_destroy(filter);

        if (rc < 0) {
            state.SkipWithError("Delete one by _id failed");
            break;
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(DeleteFixture, BM_DeleteOneById)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(5000);  // Limited to avoid exhausting documents

// ============================================================
// Benchmark: Delete One by field (requires scan)
// ============================================================

BENCHMARK_DEFINE_F(DeleteFixture, BM_DeleteOneByField)(benchmark::State& state) {
    for (auto _ : state) {
        bson_oid_t oid;
        int64_t ref_id;

        if (!get_next_delete_target(&oid, &ref_id)) {
            state.SkipWithError("Ran out of documents to delete");
            break;
        }

        bson_t* filter = bson_new();
        BSON_APPEND_INT64(filter, "ref_id", ref_id);

        int rc = mongolite_delete_one(db, "bench", filter, &error);

        bson_destroy(filter);

        if (rc < 0) {
            state.SkipWithError("Delete one by field failed");
            break;
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(DeleteFixture, BM_DeleteOneByField)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(5000);

// ============================================================
// Fixture for DeleteMany: Needs repopulation between iterations
// ============================================================

class DeleteManyFixture : public benchmark::Fixture {
public:
    mongolite_db_t* db = nullptr;
    bench::DocumentGenerator generator;
    std::string db_path;
    gerror_t error;

    static constexpr size_t COLLECTION_SIZE = 10000;

    void SetUp(const benchmark::State& state) override {
        (void)state;
        memset(&error, 0, sizeof(error));

        db_path = "./bench_delete_many_db_" + std::to_string(rand());
        remove_directory(db_path.c_str());

        db_config_t config = {0};
        config.max_bytes = 1ULL * 1024 * 1024 * 1024;

        int rc = mongolite_open(db_path.c_str(), &db, &config, &error);
        if (rc != 0) {
            fprintf(stderr, "Failed to open database: %s\n", error.message);
            return;
        }

        generator.reset(42);
    }

    void TearDown(const benchmark::State& state) override {
        (void)state;
        if (db) {
            mongolite_close(db);
            db = nullptr;
        }
        remove_directory(db_path.c_str());
    }

    void populate_collection(const char* collection_name) {
        mongolite_collection_create(db, collection_name, nullptr, &error);

        generator.reset(42);
        const size_t batch = 1000;
        for (size_t i = 0; i < COLLECTION_SIZE; i += batch) {
            size_t to_insert = std::min(batch, COLLECTION_SIZE - i);
            std::vector<bench::BenchDocument> docs = generator.generate_batch(to_insert);
            std::vector<bson_t*> bson_docs;

            for (const auto& doc : docs) {
                bson_docs.push_back(bench::bench_doc_to_bson(doc));
            }

            bson_oid_t* ids = nullptr;
            mongolite_insert_many(db, collection_name,
                                  const_cast<const bson_t**>(bson_docs.data()),
                                  to_insert, &ids, &error);

            for (auto* b : bson_docs) bson_destroy(b);
            if (ids) free(ids);
        }
    }
};

// ============================================================
// Benchmark: Delete Many (by department ~12.5% each)
// ============================================================

BENCHMARK_DEFINE_F(DeleteManyFixture, BM_DeleteMany)(benchmark::State& state) {
    const char* departments[] = {"engineering", "sales", "marketing", "support",
                                  "finance", "hr", "operations", "legal"};
    size_t dept_idx = 0;

    for (auto _ : state) {
        // Repopulate for each iteration
        state.PauseTiming();
        char coll_name[64];
        snprintf(coll_name, sizeof(coll_name), "bench_%zu", dept_idx);
        populate_collection(coll_name);
        state.ResumeTiming();

        // Delete all docs in one department (~12.5%)
        const char* target_dept = departments[dept_idx % 8];
        dept_idx++;

        bson_t* filter = bson_new();
        BSON_APPEND_UTF8(filter, "department", target_dept);

        int64_t deleted_count = 0;
        int rc = mongolite_delete_many(db, coll_name, filter, &deleted_count, &error);

        bson_destroy(filter);

        if (rc < 0) {
            state.SkipWithError("Delete many failed");
            break;
        }

        state.counters["deleted"] = static_cast<double>(deleted_count);

        // Cleanup collection
        state.PauseTiming();
        mongolite_collection_drop(db, coll_name, &error);
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(DeleteManyFixture, BM_DeleteMany)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(10);

// ============================================================
// Benchmark: Delete Many with varying selectivity
// ============================================================

BENCHMARK_DEFINE_F(DeleteManyFixture, BM_DeleteManySelectivity)(benchmark::State& state) {
    // 0 = ~12%, 1 = ~50%, 2 = ~100%
    const int selectivity = static_cast<int>(state.range(0));

    int32_t age_threshold;
    switch (selectivity) {
        case 0: age_threshold = 70; break;  // ~16% (age 70-80 out of 18-80)
        case 1: age_threshold = 50; break;  // ~48% (age 50-80)
        default: age_threshold = 18; break; // ~100% (all ages)
    }

    size_t iter = 0;
    for (auto _ : state) {
        state.PauseTiming();
        char coll_name[64];
        snprintf(coll_name, sizeof(coll_name), "bench_sel_%zu", iter++);
        populate_collection(coll_name);
        state.ResumeTiming();

        // Filter: {"age": {"$gte": threshold}}
        bson_t* filter = bson_new();
        bson_t child;
        BSON_APPEND_DOCUMENT_BEGIN(filter, "age", &child);
        BSON_APPEND_INT32(&child, "$gte", age_threshold);
        bson_append_document_end(filter, &child);

        int64_t deleted_count = 0;
        int rc = mongolite_delete_many(db, coll_name, filter, &deleted_count, &error);

        bson_destroy(filter);

        if (rc < 0) {
            state.SkipWithError("Delete many selectivity failed");
            break;
        }

        state.counters["deleted"] = static_cast<double>(deleted_count);

        state.PauseTiming();
        mongolite_collection_drop(db, coll_name, &error);
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(DeleteManyFixture, BM_DeleteManySelectivity)
    ->Unit(benchmark::kMillisecond)
    ->Arg(0)   // ~16%
    ->Arg(1)   // ~48%
    ->Arg(2)   // ~100%
    ->Iterations(5);

// ============================================================
// Fixture for churn test (delete + reinsert cycle)
// ============================================================

class ChurnFixture : public benchmark::Fixture {
public:
    mongolite_db_t* db = nullptr;
    bench::DocumentGenerator generator;
    std::string db_path;
    gerror_t error;

    std::vector<bson_oid_t> known_ids;
    size_t churn_index = 0;

    static constexpr size_t COLLECTION_SIZE = 5000;

    void SetUp(const benchmark::State& state) override {
        (void)state;
        memset(&error, 0, sizeof(error));

        db_path = "./bench_churn_db_" + std::to_string(rand());
        remove_directory(db_path.c_str());

        db_config_t config = {0};
        config.max_bytes = 1ULL * 1024 * 1024 * 1024;

        int rc = mongolite_open(db_path.c_str(), &db, &config, &error);
        if (rc != 0) return;

        mongolite_collection_create(db, "bench", nullptr, &error);

        // Populate
        generator.reset(42);
        known_ids.clear();

        const size_t batch = 500;
        for (size_t i = 0; i < COLLECTION_SIZE; i += batch) {
            size_t to_insert = std::min(batch, COLLECTION_SIZE - i);
            std::vector<bench::BenchDocument> docs = generator.generate_batch(to_insert);
            std::vector<bson_t*> bson_docs;

            for (const auto& doc : docs) {
                bson_docs.push_back(bench::bench_doc_to_bson(doc));
            }

            bson_oid_t* ids = nullptr;
            mongolite_insert_many(db, "bench",
                                  const_cast<const bson_t**>(bson_docs.data()),
                                  to_insert, &ids, &error);

            if (ids) {
                for (size_t j = 0; j < to_insert; ++j) {
                    known_ids.push_back(ids[j]);
                }
                free(ids);
            }

            for (auto* b : bson_docs) bson_destroy(b);
        }

        generator.reset(99999);
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
// Benchmark: Delete + Reinsert cycle (simulates real workload churn)
// ============================================================

BENCHMARK_DEFINE_F(ChurnFixture, BM_DeleteAndReinsert)(benchmark::State& state) {
    for (auto _ : state) {
        // Delete one document
        if (churn_index >= known_ids.size()) {
            churn_index = 0;  // Wrap around
        }

        bson_oid_t& oid_to_delete = known_ids[churn_index];

        bson_t* filter = bson_new();
        BSON_APPEND_OID(filter, "_id", &oid_to_delete);

        int rc = mongolite_delete_one(db, "bench", filter, &error);
        bson_destroy(filter);

        if (rc < 0) {
            state.SkipWithError("Delete in churn failed");
            break;
        }

        // Insert a new document
        bench::BenchDocument new_doc = generator.generate();
        bson_t* bson = bench::bench_doc_to_bson(new_doc);

        bson_oid_t new_id;
        rc = mongolite_insert_one(db, "bench", bson, &new_id, &error);
        bson_destroy(bson);

        if (rc != 0) {
            state.SkipWithError("Insert in churn failed");
            break;
        }

        // Update our tracking with the new ID
        known_ids[churn_index] = new_id;
        churn_index++;
    }

    state.SetItemsProcessed(state.iterations() * 2);  // 1 delete + 1 insert per iteration
}

BENCHMARK_REGISTER_F(ChurnFixture, BM_DeleteAndReinsert)
    ->Unit(benchmark::kMicrosecond);

// ============================================================
// Main
// ============================================================

BENCHMARK_MAIN();
