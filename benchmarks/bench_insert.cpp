/**
 * bench_insert.cpp - Insert operation benchmarks for mongolite
 *
 * Benchmarks:
 * - BM_InsertOne: Single document insertion
 * - BM_InsertMany: Batch insertion with varying batch sizes
 * - BM_InsertOneJson: Single document insertion via JSON API
 * - BM_InsertManyJson: Batch insertion via JSON API
 */

#include <benchmark/benchmark.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <string>

#ifdef _WIN32
#include <windows.h>
#include <direct.h>
#define rmdir _rmdir
#define unlink _unlink
#else
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#endif

extern "C" {
#include "mongolite.h"
}

#include "bench_docs.h"

// ============================================================
// Helper: Remove directory recursively (for cleanup)
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
// Fixture: Database setup/teardown
// ============================================================

class MongoliteFixture : public benchmark::Fixture {
public:
    mongolite_db_t* db = nullptr;
    bench::DocumentGenerator generator;
    std::string db_path;
    gerror_t error;

    void SetUp(const benchmark::State& state) override {
        (void)state;
        memset(&error, 0, sizeof(error));

        // Create unique path for this benchmark run
        db_path = "./bench_db_" + std::to_string(rand());

        // Remove if exists from previous failed run
        remove_directory(db_path.c_str());

        // Open database
        db_config_t config = {0};
        config.max_bytes = 1ULL * 1024 * 1024 * 1024; // 1GB

        int rc = mongolite_open(db_path.c_str(), &db, &config, &error);
        if (rc != 0) {
            fprintf(stderr, "Failed to open database: %s\n", error.message);
        }

        // Create collection
        rc = mongolite_collection_create(db, "bench", nullptr, &error);
        if (rc != 0 && rc != -1) { // -1 might mean already exists
            fprintf(stderr, "Failed to create collection: %s\n", error.message);
        }

        // Reset generator for reproducibility
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
};

// ============================================================
// Benchmark: Insert One (BSON API)
// ============================================================

BENCHMARK_DEFINE_F(MongoliteFixture, BM_InsertOne)(benchmark::State& state) {
    for (auto _ : state) {
        bench::BenchDocument doc = generator.generate();
        bson_t* bson = bench::bench_doc_to_bson(doc);

        bson_oid_t inserted_id;
        int rc = mongolite_insert_one(db, "bench", bson, &inserted_id, &error);

        bson_destroy(bson);

        if (rc != 0) {
            state.SkipWithError("Insert failed");
            break;
        }
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() * sizeof(bench::BenchDocument));
}

BENCHMARK_REGISTER_F(MongoliteFixture, BM_InsertOne)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(10000);

// ============================================================
// Benchmark: Insert Many (BSON API) - varying batch sizes
// ============================================================

BENCHMARK_DEFINE_F(MongoliteFixture, BM_InsertMany)(benchmark::State& state) {
    const size_t batch_size = static_cast<size_t>(state.range(0));

    for (auto _ : state) {
        // Generate batch
        std::vector<bench::BenchDocument> docs = generator.generate_batch(batch_size);
        std::vector<bson_t*> bson_docs;
        bson_docs.reserve(batch_size);

        for (const auto& doc : docs) {
            bson_docs.push_back(bench::bench_doc_to_bson(doc));
        }

        // Insert batch
        bson_oid_t* inserted_ids = nullptr;
        int rc = mongolite_insert_many(db, "bench",
                                       const_cast<const bson_t**>(bson_docs.data()),
                                       batch_size, &inserted_ids, &error);

        // Cleanup
        for (auto* b : bson_docs) {
            bson_destroy(b);
        }
        if (inserted_ids) {
            free(inserted_ids);
        }

        if (rc < 0) {
            state.SkipWithError("Insert many failed");
            break;
        }
    }

    state.SetItemsProcessed(state.iterations() * batch_size);
    state.SetBytesProcessed(state.iterations() * batch_size * sizeof(bench::BenchDocument));
}

BENCHMARK_REGISTER_F(MongoliteFixture, BM_InsertMany)
    ->Unit(benchmark::kMicrosecond)
    ->Arg(10)      // batch of 10
    ->Arg(100)     // batch of 100
    ->Arg(1000);   // batch of 1000

// ============================================================
// Benchmark: Insert One JSON
// ============================================================

BENCHMARK_DEFINE_F(MongoliteFixture, BM_InsertOneJson)(benchmark::State& state) {
    for (auto _ : state) {
        bench::BenchDocument doc = generator.generate();
        char* json = bench::bench_doc_to_json(doc);

        bson_oid_t inserted_id;
        int rc = mongolite_insert_one_json(db, "bench", json, &inserted_id, &error);

        free(json);

        if (rc != 0) {
            state.SkipWithError("Insert JSON failed");
            break;
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(MongoliteFixture, BM_InsertOneJson)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(10000);

// ============================================================
// Benchmark: Insert Many JSON - varying batch sizes
// ============================================================

BENCHMARK_DEFINE_F(MongoliteFixture, BM_InsertManyJson)(benchmark::State& state) {
    const size_t batch_size = static_cast<size_t>(state.range(0));

    for (auto _ : state) {
        // Generate batch
        std::vector<bench::BenchDocument> docs = generator.generate_batch(batch_size);
        std::vector<char*> json_docs;
        json_docs.reserve(batch_size);

        for (const auto& doc : docs) {
            json_docs.push_back(bench::bench_doc_to_json(doc));
        }

        // Insert batch
        bson_oid_t* inserted_ids = nullptr;
        int rc = mongolite_insert_many_json(db, "bench",
                                            const_cast<const char**>(json_docs.data()),
                                            batch_size, &inserted_ids, &error);

        // Cleanup
        for (auto* j : json_docs) {
            free(j);
        }
        if (inserted_ids) {
            free(inserted_ids);
        }

        if (rc < 0) {
            state.SkipWithError("Insert many JSON failed");
            break;
        }
    }

    state.SetItemsProcessed(state.iterations() * batch_size);
}

BENCHMARK_REGISTER_F(MongoliteFixture, BM_InsertManyJson)
    ->Unit(benchmark::kMicrosecond)
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000);

// ============================================================
// Benchmark: Insert with explicit transaction
// ============================================================

BENCHMARK_DEFINE_F(MongoliteFixture, BM_InsertManyInTransaction)(benchmark::State& state) {
    const size_t batch_size = static_cast<size_t>(state.range(0));

    for (auto _ : state) {
        // Begin transaction
        int rc = mongolite_begin_transaction(db);
        if (rc != 0) {
            state.SkipWithError("Begin transaction failed");
            break;
        }

        // Insert documents one by one within transaction
        bool failed = false;
        for (size_t i = 0; i < batch_size && !failed; ++i) {
            bench::BenchDocument doc = generator.generate();
            bson_t* bson = bench::bench_doc_to_bson(doc);

            bson_oid_t inserted_id;
            rc = mongolite_insert_one(db, "bench", bson, &inserted_id, &error);

            bson_destroy(bson);

            if (rc != 0) {
                failed = true;
            }
        }

        if (failed) {
            mongolite_rollback(db);
            state.SkipWithError("Insert in transaction failed");
            break;
        }

        // Commit
        rc = mongolite_commit(db);
        if (rc != 0) {
            state.SkipWithError("Commit failed");
            break;
        }
    }

    state.SetItemsProcessed(state.iterations() * batch_size);
    state.SetBytesProcessed(state.iterations() * batch_size * sizeof(bench::BenchDocument));
}

BENCHMARK_REGISTER_F(MongoliteFixture, BM_InsertManyInTransaction)
    ->Unit(benchmark::kMicrosecond)
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000);

// ============================================================
// Benchmark: Throughput at scale (insert into growing collection)
// ============================================================

BENCHMARK_DEFINE_F(MongoliteFixture, BM_InsertAtScale)(benchmark::State& state) {
    const int64_t initial_docs = state.range(0);

    // Pre-populate collection
    state.PauseTiming();
    {
        const size_t batch = 1000;
        for (int64_t i = 0; i < initial_docs; i += batch) {
            size_t to_insert = std::min(batch, static_cast<size_t>(initial_docs - i));
            std::vector<bench::BenchDocument> docs = generator.generate_batch(to_insert);
            std::vector<bson_t*> bson_docs;
            for (const auto& doc : docs) {
                bson_docs.push_back(bench::bench_doc_to_bson(doc));
            }

            bson_oid_t* ids = nullptr;
            mongolite_insert_many(db, "bench",
                                  const_cast<const bson_t**>(bson_docs.data()),
                                  to_insert, &ids, &error);

            for (auto* b : bson_docs) bson_destroy(b);
            if (ids) free(ids);
        }
    }
    state.ResumeTiming();

    // Now benchmark inserting into the pre-populated collection
    for (auto _ : state) {
        bench::BenchDocument doc = generator.generate();
        bson_t* bson = bench::bench_doc_to_bson(doc);

        bson_oid_t inserted_id;
        int rc = mongolite_insert_one(db, "bench", bson, &inserted_id, &error);

        bson_destroy(bson);

        if (rc != 0) {
            state.SkipWithError("Insert at scale failed");
            break;
        }
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["initial_docs"] = static_cast<double>(initial_docs);
}

BENCHMARK_REGISTER_F(MongoliteFixture, BM_InsertAtScale)
    ->Unit(benchmark::kMicrosecond)
    ->Arg(0)        // empty collection
    ->Arg(1000)     // 1K docs
    ->Arg(10000)    // 10K docs
    ->Arg(100000)   // 100K docs
    ->Iterations(1000);

// ============================================================
// Main (provided by benchmark::benchmark_main)
// ============================================================

BENCHMARK_MAIN();
