/**
 * bench_update.cpp - Update operation benchmarks for mongolite
 *
 * Benchmarks:
 * - BM_UpdateOneSet: Single field $set update
 * - BM_UpdateOneInc: $inc operator (atomic increment)
 * - BM_UpdateOneMultiOp: Multiple operators in one update
 * - BM_UpdateMany: Update multiple documents
 * - BM_ReplaceOne: Full document replacement
 * - BM_UpdateUpsert: Update with upsert (insert if not found)
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
// Fixture: Pre-populated database for update benchmarks
// ============================================================

class UpdateFixture : public benchmark::Fixture {
public:
    mongolite_db_t* db = nullptr;
    bench::DocumentGenerator generator;
    std::string db_path;
    gerror_t error;

    std::vector<bson_oid_t> known_ids;
    std::vector<int64_t> known_ref_ids;

    static size_t instance_counter;
    static constexpr size_t COLLECTION_SIZE = 10000;

    void SetUp(const benchmark::State& state) override {
        (void)state;
        memset(&error, 0, sizeof(error));

        // Use unique counter instead of rand() to avoid collisions
        db_path = "./bench_update_db_" + std::to_string(++instance_counter);
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
                if (known_ref_ids.size() < 1000) {
                    known_ref_ids.push_back(doc.id);
                }
            }

            bson_oid_t* ids = nullptr;
            rc = mongolite_insert_many(db, "bench",
                                       const_cast<const bson_t**>(bson_docs.data()),
                                       to_insert, &ids, &error);

            if (ids && known_ids.size() < 1000) {
                for (size_t j = 0; j < to_insert && known_ids.size() < 1000; ++j) {
                    known_ids.push_back(ids[j]);
                }
            }

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
#ifdef _WIN32
        Sleep(100);  // Give Windows time to release file handles
#endif
        remove_directory(db_path.c_str());
    }
};

// Static counter initialization
size_t UpdateFixture::instance_counter = 0;

// ============================================================
// Benchmark: Update One with $set (by _id)
// ============================================================

BENCHMARK_DEFINE_F(UpdateFixture, BM_UpdateOneSetById)(benchmark::State& state) {
    size_t idx = 0;
    double new_balance = 99999.99;

    for (auto _ : state) {
        const bson_oid_t& oid = known_ids[idx % known_ids.size()];
        idx++;

        // Filter: {"_id": <oid>}
        bson_t* filter = bson_new();
        BSON_APPEND_OID(filter, "_id", &oid);

        // Update: {"$set": {"balance": 99999.99}}
        bson_t* update = bson_new();
        bson_t set_doc;
        BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
        BSON_APPEND_DOUBLE(&set_doc, "balance", new_balance);
        bson_append_document_end(update, &set_doc);

        int rc = mongolite_update_one(db, "bench", filter, update, false, &error);

        bson_destroy(filter);
        bson_destroy(update);

        if (rc < 0) {
            state.SkipWithError("Update one $set failed");
            break;
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(UpdateFixture, BM_UpdateOneSetById)
    ->Unit(benchmark::kMicrosecond);

// ============================================================
// Benchmark: Update One with $set (by field scan)
// ============================================================

BENCHMARK_DEFINE_F(UpdateFixture, BM_UpdateOneSetByField)(benchmark::State& state) {
    size_t idx = 0;

    for (auto _ : state) {
        int64_t ref_id = known_ref_ids[idx % known_ref_ids.size()];
        idx++;

        // Filter: {"ref_id": <value>}
        bson_t* filter = bson_new();
        BSON_APPEND_INT64(filter, "ref_id", ref_id);

        // Update: {"$set": {"active": false}}
        bson_t* update = bson_new();
        bson_t set_doc;
        BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
        BSON_APPEND_BOOL(&set_doc, "active", false);
        bson_append_document_end(update, &set_doc);

        int rc = mongolite_update_one(db, "bench", filter, update, false, &error);

        bson_destroy(filter);
        bson_destroy(update);

        if (rc < 0) {
            state.SkipWithError("Update one by field failed");
            break;
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(UpdateFixture, BM_UpdateOneSetByField)
    ->Unit(benchmark::kMicrosecond);

// ============================================================
// Benchmark: Update One with $inc (atomic increment)
// ============================================================

BENCHMARK_DEFINE_F(UpdateFixture, BM_UpdateOneInc)(benchmark::State& state) {
    size_t idx = 0;

    for (auto _ : state) {
        const bson_oid_t& oid = known_ids[idx % known_ids.size()];
        idx++;

        bson_t* filter = bson_new();
        BSON_APPEND_OID(filter, "_id", &oid);

        // Update: {"$inc": {"score": 1.0}}
        bson_t* update = bson_new();
        bson_t inc_doc;
        BSON_APPEND_DOCUMENT_BEGIN(update, "$inc", &inc_doc);
        BSON_APPEND_DOUBLE(&inc_doc, "score", 1.0);
        bson_append_document_end(update, &inc_doc);

        int rc = mongolite_update_one(db, "bench", filter, update, false, &error);

        bson_destroy(filter);
        bson_destroy(update);

        if (rc < 0) {
            state.SkipWithError("Update one $inc failed");
            break;
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(UpdateFixture, BM_UpdateOneInc)
    ->Unit(benchmark::kMicrosecond);

// ============================================================
// Benchmark: Update One with multiple operators
// ============================================================

BENCHMARK_DEFINE_F(UpdateFixture, BM_UpdateOneMultiOp)(benchmark::State& state) {
    size_t idx = 0;

    for (auto _ : state) {
        const bson_oid_t& oid = known_ids[idx % known_ids.size()];
        idx++;

        bson_t* filter = bson_new();
        BSON_APPEND_OID(filter, "_id", &oid);

        // Update: {"$set": {"active": true, "department": "updated"}, "$inc": {"age": 1, "score": 0.5}}
        bson_t* update = bson_new();

        bson_t set_doc;
        BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
        BSON_APPEND_BOOL(&set_doc, "active", true);
        BSON_APPEND_UTF8(&set_doc, "department", "updated");
        bson_append_document_end(update, &set_doc);

        bson_t inc_doc;
        BSON_APPEND_DOCUMENT_BEGIN(update, "$inc", &inc_doc);
        BSON_APPEND_INT32(&inc_doc, "age", 1);
        BSON_APPEND_DOUBLE(&inc_doc, "score", 0.5);
        bson_append_document_end(update, &inc_doc);

        int rc = mongolite_update_one(db, "bench", filter, update, false, &error);

        bson_destroy(filter);
        bson_destroy(update);

        if (rc < 0) {
            state.SkipWithError("Update one multi-op failed");
            break;
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(UpdateFixture, BM_UpdateOneMultiOp)
    ->Unit(benchmark::kMicrosecond);

// ============================================================
// Benchmark: Update Many (varying match count)
// ============================================================

BENCHMARK_DEFINE_F(UpdateFixture, BM_UpdateMany)(benchmark::State& state) {
    // Selectivity determines how many docs match
    const int selectivity = static_cast<int>(state.range(0));

    const char* target_dept;
    switch (selectivity) {
        case 0: target_dept = "engineering"; break;  // ~12.5% (1/8 departments)
        case 1: target_dept = "sales"; break;
        default: target_dept = "marketing"; break;
    }

    for (auto _ : state) {
        // Filter: {"department": "<dept>"}
        bson_t* filter = bson_new();
        BSON_APPEND_UTF8(filter, "department", target_dept);

        // Update: {"$set": {"active": true}}
        bson_t* update = bson_new();
        bson_t set_doc;
        BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
        BSON_APPEND_BOOL(&set_doc, "active", true);
        bson_append_document_end(update, &set_doc);

        int64_t modified_count = 0;
        int rc = mongolite_update_many(db, "bench", filter, update, false, &modified_count, &error);

        bson_destroy(filter);
        bson_destroy(update);

        if (rc < 0) {
            state.SkipWithError("Update many failed");
            break;
        }

        state.counters["modified"] = static_cast<double>(modified_count);
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(UpdateFixture, BM_UpdateMany)
    ->Unit(benchmark::kMillisecond)
    ->Arg(0)
    ->Arg(1)
    ->Arg(2);

// ============================================================
// Benchmark: Replace One (full document replacement)
// ============================================================

BENCHMARK_DEFINE_F(UpdateFixture, BM_ReplaceOne)(benchmark::State& state) {
    size_t idx = 0;

    for (auto _ : state) {
        const bson_oid_t& oid = known_ids[idx % known_ids.size()];
        idx++;

        // Filter
        bson_t* filter = bson_new();
        BSON_APPEND_OID(filter, "_id", &oid);

        // Generate a new document for replacement
        bench::BenchDocument new_doc = generator.generate();
        bson_t* replacement = bench::bench_doc_to_bson(new_doc);

        int rc = mongolite_replace_one(db, "bench", filter, replacement, false, &error);

        bson_destroy(filter);
        bson_destroy(replacement);

        if (rc < 0) {
            state.SkipWithError("Replace one failed");
            break;
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(UpdateFixture, BM_ReplaceOne)
    ->Unit(benchmark::kMicrosecond);

// ============================================================
// Benchmark: Upsert (update or insert)
// ============================================================

BENCHMARK_DEFINE_F(UpdateFixture, BM_Upsert)(benchmark::State& state) {
    // 0 = existing docs (update), 1 = new docs (insert)
    const bool insert_new = state.range(0) == 1;
    size_t idx = 0;
    int64_t new_id_counter = 999999;

    for (auto _ : state) {
        bson_t* filter = bson_new();

        if (insert_new) {
            // Filter for non-existent document
            BSON_APPEND_INT64(filter, "ref_id", new_id_counter++);
        } else {
            // Filter for existing document
            int64_t ref_id = known_ref_ids[idx % known_ref_ids.size()];
            idx++;
            BSON_APPEND_INT64(filter, "ref_id", ref_id);
        }

        // Update with $set
        bson_t* update = bson_new();
        bson_t set_doc;
        BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &set_doc);
        BSON_APPEND_UTF8(&set_doc, "name", "Upserted User");
        BSON_APPEND_DOUBLE(&set_doc, "balance", 12345.67);
        BSON_APPEND_BOOL(&set_doc, "active", true);
        bson_append_document_end(update, &set_doc);

        int rc = mongolite_update_one(db, "bench", filter, update, true /* upsert */, &error);

        bson_destroy(filter);
        bson_destroy(update);

        if (rc < 0) {
            state.SkipWithError("Upsert failed");
            break;
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(UpdateFixture, BM_Upsert)
    ->Unit(benchmark::kMicrosecond)
    ->Arg(0)   // update existing
    ->Arg(1);  // insert new

// ============================================================
// Benchmark: Update One JSON API
// ============================================================

BENCHMARK_DEFINE_F(UpdateFixture, BM_UpdateOneJson)(benchmark::State& state) {
    size_t idx = 0;

    for (auto _ : state) {
        int64_t ref_id = known_ref_ids[idx % known_ref_ids.size()];
        idx++;

        char filter_json[128];
        snprintf(filter_json, sizeof(filter_json), "{\"ref_id\": %" PRId64 "}", ref_id);

        const char* update_json = "{\"$set\": {\"active\": false, \"score\": 50.0}}";

        int rc = mongolite_update_one_json(db, "bench", filter_json, update_json, false, &error);

        if (rc < 0) {
            state.SkipWithError("Update one JSON failed");
            break;
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(UpdateFixture, BM_UpdateOneJson)
    ->Unit(benchmark::kMicrosecond);

// ============================================================
// Main
// ============================================================

BENCHMARK_MAIN();
