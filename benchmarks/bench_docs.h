/**
 * bench_docs.h - Benchmark Document Generator
 *
 * Generates test documents with a flat structure suitable for both:
 * - MongoDB-style (BSON documents)
 * - SQLite (relational tables)
 *
 * Document Schema (flat/relational):
 * ---------------------------------
 * | Field       | Type    | Description                    |
 * |-------------|---------|--------------------------------|
 * | id          | int64   | Unique identifier              |
 * | name        | string  | User name                      |
 * | email       | string  | Email address                  |
 * | age         | int32   | Age (18-80)                    |
 * | balance     | double  | Account balance                |
 * | active      | bool    | Account status                 |
 * | created_at  | int64   | Unix timestamp (ms)            |
 * | department  | string  | Department name (for grouping) |
 * | score       | double  | Score (0.0-100.0)              |
 * ---------------------------------
 *
 * SQLite equivalent:
 * CREATE TABLE users (
 *   id INTEGER PRIMARY KEY,
 *   name TEXT,
 *   email TEXT,
 *   age INTEGER,
 *   balance REAL,
 *   active INTEGER,
 *   created_at INTEGER,
 *   department TEXT,
 *   score REAL
 * );
 */

#ifndef BENCH_DOCS_H
#define BENCH_DOCS_H

#include <cstdint>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <vector>
#include <string>
#include <random>

// Forward declare bson_t for BSON generation
extern "C" {
#include <bson/bson.h>
}

namespace bench {

// Department names for grouping/filtering benchmarks
static const char* DEPARTMENTS[] = {
    "engineering",
    "sales",
    "marketing",
    "support",
    "finance",
    "hr",
    "operations",
    "legal"
};
static const size_t NUM_DEPARTMENTS = sizeof(DEPARTMENTS) / sizeof(DEPARTMENTS[0]);

// First names for generating realistic data
static const char* FIRST_NAMES[] = {
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry",
    "Ivy", "Jack", "Kate", "Leo", "Mia", "Noah", "Olivia", "Paul",
    "Quinn", "Rose", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xavier"
};
static const size_t NUM_FIRST_NAMES = sizeof(FIRST_NAMES) / sizeof(FIRST_NAMES[0]);

// Last names
static const char* LAST_NAMES[] = {
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"
};
static const size_t NUM_LAST_NAMES = sizeof(LAST_NAMES) / sizeof(LAST_NAMES[0]);

/**
 * Plain C struct representing a benchmark document.
 * This structure is database-agnostic and can be used
 * to generate both BSON documents and SQL INSERT statements.
 */
struct BenchDocument {
    int64_t     id;
    char        name[64];
    char        email[128];
    int32_t     age;
    double      balance;
    bool        active;
    int64_t     created_at;
    char        department[32];
    double      score;
};

/**
 * Document generator with deterministic random generation.
 * Uses a seed for reproducibility across benchmark runs.
 */
class DocumentGenerator {
public:
    explicit DocumentGenerator(uint32_t seed = 42)
        : rng_(seed)
        , id_counter_(0)
        , age_dist_(18, 80)
        , balance_dist_(0.0, 100000.0)
        , score_dist_(0.0, 100.0)
        , active_dist_(0, 1)
        , dept_dist_(0, NUM_DEPARTMENTS - 1)
        , first_name_dist_(0, NUM_FIRST_NAMES - 1)
        , last_name_dist_(0, NUM_LAST_NAMES - 1)
    {
        base_timestamp_ = static_cast<int64_t>(std::time(nullptr)) * 1000;
    }

    /**
     * Generate a single document with random but reproducible data.
     */
    BenchDocument generate() {
        BenchDocument doc;

        doc.id = ++id_counter_;

        // Generate name
        const char* first = FIRST_NAMES[first_name_dist_(rng_)];
        const char* last = LAST_NAMES[last_name_dist_(rng_)];
        snprintf(doc.name, sizeof(doc.name), "%s %s", first, last);

        // Generate email (lowercase, no spaces)
        snprintf(doc.email, sizeof(doc.email), "%s.%s%lld@example.com",
                 first, last, static_cast<long long>(doc.id));
        // Convert to lowercase
        for (char* p = doc.email; *p; ++p) {
            if (*p >= 'A' && *p <= 'Z') *p = *p + 32;
        }

        doc.age = age_dist_(rng_);
        doc.balance = balance_dist_(rng_);
        doc.active = active_dist_(rng_) == 1;
        doc.created_at = base_timestamp_ + (doc.id * 1000); // 1 second apart
        doc.score = score_dist_(rng_);

        const char* dept = DEPARTMENTS[dept_dist_(rng_)];
        strncpy(doc.department, dept, sizeof(doc.department) - 1);
        doc.department[sizeof(doc.department) - 1] = '\0';

        return doc;
    }

    /**
     * Generate multiple documents.
     */
    std::vector<BenchDocument> generate_batch(size_t count) {
        std::vector<BenchDocument> docs;
        docs.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            docs.push_back(generate());
        }
        return docs;
    }

    /**
     * Reset the generator to initial state (same seed).
     */
    void reset(uint32_t seed = 42) {
        rng_.seed(seed);
        id_counter_ = 0;
        base_timestamp_ = static_cast<int64_t>(std::time(nullptr)) * 1000;
    }

    /**
     * Get current document count.
     */
    int64_t count() const { return id_counter_; }

private:
    std::mt19937 rng_;
    int64_t id_counter_;
    int64_t base_timestamp_;

    std::uniform_int_distribution<int32_t> age_dist_;
    std::uniform_real_distribution<double> balance_dist_;
    std::uniform_real_distribution<double> score_dist_;
    std::uniform_int_distribution<int> active_dist_;
    std::uniform_int_distribution<size_t> dept_dist_;
    std::uniform_int_distribution<size_t> first_name_dist_;
    std::uniform_int_distribution<size_t> last_name_dist_;
};

// ============================================================
// BSON Conversion (for mongolite)
// ============================================================

/**
 * Convert a BenchDocument to a BSON document.
 * Caller is responsible for calling bson_destroy() on the result.
 */
inline bson_t* bench_doc_to_bson(const BenchDocument& doc) {
    bson_t* bson = bson_new();

    // Note: We don't set _id here - mongolite will auto-generate it
    // But we store our id as a separate field for reference
    BSON_APPEND_INT64(bson, "ref_id", doc.id);
    BSON_APPEND_UTF8(bson, "name", doc.name);
    BSON_APPEND_UTF8(bson, "email", doc.email);
    BSON_APPEND_INT32(bson, "age", doc.age);
    BSON_APPEND_DOUBLE(bson, "balance", doc.balance);
    BSON_APPEND_BOOL(bson, "active", doc.active);
    BSON_APPEND_INT64(bson, "created_at", doc.created_at);
    BSON_APPEND_UTF8(bson, "department", doc.department);
    BSON_APPEND_DOUBLE(bson, "score", doc.score);

    return bson;
}

/**
 * Convert a BenchDocument to a BSON document with explicit _id.
 * Uses the doc.id as _id (as int64).
 * Caller is responsible for calling bson_destroy() on the result.
 */
inline bson_t* bench_doc_to_bson_with_id(const BenchDocument& doc) {
    bson_t* bson = bson_new();

    BSON_APPEND_INT64(bson, "_id", doc.id);
    BSON_APPEND_UTF8(bson, "name", doc.name);
    BSON_APPEND_UTF8(bson, "email", doc.email);
    BSON_APPEND_INT32(bson, "age", doc.age);
    BSON_APPEND_DOUBLE(bson, "balance", doc.balance);
    BSON_APPEND_BOOL(bson, "active", doc.active);
    BSON_APPEND_INT64(bson, "created_at", doc.created_at);
    BSON_APPEND_UTF8(bson, "department", doc.department);
    BSON_APPEND_DOUBLE(bson, "score", doc.score);

    return bson;
}

// ============================================================
// JSON Conversion (for both mongolite JSON API and SQLite)
// ============================================================

/**
 * Convert a BenchDocument to a JSON string.
 * Returns a heap-allocated string. Caller must free().
 */
inline char* bench_doc_to_json(const BenchDocument& doc) {
    char* json = static_cast<char*>(malloc(512));
    if (!json) return nullptr;

    snprintf(json, 512,
        "{\"ref_id\":%" PRId64 ","
        "\"name\":\"%s\","
        "\"email\":\"%s\","
        "\"age\":%d,"
        "\"balance\":%.2f,"
        "\"active\":%s,"
        "\"created_at\":%" PRId64 ","
        "\"department\":\"%s\","
        "\"score\":%.2f}",
        doc.id,
        doc.name,
        doc.email,
        doc.age,
        doc.balance,
        doc.active ? "true" : "false",
        doc.created_at,
        doc.department,
        doc.score
    );

    return json;
}

// ============================================================
// SQL Helpers (for future SQLite benchmarks)
// ============================================================

/**
 * Get the CREATE TABLE statement for the benchmark schema.
 */
inline const char* bench_doc_create_table_sql() {
    return
        "CREATE TABLE IF NOT EXISTS users ("
        "  id INTEGER PRIMARY KEY,"
        "  name TEXT NOT NULL,"
        "  email TEXT NOT NULL,"
        "  age INTEGER NOT NULL,"
        "  balance REAL NOT NULL,"
        "  active INTEGER NOT NULL,"
        "  created_at INTEGER NOT NULL,"
        "  department TEXT NOT NULL,"
        "  score REAL NOT NULL"
        ")";
}

/**
 * Get an INSERT statement template for prepared statements.
 */
inline const char* bench_doc_insert_sql() {
    return
        "INSERT INTO users (id, name, email, age, balance, active, created_at, department, score) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
}

} // namespace bench

#endif // BENCH_DOCS_H
