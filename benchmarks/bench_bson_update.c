/**
 * bench_bson_update.c - Benchmarks for BSON update operators
 *
 * Measures throughput and latency of update operations in isolation.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <bson/bson.h>
#include "bson_update.h"
#include "macros.h"

/* ============================================================
 * Timing utilities
 * ============================================================ */

#if defined(_WIN32)
#include <windows.h>
static double get_time_ns(void) {
    LARGE_INTEGER freq, counter;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&counter);
    return (double)counter.QuadPart / freq.QuadPart * 1e9;
}
#else
static double get_time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e9 + ts.tv_nsec;
}
#endif

typedef struct {
    const char *name;
    double total_ns;
    size_t iterations;
    size_t ops_per_iter;
} bench_result_t;

static void print_result(bench_result_t *r) {
    double total_ops = r->iterations * r->ops_per_iter;
    double ops_per_sec = total_ops / (r->total_ns / 1e9);
    double ns_per_op = r->total_ns / total_ops;

    printf("%-30s %8.0f ops/s  %8.1f ns/op\n", r->name, ops_per_sec, ns_per_op);
}

/* ============================================================
 * Helper: Create test documents
 * ============================================================ */

static bson_t* create_simple_doc(void) {
    bson_t *doc = bson_new();
    bson_oid_t oid;
    bson_oid_init(&oid, NULL);
    BSON_APPEND_OID(doc, "_id", &oid);
    BSON_APPEND_UTF8(doc, "name", "test_user");
    BSON_APPEND_INT32(doc, "age", 25);
    BSON_APPEND_INT32(doc, "count", 100);
    BSON_APPEND_UTF8(doc, "email", "test@example.com");
    return doc;
}

static bson_t* create_doc_with_array(void) {
    bson_t *doc = bson_new();
    bson_t array;
    bson_oid_t oid;
    bson_oid_init(&oid, NULL);
    BSON_APPEND_OID(doc, "_id", &oid);
    BSON_APPEND_UTF8(doc, "name", "test");

    bson_append_array_begin(doc, "items", -1, &array);
    for (int i = 0; i < 10; i++) {
        char key[16];
        snprintf(key, sizeof(key), "%d", i);
        BSON_APPEND_INT32(&array, key, i * 10);
    }
    bson_append_array_end(doc, &array);

    return doc;
}

static bson_t* create_large_doc(int num_fields) {
    bson_t *doc = bson_new();
    bson_oid_t oid;
    bson_oid_init(&oid, NULL);
    BSON_APPEND_OID(doc, "_id", &oid);

    for (int i = 0; i < num_fields; i++) {
        char key[32];
        snprintf(key, sizeof(key), "field_%d", i);
        BSON_APPEND_INT32(doc, key, i);
    }
    return doc;
}

/* ============================================================
 * Benchmarks
 * ============================================================ */

static void bench_set_single_field(size_t iterations) {
    bson_t *doc = create_simple_doc();
    bson_t *update = BCON_NEW("$set", "{", "name", BCON_UTF8("updated"), "}");

    double start = get_time_ns();

    for (size_t i = 0; i < iterations; i++) {
        bson_t *result = bson_update_apply(doc, update, NULL);
        bson_destroy(result);
    }

    double end = get_time_ns();

    bench_result_t r = {"$set (single field)", end - start, iterations, 1};
    print_result(&r);

    bson_destroy(update);
    bson_destroy(doc);
}

static void bench_set_multiple_fields(size_t iterations) {
    bson_t *doc = create_simple_doc();
    bson_t *update = BCON_NEW(
        "$set", "{",
            "name", BCON_UTF8("updated"),
            "age", BCON_INT32(30),
            "email", BCON_UTF8("new@example.com"),
        "}"
    );

    double start = get_time_ns();

    for (size_t i = 0; i < iterations; i++) {
        bson_t *result = bson_update_apply(doc, update, NULL);
        bson_destroy(result);
    }

    double end = get_time_ns();

    bench_result_t r = {"$set (3 fields)", end - start, iterations, 1};
    print_result(&r);

    bson_destroy(update);
    bson_destroy(doc);
}

static void bench_inc_single_field(size_t iterations) {
    bson_t *doc = create_simple_doc();
    bson_t *update = BCON_NEW("$inc", "{", "count", BCON_INT32(1), "}");

    double start = get_time_ns();

    for (size_t i = 0; i < iterations; i++) {
        bson_t *result = bson_update_apply(doc, update, NULL);
        bson_destroy(result);
    }

    double end = get_time_ns();

    bench_result_t r = {"$inc (single field)", end - start, iterations, 1};
    print_result(&r);

    bson_destroy(update);
    bson_destroy(doc);
}

static void bench_unset_single_field(size_t iterations) {
    bson_t *doc = create_simple_doc();
    bson_t *update = BCON_NEW("$unset", "{", "email", BCON_INT32(1), "}");

    double start = get_time_ns();

    for (size_t i = 0; i < iterations; i++) {
        bson_t *result = bson_update_apply(doc, update, NULL);
        bson_destroy(result);
    }

    double end = get_time_ns();

    bench_result_t r = {"$unset (single field)", end - start, iterations, 1};
    print_result(&r);

    bson_destroy(update);
    bson_destroy(doc);
}

static void bench_push_to_array(size_t iterations) {
    bson_t *doc = create_doc_with_array();
    bson_t *update = BCON_NEW("$push", "{", "items", BCON_INT32(999), "}");

    double start = get_time_ns();

    for (size_t i = 0; i < iterations; i++) {
        bson_t *result = bson_update_apply(doc, update, NULL);
        bson_destroy(result);
    }

    double end = get_time_ns();

    bench_result_t r = {"$push (10-element array)", end - start, iterations, 1};
    print_result(&r);

    bson_destroy(update);
    bson_destroy(doc);
}

static void bench_pull_from_array(size_t iterations) {
    bson_t *doc = create_doc_with_array();
    bson_t *update = BCON_NEW("$pull", "{", "items", BCON_INT32(50), "}");

    double start = get_time_ns();

    for (size_t i = 0; i < iterations; i++) {
        bson_t *result = bson_update_apply(doc, update, NULL);
        bson_destroy(result);
    }

    double end = get_time_ns();

    bench_result_t r = {"$pull (10-element array)", end - start, iterations, 1};
    print_result(&r);

    bson_destroy(update);
    bson_destroy(doc);
}

static void bench_rename_field(size_t iterations) {
    bson_t *doc = create_simple_doc();
    bson_t *update = BCON_NEW("$rename", "{", "name", BCON_UTF8("username"), "}");

    double start = get_time_ns();

    for (size_t i = 0; i < iterations; i++) {
        bson_t *result = bson_update_apply(doc, update, NULL);
        bson_destroy(result);
    }

    double end = get_time_ns();

    bench_result_t r = {"$rename (single field)", end - start, iterations, 1};
    print_result(&r);

    bson_destroy(update);
    bson_destroy(doc);
}

static void bench_combined_update(size_t iterations) {
    bson_t *doc = create_simple_doc();
    bson_t *update = BCON_NEW(
        "$set", "{", "name", BCON_UTF8("updated"), "}",
        "$inc", "{", "count", BCON_INT32(1), "}",
        "$unset", "{", "email", BCON_INT32(1), "}"
    );

    double start = get_time_ns();

    for (size_t i = 0; i < iterations; i++) {
        bson_t *result = bson_update_apply(doc, update, NULL);
        bson_destroy(result);
    }

    double end = get_time_ns();

    bench_result_t r = {"Combined ($set+$inc+$unset)", end - start, iterations, 1};
    print_result(&r);

    bson_destroy(update);
    bson_destroy(doc);
}

static void bench_set_on_large_doc(size_t iterations, int num_fields) {
    bson_t *doc = create_large_doc(num_fields);
    bson_t *update = BCON_NEW("$set", "{", "field_0", BCON_INT32(999), "}");

    double start = get_time_ns();

    for (size_t i = 0; i < iterations; i++) {
        bson_t *result = bson_update_apply(doc, update, NULL);
        bson_destroy(result);
    }

    double end = get_time_ns();

    char name[64];
    snprintf(name, sizeof(name), "$set (doc with %d fields)", num_fields);
    bench_result_t r = {name, end - start, iterations, 1};
    print_result(&r);

    bson_destroy(update);
    bson_destroy(doc);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(int argc, char *argv[]) {
    size_t iterations = 100000;

    if (argc > 1) {
        iterations = atoi(argv[1]);
    }

    printf("BSON Update Operator Benchmarks\n");
    printf("================================\n");
    printf("Iterations: %zu\n\n", iterations);

    printf("%-30s %12s  %12s\n", "Operation", "Throughput", "Latency");
    printf("%-30s %12s  %12s\n", "---------", "----------", "-------");

    bench_set_single_field(iterations);
    bench_set_multiple_fields(iterations);
    bench_inc_single_field(iterations);
    bench_unset_single_field(iterations);
    bench_push_to_array(iterations);
    bench_pull_from_array(iterations);
    bench_rename_field(iterations);
    bench_combined_update(iterations);

    printf("\nDocument Size Scaling:\n");
    bench_set_on_large_doc(iterations, 5);
    bench_set_on_large_doc(iterations, 20);
    bench_set_on_large_doc(iterations, 50);
    bench_set_on_large_doc(iterations, 100);

    return 0;
}
