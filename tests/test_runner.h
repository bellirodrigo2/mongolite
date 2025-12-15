#ifndef TEST_RUNNER_H
#define TEST_RUNNER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

// Test statistics
static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;
static int assertions_run = 0;
static const char* current_test = NULL;

// Colors for output (optional)
#define COLOR_RED     "\x1b[31m"
#define COLOR_GREEN   "\x1b[32m"
#define COLOR_YELLOW  "\x1b[33m"
#define COLOR_RESET   "\x1b[0m"

// Core test macros (Unity-compatible names)
#define TEST_ASSERT_TRUE(condition) \
    do { \
        assertions_run++; \
        if (!(condition)) { \
            printf(COLOR_RED "  ✗ %s:%d: " COLOR_RESET "assertion failed: %s\n", \
                   __FILE__, __LINE__, #condition); \
            return 1; \
        } \
    } while(0)

#define TEST_ASSERT_FALSE(condition) \
    TEST_ASSERT_TRUE(!(condition))

#define TEST_ASSERT_EQUAL(expected, actual) \
    TEST_ASSERT_TRUE((expected) == (actual))

#define TEST_ASSERT_NOT_EQUAL(expected, actual) \
    TEST_ASSERT_TRUE((expected) != (actual))

#define TEST_ASSERT_NULL(pointer) \
    TEST_ASSERT_TRUE((pointer) == NULL)

#define TEST_ASSERT_NOT_NULL(pointer) \
    TEST_ASSERT_TRUE((pointer) != NULL)

#define TEST_ASSERT_EQUAL_STRING(expected, actual) \
    do { \
        assertions_run++; \
        if (strcmp((expected), (actual)) != 0) { \
            printf(COLOR_RED "  ✗ %s:%d: " COLOR_RESET "strings differ:\n    expected: '%s'\n    actual:   '%s'\n", \
                   __FILE__, __LINE__, (expected), (actual)); \
            return 1; \
        } \
    } while(0)

#define TEST_ASSERT_EQUAL_MEMORY(expected, actual, size) \
    do { \
        assertions_run++; \
        if (memcmp((expected), (actual), (size)) != 0) { \
            printf(COLOR_RED "  ✗ %s:%d: " COLOR_RESET "memory differs (size=%zu)\n", \
                   __FILE__, __LINE__, (size_t)(size)); \
            return 1; \
        } \
    } while(0)

// Test definition macro
#define TEST(name) \
    static int test_##name(void)

// Run a single test
#define RUN_TEST(test_func) \
    do { \
        current_test = #test_func; \
        printf("  %s ... ", current_test); \
        fflush(stdout); \
        tests_run++; \
        int result = test_func(); \
        if (result == 0) { \
            tests_passed++; \
            printf(COLOR_GREEN "✓" COLOR_RESET "\n"); \
        } else { \
            tests_failed++; \
            printf(COLOR_RED "✗" COLOR_RESET "\n"); \
        } \
    } while(0)

// Test suite macros
#define TEST_SUITE_BEGIN(name) \
    int main(int argc, char **argv) { \
        (void)argc; (void)argv; \
        printf("\n=== %s ===\n", name);

#define TEST_SUITE_END() \
        printf("\n"); \
        printf("Results: %d tests, ", tests_run); \
        printf(COLOR_GREEN "%d passed" COLOR_RESET ", ", tests_passed); \
        if (tests_failed > 0) { \
            printf(COLOR_RED "%d failed" COLOR_RESET "\n", tests_failed); \
        } else { \
            printf("0 failed\n"); \
        } \
        printf("Assertions: %d\n\n", assertions_run); \
        return tests_failed > 0 ? 1 : 0; \
    }

// Setup and teardown (optional)
#define TEST_SETUP() \
    static void setUp(void)

#define TEST_TEARDOWN() \
    static void tearDown(void)

// Shortcuts (more Unity-compatible)
#define ASSERT TEST_ASSERT_TRUE
#define ASSERT_EQ TEST_ASSERT_EQUAL
#define ASSERT_NE TEST_ASSERT_NOT_EQUAL
#define ASSERT_STR_EQ TEST_ASSERT_EQUAL_STRING

// TEST_ASSERT with format string message
#define TEST_ASSERT(condition, ...) \
    do { \
        assertions_run++; \
        if (!(condition)) { \
            printf(COLOR_RED "  FAIL %s:%d: " COLOR_RESET, __FILE__, __LINE__); \
            printf(__VA_ARGS__); \
            printf("\n"); \
            return 1; \
        } \
    } while(0)

#endif // TEST_RUNNER_H