// test_gerror.c - Unit tests for gerror module

#include "test_runner.h"
#include "gerror.h"
#include <stdarg.h>

TEST(error_initialization) {
    gerror_t error = {0};
    
    TEST_ASSERT_EQUAL(0, error.code);
    TEST_ASSERT_EQUAL_STRING("", error.lib);
    TEST_ASSERT_EQUAL_STRING("", error.message);
    
    return 0;
}

TEST(set_error_basic) {
    gerror_t error = {0};
    
    set_error(&error, "mylib", 42, "Test error: %d", 123);
    
    TEST_ASSERT_EQUAL(42, error.code);
    TEST_ASSERT_EQUAL_STRING("mylib", error.lib);
    TEST_ASSERT_EQUAL_STRING("Test error: 123", error.message);
    
    return 0;
}

TEST(set_error_null_lib) {
    gerror_t error = {0};
    
    set_error(&error, NULL, 100, "Error without lib");
    
    TEST_ASSERT_EQUAL(100, error.code);
    TEST_ASSERT_EQUAL_STRING("unknown", error.lib);
    TEST_ASSERT_EQUAL_STRING("Error without lib", error.message);
    
    return 0;
}

TEST(set_error_null_error) {
    // Should not crash when error is NULL
    set_error(NULL, "lib", 1, "test");
    
    TEST_ASSERT_TRUE(1); // If we get here, it didn't crash
    
    return 0;
}


TEST(error_message_basic) {
    gerror_t error = {0};
    set_error(&error, "test", 1, "Simple message");
    
    const char *msg = error_message(&error);
    TEST_ASSERT_EQUAL_STRING("Simple message", msg);
    
    return 0;
}

TEST(error_message_empty) {
    gerror_t error = {0};
    
    const char *msg = error_message(&error);
    TEST_ASSERT_EQUAL_STRING("No error", msg);
    
    return 0;
}

TEST(error_message_null) {
    const char *msg = error_message(NULL);
    TEST_ASSERT_EQUAL_STRING("No error", msg);
    
    return 0;
}

TEST(error_message_ex_with_lib) {
    gerror_t error = {0};
    char buffer[256];
    
    set_error(&error, "mylib", 42, "Something failed");
    
    const char *msg = error_message_ex(&error, buffer, sizeof(buffer));
    TEST_ASSERT_EQUAL_STRING("mylib: Something failed", msg);
    TEST_ASSERT_EQUAL_STRING("mylib: Something failed", buffer);
    
    return 0;
}

TEST(error_message_ex_no_lib) {
    gerror_t error = {0};
    char buffer[256];
    
    error.code = 1;
    strcpy(error.message, "Just a message");
    error.lib[0] = '\0';
    
    const char *msg = error_message_ex(&error, buffer, sizeof(buffer));
    TEST_ASSERT_EQUAL_STRING("Just a message", msg);
    
    return 0;
}

TEST(error_message_ex_empty) {
    gerror_t error = {0};
    char buffer[256];
    
    const char *msg = error_message_ex(&error, buffer, sizeof(buffer));
    TEST_ASSERT_EQUAL_STRING("No error", msg);
    
    return 0;
}

TEST(error_message_ex_invalid_buffer) {
    gerror_t error = {0};
    
    const char *msg = error_message_ex(&error, NULL, 0);
    TEST_ASSERT_EQUAL_STRING("Invalid buffer", msg);
    
    return 0;
}

TEST(error_clear_test) {
    gerror_t error = {0};
    
    set_error(&error, "lib", 99, "Error message");
    TEST_ASSERT_EQUAL(99, error.code);
    
    error_clear(&error);
    
    TEST_ASSERT_EQUAL(0, error.code);
    TEST_ASSERT_EQUAL_STRING("", error.lib);
    TEST_ASSERT_EQUAL_STRING("", error.message);
    
    return 0;
}

TEST(error_clear_null) {
    // Should not crash
    error_clear(NULL);
    TEST_ASSERT_TRUE(1);
    
    return 0;
}

TEST(error_overwrite) {
    gerror_t error = {0};
    
    set_error(&error, "lib1", 1, "First error");
    TEST_ASSERT_EQUAL_STRING("First error", error.message);
    
    set_error(&error, "lib2", 2, "Second error");
    TEST_ASSERT_EQUAL_STRING("lib2", error.lib);
    TEST_ASSERT_EQUAL(2, error.code);
    TEST_ASSERT_EQUAL_STRING("Second error", error.message);
    
    return 0;
}

TEST(error_long_message) {
    gerror_t error = {0};
    
    // Create a message that would overflow if not handled properly
    set_error(&error, "lib", 1, "%s", 
        "This is a very long message that goes on and on and on "
        "and should be truncated if it exceeds the buffer size "
        "which is 256 characters according to the structure definition "
        "so let's make it even longer to ensure proper truncation "
        "because we want to test boundary conditions properly "
        "and make sure nothing bad happens when limits are exceeded");
    
    // Just check it didn't crash and has some content
    TEST_ASSERT_NOT_NULL(error.message);
    TEST_ASSERT_TRUE(strlen(error.message) > 0);
    TEST_ASSERT_TRUE(strlen(error.message) < 256);
    
    return 0;
}

TEST_SUITE_BEGIN("gerror tests")
    RUN_TEST(test_error_initialization);
    RUN_TEST(test_set_error_basic);
    RUN_TEST(test_set_error_null_lib);
    RUN_TEST(test_set_error_null_error);
    RUN_TEST(test_error_message_basic);
    RUN_TEST(test_error_message_empty);
    RUN_TEST(test_error_message_null);
    RUN_TEST(test_error_message_ex_with_lib);
    RUN_TEST(test_error_message_ex_no_lib);
    RUN_TEST(test_error_message_ex_empty);
    RUN_TEST(test_error_message_ex_invalid_buffer);
    RUN_TEST(test_error_clear_test);
    RUN_TEST(test_error_clear_null);
    RUN_TEST(test_error_overwrite);
    RUN_TEST(test_error_long_message);
TEST_SUITE_END()