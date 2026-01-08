/*
 * test_wregex.c - Unit tests for wregex wrapper
 */

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "wregex.h"

#define TEST(name) static void name(void)
#define RUN_TEST(name) do { \
    printf("  %s... ", #name); \
    name(); \
    printf("OK\n"); \
} while(0)

/* ------------------------------------------------------------------
 * Basic tests
 * ------------------------------------------------------------------ */

TEST(test_simple_match)
{
    wregex_t *re = wregex_compile("hello", 0);
    assert(re != NULL);
    
    assert(wregex_match(re, "hello world", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "say hello", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "goodbye", WREGEX_ZERO_TERMINATED) == false);
    
    wregex_free(re);
}

TEST(test_case_insensitive)
{
    wregex_t *re = wregex_compile("hello", WREGEX_CASELESS);
    assert(re != NULL);
    
    assert(wregex_match(re, "HELLO", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "HeLLo", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "hello", WREGEX_ZERO_TERMINATED) == true);
    
    wregex_free(re);
}

TEST(test_case_sensitive)
{
    wregex_t *re = wregex_compile("Hello", 0);
    assert(re != NULL);
    
    assert(wregex_match(re, "Hello", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "hello", WREGEX_ZERO_TERMINATED) == false);
    assert(wregex_match(re, "HELLO", WREGEX_ZERO_TERMINATED) == false);
    
    wregex_free(re);
}

TEST(test_regex_patterns)
{
    wregex_t *re;
    
    /* Digit pattern */
    re = wregex_compile("\\d+", 0);
    assert(re != NULL);
    assert(wregex_match(re, "abc123def", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "no digits", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);
    
    /* Word boundary */
    re = wregex_compile("\\bword\\b", 0);
    assert(re != NULL);
    assert(wregex_match(re, "a word here", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "wording", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);
    
    /* Anchors */
    re = wregex_compile("^start", 0);
    assert(re != NULL);
    assert(wregex_match(re, "start of line", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "not start", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);
}

TEST(test_with_length)
{
    wregex_t *re = wregex_compile("test", 0);
    assert(re != NULL);
    
    /* Match with explicit length */
    assert(wregex_match(re, "testing", 4) == true);   /* "test" */
    assert(wregex_match(re, "testing", 3) == false);  /* "tes" */
    assert(wregex_match(re, "testing", 7) == true);   /* "testing" */
    
    wregex_free(re);
}

/* ------------------------------------------------------------------
 * Cache tests
 * ------------------------------------------------------------------ */

TEST(test_cache_reuse)
{
    /* Compile same pattern twice - should return same underlying code */
    wregex_t *re1 = wregex_compile("cached", 0);
    wregex_t *re2 = wregex_compile("cached", 0);
    
    assert(re1 != NULL);
    assert(re2 != NULL);
    
    /* Both should work */
    assert(wregex_match(re1, "cached pattern", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re2, "cached pattern", WREGEX_ZERO_TERMINATED) == true);
    
    wregex_free(re1);
    wregex_free(re2);
}

TEST(test_different_options_cached_separately)
{
    wregex_t *re_cs = wregex_compile("Test", 0);
    wregex_t *re_ci = wregex_compile("Test", WREGEX_CASELESS);
    
    assert(re_cs != NULL);
    assert(re_ci != NULL);
    
    /* Case-sensitive should not match lowercase */
    assert(wregex_match(re_cs, "test", WREGEX_ZERO_TERMINATED) == false);
    /* Case-insensitive should match */
    assert(wregex_match(re_ci, "test", WREGEX_ZERO_TERMINATED) == true);
    
    wregex_free(re_cs);
    wregex_free(re_ci);
}

/* ------------------------------------------------------------------
 * Error handling tests
 * ------------------------------------------------------------------ */

TEST(test_invalid_pattern)
{
    /* Unmatched parenthesis */
    wregex_t *re = wregex_compile("(unclosed", 0);
    assert(re == NULL);
}

TEST(test_null_inputs)
{
    assert(wregex_compile(NULL, 0) == NULL);
    assert(wregex_match(NULL, "test", 4) == false);
    
    wregex_t *re = wregex_compile("test", 0);
    assert(wregex_match(re, NULL, 4) == false);
    wregex_free(re);
}

/* ------------------------------------------------------------------
 * MongoDB-like regex tests (what bsonmatch uses)
 * ------------------------------------------------------------------ */

TEST(test_mongodb_style_patterns)
{
    wregex_t *re;

    /* Simple substring (most common) */
    re = wregex_compile("world", 0);
    assert(wregex_match(re, "hello world", WREGEX_ZERO_TERMINATED) == true);
    wregex_free(re);

    /* Case-insensitive (MongoDB /pattern/i) */
    re = wregex_compile("mongodb", WREGEX_CASELESS);
    assert(wregex_match(re, "MongoDB is great", WREGEX_ZERO_TERMINATED) == true);
    wregex_free(re);

    /* Anchored pattern */
    re = wregex_compile("^user_", 0);
    assert(wregex_match(re, "user_123", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "admin_user_1", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);

    /* Email-like pattern */
    re = wregex_compile("[a-z]+@[a-z]+\\.[a-z]+", WREGEX_CASELESS);
    assert(wregex_match(re, "test@example.com", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "not an email", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);
}

/* ------------------------------------------------------------------
 * Extended edge case tests
 * ------------------------------------------------------------------ */

TEST(test_empty_string)
{
    wregex_t *re;

    /* Empty pattern matches everything */
    re = wregex_compile("", 0);
    assert(re != NULL);
    assert(wregex_match(re, "anything", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "", WREGEX_ZERO_TERMINATED) == true);
    wregex_free(re);

    /* Pattern against empty string */
    re = wregex_compile(".", 0);
    assert(wregex_match(re, "", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);

    /* Empty anchored pattern */
    re = wregex_compile("^$", 0);
    assert(wregex_match(re, "", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "not empty", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);
}

TEST(test_special_characters)
{
    wregex_t *re;

    /* Escaped special chars */
    re = wregex_compile("\\[test\\]", 0);
    assert(re != NULL);
    assert(wregex_match(re, "[test]", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "test", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);

    /* Dot matches any char */
    re = wregex_compile("a.c", 0);
    assert(wregex_match(re, "abc", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "aXc", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "ac", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);

    /* Escaped dot */
    re = wregex_compile("a\\.c", 0);
    assert(wregex_match(re, "a.c", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "abc", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);

    /* Backslash */
    re = wregex_compile("\\\\", 0);
    assert(wregex_match(re, "a\\b", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "ab", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);
}

TEST(test_quantifiers)
{
    wregex_t *re;

    /* Zero or more */
    re = wregex_compile("ab*c", 0);
    assert(wregex_match(re, "ac", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "abc", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "abbbbc", WREGEX_ZERO_TERMINATED) == true);
    wregex_free(re);

    /* One or more */
    re = wregex_compile("ab+c", 0);
    assert(wregex_match(re, "ac", WREGEX_ZERO_TERMINATED) == false);
    assert(wregex_match(re, "abc", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "abbbbc", WREGEX_ZERO_TERMINATED) == true);
    wregex_free(re);

    /* Optional */
    re = wregex_compile("colou?r", 0);
    assert(wregex_match(re, "color", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "colour", WREGEX_ZERO_TERMINATED) == true);
    wregex_free(re);

    /* Exact count */
    re = wregex_compile("a{3}", 0);
    assert(wregex_match(re, "aa", WREGEX_ZERO_TERMINATED) == false);
    assert(wregex_match(re, "aaa", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "aaaa", WREGEX_ZERO_TERMINATED) == true); /* substring match */
    wregex_free(re);

    /* Range */
    re = wregex_compile("^a{2,4}$", 0);
    assert(wregex_match(re, "a", WREGEX_ZERO_TERMINATED) == false);
    assert(wregex_match(re, "aa", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "aaa", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "aaaa", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "aaaaa", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);
}

TEST(test_character_classes)
{
    wregex_t *re;

    /* Custom class */
    re = wregex_compile("[aeiou]", 0);
    assert(wregex_match(re, "a", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "x", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);

    /* Negated class */
    re = wregex_compile("[^0-9]", 0);
    assert(wregex_match(re, "a", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "5", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);

    /* Range */
    re = wregex_compile("[a-z]", 0);
    assert(wregex_match(re, "m", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "M", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);

    /* Whitespace class */
    re = wregex_compile("\\s", 0);
    assert(wregex_match(re, " ", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "\t", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "\n", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "a", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);

    /* Non-whitespace */
    re = wregex_compile("\\S+", 0);
    assert(wregex_match(re, "word", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "   ", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);
}

TEST(test_alternation)
{
    wregex_t *re;

    /* Simple alternation */
    re = wregex_compile("cat|dog", 0);
    assert(wregex_match(re, "cat", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "dog", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "bird", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);

    /* Grouped alternation */
    re = wregex_compile("(red|blue) car", 0);
    assert(wregex_match(re, "red car", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "blue car", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "green car", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);
}

TEST(test_groups)
{
    wregex_t *re;

    /* Basic grouping */
    re = wregex_compile("(ab)+", 0);
    assert(wregex_match(re, "ab", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "abab", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "aba", WREGEX_ZERO_TERMINATED) == true); /* "ab" matches */
    wregex_free(re);

    /* Non-capturing group */
    re = wregex_compile("(?:ab)+c", 0);
    assert(wregex_match(re, "abc", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "ababc", WREGEX_ZERO_TERMINATED) == true);
    wregex_free(re);

    /* Nested groups */
    re = wregex_compile("((a)(b))", 0);
    assert(wregex_match(re, "ab", WREGEX_ZERO_TERMINATED) == true);
    wregex_free(re);
}

TEST(test_anchors_extended)
{
    wregex_t *re;

    /* Start anchor */
    re = wregex_compile("^hello", 0);
    assert(wregex_match(re, "hello world", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "say hello", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);

    /* End anchor */
    re = wregex_compile("world$", 0);
    assert(wregex_match(re, "hello world", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "world domination", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);

    /* Both anchors */
    re = wregex_compile("^exact$", 0);
    assert(wregex_match(re, "exact", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "not exact", WREGEX_ZERO_TERMINATED) == false);
    assert(wregex_match(re, "exactly", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);
}

TEST(test_multiline_option)
{
    wregex_t *re;

    /* Without multiline, ^ only matches start of string */
    re = wregex_compile("^line", 0);
    assert(wregex_match(re, "line one\nline two", WREGEX_ZERO_TERMINATED) == true);
    wregex_free(re);

    /* With multiline, ^ matches start of each line */
    re = wregex_compile("^line", WREGEX_MULTILINE);
    assert(wregex_match(re, "first\nline two", WREGEX_ZERO_TERMINATED) == true);
    wregex_free(re);

    /* DOTALL makes . match newlines */
    re = wregex_compile("a.b", 0);
    assert(wregex_match(re, "a\nb", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);

    re = wregex_compile("a.b", WREGEX_DOTALL);
    assert(wregex_match(re, "a\nb", WREGEX_ZERO_TERMINATED) == true);
    wregex_free(re);
}

TEST(test_unicode_basic)
{
    wregex_t *re;

    /* UTF-8 literal match */
    re = wregex_compile("café", WREGEX_UTF);
    assert(re != NULL);
    assert(wregex_match(re, "I love café", WREGEX_ZERO_TERMINATED) == true);
    wregex_free(re);

    /* Unicode word */
    re = wregex_compile("日本", WREGEX_UTF);
    assert(re != NULL);
    assert(wregex_match(re, "日本語", WREGEX_ZERO_TERMINATED) == true);
    wregex_free(re);
}

TEST(test_lookahead)
{
    wregex_t *re;

    /* Positive lookahead */
    re = wregex_compile("foo(?=bar)", 0);
    assert(re != NULL);
    assert(wregex_match(re, "foobar", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "foobaz", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);

    /* Negative lookahead */
    re = wregex_compile("foo(?!bar)", 0);
    assert(re != NULL);
    assert(wregex_match(re, "foobaz", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "foobar", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);
}

TEST(test_lookbehind)
{
    wregex_t *re;

    /* Positive lookbehind */
    re = wregex_compile("(?<=foo)bar", 0);
    assert(re != NULL);
    assert(wregex_match(re, "foobar", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "bazbar", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);

    /* Negative lookbehind */
    re = wregex_compile("(?<!foo)bar", 0);
    assert(re != NULL);
    assert(wregex_match(re, "bazbar", WREGEX_ZERO_TERMINATED) == true);
    assert(wregex_match(re, "foobar", WREGEX_ZERO_TERMINATED) == false);
    wregex_free(re);
}

TEST(test_invalid_patterns_extended)
{
    /* Various invalid patterns should return NULL */
    assert(wregex_compile("(", 0) == NULL);
    assert(wregex_compile(")", 0) == NULL);
    assert(wregex_compile("[", 0) == NULL);
    assert(wregex_compile("*", 0) == NULL);
    assert(wregex_compile("+", 0) == NULL);
    assert(wregex_compile("?", 0) == NULL);
    /* Note: {} is valid in PCRE2 (matches empty), so not testing it */
    assert(wregex_compile("(?P<name)", 0) == NULL); /* unclosed named group */
    assert(wregex_compile("\\", 0) == NULL);        /* trailing backslash */
}

TEST(test_long_patterns)
{
    wregex_t *re;

    /* Long pattern */
    re = wregex_compile("a{1,100}", 0);
    assert(re != NULL);
    assert(wregex_match(re, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        WREGEX_ZERO_TERMINATED) == true);
    wregex_free(re);

    /* Long subject */
    re = wregex_compile("needle", 0);
    assert(re != NULL);
    char long_haystack[10001];
    memset(long_haystack, 'x', 10000);
    long_haystack[5000] = 'n';
    long_haystack[5001] = 'e';
    long_haystack[5002] = 'e';
    long_haystack[5003] = 'd';
    long_haystack[5004] = 'l';
    long_haystack[5005] = 'e';
    long_haystack[10000] = '\0';
    assert(wregex_match(re, long_haystack, WREGEX_ZERO_TERMINATED) == true);
    wregex_free(re);
}

TEST(test_binary_data)
{
    wregex_t *re;

    /* Match with embedded null using explicit length */
    re = wregex_compile("test", 0);
    assert(re != NULL);

    char data[] = "pre\0test\0post";
    /* With length, can find "test" after the null */
    assert(wregex_match(re, data + 4, 4) == true);
    wregex_free(re);
}

TEST(test_stress_cache)
{
    /* Compile many different patterns to stress the cache */
    char pattern[32];
    wregex_t *handles[100];

    for (int i = 0; i < 100; i++) {
        snprintf(pattern, sizeof(pattern), "pattern_%d", i);
        handles[i] = wregex_compile(pattern, 0);
        assert(handles[i] != NULL);
    }

    /* All should still work */
    for (int i = 0; i < 100; i++) {
        snprintf(pattern, sizeof(pattern), "pattern_%d", i);
        char subject[64];
        snprintf(subject, sizeof(subject), "test pattern_%d here", i);
        assert(wregex_match(handles[i], subject, WREGEX_ZERO_TERMINATED) == true);
    }

    for (int i = 0; i < 100; i++) {
        wregex_free(handles[i]);
    }
}

/* ------------------------------------------------------------------
 * Main
 * ------------------------------------------------------------------ */

int main(void)
{
    printf("\n=== wregex tests ===\n\n");

    printf("Basic tests:\n");
    RUN_TEST(test_simple_match);
    RUN_TEST(test_case_insensitive);
    RUN_TEST(test_case_sensitive);
    RUN_TEST(test_regex_patterns);
    RUN_TEST(test_with_length);

    printf("\nCache tests:\n");
    RUN_TEST(test_cache_reuse);
    RUN_TEST(test_different_options_cached_separately);

    printf("\nError handling:\n");
    RUN_TEST(test_invalid_pattern);
    RUN_TEST(test_null_inputs);

    printf("\nMongoDB-style patterns:\n");
    RUN_TEST(test_mongodb_style_patterns);

    printf("\nEdge cases - Empty/Special:\n");
    RUN_TEST(test_empty_string);
    RUN_TEST(test_special_characters);

    printf("\nEdge cases - Quantifiers:\n");
    RUN_TEST(test_quantifiers);

    printf("\nEdge cases - Character classes:\n");
    RUN_TEST(test_character_classes);

    printf("\nEdge cases - Alternation & Groups:\n");
    RUN_TEST(test_alternation);
    RUN_TEST(test_groups);

    printf("\nEdge cases - Anchors:\n");
    RUN_TEST(test_anchors_extended);
    RUN_TEST(test_multiline_option);

    printf("\nEdge cases - Unicode:\n");
    RUN_TEST(test_unicode_basic);

    printf("\nEdge cases - Lookahead/Lookbehind:\n");
    RUN_TEST(test_lookahead);
    RUN_TEST(test_lookbehind);

    printf("\nEdge cases - Invalid patterns:\n");
    RUN_TEST(test_invalid_patterns_extended);

    printf("\nStress tests:\n");
    RUN_TEST(test_long_patterns);
    RUN_TEST(test_binary_data);
    RUN_TEST(test_stress_cache);

    printf("\nCache statistics:\n");
    wregex_cache_stats();

    /* Cleanup */
    wregex_cache_destroy();

    printf("\n=== All tests passed! ===\n\n");

    return 0;
}