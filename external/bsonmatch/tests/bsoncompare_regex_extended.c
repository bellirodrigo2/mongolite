/*
 * bsoncompare_regex_extended.c - Extended regex tests for bsonmatch
 *
 * Tests BSON regex matching with MongoDB-style $regex and $options operators.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <bsoncompare.h>

#define TEST(name) static int name(void)
#define RUN_TEST(name) do { \
    printf("  %s... ", #name); \
    if (name()) { \
        printf("OK\n"); \
    } else { \
        printf("FAILED\n"); \
        failed++; \
    } \
} while(0)

/* Helper function to compare JSON spec against JSON doc */
static int compare_json(const char *spec_json, const char *doc_json)
{
    bson_error_t spec_error, doc_error;
    bson_t *spec = bson_new_from_json((const uint8_t*)spec_json, -1, &spec_error);
    bson_t *doc = bson_new_from_json((const uint8_t*)doc_json, -1, &doc_error);

    if (!spec || !doc) {
        if (spec) bson_destroy(spec);
        if (doc) bson_destroy(doc);
        return -1; /* parse error */
    }

    const uint8_t *spec_bson = bson_get_data(spec);
    const uint8_t *doc_bson = bson_get_data(doc);
    int result = compare(spec_bson, spec->len, doc_bson, doc->len);

    bson_destroy(spec);
    bson_destroy(doc);
    return result;
}

/* ------------------------------------------------------------------
 * Basic regex tests
 * ------------------------------------------------------------------ */

TEST(test_basic_substring_match)
{
    /* Simple substring match */
    return compare_json(
        "{\"name\": {\"$regex\": \"john\", \"$options\": \"\"}}",
        "{\"name\": \"john doe\"}"
    ) == 1;
}

TEST(test_basic_no_match)
{
    /* Should not match */
    return compare_json(
        "{\"name\": {\"$regex\": \"jane\", \"$options\": \"\"}}",
        "{\"name\": \"john doe\"}"
    ) == 0;
}

TEST(test_case_sensitive_match)
{
    /* Case sensitive - should match */
    return compare_json(
        "{\"name\": {\"$regex\": \"John\", \"$options\": \"\"}}",
        "{\"name\": \"John Doe\"}"
    ) == 1;
}

TEST(test_case_sensitive_no_match)
{
    /* Case sensitive - should NOT match */
    return compare_json(
        "{\"name\": {\"$regex\": \"john\", \"$options\": \"\"}}",
        "{\"name\": \"John Doe\"}"
    ) == 0;
}

/* ------------------------------------------------------------------
 * Case insensitive tests ($options: "i")
 * ------------------------------------------------------------------ */

TEST(test_case_insensitive_lowercase)
{
    return compare_json(
        "{\"name\": {\"$regex\": \"john\", \"$options\": \"i\"}}",
        "{\"name\": \"JOHN DOE\"}"
    ) == 1;
}

TEST(test_case_insensitive_uppercase)
{
    return compare_json(
        "{\"name\": {\"$regex\": \"JOHN\", \"$options\": \"i\"}}",
        "{\"name\": \"john doe\"}"
    ) == 1;
}

TEST(test_case_insensitive_mixed)
{
    return compare_json(
        "{\"name\": {\"$regex\": \"jOhN\", \"$options\": \"i\"}}",
        "{\"name\": \"JoHn DoE\"}"
    ) == 1;
}

/* ------------------------------------------------------------------
 * Anchored patterns (^ and $)
 * ------------------------------------------------------------------ */

TEST(test_start_anchor_match)
{
    return compare_json(
        "{\"name\": {\"$regex\": \"^John\", \"$options\": \"\"}}",
        "{\"name\": \"John Doe\"}"
    ) == 1;
}

TEST(test_start_anchor_no_match)
{
    return compare_json(
        "{\"name\": {\"$regex\": \"^Doe\", \"$options\": \"\"}}",
        "{\"name\": \"John Doe\"}"
    ) == 0;
}

TEST(test_end_anchor_match)
{
    return compare_json(
        "{\"name\": {\"$regex\": \"Doe$\", \"$options\": \"\"}}",
        "{\"name\": \"John Doe\"}"
    ) == 1;
}

TEST(test_end_anchor_no_match)
{
    return compare_json(
        "{\"name\": {\"$regex\": \"John$\", \"$options\": \"\"}}",
        "{\"name\": \"John Doe\"}"
    ) == 0;
}

TEST(test_full_match_anchors)
{
    return compare_json(
        "{\"name\": {\"$regex\": \"^John Doe$\", \"$options\": \"\"}}",
        "{\"name\": \"John Doe\"}"
    ) == 1;
}

TEST(test_full_match_anchors_no_match)
{
    return compare_json(
        "{\"name\": {\"$regex\": \"^John$\", \"$options\": \"\"}}",
        "{\"name\": \"John Doe\"}"
    ) == 0;
}

/* ------------------------------------------------------------------
 * Pattern features
 * ------------------------------------------------------------------ */

TEST(test_digit_pattern)
{
    return compare_json(
        "{\"phone\": {\"$regex\": \"\\\\d{3}-\\\\d{4}\", \"$options\": \"\"}}",
        "{\"phone\": \"555-1234\"}"
    ) == 1;
}

TEST(test_word_boundary)
{
    return compare_json(
        "{\"text\": {\"$regex\": \"\\\\bword\\\\b\", \"$options\": \"\"}}",
        "{\"text\": \"a word here\"}"
    ) == 1;
}

TEST(test_word_boundary_no_match)
{
    return compare_json(
        "{\"text\": {\"$regex\": \"\\\\bword\\\\b\", \"$options\": \"\"}}",
        "{\"text\": \"wording\"}"
    ) == 0;
}

TEST(test_alternation_active)
{
    return compare_json(
        "{\"status\": {\"$regex\": \"active|pending\", \"$options\": \"\"}}",
        "{\"status\": \"active\"}"
    ) == 1;
}

TEST(test_alternation_pending)
{
    return compare_json(
        "{\"status\": {\"$regex\": \"active|pending\", \"$options\": \"\"}}",
        "{\"status\": \"pending\"}"
    ) == 1;
}

TEST(test_alternation_no_match)
{
    /* "inactive" contains "active" so it should match! */
    return compare_json(
        "{\"status\": {\"$regex\": \"active|pending\", \"$options\": \"\"}}",
        "{\"status\": \"inactive\"}"
    ) == 1; /* substring match, not full word */
}

TEST(test_alternation_no_match_real)
{
    /* This should NOT match */
    return compare_json(
        "{\"status\": {\"$regex\": \"^(active|pending)$\", \"$options\": \"\"}}",
        "{\"status\": \"inactive\"}"
    ) == 0;
}

TEST(test_character_class)
{
    return compare_json(
        "{\"grade\": {\"$regex\": \"^[A-F]$\", \"$options\": \"\"}}",
        "{\"grade\": \"B\"}"
    ) == 1;
}

TEST(test_negated_character_class)
{
    return compare_json(
        "{\"char\": {\"$regex\": \"^[^0-9]+$\", \"$options\": \"\"}}",
        "{\"char\": \"abc\"}"
    ) == 1;
}

TEST(test_quantifier_plus)
{
    return compare_json(
        "{\"value\": {\"$regex\": \"a+\", \"$options\": \"\"}}",
        "{\"value\": \"aaa\"}"
    ) == 1;
}

TEST(test_quantifier_star)
{
    return compare_json(
        "{\"value\": {\"$regex\": \"ab*c\", \"$options\": \"\"}}",
        "{\"value\": \"ac\"}"
    ) == 1;
}

TEST(test_quantifier_optional)
{
    int match1 = compare_json(
        "{\"word\": {\"$regex\": \"colou?r\", \"$options\": \"\"}}",
        "{\"word\": \"color\"}"
    );
    int match2 = compare_json(
        "{\"word\": {\"$regex\": \"colou?r\", \"$options\": \"\"}}",
        "{\"word\": \"colour\"}"
    );
    return match1 == 1 && match2 == 1;
}

/* ------------------------------------------------------------------
 * Array field tests
 * ------------------------------------------------------------------ */

TEST(test_regex_in_array_match)
{
    return compare_json(
        "{\"tags\": {\"$regex\": \"tech\", \"$options\": \"\"}}",
        "{\"tags\": [\"technology\", \"science\", \"art\"]}"
    ) == 1;
}

TEST(test_regex_in_array_no_match)
{
    return compare_json(
        "{\"tags\": {\"$regex\": \"music\", \"$options\": \"\"}}",
        "{\"tags\": [\"technology\", \"science\", \"art\"]}"
    ) == 0;
}

TEST(test_regex_in_array_with_mixed_types)
{
    /* Array contains strings and numbers - regex should match string */
    return compare_json(
        "{\"items\": {\"$regex\": \"hello\", \"$options\": \"\"}}",
        "{\"items\": [123, \"hello world\", true]}"
    ) == 1;
}

/* ------------------------------------------------------------------
 * Nested field tests
 * ------------------------------------------------------------------ */

TEST(test_nested_field_match)
{
    return compare_json(
        "{\"user.name\": {\"$regex\": \"john\", \"$options\": \"i\"}}",
        "{\"user\": {\"name\": \"John Doe\", \"age\": 30}}"
    ) == 1;
}

TEST(test_deeply_nested_field)
{
    return compare_json(
        "{\"a.b.c.d\": {\"$regex\": \"value\", \"$options\": \"\"}}",
        "{\"a\": {\"b\": {\"c\": {\"d\": \"the value here\"}}}}"
    ) == 1;
}

/* ------------------------------------------------------------------
 * Special character tests
 * ------------------------------------------------------------------ */

TEST(test_escaped_dot)
{
    int match = compare_json(
        "{\"domain\": {\"$regex\": \"example\\\\.com\", \"$options\": \"\"}}",
        "{\"domain\": \"example.com\"}"
    );
    int no_match = compare_json(
        "{\"domain\": {\"$regex\": \"example\\\\.com\", \"$options\": \"\"}}",
        "{\"domain\": \"exampleXcom\"}"
    );
    return match == 1 && no_match == 0;
}

TEST(test_email_pattern)
{
    return compare_json(
        "{\"email\": {\"$regex\": \"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$\", \"$options\": \"\"}}",
        "{\"email\": \"test@example.com\"}"
    ) == 1;
}

TEST(test_url_pattern)
{
    return compare_json(
        "{\"url\": {\"$regex\": \"^https?://\", \"$options\": \"\"}}",
        "{\"url\": \"https://example.com/path\"}"
    ) == 1;
}

/* ------------------------------------------------------------------
 * Edge cases
 * ------------------------------------------------------------------ */

TEST(test_empty_string_match)
{
    /* Empty pattern matches empty string */
    return compare_json(
        "{\"value\": {\"$regex\": \"\", \"$options\": \"\"}}",
        "{\"value\": \"\"}"
    ) == 1;
}

TEST(test_empty_pattern_matches_any)
{
    /* Empty pattern matches any string */
    return compare_json(
        "{\"value\": {\"$regex\": \"\", \"$options\": \"\"}}",
        "{\"value\": \"anything\"}"
    ) == 1;
}

TEST(test_unicode_match)
{
    return compare_json(
        "{\"text\": {\"$regex\": \"caf\\u00e9\", \"$options\": \"\"}}",
        "{\"text\": \"I love caf\\u00e9\"}"
    ) == 1;
}

TEST(test_whitespace_pattern)
{
    return compare_json(
        "{\"text\": {\"$regex\": \"\\\\s+\", \"$options\": \"\"}}",
        "{\"text\": \"hello   world\"}"
    ) == 1;
}

TEST(test_lookahead)
{
    return compare_json(
        "{\"text\": {\"$regex\": \"foo(?=bar)\", \"$options\": \"\"}}",
        "{\"text\": \"foobar\"}"
    ) == 1;
}

TEST(test_negative_lookahead)
{
    int no_match = compare_json(
        "{\"text\": {\"$regex\": \"foo(?!bar)\", \"$options\": \"\"}}",
        "{\"text\": \"foobar\"}"
    );
    int match = compare_json(
        "{\"text\": {\"$regex\": \"foo(?!bar)\", \"$options\": \"\"}}",
        "{\"text\": \"foobaz\"}"
    );
    return no_match == 0 && match == 1;
}

/* ------------------------------------------------------------------
 * Combined with other operators
 * ------------------------------------------------------------------ */

TEST(test_regex_with_and)
{
    return compare_json(
        "{\"$and\": [{\"name\": {\"$regex\": \"john\", \"$options\": \"i\"}}, {\"age\": {\"$gte\": 18}}]}",
        "{\"name\": \"John Doe\", \"age\": 25}"
    ) == 1;
}

TEST(test_regex_with_or)
{
    int match1 = compare_json(
        "{\"$or\": [{\"name\": {\"$regex\": \"john\", \"$options\": \"i\"}}, {\"name\": {\"$regex\": \"jane\", \"$options\": \"i\"}}]}",
        "{\"name\": \"John Doe\"}"
    );
    int match2 = compare_json(
        "{\"$or\": [{\"name\": {\"$regex\": \"john\", \"$options\": \"i\"}}, {\"name\": {\"$regex\": \"jane\", \"$options\": \"i\"}}]}",
        "{\"name\": \"Jane Doe\"}"
    );
    return match1 == 1 && match2 == 1;
}

/* ------------------------------------------------------------------
 * Performance/stress tests
 * ------------------------------------------------------------------ */

TEST(test_long_string)
{
    /* Build a long string */
    char doc[2048];
    snprintf(doc, sizeof(doc), "{\"text\": \"");
    for (int i = 0; i < 100; i++) {
        strncat(doc, "abcdefghij", sizeof(doc) - strlen(doc) - 1);
    }
    strncat(doc, "NEEDLE", sizeof(doc) - strlen(doc) - 1);
    for (int i = 0; i < 100; i++) {
        strncat(doc, "klmnopqrst", sizeof(doc) - strlen(doc) - 1);
    }
    strncat(doc, "\"}", sizeof(doc) - strlen(doc) - 1);

    return compare_json(
        "{\"text\": {\"$regex\": \"NEEDLE\", \"$options\": \"\"}}",
        doc
    ) == 1;
}

TEST(test_multiple_regex_same_query)
{
    return compare_json(
        "{\"$and\": [{\"first\": {\"$regex\": \"^J\", \"$options\": \"\"}}, {\"last\": {\"$regex\": \"e$\", \"$options\": \"\"}}]}",
        "{\"first\": \"John\", \"last\": \"Doe\"}"
    ) == 1;
}

/* ------------------------------------------------------------------
 * Multiline option tests ($options: "m")
 * ------------------------------------------------------------------ */

TEST(test_multiline_anchor_start)
{
    return compare_json(
        "{\"text\": {\"$regex\": \"^second\", \"$options\": \"m\"}}",
        "{\"text\": \"first line\\nsecond line\"}"
    ) == 1;
}

TEST(test_multiline_anchor_end)
{
    return compare_json(
        "{\"text\": {\"$regex\": \"line$\", \"$options\": \"m\"}}",
        "{\"text\": \"first line\\nsecond\"}"
    ) == 1;
}

/* ------------------------------------------------------------------
 * Main
 * ------------------------------------------------------------------ */

int main(int argc, char *argv[])
{
    int failed = 0;
    (void)argc;
    (void)argv;

    printf("\n=== bsoncompare regex extended tests ===\n\n");

    printf("Basic tests:\n");
    RUN_TEST(test_basic_substring_match);
    RUN_TEST(test_basic_no_match);
    RUN_TEST(test_case_sensitive_match);
    RUN_TEST(test_case_sensitive_no_match);

    printf("\nCase insensitive tests:\n");
    RUN_TEST(test_case_insensitive_lowercase);
    RUN_TEST(test_case_insensitive_uppercase);
    RUN_TEST(test_case_insensitive_mixed);

    printf("\nAnchor tests:\n");
    RUN_TEST(test_start_anchor_match);
    RUN_TEST(test_start_anchor_no_match);
    RUN_TEST(test_end_anchor_match);
    RUN_TEST(test_end_anchor_no_match);
    RUN_TEST(test_full_match_anchors);
    RUN_TEST(test_full_match_anchors_no_match);

    printf("\nPattern features:\n");
    RUN_TEST(test_digit_pattern);
    RUN_TEST(test_word_boundary);
    RUN_TEST(test_word_boundary_no_match);
    RUN_TEST(test_alternation_active);
    RUN_TEST(test_alternation_pending);
    RUN_TEST(test_alternation_no_match);
    RUN_TEST(test_alternation_no_match_real);
    RUN_TEST(test_character_class);
    RUN_TEST(test_negated_character_class);
    RUN_TEST(test_quantifier_plus);
    RUN_TEST(test_quantifier_star);
    RUN_TEST(test_quantifier_optional);

    printf("\nArray field tests:\n");
    RUN_TEST(test_regex_in_array_match);
    RUN_TEST(test_regex_in_array_no_match);
    RUN_TEST(test_regex_in_array_with_mixed_types);

    printf("\nNested field tests:\n");
    RUN_TEST(test_nested_field_match);
    RUN_TEST(test_deeply_nested_field);

    printf("\nSpecial character tests:\n");
    RUN_TEST(test_escaped_dot);
    RUN_TEST(test_email_pattern);
    RUN_TEST(test_url_pattern);

    printf("\nEdge cases:\n");
    RUN_TEST(test_empty_string_match);
    RUN_TEST(test_empty_pattern_matches_any);
    RUN_TEST(test_unicode_match);
    RUN_TEST(test_whitespace_pattern);
    RUN_TEST(test_lookahead);
    RUN_TEST(test_negative_lookahead);

    printf("\nCombined operators:\n");
    RUN_TEST(test_regex_with_and);
    RUN_TEST(test_regex_with_or);

    printf("\nStress tests:\n");
    RUN_TEST(test_long_string);
    RUN_TEST(test_multiple_regex_same_query);

    printf("\nMultiline option tests:\n");
    RUN_TEST(test_multiline_anchor_start);
    RUN_TEST(test_multiline_anchor_end);

    /* Cleanup regex cache */
    regex_destroy();

    if (failed > 0) {
        printf("\n=== %d tests FAILED ===\n\n", failed);
        return 1;
    }

    printf("\n=== All tests passed! ===\n\n");
    return 0;
}
