#include <stdio.h>
#include <regex.h>
#include <string.h>

int main() {
    const char *pattern = ".*@.*\\.com";
    const char *text = "alice@example.com";
    
    regex_t regex;
    int flags = REG_EXTENDED;
    
    printf("Testing pattern: '%s'\n", pattern);
    printf("Against text: '%s'\n", text);
    
    int compile_result = regcomp(&regex, pattern, flags);
    if (compile_result != 0) {
        char error_buf[100];
        regerror(compile_result, &regex, error_buf, sizeof(error_buf));
        printf("Regex compilation failed: %s\n", error_buf);
        return 1;
    }
    
    int match_result = regexec(&regex, text, 0, NULL, 0);
    printf("Match result: %d (0=match, non-zero=no match)\n", match_result);
    
    if (match_result == 0) {
        printf("✓ Pattern matches!\n");
    } else {
        printf("✗ Pattern does not match\n");
    }
    
    regfree(&regex);
    return 0;
}