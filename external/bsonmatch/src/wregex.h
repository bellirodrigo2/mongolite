/*
 * wregex.h - PCRE2 wrapper with global cache
 * 
 * Thread-safe regex compilation cache with on-demand match_data allocation.
 * Designed for use with bsonmatch (MongoDB-like query matcher).
 */

#ifndef WREGEX_H
#define WREGEX_H

#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque handle for compiled regex */
typedef struct wregex wregex_t;

/* Options (mirrors PCRE2 options) */
#define WREGEX_CASELESS     0x00000008u  /* PCRE2_CASELESS */
#define WREGEX_MULTILINE    0x00000400u  /* PCRE2_MULTILINE */
#define WREGEX_DOTALL       0x00000020u  /* PCRE2_DOTALL */
#define WREGEX_UTF          0x00080000u  /* PCRE2_UTF */

/*
 * wregex_compile - Compile a regex pattern (cached)
 *
 * @pattern: The regex pattern string (null-terminated)
 * @options: Compilation options (WREGEX_* flags)
 *
 * Returns: Pointer to compiled regex, or NULL on error.
 *          Call wregex_free() when done to free the wrapper.
 *          The underlying compiled regex remains cached.
 *
 * Thread-safety: Safe to call from multiple threads.
 */
wregex_t *wregex_compile(const char *pattern, unsigned int options);

/*
 * wregex_match - Test if subject matches the compiled regex
 *
 * @re:      Compiled regex from wregex_compile()
 * @subject: String to match against
 * @len:     Length of subject (use WREGEX_ZERO_TERMINATED for null-terminated)
 *
 * Returns: true if match found, false otherwise.
 *
 * Thread-safety: Safe to call from multiple threads with same regex.
 */
#define WREGEX_ZERO_TERMINATED ((size_t)-1)
bool wregex_match(wregex_t *re, const char *subject, size_t len);

/*
 * wregex_cache_destroy - Free all cached regexes
 *
 * Call this at program shutdown to free all memory.
 * After calling this, all wregex_t pointers become invalid.
 *
 * Thread-safety: NOT safe to call while other threads use regexes.
 */
void wregex_cache_destroy(void);

/*
 * wregex_free - Free wrapper for wregex_t
 */
void wregex_free(wregex_t *re);

/*
 * wregex_cache_stats - Print cache statistics (for debugging)
 */
void wregex_cache_stats(void);

#ifdef __cplusplus
}
#endif

#endif /* WREGEX_H */