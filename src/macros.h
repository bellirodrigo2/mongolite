/**
 * macros.h - Compiler hints and optimization macros
 *
 * Provides portable macros for:
 * - Branch prediction hints (likely/unlikely)
 * - Function attributes (pure, const, malloc, nonnull, etc.)
 * - Inline hints
 * - Alignment
 */

#ifndef MACROS_H
#define MACROS_H

/* ============================================================
 * Compiler Detection
 * ============================================================ */

#if defined(__GNUC__) || defined(__clang__)
#define MONGOLITE_GCC_LIKE 1
#else
#define MONGOLITE_GCC_LIKE 0
#endif

#if defined(_MSC_VER)
#define MONGOLITE_MSVC 1
#else
#define MONGOLITE_MSVC 0
#endif

/* ============================================================
 * Branch Prediction Hints
 * ============================================================ */

#if MONGOLITE_GCC_LIKE
/**
 * MONGOLITE_LIKELY(x) - Hint that condition is likely true
 * Use for common/fast paths
 */
#define MONGOLITE_LIKELY(x)   __builtin_expect(!!(x), 1)

/**
 * MONGOLITE_UNLIKELY(x) - Hint that condition is unlikely true
 * Use for error paths, rare conditions
 */
#define MONGOLITE_UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#define MONGOLITE_LIKELY(x)   (x)
#define MONGOLITE_UNLIKELY(x) (x)
#endif

/* ============================================================
 * Function Attributes
 * ============================================================ */

#if MONGOLITE_GCC_LIKE

/**
 * MONGOLITE_PURE - Function has no side effects, only reads memory
 * Result depends only on arguments and global state.
 * Example: strlen, strcmp
 */
#define MONGOLITE_PURE __attribute__((pure))

/**
 * MONGOLITE_CONST - Function is pure AND doesn't read global memory
 * Result depends ONLY on arguments. Most restrictive.
 * Example: abs, sin (without errno)
 */
#define MONGOLITE_CONST __attribute__((const))

/**
 * MONGOLITE_MALLOC - Function returns newly allocated memory
 * Returned pointer doesn't alias anything.
 */
#define MONGOLITE_MALLOC __attribute__((malloc))

/**
 * MONGOLITE_NONNULL(...) - Specified arguments must not be NULL
 * Arguments are 1-indexed.
 * Example: MONGOLITE_NONNULL(1, 2) = args 1 and 2 must be non-null
 */
#define MONGOLITE_NONNULL(...) __attribute__((nonnull(__VA_ARGS__)))

/**
 * MONGOLITE_NONNULL_ALL - All pointer arguments must not be NULL
 */
#define MONGOLITE_NONNULL_ALL __attribute__((nonnull))

/**
 * MONGOLITE_RETURNS_NONNULL - Function never returns NULL
 */
#define MONGOLITE_RETURNS_NONNULL __attribute__((returns_nonnull))

/**
 * MONGOLITE_WARN_UNUSED - Warn if return value is ignored
 */
#define MONGOLITE_WARN_UNUSED __attribute__((warn_unused_result))

/**
 * MONGOLITE_HOT - Function is called frequently (optimize for speed)
 */
#define MONGOLITE_HOT __attribute__((hot))

/**
 * MONGOLITE_COLD - Function is rarely called (optimize for size)
 * Use for error handlers, initialization code
 */
#define MONGOLITE_COLD __attribute__((cold))

/**
 * MONGOLITE_FLATTEN - Inline all calls within this function
 * Aggressive inlining for critical paths
 */
#define MONGOLITE_FLATTEN __attribute__((flatten))

/**
 * MONGOLITE_ALWAYS_INLINE - Force inlining
 */
#define MONGOLITE_ALWAYS_INLINE __attribute__((always_inline)) inline

/**
 * MONGOLITE_NOINLINE - Never inline this function
 * Use for cold paths to reduce code size
 */
#define MONGOLITE_NOINLINE __attribute__((noinline))

/**
 * MONGOLITE_ALIGNED(n) - Align to n bytes
 */
#define MONGOLITE_ALIGNED(n) __attribute__((aligned(n)))

/**
 * MONGOLITE_PACKED - Remove padding from struct
 */
#define MONGOLITE_PACKED __attribute__((packed))

/**
 * MONGOLITE_PREFETCH(addr, rw, locality)
 * rw: 0 = read, 1 = write
 * locality: 0 = no temporal locality, 3 = high temporal locality
 */
#define MONGOLITE_PREFETCH(addr, rw, locality) __builtin_prefetch(addr, rw, locality)

#else /* MSVC or other */

#define MONGOLITE_PURE
#define MONGOLITE_CONST
#define MONGOLITE_MALLOC
#define MONGOLITE_NONNULL(...)
#define MONGOLITE_NONNULL_ALL
#define MONGOLITE_RETURNS_NONNULL
#define MONGOLITE_WARN_UNUSED
#define MONGOLITE_HOT
#define MONGOLITE_COLD
#define MONGOLITE_FLATTEN
#define MONGOLITE_ALWAYS_INLINE __forceinline
#define MONGOLITE_NOINLINE __declspec(noinline)
#define MONGOLITE_ALIGNED(n) __declspec(align(n))
#define MONGOLITE_PACKED
#define MONGOLITE_PREFETCH(addr, rw, locality)

#endif

/* ============================================================
 * Restrict Pointer (C99)
 * ============================================================ */

#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L
#define MONGOLITE_RESTRICT restrict
#elif MONGOLITE_GCC_LIKE
#define MONGOLITE_RESTRICT __restrict__
#elif MONGOLITE_MSVC
#define MONGOLITE_RESTRICT __restrict
#else
#define MONGOLITE_RESTRICT
#endif

/* ============================================================
 * Unreachable Code
 * ============================================================ */

#if MONGOLITE_GCC_LIKE
#define MONGOLITE_UNREACHABLE() __builtin_unreachable()
#elif MONGOLITE_MSVC
#define MONGOLITE_UNREACHABLE() __assume(0)
#else
#define MONGOLITE_UNREACHABLE() ((void)0)
#endif

/* ============================================================
 * Assume (Optimization Hint)
 * ============================================================ */

#if MONGOLITE_GCC_LIKE && defined(__clang__)
#define MONGOLITE_ASSUME(cond) __builtin_assume(cond)
#elif MONGOLITE_MSVC
#define MONGOLITE_ASSUME(cond) __assume(cond)
#else
#define MONGOLITE_ASSUME(cond) ((void)0)
#endif

/* ============================================================
 * Common Patterns
 * ============================================================ */

/**
 * Error check pattern - for paths that should rarely fail
 */
#define MONGOLITE_CHECK(cond) if (MONGOLITE_UNLIKELY(!(cond)))

/**
 * Success check pattern - for paths that usually succeed
 */
#define MONGOLITE_SUCCESS(cond) if (MONGOLITE_LIKELY(cond))

#endif /* MACROS */
