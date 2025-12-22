#include <math.h>
#include <string.h>
#include <bson/bson.h>

/*
 * NOTE:
 * This numeric comparison guarantees a total, deterministic order.
 * It matches MongoDB behavior for "safe" numeric ranges, and falls
 * back to a stable ordering for values that would lose precision
 * (large int64, decimal128, NaN, infinities).
 *
 * This is NOT a full reimplementation of MongoDB numeric comparison.
 */

#define MAX_SAFE_INT_DOUBLE (9007199254740992LL) /* 2^53 */

/* Classificação simples de risco */
static bool
number_is_safe(const bson_iter_t *it)
{
    switch (bson_iter_type(it)) {

    case BSON_TYPE_INT32:
        return true;

    case BSON_TYPE_INT64: {
        int64_t v = bson_iter_int64(it);
        return (v >= -MAX_SAFE_INT_DOUBLE &&
                v <=  MAX_SAFE_INT_DOUBLE);
    }

    case BSON_TYPE_DOUBLE: {
        double d = bson_iter_double(it);
        if (isnan(d) || isinf(d))
            return false;
        if (fabs(d) > (double)MAX_SAFE_INT_DOUBLE)
            return false;
        return true;
    }

    default:
        return false;
    }
}

/* Conversão segura → double (somente após number_is_safe == true) */
static double
number_to_double_safe(const bson_iter_t *it)
{
    switch (bson_iter_type(it)) {
    case BSON_TYPE_INT32:
        return (double)bson_iter_int32(it);
    case BSON_TYPE_INT64:
        return (double)bson_iter_int64(it);
    case BSON_TYPE_DOUBLE:
        return bson_iter_double(it);
    default:
        return 0.0; /* não deve acontecer */
    }
}

/* Fallback determinístico para casos perigosos */
static int
numeric_fallback_compare(const bson_iter_t *a,
                          const bson_iter_t *b)
{
    /* 1) ordem por tipo BSON */
    int ta = bson_iter_type(a);
    int tb = bson_iter_type(b);

    if (ta != tb)
        return ta < tb ? -1 : 1;

    /* 2) Compare by raw value based on type */
    switch (ta) {
    case BSON_TYPE_INT32: {
        int32_t va = bson_iter_int32(a);
        int32_t vb = bson_iter_int32(b);
        if (va < vb) return -1;
        if (va > vb) return 1;
        return 0;
    }
    case BSON_TYPE_INT64: {
        int64_t va = bson_iter_int64(a);
        int64_t vb = bson_iter_int64(b);
        if (va < vb) return -1;
        if (va > vb) return 1;
        return 0;
    }
    case BSON_TYPE_DOUBLE: {
        double va = bson_iter_double(a);
        double vb = bson_iter_double(b);
        /* Handle NaN - NaN == NaN for ordering purposes */
        if (isnan(va) && isnan(vb)) return 0;
        if (isnan(va)) return -1;
        if (isnan(vb)) return 1;
        /* Handle infinity */
        if (va < vb) return -1;
        if (va > vb) return 1;
        return 0;
    }
    default:
        return 0;
    }
}

/*
 * Comparação numérica "meio-termo":
 * - segura quando possível
 * - fallback determinístico quando necessário
 */
int
mongodb_compare_numbers(const bson_iter_t *a,
                        const bson_iter_t *b)
{
    bool safe_a = number_is_safe(a);
    bool safe_b = number_is_safe(b);

    /* Caso seguro: compara numericamente */
    if (safe_a && safe_b) {
        double da = number_to_double_safe(a);
        double db = number_to_double_safe(b);

        /* tratamento explícito de NaN */
        if (isnan(da) || isnan(db)) {
            if (isnan(da) && isnan(db)) return 0;
            return isnan(da) ? -1 : 1;
        }

        /* -0 e +0 são equivalentes */
        if (da == 0.0 && db == 0.0)
            return 0;

        if (da < db) return -1;
        if (da > db) return 1;
        return 0;
    }

    /* Caso perigoso: fallback estável */
    return numeric_fallback_compare(a, b);
}
