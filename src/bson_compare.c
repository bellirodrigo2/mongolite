#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>     // strtod
#include <math.h>       // isnan, isinf (se quiser usar)

#include "bson_compare.h"
#include "wbson.h"

/* ============================================================
 * 1) PRECEDÊNCIA DE TIPOS (ORDEM OFICIAL DO MONGODB)
 *    https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/
 * ============================================================ */

static int get_mongodb_type_precedence(wbson_type_t type) {
    switch (type) {
        case BSON_TYPE_MINKEY:     return 1;
        case BSON_TYPE_NULL:       return 2;

        /* 3) Todos os números: int32, int64, double, decimal128 */
        case BSON_TYPE_INT32:
        case BSON_TYPE_INT64:
        case BSON_TYPE_DOUBLE:
        case BSON_TYPE_DECIMAL128: return 3;

        /* 4) Strings/símbolos */
        case BSON_TYPE_UTF8:
        case BSON_TYPE_SYMBOL:     return 4;

        /* 5..12) Restante conforme docs */
        case BSON_TYPE_DOCUMENT:   return 5;
        case BSON_TYPE_ARRAY:      return 6;
        case BSON_TYPE_BINARY:     return 7;
        case BSON_TYPE_OID:        return 8;
        case BSON_TYPE_BOOL:       return 9;
        case BSON_TYPE_DATE_TIME:  return 10;
        case BSON_TYPE_TIMESTAMP:  return 11;
        case BSON_TYPE_REGEX:      return 12;

        /* 15) MaxKey */
        case BSON_TYPE_MAXKEY:     return 15;

        default:
            /* Tipos não suportados: jogar lá pra cima, logo acima de MaxKey */
            return 14;
    }
}

/* ============================================================
 * 2) CONVERSÃO NUMÉRICA PARA DOUBLE
 *    (para comparação mista entre int/double/decimal)
 *
 *    Obs: isso não preserva toda precisão de decimal128, mas
 *    é suficientemente compatível com o comportamento do Mongo
 *    na maior parte dos casos.
 * ============================================================ */

static double mongodb_numeric_to_double(const wbson_iter_t *it) {
    wbson_type_t t = wbson_iter_type(it);

    switch (t) {
        case BSON_TYPE_INT32:
            return (double) wbson_iter_int32(it);

        case BSON_TYPE_INT64:
            return (double) wbson_iter_int64(it);

        case BSON_TYPE_DOUBLE:
            return wbson_iter_double(it);

        case BSON_TYPE_DECIMAL128: {
            wbson_decimal128_t dec;
            char buf[BSON_DECIMAL128_STRING];

            wbson_iter_decimal128(it, &dec);
            wbson_decimal128_to_string(&dec, buf);

            /* strtod faz o parse da representação textual */
            return strtod(buf, NULL);
        }

        default:
            /* Não deveria chegar aqui para tipos numéricos */
            return 0.0;
    }
}

/* ============================================================
 * 3) COMPARAÇÃO NUMÉRICA
 *
 *    - int32/int64 vs int32/int64: compara em int64 (sem perda)
 *    - demais (double/decimal128 ou mistos): converte pra double
 * ============================================================ */

static int mongodb_compare_numbers(const wbson_iter_t *a, const wbson_iter_t *b) {
    wbson_type_t ta = wbson_iter_type(a);
    wbson_type_t tb = wbson_iter_type(b);

    bool a_is_int =
        (ta == BSON_TYPE_INT32) || (ta == BSON_TYPE_INT64);
    bool b_is_int =
        (tb == BSON_TYPE_INT32) || (tb == BSON_TYPE_INT64);

    bool a_is_dec = (ta == BSON_TYPE_DECIMAL128);
    bool b_is_dec = (tb == BSON_TYPE_DECIMAL128);

    /* Caso 1: ambos inteiros (int32/int64) → compara em int64 */
    if (a_is_int && b_is_int) {
        int64_t ia = (ta == BSON_TYPE_INT32)
                        ? (int64_t) wbson_iter_int32(a)
                        : wbson_iter_int64(a);

        int64_t ib = (tb == BSON_TYPE_INT32)
                        ? (int64_t) wbson_iter_int32(b)
                        : wbson_iter_int64(b);

        if (ia < ib) return -1;
        if (ia > ib) return 1;
        return 0;
    }

    /* Caso 2: qualquer combinação envolvendo double/decimal128
     *         ou mistura int/float → converte tudo para double.
     *         Isso replica o comportamento "numéricos equivalentes"
     *         do MongoDB, com a limitação de precisão do double.
     */
    double da = mongodb_numeric_to_double(a);
    double db = mongodb_numeric_to_double(b);

    if (da < db) return -1;
    if (da > db) return 1;

    /* Tratamento extra opcional para NaN, se quiser:
     * if (isnan(da) && !isnan(db)) return -1; etc...
     */

    return 0;
}

/* ============================================================
 * 4) FORWARD DECLARATION: COMPARAÇÃO DE VALORES (ITERADORES)
 * ============================================================ */

int mongodb_compare_iter(const wbson_iter_t *a, const wbson_iter_t *b);

/* ============================================================
 * 5) COMPARAÇÃO DE DOCUMENTOS (RECURSIVA)
 *
 *    Regras (MongoDB):
 *    - compara pares chave/valor na ordem em que aparecem;
 *    - se chave difere, usa strcmp da chave;
 *    - se chave igual, compara valor;
 *    - documento que acabar primeiro é "menor".
 * ============================================================ */

int wbson_compare_docs(const wbson_t *doc1, const wbson_t *doc2) {
    wbson_iter_t it1, it2;

    bool ok1 = wbson_iter_init(&it1, doc1);
    bool ok2 = wbson_iter_init(&it2, doc2);

    while (ok1 && ok2) {
        const char *k1 = wbson_iter_key(&it1);
        const char *k2 = wbson_iter_key(&it2);

        int keyCmp = strcmp(k1, k2);
        if (keyCmp != 0)
            return (keyCmp < 0) ? -1 : 1;

        int valCmp = mongodb_compare_iter(&it1, &it2);
        if (valCmp != 0)
            return valCmp;

        ok1 = wbson_iter_next(&it1);
        ok2 = wbson_iter_next(&it2);
    }

    if (ok1) return 1;   /* doc1 tem mais campos → maior */
    if (ok2) return -1;  /* doc2 tem mais campos → maior */
    return 0;
}

/* ============================================================
 * 6) COMPARAÇÃO DE VALORES BSON (ITERADORES)
 * ============================================================ */

int mongodb_compare_iter(const wbson_iter_t *a, const wbson_iter_t *b) {
    wbson_type_t ta = wbson_iter_type(a);
    wbson_type_t tb = wbson_iter_type(b);

    int pa = get_mongodb_type_precedence(ta);
    int pb = get_mongodb_type_precedence(tb);

    /* Se tipos diferentes, só a precedência importa */
    if (pa != pb)
        return (pa < pb) ? -1 : 1;

    /* Mesma “classe” de tipo → comparar pelo valor */
    switch (ta) {

        case BSON_TYPE_MINKEY:
        case BSON_TYPE_MAXKEY:
        case BSON_TYPE_NULL:
            return 0;

        case BSON_TYPE_BOOL: {
            bool va = wbson_iter_bool(a);
            bool vb = wbson_iter_bool(b);
            if (va < vb) return -1;
            if (va > vb) return 1;
            return 0;
        }

        case BSON_TYPE_UTF8:
        case BSON_TYPE_SYMBOL: {
            uint32_t la, lb;
            const char *sa = wbson_iter_utf8(a, &la);
            const char *sb = wbson_iter_utf8(b, &lb);

            uint32_t min = (la < lb) ? la : lb;
            int cmp = memcmp(sa, sb, min);
            if (cmp != 0)
                return (cmp < 0) ? -1 : 1;

            if (la < lb) return -1;
            if (la > lb) return 1;
            return 0;
        }

        case BSON_TYPE_INT32:
        case BSON_TYPE_INT64:
        case BSON_TYPE_DOUBLE:
        case BSON_TYPE_DECIMAL128:
            return mongodb_compare_numbers(a, b);

        case BSON_TYPE_OID: {
            const wbson_oid_t *oa = wbson_iter_oid(a);
            const wbson_oid_t *ob = wbson_iter_oid(b);
            return wbson_oid_compare(oa, ob);
        }

        case BSON_TYPE_DATE_TIME: {
            int64_t da = wbson_iter_date_time(a);
            int64_t db = wbson_iter_date_time(b);
            if (da < db) return -1;
            if (da > db) return 1;
            return 0;
        }

        case BSON_TYPE_TIMESTAMP: {
            uint32_t ts_a, inc_a, ts_b, inc_b;
            wbson_iter_timestamp(a, &ts_a, &inc_a);
            wbson_iter_timestamp(b, &ts_b, &inc_b);

            if (ts_a < ts_b) return -1;
            if (ts_a > ts_b) return 1;

            if (inc_a < inc_b) return -1;
            if (inc_a > inc_b) return 1;

            return 0;
        }

        case BSON_TYPE_BINARY: {
            bson_subtype_t sub_a, sub_b;
            uint32_t len_a, len_b;
            const uint8_t *data_a, *data_b;

            wbson_iter_binary(a, &sub_a, &len_a, &data_a);
            wbson_iter_binary(b, &sub_b, &len_b, &data_b);

            /* MongoDB: BinData: primeiro tamanho, depois subtipo, depois bytes */
            if (len_a < len_b) return -1;
            if (len_a > len_b) return 1;

            if (sub_a < sub_b) return -1;
            if (sub_a > sub_b) return 1;

            uint32_t min = len_a; /* iguais aqui */
            int cmp = memcmp(data_a, data_b, min);
            if (cmp != 0)
                return (cmp < 0) ? -1 : 1;

            return 0;
        }

        case BSON_TYPE_DOCUMENT:
        case BSON_TYPE_ARRAY: {
            uint32_t len_a, len_b;
            const uint8_t *data_a, *data_b;
            wbson_iter_document(a, &len_a, &data_a);
            wbson_iter_document(b, &len_b, &data_b);

            wbson_t A, B;
            wbson_init_static(&A, data_a, len_a);
            wbson_init_static(&B, data_b, len_b);

            return wbson_compare_docs(&A, &B);
        }

        case BSON_TYPE_REGEX: {
            const char *patA, *optA;
            const char *patB, *optB;

            wbson_iter_regex(a, &patA, &optA);
            wbson_iter_regex(b, &patB, &optB);

            int cmp = strcmp(patA, patB);
            if (cmp != 0) return (cmp < 0) ? -1 : 1;

            cmp = strcmp(optA, optB);
            if (cmp != 0) return (cmp < 0) ? -1 : 1;

            return 0;
        }

        default:
            /* Tipo “estranho” mas com mesma precedência:
             * aqui você pode dar um assert() em debug, se quiser.
             */
            return 0;
    }
}
