#ifndef WBSON_H
#define WBSON_H

#ifdef __cplusplus
extern "C" {
#endif

#include <bson/bson.h>

#ifdef __cplusplus
}
#endif

typedef bson_t wbson_t;

typedef bson_oid_t wbson_oid_t;

typedef bson_type_t wbson_type_t;

typedef bson_iter_t wbson_iter_t;

typedef bson_decimal128_t wbson_decimal128_t;

wbson_type_t  wbson_iter_type(wbson_iter_t *iter);
int32_t wbson_iter_int32(const wbson_iter_t *iter);
int64_t wbson_iter_int64(const wbson_iter_t *iter);
double wbson_iter_double(const wbson_iter_t *iter);
const char* wbson_iter_utf8(const wbson_iter_t *iter, uint32_t *length);
bool wbson_iter_bool(const wbson_iter_t *iter);
const wbson_oid_t* wbson_iter_oid(const wbson_iter_t *iter);
int wbson_oid_compare(const wbson_oid_t *oid1, const wbson_oid_t *oid2);
int64_t wbson_iter_date_time(const wbson_iter_t *iter);
void wbson_iter_document(const wbson_iter_t *iter,uint32_t *document_len,const uint8_t **document);
void wbson_iter_array(const wbson_iter_t *iter, uint32_t *array_len, const uint8_t **array); 
bool wbson_init_static(wbson_t *bson, const uint8_t *data, size_t length);
int wbson_compare(const wbson_t *bson, const wbson_t *other);
bool wbson_iter_decimal128(const wbson_iter_t *iter,wbson_decimal128_t *dec);
void wbson_decimal128_to_string(const wbson_decimal128_t *dec,char *str);
#endif // WBSON_H