#include "wbson.h"

inline wbson_type_t  wbson_iter_type(wbson_iter_t *iter){
    return (wbson_type_t)bson_iter_type((bson_iter_t*)iter);
}
inline int32_t wbson_iter_int32(const wbson_iter_t *iter){
    return bson_iter_int32((const bson_iter_t*)iter);
}
inline int64_t wbson_iter_int64(const wbson_iter_t *iter){
    return bson_iter_int64((const bson_iter_t*)iter);
}
inline double wbson_iter_double(const wbson_iter_t *iter){
    return bson_iter_double((const bson_iter_t*)iter);
}
inline const char* wbson_iter_utf8(const wbson_iter_t *iter, uint32_t *length){
    return bson_iter_utf8((const bson_iter_t*)iter,length);
}
inline bool wbson_iter_bool(const wbson_iter_t *iter){
    return bson_iter_bool((const bson_iter_t*)iter);
}
inline const wbson_oid_t* wbson_iter_oid(const wbson_iter_t *iter){
    return (const wbson_oid_t*)bson_iter_oid((const bson_iter_t*)iter);
}
inline int wbson_oid_compare(const wbson_oid_t *oid1, const wbson_oid_t *oid2){
    return bson_oid_compare((const bson_oid_t*)oid1,(const bson_oid_t*)oid2);
}
inline int64_t wbson_iter_date_time(const wbson_iter_t *iter){
    return bson_iter_date_time((const bson_iter_t*)iter);
}
inline void wbson_iter_document(const wbson_iter_t *iter,uint32_t *document_len,const uint8_t **document){
    bson_iter_document((const bson_iter_t*)iter,document_len,document);
}
inline void wbson_iter_array(const wbson_iter_t *iter, uint32_t *array_len, const uint8_t **array){
    bson_iter_array((const bson_iter_t*)iter,array_len,array);
}
inline bool wbson_init_static(wbson_t *bson, const uint8_t *data, size_t length){
    return bson_init_static((bson_t*)bson,data,length);
}
inline int wbson_compare(const wbson_t *bson, const wbson_t *other){
    return bson_compare((const bson_t*)bson,(const bson_t*)other);
}
inline bool wbson_iter_decimal128(const wbson_iter_t *iter,wbson_decimal128_t *dec){
    return bson_iter_decimal128((const bson_iter_t*)iter,(bson_decimal128_t*)dec);
}
inline void wbson_decimal128_to_string(const wbson_decimal128_t *dec,char *str){
    bson_decimal128_to_string((const bson_decimal128_t*)dec,str);
}