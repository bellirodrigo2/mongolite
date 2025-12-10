#ifndef BSON_COMPARE_H
#define BSON_COMPARE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "wbson.h"

int wbson_compare_docs(const wbson_t *doc1, const wbson_t *doc2);

#ifdef __cplusplus
}
#endif



#endif // BSON_COMPARE_H