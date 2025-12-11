#ifndef BSON_COMPARE_H
#define BSON_COMPARE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <bson/bson.h>

int bson_compare_docs(const bson_t *doc1, const bson_t *doc2);

#ifdef __cplusplus
}
#endif



#endif // BSON_COMPARE_H