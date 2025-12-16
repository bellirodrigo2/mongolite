#ifndef BSON_COMPARE_H
#define BSON_COMPARE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <bson/bson.h>

// Compara dois documentos BSON inteiros (ordem MongoDB)
int bson_compare_docs(const bson_t *doc1, const bson_t *doc2);

// Compara valores BSON de dois iteradores (ordem MongoDB)
// Retorna: -1 se a < b, 0 se a == b, 1 se a > b
int mongodb_compare_iter(const bson_iter_t *a, const bson_iter_t *b);

// Extrai campos de um documento para criar uma index key
// doc: documento fonte
// keys: especificação do índice, ex: {"name": 1, "age": -1}
// Retorna: novo bson_t alocado com os campos extraídos (caller deve chamar bson_destroy)
//          NULL se erro
bson_t* bson_extract_index_key(const bson_t *doc, const bson_t *keys);

#ifdef __cplusplus
}
#endif



#endif // BSON_COMPARE_H