#ifndef BSON_COMPARE_H
#define BSON_COMPARE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <bson/bson.h>
#include <stdbool.h>
#include <stddef.h>

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

/* ============================================================
 * wtree2 Index Key Extraction Callbacks
 *
 * These callbacks are used with wtree2 for automatic index maintenance.
 * user_data should be a bson_t* containing the index key specification
 * (e.g., {"email": 1} or {"name": 1, "age": -1})
 * ============================================================ */

// Standard key extractor: extracts index key from BSON document
// Returns true on success, false on error
// out_key is allocated with malloc(), caller must free()
bool bson_index_key_extractor(const void *value, size_t value_len,
                               void *user_data,
                               void **out_key, size_t *out_len);

// Check if all indexed fields are null or missing
// Returns true if document should be skipped for sparse indexes
bool bson_index_key_is_null(const void *value, size_t value_len, void *user_data);

// Sparse-aware key extractor: returns false (skip) if all fields are null/missing
// Use this for sparse indexes
bool bson_index_key_extractor_sparse(const void *value, size_t value_len,
                                      void *user_data,
                                      void **out_key, size_t *out_len);

#ifdef __cplusplus
}
#endif



#endif // BSON_COMPARE_H