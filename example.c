// example_modular.c - Exemplo do uso modular com gerror

#include "wtree.h"
#include "gerror.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

// Exemplo de outra lib que usa o mesmo sistema de erro
void my_app_function(const char *param, gerror_t *error) {
    if (!param) {
        set_error(error, "myapp", 100, "Parameter cannot be NULL");
        return;
    }
    // ... do something
}

// Função helper para imprimir erros com detalhes
void print_error(const char *context, gerror_t *error) {
    char buffer[512];
    printf("[ERROR in %s] %s\n", context, 
           error_message_ex(error, buffer, sizeof(buffer)));
}

// Exemplo de comparação customizada
int compare_numeric_keys(const MDB_val *a, const MDB_val *b) {
    if (a->mv_size != sizeof(int) || b->mv_size != sizeof(int)) {
        return memcmp(a->mv_data, b->mv_data, 
                     (a->mv_size < b->mv_size) ? a->mv_size : b->mv_size);
    }
    
    int ka = *(int*)a->mv_data;
    int kb = *(int*)b->mv_data;
    return (ka < kb) ? -1 : (ka > kb) ? 1 : 0;
}

int main() {
    // Error handler global que pode ser usado por múltiplas libs
    gerror_t error = {0};
    
    printf("=== Modular WTREE + GERROR Example ===\n\n");
    
    // 1. Criar database
    printf("1. Creating database...\n");
    wtree_db_t *db = wtree_db_create("./testdb", 0, 0, &error);
    if (!db) {
        print_error("db_create", &error);
        return 1;
    }
    printf("   ✓ Database created\n\n");
    
    // 2. Criar tree com comparação customizada
    printf("2. Creating tree with custom comparison...\n");
    wtree_tree_t *tree = wtree_tree_create(db, "numbers", 0, &error);
    if (!tree) {
        print_error("tree_create", &error);
        wtree_db_close(db);
        return 1;
    }
    
    // Setar comparação numérica
    if (wtree_tree_set_compare(tree, compare_numeric_keys, &error) != 0) {
        print_error("tree_set_compare", &error);
        // Continua mesmo com erro (usa comparação padrão)
    } else {
        printf("   ✓ Custom numeric comparison set\n");
    }
    printf("\n");
    
    // 3. Inserir dados numéricos
    printf("3. Inserting numeric data...\n");
    int keys[] = {100, 20, 300, 15, 250};
    const char *values[] = {"hundred", "twenty", "three hundred", "fifteen", "two fifty"};
    
    for (int i = 0; i < 5; i++) {
        if (wtree_insert_one(tree, &keys[i], sizeof(int), 
                            values[i], strlen(values[i]) + 1, &error) != 0) {
            char buffer[512];
            printf("   ✗ Failed to insert %d: %s\n", keys[i],
                   error_message_ex(&error, buffer, sizeof(buffer)));
            error_clear(&error);  // Limpar para próxima operação
        } else {
            printf("   ✓ Inserted: %d -> %s\n", keys[i], values[i]);
        }
    }
    printf("\n");
    
    // 4. Testar erro de outra lib usando mesmo sistema
    printf("4. Testing error from another module...\n");
    my_app_function(NULL, &error);
    if (error.code != 0) {
        char buffer[512];
        printf("   Got expected error: %s\n", 
               error_message_ex(&error, buffer, sizeof(buffer)));
        error_clear(&error);
    }
    printf("\n");
    
    // 5. Iterar em ordem (deve ser numérica, não lexical)
    printf("5. Iterating in numeric order...\n");
    wtree_iterator_t *iter = wtree_iterator_create(tree, &error);
    if (!iter) {
        print_error("iterator_create", &error);
    } else {
        printf("   Keys in order:\n");
        for (wtree_iterator_first(iter); 
             wtree_iterator_valid(iter); 
             wtree_iterator_next(iter)) {
            
            void *key, *value;
            size_t key_size, value_size;
            
            if (wtree_iterator_key(iter, &key, &key_size) &&
                wtree_iterator_value(iter, &value, &value_size)) {
                int k = *(int*)key;
                printf("   - %d: %s\n", k, (char*)value);
            }
        }
        wtree_iterator_close(iter);
    }
    printf("\n");
    
    // 6. Busca direta
    printf("6. Direct key lookup...\n");
    int search_key = 250;
    void *found_value;
    size_t found_size;
    
    if (wtree_get(tree, &search_key, sizeof(int), &found_value, &found_size, &error) == 0) {
        printf("   ✓ Found key %d: %s\n", search_key, (char*)found_value);
        free(found_value);
    } else {
        print_error("get", &error);
    }
    printf("\n");
    
    // 7. Listar todas as trees
    printf("7. Listing all trees...\n");
    size_t count;
    char **trees = wtree_tree_list(db, &count, &error);
    if (trees) {
        printf("   Found %zu tree(s):\n", count);
        for (size_t i = 0; i < count; i++) {
            printf("   - %s\n", trees[i]);
        }
        wtree_tree_list_free(trees, count);
    } else if (count == 0) {
        printf("   No named trees found (using default tree)\n");
    } else {
        print_error("tree_list", &error);
    }
    printf("\n");
    
    // 8. Stats
    printf("8. Database statistics...\n");
    MDB_stat stat;
    if (wtree_db_stats(db, &stat, &error) == 0) {
        printf("   Page size: %u\n", stat.ms_psize);
        printf("   Tree depth: %u\n", stat.ms_depth);
        printf("   Entries: %zu\n", stat.ms_entries);
        printf("   Leaf pages: %zu\n", stat.ms_leaf_pages);
        printf("   Branch pages: %zu\n", stat.ms_branch_pages);
        printf("   Overflow pages: %zu\n", stat.ms_overflow_pages);
    } else {
        print_error("db_stats", &error);
    }
    printf("\n");
    
    // Cleanup
    printf("9. Cleaning up...\n");
    wtree_tree_close(tree);
    wtree_db_close(db);
    
    // Deletar database (opcional)
    if (wtree_db_delete("./testdb", &error) == 0) {
        printf("   ✓ Database deleted\n");
    } else {
        print_error("db_delete", &error);
    }
    
    printf("\n=== Example completed successfully ===\n");
    return 0;
}
