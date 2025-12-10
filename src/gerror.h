#ifndef GERROR_H
#define GERROR_H

#include <stdio.h>
#include <stddef.h> 
#include <stdarg.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct gerror_t {
    int code;
    char lib[64];
    char message[256];
} gerror_t;

void vset_error(gerror_t *error, const char *lib, int code, const char *format, va_list args);

void set_error(gerror_t *error, const char *lib, int code, const char *format, ...);

const char* error_message(const gerror_t *error);

const char* error_message_ex(
    const gerror_t *error,
    char *buffer,
    size_t buffer_size
);

void error_clear(gerror_t *error);

#ifdef __cplusplus
}
#endif

#endif
