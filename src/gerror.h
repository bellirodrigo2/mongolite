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

static void set_error(gerror_t *error, const char *lib, int code, const char *format, ...){
     {
    if (!error) return;
    va_list args;
    va_start(args, format);
    error->code = code;
    snprintf(error->lib, sizeof(error->lib), "%s", lib ? lib : "unknown");
    vsnprintf(error->message, sizeof(error->message), format, args);
    va_end(args);
}
}

static char* error_message(const gerror_t *error){
    return (error && error->message[0]) ? error->message : "No error";
}

static const char* error_message_ex(
    const gerror_t *error,
    char *buffer,
    size_t buffer_size
){
    if (!buffer || buffer_size == 0)
        return "Invalid buffer";

    if (!error || error->message[0] == '\0') {
        snprintf(buffer, buffer_size, "No error");
        return buffer;
    }

    if (error->lib[0]) {
        snprintf(buffer, buffer_size, "%s: %s", error->lib, error->message);
    } else {
        snprintf(buffer, buffer_size, "%s", error->message);
    }

    return buffer;
}


static void error_clear(gerror_t *error) {
    if (!error) return;
    error->code = 0;
    error->lib[0] = '\0';
    error->message[0] = '\0';
}

#ifdef __cplusplus
}
#endif

#endif
