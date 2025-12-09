#include "gerror.h"
#include <stdio.h>

void set_error(gerror_t *error, const char *lib, int code, const char *format, ...) {
    va_list args;
    va_start(args, format);
    vset_error(error, lib, code, format, args);
    va_end(args);
}

const char* error_message(const gerror_t *error) {
    return (error && error->message[0]) ? error->message : "No error";
}

const char* error_message_ex(
    const gerror_t *error,
    char *buffer,
    size_t buffer_size
) {
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


void error_clear(gerror_t *error) {
    if (!error) return;
    error->code = 0;
    error->lib[0] = '\0';
    error->message[0] = '\0';
}
