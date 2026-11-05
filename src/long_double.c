/* INCRBYFLOAT's and HINCRBYFLOAT's arithmetic, as in Redis: in C's `long
 * double` (80 bits on x86-64), which Rust doesn't have, parsed and formatted as
 * Redis' `string2ld` and `ld2string(LD_STR_HUMAN)` do. In a `double`, 10.5 + 0.1
 * would come out as 10.59999999999999964 rather than 10.6. */

#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Redis' MAX_LONG_DOUBLE_CHARS. */
#define MAX_CHARS (5 * 1024)

static int parse(const char *s, size_t len, long double *out) {
    char buf[MAX_CHARS];
    char *end;
    long double value;
    if (len == 0 || len >= sizeof(buf)) return 0;
    memcpy(buf, s, len);
    buf[len] = '\0';
    errno = 0;
    value = strtold(buf, &end);
    if (isspace((unsigned char)buf[0]) || end[0] != '\0' || (size_t)(end - buf) != len ||
        (errno == ERANGE &&
         (value == HUGE_VAL || value == -HUGE_VAL || fpclassify(value) == FP_ZERO)) ||
        errno == EINVAL || isnan(value)) {
        return 0;
    }
    *out = value;
    return 1;
}

/* Adds `incr` to `current` (0 if NULL) and writes the sum to `out`, which holds
 * TANTALUM_LD_CHARS bytes, setting `*out_len`. Returns 0, or: 1, `current` isn't
 * a number; 2, `incr` isn't; 3, `incr` isn't finite; 4, the sum isn't. */
int tantalum_long_double_add(const char *current, size_t current_len, const char *incr,
                             size_t incr_len, char *out, size_t *out_len) {
    long double value = 0, by;
    int written;
    char *p;
    size_t len;
    if (current != NULL && !parse(current, current_len, &value)) return 1;
    if (!parse(incr, incr_len, &by)) return 2;
    if (isnan(by) || isinf(by)) return 3;
    value += by;
    if (isnan(value) || isinf(value)) return 4;
    written = snprintf(out, MAX_CHARS, "%.17Lf", value);
    if (written < 0 || written + 1 > MAX_CHARS) return 4;
    len = (size_t)written;
    /* Without the zeroes after the point, or the point if that's all. */
    if (memchr(out, '.', len) != NULL) {
        p = out + len - 1;
        while (*p == '0') {
            p--;
            len--;
        }
        if (*p == '.') len--;
    }
    /* Not "-0". */
    if (len == 2 && out[0] == '-' && out[1] == '0') {
        out[0] = '0';
        len = 1;
    }
    *out_len = len;
    return 0;
}

/* A number for SORT, as Redis reads it with `strtod`: up to a NUL, with what
 * `strtod` allows. Returns 0 if there's more after it, or it's out of range or
 * NaN. */
int tantalum_strtod(const char *s, size_t len, double *out) {
    char *copy = malloc(len + 1);
    char *end;
    double value;
    int ok;
    if (copy == NULL) return 0;
    memcpy(copy, s, len);
    copy[len] = '\0';
    errno = 0;
    value = strtod(copy, &end);
    ok = end[0] == '\0' && errno != ERANGE && !isnan(value);
    free(copy);
    *out = value;
    return ok;
}
