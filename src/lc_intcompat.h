#ifndef LC_INTCOMPAT_H
#define LC_INTCOMPAT_H

#include <stddef.h>
#include <stdint.h>

/* All shipped toolchains provide these fixed-width types in their C90 mode. */
typedef int64_t lc_i64;
typedef uint64_t lc_u64;

/* Avoid fixed-width limit macros that expand to non-C90 integer literals. */
#define LC_I64_MAX ((lc_i64)(((lc_u64) - 1) >> 1U))
#define LC_I64_MIN (-LC_I64_MAX - 1L)
#define LC_U64_MAX ((lc_u64) - 1)

int lc_i64_parse_base10(const char *text, lc_i64 *out_value);
int lc_i64_format_base10(lc_i64 value, char *buffer, size_t buffer_size);
int lc_i64_format_base10_padded(lc_i64 value, size_t width, char *buffer,
                                size_t buffer_size);
int lc_u64_parse_base10(const char *text, lc_u64 *out_value);
int lc_u64_format_base10(lc_u64 value, char *buffer, size_t buffer_size);
int lc_u64_format_base10_padded(lc_u64 value, size_t width, char *buffer,
                                size_t buffer_size);
int lc_u64_format_base16_padded(lc_u64 value, size_t width, char *buffer,
                                size_t buffer_size);
int lc_i64_to_long_checked(lc_i64 value, long *out_value);
int lc_i64_to_int_checked(lc_i64 value, int *out_value);
int lc_u64_to_ulong_checked(lc_u64 value, unsigned long *out_value);
int lc_u64_to_size_checked(lc_u64 value, size_t *out_value);
int lc_parse_long_base10_checked(const char *text, long *out_value);
int lc_parse_int_base10_checked(const char *text, int *out_value);
int lc_parse_ulong_base10_checked(const char *text, unsigned long *out_value);
int lc_parse_long_base10_range_checked(const char *text, size_t length,
                                       long *out_value);
int lc_parse_i64_base10_range_checked(const char *text, size_t length,
                                      lc_i64 *out_value);
int lc_parse_u64_base10_range_checked(const char *text, size_t length,
                                      lc_u64 *out_value);

#endif
