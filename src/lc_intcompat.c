#include "lc_intcompat.h"

#include <limits.h>
#include <string.h>

static lc_u64 lc_i64_positive_limit(void) {
  lc_u64 limit;

  limit = (lc_u64)-1;
  limit >>= 1;
  return limit;
}

static lc_u64 lc_i64_negative_limit(void) {
  return lc_i64_positive_limit() + 1U;
}

int lc_i64_parse_base10(const char *text, lc_i64 *out_value) {
  const unsigned char *cursor;
  lc_u64 value;
  lc_u64 limit;
  int negative;
  unsigned int digit;

  if (text == NULL || out_value == NULL || text[0] == '\0') {
    return 0;
  }

  cursor = (const unsigned char *)text;
  negative = 0;
  if (*cursor == '-') {
    negative = 1;
    ++cursor;
  } else if (*cursor == '+') {
    ++cursor;
  }
  if (*cursor == '\0') {
    return 0;
  }

  limit = negative ? lc_i64_negative_limit() : lc_i64_positive_limit();
  value = 0U;
  while (*cursor != '\0') {
    if (*cursor < (unsigned char)'0' || *cursor > (unsigned char)'9') {
      return 0;
    }
    digit = (unsigned int)(*cursor - (unsigned char)'0');
    if (value > (limit - (lc_u64)digit) / 10U) {
      return 0;
    }
    value = (value * 10U) + (lc_u64)digit;
    ++cursor;
  }

  if (negative) {
    if (value == lc_i64_negative_limit()) {
      *out_value = -((lc_i64)(value - 1U)) - 1;
    } else {
      *out_value = -(lc_i64)value;
    }
  } else {
    *out_value = (lc_i64)value;
  }

  return 1;
}

int lc_i64_format_base10(lc_i64 value, char *buffer, size_t buffer_size) {
  char scratch[32];
  lc_u64 magnitude;
  size_t count;
  size_t i;
  int negative;

  if (buffer == NULL || buffer_size == 0U) {
    return -1;
  }

  negative = value < 0;
  if (negative) {
    magnitude = (lc_u64)(-(value + 1)) + 1U;
  } else {
    magnitude = (lc_u64)value;
  }

  count = 0U;
  do {
    scratch[count++] = (char)('0' + (magnitude % 10U));
    magnitude /= 10U;
  } while (magnitude != 0U);

  if (negative) {
    scratch[count++] = '-';
  }
  if (count + 1U > buffer_size) {
    return -1;
  }

  for (i = 0U; i < count; ++i) {
    buffer[i] = scratch[count - i - 1U];
  }
  buffer[count] = '\0';
  return (int)count;
}

int lc_i64_format_base10_padded(lc_i64 value, size_t width, char *buffer,
                                size_t buffer_size) {
  char digits[32];
  size_t digit_count;
  size_t zero_count;
  size_t prefix;
  int formatted;

  formatted = lc_i64_format_base10(value, digits, sizeof(digits));
  if (formatted < 0 || buffer == NULL || buffer_size == 0U) {
    return -1;
  }
  digit_count = (size_t)formatted;
  zero_count = width > digit_count ? width - digit_count : 0U;
  if (zero_count > buffer_size || digit_count > buffer_size - zero_count - 1U) {
    return -1;
  }
  prefix = digits[0] == '-' ? 1U : 0U;
  if (prefix != 0U) {
    buffer[0] = '-';
  }
  memset(buffer + prefix, '0', zero_count);
  memcpy(buffer + prefix + zero_count, digits + prefix,
         digit_count - prefix + 1U);
  return (int)(digit_count + zero_count);
}

int lc_u64_parse_base10(const char *text, lc_u64 *out_value) {
  const unsigned char *cursor;
  lc_u64 value;
  unsigned int digit;

  if (text == NULL || out_value == NULL || text[0] == '\0') {
    return 0;
  }

  cursor = (const unsigned char *)text;
  if (*cursor == '+') {
    ++cursor;
  } else if (*cursor == '-') {
    return 0;
  }
  if (*cursor == '\0') {
    return 0;
  }

  value = 0U;
  while (*cursor != '\0') {
    if (*cursor < (unsigned char)'0' || *cursor > (unsigned char)'9') {
      return 0;
    }
    digit = (unsigned int)(*cursor - (unsigned char)'0');
    if (value > (((lc_u64)-1) - (lc_u64)digit) / 10U) {
      return 0;
    }
    value = (value * 10U) + (lc_u64)digit;
    ++cursor;
  }

  *out_value = value;
  return 1;
}

int lc_u64_format_base10(lc_u64 value, char *buffer, size_t buffer_size) {
  char scratch[32];
  size_t count;
  size_t i;

  if (buffer == NULL || buffer_size == 0U) {
    return -1;
  }
  count = 0U;
  do {
    scratch[count++] = (char)('0' + (value % 10U));
    value /= 10U;
  } while (value != 0U);
  if (count + 1U > buffer_size) {
    return -1;
  }
  for (i = 0U; i < count; ++i) {
    buffer[i] = scratch[count - i - 1U];
  }
  buffer[count] = '\0';
  return (int)count;
}

int lc_u64_format_base10_padded(lc_u64 value, size_t width, char *buffer,
                                size_t buffer_size) {
  char digits[32];
  size_t digit_count;
  size_t zero_count;
  size_t i;
  int formatted;

  formatted = lc_u64_format_base10(value, digits, sizeof(digits));
  if (formatted < 0 || buffer == NULL) {
    return -1;
  }
  digit_count = (size_t)formatted;
  zero_count = width > digit_count ? width - digit_count : 0U;
  if (buffer_size == 0U || zero_count > buffer_size ||
      digit_count > buffer_size - zero_count - 1U) {
    return -1;
  }
  for (i = 0U; i < zero_count; ++i) {
    buffer[i] = '0';
  }
  memcpy(buffer + zero_count, digits, digit_count + 1U);
  return (int)(zero_count + digit_count);
}

int lc_u64_format_base16_padded(lc_u64 value, size_t width, char *buffer,
                                size_t buffer_size) {
  static const char digits[] = "0123456789abcdef";
  char scratch[32];
  size_t digit_count;
  size_t zero_count;
  size_t i;

  if (buffer == NULL || buffer_size == 0U) {
    return -1;
  }
  digit_count = 0U;
  do {
    scratch[digit_count++] = digits[(unsigned int)(value & 15U)];
    value >>= 4U;
  } while (value != 0U);
  zero_count = width > digit_count ? width - digit_count : 0U;
  if (zero_count > buffer_size || digit_count > buffer_size - zero_count - 1U) {
    return -1;
  }
  for (i = 0U; i < zero_count; ++i) {
    buffer[i] = '0';
  }
  for (i = 0U; i < digit_count; ++i) {
    buffer[zero_count + i] = scratch[digit_count - i - 1U];
  }
  buffer[zero_count + digit_count] = '\0';
  return (int)(zero_count + digit_count);
}

int lc_i64_to_long_checked(lc_i64 value, long *out_value) {
  if (out_value == NULL) {
    return 0;
  }
  if (value < (lc_i64)LONG_MIN || value > (lc_i64)LONG_MAX) {
    return 0;
  }
  *out_value = (long)value;
  return 1;
}

int lc_i64_to_int_checked(lc_i64 value, int *out_value) {
  if (out_value == NULL) {
    return 0;
  }
  if (value < (lc_i64)INT_MIN || value > (lc_i64)INT_MAX) {
    return 0;
  }
  *out_value = (int)value;
  return 1;
}

int lc_u64_to_ulong_checked(lc_u64 value, unsigned long *out_value) {
  if (out_value == NULL) {
    return 0;
  }
  if (value > (lc_u64)ULONG_MAX) {
    return 0;
  }
  *out_value = (unsigned long)value;
  return 1;
}

int lc_u64_to_size_checked(lc_u64 value, size_t *out_value) {
  if (out_value == NULL) {
    return 0;
  }
  if (value > (lc_u64)((size_t)-1)) {
    return 0;
  }
  *out_value = (size_t)value;
  return 1;
}

int lc_parse_long_base10_checked(const char *text, long *out_value) {
  lc_i64 value;

  return lc_i64_parse_base10(text, &value) &&
         lc_i64_to_long_checked(value, out_value);
}

int lc_parse_int_base10_checked(const char *text, int *out_value) {
  lc_i64 value;

  return lc_i64_parse_base10(text, &value) &&
         lc_i64_to_int_checked(value, out_value);
}

int lc_parse_ulong_base10_checked(const char *text, unsigned long *out_value) {
  lc_u64 value;

  return lc_u64_parse_base10(text, &value) &&
         lc_u64_to_ulong_checked(value, out_value);
}

static int lc_parse_base10_copy(const char *text, size_t length, char *buffer,
                                size_t buffer_size, size_t *out_length) {
  size_t start;
  size_t end;
  size_t trimmed;

  if (text == NULL || buffer == NULL || out_length == NULL ||
      buffer_size == 0U) {
    return 0;
  }
  start = 0U;
  end = length;
  while (start < end && (text[start] == ' ' || text[start] == '\t' ||
                         text[start] == '\r' || text[start] == '\n')) {
    ++start;
  }
  while (end > start && (text[end - 1U] == ' ' || text[end - 1U] == '\t' ||
                         text[end - 1U] == '\r' || text[end - 1U] == '\n')) {
    --end;
  }
  trimmed = end - start;
  if (trimmed == 0U || trimmed + 1U > buffer_size) {
    return 0;
  }
  memcpy(buffer, text + start, trimmed);
  buffer[trimmed] = '\0';
  *out_length = trimmed;
  return 1;
}

int lc_parse_long_base10_range_checked(const char *text, size_t length,
                                       long *out_value) {
  char buffer[64];
  size_t parsed_length;

  if (!lc_parse_base10_copy(text, length, buffer, sizeof(buffer),
                            &parsed_length)) {
    return 0;
  }
  (void)parsed_length;
  return lc_parse_long_base10_checked(buffer, out_value);
}

int lc_parse_i64_base10_range_checked(const char *text, size_t length,
                                      lc_i64 *out_value) {
  char buffer[64];
  size_t parsed_length;

  if (!lc_parse_base10_copy(text, length, buffer, sizeof(buffer),
                            &parsed_length)) {
    return 0;
  }
  (void)parsed_length;
  return lc_i64_parse_base10(buffer, out_value);
}

int lc_parse_u64_base10_range_checked(const char *text, size_t length,
                                      lc_u64 *out_value) {
  char buffer[64];
  size_t parsed_length;

  if (!lc_parse_base10_copy(text, length, buffer, sizeof(buffer),
                            &parsed_length)) {
    return 0;
  }
  (void)parsed_length;
  return lc_u64_parse_base10(buffer, out_value);
}
