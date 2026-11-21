#ifndef POUCH_SPACE_CRC_H
#define POUCH_SPACE_CRC_H

#include <stddef.h>
#include <stdint.h>
#include <zlib.h>

#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
uint32_t ps_crc32_pclmul(uint32_t crc, const unsigned char *bytes, size_t size);
#endif

/* IEEE CRC-32 with the same seed and result convention as zlib. */
static __inline__ uint32_t ps_crc32(uint32_t crc, const void *bytes,
                                    size_t size) {
#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
  if (size >= 32 && bytes && __builtin_cpu_supports("pclmul"))
    return ps_crc32_pclmul(crc, bytes, size);
#endif
  return (uint32_t)crc32_z(crc, bytes, size);
}

#endif
