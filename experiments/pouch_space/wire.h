#ifndef POUCH_SPACE_WIRE_H
#define POUCH_SPACE_WIRE_H

#include <stdint.h>
#include <string.h>

/* Fields are little-endian and may start at any byte in a packed record.
 * Fixed-size copies let the compiler use native loads and stores without
 * making alignment or aliasing assumptions about the source bytes. */
static __inline__ uint64_t ps_wire_get64(const unsigned char *p) {
  uint64_t n;
  memcpy(&n, p, sizeof(n));
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
  n = __builtin_bswap64(n);
#endif
  return n;
}

static __inline__ uint32_t ps_wire_get32(const unsigned char *p) {
  uint32_t n;
  memcpy(&n, p, sizeof(n));
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
  n = __builtin_bswap32(n);
#endif
  return n;
}

static __inline__ void ps_wire_put64(unsigned char *p, uint64_t n) {
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
  n = __builtin_bswap64(n);
#endif
  memcpy(p, &n, sizeof(n));
}

static __inline__ void ps_wire_put32(unsigned char *p, uint32_t n) {
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
  n = __builtin_bswap32(n);
#endif
  memcpy(p, &n, sizeof(n));
}

#endif
