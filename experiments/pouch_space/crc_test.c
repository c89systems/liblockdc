#define _GNU_SOURCE
#include "crc.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>

static void check(int condition) {
  if (!condition)
    abort();
}

static void verify(const unsigned char *bytes, size_t size, size_t split,
                   uint32_t seed) {
  uint32_t expected = (uint32_t)crc32_z(seed, bytes, size);
  uint32_t direct = ps_crc32(seed, bytes, size);
  uint32_t chained =
      ps_crc32(ps_crc32(seed, bytes, split), bytes + split, size - split);
  if (direct != expected || chained != expected) {
    fprintf(stderr, "CRC mismatch at length %zu, split %zu, seed %08x\n", size,
            split, (unsigned int)seed);
    abort();
  }
}

int main(void) {
  unsigned char *mapping, *end;
  long page = sysconf(_SC_PAGESIZE);
  size_t bytes, size, offset, tail;
  uint32_t value = UINT32_C(0xabbad00d);
  check(page > 0);
  bytes = 8 * (size_t)page;
  mapping = mmap(NULL, bytes + (size_t)page, PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  check(mapping != MAP_FAILED);
  end = mapping + bytes;
  check(mprotect(end, (size_t)page, PROT_NONE) == 0);
  for (offset = 0; offset < bytes; ++offset) {
    value ^= value << 13;
    value ^= value >> 17;
    value ^= value << 5;
    mapping[offset] = (unsigned char)value;
  }

  check(ps_crc32(0, "123456789", 9) == UINT32_C(0xcbf43926));
  check(ps_crc32(value, NULL, 512) == 0);
  verify(end, 0, 0, value);
  for (size = 1; size <= 1024; ++size)
    verify(end - size, size, size / 2, (uint32_t)size * value);
  for (offset = 0; offset < 32; ++offset)
    for (tail = 0; tail < 16; ++tail) {
      size = 4 * (size_t)page + tail;
      verify(mapping + offset, size, 127 + tail, value);
      verify(end - size, size, size - 128 - offset, 0);
    }
  check(munmap(mapping, bytes + (size_t)page) == 0);
  return 0;
}
