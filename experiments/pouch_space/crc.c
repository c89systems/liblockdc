/* Reflected IEEE folding adapted from Chromium's zlib crc32_simd.c.
 * Copyright 2017 The Chromium Authors
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of Google LLC nor the names of its
 *    contributors may be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
#include "crc.h"

#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
#include <wmmintrin.h>

static __inline__ __attribute__((target("pclmul"))) __m128i
fold(__m128i crc, __m128i input, __m128i factor) {
  __m128i low = _mm_clmulepi64_si128(crc, factor, 0x00);
  __m128i high = _mm_clmulepi64_si128(crc, factor, 0x11);
  return _mm_xor_si128(input, _mm_xor_si128(low, high));
}

__attribute__((target("pclmul"))) uint32_t
ps_crc32_pclmul(uint32_t crc, const unsigned char *bytes, size_t size) {
  const __m128i stride_64 =
      _mm_set_epi64x(UINT64_C(0x01c6e41596), UINT64_C(0x0154442bd4));
  const __m128i stride_16 =
      _mm_set_epi64x(UINT64_C(0x00ccaa009e), UINT64_C(0x01751997d0));
  const __m128i stride_4 = _mm_set_epi64x(0, UINT64_C(0x0163cd6124));
  const __m128i polynomial =
      _mm_set_epi64x(UINT64_C(0x01f7011641), UINT64_C(0x01db710641));
  const __m128i mask = _mm_setr_epi32(-1, 0, -1, 0);
  __m128i a = _mm_loadu_si128((const __m128i_u *)bytes);
  __m128i product;

  a = _mm_xor_si128(a, _mm_cvtsi32_si128((int)~crc));
  bytes += 16;
  size -= 16;
  if (size >= 48) {
    __m128i b = _mm_loadu_si128((const __m128i_u *)bytes);
    __m128i c = _mm_loadu_si128((const __m128i_u *)(bytes + 16));
    __m128i d = _mm_loadu_si128((const __m128i_u *)(bytes + 32));
    bytes += 48;
    size -= 48;
    while (size >= 64) {
      a = fold(a, _mm_loadu_si128((const __m128i_u *)bytes), stride_64);
      b = fold(b, _mm_loadu_si128((const __m128i_u *)(bytes + 16)), stride_64);
      c = fold(c, _mm_loadu_si128((const __m128i_u *)(bytes + 32)), stride_64);
      d = fold(d, _mm_loadu_si128((const __m128i_u *)(bytes + 48)), stride_64);
      bytes += 64;
      size -= 64;
    }
    a = fold(a, b, stride_16);
    a = fold(a, c, stride_16);
    a = fold(a, d, stride_16);
  }
  while (size >= 16) {
    a = fold(a, _mm_loadu_si128((const __m128i_u *)bytes), stride_16);
    bytes += 16;
    size -= 16;
  }

  /* Fold to 64 bits, then reduce modulo the reflected IEEE polynomial. */
  product = _mm_clmulepi64_si128(a, stride_16, 0x10);
  a = _mm_xor_si128(_mm_srli_si128(a, 8), product);
  product = _mm_clmulepi64_si128(_mm_and_si128(a, mask), stride_4, 0x00);
  a = _mm_xor_si128(_mm_srli_si128(a, 4), product);
  product = _mm_clmulepi64_si128(_mm_and_si128(a, mask), polynomial, 0x10);
  product =
      _mm_clmulepi64_si128(_mm_and_si128(product, mask), polynomial, 0x00);
  a = _mm_xor_si128(a, product);
  crc = ~(uint32_t)_mm_cvtsi128_si32(_mm_srli_si128(a, 4));
  return size ? (uint32_t)crc32_z(crc, bytes, size) : crc;
}
#endif
