#ifndef POUCH_SPACE_FRAME_H
#define POUCH_SPACE_FRAME_H
#include "lc_pouch_record_io.h"

/* Private container envelope; inner records retain the production encoding. */
#define PS_FRAME_TYPE 9U
#define PS_FRAME_BYTES LC_POUCH_RECORD_HEADER_BYTES
typedef struct ps_frame {
  uint64_t start, end;
  int framed;
} ps_frame;
/* Probe one recovery unit. Ordinary records use the caller's end bound.
 * Frames validate their complete header, span and CRC before returning their
 * inner bounds. Scratch is borrowed or allocated with the default allocator,
 * and must hold PS_CHUNK bytes. This never buffers the whole batch. */
int ps_frame_read(lc_pouch_record_io, uint64_t start, uint64_t end,
                  unsigned char **scratch, ps_frame *, lc_error *);
#endif
