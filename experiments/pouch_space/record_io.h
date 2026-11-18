#ifndef POUCH_SPACE_RECORD_IO_H
#define POUCH_SPACE_RECORD_IO_H
#include "lc_pouch_record.h"
#include "lc_pouch_record_io.h"
#include "stream.h"

/* One private canonical append. Caller owns this small context; no root lock
 * spans producer calls. A record writer may discard the entire private append
 * on failure. Commit/abort close its physical writer and are never implicit in
 * canonical header finalization. Stop using the borrowed I/O after either. */
typedef int (*ps_record_admit_fn)(void *, const lc_pouch_record_header *,
                                  const void *key, const void *metadata,
                                  lc_error *);
typedef struct ps_record_writer {
  ps_stream_writer *writer;
  uint64_t start, tail;
  uint64_t limit; /* zero is unbounded; otherwise an absolute exclusive end */
  uint64_t frame_length; /* zero is an ordinary canonical append */
  unsigned long frame_crc;
  int frame_dirty;
  int failed;
  ps_record_admit_fn admit;
  void *admission_context;
  uint64_t record_start, record_end;
} ps_record_writer;
int ps_record_begin(ps_stream *, ps_record_writer *, lc_error *);
/* Reserve a known private span without emitting or publishing record bytes. */
int ps_record_reserve(ps_record_writer *, ps_budget *, uint64_t bytes,
                      lc_error *);
/* The caller reserves the extra header before beginning this bounded frame.
 * CRC accumulates over appended bytes. Inner canonical fixups trigger a bounded
 * checksum of the final private bytes at commit instead. */
int ps_record_frame_begin(ps_record_writer *, uint64_t length, lc_error *);
/* Install once, before the first inner record. The callback borrows the
 * structured header/key/metadata prefix (the first three vectors, optionally
 * followed by already encoded inline payload) before any prefix write or body
 * producer. PENDING fixups retain the boundary until finalized; body chunks
 * are never interpreted as prefixes. Used to fund checkpoint obligations. */
int ps_record_admission_set(ps_record_writer *, ps_record_admit_fn, void *,
                            lc_error *);
lc_pouch_record_io ps_record_output(ps_record_writer *);
int ps_record_commit(ps_record_writer *, enum ps_commit_mode,
                     ps_commit_publish_fn, void *, lc_error *);
void ps_record_abort(ps_record_writer *);
/* Input borrows one already admitted reader, preserving its captured window.
 * Recovery input is usable only inside the stream's canonical recovery call. */
lc_pouch_record_io ps_record_input(ps_stream_reader *);
lc_pouch_record_io ps_record_value_input(ps_reader *);
lc_pouch_record_io ps_record_window_input(ps_stream_window *);
lc_pouch_record_io ps_record_recovery_input(ps_stream *);
#endif
