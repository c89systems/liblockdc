#ifndef POUCH_SPACE_RECORD_IO_H
#define POUCH_SPACE_RECORD_IO_H
#include "lc_pouch_record_io.h"
#include "stream.h"

/* One private canonical append. Caller owns this small context; no root lock
 * spans producer calls. A record writer may discard the entire private append
 * on failure. Commit/abort close its physical writer and are never implicit in
 * canonical header finalization. Stop using the borrowed I/O after either. */
typedef struct ps_record_writer {
  ps_stream_writer *writer;
  uint64_t start, tail;
  uint64_t limit; /* zero is unbounded; otherwise an absolute exclusive end */
  int failed;
} ps_record_writer;
int ps_record_begin(ps_stream *, ps_record_writer *, lc_error *);
/* Reserve a known private span without emitting or publishing record bytes. */
int ps_record_reserve(ps_record_writer *, ps_budget *, uint64_t bytes,
                      lc_error *);
lc_pouch_record_io ps_record_output(ps_record_writer *);
int ps_record_commit(ps_record_writer *, enum ps_commit_mode,
                     ps_commit_publish_fn, void *, lc_error *);
void ps_record_abort(ps_record_writer *);
/* Input borrows one already admitted reader, preserving its captured window.
 * Recovery input is usable only inside the stream's canonical recovery call. */
lc_pouch_record_io ps_record_input(ps_stream_reader *);
lc_pouch_record_io ps_record_window_input(ps_stream_window *);
lc_pouch_record_io ps_record_recovery_input(ps_stream *);
#endif
