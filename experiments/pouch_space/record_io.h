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
} ps_record_writer;
int ps_record_begin(ps_stream *, ps_record_writer *, lc_error *);
lc_pouch_record_io ps_record_output(ps_record_writer *);
int ps_record_commit(ps_record_writer *, lc_error *);
void ps_record_abort(ps_record_writer *);
/* Input borrows one already admitted reader, preserving its captured window.
 * Recovery input is usable only inside the stream's canonical recovery call. */
lc_pouch_record_io ps_record_input(ps_stream_reader *);
lc_pouch_record_io ps_record_window_input(ps_stream_window *);
lc_pouch_record_io ps_record_recovery_input(ps_stream *);
#endif
