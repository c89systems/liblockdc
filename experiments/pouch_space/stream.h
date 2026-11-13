#ifndef POUCH_SPACE_STREAM_H
#define POUCH_SPACE_STREAM_H

#include "space.h"

typedef struct ps_stream ps_stream;
typedef struct ps_stream_writer ps_stream_writer;
typedef struct ps_stream_reader ps_stream_reader;
typedef struct ps_stream_info {
  ps_ref ref;
  uint64_t tail, capacity, runs;
  int sealed, retired;
} ps_stream_info;

/* The canonical owner validates its existing record format, starting at the
 * trusted checkpoint tail, and returns the last complete record boundary.
 * This callback runs outside the root/allocator mutex. Only it may inspect
 * candidate bytes beyond the published tail through recovery_read(). The
 * stream layer then clears and synchronizes the rejected suffix before write
 * admission. It never interprets application records or adds a per-record WAL.
 */
typedef int (*ps_stream_recover_fn)(ps_stream *, uint64_t checkpoint_tail,
                                    uint64_t capacity, uint64_t *accepted_tail,
                                    void *, lc_error *);
int ps_stream_open(ps_space *, ps_key, int create, const ps_ref *,
                   ps_stream_recover_fn, void *, ps_stream **, lc_error *);
/* One cached owner per typed logical stream. Close requires no reader or
 * appender. Stop concurrent API calls before close; readers pin this owner. */
int ps_stream_close(ps_stream *, lc_error *);
int ps_stream_status(ps_stream *, ps_stream_info *, lc_error *);
/* No active appender; readers may remain open. These structural operations
 * always wait for durability, including NoSync stores. Sealing is idempotent
 * and forbids further appends. Retirement is idempotent and forbids new read
 * admission; existing readers remain valid until closed. Neither closes the
 * owner. Physical reuse still waits for checkpoint retirement and reader pins.
 * A sealed reopen trusts its final tail and does not call canonical recovery.
 */
int ps_stream_seal(ps_stream *, lc_error *);
int ps_stream_retire(ps_stream *, lc_error *);
int ps_stream_recovery_read(ps_stream *, uint64_t offset, void *, size_t,
                            lc_error *);
/* At most one appender per logical stream; contention is an explicit error.
 * Other streams and readers remain independent. The producer never owns root
 * or allocator locks. Buffers are bounded at PS_CHUNK. */
int ps_stream_begin(ps_stream *, ps_stream_writer **, lc_error *);
uint64_t ps_stream_writer_start(const ps_stream_writer *);
int ps_stream_write(ps_stream_writer *, const void *, size_t, lc_error *);
/* Relative to this private append, never to previously committed bytes.
 * Supports the canonical PENDING-header/metadata fixup before publication. */
int ps_stream_write_at(ps_stream_writer *, uint64_t, const void *, size_t,
                       lc_error *);
/* The canonical owner has completed its records before commit. Durable
 * success waits for the root sync covering these bytes; no publication journal
 * entry is emitted when appending inside an already owned run. */
int ps_stream_commit(ps_stream_writer *, lc_error *);
/* An abandoned written suffix is cleared and synced before the next appender.
 * A failed cleanup poisons storage; close/reopen reconciles uncertain I/O. */
void ps_stream_writer_close(ps_stream_writer *);
/* Readers snapshot an immutable committed range across subsequent appends.
 * Canonical record/payload checksums remain the record owner's responsibility.
 */
int ps_stream_read_open(ps_stream *, uint64_t offset, uint64_t bytes,
                        ps_stream_reader **, lc_error *);
int ps_stream_read(ps_stream_reader *, void *, size_t, size_t *, lc_error *);
void ps_stream_reader_close(ps_stream_reader *);

#endif
