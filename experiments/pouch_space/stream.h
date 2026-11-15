#ifndef POUCH_SPACE_STREAM_H
#define POUCH_SPACE_STREAM_H

#include "space.h"

typedef struct ps_stream ps_stream;
typedef struct ps_stream_writer ps_stream_writer;
typedef struct ps_stream_reader ps_stream_reader;
typedef struct ps_stream_window ps_stream_window;
typedef struct ps_stream_info {
  ps_ref ref;
  uint64_t tail, capacity, runs, readers;
  uint64_t preferred_grant_bytes;
  int sealed, retired;
} ps_stream_info;

/* The canonical owner validates its existing record format, starting at the
 * trusted checkpoint tail, and returns the last complete record boundary.
 * This callback runs outside the root/allocator mutex. Only it may inspect
 * candidate bytes beyond the published tail through recovery_read().
 * Readers may use that validated prefix without repairing the unused suffix.
 * The stream layer clears and synchronizes the rejected suffix before its
 * first write admission. Newly recovered bytes still participate in root
 * readiness and checkpoint dependencies. The layer never interprets
 * application records or adds a per-record WAL.
 */
typedef int (*ps_stream_recover_fn)(ps_stream *, uint64_t checkpoint_tail,
                                    uint64_t capacity, uint64_t *accepted_tail,
                                    void *, lc_error *);
/* The optional budget funds head creation only, never subsequent grants. */
int ps_stream_open(ps_space *, ps_budget *, ps_key, int create, const ps_ref *,
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
 * or allocator locks. Buffers are bounded at PS_CHUNK. A recovered owner
 * repairs its unused suffix before returning its first writer; failure returns
 * no writer and retains the existing uncertain-I/O poison semantics. */
int ps_stream_begin(ps_stream *, ps_stream_writer **, lc_error *);
uint64_t ps_stream_writer_start(const ps_stream_writer *);
/* Reserve at least bytes from this append's start without advancing its tail
 * or writing canonical bytes. Existing grants stay private to this stream.
 * Allocation failure may leave reusable zero grants and fails this writer.
 * Caller knows a bounded canonical span, not necessarily the producer's size.
 */
/* A budget funds this reservation call only and is not retained by the cached
 * stream. Further automatic growth uses ordinary admission. Geometric grants
 * can exceed bytes; use reserve_exact with the conservative span planner. */
int ps_stream_reserve(ps_stream_writer *, ps_budget *, uint64_t bytes,
                      lc_error *);
/* Known final span: retain geometric growth for full grants, but clip the last
 * grant to the remaining page-rounded need. Intended for sealed builds. */
int ps_stream_reserve_exact(ps_stream_writer *, ps_budget *, uint64_t bytes,
                            lc_error *);
int ps_stream_write(ps_stream_writer *, const void *, size_t, lc_error *);
/* Relative to this private append, never to previously committed bytes.
 * Supports the canonical PENDING-header/metadata fixup before publication. */
int ps_stream_write_at(ps_stream_writer *, uint64_t, const void *, size_t,
                       lc_error *);
/* The canonical owner has completed its records before commit. Durable
 * success waits for the root sync covering these bytes; no publication journal
 * entry is emitted when appending inside an already owned run. An optional
 * publisher selects a containing logical head before exposing this tail;
 * durable roots check its success against the shared ready prefix. An explicit
 * DEFER_SYNC acknowledges completed bytes before the caller's group barrier;
 * it never relaxes a publisher's dependency coverage requirement. */
int ps_stream_commit(ps_stream_writer *, enum ps_commit_mode,
                     ps_commit_publish_fn, void *, lc_error *);
/* An abandoned written suffix is cleared and synced before the next appender.
 * A failed cleanup poisons storage; close/reopen reconciles uncertain I/O. */
void ps_stream_writer_close(ps_stream_writer *);
/* Readers snapshot an immutable committed range across subsequent appends.
 * Canonical record/payload checksums remain the record owner's responsibility.
 */
int ps_stream_read_open(ps_stream *, uint64_t offset, uint64_t bytes,
                        ps_stream_reader **, lc_error *);
/* Lightweight captured range: same lifetime/retirement pin as a buffered
 * reader, with no sequential cursor or body buffer. Metadata views may retain
 * many windows while sharing bounded scratch across the actual replay. */
int ps_stream_window_open(ps_stream *, uint64_t offset, uint64_t bytes,
                          ps_stream_window **, lc_error *);
int ps_stream_window_read(ps_stream_window *, uint64_t offset, void *, size_t,
                          lc_error *);
void ps_stream_window_close(ps_stream_window *);
int ps_stream_read(ps_stream_reader *, void *, size_t, size_t *, lc_error *);
/* Exact offset reads inside this reader's captured logical window. Does not
 * change its sequential cursor or buffer; no fresh admission or allocation. */
int ps_stream_read_at(ps_stream_reader *, uint64_t offset, void *, size_t,
                      lc_error *);
void ps_stream_reader_close(ps_stream_reader *);

#endif
