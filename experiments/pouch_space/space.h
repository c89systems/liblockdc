#ifndef POUCH_SPACE_EXPERIMENT_H
#define POUCH_SPACE_EXPERIMENT_H

#include "lc/lc.h"
#include <stdint.h>
#include <sys/types.h>

/* Experimental physical storage boundary, deliberately outside liblockdc's
 * public ABI and normal Pouch open path. Handles must not be used after fork.
 * A writer/reader belongs to one caller; independent callers share a space.
 * Stop concurrent calls before closing the space itself. */
#define PS_MIB UINT64_C(1048576)
#define PS_DEFAULT_CAPACITY (10 * PS_MIB)
/* Native experiment: filesystem block-sized physical allocation with small
 * logical tails sharing independently owned cells. */
#define PS_PAGE 4096U
#define PS_CELL 64U
#define PS_CHUNK 16384U
#define PS_METADATA_RESERVE 131072U
#define PS_JOURNAL 131072U
#define PS_JOURNAL_OFFSET (2U * PS_PAGE + PS_METADATA_RESERVE)
#define PS_DATA (PS_JOURNAL_OFFSET + PS_JOURNAL)
#define PS_MAINTENANCE_RESERVE PS_CHUNK
#define PS_ROOT_BYTES 80U

enum ps_kind { PS_STATE = 1, PS_COLLECTION = 2, PS_STREAM = 3 };
typedef struct ps_space ps_space;
typedef struct ps_writer ps_writer;
typedef struct ps_reader ps_reader;
typedef struct ps_key {
  uint64_t owner, key;
  unsigned int kind;
} ps_key;
typedef struct ps_ref {
  unsigned char container[16];
  uint64_t incarnation;
} ps_ref;
typedef struct ps_stats {
  /* allocated_bytes counts physical payload reservations once per page.
   * Retired/private bytes attribute whole runs or rounded cells to owners;
   * packed_bytes includes those owners and excludes unused page slack. */
  uint64_t capacity, live_bytes, allocated_bytes, retired_bytes, private_bytes;
  uint64_t entries, metadata_bytes, journal_bytes, syncs, checkpoints;
  uint64_t sync_requests, sync_batches, max_sync_batch;
  uint64_t metadata_pages, directory_page_writes, descriptor_page_writes;
  uint64_t packed_pages, packed_bytes;
} ps_stats;

/* Optional syscall seam for deterministic storage faults, not another backend.
 * NULL selects the real native Linux operations. Every callback has POSIX
 * error/short-I/O semantics; reserve is Linux fallocate, never zero filling. */
typedef struct ps_io {
  void *context;
  ssize_t (*read)(void *, int, void *, size_t, off_t);
  ssize_t (*write)(void *, int, const void *, size_t, off_t);
  int (*sync)(void *, int);
  int (*reserve)(void *, int, off_t, off_t);
  /* Optional preallocated zero-range operation; NULL or unsupported selects
   * bounded offset writes. It must preserve file size and backing allocation.
   */
  int (*zero)(void *, int, off_t, off_t);
} ps_io;
typedef struct ps_options {
  uint64_t initial_capacity; /* zero selects 10 MiB; creation only */
  uint64_t maximum_capacity; /* zero means no configured maximum */
  int durable_sync;          /* zero is NoSync */
  const ps_io *io;
} ps_options;

/* create != 0 requires a new pathname; open never creates or truncates. */
int ps_open(const char *, int create, const ps_options *, ps_space **,
            lc_error *);
/* Close requires every writer and reader to be closed. It does not implicitly
 * checkpoint or make NoSync publications durable. Error leaves the handle. */
int ps_close(ps_space *, lc_error *);
int ps_begin(ps_space *, ps_key, ps_writer **, lc_error *);
int ps_write(ps_writer *, const void *, size_t, lc_error *);
/* Publication replaces STATE, but rejects a currently bound COLLECTION key.
 * These are physical record keys: permanent collection position allocation and
 * tombstone/receipt authority belong to the future Pouch adapter. Deletion
 * removes the binding; this foundation does not remember deleted positions.
 * Publication consumes no writer handle.
 * Durable success waits for a file sync covering this publication; concurrent
 * callers can share a barrier, without an artificial batching delay.
 * I/O errors poison the space; outcome may be uncertain. Reopen/reconcile. */
int ps_publish(ps_writer *, ps_ref *, lc_error *);
void ps_writer_close(ps_writer *);
/* Readers pin an immutable incarnation across replacement, delete/reclaim.
 * Durable reads select the completed commit prefix, keeping the prior value
 * visible while replacements/deletions await sync. NoSync reads select the
 * latest complete publication. */
int ps_read_open(ps_space *, ps_key, const ps_ref *, ps_reader **, lc_error *);
int ps_read(ps_reader *, void *, size_t, size_t *, lc_error *);
uint64_t ps_read_size(const ps_reader *);
void ps_reader_close(ps_reader *);
int ps_delete(ps_space *, ps_key, lc_error *);
/* Durably checkpoint metadata, retire the previous recoverable root, then
 * reclaim unpinned retired runs. No live payload copying or file shrinking. */
int ps_checkpoint(ps_space *, lc_error *);
/* Explicit growth only, in whole MiB increments. Never exposes trailing
 * physical capacity until its checkpoint publication has completed. */
int ps_grow(ps_space *, uint64_t, lc_error *);
int ps_status(ps_space *, ps_stats *, lc_error *);

#endif
