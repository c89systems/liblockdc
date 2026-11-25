#ifndef POUCH_SPACE_EXPERIMENT_H
#define POUCH_SPACE_EXPERIMENT_H

#include "lc/lc.h"
#include <stdint.h>
#include <sys/types.h>

/* Experimental physical storage boundary, deliberately outside liblockdc's
 * public ABI and normal Pouch open path. Handles must not be used after fork.
 * A writer/reader belongs to one caller; independent callers share a space.
 * Stop concurrent calls before closing the space itself. */
#define PS_MIB ((uint64_t)1048576)
#define PS_DEFAULT_CAPACITY (10 * PS_MIB)
/* The upper half of signed file offsets is reserved for stream,
 * credit-record and transaction witnesses. It has no physical pages. */
#define PS_MAX_CAPACITY ((uint64_t)1 << 62)
#define PS_TRANSACTION_GUARD_SLOTS ((uint64_t)1 << 60)
#define PS_CREDIT_RECORD_SLOTS (PS_MAX_CAPACITY / PS_PAGE)
#define PS_TRANSACTION_GUARD_BASE                                              \
  (((uint64_t)-1 >> 1U) - PS_TRANSACTION_GUARD_SLOTS)
#define PS_CREDIT_RECORD_BASE                                                  \
  (PS_TRANSACTION_GUARD_BASE - PS_CREDIT_RECORD_SLOTS)
/* The top page of advisory addresses below the credit records is reserved
 * for namespace handoff. The incarnation grant boundary is still aligned
 * down to the same complete token range. */
#define PS_MAX_INCARNATION (PS_CREDIT_RECORD_BASE - PS_MAX_CAPACITY - PS_PAGE)
/* Native experiment: filesystem block-sized physical allocation with small
 * logical tails sharing independently owned cells. */
#define PS_PAGE 4096U
#define PS_CELL 64U
#define PS_CHUNK 16384U
/* Canonical ownership and relocation granularity. Streaming I/O still uses
 * PS_CHUNK, including when a piece spans several I/O windows. */
#define PS_STREAM_PIECE_BYTES 32768U
#define PS_METADATA_RESERVE 131072U
/* Isolated volatile coordination page, excluded from descriptor ownership. */
#define PS_READINESS_OFFSET (2U * PS_PAGE)
#define PS_METADATA_START (3U * PS_PAGE)
#define PS_METADATA_WORKSPACE (PS_METADATA_RESERVE - PS_PAGE)
#define PS_JOURNAL 131072U
#define PS_JOURNAL_OFFSET (2U * PS_PAGE + PS_METADATA_RESERVE)
#define PS_DATA (PS_JOURNAL_OFFSET + PS_JOURNAL)
#define PS_MAINTENANCE_RESERVE PS_CHUNK
#define PS_ROOT_BYTES 92U
/* The incarnation grant slots occupy unused sectors of the two root pages. */
#define PS_TOKEN_RECORD_OFFSET 512U
#define PS_TOKEN_RECORD_BYTES 40U

enum ps_kind {
  PS_STATE = 1,
  PS_COLLECTION = 2,
  PS_STREAM = 3,
  /* Canonical control values: replaceable like STATE, but
   * always publish, read and delete at a completed durable prefix, even in
   * NoSync. Replacements may use 4 KiB of the protected maintenance reserve;
   * checkpoint releases superseded values before that allowance is reused.
   * Unbudgeted controls are at most one page. Larger controls require an
   * explicit capacity claim covering their complete payload and descriptor;
   * they acquire no larger automatic replacement allowance. */
  PS_CONTROL = 6,
  /* Bounded logical routing values use the same replacement allowance as
   * CONTROL, but follow root durability. Ownership and allocation authority
   * must remain in always-durable controls/catalogs/grants. */
  PS_ROUTING = 7,
  /* Immutable derived query values. Domain kind and segmented identity occupy
   * separate physical kinds; owner is the real namespace, key is generation.
   * They share value allocation, publication and recovery, never stream seals
   * or catalog aliases. Keep the domain range independent of logical formats.
   */
  PS_QUERY_FIRST = 16,
  PS_QUERY_LAST = 37,
  /* Rebuildable index page keyed by collection owner and row ordinal. */
  PS_COLLECTION_QUERY = 38,
  /* Canonical per-key metadata and retained transaction decisions. Numeric
   * slot identities belong to the actual namespace. Checkpoint selection
   * binds their coverage/high-water control through the same directory. */
  PS_KEY_METADATA = 40,
  PS_KEY_DECISION = 41,
  PS_NAMESPACE_CHECKPOINT = 42
};
typedef struct ps_space ps_space;
/* Completed journal publication and durable acknowledgement prefixes. Scalar
 * observation only; no I/O, allocation, waiting or publication authority. */
int ps_publication_prefix(ps_space *, uint64_t *, uint64_t *, lc_error *);
typedef struct ps_budget ps_budget;
typedef struct ps_writer ps_writer;
typedef struct ps_reader ps_reader;
typedef struct ps_values ps_values;
/* Canonical append acknowledgement only. Structural publications retain their
 * existing barriers. DEFER_SYNC requires a caller-owned completion barrier;
 * it changes neither the root policy nor another writer's acknowledgement. */
enum ps_commit_mode { PS_COMMIT_DEFAULT = 0, PS_COMMIT_DEFER_SYNC };
/* Optional logical publication after complete canonical bytes are ready,
 * before their tail becomes visible. On durable roots, success must durably
 * cover those bytes through this same space. NoSync logical publication may
 * roll back together with its canonical bytes. Failure on a
 * healthy space must leave no authoritative logical publication. Called
 * outside allocator/commit locks; no callback may reuse its active appender. */
typedef int (*ps_commit_publish_fn)(void *, uint64_t tail, lc_error *);
typedef struct ps_key {
  uint64_t owner, key;
  unsigned int kind;
} ps_key;
typedef struct ps_ref {
  unsigned char container[16];
  uint64_t incarnation;
} ps_ref;
typedef struct ps_stats {
  /* allocated_bytes counts physical payload reservations once per page and
   * unbound foreign workspace quarantined during recovery.
   * Retired/private bytes attribute whole runs or rounded cells to owners;
   * packed_bytes includes those owners and excludes unused page slack.
   * Recovered unbound foreign pins count whole quarantined pages as retired.
   * A recovered packed page held by foreign pins remains allocated, with only
   * its known local cells attributed in packed_bytes; no old bitmap is guessed.
   */
  uint64_t capacity, live_bytes, allocated_bytes, retired_bytes, private_bytes;
  uint64_t entries, metadata_bytes, journal_bytes, syncs, checkpoints;
  uint64_t sync_requests, sync_batches, max_sync_batch;
  uint64_t metadata_pages, directory_page_writes, descriptor_page_writes;
  /* Resident free metadata pages and the last computed packed checkpoint
   * bound. Shared roots borrow any additional pages under allocator authority.
   */
  uint64_t metadata_available_bytes, metadata_workspace_bytes;
  uint64_t metadata_fallbacks;
  uint64_t reserved_payload_bytes, reserved_metadata_bytes, reserved_entries;
  /* Unspent claims plus unpublished payload allocations, covered by lifetime
   * file authority or shared range witnesses. No second allocation charge. */
  uint64_t claimed_payload_bytes;
  /* The volatile credit record plus prepared workspace and retirement claims
   * of a shared participant. Selected pages belong to the root. */
  uint64_t claimed_metadata_bytes;
  /* Local captured payload witnesses on shared roots, excluding unspent and
   * unpublished producer ownership. Counted once per retained incarnation. */
  uint64_t retained_payload_bytes;
  uint64_t budgets;
  /* Component maxima of unclaimed future maintenance, not summed owner
   * credits. Allocator admission also protects their incremental checkpoint
   * workspace. reserve_owners excludes the space-owned base plan. */
  uint64_t reserve_payload_bytes, reserve_metadata_bytes, reserve_entries;
  uint64_t reserve_owners;
  uint64_t packed_pages, packed_bytes;
} ps_stats;

/* Optional syscall seam for deterministic storage faults, not another backend.
 * NULL selects native operations. Every callback has POSIX error/short-I/O
 * semantics. Reserve physically preallocates the backing range. */
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
struct ps_coordination_io;
typedef struct ps_options {
  uint64_t initial_capacity; /* zero selects 10 MiB; creation only */
  uint64_t maximum_capacity; /* zero means no configured maximum */
  int durable_sync;          /* zero is NoSync */
  const ps_io *io;
  /* Optional native OFD query/guard seam, including admission before recovery.
   */
  const struct ps_coordination_io *coordination_io;
  int shared; /* zero selects exclusive lifetime ownership */
} ps_options;

/* create != 0 requires a new pathname; open never creates or truncates. */
int ps_open(const char *, int create, const ps_options *, ps_space **,
            lc_error *);
/* Close requires every writer, reader and capacity budget to be closed. It
 * does not implicitly checkpoint or make NoSync publications durable.
 * Error leaves the handle. */
int ps_close(ps_space *, lc_error *);
/* Reconcile certified journal frames or adopt a newer checkpoint and its
 * certified application tail while retaining captures, producers and budgets.
 * Adoption stabilizes obsolete-root retirement before reusing its space.
 * Shared capture entry points also reconcile before selecting a new handle;
 * existing captures and I/O-free scalar observations do not trigger adoption.
 * Exclusive spaces require no observation I/O. */
int ps_refresh(ps_space *, lc_error *);
/* Pre-close check for live private producers or externally retained budgets.
 * The caller supplies its cache-owned budget count, which close will release.
 * Cached immutable readers are excluded; root caches release those on close. */
int ps_space_private_active(ps_space *, size_t cached_budgets);
/* NULL budget uses ordinary admission. Explicit claims cover physical pages
 * and positive descriptor growth, and remain pinned until writer_close. */
int ps_begin(ps_space *, ps_budget *, ps_key, ps_writer **, lc_error *);
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
/* Bind a canonical key slot's deletion requirement to its publication. */
int ps_writer_key_deletion(ps_writer *, uint64_t bytes, lc_error *);
uint64_t ps_reader_key_deletion(const ps_reader *);
/* Readers pin an immutable incarnation across replacement, delete/reclaim.
 * Durable reads select the completed commit prefix, keeping the prior value
 * visible while replacements/deletions await sync. NoSync reads select the
 * latest complete publication. */
int ps_read_open(ps_space *, ps_key, const ps_ref *, ps_reader **, lc_error *);
int ps_read(ps_reader *, void *, size_t, size_t *, lc_error *);
/* Exact bounded reads on an already captured incarnation. Its sequential
 * cursor is unchanged; payload checksums are validated with bounded scratch.
 * One caller owns a reader. Repeated forward ranges amortize extent lookup. */
int ps_read_at(ps_reader *, uint64_t, void *, size_t, lc_error *);
void ps_read_reference(const ps_reader *, ps_ref *);
/* Scalar observation of selected immutable metadata, with no pin, allocation
 * or payload I/O. Missing is success with present=0. */
int ps_value_info(ps_space *, ps_key, ps_ref *, uint64_t *, int *, lc_error *);
/* Verify a selected immutable incarnation once, outside publication authority.
 * The result is memoized per incarnation; missing/changed/invalid is valid=0.
 * Allocation and transport failures remain errors. Uses bounded scratch. */
int ps_value_validate(ps_space *, ps_key, const ps_ref *, int *, lc_error *);
typedef int (*ps_value_visit_fn)(void *, ps_key, const ps_ref *, uint64_t,
                                 lc_error *);
/* Capture one selected prefix for this owner/kind range. Metadata stays pinned
 * while callbacks run outside publication authority; no bodies are read. */
int ps_values_visit(ps_space *, uint64_t, unsigned int, unsigned int,
                    ps_value_visit_fn, void *, lc_error *);
/* A metadata-only capture of the same prefix. Pins immutable incarnations
 * without allocating one payload buffer per value. Readers opened from it
 * address that captured incarnation even after replacement or removal. */
int ps_values_capture(ps_space *, uint64_t, unsigned int, unsigned int,
                      ps_values **, lc_error *);
size_t ps_values_count(const ps_values *);
int ps_values_info(const ps_values *, size_t, ps_key *, ps_ref *, uint64_t *,
                   lc_error *);
int ps_values_read_open(const ps_values *, size_t, ps_reader **, lc_error *);
/* Visit retained payloads in physical order with a shared packed-page buffer.
 * Identity-only entries in a selective capture are skipped.
 * Each reader is borrowed for its callback. The caller may read it but must
 * leave its ownership with the walk and keep the capture open throughout. */
typedef int (*ps_value_read_fn)(void *, ps_key, ps_reader *, lc_error *);
int ps_values_read_walk(const ps_values *, ps_value_read_fn, void *,
                        lc_error *);
void ps_values_close(ps_values *);
uint64_t ps_read_size(const ps_reader *);
void ps_reader_close(ps_reader *);
/* Private cross-process captured-value handoff. Only immutable ps_reader
 * values are supported (not mutable owned stream heads). Tickets contain
 * extent metadata, never payload bytes. The issuer must keep its reader alive
 * until import succeeds; an imported reader can issue another ticket under
 * the same rule. The caller coordinates issuer lifetime; source loss during
 * an unfinished handoff invalidates that transfer. Tickets alone pin nothing
 * and cannot be persisted for later reopening. One caller owns each reader;
 * exec before using a child.
 * Export performs no payload I/O, allocation or durability barrier.
 * Import opens a fresh read-only OFD, independently pins the captured extents,
 * and returns an ordinary bounded streaming reader. It survives writer close,
 * checkpoint and reopen. Process exit/last close releases its kernel pins.
 * No allocator, recovery or logical publication capability is granted. */
size_t ps_reader_ticket_bytes(const ps_reader *);
int ps_reader_ticket(ps_reader *, void *, size_t, lc_error *);
int ps_reader_import(const char *, const void *, size_t, ps_reader **,
                     lc_error *);
int ps_delete(ps_space *, ps_key, lc_error *);
/* Missing or a changed expected incarnation is a successful no-op. */
int ps_value_remove(ps_space *, ps_key, const ps_ref *, lc_error *);
/* Complete the ready publication prefix, including on NoSync roots. Does not
 * checkpoint or publish private/unready bytes. Already covered prefixes avoid
 * another file sync. */
int ps_flush(ps_space *, lc_error *);
/* Durably checkpoint metadata, retire the previous recoverable root, then
 * reclaim unpinned retired runs. Reuses a completed checkpoint when no journal,
 * metadata, readiness or metadata-trimming work remains, without another root
 * publication. Private/unready producers need not finish. No live payload
 * copying or file shrinking. Growth and explicit repacking retain their own
 * mandatory checkpoint publication. */
int ps_checkpoint(ps_space *, lc_error *);
/* Settle canonical checkpoint work and reclaim unpinned runs. Shared cleanup
 * may leave live-only journal stamps for the next normal checkpoint. */
int ps_checkpoint_cleanup(ps_space *, int allow_log_only, lc_error *);
/* Explicit metadata maintenance: rewrite all current descriptors and their
 * directory references densely through the same durable root-retirement
 * protocol. Payloads, stream mappings and captured readers are unchanged.
 * Does not grow the file. A healthy failure leaves the promoted
 * metadata dirty for a later checkpoint/repack retry; no new root is selected.
 */
int ps_metadata_repack(ps_space *, lc_error *);
/* Explicit growth only, in whole MiB increments. The new range can privately
 * fund its own metadata checkpoint under allocation authority. Capacity and
 * metadata become visible together after root publication; healthy failure
 * retains the old capacity. Uncertain I/O requires reopen/reconciliation. */
int ps_grow(ps_space *, uint64_t, lc_error *);
/* Cached logical owners still check admission after uncertain I/O. Success
 * performs no allocation, lock acquisition, I/O or durability barrier. Existing
 * reader pins and diagnostic status remain usable after admission is poisoned.
 */
int ps_check(ps_space *, lc_error *);
int ps_status(ps_space *, ps_stats *, lc_error *);
int ps_maintenance_pressure_requested(ps_space *);
enum ps_maintenance_pressure_level {
  PS_PRESSURE_NONE = 0,
  PS_PRESSURE_ADVISORY,
  PS_PRESSURE_URGENT
};
/* A busy probe preserves the advisory level. A drain can request a sample
 * even while pressure is clear. At low headroom a pressured sample can
 * commit enough eligible retirements with one physical checkpoint, then
 * report urgent pressure if ordinary headroom is still low. */
int ps_maintenance_pressure(ps_space *, int draining, int *, uint64_t *,
                            lc_error *);

#endif
