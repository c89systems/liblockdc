#ifndef POUCH_SPACE_INTERNAL_H
#define POUCH_SPACE_INTERNAL_H
#include "budget.h"
#include "catalog.h"
#include "coordination.h"
#include "directory.h"
#include "owned.h"
#include "readiness.h"
#include "space.h"
#include <pthread.h>

/* A creating root retains initialization authority until its root metadata has
 * committed. Ordinary physical opens release authority before returning. */
int ps_internal_open(const char *, int create, const ps_options *,
                     int hold_initialization, ps_space **, lc_error *);
typedef struct ps_catalog_index {
  struct ps_entry **buckets;
  size_t capacity, count;
} ps_catalog_index;
#define ROOT_MAGIC UINT64_C(0x31544f4f525350)
#define EVENT_MAGIC UINT64_C(0x31565350)
#define REVISION_MAGIC UINT32_C(0x31525350)
#define BUCKETS 257U
#define JOURNAL_OFFSET PS_JOURNAL_OFFSET
#define JOURNAL_POSITION(cursor) (JOURNAL_OFFSET + ((cursor) % PS_JOURNAL))
#define ROOT_JOURNAL_COVERED 76U
#define ROOT_JOURNAL_START 80U
#define ROOT_CHECKSUM 88U
#define META_MAGIC UINT32_C(0x3141544d)
#define META_HEADER 64U
#define META_PAYLOAD (PS_PAGE - META_HEADER)
#define TOKEN_RANGE UINT64_C(4294967296)
#define TOKEN_POOL (16 * TOKEN_RANGE)
enum ps_event {
  PS_PUT,
  PS_REMOVE,
  PS_SEAL = 3,
  PS_STREAM_TAIL,
  PS_STREAM_OWNER,
  PS_NAMESPACE_OWNER,
  PS_STREAM_EDIT,
  PS_STREAM_COMPLETE,
  PS_STREAM_ADMITTED
};
#define EXTENT_PACKED 1U
#define EXTENT_OWNED 2U
#define OWNED_MAGIC UINT64_C(0x314e55524f5350)
#define CELLS (PS_PAGE / PS_CELL)

typedef struct ps_extent {
  uint64_t offset, bytes;
  uint32_t crc, flags;
} ps_extent;
typedef struct ps_mapped_range {
  uint64_t offset, bytes, data;
} ps_mapped_range;
typedef struct ps_writer_stamp {
  uint64_t token, generation;
} ps_writer_stamp;
/* Stream heads and owned runs need state that ordinary values never use.
 * This state shares an allocation and lifetime with its descriptor. */
typedef struct ps_stream_state {
  uint64_t capacity, runs, ready, wire;
  ps_writer_stamp writer;
  uint64_t edit_wire, complete_generation, structure_wire;
  uint64_t storage; /* derived selected mappings, excluding pinned old ones */
  ps_mapped_range *ranges;
  unsigned char *range_bitmap;
  uint16_t *range_rank;
  uint64_t mapped_end;
  size_t range_count;
  struct ps_entry *owned_first, *owned_last, *owned_prev, *owned_next;
  struct ps_entry *next;
  int mapped, owned_linked;
  size_t handles;
  int guarded, sealed, admitted, orphan_claimed;
  ps_coordination_guard *claim; /* Local preparation or orphan pass. */
  uint64_t catalog_refs, catalog_release, catalog_release_wire;
  size_t catalog_pins;
} ps_stream_state;
typedef struct ps_entry {
  ps_key key;
  uint64_t token, bytes;
  uint64_t publication_sequence;
  uint64_t
      peer_sequence; /* imported visibility uses the certified wire clock */
  uint8_t embedded_extents;
  uint8_t durable_visibility; /* pending revision uses completed prefix */
  int8_t value_checked; /* transient immutable-payload validation: 0 unknown */
  uint8_t payload_claimed, reader_claimed;
  uint8_t state, safe, deleted; /* state: 0 private, 1 live, 2 retired */
  ps_stream_state *stream;
  uint64_t deletion_bytes;
  struct ps_reserve *deletion_reserve;
  /* Temporary checkpoint projection binding; never serialized. */
  struct ps_entry *projection;
  struct ps_entry_allocation *allocation;
  ps_extent *extents;
  size_t count, pins;
  struct ps_entry *previous;
  struct ps_entry *all_next;
  struct ps_entry **all_link;
  struct ps_entry *value_next, **value_link;
  struct ps_entry *retired_next;
  struct ps_entry *dirty_next, **dirty_link;
  ps_descriptor_ref checkpoint_ref;
  struct ps_catalog_record *catalog;
} ps_entry;
typedef struct ps_run {
  uint64_t offset, bytes;
  int clean;
  int claimed; /* 0 common, 1 process budget pool, 2 admission preparation */
  struct ps_run *next;
} ps_run;
typedef struct ps_payload_page {
  ps_run run; /* reusable free-interval node; must be first */
  uint64_t occupied;
  /* Scratch for a retirement forecast under allocation authority. */
  uint64_t reclaim_cells;
  struct ps_payload_page *reclaim_next;
  int external_pins;
  struct ps_payload_page *pinned_next;
  struct ps_payload_page *next, **link, *hash_next;
  struct ps_payload_page *available_next, **available_link;
} ps_payload_page;
typedef struct ps_meta_page {
  ps_run run; /* reusable free-interval node; must be first */
  uint64_t generation, following;
  size_t used, references;
  int index;
  int claimed;
  struct ps_meta_page *claim_next, **claim_link;
  struct ps_meta_page *next, **link, *hash_next;
} ps_meta_page;
typedef struct ps_key_slot {
  ps_entry *entry;
  uint64_t hash;
} ps_key_slot;
typedef struct ps_key_index {
  ps_key_slot *slots;
  size_t count, capacity;
} ps_key_index;
typedef struct ps_value_bucket {
  ps_entry *head;
  size_t count;
} ps_value_bucket;
#define PS_VALUE_GROUPS (2U + PS_NAMESPACE_CHECKPOINT - PS_KEY_METADATA)
struct ps_space {
  /* fd is borrowed from coordination after successful lifetime admission. */
  int fd, poisoned, durable, root;
  int range_pins;
  ps_coordination *coordination;
  /* Protected by the local allocator mutex, or sole-owner open recovery.
   * Nested allocation helpers borrow the outer guard; no producer I/O spans it.
   */
  ps_coordination_guard *allocation_guard;
  int allocation_ranges_checked;
  ps_readiness *readiness;
  /* Local completed-byte prefix certified by this owner's journal framing. */
  uint64_t own_ready_sequence, own_ready_wire;
  uint64_t peer_durable;
  pthread_mutex_t mutex;
  pthread_mutex_t sync_io_mutex, commit_mutex;
  pthread_cond_t commit_cond;
  int locks_initialized, syncing;
  ps_io io;
  unsigned char identity[16];
  uint64_t capacity, maximum, generation, sequence, syncs, checkpoints;
  uint64_t token_next, token_end, token_limit;
  uint64_t writer_id;
  int reused_open_barrier;
  int deferred_recovery, open_grant_covered;
  unsigned char close_proof[2U * PS_READINESS_SLOT_BYTES];
  uint64_t exclusive_raw_writes, exclusive_raw_durable;
  /* Observed control journal for shared revision comparison, never bodies. */
  unsigned char *journal_image;
  /* Monotonic byte position selected by the root. The resident buffer and
   * journal length are linear relative to this cursor. */
  uint64_t journal_start;
  uint64_t pending_sequence, durable_sequence;
  uint64_t sync_requests, sync_batches, max_sync_batch;
  size_t journal, metadata, readers, writers;
  uint64_t live_entries, free_bytes, metadata_free_bytes;
  uint64_t maintenance_activity;
  int maintenance_pressure;
  uint64_t metadata_workspace;
  uint64_t metadata_fallbacks;
  uint64_t reserved_payload, reserved_metadata, reserved_entries;
  /* Volatile budget ledger. A record page is physically owned and registered
   * by its offset in a distinct OFD witness range. Foreign totals belong to
   * one allocator capture. */
  ps_run *credit_record;
  uint64_t *credit_words;
  uint64_t peer_metadata_credits, peer_entry_credits;
  int credit_totals_checked;
  uint64_t physical_claim_bytes;
  /* Shared participants witness private checkpoint workspace. Selected pages
   * belong to canonical authority; retirement claims precede root selection. */
  uint64_t metadata_claim_bytes;
  uint64_t reader_claim_bytes;
  int metadata_claims_active;
  int metadata_materialized;
  uint64_t claimed_free_bytes;
  size_t budgets;
  ps_budget_plan reserve_base;
  struct ps_reserve *reserves;
  uint64_t reserve_next, reserve_count;
  struct ps_reserve *selected_reserves, *retiring_reserves;
  uint64_t selected_reserve_count;
  uint64_t reserve_pruned_local, reserve_pruned_peer;
  uint64_t directory_page_writes, descriptor_page_writes;
  size_t metadata_pages;
  size_t packed_pages;
  uint64_t packed_bytes;
  ps_entry *entries, *retired, *dirty;
  struct ps_entry_allocation *entry_allocations, *entry_available;
  struct ps_entry_allocation *entry_temporary;
  /* A private checkpoint draft may point into the resident's entry slabs.
   * The resident mutex protects those entries through adoption or discard. */
  struct ps_space *projection_resident;
  ps_key_index keys;
  /* Separate checkpoint kinds from other immutable keys and from each
   * other. Include current tombstones that can still select older values. */
  ps_value_bucket value_buckets[PS_VALUE_GROUPS][BUCKETS];
  ps_entry *stream_buckets[BUCKETS];
  ps_directory_root directory;
  ps_meta_page *pages, *page_buckets[BUCKETS];
  ps_meta_page *metadata_retirement_claims;
  ps_payload_page *payload_pages, *payload_buckets[BUCKETS];
  ps_payload_page *pinned_pages;
  ps_payload_page **pinned_page_probe;
  ps_payload_page *available[CELLS + 1];
  ps_run *free, *metadata_free, *pinned_free;
  ps_run **pinned_free_probe;
  uint64_t pinned_free_bytes;
  ps_catalog_index catalogs;
};
struct ps_budget {
  ps_space *space;
  ps_budget_plan remaining;
  size_t users;
  ps_run *trim, *payload_trim;
};
struct ps_reserve {
  ps_space *space;
  uint64_t id;
  unsigned int height;
  ps_budget_plan plan, maximum;
  struct ps_reserve *left, *right;
  uint64_t retire_local, retire_peer;
  struct ps_reserve *retiring_next, **retiring_link;
};

typedef struct ps_catalog_record {
  unsigned int kind;
  ps_writer_stamp writer;
  size_t length, value_length;
  unsigned char *name;
  const unsigned char *value;
  ps_key stream;
  uint64_t token, hash;
  ps_entry *target, *next;
  ps_entry **link;
} ps_catalog_record;
int ps_catalog_decode_record(ps_entry *, const unsigned char *, size_t,
                             lc_error *);
int ps_catalog_decode_namespace(ps_entry *, const void *, size_t, lc_error *);
void ps_catalog_encode_record(unsigned char *, const ps_entry *);
size_t ps_catalog_record_bytes(const ps_entry *);
void ps_catalog_free_record(ps_entry *);
int ps_catalog_copy_record(ps_entry *, const ps_entry *, lc_error *);
int ps_catalog_prepare_record(ps_space *, ps_entry *, int checkpoint,
                              lc_error *);
void ps_catalog_apply_record(ps_space *, ps_entry *, uint64_t);
void ps_catalog_unlink(ps_space *, ps_entry *);
int ps_catalog_validate_directory(ps_space *, lc_error *);
void ps_catalog_drop_record(ps_space *, ps_entry *, uint64_t, uint64_t);
int ps_catalog_target_retained(ps_space *, const ps_entry *);
ps_entry *ps_internal_lookup(ps_space *, ps_key);
/* The selector reads resident state only and may run again on capture retry.
 * Every selected row's identity is captured. The selector chooses which
 * payloads must remain readable for the capture's lifetime. */
typedef int (*ps_value_retain_fn)(void *, ps_key, uint64_t incarnation);
int ps_internal_values_capture(ps_space *, uint64_t owner, unsigned int first,
                               unsigned int last, ps_value_retain_fn, void *,
                               ps_values **, lc_error *);
/* Under the space mutex, capture at the sampled prefix. A provisional
 * capture must be certified by its caller before returning or reading it. */
int ps_internal_values_capture_locked(ps_space *, uint64_t owner,
                                      unsigned int first, unsigned int last,
                                      uint64_t prefix, int provisional,
                                      ps_value_retain_fn, void *, ps_values **,
                                      lc_error *);
int ps_internal_read_witness_valid(ps_space *);
/* Only on an unpublished root while building disposable checkpoint metadata.
 * The callback runs under the local space mutex with a borrowed reader. It
 * must only build private state, which the caller discards if accepted is 0.
 * Borrowed readers cannot escape the walk or re-enter the space. */
int ps_internal_values_startup_walk(ps_space *, uint64_t owner,
                                    ps_value_read_fn, void *, int *accepted,
                                    lc_error *);
/* Borrow checked metadata bytes for the startup walk callback. A null span
 * means the reader needs ordinary I/O. */
int ps_internal_checkpoint_span(ps_reader *, const unsigned char **,
                                lc_error *);
/* Drop an unreturned capture before allowing another local adoption. */
void ps_internal_values_discard_locked(ps_values *);
int ps_internal_healthy(ps_space *, lc_error *);
/* Capture selection and its first physical witness under the same canonical
 * authority. Captured handles subsequently retain their incarnation directly.
 */
int ps_internal_capture_begin(ps_space *, ps_coordination_guard **, lc_error *);
/* Called under the space mutex, with or without allocator authority. */
int ps_internal_token_range(ps_space *, lc_error *);
/* Like capture for memory-only observation. A proven resident prefix can
 * return without allocator authority. Callers must not claim physical bytes. */
int ps_internal_observe_begin(ps_space *, ps_coordination_guard **, lc_error *);
int ps_internal_capture_end(ps_space *, ps_coordination_guard *, int,
                            lc_error *);
/* Select at the caller's sampled prefix. A deferred replacement requires
 * the namespace owner to resolve visibility. An already decoded incarnation
 * leaves the buffer alone. NULL requests a memo-only observation and reports
 * a changed incarnation as noncurrent. Reading a buffer requires a capture.
 */
int ps_internal_fixed_value(ps_space *, ps_key, uint64_t prefix, void *, size_t,
                            uint64_t known, uint64_t *incarnation, int *current,
                            lc_error *);
/* Scalar catalog and stream observations share the caller's capture. */
int ps_internal_catalog_find(ps_space *, uint64_t owner, unsigned int kind,
                             const void *, size_t, uint64_t prefix,
                             ps_catalog_info *);
int ps_internal_complete_tail(ps_space *, ps_key, const ps_ref *, uint64_t *);
int ps_internal_admitted_tail(ps_space *, ps_key, const ps_ref *, uint64_t *);
/* A namespace owner verified this exact active tail under mutation authority.
 * The cohort proof only excludes replay debt. Writers still repair suffixes. */
int ps_internal_certify_tail(ps_space *, ps_key, const ps_ref *, uint64_t,
                             lc_error *);
int ps_internal_stream_claim(ps_space *, uint64_t, ps_coordination_guard **,
                             lc_error *);
/* Caller holds the namespace-name guard while checking and claiming its
 * immutable catalog owner's writer stamp. */
int ps_internal_namespace_stamp(ps_space *, uint64_t, int *, lc_error *);
/* Under namespace edit authority, settle any visibility required by its
 * already framed checkpoint slots before replay can assign slot identities. */
int ps_internal_namespace_checkpoint_stabilize(ps_space *, uint64_t,
                                               lc_error *);
/* Under namespace edit authority, stabilize the route and, for unlink,
 * the checkpoint coverage on which retirement depends. */
int ps_internal_namespace_cut_stabilize(ps_space *, uint64_t owner,
                                        uint64_t route, uint64_t coverage,
                                        lc_error *);
int ps_internal_entry_pin(ps_space *, ps_entry *, lc_error *);
void ps_internal_entry_unpin(ps_space *, ps_entry *);
/* Refresh the peer prefix and free ranges before changing local reservations.
 * Finish with ps_internal_capture_end. No caller retains an allocator choice
 * across this boundary. */
int ps_internal_admission_begin(ps_space *, ps_coordination_guard **,
                                lc_error *);
int ps_internal_event(ps_space *, ps_entry *, int, lc_error *);
int ps_internal_allowance(ps_space *, ps_budget *, uint64_t, uint64_t,
                          lc_error *);
int ps_internal_budget_admit(ps_space *, int, const ps_budget_plan *,
                             const ps_budget_plan *, lc_error *);
int ps_internal_budget_prepare_record(ps_space *, lc_error *);
void ps_internal_budget_publish_record(ps_space *);
void ps_internal_reserve_plan(ps_space *, ps_budget_plan *);
int ps_internal_reserve_entry(ps_entry *, uint64_t deletion_bytes, lc_error *);
int ps_internal_reserve_recheck(ps_space *, uint64_t deletion_bytes,
                                lc_error *);
void ps_internal_reserve_install(ps_space *, ps_entry *, ps_entry *previous);
void ps_internal_reserve_forget(ps_space *, ps_entry *);
void ps_internal_budget_trim(ps_space *, ps_run **);
int ps_internal_budget_check(ps_space *, ps_budget *, uint64_t, uint64_t,
                             uint64_t, lc_error *);
void ps_internal_budget_spend(ps_budget *, uint64_t, uint64_t, uint64_t);
ps_entry *ps_internal_entry_new(unsigned int kind, size_t extents);
void ps_internal_entry_free(ps_entry *);
void ps_internal_entry_auxiliary(ps_entry *);
void ps_internal_install(ps_space *, ps_entry *);
void ps_internal_remove(ps_space *, ps_entry *, uint64_t);
int ps_internal_durable(ps_space *, uint64_t, lc_error *);
int ps_internal_durable_prefix(ps_space *, uint64_t local, uint64_t peer,
                               lc_error *);
void ps_internal_collect(ps_space *);
int ps_internal_budget_ranges(ps_space *, uint64_t, ps_run **, lc_error *);
void ps_internal_free_coalesce(ps_space *);
#endif
