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
#define META_MAGIC UINT32_C(0x3141544d)
#define META_HEADER 64U
#define META_PAYLOAD (PS_PAGE - META_HEADER)
#define TOKEN_RANGE UINT64_C(4294967296)
#define TOKEN_POOL (16 * TOKEN_RANGE)
enum ps_event {
  PS_PUT,
  PS_REMOVE,
  PS_RESERVE_IDS,
  PS_SEAL,
  PS_STREAM_TAIL,
  PS_STREAM_OWNER,
  PS_NAMESPACE_OWNER,
  PS_STREAM_EDIT,
  PS_STREAM_COMPLETE
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
typedef struct ps_entry {
  ps_key key;
  uint64_t token, bytes;
  uint64_t publication_sequence;
  uint64_t
      peer_sequence; /* imported visibility uses the certified wire clock */
  uint64_t stream_capacity, stream_runs, stream_ready, stream_wire;
  uint64_t stream_edit_wire, stream_complete_generation, stream_structure_wire;
  uint64_t writer_stamp, writer_generation;
  uint64_t
      stream_storage; /* derived selected mappings, not pinned old values */
  int owned_mapped;
  ps_mapped_range *ranges;
  unsigned char *range_bitmap;
  uint16_t *range_rank;
  uint64_t mapped_end;
  size_t range_count;
  struct ps_entry *owned_first, *owned_last, *owned_prev, *owned_next;
  int owned_linked;
  size_t stream_handles;
  int stream_guarded, stream_sealed, orphan_claimed;
  ps_coordination_guard *stream_claim; /* Local preparation or orphan pass. */
  int durable_visibility; /* pending revision uses completed prefix */
  int value_checked; /* transient immutable-payload validation: 0 unknown */
  int payload_claimed;
  int reader_claimed;
  /* Temporary checkpoint projection binding; never serialized. */
  struct ps_entry *projection;
  ps_extent *extents;
  size_t count, capacity, pins;
  int state, safe, deleted; /* 0 private, 1 live, 2 retired */
  struct ps_entry *previous;
  struct ps_entry *all_next, *hash_next;
  struct ps_entry **all_link;
  struct ps_entry *retired_next, **retired_link;
  struct ps_entry *dirty_next, **dirty_link;
  ps_directory_value checkpoint_ref, pending_ref;
  struct ps_entry *stream_next;
  struct ps_catalog_record *catalog;
  uint64_t catalog_refs, catalog_release, catalog_release_wire;
  size_t catalog_pins;
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
  /* Observed control journal for shared revision comparison, never bodies. */
  unsigned char *journal_image;
  uint64_t pending_sequence, durable_sequence;
  uint64_t sync_requests, sync_batches, max_sync_batch;
  size_t journal, metadata, readers, writers;
  uint64_t live_entries, free_bytes, metadata_free_bytes;
  uint64_t maintenance_activity;
  int maintenance_pressure;
  uint64_t metadata_workspace;
  uint64_t metadata_fallbacks;
  uint64_t reserved_payload, reserved_metadata, reserved_entries;
  uint64_t physical_claim_bytes;
  /* Shared participants witness private checkpoint workspace. Selected pages
   * belong to canonical authority; retirement claims precede root selection. */
  uint64_t metadata_claim_bytes;
  uint64_t reader_claim_bytes;
  int metadata_claims_active;
  uint64_t claimed_free_bytes;
  size_t budgets;
  ps_budget_plan reserve_base;
  struct ps_reserve *reserves;
  uint64_t reserve_next, reserve_count;
  uint64_t directory_page_writes, descriptor_page_writes;
  size_t metadata_pages;
  size_t packed_pages;
  uint64_t packed_bytes;
  ps_entry *entries, *retired, *dirty, *buckets[BUCKETS];
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
};

typedef struct ps_catalog_record {
  unsigned int kind;
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
void ps_catalog_encode_record(unsigned char *, const ps_entry *);
size_t ps_catalog_record_bytes(const ps_entry *);
void ps_catalog_free_record(ps_entry *);
int ps_catalog_prepare_record(ps_space *, ps_entry *, int checkpoint,
                              lc_error *);
void ps_catalog_apply_record(ps_space *, ps_entry *, uint64_t);
void ps_catalog_unlink(ps_space *, ps_entry *);
int ps_catalog_validate_directory(ps_space *, lc_error *);
void ps_catalog_drop_record(ps_space *, ps_entry *, uint64_t, uint64_t);
int ps_catalog_target_retained(ps_space *, const ps_entry *);
ps_entry *ps_internal_lookup(ps_space *, ps_key);
int ps_internal_healthy(ps_space *, lc_error *);
/* Capture selection and its first physical witness under the same canonical
 * authority. Captured handles subsequently retain their incarnation directly.
 */
int ps_internal_capture_begin(ps_space *, ps_coordination_guard **, lc_error *);
int ps_internal_capture_end(ps_space *, ps_coordination_guard *, int,
                            lc_error *);
/* Caller holds a canonical capture and samples one local visibility prefix
 * for all values. Fixed controls have no application body or reader lifetime.
 * A deferred replacement requires the namespace owner to resolve visibility.
 * An exact match to a previously decoded incarnation leaves the buffer alone.
 */
int ps_internal_fixed_value(ps_space *, ps_key, uint64_t prefix, void *, size_t,
                            uint64_t known, uint64_t *incarnation, int *current,
                            lc_error *);
/* Scalar catalog and stream observations share the caller's capture. */
int ps_internal_catalog_find(ps_space *, uint64_t owner, unsigned int kind,
                             const void *, size_t, uint64_t prefix,
                             ps_catalog_info *);
int ps_internal_complete_tail(ps_space *, ps_key, const ps_ref *, uint64_t *);
int ps_internal_stream_claim(ps_space *, uint64_t, ps_coordination_guard **,
                             lc_error *);
/* Caller holds the namespace-name guard while checking and claiming its
 * immutable catalog owner's writer stamp. */
int ps_internal_namespace_stamp(ps_space *, uint64_t, int *, lc_error *);
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
int ps_internal_budget_admit(ps_space *, int, lc_error *);
void ps_internal_reserve_plan(ps_space *, ps_budget_plan *);
void ps_internal_budget_trim(ps_space *, ps_run **);
int ps_internal_budget_check(ps_space *, ps_budget *, uint64_t, uint64_t,
                             uint64_t, lc_error *);
void ps_internal_budget_spend(ps_budget *, uint64_t, uint64_t, uint64_t);
void ps_internal_install(ps_space *, ps_entry *);
void ps_internal_remove(ps_space *, ps_entry *, uint64_t);
int ps_internal_durable(ps_space *, uint64_t, lc_error *);
void ps_internal_collect(ps_space *);
int ps_internal_budget_ranges(ps_space *, uint64_t, ps_run **, lc_error *);
void ps_internal_free_coalesce(ps_space *);
#endif
