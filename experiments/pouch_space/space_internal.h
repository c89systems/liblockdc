#ifndef POUCH_SPACE_INTERNAL_H
#define POUCH_SPACE_INTERNAL_H
#include "catalog.h"
#include "directory.h"
#include "owned.h"
#include "space.h"
#include <pthread.h>
typedef struct ps_catalog_index {
  struct ps_entry **buckets;
  size_t capacity, count;
} ps_catalog_index;
#define ROOT_MAGIC UINT64_C(0x31544f4f525350)
#define EVENT_MAGIC UINT64_C(0x31565350)
#define BUCKETS 257U
#define JOURNAL_OFFSET PS_JOURNAL_OFFSET
#define META_MAGIC UINT32_C(0x3141544d)
#define META_HEADER 64U
#define META_PAYLOAD (PS_PAGE - META_HEADER)
#define TOKEN_RANGE UINT64_C(4294967296)
enum ps_event { PS_PUT, PS_REMOVE, PS_RESERVE_IDS, PS_SEAL };
#define EXTENT_PACKED 1U
#define EXTENT_OWNED 2U
#define OWNED_MAGIC UINT64_C(0x314e55524f5350)
#define CELLS (PS_PAGE / PS_CELL)

typedef struct ps_extent {
  uint64_t offset, bytes;
  uint32_t crc, flags;
} ps_extent;
typedef struct ps_entry {
  ps_key key;
  uint64_t token, bytes;
  uint64_t publication_sequence;
  uint64_t stream_capacity, stream_runs, stream_ready;
  int stream_open, stream_sealed;
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
  uint64_t catalog_refs, catalog_release;
  size_t catalog_pins;
} ps_entry;
typedef struct ps_run {
  uint64_t offset, bytes;
  int clean;
  struct ps_run *next;
} ps_run;
typedef struct ps_payload_page {
  ps_run run; /* reusable free-interval node; must be first */
  uint64_t occupied;
  struct ps_payload_page *next, **link, *hash_next;
  struct ps_payload_page *available_next, **available_link;
} ps_payload_page;
typedef struct ps_meta_page {
  ps_run run; /* reusable free-interval node; must be first */
  uint64_t generation, following;
  size_t used, references;
  int index;
  struct ps_meta_page *next, **link, *hash_next;
} ps_meta_page;
struct ps_space {
  int fd, poisoned, durable, root;
  pthread_mutex_t mutex;
  pthread_mutex_t sync_io_mutex, commit_mutex;
  pthread_cond_t commit_cond;
  int locks_initialized, syncing;
  ps_io io;
  unsigned char identity[16];
  uint64_t capacity, maximum, generation, sequence, syncs, checkpoints;
  uint64_t token_next, token_limit;
  uint64_t pending_sequence, durable_sequence;
  uint64_t sync_requests, sync_batches, max_sync_batch;
  size_t journal, metadata, readers, writers;
  uint64_t live_entries, free_bytes, metadata_free_bytes;
  uint64_t directory_page_writes, descriptor_page_writes;
  size_t metadata_pages;
  size_t packed_pages;
  uint64_t packed_bytes;
  ps_entry *entries, *retired, *dirty, *buckets[BUCKETS];
  ps_entry *stream_buckets[BUCKETS];
  ps_directory_root directory;
  ps_meta_page *pages, *page_buckets[BUCKETS];
  ps_payload_page *payload_pages, *payload_buckets[BUCKETS];
  ps_payload_page *available[CELLS + 1];
  ps_run *free, *metadata_free;
  ps_catalog_index catalogs;
};

typedef struct ps_catalog_record {
  unsigned int kind;
  size_t length;
  unsigned char *name;
  ps_key stream;
  uint64_t token, hash;
  ps_entry *target, *next;
  ps_entry **link;
} ps_catalog_record;
int ps_catalog_decode_record(ps_entry *, const unsigned char *, size_t,
                             lc_error *);
void ps_catalog_encode_record(unsigned char *, const ps_entry *);
void ps_catalog_free_record(ps_entry *);
int ps_catalog_prepare_record(ps_space *, ps_entry *, int checkpoint,
                              lc_error *);
void ps_catalog_apply_record(ps_space *, ps_entry *, uint64_t);
void ps_catalog_unlink(ps_space *, ps_entry *);
int ps_catalog_validate_directory(ps_space *, lc_error *);
void ps_catalog_drop_record(ps_space *, ps_entry *, uint64_t);
ps_entry *ps_internal_lookup(ps_space *, ps_key);
int ps_internal_healthy(ps_space *, lc_error *);
int ps_internal_event(ps_space *, ps_entry *, int, lc_error *);
int ps_internal_allowance(ps_space *, uint64_t, uint64_t, lc_error *);
void ps_internal_install(ps_space *, ps_entry *);
void ps_internal_remove(ps_space *, ps_entry *, uint64_t);
int ps_internal_durable(ps_space *, uint64_t, lc_error *);
void ps_internal_collect(ps_space *);
#endif
