#ifndef LC_COLLECTION_HEAD_H
#define LC_COLLECTION_HEAD_H

#include "lc_pouch.h"

#define LC_COLLECTION_HEAD_BYTES 384U
#define LC_COLLECTION_SNAPSHOT_BIT ((lc_u64)LC_I64_MAX + 1U)

typedef struct lc_collection_disk_head {
  lc_u64 active;
  lc_u64 snapshot;
  lc_u64 snapshot_generation;
  /* Latest immutable snapshot group: snapshot .. snapshot + extra_parts. */
  lc_u64 snapshot_extra_parts;
  lc_u64 tail_segment;
  lc_u64 tail_offset;
  lc_u64 next_snapshot;
  lc_u64 published_at;
  lc_pouch_unix_seconds retention;
  /* Explicit expiry overrides append directives through this generation. */
  lc_u64 retention_generation;
  lc_u64 max_entries;
  lc_collection_lease_info lease;
  int dropped;
} lc_collection_disk_head;

/* Canonical bounded control codec, shared by native and container owners.
 * Decode leaves output untouched on failure. Dropped is data; the owner
 * decides admission and reports collection_stale. No allocation or I/O. */
int lc_collection_head_encode(const lc_collection_disk_head *,
                              unsigned char[LC_COLLECTION_HEAD_BYTES],
                              lc_error *);
int lc_collection_head_decode(const unsigned char[LC_COLLECTION_HEAD_BYTES],
                              lc_collection_disk_head *, lc_error *);
/* Existing native migration validates the released shorter semantic prefix. */
int lc_collection_head_prefix_valid(const unsigned char *, size_t, lc_error *);

#endif
