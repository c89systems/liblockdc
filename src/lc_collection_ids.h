#ifndef LC_COLLECTION_IDS_H
#define LC_COLLECTION_IDS_H

#include "lc/lc.h"
#include "lc_intcompat.h"

/* Derived append-ID lookup. SHA-256 selects candidates; callers compare the
 * canonical append ID as well, so digest collisions never merge identities. */
typedef struct lc_collection_id_ref {
  unsigned char digest[32];
  lc_u64 position;
  lc_u64 generation;
  lc_u64 segment;
  lc_u64 offset;
} lc_collection_id_ref;

typedef struct lc_collection_ids {
  int fd;
  char *path;
  lc_allocator allocator;
  lc_u64 capacity;
  lc_u64 count;
  lc_u64 generation;
  lc_u64 replay_segment;
  lc_u64 replay_offset;
  lc_u64 snapshot;
} lc_collection_ids;

typedef int (*lc_collection_id_match_fn)(void *context,
                                         const lc_collection_id_ref *candidate,
                                         int *matched, lc_error *error);

int lc_collection_ids_open(const char *path, int create,
                           const lc_allocator *allocator,
                           lc_collection_ids *out, lc_error *error);
void lc_collection_ids_close(lc_collection_ids *ids);
int lc_collection_ids_refresh(lc_collection_ids *ids, lc_error *error);
/* Bounded physical iteration for compaction of typed operation bindings. */
int lc_collection_ids_slot(lc_collection_ids *, lc_u64, lc_collection_id_ref *,
                           int *, lc_error *);
/* Caller holds collection metadata authority. No document bodies are read or
 * retained here; match sees only equal-digest candidates. */
int lc_collection_ids_find(lc_collection_ids *ids,
                           const unsigned char digest[32],
                           lc_collection_id_match_fn match, void *context,
                           lc_collection_id_ref *out, int *found,
                           lc_error *error);
/* Stages the slot and in-memory count/generation under metadata authority.
 * checkpoint must follow before releasing that authority: it publishes the
 * complete header once, together with its canonical replay boundary.
 * Replaying an older reference never overwrites a later deletion binding.
 * Relocation at the same generation may replace the canonical file reference.
 * Growth copies one small entry at a time and publishes a synced replacement.
 */
int lc_collection_ids_put(lc_collection_ids *ids,
                          const lc_collection_id_ref *ref, lc_error *error);
/* Reconciliation establishes exact cardinality from canonical/order facts,
 * rather than trusting a possibly stale persisted count after power loss. */
int lc_collection_ids_checkpoint(lc_collection_ids *ids, lc_u64 count,
                                 lc_u64 generation, lc_u64 replay_segment,
                                 lc_u64 replay_offset, lc_error *error);

#endif
