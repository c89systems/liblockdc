#ifndef LC_POUCH_STATE_RECORD_H
#define LC_POUCH_STATE_RECORD_H

#include "lc_pouch.h"
#include "lc_pouch_crypto.h"
#include "lc_pouch_record_io.h"

typedef struct lc_pouch_state_payload_span {
  char *container_leaf;
  uint64_t record_offset;
  uint64_t payload_offset;
  uint64_t payload_length;
  unsigned long payload_crc;
  int present;
} lc_pouch_state_payload_span;

typedef struct lc_pouch_state_entry {
  char *key;
  char *content_type;
  char *etag;
  lc_pouch_state_payload_span payload_span;
  char *record_container_leaf;
  uint64_t record_offset;
  char *payload_context;
  char *descriptor;
  unsigned char *metadata;
  size_t metadata_length;
  char *decision;
  lc_pouch_generation index_seq;
  /* HIGH_WATER persists public-query freshness independently of state.
   * Snapshots carry per-key query sequences; live records inherit them. */
  lc_pouch_generation query_index_seq;
  int has_query_index_seq;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  lc_pouch_unix_seconds updated_at_unix;
  /* Zero disables expiry, positive values are deadlines, negative values are
   * staged durations pending commit and require a staged key. */
  lc_pouch_unix_seconds retention;
  int has_query_hidden;
  int query_hidden;
  int staged_delete_marker;
  int object_link;
  int seen;
  int found;
  int control;
  int has_record_ref;
  unsigned char record_type;
} lc_pouch_state_entry;

/* Internal canonical bytes, not a namespace mutation API. The caller owns
 * versions, index sequences, append exclusion, publication and durability.
 * Inputs are borrowed for the call. Result entries own their decoded fields
 * and must start zeroed or contain a previously decoded entry. */
void lc_pouch_state_entry_cleanup(const lc_allocator *, lc_pouch_state_entry *);
char *lc_pouch_state_crypto_context(const lc_allocator *, const char *ns,
                                    const char *key, lc_pouch_generation,
                                    unsigned char payload_class);
/* Reads within one captured [start,end) range. On a valid call with no complete
 * record, next remains start and the prior entry remains intact. Corruption
 * and I/O errors are not reported as a repairable incomplete tail. Verification
 * reuses a caller-owned scratch allocation of at least 16384 bytes; a NULL
 * allocation is initialized with this allocator. The caller frees it. scratch
 * itself may be NULL only when verification is disabled. */
int lc_pouch_state_record_read(lc_pouch_record_io, const lc_allocator *,
                               const char *leaf, uint64_t start, uint64_t end,
                               int allow_truncated_tail, int verify_payload,
                               unsigned char **scratch, lc_pouch_state_entry *,
                               uint64_t *next, int *found, int *truncated_tail,
                               lc_error *);
/* Streams an ordinary PUT (type 1 or 7), including plaintext SHA-256 and
 * optional compression/encryption. No whole-body staging. Failure asks the
 * owner to discard its private append, possibly containing earlier records in
 * the same batch. This codec never commits or syncs the owner's stream.
 * The request supplies key, type, version, timestamp, retention, content type,
 * payload context, side metadata and query visibility. Other derived fields
 * are ignored. Only index_seq is encoded in a live record; query sequence
 * inheritance belongs to namespace replay. The crypto provider and record
 * owner must use the same allocator, as in normal Pouch ownership. */
int lc_pouch_state_record_append(const lc_allocator *, lc_pouch_crypto *,
                                 lc_pouch_record_io, const char *leaf,
                                 const lc_pouch_state_entry *request,
                                 lc_source *, int disable_compression,
                                 lc_pouch_state_entry *result, lc_error *);

#endif
