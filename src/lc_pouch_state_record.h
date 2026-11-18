#ifndef LC_POUCH_STATE_RECORD_H
#define LC_POUCH_STATE_RECORD_H

#include "lc_pouch.h"
#include "lc_pouch_crypto.h"
#include "lc_pouch_record_io.h"

#define LC_POUCH_STATE_SNAPSHOT_CHUNK_BYTES (128U * 1024U)
#define LC_POUCH_STATE_RECORD_META_MAX_BYTES (1024U * 1024U)
#define LC_POUCH_STATE_RECORD_KEY_MAX_BYTES (1024U * 1024U)
#define LC_POUCH_STATE_HIGH_WATER_KEY ".lockd/high-water"

/* Allocation-free future tombstone bound from a structured canonical prefix.
 * Live payload/link/placeholder metadata retains only its etag in a deletion;
 * decisions, high waters and existing tombstones need no future deletion.
 * This measures no body and does not replace full canonical validation. */
int lc_pouch_state_record_delete_bound(unsigned char type, uint64_t key_bytes,
                                       const void *metadata, size_t meta_bytes,
                                       uint64_t *out, lc_error *);

typedef enum lc_pouch_state_record_type {
  LC_POUCH_STATE_RECORD_STATE_PUT = 1,
  LC_POUCH_STATE_RECORD_STATE_DELETE = 2,
  LC_POUCH_STATE_RECORD_STATE_LINK = 3,
  LC_POUCH_STATE_RECORD_STATE_META = 4,
  LC_POUCH_STATE_RECORD_DECISION = 5,
  LC_POUCH_STATE_RECORD_HIGH_WATER = 6,
  LC_POUCH_STATE_RECORD_OBJECT_PUT = 7,
  LC_POUCH_STATE_RECORD_OBJECT_DELETE = 8
} lc_pouch_state_record_type;

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
  /* Namespace index of this key's first body in its current lifetime.
   * Zero belongs to metadata-only placeholders before any body. */
  uint64_t lifetime;
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
/* Same future tombstone size for an already decoded canonical projection. */
uint64_t lc_pouch_state_entry_delete_bytes(const lc_pouch_state_entry *);

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
 * The request supplies key, type, version, lifetime, timestamp, retention,
 * content type, payload context, side metadata and query visibility. Other
 * derived fields are ignored. Live records encode index_seq and lifetime;
 * query sequence inheritance belongs to namespace replay. The crypto provider
 * and record owner must use the same allocator, as in normal Pouch ownership.
 */
int lc_pouch_state_record_append(const lc_allocator *, lc_pouch_crypto *,
                                 lc_pouch_record_io, const char *leaf,
                                 const lc_pouch_state_entry *request,
                                 lc_source *, int disable_compression,
                                 lc_pouch_state_entry *result, lc_error *);

/* Re-encode an immutable body into private canonical output, without changing
 * logical identity. The target provider keeps the input encryption identity;
 * its compression policy may differ. Input and binding are borrowed and must
 * remain valid through the call. No path, spool or whole-body buffer is used.
 * Verify encoded CRC, plaintext byte count and etag before clearing PENDING;
 * preserve versions, lifetime, both index sequences and side metadata. A
 * projected META/LINK body becomes a physical PUT (OBJECT_PUT when applicable).
 * The owner still selects metadata references and supplies durability. */
typedef struct lc_pouch_state_recode_bounds {
  uint64_t record_bytes, metadata_bytes;
} lc_pouch_state_recode_bounds;
int lc_pouch_state_record_recode_bound(const lc_allocator *, lc_pouch_crypto *,
                                       const char *output_leaf,
                                       const lc_pouch_state_entry *,
                                       lc_pouch_state_recode_bounds *,
                                       lc_error *);
int lc_pouch_state_record_recode(const lc_allocator *, lc_pouch_crypto *,
                                 lc_pouch_record_io input,
                                 lc_pouch_record_io output,
                                 const char *output_leaf,
                                 const lc_pouch_state_entry *,
                                 const char *binding_context,
                                 lc_pouch_state_entry *result, lc_error *);

/* Canonical checkpoint bytes, including HIGH_WATER and retained decisions.
 * Control records require NULL encoded input. Preserve resolved versions,
 * lifetimes and both
 * index sequences; the owner supplies selection, history protection, commit and
 * durability. An absent per-key query sequence defaults to its index sequence.
 * NULL encoded writes a body reference, metadata-only placeholder or tombstone
 * without payload I/O. Otherwise copy exactly cipher_bytes from the borrowed
 * source at its current cursor into leaf, retaining crypto descriptors and
 * checking the original stored CRC before completing PENDING. No decoding,
 * re-encryption, spool or whole-body buffer. Scratch is borrowed; chunks are
 * capped at 128 KiB; owners may supply smaller buffers. Scratch may be NULL for
 * an empty encoded body. Failure
 * after output starts discards through the owner's private append boundary.
 * Source, record and their fields remain owned by the caller. */
int lc_pouch_state_record_write_snapshot(const lc_allocator *,
                                         lc_pouch_record_io, const char *leaf,
                                         const lc_pouch_state_entry *,
                                         lc_source *encoded, void *scratch,
                                         size_t scratch_bytes, lc_error *);
/* Exact size of the same canonical snapshot record, without body I/O.
 * copy_body selects inline encoded bytes; otherwise retain the body reference.
 * Leaf offsets do not change the fixed-width metadata representation. */
int lc_pouch_state_record_measure_snapshot(const lc_allocator *,
                                           const char *leaf,
                                           const lc_pouch_state_entry *,
                                           int copy_body, uint64_t *,
                                           lc_error *);
int lc_pouch_state_record_write_high_water(const lc_allocator *,
                                           lc_pouch_record_io,
                                           lc_pouch_generation version,
                                           lc_pouch_generation query_version,
                                           lc_error *);

#endif
