#ifndef LC_POUCH_COLLECTION_RECORD_H
#define LC_POUCH_COLLECTION_RECORD_H

#include "lc_pouch.h"

/* Collection-only canonical kinds. Bodies become visible only through the
 * immediately following edit receipt. Both use the ordinary payload codec. */
#define LC_POUCH_COLLECTION_EDIT_BODY 112U
#define LC_POUCH_COLLECTION_EDIT 113U

/* Bounded view of Pouch's canonical state record. A collection uses append ID
 * as the record key, permanent position as its logical version, and mutation
 * generation as its index sequence. No collection-specific JSON type exists. */
typedef struct lc_pouch_collection_record {
  uint64_t position;
  uint64_t published_at;
  uint64_t generation;
  uint64_t bytes;
  uint64_t record_offset;
  uint64_t payload_offset;
  uint64_t stored_bytes;
  uint64_t next_offset;
  uint64_t payload_record_offset;
  uint64_t previous_segment;
  uint64_t previous_offset;
  size_t metadata_length;
  lc_pouch_unix_seconds updated_at_unix;
  lc_pouch_unix_seconds retention;
  lc_pouch_unix_seconds collection_retention;
  int has_collection_retention;
  unsigned long stored_crc;
  char append_id[129];
  char etag[65];
  char descriptor[257];
  char context[256];
  char payload_container[64];
  int metadata_only;
  int deleted;
  int published;
  unsigned char kind;
} lc_pouch_collection_record;

typedef struct lc_pouch_collection_writer lc_pouch_collection_writer;
void lc_pouch_collection_writer_kind(lc_pouch_collection_writer *writer,
                                     unsigned char kind);
/* fd == -1 selects hash-only retry verification. Position zero selects an
 * upload context with position assigned at publication. No method owns fd. */
int lc_pouch_collection_writer_open(
    lc_pouch *pouch, int fd, const char *container_leaf,
    const char *collection_id, const char *append_id, uint64_t position,
    uint64_t generation, lc_pouch_unix_seconds retention,
    int has_collection_retention, lc_pouch_unix_seconds collection_retention,
    lc_pouch_collection_writer **out, lc_error *error);
int lc_pouch_collection_writer_write(lc_pouch_collection_writer *writer,
                                     const void *bytes, size_t count,
                                     lc_error *error);
int lc_pouch_collection_writer_finish(lc_pouch_collection_writer *writer,
                                      lc_pouch_collection_record *out,
                                      lc_error *error);
/* Consumes and validates a source with bounded scratch, without publishing. */
int lc_pouch_collection_writer_consume(lc_pouch_collection_writer *writer,
                                       lc_pouch *pouch, lc_source *source,
                                       lc_pouch_collection_record *out,
                                       lc_error *error);
int lc_pouch_collection_writer_publish(lc_pouch_collection_writer *writer,
                                       const char *container_leaf,
                                       uint64_t position, uint64_t generation,
                                       lc_pouch_collection_record *out,
                                       lc_error *error);
void lc_pouch_collection_writer_close(lc_pouch_collection_writer *writer);
/* Stages only encoded payload bytes in a borrowed sink. NULL hashes retries.
 * Copy borrows a finished staging source and canonical fd under append
 * authority; close rolls its unpublished range back before authority leaves. */
int lc_pouch_collection_writer_open_staged(
    lc_pouch *pouch, lc_sink *destination, const char *collection_id,
    const char *append_id, lc_pouch_unix_seconds retention,
    int has_collection_retention, lc_pouch_unix_seconds collection_retention,
    lc_pouch_collection_writer **out, lc_error *error);
int lc_pouch_collection_writer_copy_staged(lc_pouch_collection_writer *writer,
                                           int fd, const char *leaf,
                                           lc_source *source, lc_error *error);

/* Caller owns append authority and a repaired tail. Borrows fd and source.
 * Validation failure rolls back the unpublished byte range. After published is
 * set, any error can have a committed outcome and must be reconciled. The
 * collection owner performs the shared root's sync/projection publication.
 * Retention follows ordinary state writes: a negative duration is resolved
 * after source validation at publication; zero means no expiry; a positive
 * absolute deadline is preserved. */
int lc_pouch_collection_record_append(
    lc_pouch *pouch, int fd, const char *container_leaf,
    const char *collection_id, const char *append_id, uint64_t position,
    uint64_t generation, lc_pouch_unix_seconds retention,
    int has_collection_retention, lc_pouch_unix_seconds collection_retention,
    lc_source *source, lc_pouch_collection_record *out, lc_error *error);
/* Canonical STATE_META referencing the existing immutable payload span.
 * retention follows the same duration/deadline encoding as append, with zero
 * clearing expiry. previous_segment binds
 * the prior metadata record for readers of an earlier mutation generation.
 * No body descriptor is opened or read by this operation. */
int lc_pouch_collection_record_metadata(
    lc_pouch *pouch, int fd, const char *container_leaf,
    const lc_pouch_collection_record *current, uint64_t previous_segment,
    uint64_t generation, lc_pouch_unix_seconds retention,
    lc_pouch_collection_record *out, lc_error *error);
int lc_pouch_collection_record_delete(lc_pouch *pouch, int fd,
                                      const lc_pouch_collection_record *current,
                                      uint64_t generation,
                                      lc_pouch_collection_record *out,
                                      lc_error *error);
/* Reads only bounded key/metadata bytes with pread. file_end comes from the
 * owner's captured segment stat, avoiding an fstat or open per object. Payload
 * integrity is checked by the body reader, not by materializing it here. */
int lc_pouch_collection_record_read(int fd, uint64_t offset, uint64_t file_end,
                                    const char *container_leaf,
                                    lc_pouch_collection_record *out,
                                    lc_error *error);
/* Relocates a validated canonical record into a distinct append-only output
 * file. Payload ciphertext/compressed bytes and retry/deletion facts remain
 * unchanged; storage spans are rebased and metadata history is collapsed.
 * Borrows metadata, payload and output descriptors and a reusable bounded
 * buffer. out must not alias current. The caller syncs/publishes replacement.
 */
int lc_pouch_collection_record_copy(
    lc_pouch *pouch, int source_fd, int payload_fd, const char *source_leaf,
    const lc_pouch_collection_record *current, int destination_fd,
    const char *destination_leaf, unsigned char *buffer, size_t capacity,
    lc_pouch_collection_record *out, lc_error *error);

#endif
