#ifndef LC_COLLECTION_PAYLOAD_H
#define LC_COLLECTION_PAYLOAD_H

#include "lc_collection_chunks.h"
#include "lc_collection_internal.h"
#include "lc_pouch_crypto.h"
#include <openssl/evp.h>

/* Reusable bounded reader for one canonical collection body. The source view
 * borrows this object; initialize it to zero and keep its address stable until
 * cleanup. No payload buffer, spool, or presentation serialization is used.
 * The canonical record and direct record descriptor remain borrowed until the
 * next open/cleanup. Metadata-only records reuse one owned payload descriptor.
 * Transformed bodies borrow the pinned descriptor and reuse operation-owned,
 * bounded codec scratch across entries. Contiguous records and direct units
 * can borrow the caller's first encoded input chunk until consumed. It may
 * alias subsequent output scratch; replay drops consumed input and reads the
 * canonical fd. */
typedef struct lc_collection_payload {
  lc_source source;
  lc_source encoded;
  const unsigned char *prefix;
  size_t prefix_size, prefix_used;
  lc_source *decoded;
  lc_pouch_crypto_read_fn read_decoded;
  lc_collection_chunk_reader chunks;
  lc_pouch_crypto_reader *decoder;
  EVP_MD_CTX *hash;
  const lc_pouch_collection_record *record;
  int fd;
  int body_fd;
  int body_fd_initialized;
  lc_u64 body_segment;
  lc_u64 body_end;
  lc_u64 offset, start;
  lc_u64 bytes;
} lc_collection_payload;

int lc_collection_payload_open(lc_collection_payload *payload,
                               lc_collection_handle *collection, int fd,
                               const lc_pouch_collection_record *record,
                               lc_error *error);
/* unit is NULL for a contiguous record; either form borrows the bounded prefix.
 * A direct unit requires the manifest binding already constructed from this
 * record by the order reader. Payload opening still authenticates its proof.
 */
int lc_collection_payload_open_prefix(lc_collection_payload *,
                                      lc_collection_handle *, int,
                                      const lc_pouch_collection_record *,
                                      const lc_collection_unit *, const char *,
                                      const void *, size_t, lc_error *);
int lc_collection_payload_read(lc_collection_payload *payload, void *bytes,
                               size_t capacity, size_t *got, lc_error *error);
/* Call only after consuming EOF, before accepting an output or an index row.
 * Verifies the complete decoded length and committed plaintext fingerprint. */
int lc_collection_payload_finish(lc_collection_payload *payload,
                                 lc_error *error);
/* Ends this body while keeping hash/codec scratch for the next open. */
void lc_collection_payload_reset(lc_collection_payload *payload);
void lc_collection_payload_cleanup(lc_collection_payload *payload);

#endif
