#ifndef LC_COLLECTION_PAYLOAD_H
#define LC_COLLECTION_PAYLOAD_H

#include "lc_collection_internal.h"
#include "lc_pouch_crypto.h"
#include <openssl/evp.h>

/* Reusable bounded reader for one canonical collection body. The source view
 * borrows this object; initialize it to zero and keep its address stable until
 * cleanup. No payload buffer, spool, or presentation serialization is used.
 * The canonical record and direct record descriptor remain borrowed until the
 * next open/cleanup. Metadata-only records reuse one owned payload descriptor.
 * Transformed bodies borrow the pinned descriptor and reuse operation-owned,
 * bounded codec scratch across entries. */
typedef struct lc_collection_payload {
  lc_source source;
  lc_source *decoded;
  lc_pouch_crypto_reader *decoder;
  EVP_MD_CTX *hash;
  const lc_pouch_collection_record *record;
  int fd;
  int body_fd;
  int body_fd_initialized;
  lc_u64 body_segment;
  lc_u64 body_end;
  lc_u64 offset;
  lc_u64 bytes;
} lc_collection_payload;

int lc_collection_payload_open(lc_collection_payload *payload,
                               lc_collection_handle *collection, int fd,
                               const lc_pouch_collection_record *record,
                               lc_error *error);
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
