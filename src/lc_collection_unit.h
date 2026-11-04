#ifndef LC_COLLECTION_UNIT_H
#define LC_COLLECTION_UNIT_H

#include "lc_intcompat.h"
#include "lc_pouch_collection_record.h"
#include <openssl/evp.h>

/* Physical codec units have bounded metadata and bounded plaintext size.
 * Neither limit is a RAM buffer size. Unit payloads flow through codec scratch.
 */
#define LC_COLLECTION_UNIT_META 512U
#define LC_COLLECTION_UNIT_HEADER (32U + LC_COLLECTION_UNIT_META)
#define LC_COLLECTION_UNIT_MAX (8U * 1024U * 1024U)

typedef struct lc_collection_unit {
  lc_u64 plain, stored, ordinal;
  unsigned long crc;
  char descriptor[128];
  char context[256];
  char etag[65];
} lc_collection_unit;

int lc_collection_unit_read(int, lc_u64, lc_u64, lc_collection_unit *,
                            lc_error *);
/* Reads metadata and a caller-owned first input chunk together. Prefix length
 * must not exceed the stored span. Success leaves the fd position unchanged;
 * short reads retry, while truncation and transport failures remain errors. */
int lc_collection_unit_read_prefix(int, lc_u64, lc_u64, lc_collection_unit *,
                                   void *, size_t, lc_error *);
int lc_collection_unit_write_meta(int, lc_u64, const lc_collection_unit *,
                                  lc_error *);
/* This evidence describes logical identity, never physical offsets or mutable
 * retention. Publication binds the immutable unit's cipher core to it.
 * Initialize a reusable hash slot to NULL; its caller owns the resulting
 * SHA-256 scratch and must free it. A NULL slot uses temporary scratch. */
int lc_collection_unit_binding(const lc_pouch_collection_record *, const char *,
                               EVP_MD_CTX **, char[256], lc_error *);
int lc_collection_unit_manifest(const lc_pouch_collection_record *,
                                const char *, EVP_MD_CTX **, char *,
                                unsigned char *, lc_error *);
int lc_collection_unit_check_manifest(const lc_pouch_collection_record *,
                                      lc_error *);
int lc_collection_unit_context(const char *, lc_u64, char[256], lc_error *);
#ifdef LOCKDC_TEST_BUILD
#include <sys/uio.h>
extern ssize_t (*lc_collection_unit_test_preadv)(int, const struct iovec *, int,
                                                 off_t);
#endif
#endif
