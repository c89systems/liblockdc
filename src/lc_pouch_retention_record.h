#ifndef LC_POUCH_RETENTION_RECORD_H
#define LC_POUCH_RETENTION_RECORD_H
#include "lc_pouch_retention.h"

#define LC_POUCH_RETENTION_CONTROL_MAX (3U * 1024U * 1024U + 128U)
#define LC_POUCH_RETENTION_IDLE_TEXT "LPR1\n0\n"

/* Bounded control metadata, intentionally materialized. The LPR1 representation
 * is shared by file and typed owners; it contains no body or transaction
 * outcome. Encode returns owned NUL-terminated text and its length excluding
 * NUL. Decode may split the caller's length+1 writable bytes in place. Origin
 * is an optional diagnostic label. On failure outputs are empty; decoded
 * strings are owned by allocator and use the ordinary retention-work cleanup
 * contract. */
int lc_pouch_retention_record_encode(const lc_allocator *, int phase,
                                     const char *txn_id, const char *key,
                                     lc_u64 fencing_token,
                                     lc_pouch_generation state_version, char **,
                                     size_t *, lc_error *);
int lc_pouch_retention_record_decode(const lc_allocator *, char *, size_t,
                                     const char *origin,
                                     lc_pouch_retention_work *, lc_error *);
/* LPR1 identities and native hint cursors use the same percent encoding.
 * Unlike the filename helper, distinguish allocation failure from bad bytes. */
int lc_pouch_retention_record_unescape(const lc_allocator *, const char *,
                                       char **, const char *origin, lc_error *);
#endif
