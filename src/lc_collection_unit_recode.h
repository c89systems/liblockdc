#ifndef LC_COLLECTION_UNIT_RECODE_H
#define LC_COLLECTION_UNIT_RECODE_H
#include "lc_collection_unit.h"
#include "lc_pouch_crypto.h"

typedef struct lc_collection_unit_recode lc_collection_unit_recode;
/* Conservative private output bytes, including the canonical unit prefix.
 * No allocation, source I/O, publication or implicit capacity growth. */
int lc_collection_unit_recode_bound(lc_pouch_crypto *, lc_u64, lc_u64 *,
                                    lc_error *);
/* One serial conversion owner reuses bounded codec scratch across units.
 * Crypto is borrowed and selects output policy; input descriptors select their
 * own decoding policy. The caller supplies the same encryption key/mode and
 * retains both byte owners until run returns. */
int lc_collection_unit_recode_open(const lc_allocator *, lc_pouch_crypto *,
                                   lc_collection_unit_recode **, lc_error *);
/* Appends a private unit to a distinct output owner, streaming decoded input
 * directly into its encoder. Verify encoded CRC, authentication and decoded
 * length before finalizing metadata and clearing PENDING. Preserve context,
 * ordinal and logical etag. Failure preserves outputs and discards only this
 * private suffix. The caller owns funding, exclusion, durability,
 * index/manifest rebuilding and canonical reference publication; no outcome is
 * published here. A failed owner-level discard has the owner's normal
 * uncertain-I/O semantics.
 */
int lc_collection_unit_recode_run(lc_collection_unit_recode *,
                                  lc_pouch_record_io, lc_u64, lc_u64,
                                  lc_pouch_record_io, lc_u64 *,
                                  lc_collection_unit *, lc_error *);
void lc_collection_unit_recode_close(lc_collection_unit_recode *);
#endif
