#ifndef LC_COLLECTION_RECODE_H
#define LC_COLLECTION_RECODE_H

#include "lc_pouch_collection_record.h"
#include "lc_pouch_recode.h"

/* A metadata-only reachability pass precedes all collection source retirement.
 * Its durable tables let restart ignore orphan private ranges safely. */
int lc_collection_recode_references(lc_pouch_recode *, const char *,
                                    lc_error *);
int lc_collection_recode_segment(lc_pouch_recode *, size_t, const char *,
                                 lc_error *);

/* Canonical metadata and ordinary payload conversion share the existing state
 * encoder. Independently encoded units and index relocation live separately. */
int lc_pouch_collection_recode_record(lc_pouch_recode *, lc_pouch_recode_file *,
                                      int, lc_u64, lc_u64, int, const char *,
                                      lc_error *);
int lc_pouch_collection_record_check_body(const lc_pouch_collection_record *,
                                          const lc_pouch_collection_record *,
                                          lc_error *);

#endif
