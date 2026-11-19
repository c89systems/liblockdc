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

#endif
