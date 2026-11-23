#ifndef LC_COLLECTION_SCRATCH_H
#define LC_COLLECTION_SCRATCH_H

#include "lc/lc.h"
#include "lc_intcompat.h"

/* Process-local seekable metadata. An owner and its captured readers can
 * retain the same backing. Extending the logical length allocates no pages. */
typedef struct lc_collection_scratch lc_collection_scratch;

int lc_collection_scratch_open(const lc_allocator *, lc_collection_scratch **,
                               lc_error *);
void lc_collection_scratch_retain(lc_collection_scratch *);
void lc_collection_scratch_close(lc_collection_scratch *);
int lc_collection_scratch_io(lc_collection_scratch *, lc_u64, unsigned char *,
                             size_t, int writing, lc_error *);
void lc_collection_scratch_extend(lc_collection_scratch *, lc_u64);
void lc_collection_scratch_size(lc_collection_scratch *, lc_u64 *length,
                                lc_u64 *allocated);

#endif
