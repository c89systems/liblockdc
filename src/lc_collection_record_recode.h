#ifndef LC_COLLECTION_RECORD_RECODE_H
#define LC_COLLECTION_RECORD_RECODE_H
#include "lc_collection_chunks.h"

/* Resolved, validated target storage facts. The storage owner supplies these
 * after converting the referenced units/body and before encoding their parent.
 * Logical identity and unit count remain unchanged; index height/placement may
 * change. Values contain bounded metadata only and own no byte capabilities. */
typedef struct lc_collection_recode_stream {
  lc_collection_chunk_ref root;
  lc_collection_unit first;
  lc_u64 units, stored;
  unsigned int height;
} lc_collection_recode_stream;
typedef struct lc_collection_recode_links {
  const lc_pouch_collection_record *body;
  const lc_collection_recode_stream *stream;
  lc_u64 previous_segment, previous_offset;
} lc_collection_recode_links;

/* Offline canonical record conversion through retained byte capabilities.
 * Keep the encryption identity; the target compression policy may differ.
 * Inline bodies stream through the shared integrity-checking codec. STREAM
 * records bind a rebuilt index to its converted first unit; META records adopt
 * their converted body and translated previous-metadata link. Edit body and
 * receipt kinds are preserved. Deleted records retain their canonical facts.
 * No paths, offset tables, durability or reference publication belong here.
 * Failure preserves the caller's result and discards this private suffix.
 * The caller retains all owners, excludes mutation and supplies validated
 * links.
 */
int lc_pouch_collection_record_recode(
    lc_pouch *, lc_pouch_crypto *, const char *collection_id,
    lc_pouch_record_io, const char *source_leaf, lc_u64 offset, lc_u64 limit,
    const lc_collection_recode_links *, lc_pouch_record_io,
    const char *destination_leaf, lc_pouch_collection_record *, lc_error *);

typedef struct lc_collection_stream_recode lc_collection_stream_recode;
/* Output placement/funding belongs to the storage owner. index==0 requests a
 * private codec unit; index==1 requests a bounded canonical index page. The
 * bound includes framing. Return the appropriate unit/metadata I/O and logical
 * segment identity, retaining its byte owner through the call. No global or
 * producer mutex may span this callback or source I/O. */
typedef int (*lc_collection_recode_output_fn)(void *, lc_u64, int index,
                                              lc_pouch_record_io *, lc_u64 *,
                                              lc_error *);
/* Borrow an initialized, unused metadata unit iterator and its canonical
 * record/input owners until close. Retain callback context and crypto likewise.
 * All workspace is bounded; index pages emit through a fixed carry stack. */
int lc_collection_stream_recode_open(const lc_allocator *, lc_pouch_crypto *,
                                     lc_collection_chunk_reader *,
                                     lc_collection_recode_output_fn, void *,
                                     lc_collection_stream_recode **,
                                     lc_error *);
/* Visit at most limit complete units. Successful progress updates visited and
 * finished; the resolved stream result changes only on completion. Completion
 * is idempotent. Failure preserves outputs and terminates this build; rebind a
 * fresh source iterator to retry. Close frees scratch only: the owner reclaims
 * unselected candidate storage after cancellation/failure and publishes the
 * rebuilt parent only after the complete stream is ready and stabilized. */
int lc_collection_stream_recode_step(lc_collection_stream_recode *, size_t,
                                     size_t *, int *,
                                     lc_collection_recode_stream *, lc_error *);
void lc_collection_stream_recode_close(lc_collection_stream_recode *);
#endif
