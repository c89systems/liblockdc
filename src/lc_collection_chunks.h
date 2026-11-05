#ifndef LC_COLLECTION_CHUNKS_H
#define LC_COLLECTION_CHUNKS_H

#include "lc_collection_internal.h"
#include "lc_collection_unit.h"
#include "lc_pouch_crypto.h"
#include <sys/types.h>

#ifdef LOCKDC_TEST_BUILD
typedef ssize_t (*lc_collection_chunk_pwrite_fn)(int, const void *, size_t,
                                                 off_t);
extern lc_collection_chunk_pwrite_fn lc_collection_chunks_test_pwrite;
/* Private-range seals precede publication; they are not commit-batcher I/O. */
extern int (*lc_collection_chunks_test_sync_fd)(int);
#endif

#define LC_COLLECTION_CHUNK_HEADER 32U
#define LC_COLLECTION_CHUNK_MAX (16U * 1024U * 1024U)
/* Canonical address space, not RAM: leave room for unit metadata and framing
 * so a target-sized value does not spill just to store its overhead. */
#define LC_COLLECTION_REGION_MAX (LC_COLLECTION_CHUNK_MAX + 65536U)
#define LC_COLLECTION_CHUNK_FANOUT 64U
#define LC_COLLECTION_CHUNK_TUPLE 24U
#define LC_COLLECTION_CHUNK_LEVELS 11U
#define LC_COLLECTION_CHUNK_PAGE                                               \
  (LC_COLLECTION_CHUNK_FANOUT * LC_COLLECTION_CHUNK_TUPLE)

typedef struct lc_collection_chunk_ref {
  lc_u64 segment, offset, bytes;
} lc_collection_chunk_ref;
typedef struct lc_collection_chunk_level {
  unsigned char tuples[LC_COLLECTION_CHUNK_PAGE];
  size_t count;
} lc_collection_chunk_level;
typedef int (*lc_collection_index_append_fn)(void *, unsigned int, const void *,
                                             size_t, lc_collection_chunk_ref *,
                                             lc_error *);

/* Encoded output is written directly into an exclusively owned canonical
 * range. Consecutive codec deliveries share one contiguous extent.
 * Only offsets are accumulated. The fixed carry stack cannot grow with the
 * entry. Full pages become immutable canonical index records as input arrives.
 */
typedef struct lc_collection_chunks {
  lc_collection_chunk_level levels[LC_COLLECTION_CHUNK_LEVELS];
  lc_collection_chunk_ref root;
  lc_collection_chunk_ref pending, first;
  lc_collection_unit first_unit;
  lc_u64 region_end;
  int region_fd;
  /* Pin the first inode through final identity binding, including rollover.
   * Ownership is explicit so a zero-initialized chunks object owns no fd. */
  int first_fd, first_fd_owned;
  /* Cross-inode sync is covered: either explicitly sealed, or finalized
   * while active so any later rotation must seal it. Further writes clear it.
   */
  int first_sealed;
  lc_u64 bytes, extents;
  unsigned int height;
  lc_collection_index_append_fn append_index;
  void *index_context;
} lc_collection_chunks;
int lc_collection_chunks_write_index(int, lc_u64, unsigned int, const void *,
                                     size_t, lc_collection_chunk_ref *,
                                     lc_error *);
/* Links an already copied encoded unit. The fixed carry stack writes only
 * index metadata through append_index when used by snapshot compaction. */
int lc_collection_chunks_link(lc_collection_handle *, lc_collection_chunks *,
                              const lc_collection_chunk_ref *, lc_error *);
/* A prepared private unit needs no append authority for its payload writes.
 * Allocating its range and finalizing its index do require that authority. */
int lc_collection_chunks_append(lc_collection_handle *, lc_collection_chunks *,
                                const void *, size_t, lc_error *);
/* Ends a codec unit/span while retaining exclusive use of its remaining
 * canonical range. Prepare, end_unit, bind and finish require append authority.
 */
lc_u64 lc_collection_chunks_capacity(const lc_pouch *);
int lc_collection_chunks_prepare(lc_collection_handle *, lc_collection_chunks *,
                                 lc_u64, lc_u64 *, lc_error *);
int lc_collection_chunks_end_unit(lc_collection_handle *,
                                  lc_collection_chunks *,
                                  const lc_collection_unit *, lc_error *);
int lc_collection_chunks_bind(lc_collection_handle *, lc_collection_chunks *,
                              lc_pouch_collection_record *, lc_error *);
/* Seals this writer's immutable first-inode data, without append authority.
 * Further private writes to that inode invalidate the completed seal. */
int lc_collection_chunks_seal_first(lc_collection_chunks *, lc_error *);
int lc_collection_chunks_finish(lc_collection_handle *, lc_collection_chunks *,
                                lc_error *);
void lc_collection_chunks_close(lc_collection_chunks *);
int lc_collection_chunks_storage_bytes(lc_u64 bytes, lc_u64 extents,
                                       lc_u64 *total);

typedef struct lc_collection_chunk_frame {
  unsigned char encoded[40U + LC_COLLECTION_CHUNK_PAGE];
  lc_collection_chunk_ref ref;
  size_t count, next;
  unsigned int level;
} lc_collection_chunk_frame;
#define LC_COLLECTION_READ_HANDLES 8U
typedef struct lc_collection_read_handle {
  lc_u64 segment, window, used;
  int fd;
} lc_collection_read_handle;

typedef struct lc_collection_chunk_reader {
  lc_source source;
  lc_pouch_crypto *crypto;
  lc_pouch_crypto_reader *decoder;
  EVP_MD_CTX *manifest;
  lc_source *decoded;
  const lc_pouch_collection_record *record;
  lc_collection_unit unit;
  char binding[256];
  lc_u64 plaintext, encoded;
  int unit_active;
  const char *directory;
  const lc_allocator *allocator;
  lc_collection_chunk_frame *frames;
  lc_collection_chunk_ref root, current;
  lc_u64 length, extent_count, remaining, extents_remaining, extent_remaining;
  lc_u64 offset, index_segment, publication_segment, publication_offset;
  /* Handles preserve independent sequential positions, not payload data. */
  lc_collection_read_handle handles[LC_COLLECTION_READ_HANDLES];
  lc_u64 handle_clock;
  int current_fd, index_fd, directory_fd, publication_fd, initialized, started;
  unsigned int height, depth;
} lc_collection_chunk_reader;
int lc_collection_chunks_open(lc_collection_chunk_reader *,
                              const lc_allocator *, lc_pouch_crypto *,
                              const char *, const char *, int,
                              const lc_pouch_collection_record *, lc_error *);
/* A single contiguous extent uses the ordinary descriptor/span decoder. The
 * reader owns the returned fd; callers borrow it until the next bind/close. */
int lc_collection_chunks_single(lc_collection_chunk_reader *, int *, lc_u64 *,
                                lc_error *);
/* Metadata traversal for encoded relocation; never decodes or stages bodies.
 * The borrowed fd remains owned by the reader. Do not mix with source reads. */
int lc_collection_chunks_next_unit(lc_collection_chunk_reader *, int *, int *,
                                   lc_collection_chunk_ref *,
                                   lc_collection_unit *, lc_error *);
typedef int (*lc_collection_chunk_visit_fn)(void *,
                                            const lc_collection_chunk_ref *,
                                            unsigned int, lc_error *);
int lc_collection_chunks_visit(lc_collection_chunk_reader *,
                               lc_collection_chunk_visit_fn, void *,
                               lc_error *);
void lc_collection_chunks_reader_close(lc_collection_chunk_reader *);
#endif
