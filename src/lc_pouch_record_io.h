#ifndef LC_POUCH_RECORD_IO_H
#define LC_POUCH_RECORD_IO_H

#include "lc_pouch_record.h"
#include <sys/uio.h>

/* Borrowed canonical-record byte access, not a pathname/filesystem interface.
 * The owner supplies append authority, captured read bounds, publication,
 * durability and lifetime. Methods consume each borrowed part before returning;
 * no body staging is permitted. Patch/rollback touch only private bytes.
 * A failed append/patch can have written a prefix; discard before reuse.
 * Container discard poisons its owner if suffix cleanup is uncertain. The
 * transitional file adapter retains the segmented owner's cleanup policy.
 * A NULL ops table selects hash-only collection validation. */
typedef struct lc_pouch_record_io_ops {
  int (*read)(void *, uint64_t, const struct iovec *, int, lc_error *);
  int (*append)(void *, const struct iovec *, int, lc_error *);
  int (*patch)(void *, uint64_t, const struct iovec *, int, lc_error *);
  int (*tail)(void *, uint64_t *, lc_error *);
  void (*discard)(void *, uint64_t);
} lc_pouch_record_io_ops;
typedef struct lc_pouch_record_io {
  const lc_pouch_record_io_ops *ops;
  void *context;
} lc_pouch_record_io;

/* Transitional native adapter for existing segmented Pouch owners. Borrows fd;
 * append uses its cursor, patch may move it, tail restores it to EOF. Discard
 * retains the existing owner's best-effort truncate semantics. fd==-1 returns
 * hash-only access. No descriptor or callback context allocation is required.
 */
lc_pouch_record_io lc_pouch_record_file_io(int fd);
int lc_pouch_record_io_read(lc_pouch_record_io, uint64_t, const struct iovec *,
                            int, lc_error *);
int lc_pouch_record_io_append(lc_pouch_record_io, const struct iovec *, int,
                              lc_error *);
int lc_pouch_record_io_patch(lc_pouch_record_io, uint64_t, const struct iovec *,
                             int, lc_error *);
int lc_pouch_record_io_tail(lc_pouch_record_io, uint64_t *, lc_error *);
int lc_pouch_record_write_prefix(lc_pouch_record_io, unsigned char,
                                 const void *, size_t, const void *, size_t,
                                 uint64_t, unsigned long, unsigned long,
                                 lc_error *);
/* Rewrites key/metadata first, then clears PENDING by writing the final header
 * last. Does not publish a container tail or synchronize storage. */
int lc_pouch_record_finalize(lc_pouch_record_io, uint64_t, unsigned char,
                             const void *, size_t, const void *, size_t,
                             uint64_t, unsigned long, lc_error *);

#endif
