#ifndef POUCH_SPACE_DIRECTORY_H
#define POUCH_SPACE_DIRECTORY_H

#include "space.h"

/* Internal typed checkpoint index. Values identify immutable, packed metadata
 * records; this layer never reads, copies, or caches document bodies. */
typedef struct ps_directory ps_directory;
typedef struct ps_directory_root {
  uint64_t offset, generation;
} ps_directory_root;
typedef struct ps_directory_value {
  uint64_t offset, bytes, incarnation;
} ps_directory_value;
/* read/write transfer exactly one PS_PAGE block; the container handles short
 * I/O and EINTR. allocate reserves a distinct aligned page on success and no
 * page on failure. Pages reachable from any recoverable root or active
 * directory view remain owned.
 * Callbacks report their own errors and run under one checkpoint owner. */
typedef struct ps_directory_io {
  void *context;
  unsigned char identity[16];
  int (*read)(void *, uint64_t, void *, lc_error *);
  int (*allocate)(void *, uint64_t *, lc_error *);
  int (*write)(void *, uint64_t, const void *, lc_error *);
} ps_directory_io;
typedef struct ps_directory_pages {
  const ps_directory_root *allocated, *retired;
  size_t allocated_count, retired_count;
} ps_directory_pages;
typedef int (*ps_directory_visit_fn)(void *, ps_key, ps_directory_value,
                                     lc_error *);
typedef int (*ps_directory_page_fn)(void *, ps_directory_root, lc_error *);

/* root {0,0} is empty. A handle belongs to one checkpoint owner. Reads and
 * mutations cache only index pages actually visited. Any error after open
 * makes the handle unusable except for pages/close. */
int ps_directory_open(const ps_directory_io *, ps_directory_root,
                      ps_directory **, lc_error *);
int ps_directory_get(ps_directory *, ps_key, ps_directory_value *, int *,
                     lc_error *);
int ps_directory_put(ps_directory *, ps_key, ps_directory_value, lc_error *);
int ps_directory_remove(ps_directory *, ps_key, int *, lc_error *);
/* Walks verified entries and/or reachable page ownership. Page visitation
 * requires an unmodified view; recovery uses it to rebuild the free-space map.
 * Callbacks must not re-enter this handle. */
int ps_directory_visit(ps_directory *, ps_directory_visit_fn,
                       ps_directory_page_fn, void *, lc_error *);
/* Writes changed paths only. generation exceeds the starting root generation.
 * Does not sync or publish a root. On success or failure, pages() exposes all
 * allocated pages and old pages requiring retirement. These arrays remain
 * borrowed until close. The caller owns disk allocation/reclamation:
 * - on abort before root publication, release allocated pages;
 * - after publishing/syncing the new root AND durably retiring the old root,
 *   and after old directory views quiesce, release retired pages;
 * - after uncertain root publication, recover before releasing either set.
 * Never release old pages merely because flush succeeded. */
int ps_directory_flush(ps_directory *, uint64_t, ps_directory_root *,
                       lc_error *);
void ps_directory_changed_pages(const ps_directory *, ps_directory_pages *);
/* Releases memory only; never implicitly publishes, syncs, or frees disk pages.
 */
void ps_directory_close(ps_directory *);

#endif
