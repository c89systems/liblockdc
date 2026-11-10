#ifndef LC_POUCH_RECODE_H
#define LC_POUCH_RECODE_H

#include "lc_intcompat.h"
#include "lc_pouch.h"
#include <stdio.h>

/* Offline physical relocation only. Payload bytes are never retained here. */
typedef struct lc_pouch_recode_offset {
  lc_u64 before;
  lc_u64 after;
  lc_u64 segment, stored, units, first_segment, first_offset;
} lc_pouch_recode_offset;

typedef struct lc_pouch_recode_reference {
  lc_u64 offset, bytes, height;
} lc_pouch_recode_reference;

typedef struct lc_pouch_recode_file {
  char *source;
  char *target;
  char *scope;
  char *ns;
  char *leaf;
  char collection_id[LC_XID_STRING_SIZE];
  lc_pouch_recode_offset *offsets;
  size_t count;
  size_t capacity;
  lc_pouch_recode_reference *references;
  size_t reference_count, reference_capacity;
  /* Converted collection outputs are final canonical parts, never a body
   * spool. The source is retired only after their map/receipt is durable. */
  lc_u64 output_floor, output_first, output_parts;
  int active;
  int has_refs;
} lc_pouch_recode_file;

typedef struct lc_pouch_recode {
  lc_pouch *pouch;
  /* Reused only by this conversion operation's canonical CRC reader. */
  unsigned char *state_scratch;
  lc_pouch_recode_file *files;
  size_t count;
  size_t capacity;
  lc_u64 input_bytes;
  lc_u64 output_bytes;
} lc_pouch_recode;

int lc_pouch_recode_build(lc_pouch *pouch, const char *source,
                          const char *target, lc_u64 *input_bytes,
                          lc_u64 *output_bytes, lc_error *error);
int lc_pouch_recode_remove_tree(const char *path, lc_error *error);
int lc_pouch_recode_offset_add(lc_pouch_recode *work,
                               lc_pouch_recode_file *file, lc_u64 before,
                               lc_u64 after, lc_error *error);
int lc_pouch_recode_find(lc_pouch_recode *work, const char *scope,
                         const char *leaf, lc_u64 offset,
                         lc_pouch_recode_file **file, lc_u64 *translated,
                         lc_error *error);
int lc_pouch_recode_lookup(lc_pouch_recode *, const char *, const char *,
                           lc_u64, lc_pouch_recode_file **,
                           lc_pouch_recode_offset **, lc_error *);
char *lc_pouch_recode_location(lc_pouch_recode *, lc_pouch_recode_file *,
                               lc_u64, char[64], lc_error *);
/* One journal writer owns these helpers; all temporary paths are private to
 * the already exclusive conversion operation. */
int lc_pouch_recode_put64(FILE *, lc_u64);
int lc_pouch_recode_get64(FILE *, lc_u64 *);
int lc_pouch_recode_commit(FILE *, const char *, const char *, const char *,
                           int, lc_error *);
FILE *lc_pouch_recode_create(const char *);
char *lc_pouch_recode_progress(lc_pouch_recode *, const char *, size_t,
                               const char *);
int lc_pouch_recode_journal_error(lc_error *);
int lc_pouch_recode_phase(int, lc_error *);
/* These two passes share the canonical state parser and metadata encoder. */
int lc_pouch_state_recode_file(lc_pouch_recode *work,
                               lc_pouch_recode_file *file, lc_error *error);
int lc_pouch_state_recode_refs(lc_pouch_recode *work,
                               lc_pouch_recode_file *file, lc_error *error);

enum {
  LC_POUCH_RECODE_BUILT,
  LC_POUCH_RECODE_READY,
  LC_POUCH_RECODE_OLD_MOVED,
  LC_POUCH_RECODE_INSTALLED,
  LC_POUCH_RECODE_MANIFEST,
  LC_POUCH_RECODE_RECLAIMED,
  LC_POUCH_RECODE_COPIED,
  LC_POUCH_RECODE_SEGMENT_READY,
  LC_POUCH_RECODE_SEGMENT_INSTALLED,
  LC_POUCH_RECODE_REFS_READY,
  LC_POUCH_RECODE_REFS_INSTALLED,
  LC_POUCH_RECODE_HEAD,
  LC_POUCH_RECODE_SEGMENT_RENAMED,
  LC_POUCH_RECODE_REFS_RENAMED,
  LC_POUCH_RECODE_HEAD_WRITTEN,
  LC_POUCH_RECODE_ORDER_REBUILT
};
#ifdef LOCKDC_TEST_BUILD
typedef int (*lc_pouch_recode_hook_fn)(void *context, int phase,
                                       lc_error *error);
extern lc_pouch_recode_hook_fn lc_pouch_recode_test_hook;
extern void *lc_pouch_recode_test_context;
#endif

#endif
