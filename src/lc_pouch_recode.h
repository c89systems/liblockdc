#ifndef LC_POUCH_RECODE_H
#define LC_POUCH_RECODE_H

#include "lc_intcompat.h"
#include "lc_pouch.h"

/* Offline physical relocation only. Payload bytes are never retained here. */
typedef struct lc_pouch_recode_offset {
  lc_u64 before;
  lc_u64 after;
} lc_pouch_recode_offset;

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
  int active;
  int has_refs;
} lc_pouch_recode_file;

typedef struct lc_pouch_recode {
  lc_pouch *pouch;
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
