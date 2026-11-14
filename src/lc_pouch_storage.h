#ifndef LC_POUCH_STORAGE_H
#define LC_POUCH_STORAGE_H
#include "lc_pouch_state_record.h"

/* Internal typed storage capabilities. The logical Pouch engine retains key
 * policy, generations, projections and codecs. No pathname operations. */
typedef struct lc_pouch_storage_namespace_info {
  uint64_t active_segment, segments, tail;
} lc_pouch_storage_namespace_info;
typedef struct lc_pouch_storage_append {
  void *handle;
  lc_pouch_record_io io;
  const char *leaf;
  uint64_t start, segment;
} lc_pouch_storage_append;
typedef int (*lc_pouch_storage_visit_fn)(void *, uint64_t,
                                         const lc_pouch_state_entry *,
                                         lc_error *);
typedef struct lc_pouch_storage_ops {
  int (*check)(void *, lc_error *);
  int (*namespace_open)(void *, const char *, int, void **, lc_error *);
  void (*namespace_close)(void *, void *);
  int (*namespace_info)(void *, void *, lc_pouch_storage_namespace_info *,
                        lc_error *);
  int (*replay)(void *, void *, lc_pouch_storage_visit_fn, void *, lc_error *);
  int (*append_begin)(void *, void *, uint64_t, lc_pouch_storage_append *,
                      lc_error *);
  int (*append_commit)(void *, lc_pouch_storage_append *, lc_error *);
  void (*append_close)(void *, lc_pouch_storage_append *);
  /* Encoded, resettable bounded source owns its read pin and storage lifetime.
   * It may outlive the Pouch runtime that opened it. */
  int (*source_open)(void *, void *, const char *, uint64_t, uint64_t,
                     lc_source **, lc_error *);
  int (*retention_hint_ensure)(void *, const char *, lc_error *);
  void (*close)(void *);
} lc_pouch_storage_ops;
typedef struct lc_pouch_storage {
  const lc_pouch_storage_ops *ops;
  void *context;
} lc_pouch_storage;

/* Common logical runtime initialization, without opening a physical root.
 * Internal storage owners finish root policy/ownership before exposing it. */
int lc_pouch_runtime_open(const char *, const lc_allocator *,
                          const lc_pouch_open_options *, lc_pouch **,
                          lc_error *);
#endif
