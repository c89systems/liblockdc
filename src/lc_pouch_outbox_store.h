#ifndef LC_POUCH_OUTBOX_STORE_H
#define LC_POUCH_OUTBOX_STORE_H
#include "lc_pouch.h"

/* Bounded outbox work metadata, never application values or transaction
 * authority. Kind -1 is state-reclaim; 0..3 are the existing retention kinds.
 * Names encode the existing marker identity/deadline policy. Implementations
 * provide durable replacement/removal even for NoSync roots. Enumeration
 * observes metadata only; a body is admitted only by read. */
#define LC_POUCH_OUTBOX_MARKER_MISSING (-1002)
#define LC_POUCH_OUTBOX_MARKER_MAX_BYTES 8192U
typedef struct lc_pouch_outbox_store lc_pouch_outbox_store;
typedef struct lc_pouch_outbox_store_ops {
  int (*next)(void *, const char **, lc_error *);
  int (*read)(void *, const char *, const lc_allocator *, size_t, size_t,
              unsigned char **, size_t *, lc_error *);
  int (*write)(void *, const char *, const unsigned char *, size_t, lc_error *);
  int (*remove)(void *, const char *, lc_error *);
  int (*close)(void *, lc_error *);
} lc_pouch_outbox_store_ops;
struct lc_pouch_outbox_store {
  const lc_pouch_outbox_store_ops *ops;
  void *context;
};
int lc_pouch_outbox_store_open(lc_pouch *, const char *, int, int,
                               lc_pouch_outbox_store *, lc_error *);
int lc_pouch_outbox_store_close(lc_pouch_outbox_store *, int, lc_error *);
#endif
