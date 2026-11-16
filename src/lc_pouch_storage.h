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
  const char *leaf; /* Stable while the namespace capability is retained. */
  uint64_t start, segment, segments;
} lc_pouch_storage_append;
typedef int (*lc_pouch_storage_visit_fn)(void *, uint64_t,
                                         const lc_pouch_state_entry *,
                                         lc_error *);
typedef int (*lc_pouch_storage_replay_fn)(void *, lc_pouch_storage_visit_fn,
                                          void *, lc_error *);
typedef int (*lc_pouch_storage_namespace_visit_fn)(void *, const char *,
                                                   lc_error *);

/* Enumerate exact logical names through their owning storage. Names are
 * borrowed for the callback. No body replay, filtering or ordering guarantee.
 * Callbacks may open namespaces and return an error to stop the walk. Container
 * owners capture a durable catalog prefix; native owners walk their directory.
 * The caller retains the runtime throughout the operation. */
int lc_pouch_storage_namespaces_visit(lc_pouch *,
                                      lc_pouch_storage_namespace_visit_fn,
                                      void *, lc_error *);

/* Immutable logical projection of one captured canonical prefix. Reuses the
 * production supersession, object and query-generation rules without changing
 * the live namespace cache. Replay must visit the selected snapshot followed
 * by its captured newer prefix and stop on callback error. Capture resolves
 * historical stages/decisions, then rejects any remaining live staged records
 * until their logical maintenance policy is integrated.
 * Capture copies metadata only, never bodies. The caller retains any physical
 * source pins and the allocator context; the Pouch runtime is borrowed only
 * during capture. No logical publication or reclamation is performed here. */
typedef struct lc_pouch_state_snapshot lc_pouch_state_snapshot;
typedef int (*lc_pouch_state_snapshot_visit_fn)(void *,
                                                const lc_pouch_state_entry *,
                                                lc_error *);
int lc_pouch_state_snapshot_capture(lc_pouch *, lc_pouch_storage_replay_fn,
                                    void *replay_context,
                                    lc_pouch_state_snapshot **, lc_error *);
/* Visits HIGH_WATER first, then one resolved entry per key in byte order,
 * including tombstones and metadata-only placeholders. Fields are borrowed
 * for each call. A failed visit leaves the immutable capture reusable; a new
 * visit starts at HIGH_WATER. Concurrent visits are permitted. Keep the capture
 * alive throughout every visit; callbacks run without engine coordination. */
int lc_pouch_state_snapshot_visit(const lc_pouch_state_snapshot *,
                                  lc_pouch_state_snapshot_visit_fn, void *,
                                  lc_error *);
void lc_pouch_state_snapshot_close(lc_pouch_state_snapshot *);
/* Exact metadata-snapshot and body-pack sizes without body I/O. NULL copy_leaf
 * retains every body reference. Otherwise rebind that source's bodies into
 * leaf, storing their canonical body records separately from snapshot metadata.
 * This measures bytes, not complete physical maintenance admission. */
int lc_pouch_state_snapshot_measure(const lc_pouch_state_snapshot *,
                                    const char *leaf, const char *copy_leaf,
                                    uint64_t *metadata_bytes,
                                    uint64_t *body_bytes, lc_error *);
/* Complete a typed-owner topology publication under namespace authority and
 * invalidate its resident projection before releasing that authority, even
 * on an uncertain failure. The callback owns physical durability. */
int lc_pouch_state_snapshot_publish(lc_pouch *, const char *ns,
                                    lc_pouch_state_precondition_fn, void *,
                                    lc_error *);

enum lc_pouch_storage_append_mode {
  LC_POUCH_STORAGE_APPEND_PREFIX = 0,
  LC_POUCH_STORAGE_APPEND_COMPLETE,
  LC_POUCH_STORAGE_APPEND_MAINTENANCE
};
enum lc_pouch_storage_commit_mode {
  LC_POUCH_STORAGE_COMMIT_DEFAULT = 0,
  LC_POUCH_STORAGE_COMMIT_DEFER_SYNC
};
#define LC_POUCH_STORAGE_TXN_GUARD_STRIPES 256U
typedef struct lc_pouch_storage_ops {
  int (*check)(void *, lc_error *);
  /* Force the completed publication prefix durable, including on NoSync
   * roots. Private producers are excluded. An already covered prefix needs no
   * new barrier. This is a root capability, never a namespace file handle. */
  int (*flush)(void *, lc_error *);
  int (*namespace_open)(void *, const char *, int, void **, lc_error *);
  void (*namespace_close)(void *, void *);
  int (*namespace_info)(void *, void *, lc_pouch_storage_namespace_info *,
                        lc_error *);
  int (*namespaces_visit)(void *, lc_pouch_storage_namespace_visit_fn, void *,
                          lc_error *);
  int (*replay)(void *, void *, lc_pouch_storage_visit_fn, void *, lc_error *);
  /* Rotation target and known span. PREFIX permits an unknown remainder;
   * COMPLETE requires the declared size, MAINTENANCE additionally permits
   * spending protected work capacity. Reserve all dependencies before staging.
   * Both are indivisible recovery units. The declared span contains canonical
   * bytes only; storage accounts for its envelope in reservation and rotation.
   */
  int (*append_begin)(void *, void *, uint64_t, uint64_t,
                      enum lc_pouch_storage_append_mode,
                      lc_pouch_storage_append *, lc_error *);
  /* DEFER_SYNC belongs to the calling logical cleanup scope. Its completed
   * prefix must be flushed before that scope acknowledges durability. */
  int (*append_commit)(void *, lc_pouch_storage_append *,
                       enum lc_pouch_storage_commit_mode, lc_error *);
  void (*append_close)(void *, lc_pouch_storage_append *);
  /* Encoded, resettable bounded source owns its read pin and storage lifetime.
   * It may outlive the Pouch runtime that opened it. */
  int (*source_open)(void *, void *, const char *, uint64_t, uint64_t,
                     unsigned long stored_crc, lc_source **, lc_error *);
  /* Input belongs to this owner. Independent cursor over the same immutable
   * encoded pin, starting at zero.
   * No payload I/O, decoding, or current-directory lookup. */
  int (*source_clone)(void *, lc_source *, lc_source **, lc_error *);
  int (*retention_hint_ensure)(void *, const char *, lc_error *);
  /* Bounded, materialized retention control metadata. Load returns owned
   * length+1 writable bytes using the logical allocator, or NULL if absent.
   * Store receives the shared LPR1 bytes; NULL/zero clears work to idle while
   * retaining discovery. A hint must already exist. All changes are durable
   * even on NoSync roots. Caller retains namespace mutation authority. */
  int (*retention_work_load)(void *, const char *, char **, size_t *,
                             lc_error *);
  int (*retention_work_store)(void *, const char *, const char *, size_t,
                              lc_error *);
  int (*retention_hint_remove)(void *, const char *, lc_error *);
  /* Advisory, opaque owner cursor; allocated name/cursor use logical allocator.
   * Discovery reads only hints, without opening work records or bodies. */
  int (*retention_hint_next)(void *, const char *, char **, char **,
                             lc_error *);
  /* Volatile transaction authority for stripe < TXN_GUARD_STRIPES. wait=0
   * returns PENDING on contention without waiting. Successful acquisition
   * retains the owner and returns a non-NULL opaque handle; failure leaves
   * NULL. Release on the acquiring thread, after protected work, exactly once.
   * No pathname, publication or durability operation. The common facade owns
   * root mutation ordering. Exclusive owners coordinate threads; future shared
   * owners must additionally coordinate processes inside their root file. */
  int (*txn_guard_lock)(void *, size_t stripe, int wait, void **, lc_error *);
  void (*txn_guard_release)(void *, void *);
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
/* Private admission seam for a physical owner. The constructor resolves the
 * ordinary SDK options before invoking factory once. It owns any returned
 * runtime, including during failed initialization, and retains neither factory
 * nor context. A non-NULL factory requires a Pouch endpoint; NULL preserves the
 * native owner. This does not add a public backend selector. */
typedef int (*lc_pouch_runtime_factory_fn)(void *, const char *,
                                           const lc_allocator *,
                                           const lc_pouch_open_options *,
                                           lc_pouch **, lc_error *);
int lc_client_open_with_pouch_factory(const lc_client_config *,
                                      lc_pouch_runtime_factory_fn, void *,
                                      lc_client **, lc_error *);
#endif
