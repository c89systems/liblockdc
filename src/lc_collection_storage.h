#ifndef LC_COLLECTION_STORAGE_H
#define LC_COLLECTION_STORAGE_H

#include "lc_collection_head.h"
#include "lc_collection_ids.h"
#include "lc_collection_order.h"
#include "lc_pouch_record_io.h"

/* Segment identity admits a canonical input pin. The byte capability and pin
 * stay valid until close; context is borrowed from the owning collection. */
typedef struct lc_collection_read_storage {
  int (*open)(void *, lc_u64, lc_pouch_record_io *, void **, lc_error *);
  void (*close)(void *, void *);
  void *context;
  void (*advise)(void *, void *, int);
} lc_collection_read_storage;
typedef struct lc_collection_file_reads {
  const char *directory;
  int fd;
} lc_collection_file_reads;
void lc_collection_file_reads_init(lc_collection_file_reads *, const char *);
void lc_collection_file_reads_cleanup(lc_collection_file_reads *);
lc_collection_read_storage
lc_collection_file_reads_access(lc_collection_file_reads *);

/* An operation caches one segment's captured readable end, refreshing only
 * when a newer canonical reference requires it. Close is safe on zero state.
 * No body data or file descriptor escapes this domain capability. */
typedef struct lc_collection_input {
  lc_pouch_record_io io;
  lc_collection_read_storage storage;
  void *pin;
  lc_u64 segment, end;
} lc_collection_input;
int lc_collection_input_open(lc_collection_read_storage, lc_u64,
                             lc_collection_input *, lc_error *);
int lc_collection_input_refresh(lc_collection_input *, lc_error *);
void lc_collection_input_advise(lc_collection_input *, int);
void lc_collection_input_close(lc_collection_input *);

/* One canonical segment lifetime, shared by its active append authority and
 * disjoint private upload ranges. Record I/O owns no publication authority.
 * unit_io preserves the native owner's focused unit-finalization hooks. */
struct lc_collection_segment;
typedef struct lc_collection_segment_ops {
  int (*clone)(void *, struct lc_collection_segment *, lc_error *);
  void (*close)(void *);
  int (*reserve)(void *, lc_u64, lc_error *);
  int (*truncate)(void *, lc_u64, lc_error *);
  int (*patch_private)(void *, lc_u64, const void *, size_t, lc_error *);
  int (*sync_private)(void *, lc_error *);
  /* Register the complete canonical tail while append authority is retained.
   * publish waits for its durability after that authority is released; it must
   * not infer an outcome from a later producer's current working tail. */
  int (*ready)(void *, lc_error *);
  int (*publish)(void *, lc_pouch *, lc_error *);
  int (*force)(void *, lc_error *);
} lc_collection_segment_ops;
typedef struct lc_collection_segment {
  const lc_collection_segment_ops *ops;
  void *context;
  lc_pouch_record_io io, unit_io;
} lc_collection_segment;
/* Adopts fd on both success and failure. The native owner retains its alias
 * only for native directory/commit-batcher policy and existing test hooks.
 * Native clones own duplicated descriptors, without a heap allocation; the
 * container adapter retains its cached physical owner. Clone failure leaves
 * an empty output and never transfers the source's ownership. */
int lc_collection_segment_adopt_file(int, lc_collection_segment *, lc_error *);
int lc_collection_segment_clone(const lc_collection_segment *,
                                lc_collection_segment *, lc_error *);
void lc_collection_segment_close(lc_collection_segment *);
int lc_collection_segment_reserve(lc_collection_segment *, lc_u64, lc_error *);
int lc_collection_segment_truncate(lc_collection_segment *, lc_u64, lc_error *);
int lc_collection_segment_patch(lc_collection_segment *, lc_u64, const void *,
                                size_t, lc_error *);
int lc_collection_segment_sync_private(lc_collection_segment *, lc_error *);
int lc_collection_segment_ready(lc_collection_segment *, lc_error *);
int lc_collection_segment_force(lc_collection_segment *, lc_error *);
int lc_collection_segment_publish(lc_collection_segment *, lc_pouch *,
                                  lc_error *);
typedef struct lc_collection_segments {
  int (*open)(void *, lc_u64, int create, lc_collection_segment *, lc_error *);
  void *context;
  /* Zero retains the logical owner's normal reservation target. A physical
   * ceiling bounds private allocation, never the size of an application value.
   */
  lc_u64 private_limit;
} lc_collection_segments;
struct lc_collection_handle;
lc_collection_segments
lc_collection_file_segments(struct lc_collection_handle *);
typedef struct lc_collection_control {
  int (*load)(void *, lc_collection_disk_head *, lc_error *);
  int (*store)(void *, const lc_collection_disk_head *, lc_error *);
  void *context;
} lc_collection_control;
lc_collection_control lc_collection_file_control(struct lc_collection_handle *);
typedef struct lc_collection_projection_build {
  lc_collection_order order;
  lc_collection_ids ids;
  void *private_state;
  int published;
} lc_collection_projection_build;
/* Disposable metadata construction/publication. Canonical replay, validation,
 * cardinality and generation rules remain in the common collection engine. */
typedef struct lc_collection_projections {
  int (*order_refresh)(void *, lc_collection_order *, lc_error *);
  int (*ids_refresh)(void *, lc_collection_ids *, lc_error *);
  int (*begin)(void *, lc_collection_projection_build *, lc_error *);
  int (*publish)(void *, lc_collection_projection_build *, lc_error *);
  void (*cleanup)(void *, lc_collection_projection_build *);
  void *context;
} lc_collection_projections;
lc_collection_projections
lc_collection_file_projections(struct lc_collection_handle *);
/* Structural selection/rotation belongs to the canonical topology owner.
 * The logical engine supplies the byte target; positions and receipts do not
 * cross this boundary. The owner updates the receiver's selected capability
 * only after its canonical head dependencies have been established. */
typedef struct lc_collection_layout {
  int (*active)(void *, lc_error *);
  int (*rotate)(void *, int force, lc_u64 limit, lc_error *);
  void *context;
} lc_collection_layout;
lc_collection_layout lc_collection_file_layout(struct lc_collection_handle *);
/* Bind every native owner component together, including conversion receivers.
 */
void lc_collection_file_bind(struct lc_collection_handle *);

enum lc_collection_authority_kind {
  LC_COLLECTION_AUTH_CATALOG,
  LC_COLLECTION_AUTH_METADATA,
  LC_COLLECTION_AUTH_APPEND,
  LC_COLLECTION_AUTH_COMMIT,
  LC_COLLECTION_AUTH_READERS,
  LC_COLLECTION_AUTH_STREAMS
};
/* Collection coordination follows the established append/commit/metadata
 * order. The snapshot number is meaningful only to captured reader topology.
 * A nonwaiting collision returns PENDING. The caller retains the collection
 * through release; a pin never confers root mutation or payload authority. */
typedef struct lc_collection_authority_ops {
  int (*lock)(void *, enum lc_collection_authority_kind, lc_u64, int, int,
              void **, lc_error *);
  void (*unlock)(void *, enum lc_collection_authority_kind, void *);
} lc_collection_authority_ops;
typedef struct lc_collection_authority {
  const lc_collection_authority_ops *ops;
  void *context;
} lc_collection_authority;
typedef struct lc_collection_lock {
  lc_collection_authority authority;
  enum lc_collection_authority_kind kind;
  void *token;
} lc_collection_lock;
lc_collection_authority
lc_collection_file_authority(struct lc_collection_handle *);
int lc_collection_lock_enter(lc_collection_authority,
                             enum lc_collection_authority_kind, lc_u64,
                             int exclusive, int wait, lc_collection_lock *,
                             lc_error *);
void lc_collection_lock_leave(lc_collection_lock *);

/* Receiver admission/lifetime and exact catalog incarnation. Public collection
 * methods always remain the common logical engine. Owned admission consumes
 * one binding, including on failure; clone admits an independent receiver
 * before any ownership mutation is acknowledged. */
typedef struct lc_collection_owner_ops {
  int (*attach)(void *, struct lc_collection_handle *, lc_error *);
  int (*clone)(void *, lc_collection **, lc_error *);
  int (*bound)(void *, lc_error *);
  int (*remove)(void *, lc_error *);
  int (*finish)(void *, lc_error *);
  void (*release)(void *);
} lc_collection_owner_ops;
typedef struct lc_collection_owner {
  const lc_collection_owner_ops *ops;
  void *context;
} lc_collection_owner;
lc_collection_owner lc_collection_file_owner(struct lc_collection_handle *);

#endif
