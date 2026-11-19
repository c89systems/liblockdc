#ifndef LC_POUCH_NAMESPACE_H
#define LC_POUCH_NAMESPACE_H

#include "lc_pouch.h"

#include <stdint.h>

int lc_pouch_namespace_ensure(const lc_allocator *allocator,
                              const char *root_path, const char *ns,
                              lc_error *error);
int lc_pouch_namespace_ensure_layout(const lc_allocator *allocator,
                                     const char *root_path, const char *ns,
                                     lc_error *error);
char *lc_pouch_namespace_path(const lc_allocator *allocator,
                              const char *root_path, const char *ns);

typedef struct lc_pouch_namespace_manifest {
  char *namespace_path;
  char *active_segment;
  char *latest_snapshot;
  char **segment_leaves;
  char **obsolete_segments;
  char **obsolete_snapshots;
  uint64_t *obsolete_segment_marked_at;
  uint64_t *obsolete_snapshot_marked_at;
  uint64_t active_segment_id;
  uint64_t max_segment_id;
  uint64_t latest_snapshot_segment_id;
  lc_pouch_generation state_max_version;
  unsigned long obsolete_segment_count;
  unsigned long obsolete_snapshot_count;
  unsigned long segment_count;
  int repaired;
  /* A resident namespace owner may expose its immutable manifest fields to a
   * caller already serialized by that owner. Borrowed views are read-only and
   * their cleanup only clears this struct; durable manifest mutators reject
   * them until the caller materializes an owned manifest. */
  int borrowed;
} lc_pouch_namespace_manifest;

typedef struct lc_pouch_namespace_marker_snapshot {
  char *fingerprint;
  unsigned long marker_count;
} lc_pouch_namespace_marker_snapshot;

typedef struct lc_pouch_namespace_marker_directory_snapshot {
  uint64_t size;
  long mtime;
} lc_pouch_namespace_marker_directory_snapshot;

typedef struct lc_pouch_namespace_marker_peer_stat {
  char *name;
  uint64_t size;
  long mtime;
} lc_pouch_namespace_marker_peer_stat;

typedef struct lc_pouch_namespace_marker_refresh_state {
  int initialized;
  unsigned long skipped_refreshes;
  lc_pouch_namespace_marker_directory_snapshot directory;
  lc_pouch_namespace_marker_snapshot peers;
  lc_pouch_namespace_marker_peer_stat *peer_stats;
  unsigned long peer_stat_count;
} lc_pouch_namespace_marker_refresh_state;

char *lc_pouch_namespace_segment_leaf(const lc_allocator *allocator,
                                      uint64_t segment_id);
int lc_pouch_namespace_format_segment_leaf(uint64_t segment_id, char *buffer,
                                           size_t capacity);
char *lc_pouch_namespace_snapshot_leaf(const lc_allocator *allocator,
                                       uint64_t segment_id);
/** Validates the sole numeric Pouch segment leaf format and returns its ID. */
int lc_pouch_namespace_parse_segment_leaf(const char *leaf,
                                          uint64_t *segment_id);
int lc_pouch_namespace_manifest_open(const lc_allocator *allocator,
                                     const char *root_path, const char *ns,
                                     lc_pouch_namespace_manifest *out,
                                     unsigned long *cleanup_deleted_count,
                                     unsigned long *cleanup_pending_count,
                                     lc_error *error);
int lc_pouch_namespace_manifest_rotate(const lc_allocator *allocator,
                                       const char *ns,
                                       lc_pouch_namespace_manifest *manifest,
                                       uint64_t segment_id, lc_error *error);
int lc_pouch_namespace_manifest_install_snapshot(
    const lc_allocator *allocator, const char *ns,
    lc_pouch_namespace_manifest *manifest, const char *snapshot_leaf,
    uint64_t snapshot_segment_id, lc_error *error);
int lc_pouch_namespace_manifest_mark_obsolete_segment(
    const lc_allocator *allocator, lc_pouch_namespace_manifest *manifest,
    const char *segment_leaf, uint64_t marked_at_unix, lc_error *error);
int lc_pouch_namespace_manifest_mark_obsolete_snapshot(
    const lc_allocator *allocator, lc_pouch_namespace_manifest *manifest,
    const char *snapshot_leaf, uint64_t marked_at_unix, lc_error *error);
int lc_pouch_namespace_manifest_save(const lc_allocator *allocator,
                                     const char *ns,
                                     lc_pouch_namespace_manifest *manifest,
                                     lc_error *error);
int lc_pouch_namespace_manifest_cleanup_obsolete(
    const lc_allocator *allocator, const char *ns,
    lc_pouch_namespace_manifest *manifest, uint64_t now_unix,
    uint64_t delete_grace_seconds, unsigned long *deleted_count,
    unsigned long *pending_count, lc_error *error);
int lc_pouch_namespace_touch_marker(const lc_allocator *allocator,
                                    const char *namespace_path,
                                    const char *writer_marker_leaf,
                                    uint64_t sequence, lc_error *error);
int lc_pouch_namespace_marker_snapshot_read(
    const lc_allocator *allocator, const char *namespace_path,
    const char *self_marker_leaf, lc_pouch_namespace_marker_snapshot *out,
    lc_error *error);
int lc_pouch_namespace_marker_snapshot_changed(
    const lc_pouch_namespace_marker_snapshot *before,
    const lc_pouch_namespace_marker_snapshot *after);
int lc_pouch_namespace_marker_directory_snapshot_read(
    const lc_allocator *allocator, const char *namespace_path,
    lc_pouch_namespace_marker_directory_snapshot *out, lc_error *error);
int lc_pouch_namespace_marker_directory_snapshot_changed(
    const lc_pouch_namespace_marker_directory_snapshot *before,
    const lc_pouch_namespace_marker_directory_snapshot *after);
int lc_pouch_namespace_marker_refresh_should_scan(
    const lc_allocator *allocator, const char *namespace_path,
    const char *self_marker_leaf,
    lc_pouch_namespace_marker_refresh_state *state,
    unsigned long force_after_skips, int *should_scan, lc_error *error);
void lc_pouch_namespace_marker_snapshot_cleanup(
    const lc_allocator *allocator,
    lc_pouch_namespace_marker_snapshot *snapshot);
void lc_pouch_namespace_marker_refresh_state_cleanup(
    const lc_allocator *allocator,
    lc_pouch_namespace_marker_refresh_state *state);
void lc_pouch_namespace_manifest_cleanup(const lc_allocator *allocator,
                                         lc_pouch_namespace_manifest *manifest);

#endif
