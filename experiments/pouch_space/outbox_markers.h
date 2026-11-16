#ifndef POUCH_SPACE_OUTBOX_MARKERS_H
#define POUCH_SPACE_OUTBOX_MARKERS_H
#include "lc_pouch_outbox_store.h"
#include "root.h"
/* A named marker binds an immutable empty stream identity to its checksummed
 * PS_CONTROL value. An independent owner tag at the reserved control key
 * binds that identity before body/name publication. The numeric stream key
 * alone never authorizes child cleanup of another canonical stream. */
#define PS_OUTBOX_MARKER_STREAM_KEY UINT64_MAX
#define PS_OUTBOX_MARKER_ROLE_KEY UINT64_MAX
int ps_outbox_markers_open(ps_root *, const char *, int, int,
                           lc_pouch_outbox_store *, lc_error *);
int ps_outbox_marker_reclaim_child(ps_space *, uint64_t, const ps_ref *,
                                   lc_error *);
/* Physical marker replacement authority. Keep the namespace pinned for the
 * scope. Only matching kind/name writers wait; no namespace/root lock spans
 * publication I/O. The diagnostic permits deterministic integration checks. */
int ps_outbox_marker_enter(ps_namespace *, int, const char *, void **,
                           lc_error *);
void ps_outbox_marker_leave(ps_namespace *, void *);
size_t ps_outbox_marker_waiter_count(ps_namespace *, int, const char *);
#endif
