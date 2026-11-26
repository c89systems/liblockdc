#ifndef POUCH_SPACE_OUTBOX_MARKERS_H
#define POUCH_SPACE_OUTBOX_MARKERS_H
#include "lc_pouch_outbox_store.h"
#include "root.h"
int ps_outbox_markers_open(ps_root *, const char *, int, int,
                           lc_pouch_outbox_store *, lc_error *);
int ps_history_markers_open(ps_root *, const char *, int,
                            lc_pouch_outbox_store *, lc_error *);
int ps_history_markers_present(ps_namespace *, int *, lc_error *);
/* Physical marker replacement authority. Keep the namespace pinned for the
 * scope. Only matching kind/name writers wait; no namespace/root lock spans
 * publication I/O. The diagnostic permits deterministic integration checks. */
int ps_outbox_marker_enter(ps_namespace *, int, const char *, void **,
                           lc_error *);
void ps_outbox_marker_leave(ps_namespace *, void *);
size_t ps_outbox_marker_waiter_count(ps_namespace *, int, const char *);
#endif
