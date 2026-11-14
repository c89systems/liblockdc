#ifndef POUCH_SPACE_CATALOG_H
#define POUCH_SPACE_CATALOG_H
#include "space.h"
#define PS_CATALOG 5U
#define PS_CATALOG_NAME_MAX (PS_JOURNAL - 104U)
enum ps_catalog_kind {
  PS_CATALOG_NAMESPACE,
  PS_CATALOG_COLLECTION,
  PS_CATALOG_SEGMENT,
  PS_CATALOG_SNAPSHOT,
  PS_CATALOG_PROJECTION,
  PS_CATALOG_CONTROL
};
typedef struct ps_catalog_view ps_catalog_view;
typedef struct ps_catalog_info {
  uint64_t id;
  ps_ref revision, target;
  ps_key stream;
} ps_catalog_info;
/* IDs share the durably reserved incarnation allocator; unused IDs are lost
 * on reopen, never reused. This does not write a per-ID journal record. */
int ps_catalog_allocate_id(ps_space *, uint64_t *, lc_error *);
/* Names are exact byte strings, including empty names, not filesystem paths.
 * NULL expected means create-only; otherwise replace exactly that revision.
 * Structural publication always waits for durability, even in NoSync stores.
 * A same-name change in flight returns an error; other names stay independent.
 * The target must be a currently live stream incarnation in this container. */
int ps_catalog_put(ps_space *, uint64_t owner, unsigned int kind,
                   const void *name, size_t length, const ps_ref *expected,
                   ps_key stream, const ps_ref *target, ps_catalog_info *,
                   lc_error *);
int ps_catalog_remove(ps_space *, uint64_t owner, unsigned int kind,
                      const void *name, size_t length, const ps_ref *expected,
                      lc_error *);
/* Missing is success with *out==NULL. Views select the completed durable
 * prefix and pin both binding and target; retirement cannot invalidate them.
 * Release a view after admitting the stream/read handles that will use it. */
int ps_catalog_get(ps_space *, uint64_t owner, unsigned int kind,
                   const void *name, size_t length, ps_catalog_view **,
                   lc_error *);
int ps_catalog_view_info(const ps_catalog_view *, ps_catalog_info *,
                         lc_error *);
void ps_catalog_view_close(ps_catalog_view *);
typedef int (*ps_catalog_visit_fn)(void *, const void *, size_t,
                                   const ps_catalog_info *, lc_error *);
/* Captures matching bindings at one durable prefix. Callbacks run outside the
 * allocator mutex; names and targets stay pinned throughout the visit. Memory
 * follows captured metadata, never reserved capacity or document bodies. */
int ps_catalog_visit(ps_space *, uint64_t owner, unsigned int kind,
                     ps_catalog_visit_fn, void *, lc_error *);
typedef int (*ps_catalog_orphan_fn)(void *, ps_key, const ps_ref *, lc_error *);
/* Candidates have no live catalog references, no catalog views, no open owner
 * and no pending catalog release. Callbacks revalidate before retirement:
 * concurrent publication can bind a candidate after capture. No implicit GC. */
int ps_catalog_visit_orphans(ps_space *, ps_catalog_orphan_fn, void *,
                             lc_error *);
#endif
