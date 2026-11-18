#ifndef POUCH_SPACE_ROOT_H
#define POUCH_SPACE_ROOT_H

#include "catalog.h"
#include "lc_pouch_crypto.h"

typedef struct ps_root ps_root;
typedef struct ps_namespace ps_namespace;
typedef struct ps_root_options {
  ps_options storage;
  lc_pouch_crypto_open_options crypto;
  /* Codec outputs share the logical Pouch owner's allocation domain. */
  const lc_allocator *crypto_allocator;
  /* Creation uses crypto.compression_enabled. Reopen inherits the stored
   * policy unless explicit; a different explicit policy requires conversion,
   * which is not yet implemented by this owner. No implicit reinterpretation.
   */
  int compression_explicit;
} ps_root_options;
typedef struct ps_namespace_info {
  uint64_t owner;
  ps_ref incarnation;
} ps_namespace_info;

/* One native container owner, with the physical layer's exclusive lifetime
 * lock. Creation requires a new file. Reopen requires committed root metadata;
 * failed initialization is never silently adopted as another crypto policy.
 * Before returning, reopen recovers every catalogued namespace's canonical
 * suffix and reinstates its checkpoint capacity claims. An unused namespace
 * cannot lose completion capacity to a producer in another namespace. Recovery
 * errors reject open, with no writable capability or retained partial owners.
 * Shared-process ownership is a separate pending integration step. */
int ps_root_open(const char *, int create, const ps_root_options *, ps_root **,
                 lc_error *);
/* Stop calls before close. Outstanding namespace pins or physical child
 * handles reject close and leave the root usable. No implicit NoSync barrier.
 */
int ps_root_close(ps_root *, lc_error *);
/* Borrowed internal capabilities, never independently closed. Child owners
 * retain their root/namespace pin while using either capability. */
ps_space *ps_root_space(ps_root *);
lc_pouch_crypto *ps_root_crypto(ps_root *);

/* Nonempty C-string names are exact bytes, without pathname interpretation.
 * One cached owner per name. A same-name initialization waits for that owner;
 * different names can initialize and synchronize independently. No global
 * registry mutex spans storage I/O or application callbacks. */
int ps_root_namespace_acquire(ps_root *, const char *, int create,
                              ps_namespace **, lc_error *);
int ps_namespace_identity(const ps_namespace *, ps_namespace_info *,
                          lc_error *);
void ps_namespace_release(ps_namespace *);
typedef int (*ps_namespace_visit_fn)(const char *, const ps_namespace_info *,
                                     void *, lc_error *);
/* Captures one durable catalog prefix; callbacks may acquire/create other
 * namespaces. A callback's namespace pin remains live until it returns. */
int ps_root_namespace_visit(ps_root *, ps_namespace_visit_fn, void *,
                            lc_error *);
/* Explicit maintenance only: retire closed streams with no catalog reference
 * or view, rechecking each captured incarnation before retirement. This never
 * opens canonical records or reads bodies. Open owners are excluded; candidates
 * claimed concurrently are counted as skipped. Root ownership reserves stream
 * key zero for collection identity: its children/control retire first, and a
 * failed pass retains that identity for retry. Checkpoint enables reuse. */
int ps_root_reclaim_orphans(ps_root *, uint64_t *reclaimed, uint64_t *skipped,
                            lc_error *);

#endif
