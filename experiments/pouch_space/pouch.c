#include "pouch.h"
#include "checkpoint.h"
#include "collections.h"
#include "coordination.h"
#include "crc.h"
#include "frame.h"
#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "namespace.h"
#include "namespace_metadata.h"
#include "namespace_work.h"
#include "outbox_markers.h"
#include "pouch_internal.h"
#include "query_index.h"
#include "readiness.h"
#include "root_internal.h"
#include <errno.h>
#include <limits.h>
#include <string.h>
#include <sys/stat.h>
#include <zlib.h>

typedef struct queue_waiter {
  const char *ns, *queue;
  pthread_cond_t changed;
  int signaled;
  struct queue_waiter *next;
} queue_waiter;
typedef struct ps_pouch_owner {
  ps_root *root;
  lc_allocator allocator;
  lc_pouch *runtime; /* Borrowed while logical append/maintenance is active. */
  unsigned int references;
  queue_waiter *waiters;
} ps_pouch_owner;
typedef struct ps_pouch_source {
  lc_source pub;
  ps_pouch_owner *owner;
  ps_namespace_window *window;
  uint64_t start, length, offset;
  unsigned long crc, expected_crc;
  int verify, failed;
} ps_pouch_source;

typedef struct ps_pouch_client_admission {
  const ps_pouch_client_options *physical;
  int create;
} ps_pouch_client_admission;

static int client_runtime_open(void *context, const char *path,
                               const lc_allocator *allocator,
                               const lc_pouch_open_options *logical,
                               lc_pouch **out, lc_error *e) {
  ps_pouch_client_admission *admission = context;
  ps_pouch_state_options options;
  memset(&options, 0, sizeof(options));
  options.logical = *logical;
  if (admission->physical) {
    options.initial_capacity = admission->physical->initial_capacity;
    options.maximum_capacity = admission->physical->maximum_capacity;
    options.io = admission->physical->io;
    options.coordination_io = admission->physical->coordination_io;
  }
  return ps_pouch_state_open(path, admission->create, allocator, &options, out,
                             e);
}
int ps_pouch_client_open(const lc_client_config *config, int create,
                         const ps_pouch_client_options *physical,
                         lc_client **out, lc_error *e) {
  ps_pouch_client_admission admission;
  if (out)
    *out = NULL;
  admission.physical = physical;
  admission.create = create;
  return lc_client_open_with_pouch_factory(config, client_runtime_open,
                                           &admission, out, e);
}

static void release(void *context) {
  ps_pouch_owner *owner = context;
  lc_error ignored;
  if (__atomic_sub_fetch(&owner->references, 1, __ATOMIC_ACQ_REL))
    return;
  lc_error_init(&ignored);
  (void)ps_root_close(owner->root, &ignored);
  lc_error_cleanup(&ignored);
  lc_free_with_allocator(&owner->allocator, owner);
}
static int check(void *context, lc_error *e) {
  ps_pouch_owner *owner = context;
  return ps_check(ps_root_space(owner->root), e);
}
static int flush(void *context, lc_error *e) {
  ps_pouch_owner *owner = context;
  return ps_flush(ps_root_space(owner->root), e);
}
static int checkpoint_prepare(void *context, const char *name,
                              const ps_checkpoint_ref *refs, size_t count,
                              void **plan, lc_error *e) {
  ps_pouch_owner *owner = context;
  return lc_pouch_state_checkpoint_prepare(owner->runtime, name, refs, count,
                                           plan, e);
}
static void checkpoint_finish(void *context, void *plan, int published) {
  ps_pouch_owner *owner = context;
  lc_pouch_state_checkpoint_finish(owner->runtime, plan, published);
}
static int namespace_open(void *context, const char *name, int create,
                          void **out, lc_error *e) {
  ps_pouch_owner *owner = context;
  ps_namespace *ns = NULL;
  ps_checkpoint_observer observer = {checkpoint_prepare, checkpoint_finish,
                                     NULL};
  int rc = ps_root_namespace_acquire(owner->root, name, create, &ns, e);
  observer.context = context;
  if (rc == LC_OK)
    rc = ps_namespace_checkpoint_observe(ns, &observer, e);
  if (rc != LC_OK) {
    ps_namespace_release(ns);
    ns = NULL;
  }
  *out = ns;
  return rc;
}
static void namespace_close(void *context, void *ns) {
  (void)context;
  ps_namespace_release(ns);
}
static int namespace_info(void *context, void *ns,
                          lc_pouch_storage_namespace_info *out, lc_error *e) {
  (void)context;
  return ps_namespace_projection(ns, out, e);
}
static int namespace_state_lock(void *context, const char *name,
                                enum lc_pouch_namespace_lock_policy policy,
                                void **out, lc_error *e) {
  ps_pouch_owner *owner = context;
  ps_coordination_guard *guard = NULL;
  ps_space *space = ps_root_space(owner->root);
  ps_coordination *coordination = ps_space_coordination(space);
  int rc, pressured = ps_maintenance_pressure_requested(space);
  int wait = policy == LC_POUCH_NAMESPACE_FOREGROUND || pressured;
  if (!strcmp(name, ".txns"))
    rc = ps_coordination_lock(coordination, PS_COORDINATION_CONTROL_STATE, 0,
                              wait, &guard, e);
  else if (!strcmp(name, ".xa"))
    rc = ps_coordination_lock(coordination, PS_COORDINATION_CONTROL_STATE, 1,
                              wait, &guard, e);
  else if (policy == LC_POUCH_NAMESPACE_BACKGROUND)
    rc = ps_coordination_namespace_maintenance(
        coordination, ps_namespace_name_hash(name), pressured, &guard, e);
  else
    rc = ps_coordination_namespace_handoff(
        coordination, PS_COORDINATION_NAMESPACE_STATE,
        ps_namespace_name_hash(name), pressured, &guard, e);
  *out = guard;
  return rc;
}
static void namespace_state_unlock(void *context, void *guard) {
  (void)context;
  ps_coordination_unlock(guard);
}
typedef struct namespace_visit_context {
  lc_pouch_storage_namespace_visit_fn visit;
  void *context;
} namespace_visit_context;
static int visit_namespace(const char *name, const ps_namespace_info *identity,
                           void *context, lc_error *e) {
  namespace_visit_context *v = context;
  (void)identity;
  return v->visit(v->context, name, e);
}
static int namespaces_visit(void *context,
                            lc_pouch_storage_namespace_visit_fn visit,
                            void *opaque, lc_error *e) {
  ps_pouch_owner *owner = context;
  namespace_visit_context v;
  int rc = check(context, e);
  v.visit = visit;
  v.context = opaque;
  if (rc == LC_OK)
    rc = ps_root_namespace_visit(owner->root, visit_namespace, &v, e);
  return rc;
}
static int replay(void *context, void *ns,
                  lc_pouch_storage_namespace_info *info,
                  enum lc_pouch_storage_replay_mode mode,
                  lc_pouch_storage_visit_fn visit, void *opaque, lc_error *e) {
  ps_namespace_view *view = NULL;
  int rc;
  (void)context;
  rc = mode == LC_POUCH_STORAGE_REPLAY_STATE
           ? ps_namespace_state_view_open(ns, &view, e)
           : ps_namespace_view_open(ns, &view, e);
  if (rc == LC_OK && info)
    ps_namespace_view_projection(view, info);
  if (rc == LC_OK)
    rc = ps_namespace_view_visit(view, 1, visit, opaque, e);
  ps_namespace_view_close(view);
  return rc;
}
static int replay_suffix(void *context, void *ns,
                         const lc_pouch_storage_namespace_info *info,
                         uint64_t start, lc_pouch_storage_visit_fn visit,
                         void *opaque, lc_error *e) {
  ps_namespace_view *view = NULL;
  int rc;
  (void)context;
  rc = ps_namespace_view_suffix_open(ns, info, start, &view, e);
  if (rc == LC_OK)
    rc = ps_namespace_view_visit(view, 1, visit, opaque, e);
  ps_namespace_view_close(view);
  return rc;
}
static int refresh_state(void *context, void *ns,
                         lc_pouch_storage_namespace_info *info,
                         const lc_pouch_storage_state_refresh *refresh,
                         lc_error *e) {
  (void)context;
  return ps_namespace_refresh_state(ns, info, refresh, e);
}
static int append_begin(void *context, void *ns, uint64_t target,
                        uint64_t minimum,
                        enum lc_pouch_storage_append_mode mode,
                        const lc_pouch_storage_batch *batch,
                        lc_pouch_storage_append *out, lc_error *e) {
  ps_namespace_append *append = NULL;
  enum ps_namespace_append_mode physical;
  int rc, work_pending;
  (void)context;
  memset(out, 0, sizeof(*out));
  switch (mode) {
  case LC_POUCH_STORAGE_APPEND_PREFIX:
    physical = PS_APPEND_PREFIX;
    break;
  case LC_POUCH_STORAGE_APPEND_COMPLETE:
    physical = PS_APPEND_COMPLETE;
    break;
  case LC_POUCH_STORAGE_APPEND_MAINTENANCE:
    physical = PS_APPEND_MAINTENANCE;
    break;
  case LC_POUCH_STORAGE_APPEND_MAINTENANCE_PREFIX:
    physical = PS_APPEND_MAINTENANCE_PREFIX;
    break;
  case LC_POUCH_STORAGE_APPEND_FRAMED_PREFIX:
    physical = PS_APPEND_FRAMED_PREFIX;
    break;
  default:
    return lc_error_set(e, LC_ERR_INVALID, 0, "invalid container append mode",
                        NULL, NULL, "pouch");
  }
  if ((mode == LC_POUCH_STORAGE_APPEND_COMPLETE ||
       mode == LC_POUCH_STORAGE_APPEND_MAINTENANCE ||
       mode == LC_POUCH_STORAGE_APPEND_FRAMED_PREFIX) &&
      target)
    target = target > PS_FRAME_BYTES ? target - PS_FRAME_BYTES : 1;
  rc = ps_namespace_work_stage_ordinary(
      ns,
      mode == LC_POUCH_STORAGE_APPEND_MAINTENANCE ||
          mode == LC_POUCH_STORAGE_APPEND_MAINTENANCE_PREFIX,
      &work_pending, e);
  if (rc != LC_OK)
    return rc;
  rc = ps_namespace_append_begin(ns, target, minimum, physical, batch, &append,
                                 e);
  if (rc == LC_OK && work_pending)
    rc = ps_namespace_work_complete_ordinary(ns, e);
  if (rc != LC_OK) {
    ps_namespace_append_close(append);
    return rc;
  }
  out->handle = append;
  out->io = ps_namespace_append_output(append);
  out->leaf = ps_namespace_append_leaf(append);
  out->segment = ps_namespace_append_segment(append);
  out->segments = ps_namespace_append_segments(append);
  out->incarnation = ps_namespace_append_incarnation(append);
  rc = lc_pouch_record_io_tail(out->io, &out->start, e);
  if (rc != LC_OK) {
    ps_namespace_append_close(append);
    memset(out, 0, sizeof(*out));
  }
  return rc;
}
static int metadata_begin(void *context, void *ns,
                          enum lc_pouch_storage_append_mode mode,
                          const lc_pouch_storage_batch *batch, uint64_t bytes,
                          void **out, lc_error *e) {
  int rc;
  (void)context;
  if (mode != LC_POUCH_STORAGE_APPEND_COMPLETE &&
      mode != LC_POUCH_STORAGE_APPEND_MAINTENANCE)
    return lc_error_set(e, LC_ERR_INVALID, 0,
                        "invalid native metadata admission", NULL, NULL,
                        "pouch");
  rc = ps_namespace_work_ensure(ns, LC_POUCH_STORAGE_WORK_ORDINARY,
                                mode == LC_POUCH_STORAGE_APPEND_MAINTENANCE, e);
  if (rc == LC_OK)
    rc = ps_namespace_metadata_begin(
        ns, mode == LC_POUCH_STORAGE_APPEND_MAINTENANCE, batch, bytes,
        (ps_namespace_metadata **)out, e);
  return rc;
}
static int metadata_commit(void *context, void *handle,
                           const lc_pouch_storage_batch *batch,
                           enum lc_pouch_storage_commit_mode mode,
                           lc_pouch_storage_metadata_result *result,
                           lc_error *e) {
  int rc;
  (void)context;
  if (mode != LC_POUCH_STORAGE_COMMIT_DEFAULT &&
      mode != LC_POUCH_STORAGE_COMMIT_DEFER_SYNC)
    return lc_error_set(e, LC_ERR_INVALID, 0, "invalid native metadata commit",
                        NULL, NULL, "pouch");
  rc = ps_namespace_metadata_commit(handle, batch,
                                    mode == LC_POUCH_STORAGE_COMMIT_DEFAULT
                                        ? PS_COMMIT_DEFAULT
                                        : PS_COMMIT_DEFER_SYNC,
                                    e);
  if (rc == LC_OK && result) {
    result->checkpoint_epoch = ps_namespace_metadata_epoch(handle);
    if (result->retired)
      ps_namespace_metadata_visit_retired(handle, result->retired,
                                          result->context);
  }
  return rc;
}
static void metadata_close(void *context, void *handle) {
  (void)context;
  ps_namespace_metadata_close(handle);
}
static int metadata_reclaim(void *context, uint64_t bytes, lc_error *e) {
  ps_pouch_owner *owner = context;
  ps_space *space = ps_root_space(owner->root);
  ps_budget_plan plan = {0};
  ps_budget *probe = NULL;
  int rc = ps_budget_plan_value_bound(&plan, bytes, e);
  if (rc == LC_OK)
    rc = ps_budget_plan_value(&plan, 128, e);
  if (rc != LC_OK)
    return rc;
  plan.maintenance = 1;
  rc = ps_budget_open(space, &plan, &probe, e);
  if (rc == LC_OK)
    return ps_budget_close(probe, e);
  if (rc != LC_ERR_INVALID)
    return rc;
  lc_error_cleanup(e);
  /* Only a completed physical prefix is selected. Private producers and
   * reader-held old incarnations retain their storage through this checkpoint.
   */
  return ps_checkpoint(space, e);
}
static const lc_pouch_storage_metadata_ops metadata_ops = {
    metadata_begin, metadata_commit, metadata_close, metadata_reclaim,
    8 * PS_MIB};
static int append_commit(void *context, lc_pouch_storage_append *append,
                         enum lc_pouch_storage_commit_mode mode, lc_error *e) {
  enum ps_commit_mode physical;
  (void)context;
  switch (mode) {
  case LC_POUCH_STORAGE_COMMIT_DEFAULT:
    physical = PS_COMMIT_DEFAULT;
    break;
  case LC_POUCH_STORAGE_COMMIT_DEFER_SYNC:
    physical = PS_COMMIT_DEFER_SYNC;
    break;
  default:
    return lc_error_set(e, LC_ERR_INVALID, 0, "invalid container commit mode",
                        NULL, NULL, "pouch");
  }
  return ps_namespace_append_commit(append->handle, physical, e);
}
static void append_close(void *context, lc_pouch_storage_append *append) {
  (void)context;
  ps_namespace_append_close(append->handle);
  memset(append, 0, sizeof(*append));
}
static size_t source_read(lc_source *pub, void *bytes, size_t length,
                          lc_error *e) {
  ps_pouch_source *source = pub->impl;
  struct iovec part;
  size_t at, n;
  int rc;
  if (source->failed) {
    lc_error_set(e, LC_ERR_PROTOCOL, 0, "container body checksum mismatch",
                 NULL, NULL, "pouch");
    return 0;
  }
  if ((uint64_t)length > source->length - source->offset)
    length = (size_t)(source->length - source->offset);
  if (!length) {
    if (source->offset == source->length &&
        source->crc != source->expected_crc) {
      source->failed = 1;
      lc_error_set(e, LC_ERR_PROTOCOL, 0, "container body checksum mismatch",
                   NULL, NULL, "pouch");
    }
    return 0;
  }
  part.iov_base = bytes;
  part.iov_len = length;
  rc = lc_pouch_record_io_read(ps_namespace_read_input(source->window),
                               source->start + source->offset, &part, 1, e);
  if (rc != LC_OK) {
    if (rc == LC_ERR_PROTOCOL)
      source->failed = 1;
    return 0;
  }
  source->offset += length;
  if (source->verify) {
    for (at = 0; at < length; at += n) {
      n = length - at;
      if (n > LC_POUCH_STATE_SNAPSHOT_CHUNK_BYTES)
        n = LC_POUCH_STATE_SNAPSHOT_CHUNK_BYTES;
      source->crc = (unsigned long)ps_crc32(
          (uLong)source->crc, (const unsigned char *)bytes + at, (uInt)n);
    }
    if (source->offset == source->length &&
        source->crc != source->expected_crc) {
      source->failed = 1;
      lc_error_set(e, LC_ERR_PROTOCOL, 0, "container body checksum mismatch",
                   NULL, NULL, "pouch");
      return 0;
    }
  }
  return length;
}
static int source_reset(lc_source *pub, lc_error *e) {
  ps_pouch_source *source = pub->impl;
  (void)e;
  source->offset = 0;
  source->crc = 0;
  source->failed = 0;
  return LC_OK;
}
static void source_close(lc_source *pub) {
  ps_pouch_source *source = pub->impl;
  ps_pouch_owner *owner = source->owner;
  ps_namespace_read_close(source->window);
  lc_free_with_allocator(&owner->allocator, source);
  release(owner);
}
static int source_open(void *context, void *ns, const char *leaf,
                       uint64_t offset, uint64_t length, unsigned long crc,
                       lc_source **out, lc_error *e) {
  ps_pouch_owner *owner = context;
  ps_pouch_source *source;
  uint64_t segment;
  unsigned int kind;
  int rc;
  *out = NULL;
  if (!ps_namespace_parse_source_leaf(leaf, &kind, &segment))
    return lc_error_set(e, LC_ERR_PROTOCOL, 0, "invalid container payload leaf",
                        NULL, NULL, "pouch");
  if (!length && crc)
    return lc_error_set(e, LC_ERR_PROTOCOL, 0,
                        "empty container body checksum mismatch", NULL, NULL,
                        "pouch");
  source = lc_calloc_with_allocator(&owner->allocator, 1, sizeof(*source));
  if (!source)
    return lc_error_set(e, LC_ERR_NOMEM, 0, "cannot allocate container source",
                        NULL, NULL, "pouch");
  rc = ps_namespace_read_open(ns, kind, segment, offset, length,
                              &source->window, e);
  if (rc != LC_OK) {
    lc_free_with_allocator(&owner->allocator, source);
    return rc;
  }
  source->owner = owner;
  source->start = offset;
  source->length = length;
  source->expected_crc = crc;
  source->verify = 1;
  source->pub.read = source_read;
  source->pub.reset = source_reset;
  source->pub.close = source_close;
  source->pub.impl = source;
  __atomic_add_fetch(&owner->references, 1, __ATOMIC_RELAXED);
  *out = &source->pub;
  return LC_OK;
}
static int source_clone(void *context, lc_source *input, lc_source **out,
                        lc_error *e) {
  ps_pouch_owner *owner = context;
  ps_pouch_source *original = input->impl, *source;
  int rc = check(context, e);
  *out = NULL;
  if (rc != LC_OK)
    return rc;
  source = lc_alloc_with_allocator(&owner->allocator, sizeof(*source));
  if (!source)
    return lc_error_set(e, LC_ERR_NOMEM, 0, "cannot clone container source",
                        NULL, NULL, "pouch");
  *source = *original;
  source->offset = 0;
  source->crc = 0;
  source->failed = 0;
  source->pub.impl = source;
  ps_namespace_read_retain(source->window);
  __atomic_add_fetch(&owner->references, 1, __ATOMIC_RELAXED);
  *out = &source->pub;
  return LC_OK;
}
static int retention_hint_ensure(void *context, const char *name, lc_error *e) {
  ps_pouch_owner *owner = context;
  ps_namespace *ns = NULL;
  int rc = ps_root_namespace_acquire(owner->root, name, 1, &ns, e);
  if (rc == LC_OK)
    rc = ps_namespace_retention_hint_ensure(ns, e);
  ps_namespace_release(ns);
  return rc;
}
/* Missing controls must not create namespaces. Keep the catalog pin until
 * admission has resolved this exact binding; do not classify unrelated INVALID
 * errors as absence, including when the caller supplies no error object. */
static int retention_namespace(ps_pouch_owner *owner, const char *name,
                               ps_namespace **out, lc_error *e) {
  ps_catalog_view *view = NULL;
  size_t length;
  int rc;
  *out = NULL;
  if (!name || !*name || (length = strlen(name)) > PS_CATALOG_NAME_MAX)
    return lc_error_set(e, LC_ERR_INVALID, 0,
                        "retention control requires a valid namespace", NULL,
                        NULL, "pouch");
  rc = check(owner, e);
  if (rc == LC_OK)
    rc = ps_catalog_get(ps_root_space(owner->root), 0, PS_CATALOG_NAMESPACE,
                        name, length, &view, e);
  if (rc == LC_OK && view)
    rc = ps_root_namespace_acquire(owner->root, name, 0, out, e);
  ps_catalog_view_close(view);
  return rc;
}
static int retention_work_load(void *context, const char *name,
                               lc_pouch_retention_work *out, lc_error *e) {
  ps_pouch_owner *owner = context;
  ps_namespace *ns = NULL;
  int rc;
  memset(out, 0, sizeof(*out));
  rc = retention_namespace(owner, name, &ns, e);
  if (rc == LC_OK && ns)
    rc = ps_namespace_retention_work_load(ns, &owner->allocator, out, e);
  ps_namespace_release(ns);
  return rc;
}
static int retention_pending(void *context, const char *name, int *present,
                             lc_error *e) {
  ps_pouch_owner *owner = context;
  ps_namespace *ns = NULL;
  int rc = retention_namespace(owner, name, &ns, e);
  *present = 0;
  if (rc == LC_OK && ns)
    rc = ps_namespace_retention_hint_present(ns, present, e);
  ps_namespace_release(ns);
  return rc;
}
static int retention_work_store(void *context, const char *name,
                                const lc_pouch_retention_work *work,
                                lc_error *e) {
  ps_pouch_owner *owner = context;
  ps_namespace *ns = NULL;
  int rc = retention_namespace(owner, name, &ns, e);
  if (rc == LC_OK && !ns)
    rc =
        lc_error_set(e, LC_ERR_TRANSPORT, 0,
                     "state retention work requires an existing discovery hint",
                     NULL, NULL, "pouch");
  if (rc == LC_OK)
    rc = ps_namespace_retention_work_store(ns, work, e);
  ps_namespace_release(ns);
  return rc;
}
static int retention_hint_remove(void *context, const char *name, lc_error *e) {
  ps_pouch_owner *owner = context;
  ps_namespace *ns = NULL;
  int rc = retention_namespace(owner, name, &ns, e);
  if (rc == LC_OK && ns)
    rc = ps_namespace_retention_hint_remove(ns, e);
  ps_namespace_release(ns);
  return rc;
}
typedef struct retention_discovery {
  ps_pouch_owner *owner;
  const char *after;
  char *first, *next;
} retention_discovery;
static int retention_select(const char *name, const ps_namespace_info *info,
                            void *context, lc_error *e) {
  retention_discovery *d = context;
  ps_namespace *ns = NULL;
  char *copy;
  int present = 0;
  int rc = ps_root_namespace_acquire(d->owner->root, name, 0, &ns, e);
  (void)info;
  if (rc == LC_OK)
    rc = ps_namespace_retention_hint_present(ns, &present, e);
  ps_namespace_release(ns);
  if (rc != LC_OK || !present)
    return rc;
  if (!d->first || strcmp(name, d->first) < 0) {
    copy = lc_strdup_with_allocator(&d->owner->allocator, name);
    if (!copy)
      goto no_memory;
    lc_free_with_allocator(&d->owner->allocator, d->first);
    d->first = copy;
  }
  if (d->after && strcmp(name, d->after) > 0 &&
      (!d->next || strcmp(name, d->next) < 0)) {
    copy = lc_strdup_with_allocator(&d->owner->allocator, name);
    if (!copy)
      goto no_memory;
    lc_free_with_allocator(&d->owner->allocator, d->next);
    d->next = copy;
  }
  return LC_OK;
no_memory:
  return lc_error_set(e, LC_ERR_NOMEM, 0,
                      "cannot select state retention namespace", NULL, NULL,
                      "pouch");
}
static int retention_hint_next(void *context, const char *after, char **name,
                               char **cursor, lc_error *e) {
  ps_pouch_owner *owner = context;
  retention_discovery d;
  char *selected;
  int rc;
  memset(&d, 0, sizeof(d));
  d.owner = owner;
  d.after = after;
  *name = *cursor = NULL;
  rc = ps_root_namespace_visit(owner->root, retention_select, &d, e);
  selected = d.next ? d.next : d.first;
  if (rc == LC_OK && selected) {
    *name = lc_strdup_with_allocator(&owner->allocator, selected);
    if (!*name)
      rc = lc_error_set(e, LC_ERR_NOMEM, 0,
                        "cannot copy state retention namespace", NULL, NULL,
                        "pouch");
    else {
      *cursor = selected;
      if (d.next)
        d.next = NULL;
      else
        d.first = NULL;
    }
  }
  lc_free_with_allocator(&owner->allocator, d.first);
  lc_free_with_allocator(&owner->allocator, d.next);
  return rc;
}
static uint64_t txn_guard_slot(const char *txn_id) {
  return ps_namespace_name_hash(txn_id) % PS_TRANSACTION_GUARD_SLOTS;
}
static int txn_guard_lock(void *context, const char *txn_id, int wait,
                          void **out, lc_error *e) {
  ps_pouch_owner *owner = context;
  ps_coordination_guard *guard = NULL;
  int rc;
  if (out)
    *out = NULL;
  if (!txn_id || !*txn_id || !out)
    return lc_error_set(e, LC_ERR_INVALID, 0,
                        "container transaction authority needs XID and output",
                        NULL, NULL, "pouch");
  rc = check(context, e);
  if (rc != LC_OK)
    return rc;
  rc = ps_coordination_lock(ps_space_coordination(ps_root_space(owner->root)),
                            PS_COORDINATION_TRANSACTION, txn_guard_slot(txn_id),
                            wait, &guard, e);
  if (rc != LC_OK)
    return rc;
  /* Storage can become uncertain while a caller waits for this XID. */
  rc = check(context, e);
  if (rc != LC_OK) {
    ps_coordination_unlock(guard);
    return rc;
  }
  __atomic_add_fetch(&owner->references, 1, __ATOMIC_RELAXED);
  *out = guard;
  return LC_OK;
}
static int txn_guard_covers(void *handle, const char *txn_id) {
  return ps_coordination_transaction_covers(handle, txn_guard_slot(txn_id));
}
static void txn_guard_release(void *context, void *handle) {
  ps_coordination_unlock(handle);
  release(context);
}
static int collection_open(void *context, lc_client *client, const char *ns,
                           const lc_collection_config *config, int create,
                           lc_collection **out, lc_error *e) {
  ps_pouch_owner *owner = context;
  return ps_collection_client_open(owner->root, client, ns, config, create, out,
                                   e);
}
static int collections_visit(void *context, lc_client *client, const char *ns,
                             lc_collection_visit_fn visitor, void *opaque,
                             lc_error *e) {
  ps_pouch_owner *owner = context;
  return ps_collection_client_visit(owner->root, client, ns, visitor, opaque,
                                    e);
}
static int work_ensure(void *context, const char *name,
                       enum lc_pouch_storage_work_kind kind, lc_error *e) {
  ps_pouch_owner *owner = context;
  ps_namespace *ns = NULL;
  int rc = ps_root_namespace_acquire(owner->root, name, 1, &ns, e);
  if (rc == LC_OK)
    rc = ps_namespace_work_ensure(ns, kind,
                                  kind == LC_POUCH_STORAGE_WORK_TERMINAL, e);
  ps_namespace_release(ns);
  return rc;
}
static int work_remove(void *context, const char *name,
                       enum lc_pouch_storage_work_kind kind, lc_error *e) {
  ps_pouch_owner *owner = context;
  ps_namespace *ns = NULL;
  int rc = retention_namespace(owner, name, &ns, e);
  if (rc == LC_OK && ns)
    rc = ps_namespace_work_remove(ns, kind, e);
  ps_namespace_release(ns);
  return rc;
}
static int work_next(void *context, enum lc_pouch_storage_work_kind kind,
                     const char *after, char **name, char **cursor,
                     lc_error *e) {
  ps_pouch_owner *owner = context;
  return ps_namespace_work_next(owner->root, &owner->allocator, kind, after,
                                name, cursor, e);
}
static int state_maintain(void *context, lc_pouch *pouch,
                          const lc_pouch_maintenance_options *options,
                          lc_pouch_maintenance_result *out, lc_error *e) {
  (void)context;
  return ps_pouch_state_maintain(pouch, options, out, e);
}
static int collections_maintain(void *context, lc_pouch *pouch,
                                const lc_pouch_maintenance_options *options,
                                lc_pouch_maintenance_result *out, lc_error *e) {
  ps_pouch_owner *owner = context;
  return ps_collection_maintenance_run(owner->root, pouch, options, out, e);
}
static int capacity_pressure(void *context, int draining,
                             lc_pouch_storage_capacity *sample, lc_error *e) {
  ps_pouch_owner *owner = context;
  ps_space *space = ps_root_space(owner->root);
  int rc = ps_maintenance_pressure(space, draining, &sample->pressured,
                                   &sample->available, e);
  sample->activity = ps_readiness_activity(space);
  return rc;
}
static void note_mutation(void *context) {
  ps_pouch_owner *owner = context;
  ps_readiness_note_mutation(ps_root_space(owner->root));
}
static const lc_pouch_storage_maintenance_ops maintenance_ops = {
    work_ensure,          work_remove,       work_next,     state_maintain,
    collections_maintain, capacity_pressure, note_mutation, retention_pending};
static int outbox_markers_open(void *context, const char *ns, int kind,
                               int create, lc_pouch_outbox_store *out,
                               lc_error *e) {
  ps_pouch_owner *owner = context;
  return ps_outbox_markers_open(owner->root, ns, kind, create, out, e);
}
static void queue_notify(void *context, const char *ns, const char *queue) {
  ps_pouch_owner *owner = context;
  queue_waiter *waiter;
  pthread_mutex_lock(&owner->root->mutex);
  for (waiter = owner->waiters; waiter; waiter = waiter->next)
    if (!strcmp(waiter->ns, ns) && !strcmp(waiter->queue, queue)) {
      waiter->signaled = 1;
      pthread_cond_signal(&waiter->changed);
    }
  pthread_mutex_unlock(&owner->root->mutex);
}
#ifdef __APPLE__
static int queue_wait_until(pthread_cond_t *condition, pthread_mutex_t *mutex,
                            const struct timespec *deadline) {
  struct timespec remaining;
  if (clock_gettime(CLOCK_MONOTONIC, &remaining) != 0)
    return errno;
  remaining.tv_sec = deadline->tv_sec - remaining.tv_sec;
  remaining.tv_nsec = deadline->tv_nsec - remaining.tv_nsec;
  if (remaining.tv_nsec < 0) {
    --remaining.tv_sec;
    remaining.tv_nsec += 1000000000L;
  }
  if (remaining.tv_sec < 0 || (!remaining.tv_sec && !remaining.tv_nsec))
    return ETIMEDOUT;
  return pthread_cond_timedwait_relative_np(condition, mutex, &remaining);
}
#endif
static int queue_wait(void *context, const char *ns, const char *queue,
                      uint64_t timeout_ms) {
  ps_pouch_owner *owner = context;
  queue_waiter waiter, **link;
#ifndef __APPLE__
  pthread_condattr_t attributes;
#endif
  struct timespec deadline;
  int rc;
  if (!ns || !ns[0] || !queue || !queue[0] || check(context, NULL) != LC_OK ||
      clock_gettime(CLOCK_MONOTONIC, &deadline) != 0)
    return -1;
  /* The common watcher waits in 100 ms slices and rescans canonical state.
   * Notifications are advisory; admission before the next slice has the same
   * bounded rescan fallback as the native filesystem watcher. */
  if (timeout_ms > (uint64_t)INT_MAX)
    timeout_ms = (uint64_t)INT_MAX;
  deadline.tv_sec += (time_t)(timeout_ms / 1000U);
  deadline.tv_nsec += (long)(timeout_ms % 1000U) * 1000000L;
  if (deadline.tv_nsec >= 1000000000L) {
    ++deadline.tv_sec;
    deadline.tv_nsec -= 1000000000L;
  }
  memset(&waiter, 0, sizeof(waiter));
  waiter.ns = ns;
  waiter.queue = queue;
#ifdef __APPLE__
  rc = pthread_cond_init(&waiter.changed, NULL);
#else
  if (pthread_condattr_init(&attributes) != 0)
    return -1;
  rc = pthread_condattr_setclock(&attributes, CLOCK_MONOTONIC);
  if (rc == 0)
    rc = pthread_cond_init(&waiter.changed, &attributes);
  pthread_condattr_destroy(&attributes);
#endif
  if (rc != 0)
    return -1;
  pthread_mutex_lock(&owner->root->mutex);
  waiter.next = owner->waiters;
  owner->waiters = &waiter;
  while (!waiter.signaled && rc == 0)
#ifdef __APPLE__
    rc = queue_wait_until(&waiter.changed, &owner->root->mutex, &deadline);
#else
    rc =
        pthread_cond_timedwait(&waiter.changed, &owner->root->mutex, &deadline);
#endif
  for (link = &owner->waiters; *link != &waiter; link = &(*link)->next) {
  }
  *link = waiter.next;
  pthread_mutex_unlock(&owner->root->mutex);
  pthread_cond_destroy(&waiter.changed);
  if (check(context, NULL) != LC_OK || (rc != 0 && rc != ETIMEDOUT))
    return -1;
  return waiter.signaled ? 1 : 0;
}
static const lc_pouch_index_store_ops *query_indexes(void *context,
                                                     void **out) {
  ps_pouch_owner *owner = context;
  *out = owner->root;
  return &ps_query_index_store_ops;
}
static const lc_pouch_storage_ops storage_ops = {check,
                                                 flush,
                                                 namespace_open,
                                                 namespace_close,
                                                 namespace_info,
                                                 namespaces_visit,
                                                 replay,
                                                 replay_suffix,
                                                 refresh_state,
                                                 append_begin,
                                                 append_commit,
                                                 append_close,
                                                 source_open,
                                                 source_clone,
                                                 retention_hint_ensure,
                                                 retention_work_load,
                                                 retention_work_store,
                                                 retention_hint_remove,
                                                 retention_hint_next,
                                                 txn_guard_lock,
                                                 txn_guard_covers,
                                                 txn_guard_release,
                                                 release,
                                                 collection_open,
                                                 collections_visit,
                                                 &maintenance_ops,
                                                 outbox_markers_open,
                                                 queue_notify,
                                                 queue_wait,
                                                 query_indexes,
                                                 &metadata_ops,
                                                 namespace_state_lock,
                                                 namespace_state_unlock};

ps_root *ps_pouch_state_root(lc_pouch *pouch) {
  ps_pouch_owner *owner;
  if (!pouch || pouch->storage.ops != &storage_ops)
    return NULL;
  owner = pouch->storage.context;
  return owner->root;
}
size_t ps_pouch_queue_waiter_count(lc_pouch *pouch, const char *ns,
                                   const char *queue) {
  ps_pouch_owner *owner = pouch->storage.context;
  queue_waiter *waiter;
  size_t count = 0U;
  pthread_mutex_lock(&owner->root->mutex);
  for (waiter = owner->waiters; waiter; waiter = waiter->next)
    if (!strcmp(waiter->ns, ns) && !strcmp(waiter->queue, queue))
      ++count;
  pthread_mutex_unlock(&owner->root->mutex);
  return count;
}

int ps_pouch_state_open(const char *path, int create,
                        const lc_allocator *allocator,
                        const ps_pouch_state_options *options, lc_pouch **out,
                        lc_error *e) {
  lc_pouch *pouch = NULL;
  ps_pouch_owner *owner;
  ps_root_options root_options;
  char *compression;
  struct stat identity;
  int rc;
  if (!out)
    return lc_error_set(e, LC_ERR_INVALID, 0, "container pouch needs output",
                        NULL, NULL, "pouch");
  *out = NULL;
  rc = lc_pouch_runtime_open(path, allocator,
                             options ? &options->logical : NULL, &pouch, e);
  if (rc != LC_OK)
    return rc;
  /* One segment can turn over within the initial container's ordinary
   * headroom. Explicit settings still govern their owner. */
  if (!options || !options->logical.segment_target_bytes)
    pouch->segment_target_bytes = PS_MIB;
  if (options && options->logical.crypto_generate_key_file) {
    lc_pouch_close(pouch);
    return lc_error_set(e, LC_ERR_UNSUPPORTED, 0,
                        "container roots do not generate external key files",
                        NULL, NULL, "pouch");
  }
  owner = lc_calloc_with_allocator(&pouch->allocator, 1, sizeof(*owner));
  if (!owner) {
    lc_pouch_close(pouch);
    return lc_error_set(e, LC_ERR_NOMEM, 0, "cannot allocate container owner",
                        NULL, NULL, "pouch");
  }
  owner->allocator = pouch->allocator;
  owner->runtime = pouch;
  owner->references = 1;
  memset(&root_options, 0, sizeof(root_options));
  root_options.storage.durable_sync = pouch->durable_sync;
  root_options.storage.shared = !pouch->single_writer;
  root_options.crypto_allocator = &pouch->allocator;
  root_options.crypto.compression_enabled = !strcmp(pouch->compression, "zlib");
  if (options) {
    root_options.storage.initial_capacity = options->initial_capacity;
    root_options.storage.maximum_capacity = options->maximum_capacity;
    root_options.storage.io = options->io;
    root_options.storage.coordination_io = options->coordination_io;
    root_options.crypto.key_string = options->logical.crypto_key;
    root_options.crypto.key_file = options->logical.crypto_key_file;
    root_options.compression_explicit = options->logical.compression != NULL;
  }
  rc = ps_root_open(path, create, &root_options, &owner->root, e);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, owner);
    lc_pouch_close(pouch);
    return rc;
  }
  pouch->storage.ops = &storage_ops;
  pouch->storage.context = owner;
  pouch->storage.coordinated_namespaces =
      ps_coordination_shared(ps_space_coordination(ps_root_space(owner->root)));
  if (pouch->queue_watch_enabled) {
    pouch->queue_watch_mode = "inprocess";
    pouch->queue_watch_reason = "container_notifications";
  }
  pouch->crypto = ps_root_crypto(owner->root);
  if (stat(path, &identity) != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(e, LC_ERR_TRANSPORT, 0,
                        "cannot identify container root", NULL, NULL, "pouch");
  }
  pouch->root_device = identity.st_dev;
  pouch->root_inode = identity.st_ino;
  pouch->root_identity_initialized = 1;
  compression = lc_strdup_with_allocator(
      &pouch->allocator,
      lc_pouch_crypto_compression_enabled(pouch->crypto) ? "zlib" : "none");
  if (!compression) {
    lc_pouch_close(pouch);
    return lc_error_set(e, LC_ERR_NOMEM, 0, "cannot retain root compression",
                        NULL, NULL, "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, pouch->compression);
  pouch->compression = compression;
  rc = lc_pouch_runtime_start_maintenance(pouch, e);
  if (rc == LC_OK)
    rc = lc_pouch_runtime_start_indexing(pouch, e);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  *out = pouch;
  return LC_OK;
}
