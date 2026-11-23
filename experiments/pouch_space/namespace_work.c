#include "namespace_work.h"
#include "budget.h"
#include "lc_api_internal.h"
#include "root_internal.h"
#include "space_internal.h"
#include <string.h>
#include <zlib.h>

static int failure(lc_error *e, int code, const char *message) {
  return lc_error_set(e, code, 0L, message, NULL, NULL, "pouch");
}
static uint64_t get64(const unsigned char *p) {
  uint64_t n = 0U;
  size_t i;
  for (i = 0U; i < 8U; ++i)
    n |= (uint64_t)p[i] << (8U * i);
  return n;
}
static void put64(unsigned char *p, uint64_t n) {
  size_t i;
  for (i = 0U; i < 8U; ++i)
    p[i] = (unsigned char)(n >> (8U * i));
}
static ps_key key_for(ps_namespace *ns, enum lc_pouch_storage_work_kind kind) {
  ps_key key;
  /* Namespace control slots 4..6; retention's progress owns slot 7. */
  key.owner = ns->identity.owner;
  key.key = 4U + (unsigned int)kind;
  key.kind = PS_CONTROL;
  return key;
}
static int valid(ps_namespace *ns, enum lc_pouch_storage_work_kind kind,
                 lc_error *e) {
  if (!ns || kind < LC_POUCH_STORAGE_WORK_ORDINARY ||
      kind > LC_POUCH_STORAGE_WORK_COLLECTION)
    return failure(e, LC_ERR_INVALID, "invalid namespace maintenance hint");
  return ps_check(ns->root->space, e);
}
int ps_namespace_work_get(ps_namespace *ns,
                          enum lc_pouch_storage_work_kind kind, int *out,
                          lc_error *e) {
  ps_reader *reader = NULL;
  unsigned char bytes[32];
  size_t got = 0U;
  int rc = valid(ns, kind, e);
  if (!out)
    return failure(e, LC_ERR_INVALID, "maintenance hint requires output");
  *out = 0;
  if (rc != LC_OK)
    return rc;
  rc = ps_read_open(ns->root->space, key_for(ns, kind), NULL, &reader, e);
  if (rc == LC_ERR_INVALID) {
    lc_error_cleanup(e);
    return LC_OK;
  }
  if (rc == LC_OK && ps_read_size(reader) != sizeof(bytes))
    rc = failure(e, LC_ERR_PROTOCOL, "maintenance hint has invalid size");
  if (rc == LC_OK)
    rc = ps_read(reader, bytes, sizeof(bytes), &got, e);
  ps_reader_close(reader);
  if (rc != LC_OK)
    return rc;
  if (got != sizeof(bytes) || memcmp(bytes, "LCPSWORK", 8U) ||
      get64(bytes + 8U) != ns->identity.owner ||
      get64(bytes + 16U) != (uint64_t)kind ||
      get64(bytes + 24U) != (uint64_t)crc32(0L, bytes, 24U))
    return failure(e, LC_ERR_PROTOCOL, "maintenance hint metadata is invalid");
  *out = 1;
  return LC_OK;
}
static int prepare_work_locked(ps_namespace *ns,
                               enum lc_pouch_storage_work_kind kind,
                               int maintenance, lc_error *e) {
  unsigned char bytes[32];
  ps_writer *writer = NULL;
  ps_budget *budget = NULL;
  ps_budget_plan plan = {0};
  unsigned int bit = 1U << (unsigned int)kind;
  int ordinary = kind == LC_POUCH_STORAGE_WORK_ORDINARY;
  int present = 0, rc = LC_OK;
  if (maintenance < 0 || maintenance > 1)
    return failure(e, LC_ERR_INVALID,
                   "invalid maintenance hint admission class");
  if (ordinary && ns->ordinary_work_ready)
    return LC_OK;
  /* Shared collection name publication and hint removal take the namespace's
   * collection catalog authority. A peer may have cleared our cached hint. */
  if (!(ns->work_loaded & bit) ||
      (kind == LC_POUCH_STORAGE_WORK_COLLECTION &&
       ps_coordination_shared(ps_space_coordination(ns->root->space)))) {
    rc = ps_namespace_work_get(ns, kind, &present, e);
    if (rc == LC_OK) {
      ns->work_loaded |= bit;
      ns->work_present = (ns->work_present & ~bit) | (present ? bit : 0U);
    }
  }
  if (rc == LC_OK && !(ns->work_present & bit)) {
    memset(bytes, 0, sizeof(bytes));
    memcpy(bytes, "LCPSWORK", 8U);
    put64(bytes + 8U, ns->identity.owner);
    put64(bytes + 16U, (uint64_t)kind);
    put64(bytes + 24U, (uint64_t)crc32(0L, bytes, 24U));
    if (maintenance) {
      plan.maintenance = 1;
      rc = ps_budget_plan_value(&plan, sizeof(bytes), e);
      if (rc == LC_OK)
        rc = ps_budget_open(ns->root->space, &plan, &budget, e);
    }
    if (rc == LC_OK)
      rc = ps_begin(ns->root->space, budget, key_for(ns, kind), &writer, e);
    if (rc == LC_OK)
      rc = ps_write(writer, bytes, sizeof(bytes), e);
    if (rc == LC_OK) {
      if (ordinary)
        rc = ps_internal_publish_deferred(writer, &ns->ordinary_work_ready, e);
      else
        rc = ps_publish(writer, NULL, e);
    }
    ps_writer_close(writer);
    (void)ps_budget_close(budget, NULL);
    if (rc == LC_OK && !ordinary)
      ns->work_present |= bit;
  }
  return rc;
}
static int complete_ordinary_locked(ps_namespace *ns, lc_error *e) {
  int rc;
  if (!ns->ordinary_work_ready)
    return LC_OK;
  rc = ps_internal_durable(ns->root->space, ns->ordinary_work_ready, e);
  if (rc == LC_OK) {
    ns->ordinary_work_ready = 0;
    ns->work_present |= 1U << LC_POUCH_STORAGE_WORK_ORDINARY;
  }
  return rc;
}
int ps_namespace_work_ensure(ps_namespace *ns,
                             enum lc_pouch_storage_work_kind kind,
                             int maintenance, lc_error *e) {
  int rc = valid(ns, kind, e);
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&ns->maintenance);
  rc = prepare_work_locked(ns, kind, maintenance, e);
  if (rc == LC_OK && kind == LC_POUCH_STORAGE_WORK_ORDINARY)
    rc = complete_ordinary_locked(ns, e);
  pthread_mutex_unlock(&ns->maintenance);
  return rc;
}
int ps_namespace_work_stage_ordinary(ps_namespace *ns, int maintenance,
                                     int *pending, lc_error *e) {
  int rc = valid(ns, LC_POUCH_STORAGE_WORK_ORDINARY, e);
  *pending = 0;
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&ns->maintenance);
  rc = prepare_work_locked(ns, LC_POUCH_STORAGE_WORK_ORDINARY, maintenance, e);
  *pending = rc == LC_OK && ns->ordinary_work_ready != 0;
  pthread_mutex_unlock(&ns->maintenance);
  return rc;
}
int ps_namespace_work_complete_ordinary(ps_namespace *ns, lc_error *e) {
  int rc;
  pthread_mutex_lock(&ns->maintenance);
  rc = complete_ordinary_locked(ns, e);
  pthread_mutex_unlock(&ns->maintenance);
  return rc;
}
int ps_namespace_work_remove(ps_namespace *ns,
                             enum lc_pouch_storage_work_kind kind,
                             lc_error *e) {
  unsigned int bit;
  int present = 0, rc = valid(ns, kind, e);
  if (rc != LC_OK)
    return rc;
  bit = 1U << (unsigned int)kind;
  pthread_mutex_lock(&ns->maintenance);
  if (kind == LC_POUCH_STORAGE_WORK_ORDINARY)
    rc = complete_ordinary_locked(ns, e);
  if (rc == LC_OK)
    rc = ps_namespace_work_get(ns, kind, &present, e);
  if (rc == LC_OK && present)
    rc = ps_delete(ns->root->space, key_for(ns, kind), e);
  if (rc == LC_OK) {
    ns->work_loaded |= bit;
    ns->work_present &= ~bit;
  }
  pthread_mutex_unlock(&ns->maintenance);
  return rc;
}
typedef struct discovery {
  ps_root *root;
  const lc_allocator *allocator;
  const char *after;
  enum lc_pouch_storage_work_kind kind;
  char *first, *next;
} discovery;
static int select_work(const char *name, const ps_namespace_info *info,
                       void *context, lc_error *e) {
  discovery *d = context;
  ps_namespace *ns = NULL;
  ps_reader *hint = NULL;
  char *copy;
  int present = 0;
  int rc = ps_root_namespace_acquire(d->root, name, 0, &ns, e);
  (void)info;
  if (rc == LC_OK) {
    /* Select the durable metadata identity without decoding its value. The
     * selected pass validates it. A malformed hint must not hide other names
     * behind the same discovery source or force discovery to read payloads. */
    rc = ps_read_open(ns->root->space, key_for(ns, d->kind), NULL, &hint, e);
    if (rc == LC_ERR_INVALID) {
      lc_error_cleanup(e);
      rc = LC_OK;
    } else if (rc == LC_OK)
      present = 1;
  }
  ps_reader_close(hint);
  ps_namespace_release(ns);
  if (rc != LC_OK || !present)
    return rc;
  if (!d->first || strcmp(name, d->first) < 0) {
    copy = lc_strdup_with_allocator(d->allocator, name);
    if (!copy)
      goto no_memory;
    lc_free_with_allocator(d->allocator, d->first);
    d->first = copy;
  }
  if (d->after && strcmp(name, d->after) > 0 &&
      (!d->next || strcmp(name, d->next) < 0)) {
    copy = lc_strdup_with_allocator(d->allocator, name);
    if (!copy)
      goto no_memory;
    lc_free_with_allocator(d->allocator, d->next);
    d->next = copy;
  }
  return LC_OK;
no_memory:
  return failure(e, LC_ERR_NOMEM, "cannot select maintenance namespace");
}
int ps_namespace_work_next(ps_root *root, const lc_allocator *allocator,
                           enum lc_pouch_storage_work_kind kind,
                           const char *after, char **name, char **cursor,
                           lc_error *e) {
  discovery d;
  char *selected;
  int rc;
  memset(&d, 0, sizeof(d));
  d.root = root;
  d.allocator = allocator;
  d.after = after;
  d.kind = kind;
  if (name)
    *name = NULL;
  if (cursor)
    *cursor = NULL;
  if (!root || !name || !cursor || kind < LC_POUCH_STORAGE_WORK_ORDINARY ||
      kind > LC_POUCH_STORAGE_WORK_COLLECTION)
    return failure(e, LC_ERR_INVALID, "invalid maintenance discovery request");
  *name = *cursor = NULL;
  rc = ps_root_namespace_visit(root, select_work, &d, e);
  selected = d.next ? d.next : d.first;
  if (rc == LC_OK && selected) {
    *name = lc_strdup_with_allocator(allocator, selected);
    if (!*name)
      rc = failure(e, LC_ERR_NOMEM, "cannot copy maintenance namespace");
    else {
      *cursor = selected;
      if (d.next)
        d.next = NULL;
      else
        d.first = NULL;
    }
  }
  lc_free_with_allocator(allocator, d.first);
  lc_free_with_allocator(allocator, d.next);
  return rc;
}
