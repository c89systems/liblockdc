#include "lc_api_internal.h"
#include "lc_pouch_retention_record.h"
#include "namespace_internal.h"
#include <string.h>
#include <zlib.h>

/* Routing slot 1 is the segment head. Always-durable control slot 2 is the
 * existing discovery marker; slot 3 holds the optional LPR1 work reference.
 * Idle needs only the marker. Work contains no outcome or application body. */
static int failure(lc_error *e, int rc, const char *message) {
  return lc_error_set(e, rc, 0, message, NULL, NULL, "pouch-space");
}
static uint64_t get64(const unsigned char *p) {
  uint64_t n = 0;
  size_t i;
  for (i = 0; i < 8; ++i)
    n |= (uint64_t)p[i] << (8 * i);
  return n;
}
static void put64(unsigned char *p, uint64_t n) {
  size_t i;
  for (i = 0; i < 8; ++i)
    p[i] = (unsigned char)(n >> (8 * i));
}
static ps_key control(ps_namespace *ns, uint64_t slot) {
  ps_key key = {0, 0, PS_CONTROL};
  key.owner = ns->identity.owner;
  key.key = slot;
  return key;
}
static int delete_present(ps_namespace *ns, uint64_t slot, lc_error *e) {
  ps_reader *reader = NULL;
  ps_key key = control(ns, slot);
  int rc = ps_read_open(ns->root->space, key, NULL, &reader, e);
  if (rc == LC_ERR_INVALID) {
    lc_error_cleanup(e);
    return LC_OK;
  }
  /* Namespace mutation authority excludes a concurrent control writer. Keep
   * the selected predecessor pinned through durable deletion. */
  if (rc == LC_OK)
    rc = ps_delete(ns->root->space, key, e);
  ps_reader_close(reader);
  return rc;
}
static int retention_read(ps_namespace *ns, int *present, lc_error *e) {
  ps_key key = control(ns, 2);
  unsigned char bytes[32];
  ps_reader *reader = NULL;
  size_t got = 0, i;
  int rc;
  *present = 0;
  rc = ps_read_open(ns->root->space, key, NULL, &reader, e);
  if (rc == LC_ERR_INVALID) {
    lc_error_cleanup(e);
    return LC_OK;
  }
  if (rc != LC_OK)
    return rc;
  if (ps_read_size(reader) != sizeof(bytes))
    rc = failure(e, LC_ERR_PROTOCOL,
                 "namespace retention hint has invalid size");
  if (rc == LC_OK)
    rc = ps_read(reader, bytes, sizeof(bytes), &got, e);
  ps_reader_close(reader);
  if (rc != LC_OK)
    return rc;
  if (got != sizeof(bytes) || memcmp(bytes, "LCPSRETN", 8) ||
      get64(bytes + 8) != key.owner || bytes[16] != 1 ||
      get64(bytes + 24) != (uint64_t)crc32(0L, bytes, 24))
    return failure(e, LC_ERR_PROTOCOL, "namespace retention hint is invalid");
  for (i = 17; i < 24; ++i)
    if (bytes[i])
      return failure(e, LC_ERR_PROTOCOL,
                     "namespace retention hint has unknown fields");
  *present = 1;
  return LC_OK;
}
static int retention_cached(struct ps_namespace_data *d, lc_error *e) {
  int rc = LC_OK;
  if (!d->retention_loaded) {
    rc = retention_read(d->ns, &d->retention_present, e);
    if (rc == LC_OK)
      d->retention_loaded = 1;
  }
  return rc;
}
int ps_namespace_retention_hint_get(ps_namespace *ns, int *out, lc_error *e) {
  if (out)
    *out = 0;
  if (!ns || !out)
    return failure(e, LC_ERR_INVALID, "namespace retention hint needs output");
  /* No segment opening, canonical replay or body reads. Admission checks
   * poison. */
  return retention_read(ns, out, e);
}
int ps_namespace_retention_hint_present(ps_namespace *ns, int *out,
                                        lc_error *e) {
  ps_reader *reader = NULL;
  int rc;
  if (!ns || !out)
    return failure(e, LC_ERR_INVALID, "retention discovery requires output");
  *out = 0;
  rc = ps_read_open(ns->root->space, control(ns, 2), NULL, &reader, e);
  if (rc == LC_ERR_INVALID) {
    lc_error_cleanup(e);
    rc = LC_OK;
  } else if (rc == LC_OK)
    *out = 1;
  ps_reader_close(reader);
  return rc;
}
int ps_namespace_retention_hint_ensure(ps_namespace *ns, lc_error *e) {
  struct ps_namespace_data *d;
  unsigned char bytes[32];
  ps_writer *writer = NULL;
  int rc = ps_ns_data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&d->mutex);
  rc = retention_cached(d, e);
  if (rc == LC_OK && !d->retention_present) {
    memset(bytes, 0, sizeof(bytes));
    memcpy(bytes, "LCPSRETN", 8);
    put64(bytes + 8, ns->identity.owner);
    bytes[16] = 1;
    put64(bytes + 24, (uint64_t)crc32(0L, bytes, 24));
    rc = ps_begin(ns->root->space, NULL, control(ns, 2), &writer, e);
    if (rc == LC_OK)
      rc = ps_write(writer, bytes, sizeof(bytes), e);
    if (rc == LC_OK)
      rc = ps_publish(writer, NULL, e);
    ps_writer_close(writer);
    if (rc == LC_OK)
      d->retention_present = 1;
  }
  pthread_mutex_unlock(&d->mutex);
  return rc;
}
int ps_namespace_retention_work_load(ps_namespace *ns, const lc_allocator *a,
                                     char **out, size_t *bytes, lc_error *e) {
  ps_reader *reader = NULL;
  char *text = NULL;
  size_t length = 0, got;
  int present, rc;
  if (out)
    *out = NULL;
  if (bytes)
    *bytes = 0;
  if (!ns || !out || !bytes)
    return failure(e, LC_ERR_INVALID, "retention work load requires outputs");
  rc = retention_read(ns, &present, e);
  if (rc != LC_OK || !present)
    return rc;
  rc = ps_read_open(ns->root->space, control(ns, 3), NULL, &reader, e);
  if (rc == LC_ERR_INVALID) {
    lc_error_cleanup(e);
    text = lc_strdup_with_allocator(a, LC_POUCH_RETENTION_IDLE_TEXT);
    rc = text ? LC_OK
              : failure(e, LC_ERR_NOMEM, "cannot allocate idle retention work");
    length = 7;
  } else if (rc == LC_OK) {
    if (ps_read_size(reader) < 7 ||
        ps_read_size(reader) > LC_POUCH_RETENTION_CONTROL_MAX)
      rc = failure(e, LC_ERR_PROTOCOL, "retention work exceeds local bounds");
    else {
      length = (size_t)ps_read_size(reader);
      text = lc_alloc_with_allocator(a, length + 1);
      if (!text)
        rc = failure(e, LC_ERR_NOMEM, "cannot allocate retention work");
      else {
        rc = ps_read(reader, text, length, &got, e);
        if (rc == LC_OK && got != length)
          rc = failure(e, LC_ERR_PROTOCOL, "retention work is incomplete");
      }
    }
  }
  ps_reader_close(reader);
  if (rc == LC_OK) {
    text[length] = '\0';
    *out = text;
    *bytes = length;
  } else
    lc_free_with_allocator(a, text);
  return rc;
}
int ps_namespace_retention_work_store(ps_namespace *ns, const char *text,
                                      size_t bytes, lc_error *e) {
  struct ps_namespace_data *d;
  ps_space *space;
  ps_budget_plan plan = {0};
  ps_budget *budget = NULL;
  ps_writer *writer = NULL;
  int rc;
  if ((!text && bytes) ||
      (text && (bytes < 7 || bytes > LC_POUCH_RETENTION_CONTROL_MAX)))
    return failure(e, LC_ERR_INVALID, "retention work exceeds local bounds");
  rc = ps_ns_data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  space = ns->root->space;
  pthread_mutex_lock(&d->mutex);
  rc = retention_cached(d, e);
  if (rc == LC_OK && !d->retention_present)
    rc = failure(e, LC_ERR_TRANSPORT,
                 "state retention work requires an existing discovery hint");
  if (rc == LC_OK && !text)
    rc = delete_present(ns, 3, e);
  else if (rc == LC_OK) {
    plan.maintenance = 1;
    rc = ps_budget_plan_value(&plan, bytes, e);
    if (rc == LC_OK) {
      rc = ps_budget_open(space, &plan, &budget, e);
      /* This known bounded plan is valid. Reclaim superseded durable controls
       * once if admission is full, before any private payload is produced.
       * Pins and all-live capacity may still make admission fail. No growth. */
      if (rc == LC_ERR_INVALID) {
        lc_error_cleanup(e);
        rc = ps_checkpoint(space, e);
        if (rc == LC_OK)
          rc = ps_budget_open(space, &plan, &budget, e);
      }
    }
    if (rc == LC_OK)
      rc = ps_begin(space, budget, control(ns, 3), &writer, e);
    if (rc == LC_OK)
      rc = ps_write(writer, text, bytes, e);
    if (rc == LC_OK)
      rc = ps_publish(writer, NULL, e);
    ps_writer_close(writer);
    if (rc == LC_OK)
      rc = ps_budget_close(budget, e);
    else
      (void)ps_budget_close(budget, NULL);
  }
  pthread_mutex_unlock(&d->mutex);
  return rc;
}
int ps_namespace_retention_hint_remove(ps_namespace *ns, lc_error *e) {
  struct ps_namespace_data *d;
  int rc = ps_ns_data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&d->mutex);
  /* Work must be durably cleared before discovery can disappear. A failure
   * between them leaves an idle positive hint, never undiscoverable work. */
  rc = delete_present(ns, 3, e);
  if (rc == LC_OK)
    rc = delete_present(ns, 2, e);
  if (rc == LC_OK) {
    d->retention_loaded = 1;
    d->retention_present = 0;
  }
  pthread_mutex_unlock(&d->mutex);
  return rc;
}
