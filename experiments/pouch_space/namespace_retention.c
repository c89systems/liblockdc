#include "lc_api_internal.h"
#include "namespace_internal.h"
#include "space_internal.h"
#include <string.h>
#include <zlib.h>

/* Control slots 2/3 hold discovery and the immutable work identity. Slots
 * 4 through 6 belong to namespace maintenance hints; slot 7 holds the small
 * incarnation-bound progress record. Work contains no outcome or application
 * body. */
#define WORK_PROGRESS_SLOT 7U
#define WORK_IDENTITY_SLOT 3U
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
  int shared, rc = ps_ns_data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  shared = ps_coordination_shared(ps_space_coordination(ns->root->space));
  if (shared && (rc = ps_ns_enter(d, e)) != LC_OK)
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
  if (shared)
    ps_ns_leave(d);
  return rc;
}
#define WORK_HEADER 80U
#define WORK_PROGRESS 80U
typedef struct retention_snapshot {
  unsigned char header[WORK_HEADER];
  ps_ref identity;
  uint64_t fencing, version;
  int phase;
} retention_snapshot;
/* Zero-fenced work still needs its claim facts and terminal publication.
 * Claimed work retains only the terminal credit. Further fact changes admit
 * their replacement before consuming any of that terminal credit. */
static unsigned int future_progress(int phase, uint64_t fencing) {
  return phase == LC_POUCH_RETENTION_ACTIVE ? (fencing ? 1U : 2U) : 0U;
}
static int read_exact(ps_reader *reader, void *bytes, size_t count,
                      lc_error *e) {
  size_t got = 0;
  int rc = ps_read(reader, bytes, count, &got, e);
  return rc != LC_OK ? rc
         : got == count
             ? LC_OK
             : failure(e, LC_ERR_PROTOCOL, "incomplete retention control");
}
static int progress_plan(ps_budget_plan *plan, unsigned int writes,
                         lc_error *e) {
  int rc = LC_OK;
  memset(plan, 0, sizeof(*plan));
  plan->maintenance = 1;
  while (rc == LC_OK && writes--)
    rc = ps_budget_plan_value(plan, WORK_PROGRESS, e);
  return rc;
}
static int snapshot(ps_namespace *, ps_reader **, retention_snapshot *,
                    lc_error *);
int ps_ns_retention_recovery_needed(ps_space *s, uint64_t owner) {
  ps_key key = {0, WORK_IDENTITY_SLOT, PS_CONTROL};
  key.owner = owner;
  return ps_internal_lookup(s, key) != NULL;
}
int ps_ns_retention_restore(struct ps_namespace_data *d, lc_error *e) {
  ps_reader *reader = NULL;
  ps_budget_plan plan;
  ps_ref identity;
  uint64_t bytes;
  retention_snapshot captured;
  int present,
      rc = ps_value_info(d->ns->root->space, control(d->ns, WORK_IDENTITY_SLOT),
                         &identity, &bytes, &present, e);
  if (rc != LC_OK || !present)
    return rc;
  /* Only live work retains a completion claim. Finished work may already have
   * released its workspace to producers. Read its bounded header/progress,
   * never materialize its key or any application body during root admission. */
  rc = snapshot(d->ns, &reader, &captured, e);
  ps_reader_close(reader);
  if (rc == LC_ERR_PROTOCOL) {
    /* Malformed work is consumed as a namespace-local protocol error, without
     * hiding its discovery marker or starving unrelated namespaces at open. */
    lc_error_cleanup(e);
    return LC_OK;
  }
  if (rc == LC_OK && captured.phase == LC_POUCH_RETENTION_ACTIVE) {
    rc = progress_plan(&plan, future_progress(captured.phase, captured.fencing),
                       e);
    if (rc == LC_OK)
      rc = ps_budget_open(d->ns->root->space, &plan, &d->retention_budget, e);
  }
  return rc;
}
static int snapshot(ps_namespace *ns, ps_reader **out,
                    retention_snapshot *captured, lc_error *e) {
  unsigned char *header = captured->header;
  ps_ref *identity = &captured->identity;
  ps_reader *reader = NULL, *progress = NULL;
  ps_values *values = NULL;
  ps_key key;
  ps_ref ref;
  uint64_t length;
  unsigned char bytes[WORK_PROGRESS];
  char xid[LC_XID_STRING_SIZE];
  size_t i;
  int rc = ps_values_capture(ns->root->space, ns->identity.owner, PS_CONTROL,
                             PS_CONTROL, &values, e);
  *out = NULL;
  memset(captured, 0, sizeof(*captured));
  /* Identity and progress must name the same selected prefix. Payload reads
   * happen after capture without publication authority or a namespace mutex. */
  for (i = 0; rc == LC_OK && i < ps_values_count(values); ++i) {
    rc = ps_values_info(values, i, &key, &ref, &length, e);
    if (rc == LC_OK && (key.key == 3 || key.key == WORK_PROGRESS_SLOT))
      rc =
          ps_values_read_open(values, i, key.key == 3 ? &reader : &progress, e);
  }
  ps_values_close(values);
  if (rc == LC_OK && !reader) {
    ps_reader_close(progress);
    return LC_OK;
  }
  if (rc == LC_OK)
    rc = read_exact(reader, header, WORK_HEADER, e);
  if (rc == LC_OK) {
    memcpy(xid, header + 40, LC_XID_STRING_LENGTH);
    xid[LC_XID_STRING_LENGTH] = 0;
    if (memcmp(header, "LCPSRTWK", 8) ||
        get64(header + 8) != ns->identity.owner || !get64(header + 32) ||
        get64(header + 32) > LC_POUCH_STATE_RECORD_KEY_MAX_BYTES ||
        ps_read_size(reader) != WORK_HEADER + get64(header + 32) ||
        !lc_xid_is_valid(xid) ||
        (header[60] != LC_POUCH_RETENTION_ACTIVE &&
         header[60] != LC_POUCH_RETENTION_FINISHED) ||
        get64(header + 72) != (uint64_t)crc32(0L, header, 72))
      rc = failure(e, LC_ERR_PROTOCOL, "invalid retention work identity");
    for (i = 61; rc == LC_OK && i < 64; ++i)
      if (header[i])
        rc = failure(e, LC_ERR_PROTOCOL, "unknown retention work fields");
  }
  if (rc == LC_OK) {
    captured->phase = header[60];
    captured->fencing = get64(header + 16);
    captured->version = get64(header + 24);
    ps_read_reference(reader, identity);
    if (progress) {
      rc = ps_read_size(progress) == WORK_PROGRESS
               ? read_exact(progress, bytes, sizeof(bytes), e)
               : failure(e, LC_ERR_PROTOCOL, "invalid retention progress size");
      if (rc == LC_OK && (memcmp(bytes, "LCPSRTPR", 8) ||
                          get64(bytes + 8) != ns->identity.owner ||
                          (get64(bytes + 40) != LC_POUCH_RETENTION_ACTIVE &&
                           get64(bytes + 40) != LC_POUCH_RETENTION_FINISHED) ||
                          get64(bytes + 64) ||
                          get64(bytes + 72) != (uint64_t)crc32(0L, bytes, 72)))
        rc = failure(e, LC_ERR_PROTOCOL, "invalid retention progress");
      if (rc == LC_OK && !memcmp(bytes + 16, identity->container, 16) &&
          get64(bytes + 32) == identity->incarnation) {
        if (captured->phase == LC_POUCH_RETENTION_FINISHED &&
            get64(bytes + 40) == LC_POUCH_RETENTION_ACTIVE)
          rc = failure(e, LC_ERR_PROTOCOL,
                       "retention completion moves backwards");
        captured->phase = (int)get64(bytes + 40);
        captured->fencing = get64(bytes + 48);
        captured->version = get64(bytes + 56);
      }
    }
  }
  ps_reader_close(progress);
  if (rc == LC_OK)
    *out = reader;
  else
    ps_reader_close(reader);
  return rc;
}
int ps_namespace_retention_work_load(ps_namespace *ns, const lc_allocator *a,
                                     lc_pouch_retention_work *out,
                                     lc_error *e) {
  ps_reader *reader = NULL;
  retention_snapshot captured;
  const unsigned char *header = captured.header;
  size_t length;
  int present, rc;
  if (!ns || !out)
    return failure(e, LC_ERR_INVALID, "retention work load requires output");
  memset(out, 0, sizeof(*out));
  rc = retention_read(ns, &present, e);
  if (rc != LC_OK || !present)
    return rc;
  rc = snapshot(ns, &reader, &captured, e);
  if (rc == LC_OK && reader) {
    length = (size_t)get64(header + 32);
    out->txn_id = lc_alloc_with_allocator(a, LC_XID_STRING_SIZE);
    out->key = lc_alloc_with_allocator(a, length + 1);
    if (!out->txn_id || !out->key)
      rc = failure(e, LC_ERR_NOMEM, "cannot allocate retention work");
    if (rc == LC_OK)
      rc = read_exact(reader, out->key, length, e);
    if (rc == LC_OK && (memchr(out->key, 0, length) ||
                        get64(header + 64) !=
                            (uint64_t)crc32(0L, (const unsigned char *)out->key,
                                            (uInt)length)))
      rc = failure(e, LC_ERR_PROTOCOL, "invalid retention work key");
    if (rc == LC_OK) {
      out->key[length] = 0;
      memcpy(out->txn_id, header + 40, LC_XID_STRING_LENGTH);
      out->txn_id[LC_XID_STRING_LENGTH] = 0;
      out->fencing_token = captured.fencing;
      out->state_version = captured.version;
    }
  }
  ps_reader_close(reader);
  if (rc == LC_OK) {
    out->present = 1;
    out->phase = captured.phase;
  } else {
    lc_free_with_allocator(a, out->txn_id);
    lc_free_with_allocator(a, out->key);
    memset(out, 0, sizeof(*out));
  }
  return rc;
}
static int same_work(ps_reader *reader, const unsigned char *header,
                     const lc_pouch_retention_work *work, size_t length,
                     int *same, lc_error *e) {
  unsigned char bytes[4096];
  size_t offset = 0, take;
  int rc = LC_OK;
  *same = reader && get64(header + 32) == length &&
          !memcmp(header + 40, work->txn_id, LC_XID_STRING_LENGTH);
  while (rc == LC_OK && *same && offset < length) {
    take = length - offset;
    if (take > sizeof(bytes))
      take = sizeof(bytes);
    rc = read_exact(reader, bytes, take, e);
    if (rc == LC_OK && memcmp(bytes, work->key + offset, take))
      *same = 0;
    offset += take;
  }
  return rc;
}
static int clear_work(struct ps_namespace_data *d, lc_error *e) {
  int rc = delete_present(d->ns, 3, e);
  /* Identity absence is idle. Keep the bounded obsolete progress until hint
   * retirement; its exact incarnation cannot finish a later identity. This
   * avoids another durability barrier on every work cycle. */
  if (rc == LC_OK) {
    (void)ps_budget_close(d->retention_budget, NULL);
    d->retention_budget = NULL;
  }
  return rc;
}
static int publish_progress(struct ps_namespace_data *d, const ps_ref *identity,
                            const lc_pouch_retention_work *work, lc_error *e) {
  ps_writer *writer = NULL;
  ps_budget_plan plan;
  unsigned char bytes[WORK_PROGRESS] = {0};
  unsigned int future = future_progress(work->phase, work->fencing_token);
  int rc = progress_plan(&plan, future + 1, e);
  if (rc == LC_OK)
    rc = d->retention_budget ? ps_budget_resize(d->retention_budget, &plan, e)
                             : ps_budget_open(d->ns->root->space, &plan,
                                              &d->retention_budget, e);
  if (rc == LC_OK) {
    memcpy(bytes, "LCPSRTPR", 8);
    put64(bytes + 8, d->ns->identity.owner);
    memcpy(bytes + 16, identity->container, 16);
    put64(bytes + 32, identity->incarnation);
    put64(bytes + 40, (uint64_t)work->phase);
    put64(bytes + 48, work->fencing_token);
    put64(bytes + 56, work->state_version);
    put64(bytes + 72, (uint64_t)crc32(0L, bytes, 72));
    rc = ps_begin(d->ns->root->space, d->retention_budget,
                  control(d->ns, WORK_PROGRESS_SLOT), &writer, e);
  }
  if (rc == LC_OK)
    rc = ps_write(writer, bytes, sizeof(bytes), e);
  if (rc == LC_OK)
    rc = ps_publish(writer, NULL, e);
  ps_writer_close(writer);
  if (rc == LC_OK && future) {
    rc = progress_plan(&plan, future, e);
    if (rc == LC_OK)
      rc = ps_budget_resize(d->retention_budget, &plan, e);
  }
  return rc;
}
int ps_namespace_retention_work_store(ps_namespace *ns,
                                      const lc_pouch_retention_work *work,
                                      lc_error *e) {
  struct ps_namespace_data *d;
  ps_budget_plan plan = {0};
  ps_budget *budget = NULL;
  ps_writer *writer = NULL;
  ps_reader *reader = NULL;
  retention_snapshot captured;
  unsigned char *header = captured.header;
  size_t length = 0;
  unsigned int future;
  int same = 0, shared, rc;
  if (!work || work->phase < LC_POUCH_RETENTION_IDLE ||
      work->phase > LC_POUCH_RETENTION_FINISHED)
    return failure(e, LC_ERR_INVALID, "invalid retention work phase");
  if (work->phase != LC_POUCH_RETENTION_IDLE) {
    if (!lc_xid_is_valid(work->txn_id) || !work->key || !*work->key ||
        (length = strlen(work->key)) > LC_POUCH_STATE_RECORD_KEY_MAX_BYTES)
      return failure(e, LC_ERR_INVALID, "invalid retention work identity");
  }
  rc = ps_ns_data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  shared = ps_coordination_shared(ps_space_coordination(ns->root->space));
  if (shared && (rc = ps_ns_enter(d, e)) != LC_OK)
    return rc;
  pthread_mutex_lock(&d->mutex);
  rc = retention_cached(d, e);
  if (rc == LC_OK && !d->retention_present)
    rc = failure(e, LC_ERR_TRANSPORT,
                 "state retention work requires an existing discovery hint");
  if (rc == LC_OK && work->phase == LC_POUCH_RETENTION_IDLE)
    rc = clear_work(d, e);
  else if (rc == LC_OK) {
    rc = snapshot(ns, &reader, &captured, e);
    if (rc == LC_OK)
      rc = same_work(reader, header, work, length, &same, e);
    ps_reader_close(reader);
    if (rc == LC_OK && same &&
        !(captured.phase == LC_POUCH_RETENTION_FINISHED &&
          work->phase == LC_POUCH_RETENTION_ACTIVE)) {
      if (captured.phase != work->phase ||
          captured.fencing != work->fencing_token ||
          captured.version != work->state_version)
        rc = publish_progress(d, &captured.identity, work, e);
    } else if (rc == LC_OK) {
      plan.maintenance = 1;
      rc = ps_budget_plan_value(&plan, WORK_HEADER + length, e);
      future = future_progress(work->phase, work->fencing_token);
      if (rc == LC_OK && future) {
        unsigned int writes = future;
        while (rc == LC_OK && writes--)
          rc = ps_budget_plan_value(&plan, WORK_PROGRESS, e);
      }
      if (rc == LC_OK) {
        rc = ps_budget_open(ns->root->space, &plan, &budget, e);
        if (rc == LC_ERR_INVALID) {
          lc_error_cleanup(e);
          rc = ps_checkpoint(ns->root->space, e);
          if (rc == LC_OK)
            rc = ps_budget_open(ns->root->space, &plan, &budget, e);
        }
      }
      if (rc == LC_OK) {
        memset(header, 0, WORK_HEADER);
        memcpy(header, "LCPSRTWK", 8);
        put64(header + 8, ns->identity.owner);
        put64(header + 16, work->fencing_token);
        put64(header + 24, work->state_version);
        put64(header + 32, length);
        memcpy(header + 40, work->txn_id, LC_XID_STRING_LENGTH);
        header[60] = (unsigned char)work->phase;
        put64(header + 64, (uint64_t)crc32(0L, (const unsigned char *)work->key,
                                           (uInt)length));
        put64(header + 72, (uint64_t)crc32(0L, header, 72));
        rc = ps_begin(ns->root->space, budget, control(ns, 3), &writer, e);
      }
      if (rc == LC_OK)
        rc = ps_write(writer, header, WORK_HEADER, e);
      if (rc == LC_OK)
        rc = ps_write(writer, work->key, length, e);
      if (rc == LC_OK)
        rc = ps_publish(writer, NULL, e);
      ps_writer_close(writer);
      if (rc == LC_OK) {
        (void)ps_budget_close(d->retention_budget, NULL);
        d->retention_budget = budget;
        budget = NULL;
        if (work->phase == LC_POUCH_RETENTION_ACTIVE) {
          rc = progress_plan(&plan, future, e);
          if (rc == LC_OK)
            rc = ps_budget_resize(d->retention_budget, &plan, e);
        }
      }
      (void)ps_budget_close(budget, NULL);
    }
    if (rc == LC_OK && work->phase == LC_POUCH_RETENTION_FINISHED) {
      (void)ps_budget_close(d->retention_budget, NULL);
      d->retention_budget = NULL;
    }
  }
  pthread_mutex_unlock(&d->mutex);
  if (shared)
    ps_ns_leave(d);
  return rc;
}
int ps_namespace_retention_hint_remove(ps_namespace *ns, lc_error *e) {
  struct ps_namespace_data *d;
  int shared, rc = ps_ns_data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  shared = ps_coordination_shared(ps_space_coordination(ns->root->space));
  if (shared && (rc = ps_ns_enter(d, e)) != LC_OK)
    return rc;
  pthread_mutex_lock(&d->mutex);
  /* Retire identity before discovery. Stale progress only names its old
   * incarnation and cannot turn a later active identity into finished work. */
  rc = clear_work(d, e);
  if (rc == LC_OK)
    rc = delete_present(ns, WORK_PROGRESS_SLOT, e);
  if (rc == LC_OK)
    rc = delete_present(ns, 2, e);
  if (rc == LC_OK) {
    d->retention_loaded = 1;
    d->retention_present = 0;
  }
  pthread_mutex_unlock(&d->mutex);
  if (shared)
    ps_ns_leave(d);
  return rc;
}
