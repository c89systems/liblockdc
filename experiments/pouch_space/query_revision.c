#include "budget.h"
#include "lc_api_internal.h"
#include "namespace_work.h"
#include "query_index.h"
#include "revision.h"
#include "root_internal.h"
#include <string.h>

typedef struct value_input {
  ps_namespace *ns;
  ps_reader *reader;
} value_input;
typedef struct value_output {
  lc_sink sink;
  ps_writer *writer;
  uint64_t maximum, written;
} value_output;
typedef struct query_revision {
  ps_namespace *ns;
  ps_budget *budget;
  ps_writer *writers[2];
  uint64_t manifest_bound;
} query_revision;
static int failure(lc_error *e, int code, const char *message) {
  return lc_error_set(e, code, 0L, message, NULL, NULL, "pouch");
}
static int namespace_get(ps_root *root, const char *name, int create,
                         ps_namespace **out, lc_error *e) {
  lc_error local;
  int rc;
  *out = NULL;
  lc_error_init(&local);
  rc = ps_root_namespace_acquire(root, name, create, out, &local);
  if (!create && rc == LC_ERR_INVALID && local.server_code &&
      !strcmp(local.server_code, "pouch_namespace_not_found"))
    rc = LC_OK;
  else if (rc != LC_OK)
    rc = lc_error_set(e, local.code, local.http_status, local.message,
                      local.detail, local.server_code, local.correlation_id);
  lc_error_cleanup(&local);
  return rc;
}
static int artifact_key(uint64_t owner, const lc_pouch_index_artifact_id *id,
                        ps_key *out, lc_error *e) {
  if (!id || id->kind >= LC_POUCH_INDEX_ARTIFACT_KINDS ||
      (id->segmented && id->kind == LC_POUCH_INDEX_MANIFEST) ||
      (!id->segmented && id->generation))
    return failure(e, LC_ERR_INVALID, "invalid query-index value identity");
  out->owner = owner;
  out->key = id->generation;
  out->kind = PS_QUERY_FIRST + 2 * id->kind + (id->segmented ? 1U : 0U);
  if (out->kind > PS_QUERY_LAST)
    return failure(e, LC_ERR_INVALID,
                   "query-index domain exceeds physical kinds");
  return LC_OK;
}
static void signature(ps_key key, const ps_ref *ref, uint64_t bytes,
                      lc_pouch_index_signature *out) {
  memset(out, 0, sizeof(*out));
  out->present = 1;
  out->size = bytes;
  out->inode = (unsigned long)key.owner;
  out->mtime = (unsigned long)ref->incarnation;
}
static int input_read(void *context, uint64_t offset, const struct iovec *parts,
                      int count, lc_error *e) {
  value_input *input = context;
  int i, rc = LC_OK;
  for (i = 0; rc == LC_OK && i < count; ++i) {
    rc = ps_read_at(input->reader, offset, parts[i].iov_base, parts[i].iov_len,
                    e);
    offset += parts[i].iov_len;
  }
  return rc;
}
static const lc_pouch_record_io_ops input_ops = {input_read, NULL, NULL, NULL,
                                                 NULL};
static int query_open(void *context, const char *name,
                      const lc_pouch_index_artifact_id *id,
                      lc_pouch_index_input *out, lc_error *e) {
  ps_root *root = context;
  ps_namespace *ns = NULL;
  ps_reader *reader = NULL;
  value_input *input = NULL;
  ps_key key;
  ps_ref ref;
  int rc = artifact_key(0, id, &key, e);
  memset(out, 0, sizeof(*out));
  if (rc == LC_OK)
    rc = namespace_get(root, name, 0, &ns, e);
  if (rc == LC_OK && ns) {
    key.owner = ns->identity.owner;
    rc = ps_read_open(root->space, key, NULL, &reader, e);
    if (rc == LC_ERR_INVALID) {
      lc_error_cleanup(e);
      rc = LC_OK;
    }
  }
  if (rc == LC_OK && reader) {
    input =
        lc_calloc_with_allocator(&root->crypto_allocator, 1, sizeof(*input));
    if (!input)
      rc = failure(e, LC_ERR_NOMEM, "cannot capture query-index value");
  }
  if (rc == LC_OK && input) {
    input->reader = reader;
    input->ns = ns;
    out->handle = input;
    out->bytes = ps_read_size(reader);
    out->io.ops = &input_ops;
    out->io.context = input;
    ps_read_reference(reader, &ref);
    signature(key, &ref, out->bytes, &out->signature);
    reader = NULL;
    ns = NULL;
  }
  ps_reader_close(reader);
  ps_namespace_release(ns);
  return rc;
}
static void query_close(void *context, lc_pouch_index_input *captured) {
  value_input *input = captured->handle;
  ps_namespace *ns;
  (void)context;
  if (!input)
    return;
  ns = input->ns;
  ps_reader_close(input->reader);
  lc_free_with_allocator(&ns->root->crypto_allocator, input);
  memset(captured, 0, sizeof(*captured));
  ps_namespace_release(ns);
}
static int query_signature(void *context, const char *name,
                           const lc_pouch_index_artifact_id *id,
                           lc_pouch_index_signature *out, lc_error *e) {
  ps_root *root = context;
  ps_namespace *ns = NULL;
  ps_key key;
  ps_ref ref;
  uint64_t bytes;
  int present, valid = 0, rc = artifact_key(0, id, &key, e);
  memset(out, 0, sizeof(*out));
  if (rc == LC_OK)
    rc = namespace_get(root, name, 0, &ns, e);
  if (rc == LC_OK && ns) {
    key.owner = ns->identity.owner;
    rc = ps_value_info(root->space, key, &ref, &bytes, &present, e);
    if (rc == LC_OK && present)
      rc = ps_value_validate(root->space, key, &ref, &valid, e);
    if (rc == LC_OK && present && valid)
      signature(key, &ref, bytes, out);
  }
  ps_namespace_release(ns);
  return rc;
}
static int output_write(lc_sink *sink, const void *bytes, size_t length,
                        lc_error *e) {
  value_output *out = sink->impl;
  int rc;
  if (length > out->maximum - out->written) {
    failure(e, LC_ERR_INVALID, "query-index producer exceeds admitted bound");
    return 0;
  }
  rc = ps_write(out->writer, bytes, length, e);
  if (rc == LC_OK)
    out->written += length;
  return rc == LC_OK;
}
static int produce_value(ps_writer *writer, uint64_t bound,
                         lc_pouch_index_produce_fn produce, void *producer,
                         lc_error *e) {
  value_output out;
  memset(&out, 0, sizeof(out));
  out.sink.impl = &out;
  out.sink.write = output_write;
  out.writer = writer;
  out.maximum = bound;
  return produce(producer, &out.sink, e);
}
static int query_replace(void *context, const char *name,
                         const lc_pouch_index_artifact_id *id, uint64_t bound,
                         lc_pouch_index_produce_fn produce, void *producer,
                         int unpublished, int durable, lc_error *e) {
  ps_root *root = context;
  ps_namespace *ns = NULL;
  ps_key key;
  ps_budget_plan plan = {0};
  ps_budget *budget = NULL;
  ps_writer *writer = NULL;
  int rc = artifact_key(0, id, &key, e);
  (void)unpublished;
  if (rc == LC_OK)
    rc = namespace_get(root, name, 1, &ns, e);
  if (rc == LC_OK)
    rc = ps_namespace_work_ensure(ns, LC_POUCH_STORAGE_WORK_ORDINARY, 0, e);
  if (rc == LC_OK)
    rc = ps_budget_plan_value(&plan, bound, e);
  if (rc == LC_OK)
    rc = ps_budget_open(root->space, &plan, &budget, e);
  if (rc == LC_OK) {
    key.owner = ns->identity.owner;
    rc = ps_begin(root->space, budget, key, &writer, e);
  }
  if (rc == LC_OK)
    rc = produce_value(writer, bound, produce, producer, e);
  if (rc == LC_OK)
    rc = ps_revision_publish(&writer, 1, durable, PS_COMMIT_DEFAULT, NULL, NULL,
                             e);
  ps_writer_close(writer);
  (void)ps_budget_close(budget, NULL);
  ps_namespace_release(ns);
  return rc;
}
static int query_remove(void *context, const char *name,
                        const lc_pouch_index_artifact_id *id, lc_error *e) {
  ps_root *root = context;
  ps_namespace *ns = NULL;
  ps_key key;
  int rc = artifact_key(0, id, &key, e);
  if (rc == LC_OK)
    rc = namespace_get(root, name, 0, &ns, e);
  if (rc == LC_OK && ns)
    rc = ps_namespace_work_ensure(ns, LC_POUCH_STORAGE_WORK_ORDINARY, 0, e);
  if (rc == LC_OK && ns) {
    key.owner = ns->identity.owner;
    rc = ps_value_remove(root->space, key, NULL, e);
  }
  ps_namespace_release(ns);
  return rc;
}
typedef struct prune_context {
  ps_space *space;
  const uint64_t *keep;
  size_t count;
} prune_context;
static int prune_value(void *context, ps_key key, const ps_ref *ref,
                       uint64_t bytes, lc_error *e) {
  prune_context *p = context;
  size_t i;
  (void)bytes;
  if (p->keep) {
    if (key.kind == PS_QUERY_FIRST && !key.key)
      return LC_OK;
    if ((key.kind - PS_QUERY_FIRST) % 2)
      for (i = 0; i < p->count; ++i)
        if (p->keep[i] == key.key)
          return LC_OK;
  }
  return ps_value_remove(p->space, key, ref, e);
}
static int query_prune(void *context, const char *name, const uint64_t *keep,
                       size_t count, lc_error *e) {
  ps_root *root = context;
  ps_namespace *ns = NULL;
  uint64_t empty_keep = 0;
  prune_context p;
  int rc = namespace_get(root, name, 0, &ns, e);
  p.space = root->space;
  p.keep = keep ? keep : &empty_keep;
  p.count = count;
  if (rc == LC_OK && ns)
    rc = ps_values_visit(root->space, ns->identity.owner, PS_QUERY_FIRST,
                         PS_QUERY_LAST, prune_value, &p, e);
  ps_namespace_release(ns);
  return rc;
}
int ps_query_values_reclaim(ps_space *space, uint64_t owner, lc_error *e) {
  prune_context p;
  p.space = space;
  p.keep = NULL;
  p.count = 0;
  return ps_values_visit(space, owner, PS_QUERY_FIRST, PS_QUERY_LAST,
                         prune_value, &p, e);
}
static void query_dispose(void *context, void *build) {
  query_revision *revision = build;
  ps_namespace *ns;
  (void)context;
  if (!revision)
    return;
  ns = revision->ns;
  ps_writer_close(revision->writers[0]);
  ps_writer_close(revision->writers[1]);
  (void)ps_budget_close(revision->budget, NULL);
  lc_free_with_allocator(&ns->root->crypto_allocator, revision);
  ps_namespace_release(ns);
}
static int query_prepare(void *context, const char *name,
                         const lc_pouch_index_artifact_id *id, uint64_t bound,
                         lc_pouch_index_produce_fn produce, void *producer,
                         int unpublished, uint64_t manifest_bound, void **build,
                         lc_pouch_index_signature *out, lc_error *e) {
  ps_root *root = context;
  ps_namespace *ns = NULL;
  query_revision *revision = NULL;
  ps_budget_plan plan = {0};
  ps_key key;
  ps_ref ref;
  uint64_t bytes;
  int rc = artifact_key(0, id, &key, e);
  *build = NULL;
  (void)unpublished;
  if (rc == LC_OK)
    rc = namespace_get(root, name, 1, &ns, e);
  if (rc == LC_OK) {
    revision =
        lc_calloc_with_allocator(&root->crypto_allocator, 1, sizeof(*revision));
    if (!revision)
      rc = failure(e, LC_ERR_NOMEM, "cannot prepare query-index revision");
  }
  if (rc == LC_OK) {
    revision->ns = ns;
    revision->manifest_bound = manifest_bound;
    *build = revision;
    ns = NULL;
    key.owner = revision->ns->identity.owner;
  }
  if (rc == LC_OK)
    rc = ps_budget_plan_value(&plan, bound, e);
  if (rc == LC_OK)
    rc = ps_budget_plan_value(&plan, manifest_bound, e);
  if (rc == LC_OK)
    rc = ps_budget_open(root->space, &plan, &revision->budget, e);
  if (rc == LC_OK)
    rc = ps_begin(root->space, revision->budget, key, &revision->writers[0], e);
  if (rc == LC_OK)
    rc = produce_value(revision->writers[0], bound, produce, producer, e);
  if (rc == LC_OK)
    rc = ps_revision_writer_info(revision->writers[0], &ref, &bytes, e);
  if (rc == LC_OK)
    signature(key, &ref, bytes, out);
  ps_namespace_release(ns);
  return rc;
}
static int query_publish(void *context, void *build, const char *name,
                         const lc_pouch_index_artifact_id *id, uint64_t bound,
                         lc_pouch_index_produce_fn produce, void *producer,
                         lc_error *e) {
  query_revision *revision = build;
  ps_root *root = context;
  ps_key key;
  int rc, work_pending;
  if (!revision || strcmp(name, revision->ns->name) ||
      id->kind != LC_POUCH_INDEX_MANIFEST || bound > revision->manifest_bound)
    return failure(e, LC_ERR_INVALID, "query manifest exceeds its revision");
  rc = artifact_key(revision->ns->identity.owner, id, &key, e);
  if (rc == LC_OK)
    rc = ps_begin(root->space, revision->budget, key, &revision->writers[1], e);
  if (rc == LC_OK)
    rc = produce_value(revision->writers[1], bound, produce, producer, e);
  if (rc == LC_OK)
    rc = ps_namespace_work_stage_ordinary(revision->ns, 0, &work_pending, e);
  if (rc == LC_OK)
    /* A completed index flush has always stabilized its canonical snapshot,
     * including on NoSync roots. Preserve that boundary with one shared
     * barrier for generation, manifest, and any first maintenance hint. */
    rc = ps_revision_publish(revision->writers, 2, PS_REVISION_FORCE_DURABLE,
                             PS_COMMIT_DEFAULT, NULL, NULL, e);
  if (rc == LC_OK && work_pending)
    rc = ps_namespace_work_complete_ordinary(revision->ns, e);
  return rc;
}
static int query_acquire(void *context, const char *name, void **out,
                         lc_error *e) {
  ps_namespace *ns = NULL;
  int locked, rc = namespace_get(context, name, 1, &ns, e);
  *out = NULL;
  if (rc == LC_OK) {
    locked = pthread_mutex_lock(&ns->query);
    if (locked)
      rc = failure(e, LC_ERR_TRANSPORT,
                   "cannot acquire namespace query authority");
    else {
      *out = ns;
      ns = NULL;
    }
  }
  ps_namespace_release(ns);
  return rc;
}
static void query_release(void *context, void *guard) {
  ps_namespace *ns = guard;
  (void)context;
  pthread_mutex_unlock(&ns->query);
  ps_namespace_release(ns);
}
const lc_pouch_index_store_ops ps_query_index_store_ops = {
    query_open,      query_close,   query_replace, query_remove,
    query_signature, query_prune,   query_prepare, query_publish,
    query_dispose,   query_acquire, query_release};
