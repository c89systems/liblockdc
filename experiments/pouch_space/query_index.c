#include "query_index.h"
#include "budget.h"
#include "lc_api_internal.h"
#include "namespace_work.h"
#include "owned.h"
#include "root_internal.h"
#include "stream.h"
#include <string.h>

#define QUERY_STREAM_KEY 1U
#define QUERY_ID_BYTES 10U
typedef struct ps_query_index_generation {
  ps_stream *stream;
  ps_catalog_info binding;
  size_t users;
  struct ps_query_index_generation *next;
} ps_query_index_generation;
typedef struct query_input {
  ps_namespace *ns;
  ps_query_index_generation *generation;
  ps_stream_window *window;
} query_input;
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
static int encode_id(const lc_pouch_index_artifact_id *id,
                     unsigned char bytes[QUERY_ID_BYTES], lc_error *e) {
  size_t i;
  if (!id || id->kind >= LC_POUCH_INDEX_ARTIFACT_KINDS ||
      (id->segmented && id->kind == LC_POUCH_INDEX_MANIFEST) ||
      (!id->segmented && id->generation))
    return failure(e, LC_ERR_INVALID, "invalid query-index catalog identity");
  bytes[0] = id->segmented ? 1U : 0U;
  bytes[1] = (unsigned char)id->kind;
  for (i = 0U; i < 8U; ++i)
    bytes[2U + i] = (unsigned char)(id->generation >> (8U * i));
  return LC_OK;
}
static int scoped_binding(ps_space *space, uint64_t owner, const void *name,
                          size_t length, ps_catalog_view **out,
                          ps_catalog_info *info, lc_error *e) {
  int rc;
  *out = NULL;
  rc =
      ps_catalog_get(space, owner, PS_CATALOG_PROJECTION, name, length, out, e);
  if (rc == LC_OK && *out)
    rc = ps_catalog_view_info(*out, info, e);
  if (rc == LC_OK && *out &&
      (info->stream.kind != PS_STREAM || !info->stream.owner ||
       info->stream.key != QUERY_STREAM_KEY))
    rc = failure(e, LC_ERR_PROTOCOL, "query-index stream binding is invalid");
  return rc;
}
static int binding(ps_namespace *ns, const lc_pouch_index_artifact_id *id,
                   ps_catalog_view **out, ps_catalog_info *info, lc_error *e) {
  unsigned char name[QUERY_ID_BYTES];
  int rc = encode_id(id, name, e);
  *out = NULL;
  return rc == LC_OK ? scoped_binding(ns->root->space, ns->identity.owner, name,
                                      sizeof(name), out, info, e)
                     : rc;
}
static int inspect(ps_root *root, const ps_catalog_info *info,
                   lc_pouch_index_signature *signature, lc_error *e) {
  ps_owned_status status;
  int rc =
      ps_owned_inspect(root->space, info->stream, &info->target, &status, e);
  if (rc == LC_OK && (!status.sealed || status.retired))
    rc = failure(e, LC_ERR_PROTOCOL, "query-index artifact is not sealed");
  if (rc == LC_OK) {
    memset(signature, 0, sizeof(*signature));
    signature->size = status.tail;
    signature->inode = (unsigned long)info->stream.owner;
    signature->mtime = (unsigned long)status.ref.incarnation;
    signature->present = 1;
  }
  return rc;
}
static int query_signature(void *context, const char *name,
                           const lc_pouch_index_artifact_id *id,
                           lc_pouch_index_signature *out, lc_error *e) {
  ps_root *root = context;
  ps_namespace *ns = NULL;
  ps_catalog_view *view = NULL;
  ps_catalog_info info;
  int rc;
  memset(out, 0, sizeof(*out));
  rc = namespace_get(root, name, 0, &ns, e);
  if (rc == LC_OK && ns)
    rc = binding(ns, id, &view, &info, e);
  if (rc == LC_OK && view)
    rc = inspect(root, &info, out, e);
  ps_catalog_view_close(view);
  ps_namespace_release(ns);
  return rc;
}
/* Root registry authority spans only sealed-head/run metadata, never payload
 * I/O, durability waits or a producer. Separate input windows share this one
 * physical stream owner; each retains its own immutable read range. */
static void generation_release_locked(ps_namespace *ns,
                                      ps_query_index_generation *generation) {
  ps_query_index_generation **link;
  if (--generation->users)
    return;
  for (link = &ns->query_readers; *link != generation; link = &(*link)->next) {
  }
  *link = generation->next;
  (void)ps_stream_close(generation->stream, NULL);
  lc_free_with_allocator(&ns->root->crypto_allocator, generation);
}
static int input_read(void *context, uint64_t offset, const struct iovec *parts,
                      int count, lc_error *e) {
  ps_stream_window *window = context;
  int i, rc = LC_OK;
  for (i = 0; rc == LC_OK && i < count; ++i) {
    rc = ps_stream_window_read(window, offset, parts[i].iov_base,
                               parts[i].iov_len, e);
    offset += parts[i].iov_len;
  }
  return rc;
}
static const lc_pouch_record_io_ops input_ops = {input_read, NULL, NULL, NULL,
                                                 NULL};
int ps_query_artifact_open(ps_namespace *ns, uint64_t owner, const void *name,
                           size_t length, lc_pouch_index_input *out,
                           lc_error *e) {
  ps_root *root = ns->root;
  ps_catalog_view *view = NULL;
  ps_catalog_info info;
  ps_query_index_generation *generation = NULL;
  query_input *input = NULL;
  int rc;
  memset(out, 0, sizeof(*out));
  ps_namespace_retain(ns);
  rc = scoped_binding(root->space, owner, name, length, &view, &info, e);
  if (rc == LC_OK && view)
    rc = inspect(root, &info, &out->signature, e);
  if (rc == LC_OK && view) {
    input =
        lc_calloc_with_allocator(&root->crypto_allocator, 1U, sizeof(*input));
    if (!input)
      rc = failure(e, LC_ERR_NOMEM, "cannot allocate query-index input pin");
  }
  if (rc == LC_OK && input) {
    pthread_mutex_lock(&root->mutex);
    for (generation = ns->query_readers; generation;
         generation = generation->next)
      if (generation->binding.stream.owner == info.stream.owner &&
          generation->binding.target.incarnation == info.target.incarnation)
        break;
    if (!generation) {
      generation = lc_calloc_with_allocator(&root->crypto_allocator, 1U,
                                            sizeof(*generation));
      if (!generation)
        rc = failure(e, LC_ERR_NOMEM,
                     "cannot allocate query-index generation pin");
      if (rc == LC_OK)
        rc = ps_stream_open(root->space, NULL, info.stream, 0, &info.target,
                            NULL, NULL, &generation->stream, e);
      if (rc == LC_OK) {
        generation->binding = info;
        generation->next = ns->query_readers;
        ns->query_readers = generation;
      } else {
        lc_free_with_allocator(&root->crypto_allocator, generation);
        generation = NULL;
      }
    }
    if (rc == LC_OK) {
      ++generation->users;
      rc = ps_stream_window_open(generation->stream, 0U, out->signature.size,
                                 &input->window, e);
      if (rc != LC_OK)
        generation_release_locked(ns, generation);
    }
    pthread_mutex_unlock(&root->mutex);
    if (rc == LC_OK) {
      input->ns = ns;
      input->generation = generation;
      out->handle = input;
      out->bytes = out->signature.size;
      out->io.ops = &input_ops;
      out->io.context = input->window;
      ns = NULL;
      input = NULL;
    }
  }
  if (rc == LC_ERR_INVALID && view) {
    ps_catalog_view *current = NULL;
    ps_catalog_info selected;
    lc_error retry;
    int probed;
    /* A captured catalog view protects bytes, but a concurrent removal may
     * admit their retirement owner before this input opens its stream. Once
     * that selection has changed, derived metadata is an ordinary miss. A
     * still-selected invalid target and allocation/transport errors remain
     * explicit errors. Never wait for retirement under registry authority. */
    lc_error_init(&retry);
    probed = scoped_binding(root->space, owner, name, length, &current,
                            &selected, &retry);
    if (probed != LC_OK) {
      rc = probed;
      if (e) {
        lc_error_cleanup(e);
        *e = retry;
        memset(&retry, 0, sizeof(retry));
      }
    } else if (!current ||
               selected.revision.incarnation != info.revision.incarnation ||
               memcmp(selected.revision.container, info.revision.container,
                      sizeof(info.revision.container))) {
      lc_error_cleanup(e);
      memset(out, 0, sizeof(*out));
      rc = LC_OK;
    }
    ps_catalog_view_close(current);
    lc_error_cleanup(&retry);
  }
  ps_catalog_view_close(view);
  ps_namespace_release(ns);
  lc_free_with_allocator(&root->crypto_allocator, input);
  if (rc != LC_OK)
    memset(out, 0, sizeof(*out));
  return rc;
}
static int query_open(void *context, const char *name,
                      const lc_pouch_index_artifact_id *id,
                      lc_pouch_index_input *out, lc_error *e) {
  ps_namespace *ns = NULL;
  unsigned char encoded[QUERY_ID_BYTES];
  int rc = encode_id(id, encoded, e);
  memset(out, 0, sizeof(*out));
  if (rc == LC_OK)
    rc = namespace_get(context, name, 0, &ns, e);
  if (rc == LC_OK && ns)
    rc = ps_query_artifact_open(ns, ns->identity.owner, encoded,
                                sizeof(encoded), out, e);
  ps_namespace_release(ns);
  return rc;
}
void ps_query_artifact_close(ps_root *root, lc_pouch_index_input *captured) {
  query_input *input = captured->handle;
  if (!input)
    return;
  ps_stream_window_close(input->window);
  pthread_mutex_lock(&root->mutex);
  generation_release_locked(input->ns, input->generation);
  pthread_mutex_unlock(&root->mutex);
  ps_namespace_release(input->ns);
  lc_free_with_allocator(&root->crypto_allocator, input);
  memset(captured, 0, sizeof(*captured));
}
static void query_close(void *context, lc_pouch_index_input *captured) {
  ps_query_artifact_close(context, captured);
}
static void retire_unbound(ps_space *space, ps_key key, const ps_ref *ref) {
  ps_owned_head *head = NULL;
  ps_owned_status status;
  lc_error ignored;
  int rc;
  lc_error_init(&ignored);
  rc = ps_owned_open(space, NULL, key, 0, ref, &head, &status, &ignored);
  if (rc == LC_OK)
    rc = ps_owned_claim_orphan(head, &ignored);
  if (rc == LC_OK)
    (void)ps_owned_retire(head, &ignored);
  ps_owned_close(head);
  lc_error_cleanup(&ignored);
}
typedef struct query_output {
  lc_sink sink;
  ps_stream_writer *writer;
  uint64_t maximum, written;
} query_output;
static int output_write(lc_sink *sink, const void *bytes, size_t length,
                        lc_error *e) {
  query_output *output = sink->impl;
  int rc;
  if (length > output->maximum - output->written) {
    failure(e, LC_ERR_INVALID, "query-index producer exceeded admission");
    return 0;
  }
  rc = ps_stream_write(output->writer, bytes, length, e);
  if (rc == LC_OK)
    output->written += length;
  return rc == LC_OK;
}
typedef struct query_selection {
  ps_space *space;
  ps_budget *budget;
  uint64_t owner;
  const void *name;
  size_t length;
  ps_catalog_view *old;
  ps_catalog_info prior;
  ps_key key;
  ps_ref ref;
  int published;
} query_selection;
static int select_artifact(void *context, lc_error *e) {
  query_selection *p = context;
  ps_catalog_info selected;
  int rc = scoped_binding(p->space, p->owner, p->name, p->length, &p->old,
                          &p->prior, e);
  if (rc == LC_OK)
    rc = ps_catalog_put(p->space, p->budget, p->owner, PS_CATALOG_PROJECTION,
                        p->name, p->length, p->old ? &p->prior.revision : NULL,
                        p->key, &p->ref, &selected, e);
  p->published = rc == LC_OK;
  return rc;
}
int ps_query_artifact_replace(ps_namespace *ns, uint64_t owner,
                              const void *name, size_t length, uint64_t bound,
                              lc_pouch_index_produce_fn produce, void *producer,
                              lc_pouch_index_select_fn select, void *selector,
                              lc_error *e) {
  ps_root *root = ns->root;
  ps_stream *stream = NULL;
  ps_query_index_generation *generation = NULL;
  ps_stream_info status;
  ps_stream_writer *writer = NULL;
  ps_budget_plan plan;
  query_output output;
  query_selection selection;
  int rc, created = 0;
  uint64_t stream_owner = 0U;
  memset(&plan, 0, sizeof(plan));
  memset(&status, 0, sizeof(status));
  memset(&selection, 0, sizeof(selection));
  selection.space = root->space;
  selection.owner = owner;
  selection.name = name;
  selection.length = length;
  rc = ps_namespace_work_ensure(ns, LC_POUCH_STORAGE_WORK_ORDINARY, e);
  if (rc == LC_OK)
    rc = ps_budget_plan_stream(&plan, e);
  if (rc == LC_OK && bound)
    rc = ps_budget_plan_span(&plan, bound, e);
  if (rc == LC_OK)
    rc = ps_budget_plan_catalog(&plan, length, e);
  if (rc == LC_OK)
    rc = ps_budget_open(root->space, &plan, &selection.budget, e);
  if (rc == LC_OK)
    rc = ps_catalog_allocate_id(root->space, &stream_owner, e);
  selection.key.owner = stream_owner;
  selection.key.key = QUERY_STREAM_KEY;
  selection.key.kind = PS_STREAM;
  if (rc == LC_OK) {
    rc = ps_stream_open(root->space, selection.budget, selection.key, 1, NULL,
                        NULL, NULL, &stream, e);
    created = rc == LC_OK;
  }
  if (rc == LC_OK)
    rc = ps_stream_status(stream, &status, e);
  selection.ref = status.ref;
  if (rc == LC_OK)
    rc = ps_stream_begin(stream, &writer, e);
  if (rc == LC_OK && bound)
    rc = ps_stream_reserve_exact(writer, selection.budget, bound, e);
  if (rc == LC_OK) {
    memset(&output, 0, sizeof(output));
    output.sink.write = output_write;
    output.sink.impl = &output;
    output.writer = writer;
    output.maximum = bound;
    rc = produce(producer, &output.sink, e);
  }
  if (rc == LC_OK)
    rc = ps_stream_commit(writer, PS_COMMIT_DEFER_SYNC, NULL, NULL, e);
  ps_stream_writer_close(writer);
  if (rc == LC_OK)
    rc = ps_stream_seal(stream, e);
  if (rc == LC_OK) {
    generation = lc_calloc_with_allocator(&root->crypto_allocator, 1U,
                                          sizeof(*generation));
    if (!generation)
      rc =
          failure(e, LC_ERR_NOMEM, "cannot retain published query-index owner");
  }
  if (rc == LC_OK) {
    /* The catalog becomes readable before select returns. Share the sealed
     * physical owner first, so an immediate reader never competes with the
     * publisher's still-open private stream. The publisher retains one user
     * until selection finishes; input windows may outlive that handoff. */
    generation->stream = stream;
    generation->binding.stream = selection.key;
    generation->binding.target = status.ref;
    generation->users = 1U;
    pthread_mutex_lock(&root->mutex);
    generation->next = ns->query_readers;
    ns->query_readers = generation;
    pthread_mutex_unlock(&root->mutex);
    stream = NULL;
  }
  if (rc == LC_OK)
    rc = select ? select(selector, select_artifact, &selection, e)
                : select_artifact(&selection, e);
  if (generation) {
    pthread_mutex_lock(&root->mutex);
    generation_release_locked(ns, generation);
    pthread_mutex_unlock(&root->mutex);
  }
  (void)ps_stream_close(stream, NULL);
  (void)ps_budget_close(selection.budget, NULL);
  ps_catalog_view_close(selection.old);
  if (created && !selection.published)
    retire_unbound(root->space, selection.key, &status.ref);
  if (selection.published && selection.old)
    retire_unbound(root->space, selection.prior.stream,
                   &selection.prior.target);
  return rc;
}
static int query_replace(void *context, const char *name,
                         const lc_pouch_index_artifact_id *id, uint64_t bound,
                         lc_pouch_index_produce_fn produce, void *producer,
                         int unpublished, int durable, lc_error *e) {
  ps_namespace *ns = NULL;
  unsigned char encoded[QUERY_ID_BYTES];
  int rc = encode_id(id, encoded, e);
  (void)unpublished;
  (void)durable; /* Structural selection always waits for durability. */
  if (rc == LC_OK)
    rc = namespace_get(context, name, 1, &ns, e);
  if (rc == LC_OK)
    rc = ps_query_artifact_replace(ns, ns->identity.owner, encoded,
                                   sizeof(encoded), bound, produce, producer,
                                   NULL, NULL, e);
  ps_namespace_release(ns);
  return rc;
}
int ps_query_artifact_remove(ps_space *space, uint64_t owner, const void *name,
                             size_t length, const ps_ref *revision,
                             lc_error *e) {
  ps_catalog_view *view = NULL;
  ps_catalog_info info;
  int rc, removed = 0;
  rc = scoped_binding(space, owner, name, length, &view, &info, e);
  if (rc == LC_OK && view && revision &&
      (revision->incarnation != info.revision.incarnation ||
       memcmp(revision->container, info.revision.container,
              sizeof(revision->container))))
    rc = failure(e, LC_ERR_INVALID, "query-index cleanup binding changed");
  if (rc == LC_OK && view) {
    rc = ps_catalog_remove(space, owner, PS_CATALOG_PROJECTION, name, length,
                           &info.revision, e);
    removed = rc == LC_OK;
  }
  ps_catalog_view_close(view);
  if (removed)
    retire_unbound(space, info.stream, &info.target);
  return rc;
}
static int query_remove(void *context, const char *name,
                        const lc_pouch_index_artifact_id *id, lc_error *e) {
  ps_namespace *ns = NULL;
  unsigned char encoded[QUERY_ID_BYTES];
  int rc = encode_id(id, encoded, e);
  if (rc == LC_OK)
    rc = namespace_get(context, name, 0, &ns, e);
  if (rc == LC_OK && ns)
    rc = ps_namespace_work_ensure(ns, LC_POUCH_STORAGE_WORK_ORDINARY, e);
  if (rc == LC_OK && ns)
    rc = ps_query_artifact_remove(ns->root->space, ns->identity.owner, encoded,
                                  sizeof(encoded), NULL, e);
  ps_namespace_release(ns);
  return rc;
}
typedef struct prune_context {
  ps_root *root;
  const char *ns;
  const uint64_t *keep;
  size_t count;
} prune_context;
static int prune_artifact(void *context, const void *name, size_t length,
                          const ps_catalog_info *info, lc_error *e) {
  prune_context *p = context;
  const unsigned char *bytes = name;
  lc_pouch_index_artifact_id id;
  size_t i;
  (void)info;
  if (length != QUERY_ID_BYTES || bytes[0] > 1U ||
      bytes[1] >= LC_POUCH_INDEX_ARTIFACT_KINDS)
    return failure(e, LC_ERR_PROTOCOL,
                   "query-index catalog identity is invalid");
  memset(&id, 0, sizeof(id));
  id.segmented = bytes[0];
  id.kind = bytes[1];
  for (i = 0U; i < 8U; ++i)
    id.generation |= (uint64_t)bytes[2U + i] << (8U * i);
  if (id.kind == LC_POUCH_INDEX_MANIFEST && !id.segmented && !id.generation)
    return LC_OK;
  if (id.segmented)
    for (i = 0U; i < p->count; ++i)
      if (p->keep[i] == id.generation)
        return LC_OK;
  return query_remove(p->root, p->ns, &id, e);
}
static int query_prune(void *context, const char *name, const uint64_t *keep,
                       size_t count, lc_error *e) {
  ps_root *root = context;
  ps_namespace *ns = NULL;
  prune_context p;
  int rc = namespace_get(root, name, 0, &ns, e);
  if (rc == LC_OK && ns) {
    p.root = root;
    p.ns = name;
    p.keep = keep;
    p.count = count;
    rc = ps_catalog_visit(root->space, ns->identity.owner,
                          PS_CATALOG_PROJECTION, prune_artifact, &p, e);
  }
  ps_namespace_release(ns);
  return rc;
}
const lc_pouch_index_store_ops ps_query_index_store_ops = {
    query_open,   query_close,     query_replace,
    query_remove, query_signature, query_prune};

typedef struct query_reclamation {
  ps_space *space;
  uint64_t owner;
} query_reclamation;
static int reclaim_artifact(void *context, const void *name, size_t length,
                            const ps_catalog_info *info, lc_error *e) {
  query_reclamation *p = context;
  return ps_query_artifact_remove(p->space, p->owner, name, length,
                                  &info->revision, e);
}
int ps_query_artifacts_reclaim(ps_space *space, uint64_t owner, lc_error *e) {
  query_reclamation p;
  p.space = space;
  p.owner = owner;
  return ps_catalog_visit(space, owner, PS_CATALOG_PROJECTION, reclaim_artifact,
                          &p, e);
}
