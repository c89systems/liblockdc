#include "query_index.h"
#include "budget.h"
#include "lc_api_internal.h"
#include "root_internal.h"
#include <string.h>

typedef struct page_input {
  ps_namespace *ns;
  ps_reader *reader;
} page_input;

static ps_key page_key(uint64_t collection, uint64_t ordinal) {
  ps_key key = {0, 0, PS_COLLECTION_QUERY};
  key.owner = collection;
  key.key = ordinal;
  return key;
}
static int page_read(void *context, uint64_t offset, const struct iovec *parts,
                     int count, lc_error *e) {
  ps_reader *reader = context;
  int i, rc = LC_OK;
  for (i = 0; rc == LC_OK && i < count; ++i) {
    rc = ps_read_at(reader, offset, parts[i].iov_base, parts[i].iov_len, e);
    offset += parts[i].iov_len;
  }
  return rc;
}
static const lc_pouch_record_io_ops page_ops = {page_read, NULL, NULL, NULL,
                                                NULL};

int ps_query_page_open(ps_namespace *ns, uint64_t collection, uint64_t ordinal,
                       lc_pouch_index_input *out, lc_error *e) {
  ps_reader *reader = NULL;
  page_input *input;
  ps_ref ref;
  int rc;
  memset(out, 0, sizeof(*out));
  rc = ps_read_open(ns->root->space, page_key(collection, ordinal), NULL,
                    &reader, e);
  if (rc == LC_ERR_INVALID) {
    lc_error_cleanup(e);
    return LC_OK;
  }
  if (rc != LC_OK)
    return rc;
  input = lc_alloc_with_allocator(&ns->root->crypto_allocator, sizeof(*input));
  if (!input) {
    ps_reader_close(reader);
    return lc_error_set(e, LC_ERR_NOMEM, 0L,
                        "cannot retain collection index page", NULL, NULL,
                        "pouch");
  }
  ps_namespace_retain(ns);
  input->ns = ns;
  input->reader = reader;
  ps_read_reference(reader, &ref);
  out->signature.present = 1;
  out->signature.size = out->bytes = ps_read_size(reader);
  out->signature.inode = (unsigned long)collection;
  out->signature.mtime = (unsigned long)ref.incarnation;
  out->io.ops = &page_ops;
  out->io.context = reader;
  out->handle = input;
  return LC_OK;
}
void ps_query_page_close(lc_pouch_index_input *captured) {
  page_input *input = captured->handle;
  ps_namespace *ns;
  if (!input)
    return;
  ns = input->ns;
  ps_reader_close(input->reader);
  lc_free_with_allocator(&ns->root->crypto_allocator, input);
  memset(captured, 0, sizeof(*captured));
  ps_namespace_release(ns);
}

typedef struct page_output {
  ps_writer *writer;
  uint64_t remaining;
} page_output;
static int page_write(lc_sink *sink, const void *bytes, size_t length,
                      lc_error *e) {
  page_output *output = sink->impl;
  if (length > output->remaining) {
    lc_error_set(e, LC_ERR_INVALID, 0L,
                 "collection index page exceeded admission", NULL, NULL,
                 "pouch");
    return 0;
  }
  if (ps_write(output->writer, bytes, length, e) != LC_OK)
    return 0;
  output->remaining -= length;
  return 1;
}
static int publish_page(void *context, lc_error *e) {
  return ps_publish(context, NULL, e);
}
int ps_query_page_replace(ps_namespace *ns, uint64_t collection,
                          uint64_t ordinal, uint64_t bound,
                          lc_pouch_index_produce_fn produce, void *producer,
                          lc_pouch_index_select_fn select, void *selector,
                          lc_error *e) {
  ps_space *space = ns->root->space;
  ps_budget_plan plan = {0};
  ps_budget *budget = NULL;
  page_output output = {0};
  lc_sink sink = {0};
  int rc = ps_budget_plan_value(&plan, bound, e);
  if (rc == LC_OK)
    rc = ps_budget_open(space, &plan, &budget, e);
  if (rc == LC_OK)
    rc = ps_begin(space, budget, page_key(collection, ordinal), &output.writer,
                  e);
  if (rc == LC_OK) {
    sink.impl = &output;
    sink.write = page_write;
    output.remaining = bound;
    rc = produce(producer, &sink, e);
  }
  if (rc == LC_OK)
    rc = select ? select(selector, publish_page, output.writer, e)
                : publish_page(output.writer, e);
  ps_writer_close(output.writer);
  (void)ps_budget_close(budget, NULL);
  return rc;
}

/* Unlink old projection streams. The root orphan pass retires their bytes
 * after outstanding captures release the target. */
typedef struct query_reclamation {
  ps_space *space;
  uint64_t owner;
  unsigned long *removed;
} query_reclamation;
static int reclaim_artifact(void *context, const void *name, size_t length,
                            const ps_catalog_info *info, lc_error *e) {
  query_reclamation *p = context;
  int rc = ps_catalog_remove(p->space, p->owner, PS_CATALOG_PROJECTION, name,
                             length, &info->revision, e);
  if (rc == LC_OK && p->removed)
    ++*p->removed;
  return rc;
}
int ps_query_catalog_reclaim(ps_space *space, uint64_t owner,
                             unsigned long *removed, lc_error *e) {
  query_reclamation p;
  p.space = space;
  p.owner = owner;
  p.removed = removed;
  return ps_catalog_visit(space, owner, PS_CATALOG_PROJECTION, reclaim_artifact,
                          &p, e);
}
int ps_query_artifacts_reclaim(ps_space *space, uint64_t owner, lc_error *e) {
  int rc = ps_query_catalog_reclaim(space, owner, NULL, e);
  return rc == LC_OK ? ps_query_values_reclaim(space, owner, e) : rc;
}
