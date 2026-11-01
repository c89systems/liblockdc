#include "lc_api_internal.h"

#include <string.h>

void lc_collection_config_init(lc_collection_config *config) {
  if (config != NULL)
    memset(config, 0, sizeof(*config));
}
void lc_collection_lease_req_init(lc_collection_lease_req *req) {
  if (req != NULL) {
    memset(req, 0, sizeof(*req));
    req->ttl_seconds = 30;
  }
}
void lc_collection_append_req_init(lc_collection_append_req *req) {
  if (req != NULL)
    memset(req, 0, sizeof(*req));
}
void lc_collection_metadata_req_init(lc_collection_metadata_req *req) {
  if (req != NULL)
    memset(req, 0, sizeof(*req));
}
void lc_collection_read_opts_init(lc_collection_read_opts *opts) {
  if (opts != NULL)
    memset(opts, 0, sizeof(*opts));
}
void lc_collection_read_res_init(lc_collection_read_res *result) {
  if (result != NULL)
    memset(result, 0, sizeof(*result));
}
void lc_collection_read_res_cleanup(lc_collection_read_res *result) {
  if (result == NULL)
    return;
  lc_free_with_allocator(NULL, result->resume_cursor);
  lc_free_with_allocator(NULL, result->checkpoint_cursor);
  memset(result, 0, sizeof(*result));
}

static int lc_collection_bad_receiver(lc_error *error) {
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "operation requires a supporting collection receiver",
                      NULL, NULL, NULL);
}

int lc_collection_acquire(lc_collection *collection,
                          const lc_collection_lease_req *req,
                          lc_collection **out, lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (collection == NULL || collection->acquire == NULL)
    return lc_collection_bad_receiver(error);
  return collection->acquire(collection, req, out, error);
}
int lc_collection_takeover(lc_collection *collection,
                           const lc_collection_lease_req *req,
                           const char *observed_lease_id, lc_collection **out,
                           lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (collection == NULL || collection->takeover == NULL)
    return lc_collection_bad_receiver(error);
  return collection->takeover(collection, req, observed_lease_id, out, error);
}
int lc_collection_keepalive(lc_collection *collection, long ttl_seconds,
                            lc_collection_lease_info *out, lc_error *error) {
  if (out != NULL)
    memset(out, 0, sizeof(*out));
  if (collection == NULL || collection->keepalive == NULL)
    return lc_collection_bad_receiver(error);
  return collection->keepalive(collection, ttl_seconds, out, error);
}
int lc_collection_release(lc_collection *collection, lc_error *error) {
  if (collection == NULL || collection->release == NULL)
    return lc_collection_bad_receiver(error);
  return collection->release(collection, error);
}

int lc_client_create_collection(lc_client *client,
                                const lc_collection_config *config,
                                lc_collection **out, lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (client == NULL || client->create_collection == NULL)
    return lc_collection_bad_receiver(error);
  return client->create_collection(client, config, out, error);
}
int lc_client_open_collection(lc_client *client,
                              const lc_collection_config *config,
                              lc_collection **out, lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (client == NULL || client->open_collection == NULL)
    return lc_collection_bad_receiver(error);
  return client->open_collection(client, config, out, error);
}
int lc_client_list_collections(lc_client *client, const char *ns,
                               lc_collection_visit_fn visitor, void *context,
                               lc_error *error) {
  if (client == NULL || client->list_collections == NULL)
    return lc_collection_bad_receiver(error);
  return client->list_collections(client, ns, visitor, context, error);
}
int lc_client_collection_capabilities(lc_client *client,
                                      lc_collection_capabilities *out,
                                      lc_error *error) {
  if (client == NULL || client->collection_capabilities == NULL)
    return lc_collection_bad_receiver(error);
  return client->collection_capabilities(client, out, error);
}
int lc_collection_describe(lc_collection *collection, lc_collection_info *out,
                           lc_error *error) {
  if (collection == NULL || collection->describe == NULL)
    return lc_collection_bad_receiver(error);
  return collection->describe(collection, out, error);
}
int lc_collection_metadata(lc_collection *collection,
                           const lc_collection_metadata_req *req,
                           lc_collection_info *out, lc_error *error) {
  if (collection == NULL || collection->metadata == NULL)
    return lc_collection_bad_receiver(error);
  return collection->metadata(collection, req, out, error);
}
int lc_collection_entry_metadata(lc_collection *collection, uint64_t position,
                                 const lc_collection_metadata_req *req,
                                 lc_collection_append_res *out,
                                 lc_error *error) {
  if (collection == NULL || collection->entry_metadata == NULL)
    return lc_collection_bad_receiver(error);
  return collection->entry_metadata(collection, position, req, out, error);
}
int lc_collection_append(lc_collection *collection,
                         const lc_collection_append_req *req, lc_source *source,
                         lc_collection_append_res *out, lc_error *error) {
  if (collection == NULL || collection->append == NULL)
    return lc_collection_bad_receiver(error);
  return collection->append(collection, req, source, out, error);
}
int lc_collection_append_begin(lc_collection *collection,
                               const lc_collection_append_req *req,
                               lc_collection_writer **out, lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (collection == NULL || collection->append_begin == NULL)
    return lc_collection_bad_receiver(error);
  return collection->append_begin(collection, req, out, error);
}
int lc_collection_append_staged(lc_collection *collection,
                                const lc_collection_append_req *req,
                                lc_source *source,
                                lc_collection_append_res *out,
                                lc_error *error) {
  if (out != NULL)
    memset(out, 0, sizeof(*out));
  if (collection == NULL || collection->append_staged == NULL)
    return lc_collection_bad_receiver(error);
  return collection->append_staged(collection, req, source, out, error);
}
int lc_collection_writer_write(lc_collection_writer *writer, const void *bytes,
                               size_t count, lc_error *error) {
  if (writer == NULL || writer->write == NULL)
    return lc_collection_bad_receiver(error);
  return writer->write(writer, bytes, count, error);
}
int lc_collection_writer_commit(lc_collection_writer *writer,
                                lc_collection_append_res *out,
                                lc_error *error) {
  if (writer == NULL || writer->commit == NULL)
    return lc_collection_bad_receiver(error);
  return writer->commit(writer, out, error);
}
int lc_collection_writer_abort(lc_collection_writer *writer, lc_error *error) {
  if (writer == NULL || writer->abort == NULL)
    return lc_collection_bad_receiver(error);
  return writer->abort(writer, error);
}
void lc_collection_writer_close(lc_collection_writer *writer) {
  if (writer != NULL && writer->close != NULL)
    writer->close(writer);
}
int lc_collection_append_status(lc_collection *collection,
                                const char *append_id,
                                lc_collection_append_status_res *out,
                                lc_error *error) {
  if (collection == NULL || collection->append_status == NULL)
    return lc_collection_bad_receiver(error);
  return collection->append_status(collection, append_id, out, error);
}
int lc_collection_read(lc_collection *collection,
                       const lc_collection_read_opts *opts, lc_sink *sink,
                       lc_collection_read_res *out, lc_error *error) {
  if (collection == NULL || collection->read == NULL)
    return lc_collection_bad_receiver(error);
  return collection->read(collection, opts, sink, out, error);
}
int lc_collection_delete_entry(lc_collection *collection, uint64_t position,
                               lc_collection_delete_res *out, lc_error *error) {
  if (collection == NULL || collection->delete_entry == NULL)
    return lc_collection_bad_receiver(error);
  return collection->delete_entry(collection, position, out, error);
}
int lc_collection_drop(lc_collection *collection, lc_error *error) {
  if (collection == NULL || collection->drop == NULL)
    return lc_collection_bad_receiver(error);
  return collection->drop(collection, error);
}
void lc_collection_close(lc_collection *collection) {
  if (collection != NULL && collection->close != NULL)
    collection->close(collection);
}

void lc_collection_edit_req_init(lc_collection_edit_req *req) {
  if (req != NULL)
    memset(req, 0, sizeof(*req));
}
int lc_collection_head(lc_collection *c, lc_collection_head_res *out,
                       lc_error *e) {
  if (out != NULL)
    memset(out, 0, sizeof(*out));
  if (c == NULL || c->head == NULL)
    return lc_collection_bad_receiver(e);
  return c->head(c, out, e);
}
int lc_collection_edit(lc_collection *c, const lc_collection_edit_req *req,
                       lc_source *s, lc_collection_edit_res *out, lc_error *e) {
  if (out != NULL)
    memset(out, 0, sizeof(*out));
  if (c == NULL || c->edit == NULL)
    return lc_collection_bad_receiver(e);
  return c->edit(c, req, s, out, e);
}
int lc_collection_edit_status(lc_collection *c, const char *id,
                              lc_collection_edit_status_res *out, lc_error *e) {
  if (out != NULL)
    memset(out, 0, sizeof(*out));
  if (c == NULL || c->edit_status == NULL)
    return lc_collection_bad_receiver(e);
  return c->edit_status(c, id, out, e);
}
