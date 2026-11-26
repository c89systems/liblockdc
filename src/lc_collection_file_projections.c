#include "lc_collection_internal.h"
#include "lc_collection_storage.h"
#include "lc_pouch_path.h"
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct file_build {
  char *order, *ids;
  int order_owned, ids_owned;
} file_build;
static int failure(lc_error *e, int code, const char *message) {
  return lc_collection_failure(e, code, NULL, message);
}
static int order_refresh(void *context, lc_collection_order *order,
                         int reconciling, lc_error *e) {
  lc_collection_handle *c = context;
  struct stat named, opened;
  (void)reconciling;
  if (order->fd < 0)
    return lc_collection_order_open(c->order_path, 0, order, e);
  if (stat(c->order_path, &named) != 0 || fstat(order->fd, &opened) != 0)
    return failure(e, LC_ERR_TRANSPORT,
                   "failed to inspect collection ordered projection");
  if (named.st_dev != opened.st_dev || named.st_ino != opened.st_ino) {
    lc_collection_order_close(order);
    return lc_collection_order_open(c->order_path, 0, order, e);
  }
  return lc_collection_order_refresh(order, e);
}
static int ids_refresh(void *context, lc_collection_ids *ids, lc_error *e) {
  lc_collection_handle *c = context;
  return ids->fd < 0
             ? lc_collection_ids_open(c->ids_path, 0, &c->allocator, ids, e)
             : lc_collection_ids_refresh(ids, e);
}
static int begin(void *context, lc_collection_projection_build *build,
                 lc_error *e) {
  lc_collection_handle *c = context;
  file_build *files;
  char id[LC_XID_STRING_SIZE];
  size_t size;
  int rc = lc_xid_new(id, e);
  if (rc != LC_OK)
    return rc;
  files = lc_calloc_with_allocator(&c->allocator, 1U, sizeof(*files));
  if (!files)
    return failure(e, LC_ERR_NOMEM,
                   "cannot allocate collection projection construction");
  build->private_state = files;
  size = strlen(c->directory) + sizeof(id) + 32U;
  files->order = lc_alloc_with_allocator(&c->allocator, size);
  files->ids = lc_alloc_with_allocator(&c->allocator, size);
  if (!files->order || !files->ids)
    return failure(e, LC_ERR_NOMEM,
                   "cannot allocate collection recovery metadata paths");
  snprintf(files->order, size, "%s/order.rebuild-%s", c->directory, id);
  snprintf(files->ids, size, "%s/ids.rebuild-%s", c->directory, id);
  rc = lc_collection_order_open(files->order, 1, &build->order, e);
  files->order_owned = rc == LC_OK;
  if (rc == LC_OK) {
    rc = lc_collection_ids_open(files->ids, 1, &c->allocator, &build->ids, e);
    files->ids_owned = rc == LC_OK;
  }
  return rc;
}
static int publish(void *context, lc_collection_projection_build *build,
                   lc_error *e) {
  lc_collection_handle *c = context;
  file_build *files = build->private_state;
  int rc = LC_OK;
  if (fsync(build->order.fd) != 0 || fsync(build->ids.fd) != 0)
    rc = failure(e, LC_ERR_TRANSPORT,
                 "failed to sync rebuilt collection projections");
  if (rc == LC_OK && rename(files->order, c->order_path) != 0)
    rc = failure(e, LC_ERR_TRANSPORT,
                 "failed to publish rebuilt collection order");
  else if (rc == LC_OK) {
    files->order_owned = 0;
    build->published = 1;
  }
  if (rc == LC_OK && build->on_publish)
    rc = build->on_publish(build->publish_context,
                           LC_COLLECTION_PROJECTION_ORDER, e);
  if (rc == LC_OK && rename(files->ids, c->ids_path) != 0)
    rc = failure(e, LC_ERR_TRANSPORT,
                 "failed to publish rebuilt append identities");
  else if (rc == LC_OK)
    files->ids_owned = 0;
  if (rc == LC_OK && build->on_publish)
    rc = build->on_publish(build->publish_context, LC_COLLECTION_PROJECTION_IDS,
                           e);
  if (rc == LC_OK)
    rc = lc_pouch_path_fsync_directory(
        c->directory, "failed to sync collection recovery publication", e);
  if (rc == LC_OK) {
    /* Retarget the identity table's named growth authority after publication.
     */
    lc_collection_order_close(&build->order);
    lc_collection_ids_close(&build->ids);
    rc = lc_collection_order_open(c->order_path, 0, &build->order, e);
    if (rc == LC_OK)
      rc =
          lc_collection_ids_open(c->ids_path, 0, &c->allocator, &build->ids, e);
  }
  return rc;
}
static void cleanup(void *context, lc_collection_projection_build *build) {
  lc_collection_handle *c = context;
  file_build *files = build->private_state;
  lc_collection_order_close(&build->order);
  lc_collection_ids_close(&build->ids);
  if (files) {
    if (files->order_owned)
      (void)unlink(files->order);
    if (files->ids_owned)
      (void)unlink(files->ids);
    lc_free_with_allocator(&c->allocator, files->order);
    lc_free_with_allocator(&c->allocator, files->ids);
    lc_free_with_allocator(&c->allocator, files);
  }
  build->private_state = NULL;
}
lc_collection_projections
lc_collection_file_projections(lc_collection_handle *c) {
  lc_collection_projections projections = {order_refresh, ids_refresh, begin,
                                           publish,       cleanup,     NULL};
  projections.context = c;
  return projections;
}
