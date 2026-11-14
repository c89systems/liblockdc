#include "pouch.h"
#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "namespace.h"
#include <string.h>
#include <sys/stat.h>

typedef struct ps_pouch_owner {
  ps_root *root;
  lc_allocator allocator;
  unsigned int references;
} ps_pouch_owner;
typedef struct ps_pouch_source {
  lc_source pub;
  ps_pouch_owner *owner;
  ps_namespace_window *window;
  uint64_t start, length, offset;
} ps_pouch_source;

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
static int namespace_open(void *context, const char *name, int create,
                          void **out, lc_error *e) {
  ps_pouch_owner *owner = context;
  ps_namespace *ns = NULL;
  int rc = ps_root_namespace_acquire(owner->root, name, create, &ns, e);
  *out = ns;
  return rc;
}
static void namespace_close(void *context, void *ns) {
  (void)context;
  ps_namespace_release(ns);
}
static int namespace_info(void *context, void *ns,
                          lc_pouch_storage_namespace_info *out, lc_error *e) {
  ps_namespace_status status;
  int rc;
  (void)context;
  rc = ps_namespace_status_get(ns, &status, e);
  if (rc == LC_OK) {
    out->active_segment = status.active_segment;
    out->segments = status.segments;
    out->tail = status.tail;
  }
  return rc;
}
static int replay(void *context, void *ns, lc_pouch_storage_visit_fn visit,
                  void *opaque, lc_error *e) {
  ps_namespace_view *view = NULL;
  int rc;
  (void)context;
  rc = ps_namespace_view_open(ns, &view, e);
  if (rc == LC_OK)
    rc = ps_namespace_view_visit(view, 1, visit, opaque, e);
  ps_namespace_view_close(view);
  return rc;
}
static int append_begin(void *context, void *ns, uint64_t target,
                        uint64_t minimum, lc_pouch_storage_append *out,
                        lc_error *e) {
  ps_namespace_append *append = NULL;
  int rc;
  (void)context;
  memset(out, 0, sizeof(*out));
  rc = ps_namespace_append_begin(ns, target, minimum, &append, e);
  if (rc != LC_OK)
    return rc;
  out->handle = append;
  out->io = ps_namespace_append_output(append);
  out->leaf = ps_namespace_append_leaf(append);
  out->segment = ps_namespace_append_segment(append);
  rc = lc_pouch_record_io_tail(out->io, &out->start, e);
  if (rc != LC_OK) {
    ps_namespace_append_close(append);
    memset(out, 0, sizeof(*out));
  }
  return rc;
}
static int append_commit(void *context, lc_pouch_storage_append *append,
                         lc_error *e) {
  (void)context;
  return ps_namespace_append_commit(append->handle, e);
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
  if ((uint64_t)length > source->length - source->offset)
    length = (size_t)(source->length - source->offset);
  if (!length)
    return 0;
  part.iov_base = bytes;
  part.iov_len = length;
  if (lc_pouch_record_io_read(ps_namespace_read_input(source->window),
                              source->start + source->offset, &part, 1,
                              e) != LC_OK)
    return 0;
  source->offset += length;
  return length;
}
static int source_reset(lc_source *pub, lc_error *e) {
  ps_pouch_source *source = pub->impl;
  (void)e;
  source->offset = 0;
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
                       uint64_t offset, uint64_t length, lc_source **out,
                       lc_error *e) {
  ps_pouch_owner *owner = context;
  ps_pouch_source *source;
  uint64_t segment;
  int rc;
  *out = NULL;
  if (!leaf || !lc_pouch_namespace_parse_segment_leaf(leaf, &segment))
    return lc_error_set(e, LC_ERR_PROTOCOL, 0, "invalid container payload leaf",
                        NULL, NULL, "pouch");
  source = lc_calloc_with_allocator(&owner->allocator, 1, sizeof(*source));
  if (!source)
    return lc_error_set(e, LC_ERR_NOMEM, 0, "cannot allocate container source",
                        NULL, NULL, "pouch");
  rc = ps_namespace_read_open(ns, segment, offset, length, &source->window, e);
  if (rc != LC_OK) {
    lc_free_with_allocator(&owner->allocator, source);
    return rc;
  }
  source->owner = owner;
  source->start = offset;
  source->length = length;
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
static const lc_pouch_storage_ops storage_ops = {
    check,       namespace_open, namespace_close,       namespace_info,
    replay,      append_begin,   append_commit,         append_close,
    source_open, source_clone,   retention_hint_ensure, release};

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
  if (!pouch->single_writer || pouch->background_compaction_enabled ||
      pouch->query_indexing_enabled || pouch->queue_watch_enabled ||
      (options && options->logical.crypto_generate_key_file)) {
    lc_pouch_close(pouch);
    return lc_error_set(
        e, LC_ERR_UNSUPPORTED, 0,
        "container state harness requires exclusive mode, "
        "disabled indexing/maintenance/watches and no generated key file",
        NULL, NULL, "pouch");
  }
  owner = lc_calloc_with_allocator(&pouch->allocator, 1, sizeof(*owner));
  if (!owner) {
    lc_pouch_close(pouch);
    return lc_error_set(e, LC_ERR_NOMEM, 0, "cannot allocate container owner",
                        NULL, NULL, "pouch");
  }
  owner->allocator = pouch->allocator;
  owner->references = 1;
  memset(&root_options, 0, sizeof(root_options));
  root_options.storage.durable_sync = pouch->durable_sync;
  root_options.crypto_allocator = &pouch->allocator;
  root_options.crypto.compression_enabled = !strcmp(pouch->compression, "zlib");
  if (options) {
    root_options.storage.initial_capacity = options->initial_capacity;
    root_options.storage.maximum_capacity = options->maximum_capacity;
    root_options.storage.io = options->io;
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
  *out = pouch;
  return LC_OK;
}
