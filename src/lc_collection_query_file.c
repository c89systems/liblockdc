#define _POSIX_C_SOURCE 200809L
#include "lc_collection_query.h"
#include "lc_pouch_path.h"
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct query_file_publication {
  lc_collection_handle *collection;
  const char *directory, *temporary, *path;
  int selected;
} query_file_publication;

static int paths(lc_collection_handle *c, const char *query_id, lc_u64 ordinal,
                 char **directory, char **path, lc_error *e) {
  char number[32], leaf[40];
  char *base;
  *directory = *path = NULL;
  if (!lc_xid_is_valid(query_id) ||
      lc_u64_format_base10(ordinal, number, sizeof(number)) < 0)
    return lc_collection_failure(e, LC_ERR_INVALID, NULL,
                                 "collection query page identity is invalid");
  (void)snprintf(leaf, sizeof(leaf), "page-%s", number);
  base = lc_pouch_path_join(&c->allocator, c->directory, "query");
  if (base)
    *directory = lc_pouch_path_join(&c->allocator, base, query_id);
  lc_free_with_allocator(&c->allocator, base);
  if (*directory)
    *path = lc_pouch_path_join(&c->allocator, *directory, leaf);
  if (!*path) {
    lc_free_with_allocator(&c->allocator, *directory);
    *directory = NULL;
    return lc_collection_failure(e, LC_ERR_NOMEM, NULL,
                                 "cannot allocate collection query page path");
  }
  return LC_OK;
}
static int file_open(void *context, const char *query_id, lc_u64 ordinal,
                     lc_pouch_index_input *out, lc_error *e) {
  lc_collection_handle *c = context;
  char *directory = NULL, *path = NULL;
  struct stat st;
  int fd = -1, rc;
  memset(out, 0, sizeof(*out));
  rc = paths(c, query_id, ordinal, &directory, &path, e);
  if (rc == LC_OK) {
    fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0 && errno != ENOENT)
      rc = lc_collection_failure(e, LC_ERR_TRANSPORT, NULL,
                                 "cannot open collection query page");
    if (fd >= 0) {
      if (fstat(fd, &st) != 0)
        rc = lc_collection_failure(e, LC_ERR_TRANSPORT, NULL,
                                   "cannot inspect collection query page");
      else if (!S_ISREG(st.st_mode) || st.st_size < 0)
        rc = lc_collection_failure(e, LC_ERR_PROTOCOL, NULL,
                                   "collection query page is not regular");
      if (rc == LC_OK) {
        out->io = lc_pouch_record_file_io(fd);
        out->bytes = (uint64_t)st.st_size;
        out->signature.size = out->bytes;
        out->signature.present = 1;
        out->handle = (void *)(intptr_t)(fd + 1);
        fd = -1;
      }
    }
  }
  if (fd >= 0)
    (void)close(fd);
  lc_free_with_allocator(&c->allocator, directory);
  lc_free_with_allocator(&c->allocator, path);
  return rc;
}
static void file_close(void *context, lc_pouch_index_input *input) {
  (void)context;
  if (input->handle)
    (void)close((int)((intptr_t)input->handle - 1));
  memset(input, 0, sizeof(*input));
}
static int select_file(void *context, lc_error *e) {
  query_file_publication *p = context;
  if (rename(p->temporary, p->path) != 0)
    return lc_collection_failure(e, LC_ERR_TRANSPORT, NULL,
                                 "failed to publish collection query artifact");
  p->selected = 1;
  return p->collection->pouch->durable_sync
             ? lc_pouch_path_fsync_directory(
                   p->directory, "failed to sync collection query name", e)
             : LC_OK;
}
typedef struct query_file_output {
  lc_pouch_record_io io;
  uint64_t bound, written;
} query_file_output;
static int file_write(lc_sink *sink, const void *bytes, size_t length,
                      lc_error *e) {
  query_file_output *p = sink->impl;
  struct iovec part;
  int rc;
  if (length > p->bound - p->written) {
    lc_collection_failure(e, LC_ERR_PROTOCOL, NULL,
                          "collection query encoder exceeded admission");
    return 0;
  }
  part.iov_base = (void *)bytes;
  part.iov_len = length;
  rc = lc_pouch_record_io_append(p->io, &part, 1, e);
  if (rc == LC_OK)
    p->written += length;
  return rc == LC_OK;
}
static int file_replace(void *context, const char *query_id, lc_u64 ordinal,
                        uint64_t bound, lc_pouch_index_produce_fn produce,
                        void *producer, lc_pouch_index_select_fn select,
                        void *selector, lc_error *e) {
  lc_collection_handle *c = context;
  query_file_publication p;
  query_file_output output;
  lc_sink sink;
  char *directory = NULL, *path = NULL, *temporary = NULL, *base = NULL;
  char xid[LC_XID_STRING_SIZE], leaf[48];
  int fd = -1, held = 0, created = 0, skip = 0, rc;
  memset(&p, 0, sizeof(p));
  memset(&output, 0, sizeof(output));
  memset(&sink, 0, sizeof(sink));
  rc = paths(c, query_id, ordinal, &directory, &path, e);
  if (rc == LC_OK) {
    base = lc_pouch_path_join(&c->allocator, c->directory, "query");
    if (!base)
      rc = lc_collection_failure(e, LC_ERR_NOMEM, NULL,
                                 "cannot allocate collection query directory");
  }
  if (rc == LC_OK)
    rc = lc_xid_new(xid, e);
  if (rc == LC_OK) {
    (void)snprintf(leaf, sizeof(leaf), ".page-%s.tmp", xid);
    temporary = lc_pouch_path_join(&c->allocator, directory, leaf);
    if (!temporary)
      rc = lc_collection_failure(e, LC_ERR_NOMEM, NULL,
                                 "cannot allocate collection query temporary");
  }
  if (rc == LC_OK) {
    rc = lc_collection_metadata_enter(c, 0, e);
    held = rc == LC_OK;
  }
  if (rc == LC_OK)
    rc = lc_collection_refresh_view(c, e);
  if (rc == LC_OK)
    skip = strcmp(c->order.query_id, query_id) != 0;
  if (rc == LC_OK && !skip)
    rc = lc_pouch_path_ensure_directory(
        base, "failed to create collection query directory", e);
  if (rc == LC_OK && !skip)
    rc = lc_pouch_path_ensure_directory(
        directory, "failed to create collection query directory", e);
  if (rc == LC_OK && !skip) {
    fd = open(temporary, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd < 0)
      rc = lc_collection_failure(e, LC_ERR_TRANSPORT, NULL,
                                 "failed to create collection query artifact");
    else
      created = 1;
  }
  if (rc == LC_OK && !skip && flock(fd, LOCK_EX) != 0)
    rc = lc_collection_failure(e, LC_ERR_TRANSPORT, NULL,
                               "failed to protect collection query publisher");
  if (held)
    lc_collection_metadata_leave(c);
#ifdef LOCKDC_TEST_BUILD
  if (rc == LC_OK && !skip && lc_collection_query_test_hook != NULL)
    rc = lc_collection_query_test_hook(lc_collection_query_test_context, c,
                                       temporary, e);
#endif
  if (rc == LC_OK && !skip) {
    output.io = lc_pouch_record_file_io(fd);
    output.bound = bound;
    sink.impl = &output;
    sink.write = file_write;
    rc = produce(producer, &sink, e);
  }
  if (rc == LC_OK && !skip)
    rc = lc_pouch_fsync_commit(c->pouch, fd, e);
  if (rc == LC_OK && !skip) {
    p.collection = c;
    p.directory = directory;
    p.temporary = temporary;
    p.path = path;
    rc = select(selector, select_file, &p, e);
  }
  if (fd >= 0 && close(fd) != 0 && rc == LC_OK)
    rc = lc_collection_failure(e, LC_ERR_TRANSPORT, NULL,
                               "failed to close collection query artifact");
  if (created && !p.selected)
    (void)unlink(temporary);
  lc_free_with_allocator(&c->allocator, directory);
  lc_free_with_allocator(&c->allocator, path);
  lc_free_with_allocator(&c->allocator, temporary);
  lc_free_with_allocator(&c->allocator, base);
  return rc;
}
lc_collection_query_storage
lc_collection_file_queries(lc_collection_handle *c) {
  lc_collection_query_storage access = {file_open, file_close, file_replace,
                                        lc_collection_file_query_cleanup, NULL};
  access.context = c;
  return access;
}
