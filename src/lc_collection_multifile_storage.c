#include "lc_collection_internal.h"
#include "lc_collection_query.h"
#include "lc_collection_storage.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/file.h>
#include <unistd.h>

static int file_error(lc_error *e, const char *message) {
  return lc_error_set(e, LC_ERR_TRANSPORT, 0L, message, strerror(errno), NULL,
                      "pouch");
}

static int authority_open(lc_collection_handle *c, const char *leaf,
                          lc_error *e) {
  int directory, fd, saved;
  directory = open(c->directory, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (directory < 0) {
    file_error(e, "failed to open collection authority directory");
    return -1;
  }
  fd = openat(directory, leaf, O_RDWR | O_CREAT | O_CLOEXEC, 0600);
  saved = errno;
  (void)close(directory);
  errno = saved;
  if (fd < 0)
    file_error(e, "failed to open collection authority");
  return fd;
}
static int authority_lock(void *context, enum lc_collection_authority_kind kind,
                          lc_u64 snapshot, int exclusive, int wait,
                          void **token, lc_error *e) {
  lc_collection_handle *c = context;
  char number[32], leaf[64];
  int fd, own = 0, operation, rc;
  if (kind == LC_COLLECTION_AUTH_CATALOG) {
    rc = lc_collection_catalog_lock(c->catalog_base, &fd, e);
    if (rc == LC_OK)
      *token = (void *)(intptr_t)fd;
    return rc;
  } else if (kind == LC_COLLECTION_AUTH_METADATA)
    fd = c->metadata_fd;
  else if (kind == LC_COLLECTION_AUTH_APPEND)
    fd = c->append_fd;
  else if (kind == LC_COLLECTION_AUTH_STREAMS) {
    if (exclusive) {
      fd = authority_open(c, ".streams.lock", e);
      own = 1;
    } else {
      if (c->streams_fd < 0)
        c->streams_fd = authority_open(c, ".streams.lock", e);
      fd = c->streams_fd;
    }
  } else if (kind == LC_COLLECTION_AUTH_COMMIT) {
    fd = authority_open(c, ".commit.lock", e);
    own = 1;
  } else if (kind == LC_COLLECTION_AUTH_COMPACTION) {
    fd = authority_open(c, ".compaction.lock", e);
    own = 1;
  } else if (kind == LC_COLLECTION_AUTH_READERS) {
    if (lc_u64_format_base10_padded(snapshot, 20U, number, sizeof(number)) < 0)
      return lc_error_set(e, LC_ERR_PROTOCOL, 0L,
                          "cannot format collection topology identity", NULL,
                          NULL, "pouch");
    snprintf(leaf, sizeof(leaf), ".readers-%s.lock", number);
    fd = authority_open(c, leaf, e);
    own = 1;
  } else
    return lc_error_set(e, LC_ERR_INVALID, 0L,
                        "invalid collection authority kind", NULL, NULL,
                        "pouch");
  if (fd < 0)
    return e && e->code != LC_OK ? e->code : LC_ERR_TRANSPORT;
  operation = (exclusive ? LOCK_EX : LOCK_SH) | (wait ? 0 : LOCK_NB);
  do {
    rc = flock(fd, operation);
  } while (rc != 0 && errno == EINTR);
  if (rc != 0) {
    int saved = errno;
    if (own)
      (void)close(fd);
    errno = saved;
    if (!wait && (saved == EWOULDBLOCK || saved == EAGAIN))
      return lc_error_set(e, LC_ERR_PENDING, 0L, "collection authority is busy",
                          NULL, NULL, "pouch");
    return file_error(e, "failed to acquire collection authority");
  }
  *token = (void *)(intptr_t)fd;
  return LC_OK;
}
static void authority_unlock(void *context,
                             enum lc_collection_authority_kind kind,
                             void *token) {
  int fd = (int)(intptr_t)token;
  (void)context;
  if (kind == LC_COLLECTION_AUTH_STREAMS &&
      fd != ((lc_collection_handle *)context)->streams_fd)
    (void)close(fd);
  else if (kind == LC_COLLECTION_AUTH_CATALOG ||
           kind == LC_COLLECTION_AUTH_COMMIT ||
           kind == LC_COLLECTION_AUTH_READERS ||
           kind == LC_COLLECTION_AUTH_COMPACTION)
    (void)close(fd);
  else
    (void)flock(fd, LOCK_UN);
}
lc_collection_authority lc_collection_file_authority(lc_collection_handle *c) {
  static const lc_collection_authority_ops ops = {authority_lock,
                                                  authority_unlock};
  lc_collection_authority authority;
  authority.ops = &ops;
  authority.context = c;
  return authority;
}

void lc_collection_file_bind(lc_collection_handle *c) {
  c->owner = lc_collection_file_owner(c);
  c->layout = lc_collection_file_layout(c);
  c->projections = lc_collection_file_projections(c);
  c->queries = lc_collection_file_queries(c);
  c->control = lc_collection_file_control(c);
  c->authority = lc_collection_file_authority(c);
  c->segments = lc_collection_file_segments(c);
}
