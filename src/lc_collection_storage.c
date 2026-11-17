#include "lc_collection_storage.h"
#include "lc_collection_chunks.h"
#include "lc_collection_internal.h"
#include "lc_collection_unit.h"
#include "lc_pouch_path.h"
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <string.h>
#include <sys/file.h>
#include <unistd.h>

static int file_error(lc_error *e, const char *message) {
  return lc_error_set(e, LC_ERR_TRANSPORT, 0L, message, strerror(errno), NULL,
                      "pouch");
}
static int file_open(void *context, lc_u64 segment, lc_pouch_record_io *input,
                     void **pin, lc_error *e) {
  lc_collection_file_reads *files = context;
  char leaf[64];
  int fd, rc = lc_collection_record_leaf(segment, leaf, e);
  if (rc != LC_OK)
    return rc;
  if (files->fd < 0) {
    files->fd = open(files->directory, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (files->fd < 0)
      return file_error(e, "failed to open collection read directory");
  }
  fd = openat(files->fd, leaf, O_RDONLY | O_CLOEXEC);
  if (fd < 0)
    return file_error(e, "failed to open collection canonical input");
  *input = lc_collection_unit_file_io(fd);
  *pin = (void *)(intptr_t)fd;
  return LC_OK;
}
static void file_close(void *context, void *pin) {
  (void)context;
  (void)close((int)(intptr_t)pin);
}
static void file_advise(void *context, void *pin, int advice) {
  (void)context;
#if defined(__linux__)
  (void)posix_fadvise((int)(intptr_t)pin, 0, 0, advice);
#else
  (void)pin;
  (void)advice;
#endif
}
void lc_collection_file_reads_init(lc_collection_file_reads *files,
                                   const char *directory) {
  files->directory = directory;
  files->fd = -1;
}
void lc_collection_file_reads_cleanup(lc_collection_file_reads *files) {
  if (files->fd >= 0)
    (void)close(files->fd);
  files->fd = -1;
}
lc_collection_read_storage
lc_collection_file_reads_access(lc_collection_file_reads *files) {
  lc_collection_read_storage access = {file_open, file_close, NULL,
                                       file_advise};
  access.context = files;
  return access;
}
void lc_collection_input_close(lc_collection_input *input) {
  if (!input)
    return;
  if (input->io.ops)
    input->storage.close(input->storage.context, input->pin);
  memset(input, 0, sizeof(*input));
}
int lc_collection_input_refresh(lc_collection_input *input, lc_error *e) {
  return lc_pouch_record_io_tail(input->io, &input->end, e);
}
int lc_collection_input_open(lc_collection_read_storage storage, lc_u64 segment,
                             lc_collection_input *input, lc_error *e) {
  lc_collection_input fresh;
  int rc;
  if (!input || !storage.open || !storage.close || !segment)
    return lc_error_set(e, LC_ERR_INVALID, 0L,
                        "collection input requires owner and segment", NULL,
                        NULL, "pouch");
  if (input->io.ops && input->segment == segment &&
      input->storage.open == storage.open &&
      input->storage.context == storage.context)
    return LC_OK;
  memset(&fresh, 0, sizeof(fresh));
  fresh.storage = storage;
  fresh.segment = segment;
  rc = storage.open(storage.context, segment, &fresh.io, &fresh.pin, e);
  if (rc == LC_OK)
    rc = lc_collection_input_refresh(&fresh, e);
  if (rc != LC_OK) {
    lc_collection_input_close(&fresh);
    return rc;
  }
  lc_collection_input_close(input);
  *input = fresh;
  return LC_OK;
}
void lc_collection_input_advise(lc_collection_input *input, int advice) {
  if (input->io.ops && input->storage.advise)
    input->storage.advise(input->storage.context, input->pin, advice);
}

static int segment_clone(void *context, lc_collection_segment *out,
                         lc_error *e) {
  int fd = fcntl((int)(intptr_t)context, F_DUPFD_CLOEXEC, 0);
  if (fd < 0)
    return file_error(e, "failed to retain collection canonical segment");
  return lc_collection_segment_adopt_file(fd, out, e);
}
static void segment_close(void *context) {
  (void)close((int)(intptr_t)context);
}
static int segment_resize(void *context, lc_u64 end, lc_error *e) {
  int fd = (int)(intptr_t)context;
  if (end > (lc_u64)LC_I64_MAX)
    return lc_error_set(e, LC_ERR_INVALID, 0L,
                        "collection segment exceeds its offset range", NULL,
                        NULL, "pouch");
  if (ftruncate(fd, (off_t)end) != 0)
    return file_error(e, "failed to resize collection canonical range");
  return LC_OK;
}
static int segment_patch(void *context, lc_u64 at, const void *bytes,
                         size_t count, lc_error *e) {
  int fd = (int)(intptr_t)context;
  const unsigned char *p = bytes;
  ssize_t n;
  if (at > (lc_u64)LC_I64_MAX || count > (lc_u64)LC_I64_MAX - at)
    return lc_error_set(e, LC_ERR_INVALID, 0L,
                        "collection private range exceeds its offset bound",
                        NULL, NULL, "pouch");
  while (count) {
    n = pwrite(fd, p, count, (off_t)at);
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      return file_error(e, "failed to write private collection range");
    count -= (size_t)n;
    at += (size_t)n;
    p += (size_t)n;
  }
  return LC_OK;
}
static int segment_sync_private(void *context, lc_error *e) {
  int fd = (int)(intptr_t)context;
  int rc;
#ifdef LOCKDC_TEST_BUILD
  if (lc_collection_chunks_test_sync_fd)
    rc = lc_collection_chunks_test_sync_fd(fd);
  else
#endif
#if defined(__linux__)
    rc = fdatasync(fd);
#else
  rc = fsync(fd);
#endif
  return rc == 0 ? LC_OK
                 : file_error(e, "failed to seal private collection range");
}
static int segment_publish(void *context, lc_pouch *pouch, lc_error *e) {
  int fd = (int)(intptr_t)context;
  return lc_pouch_fsync_commit(pouch, fd, e);
}
static int segment_ready(void *context, lc_error *e) {
  (void)context;
  (void)e;
  return LC_OK;
}
static int segment_force(void *context, lc_error *e) {
  int fd = (int)(intptr_t)context;
  return fsync(fd) == 0
             ? LC_OK
             : file_error(e, "failed to sync collection policy boundary");
}
static const lc_collection_segment_ops file_segment_ops = {
    segment_clone, segment_close,        segment_resize, segment_resize,
    segment_patch, segment_sync_private, segment_ready,  segment_publish,
    segment_force, segment_force};
int lc_collection_segment_adopt_file(int fd, lc_collection_segment *out,
                                     lc_error *e) {
  if (!out || fd < 0) {
    if (fd >= 0)
      (void)close(fd);
    return lc_error_set(e, LC_ERR_INVALID, 0L,
                        "collection segment requires descriptor and output",
                        NULL, NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  out->ops = &file_segment_ops;
  out->context = (void *)(intptr_t)fd;
  out->io = lc_pouch_record_file_io(fd);
  out->unit_io = lc_collection_unit_file_io(fd);
  return LC_OK;
}
int lc_collection_segment_clone(const lc_collection_segment *source,
                                lc_collection_segment *out, lc_error *e) {
  memset(out, 0, sizeof(*out));
  if (!source || !source->ops)
    return lc_error_set(e, LC_ERR_INVALID, 0L,
                        "collection segment clone requires an owner", NULL,
                        NULL, "pouch");
  return source->ops->clone(source->context, out, e);
}
void lc_collection_segment_close(lc_collection_segment *segment) {
  if (segment->ops)
    segment->ops->close(segment->context);
  memset(segment, 0, sizeof(*segment));
}
int lc_collection_segment_reserve(lc_collection_segment *s, lc_u64 end,
                                  lc_error *e) {
  return s->ops->reserve(s->context, end, e);
}
int lc_collection_segment_truncate(lc_collection_segment *s, lc_u64 end,
                                   lc_error *e) {
  return s->ops->truncate(s->context, end, e);
}
int lc_collection_segment_patch(lc_collection_segment *s, lc_u64 at,
                                const void *bytes, size_t count, lc_error *e) {
  return s->ops->patch_private(s->context, at, bytes, count, e);
}
int lc_collection_segment_sync_private(lc_collection_segment *s, lc_error *e) {
  return s->ops->sync_private(s->context, e);
}
int lc_collection_segment_publish(lc_collection_segment *s, lc_pouch *pouch,
                                  lc_error *e) {
  return s->ops->publish(s->context, pouch, e);
}
int lc_collection_segment_ready(lc_collection_segment *s, lc_error *e) {
  return s->ops->ready(s->context, e);
}
static int segment_open(void *context, lc_u64 segment, int create,
                        lc_collection_segment *out, lc_error *e) {
  lc_collection_handle *c = context;
  char leaf[64], *path = NULL;
  int fd, rc = lc_collection_record_path(c, segment, &path, leaf, e);
  if (rc != LC_OK)
    return rc;
  fd = open(path, O_RDWR | O_CLOEXEC | (create ? O_CREAT | O_EXCL : 0), 0600);
  lc_free_with_allocator(&c->allocator, path);
  if (fd < 0)
    return file_error(e, "failed to open collection canonical segment");
  if (create) {
    rc = lc_pouch_path_fsync_directory(
        c->directory, "failed to sync collection canonical segment name", e);
    if (rc != LC_OK) {
      (void)close(fd);
      return rc;
    }
  }
  return lc_collection_segment_adopt_file(fd, out, e);
}
lc_collection_segments lc_collection_file_segments(lc_collection_handle *c) {
  lc_collection_segments segments;
  segments.open = segment_open;
  segments.context = c;
  segments.private_limit = 0U;
  return segments;
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
int lc_collection_lock_enter(lc_collection_authority authority,
                             enum lc_collection_authority_kind kind,
                             lc_u64 snapshot, int exclusive, int wait,
                             lc_collection_lock *pin, lc_error *e) {
  int rc;
  memset(pin, 0, sizeof(*pin));
  if (!authority.ops || !authority.ops->lock || !authority.ops->unlock)
    return lc_error_set(e, LC_ERR_INVALID, 0L,
                        "collection coordination requires an owner", NULL, NULL,
                        "pouch");
  rc = authority.ops->lock(authority.context, kind, snapshot, exclusive, wait,
                           &pin->token, e);
  if (rc == LC_OK) {
    pin->authority = authority;
    pin->kind = kind;
  }
  return rc;
}
void lc_collection_lock_leave(lc_collection_lock *pin) {
  if (pin->authority.ops)
    pin->authority.ops->unlock(pin->authority.context, pin->kind, pin->token);
  memset(pin, 0, sizeof(*pin));
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
int lc_collection_segment_selected(const lc_collection_disk_head *head,
                                   lc_u64 id) {
  if (id & LC_COLLECTION_SNAPSHOT_BIT) {
    id &= (lc_u64)LC_I64_MAX;
    return head->snapshot && id >= head->snapshot &&
           id - head->snapshot <= head->snapshot_extra_parts;
  }
  return id >= head->tail_segment && id <= head->active;
}

int lc_collection_segment_force(lc_collection_segment *segment, lc_error *e) {
  return segment->ops->force(segment->context, e);
}
int lc_collection_segment_seal(lc_collection_segment *segment, lc_error *e) {
  return segment->ops->seal(segment->context, e);
}
