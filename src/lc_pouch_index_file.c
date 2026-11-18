#define _POSIX_C_SOURCE 200809L
#include "lc_api_internal.h"
#include "lc_pouch_index_store.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int failure(lc_error *e, const char *message) {
  return lc_error_set(e, LC_ERR_TRANSPORT, 0L, message, strerror(errno), NULL,
                      "pouch");
}
static char *directory(lc_pouch *pouch, const char *ns) {
  char *name = lc_pouch_namespace_path(&pouch->allocator, pouch->root_path, ns);
  char *path =
      name ? lc_pouch_path_join(&pouch->allocator, name, "index") : NULL;
  lc_free_with_allocator(&pouch->allocator, name);
  return path;
}
static char *artifact_path(lc_pouch *pouch, const char *ns,
                           const lc_pouch_index_artifact_id *id, lc_error *e) {
  char leaf[96], *parent, *path;
  if (lc_pouch_index_artifact_leaf(id, leaf, sizeof(leaf), e) != LC_OK)
    return NULL;
  parent = directory(pouch, ns);
  path = parent ? lc_pouch_path_join(&pouch->allocator, parent, leaf) : NULL;
  lc_free_with_allocator(&pouch->allocator, parent);
  if (!path)
    lc_error_set(e, LC_ERR_NOMEM, 0L,
                 "cannot allocate query-index artifact path", NULL, NULL,
                 "pouch");
  return path;
}
static int signature_from_stat(const struct stat *st,
                               lc_pouch_index_signature *out, lc_error *e) {
  memset(out, 0, sizeof(*out));
  if (st->st_size < 0 || !S_ISREG(st->st_mode))
    return lc_error_set(e, LC_ERR_INVALID, 0L,
                        "query-index artifact is not a regular file", NULL,
                        NULL, "pouch");
  out->size = (uint64_t)st->st_size;
#if defined(__APPLE__)
  out->mtime = (unsigned long)st->st_mtimespec.tv_sec;
  out->mtime_nsec = (unsigned long)st->st_mtimespec.tv_nsec;
  out->ctime = (unsigned long)st->st_ctimespec.tv_sec;
  out->ctime_nsec = (unsigned long)st->st_ctimespec.tv_nsec;
#elif defined(__linux__)
  out->mtime = (unsigned long)st->st_mtim.tv_sec;
  out->mtime_nsec = (unsigned long)st->st_mtim.tv_nsec;
  out->ctime = (unsigned long)st->st_ctim.tv_sec;
  out->ctime_nsec = (unsigned long)st->st_ctim.tv_nsec;
#else
  out->mtime = (unsigned long)st->st_mtime;
  out->ctime = (unsigned long)st->st_ctime;
#endif
  out->inode = (unsigned long)st->st_ino;
  out->present = 1;
  return LC_OK;
}
static int file_signature(void *context, const char *ns,
                          const lc_pouch_index_artifact_id *id,
                          lc_pouch_index_signature *out, lc_error *e) {
  lc_pouch *pouch = context;
  char *path = artifact_path(pouch, ns, id, e);
  struct stat st;
  int rc;
  memset(out, 0, sizeof(*out));
  if (!path)
    return e && e->code != LC_OK ? e->code : LC_ERR_NOMEM;
  if (stat(path, &st) == 0)
    rc = signature_from_stat(&st, out, e);
  else
    rc = errno == ENOENT ? LC_OK
                         : failure(e, "cannot inspect query-index artifact");
  lc_free_with_allocator(&pouch->allocator, path);
  return rc;
}
static int file_open(void *context, const char *ns,
                     const lc_pouch_index_artifact_id *id,
                     lc_pouch_index_input *out, lc_error *e) {
  lc_pouch *pouch = context;
  char *path = artifact_path(pouch, ns, id, e);
  struct stat st;
  int fd, rc;
  memset(out, 0, sizeof(*out));
  if (!path)
    return e && e->code != LC_OK ? e->code : LC_ERR_NOMEM;
  fd = open(path, O_RDONLY);
  if (fd < 0)
    rc = errno == ENOENT ? LC_OK
                         : failure(e, "cannot open query-index artifact");
  else {
    rc = fstat(fd, &st) == 0
             ? signature_from_stat(&st, &out->signature, e)
             : failure(e, "cannot inspect open query-index artifact");
    if (rc == LC_OK) {
      out->io = lc_pouch_record_file_io(fd);
      out->handle = (void *)(intptr_t)(fd + 1);
      out->bytes = out->signature.size;
    } else
      (void)close(fd);
  }
  lc_free_with_allocator(&pouch->allocator, path);
  return rc;
}
static void file_close(void *context, lc_pouch_index_input *input) {
  (void)context;
  if (input->handle)
    (void)close((int)(intptr_t)input->handle - 1);
  memset(input, 0, sizeof(*input));
}
typedef struct file_output {
  lc_sink sink;
  lc_pouch_record_io io;
  uint64_t maximum, written;
} file_output;
static int file_write(lc_sink *sink, const void *bytes, size_t length,
                      lc_error *e) {
  file_output *output = sink->impl;
  struct iovec part;
  int rc;
  if (length > output->maximum - output->written) {
    lc_error_set(e, LC_ERR_INVALID, 0L,
                 "query-index producer exceeded its admitted bound", NULL, NULL,
                 "pouch");
    return 0;
  }
  part.iov_base = (void *)bytes;
  part.iov_len = length;
  rc = lc_pouch_record_io_append(output->io, &part, 1, e);
  if (rc == LC_OK)
    output->written += length;
  return rc == LC_OK;
}
static int file_replace(void *context, const char *ns,
                        const lc_pouch_index_artifact_id *id, uint64_t bound,
                        lc_pouch_index_produce_fn produce, void *producer,
                        int unpublished, int durable, lc_error *e) {
  lc_pouch *pouch = context;
  char *path = artifact_path(pouch, ns, id, e), *temporary = NULL;
  file_output output;
  unsigned int attempt;
  int fd = -1, rc = LC_OK;
  size_t length;
  if (!path)
    return e && e->code != LC_OK ? e->code : LC_ERR_NOMEM;
  length = strlen(path);
  if (!unpublished) {
    temporary = lc_alloc_with_allocator(&pouch->allocator, length + 64U);
    if (!temporary)
      rc = lc_error_set(e, LC_ERR_NOMEM, 0L,
                        "cannot allocate query-index replacement path", NULL,
                        NULL, "pouch");
  }
  for (attempt = 0U; rc == LC_OK && fd < 0 && attempt < 100U; ++attempt) {
    const char *destination;
    if (temporary)
      (void)snprintf(temporary, length + 64U, "%s.tmp.%ld.%u", path,
                     (long)getpid(), attempt);
    destination = temporary ? temporary : path;
    fd = open(destination,
              O_CREAT | O_WRONLY | (unpublished ? O_TRUNC : O_EXCL), 0666);
    if (fd >= 0)
      break;
    if (errno == ENOENT && attempt == 0U) {
      rc = lc_pouch_namespace_ensure_layout(&pouch->allocator, pouch->root_path,
                                            ns, e);
      continue;
    }
    if (errno != EEXIST || unpublished)
      rc = failure(e, "cannot create query-index artifact");
  }
  if (rc == LC_OK && fd < 0)
    rc = failure(e, "query-index temporary identities are exhausted");
  if (rc == LC_OK) {
    memset(&output, 0, sizeof(output));
    output.sink.impl = &output;
    output.sink.write = file_write;
    output.io = lc_pouch_record_file_io(fd);
    output.maximum = bound;
    rc = produce(producer, &output.sink, e);
  }
  if (rc == LC_OK && durable && fsync(fd) != 0)
    rc = failure(e, "cannot sync query-index invalidation");
  if (fd >= 0 && close(fd) != 0 && rc == LC_OK)
    rc = failure(e, "cannot close query-index output");
  if (rc == LC_OK && temporary && rename(temporary, path) != 0)
    rc = failure(e, "cannot publish query-index artifact");
  if (rc == LC_OK && durable) {
    char *parent = directory(pouch, ns);
    rc = parent ? lc_pouch_path_fsync_directory(
                      parent, "cannot sync query-index publication", e)
                : lc_error_set(e, LC_ERR_NOMEM, 0L,
                               "cannot allocate query-index directory", NULL,
                               NULL, "pouch");
    lc_free_with_allocator(&pouch->allocator, parent);
  }
  if (rc != LC_OK && temporary && fd >= 0)
    (void)unlink(temporary);
  lc_free_with_allocator(&pouch->allocator, temporary);
  lc_free_with_allocator(&pouch->allocator, path);
  return rc;
}
static int file_remove(void *context, const char *ns,
                       const lc_pouch_index_artifact_id *id, lc_error *e) {
  lc_pouch *pouch = context;
  char *path = artifact_path(pouch, ns, id, e);
  int rc;
  if (!path)
    return e && e->code != LC_OK ? e->code : LC_ERR_NOMEM;
  rc = unlink(path) == 0 || errno == ENOENT
           ? LC_OK
           : failure(e, "cannot remove query-index artifact");
  lc_free_with_allocator(&pouch->allocator, path);
  return rc;
}
static int leaf_generation(const char *leaf, uint64_t *generation) {
  uint64_t value = 0U;
  size_t i;
  if (strncmp(leaf, "query.", 6U) || strlen(leaf) <= 26U || leaf[26] != '.')
    return 0;
  for (i = 6U; i < 26U; ++i) {
    unsigned int digit = (unsigned int)(unsigned char)leaf[i] - '0';
    if (digit > 9U || value > (UINT64_MAX - digit) / 10U)
      return 0;
    value = value * 10U + digit;
  }
  *generation = value;
  return 1;
}
static int file_prune(void *context, const char *ns, const uint64_t *keep,
                      size_t count, lc_error *e) {
  lc_pouch *pouch = context;
  char *path = directory(pouch, ns), *child;
  DIR *dir;
  struct dirent *entry;
  uint64_t generation;
  size_t i;
  int remove, rc = LC_OK;
  if (!path)
    return lc_error_set(e, LC_ERR_NOMEM, 0L,
                        "cannot allocate query-index cleanup path", NULL, NULL,
                        "pouch");
  dir = opendir(path);
  if (!dir) {
    rc = errno == ENOENT ? LC_OK
                         : failure(e, "cannot enumerate query-index artifacts");
    lc_free_with_allocator(&pouch->allocator, path);
    return rc;
  }
  while ((entry = readdir(dir)) != NULL) {
    remove = !strcmp(entry->d_name, "query.index") ||
             !strncmp(entry->d_name, "query.index.", 12U);
    if (leaf_generation(entry->d_name, &generation)) {
      for (i = 0U; i < count && keep[i] != generation; ++i) {
      }
      remove = i == count;
    }
    if (!remove)
      continue;
    child = lc_pouch_path_join(&pouch->allocator, path, entry->d_name);
    if (!child) {
      rc = lc_error_set(e, LC_ERR_NOMEM, 0L,
                        "cannot allocate query-index cleanup identity", NULL,
                        NULL, "pouch");
      break;
    }
    (void)unlink(child);
    lc_free_with_allocator(&pouch->allocator, child);
  }
  if (closedir(dir) != 0 && rc == LC_OK)
    rc = failure(e, "cannot close query-index inventory");
  lc_free_with_allocator(&pouch->allocator, path);
  return rc;
}
static int file_prepare(void *context, const char *ns,
                        const lc_pouch_index_artifact_id *id, uint64_t bound,
                        lc_pouch_index_produce_fn produce, void *producer,
                        int unpublished, uint64_t manifest_bound, void **build,
                        lc_pouch_index_signature *signature, lc_error *e) {
  int rc;
  *build = NULL;
  (void)manifest_bound;
  rc = file_replace(context, ns, id, bound, produce, producer, unpublished, 0,
                    e);
  return rc == LC_OK ? file_signature(context, ns, id, signature, e) : rc;
}
static int file_publish(void *context, void *build, const char *ns,
                        const lc_pouch_index_artifact_id *id, uint64_t bound,
                        lc_pouch_index_produce_fn produce, void *producer,
                        lc_error *e) {
  (void)build;
  return file_replace(context, ns, id, bound, produce, producer, 0, 0, e);
}
static void file_dispose(void *context, void *build) {
  (void)context;
  (void)build;
}
static int file_acquire(void *context, const char *ns, void **out,
                        lc_error *e) {
  lc_pouch *pouch = context;
  int rc;
  (void)ns;
  *out = NULL;
  if (!pouch->query_flush_mutex_initialized)
    return lc_error_set(e, LC_ERR_INVALID, 0L,
                        "pouch query flush requires an open pouch", NULL, NULL,
                        "pouch");
  rc = pthread_mutex_lock(&pouch->query_flush_mutex);
  if (rc)
    return lc_error_set(e, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch query flush", strerror(rc), NULL,
                        "pouch");
  *out = pouch;
  return LC_OK;
}
static void file_release(void *context, void *guard) {
  lc_pouch *pouch = context;
  (void)guard;
  (void)pthread_mutex_unlock(&pouch->query_flush_mutex);
}
const lc_pouch_index_store_ops lc_pouch_index_file_ops = {
    file_open,      file_close,   file_replace, file_remove,
    file_signature, file_prune,   file_prepare, file_publish,
    file_dispose,   file_acquire, file_release};
