#include "lc_pouch_retention.h"
#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_path.h"
#include "lc_pouch_retention_record.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define LC_POUCH_RETENTION_WORK_LEAF "work"

#ifdef LOCKDC_TEST_BUILD
lc_pouch_retention_test_hook lc_pouch_test_retention_hook;
void *lc_pouch_test_retention_context;
#endif

int lc_pouch_retention_note_phase(int phase, lc_error *error) {
#ifdef LOCKDC_TEST_BUILD
  if (lc_pouch_test_retention_hook != NULL)
    return lc_pouch_test_retention_hook(phase, lc_pouch_test_retention_context,
                                        error);
#else
  (void)phase;
  (void)error;
#endif
  return LC_OK;
}

int lc_pouch_retention_check_reservation(lc_pouch *pouch, const char *ns,
                                         const char *key, lc_error *error) {
  lc_pouch_retention_work work;
  int rc = lc_pouch_retention_work_read(pouch, ns, &work, error);
  if (rc == LC_OK && work.present && work.phase != LC_POUCH_RETENTION_IDLE &&
      strcmp(work.key, key) == 0)
    rc = lc_error_set(error, LC_ERR_PENDING, 0L,
                      "state retention cleanup transaction is unfinished",
                      work.txn_id, NULL, "pouch");
  lc_pouch_retention_work_cleanup(pouch, &work);
  return rc;
}

int lc_pouch_retention_owns_journal(lc_pouch *pouch, const char *ns,
                                    const char *key, const char *txn_id,
                                    int *out, lc_error *error) {
  lc_pouch_retention_work work;
  int rc = lc_pouch_retention_work_read(pouch, ns, &work, error);
  *out = rc == LC_OK && work.present && work.phase != LC_POUCH_RETENTION_IDLE &&
         strcmp(work.txn_id, txn_id) == 0 && strcmp(work.key, key) == 0;
  lc_pouch_retention_work_cleanup(pouch, &work);
  return rc;
}

static int retention_paths(lc_pouch *pouch, const char *ns, int create,
                           char **directory, char **path, lc_error *error) {
  char *control = NULL;
  char *base = NULL;
  char *encoded = NULL;
  int rc = LC_OK;

  *directory = NULL;
  *path = NULL;
  control = lc_pouch_path_join(&pouch->allocator, pouch->root_path, ".lockd");
  if (control != NULL)
    base = lc_pouch_path_join(&pouch->allocator, control, "state-retention");
  if (ns != NULL) {
    encoded = lc_pouch_path_escape_name(&pouch->allocator, ns);
    if (base != NULL && encoded != NULL)
      *directory = lc_pouch_path_join(&pouch->allocator, base, encoded);
    if (*directory != NULL)
      *path = lc_pouch_path_join(&pouch->allocator, *directory,
                                 LC_POUCH_RETENTION_WORK_LEAF);
  } else if (base != NULL)
    *directory = lc_strdup_with_allocator(&pouch->allocator, base);
  if (control == NULL || base == NULL || *directory == NULL ||
      (ns != NULL && *path == NULL))
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate state retention hint path", NULL,
                      NULL, "pouch");
  if (rc == LC_OK && create) {
    rc = lc_pouch_path_ensure_directory(
        control, "failed to create pouch control directory", error);
    if (rc == LC_OK)
      rc = lc_pouch_path_ensure_directory(
          base, "failed to create state retention directory", error);
    if (rc == LC_OK)
      rc = lc_pouch_path_ensure_directory(
          *directory, "failed to create state retention directory", error);
    if (rc == LC_OK)
      rc = lc_pouch_path_fsync_directory(
          pouch->root_path, "failed to sync pouch control parent", error);
    if (rc == LC_OK)
      rc = lc_pouch_path_fsync_directory(
          control, "failed to sync state retention parent", error);
    if (rc == LC_OK)
      rc = lc_pouch_path_fsync_directory(
          base, "failed to sync state retention namespace parent", error);
  }
  lc_free_with_allocator(&pouch->allocator, control);
  lc_free_with_allocator(&pouch->allocator, base);
  lc_free_with_allocator(&pouch->allocator, encoded);
  return rc;
}

/* Only namespace-authorized control writers use this directory. Remove their
 * abandoned atomic-write temporaries, never arbitrary files in the store. */
static int retention_cleanup_temps(lc_pouch *pouch, const char *directory,
                                   lc_error *error) {
  DIR *dir = opendir(directory);
  struct dirent *entry;
  int rc = LC_OK;
  if (dir == NULL)
    return errno == ENOENT
               ? LC_OK
               : lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                              "failed to inspect retention control temporaries",
                              strerror(errno), directory, "pouch");
  while (rc == LC_OK && (entry = readdir(dir)) != NULL) {
    const char *cursor;
    const char *start;
    char *path;
    if (strncmp(entry->d_name, "work.tmp.", 9) != 0)
      continue;
    start = cursor = entry->d_name + 9;
    while (*cursor >= '0' && *cursor <= '9')
      ++cursor;
    if (cursor == start || *cursor != '.')
      continue;
    start = ++cursor;
    while (*cursor >= '0' && *cursor <= '9')
      ++cursor;
    if (cursor == start || *cursor != '\0')
      continue;
    path = lc_pouch_path_join(&pouch->allocator, directory, entry->d_name);
    if (path == NULL)
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate retention temporary path", NULL,
                        NULL, "pouch");
    else if (unlink(path) != 0 && errno != ENOENT)
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to retire retention control temporary",
                        strerror(errno), path, "pouch");
    lc_free_with_allocator(&pouch->allocator, path);
  }
  if (closedir(dir) != 0 && rc == LC_OK)
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close retention control directory",
                      strerror(errno), directory, "pouch");
  return rc;
}

int lc_pouch_retention_hint_ensure(lc_pouch *pouch, const char *ns,
                                   lc_error *error) {
  char *directory = NULL;
  char *path = NULL;
  struct stat st;
  int rc;
  if (pouch->storage.ops)
    return pouch->storage.ops->retention_hint_ensure(pouch->storage.context, ns,
                                                     error);
  rc = retention_paths(pouch, ns, 0, &directory, &path, error);
  if (rc == LC_OK && stat(path, &st) == 0) {
    if (!S_ISREG(st.st_mode))
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "state retention hint is not a regular file", NULL,
                        path, "pouch");
    goto done;
  }
  if (rc == LC_OK && errno != ENOENT) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to inspect state retention hint", strerror(errno),
                      path, "pouch");
    goto done;
  }
  if (rc == LC_OK) {
    lc_free_with_allocator(&pouch->allocator, directory);
    lc_free_with_allocator(&pouch->allocator, path);
    directory = NULL;
    path = NULL;
    rc = retention_paths(pouch, ns, 1, &directory, &path, error);
    if (rc == LC_OK)
      rc = retention_cleanup_temps(pouch, directory, error);
    if (rc == LC_OK) {
      rc = lc_pouch_path_write_text_file(path, LC_POUCH_RETENTION_IDLE_TEXT,
                                         error);
      /* Publication has not happened. Discard an unacknowledged new hint so
       * a later writer cannot mistake this failed creation for durable work. */
      if (rc != LC_OK) {
        (void)unlink(path);
        (void)rmdir(directory);
      }
    }
  }
done:
  lc_free_with_allocator(&pouch->allocator, directory);
  lc_free_with_allocator(&pouch->allocator, path);
  return rc;
}

void lc_pouch_retention_work_cleanup(lc_pouch *pouch,
                                     lc_pouch_retention_work *work) {
  lc_free_with_allocator(&pouch->allocator, work->txn_id);
  lc_free_with_allocator(&pouch->allocator, work->key);
  memset(work, 0, sizeof(*work));
}

int lc_pouch_retention_work_read(lc_pouch *pouch, const char *ns,
                                 lc_pouch_retention_work *out,
                                 lc_error *error) {
  char *directory = NULL;
  char *path = NULL;
  char *bytes = NULL;
  struct stat st;
  size_t length;
  size_t offset = 0;
  int fd = -1;
  int rc;

  memset(out, 0, sizeof(*out));
  if (pouch->storage.ops)
    return pouch->storage.ops->retention_work_load(pouch->storage.context, ns,
                                                   out, error);
  rc = retention_paths(pouch, ns, 0, &directory, &path, error);
  if (rc != LC_OK)
    goto done;
  fd = open(path, O_RDONLY);
  if (fd < 0) {
    if (errno != ENOENT)
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read state retention hint", strerror(errno),
                        path, "pouch");
    goto done;
  }
  if (fstat(fd, &st) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to stat state retention hint", strerror(errno),
                      path, "pouch");
    goto done;
  }
  if (!S_ISREG(st.st_mode) || st.st_size < 7 ||
      (uintmax_t)st.st_size > LC_POUCH_RETENTION_CONTROL_MAX)
    goto invalid;
  length = (size_t)st.st_size;
  bytes = lc_alloc_with_allocator(&pouch->allocator, length + 1);
  if (bytes == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate state retention work reference", NULL,
                      NULL, "pouch");
    goto done;
  }
  while (offset < length) {
    ssize_t count = read(fd, bytes + offset, length - offset);
    if (count < 0 && errno == EINTR)
      continue;
    if (count <= 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read complete state retention hint",
                        count < 0 ? strerror(errno) : "unexpected end of file",
                        path, "pouch");
      goto done;
    }
    offset += (size_t)count;
  }
  rc = lc_pouch_retention_record_decode(&pouch->allocator, bytes, length, path,
                                        out, error);
  goto done;
invalid:
  rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                    "invalid state retention work reference", NULL, path,
                    "pouch");
done:
  if (fd >= 0 && close(fd) != 0 && rc == LC_OK)
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close state retention hint", strerror(errno),
                      path, "pouch");
  if (rc != LC_OK)
    lc_pouch_retention_work_cleanup(pouch, out);
  lc_free_with_allocator(&pouch->allocator, bytes);
  lc_free_with_allocator(&pouch->allocator, directory);
  lc_free_with_allocator(&pouch->allocator, path);
  return rc;
}

int lc_pouch_retention_work_write(lc_pouch *pouch, const char *ns, int phase,
                                  const char *txn_id, const char *key,
                                  lc_u64 fencing_token,
                                  lc_pouch_generation state_version,
                                  lc_error *error) {
  char *directory = NULL;
  char *path = NULL;
  char *text = NULL;
  int rc;
  size_t length;
  if (pouch->storage.ops) {
    lc_pouch_retention_work work;
    memset(&work, 0, sizeof(work));
    work.present = 1;
    work.phase = phase;
    work.txn_id = (char *)txn_id;
    work.key = (char *)key;
    work.fencing_token = fencing_token;
    work.state_version = state_version;
    return pouch->storage.ops->retention_work_store(pouch->storage.context, ns,
                                                    &work, error);
  }
  rc = lc_pouch_retention_record_encode(&pouch->allocator, phase, txn_id, key,
                                        fencing_token, state_version, &text,
                                        &length, error);
  if (rc == LC_OK) {
    rc = retention_paths(pouch, ns, 0, &directory, &path, error);
    if (rc == LC_OK)
      rc = lc_pouch_path_write_text_file(path, text, error);
  }
  lc_free_with_allocator(&pouch->allocator, text);
  lc_free_with_allocator(&pouch->allocator, directory);
  lc_free_with_allocator(&pouch->allocator, path);
  return rc;
}

int lc_pouch_retention_hint_remove(lc_pouch *pouch, const char *ns,
                                   lc_error *error) {
  char *directory = NULL;
  char *path = NULL;
  char *base = NULL;
  char *unused = NULL;
  struct stat st;
  int rc;
  if (pouch->storage.ops)
    return pouch->storage.ops->retention_hint_remove(pouch->storage.context, ns,
                                                     error);
  rc = retention_paths(pouch, ns, 0, &directory, &path, error);
  if (rc == LC_OK)
    rc = retention_cleanup_temps(pouch, directory, error);
  if (rc == LC_OK && unlink(path) != 0 && errno != ENOENT)
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to remove state retention hint", strerror(errno),
                      path, "pouch");
  if (rc == LC_OK && stat(directory, &st) == 0)
    rc = lc_pouch_path_fsync_directory(
        directory, "failed to sync state retention retirement", error);
  if (rc == LC_OK && rmdir(directory) != 0 && errno != ENOENT &&
      errno != ENOTEMPTY)
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to retire state retention directory",
                      strerror(errno), directory, "pouch");
  if (rc == LC_OK)
    rc = retention_paths(pouch, NULL, 0, &base, &unused, error);
  if (rc == LC_OK && stat(base, &st) == 0)
    rc = lc_pouch_path_fsync_directory(
        base, "failed to sync state retention namespace retirement", error);
  lc_free_with_allocator(&pouch->allocator, directory);
  lc_free_with_allocator(&pouch->allocator, path);
  lc_free_with_allocator(&pouch->allocator, base);
  return rc;
}

int lc_pouch_retention_hint_next(lc_pouch *pouch, const char *after, char **ns,
                                 char **cursor, lc_error *error) {
  char *directory = NULL;
  char *unused = NULL;
  char *first = NULL;
  char *next = NULL;
  struct dirent *entry;
  DIR *dir = NULL;
  int rc;
  *ns = NULL;
  *cursor = NULL;
  if (pouch->storage.ops)
    return pouch->storage.ops->retention_hint_next(pouch->storage.context,
                                                   after, ns, cursor, error);
  rc = retention_paths(pouch, NULL, 0, &directory, &unused, error);
  if (rc != LC_OK)
    goto done;
  dir = opendir(directory);
  if (dir == NULL) {
    if (errno != ENOENT)
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to discover state retention hints",
                        strerror(errno), directory, "pouch");
    goto done;
  }
  while ((entry = readdir(dir)) != NULL) {
    char *ns_directory;
    char *work_path;
    struct stat st;
    int found;
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
      continue;
    ns_directory =
        lc_pouch_path_join(&pouch->allocator, directory, entry->d_name);
    work_path = ns_directory == NULL
                    ? NULL
                    : lc_pouch_path_join(&pouch->allocator, ns_directory,
                                         LC_POUCH_RETENTION_WORK_LEAF);
    lc_free_with_allocator(&pouch->allocator, ns_directory);
    if (work_path == NULL)
      goto no_memory;
    found = stat(work_path, &st) == 0;
    if (!found && errno != ENOENT && errno != ENOTDIR)
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to inspect state retention work",
                        strerror(errno), work_path, "pouch");
    lc_free_with_allocator(&pouch->allocator, work_path);
    if (rc != LC_OK)
      goto done;
    if (!found)
      continue;
    if (first == NULL || strcmp(entry->d_name, first) < 0) {
      lc_free_with_allocator(&pouch->allocator, first);
      first = lc_strdup_with_allocator(&pouch->allocator, entry->d_name);
      if (first == NULL)
        goto no_memory;
    }
    if (after != NULL && strcmp(entry->d_name, after) > 0 &&
        (next == NULL || strcmp(entry->d_name, next) < 0)) {
      lc_free_with_allocator(&pouch->allocator, next);
      next = lc_strdup_with_allocator(&pouch->allocator, entry->d_name);
      if (next == NULL)
        goto no_memory;
    }
  }
  *cursor = next != NULL ? next : first;
  if (*cursor != NULL) {
    rc = lc_pouch_retention_record_unescape(&pouch->allocator, *cursor, ns,
                                            directory, error);
    if (rc != LC_OK) {
      *cursor = NULL;
      goto done;
    }
    if (next != NULL)
      next = NULL;
    else
      first = NULL;
  }
  goto done;
no_memory:
  *cursor = NULL;
  rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                    "failed to select state retention namespace", NULL, NULL,
                    "pouch");
done:
  if (dir != NULL && closedir(dir) != 0 && rc == LC_OK)
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close state retention directory",
                      strerror(errno), directory, "pouch");
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, *ns);
    lc_free_with_allocator(&pouch->allocator, *cursor);
    *ns = NULL;
    *cursor = NULL;
  }
  lc_free_with_allocator(&pouch->allocator, first);
  lc_free_with_allocator(&pouch->allocator, next);
  lc_free_with_allocator(&pouch->allocator, directory);
  return rc;
}
