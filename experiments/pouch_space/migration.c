#define _GNU_SOURCE
#include "migration.h"
#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_path.h"
#include "lc_pouch_recode.h"
#include "pouch_internal.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#ifdef __linux__
#include <linux/fs.h>
#include <sys/syscall.h>
#elif defined(__APPLE__)
#include "space_internal.h"
#endif

typedef struct migration_path {
  const lc_allocator *allocator;
  char *root, *parent;
} migration_path;

#ifdef LOCKDC_TEST_BUILD
void (*ps_migration_test_hook)(const char *phase);
#endif
static void boundary(const char *phase) {
#ifdef LOCKDC_TEST_BUILD
  if (ps_migration_test_hook)
    ps_migration_test_hook(phase);
#endif
  (void)phase;
}

static int failure(lc_error *e, int code, const char *reason,
                   const char *message, const char *detail) {
  return lc_error_set(e, code, 0L, message, detail, reason, "pouch");
}

static int paths_open(migration_path *paths, const lc_allocator *allocator,
                      const char *path, lc_error *e) {
  size_t length = path ? strlen(path) : 0;
  const char *slash;
  size_t parent;
  paths->allocator = allocator;
  while (length && path[length - 1] == '/')
    --length;
  if (!length)
    return failure(e, LC_ERR_INVALID, NULL, "pouch requires a root path", path);
  paths->root = lc_alloc_with_allocator(allocator, length + 1);
  if (!paths->root)
    return failure(e, LC_ERR_NOMEM, NULL, "cannot allocate pouch root path",
                   path);
  memcpy(paths->root, path, length);
  paths->root[length] = 0;
  slash = strrchr(paths->root, '/');
  if (!slash)
    paths->parent = lc_strdup_with_allocator(allocator, ".");
  else {
    parent = slash == paths->root ? 1 : (size_t)(slash - paths->root);
    paths->parent = lc_alloc_with_allocator(allocator, parent + 1);
    if (paths->parent) {
      memcpy(paths->parent, paths->root, parent);
      paths->parent[parent] = 0;
    }
  }
  return paths->parent ? LC_OK
                       : failure(e, LC_ERR_NOMEM, NULL,
                                 "cannot allocate pouch parent", path);
}

static char *stage_path(const migration_path *paths, const char *token,
                        lc_error *e) {
  char leaf[sizeof(".pouch-import-") + LC_XID_STRING_SIZE];
  char *path;
  (void)snprintf(leaf, sizeof(leaf), ".pouch-import-%s", token);
  path = lc_pouch_path_join(paths->allocator, paths->parent, leaf);
  if (!path)
    failure(e, LC_ERR_NOMEM, NULL, "cannot allocate pouch migration path",
            paths->root);
  return path;
}

static int directory_receipt(const char *directory, const lc_allocator *a,
                             int create, char token[LC_XID_STRING_SIZE],
                             int *found, lc_error *e) {
  char *path = lc_pouch_path_join(a, directory, PS_MIGRATION_RECEIPT);
  FILE *file;
  int fd, rc = LC_OK;
  *found = 0;
  if (!path)
    return failure(e, LC_ERR_NOMEM, NULL, "cannot allocate migration receipt",
                   directory);
  fd = open(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
  if (fd < 0 && errno == ENOENT && create) {
    rc = lc_xid_new(token, e);
    if (rc == LC_OK)
      rc = lc_pouch_path_write_bytes_file(path, token, LC_XID_STRING_SIZE, e);
    *found = rc == LC_OK;
  } else if (fd < 0 && errno != ENOENT) {
    rc = failure(e, LC_ERR_TRANSPORT, NULL, "cannot open migration receipt",
                 path);
  } else if (fd >= 0) {
    file = fdopen(fd, "rb");
    if (!file) {
      close(fd);
      rc = failure(e, LC_ERR_TRANSPORT, NULL, "cannot read migration receipt",
                   path);
    } else {
      if (fread(token, 1, LC_XID_STRING_SIZE, file) != LC_XID_STRING_SIZE ||
          fgetc(file) != EOF || ferror(file) || token[LC_XID_STRING_SIZE - 1] ||
          !lc_xid_is_valid(token))
        rc = failure(e, LC_ERR_PROTOCOL, "pouch_migration_receipt",
                     "legacy pouch migration receipt is invalid", path);
      if (fclose(file) != 0 && rc == LC_OK)
        rc = failure(e, LC_ERR_TRANSPORT, NULL,
                     "cannot close migration receipt", path);
      *found = rc == LC_OK;
    }
  }
  lc_free_with_allocator(a, path);
  return rc;
}

static ps_key receipt_key(void) {
  ps_key key = {0, 3, PS_CONTROL};
  return key;
}

static int write_receipt(ps_space *space, const char *token, lc_error *e) {
  ps_writer *writer = NULL;
  int rc = ps_begin(space, NULL, receipt_key(), &writer, e);
  if (rc == LC_OK)
    rc = ps_write(writer, token, LC_XID_STRING_SIZE, e);
  if (rc == LC_OK)
    rc = ps_publish(writer, NULL, e);
  ps_writer_close(writer);
  return rc;
}

static int read_receipt(ps_space *space, char token[LC_XID_STRING_SIZE],
                        ps_ref *ref, int *found, lc_error *e) {
  ps_reader *reader = NULL;
  uint64_t length;
  int rc = ps_value_info(space, receipt_key(), ref, &length, found, e);
  if (rc != LC_OK || !*found)
    return rc;
  if (length != LC_XID_STRING_SIZE)
    return failure(e, LC_ERR_PROTOCOL, "pouch_migration_receipt",
                   "container migration receipt has invalid length", NULL);
  rc = ps_read_open(space, receipt_key(), ref, &reader, e);
  if (rc == LC_OK)
    rc = ps_read_at(reader, 0, token, LC_XID_STRING_SIZE, e);
  ps_reader_close(reader);
  if (rc == LC_OK && (token[LC_XID_STRING_SIZE - 1] || !lc_xid_is_valid(token)))
    rc = failure(e, LC_ERR_PROTOCOL, "pouch_migration_receipt",
                 "container migration receipt is invalid", NULL);
  return rc;
}

static int remove_legacy(const migration_path *paths, const char *old,
                         const char *token, lc_error *e) {
  const lc_allocator *a = paths->allocator;
  char confirmed[LC_XID_STRING_SIZE];
  int fd, found, rc = LC_OK;
  DIR *dir;
  struct dirent *entry;
  fd = open(old, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
  if (fd < 0)
    return errno == ENOENT
               ? LC_OK
               : failure(e, LC_ERR_INVALID, "pouch_migration_layout",
                         "cannot identify the retired legacy root", old);
  if (flock(fd, LOCK_EX) != 0) {
    close(fd);
    return failure(e, LC_ERR_TRANSPORT, NULL,
                   "cannot lock legacy pouch reclamation", old);
  }
  dir = fdopendir(fd);
  if (!dir) {
    close(fd);
    return failure(e, LC_ERR_TRANSPORT, NULL,
                   "cannot open legacy pouch reclamation directory", old);
  }
  rc = directory_receipt(old, a, 0, confirmed, &found, e);
  if (rc == LC_OK && !found) {
    /* An interrupted cleanup can have removed the last receipt already. */
    if (rmdir(old) != 0 && errno != ENOENT)
      rc = failure(e, LC_ERR_INVALID, "pouch_migration_receipt",
                   "retired legacy root has no migration receipt", old);
    closedir(dir);
    return rc;
  }
  if (rc == LC_OK && strcmp(confirmed, token))
    rc = failure(e, LC_ERR_PROTOCOL, "pouch_migration_receipt",
                 "retired legacy root belongs to another migration", old);
  if (rc == LC_OK)
    rc = ps_pouch_migration_inventory(old, a, e);
  while (rc == LC_OK) {
    char *child;
    errno = 0;
    entry = readdir(dir);
    if (!entry) {
      if (errno)
        rc = failure(e, LC_ERR_TRANSPORT, NULL,
                     "cannot enumerate retired legacy pouch", old);
      break;
    }
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..") ||
        !strcmp(entry->d_name, PS_MIGRATION_RECEIPT))
      continue;
    child = lc_pouch_path_join(a, old, entry->d_name);
    rc = child ? lc_pouch_recode_remove_tree(child, e)
               : failure(e, LC_ERR_NOMEM, NULL,
                         "cannot allocate retired pouch entry path", old);
    lc_free_with_allocator(a, child);
  }
  if (rc == LC_OK && unlinkat(fd, PS_MIGRATION_RECEIPT, 0) != 0)
    rc = failure(e, LC_ERR_TRANSPORT, NULL,
                 "cannot retire legacy migration receipt", old);
  if (rc == LC_OK)
    boundary("legacy-receipt-removed");
  if (rc == LC_OK && fsync(fd) != 0)
    rc = failure(e, LC_ERR_TRANSPORT, NULL, "cannot sync legacy cleanup", old);
  if (rc == LC_OK && rmdir(old) != 0)
    rc = failure(e, LC_ERR_TRANSPORT, NULL,
                 "cannot remove the retired legacy pouch directory", old);
  closedir(dir);
  return rc;
}

static int finish_migration(const migration_path *paths, lc_pouch *pouch,
                            lc_error *e) {
  ps_space *space = ps_root_space(ps_pouch_state_root(pouch));
  char token[LC_XID_STRING_SIZE], *old = NULL;
  ps_ref ref;
  int found, rc = read_receipt(space, token, &ref, &found, e);
  if (rc != LC_OK || !found)
    return rc;
  old = stage_path(paths, token, e);
  if (!old)
    return LC_ERR_NOMEM;
  rc = lc_pouch_path_fsync_directory(paths->parent, "cannot sync pouch cutover",
                                     e);
  if (rc == LC_OK)
    rc = remove_legacy(paths, old, token, e);
  if (rc == LC_OK)
    boundary("legacy-retired");
  if (rc == LC_OK)
    rc = lc_pouch_path_fsync_directory(
        paths->parent, "cannot sync pouch migration cleanup", e);
  /* A shared owner can observe the receipt concurrently. Keep it until the
   * first exclusive reopen, which can retire it without a peer racing. */
  if (rc == LC_OK && pouch->single_writer)
    rc = ps_value_remove(space, receipt_key(), &ref, e);
  lc_free_with_allocator(paths->allocator, old);
  return rc;
}

static int exchange(const char *old, const char *stage, lc_error *e) {
#if defined(__linux__) && defined(SYS_renameat2)
  if (syscall(SYS_renameat2, AT_FDCWD, old, AT_FDCWD, stage, RENAME_EXCHANGE) ==
      0)
    return LC_OK;
#elif defined(__APPLE__)
  if (renamex_np(old, stage, RENAME_SWAP) == 0)
    return LC_OK;
#else
  (void)stage;
  errno = ENOSYS;
#endif
  return failure(e,
                 errno == ENOSYS || errno == ENOTSUP || errno == EINVAL
                     ? LC_ERR_UNSUPPORTED
                     : LC_ERR_TRANSPORT,
                 "pouch_migration_exchange",
                 "cannot atomically replace legacy pouch directory",
                 strerror(errno));
}

static int legacy_candidate(const char *stage, const struct stat *original,
                            lc_error *e) {
  struct stat info;
  if (lstat(stage, &info) != 0)
    return errno == ENOENT
               ? LC_OK
               : failure(e, LC_ERR_TRANSPORT, NULL,
                         "cannot inspect earlier migration candidate", stage);
  if (!S_ISREG(info.st_mode) || info.st_nlink != 1 ||
      info.st_dev != original->st_dev || info.st_uid != geteuid())
    return failure(e, LC_ERR_INVALID, "pouch_migration_layout",
                   "earlier migration candidate has unexpected identity",
                   stage);
  if (unlink(stage) != 0)
    return failure(e, LC_ERR_TRANSPORT, NULL,
                   "cannot retire unpublished migration candidate", stage);
  return LC_OK;
}

static int key_outside_legacy(const char *root, const char *key_file,
                              lc_error *e) {
  char *key, *base, *copy = NULL;
  const char *slash;
  int rc = LC_OK;
  size_t length;
  if (!key_file || !*key_file)
    return LC_OK;
  key = realpath(key_file, NULL);
  if (!key && (slash = strrchr(key_file, '/'))) {
    copy =
        strndup(key_file, slash == key_file ? 1 : (size_t)(slash - key_file));
    if (!copy)
      return failure(e, LC_ERR_NOMEM, NULL, "cannot resolve pouch key path",
                     key_file);
    key = realpath(copy, NULL);
  }
  if (!key) {
    free(copy);
    return LC_OK;
  }
  base = realpath(root, NULL);
  if (!base)
    rc = failure(e, LC_ERR_TRANSPORT, NULL, "cannot resolve legacy pouch root",
                 root);
  else {
    length = strlen(base);
    if (!strncmp(base, key, length) &&
        (key[length] == '/' || (copy && key[length] == '\0')))
      rc = failure(e, LC_ERR_INVALID, "pouch_migration_layout",
                   "move the pouch key file outside the legacy root first",
                   key_file);
  }
  free(base);
  free(key);
  free(copy);
  return rc;
}

static int resolve_key(lc_pouch_open_options *, const lc_allocator *, int,
                       char **, lc_error *);

static int lock_empty_root(const char *root, const struct stat *original,
                           DIR **held, lc_error *e) {
  struct stat current;
  struct dirent *entry;
  int fd = open(root, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
  if (fd < 0)
    return failure(e, LC_ERR_TRANSPORT, NULL, "cannot open pouch directory",
                   root);
  *held = fdopendir(fd);
  if (!*held) {
    close(fd);
    return failure(e, LC_ERR_TRANSPORT, NULL, "cannot list pouch directory",
                   root);
  }
  if (flock(fd, LOCK_EX | LOCK_NB) != 0 || lstat(root, &current) != 0 ||
      current.st_dev != original->st_dev || current.st_ino != original->st_ino)
    return failure(e, LC_ERR_PENDING, NULL,
                   "pouch directory is changing during initialization", root);
  for (;;) {
    errno = 0;
    entry = readdir(*held);
    if (!entry)
      break;
    if (strcmp(entry->d_name, ".") && strcmp(entry->d_name, "..") &&
        strcmp(entry->d_name, PS_MIGRATION_RECEIPT))
      return failure(e, LC_ERR_INVALID, "pouch_migration_layout",
                     "nonempty directory has no legacy pouch manifest", root);
  }
  return errno ? failure(e, LC_ERR_TRANSPORT, NULL,
                         "cannot list pouch directory", root)
               : LC_OK;
}

static int migrate(const migration_path *paths, ps_pouch_state_options *options,
                   int disable_migration, const struct stat *original,
                   char **default_key_file, lc_pouch **out, lc_error *e) {
  const lc_allocator *a = paths->allocator;
  char token[LC_XID_STRING_SIZE], *stage = NULL, *manifest, *new_path = NULL;
  struct stat current, parent;
  lc_pouch *source = NULL, *target = NULL;
  lc_pouch_open_options source_options;
  ps_pouch_state_options private_options;
  ps_space *space;
  DIR *empty_root = NULL;
  int legacy = 0, found, published = 0, candidate_owned = 0, rc = LC_OK;

  if (stat(paths->parent, &parent) != 0 || parent.st_dev != original->st_dev)
    return failure(e, LC_ERR_INVALID, "pouch_migration_layout",
                   "pouch migration requires a movable root directory",
                   paths->root);
  manifest = lc_pouch_path_join(a, paths->root, "manifest");
  if (!manifest)
    return failure(e, LC_ERR_NOMEM, NULL,
                   "cannot allocate legacy manifest path", paths->root);
  if (lstat(manifest, &current) == 0) {
    if (S_ISREG(current.st_mode))
      legacy = 1;
    else
      rc = failure(e, LC_ERR_INVALID, "pouch_migration_layout",
                   "legacy pouch manifest must be a regular file", paths->root);
  } else if (errno != ENOENT)
    rc = failure(e, LC_ERR_INVALID, "pouch_migration_layout",
                 "cannot identify legacy pouch manifest", paths->root);
  lc_free_with_allocator(a, manifest);
  if (rc == LC_OK && legacy && disable_migration)
    rc = failure(e, LC_ERR_UNSUPPORTED, "pouch_migration_disabled",
                 "legacy pouch requires migration to open", paths->root);
  if (rc == LC_OK && !legacy)
    rc = lock_empty_root(paths->root, original, &empty_root, e);
  if (rc == LC_OK)
    rc = key_outside_legacy(paths->root, options->logical.crypto_key_file, e);
  if (rc == LC_OK)
    rc = resolve_key(&options->logical, a, !legacy, default_key_file, e);
  if (rc == LC_OK)
    rc = ps_pouch_migration_inventory(paths->root, a, e);
  source_options = options->logical;
  private_options = *options;
  source_options.single_writer_set = source_options.single_writer = 1;
  source_options.background_compaction_enabled_set = 1;
  source_options.query_indexing_enabled_set = 1;
  source_options.background_compaction_enabled = 0;
  source_options.query_indexing_enabled = 0;
  source_options.crypto_generate_key_file = 0;
  if (rc == LC_OK && legacy)
    rc = lc_pouch_prepare_multifile(paths->root, a, &source_options, 0, &source,
                                    e);
  if (rc == LC_OK && source &&
      (source->root_inode != (uint64_t)original->st_ino ||
       source->root_device != (uint64_t)original->st_dev))
    rc = failure(e, LC_ERR_PENDING, NULL,
                 "pouch root changed while acquiring migration ownership",
                 paths->root);
  if (rc == LC_OK)
    rc = directory_receipt(paths->root, a, 1, token, &found, e);
  if (rc == LC_OK) {
    stage = stage_path(paths, token, e);
    if (!stage)
      rc = LC_ERR_NOMEM;
  }
  if (rc == LC_OK)
    rc = legacy_candidate(stage, original, e);
  if (rc == LC_OK) {
    candidate_owned = 1;
    private_options.logical = source_options;
    if (source)
      private_options.logical.compression = source->compression;
    rc = ps_pouch_state_open(stage, 1, a, &private_options, &target, e);
  }
  if (rc == LC_OK && source)
    rc = ps_pouch_import_legacy(source, target, e);
  if (rc == LC_OK) {
    space = ps_root_space(ps_pouch_state_root(target));
    rc = write_receipt(space, token, e);
    if (rc == LC_OK)
      rc = ps_flush(space, e);
  }
  lc_pouch_close(target);
  target = NULL;
  if (rc == LC_OK) {
    /* Verify with the requested lifetime ownership while the container is
     * still private. Carry that owner across exchange into the caller. */
    ps_pouch_state_options ready = *options;
    ready.defer_workers = 1;
    rc = ps_pouch_state_open(stage, 0, a, &ready, &target, e);
  }
  if (rc == LC_OK) {
    new_path = lc_strdup_with_allocator(&target->allocator, paths->root);
    if (!new_path)
      rc = failure(e, LC_ERR_NOMEM, NULL, "cannot retain migrated pouch path",
                   paths->root);
  }
  if (rc == LC_OK)
    rc = ps_pouch_migration_inventory(paths->root, a, e);
  if (rc == LC_OK) {
    boundary("candidate-ready");
    rc = exchange(paths->root, stage, e);
  }
  published = rc == LC_OK;
  if (empty_root)
    closedir(empty_root);
  if (published) {
    lc_free_with_allocator(&target->allocator, target->root_path);
    target->root_path = new_path;
    new_path = NULL;
    boundary("root-exchanged");
    rc = finish_migration(paths, target, e);
    if (source)
      (void)lc_pouch_abort(source, NULL);
  }
  if (rc == LC_OK) {
    *out = target;
    target = NULL;
  }
  if (new_path)
    lc_free_with_allocator(&target->allocator, new_path);
  lc_pouch_close(target);
  lc_pouch_close(source);
  if (!published && candidate_owned)
    (void)unlink(stage);
  lc_free_with_allocator(a, stage);
  return rc;
}

static int resolve_key(lc_pouch_open_options *options,
                       const lc_allocator *allocator, int create,
                       char **default_file, lc_error *error) {
  int rc = LC_OK;
  if (!options->crypto_generate_key_file)
    return LC_OK;
  options->crypto_generate_key_file = 0;
  if (options->crypto_key && options->crypto_key[0])
    return LC_OK;
  if (!options->crypto_key_file || !options->crypto_key_file[0]) {
    rc = lc_pouch_crypto_default_key_file(default_file, error);
    if (rc == LC_OK)
      options->crypto_key_file = *default_file;
  }
  if (rc == LC_OK && create) {
    lc_pouch_crypto_open_options crypto_options = {0};
    lc_pouch_crypto *crypto = NULL;
    crypto_options.key_file = options->crypto_key_file;
    crypto_options.generate_key_file = 1;
    rc = lc_pouch_crypto_open(allocator, &crypto_options, &crypto, NULL, error);
    lc_pouch_crypto_close(crypto);
  }
  return rc;
}

static int existing_root(const char *path, const lc_allocator *allocator,
                         ps_pouch_state_options *options, lc_pouch **out,
                         lc_error *e) {
  ps_pouch_state_options exclusive = *options;
  lc_pouch *temporary = NULL;
  int rc = ps_pouch_state_open(path, 0, allocator, options, out, e);
  if (!e->server_code ||
      strcmp(e->server_code, "pouch_root_conversion_exclusive"))
    return rc;

  lc_error_cleanup(e);
  exclusive.logical.single_writer = exclusive.logical.single_writer_set = 1;
  exclusive.logical.background_compaction_enabled_set = 1;
  exclusive.logical.query_indexing_enabled_set = 1;
  exclusive.logical.background_compaction_enabled = 0;
  exclusive.logical.query_indexing_enabled = 0;
  rc = ps_pouch_state_open(path, 0, allocator, &exclusive, &temporary, e);
  lc_pouch_close(temporary);
  if (rc != LC_OK &&
      (!e->server_code || strcmp(e->server_code, "pouch_root_busy")))
    return rc;

  lc_error_cleanup(e);
  if (rc != LC_OK)
    options->logical.compression = NULL;
  return ps_pouch_state_open(path, 0, allocator, options, out, e);
}

static int publish_new_root(const char *stage, const char *root, int *competed,
                            lc_error *error) {
  int reason;
#if defined(__linux__) && defined(SYS_renameat2)
  if (syscall(SYS_renameat2, AT_FDCWD, stage, AT_FDCWD, root,
              RENAME_NOREPLACE) == 0)
    return LC_OK;
#elif defined(__APPLE__)
  if (renamex_np(stage, root, RENAME_EXCL) == 0)
    return LC_OK;
#else
  (void)stage;
  (void)root;
  errno = ENOSYS;
#endif
  reason = errno;
  *competed = reason == EEXIST;
  return failure(error, LC_ERR_TRANSPORT, NULL, "cannot publish new pouch root",
                 strerror(reason));
}

static int create_root(const migration_path *paths,
                       const ps_pouch_state_options *options, int *competed,
                       lc_pouch **out, lc_error *error) {
  const lc_allocator *a = paths->allocator;
  ps_pouch_state_options private_options = *options;
  lc_pouch *pouch = NULL;
  char *directory, *stage = NULL, *root = NULL;
  int rc;
  directory = lc_pouch_path_join(a, paths->parent, ".pouch-create-XXXXXX");
  if (!directory)
    return failure(error, LC_ERR_NOMEM, NULL,
                   "cannot allocate new pouch staging directory", paths->root);
  if (!mkdtemp(directory)) {
    rc = failure(error, LC_ERR_TRANSPORT, NULL,
                 "cannot create pouch staging directory", strerror(errno));
    goto done;
  }
  stage = lc_pouch_path_join(a, directory, "store");
  if (!stage) {
    rc = failure(error, LC_ERR_NOMEM, NULL, "cannot allocate new pouch path",
                 paths->root);
    goto cleanup;
  }
  private_options.defer_workers = 1;
  rc = ps_pouch_state_open(stage, 1, a, &private_options, &pouch, error);
  if (rc == LC_OK) {
    root = lc_strdup_with_allocator(&pouch->allocator, paths->root);
    if (!root)
      rc = failure(error, LC_ERR_NOMEM, NULL, "cannot retain new pouch path",
                   paths->root);
  }
  /* Commit the physical store and its root control before exposing the inode.
   * The creator holds lifetime ownership across publication. */
  if (rc == LC_OK)
    rc = ps_flush(ps_root_space(ps_pouch_state_root(pouch)), error);
  if (rc == LC_OK)
    rc = publish_new_root(stage, paths->root, competed, error);
  if (rc == LC_OK) {
    lc_free_with_allocator(&pouch->allocator, pouch->root_path);
    pouch->root_path = root;
    root = NULL;
  } else {
    if (root)
      lc_free_with_allocator(&pouch->allocator, root);
    lc_pouch_close(pouch);
    pouch = NULL;
    (void)unlink(stage);
  }
cleanup:
  (void)rmdir(directory);
  if (rc == LC_OK)
    rc = lc_pouch_path_fsync_directory(paths->parent,
                                       "cannot sync new pouch root", error);
#ifdef __APPLE__
  if (rc == LC_OK)
    rc = ps_internal_sync_published_root(
        ps_root_space(ps_pouch_state_root(pouch)), error);
#endif
  if (rc == LC_OK)
    *out = pouch;
  else
    lc_pouch_close(pouch);
done:
  lc_free_with_allocator(a, stage);
  lc_free_with_allocator(a, directory);
  return rc;
}

int ps_pouch_open_managed(const char *path, const lc_allocator *allocator,
                          const lc_pouch_open_options *logical,
                          int disable_migration, lc_pouch **out,
                          lc_error *error) {
  migration_path paths = {0};
  ps_pouch_state_options options = {0};
  struct stat info;
  lc_error local = {0};
  char *default_key_file = NULL;
  int create = 0, directory = 0, competed = 0, rc;
  if (!out)
    return failure(error, LC_ERR_INVALID, NULL, "pouch open requires output",
                   path);
  if (!error)
    error = &local;
  *out = NULL;
  if (logical)
    options.logical = *logical;
  options.grow_on_write = 1;
  rc = paths_open(&paths, allocator, path, error);
  if (rc == LC_OK && lstat(paths.root, &info) != 0) {
    if (errno == ENOENT)
      create = 1;
    else
      rc = failure(error, LC_ERR_TRANSPORT, NULL, "cannot inspect pouch root",
                   paths.root);
  }
  if (rc == LC_OK && create)
    rc = resolve_key(&options.logical, allocator, 1, &default_key_file, error);
  if (rc == LC_OK && create)
    rc = lc_pouch_path_ensure_directory(
        paths.parent, "cannot create pouch parent directory", error);
  if (rc == LC_OK && create)
    rc = create_root(&paths, &options, &competed, out, error);
  if (competed) {
    lc_error_cleanup(error);
    create = 0;
    rc = lstat(paths.root, &info) == 0
             ? LC_OK
             : failure(error, LC_ERR_TRANSPORT, NULL,
                       "cannot inspect competing pouch root", paths.root);
  }
  directory = rc == LC_OK && !create && S_ISDIR(info.st_mode);
  if (directory) {
    rc = migrate(&paths, &options, disable_migration, &info, &default_key_file,
                 out, error);
  } else if (rc == LC_OK && !create && !S_ISREG(info.st_mode))
    rc = failure(error, LC_ERR_INVALID, "pouch_migration_layout",
                 "pouch root must be a regular file or legacy directory",
                 paths.root);
  if (rc == LC_OK && !create && !directory) {
    rc = resolve_key(&options.logical, allocator, 0, &default_key_file, error);
    if (rc == LC_OK)
      rc = existing_root(paths.root, allocator, &options, out, error);
    if (rc == LC_OK)
      rc = finish_migration(&paths, *out, error);
  }
  if (rc == LC_OK && (create || directory)) {
    rc = lc_pouch_runtime_start_maintenance(*out, error);
    if (rc == LC_OK)
      rc = lc_pouch_runtime_start_indexing(*out, error);
  }
  if (rc != LC_OK && *out) {
    lc_pouch_close(*out);
    *out = NULL;
  }
  lc_free_with_allocator(allocator, paths.parent);
  lc_free_with_allocator(allocator, paths.root);
  lc_pouch_crypto_key_string_free(default_key_file);
  lc_error_cleanup(&local);
  return rc;
}
