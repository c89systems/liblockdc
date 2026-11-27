/* File marker mutations for the writable multifile receiver. */

static int write_marker(void *context, const char *name,
                        const unsigned char *bytes, size_t size, lc_error *e) {
  file_store *store = context;
  char *path = lc_pouch_path_join(&store->allocator, store->directory, name);
  char *temporary = NULL;
  size_t temporary_size, offset = 0U;
  unsigned long attempt;
  int fd = -1, rc = LC_OK;
  if (size > LC_POUCH_OUTBOX_MARKER_MAX_BYTES) {
    lc_free_with_allocator(&store->allocator, path);
    return failure(e, LC_ERR_INVALID, "outbox marker exceeds metadata bounds");
  }
  if (!path)
    return failure(e, LC_ERR_NOMEM, "cannot allocate outbox marker path");
  temporary_size = strlen(path) + 64U;
  temporary = lc_alloc_with_allocator(&store->allocator, temporary_size);
  if (!temporary)
    rc = failure(e, LC_ERR_NOMEM, "cannot allocate outbox marker temporary");
  for (attempt = 0UL; rc == LC_OK && attempt < 32UL; ++attempt) {
    snprintf(temporary, temporary_size, "%s.tmp.%ld.%lu", path, (long)getpid(),
             attempt);
    fd = open(temporary, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd >= 0)
      break;
    if (errno != EEXIST)
      rc =
          failure(e, LC_ERR_TRANSPORT, "cannot create outbox marker temporary");
  }
  if (rc == LC_OK && fd < 0)
    rc = failure(e, LC_ERR_TRANSPORT, "outbox marker temporaries exhausted");
  while (rc == LC_OK && offset < size) {
    ssize_t sent = write(fd, bytes + offset, size - offset);
    if (sent < 0 && errno == EINTR)
      continue;
    if (sent <= 0)
      rc = failure(e, LC_ERR_TRANSPORT, "cannot write outbox marker");
    else
      offset += (size_t)sent;
  }
  if (rc == LC_OK && fsync(fd) != 0)
    rc = failure(e, LC_ERR_TRANSPORT, "cannot synchronize outbox marker");
  if (fd >= 0 && close(fd) != 0 && rc == LC_OK)
    rc = failure(e, LC_ERR_TRANSPORT, "cannot close outbox marker");
  if (rc == LC_OK && rename(temporary, path) != 0)
    rc = failure(e, LC_ERR_TRANSPORT, "cannot publish outbox marker");
  if (rc == LC_OK)
    rc = lc_pouch_path_fsync_directory(store->directory,
                                       "cannot synchronize outbox markers", e);
  if (rc != LC_OK && temporary)
    (void)unlink(temporary);
  lc_free_with_allocator(&store->allocator, path);
  lc_free_with_allocator(&store->allocator, temporary);
  return rc;
}
static int remove_marker(void *context, const char *name, lc_error *e) {
  file_store *store = context;
  char *path = lc_pouch_path_join(&store->allocator, store->directory, name);
  int rc = LC_OK;
  if (!path)
    return failure(e, LC_ERR_NOMEM, "cannot allocate outbox marker path");
  if (unlink(path) != 0) {
    if (errno == ENOENT && store->state_reclaim) {
      /* Stateless completion never creates this optional directory. */
      lc_free_with_allocator(&store->allocator, path);
      return LC_OK;
    }
    if (errno != ENOENT)
      rc = failure(e, LC_ERR_TRANSPORT, "cannot remove outbox marker");
  }
  if (rc == LC_OK)
    rc = lc_pouch_path_fsync_directory(store->directory,
                                       "cannot synchronize outbox markers", e);
  lc_free_with_allocator(&store->allocator, path);
  return rc;
}
