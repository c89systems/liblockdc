/* File history mutations for the writable multifile receiver. */

static int lc_pouch_history_file_path(lc_pouch *pouch, const char *ns,
                                      const char *consumer_id, int create,
                                      char **directory_out, char **path_out,
                                      lc_error *error) {
  char *consumer_name;
  char *consumer_leaf;
  size_t consumer_name_size;
  int rc;

  *directory_out = NULL;
  *path_out = NULL;
  rc = lc_pouch_history_directory(pouch, ns, create, directory_out, error);
  if (rc != LC_OK) {
    return rc;
  }
  consumer_name_size =
      sizeof(LC_POUCH_HISTORY_CONSUMER_PREFIX) + strlen(consumer_id);
  consumer_name =
      (char *)lc_alloc_with_allocator(&pouch->allocator, consumer_name_size);
  if (consumer_name == NULL) {
    lc_free_with_allocator(&pouch->allocator, *directory_out);
    *directory_out = NULL;
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch history consumer record",
                        NULL, NULL, "pouch");
  }
  snprintf(consumer_name, consumer_name_size,
           LC_POUCH_HISTORY_CONSUMER_PREFIX "%s", consumer_id);
  consumer_leaf = lc_pouch_path_escape_name(&pouch->allocator, consumer_name);
  lc_free_with_allocator(&pouch->allocator, consumer_name);
  *path_out =
      consumer_leaf != NULL
          ? lc_pouch_path_join(&pouch->allocator, *directory_out, consumer_leaf)
          : NULL;
  lc_free_with_allocator(&pouch->allocator, consumer_leaf);
  if (*path_out == NULL) {
    lc_free_with_allocator(&pouch->allocator, *directory_out);
    *directory_out = NULL;
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch history consumer record",
                        NULL, NULL, "pouch");
  }
  return LC_OK;
}

/* Final consumer records are the retention authority.  Stage a replacement in
 * a sibling directory so an interrupted generic atomic write never leaves a
 * `.tmp.<pid>.<attempt>` file among registered consumer identities. */
static int lc_pouch_history_write(lc_pouch *pouch, const char *directory,
                                  const char *path,
                                  lc_index_seq acknowledged_index_seq,
                                  lc_error *error) {
  char text[LC_POUCH_HISTORY_BYTES];
  char *staging_directory;
  char *staging_path;
  const char *leaf;
  int rc = lc_pouch_history_encode(acknowledged_index_seq, text, error);

  if (rc != LC_OK)
    return rc;
  leaf = strrchr(path, '/');
  leaf = leaf != NULL ? leaf + 1 : path;
  staging_directory = NULL;
  staging_path = NULL;
  rc = lc_pouch_history_staging_directory(pouch, directory, 1,
                                          &staging_directory, error);
  if (rc == LC_OK) {
    staging_path =
        lc_pouch_path_join(&pouch->allocator, staging_directory, leaf);
    if (staging_path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch history staging record", NULL,
                        staging_directory, "pouch");
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_path_write_text_file(staging_path, text, error);
  }
  if (rc == LC_OK && rename(staging_path, path) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to publish pouch history consumer record",
                      strerror(errno), path, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_path_fsync_directory(
        directory, "failed to sync pouch history consumer directory", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_path_fsync_directory(
        staging_directory, "failed to sync pouch history staging directory",
        error);
  }
  lc_free_with_allocator(&pouch->allocator, staging_path);
  lc_free_with_allocator(&pouch->allocator, staging_directory);
  return rc;
}

static int lc_pouch_history_multifile_remove(const char *directory,
                                             const char *path,
                                             lc_error *error) {
  if (unlink(path) != 0 && errno != ENOENT)
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to remove pouch history consumer record",
                        strerror(errno), path, "pouch");
  return lc_pouch_path_fsync_directory(
      directory, "failed to sync pouch history consumer directory", error);
}
