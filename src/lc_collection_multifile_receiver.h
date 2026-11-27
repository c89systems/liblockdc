/* Included by lc_collections.c for the writable multifile receiver. */

static int lc_cc_lock(int fd, int operation, lc_error *error) {
  while (flock(fd, operation) != 0) {
    if (errno == EINTR)
      continue;
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to lock collection authority");
  }
  return LC_OK;
}

static int lc_cc_invalidate_clean(lc_collection_handle *collection,
                                  lc_error *error) {
  char *path;
  int removed;
  path = lc_cc_path(collection, "clean");
  if (path == NULL)
    return lc_collection_failure(
        error, LC_ERR_NOMEM, NULL,
        "failed to allocate collection checkpoint path");
  removed = unlink(path) == 0;
  lc_free_with_allocator(&collection->allocator, path);
  if (!removed && errno != ENOENT)
    return lc_collection_failure(
        error, LC_ERR_TRANSPORT, NULL,
        "failed to invalidate collection clean checkpoint");
  return removed
             ? lc_pouch_path_fsync_directory(
                   collection->directory,
                   "failed to sync collection checkpoint invalidation", error)
             : LC_OK;
}

static int file_owner_finish(void *context, lc_error *error) {
  lc_collection_handle *collection = context;
  int valid, rc = LC_OK;
  if (rc == LC_OK && collection->join_fd >= 0)
    rc = lc_cc_lock(collection->join_fd, LOCK_EX, error);
  /* Upgrade is nonblocking: other live handles retain the shared session.
   * Only the last handle can publish a clean-close proof. */
  if (rc == LC_OK && collection->session_fd >= 0 && collection->trusted &&
      flock(collection->session_fd, LOCK_EX | LOCK_NB) == 0) {
    rc = lc_cc_lock(collection->append_fd, LOCK_EX, error);
    if (rc == LC_OK) {
      rc = lc_collection_metadata_enter(collection, 1, error);
      if (rc == LC_OK) {
        rc = lc_collection_reconcile(collection, error);
        if (rc == LC_OK)
          rc = lc_cc_checkpoint(collection, 1, &valid, error);
        lc_collection_metadata_leave(collection);
      }
      (void)flock(collection->append_fd, LOCK_UN);
    }
  }
  return rc;
}
static int file_owner_attach(void *context, lc_collection_handle *collection,
                             lc_error *error) {
  (void)context;
  (void)error;
  lc_collection_file_bind(collection);
  return LC_OK;
}
static int file_owner_clone(void *context, lc_collection **out,
                            lc_error *error) {
  lc_collection_handle *c = context;
  return lc_collection_storage_open(c->pouch, c->client, c->ns, c->name, c->id,
                                    c->catalog_base, c->catalog_path, 0, 0, 0,
                                    out, error);
}
static int file_owner_bound(void *context, lc_error *error) {
  return lc_collection_file_catalog_bound(context, error);
}
static int file_owner_remove(void *context, lc_error *error) {
  return lc_collection_file_catalog_remove(context, error);
}
static void file_owner_release(void *context) { (void)context; }
lc_collection_owner lc_collection_file_owner(lc_collection_handle *c) {
  static const lc_collection_owner_ops ops = {
      file_owner_attach, file_owner_clone,  file_owner_bound,
      file_owner_remove, file_owner_finish, file_owner_release};
  lc_collection_owner owner;
  owner.ops = &ops;
  owner.context = c;
  return owner;
}

static int lc_cc_file_rotate(lc_collection_handle *collection, int force,
                             lc_u64 limit, lc_error *error) {
  char leaf[64];
  struct stat st;
  lc_u64 next;
  int fd = -1, directory = -1;
  int rc;
  lc_collection_segment fresh;
  memset(&fresh, 0, sizeof(fresh));
  if (fstat(collection->active_fd, &st) != 0 || st.st_size < 0)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to inspect collection segment size");
  if (!force && (lc_u64)st.st_size < limit)
    return LC_OK;
  next = collection->head.active + 1U;
  if (next >= LC_COLLECTION_SNAPSHOT_BIT)
    return lc_collection_failure(error, LC_ERR_INVALID, "sequence_exhausted",
                                 "collection segment identities are exhausted");
  /* Seal canonical history before naming another segment. Projections remain
   * rebuildable from that history; only a clean checkpoint promises their
   * persistence and syncs them before publishing its marker. Even a segment
   * containing only abandoned reservations needs durable framing: replay must
   * be able to traverse it to reach acknowledged entries in its successors.
   * Private writers still seal later writes before publishing cross-inode
   * references; this boundary sync cannot cover writes made after rotation. */
  if (
#ifdef LOCKDC_TEST_BUILD
      (lc_collection_test_rotate_sync_fd
           ? lc_collection_test_rotate_sync_fd(collection->active_fd)
           : fsync(collection->active_fd)) != 0)
#else
      fsync(collection->active_fd) != 0)
#endif
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to sync collection segment boundary");
  rc = lc_collection_record_leaf(next, leaf, error);
  if (rc != LC_OK)
    return rc;
  directory = open(collection->directory, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (directory < 0)
    return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to open collection segment directory");
  fd = openat(directory, leaf, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0 && errno == EEXIST) {
    fd = openat(directory, leaf, O_RDWR | O_CLOEXEC);
    /* A crash can leave a successor inode before head publication. This
     * successor is outside the committed topology and can be reclaimed under
     * current append authority. */
    if (fd >= 0 && ftruncate(fd, 0) != 0) {
      (void)close(fd);
      fd = -1;
      rc = lc_collection_failure(
          error, LC_ERR_TRANSPORT, NULL,
          "failed to reclaim unpublished collection segment");
    }
  }
  if (fd < 0 && rc == LC_OK)
    rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                               "failed to create collection segment");
  if (rc == LC_OK && fsync(fd) != 0)
    rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                               "failed to sync new collection segment");
  if (rc == LC_OK) {
    rc = lc_collection_segment_adopt_file(fd, &fresh, error);
    if (rc != LC_OK)
      fd = -1;
  }
  if (rc == LC_OK) {
    collection->head.active = next;
    rc = lc_collection_head_write(collection, error);
  }
  if (rc == LC_OK) {
    lc_collection_segment_close(&collection->active);
    collection->active = fresh;
    memset(&fresh, 0, sizeof(fresh));
    collection->active_fd = fd;
    collection->active_fd_segment = next;
    fd = -1;
  }
  if (fresh.ops) {
    lc_collection_segment_close(&fresh);
    fd = -1;
  }
  if (fd >= 0)
    (void)close(fd);
  (void)close(directory);
  if (rc != LC_OK)
    collection->head_loaded = 0;
  return rc;
}

static int file_layout_rotate(void *context, int force, lc_u64 limit,
                              lc_error *error) {
  return lc_cc_file_rotate(context, force, limit, error);
}
static int file_layout_discard(void *context, lc_u64 id, lc_error *error) {
  lc_collection_handle *c = context;
  char *path = NULL, leaf[64];
  int rc = lc_collection_record_path(c, id, &path, leaf, error);
  if (rc == LC_OK && unlink(path) != 0 && errno != ENOENT)
    rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                               "failed to discard collection snapshot");
  lc_free_with_allocator(&c->allocator, path);
  if (rc == LC_OK)
    rc = lc_pouch_path_fsync_directory(
        c->directory, "failed to sync snapshot discard", error);
  return rc;
}
static int file_layout_cleanup(void *context, lc_pouch_maintenance_result *out,
                               lc_error *error) {
  return lc_collection_file_cleanup(context, out, error);
}
lc_collection_layout lc_collection_file_layout(lc_collection_handle *c) {
  lc_collection_layout layout = {0};
  layout.active = file_layout_active;
  layout.rotate = file_layout_rotate;
  layout.discard = file_layout_discard;
  layout.cleanup = file_layout_cleanup;
  layout.context = c;
  return layout;
}

int lc_collection_open_bound(lc_client_handle *client, const char *ns,
                             const char *name, const char *id, const char *base,
                             const char *catalog, int create,
                             lc_pouch_unix_seconds retention,
                             lc_u64 max_entries, lc_collection **out,
                             lc_error *error) {
  return lc_collection_storage_open(client->pouch, client, ns, name, id, base,
                                    catalog, create, retention, max_entries,
                                    out, error);
}

int lc_collection_storage_open(lc_pouch *pouch, lc_client_handle *client,
                               const char *ns, const char *name, const char *id,
                               const char *base, const char *catalog,
                               int create, lc_pouch_unix_seconds retention,
                               lc_u64 max_entries, lc_collection **out,
                               lc_error *error) {
  lc_collection_handle *collection;
  const lc_allocator *allocator;
  lc_collection_guard guard;
  lc_collection_guard root_guard;
  lc_collection_call scope;
  lc_pouch_unix_seconds now;
  char *data;
  int first;
  int valid;
  int rc;
  int entered;
  allocator = &pouch->allocator;
  collection = (lc_collection_handle *)lc_calloc_with_allocator(
      allocator, 1U, sizeof(*collection));
  if (collection == NULL)
    return lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                 "failed to allocate collection receiver");
  lc_collection_file_bind(collection);
  collection->client = client;
  collection->pouch = pouch;
  collection->allocator = *allocator;
  if (client != NULL)
    lc_client_handle_retain(client);
  collection->session_fd = collection->join_fd = collection->metadata_fd =
      collection->append_fd = collection->active_fd = collection->head_fd =
          collection->streams_fd = -1;
  collection->order.fd = collection->ids.fd = -1;
  collection->native_reads.fd = -1;
  collection->references = 1UL;
  memcpy(collection->id, id, sizeof(collection->id));
  collection->ns = lc_strdup_with_allocator(allocator, ns);
  collection->name = lc_strdup_with_allocator(allocator, name);
  collection->catalog_path = lc_strdup_with_allocator(allocator, catalog);
  collection->catalog_base = lc_strdup_with_allocator(allocator, base);
  data = lc_pouch_path_join(allocator, base, "data");
  if (data != NULL)
    collection->directory = lc_pouch_path_join(allocator, data, id);
  lc_free_with_allocator(allocator, data);
  if (collection->directory != NULL) {
    collection->head_path = lc_cc_path(collection, "head");
    collection->order_path = lc_cc_path(collection, "order");
    collection->ids_path = lc_cc_path(collection, "ids");
  }
  rc = LC_OK;
  entered = 0;
  if (collection->ns == NULL || collection->name == NULL ||
      collection->directory == NULL || collection->catalog_path == NULL ||
      collection->catalog_base == NULL || collection->head_path == NULL ||
      collection->order_path == NULL || collection->ids_path == NULL)
    rc = lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                               "failed to allocate collection identity paths");
  if (rc == LC_OK) {
    rc = lc_collection_root_begin(pouch, &scope, &root_guard, error);
    entered = rc == LC_OK;
  }
  if (rc == LC_OK && create)
    rc = lc_collection_maintenance_note(pouch, ns, error);
  if (rc == LC_OK && create && mkdir(collection->directory, 0700) != 0)
    rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                               "failed to create collection partition");
  if (rc == LC_OK && create)
    lc_pouch_compaction_note_mutation(pouch, ns, 0);
  if (rc == LC_OK) {
    lc_collection_file_reads_init(&collection->native_reads,
                                  collection->directory);
    collection->native_reads.fd =
        open(collection->directory, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (collection->native_reads.fd < 0)
      rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to pin collection read directory");
    else
      collection->reads =
          lc_collection_file_reads_access(&collection->native_reads);
  }
  if (rc == LC_OK) {
    if (pthread_mutex_init(&collection->metadata_mutex, NULL) != 0)
      rc = lc_collection_failure(
          error, LC_ERR_TRANSPORT, NULL,
          "failed to initialize collection metadata mutex");
    else
      collection->metadata_mutex_initialized = 1;
  }
  if (rc == LC_OK) {
    if (pthread_mutex_init(&collection->append_mutex, NULL) != 0)
      rc =
          lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                "failed to initialize collection append mutex");
    else
      collection->append_mutex_initialized = 1;
  }
  if (rc == LC_OK)
    rc = lc_cc_open_file(collection, ".session.lock", O_RDWR | O_CREAT,
                         &collection->session_fd, error);
  if (rc == LC_OK)
    rc = lc_cc_open_file(collection, ".join.lock", O_RDWR | O_CREAT,
                         &collection->join_fd, error);
  if (rc == LC_OK)
    rc = lc_cc_open_file(collection, ".metadata.lock", O_RDWR | O_CREAT,
                         &collection->metadata_fd, error);
  if (rc == LC_OK)
    rc = lc_cc_open_file(collection, ".append.lock", O_RDWR | O_CREAT,
                         &collection->append_fd, error);
  first = 0;
  if (rc == LC_OK)
    rc = lc_cc_lock(collection->join_fd, LOCK_EX, error);
  if (rc == LC_OK) {
    if (flock(collection->session_fd, LOCK_EX | LOCK_NB) == 0)
      first = 1;
    else if (errno == EWOULDBLOCK || errno == EAGAIN)
      rc = lc_cc_lock(collection->session_fd, LOCK_SH, error);
    else
      rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to enter collection session");
  }
  if (rc == LC_OK && (first || create))
    rc = lc_cc_root_enter(collection->pouch, NULL, &guard, error);
  if (rc == LC_OK && (first || create)) {
    rc = lc_cc_lock(collection->append_fd, LOCK_EX, error);
    if (rc == LC_OK) {
      rc = lc_collection_metadata_enter(collection, 1, error);
      if (rc == LC_OK) {
        if (create) {
          collection->head.max_entries = max_entries;
          collection->head.active = collection->head.tail_segment = 1U;
          rc = lc_cc_open_file(collection, "seg-00000000000000000001.log",
                               O_RDWR | O_CREAT | O_EXCL,
                               &collection->active_fd, error);
          collection->active_fd_segment = 1U;
          if (rc == LC_OK) {
            rc = lc_collection_segment_adopt_file(collection->active_fd,
                                                  &collection->active, error);
            if (rc != LC_OK)
              collection->active_fd = -1;
          }
          if (rc == LC_OK && fsync(collection->active_fd) != 0)
            rc = lc_collection_failure(
                error, LC_ERR_TRANSPORT, NULL,
                "failed to sync initial collection segment");
          if (rc == LC_OK)
            rc = lc_pouch_now_unix(&now, error);
          if (rc == LC_OK)
            rc = lc_pouch_retention_resolve(retention, now,
                                            &collection->head.retention, error);
          if (rc == LC_OK)
            rc = lc_collection_head_write(collection, error);
          if (rc == LC_OK)
            rc = lc_collection_order_open(collection->order_path, 1,
                                          &collection->order, error);
          if (rc == LC_OK)
            rc = lc_collection_ids_open(collection->ids_path, 1, allocator,
                                        &collection->ids, error);
          if (rc == LC_OK)
            rc = lc_collection_order_checkpoint(&collection->order, 1U, 0U,
                                                error);
          if (rc == LC_OK)
            rc = lc_collection_ids_checkpoint(&collection->ids, 0U, 0U, 1U, 0U,
                                              error);
          if (rc == LC_OK)
            collection->trusted = 1;
        } else {
          rc = lc_collection_head_read(collection, error);
          valid = 0;
          if (rc == LC_OK && first) {
            rc = lc_cc_order_refresh(collection, error);
            if (rc == LC_OK)
              rc = lc_collection_ids_open(collection->ids_path, 0, allocator,
                                          &collection->ids, error);
            if (rc == LC_OK)
              rc = lc_cc_checkpoint(collection, 0, &valid, error);
            else if (rc == LC_ERR_PROTOCOL ||
                     (rc == LC_ERR_TRANSPORT && errno == ENOENT)) {
              lc_error_cleanup(error);
              lc_error_init(error);
              rc = LC_OK;
            }
          }
          collection->trusted = !first || valid;
          if (rc == LC_OK)
            rc = lc_collection_reconcile(collection, error);
        }
        if (rc == LC_OK && first)
          rc = lc_cc_invalidate_clean(collection, error);
        lc_collection_metadata_leave(collection);
      }
      (void)flock(collection->append_fd, LOCK_UN);
    }
    lc_cc_root_leave(&guard);
  }
  if (rc == LC_OK && !first && !create) {
    collection->trusted = 1;
    rc = lc_collection_prepare(collection, error);
    if (rc == LC_OK) {
      rc = lc_collection_metadata_enter(collection, 0, error);
      if (rc == LC_OK) {
        rc = lc_collection_refresh_view(collection, error);
        if (rc == LC_OK && collection->ids.fd < 0)
          rc = lc_collection_ids_open(collection->ids_path, 0, allocator,
                                      &collection->ids, error);
        lc_collection_metadata_leave(collection);
      }
    }
  }
  if (rc == LC_OK && first)
    rc = lc_cc_lock(collection->session_fd, LOCK_SH, error);
  if (collection->join_fd >= 0)
    (void)flock(collection->join_fd, LOCK_UN);
  if (rc == LC_OK && create) {
    data = lc_pouch_path_join(allocator, base, "data");
    if (data == NULL)
      rc = lc_collection_failure(
          error, LC_ERR_NOMEM, NULL,
          "failed to allocate collection partition parent");
    else
      rc = lc_pouch_path_fsync_directory(
          data, "failed to sync collection partition creation", error);
    lc_free_with_allocator(allocator, data);
    if (rc == LC_OK) {
      data = lc_cc_path(collection, "identity");
      if (data == NULL)
        rc = lc_collection_failure(
            error, LC_ERR_NOMEM, NULL,
            "failed to allocate collection identity path");
      else
        rc = lc_collection_catalog_write(data, id, name, error);
      lc_free_with_allocator(allocator, data);
    }
  }
  if (entered)
    lc_collection_root_end(&scope, &root_guard);
  if (rc != LC_OK) {
    collection->trusted = 0;
    lc_collection_destroy(collection);
    return rc;
  }
  lc_cc_bind_methods(collection);
  *out = &collection->pub;
  return LC_OK;
}
