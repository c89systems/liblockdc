/* Included by lc_pouch_state.c for the writable multifile receiver. */

static int lc_pouch_state_candidate_open_error(const char *path,
                                               const char *phase, int cause,
                                               lc_error *error) {
  const char *reason = strerror(cause);
  size_t capacity = strlen(path) + strlen(phase) + strlen(reason) + 64U;
  char *detail = lc_alloc_with_allocator(NULL, capacity);
  int rc;
  if (detail != NULL)
    (void)snprintf(detail, capacity, "phase=%s errno=%d path=%s: %s", phase,
                   cause, path, reason);
  rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                    "failed to open pouch compaction candidate",
                    detail != NULL ? detail : reason, NULL, "pouch");
  lc_free_with_allocator(NULL, detail);
  return rc;
}

static int lc_pouch_state_file_checksum(const char *path, const char *phase,
                                        uint64_t expected_size,
                                        unsigned char *buffer,
                                        unsigned long *checksum,
                                        lc_error *error) {
  struct stat st;
  uint64_t read_total;
  unsigned long value;
  ssize_t got;
  int fd;

  if (path == NULL || buffer == NULL || checksum == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state checksum requires path, buffer and output",
                        NULL, NULL, NULL);
  }
#ifdef LOCKDC_TEST_BUILD
  if (lc_pouch_test_candidate_open_errno != NULL) {
    int cause = lc_pouch_test_candidate_open_errno(path, phase);
    if (cause != 0) {
      errno = cause;
      fd = -1;
    } else
      fd = open(path, O_RDONLY);
  } else
#endif
    fd = open(path, O_RDONLY);
  if (fd < 0) {
    return lc_pouch_state_candidate_open_error(path, phase, errno, error);
  }
  if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 0 ||
      (uint64_t)st.st_size != expected_size) {
    (void)close(fd);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch compaction candidate changed", NULL, NULL,
                        "pouch");
  }
  value = (unsigned long)crc32(0L, Z_NULL, 0);
  read_total = 0U;
  for (;;) {
    got = read(fd, buffer, LC_POUCH_STATE_FILE_CHUNK_BYTES);
    if (got < 0) {
      int saved_errno;

      saved_errno = errno;
      (void)close(fd);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to read pouch compaction candidate",
                          strerror(saved_errno), NULL, "pouch");
    }
    if (got == 0) {
      break;
    }
    if ((uint64_t)got > expected_size - read_total) {
      (void)close(fd);
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch compaction candidate changed", NULL, NULL,
                          "pouch");
    }
    value = (unsigned long)crc32((uLong)value, buffer, (uInt)got);
    read_total += (uint64_t)got;
  }
  if (fstat(fd, &st) != 0 || st.st_size < 0 ||
      (uint64_t)st.st_size != expected_size || read_total != expected_size) {
    (void)close(fd);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch compaction candidate changed", NULL, NULL,
                        "pouch");
  }
  if (close(fd) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch compaction candidate",
                        strerror(errno), NULL, "pouch");
  }
  *checksum = value;
  return LC_OK;
}

static int lc_pouch_state_read_text_file(lc_pouch *pouch, const char *path,
                                         char **out, size_t *out_length,
                                         lc_error *error) {
  char buffer[4096];
  char *bytes;
  FILE *fp;
  size_t capacity;
  size_t length;
  int rc;

  *out = NULL;
  fp = fopen(path, "rb");
  if (fp == NULL) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch text file", strerror(errno), path,
                        NULL);
  }
  bytes = NULL;
  capacity = 0U;
  length = 0U;
  rc = LC_OK;
  while (!feof(fp)) {
    size_t got;

    got = fread(buffer, 1U, sizeof(buffer), fp);
    if (got != 0U) {
      if (length + got + 1U < length) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch text file", NULL, path,
                          NULL);
        break;
      }
      if (length + got + 1U > capacity) {
        char *grown;
        size_t next_capacity;

        next_capacity = capacity == 0U ? 4096U : capacity;
        while (next_capacity < length + got + 1U) {
          if (next_capacity > ((size_t)-1) / 2U) {
            rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                              "failed to allocate pouch text file", NULL, path,
                              NULL);
            break;
          }
          next_capacity *= 2U;
        }
        if (rc != LC_OK) {
          break;
        }
        grown = (char *)lc_realloc_with_allocator(&pouch->allocator, bytes,
                                                  next_capacity);
        if (grown == NULL) {
          rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch text file", NULL, path,
                            NULL);
          break;
        }
        bytes = grown;
        capacity = next_capacity;
      }
      memcpy(bytes + length, buffer, got);
      length += got;
    }
    if (ferror(fp)) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch text file", strerror(errno), path,
                        NULL);
      break;
    }
  }
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch text file", strerror(errno), path,
                      NULL);
  }
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    return rc;
  }
  if (bytes == NULL) {
    bytes = lc_strdup_with_allocator(&pouch->allocator, "");
    if (bytes == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch text file", NULL, path,
                          NULL);
    }
  } else {
    bytes[length] = '\0';
  }
  *out = bytes;
  if (out_length != NULL) {
    *out_length = length;
  }
  return LC_OK;
}

static void lc_pouch_state_compaction_capture_cleanup(
    lc_pouch *pouch, lc_pouch_state_compaction_capture *capture) {
  size_t index;

  if (capture == NULL) {
    return;
  }
  lc_free_with_allocator(&pouch->allocator, capture->manifest_text);
  lc_free_with_allocator(&pouch->allocator, capture->io_buffer);
  for (index = 0U; index < capture->candidate_count; ++index) {
    lc_free_with_allocator(&pouch->allocator, capture->candidate_leaves[index]);
  }
  lc_free_with_allocator(&pouch->allocator, capture->candidate_leaves);
  lc_free_with_allocator(&pouch->allocator, capture->candidate_snapshots);
  lc_free_with_allocator(&pouch->allocator, capture->candidate_sizes);
  lc_free_with_allocator(&pouch->allocator, capture->candidate_checksums);
  for (index = 0U; index < capture->captured_count; ++index) {
    lc_free_with_allocator(&pouch->allocator, capture->captured_keys[index]);
    lc_free_with_allocator(&pouch->allocator,
                           capture->captured_record_containers[index]);
  }
  lc_free_with_allocator(&pouch->allocator, capture->captured_keys);
  lc_free_with_allocator(&pouch->allocator,
                         capture->captured_record_containers);
  lc_free_with_allocator(&pouch->allocator, capture->captured_record_offsets);
  memset(capture, 0, sizeof(*capture));
}

static int lc_pouch_state_compaction_candidate_index(
    const lc_pouch_state_compaction_capture *capture, const char *leaf) {
  size_t index;

  if (capture == NULL || leaf == NULL) {
    return -1;
  }
  for (index = 0U; index < capture->candidate_count; ++index) {
    if (capture->candidate_leaves[index] != NULL &&
        strcmp(capture->candidate_leaves[index], leaf) == 0) {
      return (int)index;
    }
  }
  return -1;
}

static int lc_pouch_state_compaction_candidate_contains(
    const lc_pouch_state_compaction_capture *capture, const char *leaf) {
  return lc_pouch_state_compaction_candidate_index(capture, leaf) >= 0;
}

static int lc_pouch_state_compaction_candidate_add(
    lc_pouch *pouch, lc_pouch_state_compaction_capture *capture,
    const lc_pouch_namespace_manifest *manifest, const char *leaf, int snapshot,
    lc_error *error) {
  char *path;
  char *leaf_copy;
  uint64_t size = 0U;
  int rc;

  if (capture == NULL || leaf == NULL || leaf[0] == '\0') {
    return LC_OK;
  }
  if (lc_pouch_state_compaction_candidate_contains(capture, leaf)) {
    return LC_OK;
  }
  if (capture->candidate_count >= capture->candidate_capacity) {
    char **next_leaves;
    int *next_snapshots;
    uint64_t *next_sizes;
    unsigned long *next_checksums;
    size_t next_capacity;

    next_capacity = capture->candidate_capacity == 0U
                        ? 8U
                        : capture->candidate_capacity * 2U;
    if (next_capacity <= capture->candidate_capacity) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch compaction candidate list exceeds local limit",
                          NULL, NULL, NULL);
    }
    next_leaves = (char **)lc_realloc_with_allocator(
        &pouch->allocator, capture->candidate_leaves,
        next_capacity * sizeof(capture->candidate_leaves[0]));
    if (next_leaves == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction candidates",
                          NULL, NULL, NULL);
    }
    capture->candidate_leaves = next_leaves;
    next_snapshots = (int *)lc_realloc_with_allocator(
        &pouch->allocator, capture->candidate_snapshots,
        next_capacity * sizeof(capture->candidate_snapshots[0]));
    if (next_snapshots == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction candidate "
                          "types",
                          NULL, NULL, NULL);
    }
    capture->candidate_snapshots = next_snapshots;
    next_sizes = (uint64_t *)lc_realloc_with_allocator(
        &pouch->allocator, capture->candidate_sizes,
        next_capacity * sizeof(capture->candidate_sizes[0]));
    if (next_sizes == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction candidate "
                          "sizes",
                          NULL, NULL, NULL);
    }
    capture->candidate_sizes = next_sizes;
    next_checksums = (unsigned long *)lc_realloc_with_allocator(
        &pouch->allocator, capture->candidate_checksums,
        next_capacity * sizeof(capture->candidate_checksums[0]));
    if (next_checksums == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction candidate "
                          "checksums",
                          NULL, NULL, NULL);
    }
    capture->candidate_checksums = next_checksums;
    capture->candidate_capacity = next_capacity;
  }
  leaf_copy = lc_strdup_with_allocator(&pouch->allocator, leaf);
  if (leaf_copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch compaction candidate", NULL, NULL,
                        NULL);
  }
  path = lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                   snapshot ? "snapshots" : "segments", leaf);
  if (path == NULL) {
    lc_free_with_allocator(&pouch->allocator, leaf_copy);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch compaction candidate path",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_state_file_size(path, &size, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_file_checksum(
        path, "capture", size, capture->io_buffer,
        &capture->candidate_checksums[capture->candidate_count], error);
  }
  lc_free_with_allocator(&pouch->allocator, path);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, leaf_copy);
    return rc;
  }
  if (size > LC_U64_MAX - capture->candidate_bytes) {
    lc_free_with_allocator(&pouch->allocator, leaf_copy);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch compaction candidate bytes overflow", NULL, NULL,
                        "pouch");
  }
  capture->candidate_leaves[capture->candidate_count] = leaf_copy;
  capture->candidate_snapshots[capture->candidate_count] = snapshot ? 1 : 0;
  capture->candidate_sizes[capture->candidate_count] = size;
  ++capture->candidate_count;
  capture->candidate_bytes += size;
  return LC_OK;
}

static int lc_pouch_state_compaction_build_candidates(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    lc_pouch_state_compaction_capture *capture, lc_error *error) {
  unsigned long segment_index;
  int rc;

  if (manifest->latest_snapshot != NULL &&
      !lc_pouch_state_manifest_leaf_obsolete(manifest,
                                             manifest->latest_snapshot, 1)) {
    rc = lc_pouch_state_compaction_candidate_add(
        pouch, capture, manifest, manifest->latest_snapshot, 1, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  for (segment_index = 0UL; segment_index < manifest->segment_count;
       ++segment_index) {
    const char *segment_leaf = manifest->segment_leaves[segment_index];

    if (!lc_pouch_state_manifest_segment_visible(manifest, segment_leaf) ||
        (manifest->active_segment != NULL &&
         strcmp(segment_leaf, manifest->active_segment) == 0)) {
      continue;
    }
    rc = lc_pouch_state_compaction_candidate_add(pouch, capture, manifest,
                                                 segment_leaf, 0, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return LC_OK;
}

static void lc_pouch_state_compaction_candidate_remove_at(
    lc_pouch *pouch, lc_pouch_state_compaction_capture *capture, size_t index) {
  if (capture == NULL || index >= capture->candidate_count) {
    return;
  }
  lc_free_with_allocator(&pouch->allocator, capture->candidate_leaves[index]);
  if (capture->candidate_bytes >= capture->candidate_sizes[index]) {
    capture->candidate_bytes -= capture->candidate_sizes[index];
  } else {
    capture->candidate_bytes = 0UL;
  }
  if (index + 1U < capture->candidate_count) {
    memmove(capture->candidate_leaves + index,
            capture->candidate_leaves + index + 1U,
            (capture->candidate_count - index - 1U) *
                sizeof(capture->candidate_leaves[0]));
    memmove(capture->candidate_snapshots + index,
            capture->candidate_snapshots + index + 1U,
            (capture->candidate_count - index - 1U) *
                sizeof(capture->candidate_snapshots[0]));
    memmove(capture->candidate_sizes + index,
            capture->candidate_sizes + index + 1U,
            (capture->candidate_count - index - 1U) *
                sizeof(capture->candidate_sizes[0]));
    memmove(capture->candidate_checksums + index,
            capture->candidate_checksums + index + 1U,
            (capture->candidate_count - index - 1U) *
                sizeof(capture->candidate_checksums[0]));
  }
  --capture->candidate_count;
}

static int lc_pouch_state_compaction_protect_live_links(
    lc_pouch *pouch, lc_pouch_state_compaction_capture *capture,
    lc_pouch_namespace_logstore *cache, int *blocked, lc_error *error) {
  lc_pouch_state_cache_record *record;

  (void)error;
  if (blocked != NULL) {
    *blocked = 0;
  }
  for (record = cache->records; record != NULL; record = record->next) {
    int payload_candidate;
    int record_candidate;

    if (!record->payload_span.present ||
        !lc_pouch_state_compaction_candidate_contains(
            capture, record->payload_span.container_leaf)) {
      continue;
    }
    record_candidate =
        record->has_record_ref && lc_pouch_state_compaction_candidate_contains(
                                      capture, record->record_container_leaf);
    if (record_candidate) {
      continue;
    }
    payload_candidate = lc_pouch_state_compaction_candidate_index(
        capture, record->payload_span.container_leaf);
    if (payload_candidate < 0) {
      continue;
    }
    if (capture->candidate_snapshots[payload_candidate]) {
      if (blocked != NULL) {
        *blocked = 1;
      }
      return LC_OK;
    }
    lc_pouch_state_compaction_candidate_remove_at(pouch, capture,
                                                  (size_t)payload_candidate);
  }
  return LC_OK;
}

static int lc_pouch_state_compaction_capture_record_add(
    lc_pouch *pouch, lc_pouch_state_compaction_capture *capture,
    const lc_pouch_state_cache_record *record, lc_error *error) {
  char *key_copy;
  char *container_copy;

  if (capture->captured_count >= capture->captured_capacity) {
    char **next_keys;
    char **next_containers;
    uint64_t *next_offsets;
    size_t next_capacity;

    next_capacity = capture->captured_capacity == 0U
                        ? 32U
                        : capture->captured_capacity * 2U;
    if (next_capacity <= capture->captured_capacity) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch compaction captured refs exceed local limit",
                          NULL, NULL, NULL);
    }
    next_keys = (char **)lc_realloc_with_allocator(
        &pouch->allocator, capture->captured_keys,
        next_capacity * sizeof(capture->captured_keys[0]));
    if (next_keys == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction captured keys",
                          NULL, NULL, NULL);
    }
    capture->captured_keys = next_keys;
    next_containers = (char **)lc_realloc_with_allocator(
        &pouch->allocator, capture->captured_record_containers,
        next_capacity * sizeof(capture->captured_record_containers[0]));
    if (next_containers == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction captured "
                          "containers",
                          NULL, NULL, NULL);
    }
    capture->captured_record_containers = next_containers;
    next_offsets = (uint64_t *)lc_realloc_with_allocator(
        &pouch->allocator, capture->captured_record_offsets,
        next_capacity * sizeof(capture->captured_record_offsets[0]));
    if (next_offsets == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction captured "
                          "offsets",
                          NULL, NULL, NULL);
    }
    capture->captured_record_offsets = next_offsets;
    capture->captured_capacity = next_capacity;
  }
  key_copy = lc_strdup_with_allocator(&pouch->allocator, record->key);
  container_copy = lc_strdup_with_allocator(&pouch->allocator,
                                            record->record_container_leaf);
  if (key_copy == NULL || container_copy == NULL) {
    lc_free_with_allocator(&pouch->allocator, key_copy);
    lc_free_with_allocator(&pouch->allocator, container_copy);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch compaction captured ref", NULL,
                        NULL, NULL);
  }
  capture->captured_keys[capture->captured_count] = key_copy;
  capture->captured_record_containers[capture->captured_count] = container_copy;
  capture->captured_record_offsets[capture->captured_count] =
      record->record_offset;
  ++capture->captured_count;
  return LC_OK;
}

static int lc_pouch_state_compaction_capture_records(
    lc_pouch *pouch, lc_pouch_state_compaction_capture *capture,
    lc_pouch_namespace_logstore *cache, lc_error *error) {
  lc_pouch_state_cache_record *record;
  int rc = LC_OK;
  for (record = lc_pouch_state_order_bound(cache, NULL, 0);
       rc == LC_OK && record; record = lc_pouch_state_order_next(record)) {
    if (!record->found || !record->has_record_ref) {
      continue;
    }
    if (!lc_pouch_state_compaction_candidate_contains(
            capture, record->record_container_leaf)) {
      continue;
    }
    rc = lc_pouch_state_compaction_capture_record_add(pouch, capture, record,
                                                      error);
  }
  return rc;
}

static int lc_pouch_state_compaction_capture_now(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    lc_pouch_state_compaction_capture *capture, lc_error *error) {
  char *manifest_path;
  int rc;

  memset(capture, 0, sizeof(*capture));
  manifest_path = lc_pouch_path_join(&pouch->allocator,
                                     manifest->namespace_path, "manifest");
  if (manifest_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace manifest path",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_state_read_text_file(pouch, manifest_path,
                                     &capture->manifest_text, NULL, error);
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  if (rc != LC_OK) {
    return rc;
  }
  capture->io_buffer = (unsigned char *)lc_alloc_with_allocator(
      &pouch->allocator, LC_POUCH_STATE_FILE_CHUNK_BYTES);
  if (capture->io_buffer == NULL) {
    lc_pouch_state_compaction_capture_cleanup(pouch, capture);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch compaction I/O buffer", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_state_compaction_build_candidates(pouch, manifest, capture,
                                                  error);
  if (rc != LC_OK) {
    lc_pouch_state_compaction_capture_cleanup(pouch, capture);
  }
  return rc;
}

static int lc_pouch_state_compaction_validate_candidates(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    const lc_pouch_state_compaction_capture *capture, lc_error *error) {
  size_t index;

  if (pouch == NULL || manifest == NULL || capture == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch compaction candidate validation requires "
                        "inputs",
                        NULL, NULL, NULL);
  }
  for (index = 0U; index < capture->candidate_count; ++index) {
    char *path;
    unsigned long checksum;
    int rc;

    path = lc_pouch_state_child_path(
        &pouch->allocator, manifest->namespace_path,
        capture->candidate_snapshots[index] ? "snapshots" : "segments",
        capture->candidate_leaves[index]);
    if (path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction candidate path",
                          NULL, NULL, NULL);
    }
    checksum = 0UL;
    rc = lc_pouch_state_file_checksum(path, "validation",
                                      capture->candidate_sizes[index],
                                      capture->io_buffer, &checksum, error);
    lc_free_with_allocator(&pouch->allocator, path);
    if (rc != LC_OK) {
      return rc;
    }
    if (checksum != capture->candidate_checksums[index]) {
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch compaction validation drift", NULL, NULL,
                          "pouch");
    }
  }
  return LC_OK;
}

static int lc_pouch_state_compaction_validate_capture(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    const lc_pouch_state_compaction_capture *before,
    lc_pouch_namespace_logstore *cache, lc_error *error) {
  char *manifest_path;
  char *manifest_text;
  size_t index;
  int rc;

  manifest_text = NULL;
  manifest_path = lc_pouch_path_join(&pouch->allocator,
                                     manifest->namespace_path, "manifest");
  if (manifest_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace manifest path",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_state_read_text_file(pouch, manifest_path, &manifest_text, NULL,
                                     error);
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  if (rc != LC_OK) {
    return rc;
  }
  if (before->manifest_text == NULL || manifest_text == NULL ||
      strcmp(before->manifest_text, manifest_text) != 0) {
    lc_free_with_allocator(&pouch->allocator, manifest_text);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch compaction validation drift", NULL, NULL,
                        "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, manifest_text);
  rc = lc_pouch_state_compaction_validate_candidates(pouch, manifest, before,
                                                     error);
  if (rc != LC_OK) {
    return rc;
  }
  for (index = 0U; index < before->captured_count; ++index) {
    lc_pouch_state_cache_record *record;

    record =
        lc_pouch_state_cache_record_find(cache, before->captured_keys[index]);
    if (!lc_pouch_state_compaction_record_ref_matches(
            record, before->captured_record_containers[index],
            before->captured_record_offsets[index])) {
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch compaction validation drift", NULL, NULL,
                          "pouch");
    }
  }
  return LC_OK;
}

static void lc_pouch_maintenance_set_diagnostic_best_effort(
    lc_pouch *pouch, lc_pouch_maintenance_result *out, const char *diagnostic) {
  char *copy;

  if (out == NULL) {
    return;
  }
  copy = lc_strdup_with_allocator(&pouch->allocator,
                                  diagnostic != NULL ? diagnostic : "");
  if (copy == NULL) {
    return;
  }
  lc_free_with_allocator(&pouch->allocator, out->diagnostic);
  out->diagnostic = copy;
}

static void lc_pouch_maintenance_mark_aborted(lc_pouch *pouch,
                                              lc_pouch_maintenance_result *out,
                                              const char *diagnostic) {
  if (out == NULL) {
    return;
  }
  out->aborted = 1;
  out->compacted = 0;
  out->skipped = 0;
  lc_pouch_maintenance_set_diagnostic_best_effort(
      pouch, out, diagnostic != NULL ? diagnostic : "compaction-aborted");
}

static int lc_pouch_state_compact_namespace(
    lc_pouch *pouch, const char *ns, lc_pouch_namespace_manifest *manifest,
    unsigned long *cleanup_deleted_count, unsigned long *cleanup_pending_count,
    const char **abort_diagnostic, int *compacted_out, lc_error *error);

static int lc_pouch_state_queue_compacted_files(
    lc_pouch *pouch, lc_pouch_namespace_manifest *manifest, const char *ns,
    const lc_pouch_state_compaction_capture *capture,
    unsigned long *cleanup_deleted_count, unsigned long *cleanup_pending_count,
    lc_error *error);

static int lc_pouch_state_queue_compacted_files(
    lc_pouch *pouch, lc_pouch_namespace_manifest *manifest, const char *ns,
    const lc_pouch_state_compaction_capture *capture,
    unsigned long *cleanup_deleted_count, unsigned long *cleanup_pending_count,
    lc_error *error) {
  size_t index;
  uint64_t now_unix;
  int rc;

  if (capture == NULL) {
    return LC_OK;
  }
  now_unix = (uint64_t)lc_pouch_maintenance_now_seconds();
  if (now_unix == 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch obsolete file timestamp is invalid", NULL, NULL,
                        "pouch");
  }
  for (index = 0U; index < capture->candidate_count; ++index) {
    if (capture->candidate_snapshots[index]) {
      rc = lc_pouch_namespace_manifest_mark_obsolete_snapshot(
          &pouch->allocator, manifest, capture->candidate_leaves[index],
          now_unix, error);
      if (rc != LC_OK) {
        return rc;
      }
    } else {
      rc = lc_pouch_namespace_manifest_mark_obsolete_segment(
          &pouch->allocator, manifest, capture->candidate_leaves[index],
          now_unix, error);
      if (rc != LC_OK) {
        return rc;
      }
    }
  }
  rc = lc_pouch_namespace_manifest_save(&pouch->allocator, ns, manifest, error);
  if (rc != LC_OK) {
    return rc;
  }
  lc_pouch_state_source_cache_cleanup(pouch);
  return lc_pouch_namespace_manifest_cleanup_obsolete(
      &pouch->allocator, ns, manifest, now_unix,
      pouch->compaction_delete_grace_seconds, cleanup_deleted_count,
      cleanup_pending_count, error);
}

static int lc_pouch_state_compact_namespace_if_needed(
    lc_pouch *pouch, const char *ns, lc_pouch_namespace_manifest *manifest,
    int force, int terminal_reclaim, lc_pouch_maintenance_result *out,
    lc_error *error) {
  lc_pouch_generation oldest_history_acknowledged;
  lc_pouch_generation current_history_index_seq;
  unsigned long candidate_count;
  uint64_t candidate_bytes;
  unsigned long cleanup_deleted_count;
  unsigned long cleanup_pending_count;
  uint64_t compacted_segment_id;
  uint64_t now_seconds;
  uint64_t minimum_reclaimable_bytes;
  unsigned long minimum_segment_count;
  lc_pouch_state_compaction_capture capture;
  lc_pouch_namespace_logstore candidate_cache;
  int protected_snapshot_blocked;
  int has_history_consumers;
  int rc;

  if (out != NULL) {
    memset(out, 0, sizeof(*out));
    rc = lc_pouch_maintenance_set_string(pouch, &out->ns, ns, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  minimum_segment_count =
      terminal_reclaim ? 1UL : pouch->compaction_min_segment_count;
  minimum_reclaimable_bytes = terminal_reclaim
                                  ? pouch->terminal_reclaim_min_bytes
                                  : pouch->compaction_min_reclaimable_bytes;
  cleanup_deleted_count = 0UL;
  cleanup_pending_count = 0UL;
  now_seconds = (uint64_t)lc_pouch_maintenance_now_seconds();
  rc = lc_pouch_namespace_manifest_cleanup_obsolete(
      &pouch->allocator, ns, manifest, now_seconds,
      pouch->compaction_delete_grace_seconds, &cleanup_deleted_count,
      &cleanup_pending_count, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (out != NULL) {
    out->cleanup_deleted_count = cleanup_deleted_count;
    out->cleanup_pending_count = cleanup_pending_count;
  }
  if (!force && !pouch->background_compaction_enabled) {
    if (out != NULL) {
      out->skipped = 1;
    }
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_str_field("reason", "disabled");
      lc_log_trace(pouch->logger, "compaction.skip", fields, 2U);
    }
    return lc_pouch_maintenance_set_diagnostic(pouch, out, "disabled", error);
  }
  has_history_consumers = 0;
  oldest_history_acknowledged = 0U;
  current_history_index_seq = 0U;
  rc = lc_pouch_history_oldest_acknowledged(
      pouch, ns, &has_history_consumers, &oldest_history_acknowledged, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (has_history_consumers) {
    rc = lc_pouch_state_index_seq(pouch, ns, &current_history_index_seq, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  if (has_history_consumers &&
      oldest_history_acknowledged < current_history_index_seq) {
    if (out != NULL) {
      out->skipped = 1;
    }
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_u64_field("ack", oldest_history_acknowledged);
      fields[2] = lc_log_u64_field("current", current_history_index_seq);
      lc_log_trace(pouch->logger, "compaction.skip.history_consumer", fields,
                   3U);
    }
    return lc_pouch_maintenance_set_diagnostic(
        pouch, out, "history-consumer-behind", error);
  }
  if (terminal_reclaim && manifest->active_segment_id != LC_U64_MAX) {
    char *active_path;
    uint64_t active_size;

    active_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                  "segments", manifest->active_segment);
    active_size = 0U;
    if (active_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch terminal reclaim segment",
                          NULL, NULL, "pouch");
    }
    rc = lc_pouch_state_file_size(active_path, &active_size, error);
    lc_free_with_allocator(&pouch->allocator, active_path);
    if (rc != LC_OK) {
      return rc;
    }
    if (active_size >= pouch->terminal_reclaim_min_bytes) {
      lc_pouch_namespace_logstore *cache;
      char *next_active_path;
      int next_active_fd;

      rc = lc_pouch_namespace_manifest_rotate(&pouch->allocator, ns, manifest,
                                              manifest->active_segment_id + 1U,
                                              error);
      if (rc != LC_OK) {
        return rc;
      }
      next_active_path =
          lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                    "segments", manifest->active_segment);
      if (next_active_path == NULL) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch terminal active segment",
                            NULL, NULL, "pouch");
      }
      next_active_fd = open(next_active_path, O_WRONLY | O_CREAT, 0666);
      if (next_active_fd < 0) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to create pouch terminal active segment",
                          strerror(errno), next_active_path, "pouch");
      } else if (close(next_active_fd) != 0) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to close pouch terminal active segment",
                          strerror(errno), next_active_path, "pouch");
      }
      lc_free_with_allocator(&pouch->allocator, next_active_path);
      if (rc != LC_OK) {
        return rc;
      }
      cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
      if (cache != NULL && cache->initialized &&
          lc_pouch_state_cache_set_active_segment(pouch, cache, manifest, 0U,
                                                  error) != LC_OK) {
        lc_pouch_state_cache_invalidate_namespace(pouch, ns);
        return error != NULL && error->code != LC_OK ? error->code
                                                     : LC_ERR_INVALID;
      }
    }
  }
  candidate_count = 0UL;
  {
    unsigned long segment_index;

    for (segment_index = 0UL; segment_index < manifest->segment_count;
         ++segment_index) {
      const char *segment_leaf = manifest->segment_leaves[segment_index];

      if (lc_pouch_state_manifest_segment_visible(manifest, segment_leaf) &&
          (manifest->active_segment == NULL ||
           strcmp(segment_leaf, manifest->active_segment) != 0)) {
        ++candidate_count;
      }
    }
  }
  if (out != NULL) {
    out->candidate_segment_count = candidate_count;
  }
  if (candidate_count == 0UL) {
    if (out != NULL) {
      out->skipped = 1;
    }
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_str_field("reason", "no-candidates");
      lc_log_trace(pouch->logger, "compaction.skip", fields, 2U);
    }
    return lc_pouch_maintenance_set_diagnostic(pouch, out, "no-candidates",
                                               error);
  }
  memset(&capture, 0, sizeof(capture));
  memset(&candidate_cache, 0, sizeof(candidate_cache));
  protected_snapshot_blocked = 0;
  rc = lc_pouch_state_compaction_capture_now(pouch, manifest, &capture, error);
  if (rc != LC_OK) {
    lc_pouch_maintenance_mark_aborted(pouch, out, "candidate-read-aborted");
    return rc;
  }
  candidate_cache.ns = lc_strdup_with_allocator(&pouch->allocator, ns);
  if (candidate_cache.ns == NULL) {
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch compaction namespace name",
                        NULL, NULL, NULL);
  }
  rc =
      lc_pouch_state_cache_refresh(pouch, &candidate_cache, manifest, 1, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_compaction_protect_live_links(
        pouch, &capture, &candidate_cache, &protected_snapshot_blocked, error);
  }
  lc_free_with_allocator(&pouch->allocator, candidate_cache.ns);
  candidate_cache.ns = NULL;
  lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                            &candidate_cache);
  if (rc != LC_OK) {
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_pouch_maintenance_mark_aborted(pouch, out, "candidate-read-aborted");
    return rc;
  }
  candidate_bytes = capture.candidate_bytes;
  candidate_count = (unsigned long)capture.candidate_count;
  lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
  if (out != NULL) {
    out->candidate_segment_count = candidate_count;
    out->candidate_bytes = candidate_bytes;
  }
  if (protected_snapshot_blocked) {
    if (out != NULL) {
      out->skipped = 1;
    }
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_str_field("reason", "protected-live-links");
      lc_log_trace(pouch->logger, "compaction.skip", fields, 2U);
    }
    return lc_pouch_maintenance_set_diagnostic(pouch, out,
                                               "protected-live-links", error);
  }
  if (candidate_count == 0UL) {
    if (out != NULL) {
      out->skipped = 1;
    }
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_str_field("reason", "no-candidates");
      lc_log_trace(pouch->logger, "compaction.skip", fields, 2U);
    }
    return lc_pouch_maintenance_set_diagnostic(pouch, out, "no-candidates",
                                               error);
  }
  if (!force && candidate_count < minimum_segment_count) {
    if (out != NULL) {
      out->skipped = 1;
    }
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_str_field("reason", "below-segment-threshold");
      fields[2] = lc_log_u64_field("count", candidate_count);
      lc_log_trace(pouch->logger, "compaction.skip", fields, 3U);
    }
    return lc_pouch_maintenance_set_diagnostic(
        pouch, out, "below-segment-threshold", error);
  }
  if (!force && candidate_bytes < minimum_reclaimable_bytes) {
    if (out != NULL) {
      out->skipped = 1;
    }
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_str_field("reason", "below-reclaimable-threshold");
      fields[2] = lc_log_u64_field("reclaim_bytes", candidate_bytes);
      lc_log_trace(pouch->logger, "compaction.skip", fields, 3U);
    }
    return lc_pouch_maintenance_set_diagnostic(
        pouch, out, "below-reclaimable-threshold", error);
  }
  compacted_segment_id = manifest->max_segment_id;
  if (manifest->latest_snapshot_segment_id > compacted_segment_id) {
    compacted_segment_id = manifest->latest_snapshot_segment_id;
  }
  if (compacted_segment_id == LC_U64_MAX) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch snapshot sequence overflow", NULL, NULL,
                        "pouch");
  }
  ++compacted_segment_id;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_u64_field("count", candidate_count);
    fields[2] = lc_log_u64_field("reclaim_bytes", candidate_bytes);
    fields[3] = lc_log_bool_field("force", force);
    lc_log_debug(pouch->logger, "compaction.start", fields, 4U);
  }
  {
    const char *abort_diagnostic;

    abort_diagnostic = NULL;
    rc = lc_pouch_state_compact_namespace(
        pouch, ns, manifest, &cleanup_deleted_count, &cleanup_pending_count,
        &abort_diagnostic, out != NULL ? &out->compacted : NULL, error);
    if (rc != LC_OK) {
      lc_pouch_maintenance_mark_aborted(pouch, out, abort_diagnostic);
      {
        pslog_field fields[4];

        fields[0] = lc_log_str_field("ns", ns);
        fields[1] = lc_log_str_field("reason", abort_diagnostic);
        fields[2] = lc_log_error_field("error", error);
        fields[3] = lc_log_code_field(error);
        lc_log_error(pouch->logger, "compaction.error", fields, 4U);
      }
      return rc;
    }
    if (out != NULL && !out->compacted) {
      out->skipped = 1;
      return lc_pouch_maintenance_set_diagnostic(
          pouch, out,
          abort_diagnostic != NULL ? abort_diagnostic : "no-candidates", error);
    }
  }
  if (out != NULL) {
    out->compacted = 1;
    out->compacted_segment_id = compacted_segment_id;
    out->cleanup_deleted_count = cleanup_deleted_count;
    out->cleanup_pending_count = cleanup_pending_count;
  }
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_u64_field("segment", compacted_segment_id);
    fields[2] = lc_log_u64_field("reclaim_bytes", candidate_bytes);
    fields[3] = lc_log_u64_field("cleanup_deleted", cleanup_deleted_count);
    fields[4] = lc_log_u64_field("pending", cleanup_pending_count);
    lc_log_debug(pouch->logger, "compaction.complete", fields, 5U);
  }
  return lc_pouch_maintenance_set_diagnostic(pouch, out, "compacted", error);
}

static int lc_pouch_state_compact_namespace(
    lc_pouch *pouch, const char *ns, lc_pouch_namespace_manifest *manifest,
    unsigned long *cleanup_deleted_count, unsigned long *cleanup_pending_count,
    const char **abort_diagnostic, int *compacted_out, lc_error *error) {
  lc_pouch_namespace_logstore snapshot_cache;
  lc_pouch_state_compaction_capture capture;
  char *snapshot_leaf;
  uint64_t compacted_segment_id;
  int snapshot_installed;
  int protected_snapshot_blocked;
  int rc;

  snapshot_installed = 0;
  protected_snapshot_blocked = 0;
  if (compacted_out != NULL) {
    *compacted_out = 0;
  }
  if (abort_diagnostic != NULL) {
    *abort_diagnostic = NULL;
  }
  if (manifest->segment_count == 0UL) {
    return LC_OK;
  }
  memset(&snapshot_cache, 0, sizeof(snapshot_cache));
  snapshot_cache.ns = lc_strdup_with_allocator(&pouch->allocator, ns);
  if (snapshot_cache.ns == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch snapshot namespace name",
                        NULL, NULL, NULL);
  }
  memset(&capture, 0, sizeof(capture));
  rc = lc_pouch_state_compaction_capture_now(pouch, manifest, &capture, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.ns);
    if (abort_diagnostic != NULL) {
      *abort_diagnostic = "validation-read-aborted";
    }
    return rc;
  }
  rc = lc_pouch_state_cache_refresh(pouch, &snapshot_cache, manifest, 1, error);
  if (rc != LC_OK) {
    if (abort_diagnostic != NULL) {
      *abort_diagnostic = "snapshot-refresh-aborted";
    }
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.ns);
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                              &snapshot_cache);
    return rc;
  }
  rc = lc_pouch_state_compaction_protect_live_links(
      pouch, &capture, &snapshot_cache, &protected_snapshot_blocked, error);
  if (rc != LC_OK) {
    if (abort_diagnostic != NULL) {
      *abort_diagnostic = "candidate-protection-aborted";
    }
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.ns);
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                              &snapshot_cache);
    return rc;
  }
  if (protected_snapshot_blocked || capture.candidate_count == 0U) {
    if (abort_diagnostic != NULL) {
      *abort_diagnostic =
          protected_snapshot_blocked ? "protected-live-links" : "no-candidates";
    }
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.ns);
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                              &snapshot_cache);
    return LC_OK;
  }
  /* An exclusive writer with live index maintenance carries a complete
   * in-memory capture across each mutation, so its current manifest remains
   * coupled to canonical state. A shared root, or any root opened with index
   * maintenance disabled, can discard mutations that an existing manifest has
   * not observed. Retire that derived manifest before compaction replaces the
   * topology, so a later indexed reopen rebuilds from canonical state. */
  if (!lc_pouch_single_writer_enabled(pouch) ||
      !pouch->query_indexing_enabled) {
    rc = lc_pouch_query_index_invalidate_namespace(pouch, ns, error);
    if (rc != LC_OK) {
      if (abort_diagnostic != NULL) {
        *abort_diagnostic = "query-index-invalidation-aborted";
      }
      lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
      lc_free_with_allocator(&pouch->allocator, snapshot_cache.ns);
      lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                                &snapshot_cache);
      return rc;
    }
  }
  rc = lc_pouch_state_compaction_capture_records(pouch, &capture,
                                                 &snapshot_cache, error);
  if (rc != LC_OK) {
    if (abort_diagnostic != NULL) {
      *abort_diagnostic = "capture-records-aborted";
    }
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.ns);
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                              &snapshot_cache);
    return rc;
  }
  compacted_segment_id = manifest->max_segment_id;
  if (manifest->latest_snapshot_segment_id > compacted_segment_id) {
    compacted_segment_id = manifest->latest_snapshot_segment_id;
  }
  if (compacted_segment_id == LC_U64_MAX) {
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.ns);
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                              &snapshot_cache);
    if (abort_diagnostic != NULL) {
      *abort_diagnostic = "snapshot-sequence-overflow";
    }
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch snapshot sequence overflow", NULL, NULL,
                        "pouch");
  }
  ++compacted_segment_id;
  snapshot_leaf =
      lc_pouch_namespace_snapshot_leaf(&pouch->allocator, compacted_segment_id);
  if (snapshot_leaf == NULL) {
    lc_free_with_allocator(&pouch->allocator, snapshot_leaf);
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.ns);
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                              &snapshot_cache);
    if (abort_diagnostic != NULL) {
      *abort_diagnostic = "snapshot-prepare-aborted";
    }
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state snapshot name", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_state_write_snapshot(pouch, manifest, snapshot_leaf,
                                     &snapshot_cache, &capture, error);
  if (rc == LC_OK) {
#ifdef LOCKDC_TEST_BUILD
    if (lc_pouch_test_after_snapshot_write_hook != NULL) {
      rc = lc_pouch_test_after_snapshot_write_hook(
          lc_pouch_test_after_snapshot_write_context, error);
    }
#endif
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_cache_refresh(pouch, &snapshot_cache, manifest, 1,
                                      error);
    if (rc == LC_OK) {
      rc = lc_pouch_state_compaction_validate_capture(pouch, manifest, &capture,
                                                      &snapshot_cache, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_namespace_manifest_install_snapshot(
          &pouch->allocator, ns, manifest, snapshot_leaf, compacted_segment_id,
          error);
      if (rc == LC_OK) {
        snapshot_installed = 1;
      } else if (abort_diagnostic != NULL) {
        *abort_diagnostic = "snapshot-install-aborted";
      }
    } else if (abort_diagnostic != NULL) {
      *abort_diagnostic = "validation-drift-aborted";
    }
  } else if (abort_diagnostic != NULL) {
    *abort_diagnostic = "snapshot-write-aborted";
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_queue_compacted_files(pouch, manifest, ns, &capture,
                                              cleanup_deleted_count,
                                              cleanup_pending_count, error);
    if (rc != LC_OK && abort_diagnostic != NULL) {
      *abort_diagnostic = "obsolete-cleanup-aborted";
    }
  }
  if (rc == LC_OK) {
    lc_pouch_namespace_logstore *live_cache;

    live_cache = lc_pouch_namespace_logstore_find(pouch, ns, 0, NULL);
    if (live_cache != NULL) {
      lc_pouch_namespace_logstore_clear_records(&pouch->allocator, live_cache);
      live_cache->initialized = 0;
      live_cache->max_segment_id = 0UL;
      live_cache->segment_count = 0UL;
      live_cache->max_version = 0UL;
      live_cache->max_query_index_seq = 0UL;
    }
  }
  if (rc == LC_OK) {
    if (compacted_out != NULL) {
      *compacted_out = 1;
    }
  } else if (!snapshot_installed) {
    char *snapshot_path;

    snapshot_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                  "snapshots", snapshot_leaf);
    if (snapshot_path != NULL) {
      (void)unlink(snapshot_path);
      lc_free_with_allocator(&pouch->allocator, snapshot_path);
    }
  }
  lc_free_with_allocator(&pouch->allocator, snapshot_leaf);
  lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
  lc_free_with_allocator(&pouch->allocator, snapshot_cache.ns);
  snapshot_cache.ns = NULL;
  lc_pouch_namespace_logstore_clear_records(&pouch->allocator, &snapshot_cache);
  return rc;
}

static int lc_pouch_maintenance_run_locked(
    lc_pouch *pouch, const lc_pouch_maintenance_options *options,
    lc_pouch_maintenance_result *out, lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  const char *ns;
  unsigned long cleanup_deleted_count;
  unsigned long cleanup_pending_count;
  uint64_t now_seconds;
  int force;
  int rc;

  if (out != NULL) {
    memset(out, 0, sizeof(*out));
  }
  if (pouch == NULL || options == NULL || options->ns == NULL ||
      options->ns[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch maintenance requires a namespace", NULL, NULL,
                        NULL);
  }
  ns = options->ns;
  force = options->force ? 1 : 0;
  cleanup_deleted_count = 0UL;
  cleanup_pending_count = 0UL;
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_bool_field("force", force);
    fields[2] = lc_log_bool_field("cleanup_only", options->cleanup_only);
    lc_log_debug(pouch->logger, "maintenance.start", fields, 3U);
  }
  rc = lc_pouch_namespace_ensure_layout(&pouch->allocator, pouch->root_path, ns,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path, ns,
                                        &manifest, &cleanup_deleted_count,
                                        &cleanup_pending_count, error);
  if (rc != LC_OK) {
    return rc;
  }
  now_seconds = (uint64_t)lc_pouch_maintenance_now_seconds();
  rc = lc_pouch_namespace_manifest_cleanup_obsolete(
      &pouch->allocator, ns, &manifest, now_seconds,
      pouch->compaction_delete_grace_seconds, &cleanup_deleted_count,
      &cleanup_pending_count, error);
  if (rc != LC_OK) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  if (out != NULL) {
    out->cleanup_deleted_count = cleanup_deleted_count;
    out->cleanup_pending_count = cleanup_pending_count;
  }
  if (options->retention_updated_before_unix > 0L) {
    rc = lc_pouch_state_retention_sweep(
        pouch, ns, options->retention_updated_before_unix, out, error);
  } else if (options->cleanup_only) {
    if (out != NULL) {
      out->skipped = 1;
      rc = lc_pouch_maintenance_set_string(pouch, &out->ns, ns, error);
      if (rc != LC_OK) {
        lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
        return rc;
      }
    }
    rc = lc_pouch_maintenance_set_diagnostic(
        pouch, out,
        cleanup_pending_count != 0UL ? "cleanup-pending" : "cleanup-complete",
        error);
  } else {
    rc = lc_pouch_state_compact_namespace_if_needed(
        pouch, ns, &manifest, force, options->terminal_reclaim, out, error);
    if (out != NULL) {
      out->cleanup_deleted_count += cleanup_deleted_count;
      out->cleanup_pending_count += cleanup_pending_count;
    }
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  if (rc == LC_OK) {
    pslog_field fields[7];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] =
        lc_log_bool_field("compacted", out != NULL ? out->compacted : 0);
    fields[2] = lc_log_bool_field("skipped", out != NULL ? out->skipped : 0);
    fields[3] =
        lc_log_str_field("reason", out != NULL ? out->diagnostic : NULL);
    fields[4] = lc_log_u64_field("reclaim_bytes",
                                 out != NULL ? out->candidate_bytes : 0UL);
    fields[5] = lc_log_u64_field(
        "cleanup_deleted", out != NULL ? out->cleanup_deleted_count : 0UL);
    fields[6] = lc_log_u64_field(
        "pending", out != NULL ? out->cleanup_pending_count : 0UL);
    lc_log_debug(pouch->logger, "maintenance.complete", fields, 7U);
  } else {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_error_field("error", error);
    fields[2] = lc_log_code_field(error);
    fields[3] =
        lc_log_str_field("detail", error != NULL ? error->detail : NULL);
    lc_log_error(pouch->logger, "maintenance.error", fields, 4U);
  }
  return rc;
}

static int lc_pouch_multifile_maintenance_run(
    lc_pouch *pouch, const lc_pouch_maintenance_options *options,
    lc_pouch_maintenance_result *out, lc_error *error) {
  lc_pouch_state_namespace_lock lock;
  const char *ns;
  lc_pouch_state_commit_group *commit_group;
  lc_pouch_maintenance_result retention = {0};
  int owns_commit_group;
  int rc;

  ns = options != NULL ? options->ns : NULL;
  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || ns == NULL || ns[0] == '\0') {
    return lc_pouch_maintenance_run_locked(pouch, options, out, error);
  }
  rc = lc_pouch_writer_mode_operation_begin(pouch, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!options->cleanup_only) {
    rc = lc_pouch_retention_run(pouch, ns, LC_POUCH_RETENTION_ATTEMPT_LIMIT,
                                &retention, error);
    if (out != NULL)
      *out = retention;
    if (rc != LC_OK) {
      lc_pouch_writer_mode_operation_end(pouch);
      return rc;
    }
  }
  rc = lc_pouch_state_lock_namespace_for_mutation(
      pouch, ns, LC_POUCH_NAMESPACE_FOREGROUND, &lock, error);
  if (rc != LC_OK) {
    lc_pouch_writer_mode_operation_end(pouch);
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_namespace_lock_release(&lock);
    lc_pouch_writer_mode_operation_end(pouch);
    return rc;
  }
  rc = lc_pouch_maintenance_run_locked(pouch, options, out, error);
  if (out != NULL) {
    out->retention_scanned_count += retention.retention_scanned_count;
    out->retention_expired_count += retention.retention_expired_count;
    out->retention_deleted_state_count +=
        retention.retention_deleted_state_count;
    out->retention_failed_count += retention.retention_failed_count;
  }
  rc = lc_pouch_state_finish_commit_group(commit_group, owns_commit_group, rc,
                                          error);
  if (rc == LC_OK && options->terminal_reclaim &&
      (out == NULL || out->cleanup_pending_count == 0UL)) {
    lc_error marker_error;

    lc_error_init(&marker_error);
#ifdef LOCKDC_TEST_BUILD
    if (lc_pouch_test_before_terminal_reclaim_marker_remove_hook != NULL) {
      (void)lc_pouch_test_before_terminal_reclaim_marker_remove_hook(
          lc_pouch_test_before_terminal_reclaim_marker_remove_context,
          &marker_error);
    }
#endif
    if (marker_error.code == LC_OK && lc_pouch_terminal_reclaim_marker_remove(
                                          pouch, ns, &marker_error) != LC_OK) {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", ns);
      fields[1] = lc_log_error_field("error", &marker_error);
      fields[2] = lc_log_code_field(&marker_error);
      lc_log_warn(pouch->logger, "compaction.terminal.marker.error", fields,
                  3U);
    }
    lc_error_cleanup(&marker_error);
  }
  if (rc != LC_OK) {
    lc_pouch_state_cache_invalidate_namespace(pouch, ns);
  }
  lc_pouch_state_namespace_lock_release(&lock);
  lc_pouch_writer_mode_operation_end(pouch);
  if (rc == LC_OK)
    rc = lc_collection_maintenance_run(pouch, options, out, error);
  return rc;
}
