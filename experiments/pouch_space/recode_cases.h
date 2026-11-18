#ifndef POUCH_SPACE_RECODE_CASES_H
#define POUCH_SPACE_RECODE_CASES_H

static unsigned char recode_random(uint32_t *state) {
  *state ^= *state << 13;
  *state ^= *state >> 17;
  *state ^= *state << 5;
  return (unsigned char)*state;
}
static void recode_check(ps_root *root, ps_namespace *ns,
                         const lc_pouch_state_entry *entry, const char *binding,
                         lc_error *e) {
  unsigned int kind;
  uint64_t id, next = 0;
  ps_namespace_window *window = NULL;
  lc_pouch_state_entry parsed = {0};
  unsigned char *scratch = NULL, bytes[1024];
  lc_pouch_record_span span;
  lc_pouch_crypto_reader *reader = NULL;
  lc_source *decoded = NULL;
  int found = 0, truncated = 0;
  REQUIRE(ps_namespace_parse_source_leaf(entry->payload_span.container_leaf,
                                         &kind, &id));
  uint64_t end = entry->payload_span.payload_offset + entry->cipher_bytes;
  ok(ps_namespace_read_open(ns, kind, id, entry->payload_span.record_offset,
                            end - entry->payload_span.record_offset, &window,
                            e),
     e);
  ok(lc_pouch_state_record_read(
         ps_namespace_read_input(window), &allocator,
         entry->payload_span.container_leaf, entry->payload_span.record_offset,
         end, 0, 1, &scratch, &parsed, &next, &found, &truncated, e),
     e);
  REQUIRE(found && !truncated && next == end &&
          parsed.version == entry->version &&
          parsed.index_seq == entry->index_seq &&
          parsed.lifetime == entry->lifetime && parsed.has_query_index_seq &&
          parsed.query_index_seq == entry->query_index_seq &&
          parsed.updated_at_unix == entry->updated_at_unix &&
          parsed.record_type == entry->record_type &&
          parsed.has_query_hidden == entry->has_query_hidden &&
          parsed.query_hidden == entry->query_hidden &&
          parsed.staged_delete_marker == entry->staged_delete_marker &&
          parsed.retention == entry->retention &&
          !strcmp(parsed.content_type, entry->content_type) &&
          parsed.bytes == entry->bytes && !strcmp(parsed.etag, entry->etag) &&
          parsed.metadata_length == entry->metadata_length &&
          !memcmp(parsed.metadata, entry->metadata, entry->metadata_length));
  ok(lc_pouch_record_span_bind(&span, ps_namespace_read_input(window),
                               entry->payload_span.payload_offset,
                               entry->cipher_bytes, NULL, 0, e),
     e);
  ok(lc_pouch_crypto_reader_open(ps_root_crypto(root), &reader, e), e);
  ok(lc_pouch_crypto_reader_bind_source(reader, entry->payload_context, binding,
                                        &span.source, entry->cipher_bytes,
                                        entry->descriptor, &decoded, e),
     e);
  size_t at = 0, got;
  uint32_t random = 0xdeadbeef;
  while ((got = decoded->read(decoded, bytes, sizeof(bytes), e))) {
    REQUIRE(at + got <= entry->bytes);
    if (entry->bytes <= sizeof(body))
      REQUIRE(!memcmp(bytes, body + at, got));
    else {
      int match = 1;
      for (size_t i = 0; i < got; ++i)
        if (bytes[i] !=
            (!strcmp(entry->content_type, "application/incompressible")
                 ? recode_random(&random)
                 : (unsigned char)('a' + (at + i) % 23)))
          match = 0;
      REQUIRE(match);
    }
    at += got;
  }
  ok(e->code, e);
  REQUIRE(at == entry->bytes);
  lc_pouch_crypto_reader_close(reader);
  lc_pouch_state_entry_cleanup(&allocator, &parsed);
  lc_free_with_allocator(&allocator, scratch);
  ps_namespace_read_close(window);
}
static void recode_projection(ps_root *root, lc_pouch_state_entry *entry,
                              int alias, lc_error *e) {
  entry->index_seq = 23;
  entry->has_query_index_seq = 1;
  entry->query_index_seq = 3;
  entry->updated_at_unix = 1234;
  entry->retention = alias ? -60 : 4567;
  entry->has_query_hidden = entry->query_hidden = 1;
  entry->metadata =
      (unsigned char *)lc_strdup_with_allocator(&allocator, "fence=41");
  REQUIRE(entry->metadata);
  entry->metadata_length = 8;
  entry->record_type = alias ? LC_POUCH_STATE_RECORD_STATE_LINK
                             : LC_POUCH_STATE_RECORD_STATE_META;
  if (alias) {
    char *rebound = NULL;
    if (entry->descriptor) {
      ok(lc_pouch_crypto_rebind_descriptor(ps_root_crypto(root),
                                           entry->descriptor, "namespace-test",
                                           "conversion-alias", &rebound, e),
         e);
      lc_free_with_allocator(&allocator, entry->descriptor);
      entry->descriptor = rebound;
    }
    lc_free_with_allocator(&allocator, entry->key);
    entry->key = lc_strdup_with_allocator(&allocator, "key/.staging/txn");
    REQUIRE(entry->key);
    entry->object_link = 1;
  }
}
static size_t recode_trial(const char *path, const char *key, int codec,
                           int sync, int alias, size_t bytes, int kind,
                           size_t cut, int nullable) {
  lc_error e;
  lc_error_init(&e);
  ps_root_options options = {0};
  options.crypto_allocator = &allocator;
  options.crypto.key_string = codec & 1 ? key : NULL;
  options.crypto.compression_enabled = !!(codec & 2);
  options.storage.durable_sync = sync;
  model m = {0};
  ps_io io = {&m, rd, wr, sync_file, reserve, NULL};
  if (kind >= 2) {
    m.live = calloc(1, PS_MIB);
    m.stable = calloc(1, PS_MIB);
    REQUIRE(m.live && m.stable);
    m.torn = kind - 2;
    options.storage.io = &io;
    options.storage.initial_capacity = options.storage.maximum_capacity =
        PS_MIB;
  }
  ps_root *root = NULL;
  ok(ps_root_open(path, 1, &options, &root, &e), &e);
  ps_namespace *ns = acquire(root, "recode", 1, &e);
  lc_pouch_state_entry entry = {0}, converted = {0}, roundtrip = {0};
  ok(put(root, ns, 7, 0, bytes, NULL, NULL, &entry, &e), &e);
  recode_projection(root, &entry, alias, &e);
  const char *binding = alias ? "conversion-alias" : "namespace-test";
  lc_pouch_crypto *target = NULL;
  lc_pouch_crypto_open_options target_options = options.crypto;
  target_options.compression_enabled = !options.crypto.compression_enabled;
  ok(lc_pouch_crypto_open(&allocator, &target_options, &target, NULL, &e), &e);
  ok(ps_flush(ps_root_space(root), &e), &e);
  converted.key = lc_strdup_with_allocator(&allocator, "sentinel");
  converted.version = 999;
  REQUIRE(converted.key);
  allocation = 0;
  m.calls = 0;
  if (kind == 1)
    fail_allocation = cut ? cut : SIZE_MAX;
  if (kind >= 2)
    m.fail = cut;
  int rc = ps_namespace_body_recode(ns, target, &entry, binding, &converted,
                                    nullable ? NULL : &e);
  size_t calls = kind == 1 ? allocation : m.calls;
  fail_allocation = m.fail = 0;
  if (!cut)
    ok(rc, &e);
  else {
    REQUIRE(calls >= cut);
    REQUIRE(rc == LC_OK || rc == (kind == 1 ? LC_ERR_NOMEM : LC_ERR_TRANSPORT));
    if (kind >= 2)
      REQUIRE(rc == LC_ERR_TRANSPORT);
  }
  if (rc != LC_OK)
    REQUIRE(converted.version == 999 && !strcmp(converted.key, "sentinel"));
  clear(&e);
  if (rc == LC_OK)
    recode_check(root, ns, &converted, binding, &e);
  if (kind >= 2) {
    ps_namespace_release(ns);
    ok(ps_root_close(root, &e), &e);
    crash(&m, path);
    ok(ps_root_open(path, 0, &options, &root, &e), &e);
    ns = acquire(root, "recode", 0, &e);
    if (rc == LC_OK)
      recode_check(root, ns, &converted, binding, &e);
  }
  if (rc != LC_OK) {
    ok(ps_namespace_body_recode(ns, target, &entry, binding, &converted, &e),
       &e);
    recode_check(root, ns, &converted, binding, &e);
  }
  if (!kind) {
    lc_pouch_state_entry bad = entry;
    char wrong[65];
    lc_pouch_state_entry unchanged = {0};
    unchanged.key = lc_strdup_with_allocator(&allocator, "unchanged");
    unchanged.version = 99;
    REQUIRE(unchanged.key);
    bad.payload_span.payload_crc ^= 1;
    REQUIRE(ps_namespace_body_recode(ns, target, &bad, binding, &unchanged,
                                     &e) == LC_ERR_PROTOCOL);
    clear(&e);
    strcpy(wrong, entry.etag);
    wrong[0] = wrong[0] == '0' ? '1' : '0';
    bad = entry;
    bad.etag = wrong;
    REQUIRE(ps_namespace_body_recode(ns, target, &bad, binding, &unchanged,
                                     NULL) == LC_ERR_PROTOCOL);
    REQUIRE(unchanged.version == 99 && !strcmp(unchanged.key, "unchanged"));
    lc_pouch_state_entry_cleanup(&allocator, &unchanged);
  }
  ok(ps_namespace_body_recode(ns, ps_root_crypto(root), &converted, binding,
                              &roundtrip, &e),
     &e);
  recode_check(root, ns, &roundtrip, binding, &e);
  REQUIRE(lc_pouch_crypto_compression_enabled(ps_root_crypto(root)) ==
          options.crypto.compression_enabled);
  count(ns, 1, &e);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  ok(ps_root_open(path, 0, &options, &root, &e), &e);
  ns = acquire(root, "recode", 0, &e);
  recode_check(root, ns, &converted, binding, &e);
  recode_check(root, ns, &roundtrip, binding, &e);
  count(ns, 1, &e);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  lc_pouch_crypto_close(target);
  lc_pouch_state_entry_cleanup(&allocator, &entry);
  lc_pouch_state_entry_cleanup(&allocator, &converted);
  lc_pouch_state_entry_cleanup(&allocator, &roundtrip);
  REQUIRE(!unlink(path));
  free(m.live);
  free(m.stable);
  lc_error_cleanup(&e);
  return calls;
}
static void record_recode(const char *path, const char *key, int faults) {
  size_t sites = 0;
  if (!faults) {
    const size_t sizes[] = {0, 1, sizeof(body)};
    for (int codec = 0; codec < 4; ++codec)
      for (int sync = 0; sync < 2; ++sync)
        for (int alias = 0; alias < 2; ++alias)
          for (size_t size = 0; size < sizeof(sizes) / sizeof(*sizes); ++size)
            (void)recode_trial(path, key, codec, sync, alias, sizes[size], 0, 0,
                               0);
  } else {
    for (int codec = 0; codec <= 3; codec += 3)
      for (int sync = 0; sync < 2; ++sync)
        for (int kind = 1; kind <= 5; ++kind) {
          size_t count =
              recode_trial(path, key, codec, sync, 1, 4097, kind, 0, 0);
          REQUIRE(count);
          for (size_t cut = 1; cut <= count; ++cut) {
            (void)recode_trial(path, key, codec, sync, 1, 4097, kind, cut,
                               kind == 1 && (cut % 2));
            ++sites;
          }
        }
    printf("record recode verified allocation/I/O/torn sites=%zu\n", sites);
  }
}
typedef struct recode_generated {
  lc_source source;
  size_t at, length, calls;
  uint32_t random;
} recode_generated;
static size_t recode_generate(lc_source *source, void *output, size_t bytes,
                              lc_error *e) {
  recode_generated *g = source->impl;
  unsigned char *p = output;
  (void)e;
  if (bytes > 4096)
    bytes = 4096;
  if (bytes > g->length - g->at)
    bytes = g->length - g->at;
  for (size_t i = 0; i < bytes; ++i)
    p[i] = g->random ? recode_random(&g->random)
                     : (unsigned char)('a' + (g->at + i) % 23);
  g->at += bytes;
  ++g->calls;
  return bytes;
}
static void recode_large(const char *path, const char *key, int encrypted,
                         int sync, int incompressible) {
  lc_error e;
  lc_error_init(&e);
  ps_root_options options = {0};
  options.crypto_allocator = &allocator;
  options.crypto.key_string = encrypted ? key : NULL;
  options.storage.durable_sync = sync;
  options.storage.initial_capacity = options.storage.maximum_capacity =
      PS_DEFAULT_CAPACITY;
  ps_root *root = NULL;
  ok(ps_root_open(path, 1, &options, &root, &e), &e);
  ps_namespace *ns = acquire(root, "recode", 1, &e);
  recode_generated g = {0};
  g.source.impl = &g;
  g.source.read = recode_generate;
  g.length = 2 * PS_MIB;
  g.random = incompressible ? 0xdeadbeef : 0;
  lc_pouch_state_entry request = {0}, entry = {0}, converted = {0};
  request.key = "key";
  request.content_type =
      incompressible ? "application/incompressible" : "application/test";
  request.payload_context = "namespace-test";
  request.version = request.index_seq = 7;
  request.lifetime = 1;
  request.record_type = LC_POUCH_STATE_RECORD_STATE_PUT;
  ps_namespace_append *append = NULL;
  ok(ps_namespace_append_begin(ns, 0, 0, PS_APPEND_PREFIX, NULL, &append, &e),
     &e);
  ok(lc_pouch_state_record_append(
         &allocator, ps_root_crypto(root), ps_namespace_append_output(append),
         ps_namespace_append_leaf(append), &request, &g.source, 0, &entry, &e),
     &e);
  ok(ps_namespace_append_commit(append, PS_COMMIT_DEFAULT, &e), &e);
  ps_namespace_append_close(append);
  REQUIRE(g.at == g.length && g.calls > 500);
  recode_projection(root, &entry, 0, &e);
  lc_pouch_crypto_open_options target_options = options.crypto;
  target_options.compression_enabled = 1;
  lc_pouch_crypto *target = NULL;
  ok(lc_pouch_crypto_open(&allocator, &target_options, &target, NULL, &e), &e);
  ps_stats before, after;
  ok(ps_status(ps_root_space(root), &before, &e), &e);
  measuring = 1;
  largest = 0;
  int rc = ps_namespace_body_recode(ns, target, &entry, "namespace-test",
                                    &converted, &e);
  measuring = 0;
  ok(rc, &e);
  /* Crypto uses fixed 64 KiB frames and a two-frame decryptor. Neither is
   * proportional to this 2 MiB body. */
  REQUIRE(largest < 256 * 1024);
  ok(ps_status(ps_root_space(root), &after, &e), &e);
  REQUIRE(after.capacity == PS_DEFAULT_CAPACITY &&
          after.allocated_bytes >= before.allocated_bytes &&
          after.reserved_payload_bytes == before.reserved_payload_bytes &&
          after.budgets == before.budgets);
  uint64_t allocated = after.allocated_bytes - before.allocated_bytes;
  REQUIRE(incompressible
              ? allocated >= g.length && allocated < g.length + 128 * 1024
              : allocated < PS_MIB / 4);
  printf("recode large encrypted=%d sync=%d incompressible=%d plain=%zu "
         "encoded=%llu "
         "allocated=%llu largest=%zu\n",
         encrypted, sync, incompressible, g.length,
         (unsigned long long)converted.cipher_bytes,
         (unsigned long long)(after.allocated_bytes - before.allocated_bytes),
         largest);
  recode_check(root, ns, &converted, "namespace-test", &e);
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  ok(ps_root_open(path, 0, &options, &root, &e), &e);
  ns = acquire(root, "recode", 0, &e);
  recode_check(root, ns, &converted, "namespace-test", &e);
  REQUIRE(!lc_pouch_crypto_compression_enabled(ps_root_crypto(root)));
  ps_namespace_release(ns);
  ok(ps_root_close(root, &e), &e);
  lc_pouch_crypto_close(target);
  lc_pouch_state_entry_cleanup(&allocator, &entry);
  lc_pouch_state_entry_cleanup(&allocator, &converted);
  REQUIRE(!unlink(path));
  lc_error_cleanup(&e);
}
static void recode_capacity(const char *path, const char *key) {
  for (int encrypted = 0; encrypted < 2; ++encrypted)
    for (int sync = 0; sync < 2; ++sync) {
      recode_large(path, key, encrypted, sync, 0);
      recode_large(path, key, encrypted, sync, 1);
      lc_error e;
      lc_error_init(&e);
      model m = {0};
      m.live = calloc(1, PS_MIB);
      m.stable = calloc(1, PS_MIB);
      REQUIRE(m.live && m.stable);
      ps_io io = {&m, rd, wr, sync_file, reserve, NULL};
      ps_root_options options = {0};
      options.crypto_allocator = &allocator;
      options.crypto.key_string = encrypted ? key : NULL;
      options.storage.durable_sync = sync;
      options.storage.io = &io;
      options.storage.initial_capacity = options.storage.maximum_capacity =
          PS_MIB;
      ps_root *root = NULL;
      ok(ps_root_open(path, 1, &options, &root, &e), &e);
      ps_namespace *ns = acquire(root, "recode", 1, &e);
      ps_namespace_body *bounded = NULL;
      ok(ps_namespace_body_begin_bound(ns, PS_PAGE, &bounded, &e), &e);
      ps_budget *bounded_peer = capacity_peer(ps_root_space(root), 0, &e);
      REQUIRE(ps_namespace_body_commit(bounded, &e) == LC_ERR_INVALID);
      clear(&e);
      lc_pouch_record_io output = ps_namespace_body_output(bounded);
      struct iovec part = {body, 31};
      ok(lc_pouch_record_io_append(output, &part, 1, &e), &e);
      part.iov_len = PS_PAGE;
      REQUIRE(lc_pouch_record_io_append(output, &part, 1, &e) ==
              LC_ERR_INVALID);
      clear(&e);
      uint64_t tail = 0;
      ok(lc_pouch_record_io_tail(output, &tail, &e), &e);
      REQUIRE(tail == 31);
      ok(ps_namespace_body_commit(bounded, &e), &e);
      REQUIRE(lc_pouch_record_io_append(output, &part, 1, &e) ==
              LC_ERR_INVALID);
      clear(&e);
      unsigned int source_kind;
      uint64_t source_id;
      REQUIRE(ps_namespace_parse_source_leaf(ps_namespace_body_leaf(bounded),
                                             &source_kind, &source_id));
      ps_namespace_window *window = NULL;
      ok(ps_namespace_read_open(ns, source_kind, source_id, 0, 31, &window, &e),
         &e);
      unsigned char bytes[31];
      part.iov_base = bytes;
      part.iov_len = sizeof(bytes);
      ok(lc_pouch_record_io_read(ps_namespace_read_input(window), 0, &part, 1,
                                 &e),
         &e);
      REQUIRE(!memcmp(bytes, body, sizeof(bytes)));
      ps_namespace_read_close(window);
      ps_namespace_body_close(bounded);
      ok(ps_budget_close(bounded_peer, &e), &e);
      lc_pouch_state_entry entry = {0}, converted = {0};
      ok(put(root, ns, 7, 0, sizeof(body), NULL, NULL, &entry, &e), &e);
      recode_projection(root, &entry, 0, &e);
      lc_pouch_crypto_open_options target_options = options.crypto;
      target_options.compression_enabled = 1;
      lc_pouch_crypto *target = NULL;
      ok(lc_pouch_crypto_open(&allocator, &target_options, &target, NULL, &e),
         &e);
      ps_budget *peer = capacity_peer(ps_root_space(root), 0, &e);
      size_t reads = m.reads;
      REQUIRE(ps_namespace_body_recode(ns, target, &entry, "namespace-test",
                                       &converted, &e) == LC_ERR_INVALID);
      REQUIRE(m.reads == reads && !converted.found);
      clear(&e);
      ok(ps_budget_close(peer, &e), &e);
      ok(ps_namespace_body_recode(ns, target, &entry, "namespace-test",
                                  &converted, &e),
         &e);
      recode_check(root, ns, &converted, "namespace-test", &e);
      ps_namespace_release(ns);
      ok(ps_root_close(root, &e), &e);
      lc_pouch_crypto_close(target);
      lc_pouch_state_entry_cleanup(&allocator, &entry);
      lc_pouch_state_entry_cleanup(&allocator, &converted);
      REQUIRE(!unlink(path));
      free(m.live);
      free(m.stable);
      lc_error_cleanup(&e);
    }
}
#endif
