#ifndef POUCH_SPACE_BODY_SELECT_CLIENT_CASES_H
#define POUCH_SPACE_BODY_SELECT_CLIENT_CASES_H
typedef struct client_recode {
  ps_namespace *ns;
  lc_pouch_crypto *target;
  size_t converted;
} client_recode;
static int client_recode_record(void *context, uint64_t segment,
                                const lc_pouch_state_entry *entry,
                                lc_error *e) {
  client_recode *work = context;
  REQUIRE(!segment);
  if (entry->control || !entry->found || !entry->payload_span.present)
    return LC_OK;
  unsigned char body_class =
      entry->record_type == LC_POUCH_STATE_RECORD_OBJECT_PUT ||
              entry->object_link
          ? 2
          : 1;
  char *binding = lc_pouch_state_crypto_context(
      &allocator, "projection", entry->key, entry->version, body_class);
  REQUIRE(binding);
  lc_pouch_state_entry converted = {0};
  int rc = ps_namespace_body_recode(work->ns, work->target, entry, binding,
                                    &converted, e);
  if (rc == LC_OK)
    rc = ps_namespace_body_select(work->ns, entry, &converted, e);
  if (rc == LC_OK)
    ++work->converted;
  lc_pouch_state_entry_cleanup(&allocator, &converted);
  lc_free_with_allocator(&allocator, binding);
  return rc;
}
static void body_select_client(const char *path, const char *directory) {
  lc_error e;
  lc_error_init(&e);
  for (int codec = 0; codec < 4; ++codec)
    for (int sync = 0; sync < 2; ++sync) {
      ps_pouch_state_options o = options(codec, sync);
      o.initial_capacity = o.maximum_capacity = PS_DEFAULT_CAPACITY;
      lc_pouch *p = NULL;
      ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
      projection_fixture(p, &e);
      projection_put(p, "a-state", 31, 99, 0, 12, &e);
      projection_put(p, "tail", 31, 77, 0, 13, &e);
      projection_meta(p, "z-object", "tail-object", 0, 14, &e);
      lc_pouch_close(p);
      REQUIRE(!live);
      for (int direction = 0; direction < 2; ++direction) {
        ps_root_options root_options = {0};
        root_options.crypto_allocator = &allocator;
        root_options.crypto.key_string = o.logical.crypto_key;
        root_options.storage.durable_sync = sync;
        ps_root *root = NULL;
        ok(ps_root_open(path, 0, &root_options, &root, &e), &e);
        ps_namespace *ns = NULL;
        ok(ps_root_namespace_acquire(root, "projection", 0, &ns, &e), &e);
        ok(ps_namespace_checkpoint_flush(ns, &e), &e);
        lc_pouch_crypto_open_options target_options = root_options.crypto;
        target_options.compression_enabled =
            direction ? !!(codec & 2) : !(codec & 2);
        lc_pouch_crypto *target = NULL;
        ok(lc_pouch_crypto_open(&allocator, &target_options, &target, NULL, &e),
           &e);
        ps_namespace_view *view = NULL;
        ok(ps_namespace_view_open(ns, &view, &e), &e);
        client_recode work = {ns, target, 0};
        ok(ps_namespace_view_visit(view, 1, client_recode_record, &work, &e),
           &e);
        REQUIRE(work.converted == 4);
        ps_namespace_view_close(view);
        ps_namespace_release(ns);
        ok(ps_root_close(root, &e), &e);
        lc_pouch_crypto_close(target);
        REQUIRE(!live);
        ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
        projection_readback(p, &e);
        unsigned int objects = 0;
        ok(lc_pouch_state_visit(p, "projection", projection_cold_object,
                                &objects, &e),
           &e);
        REQUIRE(objects == 1);
        lc_pouch_close(p);
        REQUIRE(!live);
      }
      ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
      projection_meta(p, "z-object", "after-codec", 0, 15, &e);
      read_value(p, "projection", "z-object", 1048601, 35, 2, &e);
      lc_pouch_generation query;
      ok(lc_pouch_state_query_index_seq(p, "projection", &query, &e), &e);
      REQUIRE(query == 13);
      lc_pouch_close(p);
      REQUIRE(!live);
      one_file(directory);
      REQUIRE(!unlink(path));
    }
  lc_error_cleanup(&e);
}
#endif
