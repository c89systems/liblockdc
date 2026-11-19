#ifndef POUCH_SPACE_ROOT_CONVERSION_CASES_H
#define POUCH_SPACE_ROOT_CONVERSION_CASES_H
#include "lc_pouch_query_index.h"

static int root_count_value(void *context, ps_key key, const ps_ref *ref,
                            uint64_t bytes, lc_error *e) {
  (void)key;
  (void)ref;
  (void)bytes;
  (void)e;
  ++*(size_t *)context;
  return LC_OK;
}
static size_t root_artifacts(fixture *f, int collection) {
  ps_root *root = client_root(f);
  ps_namespace *ns = NULL;
  ps_catalog_view *view = NULL;
  ps_catalog_info info;
  ok(ps_root_namespace_acquire(root, "journal", 0, &ns, &f->error), &f->error);
  uint64_t owner = ns->identity.owner;
  if (collection) {
    ok(ps_catalog_get(root->space, owner, PS_CATALOG_COLLECTION,
                      "events/\303\245", strlen("events/\303\245"), &view,
                      &f->error),
       &f->error);
    REQUIRE(view);
    ok(ps_catalog_view_info(view, &info, &f->error), &f->error);
    owner = info.stream.owner;
    ps_catalog_view_close(view);
  }
  ps_namespace_release(ns);
  if (collection)
    return cq_artifacts(f, owner);
  size_t count = 0;
  ok(ps_values_visit(root->space, owner, PS_QUERY_FIRST, PS_QUERY_LAST,
                     root_count_value, &count, &f->error),
     &f->error);
  return count;
}
static void root_build_indexes(fixture *f) {
  lc_pouch *pouch = ((lc_client_handle *)f->client)->pouch;
  lc_pouch_state_read_result metadata = {0};
  lc_pouch_query_index_flush_result result = {0};
  ok(lc_pouch_state_read_metadata(pouch, "journal", "value", &metadata,
                                  &f->error),
     &f->error);
  ok(lc_pouch_query_index_flush(pouch, "journal", metadata.index_seq, &result,
                                &f->error),
     &f->error);
  REQUIRE(result.index_seq >= metadata.index_seq);
  lc_pouch_state_read_result_cleanup(&pouch->allocator, &metadata);
  ok(cq_build(f, 0), &f->error);
  REQUIRE(root_artifacts(f, 0) > 0 && root_artifacts(f, 1) > 0);
}

static void root_value(fixture *f, const char *ns, size_t bytes, int object,
                       int writing, int policy, char etag[65]) {
  lc_pouch *pouch = ((lc_client_handle *)f->client)->pouch;
  if (writing) {
    producer p;
    producer_init(&p, bytes);
    lc_pouch_state_write_options options = {0};
    lc_pouch_state_write_result result = {0};
    options.object_record = object;
    options.content_type = "application/json";
    options.has_metadata = 1;
    options.metadata = (const unsigned char *)"root-metadata";
    options.metadata_length = 13;
    ok(lc_pouch_state_write(pouch, ns, "value", &p.source, &options, &result,
                            &f->error),
       &f->error);
    REQUIRE(result.etag && strlen(result.etag) < 65 && result.version == 1);
    strcpy(etag, result.etag);
    lc_pouch_state_write_result_cleanup(&pouch->allocator, &result);
  } else {
    lc_pouch_state_read_result result = {0};
    ok(lc_pouch_state_read(pouch, ns, "value", &result, &f->error), &f->error);
    REQUIRE(result.found && result.has_body && result.body &&
            result.bytes == bytes && result.version == 1 && result.etag &&
            !strcmp(result.etag, etag) && result.metadata_length == 13 &&
            !memcmp(result.metadata, "root-metadata", 13));
    lc_pouch_crypto_open_options options = {0};
    options.key_string = f->config.pouch_crypto_key;
    options.compression_enabled = policy;
    lc_pouch_crypto *target = NULL;
    ok(lc_pouch_crypto_open(NULL, &options, &target, NULL, &f->error),
       &f->error);
    int matches;
    ok(lc_pouch_crypto_descriptor_matches(target, result.descriptor, &matches,
                                          &f->error),
       &f->error);
    REQUIRE(matches);
    lc_pouch_crypto_close(target);
    unsigned char buffer[2048];
    size_t at = 0, n;
    while ((n = result.body->read(result.body, buffer, sizeof(buffer),
                                  &f->error))) {
      REQUIRE(n <= bytes - at);
      for (size_t i = 0; i < n; ++i)
        REQUIRE(buffer[i] == byte_at(at + i, bytes));
      at += n;
    }
    ok(f->error.code, &f->error);
    REQUIRE(at == bytes);
    lc_pouch_state_read_result_cleanup(&pouch->allocator, &result);
  }
}
static lc_collection *root_other(fixture *f, int create) {
  lc_collection_config config;
  lc_collection_config_init(&config);
  config.ns = "aux";
  config.name = "other";
  lc_collection *other = NULL;
  ok(create ? lc_client_create_collection(f->client, &config, &other, &f->error)
            : lc_client_open_collection(f->client, &config, &other, &f->error),
     &f->error);
  return other;
}
static void root_seed(fixture *f, const char *directory, int codec, int sync,
                      size_t bytes, int compact, const ps_io *io,
                      char etags[3][65]) {
  setup_io(f, directory, codec, sync, 1, io);
  (void)append(f->collection, "root-first", 4097, &f->error);
  (void)append(f->collection, "root-stream", bytes, &f->error);
  lc_collection *other = root_other(f, 1);
  (void)append(other, "other-first", 8193, &f->error);
  lc_collection_close(other);
  root_value(f, "journal", bytes, 0, 1, 0, etags[0]);
  root_value(f, "aux", 8193, 1, 1, 0, etags[1]);
  root_value(f, "third", 4097, 0, 1, 0, etags[2]);
  if (compact)
    ok(lc_collection_compact(f->collection, 1, 0, NULL, &f->error), &f->error);
  if (query_mode)
    root_build_indexes(f);
  ok(ps_flush(ps_root_space(client_root(f)), &f->error), &f->error);
}
static void root_check(fixture *f, size_t bytes, int policy,
                       char etags[3][65]) {
  REQUIRE(lc_pouch_crypto_compression_enabled(ps_root_crypto(client_root(f))) ==
          policy);
  open_collection(f, 0, &f->collection);
  open_collection(f, 0, &f->peer);
  read_entry(f->collection, 1, 4097, &f->error);
  read_entry(f->collection, 2, bytes, &f->error);
  producer p;
  producer_init(&p, bytes);
  lc_collection_append_req retry = {0};
  lc_collection_append_res repeated;
  retry.append_id = "root-stream";
  ok(lc_collection_append_staged(f->peer, &retry, &p.source, &repeated,
                                 &f->error),
     &f->error);
  REQUIRE(repeated.duplicate && repeated.position == 2);
  lc_collection *other = root_other(f, 0);
  read_entry(other, 1, 8193, &f->error);
  lc_collection_close(other);
  root_value(f, "journal", bytes, 0, 0, policy, etags[0]);
  root_value(f, "aux", 8193, 1, 0, policy, etags[1]);
  root_value(f, "third", 4097, 0, 0, policy, etags[2]);
  ps_key key = {0, 2, PS_CONTROL};
  ps_ref ref;
  uint64_t size;
  int found;
  ok(ps_value_info(ps_root_space(client_root(f)), key, &ref, &size, &found,
                   &f->error),
     &f->error);
  REQUIRE(!found);
}
static void root_conversion_cases(const char *directory) {
  int original_query_mode = query_mode;
  query_mode = 1;
  for (int codec = 0; codec < 4; ++codec)
    for (int sync = 0; sync < 2; ++sync)
      for (int compact = 0; compact < 2; ++compact) {
        fixture f;
        char etags[3][65];
        root_seed(&f, directory, codec, sync, 262161, compact, NULL, etags);
        close_store(&f);
        f.settings.compression = codec & 2 ? "none" : "zlib";
        open_client(&f, 0);
        REQUIRE(!root_artifacts(&f, 0) && !root_artifacts(&f, 1));
        root_check(&f, 262161, !(codec & 2), etags);
        REQUIRE(append(f.collection, "after-root", 4097, &f.error).position ==
                3);
        reopen(&f);
        read_entry(f.collection, 3, 4097, &f.error);
        close_store(&f);
        f.settings.set_mask &= ~LC_POUCH_SETTING_COMPRESSION;
        open_client(&f, 0);
        root_check(&f, 262161, !(codec & 2), etags);
        close_store(&f);
        f.settings.set_mask |= LC_POUCH_SETTING_COMPRESSION;
        f.settings.compression = codec & 2 ? "zlib" : "none";
        open_client(&f, 0);
        REQUIRE(!root_artifacts(&f, 0) && !root_artifacts(&f, 1));
        root_check(&f, 262161, !!(codec & 2), etags);
        root_build_indexes(&f);
        close_store(&f);
        lc_error_cleanup(&f.error);
        REQUIRE(!unlink(f.path));
      }
  query_mode = original_query_mode;
  printf(
      "root codec conversion profiles=16 with reverse and inherited reopen\n");
}
typedef struct root_cut {
  disk_model *disk;
  size_t events, fail, scopes, scope, cut, used;
  int allocation;
  const char *labels[64];
} root_cut;
static int root_cut_event(void *context, const char *event, lc_error *e) {
  root_cut *cut = context;
  if (++cut->events == cut->fail) {
    cut->disk->freeze = 1;
    return lc_error_set(e, LC_ERR_TRANSPORT, 0L,
                        "interrupted root conversion boundary", NULL, NULL,
                        "test");
  }
  if (!strncmp(event, "before-", 7)) {
    ++cut->scopes;
    REQUIRE(cut->scopes <= 64);
    cut->labels[cut->scopes - 1] = event;
    if (cut->scopes == cut->scope) {
      if (cut->allocation) {
        allocation_calls = allocation_fired = 0;
        allocation_fail = cut->cut;
        allocation_thread = pthread_self();
        __atomic_store_n(&allocation_armed, 1, __ATOMIC_RELEASE);
      } else {
        cut->disk->calls = 0;
        cut->disk->cut = cut->cut;
        cut->disk->armed = 1;
      }
    }
  } else if (cut->scopes == cut->scope) {
    cut->used = cut->allocation ? allocation_calls : cut->disk->calls;
    __atomic_store_n(&allocation_armed, 0, __ATOMIC_RELEASE);
    cut->disk->armed = 0;
  }
  return LC_OK;
}
typedef struct root_seed_image {
  fixture base;
  char etags[3][65];
  unsigned char *image;
} root_seed_image;
static void root_image_open(root_seed_image *seed, const char *directory,
                            int codec, int sync) {
  root_seed(&seed->base, directory, codec, sync, 8193, 1, NULL, seed->etags);
  close_store(&seed->base);
  seed->image = malloc(PS_DEFAULT_CAPACITY);
  REQUIRE(seed->image);
  int fd = open(seed->base.path, O_RDONLY | O_CLOEXEC);
  REQUIRE(fd >= 0 &&
          pread(fd, seed->image, PS_DEFAULT_CAPACITY, 0) ==
              PS_DEFAULT_CAPACITY &&
          !close(fd));
  REQUIRE(!unlink(seed->base.path));
  lc_error_cleanup(&seed->base.error);
}
static size_t root_conversion_trial(root_seed_image *seed, int codec,
                                    size_t event, int request, size_t scope,
                                    size_t cut, int allocation, int mode,
                                    root_cut *observed) {
  disk_model model = {0};
  model.stable = calloc(1, PS_DEFAULT_CAPACITY);
  model.dirty = calloc(PS_DEFAULT_CAPACITY / PS_PAGE, 1);
  model.scratch = malloc(PS_PAGE);
  model.mode = mode;
  REQUIRE(model.stable && model.dirty && model.scratch);
  memcpy(model.stable, seed->image, PS_DEFAULT_CAPACITY);
  ps_io io = {&model, disk_read, disk_write, disk_sync, disk_reserve, NULL};
  fixture f = seed->base;
  lc_error_init(&f.error);
  f.endpoints[0] = f.endpoint;
  f.config.endpoints = f.endpoints;
  f.config.pouch_settings = &f.settings;
  f.io = &io;
  char etags[3][65];
  memcpy(etags, seed->etags, sizeof(etags));
  int fd = open(f.path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
  REQUIRE(fd >= 0 &&
          pwrite(fd, seed->image, PS_DEFAULT_CAPACITY, 0) ==
              PS_DEFAULT_CAPACITY &&
          !fdatasync(fd) && !close(fd));
  f.settings.compression = codec & 2 ? "none" : "zlib";
  root_cut state = {.disk = &model,
                    .fail = event,
                    .scope = scope,
                    .cut = cut,
                    .allocation = allocation};
  ps_root_conversion_test_context = &state;
  ps_root_conversion_test_hook = root_cut_event;
  ps_pouch_client_options physical = {0};
  physical.io = &io;
  int rc = ps_pouch_client_open(&f.config, 0, &physical, &f.client, &f.error);
  if (rc == LC_OK)
    lc_client_close(f.client);
  f.client = NULL;
  ps_root_conversion_test_hook = NULL;
  ps_root_conversion_test_context = NULL;
  size_t used = state.used;
  if (scope && state.scopes == scope && !used)
    used = allocation ? allocation_calls : model.calls;
  __atomic_store_n(&allocation_armed, 0, __ATOMIC_RELEASE);
  model.armed = 0;
  if (rc != LC_OK && rc != LC_ERR_NOMEM && rc != LC_ERR_TRANSPORT)
    fprintf(stderr, "root failure event=%zu scope=%zu cut=%zu rc=%d %s\n",
            event, scope, cut, rc, f.error.message ? f.error.message : "?");
  REQUIRE(rc == LC_OK || rc == LC_ERR_NOMEM || rc == LC_ERR_TRANSPORT);
  if (event != SIZE_MAX)
    REQUIRE(rc != LC_OK);
  if (scope && cut != SIZE_MAX) {
    REQUIRE(allocation ? allocation_fired : model.fired);
    if (rc == LC_OK)
      printf(
          "root conversion recovered injected %s scope=%zu cut=%zu mode=%d\n",
          allocation ? "allocation" : "I/O", scope, cut, mode);
  }
  clear(&f.error);
  restore_disk(&model, f.path);
  ps_space *raw = NULL;
  ps_key intent = {0, 2, PS_CONTROL}, config = {0, 1, PS_CONTROL};
  ps_ref ref;
  uint64_t length;
  int pending;
  ok(ps_open(f.path, 0, NULL, &raw, &f.error), &f.error);
  ok(ps_value_info(raw, intent, &ref, &length, &pending, &f.error), &f.error);
  ps_reader *reader = NULL;
  unsigned char bytes[128];
  ok(ps_read_open(raw, config, NULL, &reader, &f.error), &f.error);
  ok(ps_read_at(reader, 0, bytes, sizeof(bytes), &f.error), &f.error);
  ps_reader_close(reader);
  ok(ps_close(raw, &f.error), &f.error);
  if (request == 1)
    f.settings.set_mask &= ~LC_POUCH_SETTING_COMPRESSION;
  if (request == 2)
    f.settings.compression = codec & 2 ? "zlib" : "none";
  open_client(&f, 0);
  int policy = request == 1   ? (pending ? !(codec & 2) : bytes[9])
               : request == 2 ? !!(codec & 2)
                              : !(codec & 2);
  root_check(&f, 8193, policy, etags);
  close_store(&f);
  lc_error_cleanup(&f.error);
  REQUIRE(!unlink(f.path));
  free(model.stable);
  free(model.dirty);
  free(model.scratch);
  if (observed)
    *observed = state;
  return scope ? used : state.events;
}
static void root_conversion_faults(const char *directory, int codec, int sync) {
  root_seed_image seed;
  root_image_open(&seed, directory, codec, sync);
  root_cut observed;
  size_t events =
      root_conversion_trial(&seed, codec, SIZE_MAX, 0, 0, 0, 0, 0, &observed);
  REQUIRE(events > 10 && observed.scopes > 5);
  size_t total = 0;
  for (size_t event = 1; event <= events; ++event)
    for (int request = 0; request < 3; ++request) {
      (void)root_conversion_trial(&seed, codec, event, request, 0, 0, 0, 0,
                                  NULL);
      ++total;
    }
  printf("root conversion boundary failures codec=%d sync=%d points=%zu "
         "scopes=%zu\n",
         codec, sync, total, observed.scopes);
  free(seed.image);
}
static void root_conversion_controls(const char *directory, int codec,
                                     int sync) {
  root_seed_image seed;
  root_image_open(&seed, directory, codec, sync);
  root_cut observed;
  (void)root_conversion_trial(&seed, codec, SIZE_MAX, 0, 0, 0, 0, 0, &observed);
  size_t total = 0, progress = 0;
  for (size_t scope = 1; scope <= observed.scopes; ++scope) {
    const char *label = observed.labels[scope - 1];
    int publication =
        !strcmp(label, "before-progress") || !strcmp(label, "before-cleanup") ||
        !strcmp(label, "before-checkpoint") ||
        !strcmp(label, "before-policy") || !strcmp(label, "before-complete");
    if (!strcmp(label, "before-progress") && ++progress > 2)
      continue;
    size_t allocations = root_conversion_trial(&seed, codec, SIZE_MAX, 0, scope,
                                               SIZE_MAX, 1, 0, NULL);
    for (size_t cut = 1; cut <= allocations; ++cut) {
      (void)root_conversion_trial(&seed, codec, SIZE_MAX, 1, scope, cut, 1, 0,
                                  NULL);
      ++total;
    }
    if (publication) {
      size_t calls = root_conversion_trial(&seed, codec, SIZE_MAX, 0, scope,
                                           SIZE_MAX, 0, 0, NULL);
      for (int mode = 0; mode < 4; ++mode)
        for (size_t cut = 1; cut <= calls; ++cut) {
          (void)root_conversion_trial(&seed, codec, SIZE_MAX, 1, scope, cut, 0,
                                      mode, NULL);
          ++total;
        }
    }
    printf("root controls codec=%d sync=%d scope=%zu %s allocations=%zu "
           "total=%zu\n",
           codec, sync, scope, label, allocations, total);
    fflush(stdout);
  }
  free(seed.image);
  REQUIRE(total);
  printf("root coordinator control failure points=%zu\n", total);
}
static int root_kill_event(void *context, const char *event, lc_error *e) {
  (void)e;
  if (!strcmp(context, event))
    REQUIRE(!kill(getpid(), SIGKILL));
  return LC_OK;
}
static void root_conversion_crashes(const char *directory, int codec,
                                    int sync) {
  const char *events[] = {"progress", "collection-selected", "policy",
                          "complete"};
  for (size_t i = 0; i < sizeof(events) / sizeof(*events); ++i) {
    fixture f;
    char etags[3][65];
    root_seed(&f, directory, codec, sync, 8193, 1, NULL, etags);
    close_store(&f);
    f.settings.compression = codec & 2 ? "none" : "zlib";
    pid_t child = fork();
    REQUIRE(child >= 0);
    if (!child) {
      ps_root_conversion_test_context = (void *)events[i];
      ps_root_conversion_test_hook = root_kill_event;
      open_client(&f, 0);
      _exit(2);
    }
    int status;
    REQUIRE(waitpid(child, &status, 0) == child && WIFSIGNALED(status) &&
            WTERMSIG(status) == SIGKILL);
    f.settings.set_mask &= ~LC_POUCH_SETTING_COMPRESSION;
    open_client(&f, 0);
    root_check(&f, 8193, !(codec & 2), etags);
    close_store(&f);
    REQUIRE(!unlink(f.path));
    lc_error_cleanup(&f.error);
  }
  printf("root conversion process deaths codec=%d sync=%d points=4\n", codec,
         sync);
}
static void root_fill(fixture *f) {
  ps_space *space = ps_root_space(client_root(f));
  uint64_t lo = 0, hi = PS_DEFAULT_CAPACITY / PS_PAGE;
  while (lo + 1 < hi) {
    uint64_t mid = (lo + hi) / 2;
    ps_budget_plan plan = {0};
    ps_budget *budget = NULL;
    ok(ps_budget_plan_value(&plan, mid * PS_PAGE, &f->error), &f->error);
    int rc = ps_budget_open(space, &plan, &budget, &f->error);
    if (rc == LC_OK) {
      lo = mid;
      ok(ps_budget_close(budget, &f->error), &f->error);
    } else {
      REQUIRE(rc == LC_ERR_INVALID);
      clear(&f->error);
      hi = mid;
    }
  }
  REQUIRE(lo * PS_PAGE > PS_MIB);
  uint64_t bytes = lo * PS_PAGE - 65536;
  ps_budget_plan plan = {0};
  ps_budget *budget = NULL;
  ps_writer *writer = NULL;
  ps_key key = {UINT64_C(0x7fffffffffffffff), 1, PS_STATE};
  ok(ps_budget_plan_value(&plan, bytes, &f->error), &f->error);
  ok(ps_budget_open(space, &plan, &budget, &f->error), &f->error);
  ok(ps_begin(space, budget, key, &writer, &f->error), &f->error);
  unsigned char *scratch = calloc(1, PS_CHUNK);
  REQUIRE(scratch);
  for (uint64_t at = 0; at < bytes;) {
    size_t n = bytes - at < PS_CHUNK ? (size_t)(bytes - at) : PS_CHUNK;
    ok(ps_write(writer, scratch, n, &f->error), &f->error);
    at += n;
  }
  free(scratch);
  ok(ps_publish(writer, NULL, &f->error), &f->error);
  ps_writer_close(writer);
  ok(ps_budget_close(budget, &f->error), &f->error);
  ok(ps_flush(space, &f->error), &f->error);
}
static void root_conversion_growth(const char *directory) {
  for (int codec = 0; codec <= 3; codec += 3)
    for (int sync = 0; sync < 2; ++sync) {
      fixture f;
      char etags[3][65];
      root_seed(&f, directory, codec, sync, 262161, 0, NULL, etags);
      root_fill(&f);
      close_store(&f);
      f.settings.compression = codec & 2 ? "none" : "zlib";
      REQUIRE(ps_pouch_client_open(&f.config, 0, NULL, &f.client, &f.error) ==
                  LC_ERR_INVALID &&
              !f.client);
      clear(&f.error);
      ps_space *space = NULL;
      ps_key intent = {0, 2, PS_CONTROL};
      ps_ref ref;
      uint64_t bytes;
      int found;
      ok(ps_open(f.path, 0, NULL, &space, &f.error), &f.error);
      ok(ps_value_info(space, intent, &ref, &bytes, &found, &f.error),
         &f.error);
      REQUIRE(found);
      ok(ps_grow(space, PS_MIB, &f.error), &f.error);
      ok(ps_close(space, &f.error), &f.error);
      f.settings.set_mask &= ~LC_POUCH_SETTING_COMPRESSION;
      open_client(&f, 0);
      root_check(&f, 262161, !(codec & 2), etags);
      lc_collection_close(f.peer);
      lc_collection_close(f.collection);
      lc_client_close(f.client);
      struct stat status;
      REQUIRE(!stat(f.path, &status) &&
              status.st_size == PS_DEFAULT_CAPACITY + PS_MIB);
      REQUIRE(!unlink(f.path));
      lc_error_cleanup(&f.error);
    }
  printf("root conversion capacity resumes after explicit 1 MiB growth "
         "profiles=4\n");
}
#endif
