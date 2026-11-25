#ifndef POUCH_SPACE_COLLECTION_TRANSFORM_CASES_H
#define POUCH_SPACE_COLLECTION_TRANSFORM_CASES_H
#include "collection_reclaim.h"
#include "collection_transform.h"
#include "collection_walk_cases.h"
#include <time.h>

static void transform_root_reopen(record_fixture *f, workspace_fault *model,
                                  lc_error *e) {
  lc_collection_chunks_reader_close(&f->reader);
  ps_collection_segment_close(f->source);
  f->source = NULL;
  for (size_t i = 0; i < f->count; ++i)
    ps_collection_segment_close(f->parts[i].owner);
  f->count = 0;
  f->pouch->crypto = NULL;
  lc_pouch_close(f->pouch);
  ok(ps_root_close(f->root, e), e);
  if (model)
    workspace_fault_restore(model);
  ok(ps_root_open(f->path, 0, &f->options, &f->root, e), e);
  lc_pouch_open_options logical = {0};
  logical.background_compaction_enabled_set =
      logical.query_indexing_enabled_set = 1;
  logical.compression = "none";
  ok(lc_pouch_runtime_open(f->path, NULL, &logical, &f->pouch, e), e);
  f->pouch->crypto = ps_root_crypto(f->root);
}
static void transform_check(record_fixture *f, lc_error *e) {
  lc_collection_disk_head head;
  ps_collection_resources resources;
  ps_space *space = ps_root_space(f->root);
  ok(ps_collection_head_load(space, 73, NULL, &head, e), e);
  REQUIRE(head.tail_segment > 1 && head.active >= head.tail_segment);
  ok(ps_collection_resources_capture(space, 73, &resources, e), e);
  for (size_t i = 0; i < resources.count; ++i) {
    ps_collection_resource *r = &resources.items[i];
    if (r->kind == PS_CATALOG_CONTROL ||
        !lc_collection_segment_selected(&head, r->id))
      continue;
    REQUIRE(f->count < RECORD_PARTS);
    record_part *part = &f->parts[f->count++];
    part->id = r->id;
    ok(lc_collection_record_leaf(part->id, part->leaf, e), e);
    ok(ps_collection_segment_open(space, r->binding.stream, 0,
                                  &r->binding.target, NULL, NULL, &part->owner,
                                  e),
       e);
    part->access.io = ps_collection_segment_io(part->owner);
    part->access.watch = &f->watch;
  }
  ps_collection_resources_clear(&resources);
  const lc_pouch_collection_record *original[] = {
      &f->stream,    &f->meta[0], &f->meta[1], &f->small,
      &f->edit_body, &f->receipt, &f->deleted, &f->small_meta};
  typedef struct publication {
    lc_pouch_collection_record r;
    lc_u64 segment;
  } publication;
  publication *records = calloc(8, sizeof(*records));
  REQUIRE(records);
  size_t found = 0;
  for (size_t i = 0; i < f->count; ++i) {
    record_part *part = &f->parts[i];
    lc_u64 tail;
    ok(lc_pouch_record_io_tail(part->access.io, &tail, e), e);
    for (lc_u64 at = 0; at < tail;) {
      lc_pouch_collection_record r;
      ok(lc_pouch_collection_record_read(part->access.io, at, tail, part->leaf,
                                         &r, e),
         e);
      if (r.kind != LC_POUCH_COLLECTION_CHUNK &&
          r.kind != LC_POUCH_COLLECTION_NODE) {
        size_t j;
        for (j = 0; j < 8 && original[j]->generation != r.generation; ++j) {
        }
        REQUIRE(j < 8 && !records[j].segment);
        record_identity(original[j], &r);
        records[j].r = r;
        records[j].segment = part->id;
        ++found;
      }
      at = r.next_offset;
    }
  }
  REQUIRE(found == 8);
  REQUIRE(records[4].segment == records[5].segment &&
          records[4].r.next_offset == records[5].r.record_offset);
  for (size_t i = 0; i < 8; ++i) {
    publication *p = &records[i];
    if (p->r.stream_extents) {
      lc_u64 body_id;
      ok(lc_collection_record_segment(p->r.payload_container, &body_id, e), e);
      record_stream_check(f, &p->r, body_id, e);
    } else if (!p->r.deleted && !p->r.metadata_only) {
      ps_collection_segment *owner = NULL;
      for (size_t j = 0; j < f->count; ++j)
        if (f->parts[j].id == p->segment)
          owner = f->parts[j].owner;
      REQUIRE(owner);
      body(f->pouch, ps_collection_segment_io(owner), &p->r, e);
    }
    if (i == 1 || i == 2 || i == 7) {
      size_t prior = i == 7 ? 3 : i - 1;
      REQUIRE(p->r.previous_segment == records[prior].segment &&
              p->r.previous_offset == records[prior].r.record_offset);
      ok(lc_pouch_collection_record_check_body(&p->r,
                                               &records[i == 7 ? 3 : 0].r, e),
         e);
    }
  }
  free(records);
}
static void transform_cases(const char *directory, const char *key) {
  for (int codec = 0; codec < 4; ++codec)
    for (int sync = 0; sync < 2; ++sync)
      for (int large = 0; large < 2; ++large) {
        lc_error e;
        lc_error_init(&e);
        record_fixture *f =
            record_fixture_open(directory, key, codec, sync, large ? 3 : 66,
                                large ? 6 * PS_MIB + 17 : PS_MIB + 17, &e);
        lc_collection_chunks_reader_close(&f->reader);
        ps_collection_conversion *work = NULL;
        walk_bind_fixture(f, &work, &e);
        ps_collection_segment_close(f->source);
        f->source = NULL;
        ps_collection_transform *transform = NULL;
        REQUIRE(ps_collection_transform_open(
                    f->root, f->pouch, f->pouch->crypto, work, f->id, 65536,
                    &transform, &e) == LC_ERR_INVALID &&
                !transform);
        clear(&e);
        ok(ps_collection_transform_open(f->root, f->pouch, f->target, work,
                                        f->id, 65536, &transform, &e),
           &e);
        int done = 0;
        size_t calls = 0;
        ps_collection_transform_result result;
        while (!done) {
          ok(ps_collection_transform_step(transform, large ? 2 : 7, &done,
                                          &result, &e),
             &e);
          REQUIRE(++calls < 100);
          if (!done && calls == 2) {
            ps_collection_transform_close(transform);
            ps_collection_conversion_close(work);
            transform_root_reopen(f, NULL, &e);
            ok(ps_collection_conversion_open(ps_root_space(f->root), 73,
                                             f->target, 0, &work, &e),
               &e);
            ok(ps_collection_transform_open(f->root, f->pouch, f->target, work,
                                            f->id, 1, &transform, &e),
               &e);
          }
        }
        ps_stats before, after;
        ok(ps_status(ps_root_space(f->root), &before, &e), &e);
        ok(ps_collection_transform_step(transform, 1, &done, &result, &e), &e);
        ok(ps_status(ps_root_space(f->root), &after, &e), &e);
        REQUIRE(done && before.syncs == after.syncs &&
                after.capacity == 10 * PS_MIB);
        ps_collection_transform_close(transform);
        ps_collection_conversion_close(work);
        workspace_gc(f->root, &e);
        transform_root_reopen(f, NULL, &e);
        transform_check(f, &e);
        printf(
            "native collection transform codec=%d sync=%d large=%d steps=%zu\n",
            codec, sync, large, calls);
        record_fixture_close(f, &e);
        lc_error_cleanup(&e);
      }
}
typedef struct transform_parallel {
  pthread_mutex_t mutex;
  pthread_cond_t changed;
  record_fixture *fixture;
  ps_collection_transform *transform;
  int armed, paused, released, peer_done, converted, written;
} transform_parallel;
static ssize_t transform_pause_read(void *context, int fd, void *p, size_t n,
                                    off_t at) {
  transform_parallel *state = context;
  pthread_mutex_lock(&state->mutex);
  if (state->armed && at >= PS_DATA && n >= 8192) {
    state->armed = 0;
    state->paused = 1;
    pthread_cond_broadcast(&state->changed);
    while (!state->released)
      pthread_cond_wait(&state->changed, &state->mutex);
  }
  pthread_mutex_unlock(&state->mutex);
  return pread(fd, p, n, at);
}
static ssize_t transform_parallel_write(void *context, int fd, const void *p,
                                        size_t n, off_t at) {
  (void)context;
  return pwrite(fd, p, n, at);
}
static int transform_parallel_sync(void *context, int fd) {
  (void)context;
  return ps_native_io.sync(NULL, fd);
}
static int transform_parallel_reserve(void *context, int fd, off_t at,
                                      off_t length) {
  (void)context;
  return ps_native_io.reserve(NULL, fd, at, length);
}
static void *transform_parallel_convert(void *context) {
  transform_parallel *state = context;
  lc_error e;
  lc_error_init(&e);
  ps_collection_transform_result result;
  int done = 0, rc = LC_OK;
  while (rc == LC_OK && !done)
    rc = ps_collection_transform_step(state->transform, 64, &done, &result, &e);
  state->converted = rc;
  lc_error_cleanup(&e);
  return NULL;
}
static void *transform_parallel_peer(void *context) {
  transform_parallel *state = context;
  lc_error e;
  lc_error_init(&e);
  ps_collection_segment *peer = NULL;
  ps_key key = {74, 1, PS_STREAM};
  unsigned char bytes[2048];
  memset(bytes, 0x74, sizeof(bytes));
  struct iovec part = {bytes, sizeof(bytes)};
  int rc = ps_collection_segment_open(ps_root_space(state->fixture->root), key,
                                      1, NULL, NULL, NULL, &peer, &e);
  for (size_t i = 0; rc == LC_OK && i < 64; ++i)
    rc =
        lc_pouch_record_io_append(ps_collection_segment_io(peer), &part, 1, &e);
  if (rc == LC_OK)
    rc = ps_collection_segment_seal(peer, &e);
  ps_collection_segment_close(peer);
  state->written = rc;
  pthread_mutex_lock(&state->mutex);
  state->peer_done = 1;
  pthread_cond_broadcast(&state->changed);
  pthread_mutex_unlock(&state->mutex);
  lc_error_cleanup(&e);
  return NULL;
}
static void transform_concurrency(const char *directory, const char *key) {
  for (int codec = 0; codec < 4; ++codec)
    for (int sync = 0; sync < 2; ++sync) {
      transform_parallel state = {0};
      REQUIRE(!pthread_mutex_init(&state.mutex, NULL));
      REQUIRE(!pthread_cond_init(&state.changed, NULL));
      ps_io io = {&state,
                  transform_pause_read,
                  transform_parallel_write,
                  transform_parallel_sync,
                  transform_parallel_reserve,
                  NULL};
      lc_error e;
      lc_error_init(&e);
      record_fixture *f = record_fixture_open_io(directory, key, codec, sync, 3,
                                                 65537, &io, 1, &e);
      state.fixture = f;
      lc_collection_chunks_reader_close(&f->reader);
      ps_collection_conversion *work = NULL;
      walk_bind_fixture(f, &work, &e);
      ps_collection_segment_close(f->source);
      f->source = NULL;
      ok(ps_collection_transform_open(f->root, f->pouch, f->target, work, f->id,
                                      16384, &state.transform, &e),
         &e);
      state.armed = 1;
      pthread_t converter, writer;
      REQUIRE(!pthread_create(&converter, NULL, transform_parallel_convert,
                              &state));
      struct timespec until;
      REQUIRE(!clock_gettime(CLOCK_REALTIME, &until));
      until.tv_sec += 10;
      int rc = 0;
      pthread_mutex_lock(&state.mutex);
      while (!state.paused && !rc)
        rc = pthread_cond_timedwait(&state.changed, &state.mutex, &until);
      int paused = state.paused;
      pthread_mutex_unlock(&state.mutex);
      if (paused)
        REQUIRE(
            !pthread_create(&writer, NULL, transform_parallel_peer, &state));
      REQUIRE(!clock_gettime(CLOCK_REALTIME, &until));
      until.tv_sec += 10;
      rc = 0;
      pthread_mutex_lock(&state.mutex);
      while (paused && !state.peer_done && !rc)
        rc = pthread_cond_timedwait(&state.changed, &state.mutex, &until);
      int independent = state.peer_done;
      state.released = 1;
      pthread_cond_broadcast(&state.changed);
      pthread_mutex_unlock(&state.mutex);
      REQUIRE(!pthread_join(converter, NULL));
      if (paused)
        REQUIRE(!pthread_join(writer, NULL));
      REQUIRE(paused && independent && state.converted == LC_OK &&
              state.written == LC_OK);
      ps_collection_transform_close(state.transform);
      ps_collection_conversion_close(work);
      transform_root_reopen(f, NULL, &e);
      transform_check(f, &e);
      record_fixture_close(f, &e);
      lc_error_cleanup(&e);
      REQUIRE(!pthread_cond_destroy(&state.changed));
      REQUIRE(!pthread_mutex_destroy(&state.mutex));
    }
  printf("native collection controller independent-writer profiles=8\n");
}
static size_t transform_fault_trial(const char *directory, const char *key,
                                    workspace_fault *model, int codec, int sync,
                                    int mode, size_t cut, int prepare_only) {
  lc_error e;
  lc_error_init(&e);
  workspace_fault_reset(model, mode);
  ps_io io = {model,          workspace_rd,      workspace_wr,
              workspace_sync, workspace_reserve, NULL};
  record_fixture *f = record_fixture_open_io(directory, key, codec, sync, 3,
                                             65537, &io, !prepare_only, &e);
  lc_collection_chunks_reader_close(&f->reader);
  ps_collection_conversion *work = NULL;
  walk_bind_fixture(f, &work, &e);
  ps_collection_segment_close(f->source);
  f->source = NULL;
  ok(ps_flush(ps_root_space(f->root), &e), &e);
  ps_collection_transform *transform = NULL;
  unit_allocations = 0;
  unit_fail_allocation = mode == 0 ? cut : 0;
  model->calls = 0;
  model->fail = mode ? cut : 0;
  int rc = ps_collection_transform_open(f->root, f->pouch, f->target, work,
                                        f->id, 16384, &transform, &e);
  size_t prepare_calls = mode ? model->calls : unit_allocations;
  if (prepare_only)
    unit_fail_allocation = model->fail = 0;
  int done = 0;
  ps_collection_transform_result result;
  while (rc == LC_OK && !done)
    rc = ps_collection_transform_step(transform, 64, &done, &result, &e);
  size_t calls = prepare_only ? prepare_calls
                 : mode       ? model->calls
                              : unit_allocations;
  unit_fail_allocation = model->fail = 0;
  REQUIRE(cut == SIZE_MAX || calls >= cut);
  if (rc != LC_OK && rc != LC_ERR_NOMEM && rc != LC_ERR_TRANSPORT)
    fprintf(stderr,
            "transform fault codec=%d sync=%d mode=%d cut=%zu rc=%d %s\n",
            codec, sync, mode, cut, rc, e.message ? e.message : "?");
  REQUIRE(rc == LC_OK || rc == LC_ERR_NOMEM || rc == LC_ERR_TRANSPORT);
  clear(&e);
  ps_collection_transform_close(transform);
  ps_collection_conversion_close(work);
  transform_root_reopen(f, mode ? model : NULL, &e);
  work = NULL;
  rc = ps_collection_conversion_open(ps_root_space(f->root), 73, f->target, 0,
                                     &work, &e);
  if (rc == LC_OK) {
    ok(ps_collection_transform_open(f->root, f->pouch, f->target, work, f->id,
                                    65536, &transform, &e),
       &e);
    done = 0;
    while (!done)
      ok(ps_collection_transform_step(transform, 64, &done, &result, &e), &e);
    ps_collection_transform_close(transform);
    ps_collection_conversion_close(work);
  } else {
    REQUIRE(rc == LC_ERR_INVALID && !work);
    clear(&e);
    lc_collection_disk_head head;
    ok(ps_collection_head_load(ps_root_space(f->root), 73, NULL, &head, &e),
       &e);
    REQUIRE(head.tail_segment > 1);
  }
  workspace_gc(f->root, &e);
  transform_root_reopen(f, NULL, &e);
  transform_check(f, &e);
  record_fixture_close(f, &e);
  lc_error_cleanup(&e);
  return calls;
}
static void transform_faults(const char *directory, const char *key,
                             unsigned int first, unsigned int last,
                             int prepare_only) {
  size_t total = 0;
  unsigned int group = 0;
  /* Keep the same barrier and torn-write model in memory across trials.
   * The sparse fixture still supplies the inode used by real root opens and
   * locks. The ordinary transform cases exercise native file I/O. */
  workspace_fault model = {0};
  workspace_fault_open(&model);
  for (int codec = 0; codec <= 3; codec += 3)
    for (int sync = 0; sync < 2; ++sync)
      for (int mode = 0; mode < 4; ++mode) {
        if (group < first || group >= last) {
          ++group;
          continue;
        }
        ++group;
        size_t calls = transform_fault_trial(
            directory, key, &model, codec, sync, mode, SIZE_MAX, prepare_only);
        for (size_t cut = 1; cut <= calls; ++cut) {
          transform_fault_trial(directory, key, &model, codec, sync, mode, cut,
                                prepare_only);
          ++total;
        }
        printf("transform faults prepare=%d group=%u codec=%d sync=%d mode=%d "
               "cuts=%zu\n",
               prepare_only, group - 1, codec, sync, mode, calls);
        fflush(stdout);
      }
  REQUIRE(total);
  workspace_fault_close(&model);
  printf("native collection transform failure points=%zu\n", total);
}
#endif
