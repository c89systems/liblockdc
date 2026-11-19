#ifndef POUCH_SPACE_COLLECTION_WORKSPACE_CASES_H
#define POUCH_SPACE_COLLECTION_WORKSPACE_CASES_H
#include "collection_conversion.h"
#include <errno.h>
#include <fcntl.h>

typedef struct workspace_output {
  ps_collection_conversion *work;
  ps_collection_segment *segment;
  lc_u64 id;
} workspace_output;
static void workspace_fill(ps_collection_segment *, lc_error *);
static int workspace_output_io(void *context, lc_u64 bound, int index,
                               lc_pouch_record_io *io, lc_u64 *segment,
                               lc_error *e) {
  workspace_output *o = context;
  REQUIRE(bound && (index == 0 || index == 1));
  (void)e;
  *io = ps_collection_segment_io(o->segment);
  *segment = o->id;
  return LC_OK;
}
static int workspace_read_open(void *context, lc_u64 id, lc_pouch_record_io *io,
                               void **pin, lc_error *e) {
  ps_collection_segment *segment = NULL;
  workspace_output *access = context;
  int rc = LC_OK;
  if (id == access->id) {
    segment = access->segment;
    ps_collection_segment_retain(segment);
  } else
    rc = ps_collection_conversion_candidate_open(access->work, id, &segment, e);
  if (rc == LC_OK) {
    *io = ps_collection_segment_io(segment);
    *pin = segment;
  }
  return rc;
}
static void workspace_read_close(void *context, void *pin) {
  (void)context;
  ps_collection_segment_close(pin);
}
static void workspace_gc(ps_root *root, lc_error *e) {
  for (int pass = 0; pass < 3; ++pass) {
    uint64_t reclaimed, skipped;
    ok(ps_root_reclaim_orphans(root, &reclaimed, &skipped, e), e);
    ok(ps_checkpoint(ps_root_space(root), e), e);
  }
}
static ps_catalog_info workspace_source(ps_collection_segment *source,
                                        ps_key key, lc_error *e) {
  ps_owned_status status;
  ok(ps_collection_segment_status(source, &status, NULL, e), e);
  ps_catalog_info info = {.stream = key, .target = status.ref};
  return info;
}
static void workspace_decode(record_fixture *f, ps_collection_conversion *w,
                             const lc_pouch_collection_record *r, lc_u64 id,
                             lc_error *e) {
  ps_collection_segment *owner = NULL;
  ok(ps_collection_conversion_candidate_open(w, id, &owner, e), e);
  lc_collection_chunk_reader *reader = calloc(1, sizeof(*reader));
  REQUIRE(reader);
  workspace_output access = {w, owner, id};
  lc_collection_read_storage storage = {workspace_read_open,
                                        workspace_read_close, &access};
  ok(lc_collection_chunks_open(reader, &f->pouch->allocator, f->target, f->id,
                               storage, ps_collection_segment_io(owner), r, e),
     e);
  unsigned char bytes[2048];
  lc_u64 at = 0;
  size_t got;
  while (
      (got = reader->source.read(&reader->source, bytes, sizeof(bytes), e))) {
    REQUIRE(got <= r->bytes - at);
    for (size_t i = 0; i < got; ++i)
      REQUIRE(bytes[i] == content_at(at + i, r->bytes));
    at += got;
  }
  ok(e->code, e);
  REQUIRE(at == r->bytes);
  lc_collection_chunks_reader_close(reader);
  free(reader);
  ps_collection_segment_close(owner);
}
static void workspace_records(const char *directory, const char *key) {
  for (int codec = 0; codec < 4; ++codec)
    for (int sync = 0; sync < 2; ++sync) {
      lc_error e;
      lc_error_init(&e);
      record_fixture *f =
          record_fixture_open(directory, key, codec, sync, 3, PS_MIB + 17, &e);
      ps_space *s = ps_root_space(f->root);
      lc_collection_disk_head source_head = {.active = 1, .tail_segment = 1};
      ps_ref source_ref;
      ok(ps_collection_head_store(s, 73, &source_head, &source_ref, &e), &e);
      ps_collection_conversion *w = NULL, *other = NULL;
      ok(ps_collection_conversion_open(s, 73, f->target, 1, &w, &e), &e);
      REQUIRE(ps_collection_conversion_open(s, 73, f->target, 1, &other, &e) ==
                  LC_ERR_INVALID &&
              !other);
      clear(&e);
      ps_key source_key = {73, 1, PS_STREAM};
      ps_catalog_info source = workspace_source(f->source, source_key, &e);
      ok(ps_collection_conversion_source_bind(w, 1, &source, &e), &e);
      ok(ps_collection_conversion_source_bind(w, 1, &source, &e), &e);
      source.target.incarnation++;
      REQUIRE(ps_collection_conversion_source_bind(w, 1, &source, NULL) ==
              LC_ERR_INVALID);
      workspace_output output = {.work = w, .id = 1000};
      ok(ps_collection_conversion_candidate_create(w, output.id,
                                                   &output.segment, &e),
         &e);
      lc_collection_stream_recode *build = NULL;
      ok(lc_collection_stream_recode_open(&f->pouch->allocator, f->target,
                                          &f->reader, workspace_output_io,
                                          &output, &build, &e),
         &e);
      if (codec == 0 && sync == 0)
        record_paused_step(f, build, &e);
      int finished = 0;
      size_t visited;
      lc_collection_recode_stream stream;
      while (!finished)
        ok(lc_collection_stream_recode_step(build, 1, &visited, &finished,
                                            &stream, &e),
           &e);
      lc_collection_stream_recode_close(build);
      lc_collection_recode_links links = {.stream = &stream};
      lc_pouch_collection_record converted;
      char target_leaf[64];
      ok(lc_collection_record_leaf(output.id, target_leaf, &e), &e);
      ok(lc_pouch_collection_record_recode(
             f->pouch, f->target, f->id, record_source_io(f), leaf,
             f->stream.record_offset, f->stream.next_offset, &links,
             ps_collection_segment_io(output.segment), target_leaf, &converted,
             &e),
         &e);
      record_identity(&f->stream, &converted);
      ps_collection_relocation mapping = {1, f->stream.record_offset, output.id,
                                          converted.record_offset};
      REQUIRE(ps_collection_conversion_checkpoint(w, 1, f->stream.next_offset,
                                                  &mapping, 1,
                                                  &e) == LC_ERR_PENDING);
      clear(&e);
      ok(ps_collection_segment_seal(output.segment, &e), &e);
      ps_collection_segment_close(output.segment);
      output.segment = NULL;
      /* Catalog protection survives closed byte owners and ordinary GC even
       * before the progress publication. */
      workspace_gc(f->root, &e);
      ok(ps_collection_conversion_checkpoint(w, 1, f->stream.next_offset,
                                             &mapping, 1, &e),
         &e);
      ps_collection_conversion_progress progress;
      ps_collection_conversion_status(w, &progress);
      REQUIRE(progress.pages == 1 && progress.records == 1 &&
              progress.candidates == 1);
      workspace_decode(f, w, &converted, mapping.target_segment, &e);
      /* A later sealed candidate is still speculative. Reopen must discard it
       * while retaining the selected stream and its relocation page. */
      ok(ps_collection_conversion_candidate_create(w, 1001, &output.segment,
                                                   &e),
         &e);
      ok(ps_collection_segment_seal(output.segment, &e), &e);
      ps_collection_segment_close(output.segment);
      ps_collection_conversion_close(w);
      lc_collection_chunks_reader_close(&f->reader);
      ps_collection_segment_close(f->source);
      f->source = NULL;
      f->pouch->crypto = NULL;
      lc_pouch_close(f->pouch);
      ok(ps_root_close(f->root, &e), &e);
      ok(ps_root_open(f->path, 0, &f->options, &f->root, &e), &e);
      s = ps_root_space(f->root);
      lc_pouch_open_options logical = {0};
      logical.background_compaction_enabled_set =
          logical.query_indexing_enabled_set = 1;
      logical.compression = "none";
      ok(lc_pouch_runtime_open(f->path, NULL, &logical, &f->pouch, &e), &e);
      f->pouch->crypto = ps_root_crypto(f->root);
      REQUIRE(ps_collection_conversion_open(s, 73, f->pouch->crypto, 0, &other,
                                            &e) == LC_ERR_INVALID &&
              !other);
      clear(&e);
      if (codec == 1 && !sync) {
        char *foreign_key = NULL;
        lc_pouch_crypto *foreign = NULL;
        ok(lc_pouch_crypto_generate_key_string(&foreign_key, &e), &e);
        lc_pouch_crypto_open_options foreign_options = {
            .key_string = foreign_key, .compression_enabled = 1};
        ok(lc_pouch_crypto_open(NULL, &foreign_options, &foreign, NULL, &e),
           &e);
        REQUIRE(ps_collection_conversion_open(s, 73, foreign, 0, &other, &e) ==
                    LC_ERR_INVALID &&
                !other);
        clear(&e);
        lc_pouch_crypto_close(foreign);
        lc_pouch_crypto_key_string_free(foreign_key);
      }
      ok(ps_collection_conversion_open(s, 73, f->target, 0, &w, &e), &e);
      REQUIRE(ps_collection_conversion_candidate_open(w, 1001, &output.segment,
                                                      &e) == LC_ERR_PROTOCOL &&
              !output.segment);
      clear(&e);
      workspace_gc(f->root, &e);
      ps_catalog_info captured;
      ok(ps_collection_conversion_source_get(w, 1, &captured, &e), &e);
      ok(ps_collection_segment_open(s, captured.stream, 0, &captured.target,
                                    NULL, NULL, &f->source, &e),
         &e);
      f->input.io = ps_collection_segment_io(f->source);
      ps_ref captured_ref;
      lc_collection_disk_head captured_head;
      ps_collection_conversion_source_head(w, &captured_head, &captured_ref);
      REQUIRE(captured_head.active == 1 &&
              captured_ref.incarnation == source_ref.incarnation &&
              !memcmp(captured_ref.container, source_ref.container, 16));
      ps_collection_relocation resolved = {0};
      int present = 0;
      ok(ps_collection_conversion_lookup(w, mapping.source_segment,
                                         mapping.source_offset, &resolved,
                                         &present, &e),
         &e);
      REQUIRE(present && !memcmp(&mapping, &resolved, sizeof(mapping)));
      workspace_decode(f, w, &converted, mapping.target_segment, &e);
      record_stream_check(f, &f->stream, 1, &e);
      /* Several pages, gaps, snapshots-before-normal traversal and an empty
       * cursor-only publication exercise lookup without a whole-history map. */
      ps_collection_relocation rows[PS_COLLECTION_CONVERSION_BATCH];
      for (size_t page = 0; page < 5; ++page) {
        for (size_t i = 0; i < PS_COLLECTION_CONVERSION_BATCH; ++i)
          rows[i] = (ps_collection_relocation){2 + page, 2 * i, 1000,
                                               converted.record_offset};
        ok(ps_collection_conversion_checkpoint(
               w, 2 + page, 128, rows, PS_COLLECTION_CONVERSION_BATCH, &e),
           &e);
      }
      for (lc_u64 segment = 2; segment < 7; ++segment)
        for (lc_u64 at = 0; at < 129; ++at) {
          ok(ps_collection_conversion_lookup(w, segment, at, &resolved,
                                             &present, &e),
             &e);
          REQUIRE(present == (at < 128 && !(at & 1)));
          if (present)
            REQUIRE(resolved.source_segment == segment &&
                    resolved.source_offset == at &&
                    resolved.target_segment == 1000);
        }
      REQUIRE(ps_collection_conversion_checkpoint(w, 1, 0, NULL, 0, NULL) ==
              LC_ERR_INVALID);
      REQUIRE(ps_collection_conversion_checkpoint(
                  w, LC_COLLECTION_SNAPSHOT_BIT + 1, 0, NULL, 0, NULL) ==
              LC_ERR_INVALID);
      ok(ps_collection_conversion_checkpoint(w, 7, 0, NULL, 0, &e), &e);
      ps_collection_relocation behind = {6, 127, 1000, converted.record_offset};
      REQUIRE(ps_collection_conversion_checkpoint(w, 7, 0, &behind, 1, NULL) ==
              LC_ERR_INVALID);
      /* Reusing a rolled-back logical ID gets a fresh physical identity. */
      ok(ps_collection_conversion_candidate_create(w, 1001, &output.segment,
                                                   &e),
         &e);
      ok(ps_collection_segment_seal(output.segment, &e), &e);
      ps_collection_segment_close(output.segment);
      ok(ps_collection_conversion_discard(w, &e), &e);
      ps_collection_conversion_close(w);
      workspace_gc(f->root, &e);
      REQUIRE(ps_collection_conversion_open(s, 73, f->target, 0, &other,
                                            NULL) == LC_ERR_INVALID &&
              !other);
      /* Old source owner remains readable after all workspace references go. */
      record_stream_check(f, &f->stream, 1, &e);
      if (!codec && !sync) {
        ok(ps_collection_conversion_open(s, 73, f->target, 1, &w, &e), &e);
        REQUIRE(ps_collection_conversion_reclaim(s, 73, NULL) ==
                LC_ERR_INVALID);
        for (lc_u64 i = 0; i < PS_COLLECTION_CONVERSION_BATCH; ++i) {
          ok(ps_collection_conversion_candidate_create(
                 w, LC_COLLECTION_SNAPSHOT_BIT + 100 + i, &output.segment, &e),
             &e);
          if (!i)
            workspace_fill(output.segment, &e);
          else
            ok(ps_collection_segment_seal(output.segment, &e), &e);
          ps_collection_segment_close(output.segment);
        }
        REQUIRE(ps_collection_conversion_candidate_create(
                    w, 999, &output.segment, NULL) == LC_ERR_PENDING &&
                !output.segment);
        ps_collection_relocation snapshot = {LC_COLLECTION_SNAPSHOT_BIT + 1, 0,
                                             LC_COLLECTION_SNAPSHOT_BIT + 100,
                                             0};
        ok(ps_collection_conversion_checkpoint(
               w, LC_COLLECTION_SNAPSHOT_BIT + 1, 1, &snapshot, 1, &e),
           &e);
        ok(ps_collection_conversion_checkpoint(w, 1, 0, NULL, 0, &e), &e);
        ok(ps_collection_conversion_lookup(w, snapshot.source_segment, 0,
                                           &resolved, &present, &e),
           &e);
        REQUIRE(present && !memcmp(&resolved, &snapshot, sizeof(snapshot)));
        REQUIRE(ps_collection_conversion_candidate_create(
                    w, snapshot.target_segment, &output.segment, NULL) ==
                    LC_ERR_INVALID &&
                !output.segment);
        ps_owned_head *identity = NULL;
        ps_owned_status identity_status;
        ps_key identity_key = {73, 0, PS_STREAM};
        ok(ps_owned_open(s, NULL, identity_key, 1, NULL, &identity,
                         &identity_status, &e),
           &e);
        ps_owned_close(identity);
        workspace_gc(f->root, &e);
        /* The unbound collection identity cannot cascade through a converter
         * holding its anchor. Once it closes, the same orphan hook cleans the
         * workspace, its controls and collection head without a special pass.
         */
        ok(ps_collection_head_load(s, 73, &source_ref, &captured_head, &e), &e);
        ps_collection_conversion_close(w);
        workspace_gc(f->root, &e);
        REQUIRE(ps_collection_head_load(s, 73, NULL, &captured_head, NULL) ==
                LC_ERR_INVALID);
      }
      printf("native workspace codec=%d sync=%d records=321 pages=6\n", codec,
             sync);
      record_fixture_close(f, &e);
      lc_error_cleanup(&e);
    }
}

typedef struct workspace_fault {
  unsigned char *stable;
  unsigned char *dirty;
  size_t calls, fail;
  int mode;
} workspace_fault;
static int workspace_cut(workspace_fault *m) {
  return m->fail && ++m->calls == m->fail;
}
static ssize_t workspace_rd(void *context, int fd, void *p, size_t n,
                            off_t at) {
  workspace_fault *m = context;
  if (workspace_cut(m)) {
    errno = EIO;
    return -1;
  }
  return pread(fd, p, n, at);
}
static ssize_t workspace_wr(void *context, int fd, const void *p, size_t n,
                            off_t at) {
  workspace_fault *m = context;
  REQUIRE(at >= 0 && (uint64_t)at + n <= 10 * PS_MIB);
  for (size_t page = (size_t)at / PS_PAGE;
       page < ((size_t)at + n + PS_PAGE - 1) / PS_PAGE; ++page)
    m->dirty[page] = 1;
  if (workspace_cut(m)) {
    if (m->mode >= 2 && n > 1) {
      REQUIRE(pwrite(fd, p, n / 2, at) == (ssize_t)(n / 2));
      if (m->mode == 3)
        memcpy(m->stable + at, p, n / 2);
    }
    errno = EIO;
    return -1;
  }
  return pwrite(fd, p, n, at);
}
static int workspace_sync(void *context, int fd) {
  workspace_fault *m = context;
  if (workspace_cut(m)) {
    errno = EIO;
    return -1;
  }
  int rc = fdatasync(fd);
  if (!rc)
    for (size_t page = 0; page < 10 * PS_MIB / PS_PAGE; ++page)
      if (m->dirty[page]) {
        REQUIRE(pread(fd, m->stable + page * PS_PAGE, PS_PAGE,
                      page * PS_PAGE) == PS_PAGE);
        m->dirty[page] = 0;
      }
  return rc;
}
static int workspace_reserve(void *context, int fd, off_t at, off_t length) {
  workspace_fault *m = context;
  if (workspace_cut(m)) {
    errno = EIO;
    return -1;
  }
  return posix_fallocate(fd, at, length) ? (errno = ENOSPC, -1) : 0;
}
static void workspace_fill(ps_collection_segment *segment, lc_error *e) {
  unsigned char bytes[128];
  memset(bytes, 0x5a, sizeof(bytes));
  struct iovec part = {bytes, sizeof(bytes)};
  ok(lc_pouch_record_io_append(ps_collection_segment_io(segment), &part, 1, e),
     e);
  ok(ps_collection_segment_seal(segment, e), e);
}
static size_t workspace_fault_trial(const char *directory, const char *key_text,
                                    int sync, int stage, int mode, size_t cut) {
  char path[1024];
  REQUIRE(snprintf(path, sizeof(path), "%s/workspace-fault", directory) > 0);
  workspace_fault model = {.stable = calloc(1, 10 * PS_MIB),
                           .dirty = calloc(1, 10 * PS_MIB / PS_PAGE),
                           .mode = mode};
  REQUIRE(model.stable && model.dirty);
  ps_io io = {&model,         workspace_rd,      workspace_wr,
              workspace_sync, workspace_reserve, NULL};
  ps_root_options options = {0};
  options.storage.initial_capacity = options.storage.maximum_capacity =
      10 * PS_MIB;
  options.storage.durable_sync = sync;
  options.storage.io = &io;
  options.crypto.key_string = sync ? key_text : NULL;
  lc_error e;
  lc_error_init(&e);
  ps_root *root = NULL;
  ok(ps_root_open(path, 1, &options, &root, &e), &e);
  ps_space *s = ps_root_space(root);
  uint64_t collection;
  ok(ps_catalog_allocate_id(s, &collection, &e), &e);
  ps_key source_key = {collection, 1, PS_STREAM};
  ps_collection_segment *source = NULL, *candidate = NULL;
  ok(ps_collection_segment_open(s, source_key, 1, NULL, NULL, NULL, &source,
                                &e),
     &e);
  if (stage == 7) {
    ok(ps_collection_segment_reserve(source, 2 * PS_MIB, &e), &e);
    ok(ps_collection_segment_truncate(source, 0, &e), &e);
  }
  workspace_fill(source, &e);
  ps_catalog_info info = workspace_source(source, source_key, &e), binding;
  unsigned char name[8] = {1};
  ok(ps_catalog_put(s, NULL, collection, PS_CATALOG_SEGMENT, name, 8, NULL,
                    source_key, &info.target, &binding, &e),
     &e);
  lc_collection_disk_head head = {.active = 1, .tail_segment = 1};
  ok(ps_collection_head_store(s, collection, &head, NULL, &e), &e);
  ps_collection_conversion *w = NULL;
  lc_pouch_crypto *target = NULL;
  lc_pouch_crypto_open_options crypto = options.crypto;
  crypto.compression_enabled = 1;
  ok(lc_pouch_crypto_open(NULL, &crypto, &target, NULL, &e), &e);
  ps_collection_relocation row = {1, 0, 1000, 0};
  if (stage >= 1)
    ok(ps_collection_conversion_open(s, collection, target, 1, &w, &e), &e);
  if (stage >= 2)
    ok(ps_collection_conversion_source_bind(w, 1, &info, &e), &e);
  if (stage >= 3) {
    ok(ps_collection_conversion_candidate_create(w, 1000, &candidate, &e), &e);
    workspace_fill(candidate, &e);
    ps_collection_segment_close(candidate);
    candidate = NULL;
  }
  if (stage >= 4)
    ok(ps_collection_conversion_checkpoint(w, 1, 128, &row, 1, &e), &e);
  if (stage == 4) {
    ok(ps_collection_conversion_candidate_create(w, 1001, &candidate, &e), &e);
    workspace_fill(candidate, &e);
    ps_collection_segment_close(candidate);
    candidate = NULL;
    ps_collection_conversion_close(w);
    w = NULL;
  }
  if (stage == 6) {
    ps_collection_conversion_close(w);
    w = NULL;
  }
  /* Establish the pre-operation durable image even on NoSync roots. */
  ok(ps_flush(s, &e), &e);
  unit_allocations = 0;
  unit_fail_allocation = mode == 0 ? cut : 0;
  model.calls = 0;
  model.fail = mode ? cut : 0;
  int rc;
  uint64_t removed = 999;
  switch (stage) {
  case 0:
    rc = ps_collection_conversion_open(s, collection, target, 1, &w, &e);
    break;
  case 1:
    rc = ps_collection_conversion_source_bind(w, 1, &info, &e);
    break;
  case 2:
    rc = ps_collection_conversion_candidate_create(w, 1000, &candidate, &e);
    break;
  case 3:
    rc = ps_collection_conversion_checkpoint(w, 1, 128, &row, 1, &e);
    break;
  case 4:
    rc = ps_collection_conversion_open(s, collection, target, 0, &w, &e);
    break;
  case 5:
    rc = ps_collection_conversion_discard(w, &e);
    break;
  case 7:
    rc = ps_collection_conversion_source_prune(w, 1, source, &removed, &e);
    REQUIRE(rc == LC_OK || removed == 999);
    break;
  default:
    rc = ps_collection_conversion_reclaim(s, collection, &e);
    break;
  }
  size_t calls = mode ? model.calls : unit_allocations;
  unit_fail_allocation = model.fail = 0;
  REQUIRE(rc == LC_OK || rc == LC_ERR_NOMEM || rc == LC_ERR_TRANSPORT);
  clear(&e);
  ps_collection_segment_close(candidate);
  ps_collection_conversion_close(w);
  ps_collection_segment_close(source);
  ok(ps_root_close(root, &e), &e);
  if (mode) {
    int fd = open(path, O_RDWR | O_CLOEXEC);
    REQUIRE(fd >= 0 &&
            pwrite(fd, model.stable, 10 * PS_MIB, 0) == 10 * PS_MIB &&
            !fdatasync(fd) && !close(fd));
  }
  ok(ps_root_open(path, 0, &options, &root, &e), &e);
  s = ps_root_space(root);
  ok(ps_collection_conversion_open(s, collection, target, 1, &w, &e), &e);
  ok(ps_collection_conversion_source_bind(w, 1, &info, &e), &e);
  ps_collection_conversion_progress progress;
  ps_collection_conversion_status(w, &progress);
  REQUIRE(progress.records <= 1 && progress.pages == progress.records);
  ps_collection_relocation found;
  int present = -1;
  ok(ps_collection_conversion_lookup(w, 1, 0, &found, &present, &e), &e);
  REQUIRE(present == (int)progress.records);
  workspace_gc(root, &e);
  if (!progress.records) {
    ok(ps_collection_conversion_candidate_create(w, 1000, &candidate, &e), &e);
    workspace_fill(candidate, &e);
    ps_collection_segment_close(candidate);
    ok(ps_collection_conversion_checkpoint(w, 1, 128, &row, 1, &e), &e);
  }
  ok(ps_collection_conversion_candidate_open(w, 1000, &candidate, &e), &e);
  unsigned char bytes[128];
  struct iovec part = {bytes, sizeof(bytes)};
  ok(lc_pouch_record_io_read(ps_collection_segment_io(candidate), 0, &part, 1,
                             &e),
     &e);
  for (size_t i = 0; i < sizeof(bytes); ++i)
    REQUIRE(bytes[i] == 0x5a);
  ps_collection_segment_close(candidate);
  if (stage == 7) {
    ok(ps_collection_segment_open(s, source_key, 0, &info.target, NULL, NULL,
                                  &source, &e),
       &e);
    ok(ps_collection_conversion_source_prune(w, 1, source, &removed, &e), &e);
    ok(ps_collection_conversion_source_prune(w, 1, source, &removed, &e), &e);
    REQUIRE(!removed &&
            ps_collection_conversion_discard(w, NULL) == LC_ERR_INVALID);
    REQUIRE(lc_pouch_record_io_read(ps_collection_segment_io(source), 0, &part,
                                    1, NULL) == LC_ERR_PROTOCOL);
    ps_collection_segment_close(source);
    ps_collection_conversion_close(w);
    ok(ps_collection_conversion_reclaim(s, collection, &e), &e);
    workspace_gc(root, &e);
    lc_pouch_crypto_close(target);
    ok(ps_root_close(root, &e), &e);
    REQUIRE(!unlink(path));
    free(model.stable);
    free(model.dirty);
    lc_error_cleanup(&e);
    return calls;
  }
  ok(ps_collection_conversion_discard(w, &e), &e);
  ps_collection_conversion_close(w);
  workspace_gc(root, &e);
  ok(ps_collection_segment_open(s, source_key, 0, &info.target, NULL, NULL,
                                &source, &e),
     &e);
  ok(lc_pouch_record_io_read(ps_collection_segment_io(source), 0, &part, 1, &e),
     &e);
  for (size_t i = 0; i < sizeof(bytes); ++i)
    REQUIRE(bytes[i] == 0x5a);
  ps_collection_segment_close(source);
  lc_pouch_crypto_close(target);
  ok(ps_root_close(root, &e), &e);
  REQUIRE(!unlink(path));
  free(model.stable);
  free(model.dirty);
  lc_error_cleanup(&e);
  return calls;
}
static void workspace_faults(const char *directory, const char *key) {
  size_t total = 0;
  for (int sync = 0; sync < 2; ++sync)
    for (int stage = 0; stage < 7; ++stage)
      for (int mode = 0; mode < 4; ++mode) {
        size_t calls =
            workspace_fault_trial(directory, key, sync, stage, mode, SIZE_MAX);
        for (size_t cut = 1; cut <= calls; ++cut) {
          workspace_fault_trial(directory, key, sync, stage, mode, cut);
          ++total;
        }
        printf("workspace faults sync=%d stage=%d mode=%d cuts=%zu\n", sync,
               stage, mode, calls);
        fflush(stdout);
      }
  REQUIRE(total > 300);
  printf("workspace failure points=%zu\n", total);
}
#endif
