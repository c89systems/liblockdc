#ifndef POUCH_SPACE_COLLECTION_WALK_CASES_H
#define POUCH_SPACE_COLLECTION_WALK_CASES_H
#include "collection_walk.h"
#include "collection_workspace_cases.h"

typedef struct walk_guard {
  record_fixture *fixture;
  lc_u64 first[128], last[128];
  size_t count, reads;
} walk_guard;
static int walk_guard_read(void *context, uint64_t at,
                           const struct iovec *parts, int count, lc_error *e) {
  walk_guard *g = context;
  lc_u64 end = at;
  for (int i = 0; i < count; ++i)
    end += parts[i].iov_len;
  for (size_t i = 0; i < g->count; ++i)
    REQUIRE(end <= g->first[i] || at >= g->last[i]);
  ++g->reads;
  return lc_pouch_record_io_read(ps_collection_segment_io(g->fixture->source),
                                 at, parts, count, e);
}
static int walk_guard_open(void *context, lc_u64 id, lc_pouch_record_io *io,
                           void **pin, lc_error *e) {
  static const lc_pouch_record_io_ops ops = {walk_guard_read, NULL, NULL, NULL,
                                             NULL};
  walk_guard *g = context;
  (void)e;
  REQUIRE(id == 1);
  ps_collection_segment_retain(g->fixture->source);
  *pin = g->fixture->source;
  *io = (lc_pouch_record_io){&ops, g};
  return LC_OK;
}
static void walk_guard_close(void *context, void *pin) {
  (void)context;
  ps_collection_segment_close(pin);
}
static void walk_guard_init(walk_guard *g, record_fixture *f, lc_error *e) {
  memset(g, 0, sizeof(*g));
  g->fixture = f;
  lc_u64 tail;
  ok(lc_pouch_record_io_tail(ps_collection_segment_io(f->source), &tail, e), e);
  for (lc_u64 at = 0; at < tail;) {
    lc_pouch_collection_record r;
    ok(lc_pouch_collection_record_read(ps_collection_segment_io(f->source), at,
                                       tail, leaf, &r, e),
       e);
    if (r.kind == LC_POUCH_COLLECTION_CHUNK) {
      REQUIRE(g->count < 128);
      g->first[g->count] = at + LC_COLLECTION_UNIT_HEADER;
      g->last[g->count++] = r.next_offset;
    }
    at = r.next_offset;
  }
}

static void walk_bind_fixture(record_fixture *f, ps_collection_conversion **w,
                              lc_error *e) {
  ps_space *s = ps_root_space(f->root);
  lc_collection_disk_head head = {.active = 1, .tail_segment = 1};
  ps_key key = {73, 1, PS_STREAM};
  ps_catalog_info info = workspace_source(f->source, key, e), binding;
  unsigned char name[8] = {1};
  ok(ps_catalog_put(s, NULL, 73, PS_CATALOG_SEGMENT, name, 8, NULL, key,
                    &info.target, &binding, e),
     e);
  ok(ps_collection_head_store(s, 73, &head, NULL, e), e);
  ok(ps_collection_conversion_open(s, 73, f->target, 1, w, e), e);
}
static void walk_plan_check(record_fixture *f, ps_collection_conversion *work,
                            size_t units, lc_error *e) {
  ps_collection_walk *walk = NULL;
  ps_collection_walk_item item;
  lc_u64 segment = 0, offset = 0, ns, no;
  size_t chunks = 0, nodes = 0, streams = 0, pairs = 0, ends = 0, total = 0;
  int found = 0;
  unit_allocations = 0;
  unit_fail_allocation = SIZE_MAX;
  ok(ps_collection_walk_open(work, f->id, &walk, e), e);
  for (;;) {
    ok(ps_collection_walk_next(walk, segment, offset, &item, &ns, &no, &found,
                               e),
       e);
    if (!found)
      break;
    REQUIRE(item.segment == 1 && ++total < 200);
    if (!item.kind)
      ++ends;
    else if (item.kind == LC_POUCH_COLLECTION_CHUNK) {
      REQUIRE(!item.height &&
              item.next_offset ==
                  item.offset + LC_COLLECTION_UNIT_HEADER + item.bytes);
      ++chunks;
    } else if (item.kind == LC_POUCH_COLLECTION_NODE) {
      REQUIRE(item.height && item.bytes <= LC_COLLECTION_CHUNK_PAGE);
      ++nodes;
    } else if (item.kind == LC_POUCH_COLLECTION_STREAM)
      ++streams;
    else if (item.kind == LC_POUCH_COLLECTION_EDIT_BODY) {
      REQUIRE(item.offset == f->edit_body.record_offset &&
              item.bytes == f->edit_body.next_offset - item.offset &&
              item.next_offset == f->receipt.next_offset);
      ++pairs;
    }
    segment = ns;
    offset = no;
  }
  REQUIRE(chunks == units && streams == 1 && pairs == 1 && ends == 1 &&
          (units == 1 || nodes) && total == chunks + nodes + 8);
  ps_collection_walk_close(walk);
  size_t allocations = unit_allocations;
  unit_fail_allocation = 0;
  REQUIRE(allocations < 32);
}
static void walk_decode_first(record_fixture *f, lc_pouch_record_io io,
                              lc_u64 at, const lc_collection_unit *unit,
                              lc_u64 base, lc_error *e) {
  lc_pouch_record_span span;
  lc_pouch_crypto_reader *reader = NULL;
  lc_source *source = NULL;
  ok(lc_pouch_record_span_bind(&span, io, at + LC_COLLECTION_UNIT_HEADER,
                               unit->stored, NULL, 0, e),
     e);
  ok(lc_pouch_crypto_reader_open(f->target, &reader, e), e);
  ok(lc_pouch_crypto_reader_bind_source(reader, unit->context, unit->context,
                                        &span.source, unit->stored,
                                        unit->descriptor, &source, e),
     e);
  unsigned char bytes[2048];
  lc_u64 total = 0;
  size_t got;
  while ((got = source->read(source, bytes, sizeof(bytes), e))) {
    REQUIRE(got <= unit->plain - total);
    int same = 1;
    for (size_t i = 0; i < got; ++i)
      if (bytes[i] != content_at(base + total + i, f->stream.bytes))
        same = 0;
    REQUIRE(same);
    total += got;
  }
  ok(e->code, e);
  REQUIRE(total == unit->plain);
  lc_pouch_crypto_reader_close(reader);
}
static void walk_capacity(const char *, const char *);
static void walk_cases(const char *directory, const char *key) {
  for (int codec = 0; codec < 4; ++codec)
    for (int sync = 0; sync < 2; ++sync)
      for (size_t units = 1; units <= 66; units = units == 1   ? 3
                                                  : units == 3 ? 66
                                                               : 67) {
        lc_error e;
        lc_error_init(&e);
        record_fixture *f = record_fixture_open(directory, key, codec, sync,
                                                units, PS_MIB + 17, &e);
        lc_collection_chunks_reader_close(&f->reader);
        ps_collection_conversion *work = NULL;
        walk_bind_fixture(f, &work, &e);
        ps_collection_walk *walk = NULL;
        REQUIRE(ps_collection_walk_open(work, f->id, &walk, NULL) ==
                    LC_ERR_INVALID &&
                !walk);
        walk_guard guard;
        walk_guard_init(&guard, f, &e);
        lc_collection_read_storage guarded = {walk_guard_open, walk_guard_close,
                                              &guard, NULL};
        ok(ps_collection_walk_prepare(work, f->id, f->pouch->crypto, guarded,
                                      &e),
           &e);
        REQUIRE(guard.reads && guard.count == units);
        REQUIRE(ps_collection_walk_prepare(work, f->id, f->pouch->crypto,
                                           record_reads(f),
                                           NULL) == LC_ERR_INVALID);
        walk_plan_check(f, work, units, &e);
        if (!codec && !sync && units == 1) {
          /* A corrupt relocation count must not enter the planner's disjoint
           * high key range, even when other progress fields are consistent. */
          ps_catalog_view *view = NULL;
          ps_catalog_info info;
          ps_space *space = ps_root_space(f->root);
          ok(ps_catalog_get(space, 73, PS_CATALOG_CONVERSION, NULL, 0, &view,
                            &e),
             &e);
          REQUIRE(view);
          ok(ps_catalog_view_info(view, &info, &e), &e);
          ps_catalog_view_close(view);
          ps_key progress_key = {info.stream.owner, 1, PS_CONTROL};
          unsigned char wire[96] = {0};
          memcpy(wire, "LCPSCCPR", 8);
          lc_u64 fields[] = {info.stream.owner,
                             LC_COLLECTION_SNAPSHOT_BIT - 1,
                             LC_COLLECTION_SNAPSHOT_BIT - 1,
                             0,
                             1,
                             1,
                             0,
                             1,
                             0,
                             0,
                             0};
          for (size_t i = 0; i < 11; ++i)
            for (size_t byte = 0; byte < 8; ++byte)
              wire[8 + 8 * i + byte] = (unsigned char)(fields[i] >> (8 * byte));
          ps_writer *writer = NULL;
          ok(ps_begin(space, NULL, progress_key, &writer, &e), &e);
          ok(ps_write(writer, wire, sizeof(wire), &e), &e);
          ok(ps_publish(writer, NULL, &e), &e);
          ps_writer_close(writer);
          ps_collection_conversion_close(work);
          work = NULL;
          REQUIRE(ps_collection_conversion_open(space, 73, f->target, 0, &work,
                                                &e) == LC_ERR_PROTOCOL &&
                  !work);
          clear(&e);
          ok(ps_delete(space, progress_key, &e), &e);
          ok(ps_collection_conversion_open(space, 73, f->target, 0, &work, &e),
             &e);
        }
        ok(ps_collection_walk_open(work, f->id, &walk, &e), &e);
        ps_collection_walk_item item;
        lc_u64 next_segment, next_offset;
        int found;
        ok(ps_collection_walk_next(walk, 0, 0, &item, &next_segment,
                                   &next_offset, &found, &e),
           &e);
        REQUIRE(found && item.kind == LC_POUCH_COLLECTION_CHUNK &&
                item.offset == 0);
        ps_collection_segment *output = NULL;
        ok(ps_collection_conversion_candidate_create(work, 1000, &output, &e),
           &e);
        uint64_t removed = 999;
        REQUIRE(ps_collection_conversion_source_prune(
                    work, 1, f->source, &removed, NULL) == LC_ERR_INVALID &&
                removed == 999);
        lc_collection_unit_recode *codec_work = NULL;
        lc_collection_unit converted;
        lc_u64 converted_offset;
        ok(lc_collection_unit_recode_open(NULL, f->target, &codec_work, &e),
           &e);
        ok(lc_collection_unit_recode_run(
               codec_work, ps_collection_segment_io(f->source), item.offset,
               item.bytes, ps_collection_segment_io(output), &converted_offset,
               &converted, &e),
           &e);
        lc_collection_unit_recode_close(codec_work);
        ok(ps_collection_segment_seal(output, &e), &e);
        walk_decode_first(f, ps_collection_segment_io(output), converted_offset,
                          &converted, 0, &e);
        ps_collection_segment_close(output);
        ps_collection_relocation mapping = {1, item.offset, 1000,
                                            converted_offset};
        ok(ps_collection_conversion_checkpoint(work, next_segment, next_offset,
                                               &mapping, 1, &e),
           &e);
        ps_stats before, after;
        ok(ps_status(ps_root_space(f->root), &before, &e), &e);
        ok(ps_collection_conversion_source_prune(work, 1, f->source, &removed,
                                                 &e),
           &e);
        /* With compression disabled, this crosses several entire grants. The
         * crossing grant retains the next unit/publication without copying. */
        if (!(codec & 2) && units <= 3)
          REQUIRE(removed > 64 * 1024);
        uint64_t again = 999;
        ok(ps_collection_conversion_source_prune(work, 1, f->source, &again,
                                                 &e),
           &e);
        REQUIRE(!again &&
                ps_collection_conversion_discard(work, NULL) == LC_ERR_INVALID);
        ps_collection_walk_close(walk);
        ps_collection_conversion_close(work);
        record_reopen(f, &e);
        if (removed) {
          unsigned char old[32];
          struct iovec old_part = {old, sizeof(old)};
          REQUIRE(lc_pouch_record_io_read(ps_collection_segment_io(f->source),
                                          0, &old_part, 1,
                                          NULL) == LC_ERR_PROTOCOL);
        }
        ok(ps_collection_conversion_open(ps_root_space(f->root), 73, f->target,
                                         0, &work, &e),
           &e);
        walk_plan_check(f, work, units, &e);
        ok(ps_collection_walk_open(work, f->id, &walk, &e), &e);
        ps_collection_conversion_progress progress;
        ps_collection_conversion_status(work, &progress);
        ok(ps_collection_walk_next(walk, progress.source_segment,
                                   progress.source_offset, &item, &next_segment,
                                   &next_offset, &found, &e),
           &e);
        REQUIRE(found && item.offset == progress.source_offset);
        lc_pouch_collection_record retained;
        if (item.kind != LC_POUCH_COLLECTION_NODE)
          ok(lc_pouch_collection_record_read(
                 ps_collection_segment_io(f->source), item.offset,
                 f->stream.next_offset, leaf, &retained, &e),
             &e);
        workspace_gc(f->root, &e);
        ok(ps_status(ps_root_space(f->root), &after, &e), &e);
        REQUIRE(after.capacity == 10 * PS_MIB);
        if (removed)
          REQUIRE(after.allocated_bytes < before.allocated_bytes);
        ok(ps_collection_conversion_candidate_open(work, 1000, &output, &e),
           &e);
        walk_decode_first(f, ps_collection_segment_io(output), converted_offset,
                          &converted, 0, &e);
        ps_collection_segment_close(output);
        REQUIRE(ps_collection_conversion_discard(work, NULL) == LC_ERR_INVALID);
        ps_collection_walk_close(walk);
        ps_collection_conversion_close(work);
        printf("native walk codec=%d sync=%d units=%zu reclaimed=%llu\n", codec,
               sync, units, (unsigned long long)removed);
        record_fixture_close(f, &e);
        lc_error_cleanup(&e);
      }
  walk_capacity(directory, key);
}
static void walk_capacity(const char *directory, const char *key) {
  lc_error e;
  lc_error_init(&e);
  record_fixture *f =
      record_fixture_open(directory, key, 0, 1, 3, 6 * PS_MIB + 17, &e);
  lc_collection_chunks_reader_close(&f->reader);
  ps_collection_conversion *work = NULL;
  walk_bind_fixture(f, &work, &e);
  ok(ps_collection_walk_prepare(work, f->id, f->pouch->crypto, record_reads(f),
                                &e),
     &e);
  ps_collection_walk *walk = NULL;
  ok(ps_collection_walk_open(work, f->id, &walk, &e), &e);
  lc_u64 sid = 0, at = 0, next_sid, next_at;
  int found;
  size_t copied = 0;
  ps_collection_walk_item item;
  lc_collection_unit_recode *codec = NULL;
  ok(lc_collection_unit_recode_open(NULL, f->target, &codec, &e), &e);
  uint64_t reclaimed = 0;
  lc_collection_unit converted_units[3];
  lc_u64 converted_offsets[3], source_offsets[3];
  for (;;) {
    ok(ps_collection_walk_next(walk, sid, at, &item, &next_sid, &next_at,
                               &found, &e),
       &e);
    if (!found || item.kind != LC_POUCH_COLLECTION_CHUNK)
      break;
    ps_collection_segment *output = NULL;
    ok(ps_collection_conversion_candidate_create(work, 1000 + copied, &output,
                                                 &e),
       &e);
    lc_collection_unit converted;
    lc_u64 converted_at;
    int rc = lc_collection_unit_recode_run(
        codec, ps_collection_segment_io(f->source), item.offset, item.bytes,
        ps_collection_segment_io(output), &converted_at, &converted, &e);
    if (rc != LC_OK)
      fprintf(stderr, "six-MiB conversion unit=%zu: %s\n", copied,
              e.message ? e.message : "?");
    ok(rc, &e);
    REQUIRE(copied < 3);
    converted_units[copied] = converted;
    converted_offsets[copied] = converted_at;
    source_offsets[copied] = item.offset;
    ok(ps_collection_segment_seal(output, &e), &e);
    ps_collection_segment_close(output);
    ps_collection_relocation mapping = {item.segment, item.offset,
                                        1000 + copied, converted_at};
    ok(ps_collection_conversion_checkpoint(work, next_sid, next_at, &mapping, 1,
                                           &e),
       &e);
    uint64_t removed;
    ok(ps_collection_conversion_source_prune(work, 1, f->source, &removed, &e),
       &e);
    reclaimed += removed;
    ok(ps_checkpoint(ps_root_space(f->root), &e), &e);
    ++copied;
    sid = next_sid;
    at = next_at;
  }
  REQUIRE(copied == 3 && reclaimed > 4 * PS_MIB);
  ps_stats stats;
  ok(ps_status(ps_root_space(f->root), &stats, &e), &e);
  REQUIRE(stats.capacity == 10 * PS_MIB);
  printf("six-MiB native conversion copied=%zu reclaimed=%llu allocated=%llu\n",
         copied, (unsigned long long)reclaimed,
         (unsigned long long)stats.allocated_bytes);
  lc_collection_unit_recode_close(codec);
  ps_collection_walk_close(walk);
  ps_collection_conversion_close(work);
  record_reopen(f, &e);
  ok(ps_collection_conversion_open(ps_root_space(f->root), 73, f->target, 0,
                                   &work, &e),
     &e);
  lc_u64 plain = 0;
  for (size_t i = 0; i < copied; ++i) {
    ps_collection_relocation mapping;
    int present;
    ok(ps_collection_conversion_lookup(work, 1, source_offsets[i], &mapping,
                                       &present, &e),
       &e);
    REQUIRE(present && mapping.target_segment == 1000 + i &&
            mapping.target_offset == converted_offsets[i]);
    ps_collection_segment *output = NULL;
    ok(ps_collection_conversion_candidate_open(work, mapping.target_segment,
                                               &output, &e),
       &e);
    walk_decode_first(f, ps_collection_segment_io(output),
                      mapping.target_offset, &converted_units[i], plain, &e);
    plain += converted_units[i].plain;
    ps_collection_segment_close(output);
  }
  REQUIRE(plain == f->stream.bytes &&
          ps_collection_conversion_discard(work, NULL) == LC_ERR_INVALID);
  ps_collection_conversion_close(work);
  record_fixture_close(f, &e);
  lc_error_cleanup(&e);
}
static size_t walk_fault_trial(const char *directory, const char *key,
                               int codec, int sync, int mode, size_t cut) {
  lc_error e;
  lc_error_init(&e);
  record_fixture *f =
      record_fixture_open(directory, key, codec, sync, 3, 65537, &e);
  lc_collection_chunks_reader_close(&f->reader);
  ps_collection_conversion *work = NULL;
  walk_bind_fixture(f, &work, &e);
  unit_allocations = 0;
  unit_fail_allocation = mode == 0 ? cut : 0;
  f->watch.calls = 0;
  f->watch.fail = mode ? cut : 0;
  int rc = ps_collection_walk_prepare(work, f->id, f->pouch->crypto,
                                      record_reads(f), &e);
  size_t calls = mode ? f->watch.calls : unit_allocations;
  unit_fail_allocation = f->watch.fail = 0;
  REQUIRE(rc == LC_OK || rc == LC_ERR_NOMEM || rc == LC_ERR_TRANSPORT);
  clear(&e);
  ps_collection_conversion_close(work);
  record_reopen(f, &e);
  ok(ps_collection_conversion_open(ps_root_space(f->root), 73, f->target, 0,
                                   &work, &e),
     &e);
  ps_collection_walk *walk = NULL;
  int opened = ps_collection_walk_open(work, f->id, &walk, &e);
  if (opened == LC_OK)
    REQUIRE(rc == LC_OK);
  else {
    REQUIRE(opened == LC_ERR_INVALID && rc != LC_OK);
    clear(&e);
    ok(ps_collection_walk_prepare(work, f->id, f->pouch->crypto,
                                  record_reads(f), &e),
       &e);
  }
  ps_collection_walk_close(walk);
  walk_plan_check(f, work, 3, &e);
  ok(ps_collection_conversion_discard(work, &e), &e);
  ps_collection_conversion_close(work);
  record_stream_check(f, &f->stream, 1, &e);
  record_fixture_close(f, &e);
  lc_error_cleanup(&e);
  return calls;
}
static void walk_faults(const char *directory, const char *key) {
  size_t total = 0;
  {
    lc_error e;
    lc_error_init(&e);
    record_fixture *f =
        record_fixture_open(directory, key, 3, 1, 66, 65537, &e);
    lc_collection_chunks_reader_close(&f->reader);
    ps_collection_conversion *work = NULL;
    walk_bind_fixture(f, &work, &e);
    ok(ps_collection_walk_prepare(work, f->id, f->pouch->crypto,
                                  record_reads(f), &e),
       &e);
    size_t calls = 1;
    for (size_t cut = 0; cut <= calls; ++cut) {
      ps_collection_walk *walk = NULL;
      ok(ps_collection_walk_open(work, f->id, &walk, &e), &e);
      ps_collection_walk_item item, sentinel;
      memset(&item, 0xa5, sizeof(item));
      sentinel = item;
      lc_u64 sid = 777, offset = 888;
      int found = -1;
      unit_allocations = 0;
      unit_fail_allocation = cut ? cut : SIZE_MAX;
      int rc =
          ps_collection_walk_next(walk, 0, 0, &item, &sid, &offset, &found, &e);
      size_t visited = unit_allocations;
      unit_fail_allocation = 0;
      if (!cut) {
        ok(rc, &e);
        calls = visited;
        REQUIRE(calls);
      } else {
        REQUIRE(rc == LC_ERR_NOMEM && found == -1 && sid == 777 &&
                offset == 888 && !memcmp(&item, &sentinel, sizeof(item)));
        clear(&e);
        ok(ps_collection_walk_next(walk, 0, 0, &item, &sid, &offset, &found,
                                   &e),
           &e);
        REQUIRE(found && item.kind == LC_POUCH_COLLECTION_CHUNK);
        ++total;
      }
      ps_collection_walk_close(walk);
    }
    ok(ps_collection_conversion_discard(work, &e), &e);
    ps_collection_conversion_close(work);
    record_fixture_close(f, &e);
    lc_error_cleanup(&e);
  }
  for (int codec = 0; codec <= 3; codec += 3)
    for (int sync = 0; sync < 2; ++sync)
      for (int mode = 0; mode < 2; ++mode) {
        size_t calls =
            walk_fault_trial(directory, key, codec, sync, mode, SIZE_MAX);
        for (size_t cut = 1; cut <= calls; ++cut) {
          walk_fault_trial(directory, key, codec, sync, mode, cut);
          ++total;
        }
        printf("walk faults codec=%d sync=%d mode=%d cuts=%zu\n", codec, sync,
               mode, calls);
        fflush(stdout);
      }
  for (int sync = 0; sync < 2; ++sync)
    for (int mode = 0; mode < 4; ++mode) {
      size_t calls =
          workspace_fault_trial(directory, key, sync, 7, mode, SIZE_MAX);
      for (size_t cut = 1; cut <= calls; ++cut) {
        workspace_fault_trial(directory, key, sync, 7, mode, cut);
        ++total;
      }
      printf("prune faults sync=%d mode=%d cuts=%zu\n", sync, mode, calls);
      fflush(stdout);
    }
  REQUIRE(total > 300);
  printf("native walk and source disposal fault points=%zu\n", total);
}
#endif
