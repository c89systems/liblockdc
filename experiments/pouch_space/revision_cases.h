#ifndef POUCH_SPACE_REVISION_CASES_H
#define POUCH_SPACE_REVISION_CASES_H
#include "revision.h"

static int revision_write(ps_space *s, int control, const void *first,
                          size_t first_bytes, const void *second,
                          size_t second_bytes, lc_error *e) {
  ps_budget_plan plan = {0};
  ps_budget *budget = NULL;
  ps_writer *writers[2] = {NULL, NULL};
  int rc = ps_budget_plan_value(&plan, first_bytes, e);
  if (rc == LC_OK)
    rc = ps_budget_plan_value(&plan, second_bytes, e);
  if (rc == LC_OK)
    rc = ps_budget_open(s, &plan, &budget, e);
  if (rc == LC_OK)
    rc = ps_begin(s, budget, state(1), &writers[0], e);
  if (rc == LC_OK)
    rc = ps_begin(s, budget, key(control ? PS_CONTROL : PS_STATE, 1, 2),
                  &writers[1], e);
  if (rc == LC_OK)
    rc = ps_write(writers[0], first, first_bytes, e);
  if (rc == LC_OK)
    rc = ps_write(writers[1], second, second_bytes, e);
  if (rc == LC_OK)
    rc = ps_revision_publish(writers, 2, 0, PS_COMMIT_DEFAULT, NULL, NULL, e);
  ps_writer_close(writers[0]);
  ps_writer_close(writers[1]);
  CHECK(ps_budget_close(budget, NULL) == LC_OK);
  return rc;
}
static void revision_expect(ps_space *s, int control, const void *first,
                            size_t first_bytes, const void *second,
                            size_t second_bytes, int require_new) {
  ps_reader *reader = NULL;
  OK(ps_read_open(s, state(1), NULL, &reader, &error));
  int changed = ps_read_size(reader) == first_bytes;
  CHECK(require_new < 0 ? !changed : !require_new || changed);
  read_value(reader, changed ? first : "old", changed ? first_bytes : 3);
  ps_reader_close(reader);
  expect(s, key(control ? PS_CONTROL : PS_STATE, 1, 2), NULL,
         changed ? second : "initial", changed ? second_bytes : 7);
  expect(s, state(99), NULL, "neighbor", 8);
}
static void revision_seed(ps_space *s, int control) {
  put(s, state(1), "old", 3, 1);
  put(s, key(control ? PS_CONTROL : PS_STATE, 1, 2), "initial", 7, 2);
  put(s, state(99), "neighbor", 8, 2);
  OK(ps_checkpoint(s, &error));
}
static void revision_lifecycle(int durable, int control) {
  ps_space *s;
  ps_writer *writers[2] = {NULL, NULL};
  ps_reader *held = NULL;
  ps_budget_plan plan = {0};
  ps_budget *budget = NULL;
  ps_ref prepared[2], published[2];
  uint64_t length;
  ps_stats before, after;
  unsigned char *bytes = malloc(PS_CHUNK + 37);
  CHECK(bytes);
  memset(bytes, 0x59, PS_CHUNK + 37);
  clean();
  s = create(PS_MIB, durable, NULL);
  revision_seed(s, control);
  OK(ps_read_open(s, state(1), NULL, &held, &error));
  OK(ps_budget_plan_value(&plan, PS_CHUNK + 37, &error));
  OK(ps_budget_plan_value(&plan, 0, &error));
  OK(ps_budget_open(s, &plan, &budget, &error));
  OK(ps_begin(s, budget, state(1), &writers[0], &error));
  OK(ps_begin(s, budget, key(control ? PS_CONTROL : PS_STATE, 1, 2),
              &writers[1], &error));
  OK(ps_write(writers[0], bytes, PS_CHUNK + 37, &error));
  OK(ps_revision_writer_info(writers[0], &prepared[0], &length, &error));
  CHECK(length == PS_CHUNK + 37);
  OK(ps_revision_writer_info(writers[1], &prepared[1], &length, &error));
  CHECK(!length && prepared[0].incarnation != prepared[1].incarnation);
  expect(s, state(1), NULL, "old", 3);
  expect(s, key(control ? PS_CONTROL : PS_STATE, 1, 2), NULL, "initial", 7);
  OK(ps_status(s, &before, &error));
  OK(ps_revision_publish(writers, 2, 0, PS_COMMIT_DEFAULT, published, NULL,
                         &error));
  OK(ps_status(s, &after, &error));
  CHECK(after.syncs - before.syncs == (uint64_t)(durable || control));
  CHECK(!memcmp(prepared, published, sizeof(prepared)));
  BAD(ps_revision_writer_info(writers[0], &prepared[0], &length, &error));
  BAD(ps_publish(writers[1], NULL, &error));
  ps_writer_close(writers[0]);
  ps_writer_close(writers[1]);
  OK(ps_budget_close(budget, &error));
  revision_expect(s, control, bytes, PS_CHUNK + 37, NULL, 0, 1);
  OK(ps_checkpoint(s, &error));
  read_value(held, "old", 3);
  BAD(ps_close(s, &error));
  ps_reader_close(held);
  OK(ps_close(s, &error));
  s = reopen(NULL);
  revision_expect(s, control, bytes, PS_CHUNK + 37, NULL, 0, 1);
  OK(ps_close(s, &error));
  free(bytes);
  clean();
}
static void revision_admission(void) {
  ps_space *s;
  ps_stats before, after;
  ps_writer *writers[2] = {NULL, NULL}, *peer = NULL;
  ps_budget *budget = NULL;
  ps_budget_plan plan;
  unsigned char *bytes = malloc(PS_CHUNK + 19);
  CHECK(bytes);
  memset(bytes, 0x3d, PS_CHUNK + 19);
  for (int kind = 0; kind < 3; ++kind) {
    clean();
    s = create(PS_MIB, 0, NULL);
    revision_seed(s, 0);
    plan = (ps_budget_plan){0};
    OK(ps_budget_plan_value(&plan, PS_CHUNK + 19, &error));
    OK(ps_budget_plan_value(&plan, PS_CHUNK + 19, &error));
    if (kind == 0)
      plan.entries = 1;
    if (kind == 1)
      plan.metadata_bytes /= 2;
    OK(ps_budget_open(s, &plan, &budget, &error));
    for (size_t i = 0; i < 2; ++i) {
      OK(ps_begin(s, budget, state(10 + i), &writers[i], &error));
      OK(ps_write(writers[i], bytes, PS_CHUNK + 19, &error));
    }
    if (kind == 2) {
      /* An unrelated producer exhausts ordinary capacity after this complete
       * revision has been funded. It owns no publication authority. */
      OK(ps_begin(s, NULL, state(90), &peer, &error));
      int rc;
      do {
        rc = ps_write(peer, bytes, PS_CHUNK, &error);
      } while (rc == LC_OK);
      CHECK(rc == LC_ERR_INVALID);
      lc_error_cleanup(&error);
    }
    OK(ps_status(s, &before, &error));
    int rc = ps_revision_publish(writers, 2, 0, PS_COMMIT_DEFAULT, NULL, NULL,
                                 &error);
    if (kind < 2) {
      CHECK(rc == LC_ERR_INVALID);
      lc_error_cleanup(&error);
      absent(s, state(10));
      absent(s, state(11));
    } else {
      OK(rc);
      expect(s, state(10), NULL, bytes, PS_CHUNK + 19);
      expect(s, state(11), NULL, bytes, PS_CHUNK + 19);
    }
    ps_writer_close(peer);
    peer = NULL;
    ps_writer_close(writers[0]);
    ps_writer_close(writers[1]);
    OK(ps_budget_close(budget, &error));
    OK(ps_status(s, &after, &error));
    CHECK(!after.private_bytes && !after.budgets &&
          !after.reserved_payload_bytes && !after.reserved_metadata_bytes &&
          !after.reserved_entries);
    CHECK(after.capacity == PS_MIB);
    expect(s, state(1), NULL, "old", 3);
    expect(s, state(2), NULL, "initial", 7);
    OK(ps_close(s, &error));
  }
  clean();
  s = create(PS_MIB, 0, NULL);
  OK(ps_begin(s, NULL, state(1), &writers[0], &error));
  OK(ps_begin(s, NULL, state(1), &writers[1], &error));
  BAD(ps_revision_publish(writers, 2, 0, PS_COMMIT_DEFAULT, NULL, NULL,
                          &error));
  /* Invalid invocation did not consume otherwise valid private owners. */
  OK(ps_write(writers[0], "retry", 5, &error));
  OK(ps_publish(writers[0], NULL, &error));
  ps_writer_close(writers[0]);
  ps_writer_close(writers[1]);
  expect(s, state(1), NULL, "retry", 5);
  OK(ps_close(s, &error));
  free(bytes);
  clean();
}
static void revision_faults(int durable, int control) {
  model m;
  ps_io io = model_init(&m);
  unsigned char *baseline = malloc((size_t)PS_MIB);
  unsigned char *bytes = malloc(PS_CHUNK + 37);
  ps_space *s;
  size_t sites;
  CHECK(baseline && bytes);
  memset(bytes, 0x7a, PS_CHUNK + 37);
  clean();
  s = create(PS_MIB, durable, &io);
  revision_seed(s, control);
  OK(ps_close(s, &error));
  memcpy(baseline, m.stable, (size_t)PS_MIB);
  for (int kind = 0; kind < 2; ++kind) {
    sites = 0;
    for (size_t cut = 0; cut <= sites; ++cut) {
      memcpy(m.stable, baseline, (size_t)PS_MIB);
      m.stable_bytes = PS_MIB;
      crash(&m);
      ps_options options = {0};
      options.io = &io;
      options.durable_sync = durable;
      s = NULL;
      OK(ps_open(path, 0, &options, &s, &error));
      m.operations = allocation_call = 0;
      m.tear = m.persist_torn = 1;
      m.fail_at = kind == 0 ? cut : 0;
      allocation_fail = kind == 1 ? (cut ? cut : (size_t)-1) : 0;
      int rc = revision_write(s, control, bytes, PS_CHUNK + 37, "revision two",
                              12, &error);
      allocation_fail = m.fail_at = 0;
      int completed = rc == LC_OK;
      if (!cut) {
        OK(rc);
        sites = kind == 0 ? m.operations : allocation_call;
        CHECK(sites);
      } else {
        CHECK(kind == 0 ? rc == LC_ERR_TRANSPORT : rc == LC_ERR_NOMEM);
        lc_error_cleanup(&error);
      }
      if (kind == 1)
        revision_expect(s, control, bytes, PS_CHUNK + 37, "revision two", 12,
                        completed ? 1 : -1);
      OK(ps_close(s, &error));
      crash(&m);
      s = reopen(&io);
      revision_expect(s, control, bytes, PS_CHUNK + 37, "revision two", 12,
                      completed && (durable || control));
      OK(revision_write(s, control, bytes, PS_CHUNK + 37, "revision two", 12,
                        &error));
      OK(ps_flush(s, &error));
      OK(ps_checkpoint(s, &error));
      OK(ps_close(s, &error));
    }
    printf("revision sync%d control%d %s: %zu failure sites\n", durable,
           control, kind ? "allocation" : "write/sync", sites);
  }
  free(bytes);
  free(baseline);
  free(m.stable);
  clean();
}
static void revision_candidate_images(void) {
  model m;
  ps_io io = model_init(&m);
  ps_space *s;
  ps_stats before, after;
  unsigned char *baseline = malloc((size_t)PS_MIB);
  unsigned char *complete = malloc((size_t)PS_MIB);
  unsigned char *candidate = malloc((size_t)PS_MIB);
  unsigned char *bytes = malloc(PS_CHUNK + 37);
  CHECK(baseline && complete && candidate && bytes);
  memset(bytes, 0x6b, PS_CHUNK + 37);
  clean();
  s = create(PS_MIB, 0, &io);
  revision_seed(s, 0);
  OK(ps_status(s, &before, &error));
  memcpy(baseline, m.stable, (size_t)PS_MIB);
  OK(revision_write(s, 0, bytes, PS_CHUNK + 37, "revision two", 12, &error));
  OK(ps_status(s, &after, &error));
  OK(ps_close(s, &error));
  uint64_t offset = PS_JOURNAL_OFFSET + before.journal_bytes;
  size_t frame_bytes = (size_t)(after.journal_bytes - before.journal_bytes);
  int fd = open(path, O_RDONLY);
  CHECK(fd >= 0 && pread(fd, complete, (size_t)PS_MIB, 0) == (ssize_t)PS_MIB &&
        !close(fd));
  unsigned char *frame = complete + offset;
  CHECK(!memcmp(frame, "PSR1", 4) && frame_bytes > 192);
  size_t second_at = 64 + (uint32_t)fixture_get64(frame + 64 + 4);
  CHECK(second_at + 64 <= frame_bytes);
  /* Every physical byte prefix of the descriptor frame, with all payload
   * already persisted. Complete inner entries cannot escape their frame. */
  for (size_t cut = 0; cut <= frame_bytes; ++cut) {
    memcpy(m.stable, complete, (size_t)PS_MIB);
    memcpy(m.stable + offset, baseline + offset, frame_bytes);
    memcpy(m.stable + offset, complete + offset, cut);
    /* Unwritten zero bytes can already equal the complete descriptor. Such a
     * frame is fully present and may recover even before the write returns. */
    int whole = !memcmp(m.stable + offset, complete + offset, frame_bytes);
    m.stable_bytes = PS_MIB;
    crash(&m);
    s = reopen(&io);
    revision_expect(s, 0, bytes, PS_CHUNK + 37, "revision two", 12,
                    whole ? 1 : -1);
    OK(ps_close(s, &error));
  }
  /* Valid outer checksums with invalid final members and a selectively lost
   * final payload. None may admit the first, otherwise valid member. */
  for (int damage = 0; damage < 8; ++damage) {
    memcpy(candidate, complete, (size_t)PS_MIB);
    unsigned char *group = candidate + offset;
    unsigned char *second = group + second_at;
    if (damage == 0)
      second[60] ^= 1; /* inner checksum */
    else if (damage == 1) {
      memcpy(second + 16, group + 64 + 16, 24); /* duplicate typed key */
      fixture_entry_crc(second);
    } else if (damage == 2) {
      fixture_put(second + 8, fixture_get64(second + 8) + 1, 8);
      fixture_entry_crc(second);
    } else if (damage == 3)
      fixture_put(group + 16, 0, 4);
    else if (damage == 4)
      fixture_put(group + 16, 1, 4); /* trailing second member */
    else if (damage == 5)
      group[24] = 1; /* reserved header field */
    else if (damage == 6) {
      uint64_t payload = fixture_get64(second + 64);
      CHECK(payload >= PS_DATA && payload < PS_MIB);
      candidate[payload] ^= 1;
    } else {
      fixture_put(second + 4, frame_bytes, 4); /* truncated last member */
    }
    fixture_entry_crc(group);
    memcpy(m.stable, candidate, (size_t)PS_MIB);
    m.stable_bytes = PS_MIB;
    crash(&m);
    s = reopen(&io);
    revision_expect(s, 0, bytes, PS_CHUNK + 37, "revision two", 12, -1);
    OK(revision_write(s, 0, bytes, PS_CHUNK + 37, "revision two", 12, &error));
    put(s, state(100), "later", 5, 1);
    OK(ps_flush(s, &error));
    OK(ps_close(s, &error));
    crash(&m);
    s = reopen(&io);
    revision_expect(s, 0, bytes, PS_CHUNK + 37, "revision two", 12, 1);
    expect(s, state(100), NULL, "later", 5);
    OK(ps_checkpoint(s, &error));
    OK(ps_close(s, &error));
  }
  printf("revision recovery: %zu frame prefixes and 8 invalid complete "
         "candidates\n",
         frame_bytes + 1);
  free(bytes);
  free(candidate);
  free(complete);
  free(baseline);
  free(m.stable);
  clean();
}
static void revision_recovery_failures(void) {
  model m;
  ps_io io = model_init(&m);
  unsigned char *baseline = malloc((size_t)PS_MIB);
  unsigned char *bytes = malloc(PS_CHUNK + 37);
  CHECK(baseline && bytes);
  memset(bytes, 0x27, PS_CHUNK + 37);
  clean();
  ps_space *s = create(PS_MIB, 0, &io);
  revision_seed(s, 0);
  OK(revision_write(s, 0, bytes, PS_CHUNK + 37, "revision two", 12, &error));
  OK(ps_flush(s, &error));
  OK(ps_close(s, &error));
  memcpy(baseline, m.stable, (size_t)PS_MIB);
  for (int kind = 0; kind < 2; ++kind) {
    size_t sites = 0;
    for (size_t cut = 0; cut <= sites; ++cut) {
      memcpy(m.stable, baseline, (size_t)PS_MIB);
      m.stable_bytes = PS_MIB;
      crash(&m);
      m.reads = allocation_call = 0;
      m.fail_read = kind == 0 ? cut : 0;
      allocation_fail = kind == 1 ? (cut ? cut : (size_t)-1) : 0;
      ps_options options = {0};
      options.io = &io;
      s = NULL;
      int rc = ps_open(path, 0, &options, &s, &error);
      allocation_fail = m.fail_read = 0;
      if (!cut) {
        OK(rc);
        sites = kind == 0 ? m.reads : allocation_call;
        CHECK(sites);
        OK(ps_close(s, &error));
      } else {
        CHECK(rc == (kind == 0 ? LC_ERR_TRANSPORT : LC_ERR_NOMEM) && !s);
        lc_error_cleanup(&error);
      }
      s = reopen(&io);
      revision_expect(s, 0, bytes, PS_CHUNK + 37, "revision two", 12, 1);
      OK(ps_checkpoint(s, &error));
      OK(ps_close(s, &error));
    }
    printf("revision recovery %s: %zu failure sites\n",
           kind ? "allocation" : "read", sites);
  }
  free(bytes);
  free(baseline);
  free(m.stable);
  clean();
}
typedef struct revision_actor {
  ps_writer *writers[2];
  int rc;
} revision_actor;
static void *revision_worker(void *context) {
  revision_actor *actor = context;
  lc_error e;
  lc_error_init(&e);
  actor->rc = ps_revision_publish(actor->writers, 2, 0, PS_COMMIT_DEFAULT, NULL,
                                  NULL, &e);
  lc_error_cleanup(&e);
  return NULL;
}
static void revision_concurrency(void) {
  unsigned char *bytes = malloc(PS_CHUNK);
  CHECK(bytes);
  memset(bytes, 0x4f, PS_CHUNK);
  for (int phase = 0; phase < 3; ++phase) {
    commit_gate gate = {0};
    ps_io io = {&gate, gate_read, gate_write, gate_sync, gate_reserve, NULL};
    ps_space *s;
    ps_writer *peer = NULL;
    revision_actor actor = {{NULL, NULL}, -1};
    pthread_t thread;
    int control = phase == 2;
    CHECK(!pthread_mutex_init(&gate.mutex, NULL) &&
          !pthread_cond_init(&gate.cond, NULL));
    clean();
    s = create(PS_MIB, !control, &io);
    revision_seed(s, control);
    OK(ps_begin(s, NULL, state(1), &actor.writers[0], &error));
    OK(ps_begin(s, NULL, key(control ? PS_CONTROL : PS_STATE, 1, 2),
                &actor.writers[1], &error));
    OK(ps_write(actor.writers[0], "revision one", 12, &error));
    OK(ps_write(actor.writers[1], "revision two", 12, &error));
    gate.hold_payload = phase == 0;
    gate.enabled = 1;
    CHECK(!pthread_create(&thread, NULL, revision_worker, &actor));
    await_gate(&gate, 0);
    expect(s, state(1), NULL, "old", 3);
    expect(s, key(control ? PS_CONTROL : PS_STATE, 1, 2), NULL, "initial", 7);
    /* This producer performs real payload I/O while revision payload I/O or
     * the physical sync is paused. It holds no revision publication lock. */
    OK(ps_begin(s, NULL, state(100), &peer, &error));
    OK(ps_write(peer, bytes, PS_CHUNK, &error));
    pthread_mutex_lock(&gate.mutex);
    gate.released = 1;
    pthread_cond_broadcast(&gate.cond);
    pthread_mutex_unlock(&gate.mutex);
    CHECK(!pthread_join(thread, NULL) && actor.rc == LC_OK);
    ps_writer_close(actor.writers[0]);
    ps_writer_close(actor.writers[1]);
    OK(ps_publish(peer, NULL, &error));
    ps_writer_close(peer);
    expect(s, state(1), NULL, "revision one", 12);
    expect(s, key(control ? PS_CONTROL : PS_STATE, 1, 2), NULL, "revision two",
           12);
    expect(s, state(100), NULL, bytes, PS_CHUNK);
    OK(ps_close(s, &error));
    CHECK(!pthread_mutex_destroy(&gate.mutex) &&
          !pthread_cond_destroy(&gate.cond));
  }
  free(bytes);
  clean();
}
static void revision_rollover(void) {
  ps_space *s;
  ps_stats before, after;
  unsigned char *bytes = malloc(PS_CHUNK + 37);
  CHECK(bytes);
  memset(bytes, 0x37, PS_CHUNK + 37);
  clean();
  s = create(PS_MIB, 0, NULL);
  revision_seed(s, 0);
  do {
    put(s, state(77), "f", 1, 1);
    OK(ps_status(s, &before, &error));
  } while (PS_JOURNAL - before.journal_bytes >= 264);
  OK(revision_write(s, 0, bytes, PS_CHUNK + 37, "revision two", 12, &error));
  OK(ps_status(s, &after, &error));
  CHECK(after.checkpoints == before.checkpoints + 1);
  revision_expect(s, 0, bytes, PS_CHUNK + 37, "revision two", 12, 1);
  OK(ps_close(s, &error));
  s = reopen(NULL);
  revision_expect(s, 0, bytes, PS_CHUNK + 37, "revision two", 12, 1);
  expect(s, state(77), NULL, "f", 1);
  OK(ps_checkpoint(s, &error));
  OK(ps_close(s, &error));
  s = reopen(NULL);
  revision_expect(s, 0, bytes, PS_CHUNK + 37, "revision two", 12, 1);
  OK(ps_close(s, &error));
  free(bytes);
  clean();
}
typedef struct revision_death_io {
  size_t calls, stop;
  int armed, after;
} revision_death_io;
static void revision_death_boundary(revision_death_io *io, int after) {
  if (!io->armed)
    return;
  if (!after)
    ++io->calls;
  if (io->stop && io->calls == io->stop && io->after == after)
    _exit(73);
}
static ssize_t revision_death_write(void *context, int fd, const void *p,
                                    size_t bytes, off_t offset) {
  revision_death_io *io = context;
  revision_death_boundary(io, 0);
  ssize_t rc = pwrite(fd, p, bytes, offset);
  revision_death_boundary(io, 1);
  return rc;
}
static int revision_death_sync(void *context, int fd) {
  revision_death_io *io = context;
  revision_death_boundary(io, 0);
  int rc = fdatasync(fd);
  revision_death_boundary(io, 1);
  return rc;
}
static int revision_death_reserve(void *context, int fd, off_t offset,
                                  off_t bytes) {
  revision_death_io *io = context;
  revision_death_boundary(io, 0);
  int rc = fallocate(fd, 0, offset, bytes);
  revision_death_boundary(io, 1);
  return rc;
}
static ps_space *revision_death_open(revision_death_io *context, int durable) {
  ps_io io = {context,
              NULL,
              revision_death_write,
              revision_death_sync,
              revision_death_reserve,
              NULL};
  /* NULL syscall members are not defaults: supply the ordinary read shim. */
  io.read = gate_read;
  ps_options options = {0};
  ps_space *s = NULL;
  options.io = &io;
  options.durable_sync = durable;
  OK(ps_open(path, 0, &options, &s, &error));
  return s;
}
static void revision_death_child(void) {
  const char *target = getenv("PS_REVISION_DEATH_PATH");
  const char *stop = getenv("PS_REVISION_DEATH_STOP");
  const char *after = getenv("PS_REVISION_DEATH_AFTER");
  const char *durable = getenv("PS_REVISION_DEATH_SYNC");
  CHECK(target && stop && after && durable && strlen(target) < sizeof(path));
  memcpy(path, target, strlen(target) + 1);
  revision_death_io context = {0};
  context.stop = (size_t)strtoul(stop, NULL, 10);
  context.after = atoi(after);
  ps_space *s = revision_death_open(&context, atoi(durable));
  context.armed = 1;
  unsigned char *bytes = malloc(PS_CHUNK + 37);
  CHECK(bytes);
  memset(bytes, 0x61, PS_CHUNK + 37);
  OK(revision_write(s, 0, bytes, PS_CHUNK + 37, "revision two", 12, &error));
  _exit(74);
}
static void revision_processes(const char *directory) {
  unsigned char *bytes = malloc(PS_CHUNK + 37);
  size_t deaths = 0;
  CHECK(bytes);
  memset(bytes, 0x61, PS_CHUNK + 37);
  for (int durable = 0; durable < 2; ++durable) {
    clean();
    ps_space *s = create(PS_MIB, durable, NULL);
    revision_seed(s, 0);
    OK(ps_close(s, &error));
    revision_death_io probe = {0};
    s = revision_death_open(&probe, durable);
    probe.armed = 1;
    OK(revision_write(s, 0, bytes, PS_CHUNK + 37, "revision two", 12, &error));
    probe.armed = 0;
    OK(ps_close(s, &error));
    CHECK(probe.calls);
    for (size_t stop = 1; stop <= probe.calls; ++stop)
      for (int after = 0; after < 2; ++after) {
        clean();
        s = create(PS_MIB, durable, NULL);
        revision_seed(s, 0);
        OK(ps_close(s, &error));
        char stop_text[32];
        CHECK(snprintf(stop_text, sizeof(stop_text), "%zu", stop) > 0);
        CHECK(!setenv("PS_REVISION_DEATH_PATH", path, 1) &&
              !setenv("PS_REVISION_DEATH_STOP", stop_text, 1) &&
              !setenv("PS_REVISION_DEATH_AFTER", after ? "1" : "0", 1) &&
              !setenv("PS_REVISION_DEATH_SYNC", durable ? "1" : "0", 1));
        pid_t child = fork();
        CHECK(child >= 0);
        if (!child) {
          execl("/proc/self/exe", "pouch_space_test", directory,
                "revision-crash-child", (char *)NULL);
          _exit(75);
        }
        int status;
        CHECK(waitpid(child, &status, 0) == child && WIFEXITED(status) &&
              WEXITSTATUS(status) == 73);
        ++deaths;
        s = reopen(NULL);
        revision_expect(s, 0, bytes, PS_CHUNK + 37, "revision two", 12, 0);
        OK(revision_write(s, 0, bytes, PS_CHUNK + 37, "revision two", 12,
                          &error));
        OK(ps_checkpoint(s, &error));
        OK(ps_close(s, &error));
      }
  }
  CHECK(!unsetenv("PS_REVISION_DEATH_PATH") &&
        !unsetenv("PS_REVISION_DEATH_STOP") &&
        !unsetenv("PS_REVISION_DEATH_AFTER") &&
        !unsetenv("PS_REVISION_DEATH_SYNC"));
  printf("revision: %zu exec process exits across publication I/O\n", deaths);
  free(bytes);
  clean();
}
typedef struct revision_visit_context {
  ps_space *space;
  ps_key key;
  ps_ref original;
  size_t seen;
} revision_visit_context;
static int revision_visit_replace(void *context, ps_key k, const ps_ref *ref,
                                  uint64_t bytes, lc_error *e) {
  revision_visit_context *c = context;
  (void)e;
  CHECK(k.owner == c->key.owner && k.key == c->key.key &&
        k.kind == c->key.kind && bytes == PS_CHUNK + 177);
  CHECK(!memcmp(ref, &c->original, sizeof(*ref)));
  ++c->seen;
  /* Re-entry is outside publication authority. An old captured incarnation
   * must never let cleanup remove this newly selected replacement. */
  put(c->space, k, "replacement", 11, 2);
  OK(ps_value_remove(c->space, k, ref, &error));
  expect(c->space, k, NULL, "replacement", 11);
  return LC_OK;
}
static void revision_values(void) {
  ps_space *s;
  ps_reader *r = NULL;
  ps_ref observed, held;
  uint64_t length;
  size_t got;
  int present;
  unsigned char scratch[64];
  size_t size = PS_CHUNK + 177;
  unsigned char *bytes = malloc(size);
  CHECK(bytes);
  for (size_t i = 0; i < size; ++i)
    bytes[i] = (unsigned char)(i * 17 + 3);
  clean();
  s = create(PS_MIB, 0, NULL);
  ps_key k = key(PS_QUERY_FIRST + 5, 81, 9);
  OK(ps_value_info(s, k, &observed, &length, &present, &error));
  CHECK(!present && !length && !observed.incarnation);
  put(s, k, bytes, size, 317);
  put(s, key(PS_QUERY_FIRST + 4, 81, 9), "other kind", 10, 3);
  put(s, key(k.kind, 82, 9), "other owner", 11, 3);
  OK(ps_value_info(s, k, &observed, &length, &present, &error));
  CHECK(present && length == size);
  OK(ps_read_open(s, k, &observed, &r, &error));
  ps_read_reference(r, &held);
  CHECK(!memcmp(&held, &observed, sizeof(held)));
  size_t offsets[] = {size - 4, 0, PS_CHUNK - 13, 1, PS_CHUNK + 4};
  for (size_t i = 0; i < sizeof(offsets) / sizeof(offsets[0]); ++i) {
    size_t count = size - offsets[i];
    if (count > sizeof(scratch))
      count = sizeof(scratch);
    OK(ps_read_at(r, offsets[i], scratch, count, &error));
    CHECK(!memcmp(scratch, bytes + offsets[i], count));
  }
  BAD(ps_read_at(r, size, scratch, 1, &error));
  OK(ps_read(r, scratch, 17, &got, &error));
  CHECK(got == 17 && !memcmp(scratch, bytes, got));
  OK(ps_read_at(r, PS_CHUNK + 11, scratch, sizeof(scratch), &error));
  OK(ps_read(r, scratch, 7, &got, &error));
  CHECK(got == 7 && !memcmp(scratch, bytes + 17, got));
  revision_visit_context context = {s, k, held, 0};
  OK(ps_values_visit(s, k.owner, k.kind, k.kind, revision_visit_replace,
                     &context, &error));
  CHECK(context.seen == 1);
  OK(ps_checkpoint(s, &error));
  OK(ps_read_at(r, 0, scratch, sizeof(scratch), &error));
  CHECK(!memcmp(scratch, bytes, sizeof(scratch)));
  ps_reader_close(r);
  OK(ps_value_remove(s, key(k.kind, 999, 1), NULL, &error));
  OK(ps_close(s, &error));
  s = reopen(NULL);
  expect(s, k, NULL, "replacement", 11);
  expect(s, key(PS_QUERY_FIRST + 4, 81, 9), NULL, "other kind", 10);
  expect(s, key(k.kind, 82, 9), NULL, "other owner", 11);
  OK(ps_close(s, &error));
  clean();
  free(bytes);
}
static void revision_derived_recovery(void) {
  model m;
  ps_io io = model_init(&m);
  ps_space *s;
  for (int damaged = 0; damaged < 2; ++damaged) {
    clean();
    s = create(PS_MIB, 0, &io);
    revision_seed(s, 0);
    ps_writer *writers[2] = {NULL, NULL};
    ps_key keys[2] = {key(PS_QUERY_FIRST + 5, 1, 7), key(PS_QUERY_FIRST, 1, 0)};
    ps_stats before;
    OK(ps_status(s, &before, &error));
    for (size_t i = 0; i < 2; ++i) {
      OK(ps_begin(s, NULL, keys[i], &writers[i], &error));
      OK(ps_write(writers[i], i ? "manifest" : "query", i ? 8 : 5, &error));
    }
    OK(ps_revision_publish(writers, 2, 0, PS_COMMIT_DEFAULT, NULL, NULL,
                           &error));
    ps_writer_close(writers[0]);
    ps_writer_close(writers[1]);
    /* This canonical publication is acknowledged after the query revision. */
    put(s, state(99), "after", 5, 2);
    OK(ps_flush(s, &error));
    OK(ps_close(s, &error));
    uint64_t member = PS_JOURNAL_OFFSET + before.journal_bytes + 64;
    if (damaged)
      member += (uint32_t)fixture_get64(m.stable + member + 4);
    uint64_t payload = fixture_get64(m.stable + member + 64);
    CHECK(payload >= PS_DATA && payload < PS_MIB);
    m.stable[payload] ^= 1;
    crash(&m);
    s = reopen(&io);
    expect(s, state(99), NULL, "after", 5);
    ps_ref ref;
    uint64_t value_bytes;
    int present, valid = 1;
    OK(ps_value_info(s, keys[damaged], &ref, &value_bytes, &present, &error));
    CHECK(present);
    size_t reads = m.reads;
    OK(ps_value_validate(s, keys[damaged], &ref, &valid, &error));
    CHECK(!valid && m.reads > reads);
    reads = m.reads;
    OK(ps_value_validate(s, keys[damaged], &ref, &valid, &error));
    CHECK(!valid && m.reads == reads);
    OK(ps_value_info(s, keys[!damaged], &ref, &value_bytes, &present, &error));
    CHECK(present);
    allocation_call = 0;
    allocation_fail = 1;
    int validation = ps_value_validate(s, keys[!damaged], &ref, &valid, &error);
    allocation_fail = 0;
    CHECK(validation == LC_ERR_NOMEM);
    lc_error_cleanup(&error);
    OK(ps_value_validate(s, keys[!damaged], &ref, &valid, &error));
    CHECK(valid);
    reads = m.reads;
    OK(ps_value_validate(s, keys[!damaged], &ref, &valid, &error));
    CHECK(valid && m.reads == reads);
    ps_reader *r = NULL;
    unsigned char scratch[16];
    OK(ps_read_open(s, keys[damaged], NULL, &r, &error));
    CHECK(ps_read_at(r, 0, scratch, 1, &error) == LC_ERR_PROTOCOL);
    lc_error_cleanup(&error);
    ps_reader_close(r);
    OK(ps_check(s, &error));
    OK(ps_value_remove(s, keys[damaged], NULL, &error));
    OK(ps_checkpoint(s, &error));
    OK(ps_close(s, &error));
    s = reopen(&io);
    expect(s, state(99), NULL, "after", 5);
    OK(ps_close(s, &error));
  }
  free(m.stable);
  clean();
}
#endif
