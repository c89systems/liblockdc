#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#include "lc_api_internal.h"
#include "revision.h"
#include "space_internal.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <linux/falloc.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>
struct ps_writer {
  ps_space *space;
  ps_budget *budget;
  ps_entry *entry;
  ps_entry *expected_owned;
  size_t used;
  int failed, published;
  unsigned char buffer[PS_CHUNK];
};
struct ps_reader {
  ps_space *space;
  ps_entry *entry;
  size_t extent, offset, loaded;
  size_t random_extent;
  uint64_t random_base;
  int random_loaded;
  int imported;
  unsigned char buffer[PS_CHUNK];
};
/* Every imported reader holds presence continuously from before acquiring its
 * extents until last close. A handoff retains its source until import succeeds.
 * Exclusive lifetime admission excludes an old local issuer; a foreign issuer
 * already holds presence. Thus one probe before recovery detects every pin
 * that can legitimately arrive from a previous owner, without a persisted hint.
 * A new local export enables collection probes before its ticket escapes. */
#define PIN_PRESENCE_OFFSET 2U
#define TICKET_MAGIC UINT64_C(0x3154454b435450)
#define TICKET_HEADER 112U
static int allocation_refresh(ps_space *, lc_error *);
static int allocation_ranges_refresh(ps_space *, lc_error *);
static int pins_reclaim_locked(ps_space *, lc_error *);
static int canonical_observe(ps_space *, lc_error *);
static int canonical_journal(ps_space *, const ps_readiness_certificate *,
                             int replay, int verify_end, lc_error *);
static int replay_frame(ps_space *, const unsigned char *, uint64_t,
                        lc_error *);
static int apply_journal_event(ps_space *, ps_entry *, int, uint64_t,
                               lc_error *);
static ps_entry *selected_value(ps_space *, ps_entry *, uint64_t);
static int canonical_refresh(ps_space *, int verify_disk, lc_error *);
static int allocation_begin(ps_space *, ps_coordination_guard **, lc_error *);
static int allocation_end(ps_space *, ps_coordination_guard *, int, lc_error *);
struct ps_values {
  ps_space *space;
  ps_entry **items;
  size_t count;
};
struct ps_owned_head {
  ps_space *space;
  ps_entry *entry;
};
struct ps_owned_run {
  ps_space *space;
  ps_entry *entry;
  size_t references;
};

static void put64(unsigned char *p, uint64_t n) {
  size_t i;
  for (i = 0; i < 8; ++i)
    p[i] = (unsigned char)(n >> (8U * i));
}
static uint64_t get64(const unsigned char *p) {
  uint64_t n = 0;
  size_t i;
  for (i = 0; i < 8; ++i)
    n |= (uint64_t)p[i] << (8U * i);
  return n;
}
static void put32(unsigned char *p, uint32_t n) {
  size_t i;
  for (i = 0; i < 4; ++i)
    p[i] = (unsigned char)(n >> (8U * i));
}
static uint32_t get32(const unsigned char *p) {
  return (uint32_t)p[0] | (uint32_t)p[1] << 8U | (uint32_t)p[2] << 16U |
         (uint32_t)p[3] << 24U;
}
static uint32_t checksum(const void *p, size_t n) {
  return (uint32_t)crc32(0, p, (uInt)n);
}
static int err(lc_error *e, int rc, const char *message) {
  return lc_error_set(e, rc, 0, message, NULL, NULL, "pouch-space");
}
static int io_error(ps_space *s, lc_error *e, const char *message) {
  __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
  return lc_error_set(e, LC_ERR_TRANSPORT, errno, message, strerror(errno),
                      NULL, "pouch-space");
}
static int healthy(ps_space *s, lc_error *e) {
  return __atomic_load_n(&s->poisoned, __ATOMIC_RELAXED)
             ? err(e, LC_ERR_TRANSPORT,
                   "container I/O outcome uncertain; close and reopen")
             : LC_OK;
}
static ssize_t native_read(void *c, int fd, void *p, size_t n, off_t o) {
  (void)c;
  return pread(fd, p, n, o);
}
static ssize_t native_write(void *c, int fd, const void *p, size_t n, off_t o) {
  (void)c;
  return pwrite(fd, p, n, o);
}
static int native_sync(void *c, int fd) {
  (void)c;
  return fdatasync(fd);
}
static int native_reserve(void *c, int fd, off_t o, off_t n) {
  (void)c;
  return fallocate(fd, 0, o, n);
}
static int native_zero(void *c, int fd, off_t o, off_t n) {
  (void)c;
  return fallocate(fd, FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE, o, n);
}
static int transfer(ps_space *s, void *bytes, size_t n, uint64_t offset,
                    int writing, lc_error *error) {
  unsigned char *p = bytes;
  ssize_t result;
  while (n) {
    result = writing ? s->io.write(s->io.context, s->fd, p, n, (off_t)offset)
                     : s->io.read(s->io.context, s->fd, p, n, (off_t)offset);
    if (result < 0 && errno == EINTR)
      continue;
    if (result <= 0 || (size_t)result > n) {
      if (result == 0)
        errno = EIO;
      return io_error(s, error,
                      writing ? "container offset write failed"
                              : "container offset read failed");
    }
    p += result;
    n -= (size_t)result;
    offset += (uint64_t)result;
  }
  return LC_OK;
}
static int initialize_range(ps_space *s, uint64_t offset, uint64_t bytes,
                            unsigned char *zero, lc_error *error) {
  size_t n;
  int rc;
  if (!bytes)
    return LC_OK;
  if (s->io.zero) {
    do {
      rc = s->io.zero(s->io.context, s->fd, (off_t)offset, (off_t)bytes);
    } while (rc < 0 && errno == EINTR);
    if (rc == 0)
      return LC_OK;
    if (errno != EOPNOTSUPP && errno != ENOSYS && errno != EINVAL)
      return io_error(s, error, "container zero initialization failed");
  }
  while (bytes) {
    n = bytes > PS_CHUNK ? PS_CHUNK : (size_t)bytes;
    rc = transfer(s, zero, n, offset, 1, error);
    if (rc != LC_OK)
      return rc;
    offset += n;
    bytes -= n;
  }
  return LC_OK;
}
static int sync_file(ps_space *s, lc_error *error) {
  int rc;
  pthread_mutex_lock(&s->sync_io_mutex);
  rc = healthy(s, error);
  if (rc != LC_OK) {
    pthread_mutex_unlock(&s->sync_io_mutex);
    return rc;
  }
  do {
    rc = s->io.sync(s->io.context, s->fd);
  } while (rc < 0 && errno == EINTR);
  __atomic_fetch_add(&s->syncs, 1, __ATOMIC_RELAXED);
  if (rc != 0)
    rc = io_error(s, error, "container sync failed");
  pthread_mutex_unlock(&s->sync_io_mutex);
  return rc;
}
static void mark_durable(ps_space *s, uint64_t sequence) {
  pthread_mutex_lock(&s->commit_mutex);
  if (sequence > __atomic_load_n(&s->durable_sequence, __ATOMIC_RELAXED))
    __atomic_store_n(&s->durable_sequence, sequence, __ATOMIC_RELEASE);
  pthread_cond_broadcast(&s->commit_cond);
  pthread_mutex_unlock(&s->commit_mutex);
}
/* Global sync leadership precedes allocation authority. No allocator/commit
 * mutex spans file I/O or a wait for that leader. Only this owner's journal
 * framing establishes a dependency from its local completed-byte H prefix to
 * a global wire prefix; observing a foreign frame cannot certify local bytes.
 */
static int shared_durable(ps_space *s, uint64_t requested, uint64_t *completed,
                          int *synced, lc_error *error) {
  ps_coordination_guard *leader = NULL, *guard = NULL;
  ps_readiness_certificate certificate;
  uint64_t target = 0, wire = 0;
  int rc, covered = 0;
  *synced = 0;
  rc = ps_coordination_lock(s->coordination, PS_COORDINATION_SYNC, 0, 1,
                            &leader, error);
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&s->mutex);
  rc = allocation_begin(s, &guard, error);
  if (rc == LC_OK)
    rc = ps_readiness_read(s, &certificate, error);
  if (rc == LC_OK) {
    s->peer_durable = certificate.durable;
    covered = requested <= s->own_ready_sequence &&
              certificate.durable >= s->own_ready_wire;
    target = covered ? s->own_ready_sequence
                     : __atomic_load_n(&s->pending_sequence, __ATOMIC_ACQUIRE);
    wire = certificate.ready;
  }
  rc = allocation_end(s, guard, rc, error);
  guard = NULL;
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK && !covered) {
    *synced = 1;
    rc = sync_file(s, error);
    if (rc == LC_OK) {
      pthread_mutex_lock(&s->mutex);
      rc = allocation_begin(s, &guard, error);
      if (rc == LC_OK)
        rc = ps_readiness_stabilize(s, wire, 0, error);
      if (rc == LC_OK && s->peer_durable < wire)
        s->peer_durable = wire;
      rc = allocation_end(s, guard, rc, error);
      pthread_mutex_unlock(&s->mutex);
    }
  }
  ps_coordination_unlock(leader);
  if (__atomic_load_n(&s->poisoned, __ATOMIC_RELAXED)) {
    lc_error_cleanup(error);
    rc = healthy(s, error);
  }
  if (rc == LC_OK)
    *completed = target;
  return rc;
}
/* The caller has already published complete immutable bytes. One caller leads
 * a sync of the ready prefix; arrivals during I/O form the next group. Neither
 * the directory mutex nor the commit mutex is held across the actual sync.
 * Structural and commit barriers share the separate sync I/O mutex. */
static int wait_durable(ps_space *s, uint64_t sequence, lc_error *error) {
  uint64_t target, covered, completed;
  int rc = LC_OK, synced;
  pthread_mutex_lock(&s->commit_mutex);
  ++s->sync_requests;
  while (__atomic_load_n(&s->durable_sequence, __ATOMIC_RELAXED) < sequence) {
    rc = healthy(s, error);
    if (rc != LC_OK)
      break;
    if (s->syncing) {
      pthread_cond_wait(&s->commit_cond, &s->commit_mutex);
      continue;
    }
    target = __atomic_load_n(&s->pending_sequence, __ATOMIC_ACQUIRE);
    covered = target - __atomic_load_n(&s->durable_sequence, __ATOMIC_RELAXED);
    s->syncing = 1;
    pthread_mutex_unlock(&s->commit_mutex);
    completed = target;
    synced = 1;
    rc = s->readiness ? shared_durable(s, sequence, &completed, &synced, error)
                      : sync_file(s, error);
    pthread_mutex_lock(&s->commit_mutex);
    s->sync_batches += synced;
    if (rc == LC_OK) {
      if (completed > __atomic_load_n(&s->durable_sequence, __ATOMIC_RELAXED))
        __atomic_store_n(&s->durable_sequence, completed, __ATOMIC_RELEASE);
      if (synced && covered > s->max_sync_batch)
        s->max_sync_batch = covered;
    }
    s->syncing = 0;
    pthread_cond_broadcast(&s->commit_cond);
    if (rc != LC_OK)
      break;
  }
  pthread_mutex_unlock(&s->commit_mutex);
  return rc;
}
static int reserve_file(ps_space *s, uint64_t begin, uint64_t bytes,
                        lc_error *error) {
  int rc;
  do {
    rc = s->io.reserve(s->io.context, s->fd, (off_t)begin, (off_t)bytes);
  } while (rc < 0 && errno == EINTR);
  return rc == 0 ? LC_OK : io_error(s, error, "container preallocation failed");
}
static uint64_t aligned(uint64_t n) {
  return (n + PS_PAGE - 1) / PS_PAGE * PS_PAGE;
}
static int valid_key(ps_key k) {
  return (k.kind >= PS_STATE && k.kind <= PS_CATALOG) || k.kind == PS_CONTROL ||
         k.kind == PS_ROUTING ||
         (k.kind >= PS_QUERY_FIRST && k.kind <= PS_QUERY_LAST) ||
         (k.kind >= PS_KEY_METADATA && k.kind <= PS_NAMESPACE_CHECKPOINT);
}
static int immutable_key(ps_key k) {
  return k.kind == PS_STATE || k.kind == PS_COLLECTION ||
         k.kind == PS_CONTROL || k.kind == PS_ROUTING ||
         (k.kind >= PS_QUERY_FIRST && k.kind <= PS_QUERY_LAST) ||
         (k.kind >= PS_KEY_METADATA && k.kind <= PS_NAMESPACE_CHECKPOINT);
}
static size_t bucket(ps_key k) {
  uint64_t h = k.key ^ (k.owner * UINT64_C(0x9e3779b97f4a7c15)) ^ k.kind;
  h ^= h >> 33;
  h *= UINT64_C(0xff51afd7ed558ccd);
  return (size_t)((h ^ (h >> 33)) % BUCKETS);
}
static int same_key(ps_key a, ps_key b) {
  return a.owner == b.owner && a.key == b.key && a.kind == b.kind;
}
static ps_entry *lookup(ps_space *s, ps_key k) {
  ps_entry *p;
  for (p = s->buckets[bucket(k)]; p; p = p->hash_next)
    if (same_key(k, p->key))
      return p;
  return NULL;
}
static size_t entry_bytes(const ps_entry *p) {
  if (p->key.kind == PS_CATALOG)
    return ps_catalog_record_bytes(p);
  return p->key.kind == PS_STREAM      ? 80
         : p->key.kind == PS_OWNED_RUN ? 80 + 24 * p->count
                                       : 64 + 24 * p->count;
}
static ps_entry *stream_lookup(ps_space *s, uint64_t token) {
  ps_entry *p;
  for (p = s->stream_buckets[token % BUCKETS]; p; p = p->stream_next)
    if (p->token == token)
      return p;
  return NULL;
}
static void stream_register(ps_space *s, ps_entry *p) {
  p->stream_next = s->stream_buckets[p->token % BUCKETS];
  s->stream_buckets[p->token % BUCKETS] = p;
}
static void stream_unregister(ps_space *s, ps_entry *p) {
  ps_entry **link = &s->stream_buckets[p->token % BUCKETS];
  while (*link != p)
    link = &(*link)->stream_next;
  *link = p->stream_next;
}
static void register_entry(ps_space *s, ps_entry *p) {
  p->all_next = s->entries;
  p->all_link = &s->entries;
  if (p->all_next)
    p->all_next->all_link = &p->all_next;
  s->entries = p;
}
static void unregister_entry(ps_entry *p) {
  *p->all_link = p->all_next;
  if (p->all_next)
    p->all_next->all_link = p->all_link;
}
static void dirty_entry(ps_space *s, ps_entry *p, ps_entry *old) {
  if (old) {
    p->checkpoint_ref = old->checkpoint_ref;
    if (old->dirty_link) {
      *old->dirty_link = old->dirty_next;
      if (old->dirty_next)
        old->dirty_next->dirty_link = old->dirty_link;
      old->dirty_link = NULL;
    }
  }
  p->dirty_link = &s->dirty;
  p->dirty_next = s->dirty;
  if (p->dirty_next)
    p->dirty_next->dirty_link = &p->dirty_next;
  s->dirty = p;
}
static uint64_t entry_storage(const ps_entry *);
static void owned_unlink(ps_space *s, ps_entry *p) {
  ps_entry *head;
  if (!p->owned_linked)
    return;
  head = stream_lookup(s, p->key.owner);
  head->stream_storage -= entry_storage(p);
  if (p->owned_prev)
    p->owned_prev->owned_next = p->owned_next;
  else
    head->owned_first = p->owned_next;
  if (p->owned_next)
    p->owned_next->owned_prev = p->owned_prev;
  else
    head->owned_last = p->owned_prev;
  p->owned_linked = 0;
}
static void owned_link(ps_space *s, ps_entry *p, ps_entry *old) {
  ps_entry *head = stream_lookup(s, p->key.owner);
  if (!head)
    return; /* recovery validation reports an orphan */
  if (old && old->owned_linked) {
    head->stream_storage -= entry_storage(old);
    p->owned_prev = old->owned_prev;
    p->owned_next = old->owned_next;
    old->owned_linked = 0;
  } else {
    p->owned_prev = head->owned_last;
    p->owned_next = NULL;
  }
  if (p->owned_prev)
    p->owned_prev->owned_next = p;
  else
    head->owned_first = p;
  if (p->owned_next)
    p->owned_next->owned_prev = p;
  else
    head->owned_last = p;
  p->owned_linked = 1;
  head->stream_storage += entry_storage(p);
}
static void retire(ps_space *s, ps_entry *p) {
  ps_entry **link = &s->buckets[bucket(p->key)];
  while (*link != p)
    link = &(*link)->hash_next;
  *link = p->hash_next;
  if (p->key.kind == PS_CATALOG)
    ps_catalog_unlink(s, p);
  if (p->key.kind == PS_OWNED_RUN)
    owned_unlink(s, p);
  if (p->key.kind == PS_STREAM && !p->deleted) {
    stream_unregister(s, p);
  }
  p->hash_next = NULL;
  p->state = 2;
  p->retired_link = &s->retired;
  p->retired_next = s->retired;
  if (p->retired_next)
    p->retired_next->retired_link = &p->retired_next;
  s->retired = p;
  if (!p->deleted) {
    s->metadata -= entry_bytes(p);
    --s->live_entries;
  }
}
static void install(ps_space *s, ps_entry *p) {
  ps_entry *old = lookup(s, p->key);
  p->previous = old;
  if (p->key.kind == PS_OWNED_RUN && !p->deleted)
    owned_link(s, p, old);
  if (old)
    retire(s, old);
  p->state = 1;
  p->hash_next = s->buckets[bucket(p->key)];
  s->buckets[bucket(p->key)] = p;
  if (p->key.kind == PS_STREAM && !p->deleted)
    stream_register(s, p);
  if (!p->deleted) {
    s->metadata += entry_bytes(p);
    ++s->live_entries;
  }
}
static void entry_free(ps_entry *p) {
  ps_catalog_free_record(p);
  lc_free_with_allocator(NULL, p->ranges);
  lc_free_with_allocator(NULL, p->range_bitmap);
  lc_free_with_allocator(NULL, p->range_rank);
  lc_free_with_allocator(NULL, p->extents);
  lc_free_with_allocator(NULL, p);
}
static int allocation_begin(ps_space *s, ps_coordination_guard **guard,
                            lc_error *error) {
  int rc;
  *guard = NULL;
  if (!ps_coordination_shared(s->coordination) || s->allocation_guard)
    return LC_OK;
  rc = ps_coordination_lock(s->coordination, PS_COORDINATION_ALLOCATION, 0, 1,
                            guard, error);
  if (rc == LC_OK) {
    s->allocation_guard = *guard;
    s->allocation_ranges_checked = 0;
  }
  return rc;
}
static int allocation_end(ps_space *s, ps_coordination_guard *guard, int rc,
                          lc_error *error) {
  if (guard) {
    s->allocation_guard = NULL;
    ps_coordination_unlock(guard);
    if (__atomic_load_n(&s->poisoned, __ATOMIC_RELAXED)) {
      lc_error_cleanup(error);
      return healthy(s, error);
    }
  }
  return rc;
}
static int capture_begin(ps_space *s, ps_coordination_guard **guard,
                         int verify_disk, lc_error *error) {
  int rc = healthy(s, error);
  *guard = NULL;
  if (rc == LC_OK)
    rc = allocation_begin(s, guard, error);
  if (rc == LC_OK && *guard)
    rc = canonical_refresh(s, verify_disk, error);
  return rc;
}
int ps_internal_capture_begin(ps_space *s, ps_coordination_guard **guard,
                              lc_error *error) {
  return capture_begin(s, guard, 0, error);
}
int ps_internal_capture_end(ps_space *s, ps_coordination_guard *guard, int rc,
                            lc_error *error) {
  return allocation_end(s, guard, rc, error);
}
/* Mutations and reservation plans select against the imported prefix before
 * looking at keys or spending workspace. Nested allocation stays observe-only.
 */
int ps_internal_admission_begin(ps_space *s, ps_coordination_guard **guard,
                                lc_error *error) {
  int rc = ps_internal_capture_begin(s, guard, error);
  return rc == LC_OK && *guard ? allocation_ranges_refresh(s, error) : rc;
}
static int physical_claim(ps_space *s, uint64_t offset, uint64_t bytes,
                          lc_error *error) {
  int first = s->physical_claim_bytes == 0, rc;
  if (first) {
    rc = ps_coordination_range(s->coordination, PIN_PRESENCE_OFFSET, 1, 1,
                               error);
    if (rc != LC_OK)
      return rc;
  }
  rc = ps_coordination_range(s->coordination, offset, bytes, 1, error);
  if (rc == LC_OK) {
    ps_readiness_zero_pages(s, offset, bytes, 0);
    s->physical_claim_bytes += bytes;
  } else if (first &&
             ps_coordination_range(s->coordination, PIN_PRESENCE_OFFSET, 1, 0,
                                   NULL) != LC_OK) {
    lc_error_cleanup(error);
    rc = healthy(s, error);
  }
  return rc;
}
static int physical_release(ps_space *s, uint64_t offset, uint64_t bytes,
                            lc_error *error) {
  int rc;
  /* Failed owners retain uncertain kernel ownership until FD close. Cleanup
   * may discard local handles, but can never readmit an allocator on this FD.
   */
  if (__atomic_load_n(&s->poisoned, __ATOMIC_RELAXED))
    return LC_OK;
  rc = ps_coordination_range(s->coordination, offset, bytes, 0, error);
  if (rc != LC_OK)
    return rc;
  s->physical_claim_bytes -= bytes;
  return s->physical_claim_bytes
             ? LC_OK
             : ps_coordination_range(s->coordination, PIN_PRESENCE_OFFSET, 1, 0,
                                     error);
}
static int metadata_claim_range(ps_space *s, uint64_t offset, uint64_t bytes,
                                lc_error *error) {
  int rc = physical_claim(s, offset, bytes, error);
  if (rc == LC_OK)
    s->metadata_claim_bytes += bytes;
  return rc;
}
static int metadata_release_range(ps_space *s, uint64_t offset, uint64_t bytes,
                                  lc_error *error) {
  uint64_t before = s->physical_claim_bytes;
  int rc = physical_release(s, offset, bytes, error);
  /* A failed final presence unlock can follow a known successful range
   * release. Attribute exactly the ownership physical_release relinquished;
   * uncertain releases stay assigned to metadata until owner close. */
  s->metadata_claim_bytes -= before - s->physical_claim_bytes;
  return rc;
}
/* Sorted free intervals. Their number follows fragmentation, not capacity.
 * Nodes are preallocated before irreversible metadata changes when needed. */
static void interval_insert(ps_run **head, uint64_t *available, ps_run *node) {
  ps_run **link = head, *next;
  *available += node->bytes;
  while (*link && (*link)->offset < node->offset)
    link = &(*link)->next;
  node->next = *link;
  *link = node;
  link = head;
  while (*link && (*link)->next) {
    next = (*link)->next;
    if ((*link)->offset + (*link)->bytes == next->offset &&
        (*link)->clean == next->clean && (*link)->claimed == next->claimed) {
      (*link)->bytes += next->bytes;
      (*link)->next = next->next;
      lc_free_with_allocator(NULL, next);
    } else
      link = &(*link)->next;
  }
}
void ps_internal_free_coalesce(ps_space *s) {
  ps_run *run = s->free, *next;
  while (run && run->next) {
    next = run->next;
    if (run->offset + run->bytes == next->offset && run->clean == next->clean &&
        run->claimed == next->claimed) {
      run->bytes += next->bytes;
      run->next = next->next;
      lc_free_with_allocator(NULL, next);
    } else
      run = next;
  }
}
/* Physical claims belong to this process's OFD, with per-handle spending
 * authority in budget.c. Keeping one fungible pool preserves allocation-free,
 * syscall-free credit transfers and lets producers use prepared common runs
 * while returning an equal amount of their protected pool. */
static int budget_drop_ranges(ps_space *s, uint64_t bytes, ps_run **workspace,
                              lc_error *error) {
  ps_run *run, *tail = NULL;
  uint64_t n;
  int rc;
  if (bytes > s->claimed_free_bytes)
    return err(error, LC_ERR_PROTOCOL, "concrete payload claim mismatch");
  if (bytes && bytes < s->claimed_free_bytes) {
    if (workspace) {
      tail = *workspace;
      *workspace = NULL;
    } else
      tail = lc_calloc_with_allocator(NULL, 1, sizeof(*tail));
    if (!tail)
      return err(error, LC_ERR_NOMEM, "cannot split concrete payload claim");
  }
  for (run = s->free; bytes && run; run = run->next) {
    if (run->claimed != 1)
      continue;
    n = bytes < run->bytes ? bytes : run->bytes;
    rc = physical_release(s, run->offset, n, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(NULL, tail);
      return rc;
    }
    if (n < run->bytes) {
      *tail = *run;
      tail->offset += n;
      tail->bytes -= n;
      run->bytes = n;
      run->next = tail;
      tail = NULL;
    }
    run->claimed = 0;
    s->claimed_free_bytes -= n;
    bytes -= n;
  }
  lc_free_with_allocator(NULL, tail);
  return bytes ? err(error, LC_ERR_PROTOCOL, "concrete payload ranges lost")
               : LC_OK;
}
static int budget_ranges_locked(ps_space *s, uint64_t target,
                                ps_run **workspace, lc_error *error) {
  ps_run *run, *tail;
  uint64_t bytes, n;
  int rc = LC_OK;
  if (target == s->claimed_free_bytes)
    return LC_OK;
  if (target <= s->claimed_free_bytes) {
    rc =
        budget_drop_ranges(s, s->claimed_free_bytes - target, workspace, error);
    ps_internal_free_coalesce(s);
    return rc;
  }
  bytes = target - s->claimed_free_bytes;
  if (target > s->free_bytes) {
    rc = pins_reclaim_locked(s, error);
    if (rc != LC_OK)
      return rc;
    if (target > s->free_bytes)
      return err(error, LC_ERR_INVALID, "concrete payload capacity exhausted");
  }
  for (run = s->free; bytes && run; run = run->next) {
    if (run->claimed)
      continue;
    n = bytes < run->bytes ? bytes : run->bytes;
    tail = NULL;
    if (n < run->bytes) {
      tail = lc_calloc_with_allocator(NULL, 1, sizeof(*tail));
      if (!tail) {
        rc = err(error, LC_ERR_NOMEM, "cannot assign concrete payload claim");
        break;
      }
      *tail = *run;
      tail->offset += n;
      tail->bytes -= n;
    }
    rc = physical_claim(s, run->offset, n, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(NULL, tail);
      break;
    }
    if (tail) {
      run->next = tail;
      run->bytes = n;
    }
    run->claimed = 2;
    bytes -= n;
  }
  if (rc == LC_OK && bytes)
    rc = err(error, LC_ERR_INVALID, "concrete payload capacity exhausted");
  if (rc != LC_OK) {
    for (run = s->free; run; run = run->next)
      if (run->claimed == 2) {
        (void)physical_release(s, run->offset, run->bytes, NULL);
        run->claimed = 0;
      }
    if (__atomic_load_n(&s->poisoned, __ATOMIC_RELAXED)) {
      lc_error_cleanup(error);
      rc = healthy(s, error);
    }
  } else {
    for (run = s->free; run; run = run->next)
      if (run->claimed == 2)
        run->claimed = 1;
    s->claimed_free_bytes = target;
  }
  ps_internal_free_coalesce(s);
  return rc;
}
int ps_internal_budget_ranges(ps_space *s, uint64_t target, ps_run **workspace,
                              lc_error *error) {
  ps_coordination_guard *guard = NULL;
  int rc;
  if (target == s->claimed_free_bytes)
    return LC_OK;
  if (target < s->claimed_free_bytes)
    return budget_ranges_locked(s, target, workspace, error);
  rc = allocation_begin(s, &guard, error);
  if (rc == LC_OK && guard && target > s->claimed_free_bytes)
    rc = allocation_refresh(s, error);
  if (rc == LC_OK)
    rc = budget_ranges_locked(s, target, workspace, error);
  return allocation_end(s, guard, rc, error);
}
static void free_insert(ps_space *s, ps_run *node) {
  if (node->offset < PS_DATA)
    interval_insert(&s->metadata_free, &s->metadata_free_bytes, node);
  else
    interval_insert(&s->free, &s->free_bytes, node);
}
static void metadata_insert(ps_space *s, ps_run *node) {
  interval_insert(&s->metadata_free, &s->metadata_free_bytes, node);
}
static void take_selected(ps_run **link, uint64_t *available, uint64_t bytes,
                          uint64_t *out, int *clean) {
  ps_run *node = *link;
  if (clean)
    *clean = node->clean;
  *available -= bytes;
  *out = node->offset;
  node->offset += bytes;
  node->bytes -= bytes;
  if (!node->bytes) {
    *link = node->next;
    lc_free_with_allocator(NULL, node);
  }
}
static int take_interval(ps_run **head, uint64_t *available, uint64_t *bytes,
                         uint64_t *out, int *clean, lc_error *error) {
  ps_run **link, **first = NULL;
  for (link = head; *link; link = &(*link)->next) {
    if ((*link)->claimed)
      continue;
    if (!first)
      first = link;
    if ((*link)->bytes >= *bytes)
      break;
  }
  /* Fragmentation must not strand usable pages behind a preferred run size. */
  if (!*link && first) {
    link = first;
    *bytes = (*link)->bytes;
  }
  if (!*link)
    return err(error, LC_ERR_INVALID,
               "container payload capacity exhausted; checkpoint or grow");
  take_selected(link, available, *bytes, out, clean);
  return LC_OK;
}
static int take_payload_selected(ps_space *s, ps_budget *budget,
                                 ps_run **choice, uint64_t reserved,
                                 uint64_t funded, uint64_t *out,
                                 lc_error *error) {
  uint64_t offset = (*choice)->offset;
  int rc;
  if ((*choice)->claimed == 1 && budget) {
    if (reserved != funded || reserved > s->claimed_free_bytes)
      return err(error, LC_ERR_PROTOCOL, "invalid concrete payload spend");
    s->claimed_free_bytes -= funded; /* ownership moves to the writer */
  } else {
    rc = physical_claim(s, offset, reserved, error);
    if (rc != LC_OK)
      return rc;
    if (budget) {
      rc = budget_drop_ranges(s, funded, NULL, error);
      if (rc != LC_OK) {
        (void)physical_release(s, offset, reserved, NULL);
        return rc;
      }
      /* Dropping an earlier claim may have split the predecessor link. */
      for (choice = &s->free; *choice && (*choice)->offset != offset;
           choice = &(*choice)->next) {
      }
      if (!*choice)
        return err(error, LC_ERR_PROTOCOL, "payload selection changed");
    }
  }
  take_selected(choice, &s->free_bytes, reserved, out, NULL);
  return LC_OK;
}
static int take_payload_locked(ps_space *s, ps_budget *budget, uint64_t *bytes,
                               uint64_t *out, lc_error *error) {
  ps_run **link, **first = NULL;
  int rc, reclaimed = 0;
again:
  for (link = &s->free; *link; link = &(*link)->next) {
    if ((*link)->claimed && !budget)
      continue;
    if (!first)
      first = link;
    if ((*link)->bytes >= *bytes)
      break;
  }
  if (!*link && first) {
    link = first;
    *bytes = (*link)->bytes;
  }
  if (!*link && !reclaimed && (s->pinned_free || s->pinned_pages)) {
    rc = pins_reclaim_locked(s, error);
    if (rc != LC_OK)
      return rc;
    reclaimed = 1;
    goto again;
  }
  if (!*link)
    return err(error, LC_ERR_INVALID, "payload capacity exhausted; grow");
  return take_payload_selected(s, budget, link, *bytes, *bytes, out, error);
}
static int take_payload(ps_space *s, ps_budget *budget, uint64_t *bytes,
                        uint64_t *out, lc_error *error) {
  ps_coordination_guard *guard = NULL;
  int rc = allocation_begin(s, &guard, error);
  if (rc == LC_OK && guard)
    rc = allocation_refresh(s, error);
  if (rc == LC_OK)
    rc = take_payload_locked(s, budget, bytes, out, error);
  return allocation_end(s, guard, rc, error);
}
/* Rebuild free intervals by subtracting reachable runs. Also rejects overlap.
 */
static int exclude_selected(ps_run **link, uint64_t *available, uint64_t offset,
                            uint64_t bytes, lc_error *error) {
  ps_run *node = *link, *tail;
  if (!node || node->offset > offset || bytes > node->bytes ||
      offset - node->offset > node->bytes - bytes)
    return err(error, LC_ERR_PROTOCOL, "overlapping container ownership");
  if (node->offset == offset) {
    *available -= bytes;
    node->offset += bytes;
    node->bytes -= bytes;
    if (!node->bytes) {
      *link = node->next;
      lc_free_with_allocator(NULL, node);
    }
    return LC_OK;
  }
  if (offset + bytes < node->offset + node->bytes) {
    tail = lc_calloc_with_allocator(NULL, 1, sizeof(*tail));
    if (!tail)
      return err(error, LC_ERR_NOMEM, "cannot allocate free interval");
    tail->offset = offset + bytes;
    tail->bytes = node->offset + node->bytes - tail->offset;
    tail->next = node->next;
    tail->clean = node->clean;
    tail->claimed = node->claimed;
    node->next = tail;
  }
  *available -= bytes;
  node->bytes = offset - node->offset;
  return LC_OK;
}
static int exclude_run(ps_space *s, uint64_t offset, uint64_t bytes,
                       lc_error *error) {
  ps_run **link = offset < PS_DATA ? &s->metadata_free : &s->free;
  uint64_t *available =
      offset < PS_DATA ? &s->metadata_free_bytes : &s->free_bytes;
  while (*link && (*link)->offset + (*link)->bytes <= offset)
    link = &(*link)->next;
  if (*link && (*link)->claimed)
    return err(error, LC_ERR_PROTOCOL, "range overlaps private workspace");
  return exclude_selected(link, available, offset, bytes, error);
}
static size_t cell_count(uint64_t bytes) {
  return (size_t)((bytes + PS_CELL - 1) / PS_CELL);
}
static uint64_t cell_mask(size_t at, size_t count) {
  return count == CELLS ? UINT64_MAX : ((UINT64_C(1) << count) - 1) << at;
}
static uint64_t extent_mask(const ps_extent *extent) {
  return cell_mask((size_t)(extent->offset % PS_PAGE / PS_CELL),
                   cell_count(extent->bytes));
}
static uint64_t extent_allocation(const ps_extent *extent) {
  return extent->flags & EXTENT_PACKED ? cell_count(extent->bytes) * PS_CELL
                                       : aligned(extent->bytes);
}
static size_t entry_range(const ps_entry *p, size_t at, size_t end,
                          uint64_t *offset, uint64_t *bytes) {
  *offset = p->extents[at].offset;
  *bytes = extent_allocation(&p->extents[at++]);
  while (at < end && *offset + *bytes == p->extents[at].offset)
    *bytes += extent_allocation(&p->extents[at++]);
  return at;
}
static int reader_release_range(ps_space *s, uint64_t offset, uint64_t bytes,
                                lc_error *error) {
  uint64_t before = s->physical_claim_bytes;
  int rc = physical_release(s, offset, bytes, error);
  s->reader_claim_bytes -= before - s->physical_claim_bytes;
  return rc;
}
int ps_internal_entry_pin(ps_space *s, ps_entry *p, lc_error *error) {
  size_t i, next, undo;
  uint64_t offset, bytes;
  int rc;
  /* Private producers already hold these exact witnesses. Publication hands
   * them to the captured incarnation without an OFD unlock/relock gap. OFD
   * locks are not reference counted, so overlapping claims are never added. */
  if (!p->pins && !p->payload_claimed &&
      ps_coordination_shared(s->coordination)) {
    for (i = 0; i < p->count; i = next) {
      next = entry_range(p, i, p->count, &offset, &bytes);
      rc = physical_claim(s, offset, bytes, error);
      if (rc != LC_OK) {
        for (undo = 0; undo < i;) {
          undo = entry_range(p, undo, i, &offset, &bytes);
          (void)reader_release_range(s, offset, bytes, NULL);
        }
        if (__atomic_load_n(&s->poisoned, __ATOMIC_RELAXED)) {
          lc_error_cleanup(error);
          return healthy(s, error);
        }
        return rc;
      }
      s->reader_claim_bytes += bytes;
    }
    p->reader_claimed = p->count != 0;
  }
  ++p->pins;
  return LC_OK;
}
void ps_internal_entry_unpin(ps_space *s, ps_entry *p) {
  size_t i;
  uint64_t offset, bytes;
  if (--p->pins || !p->reader_claimed)
    return;
  for (i = 0; i < p->count;) {
    i = entry_range(p, i, p->count, &offset, &bytes);
    if (reader_release_range(s, offset, bytes, NULL) != LC_OK)
      return; /* Uncertain authority stays on this poisoned OFD until close. */
  }
  p->reader_claimed = 0;
}
static int entry_release_claims(ps_space *s, ps_entry *p, lc_error *error) {
  uint64_t offset = 0, bytes = 0, n;
  size_t i;
  int rc;
  if (!p->payload_claimed)
    return LC_OK;
  if (p->pins && p->state == 1 && ps_coordination_shared(s->coordination)) {
    for (i = 0; i < p->count; ++i)
      s->reader_claim_bytes += extent_allocation(&p->extents[i]);
    p->reader_claimed = p->count != 0;
    p->payload_claimed = 0;
    return LC_OK;
  }
  for (i = 0; i < p->count; ++i) {
    n = extent_allocation(&p->extents[i]);
    if (bytes && offset + bytes != p->extents[i].offset) {
      rc = physical_release(s, offset, bytes, error);
      if (rc != LC_OK)
        return rc;
      bytes = 0;
    }
    if (!bytes)
      offset = p->extents[i].offset;
    bytes += n;
  }
  rc = bytes ? physical_release(s, offset, bytes, error) : LC_OK;
  if (rc == LC_OK)
    p->payload_claimed = 0;
  return rc;
}
static uint64_t entry_storage(const ps_entry *p) {
  uint64_t bytes = 0;
  size_t i;
  for (i = 0; i < p->count; ++i)
    bytes += extent_allocation(&p->extents[i]);
  return bytes;
}
static ps_payload_page *payload_find(ps_space *s, uint64_t offset) {
  ps_payload_page *p;
  offset = offset / PS_PAGE * PS_PAGE;
  for (p = s->payload_buckets[(offset / PS_PAGE) % BUCKETS]; p;
       p = p->hash_next)
    if (p->run.offset == offset)
      return p;
  return NULL;
}
static void payload_unavailable(ps_payload_page *p) {
  if (p->available_link) {
    *p->available_link = p->available_next;
    if (p->available_next)
      p->available_next->available_link = p->available_link;
    p->available_link = NULL;
  }
}
/* One bin per largest contiguous free cell span. Admission probes at most 64
 * bins and 64 cells, independent of the number of partially occupied pages. */
static void payload_available(ps_space *s, ps_payload_page *p) {
  size_t i, span = 0, largest = 0;
  payload_unavailable(p);
  if (p->external_pins)
    return;
  for (i = 0; i < CELLS; ++i) {
    span = p->occupied & (UINT64_C(1) << i) ? 0 : span + 1;
    if (span > largest)
      largest = span;
  }
  if (largest) {
    p->available_next = s->available[largest];
    p->available_link = &s->available[largest];
    if (p->available_next)
      p->available_next->available_link = &p->available_next;
    s->available[largest] = p;
  }
}
static void payload_register(ps_space *s, ps_payload_page *p) {
  size_t hash = (size_t)((p->run.offset / PS_PAGE) % BUCKETS);
  p->next = s->payload_pages;
  p->link = &s->payload_pages;
  if (p->next)
    p->next->link = &p->next;
  s->payload_pages = p;
  p->hash_next = s->payload_buckets[hash];
  s->payload_buckets[hash] = p;
  ++s->packed_pages;
}
static void payload_unregister(ps_space *s, ps_payload_page *p) {
  ps_payload_page **link;
  payload_unavailable(p);
  link = &s->payload_buckets[(p->run.offset / PS_PAGE) % BUCKETS];
  while (*link != p)
    link = &(*link)->hash_next;
  *link = p->hash_next;
  *p->link = p->next;
  if (p->next)
    p->next->link = p->link;
  --s->packed_pages;
}
static void quarantine_run(ps_space *s, ps_run *run) {
  run->next = s->pinned_free;
  s->pinned_free = run;
  s->pinned_free_bytes += run->bytes;
}
static void quarantine_page(ps_space *s, ps_payload_page *page) {
  if (!page->external_pins) {
    page->external_pins = 1;
    page->pinned_next = s->pinned_pages;
    s->pinned_pages = page;
    payload_unavailable(page);
  }
}
static void payload_reconsider(ps_space *s, ps_payload_page *p) {
  if (p->occupied) {
    payload_available(s, p);
    return;
  }
  payload_unregister(s, p);
  if (p->external_pins) {
    ps_payload_page **link = &s->pinned_pages;
    while (*link != p)
      link = &(*link)->pinned_next;
    if (s->pinned_page_probe == &p->pinned_next)
      s->pinned_page_probe = link;
    *link = p->pinned_next;
    /* No selected cells remain. Keep the unknown physical witness without
     * retaining a local cell map that would reject a peer's full-page reuse. */
    quarantine_run(s, &p->run);
  } else
    free_insert(s, &p->run);
}
static void payload_release(ps_space *s, ps_extent *extent) {
  ps_payload_page *p = payload_find(s, extent->offset);
  p->occupied &= ~extent_mask(extent);
  s->packed_bytes -= extent_allocation(extent);
  payload_reconsider(s, p);
}
static int payload_claim(ps_space *s, ps_extent *extent, lc_error *error) {
  ps_payload_page *p;
  uint64_t mask;
  int rc;
  if (!(extent->flags & EXTENT_PACKED))
    return exclude_run(s, extent->offset, aligned(extent->bytes), error);
  mask = extent_mask(extent);
  p = payload_find(s, extent->offset);
  if (!p) {
    p = lc_calloc_with_allocator(NULL, 1, sizeof(*p));
    if (!p)
      return err(error, LC_ERR_NOMEM, "cannot recover packed payload page");
    p->run.offset = extent->offset / PS_PAGE * PS_PAGE;
    p->run.bytes = PS_PAGE;
    rc = exclude_run(s, p->run.offset, PS_PAGE, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(NULL, p);
      return rc;
    }
    payload_register(s, p);
  }
  if (p->occupied & mask)
    return err(error, LC_ERR_PROTOCOL, "overlapping packed payload ownership");
  p->occupied |= mask;
  s->packed_bytes += extent_allocation(extent);
  payload_available(s, p);
  return LC_OK;
}
static int release_entry_runs(ps_space *s, ps_entry *p, int quarantine,
                              lc_error *error) {
  size_t i;
  ps_run *list = NULL, *node;
  ps_payload_page *page;
  int rc = LC_OK;
  for (i = 0; i < p->count; ++i) {
    if (p->extents[i].flags & EXTENT_PACKED) {
      page = payload_find(s, p->extents[i].offset);
      if (!page || (page->occupied & extent_mask(&p->extents[i])) !=
                       extent_mask(&p->extents[i])) {
        rc = err(error, LC_ERR_PROTOCOL, "packed payload retirement mismatch");
        break;
      }
      continue;
    }
    node = lc_calloc_with_allocator(NULL, 1, sizeof(*node));
    if (!node) {
      rc = err(error, LC_ERR_NOMEM, "cannot allocate retired free intervals");
      break;
    }
    node->offset = p->extents[i].offset;
    node->bytes = aligned(p->extents[i].bytes);
    node->next = list;
    list = node;
  }
  if (rc == LC_OK)
    rc = entry_release_claims(s, p, error);
  while (list) {
    node = list;
    list = node->next;
    if (rc != LC_OK)
      lc_free_with_allocator(NULL, node);
    else if (quarantine)
      quarantine_run(s, node);
    else
      free_insert(s, node);
  }
  if (rc == LC_OK)
    for (i = 0; i < p->count; ++i) {
      if (p->extents[i].flags & EXTENT_PACKED) {
        if (quarantine)
          quarantine_page(s, payload_find(s, p->extents[i].offset));
        payload_release(s, &p->extents[i]);
      }
    }
  return rc;
}
static int pin_conflict(ps_space *s, uint64_t offset, uint64_t bytes,
                        int *present, lc_error *error) {
  uint64_t first, last;
  int rc = ps_coordination_conflict(s->coordination, offset, bytes, &first,
                                    &last, error);
  *present = rc != LC_OK || first != last;
  return rc;
}
static int entry_foreign_pinned(ps_space *s, ps_entry *p) {
  size_t i;
  int present;
  for (i = 0; i < p->count; ++i)
    if (pin_conflict(s, p->extents[i].offset, p->extents[i].bytes, &present,
                     NULL) != LC_OK ||
        present)
      return 1;
  return 0;
}
/* Rebuild ownership that is intentionally absent from the current directory:
 * an imported reader may outlive the writer that retired its incarnation.
 * Whole free pages are quarantined; recovered packed pages retain their real
 * occupancy and temporarily leave the admission bins. No payload is copied.
 * GETLK may return ANY conflict. Split that interval, requery its residuals,
 * and visit each clear residual once. Both traversal and queries follow actual
 * free intervals plus pin fragments, without restarting from the pool head. */
static int foreign_pool(ps_space *s, ps_run **head, uint64_t *available,
                        lc_error *error) {
  ps_run *run, *held, **link = head;
  uint64_t first, last;
  int rc;
  while (*link) {
    run = *link;
    if (run->claimed) {
      link = &run->next;
      continue;
    }
    rc = ps_coordination_conflict(s->coordination, run->offset, run->bytes,
                                  &first, &last, error);
    if (rc != LC_OK)
      return rc;
    if (first == last) {
      link = &run->next;
      continue;
    }
    first = first / PS_PAGE * PS_PAGE;
    last = aligned(last);
    held = lc_calloc_with_allocator(NULL, 1, sizeof(*held));
    if (!held)
      return err(error, LC_ERR_NOMEM, "cannot retain foreign ownership pages");
    held->offset = first;
    held->bytes = last - first;
    /* Foreign preparation may have dirtied formerly clean pages. Reclaimed
     * nodes deliberately retain clean=0, rather than the old local hint. */
    rc = exclude_selected(link, available, held->offset, held->bytes, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(NULL, held);
      return rc;
    }
    quarantine_run(s, held);
  }
  return LC_OK;
}
static int pins_recover(ps_space *s, lc_error *error) {
  ps_payload_page *page;
  int rc, present;
  if (!s->range_pins)
    return LC_OK;
  rc = pin_conflict(s, PIN_PRESENCE_OFFSET, 1, &present, error);
  if (rc != LC_OK || !present)
    return rc;
  for (page = s->payload_pages; page; page = page->next) {
    rc = pin_conflict(s, page->run.offset, PS_PAGE, &present, error);
    if (rc != LC_OK)
      return rc;
    if (present)
      quarantine_page(s, page);
  }
  rc = foreign_pool(s, &s->free, &s->free_bytes, error);
  return rc == LC_OK ? foreign_pool(s, &s->metadata_free,
                                    &s->metadata_free_bytes, error)
                     : rc;
}
/* Quarantine is a conservative positive observation. A live participant can
 * release ranges, or exit without notifying us. Advance through both lists
 * on ordinary work, and prove all remaining candidates on capacity pressure
 * or explicit collection. The common free pool and selected packed pages
 * still undergo conflict checks. Resume at a link to bound each pass.
 * New quarantines enter at the head, ahead of a pass already in progress. */
#define PIN_PROBES 2
static void pins_collect_locked(ps_space *s, int present, int full) {
  ps_run **link, *run;
  ps_payload_page *page, **pages;
  size_t probes;
  int held;
  full = full || !present;
  link = !full && s->pinned_free_probe ? s->pinned_free_probe : &s->pinned_free;
  for (probes = 0; *link && (full || probes < PIN_PROBES); ++probes) {
    run = *link;
    if (present &&
        (pin_conflict(s, run->offset, run->bytes, &held, NULL) != LC_OK ||
         held)) {
      link = &run->next;
      continue;
    }
    if (run->offset < PS_DATA && s->metadata_claims_active &&
        metadata_claim_range(s, run->offset, run->bytes, NULL) != LC_OK) {
      link = &run->next;
      continue;
    }
    *link = run->next;
    s->pinned_free_bytes -= run->bytes;
    free_insert(s, run);
  }
  s->pinned_free_probe = *link ? link : NULL;
  pages =
      !full && s->pinned_page_probe ? s->pinned_page_probe : &s->pinned_pages;
  for (probes = 0; *pages && (full || probes < PIN_PROBES); ++probes) {
    page = *pages;
    if (!present ||
        (pin_conflict(s, page->run.offset, PS_PAGE, &held, NULL) == LC_OK &&
         !held)) {
      *pages = page->pinned_next;
      page->external_pins = 0;
      payload_reconsider(s, page);
    } else
      pages = &page->pinned_next;
  }
  s->pinned_page_probe = *pages ? pages : NULL;
}
static int pins_reclaim_locked(ps_space *s, lc_error *error) {
  int present, rc;
  if (!s->pinned_free && !s->pinned_pages)
    return LC_OK;
  rc = pin_conflict(s, PIN_PRESENCE_OFFSET, 1, &present, error);
  if (rc != LC_OK)
    return rc;
  pins_collect_locked(s, present, 1);
  return healthy(s, error);
}
static void pins_collect(ps_space *s, int present, int full) {
  ps_coordination_guard *guard = NULL;
  int rc = allocation_begin(s, &guard, NULL);
  if (rc == LC_OK && guard)
    rc = pin_conflict(s, PIN_PRESENCE_OFFSET, 1, &present, NULL);
  if (rc == LC_OK)
    pins_collect_locked(s, present, full);
  (void)allocation_end(s, guard, rc, NULL);
}
static int allocation_refresh(ps_space *s, lc_error *error) {
  int rc = canonical_observe(s, error);
  return rc == LC_OK ? allocation_ranges_refresh(s, error) : rc;
}
static int allocation_ranges_refresh(ps_space *s, lc_error *error) {
  ps_run *run;
  int present, rc = pin_conflict(s, PIN_PRESENCE_OFFSET, 1, &present, error);
  if (rc != LC_OK)
    return rc;
  pins_collect_locked(s, present, 0);
  /* A foreign writer can dirty common pages between observations. Drop local
   * hints for those pages. The cohort bitmap follows every physical claim. */
  for (run = s->free; run; run = run->next)
    if (!run->claimed)
      run->clean = 0;
  ps_internal_free_coalesce(s);
  rc = healthy(s, error);
  if (rc == LC_OK && present)
    rc = foreign_pool(s, &s->free, &s->free_bytes, error);
  s->allocation_ranges_checked = rc == LC_OK;
  return rc;
}
static void collect_retired(ps_space *s, int full) {
  ps_entry **link = &s->retired, *p;
  int present = 0, foreign, shared;
  if (__atomic_load_n(&s->poisoned, __ATOMIC_RELAXED))
    return;
  if (s->range_pins) {
    if (pin_conflict(s, PIN_PRESENCE_OFFSET, 1, &present, NULL) != LC_OK)
      return;
    if (s->pinned_free || s->pinned_pages)
      pins_collect(s, present, full);
  }
  shared = ps_coordination_shared(s->coordination);
  while (*link) {
    p = *link;
    if (p->state != 2 || !p->safe || p->pins) {
      link = &p->retired_next;
      continue;
    }
    foreign = present && entry_foreign_pinned(s, p);
    /* After a shared checkpoint a foreign reader owns only physical
     * retention. A later publication can be the first evidence that its
     * witness drained. Keep the bytes quarantined, without obsolete logical
     * ownership that would reject that publication during same-root replay. */
    if ((foreign && !shared) ||
        __atomic_load_n(&s->poisoned, __ATOMIC_RELAXED) ||
        release_entry_runs(s, p, foreign, NULL) != LC_OK) {
      link = &p->retired_next;
      continue;
    }
    *link = p->retired_next;
    if (*link)
      (*link)->retired_link = link;
    unregister_entry(p);
    entry_free(p);
  }
}
static void collect(ps_space *s) { collect_retired(s, 0); }
static size_t event_bytes(const ps_entry *p, int event) {
  return event == PS_PUT || event == PS_SEAL ? entry_bytes(p) : 64;
}
static int live_hint(uint32_t event) {
  return event == PS_STREAM_TAIL || event == PS_STREAM_OWNER ||
         event == PS_NAMESPACE_OWNER || event == PS_STREAM_EDIT ||
         event == PS_STREAM_COMPLETE;
}
static void encode_entry(unsigned char *wire, const ps_entry *p, uint64_t seq,
                         int event) {
  size_t i, bytes = event_bytes(p, event), first;
  memset(wire, 0, bytes);
  put32(wire, (uint32_t)EVENT_MAGIC);
  put32(wire + 4, (uint32_t)bytes);
  put64(wire + 8, seq);
  put32(wire + 16, p->key.kind);
  put32(wire + 20, (uint32_t)event);
  put64(wire + 24, p->key.owner);
  put64(wire + 32, p->key.key);
  put64(wire + 40, p->token);
  if (live_hint((uint32_t)event))
    put64(wire + 48, p->bytes);
  if (event == PS_PUT || event == PS_SEAL) {
    put64(wire + 48, p->bytes);
    put32(wire + 56, (uint32_t)p->count);
    first = p->key.kind == PS_OWNED_RUN ? 80 : 64;
    if (p->key.kind == PS_OWNED_RUN) {
      put64(wire + 64, p->stream_capacity);
      put32(wire + 72, (uint32_t)p->owned_mapped);
    }
    for (i = 0; i < p->count; ++i) {
      put64(wire + first + 24 * i, p->extents[i].offset);
      put64(wire + first + 8 + 24 * i, p->extents[i].bytes);
      put32(wire + first + 16 + 24 * i, p->extents[i].crc);
      put32(wire + first + 20 + 24 * i, p->extents[i].flags);
    }
    if (p->key.kind == PS_STREAM) {
      put32(wire + 56, (uint32_t)p->stream_sealed);
      put64(wire + 64, p->stream_capacity);
      put64(wire + 72, p->stream_runs);
    }
    if (p->key.kind == PS_CATALOG)
      ps_catalog_encode_record(wire, p);
  }
  put32(wire + 60,
        (uint32_t)crc32(checksum(wire, 60), wire + 64, (uInt)(bytes - 64)));
}
static int decode_entry(ps_space *s, const unsigned char *wire, size_t bytes,
                        ps_entry **out, int *event, lc_error *error) {
  size_t count, i, first;
  uint64_t sum = 0, offset, n;
  ps_entry *p;
  *out = NULL;
  if (bytes < 64 || get32(wire) != EVENT_MAGIC || get32(wire + 4) != bytes ||
      get32(wire + 60) !=
          (uint32_t)crc32(checksum(wire, 60), wire + 64, (uInt)(bytes - 64)))
    return err(error, LC_ERR_PROTOCOL, "invalid container metadata checksum");
  count = get32(wire + 16) == PS_STREAM || get32(wire + 16) == PS_CATALOG
              ? 0
              : get32(wire + 56);
  first =
      get32(wire + 16) == PS_OWNED_RUN && get32(wire + 20) == PS_PUT ? 80 : 64;
  if (count > (PS_JOURNAL - 64) / 24 ||
      (get32(wire + 16) != PS_CATALOG &&
       bytes != (get32(wire + 16) == PS_STREAM && (get32(wire + 20) == PS_PUT ||
                                                   get32(wire + 20) == PS_SEAL)
                     ? 80
                     : first + count * 24)) ||
      get32(wire + 20) > PS_SEAL)
    return err(error, LC_ERR_PROTOCOL, "invalid container extent count");
  p = lc_calloc_with_allocator(NULL, 1, sizeof(*p));
  if (!p)
    return err(error, LC_ERR_NOMEM, "cannot allocate container entry");
  p->key.kind = get32(wire + 16);
  p->key.owner = get64(wire + 24);
  p->key.key = get64(wire + 32);
  p->token = get64(wire + 40);
  p->bytes = get64(wire + 48);
  *event = (int)get32(wire + 20);
  if (*event == PS_RESERVE_IDS) {
    if (p->key.kind || p->key.owner || p->key.key || !p->token ||
        p->token % TOKEN_RANGE || count || p->bytes)
      goto invalid;
    *out = p;
    return LC_OK;
  }
  if (!valid_key(p->key) || !p->token || p->token > s->token_limit ||
      (*event == PS_REMOVE &&
       (count || p->bytes || get32(wire + 56) ||
        (!immutable_key(p->key) && p->key.kind != PS_OWNED_RUN &&
         p->key.kind != PS_STREAM && p->key.kind != PS_CATALOG))) ||
      (*event == PS_SEAL && p->key.kind != PS_STREAM))
    goto invalid;
  if (p->key.kind == PS_CATALOG) {
    int rc;
    if (p->bytes || get32(wire + 56) || (*event == PS_REMOVE && bytes != 64))
      goto invalid;
    rc = *event == PS_PUT ? ps_catalog_decode_record(p, wire, bytes, error)
                          : LC_OK;
    if (rc != LC_OK) {
      entry_free(p);
      return rc;
    }
    *out = p;
    return LC_OK;
  }
  if (p->key.kind == PS_STREAM && (*event == PS_PUT || *event == PS_SEAL)) {
    p->stream_sealed = (int)get32(wire + 56);
    p->stream_capacity = get64(wire + 64);
    p->stream_runs = get64(wire + 72);
    if (get32(wire + 56) > 1 || (*event == PS_SEAL && !p->stream_sealed) ||
        p->bytes > p->stream_capacity || p->stream_capacity > INT64_MAX ||
        (!p->stream_capacity && p->stream_runs) ||
        p->stream_runs > p->stream_capacity / (PS_PAGE - PS_RUN_HEADER))
      goto invalid;
    *out = p;
    return LC_OK;
  }
  if (p->key.kind == PS_OWNED_RUN && *event == PS_PUT) {
    p->stream_capacity = get64(wire + 64);
    p->owned_mapped = (int)get32(wire + 72);
    if (!p->stream_capacity || p->stream_capacity > PS_MIB - PS_RUN_HEADER ||
        p->key.key > (uint64_t)INT64_MAX - p->stream_capacity ||
        get32(wire + 72) > 1 || get32(wire + 76) ||
        (!p->owned_mapped &&
         (p->bytes < PS_RUN_HEADER ||
          p->stream_capacity != p->bytes - PS_RUN_HEADER)) ||
        (p->owned_mapped &&
         (p->stream_capacity > PS_CHUNK || !count || p->bytes < 64 ||
          p->bytes > 64 + 9 * p->stream_capacity)))
      goto invalid;
  }
  if (count) {
    p->extents = lc_calloc_with_allocator(NULL, count, sizeof(*p->extents));
    if (!p->extents) {
      entry_free(p);
      return err(error, LC_ERR_NOMEM,
                 "cannot allocate container extent vector");
    }
  }
  p->count = p->capacity = count;
  for (i = 0; i < count; ++i) {
    offset = get64(wire + first + 24 * i);
    n = get64(wire + first + 8 + 24 * i);
    p->extents[i].flags = get32(wire + first + 20 + 24 * i);
    if (!n ||
        n > (p->key.kind == PS_OWNED_RUN && !p->owned_mapped ? PS_MIB
                                                             : PS_CHUNK) ||
        offset < PS_DATA || offset > s->capacity || n > s->capacity - offset ||
        (p->key.kind == PS_OWNED_RUN && !p->owned_mapped
             ? (count != 1 || p->extents[i].flags != EXTENT_OWNED ||
                offset % PS_PAGE || n % PS_PAGE)
         : p->extents[i].flags == EXTENT_PACKED
             ? (n >= PS_PAGE || offset % PS_CELL ||
                cell_count(n) * PS_CELL > PS_PAGE - offset % PS_PAGE)
             : (p->extents[i].flags || offset % PS_PAGE || n % PS_PAGE)) ||
        UINT64_MAX - sum < n)
      goto invalid;
    p->extents[i].offset = offset;
    p->extents[i].bytes = n;
    p->extents[i].crc = get32(wire + first + 16 + 24 * i);
    sum += n;
  }
  if (sum != p->bytes)
    goto invalid;
  *out = p;
  return LC_OK;
invalid:
  entry_free(p);
  return err(error, LC_ERR_PROTOCOL, "invalid container ownership descriptor");
}
static int owned_encoded_read(ps_space *s, ps_entry *p, uint64_t at,
                              void *bytes, size_t n, lc_error *e) {
  unsigned char *out = bytes;
  size_t i, take;
  uint64_t base = 0;
  int rc;
  if (at > p->bytes || n > p->bytes - at)
    return err(e, LC_ERR_PROTOCOL, "mapped piece metadata is truncated");
  for (i = 0; n && i < p->count; ++i) {
    if (at >= base + p->extents[i].bytes) {
      base += p->extents[i].bytes;
      continue;
    }
    take = (size_t)(base + p->extents[i].bytes - at);
    if (take > n)
      take = n;
    rc = transfer(s, out, take, p->extents[i].offset + at - base, 0, e);
    if (rc != LC_OK)
      return rc;
    out += take;
    n -= take;
    at += take;
    base += p->extents[i].bytes;
  }
  return n ? err(e, LC_ERR_PROTOCOL, "mapped piece extent is missing") : LC_OK;
}
static int mapped_range_add(ps_entry *p, ps_mapped_range *range, uint64_t start,
                            uint64_t bytes, uint64_t *data, uint64_t *end,
                            lc_error *e) {
  if (!bytes || start < *end || start > p->stream_capacity ||
      bytes > p->stream_capacity - start || bytes > p->bytes - *data)
    return err(e, LC_ERR_PROTOCOL, "invalid mapped piece range");
  range->offset = start;
  range->bytes = bytes;
  range->data = *data;
  *data += bytes;
  *end = start + bytes;
  return LC_OK;
}
static unsigned int bitmap_count(unsigned int value) {
  value = value - ((value >> 1) & 0x55U);
  value = (value & 0x33U) + ((value >> 2) & 0x33U);
  return (value + (value >> 4)) & 0x0fU;
}
static uint64_t bitmap_rank(const ps_entry *p, uint64_t offset) {
  size_t at = (size_t)(offset / 64 * 8), end = (size_t)(offset / 8);
  uint64_t count = p->range_rank[offset / 64];
  for (; at < end; ++at)
    count += bitmap_count(p->range_bitmap[at]);
  if (offset % 8)
    count += bitmap_count(p->range_bitmap[at] & ((1U << (offset % 8)) - 1));
  return count;
}
static int owned_map_load(ps_space *s, ps_entry *p, lc_error *e) {
  unsigned char header[64], *table = NULL;
  ps_mapped_range *ranges = NULL;
  ps_mapped_range checked;
  uint16_t *rank = NULL;
  uint64_t count, i, data, end = 0, start, bytes, table_bytes, pos;
  int rc, bitmap;
  if (p->ranges || p->range_bitmap)
    return LC_OK;
  rc = owned_encoded_read(s, p, 0, header, sizeof(header), e);
  if (rc != LC_OK)
    return rc;
  count = get32(header + 32);
  bitmap = (int)get32(header + 60);
  table_bytes = bitmap ? (p->stream_capacity + 7) / 8 : count * 8;
  if (memcmp(header, "LCPSMAPR", 8) || get64(header + 8) != p->key.owner ||
      get64(header + 16) != p->key.key ||
      get64(header + 24) != p->stream_capacity || !count ||
      count > p->stream_capacity || get64(header + 40) != p->bytes ||
      get64(header + 48) != p->token || get32(header + 60) > 1 ||
      get32(header + 56) !=
          (uint32_t)crc32(checksum(header, 56), header + 60, 4) ||
      64 + table_bytes > p->bytes)
    return err(e, LC_ERR_PROTOCOL, "invalid mapped piece header");
  table = lc_alloc_with_allocator(NULL, (size_t)table_bytes);
  if (bitmap)
    rank = lc_calloc_with_allocator(
        NULL, (size_t)((p->stream_capacity + 63) / 64 + 1), sizeof(*rank));
  else
    ranges = lc_calloc_with_allocator(NULL, (size_t)count, sizeof(*ranges));
  if (!table || (bitmap ? !rank : !ranges))
    rc = err(e, LC_ERR_NOMEM, "cannot allocate mapped piece ranges");
  if (rc == LC_OK)
    rc = owned_encoded_read(s, p, 64, table, (size_t)table_bytes, e);
  if (rc == LC_OK && checksum(table, (size_t)table_bytes) != get32(header + 36))
    rc = err(e, LC_ERR_PROTOCOL, "mapped piece range checksum mismatch");
  data = 64 + table_bytes;
  for (i = 0; !bitmap && rc == LC_OK && i < count; ++i) {
    start = get32(table + i * 8);
    bytes = get32(table + i * 8 + 4);
    rc = mapped_range_add(p, ranges + i, start, bytes, &data, &end, e);
  }
  if (bitmap && rc == LC_OK) {
    i = pos = 0;
    while (pos < p->stream_capacity && rc == LC_OK) {
      if (!(table[pos / 8] & (1U << (pos % 8)))) {
        ++pos;
        continue;
      }
      start = pos++;
      while (pos < p->stream_capacity && (table[pos / 8] & (1U << (pos % 8))))
        ++pos;
      if (i == count)
        rc = err(e, LC_ERR_PROTOCOL, "mapped bitmap range count overflow");
      else
        rc = mapped_range_add(p, &checked, start, pos - start, &data, &end, e);
      ++i;
    }
    if (rc == LC_OK &&
        (i != count || (p->stream_capacity % 8 &&
                        table[table_bytes - 1] >> (p->stream_capacity % 8))))
      rc = err(e, LC_ERR_PROTOCOL, "mapped bitmap count or padding is invalid");
  }
  if (rc == LC_OK && data != p->bytes)
    rc = err(e, LC_ERR_PROTOCOL, "mapped piece has trailing bytes");
  if (rc == LC_OK) {
    p->ranges = ranges;
    p->range_count = (size_t)count;
    p->mapped_end = end;
    if (bitmap) {
      pos = 0;
      for (i = 0; i < table_bytes; ++i) {
        if (!(i % 8))
          rank[i / 8] = (uint16_t)pos;
        pos += bitmap_count(table[i]);
      }
      if (!(table_bytes % 8))
        rank[table_bytes / 8] = (uint16_t)pos;
      p->range_bitmap = table;
      p->range_rank = rank;
      table = NULL;
      rank = NULL;
    }
    ranges = NULL;
  }
  lc_free_with_allocator(NULL, ranges);
  lc_free_with_allocator(NULL, rank);
  lc_free_with_allocator(NULL, table);
  return rc;
}
static int verify_owned(ps_space *s, ps_entry *p, unsigned char *header,
                        lc_error *error) {
  int rc;
  if (p->owned_mapped)
    return owned_map_load(s, p, error);
  rc = transfer(s, header, PS_RUN_IDENTITY_BYTES, p->extents[0].offset, 0,
                error);
  if (rc == LC_OK &&
      (checksum(header, PS_RUN_IDENTITY_BYTES) != p->extents[0].crc ||
       get64(header) != OWNED_MAGIC || get64(header + 8) != p->token ||
       get64(header + 16) != p->key.owner || get64(header + 24) != p->key.key ||
       get64(header + 32) != p->extents[0].offset ||
       get64(header + 40) != p->bytes || memcmp(header + 48, s->identity, 16)))
    rc = err(error, LC_ERR_PROTOCOL, "owned run identity header mismatch");
  return rc;
}
static int verify_value_payloads(ps_space *s, ps_entry *p,
                                 unsigned char *buffer, lc_error *error) {
  size_t i;
  int rc;
  if (p->key.kind == PS_OWNED_RUN && !p->owned_mapped)
    return verify_owned(s, p, buffer, error);
  for (i = 0; i < p->count; ++i) {
    rc = transfer(s, buffer, (size_t)p->extents[i].bytes, p->extents[i].offset,
                  0, error);
    if (rc != LC_OK)
      return rc;
    if (checksum(buffer, (size_t)p->extents[i].bytes) != p->extents[i].crc)
      return err(error, LC_ERR_PROTOCOL, "container payload checksum mismatch");
  }
  return p->key.kind == PS_OWNED_RUN ? owned_map_load(s, p, error) : LC_OK;
}
static int verify_entry(ps_space *s, ps_entry *p, unsigned char *buffer,
                        lc_error *error) {
  /* Derived bytes never determine how far canonical authority can recover.
   * Their descriptors still own exact, checked ranges; readers validate their
   * payload CRC and the query owner rebuilds invalid data. Lost/corrupt query
   * payload must not truncate later acknowledged canonical publications. */
  if (p->key.kind >= PS_QUERY_FIRST && p->key.kind <= PS_QUERY_LAST)
    return LC_OK;
  return verify_value_payloads(s, p, buffer, error);
}
static int initial_free(ps_space *s, lc_error *error) {
  s->free = lc_calloc_with_allocator(NULL, 1, sizeof(*s->free));
  s->metadata_free =
      lc_calloc_with_allocator(NULL, 1, sizeof(*s->metadata_free));
  if (!s->free || !s->metadata_free)
    return err(error, LC_ERR_NOMEM, "cannot allocate container free intervals");
  s->free->offset = PS_DATA;
  s->free->bytes = s->free_bytes = s->capacity - PS_DATA;
  s->free->clean = s->generation == 0;
  s->metadata_free->offset = PS_METADATA_START;
  s->metadata_free->bytes = s->metadata_free_bytes = PS_METADATA_WORKSPACE;
  s->metadata_free->clean = s->generation == 0;
  return LC_OK;
}
static int metadata_bound(uint64_t bytes, uint64_t entries, uint64_t *bound,
                          lc_error *error) {
  uint64_t pages;
  *bound = 0;
  if (bytes > (uint64_t)INT64_MAX / 2 ||
      entries > (uint64_t)INT64_MAX / PS_PAGE)
    return err(error, LC_ERR_INVALID, "container metadata capacity exhausted");
  pages = (bytes + META_PAYLOAD - 1) / META_PAYLOAD;
  pages += ps_directory_page_bound(entries);
  if (pages > UINT64_MAX / (2 * PS_PAGE))
    return err(error, LC_ERR_INVALID, "container metadata capacity exhausted");
  *bound = pages * PS_PAGE;
  return LC_OK;
}
static int metadata_claimed_bound(ps_space *s, uint64_t bytes, uint64_t entries,
                                  uint64_t *bound, lc_error *error) {
  if (bytes > UINT64_MAX - s->reserved_metadata ||
      entries > UINT64_MAX - s->reserved_entries)
    return err(error, LC_ERR_INVALID, "container metadata budget overflow");
  return metadata_bound(bytes + s->reserved_metadata,
                        entries + s->reserved_entries, bound, error);
}
/* Metadata is one private physical pool. Preparation consumes witnessed free
 * pages; root selection exchanges their witnesses for the pages it retires.
 * Initial recovery subtracts foreign claims before acquiring this pool. */
static int metadata_claim_initial_locked(ps_space *s, lc_error *error) {
  ps_run *run;
  int rc;
  if (!ps_coordination_shared(s->coordination))
    return LC_OK;
  for (run = s->metadata_free; run; run = run->next) {
    rc = metadata_claim_range(s, run->offset, run->bytes, error);
    if (rc != LC_OK)
      return rc;
  }
  s->metadata_claims_active = 1;
  return LC_OK;
}
static int metadata_claim_initial(ps_space *s, lc_error *error) {
  ps_coordination_guard *guard = NULL;
  int rc = allocation_begin(s, &guard, error);
  if (rc == LC_OK)
    rc = pins_recover(s, error);
  if (rc == LC_OK)
    rc = metadata_claim_initial_locked(s, error);
  return allocation_end(s, guard, rc, error);
}
static uint64_t metadata_target(ps_space *s, uint64_t bound) {
  uint64_t allocated = (uint64_t)s->metadata_pages * PS_PAGE;
  /* Enough free pages for one complete rewrite, and two complete images
   * counting the current checkpoint. A dense rewrite restores this invariant
   * from its retired image without relying on unprotected payload capacity. */
  return allocated < bound ? 2 * bound - allocated : bound;
}
static int metadata_fits(ps_space *s, uint64_t target, uint64_t payload,
                         uint64_t floor) {
  uint64_t extra =
      target > s->metadata_free_bytes ? target - s->metadata_free_bytes : 0;
  return s->reserved_payload <= s->free_bytes &&
         payload <= s->free_bytes - s->reserved_payload &&
         floor <= s->free_bytes - s->reserved_payload - payload &&
         extra <= s->free_bytes - s->reserved_payload - payload - floor;
}
static int metadata_fill_locked(ps_space *s, uint64_t bound, uint64_t target,
                                uint64_t payload, uint64_t floor,
                                lc_error *error) {
  uint64_t bytes;
  ps_run *node;
  int rc;
  /* Catalog and stream publication may borrow a capture guard. Its selected
   * journal does not describe a peer's private writer or budget ranges. */
  if (s->allocation_guard && !s->allocation_ranges_checked) {
    rc = allocation_ranges_refresh(s, error);
    if (rc != LC_OK)
      return rc;
  }
  if (!metadata_fits(s, target, payload, floor)) {
    rc = pins_reclaim_locked(s, error);
    if (rc != LC_OK)
      return rc;
    if (!metadata_fits(s, target, payload, floor))
      return err(error, LC_ERR_INVALID,
                 "container capacity reserved for checkpoint and maintenance; "
                 "checkpoint or grow");
  }
  while (s->metadata_free_bytes < target) {
    node = lc_calloc_with_allocator(NULL, 1, sizeof(*node));
    if (!node)
      return err(error, LC_ERR_NOMEM,
                 "cannot protect checkpoint free interval");
    bytes = target - s->metadata_free_bytes;
    rc = take_interval(&s->free, &s->free_bytes, &bytes, &node->offset,
                       &node->clean, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(NULL, node);
      return rc;
    }
    node->bytes = bytes;
    if (s->metadata_claims_active) {
      rc = metadata_claim_range(s, node->offset, node->bytes, error);
      if (rc != LC_OK) {
        free_insert(s, node);
        return rc;
      }
    }
    metadata_insert(s, node);
  }
  s->metadata_workspace = bound;
  return LC_OK;
}
static int metadata_fill(ps_space *s, uint64_t bound, uint64_t target,
                         uint64_t payload, uint64_t floor, lc_error *error) {
  ps_coordination_guard *guard = NULL;
  int rc = allocation_begin(s, &guard, error);
  if (rc == LC_OK && guard)
    rc = allocation_refresh(s, error);
  if (rc == LC_OK)
    rc = metadata_fill_locked(s, bound, target, payload, floor, error);
  return allocation_end(s, guard, rc, error);
}
static int metadata_protect(ps_space *s, uint64_t bound, uint64_t payload,
                            uint64_t floor, lc_error *error) {
  return metadata_fill(s, bound, metadata_target(s, bound), payload, floor,
                       error);
}
static int reserve_floor(ps_space *s, uint64_t bytes, uint64_t entries,
                         uint64_t bound, uint64_t *floor, lc_error *e) {
  ps_budget_plan plan;
  uint64_t future, target, extra;
  int rc;
  ps_internal_reserve_plan(s, &plan);
  if (bytes < s->metadata)
    bytes = s->metadata;
  if (entries < s->live_entries)
    entries = s->live_entries;
  if (plan.metadata_bytes > UINT64_MAX - bytes ||
      plan.entries > UINT64_MAX - entries)
    return err(e, LC_ERR_INVALID, "maintenance reserve metadata overflows");
  rc = metadata_claimed_bound(s, bytes + plan.metadata_bytes,
                              entries + plan.entries, &future, e);
  if (rc != LC_OK)
    return rc;
  if (future < bound)
    future = bound;
  target = metadata_target(s, bound);
  future = metadata_target(s, future);
  extra = future > target ? future - target : 0;
  if (plan.payload_bytes > UINT64_MAX - extra ||
      *floor > UINT64_MAX - extra - plan.payload_bytes)
    return err(e, LC_ERR_INVALID, "maintenance reserve payload overflows");
  *floor += extra + plan.payload_bytes;
  return LC_OK;
}
static int checkpoint_allowance_floor(ps_space *s, uint64_t bytes,
                                      uint64_t entries, uint64_t payload,
                                      uint64_t floor, ps_budget *budget,
                                      lc_error *error) {
  uint64_t bound, current;
  uint64_t growth = bytes > s->metadata ? bytes - s->metadata : 0;
  uint64_t added = entries > s->live_entries ? entries - s->live_entries : 0;
  int rc = ps_internal_budget_check(s, budget, payload, growth, added, error);
  if (rc != LC_OK)
    return rc;
  /* The owner's positive growth is already present in aggregate future
   * credits. Keep that protection until publication, including through peer
   * checkpoints while a writer performs private payload I/O. */
  if (budget) {
    bytes = s->metadata;
    entries = s->live_entries;
    payload = 0;
    /* The floor governed admission. Funded work must remain possible when
     * another maintenance claimant subsequently uses that unclaimed floor. */
    floor = 0;
  }
  rc = metadata_claimed_bound(s, bytes, entries, &bound, error);
  if (rc == LC_OK)
    rc = metadata_claimed_bound(s, s->metadata, s->live_entries, &current,
                                error);
  if (rc != LC_OK)
    return rc;
  if (bound < current)
    bound = current;
  if (floor) {
    rc = reserve_floor(s, bytes, entries, bound, &floor, error);
    if (rc != LC_OK)
      return rc;
  }
  return metadata_protect(s, bound, payload, floor, error);
}
static int immutable_allowance(ps_space *s, ps_entry *entry, uint64_t bytes,
                               uint64_t entries, uint64_t payload,
                               ps_budget *budget, lc_error *error) {
  uint64_t floor = PS_MAINTENANCE_RESERVE;
  if (entry->key.kind == PS_CONTROL || entry->key.kind == PS_ROUTING) {
    ps_entry *old = lookup(s, entry->key);
    if (old && !old->deleted)
      floor -= PS_PAGE;
  }
  return checkpoint_allowance_floor(s, bytes, entries, payload, floor, budget,
                                    error);
}
static int immutable_extent_allowance(ps_space *s, ps_entry *entry,
                                      uint64_t payload, ps_budget *budget,
                                      lc_error *error) {
  ps_entry *old = lookup(s, entry->key);
  size_t prior = old && !old->deleted ? entry_bytes(old) : 0;
  return immutable_allowance(
      s, entry, s->metadata - prior + entry_bytes(entry) + 24,
      s->live_entries + (old && !old->deleted ? 0 : 1), payload, budget, error);
}
static int payload_reserve_locked(ps_space *s, ps_entry *entry,
                                  ps_extent *extent, size_t *bytes, int split,
                                  ps_budget *budget, lc_error *error) {
  ps_payload_page *p = NULL;
  size_t requested = *bytes;
  size_t count = cell_count(*bytes), bin, at;
  uint64_t mask, reserve = PS_PAGE;
  int rc, fresh = 0, present, retry_packed = 1, check_quarantine;
again:
  p = NULL;
  *bytes = requested;
  count = cell_count(requested);
  for (bin = count; bin <= CELLS; ++bin)
    if (s->available[bin]) {
      p = s->available[bin];
      break;
    }
  /* Use existing slack before buying another page. Only the final subpage
   * tail may split, and only once: its remainder can always fit a fresh page.
   * Thus the physical page bound is unchanged and at most one descriptor is
   * added, even with adversarial cell fragmentation. */
  if (!p && split)
    for (bin = count - 1; bin; --bin)
      if (s->available[bin]) {
        p = s->available[bin];
        count = bin;
        *bytes = count * PS_CELL;
        break;
      }
  if (p && s->allocation_guard) {
    rc = pin_conflict(s, p->run.offset, PS_PAGE, &present, error);
    if (rc != LC_OK)
      return rc;
    if (present) {
      quarantine_page(s, p);
      goto again;
    }
  }
  check_quarantine = !p && retry_packed && s->pinned_pages;
  rc = immutable_extent_allowance(s, entry, p ? 0 : PS_PAGE, budget, error);
  if (rc == LC_ERR_INVALID && check_quarantine) {
    lc_error_cleanup(error);
    rc = pins_reclaim_locked(s, error);
    if (rc != LC_OK)
      return rc;
    retry_packed = 0;
    goto again;
  }
  if (rc != LC_OK)
    return rc;
  if (!p) {
    p = lc_calloc_with_allocator(NULL, 1, sizeof(*p));
    if (!p)
      return err(error, LC_ERR_NOMEM, "cannot allocate packed payload page");
    rc = take_payload(s, budget, &reserve, &p->run.offset, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(NULL, p);
      return rc;
    }
    p->run.bytes = PS_PAGE;
    ps_internal_budget_spend(budget, PS_PAGE, 0, 0);
    payload_register(s, p);
    fresh = 1;
  }
  for (at = 0; at + count <= CELLS; ++at) {
    mask = cell_mask(at, count);
    if (!(p->occupied & mask)) {
      if (!fresh) {
        rc = physical_claim(s, p->run.offset + at * PS_CELL, count * PS_CELL,
                            error);
        if (rc != LC_OK)
          return rc;
      }
      p->occupied |= mask;
      s->packed_bytes += count * PS_CELL;
      payload_available(s, p);
      extent->offset = p->run.offset + at * PS_CELL;
      extent->flags = EXTENT_PACKED;
      if (fresh) {
        if (at && (rc = physical_release(s, p->run.offset, at * PS_CELL,
                                         error)) != LC_OK)
          return rc;
        if (at + count < CELLS &&
            (rc = physical_release(s, p->run.offset + (at + count) * PS_CELL,
                                   (CELLS - at - count) * PS_CELL, error)) !=
                LC_OK)
          return rc;
      }
      return LC_OK;
    }
  }
  return err(error, LC_ERR_PROTOCOL, "packed payload free-span mismatch");
}
static int payload_reserve(ps_space *s, ps_entry *entry, ps_extent *extent,
                           size_t *bytes, int split, ps_budget *budget,
                           lc_error *error) {
  ps_coordination_guard *guard = NULL;
  int rc = allocation_begin(s, &guard, error);
  if (rc == LC_OK && guard)
    rc = allocation_refresh(s, error);
  if (rc == LC_OK)
    rc = payload_reserve_locked(s, entry, extent, bytes, split, budget, error);
  return allocation_end(s, guard, rc, error);
}
static int metadata_offset(ps_space *s, uint64_t offset) {
  return offset % PS_PAGE == 0 && offset >= PS_METADATA_START &&
         offset <= s->capacity - PS_PAGE &&
         (offset < JOURNAL_OFFSET || offset >= PS_DATA);
}
static ps_meta_page *metadata_find(ps_space *s, uint64_t offset) {
  ps_meta_page *p;
  for (p = s->page_buckets[(offset / PS_PAGE) % BUCKETS]; p; p = p->hash_next)
    if (p->run.offset == offset)
      return p;
  return NULL;
}
static void metadata_register(ps_space *s, ps_meta_page *p) {
  size_t hash = (size_t)((p->run.offset / PS_PAGE) % BUCKETS);
  p->next = s->pages;
  p->link = &s->pages;
  if (p->next)
    p->next->link = &p->next;
  s->pages = p;
  p->hash_next = s->page_buckets[hash];
  s->page_buckets[hash] = p;
  ++s->metadata_pages;
}
static void metadata_claim_unlink(ps_meta_page *p) {
  if (!p->claim_link)
    return;
  *p->claim_link = p->claim_next;
  if (p->claim_next)
    p->claim_next->claim_link = p->claim_link;
  p->claim_link = NULL;
  p->claim_next = NULL;
}
static int metadata_unclaim_page(ps_space *s, ps_meta_page *p,
                                 lc_error *error) {
  uint64_t before = s->metadata_claim_bytes;
  int rc = metadata_release_range(s, p->run.offset, PS_PAGE, error);
  if (s->metadata_claim_bytes < before)
    p->claimed = 0;
  return rc;
}
static int metadata_claim_retirement(ps_space *s, ps_meta_page *p,
                                     lc_error *error) {
  int rc;
  if (p->claimed)
    return LC_OK;
  rc = metadata_claim_range(s, p->run.offset, PS_PAGE, error);
  if (rc != LC_OK)
    return rc;
  p->claimed = 1;
  p->claim_link = &s->metadata_retirement_claims;
  p->claim_next = s->metadata_retirement_claims;
  if (p->claim_next)
    p->claim_next->claim_link = &p->claim_next;
  s->metadata_retirement_claims = p;
  return LC_OK;
}
static int metadata_retirement_done(ps_space *s, lc_error *error) {
  ps_meta_page *p;
  int rc = LC_OK, result;
  /* Retired pages unlink before their nodes enter the free pool. Remaining
   * claims cover old descriptor pages that still have selected references.
   * On an aborted checkpoint all old pages remain selected. */
  while ((p = s->metadata_retirement_claims) != NULL) {
    metadata_claim_unlink(p);
    result = metadata_unclaim_page(s, p, rc == LC_OK ? error : NULL);
    if (rc == LC_OK)
      rc = result;
  }
  return rc;
}
static void metadata_release(ps_space *s, ps_meta_page *p) {
  ps_meta_page **link = &s->page_buckets[(p->run.offset / PS_PAGE) % BUCKETS];
  if (s->metadata_claims_active && !p->claimed)
    __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
  metadata_claim_unlink(p);
  while (*link != p)
    link = &(*link)->hash_next;
  *link = p->hash_next;
  *p->link = p->next;
  if (p->next)
    p->next->link = p->link;
  --s->metadata_pages;
  if (s->metadata_free_bytes < metadata_target(s, s->metadata_workspace))
    metadata_insert(s, &p->run);
  else {
    if (s->metadata_claims_active && p->run.offset >= PS_DATA)
      (void)metadata_release_range(s, p->run.offset, p->run.bytes, NULL);
    free_insert(s, &p->run);
  }
}
static int metadata_allocate(ps_space *s, int index, ps_meta_page **out,
                             lc_error *error) {
  ps_meta_page *p = lc_calloc_with_allocator(NULL, 1, sizeof(*p));
  uint64_t bytes = PS_PAGE;
  int rc;
  if (!p)
    return err(error, LC_ERR_NOMEM, "cannot allocate metadata ownership");
  rc = take_interval(&s->metadata_free, &s->metadata_free_bytes, &bytes,
                     &p->run.offset, NULL, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(NULL, p);
    return rc;
  }
  p->run.bytes = PS_PAGE;
  p->generation = s->generation + 1;
  p->index = index;
  p->references = index != 0;
  p->claimed = s->metadata_claims_active;
  metadata_register(s, p);
  *out = p;
  return LC_OK;
}
static int directory_read(void *v, uint64_t offset, void *page,
                          lc_error *error) {
  ps_space *s = v;
  if (!metadata_offset(s, offset))
    return err(error, LC_ERR_PROTOCOL, "directory page outside metadata space");
  return transfer(s, page, PS_PAGE, offset, 0, error);
}
static int directory_allocate(void *v, uint64_t *offset, lc_error *error) {
  ps_meta_page *p = NULL;
  int rc = metadata_allocate(v, 1, &p, error);
  if (rc == LC_OK)
    *offset = p->run.offset;
  return rc;
}
static int directory_write(void *v, uint64_t offset, const void *page,
                           lc_error *error) {
  ps_space *s = v;
  int rc = transfer(s, (void *)page, PS_PAGE, offset, 1, error);
  if (rc == LC_OK)
    ++s->directory_page_writes;
  return rc;
}
static int directory_open(ps_space *s, ps_directory **out, lc_error *error) {
  ps_directory_io io;
  memset(&io, 0, sizeof(io));
  io.context = s;
  memcpy(io.identity, s->identity, 16);
  io.read = directory_read;
  io.allocate = directory_allocate;
  io.write = directory_write;
  return ps_directory_open(&io, s->directory, out, error);
}
static uint32_t metadata_crc(const unsigned char *page) {
  return (uint32_t)crc32(checksum(page, 48), page + 52, PS_PAGE - 52);
}
typedef struct metadata_builder {
  ps_space *space;
  ps_meta_page *page;
  unsigned char *buffer;
} metadata_builder;
static int metadata_flush(metadata_builder *b, lc_error *error) {
  ps_space *s = b->space;
  ps_meta_page *p = b->page;
  int rc;
  if (!p)
    return LC_OK;
  put32(b->buffer, META_MAGIC);
  put32(b->buffer + 4, (uint32_t)p->used);
  put64(b->buffer + 8, p->following);
  put64(b->buffer + 16, p->generation);
  put64(b->buffer + 24, p->run.offset);
  memcpy(b->buffer + 32, s->identity, 16);
  put32(b->buffer + 48, metadata_crc(b->buffer));
  rc = transfer(s, b->buffer, PS_PAGE, p->run.offset, 1, error);
  if (rc == LC_OK)
    ++s->descriptor_page_writes;
  return rc;
}
static int metadata_next(metadata_builder *b, int continuation,
                         lc_error *error) {
  ps_meta_page *p;
  int rc = metadata_allocate(b->space, 0, &p, error);
  if (rc != LC_OK)
    return rc;
  if (b->page && continuation)
    b->page->following = p->run.offset;
  rc = metadata_flush(b, error);
  if (rc == LC_OK) {
    b->page = p;
    memset(b->buffer, 0, PS_PAGE);
  }
  return rc;
}
static int metadata_append(metadata_builder *b, const unsigned char *wire,
                           size_t bytes, ps_directory_value *ref,
                           lc_error *error) {
  size_t done = 0, take;
  int rc;
  if (!b->page || b->page->used == META_PAYLOAD) {
    rc = metadata_next(b, 0, error);
    if (rc != LC_OK)
      return rc;
  }
  ref->offset = b->page->run.offset + META_HEADER + b->page->used;
  ref->bytes = bytes;
  ref->incarnation = get64(wire + 40);
  for (;;) {
    ++b->page->references;
    take = META_PAYLOAD - b->page->used;
    if (take > bytes - done)
      take = bytes - done;
    memcpy(b->buffer + META_HEADER + b->page->used, wire + done, take);
    b->page->used += take;
    done += take;
    if (done == bytes)
      return LC_OK;
    rc = metadata_next(b, 1, error);
    if (rc != LC_OK)
      return rc;
  }
}
static int metadata_unref(ps_space *s, ps_directory_value ref,
                          lc_error *error) {
  uint64_t offset = ref.offset / PS_PAGE * PS_PAGE, following;
  size_t at = (size_t)(ref.offset % PS_PAGE), take;
  ps_meta_page *p;
  if (!ref.offset)
    return LC_OK;
  while (ref.bytes) {
    p = metadata_find(s, offset);
    if (!p || p->index || !p->references || at < META_HEADER ||
        at >= META_HEADER + p->used)
      return err(error, LC_ERR_PROTOCOL,
                 "metadata retirement ownership mismatch");
    take = META_HEADER + p->used - at;
    if (take > ref.bytes)
      take = (size_t)ref.bytes;
    ref.bytes -= take;
    following = p->following;
    if (--p->references == 0)
      metadata_release(s, p);
    offset = following;
    at = META_HEADER;
  }
  return LC_OK;
}
static int metadata_prepare_retirement(ps_space *s,
                                       const ps_directory_pages *changes,
                                       lc_error *error) {
  ps_meta_page *page;
  ps_entry *p;
  ps_directory_value ref;
  uint64_t offset;
  size_t i, at, take;
  int rc;
  if (!s->metadata_claims_active)
    return LC_OK;
  for (i = 0; i < changes->retired_count; ++i) {
    page = metadata_find(s, changes->retired[i].offset);
    if (!page || !page->index ||
        page->generation != changes->retired[i].generation)
      return err(error, LC_ERR_PROTOCOL,
                 "directory retirement ownership mismatch");
    rc = metadata_claim_retirement(s, page, error);
    if (rc != LC_OK)
      return rc;
  }
  for (p = s->dirty; p; p = p->dirty_next) {
    ref = p->checkpoint_ref;
    offset = ref.offset / PS_PAGE * PS_PAGE;
    at = (size_t)(ref.offset % PS_PAGE);
    while (ref.bytes) {
      page = metadata_find(s, offset);
      if (!page || page->index || !page->references || at < META_HEADER ||
          at >= META_HEADER + page->used)
        return err(error, LC_ERR_PROTOCOL,
                   "metadata retirement ownership mismatch");
      rc = metadata_claim_retirement(s, page, error);
      if (rc != LC_OK)
        return rc;
      take = META_HEADER + page->used - at;
      if (take > ref.bytes)
        take = (size_t)ref.bytes;
      ref.bytes -= take;
      offset = page->following;
      at = META_HEADER;
    }
  }
  return LC_OK;
}
static void metadata_abort(ps_space *s, uint64_t generation) {
  ps_meta_page *p = s->pages, *next;
  while (p) {
    next = p->next;
    if (p->generation == generation)
      metadata_release(s, p);
    p = next;
  }
}
static void metadata_dirty_all(ps_space *s) {
  ps_entry *p;
  for (p = s->entries; p; p = p->all_next)
    if (p->state == 1 && !p->deleted && !p->dirty_link)
      dirty_entry(s, p, NULL);
}
/* Return only excess dynamic workspace; the original fixed metadata region
 * remains protected. A preallocated node makes post-publication trimming
 * infallible, including when a final interval needs splitting. */
static int metadata_trim_needed(ps_space *s) {
  ps_run *p;
  if (s->metadata_free_bytes <= metadata_target(s, s->metadata_workspace))
    return 0;
  for (p = s->metadata_free; p; p = p->next)
    if (p->offset >= PS_DATA)
      return 1;
  return 0;
}
static ps_run *metadata_trim(ps_space *s, ps_run *spare) {
  uint64_t target = metadata_target(s, s->metadata_workspace), bytes;
  ps_run **link = &s->metadata_free, *node;
  while (*link && s->metadata_free_bytes > target) {
    node = *link;
    if (node->offset < PS_DATA) {
      link = &node->next;
      continue;
    }
    bytes = s->metadata_free_bytes - target;
    if (bytes >= node->bytes) {
      *link = node->next;
      s->metadata_free_bytes -= node->bytes;
      if (s->metadata_claims_active)
        (void)metadata_release_range(s, node->offset, node->bytes, NULL);
      free_insert(s, node);
    } else {
      spare->offset = node->offset;
      spare->bytes = bytes;
      spare->clean = node->clean;
      node->offset += bytes;
      node->bytes -= bytes;
      s->metadata_free_bytes -= bytes;
      if (s->metadata_claims_active)
        (void)metadata_release_range(s, spare->offset, spare->bytes, NULL);
      free_insert(s, spare);
      return NULL;
    }
  }
  return spare;
}
int ps_internal_budget_admit(ps_space *s, int maintenance, lc_error *e) {
  return checkpoint_allowance_floor(s, s->metadata, s->live_entries, 0,
                                    maintenance ? 0 : PS_MAINTENANCE_RESERVE,
                                    NULL, e);
}
void ps_internal_budget_trim(ps_space *s, ps_run **spare) {
  uint64_t bound;
  if (metadata_claimed_bound(s, s->metadata, s->live_entries, &bound, NULL) ==
      LC_OK) {
    s->metadata_workspace = bound;
    *spare = metadata_trim(s, *spare);
  }
}
static int checkpoint_body(ps_space *s, lc_error *error) {
  unsigned char root[PS_ROOT_BYTES], zero[PS_ROOT_BYTES] = {0}, *wire = NULL;
  ps_entry *p;
  ps_meta_page *page;
  ps_run *trim = NULL;
  uint64_t bound, floor = PS_MAINTENANCE_RESERVE;
  ps_directory *directory = NULL;
  ps_directory_root new_root = {0};
  ps_directory_pages changes;
  metadata_builder builder;
  size_t i;
  int next = 1 - s->root, rc, removed, dense = 0;
  if (s->generation == UINT64_MAX)
    return err(error, LC_ERR_INVALID,
               "container checkpoint generation exhausted");
  memset(&builder, 0, sizeof(builder));
  builder.space = s;
  rc = metadata_claimed_bound(s, s->metadata, s->live_entries, &bound, error);
  if (rc == LC_OK)
    s->metadata_workspace = bound;
  if (rc == LC_OK && s->dirty)
    rc = metadata_protect(s, bound, 0, 0, error);
  /* Keep ordinary checkpoints incremental when spare common capacity exists.
   * At capacity, a dense rewrite returns every old metadata page and restores
   * the protected image allowance instead of slowly fragmenting it away. */
  if (rc == LC_OK && s->dirty && s->metadata_free_bytes < 2 * bound &&
      reserve_floor(s, s->metadata, s->live_entries, bound, &floor, NULL) ==
          LC_OK &&
      s->free_bytes >= s->reserved_payload &&
      s->free_bytes - s->reserved_payload >=
          2 * bound - s->metadata_free_bytes &&
      s->free_bytes - s->reserved_payload -
              (2 * bound - s->metadata_free_bytes) >=
          floor)
    rc = metadata_fill(s, bound, 2 * bound, 0, floor, error);
  if (rc == LC_OK && s->dirty && s->metadata_free_bytes < 2 * bound) {
    metadata_dirty_all(s);
    dense = 1;
  }
  if (rc == LC_OK && (s->dirty || metadata_trim_needed(s))) {
    trim = lc_calloc_with_allocator(NULL, 1, sizeof(*trim));
    if (!trim)
      rc = err(error, LC_ERR_NOMEM, "cannot allocate checkpoint trim interval");
  }
  if (rc == LC_OK)
    rc = directory_open(s, &directory, error);
  if (rc == LC_OK && s->dirty) {
    builder.buffer = lc_alloc_with_allocator(NULL, PS_PAGE);
    if (!builder.buffer)
      rc = err(error, LC_ERR_NOMEM, "cannot allocate descriptor page buffer");
  }
  for (p = s->dirty; p && rc == LC_OK; p = p->dirty_next) {
    memset(&p->pending_ref, 0, sizeof(p->pending_ref));
    if (p->deleted)
      rc = ps_directory_remove(directory, p->key, &removed, error);
    else {
      wire = lc_alloc_with_allocator(NULL, entry_bytes(p));
      if (!wire) {
        rc = err(error, LC_ERR_NOMEM, "cannot allocate checkpoint descriptor");
        break;
      }
      encode_entry(wire, p, s->sequence, PS_PUT);
      rc = metadata_append(&builder, wire, entry_bytes(p), &p->pending_ref,
                           error);
      lc_free_with_allocator(NULL, wire);
      wire = NULL;
      if (rc == LC_OK)
        rc = ps_directory_put(directory, p->key, p->pending_ref, error);
    }
  }
  if (rc == LC_OK)
    rc = metadata_flush(&builder, error);
  if (rc == LC_OK)
    rc = ps_directory_flush(directory, s->generation + 1, &new_root, error);
  if (rc == LC_OK) {
    ps_directory_changed_pages(directory, &changes);
    rc = metadata_prepare_retirement(s, &changes, error);
  }
  if (rc == LC_OK)
    rc = sync_file(s, error); /* payloads, journal and new metadata pages */
  memset(root, 0, sizeof(root));
  put64(root, ROOT_MAGIC);
  put64(root + 8, s->generation + 1);
  put64(root + 16, s->capacity);
  put64(root + 24, s->sequence);
  put64(root + 32, new_root.offset);
  put64(root + 40, new_root.generation);
  put64(root + 48, s->token_limit);
  memcpy(root + 56, s->identity, 16);
  put32(root + 72, PS_ROOT_LAYOUT);
  put32(root + 76, checksum(root, 76));
  if (rc == LC_OK) {
    ps_readiness_begin(s, PS_READINESS_ROOT);
    rc = transfer(s, root, sizeof(root), (uint64_t)next * PS_PAGE, 1, error);
  }
  if (rc == LC_OK)
    rc = sync_file(s, error);
  if (rc == LC_OK)
    rc = transfer(s, zero, sizeof(zero), (uint64_t)s->root * PS_PAGE, 1, error);
  if (rc == LC_OK)
    rc = sync_file(s, error); /* obsolete root cannot survive reuse */
  lc_free_with_allocator(NULL, builder.buffer);
  if (rc != LC_OK) {
    if (rc == LC_ERR_PROTOCOL)
      __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
    if (!__atomic_load_n(&s->poisoned, __ATOMIC_RELAXED))
      metadata_abort(s, s->generation + 1);
    if (metadata_retirement_done(s, NULL) != LC_OK) {
      lc_error_cleanup(error);
      rc = healthy(s, error);
    }
    ps_directory_close(directory);
    lc_free_with_allocator(NULL, trim);
    return rc;
  }
  s->root = next;
  ++s->generation;
  ++s->checkpoints;
  s->metadata_fallbacks += dense;
  s->directory = new_root;
  s->journal = 0;
  rc = ps_readiness_stabilize(s, s->sequence, s->generation, error);
  if (rc != LC_OK) {
    ps_directory_close(directory);
    lc_free_with_allocator(NULL, trim);
    return rc;
  }
  s->peer_durable = s->sequence;
  mark_durable(s, __atomic_load_n(&s->pending_sequence, __ATOMIC_ACQUIRE));
  if (s->metadata_claims_active) {
    /* New pages are the prefix inserted by this checkpoint. Selected pages
     * are canonical ownership, not a process-private reservation. */
    for (page = s->pages; page && page->generation == s->generation;
         page = page->next)
      if (metadata_unclaim_page(s, page, error) != LC_OK) {
        lc_free_with_allocator(NULL, trim);
        ps_directory_close(directory);
        return healthy(s, error);
      }
  }
  ps_directory_changed_pages(directory, &changes);
  for (i = 0; i < changes.retired_count; ++i) {
    page = metadata_find(s, changes.retired[i].offset);
    if (!page || !page->index ||
        page->generation != changes.retired[i].generation) {
      rc = err(error, LC_ERR_PROTOCOL,
               "directory retirement ownership mismatch");
      break;
    }
    metadata_release(s, page);
  }
  ps_directory_close(directory);
  while (s->dirty && rc == LC_OK) {
    p = s->dirty;
    rc = metadata_unref(s, p->checkpoint_ref, error);
    s->dirty = p->dirty_next;
    if (s->dirty)
      s->dirty->dirty_link = &s->dirty;
    p->dirty_link = NULL;
    p->checkpoint_ref = p->pending_ref;
    p->previous = NULL;
    if (p->deleted)
      retire(s, p);
  }
  if (rc != LC_OK) {
    __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
    lc_free_with_allocator(NULL, trim);
    return rc;
  }
  rc = metadata_retirement_done(s, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(NULL, trim);
    return rc;
  }
  if (trim)
    trim = metadata_trim(s, trim);
  lc_free_with_allocator(NULL, trim);
  for (p = s->retired; p; p = p->retired_next) {
    p->previous = NULL;
    p->safe = 1;
  }
  collect_retired(s, 1);
  return healthy(s, error);
}
static int checkpoint_locked(ps_space *s, lc_error *error) {
  ps_coordination_guard *guard = NULL;
  int rc = allocation_begin(s, &guard, error);
  if (rc == LC_OK && guard)
    rc = allocation_refresh(s, error);
  if (rc == LC_OK)
    rc = checkpoint_body(s, error);
  return allocation_end(s, guard, rc, error);
}
/* A shared publisher terminates a complete frame with a clear next header.
 * An adopter can distinguish obsolete bytes from an abandoned append even
 * when a shorter frame has overwritten only part of the previous generation. */
static size_t journal_end_marker(size_t offset) {
  size_t remaining = PS_JOURNAL - offset;
  return remaining < 64 ? remaining : 64;
}
static int write_journal_frame(ps_space *s, unsigned char *wire, size_t bytes,
                               lc_error *error) {
  size_t length = bytes;
  int rc;
  if (s->journal_image) {
    size_t marker = journal_end_marker(s->journal + bytes);
    memset(wire + bytes, 0, marker);
    length += marker;
  }
  ps_readiness_begin(s, PS_READINESS_JOURNAL);
  rc = transfer(s, wire, length, JOURNAL_OFFSET + s->journal, 1, error);
  if (rc == LC_OK && s->journal_image)
    memcpy(s->journal_image + s->journal, wire, length);
  return rc;
}
static int append_event_locked(ps_space *s, ps_entry *p, int event,
                               lc_error *error) {
  size_t bytes = event_bytes(p, event);
  unsigned char hint_frame[128];
  unsigned char *wire;
  int rc;
  if (s->sequence == UINT64_MAX ||
      __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED) == UINT64_MAX)
    return err(error, LC_ERR_INVALID,
               "container publication sequence exhausted");
  wire =
      live_hint((uint32_t)event)
          ? hint_frame
          : lc_alloc_with_allocator(NULL, bytes + (s->journal_image ? 64 : 0));
  if (!wire)
    return err(error, LC_ERR_NOMEM, "cannot allocate publication metadata");
  rc = LC_OK;
  if (bytes > PS_JOURNAL - s->journal)
    rc = checkpoint_locked(s, error);
  encode_entry(wire, p, s->sequence + 1, event);
  if (rc == LC_OK)
    rc = write_journal_frame(s, wire, bytes, error);
  if (wire != hint_frame)
    lc_free_with_allocator(NULL, wire);
  if (rc == LC_OK) {
    ++s->sequence;
    s->journal += bytes;
    __atomic_fetch_add(&s->pending_sequence, 1, __ATOMIC_RELEASE);
    rc = ps_readiness_publish(s, error);
  }
  return rc;
}
static int append_event(ps_space *s, ps_entry *p, int event, lc_error *error) {
  ps_coordination_guard *guard = NULL;
  int rc = allocation_begin(s, &guard, error);
  if (rc == LC_OK && guard)
    rc = allocation_refresh(s, error);
  if (rc == LC_OK)
    rc = append_event_locked(s, p, event, error);
  return allocation_end(s, guard, rc, error);
}
/* Reserve incarnation numbers once per open. Returned NoSync references must
 * never alias a later value even if every write since this grant is lost. */
static int grant_tokens(ps_space *s, lc_error *error) {
  ps_coordination_guard *guard = NULL;
  ps_entry grant;
  uint64_t limit;
  int rc = allocation_begin(s, &guard, error);
  if (rc == LC_OK && guard)
    rc = allocation_refresh(s, error);
  if (rc != LC_OK)
    return allocation_end(s, guard, rc, error);
  limit = s->token_limit;
  if (limit > UINT64_MAX - TOKEN_RANGE) {
    rc = err(error, LC_ERR_INVALID, "container incarnation ranges exhausted");
    return allocation_end(s, guard, rc, error);
  }
  memset(&grant, 0, sizeof(grant));
  grant.token = limit + TOKEN_RANGE;
  rc = append_event(s, &grant, PS_RESERVE_IDS, error);
  if (rc == LC_OK) {
    s->token_next = limit + 1;
    s->token_limit = grant.token;
    s->token_end = grant.token;
  }
  return allocation_end(s, guard, rc, error);
}
static void destroy(ps_space *s) {
  ps_entry *p;
  ps_run *r;
  ps_meta_page *page;
  ps_payload_page *payload;
  while (s->entries) {
    p = s->entries;
    s->entries = p->all_next;
    entry_free(p);
  }
  while (s->free) {
    r = s->free;
    s->free = r->next;
    lc_free_with_allocator(NULL, r);
  }
  while (s->metadata_free) {
    r = s->metadata_free;
    s->metadata_free = r->next;
    lc_free_with_allocator(NULL, r);
  }
  while (s->pinned_free) {
    r = s->pinned_free;
    s->pinned_free = r->next;
    lc_free_with_allocator(NULL, r);
  }
  while (s->pages) {
    page = s->pages;
    s->pages = page->next;
    lc_free_with_allocator(NULL, page);
  }
  while (s->payload_pages) {
    payload = s->payload_pages;
    s->payload_pages = payload->next;
    lc_free_with_allocator(NULL, payload);
  }
  lc_free_with_allocator(NULL, s->catalogs.buckets);
  lc_free_with_allocator(NULL, s->journal_image);
  ps_readiness_close(s);
  if (s->coordination) {
    (void)ps_coordination_close(s->coordination, NULL);
    s->fd = -1;
  } else if (s->fd >= 0)
    (void)close(s->fd);
  if (s->locks_initialized >= 4)
    (void)pthread_cond_destroy(&s->commit_cond);
  if (s->locks_initialized >= 3)
    (void)pthread_mutex_destroy(&s->commit_mutex);
  if (s->locks_initialized >= 2)
    (void)pthread_mutex_destroy(&s->sync_io_mutex);
  if (s->locks_initialized >= 1)
    (void)pthread_mutex_destroy(&s->mutex);
  lc_free_with_allocator(NULL, s);
}
static int random_bytes(void *bytes, size_t n, lc_error *error) {
  unsigned char *p = bytes;
  ssize_t got;
  while (n) {
    got = getrandom(p, n, 0);
    if (got < 0 && errno == EINTR)
      continue;
    if (got <= 0)
      return err(error, LC_ERR_TRANSPORT,
                 "cannot obtain container identity entropy");
    p += got;
    n -= (size_t)got;
  }
  return LC_OK;
}
static int root_valid(const unsigned char *p) {
  uint64_t capacity = get64(p + 16);
  uint64_t offset = get64(p + 32), generation = get64(p + 40);
  return get64(p) == ROOT_MAGIC && get64(p + 8) && capacity >= PS_MIB &&
         capacity <= INT64_MAX && capacity % PS_MIB == 0 &&
         ((!offset && !generation) ||
          (offset >= PS_METADATA_START && offset % PS_PAGE == 0 &&
           offset <= capacity - PS_PAGE &&
           (offset < JOURNAL_OFFSET || offset >= PS_DATA) && generation &&
           generation <= get64(p + 8))) &&
         get64(p + 48) && get64(p + 48) % TOKEN_RANGE == 0 &&
         get32(p + 72) == PS_ROOT_LAYOUT && checksum(p, 76) == get32(p + 76);
}
static int canonical_observe(ps_space *s, lc_error *error) {
  unsigned char roots[2 * PS_ROOT_BYTES];
  const unsigned char *root;
  ps_readiness_certificate certificate;
  int rc, a, b;
  if (!s->journal_image)
    return err(error, LC_ERR_PENDING, "container canonical view needs refresh");
  rc = ps_readiness_read(s, &certificate, error);
  if (rc != LC_OK)
    return rc;
  if (certificate.root == s->generation &&
      !(certificate.pending & PS_READINESS_ROOT))
    return canonical_journal(s, &certificate, 0,
                             (certificate.pending & PS_READINESS_JOURNAL) != 0,
                             error);
  rc = transfer(s, roots, PS_ROOT_BYTES, 0, 0, error);
  if (rc == LC_OK)
    rc = transfer(s, roots + PS_ROOT_BYTES, PS_ROOT_BYTES, PS_PAGE, 0, error);
  if (rc != LC_OK)
    return rc;
  a = root_valid(roots);
  b = root_valid(roots + PS_ROOT_BYTES);
  root = b && (!a || get64(roots + PS_ROOT_BYTES + 8) > get64(roots + 8))
             ? roots + PS_ROOT_BYTES
             : roots;
  if ((!a && !b) || get64(root + 8) != s->generation ||
      memcmp(root + 56, s->identity, 16)) {
    if (certificate.root == s->generation)
      ps_readiness_begin(s, PS_READINESS_ROOT);
    return err(error, LC_ERR_PENDING,
               "container checkpoint view needs refresh");
  }
  return canonical_journal(s, &certificate, 0, 1, error);
}
static int canonical_journal(ps_space *s,
                             const ps_readiness_certificate *certificate,
                             int replay, int verify_end, lc_error *error) {
  unsigned char next[64];
  int rc;
  if (s->generation < certificate->root)
    return err(error, LC_ERR_PENDING,
               "certified checkpoint view needs refresh");
  s->peer_durable = certificate->durable;
  if (s->readiness && certificate->durable >= s->own_ready_wire &&
      __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE) <
          s->own_ready_sequence)
    mark_durable(s, s->own_ready_sequence);
  while ((verify_end || s->sequence < certificate->ready) &&
         s->journal <= PS_JOURNAL - sizeof(next)) {
    rc = transfer(s, next, sizeof(next), JOURNAL_OFFSET + s->journal, 0, error);
    if (rc != LC_OK)
      return rc;
    if (!memcmp(next, s->journal_image + s->journal, sizeof(next)))
      break;
    if (s->readiness && s->sequence >= certificate->ready) {
      ps_readiness_begin(s, PS_READINESS_JOURNAL);
      return err(error, LC_ERR_PENDING,
                 "journal boundary is not yet certified");
    }
    if (replay &&
        (get32(next) == REVISION_MAGIC ||
         (get32(next) == EVENT_MAGIC && get32(next + 20) != PS_RESERVE_IDS))) {
      rc = replay_frame(s, next, certificate->ready, error);
      if (rc != LC_OK)
        return rc;
      continue;
    }
    /* Allocation helpers may already hold state derived from the old view.
     * Only a grant can advance their observation without revising that state.
     * Its global frontier is distinct from our private spending endpoint. */
    if (get32(next) != (uint32_t)EVENT_MAGIC || get32(next + 4) != 64 ||
        s->sequence == UINT64_MAX || get64(next + 8) != s->sequence + 1 ||
        get32(next + 16) || get32(next + 20) != PS_RESERVE_IDS ||
        get64(next + 24) || get64(next + 32) ||
        s->token_limit > UINT64_MAX - TOKEN_RANGE ||
        get64(next + 40) != s->token_limit + TOKEN_RANGE || get64(next + 48) ||
        get32(next + 56) || get32(next + 60) != checksum(next, 60) ||
        __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED) == UINT64_MAX)
      return err(error, LC_ERR_PENDING, "container journal view needs refresh");
    memcpy(s->journal_image + s->journal, next, sizeof(next));
    s->token_limit = get64(next + 40);
    ++s->sequence;
    s->journal += sizeof(next);
    /* A reopen grant follows complete rejected-tail retirement and its
     * barrier. A peer may have erased this observer's old checkpoint residue.
     * Later changed prefixes still have to reconcile against these zeros. */
    memset(s->journal_image + s->journal, 0, PS_JOURNAL - s->journal);
    __atomic_fetch_add(&s->pending_sequence, 1, __ATOMIC_RELEASE);
  }
  if (s->readiness && s->sequence < certificate->ready)
    return err(error, LC_ERR_PENDING, "certified journal view needs refresh");
  if (verify_end && (certificate->pending & PS_READINESS_JOURNAL))
    ps_readiness_finish(s, PS_READINESS_JOURNAL);
  return LC_OK;
}
typedef struct metadata_read_page {
  uint64_t offset;
  struct metadata_read_page *next;
  unsigned char bytes[PS_PAGE];
} metadata_read_page;
typedef struct metadata_recovery {
  ps_space *space;
  unsigned char *wire, *buffer;
  uint64_t loaded;
  /* Warm directory order need not match packed descriptor order. Cache each
   * checked metadata page once for this adoption, never application values. */
  metadata_read_page **cache;
} metadata_recovery;
static void metadata_read_cache_close(metadata_recovery *r) {
  metadata_read_page *p, *next;
  size_t i;
  if (!r->cache)
    return;
  for (i = 0; i < BUCKETS; ++i)
    for (p = r->cache[i]; p; p = next) {
      next = p->next;
      lc_free_with_allocator(NULL, p);
    }
  lc_free_with_allocator(NULL, r->cache);
}
static int recover_directory_page(void *v, ps_directory_root ref,
                                  lc_error *error) {
  ps_space *s = ((metadata_recovery *)v)->space;
  ps_meta_page *p;
  int rc;
  if (metadata_find(s, ref.offset))
    return err(error, LC_ERR_PROTOCOL, "overlapping directory ownership");
  p = lc_calloc_with_allocator(NULL, 1, sizeof(*p));
  if (!p)
    return err(error, LC_ERR_NOMEM, "cannot recover directory ownership");
  rc = exclude_run(s, ref.offset, PS_PAGE, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(NULL, p);
    return rc;
  }
  p->run.offset = ref.offset;
  p->run.bytes = PS_PAGE;
  p->generation = ref.generation;
  p->index = 1;
  p->references = 1;
  metadata_register(s, p);
  return LC_OK;
}
static int metadata_read(metadata_recovery *recovery, ps_directory_value ref,
                         lc_error *error) {
  ps_space *s = recovery->space;
  unsigned char *wire = recovery->wire, *buffer = recovery->buffer;
  uint64_t offset = ref.offset / PS_PAGE * PS_PAGE, generation = 0, next;
  size_t at = (size_t)(ref.offset % PS_PAGE), done = 0, take, used;
  ps_meta_page *p;
  metadata_read_page *cached;
  int rc, fresh;
  while (done < ref.bytes) {
    if (!metadata_offset(s, offset))
      return err(error, LC_ERR_PROTOCOL, "metadata record outside container");
    fresh = recovery->loaded != offset;
    if (fresh && recovery->cache)
      for (cached = recovery->cache[(offset / PS_PAGE) % BUCKETS]; cached;
           cached = cached->next)
        if (cached->offset == offset) {
          memcpy(buffer, cached->bytes, PS_PAGE);
          fresh = 0; /* This immutable page has already passed its CRC. */
          break;
        }
    if (fresh) {
      rc = transfer(s, buffer, PS_PAGE, offset, 0, error);
      if (rc != LC_OK)
        return rc;
    }
    used = get32(buffer + 4);
    next = get64(buffer + 8);
    if (get32(buffer) != META_MAGIC || !used || used > META_PAYLOAD ||
        !get64(buffer + 16) || get64(buffer + 16) > s->generation ||
        (generation && get64(buffer + 16) != generation) ||
        get64(buffer + 24) != offset || memcmp(buffer + 32, s->identity, 16) ||
        (fresh && get32(buffer + 48) != metadata_crc(buffer)) ||
        get32(buffer + 52) || get64(buffer + 56) ||
        (next && !metadata_offset(s, next)) || at < META_HEADER ||
        at >= META_HEADER + used)
      return err(error, LC_ERR_PROTOCOL, "invalid packed metadata page");
    if (fresh && recovery->cache) {
      cached = lc_alloc_with_allocator(NULL, sizeof(*cached));
      if (!cached)
        return err(error, LC_ERR_NOMEM,
                   "cannot cache checkpoint descriptor page");
      cached->offset = offset;
      memcpy(cached->bytes, buffer, PS_PAGE);
      cached->next = recovery->cache[(offset / PS_PAGE) % BUCKETS];
      recovery->cache[(offset / PS_PAGE) % BUCKETS] = cached;
    }
    recovery->loaded = offset;
    generation = get64(buffer + 16);
    p = metadata_find(s, offset);
    if (p && (p->index || p->generation != generation || p->used != used ||
              p->following != next))
      return err(error, LC_ERR_PROTOCOL, "conflicting metadata ownership");
    if (!p) {
      p = lc_calloc_with_allocator(NULL, 1, sizeof(*p));
      if (!p)
        return err(error, LC_ERR_NOMEM, "cannot recover descriptor ownership");
      rc = exclude_run(s, offset, PS_PAGE, error);
      if (rc != LC_OK) {
        lc_free_with_allocator(NULL, p);
        return rc;
      }
      p->run.offset = offset;
      p->run.bytes = PS_PAGE;
      p->generation = generation;
      p->used = used;
      p->following = next;
      metadata_register(s, p);
    }
    ++p->references;
    take = META_HEADER + used - at;
    if (take > ref.bytes - done)
      take = (size_t)ref.bytes - done;
    memcpy(wire + done, buffer + at, take);
    done += take;
    if (done < ref.bytes && (!next || used != META_PAYLOAD))
      return err(error, LC_ERR_PROTOCOL, "truncated packed metadata record");
    offset = next;
    at = META_HEADER;
  }
  return LC_OK;
}
static int recover_directory_entry(void *v, ps_key key, ps_directory_value ref,
                                   lc_error *error) {
  metadata_recovery *recovery = v;
  ps_space *s = recovery->space;
  unsigned char *wire = recovery->wire;
  ps_entry *p = NULL;
  int event, rc;
  if (ref.bytes < 64 || ref.bytes > PS_JOURNAL)
    return err(error, LC_ERR_PROTOCOL, "invalid checkpoint descriptor span");
  rc = metadata_read(recovery, ref, error);
  if (rc == LC_OK)
    rc = decode_entry(s, wire, (size_t)ref.bytes, &p, &event, error);
  if (rc == LC_OK && (event != PS_PUT || !same_key(p->key, key) ||
                      p->token != ref.incarnation ||
                      get64(wire + 8) > s->sequence || lookup(s, key)))
    rc = err(error, LC_ERR_PROTOCOL, "checkpoint descriptor binding mismatch");
  if (rc == LC_OK && p->key.kind == PS_CATALOG)
    rc = ps_catalog_prepare_record(s, p, 1, error);
  if (rc == LC_OK && p->key.kind == PS_OWNED_RUN && p->owned_mapped)
    rc = owned_map_load(s, p, error);
  if (rc == LC_OK) {
    if (p->key.kind == PS_CATALOG)
      ps_catalog_apply_record(s, p, 0);
    p->checkpoint_ref = ref;
    register_entry(s, p);
    install(s, p);
  } else if (p)
    entry_free(p);
  return rc;
}
/* Validate the entire candidate before changing any selected ownership. A
 * complete inner record is not independently recoverable inside a revision. */
static int decode_revision(ps_space *s, const unsigned char *wire, size_t bytes,
                           unsigned char *buffer, ps_entry ***out,
                           lc_error *error) {
  ps_entry **entries = NULL, *old;
  size_t count, i, j, cursor = 64, member_bytes;
  int event, rc = LC_OK;
  count = get32(wire + 16);
  if (bytes < 64 || get32(wire) != REVISION_MAGIC || get32(wire + 4) != bytes ||
      !count || count > (bytes - 64) / 64 || count > UINT64_MAX - s->sequence ||
      get64(wire + 8) != s->sequence + 1 || get32(wire + 20) > 1 ||
      get64(wire + 24) || get64(wire + 32) || get64(wire + 40) ||
      get64(wire + 48) || get32(wire + 56) ||
      get32(wire + 60) !=
          (uint32_t)crc32(checksum(wire, 60), wire + 64, (uInt)(bytes - 64)))
    return err(error, LC_ERR_PROTOCOL, "invalid container revision frame");
  entries = lc_calloc_with_allocator(NULL, count, sizeof(*entries));
  if (!entries)
    return err(error, LC_ERR_NOMEM, "cannot allocate revision recovery owners");
  for (i = 0; rc == LC_OK && i < count; ++i) {
    if (bytes - cursor < 64) {
      rc = err(error, LC_ERR_PROTOCOL, "truncated revision member");
      break;
    }
    member_bytes = get32(wire + cursor + 4);
    if (member_bytes < 64 || member_bytes > bytes - cursor ||
        get64(wire + cursor + 8) != s->sequence + 1 + i) {
      rc = err(error, LC_ERR_PROTOCOL, "invalid revision member boundary");
      break;
    }
    rc = decode_entry(s, wire + cursor, member_bytes, &entries[i], &event,
                      error);
    if (rc == LC_OK && (event != PS_PUT || !immutable_key(entries[i]->key)))
      rc = err(error, LC_ERR_PROTOCOL, "invalid revision member kind");
    for (j = 0; rc == LC_OK && j < i; ++j)
      if (same_key(entries[j]->key, entries[i]->key))
        rc = err(error, LC_ERR_PROTOCOL, "duplicate revision member key");
    if (rc == LC_OK) {
      old = lookup(s, entries[i]->key);
      if (old && !old->deleted && entries[i]->key.kind == PS_COLLECTION)
        rc = err(error, LC_ERR_PROTOCOL,
                 "revision overwrites collection position");
    }
    if (rc == LC_OK && buffer)
      rc = verify_entry(s, entries[i], buffer, error);
    cursor += member_bytes;
  }
  if (rc == LC_OK && cursor != bytes)
    rc = err(error, LC_ERR_PROTOCOL, "revision has trailing members");
  if (rc == LC_OK) {
    *out = entries;
    return LC_OK;
  }
  for (i = 0; i < count; ++i)
    if (entries[i])
      entry_free(entries[i]);
  lc_free_with_allocator(NULL, entries);
  return rc;
}
static void install_revision(ps_space *s, ps_entry **entries, size_t count,
                             int peer, int durable) {
  size_t i;
  for (i = 0; i < count; ++i) {
    ps_entry *p = entries[i], *old = lookup(s, p->key);
    if (peer) {
      p->peer_sequence = s->sequence + i + 1;
      p->durable_visibility = durable;
    }
    register_entry(s, p);
    dirty_entry(s, p, old);
    install(s, p);
  }
  s->sequence += count;
}
static int recover_revision(ps_space *s, const unsigned char *wire,
                            size_t bytes, unsigned char *buffer,
                            lc_error *error) {
  ps_entry **entries;
  int rc = decode_revision(s, wire, bytes, buffer, &entries, error);
  if (rc == LC_OK) {
    install_revision(s, entries, get32(wire + 16), 0, 0);
    lc_free_with_allocator(NULL, entries);
  }
  return rc;
}

/* Record only affected intervals and packed pages. Keep removed nodes alive
 * until commit so rollback can restore links even across adjacent cuts. */
typedef struct replay_change {
  struct replay_change *next;
  ps_run **link, *run, *split;
  ps_run **prior_probe;
  ps_run before;
  uint64_t *available, bytes;
  ps_payload_page *page;
  uint64_t occupied;
  int removed, new_page;
} replay_change;
static replay_change *replay_record(replay_change **changes, lc_error *error) {
  replay_change *c = lc_calloc_with_allocator(NULL, 1, sizeof(*c));
  if (c) {
    c->next = *changes;
    *changes = c;
  } else
    err(error, LC_ERR_NOMEM, "cannot stage journal payload ownership");
  return c;
}
static void replay_finish(ps_space *s, replay_change *change, int commit) {
  while (change) {
    replay_change *c = change;
    change = c->next;
    if (c->page) {
      if (commit)
        payload_available(s, c->page);
      else {
        c->page->occupied = c->occupied;
        s->packed_bytes -= c->bytes;
        if (c->new_page) {
          payload_unregister(s, c->page);
          lc_free_with_allocator(NULL, c->page);
        }
      }
    } else if (c->run) {
      if (commit) {
        if (c->removed)
          lc_free_with_allocator(NULL, c->run);
      } else {
        *c->run = c->before;
        *c->link = c->run;
        if (c->prior_probe)
          s->pinned_free_probe = c->prior_probe;
        *c->available += c->bytes;
        lc_free_with_allocator(NULL, c->split);
      }
    }
    lc_free_with_allocator(NULL, c);
  }
}
static ps_run **replay_find(ps_run **link, uint64_t offset) {
  for (; *link; link = &(*link)->next)
    if ((*link)->offset <= offset && offset - (*link)->offset < (*link)->bytes)
      return link;
  return NULL;
}
static int replay_range(ps_space *s, uint64_t offset, uint64_t bytes,
                        replay_change **changes, lc_error *error) {
  while (bytes) {
    uint64_t *available = &s->free_bytes, left, right, take;
    ps_run **link = replay_find(&s->free, offset), *run, *split = NULL;
    replay_change *c;
    if (!link) {
      link = replay_find(&s->pinned_free, offset);
      available = &s->pinned_free_bytes;
    }
    if (!link || (*link)->claimed)
      return err(error, LC_ERR_PROTOCOL,
                 "peer journal overlaps local ownership");
    run = *link;
    left = offset - run->offset;
    take = run->bytes - left;
    if (take > bytes)
      take = bytes;
    right = run->bytes - left - take;
    c = replay_record(changes, error);
    if (!c)
      return LC_ERR_NOMEM;
    if (left && right) {
      split = lc_calloc_with_allocator(NULL, 1, sizeof(*split));
      if (!split)
        return err(error, LC_ERR_NOMEM, "cannot split peer journal interval");
      *split = *run;
      split->offset = offset + take;
      split->bytes = right;
    }
    c->link = link;
    c->run = run;
    c->before = *run;
    c->split = split;
    c->available = available;
    c->bytes = take;
    if (left) {
      run->bytes = left;
      if (split)
        run->next = split;
    } else if (right) {
      run->offset += take;
      run->bytes = right;
    } else {
      /* A suspended quarantine sweep may resume through this node. Move
       * its link before commit can free the node, and undo it on rollback. */
      if (s->pinned_free_probe == &run->next) {
        c->prior_probe = s->pinned_free_probe;
        s->pinned_free_probe = link;
      }
      *link = run->next;
      c->removed = 1;
    }
    *available -= take;
    offset += take;
    bytes -= take;
  }
  return LC_OK;
}
static int replay_payload(ps_space *s, ps_entry *p, replay_change **changes,
                          lc_error *error) {
  size_t i;
  int rc;
  for (i = 0; i < p->count; ++i) {
    ps_extent *x = &p->extents[i];
    uint64_t bytes = extent_allocation(x), mask, base;
    ps_payload_page *page;
    replay_change *c;
    if (!(x->flags & EXTENT_PACKED)) {
      rc = replay_range(s, x->offset, bytes, changes, error);
      if (rc != LC_OK)
        return rc;
      continue;
    }
    base = x->offset / PS_PAGE * PS_PAGE;
    page = payload_find(s, base);
    mask = extent_mask(x);
    if (page && (page->occupied & mask))
      return err(error, LC_ERR_PROTOCOL, "peer journal overlaps packed cells");
    /* A new page needs a free-interval edit before its page undo record. */
    if (!page) {
      rc = replay_range(s, base, PS_PAGE, changes, error);
      if (rc != LC_OK)
        return rc;
    }
    c = replay_record(changes, error);
    if (!c)
      return LC_ERR_NOMEM;
    if (!page) {
      page = lc_calloc_with_allocator(NULL, 1, sizeof(*page));
      if (!page)
        return err(error, LC_ERR_NOMEM, "cannot import packed journal page");
      page->run.offset = base;
      page->run.bytes = PS_PAGE;
      payload_register(s, page);
      c->new_page = 1;
    }
    c->page = page;
    c->occupied = page->occupied;
    c->bytes = bytes;
    page->occupied |= mask;
    s->packed_bytes += bytes;
  }
  return LC_OK;
}
/* Stream completion is live-only. A replacement checkpoint supplies the
 * accepted boundary, with no claim about edits beyond it. */
static int apply_live_hint(ps_space *s, const unsigned char *wire, size_t bytes,
                           int select, lc_error *error) {
  ps_key key;
  ps_entry *head;
  uint64_t value = get64(wire + 48);
  uint32_t event = get32(wire + 20);
  key.kind = get32(wire + 16);
  key.owner = get64(wire + 24);
  key.key = get64(wire + 32);
  if (bytes != 64 || get32(wire + 56) || get32(wire + 60) != checksum(wire, 60))
    return err(error, LC_ERR_PROTOCOL, "invalid live owner hint");
  head = lookup(s, key);
  if (!head || head->deleted || head->token != get64(wire + 40) ||
      (event == PS_NAMESPACE_OWNER
           ? key.kind != PS_CATALOG || key.owner || !head->catalog ||
                 head->catalog->kind != PS_CATALOG_NAMESPACE
           : key.kind != PS_STREAM || head->stream_sealed))
    return err(error, LC_ERR_PROTOCOL, "live owner hint does not match head");
  if (event != PS_STREAM_TAIL && event != PS_STREAM_COMPLETE) {
    if (!value || value > s->token_limit || value % TOKEN_RANGE)
      return err(error, LC_ERR_PROTOCOL, "invalid owned stream writer stamp");
    if (select) {
      head->writer_stamp = value;
      head->writer_generation = s->generation;
      if (event == PS_STREAM_EDIT) {
        head->stream_edit_wire = get64(wire + 8);
        head->stream_complete_generation = 0;
      }
    }
  } else if (value < head->bytes || value > head->stream_capacity)
    return err(error, LC_ERR_PROTOCOL, "owned tail outside granted range");
  else if (select) {
    head->bytes = value;
    head->stream_wire = get64(wire + 8);
    head->stream_complete_generation =
        event == PS_STREAM_COMPLETE ? s->generation : 0;
    if (!head->dirty_link)
      dirty_entry(s, head, NULL);
  }
  return LC_OK;
}
static int apply_peer_frame(ps_space *s, const unsigned char *wire,
                            size_t bytes, uint64_t certified, int staged,
                            lc_error *error) {
  ps_entry **entries = NULL, *single = NULL, *marker_removed = NULL;
  ps_key key;
  replay_change *changes = NULL;
  size_t i, j, count = 1;
  int group = get32(wire) == REVISION_MAGIC, event = PS_PUT, additive,
      marker = 0, structural, rc, retried = 0;
  if (group)
    count = get32(wire + 16);
  if (certified <= s->sequence || !count || count > certified - s->sequence ||
      get64(wire + 8) != s->sequence + 1)
    return err(error, LC_ERR_PROTOCOL, "invalid certified journal framing");
  if (!group && live_hint(get32(wire + 20))) {
    rc = apply_live_hint(s, wire, bytes, 1, error);
    if (rc == LC_OK)
      ++s->sequence;
    return rc;
  }
  key.kind = get32(wire + 16);
  key.owner = get64(wire + 24);
  key.key = get64(wire + 32);
  if (!group && key.kind == PS_CATALOG) {
    if (get32(wire + 20) == PS_REMOVE)
      marker_removed = lookup(s, key);
    marker = ps_catalog_marker_frame(marker_removed, wire, bytes);
    if (!marker)
      marker_removed = NULL;
  }
  additive = !group && get32(wire + 20) == PS_PUT &&
             (key.kind == PS_STREAM ||
              (key.kind == PS_CATALOG && key.key == get64(wire + 40)) ||
              (key.kind == PS_OWNED_RUN && bytes >= 80 && !get32(wire + 72)));
  structural = !group && !immutable_key(key) && !additive && !marker;
  /* New heads, bindings and raw capacity consume ownership without releasing
   * a predecessor. Import their checked frames to keep unrelated writers
   * moving. Inline marker edits also own no external resources. Both live
   * catalog forms keep their previous visible revision until durable.
   */
  if (structural && s->sequence >= s->peer_durable)
    return err(error, LC_ERR_PENDING,
               "peer structural frame awaits durability");
  if (group)
    rc = decode_revision(s, wire, bytes, NULL, &entries, error);
  else {
    rc = decode_entry(s, wire, bytes, &single, &event, error);
    if (rc == LC_OK)
      entries = &single;
  }
  if (rc != LC_OK)
    return rc;
replay_extents:
  for (i = 0; rc == LC_OK && i < count; ++i) {
    if (staged) {
      /* The complete staged projection can be discarded on error. */
      for (j = 0; rc == LC_OK && j < entries[i]->count; ++j)
        rc = payload_claim(s, &entries[i]->extents[j], error);
    } else
      rc = replay_payload(s, entries[i], &changes, error);
  }
  if (!staged && rc == LC_ERR_PROTOCOL && s->retired && !retried) {
    /* Retirement collection can defer a demotion on allocation failure.
     * Undo this frame before collection changes any recorded free links. */
    replay_finish(s, changes, 0);
    changes = NULL;
    collect_retired(s, 0);
    lc_error_cleanup(error);
    rc = healthy(s, error);
    retried = 1;
    if (rc == LC_OK)
      goto replay_extents;
  }
  if (rc == LC_OK && !group) {
    if (event == PS_PUT && single->key.kind == PS_OWNED_RUN) {
      unsigned char header[PS_RUN_IDENTITY_BYTES];
      rc = verify_owned(s, single, header, error);
    }
    if (rc == LC_OK) {
      if (immutable_key(key) || (key.kind == PS_CATALOG && event == PS_PUT))
        single->peer_sequence = s->sequence + 1;
      rc = apply_journal_event(s, single, event, 0, error);
      if (rc == LC_OK && marker_removed)
        marker_removed->peer_sequence = s->sequence + 1;
    }
  }
  replay_finish(s, changes, rc == LC_OK);
  if (rc == LC_OK) {
    if (group)
      install_revision(s, entries, count, 1, get32(wire + 20));
    else
      ++s->sequence;
  } else if (entries)
    for (i = 0; i < count; ++i)
      entry_free(entries[i]);
  if (group)
    lc_free_with_allocator(NULL, entries);
  return rc;
}
static int replay_frame(ps_space *s, const unsigned char *prefix,
                        uint64_t certified, lc_error *error) {
  size_t bytes = get32(prefix + 4);
  unsigned char *body = NULL;
  const unsigned char *wire = prefix;
  int rc = LC_OK;
  if (bytes < 64 || bytes > PS_JOURNAL - s->journal)
    return err(error, LC_ERR_PROTOCOL, "invalid certified journal length");
  if (bytes > 64) {
    body = lc_alloc_with_allocator(NULL, bytes);
    if (!body)
      return err(error, LC_ERR_NOMEM, "cannot load certified journal frame");
    memcpy(body, prefix, 64);
    rc = transfer(s, body + 64, bytes - 64, JOURNAL_OFFSET + s->journal + 64, 0,
                  error);
    wire = body;
  }
  if (rc == LC_OK)
    rc = apply_peer_frame(s, wire, bytes, certified, 0, error);
  if (rc == LC_OK) {
    memcpy(s->journal_image + s->journal, wire, bytes);
    s->journal += bytes;
    memset(s->journal_image + s->journal, 0, journal_end_marker(s->journal));
  }
  lc_free_with_allocator(NULL, body);
  return rc;
}
static int validate_owned_directory(ps_space *s, lc_error *error) {
  ps_entry *p, *run;
  uint64_t count, end, stored, expected = 0, actual = 0;
  for (p = s->entries; p; p = p->all_next) {
    if (p->key.kind == PS_OWNED_RUN) {
      ++actual;
      if (!stream_lookup(s, p->key.owner))
        return err(error, LC_ERR_PROTOCOL, "owned run has no stream head");
    }
    if (p->key.kind != PS_STREAM)
      continue;
    end = count = stored = 0;
    for (run = p->owned_first; run; run = run->owned_next) {
      if (run->deleted || run->key.key < end ||
          run->key.key > p->stream_capacity ||
          run->stream_capacity > p->stream_capacity - run->key.key)
        return err(error, LC_ERR_PROTOCOL, "incomplete owned run directory");
      end = run->key.key + run->stream_capacity;
      stored += entry_storage(run);
      ++count;
    }
    if (count != p->stream_runs || stored != p->stream_storage)
      return err(error, LC_ERR_PROTOCOL, "owned stream capacity mismatch");
    expected += p->stream_runs;
  }
  return actual == expected
             ? LC_OK
             : err(error, LC_ERR_PROTOCOL, "unreferenced owned run directory");
}
static int install_owned_grant(ps_space *s, ps_entry *p, lc_error *error) {
  ps_entry *head = stream_lookup(s, p->key.owner);
  if (!head || head->stream_sealed || p->key.key != head->stream_capacity ||
      p->owned_mapped ||
      p->stream_capacity > (uint64_t)INT64_MAX - head->stream_capacity)
    return err(error, LC_ERR_PROTOCOL, "owned run grant is not contiguous");
  head->stream_capacity += p->stream_capacity;
  ++head->stream_runs;
  if (!head->dirty_link)
    dirty_entry(s, head, NULL);
  return LC_OK;
}
static void mark_removed(ps_space *s, ps_entry *p, uint64_t sequence) {
  if (p->key.kind == PS_OWNED_RUN)
    owned_unlink(s, p);
  if (p->key.kind == PS_STREAM)
    stream_unregister(s, p);
  s->metadata -= entry_bytes(p);
  --s->live_entries;
  p->deleted = 1;
  p->publication_sequence = sequence;
  p->peer_sequence = 0;
  if (!p->dirty_link)
    dirty_entry(s, p, NULL);
}
/* Two walks, no allocation after the journal action. Each owned key belongs
 * exclusively to this head incarnation. Deleted entries retain their payload
 * and checkpoint references until the usual root-retirement protocol. */
static int remove_owned(ps_space *s, ps_entry *head, uint64_t sequence,
                        int apply, lc_error *error) {
  ps_entry *run, *next;
  uint64_t count = 0, end = 0;
  for (run = head->owned_first; run; run = next) {
    next = run->owned_next;
    if (run->deleted || run->key.key < end ||
        run->key.key > head->stream_capacity ||
        run->stream_capacity > head->stream_capacity - run->key.key)
      return err(error, LC_ERR_PROTOCOL, "retired stream grant mismatch");
    end = run->key.key + run->stream_capacity;
    ++count;
    if (apply)
      mark_removed(s, run, sequence);
  }
  if (count != head->stream_runs)
    return err(error, LC_ERR_PROTOCOL, "retired stream capacity mismatch");
  if (apply)
    mark_removed(s, head, sequence);
  return LC_OK;
}
/* Common transition checks for cold recovery and certified live replay.
 * Success consumes the decoded entry. Errors leave it with the caller. */
static int apply_journal_event(ps_space *s, ps_entry *p, int event,
                               uint64_t release, lc_error *error) {
  ps_entry *old, *head;
  int rc;
  if (event == PS_RESERVE_IDS) {
    if (s->token_limit > UINT64_MAX - TOKEN_RANGE ||
        p->token != s->token_limit + TOKEN_RANGE)
      return err(error, LC_ERR_PROTOCOL,
                 "invalid incarnation range progression");
    s->token_limit = p->token;
    entry_free(p);
    return LC_OK;
  }
  old = lookup(s, p->key);
  if (event == PS_PUT) {
    if (p->key.kind == PS_OWNED_RUN) {
      if (p->owned_mapped) {
        head = stream_lookup(s, p->key.owner);
        if (!old || old->deleted || !head ||
            p->stream_capacity != old->stream_capacity)
          return err(error, LC_ERR_PROTOCOL, "mapped piece owner mismatch");
      } else {
        if (old && !old->deleted)
          return err(error, LC_ERR_PROTOCOL,
                     "owned grant overwritten in journal");
        rc = install_owned_grant(s, p, error);
        if (rc != LC_OK)
          return rc;
      }
    } else if (p->key.kind == PS_CATALOG) {
      rc = ps_catalog_prepare_record(s, p, 0, error);
      if (rc != LC_OK)
        return rc;
      ps_catalog_apply_record(s, p, release);
    } else {
      if ((!immutable_key(p->key) || p->key.kind == PS_COLLECTION) && old &&
          !old->deleted)
        return err(error, LC_ERR_PROTOCOL,
                   "journal overwrites typed ownership");
      if (p->key.kind == PS_STREAM && (p->bytes || p->stream_capacity ||
                                       p->stream_runs || p->stream_sealed))
        return err(error, LC_ERR_PROTOCOL, "nonempty initial stream head");
    }
  } else {
    if (!old || old->deleted || old->token != p->token)
      return err(error, LC_ERR_PROTOCOL, "journal retirement owner mismatch");
    if (event == PS_SEAL) {
      if (old->stream_sealed || p->bytes < old->bytes ||
          p->stream_capacity != old->stream_capacity ||
          p->stream_runs != old->stream_runs)
        return err(error, LC_ERR_PROTOCOL, "journal stream seal mismatch");
      old->bytes = p->bytes;
      old->stream_sealed = 1;
      if (!old->dirty_link)
        dirty_entry(s, old, NULL);
      entry_free(p);
      return LC_OK;
    }
    if (p->key.kind == PS_CATALOG) {
      ps_catalog_drop_record(s, old, release);
      entry_free(p);
      return LC_OK;
    }
    if (p->key.kind == PS_STREAM) {
      if (old->catalog_refs)
        return err(error, LC_ERR_PROTOCOL, "journal retires a catalog target");
      rc = remove_owned(s, old, release, 0, error);
      if (rc == LC_OK)
        rc = remove_owned(s, old, release, 1, error);
      if (rc == LC_OK)
        entry_free(p);
      return rc;
    }
    if (p->key.kind == PS_OWNED_RUN) {
      head = stream_lookup(s, p->key.owner);
      if (!head || !head->stream_runs)
        return err(error, LC_ERR_PROTOCOL, "piece removal has no head");
      --head->stream_runs;
      if (!head->dirty_link)
        dirty_entry(s, head, NULL);
    }
    p->deleted = 1;
  }
  register_entry(s, p);
  dirty_entry(s, p, old);
  install(s, p);
  return LC_OK;
}
static int recover(ps_space *s, lc_error *error) {
  unsigned char roots[2 * PS_ROOT_BYTES], zero[PS_ROOT_BYTES] = {0}, *wire,
                                          *buffer;
  ps_readiness_certificate certificate;
  const unsigned char *root;
  ps_directory *directory = NULL;
  metadata_recovery recovery;
  ps_entry *p = NULL;
  struct stat st;
  size_t cursor = 0, bytes, i, first, last;
  int a, b, event, rc;
  rc = transfer(s, roots, PS_ROOT_BYTES, 0, 0, error);
  if (rc == LC_OK)
    rc = transfer(s, roots + PS_ROOT_BYTES, PS_ROOT_BYTES, PS_PAGE, 0, error);
  if (rc != LC_OK)
    return rc;
  a = root_valid(roots);
  b = root_valid(roots + PS_ROOT_BYTES);
  if (!a && !b)
    return err(error, LC_ERR_PROTOCOL, "no recoverable container root");
  if (a && b &&
      (memcmp(roots + 56, roots + PS_ROOT_BYTES + 56, 16) == 0
           ? get64(roots + 8) == get64(roots + PS_ROOT_BYTES + 8)
           : 1))
    return err(error, LC_ERR_PROTOCOL, "conflicting container roots");
  s->root = b && (!a || get64(roots + PS_ROOT_BYTES + 8) > get64(roots + 8));
  root = roots + PS_ROOT_BYTES * s->root;
  s->generation = get64(root + 8);
  s->capacity = get64(root + 16);
  s->sequence = get64(root + 24);
  s->directory.offset = get64(root + 32);
  s->directory.generation = get64(root + 40);
  s->token_limit = get64(root + 48);
  memcpy(s->identity, root + 56, 16);
  rc = ps_readiness_read(s, &certificate, error);
  if (rc != LC_OK)
    return rc;
  if (s->maximum && s->capacity > s->maximum)
    return err(error, LC_ERR_INVALID,
               "committed capacity exceeds configured maximum");
  if (fstat(s->fd, &st) != 0 || st.st_size < (off_t)s->capacity)
    return err(error, LC_ERR_PROTOCOL,
               "container shorter than committed capacity");
  wire = lc_alloc_with_allocator(NULL, PS_JOURNAL);
  buffer = lc_alloc_with_allocator(NULL, PS_CHUNK);
  if (!wire || !buffer) {
    rc = err(error, LC_ERR_NOMEM, "cannot allocate recovery buffers");
    goto done;
  }
  memset(&recovery, 0, sizeof(recovery));
  recovery.space = s;
  recovery.wire = wire;
  recovery.buffer = buffer;
  rc = initial_free(s, error);
  if (rc == LC_OK)
    rc = directory_open(s, &directory, error);
  if (rc == LC_OK)
    rc = ps_directory_visit(directory, recover_directory_entry,
                            recover_directory_page, &recovery, error);
  ps_directory_close(directory);
  if (rc == LC_OK)
    rc = validate_owned_directory(s, error);
  if (rc == LC_OK)
    rc = ps_catalog_validate_directory(s, error);
  if (rc != LC_OK)
    goto done;
  rc = transfer(s, wire, PS_JOURNAL, JOURNAL_OFFSET, 0, error);
  if (rc != LC_OK)
    goto done;
  cursor = 0;
  while (PS_JOURNAL - cursor >= 64 && s->sequence != UINT64_MAX &&
         (get32(wire + cursor) == EVENT_MAGIC ||
          get32(wire + cursor) == REVISION_MAGIC) &&
         get64(wire + cursor + 8) == s->sequence + 1) {
    bytes = get32(wire + cursor + 4);
    if (bytes < 64 || bytes > PS_JOURNAL - cursor)
      break;
    if (get32(wire + cursor) == REVISION_MAGIC) {
      rc = recover_revision(s, wire + cursor, bytes, buffer, error);
      if (rc == LC_ERR_NOMEM || __atomic_load_n(&s->poisoned, __ATOMIC_RELAXED))
        goto done;
      if (rc != LC_OK) {
        lc_error_cleanup(error);
        break;
      }
      cursor += bytes;
      continue;
    }
    if (live_hint(get32(wire + cursor + 20))) {
      if (certificate.root && s->sequence >= certificate.ready)
        break; /* An abandoned live hint must not become certified on open. */
      rc = apply_live_hint(
          s, wire + cursor, bytes,
          certificate.root && s->sequence >= certificate.origin, error);
      if (rc != LC_OK) {
        lc_error_cleanup(error);
        break;
      }
      cursor += bytes;
      ++s->sequence;
      continue;
    }
    rc = decode_entry(s, wire + cursor, bytes, &p, &event, error);
    if (rc == LC_ERR_NOMEM)
      goto done;
    if (rc != LC_OK) {
      lc_error_cleanup(error);
      break;
    }
    if (event == PS_PUT) {
      rc = verify_entry(s, p, buffer, error);
      if (rc != LC_OK) {
        entry_free(p);
        if (__atomic_load_n(&s->poisoned, __ATOMIC_RELAXED))
          goto done;
        lc_error_cleanup(error);
        break;
      }
    }
    rc = apply_journal_event(s, p, event, s->sequence + 1, error);
    if (rc != LC_OK) {
      entry_free(p);
      goto done;
    }
    cursor += bytes;
    ++s->sequence;
  }
  s->journal = cursor;
  rc = ps_readiness_validate(s, error);
  if (rc != LC_OK)
    goto done;
  /* Clear all of the rejected tail, not only its first record: subsequent
   * equal-sized appends must never resurrect a stale suffix with reused seqs.
   * The exact journal image is already in memory. Leave known-zero bytes
   * alone so a clean reopen does not dirty the entire journal for its grant.
   */
  first = cursor;
  last = PS_JOURNAL;
  while (first < last && !wire[first])
    ++first;
  while (last > first && !wire[last - 1])
    --last;
  if (first < last) {
    memset(wire + first, 0, last - first);
    ps_readiness_begin(s, PS_READINESS_JOURNAL);
    rc = transfer(s, wire + first, last - first, JOURNAL_OFFSET + first, 1,
                  error);
    if (rc != LC_OK)
      goto done;
  }
  if (memcmp(roots + (1 - s->root) * PS_ROOT_BYTES, zero, sizeof(zero))) {
    ps_readiness_begin(s, PS_READINESS_ROOT);
    rc = transfer(s, zero, sizeof(zero), (uint64_t)(1 - s->root) * PS_PAGE, 1,
                  error);
    if (rc != LC_OK)
      goto done;
  }
  /* Replay verifies every accepted PUT body, including superseded versions.
   * Reopening must preserve that ownership until a new checkpoint retires the
   * journal; otherwise reuse can make the next replay discard a durable tail.
   */
  for (p = s->entries; p; p = p->all_next) {
    for (i = 0; i < p->count; ++i) {
      rc = payload_claim(s, &p->extents[i], error);
      if (rc != LC_OK)
        goto done;
    }
    p->previous = NULL;
  }
  rc = metadata_claim_initial(s, error);
  if (rc != LC_OK)
    goto done;
  /* The grant reuses rejected journal sequences. Stabilize their retirement
   * before that valid event can reach storage, including an apparently clean
   * retry whose zeros came from an earlier failed recovery's file cache.
   * The final barrier below separately fences the new incarnation range. */
  rc = sync_file(s, error);
  if (rc != LC_OK)
    goto done;
  __atomic_store_n(&s->pending_sequence, s->sequence, __ATOMIC_RELEASE);
  if (ps_coordination_shared(s->coordination)) {
    s->journal_image = wire;
    wire = NULL;
  }
  rc = grant_tokens(s, error);
  if (rc == LC_OK)
    rc = sync_file(s, error); /* accepted tail, retired sibling and grant */
done:
  lc_free_with_allocator(NULL, buffer);
  lc_free_with_allocator(NULL, wire);
  return rc;
}
/* A warm projection owns only resident metadata until installation. Existing
 * private witnesses stay on s; newly staged workspace is tagged 2 so every
 * fallible preparation path can relinquish precisely its own additions. */
static int projection_same(const ps_entry *a, const ps_entry *b) {
  size_t i;
  if (a->deleted || a->count != b->count ||
      a->owned_mapped != b->owned_mapped ||
      (a->key.kind != PS_STREAM && a->bytes != b->bytes) ||
      (a->key.kind == PS_OWNED_RUN && a->stream_capacity != b->stream_capacity))
    return 0;
  for (i = 0; i < a->count; ++i)
    if (a->extents[i].offset != b->extents[i].offset ||
        a->extents[i].bytes != b->extents[i].bytes ||
        a->extents[i].crc != b->extents[i].crc ||
        a->extents[i].flags != b->extents[i].flags)
      return 0;
  if (a->catalog &&
      (a->catalog->kind != b->catalog->kind ||
       a->catalog->length != b->catalog->length ||
       a->catalog->value_length != b->catalog->value_length ||
       !same_key(a->catalog->stream, b->catalog->stream) ||
       a->catalog->token != b->catalog->token ||
       memcmp(a->catalog->name, b->catalog->name, a->catalog->length) ||
       memcmp(a->catalog->value, b->catalog->value, a->catalog->value_length)))
    return 0;
  return a->key.kind != PS_STREAM ||
         (a->stream_capacity <= b->stream_capacity &&
          (!a->stream_sealed || b->stream_sealed) &&
          a->bytes <= b->stream_capacity &&
          (!b->stream_sealed || a->bytes <= b->bytes));
}
static int projection_journal(ps_space *v, uint64_t checkpoint,
                              uint64_t certified, lc_error *error) {
  static const unsigned char zero[64] = {0};
  const unsigned char *p;
  size_t bytes;
  uint32_t magic;
  int rc = transfer(v, v->journal_image, PS_JOURNAL, JOURNAL_OFFSET, 0, error);
  if (rc != LC_OK)
    return rc;
  while (v->journal <= PS_JOURNAL - 64) {
    p = v->journal_image + v->journal;
    magic = get32(p);
    bytes = get32(p + 4);
    if (!memcmp(p, zero, sizeof(zero)))
      return LC_OK;
    if ((magic == (uint32_t)EVENT_MAGIC || magic == REVISION_MAGIC) &&
        bytes >= 64 && bytes <= PS_JOURNAL - v->journal) {
      /* An untouched journal may still start with a checked frame from the
       * previous generation. Once replay starts, require its end marker. */
      if (!v->journal && get64(p + 8) <= checkpoint &&
          get32(p + 60) ==
              (uint32_t)crc32(checksum(p, 60), p + 64, (uInt)(bytes - 64)))
        return LC_OK;
      if (get64(p + 8) > checkpoint && get64(p + 8) <= certified) {
        rc = apply_peer_frame(v, p, bytes, certified, 1, error);
        if (rc != LC_OK)
          return rc;
        v->journal += bytes;
        continue;
      }
    }
    return err(error, LC_ERR_PENDING,
               "checkpoint journal boundary is uncertain");
  }
  return LC_OK;
}
static int projection_pool(ps_space *v, const ps_run *source, int metadata,
                           lc_error *error) {
  ps_run *copy;
  int rc;
  for (; source; source = source->next) {
    if (!metadata && source->claimed != 1)
      continue;
    copy = lc_calloc_with_allocator(NULL, 1, sizeof(*copy));
    if (!copy)
      return err(error, LC_ERR_NOMEM, "cannot preserve private workspace");
    *copy = *source;
    copy->next = NULL;
    copy->claimed = 1;
    rc = exclude_run(v, source->offset, source->bytes, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(NULL, copy);
      return rc;
    }
    if (metadata)
      metadata_insert(v, copy);
    else
      free_insert(v, copy);
  }
  return LC_OK;
}
static int projection_workspace(ps_space *s, ps_space *v, lc_error *error) {
  ps_run *run, *node;
  uint64_t bound, target, bytes, floor = PS_MAINTENANCE_RESERVE;
  int rc;
  /* Initial fixed workspace and overflow additions are claimed only after
   * foreign ranges and this OFD's continuously private ranges are excluded. */
  for (run = v->metadata_free; run; run = run->next) {
    if (run->claimed)
      continue;
    rc = metadata_claim_range(s, run->offset, run->bytes, error);
    if (rc != LC_OK)
      return rc;
    run->claimed = 2;
  }
  v->reserved_payload = s->reserved_payload;
  v->reserved_metadata = s->reserved_metadata;
  v->reserved_entries = s->reserved_entries;
  v->reserve_base = s->reserve_base;
  v->reserves = s->reserves;
  rc = metadata_claimed_bound(v, v->metadata, v->live_entries, &bound, error);
  if (rc == LC_OK)
    rc = reserve_floor(v, v->metadata, v->live_entries, bound, &floor, error);
  if (rc != LC_OK)
    return rc;
  target = metadata_target(v, bound);
  bytes = target > v->metadata_free_bytes ? target - v->metadata_free_bytes : 0;
  if (v->reserved_payload > v->free_bytes ||
      floor > v->free_bytes - v->reserved_payload ||
      bytes > v->free_bytes - v->reserved_payload - floor)
    return err(error, LC_ERR_PENDING,
               "checkpoint needs more protected workspace");
  while (v->metadata_free_bytes < target) {
    node = lc_calloc_with_allocator(NULL, 1, sizeof(*node));
    if (!node)
      return err(error, LC_ERR_NOMEM, "cannot stage checkpoint workspace");
    bytes = target - v->metadata_free_bytes;
    rc = take_interval(&v->free, &v->free_bytes, &bytes, &node->offset,
                       &node->clean, error);
    if (rc == LC_OK)
      rc = metadata_claim_range(s, node->offset, bytes, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(NULL, node);
      return rc;
    }
    node->bytes = bytes;
    node->claimed = 2;
    metadata_insert(v, node);
  }
  v->metadata_workspace = bound;
  return LC_OK;
}
static ps_entry *projected(ps_entry *p) {
  return p && p->projection ? p->projection : p;
}
static void projection_entries(ps_space *s, ps_space *v, ps_entry **items,
                               size_t count) {
  ps_entry *p, *next, *q, *keep = NULL, *dest;
  ps_catalog_record *record;
  size_t i;
  /* Preparation is finished. Nothing below allocates or performs file I/O. */
  for (i = 0; i < count; ++i) {
    q = items[i];
    if (q->projection)
      q->projection->projection = q;
  }
  for (p = s->entries; p; p = next) {
    next = p->all_next;
    if (p->projection) {
      p->projection = NULL;
      continue;
    }
    if (p->state != 0 && !p->pins && !p->catalog_pins) {
      entry_free(p); /* Drop stale resident metadata, never free its ranges. */
      continue;
    }
    p->previous = p->hash_next = p->stream_next = NULL;
    p->retired_next = p->dirty_next = NULL;
    p->retired_link = p->dirty_link = NULL;
    p->checkpoint_ref.offset = p->checkpoint_ref.bytes = 0;
    memset(&p->pending_ref, 0, sizeof(p->pending_ref));
    p->owned_first = p->owned_last = p->owned_prev = p->owned_next = NULL;
    p->owned_linked = 0;
    if (p->catalog)
      p->catalog->link = NULL;
    if (p->state != 0) {
      p->state = 2;
      p->safe = 1;
      if (p->key.kind == PS_STREAM || p->key.kind == PS_OWNED_RUN)
        p->deleted = 1;
    }
    p->all_next = keep;
    keep = p;
  }
  s->retired = projected(v->retired);
  s->dirty = projected(v->dirty);
  for (i = 0; i < count; ++i) {
    q = items[i];
    dest = projected(q);
    if (dest != q) {
      /* Captured readers access immutable fields without the allocator
       * mutex. Even assigning the same bytes/pointers would race them. Only
       * replace canonical links and mutex-protected projection metadata. */
      dest->publication_sequence = q->publication_sequence;
      dest->peer_sequence = q->peer_sequence;
      dest->durable_visibility = q->durable_visibility;
      dest->state = q->state;
      dest->safe = q->safe;
      dest->deleted = q->deleted;
      dest->checkpoint_ref = q->checkpoint_ref;
      dest->pending_ref = q->pending_ref;
      dest->catalog_refs = q->catalog_refs;
      dest->catalog_release = 0;
      dest->owned_linked = q->owned_linked;
      dest->writer_stamp = q->writer_stamp;
      dest->writer_generation = q->writer_generation;
      if (dest->key.kind == PS_STREAM) {
        dest->stream_capacity = q->stream_capacity;
        dest->stream_runs = q->stream_runs;
        dest->stream_storage = q->stream_storage;
        dest->stream_sealed = q->stream_sealed;
        dest->stream_wire = q->stream_wire;
        dest->stream_edit_wire = q->stream_edit_wire;
        dest->stream_complete_generation = q->stream_complete_generation;
        dest->bytes = q->bytes;
      }
    }
    dest->all_next = projected(q->all_next);
    dest->hash_next = projected(q->hash_next);
    dest->stream_next = projected(q->stream_next);
    dest->previous = projected(q->previous);
    dest->retired_next = projected(q->retired_next);
    dest->dirty_next = projected(q->dirty_next);
    dest->retired_link = dest->dirty_link = NULL;
    dest->owned_first = projected(q->owned_first);
    dest->owned_last = projected(q->owned_last);
    dest->owned_prev = projected(q->owned_prev);
    dest->owned_next = projected(q->owned_next);
    if ((record = dest->catalog) != NULL) {
      record->target = projected(q->catalog->target);
      record->next = projected(q->catalog->next);
    }
  }
  s->entries = projected(v->entries);
  for (i = 0; i < BUCKETS; ++i) {
    s->buckets[i] = projected(v->buckets[i]);
    s->stream_buckets[i] = projected(v->stream_buckets[i]);
  }
  for (i = 0; i < v->catalogs.capacity; ++i)
    v->catalogs.buckets[i] = projected(v->catalogs.buckets[i]);
  for (p = s->entries; p; p = p->all_next) {
    p->all_link = p == s->entries ? &s->entries : p->all_link;
    if (p->all_next)
      p->all_next->all_link = &p->all_next;
  }
  {
    ps_entry **link;
    for (link = &s->retired; *link; link = &(*link)->retired_next)
      (*link)->retired_link = link;
    for (link = &s->dirty; *link; link = &(*link)->dirty_next)
      (*link)->dirty_link = link;
  }
  while (keep) {
    p = keep;
    keep = p->all_next;
    register_entry(s, p);
    if (p->state == 2) {
      p->retired_link = &s->retired;
      p->retired_next = s->retired;
      if (p->retired_next)
        p->retired_next->retired_link = &p->retired_next;
      s->retired = p;
    }
  }
  for (i = 0; i < count; ++i)
    if (items[i]->projection)
      entry_free(items[i]);
  v->entries = NULL;
}
static void projection_install(ps_space *s, ps_space *v) {
  ps_entry *p, **link;
  ps_payload_page *page;
  ps_run *run;
  size_t i;
#define PROJECTION_SWAP(field, type)                                           \
  do {                                                                         \
    type previous = s->field;                                                  \
    s->field = v->field;                                                       \
    v->field = previous;                                                       \
  } while (0)
  PROJECTION_SWAP(free, ps_run *);
  PROJECTION_SWAP(metadata_free, ps_run *);
  PROJECTION_SWAP(pinned_free, ps_run *);
  PROJECTION_SWAP(pages, ps_meta_page *);
  PROJECTION_SWAP(payload_pages, ps_payload_page *);
  PROJECTION_SWAP(catalogs, ps_catalog_index);
  PROJECTION_SWAP(journal_image, unsigned char *);
#undef PROJECTION_SWAP
  memcpy(s->page_buckets, v->page_buckets, sizeof(s->page_buckets));
  memcpy(s->payload_buckets, v->payload_buckets, sizeof(s->payload_buckets));
  memcpy(s->available, v->available, sizeof(s->available));
  s->pinned_pages = v->pinned_pages;
  s->pinned_page_probe = NULL;
  s->pinned_free_probe = NULL;
  if (s->pages)
    s->pages->link = &s->pages;
  if (s->payload_pages)
    s->payload_pages->link = &s->payload_pages;
  for (i = 0; i <= CELLS; ++i)
    if ((page = s->available[i]) != NULL)
      page->available_link = &s->available[i];
  for (i = 0; i < s->catalogs.capacity; ++i)
    for (link = &s->catalogs.buckets[i]; (p = *link) != NULL;
         link = &p->catalog->next)
      p->catalog->link = link;
  for (run = s->metadata_free; run; run = run->next)
    run->claimed = 0;
  s->root = v->root;
  s->capacity = v->capacity;
  s->generation = v->generation;
  s->sequence = v->sequence;
  s->token_limit = v->token_limit;
  s->journal = v->journal;
  s->directory = v->directory;
  s->metadata = v->metadata;
  s->live_entries = v->live_entries;
  s->metadata_pages = v->metadata_pages;
  s->metadata_workspace = v->metadata_workspace;
  s->metadata_free_bytes = v->metadata_free_bytes;
  s->free_bytes = v->free_bytes;
  s->packed_pages = v->packed_pages;
  s->packed_bytes = v->packed_bytes;
  s->pinned_free_bytes = v->pinned_free_bytes;
  s->peer_durable = v->peer_durable;
  if (s->peer_durable >= s->own_ready_wire)
    mark_durable(s, s->own_ready_sequence);
}
static int canonical_refresh(ps_space *s, int verify_disk, lc_error *error) {
  unsigned char roots[2 * PS_ROOT_BYTES], zero[PS_ROOT_BYTES] = {0},
                                          *buffer = NULL;
  const unsigned char *root;
  ps_space *v = NULL;
  ps_directory *directory = NULL;
  metadata_recovery recovery;
  ps_entry *p, *old, **items = NULL;
  ps_run *run;
  struct stat st;
  size_t count = 0, i;
  int a, b, selected, rc;
  ps_readiness_certificate certificate;
  memset(&recovery, 0, sizeof(recovery));
  rc = ps_readiness_read(s, &certificate, error);
  if (rc != LC_OK)
    return rc;
  /* A writer announces every root or journal change before touching disk.
   * Under allocation authority the checked certificate can therefore select
   * the resident root and bound journal import without probing either end. */
  if (!verify_disk && certificate.root == s->generation &&
      !(certificate.pending & PS_READINESS_ROOT))
    return canonical_journal(s, &certificate, 1,
                             (certificate.pending & PS_READINESS_JOURNAL) != 0,
                             error);
  rc = transfer(s, roots, PS_ROOT_BYTES, 0, 0, error);
  if (rc == LC_OK)
    rc = transfer(s, roots + PS_ROOT_BYTES, PS_ROOT_BYTES, PS_PAGE, 0, error);
  if (rc != LC_OK)
    return rc;
  a = root_valid(roots);
  b = root_valid(roots + PS_ROOT_BYTES);
  selected = b && (!a || get64(roots + PS_ROOT_BYTES + 8) > get64(roots + 8));
  root = roots + selected * PS_ROOT_BYTES;
  if ((!a && !b) || memcmp(root + 56, s->identity, 16) ||
      (a && b &&
       (get64(roots + 8) == get64(roots + PS_ROOT_BYTES + 8) ||
        memcmp(roots + 56, roots + PS_ROOT_BYTES + 56, 16)))) {
    ps_readiness_begin(s, PS_READINESS_ROOT);
    return err(error, LC_ERR_PROTOCOL,
               "conflicting checkpoint projection roots");
  }
  if (get64(root + 8) != certificate.root)
    ps_readiness_begin(s, PS_READINESS_ROOT);
  if (get64(root + 8) == s->generation)
    return canonical_journal(s, &certificate, 1, 1, error);
  if (get64(root + 8) < s->generation || get64(root + 24) < s->sequence ||
      get64(root + 16) < s->capacity || get64(root + 48) < s->token_limit)
    return err(error, LC_ERR_PENDING,
               "checkpoint would regress a live projection");
  if (get64(root + 8) < certificate.root)
    return err(error, LC_ERR_PENDING,
               "selected root is behind certified retirement");
  if ((s->maximum && get64(root + 16) > s->maximum) || fstat(s->fd, &st) ||
      st.st_size < (off_t)get64(root + 16))
    return err(error, LC_ERR_INVALID, "checkpoint capacity unavailable");
  v = lc_calloc_with_allocator(NULL, 1, sizeof(*v));
  buffer = lc_alloc_with_allocator(NULL, PS_PAGE);
  if (!v || !buffer) {
    rc = err(error, LC_ERR_NOMEM, "cannot allocate checkpoint projection");
    goto done;
  }
  v->fd = s->fd;
  v->coordination = s->coordination;
  v->io = s->io;
  v->root = selected;
  v->generation = get64(root + 8);
  v->capacity = get64(root + 16);
  v->sequence = get64(root + 24);
  v->peer_durable = certificate.durable;
  v->directory.offset = get64(root + 32);
  v->directory.generation = get64(root + 40);
  v->token_limit = get64(root + 48);
  memcpy(v->identity, s->identity, 16);
  v->journal_image = lc_alloc_with_allocator(NULL, PS_JOURNAL);
  if (!v->journal_image) {
    rc = err(error, LC_ERR_NOMEM, "cannot allocate projection control buffer");
    goto done;
  }
  recovery.space = v;
  recovery.wire = v->journal_image;
  recovery.buffer = buffer;
  recovery.cache =
      lc_calloc_with_allocator(NULL, BUCKETS, sizeof(*recovery.cache));
  if (!recovery.cache) {
    rc = err(error, LC_ERR_NOMEM, "cannot allocate projection page cache");
    goto done;
  }
  rc = initial_free(v, error);
  if (rc == LC_OK)
    rc = directory_open(v, &directory, error);
  if (rc == LC_OK)
    rc = ps_directory_visit(directory, recover_directory_entry,
                            recover_directory_page, &recovery, error);
  ps_directory_close(directory);
  directory = NULL;
  if (rc == LC_OK)
    rc = validate_owned_directory(v, error);
  if (rc == LC_OK)
    rc = ps_catalog_validate_directory(v, error);
  if (rc != LC_OK)
    goto done;
  /* The checkpoint prefix covers the resident journal. Consequently only
   * checkpoint entries, including those superseded by the following tail,
   * can match an existing incarnation. Bind them before decoding that tail. */
  for (p = v->entries; p; p = p->all_next) {
    old = lookup(s, p->key);
    if (old && old->token == p->token) {
      if (!projection_same(old, p)) {
        rc = err(error, LC_ERR_PROTOCOL,
                 "checkpoint changes an existing incarnation");
        goto done;
      }
      p->projection = old;
      if (p->key.kind == PS_STREAM && old->bytes > p->bytes) {
        p->bytes = old->bytes;
        dirty_entry(v, p, NULL);
      }
    }
    for (i = 0; i < p->count; ++i) {
      rc = payload_claim(v, &p->extents[i], error);
      if (rc != LC_OK)
        goto done;
    }
  }
  /* Overlay only continuously retained ownership. Unpinned old canonical
   * ranges may already have been reused by peers; never retire them here. */
  for (p = s->entries; p; p = p->all_next) {
    old = lookup(v, p->key);
    if ((old && old->projection == p) ||
        (p->state != 0 && !p->pins && !p->catalog_pins))
      continue;
    for (i = 0; i < p->count; ++i) {
      rc = payload_claim(v, &p->extents[i], error);
      if (rc != LC_OK)
        goto done;
    }
  }
  rc = projection_pool(v, s->metadata_free, 1, error);
  if (rc == LC_OK)
    rc = projection_pool(v, s->free, 0, error);
  if (rc == LC_OK)
    rc = projection_journal(v, get64(root + 24), certificate.ready, error);
  if (rc == LC_OK && v->sequence < certificate.ready)
    rc = err(error, LC_ERR_PENDING,
             "checkpoint is behind the certified journal prefix");
  if (rc == LC_ERR_PENDING || rc == LC_ERR_PROTOCOL)
    ps_readiness_begin(s, PS_READINESS_JOURNAL);
  if (rc != LC_OK)
    goto done;
  /* Include every superseded frame still needed by readers or cold replay.
   * The vector lets installation rewire links without walking a live list
   * whose nodes are moving in place. */
  for (p = v->entries; p; p = p->all_next) {
    if (count == (size_t)-1 / sizeof(*items)) {
      rc = err(error, LC_ERR_NOMEM, "checkpoint projection is too large");
      goto done;
    }
    ++count;
  }
  if (count) {
    items = lc_calloc_with_allocator(NULL, count, sizeof(*items));
    if (!items) {
      rc =
          err(error, LC_ERR_NOMEM, "cannot bind checkpoint projection entries");
      goto done;
    }
    for (i = 0, p = v->entries; p; p = p->all_next)
      items[i++] = p;
  }
  v->range_pins = 1;
  if (rc == LC_OK)
    rc = pins_recover(v, error);
  if (rc == LC_OK)
    rc = projection_workspace(s, v, error);
  /* Even apparently zero sibling bytes can be an abandoned cached retirement
   * whose barrier never completed. Stabilize that retirement before admitting
   * any of its ranges. This is a structural adoption barrier, not a journal
   * refresh or per-record commit barrier. Shared readiness may later certify
   * it.
   */
  if (rc == LC_OK &&
      memcmp(roots + (1 - selected) * PS_ROOT_BYTES, zero, sizeof(zero))) {
    ps_readiness_begin(s, PS_READINESS_ROOT);
    rc = transfer(s, zero, sizeof(zero), (uint64_t)(1 - selected) * PS_PAGE, 1,
                  error);
  }
  if (rc == LC_OK &&
      (!s->readiness || certificate.root < v->generation ||
       memcmp(roots + (1 - selected) * PS_ROOT_BYTES, zero, sizeof(zero)))) {
    rc = sync_file(s, error);
    if (rc == LC_OK) {
      rc = ps_readiness_stabilize(s, v->sequence, v->generation, error);
      if (rc == LC_OK)
        v->peer_durable = v->sequence;
    }
  }
  if (rc == LC_OK) {
    projection_entries(s, v, items, count);
    projection_install(s, v);
    if (certificate.pending & PS_READINESS_JOURNAL)
      ps_readiness_finish(s, PS_READINESS_JOURNAL);
  } else {
  done:
    if (v)
      for (run = v->metadata_free; run; run = run->next)
        if (run->claimed == 2)
          (void)metadata_release_range(s, run->offset, run->bytes, NULL);
  }
  if (v) {
    if (v->poisoned)
      __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
    v->fd = -1;
    v->coordination = NULL;
    destroy(v);
  }
  lc_free_with_allocator(NULL, items);
  lc_free_with_allocator(NULL, buffer);
  metadata_read_cache_close(&recovery);
  return rc;
}
int ps_refresh(ps_space *s, lc_error *error) {
  ps_coordination_guard *guard;
  int rc;
  if (!s)
    return err(error, LC_ERR_INVALID, "checkpoint refresh requires a space");
  pthread_mutex_lock(&s->mutex);
  rc = capture_begin(s, &guard, 1, error);
  rc = ps_internal_capture_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
static int sync_parent(const char *path, lc_error *error) {
  char *parent = lc_strdup_with_allocator(NULL, path), *slash;
  int fd, rc;
  if (!parent)
    return err(error, LC_ERR_NOMEM, "cannot allocate container parent path");
  slash = strrchr(parent, '/');
  if (!slash) {
    lc_free_with_allocator(NULL, parent);
    parent = lc_strdup_with_allocator(NULL, ".");
    if (!parent)
      return err(error, LC_ERR_NOMEM, "cannot allocate container parent path");
  } else if (slash == parent)
    slash[1] = '\0';
  else
    *slash = '\0';
  fd = open(parent, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  lc_free_with_allocator(NULL, parent);
  if (fd < 0)
    return err(error, LC_ERR_TRANSPORT,
               "cannot open container parent for sync");
  do {
    rc = fsync(fd);
  } while (rc < 0 && errno == EINTR);
  (void)close(fd);
  return rc == 0 ? LC_OK
                 : err(error, LC_ERR_TRANSPORT, "container parent sync failed");
}
int ps_internal_open(const char *path, int create, const ps_options *options,
                     int hold_initialization, ps_space **out, lc_error *error) {
  ps_options defaults = {0};
  ps_coordination_options coordination = {0};
  ps_coordination_guard *opening = NULL;
  ps_space *s;
  uint64_t bound;
  int rc;
  if (out)
    *out = NULL;
  if (!path || !*path || !out)
    return err(error, LC_ERR_INVALID,
               "container open requires path and output");
  if (!options)
    options = &defaults;
  if (options->shared != 0 && options->shared != 1)
    return err(error, LC_ERR_INVALID, "invalid container ownership mode");
  if ((options->initial_capacity && (options->initial_capacity < PS_MIB ||
                                     options->initial_capacity % PS_MIB ||
                                     options->initial_capacity > INT64_MAX)) ||
      (options->maximum_capacity && (options->maximum_capacity % PS_MIB ||
                                     options->maximum_capacity > INT64_MAX)))
    return err(
        error, LC_ERR_INVALID,
        "container capacity must be whole MiB within signed 64-bit offsets");
  s = lc_calloc_with_allocator(NULL, 1, sizeof(*s));
  if (!s)
    return err(error, LC_ERR_NOMEM, "cannot allocate container handle");
  s->fd = -1;
  rc = pthread_mutex_init(&s->mutex, NULL);
  if (rc) {
    lc_free_with_allocator(NULL, s);
    return err(error, LC_ERR_TRANSPORT, "cannot initialize container mutex");
  }
  s->locks_initialized = 1;
  rc = pthread_mutex_init(&s->sync_io_mutex, NULL);
  if (rc == 0) {
    s->locks_initialized = 2;
    rc = pthread_mutex_init(&s->commit_mutex, NULL);
  }
  if (rc == 0) {
    s->locks_initialized = 3;
    rc = pthread_cond_init(&s->commit_cond, NULL);
  }
  if (rc != 0) {
    destroy(s);
    return err(error, LC_ERR_TRANSPORT,
               "cannot initialize durability coordination");
  }
  s->locks_initialized = 4;
  s->io.read = native_read;
  s->io.write = native_write;
  s->io.sync = native_sync;
  s->io.reserve = native_reserve;
  s->io.zero = native_zero;
  if (options->io) {
    s->io = *options->io;
    if (!s->io.read || !s->io.write || !s->io.sync || !s->io.reserve) {
      destroy(s);
      return err(error, LC_ERR_INVALID,
                 "container I/O callbacks must be complete");
    }
  }
  s->durable = options->durable_sync;
  s->maximum = options->maximum_capacity;
  s->capacity = options->initial_capacity ? options->initial_capacity
                                          : PS_DEFAULT_CAPACITY;
  if (create && s->maximum && s->capacity > s->maximum) {
    destroy(s);
    return err(error, LC_ERR_INVALID, "initial capacity exceeds maximum");
  }
  s->fd = open(
      path, O_RDWR | O_CLOEXEC | O_NOFOLLOW | (create ? O_CREAT | O_EXCL : 0),
      0600);
  if (s->fd < 0) {
    rc = io_error(s, error, "cannot open container file");
    destroy(s);
    return rc;
  }
  coordination.failure_flag = &s->poisoned;
  coordination.io = options->coordination_io;
  coordination.shared = options->shared;
  coordination.wait_lifetime = create != 0;
  rc = ps_coordination_open(s->fd, &coordination, &s->coordination, error);
  if (rc != LC_OK) {
    if (rc == LC_ERR_PENDING)
      rc = err(error, LC_ERR_INVALID,
               "container already open; another owner is active");
    destroy(s);
    return rc;
  }
  rc = ps_coordination_initialize(s->coordination, create, 1, error);
  if (rc == LC_OK)
    rc = pin_conflict(s, PIN_PRESENCE_OFFSET, 1, &s->range_pins, error);
  if (rc != LC_OK) {
    destroy(s);
    return rc;
  }
  if (ps_coordination_shared(s->coordination))
    s->range_pins = 1; /* Later participants may introduce new witnesses. */
  rc = allocation_begin(s, &opening, error);
  if (rc == LC_OK && create && opening) {
    s->journal_image = lc_calloc_with_allocator(NULL, 1, PS_JOURNAL);
    if (!s->journal_image)
      rc = err(error, LC_ERR_NOMEM, "cannot allocate observed control journal");
  }
  if (rc == LC_OK && create) {
    rc = reserve_file(s, 0, s->capacity, error);
    if (rc == LC_OK)
      rc = ps_readiness_attach(s, error);
    if (rc == LC_OK)
      rc = random_bytes(s->identity, 16, error);
    s->token_next = 1;
    s->token_limit = TOKEN_RANGE;
    s->token_end = TOKEN_RANGE;
    if (rc == LC_OK)
      rc = initial_free(s, error);
    if (rc == LC_OK)
      rc = metadata_claim_initial(s, error);
    if (rc == LC_OK)
      rc = checkpoint_locked(s, error);
    if (rc == LC_OK)
      rc = sync_parent(path, error);
  } else if (rc == LC_OK) {
    rc = ps_readiness_attach(s, error);
    if (rc == LC_OK)
      rc = recover(s, error);
  }
  if (rc == LC_OK && s->maximum && s->capacity > s->maximum)
    rc = err(error, LC_ERR_INVALID,
             "committed capacity exceeds configured maximum");
  if (rc == LC_OK)
    rc = metadata_bound(s->metadata, s->live_entries, &bound, error);
  if (rc == LC_OK)
    rc = metadata_protect(s, bound, 0, 0, error);
  if (rc == LC_OK)
    rc = ps_readiness_opened(s, error);
  rc = allocation_end(s, opening, rc, error);
  if (rc == LC_OK && !(create && hold_initialization))
    rc = ps_coordination_initialize(s->coordination, create, 0, error);
  if (rc != LC_OK) {
    destroy(s);
    return rc;
  }
  mark_durable(s, __atomic_load_n(&s->pending_sequence, __ATOMIC_ACQUIRE));
  *out = s;
  return LC_OK;
}
int ps_open(const char *path, int create, const ps_options *options,
            ps_space **out, lc_error *error) {
  return ps_internal_open(path, create, options, 0, out, error);
}
int ps_close(ps_space *s, lc_error *error) {
  if (!s)
    return LC_OK;
  pthread_mutex_lock(&s->mutex);
  if (s->readers || s->writers || s->budgets || s->reserve_count ||
      ps_coordination_users(s->coordination)) {
    pthread_mutex_unlock(&s->mutex);
    return err(error, LC_ERR_INVALID,
               "close container readers, writers, budgets, reserves and "
               "coordination guards first");
  }
  pthread_mutex_unlock(&s->mutex);
  destroy(s);
  return LC_OK;
}
ps_coordination *ps_space_coordination(ps_space *s) {
  return s ? s->coordination : NULL;
}
int ps_space_private_active(ps_space *s, size_t cached_budgets) {
  int active;
  if (!s)
    return 0;
  pthread_mutex_lock(&s->mutex);
  active = s->writers != 0 || s->budgets > cached_budgets;
  pthread_mutex_unlock(&s->mutex);
  return active;
}
static int begin_value(ps_space *s, ps_budget *budget, ps_key key, int owned,
                       ps_writer **out, lc_error *error) {
  ps_writer *w;
  ps_entry *p;
  int rc;
  if (out)
    *out = NULL;
  if (!s || !out ||
      (!immutable_key(key) && !(owned && key.kind == PS_OWNED_RUN)))
    return err(error, LC_ERR_INVALID,
               "container writer requires typed key and output");
  w = lc_calloc_with_allocator(NULL, 1, sizeof(*w));
  p = lc_calloc_with_allocator(NULL, 1, sizeof(*p));
  if (!w || !p) {
    lc_free_with_allocator(NULL, w);
    lc_free_with_allocator(NULL, p);
    return err(error, LC_ERR_NOMEM, "cannot allocate container writer");
  }
  p->key = key;
  w->entry = p;
  w->space = s;
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc == LC_OK)
    rc = ps_internal_budget_check(s, budget, 0, 0, 0, error);
  if (rc == LC_OK && s->token_next > s->token_end)
    rc = err(error, LC_ERR_INVALID,
             "incarnation grant consumed; close and reopen");
  if (rc == LC_OK) {
    p->token = s->token_next++;
    register_entry(s, p);
    ++s->writers;
    w->budget = budget;
    if (budget)
      ++budget->users;
  }
  pthread_mutex_unlock(&s->mutex);
  if (rc != LC_OK) {
    entry_free(p);
    lc_free_with_allocator(NULL, w);
  } else
    *out = w;
  return rc;
}
int ps_begin(ps_space *s, ps_budget *budget, ps_key key, ps_writer **out,
             lc_error *error) {
  return begin_value(s, budget, key, 0, out, error);
}
static int flush_writer(ps_writer *w, lc_error *error) {
  ps_space *s = w->space;
  ps_entry *p = w->entry;
  ps_extent *extent, *grown;
  size_t n, done = 0, take;
  uint64_t run_bytes;
  ps_coordination_guard *guard = NULL;
  int rc, split = 0;
  if (!w->used)
    return LC_OK;
  while (done < w->used) {
    if (p->count == (PS_JOURNAL - 64) / 24)
      return err(error, LC_ERR_INVALID,
                 "value extent metadata exceeds journal capacity");
    pthread_mutex_lock(&s->mutex);
    rc = healthy(s, error);
    if (rc != LC_OK) {
      pthread_mutex_unlock(&s->mutex);
      return rc;
    }
    if (p->count == p->capacity) {
      n = p->capacity ? p->capacity * 2 : 4;
      grown = lc_realloc_with_allocator(NULL, p->extents, n * sizeof(*grown));
      if (!grown) {
        pthread_mutex_unlock(&s->mutex);
        return err(error, LC_ERR_NOMEM, "cannot grow private extent vector");
      }
      p->extents = grown;
      p->capacity = n;
    }
    rc = ps_internal_admission_begin(s, &guard, error);
    if (rc != LC_OK) {
      rc = allocation_end(s, guard, rc, error);
      pthread_mutex_unlock(&s->mutex);
      return rc;
    }
    extent = &p->extents[p->count];
    memset(extent, 0, sizeof(*extent));
    take = w->used - done;
    run_bytes = take / PS_PAGE * PS_PAGE;
    if (!run_bytes) {
      n = take;
      rc = payload_reserve(s, p, extent, &take, !split, w->budget, error);
      if (take < n)
        split = 1;
      run_bytes = take;
    } else {
      rc = immutable_extent_allowance(s, p, run_bytes, w->budget, error);
      if (rc == LC_OK)
        rc = take_payload(s, w->budget, &run_bytes, &extent->offset, error);
      if (rc == LC_OK)
        ps_internal_budget_spend(w->budget, run_bytes, 0, 0);
    }
    if (rc == LC_OK) {
      if (run_bytes < take)
        take = (size_t)run_bytes;
      extent->bytes = take;
      extent->crc = checksum(w->buffer + done, take);
      ++p->count;
      p->payload_claimed = 1;
      p->bytes += take;
    }
    rc = allocation_end(s, guard, rc, error);
    pthread_mutex_unlock(&s->mutex);
    if (rc != LC_OK)
      return rc;
    /* No mutex is held during payload I/O or while callers produce input. */
    rc = transfer(s, w->buffer + done, take, extent->offset, 1, error);
    if (rc != LC_OK)
      return rc;
    done += take;
  }
  w->used = 0;
  return LC_OK;
}
int ps_write(ps_writer *w, const void *bytes, size_t count, lc_error *error) {
  const unsigned char *p = bytes;
  size_t n;
  int rc = LC_OK;
  if (!w || (!bytes && count) || w->failed || w->published)
    return err(error, LC_ERR_INVALID,
               "container writer is invalid, failed or published");
  if ((w->entry->key.kind == PS_ROUTING ||
       (w->entry->key.kind == PS_CONTROL && !w->budget)) &&
      count > PS_PAGE - w->entry->bytes - w->used)
    return err(error, LC_ERR_INVALID,
               "container bounded metadata exceeds one page");
  while (count) {
    n = PS_CHUNK - w->used;
    if (n > count)
      n = count;
    memcpy(w->buffer + w->used, p, n);
    w->used += n;
    count -= n;
    p += n;
    if (w->used == PS_CHUNK && (rc = flush_writer(w, error)) != LC_OK) {
      w->failed = 1;
      break;
    }
  }
  return rc;
}
int ps_publish(ps_writer *w, ps_ref *ref, lc_error *error) {
  ps_space *s;
  ps_entry *old;
  size_t prior;
  uint64_t sequence = 0;
  ps_ref published_ref;
  ps_coordination_guard *guard;
  int rc, durable;
  if (!w || w->failed || w->published)
    return err(error, LC_ERR_INVALID, "container writer cannot publish");
  rc = flush_writer(w, error);
  if (rc != LC_OK) {
    w->failed = 1;
    return rc;
  }
  s = w->space;
  durable = s->durable || w->entry->key.kind == PS_CONTROL ||
            w->entry->key.kind == PS_OWNED_RUN;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_admission_begin(s, &guard, error);
  old = lookup(s, w->entry->key);
  prior = old && !old->deleted ? entry_bytes(old) : 0;
  if (rc == LC_OK && w->entry->key.kind == PS_OWNED_RUN &&
      (!w->expected_owned || old != w->expected_owned || old->deleted ||
       !stream_lookup(s, old->key.owner) || !w->entry->owned_mapped ||
       w->entry->stream_capacity != old->stream_capacity))
    rc = err(error, LC_ERR_INVALID, "captured piece mapping changed");
  if (rc == LC_OK && old && !old->deleted &&
      w->entry->key.kind == PS_COLLECTION)
    rc = err(error, LC_ERR_INVALID, "collection position already published");
  if (rc == LC_OK)
    rc = immutable_allowance(
        s, w->entry, s->metadata - prior + entry_bytes(w->entry),
        s->live_entries + (old && !old->deleted ? 0 : 1), 0, w->budget, error);
  if (rc == LC_OK)
    rc = append_event(s, w->entry, PS_PUT, error);
  if (rc == LC_OK) {
    ps_internal_budget_spend(
        w->budget, 0,
        entry_bytes(w->entry) > prior ? entry_bytes(w->entry) - prior : 0,
        prior ? 0 : 1);
    w->entry->publication_sequence =
        __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED);
    w->entry->value_checked = 1;
    old = lookup(s, w->entry->key); /* rollover may have retired a tombstone */
    dirty_entry(s, w->entry, old);
    install(s, w->entry);
    w->published = 1;
    rc = entry_release_claims(s, w->entry, error);
    sequence = w->entry->publication_sequence;
    memcpy(published_ref.container, s->identity, 16);
    published_ref.incarnation = w->entry->token;
  } else
    w->failed = 1;
  rc = allocation_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK && durable)
    rc = wait_durable(s, sequence, error);
  if (rc == LC_OK && ref)
    *ref = published_ref;
  if (rc != LC_OK)
    w->failed = 1;
  return rc;
}
int ps_revision_writer_info(const ps_writer *w, ps_ref *ref, uint64_t *bytes,
                            lc_error *error) {
  if (!w || !ref || !bytes || w->failed || w->published)
    return err(error, LC_ERR_INVALID,
               "revision identity requires an unpublished writer");
  memcpy(ref->container, w->space->identity, sizeof(ref->container));
  ref->incarnation = w->entry->token;
  *bytes = w->entry->bytes + w->used;
  return LC_OK;
}
int ps_revision_publish(ps_writer *const *writers, size_t count,
                        int force_durable, enum ps_commit_mode mode,
                        ps_ref *refs, uint64_t *prefix, lc_error *error) {
  ps_space *s;
  ps_budget *budget;
  ps_coordination_guard *guard = NULL;
  ps_entry *p, *old;
  unsigned char *wire = NULL;
  size_t i, j, bytes = 64, cursor, prior, descriptor;
  uint64_t metadata, entries, growth = 0, added = 0, sequence = 0;
  int rc = LC_OK, durable, forced;
  if (prefix)
    *prefix = 0;
  if (!writers || !count || count > (PS_JOURNAL - 64) / 64 || !writers[0] ||
      (force_durable != 0 && force_durable != 1) ||
      (mode != PS_COMMIT_DEFAULT && mode != PS_COMMIT_DEFER_SYNC))
    return err(error, LC_ERR_INVALID, "revision requires immutable writers");
  s = writers[0]->space;
  budget = writers[0]->budget;
  for (i = 0; i < count; ++i) {
    if (!writers[i] || writers[i]->space != s || writers[i]->budget != budget ||
        writers[i]->failed || writers[i]->published)
      return err(error, LC_ERR_INVALID,
                 "revision writers must share space and budget");
    for (j = 0; j < i; ++j)
      if (same_key(writers[j]->entry->key, writers[i]->entry->key))
        return err(error, LC_ERR_INVALID, "revision keys must be distinct");
  }
  for (i = 0; rc == LC_OK && i < count; ++i) {
    rc = flush_writer(writers[i], error);
    descriptor = entry_bytes(writers[i]->entry);
    if (rc == LC_OK && descriptor > PS_JOURNAL - bytes)
      rc = err(error, LC_ERR_INVALID, "revision metadata exceeds journal");
    if (rc == LC_OK)
      bytes += descriptor;
  }
  if (rc == LC_OK) {
    wire = lc_alloc_with_allocator(NULL, bytes + (s->readiness ? 64 : 0));
    if (!wire)
      rc = err(error, LC_ERR_NOMEM, "cannot allocate revision publication");
  }
  if (rc != LC_OK)
    goto done;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_admission_begin(s, &guard, error);
  if (rc == LC_OK && (count > UINT64_MAX - s->sequence ||
                      count > UINT64_MAX - __atomic_load_n(&s->pending_sequence,
                                                           __ATOMIC_RELAXED)))
    rc = err(error, LC_ERR_INVALID, "revision publication sequence exhausted");
  if (rc == LC_OK && bytes > PS_JOURNAL - s->journal)
    rc = checkpoint_locked(s, error);
  metadata = s->metadata;
  entries = s->live_entries;
  durable = s->durable || force_durable;
  forced = force_durable;
  for (i = 0; rc == LC_OK && i < count; ++i) {
    p = writers[i]->entry;
    old = lookup(s, p->key);
    prior = old && !old->deleted ? entry_bytes(old) : 0;
    descriptor = entry_bytes(p);
    if (old && !old->deleted && p->key.kind == PS_COLLECTION) {
      rc = err(error, LC_ERR_INVALID, "collection position already published");
      break;
    }
    metadata = metadata - prior + descriptor;
    if (descriptor > prior)
      growth += descriptor - prior;
    if (!prior) {
      ++entries;
      ++added;
    }
    if (p->key.kind == PS_CONTROL)
      durable = forced = 1;
  }
  if (rc == LC_OK)
    rc = ps_internal_budget_check(s, budget, 0, growth, added, error);
  if (rc == LC_OK)
    rc = checkpoint_allowance_floor(s, metadata, entries, 0,
                                    PS_MAINTENANCE_RESERVE, budget, error);
  if (rc == LC_OK) {
    memset(wire, 0, 64);
    put32(wire, REVISION_MAGIC);
    put32(wire + 4, (uint32_t)bytes);
    put64(wire + 8, s->sequence + 1);
    put32(wire + 16, (uint32_t)count);
    put32(wire + 20, (uint32_t)durable);
    cursor = 64;
    for (i = 0; i < count; ++i) {
      encode_entry(wire + cursor, writers[i]->entry, s->sequence + 1 + i,
                   PS_PUT);
      cursor += entry_bytes(writers[i]->entry);
    }
    put32(wire + 60,
          (uint32_t)crc32(checksum(wire, 60), wire + 64, (uInt)(bytes - 64)));
    rc = write_journal_frame(s, wire, bytes, error);
  }
  if (rc == LC_OK) {
    s->sequence += count;
    s->journal += bytes;
    sequence = __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED);
    ps_internal_budget_spend(budget, 0, growth, added);
    for (i = 0; i < count; ++i) {
      p = writers[i]->entry;
      old = lookup(s, p->key);
      p->publication_sequence = ++sequence;
      p->durable_visibility = durable;
      p->value_checked = 1;
      dirty_entry(s, p, old);
      install(s, p);
      writers[i]->published = 1;
      if (refs) {
        memcpy(refs[i].container, s->identity, sizeof(refs[i].container));
        refs[i].incarnation = p->token;
      }
    }
    __atomic_store_n(&s->pending_sequence, sequence, __ATOMIC_RELEASE);
    rc = ps_readiness_publish(s, error);
    for (i = 0; i < count; ++i)
      if (entry_release_claims(s, writers[i]->entry, error) != LC_OK) {
        rc = healthy(s, error);
        break;
      }
  }
  rc = allocation_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK && durable && (forced || mode == PS_COMMIT_DEFAULT))
    rc = wait_durable(s, sequence, error);
  if (rc == LC_OK && prefix)
    *prefix = sequence;
done:
  if (rc != LC_OK)
    for (i = 0; i < count; ++i)
      writers[i]->failed = 1;
  lc_free_with_allocator(NULL, wire);
  return rc;
}
int ps_publication_prefix(ps_space *s, uint64_t *selected, uint64_t *durable,
                          lc_error *error) {
  int rc;
  if (!s || !selected || !durable)
    return err(error, LC_ERR_INVALID,
               "publication observation requires output");
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc == LC_OK) {
    *selected = __atomic_load_n(&s->pending_sequence, __ATOMIC_ACQUIRE);
    *durable = __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE);
  }
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
void ps_writer_close(ps_writer *w) {
  ps_space *s;
  if (!w)
    return;
  s = w->space;
  pthread_mutex_lock(&s->mutex);
  --s->writers;
  if (w->budget)
    --w->budget->users;
  if (!w->published) {
    /* Even an uncertain publication is left untouched on disk by a poisoned
     * handle. Its memory can be discarded; no further allocation is allowed. */
    if (!__atomic_load_n(&s->poisoned, __ATOMIC_RELAXED) &&
        release_entry_runs(s, w->entry, 0, NULL) != LC_OK)
      __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
    unregister_entry(w->entry);
    entry_free(w->entry);
  }
  pthread_mutex_unlock(&s->mutex);
  lc_free_with_allocator(NULL, w);
}
int ps_read_open(ps_space *s, ps_key key, const ps_ref *ref, ps_reader **out,
                 lc_error *error) {
  ps_reader *r;
  ps_coordination_guard *guard;
  ps_entry *p;
  int rc;
  if (out)
    *out = NULL;
  if (!s || !out || !immutable_key(key))
    return err(error, LC_ERR_INVALID,
               "container reader requires typed key and output");
  r = lc_calloc_with_allocator(NULL, 1, sizeof(*r));
  if (!r)
    return err(error, LC_ERR_NOMEM, "cannot allocate bounded reader");
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &guard, error);
  p = selected_value(s, lookup(s, key),
                     __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE));
  if (rc == LC_OK && (!p || p->deleted))
    rc = err(error, LC_ERR_INVALID, "container key not found");
  if (rc == LC_OK && ref &&
      (memcmp(ref->container, s->identity, 16) || ref->incarnation != p->token))
    rc = err(error, LC_ERR_INVALID, "stale or foreign container reference");
  if (rc == LC_OK)
    rc = ps_internal_entry_pin(s, p, error);
  if (rc == LC_OK)
    r->entry = p;
  rc = ps_internal_capture_end(s, guard, rc, error);
  if (rc == LC_OK) {
    ++s->readers;
    r->space = s;
    r->entry = p;
    *out = r;
  } else if (r->entry)
    ps_internal_entry_unpin(s, r->entry);
  pthread_mutex_unlock(&s->mutex);
  if (rc != LC_OK)
    lc_free_with_allocator(NULL, r);
  return rc;
}
static void reader_payload_invalid(ps_reader *r) {
  if (r->imported) {
    r->entry->value_checked = -1;
    return;
  }
  pthread_mutex_lock(&r->space->mutex);
  r->entry->value_checked = -1;
  pthread_mutex_unlock(&r->space->mutex);
}
int ps_read(ps_reader *r, void *buffer, size_t count, size_t *out,
            lc_error *error) {
  unsigned char *p = buffer;
  const ps_extent *extent;
  size_t n;
  int rc;
  if (out)
    *out = 0;
  if (!r || (!buffer && count) || !out)
    return err(error, LC_ERR_INVALID,
               "container read requires reader, buffer and byte count");
  if (r->random_loaded) {
    r->random_loaded = 0;
    r->loaded = 0;
  }
  while (count && r->extent < r->entry->count) {
    extent = &r->entry->extents[r->extent];
    if (!r->loaded) {
      rc = transfer(r->space, r->buffer, (size_t)extent->bytes, extent->offset,
                    0, error);
      if (rc != LC_OK)
        return rc;
      if (checksum(r->buffer, (size_t)extent->bytes) != extent->crc) {
        reader_payload_invalid(r);
        return err(error, LC_ERR_PROTOCOL,
                   "container read payload checksum mismatch");
      }
      r->loaded = (size_t)extent->bytes;
    }
    n = r->loaded - r->offset;
    if (n > count)
      n = count;
    memcpy(p, r->buffer + r->offset, n);
    *out += n;
    p += n;
    count -= n;
    r->offset += n;
    if (r->offset == r->loaded) {
      ++r->extent;
      r->offset = r->loaded = 0;
    }
  }
  return LC_OK;
}
int ps_read_at(ps_reader *r, uint64_t offset, void *buffer, size_t count,
               lc_error *error) {
  unsigned char *p = buffer;
  const ps_extent *extent;
  size_t within, take;
  int rc;
  if (!r || (!buffer && count) || offset > r->entry->bytes ||
      count > r->entry->bytes - offset)
    return err(error, LC_ERR_INVALID, "captured value read exceeds its range");
  if (offset < r->random_base) {
    r->random_extent = 0;
    r->random_base = 0;
    r->random_loaded = 0;
  }
  while (count) {
    extent = &r->entry->extents[r->random_extent];
    if (offset >= r->random_base + extent->bytes) {
      r->random_base += extent->bytes;
      ++r->random_extent;
      r->random_loaded = 0;
      continue;
    }
    if (!r->random_loaded) {
      r->loaded = 0;
      rc = transfer(r->space, r->buffer, (size_t)extent->bytes, extent->offset,
                    0, error);
      if (rc != LC_OK)
        return rc;
      if (checksum(r->buffer, (size_t)extent->bytes) != extent->crc) {
        reader_payload_invalid(r);
        return err(error, LC_ERR_PROTOCOL,
                   "captured value payload checksum mismatch");
      }
      r->random_loaded = 1;
    }
    within = (size_t)(offset - r->random_base);
    take = (size_t)extent->bytes - within;
    if (take > count)
      take = count;
    memcpy(p, r->buffer + within, take);
    offset += take;
    p += take;
    count -= take;
  }
  return LC_OK;
}
void ps_read_reference(const ps_reader *r, ps_ref *out) {
  memcpy(out->container, r->space->identity, sizeof(out->container));
  out->incarnation = r->entry->token;
}
static ps_entry *selected_value(ps_space *s, ps_entry *p, uint64_t prefix) {
  if (s->durable || (p && (p->key.kind == PS_CONTROL || p->durable_visibility)))
    while (p && (p->peer_sequence ? p->peer_sequence > s->peer_durable
                                  : p->publication_sequence > prefix))
      p = p->previous;
  return p && !p->deleted ? p : NULL;
}
int ps_internal_fixed_value(ps_space *s, ps_key key, uint64_t prefix,
                            void *buffer, size_t bytes, uint64_t known,
                            uint64_t *incarnation, int *current,
                            lc_error *error) {
  ps_entry *latest = lookup(s, key);
  ps_entry *p = selected_value(s, latest, prefix);
  unsigned char *dest = buffer;
  size_t i, left = bytes;
  int rc;
  *incarnation = 0;
  *current = p == latest;
  if (!p || !*current)
    return LC_OK;
  if (p->bytes != bytes)
    return err(error, LC_ERR_PROTOCOL, "invalid fixed control size");
  if (p->token == known) {
    *incarnation = known;
    return LC_OK;
  }
  for (i = 0; i < p->count; ++i) {
    const ps_extent *extent = p->extents + i;
    if (extent->bytes > left)
      return err(error, LC_ERR_PROTOCOL, "invalid fixed control extent");
    rc = transfer(s, dest, (size_t)extent->bytes, extent->offset, 0, error);
    if (rc != LC_OK)
      return rc;
    if (checksum(dest, (size_t)extent->bytes) != extent->crc)
      return err(error, LC_ERR_PROTOCOL, "fixed control checksum mismatch");
    dest += (size_t)extent->bytes;
    left -= (size_t)extent->bytes;
  }
  if (left)
    return err(error, LC_ERR_PROTOCOL, "incomplete fixed control");
  *incarnation = p->token;
  return LC_OK;
}
int ps_value_info(ps_space *s, ps_key key, ps_ref *ref, uint64_t *bytes,
                  int *present, lc_error *error) {
  ps_entry *p;
  int rc;
  if (!s || !ref || !bytes || !present || !immutable_key(key))
    return err(error, LC_ERR_INVALID,
               "value observation requires typed output");
  memset(ref, 0, sizeof(*ref));
  *bytes = 0;
  *present = 0;
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  p = selected_value(s, lookup(s, key),
                     __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE));
  if (rc == LC_OK && p) {
    memcpy(ref->container, s->identity, sizeof(ref->container));
    ref->incarnation = p->token;
    *bytes = p->bytes;
    *present = 1;
  }
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_value_validate(ps_space *s, ps_key key, const ps_ref *expected,
                      int *valid, lc_error *error) {
  ps_entry *p;
  ps_coordination_guard *guard;
  unsigned char *buffer = NULL;
  int rc, checked = 0;
  if (!s || !valid || !immutable_key(key))
    return err(error, LC_ERR_INVALID, "value validation requires typed output");
  *valid = 0;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &guard, error);
  p = selected_value(s, lookup(s, key),
                     __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE));
  if (rc != LC_OK || !p ||
      (expected && (memcmp(expected->container, s->identity, 16) ||
                    expected->incarnation != p->token)))
    p = NULL;
  if (p && p->value_checked) {
    *valid = p->value_checked > 0;
    p = NULL;
  }
  if (p) {
    rc = ps_internal_entry_pin(s, p, error);
    if (rc == LC_OK)
      ++s->readers;
    else
      p = NULL;
  }
  rc = ps_internal_capture_end(s, guard, rc, error);
  if (rc != LC_OK && p) {
    ps_internal_entry_unpin(s, p);
    --s->readers;
    p = NULL;
  }
  pthread_mutex_unlock(&s->mutex);
  if (!p)
    return rc;
  buffer = lc_alloc_with_allocator(NULL, PS_CHUNK);
  if (!buffer)
    rc = err(error, LC_ERR_NOMEM,
             "cannot allocate immutable validation scratch");
  else
    rc = verify_value_payloads(s, p, buffer, error);
  if (rc == LC_ERR_PROTOCOL) {
    lc_error_cleanup(error);
    checked = -1;
    rc = LC_OK;
  } else if (rc == LC_OK)
    checked = 1;
  pthread_mutex_lock(&s->mutex);
  if (rc == LC_OK) {
    if (checked < 0 || !p->value_checked)
      p->value_checked = checked;
    *valid = p->value_checked > 0;
  }
  ps_internal_entry_unpin(s, p);
  --s->readers;
  if (p->state == 2 && p->safe && !p->pins)
    collect(s);
  pthread_mutex_unlock(&s->mutex);
  lc_free_with_allocator(NULL, buffer);
  return rc;
}
int ps_values_capture(ps_space *s, uint64_t owner, unsigned int first,
                      unsigned int last, ps_values **out, lc_error *error) {
  ps_values *view;
  ps_coordination_guard *guard;
  ps_entry *p, *selected;
  uint64_t prefix;
  size_t i, n = 0, at = 0;
  int rc;
  if (out)
    *out = NULL;
  if (!s || !out || first > last)
    return err(error, LC_ERR_INVALID, "value visit requires owner and kinds");
  view = lc_calloc_with_allocator(NULL, 1, sizeof(*view));
  if (!view)
    return err(error, LC_ERR_NOMEM, "cannot allocate selected value capture");
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &guard, error);
  prefix = __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE);
  for (i = 0; rc == LC_OK && i < BUCKETS; ++i)
    for (p = s->buckets[i]; p; p = p->hash_next)
      if (p->key.owner == owner && p->key.kind >= first &&
          p->key.kind <= last && immutable_key(p->key) &&
          selected_value(s, p, prefix))
        ++n;
  if (rc == LC_OK && n) {
    if (n <= (size_t)-1 / sizeof(*view->items))
      view->items = lc_calloc_with_allocator(NULL, n, sizeof(*view->items));
    if (!view->items)
      rc = err(error, LC_ERR_NOMEM, "cannot capture selected value metadata");
  }
  for (i = 0; rc == LC_OK && i < BUCKETS; ++i)
    for (p = s->buckets[i]; p; p = p->hash_next)
      if (p->key.owner == owner && p->key.kind >= first &&
          p->key.kind <= last && immutable_key(p->key) &&
          (selected = selected_value(s, p, prefix)) != NULL) {
        rc = ps_internal_entry_pin(s, selected, error);
        if (rc == LC_OK)
          view->items[at++] = selected;
        else
          break;
      }
  rc = ps_internal_capture_end(s, guard, rc, error);
  if (rc == LC_OK) {
    view->space = s;
    view->count = at;
    ++s->readers;
  } else
    while (at)
      ps_internal_entry_unpin(s, view->items[--at]);
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK)
    *out = view;
  else {
    lc_free_with_allocator(NULL, view->items);
    lc_free_with_allocator(NULL, view);
  }
  return rc;
}
size_t ps_values_count(const ps_values *view) { return view ? view->count : 0; }
int ps_values_info(const ps_values *view, size_t at, ps_key *key, ps_ref *ref,
                   uint64_t *bytes, lc_error *error) {
  ps_entry *p;
  if (!view || at >= view->count || !key || !ref || !bytes)
    return err(error, LC_ERR_INVALID,
               "captured value requires index and output");
  p = view->items[at];
  *key = p->key;
  memcpy(ref->container, view->space->identity, sizeof(ref->container));
  ref->incarnation = p->token;
  *bytes = p->bytes;
  return LC_OK;
}
int ps_values_read_open(const ps_values *view, size_t at, ps_reader **out,
                        lc_error *error) {
  ps_reader *reader;
  ps_space *s;
  int rc;
  if (out)
    *out = NULL;
  if (!view || at >= view->count || !out)
    return err(error, LC_ERR_INVALID,
               "captured reader requires index and output");
  reader = lc_calloc_with_allocator(NULL, 1, sizeof(*reader));
  if (!reader)
    return err(error, LC_ERR_NOMEM, "cannot allocate captured value reader");
  s = view->space;
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc == LC_OK) {
    reader->space = s;
    reader->entry = view->items[at];
    ++reader->entry->pins;
    ++s->readers;
  }
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK)
    *out = reader;
  else
    lc_free_with_allocator(NULL, reader);
  return rc;
}
void ps_values_close(ps_values *view) {
  ps_space *s;
  size_t i;
  if (!view)
    return;
  s = view->space;
  pthread_mutex_lock(&s->mutex);
  for (i = 0; i < view->count; ++i)
    ps_internal_entry_unpin(s, view->items[i]);
  --s->readers;
  collect(s);
  pthread_mutex_unlock(&s->mutex);
  lc_free_with_allocator(NULL, view->items);
  lc_free_with_allocator(NULL, view);
}
int ps_values_visit(ps_space *s, uint64_t owner, unsigned int first,
                    unsigned int last, ps_value_visit_fn visit, void *context,
                    lc_error *error) {
  ps_values *view = NULL;
  ps_ref ref;
  ps_key key;
  uint64_t bytes;
  size_t i;
  int rc;
  if (!visit)
    return err(error, LC_ERR_INVALID, "value visit requires callback");
  rc = ps_values_capture(s, owner, first, last, &view, error);
  for (i = 0; rc == LC_OK && i < ps_values_count(view); ++i) {
    rc = ps_values_info(view, i, &key, &ref, &bytes, error);
    if (rc == LC_OK)
      rc = visit(context, key, &ref, bytes, error);
  }
  ps_values_close(view);
  return rc;
}
uint64_t ps_read_size(const ps_reader *r) { return r ? r->entry->bytes : 0; }
void ps_reader_close(ps_reader *r) {
  ps_space *s;
  if (!r)
    return;
  s = r->space;
  if (r->imported) {
    entry_free(r->entry);
    destroy(s);
    lc_free_with_allocator(NULL, r);
    return;
  }
  pthread_mutex_lock(&s->mutex);
  ps_internal_entry_unpin(s, r->entry);
  --s->readers;
  if (r->entry->state == 2 && r->entry->safe && !r->entry->pins)
    collect(s);
  pthread_mutex_unlock(&s->mutex);
  lc_free_with_allocator(NULL, r);
}
size_t ps_reader_ticket_bytes(const ps_reader *r) {
  if (!r || r->entry->count > (PS_JOURNAL - 64U) / 24U)
    return 0;
  return TICKET_HEADER + r->entry->count * 24U;
}
static uint32_t ticket_checksum(const unsigned char *wire, size_t bytes) {
  static const unsigned char zero[4] = {0};
  uLong crc = crc32(0, wire, 96);
  crc = crc32(crc, zero, sizeof(zero));
  return (uint32_t)crc32(crc, wire + 100, (uInt)(bytes - 100));
}
int ps_reader_ticket(ps_reader *r, void *buffer, size_t bytes,
                     lc_error *error) {
  unsigned char *wire = buffer;
  ps_space *s;
  ps_entry *p;
  struct stat st;
  uint64_t capacity;
  size_t i, at;
  int rc;
  if (!r || !buffer || bytes != ps_reader_ticket_bytes(r) ||
      bytes < TICKET_HEADER)
    return err(error, LC_ERR_INVALID, "captured reader ticket size mismatch");
  s = r->space;
  p = r->entry;
  if (fstat(s->fd, &st) != 0)
    return err(error, LC_ERR_TRANSPORT, "cannot identify captured reader file");
  if (!r->imported)
    pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc == LC_OK)
    s->range_pins = 1;
  capacity = s->capacity;
  if (!r->imported)
    pthread_mutex_unlock(&s->mutex);
  if (rc != LC_OK)
    return rc;
  memset(wire, 0, bytes);
  put64(wire, TICKET_MAGIC);
  put64(wire + 8, (uint64_t)st.st_dev);
  put64(wire + 16, (uint64_t)st.st_ino);
  put64(wire + 24, capacity);
  memcpy(wire + 32, s->identity, 16);
  put64(wire + 48, p->token);
  put64(wire + 56, p->bytes);
  put64(wire + 64, p->count);
  put32(wire + 72, p->key.kind);
  put64(wire + 80, p->key.owner);
  put64(wire + 88, p->key.key);
  for (i = 0, at = TICKET_HEADER; i < p->count; ++i, at += 24) {
    put64(wire + at, p->extents[i].offset);
    put64(wire + at + 8, p->extents[i].bytes);
    put32(wire + at + 16, p->extents[i].crc);
    put32(wire + at + 20, p->extents[i].flags);
  }
  put32(wire + 96, ticket_checksum(wire, bytes));
  return LC_OK;
}
static int import_pin(int fd, uint64_t offset, uint64_t bytes,
                      lc_error *error) {
  struct flock lock;
  int rc;
  memset(&lock, 0, sizeof(lock));
  lock.l_type = F_RDLCK;
  lock.l_whence = SEEK_SET;
  lock.l_start = (off_t)offset;
  lock.l_len = (off_t)bytes;
  do {
    rc = fcntl(fd, F_OFD_SETLK, &lock);
  } while (rc < 0 && errno == EINTR);
  return rc == 0 ? LC_OK
                 : err(error,
                       errno == EACCES || errno == EAGAIN ? LC_ERR_PENDING
                                                          : LC_ERR_TRANSPORT,
                       "cannot pin captured reader range");
}
int ps_reader_import(const char *path, const void *buffer, size_t bytes,
                     ps_reader **out, lc_error *error) {
  const unsigned char *wire = buffer;
  ps_space *s = NULL;
  ps_entry *p = NULL;
  ps_reader *r = NULL;
  struct stat st;
  uint64_t count, total = 0;
  size_t i, at;
  int rc = LC_OK;
  if (out)
    *out = NULL;
  if (!path || !*path || !out || !wire || bytes < TICKET_HEADER ||
      bytes > TICKET_HEADER + PS_JOURNAL || get64(wire) != TICKET_MAGIC ||
      get32(wire + 96) != ticket_checksum(wire, bytes))
    return err(error, LC_ERR_INVALID, "invalid captured reader ticket");
  count = get64(wire + 64);
  if (count > (PS_JOURNAL - 64U) / 24U ||
      bytes != TICKET_HEADER + count * 24U || !get64(wire + 48) ||
      get64(wire + 24) < PS_MIB || get64(wire + 24) > INT64_MAX ||
      get64(wire + 24) % PS_MIB || get32(wire + 76) || get32(wire + 100) ||
      get64(wire + 104))
    return err(error, LC_ERR_INVALID, "invalid captured reader metadata");
  s = lc_calloc_with_allocator(NULL, 1, sizeof(*s));
  if (s)
    s->fd = -1;
  p = lc_calloc_with_allocator(NULL, 1, sizeof(*p));
  r = lc_calloc_with_allocator(NULL, 1, sizeof(*r));
  if (!s || !p || !r) {
    rc = err(error, LC_ERR_NOMEM, "cannot allocate imported captured reader");
    goto done;
  }
  s->io.read = native_read;
  s->capacity = get64(wire + 24);
  s->range_pins = 1;
  memcpy(s->identity, wire + 32, 16);
  p->key.owner = get64(wire + 80);
  p->key.key = get64(wire + 88);
  p->key.kind = get32(wire + 72);
  p->token = get64(wire + 48);
  p->bytes = get64(wire + 56);
  if (!immutable_key(p->key)) {
    rc = err(error, LC_ERR_INVALID, "captured ticket requires immutable value");
    goto done;
  }
  if (count) {
    p->extents =
        lc_calloc_with_allocator(NULL, (size_t)count, sizeof(*p->extents));
    if (!p->extents) {
      rc = err(error, LC_ERR_NOMEM, "cannot allocate captured reader extents");
      goto done;
    }
  }
  p->count = p->capacity = (size_t)count;
  for (i = 0, at = TICKET_HEADER; i < p->count; ++i, at += 24) {
    ps_extent *x = &p->extents[i];
    x->offset = get64(wire + at);
    x->bytes = get64(wire + at + 8);
    x->crc = get32(wire + at + 16);
    x->flags = get32(wire + at + 20);
    if (!x->bytes || x->bytes > PS_CHUNK || x->offset < PS_DATA ||
        x->offset > s->capacity || x->bytes > s->capacity - x->offset ||
        (x->flags != 0 && x->flags != EXTENT_PACKED) ||
        (x->flags
             ? x->offset % PS_CELL || x->bytes > PS_PAGE - x->offset % PS_PAGE
             : x->offset % PS_PAGE) ||
        total > UINT64_MAX - x->bytes) {
      rc = err(error, LC_ERR_INVALID, "captured ticket extent exceeds bounds");
      goto done;
    }
    total += x->bytes;
  }
  if (total != p->bytes) {
    rc = err(error, LC_ERR_INVALID, "captured ticket byte count mismatch");
    goto done;
  }
  s->fd = open(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
  if (s->fd < 0 || fstat(s->fd, &st) != 0) {
    rc = err(error, LC_ERR_TRANSPORT, "cannot open captured reader file");
    goto done;
  }
  if (!S_ISREG(st.st_mode) || st.st_size < (off_t)s->capacity ||
      (uint64_t)st.st_dev != get64(wire + 8) ||
      (uint64_t)st.st_ino != get64(wire + 16)) {
    rc = err(error, LC_ERR_INVALID, "captured ticket belongs to another file");
    goto done;
  }
  rc = import_pin(s->fd, PIN_PRESENCE_OFFSET, 1, error);
  for (i = 0; rc == LC_OK && i < p->count; ++i)
    rc = import_pin(s->fd, p->extents[i].offset, p->extents[i].bytes, error);
  if (rc == LC_OK) {
    r->space = s;
    r->entry = p;
    r->imported = 1;
    *out = r;
    return LC_OK;
  }
done:
  if (s)
    destroy(s);
  if (p)
    entry_free(p);
  lc_free_with_allocator(NULL, r);
  return rc;
}
static int delete_value(ps_space *s, ps_key key, const ps_ref *expected,
                        int missing_ok, lc_error *error) {
  ps_entry *p, *tombstone;
  ps_coordination_guard *guard;
  int rc, matched;
  uint64_t sequence = 0;
  if (!s || !immutable_key(key))
    return err(error, LC_ERR_INVALID, "container delete requires typed key");
  tombstone = lc_calloc_with_allocator(NULL, 1, sizeof(*tombstone));
  if (!tombstone)
    return err(error, LC_ERR_NOMEM, "cannot allocate pending retirement");
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_admission_begin(s, &guard, error);
  p = lookup(s, key);
  matched = p && !p->deleted &&
            (!expected || (!memcmp(expected->container, s->identity, 16) &&
                           expected->incarnation == p->token));
  if (rc == LC_OK && !matched && !missing_ok)
    rc = err(error, LC_ERR_INVALID, "container key not found");
  if (rc == LC_OK && matched)
    rc = append_event(s, p, PS_REMOVE, error);
  if (rc == LC_OK && matched) {
    tombstone->key = key;
    tombstone->token = p->token;
    tombstone->deleted = 1;
    tombstone->publication_sequence =
        __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED);
    register_entry(s, tombstone);
    dirty_entry(s, tombstone, p);
    install(s, tombstone);
    sequence = tombstone->publication_sequence;
  } else
    entry_free(tombstone);
  rc = allocation_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK && matched && (s->durable || key.kind == PS_CONTROL))
    rc = wait_durable(s, sequence, error);
  return rc;
}
int ps_delete(ps_space *s, ps_key key, lc_error *error) {
  return delete_value(s, key, NULL, 0, error);
}
int ps_value_remove(ps_space *s, ps_key key, const ps_ref *expected,
                    lc_error *error) {
  return delete_value(s, key, expected, 1, error);
}
int ps_flush(ps_space *s, lc_error *error) {
  int rc;
  uint64_t sequence;
  if (!s)
    return err(error, LC_ERR_INVALID, "flush requires container");
  rc = healthy(s, error);
  if (rc != LC_OK)
    return rc;
  sequence = __atomic_load_n(&s->pending_sequence, __ATOMIC_ACQUIRE);
  return wait_durable(s, sequence, error);
}
int ps_checkpoint(ps_space *s, lc_error *error) {
  uint64_t bound;
  int rc;
  if (!s)
    return err(error, LC_ERR_INVALID, "checkpoint requires container");
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc == LC_OK)
    rc = metadata_claimed_bound(s, s->metadata, s->live_entries, &bound, error);
  if (rc == LC_OK)
    s->metadata_workspace = bound;
  if (rc == LC_OK) {
    /* Namespace maintenance shares one physical checkpoint. A completed root
     * needs no new root selection merely because another namespace visits it.
     * Keep unpublished preparation outside this boundary; completed readiness
     * debt and dynamic metadata trimming still require the normal protocol.
     * Refresh the bound first: an aborted private producer can leave excess
     * workspace even though it never published a journal record.
     * Growth and explicit repacking call checkpoint_locked directly. */
    if (s->journal || s->dirty ||
        __atomic_load_n(&s->pending_sequence, __ATOMIC_ACQUIRE) !=
            __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE) ||
        metadata_trim_needed(s))
      rc = checkpoint_locked(s, error);
    else
      collect_retired(s, 1);
  }
  if (rc == LC_OK)
    rc = healthy(s, error);
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_metadata_repack(ps_space *s, lc_error *error) {
  int rc;
  if (!s)
    return err(error, LC_ERR_INVALID, "metadata repack requires container");
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc == LC_OK) {
    /* Dirty ownership already carries each old checkpoint reference through
     * replacement/tombstone chains. Promote only current live entries; the
     * normal checkpoint then retires every old descriptor reference exactly
     * once, after the new root and old-root retirement are durable. */
    metadata_dirty_all(s);
    rc = checkpoint_locked(s, error);
  }
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
/* Healthy checkpoint abort may have returned growth pages to the protected
 * metadata pool. Move those free pages back before excluding the new range.
 * Sorted, disjoint intervals permit at most one split at the old boundary. */
static ps_run *metadata_ungrow(ps_space *s, uint64_t capacity, ps_run *spare) {
  ps_run **link = &s->metadata_free, *node;
  while (*link) {
    node = *link;
    if (node->offset + node->bytes <= capacity) {
      link = &node->next;
      continue;
    }
    if (node->offset < capacity) {
      spare->offset = capacity;
      spare->bytes = node->offset + node->bytes - capacity;
      spare->clean = node->clean;
      node->bytes -= spare->bytes;
      s->metadata_free_bytes -= spare->bytes;
      free_insert(s, spare);
      spare = NULL;
      link = &node->next;
    } else {
      *link = node->next;
      s->metadata_free_bytes -= node->bytes;
      free_insert(s, node);
    }
  }
  return spare;
}
int ps_grow(ps_space *s, uint64_t bytes, lc_error *error) {
  uint64_t capacity;
  ps_run *run, *rollback, *trim;
  int rc;
  if (!s || !bytes || bytes % PS_MIB)
    return err(error, LC_ERR_INVALID,
               "growth requires positive whole MiB increment");
  run = lc_calloc_with_allocator(NULL, 1, sizeof(*run));
  if (!run)
    return err(error, LC_ERR_NOMEM, "cannot allocate growth interval");
  rollback = lc_calloc_with_allocator(NULL, 1, sizeof(*rollback));
  if (!rollback) {
    lc_free_with_allocator(NULL, run);
    return err(error, LC_ERR_NOMEM, "cannot allocate growth rollback interval");
  }
  trim = lc_calloc_with_allocator(NULL, 1, sizeof(*trim));
  if (!trim) {
    lc_free_with_allocator(NULL, rollback);
    lc_free_with_allocator(NULL, run);
    return err(error, LC_ERR_NOMEM,
               "cannot allocate growth rollback trim interval");
  }
  pthread_mutex_lock(&s->mutex);
  capacity = s->capacity;
  rc = healthy(s, error);
  if (rc == LC_OK && (bytes > (uint64_t)INT64_MAX - capacity ||
                      (s->maximum && bytes > s->maximum - capacity)))
    rc =
        err(error, LC_ERR_INVALID, "container growth exceeds maximum capacity");
  if (rc == LC_OK)
    rc = reserve_file(s, capacity, bytes, error);
  if (rc == LC_OK) {
    s->capacity += bytes;
    /* These pages remain private under the allocator mutex until the new
     * root commits capacity and metadata together. A full old space must not
     * prevent growth from funding the checkpoint which publishes that growth.
     * Trailing bytes are uncertified even when fallocate found an old range. */
    run->offset = capacity;
    run->bytes = bytes;
    run->clean = 0;
    free_insert(s, run);
    run = NULL;
    rc = checkpoint_locked(s, error);
    if (rc != LC_OK) {
      /* Healthy failure aborted all private checkpoint pages. Return excess
       * workspace and remove the new range; committed pages stay owned.
       * Uncertain I/O retains both ownership sets until reopen/reconciliation.
       */
      if (!__atomic_load_n(&s->poisoned, __ATOMIC_RELAXED)) {
        rollback = metadata_ungrow(s, capacity, rollback);
        trim = metadata_trim(s, trim);
        if (exclude_run(s, capacity, bytes, NULL) != LC_OK) {
          __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
          rc = err(error, LC_ERR_PROTOCOL,
                   "cannot roll back private growth range");
        }
      }
      s->capacity = capacity;
    }
  }
  lc_free_with_allocator(NULL, run);
  lc_free_with_allocator(NULL, rollback);
  lc_free_with_allocator(NULL, trim);
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_check(ps_space *s, lc_error *error) {
  if (!s)
    return err(error, LC_ERR_INVALID, "container admission requires space");
  return healthy(s, error);
}
int ps_status(ps_space *s, ps_stats *out, lc_error *error) {
  ps_entry *p;
  ps_budget_plan reserve;
  uint64_t n;
  size_t i;
  if (!s || !out)
    return err(error, LC_ERR_INVALID, "container status requires output");
  pthread_mutex_lock(&s->mutex);
  memset(out, 0, sizeof(*out));
  out->capacity = s->capacity;
  out->metadata_bytes = s->metadata;
  out->metadata_pages = s->metadata_pages;
  out->metadata_available_bytes = s->metadata_free_bytes;
  out->metadata_workspace_bytes = s->metadata_workspace;
  out->metadata_fallbacks = s->metadata_fallbacks;
  out->reserved_payload_bytes = s->reserved_payload;
  out->claimed_payload_bytes =
      s->physical_claim_bytes - s->metadata_claim_bytes - s->reader_claim_bytes;
  out->claimed_metadata_bytes = s->metadata_claim_bytes;
  out->retained_payload_bytes = s->reader_claim_bytes;
  out->reserved_metadata_bytes = s->reserved_metadata;
  out->reserved_entries = s->reserved_entries;
  out->budgets = s->budgets;
  ps_internal_reserve_plan(s, &reserve);
  out->reserve_payload_bytes = reserve.payload_bytes;
  out->reserve_metadata_bytes = reserve.metadata_bytes;
  out->reserve_entries = reserve.entries;
  out->reserve_owners = s->reserve_count;
  out->directory_page_writes = s->directory_page_writes;
  out->descriptor_page_writes = s->descriptor_page_writes;
  out->packed_pages = s->packed_pages;
  out->packed_bytes = s->packed_bytes;
  out->allocated_bytes =
      s->packed_pages * (uint64_t)PS_PAGE + s->pinned_free_bytes;
  out->retired_bytes = s->pinned_free_bytes;
  out->journal_bytes = s->journal;
  out->syncs = __atomic_load_n(&s->syncs, __ATOMIC_RELAXED);
  out->checkpoints = s->checkpoints;
  pthread_mutex_lock(&s->commit_mutex);
  out->sync_requests = s->sync_requests;
  out->sync_batches = s->sync_batches;
  out->max_sync_batch = s->max_sync_batch;
  pthread_mutex_unlock(&s->commit_mutex);
  for (p = s->entries; p; p = p->all_next) {
    n = 0;
    for (i = 0; i < p->count; ++i) {
      n += extent_allocation(&p->extents[i]);
      if (!(p->extents[i].flags & EXTENT_PACKED))
        out->allocated_bytes += extent_allocation(&p->extents[i]);
    }
    if (p->state == 1 && !p->deleted) {
      ++out->entries;
      if (p->key.kind != PS_OWNED_RUN)
        out->live_bytes += p->bytes;
    } else if (p->state == 2 || p->deleted)
      out->retired_bytes += n;
    else
      out->private_bytes += n;
  }
  pthread_mutex_unlock(&s->mutex);
  return LC_OK;
}

static void owned_status(ps_owned_head *head, ps_owned_status *out) {
  memcpy(out->ref.container, head->space->identity, 16);
  out->ref.incarnation = head->entry->token;
  out->tail = head->entry->bytes;
  out->capacity = head->entry->stream_capacity;
  out->runs = head->entry->stream_runs;
  out->stored_bytes = head->entry->stream_storage;
  out->edit_wire = head->entry->stream_edit_wire;
  out->sealed = head->entry->stream_sealed;
  out->retired = head->entry->deleted;
  out->complete =
      head->entry->stream_complete_generation &&
      head->entry->stream_complete_generation == head->space->generation;
}
int ps_internal_stream_claim(ps_space *s, uint64_t token,
                             ps_coordination_guard **claim, lc_error *e) {
  *claim = NULL;
  if (!ps_coordination_shared(s->coordination))
    return LC_OK;
  return ps_coordination_lock(s->coordination, PS_COORDINATION_STREAM_CLAIM,
                              token, 0, claim, e);
}
static int prepare_new_stream(ps_space *s, ps_entry *p, lc_error *e) {
  uint64_t token = s->token_next;
  size_t i;
  int rc;
  if (!ps_coordination_shared(s->coordination)) {
    p->token = s->token_next++;
    return LC_OK;
  }
  for (i = 0; i < PS_COORDINATION_STREAM_CLAIM_SLOTS && token <= s->token_end;
       ++i, ++token) {
    rc = ps_coordination_stream_prepare(s->coordination, token,
                                        &p->stream_claim, e);
    if (rc == LC_OK) {
      p->token = token;
      s->token_next = token + 1;
      return LC_OK;
    }
    if (rc != LC_ERR_PENDING)
      return rc;
    lc_error_cleanup(e);
  }
  return err(e, LC_ERR_PENDING, "no stream publication claim available");
}
int ps_internal_complete_tail(ps_space *s, ps_key key, const ps_ref *ref,
                              uint64_t *tail) {
  ps_owned_head head;
  ps_owned_status status;
  head.space = s;
  head.entry = lookup(s, key);
  if (!head.entry || head.entry->token != ref->incarnation ||
      memcmp(s->identity, ref->container, 16))
    return 0;
  owned_status(&head, &status);
  if (status.retired || status.sealed ||
      (!status.complete && (status.capacity || status.tail)))
    return 0;
  *tail = status.tail;
  return 1;
}
static int owned_open(ps_space *s, ps_budget *budget, ps_key key, int create,
                      const ps_ref *ref, int guarded, int prepare,
                      ps_owned_head **out, ps_owned_status *status,
                      lc_error *error) {
  ps_owned_head *head;
  ps_coordination_guard *guard;
  ps_entry *p = NULL, *old;
  uint64_t sequence = 0;
  int rc;
  if (out)
    *out = NULL;
  if (!s || !out || !status || key.kind != PS_STREAM || (create && ref))
    return err(error, LC_ERR_INVALID, "owned stream open arguments invalid");
  guarded = guarded && s->readiness;
  head = lc_calloc_with_allocator(NULL, 1, sizeof(*head));
  if (!head)
    return err(error, LC_ERR_NOMEM, "cannot allocate owned stream handle");
  if (create) {
    p = lc_calloc_with_allocator(NULL, 1, sizeof(*p));
    if (!p) {
      lc_free_with_allocator(NULL, head);
      return err(error, LC_ERR_NOMEM, "cannot allocate owned stream head");
    }
    p->key = key;
  }
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &guard, error);
  old = lookup(s, key);
  if (rc == LC_OK && create && old &&
      (!old->deleted ||
       old->publication_sequence >
           __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE)))
    rc = err(error, LC_ERR_INVALID, "owned stream already exists");
  if (rc == LC_OK && create && s->token_next > s->token_end)
    rc = err(error, LC_ERR_INVALID, "incarnation grant consumed; reopen");
  if (rc == LC_OK && create)
    rc = checkpoint_allowance_floor(s, s->metadata + 80, s->live_entries + 1, 0,
                                    PS_MAINTENANCE_RESERVE, budget, error);
  if (rc == LC_OK && create)
    rc = prepare_new_stream(s, p, error);
  if (rc == LC_OK && create) {
    rc = append_event(s, p, PS_PUT, error);
    if (rc == LC_OK) {
      ps_internal_budget_spend(budget, 0, 80, 1);
      /* Journal rollover may checkpoint and collect the prior tombstone. */
      old = lookup(s, key);
      sequence = __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED);
      p->publication_sequence = sequence;
      register_entry(s, p);
      dirty_entry(s, p, old);
      install(s, p);
    }
  }
  if (!create)
    p = lookup(s, key);
  if (rc == LC_OK && (!p || p->deleted ||
                      (p->stream_handles && (!guarded || !p->stream_guarded ||
                                             (prepare && !p->catalog_refs)))))
    rc = err(error, LC_ERR_INVALID,
             "owned stream missing or already has an open owner");
  if (rc == LC_OK && ref &&
      (memcmp(ref->container, s->identity, 16) || ref->incarnation != p->token))
    rc = err(error, LC_ERR_INVALID, "stale or foreign owned stream reference");
  if (rc == LC_OK && prepare && !p->catalog_refs && !p->stream_claim)
    rc = ps_internal_stream_claim(s, p->token, &p->stream_claim, error);
  if (rc == LC_OK) {
    head->space = s;
    head->entry = p;
    p->stream_guarded = guarded;
    ++p->stream_handles;
    ++p->pins; /* Stream heads own no payload extents. */
    ++s->readers;
    owned_status(head, status);
  } else if (create && p->state == 0) {
    ps_coordination_unlock(p->stream_claim);
    entry_free(p);
  }
  rc = ps_internal_capture_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK && create && s->durable)
    rc = wait_durable(s, sequence, error);
  if (rc == LC_OK)
    *out = head;
  else if (head->entry)
    ps_owned_close(head);
  else
    lc_free_with_allocator(NULL, head);
  return rc;
}
int ps_owned_open(ps_space *s, ps_budget *budget, ps_key key, int create,
                  const ps_ref *ref, ps_owned_head **out,
                  ps_owned_status *status, lc_error *error) {
  return owned_open(s, budget, key, create, ref, 0, 0, out, status, error);
}
int ps_owned_open_guarded(ps_space *s, ps_budget *budget, ps_key key,
                          int create, const ps_ref *ref, ps_owned_head **out,
                          ps_owned_status *status, lc_error *error) {
  return owned_open(s, budget, key, create, ref, 1, 0, out, status, error);
}
int ps_owned_open_prepared(ps_space *s, ps_key key, ps_owned_head **out,
                           ps_owned_status *status, lc_error *error) {
  int shared = ps_coordination_shared(ps_space_coordination(s));
  return owned_open(s, NULL, key, 0, NULL, shared, 1, out, status, error);
}
void ps_owned_close(ps_owned_head *head) {
  ps_space *s;
  if (!head)
    return;
  s = head->space;
  pthread_mutex_lock(&s->mutex);
  if (!--head->entry->stream_handles) {
    head->entry->orphan_claimed = 0;
    ps_coordination_unlock(head->entry->stream_claim);
    head->entry->stream_claim = NULL;
  }
  ps_internal_entry_unpin(s, head->entry);
  --s->readers;
  if (head->entry->state == 2 && head->entry->safe && !head->entry->pins)
    collect(s);
  pthread_mutex_unlock(&s->mutex);
  lc_free_with_allocator(NULL, head);
}
int ps_owned_status_get(ps_owned_head *head, ps_owned_status *out,
                        lc_error *error) {
  int rc;
  if (!head || !out)
    return err(error, LC_ERR_INVALID, "owned status requires head and output");
  pthread_mutex_lock(&head->space->mutex);
  rc = healthy(head->space, error);
  if (rc == LC_OK)
    owned_status(head, out);
  pthread_mutex_unlock(&head->space->mutex);
  return rc;
}
int ps_owned_status_capture(ps_owned_head *head, ps_owned_status *out,
                            lc_error *error) {
  ps_space *s = head->space;
  ps_coordination_guard *guard;
  int rc;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &guard, error);
  if (rc == LC_OK) {
    owned_status(head, out);
    /* Local removal already fences mutations and name reuse. Read windows
     * can still retain this handle's mappings until retirement is durable. */
    if (out->retired &&
        head->entry->publication_sequence >
            __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE))
      out->retired = 0;
  }
  rc = allocation_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_owned_edit_end(ps_owned_head *head, ps_coordination_guard *guard, int rc,
                      lc_error *error) {
  if (guard) {
    ps_coordination_unlock(guard);
    if (__atomic_load_n(&head->space->poisoned, __ATOMIC_RELAXED)) {
      lc_error_cleanup(error);
      return healthy(head->space, error);
    }
  }
  return rc;
}
static int claim_writer_stamp(ps_space *s, ps_entry *p, int event,
                              int *continued, lc_error *error) {
  ps_entry stamp;
  int rc;
  *continued =
      p->writer_generation == s->generation && p->writer_stamp == s->token_end;
  if (*continued && event != PS_STREAM_EDIT)
    return LC_OK;
  memset(&stamp, 0, sizeof(stamp));
  stamp.key = p->key;
  stamp.token = p->token;
  stamp.bytes = s->token_end;
  rc = append_event_locked(s, &stamp, event, error);
  if (rc == LC_OK) {
    p->writer_stamp = s->token_end;
    p->writer_generation = s->generation;
    if (event == PS_STREAM_EDIT) {
      p->stream_edit_wire = s->sequence;
      p->stream_complete_generation = 0;
    }
  }
  return rc;
}
int ps_internal_namespace_stamp(ps_space *s, uint64_t identity, int *continued,
                                lc_error *error) {
  ps_key key = {0, 0, PS_CATALOG};
  ps_entry *p;
  ps_coordination_guard *allocator;
  int rc;
  key.key = identity;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &allocator, error);
  if (rc == LC_OK) {
    p = lookup(s, key);
    if (!p || p->deleted || !p->catalog ||
        p->catalog->kind != PS_CATALOG_NAMESPACE)
      rc = err(error, LC_ERR_PROTOCOL, "namespace writer has no catalog owner");
    else
      rc = claim_writer_stamp(s, p, PS_NAMESPACE_OWNER, continued, error);
  }
  rc = allocation_end(s, allocator, rc, error);
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_owned_edit_begin(ps_owned_head *head, int append,
                        ps_coordination_guard **edit, ps_owned_status *status,
                        int *continued, lc_error *error) {
  ps_space *s = head->space;
  ps_entry *p = head->entry;
  ps_coordination_guard *allocator = NULL;
  int rc;
  *edit = NULL;
  *continued = 1;
  if (!s->readiness)
    return ps_owned_status_get(head, status, error);
  rc = ps_coordination_lock(s->coordination, PS_COORDINATION_STREAM, p->token,
                            0, edit, error);
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &allocator, error);
  if (rc == LC_OK) {
    owned_status(head, status);
    if (!p->deleted && !p->stream_sealed)
      rc = claim_writer_stamp(s, p, append ? PS_STREAM_EDIT : PS_STREAM_OWNER,
                              continued, error);
    else
      *continued = 0;
  }
  rc = allocation_end(s, allocator, rc, error);
  pthread_mutex_unlock(&s->mutex);
  if (rc != LC_OK) {
    rc = ps_owned_edit_end(head, *edit, rc, error);
    *edit = NULL;
  }
  return rc;
}
int ps_owned_mark_edit(ps_owned_head *head, lc_error *error) {
  ps_space *s = head->space;
  ps_coordination_guard *guard;
  int continued, rc;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &guard, error);
  if (rc == LC_OK && (head->entry->deleted || head->entry->stream_sealed))
    rc = err(error, LC_ERR_INVALID, "owned stream is retired or sealed");
  if (rc == LC_OK)
    rc = claim_writer_stamp(s, head->entry, PS_STREAM_EDIT, &continued, error);
  rc = ps_internal_capture_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_owned_inspect(ps_space *space, ps_key key, const ps_ref *ref,
                     ps_owned_status *out, lc_error *error) {
  ps_owned_head head;
  int rc;
  if (!space || !out || key.kind != PS_STREAM)
    return err(error, LC_ERR_INVALID, "owned inspection arguments invalid");
  memset(out, 0, sizeof(*out));
  pthread_mutex_lock(&space->mutex);
  rc = healthy(space, error);
  head.space = space;
  head.entry = lookup(space, key);
  if (rc == LC_OK && (!head.entry || head.entry->deleted))
    rc = err(error, LC_ERR_INVALID, "owned inspection target is missing");
  if (rc == LC_OK && ref &&
      (memcmp(ref->container, space->identity, 16) ||
       ref->incarnation != head.entry->token))
    rc = err(error, LC_ERR_INVALID, "stale or foreign owned inspection");
  if (rc == LC_OK)
    owned_status(&head, out);
  pthread_mutex_unlock(&space->mutex);
  return rc;
}
static int owned_allowance(ps_space *s, ps_budget *budget, uint64_t payload,
                           lc_error *error) {
  return checkpoint_allowance_floor(s, s->metadata + PS_OWNED_DESCRIPTOR_BYTES,
                                    s->live_entries + 1, payload,
                                    PS_MAINTENANCE_RESERVE, budget, error);
}
static int owned_preparation_allowance(ps_space *s, ps_budget *budget,
                                       uint64_t bytes, uint64_t reserved,
                                       lc_error *error) {
  if (!budget)
    return owned_allowance(s, NULL, reserved, error);
  /* The grant's pages and descriptor are already included in the claim.
   * Extra zero preparation may borrow only unclaimed common headroom. */
  return checkpoint_allowance_floor(s, s->metadata, s->live_entries,
                                    reserved - bytes, PS_MAINTENANCE_RESERVE,
                                    NULL, error);
}
static int owned_allocate(ps_owned_head *head, ps_budget *budget, uint64_t base,
                          uint64_t desired, ps_owned_run *run,
                          lc_error *error) {
  ps_space *s = head->space;
  ps_entry *p = lc_calloc_with_allocator(NULL, 1, sizeof(*p));
  unsigned char *zero = lc_calloc_with_allocator(NULL, 1, PS_CHUNK);
  unsigned char header[PS_RUN_HEADER];
  uint64_t bytes = desired, reserved = 0, sequence = 0;
  uint64_t prepared, partial_bytes, candidate, free_before;
  ps_run **choice = NULL, **link, **partial, **fit;
  ps_run *spare = NULL;
  ps_coordination_guard *guard = NULL;
  int rc, clean = 0, registered = 0, reclaimed = 0;
  if (p)
    p->extents = lc_calloc_with_allocator(NULL, 1, sizeof(*p->extents));
  if (!p || !p->extents || !zero) {
    if (p)
      entry_free(p);
    lc_free_with_allocator(NULL, zero);
    return err(error, LC_ERR_NOMEM, "cannot allocate owned run preparation");
  }
  p->key.kind = PS_OWNED_RUN;
  p->key.owner = head->entry->token;
  p->key.key = base;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_admission_begin(s, &guard, error);
  if (rc == LC_OK && (head->entry->deleted || head->entry->stream_sealed))
    rc = err(error, LC_ERR_INVALID, "owned stream cannot accept grants");
  if (rc == LC_OK &&
      (base != head->entry->stream_capacity || s->token_next > s->token_end ||
       base > (uint64_t)INT64_MAX - (desired - PS_RUN_HEADER)))
    rc = err(error, LC_ERR_INVALID, "owned grant base or incarnation invalid");
  /* Protect metadata before retaining a free-list selection: admission may
   * transfer common intervals into the checkpoint-only pool. */
  if (rc == LC_OK)
    rc = owned_allowance(s, budget, 0, error);
select_run:
  if (rc == LC_OK) {
    for (;;) {
      partial = fit = NULL;
      choice = NULL;
      partial_bytes = 0;
      clean = 0;
      for (link = &s->free; *link; link = &(*link)->next) {
        if ((*link)->claimed && !budget)
          continue;
        candidate = (*link)->bytes < bytes ? (*link)->bytes : bytes;
        prepared = s->readiness
                       ? ps_readiness_zero_prefix(s, (*link)->offset, candidate)
                   : (*link)->clean ? candidate
                                    : 0;
        if (prepared == bytes) {
          choice = link;
          clean = 1;
          break;
        }
        if (prepared && !partial) {
          partial = link;
          partial_bytes = prepared;
        }
        if ((*link)->bytes >= bytes &&
            (!fit || ((*fit)->claimed && !(*link)->claimed)))
          fit = link;
      }
      if (!choice && partial) {
        choice = partial;
        bytes = partial_bytes;
        clean = 1;
      }
      if (!choice)
        choice = fit;
      if (!choice)
        for (link = &s->free; *link; link = &(*link)->next)
          if (!(*link)->claimed || budget) {
            choice = link;
            break;
          }
      if (!choice && !reclaimed && (s->pinned_free || s->pinned_pages)) {
        rc = pins_reclaim_locked(s, error);
        if (rc != LC_OK)
          break;
        reclaimed = 1;
        continue;
      }
      if (!choice) {
        rc = err(error, LC_ERR_INVALID,
                 "owned payload capacity exhausted; grow");
        break;
      }
      if (bytes > (*choice)->bytes)
        bytes = (*choice)->bytes;
      free_before = s->free_bytes;
      rc = owned_allowance(s, budget, bytes, error);
      /* The bound is already protected. A newly reclaimed common interval
       * can change or coalesce the selected node before we take it. */
      if (rc == LC_OK && s->free_bytes != free_before)
        continue;
      if (rc == LC_OK || bytes == PS_PAGE)
        break;
      lc_error_cleanup(error);
      bytes /= 4;
      if (bytes < PS_PAGE)
        bytes = PS_PAGE;
      bytes = bytes / PS_PAGE * PS_PAGE;
    }
  }
  if (rc == LC_OK) {
    reserved = bytes;
    if (!(*choice)->claimed && !clean && bytes < PS_MIB) {
      reserved = (*choice)->bytes < PS_MIB ? (*choice)->bytes : PS_MIB;
      free_before = s->free_bytes;
      while (reserved > bytes) {
        rc = owned_preparation_allowance(s, budget, bytes, reserved, error);
        if (rc == LC_OK)
          break;
        lc_error_cleanup(error);
        rc = LC_OK;
        reserved = reserved / 2 / PS_PAGE * PS_PAGE;
        if (reserved < bytes)
          reserved = bytes;
      }
      if (s->free_bytes != free_before)
        goto select_run;
      if (reserved > bytes) {
        spare = lc_calloc_with_allocator(NULL, 1, sizeof(*spare));
        if (!spare)
          rc = err(error, LC_ERR_NOMEM,
                   "cannot allocate prepared free interval");
      }
    }
  }
  if (rc == LC_OK) {
    rc = take_payload_selected(s, budget, choice, reserved, bytes,
                               &p->extents[0].offset, error);
    if (rc == LC_OK)
      ps_internal_budget_spend(budget, bytes, 0, 0);
  }
  if (rc == LC_OK) {
    p->token = s->token_next++;
    p->bytes = p->extents[0].bytes = reserved; /* entire private preparation */
    p->count = p->capacity = 1;
    p->payload_claimed = 1;
    p->extents[0].flags = EXTENT_OWNED;
    register_entry(s, p);
    ++s->writers;
    if (budget)
      ++budget->users;
    registered = 1;
  }
  rc = allocation_end(s, guard, rc, error);
  guard = NULL;
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK && !clean)
    rc = initialize_range(s, p->extents[0].offset, reserved, zero, error);
  if (rc == LC_OK) {
    memset(header, 0, sizeof(header));
    put64(header, OWNED_MAGIC);
    put64(header + 8, p->token);
    put64(header + 16, p->key.owner);
    put64(header + 24, base);
    put64(header + 32, p->extents[0].offset);
    put64(header + 40, bytes);
    memcpy(header + 48, s->identity, 16);
    p->extents[0].crc = checksum(header, PS_RUN_IDENTITY_BYTES);
    rc = transfer(s, header, sizeof(header), p->extents[0].offset, 1, error);
  }
  if (rc == LC_OK && !clean) {
    pthread_mutex_lock(&s->mutex);
    rc = healthy(s, error);
    if (rc == LC_OK &&
        __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED) == UINT64_MAX) {
      __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
      rc = err(error, LC_ERR_TRANSPORT,
               "preparation readiness exhausted; reopen");
    }
    if (rc == LC_OK)
      sequence = __atomic_add_fetch(&s->pending_sequence, 1, __ATOMIC_RELEASE);
    pthread_mutex_unlock(&s->mutex);
    if (rc == LC_OK)
      rc = wait_durable(s, sequence, error);
  }
  pthread_mutex_lock(&s->mutex);
  if (rc == LC_OK)
    rc = ps_internal_admission_begin(s, &guard, error);
  if (rc == LC_OK && spare)
    rc = physical_release(s, p->extents[0].offset + bytes, reserved - bytes,
                          error);
  if (rc == LC_OK) {
    p->bytes = p->extents[0].bytes = bytes;
    p->stream_capacity = bytes - PS_RUN_HEADER;
    if (spare) {
      spare->offset = p->extents[0].offset + bytes;
      spare->bytes = reserved - bytes;
      spare->clean = !s->readiness;
      ps_readiness_zero_pages(s, spare->offset, spare->bytes, 1);
      free_insert(s, spare);
      spare = NULL;
    }
  }
  /* Preparation released allocator authority. Recheck against peer metadata
   * growth before installing this new grant's descriptor. Its payload is
   * already privately owned, so no second payload charge is required. */
  if (rc == LC_OK && (head->entry->deleted || head->entry->stream_sealed ||
                      stream_lookup(s, head->entry->token) != head->entry ||
                      base != head->entry->stream_capacity))
    rc = err(error, LC_ERR_INVALID, "owned stream changed during preparation");
  if (rc == LC_OK)
    rc = owned_allowance(s, budget, 0, error);
  if (rc == LC_OK)
    rc = append_event(s, p, PS_PUT, error);
  if (rc == LC_OK) {
    rc = install_owned_grant(s, p, error);
    if (rc != LC_OK)
      __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
  }
  if (rc == LC_OK) {
    ps_internal_budget_spend(budget, 0, PS_OWNED_DESCRIPTOR_BYTES, 1);
    p->publication_sequence =
        __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED);
    dirty_entry(s, p, NULL);
    install(s, p);
    /* Transfer private range witnesses directly to the returned run. */
    (void)ps_internal_entry_pin(s, p, NULL);
    rc = entry_release_claims(s, p, error);
    if (rc == LC_OK) {
      ++s->readers;
      run->space = s;
      run->entry = p;
      run->references = 1;
    }
  } else {
    if (registered) {
      if (!__atomic_load_n(&s->poisoned, __ATOMIC_RELAXED) &&
          release_entry_runs(s, p, 0, NULL) != LC_OK)
        __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
      unregister_entry(p);
    }
    entry_free(p);
  }
  if (registered) {
    --s->writers;
    if (budget)
      --budget->users;
  }
  rc = allocation_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  lc_free_with_allocator(NULL, zero);
  lc_free_with_allocator(NULL, spare);
  return rc;
}
int ps_owned_grant(ps_owned_head *head, ps_budget *budget, uint64_t base,
                   uint64_t desired, ps_owned_run **out, int *found,
                   lc_error *error) {
  ps_owned_run *run;
  ps_coordination_guard *guard;
  ps_space *s;
  ps_entry *p;
  ps_key key;
  unsigned char header[PS_RUN_HEADER];
  int rc;
  if (out)
    *out = NULL;
  if (found)
    *found = 0;
  if (!head || !out || !found ||
      (desired && (desired < PS_PAGE || desired > PS_MIB || desired % PS_PAGE)))
    return err(error, LC_ERR_INVALID, "owned run grant arguments invalid");
  s = head->space;
  run = lc_calloc_with_allocator(NULL, 1, sizeof(*run));
  if (!run)
    return err(error, LC_ERR_NOMEM, "cannot allocate owned run handle");
  if (desired)
    rc = owned_allocate(head, budget, base, desired, run, error);
  else {
    key.kind = PS_OWNED_RUN;
    key.owner = head->entry->token;
    key.key = base;
    pthread_mutex_lock(&s->mutex);
    rc = ps_internal_capture_begin(s, &guard, error);
    p = lookup(s, key);
    if (rc == LC_OK && p && !p->deleted)
      rc = ps_internal_entry_pin(s, p, error);
    if (rc == LC_OK && p && !p->deleted) {
      ++s->readers;
      run->space = s;
      run->entry = p;
      run->references = 1;
    }
    rc = ps_internal_capture_end(s, guard, rc, error);
    pthread_mutex_unlock(&s->mutex);
    if (rc == LC_OK && run->entry)
      rc = verify_owned(s, p, header, error);
  }
  if (rc == LC_OK && run->entry) {
    *out = run;
    *found = 1;
  } else if (run->entry)
    ps_owned_run_close(run);
  else
    lc_free_with_allocator(NULL, run);
  return rc;
}
uint64_t ps_owned_run_capacity(const ps_owned_run *run) {
  return run ? run->entry->stream_capacity : 0;
}
uint64_t ps_owned_run_base(const ps_owned_run *run) {
  return run ? run->entry->key.key : 0;
}
static int capture_runs(ps_space *s, ps_entry *head, ps_key key,
                        const ps_ref *ref, uint64_t start, uint64_t end,
                        ps_owned_status *status, ps_owned_run ***out,
                        size_t *count, lc_error *error) {
  ps_coordination_guard *guard;
  ps_entry *p, *first = NULL, *limit = NULL;
  ps_owned_run **items = NULL;
  size_t n = 0, capacity = 0, i;
  int rc;
  if (out)
    *out = NULL;
  if (count)
    *count = 0;
  if (!s || !out || !count ||
      (!head && (!ref || key.kind != PS_STREAM || (!status && start >= end))))
    return err(error, LC_ERR_INVALID,
               "mapping capture requires head and outputs");
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &guard, error);
  if (rc == LC_OK && !head) {
    head = lookup(s, key);
    if (!head || head->token != ref->incarnation ||
        memcmp(ref->container, s->identity, 16) || end > head->bytes)
      rc = err(error, LC_ERR_PROTOCOL, "certified stream prefix changed");
  }
  if (rc == LC_OK &&
      (head->deleted ||
       head->stream_runs > (uint64_t)((size_t)-1 / sizeof(*items))))
    rc = err(error, LC_ERR_INVALID,
             "mapping capture is unavailable or too large");
  if (rc == LC_OK) {
    first = head->owned_first;
    capacity = (size_t)head->stream_runs;
    if (status) {
      ps_owned_head selected;
      selected.space = s;
      selected.entry = head;
      owned_status(&selected, status);
    }
  }
  if (rc == LC_OK && ref && end) {
    while (first && first->key.key + first->stream_capacity <= start)
      first = first->owned_next;
    capacity = 0;
    for (limit = first; limit && limit->key.key < end;
         limit = limit->owned_next)
      ++capacity;
  }
  if (rc == LC_OK && capacity) {
    items = lc_calloc_with_allocator(NULL, capacity, sizeof(*items));
    if (!items)
      rc = err(error, LC_ERR_NOMEM, "cannot allocate mapping capture");
  }
  for (p = first; rc == LC_OK && p != limit; p = p->owned_next) {
    if (n == capacity) {
      rc = err(error, LC_ERR_PROTOCOL, "mapping count exceeds its head");
      break;
    }
    items[n] = lc_calloc_with_allocator(NULL, 1, sizeof(**items));
    if (!items[n]) {
      rc = err(error, LC_ERR_NOMEM, "cannot capture mapping handle");
      break;
    }
    rc = ps_internal_entry_pin(s, p, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(NULL, items[n]);
      break;
    }
    items[n]->space = s;
    items[n]->entry = p;
    items[n]->references = 1;
    ++s->readers;
    ++n;
  }
  if (rc == LC_OK && n != capacity)
    rc = err(error, LC_ERR_PROTOCOL, "mapping count differs from its head");
  rc = ps_internal_capture_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  for (i = 0; rc == LC_OK && i < n; ++i) {
    unsigned char header[PS_RUN_IDENTITY_BYTES];
    rc = verify_owned(s, items[i]->entry, header, error);
  }
  if (rc == LC_OK) {
    *out = items;
    *count = n;
  } else {
    for (i = 0; i < n; ++i)
      ps_owned_run_close(items[i]);
    lc_free_with_allocator(NULL, items);
  }
  return rc;
}
int ps_owned_runs_capture(ps_owned_head *head, ps_owned_run ***out,
                          size_t *count, lc_error *error) {
  return ps_owned_runs_capture_status(head, NULL, out, count, error);
}
int ps_owned_runs_capture_status(ps_owned_head *head, ps_owned_status *status,
                                 ps_owned_run ***out, size_t *count,
                                 lc_error *error) {
  const ps_key none = {0};
  return capture_runs(head ? head->space : NULL, head ? head->entry : NULL,
                      none, NULL, 0, 0, status, out, count, error);
}
int ps_owned_runs_key_status(ps_space *s, ps_key key, const ps_ref *ref,
                             ps_owned_status *status, ps_owned_run ***out,
                             size_t *count, lc_error *error) {
  return capture_runs(s, NULL, key, ref, 0, 0, status, out, count, error);
}
int ps_owned_runs_range(ps_space *s, ps_key key, const ps_ref *ref,
                        uint64_t start, uint64_t end, ps_owned_run ***out,
                        size_t *count, lc_error *error) {
  return capture_runs(s, NULL, key, ref, start, end, NULL, out, count, error);
}
int ps_owned_run_mapped(const ps_owned_run *run) {
  return run && run->entry->owned_mapped;
}
void ps_owned_run_retain(ps_owned_run *run) {
  if (run)
    (void)__atomic_add_fetch(&run->references, 1, __ATOMIC_RELAXED);
}
void ps_owned_run_close(ps_owned_run *run) {
  ps_space *s;
  if (!run)
    return;
  if (__atomic_sub_fetch(&run->references, 1, __ATOMIC_ACQ_REL))
    return;
  s = run->space;
  pthread_mutex_lock(&s->mutex);
  ps_internal_entry_unpin(s, run->entry);
  --s->readers;
  if (run->entry->state == 2 && run->entry->safe && !run->entry->pins)
    collect(s);
  pthread_mutex_unlock(&s->mutex);
  lc_free_with_allocator(NULL, run);
}
static int owned_io(ps_owned_run *run, uint64_t offset, void *bytes, size_t n,
                    int writing, lc_error *error) {
  int rc;
  if (!run || (!bytes && n) || offset > ps_owned_run_capacity(run) ||
      n > ps_owned_run_capacity(run) - offset)
    return err(error, LC_ERR_INVALID, "owned run I/O outside granted range");
  rc = healthy(run->space, error);
  if (rc == LC_OK && run->entry->owned_mapped) {
    ps_entry *p = run->entry;
    ps_reader *reader;
    unsigned char *out = bytes;
    size_t lo = 0, hi = p->range_count, mid, take;
    if (writing)
      return err(error, LC_ERR_INVALID, "mapped pieces are immutable");
    if (!n)
      return LC_OK;
    while (!p->range_bitmap && lo < hi) {
      mid = lo + (hi - lo) / 2;
      if (p->ranges[mid].offset + p->ranges[mid].bytes <= offset)
        lo = mid + 1;
      else
        hi = mid;
    }
    reader = lc_calloc_with_allocator(NULL, 1, sizeof(*reader));
    if (!reader)
      return err(error, LC_ERR_NOMEM, "cannot allocate mapped read scratch");
    reader->space = run->space;
    reader->entry = p; /* run already pins this exact incarnation */
    while (n && rc == LC_OK) {
      uint64_t data;
      if (p->range_bitmap) {
        take = 0;
        while (take < n && (p->range_bitmap[(offset + take) / 8] &
                            (1U << ((offset + take) % 8))))
          ++take;
        data = 64 + (p->stream_capacity + 7) / 8 + bitmap_rank(p, offset);
      } else if (lo < p->range_count && offset >= p->ranges[lo].offset) {
        take = (size_t)(p->ranges[lo].offset + p->ranges[lo].bytes - offset);
        if (take > n)
          take = n;
        data = p->ranges[lo].data + offset - p->ranges[lo].offset;
      } else {
        take = 0;
        data = 0;
      }
      if (!take) {
        rc = err(error, LC_ERR_PROTOCOL, "required stream range is unmapped");
        break;
      }
      rc = ps_read_at(reader, data, out, take, error);
      offset += take;
      out += take;
      n -= take;
      ++lo;
    }
    lc_free_with_allocator(NULL, reader);
    return rc;
  }
  return rc == LC_OK
             ? transfer(run->space, bytes, n,
                        run->entry->extents[0].offset + PS_RUN_HEADER + offset,
                        writing, error)
             : rc;
}
int ps_owned_read(ps_owned_run *run, uint64_t offset, void *bytes, size_t n,
                  lc_error *error) {
  return owned_io(run, offset, bytes, n, 0, error);
}
int ps_owned_write(ps_owned_run *run, uint64_t offset, const void *bytes,
                   size_t n, lc_error *error) {
  return owned_io(run, offset, (void *)bytes, n, 1, error);
}
static int owned_piece(ps_owned_run *run, uint64_t bytes, void *scratch,
                       int operation, uint64_t prior,
                       const ps_piece_checksum *prefix, ps_piece_checksum *out,
                       lc_error *error) {
  unsigned char footer[PS_RUN_HEADER - PS_RUN_IDENTITY_BYTES];
  ps_space *s;
  uint64_t offset;
  uint32_t crc;
  uint64_t reconstructed = UINT64_C(1) << 63;
  int rc;
  if (!run || !bytes || bytes > ps_owned_run_capacity(run) ||
      ps_owned_run_capacity(run) > PS_CHUNK || (operation && !scratch) ||
      prior > bytes || (prior && !prefix) ||
      (prefix && (prefix->reconstructed < 0 || prefix->reconstructed > 1 ||
                  (!prior && prefix->crc))))
    return err(error, LC_ERR_INVALID,
               "piece completion requires bounded finalized bytes");
  s = run->space;
  if (run->entry->owned_mapped) {
    if (operation == 2)
      return err(error, LC_ERR_INVALID, "mapped pieces cannot be extended");
    if (run->entry->mapped_end > bytes)
      return err(error, LC_ERR_PROTOCOL,
                 "mapped piece exceeds its completed prefix");
    rc = operation ? verify_value_payloads(s, run->entry, scratch, error)
                   : healthy(s, error);
    if (rc == LC_OK && out)
      memset(out, 0, sizeof(*out));
    return rc;
  }
  offset = run->entry->extents[0].offset;
  rc = healthy(s, error);
  if (rc == LC_OK && operation == 2) {
    rc = transfer(s, scratch, (size_t)(bytes - prior),
                  offset + PS_RUN_HEADER + prior, 0, error);
    if (rc == LC_OK) {
      crc = checksum(scratch, (size_t)(bytes - prior));
      if (prior)
        crc =
            (uint32_t)crc32_combine(prefix->crc, crc, (z_off_t)(bytes - prior));
      memcpy(footer, "LCPSPIEC", 8);
      put64(footer + 8,
            bytes | (prefix && prefix->reconstructed ? reconstructed : 0));
      put32(footer + 16, crc);
      put64(footer + 20, run->entry->token);
      put32(footer + 28, checksum(footer, 28));
      rc = transfer(s, footer, sizeof(footer), offset + PS_RUN_IDENTITY_BYTES,
                    1, error);
      if (rc == LC_OK && out) {
        out->crc = crc;
        out->reconstructed = prefix && prefix->reconstructed;
      }
    }
    return rc;
  }
  if (rc == LC_OK)
    rc = transfer(s, footer, sizeof(footer), offset + PS_RUN_IDENTITY_BYTES, 0,
                  error);
  if (rc == LC_OK && (memcmp(footer, "LCPSPIEC", 8) ||
                      (get64(footer + 8) & ~reconstructed) != bytes ||
                      get64(footer + 20) != run->entry->token ||
                      get32(footer + 28) != checksum(footer, 28)))
    rc = err(error, LC_ERR_PROTOCOL, "invalid finalized piece checksum footer");
  if (rc == LC_OK && operation && (get64(footer + 8) & reconstructed))
    rc = lc_error_set(error, LC_ERR_INVALID, 0,
                      "reconstructed piece needs canonical body validation",
                      NULL, "pouch_piece_needs_validation", "pouch-space");
  if (rc == LC_OK && operation) {
    rc = transfer(s, scratch, (size_t)bytes, offset + PS_RUN_HEADER, 0, error);
    if (rc == LC_OK && checksum(scratch, (size_t)bytes) != get32(footer + 16))
      rc = err(error, LC_ERR_PROTOCOL, "finalized piece checksum mismatch");
  }
  if (rc == LC_OK && out) {
    out->crc = get32(footer + 16);
    out->reconstructed = (get64(footer + 8) & reconstructed) != 0;
  }
  return rc;
}
int ps_owned_piece_extend(ps_owned_run *run, uint64_t prior,
                          const ps_piece_checksum *prefix, uint64_t bytes,
                          void *scratch, ps_piece_checksum *out,
                          lc_error *error) {
  if (!out)
    return err(error, LC_ERR_INVALID, "piece extension requires CRC output");
  return owned_piece(run, bytes, scratch, 2, prior, prefix, out, error);
}
int ps_owned_piece_inspect(ps_owned_run *run, uint64_t bytes,
                           ps_piece_checksum *out, lc_error *error) {
  return owned_piece(run, bytes, NULL, 0, 0, NULL, out, error);
}
int ps_owned_piece_verify(ps_owned_run *run, uint64_t bytes, void *scratch,
                          lc_error *error) {
  return owned_piece(run, bytes, scratch, 1, 0, NULL, NULL, error);
}
typedef struct piece_selection {
  ps_owned_head *head;
  ps_entry *old, *tombstone;
  ps_writer *writer;
  int published;
} piece_selection;
uint64_t ps_owned_run_storage(const ps_owned_run *run) {
  return entry_storage(run->entry);
}
int ps_owned_run_matches(const ps_owned_run *run, const ps_owned_range *ranges,
                         size_t count) {
  ps_entry *p = run->entry;
  uint64_t live = 0, end = 0;
  size_t i;
  if (!p->owned_mapped || count != p->range_count)
    return 0;
  if (p->range_bitmap) {
    for (i = 0; i < count; ++i) {
      if (!ranges[i].bytes || (i && ranges[i].offset <= end) ||
          ranges[i].offset > p->stream_capacity ||
          ranges[i].bytes > p->stream_capacity - ranges[i].offset)
        return 0;
      end = ranges[i].offset + ranges[i].bytes;
      if (bitmap_rank(p, end) - bitmap_rank(p, ranges[i].offset) !=
          ranges[i].bytes)
        return 0;
      live += ranges[i].bytes;
    }
    return live == p->bytes - 64 - (p->stream_capacity + 7) / 8;
  }
  for (i = 0; i < count; ++i)
    if (ranges[i].offset != run->entry->ranges[i].offset ||
        ranges[i].bytes != run->entry->ranges[i].bytes)
      return 0;
  return 1;
}
static int piece_publish(void *context, lc_error *error) {
  piece_selection *selection = context;
  ps_space *s = selection->head->space;
  ps_entry *old = selection->old, *tombstone = selection->tombstone;
  uint64_t sequence = 0;
  int rc;
  if (selection->published)
    return err(error, LC_ERR_INVALID, "piece publication already selected");
  if (selection->writer) {
    rc = ps_publish(selection->writer, NULL, error);
    selection->published = selection->writer->published;
    return rc;
  }
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc == LC_OK &&
      (lookup(s, old->key) != old || old->deleted ||
       selection->head->entry->deleted || !selection->head->entry->stream_runs))
    rc = err(error, LC_ERR_INVALID, "captured piece mapping changed");
  if (rc == LC_OK)
    rc = append_event(s, old, PS_REMOVE, error);
  if (rc == LC_OK) {
    sequence = __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED);
    --selection->head->entry->stream_runs;
    if (!selection->head->entry->dirty_link)
      dirty_entry(s, selection->head->entry, NULL);
    tombstone->key = old->key;
    tombstone->token = old->token;
    tombstone->deleted = 1;
    tombstone->publication_sequence = sequence;
    register_entry(s, tombstone);
    dirty_entry(s, tombstone, old);
    install(s, tombstone);
    selection->tombstone = NULL;
    selection->published = 1;
  }
  pthread_mutex_unlock(&s->mutex);
  return rc == LC_OK ? wait_durable(s, sequence, error) : rc;
}
static int piece_select(piece_selection *selection, ps_piece_select_fn select,
                        void *context, lc_error *error) {
  int rc = select ? select(context, piece_publish, selection, error)
                  : piece_publish(selection, error);
  if (rc == LC_OK && !selection->published)
    rc = err(error, LC_ERR_PROTOCOL, "piece selector omitted publication");
  if (rc != LC_OK && selection->published)
    __atomic_store_n(&selection->head->space->poisoned, 1, __ATOMIC_RELAXED);
  return rc;
}
int ps_owned_run_repack(ps_owned_head *head, ps_owned_run *source,
                        ps_budget *budget, const ps_owned_range *ranges,
                        size_t count, ps_piece_select_fn select, void *context,
                        ps_owned_run **out, lc_error *error) {
  ps_space *s;
  ps_entry *old;
  piece_selection selection = {0};
  ps_writer *writer = NULL;
  ps_owned_run *result = NULL;
  unsigned char header[64] = {0}, *table = NULL, *scratch = NULL;
  uint64_t valid = 0, end = 0, live = 0, table_bytes, pos;
  size_t i;
  int rc, bitmap;
  if (out)
    *out = NULL;
  if (!head || !source || !out || (count && !ranges) ||
      source->space != head->space ||
      source->entry->key.owner != head->entry->token ||
      (count && ps_owned_run_capacity(source) > PS_CHUNK) ||
      count > ps_owned_run_capacity(source))
    return err(error, LC_ERR_INVALID,
               "piece replacement requires matching bounded owners");
  s = head->space;
  old = source->entry;
  selection.head = head;
  selection.old = old;
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc == LC_OK &&
      (head->entry->deleted || old->deleted || lookup(s, old->key) != old ||
       (old->key.key >= head->entry->bytes &&
        (!head->entry->stream_sealed || count)) ||
       (!head->entry->stream_sealed &&
        old->stream_capacity > head->entry->bytes - old->key.key)))
    rc = err(error, LC_ERR_INVALID,
             "piece is changing, incomplete or no longer selected");
  if (rc == LC_OK) {
    /* Sealing fixes the committed tail. An entirely unused grant beyond it
     * has no canonical payload/footer to verify and can only be discarded. */
    valid = old->key.key < head->entry->bytes
                ? head->entry->bytes - old->key.key
                : 0;
    if (valid > old->stream_capacity)
      valid = old->stream_capacity;
  }
  pthread_mutex_unlock(&s->mutex);
  for (i = 0; rc == LC_OK && i < count; ++i) {
    if (!ranges[i].bytes || (i && ranges[i].offset <= end) ||
        ranges[i].offset > valid || ranges[i].bytes > valid - ranges[i].offset)
      rc = err(error, LC_ERR_INVALID,
               "piece live ranges overlap or exceed completion");
    else {
      end = ranges[i].offset + ranges[i].bytes;
      live += ranges[i].bytes;
    }
  }
  if (rc != LC_OK)
    return rc;
  if (!count) {
    selection.tombstone =
        lc_calloc_with_allocator(NULL, 1, sizeof(*selection.tombstone));
    if (!selection.tombstone)
      return err(error, LC_ERR_NOMEM, "cannot allocate piece retirement");
    rc = piece_select(&selection, select, context, error);
    lc_free_with_allocator(NULL, selection.tombstone);
    return rc;
  }
  table_bytes = (uint64_t)count * 8;
  bitmap = table_bytes > (old->stream_capacity + 7) / 8;
  if (bitmap)
    table_bytes = (old->stream_capacity + 7) / 8;
  scratch = lc_alloc_with_allocator(NULL, PS_CHUNK);
  table = lc_calloc_with_allocator(NULL, 1, (size_t)table_bytes);
  result = lc_calloc_with_allocator(NULL, 1, sizeof(*result));
  if (!scratch || !table || !result)
    rc = err(error, LC_ERR_NOMEM, "cannot allocate bounded piece replacement");
  if (rc == LC_OK)
    rc = ps_owned_piece_verify(source, valid, scratch, error);
  if (rc == LC_OK)
    rc = begin_value(s, budget, old->key, 1, &writer, error);
  if (rc == LC_OK) {
    writer->expected_owned = old;
    writer->entry->owned_mapped = 1;
    writer->entry->stream_capacity = old->stream_capacity;
    for (i = 0; i < count; ++i) {
      if (bitmap) {
        for (pos = ranges[i].offset; pos < ranges[i].offset + ranges[i].bytes;
             ++pos)
          table[pos / 8] |= (unsigned char)(1U << (pos % 8));
      } else {
        put32(table + i * 8, (uint32_t)ranges[i].offset);
        put32(table + i * 8 + 4, (uint32_t)ranges[i].bytes);
      }
    }
    memcpy(header, "LCPSMAPR", 8);
    put64(header + 8, old->key.owner);
    put64(header + 16, old->key.key);
    put64(header + 24, old->stream_capacity);
    put32(header + 32, (uint32_t)count);
    put32(header + 36, checksum(table, (size_t)table_bytes));
    put64(header + 40, 64 + table_bytes + live);
    put64(header + 48, writer->entry->token);
    put32(header + 60, (uint32_t)bitmap);
    put32(header + 56, (uint32_t)crc32(checksum(header, 56), header + 60, 4));
    rc = ps_write(writer, header, sizeof(header), error);
    if (rc == LC_OK)
      rc = ps_write(writer, table, (size_t)table_bytes, error);
  }
  for (i = 0; rc == LC_OK && i < count; ++i) {
    if (old->owned_mapped) {
      rc = ps_owned_read(source, ranges[i].offset, scratch,
                         (size_t)ranges[i].bytes, error);
      if (rc == LC_OK)
        rc = ps_write(writer, scratch, (size_t)ranges[i].bytes, error);
    } else
      rc = ps_write(writer, scratch + ranges[i].offset, (size_t)ranges[i].bytes,
                    error);
  }
  if (rc == LC_OK)
    rc = flush_writer(writer, error);
  if (rc == LC_OK)
    rc = owned_map_load(s, writer->entry, error);
  if (rc == LC_OK) {
    pthread_mutex_lock(&s->mutex);
    result->space = s;
    result->entry = writer->entry;
    result->references = 1;
    ++result->entry->pins;
    ++s->readers;
    pthread_mutex_unlock(&s->mutex);
    selection.writer = writer;
    rc = piece_select(&selection, select, context, error);
  }
  if (rc == LC_OK) {
    *out = result;
    result = NULL;
  }
  if (result && result->entry)
    ps_owned_run_close(result);
  else
    lc_free_with_allocator(NULL, result);
  ps_writer_close(writer);
  lc_free_with_allocator(NULL, scratch);
  lc_free_with_allocator(NULL, table);
  return rc;
}
int ps_owned_clear(ps_owned_run *run, uint64_t offset, uint64_t bytes,
                   void *scratch, lc_error *error) {
  int rc;
  if (!run || !scratch || run->entry->owned_mapped ||
      offset > ps_owned_run_capacity(run) ||
      bytes > ps_owned_run_capacity(run) - offset)
    return err(error, LC_ERR_INVALID, "owned clear outside granted range");
  rc = healthy(run->space, error);
  if (rc != LC_OK)
    return rc;
  memset(scratch, 0, PS_CHUNK);
  return initialize_range(
      run->space, run->entry->extents[0].offset + PS_RUN_HEADER + offset, bytes,
      scratch, error);
}
static int owned_tail_allowed(ps_entry *p, uint64_t tail) {
  return !p->deleted && !p->stream_sealed && tail >= p->bytes &&
         tail <= p->stream_capacity;
}
static int owned_ready_sequence(ps_space *s, uint64_t *sequence,
                                lc_error *error) {
  if (__atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED) == UINT64_MAX) {
    __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
    return err(error, LC_ERR_TRANSPORT, "owned readiness exhausted; reopen");
  }
  *sequence = __atomic_add_fetch(&s->pending_sequence, 1, __ATOMIC_RELEASE);
  return LC_OK;
}
/* Payload writes and any logical selection have already finished. Only the
 * ready boundary joins the shared journal, under allocation authority. */
static int owned_publish_tail(ps_space *s, ps_entry *head, uint64_t tail,
                              int complete, uint64_t *sequence,
                              lc_error *error) {
  int rc = LC_OK;
  if (s->readiness && (tail > head->bytes || complete)) {
    ps_entry hint;
    memset(&hint, 0, sizeof(hint));
    hint.key = head->key;
    hint.token = head->token;
    hint.bytes = tail;
    rc = append_event_locked(
        s, &hint, complete ? PS_STREAM_COMPLETE : PS_STREAM_TAIL, error);
    if (rc == LC_OK) {
      *sequence = __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED);
      head->stream_wire = s->sequence;
    }
  } else if (!*sequence)
    rc = owned_ready_sequence(s, sequence, error);
  if (rc == LC_OK) {
    head->bytes = tail;
    head->stream_complete_generation =
        complete && s->readiness ? s->generation : 0;
    if (!head->dirty_link)
      dirty_entry(s, head, NULL);
    head->stream_ready = *sequence;
  }
  return rc;
}
static int owned_commit(ps_owned_head *head, uint64_t tail, int complete,
                        enum ps_commit_mode mode, ps_commit_publish_fn publish,
                        void *context, lc_error *error) {
  ps_space *s;
  ps_coordination_guard *guard;
  uint64_t sequence = 0, previous;
  int rc;
  if (!head)
    return err(error, LC_ERR_INVALID, "owned commit requires head");
  if (mode != PS_COMMIT_DEFAULT && mode != PS_COMMIT_DEFER_SYNC)
    return err(error, LC_ERR_INVALID, "invalid canonical commit mode");
  s = head->space;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &guard, error);
  previous = head->entry->bytes;
  if (rc == LC_OK && !owned_tail_allowed(head->entry, tail))
    rc = err(error, LC_ERR_INVALID, "owned commit tail outside append range");
  if (rc == LC_OK)
    rc = publish ? owned_ready_sequence(s, &sequence, error)
                 : owned_publish_tail(s, head->entry, tail, complete, &sequence,
                                      error);
  rc = allocation_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK && publish) {
    rc = publish(context, tail, error);
    if (rc == LC_OK) {
      pthread_mutex_lock(&s->mutex);
      rc = ps_internal_capture_begin(s, &guard, error);
      if (rc == LC_OK && s->durable &&
          __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE) < sequence)
        rc = err(error, LC_ERR_PROTOCOL,
                 "logical publication did not cover prepared canonical bytes");
      if (rc == LC_OK && (!owned_tail_allowed(head->entry, tail) ||
                          head->entry->bytes != previous))
        rc = err(error, LC_ERR_PROTOCOL,
                 "owned tail changed during logical publication");
      if (rc == LC_OK)
        rc = owned_publish_tail(s, head->entry, tail, complete, &sequence,
                                error);
      rc = allocation_end(s, guard, rc, error);
      /* The logical selection already succeeded. Abort cleanup must never
       * clear its bytes, even if publishing the live hint failed. */
      if (rc != LC_OK) {
        __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
        if (rc == LC_ERR_PENDING || rc == LC_ERR_NOMEM) {
          lc_error_cleanup(error);
          rc = err(error, LC_ERR_TRANSPORT,
                   "logical selection completed; reopen to recover its tail");
        }
      }
      pthread_mutex_unlock(&s->mutex);
    }
  } else if (rc == LC_OK && s->durable && mode == PS_COMMIT_DEFAULT)
    rc = wait_durable(s, sequence, error);
  return rc;
}
int ps_owned_commit(ps_owned_head *head, uint64_t tail,
                    enum ps_commit_mode mode, ps_commit_publish_fn publish,
                    void *context, lc_error *error) {
  return owned_commit(head, tail, 0, mode, publish, context, error);
}
int ps_owned_commit_canonical(ps_owned_head *head, uint64_t tail,
                              enum ps_commit_mode mode,
                              ps_commit_publish_fn publish, void *context,
                              lc_error *error) {
  return owned_commit(head, tail, 1, mode, publish, context, error);
}
int ps_owned_barrier(ps_owned_head *head, lc_error *error) {
  return head ? sync_file(head->space, error)
              : err(error, LC_ERR_INVALID, "owned barrier requires head");
}
int ps_owned_recovered(ps_owned_head *head, uint64_t tail, lc_error *error) {
  ps_space *s;
  ps_coordination_guard *guard;
  uint64_t sequence = 0, previous;
  int rc;
  if (!head)
    return err(error, LC_ERR_INVALID, "owned recovery requires head");
  s = head->space;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &guard, error);
  previous = head->entry->bytes;
  if (rc == LC_OK && !owned_tail_allowed(head->entry, tail))
    rc = err(error, LC_ERR_PROTOCOL, "recovered tail outside owned range");
  if (rc == LC_OK)
    rc = owned_ready_sequence(s, &sequence, error);
  rc = allocation_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK)
    rc = wait_durable(s, sequence, error);
  if (rc == LC_OK) {
    pthread_mutex_lock(&s->mutex);
    rc = ps_internal_capture_begin(s, &guard, error);
    if (rc == LC_OK && (!owned_tail_allowed(head->entry, tail) ||
                        head->entry->bytes != previous))
      rc = err(error, LC_ERR_PENDING, "owned stream changed during recovery");
    if (rc == LC_OK)
      rc = owned_publish_tail(s, head->entry, tail, 0, &sequence, error);
    rc = allocation_end(s, guard, rc, error);
    pthread_mutex_unlock(&s->mutex);
  }
  return rc;
}
int ps_owned_seal(ps_owned_head *head, lc_error *error) {
  ps_space *s;
  ps_entry snapshot;
  ps_coordination_guard *guard;
  uint64_t sequence = 0, tail;
  int rc, sealed, needs_sync;
  if (!head)
    return err(error, LC_ERR_INVALID, "owned seal requires head");
  s = head->space;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &guard, error);
  sealed = head->entry->stream_sealed;
  tail = head->entry->bytes;
  needs_sync = head->entry->stream_ready >
                   __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE) ||
               head->entry->stream_wire > s->peer_durable;
  if (rc == LC_OK && head->entry->deleted)
    rc = err(error, LC_ERR_INVALID, "retired stream cannot be sealed");
  rc = allocation_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  if (rc != LC_OK || sealed)
    return rc;
  /* Any valid seal event may recover before its acknowledgement. Its final
   * canonical tail must already be durable, even if journal bytes reorder. */
  if (needs_sync)
    rc = sync_file(s, error);
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &guard, error);
  if (rc == LC_OK && head->entry->deleted)
    rc = err(error, LC_ERR_INVALID, "retired stream cannot be sealed");
  if (rc == LC_OK && head->entry->bytes != tail)
    rc = err(error, LC_ERR_PENDING, "owned stream advanced during seal");
  sealed = head->entry->stream_sealed;
  if (rc == LC_OK && !sealed) {
    snapshot = *head->entry;
    snapshot.stream_sealed = 1;
    rc = append_event_locked(s, &snapshot, PS_SEAL, error);
  }
  if (rc == LC_OK && !sealed) {
    head->entry->stream_sealed = 1;
    if (!head->entry->dirty_link)
      dirty_entry(s, head->entry, NULL);
    sequence = __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED);
  }
  rc = allocation_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  return rc == LC_OK && sequence ? wait_durable(s, sequence, error) : rc;
}
int ps_owned_claim_orphan(ps_owned_head *head, lc_error *error) {
  ps_space *s;
  ps_coordination_guard *guard;
  int rc;
  if (!head)
    return err(error, LC_ERR_INVALID,
               "orphan claim requires an owned identity");
  s = head->space;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &guard, error);
  if (rc == LC_OK &&
      (head->entry->deleted || head->entry->catalog_refs ||
       head->entry->catalog_pins ||
       head->entry->catalog_release >
           __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE)))
    rc = err(error, LC_ERR_INVALID,
             "owned identity is retained by a catalog binding or view");
  if (rc == LC_OK && !head->entry->stream_claim)
    rc = ps_internal_stream_claim(s, head->entry->token,
                                  &head->entry->stream_claim, error);
  if (rc == LC_OK)
    head->entry->orphan_claimed = 1;
  rc = ps_internal_capture_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_owned_retire(ps_owned_head *head, lc_error *error) {
  ps_space *s;
  ps_coordination_guard *guard;
  uint64_t sequence = 0;
  int rc;
  if (!head)
    return err(error, LC_ERR_INVALID, "owned retirement requires head");
  s = head->space;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &guard, error);
  if (rc == LC_OK && !head->entry->deleted &&
      (head->entry->catalog_refs || head->entry->catalog_pins ||
       head->entry->catalog_release >
           __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE)))
    rc = err(error, LC_ERR_INVALID,
             "stream is retained by a catalog binding or view");
  if (rc == LC_OK && !head->entry->deleted)
    rc = remove_owned(s, head->entry, 0, 0, error);
  if (rc == LC_OK && !head->entry->deleted) {
    rc = append_event_locked(s, head->entry, PS_REMOVE, error);
    if (rc == LC_OK) {
      sequence = __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED);
      rc = remove_owned(s, head->entry, sequence, 1, error);
      if (rc != LC_OK)
        __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
    }
  } else if (rc == LC_OK)
    sequence = head->entry->publication_sequence;
  rc = allocation_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  return rc == LC_OK ? wait_durable(s, sequence, error) : rc;
}
ps_entry *ps_internal_lookup(ps_space *s, ps_key key) { return lookup(s, key); }
int ps_internal_healthy(ps_space *s, lc_error *e) { return healthy(s, e); }
int ps_internal_event(ps_space *s, ps_entry *p, int event, lc_error *e) {
  return append_event(s, p, event, e);
}
int ps_internal_allowance(ps_space *s, ps_budget *budget, uint64_t metadata,
                          uint64_t entries, lc_error *e) {
  return checkpoint_allowance_floor(s, metadata, entries, 0,
                                    PS_MAINTENANCE_RESERVE, budget, e);
}
void ps_internal_install(ps_space *s, ps_entry *p) {
  ps_entry *old = lookup(s, p->key);
  register_entry(s, p);
  dirty_entry(s, p, old);
  install(s, p);
}
void ps_internal_remove(ps_space *s, ps_entry *p, uint64_t sequence) {
  mark_removed(s, p, sequence);
}
int ps_internal_durable(ps_space *s, uint64_t sequence, lc_error *e) {
  return wait_durable(s, sequence, e);
}
void ps_internal_collect(ps_space *s) { collect(s); }
