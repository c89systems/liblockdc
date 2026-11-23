#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#include "crc.h"
#include "lc_api_internal.h"
#include "lc_intcompat.h"
#include "revision.h"
#include "space_internal.h"
#include "wire.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <linux/falloc.h>
#include <openssl/evp.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>
struct ps_writer {
  ps_space *space;
  ps_budget *budget;
  ps_entry *entry;
  ps_entry *expected_owned;
  size_t used, extent_capacity;
  int failed, published;
  unsigned char buffer[PS_CHUNK];
};
typedef struct packed_read {
  uint64_t offset;
  unsigned char bytes[PS_PAGE];
} packed_read;
struct ps_reader {
  ps_space *space;
  ps_entry *entry;
  packed_read *packed;
  unsigned char *buffer;
  const unsigned char *data;
  size_t extent, offset, loaded;
  size_t random_extent;
  uint64_t random_base;
  int random_loaded;
  int imported;
  int *private_failure;
};
/* Every imported reader holds presence continuously from before acquiring its
 * extents until last close. A handoff retains its source until import succeeds.
 * Exclusive lifetime admission excludes an old local issuer; a foreign issuer
 * already holds presence. Thus one probe before recovery detects every pin
 * that can legitimately arrive from a previous owner, without a persisted hint.
 * A new local export enables collection probes before its ticket escapes. */
#define PIN_PRESENCE_OFFSET 2U
/* Read-only imports keep byte 2, including across a cohort reset. Member
 * claims are tracked separately and publish a page hint before acquisition. */
#define MEMBER_PIN_PRESENCE_OFFSET 7U
#define TICKET_MAGIC LC_U64_WORDS(0x00315445UL, 0x4b435450UL)
#define TICKET_HEADER 112U
static int allocation_refresh(ps_space *, lc_error *);
static int allocation_ranges_refresh(ps_space *, lc_error *);
static int pins_reclaim_locked(ps_space *, lc_error *);
static int canonical_observe(ps_space *, lc_error *);
static int canonical_journal(ps_space *, const ps_readiness_certificate *,
                             int replay, int verify_end, lc_error *);
static int replay_frame(ps_space *, const unsigned char *, uint64_t,
                        lc_error *);
static int import_certified_journal(ps_space *,
                                    const ps_readiness_certificate *,
                                    int staged, lc_error *);
static int apply_journal_event(ps_space *, ps_entry *, int, uint64_t, uint64_t,
                               lc_error *);
static ps_entry *selected_value(ps_space *, ps_entry *, uint64_t);
typedef struct checkpoint_preview {
  ps_readiness_certificate certificate;
  ps_space *view;
  lc_error error;
  unsigned char roots[2 * PS_ROOT_BYTES];
  int result;
} checkpoint_preview;
static void preview_checkpoint(ps_space *, checkpoint_preview *);
static ps_space *projection_new(ps_space *, lc_error *);
static void projection_destroy(ps_space *);
static void projection_select(ps_space *, const unsigned char *, int);
static void projection_install(ps_space *, ps_space *);
static int canonical_refresh(ps_space *, checkpoint_preview *, int verify_disk,
                             lc_error *);
static int allocation_begin(ps_space *, ps_coordination_guard **, lc_error *);
static int allocation_end(ps_space *, ps_coordination_guard *, int, lc_error *);
static int stream_structure_ready(ps_space *, const ps_entry *, lc_error *);
static int claim_writer_stamp(ps_space *, ps_entry *, int, int *, lc_error *);
static int pin_runs_locked(ps_space *, ps_entry *, ps_key, const ps_ref *,
                           uint64_t, uint64_t, int, ps_owned_status *,
                           ps_owned_mapping **, size_t *, lc_error *);
typedef struct ps_value_slot {
  uint64_t key, token, bytes;
  ps_entry *retained;
  unsigned int kind;
  int witness;
} ps_value_slot;
typedef struct ps_value_range {
  uint64_t offset, bytes;
  ps_value_slot *item;
} ps_value_range;
struct ps_values {
  ps_space *space;
  uint64_t owner;
  ps_value_slot *items;
  /* Pin transitions and payload walks only visit retained items. */
  ps_value_slot **retained;
  size_t count, retained_count;
  ps_value_range *ranges, one_range;
  size_t range_count;
};
struct ps_owned_head {
  ps_space *space;
  ps_entry *entry;
};
typedef struct ps_owned_run_batch {
  size_t remaining;
} ps_owned_run_batch;
struct ps_owned_run {
  ps_space *space;
  ps_entry *entry;
  size_t references;
  ps_owned_run_batch *batch;
};

static uint32_t checksum(const void *p, size_t n) {
  return (uint32_t)ps_crc32(0, p, (uInt)n);
}
static int err(lc_error *e, int rc, const char *message) {
  return lc_error_set(e, rc, 0, message, NULL, NULL, "pouch-space");
}
static int io_failure(int *poisoned, lc_error *e, const char *message) {
  __atomic_store_n(poisoned, 1, __ATOMIC_RELAXED);
  return lc_error_set(e, LC_ERR_TRANSPORT, errno, message, strerror(errno),
                      NULL, "pouch-space");
}
static int io_error(ps_space *s, lc_error *e, const char *message) {
  return io_failure(&s->poisoned, e, message);
}
static int io_healthy(const int *poisoned, lc_error *e) {
  return __atomic_load_n(poisoned, __ATOMIC_RELAXED)
             ? err(e, LC_ERR_TRANSPORT,
                   "container I/O outcome uncertain; close and reopen")
             : LC_OK;
}
static int healthy(ps_space *s, lc_error *e) {
  return io_healthy(&s->poisoned, e);
}
static ssize_t native_read(void *c, int fd, void *p, size_t n, off_t o) {
  (void)c;
  return pread(fd, p, n, o);
}
static ssize_t native_write(void *c, int fd, const void *p, size_t n, off_t o) {
  (void)c;
  return pwrite(fd, p, n, o);
}
void *ps_map_page(int fd, uint64_t offset, ps_mapping *mapping) {
  size_t displacement, length;
  void *base;
  long page_size = sysconf(_SC_PAGESIZE);
  if (page_size <= 0) {
    errno = EINVAL;
    return MAP_FAILED;
  }
  /* File pages stay 4 KiB when kernel mapping alignment is larger. */
  displacement = (size_t)(offset % (uint64_t)page_size);
  length = displacement + PS_PAGE;
  base = mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
              (off_t)(offset - displacement));
  if (base == MAP_FAILED)
    return MAP_FAILED;
  mapping->base = base;
  mapping->length = length;
  return (unsigned char *)base + displacement;
}
void ps_unmap_page(ps_mapping *mapping) {
  (void)munmap(mapping->base, mapping->length);
  mapping->base = NULL;
  mapping->length = 0;
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
static int sync_file_if(ps_space *, int recovery_only, lc_error *);
static void mark_durable(ps_space *, uint64_t sequence);
static int settle_recovery(ps_space *s, lc_error *error) {
  return __atomic_load_n(&s->deferred_recovery, __ATOMIC_ACQUIRE)
             ? sync_file_if(s, 1, error)
             : LC_OK;
}
static int file_transfer(ps_space *s, void *bytes, size_t n, uint64_t offset,
                         int writing, int *private_failure, lc_error *error) {
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
      return io_failure(private_failure ? private_failure : &s->poisoned, error,
                        writing ? "container offset write failed"
                                : "container offset read failed");
    }
    p += result;
    n -= (size_t)result;
    offset += (uint64_t)result;
  }
  return LC_OK;
}
static int transfer(ps_space *s, void *bytes, size_t n, uint64_t offset,
                    int writing, lc_error *error) {
  int rc = writing ? settle_recovery(s, error) : LC_OK;
  return rc == LC_OK ? file_transfer(s, bytes, n, offset, writing, NULL, error)
                     : rc;
}
static int journal_transfer(ps_space *s, uint64_t cursor, void *bytes,
                            size_t count, int writing, lc_error *error) {
  uint64_t offset = JOURNAL_POSITION(cursor);
  size_t first = (size_t)(PS_DATA - offset);
  int rc;
  if (first > count)
    first = count;
  rc = transfer(s, bytes, first, offset, writing, error);
  if (rc == LC_OK && first < count)
    rc = transfer(s, (unsigned char *)bytes + first, count - first,
                  JOURNAL_OFFSET, writing, error);
  return rc;
}
static int zero_file_range(ps_space *s, uint64_t offset, uint64_t bytes) {
  int rc;
  do {
    rc = s->io.zero(s->io.context, s->fd, (off_t)offset, (off_t)bytes);
  } while (rc < 0 && errno == EINTR);
  return rc;
}
static int initialize_range(ps_space *s, uint64_t offset, uint64_t bytes,
                            unsigned char *zero, lc_error *error) {
  size_t n;
  int rc;
  if (!bytes)
    return LC_OK;
  rc = settle_recovery(s, error);
  if (rc != LC_OK)
    return rc;
  if (s->io.zero) {
    rc = zero_file_range(s, offset, bytes);
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
static int sync_file_if(ps_space *s, int recovery_only, lc_error *error) {
  uint64_t raw = 0, recovered = 0;
  int rc, deferred;
  pthread_mutex_lock(&s->sync_io_mutex);
  rc = healthy(s, error);
  deferred = __atomic_load_n(&s->deferred_recovery, __ATOMIC_ACQUIRE);
  if (rc != LC_OK || (recovery_only && !deferred)) {
    pthread_mutex_unlock(&s->sync_io_mutex);
    return rc;
  }
  if (!s->readiness)
    raw = __atomic_load_n(&s->exclusive_raw_writes, __ATOMIC_ACQUIRE);
  if (deferred)
    recovered = __atomic_load_n(&s->pending_sequence, __ATOMIC_ACQUIRE);
  do {
    rc = s->io.sync(s->io.context, s->fd);
  } while (rc < 0 && errno == EINTR);
  __atomic_fetch_add(&s->syncs, 1, __ATOMIC_RELAXED);
  if (rc != 0)
    rc = io_error(s, error, "container sync failed");
  else if (!s->readiness)
    __atomic_store_n(&s->exclusive_raw_durable, raw, __ATOMIC_RELEASE);
  if (rc == LC_OK && deferred) {
    mark_durable(s, recovered);
    __atomic_store_n(&s->deferred_recovery, 0, __ATOMIC_RELEASE);
  }
  pthread_mutex_unlock(&s->sync_io_mutex);
  return rc;
}
static int sync_file(ps_space *s, lc_error *error) {
  return sync_file_if(s, 0, error);
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
 * a global wire prefix. Completed private writes can separately register a
 * ticket that precedes the shared barrier, without publishing a journal frame.
 */
static int durability_acquire(ps_space *s, uint64_t wire, uint64_t ticket,
                              ps_coordination_guard **leader, lc_error *error) {
  uint32_t observed;
  int rc;
  for (;;) {
    observed = ps_readiness_sync_observe(s);
    if (ps_readiness_durability_covers(s, wire, ticket))
      return healthy(s, error);
    rc = ps_coordination_lock(s->coordination, PS_COORDINATION_SYNC, 0, 0,
                              leader, error);
    if (rc != LC_ERR_PENDING)
      return rc;
    lc_error_cleanup(error);
    if (!ps_readiness_sync_wait(s, observed))
      break;
  }
  return ps_coordination_lock(s->coordination, PS_COORDINATION_SYNC, 0, 1,
                              leader, error);
}
static int shared_durable(ps_space *s, uint64_t requested, uint64_t ticket,
                          uint64_t required_wire, uint64_t *completed,
                          int *synced, lc_error *error) {
  ps_coordination_guard *leader = NULL, *guard = NULL;
  ps_readiness_certificate certificate;
  uint64_t target = 0, wire = 0, prepared = 0, needed = required_wire;
  int rc, covered = 0;
  *synced = 0;
  if (requested && !ticket) {
    pthread_mutex_lock(&s->mutex);
    /* Capture one coherent local-to-journal witness before waiting. A peer
     * can acknowledge this prefix even if a later local operation is pending.
     * A local target beyond the published prefix still needs a leader. */
    if (requested > s->own_ready_sequence)
      needed = 0;
    else if (s->own_ready_wire > needed)
      needed = s->own_ready_wire;
    pthread_mutex_unlock(&s->mutex);
  }
  rc = ticket || needed
           ? durability_acquire(s, needed, ticket, &leader, error)
           : ps_coordination_lock(s->coordination, PS_COORDINATION_SYNC, 0, 1,
                                  &leader, error);
  if (rc != LC_OK)
    return rc;
  if (!leader) {
    *completed = requested;
    return LC_OK;
  }
  pthread_mutex_lock(&s->mutex);
  rc = allocation_begin(s, &guard, error);
  if (rc == LC_OK)
    rc = ps_readiness_read(s, &certificate, error);
  if (rc == LC_OK) {
    s->peer_durable = certificate.durable;
    if (certificate.durable >= required_wire &&
        requested <= s->own_ready_sequence &&
        certificate.durable >= s->own_ready_wire) {
      covered = 1;
      target = s->own_ready_sequence;
    } else if (needed && !ticket && certificate.durable >= needed) {
      /* The exact request may have become durable during lock acquisition,
       * even when a later local publication is still outstanding. */
      covered = 1;
      target = requested;
    } else if (ticket && ticket <= certificate.prepared_durable &&
               certificate.durable >= required_wire) {
      covered = 1;
      /* The ticket follows this owner's completed local prefix. A later
       * local completion can still arrive after the peer's barrier. */
      target = requested;
    } else {
      target = __atomic_load_n(&s->pending_sequence, __ATOMIC_ACQUIRE);
      wire = certificate.ready;
      prepared = ps_readiness_issued_tickets(s);
    }
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
        rc = ps_readiness_stabilize(s, wire, 0, 0, prepared, error);
      if (rc == LC_OK && s->peer_durable < wire)
        s->peer_durable = wire;
      rc = allocation_end(s, guard, rc, error);
      pthread_mutex_unlock(&s->mutex);
    }
  }
  ps_coordination_unlock(leader);
  ps_readiness_sync_wake(s);
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
static int wait_ready(ps_space *s, uint64_t sequence, uint64_t ticket,
                      lc_error *error) {
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
    rc = s->readiness ? shared_durable(s, sequence, ticket, 0, &completed,
                                       &synced, error)
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
static int wait_durable(ps_space *s, uint64_t sequence, lc_error *error) {
  return wait_ready(s, sequence, 0, error);
}
static int wait_prepared(ps_space *s, uint64_t sequence, lc_error *error) {
  uint64_t ticket;
  int rc = ps_readiness_prepare_ticket(s, &ticket, error);
  return rc == LC_OK ? wait_ready(s, sequence, ticket, error) : rc;
}
static int reserve_file(ps_space *s, uint64_t begin, uint64_t bytes,
                        lc_error *error) {
  int rc = settle_recovery(s, error);
  if (rc != LC_OK)
    return rc;
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
int ps_internal_immutable_key(ps_key k) {
  return k.kind == PS_STATE || k.kind == PS_COLLECTION ||
         k.kind == PS_CONTROL || k.kind == PS_ROUTING ||
         (k.kind >= PS_QUERY_FIRST && k.kind <= PS_QUERY_LAST) ||
         (k.kind >= PS_KEY_METADATA && k.kind <= PS_NAMESPACE_CHECKPOINT);
}
static uint64_t key_hash(ps_key k) {
  uint64_t h = k.key ^ (k.owner * LC_U64_WORDS(0x9e3779b9UL, 0x7f4a7c15UL)) ^
               (k.kind * LC_U64_WORDS(0xc2b2ae3dUL, 0x27d4eb4fUL));
  h ^= h >> 33;
  h *= LC_U64_WORDS(0xff51afd7UL, 0xed558ccdUL);
  return h ^ (h >> 33);
}
static int same_key(ps_key a, ps_key b) {
  return a.owner == b.owner && a.key == b.key && a.kind == b.kind;
}
static ps_key_slot *key_slot(const ps_key_index *index, ps_key k,
                             uint64_t hash) {
  size_t mask = index->capacity - 1, at = (size_t)hash & mask;
  ps_key_slot *slot = &index->slots[at];
  while (slot->entry &&
         (slot->hash != hash || !same_key(k, slot->entry->key))) {
    at = (at + 1) & mask;
    slot = &index->slots[at];
  }
  return slot;
}
static ps_entry *lookup(ps_space *s, ps_key k) {
  return s->keys.capacity ? key_slot(&s->keys, k, key_hash(k))->entry : NULL;
}
static size_t key_index_limit(size_t capacity) {
  return capacity - capacity / 4;
}
/* Used after the directory walk has proved that all selected keys are distinct.
 */
static void key_index_add_distinct(ps_key_index *index, ps_entry *entry,
                                   uint64_t hash) {
  size_t mask = index->capacity - 1;
  size_t at = (size_t)hash & mask;
  while (index->slots[at].entry)
    at = (at + 1) & mask;
  index->slots[at].entry = entry;
  index->slots[at].hash = hash;
  ++index->count;
}
/* Reserve for selected keys, including current tombstones, before appending
 * or applying journal bytes. Keep a free slot at the end of every probe. */
static int key_index_reserve(ps_space *s, size_t added, lc_error *error) {
  ps_key_slot *slots, slot;
  size_t needed, capacity, i, at;
  if (added > (size_t)-1 - s->keys.count)
    return err(error, LC_ERR_NOMEM, "container key index exceeds local limit");
  needed = s->keys.count + added;
  if (needed <= key_index_limit(s->keys.capacity))
    return LC_OK;
  capacity = s->keys.capacity ? s->keys.capacity : 64U;
  while (key_index_limit(capacity) < needed) {
    if (capacity > (size_t)-1 / sizeof(*slots) / 2)
      return err(error, LC_ERR_NOMEM,
                 "container key index exceeds local limit");
    capacity *= 2;
  }
  slots = lc_calloc_with_allocator(NULL, capacity, sizeof(*slots));
  if (!slots)
    return err(error, LC_ERR_NOMEM, "cannot grow container key index");
  for (i = 0; i < s->keys.capacity; ++i) {
    slot = s->keys.slots[i];
    if (!slot.entry)
      continue;
    at = (size_t)slot.hash & (capacity - 1);
    while (slots[at].entry)
      at = (at + 1) & (capacity - 1);
    slots[at] = slot;
  }
  lc_free_with_allocator(NULL, s->keys.slots);
  s->keys.slots = slots;
  s->keys.capacity = capacity;
  return LC_OK;
}
static void key_index_remove(ps_key_index *index, ps_key key) {
  size_t mask = index->capacity - 1;
  size_t hole = (size_t)(key_slot(index, key, key_hash(key)) - index->slots);
  size_t at, home;
  /* A displaced entry crosses the hole if the hole is nearer its home
   * than its present position, including across the end of the array. */
  for (at = (hole + 1) & mask; index->slots[at].entry; at = (at + 1) & mask) {
    home = (size_t)index->slots[at].hash & mask;
    if (((hole - home) & mask) < ((at - home) & mask)) {
      index->slots[hole] = index->slots[at];
      hole = at;
    }
  }
  index->slots[hole].entry = NULL;
  --index->count;
}
static size_t entry_bytes(const ps_entry *p) {
  size_t header = 64;
  if (p->key.kind == PS_CATALOG)
    return ps_catalog_record_bytes(p);
  if (p->key.kind == PS_STREAM)
    return 80;
  if (p->key.kind == PS_OWNED_RUN)
    header = 80;
  if (p->key.kind == PS_KEY_METADATA)
    header += PS_KEY_SLOT_DESCRIPTOR_EXTRA;
  return header + 24 * p->count;
}
static ps_entry *stream_lookup(ps_space *s, uint64_t token) {
  ps_entry *p;
  for (p = s->buckets->streams[token % BUCKETS]; p; p = p->stream.head->next)
    if (p->token == token)
      return p;
  return NULL;
}
static void stream_register(ps_space *s, ps_entry *p) {
  p->stream.head->next = s->buckets->streams[p->token % BUCKETS];
  s->buckets->streams[p->token % BUCKETS] = p;
}
static void stream_unregister(ps_space *s, ps_entry *p) {
  ps_entry **link = &s->buckets->streams[p->token % BUCKETS];
  while (*link != p)
    link = &(*link)->stream.head->next;
  *link = p->stream.head->next;
}
enum ps_entry_layout { PS_ENTRY_VALUE, PS_ENTRY_HEAD, PS_ENTRY_RUN };
typedef struct ps_entry_allocation {
  struct ps_entry_allocation *next, **link;
  /* Only slabs have an owner and can provide further entries. */
  ps_space *owner;
  struct ps_entry_allocation *available_next, **available_link;
  uint64_t used, auxiliary;
  int temporary;
  enum ps_entry_layout layout;
} ps_entry_allocation;

static void entry_allocation_register(ps_space *s, ps_entry_allocation *block) {
  block->link = &s->entry_allocations;
  block->next = s->entry_allocations;
  if (block->next)
    block->next->link = &block->next;
  s->entry_allocations = block;
}

static void register_entry(ps_space *s, ps_entry *p) {
  if (!p->allocation->link)
    entry_allocation_register(s, p->allocation);
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
static unsigned int value_group(unsigned int kind) {
  return kind < PS_KEY_METADATA ? 0 : kind - PS_KEY_METADATA + 1;
}
static ps_value_bucket *value_bucket(ps_space *s, ps_key key) {
  return &s->buckets->values[value_group(key.kind)][key.owner % BUCKETS];
}
static void value_register(ps_space *s, ps_entry *p) {
  ps_value_bucket *bucket = value_bucket(s, p->key);
  p->value_next = bucket->head;
  p->value_link = &bucket->head;
  if (p->value_next)
    p->value_next->value_link = &p->value_next;
  bucket->head = p;
  ++bucket->count;
}
static void value_unregister(ps_space *s, ps_entry *p) {
  if (!p->value_link)
    return;
  *p->value_link = p->value_next;
  if (p->value_next)
    p->value_next->value_link = p->value_link;
  p->value_link = NULL;
  p->value_next = NULL;
  --value_bucket(s, p->key)->count;
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
  if (!p->stream.run->owned_linked)
    return;
  head = stream_lookup(s, p->key.owner);
  head->stream.head->storage -= entry_storage(p);
  if (p->stream.run->owned_prev)
    p->stream.run->owned_prev->stream.run->owned_next =
        p->stream.run->owned_next;
  else
    head->stream.head->owned_first = p->stream.run->owned_next;
  if (p->stream.run->owned_next)
    p->stream.run->owned_next->stream.run->owned_prev =
        p->stream.run->owned_prev;
  else
    head->stream.head->owned_last = p->stream.run->owned_prev;
  p->stream.run->owned_linked = 0;
}
static void owned_link(ps_space *s, ps_entry *p, ps_entry *old) {
  ps_entry *head = stream_lookup(s, p->key.owner);
  if (!head)
    return; /* checkpoint installation rejects an unlinked run */
  if (old && old->stream.run->owned_linked) {
    head->stream.head->storage -= entry_storage(old);
    p->stream.run->owned_prev = old->stream.run->owned_prev;
    p->stream.run->owned_next = old->stream.run->owned_next;
    old->stream.run->owned_linked = 0;
  } else {
    p->stream.run->owned_prev = head->stream.head->owned_last;
    p->stream.run->owned_next = NULL;
  }
  if (p->stream.run->owned_prev)
    p->stream.run->owned_prev->stream.run->owned_next = p;
  else
    head->stream.head->owned_first = p;
  if (p->stream.run->owned_next)
    p->stream.run->owned_next->stream.run->owned_prev = p;
  else
    head->stream.head->owned_last = p;
  p->stream.run->owned_linked = 1;
  head->stream.head->storage += entry_storage(p);
}
static void retire(ps_space *s, ps_entry *p) {
  value_unregister(s, p);
  if (p->key.kind == PS_CATALOG)
    ps_catalog_unlink(s, p);
  if (p->key.kind == PS_OWNED_RUN)
    owned_unlink(s, p);
  if (p->key.kind == PS_STREAM && !p->deleted) {
    stream_unregister(s, p);
  }
  p->state = 2;
  p->retired_next = s->retired;
  s->retired = p;
  if (!p->deleted) {
    s->metadata -= entry_bytes(p);
    --s->live_entries;
  }
}
static void install(ps_space *s, ps_entry *p, ps_entry *old) {
  uint64_t hash = key_hash(p->key);
  ps_key_slot *slot = key_slot(&s->keys, p->key, hash);
  p->previous = old;
  ps_internal_reserve_install(s, p, old);
  if (p->key.kind == PS_OWNED_RUN && !p->deleted)
    owned_link(s, p, old);
  if (old)
    retire(s, old);
  p->state = 1;
  slot->entry = p;
  slot->hash = hash;
  s->keys.count += old == NULL;
  if (ps_internal_immutable_key(p->key))
    value_register(s, p);
  if (p->key.kind == PS_STREAM && !p->deleted)
    stream_register(s, p);
  if (!p->deleted) {
    s->metadata += entry_bytes(p);
    ++s->live_entries;
  }
}
#define ENTRY_SLAB_SLOTS 32U
#define ENTRY_SLAB_MASK ((((lc_u64)1) << ENTRY_SLAB_SLOTS) - 1)
typedef struct ps_stream_entry {
  ps_entry entry;
  ps_stream_state stream;
} ps_stream_entry;
typedef struct ps_owned_run_entry {
  ps_entry entry;
  ps_owned_run_state run;
} ps_owned_run_entry;
typedef struct ps_entry_slab_slot {
  ps_entry entry;
  ps_extent extent;
} ps_entry_slab_slot;
typedef struct ps_stream_slab_slot {
  ps_stream_entry record;
  ps_extent extent;
} ps_stream_slab_slot;
typedef struct ps_owned_run_slab_slot {
  ps_owned_run_entry record;
  ps_extent extent;
} ps_owned_run_slab_slot;
static const size_t entry_base_bytes[] = {
    sizeof(ps_entry), sizeof(ps_stream_entry), sizeof(ps_owned_run_entry)};
static const size_t entry_slab_stride[] = {sizeof(ps_entry_slab_slot),
                                           sizeof(ps_stream_slab_slot),
                                           sizeof(ps_owned_run_slab_slot)};

static ps_entry *entry_slab_entry(ps_entry_allocation *block,
                                  unsigned int index) {
  unsigned char *slots = (unsigned char *)(block + 1);
  return (ps_entry *)(slots + index * entry_slab_stride[block->layout]);
}

static void entry_allocation_unlink(ps_entry_allocation *block) {
  if (block->link) {
    *block->link = block->next;
    if (block->next)
      block->next->link = block->link;
  }
}
static ps_entry_allocation **
entry_available_head(ps_space *s, enum ps_entry_layout layout, int temporary) {
  ps_entry_slabs *slabs = &s->value_entries;
  if (layout == PS_ENTRY_HEAD)
    slabs = &s->stream_entries;
  else if (layout == PS_ENTRY_RUN)
    slabs = &s->owned_run_entries;
  return temporary ? &slabs->temporary : &slabs->available;
}
static void entry_available_link(ps_entry_allocation *block) {
  ps_entry_allocation **head =
      entry_available_head(block->owner, block->layout, block->temporary);
  block->available_link = head;
  block->available_next = *head;
  if (*head)
    (*head)->available_link = &block->available_next;
  *head = block;
}
static void entry_available_unlink(ps_entry_allocation *block) {
  if (!block->available_link)
    return;
  *block->available_link = block->available_next;
  if (block->available_next)
    block->available_next->available_link = block->available_link;
  block->available_link = NULL;
}
static enum ps_entry_layout entry_layout(unsigned int kind) {
  if (kind == PS_STREAM)
    return PS_ENTRY_HEAD;
  if (kind == PS_OWNED_RUN)
    return PS_ENTRY_RUN;
  return PS_ENTRY_VALUE;
}
static void entry_initialize(ps_entry *p, ps_entry_allocation *block,
                             unsigned int kind, ps_extent *extent) {
  if (block->layout == PS_ENTRY_HEAD)
    p->stream.head = &((ps_stream_entry *)p)->stream;
  else if (block->layout == PS_ENTRY_RUN)
    p->stream.run = &((ps_owned_run_entry *)p)->run;
  p->key.kind = kind;
  p->allocation = block;
  if (extent) {
    p->extents = extent;
    p->embedded_extents = 1;
  }
}
ps_entry *ps_internal_entry_new(unsigned int kind, size_t extent_count) {
  enum ps_entry_layout layout = entry_layout(kind);
  size_t base = entry_base_bytes[layout];
  ps_entry_allocation *block = lc_calloc_with_allocator(
      NULL, 1, sizeof(*block) + base + extent_count * sizeof(ps_extent));
  ps_entry *p;
  if (!block)
    return NULL;
  block->layout = layout;
  p = (ps_entry *)(block + 1);
  entry_initialize(p, block, kind,
                   extent_count ? (ps_extent *)((unsigned char *)p + base)
                                : NULL);
  return p;
}
/* The local allocator mutex, or private recovery ownership, protects slots.
 * A released slot can be reused while a sibling is retained by a reader.
 * Keep stream state out of the slots used by small values. */
static ps_entry *entry_slab_take(ps_space *s, unsigned int kind,
                                 int temporary) {
  enum ps_entry_layout layout = entry_layout(kind);
  size_t base = entry_base_bytes[layout];
  ps_entry_allocation *block = *entry_available_head(s, layout, temporary);
  ps_entry *entry;
  unsigned int index;
  if (!block) {
    size_t stride = entry_slab_stride[layout];
    block = lc_alloc_with_allocator(NULL,
                                    sizeof(*block) + ENTRY_SLAB_SLOTS * stride);
    if (!block)
      return NULL;
    memset(block, 0, sizeof(*block));
    block->owner = s;
    block->temporary = temporary;
    block->layout = layout;
    entry_allocation_register(s, block);
    entry_available_link(block);
  }
  index = (unsigned int)__builtin_ctzll(~block->used & ENTRY_SLAB_MASK);
  block->used |= ((lc_u64)1) << index;
  if (block->used == ENTRY_SLAB_MASK)
    entry_available_unlink(block);
  entry = entry_slab_entry(block, index);
  memset(entry, 0, base);
  entry_initialize(entry, block, kind,
                   (ps_extent *)((unsigned char *)entry + base));
  return entry;
}
static ps_entry *space_entry_new(ps_space *s, unsigned int kind, size_t count,
                                 int temporary) {
  if ((kind != PS_KEY_METADATA && kind != PS_KEY_DECISION) || count > 1)
    return ps_internal_entry_new(kind, count);
  return entry_slab_take(s, kind, temporary);
}
static int projection_borrowed(const ps_space *s, const ps_entry *p) {
  return p && s->projection_resident &&
         p->allocation->owner == s->projection_resident;
}
/* Borrowed entries are present only in the draft key index. Replay needs
 * independent links for a predecessor it retires or makes visible again. */
static int projection_copy_predecessor(ps_space *s, ps_key key,
                                       lc_error *error) {
  ps_key_slot *slot = key_slot(&s->keys, key, key_hash(key));
  ps_entry *old = slot->entry, *copy;
  if (!projection_borrowed(s, old))
    return LC_OK;
  copy = space_entry_new(s, key.kind, 1, 1);
  if (!copy)
    return err(error, LC_ERR_NOMEM, "cannot stage journal predecessor");
  copy->key = key;
  copy->token = old->token;
  copy->bytes = old->bytes;
  copy->count = 1;
  copy->extents[0] = old->extents[0];
  copy->projection = old;
  copy->state = 1;
  slot->entry = copy;
  register_entry(s, copy);
  value_register(s, copy);
  return LC_OK;
}
static uint64_t entry_slab_bit(const ps_entry *p) {
  const void *slots = p->allocation + 1;
  if (p->allocation->layout == PS_ENTRY_HEAD)
    return ((lc_u64)1) << ((const ps_stream_slab_slot *)p -
                           (const ps_stream_slab_slot *)slots);
  if (p->allocation->layout == PS_ENTRY_RUN)
    return ((lc_u64)1) << ((const ps_owned_run_slab_slot *)p -
                           (const ps_owned_run_slab_slot *)slots);
  return ((lc_u64)1) << ((const ps_entry_slab_slot *)p -
                         (const ps_entry_slab_slot *)slots);
}
void ps_internal_entry_auxiliary(ps_entry *p) {
  if (p->allocation->owner)
    /* Independent writers can add deletion reserves to private slots. */
    (void)__atomic_fetch_or(&p->allocation->auxiliary, entry_slab_bit(p),
                            __ATOMIC_RELAXED);
}
static void entry_fields_free(ps_entry *p) {
  if (p->catalog)
    ps_catalog_free_record(p);
  lc_free_with_allocator(NULL, p->deletion_reserve);
  if (p->allocation->layout == PS_ENTRY_RUN && p->stream.run->mapped) {
    lc_free_with_allocator(NULL, p->stream.run->ranges);
    lc_free_with_allocator(NULL, p->stream.run->range_bitmap);
    lc_free_with_allocator(NULL, p->stream.run->range_rank);
  }
  if (!p->embedded_extents)
    lc_free_with_allocator(NULL, p->extents);
}
void ps_internal_entry_free(ps_entry *p) {
  ps_entry_allocation *block;
  uint64_t bit, before;
  if (!p)
    return;
  block = p->allocation;
  if (!block->owner)
    entry_fields_free(p);
  else {
    bit = entry_slab_bit(p);
    if (__atomic_load_n(&block->auxiliary, __ATOMIC_RELAXED) & bit) {
      entry_fields_free(p);
      (void)__atomic_fetch_and(&block->auxiliary, ~bit, __ATOMIC_RELAXED);
    }
    before = block->used;
    block->used &= ~bit;
    if (block->used) {
      if (before == ENTRY_SLAB_MASK)
        entry_available_link(block);
      return;
    }
    entry_available_unlink(block);
  }
  entry_allocation_unlink(block);
  lc_free_with_allocator(NULL, block);
}
static int allocation_lock(ps_space *s, ps_coordination_guard **guard, int wait,
                           lc_error *error) {
  int rc;
  *guard = NULL;
  if (!ps_coordination_shared(s->coordination) || s->allocation_guard)
    return LC_OK;
  rc = ps_coordination_lock(s->coordination, PS_COORDINATION_ALLOCATION, 0,
                            wait, guard, error);
  if (rc == LC_OK) {
    s->allocation_guard = *guard;
    s->allocation_ranges_checked = 0;
    s->credit_totals_checked = 0;
  }
  return rc;
}
static int allocation_begin(ps_space *s, ps_coordination_guard **guard,
                            lc_error *error) {
  return allocation_lock(s, guard, 1, error);
}
static int allocation_end(ps_space *s, ps_coordination_guard *guard, int rc,
                          lc_error *error) {
  if (guard) {
    ps_internal_budget_publish_record(s);
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
  checkpoint_preview preview, *prepared = NULL;
  int rc = healthy(s, error);
  *guard = NULL;
  if (rc == LC_OK && !verify_disk && !s->allocation_guard &&
      ps_readiness_preview(s, &preview.certificate)) {
    preview_checkpoint(s, &preview);
    prepared = &preview;
  }
  if (rc == LC_OK)
    rc = allocation_begin(s, guard, error);
  if (rc == LC_OK && *guard)
    rc = canonical_refresh(s, prepared, verify_disk, error);
  if (prepared) {
    projection_destroy(preview.view);
    lc_error_cleanup(&preview.error);
  }
  return rc;
}
int ps_internal_capture_begin(ps_space *s, ps_coordination_guard **guard,
                              lc_error *error) {
  return capture_begin(s, guard, 0, error);
}
static void observe_local_durability(ps_space *s) {
  if (s->readiness && s->peer_durable >= s->own_ready_wire &&
      __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE) <
          s->own_ready_sequence)
    mark_durable(s, s->own_ready_sequence);
}
int ps_internal_observe_begin(ps_space *s, ps_coordination_guard **guard,
                              lc_error *error) {
  int rc = healthy(s, error);
  *guard = NULL;
  if (rc != LC_OK)
    return rc;
  if (!ps_readiness_resident(s))
    return ps_internal_capture_begin(s, guard, error);
  observe_local_durability(s);
  return LC_OK;
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
static int take_physical_witness(ps_space *s, uint64_t offset, uint64_t bytes,
                                 lc_error *error) {
  int first = s->physical_claim_bytes == 0, rc;
  if (first) {
    rc = ps_coordination_range(s->coordination, MEMBER_PIN_PRESENCE_OFFSET, 1,
                               1, error);
    if (rc != LC_OK)
      return rc;
  }
  ps_readiness_witness_pages(s, offset, bytes, 1);
  rc = ps_coordination_range(s->coordination, offset, bytes, 1, error);
  if (rc == LC_OK)
    s->physical_claim_bytes += bytes;
  else if (first &&
           ps_coordination_range(s->coordination, MEMBER_PIN_PRESENCE_OFFSET, 1,
                                 0, NULL) != LC_OK) {
    lc_error_cleanup(error);
    rc = healthy(s, error);
  }
  return rc;
}
static int physical_witness(ps_space *s, uint64_t offset, uint64_t bytes,
                            lc_error *error) {
  if (s->readiness && !s->allocation_guard)
    return err(error, LC_ERR_PROTOCOL,
               "member physical claim requires allocator authority");
  return take_physical_witness(s, offset, bytes, error);
}
static int physical_claim(ps_space *s, uint64_t offset, uint64_t bytes,
                          lc_error *error) {
  int rc = physical_witness(s, offset, bytes, error);
  if (rc == LC_OK)
    ps_readiness_zero_pages(s, offset, bytes, 0);
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
             : ps_coordination_range(s->coordination,
                                     MEMBER_PIN_PRESENCE_OFFSET, 1, 0, error);
}
static int metadata_claim_range(ps_space *s, uint64_t offset, uint64_t bytes,
                                lc_error *error) {
  int rc = physical_witness(s, offset, bytes, error);
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
    /* The concrete pool reserves free pages without modifying their bytes. */
    rc = physical_witness(s, run->offset, n, error);
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
static uint64_t credit_record_slot(uint64_t offset) {
  return PS_CREDIT_RECORD_BASE + offset / PS_PAGE;
}
int ps_internal_budget_prepare_record(ps_space *s, lc_error *error) {
  ps_run **link, **last = NULL, *source, *run;
  void *mapping;
  int rc;
  if (!s->allocation_guard || s->credit_record ||
      ps_readiness_credit_registered(s) ||
      (!s->reserved_metadata && !s->reserved_entries))
    return LC_OK;
  /* Keep contiguous prepared payload at the start of common intervals. */
  for (link = &s->free; *link; link = &(*link)->next)
    if (!(*link)->claimed)
      last = link;
  if (!last)
    return err(error, LC_ERR_INVALID, "container budget credit page exhausted");
  run = lc_calloc_with_allocator(NULL, 1, sizeof(*run));
  if (!run)
    return err(error, LC_ERR_NOMEM, "cannot allocate budget credit record");
  source = *last;
  source->bytes -= PS_PAGE;
  run->offset = source->offset + source->bytes;
  run->bytes = PS_PAGE;
  s->free_bytes -= PS_PAGE;
  if (!source->bytes) {
    *last = source->next;
    lc_free_with_allocator(NULL, source);
  }
  rc = metadata_claim_range(s, run->offset, run->bytes, error);
  if (rc == LC_OK) {
    ps_readiness_zero_pages(s, run->offset, run->bytes, 0);
    mapping = ps_map_page(s->fd, run->offset, &s->credit_mapping);
    if (mapping == MAP_FAILED)
      rc = err(error, LC_ERR_TRANSPORT, "cannot map budget credit record");
    else {
      uint64_t *words = mapping;
      words[0] = words[1] = 0;
      rc = ps_coordination_range(s->coordination,
                                 credit_record_slot(run->offset), 1, 1, error);
      if (rc == LC_OK) {
        s->credit_record = run;
        s->credit_words = words;
        ps_readiness_credit_note_spill(s);
        return LC_OK;
      }
      ps_unmap_page(&s->credit_mapping);
    }
    (void)metadata_release_range(s, run->offset, run->bytes, NULL);
  }
  free_insert(s, run);
  return rc;
}
void ps_internal_budget_publish_record(ps_space *s) {
  ps_run *run = s->credit_record;
  ps_coordination_guard *guard = NULL;
  if (__atomic_load_n(&s->poisoned, __ATOMIC_RELAXED))
    return;
  if (ps_readiness_credit_registered(s)) {
    if (!ps_readiness_credit_publish(s)) {
      if (allocation_begin(s, &guard, NULL) == LC_OK)
        (void)allocation_end(s, guard, LC_OK, NULL);
      else
        __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
    }
    return;
  }
  if (!run)
    return;
  if (s->reserved_metadata || s->reserved_entries) {
    if (__atomic_load_n(s->credit_words, __ATOMIC_RELAXED) ==
            s->reserved_metadata &&
        __atomic_load_n(s->credit_words + 1, __ATOMIC_RELAXED) ==
            s->reserved_entries)
      return;
    if (s->allocation_guard) {
      __atomic_store_n(s->credit_words, s->reserved_metadata, __ATOMIC_RELEASE);
      __atomic_store_n(s->credit_words + 1, s->reserved_entries,
                       __ATOMIC_RELEASE);
    } else if (allocation_begin(s, &guard, NULL) == LC_OK) {
      /* An out-of-capture partial shrink changes the same two-word record
       * a peer may be reading. Publish both words under allocator authority. */
      (void)allocation_end(s, guard, LC_OK, NULL);
    } else {
      __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
    }
    return;
  }
  /* Leave the record unchanged. A peer may already have observed its slot
   * under allocation authority. That peer can still read the old promise:
   * reuse of the physical page needs the same allocator guard. */
  if (ps_coordination_range(s->coordination, credit_record_slot(run->offset), 1,
                            0, NULL) != LC_OK ||
      metadata_release_range(s, run->offset, run->bytes, NULL) != LC_OK)
    return;
  ps_unmap_page(&s->credit_mapping);
  s->credit_words = NULL;
  s->credit_record = NULL;
  free_insert(s, run);
}
static int peer_credits_read(ps_space *s, uint64_t low, uint64_t high,
                             uint64_t *metadata, uint64_t *entries,
                             lc_error *error) {
  uint64_t first, last, offset, words[2];
  int rc;
  while (low < high) {
    rc = ps_coordination_conflict(s->coordination, low, high - low, &first,
                                  &last, error);
    if (rc != LC_OK || first == last)
      return rc;
    offset = (first - PS_CREDIT_RECORD_BASE) * PS_PAGE;
    if (last - first != 1 || offset < PS_DATA || offset >= s->capacity)
      return err(error, LC_ERR_PROTOCOL, "invalid budget credit witness");
    rc = transfer(s, words, sizeof(words), offset, 0, error);
    if (rc != LC_OK)
      return rc;
    if (words[0] > LC_U64_MAX - *metadata || words[1] > LC_U64_MAX - *entries)
      return err(error, LC_ERR_PROTOCOL,
                 "shared budget credit totals overflow");
    *metadata += words[0];
    *entries += words[1];
    /* GETLK can return any witness. Recurse into the smaller residual to
     * bound stack use by the address width, even with many live writers. */
    if (first - low < high - last) {
      rc = peer_credits_read(s, low, first, metadata, entries, error);
      low = last;
    } else {
      rc = peer_credits_read(s, last, high, metadata, entries, error);
      high = first;
    }
    if (rc != LC_OK)
      return rc;
  }
  return LC_OK;
}
static int peer_credits(ps_space *s, lc_error *error) {
  int rc;
  if (!s->allocation_guard || s->credit_totals_checked)
    return LC_OK;
  s->peer_metadata_credits = s->peer_entry_credits = 0;
  rc = ps_readiness_credit_collect(s, &s->peer_metadata_credits,
                                   &s->peer_entry_credits, error);
  if (rc == LC_OK && ps_readiness_credit_spilled(s))
    rc = peer_credits_read(s, credit_record_slot(PS_DATA),
                           PS_TRANSACTION_GUARD_BASE, &s->peer_metadata_credits,
                           &s->peer_entry_credits, error);
  s->credit_totals_checked = rc == LC_OK;
  return rc;
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
    ps_readiness_zero_pages(s, offset, funded, 0);
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
  return count == CELLS ? LC_U64_MAX : ((((lc_u64)1) << count) - 1) << at;
}
/* Consume the lowest contiguous span in a nonempty free-cell mask. */
static size_t cell_span(uint64_t *free_cells, size_t *at) {
  uint64_t tail;
  size_t count;
  *at = (size_t)__builtin_ctzll(*free_cells);
  tail = *free_cells >> *at;
  count = tail == LC_U64_MAX ? CELLS : (size_t)__builtin_ctzll(~tail);
  *free_cells &= ~cell_mask(*at, count);
  return count;
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
static int entry_pin(ps_space *s, ps_entry *p, int provisional,
                     lc_error *error) {
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
      rc = provisional ? take_physical_witness(s, offset, bytes, error)
                       : physical_claim(s, offset, bytes, error);
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
int ps_internal_entry_pin(ps_space *s, ps_entry *p, lc_error *error) {
  return entry_pin(s, p, 0, error);
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
  for (p = s->buckets->payload[(offset / PS_PAGE) % BUCKETS]; p;
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
/* One bin per largest contiguous free cell span. Both binning and placement
 * can skip occupied cells and consume each contiguous hole at once. */
static void payload_available(ps_space *s, ps_payload_page *p) {
  uint64_t free_cells = ~p->occupied;
  size_t at, span, largest = 0;
  payload_unavailable(p);
  if (p->external_pins)
    return;
  while (free_cells) {
    span = cell_span(&free_cells, &at);
    if (span > largest)
      largest = span;
  }
  if (largest) {
    p->available_next = s->buckets->available[largest];
    p->available_link = &s->buckets->available[largest];
    if (p->available_next)
      p->available_next->available_link = &p->available_next;
    s->buckets->available[largest] = p;
  }
}
static void payload_register(ps_space *s, ps_payload_page *p) {
  size_t hash = (size_t)((p->run.offset / PS_PAGE) % BUCKETS);
  p->next = s->payload_pages;
  p->link = &s->payload_pages;
  if (p->next)
    p->next->link = &p->next;
  s->payload_pages = p;
  p->hash_next = s->buckets->payload[hash];
  s->buckets->payload[hash] = p;
  ++s->packed_pages;
}
static void payload_unregister(ps_space *s, ps_payload_page *p) {
  ps_payload_page **link;
  payload_unavailable(p);
  link = &s->buckets->payload[(p->run.offset / PS_PAGE) % BUCKETS];
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
typedef struct recovery_range {
  uint64_t offset, bytes;
} recovery_range;
#define REBUILD_BITMAP_WORDS 64U
#define REBUILD_BITMAP_CAPACITY (REBUILD_BITMAP_WORDS * 64U * PS_PAGE)
typedef struct payload_rebuild {
  recovery_range *ranges;
  size_t count, capacity;
  int packed_changed, initial;
  uint64_t pages[REBUILD_BITMAP_WORDS];
} payload_rebuild;

/* A packed page is marked once and its cells are checked separately.
 * Regular extents and owned stream runs mark every page in their range. */
static int payload_mark_pages(payload_rebuild *build, uint64_t offset,
                              uint64_t bytes, lc_error *error) {
  unsigned int page = (unsigned int)(offset / PS_PAGE);
  unsigned int end = page + (unsigned int)(bytes / PS_PAGE), count, bit;
  uint64_t mask, *word;
  while (page < end) {
    bit = page % 64U;
    count = 64U - bit;
    if (count > end - page)
      count = end - page;
    mask = (LC_U64_MAX >> (64U - count)) << bit;
    word = &build->pages[page / 64U];
    if (*word & mask)
      return err(error, LC_ERR_PROTOCOL, "overlapping container ownership");
    *word |= mask;
    page += count;
  }
  return LC_OK;
}
static int payload_exclude(ps_space *s, payload_rebuild *build, uint64_t offset,
                           uint64_t bytes, lc_error *error) {
  recovery_range *ranges;
  size_t capacity;
  if (!build)
    return exclude_run(s, offset, bytes, error);
  if (s->capacity <= REBUILD_BITMAP_CAPACITY) {
    if (offset < PS_DATA || offset > s->capacity ||
        bytes > s->capacity - offset || offset % PS_PAGE || bytes % PS_PAGE)
      return err(error, LC_ERR_PROTOCOL, "invalid payload page ownership");
    return payload_mark_pages(build, offset, bytes, error);
  }
  if (build->count == build->capacity) {
    if (build->capacity > (size_t)-1 / sizeof(*ranges) / 2)
      return err(error, LC_ERR_NOMEM, "payload ownership list too large");
    capacity = build->capacity ? build->capacity * 2 : 64;
    ranges = lc_realloc_with_allocator(NULL, build->ranges,
                                       capacity * sizeof(*ranges));
    if (!ranges)
      return err(error, LC_ERR_NOMEM, "cannot collect payload ownership");
    build->ranges = ranges;
    build->capacity = capacity;
  }
  build->ranges[build->count].offset = offset;
  build->ranges[build->count++].bytes = bytes;
  return LC_OK;
}
static int payload_claim(ps_space *s, const ps_extent *extent,
                         payload_rebuild *build, lc_error *error) {
  ps_payload_page *p;
  uint64_t mask;
  int rc;
  if (!(extent->flags & EXTENT_PACKED))
    return payload_exclude(s, build, extent->offset, aligned(extent->bytes),
                           error);
  mask = extent_mask(extent);
  p = payload_find(s, extent->offset);
  if (!p) {
    p = lc_calloc_with_allocator(NULL, 1, sizeof(*p));
    if (!p)
      return err(error, LC_ERR_NOMEM, "cannot recover packed payload page");
    p->run.offset = extent->offset / PS_PAGE * PS_PAGE;
    p->run.bytes = PS_PAGE;
    rc = payload_exclude(s, build, p->run.offset, PS_PAGE, error);
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
  if (!build)
    payload_available(s, p);
  else
    build->packed_changed = 1;
  return LC_OK;
}
static int payload_claim_entry(ps_space *s, const ps_entry *p,
                               payload_rebuild *build, lc_error *error) {
  size_t i;
  int rc = LC_OK;
  for (i = 0; rc == LC_OK && i < p->count; ++i)
    rc = payload_claim(s, &p->extents[i], build, error);
  return rc;
}
static int recovery_range_order(const void *a, const void *b) {
  uint64_t left = ((const recovery_range *)a)->offset;
  uint64_t right = ((const recovery_range *)b)->offset;
  return left < right ? -1 : left != right;
}
static int payload_subtract(ps_space *s, ps_run ***cursor, uint64_t offset,
                            uint64_t bytes, lc_error *error) {
  ps_run **link = *cursor;
  while (*link && (*link)->offset + (*link)->bytes <= offset)
    link = &(*link)->next;
  *cursor = link;
  return exclude_selected(link, &s->free_bytes, offset, bytes, error);
}
/* Locate a used page, or a free page with flip set to LC_U64_MAX. The
 * caller supplies a start strictly inside the compact bitmap. */
static unsigned int payload_bitmap_next(const uint64_t *pages, unsigned int at,
                                        unsigned int limit, uint64_t flip) {
  unsigned int word = at / 64U;
  uint64_t bits = (pages[word] ^ flip) & (LC_U64_MAX << (at % 64U));
  while (!bits) {
    if (++word * 64U >= limit)
      return limit;
    bits = pages[word] ^ flip;
  }
  at = word * 64U + (unsigned int)__builtin_ctzll(bits);
  return at < limit ? at : limit;
}
/* Initial recovery collects every selected common page in one bitmap. Build
 * the complement directly, starting with the node from initial_free. */
static int payload_build_initial_free(ps_space *s, const payload_rebuild *b,
                                      lc_error *error) {
  ps_run *spare = s->free, *run, **link = &s->free;
  unsigned int first = PS_DATA / PS_PAGE, last;
  unsigned int limit = (unsigned int)(s->capacity / PS_PAGE);
  int clean = spare->clean;
  s->free = NULL;
  s->free_bytes = 0;
  while (first < limit) {
    first = payload_bitmap_next(b->pages, first, limit, LC_U64_MAX);
    if (first == limit)
      break;
    last = payload_bitmap_next(b->pages, first, limit, 0);
    run = spare ? spare : lc_calloc_with_allocator(NULL, 1, sizeof(*run));
    if (!run)
      return err(error, LC_ERR_NOMEM,
                 "cannot allocate recovered free interval");
    spare = NULL;
    run->offset = (uint64_t)first * PS_PAGE;
    run->bytes = (uint64_t)(last - first) * PS_PAGE;
    run->clean = clean;
    *link = run;
    link = &run->next;
    s->free_bytes += run->bytes;
    first = last;
  }
  lc_free_with_allocator(NULL, spare);
  return LC_OK;
}
static int payload_rebuild_bitmap(ps_space *s, const payload_rebuild *build,
                                  lc_error *error) {
  ps_run **link = &s->free;
  uint64_t word, suffix;
  uint64_t first = 0, end = 0, offset;
  unsigned int i, bit, count;
  unsigned int words = (unsigned int)(s->capacity / PS_PAGE + 63U) / 64U;
  int rc;
  for (i = PS_DATA / PS_PAGE / 64U; i < words; ++i) {
    word = build->pages[i];
    while (word) {
      bit = (unsigned int)__builtin_ctzll(word);
      suffix = word >> bit;
      count =
          suffix == LC_U64_MAX ? 64U : (unsigned int)__builtin_ctzll(~suffix);
      offset = (uint64_t)(i * 64U + bit) * PS_PAGE;
      if (offset != end) {
        if (end) {
          rc = payload_subtract(s, &link, first, end - first, error);
          if (rc != LC_OK)
            return rc;
        }
        first = offset;
      }
      end = offset + (uint64_t)count * PS_PAGE;
      word &= ~((LC_U64_MAX >> (64U - count)) << bit);
    }
  }
  return end ? payload_subtract(s, &link, first, end - first, error) : LC_OK;
}
/* Compact initial recovery can publish the bitmap's gaps directly. Journal
 * replay and sparse roots subtract ordered claims from existing intervals.
 * Each packed page contributes one range and one bin update. */
static int payload_rebuild_free(ps_space *s, payload_rebuild *build,
                                lc_error *error) {
  ps_run **link = &s->free;
  ps_payload_page *page;
  size_t i;
  int rc = LC_OK;
  if (s->capacity <= REBUILD_BITMAP_CAPACITY)
    rc = build->initial ? payload_build_initial_free(s, build, error)
                        : payload_rebuild_bitmap(s, build, error);
  if (build->count > 1)
    qsort(build->ranges, build->count, sizeof(*build->ranges),
          recovery_range_order);
  for (i = 0; rc == LC_OK && i < build->count; ++i) {
    const recovery_range *range = &build->ranges[i];
    rc = payload_subtract(s, &link, range->offset, range->bytes, error);
  }
  if (rc == LC_OK && build->packed_changed)
    for (page = s->payload_pages; page; page = page->next)
      payload_available(s, page);
  return rc;
}
/* Recovery prepends entries and retains their physical ownership even when
 * a later frame supersedes them. The checkpoint boundary can already own
 * every entry at and after end. */
static int recover_payload_ownership(ps_space *s, const ps_entry *end, int live,
                                     lc_error *error) {
  payload_rebuild build = {0};
  ps_entry *p;
  int rc = LC_OK;
  if (s->entries == end)
    return LC_OK;
  for (p = s->entries; rc == LC_OK && p != end; p = p->all_next) {
    rc = payload_claim_entry(s, p, &build, error);
    if (!live)
      p->previous = NULL;
  }
  if (rc == LC_OK)
    rc = payload_rebuild_free(s, &build, error);
  lc_free_with_allocator(NULL, build.ranges);
  return rc;
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
static int physical_presence(ps_space *s, int *present, lc_error *error) {
  /* One kernel observation covers both kinds of presence even during a
   * handoff. Intervening coordination locks can conservatively request a
   * scan on shared roots. Exclusive admission only needs imported pins. */
  uint64_t bytes = ps_coordination_shared(s->coordination)
                       ? MEMBER_PIN_PRESENCE_OFFSET - PIN_PRESENCE_OFFSET + 1
                       : 1;
  return pin_conflict(s, PIN_PRESENCE_OFFSET, bytes, present, error);
}
static int entry_foreign_pinned(ps_space *s, const ps_entry *p) {
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
                        ps_space *witnesses, lc_error *error) {
  ps_run *run, *held, **link = head;
  uint64_t first, last;
  int rc;
  while (*link) {
    run = *link;
    if (run->claimed ||
        (witnesses &&
         !ps_readiness_witness_possible(witnesses, run->offset, run->bytes))) {
      link = &run->next;
      continue;
    }
    rc = ps_coordination_conflict(s->coordination, run->offset, run->bytes,
                                  &first, &last, error);
    if (rc != LC_OK)
      return rc;
    if (first == last) {
      /* A provisional reader must certify its selection after witnessing
       * it. Free pages cannot belong to a successfully certified read.
       * The allocator excludes other new member claims during this probe. */
      if (witnesses)
        ps_readiness_witness_pages(witnesses, run->offset, run->bytes, 0);
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
static int pins_recover(ps_space *s, ps_space *owner, lc_error *error) {
  ps_payload_page *page;
  ps_space *witnesses = NULL;
  int rc, present;
  if (!s->range_pins)
    return LC_OK;
  rc = physical_presence(s, &present, error);
  if (rc != LC_OK || !present)
    return rc;
  if (owner->readiness) {
    rc = pin_conflict(s, PIN_PRESENCE_OFFSET, 1, &present, error);
    if (rc != LC_OK)
      return rc;
    if (!present)
      witnesses = owner;
  }
  for (page = s->payload_pages; page; page = page->next) {
    if (witnesses &&
        !ps_readiness_witness_possible(witnesses, page->run.offset, PS_PAGE))
      continue;
    rc = pin_conflict(s, page->run.offset, PS_PAGE, &present, error);
    if (rc != LC_OK)
      return rc;
    if (present)
      quarantine_page(s, page);
  }
  rc = foreign_pool(s, &s->free, &s->free_bytes, witnesses, error);
  return rc == LC_OK ? foreign_pool(s, &s->metadata_free,
                                    &s->metadata_free_bytes, NULL, error)
                     : rc;
}
/* Quarantine is a conservative positive observation. A live participant can
 * release ranges, or exit without notifying us. Advance through both lists
 * on ordinary work, and prove all remaining candidates on capacity pressure
 * or explicit collection. The common free pool and selected packed pages
 * still undergo conflict checks. Resume at a link to bound each pass.
 * New quarantines enter at the head, ahead of a pass already in progress. */
#define PIN_PROBES 2
static int quarantine_partition(ps_run *run, uint64_t first, uint64_t last,
                                lc_error *error) {
  uint64_t end = run->offset + run->bytes;
  ps_run *before = NULL, *after = NULL;
  if (run->offset < first) {
    before = lc_calloc_with_allocator(NULL, 1, sizeof(*before));
    if (!before)
      return err(error, LC_ERR_NOMEM, "cannot split quarantined interval");
    before->offset = run->offset;
    before->bytes = first - run->offset;
  }
  if (last < end) {
    after = lc_calloc_with_allocator(NULL, 1, sizeof(*after));
    if (!after) {
      lc_free_with_allocator(NULL, before);
      return err(error, LC_ERR_NOMEM, "cannot split quarantined interval");
    }
    after->offset = last;
    after->bytes = end - last;
    after->next = run->next;
    run->next = after;
  }
  if (before) {
    before->next = run->next;
    run->next = before;
  }
  run->offset = first;
  run->bytes = last - first;
  return LC_OK;
}
static int pins_collect_locked(ps_space *s, int present, int full,
                               lc_error *error) {
  ps_run **link, *run;
  ps_payload_page *page, **pages;
  uint64_t first, last;
  size_t probes;
  int held, rc = LC_OK;
  full = full || !present;
  link = !full && s->pinned_free_probe ? s->pinned_free_probe : &s->pinned_free;
  for (probes = 0; *link && (full || probes < PIN_PROBES); ++probes) {
    run = *link;
    first = last = 0;
    if (present)
      rc = ps_coordination_conflict(s->coordination, run->offset, run->bytes,
                                    &first, &last, error);
    if (rc != LC_OK)
      break;
    if (first != last) {
      first = first / PS_PAGE * PS_PAGE;
      last = aligned(last);
      /* An OFD can shrink a formerly coalesced range. Requery both
       * residuals on the full pass before returning any part to allocation.
       * Ordinary admission retains its fixed probe budget. */
      if (full && (first != run->offset || last - first != run->bytes)) {
        rc = quarantine_partition(run, first, last, error);
        if (rc != LC_OK)
          break;
      }
      link = &run->next;
      continue;
    }
    if (run->offset < PS_DATA && s->metadata_materialized &&
        metadata_claim_range(s, run->offset, run->bytes, NULL) != LC_OK) {
      link = &run->next;
      continue;
    }
    *link = run->next;
    s->pinned_free_bytes -= run->bytes;
    free_insert(s, run);
  }
  s->pinned_free_probe = *link ? link : NULL;
  if (rc != LC_OK)
    return rc;
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
  return healthy(s, error);
}
static int pins_reclaim_locked(ps_space *s, lc_error *error) {
  int present, rc;
  if (!s->pinned_free && !s->pinned_pages)
    return LC_OK;
  rc = physical_presence(s, &present, error);
  if (rc != LC_OK)
    return rc;
  return pins_collect_locked(s, present, 1, error);
}
/* A missing witness can mean a peer selected the range in a new checkpoint
 * or publication. A no-work checkpoint may sweep only against a certified
 * current view. Its next admission or refresh handles a newer view. */
static int pins_reclaim_current(ps_space *s, lc_error *error) {
  ps_coordination_guard *guard = NULL;
  ps_readiness_certificate certificate;
  int rc;
  if (!s->pinned_free && !s->pinned_pages)
    return LC_OK;
  rc = allocation_begin(s, &guard, error);
  if (rc == LC_OK && s->allocation_guard)
    rc = ps_readiness_read(s, &certificate, error);
  if (rc == LC_OK && s->allocation_guard &&
      (certificate.pending || certificate.root != s->generation ||
       certificate.ready != s->sequence))
    return allocation_end(s, guard, LC_OK, error);
  if (rc == LC_OK)
    rc = pins_reclaim_locked(s, error);
  return allocation_end(s, guard, rc, error);
}
static int allocation_refresh(ps_space *s, lc_error *error) {
  int rc = canonical_observe(s, error);
  return rc == LC_OK ? allocation_ranges_refresh(s, error) : rc;
}
static int allocation_ranges_refresh(ps_space *s, lc_error *error) {
  ps_run *run;
  int present, imported, rc = physical_presence(s, &present, error);
  if (rc != LC_OK)
    return rc;
  rc = pins_collect_locked(s, present, 0, error);
  if (rc != LC_OK)
    return rc;
  /* A foreign writer can dirty common pages between observations. Drop local
   * hints for those pages. The cohort bitmap follows every physical claim. */
  for (run = s->free; run; run = run->next)
    if (!run->claimed)
      run->clean = 0;
  ps_internal_free_coalesce(s);
  rc = healthy(s, error);
  if (rc == LC_OK && present) {
    rc = pin_conflict(s, PIN_PRESENCE_OFFSET, 1, &imported, error);
    if (rc == LC_OK)
      rc =
          foreign_pool(s, &s->free, &s->free_bytes, imported ? NULL : s, error);
  }
  if (rc == LC_OK && present && !s->metadata_materialized)
    rc = foreign_pool(s, &s->metadata_free, &s->metadata_free_bytes, NULL,
                      error);
  s->allocation_ranges_checked = rc == LC_OK;
  return rc;
}
/* Releasing resident entries can narrow ownership on a stale projection.
 * Sweeping foreign quarantine and acquiring workspace needs a current view. */
static void collect_retired(ps_space *s) {
  ps_entry **link = &s->retired, *p;
  int present = 0, foreign, shared;
  if (!s->retired || __atomic_load_n(&s->poisoned, __ATOMIC_RELAXED))
    return;
  if (s->range_pins && physical_presence(s, &present, NULL) != LC_OK)
    return;
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
    unregister_entry(p);
    ps_internal_reserve_forget(s, p);
    ps_internal_entry_free(p);
  }
}
static size_t event_bytes(const ps_entry *p, int event) {
  return event == PS_PUT || event == PS_SEAL ? entry_bytes(p) : 64;
}
#define STREAM_SEALED_FLAG 1U
#define STREAM_ADMITTED_FLAG 2U
static int live_hint(uint32_t event) {
  return event == PS_STREAM_TAIL || event == PS_STREAM_OWNER ||
         event == PS_NAMESPACE_OWNER || event == PS_STREAM_EDIT ||
         event == PS_STREAM_COMPLETE || event == PS_STREAM_ADMITTED;
}
static void encode_entry(unsigned char *wire, const ps_entry *p, uint64_t seq,
                         int event) {
  size_t i, bytes = event_bytes(p, event), first;
  memset(wire, 0, bytes);
  ps_wire_put32(wire, (uint32_t)EVENT_MAGIC);
  ps_wire_put32(wire + 4, (uint32_t)bytes);
  ps_wire_put64(wire + 8, seq);
  ps_wire_put32(wire + 16, p->key.kind);
  ps_wire_put32(wire + 20, (uint32_t)event);
  ps_wire_put64(wire + 24, p->key.owner);
  ps_wire_put64(wire + 32, p->key.key);
  ps_wire_put64(wire + 40, p->token);
  if (live_hint((uint32_t)event))
    ps_wire_put64(wire + 48, p->bytes);
  if (event == PS_PUT || event == PS_SEAL) {
    ps_wire_put64(wire + 48, p->bytes);
    ps_wire_put32(wire + 56, (uint32_t)p->count);
    first = p->key.kind == PS_OWNED_RUN ? 80 : 64;
    if (p->key.kind == PS_OWNED_RUN) {
      ps_wire_put64(wire + 64, p->stream.run->capacity);
      ps_wire_put32(wire + 72, (uint32_t)p->stream.run->mapped);
    }
    if (p->key.kind == PS_KEY_METADATA) {
      ps_wire_put64(wire + 64, p->deletion_bytes);
      first += PS_KEY_SLOT_DESCRIPTOR_EXTRA;
    }
    for (i = 0; i < p->count; ++i) {
      ps_wire_put64(wire + first + 24 * i, p->extents[i].offset);
      ps_wire_put64(wire + first + 8 + 24 * i, p->extents[i].bytes);
      ps_wire_put32(wire + first + 16 + 24 * i, p->extents[i].crc);
      ps_wire_put32(wire + first + 20 + 24 * i, p->extents[i].flags);
    }
    if (p->key.kind == PS_STREAM) {
      ps_wire_put32(wire + 56, p->stream.head->sealed     ? STREAM_SEALED_FLAG
                               : p->stream.head->admitted ? STREAM_ADMITTED_FLAG
                                                          : 0U);
      ps_wire_put64(wire + 64, p->stream.head->capacity);
      ps_wire_put64(wire + 72, p->stream.head->runs);
    }
    if (p->key.kind == PS_CATALOG)
      ps_catalog_encode_record(wire, p);
  }
  ps_wire_put32(wire + 60, (uint32_t)ps_crc32(checksum(wire, 60), wire + 64,
                                              (uInt)(bytes - 64)));
}
/* The CRC covers the same byte sequence with the stored checksum omitted.
 * Most descriptors fit in a bounded buffer and can use one zlib update. */
static uint32_t entry_checksum(const unsigned char *wire, size_t bytes) {
  unsigned char compact[124];
  if (bytes == 64)
    return checksum(wire, 60);
  if (bytes <= sizeof(compact) + 4) {
    memcpy(compact, wire, 60);
    memcpy(compact + 60, wire + 64, bytes - 64);
    return checksum(compact, bytes - 4);
  }
  return (uint32_t)ps_crc32(checksum(wire, 60), wire + 64, (uInt)(bytes - 64));
}
static int valid_packed_extent(uint64_t offset, uint64_t bytes) {
  return bytes && bytes < PS_PAGE && offset % PS_CELL == 0 &&
         cell_count(bytes) * PS_CELL <= PS_PAGE - offset % PS_PAGE;
}
static int valid_stream_span(uint64_t tail, uint64_t capacity, uint64_t runs) {
  return tail <= capacity && capacity <= LC_I64_MAX &&
         runs <= capacity / (PS_PAGE - PS_RUN_HEADER);
}
static int decode_entry(ps_space *s, const unsigned char *wire, size_t bytes,
                        int temporary, ps_entry **out, int *event,
                        lc_error *error) {
  size_t count, i, first;
  uint64_t sum = 0, offset, n;
  ps_entry *p;
  uint32_t type, kind, wire_count;
  *out = NULL;
  if (bytes < 64 || ps_wire_get32(wire) != EVENT_MAGIC ||
      ps_wire_get32(wire + 4) != bytes ||
      ps_wire_get32(wire + 60) != entry_checksum(wire, bytes))
    return err(error, LC_ERR_PROTOCOL, "invalid container metadata checksum");
  type = ps_wire_get32(wire + 20);
  kind = ps_wire_get32(wire + 16);
  wire_count = ps_wire_get32(wire + 56);
  count = kind == PS_STREAM || kind == PS_CATALOG ? 0 : wire_count;
  first = kind == PS_OWNED_RUN && type == PS_PUT ? 80 : 64;
  if (kind == PS_KEY_METADATA && type == PS_PUT)
    first += PS_KEY_SLOT_DESCRIPTOR_EXTRA;
  if (count > (PS_JOURNAL - 64) / 24 ||
      (kind != PS_CATALOG &&
       bytes != (kind == PS_STREAM && (type == PS_PUT || type == PS_SEAL)
                     ? 80
                     : first + count * 24)) ||
      (type != PS_PUT && type != PS_REMOVE && type != PS_SEAL))
    return err(error, LC_ERR_PROTOCOL, "invalid container extent count");
  p = space_entry_new(s, kind, count, temporary);
  if (!p)
    return err(error, LC_ERR_NOMEM, "cannot allocate container entry");
  p->key.owner = ps_wire_get64(wire + 24);
  p->key.key = ps_wire_get64(wire + 32);
  p->token = ps_wire_get64(wire + 40);
  p->bytes = ps_wire_get64(wire + 48);
  *event = (int)type;
  if (!valid_key(p->key) || !p->token || p->token > s->token_limit ||
      (*event == PS_REMOVE &&
       (count || p->bytes || wire_count ||
        (!ps_internal_immutable_key(p->key) && p->key.kind != PS_OWNED_RUN &&
         p->key.kind != PS_STREAM && p->key.kind != PS_CATALOG))) ||
      (*event == PS_SEAL && p->key.kind != PS_STREAM))
    goto invalid;
  if (p->key.kind == PS_CATALOG) {
    int rc;
    if (p->bytes || wire_count || (*event == PS_REMOVE && bytes != 64))
      goto invalid;
    rc = *event == PS_PUT ? ps_catalog_decode_record(p, wire, bytes, error)
                          : LC_OK;
    if (rc != LC_OK) {
      ps_internal_entry_free(p);
      return rc;
    }
    *out = p;
    return LC_OK;
  }
  if (p->key.kind == PS_STREAM && (*event == PS_PUT || *event == PS_SEAL)) {
    uint32_t flags = wire_count;
    p->stream.head->sealed = (flags & STREAM_SEALED_FLAG) != 0;
    p->stream.head->admitted = (flags & STREAM_ADMITTED_FLAG) != 0;
    p->stream.head->capacity = ps_wire_get64(wire + 64);
    p->stream.head->runs = ps_wire_get64(wire + 72);
    if (flags > STREAM_ADMITTED_FLAG ||
        (*event == PS_SEAL && !p->stream.head->sealed) ||
        !valid_stream_span(p->bytes, p->stream.head->capacity,
                           p->stream.head->runs))
      goto invalid;
    *out = p;
    return LC_OK;
  }
  if (p->key.kind == PS_OWNED_RUN && *event == PS_PUT) {
    p->stream.run->capacity = ps_wire_get64(wire + 64);
    p->stream.run->mapped = (int)ps_wire_get32(wire + 72);
    if (!p->stream.run->capacity ||
        p->stream.run->capacity > PS_MIB - PS_RUN_HEADER ||
        p->key.key > (uint64_t)LC_I64_MAX - p->stream.run->capacity ||
        ps_wire_get32(wire + 72) > 1 || ps_wire_get32(wire + 76) ||
        (!p->stream.run->mapped &&
         (p->bytes < PS_RUN_HEADER ||
          p->stream.run->capacity != p->bytes - PS_RUN_HEADER)) ||
        (p->stream.run->mapped &&
         (p->stream.run->capacity > PS_STREAM_PIECE_BYTES || !count ||
          p->bytes < 64 || p->bytes > 64 + 9 * p->stream.run->capacity)))
      goto invalid;
  }
  p->count = count;
  for (i = 0; i < count; ++i) {
    offset = ps_wire_get64(wire + first + 24 * i);
    n = ps_wire_get64(wire + first + 8 + 24 * i);
    p->extents[i].flags = ps_wire_get32(wire + first + 20 + 24 * i);
    if (!n ||
        n > (p->key.kind == PS_OWNED_RUN && !p->stream.run->mapped
                 ? PS_MIB
                 : PS_CHUNK) ||
        offset < PS_DATA || offset > s->capacity || n > s->capacity - offset ||
        (p->key.kind == PS_OWNED_RUN && !p->stream.run->mapped
             ? (count != 1 || p->extents[i].flags != EXTENT_OWNED ||
                offset % PS_PAGE || n % PS_PAGE)
         : p->extents[i].flags == EXTENT_PACKED
             ? !valid_packed_extent(offset, n)
             : (p->extents[i].flags || offset % PS_PAGE || n % PS_PAGE)) ||
        LC_U64_MAX - sum < n)
      goto invalid;
    p->extents[i].offset = offset;
    p->extents[i].bytes = n;
    p->extents[i].crc = ps_wire_get32(wire + first + 16 + 24 * i);
    sum += n;
  }
  if (sum != p->bytes)
    goto invalid;
  if (p->key.kind == PS_KEY_METADATA && type == PS_PUT) {
    uint64_t deletion = ps_wire_get64(wire + 64);
    int rc;
    if (deletion > p->bytes && deletion - p->bytes > 512)
      goto invalid;
    rc = ps_internal_reserve_entry(p, deletion, error);
    if (rc != LC_OK) {
      ps_internal_entry_free(p);
      return rc;
    }
  }
  *out = p;
  return LC_OK;
invalid:
  ps_internal_entry_free(p);
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
  if (!bytes || start < *end || start > p->stream.run->capacity ||
      bytes > p->stream.run->capacity - start || bytes > p->bytes - *data)
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
  uint64_t count = p->stream.run->range_rank[offset / 64];
  for (; at < end; ++at)
    count += bitmap_count(p->stream.run->range_bitmap[at]);
  if (offset % 8)
    count += bitmap_count(p->stream.run->range_bitmap[at] &
                          ((1U << (offset % 8)) - 1));
  return count;
}
static int owned_map_load(ps_space *s, ps_entry *p, lc_error *e) {
  unsigned char header[64], *table = NULL;
  ps_mapped_range *ranges = NULL;
  ps_mapped_range checked;
  uint16_t *rank = NULL;
  uint64_t count, i, data, end = 0, start, bytes, table_bytes, pos;
  int rc, bitmap;
  if (p->stream.run->ranges || p->stream.run->range_bitmap)
    return LC_OK;
  rc = owned_encoded_read(s, p, 0, header, sizeof(header), e);
  if (rc != LC_OK)
    return rc;
  count = ps_wire_get32(header + 32);
  bitmap = (int)ps_wire_get32(header + 60);
  table_bytes = bitmap ? (p->stream.run->capacity + 7) / 8 : count * 8;
  if (memcmp(header, "LCPSMAPR", 8) ||
      ps_wire_get64(header + 8) != p->key.owner ||
      ps_wire_get64(header + 16) != p->key.key ||
      ps_wire_get64(header + 24) != p->stream.run->capacity || !count ||
      count > p->stream.run->capacity ||
      ps_wire_get64(header + 40) != p->bytes ||
      ps_wire_get64(header + 48) != p->token ||
      ps_wire_get32(header + 60) > 1 ||
      ps_wire_get32(header + 56) !=
          (uint32_t)ps_crc32(checksum(header, 56), header + 60, 4) ||
      64 + table_bytes > p->bytes)
    return err(e, LC_ERR_PROTOCOL, "invalid mapped piece header");
  table = lc_alloc_with_allocator(NULL, (size_t)table_bytes);
  if (bitmap)
    rank = lc_calloc_with_allocator(
        NULL, (size_t)((p->stream.run->capacity + 63) / 64 + 1), sizeof(*rank));
  else
    ranges = lc_calloc_with_allocator(NULL, (size_t)count, sizeof(*ranges));
  if (!table || (bitmap ? !rank : !ranges))
    rc = err(e, LC_ERR_NOMEM, "cannot allocate mapped piece ranges");
  if (rc == LC_OK)
    rc = owned_encoded_read(s, p, 64, table, (size_t)table_bytes, e);
  if (rc == LC_OK &&
      checksum(table, (size_t)table_bytes) != ps_wire_get32(header + 36))
    rc = err(e, LC_ERR_PROTOCOL, "mapped piece range checksum mismatch");
  data = 64 + table_bytes;
  for (i = 0; !bitmap && rc == LC_OK && i < count; ++i) {
    start = ps_wire_get32(table + i * 8);
    bytes = ps_wire_get32(table + i * 8 + 4);
    rc = mapped_range_add(p, ranges + i, start, bytes, &data, &end, e);
  }
  if (bitmap && rc == LC_OK) {
    i = pos = 0;
    while (pos < p->stream.run->capacity && rc == LC_OK) {
      if (!(table[pos / 8] & (1U << (pos % 8)))) {
        ++pos;
        continue;
      }
      start = pos++;
      while (pos < p->stream.run->capacity &&
             (table[pos / 8] & (1U << (pos % 8))))
        ++pos;
      if (i == count)
        rc = err(e, LC_ERR_PROTOCOL, "mapped bitmap range count overflow");
      else
        rc = mapped_range_add(p, &checked, start, pos - start, &data, &end, e);
      ++i;
    }
    if (rc == LC_OK && (i != count || (p->stream.run->capacity % 8 &&
                                       table[table_bytes - 1] >>
                                           (p->stream.run->capacity % 8))))
      rc = err(e, LC_ERR_PROTOCOL, "mapped bitmap count or padding is invalid");
  }
  if (rc == LC_OK && data != p->bytes)
    rc = err(e, LC_ERR_PROTOCOL, "mapped piece has trailing bytes");
  if (rc == LC_OK) {
    p->stream.run->ranges = ranges;
    p->stream.run->range_count = (size_t)count;
    p->stream.run->mapped_end = end;
    if (bitmap) {
      pos = 0;
      for (i = 0; i < table_bytes; ++i) {
        if (!(i % 8))
          rank[i / 8] = (uint16_t)pos;
        pos += bitmap_count(table[i]);
      }
      if (!(table_bytes % 8))
        rank[table_bytes / 8] = (uint16_t)pos;
      p->stream.run->range_bitmap = table;
      p->stream.run->range_rank = rank;
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
static int validate_owned_identity(ps_space *s, ps_entry *p,
                                   const unsigned char *header,
                                   lc_error *error) {
  if (checksum(header, PS_RUN_IDENTITY_BYTES) != p->extents[0].crc ||
      ps_wire_get64(header) != OWNED_MAGIC ||
      ps_wire_get64(header + 8) != p->token ||
      ps_wire_get64(header + 16) != p->key.owner ||
      ps_wire_get64(header + 24) != p->key.key ||
      ps_wire_get64(header + 32) != p->extents[0].offset ||
      ps_wire_get64(header + 40) != p->bytes ||
      memcmp(header + 48, s->identity, 16))
    return err(error, LC_ERR_PROTOCOL, "owned run identity header mismatch");
  return LC_OK;
}
static int verify_owned(ps_space *s, ps_entry *p, unsigned char *header,
                        lc_error *error) {
  int rc;
  if (p->stream.run->mapped)
    return owned_map_load(s, p, error);
  rc = transfer(s, header, PS_RUN_IDENTITY_BYTES, p->extents[0].offset, 0,
                error);
  return rc == LC_OK ? validate_owned_identity(s, p, header, error) : rc;
}
/* The caller retains the captured cells for the whole read pass. Unrelated
 * cells in this buffer confer no authority and are never exposed. */
static int read_packed_bytes(ps_space *s, const ps_extent *extent,
                             packed_read *packed, const unsigned char **out,
                             int *private_failure, lc_error *error) {
  uint64_t page = extent->offset / PS_PAGE * PS_PAGE;
  int rc;
  if (packed->offset != page) {
    rc = file_transfer(s, packed->bytes, PS_PAGE, page, 0, private_failure,
                       error);
    if (rc != LC_OK)
      return rc;
    packed->offset = page;
  }
  *out = packed->bytes + (extent->offset - page);
  return LC_OK;
}
static int read_extent(ps_space *s, const ps_extent *extent,
                       unsigned char *buffer, packed_read *packed,
                       const unsigned char **data, int *private_failure,
                       lc_error *error) {
  if (packed && (extent->flags & EXTENT_PACKED))
    return read_packed_bytes(s, extent, packed, data, private_failure, error);
  *data = buffer;
  return file_transfer(s, buffer, (size_t)extent->bytes, extent->offset, 0,
                       private_failure, error);
}
static int verify_value_payloads(ps_space *s, ps_entry *p,
                                 unsigned char *buffer, packed_read *packed,
                                 lc_error *error) {
  const unsigned char *data;
  size_t i;
  int rc;
  if (p->key.kind == PS_OWNED_RUN && !p->stream.run->mapped)
    return verify_owned(s, p, buffer, error);
  for (i = 0; i < p->count; ++i) {
    rc = read_extent(s, &p->extents[i], buffer, packed, &data, NULL, error);
    if (rc != LC_OK)
      return rc;
    if (checksum(data, (size_t)p->extents[i].bytes) != p->extents[i].crc)
      return err(error, LC_ERR_PROTOCOL, "container payload checksum mismatch");
  }
  return p->key.kind == PS_OWNED_RUN ? owned_map_load(s, p, error) : LC_OK;
}
typedef struct recovery_values {
  unsigned char buffer[PS_CHUNK];
  packed_read packed;
} recovery_values;
static int verify_entry(ps_space *s, ps_entry *p, recovery_values *values,
                        lc_error *error) {
  /* Derived bytes never determine how far canonical authority can recover.
   * Their descriptors still own exact, checked ranges; readers validate their
   * payload CRC and the query owner rebuilds invalid data. Lost/corrupt query
   * payload must not truncate later acknowledged canonical publications. */
  if (p->key.kind >= PS_QUERY_FIRST && p->key.kind <= PS_QUERY_LAST)
    return LC_OK;
  return verify_value_payloads(s, p, values->buffer, &values->packed, error);
}
static int initial_free(ps_space *s, lc_error *error) {
  s->buckets = lc_calloc_with_allocator(NULL, 1, sizeof(*s->buckets));
  if (!s->buckets)
    return err(error, LC_ERR_NOMEM, "cannot allocate container buckets");
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
  if (bytes > (uint64_t)LC_I64_MAX / 2 ||
      entries > (uint64_t)LC_I64_MAX / PS_PAGE)
    return err(error, LC_ERR_INVALID, "container metadata capacity exhausted");
  pages = (bytes + META_PAYLOAD - 1) / META_PAYLOAD;
  pages += ps_directory_packed_pages(entries);
  if (pages > LC_U64_MAX / (2 * PS_PAGE))
    return err(error, LC_ERR_INVALID, "container metadata capacity exhausted");
  *bound = pages * PS_PAGE;
  return LC_OK;
}
static int metadata_claimed_bound(ps_space *s, uint64_t bytes, uint64_t entries,
                                  uint64_t *bound, lc_error *error) {
  int rc = peer_credits(s, error);
  if (rc != LC_OK)
    return rc;
  if (bytes > LC_U64_MAX - s->reserved_metadata ||
      entries > LC_U64_MAX - s->reserved_entries)
    return err(error, LC_ERR_INVALID, "container metadata budget overflow");
  bytes += s->reserved_metadata;
  entries += s->reserved_entries;
  if (s->allocation_guard) {
    if (bytes > LC_U64_MAX - s->peer_metadata_credits ||
        entries > LC_U64_MAX - s->peer_entry_credits)
      return err(error, LC_ERR_INVALID, "shared metadata budget overflow");
    bytes += s->peer_metadata_credits;
    entries += s->peer_entry_credits;
  }
  return metadata_bound(bytes, entries, bound, error);
}
/* Checkpoint preparation witnesses physical pages under allocator authority.
 * Between checkpoints the common floor is an admission rule. */
static int metadata_prepare_workspace(ps_space *s, lc_error *error) {
  ps_run *run;
  int rc;
  if (!s->metadata_claims_active)
    return LC_OK;
  if (!s->allocation_ranges_checked) {
    rc = allocation_ranges_refresh(s, error);
    if (rc != LC_OK)
      return rc;
  }
  for (run = s->metadata_free; run; run = run->next) {
    rc = metadata_claim_range(s, run->offset, run->bytes, error);
    if (rc != LC_OK)
      break;
  }
  if (run) {
    ps_run *claimed;
    for (claimed = s->metadata_free; claimed != run; claimed = claimed->next)
      (void)metadata_release_range(s, claimed->offset, claimed->bytes, NULL);
    return rc;
  }
  s->metadata_materialized = 1;
  return LC_OK;
}
static void metadata_return_workspace(ps_space *s) {
  ps_run **link = &s->metadata_free, *run;
  if (!s->metadata_materialized ||
      __atomic_load_n(&s->poisoned, __ATOMIC_RELAXED))
    return;
  while ((run = *link) != NULL) {
    if (metadata_release_range(s, run->offset, run->bytes, NULL) != LC_OK)
      return;
    if (run->offset < PS_DATA)
      link = &run->next;
    else {
      *link = run->next;
      s->metadata_free_bytes -= run->bytes;
      free_insert(s, run);
    }
  }
  s->metadata_materialized = 0;
}
static uint64_t metadata_target(ps_space *s, uint64_t bound) {
  uint64_t allocated = (uint64_t)s->metadata_pages * PS_PAGE;
  /* Enough free pages for one complete rewrite, and two complete images
   * counting the current checkpoint. A dense rewrite restores this invariant
   * from its retired image without relying on unprotected payload capacity. */
  return allocated < bound ? 2 * bound - allocated : bound;
}
static uint64_t ordinary_shortfall(ps_space *s, uint64_t target,
                                   uint64_t payload, uint64_t floor,
                                   uint64_t *available) {
  uint64_t extra =
      target > s->metadata_free_bytes ? target - s->metadata_free_bytes : 0;
  uint64_t total;
  if (available)
    *available = 0;
  if (floor > LC_U64_MAX - extra ||
      s->reserved_payload > LC_U64_MAX - floor - extra)
    return LC_U64_MAX;
  total = s->reserved_payload + floor + extra;
  if (available && total <= s->free_bytes)
    *available = s->free_bytes - total;
  if (payload > LC_U64_MAX - total)
    return LC_U64_MAX;
  total += payload;
  return total > s->free_bytes ? total - s->free_bytes : 0;
}
static int metadata_fill_locked(ps_space *s, uint64_t bound, uint64_t target,
                                uint64_t payload, uint64_t floor,
                                uint64_t *shortfall, lc_error *error) {
  uint64_t usable = s->capacity - PS_DATA;
  uint64_t bytes, missing;
  ps_run *node;
  int rc;
  /* Catalog and stream publication may borrow a capture guard. Its selected
   * journal does not describe a peer's private writer or budget ranges. */
  if (s->allocation_guard && !s->allocation_ranges_checked) {
    rc = allocation_ranges_refresh(s, error);
    if (rc != LC_OK)
      return rc;
  }
  /* Leave a burst of ordinary headroom for independent producers while the
   * worker observes the hint and admits its first bounded pass. */
  missing = ordinary_shortfall(s, target, payload, floor, &bytes);
  if (missing || bytes - payload <= usable / 2 + usable / 8)
    ps_readiness_set_pressure(s, 1);
  if (missing) {
    rc = pins_reclaim_locked(s, error);
    if (rc != LC_OK)
      return rc;
    missing = ordinary_shortfall(s, target, payload, floor, NULL);
    if (missing) {
      if (shortfall)
        *shortfall = missing;
      return err(error, LC_ERR_INVALID,
                 "container capacity reserved for checkpoint and maintenance; "
                 "checkpoint or grow");
    }
  }
  while ((!s->metadata_claims_active || s->metadata_materialized) &&
         s->metadata_free_bytes < target) {
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
                         uint64_t payload, uint64_t floor, uint64_t *shortfall,
                         lc_error *error) {
  ps_coordination_guard *guard = NULL;
  int rc = allocation_begin(s, &guard, error);
  if (rc == LC_OK && guard)
    rc = allocation_refresh(s, error);
  if (rc == LC_OK)
    rc = metadata_fill_locked(s, bound, target, payload, floor, shortfall,
                              error);
  return allocation_end(s, guard, rc, error);
}
static int metadata_protect(ps_space *s, uint64_t bound, uint64_t payload,
                            uint64_t floor, lc_error *error) {
  return metadata_fill(s, bound, metadata_target(s, bound), payload, floor,
                       NULL, error);
}
static int reserve_floor(ps_space *s, uint64_t bytes, uint64_t entries,
                         uint64_t bound, uint64_t *floor, lc_error *e) {
  ps_budget_plan plan;
  uint64_t future, target, extra;
  int rc;
  ps_internal_reserve_plan(s, &plan);
  /* The first positive allowance on a handle needs one ledger page. */
  if (s->metadata_claims_active && (plan.metadata_bytes || plan.entries))
    *floor += PS_PAGE;
  if (bytes < s->metadata)
    bytes = s->metadata;
  if (entries < s->live_entries)
    entries = s->live_entries;
  if (plan.metadata_bytes > LC_U64_MAX - bytes ||
      plan.entries > LC_U64_MAX - entries)
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
  if (plan.payload_bytes > LC_U64_MAX - extra ||
      *floor > LC_U64_MAX - extra - plan.payload_bytes)
    return err(e, LC_ERR_INVALID, "maintenance reserve payload overflows");
  *floor += extra + plan.payload_bytes;
  return LC_OK;
}
static int capacity_allowance(ps_space *s, uint64_t bytes, uint64_t entries,
                              uint64_t payload, uint64_t floor,
                              ps_budget *budget, uint64_t *shortfall,
                              lc_error *error) {
  uint64_t bound, current;
  uint64_t growth = bytes > s->metadata ? bytes - s->metadata : 0;
  uint64_t added = entries > s->live_entries ? entries - s->live_entries : 0;
  int rc = ps_internal_budget_check(s, budget, payload, growth, added, error);
  if (rc != LC_OK)
    return rc;
  /* Shared admission counts exact outstanding credits in the checkpoint
   * bound. With no additional physical pages to acquire, exchanging a credit
   * for selected metadata cannot increase that bound. Rollover still proves
   * the ranges it actually borrows when it prepares a checkpoint. */
  if (budget && !payload && s->allocation_guard)
    return LC_OK;
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
  return metadata_fill(s, bound, metadata_target(s, bound), payload, floor,
                       shortfall, error);
}
static int checkpoint_allowance_floor(ps_space *s, uint64_t bytes,
                                      uint64_t entries, uint64_t payload,
                                      uint64_t floor, ps_budget *budget,
                                      lc_error *error) {
  return capacity_allowance(s, bytes, entries, payload, floor, budget, NULL,
                            error);
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
  uint64_t reserve = PS_PAGE;
  int rc, fresh = 0, present, retry_packed = 1, check_quarantine;
again:
  p = NULL;
  *bytes = requested;
  count = cell_count(requested);
  for (bin = count; bin <= CELLS; ++bin)
    if (s->buckets->available[bin]) {
      p = s->buckets->available[bin];
      break;
    }
  /* Use existing slack before buying another page. Only the final subpage
   * tail may split, and only once: its remainder can always fit a fresh page.
   * Thus the physical page bound is unchanged and at most one descriptor is
   * added, even with adversarial cell fragmentation. */
  if (!p && split)
    for (bin = count - 1; bin; --bin)
      if (s->buckets->available[bin]) {
        p = s->buckets->available[bin];
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
    rc = take_payload_locked(s, budget, &reserve, &p->run.offset, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(NULL, p);
      return rc;
    }
    p->run.bytes = PS_PAGE;
    ps_internal_budget_spend(budget, PS_PAGE, 0, 0);
    payload_register(s, p);
    fresh = 1;
  }
  at = 0;
  if (!fresh) {
    uint64_t free_cells = ~p->occupied;
    size_t span = 0;
    while (free_cells && span < count)
      span = cell_span(&free_cells, &at);
    if (span < count)
      return err(error, LC_ERR_PROTOCOL, "packed payload free-span mismatch");
    rc =
        physical_claim(s, p->run.offset + at * PS_CELL, count * PS_CELL, error);
    if (rc != LC_OK)
      return rc;
  }
  p->occupied |= cell_mask(at, count);
  s->packed_bytes += count * PS_CELL;
  payload_available(s, p);
  extent->offset = p->run.offset + at * PS_CELL;
  extent->flags = EXTENT_PACKED;
  /* A fresh allocation starts at cell zero. Release its trailing slack. */
  if (fresh && count < CELLS)
    return physical_release(s, p->run.offset + count * PS_CELL,
                            (CELLS - count) * PS_CELL, error);
  return LC_OK;
}
static int metadata_offset(ps_space *s, uint64_t offset) {
  return offset % PS_PAGE == 0 && offset >= PS_METADATA_START &&
         offset <= s->capacity - PS_PAGE &&
         (offset < JOURNAL_OFFSET || offset >= PS_DATA);
}
static ps_meta_page *metadata_find(ps_space *s, uint64_t offset) {
  ps_meta_page *p;
  for (p = s->buckets->metadata[(offset / PS_PAGE) % BUCKETS]; p;
       p = p->hash_next)
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
  p->hash_next = s->buckets->metadata[hash];
  s->buckets->metadata[hash] = p;
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
  ps_meta_page **link =
      &s->buckets->metadata[(p->run.offset / PS_PAGE) % BUCKETS];
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
  ps_readiness_zero_pages(s, p->run.offset, PS_PAGE, 0);
  p->run.bytes = PS_PAGE;
  p->generation = s->generation + 1;
  p->index = index;
  p->references = index != 0;
  p->claimed = s->metadata_claims_active;
  metadata_register(s, p);
  *out = p;
  return LC_OK;
}
static int directory_read(void *v, uint64_t offset, void *pages, size_t bytes,
                          lc_error *error) {
  ps_space *s = v;
  if (!metadata_offset(s, offset) || bytes > s->capacity - offset ||
      (offset < JOURNAL_OFFSET && bytes > JOURNAL_OFFSET - offset))
    return err(error, LC_ERR_PROTOCOL,
               "directory pages outside metadata space");
  return transfer(s, pages, bytes, offset, 0, error);
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
static int directory_open(ps_space *s, ps_directory_root root,
                          ps_directory **out, lc_error *error) {
  ps_directory_io io;
  memset(&io, 0, sizeof(io));
  io.context = s;
  memcpy(io.identity, s->identity, 16);
  io.read = directory_read;
  io.allocate = directory_allocate;
  io.write = directory_write;
  return ps_directory_open(&io, root, out, error);
}
static uint32_t metadata_crc(const unsigned char *page) {
  return (uint32_t)ps_crc32(checksum(page, 48), page + 52, PS_PAGE - 52);
}
typedef struct checkpoint_binding {
  ps_entry *entry;
  ps_descriptor_ref ref;
} checkpoint_binding;
typedef struct metadata_builder {
  ps_space *space;
  ps_meta_page *page;
  unsigned char *buffer, *wire;
  size_t wire_capacity;
  checkpoint_binding *bindings;
  size_t binding_count, binding_capacity;
} metadata_builder;
static int metadata_flush(metadata_builder *b, lc_error *error) {
  ps_space *s = b->space;
  ps_meta_page *p = b->page;
  int rc;
  if (!p)
    return LC_OK;
  ps_wire_put32(b->buffer, META_MAGIC);
  ps_wire_put32(b->buffer + 4, (uint32_t)p->used);
  ps_wire_put64(b->buffer + 8, p->following);
  ps_wire_put64(b->buffer + 16, p->generation);
  ps_wire_put64(b->buffer + 24, p->run.offset);
  memcpy(b->buffer + 32, s->identity, 16);
  ps_wire_put32(b->buffer + 48, metadata_crc(b->buffer));
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
                           size_t bytes, ps_descriptor_ref *ref,
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
  ref->incarnation = ps_wire_get64(wire + 40);
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
static int metadata_unref(ps_space *s, ps_descriptor_ref ref, lc_error *error) {
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
                                       int dense, lc_error *error) {
  ps_meta_page *page;
  ps_entry *p;
  ps_descriptor_ref ref;
  uint64_t offset;
  size_t i, at, take;
  int rc;
  if (!s->metadata_claims_active)
    return LC_OK;
  if (dense)
    for (page = s->pages; page; page = page->next) {
      if (!page->index || page->generation > s->generation)
        continue;
      rc = metadata_claim_retirement(s, page, error);
      if (rc != LC_OK)
        return rc;
    }
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
void ps_internal_budget_trim(ps_space *s, ps_run **spare) {
  uint64_t bound;
  if (!s->metadata_claims_active &&
      metadata_claimed_bound(s, s->metadata, s->live_entries, &bound, NULL) ==
          LC_OK) {
    s->metadata_workspace = bound;
    *spare = metadata_trim(s, *spare);
  }
}
static int checkpoint_descriptor(metadata_builder *builder, ps_entry *p,
                                 ps_directory_value *value, lc_error *error) {
  unsigned char *wire;
  checkpoint_binding *bindings;
  size_t bytes, capacity;
  int rc;
  memset(value, 0, sizeof(*value));
  if (p->key.kind == PS_STREAM && p->stream.head->runs <= UINT32_MAX) {
    ps_directory_stream *stream = &value->stream;
    stream->storage = p->stream.head->sealed ? PS_DIRECTORY_SEALED_STREAM
                                             : PS_DIRECTORY_STREAM;
    stream->tail = p->bytes;
    stream->capacity = p->stream.head->capacity;
    stream->incarnation = p->token;
    stream->runs = (uint32_t)p->stream.head->runs;
    /* Match the descriptor's checkpoint sequence for cohort admission. */
    if (p->stream.head->admitted && !p->stream.head->sealed)
      stream->admitted_at = builder->space->sequence;
    return LC_OK;
  }
  if (p->key.kind == PS_CATALOG && p->catalog->kind == PS_CATALOG_NAMESPACE &&
      !p->catalog->stream.kind &&
      p->catalog->length <= PS_DIRECTORY_NAMESPACE_MAX) {
    ps_directory_name *name = &value->name;
    name->storage = PS_DIRECTORY_NAMESPACE;
    name->length = (unsigned char)p->catalog->length;
    memcpy(name->bytes, p->catalog->name, name->length);
    return LC_OK;
  }
  if (p->count == 1) {
    if (ps_internal_immutable_key(p->key) &&
        p->extents[0].flags == EXTENT_PACKED)
      value->extent.storage = PS_DIRECTORY_PACKED_VALUE;
    else if (p->key.kind == PS_OWNED_RUN && !p->stream.run->mapped)
      value->extent.storage = PS_DIRECTORY_OWNED_RUN;
  }
  if (value->tag.storage != PS_DIRECTORY_DESCRIPTOR) {
    value->extent.ref.offset = p->extents[0].offset;
    value->extent.ref.bytes = p->bytes;
    value->extent.ref.incarnation = p->token;
    value->extent.deletion_bytes = p->deletion_bytes;
    value->extent.crc = p->extents[0].crc;
    return LC_OK;
  }
  /* Inline extents and deletions need no new descriptor reference. Record
   * only the descriptors written by this build, before selecting its root. */
  if (builder->binding_count == builder->binding_capacity) {
    if (builder->binding_capacity > SIZE_MAX / sizeof(*bindings) / 2)
      return err(error, LC_ERR_NOMEM, "too many staged checkpoint bindings");
    capacity = builder->binding_capacity ? 2 * builder->binding_capacity : 16;
    bindings = lc_realloc_with_allocator(NULL, builder->bindings,
                                         capacity * sizeof(*bindings));
    if (!bindings)
      return err(error, LC_ERR_NOMEM, "cannot stage checkpoint bindings");
    builder->bindings = bindings;
    builder->binding_capacity = capacity;
  }
  bytes = entry_bytes(p);
  if (bytes > builder->wire_capacity) {
    wire = lc_realloc_with_allocator(NULL, builder->wire, bytes);
    if (!wire)
      return err(error, LC_ERR_NOMEM, "cannot allocate checkpoint descriptor");
    builder->wire = wire;
    builder->wire_capacity = bytes;
  }
  encode_entry(builder->wire, p, builder->space->sequence, PS_PUT);
  rc =
      metadata_append(builder, builder->wire, bytes, &value->extent.ref, error);
  if (rc == LC_OK) {
    checkpoint_binding *binding = &builder->bindings[builder->binding_count++];
    binding->entry = p;
    binding->ref = value->extent.ref;
  }
  return rc;
}
typedef struct checkpoint_entries {
  metadata_builder *builder;
  ps_entry **next;
} checkpoint_entries;
static int checkpoint_entry_compare(const void *a, const void *b) {
  ps_entry *const *left = a, *const *right = b;
  return ps_directory_key_compare((*left)->key, (*right)->key);
}
static int checkpoint_entry_next(void *v, ps_key *key,
                                 ps_directory_value *value, lc_error *error) {
  checkpoint_entries *entries = v;
  ps_entry *p = *entries->next++;
  *key = p->key;
  return checkpoint_descriptor(entries->builder, p, value, error);
}
static int checkpoint_pack_directory(ps_space *s, ps_directory *directory,
                                     metadata_builder *builder,
                                     lc_error *error) {
  checkpoint_entries entries;
  ps_entry *p, **slots = NULL;
  size_t count = 0;
  int rc;
  if (s->live_entries > SIZE_MAX / sizeof(*slots))
    return err(error, LC_ERR_NOMEM, "checkpoint directory is too large");
  if (s->live_entries) {
    slots =
        lc_calloc_with_allocator(NULL, (size_t)s->live_entries, sizeof(*slots));
    if (!slots)
      return err(error, LC_ERR_NOMEM, "cannot collect checkpoint keys");
  }
  for (p = s->dirty; p; p = p->dirty_next) {
    if (!p->deleted && count < s->live_entries)
      slots[count++] = p;
    else if (!p->deleted)
      break;
  }
  if (p || count != s->live_entries)
    rc =
        err(error, LC_ERR_PROTOCOL, "checkpoint directory entry count changed");
  else {
    if (count > 1)
      qsort(slots, count, sizeof(*slots), checkpoint_entry_compare);
    entries.builder = builder;
    entries.next = slots;
    rc = ps_directory_build(directory, count, checkpoint_entry_next, &entries,
                            error);
  }
  lc_free_with_allocator(NULL, slots);
  return rc;
}
static int clear_checkpoint_journal(ps_space *s, uint64_t cursor,
                                    unsigned char *image, size_t bytes,
                                    lc_error *error) {
  memset(image, 0, bytes);
  return journal_transfer(s, cursor, image, bytes, 1, error);
}
#define CHECKPOINT_ZERO_RANGES 16U
typedef struct checkpoint_zero_range {
  uint64_t offset, bytes;
} checkpoint_zero_range;
/* Reserve no capacity. Allocator authority protects already-free common
 * pages during this bounded pass and the checkpoint's existing barriers.
 * A failed optional zero write can only affect those free bytes. */
static size_t checkpoint_prepare_free(ps_space *s,
                                      checkpoint_zero_range *ranges) {
  ps_run *run;
  size_t count = 0;
  uint64_t bytes, clean, remaining = PS_MIB;
  if (!s->readiness || !s->allocation_guard || !s->generation || s->durable ||
      !s->io.zero || !s->allocation_ranges_checked || !ps_readiness_pressure(s))
    return 0;
  for (run = s->free; run && count < CHECKPOINT_ZERO_RANGES &&
                      remaining >= PS_STREAM_PIECE_BYTES;
       run = run->next) {
    if (run->claimed)
      continue;
    bytes = ps_readiness_zero_coverage(
        s, run->offset, run->bytes < remaining ? run->bytes : remaining);
    if (bytes < PS_STREAM_PIECE_BYTES)
      continue;
    clean = ps_readiness_zero_prefix(s, run->offset, bytes);
    if (clean == bytes)
      continue;
    if (zero_file_range(s, run->offset + clean, bytes - clean) != 0)
      break;
    ranges[count].offset = run->offset + clean;
    ranges[count++].bytes = bytes - clean;
    remaining -= bytes - clean;
  }
  return count;
}
static int checkpoint_body(ps_space *s, metadata_builder *builder,
                           int force_dense, lc_error *error) {
  unsigned char root[PS_ROOT_BYTES], zero[PS_ROOT_BYTES] = {0};
  ps_entry *p;
  ps_meta_page *page, *next_page;
  ps_run *trim = NULL;
  uint64_t bound, extra, incremental, floor = PS_MAINTENANCE_RESERVE;
  uint64_t prepared = 0;
  size_t covered = s->journal_image ? s->journal : 0;
  uint64_t journal_start = s->journal_start;
  checkpoint_zero_range zero_ranges[CHECKPOINT_ZERO_RANGES];
  ps_directory *directory = NULL;
  ps_directory_root new_root = {0}, empty = {0};
  ps_directory_value value;
  ps_directory_pages changes;
  size_t i, zero_count = 0;
  int next = 1 - s->root, rc, removed, dense = 0;
  if (s->generation == LC_U64_MAX)
    return err(error, LC_ERR_INVALID,
               "container checkpoint generation exhausted");
  if (covered > LC_U64_MAX - journal_start - PS_JOURNAL)
    return err(error, LC_ERR_INVALID, "container journal cursor exhausted");
  journal_start += covered;
  if (force_dense)
    metadata_dirty_all(s);
  rc = metadata_claimed_bound(s, s->metadata, s->live_entries, &bound, error);
  if (rc == LC_OK)
    s->metadata_workspace = bound;
  if (rc == LC_OK && s->dirty)
    rc = metadata_prepare_workspace(s, error);
  if (rc == LC_OK && s->dirty)
    rc = metadata_protect(s, bound, 0, 0, error);
  if (rc == LC_OK && s->dirty) {
    /* Leave a complete packed image for the next checkpoint. A path-copy
     * attempt can need more pages because the resulting tree is not packed. */
    extra = (ps_directory_page_bound(s->live_entries) -
             ps_directory_packed_pages(s->live_entries)) *
            PS_PAGE;
    incremental =
        bound <= (LC_U64_MAX - extra) / 2 ? 2 * bound + extra : LC_U64_MAX;
    if (!force_dense && s->metadata_free_bytes < incremental &&
        reserve_floor(s, s->metadata, s->live_entries, bound, &floor, NULL) ==
            LC_OK &&
        !ordinary_shortfall(s, incremental, 0, floor, NULL))
      rc = metadata_fill(s, bound, incremental, 0, floor, NULL, error);
    dense = force_dense || s->metadata_free_bytes < incremental;
    if (dense)
      metadata_dirty_all(s);
  }
  if (rc == LC_OK && !s->metadata_claims_active &&
      (s->dirty || metadata_trim_needed(s))) {
    trim = lc_calloc_with_allocator(NULL, 1, sizeof(*trim));
    if (!trim)
      rc = err(error, LC_ERR_NOMEM, "cannot allocate checkpoint trim interval");
  }
  if (rc == LC_OK)
    rc = directory_open(s, dense ? empty : s->directory, &directory, error);
  if (rc == LC_OK && s->dirty) {
    builder->buffer = lc_alloc_with_allocator(NULL, PS_PAGE);
    if (!builder->buffer)
      rc = err(error, LC_ERR_NOMEM, "cannot allocate descriptor page buffer");
  }
  if (rc == LC_OK && dense)
    rc = checkpoint_pack_directory(s, directory, builder, error);
  for (p = s->dirty; !dense && p && rc == LC_OK; p = p->dirty_next) {
    if (p->deleted)
      rc = ps_directory_remove(directory, p->key, &removed, error);
    else {
      rc = checkpoint_descriptor(builder, p, &value, error);
      if (rc == LC_OK)
        rc = ps_directory_put(directory, p->key, value, error);
    }
  }
  if (rc == LC_OK)
    rc = metadata_flush(builder, error);
  if (rc == LC_OK)
    rc = ps_directory_flush(directory, s->generation + 1, &new_root, error);
  if (rc == LC_OK) {
    ps_directory_changed_pages(directory, &changes);
    rc = metadata_prepare_retirement(s, &changes, dense, error);
  }
  if (rc == LC_OK)
    zero_count = checkpoint_prepare_free(s, zero_ranges);
  if (rc == LC_OK)
    rc = sync_file(s, error); /* payloads, journal and new metadata pages */
  memset(root, 0, sizeof(root));
  ps_wire_put64(root, ROOT_MAGIC);
  ps_wire_put64(root + 8, s->generation + 1);
  ps_wire_put64(root + 16, s->capacity);
  ps_wire_put64(root + 24, s->sequence);
  ps_wire_put64(root + 32, new_root.offset);
  ps_wire_put64(root + 40, new_root.generation);
  ps_wire_put64(root + 48, s->token_limit);
  memcpy(root + 56, s->identity, 16);
  ps_wire_put32(root + 72, PS_ROOT_LAYOUT);
  ps_wire_put32(root + ROOT_JOURNAL_COVERED, (uint32_t)covered);
  ps_wire_put64(root + ROOT_JOURNAL_START, journal_start);
  ps_wire_put32(root + ROOT_CHECKSUM, checksum(root, ROOT_CHECKSUM));
  if (rc == LC_OK) {
    ps_readiness_begin(s, PS_READINESS_ROOT);
    rc = transfer(s, root, sizeof(root), (uint64_t)next * PS_PAGE, 1, error);
  }
  if (rc == LC_OK)
    rc = sync_file(s, error);
  if (rc == LC_OK && covered)
    ps_readiness_begin(s, PS_READINESS_CHECKPOINT_RETIRE);
  if (rc == LC_OK)
    rc = transfer(s, zero, sizeof(zero), (uint64_t)s->root * PS_PAGE, 1, error);
  if (rc == LC_OK && covered)
    rc = clear_checkpoint_journal(s, s->journal_start, s->journal_image,
                                  covered, error);
  if (rc == LC_OK) {
    prepared = ps_readiness_issued_tickets(s);
    rc = sync_file(s, error); /* obsolete root and covered journal retired */
  }
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
  ps_readiness_finish(s, PS_READINESS_CHECKPOINT_RETIRE);
  s->root = next;
  ++s->generation;
  ++s->checkpoints;
  s->metadata_fallbacks += dense;
  s->directory = new_root;
  s->journal_start = journal_start;
  s->journal = 0;
  rc = ps_readiness_stabilize(s, s->sequence, s->generation, s->journal,
                              prepared, error);
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
  if (dense)
    for (page = s->pages; page; page = next_page) {
      next_page = page->next;
      if (page->index && page->generation < s->generation)
        metadata_release(s, page);
    }
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
    memset(&p->checkpoint_ref, 0, sizeof(p->checkpoint_ref));
    p->previous = NULL;
    if (p->deleted) {
      key_index_remove(&s->keys, p->key);
      retire(s, p);
    }
  }
  if (rc != LC_OK) {
    __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
    lc_free_with_allocator(NULL, trim);
    return rc;
  }
  for (i = 0; i < builder->binding_count; ++i) {
    const checkpoint_binding *binding = &builder->bindings[i];
    binding->entry->checkpoint_ref = binding->ref;
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
  rc = pins_reclaim_locked(s, error);
  if (rc == LC_OK) {
    collect_retired(s);
    rc = healthy(s, error);
  }
  /* Publish proofs only after successful checkpoint completion, while these
   * intervals are still protected by the same allocator scope. */
  if (rc == LC_OK)
    for (i = 0; i < zero_count; ++i)
      ps_readiness_zero_pages(s, zero_ranges[i].offset, zero_ranges[i].bytes,
                              1);
  return rc;
}
static int checkpoint_select(ps_space *s, int force_dense, lc_error *error) {
  metadata_builder builder = {0};
  ps_coordination_guard *guard = NULL;
  int rc = allocation_begin(s, &guard, error);
  if (rc == LC_OK && guard)
    rc = allocation_refresh(s, error);
  if (rc == LC_OK) {
    builder.space = s;
    rc = checkpoint_body(s, &builder, force_dense, error);
  }
  lc_free_with_allocator(NULL, builder.bindings);
  lc_free_with_allocator(NULL, builder.wire);
  lc_free_with_allocator(NULL, builder.buffer);
  metadata_return_workspace(s);
  return allocation_end(s, guard, rc, error);
}
static int checkpoint_locked(ps_space *s, lc_error *error) {
  return checkpoint_select(s, 0, error);
}
static uint64_t retirement_shortfall(ps_space *s, const ps_entry *p,
                                     uint64_t missing, int foreign,
                                     ps_payload_page **touched) {
  size_t i;
  if (!p->count || p->pins || (foreign && entry_foreign_pinned(s, p)))
    return missing;
  for (i = 0; i < p->count && missing; ++i) {
    const ps_extent *extent = &p->extents[i];
    uint64_t bytes = aligned(extent->bytes);
    if (extent->flags & EXTENT_PACKED) {
      int pinned = 0;
      ps_payload_page *page = payload_find(s, extent->offset);
      if (!page || page->external_pins || page->reclaim_cells == page->occupied)
        continue;
      if (!page->reclaim_cells) {
        page->reclaim_next = *touched;
        *touched = page;
      }
      page->reclaim_cells |= extent_mask(extent);
      if (page->reclaim_cells != page->occupied ||
          (foreign && (pin_conflict(s, page->run.offset, PS_PAGE, &pinned,
                                    NULL) != LC_OK ||
                       pinned)))
        continue;
    }
    missing = bytes < missing ? missing - bytes : 0;
  }
  return missing;
}
/* Whole-stream retirement marks its runs deleted in place. They keep their
 * last checkpoint references and payload ownership until root selection.
 * Include those dirty deletions and replaced entries in one forecast. Packed
 * contributors share a cell mask, and the full-page witness check covers
 * foreign readers of cells absent from this projection. */
static int retirement_can_fund(ps_space *s, uint64_t missing) {
  ps_entry *p;
  ps_payload_page *page, *touched = NULL;
  int foreign = 0;
  if (missing > s->capacity - PS_DATA || (!s->dirty && !s->retired))
    return 0;
  if (s->range_pins && physical_presence(s, &foreign, NULL) != LC_OK)
    return 0;
  for (p = s->dirty; p && missing; p = p->dirty_next)
    if (p->state == 1 && p->deleted)
      missing = retirement_shortfall(s, p, missing, foreign, &touched);
  for (p = s->retired; p && missing; p = p->retired_next)
    missing = retirement_shortfall(s, p, missing, foreign, &touched);
  while (touched) {
    page = touched;
    touched = page->reclaim_next;
    page->reclaim_next = NULL;
    page->reclaim_cells = 0;
  }
  return missing == 0;
}
/* Under allocation authority, use already admitted credits when eligible
 * retirements can cover the requested amount. The caller resamples the
 * selected directory and reader witnesses before admitting work. */
static int reclaim_capacity(ps_space *s, uint64_t missing, int *attempted,
                            lc_error *e) {
  uint64_t bound;
  int rc;
  if (*attempted || !missing || !retirement_can_fund(s, missing))
    return LC_ERR_INVALID;
  rc = metadata_claimed_bound(s, s->metadata, s->live_entries, &bound, NULL);
  if (rc != LC_OK ||
      ordinary_shortfall(s, metadata_target(s, bound), 0, 0, NULL))
    return LC_ERR_INVALID;
  *attempted = 1;
  lc_error_cleanup(e);
  if (s->journal || s->dirty)
    return checkpoint_locked(s, e);
  rc = pins_reclaim_locked(s, e);
  if (rc == LC_OK)
    collect_retired(s);
  return rc == LC_OK ? healthy(s, e) : rc;
}
int ps_internal_budget_admit(ps_space *s, int maintenance,
                             const ps_budget_plan *prior,
                             const ps_budget_plan *proposed, lc_error *e) {
  ps_budget_plan empty = {0};
  uint64_t floor = maintenance ? 0 : PS_MAINTENANCE_RESERVE, missing = 0;
  uint64_t record = 0;
  int rc, attempted = 0;
  if (s->allocation_guard && !s->credit_record &&
      (s->reserved_metadata || s->reserved_entries)) {
    rc = ps_readiness_credit_reserve(s, e);
    if (rc != LC_OK)
      return rc;
    if (!ps_readiness_credit_registered(s))
      record = PS_PAGE;
  }
  rc = capacity_allowance(s, s->metadata, s->live_entries, record, floor, NULL,
                          &missing, e);
  if (rc != LC_ERR_INVALID || !missing)
    return rc;
  /* Raising the standing maintenance floor may need the same retirement
   * pass as opening or growing a budget. It has no tentative credits to
   * remove while the checkpoint runs. */
  if (!proposed)
    proposed = &empty;
  if (!prior)
    prior = &empty;
  s->reserved_payload =
      s->reserved_payload - proposed->payload_bytes + prior->payload_bytes;
  s->reserved_metadata =
      s->reserved_metadata - proposed->metadata_bytes + prior->metadata_bytes;
  s->reserved_entries =
      s->reserved_entries - proposed->entries + prior->entries;
  rc = reclaim_capacity(s, missing, &attempted, e);
  s->reserved_payload =
      s->reserved_payload - prior->payload_bytes + proposed->payload_bytes;
  s->reserved_metadata =
      s->reserved_metadata - prior->metadata_bytes + proposed->metadata_bytes;
  s->reserved_entries =
      s->reserved_entries - prior->entries + proposed->entries;
  if (rc == LC_OK)
    rc = checkpoint_allowance_floor(s, s->metadata, s->live_entries, record,
                                    floor, NULL, e);
  return rc;
}
/* A shared publisher terminates a complete frame with a clear next header.
 * An adopter can distinguish obsolete bytes from an abandoned append even
 * when a shorter frame has overwritten only part of the previous generation. */
static size_t journal_end_marker(size_t offset) {
  size_t remaining = PS_JOURNAL - offset;
  return remaining < 64 ? remaining : 64;
}
/* The caller holds the container mutex and is about to mutate. A successor
 * to the checkpointed grant can share the deferred recovery barrier. */
static int prepare_recovery_write(ps_space *s, lc_error *error) {
  if (s->open_grant_covered &&
      __atomic_load_n(&s->deferred_recovery, __ATOMIC_ACQUIRE) &&
      s->token_limit <= PS_MAX_INCARNATION - TOKEN_RANGE)
    return ps_internal_token_range(s, error);
  return LC_OK;
}
static int write_journal_frame(ps_space *s, unsigned char *wire, size_t bytes,
                               lc_error *error) {
  size_t length = bytes;
  int rc = prepare_recovery_write(s, error);
  if (rc != LC_OK)
    return rc;
  if (s->journal_image) {
    size_t marker = journal_end_marker(s->journal + bytes);
    memset(wire + bytes, 0, marker);
    length += marker;
  }
  ps_readiness_begin(s, PS_READINESS_JOURNAL);
  rc = journal_transfer(s, s->journal_start + s->journal, wire, length, 1,
                        error);
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
  if (s->sequence == LC_U64_MAX ||
      __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED) == LC_U64_MAX)
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
  if (rc == LC_OK && event == PS_PUT &&
      s->keys.count == key_index_limit(s->keys.capacity) && !lookup(s, p->key))
    rc = key_index_reserve(s, 1, error);
  if (rc == LC_OK) {
    encode_entry(wire, p, s->sequence + 1, event);
    rc = write_journal_frame(s, wire, bytes, error);
  }
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
static uint64_t token_record_limit(ps_space *s, const unsigned char *record) {
  uint64_t limit = ps_wire_get64(record + 8);
  return !memcmp(record, "LCPSID01", 8) && limit &&
                 limit <= PS_MAX_INCARNATION && limit % TOKEN_RANGE == 0 &&
                 !memcmp(record + 16, s->identity, 16) &&
                 ps_wire_get32(record + 32) == checksum(record, 32) &&
                 !ps_wire_get32(record + 36)
             ? limit
             : 0;
}
static int decode_token_limit(ps_space *s, const unsigned char *first,
                              const unsigned char *second, uint64_t *limit,
                              int *selected, lc_error *error) {
  uint64_t a, b, prior;
  a = token_record_limit(s, first);
  b = token_record_limit(s, second);
  *selected = b > a;
  *limit = *selected ? b : a;
  prior = *selected ? a : b;
  if (*limit < s->token_limit ||
      (prior && *limit - prior != TOKEN_RANGE && *limit - prior != TOKEN_POOL))
    return err(error, LC_ERR_PROTOCOL,
               "invalid container incarnation grant records");
  return LC_OK;
}
static int write_token_limit(ps_space *s, int slot, uint64_t limit,
                             lc_error *error) {
  unsigned char record[PS_TOKEN_RECORD_BYTES] = {0};
  memcpy(record, "LCPSID01", 8);
  ps_wire_put64(record + 8, limit);
  memcpy(record + 16, s->identity, 16);
  ps_wire_put32(record + 32, checksum(record, 32));
  return file_transfer(s, record, sizeof(record),
                       (uint64_t)slot * PS_PAGE + PS_TOKEN_RECORD_OFFSET, 1,
                       NULL, error);
}
/* The preceding slot remains intact. A new pool that persists ahead of its
 * barrier can be skipped safely: it does not advance the journal prefix. */
static int grant_tokens(ps_space *s, int slot, uint64_t limit,
                        lc_error *error) {
  uint64_t count = ps_coordination_shared(s->coordination) &&
                           limit <= PS_MAX_INCARNATION - TOKEN_POOL
                       ? TOKEN_POOL
                       : TOKEN_RANGE;
  int rc;
  if (limit > PS_MAX_INCARNATION - count)
    return err(error, LC_ERR_INVALID, "container incarnation ranges exhausted");
  rc = write_token_limit(s, slot, limit + count, error);
  if (rc == LC_OK) {
    s->token_next = limit + 1;
    s->token_limit = limit + count;
    s->token_end = limit + TOKEN_RANGE;
  }
  return rc;
}
int ps_internal_token_range(ps_space *s, lc_error *error) {
  ps_coordination_guard *guard = NULL;
  ps_readiness_certificate certificate;
  unsigned char first[PS_TOKEN_RECORD_BYTES], second[PS_TOKEN_RECORD_BYTES];
  uint64_t limit;
  int rc, slot;
  if (s->token_end)
    return s->token_next <= s->token_end
               ? LC_OK
               : err(error, LC_ERR_INVALID,
                     "incarnation grant consumed; close and reopen");
  rc = ps_internal_capture_begin(s, &guard, error);
  if (rc == LC_OK)
    rc = ps_readiness_read(s, &certificate, error);
  if (rc == LC_OK && certificate.grant_used < certificate.grant_limit) {
    s->token_next = certificate.grant_used + 1;
    s->token_end = certificate.grant_used + TOKEN_RANGE;
  } else if (rc == LC_OK) {
    /* A killed refill may have written a newer frontier without registering
     * any owner. Stabilize that slot before spending it. Overwriting the
     * other slot here could destroy the only known durable grant. */
    rc = transfer(s, first, sizeof(first), PS_TOKEN_RECORD_OFFSET, 0, error);
    if (rc == LC_OK)
      rc = transfer(s, second, sizeof(second), PS_PAGE + PS_TOKEN_RECORD_OFFSET,
                    0, error);
    if (rc == LC_OK)
      rc = decode_token_limit(s, first, second, &limit, &slot, error);
    if (rc == LC_OK && !s->readiness && limit != s->token_limit)
      rc =
          err(error, LC_ERR_PROTOCOL, "exclusive incarnation frontier changed");
    if (rc == LC_OK && !s->readiness && !s->open_grant_covered)
      rc = settle_recovery(s, error);
    if (rc == LC_OK && (!s->readiness || limit == certificate.grant_limit))
      rc = grant_tokens(s, 1 - slot, limit, error);
    else if (rc == LC_OK && limit - certificate.grant_limit <= TOKEN_POOL) {
      s->token_next = certificate.grant_limit + 1;
      s->token_end = certificate.grant_limit + TOKEN_RANGE;
      s->token_limit = limit;
    } else if (rc == LC_OK)
      rc = err(error, LC_ERR_PROTOCOL, "unassigned incarnation pool invalid");
    if (rc == LC_OK)
      rc = sync_file(s, error);
  }
  if (rc == LC_OK && s->readiness)
    rc = ps_readiness_assign_grant(s, error);
  return allocation_end(s, guard, rc, error);
}
static void destroy(ps_space *s) {
  ps_entry_allocation *block;
  uint64_t auxiliary;
  ps_run *r;
  ps_meta_page *page;
  ps_payload_page *payload;
  while (s->entry_allocations) {
    block = s->entry_allocations;
    s->entry_allocations = block->next;
    if (!block->owner)
      entry_fields_free((ps_entry *)(block + 1));
    else {
      auxiliary = __atomic_load_n(&block->auxiliary, __ATOMIC_RELAXED);
      for (; auxiliary; auxiliary &= auxiliary - 1)
        entry_fields_free(entry_slab_entry(block, __builtin_ctzll(auxiliary)));
    }
    lc_free_with_allocator(NULL, block);
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
  lc_free_with_allocator(NULL, s->keys.slots);
  lc_free_with_allocator(NULL, s->buckets);
  lc_free_with_allocator(NULL, s->journal_image);
  if (s->credit_record) {
    ps_unmap_page(&s->credit_mapping);
    lc_free_with_allocator(NULL, s->credit_record);
  }
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
  uint64_t capacity = ps_wire_get64(p + 16);
  uint64_t offset = ps_wire_get64(p + 32), generation = ps_wire_get64(p + 40);
  uint64_t journal_start = ps_wire_get64(p + ROOT_JOURNAL_START);
  uint32_t covered = ps_wire_get32(p + ROOT_JOURNAL_COVERED);
  return ps_wire_get64(p) == ROOT_MAGIC && ps_wire_get64(p + 8) &&
         capacity >= PS_MIB && capacity <= PS_MAX_CAPACITY &&
         capacity % PS_MIB == 0 &&
         ((!offset && !generation) ||
          (offset >= PS_METADATA_START && offset % PS_PAGE == 0 &&
           offset <= capacity - PS_PAGE &&
           (offset < JOURNAL_OFFSET || offset >= PS_DATA) && generation &&
           generation <= ps_wire_get64(p + 8))) &&
         ps_wire_get64(p + 48) && ps_wire_get64(p + 48) <= PS_MAX_INCARNATION &&
         ps_wire_get64(p + 48) % TOKEN_RANGE == 0 &&
         ps_wire_get32(p + 72) == PS_ROOT_LAYOUT &&
         journal_start <= LC_U64_MAX - PS_JOURNAL && covered <= PS_JOURNAL &&
         covered <= journal_start &&
         checksum(p, ROOT_CHECKSUM) == ps_wire_get32(p + ROOT_CHECKSUM);
}
static int root_follows(const unsigned char *root, const unsigned char *prior) {
  return !memcmp(root + 56, prior + 56, 16) &&
         ps_wire_get64(root + 8) - 1 == ps_wire_get64(prior + 8) &&
         ps_wire_get64(root + 24) >= ps_wire_get64(prior + 24) &&
         ps_wire_get64(root + ROOT_JOURNAL_START) -
                 ps_wire_get32(root + ROOT_JOURNAL_COVERED) ==
             ps_wire_get64(prior + ROOT_JOURNAL_START);
}
static int checkpoint_retirement(const ps_readiness_certificate *certificate,
                                 uint64_t generation, uint64_t sequence,
                                 lc_error *error) {
  if ((certificate->pending & PS_READINESS_CHECKPOINT_RETIRE) &&
      (generation <= certificate->root || sequence != certificate->ready))
    return err(error, LC_ERR_PROTOCOL,
               "checkpoint retirement disagrees with the selected root");
  return LC_OK;
}
static int canonical_certificate(ps_space *s, ps_readiness_certificate *out,
                                 lc_error *error) {
  int rc = ps_readiness_read(s, out, error);
  if (rc == LC_OK && (out->pending & PS_READINESS_RECOVERY))
    return err(error, LC_ERR_PENDING,
               "container recovery cleanup awaits durability");
  if (rc == LC_OK)
    s->token_limit = out->grant_limit;
  return rc;
}
static int canonical_observe(ps_space *s, lc_error *error) {
  unsigned char roots[2 * PS_ROOT_BYTES];
  const unsigned char *root;
  ps_readiness_certificate certificate;
  int rc, a, b;
  if (!s->journal_image)
    return err(error, LC_ERR_PENDING, "container canonical view needs refresh");
  rc = canonical_certificate(s, &certificate, error);
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
  root = b && (!a || ps_wire_get64(roots + PS_ROOT_BYTES + 8) >
                         ps_wire_get64(roots + 8))
             ? roots + PS_ROOT_BYTES
             : roots;
  if ((!a && !b) || ps_wire_get64(root + 8) != s->generation ||
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
  static const unsigned char zero[64] = {0};
  unsigned char next[64];
  int rc;
  if (s->generation < certificate->root)
    return err(error, LC_ERR_PENDING,
               "certified checkpoint view needs refresh");
  s->peer_durable = certificate->durable;
  observe_local_durability(s);
  if (!verify_end && !certificate->pending && s->readiness &&
      s->generation == certificate->root) {
    if (!replay && s->sequence < certificate->ready)
      return err(error, LC_ERR_PENDING, "container journal view needs refresh");
    return import_certified_journal(s, certificate, 0, error);
  }
  while ((verify_end || s->sequence < certificate->ready) &&
         s->journal <= PS_JOURNAL - sizeof(next)) {
    rc = journal_transfer(s, s->journal_start + s->journal, next, sizeof(next),
                          0, error);
    if (rc != LC_OK)
      return rc;
    if (!memcmp(next, s->journal_image + s->journal, sizeof(next)))
      break;
    /* A recovering join can erase old checkpoint residue without advancing
     * the ready sequence. Only a clean, exact certificate and a zero disk
     * header authorize replacing that observer's cached end. */
    if (!certificate->pending && s->generation == certificate->root &&
        s->sequence == certificate->ready &&
        !memcmp(next, zero, sizeof(next))) {
      memset(s->journal_image + s->journal, 0, sizeof(next));
      break;
    }
    if (s->readiness && s->sequence >= certificate->ready) {
      ps_readiness_begin(s, PS_READINESS_JOURNAL);
      return err(error, LC_ERR_PENDING,
                 "journal boundary is not yet certified");
    }
    /* Allocation helpers may already hold state derived from the old view.
     * Advancing the journal would revise that state. Grant publication is
     * observed separately from the journal through the certificate. */
    if (!replay || (ps_wire_get32(next) != REVISION_MAGIC &&
                    ps_wire_get32(next) != EVENT_MAGIC))
      return err(error, LC_ERR_PENDING, "container journal view needs refresh");
    rc = replay_frame(s, next, certificate->ready, error);
    if (rc != LC_OK)
      return rc;
  }
  if (s->readiness && s->sequence < certificate->ready)
    return err(error, LC_ERR_PENDING, "certified journal view needs refresh");
  if (s->readiness && s->generation == certificate->root &&
      s->journal != certificate->journal_bytes)
    return err(error, LC_ERR_PROTOCOL, "inconsistent certified journal bound");
  if (verify_end && (certificate->pending & PS_READINESS_JOURNAL))
    ps_readiness_finish(s, PS_READINESS_JOURNAL);
  return LC_OK;
}
#define META_DIRECTORY_BATCH 128U
#define META_READ_PAGES META_DIRECTORY_BATCH
#define META_READ_AHEAD_PAGES 8U
typedef struct metadata_read_page {
  uint64_t offset, generation, following;
  size_t used, batch;
  struct metadata_read_page *next, **link;
  unsigned char bytes[PS_PAGE];
} metadata_read_page;
typedef struct metadata_directory_entry {
  ps_entry *decoded;
  /* Inline bindings go straight into their resident entry slots. Only
   * deferred descriptor reads need a copy of the directory reference. */
  ps_key key;
  ps_descriptor_ref ref;
  ps_entry *resident;
} metadata_directory_entry;
typedef struct projection_packed_binding {
  ps_key key;
  ps_directory_value value;
  uint64_t hash;
  ps_entry *resident;
} projection_packed_binding;
typedef struct checkpoint_slab {
  ps_entry_allocation *block;
  unsigned int next;
} checkpoint_slab;
typedef struct metadata_recovery {
  ps_space *space, *resident;
  payload_rebuild *payload;
  unsigned char *wire;
  uint64_t cohort_origin;
  int certified_root, reuse, shared_leaf;
  /* Directory order need not match the packing order of descriptor pages.
   * Retain a bounded set of checked immutable pages for this traversal. */
  metadata_read_page *slots[META_READ_PAGES], *buckets[META_READ_PAGES];
  size_t next_slot, batch;
  metadata_directory_entry *entries;
  size_t count, capacity, deferred;
  projection_packed_binding *packed;
  size_t packed_count;
  checkpoint_slab open_slab;
} metadata_recovery;
static void metadata_recovery_close(metadata_recovery *r) {
  size_t i;
  for (i = 0; i < META_READ_PAGES; ++i)
    lc_free_with_allocator(NULL, r->slots[i]);
  for (i = 0; i < r->count; ++i)
    if (r->entries[i].decoded)
      ps_internal_entry_free(r->entries[i].decoded);
  lc_free_with_allocator(NULL, r->entries);
  lc_free_with_allocator(NULL, r->packed);
  lc_free_with_allocator(NULL, r);
}
static metadata_read_page *metadata_read_find(const metadata_recovery *r,
                                              uint64_t offset) {
  metadata_read_page *p;
  for (p = r->buckets[(offset / PS_PAGE) % META_READ_PAGES]; p; p = p->next)
    if (p->offset == offset)
      return p;
  return NULL;
}
static int metadata_read_cache_page(metadata_recovery *r, uint64_t offset,
                                    const unsigned char *read_ahead,
                                    metadata_read_page **out, lc_error *error) {
  ps_space *s = r->space;
  metadata_read_page **bucket, *page;
  const unsigned char *buffer;
  size_t examined;
  int rc = LC_OK;
  if (!metadata_offset(s, offset))
    return err(error, LC_ERR_PROTOCOL, "metadata record outside container");
  page = metadata_read_find(r, offset);
  if (page) {
    page->batch = r->batch;
    *out = page;
    return LC_OK;
  }
  bucket = &r->buckets[(offset / PS_PAGE) % META_READ_PAGES];
  /* Prefer an unused slot or an older batch. A descriptor spanning pages can
   * take the working set beyond one page per binding. In that case the walk
   * makes one full turn and reuses the next slot. */
  for (examined = 0; examined < META_READ_PAGES; ++examined) {
    page = r->slots[r->next_slot];
    if (!page || page->batch != r->batch)
      break;
    r->next_slot = (r->next_slot + 1) % META_READ_PAGES;
  }
  page = r->slots[r->next_slot];
  if (page) {
    *page->link = page->next;
    if (page->next)
      page->next->link = page->link;
  } else {
    page = lc_alloc_with_allocator(NULL, sizeof(*page));
    if (!page)
      return err(error, LC_ERR_NOMEM,
                 "cannot cache checkpoint descriptor page");
    r->slots[r->next_slot] = page;
  }
  if (read_ahead)
    memcpy(page->bytes, read_ahead, PS_PAGE);
  else
    rc = transfer(s, page->bytes, PS_PAGE, offset, 0, error);
  if (rc != LC_OK)
    return rc;
  buffer = page->bytes;
  page->offset = offset;
  page->batch = r->batch;
  page->generation = ps_wire_get64(buffer + 16);
  page->following = ps_wire_get64(buffer + 8);
  page->used = ps_wire_get32(buffer + 4);
  if (ps_wire_get32(buffer) != META_MAGIC || !page->used ||
      page->used > META_PAYLOAD || !page->generation ||
      page->generation > s->generation ||
      ps_wire_get64(buffer + 24) != offset ||
      memcmp(buffer + 32, s->identity, 16) ||
      ps_wire_get32(buffer + 48) != metadata_crc(buffer) ||
      ps_wire_get32(buffer + 52) || ps_wire_get64(buffer + 56) ||
      (page->following && !metadata_offset(s, page->following)))
    return err(error, LC_ERR_PROTOCOL, "invalid packed metadata page");
  page->next = *bucket;
  page->link = bucket;
  if (page->next)
    page->next->link = &page->next;
  *bucket = page;
  r->next_slot = (r->next_slot + 1) % META_READ_PAGES;
  *out = page;
  return LC_OK;
}
static int metadata_physical_order(const void *a, const void *b) {
  uint64_t left = (*(metadata_directory_entry *const *)a)->ref.offset;
  uint64_t right = (*(metadata_directory_entry *const *)b)->ref.offset;
  return left < right ? -1 : left != right;
}
static int metadata_read_ahead(metadata_recovery *r,
                               metadata_directory_entry *const *entries,
                               size_t n, lc_error *error) {
  uint64_t offsets[META_DIRECTORY_BATCH];
  uint64_t offset;
  metadata_read_page *page;
  size_t i, start, part, count = 0;
  int rc = LC_OK;
  ++r->batch;
  for (i = 0; i < n; ++i) {
    offset = entries[i]->ref.offset / PS_PAGE * PS_PAGE;
    if (count && offset == offsets[count - 1])
      continue;
    if (!metadata_offset(r->space, offset))
      return err(error, LC_ERR_PROTOCOL, "metadata record outside container");
    /* Protect every known starting page before loading adjacent misses. */
    page = metadata_read_find(r, offset);
    if (page)
      page->batch = r->batch;
    offsets[count++] = offset;
  }
  for (i = 0; rc == LC_OK && i < count;) {
    start = i++;
    if (metadata_read_find(r, offsets[start]))
      continue;
    while (i < count && i - start < META_READ_AHEAD_PAGES &&
           offsets[i] == offsets[i - 1] + PS_PAGE &&
           !metadata_read_find(r, offsets[i]))
      ++i;
    if (i - start < 2)
      continue;
    rc = transfer(r->space, r->wire, (i - start) * PS_PAGE, offsets[start], 0,
                  error);
    for (part = start; rc == LC_OK && part < i; ++part)
      rc = metadata_read_cache_page(
          r, offsets[part], r->wire + (part - start) * PS_PAGE, &page, error);
  }
  return rc;
}
static int recover_metadata_claim(metadata_recovery *r, uint64_t offset,
                                  lc_error *error) {
  ps_space *s = r->space;
  if (s->capacity <= REBUILD_BITMAP_CAPACITY && offset >= PS_DATA)
    return payload_exclude(s, r->payload, offset, PS_PAGE, error);
  return exclude_run(s, offset, PS_PAGE, error);
}
static int recover_directory_page(void *v, ps_directory_root ref,
                                  lc_error *error) {
  metadata_recovery *r = v;
  ps_space *s = r->space;
  const ps_meta_page *old =
      r->reuse ? metadata_find(r->resident, ref.offset) : NULL;
  ps_meta_page *p;
  int rc;
  /* The page callback precedes a leaf's bindings. Descending an internal
   * page calls it again before visiting any binding. */
  r->shared_leaf = old && old->index && old->generation == ref.generation;
  if (metadata_find(s, ref.offset))
    return err(error, LC_ERR_PROTOCOL, "overlapping directory ownership");
  p = lc_calloc_with_allocator(NULL, 1, sizeof(*p));
  if (!p)
    return err(error, LC_ERR_NOMEM, "cannot recover directory ownership");
  rc = recover_metadata_claim(r, ref.offset, error);
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
static int recover_descriptor_page(metadata_recovery *r, uint64_t offset,
                                   uint64_t generation, size_t used,
                                   uint64_t following, lc_error *error) {
  ps_space *s = r->space;
  ps_meta_page *p = metadata_find(s, offset);
  int rc;
  if (p && (p->index || p->generation != generation || p->used != used ||
            p->following != following))
    return err(error, LC_ERR_PROTOCOL, "conflicting metadata ownership");
  if (!p) {
    p = lc_calloc_with_allocator(NULL, 1, sizeof(*p));
    if (!p)
      return err(error, LC_ERR_NOMEM, "cannot recover descriptor ownership");
    rc = recover_metadata_claim(r, offset, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(NULL, p);
      return rc;
    }
    p->run.offset = offset;
    p->run.bytes = PS_PAGE;
    p->generation = generation;
    p->used = used;
    p->following = following;
    metadata_register(s, p);
  }
  ++p->references;
  return LC_OK;
}
static int metadata_read(metadata_recovery *recovery, ps_descriptor_ref ref,
                         const unsigned char **wire, lc_error *error) {
  uint64_t offset = ref.offset / PS_PAGE * PS_PAGE, generation = 0;
  size_t at = (size_t)(ref.offset % PS_PAGE), done = 0, take;
  metadata_read_page *page = NULL;
  int rc;
  while (done < ref.bytes) {
    rc = metadata_read_cache_page(recovery, offset, NULL, &page, error);
    if (rc != LC_OK)
      return rc;
    if ((generation && page->generation != generation) || at < META_HEADER ||
        at >= META_HEADER + page->used)
      return err(error, LC_ERR_PROTOCOL, "invalid packed metadata page");
    generation = page->generation;
    rc = recover_descriptor_page(recovery, offset, generation, page->used,
                                 page->following, error);
    if (rc != LC_OK)
      return rc;
    take = META_HEADER + page->used - at;
    if (take > ref.bytes - done)
      take = (size_t)ref.bytes - done;
    if (!done && take == ref.bytes) {
      *wire = page->bytes + at;
      return LC_OK;
    }
    memcpy(recovery->wire + done, page->bytes + at, take);
    done += take;
    if (done < ref.bytes && (!page->following || page->used != META_PAYLOAD))
      return err(error, LC_ERR_PROTOCOL, "truncated packed metadata record");
    offset = page->following;
    at = META_HEADER;
  }
  *wire = recovery->wire;
  return LC_OK;
}
/* A checked copy-on-write leaf that kept its identity still owns each
 * immutable descriptor it named. Rebuild references to its checked page
 * chain in the draft without copying the old projection's mutable links. */
static int reuse_directory_entry(metadata_recovery *r,
                                 metadata_directory_entry *item,
                                 lc_error *error) {
  const ps_entry *old = item->resident;
  ps_descriptor_ref ref = item->ref;
  uint64_t offset = ref.offset / PS_PAGE * PS_PAGE, generation = 0;
  size_t at = (size_t)(ref.offset % PS_PAGE), remaining = (size_t)ref.bytes;
  ps_entry *p;
  int rc;
  while (remaining) {
    const ps_meta_page *page = metadata_find(r->resident, offset);
    size_t take;
    if (!page || page->index ||
        (generation && generation != page->generation) || at < META_HEADER ||
        at >= META_HEADER + page->used)
      return err(error, LC_ERR_PROTOCOL, "invalid retained descriptor span");
    generation = page->generation;
    rc = recover_descriptor_page(r, offset, generation, page->used,
                                 page->following, error);
    if (rc != LC_OK)
      return rc;
    take = META_HEADER + page->used - at;
    if (remaining <= take)
      break;
    remaining -= take;
    offset = page->following;
    at = META_HEADER;
  }
  p = space_entry_new(r->space, old->key.kind, old->count, 1);
  if (!p)
    return err(error, LC_ERR_NOMEM, "cannot stage retained descriptor");
  p->key = old->key;
  p->token = old->token;
  p->bytes = old->bytes;
  p->count = old->count;
  if (p->count)
    memcpy(p->extents, old->extents, p->count * sizeof(*p->extents));
  p->checkpoint_ref = ref;
  p->projection = item->resident;
  item->decoded = p;
  if (p->key.kind == PS_KEY_METADATA)
    return ps_internal_reserve_entry(p, old->deletion_bytes, error);
  if (p->key.kind == PS_CATALOG)
    return ps_catalog_copy_record(p, old, error);
  return LC_OK;
}
static int decode_directory_entry(metadata_recovery *recovery,
                                  metadata_directory_entry *item,
                                  lc_error *error) {
  ps_space *s = recovery->space;
  ps_descriptor_ref ref = item->ref;
  const unsigned char *wire = NULL;
  ps_entry *p = NULL;
  ps_entry *old =
      recovery->resident ? lookup(recovery->resident, item->key) : NULL;
  int event, rc;
  if (old && old->token != ref.incarnation)
    old = NULL;
  if (ref.bytes < 64 || ref.bytes > PS_JOURNAL)
    return err(error, LC_ERR_PROTOCOL, "invalid checkpoint descriptor span");
  rc = metadata_read(recovery, ref, &wire, error);
  if (rc == LC_OK)
    rc = decode_entry(s, wire, (size_t)ref.bytes, old != NULL, &p, &event,
                      error);
  if (rc == LC_OK &&
      (event != PS_PUT || !same_key(p->key, item->key) ||
       p->token != ref.incarnation || ps_wire_get64(wire + 8) > s->sequence))
    rc = err(error, LC_ERR_PROTOCOL, "checkpoint descriptor binding mismatch");
  if (rc == LC_OK) {
    if (p->key.kind == PS_STREAM &&
        (!recovery->certified_root ||
         ps_wire_get64(wire + 8) <= recovery->cohort_origin))
      p->stream.head->admitted = 0;
    p->checkpoint_ref = ref;
    p->projection = old;
    item->decoded = p;
  } else if (p)
    ps_internal_entry_free(p);
  return rc;
}
static int directory_extent_in_bounds(const ps_space *s,
                                      const ps_descriptor_ref *ref) {
  return ref->incarnation <= s->token_limit && ref->offset >= PS_DATA &&
         ref->offset <= s->capacity && ref->bytes <= s->capacity - ref->offset;
}
static int check_packed_binding(ps_space *s, const ps_directory_value *value,
                                lc_error *error) {
  ps_descriptor_ref ref = value->extent.ref;
  if (!directory_extent_in_bounds(s, &ref) ||
      !valid_packed_extent(ref.offset, ref.bytes) ||
      (value->extent.deletion_bytes > ref.bytes &&
       value->extent.deletion_bytes - ref.bytes > 512))
    return err(error, LC_ERR_PROTOCOL, "invalid packed directory extent");
  return LC_OK;
}
static void unpack_binding(ps_entry *p, ps_key key,
                           const ps_directory_value *value) {
  p->key = key;
  p->token = value->extent.ref.incarnation;
  p->bytes = value->extent.ref.bytes;
  p->count = 1;
  p->extents[0].offset = value->extent.ref.offset;
  p->extents[0].bytes = value->extent.ref.bytes;
  p->extents[0].crc = value->extent.crc;
  p->extents[0].flags = value->tag.storage == PS_DIRECTORY_OWNED_RUN
                            ? EXTENT_OWNED
                            : EXTENT_PACKED;
}
static int load_owned_binding(metadata_recovery *recovery, ps_key key,
                              const ps_directory_value *value, ps_entry **out,
                              lc_error *error) {
  ps_space *s = recovery->space;
  const ps_descriptor_ref *ref = &value->extent.ref;
  ps_entry *p,
      *old = recovery->resident ? lookup(recovery->resident, key) : NULL;
  /* The directory checked the alignment and extent size. A plain run's
   * capacity is its extent minus the immutable identity and footer area. */
  if (!directory_extent_in_bounds(s, ref) || ref->bytes <= PS_RUN_HEADER ||
      key.key > (uint64_t)LC_I64_MAX - (ref->bytes - PS_RUN_HEADER))
    return err(error, LC_ERR_PROTOCOL, "invalid inline owned run extent");
  if (old && old->token != ref->incarnation)
    old = NULL;
  p = entry_slab_take(s, PS_OWNED_RUN, old != NULL);
  if (!p)
    return err(error, LC_ERR_NOMEM, "cannot recover inline owned run");
  unpack_binding(p, key, value);
  p->stream.run->capacity = ref->bytes - PS_RUN_HEADER;
  p->projection = old;
  *out = p;
  return LC_OK;
}
static int load_stream_binding(metadata_recovery *r, ps_key key,
                               const ps_directory_stream *stream,
                               ps_entry **out, lc_error *error) {
  ps_space *s = r->space;
  ps_entry *p, *old = r->resident ? lookup(r->resident, key) : NULL;
  if (stream->incarnation > s->token_limit ||
      stream->admitted_at > s->sequence ||
      !valid_stream_span(stream->tail, stream->capacity, stream->runs))
    return err(error, LC_ERR_PROTOCOL, "invalid inline stream head");
  if (old && old->token != stream->incarnation)
    old = NULL;
  p = entry_slab_take(s, PS_STREAM, old != NULL);
  if (!p)
    return err(error, LC_ERR_NOMEM, "cannot recover inline stream head");
  p->key = key;
  p->token = stream->incarnation;
  p->bytes = stream->tail;
  p->stream.head->capacity = stream->capacity;
  p->stream.head->runs = stream->runs;
  p->stream.head->sealed = stream->storage == PS_DIRECTORY_SEALED_STREAM;
  p->stream.head->admitted =
      r->certified_root && stream->admitted_at > r->cohort_origin;
  p->projection = old;
  *out = p;
  return LC_OK;
}
static int load_namespace_binding(metadata_recovery *r, ps_key key,
                                  const ps_directory_name *name, ps_entry **out,
                                  lc_error *error) {
  ps_space *s = r->space;
  ps_entry *p, *old = r->resident ? lookup(r->resident, key) : NULL;
  if (key.key > s->token_limit)
    return err(error, LC_ERR_PROTOCOL, "invalid inline namespace identity");
  if (old && old->token != key.key)
    old = NULL;
  p = space_entry_new(s, PS_CATALOG, 0, old != NULL);
  if (!p)
    return err(error, LC_ERR_NOMEM, "cannot recover inline namespace");
  p->key = key;
  p->token = key.key;
  p->projection = old;
  *out = p;
  return ps_catalog_decode_namespace(p, name->bytes, name->length, error);
}
static int register_packed_binding(ps_space *s, ps_entry *p,
                                   uint64_t deletion_bytes, lc_error *error) {
  if (deletion_bytes) {
    int rc = ps_internal_reserve_entry(p, deletion_bytes, error);
    if (rc != LC_OK)
      return rc;
    ps_internal_reserve_install(s, p, NULL);
  }
  register_entry(s, p);
  value_register(s, p);
  p->state = 1;
  s->metadata += entry_bytes(p);
  ++s->live_entries;
  return LC_OK;
}
static int load_projection_binding(metadata_recovery *r,
                                   const projection_packed_binding *binding,
                                   lc_error *error) {
  ps_space *s = r->space;
  ps_key key = binding->key;
  const ps_directory_value *value = &binding->value;
  ps_descriptor_ref ref = value->extent.ref;
  ps_entry *p, *old = binding->resident;
  int rc = check_packed_binding(s, value, error);
  if (rc != LC_OK)
    return rc;
  if (old && old->token != ref.incarnation)
    old = NULL;
  /* The checked leaf gives both identity and content of this descriptor.
   * Borrow only a complete match with no mutable deletion-reserve node. */
  if (old && projection_borrowed(s, old) && !value->extent.deletion_bytes &&
      !old->deletion_bytes && !old->deleted && old->count == 1 &&
      old->bytes == ref.bytes && old->extents[0].offset == ref.offset &&
      old->extents[0].bytes == ref.bytes &&
      old->extents[0].crc == value->extent.crc &&
      old->extents[0].flags == EXTENT_PACKED) {
    rc = payload_claim(s, &old->extents[0], r->payload, error);
    if (rc == LC_OK) {
      key_index_add_distinct(&s->keys, old, binding->hash);
      s->metadata += entry_bytes(old);
      ++s->live_entries;
    }
    return rc;
  }
  /* Draft copies that keep a resident incarnation release their slots after
   * remapping. Group those copies away from bindings that survive adoption. */
  p = space_entry_new(s, key.kind, 1, old != NULL);
  if (!p)
    return err(error, LC_ERR_NOMEM, "cannot allocate packed directory entry");
  unpack_binding(p, key, value);
  p->projection = old;
  rc = register_packed_binding(s, p, value->extent.deletion_bytes, error);
  if (rc == LC_OK)
    key_index_add_distinct(&s->keys, p, binding->hash);
  else
    ps_internal_entry_free(p);
  return rc;
}
/* Resolve the window's independent resident probes before walking its
 * payload ownership. The directory traversal has proved distinct keys. */
static int load_projection_batch(metadata_recovery *r, lc_error *error) {
  const ps_key_index *resident = &r->resident->keys;
  ps_key_index *draft = &r->space->keys;
  projection_packed_binding *p;
  size_t i, added = r->packed_count;
  int rc;
  if (!draft->capacity && resident->count > added)
    added = resident->count;
  rc = key_index_reserve(r->space, added, error);
  if (rc != LC_OK)
    return rc;
  if (resident->capacity)
    for (i = 0; i < r->packed_count; ++i)
      __builtin_prefetch(
          &resident->slots[r->packed[i].hash & (resident->capacity - 1)]);
  for (i = 0; i < r->packed_count; ++i) {
    p = &r->packed[i];
    p->resident =
        resident->capacity ? key_slot(resident, p->key, p->hash)->entry : NULL;
    if (p->resident)
      __builtin_prefetch(p->resident->extents);
  }
  for (i = 0; rc == LC_OK && i < r->packed_count; ++i)
    rc = load_projection_binding(r, &r->packed[i], error);
  r->packed_count = 0;
  return rc;
}
static int collect_projection_binding(metadata_recovery *r, ps_key key,
                                      const ps_directory_value *value,
                                      lc_error *error) {
  projection_packed_binding *p;
  if (!r->packed) {
    r->packed =
        lc_alloc_with_allocator(NULL, META_DIRECTORY_BATCH * sizeof(*p));
    if (!r->packed)
      return err(error, LC_ERR_NOMEM, "cannot allocate packed binding batch");
  }
  p = &r->packed[r->packed_count++];
  p->key = key;
  p->value = *value;
  p->hash = key_hash(key);
  return r->packed_count == META_DIRECTORY_BATCH
             ? load_projection_batch(r, error)
             : LC_OK;
}
/* Packed leaf values have no descriptor reads or stream dependencies. Load
 * them straight into the new owner's slabs and size the key index after the
 * walk. This owner is private until recovery has validated its selection. */
static int load_open_binding(metadata_recovery *r, ps_key key,
                             const ps_directory_value *value, lc_error *error) {
  ps_space *s = r->space;
  checkpoint_slab *batch = &r->open_slab;
  ps_entry_allocation *block = batch->block;
  ps_entry_slab_slot *slot;
  ps_entry *p;
  unsigned int index;
  int rc = check_packed_binding(s, value, error);
  if (rc != LC_OK)
    return rc;
  if (!block || batch->next == ENTRY_SLAB_SLOTS) {
    block = lc_calloc_with_allocator(
        NULL, 1, sizeof(*block) + ENTRY_SLAB_SLOTS * sizeof(*slot));
    if (!block)
      return err(error, LC_ERR_NOMEM, "cannot allocate checkpoint entry slab");
    block->owner = s;
    entry_allocation_register(s, block);
    batch->block = block;
    batch->next = 0;
  }
  index = batch->next++;
  block->used |= ((lc_u64)1) << index;
  slot = (ps_entry_slab_slot *)(block + 1) + index;
  p = &slot->entry;
  p->allocation = block;
  p->extents = &slot->extent;
  p->embedded_extents = 1;
  unpack_binding(p, key, value);
  rc = register_packed_binding(s, p, value->extent.deletion_bytes, error);
  return rc == LC_OK ? payload_claim(s, p->extents, r->payload, error) : rc;
}
static int recover_directory_bindings(metadata_recovery *r, lc_error *error) {
  ps_space *s = r->space;
  checkpoint_slab batch = r->open_slab;
  metadata_directory_entry **fetch;
  ps_entry *p;
  size_t i, end, count = 0;
  int rc;
  uint64_t unindexed = r->resident ? 0 : s->live_entries;
  if (r->packed_count) {
    rc = load_projection_batch(r, error);
    if (rc != LC_OK)
      return rc;
  }
  if (unindexed > SIZE_MAX - r->count)
    return err(error, LC_ERR_NOMEM, "container key index exceeds local limit");
  rc = key_index_reserve(s, r->count + (size_t)unindexed, error);
  if (rc != LC_OK)
    return rc;
  /* A fresh open can size the index once after collecting its packed slots.
   * Live adoption has already indexed both private and borrowed bindings. */
  if (unindexed)
    for (p = s->entries; p; p = p->all_next)
      key_index_add_distinct(&s->keys, p, key_hash(p->key));
  if (batch.block && batch.next < ENTRY_SLAB_SLOTS)
    entry_available_link(batch.block);
  fetch = NULL;
  if (r->deferred) {
    fetch = lc_alloc_with_allocator(NULL, r->deferred * sizeof(*fetch));
    if (!fetch)
      return err(error, LC_ERR_NOMEM,
                 "cannot order checkpoint descriptor reads");
  }
  for (i = 0; rc == LC_OK && i < r->count; ++i) {
    metadata_directory_entry *item = &r->entries[i];
    if (item->decoded)
      continue;
    if (item->resident)
      rc = reuse_directory_entry(r, item, error);
    else
      fetch[count++] = item;
  }
  /* Order only the reads still needed. The deferred bindings are installed in
   * directory order, including stream heads before their runs. */
  if (rc == LC_OK && count > 1)
    qsort(fetch, count, sizeof(*fetch), metadata_physical_order);
  for (i = 0; rc == LC_OK && i < count;) {
    end = count - i > META_DIRECTORY_BATCH ? i + META_DIRECTORY_BATCH : count;
    rc = metadata_read_ahead(r, fetch + i, end - i, error);
    for (; rc == LC_OK && i < end; ++i) {
      rc = decode_directory_entry(r, fetch[i], error);
    }
  }
  lc_free_with_allocator(NULL, fetch);
  if (rc != LC_OK)
    return rc;
  for (i = 0; i < r->count; ++i) {
    p = r->entries[i].decoded;
    if (p->key.kind == PS_CATALOG)
      rc = ps_catalog_prepare_record(s, p, 1, error);
    else if (p->key.kind == PS_OWNED_RUN && p->stream.run->mapped)
      rc = owned_map_load(s, p, error);
    if (rc == LC_OK && !r->resident)
      rc = payload_claim_entry(s, p, r->payload, error);
    if (rc != LC_OK)
      return rc;
    if (p->key.kind == PS_CATALOG)
      ps_catalog_apply_record(s, p, 0);
    r->entries[i].decoded = NULL;
    register_entry(s, p);
    install(s, p, NULL);
    if (p->key.kind == PS_OWNED_RUN && !p->stream.run->owned_linked)
      return err(error, LC_ERR_PROTOCOL, "owned run has no stream head");
  }
  return LC_OK;
}
static int capture_directory_entry(void *v, ps_key key,
                                   const ps_directory_value *value,
                                   lc_error *error) {
  metadata_recovery *r = v;
  metadata_directory_entry *item;
  ps_entry *old = NULL;
  size_t capacity;
  if (value->tag.storage == PS_DIRECTORY_PACKED_VALUE)
    return r->resident ? collect_projection_binding(r, key, value, error)
                       : load_open_binding(r, key, value, error);
  if (r->count == r->capacity) {
    if (r->capacity > (size_t)-1 / sizeof(*item) / 2)
      return err(error, LC_ERR_NOMEM, "checkpoint binding list too large");
    capacity = r->capacity ? 2 * r->capacity : META_DIRECTORY_BATCH;
    item =
        lc_realloc_with_allocator(NULL, r->entries, capacity * sizeof(*item));
    if (!item)
      return err(error, LC_ERR_NOMEM, "cannot collect checkpoint bindings");
    r->entries = item;
    r->capacity = capacity;
  }
  if (value->tag.storage == PS_DIRECTORY_DESCRIPTOR && r->shared_leaf) {
    ps_descriptor_ref ref = value->extent.ref;
    if (ref.incarnation <= r->space->token_limit &&
        (ps_internal_immutable_key(key) || key.kind == PS_CATALOG)) {
      old = lookup(r->resident, key);
      if (old &&
          (old->deleted || old->dirty_link || old->token != ref.incarnation ||
           old->checkpoint_ref.incarnation != ref.incarnation ||
           old->checkpoint_ref.offset != ref.offset ||
           old->checkpoint_ref.bytes != ref.bytes))
        old = NULL;
    }
  }
  item = &r->entries[r->count++];
  item->decoded = NULL;
  switch (value->tag.storage) {
  case PS_DIRECTORY_OWNED_RUN:
    return load_owned_binding(r, key, value, &item->decoded, error);
  case PS_DIRECTORY_STREAM:
  case PS_DIRECTORY_SEALED_STREAM:
    return load_stream_binding(r, key, &value->stream, &item->decoded, error);
  case PS_DIRECTORY_NAMESPACE:
    return load_namespace_binding(r, key, &value->name, &item->decoded, error);
  default:
    break; /* Indirect descriptors are fetched in physical page order. */
  }
  item->key = key;
  item->ref = value->extent.ref;
  item->resident = old;
  r->deferred += old == NULL;
  return LC_OK;
}
static int revision_retirement_matches(const ps_entry *entry,
                                       uint64_t incarnation) {
  return entry && !entry->deleted && entry->token == incarnation;
}
/* Validate the entire candidate before changing any selected ownership. A
 * complete inner record is not independently recoverable inside a revision. */
static int decode_revision(ps_space *s, const unsigned char *wire, size_t bytes,
                           recovery_values *values, ps_entry ***out,
                           lc_error *error) {
  ps_entry **entries = NULL, *old;
  size_t count, i, j, cursor = 64, member_bytes, added = 0;
  int event, rc = LC_OK;
  count = ps_wire_get32(wire + 16);
  if (bytes < 64 || ps_wire_get32(wire) != REVISION_MAGIC ||
      ps_wire_get32(wire + 4) != bytes || !count || count > (bytes - 64) / 64 ||
      count > LC_U64_MAX - s->sequence ||
      ps_wire_get64(wire + 8) != s->sequence + 1 ||
      ps_wire_get32(wire + 20) > 1 || ps_wire_get64(wire + 24) ||
      ps_wire_get64(wire + 32) || ps_wire_get64(wire + 40) ||
      ps_wire_get64(wire + 48) || ps_wire_get32(wire + 56) ||
      ps_wire_get32(wire + 60) !=
          (uint32_t)ps_crc32(checksum(wire, 60), wire + 64, (uInt)(bytes - 64)))
    return err(error, LC_ERR_PROTOCOL, "invalid container revision frame");
  entries = lc_calloc_with_allocator(NULL, count, sizeof(*entries));
  if (!entries)
    return err(error, LC_ERR_NOMEM, "cannot allocate revision recovery owners");
  for (i = 0; rc == LC_OK && i < count; ++i) {
    if (bytes - cursor < 64) {
      rc = err(error, LC_ERR_PROTOCOL, "truncated revision member");
      break;
    }
    member_bytes = ps_wire_get32(wire + cursor + 4);
    if (member_bytes < 64 || member_bytes > bytes - cursor ||
        ps_wire_get64(wire + cursor + 8) != s->sequence + 1 + i) {
      rc = err(error, LC_ERR_PROTOCOL, "invalid revision member boundary");
      break;
    }
    rc = decode_entry(s, wire + cursor, member_bytes, 0, &entries[i], &event,
                      error);
    if (rc == LC_OK &&
        (event == PS_REMOVE
             ? entries[i]->key.kind != PS_KEY_METADATA
             : event != PS_PUT || !ps_internal_immutable_key(entries[i]->key)))
      rc = err(error, LC_ERR_PROTOCOL, "invalid revision member kind");
    for (j = 0; rc == LC_OK && j < i; ++j)
      if (same_key(entries[j]->key, entries[i]->key))
        rc = err(error, LC_ERR_PROTOCOL, "duplicate revision member key");
    if (rc == LC_OK) {
      old = lookup(s, entries[i]->key);
      added += old == NULL;
      if (event == PS_REMOVE) {
        if (!revision_retirement_matches(old, entries[i]->token))
          rc =
              err(error, LC_ERR_PROTOCOL, "revision retirement owner mismatch");
        entries[i]->deleted = 1;
      } else if (old && !old->deleted && entries[i]->key.kind == PS_COLLECTION)
        rc = err(error, LC_ERR_PROTOCOL,
                 "revision overwrites collection position");
    }
    if (rc == LC_OK && values && event == PS_PUT)
      rc = verify_entry(s, entries[i], values, error);
    cursor += member_bytes;
  }
  if (rc == LC_OK && cursor != bytes)
    rc = err(error, LC_ERR_PROTOCOL, "revision has trailing members");
  if (rc == LC_OK)
    rc = key_index_reserve(s, added, error);
  if (rc == LC_OK) {
    *out = entries;
    return LC_OK;
  }
  for (i = 0; i < count; ++i)
    if (entries[i])
      ps_internal_entry_free(entries[i]);
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
    install(s, p, old);
  }
  s->sequence += count;
}
static int recover_revision(ps_space *s, const unsigned char *wire,
                            size_t bytes, recovery_values *values, int live,
                            lc_error *error) {
  ps_entry **entries;
  int rc = decode_revision(s, wire, bytes, values, &entries, error);
  if (rc == LC_OK) {
    install_revision(s, entries, ps_wire_get32(wire + 16), live,
                     ps_wire_get32(wire + 20));
    lc_free_with_allocator(NULL, entries);
  }
  return rc;
}

/* Record only affected intervals and packed pages. Keep removed nodes alive
 * until commit so rollback can restore links even across adjacent cuts. */
typedef struct replay_change {
  ps_run **link, *run, *split;
  ps_run **prior_probe;
  ps_run before;
  uint64_t *available, bytes;
  ps_payload_page *page;
  uint64_t occupied;
  int removed, new_page;
} replay_change;
#define REPLAY_INLINE_CHANGES 8
#define REPLAY_BLOCK_CHANGES 32
typedef struct replay_block {
  struct replay_block *previous;
  size_t count;
  replay_change records[REPLAY_BLOCK_CHANGES];
} replay_block;
typedef struct replay_log {
  replay_block *block;
  size_t count;
  replay_change records[REPLAY_INLINE_CHANGES];
} replay_log;
static replay_change *replay_record(replay_log *log, lc_error *error) {
  replay_block *block = log->block;
  replay_change *c;
  if (!block && log->count < REPLAY_INLINE_CHANGES)
    c = &log->records[log->count++];
  else {
    if (!block || block->count == REPLAY_BLOCK_CHANGES) {
      block = lc_alloc_with_allocator(NULL, sizeof(*block));
      if (!block) {
        err(error, LC_ERR_NOMEM, "cannot stage journal payload ownership");
        return NULL;
      }
      block->previous = log->block;
      block->count = 0;
      log->block = block;
    }
    c = &block->records[block->count++];
  }
  memset(c, 0, sizeof(*c));
  return c;
}
static void replay_finish_changes(ps_space *s, replay_change *records,
                                  size_t count, int commit) {
  while (count) {
    replay_change *c = &records[--count];
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
  }
}
static void replay_finish(ps_space *s, replay_log *log, int commit) {
  replay_block *block;
  /* Adjacent interval edits may refer to nodes split by earlier edits.
   * Drain the entire log in reverse order on both rollback and commit. */
  while ((block = log->block) != NULL) {
    replay_finish_changes(s, block->records, block->count, commit);
    log->block = block->previous;
    lc_free_with_allocator(NULL, block);
  }
  replay_finish_changes(s, log->records, log->count, commit);
  log->count = 0;
}
static ps_run **replay_find(ps_run **link, uint64_t offset) {
  for (; *link; link = &(*link)->next)
    if ((*link)->offset <= offset && offset - (*link)->offset < (*link)->bytes)
      return link;
  return NULL;
}
static int replay_range(ps_space *s, uint64_t offset, uint64_t bytes,
                        replay_log *changes, lc_error *error) {
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
static int replay_payload(ps_space *s, ps_entry *p, replay_log *changes,
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
/* Writer completion stays within one physical generation. An admission proof
 * can survive a checkpoint in the same cohort, until the next edit. */
static void stream_admitted(ps_space *s, ps_entry *p, int admitted) {
  if (p->stream.head->admitted != admitted) {
    p->stream.head->admitted = admitted;
    if (!p->dirty_link)
      dirty_entry(s, p, NULL);
  }
}
static ps_writer_stamp *entry_writer_stamp(const ps_entry *p) {
  return p->key.kind == PS_STREAM ? &p->stream.head->writer
                                  : &p->catalog->writer;
}
static int apply_live_hint(ps_space *s, const unsigned char *wire, size_t bytes,
                           int select, lc_error *error) {
  ps_key key;
  ps_entry *head;
  uint64_t value = ps_wire_get64(wire + 48);
  uint32_t event = ps_wire_get32(wire + 20);
  key.kind = ps_wire_get32(wire + 16);
  key.owner = ps_wire_get64(wire + 24);
  key.key = ps_wire_get64(wire + 32);
  if (bytes != 64 || ps_wire_get32(wire + 56) ||
      ps_wire_get32(wire + 60) != checksum(wire, 60))
    return err(error, LC_ERR_PROTOCOL, "invalid live owner hint");
  head = lookup(s, key);
  if (!head || head->deleted || head->token != ps_wire_get64(wire + 40) ||
      (event == PS_NAMESPACE_OWNER
           ? key.kind != PS_CATALOG || key.owner || !head->catalog ||
                 head->catalog->kind != PS_CATALOG_NAMESPACE
           : key.kind != PS_STREAM || head->stream.head->sealed))
    return err(error, LC_ERR_PROTOCOL, "live owner hint does not match head");
  if (event == PS_STREAM_ADMITTED) {
    if (value > head->stream.head->capacity || (select && value != head->bytes))
      return err(error, LC_ERR_PROTOCOL, "admission proof has the wrong tail");
    if (select)
      stream_admitted(s, head, 1);
  } else if (event != PS_STREAM_TAIL && event != PS_STREAM_COMPLETE) {
    if (!value || (select && value > ps_wire_get64(wire + 8)))
      return err(error, LC_ERR_PROTOCOL, "invalid owned stream writer stamp");
    if (select) {
      ps_writer_stamp *stamp = entry_writer_stamp(head);
      stamp->token = value;
      stamp->generation = s->generation;
      if (event == PS_STREAM_EDIT) {
        head->stream.head->edit_wire = ps_wire_get64(wire + 8);
        head->stream.head->complete_generation = 0;
        stream_admitted(s, head, 0);
      }
    }
  } else if (value < head->bytes || value > head->stream.head->capacity)
    return err(error, LC_ERR_PROTOCOL, "owned tail outside granted range");
  else if (select) {
    head->bytes = value;
    head->stream.head->wire = ps_wire_get64(wire + 8);
    head->stream.head->complete_generation =
        event == PS_STREAM_COMPLETE ? s->generation : 0;
    stream_admitted(s, head, event == PS_STREAM_COMPLETE);
    if (!head->dirty_link)
      dirty_entry(s, head, NULL);
  }
  return LC_OK;
}
static int apply_peer_event(ps_space *s, ps_entry *p, int event,
                            lc_error *error) {
  uint64_t wire = s->sequence + 1;
  if (ps_internal_immutable_key(p->key) || p->key.kind == PS_CATALOG)
    p->peer_sequence = wire;
  return apply_journal_event(s, p, event, 0, wire, error);
}
static int apply_peer_frame(ps_space *s, const unsigned char *wire,
                            size_t bytes, uint64_t certified, int staged,
                            lc_error *error) {
  ps_entry **entries = NULL, *single = NULL;
  replay_log changes;
  size_t i, count = 1;
  int group = ps_wire_get32(wire) == REVISION_MAGIC, event = PS_PUT, rc,
      retried = 0;
  if (group)
    count = ps_wire_get32(wire + 16);
  if (certified <= s->sequence || !count || count > certified - s->sequence ||
      ps_wire_get64(wire + 8) != s->sequence + 1)
    return err(error, LC_ERR_PROTOCOL, "invalid certified journal framing");
  if (!group && live_hint(ps_wire_get32(wire + 20))) {
    rc = apply_live_hint(s, wire, bytes, 1, error);
    if (rc == LC_OK)
      ++s->sequence;
    return rc;
  }
  /* Ready frames establish allocator ownership in journal order. Catalog
   * visibility and stream seal/retirement use the durable clock. Piece maps
   * preserve live bytes and can be captured at the ready prefix. Retired
   * extents still belong to the old root or its captured readers. */
  if (group)
    rc = decode_revision(s, wire, bytes, NULL, &entries, error);
  else {
    rc = decode_entry(s, wire, bytes, 0, &single, &event, error);
    if (rc == LC_OK)
      entries = &single;
  }
  if (rc != LC_OK)
    return rc;
  changes.block = NULL;
  changes.count = 0;
replay_extents:
  for (i = 0; rc == LC_OK && i < count; ++i) {
    if (staged) {
      /* The complete staged projection can be discarded on error. */
      rc = payload_claim_entry(s, entries[i], NULL, error);
    } else
      rc = replay_payload(s, entries[i], &changes, error);
  }
  if (staged && s->projection_resident && s->keys.capacity)
    for (i = 0; rc == LC_OK && i < count; ++i)
      rc = projection_copy_predecessor(s, entries[i]->key, error);
  if (!staged && rc == LC_ERR_PROTOCOL && s->retired && !retried) {
    /* Retirement collection can defer a demotion on allocation failure.
     * Undo this frame before collection changes any recorded free links. */
    replay_finish(s, &changes, 0);
    collect_retired(s);
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
    if (rc == LC_OK)
      rc = apply_peer_event(s, single, event, error);
  }
  replay_finish(s, &changes, rc == LC_OK);
  if (rc == LC_OK) {
    if (group)
      install_revision(s, entries, count, 1, ps_wire_get32(wire + 20));
    else
      ++s->sequence;
  } else if (entries)
    for (i = 0; i < count; ++i)
      ps_internal_entry_free(entries[i]);
  if (group)
    lc_free_with_allocator(NULL, entries);
  return rc;
}
static int replay_frame(ps_space *s, const unsigned char *prefix,
                        uint64_t certified, lc_error *error) {
  size_t bytes = ps_wire_get32(prefix + 4);
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
    rc = journal_transfer(s, s->journal_start + s->journal + 64, body + 64,
                          bytes - 64, 0, error);
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
/* The allocator guards the certificate and its immutable published range.
 * Use the reserved space beyond the resident prefix as the import buffer.
 * Only complete frames move the byte and sequence watermarks together. */
static int import_certified_journal(ps_space *s,
                                    const ps_readiness_certificate *certificate,
                                    int staged, lc_error *error) {
  size_t first = s->journal, end = certificate->journal_bytes, bytes;
  unsigned char old_end[64];
  const unsigned char *wire;
  uint64_t records, remaining;
  uint32_t magic;
  int rc;
  if (end < first || end > PS_JOURNAL || s->sequence > certificate->ready ||
      ((first == end) != (s->sequence == certificate->ready)) ||
      (first != end && end - first < sizeof(old_end)))
    return err(error, LC_ERR_PROTOCOL, "inconsistent certified journal bound");
  if (first == end) {
    if (staged)
      memset(s->journal_image + first, 0, journal_end_marker(first));
    return LC_OK;
  }
  if (!staged)
    memcpy(old_end, s->journal_image + first, sizeof(old_end));
  rc = journal_transfer(s, s->journal_start + first, s->journal_image + first,
                        end - first, 0, error);
  while (rc == LC_OK && s->journal < end) {
    wire = s->journal_image + s->journal;
    if (end - s->journal < 64) {
      rc = err(error, LC_ERR_PROTOCOL, "truncated certified journal header");
      break;
    }
    magic = ps_wire_get32(wire);
    bytes = ps_wire_get32(wire + 4);
    records = magic == REVISION_MAGIC ? ps_wire_get32(wire + 16) : 1;
    remaining = certificate->ready - s->sequence;
    if ((magic != EVENT_MAGIC && magic != REVISION_MAGIC) || bytes < 64 ||
        bytes > end - s->journal || !records || records > remaining ||
        ((records == remaining) != (bytes == end - s->journal))) {
      rc = err(error, LC_ERR_PROTOCOL, "invalid certified journal boundary");
      break;
    }
    rc = apply_peer_frame(s, wire, bytes, certificate->ready, staged, error);
    if (rc == LC_OK)
      s->journal += bytes;
  }
  if (rc != LC_OK && !staged && s->journal == first)
    memcpy(s->journal_image + first, old_end, sizeof(old_end));
  else
    memset(s->journal_image + s->journal, 0, journal_end_marker(s->journal));
  return rc;
}
static int validate_owned_stream(const ps_stream_state *head, lc_error *error) {
  const ps_entry *run;
  uint64_t count = 0, end = 0, stored = 0;
  for (run = head->owned_first; run; run = run->stream.run->owned_next) {
    if (run->deleted || run->key.key < end || run->key.key > head->capacity ||
        run->stream.run->capacity > head->capacity - run->key.key)
      return err(error, LC_ERR_PROTOCOL, "incomplete owned run directory");
    end = run->key.key + run->stream.run->capacity;
    stored += entry_storage(run);
    ++count;
  }
  return count == head->runs && stored == head->storage
             ? LC_OK
             : err(error, LC_ERR_PROTOCOL, "owned stream capacity mismatch");
}
static int validate_owned_directory(ps_space *s, lc_error *error) {
  const ps_entry *p;
  size_t i;
  int rc = LC_OK;
  for (i = 0; rc == LC_OK && i < BUCKETS; ++i)
    for (p = s->buckets->streams[i]; rc == LC_OK && p; p = p->stream.head->next)
      rc = validate_owned_stream(p->stream.head, error);
  return rc;
}
static int recover_directory(ps_space *s, unsigned char *wire,
                             const ps_readiness_certificate *certificate,
                             ps_space *resident, int reuse,
                             payload_rebuild *payload, lc_error *error) {
  ps_directory *directory = NULL;
  metadata_recovery *recovery = NULL;
  int rc = initial_free(s, error);
  payload->initial = 1;
  if (rc == LC_OK && s->directory.offset)
    rc = directory_open(s, s->directory, &directory, error);
  if (rc == LC_OK && directory) {
    recovery = lc_calloc_with_allocator(NULL, 1, sizeof(*recovery));
    if (!recovery)
      rc = err(error, LC_ERR_NOMEM, "cannot allocate directory recovery batch");
  }
  if (rc == LC_OK && recovery) {
    recovery->space = s;
    /* A live batch groups entries already bound to the same resident
     * incarnation. Only independent allocations can become new residents. */
    recovery->resident = resident;
    recovery->payload = payload;
    recovery->wire = wire;
    recovery->certified_root = certificate->root == s->generation;
    recovery->cohort_origin = certificate->origin;
    recovery->reuse =
        resident && reuse && recovery->certified_root && !certificate->pending;
    rc = ps_directory_visit(directory, capture_directory_entry,
                            recover_directory_page, recovery, error);
  }
  ps_directory_close(directory);
  if (rc == LC_OK && recovery)
    rc = recover_directory_bindings(recovery, error);
  if (recovery) {
    metadata_recovery_close(recovery);
  }
  if (rc == LC_OK)
    rc = validate_owned_directory(s, error);
  if (rc == LC_OK)
    rc = ps_catalog_validate_directory(s, error);
  return rc;
}
static int
recover_checkpoint_directory(ps_space *s, unsigned char *wire,
                             const ps_readiness_certificate *certificate,
                             lc_error *error) {
  payload_rebuild build = {0};
  int rc = recover_directory(s, wire, certificate, NULL, 0, &build, error);
  if (rc == LC_OK)
    rc = payload_rebuild_free(s, &build, error);
  lc_free_with_allocator(NULL, build.ranges);
  return rc;
}
static int install_owned_grant(ps_space *s, ps_entry *p, lc_error *error) {
  ps_entry *head = stream_lookup(s, p->key.owner);
  if (!head || head->stream.head->sealed ||
      p->key.key != head->stream.head->capacity || p->stream.run->mapped ||
      p->stream.run->capacity >
          (uint64_t)LC_I64_MAX - head->stream.head->capacity)
    return err(error, LC_ERR_PROTOCOL, "owned run grant is not contiguous");
  head->stream.head->capacity += p->stream.run->capacity;
  ++head->stream.head->runs;
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
  for (run = head->stream.head->owned_first; run; run = next) {
    next = run->stream.run->owned_next;
    if (run->deleted || run->key.key < end ||
        run->key.key > head->stream.head->capacity ||
        run->stream.run->capacity > head->stream.head->capacity - run->key.key)
      return err(error, LC_ERR_PROTOCOL, "retired stream grant mismatch");
    end = run->key.key + run->stream.run->capacity;
    ++count;
    if (apply)
      mark_removed(s, run, sequence);
  }
  if (count != head->stream.head->runs)
    return err(error, LC_ERR_PROTOCOL, "retired stream capacity mismatch");
  if (apply)
    mark_removed(s, head, sequence);
  return LC_OK;
}
/* Common transition checks for cold recovery and certified live replay.
 * Success consumes the decoded entry. Errors leave it with the caller. */
static int apply_journal_event(ps_space *s, ps_entry *p, int event,
                               uint64_t release, uint64_t wire,
                               lc_error *error) {
  ps_entry *old, *head;
  int rc;
  old = lookup(s, p->key);
  if (event == PS_PUT) {
    if (!old) {
      rc = key_index_reserve(s, 1, error);
      if (rc != LC_OK)
        return rc;
    }
    if (p->key.kind == PS_OWNED_RUN) {
      if (p->stream.run->mapped) {
        head = stream_lookup(s, p->key.owner);
        if (!old || old->deleted || !head ||
            p->stream.run->capacity != old->stream.run->capacity)
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
      if ((!ps_internal_immutable_key(p->key) ||
           p->key.kind == PS_COLLECTION) &&
          old && !old->deleted)
        return err(error, LC_ERR_PROTOCOL,
                   "journal overwrites typed ownership");
      if (p->key.kind == PS_STREAM &&
          (p->bytes || p->stream.head->capacity || p->stream.head->runs ||
           p->stream.head->sealed))
        return err(error, LC_ERR_PROTOCOL, "nonempty initial stream head");
    }
  } else {
    if (!old || old->deleted || old->token != p->token)
      return err(error, LC_ERR_PROTOCOL, "journal retirement owner mismatch");
    if (event == PS_SEAL) {
      if (old->stream.head->sealed || p->bytes < old->bytes ||
          p->stream.head->capacity != old->stream.head->capacity ||
          p->stream.head->runs != old->stream.head->runs)
        return err(error, LC_ERR_PROTOCOL, "journal stream seal mismatch");
      old->bytes = p->bytes;
      old->stream.head->sealed = 1;
      old->stream.head->structure_wire = wire;
      if (!old->dirty_link)
        dirty_entry(s, old, NULL);
      ps_internal_entry_free(p);
      return LC_OK;
    }
    if (p->key.kind == PS_CATALOG) {
      ps_catalog_drop_record(s, old, release, wire);
      if (!wire && !s->readiness)
        /* The selected recovery view already includes this removal. The
         * target's release fence still carries the real journal sequence. */
        old->publication_sequence = 0;
      ps_internal_entry_free(p);
      return LC_OK;
    }
    if (p->key.kind == PS_STREAM) {
      if (old->stream.head->catalog_refs)
        return err(error, LC_ERR_PROTOCOL, "journal retires a catalog target");
      rc = remove_owned(s, old, release, 0, error);
      if (rc == LC_OK)
        rc = remove_owned(s, old, release, 1, error);
      if (rc == LC_OK) {
        old->stream.head->structure_wire = wire;
        ps_internal_entry_free(p);
      }
      return rc;
    }
    if (p->key.kind == PS_OWNED_RUN) {
      head = stream_lookup(s, p->key.owner);
      if (!head || !head->stream.head->runs)
        return err(error, LC_ERR_PROTOCOL, "piece removal has no head");
      --head->stream.head->runs;
      if (!head->dirty_link)
        dirty_entry(s, head, NULL);
    }
    p->deleted = 1;
  }
  register_entry(s, p);
  dirty_entry(s, p, old);
  install(s, p, old);
  return LC_OK;
}
#define CLOSE_PROOF_BYTES (2U * PS_READINESS_SLOT_BYTES)
/* The gap before the tail bounds leaves the second shared commit word zero. */
#define CLOSE_PROOF_HEADER PS_READINESS_SLOT_BYTES
#define CLOSE_PROOF_TAIL 16U
#define CLOSE_PROOF_MAX_TAILS                                                  \
  ((CLOSE_PROOF_BYTES - PS_CLOSE_PROOF_OFFSET - CLOSE_PROOF_HEADER) /          \
   CLOSE_PROOF_TAIL)
static uint32_t close_proof_crc(const unsigned char *record, size_t count) {
  return (uint32_t)ps_crc32(checksum(record, 92), record + CLOSE_PROOF_HEADER,
                            (uInt)(count * CLOSE_PROOF_TAIL));
}
static int close_proof_hash(const unsigned char *wire, size_t bytes,
                            unsigned char *hash) {
  unsigned int size = 0;
  return EVP_Digest(wire, bytes, hash, &size, EVP_sha256(), NULL) == 1 &&
         size == 32;
}
static int exclusive_raw_covered(const ps_space *s) {
  return __atomic_load_n(&s->exclusive_raw_durable, __ATOMIC_ACQUIRE) ==
         __atomic_load_n(&s->exclusive_raw_writes, __ATOMIC_ACQUIRE);
}
static int close_proof_matches(const ps_space *s, const unsigned char *proof) {
  return !memcmp(proof + 8, s->identity, 16) &&
         ps_wire_get64(proof + 24) == s->generation &&
         ps_wire_get64(proof + 32) == s->sequence &&
         ps_wire_get64(proof + 40) == s->token_limit &&
         ps_wire_get64(proof + 48) == s->journal;
}
/* Exclusive lifetime authority makes the wire prefix and stream ends stable
 * at close. The optional proof follows their completed sync. It needs no
 * barrier of its own and can be lost without losing any acknowledged data. */
static void exclusive_record_close(ps_space *s) {
  unsigned char page[CLOSE_PROOF_BYTES] = {0}, *wire = page;
  const ps_entry *stream;
  unsigned char *proof = page + PS_CLOSE_PROOF_OFFSET;
  unsigned char *tail = proof + CLOSE_PROOF_HEADER;
  size_t i, count = 0;
  int rc, covered;
  if (s->readiness || healthy(s, NULL) != LC_OK)
    return;
  covered = exclusive_raw_covered(s) &&
            __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE) >=
                __atomic_load_n(&s->pending_sequence, __ATOMIC_ACQUIRE);
  if (!covered) {
    /* Reinstating proven tails can advance the local clock. If the journal
     * and raw stream bytes stayed put, the original receipt still applies. */
    if (ps_wire_get64(s->close_proof) == PS_CLOSE_PROOF_MAGIC &&
        close_proof_matches(s, s->close_proof) &&
        !__atomic_load_n(&s->exclusive_raw_writes, __ATOMIC_ACQUIRE)) {
      count = ps_wire_get32(s->close_proof + 88);
      memcpy(proof, s->close_proof,
             CLOSE_PROOF_HEADER + count * CLOSE_PROOF_TAIL);
      (void)file_transfer(s, page,
                          (size_t)(tail - page) + count * CLOSE_PROOF_TAIL,
                          PS_READINESS_OFFSET, 1, NULL, NULL);
      return;
    }
    if (s->durable)
      return;
  }
  for (i = 0; covered && i < BUCKETS; ++i)
    for (stream = s->buckets->streams[i]; stream;
         stream = stream->stream.head->next) {
      if (!stream->bytes || stream->stream.head->sealed)
        continue;
      if (count == CLOSE_PROOF_MAX_TAILS)
        return; /* Keep one physical barrier for a larger stream set. */
      ps_wire_put64(tail, stream->token);
      ps_wire_put64(tail + 8, stream->bytes);
      tail += CLOSE_PROOF_TAIL;
      ++count;
    }
  if (s->journal) {
    wire = lc_alloc_with_allocator(NULL, s->journal);
    if (!wire)
      return;
  }
  rc = journal_transfer(s, s->journal_start, wire, s->journal, 0, NULL);
  if (rc == LC_OK && !close_proof_hash(wire, s->journal, proof + 56))
    rc = LC_ERR_INVALID;
  if (wire != page)
    lc_free_with_allocator(NULL, wire);
  if (rc != LC_OK)
    return;
  ps_wire_put64(proof, covered ? PS_CLOSE_PROOF_MAGIC : PS_CLOSE_NOSYNC_MAGIC);
  memcpy(proof + 8, s->identity, 16);
  ps_wire_put64(proof + 24, s->generation);
  ps_wire_put64(proof + 32, s->sequence);
  ps_wire_put64(proof + 40, s->token_limit);
  ps_wire_put64(proof + 48, s->journal);
  ps_wire_put32(proof + 88, (uint32_t)count);
  ps_wire_put32(proof + 92, close_proof_crc(proof, count));
  (void)file_transfer(s, page, (size_t)(tail - page), PS_READINESS_OFFSET, 1,
                      NULL, NULL);
}
enum exclusive_proof { EXCLUSIVE_UNPROVED, EXCLUSIVE_DURABLE, EXCLUSIVE_CLEAN };
static int exclusive_open_proof(ps_space *s, const unsigned char *wire,
                                int clean, enum exclusive_proof *accepted,
                                lc_error *error) {
  unsigned char page[CLOSE_PROOF_BYTES], hash[32];
  const unsigned char *proof = page + PS_CLOSE_PROOF_OFFSET;
  size_t count, bytes;
  uint64_t magic;
  int rc = transfer(s, page, sizeof(page), PS_READINESS_OFFSET, 0, error);
  if (rc != LC_OK)
    return rc;
  count = ps_wire_get32(proof + 88);
  magic = ps_wire_get64(proof);
  if (clean && !ps_wire_get64(page) &&
      !ps_wire_get64(page + PS_READINESS_SLOT_BYTES) &&
      (magic == PS_CLOSE_PROOF_MAGIC ||
       (magic == PS_CLOSE_NOSYNC_MAGIC && !s->durable && !count)) &&
      close_proof_matches(s, proof) && count <= CLOSE_PROOF_MAX_TAILS &&
      ps_wire_get32(proof + 92) == close_proof_crc(proof, count) &&
      close_proof_hash(wire, s->journal, hash) &&
      !memcmp(proof + 56, hash, sizeof(hash))) {
    bytes = CLOSE_PROOF_HEADER + count * CLOSE_PROOF_TAIL;
    memcpy(s->close_proof, proof, bytes);
    *accepted =
        magic == PS_CLOSE_PROOF_MAGIC ? EXCLUSIVE_DURABLE : EXCLUSIVE_CLEAN;
    /* This opening owns a private copy. Revoke the on-disk receipt before
     * any producer can touch bytes in a previously covered grant. */
    memset(page, 0, 8);
    return transfer(s, page, 8, PS_READINESS_OFFSET + PS_CLOSE_PROOF_OFFSET, 1,
                    error);
  }
  /* A failed cleanup may leave zeros in the page cache. Revoke the old
   * proof before making those changes so a retry still takes the barrier.
   * An exclusive transition also revokes certificates from a shared cohort. */
  if (ps_wire_get64(proof) || ps_wire_get64(page) ||
      ps_wire_get64(page + PS_READINESS_SLOT_BYTES)) {
    bytes = PS_READINESS_SLOT_BYTES + 8;
    memset(page, 0, bytes);
    return transfer(s, page, bytes, PS_READINESS_OFFSET, 1, error);
  }
  return LC_OK;
}

#define RECOVERY_HEADERS                                                       \
  (PS_PAGE + PS_TOKEN_RECORD_OFFSET + PS_TOKEN_RECORD_BYTES)

static int recover_live_directory(ps_space *s, ps_coordination_guard **guard,
                                  const unsigned char *headers,
                                  unsigned char *wire,
                                  ps_readiness_certificate *certificate,
                                  int *moved, lc_error *error) {
  ps_readiness_certificate current;
  lc_error draft_error;
  ps_space *draft = projection_new(s, error);
  int rc, prepared = LC_OK;
  *moved = 0;
  if (!draft)
    return LC_ERR_NOMEM;
  projection_select(draft, headers + s->root * PS_PAGE, s->root);
  lc_error_init(&draft_error);
  /* The draft owns only memory. A checkpoint may retire any of the pages
   * being read, so even a draft error is provisional until revalidation. */
  rc = allocation_end(s, *guard, LC_OK, error);
  *guard = NULL;
  if (rc == LC_OK) {
    prepared =
        recover_checkpoint_directory(draft, wire, certificate, &draft_error);
    rc = allocation_begin(s, guard, error);
  }
  if (rc == LC_OK)
    rc = ps_readiness_read(s, &current, error);
  if (rc == LC_OK)
    rc = transfer(s, wire, RECOVERY_HEADERS, 0, 0, error);
  if (rc == LC_OK) {
    *moved = current.pending || current.root != certificate->root ||
             current.origin != certificate->origin ||
             current.grant_limit != certificate->grant_limit ||
             memcmp(wire, headers, RECOVERY_HEADERS);
    if (!*moved) {
      rc = prepared;
      if (rc == LC_OK) {
        s->entries = draft->entries;
        draft->entries = NULL;
        if (s->entries)
          s->entries->all_link = &s->entries;
        projection_install(s, draft);
        *certificate = current;
      } else if (error) {
        lc_error_cleanup(error);
        *error = draft_error;
        lc_error_init(&draft_error);
      }
    }
  }
  lc_error_cleanup(&draft_error);
  projection_destroy(draft);
  return rc;
}
static int recover(ps_space *s, ps_coordination_guard **guard,
                   lc_error *error) {
  unsigned char zero[PS_ROOT_BYTES] = {0};
  unsigned char superblocks[RECOVERY_HEADERS], *wire = NULL;
  recovery_values *values = NULL;
  ps_readiness_certificate certificate, prior;
  const unsigned char *roots[2], *root;
  ps_entry *p = NULL, *checkpoint_entries = NULL;
  struct stat st;
  uint64_t grant_limit, effect;
  size_t cursor = 0, bytes, first, last, journal_bound = PS_JOURNAL, marker;
  int a, b, event, rc, retire_sibling, retirement_needed, known_grant;
  int needs_prefix_sync = 0, grant_slot, moved = 0, certified, cold_covered;
  int live_prefix = 0;
  enum exclusive_proof exclusive = EXCLUSIVE_UNPROVED;
  roots[0] = superblocks;
  roots[1] = superblocks + PS_PAGE;
restart:
  rc = transfer(s, superblocks, sizeof(superblocks), 0, 0, error);
  if (rc != LC_OK)
    goto done;
  a = root_valid(roots[0]);
  b = root_valid(roots[1]);
  if (!a && !b) {
    rc = err(error, LC_ERR_PROTOCOL, "no recoverable container root");
    goto done;
  }
  s->root =
      b && (!a || ps_wire_get64(roots[1] + 8) > ps_wire_get64(roots[0] + 8));
  root = roots[s->root];
  if (a && b && !root_follows(root, roots[1 - s->root])) {
    rc = err(error, LC_ERR_PROTOCOL, "conflicting container roots");
    goto done;
  }
  projection_select(s, root, s->root);
  rc = decode_token_limit(s, roots[0] + PS_TOKEN_RECORD_OFFSET,
                          roots[1] + PS_TOKEN_RECORD_OFFSET, &grant_limit,
                          &grant_slot, error);
  if (rc == LC_OK)
    rc = ps_readiness_read(s, &certificate, error);
  if (rc == LC_OK)
    rc = checkpoint_retirement(&certificate, s->generation, s->sequence, error);
  if (rc != LC_OK)
    goto done;
  if (s->maximum && s->capacity > s->maximum) {
    rc = err(error, LC_ERR_INVALID,
             "committed capacity exceeds configured maximum");
    goto done;
  }
  if (fstat(s->fd, &st) != 0 || st.st_size < (off_t)s->capacity) {
    rc = err(error, LC_ERR_PROTOCOL,
             "container shorter than committed capacity");
    goto done;
  }
  if (!wire) {
    wire = lc_alloc_with_allocator(NULL, PS_JOURNAL);
    values = lc_alloc_with_allocator(NULL, sizeof(*values));
    if (!wire || !values) {
      rc = err(error, LC_ERR_NOMEM, "cannot allocate recovery buffers");
      goto done;
    }
  }
  values->packed.offset = 0;
  retire_sibling = memcmp(roots[1 - s->root], zero, sizeof(zero)) != 0;
  certified = *guard && !retire_sibling && !certificate.pending &&
              certificate.root == s->generation &&
              certificate.grant_limit == grant_limit;
  if (!moved && certified && s->directory.offset) {
    rc = recover_live_directory(s, guard, superblocks, wire, &certificate,
                                &moved, error);
    if (rc == LC_OK && moved)
      goto restart;
  } else
    rc = recover_checkpoint_directory(s, wire, &certificate, error);
  if (rc != LC_OK)
    goto done;
  checkpoint_entries = s->entries;
  s->token_limit = grant_limit;
  bytes = PS_JOURNAL;
  if (certified) {
    marker = journal_end_marker(certificate.journal_bytes);
    bytes = certificate.journal_bytes + marker;
    /* Publishers mark a pending transition before touching the journal and
     * clear it only after certifying the complete frame and its end marker.
     * The revalidated certificate is stable under allocation authority. */
    rc = journal_transfer(s, s->journal_start, wire, bytes, 0, error);
    if (rc != LC_OK)
      goto done;
    if (!memcmp(wire + certificate.journal_bytes, zero, marker)) {
      journal_bound = certificate.journal_bytes;
      /* A live NoSync owner can adopt the completed wire prefix with its
       * existing durability watermark. Use the peer visibility clock for
       * durable-only values and retain their predecessors until selection. */
      live_prefix = !s->durable && ps_readiness_joined(s) &&
                    certificate.ready > certificate.durable;
    }
  }
  if (journal_bound == PS_JOURNAL && (!certified || bytes < PS_JOURNAL)) {
    first = certified ? bytes : 0;
    rc = journal_transfer(s, s->journal_start + first, wire + first,
                          PS_JOURNAL - first, 0, error);
  }
  if (rc != LC_OK)
    goto done;
  cursor = 0;
  effect = s->sequence;
  while (!(certificate.pending & PS_READINESS_CHECKPOINT_RETIRE) &&
         journal_bound - cursor >= 64 && s->sequence != LC_U64_MAX &&
         (ps_wire_get32(wire + cursor) == EVENT_MAGIC ||
          ps_wire_get32(wire + cursor) == REVISION_MAGIC) &&
         ps_wire_get64(wire + cursor + 8) == s->sequence + 1) {
    bytes = ps_wire_get32(wire + cursor + 4);
    if (bytes < 64 || bytes > journal_bound - cursor)
      break;
    event = ps_wire_get32(wire + cursor + 20);
    if (s->sequence >= certificate.durable) {
      if (live_prefix) {
        uint32_t kind = ps_wire_get32(wire + cursor + 16);
        /* Root open can need these catalog and stream bindings to recover
         * namespaces. Finish their structural barrier before admission. */
        if (ps_wire_get32(wire + cursor) == EVENT_MAGIC &&
            !live_hint((uint32_t)event) &&
            (kind == PS_CATALOG || kind == PS_STREAM || kind == PS_OWNED_RUN))
          needs_prefix_sync = 1;
      } else if (ps_wire_get32(wire + cursor) == REVISION_MAGIC ||
                 !live_hint((uint32_t)event) ||
                 (s->sequence >= certificate.origin &&
                  event != PS_STREAM_OWNER && event != PS_NAMESPACE_OWNER &&
                  event != PS_STREAM_ADMITTED))
        needs_prefix_sync = 1;
    }
    if (ps_wire_get32(wire + cursor) == REVISION_MAGIC) {
      rc =
          recover_revision(s, wire + cursor, bytes, values, live_prefix, error);
      if (rc == LC_ERR_NOMEM || __atomic_load_n(&s->poisoned, __ATOMIC_RELAXED))
        goto done;
      if (rc != LC_OK) {
        lc_error_cleanup(error);
        break;
      }
      effect = s->sequence;
      cursor += bytes;
      continue;
    }
    if (live_hint(ps_wire_get32(wire + cursor + 20))) {
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
    rc = decode_entry(s, wire + cursor, bytes, 0, &p, &event, error);
    if (rc == LC_ERR_NOMEM)
      goto done;
    if (rc != LC_OK) {
      lc_error_cleanup(error);
      break;
    }
    if (event == PS_PUT) {
      rc = verify_entry(s, p, values, error);
      if (rc != LC_OK) {
        ps_internal_entry_free(p);
        if (__atomic_load_n(&s->poisoned, __ATOMIC_RELAXED))
          goto done;
        lc_error_cleanup(error);
        break;
      }
    }
    rc = live_prefix
             ? apply_peer_event(s, p, event, error)
             : apply_journal_event(s, p, event, s->sequence + 1, 0, error);
    if (rc != LC_OK) {
      ps_internal_entry_free(p);
      goto done;
    }
    cursor += bytes;
    ++s->sequence;
    effect = s->sequence;
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
  last = journal_bound;
  while (last - first >= sizeof(zero) &&
         !memcmp(wire + first, zero, sizeof(zero)))
    first += sizeof(zero);
  while (first < last && !wire[first])
    ++first;
  while (last - first >= sizeof(zero) &&
         !memcmp(wire + last - sizeof(zero), zero, sizeof(zero)))
    last -= sizeof(zero);
  while (last > first && !wire[last - 1])
    --last;
  retirement_needed = first < last || retire_sibling;
  if (!s->readiness) {
    rc = exclusive_open_proof(s, wire, !retirement_needed, &exclusive, error);
    if (rc != LC_OK)
      goto done;
  }
  /* A process can die with the new selector readable but unsynced. Fence
   * it before erasing bytes needed by a still-valid predecessor. */
  if (a && b && certificate.root < s->generation &&
      !(certificate.pending & PS_READINESS_CHECKPOINT_RETIRE)) {
    rc = sync_file(s, error);
    if (rc != LC_OK)
      goto done;
  }
  if (retirement_needed)
    ps_readiness_begin(s, PS_READINESS_RECOVERY);
  if (first < last) {
    memset(wire + first, 0, last - first);
    ps_readiness_begin(s, PS_READINESS_JOURNAL);
    rc = journal_transfer(s, s->journal_start + first, wire + first,
                          last - first, 1, error);
    if (rc != LC_OK)
      goto done;
  }
  if (retire_sibling) {
    ps_readiness_begin(s, PS_READINESS_ROOT);
    rc = transfer(s, zero, sizeof(zero), (uint64_t)(1 - s->root) * PS_PAGE, 1,
                  error);
    if (rc != LC_OK)
      goto done;
  }
  /* Keep the physical ownership of every accepted journal PUT until a
   * checkpoint retires its frame. */
  rc = recover_payload_ownership(s, checkpoint_entries, live_prefix, error);
  if (rc == LC_OK)
    rc = pins_recover(s, s, error);
  if (rc != LC_OK)
    goto done;
  /* Open built its free map from scratch and queried the physical witnesses
   * under the same allocator hold that admits the protected metadata pool. */
  s->allocation_ranges_checked = s->allocation_guard != NULL;
  /* Check the complete cold replay before relying on the preceding barrier.
   * Hints past that barrier are ignored in the next cohort. The previous
   * cohort's allocator ranges and readiness hints do not carry forward. */
  known_grant = ps_readiness_prior_durable(s, &prior) &&
                prior.grant_limit == s->token_limit;
  cold_covered = exclusive == EXCLUSIVE_DURABLE ||
                 (!retirement_needed && known_grant &&
                  prior.root == s->generation && prior.ready == s->sequence &&
                  prior.journal_bytes == s->journal && prior.durable >= effect);
  s->peer_durable = certificate.durable;
  if (ps_readiness_joined(s) && certificate.grant_limit != grant_limit) {
    if (certificate.grant_used != certificate.grant_limit ||
        grant_limit - certificate.grant_limit > TOKEN_POOL) {
      rc = err(error, LC_ERR_PROTOCOL, "unassigned incarnation pool invalid");
      goto done;
    }
    s->token_next = certificate.grant_limit + 1;
    s->token_end = certificate.grant_limit + TOKEN_RANGE;
  } else if (!ps_readiness_joined(s) && !cold_covered &&
             exclusive != EXCLUSIVE_CLEAN &&
             (known_grant || ps_wire_get64(root + 48) == grant_limit)) {
    /* Only replace the preceding slot when this frontier crossed a known
     * barrier. A cached refill may otherwise be the only readable slot. */
    rc = grant_tokens(s, 1 - grant_slot, grant_limit, error);
    if (rc != LC_OK)
      goto done;
  }
  if (cold_covered) {
    /* Ignoring old hints is safe, but does not make their bytes durable.
     * A later crash may reuse their wire positions for canonical data. */
    s->peer_durable =
        exclusive == EXCLUSIVE_DURABLE ? s->sequence : prior.durable;
    s->reused_open_barrier = 1;
  } else if (exclusive == EXCLUSIVE_CLEAN) {
    /* Publish the checked NoSync view for reads. The first mutation or
     * explicit barrier must stabilize it before changing persistence.
     * A checkpointed grant slot can fund that barrier without another sync. */
    s->open_grant_covered = ps_wire_get64(root + 48) == grant_limit;
    __atomic_store_n(&s->deferred_recovery, 1, __ATOMIC_RELEASE);
  } else if (retirement_needed || certificate.pending ||
             certificate.root != s->generation ||
             certificate.ready != s->sequence || needs_prefix_sync ||
             certificate.grant_limit != s->token_limit) {
    rc = sync_file(s, error);
    if (rc != LC_OK)
      goto done;
    s->peer_durable = s->sequence;
    ps_readiness_finish(s,
                        PS_READINESS_RECOVERY | PS_READINESS_CHECKPOINT_RETIRE);
  }
  __atomic_store_n(&s->pending_sequence, s->sequence, __ATOMIC_RELEASE);
  if (ps_coordination_shared(s->coordination)) {
    s->journal_image = wire;
    wire = NULL;
  }
done:
  lc_free_with_allocator(NULL, values);
  lc_free_with_allocator(NULL, wire);
  return rc;
}
/* A warm projection owns only resident metadata until installation. Existing
 * private witnesses stay on s; newly staged workspace is tagged 2 so every
 * fallible preparation path can relinquish precisely its own additions. */
static int projection_same(const ps_entry *a, const ps_entry *b) {
  size_t i;
  if (a->deleted || a->count != b->count ||
      a->deletion_bytes != b->deletion_bytes ||
      (a->key.kind != PS_STREAM && a->bytes != b->bytes) ||
      (a->key.kind == PS_OWNED_RUN &&
       (a->stream.run->mapped != b->stream.run->mapped ||
        a->stream.run->capacity != b->stream.run->capacity)))
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
         (a->stream.head->capacity <= b->stream.head->capacity &&
          (!a->stream.head->sealed || b->stream.head->sealed) &&
          a->bytes <= b->stream.head->capacity &&
          (!b->stream.head->sealed || a->bytes <= b->bytes));
}
static int projection_journal(ps_space *v, uint64_t checkpoint,
                              uint64_t certified, lc_error *error) {
  static const unsigned char zero[64] = {0};
  const unsigned char *p;
  size_t bytes, loaded = 0;
  uint32_t magic;
  int rc;
  if (checkpoint == certified) {
    loaded = sizeof(zero);
    rc = journal_transfer(v, v->journal_start, v->journal_image, loaded, 0,
                          error);
    if (rc != LC_OK || !memcmp(v->journal_image, zero, loaded))
      return rc;
  }
  rc = journal_transfer(v, v->journal_start + loaded, v->journal_image + loaded,
                        PS_JOURNAL - loaded, 0, error);
  if (rc != LC_OK)
    return rc;
  while (v->journal <= PS_JOURNAL - 64) {
    p = v->journal_image + v->journal;
    magic = ps_wire_get32(p);
    bytes = ps_wire_get32(p + 4);
    if (!memcmp(p, zero, sizeof(zero)))
      return LC_OK;
    if ((magic == (uint32_t)EVENT_MAGIC || magic == REVISION_MAGIC) &&
        bytes >= 64 && bytes <= PS_JOURNAL - v->journal) {
      /* An untouched journal may still start with a checked frame from the
       * previous generation. Once replay starts, require its end marker. */
      if (!v->journal && ps_wire_get64(p + 8) <= checkpoint &&
          ps_wire_get32(p + 60) ==
              (uint32_t)ps_crc32(checksum(p, 60), p + 64, (uInt)(bytes - 64)))
        return LC_OK;
      if (ps_wire_get64(p + 8) > checkpoint &&
          ps_wire_get64(p + 8) <= certified) {
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
static int projection_pool(ps_space *v, const ps_run *source, lc_error *error) {
  ps_run *copy;
  int rc;
  for (; source; source = source->next) {
    if (source->claimed != 1)
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
    free_insert(v, copy);
  }
  return LC_OK;
}
static ps_entry *projected(ps_entry *p) {
  return p && p->projection ? p->projection : p;
}
static int entry_catalog_pinned(const ps_entry *p) {
  return p->key.kind == PS_STREAM && p->stream.head->catalog_pins;
}
static void projection_entries(ps_space *s, ps_space *v) {
  ps_entry *p, *next, *q, *keep = NULL, *consumed = NULL, *borrowed = NULL,
                          *dest;
  ps_catalog_record *record;
  size_t group, i;
  /* Preparation is finished. Nothing below allocates or performs file I/O. */
  for (i = 0; i < v->keys.capacity; ++i) {
    p = v->keys.slots[i].entry;
    if (projection_borrowed(v, p))
      p->projection = p;
    else
      v->keys.slots[i].entry = projected(p);
  }
  for (q = v->entries; q; q = q->all_next)
    if (q->projection)
      q->projection->projection = q;
  for (p = s->entries; p; p = next) {
    next = p->all_next;
    if (p->projection) {
      if (p->projection == p) {
        /* Rebase the publication onto the selected checkpoint. Payload and
         * reader ownership stay intact while local selection is relinked. */
        p->publication_sequence = p->peer_sequence = 0;
        p->durable_visibility = p->safe = 0;
        p->previous = NULL;
        p->retired_next = p->dirty_next = NULL;
        p->dirty_link = NULL;
        memset(&p->checkpoint_ref, 0, sizeof(p->checkpoint_ref));
        p->all_next = borrowed;
        borrowed = p;
      }
      p->projection = NULL;
      continue;
    }
    if (p->state != 0 && !p->pins && !entry_catalog_pinned(p)) {
      ps_internal_entry_free(
          p); /* Drop stale resident metadata, never free its ranges. */
      continue;
    }
    p->previous = NULL;
    p->value_next = NULL;
    p->value_link = NULL;
    p->retired_next = p->dirty_next = NULL;
    p->dirty_link = NULL;
    p->checkpoint_ref.offset = p->checkpoint_ref.bytes = 0;
    if (p->key.kind == PS_STREAM) {
      ps_stream_state *head = p->stream.head;
      head->next = NULL;
      head->owned_first = head->owned_last = NULL;
    } else if (p->key.kind == PS_OWNED_RUN) {
      ps_owned_run_state *run = p->stream.run;
      run->owned_prev = run->owned_next = NULL;
      run->owned_linked = 0;
    }
    if (p->catalog)
      p->catalog->link = NULL;
    if (p->state != 0) {
      lc_free_with_allocator(NULL, p->deletion_reserve);
      p->deletion_reserve = NULL;
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
  for (q = v->entries; q; q = next) {
    next = q->all_next;
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
      /* The staged tree owns independent nodes, even when its selected
       * descriptor reuses an existing reader-visible incarnation. */
      lc_free_with_allocator(NULL, dest->deletion_reserve);
      dest->deletion_reserve = q->deletion_reserve;
      q->deletion_reserve = NULL;
      if (dest->deletion_reserve)
        ps_internal_entry_auxiliary(dest);
      if (dest->key.kind == PS_OWNED_RUN)
        dest->stream.run->owned_linked = q->stream.run->owned_linked;
      if (dest->key.kind == PS_STREAM) {
        ps_stream_state *to = dest->stream.head, *from = q->stream.head;
        to->writer = from->writer;
        to->catalog_refs = from->catalog_refs;
        to->catalog_release = 0;
        to->catalog_release_wire = from->catalog_release_wire;
        to->capacity = from->capacity;
        to->runs = from->runs;
        to->storage = from->storage;
        to->sealed = from->sealed;
        to->admitted = from->admitted;
        to->wire = from->wire;
        to->edit_wire = from->edit_wire;
        to->complete_generation = from->complete_generation;
        to->structure_wire = from->structure_wire;
        dest->bytes = q->bytes;
      }
    }
    dest->all_next = projected(next);
    dest->value_next = projected(q->value_next);
    dest->value_link = NULL;
    dest->previous = projected(q->previous);
    dest->retired_next = projected(q->retired_next);
    dest->dirty_next = projected(q->dirty_next);
    dest->dirty_link = NULL;
    if (dest->key.kind == PS_STREAM) {
      ps_stream_state *to = dest->stream.head, *from = q->stream.head;
      to->next = projected(from->next);
      to->owned_first = projected(from->owned_first);
      to->owned_last = projected(from->owned_last);
    } else if (dest->key.kind == PS_OWNED_RUN) {
      ps_owned_run_state *to = dest->stream.run, *from = q->stream.run;
      to->owned_prev = projected(from->owned_prev);
      to->owned_next = projected(from->owned_next);
    }
    if ((record = dest->catalog) != NULL) {
      record->writer = q->catalog->writer;
      record->target = projected(q->catalog->target);
      record->next = projected(q->catalog->next);
    }
    if (dest != q) {
      q->all_next = consumed;
      consumed = q;
    }
  }
  s->entries = projected(v->entries);
  for (group = 0; group < PS_VALUE_GROUPS; ++group)
    for (i = 0; i < BUCKETS; ++i) {
      ps_value_bucket *bucket = &v->buckets->values[group][i];
      ps_entry **link;
      bucket->head = projected(bucket->head);
      for (link = &bucket->head; *link; link = &(*link)->value_next)
        (*link)->value_link = link;
    }
  for (i = 0; i < BUCKETS; ++i)
    v->buckets->streams[i] = projected(v->buckets->streams[i]);
  for (i = 0; i < v->catalogs.capacity; ++i) {
    ps_entry **link = &v->catalogs.buckets[i];
    *link = projected(*link);
    for (; (p = *link) != NULL; link = &p->catalog->next)
      p->catalog->link = link;
  }
  for (p = s->entries; p; p = p->all_next) {
    p->all_link = p == s->entries ? &s->entries : p->all_link;
    if (p->all_next)
      p->all_next->all_link = &p->all_next;
  }
  {
    ps_entry **link;
    for (link = &s->dirty; *link; link = &(*link)->dirty_next)
      (*link)->dirty_link = link;
  }
  while (keep) {
    p = keep;
    keep = p->all_next;
    register_entry(s, p);
    if (p->state == 2) {
      p->retired_next = s->retired;
      s->retired = p;
    }
  }
  while (borrowed) {
    p = borrowed;
    borrowed = p->all_next;
    register_entry(s, p);
    value_register(v, p);
  }
  /* Every link now points at its final entry. Reused staged entries are
   * kept until here because other links can still refer to their bindings. */
  while (consumed) {
    q = consumed;
    consumed = q->all_next;
    ps_internal_entry_free(q);
  }
  v->entries = NULL;
}
static void projection_install(ps_space *s, ps_space *v) {
  ps_entry_allocation *block;
  /* Entries imported from the draft now belong to the resident owner. */
  while ((block = v->entry_allocations) != NULL) {
    entry_allocation_unlink(block);
    entry_allocation_register(s, block);
    if (block->owner) {
      entry_available_unlink(block);
      block->owner = s;
      block->temporary = 0;
      if (block->used != ENTRY_SLAB_MASK)
        entry_available_link(block);
    }
  }
#define PROJECTION_SWAP(field, type)                                           \
  do {                                                                         \
    type previous = s->field;                                                  \
    s->field = v->field;                                                       \
    v->field = previous;                                                       \
  } while (0)
  PROJECTION_SWAP(free, ps_run *);
  PROJECTION_SWAP(keys, ps_key_index);
  PROJECTION_SWAP(buckets, ps_space_buckets *);
  PROJECTION_SWAP(metadata_free, ps_run *);
  PROJECTION_SWAP(pinned_free, ps_run *);
  PROJECTION_SWAP(pages, ps_meta_page *);
  PROJECTION_SWAP(payload_pages, ps_payload_page *);
  PROJECTION_SWAP(catalogs, ps_catalog_index);
  PROJECTION_SWAP(journal_image, unsigned char *);
#undef PROJECTION_SWAP
  s->pinned_pages = v->pinned_pages;
  s->pinned_page_probe = NULL;
  s->pinned_free_probe = NULL;
  if (s->pages)
    s->pages->link = &s->pages;
  if (s->payload_pages)
    s->payload_pages->link = &s->payload_pages;
  s->root = v->root;
  s->capacity = v->capacity;
  s->generation = v->generation;
  s->sequence = v->sequence;
  s->token_limit = v->token_limit;
  s->journal_start = v->journal_start;
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
  s->selected_reserves = v->selected_reserves;
  s->retiring_reserves = v->retiring_reserves;
  s->selected_reserve_count = v->selected_reserve_count;
  s->reserve_pruned_local = v->reserve_pruned_local;
  s->reserve_pruned_peer = v->reserve_pruned_peer;
  if (s->retiring_reserves)
    s->retiring_reserves->retiring_link = &s->retiring_reserves;
  s->peer_durable = v->peer_durable;
  if (s->peer_durable >= s->own_ready_wire)
    mark_durable(s, s->own_ready_sequence);
}
/* Prepare only private metadata. The resident mutex protects the local
 * entries and witnesses this projection will carry across installation. The
 * selected checkpoint must be revalidated before importing any peer journal. */
static int projection_stage(ps_space *s, ps_space *v,
                            const ps_readiness_certificate *certificate,
                            int reuse, lc_error *error) {
  ps_entry *p, *old;
  payload_rebuild build = {0};
  int rc;
  v->projection_resident = s;
  rc = recover_directory(v, v->journal_image, certificate, s, reuse, &build,
                         error);
  /* The checkpoint prefix covers the resident journal. Consequently only
   * checkpoint entries, including those superseded by the following tail,
   * can match an existing incarnation. Bind them before decoding that tail. */
  for (p = v->entries; rc == LC_OK && p; p = p->all_next) {
    old = p->projection;
    if (old) {
      if (!projection_same(old, p)) {
        rc = err(error, LC_ERR_PROTOCOL,
                 "checkpoint changes an existing incarnation");
        break;
      }
      if (p->key.kind == PS_STREAM && old->bytes > p->bytes) {
        p->bytes = old->bytes;
        p->stream.head->admitted = 0;
        if (!p->dirty_link)
          dirty_entry(v, p, NULL);
      }
    }
    rc = payload_claim_entry(v, p, &build, error);
  }
  /* Overlay only continuously retained ownership. Unpinned old canonical
   * ranges may already have been reused by peers; never retire them here. */
  for (p = s->entries; rc == LC_OK && p; p = p->all_next) {
    if (p->state != 0 && !p->pins && !entry_catalog_pinned(p))
      continue;
    old = lookup(v, p->key);
    if (old == p || (old && old->projection == p))
      continue;
    rc = payload_claim_entry(v, p, &build, error);
  }
  if (rc == LC_OK)
    rc = payload_rebuild_free(v, &build, error);
  lc_free_with_allocator(NULL, build.ranges);
  if (rc == LC_OK && s->credit_record)
    rc = exclude_run(v, s->credit_record->offset, PS_PAGE, error);
  return rc == LC_OK ? projection_pool(v, s->free, error) : rc;
}
static ps_space *projection_new(ps_space *s, lc_error *error) {
  ps_space *v = lc_calloc_with_allocator(NULL, 1, sizeof(*v));
  if (!v) {
    err(error, LC_ERR_NOMEM, "cannot allocate checkpoint projection");
    return NULL;
  }
  v->fd = s->fd;
  v->coordination = s->coordination;
  v->io = s->io;
  return v;
}
static void projection_destroy(ps_space *v) {
  if (v) {
    v->fd = -1;
    v->coordination = NULL;
    destroy(v);
  }
}
static void projection_select(ps_space *v, const unsigned char *root,
                              int selected) {
  v->root = selected;
  v->generation = ps_wire_get64(root + 8);
  v->capacity = ps_wire_get64(root + 16);
  v->sequence = ps_wire_get64(root + 24);
  v->directory.offset = ps_wire_get64(root + 32);
  v->directory.generation = ps_wire_get64(root + 40);
  v->token_limit = ps_wire_get64(root + 48);
  v->journal_start = ps_wire_get64(root + ROOT_JOURNAL_START);
  memcpy(v->identity, root + 56, sizeof(v->identity));
}
static int projection_load(ps_space *s, ps_space *v, const unsigned char *root,
                           int selected,
                           const ps_readiness_certificate *certificate,
                           int reuse, lc_error *error) {
  projection_select(v, root, selected);
  v->peer_durable = certificate->durable;
  v->journal_image = lc_alloc_with_allocator(NULL, PS_JOURNAL);
  if (!v->journal_image)
    return err(error, LC_ERR_NOMEM,
               "cannot allocate projection control buffer");
  return projection_stage(s, v, certificate, reuse, error);
}
static int read_checkpoint_roots(ps_space *s, unsigned char *roots,
                                 lc_error *error) {
  int rc = transfer(s, roots, PS_ROOT_BYTES, 0, 0, error);
  if (rc == LC_OK)
    rc = transfer(s, roots + PS_ROOT_BYTES, PS_ROOT_BYTES, PS_PAGE, 0, error);
  return rc;
}
static void preview_checkpoint(ps_space *s, checkpoint_preview *preview) {
  const unsigned char zero[PS_ROOT_BYTES] = {0};
  const unsigned char *root;
  int selected;
  lc_error_init(&preview->error);
  preview->view = projection_new(s, &preview->error);
  preview->result = preview->view
                        ? read_checkpoint_roots(preview->view, preview->roots,
                                                &preview->error)
                        : LC_ERR_NOMEM;
  if (preview->result != LC_OK)
    return;
  selected = ps_wire_get64(preview->roots + PS_ROOT_BYTES + 8) ==
             preview->certificate.root;
  root = preview->roots + selected * PS_ROOT_BYTES;
  if (root_valid(root) &&
      ps_wire_get64(root + 8) == preview->certificate.root &&
      !memcmp(root + 56, s->identity, 16) &&
      !memcmp(preview->roots + (1 - selected) * PS_ROOT_BYTES, zero,
              sizeof(zero)) &&
      ps_wire_get64(root + 16) >= s->capacity &&
      ps_wire_get64(root + 24) >= s->sequence &&
      ps_wire_get64(root + 48) <= preview->certificate.grant_limit &&
      (!s->maximum || ps_wire_get64(root + 16) <= s->maximum))
    preview->result =
        projection_load(s, preview->view, root, selected, &preview->certificate,
                        1, &preview->error);
}
static int canonical_refresh(ps_space *s, checkpoint_preview *preview,
                             int verify_disk, lc_error *error) {
  unsigned char roots[2 * PS_ROOT_BYTES], zero[PS_ROOT_BYTES] = {0};
  const unsigned char *root;
  ps_space *v = NULL;
  struct stat st;
  int a, b, selected, rc;
  int retiring_journal, retired_certificate;
  ps_readiness_certificate certificate;
  rc = canonical_certificate(s, &certificate, error);
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
  rc = read_checkpoint_roots(s, roots, error);
  if (rc != LC_OK)
    return rc;
  a = root_valid(roots);
  b = root_valid(roots + PS_ROOT_BYTES);
  selected = b && (!a || ps_wire_get64(roots + PS_ROOT_BYTES + 8) >
                             ps_wire_get64(roots + 8));
  root = roots + selected * PS_ROOT_BYTES;
  if ((!a && !b) || memcmp(root + 56, s->identity, 16) ||
      (a && b && !root_follows(root, roots + (1 - selected) * PS_ROOT_BYTES))) {
    ps_readiness_begin(s, PS_READINESS_ROOT);
    return err(error, LC_ERR_PROTOCOL,
               "conflicting checkpoint projection roots");
  }
  if (ps_wire_get64(root + 8) != certificate.root)
    ps_readiness_begin(s, PS_READINESS_ROOT);
  rc = checkpoint_retirement(&certificate, ps_wire_get64(root + 8),
                             ps_wire_get64(root + 24), error);
  if (rc != LC_OK)
    return rc;
  retired_certificate =
      (certificate.pending & PS_READINESS_CHECKPOINT_RETIRE) != 0;
  /* A successor that covers the entire certified prefix can still have
   * residue from its predecessor at the wrap. Its own window is empty. */
  retiring_journal =
      retired_certificate || (ps_wire_get64(root + 8) > certificate.root &&
                              ps_wire_get64(root + 24) == certificate.ready &&
                              ps_wire_get32(root + ROOT_JOURNAL_COVERED));
  if (ps_wire_get64(root + 8) == s->generation)
    return canonical_journal(s, &certificate, 1, 1, error);
  if (ps_wire_get64(root + 8) < s->generation ||
      ps_wire_get64(root + 24) < s->sequence ||
      ps_wire_get64(root + 16) < s->capacity)
    return err(error, LC_ERR_PENDING,
               "checkpoint would regress a live projection");
  if (ps_wire_get64(root + 48) > certificate.grant_limit)
    return err(error, LC_ERR_PENDING,
               "checkpoint incarnation frontier awaits certification");
  if (ps_wire_get64(root + 8) < certificate.root)
    return err(error, LC_ERR_PENDING,
               "selected root is behind certified retirement");
  if ((s->maximum && ps_wire_get64(root + 16) > s->maximum) ||
      fstat(s->fd, &st) || st.st_size < (off_t)ps_wire_get64(root + 16))
    return err(error, LC_ERR_INVALID, "checkpoint capacity unavailable");
  /* A peer can retire and reuse pages while we prepare. Accept successful
   * work only against the same selected roots and readiness certificate.
   * A failed draft belongs to that same epoch or is discarded with it. */
  if (preview && !certificate.pending &&
      certificate.root == preview->certificate.root &&
      certificate.origin == preview->certificate.origin &&
      certificate.root == ps_wire_get64(root + 8) &&
      (preview->result != LC_OK ||
       (preview->view && preview->view->generation &&
        !memcmp(preview->roots, roots, sizeof(roots))))) {
    v = preview->view;
    preview->view = NULL;
    rc = preview->result;
    if (rc != LC_OK && error) {
      lc_error_cleanup(error);
      *error = preview->error;
      lc_error_init(&preview->error);
    }
  } else {
    v = projection_new(s, error);
    rc = v ? projection_load(s, v, root, selected, &certificate, !verify_disk,
                             error)
           : LC_ERR_NOMEM;
  }
  if (rc != LC_OK)
    goto done;
  v->token_limit = certificate.grant_limit;
  v->peer_durable = certificate.durable;
  if (!retiring_journal) {
    if (!verify_disk && !certificate.pending &&
        certificate.root == v->generation)
      rc = import_certified_journal(v, &certificate, 1, error);
    else
      rc = projection_journal(v, ps_wire_get64(root + 24), certificate.ready,
                              error);
  }
  if (rc == LC_OK && v->sequence < certificate.ready)
    rc = err(error, LC_ERR_PENDING,
             "checkpoint is behind the certified journal prefix");
  if (rc == LC_OK && certificate.root == v->generation &&
      v->journal != certificate.journal_bytes)
    rc = err(error, LC_ERR_PROTOCOL, "inconsistent certified journal bound");
  if (rc == LC_ERR_PENDING || rc == LC_ERR_PROTOCOL)
    ps_readiness_begin(s, PS_READINESS_JOURNAL);
  if (rc != LC_OK)
    goto done;
  v->range_pins = 1;
  if (rc == LC_OK)
    rc = pins_recover(v, s, error);
  if (rc == LC_OK)
    rc = metadata_bound(v->metadata, v->live_entries, &v->metadata_workspace,
                        error);
  if (rc == LC_OK && a && b && certificate.root < v->generation &&
      !retired_certificate) {
    rc = sync_file(s, error);
    if (rc == LC_OK && retiring_journal)
      ps_readiness_begin(s, PS_READINESS_CHECKPOINT_RETIRE);
  }
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
  if (rc == LC_OK && retiring_journal) {
    size_t covered = ps_wire_get32(root + ROOT_JOURNAL_COVERED);
    rc = clear_checkpoint_journal(s, v->journal_start - covered,
                                  v->journal_image, covered, error);
    memset(v->journal_image, 0, 64);
  }
  if (rc == LC_OK &&
      (!s->readiness || certificate.root < v->generation ||
       memcmp(roots + (1 - selected) * PS_ROOT_BYTES, zero, sizeof(zero)))) {
    uint64_t prepared = ps_readiness_issued_tickets(s);
    rc = sync_file(s, error);
    if (rc == LC_OK) {
      ps_readiness_finish(s, PS_READINESS_CHECKPOINT_RETIRE);
      rc = ps_readiness_stabilize(s, v->sequence, v->generation, v->journal,
                                  prepared, error);
      if (rc == LC_OK)
        v->peer_durable = v->sequence;
    }
  }
  if (rc == LC_OK) {
    projection_entries(s, v);
    projection_install(s, v);
    if (certificate.pending & PS_READINESS_JOURNAL)
      ps_readiness_finish(s, PS_READINESS_JOURNAL);
  }
done:
  if (v && v->poisoned)
    __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
  projection_destroy(v);
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
  if ((options->initial_capacity &&
       (options->initial_capacity < PS_MIB ||
        options->initial_capacity % PS_MIB ||
        options->initial_capacity > PS_MAX_CAPACITY)) ||
      (options->maximum_capacity &&
       (options->maximum_capacity % PS_MIB ||
        options->maximum_capacity > PS_MAX_CAPACITY)))
    return err(error, LC_ERR_INVALID,
               "container capacity must be whole MiB and at most 4 EiB");
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
  if (ps_coordination_shared(s->coordination)) {
    s->range_pins = 1; /* Later participants may introduce new witnesses. */
    s->metadata_claims_active = 1;
  }
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
    s->token_limit = opening ? TOKEN_POOL : TOKEN_RANGE;
    s->token_end = TOKEN_RANGE;
    if (rc == LC_OK)
      rc = write_token_limit(s, 0, s->token_limit, error);
    if (rc == LC_OK)
      rc = initial_free(s, error);
    if (rc == LC_OK)
      rc = checkpoint_locked(s, error);
    if (rc == LC_OK)
      rc = sync_parent(path, error);
  } else if (rc == LC_OK) {
    rc = ps_readiness_attach(s, error);
    if (rc == LC_OK)
      rc = recover(s, &opening, error);
  }
  if (rc == LC_OK && s->maximum && s->capacity > s->maximum)
    rc = err(error, LC_ERR_INVALID,
             "committed capacity exceeds configured maximum");
  if (rc == LC_OK)
    rc = metadata_claimed_bound(s, s->metadata, s->live_entries, &bound, error);
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
  mark_durable(s, s->peer_durable);
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
  exclusive_record_close(s);
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
  ps_entry *p = NULL;
  int rc;
  if (out)
    *out = NULL;
  if (!s || !out ||
      (!ps_internal_immutable_key(key) && !(owned && key.kind == PS_OWNED_RUN)))
    return err(error, LC_ERR_INVALID,
               "container writer requires typed key and output");
  w = lc_calloc_with_allocator(NULL, 1, sizeof(*w));
  if (!w)
    return err(error, LC_ERR_NOMEM, "cannot allocate container writer");
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc == LC_OK)
    rc = ps_internal_budget_check(s, budget, 0, 0, 0, error);
  if (rc == LC_OK)
    rc = ps_internal_token_range(s, error);
  if (rc == LC_OK) {
    p = space_entry_new(s, key.kind, 0, 0);
    if (!p)
      rc = err(error, LC_ERR_NOMEM, "cannot allocate container writer entry");
  }
  if (rc == LC_OK) {
    p->key = key;
    p->token = s->token_next++;
    register_entry(s, p);
    ++s->writers;
    w->entry = p;
    w->extent_capacity = p->embedded_extents ? 1 : 0;
    w->space = s;
    w->budget = budget;
    if (budget)
      ++budget->users;
  }
  pthread_mutex_unlock(&s->mutex);
  if (rc != LC_OK)
    lc_free_with_allocator(NULL, w);
  else
    *out = w;
  return rc;
}
int ps_begin(ps_space *s, ps_budget *budget, ps_key key, ps_writer **out,
             lc_error *error) {
  return begin_value(s, budget, key, 0, out, error);
}
/* Admit the buffered tail while the caller holds allocator authority.
 * Every admitted extent belongs to the unpublished writer, including on
 * failure and while payload I/O runs without the allocator. */
static int reserve_writer_tail(ps_writer *w, lc_error *error) {
  ps_space *s = w->space;
  ps_entry *p = w->entry;
  ps_extent *extent, *grown;
  size_t n, done = 0, take;
  uint64_t run_bytes;
  int rc, split = 0;
  while (done < w->used) {
    if (p->count == (PS_JOURNAL - 64) / 24)
      return err(error, LC_ERR_INVALID,
                 "value extent metadata exceeds journal capacity");
    if (p->count == w->extent_capacity) {
      n = w->extent_capacity ? w->extent_capacity * 2 : 4;
      grown =
          p->embedded_extents
              ? lc_alloc_with_allocator(NULL, n * sizeof(*grown))
              : lc_realloc_with_allocator(NULL, p->extents, n * sizeof(*grown));
      if (!grown)
        return err(error, LC_ERR_NOMEM, "cannot grow private extent vector");
      if (p->embedded_extents)
        memcpy(grown, p->extents, p->count * sizeof(*grown));
      p->extents = grown;
      w->extent_capacity = n;
      p->embedded_extents = 0;
      ps_internal_entry_auxiliary(p);
    }
    extent = &p->extents[p->count];
    memset(extent, 0, sizeof(*extent));
    take = w->used - done;
    run_bytes = take / PS_PAGE * PS_PAGE;
    if (!run_bytes) {
      n = take;
      rc =
          payload_reserve_locked(s, p, extent, &take, !split, w->budget, error);
      if (take < n)
        split = 1;
      run_bytes = take;
    } else {
      rc = immutable_extent_allowance(s, p, run_bytes, w->budget, error);
      if (rc == LC_OK)
        rc = take_payload_locked(s, w->budget, &run_bytes, &extent->offset,
                                 error);
      if (rc == LC_OK)
        ps_internal_budget_spend(w->budget, run_bytes, 0, 0);
    }
    if (rc != LC_OK)
      return rc;
    if (run_bytes < take)
      take = (size_t)run_bytes;
    extent->bytes = take;
    extent->crc = checksum(w->buffer + done, take);
    ++p->count;
    p->payload_claimed = 1;
    p->bytes += take;
    done += take;
  }
  return LC_OK;
}
#define WRITER_FLUSH_BATCH 8
/* One allocator capture for up to a chunk of already buffered tails. The
 * caller limits both the number of writers and the bytes in this batch. */
static int flush_writer_batch(ps_writer *const *writers, size_t count,
                              lc_error *error) {
  ps_space *s = writers[0]->space;
  ps_coordination_guard *guard;
  size_t i, j, at, first[WRITER_FLUSH_BATCH] = {0};
  int rc;
  for (i = 0; i < count && !writers[i]->used; ++i) {
  }
  if (i == count)
    return LC_OK;
  pthread_mutex_lock(&s->mutex);
  /* Existing packed pages prove their individual witnesses. A new page or
   * unbudgeted growth refreshes physical ownership through admission. */
  rc = ps_internal_capture_begin(s, &guard, error);
  for (j = i; rc == LC_OK && j < count; ++j) {
    first[j] = writers[j]->entry->count;
    rc = reserve_writer_tail(writers[j], error);
  }
  rc = allocation_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  for (; rc == LC_OK && i < count; ++i) {
    ps_writer *w = writers[i];
    ps_entry *p = w->entry;
    at = 0;
    for (j = first[i]; rc == LC_OK && j < p->count; ++j) {
      const ps_extent *extent = &p->extents[j];
      rc = transfer(s, w->buffer + at, (size_t)extent->bytes, extent->offset, 1,
                    error);
      at += (size_t)extent->bytes;
    }
    if (rc == LC_OK)
      w->used = 0;
  }
  return rc;
}
static int flush_writer(ps_writer *w, lc_error *error) {
  return flush_writer_batch(&w, 1, error);
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
static int publish_value(ps_writer *w, ps_ref *ref, uint64_t *ready,
                         lc_error *error) {
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
  rc = ps_internal_capture_begin(s, &guard, error);
  old = lookup(s, w->entry->key);
  prior = old && !old->deleted ? entry_bytes(old) : 0;
  if (rc == LC_OK && w->entry->key.kind == PS_OWNED_RUN &&
      (!w->expected_owned || old != w->expected_owned || old->deleted ||
       !stream_lookup(s, old->key.owner) || !w->entry->stream.run->mapped ||
       w->entry->stream.run->capacity != old->stream.run->capacity))
    rc = err(error, LC_ERR_PENDING, "captured piece mapping changed");
  if (rc == LC_OK && w->entry->key.kind == PS_OWNED_RUN)
    rc =
        stream_structure_ready(s, stream_lookup(s, w->entry->key.owner), error);
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
    install(s, w->entry, old);
    w->published = 1;
    rc = entry_release_claims(s, w->entry, error);
    sequence = w->entry->publication_sequence;
    memcpy(published_ref.container, s->identity, 16);
    published_ref.incarnation = w->entry->token;
  } else
    w->failed = 1;
  rc = allocation_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK) {
    if (ready)
      *ready = sequence;
    else if (durable)
      rc = wait_durable(s, sequence, error);
  }
  if (rc == LC_OK && ref)
    *ref = published_ref;
  if (rc != LC_OK)
    w->failed = 1;
  return rc;
}
int ps_publish(ps_writer *w, ps_ref *ref, lc_error *error) {
  return publish_value(w, ref, NULL, error);
}
int ps_internal_publish_deferred(ps_writer *w, uint64_t *ready,
                                 lc_error *error) {
  return publish_value(w, NULL, ready, error);
}
int ps_writer_key_deletion(ps_writer *w, uint64_t bytes, lc_error *error) {
  uint64_t encoded;
  if (!w || w->failed || w->published || w->entry->key.kind != PS_KEY_METADATA)
    return err(error, LC_ERR_INVALID,
               "deletion reserve requires an unpublished key slot");
  encoded = w->entry->bytes + w->used;
  if (bytes > encoded && bytes - encoded > 512)
    return err(error, LC_ERR_INVALID, "deletion reserve exceeds its key slot");
  return ps_internal_reserve_entry(w->entry, bytes, error);
}
uint64_t ps_reader_key_deletion(const ps_reader *r) {
  return r->entry->deletion_bytes;
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
static int publish_revision(ps_writer *const *writers, size_t count,
                            const ps_revision_retirement *retirements,
                            size_t retire_count, int flags,
                            enum ps_commit_mode mode, ps_ref *refs,
                            uint64_t *prefix, lc_error *error) {
  ps_space *s;
  ps_budget *budget;
  ps_coordination_guard *guard = NULL;
  ps_entry *p, *old;
  ps_entry **removed = NULL;
  unsigned char *wire = NULL;
  size_t i, j, batch, pending, bytes = 64, cursor, prior, descriptor;
  uint64_t metadata, entries, growth = 0, added = 0, sequence = 0, deletion = 0;
  int rc = LC_OK, durable, forced;
  if (prefix)
    *prefix = 0;
  if (!writers || !count || count > (PS_JOURNAL - 64) / 64 || !writers[0] ||
      retire_count > (PS_JOURNAL - 64) / 64 - count ||
      (retire_count && !retirements) ||
      (flags & ~(PS_REVISION_FORCE_DURABLE | PS_REVISION_ADMIT_RESERVE)) ||
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
  for (i = 0; i < retire_count; ++i) {
    ps_key key = retirements[i].key;
    if (key.kind != PS_KEY_METADATA || !valid_key(key) ||
        !retirements[i].incarnation)
      return err(error, LC_ERR_INVALID,
                 "revision requires metadata retirement");
    for (j = 0; j < i; ++j)
      if (same_key(retirements[j].key, key))
        return err(error, LC_ERR_INVALID, "revision keys must be distinct");
    for (j = 0; j < count; ++j)
      if (same_key(writers[j]->entry->key, key))
        return err(error, LC_ERR_INVALID, "revision keys must be distinct");
  }
  bytes += retire_count * 64;
  for (i = 0; rc == LC_OK && i < count; i += batch) {
    pending = 0;
    for (batch = 0; batch < WRITER_FLUSH_BATCH && i + batch < count &&
                    writers[i + batch]->used <= PS_CHUNK - pending;
         ++batch)
      pending += writers[i + batch]->used;
    rc = flush_writer_batch(writers + i, batch, error);
    for (j = i; rc == LC_OK && j < i + batch; ++j) {
      descriptor = entry_bytes(writers[j]->entry);
      if (descriptor > PS_JOURNAL - bytes)
        rc = err(error, LC_ERR_INVALID, "revision metadata exceeds journal");
      else
        bytes += descriptor;
    }
  }
  if (rc == LC_OK) {
    wire = lc_alloc_with_allocator(NULL, bytes + (s->readiness ? 64 : 0));
    if (!wire)
      rc = err(error, LC_ERR_NOMEM, "cannot allocate revision publication");
  }
  if (rc == LC_OK && retire_count) {
    removed = lc_calloc_with_allocator(NULL, retire_count, sizeof(*removed));
    if (!removed)
      rc = err(error, LC_ERR_NOMEM, "cannot allocate revision retirements");
  }
  for (i = 0; rc == LC_OK && i < retire_count; ++i) {
    removed[i] = ps_internal_entry_new(retirements[i].key.kind, 0);
    if (!removed[i])
      rc = err(error, LC_ERR_NOMEM, "cannot allocate revision retirement");
    else {
      removed[i]->key = retirements[i].key;
      removed[i]->token = retirements[i].incarnation;
      removed[i]->deleted = 1;
    }
  }
  if (rc != LC_OK)
    goto done;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &guard, error);
  if (rc == LC_OK &&
      (count + retire_count > LC_U64_MAX - s->sequence ||
       count + retire_count > LC_U64_MAX - __atomic_load_n(&s->pending_sequence,
                                                           __ATOMIC_RELAXED)))
    rc = err(error, LC_ERR_INVALID, "revision publication sequence exhausted");
  if (rc == LC_OK && bytes > PS_JOURNAL - s->journal)
    rc = checkpoint_locked(s, error);
  metadata = s->metadata;
  entries = s->live_entries;
  forced = (flags & PS_REVISION_FORCE_DURABLE) != 0;
  durable = s->durable || forced;
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
    if (p->deletion_bytes > deletion)
      deletion = p->deletion_bytes;
  }
  for (i = 0; rc == LC_OK && i < retire_count; ++i) {
    old = lookup(s, removed[i]->key);
    if (!revision_retirement_matches(old, removed[i]->token))
      rc = err(error, LC_ERR_PENDING, "revision retirement owner changed");
    else {
      metadata -= entry_bytes(old);
      --entries;
    }
  }
  if (rc == LC_OK)
    rc = ps_internal_budget_check(s, budget, 0, growth, added, error);
  if (rc == LC_OK)
    rc = checkpoint_allowance_floor(s, metadata, entries, 0,
                                    PS_MAINTENANCE_RESERVE, budget, error);
  if (rc == LC_OK && deletion && (flags & PS_REVISION_ADMIT_RESERVE) &&
      ps_coordination_shared(s->coordination))
    rc = ps_internal_reserve_recheck(s, deletion, error);
  if (rc == LC_OK)
    rc = key_index_reserve(s, (size_t)added, error);
  if (rc == LC_OK) {
    memset(wire, 0, 64);
    ps_wire_put32(wire, REVISION_MAGIC);
    ps_wire_put32(wire + 4, (uint32_t)bytes);
    ps_wire_put64(wire + 8, s->sequence + 1);
    ps_wire_put32(wire + 16, (uint32_t)(count + retire_count));
    ps_wire_put32(wire + 20, (uint32_t)durable);
    cursor = 64;
    for (i = 0; i < count; ++i) {
      encode_entry(wire + cursor, writers[i]->entry, s->sequence + 1 + i,
                   PS_PUT);
      cursor += entry_bytes(writers[i]->entry);
    }
    for (i = 0; i < retire_count; ++i) {
      encode_entry(wire + cursor, removed[i], s->sequence + 1 + count + i,
                   PS_REMOVE);
      cursor += 64;
    }
    ps_wire_put32(wire + 60, (uint32_t)ps_crc32(checksum(wire, 60), wire + 64,
                                                (uInt)(bytes - 64)));
    rc = write_journal_frame(s, wire, bytes, error);
  }
  if (rc == LC_OK) {
    s->sequence += count + retire_count;
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
      install(s, p, old);
      writers[i]->published = 1;
      if (refs) {
        memcpy(refs[i].container, s->identity, sizeof(refs[i].container));
        refs[i].incarnation = p->token;
      }
    }
    for (i = 0; i < retire_count; ++i) {
      p = removed[i];
      old = lookup(s, p->key);
      p->publication_sequence = ++sequence;
      p->durable_visibility = durable;
      register_entry(s, p);
      dirty_entry(s, p, old);
      install(s, p, old);
      removed[i] = NULL;
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
  if (removed)
    for (i = 0; i < retire_count; ++i)
      if (removed[i])
        ps_internal_entry_free(removed[i]);
  lc_free_with_allocator(NULL, removed);
  return rc;
}
int ps_revision_publish(ps_writer *const *writers, size_t count, int flags,
                        enum ps_commit_mode mode, ps_ref *refs,
                        uint64_t *prefix, lc_error *error) {
  return publish_revision(writers, count, NULL, 0, flags, mode, refs, prefix,
                          error);
}
int ps_revision_publish_retiring(ps_writer *const *writers, size_t count,
                                 const ps_revision_retirement *retirements,
                                 size_t retire_count, int flags,
                                 enum ps_commit_mode mode, uint64_t *prefix,
                                 lc_error *error) {
  return publish_revision(writers, count, retirements, retire_count, flags,
                          mode, NULL, prefix, error);
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
    ps_internal_entry_free(w->entry);
  }
  pthread_mutex_unlock(&s->mutex);
  lc_free_with_allocator(NULL, w);
}
int ps_internal_read_witness_valid(ps_space *s) {
  __atomic_thread_fence(__ATOMIC_SEQ_CST);
  return ps_readiness_retains_capture(s);
}
int ps_internal_resident_read_valid(ps_space *s, int private_failure) {
  __atomic_thread_fence(__ATOMIC_SEQ_CST);
  if (!ps_readiness_resident(s))
    return 0;
  if (private_failure)
    __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
  return 1;
}
static ps_reader *reader_new(ps_space *s, ps_entry *entry) {
  size_t scratch =
      entry && entry->bytes < PS_CHUNK ? (size_t)entry->bytes : PS_CHUNK;
  ps_reader *r = lc_alloc_with_allocator(NULL, sizeof(*r) + scratch);
  if (r) {
    memset(r, 0, sizeof(*r));
    r->space = s;
    r->entry = entry;
    r->buffer = (unsigned char *)(r + 1);
  }
  return r;
}
static int read_pin(ps_space *s, ps_key key, const ps_ref *ref, int resident,
                    ps_entry **out, lc_error *error) {
  ps_entry *p =
      selected_value(s, lookup(s, key),
                     __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE));
  int provisional, rc;
  if (!p)
    return err(error, LC_ERR_INVALID, "container key not found");
  if (ref &&
      (memcmp(ref->container, s->identity, 16) || ref->incarnation != p->token))
    return err(error, LC_ERR_INVALID, "stale or foreign container reference");
  provisional = resident && p->count && !p->pins && !p->payload_claimed;
  rc = entry_pin(s, p, provisional, error);
  if (rc != LC_OK)
    return rc;
  /* The resident entry stays in memory under s->mutex. Its bytes may have
   * been retired since selection. Publish the extent witnesses before
   * rechecking the certificate. Reclaimers can clear a page hint only after
   * observing a free range with no kernel witness under allocator authority.
   * A matching root rules out reuse since selection. Any subsequent
   * checkpoint that retires these bytes must observe our witness. */
  if (provisional && !ps_internal_read_witness_valid(s)) {
    ps_internal_entry_unpin(s, p);
    return healthy(s, error);
  }
  *out = p;
  return LC_OK;
}
int ps_read_open(ps_space *s, ps_key key, const ps_ref *ref, ps_reader **out,
                 lc_error *error) {
  ps_reader *r = NULL;
  ps_entry *entry = NULL;
  int rc;
  if (out)
    *out = NULL;
  if (!s || !out || !ps_internal_immutable_key(key))
    return err(error, LC_ERR_INVALID,
               "container reader requires typed key and output");
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc == LC_OK && (!s->readiness || ps_readiness_resident(s))) {
    observe_local_durability(s);
    rc = read_pin(s, key, ref, s->readiness && !s->allocation_guard, &entry,
                  error);
  }
  /* A resident selection can size its scratch exactly. On refresh, reserve
   * the maximum before adopting the peer's checkpoint. */
  if (rc == LC_OK) {
    r = reader_new(s, entry);
    if (!r)
      rc = err(error, LC_ERR_NOMEM, "cannot allocate bounded reader");
  }
  if (rc == LC_OK && !entry) {
    ps_coordination_guard *guard;
    rc = ps_internal_capture_begin(s, &guard, error);
    if (rc == LC_OK)
      rc = read_pin(s, key, ref, 0, &entry, error);
    rc = ps_internal_capture_end(s, guard, rc, error);
  }
  if (rc == LC_OK) {
    r->entry = entry;
    ++s->readers;
    *out = r;
  } else if (entry)
    ps_internal_entry_unpin(s, entry);
  pthread_mutex_unlock(&s->mutex);
  if (rc != LC_OK)
    lc_free_with_allocator(NULL, r);
  return rc;
}
static void reader_payload_invalid(ps_reader *r) {
  if (r->private_failure)
    return;
  if (r->imported) {
    r->entry->value_checked = -1;
    return;
  }
  pthread_mutex_lock(&r->space->mutex);
  r->entry->value_checked = -1;
  pthread_mutex_unlock(&r->space->mutex);
}
/* A walk retains the selected cells before reading a packed page. Reuse the
 * checked bytes until the reader loads another extent. Standalone readers
 * reserve at most one extent of scratch, including for random reads. */
static int reader_load(ps_reader *r, const ps_extent *extent,
                       const char *mismatch, lc_error *error) {
  const unsigned char *data;
  int rc = read_extent(r->space, extent, r->buffer, r->packed, &data,
                       r->private_failure, error);
  if (rc != LC_OK)
    return rc;
  if (checksum(data, (size_t)extent->bytes) != extent->crc) {
    reader_payload_invalid(r);
    return err(error, LC_ERR_PROTOCOL, mismatch);
  }
  r->data = data;
  return LC_OK;
}
int ps_internal_checkpoint_span(ps_reader *r, const unsigned char **out,
                                lc_error *error) {
  const ps_entry *p = r->entry;
  const ps_extent *extent = p->extents;
  const unsigned char *bytes;
  int rc;
  *out = NULL;
  if (!r->private_failure || p->count != 1 ||
      !(extent->flags & EXTENT_PACKED) || extent->bytes != p->bytes ||
      (p->key.kind != PS_KEY_METADATA && p->key.kind != PS_KEY_DECISION))
    return LC_OK;
  rc = read_packed_bytes(r->space, extent, r->packed, &bytes,
                         r->private_failure, error);
  if (rc != LC_OK)
    return rc;
  if (checksum(bytes, (size_t)extent->bytes) != extent->crc)
    return err(error, LC_ERR_PROTOCOL,
               "captured value payload checksum mismatch");
  *out = bytes;
  return LC_OK;
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
      rc = reader_load(r, extent, "container read payload checksum mismatch",
                       error);
      if (rc != LC_OK)
        return rc;
      r->loaded = (size_t)extent->bytes;
    }
    n = r->loaded - r->offset;
    if (n > count)
      n = count;
    memcpy(p, r->data + r->offset, n);
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
      rc = reader_load(r, extent, "captured value payload checksum mismatch",
                       error);
      if (rc != LC_OK)
        return rc;
      r->random_loaded = 1;
    }
    within = (size_t)(offset - r->random_base);
    take = (size_t)extent->bytes - within;
    if (take > count)
      take = count;
    memcpy(p, r->data + within, take);
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
static int checkpoint_value_pending(const ps_space *s, const ps_entry *p,
                                    uint64_t prefix) {
  return (s->durable || p->durable_visibility) &&
         (p->peer_sequence ? p->peer_sequence > s->peer_durable
                           : p->publication_sequence > prefix);
}
int ps_internal_namespace_seed_current(ps_space *s, uint64_t owner,
                                       const ps_catalog_info *active,
                                       uint64_t tail, size_t expected,
                                       ps_value_retain_fn match, void *context,
                                       int *current, lc_error *error) {
  ps_coordination_guard *guard;
  ps_entry *p;
  size_t bucket = owner % BUCKETS, count = 0;
  unsigned int group;
  uint64_t prefix;
  int rc, same = 1;
  *current = 0;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_observe_begin(s, &guard, error);
  prefix = __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE);
  if (rc == LC_OK && active) {
    p = lookup(s, active->stream);
    same = p && !p->deleted && !p->stream.head->sealed && p->bytes == tail &&
           p->token == active->target.incarnation &&
           !memcmp(s->identity, active->target.container, 16) &&
           stream_structure_ready(s, p, NULL) == LC_OK;
  }
  for (group = 1; rc == LC_OK && same && group < PS_VALUE_GROUPS; ++group)
    for (p = s->buckets->values[group][bucket].head; p; p = p->value_next) {
      if (p->key.owner != owner)
        continue;
      if (checkpoint_value_pending(s, p, prefix) ||
          (!p->deleted &&
           (++count > expected || !match(context, p->key, p->token)))) {
        same = 0;
        break;
      }
    }
  if (rc == LC_OK && same && count == expected &&
      (!s->readiness || s->allocation_guard || ps_readiness_resident(s)))
    *current = 1;
  rc = ps_internal_capture_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_internal_namespace_checkpoint_stabilize(ps_space *s, uint64_t owner,
                                               lc_error *error) {
  ps_coordination_guard *guard;
  uint64_t prefix, local = 0, peer = 0;
  ps_entry *p;
  unsigned int group;
  size_t bucket = owner % BUCKETS;
  int rc;

  if (!s->readiness)
    return LC_OK;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_observe_begin(s, &guard, error);
  prefix = __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE);
  for (group = 1; rc == LC_OK && group < PS_VALUE_GROUPS; ++group)
    for (p = s->buckets->values[group][bucket].head; p; p = p->value_next) {
      if (p->key.owner != owner || !checkpoint_value_pending(s, p, prefix))
        continue;
      if (p->peer_sequence) {
        if (p->peer_sequence > peer)
          peer = p->peer_sequence;
      } else if (p->publication_sequence > local) {
        local = p->publication_sequence;
      }
    }
  rc = ps_internal_capture_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  return rc == LC_OK ? ps_internal_durable_prefix(s, local, peer, error) : rc;
}
int ps_internal_durable_prefix(ps_space *s, uint64_t local, uint64_t peer,
                               lc_error *error) {
  uint64_t completed;
  int rc, synced;
  if (!local && !peer)
    return LC_OK;
  if (!peer)
    return wait_durable(s, local, error);

  rc = shared_durable(s, local, 0, peer, &completed, &synced, error);
  pthread_mutex_lock(&s->commit_mutex);
  ++s->sync_requests;
  s->sync_batches += synced;
  pthread_mutex_unlock(&s->commit_mutex);
  if (rc == LC_OK)
    mark_durable(s, completed);
  return rc;
}
int ps_internal_namespace_cut_stabilize(ps_space *s, uint64_t owner,
                                        uint64_t route, uint64_t coverage,
                                        lc_error *error) {
  const unsigned int kinds[] = {PS_ROUTING, PS_NAMESPACE_CHECKPOINT};
  ps_coordination_guard *guard;
  uint64_t local = 0, peer = 0, prefix;
  size_t i;
  int rc;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &guard, error);
  prefix = __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE);
  for (i = 0; rc == LC_OK && i < (coverage ? 2U : 1U); ++i) {
    ps_key key;
    ps_entry *entry;
    key.owner = owner;
    key.key = 1;
    key.kind = kinds[i];
    entry = lookup(s, key);
    if (!entry || entry->deleted || entry->token != (i ? coverage : route))
      rc = err(error, LC_ERR_PENDING, "namespace cut has since changed");
    else if (entry->peer_sequence > s->peer_durable &&
             entry->peer_sequence > peer)
      peer = entry->peer_sequence;
    else if (!entry->peer_sequence && entry->publication_sequence > prefix &&
             entry->publication_sequence > local)
      local = entry->publication_sequence;
  }
  rc = ps_internal_capture_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  return rc == LC_OK ? ps_internal_durable_prefix(s, local, peer, error) : rc;
}
int ps_internal_fixed_value(ps_space *s, ps_key key, uint64_t prefix,
                            void *buffer, size_t bytes, uint64_t known,
                            uint64_t *incarnation, int *current,
                            int *private_failure, lc_error *error) {
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
  if (!buffer) {
    *current = 0;
    return LC_OK;
  }
  for (i = 0; i < p->count; ++i) {
    const ps_extent *extent = p->extents + i;
    if (extent->bytes > left)
      return err(error, LC_ERR_PROTOCOL, "invalid fixed control extent");
    rc = file_transfer(s, dest, (size_t)extent->bytes, extent->offset, 0,
                       private_failure, error);
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
  if (!s || !ref || !bytes || !present || !ps_internal_immutable_key(key))
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
static ps_entry *validation_value(ps_space *s, ps_key key,
                                  const ps_ref *expected) {
  ps_entry *p =
      selected_value(s, lookup(s, key),
                     __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE));
  return p && (!expected || (expected->incarnation == p->token &&
                             !memcmp(expected->container, s->identity, 16)))
             ? p
             : NULL;
}
int ps_value_validate(ps_space *s, ps_key key, const ps_ref *expected,
                      int *valid, lc_error *error) {
  ps_entry *p;
  ps_coordination_guard *guard;
  unsigned char *buffer = NULL;
  int rc, checked = 0;
  if (!s || !valid || !ps_internal_immutable_key(key))
    return err(error, LC_ERR_INVALID, "value validation requires typed output");
  *valid = 0;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_observe_begin(s, &guard, error);
  p = rc == LC_OK ? validation_value(s, key, expected) : NULL;
  if (p && !p->value_checked && s->readiness && !s->allocation_guard) {
    rc = ps_internal_capture_begin(s, &guard, error);
    p = rc == LC_OK ? validation_value(s, key, expected) : NULL;
  }
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
    rc = verify_value_payloads(s, p, buffer, NULL, error);
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
    collect_retired(s);
  pthread_mutex_unlock(&s->mutex);
  lc_free_with_allocator(NULL, buffer);
  return rc;
}
static int value_range_order(const void *left, const void *right) {
  uint64_t a = ((const ps_value_range *)left)->offset;
  uint64_t b = ((const ps_value_range *)right)->offset;
  return a < b ? -1 : a != b;
}
/* Sorted ranges belong to distinct retained entries. Merge only adjacent
 * bytes whose entries are making the same pin transition in this capture. */
static int values_witness_range(const ps_values *view, size_t *at, size_t limit,
                                uint64_t *offset, uint64_t *bytes) {
  const ps_value_range *range;
  *bytes = 0;
  while (*at < limit) {
    range = &view->ranges[*at];
    if (range->item->witness) {
      if (*bytes && *offset + *bytes != range->offset)
        break;
      if (!*bytes)
        *offset = range->offset;
      *bytes += range->bytes;
    }
    ++*at;
  }
  return *bytes != 0;
}
static void values_free(ps_values *view) {
  if (view->ranges != &view->one_range)
    lc_free_with_allocator(NULL, view->ranges);
  lc_free_with_allocator(NULL, view);
}
static void values_unwitness(ps_values *view, size_t limit) {
  size_t at = 0;
  uint64_t offset, bytes;
  while (values_witness_range(view, &at, limit, &offset, &bytes))
    if (reader_release_range(view->space, offset, bytes, NULL) != LC_OK)
      break;
}
static int values_witness(ps_values *view, int provisional, lc_error *error) {
  ps_space *s = view->space;
  ps_value_slot *item;
  ps_entry *p;
  ps_value_range *range;
  size_t i, j, count = 0, at = 0, done = 0;
  uint64_t offset, bytes;
  int rc;
  for (i = 0; i < view->retained_count; ++i) {
    item = view->retained[i];
    p = item->retained;
    if (p->count > (size_t)-1 / sizeof(*range) - count)
      return err(error, LC_ERR_NOMEM, "captured ranges exceed local limit");
    count += p->count;
    item->witness = p->count && !p->pins && !p->payload_claimed;
  }
  if (!count)
    return LC_OK;
  view->ranges = count == 1
                     ? &view->one_range
                     : lc_alloc_with_allocator(NULL, count * sizeof(*range));
  if (!view->ranges)
    return err(error, LC_ERR_NOMEM, "cannot allocate captured value ranges");
  view->range_count = count;
  range = view->ranges;
  /* Keep the extents that already have a witness as well. Other readers
   * may leave before this view and hand it their last pin. */
  for (i = 0; i < view->retained_count; ++i) {
    item = view->retained[i];
    p = item->retained;
    for (j = 0; j < p->count; ++j, ++range) {
      range->offset = p->extents[j].offset;
      range->bytes = extent_allocation(&p->extents[j]);
      range->item = item;
    }
  }
  if (count > 1)
    qsort(view->ranges, count, sizeof(*range), value_range_order);
  while (values_witness_range(view, &at, count, &offset, &bytes)) {
    rc = provisional ? take_physical_witness(s, offset, bytes, error)
                     : physical_claim(s, offset, bytes, error);
    if (rc != LC_OK) {
      values_unwitness(view, done);
      if (__atomic_load_n(&s->poisoned, __ATOMIC_RELAXED)) {
        lc_error_cleanup(error);
        return healthy(s, error);
      }
      return rc;
    }
    s->reader_claim_bytes += bytes;
    done = at;
  }
  return LC_OK;
}
void ps_internal_values_discard_locked(ps_values *view) {
  ps_value_slot *item;
  ps_entry *p;
  size_t i;
  if (!view)
    return;
  --view->space->readers;
  for (i = 0; i < view->retained_count; ++i) {
    item = view->retained[i];
    p = item->retained;
    item->witness = !--p->pins && p->reader_claimed;
  }
  values_unwitness(view, view->range_count);
  for (i = 0; i < view->retained_count; ++i) {
    item = view->retained[i];
    if (item->witness)
      item->retained->reader_claimed = 0;
  }
  values_free(view);
}
/* Occupancy bounds a selection even when a tombstone picks a predecessor or
 * two owners share a bucket. The caller holds the local allocator mutex. */
static int values_capacity(const ps_space *s, size_t bucket, unsigned int begin,
                           unsigned int end, size_t limit, size_t *count,
                           lc_error *error) {
  unsigned int group;
  *count = 0;
  for (group = begin; group <= end; ++group) {
    size_t occupied = s->buckets->values[group][bucket].count;
    if (occupied > limit - *count)
      return err(error, LC_ERR_NOMEM, "selected value metadata exceeds limit");
    *count += occupied;
  }
  return LC_OK;
}
static int values_select_locked(ps_space *s, uint64_t owner, unsigned int first,
                                unsigned int last, uint64_t prefix,
                                ps_value_retain_fn retain, void *context,
                                ps_values **out, lc_error *error) {
  ps_values *view;
  ps_entry *p, *selected;
  size_t count, bucket = owner % BUCKETS;
  size_t slot_bytes = sizeof(*view->items) + sizeof(*view->retained);
  size_t limit = (SIZE_MAX - sizeof(*view)) / slot_bytes;
  unsigned int group, begin = value_group(first), end;
  int rc;
  *out = NULL;
  if (last > PS_NAMESPACE_CHECKPOINT)
    last = PS_NAMESPACE_CHECKPOINT;
  end = value_group(last);
  rc = values_capacity(s, bucket, begin, end, limit, &count, error);
  if (rc != LC_OK)
    return rc;
  view = lc_alloc_with_allocator(NULL, sizeof(*view) + count * slot_bytes);
  if (!view)
    return err(error, LC_ERR_NOMEM, "cannot allocate selected value capture");
  memset(view, 0, sizeof(*view));
  view->space = s;
  view->owner = owner;
  view->items = (ps_value_slot *)(view + 1);
  view->retained = (ps_value_slot **)(view->items + count);
  for (group = begin; group <= end; ++group)
    for (p = s->buckets->values[group][bucket].head; p; p = p->value_next) {
      if (p->key.owner != owner ||
          (!group && (p->key.kind < first || p->key.kind > last)))
        continue;
      selected = selected_value(s, p, prefix);
      if (selected) {
        ps_value_slot *item = &view->items[view->count++];
        item->key = selected->key.key;
        item->kind = selected->key.kind;
        item->token = selected->token;
        item->bytes = selected->bytes;
        if (!retain || retain(context, selected->key, item->token)) {
          item->retained = selected;
          item->witness = 0;
          view->retained[view->retained_count++] = item;
        } else
          item->retained = NULL;
      }
    }
  *out = view;
  return LC_OK;
}
int ps_internal_values_capture_locked(ps_space *s, uint64_t owner,
                                      unsigned int first, unsigned int last,
                                      uint64_t prefix, int provisional,
                                      ps_value_retain_fn retain, void *context,
                                      ps_values **out, lc_error *error) {
  ps_values *view;
  ps_value_slot *item;
  size_t i;
  int rc = values_select_locked(s, owner, first, last, prefix, retain, context,
                                &view, error);
  *out = NULL;
  if (rc != LC_OK)
    return rc;
  if (ps_coordination_shared(s->coordination))
    rc = values_witness(view, provisional, error);
  if (rc == LC_OK) {
    for (i = 0; i < view->retained_count; ++i) {
      item = view->retained[i];
      if (item->witness)
        item->retained->reader_claimed = 1;
      ++item->retained->pins;
    }
    ++s->readers;
    *out = view;
  } else
    values_free(view);
  return rc;
}
int ps_internal_values_capture(ps_space *s, uint64_t owner, unsigned int first,
                               unsigned int last, ps_value_retain_fn retain,
                               void *context, ps_values **out,
                               lc_error *error) {
  ps_coordination_guard *guard;
  uint64_t prefix;
  int rc, provisional;
  if (out)
    *out = NULL;
  if (!s || !out || first > last)
    return err(error, LC_ERR_INVALID, "value visit requires owner and kinds");
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_observe_begin(s, &guard, error);
  provisional = s->readiness && !s->allocation_guard;
  if (rc == LC_OK) {
    prefix = __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE);
    rc = ps_internal_values_capture_locked(s, owner, first, last, prefix,
                                           provisional, retain, context, out,
                                           error);
  }
  if (rc == LC_OK && provisional && !ps_internal_read_witness_valid(s)) {
    ps_internal_values_discard_locked(*out);
    *out = NULL;
    rc = ps_internal_capture_begin(s, &guard, error);
    if (rc == LC_OK) {
      prefix = __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE);
      rc = ps_internal_values_capture_locked(s, owner, first, last, prefix, 0,
                                             retain, context, out, error);
    }
  }
  rc = ps_internal_capture_end(s, guard, rc, error);
  if (rc != LC_OK && *out) {
    ps_internal_values_discard_locked(*out);
    *out = NULL;
  }
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_values_capture(ps_space *s, uint64_t owner, unsigned int first,
                      unsigned int last, ps_values **out, lc_error *error) {
  return ps_internal_values_capture(s, owner, first, last, NULL, NULL, out,
                                    error);
}
size_t ps_values_count(const ps_values *view) { return view ? view->count : 0; }
int ps_values_info(const ps_values *view, size_t at, ps_key *key, ps_ref *ref,
                   uint64_t *bytes, lc_error *error) {
  const ps_value_slot *p;
  if (!view || at >= view->count || !key || !ref || !bytes)
    return err(error, LC_ERR_INVALID,
               "captured value requires index and output");
  p = &view->items[at];
  key->owner = view->owner;
  key->key = p->key;
  key->kind = p->kind;
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
  if (!view->items[at].retained)
    return err(error, LC_ERR_INVALID, "capture has no retained payload");
  reader = reader_new(view->space, view->items[at].retained);
  if (!reader)
    return err(error, LC_ERR_NOMEM, "cannot allocate captured value reader");
  s = view->space;
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc == LC_OK) {
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
typedef struct value_walk {
  ps_reader reader;
  packed_read packed;
  unsigned char buffer[PS_CHUNK];
} value_walk;
static int value_position(const void *left, const void *right) {
  const ps_entry *a = *(ps_entry *const *)left;
  const ps_entry *b = *(ps_entry *const *)right;
  uint64_t x = a->count ? a->extents[0].offset : 0;
  uint64_t y = b->count ? b->extents[0].offset : 0;
  return x < y ? -1 : x != y;
}
static int values_read_walk(ps_space *s, value_walk *walk, size_t count,
                            int *private_failure, ps_value_read_fn read,
                            void *context, lc_error *error) {
  ps_reader *r = &walk->reader;
  ps_entry **order = (void *)(walk + 1);
  size_t i;
  int rc = LC_OK;
  if (count > 1)
    qsort(order, count, sizeof(*order), value_position);
  walk->packed.offset = 0;
  for (i = 0; rc == LC_OK && i < count; ++i) {
    rc = io_healthy(private_failure ? private_failure : &s->poisoned, error);
    if (rc != LC_OK)
      break;
    memset(r, 0, sizeof(*r));
    r->space = s;
    r->private_failure = private_failure;
    r->entry = order[i];
    r->packed = &walk->packed;
    r->buffer = walk->buffer;
    rc = read(context, r->entry->key, r, error);
  }
  return rc;
}
int ps_values_read_walk(const ps_values *view, ps_value_read_fn read,
                        void *context, lc_error *error) {
  value_walk *walk;
  ps_entry **order;
  size_t i, count;
  int rc;
  if (!view || !read)
    return err(error, LC_ERR_INVALID, "captured read walk requires visitor");
  count = view->retained_count;
  if (!count)
    return LC_OK;
  if (count > (SIZE_MAX - sizeof(*walk)) / sizeof(*order))
    return err(error, LC_ERR_NOMEM, "captured read order exceeds local limit");
  walk = lc_alloc_with_allocator(NULL, sizeof(*walk) + count * sizeof(*order));
  if (!walk)
    return err(error, LC_ERR_NOMEM, "cannot allocate captured read order");
  order = (void *)(walk + 1);
  for (i = 0; i < count; ++i)
    order[i] = view->retained[i]->retained;
  rc = values_read_walk(view->space, walk, count, NULL, read, context, error);
  lc_free_with_allocator(NULL, walk);
  return rc;
}
int ps_internal_values_startup_walk(ps_space *s, uint64_t owner,
                                    ps_value_read_fn read, void *context,
                                    int *accepted, lc_error *error) {
  value_walk *walk = NULL;
  ps_entry **order, *p, *selected;
  size_t capacity, count = 0, bucket = owner % BUCKETS;
  size_t limit = (SIZE_MAX - sizeof(*walk)) / sizeof(*order);
  unsigned int group, begin = value_group(PS_KEY_METADATA),
                      end = value_group(PS_NAMESPACE_CHECKPOINT);
  uint64_t prefix;
  int rc, private_failure = 0;
  *accepted = 0;
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc != LC_OK || !ps_readiness_resident(s))
    goto done;
  observe_local_durability(s);
  prefix = __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE);
  rc = values_capacity(s, bucket, begin, end, limit, &capacity, error);
  if (rc != LC_OK)
    goto done;
  if (capacity) {
    walk = lc_alloc_with_allocator(NULL,
                                   sizeof(*walk) + capacity * sizeof(*order));
    if (!walk) {
      rc = err(error, LC_ERR_NOMEM, "cannot allocate startup read order");
      goto done;
    }
    order = (void *)(walk + 1);
    for (group = begin; group <= end; ++group)
      for (p = s->buckets->values[group][bucket].head; p; p = p->value_next)
        if (p->key.owner == owner &&
            (selected = selected_value(s, p, prefix)) != NULL)
          order[count++] = selected;
    rc = values_read_walk(s, walk, count, &private_failure, read, context,
                          error);
  }
  /* The mutex preserves resident descriptors. A peer can retire their
   * bytes only across a physical checkpoint. Accept the private metadata
   * or its read error only if that transition has not begun. */
  if (ps_internal_read_witness_valid(s)) {
    *accepted = 1;
    if (private_failure)
      __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
  } else {
    lc_error_cleanup(error);
    rc = LC_OK;
  }
done:
  lc_free_with_allocator(NULL, walk);
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
void ps_values_close(ps_values *view) {
  ps_space *s;
  if (!view)
    return;
  s = view->space;
  pthread_mutex_lock(&s->mutex);
  ps_internal_values_discard_locked(view);
  collect_retired(s);
  pthread_mutex_unlock(&s->mutex);
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
    ps_internal_entry_free(r->entry);
    destroy(s);
    lc_free_with_allocator(NULL, r);
    return;
  }
  pthread_mutex_lock(&s->mutex);
  ps_internal_entry_unpin(s, r->entry);
  --s->readers;
  if (r->entry->state == 2 && r->entry->safe && !r->entry->pins)
    collect_retired(s);
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
  uLong crc = ps_crc32(0, wire, 96);
  crc = ps_crc32(crc, zero, sizeof(zero));
  return (uint32_t)ps_crc32(crc, wire + 100, (uInt)(bytes - 100));
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
  ps_wire_put64(wire, TICKET_MAGIC);
  ps_wire_put64(wire + 8, (uint64_t)st.st_dev);
  ps_wire_put64(wire + 16, (uint64_t)st.st_ino);
  ps_wire_put64(wire + 24, capacity);
  memcpy(wire + 32, s->identity, 16);
  ps_wire_put64(wire + 48, p->token);
  ps_wire_put64(wire + 56, p->bytes);
  ps_wire_put64(wire + 64, p->count);
  ps_wire_put32(wire + 72, p->key.kind);
  ps_wire_put64(wire + 80, p->key.owner);
  ps_wire_put64(wire + 88, p->key.key);
  for (i = 0, at = TICKET_HEADER; i < p->count; ++i, at += 24) {
    ps_wire_put64(wire + at, p->extents[i].offset);
    ps_wire_put64(wire + at + 8, p->extents[i].bytes);
    ps_wire_put32(wire + at + 16, p->extents[i].crc);
    ps_wire_put32(wire + at + 20, p->extents[i].flags);
  }
  ps_wire_put32(wire + 96, ticket_checksum(wire, bytes));
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
      bytes > TICKET_HEADER + PS_JOURNAL ||
      ps_wire_get64(wire) != TICKET_MAGIC ||
      ps_wire_get32(wire + 96) != ticket_checksum(wire, bytes))
    return err(error, LC_ERR_INVALID, "invalid captured reader ticket");
  count = ps_wire_get64(wire + 64);
  if (count > (PS_JOURNAL - 64U) / 24U ||
      bytes != TICKET_HEADER + count * 24U || !ps_wire_get64(wire + 48) ||
      ps_wire_get64(wire + 24) < PS_MIB ||
      ps_wire_get64(wire + 24) > PS_MAX_CAPACITY ||
      ps_wire_get64(wire + 24) % PS_MIB || ps_wire_get32(wire + 76) ||
      ps_wire_get32(wire + 100) || ps_wire_get64(wire + 104))
    return err(error, LC_ERR_INVALID, "invalid captured reader metadata");
  s = lc_calloc_with_allocator(NULL, 1, sizeof(*s));
  if (s)
    s->fd = -1;
  p = ps_internal_entry_new(0, 0);
  if (!s || !p) {
    rc = err(error, LC_ERR_NOMEM, "cannot allocate imported captured reader");
    goto done;
  }
  s->io.read = native_read;
  s->capacity = ps_wire_get64(wire + 24);
  s->range_pins = 1;
  memcpy(s->identity, wire + 32, 16);
  p->key.owner = ps_wire_get64(wire + 80);
  p->key.key = ps_wire_get64(wire + 88);
  p->key.kind = ps_wire_get32(wire + 72);
  p->token = ps_wire_get64(wire + 48);
  p->bytes = ps_wire_get64(wire + 56);
  if (!ps_internal_immutable_key(p->key)) {
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
  p->count = (size_t)count;
  for (i = 0, at = TICKET_HEADER; i < p->count; ++i, at += 24) {
    ps_extent *x = &p->extents[i];
    x->offset = ps_wire_get64(wire + at);
    x->bytes = ps_wire_get64(wire + at + 8);
    x->crc = ps_wire_get32(wire + at + 16);
    x->flags = ps_wire_get32(wire + at + 20);
    if (!x->bytes || x->bytes > PS_CHUNK || x->offset < PS_DATA ||
        x->offset > s->capacity || x->bytes > s->capacity - x->offset ||
        (x->flags != 0 && x->flags != EXTENT_PACKED) ||
        (x->flags
             ? x->offset % PS_CELL || x->bytes > PS_PAGE - x->offset % PS_PAGE
             : x->offset % PS_PAGE) ||
        total > LC_U64_MAX - x->bytes) {
      rc = err(error, LC_ERR_INVALID, "captured ticket extent exceeds bounds");
      goto done;
    }
    total += x->bytes;
  }
  if (total != p->bytes) {
    rc = err(error, LC_ERR_INVALID, "captured ticket byte count mismatch");
    goto done;
  }
  r = reader_new(s, p);
  if (!r) {
    rc = err(error, LC_ERR_NOMEM, "cannot allocate imported captured reader");
    goto done;
  }
  s->fd = open(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
  if (s->fd < 0 || fstat(s->fd, &st) != 0) {
    rc = err(error, LC_ERR_TRANSPORT, "cannot open captured reader file");
    goto done;
  }
  if (!S_ISREG(st.st_mode) || st.st_size < (off_t)s->capacity ||
      (uint64_t)st.st_dev != ps_wire_get64(wire + 8) ||
      (uint64_t)st.st_ino != ps_wire_get64(wire + 16)) {
    rc = err(error, LC_ERR_INVALID, "captured ticket belongs to another file");
    goto done;
  }
  rc = import_pin(s->fd, PIN_PRESENCE_OFFSET, 1, error);
  for (i = 0; rc == LC_OK && i < p->count; ++i)
    rc = import_pin(s->fd, p->extents[i].offset, p->extents[i].bytes, error);
  if (rc == LC_OK) {
    r->imported = 1;
    *out = r;
    return LC_OK;
  }
done:
  if (s)
    destroy(s);
  if (p)
    ps_internal_entry_free(p);
  lc_free_with_allocator(NULL, r);
  return rc;
}
static int delete_value(ps_space *s, ps_key key, const ps_ref *expected,
                        int missing_ok, lc_error *error) {
  ps_entry *p, *tombstone;
  ps_coordination_guard *guard;
  int rc, matched;
  uint64_t sequence = 0;
  if (!s || !ps_internal_immutable_key(key))
    return err(error, LC_ERR_INVALID, "container delete requires typed key");
  tombstone = ps_internal_entry_new(key.kind, 0);
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
    install(s, tombstone, p);
    sequence = tombstone->publication_sequence;
  } else
    ps_internal_entry_free(tombstone);
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
  return sequence ? wait_durable(s, sequence, error)
                  : settle_recovery(s, error);
}
static int checkpoint(ps_space *s, int cleanup, lc_error *error) {
  ps_coordination_guard *guard;
  uint64_t bound;
  int rc;
  if (!s)
    return err(error, LC_ERR_INVALID, "checkpoint requires container");
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc == LC_OK && cleanup && s->readiness && !ps_readiness_resident(s)) {
    /* A peer may have published canonical work since the cleanup scan. */
    rc = ps_internal_capture_begin(s, &guard, error);
    rc = ps_internal_capture_end(s, guard, rc, error);
  }
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
    /* Shared namespace visits can append ownership stamps without changing
     * the directory. Each stamp-only checkpoint advances the root generation
     * and invalidates those stamps. Canonical changes mark entries dirty,
     * including stream tails and admission metadata. */
    if (s->dirty || metadata_trim_needed(s) ||
        ((!cleanup || !s->readiness) &&
         (s->journal ||
          __atomic_load_n(&s->pending_sequence, __ATOMIC_ACQUIRE) !=
              __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE))))
      rc = checkpoint_locked(s, error);
    else {
      rc = pins_reclaim_current(s, error);
      if (rc == LC_OK)
        collect_retired(s);
    }
  }
  if (rc == LC_OK)
    rc = healthy(s, error);
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_checkpoint(ps_space *s, lc_error *error) {
  return checkpoint(s, 0, error);
}
int ps_checkpoint_cleanup(ps_space *s, lc_error *error) {
  return checkpoint(s, 1, error);
}
int ps_metadata_repack(ps_space *s, lc_error *error) {
  int rc;
  if (!s)
    return err(error, LC_ERR_INVALID, "metadata repack requires container");
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc == LC_OK)
    rc = checkpoint_select(s, 1, error);
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
  if (rc == LC_OK && (bytes > PS_MAX_CAPACITY - capacity ||
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
int ps_maintenance_pressure_requested(ps_space *s) {
  return ps_readiness_pressure(s);
}
static int sample_headroom(ps_space *s, uint64_t *available, lc_error *e) {
  uint64_t bound, floor = PS_MAINTENANCE_RESERVE;
  int rc = metadata_claimed_bound(s, s->metadata, s->live_entries, &bound, e);
  if (rc == LC_OK)
    rc = reserve_floor(s, s->metadata, s->live_entries, bound, &floor, e);
  if (rc == LC_OK)
    (void)ordinary_shortfall(s, metadata_target(s, bound), 0, floor, available);
  return rc;
}
int ps_maintenance_pressure(ps_space *s, int draining, int *needed,
                            uint64_t *available, lc_error *error) {
  ps_coordination_guard *guard = NULL;
  uint64_t quarter, amount;
  int rc, attempted = 0, reclaimed;
  if (!s || !needed || !available)
    return err(error, LC_ERR_INVALID, "capacity pressure requires outputs");
  *needed = ps_readiness_pressure(s);
  *available = 0;
  if (!*needed && !draining)
    return LC_OK;
  rc = pthread_mutex_trylock(&s->mutex);
  if (rc)
    return err(error, rc == EBUSY ? LC_ERR_PENDING : LC_ERR_TRANSPORT,
               "capacity pressure cannot acquire allocator");
  rc = healthy(s, error);
  if (rc == LC_OK)
    rc = allocation_lock(s, &guard, 0, error);
  if (rc == LC_OK && guard)
    rc = canonical_refresh(s, NULL, 0, error);
  if (rc == LC_OK && guard)
    rc = allocation_ranges_refresh(s, error);
  if (rc == LC_OK)
    rc = sample_headroom(s, available, error);
  if (rc == LC_OK)
    *needed = ps_readiness_pressure(s);
  quarter = (s->capacity - PS_DATA) / 4;
  if (rc == LC_OK && *needed && *available < quarter) {
    /* At low headroom, commit selected retirements before the next logical
     * maintenance pass. Require at least one stream chunk, or an eighth of
     * remaining headroom, to amortize the root selection barriers. */
    amount = *available / 8;
    reclaimed = reclaim_capacity(s, amount > PS_CHUNK ? amount : PS_CHUNK,
                                 &attempted, error);
    if (attempted)
      rc =
          reclaimed == LC_OK ? sample_headroom(s, available, error) : reclaimed;
  }
  if (rc == LC_OK && *needed && *available >= s->capacity - PS_DATA - quarter) {
    ps_readiness_set_pressure(s, 0);
    *needed = 0;
  }
  rc = allocation_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  return rc;
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
  out->reserve_owners = s->reserve_count + s->selected_reserve_count;
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
  out->capacity = head->entry->stream.head->capacity;
  out->runs = head->entry->stream.head->runs;
  out->stored_bytes = head->entry->stream.head->storage;
  out->edit_wire = head->entry->stream.head->edit_wire;
  out->sealed = head->entry->stream.head->sealed;
  out->retired = head->entry->deleted;
  out->complete =
      head->entry->stream.head->complete_generation &&
      head->entry->stream.head->complete_generation == head->space->generation;
}
/* Existing run handles already own immutable mappings. New admissions fence
 * a seal or stream retirement until it is durable. */
static int stream_structure_ready(ps_space *s, const ps_entry *head,
                                  lc_error *error) {
  return head && head->stream.head->structure_wire > s->peer_durable
             ? err(error, LC_ERR_PENDING, "stream geometry awaits durability")
             : LC_OK;
}
static int owned_capture_begin(ps_owned_head *head,
                               ps_coordination_guard **guard, lc_error *e) {
  int rc = ps_internal_capture_begin(head->space, guard, e);
  return rc == LC_OK ? stream_structure_ready(head->space, head->entry, e) : rc;
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
                                        &p->stream.head->claim, e);
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
      memcmp(s->identity, ref->container, 16) ||
      stream_structure_ready(s, head.entry, NULL) != LC_OK)
    return 0;
  owned_status(&head, &status);
  if (status.retired || status.sealed ||
      (!status.complete && (status.capacity || status.tail)))
    return 0;
  *tail = status.tail;
  return 1;
}
int ps_internal_admitted_tail(ps_space *s, ps_key key, const ps_ref *ref,
                              uint64_t *tail) {
  ps_entry *p = lookup(s, key);
  if (!p || p->deleted || p->stream.head->sealed || !p->stream.head->admitted ||
      p->token != ref->incarnation || memcmp(s->identity, ref->container, 16) ||
      stream_structure_ready(s, p, NULL) != LC_OK)
    return 0;
  *tail = p->bytes;
  return 1;
}
int ps_internal_certify_tail(ps_space *s, ps_key key, const ps_ref *ref,
                             uint64_t tail, lc_error *e) {
  ps_coordination_guard *guard;
  ps_entry *p;
  int rc;
  if (!s->readiness)
    return LC_OK;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &guard, e);
  p = lookup(s, key);
  if (rc == LC_OK && p && !p->deleted && !p->stream.head->sealed &&
      !p->stream.head->admitted && p->bytes == tail &&
      p->token == ref->incarnation &&
      !memcmp(s->identity, ref->container, 16)) {
    ps_entry hint = {0};
    hint.key = key;
    hint.token = p->token;
    hint.bytes = tail;
    /* A proof is optional. Leave rollover to a canonical publisher. */
    if (event_bytes(&hint, PS_STREAM_ADMITTED) <= PS_JOURNAL - s->journal) {
      rc = append_event_locked(s, &hint, PS_STREAM_ADMITTED, e);
      if (rc == LC_OK)
        stream_admitted(s, p, 1);
    }
  }
  rc = ps_internal_capture_end(s, guard, rc, e);
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
static int owned_open(ps_space *s, ps_budget *budget, ps_key key, int create,
                      const ps_ref *ref, int guarded, int prepare, int defer,
                      ps_owned_head **out, ps_owned_status *status,
                      ps_owned_initial *initial, lc_error *error) {
  ps_owned_head *head;
  ps_coordination_guard *guard;
  ps_entry *p = NULL, *old;
  uint64_t sequence = 0;
  int rc, continued;
  if (out)
    *out = NULL;
  if (!s || !out || !status || key.kind != PS_STREAM || (create && ref))
    return err(error, LC_ERR_INVALID, "owned stream open arguments invalid");
  guarded = guarded && s->readiness;
  head = lc_calloc_with_allocator(NULL, 1, sizeof(*head));
  if (!head)
    return err(error, LC_ERR_NOMEM, "cannot allocate owned stream handle");
  if (create) {
    p = ps_internal_entry_new(PS_STREAM, 0);
    if (!p) {
      lc_free_with_allocator(NULL, head);
      return err(error, LC_ERR_NOMEM, "cannot allocate owned stream head");
    }
    p->key = key;
  }
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &guard, error);
  old = lookup(s, key);
  if (rc == LC_OK)
    rc = stream_structure_ready(s, old, error);
  if (rc == LC_OK && create && old &&
      (!old->deleted ||
       old->publication_sequence >
           __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE)))
    rc = err(error, LC_ERR_INVALID, "owned stream already exists");
  if (rc == LC_OK && create)
    rc = ps_internal_token_range(s, error);
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
      install(s, p, old);
    }
  }
  if (!create)
    p = lookup(s, key);
  if (rc == LC_OK && (!p || p->deleted ||
                      (p->stream.head->handles &&
                       (!guarded || !p->stream.head->guarded ||
                        (prepare && !p->stream.head->catalog_refs)))))
    rc = err(error, LC_ERR_INVALID,
             "owned stream missing or already has an open owner");
  if (rc == LC_OK && ref &&
      (memcmp(ref->container, s->identity, 16) || ref->incarnation != p->token))
    rc = err(error, LC_ERR_INVALID, "stale or foreign owned stream reference");
  if (rc == LC_OK && prepare && !p->stream.head->catalog_refs &&
      ps_catalog_target_retained(s, p))
    rc = err(error, LC_ERR_PENDING, "prior stream binding still retained");
  if (rc == LC_OK && prepare && !p->stream.head->catalog_refs &&
      !p->stream.head->claim)
    rc = ps_internal_stream_claim(s, p->token, &p->stream.head->claim, error);
  if (rc == LC_OK) {
    head->space = s;
    head->entry = p;
    p->stream.head->guarded = guarded;
    ++p->stream.head->handles;
    ++p->pins; /* Stream heads own no payload extents. */
    ++s->readers;
    owned_status(head, status);
    if (initial && !status->sealed)
      rc = claim_writer_stamp(s, p, PS_STREAM_OWNER, &continued, error);
    if (rc == LC_OK && initial)
      rc = pin_runs_locked(s, p, key, ref, 0, 0, 0, NULL, &initial->runs,
                           &initial->count, error);
  } else if (create && p->state == 0) {
    ps_coordination_unlock(p->stream.head->claim);
    ps_internal_entry_free(p);
  }
  rc = ps_internal_capture_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK && create && s->durable && !defer)
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
  return owned_open(s, budget, key, create, ref, 0, 0, 0, out, status, NULL,
                    error);
}
int ps_owned_open_guarded(ps_space *s, ps_budget *budget, ps_key key,
                          int create, const ps_ref *ref, ps_owned_head **out,
                          ps_owned_status *status, lc_error *error) {
  return owned_open(s, budget, key, create, ref, 1, 0, 0, out, status, NULL,
                    error);
}
int ps_owned_stage_empty(ps_space *s, ps_budget *budget, ps_key key,
                         ps_owned_head **out, ps_ref *ref, lc_error *error) {
  ps_owned_status status;
  int rc;
  if (out)
    *out = NULL;
  if (!ref)
    return err(error, LC_ERR_INVALID, "staged stream requires a reference");
  rc = owned_open(s, budget, key, 1, NULL, 1, 0, 1, out, &status, NULL, error);
  if (rc == LC_OK)
    *ref = status.ref;
  return rc;
}
int ps_owned_open_prepared(ps_space *s, ps_key key, ps_owned_head **out,
                           ps_owned_status *status, lc_error *error) {
  int shared = ps_coordination_shared(ps_space_coordination(s));
  return owned_open(s, NULL, key, 0, NULL, shared, 1, 0, out, status, NULL,
                    error);
}
void ps_owned_close(ps_owned_head *head) {
  ps_space *s;
  if (!head)
    return;
  s = head->space;
  pthread_mutex_lock(&s->mutex);
  if (!--head->entry->stream.head->handles) {
    head->entry->stream.head->orphan_claimed = 0;
    ps_coordination_unlock(head->entry->stream.head->claim);
    head->entry->stream.head->claim = NULL;
  }
  ps_internal_entry_unpin(s, head->entry);
  --s->readers;
  if (head->entry->state == 2 && head->entry->safe && !head->entry->pins)
    collect_retired(s);
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
    rc = stream_structure_ready(head->space, head->entry, error);
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
  rc = owned_capture_begin(head, &guard, error);
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
static int owned_edit_end(ps_space *s, ps_coordination_guard *guard, int rc,
                          lc_error *error) {
  if (guard) {
    ps_coordination_unlock(guard);
    if (__atomic_load_n(&s->poisoned, __ATOMIC_RELAXED)) {
      lc_error_cleanup(error);
      return healthy(s, error);
    }
  }
  return rc;
}
int ps_owned_edit_end(ps_owned_head *head, ps_coordination_guard *guard, int rc,
                      lc_error *error) {
  return guard ? owned_edit_end(head->space, guard, rc, error) : rc;
}
static int writer_stamp_current(const ps_space *s, const ps_entry *p) {
  const ps_writer_stamp *stamp = entry_writer_stamp(p);
  return s->writer_id && stamp->generation == s->generation &&
         stamp->token == s->writer_id;
}
static int claim_writer_stamp(ps_space *s, ps_entry *p, int event,
                              int *continued, lc_error *error) {
  ps_entry stamp;
  int rc;
  *continued = writer_stamp_current(s, p);
  if (*continued && event != PS_STREAM_EDIT)
    return LC_OK;
  memset(&stamp, 0, sizeof(stamp));
  stamp.key = p->key;
  stamp.token = p->token;
  /* The first framed stamp supplies an identity in this live cohort.
   * Recovered hints at or before the cohort origin cannot claim ownership. */
  stamp.bytes = s->writer_id ? s->writer_id : s->sequence + 1;
  rc = append_event_locked(s, &stamp, event, error);
  if (rc == LC_OK) {
    ps_writer_stamp *writer = entry_writer_stamp(p);
    s->writer_id = stamp.bytes;
    writer->token = s->writer_id;
    writer->generation = s->generation;
    if (event == PS_STREAM_EDIT) {
      p->stream.head->edit_wire = s->sequence;
      p->stream.head->complete_generation = 0;
      stream_admitted(s, p, 0);
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
  p = lookup(s, key);
  /* The caller owns the namespace guard. A peer cannot replace this
   * stamp while that guard is held. A new stamp still needs the allocator. */
  if (p && !p->deleted && p->catalog &&
      p->catalog->kind == PS_CATALOG_NAMESPACE &&
      p->peer_sequence <= s->peer_durable && writer_stamp_current(s, p) &&
      ps_readiness_resident(s)) {
    *continued = 1;
    rc = healthy(s, error);
    pthread_mutex_unlock(&s->mutex);
    return rc;
  }
  rc = ps_internal_capture_begin(s, &allocator, error);
  if (rc == LC_OK) {
    p = lookup(s, key);
    if (p && p->peer_sequence > s->peer_durable)
      rc = err(error, LC_ERR_PENDING, "namespace ownership awaits durability");
    else if (!p || p->deleted || !p->catalog ||
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
  if (s->readiness) {
    rc = ps_coordination_lock(s->coordination, PS_COORDINATION_STREAM, p->token,
                              0, edit, error);
    if (rc != LC_OK)
      return rc;
  } else if (!append || !s->open_grant_covered ||
             !__atomic_load_n(&s->deferred_recovery, __ATOMIC_ACQUIRE))
    return ps_owned_status_get(head, status, error);
  pthread_mutex_lock(&s->mutex);
  rc = owned_capture_begin(head, &allocator, error);
  if (rc == LC_OK) {
    owned_status(head, status);
    if (!p->deleted && !p->stream.head->sealed)
      rc = s->readiness
               ? claim_writer_stamp(s, p,
                                    append ? PS_STREAM_EDIT : PS_STREAM_OWNER,
                                    continued, error)
               : prepare_recovery_write(s, error);
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
  rc = owned_capture_begin(head, &guard, error);
  if (rc == LC_OK && (head->entry->deleted || head->entry->stream.head->sealed))
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
  if (rc == LC_OK)
    rc = stream_structure_ready(space, head.entry, error);
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
                           int *attempted, lc_error *error) {
  uint64_t missing = 0;
  int rc = capacity_allowance(
      s, s->metadata + PS_OWNED_DESCRIPTOR_BYTES, s->live_entries + 1, payload,
      PS_MAINTENANCE_RESERVE, budget,
      attempted && !*attempted ? &missing : NULL, error);
  if (rc == LC_ERR_INVALID && missing) {
    rc = reclaim_capacity(s, missing, attempted, error);
    if (rc == LC_OK)
      rc = checkpoint_allowance_floor(
          s, s->metadata + PS_OWNED_DESCRIPTOR_BYTES, s->live_entries + 1,
          payload, PS_MAINTENANCE_RESERVE, budget, error);
  }
  return rc;
}
static int owned_preparation_allowance(ps_space *s, ps_budget *budget,
                                       uint64_t bytes, uint64_t reserved,
                                       lc_error *error) {
  if (!budget)
    return owned_allowance(s, NULL, reserved, NULL, error);
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
  ps_entry *p = ps_internal_entry_new(PS_OWNED_RUN, 1);
  unsigned char *zero = lc_calloc_with_allocator(NULL, 1, PS_CHUNK);
  unsigned char header[PS_RUN_HEADER];
  uint64_t bytes = desired, reserved = 0, sequence = 0;
  uint64_t prepared, partial_bytes, at, selected = 0, partial_at, free_before;
  uint64_t generation;
  ps_run **choice = NULL, **link, **partial, **fit;
  ps_run *spare = NULL;
  ps_coordination_guard *guard = NULL;
  int rc, clean = 0, registered = 0, reclaimed = 0, checkpointed = 0;
  if (!p || !zero) {
    if (p)
      ps_internal_entry_free(p);
    lc_free_with_allocator(NULL, zero);
    return err(error, LC_ERR_NOMEM, "cannot allocate owned run preparation");
  }
  p->key.kind = PS_OWNED_RUN;
  p->key.owner = head->entry->token;
  p->key.key = base;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_admission_begin(s, &guard, error);
  if (rc == LC_OK)
    rc = stream_structure_ready(s, head->entry, error);
  if (rc == LC_OK && (head->entry->deleted || head->entry->stream.head->sealed))
    rc = err(error, LC_ERR_INVALID, "owned stream cannot accept grants");
  if (rc == LC_OK && (base != head->entry->stream.head->capacity ||
                      base > (uint64_t)LC_I64_MAX - (desired - PS_RUN_HEADER)))
    rc = err(error, LC_ERR_INVALID, "owned grant base or incarnation invalid");
  if (rc == LC_OK)
    rc = ps_internal_token_range(s, error);
  /* Protect metadata before retaining a free-list selection: admission may
   * transfer common intervals into the checkpoint-only pool. */
  if (rc == LC_OK)
    rc = owned_allowance(s, budget, 0, !budget ? &checkpointed : NULL, error);
select_run:
  if (rc == LC_OK) {
    for (;;) {
      partial = fit = NULL;
      choice = NULL;
      partial_bytes = 0;
      partial_at = 0;
      clean = 0;
      for (link = &s->free; *link; link = &(*link)->next) {
        if ((*link)->claimed && !budget)
          continue;
        at = (*link)->offset;
        prepared =
            s->readiness
                ? ps_readiness_zero_find(s, at, (*link)->bytes, bytes, &at)
            : (*link)->clean ? ((*link)->bytes < bytes ? (*link)->bytes : bytes)
                             : 0;
        if (prepared == bytes) {
          choice = link;
          selected = at;
          clean = 1;
          break;
        }
        if (prepared > partial_bytes) {
          partial = link;
          partial_bytes = prepared;
          partial_at = at;
        }
        /* When no prepared interval fits, favor a common run that can
         * certify more spare pages within the same preparation barrier. */
        if ((*link)->bytes >= bytes &&
            (!fit || ((*fit)->claimed && !(*link)->claimed) ||
             (s->readiness && !budget && (*fit)->bytes < PS_MIB &&
              (*link)->bytes > (*fit)->bytes)))
          fit = link;
      }
      if (!choice && partial) {
        choice = partial;
        selected = partial_at;
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
      if (!clean)
        selected = (*choice)->offset;
      if (bytes > (*choice)->bytes)
        bytes = (*choice)->bytes;
      free_before = s->free_bytes;
      generation = s->generation;
      rc = owned_allowance(s, budget, bytes,
                           !budget && bytes == PS_PAGE ? &checkpointed : NULL,
                           error);
      /* The bound is already protected. A newly reclaimed common interval
       * can change or coalesce the selected node before we take it. */
      if (rc == LC_OK &&
          (s->free_bytes != free_before || s->generation != generation))
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
      if (s->readiness && ps_readiness_pressure(s)) {
        /* Spare zeroing may borrow a small share of common space while
         * peers are competing for room to continue admitted appends. */
        uint64_t spare_bytes = (s->free_bytes - s->claimed_free_bytes) / 8;
        spare_bytes = spare_bytes / PS_PAGE * PS_PAGE;
        if (reserved - bytes > spare_bytes)
          reserved = bytes + spare_bytes;
      }
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
  if (rc == LC_OK && selected != (*choice)->offset) {
    ps_run *prefix = lc_alloc_with_allocator(NULL, sizeof(*prefix));
    if (!prefix)
      rc = err(error, LC_ERR_NOMEM, "cannot split prepared payload interval");
    else {
      *prefix = **choice;
      prefix->bytes = selected - prefix->offset;
      (*choice)->offset = selected;
      (*choice)->bytes -= prefix->bytes;
      prefix->next = *choice;
      *choice = prefix;
      choice = &prefix->next;
    }
  }
  if (rc == LC_OK) {
    rc = take_payload_selected(s, budget, choice, reserved, bytes,
                               &p->extents[0].offset, error);
    if (rc == LC_OK)
      ps_internal_budget_spend(budget, bytes, 0, 0);
    else
      ps_internal_free_coalesce(s);
  }
  if (rc == LC_OK) {
    p->token = s->token_next++;
    p->bytes = p->extents[0].bytes = reserved; /* entire private preparation */
    p->count = 1;
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
    ps_wire_put64(header, OWNED_MAGIC);
    ps_wire_put64(header + 8, p->token);
    ps_wire_put64(header + 16, p->key.owner);
    ps_wire_put64(header + 24, base);
    ps_wire_put64(header + 32, p->extents[0].offset);
    ps_wire_put64(header + 40, bytes);
    memcpy(header + 48, s->identity, 16);
    p->extents[0].crc = checksum(header, PS_RUN_IDENTITY_BYTES);
    rc = transfer(s, header, sizeof(header), p->extents[0].offset, 1, error);
  }
  if (rc == LC_OK && !clean) {
    pthread_mutex_lock(&s->mutex);
    rc = healthy(s, error);
    if (rc == LC_OK &&
        __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED) == LC_U64_MAX) {
      __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
      rc = err(error, LC_ERR_TRANSPORT,
               "preparation readiness exhausted; reopen");
    }
    if (rc == LC_OK)
      sequence = __atomic_add_fetch(&s->pending_sequence, 1, __ATOMIC_RELEASE);
    pthread_mutex_unlock(&s->mutex);
    if (rc == LC_OK)
      rc = wait_prepared(s, sequence, error);
  }
  pthread_mutex_lock(&s->mutex);
  if (rc == LC_OK)
    rc = ps_internal_capture_begin(s, &guard, error);
  if (rc == LC_OK)
    rc = stream_structure_ready(s, head->entry, error);
  if (rc == LC_OK && spare)
    rc = physical_release(s, p->extents[0].offset + bytes, reserved - bytes,
                          error);
  if (rc == LC_OK) {
    p->bytes = p->extents[0].bytes = bytes;
    p->stream.run->capacity = bytes - PS_RUN_HEADER;
    if (spare) {
      spare->offset = p->extents[0].offset + bytes;
      spare->bytes = reserved - bytes;
      spare->clean = !s->readiness;
      ps_readiness_zero_pages(s, spare->offset, spare->bytes, 1);
      free_insert(s, spare);
      spare = NULL;
    }
  }
  /* Preparation released allocator authority. Revalidate the current
   * selection and the descriptor's allowance. The grant already owns its
   * physical bytes, so publication only spends metadata credits. */
  if (rc == LC_OK &&
      (head->entry->deleted || head->entry->stream.head->sealed ||
       stream_lookup(s, head->entry->token) != head->entry ||
       base != head->entry->stream.head->capacity))
    rc = err(error, LC_ERR_INVALID, "owned stream changed during preparation");
  if (rc == LC_OK)
    rc = owned_allowance(s, budget, 0, !budget ? &checkpointed : NULL, error);
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
    install(s, p, lookup(s, p->key));
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
    ps_internal_entry_free(p);
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
static int owned_grant(ps_owned_head *head, ps_budget *budget, uint64_t base,
                       uint64_t desired, int verify_identity,
                       ps_owned_run **out, int *found, lc_error *error) {
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
    rc = owned_capture_begin(head, &guard, error);
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
    if (rc == LC_OK && run->entry && verify_identity)
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
int ps_owned_grant(ps_owned_head *head, ps_budget *budget, uint64_t base,
                   uint64_t desired, ps_owned_run **out, int *found,
                   lc_error *error) {
  return owned_grant(head, budget, base, desired, 1, out, found, error);
}
int ps_owned_capture_piece(ps_owned_head *head, uint64_t base, int keep_bytes,
                           ps_owned_run **out, lc_error *error) {
  int found;
  int rc = owned_grant(head, NULL, base, 0, keep_bytes, out, &found, error);
  if (rc == LC_OK && !found)
    rc = err(error, LC_ERR_PENDING, "captured piece mapping changed");
  return rc;
}
uint64_t ps_owned_run_capacity(const ps_owned_run *run) {
  return run ? run->entry->stream.run->capacity : 0;
}
uint64_t ps_owned_run_base(const ps_owned_run *run) {
  return run ? run->entry->key.key : 0;
}
static void owned_run_free_locked(ps_owned_run *run) {
  ps_owned_run_batch *batch = run->batch;
  if (!batch)
    lc_free_with_allocator(NULL, run);
  else if (!--batch->remaining)
    lc_free_with_allocator(NULL, batch);
}
/* A merged OFD witness still belongs to the exact entries. The kernel can
 * split the range when one entry loses its last pin before its neighbors. */
static int extend_run_range(ps_owned_range *range, const ps_entry *p) {
  uint64_t offset = p->extents[0].offset;
  uint64_t bytes = extent_allocation(&p->extents[0]);
  if (!range->bytes || offset + bytes == range->offset)
    range->offset = offset;
  else if (range->offset + range->bytes != offset)
    return 0;
  range->bytes += bytes;
  return 1;
}
static int run_needs_witness(const ps_entry *p) {
  return p->count == 1 && !p->pins && !p->payload_claimed;
}
static int pin_run_mappings(ps_space *s, ps_owned_mapping *items, size_t count,
                            int provisional, size_t *pinned, lc_error *e) {
  size_t first, next, i;
  int shared = ps_coordination_shared(s->coordination);
  int rc;
  for (first = 0; first < count; first = next) {
    ps_entry *p = items[first].handle->entry;
    next = first + 1;
    if (shared && run_needs_witness(p)) {
      ps_owned_range range = {0};
      (void)extend_run_range(&range, p);
      for (; next < count; ++next) {
        p = items[next].handle->entry;
        if (!run_needs_witness(p) || !extend_run_range(&range, p))
          break;
      }
      rc = provisional ? take_physical_witness(s, range.offset, range.bytes, e)
                       : physical_claim(s, range.offset, range.bytes, e);
      if (rc != LC_OK)
        return rc;
      s->reader_claim_bytes += range.bytes;
      for (i = first; i < next; ++i) {
        p = items[i].handle->entry;
        p->reader_claimed = 1;
        ++p->pins;
      }
    } else {
      rc = entry_pin(s, p, provisional, e);
      if (rc != LC_OK)
        return rc;
    }
    s->readers += next - first;
    *pinned = next;
  }
  return LC_OK;
}
static void release_run_range(ps_space *s, ps_owned_range *range) {
  if (range->bytes) {
    (void)reader_release_range(s, range->offset, range->bytes, NULL);
    range->bytes = 0;
  }
}
static int close_run_mappings_locked(ps_space *s, ps_owned_mapping *items,
                                     size_t count, size_t pinned) {
  ps_owned_range range = {0};
  size_t i;
  int collect = 0;
  for (i = 0; i < count; ++i) {
    ps_owned_run *run = items[i].handle;
    ps_entry *p = run->entry;
    if (__atomic_sub_fetch(&run->references, 1, __ATOMIC_ACQ_REL))
      continue;
    if (i < pinned) {
      if (p->pins == 1 && p->reader_claimed && p->count == 1) {
        if (!extend_run_range(&range, p)) {
          release_run_range(s, &range);
          (void)extend_run_range(&range, p);
        }
        p->pins = 0;
        p->reader_claimed = 0;
      } else
        ps_internal_entry_unpin(s, p);
      --s->readers;
      collect |= p->state == 2 && p->safe && !p->pins;
    }
    owned_run_free_locked(run);
  }
  release_run_range(s, &range);
  return collect;
}
void ps_owned_runs_discard_locked(ps_owned_mapping *items, size_t count) {
  if (count)
    (void)close_run_mappings_locked(items[0].handle->space, items, count,
                                    count);
  lc_free_with_allocator(NULL, items);
}
static int pin_runs_locked(ps_space *s, ps_entry *head, ps_key key,
                           const ps_ref *ref, uint64_t start, uint64_t end,
                           int provisional, ps_owned_status *status,
                           ps_owned_mapping **out, size_t *count,
                           lc_error *error) {
  ps_entry *p, *first = NULL, *limit = NULL;
  ps_owned_mapping *items = NULL;
  ps_owned_run_batch *batch = NULL;
  size_t n = 0, capacity = 0, pinned = 0;
  size_t in_batch = 0, batch_size = 0;
  int rc = LC_OK;
  if (out)
    *out = NULL;
  if (count)
    *count = 0;
  if (!out || !count ||
      (!head && (!ref || key.kind != PS_STREAM || (!status && start >= end))))
    return err(error, LC_ERR_INVALID,
               "mapping capture requires head and outputs");
  if (!head) {
    head = lookup(s, key);
    if (!head || head->token != ref->incarnation ||
        memcmp(ref->container, s->identity, 16) || end > head->bytes)
      rc = err(error, LC_ERR_PROTOCOL, "certified stream prefix changed");
  }
  if (rc == LC_OK)
    rc = stream_structure_ready(s, head, error);
  if (rc == LC_OK &&
      (head->deleted ||
       (end && (start >= end || end > head->stream.head->capacity)) ||
       head->stream.head->runs > (uint64_t)((size_t)-1 / sizeof(*items))))
    rc = err(error, LC_ERR_INVALID,
             "mapping capture is unavailable or too large");
  if (rc == LC_OK) {
    first = head->stream.head->owned_first;
    capacity = (size_t)head->stream.head->runs;
    if (status) {
      ps_owned_head selected;
      selected.space = s;
      selected.entry = head;
      owned_status(&selected, status);
    }
  }
  if (rc == LC_OK && end && end == head->stream.head->capacity) {
    /* A continued append usually needs only the last, partially used run. */
    first = NULL;
    capacity = 0;
    for (p = head->stream.head->owned_last;
         p && p->key.key + p->stream.run->capacity > start;
         p = p->stream.run->owned_prev) {
      first = p;
      ++capacity;
    }
  } else if (rc == LC_OK && end) {
    while (first && first->key.key + first->stream.run->capacity <= start)
      first = first->stream.run->owned_next;
    capacity = 0;
    for (limit = first; limit && limit->key.key < end;
         limit = limit->stream.run->owned_next)
      ++capacity;
  }
  if (rc == LC_OK && capacity) {
    items = lc_alloc_with_allocator(NULL, capacity * sizeof(*items));
    if (!items)
      rc = err(error, LC_ERR_NOMEM, "cannot allocate mapping capture");
  }
  for (p = first; rc == LC_OK && p != limit; p = p->stream.run->owned_next) {
    ps_owned_run *run;
    if (n == capacity) {
      rc = err(error, LC_ERR_PROTOCOL, "mapping count exceeds its head");
      break;
    }
    if (in_batch == batch_size) {
      /* Retaining an individual handle should retain only a small batch. */
      batch_size = capacity - n;
      if (batch_size > ENTRY_SLAB_SLOTS)
        batch_size = ENTRY_SLAB_SLOTS;
      batch = lc_alloc_with_allocator(NULL, sizeof(*batch) +
                                                batch_size * sizeof(*run));
      if (!batch) {
        rc = err(error, LC_ERR_NOMEM, "cannot capture mapping handles");
        break;
      }
      batch->remaining = 0;
      in_batch = 0;
    }
    run = (ps_owned_run *)(batch + 1) + in_batch++;
    run->space = s;
    run->entry = p;
    run->references = 1;
    run->batch = batch;
    ++batch->remaining;
    items[n].handle = run;
    items[n].base = p->key.key;
    items[n++].capacity = p->stream.run->capacity;
  }
  if (batch && !batch->remaining)
    lc_free_with_allocator(NULL, batch);
  if (rc == LC_OK && n != capacity)
    rc = err(error, LC_ERR_PROTOCOL, "mapping count differs from its head");
  if (rc == LC_OK)
    rc = pin_run_mappings(s, items, n, provisional, &pinned, error);
  if (rc == LC_OK) {
    *out = items;
    *count = n;
  } else {
    (void)close_run_mappings_locked(s, items, n, pinned);
    lc_free_with_allocator(NULL, items);
    if (__atomic_load_n(&s->poisoned, __ATOMIC_RELAXED)) {
      lc_error_cleanup(error);
      rc = healthy(s, error);
    }
  }
  return rc;
}
int ps_owned_runs_pin_range(ps_space *s, ps_key key, const ps_ref *ref,
                            uint64_t start, uint64_t end, int provisional,
                            ps_owned_mapping **out, size_t *count,
                            lc_error *e) {
  return pin_runs_locked(s, NULL, key, ref, start, end, provisional, NULL, out,
                         count, e);
}
int ps_owned_runs_verify(const ps_owned_mapping *items, size_t count,
                         ps_owned_footer *footers, lc_error *error) {
  size_t i;
  int rc = LC_OK;
  for (i = 0; rc == LC_OK && i < count; ++i) {
    unsigned char header[PS_RUN_HEADER];
    ps_owned_run *run = items[i].handle;
    ps_entry *p = run->entry;
    ps_space *s = run->space;
    if (footers && !p->stream.run->mapped) {
      rc = transfer(s, header, sizeof(header), p->extents[0].offset, 0, error);
      if (rc == LC_OK)
        rc = validate_owned_identity(s, p, header, error);
      if (rc == LC_OK)
        memcpy(footers[i].bytes, header + PS_RUN_IDENTITY_BYTES,
               sizeof(footers[i].bytes));
    } else
      rc = verify_owned(s, p, header, error);
  }
  return rc;
}
static int verify_captured_runs(ps_owned_mapping *items, size_t count,
                                ps_owned_footer **footers, lc_error *error) {
  ps_owned_footer *captured = NULL;
  int rc;
  if (footers && count) {
    if (count <= SIZE_MAX / sizeof(*captured))
      captured = lc_alloc_with_allocator(NULL, count * sizeof(*captured));
    if (!captured)
      return err(error, LC_ERR_NOMEM, "cannot allocate mapping footers");
  }
  rc = ps_owned_runs_verify(items, count, captured, error);
  if (rc != LC_OK)
    lc_free_with_allocator(NULL, captured);
  else if (footers)
    *footers = captured;
  return rc;
}
int ps_owned_open_initial(ps_space *s, ps_key key, const ps_ref *ref,
                          ps_owned_head **out, ps_owned_status *status,
                          ps_owned_initial *initial, lc_error *error) {
  int rc;
  *out = NULL;
  memset(initial, 0, sizeof(*initial));
  if (key.kind != PS_STREAM || memcmp(ref->container, s->identity, 16))
    return err(error, LC_ERR_INVALID,
               "stale or foreign owned stream reference");
  /* The exact reference names the edit guard before looking up the head.
   * Validate that reference and pin its runs within one allocator hold.
   * Keep the physical reads outside allocator authority. */
  rc = ps_coordination_lock(s->coordination, PS_COORDINATION_STREAM,
                            ref->incarnation, 0, &initial->edit, error);
  if (rc == LC_OK)
    rc = owned_open(s, NULL, key, 0, ref, 1, 0, 0, out, status, initial, error);
  if (rc == LC_OK)
    rc = verify_captured_runs(initial->runs, initial->count, &initial->footers,
                              error);
  if (rc != LC_OK) {
    ps_owned_runs_close(initial->runs, initial->count);
    rc = owned_edit_end(s, initial->edit, rc, error);
    ps_owned_close(*out);
    *out = NULL;
    memset(initial, 0, sizeof(*initial));
  }
  return rc;
}
static int capture_runs(ps_space *s, ps_entry *head, ps_key key,
                        const ps_ref *ref, uint64_t start, uint64_t end,
                        ps_owned_status *status, ps_owned_mapping **out,
                        size_t *count, ps_owned_footer **footers,
                        lc_error *error) {
  ps_coordination_guard *guard;
  ps_owned_mapping *items = NULL;
  size_t n = 0;
  int rc;
  if (out)
    *out = NULL;
  if (count)
    *count = 0;
  if (footers)
    *footers = NULL;
  if (!s || !out || !count ||
      (!head && (!ref || key.kind != PS_STREAM || (!status && start >= end))))
    return err(error, LC_ERR_INVALID,
               "mapping capture requires head and outputs");
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &guard, error);
  if (rc == LC_OK)
    rc = pin_runs_locked(s, head, key, ref, start, end, 0, status, &items, &n,
                         error);
  rc = ps_internal_capture_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK)
    rc = verify_captured_runs(items, n, footers, error);
  if (rc == LC_OK) {
    *out = items;
    *count = n;
  } else
    ps_owned_runs_close(items, n);
  return rc;
}
int ps_owned_runs_capture(ps_owned_head *head, ps_owned_mapping **out,
                          size_t *count, lc_error *error) {
  return ps_owned_runs_capture_status(head, NULL, out, count, error);
}
int ps_owned_runs_capture_status(ps_owned_head *head, ps_owned_status *status,
                                 ps_owned_mapping **out, size_t *count,
                                 lc_error *error) {
  const ps_key none = {0};
  return capture_runs(head ? head->space : NULL, head ? head->entry : NULL,
                      none, NULL, 0, 0, status, out, count, NULL, error);
}
int ps_owned_runs_capture_initial(ps_owned_head *head, ps_owned_mapping **out,
                                  size_t *count, ps_owned_footer **footers,
                                  lc_error *error) {
  const ps_key none = {0};
  return capture_runs(head ? head->space : NULL, head ? head->entry : NULL,
                      none, NULL, 0, 0, NULL, out, count, footers, error);
}
int ps_owned_runs_key_status(ps_space *s, ps_key key, const ps_ref *ref,
                             ps_owned_status *status, ps_owned_mapping **out,
                             size_t *count, lc_error *error) {
  return capture_runs(s, NULL, key, ref, 0, 0, status, out, count, NULL, error);
}
int ps_owned_runs_range(ps_space *s, ps_key key, const ps_ref *ref,
                        uint64_t start, uint64_t end, ps_owned_mapping **out,
                        size_t *count, lc_error *error) {
  return capture_runs(s, NULL, key, ref, start, end, NULL, out, count, NULL,
                      error);
}
int ps_owned_runs_edit_range(ps_owned_head *head, uint64_t start, uint64_t end,
                             ps_owned_mapping **out, size_t *count,
                             lc_error *error) {
  const ps_key none = {0};
  return capture_runs(head->space, head->entry, none, NULL, start, end, NULL,
                      out, count, NULL, error);
}
int ps_owned_run_mapped(const ps_owned_run *run) {
  return run && run->entry->stream.run->mapped;
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
    collect_retired(s);
  owned_run_free_locked(run);
  pthread_mutex_unlock(&s->mutex);
}
void ps_owned_runs_close(ps_owned_mapping *items, size_t count) {
  if (count) {
    ps_space *s = items[0].handle->space;
    pthread_mutex_lock(&s->mutex);
    if (close_run_mappings_locked(s, items, count, count))
      collect_retired(s);
    pthread_mutex_unlock(&s->mutex);
  }
  lc_free_with_allocator(NULL, items);
}
static int owned_io(ps_owned_run *run, uint64_t offset, void *bytes, size_t n,
                    int writing, lc_error *error) {
  int rc;
  if (!run || (!bytes && n) || offset > ps_owned_run_capacity(run) ||
      n > ps_owned_run_capacity(run) - offset)
    return err(error, LC_ERR_INVALID, "owned run I/O outside granted range");
  rc = healthy(run->space, error);
  if (rc == LC_OK && run->entry->stream.run->mapped) {
    ps_entry *p = run->entry;
    ps_reader *reader;
    unsigned char *out = bytes;
    size_t lo = 0, hi = p->stream.run->range_count, mid, take;
    if (writing)
      return err(error, LC_ERR_INVALID, "mapped pieces are immutable");
    if (!n)
      return LC_OK;
    while (!p->stream.run->range_bitmap && lo < hi) {
      mid = lo + (hi - lo) / 2;
      if (p->stream.run->ranges[mid].offset +
              p->stream.run->ranges[mid].bytes <=
          offset)
        lo = mid + 1;
      else
        hi = mid;
    }
    /* The run already pins this exact incarnation. */
    reader = reader_new(run->space, p);
    if (!reader)
      return err(error, LC_ERR_NOMEM, "cannot allocate mapped read scratch");
    while (n && rc == LC_OK) {
      uint64_t data;
      if (p->stream.run->range_bitmap) {
        take = 0;
        while (take < n && (p->stream.run->range_bitmap[(offset + take) / 8] &
                            (1U << ((offset + take) % 8))))
          ++take;
        data = 64 + (p->stream.run->capacity + 7) / 8 + bitmap_rank(p, offset);
      } else if (lo < p->stream.run->range_count &&
                 offset >= p->stream.run->ranges[lo].offset) {
        take = (size_t)(p->stream.run->ranges[lo].offset +
                        p->stream.run->ranges[lo].bytes - offset);
        if (take > n)
          take = n;
        data = p->stream.run->ranges[lo].data + offset -
               p->stream.run->ranges[lo].offset;
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
static void note_owned_write(ps_space *s, uint64_t bytes, int rc) {
  if (rc == LC_OK && bytes && !s->readiness)
    (void)__atomic_add_fetch(&s->exclusive_raw_writes, 1, __ATOMIC_RELEASE);
}
int ps_owned_write(ps_owned_run *run, uint64_t offset, const void *bytes,
                   size_t n, lc_error *error) {
  int rc = owned_io(run, offset, (void *)bytes, n, 1, error);
  if (run)
    note_owned_write(run->space, n, rc);
  return rc;
}
static int owned_piece_crc(ps_space *s, uint64_t offset, uint64_t length,
                           void *scratch, uint32_t *crc, lc_error *error) {
  size_t n;
  int rc;
  while (length) {
    n = length < PS_CHUNK ? (size_t)length : PS_CHUNK;
    rc = transfer(s, scratch, n, offset, 0, error);
    if (rc != LC_OK)
      return rc;
    *crc = (uint32_t)ps_crc32(*crc, scratch, (uInt)n);
    length -= n;
    offset += n;
  }
  return LC_OK;
}
static int owned_piece(ps_owned_run *run, uint64_t bytes, void *scratch,
                       int operation, uint64_t prior,
                       const ps_piece_checksum *prefix, ps_piece_checksum *out,
                       const ps_owned_footer *captured, lc_error *error) {
  unsigned char footer[PS_RUN_HEADER - PS_RUN_IDENTITY_BYTES];
  ps_space *s;
  uint64_t offset;
  uint32_t crc;
  uint64_t reconstructed = ((lc_u64)1) << 63;
  int rc;
  if (!run || !bytes || bytes > ps_owned_run_capacity(run) ||
      ps_owned_run_capacity(run) > PS_STREAM_PIECE_BYTES ||
      (operation && !scratch) || prior > bytes || (prior && !prefix) ||
      (prefix && (prefix->reconstructed < 0 || prefix->reconstructed > 1 ||
                  (!prior && prefix->crc))))
    return err(error, LC_ERR_INVALID,
               "piece completion requires bounded finalized bytes");
  s = run->space;
  if (run->entry->stream.run->mapped) {
    if (operation == 2)
      return err(error, LC_ERR_INVALID, "mapped pieces cannot be extended");
    if (run->entry->stream.run->mapped_end > bytes)
      return err(error, LC_ERR_PROTOCOL,
                 "mapped piece exceeds its completed prefix");
    rc = operation ? verify_value_payloads(s, run->entry, scratch, NULL, error)
                   : healthy(s, error);
    if (rc == LC_OK && out)
      memset(out, 0, sizeof(*out));
    return rc;
  }
  offset = run->entry->extents[0].offset;
  rc = healthy(s, error);
  if (rc == LC_OK && operation == 2) {
    crc = prior ? prefix->crc : 0;
    rc = owned_piece_crc(s, offset + PS_RUN_HEADER + prior, bytes - prior,
                         scratch, &crc, error);
    if (rc == LC_OK) {
      memcpy(footer, "LCPSPIEC", 8);
      ps_wire_put64(footer + 8,
                    bytes |
                        (prefix && prefix->reconstructed ? reconstructed : 0));
      ps_wire_put32(footer + 16, crc);
      ps_wire_put64(footer + 20, run->entry->token);
      ps_wire_put32(footer + 28, checksum(footer, 28));
      rc = transfer(s, footer, sizeof(footer), offset + PS_RUN_IDENTITY_BYTES,
                    1, error);
      if (rc == LC_OK && out) {
        out->crc = crc;
        out->reconstructed = prefix && prefix->reconstructed;
      }
    }
    return rc;
  }
  if (rc == LC_OK) {
    if (captured)
      memcpy(footer, captured->bytes, sizeof(footer));
    else
      rc = transfer(s, footer, sizeof(footer), offset + PS_RUN_IDENTITY_BYTES,
                    0, error);
  }
  if (rc == LC_OK && (memcmp(footer, "LCPSPIEC", 8) ||
                      (ps_wire_get64(footer + 8) & ~reconstructed) != bytes ||
                      ps_wire_get64(footer + 20) != run->entry->token ||
                      ps_wire_get32(footer + 28) != checksum(footer, 28)))
    rc = err(error, LC_ERR_PROTOCOL, "invalid finalized piece checksum footer");
  if (rc == LC_OK && operation && (ps_wire_get64(footer + 8) & reconstructed))
    rc = lc_error_set(error, LC_ERR_INVALID, 0,
                      "reconstructed piece needs canonical body validation",
                      NULL, "pouch_piece_needs_validation", "pouch-space");
  if (rc == LC_OK && operation) {
    crc = 0;
    rc =
        owned_piece_crc(s, offset + PS_RUN_HEADER, bytes, scratch, &crc, error);
    if (rc == LC_OK && crc != ps_wire_get32(footer + 16))
      rc = err(error, LC_ERR_PROTOCOL, "finalized piece checksum mismatch");
  }
  if (rc == LC_OK && out) {
    out->crc = ps_wire_get32(footer + 16);
    out->reconstructed = (ps_wire_get64(footer + 8) & reconstructed) != 0;
  }
  return rc;
}
int ps_owned_piece_extend(ps_owned_run *run, uint64_t prior,
                          const ps_piece_checksum *prefix, uint64_t bytes,
                          void *scratch, ps_piece_checksum *out,
                          lc_error *error) {
  if (!out)
    return err(error, LC_ERR_INVALID, "piece extension requires CRC output");
  return owned_piece(run, bytes, scratch, 2, prior, prefix, out, NULL, error);
}
int ps_owned_piece_inspect(ps_owned_run *run, uint64_t bytes,
                           ps_piece_checksum *out, lc_error *error) {
  return owned_piece(run, bytes, NULL, 0, 0, NULL, out, NULL, error);
}
int ps_owned_piece_inspect_initial(ps_owned_run *run, uint64_t bytes,
                                   const ps_owned_footer *captured,
                                   ps_piece_checksum *out, lc_error *error) {
  return owned_piece(run, bytes, NULL, 0, 0, NULL, out, captured, error);
}
int ps_owned_piece_verify(ps_owned_run *run, uint64_t bytes, void *scratch,
                          lc_error *error) {
  return owned_piece(run, bytes, scratch, 1, 0, NULL, NULL, NULL, error);
}
typedef struct piece_selection {
  ps_owned_head *head;
  ps_entry *old, *tombstone;
  ps_writer *writer;
  uint64_t sequence;
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
  if (!p->stream.run->mapped || count != p->stream.run->range_count)
    return 0;
  if (p->stream.run->range_bitmap) {
    for (i = 0; i < count; ++i) {
      if (!ranges[i].bytes || (i && ranges[i].offset <= end) ||
          ranges[i].offset > p->stream.run->capacity ||
          ranges[i].bytes > p->stream.run->capacity - ranges[i].offset)
        return 0;
      end = ranges[i].offset + ranges[i].bytes;
      if (bitmap_rank(p, end) - bitmap_rank(p, ranges[i].offset) !=
          ranges[i].bytes)
        return 0;
      live += ranges[i].bytes;
    }
    return live == p->bytes - 64 - (p->stream.run->capacity + 7) / 8;
  }
  for (i = 0; i < count; ++i)
    if (ranges[i].offset != run->entry->stream.run->ranges[i].offset ||
        ranges[i].bytes != run->entry->stream.run->ranges[i].bytes)
      return 0;
  return 1;
}
static int piece_publish(void *context, lc_error *error) {
  piece_selection *selection = context;
  ps_space *s = selection->head->space;
  ps_entry *old = selection->old, *tombstone = selection->tombstone;
  ps_coordination_guard *guard;
  uint64_t sequence = 0;
  int rc;
  if (selection->published)
    return err(error, LC_ERR_INVALID, "piece publication already selected");
  if (selection->writer) {
    rc = publish_value(selection->writer, NULL, &selection->sequence, error);
    selection->published = selection->writer->published;
    return rc;
  }
  pthread_mutex_lock(&s->mutex);
  rc = owned_capture_begin(selection->head, &guard, error);
  if (rc == LC_OK && (lookup(s, old->key) != old || old->deleted ||
                      selection->head->entry->deleted ||
                      !selection->head->entry->stream.head->runs))
    rc = err(error, LC_ERR_PENDING, "captured piece mapping changed");
  if (rc == LC_OK)
    rc = append_event_locked(s, old, PS_REMOVE, error);
  if (rc == LC_OK) {
    sequence = __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED);
    --selection->head->entry->stream.head->runs;
    if (!selection->head->entry->dirty_link)
      dirty_entry(s, selection->head->entry, NULL);
    tombstone->key = old->key;
    tombstone->token = old->token;
    tombstone->deleted = 1;
    tombstone->publication_sequence = sequence;
    register_entry(s, tombstone);
    dirty_entry(s, tombstone, old);
    install(s, tombstone, old);
    selection->tombstone = NULL;
    selection->sequence = sequence;
    selection->published = 1;
  }
  rc = ps_internal_capture_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
static int piece_select(piece_selection *selection, ps_piece_select_fn select,
                        void *context, lc_error *error) {
  int rc = select ? select(context, piece_publish, selection, error)
                  : piece_publish(selection, error);
  if (rc == LC_OK && !selection->published)
    rc = err(error, LC_ERR_PROTOCOL, "piece selector omitted publication");
  /* The owner has released liveness and stream edit authority. Both maps
   * represent the same live bytes, and the old extents stay protected by
   * physical checkpoint ordering and existing capture pins. */
  if (rc == LC_OK)
    rc = wait_durable(selection->head->space, selection->sequence, error);
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
  size_t i, n;
  int rc, bitmap;
  if (out)
    *out = NULL;
  if (!head || !source || !out || (count && !ranges) ||
      source->space != head->space ||
      source->entry->key.owner != head->entry->token ||
      (count && ps_owned_run_capacity(source) > PS_STREAM_PIECE_BYTES) ||
      count > ps_owned_run_capacity(source))
    return err(error, LC_ERR_INVALID,
               "piece replacement requires matching bounded owners");
  s = head->space;
  old = source->entry;
  selection.head = head;
  selection.old = old;
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc == LC_OK)
    rc = stream_structure_ready(s, head->entry, error);
  if (rc == LC_OK &&
      (head->entry->deleted || old->deleted || lookup(s, old->key) != old ||
       (old->key.key >= head->entry->bytes &&
        (!head->entry->stream.head->sealed || count)) ||
       (!head->entry->stream.head->sealed &&
        old->stream.run->capacity > head->entry->bytes - old->key.key)))
    rc = err(error, LC_ERR_INVALID,
             "piece is changing, incomplete or no longer selected");
  if (rc == LC_OK) {
    /* Sealing fixes the committed tail. An entirely unused grant beyond it
     * has no canonical payload/footer to verify and can only be discarded. */
    valid = old->key.key < head->entry->bytes
                ? head->entry->bytes - old->key.key
                : 0;
    if (valid > old->stream.run->capacity)
      valid = old->stream.run->capacity;
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
    selection.tombstone = ps_internal_entry_new(PS_OWNED_RUN, 0);
    if (!selection.tombstone)
      return err(error, LC_ERR_NOMEM, "cannot allocate piece retirement");
    rc = piece_select(&selection, select, context, error);
    ps_internal_entry_free(selection.tombstone);
    return rc;
  }
  table_bytes = (uint64_t)count * 8;
  bitmap = table_bytes > (old->stream.run->capacity + 7) / 8;
  if (bitmap)
    table_bytes = (old->stream.run->capacity + 7) / 8;
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
    writer->entry->stream.run->mapped = 1;
    writer->entry->stream.run->capacity = old->stream.run->capacity;
    for (i = 0; i < count; ++i) {
      if (bitmap) {
        for (pos = ranges[i].offset; pos < ranges[i].offset + ranges[i].bytes;
             ++pos)
          table[pos / 8] |= (unsigned char)(1U << (pos % 8));
      } else {
        ps_wire_put32(table + i * 8, (uint32_t)ranges[i].offset);
        ps_wire_put32(table + i * 8 + 4, (uint32_t)ranges[i].bytes);
      }
    }
    memcpy(header, "LCPSMAPR", 8);
    ps_wire_put64(header + 8, old->key.owner);
    ps_wire_put64(header + 16, old->key.key);
    ps_wire_put64(header + 24, old->stream.run->capacity);
    ps_wire_put32(header + 32, (uint32_t)count);
    ps_wire_put32(header + 36, checksum(table, (size_t)table_bytes));
    ps_wire_put64(header + 40, 64 + table_bytes + live);
    ps_wire_put64(header + 48, writer->entry->token);
    ps_wire_put32(header + 60, (uint32_t)bitmap);
    ps_wire_put32(header + 56,
                  (uint32_t)ps_crc32(checksum(header, 56), header + 60, 4));
    rc = ps_write(writer, header, sizeof(header), error);
    if (rc == LC_OK)
      rc = ps_write(writer, table, (size_t)table_bytes, error);
  }
  for (i = 0; rc == LC_OK && i < count; ++i) {
    if (!old->stream.run->mapped && valid <= PS_CHUNK)
      rc = ps_write(writer, scratch + ranges[i].offset, (size_t)ranges[i].bytes,
                    error);
    else {
      end = ranges[i].offset + ranges[i].bytes;
      for (pos = ranges[i].offset; rc == LC_OK && pos < end; pos += n) {
        n = end - pos < PS_CHUNK ? (size_t)(end - pos) : PS_CHUNK;
        rc = ps_owned_read(source, pos, scratch, n, error);
        if (rc == LC_OK)
          rc = ps_write(writer, scratch, n, error);
      }
    }
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
    /* The selector can install its resident mapping before releasing edit
     * authority. It must retain its own handle after a successful publish. */
    *out = result;
    rc = piece_select(&selection, select, context, error);
  }
  if (rc == LC_OK)
    result = NULL;
  else
    *out = NULL;
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
  if (!run || !scratch || run->entry->stream.run->mapped ||
      offset > ps_owned_run_capacity(run) ||
      bytes > ps_owned_run_capacity(run) - offset)
    return err(error, LC_ERR_INVALID, "owned clear outside granted range");
  rc = healthy(run->space, error);
  if (rc != LC_OK)
    return rc;
  memset(scratch, 0, PS_CHUNK);
  rc = initialize_range(run->space,
                        run->entry->extents[0].offset + PS_RUN_HEADER + offset,
                        bytes, scratch, error);
  note_owned_write(run->space, bytes, rc);
  return rc;
}
static int owned_tail_allowed(ps_entry *p, uint64_t tail) {
  return !p->deleted && !p->stream.head->sealed && tail >= p->bytes &&
         tail <= p->stream.head->capacity;
}
static int owned_ready_sequence(ps_space *s, uint64_t *sequence,
                                lc_error *error) {
  if (__atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED) == LC_U64_MAX) {
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
      head->stream.head->wire = s->sequence;
    }
  } else if (!*sequence)
    rc = owned_ready_sequence(s, sequence, error);
  if (rc == LC_OK) {
    head->bytes = tail;
    head->stream.head->complete_generation =
        complete && s->readiness ? s->generation : 0;
    stream_admitted(s, head, complete && s->readiness);
    if (!head->dirty_link)
      dirty_entry(s, head, NULL);
    head->stream.head->ready = *sequence;
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
  rc = owned_capture_begin(head, &guard, error);
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
      rc = owned_capture_begin(head, &guard, error);
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
int ps_owned_restore_tail(ps_owned_head *head, uint64_t tail, int advanced,
                          lc_error *error) {
  const unsigned char *proof = head->space->close_proof;
  size_t count, i;
  if (advanced && !head->space->readiness && head->space->reused_open_barrier) {
    if (!exclusive_raw_covered(head->space))
      return ps_owned_recovered(head, tail, error);
    count = ps_wire_get32(proof + 88);
    proof += CLOSE_PROOF_HEADER;
    for (i = 0; i < count; ++i, proof += CLOSE_PROOF_TAIL)
      if (ps_wire_get64(proof) == head->entry->token &&
          ps_wire_get64(proof + 8) >= tail)
        break;
    if (i == count)
      return ps_owned_recovered(head, tail, error);
  }
  if (advanced && ps_readiness_reused_open_barrier(head->space))
    return ps_owned_recovered(head, tail, error);
  return ps_owned_commit(head, tail, PS_COMMIT_DEFER_SYNC, NULL, NULL, error);
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
  rc = owned_capture_begin(head, &guard, error);
  previous = head->entry->bytes;
  if (rc == LC_OK && !owned_tail_allowed(head->entry, tail))
    rc = err(error, LC_ERR_PROTOCOL, "recovered tail outside owned range");
  if (rc == LC_OK)
    rc = owned_ready_sequence(s, &sequence, error);
  rc = allocation_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK)
    rc = wait_prepared(s, sequence, error);
  if (rc == LC_OK) {
    pthread_mutex_lock(&s->mutex);
    rc = owned_capture_begin(head, &guard, error);
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
  ps_stream_state stream_snapshot;
  ps_coordination_guard *guard;
  uint64_t sequence = 0, tail;
  int rc, sealed, needs_sync;
  if (!head)
    return err(error, LC_ERR_INVALID, "owned seal requires head");
  s = head->space;
  pthread_mutex_lock(&s->mutex);
  rc = owned_capture_begin(head, &guard, error);
  sealed = head->entry->stream.head->sealed;
  tail = head->entry->bytes;
  needs_sync = head->entry->stream.head->ready >
                   __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE) ||
               head->entry->stream.head->wire > s->peer_durable;
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
  rc = owned_capture_begin(head, &guard, error);
  if (rc == LC_OK && head->entry->deleted)
    rc = err(error, LC_ERR_INVALID, "retired stream cannot be sealed");
  if (rc == LC_OK && head->entry->bytes != tail)
    rc = err(error, LC_ERR_PENDING, "owned stream advanced during seal");
  sealed = head->entry->stream.head->sealed;
  if (rc == LC_OK && !sealed) {
    snapshot = *head->entry;
    stream_snapshot = *snapshot.stream.head;
    snapshot.stream.head = &stream_snapshot;
    stream_snapshot.sealed = 1;
    rc = append_event_locked(s, &snapshot, PS_SEAL, error);
  }
  if (rc == LC_OK && !sealed) {
    head->entry->stream.head->sealed = 1;
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
  rc = owned_capture_begin(head, &guard, error);
  if (rc == LC_OK &&
      (head->entry->deleted || ps_catalog_target_retained(s, head->entry)))
    rc = err(error, LC_ERR_INVALID,
             "owned identity is retained by a catalog binding or view");
  if (rc == LC_OK && !head->entry->stream.head->claim)
    rc = ps_internal_stream_claim(s, head->entry->token,
                                  &head->entry->stream.head->claim, error);
  if (rc == LC_OK)
    head->entry->stream.head->orphan_claimed = 1;
  rc = ps_internal_capture_end(s, guard, rc, error);
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_owned_retire_record(ps_owned_head *head, uint64_t *recorded,
                           lc_error *error) {
  ps_space *s;
  ps_coordination_guard *guard;
  uint64_t sequence = 0;
  int rc;
  *recorded = 0;
  if (!head)
    return err(error, LC_ERR_INVALID, "owned retirement requires head");
  s = head->space;
  pthread_mutex_lock(&s->mutex);
  rc = owned_capture_begin(head, &guard, error);
  if (rc == LC_OK && !head->entry->deleted &&
      ps_catalog_target_retained(s, head->entry))
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
  if (rc == LC_OK)
    *recorded = sequence;
  return rc;
}
int ps_owned_retire_wait(ps_owned_head *head, uint64_t sequence,
                         lc_error *error) {
  return wait_durable(head->space, sequence, error);
}
int ps_owned_retire(ps_owned_head *head, lc_error *error) {
  uint64_t sequence;
  int rc = ps_owned_retire_record(head, &sequence, error);
  return rc == LC_OK ? ps_owned_retire_wait(head, sequence, error) : rc;
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
  install(s, p, old);
}
void ps_internal_remove(ps_space *s, ps_entry *p, uint64_t sequence) {
  mark_removed(s, p, sequence);
}
int ps_internal_durable(ps_space *s, uint64_t sequence, lc_error *e) {
  return wait_durable(s, sequence, e);
}
void ps_internal_collect(ps_space *s) { collect_retired(s); }
