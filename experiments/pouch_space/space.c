#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#include "space.h"
#include "lc_api_internal.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>

#define ROOT_MAGIC UINT64_C(0x31544f4f525350)
#define EVENT_MAGIC UINT64_C(0x31565350)
#define BUCKETS 257U
#define JOURNAL_OFFSET (2U * PS_PAGE + 2U * PS_BANK)
#define TOKEN_RANGE UINT64_C(4294967296)
enum ps_event { PS_PUT, PS_REMOVE, PS_RESERVE_IDS };

typedef struct ps_extent {
  uint64_t offset, bytes;
  uint32_t crc;
} ps_extent;
typedef struct ps_entry {
  ps_key key;
  uint64_t token, bytes;
  uint64_t publication_sequence;
  ps_extent *extents;
  size_t count, capacity, pins;
  int state, safe, deleted; /* 0 private, 1 live, 2 retired */
  struct ps_entry *previous;
  struct ps_entry *all_next, *hash_next;
} ps_entry;
typedef struct ps_run {
  uint64_t offset, bytes;
  struct ps_run *next;
} ps_run;
struct ps_space {
  int fd, poisoned, durable, root;
  pthread_mutex_t mutex;
  pthread_mutex_t sync_io_mutex, commit_mutex;
  pthread_cond_t commit_cond;
  int locks_initialized, syncing;
  ps_io io;
  unsigned char identity[16];
  uint64_t capacity, maximum, generation, sequence, syncs, checkpoints;
  uint64_t token_next, token_limit;
  uint64_t pending_sequence, durable_sequence;
  uint64_t sync_requests, sync_batches, max_sync_batch;
  size_t journal, metadata, readers, writers;
  ps_entry *entries, *buckets[BUCKETS];
  ps_run *free;
};
struct ps_writer {
  ps_space *space;
  ps_entry *entry;
  size_t used;
  int failed, published;
  unsigned char buffer[PS_CHUNK];
};
struct ps_reader {
  ps_space *space;
  ps_entry *entry;
  size_t extent, offset, loaded;
  unsigned char buffer[PS_CHUNK];
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
/* The caller has already published complete immutable bytes. One caller leads
 * a sync of the ready prefix; arrivals during I/O form the next group. Neither
 * the directory mutex nor the commit mutex is held across the actual sync.
 * Structural and commit barriers share the separate sync I/O mutex. */
static int wait_durable(ps_space *s, uint64_t sequence, lc_error *error) {
  uint64_t target, covered;
  int rc = LC_OK;
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
    rc = sync_file(s, error);
    pthread_mutex_lock(&s->commit_mutex);
    ++s->sync_batches;
    if (rc == LC_OK) {
      if (target > __atomic_load_n(&s->durable_sequence, __ATOMIC_RELAXED))
        __atomic_store_n(&s->durable_sequence, target, __ATOMIC_RELEASE);
      if (covered > s->max_sync_batch)
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
  return k.kind == PS_STATE || k.kind == PS_COLLECTION;
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
static size_t entry_bytes(const ps_entry *p) { return 64 + 24 * p->count; }
static void retire(ps_space *s, ps_entry *p) {
  ps_entry **link = &s->buckets[bucket(p->key)];
  while (*link != p)
    link = &(*link)->hash_next;
  *link = p->hash_next;
  p->hash_next = NULL;
  p->state = 2;
  if (!p->deleted)
    s->metadata -= entry_bytes(p);
}
static void install(ps_space *s, ps_entry *p) {
  ps_entry *old = lookup(s, p->key);
  p->previous = old;
  if (old)
    retire(s, old);
  p->state = 1;
  p->hash_next = s->buckets[bucket(p->key)];
  s->buckets[bucket(p->key)] = p;
  if (!p->deleted)
    s->metadata += entry_bytes(p);
}
static void entry_free(ps_entry *p) {
  lc_free_with_allocator(NULL, p->extents);
  lc_free_with_allocator(NULL, p);
}
/* Sorted free intervals. Their number follows fragmentation, not capacity.
 * Nodes are preallocated before irreversible metadata changes when needed. */
static void free_insert(ps_space *s, ps_run *node) {
  ps_run **link = &s->free, *next;
  while (*link && (*link)->offset < node->offset)
    link = &(*link)->next;
  node->next = *link;
  *link = node;
  link = &s->free;
  while (*link && (*link)->next) {
    next = (*link)->next;
    if ((*link)->offset + (*link)->bytes == next->offset) {
      (*link)->bytes += next->bytes;
      (*link)->next = next->next;
      lc_free_with_allocator(NULL, next);
    } else
      link = &(*link)->next;
  }
}
static int take_run(ps_space *s, uint64_t *bytes, uint64_t *out,
                    lc_error *error) {
  ps_run **link = &s->free, *node;
  while (*link && (*link)->bytes < *bytes)
    link = &(*link)->next;
  /* Fragmentation must not strand usable pages behind a preferred run size. */
  if (!*link && s->free) {
    link = &s->free;
    *bytes = (*link)->bytes;
  }
  if (!*link)
    return err(error, LC_ERR_INVALID,
               "container payload capacity exhausted; checkpoint or grow");
  node = *link;
  *out = node->offset;
  node->offset += *bytes;
  node->bytes -= *bytes;
  if (!node->bytes) {
    *link = node->next;
    lc_free_with_allocator(NULL, node);
  }
  return LC_OK;
}
/* Rebuild free intervals by subtracting reachable runs. Also rejects overlap.
 */
static int exclude_run(ps_space *s, uint64_t offset, uint64_t bytes,
                       lc_error *error) {
  ps_run **link = &s->free, *node, *tail;
  while (*link && (*link)->offset + (*link)->bytes <= offset)
    link = &(*link)->next;
  node = *link;
  if (!node || node->offset > offset || bytes > node->bytes ||
      offset - node->offset > node->bytes - bytes)
    return err(error, LC_ERR_PROTOCOL, "overlapping container ownership");
  if (node->offset == offset) {
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
    node->next = tail;
  }
  node->bytes = offset - node->offset;
  return LC_OK;
}
static int release_entry_runs(ps_space *s, ps_entry *p, lc_error *error) {
  size_t i;
  ps_run *list = NULL, *node;
  for (i = 0; i < p->count; ++i) {
    node = lc_calloc_with_allocator(NULL, 1, sizeof(*node));
    if (!node) {
      while (list) {
        node = list;
        list = node->next;
        lc_free_with_allocator(NULL, node);
      }
      return err(error, LC_ERR_NOMEM, "cannot allocate retired free intervals");
    }
    node->offset = p->extents[i].offset;
    node->bytes = aligned(p->extents[i].bytes);
    node->next = list;
    list = node;
  }
  while (list) {
    node = list;
    list = node->next;
    free_insert(s, node);
  }
  return LC_OK;
}
static void collect(ps_space *s) {
  ps_entry **link = &s->entries, *p;
  while (*link) {
    p = *link;
    if (p->state == 2 && p->safe && !p->pins &&
        release_entry_runs(s, p, NULL) == LC_OK) {
      *link = p->all_next;
      entry_free(p);
    } else
      link = &p->all_next;
  }
}
static void encode_entry(unsigned char *wire, const ps_entry *p, uint64_t seq,
                         int event) {
  size_t i, bytes = event == PS_PUT ? entry_bytes(p) : 64;
  memset(wire, 0, bytes);
  put32(wire, (uint32_t)EVENT_MAGIC);
  put32(wire + 4, (uint32_t)bytes);
  put64(wire + 8, seq);
  put32(wire + 16, p->key.kind);
  put32(wire + 20, (uint32_t)event);
  put64(wire + 24, p->key.owner);
  put64(wire + 32, p->key.key);
  put64(wire + 40, p->token);
  if (event == PS_PUT) {
    put64(wire + 48, p->bytes);
    put32(wire + 56, (uint32_t)p->count);
    for (i = 0; i < p->count; ++i) {
      put64(wire + 64 + 24 * i, p->extents[i].offset);
      put64(wire + 72 + 24 * i, p->extents[i].bytes);
      put32(wire + 80 + 24 * i, p->extents[i].crc);
    }
  }
  put32(wire + 60,
        (uint32_t)crc32(checksum(wire, 60), wire + 64, (uInt)(bytes - 64)));
}
static int decode_entry(ps_space *s, const unsigned char *wire, size_t bytes,
                        ps_entry **out, int *event, lc_error *error) {
  size_t count, i;
  uint64_t sum = 0, offset, n;
  ps_entry *p;
  *out = NULL;
  if (bytes < 64 || get32(wire) != EVENT_MAGIC || get32(wire + 4) != bytes ||
      get32(wire + 60) !=
          (uint32_t)crc32(checksum(wire, 60), wire + 64, (uInt)(bytes - 64)))
    return err(error, LC_ERR_PROTOCOL, "invalid container metadata checksum");
  count = get32(wire + 56);
  if (count > (PS_BANK - 64) / 24 || bytes != 64 + count * 24 ||
      get32(wire + 20) > PS_RESERVE_IDS)
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
      (*event == PS_REMOVE && (count || p->bytes)))
    goto invalid;
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
    offset = get64(wire + 64 + 24 * i);
    n = get64(wire + 72 + 24 * i);
    if (!n || n > PS_CHUNK || offset < PS_DATA || offset % PS_PAGE ||
        offset > s->capacity || aligned(n) > s->capacity - offset ||
        UINT64_MAX - sum < n)
      goto invalid;
    p->extents[i].offset = offset;
    p->extents[i].bytes = n;
    p->extents[i].crc = get32(wire + 80 + 24 * i);
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
static int verify_entry(ps_space *s, ps_entry *p, unsigned char *buffer,
                        lc_error *error) {
  size_t i;
  int rc;
  for (i = 0; i < p->count; ++i) {
    rc = transfer(s, buffer, (size_t)p->extents[i].bytes, p->extents[i].offset,
                  0, error);
    if (rc != LC_OK)
      return rc;
    if (checksum(buffer, (size_t)p->extents[i].bytes) != p->extents[i].crc)
      return err(error, LC_ERR_PROTOCOL, "container payload checksum mismatch");
  }
  return LC_OK;
}
static int checkpoint_locked(ps_space *s, lc_error *error) {
  unsigned char root[64], zero[64] = {0}, *snapshot;
  ps_entry *p, grant;
  size_t cursor = 64;
  int next = 1 - s->root, rc;
  if (s->generation == UINT64_MAX)
    return err(error, LC_ERR_INVALID,
               "container checkpoint generation exhausted");
  snapshot = lc_alloc_with_allocator(NULL, s->metadata + 64);
  if (!snapshot)
    return err(error, LC_ERR_NOMEM, "cannot allocate metadata checkpoint");
  memset(&grant, 0, sizeof(grant));
  grant.token = s->token_limit;
  encode_entry(snapshot, &grant, s->sequence, PS_RESERVE_IDS);
  for (p = s->entries; p; p = p->all_next)
    if (p->state == 1 && !p->deleted) {
      encode_entry(snapshot + cursor, p, s->sequence, PS_PUT);
      cursor += entry_bytes(p);
    }
  rc = transfer(s, snapshot, cursor, 2U * PS_PAGE + (uint64_t)next * PS_BANK, 1,
                error);
  if (rc == LC_OK)
    rc = sync_file(s, error); /* payloads, old journal, and new bank */
  memset(root, 0, sizeof(root));
  put64(root, ROOT_MAGIC);
  put64(root + 8, s->generation + 1);
  put64(root + 16, s->capacity);
  put64(root + 24, s->sequence);
  put64(root + 32, cursor);
  memcpy(root + 40, s->identity, 16);
  put32(root + 56, checksum(snapshot, cursor));
  put32(root + 60, checksum(root, 60));
  lc_free_with_allocator(NULL, snapshot);
  if (rc == LC_OK)
    rc = transfer(s, root, sizeof(root), (uint64_t)next * PS_PAGE, 1, error);
  if (rc == LC_OK)
    rc = sync_file(s, error);
  if (rc == LC_OK)
    rc = transfer(s, zero, sizeof(zero), (uint64_t)s->root * PS_PAGE, 1, error);
  if (rc == LC_OK)
    rc = sync_file(s, error); /* obsolete root cannot survive reuse */
  if (rc != LC_OK)
    return rc;
  s->root = next;
  ++s->generation;
  ++s->checkpoints;
  s->journal = 0;
  mark_durable(s, s->sequence);
  /* The checkpoint covers every projected version. Existing readers retain
   * their own pins, and new readers no longer need pending version history. */
  for (p = s->entries; p; p = p->all_next) {
    p->previous = NULL;
    if (p->state == 1 && p->deleted)
      retire(s, p);
  }
  for (p = s->entries; p; p = p->all_next)
    if (p->state == 2)
      p->safe = 1;
  collect(s);
  return LC_OK;
}
static int append_event(ps_space *s, ps_entry *p, int event, lc_error *error) {
  size_t bytes = event == PS_PUT ? entry_bytes(p) : 64;
  unsigned char *wire;
  int rc;
  if (s->sequence == UINT64_MAX)
    return err(error, LC_ERR_INVALID,
               "container publication sequence exhausted");
  wire = lc_alloc_with_allocator(NULL, bytes);
  if (!wire)
    return err(error, LC_ERR_NOMEM, "cannot allocate publication metadata");
  rc = LC_OK;
  if (bytes > PS_JOURNAL - s->journal)
    rc = checkpoint_locked(s, error);
  encode_entry(wire, p, s->sequence + 1, event);
  if (rc == LC_OK)
    rc = transfer(s, wire, bytes, JOURNAL_OFFSET + s->journal, 1, error);
  lc_free_with_allocator(NULL, wire);
  if (rc == LC_OK) {
    ++s->sequence;
    s->journal += bytes;
    __atomic_store_n(&s->pending_sequence, s->sequence, __ATOMIC_RELEASE);
  }
  return rc;
}
/* Reserve incarnation numbers once per open. Returned NoSync references must
 * never alias a later value even if every write since this grant is lost. */
static int grant_tokens(ps_space *s, lc_error *error) {
  ps_entry grant;
  uint64_t limit = s->token_limit;
  int rc;
  if (limit > UINT64_MAX - TOKEN_RANGE)
    return err(error, LC_ERR_INVALID, "container incarnation ranges exhausted");
  memset(&grant, 0, sizeof(grant));
  grant.token = limit + TOKEN_RANGE;
  rc = append_event(s, &grant, PS_RESERVE_IDS, error);
  if (rc == LC_OK) {
    s->token_next = limit + 1;
    s->token_limit = grant.token;
  }
  return rc;
}
static void destroy(ps_space *s) {
  ps_entry *p;
  ps_run *r;
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
  if (s->fd >= 0)
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
  return get64(p) == ROOT_MAGIC && get64(p + 8) && capacity >= PS_MIB &&
         capacity <= INT64_MAX && capacity % PS_MIB == 0 &&
         get64(p + 32) <= PS_BANK && checksum(p, 60) == get32(p + 60);
}
static int recover(ps_space *s, lc_error *error) {
  unsigned char roots[128], zero[1024] = {0}, *wire, *buffer;
  const unsigned char *root;
  ps_entry *p = NULL, *old;
  struct stat st;
  size_t cursor = 0, bytes, end, i;
  int a, b, event, rc;
  rc = transfer(s, roots, 64, 0, 0, error);
  if (rc == LC_OK)
    rc = transfer(s, roots + 64, 64, PS_PAGE, 0, error);
  if (rc != LC_OK)
    return rc;
  a = root_valid(roots);
  b = root_valid(roots + 64);
  if (!a && !b)
    return err(error, LC_ERR_PROTOCOL, "no recoverable container root");
  if (a && b &&
      (memcmp(roots + 40, roots + 104, 16) == 0
           ? get64(roots + 8) == get64(roots + 72)
           : 1))
    return err(error, LC_ERR_PROTOCOL, "conflicting container roots");
  s->root = b && (!a || get64(roots + 72) > get64(roots + 8));
  root = roots + 64 * s->root;
  s->generation = get64(root + 8);
  s->capacity = get64(root + 16);
  s->sequence = get64(root + 24);
  memcpy(s->identity, root + 40, 16);
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
  end = (size_t)get64(root + 32);
  rc = transfer(s, wire, end, 2U * PS_PAGE + (uint64_t)s->root * PS_BANK, 0,
                error);
  if (rc != LC_OK)
    goto done;
  if (checksum(wire, end) != get32(root + 56)) {
    rc = err(error, LC_ERR_PROTOCOL,
             "selected container checkpoint checksum mismatch");
    goto done;
  }
  while (cursor < end) {
    if (end - cursor < 64 ||
        (bytes = get32(wire + cursor + 4)) > end - cursor || bytes < 64) {
      rc = err(error, LC_ERR_PROTOCOL, "truncated container checkpoint");
      goto done;
    }
    rc = decode_entry(s, wire + cursor, bytes, &p, &event, error);
    if (rc != LC_OK)
      goto done;
    if (event == PS_RESERVE_IDS && cursor == 0) {
      s->token_limit = p->token;
      entry_free(p);
      cursor += bytes;
      continue;
    }
    if (event != PS_PUT || !s->token_limit || lookup(s, p->key)) {
      entry_free(p);
      rc = err(error, LC_ERR_PROTOCOL, "duplicate or deleted checkpoint entry");
      goto done;
    }
    p->all_next = s->entries;
    s->entries = p;
    install(s, p);
    cursor += bytes;
  }
  if (!s->token_limit) {
    rc = err(error, LC_ERR_PROTOCOL, "checkpoint lacks incarnation grant");
    goto done;
  }
  rc = transfer(s, wire, PS_JOURNAL, JOURNAL_OFFSET, 0, error);
  if (rc != LC_OK)
    goto done;
  cursor = 0;
  while (PS_JOURNAL - cursor >= 64 && s->sequence != UINT64_MAX &&
         get32(wire + cursor) == EVENT_MAGIC &&
         get64(wire + cursor + 8) == s->sequence + 1) {
    bytes = get32(wire + cursor + 4);
    if (bytes < 64 || bytes > PS_JOURNAL - cursor)
      break;
    rc = decode_entry(s, wire + cursor, bytes, &p, &event, error);
    if (rc == LC_ERR_NOMEM)
      goto done;
    if (rc != LC_OK) {
      lc_error_cleanup(error);
      break;
    }
    if (event == PS_RESERVE_IDS) {
      if (s->token_limit > UINT64_MAX - TOKEN_RANGE ||
          p->token != s->token_limit + TOKEN_RANGE) {
        entry_free(p);
        rc = err(error, LC_ERR_PROTOCOL,
                 "invalid incarnation range progression");
        goto done;
      }
      s->token_limit = p->token;
      entry_free(p);
    } else if (event == PS_PUT) {
      rc = verify_entry(s, p, buffer, error);
      if (rc != LC_OK) {
        entry_free(p);
        if (__atomic_load_n(&s->poisoned, __ATOMIC_RELAXED))
          goto done;
        lc_error_cleanup(error);
        break;
      }
      if (p->key.kind == PS_COLLECTION && lookup(s, p->key)) {
        entry_free(p);
        rc = err(error, LC_ERR_PROTOCOL,
                 "collection position overwritten in journal");
        goto done;
      }
      p->all_next = s->entries;
      s->entries = p;
      install(s, p);
    } else {
      old = lookup(s, p->key);
      if (!old || old->token != p->token) {
        entry_free(p);
        rc = err(error, LC_ERR_PROTOCOL, "journal retirement owner mismatch");
        goto done;
      }
      retire(s, old);
      entry_free(p);
    }
    cursor += bytes;
    ++s->sequence;
  }
  if (s->metadata > PS_BANK - 64) {
    rc = err(error, LC_ERR_PROTOCOL,
             "recovered metadata exceeds checkpoint capacity");
    goto done;
  }
  s->journal = cursor;
  /* Clear all of the rejected tail, not only its first record: subsequent
   * equal-sized appends must never resurrect a stale suffix with reused seqs.
   */
  memset(wire + cursor, 0, PS_JOURNAL - cursor);
  rc = transfer(s, wire + cursor, PS_JOURNAL - cursor, JOURNAL_OFFSET + cursor,
                1, error);
  if (rc != LC_OK)
    goto done;
  rc = transfer(s, zero, 64, (uint64_t)(1 - s->root) * PS_PAGE, 1, error);
  if (rc != LC_OK)
    goto done;
  s->free = lc_calloc_with_allocator(NULL, 1, sizeof(*s->free));
  if (!s->free) {
    rc = err(error, LC_ERR_NOMEM, "cannot allocate free space directory");
    goto done;
  }
  s->free->offset = PS_DATA;
  s->free->bytes = s->capacity - PS_DATA;
  /* Replay verifies every accepted PUT body, including superseded versions.
   * Reopening must preserve that ownership until a new checkpoint retires the
   * journal; otherwise reuse can make the next replay discard a durable tail.
   */
  for (p = s->entries; p; p = p->all_next) {
    for (i = 0; i < p->count; ++i) {
      rc = exclude_run(s, p->extents[i].offset, aligned(p->extents[i].bytes),
                       error);
      if (rc != LC_OK)
        goto done;
    }
    p->previous = NULL;
  }
  rc = grant_tokens(s, error);
  if (rc == LC_OK)
    rc = sync_file(s, error); /* accepted tail, retired sibling and grant */
done:
  lc_free_with_allocator(NULL, buffer);
  lc_free_with_allocator(NULL, wire);
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
int ps_open(const char *path, int create, const ps_options *options,
            ps_space **out, lc_error *error) {
  ps_options defaults = {0};
  ps_space *s;
  int rc;
  if (out)
    *out = NULL;
  if (!path || !*path || !out)
    return err(error, LC_ERR_INVALID,
               "container open requires path and output");
  if (!options)
    options = &defaults;
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
  if (flock(s->fd, LOCK_EX | LOCK_NB) != 0) {
    rc = err(error, LC_ERR_INVALID,
             "container already open; shared-process mode unsupported");
    destroy(s);
    return rc;
  }
  if (create) {
    rc = reserve_file(s, 0, s->capacity, error);
    if (rc == LC_OK)
      rc = random_bytes(s->identity, 16, error);
    s->token_next = 1;
    s->token_limit = TOKEN_RANGE;
    if (rc == LC_OK)
      rc = checkpoint_locked(s, error);
    if (rc == LC_OK)
      rc = sync_parent(path, error);
    if (rc == LC_OK) {
      s->free = lc_calloc_with_allocator(NULL, 1, sizeof(*s->free));
      if (!s->free)
        rc = err(error, LC_ERR_NOMEM, "cannot allocate initial free interval");
      else {
        s->free->offset = PS_DATA;
        s->free->bytes = s->capacity - PS_DATA;
      }
    }
  } else
    rc = recover(s, error);
  if (rc == LC_OK && s->maximum && s->capacity > s->maximum)
    rc = err(error, LC_ERR_INVALID,
             "committed capacity exceeds configured maximum");
  if (rc != LC_OK) {
    destroy(s);
    return rc;
  }
  mark_durable(s, s->sequence);
  *out = s;
  return LC_OK;
}
int ps_close(ps_space *s, lc_error *error) {
  if (!s)
    return LC_OK;
  pthread_mutex_lock(&s->mutex);
  if (s->readers || s->writers) {
    pthread_mutex_unlock(&s->mutex);
    return err(error, LC_ERR_INVALID,
               "close container readers and writers first");
  }
  pthread_mutex_unlock(&s->mutex);
  destroy(s);
  return LC_OK;
}
int ps_begin(ps_space *s, ps_key key, ps_writer **out, lc_error *error) {
  ps_writer *w;
  ps_entry *p;
  int rc;
  if (out)
    *out = NULL;
  if (!s || !out || !valid_key(key))
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
  if (rc == LC_OK && s->token_next > s->token_limit)
    rc = err(error, LC_ERR_INVALID,
             "incarnation grant consumed; close and reopen");
  if (rc == LC_OK) {
    p->token = s->token_next++;
    p->all_next = s->entries;
    s->entries = p;
    ++s->writers;
  }
  pthread_mutex_unlock(&s->mutex);
  if (rc != LC_OK) {
    entry_free(p);
    lc_free_with_allocator(NULL, w);
  } else
    *out = w;
  return rc;
}
static int flush_writer(ps_writer *w, lc_error *error) {
  ps_space *s = w->space;
  ps_entry *p = w->entry;
  ps_extent *extent, *grown;
  size_t n, done = 0, take;
  uint64_t run_bytes;
  int rc;
  if (!w->used)
    return LC_OK;
  while (done < w->used) {
    if (p->count == (PS_BANK - 64) / 24)
      return err(error, LC_ERR_INVALID,
                 "value extent metadata exceeds checkpoint capacity");
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
    extent = &p->extents[p->count];
    take = w->used - done;
    run_bytes = aligned(take);
    rc = take_run(s, &run_bytes, &extent->offset, error);
    if (rc == LC_OK) {
      if (run_bytes < take)
        take = (size_t)run_bytes;
      extent->bytes = take;
      extent->crc = checksum(w->buffer + done, take);
      ++p->count;
      p->bytes += take;
    }
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
  int rc;
  if (!w || w->failed || w->published)
    return err(error, LC_ERR_INVALID, "container writer cannot publish");
  rc = flush_writer(w, error);
  if (rc != LC_OK) {
    w->failed = 1;
    return rc;
  }
  s = w->space;
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  old = lookup(s, w->entry->key);
  prior = old && !old->deleted ? entry_bytes(old) : 0;
  if (rc == LC_OK && old && !old->deleted &&
      w->entry->key.kind == PS_COLLECTION)
    rc = err(error, LC_ERR_INVALID, "collection position already published");
  if (rc == LC_OK &&
      entry_bytes(w->entry) > PS_BANK - 64 - (s->metadata - prior))
    rc = err(error, LC_ERR_INVALID,
             "container checkpoint metadata capacity exhausted");
  if (rc == LC_OK)
    rc = append_event(s, w->entry, PS_PUT, error);
  if (rc == LC_OK) {
    w->entry->publication_sequence = s->sequence;
    install(s, w->entry);
    w->published = 1;
    sequence = s->sequence;
    memcpy(published_ref.container, s->identity, 16);
    published_ref.incarnation = w->entry->token;
  } else
    w->failed = 1;
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK && s->durable)
    rc = wait_durable(s, sequence, error);
  if (rc == LC_OK && ref)
    *ref = published_ref;
  if (rc != LC_OK)
    w->failed = 1;
  return rc;
}
void ps_writer_close(ps_writer *w) {
  ps_entry **link;
  ps_space *s;
  if (!w)
    return;
  s = w->space;
  pthread_mutex_lock(&s->mutex);
  --s->writers;
  if (!w->published) {
    /* Even an uncertain publication is left untouched on disk by a poisoned
     * handle. Its memory can be discarded; no further allocation is allowed. */
    if (!__atomic_load_n(&s->poisoned, __ATOMIC_RELAXED) &&
        release_entry_runs(s, w->entry, NULL) != LC_OK)
      __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
    link = &s->entries;
    while (*link != w->entry)
      link = &(*link)->all_next;
    *link = w->entry->all_next;
    entry_free(w->entry);
  }
  pthread_mutex_unlock(&s->mutex);
  lc_free_with_allocator(NULL, w);
}
int ps_read_open(ps_space *s, ps_key key, const ps_ref *ref, ps_reader **out,
                 lc_error *error) {
  ps_reader *r;
  ps_entry *p;
  uint64_t visible;
  int rc;
  if (out)
    *out = NULL;
  if (!s || !out || !valid_key(key))
    return err(error, LC_ERR_INVALID,
               "container reader requires typed key and output");
  r = lc_calloc_with_allocator(NULL, 1, sizeof(*r));
  if (!r)
    return err(error, LC_ERR_NOMEM, "cannot allocate bounded reader");
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  p = lookup(s, key);
  visible = s->durable ? __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE)
                       : UINT64_MAX;
  while (p && p->publication_sequence > visible)
    p = p->previous;
  if (rc == LC_OK && (!p || p->deleted))
    rc = err(error, LC_ERR_INVALID, "container key not found");
  if (rc == LC_OK && ref &&
      (memcmp(ref->container, s->identity, 16) || ref->incarnation != p->token))
    rc = err(error, LC_ERR_INVALID, "stale or foreign container reference");
  if (rc == LC_OK) {
    ++p->pins;
    ++s->readers;
    r->space = s;
    r->entry = p;
    *out = r;
  }
  pthread_mutex_unlock(&s->mutex);
  if (rc != LC_OK)
    lc_free_with_allocator(NULL, r);
  return rc;
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
  while (count && r->extent < r->entry->count) {
    extent = &r->entry->extents[r->extent];
    if (!r->loaded) {
      rc = transfer(r->space, r->buffer, (size_t)extent->bytes, extent->offset,
                    0, error);
      if (rc != LC_OK)
        return rc;
      if (checksum(r->buffer, (size_t)extent->bytes) != extent->crc)
        return err(error, LC_ERR_PROTOCOL,
                   "container read payload checksum mismatch");
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
uint64_t ps_read_size(const ps_reader *r) { return r ? r->entry->bytes : 0; }
void ps_reader_close(ps_reader *r) {
  ps_space *s;
  if (!r)
    return;
  s = r->space;
  pthread_mutex_lock(&s->mutex);
  --r->entry->pins;
  --s->readers;
  if (r->entry->state == 2 && r->entry->safe && !r->entry->pins)
    collect(s);
  pthread_mutex_unlock(&s->mutex);
  lc_free_with_allocator(NULL, r);
}
int ps_delete(ps_space *s, ps_key key, lc_error *error) {
  ps_entry *p, *tombstone;
  int rc;
  uint64_t sequence = 0;
  if (!s || !valid_key(key))
    return err(error, LC_ERR_INVALID, "container delete requires typed key");
  tombstone = lc_calloc_with_allocator(NULL, 1, sizeof(*tombstone));
  if (!tombstone)
    return err(error, LC_ERR_NOMEM, "cannot allocate pending retirement");
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  p = lookup(s, key);
  if (rc == LC_OK && (!p || p->deleted))
    rc = err(error, LC_ERR_INVALID, "container key not found");
  if (rc == LC_OK)
    rc = append_event(s, p, PS_REMOVE, error);
  if (rc == LC_OK) {
    tombstone->key = key;
    tombstone->token = p->token;
    tombstone->deleted = 1;
    tombstone->publication_sequence = s->sequence;
    tombstone->all_next = s->entries;
    s->entries = tombstone;
    install(s, tombstone);
    sequence = s->sequence;
  } else
    entry_free(tombstone);
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK && s->durable)
    rc = wait_durable(s, sequence, error);
  return rc;
}
int ps_checkpoint(ps_space *s, lc_error *error) {
  int rc;
  if (!s)
    return err(error, LC_ERR_INVALID, "checkpoint requires container");
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc == LC_OK)
    rc = checkpoint_locked(s, error);
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_grow(ps_space *s, uint64_t bytes, lc_error *error) {
  uint64_t capacity;
  ps_run *run;
  int rc;
  if (!s || !bytes || bytes % PS_MIB)
    return err(error, LC_ERR_INVALID,
               "growth requires positive whole MiB increment");
  run = lc_calloc_with_allocator(NULL, 1, sizeof(*run));
  if (!run)
    return err(error, LC_ERR_NOMEM, "cannot allocate growth interval");
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
    rc = checkpoint_locked(s, error);
    if (rc != LC_OK)
      s->capacity = capacity; /* handle is poisoned for any structural I/O */
  }
  if (rc == LC_OK) {
    run->offset = capacity;
    run->bytes = bytes;
    free_insert(s, run);
  } else
    lc_free_with_allocator(NULL, run);
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_status(ps_space *s, ps_stats *out, lc_error *error) {
  ps_entry *p;
  uint64_t n;
  size_t i;
  if (!s || !out)
    return err(error, LC_ERR_INVALID, "container status requires output");
  pthread_mutex_lock(&s->mutex);
  memset(out, 0, sizeof(*out));
  out->capacity = s->capacity;
  out->metadata_bytes = s->metadata + 64;
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
    for (i = 0; i < p->count; ++i)
      n += aligned(p->extents[i].bytes);
    out->allocated_bytes += n;
    if (p->state == 1 && !p->deleted) {
      ++out->entries;
      out->live_bytes += p->bytes;
    } else if (p->state == 2)
      out->retired_bytes += n;
    else
      out->private_bytes += n;
  }
  pthread_mutex_unlock(&s->mutex);
  return LC_OK;
}
