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
#include <sys/file.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>
struct ps_writer {
  ps_space *space;
  ps_budget *budget;
  ps_entry *entry;
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
  unsigned char buffer[PS_CHUNK];
};
struct ps_owned_head {
  ps_space *space;
  ps_entry *entry;
};
struct ps_owned_run {
  ps_space *space;
  ps_entry *entry;
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
  return (k.kind >= PS_STATE && k.kind <= PS_CATALOG) || k.kind == PS_CONTROL ||
         k.kind == PS_ROUTING ||
         (k.kind >= PS_QUERY_FIRST && k.kind <= PS_QUERY_LAST);
}
static int immutable_key(ps_key k) {
  return k.kind == PS_STATE || k.kind == PS_COLLECTION ||
         k.kind == PS_CONTROL || k.kind == PS_ROUTING ||
         (k.kind >= PS_QUERY_FIRST && k.kind <= PS_QUERY_LAST);
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
    return 104 + p->catalog->length;
  return p->key.kind == PS_STREAM ? 80 : 64 + 24 * p->count;
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
static void retire(ps_space *s, ps_entry *p) {
  ps_entry **link = &s->buckets[bucket(p->key)];
  while (*link != p)
    link = &(*link)->hash_next;
  *link = p->hash_next;
  if (p->key.kind == PS_CATALOG)
    ps_catalog_unlink(s, p);
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
  lc_free_with_allocator(NULL, p->extents);
  lc_free_with_allocator(NULL, p);
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
        (*link)->clean == next->clean) {
      (*link)->bytes += next->bytes;
      (*link)->next = next->next;
      lc_free_with_allocator(NULL, next);
    } else
      link = &(*link)->next;
  }
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
  ps_run **link = head;
  while (*link && (*link)->bytes < *bytes)
    link = &(*link)->next;
  /* Fragmentation must not strand usable pages behind a preferred run size. */
  if (!*link && *head) {
    link = head;
    *bytes = (*link)->bytes;
  }
  if (!*link)
    return err(error, LC_ERR_INVALID,
               "container payload capacity exhausted; checkpoint or grow");
  take_selected(link, available, *bytes, out, clean);
  return LC_OK;
}
static int take_run(ps_space *s, uint64_t *bytes, uint64_t *out,
                    lc_error *error) {
  return take_interval(&s->free, &s->free_bytes, bytes, out, NULL, error);
}
/* Rebuild free intervals by subtracting reachable runs. Also rejects overlap.
 */
static int exclude_run(ps_space *s, uint64_t offset, uint64_t bytes,
                       lc_error *error) {
  ps_run **link = offset < PS_DATA ? &s->metadata_free : &s->free, *node, *tail;
  uint64_t *available =
      offset < PS_DATA ? &s->metadata_free_bytes : &s->free_bytes;
  while (*link && (*link)->offset + (*link)->bytes <= offset)
    link = &(*link)->next;
  node = *link;
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
    node->next = tail;
  }
  *available -= bytes;
  node->bytes = offset - node->offset;
  return LC_OK;
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
static void payload_release(ps_space *s, ps_extent *extent) {
  ps_payload_page *p = payload_find(s, extent->offset), **link;
  p->occupied &= ~extent_mask(extent);
  s->packed_bytes -= extent_allocation(extent);
  payload_unavailable(p);
  if (p->occupied) {
    payload_available(s, p);
    return;
  }
  link = &s->payload_buckets[(p->run.offset / PS_PAGE) % BUCKETS];
  while (*link != p)
    link = &(*link)->hash_next;
  *link = p->hash_next;
  *p->link = p->next;
  if (p->next)
    p->next->link = p->link;
  --s->packed_pages;
  free_insert(s, &p->run);
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
static int release_entry_runs(ps_space *s, ps_entry *p, lc_error *error) {
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
  while (list) {
    node = list;
    list = node->next;
    if (rc == LC_OK)
      free_insert(s, node);
    else
      lc_free_with_allocator(NULL, node);
  }
  if (rc == LC_OK)
    for (i = 0; i < p->count; ++i)
      if (p->extents[i].flags & EXTENT_PACKED)
        payload_release(s, &p->extents[i]);
  return rc;
}
static void collect(ps_space *s) {
  ps_entry **link = &s->retired, *p;
  while (*link) {
    p = *link;
    if (p->state == 2 && p->safe && !p->pins &&
        release_entry_runs(s, p, NULL) == LC_OK) {
      *link = p->retired_next;
      if (*link)
        (*link)->retired_link = link;
      unregister_entry(p);
      entry_free(p);
    } else
      link = &p->retired_next;
  }
}
static size_t event_bytes(const ps_entry *p, int event) {
  return event == PS_PUT || event == PS_SEAL ? entry_bytes(p) : 64;
}
static void encode_entry(unsigned char *wire, const ps_entry *p, uint64_t seq,
                         int event) {
  size_t i, bytes = event_bytes(p, event);
  memset(wire, 0, bytes);
  put32(wire, (uint32_t)EVENT_MAGIC);
  put32(wire + 4, (uint32_t)bytes);
  put64(wire + 8, seq);
  put32(wire + 16, p->key.kind);
  put32(wire + 20, (uint32_t)event);
  put64(wire + 24, p->key.owner);
  put64(wire + 32, p->key.key);
  put64(wire + 40, p->token);
  if (event == PS_PUT || event == PS_SEAL) {
    put64(wire + 48, p->bytes);
    put32(wire + 56, (uint32_t)p->count);
    for (i = 0; i < p->count; ++i) {
      put64(wire + 64 + 24 * i, p->extents[i].offset);
      put64(wire + 72 + 24 * i, p->extents[i].bytes);
      put32(wire + 80 + 24 * i, p->extents[i].crc);
      put32(wire + 84 + 24 * i, p->extents[i].flags);
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
  size_t count, i;
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
  if (count > (PS_JOURNAL - 64) / 24 ||
      (get32(wire + 16) != PS_CATALOG &&
       bytes != (get32(wire + 16) == PS_STREAM && (get32(wire + 20) == PS_PUT ||
                                                   get32(wire + 20) == PS_SEAL)
                     ? 80
                     : 64 + count * 24)) ||
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
        (!immutable_key(p->key) && p->key.kind != PS_STREAM &&
         p->key.kind != PS_CATALOG))) ||
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
        p->bytes > p->stream_capacity || p->stream_capacity > s->capacity ||
        (p->stream_runs == 0) != (p->stream_capacity == 0) ||
        p->stream_runs > p->stream_capacity / (PS_PAGE - PS_RUN_HEADER))
      goto invalid;
    *out = p;
    return LC_OK;
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
    offset = get64(wire + 64 + 24 * i);
    n = get64(wire + 72 + 24 * i);
    p->extents[i].flags = get32(wire + 84 + 24 * i);
    if (!n || n > (p->key.kind == PS_OWNED_RUN ? PS_MIB : PS_CHUNK) ||
        offset < PS_DATA || offset > s->capacity || n > s->capacity - offset ||
        (p->key.kind == PS_OWNED_RUN
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
static int verify_owned(ps_space *s, ps_entry *p, unsigned char *header,
                        lc_error *error) {
  int rc = transfer(s, header, PS_RUN_HEADER, p->extents[0].offset, 0, error);
  if (rc == LC_OK &&
      (checksum(header, PS_RUN_HEADER) != p->extents[0].crc ||
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
  if (p->key.kind == PS_OWNED_RUN)
    return verify_owned(s, p, buffer, error);
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
  s->metadata_free->offset = 2 * PS_PAGE;
  s->metadata_free->bytes = s->metadata_free_bytes = PS_METADATA_RESERVE;
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
static uint64_t metadata_target(ps_space *s, uint64_t bound) {
  uint64_t allocated = (uint64_t)s->metadata_pages * PS_PAGE;
  /* Enough free pages for one complete rewrite, and two complete images
   * counting the current checkpoint. A dense rewrite restores this invariant
   * from its retired image without relying on unprotected payload capacity. */
  return allocated < bound ? 2 * bound - allocated : bound;
}
static int metadata_fill(ps_space *s, uint64_t bound, uint64_t target,
                         uint64_t payload, uint64_t floor, lc_error *error) {
  uint64_t extra, bytes;
  ps_run *node;
  int rc;
  extra = target > s->metadata_free_bytes ? target - s->metadata_free_bytes : 0;
  if (s->reserved_payload > s->free_bytes ||
      payload > s->free_bytes - s->reserved_payload ||
      s->free_bytes - s->reserved_payload - payload < floor ||
      extra > s->free_bytes - s->reserved_payload - payload - floor)
    return err(error, LC_ERR_INVALID,
               "container capacity reserved for checkpoint and maintenance; "
               "checkpoint or grow");
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
    metadata_insert(s, node);
  }
  s->metadata_workspace = bound;
  return LC_OK;
}
static int metadata_protect(ps_space *s, uint64_t bound, uint64_t payload,
                            uint64_t floor, lc_error *error) {
  return metadata_fill(s, bound, metadata_target(s, bound), payload, floor,
                       error);
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
static int payload_reserve(ps_space *s, ps_entry *entry, ps_extent *extent,
                           size_t bytes, ps_budget *budget, lc_error *error) {
  ps_payload_page *p = NULL;
  size_t count = cell_count(bytes), bin, at;
  uint64_t mask, reserve = PS_PAGE;
  int rc;
  for (bin = count; bin <= CELLS; ++bin)
    if (s->available[bin]) {
      p = s->available[bin];
      break;
    }
  rc = immutable_extent_allowance(s, entry, p ? 0 : PS_PAGE, budget, error);
  if (rc != LC_OK)
    return rc;
  if (!p) {
    p = lc_calloc_with_allocator(NULL, 1, sizeof(*p));
    if (!p)
      return err(error, LC_ERR_NOMEM, "cannot allocate packed payload page");
    rc = take_run(s, &reserve, &p->run.offset, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(NULL, p);
      return rc;
    }
    p->run.bytes = PS_PAGE;
    ps_internal_budget_spend(budget, PS_PAGE, 0, 0);
    payload_register(s, p);
  }
  for (at = 0; at + count <= CELLS; ++at) {
    mask = cell_mask(at, count);
    if (!(p->occupied & mask)) {
      p->occupied |= mask;
      s->packed_bytes += count * PS_CELL;
      payload_available(s, p);
      extent->offset = p->run.offset + at * PS_CELL;
      extent->flags = EXTENT_PACKED;
      return LC_OK;
    }
  }
  return err(error, LC_ERR_PROTOCOL, "packed payload free-span mismatch");
}
static int metadata_offset(ps_space *s, uint64_t offset) {
  return offset % PS_PAGE == 0 && offset >= 2 * PS_PAGE &&
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
static void metadata_release(ps_space *s, ps_meta_page *p) {
  ps_meta_page **link = &s->page_buckets[(p->run.offset / PS_PAGE) % BUCKETS];
  while (*link != p)
    link = &(*link)->hash_next;
  *link = p->hash_next;
  *p->link = p->next;
  if (p->next)
    p->next->link = p->link;
  --s->metadata_pages;
  if (s->metadata_free_bytes < metadata_target(s, s->metadata_workspace))
    metadata_insert(s, &p->run);
  else
    free_insert(s, &p->run);
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
      free_insert(s, node);
    } else {
      spare->offset = node->offset;
      spare->bytes = bytes;
      spare->clean = node->clean;
      node->offset += bytes;
      node->bytes -= bytes;
      s->metadata_free_bytes -= bytes;
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
static int checkpoint_locked(ps_space *s, lc_error *error) {
  unsigned char root[PS_ROOT_BYTES], zero[PS_ROOT_BYTES] = {0}, *wire = NULL;
  ps_entry *p;
  ps_meta_page *page;
  ps_run *trim = NULL;
  uint64_t bound;
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
      s->free_bytes >= s->reserved_payload &&
      s->free_bytes - s->reserved_payload >=
          2 * bound - s->metadata_free_bytes &&
      s->free_bytes - s->reserved_payload -
              (2 * bound - s->metadata_free_bytes) >=
          PS_MAINTENANCE_RESERVE)
    rc = metadata_fill(s, bound, 2 * bound, 0, PS_MAINTENANCE_RESERVE, error);
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
  put32(root + 76, checksum(root, 76));
  if (rc == LC_OK)
    rc = transfer(s, root, sizeof(root), (uint64_t)next * PS_PAGE, 1, error);
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
  mark_durable(s, __atomic_load_n(&s->pending_sequence, __ATOMIC_ACQUIRE));
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
  if (trim)
    trim = metadata_trim(s, trim);
  lc_free_with_allocator(NULL, trim);
  for (p = s->retired; p; p = p->retired_next) {
    p->previous = NULL;
    p->safe = 1;
  }
  collect(s);
  return LC_OK;
}
static int append_event(ps_space *s, ps_entry *p, int event, lc_error *error) {
  size_t bytes = event_bytes(p, event);
  unsigned char *wire;
  int rc;
  if (s->sequence == UINT64_MAX ||
      __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED) == UINT64_MAX)
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
    __atomic_fetch_add(&s->pending_sequence, 1, __ATOMIC_RELEASE);
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
  uint64_t offset = get64(p + 32), generation = get64(p + 40);
  return get64(p) == ROOT_MAGIC && get64(p + 8) && capacity >= PS_MIB &&
         capacity <= INT64_MAX && capacity % PS_MIB == 0 &&
         ((!offset && !generation) ||
          (offset >= 2 * PS_PAGE && offset % PS_PAGE == 0 &&
           offset <= capacity - PS_PAGE &&
           (offset < JOURNAL_OFFSET || offset >= PS_DATA) && generation &&
           generation <= get64(p + 8))) &&
         get64(p + 48) && get64(p + 48) % TOKEN_RANGE == 0 && !get32(p + 72) &&
         checksum(p, 76) == get32(p + 76);
}
typedef struct metadata_recovery {
  ps_space *space;
  unsigned char *wire, *buffer;
  uint64_t loaded;
} metadata_recovery;
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
  int rc, fresh;
  while (done < ref.bytes) {
    if (!metadata_offset(s, offset))
      return err(error, LC_ERR_PROTOCOL, "metadata record outside container");
    fresh = recovery->loaded != offset;
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
static int recover_revision(ps_space *s, const unsigned char *wire,
                            size_t bytes, unsigned char *buffer,
                            lc_error *error) {
  ps_entry **entries = NULL, *old;
  size_t count, i, j, cursor = 64, member_bytes;
  int event, rc = LC_OK;
  count = get32(wire + 16);
  if (bytes < 64 || get32(wire) != REVISION_MAGIC || get32(wire + 4) != bytes ||
      !count || count > (bytes - 64) / 64 || count > UINT64_MAX - s->sequence ||
      get64(wire + 8) != s->sequence + 1 || get32(wire + 20) ||
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
    if (rc == LC_OK)
      rc = verify_entry(s, entries[i], buffer, error);
    cursor += member_bytes;
  }
  if (rc == LC_OK && cursor != bytes)
    rc = err(error, LC_ERR_PROTOCOL, "revision has trailing members");
  if (rc == LC_OK) {
    for (i = 0; i < count; ++i) {
      old = lookup(s, entries[i]->key);
      register_entry(s, entries[i]);
      dirty_entry(s, entries[i], old);
      install(s, entries[i]);
      entries[i] = NULL;
    }
    s->sequence += count;
  }
  for (i = 0; i < count; ++i)
    if (entries[i])
      entry_free(entries[i]);
  lc_free_with_allocator(NULL, entries);
  return rc;
}
static int validate_owned_directory(ps_space *s, lc_error *error) {
  ps_entry *p, *run;
  uint64_t i, base, expected = 0, actual = 0;
  ps_key key;
  key.kind = PS_OWNED_RUN;
  for (p = s->entries; p; p = p->all_next) {
    if (p->key.kind == PS_OWNED_RUN) {
      ++actual;
      if (!stream_lookup(s, p->key.owner))
        return err(error, LC_ERR_PROTOCOL, "owned run has no stream head");
    }
    if (p->key.kind != PS_STREAM)
      continue;
    key.owner = p->token;
    base = 0;
    for (i = 0; i < p->stream_runs; ++i) {
      key.key = base;
      run = lookup(s, key);
      if (!run || run->deleted ||
          run->bytes - PS_RUN_HEADER > s->capacity - base)
        return err(error, LC_ERR_PROTOCOL, "incomplete owned run directory");
      base += run->bytes - PS_RUN_HEADER;
    }
    if (base != p->stream_capacity)
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
      p->bytes - PS_RUN_HEADER > s->capacity - head->stream_capacity)
    return err(error, LC_ERR_PROTOCOL, "owned run grant is not contiguous");
  head->stream_capacity += p->bytes - PS_RUN_HEADER;
  ++head->stream_runs;
  if (!head->dirty_link)
    dirty_entry(s, head, NULL);
  return LC_OK;
}
static void mark_removed(ps_space *s, ps_entry *p, uint64_t sequence) {
  if (p->key.kind == PS_STREAM)
    stream_unregister(s, p);
  s->metadata -= entry_bytes(p);
  --s->live_entries;
  p->deleted = 1;
  p->publication_sequence = sequence;
  if (!p->dirty_link)
    dirty_entry(s, p, NULL);
}
/* Two walks, no allocation after the journal action. Each owned key belongs
 * exclusively to this head incarnation. Deleted entries retain their payload
 * and checkpoint references until the usual root-retirement protocol. */
static int remove_owned(ps_space *s, ps_entry *head, uint64_t sequence,
                        int apply, lc_error *error) {
  ps_key key;
  ps_entry *run;
  uint64_t i, base = 0;
  key.kind = PS_OWNED_RUN;
  key.owner = head->token;
  for (i = 0; i < head->stream_runs; ++i) {
    key.key = base;
    run = lookup(s, key);
    if (!run || run->deleted ||
        run->bytes - PS_RUN_HEADER > head->stream_capacity - base)
      return err(error, LC_ERR_PROTOCOL, "retired stream grant mismatch");
    base += run->bytes - PS_RUN_HEADER;
    if (apply)
      mark_removed(s, run, sequence);
  }
  if (base != head->stream_capacity)
    return err(error, LC_ERR_PROTOCOL, "retired stream capacity mismatch");
  if (apply)
    mark_removed(s, head, sequence);
  return LC_OK;
}
static int recover(ps_space *s, lc_error *error) {
  unsigned char roots[2 * PS_ROOT_BYTES], zero[PS_ROOT_BYTES] = {0}, *wire,
                                          *buffer;
  const unsigned char *root;
  ps_directory *directory = NULL;
  metadata_recovery recovery;
  ps_entry *p = NULL, *old;
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
      old = lookup(s, p->key);
      if (p->key.kind != PS_CATALOG &&
          (!immutable_key(p->key) || p->key.kind == PS_COLLECTION) && old &&
          !old->deleted) {
        entry_free(p);
        rc = err(error, LC_ERR_PROTOCOL,
                 "collection position overwritten in journal");
        goto done;
      }
      if (p->key.kind == PS_OWNED_RUN) {
        rc = install_owned_grant(s, p, error);
        if (rc != LC_OK) {
          entry_free(p);
          goto done;
        }
      } else if (p->key.kind == PS_STREAM &&
                 (p->bytes || p->stream_capacity || p->stream_runs ||
                  p->stream_sealed)) {
        entry_free(p);
        rc = err(error, LC_ERR_PROTOCOL, "nonempty initial stream head");
        goto done;
      }
      if (p->key.kind == PS_CATALOG) {
        rc = ps_catalog_prepare_record(s, p, 0, error);
        if (rc != LC_OK) {
          entry_free(p);
          goto done;
        }
        ps_catalog_apply_record(s, p, s->sequence + 1);
      }
      register_entry(s, p);
      dirty_entry(s, p, old);
      install(s, p);
    } else {
      old = lookup(s, p->key);
      if (!old || old->deleted || old->token != p->token) {
        entry_free(p);
        rc = err(error, LC_ERR_PROTOCOL, "journal retirement owner mismatch");
        goto done;
      }
      if (event == PS_SEAL) {
        if (old->stream_sealed || p->bytes < old->bytes ||
            p->stream_capacity != old->stream_capacity ||
            p->stream_runs != old->stream_runs) {
          entry_free(p);
          rc = err(error, LC_ERR_PROTOCOL, "journal stream seal mismatch");
          goto done;
        }
        old->bytes = p->bytes;
        old->stream_sealed = 1;
        if (!old->dirty_link)
          dirty_entry(s, old, NULL);
        entry_free(p);
      } else if (p->key.kind == PS_CATALOG) {
        ps_catalog_drop_record(s, old, s->sequence + 1);
        entry_free(p);
      } else if (p->key.kind == PS_STREAM) {
        rc = old->catalog_refs
                 ? err(error, LC_ERR_PROTOCOL,
                       "journal retires a catalog target")
                 : remove_owned(s, old, s->sequence + 1, 0, error);
        if (rc == LC_OK)
          rc = remove_owned(s, old, s->sequence + 1, 1, error);
        entry_free(p);
        if (rc != LC_OK)
          goto done;
      } else {
        p->deleted = 1;
        register_entry(s, p);
        dirty_entry(s, p, old);
        install(s, p);
      }
    }
    cursor += bytes;
    ++s->sequence;
  }
  s->journal = cursor;
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
    rc = transfer(s, wire + first, last - first, JOURNAL_OFFSET + first, 1,
                  error);
    if (rc != LC_OK)
      goto done;
  }
  if (memcmp(roots + (1 - s->root) * PS_ROOT_BYTES, zero, sizeof(zero))) {
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
  /* The grant reuses rejected journal sequences. Stabilize their retirement
   * before that valid event can reach storage, including an apparently clean
   * retry whose zeros came from an earlier failed recovery's file cache.
   * The final barrier below separately fences the new incarnation range. */
  rc = sync_file(s, error);
  if (rc != LC_OK)
    goto done;
  __atomic_store_n(&s->pending_sequence, s->sequence, __ATOMIC_RELEASE);
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
  uint64_t bound;
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
      rc = initial_free(s, error);
    if (rc == LC_OK)
      rc = checkpoint_locked(s, error);
    if (rc == LC_OK)
      rc = sync_parent(path, error);
  } else
    rc = recover(s, error);
  if (rc == LC_OK && s->maximum && s->capacity > s->maximum)
    rc = err(error, LC_ERR_INVALID,
             "committed capacity exceeds configured maximum");
  if (rc == LC_OK)
    rc = metadata_bound(s->metadata, s->live_entries, &bound, error);
  if (rc == LC_OK)
    rc = metadata_protect(s, bound, 0, 0, error);
  if (rc != LC_OK) {
    destroy(s);
    return rc;
  }
  mark_durable(s, __atomic_load_n(&s->pending_sequence, __ATOMIC_ACQUIRE));
  *out = s;
  return LC_OK;
}
int ps_close(ps_space *s, lc_error *error) {
  if (!s)
    return LC_OK;
  pthread_mutex_lock(&s->mutex);
  if (s->readers || s->writers || s->budgets) {
    pthread_mutex_unlock(&s->mutex);
    return err(error, LC_ERR_INVALID,
               "close container readers, writers and capacity budgets first");
  }
  pthread_mutex_unlock(&s->mutex);
  destroy(s);
  return LC_OK;
}
int ps_begin(ps_space *s, ps_budget *budget, ps_key key, ps_writer **out,
             lc_error *error) {
  ps_writer *w;
  ps_entry *p;
  int rc;
  if (out)
    *out = NULL;
  if (!s || !out || !immutable_key(key))
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
  if (rc == LC_OK && s->token_next > s->token_limit)
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
    extent = &p->extents[p->count];
    memset(extent, 0, sizeof(*extent));
    take = w->used - done;
    run_bytes = take / PS_PAGE * PS_PAGE;
    if (!run_bytes) {
      run_bytes = take;
      rc = payload_reserve(s, p, extent, take, w->budget, error);
    } else {
      rc = immutable_extent_allowance(s, p, run_bytes, w->budget, error);
      if (rc == LC_OK)
        rc = take_run(s, &run_bytes, &extent->offset, error);
      if (rc == LC_OK)
        ps_internal_budget_spend(w->budget, run_bytes, 0, 0);
    }
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
  int rc, durable;
  if (!w || w->failed || w->published)
    return err(error, LC_ERR_INVALID, "container writer cannot publish");
  rc = flush_writer(w, error);
  if (rc != LC_OK) {
    w->failed = 1;
    return rc;
  }
  s = w->space;
  durable = s->durable || w->entry->key.kind == PS_CONTROL;
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  old = lookup(s, w->entry->key);
  prior = old && !old->deleted ? entry_bytes(old) : 0;
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
    sequence = w->entry->publication_sequence;
    memcpy(published_ref.container, s->identity, 16);
    published_ref.incarnation = w->entry->token;
  } else
    w->failed = 1;
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
                        int force_durable, ps_ref *refs, lc_error *error) {
  ps_space *s;
  ps_budget *budget;
  ps_entry *p, *old;
  unsigned char *wire = NULL;
  size_t i, j, bytes = 64, cursor, prior, descriptor;
  uint64_t metadata, entries, growth = 0, added = 0, sequence = 0;
  int rc = LC_OK, durable;
  if (!writers || !count || count > (PS_JOURNAL - 64) / 64 || !writers[0] ||
      (force_durable != 0 && force_durable != 1))
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
    wire = lc_alloc_with_allocator(NULL, bytes);
    if (!wire)
      rc = err(error, LC_ERR_NOMEM, "cannot allocate revision publication");
  }
  if (rc != LC_OK)
    goto done;
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc == LC_OK && (count > UINT64_MAX - s->sequence ||
                      count > UINT64_MAX - __atomic_load_n(&s->pending_sequence,
                                                           __ATOMIC_RELAXED)))
    rc = err(error, LC_ERR_INVALID, "revision publication sequence exhausted");
  if (rc == LC_OK && bytes > PS_JOURNAL - s->journal)
    rc = checkpoint_locked(s, error);
  metadata = s->metadata;
  entries = s->live_entries;
  durable = s->durable || force_durable;
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
      durable = 1;
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
    cursor = 64;
    for (i = 0; i < count; ++i) {
      encode_entry(wire + cursor, writers[i]->entry, s->sequence + 1 + i,
                   PS_PUT);
      cursor += entry_bytes(writers[i]->entry);
    }
    put32(wire + 60,
          (uint32_t)crc32(checksum(wire, 60), wire + 64, (uInt)(bytes - 64)));
    rc = transfer(s, wire, bytes, JOURNAL_OFFSET + s->journal, 1, error);
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
  }
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK && durable)
    rc = wait_durable(s, sequence, error);
done:
  if (rc != LC_OK)
    for (i = 0; i < count; ++i)
      writers[i]->failed = 1;
  lc_free_with_allocator(NULL, wire);
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
        release_entry_runs(s, w->entry, NULL) != LC_OK)
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
  ps_entry *p;
  uint64_t visible;
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
  rc = healthy(s, error);
  p = lookup(s, key);
  visible = s->durable || key.kind == PS_CONTROL || (p && p->durable_visibility)
                ? __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE)
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
static void reader_payload_invalid(ps_reader *r) {
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
    while (p && p->publication_sequence > prefix)
      p = p->previous;
  return p && !p->deleted ? p : NULL;
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
  unsigned char *buffer = NULL;
  int rc, checked = 0;
  if (!s || !valid || !immutable_key(key))
    return err(error, LC_ERR_INVALID, "value validation requires typed output");
  *valid = 0;
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
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
    ++p->pins;
    ++s->readers;
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
  --p->pins;
  --s->readers;
  if (p->state == 2 && p->safe && !p->pins)
    collect(s);
  pthread_mutex_unlock(&s->mutex);
  lc_free_with_allocator(NULL, buffer);
  return rc;
}
int ps_values_visit(ps_space *s, uint64_t owner, unsigned int first,
                    unsigned int last, ps_value_visit_fn visit, void *context,
                    lc_error *error) {
  ps_entry **captured = NULL, *p, *selected;
  ps_ref ref;
  uint64_t prefix;
  size_t i, n = 0, at = 0;
  int rc;
  if (!s || !visit || first > last)
    return err(error, LC_ERR_INVALID, "value visit requires owner and kinds");
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  prefix = __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE);
  for (i = 0; rc == LC_OK && i < BUCKETS; ++i)
    for (p = s->buckets[i]; p; p = p->hash_next)
      if (p->key.owner == owner && p->key.kind >= first &&
          p->key.kind <= last && immutable_key(p->key) &&
          selected_value(s, p, prefix))
        ++n;
  if (rc == LC_OK && n) {
    if (n <= (size_t)-1 / sizeof(*captured))
      captured = lc_calloc_with_allocator(NULL, n, sizeof(*captured));
    if (!captured)
      rc = err(error, LC_ERR_NOMEM, "cannot capture selected value metadata");
  }
  for (i = 0; rc == LC_OK && i < BUCKETS; ++i)
    for (p = s->buckets[i]; p; p = p->hash_next)
      if (p->key.owner == owner && p->key.kind >= first &&
          p->key.kind <= last && immutable_key(p->key) &&
          (selected = selected_value(s, p, prefix)) != NULL) {
        captured[at++] = selected;
        ++selected->pins;
        ++s->readers;
      }
  pthread_mutex_unlock(&s->mutex);
  for (i = 0; rc == LC_OK && i < at; ++i) {
    memcpy(ref.container, s->identity, sizeof(ref.container));
    ref.incarnation = captured[i]->token;
    rc = visit(context, captured[i]->key, &ref, captured[i]->bytes, error);
  }
  pthread_mutex_lock(&s->mutex);
  for (i = 0; i < at; ++i) {
    --captured[i]->pins;
    --s->readers;
  }
  collect(s);
  pthread_mutex_unlock(&s->mutex);
  lc_free_with_allocator(NULL, captured);
  return rc;
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
static int delete_value(ps_space *s, ps_key key, const ps_ref *expected,
                        int missing_ok, lc_error *error) {
  ps_entry *p, *tombstone;
  int rc, matched;
  uint64_t sequence = 0;
  if (!s || !immutable_key(key))
    return err(error, LC_ERR_INVALID, "container delete requires typed key");
  tombstone = lc_calloc_with_allocator(NULL, 1, sizeof(*tombstone));
  if (!tombstone)
    return err(error, LC_ERR_NOMEM, "cannot allocate pending retirement");
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
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
  out->reserved_metadata_bytes = s->reserved_metadata;
  out->reserved_entries = s->reserved_entries;
  out->budgets = s->budgets;
  out->directory_page_writes = s->directory_page_writes;
  out->descriptor_page_writes = s->descriptor_page_writes;
  out->packed_pages = s->packed_pages;
  out->packed_bytes = s->packed_bytes;
  out->allocated_bytes = s->packed_pages * (uint64_t)PS_PAGE;
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
  out->sealed = head->entry->stream_sealed;
  out->retired = head->entry->deleted;
}
int ps_owned_open(ps_space *s, ps_budget *budget, ps_key key, int create,
                  const ps_ref *ref, ps_owned_head **out,
                  ps_owned_status *status, lc_error *error) {
  ps_owned_head *head;
  ps_entry *p = NULL, *old;
  uint64_t sequence = 0;
  int rc;
  if (out)
    *out = NULL;
  if (!s || !out || !status || key.kind != PS_STREAM || (create && ref))
    return err(error, LC_ERR_INVALID, "owned stream open arguments invalid");
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
  rc = healthy(s, error);
  old = lookup(s, key);
  if (rc == LC_OK && create && old &&
      (!old->deleted ||
       old->publication_sequence >
           __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE)))
    rc = err(error, LC_ERR_INVALID, "owned stream already exists");
  if (rc == LC_OK && create && s->token_next > s->token_limit)
    rc = err(error, LC_ERR_INVALID, "incarnation grant consumed; reopen");
  if (rc == LC_OK && create)
    rc = checkpoint_allowance_floor(s, s->metadata + 80, s->live_entries + 1, 0,
                                    PS_MAINTENANCE_RESERVE, budget, error);
  if (rc == LC_OK && create) {
    p->token = s->token_next++;
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
  if (rc == LC_OK && (!p || p->deleted || p->stream_open))
    rc = err(error, LC_ERR_INVALID,
             "owned stream missing or already has an open owner");
  if (rc == LC_OK && ref &&
      (memcmp(ref->container, s->identity, 16) || ref->incarnation != p->token))
    rc = err(error, LC_ERR_INVALID, "stale or foreign owned stream reference");
  if (rc == LC_OK) {
    head->space = s;
    head->entry = p;
    p->stream_open = 1;
    ++p->pins;
    ++s->readers;
    owned_status(head, status);
  } else if (create && p->state == 0)
    entry_free(p);
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
void ps_owned_close(ps_owned_head *head) {
  ps_space *s;
  if (!head)
    return;
  s = head->space;
  pthread_mutex_lock(&s->mutex);
  head->entry->stream_open = 0;
  head->entry->orphan_claimed = 0;
  --head->entry->pins;
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
  return checkpoint_allowance_floor(s, s->metadata + 88, s->live_entries + 1,
                                    payload, PS_MAINTENANCE_RESERVE, budget,
                                    error);
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
  ps_run **choice = NULL, **link, **partial, **fit;
  ps_run *spare = NULL;
  int rc, clean = 0, registered = 0;
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
  rc = healthy(s, error);
  if (rc == LC_OK && (head->entry->deleted || head->entry->stream_sealed))
    rc = err(error, LC_ERR_INVALID, "owned stream cannot accept grants");
  if (rc == LC_OK &&
      (base != head->entry->stream_capacity || s->token_next > s->token_limit))
    rc = err(error, LC_ERR_INVALID, "owned grant base or incarnation invalid");
  /* Protect metadata before retaining a free-list selection: admission may
   * transfer common intervals into the checkpoint-only pool. */
  if (rc == LC_OK)
    rc = owned_allowance(s, budget, 0, error);
  if (rc == LC_OK) {
    for (;;) {
      partial = fit = NULL;
      choice = NULL;
      for (link = &s->free; *link; link = &(*link)->next) {
        if ((*link)->clean && (*link)->bytes >= bytes) {
          choice = link;
          break;
        }
        if ((*link)->clean && !partial)
          partial = link;
        if ((*link)->bytes >= bytes && !fit)
          fit = link;
      }
      if (!choice)
        choice = partial ? partial : fit ? fit : &s->free;
      if (!*choice) {
        rc = err(error, LC_ERR_INVALID,
                 "owned payload capacity exhausted; grow");
        break;
      }
      if (bytes > (*choice)->bytes)
        bytes = (*choice)->bytes;
      rc = owned_allowance(s, budget, bytes, error);
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
    if (!(*choice)->clean && bytes < PS_MIB) {
      reserved = (*choice)->bytes < PS_MIB ? (*choice)->bytes : PS_MIB;
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
      if (reserved > bytes) {
        spare = lc_calloc_with_allocator(NULL, 1, sizeof(*spare));
        if (!spare)
          rc = err(error, LC_ERR_NOMEM,
                   "cannot allocate prepared free interval");
      }
    }
  }
  if (rc == LC_OK) {
    take_selected(choice, &s->free_bytes, reserved, &p->extents[0].offset,
                  &clean);
    ps_internal_budget_spend(budget, bytes, 0, 0);
  }
  if (rc == LC_OK) {
    p->token = s->token_next++;
    p->bytes = p->extents[0].bytes = reserved; /* entire private preparation */
    p->count = p->capacity = 1;
    p->extents[0].flags = EXTENT_OWNED;
    register_entry(s, p);
    ++s->writers;
    if (budget)
      ++budget->users;
    registered = 1;
  }
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK && !clean)
    rc = initialize_range(s, p->extents[0].offset, reserved, zero, error);
  if (rc == LC_OK) {
    put64(header, OWNED_MAGIC);
    put64(header + 8, p->token);
    put64(header + 16, p->key.owner);
    put64(header + 24, base);
    put64(header + 32, p->extents[0].offset);
    put64(header + 40, bytes);
    memcpy(header + 48, s->identity, 16);
    p->extents[0].crc = checksum(header, sizeof(header));
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
    rc = healthy(s, error);
  if (rc == LC_OK) {
    p->bytes = p->extents[0].bytes = bytes;
    if (spare) {
      spare->offset = p->extents[0].offset + bytes;
      spare->bytes = reserved - bytes;
      spare->clean = 1;
      free_insert(s, spare); /* only fully synchronized zeros become reusable */
      spare = NULL;
    }
  }
  /* Preparation released allocator authority. Recheck against peer metadata
   * growth before installing this new grant's descriptor. Its payload is
   * already privately owned, so no second payload charge is required. */
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
    ps_internal_budget_spend(budget, 0, 88, 1);
    p->publication_sequence =
        __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED);
    dirty_entry(s, p, NULL);
    install(s, p);
    ++p->pins;
    ++s->readers;
    run->space = s;
    run->entry = p;
  } else {
    if (registered) {
      if (!__atomic_load_n(&s->poisoned, __ATOMIC_RELAXED) &&
          release_entry_runs(s, p, NULL) != LC_OK)
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
  pthread_mutex_unlock(&s->mutex);
  lc_free_with_allocator(NULL, zero);
  lc_free_with_allocator(NULL, spare);
  return rc;
}
int ps_owned_grant(ps_owned_head *head, ps_budget *budget, uint64_t base,
                   uint64_t desired, ps_owned_run **out, int *found,
                   lc_error *error) {
  ps_owned_run *run;
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
    rc = healthy(s, error);
    p = lookup(s, key);
    if (rc == LC_OK && p && !p->deleted) {
      ++p->pins;
      ++s->readers;
      run->space = s;
      run->entry = p;
    }
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
  return run ? run->entry->bytes - PS_RUN_HEADER : 0;
}
void ps_owned_run_close(ps_owned_run *run) {
  ps_space *s;
  if (!run)
    return;
  s = run->space;
  pthread_mutex_lock(&s->mutex);
  --run->entry->pins;
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
int ps_owned_clear(ps_owned_run *run, uint64_t offset, uint64_t bytes,
                   void *scratch, lc_error *error) {
  int rc;
  if (!run || !scratch || offset > ps_owned_run_capacity(run) ||
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
static void owned_publish_tail(ps_space *s, ps_owned_head *head, uint64_t tail,
                               uint64_t sequence) {
  head->entry->bytes = tail;
  if (!head->entry->dirty_link)
    dirty_entry(s, head->entry, NULL);
  head->entry->stream_ready = sequence;
}
int ps_owned_commit(ps_owned_head *head, uint64_t tail,
                    enum ps_commit_mode mode, ps_commit_publish_fn publish,
                    void *context, lc_error *error) {
  ps_space *s;
  uint64_t sequence = 0;
  int rc;
  if (!head)
    return err(error, LC_ERR_INVALID, "owned commit requires head");
  if (mode != PS_COMMIT_DEFAULT && mode != PS_COMMIT_DEFER_SYNC)
    return err(error, LC_ERR_INVALID, "invalid canonical commit mode");
  s = head->space;
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc == LC_OK &&
      (head->entry->deleted || head->entry->stream_sealed ||
       tail < head->entry->bytes || tail > head->entry->stream_capacity))
    rc = err(error, LC_ERR_INVALID, "owned commit tail outside append range");
  if (rc == LC_OK &&
      __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED) == UINT64_MAX) {
    __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
    rc = err(error, LC_ERR_TRANSPORT, "owned readiness exhausted; reopen");
  }
  if (rc == LC_OK) {
    sequence = __atomic_add_fetch(&s->pending_sequence, 1, __ATOMIC_RELEASE);
    if (!publish)
      owned_publish_tail(s, head, tail, sequence);
  }
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK && publish) {
    rc = publish(context, tail, error);
    pthread_mutex_lock(&s->mutex);
    if (rc == LC_OK)
      rc = healthy(s, error);
    if (rc == LC_OK && s->durable &&
        __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE) < sequence) {
      __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
      rc = err(error, LC_ERR_PROTOCOL,
               "logical publication did not cover prepared canonical bytes");
    }
    if (rc == LC_OK)
      owned_publish_tail(s, head, tail, sequence);
    pthread_mutex_unlock(&s->mutex);
  } else if (rc == LC_OK && s->durable && mode == PS_COMMIT_DEFAULT)
    rc = wait_durable(s, sequence, error);
  return rc;
}
int ps_owned_barrier(ps_owned_head *head, lc_error *error) {
  return head ? sync_file(head->space, error)
              : err(error, LC_ERR_INVALID, "owned barrier requires head");
}
int ps_owned_recovered(ps_owned_head *head, uint64_t tail, lc_error *error) {
  ps_space *s;
  uint64_t sequence = 0;
  int rc;
  if (!head)
    return err(error, LC_ERR_INVALID, "owned recovery requires head");
  s = head->space;
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc == LC_OK &&
      (head->entry->deleted || head->entry->stream_sealed ||
       tail < head->entry->bytes || tail > head->entry->stream_capacity))
    rc = err(error, LC_ERR_PROTOCOL, "recovered tail outside owned range");
  if (rc == LC_OK &&
      __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED) == UINT64_MAX) {
    __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
    rc = err(error, LC_ERR_TRANSPORT, "recovery readiness exhausted; reopen");
  }
  if (rc == LC_OK)
    sequence = __atomic_add_fetch(&s->pending_sequence, 1, __ATOMIC_RELEASE);
  pthread_mutex_unlock(&s->mutex);
  if (rc == LC_OK)
    rc = wait_durable(s, sequence, error);
  pthread_mutex_lock(&s->mutex);
  if (rc == LC_OK)
    rc = healthy(s, error);
  if (rc == LC_OK) {
    head->entry->bytes = tail;
    if (!head->entry->dirty_link)
      dirty_entry(s, head->entry, NULL);
  }
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_owned_seal(ps_owned_head *head, lc_error *error) {
  ps_space *s;
  ps_entry snapshot;
  uint64_t sequence = 0;
  int rc, sealed, needs_sync;
  if (!head)
    return err(error, LC_ERR_INVALID, "owned seal requires head");
  s = head->space;
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  sealed = head->entry->stream_sealed;
  needs_sync = head->entry->stream_ready >
               __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE);
  if (rc == LC_OK && head->entry->deleted)
    rc = err(error, LC_ERR_INVALID, "retired stream cannot be sealed");
  pthread_mutex_unlock(&s->mutex);
  if (rc != LC_OK || sealed)
    return rc;
  /* Any valid seal event may recover before its acknowledgement. Its final
   * canonical tail must already be durable, even if journal bytes reorder. */
  if (needs_sync)
    rc = sync_file(s, error);
  pthread_mutex_lock(&s->mutex);
  if (rc == LC_OK)
    rc = healthy(s, error);
  if (rc == LC_OK) {
    snapshot = *head->entry;
    snapshot.stream_sealed = 1;
    rc = append_event(s, &snapshot, PS_SEAL, error);
  }
  if (rc == LC_OK) {
    head->entry->stream_sealed = 1;
    if (!head->entry->dirty_link)
      dirty_entry(s, head->entry, NULL);
    sequence = __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED);
  }
  pthread_mutex_unlock(&s->mutex);
  return rc == LC_OK ? wait_durable(s, sequence, error) : rc;
}
int ps_owned_claim_orphan(ps_owned_head *head, lc_error *error) {
  ps_space *s;
  int rc;
  if (!head)
    return err(error, LC_ERR_INVALID,
               "orphan claim requires an owned identity");
  s = head->space;
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc == LC_OK &&
      (head->entry->deleted || head->entry->catalog_refs ||
       head->entry->catalog_pins ||
       head->entry->catalog_release >
           __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE)))
    rc = err(error, LC_ERR_INVALID,
             "owned identity is retained by a catalog binding or view");
  if (rc == LC_OK)
    head->entry->orphan_claimed = 1;
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_owned_retire(ps_owned_head *head, lc_error *error) {
  ps_space *s;
  uint64_t sequence = 0;
  int rc;
  if (!head)
    return err(error, LC_ERR_INVALID, "owned retirement requires head");
  s = head->space;
  pthread_mutex_lock(&s->mutex);
  rc = healthy(s, error);
  if (rc == LC_OK && !head->entry->deleted &&
      (head->entry->catalog_refs || head->entry->catalog_pins ||
       head->entry->catalog_release >
           __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE)))
    rc = err(error, LC_ERR_INVALID,
             "stream is retained by a catalog binding or view");
  if (rc == LC_OK && !head->entry->deleted)
    rc = remove_owned(s, head->entry, 0, 0, error);
  if (rc == LC_OK && !head->entry->deleted) {
    rc = append_event(s, head->entry, PS_REMOVE, error);
    if (rc == LC_OK) {
      sequence = __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED);
      rc = remove_owned(s, head->entry, sequence, 1, error);
      if (rc != LC_OK)
        __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELAXED);
    }
  } else if (rc == LC_OK)
    sequence = head->entry->publication_sequence;
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
