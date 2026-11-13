#include "directory.h"

#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

#define PAGES 2560U
#define KEYS 8192U
static lc_error error;
static size_t checks, allocation_at, allocation_calls, allocation_largest;
static int count_allocations;

void *__real_lc_alloc_with_allocator(const lc_allocator *, size_t);
void *__real_lc_calloc_with_allocator(const lc_allocator *, size_t, size_t);
static int allocation_fault(size_t bytes) {
  if (!count_allocations)
    return 0;
  if (bytes > allocation_largest)
    allocation_largest = bytes;
  return ++allocation_calls == allocation_at;
}
void *__wrap_lc_alloc_with_allocator(const lc_allocator *a, size_t bytes) {
  return allocation_fault(bytes) ? NULL
                                 : __real_lc_alloc_with_allocator(a, bytes);
}
void *__wrap_lc_calloc_with_allocator(const lc_allocator *a, size_t n,
                                      size_t bytes) {
  return allocation_fault(n * bytes)
             ? NULL
             : __real_lc_calloc_with_allocator(a, n, bytes);
}
#define CHECK(x)                                                               \
  do {                                                                         \
    ++checks;                                                                  \
    if (!(x)) {                                                                \
      fprintf(stderr, "%s:%d: %s (%s)\n", __FILE__, __LINE__, #x,              \
              error.message ? error.message : "no error");                     \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)
#define OK(x) CHECK((x) == LC_OK)
#define BAD(x)                                                                 \
  do {                                                                         \
    CHECK((x) != LC_OK);                                                       \
    lc_error_cleanup(&error);                                                  \
  } while (0)

typedef struct pager {
  unsigned char *image, *used;
  size_t operations, fail_at, reads, writes, allocations;
} pager;
static int storage_error(lc_error *e) {
  return lc_error_set(e, LC_ERR_TRANSPORT, 0, "injected directory page failure",
                      NULL, NULL, "directory-test");
}
static int io_fault(pager *p) { return ++p->operations == p->fail_at; }
static int read_page(void *v, uint64_t offset, void *out, lc_error *e) {
  pager *p = v;
  ++p->reads;
  if (io_fault(p))
    return storage_error(e);
  CHECK(offset && offset % PS_PAGE == 0 && offset / PS_PAGE < PAGES);
  CHECK(p->used[offset / PS_PAGE]);
  memcpy(out, p->image + offset, PS_PAGE);
  return LC_OK;
}
static int allocate_page(void *v, uint64_t *out, lc_error *e) {
  pager *p = v;
  size_t i;
  if (io_fault(p))
    return storage_error(e);
  for (i = 1; i < PAGES; ++i)
    if (!p->used[i]) {
      p->used[i] = 1;
      ++p->allocations;
      *out = i * PS_PAGE;
      return LC_OK;
    }
  return lc_error_set(e, LC_ERR_INVALID, 0, "directory page capacity exhausted",
                      NULL, NULL, "directory-test");
}
static int write_page(void *v, uint64_t offset, const void *bytes,
                      lc_error *e) {
  pager *p = v;
  ++p->writes;
  CHECK(offset && offset % PS_PAGE == 0 && offset / PS_PAGE < PAGES);
  CHECK(p->used[offset / PS_PAGE]);
  if (io_fault(p)) {
    memcpy(p->image + offset, bytes, PS_PAGE / 2); /* persist a torn page */
    return storage_error(e);
  }
  memcpy(p->image + offset, bytes, PS_PAGE);
  return LC_OK;
}
static ps_directory_io pager_init(pager *p) {
  ps_directory_io io = {0};
  memset(p, 0, sizeof(*p));
  p->image = calloc(PAGES, PS_PAGE);
  p->used = calloc(PAGES, 1);
  CHECK(p->image && p->used);
  io.context = p;
  memset(io.identity, 0x93, sizeof(io.identity));
  io.read = read_page;
  io.allocate = allocate_page;
  io.write = write_page;
  return io;
}
static void pager_clear(pager *p) {
  memset(p->image, 0, PAGES * PS_PAGE);
  memset(p->used, 0, PAGES);
  p->operations = p->fail_at = p->reads = p->writes = p->allocations = 0;
}
static void pager_close(pager *p) {
  free(p->image);
  free(p->used);
}
static size_t used_pages(pager *p) {
  size_t i, used = 0;
  for (i = 1; i < PAGES; ++i)
    used += p->used[i] != 0;
  return used;
}
static void release_pages(pager *p, const ps_directory_root *refs, size_t n) {
  size_t i;
  for (i = 0; i < n; ++i) {
    CHECK(refs[i].offset && refs[i].offset % PS_PAGE == 0 &&
          refs[i].offset / PS_PAGE < PAGES);
    CHECK(p->used[refs[i].offset / PS_PAGE]);
    p->used[refs[i].offset / PS_PAGE] = 0;
  }
}
static ps_key key(uint64_t number) {
  ps_key result = {0, number, PS_STATE};
  return result;
}
static ps_directory_value value(uint64_t number, uint64_t incarnation) {
  ps_directory_value result = {PS_DATA + number * 128, 64, incarnation};
  return result;
}
static ps_directory *open_directory(ps_directory_io *io,
                                    ps_directory_root root) {
  ps_directory *d = NULL;
  OK(ps_directory_open(io, root, &d, &error));
  return d;
}
static void expect(ps_directory *d, uint64_t number, uint64_t incarnation) {
  ps_directory_value actual, wanted = value(number, incarnation);
  int found = -1;
  OK(ps_directory_get(d, key(number), &actual, &found, &error));
  CHECK(found == (incarnation != 0));
  if (incarnation) {
    CHECK(actual.offset == wanted.offset && actual.bytes == wanted.bytes);
    CHECK(actual.incarnation == incarnation);
  }
}
typedef struct scan {
  const uint64_t *expected;
  size_t count, previous;
  pager *storage;
  unsigned char *seen;
  size_t pages;
} scan;
static int scan_page(void *v, ps_directory_root ref, lc_error *e) {
  scan *s = v;
  size_t index = (size_t)(ref.offset / PS_PAGE);
  (void)e;
  CHECK(ref.offset && ref.offset % PS_PAGE == 0 && index < PAGES);
  CHECK(ref.generation && s->storage->used[index] && !s->seen[index]);
  s->seen[index] = 1;
  ++s->pages;
  return LC_OK;
}
static int scan_entry(void *v, ps_key k, ps_directory_value actual,
                      lc_error *e) {
  scan *s = v;
  ps_directory_value wanted;
  (void)e;
  CHECK(k.kind == PS_STATE && k.owner == 0 && k.key < KEYS);
  CHECK(!s->count || s->previous < k.key);
  CHECK(s->expected[k.key] != 0);
  wanted = value(k.key, s->expected[k.key]);
  CHECK(actual.offset == wanted.offset && actual.bytes == wanted.bytes &&
        actual.incarnation == wanted.incarnation);
  s->previous = (size_t)k.key;
  ++s->count;
  return LC_OK;
}
static void verify(ps_directory_io *io, ps_directory_root root,
                   const uint64_t *expected) {
  ps_directory *d = open_directory(io, root);
  scan s = {0};
  size_t i, count = 0;
  s.expected = expected;
  s.storage = io->context;
  s.seen = calloc(PAGES, 1);
  CHECK(s.seen != NULL);
  for (i = 0; i < KEYS; ++i) {
    expect(d, i, expected[i]);
    count += expected[i] != 0;
  }
  OK(ps_directory_visit(d, scan_entry, scan_page, &s, &error));
  CHECK(s.count == count);
  CHECK(s.pages <= ps_directory_page_bound(count));
  CHECK((s.pages != 0) == (root.offset != 0));
  free(s.seen);
  ps_directory_close(d);
}
static ps_directory_root flush(pager *p, ps_directory *d, uint64_t generation,
                               int retire) {
  ps_directory_root root;
  ps_directory_pages pages;
  OK(ps_directory_flush(d, generation, &root, &error));
  ps_directory_changed_pages(d, &pages);
  CHECK(pages.allocated_count <= p->allocations);
  if (retire)
    release_pages(p, pages.retired, pages.retired_count);
  ps_directory_close(d);
  return root;
}
static ps_directory_root seed(ps_directory_io *io, pager *p,
                              uint64_t *expected) {
  ps_directory_root empty = {0};
  ps_directory *d = open_directory(io, empty);
  size_t i, k;
  for (i = 0; i < KEYS; ++i) {
    k = (i * 4051U) %
        KEYS; /* a full permutation, repeatedly splits both sides */
    expected[k] = k + 1;
    OK(ps_directory_put(d, key(k), value(k, expected[k]), &error));
  }
  return flush(p, d, 1, 1);
}

static void growth_and_copy_on_write(void) {
  pager p;
  ps_directory_io io = pager_init(&p);
  ps_directory_root root, next;
  ps_directory_pages pages;
  ps_directory *d, *old;
  uint64_t *expected = calloc(KEYS, sizeof(*expected));
  size_t original_pages, i;
  CHECK(expected != NULL);
  root = seed(&io, &p, expected);
  CHECK(used_pages(&p) > 16); /* crosses the prototype's 64 KiB ceiling */
  verify(&io, root, expected);
  original_pages = used_pages(&p);
  {
    scan ownership = {0};
    ownership.storage = &p;
    ownership.seen = calloc(PAGES, 1);
    CHECK(ownership.seen != NULL);
    d = open_directory(&io, root);
    OK(ps_directory_visit(d, NULL, scan_page, &ownership, &error));
    CHECK(ownership.pages == original_pages);
    ps_directory_close(d);
    free(ownership.seen);
  }
  p.reads = p.writes = 0;
  d = open_directory(&io, root);
  allocation_largest = allocation_calls = allocation_at = 0;
  count_allocations = 1;
  expect(d, 4111, expected[4111]);
  count_allocations = 0;
  CHECK(p.reads <= 4 && allocation_largest <= PS_PAGE + 256);
  OK(ps_directory_put(d, key(4111), value(4111, UINT64_C(999999)), &error));
  OK(ps_directory_flush(d, 2, &next, &error));
  ps_directory_changed_pages(d, &pages);
  CHECK(pages.allocated_count >= 2 && pages.allocated_count <= 4);
  CHECK(pages.retired_count == pages.allocated_count);
  CHECK(p.writes == pages.allocated_count);
  printf("directory I/O: %u entries, %zu live pages, %zu pages per single-key "
         "checkpoint\n",
         KEYS, original_pages, pages.allocated_count);
  old = open_directory(&io, root);
  expect(old, 4111,
         expected[4111]); /* flush alone never retires the old root */
  ps_directory_close(old);
  expected[4111] = UINT64_C(999999);
  verify(&io, next, expected);
  release_pages(&p, pages.retired, pages.retired_count);
  ps_directory_close(d);
  CHECK(used_pages(&p) == original_pages);
  root = next;
  d = open_directory(&io, root);
  p.writes = 0;
  for (i = 2000; i < 2064; ++i) {
    expected[i] += UINT64_C(1000000);
    OK(ps_directory_put(d, key(i), value(i, expected[i]), &error));
  }
  root = flush(&p, d, 3, 1);
  CHECK(p.writes <= 12); /* dirty paths are written once per checkpoint */
  verify(&io, root, expected);
  p.writes = p.allocations = 0;
  d = open_directory(&io, root);
  OK(ps_directory_put(d, key(2000), value(2000, expected[2000]), &error));
  {
    int removed;
    OK(ps_directory_remove(d, key(KEYS), &removed, &error));
    CHECK(removed == 0);
  }
  next = flush(&p, d, 4, 1);
  CHECK(root.offset == next.offset && root.generation == next.generation);
  CHECK(p.writes == 0 && p.allocations == 0);
  free(expected);
  pager_close(&p);
  puts(
      "directory: growable pages, bounded lookup and changed-path checkpoints");
}

static void deletion_and_churn(void) {
  pager p;
  ps_directory_io io = pager_init(&p);
  ps_directory_root root;
  ps_directory *d;
  uint64_t *expected = calloc(KEYS, sizeof(*expected));
  uint64_t random = UINT64_C(7654321), generation = 2;
  size_t i, k;
  int removed;
  CHECK(expected != NULL);
  root = seed(&io, &p, expected);
  d = open_directory(&io, root);
  for (i = 0; i < KEYS; ++i) {
    k = (i * 4051U) % KEYS;
    OK(ps_directory_remove(d, key(k), &removed, &error));
    CHECK(removed == 1);
    expected[k] = 0;
    if (i % 127 == 126) {
      root = flush(&p, d, generation++, 1);
      verify(&io, root, expected);
      d = open_directory(&io, root);
    }
  }
  expect(d, 0, 0); /* deleting the final entry must not reload the old root */
  root = flush(&p, d, generation++, 1);
  CHECK(root.offset == 0 && root.generation == 0 && used_pages(&p) == 0);
  d = open_directory(&io, root);
  for (i = 0; i < 12000; ++i) {
    random = random * UINT64_C(6364136223846793005) + 1;
    k = (size_t)((random >> 17) % KEYS);
    if (random & UINT64_C(0x100)) {
      expected[k] = i + 1;
      OK(ps_directory_put(d, key(k), value(k, expected[k]), &error));
    } else {
      OK(ps_directory_remove(d, key(k), &removed, &error));
      CHECK(removed == (expected[k] != 0));
      expected[k] = 0;
    }
    if (i % 113 == 112) {
      root = flush(&p, d, generation++, 1);
      CHECK(used_pages(&p) < 300);
      verify(&io, root, expected);
      d = open_directory(&io, root);
    }
  }
  root = flush(&p, d, generation, 1);
  verify(&io, root, expected);
  free(expected);
  pager_close(&p);
  puts(
      "directory: merges, redistribution, empty roots and deterministic churn");
}

static int change(ps_directory *d) {
  int removed, rc;
  rc = ps_directory_put(d, key(0), value(0, UINT64_C(700000)), &error);
  if (rc == LC_OK)
    rc = ps_directory_put(d, key(KEYS - 1), value(KEYS - 1, UINT64_C(700001)),
                          &error);
  if (rc == LC_OK)
    rc = ps_directory_remove(d, key(KEYS / 2), &removed, &error);
  return rc;
}
static void failure_boundaries(void) {
  pager p;
  ps_directory_io io = pager_init(&p);
  ps_directory_root root, next;
  ps_directory_pages pages;
  ps_directory *d = NULL;
  uint64_t *expected = calloc(KEYS, sizeof(*expected));
  unsigned char *saved_image = malloc(PAGES * PS_PAGE),
                *saved_used = malloc(PAGES);
  size_t phase, count, initial_pages;
  int rc, kind;
  CHECK(expected && saved_image && saved_used);
  root = seed(&io, &p, expected);
  memcpy(saved_image, p.image, PAGES * PS_PAGE);
  memcpy(saved_used, p.used, PAGES);
  initial_pages = used_pages(&p);
  for (kind = 0; kind < 2; ++kind) {
    count = 0;
    for (phase = 0; phase <= count; ++phase) {
      memcpy(p.image, saved_image, PAGES * PS_PAGE);
      memcpy(p.used, saved_used, PAGES);
      p.operations = p.reads = p.writes = p.allocations = 0;
      p.fail_at = kind == 0 ? phase : 0;
      allocation_calls = 0;
      allocation_at = kind == 1 ? phase : 0;
      count_allocations = kind == 1;
      rc = ps_directory_open(&io, root, &d, &error);
      if (rc == LC_OK)
        rc = change(d);
      if (rc == LC_OK)
        rc = ps_directory_flush(d, 2, &next, &error);
      count_allocations = 0;
      allocation_at = 0;
      if (phase == 0)
        count = kind == 0 ? p.operations : allocation_calls;
      else
        CHECK(rc != LC_OK);
      p.fail_at = 0;
      if (rc != LC_OK)
        lc_error_cleanup(&error);
      if (rc != LC_OK && d) {
        size_t operations = p.operations;
        BAD(ps_directory_flush(d, 3, &next, &error));
        CHECK(p.operations == operations);
      }
      ps_directory_changed_pages(d, &pages);
      release_pages(&p, pages.allocated, pages.allocated_count);
      /* Abort before root publication: every old page is still owned. */
      ps_directory_close(d);
      d = NULL;
      CHECK(used_pages(&p) == initial_pages);
      verify(&io, root, expected);
    }
    CHECK(count > 10);
  }
  memcpy(p.image, saved_image, PAGES * PS_PAGE);
  memcpy(p.used, saved_used, PAGES);
  d = open_directory(&io, root);
  OK(change(d));
  memset(p.used, 1, PAGES); /* genuine capacity exhaustion, no fallback */
  BAD(ps_directory_flush(d, 2, &next, &error));
  ps_directory_changed_pages(d, &pages);
  CHECK(pages.allocated_count == 0);
  BAD(ps_directory_put(d, key(1), value(1, 1), &error));
  ps_directory_close(d);
  free(saved_image);
  free(saved_used);
  free(expected);
  pager_close(&p);
  puts("directory: every read/allocation/torn-write and heap failure preserves "
       "old root");
}

static uint64_t disk64(const unsigned char *p) {
  uint64_t result = 0;
  unsigned int i;
  for (i = 0; i < 8; ++i)
    result |= (uint64_t)p[i] << (8U * i);
  return result;
}
static void disk32(unsigned char *p, uint32_t n) {
  unsigned int i;
  for (i = 0; i < 4; ++i)
    p[i] = (unsigned char)(n >> (8U * i));
}
static void repair_crc(unsigned char *p) {
  disk32(p + 12, (uint32_t)crc32(crc32(0, p, 12), p + 16, PS_PAGE - 16));
}
static int discard_entry(void *v, ps_key k, ps_directory_value value_,
                         lc_error *e) {
  (void)v;
  (void)k;
  (void)value_;
  (void)e;
  return LC_OK;
}
static void corruption_and_identity(void) {
  pager p;
  ps_directory_io io = pager_init(&p), foreign;
  ps_directory_root root, stale, empty = {0};
  ps_directory *d;
  ps_directory_value out;
  unsigned char *saved = malloc(PS_PAGE), *page;
  uint64_t child;
  unsigned int mode;
  int found;
  ps_key wide = {UINT64_MAX, UINT64_MAX, UINT_MAX};
  ps_key collection = {0, 1, PS_COLLECTION};
  ps_key owner = {UINT64_MAX, 1, PS_STATE};
  CHECK(saved != NULL);
  d = open_directory(&io, empty);
  OK(ps_directory_put(d, wide, value(1, UINT64_MAX), &error));
  OK(ps_directory_put(d, key(1), value(1, 1), &error));
  OK(ps_directory_put(d, collection, value(1, 2), &error));
  OK(ps_directory_put(d, owner, value(1, 3), &error));
  root = flush(&p, d, 1, 1);
  d = open_directory(&io, root);
  OK(ps_directory_get(d, wide, &out, &found, &error));
  CHECK(found && out.incarnation == UINT64_MAX);
  OK(ps_directory_get(d, collection, &out, &found, &error));
  CHECK(found && out.incarnation == 2);
  OK(ps_directory_get(d, owner, &out, &found, &error));
  CHECK(found && out.incarnation == 3);
  ps_directory_close(d);
  foreign = io;
  foreign.identity[7] ^= 1;
  d = open_directory(&foreign, root);
  BAD(ps_directory_get(d, key(1), &out, &found, &error));
  ps_directory_close(d);
  page = p.image + root.offset;
  memcpy(saved, page, PS_PAGE);
  for (mode = 0; mode < 9; ++mode) {
    memcpy(page, saved, PS_PAGE);
    switch (mode) {
    case 0:
      page[100] ^= 1;
      break; /* checksum */
    case 1:
      disk32(page + 4, UINT32_MAX);
      break; /* depth */
    case 2:
      disk32(page + 8, UINT32_MAX);
      break; /* slot count */
    case 3:
      page[16] ^= 1;
      break; /* reused physical location */
    case 4:
      page[24] ^= 1;
      break; /* wrong physical binding */
    case 5:
      page[64] = 0;
      break; /* untyped key */
    case 6:
      page[64 + 40] = 0;
      break; /* invalid incarnation */
    case 7:
      memcpy(page + 64 + 64, page + 64, 64);
      break; /* duplicate key */
    default:
      page[48] = 1;
      break; /* noncanonical header */
    }
    if (mode)
      repair_crc(page);
    d = open_directory(&io, root);
    BAD(ps_directory_visit(d, discard_entry, NULL, NULL, &error));
    ps_directory_close(d);
  }
  memcpy(page, saved, PS_PAGE);
  stale = root;
  ++stale.generation;
  d = open_directory(&io, stale);
  BAD(ps_directory_get(d, key(1), &out, &found, &error));
  ps_directory_close(d);
  pager_clear(&p);
  d = open_directory(&io, empty);
  for (mode = 0; mode < 200; ++mode)
    OK(ps_directory_put(d, key(mode), value(mode, mode + 1), &error));
  root = flush(&p, d, 1, 1);
  child = disk64(p.image + root.offset + 64 + 24);
  CHECK(child && child != root.offset);
  page = p.image + child;
  memcpy(saved, page, PS_PAGE);
  page[64 + 16] ^= 1; /* CRC-valid child with the wrong lower key bound */
  repair_crc(page);
  d = open_directory(&io, root);
  BAD(ps_directory_visit(d, discard_entry, NULL, NULL, &error));
  ps_directory_close(d);
  memcpy(page, saved, PS_PAGE);
  page[4] = 1; /* CRC-valid wrong child level */
  repair_crc(page);
  d = open_directory(&io, root);
  BAD(ps_directory_get(d, key(0), &out, &found, &error));
  ps_directory_close(d);
  memcpy(page, saved, PS_PAGE);
  disk32(page + 8, 1); /* non-root pages cannot silently become underfull */
  repair_crc(page);
  d = open_directory(&io, root);
  BAD(ps_directory_get(d, key(0), &out, &found, &error));
  ps_directory_close(d);
  memcpy(page, saved, PS_PAGE);
  page = p.image + root.offset;
  disk32(page + 8, 1); /* a single-child root must have been collapsed */
  repair_crc(page);
  d = open_directory(&io, root);
  BAD(ps_directory_get(d, key(0), &out, &found, &error));
  ps_directory_close(d);
  free(saved);
  pager_close(&p);
  puts("directory: CRC, identity, generation, ordering, depth and child "
       "binding validation");
}

static int failed_visit(void *context, ps_key k, ps_directory_value val,
                        lc_error *e) {
  (void)context;
  (void)k;
  (void)val;
  return storage_error(e);
}
static int failed_page(void *context, ps_directory_root root, lc_error *e) {
  size_t *calls = context;
  (void)root;
  ++*calls;
  return storage_error(e);
}
static void api_boundaries(void) {
  pager p;
  ps_directory_io io = pager_init(&p), invalid_io;
  ps_directory_root empty = {0}, root, invalid_root;
  ps_directory *d = NULL;
  ps_directory_value actual, invalid_value;
  ps_directory_pages pages;
  int found, removed;
  size_t i;
  d = open_directory(&io, empty);
  expect(d, 0, 0);
  OK(ps_directory_remove(d, key(0), &removed, &error));
  CHECK(!removed);
  OK(ps_directory_visit(d, discard_entry, NULL, NULL, &error));
  OK(ps_directory_flush(d, 1, &root, &error));
  CHECK(!root.offset && !root.generation && !p.operations);
  ps_directory_changed_pages(d, &pages);
  CHECK(!pages.allocated_count && !pages.retired_count);
  BAD(ps_directory_flush(d, 2, &root, &error));
  ps_directory_close(d);
  for (i = 0; i < 4; ++i) {
    invalid_root.offset = i == 0 ? 0 : i == 1 ? 1 : PS_PAGE;
    invalid_root.generation = i == 2 ? 0 : 1;
    if (i == 3)
      invalid_root.offset = UINT64_MAX - PS_PAGE + 1;
    BAD(ps_directory_open(&io, invalid_root, &d, &error));
    CHECK(d == NULL);
  }
  invalid_io = io;
  invalid_io.allocate = NULL;
  BAD(ps_directory_open(&invalid_io, empty, &d, &error));
  CHECK(d == NULL);
  for (i = 0; i < 4; ++i) {
    d = open_directory(&io, empty);
    invalid_value = value(1, 1);
    if (i == 0)
      invalid_value.offset = 0;
    else if (i == 1)
      invalid_value.bytes = 0;
    else if (i == 2)
      invalid_value.incarnation = 0;
    else
      invalid_value.bytes = UINT64_MAX;
    BAD(ps_directory_put(d, key(1), invalid_value, &error));
    ps_directory_close(d);
  }
  d = open_directory(&io, empty);
  OK(ps_directory_put(d, key(1), value(1, 1), &error));
  root = flush(&p, d, 9, 1);
  d = open_directory(&io, root);
  OK(ps_directory_remove(d, key(1), &removed, &error));
  i = 0;
  BAD(ps_directory_visit(d, NULL, failed_page, &i, &error));
  CHECK(i == 0); /* deleted empty view still cannot report old page ownership */
  ps_directory_close(d);
  d = open_directory(&io, root);
  i = 0;
  BAD(ps_directory_visit(d, NULL, failed_page, &i, &error));
  CHECK(i == 1);
  ps_directory_close(d);
  d = open_directory(&io, root);
  OK(ps_directory_remove(d, key(1), &removed, &error));
  CHECK(removed);
  BAD(ps_directory_flush(d, 9, &empty, &error));
  ps_directory_close(d);
  d = open_directory(&io, root);
  expect(d, 1, 1);
  BAD(ps_directory_visit(d, failed_visit, NULL, NULL, &error));
  BAD(ps_directory_get(d, key(1), &actual, &found, &error));
  ps_directory_close(d);
  pager_clear(&p);
  empty.offset = empty.generation = 0;
  d = open_directory(&io, empty);
  for (i = 0; i < 64; ++i)
    OK(ps_directory_put(d, key(i), value(i, i + 1), &error));
  root = flush(&p, d, 1, 1);
  d = open_directory(&io, root);
  OK(ps_directory_remove(d, key(0), &removed, &error));
  OK(ps_directory_remove(d, key(63), &removed, &error));
  root = flush(&p, d, 2, 1);
  CHECK(used_pages(&p) == 3 && ps_directory_page_bound(62) >= used_pages(&p));
  CHECK(ps_directory_page_bound(0) == 0 && ps_directory_page_bound(1) == 1);
  CHECK(ps_directory_page_bound(UINT64_MAX) >= UINT64_MAX / 63);
  d = open_directory(&io, root);
  expect(d, 0, 0);
  expect(d, 1, 2);
  expect(d, 62, 63);
  expect(d, 63, 0);
  ps_directory_close(d);
  pager_close(&p);
  puts("directory: empty, finalized, invalid-span and failed-visitor "
       "boundaries");
}

static void *default_stack_worker(void *unused) {
  (void)unused;
  growth_and_copy_on_write();
  deletion_and_churn();
  failure_boundaries();
  corruption_and_identity();
  api_boundaries();
  return NULL;
}
int main(void) {
  pthread_t worker;
  int rc;
  lc_error_init(&error);
  rc = pthread_create(&worker, NULL, default_stack_worker, NULL);
  if (rc != 0)
    return 1;
  rc = pthread_join(worker, NULL);
  CHECK(rc == 0);
  lc_error_cleanup(&error);
  printf("pouch-space directory: %zu observable checks passed\n", checks);
  return 0;
}
