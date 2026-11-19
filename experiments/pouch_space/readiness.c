#define _GNU_SOURCE
#include "lc_api_internal.h"
#include "space_internal.h"
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <zlib.h>

#define READY_MAGIC UINT64_C(0x3159444145525350)
#define MEMBER_OFFSET 3U
#define WORDS 8U
enum readiness_word {
  GENERATION,
  MAGIC,
  IDENTITY,
  READY = IDENTITY + 2,
  DURABLE,
  ROOT,
  CRC
};
/* Each slot has an atomic commit generation followed by a checked record.
 * A killed writer leaves either the preceding slot or its completed successor.
 * Atomic words also make same-process alias mappings valid native C accesses.
 */
struct ps_readiness {
  uint64_t *page;
  int existing, active;
};
static int failed(ps_space *s, lc_error *e, const char *message) {
  __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELEASE);
  lc_error_set(e, LC_ERR_TRANSPORT, 0, message, NULL, NULL, "pouch-space");
  return LC_ERR_TRANSPORT;
}
static uint64_t record_crc(const uint64_t *record) {
  return (uint64_t)crc32(0, (const unsigned char *)record,
                         (uInt)((WORDS - 1) * sizeof(*record)));
}
static int slot_read(const uint64_t *source, uint64_t *record) {
  size_t i;
  uint64_t generation = __atomic_load_n(source, __ATOMIC_ACQUIRE);
  if (!generation)
    return 0;
  record[GENERATION] = generation;
  for (i = 1; i < WORDS; ++i)
    record[i] = __atomic_load_n(source + i, __ATOMIC_RELAXED);
  if (__atomic_load_n(source, __ATOMIC_ACQUIRE) != generation ||
      record[MAGIC] != READY_MAGIC || record[DURABLE] > record[READY] ||
      record[CRC] != record_crc(record))
    return -1;
  return 1;
}
static int current(ps_space *s, uint64_t *record, int *selected, lc_error *e) {
  uint64_t second[WORDS];
  int a = slot_read(s->readiness->page, record);
  int b = slot_read(s->readiness->page + WORDS, second);
  /* Only a zero generation identifies a slot interrupted before publication.
   * A malformed committed record cannot fall back to an older acknowledgement.
   */
  if (a < 0 || b < 0 || (!a && !b) ||
      (a && b && record[GENERATION] == second[GENERATION]))
    return failed(s, e,
                  "shared readiness certificate is uncertain; close cohort");
  *selected = b && (!a || second[GENERATION] > record[GENERATION]);
  if (*selected)
    memcpy(record, second, sizeof(second));
  return LC_OK;
}
static int identity(ps_space *s, const uint64_t *record, lc_error *e) {
  return memcmp(record + IDENTITY, s->identity, 16)
             ? failed(s, e, "shared readiness belongs to another container")
             : LC_OK;
}
static int select_record(ps_space *s, uint64_t *record, int previous,
                         lc_error *e) {
  uint64_t *slot = s->readiness->page + (size_t)(1 - previous) * WORDS;
  size_t i;
  if (record[GENERATION] == UINT64_MAX)
    return failed(s, e, "shared readiness generation exhausted; close cohort");
  ++record[GENERATION];
  record[CRC] = record_crc(record);
  __atomic_store_n(slot, 0, __ATOMIC_RELEASE);
  for (i = 1; i < WORDS; ++i)
    __atomic_store_n(slot + i, record[i], __ATOMIC_RELAXED);
  __atomic_store_n(slot, record[GENERATION], __ATOMIC_RELEASE);
  return LC_OK;
}
int ps_readiness_attach(ps_space *s, lc_error *e) {
  ps_readiness *r;
  uint64_t first, last;
  struct stat st;
  int rc;
  if (!ps_coordination_shared(s->coordination))
    return LC_OK;
  if (!__atomic_always_lock_free(sizeof(uint64_t), 0))
    return failed(s, e, "shared readiness requires native atomic words");
  if (fstat(s->fd, &st) || st.st_size < PS_METADATA_START)
    return failed(s, e, "container is too short for shared readiness");
  r = lc_calloc_with_allocator(NULL, 1, sizeof(*r));
  if (!r)
    return lc_error_set(e, LC_ERR_NOMEM, 0, "cannot allocate shared readiness",
                        NULL, NULL, "pouch-space");
  rc = ps_coordination_conflict(s->coordination, MEMBER_OFFSET, 1, &first,
                                &last, e);
  if (rc != LC_OK) {
    lc_free_with_allocator(NULL, r);
    return rc;
  }
  r->existing = first != last;
  r->page = mmap(NULL, PS_PAGE, PROT_READ | PROT_WRITE, MAP_SHARED, s->fd,
                 PS_READINESS_OFFSET);
  if (r->page == MAP_FAILED) {
    lc_free_with_allocator(NULL, r);
    return failed(s, e, "cannot map shared readiness page");
  }
  s->readiness = r;
  return ps_coordination_range(s->coordination, MEMBER_OFFSET, 1, 1, e);
}
void ps_readiness_close(ps_space *s) {
  if (s->readiness) {
    (void)munmap(s->readiness->page, PS_PAGE);
    lc_free_with_allocator(NULL, s->readiness);
  }
}
int ps_readiness_validate(ps_space *s, int prefix, lc_error *e) {
  uint64_t record[WORDS];
  int selected, rc;
  if (!s->readiness || !s->readiness->existing)
    return LC_OK;
  rc = current(s, record, &selected, e);
  if (rc == LC_OK)
    rc = identity(s, record, e);
  if (rc == LC_OK && prefix &&
      (record[READY] > s->sequence || record[ROOT] > s->generation))
    rc = failed(s, e, "recovery would discard a live certified prefix");
  return rc;
}
int ps_readiness_opened(ps_space *s, lc_error *e) {
  uint64_t record[WORDS] = {0};
  int selected = 1, rc;
  if (!s->readiness)
    return LC_OK;
  if (s->readiness->existing) {
    rc = current(s, record, &selected, e);
    if (rc != LC_OK)
      return rc;
    rc = identity(s, record, e);
    if (rc != LC_OK)
      return rc;
    if (record[READY] > s->sequence || record[ROOT] > s->generation)
      return failed(s, e, "recovery regressed shared readiness");
  } else {
    __atomic_store_n(s->readiness->page, 0, __ATOMIC_RELEASE);
    __atomic_store_n(s->readiness->page + WORDS, 0, __ATOMIC_RELEASE);
  }
  record[MAGIC] = READY_MAGIC;
  memcpy(record + IDENTITY, s->identity, 16);
  record[READY] = record[DURABLE] = s->sequence;
  record[ROOT] = s->generation;
  rc = select_record(s, record, selected, e);
  if (rc == LC_OK) {
    s->readiness->active = 1;
    s->own_ready_wire = s->sequence;
    s->own_ready_sequence =
        __atomic_load_n(&s->pending_sequence, __ATOMIC_ACQUIRE);
  }
  return rc;
}
int ps_readiness_read(ps_space *s, ps_readiness_certificate *out, lc_error *e) {
  uint64_t record[WORDS];
  int selected, rc;
  memset(out, 0, sizeof(*out));
  if (!s->readiness || !s->readiness->active)
    return LC_OK;
  rc = current(s, record, &selected, e);
  if (rc == LC_OK)
    rc = identity(s, record, e);
  if (rc == LC_OK && record[READY] < s->sequence)
    rc = failed(s, e, "shared ready journal prefix regressed");
  if (rc == LC_OK) {
    out->ready = record[READY];
    out->durable = record[DURABLE];
    out->root = record[ROOT];
  }
  return rc;
}
int ps_readiness_publish(ps_space *s, lc_error *e) {
  uint64_t record[WORDS];
  int selected, rc;
  if (!s->readiness || !s->readiness->active)
    return LC_OK;
  rc = current(s, record, &selected, e);
  if (rc == LC_OK)
    rc = identity(s, record, e);
  if (rc != LC_OK)
    return rc;
  if (s->sequence <= record[READY])
    return failed(s, e, "journal publication would alias a shared prefix");
  record[READY] = s->sequence;
  rc = select_record(s, record, selected, e);
  if (rc == LC_OK) {
    s->own_ready_wire = s->sequence;
    s->own_ready_sequence =
        __atomic_load_n(&s->pending_sequence, __ATOMIC_ACQUIRE);
  }
  return rc;
}
int ps_readiness_stabilize(ps_space *s, uint64_t wire, uint64_t root,
                           lc_error *e) {
  uint64_t record[WORDS];
  int selected, rc;
  if (!s->readiness || !s->readiness->active)
    return LC_OK;
  rc = current(s, record, &selected, e);
  if (rc == LC_OK)
    rc = identity(s, record, e);
  if (rc != LC_OK)
    return rc;
  if (wire > record[READY] && (!root || root <= record[ROOT]))
    return failed(s, e, "sync completion exceeds the ready journal prefix");
  if (wire > record[READY])
    record[READY] =
        wire; /* A newly stabilized selected root proves this prefix. */
  if (wire <= record[DURABLE] && root <= record[ROOT])
    return LC_OK;
  if (wire > record[DURABLE])
    record[DURABLE] = wire;
  if (root > record[ROOT])
    record[ROOT] = root;
  return select_record(s, record, selected, e);
}
