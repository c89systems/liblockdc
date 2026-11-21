#define _GNU_SOURCE
#include "lc_api_internal.h"
#include "space_internal.h"
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <zlib.h>

/* Live participants must agree on lock addresses, bitmap geometry and how
 * the certified incarnation frontier advances. Cold admission resets this. */
#define READY_MAGIC UINT64_C(0x3859444145525350)
#define MEMBER_OFFSET 3U
#define WORDS PS_READINESS_WORDS
#define PENDING_WORD (PS_PAGE / sizeof(uint64_t) - 1)
#define ACTIVITY_WORD (PENDING_WORD - 1)
#define PREPARATION_WORD (ACTIVITY_WORD - 1)
#define PRESSURE_BIT (UINT64_C(1) << 63)
#define ZERO_WORDS (PREPARATION_WORD - 2 * WORDS)
#define ZERO_END (PS_DATA + (uint64_t)ZERO_WORDS * 64 * PS_PAGE)
enum readiness_word {
  GENERATION,
  MAGIC,
  IDENTITY,
  READY = IDENTITY + 2,
  DURABLE,
  ROOT,
  ORIGIN,
  GRANT_USED,
  GRANT_LIMIT,
  PREPARED_DURABLE,
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
uint64_t ps_readiness_issued_tickets(ps_space *s) {
  return s->readiness ? __atomic_load_n(s->readiness->page + PREPARATION_WORD,
                                        __ATOMIC_ACQUIRE)
                      : 0;
}
int ps_readiness_prepare_ticket(ps_space *s, uint64_t *out, lc_error *e) {
  uint64_t *word, issued;
  *out = 0;
  if (!s->readiness || !s->readiness->active)
    return LC_OK;
  word = s->readiness->page + PREPARATION_WORD;
  issued = __atomic_load_n(word, __ATOMIC_RELAXED);
  do {
    if (issued == UINT64_MAX)
      return failed(s, e, "shared preparation tickets exhausted; close cohort");
  } while (!__atomic_compare_exchange_n(word, &issued, issued + 1, 0,
                                        __ATOMIC_ACQ_REL, __ATOMIC_RELAXED));
  *out = issued + 1;
  return LC_OK;
}
static int slot_read(const uint64_t *source, uint64_t *record,
                     uint64_t issued) {
  size_t i;
  uint64_t generation = __atomic_load_n(source, __ATOMIC_ACQUIRE);
  if (!generation)
    return 0;
  record[GENERATION] = generation;
  for (i = 1; i < WORDS; ++i)
    record[i] = __atomic_load_n(source + i, __ATOMIC_RELAXED);
  if (__atomic_load_n(source, __ATOMIC_ACQUIRE) != generation ||
      record[MAGIC] != READY_MAGIC || record[DURABLE] > record[READY] ||
      record[ORIGIN] > record[DURABLE] || !record[GRANT_USED] ||
      record[GRANT_USED] % TOKEN_RANGE || record[GRANT_LIMIT] % TOKEN_RANGE ||
      record[GRANT_USED] > record[GRANT_LIMIT] ||
      record[GRANT_LIMIT] - record[GRANT_USED] >= TOKEN_POOL ||
      record[PREPARED_DURABLE] > issued || record[CRC] != record_crc(record))
    return -1;
  return 1;
}
static int current(ps_space *s, uint64_t *record, int *selected, lc_error *e) {
  uint64_t second[WORDS];
  uint64_t issued = ps_readiness_issued_tickets(s);
  int a = slot_read(s->readiness->page, record, issued);
  int b = slot_read(s->readiness->page + WORDS, second, issued);
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
int ps_readiness_joined(const ps_space *s) {
  return s->readiness && s->readiness->existing;
}
int ps_readiness_validate(ps_space *s, lc_error *e) {
  uint64_t record[WORDS];
  int selected, rc;
  if (!s->readiness || !s->readiness->existing)
    return LC_OK;
  rc = current(s, record, &selected, e);
  if (rc == LC_OK)
    rc = identity(s, record, e);
  if (rc == LC_OK &&
      (record[READY] > s->sequence || record[ROOT] > s->generation ||
       record[GRANT_LIMIT] > s->token_limit))
    rc = failed(s, e, "recovery would discard a live certified prefix");
  return rc;
}
int ps_readiness_opened(ps_space *s, lc_error *e) {
  uint64_t record[WORDS] = {0};
  size_t i;
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
    if (record[READY] > s->sequence || record[ROOT] > s->generation ||
        record[DURABLE] > s->peer_durable || record[GRANT_USED] >= s->token_end)
      return failed(s, e, "recovery regressed shared readiness");
  } else {
    __atomic_store_n(s->readiness->page, 0, __ATOMIC_RELEASE);
    __atomic_store_n(s->readiness->page + WORDS, 0, __ATOMIC_RELEASE);
    for (i = 0; i < ZERO_WORDS; ++i)
      __atomic_store_n(s->readiness->page + 2 * WORDS + i, 0, __ATOMIC_RELEASE);
    __atomic_store_n(s->readiness->page + PREPARATION_WORD, 0,
                     __ATOMIC_RELEASE);
    __atomic_store_n(s->readiness->page + ACTIVITY_WORD, 0, __ATOMIC_RELEASE);
    __atomic_store_n(s->readiness->page + PENDING_WORD, 0, __ATOMIC_RELEASE);
    record[ORIGIN] = s->sequence;
  }
  record[MAGIC] = READY_MAGIC;
  memcpy(record + IDENTITY, s->identity, 16);
  record[READY] = s->sequence;
  record[DURABLE] = s->peer_durable;
  record[ROOT] = s->generation;
  /* Publish assignment before the owner releases allocation authority. Death
   * after this point permanently consumes the owner's range in this cohort. */
  record[GRANT_USED] = s->token_end;
  record[GRANT_LIMIT] = s->token_limit;
  rc = select_record(s, record, selected, e);
  if (rc == LC_OK) {
    s->readiness->active = 1;
    if (s->maintenance_pressure)
      ps_readiness_set_pressure(s, 1);
    s->own_ready_wire = s->sequence;
    s->own_ready_sequence =
        __atomic_load_n(&s->pending_sequence, __ATOMIC_ACQUIRE);
    ps_readiness_finish(s, PS_READINESS_ROOT | PS_READINESS_JOURNAL);
  }
  return rc;
}
int ps_readiness_read(ps_space *s, ps_readiness_certificate *out, lc_error *e) {
  uint64_t record[WORDS];
  int selected, rc;
  memset(out, 0, sizeof(*out));
  if (!s->readiness || (!s->readiness->active && !s->readiness->existing))
    return LC_OK;
  rc = current(s, record, &selected, e);
  if (rc == LC_OK)
    rc = identity(s, record, e);
  if (rc == LC_OK && s->readiness->active && record[READY] < s->sequence)
    rc = failed(s, e, "shared ready journal prefix regressed");
  if (rc == LC_OK && s->readiness->active &&
      record[GRANT_LIMIT] < s->token_limit)
    rc = failed(s, e, "shared incarnation frontier regressed");
  if (rc == LC_OK) {
    out->ready = record[READY];
    out->durable = record[DURABLE];
    out->root = record[ROOT];
    out->origin = record[ORIGIN];
    out->grant_used = record[GRANT_USED];
    out->grant_limit = record[GRANT_LIMIT];
    out->prepared_durable = record[PREPARED_DURABLE];
    out->pending =
        __atomic_load_n(s->readiness->page + PENDING_WORD, __ATOMIC_ACQUIRE) &
        ~PRESSURE_BIT;
    if (out->pending &
        ~(uint64_t)(PS_READINESS_ROOT | PS_READINESS_JOURNAL |
                    PS_READINESS_RECOVERY | PS_READINESS_CHECKPOINT_RETIRE))
      rc = failed(s, e, "shared readiness has unknown pending changes");
    if ((out->pending & PS_READINESS_CHECKPOINT_RETIRE) &&
        !(out->pending & PS_READINESS_ROOT))
      rc = failed(s, e, "shared checkpoint retirement has no pending root");
  }
  return rc;
}
int ps_readiness_pressure(ps_space *s) {
  if (!s->readiness)
    return __atomic_load_n(&s->maintenance_pressure, __ATOMIC_ACQUIRE);
  return (__atomic_load_n(s->readiness->page + PENDING_WORD, __ATOMIC_ACQUIRE) &
          PRESSURE_BIT) != 0;
}
void ps_readiness_set_pressure(ps_space *s, int requested) {
  if (!s->readiness || !s->readiness->active) {
    __atomic_store_n(&s->maintenance_pressure, requested, __ATOMIC_RELEASE);
  } else if (requested) {
    (void)__atomic_fetch_or(s->readiness->page + PENDING_WORD, PRESSURE_BIT,
                            __ATOMIC_RELEASE);
  } else {
    (void)__atomic_fetch_and(s->readiness->page + PENDING_WORD, ~PRESSURE_BIT,
                             __ATOMIC_RELEASE);
  }
}
uint64_t ps_readiness_activity(ps_space *s) {
  uint64_t *word = s->readiness ? s->readiness->page + ACTIVITY_WORD
                                : &s->maintenance_activity;
  return __atomic_load_n(word, __ATOMIC_ACQUIRE);
}
void ps_readiness_note_mutation(ps_space *s) {
  uint64_t *word = s->readiness ? s->readiness->page + ACTIVITY_WORD
                                : &s->maintenance_activity;
  (void)__atomic_fetch_add(word, 1, __ATOMIC_RELEASE);
}
void ps_readiness_begin(ps_space *s, unsigned int changes) {
  if (s->readiness)
    (void)__atomic_fetch_or(s->readiness->page + PENDING_WORD, changes,
                            __ATOMIC_SEQ_CST);
}
void ps_readiness_finish(ps_space *s, unsigned int changes) {
  if (s->readiness)
    (void)__atomic_fetch_and(s->readiness->page + PENDING_WORD,
                             ~(uint64_t)changes, __ATOMIC_RELEASE);
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
    ps_readiness_finish(s, PS_READINESS_JOURNAL);
  }
  return rc;
}
int ps_readiness_stabilize(ps_space *s, uint64_t wire, uint64_t root,
                           uint64_t prepared, lc_error *e) {
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
  if (wire > record[DURABLE] || root > record[ROOT] ||
      prepared > record[PREPARED_DURABLE]) {
    if (wire > record[DURABLE])
      record[DURABLE] = wire;
    if (root > record[ROOT])
      record[ROOT] = root;
    if (prepared > record[PREPARED_DURABLE])
      record[PREPARED_DURABLE] = prepared;
    rc = select_record(s, record, selected, e);
  }
  if (rc == LC_OK && root)
    ps_readiness_finish(s, PS_READINESS_ROOT);
  return rc;
}

/* Bitmap operations need allocation authority, like the readiness slots.
 * A terminated preparer can leave a subset marked since every marked page
 * already crossed the barrier. A private writer cannot touch a page until
 * the whole claim has revoked those bits. A cold cohort starts empty. */
static uint64_t zero_span_end(ps_space *s, uint64_t page, uint64_t end,
                              int prepared) {
  uint64_t word;
  unsigned int shift;
  while (page < end) {
    shift = (unsigned int)(page % 64);
    word = __atomic_load_n(s->readiness->page + 2 * WORDS + page / 64,
                           __ATOMIC_ACQUIRE);
    word = (prepared ? ~word : word) >> shift;
    if (word) {
      page += (unsigned int)__builtin_ctzll(word);
      break;
    }
    page += 64 - shift;
  }
  return page < end ? page : end;
}
uint64_t ps_readiness_zero_find(ps_space *s, uint64_t offset, uint64_t bytes,
                                uint64_t wanted, uint64_t *selected) {
  uint64_t first, page, next, end, best = 0;
  *selected = offset;
  if (!s->readiness || offset < PS_DATA || offset >= ZERO_END)
    return 0;
  if (bytes > ZERO_END - offset)
    bytes = ZERO_END - offset;
  first = (offset - PS_DATA) / PS_PAGE;
  end = first + bytes / PS_PAGE;
  for (page = zero_span_end(s, first, end, 0); page < end;
       page = zero_span_end(s, next, end, 0)) {
    next = zero_span_end(s, page, end, 1);
    if (next - page > best) {
      *selected = offset + (page - first) * PS_PAGE;
      best = next - page;
      if (best * PS_PAGE >= wanted)
        return wanted;
    }
  }
  return best * PS_PAGE;
}
uint64_t ps_readiness_zero_prefix(ps_space *s, uint64_t offset,
                                  uint64_t bytes) {
  uint64_t start;
  if (!s->readiness || offset < PS_DATA || offset >= ZERO_END)
    return 0;
  if (bytes > ZERO_END - offset)
    bytes = ZERO_END - offset;
  start = (offset - PS_DATA) / PS_PAGE;
  return (zero_span_end(s, start, start + bytes / PS_PAGE, 1) - start) *
         PS_PAGE;
}
void ps_readiness_zero_pages(ps_space *s, uint64_t offset, uint64_t bytes,
                             int prepared) {
  uint64_t page, end, mask, *word;
  unsigned int shift, count;
  if (!s->readiness || !bytes || offset >= ZERO_END)
    return;
  if (offset < PS_DATA) {
    if (bytes <= PS_DATA - offset)
      return;
    bytes -= PS_DATA - offset;
    offset = PS_DATA;
  }
  if (bytes > ZERO_END - offset)
    bytes = ZERO_END - offset;
  page = (offset - PS_DATA) / PS_PAGE;
  end = (offset - PS_DATA + bytes + PS_PAGE - 1) / PS_PAGE;
  while (page < end) {
    shift = (unsigned int)(page % 64);
    count = 64 - shift;
    if (end - page < count)
      count = (unsigned int)(end - page);
    mask = count == 64 ? UINT64_MAX : ((UINT64_C(1) << count) - 1) << shift;
    word = s->readiness->page + 2 * WORDS + page / 64;
    if (prepared)
      __atomic_fetch_or(word, mask, __ATOMIC_ACQ_REL);
    else
      __atomic_fetch_and(word, ~mask, __ATOMIC_ACQ_REL);
    page += count;
  }
}
