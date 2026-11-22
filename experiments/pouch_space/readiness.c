#define _GNU_SOURCE
#include "crc.h"
#include "lc_api_internal.h"
#include "space_internal.h"
#include <errno.h>
#include <limits.h>
#include <linux/futex.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>

/* Live participants must agree on lock addresses, bitmap geometry and how
 * the certified incarnation frontier advances. Cold admission resets this. */
#define READY_MAGIC UINT64_C(0x4359444145525350)
#define MEMBER_OFFSET 3U
#define WORDS PS_READINESS_WORDS
#define PENDING_WORD (PS_PAGE / sizeof(uint64_t) - 1)
#define ACTIVITY_WORD (PENDING_WORD - 1)
#define PREPARATION_WORD (ACTIVITY_WORD - 1)
#define SYNC_WORD (PREPARATION_WORD - 1)
#define CREDIT_SPILL (SYNC_WORD - 1)
#define CREDIT_ACTIVE (CREDIT_SPILL - 1)
#define CREDIT_RECORDS (CREDIT_ACTIVE - 2 * PS_READINESS_CREDIT_SLOTS)
#define PRESSURE_BIT (UINT64_C(1) << 63)
#define REUSED_OPEN_BARRIER_BIT (UINT64_C(1) << 62)
#define NONPENDING_BITS (PRESSURE_BIT | REUSED_OPEN_BARRIER_BIT)
#define BITMAP_WORDS ((CREDIT_RECORDS - 2 * WORDS) / 2)
#define ZERO_BITMAP (2 * WORDS)
#define WITNESS_BITMAP (ZERO_BITMAP + BITMAP_WORDS)
#define BITMAP_END (PS_DATA + (uint64_t)BITMAP_WORDS * 64 * PS_PAGE)
#if PS_READINESS_CREDIT_SLOTS >= 64 ||                                         \
    PS_READINESS_CREDIT_SLOTS > PS_DATA / PS_PAGE
#error "credit slots must fit in the bitmap and below dynamic credit witnesses"
#endif
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
  JOURNAL_BYTES,
  CRC
};
/* Each slot has an atomic commit generation followed by a checked record.
 * A killed writer leaves either the preceding slot or its completed successor.
 * Atomic words also make same-process alias mappings valid native C accesses.
 */
struct ps_readiness {
  uint64_t *page;
  uint64_t credit_metadata, credit_entries;
  unsigned int credit_slot;
  int existing, active;
};
static int failed(ps_space *s, lc_error *e, const char *message) {
  __atomic_store_n(&s->poisoned, 1, __ATOMIC_RELEASE);
  lc_error_set(e, LC_ERR_TRANSPORT, 0, message, NULL, NULL, "pouch-space");
  return LC_ERR_TRANSPORT;
}
int ps_readiness_credit_registered(const ps_space *s) {
  return s->readiness && s->readiness->credit_slot < PS_READINESS_CREDIT_SLOTS;
}
int ps_readiness_credit_reserve(ps_space *s, lc_error *e) {
  ps_readiness *r = s->readiness;
  uint64_t offset, first, last;
  unsigned int i;
  int rc;
  if (!r || ps_readiness_credit_registered(s))
    return LC_OK;
  for (i = 0; i < PS_READINESS_CREDIT_SLOTS; ++i) {
    offset = PS_CREDIT_RECORD_BASE + i;
    rc = ps_coordination_conflict(s->coordination, offset, 1, &first, &last, e);
    if (rc != LC_OK)
      return rc;
    if (first != last)
      continue;
    rc = ps_coordination_range(s->coordination, offset, 1, 1, e);
    if (rc != LC_OK)
      return rc;
    /* Acquisition and peer sums share allocator authority. A previous OFD
     * can leave a positive bit on death, including during publication. */
    (void)__atomic_fetch_and(r->page + CREDIT_ACTIVE, ~(UINT64_C(1) << i),
                             __ATOMIC_RELEASE);
    r->credit_slot = i;
    break;
  }
  return LC_OK;
}
int ps_readiness_credit_publish(ps_space *s) {
  ps_readiness *r = s->readiness;
  uint64_t *record, bit;
  if (r->credit_metadata == s->reserved_metadata &&
      r->credit_entries == s->reserved_entries)
    return 1;
  bit = UINT64_C(1) << r->credit_slot;
  if (!s->reserved_metadata && !s->reserved_entries) {
    /* Peers that already observed our bit can still count the previous
     * promise. Only new positive promises need the allocator. */
    (void)__atomic_fetch_and(r->page + CREDIT_ACTIVE, ~bit, __ATOMIC_RELEASE);
  } else {
    if (!s->allocation_guard)
      return 0;
    record = r->page + CREDIT_RECORDS + 2 * r->credit_slot;
    __atomic_store_n(record, s->reserved_metadata, __ATOMIC_RELAXED);
    __atomic_store_n(record + 1, s->reserved_entries, __ATOMIC_RELAXED);
    (void)__atomic_fetch_or(r->page + CREDIT_ACTIVE, bit, __ATOMIC_RELEASE);
  }
  r->credit_metadata = s->reserved_metadata;
  r->credit_entries = s->reserved_entries;
  return 1;
}
int ps_readiness_credit_collect(ps_space *s, uint64_t *metadata,
                                uint64_t *entries, lc_error *e) {
  ps_readiness *r = s->readiness;
  const uint64_t *record;
  uint64_t active, offset, first, last, bytes, count, bit;
  unsigned int slot;
  int rc;
  if (!r)
    return LC_OK;
  active = __atomic_load_n(r->page + CREDIT_ACTIVE, __ATOMIC_ACQUIRE) &
           ((UINT64_C(1) << PS_READINESS_CREDIT_SLOTS) - 1);
  if (ps_readiness_credit_registered(s))
    active &= ~(UINT64_C(1) << r->credit_slot);
  while (active) {
    slot = (unsigned int)__builtin_ctzll(active);
    bit = UINT64_C(1) << slot;
    active &= ~bit;
    offset = PS_CREDIT_RECORD_BASE + slot;
    rc = ps_coordination_conflict(s->coordination, offset, 1, &first, &last, e);
    if (rc != LC_OK)
      return rc;
    if (first == last) {
      (void)__atomic_fetch_and(r->page + CREDIT_ACTIVE, ~bit, __ATOMIC_RELEASE);
      continue;
    }
    record = r->page + CREDIT_RECORDS + 2 * slot;
    bytes = __atomic_load_n(record, __ATOMIC_RELAXED);
    count = __atomic_load_n(record + 1, __ATOMIC_RELAXED);
    if (bytes > UINT64_MAX - *metadata || count > UINT64_MAX - *entries)
      return lc_error_set(e, LC_ERR_PROTOCOL, 0,
                          "shared budget credit totals overflow", NULL, NULL,
                          "pouch-space");
    *metadata += bytes;
    *entries += count;
  }
  return LC_OK;
}
int ps_readiness_credit_spilled(const ps_space *s) {
  return s->readiness &&
         __atomic_load_n(s->readiness->page + CREDIT_SPILL, __ATOMIC_ACQUIRE);
}
void ps_readiness_credit_note_spill(ps_space *s) {
  __atomic_store_n(s->readiness->page + CREDIT_SPILL, 1, __ATOMIC_RELEASE);
}
static uint64_t record_crc(const uint64_t *record) {
  return ps_crc32(0, record, (WORDS - 1) * sizeof(*record));
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
  __atomic_thread_fence(__ATOMIC_ACQUIRE);
  if (__atomic_load_n(source, __ATOMIC_RELAXED) != generation ||
      record[MAGIC] != READY_MAGIC || record[DURABLE] > record[READY] ||
      record[ORIGIN] > record[READY] || !record[GRANT_USED] ||
      record[GRANT_USED] % TOKEN_RANGE || record[GRANT_LIMIT] % TOKEN_RANGE ||
      record[GRANT_USED] > record[GRANT_LIMIT] ||
      record[GRANT_LIMIT] - record[GRANT_USED] >= TOKEN_POOL ||
      record[PREPARED_DURABLE] > issued || record[JOURNAL_BYTES] > PS_JOURNAL ||
      (record[JOURNAL_BYTES] && record[JOURNAL_BYTES] < 64) ||
      record[CRC] != record_crc(record))
    return -1;
  return 1;
}
static int current_record(ps_space *s, uint64_t *record, int *selected) {
  uint64_t second[WORDS];
  uint64_t issued = ps_readiness_issued_tickets(s);
  int a = slot_read(s->readiness->page, record, issued);
  int b = slot_read(s->readiness->page + WORDS, second, issued);
  /* Only a zero generation identifies a slot interrupted before publication.
   * A malformed committed record cannot fall back to an older acknowledgement.
   */
  if (a < 0 || b < 0 || (!a && !b) ||
      (a && b && record[GENERATION] == second[GENERATION]))
    return 0;
  *selected = b && (!a || second[GENERATION] > record[GENERATION]);
  if (*selected)
    memcpy(record, second, sizeof(second));
  return 1;
}
static int current(ps_space *s, uint64_t *record, int *selected, lc_error *e) {
  return current_record(s, record, selected)
             ? LC_OK
             : failed(
                   s, e,
                   "shared readiness certificate is uncertain; close cohort");
}
static int checked_snapshot(ps_space *s, uint64_t *record, uint64_t allowed) {
  const uint64_t *page = s->readiness->page;
  const uint64_t blocked = ~(NONPENDING_BITS | allowed);
  uint64_t first, second;
  int selected;
  if (__atomic_load_n(page + PENDING_WORD, __ATOMIC_ACQUIRE) & blocked)
    return 0;
  first = __atomic_load_n(page, __ATOMIC_ACQUIRE);
  second = __atomic_load_n(page + WORDS, __ATOMIC_ACQUIRE);
  if (!current_record(s, record, &selected))
    return 0;
  __atomic_thread_fence(__ATOMIC_ACQUIRE);
  return first == __atomic_load_n(page, __ATOMIC_RELAXED) &&
         second == __atomic_load_n(page + WORDS, __ATOMIC_RELAXED) &&
         !(__atomic_load_n(page + PENDING_WORD, __ATOMIC_ACQUIRE) & blocked) &&
         !memcmp(record + IDENTITY, s->identity, 16);
}
int ps_readiness_durability_covers(ps_space *s, uint64_t wire,
                                   uint64_t ticket) {
  uint64_t record[WORDS];
  return checked_snapshot(s, record, 0) && record[DURABLE] >= wire &&
         record[PREPARED_DURABLE] >= ticket;
}
int ps_readiness_resident(ps_space *s) {
  uint64_t record[WORDS];
  return s->readiness && s->readiness->active &&
         checked_snapshot(s, record, 0) && record[ROOT] == s->generation &&
         record[READY] == s->sequence && record[DURABLE] == s->peer_durable &&
         record[JOURNAL_BYTES] == s->journal &&
         record[GRANT_LIMIT] >= s->token_limit;
}
int ps_readiness_retains_capture(ps_space *s) {
  uint64_t record[WORDS];
  return s->readiness && s->readiness->active &&
         checked_snapshot(s, record, PS_READINESS_JOURNAL) &&
         record[ROOT] == s->generation && record[READY] >= s->sequence &&
         record[JOURNAL_BYTES] >= s->journal &&
         record[DURABLE] >= s->peer_durable &&
         record[GRANT_LIMIT] >= s->token_limit;
}
static void certificate_from_record(ps_readiness_certificate *out,
                                    const uint64_t *record) {
  out->ready = record[READY];
  out->durable = record[DURABLE];
  out->root = record[ROOT];
  out->origin = record[ORIGIN];
  out->journal_bytes = (size_t)record[JOURNAL_BYTES];
  out->grant_used = record[GRANT_USED];
  out->grant_limit = record[GRANT_LIMIT];
  out->prepared_durable = record[PREPARED_DURABLE];
  out->pending = 0;
}
int ps_readiness_prior_durable(ps_space *s, ps_readiness_certificate *out) {
  uint64_t record[WORDS];
  if (!s->readiness || s->readiness->existing || s->readiness->active ||
      !checked_snapshot(s, record, 0))
    return 0;
  certificate_from_record(out, record);
  return 1;
}
int ps_readiness_preview(ps_space *s, ps_readiness_certificate *out) {
  const uint64_t *page;
  uint64_t record[WORDS];
  if (!s->readiness || !s->readiness->active)
    return 0;
  page = s->readiness->page;
  /* A cheap hint leaves same-root capture on its existing path. A miss
   * cannot change which root is selected once the allocator is acquired. */
  if ((__atomic_load_n(page + ROOT, __ATOMIC_RELAXED) <= s->generation &&
       __atomic_load_n(page + WORDS + ROOT, __ATOMIC_RELAXED) <=
           s->generation) ||
      !checked_snapshot(s, record, 0) || record[ROOT] <= s->generation ||
      record[READY] < s->sequence || record[GRANT_LIMIT] < s->token_limit)
    return 0;
  certificate_from_record(out, record);
  return 1;
}
static uint32_t *sync_word(ps_space *s) {
  return (uint32_t *)(s->readiness->page + SYNC_WORD);
}
uint32_t ps_readiness_sync_observe(ps_space *s) {
  return __atomic_load_n(sync_word(s), __ATOMIC_ACQUIRE);
}
int ps_readiness_sync_wait(ps_space *s, uint32_t observed) {
  /* Owner death releases the OFD lock without changing the wake word. */
  const struct timespec retry = {0, 4000000L};
  return syscall(SYS_futex, sync_word(s), FUTEX_WAIT, observed, &retry, NULL,
                 0) == 0 ||
         errno == EAGAIN || errno == EINTR || errno == ETIMEDOUT;
}
void ps_readiness_sync_wake(ps_space *s) {
  uint32_t *word = sync_word(s);
  (void)__atomic_add_fetch(word, 1, __ATOMIC_RELEASE);
  (void)syscall(SYS_futex, word, FUTEX_WAKE, INT_MAX, NULL, NULL, 0);
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
  __atomic_store_n(slot, 0, __ATOMIC_SEQ_CST);
  for (i = 1; i < WORDS; ++i)
    __atomic_store_n(slot + i, record[i], __ATOMIC_RELAXED);
  __atomic_store_n(slot, record[GENERATION], __ATOMIC_RELEASE);
  return LC_OK;
}
int ps_readiness_attach(ps_space *s, lc_error *e) {
  ps_readiness *r;
  uint64_t first, last;
  size_t i;
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
  r->credit_slot = PS_READINESS_CREDIT_SLOTS;
  r->page = mmap(NULL, PS_PAGE, PROT_READ | PROT_WRITE, MAP_SHARED, s->fd,
                 PS_READINESS_OFFSET);
  if (r->page == MAP_FAILED) {
    lc_free_with_allocator(NULL, r);
    return failed(s, e, "cannot map shared readiness page");
  }
  s->readiness = r;
  /* The first owner resets witnesses before claiming any dynamic workspace.
   * Unlike member handles, read-only imports can survive this reset. */
  if (!r->existing) {
    /* Exclusive close uses the idle certificate slots. Revoke its proof
     * before a shared recovery can modify the journal. */
    if (__atomic_load_n(r->page + MAGIC, __ATOMIC_ACQUIRE) ==
        PS_CLOSE_PROOF_MAGIC)
      __atomic_store_n(r->page + MAGIC, 0, __ATOMIC_RELEASE);
    __atomic_store_n(r->page + CREDIT_ACTIVE, 0, __ATOMIC_RELEASE);
    __atomic_store_n(r->page + CREDIT_SPILL, 0, __ATOMIC_RELEASE);
    for (i = 0; i < BITMAP_WORDS; ++i)
      __atomic_store_n(r->page + WITNESS_BITMAP + i, 0, __ATOMIC_RELEASE);
  }
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
       record[GRANT_LIMIT] > s->token_limit ||
       (record[ROOT] == s->generation &&
        (record[JOURNAL_BYTES] > s->journal ||
         ((record[READY] == s->sequence) !=
          (record[JOURNAL_BYTES] == s->journal))))))
    rc = failed(s, e, "recovery would discard a live certified prefix");
  return rc;
}
int ps_readiness_opened(ps_space *s, lc_error *e) {
  uint64_t record[WORDS] = {0};
  size_t i;
  int selected = 1, rc, advanced = 0;
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
        record[DURABLE] > s->peer_durable ||
        (s->token_end ? record[GRANT_USED] >= s->token_end
                      : record[GRANT_LIMIT] != s->token_limit))
      return failed(s, e, "recovery regressed shared readiness");
    advanced = record[DURABLE] < s->peer_durable;
  } else {
    __atomic_store_n(s->readiness->page, 0, __ATOMIC_RELEASE);
    __atomic_store_n(s->readiness->page + WORDS, 0, __ATOMIC_RELEASE);
    for (i = 0; i < BITMAP_WORDS; ++i)
      __atomic_store_n(s->readiness->page + ZERO_BITMAP + i, 0,
                       __ATOMIC_RELEASE);
    __atomic_store_n(s->readiness->page + PREPARATION_WORD, 0,
                     __ATOMIC_RELEASE);
    __atomic_store_n(sync_word(s), 0, __ATOMIC_RELEASE);
    __atomic_store_n(s->readiness->page + ACTIVITY_WORD, 0, __ATOMIC_RELEASE);
    __atomic_store_n(s->readiness->page + PENDING_WORD,
                     s->reused_open_barrier ? REUSED_OPEN_BARRIER_BIT : 0,
                     __ATOMIC_RELEASE);
    record[ORIGIN] = s->sequence;
  }
  record[MAGIC] = READY_MAGIC;
  memcpy(record + IDENTITY, s->identity, 16);
  record[READY] = s->sequence;
  record[JOURNAL_BYTES] = s->journal;
  record[DURABLE] = s->peer_durable;
  record[ROOT] = s->generation;
  /* Burn the previous cohort's entire pool. Read-only admission to a live
   * cohort leaves its frontier available to the next allocating owner. */
  if (s->token_end || !s->readiness->existing)
    record[GRANT_USED] = s->token_end ? s->token_end : s->token_limit;
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
    if (advanced)
      ps_readiness_sync_wake(s);
  }
  return rc;
}
int ps_readiness_reused_open_barrier(const ps_space *s) {
  /* Keep this for the cohort, since a join may do logical recovery before
   * the first opener finishes loading all of its namespaces. */
  return s->readiness &&
         (__atomic_load_n(s->readiness->page + PENDING_WORD, __ATOMIC_ACQUIRE) &
          REUSED_OPEN_BARRIER_BIT);
}
int ps_readiness_assign_grant(ps_space *s, lc_error *e) {
  uint64_t record[WORDS];
  int selected, rc = current(s, record, &selected, e);
  if (rc == LC_OK)
    rc = identity(s, record, e);
  if (rc != LC_OK)
    return rc;
  if (record[ROOT] != s->generation || record[READY] != s->sequence ||
      record[GRANT_LIMIT] > s->token_limit || s->token_end > s->token_limit ||
      (record[GRANT_LIMIT] == s->token_limit
           ? s->token_end - record[GRANT_USED] != TOKEN_RANGE
           : s->token_end <= record[GRANT_LIMIT]))
    return failed(s, e, "invalid shared incarnation assignment");
  record[GRANT_USED] = s->token_end;
  record[GRANT_LIMIT] = s->token_limit;
  return select_record(s, record, selected, e);
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
    certificate_from_record(out, record);
    out->pending =
        __atomic_load_n(s->readiness->page + PENDING_WORD, __ATOMIC_ACQUIRE) &
        ~NONPENDING_BITS;
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
  if (s->sequence <= record[READY] || s->generation != record[ROOT] ||
      s->journal <= record[JOURNAL_BYTES] || s->journal > PS_JOURNAL)
    return failed(s, e, "journal publication would alias a shared prefix");
  record[READY] = s->sequence;
  record[JOURNAL_BYTES] = s->journal;
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
                           size_t journal_bytes, uint64_t prepared,
                           lc_error *e) {
  uint64_t record[WORDS];
  int selected, rc, prepared_advanced, durable_advanced;
  if (!s->readiness || !s->readiness->active)
    return LC_OK;
  rc = current(s, record, &selected, e);
  if (rc == LC_OK)
    rc = identity(s, record, e);
  if (rc != LC_OK)
    return rc;
  if (wire > record[READY] && (!root || root <= record[ROOT]))
    return failed(s, e, "sync completion exceeds the ready journal prefix");
  if (root > record[ROOT] &&
      (wire < record[READY] || journal_bytes > PS_JOURNAL))
    return failed(s, e, "stabilized root would regress journal readiness");
  if (wire > record[READY])
    record[READY] =
        wire; /* A newly stabilized selected root proves this prefix. */
  prepared_advanced = prepared > record[PREPARED_DURABLE];
  durable_advanced = wire > record[DURABLE];
  if (durable_advanced || root > record[ROOT] || prepared_advanced) {
    if (durable_advanced)
      record[DURABLE] = wire;
    if (root > record[ROOT]) {
      record[ROOT] = root;
      record[JOURNAL_BYTES] = journal_bytes;
    }
    if (prepared_advanced)
      record[PREPARED_DURABLE] = prepared;
    rc = select_record(s, record, selected, e);
  }
  if (rc == LC_OK && root)
    ps_readiness_finish(s, PS_READINESS_ROOT);
  if (rc == LC_OK && (prepared_advanced || (root && durable_advanced)))
    ps_readiness_sync_wake(s);
  return rc;
}

/* Bitmap operations need allocation authority, like the readiness slots.
 * A terminated preparer can leave a subset marked since every marked page
 * already crossed the barrier. A private writer cannot touch a page until
 * the whole claim has revoked those bits. A cold cohort starts empty. */
static uint64_t bitmap_span_end(const uint64_t *bitmap, uint64_t page,
                                uint64_t end, int set) {
  uint64_t word;
  unsigned int shift;
  while (page < end) {
    shift = (unsigned int)(page % 64);
    word = __atomic_load_n(bitmap + page / 64, __ATOMIC_ACQUIRE);
    word = (set ? ~word : word) >> shift;
    if (word) {
      page += (unsigned int)__builtin_ctzll(word);
      break;
    }
    page += 64 - shift;
  }
  return page < end ? page : end;
}
uint64_t ps_readiness_zero_coverage(ps_space *s, uint64_t offset,
                                    uint64_t bytes) {
  if (!s->readiness || offset < PS_DATA || offset >= BITMAP_END)
    return 0;
  return bytes > BITMAP_END - offset ? BITMAP_END - offset : bytes;
}
uint64_t ps_readiness_zero_find(ps_space *s, uint64_t offset, uint64_t bytes,
                                uint64_t wanted, uint64_t *selected) {
  const uint64_t *bitmap;
  uint64_t first, page, next, end, best = 0;
  *selected = offset;
  bytes = ps_readiness_zero_coverage(s, offset, bytes);
  if (!bytes)
    return 0;
  bitmap = s->readiness->page + ZERO_BITMAP;
  first = (offset - PS_DATA) / PS_PAGE;
  end = first + bytes / PS_PAGE;
  for (page = bitmap_span_end(bitmap, first, end, 0); page < end;
       page = bitmap_span_end(bitmap, next, end, 0)) {
    next = bitmap_span_end(bitmap, page, end, 1);
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
  bytes = ps_readiness_zero_coverage(s, offset, bytes);
  if (!bytes)
    return 0;
  start = (offset - PS_DATA) / PS_PAGE;
  return (bitmap_span_end(s->readiness->page + ZERO_BITMAP, start,
                          start + bytes / PS_PAGE, 1) -
          start) *
         PS_PAGE;
}
static void bitmap_pages(ps_space *s, size_t bitmap, uint64_t offset,
                         uint64_t bytes, int set) {
  uint64_t page, end, mask, *word;
  unsigned int shift, count;
  if (!s->readiness || !bytes || offset >= BITMAP_END)
    return;
  if (offset < PS_DATA) {
    if (bytes <= PS_DATA - offset)
      return;
    bytes -= PS_DATA - offset;
    offset = PS_DATA;
  }
  if (bytes > BITMAP_END - offset)
    bytes = BITMAP_END - offset;
  page = (offset - PS_DATA) / PS_PAGE;
  end = (offset - PS_DATA + bytes + PS_PAGE - 1) / PS_PAGE;
  while (page < end) {
    shift = (unsigned int)(page % 64);
    count = 64 - shift;
    if (end - page < count)
      count = (unsigned int)(end - page);
    mask = count == 64 ? UINT64_MAX : ((UINT64_C(1) << count) - 1) << shift;
    word = s->readiness->page + bitmap + page / 64;
    if (set)
      __atomic_fetch_or(word, mask, __ATOMIC_ACQ_REL);
    else
      __atomic_fetch_and(word, ~mask, __ATOMIC_ACQ_REL);
    page += count;
  }
}
void ps_readiness_zero_pages(ps_space *s, uint64_t offset, uint64_t bytes,
                             int prepared) {
  bitmap_pages(s, ZERO_BITMAP, offset, bytes, prepared);
}
void ps_readiness_witness_pages(ps_space *s, uint64_t offset, uint64_t bytes,
                                int possible) {
  bitmap_pages(s, WITNESS_BITMAP, offset, bytes, possible);
}
int ps_readiness_witness_possible(ps_space *s, uint64_t offset,
                                  uint64_t bytes) {
  uint64_t start, end;
  if (!s->readiness || (!s->readiness->active && !s->readiness->existing) ||
      offset < PS_DATA || offset >= BITMAP_END || bytes > BITMAP_END - offset)
    return 1;
  start = (offset - PS_DATA) / PS_PAGE;
  end = (offset - PS_DATA + bytes + PS_PAGE - 1) / PS_PAGE;
  return bitmap_span_end(s->readiness->page + WITNESS_BITMAP, start, end, 0) <
         end;
}
