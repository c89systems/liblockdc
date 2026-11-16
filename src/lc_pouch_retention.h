#ifndef LC_POUCH_RETENTION_H
#define LC_POUCH_RETENTION_H

#include "lc_pouch.h"

#define LC_POUCH_RETENTION_ATTEMPT_LIMIT 64U

enum {
  LC_POUCH_RETENTION_IDLE = 0,
  LC_POUCH_RETENTION_ACTIVE = 1,
  LC_POUCH_RETENTION_FINISHED = 2
};

/* One coalesced namespace work reference. Outcomes live only in .txns.
 * Callers hold namespace mutation authority while changing this file. */
typedef struct lc_pouch_retention_work {
  int present;
  int phase;
  char *txn_id;
  char *key;
  /* Claim facts survive a crash after parent deletion has removed its lease
   * metadata. They carry no transaction outcome or application body. */
  lc_u64 fencing_token;
  lc_pouch_generation state_version;
} lc_pouch_retention_work;

typedef struct lc_pouch_retention_candidate {
  char *key;
  lc_pouch_generation version;
  lc_pouch_unix_seconds deadline;
  int has_expiring;
  unsigned long scanned;
  int wrapped;
} lc_pouch_retention_candidate;

/* Selects one due parent from the canonical metadata projection, retaining
 * only an advisory key cursor. The caller holds namespace mutation authority.
 * The caller owns candidate.key; no body source or query index is opened. */
int lc_pouch_state_retention_select_locked(lc_pouch *pouch, const char *ns,
                                           lc_pouch_unix_seconds now,
                                           lc_pouch_retention_candidate *out,
                                           lc_error *error);
int lc_pouch_state_retention_has_expiring_locked(lc_pouch *pouch,
                                                 const char *ns, int *out,
                                                 lc_error *error);
/* After observing no expiring state or unfinished cleanup, synchronize its
 * canonical containers before durably retiring discovery. Requires namespace
 * mutation authority; forces this barrier even for NoSync mutations. */
int lc_pouch_state_retention_retire_hint_locked(lc_pouch *pouch, const char *ns,
                                                lc_error *error);

int lc_pouch_retention_hint_ensure(lc_pouch *pouch, const char *ns,
                                   lc_error *error);
int lc_pouch_retention_work_read(lc_pouch *pouch, const char *ns,
                                 lc_pouch_retention_work *out, lc_error *error);
int lc_pouch_retention_work_write(lc_pouch *pouch, const char *ns, int phase,
                                  const char *txn_id, const char *key,
                                  lc_u64 fencing_token,
                                  lc_pouch_generation state_version,
                                  lc_error *error);
void lc_pouch_retention_work_cleanup(lc_pouch *pouch,
                                     lc_pouch_retention_work *work);
int lc_pouch_retention_hint_remove(lc_pouch *pouch, const char *ns,
                                   lc_error *error);
/* Scans work hints only, selecting after an opaque advisory owner cursor and
 * wrapping fairly. Native cursors retain escaped filename order; typed owners
 * need no pathname encoding. The caller owns both returned strings. */
int lc_pouch_retention_hint_next(lc_pouch *pouch, const char *after, char **ns,
                                 char **cursor, lc_error *error);

/* Bounded root-owned expiry work. Runs outside namespace mutation authority;
 * uses the existing xid -> namespace order internally. */
int lc_pouch_retention_run(lc_pouch *pouch, const char *ns, size_t max_attempts,
                           lc_pouch_maintenance_result *out, lc_error *error);
int lc_pouch_retention_check_reservation(lc_pouch *pouch, const char *ns,
                                         const char *key, lc_error *error);
int lc_pouch_retention_owns_journal(lc_pouch *pouch, const char *ns,
                                    const char *key, const char *txn_id,
                                    int *out, lc_error *error);

enum {
  LC_POUCH_RETENTION_REF_CREATED = 1,
  LC_POUCH_RETENTION_PREPARED,
  LC_POUCH_RETENTION_CLAIMED,
  LC_POUCH_RETENTION_STAGED,
  LC_POUCH_RETENTION_DECIDED,
  LC_POUCH_RETENTION_PARENT_APPLIED,
  LC_POUCH_RETENTION_ATTACHMENTS_APPLIED,
  LC_POUCH_RETENTION_DATA_DURABLE,
  LC_POUCH_RETENTION_WORK_FINISHED,
  LC_POUCH_RETENTION_JOURNAL_RETIRED,
  LC_POUCH_RETENTION_REF_CLEARED,
  LC_POUCH_RETENTION_DISCOVERED
};
#ifdef LOCKDC_TEST_BUILD
typedef int (*lc_pouch_retention_test_hook)(int phase, void *context,
                                            lc_error *error);
extern lc_pouch_retention_test_hook lc_pouch_test_retention_hook;
extern void *lc_pouch_test_retention_context;
#endif
int lc_pouch_retention_note_phase(int phase, lc_error *error);

#endif
