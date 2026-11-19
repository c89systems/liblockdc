#ifndef POUCH_SPACE_COORDINATION_H
#define POUCH_SPACE_COORDINATION_H
#include "space.h"
#include <fcntl.h>

typedef struct ps_coordination ps_coordination;
typedef struct ps_coordination_guard ps_coordination_guard;
#define PS_COORDINATION_TXN_STRIPES 256U
enum ps_coordination_domain {
  PS_COORDINATION_PUBLICATION = 0,
  PS_COORDINATION_RECOVERY,
  PS_COORDINATION_TRANSACTION,
  PS_COORDINATION_NAMESPACE
};
/* Native fcntl seam for interruption/failure tests. SETLK/SETLKW return zero
 * or -1 with errno, as on Linux. No persistent data passes through this seam. */
typedef struct ps_coordination_io {
  void *context;
  int (*lock)(void *, int, int, struct flock *);
} ps_coordination_io;
typedef struct ps_coordination_options {
  int shared;
  const lc_allocator *allocator;
  const ps_coordination_io *io;
  /* Optional shared fail-stop word, initially zero. The caller keeps it alive
   * until close and uses atomic accesses. Either layer may declare failure;
   * neither may clear it while the owner is alive. No callback on hot reads. */
  int *failure_flag;
} ps_coordination_options;

/* fd must be a fresh, writable open file description, not a dup or an inherited
 * handle. Success adopts it; failure leaves it with the caller, unlocked.
 * Native flock controls lifetime compatibility. Shared participants additionally
 * use Linux OFD byte locks, independent of PIDs and released on the last close
 * of their owning file description.
 * These are coordination capabilities only: shared allocator admission and
 * cross-process projection/reader recovery require their own integration.
 * Fork before open, or exec before using a child. CLOEXEC belongs to the caller.
 */
int ps_coordination_open(int, const ps_coordination_options *,
                         ps_coordination **, lc_error *);
/* Stop new calls before closing. Active guards AND waiters reject close and
 * leave the owner usable. Otherwise consumes the owner, even on a close error,
 * and closes the adopted fd, releasing all remaining kernel authority. */
int ps_coordination_close(ps_coordination *, lc_error *);
int ps_coordination_check(ps_coordination *, lc_error *);
size_t ps_coordination_users(ps_coordination *);
/* Publication/recovery use identity zero; transaction uses stripe < 256.
 * Namespace keys map deterministically into a separate 61-bit lock range;
 * collisions serialize safely. All participants must use the same identity.
 * wait=0 returns PENDING on contention, with NULL output. No root/registry
 * mutex spans waiting. One thread owns a guard through release; guards are
 * nonrecursive. Independent identities may finish while another guard waits.
 * Exclusive transaction guards use resident mutexes without allocation or
 * file-lock syscalls; exclusive lifetime authority excludes other processes. */
int ps_coordination_lock(ps_coordination *, enum ps_coordination_domain,
                         uint64_t, int wait, ps_coordination_guard **,
                         lc_error *);
/* Release on the acquiring thread. An uncertain kernel unlock poisons the
 * owner; further acquisition fails until close/reopen. Cleanup remains legal. */
void ps_coordination_unlock(ps_coordination_guard *);

/* Borrowed while the space is alive; never independently closed. Native spaces
 * still acquire exclusive lifetime authority; shared allocator admission is
 * not enabled by this capability. */
ps_coordination *ps_space_coordination(ps_space *);
#endif
