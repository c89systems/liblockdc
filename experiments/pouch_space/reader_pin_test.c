#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#include "coordination.h"
#include "lc_api_internal.h"
#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>

static size_t checks, allocations, allocation_fail, lock_calls, lock_fail;
static size_t read_bytes, read_max;
static int measuring, lock_reason, read_measuring;
static char directory[1400], path[1500], executable[1600];
#define REQUIRE(x)                                                             \
  do {                                                                         \
    __atomic_add_fetch(&checks, 1, __ATOMIC_RELAXED);                          \
    if (!(x)) {                                                                \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x);                  \
      abort();                                                                 \
    }                                                                          \
  } while (0)
static int alloc_fails(void) {
  return measuring && ++allocations == allocation_fail;
}
void *__real_lc_alloc_with_allocator(const lc_allocator *, size_t);
void *__real_lc_calloc_with_allocator(const lc_allocator *, size_t, size_t);
void *__real_lc_realloc_with_allocator(const lc_allocator *, void *, size_t);
char *__real_lc_strdup_with_allocator(const lc_allocator *, const char *);
void *__wrap_lc_alloc_with_allocator(const lc_allocator *a, size_t n) {
  return alloc_fails() ? NULL : __real_lc_alloc_with_allocator(a, n);
}
void *__wrap_lc_calloc_with_allocator(const lc_allocator *a, size_t n,
                                      size_t b) {
  return alloc_fails() ? NULL : __real_lc_calloc_with_allocator(a, n, b);
}
void *__wrap_lc_realloc_with_allocator(const lc_allocator *a, void *p,
                                       size_t n) {
  return alloc_fails() ? NULL : __real_lc_realloc_with_allocator(a, p, n);
}
char *__wrap_lc_strdup_with_allocator(const lc_allocator *a, const char *s) {
  return alloc_fails() ? NULL : __real_lc_strdup_with_allocator(a, s);
}
#ifdef PIN_LIBC64
#define REAL_FCNTL __real_fcntl64
#define WRAP_FCNTL __wrap_fcntl64
#define REAL_PREAD __real_pread64
#define WRAP_PREAD __wrap_pread64
#else
#define REAL_FCNTL __real_fcntl
#define WRAP_FCNTL __wrap_fcntl
#define REAL_PREAD __real_pread
#define WRAP_PREAD __wrap_pread
#endif
int REAL_FCNTL(int, int, ...);
int WRAP_FCNTL(int fd, int command, ...) {
  va_list ap;
  struct flock *lock;
  REQUIRE(command == F_OFD_SETLK || command == F_OFD_SETLKW ||
          command == F_OFD_GETLK);
  va_start(ap, command);
  lock = va_arg(ap, struct flock *);
  va_end(ap);
  if (lock_fail && ++lock_calls == lock_fail) {
    errno = lock_reason;
    return -1;
  }
  return REAL_FCNTL(fd, command, lock);
}
ssize_t REAL_PREAD(int, void *, size_t, off_t);
ssize_t WRAP_PREAD(int fd, void *p, size_t n, off_t offset) {
  if (read_measuring) {
    read_bytes += n;
    if (n > read_max)
      read_max = n;
  }
  return REAL_PREAD(fd, p, n, offset);
}
static void ok(int rc, lc_error *e) {
  if (rc != LC_OK)
    fprintf(stderr, "reader pin %d: %s\n", rc, e->message ? e->message : "");
  REQUIRE(rc == LC_OK);
}
static ps_key key(uint64_t id) {
  ps_key k = {9, id, PS_STATE};
  return k;
}
static unsigned char pattern(uint64_t at, unsigned int seed) {
  return (unsigned char)((at * 37U + (at >> 8) + seed) % 251U);
}
static int publish(ps_space *s, uint64_t id, uint64_t bytes, unsigned int seed,
                   lc_error *e) {
  ps_writer *w = NULL;
  unsigned char buffer[1024];
  uint64_t at = 0;
  int rc = ps_begin(s, NULL, key(id), &w, e);
  while (rc == LC_OK && at < bytes) {
    size_t n =
        bytes - at < sizeof(buffer) ? (size_t)(bytes - at) : sizeof(buffer);
    for (size_t j = 0; j < n; ++j)
      buffer[j] = pattern(at + j, seed);
    rc = ps_write(w, buffer, n, e);
    at += n;
  }
  if (rc == LC_OK)
    rc = ps_publish(w, NULL, e);
  ps_writer_close(w);
  return rc;
}
static void verify(ps_reader *r, unsigned int seed) {
  unsigned char buffer[257];
  lc_error e = {0};
  uint64_t size = ps_read_size(r), at = 0;
  /* Random reads do not advance the streaming cursor. */
  if (size) {
    ok(ps_read_at(r, size - 1, buffer, 1, &e), &e);
    REQUIRE(buffer[0] == pattern(size - 1, seed));
    ok(ps_read_at(r, 0, buffer, 1, &e), &e);
    REQUIRE(buffer[0] == pattern(0, seed));
  }
  while (at < size) {
    size_t n =
        size - at < sizeof(buffer) ? (size_t)(size - at) : sizeof(buffer);
    ok(ps_read_at(r, at, buffer, n, &e), &e);
    for (size_t i = 0; i < n; ++i)
      REQUIRE(buffer[i] == pattern(at + i, seed));
    at += n;
  }
  REQUIRE(ps_read_at(r, size, buffer, 1, &e) == LC_ERR_INVALID);
  lc_error_cleanup(&e);
}
static void send_all(int fd, const void *buffer, size_t n) {
  const unsigned char *p = buffer;
  while (n) {
    ssize_t k = write(fd, p, n);
    if (k < 0 && errno == EINTR)
      continue;
    REQUIRE(k > 0);
    p += k;
    n -= (size_t)k;
  }
}
static void receive_all(int fd, void *buffer, size_t n) {
  unsigned char *p = buffer;
  while (n) {
    ssize_t k = read(fd, p, n);
    if (k < 0 && errno == EINTR)
      continue;
    REQUIRE(k > 0);
    p += k;
    n -= (size_t)k;
  }
}
typedef struct peer {
  pid_t pid;
  int send, receive;
} peer;
static void child_main(int input, int output) {
  lc_error e = {0};
  ps_reader *r = NULL;
  uint64_t bytes;
  unsigned char *wire, command, reply = 'R';
  receive_all(input, &bytes, sizeof(bytes));
  REQUIRE(bytes <= PS_JOURNAL + 112U);
  wire = malloc((size_t)bytes);
  REQUIRE(wire);
  receive_all(input, wire, (size_t)bytes);
  read_measuring = 1;
  read_bytes = read_max = 0;
  ok(ps_reader_import(path, wire, (size_t)bytes, &r, &e), &e);
  REQUIRE(read_bytes == 0 && read_max == 0);
  if (ps_read_size(r) > PS_CHUNK) {
    unsigned char first;
    size_t got;
    ok(ps_read(r, &first, 1, &got, &e), &e);
    REQUIRE(got == 1 && first == pattern(0, 41));
    REQUIRE(read_bytes <= PS_CHUNK && read_bytes < ps_read_size(r));
  }
  read_measuring = 0;
  free(wire);
  send_all(output, &reply, 1);
  for (;;) {
    receive_all(input, &command, 1);
    if (command == 'V') {
      verify(r, 41);
      reply = 'V';
    } else {
      REQUIRE(command == 'X');
      ps_reader_close(r);
      reply = 'X';
    }
    send_all(output, &reply, 1);
    if (command == 'X')
      break;
  }
  REQUIRE(close(input) == 0 && close(output) == 0);
}
static peer peer_open(const void *ticket, size_t bytes) {
  int to[2], from[2];
  char a[32], b[32], reply;
  peer p;
  REQUIRE(pipe(to) == 0 && pipe(from) == 0);
  p.pid = fork();
  REQUIRE(p.pid >= 0);
  if (!p.pid) {
    close(to[1]);
    close(from[0]);
    snprintf(a, sizeof(a), "%d", to[0]);
    snprintf(b, sizeof(b), "%d", from[1]);
    execl(executable, executable, "--child", path, a, b, (char *)NULL);
    _exit(127);
  }
  REQUIRE(close(to[0]) == 0 && close(from[1]) == 0);
  /* Keep unrelated pipe descriptions out of later exec'd peers. */
  REQUIRE(REAL_FCNTL(to[1], F_SETFD, FD_CLOEXEC) == 0);
  REQUIRE(REAL_FCNTL(from[0], F_SETFD, FD_CLOEXEC) == 0);
  p.send = to[1];
  p.receive = from[0];
  uint64_t n = bytes;
  send_all(p.send, &n, sizeof(n));
  send_all(p.send, ticket, bytes);
  receive_all(p.receive, &reply, 1);
  REQUIRE(reply == 'R');
  return p;
}
static void peer_verify(peer *p) {
  char reply, command = 'V';
  send_all(p->send, &command, 1);
  receive_all(p->receive, &reply, 1);
  REQUIRE(reply == command);
}
static void peer_close(peer *p, int killed) {
  int status;
  if (killed)
    REQUIRE(kill(p->pid, SIGKILL) == 0);
  else {
    char command = 'X', reply;
    send_all(p->send, &command, 1);
    receive_all(p->receive, &reply, 1);
    REQUIRE(reply == command);
  }
  REQUIRE(waitpid(p->pid, &status, 0) == p->pid);
  REQUIRE(killed ? WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL
                 : WIFEXITED(status) && WEXITSTATUS(status) == 0);
  REQUIRE(close(p->send) == 0 && close(p->receive) == 0);
}
static void *ticket(ps_reader *r, size_t *n) {
  lc_error e = {0};
  *n = ps_reader_ticket_bytes(r);
  void *wire = malloc(*n);
  REQUIRE(wire && *n >= 112);
  ok(ps_reader_ticket(r, wire, *n, &e), &e);
  return wire;
}
static void one_file(void) {
  struct dirent *item;
  struct stat st;
  size_t count = 0;
  DIR *d = opendir(directory);
  REQUIRE(d);
  while ((item = readdir(d)))
    if (strcmp(item->d_name, ".") && strcmp(item->d_name, "..")) {
      REQUIRE(!strcmp(item->d_name, "store"));
      ++count;
    }
  REQUIRE(closedir(d) == 0 && count == 1);
  REQUIRE(stat(path, &st) == 0 && st.st_size >= (off_t)PS_DEFAULT_CAPACITY);
}
static void lifecycle(int sync, uint64_t size, int sibling) {
  lc_error e = {0};
  ps_options options = {0};
  options.durable_sync = sync;
  ps_space *s = NULL;
  ps_reader *r = NULL;
  ps_stats held, released, before;
  size_t bytes;
  ok(ps_open(path, 1, &options, &s, &e), &e);
  ok(publish(s, 1, size, 41, &e), &e);
  if (sibling)
    ok(publish(s, 2, 64, 31, &e), &e);
  ok(ps_read_open(s, key(1), NULL, &r, &e), &e);
  void *wire = ticket(r, &bytes);
  peer a = peer_open(wire, bytes), b = peer_open(wire, bytes);
  ps_reader_close(r);
  free(wire);
  ok(ps_delete(s, key(1), &e), &e);
  ok(ps_checkpoint(s, &e), &e);
  peer_verify(&a);
  peer_verify(&b);
  ok(ps_close(s, &e), &e);
  s = NULL;
  ok(ps_open(path, 0, &options, &s, &e), &e);
  ok(ps_status(s, &held, &e), &e);
  if (size)
    REQUIRE(held.allocated_bytes >= PS_PAGE);
  if (sibling) {
    ok(ps_delete(s, key(2), &e), &e);
    ok(ps_checkpoint(s, &e), &e);
    peer_verify(&a);
    /* A recovered packed page can lose its last selected cell, then survive
     * another writer generation solely through its foreign byte ownership. */
    ok(ps_close(s, &e), &e);
    ok(ps_open(path, 0, &options, &s, &e), &e);
    peer_verify(&a);
  }
  /* Independent writes/reclamation and explicit growth cannot reuse pins. */
  for (size_t round = 0; round < 48; ++round) {
    ok(publish(s, 3, size > PS_CHUNK ? 65536 : 64, (unsigned int)round, &e),
       &e);
    ok(ps_checkpoint(s, &e), &e);
  }
  peer_verify(&a);
  peer_verify(&b);
  ok(ps_grow(s, PS_MIB, &e), &e);
  peer_verify(&a);
  peer_close(&a, 0);
  ok(ps_checkpoint(s, &e), &e);
  peer_verify(&b);
  ok(ps_status(s, &before, &e), &e);
  /* Last foreign OFD dies, with no participant-table cleanup. */
  peer_close(&b, 1);
  ok(ps_checkpoint(s, &e), &e);
  ok(ps_status(s, &released, &e), &e);
  if (size)
    REQUIRE(released.allocated_bytes < before.allocated_bytes);
  REQUIRE(released.retired_bytes == 0);
  ok(ps_close(s, &e), &e);
  ok(ps_open(path, 0, &options, &s, &e), &e);
  ok(ps_close(s, &e), &e);
  one_file();
  REQUIRE(unlink(path) == 0);
}
static void pressure(int sync) {
  lc_error e = {0};
  ps_options options = {0};
  options.durable_sync = sync;
  ps_space *s = NULL;
  ps_reader *r = NULL;
  size_t n;
  ok(ps_open(path, 1, &options, &s, &e), &e);
  ok(publish(s, 1, 2 * PS_MIB + 73, 41, &e), &e);
  ok(ps_read_open(s, key(1), NULL, &r, &e), &e);
  void *wire = ticket(r, &n);
  peer p = peer_open(wire, n);
  free(wire);
  ps_reader_close(r);
  ok(ps_delete(s, key(1), &e), &e);
  ok(ps_checkpoint(s, &e), &e);
  ok(ps_close(s, &e), &e);
  ok(ps_open(path, 0, &options, &s, &e), &e);
  uint64_t id;
  for (id = 2; id < 40; ++id) {
    int rc = publish(s, id, PS_MIB / 2, 17, &e);
    if (rc != LC_OK) {
      REQUIRE(rc == LC_ERR_INVALID);
      lc_error_cleanup(&e);
      break;
    }
  }
  REQUIRE(id > 10 && id < 40);
  ok(ps_check(s, &e), &e);
  peer_verify(&p);
  ok(ps_checkpoint(s, &e), &e);
  peer_verify(&p);
  peer_close(&p, 0);
  ok(ps_checkpoint(s, &e), &e);
  /* The held 2 MiB becomes ordinary payload capacity again. */
  ok(publish(s, 90, PS_MIB, 19, &e), &e);
  ok(ps_checkpoint(s, &e), &e);
  ok(ps_close(s, &e), &e);
  one_file();
  REQUIRE(unlink(path) == 0);
}
static void clones(void) {
  lc_error e = {0};
  ps_space *s = NULL;
  ps_reader *local = NULL, *first = NULL, *second = NULL;
  ps_ref a, b;
  size_t bytes;
  ok(ps_open(path, 1, NULL, &s, &e), &e);
  ok(publish(s, 1, 20003, 41, &e), &e);
  ok(ps_read_open(s, key(1), NULL, &local, &e), &e);
  void *wire = ticket(local, &bytes);
  ok(ps_reader_import(path, wire, bytes, &first, &e), &e);
  ps_read_reference(local, &a);
  ps_read_reference(first, &b);
  REQUIRE(!memcmp(&a, &b, sizeof(a)));
  ps_reader_close(local);
  free(wire);
  ok(publish(s, 1, 20003, 92, &e), &e);
  ok(ps_checkpoint(s, &e), &e);
  ok(ps_close(s, &e), &e);
  wire = ticket(first, &bytes);
  ok(ps_reader_import(path, wire, bytes, &second, &e), &e);
  free(wire);
  ps_reader_close(first);
  ok(ps_open(path, 0, NULL, &s, &e), &e);
  verify(second, 41);
  ps_reader_close(second);
  ok(ps_checkpoint(s, &e), &e);
  ok(ps_close(s, &e), &e);
  REQUIRE(unlink(path) == 0);
}
static void ticket_crc(unsigned char *p, size_t n) {
  memset(p + 96, 0, 4);
  uint32_t crc = (uint32_t)crc32(0, p, (uInt)n);
  for (size_t i = 0; i < 4; ++i)
    p[96 + i] = (unsigned char)(crc >> (8 * i));
}
static void malformed(void) {
  lc_error e = {0};
  ps_space *s = NULL, *other = NULL;
  ps_reader *r = NULL, *bad = NULL;
  char alternate[1600];
  size_t n;
  ok(ps_open(path, 1, NULL, &s, &e), &e);
  ok(publish(s, 1, 65, 41, &e), &e);
  ok(ps_read_open(s, key(1), NULL, &r, &e), &e);
  unsigned char *wire = ticket(r, &n), *copy = malloc(n);
  REQUIRE(copy);
  for (size_t i = 0; i < n; ++i) {
    memcpy(copy, wire, n);
    copy[i] ^= 1;
    REQUIRE(ps_reader_import(path, copy, n, &bad, &e) == LC_ERR_INVALID &&
            !bad);
    lc_error_cleanup(&e);
  }
  for (size_t i = 0; i < n; ++i) {
    REQUIRE(ps_reader_import(path, wire, i, &bad, NULL) == LC_ERR_INVALID &&
            !bad);
  }
  static const struct {
    size_t at;
    uint64_t value;
  } cases[] = {{24, UINT64_MAX},  {48, 0},  {56, UINT64_MAX},
               {64, UINT64_MAX},  {72, 0},  {76, 1},
               {100, 1},          {104, 1}, {112, 0},
               {112, UINT64_MAX}, {120, 0}, {120, PS_CHUNK + 1},
               {132, 8}};
  for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
    memcpy(copy, wire, n);
    /* Last field is a four-byte flag at the end of the ticket. */
    size_t width = cases[i].at == 132 ? 4 : 8;
    for (size_t j = 0; j < width; ++j)
      copy[cases[i].at + j] = (unsigned char)(cases[i].value >> (8 * j));
    ticket_crc(copy, n);
    REQUIRE(ps_reader_import(path, copy, n, &bad, &e) == LC_ERR_INVALID &&
            !bad);
    lc_error_cleanup(&e);
  }
  snprintf(alternate, sizeof(alternate), "%s.other", path);
  ok(ps_open(alternate, 1, NULL, &other, &e), &e);
  REQUIRE(ps_reader_import(alternate, wire, n, &bad, &e) == LC_ERR_INVALID &&
          !bad);
  lc_error_cleanup(&e);
  ok(ps_close(other, &e), &e);
  REQUIRE(unlink(alternate) == 0);
  /* Failed partial pin acquisitions close their fresh OFD. */
  for (size_t site = 1; site <= 2; ++site) {
    for (int reason = 0; reason < 3; ++reason) {
      lock_calls = 0;
      lock_fail = site;
      lock_reason = reason == 0 ? EINTR : reason == 1 ? EIO : EAGAIN;
      int rc = ps_reader_import(path, wire, n, &bad, &e);
      lock_fail = 0;
      if (!reason) {
        ok(rc, &e);
        ps_reader_close(bad);
        bad = NULL;
      } else {
        REQUIRE(rc == (reason == 1 ? LC_ERR_TRANSPORT : LC_ERR_PENDING) &&
                !bad);
        lc_error_cleanup(&e);
      }
      uint64_t first, last;
      ok(ps_coordination_conflict(ps_space_coordination(s), 2, 1, &first, &last,
                                  &e),
         &e);
      REQUIRE(first == last);
    }
  }
  /* Keep an unrelated lock domain independent of every payload range. */
  ps_coordination_guard *guard = NULL;
  ok(ps_coordination_lock(ps_space_coordination(s), PS_COORDINATION_NAMESPACE,
                          UINT64_MAX, 0, &guard, &e),
     &e);
  ok(ps_reader_import(path, wire, n, &bad, &e), &e);
  verify(bad, 41);
  ps_reader_close(bad);
  ps_coordination_unlock(guard);
  free(wire);
  free(copy);
  ps_reader_close(r);
  ok(ps_close(s, &e), &e);
  REQUIRE(unlink(path) == 0);
}
typedef struct fault {
  size_t calls, fail;
  int reason, upper_first;
} fault;
static int query_fault(void *context, int fd, int command, struct flock *lock) {
  fault *f = context;
  if (command == F_OFD_GETLK && ++f->calls == f->fail) {
    errno = f->reason;
    return -1;
  }
  if (command == F_OFD_GETLK && f->upper_first && lock->l_start >= PS_DATA &&
      lock->l_len > PS_PAGE) {
    struct flock probe = *lock;
    off_t skip = probe.l_len / 2;
    probe.l_start += skip;
    probe.l_len -= skip;
    int rc = REAL_FCNTL(fd, command, &probe);
    if (rc < 0 || probe.l_type != F_UNLCK) {
      *lock = probe;
      return rc;
    }
  }
  return REAL_FCNTL(fd, command, lock);
}
static void faults(void) {
  lc_error e = {0};
  ps_options options = {0};
  fault f = {0};
  ps_coordination_io io = {&f, query_fault};
  options.coordination_io = &io;
  ps_space *s = NULL;
  ps_reader *r = NULL, *imported = NULL;
  size_t n, sites;
  ok(ps_open(path, 1, &options, &s, &e), &e);
  ok(publish(s, 1, 40017, 41, &e), &e);
  ok(ps_read_open(s, key(1), NULL, &r, &e), &e);
  void *wire = ticket(r, &n);
  allocations = allocation_fail = 0;
  measuring = 1;
  int rc = ps_reader_import(path, wire, n, &imported, &e);
  measuring = 0;
  sites = allocations;
  ok(rc, &e);
  ps_reader_close(imported);
  imported = NULL;
  REQUIRE(sites >= 4);
  for (int output = 0; output < 2; ++output)
    for (size_t site = 1; site <= sites; ++site) {
      allocations = 0;
      allocation_fail = site;
      measuring = 1;
      rc = ps_reader_import(path, wire, n, &imported, output ? &e : NULL);
      measuring = 0;
      REQUIRE(allocations >= site && rc == LC_ERR_NOMEM && !imported);
      lc_error_cleanup(&e);
    }
  ok(ps_reader_import(path, wire, n, &imported, &e), &e);
  free(wire);
  ps_reader_close(r);
  ok(ps_delete(s, key(1), &e), &e);
  ok(ps_checkpoint(s, &e), &e);
  f.calls = 0;
  f.fail = 1;
  f.reason = EIO;
  REQUIRE(ps_checkpoint(s, &e) == LC_ERR_TRANSPORT);
  lc_error_cleanup(&e);
  REQUIRE(ps_check(s, &e) == LC_ERR_TRANSPORT);
  lc_error_cleanup(&e);
  verify(imported, 41);
  ok(ps_close(s, &e), &e);
  s = NULL;
  f.fail = 0;
  f.calls = 0;
  ok(ps_open(path, 0, &options, &s, &e), &e);
  size_t queries = f.calls;
  REQUIRE(queries >= 2);
  ok(ps_close(s, &e), &e);
  s = NULL;
  /* Query errors cannot yield a writable, incompletely quarantined space. */
  for (size_t site = 1; site <= queries; ++site) {
    f.calls = 0;
    f.fail = site;
    f.reason = EIO;
    REQUIRE(ps_open(path, 0, &options, &s, &e) == LC_ERR_TRANSPORT && !s);
    lc_error_cleanup(&e);
    verify(imported, 41);
  }
  f.fail = 0;
  allocations = allocation_fail = 0;
  measuring = 1;
  rc = ps_open(path, 0, &options, &s, &e);
  measuring = 0;
  sites = allocations;
  ok(rc, &e);
  ok(ps_close(s, &e), &e);
  s = NULL;
  for (size_t site = 1; site <= sites; ++site) {
    allocations = 0;
    allocation_fail = site;
    measuring = 1;
    rc = ps_open(path, 0, &options, &s, NULL);
    measuring = 0;
    REQUIRE(allocations >= site && rc == LC_ERR_NOMEM && !s);
  }
  f.calls = 0;
  f.fail = 1;
  f.reason = EINTR;
  ok(ps_open(path, 0, &options, &s, &e), &e);
  verify(imported, 41);
  ps_reader_close(imported);
  f.fail = 0;
  ok(ps_checkpoint(s, &e), &e);
  ok(ps_close(s, &e), &e);
  REQUIRE(unlink(path) == 0);
}
static void fragmented(void) {
  enum { PINS = 64 };
  lc_error e = {0};
  ps_options options = {0};
  fault queries = {0};
  ps_coordination_io io = {&queries, query_fault};
  options.coordination_io = &io;
  ps_space *s = NULL;
  ps_reader **held = calloc(PINS, sizeof(*held));
  REQUIRE(held);
  ok(ps_open(path, 1, &options, &s, &e), &e);
  /* Separate each retained page from the next with one unretained page. */
  for (size_t i = 0; i < 2 * PINS; ++i) {
    ok(publish(s, i + 1, PS_PAGE, 41, &e), &e);
    if (!(i % 2)) {
      ps_reader *r = NULL;
      size_t n;
      ok(ps_read_open(s, key(i + 1), NULL, &r, &e), &e);
      void *wire = ticket(r, &n);
      ok(ps_reader_import(path, wire, n, &held[i / 2], &e), &e);
      free(wire);
      ps_reader_close(r);
    }
  }
  for (size_t i = 0; i < 2 * PINS; ++i)
    ok(ps_delete(s, key(i + 1), &e), &e);
  ok(ps_checkpoint(s, &e), &e);
  ok(ps_close(s, &e), &e);
  queries.calls = 0;
  ok(ps_open(path, 0, &options, &s, &e), &e);
  fprintf(stderr, "fragmented pins=%u reopen_queries=%zu bound=%u\n", PINS,
          queries.calls, 2 * PINS + 3);
  /* GETLK may return any overlapping owner. Each witness splits a residual
   * interval; a clear residual is visited once, independent of query order. */
  REQUIRE(queries.calls <= 2 * PINS + 3);
  ok(ps_close(s, &e), &e);
  queries.calls = 0;
  queries.upper_first = 1;
  ok(ps_open(path, 0, &options, &s, &e), &e);
  fprintf(stderr,
          "fragmented upper-first pins=%u reopen_queries=%zu bound=%u\n", PINS,
          queries.calls, 2 * PINS + 3);
  REQUIRE(queries.calls <= 2 * PINS + 3);
  for (size_t i = 0; i < PINS; ++i)
    verify(held[i], 41);
  ok(publish(s, 1000, PS_MIB, 18, &e), &e);
  ok(ps_checkpoint(s, &e), &e);
  for (size_t i = 0; i < PINS; ++i) {
    verify(held[i], 41);
    ps_reader_close(held[i]);
  }
  free(held);
  ok(ps_checkpoint(s, &e), &e);
  ps_stats status;
  ok(ps_status(s, &status, &e), &e);
  REQUIRE(status.retired_bytes == 0);
  ok(ps_close(s, &e), &e);
  one_file();
  REQUIRE(unlink(path) == 0);
}
typedef struct admission_fault {
  int fail_write, fail_sync;
} admission_fault;
static ssize_t admission_read(void *context, int fd, void *p, size_t n,
                              off_t at) {
  (void)context;
  return pread(fd, p, n, at);
}
static ssize_t admission_write(void *context, int fd, const void *p, size_t n,
                               off_t at) {
  admission_fault *f = context;
  if (f->fail_write) {
    errno = EIO;
    return -1;
  }
  return pwrite(fd, p, n, at);
}
static int admission_sync(void *context, int fd) {
  admission_fault *f = context;
  if (f->fail_sync) {
    errno = EIO;
    return -1;
  }
  return fdatasync(fd);
}
static int admission_reserve(void *context, int fd, off_t at, off_t n) {
  (void)context;
  return fallocate(fd, 0, at, n);
}
static void admission(void) {
  for (int sync = 0; sync < 2; ++sync) {
    for (int failure = 0; failure < 3; ++failure) {
      lc_error e = {0};
      ps_options options = {0};
      admission_fault f = {0};
      ps_io io = {&f,
                  admission_read,
                  admission_write,
                  admission_sync,
                  admission_reserve,
                  NULL};
      options.io = &io;
      options.durable_sync = sync;
      ps_space *s = NULL;
      ps_reader *r = NULL;
      ps_stats before, after;
      ok(ps_open(path, 1, &options, &s, &e), &e);
      ok(publish(s, 1, 17, 41, &e), &e);
      ok(ps_read_open(s, key(1), NULL, &r, &e), &e);
      size_t n = ps_reader_ticket_bytes(r);
      void *wire = malloc(n);
      REQUIRE(wire);
      ok(ps_status(s, &before, &e), &e);
      f.fail_write = failure == 1;
      f.fail_sync = failure == 2;
      int rc = ps_reader_ticket(r, wire, n, &e);
      f.fail_write = f.fail_sync = 0;
      ok(rc, &e);
      ok(ps_check(s, &e), &e);
      ok(ps_status(s, &after, &e), &e);
      REQUIRE(after.syncs == before.syncs);
      allocations = 0;
      allocation_fail = 1;
      measuring = 1;
      rc = ps_reader_ticket(r, wire, n, &e);
      measuring = 0;
      ok(rc, &e);
      REQUIRE(allocations == 0);
      free(wire);
      ps_reader_close(r);
      ok(ps_close(s, &e), &e);
      ok(ps_open(path, 0, &options, &s, &e), &e);
      ok(ps_check(s, &e), &e);
      ok(ps_close(s, &e), &e);
      REQUIRE(unlink(path) == 0);
    }
  }
}
typedef struct concurrent_work {
  ps_space *space;
  int done;
} concurrent_work;
static void *replace_concurrently(void *context) {
  concurrent_work *work = context;
  lc_error e = {0};
  for (unsigned int i = 0; i < 64; ++i) {
    ok(publish(work->space, 1, 20003, i, &e), &e);
    ok(ps_checkpoint(work->space, &e), &e);
    if (i == 31)
      ok(ps_grow(work->space, PS_MIB, &e), &e);
  }
  __atomic_store_n(&work->done, 1, __ATOMIC_RELEASE);
  return NULL;
}
static void threads(void) {
  lc_error e = {0};
  ps_space *s = NULL;
  ps_reader *local = NULL, *held = NULL;
  pthread_t worker;
  ok(ps_open(path, 1, NULL, &s, &e), &e);
  ok(publish(s, 1, 20003, 41, &e), &e);
  ok(ps_read_open(s, key(1), NULL, &local, &e), &e);
  size_t bytes;
  void *wire = ticket(local, &bytes);
  ok(ps_reader_import(path, wire, bytes, &held, &e), &e);
  free(wire);
  concurrent_work work = {s, 0};
  REQUIRE(pthread_create(&worker, NULL, replace_concurrently, &work) == 0);
  for (size_t round = 0;
       round < 64 || !__atomic_load_n(&work.done, __ATOMIC_ACQUIRE); ++round) {
    ps_reader *imported = NULL;
    wire = ticket(local, &bytes);
    ok(ps_reader_import(path, wire, bytes, &imported, &e), &e);
    free(wire);
    verify(imported, 41);
    ps_reader_close(imported);
    verify(held, 41);
  }
  REQUIRE(pthread_join(worker, NULL) == 0);
  ps_reader_close(local);
  verify(held, 41);
  ps_reader_close(held);
  ok(ps_checkpoint(s, &e), &e);
  ok(ps_close(s, &e), &e);
  REQUIRE(unlink(path) == 0);
}
static void writer_child(int input, int output, int mode) {
  lc_error e = {0};
  ps_options options = {0};
  options.durable_sync = mode / 2;
  ps_space *s = NULL;
  ps_reader *r = NULL;
  size_t n;
  char command;
  ok(ps_open(path, 1, &options, &s, &e), &e);
  ok(publish(s, 1, 40017, 41, &e), &e);
  ok(ps_read_open(s, key(1), NULL, &r, &e), &e);
  void *wire = ticket(r, &n);
  uint64_t bytes = n;
  send_all(output, &bytes, sizeof(bytes));
  send_all(output, wire, n);
  free(wire);
  receive_all(input, &command, 1);
  REQUIRE(command == 'R');
  ps_reader_close(r);
  ok(ps_delete(s, key(1), &e), &e);
  if (mode % 2)
    ok(ps_checkpoint(s, &e), &e);
  command = 'D';
  send_all(output, &command, 1);
  receive_all(input, &command, 1);
  REQUIRE(0); /* Parent terminates this writer without running cleanup. */
}
static void writer_death(void) {
  for (int mode = 0; mode < 4; ++mode) {
    int to[2], from[2];
    char a[32], b[32], m[32], command;
    REQUIRE(pipe(to) == 0 && pipe(from) == 0);
    pid_t pid = fork();
    REQUIRE(pid >= 0);
    if (!pid) {
      close(to[1]);
      close(from[0]);
      snprintf(a, sizeof(a), "%d", to[0]);
      snprintf(b, sizeof(b), "%d", from[1]);
      snprintf(m, sizeof(m), "%d", mode);
      execl(executable, executable, "--writer", path, a, b, m, (char *)NULL);
      _exit(127);
    }
    REQUIRE(close(to[0]) == 0 && close(from[1]) == 0);
    uint64_t n;
    receive_all(from[0], &n, sizeof(n));
    REQUIRE(n < PS_JOURNAL);
    void *wire = malloc((size_t)n);
    REQUIRE(wire);
    receive_all(from[0], wire, (size_t)n);
    lc_error e = {0};
    ps_reader *held = NULL;
    ps_space *s = NULL;
    ps_options options = {0};
    options.durable_sync = mode / 2;
    ok(ps_reader_import(path, wire, (size_t)n, &held, &e), &e);
    free(wire);
    command = 'R';
    send_all(to[1], &command, 1);
    receive_all(from[0], &command, 1);
    REQUIRE(command == 'D');
    REQUIRE(kill(pid, SIGKILL) == 0);
    int status;
    REQUIRE(waitpid(pid, &status, 0) == pid && WIFSIGNALED(status) &&
            WTERMSIG(status) == SIGKILL);
    REQUIRE(close(to[1]) == 0 && close(from[0]) == 0);
    ok(ps_open(path, 0, &options, &s, &e), &e);
    ok(ps_checkpoint(s, &e), &e);
    verify(held, 41);
    for (size_t round = 0; round < 16; ++round) {
      ok(publish(s, 2, 65536, (unsigned int)round, &e), &e);
      ok(ps_checkpoint(s, &e), &e);
    }
    verify(held, 41);
    ps_reader_close(held);
    ok(ps_checkpoint(s, &e), &e);
    ok(ps_close(s, &e), &e);
    one_file();
    REQUIRE(unlink(path) == 0);
  }
}
static double milliseconds(void) {
  struct timespec t;
  REQUIRE(clock_gettime(CLOCK_MONOTONIC, &t) == 0);
  return (double)t.tv_sec * 1000.0 + (double)t.tv_nsec / 1000000.0;
}
static void read_all(ps_reader *r) {
  unsigned char buffer[1024];
  lc_error e = {0};
  uint64_t total = 0;
  for (;;) {
    size_t n = 0;
    ok(ps_read(r, buffer, sizeof(buffer), &n, &e), &e);
    total += n;
    if (!n)
      break;
  }
  REQUIRE(total == ps_read_size(r));
}
static void measure(void) {
  lc_error e = {0};
  ps_options options = {0};
  fault queries = {0};
  ps_coordination_io io = {&queries, query_fault};
  options.coordination_io = &io;
  ps_space *s = NULL;
  ps_reader *r = NULL, *held = NULL;
  ps_stats before, after;
  ok(ps_open(path, 1, &options, &s, &e), &e);
  ok(publish(s, 1, 2 * PS_MIB + 73, 41, &e), &e);
  ok(ps_read_open(s, key(1), NULL, &r, &e), &e);
  ok(ps_status(s, &before, &e), &e);
  size_t bytes = ps_reader_ticket_bytes(r);
  void *wire = malloc(bytes);
  REQUIRE(wire);
  double start = milliseconds();
  for (size_t i = 0; i < 64; ++i)
    ok(ps_reader_ticket(r, wire, bytes, &e), &e);
  double export_us = (milliseconds() - start) * 1000 / 64;
  ok(ps_status(s, &after, &e), &e);
  REQUIRE(before.syncs == after.syncs);
  start = milliseconds();
  for (size_t i = 0; i < 64; ++i) {
    ps_reader *imported = NULL;
    ok(ps_reader_import(path, wire, bytes, &imported, &e), &e);
    ps_reader_close(imported);
  }
  double import_us = (milliseconds() - start) * 1000 / 64;
  ok(ps_reader_import(path, wire, bytes, &held, &e), &e);
  free(wire);
  start = milliseconds();
  read_all(r);
  double local_ms = milliseconds() - start;
  start = milliseconds();
  read_all(held);
  double foreign_ms = milliseconds() - start;
  ps_reader_close(r);
  ok(ps_delete(s, key(1), &e), &e);
  ok(ps_checkpoint(s, &e), &e);
  ok(ps_close(s, &e), &e);
  queries.calls = 0;
  start = milliseconds();
  for (size_t i = 0; i < 8; ++i) {
    ok(ps_open(path, 0, &options, &s, &e), &e);
    ok(ps_close(s, &e), &e);
  }
  double pinned_reopen_ms = (milliseconds() - start) / 8;
  size_t pinned_queries = queries.calls / 8;
  verify(held, 41);
  ps_reader_close(held);
  queries.calls = 0;
  start = milliseconds();
  for (size_t i = 0; i < 8; ++i) {
    ok(ps_open(path, 0, &options, &s, &e), &e);
    ok(ps_close(s, &e), &e);
  }
  double drained_reopen_ms = (milliseconds() - start) / 8;
  REQUIRE(queries.calls == 8);
  fprintf(
      stdout,
      "pin_measure bytes=%llu ticket_bytes=%zu export_us=%.3f import_us=%.3f "
      "local_read_ms=%.3f imported_read_ms=%.3f pinned_reopen_ms=%.3f "
      "drained_reopen_ms=%.3f pinned_reopen_queries=%zu "
      "drained_reopen_queries=1 "
      "export_syncs=0 verified=1\n",
      (unsigned long long)(2 * PS_MIB + 73), bytes, export_us, import_us,
      local_ms, foreign_ms, pinned_reopen_ms, drained_reopen_ms,
      pinned_queries);
  one_file();
  REQUIRE(unlink(path) == 0);
}
int main(int argc, char **argv) {
  if (argc == 6 && !strcmp(argv[1], "--writer")) {
    snprintf(path, sizeof(path), "%s", argv[2]);
    writer_child(atoi(argv[3]), atoi(argv[4]), atoi(argv[5]));
    return 0;
  }
  if (argc == 5 && !strcmp(argv[1], "--child")) {
    snprintf(path, sizeof(path), "%s", argv[2]);
    child_main(atoi(argv[3]), atoi(argv[4]));
    return 0;
  }
  REQUIRE(argc == 3);
  ssize_t n = readlink("/proc/self/exe", executable, sizeof(executable) - 1);
  REQUIRE(n > 0 && (size_t)n < sizeof(executable) - 1);
  executable[n] = 0;
  REQUIRE(snprintf(directory, sizeof(directory), "%s/reader-pins-XXXXXX",
                   argv[1]) < (int)sizeof(directory));
  REQUIRE(mkdtemp(directory));
  REQUIRE(snprintf(path, sizeof(path), "%s/store", directory) <
          (int)sizeof(path));
  if (!strcmp(argv[2], "lifecycle")) {
    for (int sync = 0; sync < 2; ++sync) {
      lifecycle(sync, 0, 0);
      lifecycle(sync, 65, 0);
      lifecycle(sync, 65, 1);
      lifecycle(sync, 196731, 1);
    }
    clones();
  } else if (!strcmp(argv[2], "pressure")) {
    pressure(0);
    pressure(1);
  } else if (!strcmp(argv[2], "malformed"))
    malformed();
  else if (!strcmp(argv[2], "admission"))
    admission();
  else if (!strcmp(argv[2], "threads"))
    threads();
  else if (!strcmp(argv[2], "writer-death"))
    writer_death();
  else if (!strcmp(argv[2], "measure"))
    measure();
  else if (!strcmp(argv[2], "fragmented"))
    fragmented();
  else {
    REQUIRE(!strcmp(argv[2], "faults"));
    faults();
  }
  REQUIRE(rmdir(directory) == 0);
  fprintf(stderr, "reader pins %s: %zu checks\n", argv[2], checks);
  return 0;
}
