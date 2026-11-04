#ifndef LC_COLLECTION_INTERNAL_H
#define LC_COLLECTION_INTERNAL_H

#include "lc_api_internal.h"
#include "lc_collection_ids.h"
#include "lc_collection_maintenance.h"
#include "lc_collection_migration.h"
#include "lc_collection_order.h"
#include "lc_pouch_collection_record.h"
#include "lc_pouch_internal.h"

#define LC_COLLECTION_CURSOR_BYTES 256U
#define LC_COLLECTION_SNAPSHOT_BIT ((lc_u64)LC_I64_MAX + 1U)

typedef struct lc_collection_disk_head {
  lc_u64 active;
  lc_u64 snapshot;
  lc_u64 snapshot_generation;
  /* Latest immutable snapshot group: snapshot .. snapshot + extra_parts. */
  lc_u64 snapshot_extra_parts;
  lc_u64 tail_segment;
  lc_u64 tail_offset;
  lc_u64 next_snapshot;
  lc_u64 published_at;
  lc_pouch_unix_seconds retention;
  /* Explicit expiry overrides append directives through this generation. */
  lc_u64 retention_generation;
  lc_u64 max_entries;
  lc_collection_lease_info lease;
  int dropped;
} lc_collection_disk_head;

/* Unused canonical address ranges, owned exclusively by this handle. Only
 * metadata is retained; no payload or extra descriptor is held by idle ranges.
 * Append authority serializes transfer to/from a live upload. */
typedef struct lc_collection_region {
  lc_u64 segment, next, end;
} lc_collection_region;
#define LC_COLLECTION_IDLE_REGIONS 16U

typedef struct lc_collection_handle {
  lc_collection pub;
  lc_client_handle *client;
  lc_pouch *pouch;
  lc_allocator allocator;
  char *ns;
  char *name;
  char id[LC_XID_STRING_SIZE];
  char *directory;
  char *head_path;
  char *catalog_path;
  char *catalog_base;
  char *order_path;
  char *ids_path;
  int session_fd;
  int join_fd;
  int metadata_fd;
  int append_fd;
  int active_fd;
  int streams_fd;
  unsigned long stream_writers;
  lc_collection_region stream_regions[LC_COLLECTION_IDLE_REGIONS];
  /* Keep the immutable cached head inode alive across atomic replacements:
   * an unpinned device/inode pair can be reused for a different head. */
  int head_fd;
  lc_u64 active_fd_segment;
  pthread_mutex_t metadata_mutex;
  pthread_mutex_t append_mutex;
  int metadata_mutex_initialized;
  int append_mutex_initialized;
  lc_collection_disk_head head;
  uint64_t head_device;
  uint64_t head_inode;
  int head_loaded;
  lc_collection_order order;
  lc_collection_ids ids;
  unsigned long references;
  int closed;
  int trusted;
  /* Immutable receiver credentials, independent of refreshed control metadata.
   */
  char lease_id[LC_XID_STRING_SIZE];
  lc_u64 lease_token;
} lc_collection_handle;

typedef struct lc_collection_call {
  lc_collection_handle *collection;
  lc_pouch *pouch;
  pthread_t thread;
  /* Callback flags are confined to thread. Registry readers must test the
   * owning thread before examining them; membership and lifetime use the
   * lifecycle mutex. */
  int callback;
  int source_callback;
  int mode_guard;
  int maintenance;
  lc_error local_error;
  lc_error *error;
  struct lc_collection_call *next;
} lc_collection_call;

typedef struct lc_collection_guard {
  lc_collection_handle *collection;
  lc_pouch *pouch;
  lc_collection_call *call;
  int mode;
  int append;
  int commit_fd;
} lc_collection_guard;

int lc_collection_call_begin(lc_collection *self, int mutating,
                             lc_collection_call *call, lc_error *error);
void lc_collection_call_end(lc_collection_call *call);
/* A writer owns this independent reference, including after public close. */
void lc_collection_handle_hold(lc_collection_handle *collection);
void lc_collection_handle_release(lc_collection_handle *collection);
int lc_collection_call_begin_held(lc_collection_handle *collection,
                                  int mutating, lc_collection_call *call,
                                  lc_error *error);
int lc_collection_append_validate(lc_collection_handle *collection,
                                  const lc_collection_append_req *req,
                                  lc_error *error);
int lc_collection_append_begin_method(lc_collection *self,
                                      const lc_collection_append_req *req,
                                      lc_collection_writer **out,
                                      lc_error *error);
int lc_collection_append_staged_method(lc_collection *self,
                                       const lc_collection_append_req *req,
                                       lc_source *source,
                                       lc_collection_append_res *out,
                                       lc_error *error);
struct lc_collection_chunks;
/* Consumes the finished encoder and publishes its immutable canonical index. */
int lc_collection_publish_stream(lc_collection_call *call,
                                 lc_collection_guard *guard,
                                 lc_pouch_collection_writer *writer,
                                 lc_u64 ownership_generation,
                                 const lc_pouch_collection_record *body,
                                 struct lc_collection_chunks *storage,
                                 lc_collection_append_res *out,
                                 lc_error *error);
void lc_collection_call_callback(lc_collection_call *call, int enabled,
                                 int source);
int lc_collection_metadata_enter(lc_collection_handle *collection,
                                 int exclusive, lc_error *error);
void lc_collection_metadata_leave(lc_collection_handle *collection);
/* Capture waits for pending durable appends before taking metadata authority.
 * Release before callbacks. Appenders take their shared commit pin only after
 * consuming the source, so callback reads may still capture the previous view.
 */
int lc_collection_capture_enter(lc_collection_handle *collection, int *pin,
                                lc_error *error);
void lc_collection_capture_leave(lc_collection_handle *collection, int *pin);
/* Order for mutations: root, catalog, join/session, append, commit, metadata.
 * Snapshot capture prepares before taking commit authority and never waits
 * for append authority behind it. Root-only scopes
 * participate in callback nesting without a client or receiver. */
int lc_collection_root_begin(lc_pouch *pouch, lc_collection_call *scope,
                             lc_collection_guard *guard, lc_error *error);
void lc_collection_root_end(lc_collection_call *scope,
                            lc_collection_guard *guard);
/* Non-append mutation entry drains durable appends under an exclusive commit
 * pin and returns with metadata held; release metadata before write_leave. */
int lc_collection_write_enter(lc_collection_call *call,
                              lc_collection_guard *guard, lc_error *error);
/* Upload admission does not drain in-flight durable append groups. */
int lc_collection_append_enter(lc_collection_call *call,
                               lc_collection_guard *guard, lc_error *error);
/* Physical chunks require a valid append boundary, not an identity-table read.
 */
int lc_collection_stream_enter(lc_collection_call *, lc_collection_guard *,
                               lc_error *);

int lc_collection_edit_barrier(lc_collection_guard *, lc_error *);
void lc_collection_write_leave(lc_collection_guard *guard);
int lc_collection_head_write(lc_collection_handle *collection, lc_error *error);
int lc_collection_head_read(lc_collection_handle *collection, lc_error *error);
/* Root, commit, metadata authority, without waiting for upload append locks. */
int lc_collection_control_enter(lc_collection_call *call,
                                lc_collection_guard *guard, lc_error *error);
void lc_collection_control_leave(lc_collection_guard *guard);
int lc_collection_lease_check(lc_collection_handle *collection, int mutating,
                              lc_error *error);
int lc_collection_lease_observe(lc_collection_handle *collection,
                                lc_collection_lease_info *out, lc_error *error);
int lc_collection_lease_initialize(lc_collection_handle *collection,
                                   const lc_collection_lease_req *req,
                                   lc_error *error);
int lc_collection_acquire_method(lc_collection *,
                                 const lc_collection_lease_req *,
                                 lc_collection **, lc_error *);
int lc_collection_takeover_method(lc_collection *,
                                  const lc_collection_lease_req *, const char *,
                                  lc_collection **, lc_error *);
int lc_collection_keepalive_method(lc_collection *, long,
                                   lc_collection_lease_info *, lc_error *);
int lc_collection_release_method(lc_collection *, lc_error *);
lc_pouch_unix_seconds
lc_collection_retention_deadline(const lc_collection_handle *collection);
int lc_collection_rotate(lc_collection_handle *collection, int force,
                         lc_error *error);
/* Bound mid-upload overshoot while retaining normal publication rollovers. */
int lc_collection_rotate_stream(lc_collection_handle *, lc_error *);

int lc_collection_project_record(lc_collection_order *order,
                                 lc_collection_ids *ids,
                                 const lc_pouch_collection_record *record,
                                 lc_u64 segment, int snapshot, lc_error *error);
/* Disable tail repair while building replacement projections: canonical
 * history must be validated before discarding any unfinished active tail. */
int lc_collection_replay_into(lc_collection_handle *collection,
                              lc_collection_order *order,
                              lc_collection_ids *ids, lc_u64 segment,
                              lc_u64 offset, int snapshot, lc_u64 cutoff,
                              int repair, lc_error *error);
int lc_collection_lookup_id(lc_collection_handle *collection,
                            const char *append_id,
                            lc_pouch_collection_record *out, lc_u64 *segment,
                            int *found, lc_error *error);
/* Ensures a live session's projections include the canonical peer tail. A
 * reader can observe the previous committed view while an upload is active. */
int lc_collection_prepare(lc_collection_handle *collection, lc_error *error);
/* Called under metadata authority; pins one canonical topology until close. */
int lc_collection_read_pin(lc_collection_handle *collection, lc_u64 snapshot,
                           int *fd, lc_error *error);
int lc_collection_refresh_view(lc_collection_handle *collection,
                               lc_error *error);
int lc_collection_record_file(lc_collection_handle *, lc_u64, int *, lc_u64 *,
                              lc_u64 *, lc_error *);
int lc_collection_record_ref(lc_collection_handle *collection, lc_u64 segment,
                             lc_u64 offset, int *fd, lc_u64 *fd_segment,
                             lc_u64 *fd_end, lc_pouch_collection_record *out,
                             lc_error *error);
/* Resolves metadata history for a pinned read generation. The current record
 * must already be loaded, and fd describes it. Only bounded metadata is read.
 */
int lc_collection_record_at(lc_collection_handle *collection, lc_u64 generation,
                            int *fd, lc_u64 *fd_segment, lc_u64 *fd_end,
                            lc_pouch_collection_record *record,
                            lc_error *error);
int lc_collection_record_body(lc_collection_handle *collection,
                              const lc_pouch_collection_record *record, int *fd,
                              lc_u64 *fd_segment, lc_u64 *fd_end,
                              lc_error *error);
int lc_collection_record_path(lc_collection_handle *collection, lc_u64 segment,
                              char **out, char leaf[64], lc_error *error);
int lc_collection_record_leaf(lc_u64 segment, char leaf[64], lc_error *error);
int lc_collection_hash(const void *bytes, size_t length, unsigned char out[32],
                       lc_error *error);
int lc_collection_head_token(lc_collection_handle *, char[65], lc_error *);
int lc_collection_head_match(lc_collection_handle *, const char *, lc_error *);
int lc_collection_head_method(lc_collection *, lc_collection_head_res *,
                              lc_error *);
int lc_collection_edit_method(lc_collection *, const lc_collection_edit_req *,
                              lc_source *, lc_collection_edit_res *,
                              lc_error *);
int lc_collection_edit_status_method(lc_collection *, const char *,
                                     lc_collection_edit_status_res *,
                                     lc_error *);
int lc_collection_edit_project(lc_collection_handle *, lc_collection_order *,
                               lc_collection_ids *, int,
                               const lc_pouch_collection_record *,
                               const lc_pouch_collection_record *, lc_u64, int,
                               lc_error *);
typedef int (*lc_collection_compact_output_fn)(void *, lc_u64, int *,
                                               const char **, lc_u64 *,
                                               lc_error *);
int lc_collection_edit_compact(lc_collection_handle *, lc_collection_ids *,
                               lc_u64, lc_collection_compact_output_fn, void *,
                               lc_collection_order *, lc_collection_ids *,
                               unsigned char *, size_t, lc_error *);
lc_u64 lc_collection_binding_count(const lc_collection_order *);
enum {
  LC_COLLECTION_EDIT_PREPARED = 1,
  LC_COLLECTION_EDIT_PUBLISHED,
  LC_COLLECTION_EDIT_DIRTY,
  LC_COLLECTION_EDIT_PROJECTED,
  LC_COLLECTION_EDIT_PAGE,
  LC_COLLECTION_EDIT_BODY
};
#ifdef LOCKDC_TEST_BUILD
typedef int (*lc_collection_edit_hook_fn)(void *, lc_collection *, int,
                                          lc_error *);
extern lc_collection_edit_hook_fn lc_collection_edit_test_hook;
extern void *lc_collection_edit_test_context;
extern int (*lc_collection_test_rotate_sync_fd)(int);
#endif
int lc_collection_failure(lc_error *error, int code, const char *reason,
                          const char *message);
int lc_collection_read_method(lc_collection *self,
                              const lc_collection_read_opts *opts,
                              lc_sink *sink, lc_collection_read_res *out,
                              lc_error *error);
int lc_collection_catalog_read(const char *path, char id[LC_XID_STRING_SIZE],
                               char name[256], lc_error *error);
int lc_collection_catalog_write(const char *path, const char *id,
                                const char *name, lc_error *error);
int lc_collection_catalog_name_path(const lc_allocator *allocator,
                                    const char *base, const char *name,
                                    char **out, lc_error *error);
int lc_collection_compact(lc_collection *collection, int force,
                          int cleanup_only, lc_pouch_maintenance_result *out,
                          lc_error *error);
int lc_collection_expire(lc_collection *collection, lc_pouch_unix_seconds now,
                         int *removed, lc_error *error);
int lc_collection_expire_entry(lc_collection *collection, uint64_t position,
                               lc_pouch_unix_seconds now,
                               lc_collection_delete_res *out, lc_error *error);
/* Rechecks the current live count and policy while holding mutation authority;
 * a zero deleted result means no surplus remains. Uses canonical deletion. */
int lc_collection_trim_entry(lc_collection *collection,
                             lc_collection_delete_res *out, lc_error *error);
int lc_collection_retention_maintain(lc_collection *collection, int *dropped,
                                     lc_pouch_maintenance_result *out,
                                     lc_error *error);
/* Caller owns compaction, append and metadata authority. */
int lc_collection_cleanup_files(lc_collection_handle *collection,
                                lc_pouch_maintenance_result *out,
                                lc_error *error);
enum {
  LC_COLLECTION_COMPACT_CAPTURED = 1,
  LC_COLLECTION_COMPACT_COPIED,
  LC_COLLECTION_COMPACT_HEAD_PUBLISHED,
  LC_COLLECTION_COMPACT_ORDER_PUBLISHED,
  LC_COLLECTION_COMPACT_IDS_PUBLISHED
};
typedef int (*lc_collection_compact_hook_fn)(void *, lc_collection *, int,
                                             lc_error *);
extern lc_collection_compact_hook_fn lc_collection_compact_test_hook;
extern void *lc_collection_compact_test_context;
#ifdef LOCKDC_TEST_BUILD
typedef void (*lc_collection_root_hook_fn)(void *, lc_pouch *);
extern lc_collection_root_hook_fn lc_collection_root_test_hook;
extern void *lc_collection_root_test_context;
typedef int (*lc_collection_commit_hook_fn)(void *, lc_collection *,
                                            lc_error *);
extern lc_collection_commit_hook_fn lc_collection_commit_wait_test_hook;
extern void *lc_collection_commit_wait_test_context;
enum {
  LC_COLLECTION_APPEND_STAGED = 1,
  LC_COLLECTION_APPEND_PUBLISHED,
  LC_COLLECTION_APPEND_PROJECTED,
  LC_COLLECTION_APPEND_CHUNK_PENDING,
  LC_COLLECTION_APPEND_CHUNK_WRITTEN,
  LC_COLLECTION_APPEND_CHUNK_LINKED
};
typedef int (*lc_collection_append_hook_fn)(void *, lc_collection *, int,
                                            lc_error *);
extern lc_collection_append_hook_fn lc_collection_append_test_hook;
extern void *lc_collection_append_test_context;
#endif

struct lc_pouch_recode;
int lc_collection_recode_head(lc_pouch *pouch, const char *directory,
                              lc_collection_disk_head *head, lc_error *error);
int lc_collection_recode_finish(struct lc_pouch_recode *work,
                                const char *source, const char *target,
                                const char *id, const char *query_id,
                                lc_error *error);

int lc_collection_catalog_paths(lc_client_handle *client, const char *ns,
                                const char *name, int create, char **base,
                                char **catalog, lc_error *error);
int lc_collection_catalog_bound(lc_collection_handle *collection,
                                lc_error *error);
int lc_collection_catalog_lock(const char *base, int *fd, lc_error *error);
int lc_collection_open_bound(lc_client_handle *client, const char *ns,
                             const char *name, const char *id, const char *base,
                             const char *catalog, int create,
                             lc_pouch_unix_seconds retention,
                             lc_u64 max_entries, lc_collection **out,
                             lc_error *error);
/* Internal maintenance borrows its existing root; public receivers retain
 * their owning client. Both use the same storage and allocator paths. */
int lc_collection_storage_open(lc_pouch *pouch, lc_client_handle *owner,
                               const char *ns, const char *name, const char *id,
                               const char *base, const char *catalog,
                               int create, lc_pouch_unix_seconds retention,
                               lc_u64 max_entries, lc_collection **out,
                               lc_error *error);

#endif
