#include "outbox_markers.h"
#include "lc_api_internal.h"
#include "namespace_work.h"
#include "owned.h"
#include "root_internal.h"
#include <string.h>
#include <zlib.h>

typedef struct marker_store {
  ps_root *root;
  ps_namespace *ns;
  lc_allocator allocator;
  int kind, captured;
  char **names;
  size_t count, capacity, cursor;
} marker_store;
static int failure(lc_error *e, int code, const char *message) {
  return lc_error_set(e, code, 0L, message, NULL, NULL, "pouch");
}
typedef struct ps_outbox_marker_gate {
  char *name;
  int kind, busy;
  size_t users;
  struct ps_outbox_marker_gate *next;
} ps_outbox_marker_gate;
int ps_outbox_marker_enter(ps_namespace *ns, int kind, const char *name,
                           void **out, lc_error *e) {
  ps_outbox_marker_gate *gate;
  int rc = LC_OK;
  if (out)
    *out = NULL;
  if (!ns || !name || !name[0] || !out || kind < -1 || kind > 3)
    return failure(e, LC_ERR_INVALID, "invalid outbox marker authority");
  pthread_mutex_lock(&ns->outbox);
  for (gate = ns->outbox_writers; gate; gate = gate->next)
    if (gate->kind == kind && !strcmp(gate->name, name))
      break;
  if (!gate) {
    gate = lc_calloc_with_allocator(&ns->root->crypto_allocator, 1U,
                                    sizeof(*gate));
    if (gate)
      gate->name = lc_strdup_with_allocator(&ns->root->crypto_allocator, name);
    if (!gate || !gate->name) {
      lc_free_with_allocator(&ns->root->crypto_allocator, gate);
      pthread_mutex_unlock(&ns->outbox);
      return failure(e, LC_ERR_NOMEM,
                     "cannot allocate outbox marker authority");
    }
    gate->kind = kind;
    gate->next = ns->outbox_writers;
    ns->outbox_writers = gate;
  }
  ++gate->users;
  while (gate->busy && rc == LC_OK)
    if (pthread_cond_wait(&ns->outbox_changed, &ns->outbox) != 0)
      rc = failure(e, LC_ERR_TRANSPORT,
                   "cannot wait for outbox marker authority");
  if (rc == LC_OK) {
    gate->busy = 1;
    *out = gate;
  } else
    --gate->users;
  pthread_mutex_unlock(&ns->outbox);
  return rc;
}
void ps_outbox_marker_leave(ps_namespace *ns, void *pin) {
  ps_outbox_marker_gate *gate = pin, **link;
  int retire;
  if (!gate)
    return;
  pthread_mutex_lock(&ns->outbox);
  gate->busy = 0;
  retire = --gate->users == 0U;
  if (retire) {
    for (link = &ns->outbox_writers; *link != gate; link = &(*link)->next) {
    }
    *link = gate->next;
  }
  pthread_cond_broadcast(&ns->outbox_changed);
  pthread_mutex_unlock(&ns->outbox);
  if (retire) {
    lc_free_with_allocator(&ns->root->crypto_allocator, gate->name);
    lc_free_with_allocator(&ns->root->crypto_allocator, gate);
  }
}
size_t ps_outbox_marker_waiter_count(ps_namespace *ns, int kind,
                                     const char *name) {
  ps_outbox_marker_gate *gate;
  size_t count = 0U;
  pthread_mutex_lock(&ns->outbox);
  for (gate = ns->outbox_writers; gate; gate = gate->next)
    if (gate->kind == kind && !strcmp(gate->name, name)) {
      count = gate->users - (gate->busy ? 1U : 0U);
      break;
    }
  pthread_mutex_unlock(&ns->outbox);
  return count;
}
static ps_key control_key(uint64_t owner) {
  ps_key key;
  key.owner = owner;
  key.key = 1U;
  key.kind = PS_CONTROL;
  return key;
}
static ps_key role_key(uint64_t owner) {
  ps_key key = control_key(owner);
  key.key = PS_OUTBOX_MARKER_ROLE_KEY;
  return key;
}
static uint64_t get64(const unsigned char *bytes) {
  uint64_t value = 0U;
  size_t i;
  for (i = 0U; i < 8U; ++i)
    value |= (uint64_t)bytes[i] << (8U * i);
  return value;
}
static void put64(unsigned char *bytes, uint64_t value) {
  size_t i;
  for (i = 0U; i < 8U; ++i)
    bytes[i] = (unsigned char)(value >> (8U * i));
}
static int role_read(ps_space *space, uint64_t owner, const ps_ref *ref,
                     int *present, lc_error *e) {
  ps_reader *reader = NULL;
  unsigned char bytes[32];
  size_t got = 0U;
  int rc;
  *present = 0;
  rc = ps_read_open(space, role_key(owner), NULL, &reader, e);
  if (rc == LC_ERR_INVALID) {
    lc_error_cleanup(e);
    return LC_OK;
  }
  if (rc == LC_OK && ps_read_size(reader) != sizeof(bytes))
    rc =
        failure(e, LC_ERR_PROTOCOL, "outbox marker owner tag has invalid size");
  if (rc == LC_OK)
    rc = ps_read(reader, bytes, sizeof(bytes), &got, e);
  ps_reader_close(reader);
  if (rc != LC_OK)
    return rc;
  if (got != sizeof(bytes) || memcmp(bytes, "LCPSOBX1", 8U) ||
      get64(bytes + 8U) != owner || get64(bytes + 16U) != ref->incarnation ||
      get64(bytes + 24U) != (uint64_t)crc32(0L, bytes, 24U))
    return failure(e, LC_ERR_PROTOCOL, "outbox marker owner tag is invalid");
  *present = 1;
  return LC_OK;
}
static int identity(const ps_catalog_info *info, lc_error *e) {
  if (info->stream.kind != PS_STREAM || !info->stream.owner ||
      info->stream.key != PS_OUTBOX_MARKER_STREAM_KEY)
    return failure(e, LC_ERR_PROTOCOL,
                   "outbox marker has invalid physical identity");
  return LC_OK;
}
static int catalog_name(marker_store *store, const char *name,
                        unsigned char **out, size_t *length, lc_error *e) {
  unsigned char *bytes;
  size_t size = strlen(name);
  *out = NULL;
  if (!size || size >= PS_CATALOG_NAME_MAX)
    return failure(e, LC_ERR_INVALID,
                   "outbox marker identity exceeds catalog bounds");
  bytes = lc_alloc_with_allocator(&store->allocator, size + 1U);
  if (!bytes)
    return failure(e, LC_ERR_NOMEM, "cannot allocate outbox marker identity");
  bytes[0] = (unsigned char)(store->kind + 1);
  memcpy(bytes + 1U, name, size);
  *length = size + 1U;
  *out = bytes;
  return LC_OK;
}
static int binding(marker_store *store, const char *name, ps_catalog_view **out,
                   ps_catalog_info *info, lc_error *e) {
  unsigned char *bytes = NULL;
  size_t size = 0U;
  int rc, present = 0;
  *out = NULL;
  if (!store->ns)
    return LC_OK;
  rc = catalog_name(store, name, &bytes, &size, e);
  if (rc != LC_OK)
    return rc;
  rc = ps_catalog_get(store->root->space, store->ns->identity.owner,
                      PS_CATALOG_CONTROL, bytes, size, out, e);
  lc_free_with_allocator(&store->allocator, bytes);
  if (rc == LC_OK && *out)
    rc = ps_catalog_view_info(*out, info, e);
  if (rc == LC_OK && *out)
    rc = identity(info, e);
  if (rc == LC_OK && *out)
    rc = role_read(store->root->space, info->stream.owner, &info->target,
                   &present, e);
  if (rc == LC_OK && *out && !present)
    rc = failure(e, LC_ERR_PROTOCOL, "outbox marker owner tag is missing");
  return rc;
}
static int capture(void *context, const void *name, size_t length,
                   const ps_catalog_info *info, lc_error *e) {
  marker_store *store = context;
  const unsigned char *bytes = name;
  char **grown, *copy;
  size_t capacity;
  int rc;
  if (!length || bytes[0] != (unsigned char)(store->kind + 1))
    return LC_OK;
  rc = identity(info, e);
  if (rc != LC_OK)
    return rc;
  if (length < 2U || memchr(bytes + 1U, 0, length - 1U))
    return failure(e, LC_ERR_PROTOCOL,
                   "outbox marker has invalid catalog name");
  if (store->count == store->capacity) {
    capacity = store->capacity ? store->capacity * 2U : 16U;
    if (capacity < store->capacity || capacity > (size_t)-1 / sizeof(*grown))
      return failure(e, LC_ERR_NOMEM,
                     "outbox marker inventory exceeds local bounds");
    grown = lc_realloc_with_allocator(&store->allocator, store->names,
                                      capacity * sizeof(*grown));
    if (!grown)
      return failure(e, LC_ERR_NOMEM, "cannot capture outbox marker names");
    store->names = grown;
    store->capacity = capacity;
  }
  copy = lc_alloc_with_allocator(&store->allocator, length);
  if (!copy)
    return failure(e, LC_ERR_NOMEM, "cannot copy outbox marker name");
  memcpy(copy, bytes + 1U, length - 1U);
  copy[length - 1U] = 0;
  store->names[store->count++] = copy;
  return LC_OK;
}
static int next(void *context, const char **out, lc_error *e) {
  marker_store *store = context;
  int rc = ps_check(store->root->space, e);
  *out = NULL;
  if (rc != LC_OK || !store->ns)
    return rc;
  if (!store->captured) {
    rc = ps_catalog_visit(store->root->space, store->ns->identity.owner,
                          PS_CATALOG_CONTROL, capture, store, e);
    if (rc == LC_OK)
      store->captured = 1;
  }
  if (rc == LC_OK && store->cursor < store->count)
    *out = store->names[store->cursor++];
  return rc;
}
static int read_marker(void *context, const char *name, const lc_allocator *a,
                       size_t minimum, size_t maximum, unsigned char **out,
                       size_t *length, lc_error *e) {
  marker_store *store = context;
  ps_catalog_view *view = NULL;
  ps_catalog_info info;
  ps_reader *reader = NULL;
  unsigned char *bytes = NULL;
  uint64_t size = 0U;
  size_t got = 0U;
  int rc;
  *out = NULL;
  *length = 0U;
  rc = ps_check(store->root->space, e);
  if (rc == LC_OK)
    rc = binding(store, name, &view, &info, e);
  if (rc == LC_OK && !view)
    rc = LC_POUCH_OUTBOX_MARKER_MISSING;
  if (rc == LC_OK) {
    rc = ps_read_open(store->root->space, control_key(info.stream.owner), NULL,
                      &reader, e);
    if (rc == LC_ERR_INVALID)
      rc = failure(e, LC_ERR_PROTOCOL, "outbox marker body is unavailable");
  }
  if (rc == LC_OK) {
    size = ps_read_size(reader);
    if (size < minimum || size > maximum ||
        size > LC_POUCH_OUTBOX_MARKER_MAX_BYTES)
      rc = failure(e, LC_ERR_PROTOCOL,
                   "outbox marker body exceeds codec bounds");
  }
  if (rc == LC_OK) {
    bytes = lc_alloc_with_allocator(a, size ? (size_t)size : 1U);
    if (!bytes)
      rc = failure(e, LC_ERR_NOMEM, "cannot read outbox marker metadata");
  }
  if (rc == LC_OK)
    rc = ps_read(reader, bytes, (size_t)size, &got, e);
  if (rc == LC_OK && got != size)
    rc = failure(e, LC_ERR_PROTOCOL, "outbox marker body is incomplete");
  ps_reader_close(reader);
  ps_catalog_view_close(view);
  if (rc == LC_OK) {
    *out = bytes;
    *length = (size_t)size;
  } else
    lc_free_with_allocator(a, bytes);
  return rc;
}
int ps_outbox_marker_reclaim_child(ps_space *space, uint64_t owner,
                                   const ps_ref *ref, lc_error *e) {
  ps_reader *reader = NULL;
  int present = 0;
  int rc = role_read(space, owner, ref, &present, e);
  if (rc != LC_OK || !present)
    return rc;
  rc = ps_read_open(space, control_key(owner), NULL, &reader, e);
  if (rc == LC_ERR_INVALID) {
    lc_error_cleanup(e);
    rc = LC_OK;
  } else if (rc == LC_OK)
    rc = ps_delete(space, control_key(owner), e);
  ps_reader_close(reader);
  if (rc == LC_OK)
    rc = ps_delete(space, role_key(owner), e);
  return rc;
}
static void retire_unbound(marker_store *store, ps_key key, const ps_ref *ref) {
  ps_owned_head *head = NULL;
  ps_owned_status status;
  lc_error ignored;
  int rc;
  lc_error_init(&ignored);
  rc = ps_owned_open(store->root->space, NULL, key, 0, ref, &head, &status,
                     &ignored);
  if (rc == LC_OK)
    rc = ps_owned_claim_orphan(head, &ignored);
  if (rc == LC_OK)
    rc = ps_outbox_marker_reclaim_child(store->root->space, key.owner, ref,
                                        &ignored);
  if (rc == LC_OK)
    (void)ps_owned_retire(head, &ignored);
  ps_owned_close(head);
  lc_error_cleanup(&ignored);
}
static int write_marker(void *context, const char *name,
                        const unsigned char *bytes, size_t size, lc_error *e) {
  marker_store *store = context;
  ps_catalog_view *old = NULL;
  ps_catalog_info prior, selected;
  ps_owned_head *head = NULL;
  ps_owned_status status;
  ps_writer *writer = NULL;
  unsigned char role[32];
  ps_key key;
  unsigned char *encoded = NULL;
  void *authority = NULL;
  size_t length = 0U;
  uint64_t owner = 0U;
  int rc, created = 0, published = 0;
  if (!store->ns || size > LC_POUCH_OUTBOX_MARKER_MAX_BYTES)
    return failure(e, LC_ERR_INVALID, "invalid outbox marker write");
  rc =
      ps_namespace_work_ensure(store->ns, LC_POUCH_STORAGE_WORK_ORDINARY, 0, e);
  if (rc != LC_OK)
    return rc;
  rc = catalog_name(store, name, &encoded, &length, e);
  if (rc != LC_OK)
    return rc;
  rc = ps_outbox_marker_enter(store->ns, store->kind, name, &authority, e);
  if (rc != LC_OK) {
    lc_free_with_allocator(&store->allocator, encoded);
    return rc;
  }
  rc = binding(store, name, &old, &prior, e);
  if (rc == LC_OK)
    rc = ps_catalog_allocate_id(store->root->space, &owner, e);
  key.owner = owner;
  key.key = PS_OUTBOX_MARKER_STREAM_KEY;
  key.kind = PS_STREAM;
  if (rc == LC_OK) {
    rc = ps_owned_open(store->root->space, NULL, key, 1, NULL, &head, &status,
                       e);
    created = rc == LC_OK;
  }
  if (rc == LC_OK)
    rc = ps_owned_seal(head, e);
  if (rc == LC_OK) {
    memset(role, 0, sizeof(role));
    memcpy(role, "LCPSOBX1", 8U);
    put64(role + 8U, owner);
    put64(role + 16U, status.ref.incarnation);
    put64(role + 24U, (uint64_t)crc32(0L, role, 24U));
    rc = ps_begin(store->root->space, NULL, role_key(owner), &writer, e);
  }
  if (rc == LC_OK)
    rc = ps_write(writer, role, sizeof(role), e);
  if (rc == LC_OK)
    rc = ps_publish(writer, NULL, e);
  ps_writer_close(writer);
  writer = NULL;
  if (rc == LC_OK)
    rc = ps_begin(store->root->space, NULL, control_key(owner), &writer, e);
  if (rc == LC_OK)
    rc = ps_write(writer, bytes, size, e);
  if (rc == LC_OK)
    rc = ps_publish(writer, NULL, e);
  ps_writer_close(writer);
  if (rc == LC_OK) {
    rc = ps_catalog_put(store->root->space, NULL, store->ns->identity.owner,
                        PS_CATALOG_CONTROL, encoded, length,
                        old ? &prior.revision : NULL, key, &status.ref,
                        &selected, e);
    published = rc == LC_OK;
  }
  ps_owned_close(head);
  ps_catalog_view_close(old);
  if (created && !published)
    retire_unbound(store, key, &status.ref);
  if (published && old)
    retire_unbound(store, prior.stream, &prior.target);
  if (rc == LC_OK)
    rc = ps_check(store->root->space, e);
  ps_outbox_marker_leave(store->ns, authority);
  lc_free_with_allocator(&store->allocator, encoded);
  return rc;
}
static int remove_marker(void *context, const char *name, lc_error *e) {
  marker_store *store = context;
  ps_catalog_view *view = NULL;
  ps_catalog_info info;
  unsigned char *encoded = NULL;
  void *authority = NULL;
  size_t length = 0U;
  int rc = ps_check(store->root->space, e), removed = 0;
  if (rc != LC_OK || !store->ns)
    return rc;
  rc = catalog_name(store, name, &encoded, &length, e);
  if (rc != LC_OK)
    return rc;
  rc = ps_outbox_marker_enter(store->ns, store->kind, name, &authority, e);
  if (rc != LC_OK) {
    lc_free_with_allocator(&store->allocator, encoded);
    return rc;
  }
  rc = binding(store, name, &view, &info, e);
  if (rc == LC_OK && view) {
    rc = ps_catalog_remove(store->root->space, store->ns->identity.owner,
                           PS_CATALOG_CONTROL, encoded, length, &info.revision,
                           e);
    removed = rc == LC_OK;
  }
  ps_catalog_view_close(view);
  if (removed)
    retire_unbound(store, info.stream, &info.target);
  if (rc == LC_OK)
    rc = ps_check(store->root->space, e);
  ps_outbox_marker_leave(store->ns, authority);
  lc_free_with_allocator(&store->allocator, encoded);
  return rc;
}
static int close_store(void *context, lc_error *e) {
  marker_store *store = context;
  lc_allocator a = store->allocator;
  size_t i;
  (void)e;
  for (i = 0U; i < store->count; ++i)
    lc_free_with_allocator(&a, store->names[i]);
  lc_free_with_allocator(&a, store->names);
  ps_namespace_release(store->ns);
  lc_free_with_allocator(&a, store);
  return LC_OK;
}
int ps_outbox_markers_open(ps_root *root, const char *ns, int kind, int create,
                           lc_pouch_outbox_store *out, lc_error *e) {
  static const lc_pouch_outbox_store_ops operations = {
      next, read_marker, write_marker, remove_marker, close_store};
  marker_store *store;
  int rc;
  memset(out, 0, sizeof(*out));
  if (!root || !ns || !ns[0] || kind < -1 || kind > 3)
    return failure(e, LC_ERR_INVALID, "invalid container outbox marker store");
  store = lc_calloc_with_allocator(&root->crypto_allocator, 1U, sizeof(*store));
  if (!store)
    return failure(e, LC_ERR_NOMEM,
                   "cannot allocate container outbox marker store");
  store->allocator = root->crypto_allocator;
  store->root = root;
  store->kind = kind;
  rc = ps_root_namespace_acquire(root, ns, create, &store->ns, e);
  if (!create && rc == LC_ERR_INVALID && e && e->server_code &&
      !strcmp(e->server_code, "pouch_namespace_not_found")) {
    lc_error_cleanup(e);
    rc = LC_OK;
  }
  if (rc == LC_OK) {
    out->ops = &operations;
    out->context = store;
  } else
    (void)close_store(store, NULL);
  return rc;
}
