#include "outbox_markers.h"
#include "lc_api_internal.h"
#include "root_internal.h"
#include <string.h>

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
static int capture(void *context, const void *name, size_t length,
                   const ps_catalog_info *info, lc_error *e) {
  marker_store *store = context;
  const unsigned char *bytes = name;
  char **grown, *copy;
  size_t capacity;
  (void)info;
  if (!length || bytes[0] != (unsigned char)(store->kind + 1))
    return LC_OK;
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
                          PS_CATALOG_MARKER, capture, store, e);
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
  const unsigned char *value;
  unsigned char *encoded = NULL;
  unsigned char *bytes = NULL;
  size_t size, name_length;
  int rc;
  *out = NULL;
  *length = 0U;
  rc = ps_check(store->root->space, e);
  if (rc == LC_OK && store->ns) {
    rc = catalog_name(store, name, &encoded, &name_length, e);
    if (rc == LC_OK)
      rc = ps_catalog_get(store->root->space, store->ns->identity.owner,
                          PS_CATALOG_MARKER, encoded, name_length, &view, e);
    lc_free_with_allocator(&store->allocator, encoded);
  }
  if (rc == LC_OK && !view)
    rc = LC_POUCH_OUTBOX_MARKER_MISSING;
  if (rc == LC_OK)
    rc = ps_catalog_view_marker(view, &value, &size, e);
  if (rc == LC_OK) {
    if (size < minimum || size > maximum ||
        size > LC_POUCH_OUTBOX_MARKER_MAX_BYTES)
      rc = failure(e, LC_ERR_PROTOCOL,
                   "outbox marker body exceeds codec bounds");
  }
  if (rc == LC_OK) {
    bytes = lc_alloc_with_allocator(a, size ? size : 1U);
    if (!bytes)
      rc = failure(e, LC_ERR_NOMEM, "cannot read outbox marker metadata");
  }
  if (rc == LC_OK) {
    if (size)
      memcpy(bytes, value, size);
    *out = bytes;
    *length = size;
  }
  ps_catalog_view_close(view);
  return rc;
}
static int change_marker(marker_store *store, const char *name,
                         const unsigned char *value, size_t size, int remove,
                         lc_error *e) {
  ps_catalog_view *view = NULL;
  ps_catalog_info prior, selected;
  unsigned char *encoded = NULL;
  void *authority = NULL;
  size_t length;
  int rc = ps_check(store->root->space, e);
  if (rc != LC_OK || (remove && !store->ns))
    return rc;
  if (!store->ns || size > LC_POUCH_OUTBOX_MARKER_MAX_BYTES)
    return failure(e, LC_ERR_INVALID, "invalid outbox marker write");
  rc = catalog_name(store, name, &encoded, &length, e);
  if (rc == LC_OK)
    rc = ps_outbox_marker_enter(store->ns, store->kind, name, &authority, e);
  if (rc == LC_OK)
    rc = ps_catalog_get(store->root->space, store->ns->identity.owner,
                        PS_CATALOG_MARKER, encoded, length, &view, e);
  if (rc == LC_OK && view)
    rc = ps_catalog_view_info(view, &prior, e);
  if (rc == LC_OK && !remove)
    rc = ps_catalog_put_marker(store->root->space, store->ns->identity.owner,
                               encoded, length, view ? &prior.revision : NULL,
                               value, size, &selected, e);
  if (rc == LC_OK && remove && view)
    rc = ps_catalog_remove(store->root->space, store->ns->identity.owner,
                           PS_CATALOG_MARKER, encoded, length, &prior.revision,
                           e);
  ps_catalog_view_close(view);
  ps_outbox_marker_leave(store->ns, authority);
  lc_free_with_allocator(&store->allocator, encoded);
  return rc;
}
static int write_marker(void *context, const char *name,
                        const unsigned char *bytes, size_t size, lc_error *e) {
  return change_marker(context, name, bytes, size, 0, e);
}
static int remove_marker(void *context, const char *name, lc_error *e) {
  return change_marker(context, name, NULL, 0, 1, e);
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
