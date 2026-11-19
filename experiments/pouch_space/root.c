#include "root.h"
#include "budget.h"
#include "collections.h"
#include "coordination.h"
#include "lc_api_internal.h"
#include "lc_collection_head.h"
#include "lc_pouch_record.h"
#include "namespace.h"
#include "outbox_markers.h"
#include "owned.h"
#include "root_internal.h"
#include <pthread.h>
#include <string.h>
#include <zlib.h>

#define ROOT_CONFIG_BYTES 128U
#define ROOT_KEY_ID_BYTES 80U
static const unsigned char config_magic[8] = {'L', 'C', 'P', 'S',
                                              'R', 'O', 'O', 'T'};
static int reserve_base(ps_root *root, int recover, lc_error *e) {
  ps_budget_plan plan = {0};
  int rc;
  /* One bounded replacement, an optional successor and its routing, plus a
   * newly required maintenance hint. Key deletion adds owner plans, rather
   * than guessing a fixed largest key size. Retention phases remain pending. */
  rc = ps_budget_plan_maintenance(&plan, 0, e);
  if (rc == LC_OK)
    rc = ps_reserve_base(root->space, &plan, recover, e);
  return rc;
}
static int recover_namespace(const char *name,
                             const ps_namespace_info *identity, void *context,
                             lc_error *error) {
  ps_root *root = context;
  ps_namespace *ns = NULL;
  int rc;
  (void)identity; /* The catalog visitor has already checked the incarnation. */
  rc = ps_root_namespace_acquire(root, name, 0, &ns, error);
  if (rc == LC_OK)
    rc = ps_namespace_checkpoint_recover(ns, error);
  ps_namespace_release(ns);
  return rc;
}
static int failure(lc_error *error, int code, const char *reason,
                   const char *message) {
  return lc_error_set(error, code, 0L, message, NULL, reason, "pouch");
}
static void put32(unsigned char *p, uint32_t v) {
  size_t i;
  for (i = 0; i < 4; ++i) {
    p[i] = (unsigned char)v;
    v >>= 8;
  }
}
static uint32_t get32(const unsigned char *p) {
  return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 |
         (uint32_t)p[3] << 24;
}
static ps_key config_key(void) {
  ps_key key = {0, 1, PS_CONTROL};
  return key;
}
int ps_root_config_write(ps_root *root, lc_pouch_crypto *crypto,
                         lc_error *error) {
  unsigned char bytes[ROOT_CONFIG_BYTES];
  char *key_id = NULL;
  ps_writer *writer = NULL;
  int rc = LC_OK;
  memset(bytes, 0, sizeof(bytes));
  memcpy(bytes, config_magic, sizeof(config_magic));
  bytes[8] = (unsigned char)lc_pouch_crypto_enabled(crypto);
  bytes[9] = (unsigned char)lc_pouch_crypto_compression_enabled(crypto);
  put32(bytes + 96, LC_COLLECTION_HEAD_BYTES);
  put32(bytes + 100, LC_POUCH_RECORD_HEADER_BYTES);
  if (bytes[8]) {
    rc = lc_pouch_crypto_key_id(crypto, &key_id, error);
    if (rc == LC_OK && strlen(key_id) >= ROOT_KEY_ID_BYTES)
      rc = failure(error, LC_ERR_PROTOCOL, "pouch_root_corrupt",
                   "pouch root key identity exceeds its control field");
    if (rc == LC_OK)
      memcpy(bytes + 16, key_id, strlen(key_id));
  }
  lc_free_with_allocator(NULL, key_id);
  put32(bytes + 124, (uint32_t)crc32(0L, bytes, 124));
  if (rc == LC_OK)
    rc = ps_begin(root->space, NULL, config_key(), &writer, error);
  if (rc == LC_OK)
    rc = ps_write(writer, bytes, sizeof(bytes), error);
  if (rc == LC_OK)
    rc = ps_publish(writer, NULL, error);
  ps_writer_close(writer);
  return rc;
}
static int config_read(ps_root *root, lc_error *error) {
  unsigned char bytes[ROOT_CONFIG_BYTES];
  ps_reader *reader = NULL;
  char *key_id = NULL;
  size_t got = 0, i, end;
  int rc = ps_read_open(root->space, config_key(), NULL, &reader, error);
  if (rc == LC_ERR_INVALID)
    return failure(
        error, LC_ERR_PROTOCOL, "pouch_root_uninitialized",
        "pouch root initialization did not commit its control metadata");
  if (rc != LC_OK)
    return rc;
  if (ps_read_size(reader) != sizeof(bytes))
    rc = failure(error, LC_ERR_PROTOCOL, "pouch_root_corrupt",
                 "pouch root control has invalid length");
  if (rc == LC_OK)
    rc = ps_read(reader, bytes, sizeof(bytes), &got, error);
  ps_reader_close(reader);
  if (rc != LC_OK)
    return rc;
  if (got != sizeof(bytes) || memcmp(bytes, config_magic, 8) || bytes[8] > 1 ||
      bytes[9] > 1 || get32(bytes + 96) != LC_COLLECTION_HEAD_BYTES ||
      get32(bytes + 100) != LC_POUCH_RECORD_HEADER_BYTES ||
      get32(bytes + 124) != (uint32_t)crc32(0L, bytes, 124))
    return failure(error, LC_ERR_PROTOCOL, "pouch_root_corrupt",
                   "pouch root control is invalid");
  for (i = 10; i < 16; ++i)
    if (bytes[i])
      goto corrupt;
  for (i = 104; i < 124; ++i)
    if (bytes[i])
      goto corrupt;
  for (end = 0; end < ROOT_KEY_ID_BYTES && bytes[16 + end]; ++end) {
  }
  if (end == ROOT_KEY_ID_BYTES || (!!end != !!bytes[8]))
    goto corrupt;
  for (i = end; i < ROOT_KEY_ID_BYTES; ++i)
    if (bytes[16 + i])
      goto corrupt;
  if (bytes[8] != lc_pouch_crypto_enabled(root->crypto))
    return failure(
        error, LC_ERR_INVALID, "pouch_root_crypto_mismatch",
        "pouch root encryption mode does not match initialized root");
  if (bytes[8]) {
    rc = lc_pouch_crypto_key_id(root->crypto, &key_id, error);
    if (rc != LC_OK)
      return rc;
    rc = strcmp((const char *)bytes + 16, key_id);
    lc_free_with_allocator(NULL, key_id);
    if (rc)
      return failure(
          error, LC_ERR_INVALID, "pouch_root_crypto_mismatch",
          "pouch root key does not match initialized encrypted root");
  }
  if (bytes[9] && !root->crypto) {
    lc_pouch_crypto_open_options options = {0};
    options.compression_enabled = 1;
    rc = lc_pouch_crypto_open(&root->crypto_allocator, &options, &root->crypto,
                              NULL, error);
    if (rc != LC_OK)
      return rc;
  }
  lc_pouch_crypto_set_compression(root->crypto, bytes[9]);
  return LC_OK;
corrupt:
  return failure(error, LC_ERR_PROTOCOL, "pouch_root_corrupt",
                 "pouch root control has unknown or invalid fields");
}
int ps_root_open(const char *path, int create, const ps_root_options *options,
                 ps_root **out, lc_error *error) {
  ps_root_options defaults = {0};
  ps_root *root;
  char *key_file = NULL;
  int rc;
  if (out)
    *out = NULL;
  if (!path || !*path || !out)
    return failure(error, LC_ERR_INVALID, NULL,
                   "pouch container root requires a path and output");
  if (!options)
    options = &defaults;
  root = lc_calloc_with_allocator(NULL, 1, sizeof(*root));
  if (!root)
    return failure(error, LC_ERR_NOMEM, NULL, "cannot allocate pouch root");
  rc = pthread_mutex_init(&root->mutex, NULL);
  if (rc)
    goto mutex_failed;
  rc = pthread_cond_init(&root->changed, NULL);
  if (rc) {
    pthread_mutex_destroy(&root->mutex);
    goto mutex_failed;
  }
  if (options->crypto_allocator)
    root->crypto_allocator = *options->crypto_allocator;
  rc = lc_pouch_crypto_open(&root->crypto_allocator, &options->crypto,
                            &root->crypto, &key_file, error);
  lc_free_with_allocator(&root->crypto_allocator, key_file);
  if (rc == LC_OK)
    rc = ps_open(path, create, &options->storage, &root->space, error);
  if (rc == LC_OK)
    rc = create ? ps_root_config_write(root, root->crypto, error)
                : config_read(root, error);
  if (rc == LC_OK)
    rc = reserve_base(root, !create, error);
  if (rc == LC_OK && !create)
    rc = ps_root_namespace_visit(root, recover_namespace, root, error);
  if (rc == LC_OK && !create)
    rc = ps_root_conversion_run(root, path, options, error);
  if (rc != LC_OK) {
    (void)ps_root_close(root, NULL);
    return rc;
  }
  *out = root;
  return LC_OK;
mutex_failed:
  lc_free_with_allocator(NULL, root);
  return failure(error, LC_ERR_TRANSPORT, NULL,
                 "cannot initialize pouch root registry");
}
ps_space *ps_root_space(ps_root *root) { return root ? root->space : NULL; }
lc_pouch_crypto *ps_root_crypto(ps_root *root) {
  return root ? root->crypto : NULL;
}
int ps_root_crypto_matches(ps_root *root, lc_pouch_crypto *crypto,
                           lc_error *e) {
  char *a = NULL, *b = NULL;
  int rc = LC_OK;
  if (!root ||
      lc_pouch_crypto_enabled(root->crypto) != lc_pouch_crypto_enabled(crypto))
    return failure(e, LC_ERR_INVALID, "pouch_root_crypto_mismatch",
                   "conversion keeps root encryption mode");
  if (lc_pouch_crypto_enabled(crypto)) {
    rc = lc_pouch_crypto_key_id(root->crypto, &a, e);
    if (rc == LC_OK)
      rc = lc_pouch_crypto_key_id(crypto, &b, e);
    if (rc == LC_OK && strcmp(a, b))
      rc = failure(e, LC_ERR_INVALID, "pouch_root_crypto_mismatch",
                   "conversion keeps root encryption key");
  }
  lc_free_with_allocator(NULL, a);
  lc_free_with_allocator(NULL, b);
  return rc;
}
static uint64_t name_hash(const char *name) {
  uint64_t hash = UINT64_C(14695981039346656037);
  const unsigned char *p = (const unsigned char *)name;
  while (*p) {
    hash ^= *p++;
    hash *= UINT64_C(1099511628211);
  }
  return hash;
}
static ps_namespace *find(ps_root *root, const char *name, uint64_t hash) {
  ps_namespace *ns;
  if (!root->capacity)
    return NULL;
  for (ns = root->buckets[hash % root->capacity]; ns; ns = ns->hash_next)
    if (ns->hash == hash && !strcmp(ns->name, name))
      return ns;
  return NULL;
}
static int index_room(ps_root *root, lc_error *error) {
  ps_namespace **buckets, *ns;
  size_t capacity = root->capacity ? root->capacity * 2 : 32;
  if (root->capacity && root->count < root->capacity / 4 * 3)
    return LC_OK;
  if (capacity < root->capacity || capacity > SIZE_MAX / sizeof(*buckets))
    return failure(error, LC_ERR_NOMEM, NULL,
                   "pouch namespace registry is too large");
  buckets = lc_calloc_with_allocator(NULL, capacity, sizeof(*buckets));
  if (!buckets)
    return failure(error, LC_ERR_NOMEM, NULL, "cannot grow namespace registry");
  for (ns = root->namespaces; ns; ns = ns->next) {
    ns->hash_next = buckets[ns->hash % capacity];
    buckets[ns->hash % capacity] = ns;
  }
  lc_free_with_allocator(NULL, root->buckets);
  root->buckets = buckets;
  root->capacity = capacity;
  return LC_OK;
}
static int namespace_load(ps_namespace *ns, int create, lc_error *error) {
  ps_catalog_view *view = NULL;
  ps_catalog_info binding;
  size_t i;
  int rc = ps_catalog_get(ns->root->space, 0, PS_CATALOG_NAMESPACE, ns->name,
                          strlen(ns->name), &view, error);
  if (rc == LC_OK && view)
    rc = ps_catalog_view_info(view, &binding, error);
  else if (rc == LC_OK && !create)
    rc = failure(error, LC_ERR_INVALID, "pouch_namespace_not_found",
                 "pouch namespace does not exist");
  else if (rc == LC_OK)
    rc = ps_catalog_create_namespace(ns->root->space, ns->name,
                                     strlen(ns->name), &binding, error);
  ps_catalog_view_close(view);
  if (rc != LC_OK)
    return rc;
  if (!binding.id || binding.revision.incarnation != binding.id ||
      binding.stream.kind || binding.stream.owner || binding.stream.key ||
      binding.target.incarnation)
    goto corrupt;
  for (i = 0; i < sizeof(binding.target.container); ++i)
    if (binding.target.container[i])
      goto corrupt;
  ns->identity.owner = binding.id;
  ns->identity.incarnation = binding.revision;
  return LC_OK;
corrupt:
  return failure(error, LC_ERR_PROTOCOL, "pouch_namespace_corrupt",
                 "pouch namespace binding is not an immutable metadata owner");
}
static int namespace_locks_init(ps_namespace *ns, lc_error *error) {
  if (pthread_mutex_init(&ns->collection_catalog, NULL) != 0)
    return failure(error, LC_ERR_TRANSPORT, NULL,
                   "cannot initialize collection catalog authority");
  if (pthread_mutex_init(&ns->maintenance, NULL) != 0) {
    pthread_mutex_destroy(&ns->collection_catalog);
    return failure(error, LC_ERR_TRANSPORT, NULL,
                   "cannot initialize namespace work authority");
  }
  if (pthread_mutex_init(&ns->outbox, NULL) != 0) {
    pthread_mutex_destroy(&ns->maintenance);
    pthread_mutex_destroy(&ns->collection_catalog);
    return failure(error, LC_ERR_TRANSPORT, NULL,
                   "cannot initialize outbox marker authority");
  }
  if (pthread_cond_init(&ns->outbox_changed, NULL) != 0) {
    pthread_mutex_destroy(&ns->outbox);
    pthread_mutex_destroy(&ns->maintenance);
    pthread_mutex_destroy(&ns->collection_catalog);
    return failure(error, LC_ERR_TRANSPORT, NULL,
                   "cannot initialize outbox marker admission");
  }
  if (pthread_mutex_init(&ns->query, NULL) != 0) {
    pthread_cond_destroy(&ns->outbox_changed);
    pthread_mutex_destroy(&ns->outbox);
    pthread_mutex_destroy(&ns->maintenance);
    pthread_mutex_destroy(&ns->collection_catalog);
    return failure(error, LC_ERR_TRANSPORT, NULL,
                   "cannot initialize namespace query authority");
  }
  return LC_OK;
}
int ps_root_namespace_acquire(ps_root *root, const char *name, int create,
                              ps_namespace **out, lc_error *error) {
  ps_namespace *ns, *fresh = NULL, *dead = NULL;
  uint64_t hash;
  int rc = LC_OK;
  if (out)
    *out = NULL;
  if (!root || !name || !*name || strlen(name) > PS_CATALOG_NAME_MAX || !out)
    return failure(error, LC_ERR_INVALID, NULL,
                   "pouch namespace requires a name and output");
  rc = ps_check(root->space, error);
  if (rc != LC_OK)
    return rc;
  hash = name_hash(name);
  pthread_mutex_lock(&root->mutex);
  ++root->calls;
  ns = find(root, name, hash);
  if (!ns) {
    pthread_mutex_unlock(&root->mutex);
    fresh = lc_calloc_with_allocator(NULL, 1, sizeof(*fresh));
    if (fresh)
      fresh->name = lc_strdup_with_allocator(NULL, name);
    pthread_mutex_lock(&root->mutex);
    ns = find(root, name, hash);
    if (!ns) {
      if (!fresh || !fresh->name)
        rc = failure(error, LC_ERR_NOMEM, NULL,
                     "cannot allocate namespace owner");
      if (rc == LC_OK)
        rc = index_room(root, error);
      if (rc == LC_OK)
        rc = namespace_locks_init(fresh, error);
      if (rc == LC_OK) {
        ns = fresh;
        fresh = NULL;
        ns->root = root;
        ns->hash = hash;
        ns->next = root->namespaces;
        root->namespaces = ns;
        ns->link = &root->namespaces;
        if (ns->next)
          ns->next->link = &ns->next;
        ns->hash_next = root->buckets[hash % root->capacity];
        root->buckets[hash % root->capacity] = ns;
        ++root->count;
      }
    }
  }
  if (rc == LC_OK) {
    ++ns->users;
    while (ns->loading)
      pthread_cond_wait(&root->changed, &root->mutex);
    if (!ns->ready) {
      ns->loading = 1;
      pthread_mutex_unlock(&root->mutex);
      rc = namespace_load(ns, create, error);
      pthread_mutex_lock(&root->mutex);
      ns->loading = 0;
      ns->ready = rc == LC_OK;
      pthread_cond_broadcast(&root->changed);
    }
    if (rc == LC_OK) {
      ++ns->pins;
      ++root->pins;
      *out = ns;
    }
    --ns->users;
    if (!ns->ready && !ns->users) {
      ps_namespace **link = &root->buckets[hash % root->capacity];
      while (*link != ns)
        link = &(*link)->hash_next;
      *link = ns->hash_next;
      *ns->link = ns->next;
      if (ns->next)
        ns->next->link = ns->link;
      --root->count;
      dead = ns;
    }
  }
  --root->calls;
  pthread_mutex_unlock(&root->mutex);
  if (fresh) {
    lc_free_with_allocator(NULL, fresh->name);
    lc_free_with_allocator(NULL, fresh);
  }
  if (dead) {
    pthread_mutex_destroy(&dead->collection_catalog);
    pthread_mutex_destroy(&dead->maintenance);
    pthread_mutex_destroy(&dead->query);
    pthread_mutex_destroy(&dead->outbox);
    pthread_cond_destroy(&dead->outbox_changed);
    lc_free_with_allocator(NULL, dead->name);
    lc_free_with_allocator(NULL, dead);
  }
  return rc;
}
int ps_namespace_identity(const ps_namespace *ns, ps_namespace_info *out,
                          lc_error *error) {
  if (!ns || !out)
    return failure(error, LC_ERR_INVALID, NULL,
                   "namespace identity requires owner and output");
  *out = ns->identity;
  return LC_OK;
}
void ps_namespace_release(ps_namespace *ns) {
  if (!ns)
    return;
  pthread_mutex_lock(&ns->root->mutex);
  --ns->pins;
  --ns->root->pins;
  pthread_mutex_unlock(&ns->root->mutex);
}
void ps_namespace_retain(ps_namespace *ns) {
  pthread_mutex_lock(&ns->root->mutex);
  ++ns->pins;
  ++ns->root->pins;
  pthread_mutex_unlock(&ns->root->mutex);
}
typedef struct visit_context {
  ps_root *root;
  ps_namespace_visit_fn visit;
  void *context;
} visit_context;
static int visit_namespace(void *v, const void *name, size_t length,
                           const ps_catalog_info *binding, lc_error *error) {
  visit_context *ctx = v;
  ps_namespace *ns = NULL;
  ps_namespace_info identity;
  char *text;
  int rc;
  if (!length || memchr(name, 0, length))
    return failure(error, LC_ERR_PROTOCOL, "pouch_namespace_corrupt",
                   "namespace catalog contains invalid name");
  text = lc_alloc_with_allocator(NULL, length + 1);
  if (!text)
    return failure(error, LC_ERR_NOMEM, NULL, "cannot capture namespace name");
  memcpy(text, name, length);
  text[length] = 0;
  rc = ps_root_namespace_acquire(ctx->root, text, 0, &ns, error);
  if (rc == LC_OK)
    rc = ps_namespace_identity(ns, &identity, error);
  if (rc == LC_OK &&
      (identity.owner != binding->id ||
       identity.incarnation.incarnation != binding->revision.incarnation ||
       memcmp(identity.incarnation.container, binding->revision.container, 16)))
    rc = failure(error, LC_ERR_PROTOCOL, "pouch_namespace_corrupt",
                 "namespace identity changed during catalog visit");
  if (rc == LC_OK)
    rc = ctx->visit(text, &identity, ctx->context, error);
  ps_namespace_release(ns);
  lc_free_with_allocator(NULL, text);
  return rc;
}
int ps_root_namespace_visit(ps_root *root, ps_namespace_visit_fn visit,
                            void *context, lc_error *error) {
  visit_context ctx;
  int rc;
  if (!root || !visit)
    return failure(error, LC_ERR_INVALID, NULL,
                   "namespace visit requires root and callback");
  ctx.root = root;
  ctx.visit = visit;
  ctx.context = context;
  pthread_mutex_lock(&root->mutex);
  ++root->calls;
  pthread_mutex_unlock(&root->mutex);
  rc = ps_catalog_visit(root->space, 0, PS_CATALOG_NAMESPACE, visit_namespace,
                        &ctx, error);
  pthread_mutex_lock(&root->mutex);
  --root->calls;
  pthread_mutex_unlock(&root->mutex);
  return rc;
}
typedef struct orphan_context {
  ps_space *space;
  uint64_t reclaimed, skipped;
} orphan_context;
static int reclaim_orphan(void *context, ps_key key, const ps_ref *ref,
                          lc_error *error) {
  orphan_context *c = context;
  ps_owned_head *head = NULL;
  ps_owned_status status;
  int rc = ps_owned_open(c->space, NULL, key, 0, ref, &head, &status, error);
  if (rc == LC_OK && key.key == 0U) {
    /* Collection identity is the last retirement in its owner tree. Claiming
     * excludes a concurrent name publication before touching any children. */
    rc = ps_owned_claim_orphan(head, error);
    if (rc == LC_OK)
      rc = ps_collection_reclaim_children(c->space, key.owner, error);
  }
  if (rc == LC_OK && key.key == PS_OUTBOX_MARKER_STREAM_KEY) {
    rc = ps_owned_claim_orphan(head, error);
    if (rc == LC_OK)
      rc = ps_outbox_marker_reclaim_child(c->space, key.owner, ref, error);
  }
  if (rc == LC_OK)
    rc = ps_owned_retire(head, error);
  ps_owned_close(head);
  if (rc == LC_ERR_INVALID) {
    lc_error_cleanup(error);
    ++c->skipped;
    return LC_OK;
  }
  if (rc == LC_OK)
    ++c->reclaimed;
  return rc;
}
int ps_root_reclaim_orphans(ps_root *root, uint64_t *reclaimed,
                            uint64_t *skipped, lc_error *error) {
  orphan_context c;
  int rc;
  if (!root || !reclaimed || !skipped)
    return failure(error, LC_ERR_INVALID, NULL,
                   "orphan reclamation requires root and counts");
  memset(&c, 0, sizeof(c));
  c.space = root->space;
  *reclaimed = *skipped = 0;
  pthread_mutex_lock(&root->mutex);
  ++root->calls;
  pthread_mutex_unlock(&root->mutex);
  rc = ps_catalog_visit_orphans(root->space, reclaim_orphan, &c, error);
  pthread_mutex_lock(&root->mutex);
  --root->calls;
  pthread_mutex_unlock(&root->mutex);
  *reclaimed = c.reclaimed;
  *skipped = c.skipped;
  return rc;
}
int ps_root_close(ps_root *root, lc_error *error) {
  ps_namespace *ns, *next;
  int rc;
  if (!root)
    return LC_OK;
  pthread_mutex_lock(&root->mutex);
  if (root->pins || root->calls) {
    pthread_mutex_unlock(&root->mutex);
    return failure(error, LC_ERR_INVALID, "pouch_root_busy",
                   "release pouch namespace owners before root close");
  }
  if (ps_coordination_users(ps_space_coordination(root->space))) {
    pthread_mutex_unlock(&root->mutex);
    return failure(error, LC_ERR_INVALID, "pouch_root_busy",
                   "release container coordination guards before root close");
  }
  pthread_mutex_unlock(&root->mutex);
  for (ns = root->namespaces; ns; ns = ns->next) {
    ps_collection_cache_destroy(ns->collections);
    ns->collections = NULL;
    ps_namespace_data_destroy(ns->data);
    __atomic_store_n(&ns->data, NULL, __ATOMIC_RELEASE);
  }
  rc = ps_close(root->space, error);
  if (rc != LC_OK)
    return rc;
  for (ns = root->namespaces; ns; ns = next) {
    next = ns->next;
    pthread_mutex_destroy(&ns->collection_catalog);
    pthread_mutex_destroy(&ns->maintenance);
    pthread_mutex_destroy(&ns->query);
    pthread_mutex_destroy(&ns->outbox);
    pthread_cond_destroy(&ns->outbox_changed);
    lc_free_with_allocator(NULL, ns->name);
    lc_free_with_allocator(NULL, ns);
  }
  lc_free_with_allocator(NULL, root->buckets);
  lc_pouch_crypto_close(root->crypto);
  pthread_cond_destroy(&root->changed);
  pthread_mutex_destroy(&root->mutex);
  lc_free_with_allocator(NULL, root);
  return LC_OK;
}
