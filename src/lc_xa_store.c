#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_xa.h"
#include "lc_xa_internal.h"

#include <limits.h>
#include <stdio.h>
#include <unistd.h>

struct lc_xa_store {
  lc_client_handle *client;
  pid_t process;
  pthread_mutex_t mutex;
  unsigned long references;
  char owner_id[LC_XID_STRING_SIZE];
};

static int check(lc_xa_store *store, lc_error *error) {
  if (store == NULL || store->process != getpid())
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "XA journal is missing or inherited across fork", NULL,
                        NULL, NULL);
  return LC_OK;
}

int lc_xa_store_open(lc_client *client, lc_xa_store **out, lc_error *error) {
  lc_client_handle *handle = (lc_client_handle *)client;
  lc_xa_store *store;
  if (out != NULL)
    *out = NULL;
  if (handle == NULL || out == NULL || !handle->is_pouch)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "XA journal requires a local Pouch client and output",
                        NULL, NULL, NULL);
  store = (lc_xa_store *)lc_alloc_with_allocator(&handle->allocator,
                                                 sizeof(*store));
  if (store == NULL)
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "cannot allocate XA journal",
                        NULL, NULL, NULL);
  memset(store, 0, sizeof(*store));
  if (pthread_mutex_init(&store->mutex, NULL) != 0) {
    lc_free_with_allocator(&handle->allocator, store);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "cannot initialize XA journal references", NULL, NULL,
                        NULL);
  }
  lc_client_handle_retain(handle);
  store->client = handle;
  store->process = getpid();
  store->references = 1UL;
  *out = store;
  return LC_OK;
}

int lc_xa_store_retain(lc_xa_store *store, lc_error *error) {
  int rc = check(store, error);
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&store->mutex);
  if (store->references == ULONG_MAX) {
    pthread_mutex_unlock(&store->mutex);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "XA journal reference limit reached", NULL, NULL, NULL);
  }
  ++store->references;
  pthread_mutex_unlock(&store->mutex);
  return LC_OK;
}

int lc_xa_store_open_owned(lc_client *client, const char *owner_id,
                           lc_xa_store **out, lc_error *error) {
  int rc;
  if (out != NULL)
    *out = NULL;
  if (!lc_xid_is_valid(owner_id))
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "managed XA store requires a generated owner ID", NULL,
                        NULL, NULL);
  rc = lc_xa_store_open(client, out, error);
  if (rc == LC_OK)
    strcpy((*out)->owner_id, owner_id);
  return rc;
}

static const char *owner_id(lc_xa_store *store) {
  return store->owner_id[0] != '\0' ? store->owner_id : NULL;
}

void lc_xa_store_close(lc_xa_store *store) {
  lc_client_handle *client;
  lc_allocator allocator;
  if (store == NULL || store->process != getpid())
    return;
  pthread_mutex_lock(&store->mutex);
  if (--store->references != 0UL) {
    pthread_mutex_unlock(&store->mutex);
    return;
  }
  pthread_mutex_unlock(&store->mutex);
  pthread_mutex_destroy(&store->mutex);
  client = store->client;
  allocator = client->allocator;
  lc_free_with_allocator(&allocator, store);
  lc_client_handle_release(client);
}

int lc_xa_store_enroll_blocking(lc_xa_store *store,
                                const lc_xa_branch_record *branch,
                                lc_error *error) {
  int rc = check(store, error);
  return rc == LC_OK ? lc_pouch_client_xa_enroll(&store->client->pub, branch,
                                                 owner_id(store), error)
                     : rc;
}

int lc_xa_store_progress_blocking(lc_xa_store *store,
                                  const lc_xa_branch_record *branch,
                                  lc_error *error) {
  int rc = check(store, error);
  return rc == LC_OK ? lc_pouch_client_xa_progress(&store->client->pub, branch,
                                                   owner_id(store), error)
                     : rc;
}

void lc_xa_snapshot_cleanup(lc_xa_snapshot *snapshot) {
  if (snapshot == NULL)
    return;
  lc_free_with_allocator(NULL, snapshot->branches);
  memset(snapshot, 0, sizeof(*snapshot));
}
int lc_xa_store_claim_blocking(lc_xa_store *store, const char *txn_id,
                               long ttl_seconds, int recover,
                               lc_xa_snapshot *out, lc_error *error) {
  int rc = check(store, error);
  if (out != NULL)
    memset(out, 0, sizeof(*out));
  return rc == LC_OK
             ? lc_pouch_client_xa_claim(&store->client->pub, owner_id(store),
                                        txn_id, ttl_seconds, recover, out,
                                        error)
             : rc;
}
int lc_xa_store_renew_blocking(lc_xa_store *store, const char *txn_id,
                               long ttl_seconds, lc_error *error) {
  int rc = check(store, error);
  return rc == LC_OK ? lc_pouch_client_xa_owner_update(&store->client->pub,
                                                       owner_id(store), txn_id,
                                                       ttl_seconds, 0, 0, error)
                     : rc;
}
int lc_xa_store_release_blocking(lc_xa_store *store, const char *txn_id,
                                 lc_error *error) {
  int rc = check(store, error);
  return rc == LC_OK ? lc_pouch_client_xa_owner_update(&store->client->pub,
                                                       owner_id(store), txn_id,
                                                       0L, 0, 1, error)
                     : rc;
}
int lc_xa_store_snapshot_blocking(lc_xa_store *store, const char *txn_id,
                                  lc_xa_snapshot *out, lc_error *error) {
  int rc = check(store, error);
  if (out != NULL)
    memset(out, 0, sizeof(*out));
  return rc == LC_OK ? lc_pouch_client_xa_snapshot(&store->client->pub, txn_id,
                                                   out, error)
                     : rc;
}
int lc_xa_store_seal_blocking(lc_xa_store *store, const char *txn_id,
                              lc_error *error) {
  int rc = check(store, error);
  return rc == LC_OK ? lc_pouch_client_xa_owner_update(&store->client->pub,
                                                       owner_id(store), txn_id,
                                                       0L, 1, 0, error)
                     : rc;
}
int lc_xa_store_decide_blocking(lc_xa_store *store, const char *txn_id,
                                int commit, lc_error *error) {
  int rc = check(store, error);
  return rc == LC_OK
             ? lc_pouch_client_xa_decide(&store->client->pub, owner_id(store),
                                         txn_id, commit, error)
             : rc;
}
int lc_xa_store_prepare_pouch_blocking(lc_xa_store *store, const char *txn_id,
                                       lc_error *error) {
  int rc = check(store, error);
  return rc == LC_OK ? lc_pouch_client_xa_prepare_pouch(
                           &store->client->pub, owner_id(store), txn_id, error)
                     : rc;
}

int lc_xa_store_read_blocking(lc_xa_store *store, const char *txn_id,
                              const char *branch_id, lc_xa_branch_record *out,
                              int *prepared, lc_error *error) {
  char key[LC_POUCH_XA_KEY_SIZE];
  int ignored_prepared;
  int rc = check(store, error);
  if (rc != LC_OK)
    return rc;
  if (!lc_xid_is_valid(txn_id) || !lc_xid_is_valid(branch_id) || out == NULL)
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "XA branch lookup requires valid transaction/branch IDs", NULL, NULL,
        NULL);
  (void)snprintf(key, sizeof(key), "branch/%s/%s", txn_id, branch_id);
  rc = lc_pouch_xa_branch_read(store->client->pouch, key, out, error);
  if (rc == LC_OK)
    rc = lc_pouch_client_xa_prepared(
        &store->client->pub, txn_id, key, out->phase,
        prepared != NULL ? prepared : &ignored_prepared, error);
  return rc;
}

typedef struct registration {
  lc_xa_store *store;
  const char *key;
  unsigned char bytes[47];
} registration;

static int register_locked(void *context, lc_error *error) {
  registration *reg = (registration *)context;
  lc_pouch_state_read_result current;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  lc_source *source = NULL;
  unsigned char bytes[48];
  size_t total = 0U;
  size_t count;
  lc_error read_error;
  int rc;
  memset(&current, 0, sizeof(current));
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  rc = lc_pouch_state_read_locked(reg->store->client->pouch,
                                  LC_POUCH_XA_NAMESPACE, reg->key, &current,
                                  error);
  if (rc == LC_OK && current.found) {
    if (current.body == NULL)
      rc = LC_ERR_PROTOCOL;
    while (rc == LC_OK && total < sizeof(bytes)) {
      lc_error_init(&read_error);
      count = current.body->read(current.body, bytes + total,
                                 sizeof(bytes) - total, &read_error);
      if (read_error.code != LC_OK) {
        rc = lc_error_set(error, read_error.code, read_error.http_status,
                          read_error.message, read_error.detail,
                          read_error.server_code, read_error.correlation_id);
      }
      lc_error_cleanup(&read_error);
      if (rc != LC_OK)
        break;
      if (count == 0U)
        break;
      total += count;
    }
    if (rc == LC_OK && (total != sizeof(reg->bytes) ||
                        memcmp(bytes, reg->bytes, sizeof(reg->bytes)) != 0))
      rc = LC_ERR_PROTOCOL;
    if (rc == LC_ERR_PROTOCOL)
      rc = lc_error_set(error, rc, 0L,
                        "registered XA resource has another backend identity",
                        NULL, NULL, NULL);
  } else if (rc == LC_OK) {
    rc = lc_source_from_memory(reg->bytes, sizeof(reg->bytes), &source, error);
    if (rc == LC_OK) {
      options.create_if_absent = 1;
      options.object_record = 1;
      options.content_type = "application/x-lockdc-xa-resource";
      rc = lc_pouch_state_write_locked(reg->store->client->pouch,
                                       LC_POUCH_XA_NAMESPACE, reg->key, source,
                                       &options, &result, error);
    }
  }
  if (source != NULL)
    source->close(source);
  lc_pouch_state_read_result_cleanup(&reg->store->client->allocator, &current);
  lc_pouch_state_write_result_cleanup(&reg->store->client->allocator, &result);
  return rc;
}

int lc_xa_store_register_blocking(lc_xa_store *store,
                                  const lc_xa_branch_record *identity,
                                  lc_error *error) {
  lc_xa_branch_record validation;
  registration reg;
  char key[LC_POUCH_XA_KEY_SIZE];
  int rc = check(store, error);
  if (rc != LC_OK)
    return rc;
  if (identity == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "resource identity is required", NULL, NULL, NULL);
  validation = *identity;
  if (memchr(validation.resource_id, '\0', sizeof(validation.resource_id)) ==
      NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L, "invalid resource ID", NULL,
                        NULL, NULL);
  memcpy(validation.txn_id, validation.resource_id, sizeof(validation.txn_id));
  memcpy(validation.branch_id, validation.resource_id,
         sizeof(validation.branch_id));
  strcpy(validation.gid, "registration");
  strcpy(validation.full_xid, "1");
  validation.phase = LC_XA_ENROLLED;
  validation.prepared_observed = 0;
  validation.evidence = LC_XA_EVIDENCE_NONE;
  validation.completed_at_unix = 0;
  rc = lc_pouch_xa_branch_key(&validation, key, sizeof(key), error);
  if (rc != LC_OK)
    return rc;
  (void)snprintf(key, sizeof(key), "resource/%s", validation.resource_id);
  memset(&reg, 0, sizeof(reg));
  reg.store = store;
  reg.key = key;
  memcpy(reg.bytes, "LXAR", 4U);
  memcpy(reg.bytes + 4U, validation.system_id, strlen(validation.system_id));
  memcpy(reg.bytes + 25U, validation.database_oid,
         strlen(validation.database_oid));
  memcpy(reg.bytes + 36U, validation.role_oid, strlen(validation.role_oid));
  rc = lc_pouch_state_with_key_lock(store->client->pouch, LC_POUCH_XA_NAMESPACE,
                                    key, register_locked, &reg, error);
  if (rc == LC_OK)
    rc = lc_pouch_state_sync_namespace(store->client->pouch,
                                       LC_POUCH_XA_NAMESPACE, error);
  return rc;
}
