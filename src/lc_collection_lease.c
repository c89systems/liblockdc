#include "lc_collection_internal.h"
#include "lc_utf8.h"

#include <string.h>

static int lease_request(const lc_collection_lease_req *req, lc_error *error) {
  lc_utf8_state utf8;
  size_t length;
  if (req == NULL || req->owner == NULL || req->ttl_seconds <= 0)
    return lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "collection lease requires owner and positive TTL");
  length = strlen(req->owner);
  memset(&utf8, 0, sizeof(utf8));
  if (length == 0U || length > 128U ||
      !lc_utf8_push(&utf8, (const unsigned char *)req->owner, length) ||
      utf8.remaining != 0)
    return lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "collection lease owner must be 1..128 UTF-8 bytes");
  return LC_OK;
}

int lc_collection_lease_observe(lc_collection_handle *collection,
                                lc_collection_lease_info *out,
                                lc_error *error) {
  lc_pouch_unix_seconds now;
  int rc;
  *out = collection->head.lease;
  out->active = 0;
  if (out->expires_at_unix == 0)
    return LC_OK;
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK)
    out->active = out->expires_at_unix > now;
  return rc;
}

int lc_collection_lease_check(lc_collection_handle *collection, int mutating,
                              lc_error *error) {
  lc_collection_lease_info observed;
  int matches, rc;
  /* The ordinary unleased path neither reads a clock nor adds filesystem I/O.
   */
  if (collection->lease_id[0] == '\0' &&
      collection->head.lease.expires_at_unix == 0)
    return LC_OK;
  if (!mutating && !collection->head.lease.private_read)
    return LC_OK;
  rc = lc_collection_lease_observe(collection, &observed, error);
  if (rc != LC_OK)
    return rc;
  matches = observed.active && collection->lease_id[0] != '\0' &&
            collection->lease_token == observed.fencing_token &&
            strcmp(collection->lease_id, observed.lease_id) == 0;
  if (!mutating)
    return !observed.active || matches
               ? LC_OK
               : lc_collection_failure(error, LC_ERR_INVALID,
                                       "collection_private",
                                       "collection has a private writer lease");
  if (collection->lease_id[0] != '\0')
    return matches
               ? LC_OK
               : lc_collection_failure(error, LC_ERR_INVALID, "lease_lost",
                                       "collection ownership has been lost");
  return !observed.active
             ? LC_OK
             : lc_collection_failure(error, LC_ERR_INVALID, "collection_leased",
                                     "collection has another writer lease");
}

static int lease_replace(lc_collection_handle *collection,
                         const lc_collection_lease_req *req, lc_error *error) {
  lc_collection_lease_info next;
  lc_pouch_unix_seconds now;
  int rc;
  memset(&next, 0, sizeof(next));
  if (collection->head.lease.fencing_token == LC_U64_MAX)
    return lc_collection_failure(
        error, LC_ERR_INVALID, "sequence_exhausted",
        "collection lease fencing tokens are exhausted");
  next.fencing_token = collection->head.lease.fencing_token + 1U;
  next.private_read = req->private_read != 0;
  strcpy(next.owner, req->owner);
  rc = lc_xid_new(next.lease_id, error);
  if (rc == LC_OK)
    rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK)
    rc = lc_pouch_retention_resolve(-(lc_pouch_unix_seconds)req->ttl_seconds,
                                    now, &next.expires_at_unix, error);
  if (rc == LC_OK) {
    collection->head.lease = next;
    rc = lc_collection_head_write(collection, error);
    collection->head_loaded = 0;
  }
  return rc;
}

int lc_collection_lease_initialize(lc_collection_handle *collection,
                                   const lc_collection_lease_req *req,
                                   lc_error *error) {
  int rc = lease_request(req, error);
  if (rc == LC_OK) {
    rc = lc_collection_metadata_enter(collection, 1, error);
    if (rc == LC_OK) {
      rc = lc_collection_head_read(collection, error);
      if (rc == LC_OK)
        rc = lease_replace(collection, req, error);
      if (rc == LC_OK) {
        memcpy(collection->lease_id, collection->head.lease.lease_id,
               sizeof(collection->lease_id));
        collection->lease_token = collection->head.lease.fencing_token;
      }
      lc_collection_metadata_leave(collection);
    }
  }
  return rc;
}

static int lease_acquire(lc_collection *self,
                         const lc_collection_lease_req *req,
                         const char *expected, lc_collection **out,
                         lc_error *error) {
  lc_collection_call call;
  lc_collection_guard guard;
  lc_collection *owned = NULL;
  lc_collection_handle *collection;
  lc_collection_lease_info observed;
  int rc;
  if (out == NULL)
    return lc_collection_failure(error, LC_ERR_INVALID, NULL,
                                 "collection acquisition requires output");
  *out = NULL;
  /* A peer receiver may change control metadata from a source callback: no
   * append authority is taken, and publication will revalidate ownership. */
  rc = lc_collection_call_begin(self, 0, &call, error);
  if (rc != LC_OK)
    return rc;
  error = call.error;
  collection = call.collection;
  rc = lease_request(req, error);
  if (rc == LC_OK && expected != NULL &&
      strlen(expected) != LC_XID_STRING_LENGTH)
    rc = lc_collection_failure(error, LC_ERR_INVALID, NULL,
                               "takeover requires the observed lease ID");
  /* Allocate the independent receiver before changing ownership. No allocation
   * after a successful ownership write is needed to return its credentials. */
  if (rc == LC_OK)
    rc = collection->owner.ops->clone(collection->owner.context, &owned, error);
  if (rc == LC_OK) {
    rc = lc_collection_control_enter(&call, &guard, error);
    if (rc == LC_OK) {
      rc = lc_collection_lease_observe(collection, &observed, error);
      if (rc == LC_OK && expected != NULL &&
          (!observed.active || strcmp(expected, observed.lease_id) != 0))
        rc = lc_collection_failure(error, LC_ERR_INVALID, "lease_changed",
                                   "observed collection lease has changed");
      if (rc == LC_OK && expected == NULL && observed.active)
        rc = lc_collection_failure(error, LC_ERR_INVALID, "collection_leased",
                                   "collection already has a writer lease");
      if (rc == LC_OK) {
        rc = lease_replace(collection, req, error);
        if (rc == LC_OK) {
          lc_collection_handle *receiver = owned->impl;
          memcpy(receiver->lease_id, collection->head.lease.lease_id,
                 sizeof(receiver->lease_id));
          receiver->lease_token = collection->head.lease.fencing_token;
          /* The caller's original receiver retains its original identity. */
        }
      }
      lc_collection_control_leave(&guard);
    }
  }
  if (rc == LC_OK)
    *out = owned;
  else
    lc_collection_close(owned);
  lc_collection_call_end(&call);
  return rc;
}

int lc_collection_acquire_method(lc_collection *self,
                                 const lc_collection_lease_req *req,
                                 lc_collection **out, lc_error *error) {
  return lease_acquire(self, req, NULL, out, error);
}
int lc_collection_takeover_method(lc_collection *self,
                                  const lc_collection_lease_req *req,
                                  const char *expected, lc_collection **out,
                                  lc_error *error) {
  if (expected == NULL)
    return lc_collection_failure(error, LC_ERR_INVALID, NULL,
                                 "takeover requires the observed lease ID");
  return lease_acquire(self, req, expected, out, error);
}

static int lease_finish(lc_collection *self, long ttl,
                        lc_collection_lease_info *out, int release,
                        lc_error *error) {
  lc_collection_call call;
  lc_collection_guard guard;
  lc_collection_handle *collection;
  lc_pouch_unix_seconds now, deadline;
  int rc;
  if (!release && (ttl <= 0 || out == NULL))
    return lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "collection keepalive requires positive TTL and output");
  rc = lc_collection_call_begin(self, 0, &call, error);
  if (rc != LC_OK)
    return rc;
  error = call.error;
  collection = call.collection;
  rc = lc_collection_control_enter(&call, &guard, error);
  if (rc == LC_OK) {
    if (collection->lease_id[0] == '\0')
      rc = lc_collection_failure(error, LC_ERR_INVALID, "lease_required",
                                 "operation requires an acquired collection");
    else
      rc = lc_collection_lease_check(collection, 1, error);
    if (rc == LC_OK && !release) {
      rc = lc_pouch_now_unix(&now, error);
      if (rc == LC_OK)
        rc = lc_pouch_retention_resolve(-(lc_pouch_unix_seconds)ttl, now,
                                        &deadline, error);
      if (rc == LC_OK)
        collection->head.lease.expires_at_unix = deadline;
    } else if (rc == LC_OK) {
      lc_u64 token = collection->head.lease.fencing_token;
      memset(&collection->head.lease, 0, sizeof(collection->head.lease));
      collection->head.lease.fencing_token = token;
    }
    if (rc == LC_OK) {
      rc = lc_collection_head_write(collection, error);
      collection->head_loaded = 0;
      if (rc == LC_OK && !release) {
        *out = collection->head.lease;
        out->active = 1;
      }
    }
    lc_collection_control_leave(&guard);
  }
  if (rc == LC_OK && release)
    self->close(self);
  lc_collection_call_end(&call);
  return rc;
}
int lc_collection_keepalive_method(lc_collection *self, long ttl,
                                   lc_collection_lease_info *out,
                                   lc_error *error) {
  return lease_finish(self, ttl, out, 0, error);
}
int lc_collection_release_method(lc_collection *self, lc_error *error) {
  return lease_finish(self, 0, NULL, 1, error);
}
