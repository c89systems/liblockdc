#include "control.h"
#include "space_internal.h"

static ps_key head_key(uint64_t collection) {
  ps_key key;
  key.owner = collection;
  key.key = 1;
  key.kind = PS_CONTROL;
  return key;
}
static int invalid(lc_error *error) {
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "container collection head requires an owner and value",
                      NULL, NULL, "pouch");
}
int ps_collection_head_store(ps_space *space, uint64_t collection,
                             const lc_collection_disk_head *head, ps_ref *ref,
                             lc_error *error) {
  unsigned char bytes[LC_COLLECTION_HEAD_BYTES];
  ps_writer *writer = NULL;
  int rc;
  if (!space || !collection || !head)
    return invalid(error);
  rc = lc_collection_head_encode(head, bytes, error);
  if (rc == LC_OK)
    rc = ps_begin(space, NULL, head_key(collection), &writer, error);
  if (rc == LC_OK)
    rc = ps_write(writer, bytes, sizeof(bytes), error);
  if (rc == LC_OK)
    rc = ps_publish(writer, ref, error);
  ps_writer_close(writer);
  return rc;
}
static int head_read(ps_space *space, uint64_t collection, const ps_ref *ref,
                     ps_ref *selected, lc_collection_disk_head *head,
                     lc_error *error) {
  unsigned char bytes[LC_COLLECTION_HEAD_BYTES];
  ps_reader *reader = NULL;
  size_t got;
  int rc;
  if (!space || !collection || !head)
    return invalid(error);
  rc = ps_read_open(space, head_key(collection), ref, &reader, error);
  if (rc == LC_OK && ps_read_size(reader) != sizeof(bytes))
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "container collection head has invalid size", NULL,
                      "collection_corrupt", "pouch");
  if (rc == LC_OK)
    rc = ps_read(reader, bytes, sizeof(bytes), &got, error);
  if (rc == LC_OK && got != sizeof(bytes))
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "container collection head is truncated", NULL,
                      "collection_corrupt", "pouch");
  if (rc == LC_OK)
    rc = lc_collection_head_decode(bytes, head, error);
  if (rc == LC_OK && selected)
    ps_read_reference(reader, selected);
  ps_reader_close(reader);
  return rc;
}
int ps_collection_head_load(ps_space *space, uint64_t collection,
                            const ps_ref *ref, lc_collection_disk_head *head,
                            lc_error *error) {
  return head_read(space, collection, ref, NULL, head, error);
}
int ps_collection_head_load_cached(ps_space *space, uint64_t collection,
                                   ps_collection_head_cache *cache,
                                   lc_collection_disk_head *head, lc_error *e) {
  ps_coordination_guard *guard;
  ps_ref selected;
  uint64_t prefix, incarnation = 0;
  int rc = LC_OK, current;
  if (!space || !collection || !cache || !head)
    return invalid(e);
  pthread_mutex_lock(&space->mutex);
  if (cache->incarnation) {
    rc = ps_internal_observe_begin(space, &guard, e);
    if (rc == LC_OK) {
      prefix = __atomic_load_n(&space->durable_sequence, __ATOMIC_ACQUIRE);
      rc = ps_internal_fixed_value(space, head_key(collection), prefix, NULL,
                                   LC_COLLECTION_HEAD_BYTES, cache->incarnation,
                                   &incarnation, &current, NULL, e);
    }
    rc = ps_internal_capture_end(space, guard, rc, e);
    if (rc == LC_OK && incarnation)
      *head = cache->head;
  }
  pthread_mutex_unlock(&space->mutex);
  if (rc != LC_OK || incarnation)
    return rc;
  /* The regular reader preserves the prior durable selection while a
   * replacement is pending. The cached value is always a decoded snapshot. */
  rc = head_read(space, collection, NULL, &selected, head, e);
  if (rc == LC_OK) {
    pthread_mutex_lock(&space->mutex);
    cache->head = *head;
    cache->incarnation = selected.incarnation;
    pthread_mutex_unlock(&space->mutex);
  }
  return rc;
}
