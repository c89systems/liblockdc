#include "control.h"

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
    rc = ps_begin(space, head_key(collection), &writer, error);
  if (rc == LC_OK)
    rc = ps_write(writer, bytes, sizeof(bytes), error);
  if (rc == LC_OK)
    rc = ps_publish(writer, ref, error);
  ps_writer_close(writer);
  return rc;
}
int ps_collection_head_load(ps_space *space, uint64_t collection,
                            const ps_ref *ref, lc_collection_disk_head *head,
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
  ps_reader_close(reader);
  return rc;
}
