#include "lc_collection_head.h"
#include <string.h>
#include <zlib.h>

#define LC_CC_HEAD_MAGIC 0x4c434348UL

static int head_corrupt(lc_error *error, const char *message) {
  return lc_error_set(error, LC_ERR_PROTOCOL, 0L, message, NULL,
                      "collection_corrupt", "pouch");
}
static void head_put(unsigned char *bytes, lc_u64 value) {
  unsigned int i;
  for (i = 0U; i < 8U; ++i) {
    bytes[i] = (unsigned char)value;
    value >>= 8U;
  }
}

static lc_u64 head_get(const unsigned char *bytes) {
  lc_u64 value;
  unsigned int i;
  value = 0U;
  for (i = 8U; i > 0U; --i)
    value = (value << 8U) | bytes[i - 1U];
  return value;
}

static void head_seal(unsigned char *bytes, size_t length) {
  unsigned long crc;
  crc = (unsigned long)crc32(0L, bytes, (uInt)(length - 4U));
  bytes[length - 4U] = (unsigned char)crc;
  bytes[length - 3U] = (unsigned char)(crc >> 8U);
  bytes[length - 2U] = (unsigned char)(crc >> 16U);
  bytes[length - 1U] = (unsigned char)(crc >> 24U);
}

static int head_valid(const unsigned char *bytes, size_t length) {
  unsigned long stored;
  stored = (unsigned long)bytes[length - 4U] |
           ((unsigned long)bytes[length - 3U] << 8U) |
           ((unsigned long)bytes[length - 2U] << 16U) |
           ((unsigned long)bytes[length - 1U] << 24U);
  return stored == (unsigned long)crc32(0L, bytes, (uInt)(length - 4U));
}

int lc_collection_head_prefix_valid(const unsigned char *bytes, size_t length,
                                    lc_error *error) {
  if (bytes == NULL || (length != 128U && length != LC_COLLECTION_HEAD_BYTES))
    return head_corrupt(error, "collection head size is invalid");
  if (!head_valid(bytes, length) || head_get(bytes) != LC_CC_HEAD_MAGIC)
    return head_corrupt(error, "collection head checksum is invalid");
  if (head_get(bytes + 72U) > (lc_u64)LC_I64_MAX)
    return head_corrupt(error, "collection retention deadline is invalid");
  if (head_get(bytes + 8U) == 0U ||
      head_get(bytes + 8U) >= LC_COLLECTION_SNAPSHOT_BIT ||
      head_get(bytes + 16U) >= LC_COLLECTION_SNAPSHOT_BIT ||
      head_get(bytes + 32U) == 0U ||
      head_get(bytes + 32U) > head_get(bytes + 8U) ||
      head_get(bytes + 16U) > head_get(bytes + 48U))
    return head_corrupt(error, "collection head bounds are invalid");
  return LC_OK;
}

int lc_collection_head_encode(const lc_collection_disk_head *head,
                              unsigned char bytes[LC_COLLECTION_HEAD_BYTES],
                              lc_error *error) {
  lc_collection_disk_head checked;
  if (head == NULL || bytes == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection head codec requires input and output", NULL,
                        NULL, "pouch");
  memset(bytes, 0, LC_COLLECTION_HEAD_BYTES);
  head_put(bytes, LC_CC_HEAD_MAGIC);
  head_put(bytes + 8U, head->active);
  head_put(bytes + 16U, head->snapshot);
  head_put(bytes + 24U, head->snapshot_generation);
  head_put(bytes + 32U, head->tail_segment);
  head_put(bytes + 40U, head->tail_offset);
  head_put(bytes + 48U, head->next_snapshot);
  head_put(bytes + 56U, head->dropped ? 1U : 0U);
  head_put(bytes + 64U, head->published_at);
  head_put(bytes + 72U, (lc_u64)head->retention);
  head_put(bytes + 80U, head->max_entries);
  head_put(bytes + 88U, head->retention_generation);
  head_put(bytes + 96U, head->lease.fencing_token);
  head_put(bytes + 104U, (lc_u64)head->lease.expires_at_unix);
  head_put(bytes + 112U, head->lease.private_read ? 1U : 0U);
  memcpy(bytes + 120U, head->lease.lease_id, LC_XID_STRING_SIZE);
  memcpy(bytes + 144U, head->lease.owner, 129U);
  head_put(bytes + 280U, head->snapshot_extra_parts);
  head_seal(bytes, LC_COLLECTION_HEAD_BYTES);
  return lc_collection_head_decode(bytes, &checked, error);
}
int lc_collection_head_decode(
    const unsigned char bytes[LC_COLLECTION_HEAD_BYTES],
    lc_collection_disk_head *out, lc_error *error) {
  lc_collection_disk_head value;
  size_t i;
  int rc;
  if (bytes == NULL || out == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection head codec requires input and output", NULL,
                        NULL, "pouch");
  memset(&value, 0, sizeof(value));
  rc = lc_collection_head_prefix_valid(bytes, LC_COLLECTION_HEAD_BYTES, error);
  if (rc != LC_OK)
    return rc;
  for (i = 273U; i < LC_COLLECTION_HEAD_BYTES - 4U; ++i)
    if ((i < 280U || i >= 288U) && bytes[i] != 0U)
      return head_corrupt(error, "collection head has unknown fields");
  value.active = head_get(bytes + 8U);
  value.snapshot = head_get(bytes + 16U);
  value.snapshot_generation = head_get(bytes + 24U);
  value.tail_segment = head_get(bytes + 32U);
  value.tail_offset = head_get(bytes + 40U);
  value.next_snapshot = head_get(bytes + 48U);
  value.snapshot_extra_parts = head_get(bytes + 280U);
  if (value.next_snapshot >= LC_COLLECTION_SNAPSHOT_BIT ||
      (!value.snapshot && value.snapshot_extra_parts) ||
      value.snapshot_extra_parts > value.next_snapshot - value.snapshot)
    return head_corrupt(error, "collection snapshot group is invalid");
  value.dropped = head_get(bytes + 56U) != 0U;
  value.published_at = head_get(bytes + 64U);
  value.retention = (lc_pouch_unix_seconds)head_get(bytes + 72U);
  value.max_entries = head_get(bytes + 80U);
  value.retention_generation = head_get(bytes + 88U);
  if (head_get(bytes + 104U) > (lc_u64)LC_I64_MAX ||
      head_get(bytes + 112U) > 1U ||
      memchr(bytes + 120U, 0, LC_XID_STRING_SIZE) == NULL ||
      memchr(bytes + 144U, 0, 129U) == NULL)
    return head_corrupt(error, "collection ownership metadata is invalid");
  value.lease.fencing_token = head_get(bytes + 96U);
  value.lease.expires_at_unix = (lc_pouch_unix_seconds)head_get(bytes + 104U);
  value.lease.private_read = (int)head_get(bytes + 112U);
  memcpy(value.lease.lease_id, bytes + 120U, LC_XID_STRING_SIZE);
  memcpy(value.lease.owner, bytes + 144U, 129U);
  if ((value.lease.expires_at_unix == 0) != (value.lease.lease_id[0] == '\0') ||
      (value.lease.lease_id[0] != '\0' &&
       (!lc_xid_is_valid(value.lease.lease_id) ||
        value.lease.fencing_token == 0U || value.lease.owner[0] == '\0')))
    return head_corrupt(error, "collection ownership binding is invalid");

  *out = value;
  return LC_OK;
}
