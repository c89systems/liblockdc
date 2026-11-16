#include "lc_collection_catalog_record.h"
#include "lc_api_internal.h"
#include "lc_utf8.h"
#include <string.h>
#include <zlib.h>

static const unsigned char lc_catalog_magic[8] = {'L', 'C', 'C', 'A',
                                                  'T', 0,   0,   0};

int lc_collection_catalog_name_valid(const char *name) {
  lc_utf8_state utf8;
  size_t length;
  if (name == NULL)
    return 0;
  length = strlen(name);
  memset(&utf8, 0, sizeof(utf8));
  return length != 0U && length < LC_COLLECTION_NAME_BYTES &&
         lc_utf8_push(&utf8, (const unsigned char *)name, length) &&
         utf8.remaining == 0;
}

static int lc_catalog_id_valid(const char *id) {
  return id != NULL && strlen(id) == LC_XID_STRING_SIZE - 1U &&
         strspn(id, "0123456789abcdefghijklmnopqrstuv") ==
             LC_XID_STRING_SIZE - 1U;
}

static int lc_catalog_corrupt(lc_error *error) {
  return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "collection catalog identity is invalid", NULL,
                      "collection_corrupt", "pouch");
}

int lc_collection_catalog_encode(
    const char *id, const char *name,
    unsigned char bytes[LC_COLLECTION_CATALOG_BYTES], lc_error *error) {
  unsigned long crc;
  if (bytes == NULL || !lc_catalog_id_valid(id) ||
      !lc_collection_catalog_name_valid(name))
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection catalog requires a valid identity", NULL,
                        NULL, "pouch");
  memset(bytes, 0, LC_COLLECTION_CATALOG_BYTES);
  memcpy(bytes, lc_catalog_magic, sizeof(lc_catalog_magic));
  memcpy(bytes + 8U, id, LC_XID_STRING_SIZE);
  bytes[32] = (unsigned char)strlen(name);
  memcpy(bytes + 40U, name, strlen(name));
  crc = (unsigned long)crc32(0L, bytes, 316U);
  bytes[316] = (unsigned char)crc;
  bytes[317] = (unsigned char)(crc >> 8U);
  bytes[318] = (unsigned char)(crc >> 16U);
  bytes[319] = (unsigned char)(crc >> 24U);
  return LC_OK;
}

int lc_collection_catalog_decode(
    const unsigned char bytes[LC_COLLECTION_CATALOG_BYTES],
    char id[LC_XID_STRING_SIZE], char name[LC_COLLECTION_NAME_BYTES],
    lc_error *error) {
  unsigned long crc, actual;
  size_t length;
  char decoded_id[LC_XID_STRING_SIZE];
  char decoded_name[LC_COLLECTION_NAME_BYTES];
  if (bytes == NULL || id == NULL || name == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection catalog decode requires bytes and outputs",
                        NULL, NULL, "pouch");
  crc = (unsigned long)crc32(0L, bytes, 316U);
  actual = (unsigned long)bytes[316] | ((unsigned long)bytes[317] << 8U) |
           ((unsigned long)bytes[318] << 16U) |
           ((unsigned long)bytes[319] << 24U);
  length = (size_t)bytes[32];
  if (memcmp(bytes, lc_catalog_magic, sizeof(lc_catalog_magic)) != 0 ||
      crc != actual || length == 0U || bytes[28] != 0U ||
      bytes[40U + length] != 0U)
    return lc_catalog_corrupt(error);
  memcpy(decoded_id, bytes + 8U, sizeof(decoded_id));
  memcpy(decoded_name, bytes + 40U, length + 1U);
  if (!lc_catalog_id_valid(decoded_id) ||
      !lc_collection_catalog_name_valid(decoded_name) ||
      strlen(decoded_name) != length)
    return lc_catalog_corrupt(error);
  memcpy(id, decoded_id, sizeof(decoded_id));
  memcpy(name, decoded_name, length + 1U);
  return LC_OK;
}
