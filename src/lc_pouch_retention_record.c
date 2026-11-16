#include "lc_pouch_retention_record.h"
#include "lc_api_internal.h"
#include "lc_pouch_path.h"
#include <stdio.h>
#include <string.h>

static int record_error(lc_error *e, int rc, const char *message,
                        const char *origin) {
  return lc_error_set(e, rc, 0, message, NULL, origin, "pouch");
}
static int hex(unsigned char c) {
  if (c >= '0' && c <= '9')
    return c - '0';
  if (c >= 'a' && c <= 'f')
    return c - 'a' + 10;
  if (c >= 'A' && c <= 'F')
    return c - 'A' + 10;
  return -1;
}
int lc_pouch_retention_record_unescape(const lc_allocator *a, const char *text,
                                       char **out, const char *origin,
                                       lc_error *e) {
  char *bytes, *dst;
  int high, low;
  if (out)
    *out = NULL;
  if (!out || !text || !*text)
    return record_error(e, LC_ERR_PROTOCOL, "invalid state retention identity",
                        origin);
  bytes = lc_alloc_with_allocator(a, strlen(text) + 1);
  if (!bytes)
    return record_error(e, LC_ERR_NOMEM,
                        "failed to allocate state retention target", origin);
  dst = bytes;
  while (*text) {
    if (*text == '%') {
      if (!text[1] || !text[2] || (high = hex((unsigned char)text[1])) < 0 ||
          (low = hex((unsigned char)text[2])) < 0 || !(high || low)) {
        lc_free_with_allocator(a, bytes);
        return record_error(e, LC_ERR_PROTOCOL,
                            "invalid state retention work identity", origin);
      }
      *dst++ = (char)((high << 4) | low);
      text += 3;
    } else
      *dst++ = *text++;
  }
  *dst = '\0';
  *out = bytes;
  return LC_OK;
}
int lc_pouch_retention_record_decode(const lc_allocator *a, char *bytes,
                                     size_t length, const char *origin,
                                     lc_pouch_retention_work *out,
                                     lc_error *e) {
  char *fields[4], *end;
  size_t i;
  int rc;
  if (!out)
    return record_error(e, LC_ERR_INVALID,
                        "state retention decode requires output", origin);
  memset(out, 0, sizeof(*out));
  if (!bytes || length < 7 || length > LC_POUCH_RETENTION_CONTROL_MAX ||
      memchr(bytes, '\0', length))
    goto invalid;
  bytes[length] = '\0';
  if (length == 7 && !memcmp(bytes, LC_POUCH_RETENTION_IDLE_TEXT, 7)) {
    out->present = 1;
    return LC_OK;
  }
  if (length <= 9 || memcmp(bytes, "LPR1\n", 5) ||
      (bytes[5] != '1' && bytes[5] != '2') || bytes[6] != '\n' ||
      bytes[length - 1] != '\n')
    goto invalid;
  fields[0] = bytes + 7;
  for (i = 0; i < 3; ++i) {
    end = strchr(fields[i], '\n');
    if (!end || end == fields[i] || end + 1 >= bytes + length - 1)
      goto invalid;
    *end = '\0';
    fields[i + 1] = end + 1;
  }
  bytes[length - 1] = '\0';
  if (!lc_xid_is_valid(fields[0]) || strchr(fields[3], '\n') ||
      !lc_u64_parse_base10(fields[1], &out->fencing_token) ||
      !lc_u64_parse_base10(fields[2], &out->state_version))
    goto invalid;
  out->txn_id = lc_strdup_with_allocator(a, fields[0]);
  if (!out->txn_id)
    rc = record_error(e, LC_ERR_NOMEM,
                      "failed to allocate state retention work identity",
                      origin);
  else
    rc = lc_pouch_retention_record_unescape(a, fields[3], &out->key, origin, e);
  if (rc == LC_OK) {
    out->phase = bytes[5] - '0';
    out->present = 1;
    return LC_OK;
  }
  lc_free_with_allocator(a, out->txn_id);
  lc_free_with_allocator(a, out->key);
  memset(out, 0, sizeof(*out));
  return rc;
invalid:
  memset(out, 0, sizeof(*out));
  return record_error(e, LC_ERR_PROTOCOL,
                      "invalid state retention work reference", origin);
}
int lc_pouch_retention_record_encode(const lc_allocator *a, int phase,
                                     const char *txn_id, const char *key,
                                     lc_u64 fencing_token,
                                     lc_pouch_generation state_version,
                                     char **out, size_t *bytes, lc_error *e) {
  char *encoded = NULL, *text = NULL;
  char fencing[32], version[32];
  size_t length;
  int rc = LC_OK;
  if (out)
    *out = NULL;
  if (bytes)
    *bytes = 0;
  if (!out || !bytes || phase < LC_POUCH_RETENTION_IDLE ||
      phase > LC_POUCH_RETENTION_FINISHED ||
      (phase != LC_POUCH_RETENTION_IDLE &&
       (!lc_xid_is_valid(txn_id) || !key || !*key)))
    return record_error(e, LC_ERR_INVALID,
                        "invalid state retention work identity", NULL);
  if (phase == LC_POUCH_RETENTION_IDLE) {
    text = lc_strdup_with_allocator(a, LC_POUCH_RETENTION_IDLE_TEXT);
    length = 8;
  } else {
    (void)lc_u64_format_base10(fencing_token, fencing, sizeof(fencing));
    (void)lc_u64_format_base10(state_version, version, sizeof(version));
    encoded = lc_pouch_path_escape_name(a, key);
    if (!encoded)
      return record_error(e, LC_ERR_NOMEM,
                          "failed to encode state retention target", NULL);
    length = strlen(encoded) + strlen(txn_id) + strlen(fencing) +
             strlen(version) + 12;
    if (length > LC_POUCH_RETENTION_CONTROL_MAX)
      rc = record_error(e, LC_ERR_INVALID,
                        "state retention work exceeds local limit", NULL);
    else {
      text = lc_alloc_with_allocator(a, length);
      if (text)
        (void)snprintf(text, length, "LPR1\n%d\n%s\n%s\n%s\n%s\n", phase,
                       txn_id, fencing, version, encoded);
    }
  }
  lc_free_with_allocator(a, encoded);
  if (rc == LC_OK && !text)
    rc =
        record_error(e, LC_ERR_NOMEM,
                     "failed to allocate state retention work reference", NULL);
  if (rc == LC_OK) {
    *out = text;
    *bytes = length - 1;
  }
  return rc;
}
