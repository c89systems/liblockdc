#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_outbox_store.h"
#include <stdint.h>
#include <string.h>
#define LC_POUCH_OUTBOX_STATE_RECLAIM_MAGIC "LOR1"
#define LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES 12U
#define LC_POUCH_OUTBOX_STATE_RECLAIM_MAX_BYTES 4096U
#define LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MAGIC "LDR1"
#define LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_HEADER_BYTES 24U
#define LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MAX_BYTES                          \
  LC_POUCH_OUTBOX_MARKER_MAX_BYTES
#define LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MISSING                            \
  LC_POUCH_OUTBOX_MARKER_MISSING
static int lc_pouch_outbox_state_reclaim_marker_id_valid(const char *id) {
  const unsigned char *cursor;

  if (id == NULL || id[0] == '\0') {
    return 0;
  }
  cursor = (const unsigned char *)id;
  while (*cursor != '\0') {
    if (!((*cursor >= (unsigned char)'A' && *cursor <= (unsigned char)'Z') ||
          (*cursor >= (unsigned char)'a' && *cursor <= (unsigned char)'z') ||
          (*cursor >= (unsigned char)'0' && *cursor <= (unsigned char)'9') ||
          *cursor == (unsigned char)'_' || *cursor == (unsigned char)'-')) {
      return 0;
    }
    ++cursor;
  }
  return 1;
}
static unsigned long
lc_pouch_outbox_state_reclaim_u32_read(const unsigned char *bytes) {
  return ((unsigned long)bytes[0] << 24U) | ((unsigned long)bytes[1] << 16U) |
         ((unsigned long)bytes[2] << 8U) | (unsigned long)bytes[3];
}
static void lc_pouch_outbox_state_reclaim_u32_write(unsigned char *bytes,
                                                    unsigned long value) {
  bytes[0] = (unsigned char)((value >> 24U) & 0xffUL);
  bytes[1] = (unsigned char)((value >> 16U) & 0xffUL);
  bytes[2] = (unsigned char)((value >> 8U) & 0xffUL);
  bytes[3] = (unsigned char)(value & 0xffUL);
}
static void lc_pouch_outbox_dead_letter_reclaim_u64_write(unsigned char *bytes,
                                                          uint64_t value) {
  unsigned int index;

  for (index = 0U; index < 8U; ++index) {
    bytes[7U - index] = (unsigned char)(value & 0xffU);
    value >>= 8U;
  }
}
static uint64_t
lc_pouch_outbox_dead_letter_reclaim_u64_read(const unsigned char *bytes) {
  uint64_t value;
  unsigned int index;

  value = 0U;
  for (index = 0U; index < 8U; ++index)
    value = (value << 8U) | (uint64_t)bytes[index];
  return value;
}
static int retention_name(int kind, const char *id, lc_pouch_unix_seconds due,
                          char buffer[17U + LC_XID_STRING_SIZE],
                          const char **out, lc_error *error) {
  uint64_t value;
  unsigned int i;
  if (kind < 0 || kind >= LC_POUCH_RETENTION_KIND_COUNT || !id ||
      !lc_xid_is_valid(id))
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "invalid outbox retention marker", NULL, NULL, "pouch");
  *out = id;
  if (kind == LC_POUCH_RETENTION_DEAD_LETTER)
    return LC_OK;
  if (due <= 0)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "terminal retention requires positive deadline", NULL,
                        NULL, "pouch");
  value = (uint64_t)due;
  for (i = 0U; i < 16U; ++i) {
    buffer[15U - i] = "0123456789abcdef"[value & 15U];
    value >>= 4U;
  }
  buffer[16] = '-';
  strcpy(buffer + 17U, id);
  *out = buffer;
  return LC_OK;
}
int lc_pouch_outbox_state_reclaim_marker_write(lc_pouch *pouch, const char *ns,
                                               const char *marker_id,
                                               const char *outbox_key,
                                               const char *state_key,
                                               lc_error *error) {
  unsigned char *contents;
  lc_pouch_outbox_store store = {0};
  size_t outbox_length;
  size_t state_length;
  size_t contents_length;
  int rc;

  if (pouch == NULL || outbox_key == NULL || outbox_key[0] == '\0' ||
      state_key == NULL || state_key[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch outbox state reclaim marker requires keys", NULL,
                        NULL, "pouch");
  }
  outbox_length = strlen(outbox_key);
  state_length = strlen(state_key);
  if (outbox_length > LC_POUCH_OUTBOX_STATE_RECLAIM_MAX_BYTES -
                          LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES ||
      state_length > LC_POUCH_OUTBOX_STATE_RECLAIM_MAX_BYTES -
                         LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES -
                         outbox_length) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch outbox state reclaim marker is too large", NULL,
                        NULL, "pouch");
  }
  contents = NULL;
  if (!lc_pouch_outbox_state_reclaim_marker_id_valid(marker_id))
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "invalid outbox reclaim marker identity", NULL, NULL,
                        "pouch");
  rc = lc_pouch_outbox_store_open(pouch, ns, -1, 1, &store, error);
  contents_length =
      LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES + outbox_length + state_length;
  if (rc == LC_OK) {
    contents = (unsigned char *)lc_alloc_with_allocator(&pouch->allocator,
                                                        contents_length);
    if (contents == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch outbox state reclaim marker",
                        NULL, NULL, "pouch");
    }
  }
  if (rc == LC_OK) {
    memcpy(contents, LC_POUCH_OUTBOX_STATE_RECLAIM_MAGIC, 4U);
    lc_pouch_outbox_state_reclaim_u32_write(contents + 4U,
                                            (unsigned long)outbox_length);
    lc_pouch_outbox_state_reclaim_u32_write(contents + 8U,
                                            (unsigned long)state_length);
    memcpy(contents + LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES, outbox_key,
           outbox_length);
    memcpy(contents + LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES +
               outbox_length,
           state_key, state_length);
  }
  if (rc == LC_OK)
    rc = store.ops->write(store.context, marker_id, contents, contents_length,
                          error);
  lc_free_with_allocator(&pouch->allocator, contents);
  return lc_pouch_outbox_store_close(&store, rc, error);
}
int lc_pouch_outbox_state_reclaim_marker_take(lc_pouch *pouch, const char *ns,
                                              const char *after_marker_id,
                                              char **marker_id_out,
                                              char **outbox_key_out,
                                              char **state_key_out,
                                              lc_error *error) {
  lc_pouch_outbox_store directory = {0};
  const char *name = NULL;
  char *first;
  char *after;
  char *selected;
  unsigned char *contents;
  size_t contents_length;
  unsigned long outbox_length;
  unsigned long state_length;
  int rc;

  if (pouch == NULL || marker_id_out == NULL || outbox_key_out == NULL ||
      state_key_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch outbox state reclaim take requires outputs",
                        NULL, NULL, "pouch");
  }
  *marker_id_out = NULL;
  *outbox_key_out = NULL;
  *state_key_out = NULL;
  first = NULL;
  after = NULL;
  selected = NULL;
  contents = NULL;
  rc = lc_pouch_outbox_store_open(pouch, ns, -1, 0, &directory, error);
  if (rc != LC_OK)
    return rc;
  while (rc == LC_OK &&
         (rc = directory.ops->next(directory.context, &name, error)) == LC_OK &&
         name) {
    if (!lc_pouch_outbox_state_reclaim_marker_id_valid(name)) {
      continue;
    }
    if (first == NULL || strcmp(name, first) < 0) {
      char *copy = lc_strdup_with_allocator(&pouch->allocator, name);

      if (copy == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to retain pouch outbox state reclaim marker",
                          NULL, NULL, "pouch");
        goto cleanup;
      }
      lc_free_with_allocator(&pouch->allocator, first);
      first = copy;
    }
    if (after_marker_id != NULL && after_marker_id[0] != '\0' &&
        strcmp(name, after_marker_id) > 0 &&
        (after == NULL || strcmp(name, after) < 0)) {
      char *copy = lc_strdup_with_allocator(&pouch->allocator, name);

      if (copy == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to retain pouch outbox state reclaim marker",
                          NULL, NULL, "pouch");
        goto cleanup;
      }
      lc_free_with_allocator(&pouch->allocator, after);
      after = copy;
    }
  }
  if (rc != LC_OK)
    goto cleanup;
  selected = after != NULL ? after : first;
  if (selected != NULL) {
    if (selected == after) {
      after = NULL;
    } else {
      first = NULL;
    }
    rc = directory.ops->read(directory.context, selected, &pouch->allocator,
                             LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES,
                             LC_POUCH_OUTBOX_STATE_RECLAIM_MAX_BYTES, &contents,
                             &contents_length, error);
    if (rc == LC_POUCH_OUTBOX_MARKER_MISSING) {
      rc = LC_OK;
      goto cleanup;
    }
    if (rc != LC_OK)
      goto cleanup;
    if (memcmp(contents, LC_POUCH_OUTBOX_STATE_RECLAIM_MAGIC, 4U) != 0) {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "invalid pouch outbox state reclaim marker", NULL, NULL,
                        "pouch");
      goto cleanup;
    }
    outbox_length = lc_pouch_outbox_state_reclaim_u32_read(contents + 4U);
    state_length = lc_pouch_outbox_state_reclaim_u32_read(contents + 8U);
    if (outbox_length == 0UL || state_length == 0UL ||
        outbox_length >
            contents_length - LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES ||
        state_length != contents_length -
                            LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES -
                            outbox_length) {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "invalid pouch outbox state reclaim marker lengths",
                        NULL, NULL, "pouch");
      goto cleanup;
    }
    *marker_id_out = selected;
    selected = NULL;
    *outbox_key_out = (char *)lc_alloc_with_allocator(
        &pouch->allocator, (size_t)outbox_length + 1U);
    *state_key_out = (char *)lc_alloc_with_allocator(&pouch->allocator,
                                                     (size_t)state_length + 1U);
    if (*outbox_key_out == NULL || *state_key_out == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch outbox state reclaim marker",
                        NULL, NULL, "pouch");
      goto cleanup;
    }
    memcpy(*outbox_key_out,
           contents + LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES,
           (size_t)outbox_length);
    (*outbox_key_out)[outbox_length] = '\0';
    memcpy(*state_key_out,
           contents + LC_POUCH_OUTBOX_STATE_RECLAIM_HEADER_BYTES +
               outbox_length,
           (size_t)state_length);
    (*state_key_out)[state_length] = '\0';
  }

cleanup:
  rc = lc_pouch_outbox_store_close(&directory, rc, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, *marker_id_out);
    lc_free_with_allocator(&pouch->allocator, *outbox_key_out);
    lc_free_with_allocator(&pouch->allocator, *state_key_out);
    *marker_id_out = NULL;
    *outbox_key_out = NULL;
    *state_key_out = NULL;
  }
  lc_free_with_allocator(&pouch->allocator, first);
  lc_free_with_allocator(&pouch->allocator, after);
  lc_free_with_allocator(&pouch->allocator, selected);
  lc_free_with_allocator(&pouch->allocator, contents);
  return rc;
}
int lc_pouch_outbox_state_reclaim_marker_remove(lc_pouch *pouch, const char *ns,
                                                const char *marker_id,
                                                lc_error *error) {
  lc_pouch_outbox_store store = {0};
  int rc;
  if (!lc_pouch_outbox_state_reclaim_marker_id_valid(marker_id))
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "invalid outbox reclaim marker", NULL, NULL, "pouch");
  rc = lc_pouch_outbox_store_open(pouch, ns, -1, 0, &store, error);
  if (rc == LC_OK)
    rc = store.ops->remove(store.context, marker_id, error);
  return lc_pouch_outbox_store_close(&store, rc, error);
}
void lc_pouch_outbox_retention_marker_cleanup(
    lc_pouch *pouch, lc_pouch_outbox_retention_marker *marker) {
  if (pouch == NULL || marker == NULL)
    return;
  lc_free_with_allocator(&pouch->allocator, marker->marker_id);
  lc_free_with_allocator(&pouch->allocator, marker->outbox_key);
  memset(marker, 0, sizeof(*marker));
}
static int lc_pouch_outbox_retention_marker_clone(
    lc_pouch *pouch, const lc_pouch_outbox_retention_marker *source,
    lc_pouch_outbox_retention_marker *out, lc_error *error) {
  if (pouch == NULL || source == NULL || out == NULL ||
      source->marker_id == NULL || source->outbox_key == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "invalid pouch outbox retention marker", NULL, NULL,
                        "pouch");
  }
  memset(out, 0, sizeof(*out));
  out->marker_id =
      lc_strdup_with_allocator(&pouch->allocator, source->marker_id);
  out->outbox_key =
      lc_strdup_with_allocator(&pouch->allocator, source->outbox_key);
  if (out->marker_id == NULL || out->outbox_key == NULL) {
    lc_pouch_outbox_retention_marker_cleanup(pouch, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch outbox retention marker", NULL,
                        NULL, "pouch");
  }
  out->due_at_unix = source->due_at_unix;
  out->retained_bytes = source->retained_bytes;
  return LC_OK;
}
static int lc_pouch_outbox_retention_marker_keep_lexical_first(
    lc_pouch *pouch, const lc_pouch_outbox_retention_marker *candidate,
    lc_pouch_outbox_retention_marker *selected, lc_error *error) {
  int rc;

  if (selected->marker_id != NULL &&
      strcmp(selected->marker_id, candidate->marker_id) <= 0) {
    return LC_OK;
  }
  lc_pouch_outbox_retention_marker_cleanup(pouch, selected);
  rc =
      lc_pouch_outbox_retention_marker_clone(pouch, candidate, selected, error);
  return rc;
}
static int lc_pouch_outbox_retention_marker_read(
    lc_pouch *pouch, lc_pouch_outbox_store *directory, const char *filename,
    lc_pouch_outbox_retention_marker *out, lc_error *error) {
  const char *marker_id =
      strlen(filename) == LC_XID_STRING_SIZE - 1U ? filename : filename + 17U;
  unsigned char *contents;
  size_t contents_length = 0U;
  uint64_t due_bits;
  uint64_t retained_bytes;
  unsigned long key_length;
  int rc;

  if (pouch == NULL || directory == NULL || marker_id == NULL || out == NULL ||
      !lc_xid_is_valid(marker_id)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "invalid pouch outbox retention marker", NULL, NULL,
                        "pouch");
  }
  memset(out, 0, sizeof(*out));
  contents = NULL;
  rc = directory->ops->read(directory->context, filename, &pouch->allocator,
                            LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_HEADER_BYTES,
                            LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MAX_BYTES,
                            &contents, &contents_length, error);
  if (rc == LC_OK &&
      memcmp(contents, LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MAGIC, 4U) != 0) {
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "invalid pouch outbox retention marker", NULL, NULL,
                      "pouch");
  }
  due_bits = rc == LC_OK
                 ? lc_pouch_outbox_dead_letter_reclaim_u64_read(contents + 4U)
                 : 0U;
  retained_bytes =
      rc == LC_OK ? lc_pouch_outbox_dead_letter_reclaim_u64_read(contents + 12U)
                  : 0U;
  key_length = rc == LC_OK
                   ? lc_pouch_outbox_state_reclaim_u32_read(contents + 20U)
                   : 0UL;
  if (rc == LC_OK &&
      (key_length == 0UL ||
       key_length !=
           (unsigned long)(contents_length -
                           LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_HEADER_BYTES))) {
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "invalid pouch outbox retention marker", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK) {
    out->marker_id = lc_strdup_with_allocator(&pouch->allocator, marker_id);
    out->outbox_key = (char *)lc_alloc_with_allocator(&pouch->allocator,
                                                      (size_t)key_length + 1U);
    if (out->marker_id == NULL || out->outbox_key == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch outbox retention marker", NULL,
                        NULL, "pouch");
    }
  }
  if (rc == LC_OK) {
    memcpy(out->outbox_key,
           contents + LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_HEADER_BYTES,
           (size_t)key_length);
    out->outbox_key[key_length] = '\0';
    out->due_at_unix = (lc_pouch_unix_seconds)(int64_t)due_bits;
    out->retained_bytes = retained_bytes;
    if (out->due_at_unix <= 0 || out->outbox_key[0] == '\0') {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "invalid pouch outbox retention marker", NULL, NULL,
                        "pouch");
    }
  }
  if (rc != LC_OK)
    lc_pouch_outbox_retention_marker_cleanup(pouch, out);
  lc_free_with_allocator(&pouch->allocator, contents);
  return rc;
}
int lc_pouch_outbox_retention_marker_write(lc_pouch *pouch, const char *ns,
                                           int kind, const char *marker_id,
                                           const char *outbox_key,
                                           lc_pouch_unix_seconds due_at_unix,
                                           uint64_t retained_bytes,
                                           lc_error *error) {
  unsigned char *contents;
  lc_pouch_outbox_store store = {0};
  char filename[17U + LC_XID_STRING_SIZE];
  const char *name = NULL;
  size_t key_length;
  size_t contents_length;
  int rc;

  if (pouch == NULL || outbox_key == NULL || outbox_key[0] == '\0' ||
      due_at_unix <= 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch outbox retention marker requires key and "
                        "deadline",
                        NULL, NULL, "pouch");
  }
  key_length = strlen(outbox_key);
  if (key_length > 0xffffffffUL ||
      key_length > LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MAX_BYTES -
                       LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_HEADER_BYTES) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch outbox retention marker is too large", NULL,
                        NULL, "pouch");
  }
  contents = NULL;
  rc = retention_name(kind, marker_id, due_at_unix, filename, &name, error);
  if (rc == LC_OK)
    rc = lc_pouch_outbox_store_open(pouch, ns, kind, 1, &store, error);
  contents_length =
      LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_HEADER_BYTES + key_length;
  if (rc == LC_OK) {
    contents = (unsigned char *)lc_alloc_with_allocator(&pouch->allocator,
                                                        contents_length);
    if (contents == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch outbox retention marker",
                        NULL, NULL, "pouch");
    }
  }
  if (rc == LC_OK) {
    memcpy(contents, LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MAGIC, 4U);
    lc_pouch_outbox_dead_letter_reclaim_u64_write(
        contents + 4U, (uint64_t)(int64_t)due_at_unix);
    lc_pouch_outbox_dead_letter_reclaim_u64_write(contents + 12U,
                                                  retained_bytes);
    lc_pouch_outbox_state_reclaim_u32_write(contents + 20U,
                                            (unsigned long)key_length);
    memcpy(contents + LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_HEADER_BYTES,
           outbox_key, key_length);
  }
  if (rc == LC_OK)
    rc =
        store.ops->write(store.context, name, contents, contents_length, error);
  lc_free_with_allocator(&pouch->allocator, contents);
  return lc_pouch_outbox_store_close(&store, rc, error);
}
int lc_pouch_outbox_retention_marker_remove(lc_pouch *pouch, const char *ns,
                                            int kind, const char *marker_id,
                                            lc_pouch_unix_seconds due_at_unix,
                                            lc_error *error) {
  lc_pouch_outbox_store store = {0};
  char filename[17U + LC_XID_STRING_SIZE];
  const char *name = NULL;
  int rc = retention_name(kind, marker_id, due_at_unix, filename, &name, error);
  if (rc == LC_OK)
    rc = lc_pouch_outbox_store_open(pouch, ns, kind, 0, &store, error);
  if (rc == LC_OK)
    rc = store.ops->remove(store.context, name, error);
  return lc_pouch_outbox_store_close(&store, rc, error);
}
static int lc_pouch_terminal_retention_filename(const char *name,
                                                lc_pouch_unix_seconds *due) {
  static const char hex[] = "0123456789abcdef";
  uint64_t value;
  unsigned int index;
  if (strlen(name) != 17U + LC_XID_STRING_SIZE - 1U || name[16] != '-' ||
      !lc_xid_is_valid(name + 17U))
    return 0;
  value = 0U;
  for (index = 0U; index < 16U; ++index) {
    const char *digit = strchr(hex, name[index]);
    if (digit == NULL)
      return 0;
    value = (value << 4U) | (uint64_t)(digit - hex);
  }
  if (value == 0U || value > (uint64_t)LC_I64_MAX)
    return 0;
  *due = (lc_pouch_unix_seconds)value;
  return 1;
}
static void lc_pouch_terminal_retention_keep_name(char *selected,
                                                  const char *candidate) {
  if (selected[0] == '\0' || strcmp(candidate + 17U, selected + 17U) < 0)
    strcpy(selected, candidate);
}
static int lc_pouch_terminal_retention_take(
    lc_pouch *pouch, const char *ns, int kind, const char *after,
    lc_pouch_unix_seconds now, size_t max_count,
    lc_pouch_outbox_retention_marker *out, lc_pouch_unix_seconds *next_due,
    lc_error *error) {
  char first[17U + LC_XID_STRING_SIZE];
  char later[17U + LC_XID_STRING_SIZE];
  char other[17U + LC_XID_STRING_SIZE];
  char due_first[17U + LC_XID_STRING_SIZE];
  char due_later[17U + LC_XID_STRING_SIZE];
  lc_pouch_outbox_store directory = {0};
  const char *name = NULL;
  const char *selected;
  size_t count;
  int rc;
  first[0] = later[0] = other[0] = due_first[0] = due_later[0] = '\0';
  count = 0U;
  rc = lc_pouch_outbox_store_open(pouch, ns, kind, 0, &directory, error);
  if (rc != LC_OK)
    goto cleanup;
  while (rc == LC_OK &&
         (rc = directory.ops->next(directory.context, &name, error)) == LC_OK &&
         name) {
    lc_pouch_unix_seconds due;
    const char *id;
    if (!lc_pouch_terminal_retention_filename(name, &due))
      continue;
    id = name + 17U;
    if (count != SIZE_MAX)
      ++count;
    if (*next_due == 0 || due < *next_due)
      *next_due = due;
    lc_pouch_terminal_retention_keep_name(first, name);
    if (after != NULL && strcmp(id, after) > 0)
      lc_pouch_terminal_retention_keep_name(later, name);
    if (after == NULL || strcmp(id, after) != 0)
      lc_pouch_terminal_retention_keep_name(other, name);
    if (due <= now) {
      lc_pouch_terminal_retention_keep_name(due_first, name);
      if (after != NULL && strcmp(id, after) > 0)
        lc_pouch_terminal_retention_keep_name(due_later, name);
    }
  }
  if (rc != LC_OK)
    goto cleanup;
  selected = max_count != SIZE_MAX && count > max_count
                 ? (later[0] != '\0'   ? later
                    : other[0] != '\0' ? other
                                       : first)
                 : (due_later[0] != '\0' ? due_later : due_first);
  if (selected[0] != '\0') {
    lc_pouch_unix_seconds expected_due = 0;
    lc_pouch_terminal_retention_filename(selected, &expected_due);
    rc = lc_pouch_outbox_retention_marker_read(pouch, &directory, selected, out,
                                               error);
    if (rc == LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MISSING) {
      lc_error_cleanup(error);
      lc_error_init(error);
      rc = LC_OK;
    } else if (rc == LC_OK && out->due_at_unix != expected_due) {
      lc_pouch_outbox_retention_marker_cleanup(pouch, out);
      rc =
          lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                       "terminal retention filename and marker deadline differ",
                       NULL, selected, "pouch");
    }
    /* A body failure retains durable work but must not pin the advisory turn
     * forever. The validated filename supplies identity only, never a parent
     * key or authority to delete a receipt. */
    strcpy(out->selected_id, selected + 17U);
    out->queued_count = count;
  }
cleanup:
  return lc_pouch_outbox_store_close(&directory, rc, error);
}
static int lc_pouch_retention_turn_precedes(const char *candidate,
                                            const char *selected,
                                            const char *after) {
  if (selected == NULL || selected[0] == '\0')
    return 1;
  if (after != NULL) {
    int candidate_after = strcmp(candidate, after) > 0;
    int selected_after = strcmp(selected, after) > 0;
    if (candidate_after != selected_after)
      return candidate_after;
  }
  return strcmp(candidate, selected) < 0;
}
static int lc_pouch_retention_marker_failure_turn(
    lc_pouch *pouch, lc_pouch_outbox_retention_marker *marker,
    const char *filename, const char *after, char *failed_name, int rc,
    lc_error *error) {
  if (rc != LC_ERR_PROTOCOL && rc != LC_ERR_TRANSPORT)
    return rc;
  if (lc_pouch_retention_turn_precedes(filename, failed_name, after))
    strcpy(failed_name, filename);
  lc_pouch_outbox_retention_marker_cleanup(pouch, marker);
  lc_error_cleanup(error);
  lc_error_init(error);
  return LC_OK;
}
int lc_pouch_outbox_retention_marker_take(lc_pouch *pouch, const char *ns,
                                          int kind, const char *after_marker_id,
                                          lc_pouch_unix_seconds now_unix,
                                          size_t max_count, uint64_t max_bytes,
                                          lc_pouch_outbox_retention_marker *out,
                                          lc_pouch_unix_seconds *next_due_unix,
                                          lc_error *error) {
  lc_pouch_outbox_store directory = {0};
  const char *name = NULL;
  lc_pouch_outbox_retention_marker oldest;
  lc_pouch_outbox_retention_marker oldest_after;
  lc_pouch_outbox_retention_marker oldest_other;
  lc_pouch_outbox_retention_marker earliest;
  lc_pouch_outbox_retention_marker due_first;
  lc_pouch_outbox_retention_marker due_after;
  char later_name[LC_XID_STRING_SIZE];
  char other_name[LC_XID_STRING_SIZE];
  char failed_name[LC_XID_STRING_SIZE];
  size_t count;
  uint64_t bytes;
  int capacity_exceeded;
  int rc;

  if (pouch == NULL || out == NULL || next_due_unix == NULL || now_unix <= 0 ||
      (after_marker_id != NULL && !lc_xid_is_valid(after_marker_id))) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch outbox retention take requires outputs", NULL,
                        NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  *next_due_unix = 0;
  if (kind != LC_POUCH_RETENTION_DEAD_LETTER)
    return lc_pouch_terminal_retention_take(pouch, ns, kind, after_marker_id,
                                            now_unix, max_count, out,
                                            next_due_unix, error);
  memset(&oldest, 0, sizeof(oldest));
  memset(&oldest_after, 0, sizeof(oldest_after));
  memset(&oldest_other, 0, sizeof(oldest_other));
  memset(&earliest, 0, sizeof(earliest));
  memset(&due_first, 0, sizeof(due_first));
  memset(&due_after, 0, sizeof(due_after));
  later_name[0] = '\0';
  other_name[0] = '\0';
  failed_name[0] = '\0';
  count = 0U;
  bytes = 0U;
  capacity_exceeded = 0;
  rc = lc_pouch_outbox_store_open(pouch, ns, kind, 0, &directory, error);
  if (rc != LC_OK)
    goto cleanup;
  while (rc == LC_OK &&
         (rc = directory.ops->next(directory.context, &name, error)) == LC_OK &&
         name) {
    lc_pouch_outbox_retention_marker candidate;

    if (!lc_xid_is_valid(name))
      continue;
    /* The capacity decision needs only a bounded prefix of marker bodies.
     * Finish the directory-name walk to find the true lexical successor;
     * choosing the first successor in readdir order can cycle among blocked
     * parents and starve a marker later in that order. */
    if (capacity_exceeded && after_marker_id != NULL) {
      if (strcmp(name, after_marker_id) > 0 &&
          (later_name[0] == '\0' || strcmp(name, later_name) < 0))
        strcpy(later_name, name);
      if (strcmp(name, after_marker_id) != 0 &&
          (other_name[0] == '\0' || strcmp(name, other_name) < 0))
        strcpy(other_name, name);
      continue;
    }
    memset(&candidate, 0, sizeof(candidate));
    rc = lc_pouch_outbox_retention_marker_read(pouch, &directory, name,
                                               &candidate, error);
    if (rc == LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MISSING) {
      lc_error_cleanup(error);
      lc_error_init(error);
      rc = LC_OK;
      continue;
    }
    if (rc != LC_OK) {
      rc = lc_pouch_retention_marker_failure_turn(
          pouch, &candidate, name, after_marker_id, failed_name, rc, error);
      lc_pouch_outbox_retention_marker_cleanup(pouch, &candidate);
      if (rc != LC_OK)
        break;
      /* Damaged markers still consume count capacity. Their byte accounting
       * and parent identity are untrusted until a later successful read. */
      if (count != SIZE_MAX)
        ++count;
      continue;
    }
    if (count != SIZE_MAX)
      ++count;
    if (~(uint64_t)0U - bytes < candidate.retained_bytes)
      bytes = ~(uint64_t)0U;
    else
      bytes += candidate.retained_bytes;
    rc = lc_pouch_outbox_retention_marker_keep_lexical_first(pouch, &candidate,
                                                             &oldest, error);
    if (rc == LC_OK && after_marker_id != NULL &&
        strcmp(candidate.marker_id, after_marker_id) > 0) {
      rc = lc_pouch_outbox_retention_marker_keep_lexical_first(
          pouch, &candidate, &oldest_after, error);
    }
    if (rc == LC_OK && after_marker_id != NULL &&
        strcmp(candidate.marker_id, after_marker_id) != 0) {
      rc = lc_pouch_outbox_retention_marker_keep_lexical_first(
          pouch, &candidate, &oldest_other, error);
    }
    if (rc == LC_OK && (earliest.marker_id == NULL ||
                        candidate.due_at_unix < earliest.due_at_unix)) {
      lc_pouch_outbox_retention_marker_cleanup(pouch, &earliest);
      rc = lc_pouch_outbox_retention_marker_clone(pouch, &candidate, &earliest,
                                                  error);
    }
    if (rc == LC_OK && candidate.due_at_unix <= now_unix) {
      rc = lc_pouch_outbox_retention_marker_keep_lexical_first(
          pouch, &candidate, &due_first, error);
      if (rc == LC_OK && after_marker_id != NULL &&
          strcmp(candidate.marker_id, after_marker_id) > 0) {
        rc = lc_pouch_outbox_retention_marker_keep_lexical_first(
            pouch, &candidate, &due_after, error);
      }
    }
    lc_pouch_outbox_retention_marker_cleanup(pouch, &candidate);
    /* A capacity breach is a service-protection event, not an inventory
     * report. Read no more marker bodies after this bounded prefix; when a
     * cursor exists, finish only the directory-name walk to select its true
     * lexical successor, then yield after one reclaim attempt. */
    if (rc == LC_OK && ((max_count != SIZE_MAX && count > max_count) ||
                        (max_bytes != ~(uint64_t)0U && bytes > max_bytes))) {
      capacity_exceeded = 1;
      /* A new turn without a cursor can yield immediately. With a cursor,
       * inspect the remaining names without reading their marker bodies. */
      if (after_marker_id == NULL) {
        break;
      }
    }
  }
  if (rc != LC_OK)
    goto cleanup;
  if (later_name[0] != '\0' &&
      (oldest_after.marker_id == NULL ||
       strcmp(later_name, oldest_after.marker_id) < 0)) {
    lc_pouch_outbox_retention_marker_cleanup(pouch, &oldest_after);
    rc = lc_pouch_outbox_retention_marker_read(pouch, &directory, later_name,
                                               &oldest_after, error);
    if (rc == LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MISSING) {
      lc_error_cleanup(error);
      lc_error_init(error);
      rc = LC_OK;
    }
    rc = lc_pouch_retention_marker_failure_turn(pouch, &oldest_after,
                                                later_name, after_marker_id,
                                                failed_name, rc, error);
  }
  if (rc == LC_OK && other_name[0] != '\0' &&
      (oldest_other.marker_id == NULL ||
       strcmp(other_name, oldest_other.marker_id) < 0)) {
    lc_pouch_outbox_retention_marker_cleanup(pouch, &oldest_other);
    rc = lc_pouch_outbox_retention_marker_read(pouch, &directory, other_name,
                                               &oldest_other, error);
    if (rc == LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MISSING) {
      lc_error_cleanup(error);
      lc_error_init(error);
      rc = LC_OK;
    }
    rc = lc_pouch_retention_marker_failure_turn(pouch, &oldest_other,
                                                other_name, after_marker_id,
                                                failed_name, rc, error);
  }
  if (rc != LC_OK)
    goto cleanup;
  capacity_exceeded = capacity_exceeded ||
                      (max_count != SIZE_MAX && count > max_count) ||
                      (max_bytes != ~(uint64_t)0U && bytes > max_bytes);
  if (earliest.marker_id != NULL)
    *next_due_unix = earliest.due_at_unix;
  if (capacity_exceeded &&
      (oldest_after.marker_id != NULL || oldest_other.marker_id != NULL ||
       oldest.marker_id != NULL)) {
    if (oldest_after.marker_id != NULL) {
      *out = oldest_after;
      memset(&oldest_after, 0, sizeof(oldest_after));
    } else if (oldest_other.marker_id != NULL) {
      *out = oldest_other;
      memset(&oldest_other, 0, sizeof(oldest_other));
    } else {
      *out = oldest;
      memset(&oldest, 0, sizeof(oldest));
    }
  } else if (due_after.marker_id != NULL || due_first.marker_id != NULL) {
    if (due_after.marker_id != NULL) {
      *out = due_after;
      memset(&due_after, 0, sizeof(due_after));
    } else {
      *out = due_first;
      memset(&due_first, 0, sizeof(due_first));
    }
  }
  if (failed_name[0] != '\0' &&
      lc_pouch_retention_turn_precedes(failed_name, out->marker_id,
                                       after_marker_id)) {
    /* Re-read only the failed identity selected for this turn. Preserve its
     * actual error for the caller; other damaged bodies cannot pin that turn.
     * A concurrent repair must still satisfy expiry/capacity before deletion.
     */
    lc_pouch_outbox_retention_marker_cleanup(pouch, out);
    rc = lc_pouch_outbox_retention_marker_read(pouch, &directory, failed_name,
                                               out, error);
    if (rc == LC_POUCH_OUTBOX_DEAD_LETTER_RECLAIM_MISSING) {
      lc_error_cleanup(error);
      lc_error_init(error);
      rc = LC_OK;
    }
    if (rc == LC_OK && out->marker_id != NULL && !capacity_exceeded &&
        out->due_at_unix > now_unix) {
      if (*next_due_unix == 0 || out->due_at_unix < *next_due_unix)
        *next_due_unix = out->due_at_unix;
      lc_pouch_outbox_retention_marker_cleanup(pouch, out);
    }
    strcpy(out->selected_id, failed_name);
  } else if (out->marker_id != NULL) {
    strcpy(out->selected_id, out->marker_id);
  }
  out->queued_count = count;
  out->queued_bytes = bytes;

cleanup:
  lc_pouch_outbox_retention_marker_cleanup(pouch, &oldest);
  lc_pouch_outbox_retention_marker_cleanup(pouch, &oldest_after);
  lc_pouch_outbox_retention_marker_cleanup(pouch, &oldest_other);
  lc_pouch_outbox_retention_marker_cleanup(pouch, &earliest);
  lc_pouch_outbox_retention_marker_cleanup(pouch, &due_first);
  lc_pouch_outbox_retention_marker_cleanup(pouch, &due_after);
  return lc_pouch_outbox_store_close(&directory, rc, error);
}
