#include "lc_pouch_xa.h"
#include "lc_api_internal.h"
#include "lc_intcompat.h"
#include "lc_pouch_internal.h"

#include <ctype.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

typedef struct lc_xa_field {
  size_t offset;
  size_t size;
} lc_xa_field;

#define XA_FIELD(name)                                                         \
  {offsetof(lc_xa_branch_record, name),                                        \
   sizeof(((lc_xa_branch_record *)0)->name)}
static const lc_xa_field fields[] = {
    XA_FIELD(txn_id),       XA_FIELD(resource_id), XA_FIELD(branch_id),
    XA_FIELD(gid),          XA_FIELD(full_xid),    XA_FIELD(system_id),
    XA_FIELD(database_oid), XA_FIELD(role_oid)};
#undef XA_FIELD

/* Encode fields individually: public C integer width, alignment and structure
 * padding must not affect durable bytes. No application SQL is stored here.
 */
#define XA_WIRE_SIZE (4U + 3U * LC_XID_STRING_SIZE + 200U + 42U + 22U + 3U + 8U)

static int invalid(lc_error *error, const char *message) {
  return lc_error_set(error, LC_ERR_PROTOCOL, 0L, message, NULL, NULL,
                      "pouch-xa");
}

static int decimal(const char *text, size_t capacity, lc_u64 maximum) {
  lc_u64 value;
  size_t i;
  if (text[0] == '\0' || (text[0] == '0' && text[1] != '\0'))
    return 0;
  for (i = 0U; i < capacity && text[i] != '\0'; ++i)
    if (text[i] < '0' || text[i] > '9')
      return 0;
  return i < capacity && lc_u64_parse_base10(text, &value) && value != 0U &&
         value <= maximum;
}

static int validate(const lc_xa_branch_record *branch, lc_error *error) {
  size_t i;
  const char *field;
  if (branch == NULL)
    return invalid(error, "XA branch descriptor is required");
  for (i = 0U; i < sizeof(fields) / sizeof(fields[0]); ++i) {
    field = (const char *)branch + fields[i].offset;
    if (memchr(field, '\0', fields[i].size) == NULL || field[0] == '\0')
      return invalid(error, "XA branch identity is missing or unterminated");
  }
  if (!lc_xid_is_valid(branch->txn_id) ||
      !lc_xid_is_valid(branch->resource_id) ||
      !lc_xid_is_valid(branch->branch_id) ||
      !decimal(branch->full_xid, sizeof(branch->full_xid), LC_U64_MAX) ||
      !decimal(branch->system_id, sizeof(branch->system_id), LC_U64_MAX) ||
      !decimal(branch->database_oid, sizeof(branch->database_oid),
               (lc_u64)0xffffffffUL) ||
      !decimal(branch->role_oid, sizeof(branch->role_oid),
               (lc_u64)0xffffffffUL))
    return invalid(error, "XA branch native identity is invalid");
  for (i = 0U; branch->gid[i] != '\0'; ++i)
    if ((unsigned char)branch->gid[i] < 33U ||
        (unsigned char)branch->gid[i] > 126U || branch->gid[i] == '\'' ||
        branch->gid[i] == '\\')
      return invalid(error,
                     "XA branch GID is not a generated ASCII identifier");
  if (branch->prepared_observed > 1U ||
      (branch->phase <= LC_XA_PREPARE_SENT && branch->prepared_observed) ||
      ((branch->phase == LC_XA_PREPARED || branch->phase == LC_XA_COMMIT_SENT ||
        branch->phase == LC_XA_COMMITTED) &&
       !branch->prepared_observed) ||
      branch->phase > LC_XA_ABORTED ||
      branch->evidence > LC_XA_EVIDENCE_RECONCILED ||
      (branch->phase < LC_XA_COMMITTED &&
       (branch->evidence != LC_XA_EVIDENCE_NONE ||
        branch->completed_at_unix != 0)) ||
      (branch->phase >= LC_XA_COMMITTED &&
       (branch->evidence == LC_XA_EVIDENCE_NONE ||
        branch->completed_at_unix <= 0)))
    return invalid(error,
                   "XA branch progress or completion evidence is invalid");
  return LC_OK;
}

int lc_pouch_xa_branch_key(const lc_xa_branch_record *branch, char *out,
                           size_t capacity, lc_error *error) {
  int length;
  int rc = validate(branch, error);
  if (rc != LC_OK)
    return rc;
  if (out == NULL || capacity < LC_POUCH_XA_KEY_SIZE)
    return invalid(error, "XA branch key output is too small");
  length = snprintf(out, capacity, "branch/%s/%s", branch->txn_id,
                    branch->branch_id);
  if (length < 0 || (size_t)length >= capacity)
    return invalid(error, "XA branch key exceeds capacity");
  return LC_OK;
}

static void encode(const lc_xa_branch_record *branch,
                   unsigned char bytes[XA_WIRE_SIZE]) {
  size_t offset = 4U;
  size_t i;
  lc_u64 completed = (lc_u64)branch->completed_at_unix;
  memset(bytes, 0, XA_WIRE_SIZE);
  memcpy(bytes, "LPXB", 4U);
  for (i = 0U; i < sizeof(fields) / sizeof(fields[0]); ++i) {
    const char *field = (const char *)branch + fields[i].offset;
    memcpy(bytes + offset, field, strlen(field));
    offset += fields[i].size;
  }
  bytes[offset++] = branch->prepared_observed;
  bytes[offset++] = branch->phase;
  bytes[offset++] = branch->evidence;
  for (i = 0U; i < 8U; ++i)
    bytes[offset + i] = (unsigned char)((completed >> (i * 8U)) & 0xffU);
}

static int decode(lc_source *source, const char *key, lc_xa_branch_record *out,
                  lc_error *error) {
  unsigned char bytes[XA_WIRE_SIZE + 1U];
  char expected[LC_POUCH_XA_KEY_SIZE];
  size_t count;
  size_t total = 0U;
  size_t offset = 4U;
  size_t i;
  size_t j;
  lc_u64 completed = 0U;
  lc_error read_error;
  int rc;
  memset(out, 0, sizeof(*out));
  while (total < sizeof(bytes)) {
    count = 0U;
    lc_error_init(&read_error);
    count =
        source->read(source, bytes + total, sizeof(bytes) - total, &read_error);
    if (read_error.code != LC_OK) {
      rc = lc_error_set(error, read_error.code, read_error.http_status,
                        read_error.message, read_error.detail,
                        read_error.server_code, read_error.correlation_id);
      lc_error_cleanup(&read_error);
      return rc;
    }
    lc_error_cleanup(&read_error);
    if (count == 0U)
      break;
    total += count;
  }
  if (total != XA_WIRE_SIZE || memcmp(bytes, "LPXB", 4U) != 0)
    return invalid(error, "XA branch record is truncated, extended or invalid");
  for (i = 0U; i < sizeof(fields) / sizeof(fields[0]); ++i) {
    char *field = (char *)out + fields[i].offset;
    memcpy(field, bytes + offset, fields[i].size);
    offset += fields[i].size;
    for (j = 0U; j < fields[i].size && field[j] != '\0'; ++j) {
    }
    for (; j < fields[i].size; ++j)
      if (field[j] != '\0')
        return invalid(error,
                       "XA branch record has noncanonical string padding");
  }
  out->prepared_observed = bytes[offset++];
  out->phase = bytes[offset++];
  out->evidence = bytes[offset++];
  for (i = 0U; i < 8U; ++i)
    completed |= (lc_u64)bytes[offset + i] << (i * 8U);
  if (completed > (lc_u64)LC_I64_MAX)
    return invalid(error, "XA completion timestamp exceeds supported range");
  out->completed_at_unix = (lc_unix_seconds)completed;
  rc = lc_pouch_xa_branch_key(out, expected, sizeof(expected), error);
  if (rc == LC_OK && strcmp(expected, key) != 0)
    rc = invalid(error, "XA branch record does not match its storage identity");
  return rc;
}

int lc_pouch_xa_branch_read(lc_pouch *pouch, const char *key,
                            lc_xa_branch_record *out, lc_error *error) {
  lc_pouch_state_read_result result;
  int rc;
  if (pouch == NULL || key == NULL || out == NULL)
    return invalid(error, "XA branch read requires store, key and output");
  memset(&result, 0, sizeof(result));
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_state_read(pouch, LC_POUCH_XA_NAMESPACE, key, &result, error);
  if (rc == LC_OK && (!result.found || result.body == NULL))
    rc = invalid(error, "XA participant is missing its durable descriptor");
  if (rc == LC_OK)
    rc = decode(result.body, key, out, error);
  lc_pouch_state_read_result_cleanup(&pouch->allocator, &result);
  return rc;
}

typedef struct lc_xa_write {
  lc_pouch *pouch;
  const lc_xa_branch_record *branch;
  const char *key;
} lc_xa_write;

static int write_locked(void *context, lc_error *error) {
  lc_xa_write *write = (lc_xa_write *)context;
  lc_pouch_state_read_result current;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  lc_xa_branch_record previous;
  unsigned char bytes[XA_WIRE_SIZE];
  lc_source *source = NULL;
  size_t i;
  int rc;
  memset(&current, 0, sizeof(current));
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  rc = lc_pouch_state_read_locked(write->pouch, LC_POUCH_XA_NAMESPACE,
                                  write->key, &current, error);
  if (rc == LC_OK && current.found)
    rc = current.body != NULL
             ? decode(current.body, write->key, &previous, error)
             : invalid(error, "XA descriptor has no body");
  if (rc == LC_OK && current.found) {
    for (i = 0U; i < sizeof(fields) / sizeof(fields[0]); ++i)
      if (strcmp((const char *)&previous + fields[i].offset,
                 (const char *)write->branch + fields[i].offset) != 0) {
        rc =
            invalid(error, "XA branch identity cannot change after enrollment");
        break;
      }
    if (rc == LC_OK && previous.phase != write->branch->phase) {
      int allowed = (previous.phase == LC_XA_ENROLLED &&
                     write->branch->phase == LC_XA_PREPARE_SENT) ||
                    (previous.phase == LC_XA_PREPARE_SENT &&
                     write->branch->phase == LC_XA_PREPARED) ||
                    (previous.phase == LC_XA_PREPARED &&
                     write->branch->phase == LC_XA_COMMIT_SENT) ||
                    (previous.phase <= LC_XA_PREPARED &&
                     write->branch->phase == LC_XA_ROLLBACK_SENT) ||
                    (previous.phase == LC_XA_COMMIT_SENT &&
                     write->branch->phase == LC_XA_COMMITTED) ||
                    (previous.phase == LC_XA_ROLLBACK_SENT &&
                     write->branch->phase == LC_XA_ABORTED);
      if (!allowed)
        rc = invalid(error, "XA branch progress contradicts its durable phase");
    }
    if (rc == LC_OK &&
        previous.prepared_observed != write->branch->prepared_observed &&
        (previous.prepared_observed ||
         (write->branch->phase != LC_XA_PREPARED &&
          write->branch->phase != LC_XA_ROLLBACK_SENT)))
      rc = invalid(error, "XA native prepare observation cannot be forgotten");
    if (rc == LC_OK && previous.phase == write->branch->phase &&
        previous.prepared_observed == write->branch->prepared_observed) {
      lc_pouch_state_read_result_cleanup(&write->pouch->allocator, &current);
      return LC_OK;
    }
    options.has_expected_version = 1;
    options.expected_version = current.version;
  } else if (rc == LC_OK && write->branch->phase != LC_XA_ENROLLED) {
    rc = invalid(error, "XA branch must be enrolled before recording progress");
  } else if (rc == LC_OK) {
    options.create_if_absent = 1;
  }
  if (rc == LC_OK) {
    encode(write->branch, bytes);
    rc = lc_source_from_memory(bytes, sizeof(bytes), &source, error);
  }
  if (rc == LC_OK) {
    options.object_record = 1;
    options.content_type = "application/x-lockdc-xa-branch";
    rc = lc_pouch_state_write_locked(write->pouch, LC_POUCH_XA_NAMESPACE,
                                     write->key, source, &options, &result,
                                     error);
  }
  if (source != NULL)
    source->close(source);
  lc_pouch_state_read_result_cleanup(&write->pouch->allocator, &current);
  lc_pouch_state_write_result_cleanup(&write->pouch->allocator, &result);
  return rc;
}

int lc_pouch_xa_branch_write(lc_pouch *pouch, const lc_xa_branch_record *branch,
                             lc_error *error) {
  lc_xa_write context;
  char key[LC_POUCH_XA_KEY_SIZE];
  int rc = lc_pouch_xa_branch_key(branch, key, sizeof(key), error);
  if (rc != LC_OK)
    return rc;
  if (pouch == NULL)
    return invalid(error, "XA branch write requires an open Pouch store");
  context.pouch = pouch;
  context.branch = branch;
  context.key = key;
  rc = lc_pouch_state_with_key_lock(pouch, LC_POUCH_XA_NAMESPACE, key,
                                    write_locked, &context, error);
  if (rc == LC_OK)
    rc = lc_pouch_state_sync_namespace(pouch, LC_POUCH_XA_NAMESPACE, error);
  return rc;
}

int lc_pouch_xa_participant(const lc_txn_participant *participant) {
  return participant != NULL && participant->ns != NULL &&
         strcmp(participant->ns, LC_POUCH_XA_NAMESPACE) == 0;
}

int lc_pouch_xa_has_participants(const lc_txn_decision_req *request) {
  size_t i;
  if (request == NULL || request->participants == NULL)
    return 0;
  for (i = 0U; i < request->participant_count; ++i)
    if (lc_pouch_xa_participant(&request->participants[i]))
      return 1;
  return 0;
}

int lc_pouch_xa_application_gate(lc_pouch *pouch,
                                 const lc_txn_decision_req *request,
                                 const char *decision, lc_error *error) {
  char backend[LC_POUCH_BACKEND_HASH_HEX_BYTES + 1U];
  lc_xa_branch_record branch;
  size_t i;
  int rc;
  int pending = 0;
  if (!lc_pouch_xa_has_participants(request))
    return LC_OK;
  rc = lc_pouch_backend_hash(pouch, backend, error);
  if (rc != LC_OK)
    return rc;
  for (i = 0U; i < request->participant_count; ++i) {
    const lc_txn_participant *participant = &request->participants[i];
    unsigned char expected =
        strcmp(decision, "commit") == 0 ? LC_XA_COMMITTED : LC_XA_ABORTED;
    if (!lc_pouch_xa_participant(participant))
      continue;
    if (participant->backend_hash != NULL) {
      const char *start = participant->backend_hash;
      size_t length;
      while (*start != '\0' && isspace((unsigned char)*start))
        ++start;
      length = strlen(start);
      while (length > 0U && isspace((unsigned char)start[length - 1U]))
        --length;
      if (length > 0U &&
          (length != strlen(backend) || strncmp(start, backend, length) != 0))
        continue;
    }
    rc = lc_pouch_xa_branch_read(pouch, participant->key, &branch, error);
    if (rc != LC_OK)
      return rc;
    if (strcmp(branch.txn_id, request->txn_id) != 0)
      return invalid(error, "XA participant belongs to another transaction");
    if (branch.phase >= LC_XA_COMMITTED && branch.phase != expected)
      return invalid(error,
                     "XA branch outcome contradicts the global decision");
    if (branch.phase != expected)
      pending = 1;
  }
  if (pending)
    return lc_error_set(error, LC_ERR_PENDING, 0L,
                        "transaction awaits external participant completion",
                        NULL, "xa_pending", "pouch-xa");
  return LC_OK;
}
