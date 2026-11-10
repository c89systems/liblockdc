#include "lc_mutate_stream.h"
#include "lc_utf8.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <pwd.h>
#include <strings.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#if defined(__linux__)
extern time_t timegm(struct tm *tm_value);
#endif

#define LC_MS_SCRATCH 16384U

typedef lc_utf8_state lc_ms_utf8_state;

typedef struct lc_ms_rewrite_context {
  const lc_mutation *mutation;
} lc_ms_rewrite_context;

static int lc_ms_set_error(lc_error *error, const char *message,
                           const char *detail) {
  return lc_error_set(error, LC_ERR_INVALID, 0L, message, detail, NULL, NULL);
}

static char *lc_ms_strdup_range(const char *start, size_t length) {
  char *copy;

  copy = (char *)malloc(length + 1U);
  if (copy == NULL) {
    return NULL;
  }
  if (length != 0U) {
    memcpy(copy, start, length);
  }
  copy[length] = '\0';
  return copy;
}

static int lc_ms_append_ptr(char ***items, size_t *count, char *value) {
  char **next_items;

  next_items = (char **)realloc(*items, (*count + 1U) * sizeof(char *));
  if (next_items == NULL) {
    return 0;
  }
  *items = next_items;
  (*items)[*count] = value;
  *count += 1U;
  return 1;
}

static void lc_ms_value_cleanup(lc_mutation_value *value) {
  if (value == NULL) {
    return;
  }
  free(value->string_value);
  free(value->file_path);
  memset(value, 0, sizeof(*value));
}

static void lc_ms_mutation_cleanup(lc_mutation *mutation) {
  size_t i;

  if (mutation == NULL) {
    return;
  }
  if (mutation->path_segments != NULL) {
    for (i = 0; i < mutation->path_segment_count; ++i) {
      free(mutation->path_segments[i]);
    }
    free(mutation->path_segments);
  }
  lc_ms_value_cleanup(&mutation->value);
  memset(mutation, 0, sizeof(*mutation));
}

void lc_mutation_plan_close(lc_mutation_plan *plan) {
  size_t i;

  if (plan == NULL) {
    return;
  }
  for (i = 0; i < plan->count; ++i) {
    lc_ms_mutation_cleanup(&plan->items[i]);
  }
  free(plan->items);
  free(plan);
}

static lonejson_status lc_ms_lonejson_file_sink(void *user, const void *data,
                                                size_t len,
                                                lonejson_error *error) {
  FILE *fp;

  fp = (FILE *)user;
  if (fp == NULL || (len > 0U && data == NULL)) {
    if (error != NULL) {
      lonejson_error_init(error);
      error->code = LONEJSON_STATUS_INVALID_ARGUMENT;
      snprintf(error->message, sizeof(error->message),
               "invalid mutate output stream");
    }
    return LONEJSON_STATUS_INVALID_ARGUMENT;
  }
  if (len != 0U && fwrite(data, 1U, len, fp) != len) {
    if (error != NULL) {
      lonejson_error_init(error);
      error->code = LONEJSON_STATUS_IO_ERROR;
      error->system_errno = errno;
      snprintf(error->message, sizeof(error->message),
               "failed to write mutate output");
    }
    return LONEJSON_STATUS_IO_ERROR;
  }
  return LONEJSON_STATUS_OK;
}

static int lc_ms_write_bytes(FILE *out, const void *bytes, size_t length,
                             lc_error *error) {
  if (length == 0U) {
    return LC_OK;
  }
  if (out == NULL || bytes == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "invalid file-backed mutation spool", NULL, NULL, NULL);
  }
  if (fwrite(bytes, 1U, length, out) != length) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to write file-backed mutation spool",
                        strerror(errno), NULL, NULL);
  }
  return LC_OK;
}

static int lc_ms_copy_file(FILE *input, FILE *output, lc_error *error) {
  unsigned char *buffer;
  size_t nread;
  int rc;

  if (input == NULL || output == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "invalid local mutate copy stream", NULL, NULL, NULL);
  }
  buffer = (unsigned char *)lc_alloc_with_allocator(NULL, LC_MS_SCRATCH);
  if (buffer == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate mutation copy buffer", NULL, NULL,
                        NULL);
  }
  rc = LC_OK;
  if (fflush(output) != 0 || ftruncate(fileno(output), 0) != 0 ||
      fseek(output, 0L, SEEK_SET) != 0 || fseek(input, 0L, SEEK_SET) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to reset local mutate no-op stream",
                      strerror(errno), NULL, NULL);
    goto done;
  }
  clearerr(input);
  clearerr(output);
  for (;;) {
    nread = fread(buffer, 1U, LC_MS_SCRATCH, input);
    if (nread != 0U && fwrite(buffer, 1U, nread, output) != nread) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to write local mutate no-op stream",
                        strerror(errno), NULL, NULL);
      goto done;
    }
    if (nread < LC_MS_SCRATCH) {
      if (ferror(input)) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to read local mutate no-op stream",
                          strerror(errno), NULL, NULL);
        goto done;
      }
      break;
    }
  }
  if (fflush(output) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to flush local mutate no-op stream",
                      strerror(errno), NULL, NULL);
  }
done:
  lc_free_with_allocator(NULL, buffer);
  return rc;
}

static int lc_ms_lonejson_rewrite_error(lc_error *error, lonejson_status status,
                                        const lonejson_error *lj_error,
                                        const char *fallback) {
  const char *message;

  message = lj_error != NULL && lj_error->message[0] != '\0' ? lj_error->message
                                                             : fallback;
  switch (status) {
  case LONEJSON_STATUS_ALLOCATION_FAILED:
    return lc_error_set(error, LC_ERR_NOMEM, 0L, message, NULL, NULL, NULL);
  case LONEJSON_STATUS_CALLBACK_FAILED:
  case LONEJSON_STATUS_IO_ERROR:
  case LONEJSON_STATUS_INTERNAL_ERROR:
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L, message, NULL, NULL, NULL);
  default:
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
  }
}

static lonejson_status lc_ms_lonejson_error_from_lc(lonejson_error *error,
                                                    const lc_error *lc_error) {
  lonejson_status status;

  status = LONEJSON_STATUS_INVALID_JSON;
  if (lc_error != NULL) {
    if (lc_error->code == LC_ERR_NOMEM) {
      status = LONEJSON_STATUS_ALLOCATION_FAILED;
    } else if (lc_error->code == LC_ERR_TRANSPORT) {
      status = LONEJSON_STATUS_IO_ERROR;
    }
  }
  if (error != NULL) {
    lonejson_error_init(error);
    error->code = status;
    if (status == LONEJSON_STATUS_IO_ERROR) {
      error->system_errno = errno;
    }
    snprintf(error->message, sizeof(error->message), "%s",
             lc_error != NULL && lc_error->message != NULL
                 ? lc_error->message
                 : "failed to emit mutation value");
  }
  return status;
}

static int lc_ms_utf8_push(lc_ms_utf8_state *state, unsigned char byte,
                           lc_error *error) {
  if (!lc_utf8_push(state, &byte, 1U)) {
    return lc_ms_set_error(error, "invalid UTF-8 in textfile mutation", NULL);
  }
  return LC_OK;
}

static int lc_ms_copy_source_bytes(FILE *out, lc_source *source,
                                   int validate_text, int strict_text,
                                   int *out_text_ok, lc_error *error) {
  unsigned char *buffer;
  size_t nread;
  size_t i;
  lc_ms_utf8_state utf8;
  int text_ok;
  int rc;

  buffer = (unsigned char *)lc_alloc_with_allocator(NULL, LC_MS_SCRATCH);
  if (buffer == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate mutation source buffer", NULL, NULL,
                        NULL);
  }
  rc = LC_OK;
  memset(&utf8, 0, sizeof(utf8));
  text_ok = validate_text ? 1 : 0;
  for (;;) {
    nread = source->read(source, buffer, LC_MS_SCRATCH, error);
    if (nread == 0U) {
      if (error != NULL && error->code != LC_OK) {
        rc = error->code;
        goto done;
      }
      break;
    }
    rc = lc_ms_write_bytes(out, buffer, nread, error);
    if (rc != LC_OK) {
      goto done;
    }
    for (i = 0; i < nread; ++i) {
      if (validate_text && text_ok) {
        if (buffer[i] == '\0') {
          text_ok = 0;
          if (strict_text) {
            rc = lc_ms_set_error(error, "invalid UTF-8 in textfile mutation",
                                 NULL);
            goto done;
          }
          continue;
        }
        if (lc_ms_utf8_push(&utf8, buffer[i], error) != LC_OK) {
          if (strict_text) {
            rc = lc_ms_set_error(error, "invalid UTF-8 in textfile mutation",
                                 NULL);
            goto done;
          }
          lc_error_cleanup(error);
          lc_error_init(error);
          text_ok = 0;
        }
      }
    }
  }
  if (validate_text && text_ok && utf8.remaining != 0) {
    if (strict_text) {
      rc = lc_ms_set_error(error, "invalid UTF-8 in textfile mutation", NULL);
      goto done;
    }
    text_ok = 0;
  }
  if (out_text_ok != NULL) {
    *out_text_ok = text_ok;
  }
done:
  lc_free_with_allocator(NULL, buffer);
  return rc;
}

static int lc_ms_open_file_source(const lc_mutation_value *value,
                                  lc_source **out, lc_error *error) {
  if (value->file_value_resolver != NULL &&
      value->file_value_resolver->open != NULL) {
    return value->file_value_resolver->open(value->file_value_resolver->context,
                                            value->file_path, out, error);
  }
  return lc_source_from_file(value->file_path, out, error);
}

static lonejson_status lc_ms_emit_file_mutation_value(
    lonejson_writer *writer, const lc_mutation_value *value,
    lc_mutation_file_mode requested_mode, lonejson_error *error) {
  lc_source *source;
  FILE *scratch;
  int validate_text;
  int text_ok;
  lc_mutation_file_mode actual_mode;
  lonejson_source lj_source;
  lc_error lc_error_value;
  int rc;
  lonejson_status status;

  source = NULL;
  scratch = NULL;
  lc_error_init(&lc_error_value);
  actual_mode = requested_mode;
  if (actual_mode == LC_MUTATION_FILE_BASE64 &&
      value->file_value_resolver == NULL) {
    lonejson_source_init(&lj_source);
    status = lj_source.set_path(&lj_source, value->file_path, error);
    if (status == LONEJSON_STATUS_OK) {
      status = writer->source_base64(writer, &lj_source, error);
    }
    lj_source.cleanup(&lj_source);
    return status;
  }

  rc = lc_ms_open_file_source(value, &source, &lc_error_value);
  if (rc != LC_OK) {
    status = lc_ms_lonejson_error_from_lc(error, &lc_error_value);
    lc_error_cleanup(&lc_error_value);
    return status;
  }
  scratch = tmpfile();
  if (scratch == NULL) {
    source->close(source);
    lc_error_set(&lc_error_value, LC_ERR_TRANSPORT, 0L,
                 "failed to create file-backed mutation spool", strerror(errno),
                 NULL, NULL);
    status = lc_ms_lonejson_error_from_lc(error, &lc_error_value);
    lc_error_cleanup(&lc_error_value);
    return status;
  }
  validate_text = actual_mode == LC_MUTATION_FILE_TEXT ||
                  actual_mode == LC_MUTATION_FILE_AUTO;
  text_ok = 0;
  rc = lc_ms_copy_source_bytes(scratch, source, validate_text,
                               actual_mode == LC_MUTATION_FILE_TEXT, &text_ok,
                               &lc_error_value);
  source->close(source);
  if (rc != LC_OK) {
    fclose(scratch);
    status = lc_ms_lonejson_error_from_lc(error, &lc_error_value);
    lc_error_cleanup(&lc_error_value);
    return status;
  }
  if (actual_mode == LC_MUTATION_FILE_AUTO) {
    actual_mode = text_ok ? LC_MUTATION_FILE_TEXT : LC_MUTATION_FILE_BASE64;
  } else if (actual_mode == LC_MUTATION_FILE_TEXT && !text_ok) {
    fclose(scratch);
    lc_ms_set_error(&lc_error_value, "invalid UTF-8 in textfile mutation",
                    NULL);
    status = lc_ms_lonejson_error_from_lc(error, &lc_error_value);
    lc_error_cleanup(&lc_error_value);
    return status;
  }
  if (fflush(scratch) != 0 || fseek(scratch, 0L, SEEK_SET) != 0) {
    fclose(scratch);
    lc_error_set(&lc_error_value, LC_ERR_TRANSPORT, 0L,
                 "failed to rewind file-backed mutation spool", strerror(errno),
                 NULL, NULL);
    status = lc_ms_lonejson_error_from_lc(error, &lc_error_value);
    lc_error_cleanup(&lc_error_value);
    return status;
  }

  lonejson_source_init(&lj_source);
  status = lj_source.set_file(&lj_source, scratch, error);
  if (status == LONEJSON_STATUS_OK) {
    if (actual_mode == LC_MUTATION_FILE_TEXT) {
      status = writer->source_text(writer, &lj_source, error);
    } else {
      status = writer->source_base64(writer, &lj_source, error);
    }
  }
  lj_source.cleanup(&lj_source);
  fclose(scratch);
  lc_error_cleanup(&lc_error_value);
  return status;
}

static int lc_ms_plan_append(lc_mutation_plan *plan,
                             const lc_mutation *mutation, lc_error *error) {
  lc_mutation *next_items;

  next_items = (lc_mutation *)realloc(plan->items, (plan->count + 1U) *
                                                       sizeof(*plan->items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate mutation plan", NULL, NULL, NULL);
  }
  plan->items = next_items;
  plan->items[plan->count] = *mutation;
  plan->count += 1U;
  return LC_OK;
}

static lonejson_status lc_ms_emit_number(lonejson_writer *writer, double number,
                                         lonejson_error *error) {
  if (!isfinite(number)) {
    if (error != NULL) {
      lonejson_error_init(error);
      error->code = LONEJSON_STATUS_INVALID_JSON;
      snprintf(error->message, sizeof(error->message),
               "mutation produced non-finite number");
    }
    return LONEJSON_STATUS_INVALID_JSON;
  }
  return writer->f64(writer, number, error);
}

static lonejson_status lc_ms_emit_mutation_value(lonejson_writer *writer,
                                                 const lc_mutation *mutation,
                                                 double existing_value,
                                                 int has_existing,
                                                 lonejson_error *error) {
  lc_mutation_file_mode mode;

  switch (mutation->kind) {
  case LC_MUTATION_REMOVE:
    return writer->null_fn(writer, error);
  case LC_MUTATION_INCREMENT:
    if (has_existing) {
      return lc_ms_emit_number(writer, existing_value + mutation->delta, error);
    }
    return lc_ms_emit_number(writer, mutation->delta, error);
  case LC_MUTATION_SET:
    switch (mutation->value.kind) {
    case LC_MUTATION_VALUE_NULL:
      return writer->null_fn(writer, error);
    case LC_MUTATION_VALUE_BOOL:
      return writer->bool_fn(writer, mutation->value.bool_value, error);
    case LC_MUTATION_VALUE_LONG:
      return writer->i64(writer, mutation->value.long_value, error);
    case LC_MUTATION_VALUE_DOUBLE:
      return lc_ms_emit_number(writer, mutation->value.double_value, error);
    case LC_MUTATION_VALUE_STRING:
      return writer->string(writer, mutation->value.string_value,
                            strlen(mutation->value.string_value), error);
    case LC_MUTATION_VALUE_FILE:
      mode = mutation->value.file_mode;
      return lc_ms_emit_file_mutation_value(writer, &mutation->value, mode,
                                            error);
    default:
      if (error != NULL) {
        lonejson_error_init(error);
        error->code = LONEJSON_STATUS_INVALID_ARGUMENT;
        snprintf(error->message, sizeof(error->message),
                 "unsupported mutation value type");
      }
      return LONEJSON_STATUS_INVALID_ARGUMENT;
    }
  default:
    if (error != NULL) {
      lonejson_error_init(error);
      error->code = LONEJSON_STATUS_INVALID_ARGUMENT;
      snprintf(error->message, sizeof(error->message),
               "unsupported mutation kind");
    }
    return LONEJSON_STATUS_INVALID_ARGUMENT;
  }
}

static lonejson_status
lc_ms_rewrite_replace(lonejson_writer *writer,
                      const lonejson_value_rewrite_old_value *old_value,
                      void *user, lonejson_error *error) {
  lc_ms_rewrite_context *ctx;
  const lc_mutation *mutation;
  char *number_text;
  char *endptr;
  double existing;
  lonejson_status status;

  ctx = (lc_ms_rewrite_context *)user;
  mutation = ctx != NULL ? ctx->mutation : NULL;
  if (mutation == NULL || old_value == NULL) {
    if (error != NULL) {
      lonejson_error_init(error);
      error->code = LONEJSON_STATUS_INVALID_ARGUMENT;
      snprintf(error->message, sizeof(error->message),
               "invalid local mutate rewrite callback");
    }
    return LONEJSON_STATUS_INVALID_ARGUMENT;
  }
  if (mutation->kind != LC_MUTATION_INCREMENT) {
    return lc_ms_emit_mutation_value(writer, mutation, 0.0, 0, error);
  }
  if (!old_value->present) {
    return lc_ms_emit_mutation_value(writer, mutation, 0.0, 0, error);
  }
  if (old_value->type != LONEJSON_VALUE_NUMBER || old_value->number == NULL) {
    if (error != NULL) {
      lonejson_error_init(error);
      error->code = LONEJSON_STATUS_TYPE_MISMATCH;
      snprintf(error->message, sizeof(error->message),
               "increment target is not numeric");
    }
    return LONEJSON_STATUS_TYPE_MISMATCH;
  }
  number_text = (char *)malloc(old_value->number_len + 1U);
  if (number_text == NULL) {
    if (error != NULL) {
      lonejson_error_init(error);
      error->code = LONEJSON_STATUS_ALLOCATION_FAILED;
      snprintf(error->message, sizeof(error->message),
               "failed to allocate local mutate number buffer");
    }
    return LONEJSON_STATUS_ALLOCATION_FAILED;
  }
  memcpy(number_text, old_value->number, old_value->number_len);
  number_text[old_value->number_len] = '\0';
  errno = 0;
  existing = strtod(number_text, &endptr);
  if (errno != 0 || endptr == number_text || *endptr != '\0') {
    free(number_text);
    if (error != NULL) {
      lonejson_error_init(error);
      error->code = LONEJSON_STATUS_TYPE_MISMATCH;
      snprintf(error->message, sizeof(error->message),
               "increment target is not numeric");
    }
    return LONEJSON_STATUS_TYPE_MISMATCH;
  }
  free(number_text);
  status = lc_ms_emit_mutation_value(writer, mutation, existing, 1, error);
  return status;
}

static int lc_ms_rewrite_apply_single(const lc_mutation *mutation, FILE *input,
                                      FILE *output, lc_error *error) {
  lc_ms_rewrite_context ctx;
  lonejson *runtime;
  lonejson_value_rewrite_options options;
  lonejson_error lj_error;
  lonejson_status status;

  memset(&ctx, 0, sizeof(ctx));
  memset(&options, 0, sizeof(options));
  lonejson_error_init(&lj_error);
  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize lonejson runtime", NULL, NULL,
                        NULL);
  }
  ctx.mutation = mutation;
  options.target_segments = (const char *const *)mutation->path_segments;
  options.target_segment_count = mutation->path_segment_count;
  if (mutation->kind == LC_MUTATION_REMOVE) {
    options.action = LONEJSON_VALUE_REWRITE_DROP;
  } else {
    options.action = LONEJSON_VALUE_REWRITE_REPLACE_WITH;
    options.replace = lc_ms_rewrite_replace;
    options.replace_user = &ctx;
  }
  status = runtime->value_rewrite_filep(
      runtime, input, lc_ms_lonejson_file_sink, output, &options, &lj_error);
  if (status != LONEJSON_STATUS_OK) {
    if (mutation->kind == LC_MUTATION_REMOVE &&
        strcmp(lj_error.message, "value rewrite target was not found") == 0) {
      return lc_ms_copy_file(input, output, error);
    }
    if (strcmp(lj_error.message,
               "value rewrite cannot synthesize a missing array index") == 0) {
      return lc_ms_set_error(error, "missing array paths are not supported",
                             NULL);
    }
    return lc_ms_lonejson_rewrite_error(
        error, status, &lj_error, "failed to transform local mutate document");
  }
  if (fflush(output) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to flush mutate stream output", strerror(errno),
                        NULL, NULL);
  }
  return LC_OK;
}

static int lc_ms_split_path(const char *path, char ***out_segments,
                            size_t *out_count, lc_error *error) {
  const char *cursor;
  const char *segment_start;
  char **segments;
  size_t count;
  char *segment;
  size_t length;
  size_t i;
  char decoded[PATH_MAX];
  size_t decoded_length;

  if (path == NULL || path[0] != '/') {
    return lc_ms_set_error(error, "mutation path must start with '/'", path);
  }
  cursor = path + 1;
  segments = NULL;
  count = 0U;
  while (1) {
    segment_start = cursor;
    while (*cursor != '\0' && *cursor != '/') {
      cursor += 1;
    }
    length = (size_t)(cursor - segment_start);
    decoded_length = 0U;
    for (i = 0U; i < length; ++i) {
      if (segment_start[i] == '~' && i + 1U < length) {
        if (segment_start[i + 1U] == '0') {
          decoded[decoded_length++] = '~';
          i += 1U;
          continue;
        }
        if (segment_start[i + 1U] == '1') {
          decoded[decoded_length++] = '/';
          i += 1U;
          continue;
        }
      }
      decoded[decoded_length++] = segment_start[i];
    }
    decoded[decoded_length] = '\0';
    if (decoded_length == 0U) {
      while (count > 0U) {
        free(segments[--count]);
      }
      free(segments);
      return lc_ms_set_error(error, "mutation path refers to document root",
                             path);
    }
    if (strcmp(decoded, "*") == 0 || strcmp(decoded, "[]") == 0 ||
        strcmp(decoded, "**") == 0 || strcmp(decoded, "...") == 0) {
      while (count > 0U) {
        free(segments[--count]);
      }
      free(segments);
      return lc_ms_set_error(error,
                             "local mutate does not support wildcard LQL "
                             "paths; use direct JSON pointer paths",
                             decoded);
    }
    segment = lc_ms_strdup_range(decoded, decoded_length);
    if (segment == NULL || !lc_ms_append_ptr(&segments, &count, segment)) {
      free(segment);
      while (count > 0U) {
        free(segments[--count]);
      }
      free(segments);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate mutation path", NULL, NULL, NULL);
    }
    if (*cursor == '\0') {
      break;
    }
    cursor += 1;
  }
  *out_segments = segments;
  *out_count = count;
  return LC_OK;
}

static int lc_ms_split_expressions(const char *text, char ***out_parts,
                                   size_t *out_count, lc_error *error) {
  const char *cursor;
  const char *part_start;
  int depth;
  int in_single;
  int in_double;
  char **parts;
  size_t count;
  char *part;
  size_t length;
  const char *start;
  const char *end;

  parts = NULL;
  count = 0U;
  cursor = text;
  part_start = text;
  depth = 0;
  in_single = 0;
  in_double = 0;
  while (*cursor != '\0') {
    if (*cursor == '\\') {
      if (cursor[1] != '\0') {
        cursor += 2;
        continue;
      }
    }
    if (!in_double && *cursor == '\'') {
      in_single = !in_single;
    } else if (!in_single && *cursor == '"') {
      in_double = !in_double;
    } else if (!in_single && !in_double) {
      if (*cursor == '{') {
        depth += 1;
      } else if (*cursor == '}') {
        depth -= 1;
      } else if (depth == 0 && (*cursor == ',' || *cursor == '\n')) {
        start = part_start;
        end = cursor;
        while (start < end && isspace((unsigned char)*start)) {
          start += 1;
        }
        while (end > start && isspace((unsigned char)end[-1])) {
          end -= 1;
        }
        length = (size_t)(end - start);
        if (length != 0U) {
          part = lc_ms_strdup_range(start, length);
          if (part == NULL || !lc_ms_append_ptr(&parts, &count, part)) {
            free(part);
            while (count > 0U) {
              free(parts[--count]);
            }
            free(parts);
            return lc_error_set(error, LC_ERR_NOMEM, 0L,
                                "failed to allocate brace mutation", NULL, NULL,
                                NULL);
          }
        }
        part_start = cursor + 1;
      }
    }
    cursor += 1;
  }
  start = part_start;
  end = cursor;
  while (start < end && isspace((unsigned char)*start)) {
    start += 1;
  }
  while (end > start && isspace((unsigned char)end[-1])) {
    end -= 1;
  }
  length = (size_t)(end - start);
  if (length != 0U) {
    part = lc_ms_strdup_range(start, length);
    if (part == NULL || !lc_ms_append_ptr(&parts, &count, part)) {
      free(part);
      while (count > 0U) {
        free(parts[--count]);
      }
      free(parts);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate brace mutation", NULL, NULL,
                          NULL);
    }
  }
  *out_parts = parts;
  *out_count = count;
  return LC_OK;
}

static int lc_ms_resolve_home_path(const char *path, char **out,
                                   lc_error *error) {
  const char *home;
  struct passwd *pwd;
  char *resolved;
  size_t home_len;
  size_t path_len;

  if (strcmp(path, "~") == 0 || strncmp(path, "~/", 2) == 0) {
    home = getenv("HOME");
    if (home == NULL || home[0] == '\0') {
      pwd = getpwuid(getuid());
      if (pwd != NULL) {
        home = pwd->pw_dir;
      }
    }
    if (home == NULL || home[0] == '\0') {
      return lc_ms_set_error(
          error, "failed to resolve home directory for file-backed mutation",
          path);
    }
    home_len = strlen(home);
    path_len = strcmp(path, "~") == 0 ? 0U : strlen(path + 2);
    resolved = (char *)malloc(home_len + 1U + path_len + 1U);
    if (resolved == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate resolved file path", NULL, NULL,
                          NULL);
    }
    memcpy(resolved, home, home_len);
    if (path_len != 0U) {
      resolved[home_len] = '/';
      memcpy(resolved + home_len + 1U, path + 2, path_len);
      resolved[home_len + 1U + path_len] = '\0';
    } else {
      resolved[home_len] = '\0';
    }
    *out = resolved;
    return LC_OK;
  }
  *out = lc_strdup_local(path);
  if (*out == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate file path",
                        NULL, NULL, NULL);
  }
  return LC_OK;
}

static int lc_ms_resolve_file_path(const char *raw_path, const char *base_dir,
                                   char **out, lc_error *error) {
  char *path;
  char *resolved;
  size_t path_len;
  size_t base_len;

  path = lc_strdup_local(raw_path);
  if (path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate file-backed mutation path", NULL,
                        NULL, NULL);
  }
  path_len = strlen(path);
  if (path_len >= 2U && ((path[0] == '"' && path[path_len - 1U] == '"') ||
                         (path[0] == '\'' && path[path_len - 1U] == '\''))) {
    memmove(path, path + 1, path_len - 2U);
    path[path_len - 2U] = '\0';
  }
  if (path[0] == '\0') {
    free(path);
    return lc_ms_set_error(error, "file-backed mutation missing file path",
                           NULL);
  }
  if (lc_ms_resolve_home_path(path, &resolved, error) != LC_OK) {
    free(path);
    return LC_ERR_INVALID;
  }
  free(path);
  if (resolved[0] == '/') {
    *out = resolved;
    return LC_OK;
  }
  if (base_dir == NULL || base_dir[0] == '\0') {
    free(resolved);
    return lc_ms_set_error(
        error,
        "relative file-backed mutation path requires file value base dir",
        raw_path);
  }
  base_len = strlen(base_dir);
  path_len = strlen(resolved);
  path = (char *)malloc(base_len + 1U + path_len + 1U);
  if (path == NULL) {
    free(resolved);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate resolved file path", NULL, NULL,
                        NULL);
  }
  memcpy(path, base_dir, base_len);
  path[base_len] = '/';
  memcpy(path + base_len + 1U, resolved, path_len + 1U);
  free(resolved);
  *out = path;
  return LC_OK;
}

static int lc_ms_build_increment(const char *path_text, double delta,
                                 lc_mutation *mutation, lc_error *error) {
  if (delta == 0.0) {
    return lc_ms_set_error(error, "increment mutation requires non-zero delta",
                           path_text);
  }
  mutation->kind = LC_MUTATION_INCREMENT;
  mutation->delta = delta;
  return lc_ms_split_path(path_text, &mutation->path_segments,
                          &mutation->path_segment_count, error);
}

static int lc_ms_is_leap_year(int year) {
  return ((year % 4) == 0 && (year % 100) != 0) || ((year % 400) == 0);
}

static int lc_ms_days_in_month(int year, int month) {
  static const int days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

  if (month == 2 && lc_ms_is_leap_year(year)) {
    return 29;
  }
  return days[month - 1];
}

static int lc_ms_parse_fixed_digits(const char *text, size_t offset,
                                    size_t digits, int *out) {
  size_t i;
  int value;

  value = 0;
  for (i = 0U; i < digits; ++i) {
    if (!isdigit((unsigned char)text[offset + i])) {
      return 0;
    }
    value = (value * 10) + (text[offset + i] - '0');
  }
  *out = value;
  return 1;
}

static int lc_ms_parse_time_literal(const char *text, struct timespec *out,
                                    lc_error *error) {
  size_t length;
  int year;
  int month;
  int day;
  int hour;
  int minute;
  int second;
  int offset_hour;
  int offset_minute;
  int offset_sign;
  int offset_seconds;
  long nanoseconds;
  size_t pos;
  size_t digits;
  struct tm tm_value;
  time_t epoch;

  if (text == NULL || out == NULL) {
    return lc_ms_set_error(error, "invalid time literal", NULL);
  }
  length = strlen(text);
  if (length < 20U) {
    return lc_ms_set_error(error, "invalid RFC3339 time literal", text);
  }
  if (!lc_ms_parse_fixed_digits(text, 0U, 4U, &year) || text[4] != '-' ||
      !lc_ms_parse_fixed_digits(text, 5U, 2U, &month) || text[7] != '-' ||
      !lc_ms_parse_fixed_digits(text, 8U, 2U, &day) ||
      (text[10] != 'T' && text[10] != 't') ||
      !lc_ms_parse_fixed_digits(text, 11U, 2U, &hour) || text[13] != ':' ||
      !lc_ms_parse_fixed_digits(text, 14U, 2U, &minute) || text[16] != ':' ||
      !lc_ms_parse_fixed_digits(text, 17U, 2U, &second)) {
    return lc_ms_set_error(error, "invalid RFC3339 time literal", text);
  }
  if (month < 1 || month > 12 || day < 1 ||
      day > lc_ms_days_in_month(year, month) || hour < 0 || hour > 23 ||
      minute < 0 || minute > 59 || second < 0 || second > 59) {
    return lc_ms_set_error(error, "invalid RFC3339 time literal", text);
  }

  nanoseconds = 0L;
  pos = 19U;
  if (pos < length && text[pos] == '.') {
    pos += 1U;
    digits = 0U;
    while (pos + digits < length &&
           isdigit((unsigned char)text[pos + digits])) {
      if (digits < 9U) {
        nanoseconds = (nanoseconds * 10L) + (long)(text[pos + digits] - '0');
      }
      digits += 1U;
    }
    if (digits == 0U || digits > 9U) {
      return lc_ms_set_error(error, "invalid RFC3339Nano time literal", text);
    }
    while (digits < 9U) {
      nanoseconds *= 10L;
      digits += 1U;
    }
    pos += digits;
  }

  offset_seconds = 0;
  if (pos == length - 1U && (text[pos] == 'Z' || text[pos] == 'z')) {
    pos += 1U;
  } else {
    if (pos + 6U != length || (text[pos] != '+' && text[pos] != '-') ||
        !lc_ms_parse_fixed_digits(text, pos + 1U, 2U, &offset_hour) ||
        text[pos + 3U] != ':' ||
        !lc_ms_parse_fixed_digits(text, pos + 4U, 2U, &offset_minute)) {
      return lc_ms_set_error(error, "invalid RFC3339 time literal", text);
    }
    if (offset_hour > 23 || offset_minute > 59) {
      return lc_ms_set_error(error, "invalid RFC3339 time literal", text);
    }
    offset_sign = text[pos] == '-' ? -1 : 1;
    offset_seconds = offset_sign * ((offset_hour * 60 + offset_minute) * 60);
    pos += 6U;
  }
  if (pos != length) {
    return lc_ms_set_error(error, "invalid RFC3339 time literal", text);
  }

  memset(&tm_value, 0, sizeof(tm_value));
  tm_value.tm_year = year - 1900;
  tm_value.tm_mon = month - 1;
  tm_value.tm_mday = day;
  tm_value.tm_hour = hour;
  tm_value.tm_min = minute;
  tm_value.tm_sec = second;
  epoch = timegm(&tm_value);
  if (epoch == (time_t)-1 &&
      !(year == 1969 && month == 12 && day == 31 && hour == 23 &&
        minute == 59 && second == 59 && offset_seconds == 0)) {
    return lc_ms_set_error(error, "failed to normalize RFC3339 time literal",
                           text);
  }
  out->tv_sec = epoch - (time_t)offset_seconds;
  out->tv_nsec = nanoseconds;
  return LC_OK;
}

static int lc_ms_format_time_literal(const struct timespec *ts, char **out_text,
                                     lc_error *error) {
  struct tm tm_value;
  char buffer[64];
  char fraction[10];
  size_t fraction_len;
  int length;
  char *text;

  if (ts == NULL || out_text == NULL) {
    return lc_ms_set_error(error, "invalid timestamp value", NULL);
  }
  if (ts->tv_nsec < 0L || ts->tv_nsec >= 1000000000L) {
    return lc_ms_set_error(error, "invalid timestamp value", NULL);
  }
  if (gmtime_r(&ts->tv_sec, &tm_value) == NULL) {
    return lc_ms_set_error(error, "failed to normalize timestamp value", NULL);
  }

  length =
      snprintf(buffer, sizeof(buffer), "%04d-%02d-%02dT%02d:%02d:%02d",
               tm_value.tm_year + 1900, tm_value.tm_mon + 1, tm_value.tm_mday,
               tm_value.tm_hour, tm_value.tm_min, tm_value.tm_sec);
  if (length < 0 || (size_t)length >= sizeof(buffer)) {
    return lc_ms_set_error(error, "failed to format timestamp value", NULL);
  }
  if (ts->tv_nsec != 0L) {
    snprintf(fraction, sizeof(fraction), "%09ld", ts->tv_nsec);
    fraction_len = 9U;
    while (fraction_len != 0U && fraction[fraction_len - 1U] == '0') {
      fraction_len -= 1U;
    }
    buffer[length++] = '.';
    memcpy(buffer + length, fraction, fraction_len);
    length += (int)fraction_len;
  }
  buffer[length++] = 'Z';
  buffer[length] = '\0';
  text = lc_strdup_local(buffer);
  if (text == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate timestamp literal", NULL, NULL,
                        NULL);
  }
  *out_text = text;
  return LC_OK;
}

static int lc_ms_parse_literal_value(const char *literal, int time_mode,
                                     const lc_mutation_parse_options *options,
                                     lc_mutation_value *value,
                                     lc_error *error) {
  char *text;
  char *endptr;
  double dbl;
  lc_i64 lng;
  struct timespec now_value;
  struct timespec parsed_time;
  size_t length;
  int rc;

  text = lc_strdup_local(literal);
  if (text == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate mutation literal", NULL, NULL,
                        NULL);
  }
  while (*text != '\0' && isspace((unsigned char)*text)) {
    memmove(text, text + 1, strlen(text));
  }
  length = strlen(text);
  while (length != 0U && isspace((unsigned char)text[length - 1U])) {
    text[--length] = '\0';
  }
  if (length >= 2U && ((text[0] == '"' && text[length - 1U] == '"') ||
                       (text[0] == '\'' && text[length - 1U] == '\''))) {
    memmove(text, text + 1, length - 2U);
    text[length - 2U] = '\0';
  }
  if (time_mode) {
    if (strcmp(text, "NOW") == 0 || strcmp(text, "now") == 0) {
      memset(&now_value, 0, sizeof(now_value));
      if (options != NULL && options->has_now) {
        now_value = options->now;
      } else if (clock_gettime(CLOCK_REALTIME, &now_value) != 0) {
        now_value.tv_sec = time(NULL);
        now_value.tv_nsec = 0L;
      }
      free(text);
      value->kind = LC_MUTATION_VALUE_STRING;
      return lc_ms_format_time_literal(&now_value, &value->string_value, error);
    }
    rc = lc_ms_parse_time_literal(text, &parsed_time, error);
    free(text);
    if (rc != LC_OK) {
      return rc;
    }
    value->kind = LC_MUTATION_VALUE_STRING;
    return lc_ms_format_time_literal(&parsed_time, &value->string_value, error);
  }
  if (strcasecmp(text, "true") == 0) {
    free(text);
    value->kind = LC_MUTATION_VALUE_BOOL;
    value->bool_value = 1;
    return LC_OK;
  }
  if (strcasecmp(text, "false") == 0) {
    free(text);
    value->kind = LC_MUTATION_VALUE_BOOL;
    value->bool_value = 0;
    return LC_OK;
  }
  if (strcasecmp(text, "null") == 0) {
    free(text);
    value->kind = LC_MUTATION_VALUE_NULL;
    return LC_OK;
  }
  if (lc_i64_parse_base10(text, &lng)) {
    free(text);
    value->kind = LC_MUTATION_VALUE_LONG;
    value->long_value = lng;
    return LC_OK;
  }
  errno = 0;
  dbl = strtod(text, &endptr);
  if (errno == 0 && endptr != text && *endptr == '\0') {
    free(text);
    value->kind = LC_MUTATION_VALUE_DOUBLE;
    value->double_value = dbl;
    return LC_OK;
  }
  value->kind = LC_MUTATION_VALUE_STRING;
  value->string_value = text;
  return LC_OK;
}

static int lc_ms_build_set(const char *path_text, const char *value_text,
                           int time_mode, lc_mutation_file_mode file_mode,
                           const lc_mutation_parse_options *options,
                           lc_mutation *mutation, lc_error *error) {
  int rc;

  mutation->kind = LC_MUTATION_SET;
  rc = lc_ms_split_path(path_text, &mutation->path_segments,
                        &mutation->path_segment_count, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (file_mode != 0) {
    if (options == NULL) {
      return lc_ms_set_error(error, "file-backed mutations are disabled", NULL);
    }
    mutation->value.kind = LC_MUTATION_VALUE_FILE;
    mutation->value.file_mode = file_mode;
    mutation->value.file_value_resolver = options->file_value_resolver;
    return lc_ms_resolve_file_path(value_text, options->file_value_base_dir,
                                   &mutation->value.file_path, error);
  }
  return lc_ms_parse_literal_value(value_text, time_mode, options,
                                   &mutation->value, error);
}

typedef struct lc_ms_pending {
  struct lc_ms_pending *next;
  char expression[1];
} lc_ms_pending;

static int lc_ms_parse_expr_one(const char *expr,
                                const lc_mutation_parse_options *options,
                                lc_mutation_plan *plan, lc_ms_pending **pending,
                                lc_error *error) {
  const char *cursor;
  const char *path_text;
  const char *value_text;
  lc_mutation_file_mode file_mode;
  int remove_mode;
  int time_mode;
  lc_mutation mutation;
  char **subparts = NULL;
  size_t subcount = 0U;
  size_t i;
  char *brace_text;
  double delta;
  char *copy;
  char *eq;
  size_t cursor_len;
  int rc;

  memset(&mutation, 0, sizeof(mutation));
  file_mode = 0;
  remove_mode = 0;
  time_mode = 0;
  cursor = expr;
  if (strncmp(cursor, "file:", 5) == 0) {
    file_mode = LC_MUTATION_FILE_AUTO;
    cursor += 5;
  } else if (strncmp(cursor, "textfile:", 9) == 0) {
    file_mode = LC_MUTATION_FILE_TEXT;
    cursor += 9;
  } else if (strncmp(cursor, "base64file:", 11) == 0) {
    file_mode = LC_MUTATION_FILE_BASE64;
    cursor += 11;
  }
  if (strncmp(cursor, "rm:", 3) == 0) {
    remove_mode = 1;
    cursor += 3;
  } else if (strncmp(cursor, "remove:", 7) == 0) {
    remove_mode = 1;
    cursor += 7;
  } else if (strncmp(cursor, "delete:", 7) == 0) {
    remove_mode = 1;
    cursor += 7;
  } else if (strncmp(cursor, "del:", 4) == 0) {
    remove_mode = 1;
    cursor += 4;
  }
  if (strncmp(cursor, "time:", 5) == 0) {
    if (file_mode != 0) {
      return lc_ms_set_error(
          error, "file-backed mutation cannot be combined with time:", expr);
    }
    if (remove_mode) {
      return lc_ms_set_error(
          error, "time-prefixed mutation cannot be combined with remove", expr);
    }
    time_mode = 1;
    cursor += 5;
  }
  if (remove_mode) {
    mutation.kind = LC_MUTATION_REMOVE;
    if (lc_ms_split_path(cursor, &mutation.path_segments,
                         &mutation.path_segment_count, error) != LC_OK) {
      return LC_ERR_INVALID;
    }
    rc = lc_ms_plan_append(plan, &mutation, error);
    if (rc != LC_OK) {
      lc_ms_mutation_cleanup(&mutation);
      return rc;
    }
    return LC_OK;
  }
  cursor_len = strlen(cursor);
  if (cursor_len >= 2U && strcmp(cursor + cursor_len - 2U, "++") == 0) {
    copy = lc_ms_strdup_range(cursor, cursor_len - 2U);
    if (copy == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate increment mutation", NULL, NULL,
                          NULL);
    }
    if (lc_ms_build_increment(copy, 1.0, &mutation, error) != LC_OK) {
      free(copy);
      return LC_ERR_INVALID;
    }
    free(copy);
    rc = lc_ms_plan_append(plan, &mutation, error);
    if (rc != LC_OK) {
      lc_ms_mutation_cleanup(&mutation);
      return rc;
    }
    return LC_OK;
  }
  if (cursor_len >= 2U && strcmp(cursor + cursor_len - 2U, "--") == 0) {
    copy = lc_ms_strdup_range(cursor, cursor_len - 2U);
    if (copy == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate increment mutation", NULL, NULL,
                          NULL);
    }
    if (lc_ms_build_increment(copy, -1.0, &mutation, error) != LC_OK) {
      free(copy);
      return LC_ERR_INVALID;
    }
    free(copy);
    rc = lc_ms_plan_append(plan, &mutation, error);
    if (rc != LC_OK) {
      lc_ms_mutation_cleanup(&mutation);
      return rc;
    }
    return LC_OK;
  }
  if (cursor_len > 0U && cursor[cursor_len - 1U] == '}') {
    const char *brace = strchr(cursor, '{');
    if (brace != NULL && brace > cursor) {
      brace_text = lc_ms_strdup_range(
          brace + 1, (size_t)(cursor + cursor_len - 1U - (brace + 1)));
      if (brace_text == NULL) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate brace mutation", NULL, NULL,
                            NULL);
      }
      if (lc_ms_split_expressions(brace_text, &subparts, &subcount, error) !=
          LC_OK) {
        free(brace_text);
        return LC_ERR_INVALID;
      }
      free(brace_text);
      copy = lc_ms_strdup_range(cursor, (size_t)(brace - cursor));
      if (copy == NULL) {
        for (i = 0U; i < subcount; ++i)
          free(subparts[i]);
        free(subparts);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate brace prefix", NULL, NULL,
                            NULL);
      }
      /* Push in reverse so the iterative expansion retains depth-first order.
       */
      for (i = subcount; i > 0U; --i) {
        lc_ms_pending *task;
        size_t prefix_len = strlen(copy);
        size_t suffix_len = strlen(subparts[i - 1U]);
        if (prefix_len > (size_t)-1 - sizeof(*task) ||
            suffix_len > (size_t)-1 - sizeof(*task) - prefix_len) {
          task = NULL;
        } else {
          task = lc_alloc_with_allocator(NULL, sizeof(*task) + prefix_len +
                                                   suffix_len);
        }
        if (task == NULL) {
          size_t j;
          free(copy);
          for (j = 0U; j < subcount; ++j)
            free(subparts[j]);
          free(subparts);
          return lc_error_set(error, LC_ERR_NOMEM, 0L,
                              "failed to allocate brace mutation", NULL, NULL,
                              NULL);
        }
        memcpy(task->expression, copy, prefix_len);
        memcpy(task->expression + prefix_len, subparts[i - 1U],
               suffix_len + 1U);
        task->next = *pending;
        *pending = task;
      }
      for (i = 0U; i < subcount; ++i)
        free(subparts[i]);
      free(subparts);
      free(copy);
      return LC_OK;
    }
  }
  copy = lc_strdup_local(cursor);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate mutation expression", NULL, NULL,
                        NULL);
  }
  eq = strchr(copy, '=');
  if (eq == NULL) {
    free(copy);
    return lc_ms_set_error(error, "invalid mutation expression", expr);
  }
  *eq = '\0';
  path_text = copy;
  value_text = eq + 1;
  if (!time_mode && file_mode == 0) {
    char *delta_end;
    errno = 0;
    delta = strtod(value_text, &delta_end);
    if (errno == 0 && delta_end != value_text && *delta_end == '\0') {
      if (lc_ms_build_increment(path_text, delta, &mutation, error) != LC_OK) {
        lc_ms_mutation_cleanup(&mutation);
        free(copy);
        return LC_ERR_INVALID;
      }
      free(copy);
      rc = lc_ms_plan_append(plan, &mutation, error);
      if (rc != LC_OK) {
        lc_ms_mutation_cleanup(&mutation);
        return rc;
      }
      return LC_OK;
    }
  }
  if (lc_ms_build_set(path_text, value_text, time_mode, file_mode, options,
                      &mutation, error) != LC_OK) {
    lc_ms_mutation_cleanup(&mutation);
    free(copy);
    return LC_ERR_INVALID;
  }
  free(copy);
  rc = lc_ms_plan_append(plan, &mutation, error);
  if (rc != LC_OK) {
    lc_ms_mutation_cleanup(&mutation);
    return rc;
  }
  return LC_OK;
}

static int lc_ms_parse_expr(const char *expr,
                            const lc_mutation_parse_options *options,
                            lc_mutation_plan *plan, lc_error *error) {
  lc_ms_pending *pending = NULL;
  lc_ms_pending *task;
  int rc = lc_ms_parse_expr_one(expr, options, plan, &pending, error);
  while (rc == LC_OK && pending != NULL) {
    task = pending;
    pending = task->next;
    rc = lc_ms_parse_expr_one(task->expression, options, plan, &pending, error);
    lc_free_with_allocator(NULL, task);
    if (rc != LC_OK)
      rc = LC_ERR_INVALID;
  }
  while (pending != NULL) {
    task = pending;
    pending = task->next;
    lc_free_with_allocator(NULL, task);
  }
  return rc;
}

int lc_mutation_plan_build(const char *const *exprs, size_t expr_count,
                           const lc_mutation_parse_options *options,
                           lc_mutation_plan **out, lc_error *error) {
  lc_mutation_plan *plan;
  size_t i;
  int rc;

  if (exprs == NULL || expr_count == 0U || out == NULL) {
    return lc_ms_set_error(error, "local mutate requires mutation expressions",
                           NULL);
  }
  plan = (lc_mutation_plan *)calloc(1U, sizeof(*plan));
  if (plan == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate mutation plan", NULL, NULL, NULL);
  }
  for (i = 0U; i < expr_count; ++i) {
    if (exprs[i] == NULL || exprs[i][0] == '\0') {
      continue;
    }
    rc = lc_ms_parse_expr(exprs[i], options, plan, error);
    if (rc != LC_OK) {
      lc_mutation_plan_close(plan);
      return rc;
    }
  }
  if (plan->count == 0U) {
    lc_mutation_plan_close(plan);
    return lc_ms_set_error(error, "local mutate requires at least one mutation",
                           NULL);
  }
  *out = plan;
  return LC_OK;
}

int lc_mutation_plan_apply(const lc_mutation_plan *plan, FILE *input,
                           FILE **out_final, lc_error *error) {
  FILE *current;
  FILE *next;
  size_t i;
  int rc;

  if (plan == NULL || input == NULL || out_final == NULL) {
    return lc_ms_set_error(
        error, "mutation plan apply requires plan, input, and out_final", NULL);
  }
  current = input;
  rewind(current);
  for (i = 0U; i < plan->count; ++i) {
    next = tmpfile();
    if (next == NULL) {
      if (current != input) {
        fclose(current);
      }
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to create local mutate scratch file",
                          strerror(errno), NULL, NULL);
    }
    rewind(current);
    rc = lc_ms_rewrite_apply_single(&plan->items[i], current, next, error);
    if (rc != LC_OK) {
      fclose(next);
      if (current != input) {
        fclose(current);
      }
      return rc;
    }
    if (current != input) {
      fclose(current);
    }
    current = next;
  }
  rewind(current);
  *out_final = current;
  return LC_OK;
}
