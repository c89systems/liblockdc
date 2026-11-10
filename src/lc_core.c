#ifdef LOCKDC_TEST_SMALL_STACK
/* Keep the initializer in a required archive member, so independently built
 * Debug SDK consumers inherit the same test-only worker-stack policy. */
#include "../tests/support/lc_test_stack.c"
#endif

#include "lc/lc.h"
#include "lc_allocation_tracking.h"
#include "lc_engine_api.h"
#include "lc_intcompat.h"
#include "lc_log.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "lc_api_internal.h"

typedef struct lc_source_impl {
  lc_source pub;
  size_t (*read_impl)(struct lc_source_impl *self, void *buffer, size_t count,
                      lc_error *error);
  int (*reset_impl)(struct lc_source_impl *self, lc_error *error);
  void (*close_impl)(struct lc_source_impl *self);
} lc_source_impl;

typedef struct lc_sink_impl {
  lc_sink pub;
  int (*write_impl)(struct lc_sink_impl *self, const void *bytes, size_t count,
                    lc_error *error);
  void (*close_impl)(struct lc_sink_impl *self);
} lc_sink_impl;

typedef struct lc_memory_source {
  lc_source_impl base;
  unsigned char *bytes;
  size_t length;
  size_t offset;
  int owns_bytes;
} lc_memory_source;

typedef struct lc_file_source {
  lc_source_impl base;
  FILE *fp;
  int close_file;
} lc_file_source;

typedef struct lc_fd_source {
  lc_source_impl base;
  int fd;
} lc_fd_source;

typedef struct lc_callback_source {
  lc_source_impl base;
  lc_source_read_fn read;
  lc_source_reset_fn reset;
  lc_source_close_fn close;
  void *context;
} lc_callback_source;

typedef struct lc_bundle_capture_source {
  lc_source pub;
  lc_source *inner;
  const lc_allocator *allocator;
  unsigned char *bytes;
  size_t length;
  size_t capacity;
} lc_bundle_capture_source;

static int lc_endpoint_is_pouch(const char *endpoint) {
  return endpoint != NULL && strncmp(endpoint, "pouch://", 8U) == 0;
}

static const char *lc_pouch_endpoint_path(const char *endpoint) {
  return endpoint != NULL ? endpoint + 8 : NULL;
}

static void lc_pouch_endpoint_split(const char *endpoint, const char **path,
                                    size_t *path_len, const char **query,
                                    size_t *query_len, const char **fragment) {
  const char *base;
  const char *query_marker;
  const char *fragment_marker;
  const char *end;

  base = lc_pouch_endpoint_path(endpoint);
  if (base == NULL) {
    if (path != NULL) {
      *path = NULL;
    }
    if (path_len != NULL) {
      *path_len = 0U;
    }
    if (query != NULL) {
      *query = NULL;
    }
    if (query_len != NULL) {
      *query_len = 0U;
    }
    if (fragment != NULL) {
      *fragment = NULL;
    }
    return;
  }
  fragment_marker = base != NULL ? strchr(base, '#') : NULL;
  end = fragment_marker != NULL ? fragment_marker : base + strlen(base);
  query_marker = base != NULL ? strchr(base, '?') : NULL;
  if (query_marker != NULL && query_marker >= end) {
    query_marker = NULL;
  }
  if (path != NULL) {
    *path = base;
  }
  if (path_len != NULL) {
    *path_len = (size_t)((query_marker != NULL ? query_marker : end) - base);
  }
  if (query != NULL) {
    *query = query_marker != NULL ? query_marker + 1 : NULL;
  }
  if (query_len != NULL) {
    *query_len = query_marker != NULL ? (size_t)(end - query_marker - 1U) : 0U;
  }
  if (fragment != NULL) {
    *fragment = fragment_marker;
  }
}

typedef struct lc_fd_sink {
  lc_sink_impl base;
  int fd;
  int close_fd;
} lc_fd_sink;

typedef struct lc_discard_sink {
  lc_sink_impl base;
} lc_discard_sink;

static int lc_discard_sink_marker;

typedef struct lc_memory_sink {
  lc_sink_impl base;
  unsigned char *bytes;
  size_t length;
  size_t capacity;
} lc_memory_sink;

struct lc_stream_pipe {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  lc_allocator allocator;
  unsigned char *buffer;
  size_t capacity;
  size_t read_pos;
  size_t write_pos;
  size_t used;
  int writer_closed;
  int reader_closed;
  int reader_ref;
  int writer_ref;
  int error_code;
  char *error_message;
};

typedef struct lc_stream_source {
  lc_source_impl base;
  lc_allocator allocator;
  lc_stream_pipe *pipe;
} lc_stream_source;

void *lc_alloc_with_allocator(const lc_allocator *allocator, size_t size) {
  if (allocator != NULL && allocator->malloc_fn != NULL) {
    if (allocator->realloc_fn == NULL)
      return lc_sized_malloc(allocator->malloc_fn, allocator->context, size);
    return allocator->malloc_fn(allocator->context, size);
  }
  return lc_builtin_malloc(size);
}

void *lc_calloc_with_allocator(const lc_allocator *allocator, size_t count,
                               size_t size) {
  void *ptr;
  size_t total;

  if (count != 0U && size > ((size_t)-1) / count) {
    return NULL;
  }
  total = count * size;
  ptr = lc_alloc_with_allocator(allocator, total == 0U ? 1U : total);
  if (ptr != NULL) {
    memset(ptr, 0, total);
  }
  return ptr;
}

void *lc_realloc_with_allocator(const lc_allocator *allocator, void *ptr,
                                size_t size) {
  if (allocator != NULL) {
    if (allocator->realloc_fn != NULL)
      return allocator->realloc_fn(allocator->context, ptr, size);
    if (allocator->malloc_fn != NULL)
      return lc_sized_realloc(allocator->malloc_fn, allocator->free_fn,
                              allocator->context, ptr, size);
  }
  return lc_builtin_realloc(ptr, size);
}

void lc_free_with_allocator(const lc_allocator *allocator, void *ptr) {
  if (ptr == NULL)
    return;
  if (allocator != NULL && allocator->free_fn != NULL) {
    if (allocator->malloc_fn != NULL && allocator->realloc_fn == NULL)
      lc_sized_free(allocator->free_fn, allocator->context, ptr);
    else
      allocator->free_fn(allocator->context, ptr);
    return;
  }
  lc_builtin_free(ptr);
}

void lc_secret_wipe(void *ptr, size_t length) {
  volatile unsigned char *cursor;

  if (ptr == NULL) {
    return;
  }
  cursor = (volatile unsigned char *)ptr;
  while (length > 0U) {
    *cursor++ = 0U;
    --length;
  }
}

void lc_secret_free_string_with_allocator(const lc_allocator *allocator,
                                          char *value) {
  if (value == NULL) {
    return;
  }
  lc_secret_wipe(value, strlen(value));
  lc_free_with_allocator(allocator, value);
}

char *lc_strdup_with_allocator(const lc_allocator *allocator,
                               const char *value) {
  size_t length;
  char *copy;

  if (value == NULL) {
    return NULL;
  }
  length = strlen(value);
  copy = (char *)lc_alloc_with_allocator(allocator, length + 1U);
  if (copy == NULL) {
    return NULL;
  }
  memcpy(copy, value, length + 1U);
  return copy;
}

void *lc_client_alloc(lc_client_handle *client, size_t size) {
  return lc_alloc_with_allocator(client != NULL ? &client->allocator : NULL,
                                 size);
}

void *lc_client_calloc(lc_client_handle *client, size_t count, size_t size) {
  return lc_calloc_with_allocator(client != NULL ? &client->allocator : NULL,
                                  count, size);
}

void *lc_client_realloc(lc_client_handle *client, void *ptr, size_t size) {
  return lc_realloc_with_allocator(client != NULL ? &client->allocator : NULL,
                                   ptr, size);
}

void lc_client_free(lc_client_handle *client, void *ptr) {
  lc_free_with_allocator(client != NULL ? &client->allocator : NULL, ptr);
}

char *lc_client_strdup(lc_client_handle *client, const char *value) {
  return lc_strdup_with_allocator(client != NULL ? &client->allocator : NULL,
                                  value);
}

int lc_state_retention_validate(int selected, long seconds, const char *ns,
                                const char *key, lc_error *error) {
  if (!selected)
    return LC_OK;
  if (seconds < 0)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "state retention seconds must be nonnegative", NULL,
                        NULL, NULL);
  if ((ns != NULL &&
       (strcmp(ns, ".txns") == 0 || strcmp(ns, ".xa") == 0 ||
        strncmp(ns, ".xa/", 4) == 0 || strcmp(ns, ".lockd") == 0 ||
        strncmp(ns, ".lockd/", 7) == 0)) ||
      (key != NULL &&
       (strncmp(key, "__lockdc_io/", 12) == 0 || strncmp(key, "q/", 2) == 0 ||
        strncmp(key, "config/", 7) == 0 || strncmp(key, ".staging/", 9) == 0 ||
        strstr(key, "/.staging/") != NULL)))
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "state retention cannot target SDK lifecycle records",
                        NULL, NULL, NULL);
  return LC_OK;
}

void lc_lease_retention_observe(lc_lease *lease,
                                lc_state_retention_support support,
                                int has_expiry, lc_unix_seconds expiry) {
  lease->retention_support = support;
  lease->has_retention_expires_at_unix = has_expiry;
  lease->retention_expires_at_unix = has_expiry ? expiry : 0;
}

int lc_error_set(lc_error *error, int code, long http_status,
                 const char *message, const char *detail,
                 const char *server_code, const char *correlation_id) {
  char *message_copy;
  char *detail_copy;
  char *server_code_copy;
  char *correlation_id_copy;
  size_t length;

  if (error == NULL) {
    return code;
  }
  message_copy = NULL;
  detail_copy = NULL;
  server_code_copy = NULL;
  correlation_id_copy = NULL;
  if (message != NULL) {
    length = strlen(message);
    message_copy = (char *)malloc(length + 1U);
    if (message_copy != NULL) {
      memcpy(message_copy, message, length + 1U);
    }
  }
  if (detail != NULL) {
    length = strlen(detail);
    detail_copy = (char *)malloc(length + 1U);
    if (detail_copy != NULL) {
      memcpy(detail_copy, detail, length + 1U);
    }
  }
  if (server_code != NULL) {
    length = strlen(server_code);
    server_code_copy = (char *)malloc(length + 1U);
    if (server_code_copy != NULL) {
      memcpy(server_code_copy, server_code, length + 1U);
    }
  }
  if (correlation_id != NULL) {
    length = strlen(correlation_id);
    correlation_id_copy = (char *)malloc(length + 1U);
    if (correlation_id_copy != NULL) {
      memcpy(correlation_id_copy, correlation_id, length + 1U);
    }
  }
  lc_error_cleanup(error);
  error->code = code;
  error->http_status = http_status;
  error->message = message_copy;
  error->detail = detail_copy;
  error->server_code = server_code_copy;
  error->correlation_id = correlation_id_copy;
  return code;
}

int lc_error_from_engine(lc_error *error, lc_engine_error *engine) {
  int code;

  code = LC_ERR_SERVER;
  if (engine->code == LC_ENGINE_OK) {
    code = LC_OK;
  } else if (engine->code == LC_ENGINE_ERROR_INVALID_ARGUMENT) {
    code = LC_ERR_INVALID;
  } else if (engine->code == LC_ENGINE_ERROR_NO_MEMORY) {
    code = LC_ERR_NOMEM;
  } else if (engine->code == LC_ENGINE_ERROR_TRANSPORT) {
    code = LC_ERR_TRANSPORT;
  } else if (engine->code == LC_ENGINE_ERROR_PROTOCOL) {
    code = LC_ERR_PROTOCOL;
  }
  return lc_error_set(error, code, engine->http_status, engine->message,
                      engine->detail, engine->server_error_code,
                      engine->correlation_id);
}

void *lc_calloc_local(size_t count, size_t size) { return calloc(count, size); }

void *lc_realloc_local(void *ptr, size_t size) { return realloc(ptr, size); }

void lc_free_local(void *ptr) { free(ptr); }

char *lc_strdup_local(const char *value) {
  size_t length;
  char *copy;

  if (value == NULL) {
    return NULL;
  }
  length = strlen(value);
  copy = (char *)malloc(length + 1U);
  if (copy == NULL) {
    return NULL;
  }
  memcpy(copy, value, length + 1U);
  return copy;
}

char *lc_dup_bytes_as_text(const void *bytes, size_t length) {
  char *copy;

  copy = (char *)malloc(length + 1U);
  if (copy == NULL) {
    return NULL;
  }
  if (length > 0U) {
    memcpy(copy, bytes, length);
  }
  copy[length] = '\0';
  return copy;
}

void lc_attachment_info_copy(lc_attachment_info *dst,
                             const lc_engine_attachment_info *src) {
  memset(dst, 0, sizeof(*dst));
  if (src == NULL) {
    return;
  }
  dst->id = lc_strdup_local(src->id);
  dst->name = lc_strdup_local(src->name);
  dst->size = src->size;
  dst->plaintext_sha256 = lc_strdup_local(src->plaintext_sha256);
  dst->content_type = lc_strdup_local(src->content_type);
  dst->created_at_unix = src->created_at_unix;
  dst->updated_at_unix = src->updated_at_unix;
}

static size_t lc_source_pub_read(lc_source *self, void *buffer, size_t count,
                                 lc_error *error) {
  lc_source_impl *impl;

  impl = (lc_source_impl *)self;
  return impl->read_impl(impl, buffer, count, error);
}

static int lc_source_pub_reset(lc_source *self, lc_error *error) {
  lc_source_impl *impl;

  impl = (lc_source_impl *)self;
  if (impl->reset_impl == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, "source is not resettable",
                        NULL, NULL, NULL);
  }
  return impl->reset_impl(impl, error);
}

int lc_source_is_resettable(const lc_source *source) {
  const lc_source_impl *impl;

  if (source == NULL || source->reset == NULL) {
    return 0;
  }
  if (source->reset != lc_source_pub_reset) {
    return 1;
  }
  impl = (const lc_source_impl *)source;
  return impl->reset_impl != NULL;
}

static void lc_source_pub_close(lc_source *self) {
  lc_source_impl *impl;

  if (self == NULL) {
    return;
  }
  impl = (lc_source_impl *)self;
  if (impl->close_impl != NULL) {
    impl->close_impl(impl);
  }
}

static size_t lc_bundle_capture_source_read(lc_source *self, void *buffer,
                                            size_t count, lc_error *error) {
  lc_bundle_capture_source *capture;
  size_t nread;

  capture = (lc_bundle_capture_source *)self->impl;
  nread = capture->inner->read(capture->inner, buffer, count, error);
  if (nread == 0U) {
    return 0U;
  }
  if (nread > ((size_t)-1) - capture->length) {
    lc_error_set(error, LC_ERR_NOMEM, 0L, "client bundle source is too large",
                 NULL, NULL, NULL);
    return 0U;
  }
  if (capture->length + nread > capture->capacity) {
    size_t next_capacity;
    unsigned char *next_bytes;

    next_capacity = capture->capacity == 0U ? 8192U : capture->capacity;
    while (next_capacity < capture->length + nread) {
      if (next_capacity > ((size_t)-1) / 2U) {
        next_capacity = capture->length + nread;
        break;
      }
      next_capacity *= 2U;
    }
    next_bytes = (unsigned char *)lc_realloc_with_allocator(
        capture->allocator, capture->bytes, next_capacity);
    if (next_bytes == NULL) {
      lc_error_set(error, LC_ERR_NOMEM, 0L,
                   "failed to retain client bundle source", NULL, NULL, NULL);
      return 0U;
    }
    capture->bytes = next_bytes;
    capture->capacity = next_capacity;
  }
  memcpy(capture->bytes + capture->length, buffer, nread);
  capture->length += nread;
  return nread;
}

static int lc_bundle_capture_source_reset(lc_source *self, lc_error *error) {
  (void)self;
  return lc_error_set(error, LC_ERR_INVALID, 0L, "source is not resettable",
                      NULL, NULL, NULL);
}

static void lc_bundle_capture_source_close(lc_source *self) { (void)self; }

static int lc_sink_pub_write(lc_sink *self, const void *bytes, size_t count,
                             lc_error *error) {
  lc_sink_impl *impl;

  impl = (lc_sink_impl *)self;
  return impl->write_impl(impl, bytes, count, error);
}

static void lc_sink_pub_close(lc_sink *self) {
  lc_sink_impl *impl;

  if (self == NULL) {
    return;
  }
  impl = (lc_sink_impl *)self;
  if (impl->close_impl != NULL) {
    impl->close_impl(impl);
  }
}

static void lc_stream_pipe_release_locked(lc_stream_pipe *pipe) {
  int free_pipe;

  free_pipe = pipe->reader_ref == 0 && pipe->writer_ref == 0;
  pthread_mutex_unlock(&pipe->mutex);
  if (free_pipe) {
    pthread_cond_destroy(&pipe->cond);
    pthread_mutex_destroy(&pipe->mutex);
    lc_free_with_allocator(&pipe->allocator, pipe->buffer);
    lc_free_with_allocator(&pipe->allocator, pipe->error_message);
    lc_free_with_allocator(&pipe->allocator, pipe);
  }
}

static size_t lc_stream_source_read(lc_source_impl *self, void *buffer,
                                    size_t count, lc_error *error) {
  lc_stream_source *source;
  lc_stream_pipe *pipe;
  size_t first;
  size_t second;

  source = (lc_stream_source *)self;
  pipe = source->pipe;
  pthread_mutex_lock(&pipe->mutex);
  while (pipe->used == 0U && !pipe->writer_closed && !pipe->reader_closed) {
    pthread_cond_wait(&pipe->cond, &pipe->mutex);
  }
  if (pipe->reader_closed) {
    lc_stream_pipe_release_locked(pipe);
    return 0U;
  }
  if (pipe->used == 0U) {
    if (pipe->error_code != LC_OK) {
      lc_error_set(error, pipe->error_code, 0L,
                   pipe->error_message != NULL ? pipe->error_message
                                               : "stream delivery failed",
                   NULL, NULL, NULL);
    }
    lc_stream_pipe_release_locked(pipe);
    return 0U;
  }
  if (count > pipe->used) {
    count = pipe->used;
  }
  first = count;
  if (pipe->read_pos + first > pipe->capacity) {
    first = pipe->capacity - pipe->read_pos;
  }
  memcpy(buffer, pipe->buffer + pipe->read_pos, first);
  second = count - first;
  if (second > 0U) {
    memcpy((unsigned char *)buffer + first, pipe->buffer, second);
  }
  pipe->read_pos = (pipe->read_pos + count) % pipe->capacity;
  pipe->used -= count;
  pthread_cond_broadcast(&pipe->cond);
  lc_stream_pipe_release_locked(pipe);
  return count;
}

static void lc_stream_source_close(lc_source_impl *self) {
  lc_stream_source *source;
  lc_stream_pipe *pipe;

  source = (lc_stream_source *)self;
  pipe = source->pipe;
  pthread_mutex_lock(&pipe->mutex);
  pipe->reader_closed = 1;
  pipe->reader_ref = 0;
  pthread_cond_broadcast(&pipe->cond);
  lc_stream_pipe_release_locked(pipe);
  lc_free_with_allocator(&source->allocator, source);
}

static size_t lc_memory_source_read(lc_source_impl *base, void *buffer,
                                    size_t count, lc_error *error) {
  lc_memory_source *source;
  size_t available;
  size_t chunk;

  (void)error;
  source = (lc_memory_source *)base;
  if (source->offset >= source->length) {
    return 0U;
  }
  available = source->length - source->offset;
  chunk = count;
  if (chunk > available) {
    chunk = available;
  }
  memcpy(buffer, source->bytes + source->offset, chunk);
  source->offset += chunk;
  return chunk;
}

static int lc_memory_source_reset(lc_source_impl *base, lc_error *error) {
  lc_memory_source *source;

  (void)error;
  source = (lc_memory_source *)base;
  source->offset = 0U;
  return LC_OK;
}

static void lc_memory_source_close(lc_source_impl *base) {
  lc_memory_source *source;

  source = (lc_memory_source *)base;
  if (source->owns_bytes) {
    free(source->bytes);
  }
  free(source);
}

int lc_source_memory_view(const lc_source *source,
                          const unsigned char **bytes_out, size_t *length_out) {
  const lc_memory_source *memory;

  if (bytes_out != NULL) {
    *bytes_out = NULL;
  }
  if (length_out != NULL) {
    *length_out = 0U;
  }
  if (source == NULL || bytes_out == NULL || length_out == NULL ||
      source->read != lc_source_pub_read ||
      source->reset != lc_source_pub_reset ||
      source->close != lc_source_pub_close) {
    return 0;
  }
  memory = (const lc_memory_source *)source;
  if (memory->base.read_impl != lc_memory_source_read ||
      memory->base.reset_impl != lc_memory_source_reset ||
      memory->base.close_impl != lc_memory_source_close ||
      memory->offset > memory->length) {
    return 0;
  }
  *bytes_out = memory->bytes != NULL ? memory->bytes + memory->offset : NULL;
  *length_out = memory->length - memory->offset;
  return 1;
}

void lc_source_memory_consume(lc_source *source) {
  lc_memory_source *memory;

  if (source == NULL || source->read != lc_source_pub_read ||
      source->reset != lc_source_pub_reset ||
      source->close != lc_source_pub_close) {
    return;
  }
  memory = (lc_memory_source *)source;
  if (memory->base.read_impl == lc_memory_source_read &&
      memory->base.reset_impl == lc_memory_source_reset &&
      memory->base.close_impl == lc_memory_source_close &&
      memory->offset <= memory->length) {
    memory->offset = memory->length;
  }
}

static size_t lc_file_source_read(lc_source_impl *base, void *buffer,
                                  size_t count, lc_error *error) {
  lc_file_source *source;
  size_t nread;

  (void)error;
  source = (lc_file_source *)base;
  nread = fread(buffer, 1U, count, source->fp);
  return nread;
}

static int lc_file_source_reset(lc_source_impl *base, lc_error *error) {
  lc_file_source *source;

  source = (lc_file_source *)base;
  if (fseek(source->fp, 0L, SEEK_SET) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to rewind file source", NULL, NULL, NULL);
  }
  return LC_OK;
}

static void lc_file_source_close(lc_source_impl *base) {
  lc_file_source *source;

  source = (lc_file_source *)base;
  if (source->close_file && source->fp != NULL) {
    fclose(source->fp);
  }
  free(source);
}

static size_t lc_fd_source_read(lc_source_impl *base, void *buffer,
                                size_t count, lc_error *error) {
  lc_fd_source *source;
  ssize_t nread;

  source = (lc_fd_source *)base;
  nread = read(source->fd, buffer, count);
  if (nread < 0) {
    lc_error_set(error, LC_ERR_TRANSPORT, 0L, "failed to read from fd source",
                 NULL, NULL, NULL);
    return 0U;
  }
  return (size_t)nread;
}

static void lc_fd_source_close(lc_source_impl *base) {
  lc_fd_source *source;

  source = (lc_fd_source *)base;
  free(source);
}

static size_t lc_callback_source_read(lc_source_impl *base, void *buffer,
                                      size_t count, lc_error *error) {
  lc_callback_source *source;

  source = (lc_callback_source *)base;
  return source->read(source->context, buffer, count, error);
}

static int lc_callback_source_reset(lc_source_impl *base, lc_error *error) {
  lc_callback_source *source;

  source = (lc_callback_source *)base;
  if (source->reset == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, "source is not resettable",
                        NULL, NULL, NULL);
  }
  return source->reset(source->context, error);
}

static void lc_callback_source_close(lc_source_impl *base) {
  lc_callback_source *source;

  source = (lc_callback_source *)base;
  if (source->close != NULL) {
    source->close(source->context);
  }
  free(source);
}

static int lc_fd_sink_write(lc_sink_impl *base, const void *bytes, size_t count,
                            lc_error *error) {
  lc_fd_sink *sink;
  ssize_t nwritten;

  sink = (lc_fd_sink *)base;
  nwritten = write(sink->fd, bytes, count);
  if (nwritten < 0 || (size_t)nwritten != count) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to write to fd sink", NULL, NULL, NULL);
  }
  return 1;
}

static void lc_fd_sink_close(lc_sink_impl *base) {
  lc_fd_sink *sink;

  sink = (lc_fd_sink *)base;
  if (sink->close_fd) {
    close(sink->fd);
  }
  free(sink);
}

static int lc_discard_sink_write(lc_sink_impl *base, const void *bytes,
                                 size_t count, lc_error *error) {
  (void)base;
  (void)bytes;
  (void)count;
  (void)error;
  return 1;
}

static void lc_discard_sink_close(lc_sink_impl *base) { free(base); }

static int lc_memory_sink_write(lc_sink_impl *base, const void *bytes,
                                size_t count, lc_error *error) {
  lc_memory_sink *sink;
  unsigned char *next;
  size_t needed;
  size_t capacity;

  sink = (lc_memory_sink *)base;
  needed = sink->length + count;
  if (needed > sink->capacity) {
    capacity = sink->capacity == 0U ? 4096U : sink->capacity * 2U;
    while (capacity < needed) {
      capacity *= 2U;
    }
    next = (unsigned char *)realloc(sink->bytes, capacity);
    if (next == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to grow memory sink",
                          NULL, NULL, NULL);
    }
    sink->bytes = next;
    sink->capacity = capacity;
  }
  memcpy(sink->bytes + sink->length, bytes, count);
  sink->length += count;
  return 1;
}

static void lc_memory_sink_close(lc_sink_impl *base) {
  lc_memory_sink *sink;

  sink = (lc_memory_sink *)base;
  free(sink->bytes);
  free(sink);
}

size_t lc_engine_read_bridge(void *context, void *buffer, size_t count,
                             lc_engine_error *error) {
  lc_read_bridge *bridge;
  lc_error public_error;
  size_t nread;

  bridge = (lc_read_bridge *)context;
  lc_error_init(&public_error);
  nread = bridge->source->read(bridge->source, buffer, count, &public_error);
  if (public_error.code != LC_OK) {
    error->code = LC_ENGINE_ERROR_TRANSPORT;
    error->message = lc_strdup_local(public_error.message);
  }
  lc_error_cleanup(&public_error);
  return nread;
}

int lc_engine_reset_bridge(void *context, lc_engine_error *error) {
  lc_read_bridge *bridge;
  lc_error public_error;
  int rc;

  bridge = (lc_read_bridge *)context;
  if (bridge == NULL || bridge->source == NULL) {
    if (error != NULL) {
      if (error->message != NULL) {
        free(error->message);
        error->message = NULL;
      }
      error->code = LC_ENGINE_ERROR_INVALID_ARGUMENT;
      error->http_status = 0L;
      error->message = lc_strdup_local("payload reset requires source");
    }
    return LC_ENGINE_ERROR_INVALID_ARGUMENT;
  }
  if (bridge->source->reset == NULL) {
    if (error != NULL) {
      if (error->message != NULL) {
        free(error->message);
        error->message = NULL;
      }
      error->code = LC_ENGINE_ERROR_INVALID_ARGUMENT;
      error->http_status = 0L;
      error->message = lc_strdup_local("payload source is not rewindable");
    }
    return LC_ENGINE_ERROR_INVALID_ARGUMENT;
  }
  lc_error_init(&public_error);
  rc = bridge->source->reset(bridge->source, &public_error);
  if (rc != LC_OK && public_error.code != LC_OK) {
    error->code = LC_ENGINE_ERROR_TRANSPORT;
    error->message = lc_strdup_local(public_error.message);
  }
  lc_error_cleanup(&public_error);
  return rc;
}

int lc_engine_write_bridge(void *context, const void *bytes, size_t count,
                           lc_engine_error *error) {
  lc_write_bridge *bridge;
  lc_error public_error;
  int rc;

  bridge = (lc_write_bridge *)context;
  lc_error_init(&public_error);
  rc = bridge->sink->write(bridge->sink, bytes, count, &public_error);
  if (!rc && public_error.code != LC_OK) {
    error->code = LC_ENGINE_ERROR_TRANSPORT;
    error->message = lc_strdup_local(public_error.message);
  }
  lc_error_cleanup(&public_error);
  return rc;
}

lc_lease *lc_lease_new(lc_client_handle *client, const char *ns,
                       const char *key, const char *owner, const char *lease_id,
                       const char *txn_id, long fencing_token,
                       lc_version version, const char *state_etag,
                       const char *queue_state_etag) {
  lc_lease_handle *lease;

  lease = (lc_lease_handle *)lc_client_calloc(client, 1U, sizeof(*lease));
  if (lease == NULL) {
    return NULL;
  }
  /* A lease is independently closable and allocates through its parent
   * client. Keep that concrete owner alive after public client close until the
   * lease has released all of its fields. */
  lc_client_handle_retain(client);
  lease->pub.describe = lc_lease_describe_method;
  lease->pub.get = lc_lease_get_method;
  lease->pub.load = lc_lease_load_method;
  lease->pub.save = lc_lease_save_method;
  lease->pub.update = lc_lease_update_method;
  lease->pub.mutate = lc_lease_mutate_method;
  lease->pub.mutate_local = lc_lease_mutate_local_method;
  lease->pub.metadata = lc_lease_metadata_method;
  lease->pub.remove = lc_lease_remove_method;
  lease->pub.keepalive = lc_lease_keepalive_method;
  lease->pub.release = lc_lease_release_method;
  lease->pub.attach = lc_lease_attach_method;
  lease->pub.list_attachments = lc_lease_list_attachments_method;
  lease->pub.get_attachment = lc_lease_get_attachment_method;
  lease->pub.delete_attachment = lc_lease_delete_attachment_method;
  lease->pub.delete_all_attachments = lc_lease_delete_all_attachments_method;
  lease->pub.close = lc_lease_close_method;
  lease->client = client;
  lease->ns = lc_client_strdup(client, ns);
  lease->key = lc_client_strdup(client, key);
  lease->owner = lc_client_strdup(client, owner);
  lease->lease_id = lc_client_strdup(client, lease_id);
  lease->txn_id = lc_client_strdup(client, txn_id);
  lease->fencing_token = fencing_token;
  lease->version = version;
  lease->lease_expires_at_unix = 0L;
  lease->state_etag = lc_client_strdup(client, state_etag);
  lease->queue_state_etag = lc_client_strdup(client, queue_state_etag);
  lease->has_query_hidden = 0;
  lease->query_hidden = 0;
  lease->pub.ns = lease->ns;
  lease->pub.key = lease->key;
  lease->pub.owner = lease->owner;
  lease->pub.lease_id = lease->lease_id;
  lease->pub.txn_id = lease->txn_id;
  lease->pub.fencing_token = lease->fencing_token;
  lease->pub.version = lease->version;
  lease->pub.lease_expires_at_unix = lease->lease_expires_at_unix;
  lease->pub.state_etag = lease->state_etag;
  lease->pub.has_query_hidden = lease->has_query_hidden;
  lease->pub.query_hidden = lease->query_hidden;
  return &lease->pub;
}

lc_lease *lc_lease_clone(const lc_lease *source) {
  const lc_lease_handle *source_handle;
  lc_lease_handle *copy_handle;
  lc_lease *copy;

  if (source == NULL) {
    return NULL;
  }
  source_handle = (const lc_lease_handle *)source;
  /* A staged lease carries callback-local mutation state.  Giving that state
   * an independently closable handle would sever the commit boundary, so Lua
   * exposes it only as a scoped borrowed view. */
  if (source_handle->pouch_stage_active) {
    return NULL;
  }
  copy = lc_lease_new(source_handle->client, source->ns, source->key,
                      source->owner, source->lease_id, source->txn_id,
                      source->fencing_token, source->version,
                      source->state_etag, source_handle->queue_state_etag);
  if (copy == NULL) {
    return NULL;
  }
  copy_handle = (lc_lease_handle *)copy;
  if ((source->ns != NULL && copy_handle->ns == NULL) ||
      (source->key != NULL && copy_handle->key == NULL) ||
      (source->owner != NULL && copy_handle->owner == NULL) ||
      (source->lease_id != NULL && copy_handle->lease_id == NULL) ||
      (source->txn_id != NULL && copy_handle->txn_id == NULL) ||
      (source->state_etag != NULL && copy_handle->state_etag == NULL) ||
      (source_handle->queue_state_etag != NULL &&
       copy_handle->queue_state_etag == NULL)) {
    lc_lease_close_method(copy);
    return NULL;
  }
  copy_handle->lease_expires_at_unix = source_handle->lease_expires_at_unix;
  copy_handle->has_query_hidden = source_handle->has_query_hidden;
  copy_handle->query_hidden = source_handle->query_hidden;
  lc_lease_retention_observe(copy, source->retention_support,
                             source->has_retention_expires_at_unix,
                             source->retention_expires_at_unix);
  copy_handle->pouch_commit_immediately =
      source_handle->pouch_commit_immediately;
  copy_handle->pouch_state_removed = source_handle->pouch_state_removed;
  if (source_handle->pouch_state_key != NULL) {
    copy_handle->pouch_state_key =
        lc_client_strdup(copy_handle->client, source_handle->pouch_state_key);
  }
  if (source_handle->pouch_stage_etag != NULL) {
    copy_handle->pouch_stage_etag =
        lc_client_strdup(copy_handle->client, source_handle->pouch_stage_etag);
  }
  if ((source_handle->pouch_state_key != NULL &&
       copy_handle->pouch_state_key == NULL) ||
      (source_handle->pouch_stage_etag != NULL &&
       copy_handle->pouch_stage_etag == NULL)) {
    lc_lease_close_method(copy);
    return NULL;
  }

  copy->describe = source->describe;
  copy->get = source->get;
  copy->load = source->load;
  copy->save = source->save;
  copy->update = source->update;
  copy->mutate = source->mutate;
  copy->mutate_local = source->mutate_local;
  copy->metadata = source->metadata;
  copy->remove = source->remove;
  copy->keepalive = source->keepalive;
  copy->release = source->release;
  copy->attach = source->attach;
  copy->list_attachments = source->list_attachments;
  copy->get_attachment = source->get_attachment;
  copy->delete_attachment = source->delete_attachment;
  copy->delete_all_attachments = source->delete_all_attachments;
  copy->close = source->close;
  copy->lease_expires_at_unix = copy_handle->lease_expires_at_unix;
  copy->has_query_hidden = copy_handle->has_query_hidden;
  copy->query_hidden = copy_handle->query_hidden;
  return copy;
}

static char *lc_queue_state_key_new(lc_client_handle *client, const char *queue,
                                    const char *message_id) {
  size_t queue_length;
  size_t message_id_length;
  size_t total_length;
  char *key;

  if (queue == NULL || queue[0] == '\0' || message_id == NULL ||
      message_id[0] == '\0') {
    return NULL;
  }
  queue_length = strlen(queue);
  message_id_length = strlen(message_id);
  total_length = sizeof("q/") - 1U + queue_length + sizeof("/state/") - 1U +
                 message_id_length + 1U;
  key = (char *)lc_client_alloc(client, total_length);
  if (key == NULL) {
    return NULL;
  }
  snprintf(key, total_length, "q/%s/state/%s", queue, message_id);
  return key;
}

lc_message *lc_message_new(lc_client_handle *client,
                           const lc_engine_dequeue_response *engine,
                           lc_source *payload, int *terminal_flag) {
  lc_message_handle *message;
  char *state_key;

  message = (lc_message_handle *)lc_client_calloc(client, 1U, sizeof(*message));
  if (message == NULL) {
    return NULL;
  }
  /* Like leases, delivered messages own client-allocated fields and may
   * outlive a caller's public client receiver. */
  lc_client_handle_retain(client);
  message->pub.ack = lc_message_ack_method;
  message->pub.nack = lc_message_nack_method;
  message->pub.extend = lc_message_extend_method;
  if (client != NULL && client->is_pouch) {
    message->pub.ack = lc_pouch_message_ack_method;
    message->pub.nack = lc_pouch_message_nack_method;
    message->pub.extend = lc_pouch_message_extend_method;
  }
  message->pub.state = lc_message_state_method;
  message->pub.payload_reader = lc_message_payload_reader_method;
  message->pub.rewind_payload = lc_message_rewind_payload_method;
  message->pub.write_payload = lc_message_write_payload_method;
  message->pub.close = lc_message_close_method;
  message->client = client;
  message->ns = lc_client_strdup(client, engine->ns);
  message->queue = lc_client_strdup(client, engine->queue);
  message->message_id = lc_client_strdup(client, engine->message_id);
  message->attempts = engine->attempts;
  message->max_attempts = engine->max_attempts;
  message->failure_attempts = engine->failure_attempts;
  message->not_visible_until_unix = engine->not_visible_until_unix;
  message->visibility_timeout_seconds = engine->visibility_timeout_seconds;
  message->payload_content_type =
      lc_client_strdup(client, engine->payload_content_type);
  message->correlation_id = lc_client_strdup(client, engine->correlation_id);
  message->lease_id = lc_client_strdup(client, engine->lease_id);
  message->lease_expires_at_unix = engine->lease_expires_at_unix;
  message->fencing_token = engine->fencing_token;
  message->txn_id = lc_client_strdup(client, engine->txn_id);
  message->meta_etag = lc_client_strdup(client, engine->meta_etag);
  message->next_cursor = lc_client_strdup(client, engine->next_cursor);
  message->payload = payload;
  message->terminal_flag = terminal_flag;
  message->state_etag = lc_client_strdup(client, engine->state_etag);
  message->state_lease_id = lc_client_strdup(client, engine->state_lease_id);
  message->state_lease_expires_at_unix = engine->state_lease_expires_at_unix;
  message->state_fencing_token = engine->state_fencing_token;
  message->state_txn_id = lc_client_strdup(client, engine->state_txn_id);
  state_key = NULL;
  if (engine->state_lease_id != NULL && engine->state_lease_id[0] != '\0') {
    state_key =
        lc_queue_state_key_new(client, engine->queue, engine->message_id);
    message->state_lease =
        lc_lease_new(client, engine->ns, state_key, NULL,
                     engine->state_lease_id, engine->state_txn_id,
                     engine->state_fencing_token, 0L, NULL, engine->state_etag);
    lc_client_free(client, state_key);
  }
  message->pub.ns = message->ns;
  message->pub.queue = message->queue;
  message->pub.message_id = message->message_id;
  message->pub.attempts = message->attempts;
  message->pub.max_attempts = message->max_attempts;
  message->pub.failure_attempts = message->failure_attempts;
  message->pub.not_visible_until_unix = message->not_visible_until_unix;
  message->pub.visibility_timeout_seconds = message->visibility_timeout_seconds;
  message->pub.payload_content_type = message->payload_content_type;
  message->pub.correlation_id = message->correlation_id;
  message->pub.lease_id = message->lease_id;
  message->pub.lease_expires_at_unix = message->lease_expires_at_unix;
  message->pub.fencing_token = message->fencing_token;
  message->pub.txn_id = message->txn_id;
  message->pub.meta_etag = message->meta_etag;
  message->pub.next_cursor = message->next_cursor;
  message->pub.payload = message->payload;
  return &message->pub;
}

const char *lc_version_string(void) { return lc_engine_version_string(); }

void lc_error_init(lc_error *error) {
  if (error != NULL) {
    memset(error, 0, sizeof(*error));
  }
}

void lc_error_cleanup(lc_error *error) {
  if (error == NULL) {
    return;
  }
  free(error->message);
  free(error->detail);
  free(error->server_code);
  free(error->correlation_id);
  memset(error, 0, sizeof(*error));
}

void lc_allocator_init(lc_allocator *allocator) {
  if (allocator != NULL) {
    memset(allocator, 0, sizeof(*allocator));
  }
}

typedef struct lc_pouch_endpoint_options {
  char *root_path;
  char *query_engine;
  char *query_fallback_engine;
  int query_indexing_enabled;
  int query_indexing_enabled_set;
  char *crypto_key;
  char *crypto_key_file;
  char *compression;
  int crypto_generate_key_file;
  int single_writer_set;
  int single_writer;
  int durable_sync;
  uint64_t segment_target_bytes;
  uint64_t indexer_flush_docs;
  uint64_t indexer_flush_interval_seconds;
  uint64_t fsync_batch_max_ops;
  int queue_watch;
  int background_compaction_enabled;
  int background_compaction_enabled_set;
  int compaction_throttling_disabled;
  uint64_t terminal_reclaim_min_bytes;
} lc_pouch_endpoint_options;

static void
lc_pouch_endpoint_options_cleanup(const lc_allocator *allocator,
                                  lc_pouch_endpoint_options *options) {
  if (options == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, options->root_path);
  lc_free_with_allocator(allocator, options->query_engine);
  lc_free_with_allocator(allocator, options->query_fallback_engine);
  lc_secret_free_string_with_allocator(allocator, options->crypto_key);
  lc_free_with_allocator(allocator, options->crypto_key_file);
  lc_free_with_allocator(allocator, options->compression);
  memset(options, 0, sizeof(*options));
}

static int lc_query_part_equal(const char *part, size_t part_len,
                               const char *expected) {
  size_t expected_len;

  expected_len = strlen(expected);
  return part_len == expected_len && strncmp(part, expected, part_len) == 0;
}

static int lc_uri_hex_value(char ch) {
  if (ch >= '0' && ch <= '9') {
    return ch - '0';
  }
  if (ch >= 'a' && ch <= 'f') {
    return ch - 'a' + 10;
  }
  if (ch >= 'A' && ch <= 'F') {
    return ch - 'A' + 10;
  }
  return -1;
}

static char *lc_pouch_endpoint_decode_component(const lc_allocator *allocator,
                                                const char *src, size_t src_len,
                                                const char *component,
                                                lc_error *error) {
  char *decoded;
  size_t src_index;
  size_t dst_index;

  decoded = (char *)lc_alloc_with_allocator(allocator, src_len + 1U);
  if (decoded == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch endpoint component", NULL, NULL,
                 NULL);
    return NULL;
  }
  src_index = 0U;
  dst_index = 0U;
  while (src_index < src_len) {
    if (src[src_index] == '%') {
      int high;
      int low;
      unsigned char value;

      if (src_index + 2U >= src_len) {
        lc_free_with_allocator(allocator, decoded);
        lc_error_set(error, LC_ERR_INVALID, 0L,
                     "invalid percent escape in pouch endpoint", component,
                     NULL, NULL);
        return NULL;
      }
      high = lc_uri_hex_value(src[src_index + 1U]);
      low = lc_uri_hex_value(src[src_index + 2U]);
      if (high < 0 || low < 0) {
        lc_free_with_allocator(allocator, decoded);
        lc_error_set(error, LC_ERR_INVALID, 0L,
                     "invalid percent escape in pouch endpoint", component,
                     NULL, NULL);
        return NULL;
      }
      value = (unsigned char)((high << 4) | low);
      if (value == '\0') {
        lc_free_with_allocator(allocator, decoded);
        lc_error_set(error, LC_ERR_INVALID, 0L,
                     "pouch endpoint component must not contain NUL", component,
                     NULL, NULL);
        return NULL;
      }
      decoded[dst_index++] = (char)value;
      src_index += 3U;
    } else {
      decoded[dst_index++] = src[src_index++];
    }
  }
  decoded[dst_index] = '\0';
  return decoded;
}

static int lc_pouch_endpoint_encoded_size(const char *value, int preserve_slash,
                                          size_t *size_out, lc_error *error) {
  const unsigned char *cursor;
  size_t size;

  if (value == NULL || size_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch endpoint component and size are required", NULL,
                        NULL, "pouch");
  }
  size = 0U;
  for (cursor = (const unsigned char *)value; *cursor != '\0'; ++cursor) {
    int unreserved;
    size_t add;

    unreserved = (*cursor >= 'A' && *cursor <= 'Z') ||
                 (*cursor >= 'a' && *cursor <= 'z') ||
                 (*cursor >= '0' && *cursor <= '9') || *cursor == '-' ||
                 *cursor == '.' || *cursor == '_' || *cursor == '~';
    add = (unreserved || (preserve_slash && *cursor == '/')) ? 1U : 3U;
    if (size > SIZE_MAX - add) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch endpoint is too large", NULL, NULL, "pouch");
    }
    size += add;
  }
  *size_out = size;
  return LC_OK;
}

static void lc_pouch_endpoint_encode_component(char *dst, size_t *offset,
                                               const char *value,
                                               int preserve_slash) {
  static const char hex[] = "0123456789ABCDEF";
  const unsigned char *cursor;
  size_t out;

  out = *offset;
  for (cursor = (const unsigned char *)value; *cursor != '\0'; ++cursor) {
    int unreserved;

    unreserved = (*cursor >= 'A' && *cursor <= 'Z') ||
                 (*cursor >= 'a' && *cursor <= 'z') ||
                 (*cursor >= '0' && *cursor <= '9') || *cursor == '-' ||
                 *cursor == '.' || *cursor == '_' || *cursor == '~';
    if (unreserved || (preserve_slash && *cursor == '/')) {
      dst[out++] = (char)*cursor;
    } else {
      dst[out++] = '%';
      dst[out++] = hex[*cursor >> 4U];
      dst[out++] = hex[*cursor & 0x0fU];
    }
  }
  *offset = out;
}

static int lc_pouch_endpoint_parse_boolean(const lc_allocator *allocator,
                                           const char *value, size_t value_len,
                                           const char *option, int *out,
                                           lc_error *error) {
  char *copy;
  int result;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch endpoint boolean output is required", NULL, NULL,
                        "pouch");
  }
  copy = lc_pouch_endpoint_decode_component(allocator, value, value_len, option,
                                            error);
  if (copy == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (strcmp(copy, "true") == 0 || strcmp(copy, "1") == 0) {
    result = 1;
  } else if (strcmp(copy, "false") == 0 || strcmp(copy, "0") == 0) {
    result = 0;
  } else {
    lc_free_with_allocator(allocator, copy);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch endpoint boolean option must be true or false",
                        option, NULL, "pouch");
  }
  lc_free_with_allocator(allocator, copy);
  *out = result;
  return LC_OK;
}

static int lc_pouch_endpoint_parse_u64(const lc_allocator *allocator,
                                       const char *value, size_t value_len,
                                       const char *option, uint64_t *out,
                                       lc_error *error) {
  char *copy;
  lc_u64 parsed;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch endpoint integer output is required", NULL, NULL,
                        "pouch");
  }
  copy = lc_pouch_endpoint_decode_component(allocator, value, value_len, option,
                                            error);
  if (copy == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (!lc_u64_parse_base10(copy, &parsed)) {
    lc_free_with_allocator(allocator, copy);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch endpoint option must be a u64", option, NULL,
                        "pouch");
  }
  lc_free_with_allocator(allocator, copy);
  *out = (uint64_t)parsed;
  return LC_OK;
}

static int lc_pouch_endpoint_parse_option(const lc_allocator *allocator,
                                          const char *key, size_t key_len,
                                          const char *value, size_t value_len,
                                          lc_pouch_endpoint_options *options,
                                          lc_error *error) {
  char *decoded_key;
  char *copy;
  int rc;

  if (key_len == 0U) {
    return LC_OK;
  }
  decoded_key = lc_pouch_endpoint_decode_component(allocator, key, key_len,
                                                   "query option", error);
  if (decoded_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key), "single_writer")) {
    copy = lc_pouch_endpoint_decode_component(allocator, value, value_len,
                                              "single_writer", error);
    if (copy == NULL) {
      lc_free_with_allocator(allocator, decoded_key);
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    if (strcmp(copy, "true") == 0 || strcmp(copy, "1") == 0) {
      options->single_writer = 1;
    } else if (strcmp(copy, "false") == 0 || strcmp(copy, "0") == 0) {
      options->single_writer = 0;
    } else {
      lc_free_with_allocator(allocator, copy);
      lc_free_with_allocator(allocator, decoded_key);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch endpoint single_writer must be true or false",
                          NULL, NULL, NULL);
    }
    options->single_writer_set = 1;
    lc_free_with_allocator(allocator, copy);
    lc_free_with_allocator(allocator, decoded_key);
    return LC_OK;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key), "queue_watch")) {
    copy = lc_pouch_endpoint_decode_component(allocator, value, value_len,
                                              "queue_watch", error);
    if (copy == NULL) {
      lc_free_with_allocator(allocator, decoded_key);
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    if (strcmp(copy, "true") == 0 || strcmp(copy, "1") == 0) {
      options->queue_watch = 1;
    } else if (strcmp(copy, "false") == 0 || strcmp(copy, "0") == 0) {
      options->queue_watch = 0;
    } else {
      lc_free_with_allocator(allocator, copy);
      lc_free_with_allocator(allocator, decoded_key);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch endpoint queue_watch must be true or false",
                          NULL, NULL, "pouch");
    }
    lc_free_with_allocator(allocator, copy);
    lc_free_with_allocator(allocator, decoded_key);
    return LC_OK;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "fsync_batch_max_ops")) {
    lc_u64 parsed;

    copy = lc_pouch_endpoint_decode_component(allocator, value, value_len,
                                              "fsync_batch_max_ops", error);
    if (copy == NULL) {
      lc_free_with_allocator(allocator, decoded_key);
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    if (!lc_u64_parse_base10(copy, &parsed)) {
      lc_free_with_allocator(allocator, copy);
      lc_free_with_allocator(allocator, decoded_key);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch endpoint fsync_batch_max_ops must be a u64",
                          NULL, NULL, "pouch");
    }
    options->fsync_batch_max_ops = (uint64_t)parsed;
    lc_free_with_allocator(allocator, copy);
    lc_free_with_allocator(allocator, decoded_key);
    return LC_OK;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key), "durable_sync")) {
    rc = lc_pouch_endpoint_parse_boolean(allocator, value, value_len,
                                         "durable_sync", &options->durable_sync,
                                         error);
    lc_free_with_allocator(allocator, decoded_key);
    return rc;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key), "query_indexing")) {
    rc = lc_pouch_endpoint_parse_boolean(
        allocator, value, value_len, "query_indexing",
        &options->query_indexing_enabled, error);
    if (rc == LC_OK) {
      options->query_indexing_enabled_set = 1;
    }
    lc_free_with_allocator(allocator, decoded_key);
    return rc;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "segment_target_bytes")) {
    rc = lc_pouch_endpoint_parse_u64(allocator, value, value_len,
                                     "segment_target_bytes",
                                     &options->segment_target_bytes, error);
    lc_free_with_allocator(allocator, decoded_key);
    return rc;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "indexer_flush_docs")) {
    rc = lc_pouch_endpoint_parse_u64(allocator, value, value_len,
                                     "indexer_flush_docs",
                                     &options->indexer_flush_docs, error);
    lc_free_with_allocator(allocator, decoded_key);
    return rc;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "indexer_flush_interval_seconds")) {
    rc = lc_pouch_endpoint_parse_u64(
        allocator, value, value_len, "indexer_flush_interval_seconds",
        &options->indexer_flush_interval_seconds, error);
    lc_free_with_allocator(allocator, decoded_key);
    return rc;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "background_compaction")) {
    rc = lc_pouch_endpoint_parse_boolean(
        allocator, value, value_len, "background_compaction",
        &options->background_compaction_enabled, error);
    if (rc == LC_OK) {
      options->background_compaction_enabled_set = 1;
    }
    lc_free_with_allocator(allocator, decoded_key);
    return rc;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "disable_compaction_throttling")) {
    rc = lc_pouch_endpoint_parse_boolean(
        allocator, value, value_len, "disable_compaction_throttling",
        &options->compaction_throttling_disabled, error);
    lc_free_with_allocator(allocator, decoded_key);
    return rc;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "terminal_reclaim_min_bytes")) {
    rc = lc_pouch_endpoint_parse_u64(
        allocator, value, value_len, "terminal_reclaim_min_bytes",
        &options->terminal_reclaim_min_bytes, error);
    lc_free_with_allocator(allocator, decoded_key);
    return rc;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key), "query_engine") ||
      lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "query_fallback_engine")) {
    int fallback;

    fallback = strstr(decoded_key, "fallback") != NULL ? 1 : 0;
    copy = lc_pouch_endpoint_decode_component(
        allocator, value, value_len,
        fallback ? "query_fallback_engine" : "query_engine", error);
    if (copy == NULL) {
      lc_free_with_allocator(allocator, decoded_key);
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    if (strcmp(copy, "index") != 0 && strcmp(copy, "scan") != 0) {
      lc_free_with_allocator(allocator, copy);
      lc_free_with_allocator(allocator, decoded_key);
      return lc_error_set(
          error, LC_ERR_INVALID, 0L,
          fallback
              ? "pouch endpoint query_fallback_engine must be index or scan"
              : "pouch endpoint query_engine must be index or scan",
          NULL, NULL, "pouch");
    }
    if (fallback) {
      lc_free_with_allocator(allocator, options->query_fallback_engine);
      options->query_fallback_engine = copy;
    } else {
      lc_free_with_allocator(allocator, options->query_engine);
      options->query_engine = copy;
    }
    lc_free_with_allocator(allocator, decoded_key);
    return LC_OK;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key), "crypto_key")) {
    copy = lc_pouch_endpoint_decode_component(allocator, value, value_len,
                                              "crypto_key", error);
    if (copy == NULL) {
      lc_free_with_allocator(allocator, decoded_key);
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    lc_secret_free_string_with_allocator(allocator, options->crypto_key);
    options->crypto_key = copy;
    lc_free_with_allocator(allocator, decoded_key);
    return LC_OK;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "crypto_key_file")) {
    copy = lc_pouch_endpoint_decode_component(allocator, value, value_len,
                                              "crypto_key_file", error);
    if (copy == NULL) {
      lc_free_with_allocator(allocator, decoded_key);
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    lc_free_with_allocator(allocator, options->crypto_key_file);
    options->crypto_key_file = copy;
    lc_free_with_allocator(allocator, decoded_key);
    return LC_OK;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "crypto_generate_key_file")) {
    copy = lc_pouch_endpoint_decode_component(
        allocator, value, value_len, "crypto_generate_key_file", error);
    if (copy == NULL) {
      lc_free_with_allocator(allocator, decoded_key);
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    if (strcmp(copy, "true") == 0 || strcmp(copy, "1") == 0) {
      options->crypto_generate_key_file = 1;
    } else if (strcmp(copy, "false") == 0 || strcmp(copy, "0") == 0) {
      options->crypto_generate_key_file = 0;
    } else {
      lc_free_with_allocator(allocator, copy);
      lc_free_with_allocator(allocator, decoded_key);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch endpoint crypto generate flag must be true "
                          "or false",
                          NULL, NULL, "pouch");
    }
    lc_free_with_allocator(allocator, copy);
    lc_free_with_allocator(allocator, decoded_key);
    return LC_OK;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key), "compression")) {
    copy = lc_pouch_endpoint_decode_component(allocator, value, value_len,
                                              "compression", error);
    if (copy == NULL) {
      lc_free_with_allocator(allocator, decoded_key);
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    if (strcmp(copy, "none") != 0 && strcmp(copy, "zlib") != 0) {
      lc_free_with_allocator(allocator, copy);
      lc_free_with_allocator(allocator, decoded_key);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch endpoint compression must be none or zlib",
                          NULL, NULL, "pouch");
    }
    lc_free_with_allocator(allocator, options->compression);
    options->compression = copy;
    lc_free_with_allocator(allocator, decoded_key);
    return LC_OK;
  }
  lc_free_with_allocator(allocator, decoded_key);
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "unsupported pouch endpoint query option", NULL, NULL,
                      NULL);
}

static int lc_pouch_endpoint_options_parse(const lc_allocator *allocator,
                                           const char *endpoint,
                                           lc_pouch_endpoint_options *options,
                                           lc_error *error) {
  const char *path;
  const char *query;
  const char *cursor;
  const char *query_end;
  size_t path_len;
  size_t query_len;
  int rc;

  memset(options, 0, sizeof(*options));
  lc_pouch_endpoint_split(endpoint, &path, &path_len, &query, &query_len, NULL);
  options->root_path = lc_pouch_endpoint_decode_component(
      allocator, path, path_len, "path", error);
  if (options->root_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (options->root_path[0] != '/') {
    lc_pouch_endpoint_options_cleanup(allocator, options);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch endpoint path must be absolute", NULL, NULL,
                        NULL);
  }
  if (query == NULL) {
    return LC_OK;
  }

  query_end = query + query_len;
  cursor = query;
  while (cursor < query_end) {
    const char *part;
    const char *equals;
    const char *next;
    size_t key_len;
    size_t value_len;

    part = cursor;
    next = (const char *)memchr(part, '&', (size_t)(query_end - part));
    if (next == NULL) {
      next = query_end;
    }
    equals = part;
    while (equals < next && *equals != '=') {
      ++equals;
    }
    key_len = (size_t)(equals - part);
    value_len = equals < next ? (size_t)(next - equals - 1) : 0U;
    rc = lc_pouch_endpoint_parse_option(allocator, part, key_len,
                                        equals < next ? equals + 1 : next,
                                        value_len, options, error);
    if (rc != LC_OK) {
      lc_pouch_endpoint_options_cleanup(allocator, options);
      return rc;
    }
    cursor = next < query_end ? next + 1 : next;
  }
  return LC_OK;
}

static int lc_pouch_endpoint_redacted_copy(const lc_allocator *allocator,
                                           const char *endpoint, char **out,
                                           lc_error *error) {
  const char *query;
  const char *cursor;
  const char *query_end;
  const char *fragment;
  char *copy;
  size_t prefix_len;
  size_t dst;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch endpoint redaction requires out", NULL, NULL,
                        NULL);
  }
  *out = NULL;
  if (!lc_endpoint_is_pouch(endpoint)) {
    *out = lc_strdup_with_allocator(allocator, endpoint);
    if (*out == NULL && endpoint != NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy client endpoint", NULL, NULL, NULL);
    }
    return LC_OK;
  }

  lc_pouch_endpoint_split(endpoint, NULL, NULL, &query, NULL, &fragment);
  if (query == NULL) {
    *out = lc_strdup_with_allocator(allocator, endpoint);
    if (*out == NULL && endpoint != NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy client endpoint", NULL, NULL, NULL);
    }
    return LC_OK;
  }

  copy = (char *)lc_alloc_with_allocator(allocator, strlen(endpoint) + 1U);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy client endpoint", NULL, NULL, NULL);
  }
  prefix_len = (size_t)(query - endpoint - 1);
  memcpy(copy, endpoint, prefix_len);
  dst = prefix_len;
  query_end = fragment != NULL ? fragment : endpoint + strlen(endpoint);
  cursor = query;
  while (cursor < query_end) {
    const char *part;
    const char *equals;
    const char *next;
    char *decoded_key;
    size_t key_len;
    size_t part_len;
    int is_secret;

    part = cursor;
    next = (const char *)memchr(part, '&', (size_t)(query_end - part));
    if (next == NULL) {
      next = query_end;
    }
    equals = part;
    while (equals < next && *equals != '=') {
      ++equals;
    }
    key_len = (size_t)(equals - part);
    part_len = (size_t)(next - part);
    decoded_key = lc_pouch_endpoint_decode_component(allocator, part, key_len,
                                                     "query option", error);
    if (decoded_key == NULL) {
      lc_free_with_allocator(allocator, copy);
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    is_secret = strcmp(decoded_key, "crypto_key") == 0 ? 1 : 0;
    lc_free_with_allocator(allocator, decoded_key);
    if (!is_secret) {
      copy[dst] = dst == prefix_len ? '?' : '&';
      ++dst;
      if (part_len > 0U) {
        memcpy(copy + dst, part, part_len);
        dst += part_len;
      }
    }
    cursor = next < query_end ? next + 1 : next;
  }
  if (fragment != NULL) {
    size_t fragment_len;

    fragment_len = strlen(fragment);
    memcpy(copy + dst, fragment, fragment_len);
    dst += fragment_len;
  }
  copy[dst] = '\0';
  *out = copy;
  return LC_OK;
}

int lc_pouch_endpoint_build(const char *root_path,
                            const lc_pouch_endpoint_option *options,
                            size_t option_count, char **out, lc_error *error) {
  static const char prefix[] = "pouch://";
  size_t total;
  size_t encoded_size;
  size_t offset;
  size_t i;
  char *endpoint;
  int rc;

  if (root_path == NULL || out == NULL ||
      (option_count != 0U && options == NULL)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_endpoint_build requires root_path, options, "
                        "and out",
                        NULL, NULL, "pouch");
  }
  *out = NULL;
  if (root_path[0] != '/') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch endpoint path must be absolute", NULL, NULL,
                        "pouch");
  }
  rc = lc_pouch_endpoint_encoded_size(root_path, 1, &encoded_size, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (encoded_size > SIZE_MAX - (sizeof(prefix) - 1U) - 1U) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "pouch endpoint is too large",
                        NULL, NULL, "pouch");
  }
  total = sizeof(prefix) - 1U + encoded_size;
  for (i = 0U; i < option_count; ++i) {
    if (options[i].name == NULL || options[i].name[0] == '\0') {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch endpoint option name is required", NULL, NULL,
                          "pouch");
    }
    rc = lc_pouch_endpoint_encoded_size(options[i].name, 0, &encoded_size,
                                        error);
    if (rc != LC_OK) {
      return rc;
    }
    if (total > SIZE_MAX - 1U - encoded_size) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch endpoint is too large", NULL, NULL, "pouch");
    }
    total += 1U + encoded_size;
    if (options[i].value != NULL) {
      rc = lc_pouch_endpoint_encoded_size(options[i].value, 0, &encoded_size,
                                          error);
      if (rc != LC_OK) {
        return rc;
      }
      if (total > SIZE_MAX - 1U - encoded_size) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "pouch endpoint is too large", NULL, NULL, "pouch");
      }
      total += 1U + encoded_size;
    }
  }
  endpoint = (char *)lc_alloc_with_allocator(NULL, total + 1U);
  if (endpoint == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch endpoint", NULL, NULL,
                        "pouch");
  }
  memcpy(endpoint, prefix, sizeof(prefix) - 1U);
  offset = sizeof(prefix) - 1U;
  lc_pouch_endpoint_encode_component(endpoint, &offset, root_path, 1);
  for (i = 0U; i < option_count; ++i) {
    endpoint[offset++] = i == 0U ? '?' : '&';
    lc_pouch_endpoint_encode_component(endpoint, &offset, options[i].name, 0);
    if (options[i].value != NULL) {
      endpoint[offset++] = '=';
      lc_pouch_endpoint_encode_component(endpoint, &offset, options[i].value,
                                         0);
    }
  }
  endpoint[offset] = '\0';
  *out = endpoint;
  return LC_OK;
}

int lc_pouch_endpoint_has_option(const char *endpoint, const char *name,
                                 int *present, lc_error *error) {
  const char *path;
  const char *query;
  const char *cursor;
  const char *query_end;
  char *decoded_path;
  size_t path_len;
  size_t query_len;

  if (endpoint == NULL || name == NULL || name[0] == '\0' || present == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_endpoint_has_option requires endpoint, name, "
                        "and present",
                        NULL, NULL, "pouch");
  }
  *present = 0;
  if (!lc_endpoint_is_pouch(endpoint)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, "pouch endpoint is required",
                        NULL, NULL, "pouch");
  }
  lc_pouch_endpoint_split(endpoint, &path, &path_len, &query, &query_len, NULL);
  decoded_path =
      lc_pouch_endpoint_decode_component(NULL, path, path_len, "path", error);
  if (decoded_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (decoded_path[0] != '/') {
    lc_free_with_allocator(NULL, decoded_path);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch endpoint path must be absolute", NULL, NULL,
                        "pouch");
  }
  lc_free_with_allocator(NULL, decoded_path);
  if (query == NULL) {
    return LC_OK;
  }

  query_end = query + query_len;
  cursor = query;
  while (cursor < query_end) {
    const char *equals;
    const char *next;
    char *decoded_name;
    size_t name_len;

    next = (const char *)memchr(cursor, '&', (size_t)(query_end - cursor));
    if (next == NULL) {
      next = query_end;
    }
    equals = cursor;
    while (equals < next && *equals != '=') {
      ++equals;
    }
    name_len = (size_t)(equals - cursor);
    if (name_len != 0U) {
      decoded_name = lc_pouch_endpoint_decode_component(NULL, cursor, name_len,
                                                        "query option", error);
      if (decoded_name == NULL) {
        return error != NULL && error->code != LC_OK ? error->code
                                                     : LC_ERR_NOMEM;
      }
      if (strcmp(decoded_name, name) == 0) {
        *present = 1;
      }
      lc_free_with_allocator(NULL, decoded_name);
    }
    cursor = next < query_end ? next + 1 : next;
  }
  return LC_OK;
}

void lc_pouch_endpoint_free(char *endpoint) {
  lc_secret_free_string_with_allocator(NULL, endpoint);
}

void lc_client_config_init(lc_client_config *config) {
  if (config == NULL) {
    return;
  }
  memset(config, 0, sizeof(*config));
  config->timeout_ms = 30000L;
  config->prefer_http_2 = 1;
  config->http_json_response_limit_bytes = 0U;
}

void lc_pouch_settings_init(lc_pouch_settings *settings) {
  if (settings != NULL) {
    memset(settings, 0, sizeof(*settings));
  }
}

#define LC_INIT_STRUCT_FUNC(type_name, func_name)                              \
  void func_name(type_name *value) {                                           \
    if (value != NULL) {                                                       \
      memset(value, 0, sizeof(*value));                                        \
    }                                                                          \
  }

LC_INIT_STRUCT_FUNC(lc_lease_ref, lc_lease_ref_init)
LC_INIT_STRUCT_FUNC(lc_acquire_req, lc_acquire_req_init)
LC_INIT_STRUCT_FUNC(lc_describe_req, lc_describe_req_init)
LC_INIT_STRUCT_FUNC(lc_get_opts, lc_get_opts_init)
LC_INIT_STRUCT_FUNC(lc_update_opts, lc_update_opts_init)
LC_INIT_STRUCT_FUNC(lc_update_req, lc_update_req_init)
LC_INIT_STRUCT_FUNC(lc_mutate_req, lc_mutate_req_init)
LC_INIT_STRUCT_FUNC(lc_mutate_local_req, lc_mutate_local_req_init)
LC_INIT_STRUCT_FUNC(lc_mutate_op, lc_mutate_op_init)
LC_INIT_STRUCT_FUNC(lc_metadata_req, lc_metadata_req_init)
LC_INIT_STRUCT_FUNC(lc_metadata_op, lc_metadata_op_init)
LC_INIT_STRUCT_FUNC(lc_remove_req, lc_remove_req_init)
LC_INIT_STRUCT_FUNC(lc_remove_op, lc_remove_op_init)
LC_INIT_STRUCT_FUNC(lc_keepalive_req, lc_keepalive_req_init)
LC_INIT_STRUCT_FUNC(lc_keepalive_op, lc_keepalive_op_init)
LC_INIT_STRUCT_FUNC(lc_release_req, lc_release_req_init)
LC_INIT_STRUCT_FUNC(lc_release_op, lc_release_op_init)
LC_INIT_STRUCT_FUNC(lc_query_req, lc_query_req_init)
LC_INIT_STRUCT_FUNC(lc_namespace_config_req, lc_namespace_config_req_init)
LC_INIT_STRUCT_FUNC(lc_index_flush_req, lc_index_flush_req_init)
LC_INIT_STRUCT_FUNC(lc_txn_replay_req, lc_txn_replay_req_init)
LC_INIT_STRUCT_FUNC(lc_txn_decision_req, lc_txn_decision_req_init)
LC_INIT_STRUCT_FUNC(lc_tc_lease_acquire_req, lc_tc_lease_acquire_req_init)
LC_INIT_STRUCT_FUNC(lc_tc_lease_renew_req, lc_tc_lease_renew_req_init)
LC_INIT_STRUCT_FUNC(lc_tc_lease_release_req, lc_tc_lease_release_req_init)
LC_INIT_STRUCT_FUNC(lc_tc_cluster_announce_req, lc_tc_cluster_announce_req_init)
LC_INIT_STRUCT_FUNC(lc_tc_rm_register_req, lc_tc_rm_register_req_init)
LC_INIT_STRUCT_FUNC(lc_tc_rm_unregister_req, lc_tc_rm_unregister_req_init)
LC_INIT_STRUCT_FUNC(lc_enqueue_req, lc_enqueue_req_init)
LC_INIT_STRUCT_FUNC(lc_dequeue_req, lc_dequeue_req_init)
LC_INIT_STRUCT_FUNC(lc_queue_stats_req, lc_queue_stats_req_init)
LC_INIT_STRUCT_FUNC(lc_message_ref, lc_message_ref_init)
LC_INIT_STRUCT_FUNC(lc_nack_req, lc_nack_req_init)
LC_INIT_STRUCT_FUNC(lc_nack_op, lc_nack_op_init)
LC_INIT_STRUCT_FUNC(lc_extend_req, lc_extend_req_init)
LC_INIT_STRUCT_FUNC(lc_extend_op, lc_extend_op_init)
LC_INIT_STRUCT_FUNC(lc_watch_queue_req, lc_watch_queue_req_init)
LC_INIT_STRUCT_FUNC(lc_watch_handler, lc_watch_handler_init)
LC_INIT_STRUCT_FUNC(lc_consumer, lc_consumer_init)
LC_INIT_STRUCT_FUNC(lc_consumer_service_config, lc_consumer_service_config_init)
LC_INIT_STRUCT_FUNC(lc_history_consumer_config, lc_history_consumer_config_init)
LC_INIT_STRUCT_FUNC(lc_outbox_config, lc_outbox_config_init)
LC_INIT_STRUCT_FUNC(lc_outbox_commit_result, lc_outbox_commit_result_init)
void lc_dead_letter_export_opts_init(lc_dead_letter_export_opts *options) {
  if (options == NULL)
    return;
  memset(options, 0, sizeof(*options));
  options->format = LC_DEAD_LETTER_EXPORT_JSON;
}
LC_INIT_STRUCT_FUNC(lc_dead_letter_export_res, lc_dead_letter_export_res_init)
LC_INIT_STRUCT_FUNC(lc_outbox_stats, lc_outbox_stats_init)
LC_INIT_STRUCT_FUNC(lc_outbox_entry, lc_outbox_entry_init)
LC_INIT_STRUCT_FUNC(lc_inbox_message, lc_inbox_message_init)
LC_INIT_STRUCT_FUNC(lc_command_identity, lc_command_identity_init)
LC_INIT_STRUCT_FUNC(lc_command_request, lc_command_request_init)
LC_INIT_STRUCT_FUNC(lc_command_result, lc_command_result_init)
LC_INIT_STRUCT_FUNC(lc_command_receipt, lc_command_receipt_init)
LC_INIT_STRUCT_FUNC(lc_outbox_completion, lc_outbox_completion_init)
LC_INIT_STRUCT_FUNC(lc_outbox_retry, lc_outbox_retry_init)
LC_INIT_STRUCT_FUNC(lc_outbox_participant_request,
                    lc_outbox_participant_request_init)
LC_INIT_STRUCT_FUNC(lc_outbox_receipt, lc_outbox_receipt_init)
LC_INIT_STRUCT_FUNC(lc_attachment_selector, lc_attachment_selector_init)
LC_INIT_STRUCT_FUNC(lc_attach_req, lc_attach_req_init)
LC_INIT_STRUCT_FUNC(lc_attach_op, lc_attach_op_init)
LC_INIT_STRUCT_FUNC(lc_attachment_get_req, lc_attachment_get_req_init)
LC_INIT_STRUCT_FUNC(lc_attachment_list_req, lc_attachment_list_req_init)
LC_INIT_STRUCT_FUNC(lc_attachment_get_op, lc_attachment_get_op_init)
LC_INIT_STRUCT_FUNC(lc_attachment_delete_op, lc_attachment_delete_op_init)
LC_INIT_STRUCT_FUNC(lc_attachment_delete_all_op,
                    lc_attachment_delete_all_op_init)

#undef LC_INIT_STRUCT_FUNC

void lc_outbox_receipt_cleanup(lc_outbox_receipt *receipt) {
  if (receipt == NULL) {
    return;
  }
  free(receipt->outbox_key);
  free(receipt->effect_key);
  memset(receipt, 0, sizeof(*receipt));
}

void lc_outbox_commit_result_cleanup(lc_outbox_commit_result *result) {
  size_t i;

  if (result == NULL)
    return;
  for (i = 0U; i < result->outbox_receipt_count; ++i)
    lc_outbox_receipt_cleanup(&result->outbox_receipts[i]);
  free(result->outbox_receipts);
  memset(result, 0, sizeof(*result));
}

void lc_command_receipt_cleanup(lc_command_receipt *receipt) {
  if (receipt == NULL) {
    return;
  }
  free(receipt->command_id);
  free(receipt->scope);
  free(receipt->command_type);
  free(receipt->idempotency_key);
  free(receipt->operation_id);
  free(receipt->result_code);
  free(receipt->result_reference);
  free(receipt->failure_code);
  free(receipt->failure_message);
  memset(receipt, 0, sizeof(*receipt));
}

void lc_outbox_stats_cleanup(lc_outbox_stats *stats) {
  if (stats == NULL) {
    return;
  }
  free(stats->last_error);
  memset(stats, 0, sizeof(*stats));
}

const char *lc_nack_intent_to_string(lc_nack_intent intent) {
  switch (intent) {
  case LC_NACK_INTENT_UNSPECIFIED:
    return "unspecified";
  case LC_NACK_INTENT_FAILURE:
    return "failure";
  case LC_NACK_INTENT_DEFER:
    return "defer";
  default:
    return "invalid";
  }
}

int lc_nack_intent_to_wire_string(lc_nack_intent intent, const char **out,
                                  lc_error *error) {
  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "nack intent mapping requires output", NULL, NULL,
                        NULL);
  }
  switch (intent) {
  case LC_NACK_INTENT_UNSPECIFIED:
  case LC_NACK_INTENT_FAILURE:
    *out = "failure";
    return LC_OK;
  case LC_NACK_INTENT_DEFER:
    *out = "defer";
    return LC_OK;
  default:
    *out = NULL;
    return lc_error_set(error, LC_ERR_INVALID, 0L, "invalid nack intent", NULL,
                        NULL, NULL);
  }
}

void lc_consumer_restart_policy_init(lc_consumer_restart_policy *policy) {
  if (policy == NULL) {
    return;
  }
  memset(policy, 0, sizeof(*policy));
  policy->immediate_retries = 3;
  policy->base_delay_ms = 250L;
  policy->max_delay_ms = 300000L;
  policy->multiplier = 2.0;
}

void lc_consumer_config_init(lc_consumer_config *config) {
  if (config == NULL) {
    return;
  }
  memset(config, 0, sizeof(*config));
  config->worker_count = 1U;
  lc_consumer_restart_policy_init(&config->restart_policy);
}

#define LC_POUCH_SETTINGS_KNOWN_MASK                                           \
  (LC_POUCH_SETTING_SINGLE_WRITER | LC_POUCH_SETTING_DURABLE_SYNC |            \
   LC_POUCH_SETTING_FSYNC_BATCH_MAX_OPS |                                      \
   LC_POUCH_SETTING_SEGMENT_TARGET_BYTES |                                     \
   LC_POUCH_SETTING_INDEXER_FLUSH_DOCS |                                       \
   LC_POUCH_SETTING_INDEXER_FLUSH_INTERVAL_SECONDS |                           \
   LC_POUCH_SETTING_BACKGROUND_COMPACTION |                                    \
   LC_POUCH_SETTING_DISABLE_COMPACTION_THROTTLING |                            \
   LC_POUCH_SETTING_TERMINAL_RECLAIM_MIN_BYTES |                               \
   LC_POUCH_SETTING_QUEUE_WATCH | LC_POUCH_SETTING_QUERY_ENGINE |              \
   LC_POUCH_SETTING_QUERY_FALLBACK_ENGINE | LC_POUCH_SETTING_QUERY_INDEXING |  \
   LC_POUCH_SETTING_CRYPTO_KEY | LC_POUCH_SETTING_CRYPTO_KEY_FILE |            \
   LC_POUCH_SETTING_CRYPTO_GENERATE_KEY_FILE | LC_POUCH_SETTING_COMPRESSION)

static int lc_pouch_settings_boolean_valid(int value) {
  return value == 0 || value == 1;
}

static int lc_pouch_settings_engine_valid(const char *value) {
  return value != NULL && value[0] != '\0' &&
         (strcmp(value, "index") == 0 || strcmp(value, "scan") == 0);
}

static int lc_pouch_settings_string_valid(const char *value) {
  return value != NULL && value[0] != '\0';
}

static int lc_pouch_settings_validate(const lc_pouch_settings *settings,
                                      int is_pouch, lc_error *error) {
  uint64_t mask;

  if (settings == NULL || settings->set_mask == 0U) {
    return LC_OK;
  }
  mask = settings->set_mask;
  if ((mask & ~LC_POUCH_SETTINGS_KNOWN_MASK) != 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch settings contain an unknown set_mask bit", NULL,
                        NULL, "pouch");
  }
  if (!is_pouch) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch settings require exactly one pouch endpoint",
                        NULL, NULL, "pouch");
  }
  if (((mask & LC_POUCH_SETTING_SINGLE_WRITER) != 0U &&
       !lc_pouch_settings_boolean_valid(settings->single_writer)) ||
      ((mask & LC_POUCH_SETTING_DURABLE_SYNC) != 0U &&
       !lc_pouch_settings_boolean_valid(settings->durable_sync)) ||
      ((mask & LC_POUCH_SETTING_BACKGROUND_COMPACTION) != 0U &&
       !lc_pouch_settings_boolean_valid(
           settings->background_compaction_enabled)) ||
      ((mask & LC_POUCH_SETTING_DISABLE_COMPACTION_THROTTLING) != 0U &&
       !lc_pouch_settings_boolean_valid(
           settings->compaction_throttling_disabled)) ||
      ((mask & LC_POUCH_SETTING_QUEUE_WATCH) != 0U &&
       !lc_pouch_settings_boolean_valid(settings->queue_watch)) ||
      ((mask & LC_POUCH_SETTING_QUERY_INDEXING) != 0U &&
       !lc_pouch_settings_boolean_valid(settings->query_indexing_enabled)) ||
      ((mask & LC_POUCH_SETTING_CRYPTO_GENERATE_KEY_FILE) != 0U &&
       !lc_pouch_settings_boolean_valid(settings->crypto_generate_key_file))) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch settings boolean fields must be zero or one",
                        NULL, NULL, "pouch");
  }
  if (((mask & LC_POUCH_SETTING_QUERY_ENGINE) != 0U &&
       !lc_pouch_settings_engine_valid(settings->query_engine)) ||
      ((mask & LC_POUCH_SETTING_QUERY_FALLBACK_ENGINE) != 0U &&
       !lc_pouch_settings_engine_valid(settings->query_fallback_engine))) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch settings query engines must be index or scan",
                        NULL, NULL, "pouch");
  }
  if ((mask & LC_POUCH_SETTING_COMPRESSION) != 0U &&
      (!lc_pouch_settings_string_valid(settings->compression) ||
       (strcmp(settings->compression, "none") != 0 &&
        strcmp(settings->compression, "zlib") != 0))) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch settings compression must be none or zlib", NULL,
                        NULL, "pouch");
  }
  if (((mask & LC_POUCH_SETTING_CRYPTO_KEY) != 0U &&
       !lc_pouch_settings_string_valid(settings->crypto_key)) ||
      ((mask & LC_POUCH_SETTING_CRYPTO_KEY_FILE) != 0U &&
       !lc_pouch_settings_string_valid(settings->crypto_key_file))) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "selected pouch settings strings must be non-empty",
                        NULL, NULL, "pouch");
  }
  return LC_OK;
}

static void
lc_pouch_open_options_apply_settings(lc_pouch_open_options *options,
                                     const lc_pouch_settings *settings) {
  uint64_t mask;

  if (options == NULL || settings == NULL || settings->set_mask == 0U) {
    return;
  }
  mask = settings->set_mask;
  if ((mask & LC_POUCH_SETTING_SINGLE_WRITER) != 0U) {
    options->single_writer_set = 1;
    options->single_writer = settings->single_writer;
  }
  if ((mask & LC_POUCH_SETTING_DURABLE_SYNC) != 0U)
    options->durable_sync = settings->durable_sync;
  if ((mask & LC_POUCH_SETTING_FSYNC_BATCH_MAX_OPS) != 0U)
    options->fsync_batch_max_ops = settings->fsync_batch_max_ops;
  if ((mask & LC_POUCH_SETTING_SEGMENT_TARGET_BYTES) != 0U)
    options->segment_target_bytes = settings->segment_target_bytes;
  if ((mask & LC_POUCH_SETTING_INDEXER_FLUSH_DOCS) != 0U)
    options->indexer_flush_docs = settings->indexer_flush_docs;
  if ((mask & LC_POUCH_SETTING_INDEXER_FLUSH_INTERVAL_SECONDS) != 0U)
    options->indexer_flush_interval_seconds =
        settings->indexer_flush_interval_seconds;
  if ((mask & LC_POUCH_SETTING_BACKGROUND_COMPACTION) != 0U) {
    options->background_compaction_enabled_set = 1;
    options->background_compaction_enabled =
        settings->background_compaction_enabled;
  }
  if ((mask & LC_POUCH_SETTING_DISABLE_COMPACTION_THROTTLING) != 0U)
    options->compaction_throttling_disabled =
        settings->compaction_throttling_disabled;
  if ((mask & LC_POUCH_SETTING_TERMINAL_RECLAIM_MIN_BYTES) != 0U)
    options->terminal_reclaim_min_bytes = settings->terminal_reclaim_min_bytes;
  if ((mask & LC_POUCH_SETTING_QUEUE_WATCH) != 0U)
    options->queue_watch = settings->queue_watch;
  if ((mask & LC_POUCH_SETTING_QUERY_ENGINE) != 0U)
    options->query_engine = settings->query_engine;
  if ((mask & LC_POUCH_SETTING_QUERY_FALLBACK_ENGINE) != 0U)
    options->query_fallback_engine = settings->query_fallback_engine;
  if ((mask & LC_POUCH_SETTING_QUERY_INDEXING) != 0U) {
    options->query_indexing_enabled_set = 1;
    options->query_indexing_enabled = settings->query_indexing_enabled;
  }
  if ((mask & LC_POUCH_SETTING_CRYPTO_KEY) != 0U)
    options->crypto_key = settings->crypto_key;
  if ((mask & LC_POUCH_SETTING_CRYPTO_KEY_FILE) != 0U)
    options->crypto_key_file = settings->crypto_key_file;
  if ((mask & LC_POUCH_SETTING_CRYPTO_GENERATE_KEY_FILE) != 0U)
    options->crypto_generate_key_file = settings->crypto_generate_key_file;
  if ((mask & LC_POUCH_SETTING_COMPRESSION) != 0U)
    options->compression = settings->compression;
}

#undef LC_POUCH_SETTINGS_KNOWN_MASK

int lc_client_open(const lc_client_config *config, lc_client **out,
                   lc_error *error) {
  lc_engine_client_config engine_config;
  lc_engine_error engine_error;
  lc_bundle_capture_source bundle_capture;
  lc_pouch_endpoint_options pouch_endpoint_options;
  lc_pouch_open_options pouch_open_options;
  unsigned char *bundle_bytes;
  size_t bundle_length;
  lc_client_handle *client;
  size_t i;
  size_t pouch_endpoint_count;
  int is_pouch;
  int rc;

  if (config == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_client_open requires config and out", NULL, NULL,
                        NULL);
  }
  if ((config->allocator.malloc_fn == NULL) !=
          (config->allocator.free_fn == NULL) ||
      (config->allocator.realloc_fn != NULL &&
       config->allocator.malloc_fn == NULL)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "custom allocators require matching malloc/free hooks",
                        NULL, NULL, NULL);
  }
  lc_engine_error_init(&engine_error);
  memset(&bundle_capture, 0, sizeof(bundle_capture));
  memset(&pouch_endpoint_options, 0, sizeof(pouch_endpoint_options));
  bundle_bytes = NULL;
  bundle_length = 0U;
  pouch_endpoint_count = 0U;
  if (config->endpoints != NULL) {
    for (i = 0U; i < config->endpoint_count; ++i) {
      if (lc_endpoint_is_pouch(config->endpoints[i])) {
        ++pouch_endpoint_count;
      }
    }
  }
  if (pouch_endpoint_count != 0U &&
      (config->endpoint_count != 1U || config->unix_socket_path != NULL)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch endpoints must be configured alone", NULL, NULL,
                        NULL);
  }
  is_pouch = pouch_endpoint_count == 1U;
  rc = lc_pouch_settings_validate(config->pouch_settings, is_pouch, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!config->disable_mtls && config->client_bundle_source != NULL) {
    bundle_capture.inner = config->client_bundle_source;
    bundle_capture.allocator = &config->allocator;
    bundle_capture.pub.read = lc_bundle_capture_source_read;
    bundle_capture.pub.reset = lc_bundle_capture_source_reset;
    bundle_capture.pub.close = lc_bundle_capture_source_close;
    bundle_capture.pub.impl = &bundle_capture;
  }

  memset(&engine_config, 0, sizeof(engine_config));
  engine_config.endpoints = config->endpoints;
  engine_config.endpoint_count = config->endpoint_count;
  engine_config.unix_socket_path = config->unix_socket_path;
  engine_config.client_bundle_source =
      bundle_capture.inner != NULL ? &bundle_capture.pub : NULL;
  engine_config.client_bundle_path =
      bundle_capture.inner == NULL ? config->client_bundle_path : NULL;
  engine_config.default_namespace = config->default_namespace;
  engine_config.timeout_ms = config->timeout_ms;
  engine_config.disable_mtls = is_pouch ? 1 : config->disable_mtls;
  engine_config.insecure_skip_verify = config->insecure_skip_verify;
  engine_config.prefer_http_2 = config->prefer_http_2;
  engine_config.http_json_response_limit_bytes =
      config->http_json_response_limit_bytes;
  engine_config.logger = config->logger;
  engine_config.disable_logger_sys_field = config->disable_logger_sys_field;
  engine_config.allocator.malloc_fn = config->allocator.malloc_fn;
  engine_config.allocator.realloc_fn = config->allocator.realloc_fn;
  engine_config.allocator.free_fn = config->allocator.free_fn;
  engine_config.allocator.context = config->allocator.context;

  client = (lc_client_handle *)lc_calloc_with_allocator(&config->allocator, 1U,
                                                        sizeof(*client));
  if (client == NULL) {
    lc_free_with_allocator(&config->allocator, bundle_capture.bytes);
    lc_engine_error_cleanup(&engine_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate client",
                        NULL, NULL, NULL);
  }
  client->allocator = config->allocator;
  rc = pthread_mutex_init(&client->lifecycle_mutex, NULL);
  if (rc != 0) {
    lc_free_with_allocator(&config->allocator, bundle_capture.bytes);
    lc_engine_error_cleanup(&engine_error);
    lc_free_with_allocator(&config->allocator, client);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize client lifecycle mutex",
                        strerror(rc), NULL, NULL);
  }
  client->lifecycle_mutex_initialized = 1;
  client->refcount = 1UL;
  client->is_pouch = is_pouch;
  client->disable_logger_sys_field = config->disable_logger_sys_field;
  client->base_logger =
      config->logger != NULL ? config->logger : lc_log_noop_logger();
  if (is_pouch) {
    client->logger = lc_log_client_logger(client->base_logger,
                                          client->disable_logger_sys_field);
    if (client->logger == NULL) {
      lc_free_with_allocator(&config->allocator, bundle_capture.bytes);
      lc_engine_error_cleanup(&engine_error);
      pthread_mutex_destroy(&client->lifecycle_mutex);
      lc_free_with_allocator(&config->allocator, client);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to initialize client logger", NULL, NULL,
                          NULL);
    }
    client->owns_logger = client->logger != client->base_logger;
  }
  if (!is_pouch) {
    rc = lc_engine_client_open(&engine_config, &client->engine, &engine_error);
    if (rc != LC_ENGINE_OK) {
      int public_rc;

      public_rc = lc_error_from_engine(error, &engine_error);
      lc_engine_error_cleanup(&engine_error);
      lc_free_with_allocator(&config->allocator, bundle_capture.bytes);
      pthread_mutex_destroy(&client->lifecycle_mutex);
      lc_free_with_allocator(&config->allocator, client);
      return public_rc;
    }
  }
  if (client->is_pouch) {
    rc = lc_pouch_endpoint_options_parse(&config->allocator,
                                         config->endpoints[0],
                                         &pouch_endpoint_options, error);
    if (rc != LC_OK) {
      lc_client_close_method(&client->pub);
      lc_engine_error_cleanup(&engine_error);
      lc_free_with_allocator(&config->allocator, bundle_capture.bytes);
      return rc;
    }
    memset(&pouch_open_options, 0, sizeof(pouch_open_options));
    pouch_open_options.single_writer_set =
        pouch_endpoint_options.single_writer_set;
    pouch_open_options.single_writer = pouch_endpoint_options.single_writer;
    pouch_open_options.durable_sync = pouch_endpoint_options.durable_sync;
    pouch_open_options.segment_target_bytes =
        pouch_endpoint_options.segment_target_bytes;
    pouch_open_options.indexer_flush_docs =
        pouch_endpoint_options.indexer_flush_docs;
    pouch_open_options.indexer_flush_interval_seconds =
        pouch_endpoint_options.indexer_flush_interval_seconds;
    pouch_open_options.fsync_batch_max_ops =
        pouch_endpoint_options.fsync_batch_max_ops;
    pouch_open_options.queue_watch = pouch_endpoint_options.queue_watch;
    pouch_open_options.background_compaction_enabled =
        pouch_endpoint_options.background_compaction_enabled;
    pouch_open_options.background_compaction_enabled_set =
        pouch_endpoint_options.background_compaction_enabled_set;
    pouch_open_options.compaction_throttling_disabled =
        pouch_endpoint_options.compaction_throttling_disabled;
    pouch_open_options.terminal_reclaim_min_bytes =
        pouch_endpoint_options.terminal_reclaim_min_bytes;
    pouch_open_options.query_engine = pouch_endpoint_options.query_engine;
    pouch_open_options.query_fallback_engine =
        pouch_endpoint_options.query_fallback_engine;
    pouch_open_options.query_indexing_enabled =
        pouch_endpoint_options.query_indexing_enabled;
    pouch_open_options.query_indexing_enabled_set =
        pouch_endpoint_options.query_indexing_enabled_set;
    pouch_open_options.crypto_key = config->pouch_crypto_key != NULL
                                        ? config->pouch_crypto_key
                                        : pouch_endpoint_options.crypto_key;
    pouch_open_options.crypto_key_file =
        config->pouch_crypto_key_file != NULL
            ? config->pouch_crypto_key_file
            : pouch_endpoint_options.crypto_key_file;
    pouch_open_options.crypto_generate_key_file =
        (config->pouch_crypto_generate_key_file_set ||
         config->pouch_crypto_generate_key_file != 0)
            ? config->pouch_crypto_generate_key_file
            : pouch_endpoint_options.crypto_generate_key_file;
    pouch_open_options.compression = config->pouch_compression != NULL
                                         ? config->pouch_compression
                                         : pouch_endpoint_options.compression;
    lc_pouch_open_options_apply_settings(&pouch_open_options,
                                         config->pouch_settings);
    pouch_open_options.logger = client->base_logger;
    rc = lc_pouch_open(pouch_endpoint_options.root_path, &config->allocator,
                       &pouch_open_options, &client->pouch, error);
    if (rc != LC_OK) {
      lc_client_close_method(&client->pub);
      lc_engine_error_cleanup(&engine_error);
      lc_free_with_allocator(&config->allocator, bundle_capture.bytes);
      lc_pouch_endpoint_options_cleanup(&config->allocator,
                                        &pouch_endpoint_options);
      return rc;
    }
    client->pouch_crypto_key = lc_strdup_with_allocator(
        &config->allocator, pouch_open_options.crypto_key);
    if (pouch_open_options.crypto_key != NULL &&
        client->pouch_crypto_key == NULL) {
      lc_client_close_method(&client->pub);
      lc_engine_error_cleanup(&engine_error);
      lc_free_with_allocator(&config->allocator, bundle_capture.bytes);
      lc_pouch_endpoint_options_cleanup(&config->allocator,
                                        &pouch_endpoint_options);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy pouch crypto key", NULL, NULL, NULL);
    }
    client->pouch_crypto_key_file = lc_strdup_with_allocator(
        &config->allocator, pouch_open_options.crypto_key_file);
    if (pouch_open_options.crypto_key_file != NULL &&
        client->pouch_crypto_key_file == NULL) {
      lc_client_close_method(&client->pub);
      lc_engine_error_cleanup(&engine_error);
      lc_free_with_allocator(&config->allocator, bundle_capture.bytes);
      lc_pouch_endpoint_options_cleanup(&config->allocator,
                                        &pouch_endpoint_options);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy pouch crypto key file", NULL, NULL,
                          NULL);
    }
    client->pouch_compression = lc_strdup_with_allocator(
        &config->allocator, lc_pouch_compression_mode(client->pouch));
    if (client->pouch_compression == NULL) {
      lc_client_close_method(&client->pub);
      lc_engine_error_cleanup(&engine_error);
      lc_free_with_allocator(&config->allocator, bundle_capture.bytes);
      lc_pouch_endpoint_options_cleanup(&config->allocator,
                                        &pouch_endpoint_options);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy pouch compression", NULL, NULL, NULL);
    }
    client->pouch_crypto_generate_key_file =
        pouch_open_options.crypto_generate_key_file;
    client->pouch_crypto_generate_key_file_set = 1;
    lc_pouch_endpoint_options_cleanup(&config->allocator,
                                      &pouch_endpoint_options);
  }
  bundle_bytes = bundle_capture.bytes;
  bundle_length = bundle_capture.length;
  bundle_capture.bytes = NULL;
  client->client_bundle_bytes = bundle_bytes;
  client->client_bundle_length = bundle_length;
  client->endpoint_count = config->endpoint_count;
  if (config->endpoint_count != 0U) {
    client->endpoints = (char **)lc_calloc_with_allocator(
        &config->allocator, config->endpoint_count, sizeof(char *));
    if (client->endpoints == NULL) {
      lc_client_close_method(&client->pub);
      lc_engine_error_cleanup(&engine_error);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate client endpoint copy", NULL, NULL,
                          NULL);
    }
    for (i = 0U; i < config->endpoint_count; ++i) {
      rc = lc_pouch_endpoint_redacted_copy(&config->allocator,
                                           config->endpoints[i],
                                           &client->endpoints[i], error);
      if (rc != LC_OK) {
        lc_client_close_method(&client->pub);
        lc_engine_error_cleanup(&engine_error);
        return rc;
      }
    }
  }
  client->unix_socket_path =
      lc_strdup_with_allocator(&config->allocator, config->unix_socket_path);
  if (config->unix_socket_path != NULL && client->unix_socket_path == NULL) {
    lc_client_close_method(&client->pub);
    lc_engine_error_cleanup(&engine_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy unix_socket_path", NULL, NULL, NULL);
  }
  client->client_bundle_path =
      lc_strdup_with_allocator(&config->allocator, config->client_bundle_path);
  if (config->client_bundle_path != NULL &&
      client->client_bundle_path == NULL) {
    lc_client_close_method(&client->pub);
    lc_engine_error_cleanup(&engine_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy client bundle path", NULL, NULL, NULL);
  }
  client->default_namespace =
      lc_strdup_with_allocator(&config->allocator, config->default_namespace);
  if (config->default_namespace != NULL && client->default_namespace == NULL) {
    lc_client_close_method(&client->pub);
    lc_engine_error_cleanup(&engine_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy default namespace", NULL, NULL, NULL);
  }
  client->timeout_ms = config->timeout_ms;
  client->disable_mtls = is_pouch ? 1 : config->disable_mtls;
  client->insecure_skip_verify = config->insecure_skip_verify;
  client->prefer_http_2 = config->prefer_http_2;
  if (client->engine != NULL) {
    client->logger = lc_engine_client_logger(client->engine);
  }
  client->http_json_response_limit_bytes =
      config->http_json_response_limit_bytes;
  client->pub.acquire = lc_client_acquire_method;
  client->pub.create_collection = lc_client_create_collection_method;
  client->pub.open_collection = lc_client_open_collection_method;
  client->pub.list_collections = lc_client_list_collections_method;
  client->pub.collection_capabilities =
      lc_client_collection_capabilities_method;
  client->pub.acquire_for_update = lc_client_acquire_for_update_method;
  client->pub.describe = lc_client_describe_method;
  client->pub.get = lc_client_get_method;
  client->pub.get_in_namespace = lc_client_get_in_namespace_method;
  client->pub.load = lc_client_load_method;
  client->pub.load_in_namespace = lc_client_load_in_namespace_method;
  client->pub.update = lc_client_update_method;
  client->pub.mutate = lc_client_mutate_method;
  client->pub.metadata = lc_client_metadata_method;
  client->pub.remove = lc_client_remove_method;
  client->pub.keepalive = lc_client_keepalive_method;
  client->pub.release = lc_client_release_method;
  client->pub.attach = lc_client_attach_method;
  client->pub.list_attachments = lc_client_list_attachments_method;
  client->pub.get_attachment = lc_client_get_attachment_method;
  client->pub.delete_attachment = lc_client_delete_attachment_method;
  client->pub.delete_all_attachments = lc_client_delete_all_attachments_method;
  client->pub.queue_stats = lc_client_queue_stats_method;
  client->pub.queue_ack = lc_client_queue_ack_method;
  client->pub.queue_nack = lc_client_queue_nack_method;
  client->pub.queue_extend = lc_client_queue_extend_method;
  client->pub.query = lc_client_query_method;
  client->pub.query_keys = lc_client_query_keys_method;
  client->pub.get_namespace_config = lc_client_get_namespace_config_method;
  client->pub.update_namespace_config =
      lc_client_update_namespace_config_method;
  client->pub.flush_index = lc_client_flush_index_method;
  client->pub.txn_replay = lc_client_txn_replay_method;
  client->pub.txn_prepare = lc_client_txn_prepare_method;
  client->pub.txn_commit = lc_client_txn_commit_method;
  client->pub.txn_rollback = lc_client_txn_rollback_method;
  client->pub.tc_lease_acquire = lc_client_tc_lease_acquire_method;
  client->pub.tc_lease_renew = lc_client_tc_lease_renew_method;
  client->pub.tc_lease_release = lc_client_tc_lease_release_method;
  client->pub.tc_leader = lc_client_tc_leader_method;
  client->pub.tc_cluster_announce = lc_client_tc_cluster_announce_method;
  client->pub.tc_cluster_leave = lc_client_tc_cluster_leave_method;
  client->pub.tc_cluster_list = lc_client_tc_cluster_list_method;
  client->pub.tc_rm_register = lc_client_tc_rm_register_method;
  client->pub.tc_rm_unregister = lc_client_tc_rm_unregister_method;
  client->pub.tc_rm_list = lc_client_tc_rm_list_method;
  client->pub.enqueue = lc_client_enqueue_method;
  client->pub.dequeue = lc_client_dequeue_method;
  client->pub.dequeue_batch = lc_client_dequeue_batch_method;
  client->pub.dequeue_with_state = lc_client_dequeue_with_state_method;
  client->pub.subscribe = lc_client_subscribe_method;
  client->pub.subscribe_with_state = lc_client_subscribe_with_state_method;
  client->pub.new_consumer_service = lc_client_new_consumer_service_method;
  client->pub.new_outbox = lc_client_new_outbox_method;
  client->pub.new_outbox_with_dispatcher =
      lc_client_new_outbox_with_dispatcher_method;
  client->pub.new_history_consumer = lc_client_new_history_consumer_method;
  client->pub.watch_queue = lc_client_watch_queue_method;
  client->pub.close = lc_client_close_method;
  if (client->is_pouch) {
    client->pub.acquire = lc_pouch_client_acquire_method;
    client->pub.acquire_for_update = lc_pouch_client_acquire_for_update_method;
    client->pub.describe = lc_pouch_client_describe_method;
    client->pub.get = lc_pouch_client_get_method;
    client->pub.get_in_namespace = lc_pouch_client_get_in_namespace;
    client->pub.load = lc_pouch_client_load_method;
    client->pub.load_in_namespace = lc_pouch_client_load_in_namespace;
    client->pub.update = lc_pouch_client_update_method;
    client->pub.mutate = lc_pouch_client_mutate_method;
    client->pub.metadata = lc_pouch_client_metadata_method;
    client->pub.remove = lc_pouch_client_remove_method;
    client->pub.keepalive = lc_pouch_client_keepalive_method;
    client->pub.release = lc_pouch_client_release_method;
    client->pub.attach = lc_pouch_client_attach_method;
    client->pub.list_attachments = lc_pouch_client_list_attachments_method;
    client->pub.get_attachment = lc_pouch_client_get_attachment_method;
    client->pub.delete_attachment = lc_pouch_client_delete_attachment_method;
    client->pub.delete_all_attachments =
        lc_pouch_client_delete_all_attachments_method;
    client->pub.queue_stats = lc_pouch_client_queue_stats_method;
    client->pub.queue_ack = lc_pouch_client_queue_ack_method;
    client->pub.queue_nack = lc_pouch_client_queue_nack_method;
    client->pub.queue_extend = lc_pouch_client_queue_extend_method;
    client->pub.enqueue = lc_pouch_client_enqueue_method;
    client->pub.dequeue = lc_pouch_client_dequeue_method;
    client->pub.dequeue_batch = lc_pouch_client_dequeue_batch_method;
    client->pub.dequeue_with_state = lc_pouch_client_dequeue_with_state_method;
    client->pub.subscribe = lc_pouch_client_subscribe_method;
    client->pub.subscribe_with_state =
        lc_pouch_client_subscribe_with_state_method;
    client->pub.watch_queue = lc_pouch_client_watch_queue_method;
    client->pub.query = lc_pouch_client_query_method;
    client->pub.query_keys = lc_pouch_client_query_keys_method;
    client->pub.get_namespace_config =
        lc_pouch_client_get_namespace_config_method;
    client->pub.update_namespace_config =
        lc_pouch_client_update_namespace_config_method;
    client->pub.flush_index = lc_pouch_client_flush_index_method;
    client->pub.txn_replay = lc_pouch_client_txn_replay_method;
    client->pub.txn_prepare = lc_pouch_client_txn_prepare_method;
    client->pub.txn_commit = lc_pouch_client_txn_commit_method;
    client->pub.txn_rollback = lc_pouch_client_txn_rollback_method;
    client->pub.tc_lease_acquire = lc_pouch_client_tc_lease_acquire_method;
    client->pub.tc_lease_renew = lc_pouch_client_tc_lease_renew_method;
    client->pub.tc_lease_release = lc_pouch_client_tc_lease_release_method;
    client->pub.tc_leader = lc_pouch_client_tc_leader_method;
    client->pub.tc_cluster_announce =
        lc_pouch_client_tc_cluster_announce_method;
    client->pub.tc_cluster_leave = lc_pouch_client_tc_cluster_leave_method;
    client->pub.tc_cluster_list = lc_pouch_client_tc_cluster_list_method;
    client->pub.tc_rm_register = lc_pouch_client_tc_rm_register_method;
    client->pub.tc_rm_unregister = lc_pouch_client_tc_rm_unregister_method;
    client->pub.tc_rm_list = lc_pouch_client_tc_rm_list_method;
    client->pub.new_history_consumer =
        lc_pouch_client_new_history_consumer_method;
    rc = lc_pouch_client_recover_transactions(&client->pub, error);
    if (rc != LC_OK) {
      lc_client_close_method(&client->pub);
      lc_engine_error_cleanup(&engine_error);
      return rc;
    }
  }
  client->pub.default_namespace = client->default_namespace;
  *out = &client->pub;
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_source_from_memory(const void *bytes, size_t length, lc_source **out,
                          lc_error *error) {
  lc_memory_source *source;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "source_from_memory requires out", NULL, NULL, NULL);
  }
  source = (lc_memory_source *)calloc(1U, sizeof(*source));
  if (source == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate memory source", NULL, NULL, NULL);
  }
  source->base.pub.read = lc_source_pub_read;
  source->base.pub.reset = lc_source_pub_reset;
  source->base.pub.close = lc_source_pub_close;
  source->base.read_impl = lc_memory_source_read;
  source->base.reset_impl = lc_memory_source_reset;
  source->base.close_impl = lc_memory_source_close;
  if (length > 0U) {
    source->bytes = (unsigned char *)malloc(length);
    if (source->bytes == NULL) {
      free(source);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate memory source bytes", NULL, NULL,
                          NULL);
    }
    memcpy(source->bytes, bytes, length);
  }
  source->length = length;
  source->owns_bytes = 1;
  *out = &source->base.pub;
  return LC_OK;
}

int lc_source_from_file(const char *path, lc_source **out, lc_error *error) {
  lc_file_source *source;

  if (path == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "source_from_file requires path and out", NULL, NULL,
                        NULL);
  }
  source = (lc_file_source *)calloc(1U, sizeof(*source));
  if (source == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate file source", NULL, NULL, NULL);
  }
  source->fp = fopen(path, "rb");
  if (source->fp == NULL) {
    free(source);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open file source", NULL, NULL, NULL);
  }
  source->close_file = 1;
  source->base.pub.read = lc_source_pub_read;
  source->base.pub.reset = lc_source_pub_reset;
  source->base.pub.close = lc_source_pub_close;
  source->base.read_impl = lc_file_source_read;
  source->base.reset_impl = lc_file_source_reset;
  source->base.close_impl = lc_file_source_close;
  *out = &source->base.pub;
  return LC_OK;
}

int lc_source_from_fd(int fd, lc_source **out, lc_error *error) {
  lc_fd_source *source;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "source_from_fd requires out", NULL, NULL, NULL);
  }
  source = (lc_fd_source *)calloc(1U, sizeof(*source));
  if (source == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate fd source",
                        NULL, NULL, NULL);
  }
  source->fd = fd;
  source->base.pub.read = lc_source_pub_read;
  source->base.pub.reset = lc_source_pub_reset;
  source->base.pub.close = lc_source_pub_close;
  source->base.read_impl = lc_fd_source_read;
  source->base.close_impl = lc_fd_source_close;
  *out = &source->base.pub;
  return LC_OK;
}

int lc_source_from_callbacks(lc_source_read_fn read, lc_source_reset_fn reset,
                             lc_source_close_fn close, void *context,
                             lc_source **out, lc_error *error) {
  lc_callback_source *source;

  if (read == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "source_from_callbacks requires read and out", NULL,
                        NULL, NULL);
  }
  source = (lc_callback_source *)calloc(1U, sizeof(*source));
  if (source == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate callback source", NULL, NULL, NULL);
  }
  source->base.pub.read = lc_source_pub_read;
  source->base.pub.reset = lc_source_pub_reset;
  source->base.pub.close = lc_source_pub_close;
  source->base.read_impl = lc_callback_source_read;
  source->base.reset_impl = reset != NULL ? lc_callback_source_reset : NULL;
  source->base.close_impl = lc_callback_source_close;
  source->read = read;
  source->reset = reset;
  source->close = close;
  source->context = context;
  *out = &source->base.pub;
  return LC_OK;
}

int lc_sink_to_file(const char *path, lc_sink **out, lc_error *error) {
  int fd;
  int rc;
  lc_fd_sink *sink;

  if (path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, "sink_to_file requires path",
                        NULL, NULL, NULL);
  }
  fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L, "failed to open file sink",
                        NULL, NULL, NULL);
  }
  rc = lc_sink_to_fd(fd, out, error);
  if (rc != LC_OK) {
    close(fd);
    return rc;
  }
  sink = (lc_fd_sink *)*out;
  sink->close_fd = 1;
  return LC_OK;
}

int lc_sink_to_fd(int fd, lc_sink **out, lc_error *error) {
  lc_fd_sink *sink;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, "sink_to_fd requires out",
                        NULL, NULL, NULL);
  }
  sink = (lc_fd_sink *)calloc(1U, sizeof(*sink));
  if (sink == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate fd sink",
                        NULL, NULL, NULL);
  }
  sink->fd = fd;
  sink->base.pub.write = lc_sink_pub_write;
  sink->base.pub.close = lc_sink_pub_close;
  sink->base.write_impl = lc_fd_sink_write;
  sink->base.close_impl = lc_fd_sink_close;
  *out = &sink->base.pub;
  return LC_OK;
}

int lc_sink_to_discard(lc_sink **out, lc_error *error) {
  lc_discard_sink *sink;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "sink_to_discard requires out", NULL, NULL, NULL);
  }
  sink = (lc_discard_sink *)calloc(1U, sizeof(*sink));
  if (sink == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate discard sink", NULL, NULL, NULL);
  }
  sink->base.pub.write = lc_sink_pub_write;
  sink->base.pub.close = lc_sink_pub_close;
  sink->base.pub.impl = &lc_discard_sink_marker;
  sink->base.write_impl = lc_discard_sink_write;
  sink->base.close_impl = lc_discard_sink_close;
  *out = &sink->base.pub;
  return LC_OK;
}

int lc_sink_is_discard(const lc_sink *sink) {
  if (sink == NULL) {
    return 0;
  }
  return sink->write == lc_sink_pub_write && sink->close == lc_sink_pub_close &&
         sink->impl == &lc_discard_sink_marker;
}

int lc_sink_to_memory(lc_sink **out, lc_error *error) {
  lc_memory_sink *sink;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "sink_to_memory requires out", NULL, NULL, NULL);
  }
  sink = (lc_memory_sink *)calloc(1U, sizeof(*sink));
  if (sink == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate memory sink", NULL, NULL, NULL);
  }
  sink->base.pub.write = lc_sink_pub_write;
  sink->base.pub.close = lc_sink_pub_close;
  sink->base.write_impl = lc_memory_sink_write;
  sink->base.close_impl = lc_memory_sink_close;
  *out = &sink->base.pub;
  return LC_OK;
}

int lc_sink_memory_bytes(lc_sink *sink, const void **bytes, size_t *length,
                         lc_error *error) {
  lc_memory_sink *memory_sink;

  (void)error;
  if (sink == NULL || bytes == NULL || length == NULL) {
    return LC_ERR_INVALID;
  }
  memory_sink = (lc_memory_sink *)sink;
  *bytes = memory_sink->bytes;
  *length = memory_sink->length;
  return LC_OK;
}

int lc_sink_memory_reserve(lc_sink *sink, size_t capacity, lc_error *error) {
  lc_sink_impl *impl;
  lc_memory_sink *memory_sink;
  unsigned char *next;

  if (sink == NULL) {
    return LC_OK;
  }
  if (sink->write != lc_sink_pub_write || sink->close != lc_sink_pub_close) {
    return LC_OK;
  }
  impl = (lc_sink_impl *)sink;
  if (impl->write_impl != lc_memory_sink_write) {
    return LC_OK;
  }
  memory_sink = (lc_memory_sink *)sink;
  if (capacity <= memory_sink->capacity) {
    return LC_OK;
  }
  next = (unsigned char *)realloc(memory_sink->bytes, capacity);
  if (next == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to grow memory sink",
                        NULL, NULL, NULL);
  }
  memory_sink->bytes = next;
  memory_sink->capacity = capacity;
  return LC_OK;
}

int lc_copy(lc_source *src, lc_sink *dst, size_t *written, lc_error *error) {
  unsigned char small[2048];
  unsigned char *buffer;
  size_t capacity;
  size_t total;
  size_t nread;
  int rc;

  if (src == NULL || dst == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_copy requires src and dst", NULL, NULL, NULL);
  }
  /* `error` is an output for this copy operation. A clean source EOF must not
   * inherit an unrelated failure from an earlier operation. */
  if (error != NULL) {
    lc_error_cleanup(error);
    lc_error_init(error);
  }
  /* Short copies avoid a large allocation. Long copies keep the bulk chunk on
   * the heap so callbacks can compose copies on default musl worker stacks. */
  buffer = small;
  capacity = sizeof(small);
  total = 0U;
  for (;;) {
    nread = src->read(src, buffer, capacity, error);
    if (nread == 0U) {
      if (written != NULL) {
        *written = total;
      }
      rc = error != NULL && error->code != LC_OK ? error->code : LC_OK;
      break;
    }
    rc = dst->write(dst, buffer, nread, error);
    if (!rc) {
      rc = error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
      break;
    }
    total += nread;
    if (buffer == small && nread == sizeof(small)) {
      /* 16 KiB also avoids musl's expensive large-allocation size classes. */
      buffer = (unsigned char *)lc_alloc_with_allocator(NULL, 16384U);
      if (buffer == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate stream copy buffer", NULL, NULL,
                          NULL);
        break;
      }
      capacity = 16384U;
    }
  }
  if (buffer != small) {
    lc_free_with_allocator(NULL, buffer);
  }
  return rc;
}

/* Facade result metadata uses raw libc ownership, including buffers supplied
 * by transport parsers and custom facade implementations. */
void lc_describe_res_cleanup(lc_describe_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->ns);
  free(response->key);
  free(response->owner);
  free(response->lease_id);
  free(response->txn_id);
  free(response->state_etag);
  free(response->public_state_etag);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_get_res_cleanup(lc_get_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->content_type);
  free(response->etag);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_update_res_cleanup(lc_update_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->new_state_etag);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_mutate_res_cleanup(lc_mutate_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->new_state_etag);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_metadata_res_cleanup(lc_metadata_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->ns);
  free(response->key);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_remove_res_cleanup(lc_remove_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_keepalive_res_cleanup(lc_keepalive_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->state_etag);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_release_res_cleanup(lc_release_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_query_res_cleanup(lc_query_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->cursor);
  free(response->return_mode);
  free(response->metadata_json);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_string_list_cleanup(lc_string_list *response) {
  size_t index;

  if (response == NULL) {
    return;
  }
  for (index = 0U; index < response->count; ++index) {
    free(response->items[index]);
  }
  free(response->items);
  memset(response, 0, sizeof(*response));
}

void lc_namespace_config_res_cleanup(lc_namespace_config_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->ns);
  free(response->preferred_engine);
  free(response->fallback_engine);
  free(response->etag);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_index_flush_res_cleanup(lc_index_flush_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->ns);
  free(response->mode);
  free(response->flush_id);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_txn_replay_res_cleanup(lc_txn_replay_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->txn_id);
  free(response->state);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_txn_decision_res_cleanup(lc_txn_decision_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->txn_id);
  free(response->state);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_tc_lease_acquire_res_cleanup(lc_tc_lease_acquire_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->leader_id);
  free(response->leader_endpoint);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_tc_lease_renew_res_cleanup(lc_tc_lease_renew_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->leader_id);
  free(response->leader_endpoint);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_tc_lease_release_res_cleanup(lc_tc_lease_release_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_tc_leader_res_cleanup(lc_tc_leader_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->leader_id);
  free(response->leader_endpoint);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_tc_cluster_res_cleanup(lc_tc_cluster_res *response) {
  if (response == NULL) {
    return;
  }
  lc_string_list_cleanup(&response->endpoints);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_tc_rm_res_cleanup(lc_tc_rm_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->backend_hash);
  lc_string_list_cleanup(&response->endpoints);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_tc_rm_list_res_cleanup(lc_tc_rm_list_res *response) {
  size_t index;

  if (response == NULL) {
    return;
  }
  for (index = 0U; index < response->backend_count; ++index) {
    free(response->backends[index].backend_hash);
    lc_string_list_cleanup(&response->backends[index].endpoints);
  }
  free(response->backends);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_enqueue_res_cleanup(lc_enqueue_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->ns);
  free(response->queue);
  free(response->message_id);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_queue_stats_res_cleanup(lc_queue_stats_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->ns);
  free(response->queue);
  free(response->head_message_id);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_ack_res_cleanup(lc_ack_res *response) {
  if (response != NULL) {
    free(response->correlation_id);
    memset(response, 0, sizeof(*response));
  }
}

void lc_nack_res_cleanup(lc_nack_res *response) {
  if (response != NULL) {
    free(response->meta_etag);
    free(response->correlation_id);
    memset(response, 0, sizeof(*response));
  }
}

void lc_extend_res_cleanup(lc_extend_res *response) {
  if (response != NULL) {
    free(response->meta_etag);
    free(response->correlation_id);
    memset(response, 0, sizeof(*response));
  }
}

void lc_dequeue_batch_cleanup(lc_dequeue_batch_res *response) {
  size_t index;

  if (response == NULL) {
    return;
  }
  for (index = 0U; index < response->count; ++index) {
    if (response->messages[index] != NULL) {
      response->messages[index]->close(response->messages[index]);
    }
  }
  free(response->messages);
  memset(response, 0, sizeof(*response));
}

void lc_watch_event_cleanup(lc_watch_event *event) {
  if (event == NULL) {
    return;
  }
  free(event->ns);
  free(event->queue);
  free(event->head_message_id);
  free(event->correlation_id);
  memset(event, 0, sizeof(*event));
}

void lc_attachment_info_cleanup(lc_attachment_info *info) {
  if (info == NULL) {
    return;
  }
  free(info->id);
  free(info->name);
  free(info->plaintext_sha256);
  free(info->content_type);
  memset(info, 0, sizeof(*info));
}

void lc_attach_res_cleanup(lc_attach_res *response) {
  if (response == NULL) {
    return;
  }
  lc_attachment_info_cleanup(&response->attachment);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_attachment_list_cleanup(lc_attachment_list *response) {
  size_t index;

  if (response == NULL) {
    return;
  }
  for (index = 0U; index < response->count; ++index) {
    lc_attachment_info_cleanup(&response->items[index]);
  }
  free(response->items);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_attachment_get_res_cleanup(lc_attachment_get_res *response) {
  if (response == NULL) {
    return;
  }
  lc_attachment_info_cleanup(&response->attachment);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

lc_source *lc_source_from_open_file(FILE *fp, int close_file) {
  lc_file_source *source;

  if (fp == NULL) {
    return NULL;
  }
  source = (lc_file_source *)calloc(1U, sizeof(*source));
  if (source == NULL) {
    if (close_file) {
      fclose(fp);
    }
    return NULL;
  }
  source->fp = fp;
  source->close_file = close_file;
  source->base.pub.read = lc_source_pub_read;
  source->base.pub.reset = lc_source_pub_reset;
  source->base.pub.close = lc_source_pub_close;
  source->base.read_impl = lc_file_source_read;
  source->base.reset_impl = lc_file_source_reset;
  source->base.close_impl = lc_file_source_close;
  return &source->base.pub;
}

int lc_stream_pipe_open(size_t capacity, const lc_allocator *allocator,
                        lc_source **out, lc_stream_pipe **pipe,
                        lc_error *error) {
  lc_stream_pipe *state;
  lc_stream_source *source;

  if (out == NULL || pipe == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "stream pipe requires out and pipe", NULL, NULL, NULL);
  }
  if (capacity == 0U) {
    capacity = 65536U;
  }
  state =
      (lc_stream_pipe *)lc_calloc_with_allocator(allocator, 1U, sizeof(*state));
  source = NULL;
  if (state == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate stream pipe", NULL, NULL, NULL);
  }
  memset(&state->allocator, 0, sizeof(state->allocator));
  if (allocator != NULL) {
    state->allocator = *allocator;
  }
  state->buffer = (unsigned char *)lc_alloc_with_allocator(allocator, capacity);
  if (state->buffer == NULL) {
    lc_free_with_allocator(allocator, state);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate stream buffer", NULL, NULL, NULL);
  }
  state->capacity = capacity;
  state->reader_ref = 1;
  state->writer_ref = 1;
  pthread_mutex_init(&state->mutex, NULL);
  pthread_cond_init(&state->cond, NULL);
  source = (lc_stream_source *)lc_calloc_with_allocator(allocator, 1U,
                                                        sizeof(*source));
  if (source == NULL) {
    pthread_cond_destroy(&state->cond);
    pthread_mutex_destroy(&state->mutex);
    lc_free_with_allocator(allocator, state->buffer);
    lc_free_with_allocator(allocator, state);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate stream source", NULL, NULL, NULL);
  }
  source->allocator = state->allocator;
  source->pipe = state;
  source->base.pub.read = lc_source_pub_read;
  source->base.pub.reset = lc_source_pub_reset;
  source->base.pub.close = lc_source_pub_close;
  source->base.read_impl = lc_stream_source_read;
  source->base.reset_impl = NULL;
  source->base.close_impl = lc_stream_source_close;
  *out = &source->base.pub;
  *pipe = state;
  return LC_OK;
}

int lc_stream_pipe_write(lc_stream_pipe *pipe, const void *bytes, size_t count,
                         lc_error *error) {
  const unsigned char *src;
  size_t chunk;
  size_t first;
  size_t second;

  if (pipe == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "stream pipe write requires pipe", NULL, NULL, NULL);
  }
  src = (const unsigned char *)bytes;
  pthread_mutex_lock(&pipe->mutex);
  while (count > 0U) {
    while (pipe->used == pipe->capacity && !pipe->reader_closed) {
      pthread_cond_wait(&pipe->cond, &pipe->mutex);
    }
    if (pipe->reader_closed) {
      pthread_mutex_unlock(&pipe->mutex);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "stream consumer closed payload early", NULL, NULL,
                          NULL);
    }
    chunk = pipe->capacity - pipe->used;
    if (chunk > count) {
      chunk = count;
    }
    first = chunk;
    if (pipe->write_pos + first > pipe->capacity) {
      first = pipe->capacity - pipe->write_pos;
    }
    memcpy(pipe->buffer + pipe->write_pos, src, first);
    second = chunk - first;
    if (second > 0U) {
      memcpy(pipe->buffer, src + first, second);
    }
    pipe->write_pos = (pipe->write_pos + chunk) % pipe->capacity;
    pipe->used += chunk;
    src += chunk;
    count -= chunk;
    pthread_cond_broadcast(&pipe->cond);
  }
  pthread_mutex_unlock(&pipe->mutex);
  return LC_OK;
}

void lc_stream_pipe_finish(lc_stream_pipe *pipe) {
  if (pipe == NULL) {
    return;
  }
  pthread_mutex_lock(&pipe->mutex);
  pipe->writer_closed = 1;
  pipe->writer_ref = 0;
  pthread_cond_broadcast(&pipe->cond);
  lc_stream_pipe_release_locked(pipe);
}

void lc_stream_pipe_fail(lc_stream_pipe *pipe, int code, const char *message) {
  if (pipe == NULL) {
    return;
  }
  pthread_mutex_lock(&pipe->mutex);
  pipe->writer_closed = 1;
  pipe->writer_ref = 0;
  pipe->error_code = code != LC_OK ? code : LC_ERR_TRANSPORT;
  lc_free_with_allocator(&pipe->allocator, pipe->error_message);
  pipe->error_message = lc_strdup_with_allocator(&pipe->allocator, message);
  pthread_cond_broadcast(&pipe->cond);
  lc_stream_pipe_release_locked(pipe);
}
