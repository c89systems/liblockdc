#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "lc_internal.h"

#include <lonejson.h>

typedef struct fuzz_query_keys_capture {
  unsigned char mode;
  size_t begins;
  size_t chunks;
  size_t ends;
  size_t bytes;
} fuzz_query_keys_capture;

typedef struct fuzz_query_keys_response {
  lonejson_string_array_stream keys;
  char *cursor;
  lonejson_uint64 index_seq;
  int has_index_seq;
  lonejson_json_value metadata;
} fuzz_query_keys_response;

typedef struct fuzz_query_keys_reader {
  const unsigned char *data;
  size_t len;
  size_t offset;
  size_t chunk_size;
} fuzz_query_keys_reader;

static const lonejson_field fuzz_query_keys_response_fields[] = {
    LONEJSON_FIELD_STRING_ARRAY_STREAM(fuzz_query_keys_response, keys, "keys"),
    LONEJSON_FIELD_STRING_ALLOC(fuzz_query_keys_response, cursor, "cursor"),
    LONEJSON_FIELD_U64_PRESENT(fuzz_query_keys_response, index_seq,
                               has_index_seq, "index_seq"),
    LONEJSON_FIELD_JSON_VALUE(fuzz_query_keys_response, metadata, "metadata")};
LONEJSON_MAP_DEFINE(fuzz_query_keys_response_map, fuzz_query_keys_response,
                    fuzz_query_keys_response_fields);

static lonejson_status fuzz_key_begin(void *user, lonejson_error *error) {
  fuzz_query_keys_capture *capture;

  (void)error;
  capture = (fuzz_query_keys_capture *)user;
  capture->begins += 1U;
  return (capture->mode & 1U) != 0U ? LONEJSON_STATUS_CALLBACK_FAILED
                                    : LONEJSON_STATUS_OK;
}

static lonejson_status fuzz_key_chunk(void *user, const char *data, size_t len,
                                      lonejson_error *error) {
  fuzz_query_keys_capture *capture;

  (void)data;
  (void)error;
  capture = (fuzz_query_keys_capture *)user;
  capture->chunks += 1U;
  capture->bytes += len;
  return (capture->mode & 2U) != 0U ? LONEJSON_STATUS_CALLBACK_FAILED
                                    : LONEJSON_STATUS_OK;
}

static lonejson_status fuzz_key_end(void *user, lonejson_error *error) {
  fuzz_query_keys_capture *capture;

  (void)error;
  capture = (fuzz_query_keys_capture *)user;
  capture->ends += 1U;
  return (capture->mode & 4U) != 0U ? LONEJSON_STATUS_CALLBACK_FAILED
                                    : LONEJSON_STATUS_OK;
}

static lonejson_read_result
fuzz_query_keys_read(void *user, unsigned char *buffer, size_t capacity) {
  fuzz_query_keys_reader *reader;
  lonejson_read_result result;
  size_t current;

  reader = (fuzz_query_keys_reader *)user;
  result = lonejson_default_read_result();
  if (reader == NULL || buffer == NULL || capacity == 0U) {
    result.error_code = 22;
    return result;
  }
  if (reader->offset >= reader->len) {
    result.eof = 1;
    return result;
  }
  current = reader->len - reader->offset;
  if (current > reader->chunk_size) {
    current = reader->chunk_size;
  }
  if (current > capacity) {
    current = capacity;
  }
  memcpy(buffer, reader->data + reader->offset, current);
  reader->offset += current;
  result.bytes_read = current;
  return result;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  static const unsigned char fallback[] = "{\"keys\":[]}";
  lonejson *runtime;
  lonejson_array_stream_string_handler handler;
  lonejson_error error;
  fuzz_query_keys_capture capture;
  fuzz_query_keys_reader reader;
  fuzz_query_keys_response response;
  const unsigned char *json;
  size_t json_size;
  size_t chunk_size;

  memset(&handler, 0, sizeof(handler));
  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    return 0;
  }
  lonejson_error_init(&error);
  memset(&capture, 0, sizeof(capture));
  memset(&reader, 0, sizeof(reader));
  memset(&response, 0, sizeof(response));
  handler.begin = fuzz_key_begin;
  handler.chunk = fuzz_key_chunk;
  handler.end = fuzz_key_end;

  if (size >= 2U) {
    capture.mode = data[0];
    chunk_size = (size_t)(data[1] % 11U) + 1U;
    json = data + 2U;
    json_size = size - 2U;
  } else {
    chunk_size = 3U;
    json = fallback;
    json_size = sizeof(fallback) - 1U;
  }

  reader.data = json;
  reader.len = json_size;
  reader.chunk_size = chunk_size;
  if (runtime->init(runtime, &fuzz_query_keys_response_map, &response,
                    &error) != LONEJSON_STATUS_OK)
    return 0;
  if (response.keys.set_handler(&response.keys, &handler, &capture, &error) !=
      LONEJSON_STATUS_OK) {
    runtime->cleanup(runtime, &fuzz_query_keys_response_map, &response, NULL);
    return 0;
  }
  (void)response.metadata.methods->enable_parse_capture(&response.metadata,
                                                        &error);
  (void)runtime->parse_reader(runtime, &fuzz_query_keys_response_map, &response,
                              fuzz_query_keys_read, &reader, &error);
  runtime->cleanup(runtime, &fuzz_query_keys_response_map, &response, NULL);
  return 0;
}
