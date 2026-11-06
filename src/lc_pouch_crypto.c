#include "lc_pouch_crypto.h"

#include "lc_api_internal.h"
#include "lc_intcompat.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <openssl/core_names.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/params.h>
#include <openssl/rand.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>

#define LC_POUCH_KEY_PREFIX "lc-pouch-key-v1:"
#define LC_POUCH_DESC_PREFIX "lc-pouch-desc-v1:"
#define LC_POUCH_ROOT_KEY_BYTES 32U
#define LC_POUCH_NONCE_PREFIX_BYTES 8U
#define LC_POUCH_NONCE_BYTES 12U
#define LC_POUCH_DEK_BYTES 32U
#define LC_POUCH_GCM_TAG_BYTES 16U
#define LC_POUCH_FIRST_FRAME_PLAINTEXT_BYTES (4U * 1024U)
#define LC_POUCH_FRAME_PLAINTEXT_BYTES (64U * 1024U)
#define LC_POUCH_WRITER_FRAME_BYTES                                            \
  (LC_POUCH_FRAME_PLAINTEXT_BYTES + 2U * (8U + LC_POUCH_GCM_TAG_BYTES))
#define LC_POUCH_DESC_RAW_BYTES_V1 (1U + 4U + LC_POUCH_NONCE_PREFIX_BYTES)
#define LC_POUCH_DESC_RAW_BYTES_V2                                             \
  (LC_POUCH_DESC_RAW_BYTES_V1 + LC_POUCH_DEK_BYTES)
#define LC_POUCH_DESC_BINDING_NONCE_BYTES 12U
#define LC_POUCH_DESC_RAW_BYTES_V3                                             \
  (LC_POUCH_DESC_RAW_BYTES_V2 + LC_POUCH_DESC_BINDING_NONCE_BYTES +            \
   LC_POUCH_GCM_TAG_BYTES)
#define LC_POUCH_DESC_TEXT_BYTES                                               \
  (sizeof(LC_POUCH_DESC_PREFIX) + ((LC_POUCH_DESC_RAW_BYTES_V3 + 2U) / 3U) * 4U)
#define LC_POUCH_DESC_FLAG_ENCRYPTED 0x01U
#define LC_POUCH_DESC_FLAG_COMPRESSED 0x02U
#define LC_POUCH_ZLIB_CHUNK_BYTES (64U * 1024U)

struct lc_pouch_crypto {
  lc_allocator allocator;
  int encryption_enabled;
  int compression_enabled;
  unsigned char root_key[LC_POUCH_ROOT_KEY_BYTES];
  unsigned char data_key[LC_POUCH_DEK_BYTES];
};

typedef struct lc_pouch_crypto_desc {
  int encrypted;
  int compressed;
  int payload_key_derived;
  unsigned long frame_size;
  unsigned char nonce_prefix[LC_POUCH_NONCE_PREFIX_BYTES];
  unsigned char payload_key_salt[LC_POUCH_DEK_BYTES];
  unsigned char binding_nonce[LC_POUCH_DESC_BINDING_NONCE_BYTES];
  unsigned char binding_tag[LC_POUCH_GCM_TAG_BYTES];
  int binding_present;
} lc_pouch_crypto_desc;

typedef struct lc_pouch_crypto_source {
  lc_source pub;
  lc_allocator allocator;
  int fd;
  lc_source *input;
  EVP_CIPHER_CTX *ctx;
  unsigned char key[LC_POUCH_DEK_BYTES];
  unsigned char nonce_prefix[LC_POUCH_NONCE_PREFIX_BYTES];
  char *context;
  size_t context_len;
  size_t context_capacity;
  unsigned char descriptor_core[LC_POUCH_DESC_RAW_BYTES_V2];
  size_t descriptor_core_len;
  unsigned char plain[LC_POUCH_FRAME_PLAINTEXT_BYTES];
  unsigned char
      cipher[8U + LC_POUCH_FRAME_PLAINTEXT_BYTES + LC_POUCH_GCM_TAG_BYTES];
  size_t cipher_begin;
  size_t cipher_end;
  size_t plain_offset;
  size_t plain_length;
  size_t plain_used;
  unsigned long frame_size;
  unsigned long counter;
  uint64_t span_offset;
  uint64_t span_length;
  uint64_t span_remaining;
  uint64_t read_offset;
  int bounded;
  int done;
} lc_pouch_crypto_source;

typedef struct lc_pouch_plain_span_source {
  lc_source pub;
  int fd;
  uint64_t offset;
  uint64_t length;
  uint64_t read_bytes;
} lc_pouch_plain_span_source;

typedef struct lc_pouch_zlib_source {
  lc_source pub;
  lc_allocator allocator;
  lc_source *inner;
  z_stream stream;
  unsigned char input[LC_POUCH_ZLIB_CHUNK_BYTES];
  uint64_t input_total;
  int close_inner;
  int input_done;
  int stream_done;
} lc_pouch_zlib_source;

struct lc_pouch_crypto_reader {
  lc_source pub;
  lc_allocator allocator;
  lc_pouch_crypto *crypto;
  lc_pouch_plain_span_source plain;
  lc_pouch_crypto_source *decryptor;
  lc_pouch_zlib_source *inflater;
  EVP_MAC_CTX *derivation;
  EVP_CIPHER_CTX *binding;
  lc_source *active;
  int owned_fd;
};

static const char lc_pouch_b64url_alphabet[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/* Unsigned-byte lookup: 255 rejects every non-base64url input, including
 * padding and bytes with the high bit set. */
static const unsigned char lc_pouch_b64url_values[256] = {
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    62,  255, 255, 52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  255, 255,
    255, 255, 255, 255, 255, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,
    10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
    25,  255, 255, 255, 255, 63,  255, 26,  27,  28,  29,  30,  31,  32,  33,
    34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,
    49,  50,  51,  255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255,
};

#ifdef LOCKDC_TEST_BUILD
static const unsigned char *lc_pouch_crypto_test_random_material;
static size_t lc_pouch_crypto_test_random_material_length;
static size_t lc_pouch_crypto_test_random_material_offset;

void lc_pouch_crypto_test_set_random_material(const unsigned char *bytes,
                                              size_t length) {
  lc_pouch_crypto_test_random_material = bytes;
  lc_pouch_crypto_test_random_material_length = length;
  lc_pouch_crypto_test_random_material_offset = 0U;
}
#endif

static int lc_pouch_crypto_random_bytes(unsigned char *out, size_t len) {
#ifdef LOCKDC_TEST_BUILD
  if (lc_pouch_crypto_test_random_material != NULL) {
    if (out == NULL ||
        lc_pouch_crypto_test_random_material_offset >
            lc_pouch_crypto_test_random_material_length ||
        len > lc_pouch_crypto_test_random_material_length -
                  lc_pouch_crypto_test_random_material_offset) {
      return 0;
    }
    memcpy(out,
           lc_pouch_crypto_test_random_material +
               lc_pouch_crypto_test_random_material_offset,
           len);
    lc_pouch_crypto_test_random_material_offset += len;
    return 1;
  }
#endif
  return RAND_bytes(out, (int)len) == 1;
}

static char *lc_pouch_crypto_strdup(const lc_allocator *allocator,
                                    const char *text) {
  char *copy;
  size_t len;

  if (text == NULL) {
    return NULL;
  }
  len = strlen(text) + 1U;
  copy = (char *)lc_alloc_with_allocator(allocator, len);
  if (copy == NULL) {
    return NULL;
  }
  memcpy(copy, text, len);
  return copy;
}

static int lc_pouch_crypto_pread_available(int fd, uint64_t offset,
                                           int *available, lc_error *error) {
  off_t target;
  unsigned char byte;
  ssize_t nread;

  if (fd < 0 || available == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch payload read requires descriptor and output",
                        NULL, NULL, "pouch");
  }
  *available = 0;
  target = (off_t)offset;
  if (target < 0 || (uint64_t)target != offset) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch payload offset exceeds platform limit", NULL,
                        NULL, "pouch");
  }
  do {
    nread = pread(fd, &byte, 1U, target);
  } while (nread < 0 && errno == EINTR);
  if (nread < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to verify encrypted pouch payload",
                        strerror(errno), NULL, "pouch");
  }
  *available = nread > 0 ? 1 : 0;
  return LC_OK;
}

static int
lc_pouch_crypto_derive_data_key(const lc_pouch_crypto *crypto,
                                unsigned char key[LC_POUCH_DEK_BYTES],
                                lc_error *error);
static size_t lc_pouch_crypto_descriptor_core_encode(
    const lc_pouch_crypto_desc *desc,
    unsigned char raw[LC_POUCH_DESC_RAW_BYTES_V2]);
static char *lc_pouch_crypto_dirname(const lc_allocator *allocator,
                                     const char *path);
static const char *lc_pouch_crypto_basename(const char *path);
static int lc_pouch_crypto_open_existing_file(const lc_allocator *allocator,
                                              const char *path, int *fd_out,
                                              lc_error *error);

static int lc_pouch_crypto_write_all_fd(int fd, const void *bytes, size_t count,
                                        lc_error *error) {
  const unsigned char *cursor;

  cursor = (const unsigned char *)bytes;
  while (count > 0U) {
    ssize_t written;

    written = write(fd, cursor, count);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to write encrypted pouch payload",
                          strerror(errno), NULL, "pouch");
    }
    if (written == 0) {
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "short write while writing encrypted pouch payload",
                          NULL, NULL, "pouch");
    }
    cursor += (size_t)written;
    count -= (size_t)written;
  }
  return LC_OK;
}

static int lc_pouch_crypto_read_all_file(const lc_allocator *allocator,
                                         const char *path, char **out,
                                         lc_error *error) {
  char *buffer;
  struct stat st;
  uint64_t size;
  size_t offset;
  size_t got;
  int fd;
  int saved_errno;
  int rc;

  if (path == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch key file read requires path and out", NULL, NULL,
                        "pouch");
  }
  *out = NULL;
  rc = lc_pouch_crypto_open_existing_file(allocator, path, &fd, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (fstat(fd, &st) != 0) {
    saved_errno = errno;
    close(fd);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to stat pouch crypto key file",
                        strerror(saved_errno), NULL, "pouch");
  }
  if (!S_ISREG(st.st_mode) || st.st_size < 0) {
    close(fd);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto key file must be a regular file", NULL,
                        NULL, "pouch");
  }
  size = (uint64_t)st.st_size;
  if (size > 8192U) {
    close(fd);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto key file is invalid", NULL, NULL,
                        "pouch");
  }
  buffer = (char *)lc_alloc_with_allocator(allocator, (size_t)size + 1U);
  if (buffer == NULL) {
    close(fd);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto key buffer", NULL,
                        NULL, "pouch");
  }
  offset = 0U;
  while (offset < (size_t)size) {
    ssize_t read_count;

    read_count = read(fd, buffer + offset, (size_t)size - offset);
    if (read_count < 0 && errno == EINTR) {
      continue;
    }
    if (read_count <= 0) {
      saved_errno = read_count < 0 ? errno : 0;
      lc_free_with_allocator(allocator, buffer);
      close(fd);
      return lc_error_set(
          error, LC_ERR_TRANSPORT, 0L, "failed to read pouch crypto key file",
          saved_errno != 0 ? strerror(saved_errno) : NULL, NULL, "pouch");
    }
    offset += (size_t)read_count;
  }
  close(fd);
  got = offset;
  while (got > 0U && (buffer[got - 1U] == '\n' || buffer[got - 1U] == '\r' ||
                      buffer[got - 1U] == ' ' || buffer[got - 1U] == '\t')) {
    --got;
  }
  buffer[got] = '\0';
  *out = buffer;
  return LC_OK;
}

static int lc_pouch_crypto_mkdirs_open(const lc_allocator *allocator,
                                       const char *path, int *fd_out,
                                       lc_error *error) {
  char *component;
  char *copy;
  char *cursor;
  char *separator;
  struct stat st;
  int current_fd;
  int flags;
  int next_fd;
  int saved_errno;

  if (fd_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto key directory requires fd output", NULL,
                        NULL, "pouch");
  }
  *fd_out = -1;

  if (path == NULL || path[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto key directory is required", NULL, NULL,
                        "pouch");
  }
#if !defined(O_DIRECTORY) || !defined(O_NOFOLLOW)
  (void)allocator;
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch crypto key creation requires O_DIRECTORY and "
                      "O_NOFOLLOW support",
                      NULL, NULL, "pouch");
#else
  copy = lc_pouch_crypto_strdup(allocator, path);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto path", NULL, NULL,
                        "pouch");
  }
  flags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW;
#ifdef O_CLOEXEC
  flags |= O_CLOEXEC;
#endif
#ifdef O_NONBLOCK
  flags |= O_NONBLOCK;
#endif
  current_fd = open(copy[0] == '/' ? "/" : ".", flags);
  if (current_fd < 0) {
    saved_errno = errno;
    lc_free_with_allocator(allocator, copy);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch crypto key directory",
                        strerror(saved_errno), NULL, "pouch");
  }
  cursor = copy;
  while (*cursor == '/') {
    ++cursor;
  }
  while (*cursor != '\0') {
    component = cursor;
    separator = strchr(cursor, '/');
    if (separator != NULL) {
      *separator = '\0';
    }
    if (component[0] != '\0' && strcmp(component, ".") != 0) {
      next_fd = openat(current_fd, component, flags);
      if (next_fd < 0 && errno == ENOENT) {
        if (mkdirat(current_fd, component, 0700) != 0 && errno != EEXIST) {
          saved_errno = errno;
          close(current_fd);
          lc_free_with_allocator(allocator, copy);
          return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                              "failed to create pouch crypto config directory",
                              strerror(saved_errno), NULL, "pouch");
        }
        next_fd = openat(current_fd, component, flags);
      }
      if (next_fd < 0) {
        saved_errno = errno;
        close(current_fd);
        lc_free_with_allocator(allocator, copy);
        return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                            "failed to open pouch crypto key directory",
                            strerror(saved_errno), NULL, "pouch");
      }
      if (close(current_fd) != 0) {
        saved_errno = errno;
        close(next_fd);
        lc_free_with_allocator(allocator, copy);
        return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                            "failed to close pouch crypto key directory",
                            strerror(saved_errno), NULL, "pouch");
      }
      current_fd = next_fd;
    }
    if (separator == NULL) {
      break;
    }
    *separator = '/';
    cursor = separator + 1U;
    while (*cursor == '/') {
      ++cursor;
    }
  }
  if (fstat(current_fd, &st) != 0) {
    saved_errno = errno;
    close(current_fd);
    lc_free_with_allocator(allocator, copy);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to inspect pouch crypto key directory",
                        strerror(saved_errno), NULL, "pouch");
  }
  if (!S_ISDIR(st.st_mode) || st.st_uid != geteuid() ||
      (st.st_mode & 0777) != 0700) {
    close(current_fd);
    lc_free_with_allocator(allocator, copy);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto key directory must be owned by the "
                        "current user with mode 0700",
                        NULL, NULL, "pouch");
  }
  lc_free_with_allocator(allocator, copy);
  *fd_out = current_fd;
  return LC_OK;
#endif
}

static char *lc_pouch_crypto_dirname(const lc_allocator *allocator,
                                     const char *path) {
  const char *slash;
  char *out;
  size_t len;

  if (path == NULL) {
    return NULL;
  }
  slash = strrchr(path, '/');
  if (slash == NULL) {
    return lc_pouch_crypto_strdup(allocator, ".");
  }
  if (slash == path) {
    return lc_pouch_crypto_strdup(allocator, "/");
  }
  len = (size_t)(slash - path);
  out = (char *)lc_alloc_with_allocator(allocator, len + 1U);
  if (out == NULL) {
    return NULL;
  }
  memcpy(out, path, len);
  out[len] = '\0';
  return out;
}

static const char *lc_pouch_crypto_basename(const char *path) {
  const char *slash;

  if (path == NULL) {
    return NULL;
  }
  slash = strrchr(path, '/');
  return slash == NULL ? path : slash + 1U;
}

static int lc_pouch_crypto_open_existing_file(const lc_allocator *allocator,
                                              const char *path, int *fd_out,
                                              lc_error *error) {
  char *component;
  char *cursor;
  char *dir;
  char *separator;
  const char *name;
  int current_fd;
  int file_fd;
  int file_flags;
  int flags;
  int next_fd;
  int saved_errno;
  struct stat st;

  if (path == NULL || path[0] == '\0' || fd_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto key file path is required", NULL, NULL,
                        "pouch");
  }
  *fd_out = -1;
  name = lc_pouch_crypto_basename(path);
  if (name == NULL || name[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto key file path must name a file", NULL,
                        NULL, "pouch");
  }
#if !defined(O_DIRECTORY) || !defined(O_NOFOLLOW) || !defined(O_NONBLOCK)
  (void)allocator;
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch crypto key reads require O_DIRECTORY, O_NOFOLLOW, "
                      "and O_NONBLOCK support",
                      NULL, NULL, "pouch");
#else
  dir = lc_pouch_crypto_dirname(allocator, path);
  if (dir == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto path", NULL, NULL,
                        "pouch");
  }
  flags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_NONBLOCK;
#ifdef O_CLOEXEC
  flags |= O_CLOEXEC;
#endif
  current_fd = open(dir[0] == '/' ? "/" : ".", flags);
  if (current_fd < 0) {
    saved_errno = errno;
    lc_free_with_allocator(allocator, dir);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch crypto key directory",
                        strerror(saved_errno), NULL, "pouch");
  }
  cursor = dir;
  while (*cursor == '/') {
    ++cursor;
  }
  while (*cursor != '\0') {
    component = cursor;
    separator = strchr(cursor, '/');
    if (separator != NULL) {
      *separator = '\0';
    }
    if (component[0] != '\0' && strcmp(component, ".") != 0) {
      next_fd = openat(current_fd, component, flags);
      if (next_fd < 0) {
        saved_errno = errno;
        close(current_fd);
        lc_free_with_allocator(allocator, dir);
        return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                            "failed to open pouch crypto key directory",
                            strerror(saved_errno), NULL, "pouch");
      }
      if (close(current_fd) != 0) {
        saved_errno = errno;
        close(next_fd);
        lc_free_with_allocator(allocator, dir);
        return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                            "failed to close pouch crypto key directory",
                            strerror(saved_errno), NULL, "pouch");
      }
      current_fd = next_fd;
    }
    if (separator == NULL) {
      break;
    }
    cursor = separator + 1U;
    while (*cursor == '/') {
      ++cursor;
    }
  }
  if (fstat(current_fd, &st) != 0) {
    saved_errno = errno;
    close(current_fd);
    lc_free_with_allocator(allocator, dir);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to inspect pouch crypto key directory",
                        strerror(saved_errno), NULL, "pouch");
  }
  if (!S_ISDIR(st.st_mode) || st.st_uid != geteuid() ||
      (st.st_mode & 0777) != 0700) {
    close(current_fd);
    lc_free_with_allocator(allocator, dir);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto key directory must be owned by the "
                        "current user with mode 0700",
                        NULL, NULL, "pouch");
  }
  file_flags = O_RDONLY | O_NOFOLLOW | O_NONBLOCK;
#ifdef O_CLOEXEC
  file_flags |= O_CLOEXEC;
#endif
  file_fd = openat(current_fd, name, file_flags);
  if (file_fd < 0) {
    saved_errno = errno;
    close(current_fd);
    lc_free_with_allocator(allocator, dir);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch crypto key file",
                        strerror(saved_errno), NULL, "pouch");
  }
  if (fstat(file_fd, &st) != 0) {
    saved_errno = errno;
    close(file_fd);
    close(current_fd);
    lc_free_with_allocator(allocator, dir);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to stat pouch crypto key file",
                        strerror(saved_errno), NULL, "pouch");
  }
  if (!S_ISREG(st.st_mode)) {
    close(file_fd);
    close(current_fd);
    lc_free_with_allocator(allocator, dir);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto key file must be a regular file", NULL,
                        NULL, "pouch");
  }
  if (st.st_uid != geteuid() || (st.st_mode & 0777) != 0600) {
    close(file_fd);
    close(current_fd);
    lc_free_with_allocator(allocator, dir);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto key file must be owned by the current "
                        "user with mode 0600",
                        NULL, NULL, "pouch");
  }
  if (close(current_fd) != 0) {
    saved_errno = errno;
    close(file_fd);
    lc_free_with_allocator(allocator, dir);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch crypto key directory",
                        strerror(saved_errno), NULL, "pouch");
  }
  lc_free_with_allocator(allocator, dir);
  *fd_out = file_fd;
  return LC_OK;
#endif
}

static char *lc_pouch_crypto_temp_name(const char *name, unsigned int attempt) {
  char suffix[64];
  char *out;
  size_t name_len;
  size_t suffix_len;

  if (name == NULL || name[0] == '\0') {
    return NULL;
  }
  snprintf(suffix, sizeof(suffix), ".tmp.%ld.%u", (long)getpid(), attempt);
  name_len = strlen(name);
  suffix_len = strlen(suffix);
  out = (char *)lc_alloc_with_allocator(NULL, name_len + suffix_len + 1U);
  if (out == NULL) {
    return NULL;
  }
  memcpy(out, name, name_len);
  memcpy(out + name_len, suffix, suffix_len + 1U);
  return out;
}

static char *lc_pouch_b64url_encode(const unsigned char *bytes, size_t length) {
  char *out;
  size_t out_len;
  size_t src;
  size_t dst;

  out_len = ((length + 2U) / 3U) * 4U;
  while (out_len > 0U && (out_len * 3U) / 4U > length) {
    --out_len;
  }
  out = (char *)lc_alloc_with_allocator(NULL, out_len + 1U);
  if (out == NULL) {
    return NULL;
  }
  src = 0U;
  dst = 0U;
  while (src + 3U <= length) {
    unsigned long value;

    value = ((unsigned long)bytes[src] << 16) |
            ((unsigned long)bytes[src + 1U] << 8) | bytes[src + 2U];
    out[dst++] = lc_pouch_b64url_alphabet[(value >> 18) & 0x3FUL];
    out[dst++] = lc_pouch_b64url_alphabet[(value >> 12) & 0x3FUL];
    out[dst++] = lc_pouch_b64url_alphabet[(value >> 6) & 0x3FUL];
    out[dst++] = lc_pouch_b64url_alphabet[value & 0x3FUL];
    src += 3U;
  }
  if (src < length) {
    unsigned long value;

    value = (unsigned long)bytes[src] << 16;
    if (src + 1U < length) {
      value |= (unsigned long)bytes[src + 1U] << 8;
    }
    out[dst++] = lc_pouch_b64url_alphabet[(value >> 18) & 0x3FUL];
    out[dst++] = lc_pouch_b64url_alphabet[(value >> 12) & 0x3FUL];
    if (src + 1U < length) {
      out[dst++] = lc_pouch_b64url_alphabet[(value >> 6) & 0x3FUL];
    }
  }
  out[dst] = '\0';
  return out;
}

/* Keys and descriptors have fixed decoded bounds. Decode metadata directly
 * into caller-owned scratch instead of allocating and searching the alphabet
 * for every encoded byte. This never handles document payloads. */
static int lc_pouch_b64url_decode(const char *text, unsigned char *out,
                                  size_t capacity, size_t *out_len,
                                  lc_error *error) {
  size_t length, needed, src, dst;
  unsigned int a, b, c, d;
  unsigned long value;
  if (text == NULL || out == NULL || out_len == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch base64url decode requires inputs", NULL, NULL,
                        "pouch");
  }
  *out_len = 0U;
  length = strlen(text);
  if (length == 0U || length % 4U == 1U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch base64url value is invalid", NULL, NULL,
                        "pouch");
  }
  needed = (length / 4U) * 3U;
  if (length % 4U != 0U)
    needed += length % 4U - 1U;
  if (needed > capacity)
    goto invalid;
  src = dst = 0U;
  while (length - src >= 4U) {
    a = lc_pouch_b64url_values[(unsigned char)text[src]];
    b = lc_pouch_b64url_values[(unsigned char)text[src + 1U]];
    c = lc_pouch_b64url_values[(unsigned char)text[src + 2U]];
    d = lc_pouch_b64url_values[(unsigned char)text[src + 3U]];
    if ((a | b | c | d) & 0x80U)
      goto invalid;
    value = ((unsigned long)a << 18) | ((unsigned long)b << 12) |
            ((unsigned long)c << 6) | (unsigned long)d;
    out[dst++] = (unsigned char)(value >> 16);
    out[dst++] = (unsigned char)(value >> 8);
    out[dst++] = (unsigned char)value;
    src += 4U;
  }
  if (src < length) {
    a = lc_pouch_b64url_values[(unsigned char)text[src]];
    b = lc_pouch_b64url_values[(unsigned char)text[src + 1U]];
    c = length - src == 3U
            ? lc_pouch_b64url_values[(unsigned char)text[src + 2U]]
            : 0U;
    if ((a | b | c) & 0x80U)
      goto invalid;
    out[dst++] = (unsigned char)((a << 2) | (b >> 4));
    if (length - src == 3U)
      out[dst++] = (unsigned char)((b << 4) | (c >> 2));
  }
  *out_len = dst;
  return LC_OK;
invalid:
  OPENSSL_cleanse(out, capacity);
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch base64url value is invalid or exceeds its bound",
                      NULL, NULL, "pouch");
}

int lc_pouch_crypto_generate_key_string(char **out, lc_error *error) {
  unsigned char key[LC_POUCH_ROOT_KEY_BYTES];
  char *encoded;
  char *result;
  size_t prefix_len;
  size_t encoded_len;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_crypto_generate_key_string requires out",
                        NULL, NULL, "pouch");
  }
  *out = NULL;
  if (!lc_pouch_crypto_random_bytes(key, sizeof(key))) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to generate pouch crypto root key", NULL, NULL,
                        "pouch");
  }
  encoded = lc_pouch_b64url_encode(key, sizeof(key));
  OPENSSL_cleanse(key, sizeof(key));
  if (encoded == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to encode pouch crypto root key", NULL, NULL,
                        "pouch");
  }
  prefix_len = strlen(LC_POUCH_KEY_PREFIX);
  encoded_len = strlen(encoded);
  result = (char *)lc_alloc_with_allocator(NULL, prefix_len + encoded_len + 1U);
  if (result == NULL) {
    lc_pouch_crypto_key_string_free(encoded);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto root key string", NULL,
                        NULL, "pouch");
  }
  memcpy(result, LC_POUCH_KEY_PREFIX, prefix_len);
  memcpy(result + prefix_len, encoded, encoded_len + 1U);
  lc_pouch_crypto_key_string_free(encoded);
  *out = result;
  return LC_OK;
}

void lc_pouch_crypto_key_string_free(char *key_string) {
  if (key_string == NULL) {
    return;
  }
  OPENSSL_cleanse(key_string, strlen(key_string));
  lc_free_with_allocator(NULL, key_string);
}

int lc_pouch_crypto_default_key_file(char **out, lc_error *error) {
  const char *xdg;
  const char *home;
  const char *prefix;
  const char *suffix;
  char *path;
  size_t len;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_crypto_default_key_file requires out", NULL,
                        NULL, "pouch");
  }
  *out = NULL;
  xdg = getenv("XDG_CONFIG_HOME");
  suffix = "/liblockdc/pouch.key";
  if (xdg != NULL && xdg[0] != '\0') {
    prefix = xdg;
    len = strlen(prefix) + strlen(suffix) + 1U;
    path = (char *)lc_alloc_with_allocator(NULL, len);
    if (path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch crypto key path", NULL,
                          NULL, "pouch");
    }
    snprintf(path, len, "%s%s", prefix, suffix);
    *out = path;
    return LC_OK;
  }
  home = getenv("HOME");
  if (home == NULL || home[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "HOME or XDG_CONFIG_HOME is required for default pouch "
                        "crypto key file",
                        NULL, NULL, "pouch");
  }
  suffix = "/.config/liblockdc/pouch.key";
  len = strlen(home) + strlen(suffix) + 1U;
  path = (char *)lc_alloc_with_allocator(NULL, len);
  if (path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto key path", NULL, NULL,
                        "pouch");
  }
  snprintf(path, len, "%s%s", home, suffix);
  *out = path;
  return LC_OK;
}

static int lc_pouch_crypto_generate_key_file_status(const char *path,
                                                    int overwrite,
                                                    char **key_string_out,
                                                    int *already_exists,
                                                    lc_error *error) {
  char *key;
  char *dir;
  char *tmp_name;
  const char *name;
  unsigned int attempt;
  int dir_fd;
  int fd;
  int temp_exists;
  size_t len;
  int rc;

  if (already_exists != NULL) {
    *already_exists = 0;
  }
  if (path == NULL || path[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_crypto_generate_key_file requires path", NULL,
                        NULL, "pouch");
  }
  if (key_string_out != NULL) {
    *key_string_out = NULL;
  }
  name = lc_pouch_crypto_basename(path);
  if (name == NULL || name[0] == '\0' || strcmp(name, ".") == 0 ||
      strcmp(name, "..") == 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto key file path must name a file", NULL,
                        NULL, "pouch");
  }
  key = NULL;
  tmp_name = NULL;
  dir_fd = -1;
  temp_exists = 0;
  rc = lc_pouch_crypto_generate_key_string(&key, error);
  if (rc != LC_OK) {
    return rc;
  }
  dir = lc_pouch_crypto_dirname(NULL, path);
  if (dir == NULL) {
    lc_pouch_crypto_key_string_free(key);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto key directory", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_crypto_mkdirs_open(NULL, dir, &dir_fd, error);
  lc_free_with_allocator(NULL, dir);
  if (rc != LC_OK) {
    lc_pouch_crypto_key_string_free(key);
    return rc;
  }
  fd = -1;
  for (attempt = 0U; attempt < 100U; ++attempt) {
    tmp_name = lc_pouch_crypto_temp_name(name, attempt);
    if (tmp_name == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto key temp path", NULL,
                        NULL, "pouch");
      break;
    }
    fd = openat(dir_fd, tmp_name, O_CREAT | O_EXCL | O_NOFOLLOW | O_WRONLY,
                0600);
    if (fd >= 0 || errno != EEXIST) {
      break;
    }
    lc_free_with_allocator(NULL, tmp_name);
    tmp_name = NULL;
  }
  if (fd < 0) {
    if (rc == LC_OK) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch crypto key file",
                        strerror(errno), NULL, "pouch");
    }
    goto cleanup;
  }
  temp_exists = 1;
  if (fchmod(fd, 0600) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to set pouch crypto key file permissions",
                      strerror(errno), NULL, "pouch");
  }
  len = strlen(key);
  if (rc == LC_OK) {
    rc = lc_pouch_crypto_write_all_fd(fd, key, len, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_crypto_write_all_fd(fd, "\n", 1U, error);
  }
  if (rc == LC_OK && fsync(fd) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to fsync pouch crypto key file", strerror(errno),
                      NULL, "pouch");
  }
  if (close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch crypto key file", strerror(errno),
                      NULL, "pouch");
  }
  fd = -1;
  if (rc == LC_OK) {
    if (overwrite) {
      if (renameat(dir_fd, tmp_name, dir_fd, name) != 0) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to replace pouch crypto key file",
                          strerror(errno), NULL, "pouch");
      } else {
        temp_exists = 0;
      }
    } else {
      if (linkat(dir_fd, tmp_name, dir_fd, name, 0) != 0) {
        int saved_errno;

        saved_errno = errno;
        if (saved_errno == EEXIST && already_exists != NULL) {
          *already_exists = 1;
        }
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to create pouch crypto key file",
                          strerror(saved_errno), NULL, "pouch");
      } else if (unlinkat(dir_fd, tmp_name, 0) != 0) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to remove pouch crypto key temp file",
                          strerror(errno), NULL, "pouch");
      } else {
        temp_exists = 0;
      }
    }
  }
  if (rc == LC_OK) {
    if (fsync(dir_fd) != 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to fsync pouch crypto key directory",
                        strerror(errno), NULL, "pouch");
    }
  }
cleanup:
  if (fd >= 0) {
    close(fd);
  }
  if (temp_exists) {
    unlinkat(dir_fd, tmp_name, 0);
  }
  if (dir_fd >= 0 && close(dir_fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch crypto key directory",
                      strerror(errno), NULL, "pouch");
  }
  if (rc != LC_OK) {
    lc_free_with_allocator(NULL, tmp_name);
    lc_pouch_crypto_key_string_free(key);
    return rc;
  }
  lc_free_with_allocator(NULL, tmp_name);
  if (key_string_out != NULL) {
    *key_string_out = key;
  } else {
    lc_pouch_crypto_key_string_free(key);
  }
  return LC_OK;
}

int lc_pouch_crypto_generate_key_file(const char *path, int overwrite,
                                      char **key_string_out, lc_error *error) {
  return lc_pouch_crypto_generate_key_file_status(path, overwrite,
                                                  key_string_out, NULL, error);
}

#ifdef LOCKDC_TEST_BUILD
int lc_pouch_crypto_test_generate_key_file_status(const char *path,
                                                  int *already_exists,
                                                  lc_error *error) {
  return lc_pouch_crypto_generate_key_file_status(path, 0, NULL, already_exists,
                                                  error);
}
#endif

static int lc_pouch_crypto_parse_key(const char *key_string,
                                     unsigned char root_key[32],
                                     lc_error *error) {
  const char *encoded;
  unsigned char decoded[LC_POUCH_ROOT_KEY_BYTES];
  size_t decoded_len;
  int rc;

  if (key_string == NULL || strncmp(key_string, LC_POUCH_KEY_PREFIX,
                                    sizeof(LC_POUCH_KEY_PREFIX) - 1U) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto key must use lc-pouch-key-v1 format",
                        NULL, NULL, "pouch");
  }
  encoded = key_string + sizeof(LC_POUCH_KEY_PREFIX) - 1U;
  decoded_len = 0U;
  rc = lc_pouch_b64url_decode(encoded, decoded, sizeof(decoded), &decoded_len,
                              error);
  if (rc != LC_OK) {
    return rc;
  }
  if (decoded_len != LC_POUCH_ROOT_KEY_BYTES) {
    OPENSSL_cleanse(decoded, sizeof(decoded));
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto root key must be 32 bytes", NULL, NULL,
                        "pouch");
  }
  memcpy(root_key, decoded, LC_POUCH_ROOT_KEY_BYTES);
  OPENSSL_cleanse(decoded, decoded_len);
  return LC_OK;
}

int lc_pouch_crypto_open(const lc_allocator *allocator,
                         const lc_pouch_crypto_open_options *options,
                         lc_pouch_crypto **out, char **key_file_out,
                         lc_error *error) {
  lc_pouch_crypto *crypto;
  char *loaded_key;
  char *resolved_key_file;
  const char *key_string;
  int key_file_already_exists;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_crypto_open requires out", NULL, NULL,
                        "pouch");
  }
  *out = NULL;
  if (key_file_out != NULL) {
    *key_file_out = NULL;
  }
  if (options == NULL ||
      ((options->key_string == NULL || options->key_string[0] == '\0') &&
       (options->key_file == NULL || options->key_file[0] == '\0') &&
       !options->generate_key_file && !options->compression_enabled)) {
    return LC_OK;
  }
  if ((options->key_string == NULL || options->key_string[0] == '\0') &&
      (options->key_file == NULL || options->key_file[0] == '\0') &&
      !options->generate_key_file) {
    crypto = (lc_pouch_crypto *)lc_calloc_with_allocator(allocator, 1U,
                                                         sizeof(*crypto));
    if (crypto == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch transform provider", NULL,
                          NULL, "pouch");
    }
    if (allocator != NULL) {
      crypto->allocator = *allocator;
    } else {
      lc_allocator_init(&crypto->allocator);
    }
    crypto->compression_enabled = options->compression_enabled ? 1 : 0;
    *out = crypto;
    return LC_OK;
  }
  loaded_key = NULL;
  resolved_key_file = NULL;
  key_string = options->key_string;
  if (key_string == NULL || key_string[0] == '\0') {
    if (options->key_file != NULL && options->key_file[0] != '\0') {
      resolved_key_file = lc_pouch_crypto_strdup(allocator, options->key_file);
      if (resolved_key_file == NULL) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch crypto key file path",
                            NULL, NULL, "pouch");
      }
    } else {
      char *default_key_file;

      default_key_file = NULL;
      rc = lc_pouch_crypto_default_key_file(&default_key_file, error);
      if (rc != LC_OK) {
        return rc;
      }
      resolved_key_file = lc_pouch_crypto_strdup(allocator, default_key_file);
      lc_pouch_crypto_key_string_free(default_key_file);
      if (resolved_key_file == NULL) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch crypto key file path",
                            NULL, NULL, "pouch");
      }
    }
    rc = lc_pouch_crypto_read_all_file(NULL, resolved_key_file, &loaded_key,
                                       error);
    if (rc != LC_OK && options->generate_key_file) {
      lc_error_cleanup(error);
      key_file_already_exists = 0;
      rc = lc_pouch_crypto_generate_key_file_status(
          resolved_key_file, 0, &loaded_key, &key_file_already_exists, error);
      if (rc != LC_OK && key_file_already_exists) {
        lc_error_cleanup(error);
        rc = lc_pouch_crypto_read_all_file(NULL, resolved_key_file, &loaded_key,
                                           error);
      }
    }
    if (rc != LC_OK) {
      lc_free_with_allocator(allocator, resolved_key_file);
      return rc;
    }
    key_string = loaded_key;
  }
  crypto = (lc_pouch_crypto *)lc_calloc_with_allocator(allocator, 1U,
                                                       sizeof(*crypto));
  if (crypto == NULL) {
    lc_pouch_crypto_key_string_free(loaded_key);
    lc_free_with_allocator(allocator, resolved_key_file);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto provider", NULL, NULL,
                        "pouch");
  }
  if (allocator != NULL) {
    crypto->allocator = *allocator;
  } else {
    lc_allocator_init(&crypto->allocator);
  }
  crypto->encryption_enabled = 1;
  crypto->compression_enabled = options->compression_enabled ? 1 : 0;
  rc = lc_pouch_crypto_parse_key(key_string, crypto->root_key, error);
  lc_pouch_crypto_key_string_free(loaded_key);
  if (rc != LC_OK) {
    lc_pouch_crypto_close(crypto);
    lc_free_with_allocator(allocator, resolved_key_file);
    return rc;
  }
  rc = lc_pouch_crypto_derive_data_key(crypto, crypto->data_key, error);
  if (rc != LC_OK) {
    lc_pouch_crypto_close(crypto);
    lc_free_with_allocator(allocator, resolved_key_file);
    return rc;
  }
  *out = crypto;
  if (key_file_out != NULL) {
    *key_file_out = resolved_key_file;
  } else {
    lc_free_with_allocator(allocator, resolved_key_file);
  }
  return LC_OK;
}

void lc_pouch_crypto_close(lc_pouch_crypto *crypto) {
  lc_allocator allocator;

  if (crypto == NULL) {
    return;
  }
  allocator = crypto->allocator;
  OPENSSL_cleanse(crypto->root_key, sizeof(crypto->root_key));
  OPENSSL_cleanse(crypto->data_key, sizeof(crypto->data_key));
  lc_free_with_allocator(&allocator, crypto);
}

int lc_pouch_crypto_enabled(const lc_pouch_crypto *crypto) {
  return crypto != NULL && crypto->encryption_enabled;
}

int lc_pouch_crypto_compression_enabled(const lc_pouch_crypto *crypto) {
  return crypto != NULL && crypto->compression_enabled;
}

void lc_pouch_crypto_set_compression(lc_pouch_crypto *crypto, int enabled) {
  if (crypto != NULL)
    crypto->compression_enabled = enabled != 0;
}

int lc_pouch_crypto_key_id(const lc_pouch_crypto *crypto, char **out,
                           lc_error *error) {
  unsigned char digest[EVP_MAX_MD_SIZE];
  const char label[] = "liblockdc-pouch-root-key-id-v1";
  const char prefix[] = "hmac-sha256:";
  char *encoded;
  char *key_id;
  size_t digest_len;
  size_t encoded_len;
  size_t prefix_len;

  if (crypto == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto key id requires crypto and out", NULL,
                        NULL, "pouch");
  }
  *out = NULL;
  digest_len = 0U;
  if (EVP_Q_mac(NULL, "HMAC", NULL, "SHA256", NULL, crypto->root_key,
                sizeof(crypto->root_key), (const unsigned char *)label,
                sizeof(label) - 1U, digest, sizeof(digest),
                &digest_len) == NULL ||
      digest_len == 0U) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to derive pouch crypto key id", NULL, NULL,
                        "pouch");
  }
  encoded = lc_pouch_b64url_encode(digest, digest_len);
  OPENSSL_cleanse(digest, sizeof(digest));
  if (encoded == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto key id digest", NULL,
                        NULL, "pouch");
  }
  prefix_len = sizeof(prefix) - 1U;
  encoded_len = strlen(encoded);
  key_id = (char *)lc_alloc_with_allocator(NULL, prefix_len + encoded_len + 1U);
  if (key_id == NULL) {
    lc_free_with_allocator(NULL, encoded);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto key id", NULL, NULL,
                        "pouch");
  }
  memcpy(key_id, prefix, prefix_len);
  memcpy(key_id + prefix_len, encoded, encoded_len + 1U);
  lc_free_with_allocator(NULL, encoded);
  *out = key_id;
  return LC_OK;
}

static void lc_pouch_crypto_put32(unsigned char out[4], unsigned long value) {
  out[0] = (unsigned char)((value >> 24) & 0xFFU);
  out[1] = (unsigned char)((value >> 16) & 0xFFU);
  out[2] = (unsigned char)((value >> 8) & 0xFFU);
  out[3] = (unsigned char)(value & 0xFFU);
}

static unsigned long lc_pouch_crypto_get32(const unsigned char in[4]) {
  return ((unsigned long)in[0] << 24) | ((unsigned long)in[1] << 16) |
         ((unsigned long)in[2] << 8) | (unsigned long)in[3];
}

static int
lc_pouch_crypto_derive_data_key(const lc_pouch_crypto *crypto,
                                unsigned char key[LC_POUCH_DEK_BYTES],
                                lc_error *error) {
  const char label[] = "lc-pouch-aes-256-gcm-data-key";
  size_t out_len;

  if (crypto == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto data key derivation requires crypto",
                        NULL, NULL, "pouch");
  }
  out_len = 0U;
  if (EVP_Q_mac(NULL, "HMAC", NULL, "SHA256", NULL, crypto->root_key,
                sizeof(crypto->root_key), (const unsigned char *)label,
                sizeof(label) - 1U, key, LC_POUCH_DEK_BYTES,
                &out_len) == NULL ||
      out_len != LC_POUCH_DEK_BYTES) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to derive pouch crypto data key", NULL, NULL,
                        "pouch");
  }
  return LC_OK;
}

static int lc_pouch_crypto_derivation_open(const lc_pouch_crypto *crypto,
                                           EVP_MAC_CTX **out, lc_error *error) {
  EVP_MAC *mac;
  EVP_MAC_CTX *ctx;
  OSSL_PARAM params[2];
  char digest[] = "SHA256";
  *out = NULL;
  mac = EVP_MAC_fetch(NULL, "HMAC", NULL);
  ctx = mac != NULL ? EVP_MAC_CTX_new(mac) : NULL;
  EVP_MAC_free(mac);
  if (ctx == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch key derivation context", NULL,
                        NULL, "pouch");
  }
  params[0] =
      OSSL_PARAM_construct_utf8_string(OSSL_MAC_PARAM_DIGEST, digest, 0);
  params[1] = OSSL_PARAM_construct_end();
  if (EVP_MAC_init(ctx, crypto->data_key, sizeof(crypto->data_key), params) !=
      1) {
    EVP_MAC_CTX_free(ctx);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch key derivation context",
                        NULL, NULL, "pouch");
  }
  *out = ctx;
  return LC_OK;
}

/* Each operation owns a context keyed by its immutable root data key. Reset the
 * message for each domain-separated derivation without rebuilding that key's
 * HMAC state. Inputs match the one-shot writer and descriptor paths. */
static int lc_pouch_crypto_data_mac(const lc_pouch_crypto *crypto,
                                    EVP_MAC_CTX *ctx,
                                    const unsigned char *material,
                                    size_t length, unsigned char *key,
                                    size_t *out_len) {
  if (ctx == NULL)
    return EVP_Q_mac(NULL, "HMAC", NULL, "SHA256", NULL, crypto->data_key,
                     sizeof(crypto->data_key), material, length, key,
                     LC_POUCH_DEK_BYTES, out_len) != NULL;
  return EVP_MAC_init(ctx, NULL, 0, NULL) == 1 &&
         EVP_MAC_update(ctx, material, length) == 1 &&
         EVP_MAC_final(ctx, key, out_len, LC_POUCH_DEK_BYTES) == 1;
}

static int lc_pouch_crypto_derive_payload_key(
    const lc_pouch_crypto *crypto, const unsigned char salt[LC_POUCH_DEK_BYTES],
    unsigned char key[LC_POUCH_DEK_BYTES], EVP_MAC_CTX *ctx, lc_error *error) {
  static const char label[] = "lc-pouch-aes-256-gcm-payload-key";
  unsigned char material[sizeof(label) - 1U + LC_POUCH_DEK_BYTES];
  size_t out_len;

  if (crypto == NULL || salt == NULL || key == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto payload key derivation requires crypto, "
                        "salt, and output",
                        NULL, NULL, "pouch");
  }
  memcpy(material, label, sizeof(label) - 1U);
  memcpy(material + sizeof(label) - 1U, salt, LC_POUCH_DEK_BYTES);
  out_len = 0U;
  if (!lc_pouch_crypto_data_mac(crypto, ctx, material, sizeof(material), key,
                                &out_len) ||
      out_len != LC_POUCH_DEK_BYTES) {
    OPENSSL_cleanse(material, sizeof(material));
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to derive pouch payload key", NULL, NULL,
                        "pouch");
  }
  OPENSSL_cleanse(material, sizeof(material));
  return LC_OK;
}

static int lc_pouch_crypto_derive_binding_key(
    const lc_pouch_crypto *crypto, const unsigned char salt[LC_POUCH_DEK_BYTES],
    unsigned char key[LC_POUCH_DEK_BYTES], EVP_MAC_CTX *ctx, lc_error *error) {
  static const char label[] = "lc-pouch-aes-256-gcm-record-binding-key";
  unsigned char material[sizeof(label) - 1U + LC_POUCH_DEK_BYTES];
  size_t out_len;

  if (crypto == NULL || salt == NULL || key == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch record binding requires crypto, salt, and key",
                        NULL, NULL, "pouch");
  }
  memcpy(material, label, sizeof(label) - 1U);
  memcpy(material + sizeof(label) - 1U, salt, LC_POUCH_DEK_BYTES);
  out_len = 0U;
  if (!lc_pouch_crypto_data_mac(crypto, ctx, material, sizeof(material), key,
                                &out_len) ||
      out_len != LC_POUCH_DEK_BYTES) {
    OPENSSL_cleanse(material, sizeof(material));
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to derive pouch record binding key", NULL, NULL,
                        "pouch");
  }
  OPENSSL_cleanse(material, sizeof(material));
  return LC_OK;
}

static int lc_pouch_crypto_aad_lengths(const char *context, size_t context_len,
                                       size_t core_len,
                                       unsigned char lengths[8],
                                       lc_error *error) {
  if (context == NULL || context_len == 0U || context_len > 0xFFFFFFFFUL ||
      context_len > (size_t)INT_MAX || core_len > 0xFFFFFFFFUL ||
      core_len > (size_t)INT_MAX) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto authenticated context is invalid", NULL,
                        NULL, "pouch");
  }
  lc_pouch_crypto_put32(lengths, (unsigned long)context_len);
  lc_pouch_crypto_put32(lengths + 4U, (unsigned long)core_len);
  return LC_OK;
}

static int lc_pouch_crypto_binding_aad_encrypt(
    EVP_CIPHER_CTX *ctx, const char *context, size_t context_len,
    const unsigned char *core, size_t core_len, lc_error *error) {
  static const unsigned char domain[] = "lc-pouch-record-binding-v1";
  unsigned char lengths[8];
  int aad_len;
  int rc;

  rc = lc_pouch_crypto_aad_lengths(context, context_len, core_len, lengths,
                                   error);
  if (rc != LC_OK) {
    return rc;
  }
  if (EVP_EncryptUpdate(ctx, NULL, &aad_len, domain,
                        (int)(sizeof(domain) - 1U)) != 1 ||
      EVP_EncryptUpdate(ctx, NULL, &aad_len, lengths, sizeof(lengths)) != 1 ||
      EVP_EncryptUpdate(ctx, NULL, &aad_len, (const unsigned char *)context,
                        (int)context_len) != 1 ||
      EVP_EncryptUpdate(ctx, NULL, &aad_len, core, (int)core_len) != 1) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to authenticate pouch record metadata", NULL,
                        NULL, "pouch");
  }
  return LC_OK;
}

static int lc_pouch_crypto_binding_aad_decrypt(
    EVP_CIPHER_CTX *ctx, const char *context, size_t context_len,
    const unsigned char *core, size_t core_len, lc_error *error) {
  static const unsigned char domain[] = "lc-pouch-record-binding-v1";
  unsigned char lengths[8];
  int aad_len;
  int rc;

  rc = lc_pouch_crypto_aad_lengths(context, context_len, core_len, lengths,
                                   error);
  if (rc != LC_OK) {
    return rc;
  }
  if (EVP_DecryptUpdate(ctx, NULL, &aad_len, domain,
                        (int)(sizeof(domain) - 1U)) != 1 ||
      EVP_DecryptUpdate(ctx, NULL, &aad_len, lengths, sizeof(lengths)) != 1 ||
      EVP_DecryptUpdate(ctx, NULL, &aad_len, (const unsigned char *)context,
                        (int)context_len) != 1 ||
      EVP_DecryptUpdate(ctx, NULL, &aad_len, core, (int)core_len) != 1) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch record metadata authentication failed", NULL,
                        NULL, "pouch");
  }
  return LC_OK;
}

static int lc_pouch_crypto_desc_bind_reusing(
    lc_pouch_crypto *crypto, lc_pouch_crypto_desc *desc, const char *context,
    EVP_MAC_CTX *derivation, EVP_CIPHER_CTX *binding, lc_error *error) {
  unsigned char binding_key[LC_POUCH_DEK_BYTES];
  unsigned char core[LC_POUCH_DESC_RAW_BYTES_V2];
  EVP_CIPHER_CTX *ctx;
  size_t core_len;
  unsigned char final_byte[1];
  int final_len;
  int rc;

  if (crypto == NULL || desc == NULL || !desc->encrypted || context == NULL ||
      context[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted descriptor binding is invalid", NULL,
                        NULL, "pouch");
  }
  core_len = lc_pouch_crypto_descriptor_core_encode(desc, core);
  if (!lc_pouch_crypto_random_bytes(desc->binding_nonce,
                                    sizeof(desc->binding_nonce))) {
    OPENSSL_cleanse(core, sizeof(core));
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to generate pouch record binding nonce", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_crypto_derive_binding_key(crypto, desc->payload_key_salt,
                                          binding_key, derivation, error);
  ctx = binding;
  if (rc == LC_OK && ctx == NULL)
    ctx = EVP_CIPHER_CTX_new();
  if (rc == LC_OK && ctx == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch record binding context", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK &&
      (EVP_EncryptInit_ex(ctx, binding ? NULL : EVP_aes_256_gcm(), NULL,
                          binding_key, desc->binding_nonce) != 1 ||
       (rc = lc_pouch_crypto_binding_aad_encrypt(
            ctx, context, strlen(context), core, core_len, error)) != LC_OK ||
       EVP_EncryptFinal_ex(ctx, final_byte, &final_len) != 1 ||
       EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, LC_POUCH_GCM_TAG_BYTES,
                           desc->binding_tag) != 1)) {
    if (rc == LC_OK) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to authenticate pouch record metadata", NULL,
                        NULL, "pouch");
    }
  }
  if (binding == NULL)
    EVP_CIPHER_CTX_free(ctx);
  OPENSSL_cleanse(binding_key, sizeof(binding_key));
  OPENSSL_cleanse(core, sizeof(core));
  if (rc == LC_OK) {
    desc->binding_present = 1;
  }
  return rc;
}

static int lc_pouch_crypto_desc_bind(lc_pouch_crypto *crypto,
                                     lc_pouch_crypto_desc *desc,
                                     const char *context, lc_error *error) {
  return lc_pouch_crypto_desc_bind_reusing(crypto, desc, context, NULL, NULL,
                                           error);
}

static EVP_CIPHER_CTX *lc_pouch_crypto_decrypt_context_new(void);

static int lc_pouch_crypto_desc_verify_binding(lc_pouch_crypto *crypto,
                                               const lc_pouch_crypto_desc *desc,
                                               const char *context,
                                               EVP_MAC_CTX *derivation,
                                               EVP_CIPHER_CTX *binding,
                                               lc_error *error) {
  unsigned char binding_key[LC_POUCH_DEK_BYTES];
  unsigned char core[LC_POUCH_DESC_RAW_BYTES_V2];
  EVP_CIPHER_CTX *ctx;
  size_t core_len;
  unsigned char final_byte[1];
  int final_len;
  int rc;

  if (crypto == NULL || desc == NULL || !desc->encrypted ||
      !desc->binding_present || context == NULL || context[0] == '\0') {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch record metadata authentication is missing", NULL,
                        NULL, "pouch");
  }
  core_len = lc_pouch_crypto_descriptor_core_encode(desc, core);
  rc = lc_pouch_crypto_derive_binding_key(crypto, desc->payload_key_salt,
                                          binding_key, derivation, error);
  ctx = binding;
  if (rc == LC_OK && ctx == NULL)
    ctx = lc_pouch_crypto_decrypt_context_new();
  if (rc == LC_OK && ctx == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch record binding context", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK &&
      (EVP_DecryptInit_ex(ctx, NULL, NULL, binding_key, desc->binding_nonce) !=
           1 ||
       (rc = lc_pouch_crypto_binding_aad_decrypt(
            ctx, context, strlen(context), core, core_len, error)) != LC_OK ||
       EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, LC_POUCH_GCM_TAG_BYTES,
                           (void *)desc->binding_tag) != 1 ||
       EVP_DecryptFinal_ex(ctx, final_byte, &final_len) != 1)) {
    if (rc == LC_OK) {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch record metadata authentication failed", NULL,
                        NULL, "pouch");
    }
  }
  if (binding == NULL)
    EVP_CIPHER_CTX_free(ctx);
  OPENSSL_cleanse(binding_key, sizeof(binding_key));
  OPENSSL_cleanse(core, sizeof(core));
  return rc;
}

static size_t lc_pouch_crypto_descriptor_core_encode(
    const lc_pouch_crypto_desc *desc,
    unsigned char raw[LC_POUCH_DESC_RAW_BYTES_V2]) {
  size_t raw_len;

  memset(raw, 0, LC_POUCH_DESC_RAW_BYTES_V2);
  raw[0] = 0U;
  if (desc->encrypted) {
    raw[0] = (unsigned char)(raw[0] | LC_POUCH_DESC_FLAG_ENCRYPTED);
  }
  if (desc->compressed) {
    raw[0] = (unsigned char)(raw[0] | LC_POUCH_DESC_FLAG_COMPRESSED);
  }
  lc_pouch_crypto_put32(raw + 1U, desc->frame_size);
  memcpy(raw + 5U, desc->nonce_prefix, LC_POUCH_NONCE_PREFIX_BYTES);
  raw_len = LC_POUCH_DESC_RAW_BYTES_V1;
  if (desc->encrypted && desc->payload_key_derived) {
    memcpy(raw + LC_POUCH_DESC_RAW_BYTES_V1, desc->payload_key_salt,
           LC_POUCH_DEK_BYTES);
    raw_len = LC_POUCH_DESC_RAW_BYTES_V2;
  }
  return raw_len;
}

/* Descriptors have a fixed metadata bound. Encode directly into reusable
 * caller-owned storage; a codec unit must not allocate on every restart. */
static int
lc_pouch_crypto_descriptor_encode_into(const lc_pouch_crypto_desc *desc,
                                       char *out, size_t capacity,
                                       lc_error *error) {
  unsigned char raw[LC_POUCH_DESC_RAW_BYTES_V3];
  size_t prefix, raw_len, i, encoded;
  int n;
  memset(raw, 0, sizeof(raw));
  raw_len = lc_pouch_crypto_descriptor_core_encode(desc, raw);
  if (desc->binding_present) {
    memcpy(raw + LC_POUCH_DESC_RAW_BYTES_V2, desc->binding_nonce,
           LC_POUCH_DESC_BINDING_NONCE_BYTES);
    memcpy(raw + LC_POUCH_DESC_RAW_BYTES_V2 + LC_POUCH_DESC_BINDING_NONCE_BYTES,
           desc->binding_tag, LC_POUCH_GCM_TAG_BYTES);
    raw_len = LC_POUCH_DESC_RAW_BYTES_V3;
  }
  prefix = sizeof(LC_POUCH_DESC_PREFIX) - 1U;
  if (capacity < prefix + ((raw_len + 2U) / 3U) * 4U + 1U) {
    OPENSSL_cleanse(raw, sizeof(raw));
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch descriptor output is too small", NULL, NULL,
                        "pouch");
  }
  memcpy(out, LC_POUCH_DESC_PREFIX, prefix);
  n = EVP_EncodeBlock((unsigned char *)out + prefix, raw, (int)raw_len);
  OPENSSL_cleanse(raw, sizeof(raw));
  if (n < 0)
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to encode pouch crypto descriptor", NULL, NULL,
                        "pouch");
  encoded = (size_t)n;
  while (encoded && out[prefix + encoded - 1U] == '=')
    --encoded;
  for (i = 0; i < encoded; ++i) {
    if (out[prefix + i] == '+')
      out[prefix + i] = '-';
    else if (out[prefix + i] == '/')
      out[prefix + i] = '_';
  }
  out[prefix + encoded] = '\0';
  return LC_OK;
}

static int lc_pouch_crypto_descriptor_encode(const lc_allocator *allocator,
                                             const lc_pouch_crypto_desc *desc,
                                             char **out, lc_error *error) {
  char *result = lc_alloc_with_allocator(allocator, LC_POUCH_DESC_TEXT_BYTES);
  int rc;
  if (result == NULL)
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto descriptor", NULL,
                        NULL, "pouch");
  rc = lc_pouch_crypto_descriptor_encode_into(desc, result,
                                              LC_POUCH_DESC_TEXT_BYTES, error);
  if (rc == LC_OK)
    *out = result;
  else
    lc_free_with_allocator(allocator, result);
  return rc;
}

static int lc_pouch_crypto_descriptor_decode(const char *descriptor,
                                             lc_pouch_crypto_desc *out,
                                             lc_error *error) {
  const char *encoded;
  unsigned char raw[LC_POUCH_DESC_RAW_BYTES_V3];
  size_t raw_len;
  int rc;

  if (descriptor == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto descriptor is missing or invalid", NULL,
                        NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  if (strncmp(descriptor, LC_POUCH_DESC_PREFIX,
              sizeof(LC_POUCH_DESC_PREFIX) - 1U) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto descriptor is missing or invalid", NULL,
                        NULL, "pouch");
  }
  encoded = descriptor + sizeof(LC_POUCH_DESC_PREFIX) - 1U;
  raw_len = 0U;
  rc = lc_pouch_b64url_decode(encoded, raw, sizeof(raw), &raw_len, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (raw_len != LC_POUCH_DESC_RAW_BYTES_V1 &&
      raw_len != LC_POUCH_DESC_RAW_BYTES_V2 &&
      raw_len != LC_POUCH_DESC_RAW_BYTES_V3) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto descriptor has invalid length", NULL,
                        NULL, "pouch");
  }
  if ((raw[0] &
       ~(LC_POUCH_DESC_FLAG_ENCRYPTED | LC_POUCH_DESC_FLAG_COMPRESSED)) != 0U ||
      (raw[0] &
       (LC_POUCH_DESC_FLAG_ENCRYPTED | LC_POUCH_DESC_FLAG_COMPRESSED)) == 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto descriptor has invalid flags", NULL, NULL,
                        "pouch");
  }
  out->encrypted = (raw[0] & LC_POUCH_DESC_FLAG_ENCRYPTED) != 0U ? 1 : 0;
  out->compressed = (raw[0] & LC_POUCH_DESC_FLAG_COMPRESSED) != 0U ? 1 : 0;
  out->frame_size = lc_pouch_crypto_get32(raw + 1U);
  if (out->encrypted && (out->frame_size == 0UL ||
                         out->frame_size > LC_POUCH_FRAME_PLAINTEXT_BYTES)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto descriptor has invalid frame size", NULL,
                        NULL, "pouch");
  }
  if (!out->encrypted && out->frame_size != 0UL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto descriptor has invalid plaintext frame",
                        NULL, NULL, "pouch");
  }
  if (!out->encrypted && raw_len != LC_POUCH_DESC_RAW_BYTES_V1) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto descriptor has invalid plaintext key "
                        "material",
                        NULL, NULL, "pouch");
  }
  memcpy(out->nonce_prefix, raw + 5U, LC_POUCH_NONCE_PREFIX_BYTES);
  if (raw_len == LC_POUCH_DESC_RAW_BYTES_V2 ||
      raw_len == LC_POUCH_DESC_RAW_BYTES_V3) {
    out->payload_key_derived = 1;
    memcpy(out->payload_key_salt, raw + LC_POUCH_DESC_RAW_BYTES_V1,
           LC_POUCH_DEK_BYTES);
  }
  if (raw_len == LC_POUCH_DESC_RAW_BYTES_V3) {
    out->binding_present = 1;
    memcpy(out->binding_nonce, raw + LC_POUCH_DESC_RAW_BYTES_V2,
           LC_POUCH_DESC_BINDING_NONCE_BYTES);
    memcpy(out->binding_tag,
           raw + LC_POUCH_DESC_RAW_BYTES_V2 + LC_POUCH_DESC_BINDING_NONCE_BYTES,
           LC_POUCH_GCM_TAG_BYTES);
  }
  OPENSSL_cleanse(raw, raw_len);
  return LC_OK;
}

#ifdef LOCKDC_TEST_BUILD
int lc_pouch_crypto_test_descriptor_set_compressed(const char *descriptor,
                                                   int compressed, char **out,
                                                   lc_error *error) {
  lc_pouch_crypto_desc desc;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto descriptor test requires output", NULL,
                        NULL, "pouch");
  }
  *out = NULL;
  memset(&desc, 0, sizeof(desc));
  rc = lc_pouch_crypto_descriptor_decode(descriptor, &desc, error);
  if (rc != LC_OK) {
    return rc;
  }
  desc.compressed = compressed != 0;
  return lc_pouch_crypto_descriptor_encode(NULL, &desc, out, error);
}
#endif

static void lc_pouch_crypto_nonce(const unsigned char prefix[8],
                                  unsigned long counter,
                                  unsigned char nonce[12]) {
  memcpy(nonce, prefix, LC_POUCH_NONCE_PREFIX_BYTES);
  lc_pouch_crypto_put32(nonce + LC_POUCH_NONCE_PREFIX_BYTES, counter);
}

static int lc_pouch_crypto_check_byte_counter(uint64_t total, size_t delta,
                                              const char *message,
                                              lc_error *error) {
  if (total > LC_U64_MAX - (uint64_t)delta) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL,
                        "pouch");
  }
  return LC_OK;
}

static int lc_pouch_crypto_frame_aad_encrypt(
    EVP_CIPHER_CTX *ctx, const char *context, size_t context_len,
    const unsigned char *core, size_t core_len, const unsigned char header[8],
    lc_error *error) {
  static const unsigned char domain[] = "lc-pouch-payload-frame-v2";
  unsigned char lengths[8];
  int aad_len;
  int rc;

  rc = lc_pouch_crypto_aad_lengths(context, context_len, core_len, lengths,
                                   error);
  if (rc != LC_OK) {
    return rc;
  }
  if (EVP_EncryptUpdate(ctx, NULL, &aad_len, domain,
                        (int)(sizeof(domain) - 1U)) != 1 ||
      EVP_EncryptUpdate(ctx, NULL, &aad_len, lengths, sizeof(lengths)) != 1 ||
      EVP_EncryptUpdate(ctx, NULL, &aad_len, (const unsigned char *)context,
                        (int)context_len) != 1 ||
      EVP_EncryptUpdate(ctx, NULL, &aad_len, core, (int)core_len) != 1 ||
      EVP_EncryptUpdate(ctx, NULL, &aad_len, header, 8) != 1) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to authenticate pouch payload frame", NULL,
                        NULL, "pouch");
  }
  return LC_OK;
}

static int lc_pouch_crypto_frame_aad_decrypt(
    EVP_CIPHER_CTX *ctx, const char *context, size_t context_len,
    const unsigned char *core, size_t core_len, const unsigned char header[8],
    lc_error *error) {
  static const unsigned char domain[] = "lc-pouch-payload-frame-v2";
  unsigned char lengths[8];
  int aad_len;
  int rc;

  rc = lc_pouch_crypto_aad_lengths(context, context_len, core_len, lengths,
                                   error);
  if (rc != LC_OK) {
    return rc;
  }
  if (EVP_DecryptUpdate(ctx, NULL, &aad_len, domain,
                        (int)(sizeof(domain) - 1U)) != 1 ||
      EVP_DecryptUpdate(ctx, NULL, &aad_len, lengths, sizeof(lengths)) != 1 ||
      EVP_DecryptUpdate(ctx, NULL, &aad_len, (const unsigned char *)context,
                        (int)context_len) != 1 ||
      EVP_DecryptUpdate(ctx, NULL, &aad_len, core, (int)core_len) != 1 ||
      EVP_DecryptUpdate(ctx, NULL, &aad_len, header, 8) != 1) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "encrypted pouch payload authentication failed", NULL,
                        NULL, "pouch");
  }
  return LC_OK;
}

#ifdef LOCKDC_TEST_BUILD
int lc_pouch_crypto_test_check_byte_counter(uint64_t total, size_t delta,
                                            lc_error *error) {
  return lc_pouch_crypto_check_byte_counter(
      total, delta, "pouch payload byte count exceeds platform limit", error);
}
#endif

static int lc_pouch_crypto_encrypt_frame_to_memory(
    EVP_CIPHER_CTX *ctx, const unsigned char key[LC_POUCH_DEK_BYTES],
    const unsigned char nonce_prefix[LC_POUCH_NONCE_PREFIX_BYTES],
    const char *context, size_t context_len, const unsigned char *core,
    size_t core_len, unsigned long counter, int set_key,
    const unsigned char *plain, size_t plain_len, unsigned char *frame,
    size_t frame_capacity, size_t *frame_length_out, lc_error *error) {
  unsigned char *header;
  unsigned char *cipher;
  unsigned char *tag;
  unsigned char nonce[LC_POUCH_NONCE_BYTES];
  size_t cipher_len;
  size_t frame_len;
  int out_len;
  int final_len;
  int rc;

  if (ctx == NULL || (set_key && key == NULL) || nonce_prefix == NULL ||
      (plain_len > 0U && plain == NULL) || frame == NULL ||
      frame_length_out == NULL || plain_len > LC_POUCH_FRAME_PLAINTEXT_BYTES ||
      frame_capacity < 8U + plain_len + LC_POUCH_GCM_TAG_BYTES) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted frame arguments are invalid", NULL,
                        NULL, "pouch");
  }

  header = frame;
  cipher = frame + 8U;
  lc_pouch_crypto_put32(header, counter);
  lc_pouch_crypto_put32(header + 4U, (unsigned long)plain_len);
  lc_pouch_crypto_nonce(nonce_prefix, counter, nonce);
  /* The first frame installs the DEK; later frames retain it but always get a
   * distinct nonce and complete authenticated GCM operation. */
  rc = EVP_EncryptInit_ex(ctx, NULL, NULL, set_key ? key : NULL, nonce) == 1
           ? lc_pouch_crypto_frame_aad_encrypt(ctx, context, context_len, core,
                                               core_len, header, error)
           : LC_ERR_TRANSPORT;
  if (rc != LC_OK ||
      EVP_EncryptUpdate(ctx, cipher, &out_len, plain, (int)plain_len) != 1 ||
      EVP_EncryptFinal_ex(ctx, cipher + out_len, &final_len) != 1) {
    OPENSSL_cleanse(nonce, sizeof(nonce));
    if (rc != LC_OK && error != NULL && error->code != LC_OK) {
      return rc;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to encrypt pouch payload frame", NULL, NULL,
                        "pouch");
  }
  cipher_len = (size_t)(out_len + final_len);
  tag = cipher + cipher_len;
  if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, LC_POUCH_GCM_TAG_BYTES,
                          tag) != 1) {
    OPENSSL_cleanse(nonce, sizeof(nonce));
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to encrypt pouch payload frame", NULL, NULL,
                        "pouch");
  }
  frame_len = 8U + cipher_len + LC_POUCH_GCM_TAG_BYTES;
  *frame_length_out = frame_len;
  OPENSSL_cleanse(nonce, sizeof(nonce));
  return LC_OK;
}

/* The cipher and nonce width are invariant for every frame in one payload.
 * Reusing that setup preserves the framed format while avoiding an OpenSSL
 * provider lookup for each independently authenticated frame. */
static int lc_pouch_crypto_encrypt_context_begin(EVP_CIPHER_CTX *ctx,
                                                 lc_error *error) {
  if (ctx == NULL ||
      EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), NULL, NULL, NULL) != 1 ||
      EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, LC_POUCH_NONCE_BYTES,
                          NULL) != 1) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch payload cipher", NULL, NULL,
                        "pouch");
  }
  return LC_OK;
}

static int lc_pouch_crypto_encrypt_frame(
    int fd, lc_sink *destination, EVP_CIPHER_CTX *ctx,
    const unsigned char key[LC_POUCH_DEK_BYTES],
    const unsigned char nonce_prefix[LC_POUCH_NONCE_PREFIX_BYTES],
    const char *context, size_t context_len, const unsigned char *core,
    size_t core_len, unsigned long counter, int set_key,
    const unsigned char *plain, size_t plain_len, int terminal_after,
    uint64_t *cipher_total, unsigned long *stored_crc, unsigned char *frame,
    size_t frame_capacity, lc_error *error) {
  size_t frame_len, terminal_len;
  int rc;

  if (terminal_after && counter >= 0xFFFFFFFEUL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted payload exceeds frame counter limit",
                        NULL, NULL, "pouch");
  frame_len = 0U;
  rc = lc_pouch_crypto_encrypt_frame_to_memory(
      ctx, key, nonce_prefix, context, context_len, core, core_len, counter,
      set_key, plain, plain_len, frame, frame_capacity, &frame_len, error);
  if (rc != LC_OK) {
    return rc;
  }
  /* At EOF the residual frame and terminal fit one bounded writer buffer.
   * Full producer frames continue to flow immediately, before EOF. */
  if (terminal_after) {
    terminal_len = 0;
    rc = lc_pouch_crypto_encrypt_frame_to_memory(
        ctx, key, nonce_prefix, context, context_len, core, core_len,
        counter + 1UL, 0, NULL, 0, frame + frame_len,
        frame_capacity - frame_len, &terminal_len, error);
    if (rc != LC_OK)
      return rc;
    frame_len += terminal_len;
  }
  if (cipher_total != NULL) {
    rc = lc_pouch_crypto_check_byte_counter(
        *cipher_total, frame_len,
        "pouch encrypted payload byte count exceeds platform limit", error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  if (stored_crc != NULL) {
    *stored_crc =
        (unsigned long)crc32((uLong)*stored_crc, frame, (uInt)frame_len);
  }
  rc = destination == NULL
           ? lc_pouch_crypto_write_all_fd(fd, frame, frame_len, error)
       : destination->write(destination, frame, frame_len, error) ? LC_OK
       : error != NULL && error->code != LC_OK ? error->code
                                               : LC_ERR_TRANSPORT;
  if (rc == LC_OK && cipher_total != NULL) {
    *cipher_total += (uint64_t)frame_len;
  }
  return rc;
}

static int lc_pouch_crypto_stream_plain_to_fd(const lc_allocator *allocator,
                                              int fd, lc_source *body,
                                              uint64_t *plain_bytes,
                                              uint64_t *cipher_bytes,
                                              unsigned long *stored_crc,
                                              lc_error *error) {
  unsigned char *buffer;
  uint64_t total;
  int rc;

  if (fd < 0 || body == NULL || plain_bytes == NULL || cipher_bytes == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch plaintext stream write requires fd, body, and "
                        "byte outputs",
                        NULL, NULL, "pouch");
  }
  /* Source callbacks may run on small pthread stacks. Keep this bounded
   * streaming scratch outside that stack while preserving direct flow. */
  buffer = (unsigned char *)lc_alloc_with_allocator(
      allocator, LC_POUCH_FRAME_PLAINTEXT_BYTES);
  if (buffer == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch streaming buffer", NULL, NULL,
                        "pouch");
  }
  total = 0UL;
  rc = LC_OK;
  for (;;) {
    size_t got;

    got = body->read(body, buffer, LC_POUCH_FRAME_PLAINTEXT_BYTES, error);
    if (got == 0U) {
      if (error != NULL && error->code != LC_OK) {
        rc = error->code;
      }
      break;
    }
    rc = lc_pouch_crypto_check_byte_counter(
        total, got, "pouch payload byte count exceeds platform limit", error);
    if (rc != LC_OK) {
      break;
    }
    rc = lc_pouch_crypto_write_all_fd(fd, buffer, got, error);
    if (rc != LC_OK) {
      break;
    }
    if (stored_crc != NULL) {
      *stored_crc = (unsigned long)crc32((uLong)*stored_crc, buffer, (uInt)got);
    }
    total += (uint64_t)got;
  }
  OPENSSL_cleanse(buffer, LC_POUCH_FRAME_PLAINTEXT_BYTES);
  lc_free_with_allocator(allocator, buffer);
  if (rc == LC_OK) {
    *plain_bytes = total;
    *cipher_bytes = total;
  }
  return rc;
}

static size_t lc_pouch_zlib_source_read(lc_source *self, void *buffer,
                                        size_t count, lc_error *error) {
  lc_pouch_zlib_source *source;
  size_t total;

  if (self == NULL || buffer == NULL || count == 0U) {
    return 0U;
  }
  source = (lc_pouch_zlib_source *)self->impl;
  if (source == NULL || source->stream_done) {
    return 0U;
  }
  total = 0U;
  while (total < count && !source->stream_done) {
    size_t output_chunk;
    size_t produced;

    output_chunk = count - total;
    if (output_chunk > (size_t)UINT_MAX) {
      output_chunk = (size_t)UINT_MAX;
    }
    source->stream.next_out = (unsigned char *)buffer + total;
    source->stream.avail_out = (uInt)output_chunk;
    while (source->stream.avail_out > 0U && !source->stream_done) {
      int zrc;

      if (source->stream.avail_in == 0U && !source->input_done) {
        size_t got;

        got = source->inner->read(source->inner, source->input,
                                  sizeof(source->input), error);
        if (got == 0U) {
          if (error != NULL && error->code != LC_OK) {
            return total + output_chunk - source->stream.avail_out;
          }
          source->input_done = 1;
        } else {
          if (error != NULL && error->code != LC_OK) {
            return total + output_chunk - source->stream.avail_out;
          }
          if (lc_pouch_crypto_check_byte_counter(
                  source->input_total, got,
                  "pouch compressed plaintext byte count exceeds platform "
                  "limit",
                  error) != LC_OK) {
            return total + output_chunk - source->stream.avail_out;
          }
          source->input_total += (uint64_t)got;
          source->stream.next_in = source->input;
          source->stream.avail_in = (uInt)got;
        }
      }
      zrc = inflate(&source->stream, Z_NO_FLUSH);
      if (zrc == Z_STREAM_END) {
        size_t got;

        if (source->stream.avail_in != 0U) {
          (void)lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                             "compressed pouch payload has trailing bytes",
                             NULL, NULL, "pouch");
          return total + output_chunk - source->stream.avail_out;
        }
        /* Do not expose a completed compressed stream before the wrapped
         * source has reached EOF. In particular, encrypted sources only
         * authenticate their terminal zero-length frame while being drained.
         */
        got = source->inner->read(source->inner, source->input,
                                  sizeof(source->input), error);
        if (error != NULL && error->code != LC_OK) {
          return total + output_chunk - source->stream.avail_out;
        }
        if (got != 0U) {
          (void)lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                             "compressed pouch payload has trailing bytes",
                             NULL, NULL, "pouch");
          return total + output_chunk - source->stream.avail_out;
        }
        source->input_done = 1;
        source->stream_done = 1;
        break;
      }
      if (zrc == Z_BUF_ERROR && source->input_done &&
          source->stream.avail_in == 0U) {
        (void)lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                           "compressed pouch payload is truncated", NULL, NULL,
                           "pouch");
        return total + output_chunk - source->stream.avail_out;
      }
      if (zrc != Z_OK && zrc != Z_BUF_ERROR) {
        (void)lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                           "failed to decompress pouch payload", NULL, NULL,
                           "pouch");
        return total + output_chunk - source->stream.avail_out;
      }
      if (zrc == Z_BUF_ERROR &&
          source->stream.avail_out == (uInt)output_chunk) {
        break;
      }
    }
    produced = output_chunk - source->stream.avail_out;
    total += produced;
    if (produced == 0U && !source->stream_done) {
      break;
    }
  }
  return total;
}

static int lc_pouch_zlib_source_reset(lc_source *self, lc_error *error) {
  lc_pouch_zlib_source *source;
  int zrc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch zlib source reset requires source", NULL, NULL,
                        "pouch");
  }
  source = (lc_pouch_zlib_source *)self->impl;
  if (source == NULL || source->inner == NULL || source->inner->reset == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch zlib source is not resettable", NULL, NULL,
                        "pouch");
  }
  if (source->inner->reset(source->inner, error) != LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
  }
  zrc = inflateReset(&source->stream);
  if (zrc != Z_OK) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "failed to reset pouch zlib stream", NULL, NULL,
                        "pouch");
  }
  source->stream.next_in = NULL;
  source->stream.avail_in = 0U;
  source->input_total = 0UL;
  source->input_done = 0;
  source->stream_done = 0;
  return LC_OK;
}

static void lc_pouch_zlib_source_close(lc_source *self) {
  lc_pouch_zlib_source *source;

  if (self == NULL) {
    return;
  }
  source = (lc_pouch_zlib_source *)self->impl;
  if (source == NULL) {
    return;
  }
  (void)inflateEnd(&source->stream);
  if (source->close_inner && source->inner != NULL &&
      source->inner->close != NULL) {
    source->inner->close(source->inner);
  }
  lc_free_with_allocator(&source->allocator, source);
}

static int lc_pouch_zlib_source_open(const lc_allocator *allocator,
                                     lc_source *inner, int close_inner,
                                     lc_pouch_zlib_source **out,
                                     lc_error *error) {
  lc_pouch_zlib_source *source;
  int zrc;

  if (inner == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch zlib source requires inner source and output",
                        NULL, NULL, "pouch");
  }
  *out = NULL;
  source = (lc_pouch_zlib_source *)lc_calloc_with_allocator(allocator, 1U,
                                                            sizeof(*source));
  if (source == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch zlib source", NULL, NULL,
                        "pouch");
  }
  if (allocator != NULL) {
    source->allocator = *allocator;
  } else {
    lc_allocator_init(&source->allocator);
  }
  source->inner = inner;
  source->close_inner = close_inner ? 1 : 0;
  zrc = inflateInit(&source->stream);
  if (zrc != Z_OK) {
    lc_free_with_allocator(&source->allocator, source);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize pouch zlib stream", NULL, NULL,
                        "pouch");
  }
  source->pub.read = lc_pouch_zlib_source_read;
  source->pub.reset = lc_pouch_zlib_source_reset;
  source->pub.close = lc_pouch_zlib_source_close;
  source->pub.impl = source;
  *out = source;
  return LC_OK;
}

typedef struct lc_pouch_crypto_output {
  lc_sink pub;
  const lc_allocator *allocator;
  lc_pouch_crypto *crypto;
  const char *context;
  int fd;
  lc_sink *destination;
  lc_pouch_crypto_desc desc;
  EVP_CIPHER_CTX *cipher;
  EVP_CIPHER_CTX *binding;
  EVP_MAC_CTX *derivation;
  z_stream deflater;
  unsigned char key[LC_POUCH_DEK_BYTES];
  unsigned char core[LC_POUCH_DESC_RAW_BYTES_V2];
  size_t core_length;
  /* Encrypted output uses frame + 8 as its plaintext workspace. The GCM
   * update encrypts exactly in place; frame owns the complete allocation. */
  unsigned char *plain;
  unsigned char *frame;
  unsigned char small[2048];
  size_t fill;
  unsigned long counter;
  uint64_t plain_total;
  uint64_t cipher_total;
  unsigned long crc;
  int zlib_initialized;
  int failed;
  int finished;
  int borrowed_finished;
  char *descriptor;
} lc_pouch_crypto_output;

static int lc_pouch_crypto_output_flush(lc_pouch_crypto_output *out,
                                        int terminal, lc_error *error) {
  int rc;
  if (out->desc.encrypted) {
    if (out->counter == 0xFFFFFFFFUL) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch encrypted payload exceeds frame counter limit",
                          NULL, NULL, "pouch");
    }
    rc = lc_pouch_crypto_encrypt_frame(
        out->fd, out->destination, out->cipher, out->key,
        out->desc.nonce_prefix, out->context, strlen(out->context), out->core,
        out->core_length, out->counter, out->counter == 0UL, out->plain,
        out->fill, terminal && out->fill != 0U, &out->cipher_total, &out->crc,
        out->frame, LC_POUCH_WRITER_FRAME_BYTES, error);
    if (rc == LC_OK) {
      out->counter += terminal && out->fill != 0U ? 2UL : 1UL;
    }
  } else {
    rc = lc_pouch_crypto_check_byte_counter(
        out->cipher_total, out->fill,
        "pouch stored byte count exceeds platform limit", error);
    if (rc == LC_OK) {
      rc = out->destination == NULL ? lc_pouch_crypto_write_all_fd(
                                          out->fd, out->plain, out->fill, error)
           : out->destination->write(out->destination, out->plain, out->fill,
                                     error)
               ? LC_OK
           : error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_TRANSPORT;
    }
    if (rc == LC_OK) {
      out->crc =
          (unsigned long)crc32((uLong)out->crc, out->plain, (uInt)out->fill);
      out->cipher_total += out->fill;
    }
  }
  out->fill = 0U;
  return rc;
}

static int lc_pouch_crypto_output_feed(lc_pouch_crypto_output *out,
                                       const unsigned char *bytes, size_t count,
                                       lc_error *error) {
  size_t target;
  size_t take;
  unsigned char *grown;
  int rc;
  /* Plain short writes need no frame allocation. Grow before overflowing the
   * inline chunk, preserving the existing 64 KiB write cadence for long bodies.
   * This storage belongs only to this writer, never to a retained cache. */
  if (out->plain == out->small && count > sizeof(out->small) - out->fill) {
    grown =
        lc_alloc_with_allocator(out->allocator, LC_POUCH_FRAME_PLAINTEXT_BYTES);
    if (grown == NULL)
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to grow pouch streaming buffer", NULL, NULL,
                          "pouch");
    memcpy(grown, out->small, out->fill);
    out->plain = grown;
  }
  while (count != 0U) {
    target = out->desc.encrypted && out->counter == 0UL
                 ? LC_POUCH_FIRST_FRAME_PLAINTEXT_BYTES
                 : LC_POUCH_FRAME_PLAINTEXT_BYTES;
    /* Keep an exactly full final frame until more input proves it is not
     * final. Commit can then emit its bounded bytes and terminal together. */
    if (out->fill == target) {
      rc = lc_pouch_crypto_output_flush(out, 0, error);
      if (rc != LC_OK)
        return rc;
      continue;
    }
    take = target - out->fill;
    if (take > count) {
      take = count;
    }
    memcpy(out->plain + out->fill, bytes, take);
    out->fill += take;
    bytes += take;
    count -= take;
  }
  return LC_OK;
}

static int lc_pouch_crypto_output_deflate(lc_pouch_crypto_output *out,
                                          int finish, lc_error *error) {
  size_t target;
  size_t produced;
  int zrc;
  int rc;
  do {
    target = out->desc.encrypted && out->counter == 0UL
                 ? LC_POUCH_FIRST_FRAME_PLAINTEXT_BYTES
                 : LC_POUCH_FRAME_PLAINTEXT_BYTES;
    out->deflater.next_out = out->plain + out->fill;
    out->deflater.avail_out = (uInt)(target - out->fill);
    zrc = deflate(&out->deflater, finish ? Z_FINISH : Z_NO_FLUSH);
    if (zrc == Z_BUF_ERROR && !finish && out->deflater.avail_in == 0U) {
      return LC_OK;
    }
    if (zrc != Z_OK && zrc != Z_STREAM_END) {
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "failed to compress pouch payload", NULL, NULL,
                          "pouch");
    }
    produced = target - out->fill - out->deflater.avail_out;
    out->fill += produced;
    if (out->fill == target) {
      rc = lc_pouch_crypto_output_flush(out, 0, error);
      if (rc != LC_OK) {
        return rc;
      }
    }
  } while (out->deflater.avail_in != 0U || (finish && zrc != Z_STREAM_END) ||
           (!finish && out->deflater.avail_out == 0U));
  return LC_OK;
}

static int lc_pouch_crypto_output_write(lc_sink *sink, const void *bytes,
                                        size_t count, lc_error *error) {
  lc_pouch_crypto_output *out;
  const unsigned char *cursor;
  size_t take;
  size_t remaining;
  int rc;
  out = (lc_pouch_crypto_output *)sink;
  if (out->finished) {
    lc_error_set(error, LC_ERR_INVALID, 0L, "pouch crypto writer is finished",
                 NULL, NULL, "pouch");
    return 0;
  }
  if (out->failed != LC_OK) {
    return 0;
  }
  rc = lc_pouch_crypto_check_byte_counter(
      out->plain_total, count,
      "pouch plaintext byte count exceeds platform limit", error);
  if (rc == LC_OK && count != 0U && bytes == NULL) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch crypto sink requires bytes", NULL, NULL, "pouch");
  }
  if (rc == LC_OK && !out->desc.compressed) {
    rc = lc_pouch_crypto_output_feed(out, (const unsigned char *)bytes, count,
                                     error);
  } else if (rc == LC_OK) {
    cursor = (const unsigned char *)bytes;
    remaining = count;
    while (remaining != 0U && rc == LC_OK) {
      take = remaining > (size_t)UINT_MAX ? (size_t)UINT_MAX : remaining;
      out->deflater.next_in = (Bytef *)cursor;
      out->deflater.avail_in = (uInt)take;
      rc = lc_pouch_crypto_output_deflate(out, 0, error);
      cursor += take;
      remaining -= take;
    }
  }
  if (rc == LC_OK) {
    out->plain_total += count;
  }
  out->failed = rc;
  return rc == LC_OK;
}

/* Restartable units reuse all bounded scratch and codec contexts. Each
 * encrypted unit gets fresh key material and nonce space, even when callers
 * reuse the same identity context. No storage or publication is performed. */
static int lc_pouch_crypto_output_descriptor(lc_pouch_crypto_output *out,
                                             lc_error *error) {
  int rc = LC_OK;
  if (out->desc.encrypted) {
    out->desc.binding_present = 0;
    OPENSSL_cleanse(out->key, sizeof(out->key));
    if (!lc_pouch_crypto_random_bytes(out->desc.payload_key_salt,
                                      sizeof(out->desc.payload_key_salt)) ||
        !lc_pouch_crypto_random_bytes(out->desc.nonce_prefix,
                                      sizeof(out->desc.nonce_prefix)))
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to generate pouch crypto descriptor material",
                        NULL, NULL, "pouch");
    if (rc == LC_OK) {
      out->desc.payload_key_derived = 1;
      rc = lc_pouch_crypto_derive_payload_key(out->crypto,
                                              out->desc.payload_key_salt,
                                              out->key, out->derivation, error);
    }
    if (rc == LC_OK)
      rc = lc_pouch_crypto_desc_bind_reusing(out->crypto, &out->desc,
                                             out->context, out->derivation,
                                             out->binding, error);
  }
  if (rc == LC_OK && (out->desc.encrypted || out->desc.compressed)) {
    rc = lc_pouch_crypto_descriptor_encode_into(
        &out->desc, out->descriptor, LC_POUCH_DESC_TEXT_BYTES, error);
    out->core_length =
        lc_pouch_crypto_descriptor_core_encode(&out->desc, out->core);
  }
  return rc;
}

static int lc_pouch_crypto_output_open(lc_pouch_crypto *crypto,
                                       const char *context, int fd,
                                       int allow_compression,
                                       lc_pouch_crypto_output *out,
                                       lc_error *error) {
  int rc;
  memset(out, 0, sizeof(*out));
  out->pub.write = lc_pouch_crypto_output_write;
  out->allocator = crypto != NULL ? &crypto->allocator : NULL;
  out->crypto = crypto;
  out->context = context;
  out->fd = fd;
  out->desc.encrypted = crypto != NULL && crypto->encryption_enabled;
  out->desc.compressed =
      allow_compression && crypto != NULL && crypto->compression_enabled;
  out->desc.frame_size =
      out->desc.encrypted ? LC_POUCH_FRAME_PLAINTEXT_BYTES : 0U;
  out->crc = (unsigned long)crc32(0L, Z_NULL, 0);
  rc = LC_OK;
  if (out->desc.encrypted && (context == NULL || context[0] == '\0')) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto write requires context", NULL, NULL,
                        "pouch");
  }
  if (out->desc.encrypted) {
    out->frame = (unsigned char *)lc_alloc_with_allocator(
        out->allocator, LC_POUCH_WRITER_FRAME_BYTES);
    out->plain = out->frame != NULL ? out->frame + 8U : NULL;
  } else {
    out->plain = out->desc.compressed
                     ? (unsigned char *)lc_alloc_with_allocator(
                           out->allocator, LC_POUCH_FRAME_PLAINTEXT_BYTES)
                     : out->small;
  }
  if (out->plain == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch streaming buffer", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK && out->desc.compressed) {
    if (deflateInit2(&out->deflater, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                     MAX_WBITS, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize pouch compressor", NULL, NULL,
                        "pouch");
    } else {
      out->zlib_initialized = 1;
    }
  }
  if (rc == LC_OK && (out->desc.encrypted || out->desc.compressed)) {
    out->descriptor =
        lc_alloc_with_allocator(out->allocator, LC_POUCH_DESC_TEXT_BYTES);
    if (out->descriptor == NULL)
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto descriptor", NULL,
                        NULL, "pouch");
  }
  if (rc == LC_OK && out->desc.encrypted) {
    rc = lc_pouch_crypto_derivation_open(out->crypto, &out->derivation, error);
    if (rc == LC_OK) {
      out->binding = EVP_CIPHER_CTX_new();
      out->cipher = EVP_CIPHER_CTX_new();
      if (out->binding == NULL || out->cipher == NULL)
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch streaming cipher contexts",
                          NULL, NULL, "pouch");
    }
    if (rc == LC_OK)
      rc = lc_pouch_crypto_encrypt_context_begin(out->binding, error);
    if (rc == LC_OK)
      rc = lc_pouch_crypto_encrypt_context_begin(out->cipher, error);
  }
  if (rc == LC_OK)
    rc = lc_pouch_crypto_output_descriptor(out, error);
  return rc;
}

void lc_pouch_crypto_writer_close(lc_pouch_crypto_writer *out) {
  const lc_allocator *allocator;
  if (out == NULL)
    return;
  allocator = out->allocator;
  if (out->zlib_initialized)
    (void)deflateEnd(&out->deflater);
  EVP_CIPHER_CTX_free(out->cipher);
  EVP_CIPHER_CTX_free(out->binding);
  EVP_MAC_CTX_free(out->derivation);
  if (!out->desc.encrypted && out->plain != NULL && out->plain != out->small) {
    OPENSSL_cleanse(out->plain, LC_POUCH_FRAME_PLAINTEXT_BYTES);
    lc_free_with_allocator(allocator, out->plain);
  }
  OPENSSL_cleanse(out->small, sizeof(out->small));
  if (out->frame != NULL)
    OPENSSL_cleanse(out->frame, LC_POUCH_WRITER_FRAME_BYTES);
  lc_free_with_allocator(allocator, out->frame);
  lc_free_with_allocator(allocator, out->descriptor);
  OPENSSL_cleanse(out->key, sizeof(out->key));
  OPENSSL_cleanse(out->core, sizeof(out->core));
  OPENSSL_cleanse(&out->desc, sizeof(out->desc));
  lc_free_with_allocator(allocator, out);
}

static int lc_pouch_crypto_writer_open_output(
    lc_pouch_crypto *crypto, const char *context, int fd, lc_sink *destination,
    int allow_compression, lc_pouch_crypto_writer **result, lc_error *error) {
  lc_pouch_crypto_output *out;
  const lc_allocator *allocator = crypto != NULL ? &crypto->allocator : NULL;
  int rc;
  if (result != NULL)
    *result = NULL;
  if ((fd < 0 && (destination == NULL || destination->write == NULL)) ||
      result == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto writer requires fd and output", NULL,
                        NULL, "pouch");
  out = lc_alloc_with_allocator(allocator, sizeof(*out));
  if (out == NULL)
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto writer", NULL, NULL,
                        "pouch");
  rc = lc_pouch_crypto_output_open(crypto, context, fd, allow_compression, out,
                                   error);
  out->destination = destination;
  /* Sink writers cannot allocate scratch in response to producer progress.
   * Provision their one bounded plain chunk at admission, before input. */
  if (rc == LC_OK && destination && out->plain == out->small) {
    out->plain =
        lc_alloc_with_allocator(allocator, LC_POUCH_FRAME_PLAINTEXT_BYTES);
    if (!out->plain)
      rc = lc_error_set(error, LC_ERR_NOMEM, 0,
                        "failed to allocate streaming codec chunk", NULL, NULL,
                        "pouch");
  }
  if (rc != LC_OK)
    lc_pouch_crypto_writer_close(out);
  else
    *result = out;
  return rc;
}

int lc_pouch_crypto_writer_open(lc_pouch_crypto *crypto, const char *context,
                                int fd, int allow_compression,
                                lc_pouch_crypto_writer **result,
                                lc_error *error) {
  return lc_pouch_crypto_writer_open_output(crypto, context, fd, NULL,
                                            allow_compression, result, error);
}

int lc_pouch_crypto_writer_open_sink(lc_pouch_crypto *crypto,
                                     const char *context, lc_sink *destination,
                                     lc_pouch_crypto_writer **result,
                                     lc_error *error) {
  return lc_pouch_crypto_writer_open_output(crypto, context, -1, destination, 1,
                                            result, error);
}

lc_sink *lc_pouch_crypto_writer_sink(lc_pouch_crypto_writer *out) {
  return out != NULL ? &out->pub : NULL;
}

int lc_pouch_crypto_writer_finish_borrowed(
    lc_pouch_crypto_writer *out, uint64_t *plain_bytes, uint64_t *cipher_bytes,
    unsigned long *stored_crc, const char **descriptor_out, lc_error *error) {
  int rc;
  if (descriptor_out != NULL)
    *descriptor_out = NULL;
  if (out == NULL || plain_bytes == NULL || cipher_bytes == NULL ||
      descriptor_out == NULL || out->finished)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto writer cannot finish", NULL, NULL,
                        "pouch");
  *plain_bytes = *cipher_bytes = 0U;
  out->finished = 1;
  rc = out->failed;
  if (rc == LC_OK && out->desc.compressed) {
    out->deflater.next_in = NULL;
    out->deflater.avail_in = 0U;
    rc = lc_pouch_crypto_output_deflate(out, 1, error);
  }
  if (rc == LC_OK && (out->fill != 0U || out->desc.encrypted))
    rc = lc_pouch_crypto_output_flush(out, out->desc.encrypted, error);
  out->failed = rc;
  if (rc == LC_OK) {
    *plain_bytes = out->plain_total;
    *cipher_bytes = out->cipher_total;
    *descriptor_out = out->descriptor;
    out->borrowed_finished = 1;
    if (stored_crc != NULL)
      *stored_crc = out->crc;
  }
  return rc;
}

int lc_pouch_crypto_writer_finish(lc_pouch_crypto_writer *out,
                                  uint64_t *plain_bytes, uint64_t *cipher_bytes,
                                  unsigned long *stored_crc,
                                  char **descriptor_out, lc_error *error) {
  const char *borrowed = NULL;
  int rc;
  if (descriptor_out == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto writer requires descriptor output", NULL,
                        NULL, "pouch");
  *descriptor_out = NULL;
  rc = lc_pouch_crypto_writer_finish_borrowed(out, plain_bytes, cipher_bytes,
                                              stored_crc, &borrowed, error);
  if (rc == LC_OK) {
    *descriptor_out = out->descriptor;
    out->descriptor = NULL;
    out->borrowed_finished = 0;
  }
  return rc;
}

uint64_t lc_pouch_crypto_writer_bound(lc_pouch_crypto_writer *out,
                                      uint64_t plain) {
  uint64_t encoded, frames;
  if (!out || plain > ((uint64_t)ULONG_MAX - 64U) / 2U)
    return LC_U64_MAX;
  encoded =
      out->desc.compressed ? (uint64_t)compressBound((uLong)plain) : plain;
  if (out->desc.encrypted) {
    frames = encoded <= LC_POUCH_FIRST_FRAME_PLAINTEXT_BYTES
                 ? 1U
                 : 1U + (encoded - LC_POUCH_FIRST_FRAME_PLAINTEXT_BYTES +
                         LC_POUCH_FRAME_PLAINTEXT_BYTES - 1U) /
                            LC_POUCH_FRAME_PLAINTEXT_BYTES;
    if (frames > (LC_U64_MAX - encoded) / (8U + LC_POUCH_GCM_TAG_BYTES) - 1U)
      return LC_U64_MAX;
    encoded += (frames + 1U) * (8U + LC_POUCH_GCM_TAG_BYTES);
  }
  return encoded;
}

int lc_pouch_crypto_writer_restart(lc_pouch_crypto_writer *out,
                                   const char *context, lc_error *error) {
  int rc = LC_OK;
  if (out == NULL || !out->finished || out->failed || !out->borrowed_finished ||
      (out->desc.encrypted && (context == NULL || !context[0])))
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch codec restart requires a completed borrowed unit", NULL, NULL,
        "pouch");
  out->context = context;
  out->borrowed_finished = 0;
  if (out->zlib_initialized && deflateReset(&out->deflater) != Z_OK)
    rc =
        lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                     "failed to restart pouch compressor", NULL, NULL, "pouch");
  if (rc == LC_OK)
    rc = lc_pouch_crypto_output_descriptor(out, error);
  out->failed = rc;
  if (rc == LC_OK) {
    out->fill = 0;
    out->counter = 0;
    out->plain_total = out->cipher_total = 0;
    out->crc = (unsigned long)crc32(0L, Z_NULL, 0);
    out->finished = 0;
  }
  return rc;
}

int lc_pouch_crypto_produce_to_fd(lc_pouch_crypto *crypto, const char *context,
                                  int fd, lc_pouch_crypto_producer_fn producer,
                                  void *producer_context, uint64_t *plain_bytes,
                                  uint64_t *cipher_bytes,
                                  unsigned long *stored_crc,
                                  char **descriptor_out, int allow_compression,
                                  lc_error *error) {
  lc_pouch_crypto_writer *writer = NULL;
  int rc;
  if (descriptor_out != NULL)
    *descriptor_out = NULL;
  if (fd < 0 || producer == NULL || plain_bytes == NULL ||
      cipher_bytes == NULL || descriptor_out == NULL)
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch crypto producer requires fd, producer and outputs", NULL, NULL,
        "pouch");
  *plain_bytes = *cipher_bytes = 0U;
  if (stored_crc != NULL)
    *stored_crc = (unsigned long)crc32(0L, Z_NULL, 0);
  rc = lc_pouch_crypto_writer_open(crypto, context, fd, allow_compression,
                                   &writer, error);
  if (rc == LC_OK)
    rc = producer(producer_context, lc_pouch_crypto_writer_sink(writer), error);
  if (rc == LC_OK && error != NULL && error->code != LC_OK)
    rc = error->code;
  if (rc == LC_OK)
    rc = lc_pouch_crypto_writer_finish(writer, plain_bytes, cipher_bytes,
                                       stored_crc, descriptor_out, error);
  lc_pouch_crypto_writer_close(writer);
  return rc;
}

typedef struct lc_pouch_crypto_pull_producer {
  lc_source *source;
  const lc_allocator *allocator;
} lc_pouch_crypto_pull_producer;

static int lc_pouch_crypto_produce_source(void *context, lc_sink *sink,
                                          lc_error *error) {
  lc_pouch_crypto_pull_producer *producer;
  lc_pouch_crypto_output *out;
  unsigned char *bytes;
  size_t got;
  size_t target;
  size_t capacity;
  int rc;
  producer = (lc_pouch_crypto_pull_producer *)context;
  out = (lc_pouch_crypto_output *)sink;
  if (!out->desc.compressed) {
    /* A pull source can fill the encoder's current frame directly. The push
     * writer uses the same frame buffer without imposing another copy or
     * allocation on existing uncompressed Pouch writes. */
    for (;;) {
      target = out->counter == 0UL ? LC_POUCH_FIRST_FRAME_PLAINTEXT_BYTES
                                   : LC_POUCH_FRAME_PLAINTEXT_BYTES;
      capacity = target - out->fill;
      got = producer->source->read(producer->source, out->plain + out->fill,
                                   capacity, error);
      if (error != NULL && error->code != LC_OK) {
        return error->code;
      }
      if (got > capacity) {
        return lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch source exceeded requested chunk size", NULL,
                            NULL, "pouch");
      }
      if (got == 0U) {
        return LC_OK;
      }
      rc = lc_pouch_crypto_check_byte_counter(
          out->plain_total, got,
          "pouch plaintext byte count exceeds platform limit", error);
      if (rc != LC_OK) {
        return rc;
      }
      out->plain_total += got;
      out->fill += got;
      if (out->fill == target) {
        rc = lc_pouch_crypto_output_flush(out, 0, error);
        if (rc != LC_OK) {
          return rc;
        }
      }
    }
  }
  bytes = (unsigned char *)lc_alloc_with_allocator(
      producer->allocator, LC_POUCH_FRAME_PLAINTEXT_BYTES);
  if (bytes == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch source chunk", NULL, NULL,
                        "pouch");
  }
  rc = LC_OK;
  for (;;) {
    got = producer->source->read(producer->source, bytes,
                                 LC_POUCH_FRAME_PLAINTEXT_BYTES, error);
    if (error != NULL && error->code != LC_OK) {
      rc = error->code;
      break;
    }
    if (got > LC_POUCH_FRAME_PLAINTEXT_BYTES) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch source exceeded requested chunk size", NULL,
                        NULL, "pouch");
      break;
    }
    if (got == 0U) {
      break;
    }
    if (!sink->write(sink, bytes, got, error)) {
      rc = error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
      break;
    }
  }
  OPENSSL_cleanse(bytes, LC_POUCH_FRAME_PLAINTEXT_BYTES);
  lc_free_with_allocator(producer->allocator, bytes);
  return rc;
}

static int lc_pouch_crypto_stream_to_fd_crc_impl(
    lc_pouch_crypto *crypto, const char *context, int fd, lc_source *body,
    uint64_t *plain_bytes, uint64_t *cipher_bytes, unsigned long *stored_crc,
    char **descriptor_out, int allow_compression, lc_error *error) {
  lc_pouch_crypto_pull_producer producer;
  if (descriptor_out != NULL) {
    *descriptor_out = NULL;
  }
  if (fd < 0 || body == NULL || body->read == NULL || plain_bytes == NULL ||
      cipher_bytes == NULL || descriptor_out == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch crypto stream write requires fd, body and outputs", NULL, NULL,
        "pouch");
  }
  if (stored_crc != NULL) {
    *stored_crc = (unsigned long)crc32(0L, Z_NULL, 0);
  }
  if (crypto == NULL ||
      (!crypto->encryption_enabled &&
       (!allow_compression || !crypto->compression_enabled))) {
    return lc_pouch_crypto_stream_plain_to_fd(
        crypto != NULL ? &crypto->allocator : NULL, fd, body, plain_bytes,
        cipher_bytes, stored_crc, error);
  }
  producer.source = body;
  producer.allocator = &crypto->allocator;
  return lc_pouch_crypto_produce_to_fd(
      crypto, context, fd, lc_pouch_crypto_produce_source, &producer,
      plain_bytes, cipher_bytes, stored_crc, descriptor_out, allow_compression,
      error);
}
int lc_pouch_crypto_stream_to_fd_crc(lc_pouch_crypto *crypto,
                                     const char *context, int fd,
                                     lc_source *body, uint64_t *plain_bytes,
                                     uint64_t *cipher_bytes,
                                     unsigned long *stored_crc,
                                     char **descriptor_out, lc_error *error) {
  return lc_pouch_crypto_stream_to_fd_crc_with_compression(
      crypto, context, fd, body, plain_bytes, cipher_bytes, stored_crc,
      descriptor_out, 1, error);
}

int lc_pouch_crypto_stream_to_fd_crc_with_compression(
    lc_pouch_crypto *crypto, const char *context, int fd, lc_source *body,
    uint64_t *plain_bytes, uint64_t *cipher_bytes, unsigned long *stored_crc,
    char **descriptor_out, int allow_compression, lc_error *error) {
  return lc_pouch_crypto_stream_to_fd_crc_impl(
      crypto, context, fd, body, plain_bytes, cipher_bytes, stored_crc,
      descriptor_out, allow_compression, error);
}

int lc_pouch_crypto_transform_memory(
    const lc_allocator *allocator, lc_pouch_crypto *crypto, const char *context,
    const unsigned char *plain, size_t plain_length, int allow_compression,
    unsigned char **stored_out, size_t *stored_length_out,
    unsigned long *stored_crc_out, char **descriptor_out, lc_error *error) {
  const unsigned char empty_plain = 0U;
  const unsigned char *working;
  unsigned char payload_key[LC_POUCH_DEK_BYTES];
  unsigned char descriptor_core[LC_POUCH_DESC_RAW_BYTES_V2];
  unsigned char *compressed_bytes;
  unsigned char *stored;
  char *descriptor;
  lc_pouch_crypto_desc desc;
  EVP_CIPHER_CTX *ctx;
  uLong compressed_capacity;
  size_t working_length;
  size_t stored_capacity;
  size_t stored_length;
  size_t data_frames;
  size_t frame_count;
  size_t descriptor_core_len;
  size_t remaining;
  size_t plain_offset;
  unsigned long counter;
  int encrypted;
  int compressed;
  int rc;

  if (stored_out == NULL || stored_length_out == NULL ||
      stored_crc_out == NULL || descriptor_out == NULL ||
      (plain_length > 0U && plain == NULL) ||
      plain_length > LC_POUCH_CRYPTO_MEMORY_TRANSFORM_MAX_BYTES) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch inline payload transform is invalid", NULL, NULL,
                        "pouch");
  }
  *stored_out = NULL;
  *stored_length_out = 0U;
  *stored_crc_out = (unsigned long)crc32(0L, Z_NULL, 0);
  *descriptor_out = NULL;
  encrypted = crypto != NULL && crypto->encryption_enabled;
  compressed =
      allow_compression && crypto != NULL && crypto->compression_enabled;
  if (encrypted && (context == NULL || context[0] == '\0')) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto write requires context", NULL, NULL,
                        "pouch");
  }

  compressed_bytes = NULL;
  stored = NULL;
  descriptor = NULL;
  ctx = NULL;
  working = plain != NULL ? plain : &empty_plain;
  working_length = plain_length;
  if (compressed) {
    compressed_capacity = compressBound((uLong)plain_length);
    if ((uLong)(size_t)compressed_capacity != compressed_capacity) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch compressed inline payload exceeds platform "
                          "limit",
                          NULL, NULL, "pouch");
    }
    compressed_bytes = (unsigned char *)lc_alloc_with_allocator(
        allocator,
        (size_t)compressed_capacity == 0U ? 1U : (size_t)compressed_capacity);
    if (compressed_bytes == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate compressed inline payload", NULL,
                          NULL, "pouch");
    }
    if (compress2(compressed_bytes, &compressed_capacity, working,
                  (uLong)plain_length, Z_DEFAULT_COMPRESSION) != Z_OK) {
      lc_free_with_allocator(allocator, compressed_bytes);
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "failed to compress pouch payload", NULL, NULL,
                          "pouch");
    }
    working = compressed_bytes;
    working_length = (size_t)compressed_capacity;
  }
  if (!encrypted) {
    if (!compressed && working_length > 0U) {
      stored =
          (unsigned char *)lc_alloc_with_allocator(allocator, working_length);
      if (stored == NULL) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate inline payload", NULL, NULL,
                            "pouch");
      }
      memcpy(stored, working, working_length);
    } else if (compressed) {
      stored = compressed_bytes;
      compressed_bytes = NULL;
    }
    if (compressed) {
      memset(&desc, 0, sizeof(desc));
      desc.compressed = 1;
      rc = lc_pouch_crypto_descriptor_encode(allocator, &desc, &descriptor,
                                             error);
      if (rc != LC_OK) {
        lc_free_with_allocator(allocator, stored);
        return rc;
      }
    }
    if (working_length > 0U) {
      *stored_crc_out = (unsigned long)crc32((uLong)*stored_crc_out, stored,
                                             (uInt)working_length);
    }
    *stored_out = stored;
    *stored_length_out = working_length;
    *descriptor_out = descriptor;
    return LC_OK;
  }

  memset(&desc, 0, sizeof(desc));
  desc.encrypted = 1;
  desc.compressed = compressed;
  desc.frame_size = LC_POUCH_FRAME_PLAINTEXT_BYTES;
  desc.payload_key_derived = 1;
  if (!lc_pouch_crypto_random_bytes(desc.payload_key_salt,
                                    sizeof(desc.payload_key_salt)) ||
      !lc_pouch_crypto_random_bytes(desc.nonce_prefix,
                                    sizeof(desc.nonce_prefix))) {
    lc_free_with_allocator(allocator, compressed_bytes);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to generate pouch crypto descriptor material",
                        NULL, NULL, "pouch");
  }
  rc = lc_pouch_crypto_derive_payload_key(crypto, desc.payload_key_salt,
                                          payload_key, NULL, error);
  if (rc != LC_OK) {
    OPENSSL_cleanse(payload_key, sizeof(payload_key));
    lc_free_with_allocator(allocator, compressed_bytes);
    return rc;
  }
  rc = lc_pouch_crypto_desc_bind(crypto, &desc, context, error);
  if (rc != LC_OK) {
    OPENSSL_cleanse(payload_key, sizeof(payload_key));
    lc_free_with_allocator(allocator, compressed_bytes);
    return rc;
  }
  rc = lc_pouch_crypto_descriptor_encode(allocator, &desc, &descriptor, error);
  if (rc != LC_OK) {
    OPENSSL_cleanse(payload_key, sizeof(payload_key));
    lc_free_with_allocator(allocator, compressed_bytes);
    return rc;
  }
  descriptor_core_len =
      lc_pouch_crypto_descriptor_core_encode(&desc, descriptor_core);
  data_frames = 0U;
  if (working_length > 0U) {
    data_frames = 1U;
    if (working_length > LC_POUCH_FIRST_FRAME_PLAINTEXT_BYTES) {
      remaining = working_length - LC_POUCH_FIRST_FRAME_PLAINTEXT_BYTES;
      data_frames += (remaining + LC_POUCH_FRAME_PLAINTEXT_BYTES - 1U) /
                     LC_POUCH_FRAME_PLAINTEXT_BYTES;
    }
  }
  if (data_frames == (size_t)-1) {
    OPENSSL_cleanse(payload_key, sizeof(payload_key));
    lc_free_with_allocator(allocator, descriptor);
    lc_free_with_allocator(allocator, compressed_bytes);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted inline payload exceeds platform "
                        "limit",
                        NULL, NULL, "pouch");
  }
  frame_count = data_frames + 1U;
  if (frame_count >
      (((size_t)-1) - working_length) / (8U + LC_POUCH_GCM_TAG_BYTES)) {
    OPENSSL_cleanse(payload_key, sizeof(payload_key));
    lc_free_with_allocator(allocator, descriptor);
    lc_free_with_allocator(allocator, compressed_bytes);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted inline payload exceeds platform "
                        "limit",
                        NULL, NULL, "pouch");
  }
  stored_capacity =
      working_length + frame_count * (8U + LC_POUCH_GCM_TAG_BYTES);
  stored = (unsigned char *)lc_alloc_with_allocator(
      allocator, stored_capacity == 0U ? 1U : stored_capacity);
  if (stored == NULL) {
    OPENSSL_cleanse(payload_key, sizeof(payload_key));
    lc_free_with_allocator(allocator, descriptor);
    lc_free_with_allocator(allocator, compressed_bytes);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate encrypted inline payload", NULL,
                        NULL, "pouch");
  }
  ctx = EVP_CIPHER_CTX_new();
  if (ctx == NULL) {
    OPENSSL_cleanse(payload_key, sizeof(payload_key));
    lc_free_with_allocator(allocator, stored);
    lc_free_with_allocator(allocator, descriptor);
    lc_free_with_allocator(allocator, compressed_bytes);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto cipher context", NULL,
                        NULL, "pouch");
  }
  stored_length = 0U;
  plain_offset = 0U;
  counter = 0UL;
  rc = lc_pouch_crypto_encrypt_context_begin(ctx, error);
  while (rc == LC_OK && plain_offset < working_length) {
    size_t chunk;
    size_t frame_length;

    chunk = counter == 0UL ? LC_POUCH_FIRST_FRAME_PLAINTEXT_BYTES
                           : LC_POUCH_FRAME_PLAINTEXT_BYTES;
    if (chunk > working_length - plain_offset) {
      chunk = working_length - plain_offset;
    }
    rc = lc_pouch_crypto_encrypt_frame_to_memory(
        ctx, payload_key, desc.nonce_prefix, context, strlen(context),
        descriptor_core, descriptor_core_len, counter, counter == 0UL,
        working + plain_offset, chunk, stored + stored_length,
        stored_capacity - stored_length, &frame_length, error);
    if (rc != LC_OK) {
      break;
    }
    stored_length += frame_length;
    plain_offset += chunk;
    ++counter;
  }
  if (rc == LC_OK) {
    size_t frame_length;

    rc = lc_pouch_crypto_encrypt_frame_to_memory(
        ctx, payload_key, desc.nonce_prefix, context, strlen(context),
        descriptor_core, descriptor_core_len, counter, counter == 0UL,
        &empty_plain, 0U, stored + stored_length,
        stored_capacity - stored_length, &frame_length, error);
    if (rc == LC_OK) {
      stored_length += frame_length;
    }
  }
  EVP_CIPHER_CTX_free(ctx);
  OPENSSL_cleanse(payload_key, sizeof(payload_key));
  OPENSSL_cleanse(descriptor_core, sizeof(descriptor_core));
  if (compressed_bytes != NULL) {
    OPENSSL_cleanse(compressed_bytes, working_length);
    lc_free_with_allocator(allocator, compressed_bytes);
  }
  if (rc != LC_OK) {
    OPENSSL_cleanse(stored, stored_capacity);
    lc_free_with_allocator(allocator, stored);
    lc_free_with_allocator(allocator, descriptor);
    return rc;
  }
  *stored_crc_out =
      (unsigned long)crc32((uLong)*stored_crc_out, stored, (uInt)stored_length);
  *stored_out = stored;
  *stored_length_out = stored_length;
  *descriptor_out = descriptor;
  return LC_OK;
}

static int lc_pouch_crypto_optional_descriptor(const char *text,
                                               lc_pouch_crypto_desc *desc,
                                               lc_error *error) {
  memset(desc, 0, sizeof(*desc));
  return text && text[0] ? lc_pouch_crypto_descriptor_decode(text, desc, error)
                         : LC_OK;
}

int lc_pouch_crypto_descriptor_same_payload(const char *left, const char *right,
                                            lc_error *error) {
  lc_pouch_crypto_desc a, b;
  unsigned char ac[LC_POUCH_DESC_RAW_BYTES_V2], bc[LC_POUCH_DESC_RAW_BYTES_V2];
  size_t an, bn;
  int rc = lc_pouch_crypto_optional_descriptor(left, &a, error);
  if (rc == LC_OK)
    rc = lc_pouch_crypto_optional_descriptor(right, &b, error);
  if (rc != LC_OK)
    return rc;
  an = lc_pouch_crypto_descriptor_core_encode(&a, ac);
  bn = lc_pouch_crypto_descriptor_core_encode(&b, bc);
  if (an != bn || memcmp(ac, bc, an))
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch descriptors refer to different payloads", NULL,
                        NULL, "pouch");
  return LC_OK;
}

int lc_pouch_crypto_descriptor_proof(const char *text, unsigned char out[32],
                                     int *encrypted, lc_error *error) {
  lc_pouch_crypto_desc desc;
  int rc = lc_pouch_crypto_optional_descriptor(text, &desc, error);
  memset(out, 0, 32U);
  *encrypted = 0;
  if (rc != LC_OK)
    return rc;
  if (!desc.encrypted)
    return LC_OK;
  if (!desc.binding_present)
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch descriptor has no publication binding", NULL,
                        NULL, "pouch");
  memcpy(out, desc.binding_nonce, LC_POUCH_DESC_BINDING_NONCE_BYTES);
  memcpy(out + LC_POUCH_DESC_BINDING_NONCE_BYTES, desc.binding_tag,
         LC_POUCH_GCM_TAG_BYTES);
  *encrypted = 1;
  return LC_OK;
}

int lc_pouch_crypto_descriptor_restore(const char *text,
                                       const unsigned char proof[32],
                                       char out[128], int *encrypted,
                                       lc_error *error) {
  lc_pouch_crypto_desc desc;
  size_t i;
  int rc = lc_pouch_crypto_optional_descriptor(text, &desc, error);
  out[0] = 0;
  *encrypted = 0;
  if (rc != LC_OK)
    return rc;
  if (desc.encrypted) {
    if (!desc.binding_present)
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch unit descriptor has no binding", NULL, NULL,
                          "pouch");
    for (i = LC_POUCH_DESC_BINDING_NONCE_BYTES + LC_POUCH_GCM_TAG_BYTES;
         i < 32U; ++i)
      if (proof[i])
        return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                            "pouch publication evidence has unknown fields",
                            NULL, NULL, "pouch");
    memcpy(desc.binding_nonce, proof, LC_POUCH_DESC_BINDING_NONCE_BYTES);
    memcpy(desc.binding_tag, proof + LC_POUCH_DESC_BINDING_NONCE_BYTES,
           LC_POUCH_GCM_TAG_BYTES);
    *encrypted = 1;
  }
  return text && text[0]
             ? lc_pouch_crypto_descriptor_encode_into(&desc, out, 128U, error)
             : LC_OK;
}

int lc_pouch_crypto_rebind_descriptor(lc_pouch_crypto *crypto,
                                      const char *descriptor,
                                      const char *old_binding_context,
                                      const char *new_binding_context,
                                      char **out, lc_error *error) {
  lc_pouch_crypto_desc desc;
  int rc;

  if (descriptor == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch descriptor rebind requires descriptor and out",
                        NULL, NULL, "pouch");
  }
  *out = NULL;
  rc = lc_pouch_crypto_descriptor_decode(descriptor, &desc, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!desc.encrypted) {
    *out = lc_pouch_crypto_strdup(crypto != NULL ? &crypto->allocator : NULL,
                                  descriptor);
    return *out != NULL ? LC_OK
                        : lc_error_set(error, LC_ERR_NOMEM, 0L,
                                       "failed to copy pouch crypto descriptor",
                                       NULL, NULL, "pouch");
  }
  rc = lc_pouch_crypto_desc_verify_binding(crypto, &desc, old_binding_context,
                                           NULL, NULL, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_crypto_desc_bind(crypto, &desc, new_binding_context, error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_crypto_descriptor_encode(&crypto->allocator, &desc, out,
                                           error);
}

static size_t lc_pouch_crypto_source_read(lc_source *self, void *buffer,
                                          size_t count, lc_error *error);
static int lc_pouch_crypto_source_reset(lc_source *self, lc_error *error);
static void lc_pouch_crypto_source_close(lc_source *self);
static size_t lc_pouch_plain_span_source_read(lc_source *self, void *buffer,
                                              size_t count, lc_error *error);
static int lc_pouch_plain_span_source_reset(lc_source *self, lc_error *error);

static EVP_CIPHER_CTX *lc_pouch_crypto_decrypt_context_new(void) {
  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
  if (ctx != NULL &&
      (EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), NULL, NULL, NULL) != 1 ||
       EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, LC_POUCH_NONCE_BYTES,
                           NULL) != 1)) {
    EVP_CIPHER_CTX_free(ctx);
    ctx = NULL;
  }
  return ctx;
}

int lc_pouch_crypto_source_from_file(lc_pouch_crypto *crypto,
                                     const char *context,
                                     const char *binding_context,
                                     const char *path, const char *descriptor,
                                     lc_source **out, lc_error *error) {
  lc_pouch_crypto_source *source;
  lc_pouch_zlib_source *inflater;
  lc_source *base;
  lc_pouch_crypto_desc desc;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto source requires out", NULL, NULL,
                        "pouch");
  }
  *out = NULL;
  if (descriptor == NULL || descriptor[0] == '\0') {
    if (crypto != NULL && crypto->encryption_enabled) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "transformed pouch payload is missing descriptor",
                          NULL, NULL, "pouch");
    }
    return lc_source_from_file(path, out, error);
  }
  rc = lc_pouch_crypto_descriptor_decode(descriptor, &desc, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!desc.encrypted) {
    rc = lc_source_from_file(path, &base, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (!desc.compressed) {
      *out = base;
      return LC_OK;
    }
    inflater = NULL;
    rc = lc_pouch_zlib_source_open(crypto != NULL ? &crypto->allocator : NULL,
                                   base, 1, &inflater, error);
    if (rc != LC_OK) {
      if (base->close != NULL) {
        base->close(base);
      }
      return rc;
    }
    *out = &inflater->pub;
    return LC_OK;
  }
  if (crypto == NULL || !crypto->encryption_enabled) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "encrypted pouch payload requires crypto key", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_crypto_desc_verify_binding(crypto, &desc, binding_context, NULL,
                                           NULL, error);
  if (rc != LC_OK) {
    return rc;
  }
  source = (lc_pouch_crypto_source *)lc_calloc_with_allocator(
      &crypto->allocator, 1U, sizeof(*source));
  if (source == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto source", NULL, NULL,
                        "pouch");
  }
  source->allocator = crypto->allocator;
  source->fd = -1;
  source->ctx = lc_pouch_crypto_decrypt_context_new();
  if (source->ctx == NULL) {
    lc_free_with_allocator(&crypto->allocator, source);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch decrypt context", NULL, NULL,
                        "pouch");
  }
  source->fd = open(path, O_RDONLY);
  if (source->fd < 0) {
    EVP_CIPHER_CTX_free(source->ctx);
    lc_free_with_allocator(&crypto->allocator, source);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open encrypted pouch payload",
                        strerror(errno), NULL, "pouch");
  }
  if (context == NULL || context[0] == '\0') {
    close(source->fd);
    EVP_CIPHER_CTX_free(source->ctx);
    lc_free_with_allocator(&crypto->allocator, source);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto read requires context", NULL, NULL,
                        "pouch");
  }
  source->context = lc_pouch_crypto_strdup(&crypto->allocator, context);
  if (source->context == NULL) {
    close(source->fd);
    EVP_CIPHER_CTX_free(source->ctx);
    lc_free_with_allocator(&crypto->allocator, source);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto read context", NULL,
                        NULL, "pouch");
  }
  source->context_len = strlen(source->context);
  source->descriptor_core_len =
      lc_pouch_crypto_descriptor_core_encode(&desc, source->descriptor_core);
  if (desc.payload_key_derived) {
    rc = lc_pouch_crypto_derive_payload_key(crypto, desc.payload_key_salt,
                                            source->key, NULL, error);
    if (rc != LC_OK) {
      OPENSSL_cleanse(source->key, sizeof(source->key));
      close(source->fd);
      EVP_CIPHER_CTX_free(source->ctx);
      lc_free_with_allocator(&crypto->allocator, source->context);
      lc_free_with_allocator(&crypto->allocator, source);
      return rc;
    }
  } else {
    memcpy(source->key, crypto->data_key, sizeof(source->key));
  }
  memcpy(source->nonce_prefix, desc.nonce_prefix, sizeof(source->nonce_prefix));
  source->frame_size = desc.frame_size;
  source->pub.read = lc_pouch_crypto_source_read;
  source->pub.reset = lc_pouch_crypto_source_reset;
  source->pub.close = lc_pouch_crypto_source_close;
  source->pub.impl = source;
  if (desc.compressed) {
    inflater = NULL;
    rc = lc_pouch_zlib_source_open(&crypto->allocator, &source->pub, 1,
                                   &inflater, error);
    if (rc != LC_OK) {
      source->pub.close(&source->pub);
      return rc;
    }
    *out = &inflater->pub;
  } else {
    *out = &source->pub;
  }
  return LC_OK;
}

int lc_pouch_crypto_source_from_file_span(
    lc_pouch_crypto *crypto, const char *context, const char *binding_context,
    const char *path, uint64_t offset, uint64_t length, const char *descriptor,
    lc_source **out, lc_error *error) {
  int fd;

  if (path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto span source requires path", NULL, NULL,
                        "pouch");
  }
  fd = open(path, O_RDONLY);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch payload span", strerror(errno),
                        NULL, "pouch");
  }
  return lc_pouch_crypto_source_from_fd_span(crypto, context, binding_context,
                                             fd, offset, length, descriptor,
                                             out, error);
}

int lc_pouch_crypto_source_from_fd_span(lc_pouch_crypto *crypto,
                                        const char *context,
                                        const char *binding_context, int fd,
                                        uint64_t offset, uint64_t length,
                                        const char *descriptor, lc_source **out,
                                        lc_error *error) {
  lc_pouch_crypto_reader *reader;
  int rc;

  if (fd < 0 || out == NULL) {
    if (fd >= 0) {
      close(fd);
    }
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto span source requires fd and output", NULL,
                        NULL, "pouch");
  }
  *out = NULL;
  reader = NULL;
  rc = lc_pouch_crypto_reader_open(crypto, &reader, error);
  if (rc == LC_OK)
    rc = lc_pouch_crypto_reader_bind(reader, context, binding_context, fd,
                                     offset, length, descriptor, out, error);
  if (rc != LC_OK) {
    lc_pouch_crypto_reader_close(reader);
    close(fd);
    return rc;
  }
  reader->owned_fd = fd;
  return LC_OK;
}

void lc_pouch_crypto_reader_clear(lc_pouch_crypto_reader *reader) {
  if (reader == NULL)
    return;
  reader->active = NULL;
  if (reader->owned_fd >= 0)
    close(reader->owned_fd);
  reader->owned_fd = -1;
  reader->plain.fd = -1;
  if (reader->decryptor != NULL) {
    OPENSSL_cleanse(reader->decryptor->key, sizeof(reader->decryptor->key));
    OPENSSL_cleanse(reader->decryptor->plain, reader->decryptor->plain_used);
    reader->decryptor->plain_used = 0U;
    reader->decryptor->fd = -1;
    reader->decryptor->input = NULL;
  }
}

void lc_pouch_crypto_reader_close(lc_pouch_crypto_reader *reader) {
  if (reader == NULL)
    return;
  lc_pouch_crypto_reader_clear(reader);
  if (reader->inflater != NULL)
    lc_pouch_zlib_source_close(&reader->inflater->pub);
  if (reader->decryptor != NULL)
    lc_pouch_crypto_source_close(&reader->decryptor->pub);
  EVP_MAC_CTX_free(reader->derivation);
  EVP_CIPHER_CTX_free(reader->binding);
  lc_free_with_allocator(&reader->allocator, reader);
}

static size_t lc_pouch_crypto_reader_read(lc_source *self, void *bytes,
                                          size_t capacity, lc_error *error) {
  lc_pouch_crypto_reader *reader = self->impl;
  if (reader->active == NULL) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch payload reader is not bound", NULL, NULL,
                       "pouch");
    return 0U;
  }
  return reader->active->read(reader->active, bytes, capacity, error);
}

static int lc_pouch_crypto_reader_reset(lc_source *self, lc_error *error) {
  lc_pouch_crypto_reader *reader = self->impl;
  if (reader->active == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch payload reader is not bound", NULL, NULL,
                        "pouch");
  return reader->active->reset(reader->active, error);
}

static void lc_pouch_crypto_reader_source_close(lc_source *self) {
  lc_pouch_crypto_reader_close(self->impl);
}

int lc_pouch_crypto_reader_open(lc_pouch_crypto *crypto,
                                lc_pouch_crypto_reader **out, lc_error *error) {
  lc_pouch_crypto_reader *reader;
  const lc_allocator *allocator = crypto != NULL ? &crypto->allocator : NULL;
  if (out == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch payload reader requires output", NULL, NULL,
                        "pouch");
  *out = NULL;
  reader = lc_calloc_with_allocator(allocator, 1U, sizeof(*reader));
  if (reader == NULL)
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch payload reader", NULL, NULL,
                        "pouch");
  if (allocator != NULL)
    reader->allocator = *allocator;
  else
    lc_allocator_init(&reader->allocator);
  reader->crypto = crypto;
  reader->owned_fd = -1;
  reader->plain.fd = -1;
  reader->plain.pub.read = lc_pouch_plain_span_source_read;
  reader->plain.pub.reset = lc_pouch_plain_span_source_reset;
  reader->plain.pub.impl = &reader->plain;
  reader->pub.read = lc_pouch_crypto_reader_read;
  reader->pub.reset = lc_pouch_crypto_reader_reset;
  reader->pub.close = lc_pouch_crypto_reader_source_close;
  reader->pub.impl = reader;
  *out = reader;
  return LC_OK;
}

static int lc_pouch_crypto_reader_keys_open(lc_pouch_crypto_reader *reader,
                                            lc_error *error) {
  int rc;
  if (reader->derivation == NULL) {
    rc = lc_pouch_crypto_derivation_open(reader->crypto, &reader->derivation,
                                         error);
    if (rc != LC_OK)
      return rc;
  }
  if (reader->binding == NULL)
    reader->binding = lc_pouch_crypto_decrypt_context_new();
  if (reader->binding == NULL)
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch record binding context", NULL,
                        NULL, "pouch");
  return LC_OK;
}

int lc_pouch_crypto_reader_verify(lc_pouch_crypto_reader *reader,
                                  const char *descriptor, const char *binding,
                                  lc_error *error) {
  lc_pouch_crypto_desc desc;
  int rc;
  if (!reader || !binding)
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch binding verification requires reader and context", NULL, NULL,
        "pouch");
  rc = lc_pouch_crypto_optional_descriptor(descriptor, &desc, error);
  if (rc != LC_OK)
    return rc;
  if (!desc.encrypted && lc_pouch_crypto_enabled(reader->crypto))
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "encrypted pouch publication is missing authentication",
                        NULL, NULL, "pouch");
  if (!desc.encrypted)
    return LC_OK;
  if (!reader->crypto || !reader->crypto->encryption_enabled)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch binding verification requires crypto key", NULL,
                        NULL, "pouch");
  rc = lc_pouch_crypto_reader_keys_open(reader, error);
  return rc == LC_OK
             ? lc_pouch_crypto_desc_verify_binding(reader->crypto, &desc,
                                                   binding, reader->derivation,
                                                   reader->binding, error)
             : rc;
}

static int lc_pouch_crypto_reader_bind_input(
    lc_pouch_crypto_reader *reader, const char *context,
    const char *binding_context, int fd, lc_source *input, uint64_t offset,
    uint64_t length, const char *descriptor, lc_source **out, lc_error *error) {
  lc_pouch_crypto *crypto;
  lc_pouch_crypto_source *source;
  lc_pouch_crypto_desc desc;
  lc_source *base;
  size_t context_len;
  char *copy;
  int rc;

  lc_pouch_crypto_reader_clear(reader);
  if (out != NULL)
    *out = NULL;
  if (reader == NULL || out == NULL || (fd < 0 && input == NULL))
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch payload reader requires reader, fd and output",
                        NULL, NULL, "pouch");
  crypto = reader->crypto;
  memset(&desc, 0, sizeof(desc));
  if (descriptor == NULL || descriptor[0] == '\0') {
    if (crypto != NULL && crypto->encryption_enabled) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "transformed pouch payload is missing descriptor",
                          NULL, NULL, "pouch");
    }
  } else {
    rc = lc_pouch_crypto_descriptor_decode(descriptor, &desc, error);
    if (rc != LC_OK)
      return rc;
  }
  if (!desc.encrypted) {
    reader->plain.fd = fd;
    reader->plain.offset = offset;
    reader->plain.length = length;
    reader->plain.read_bytes = 0U;
    base = input ? input : &reader->plain.pub;
  } else {
    if (crypto == NULL || !crypto->encryption_enabled) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "encrypted pouch payload requires crypto key", NULL,
                          NULL, "pouch");
    }
    rc = lc_pouch_crypto_reader_keys_open(reader, error);
    if (rc != LC_OK)
      return rc;
    rc = lc_pouch_crypto_desc_verify_binding(crypto, &desc, binding_context,
                                             reader->derivation,
                                             reader->binding, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (context == NULL || context[0] == '\0')
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch crypto read requires context", NULL, NULL,
                          "pouch");
    source = reader->decryptor;
    if (source == NULL) {
      source = (lc_pouch_crypto_source *)lc_calloc_with_allocator(
          &crypto->allocator, 1U, sizeof(*source));
      if (source == NULL) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch crypto span source", NULL,
                            NULL, "pouch");
      }
      reader->decryptor = source;
      source->allocator = crypto->allocator;
      source->fd = -1;
      source->pub.read = lc_pouch_crypto_source_read;
      source->pub.reset = lc_pouch_crypto_source_reset;
      source->pub.close = lc_pouch_crypto_source_close;
      source->pub.impl = source;
    }
    if (source->ctx == NULL)
      source->ctx = lc_pouch_crypto_decrypt_context_new();
    if (source->ctx == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch decrypt context", NULL,
                          NULL, "pouch");
    }
    context_len = strlen(context);
    if (context_len >= source->context_capacity) {
      copy = lc_realloc_with_allocator(&crypto->allocator, source->context,
                                       context_len + 1U);
      if (copy == NULL)
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch crypto read context",
                            NULL, NULL, "pouch");
      source->context = copy;
      source->context_capacity = context_len + 1U;
    }
    memcpy(source->context, context, context_len + 1U);
    source->context_len = context_len;
    source->descriptor_core_len =
        lc_pouch_crypto_descriptor_core_encode(&desc, source->descriptor_core);
    if (desc.payload_key_derived) {
      rc = lc_pouch_crypto_derive_payload_key(crypto, desc.payload_key_salt,
                                              source->key, reader->derivation,
                                              error);
      if (rc != LC_OK) {
        OPENSSL_cleanse(source->key, sizeof(source->key));
        return rc;
      }
    } else {
      memcpy(source->key, crypto->data_key, sizeof(source->key));
    }
    memcpy(source->nonce_prefix, desc.nonce_prefix,
           sizeof(source->nonce_prefix));
    source->frame_size = desc.frame_size;
    source->span_offset = offset;
    source->span_length = length;
    source->fd = fd;
    source->input = input;
    source->bounded = 1;
    (void)lc_pouch_crypto_source_reset(&source->pub, error);
    base = &source->pub;
  }
  if (desc.compressed) {
    if (reader->inflater == NULL) {
      rc = lc_pouch_zlib_source_open(&reader->allocator, base, 0,
                                     &reader->inflater, error);
    } else {
      reader->inflater->inner = base;
      rc = lc_pouch_zlib_source_reset(&reader->inflater->pub, error);
    }
    if (rc != LC_OK)
      return rc;
    base = &reader->inflater->pub;
  }
  reader->active = base;
  *out = &reader->pub;
  return LC_OK;
}

int lc_pouch_crypto_reader_bind(lc_pouch_crypto_reader *reader,
                                const char *context, const char *binding,
                                int fd, uint64_t offset, uint64_t length,
                                const char *descriptor, lc_source **out,
                                lc_error *error) {
  return lc_pouch_crypto_reader_bind_input(reader, context, binding, fd, NULL,
                                           offset, length, descriptor, out,
                                           error);
}

int lc_pouch_crypto_reader_bind_source(lc_pouch_crypto_reader *reader,
                                       const char *context, const char *binding,
                                       lc_source *input, uint64_t length,
                                       const char *descriptor, lc_source **out,
                                       lc_error *error) {
  return lc_pouch_crypto_reader_bind_input(reader, context, binding, -1, input,
                                           0, length, descriptor, out, error);
}

/* Fill one bounded frame buffer. Small spans include their terminal frame in
 * the first read; large spans refill this same buffer. Bounded sources never
 * read beyond their canonical span, including during read-ahead. */
static int lc_pouch_crypto_frame_fill(lc_pouch_crypto_source *source,
                                      size_t needed, lc_error *error) {
  size_t buffered, want;
  uint64_t unread;
  off_t target;
  ssize_t got;
  buffered = source->cipher_end - source->cipher_begin;
  if (buffered >= needed)
    return LC_OK;
  if (source->cipher_begin != 0U) {
    memmove(source->cipher, source->cipher + source->cipher_begin, buffered);
    source->cipher_begin = 0U;
    source->cipher_end = buffered;
  }
  while (source->cipher_end < needed) {
    want = sizeof(source->cipher) - source->cipher_end;
    if (source->bounded) {
      unread = source->span_remaining - source->cipher_end;
      if ((uint64_t)want > unread)
        want = (size_t)unread;
    }
    if (want == 0U)
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "encrypted pouch payload span is truncated", NULL,
                          NULL, "pouch");
    target = (off_t)source->read_offset;
    if (target < 0 || (uint64_t)target != source->read_offset ||
        source->read_offset > LC_U64_MAX - (uint64_t)want)
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch payload offset exceeds platform limit", NULL,
                          NULL, "pouch");
    if (source->input) {
      got = (ssize_t)source->input->read(
          source->input, source->cipher + source->cipher_end, want, error);
      if (error && error->code != LC_OK)
        return error->code;
      if ((size_t)got > want)
        return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                            "payload source exceeded requested chunk", NULL,
                            NULL, "pouch");
    } else {
      do {
        got = pread(source->fd, source->cipher + source->cipher_end, want,
                    target);
      } while (got < 0 && errno == EINTR);
    }
    if (got < 0)
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to read encrypted pouch payload",
                          strerror(errno), NULL, "pouch");
    if (got == 0)
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "encrypted pouch payload is truncated", NULL, NULL,
                          "pouch");
    source->cipher_end += (size_t)got;
    source->read_offset += (uint64_t)got;
  }
  return LC_OK;
}

static int lc_pouch_crypto_read_frame(lc_pouch_crypto_source *source,
                                      unsigned char *output,
                                      size_t *output_size, lc_error *error) {
  unsigned char header[8];
  unsigned char nonce[LC_POUCH_NONCE_BYTES];
  unsigned char *tag;
  const unsigned char *cipher;
  size_t frame_len;
  unsigned long counter;
  unsigned long plain_len;
  int out_len;
  int final_len;
  int available;
  int ok;
  int rc;

  available = 0;
  if (output_size != NULL)
    *output_size = 0U;
  if (source->bounded && source->span_remaining < sizeof(header)) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "encrypted pouch payload span is truncated", NULL, NULL,
                        "pouch");
  }
  rc = lc_pouch_crypto_frame_fill(source, sizeof(header), error);
  if (rc != LC_OK) {
    return rc;
  }
  memcpy(header, source->cipher + source->cipher_begin, sizeof(header));
  counter = lc_pouch_crypto_get32(header);
  plain_len = lc_pouch_crypto_get32(header + 4U);
  if (counter != source->counter ||
      plain_len > LC_POUCH_FRAME_PLAINTEXT_BYTES) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "encrypted pouch payload frame header is invalid", NULL,
                        NULL, "pouch");
  }
  frame_len = sizeof(header) + (size_t)plain_len + LC_POUCH_GCM_TAG_BYTES;
  if (source->bounded) {
    if (source->span_remaining < (uint64_t)frame_len) {
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "encrypted pouch payload span frame is truncated",
                          NULL, NULL, "pouch");
    }
  }
  rc = lc_pouch_crypto_frame_fill(source, frame_len, error);
  if (rc != LC_OK) {
    return rc;
  }
  cipher = source->cipher + source->cipher_begin + sizeof(header);
  tag = source->cipher + source->cipher_begin + sizeof(header) + plain_len;
  lc_pouch_crypto_nonce(source->nonce_prefix, counter, nonce);
  /* Retain the selected cipher, but start independent GCM authentication with
   * this payload's key and this frame's nonce, AAD and tag on every frame. */
  ok = EVP_DecryptInit_ex(source->ctx, NULL, NULL, source->key, nonce) == 1;
  if (ok) {
    rc = lc_pouch_crypto_frame_aad_decrypt(
        source->ctx, source->context, source->context_len,
        source->descriptor_core, source->descriptor_core_len, header, error);
    ok = rc == LC_OK;
  }
  if (ok) {
    /* Include tentative plaintext from a failed authentication, and retain
     * the high-water mark across replay and shorter/terminal frames. */
    if (output == NULL && (size_t)plain_len > source->plain_used)
      source->plain_used = (size_t)plain_len;
    ok = EVP_DecryptUpdate(source->ctx, output != NULL ? output : source->plain,
                           &out_len, cipher, (int)plain_len) == 1 &&
         EVP_CIPHER_CTX_ctrl(source->ctx, EVP_CTRL_GCM_SET_TAG,
                             LC_POUCH_GCM_TAG_BYTES, tag) == 1 &&
         EVP_DecryptFinal_ex(
             source->ctx, (output != NULL ? output : source->plain) + out_len,
             &final_len) == 1;
  }
  OPENSSL_cleanse(nonce, sizeof(nonce));
  if (!ok || (unsigned long)(out_len + final_len) != plain_len) {
    if (output != NULL)
      OPENSSL_cleanse(output, (size_t)plain_len);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "encrypted pouch payload authentication failed", NULL,
                        NULL, "pouch");
  }
  source->cipher_begin += frame_len;
  if (source->bounded)
    source->span_remaining -= (uint64_t)frame_len;
  ++source->counter;
  source->plain_offset = 0U;
  source->plain_length = output != NULL ? 0U : (size_t)plain_len;
  if (output_size != NULL)
    *output_size = (size_t)plain_len;
  if (plain_len == 0UL) {
    if (source->bounded) {
      if (source->span_remaining != 0UL) {
        return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                            "encrypted pouch payload span has trailing bytes",
                            NULL, NULL, "pouch");
      }
    } else {
      available = source->cipher_begin != source->cipher_end;
      if (!available) {
        rc = lc_pouch_crypto_pread_available(source->fd, source->read_offset,
                                             &available, error);
        if (rc != LC_OK)
          return rc;
      }
      if (available) {
        return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                            "encrypted pouch payload has trailing bytes", NULL,
                            NULL, "pouch");
      }
    }
    source->done = 1;
  }
  return LC_OK;
}

static size_t lc_pouch_crypto_source_read(lc_source *self, void *buffer,
                                          size_t count, lc_error *error) {
  lc_pouch_crypto_source *source;
  size_t copied;

  if (self == NULL || buffer == NULL || count == 0U) {
    return 0U;
  }
  source = (lc_pouch_crypto_source *)self->impl;
  copied = 0U;
  while (copied < count) {
    size_t available;
    size_t take;

    available = source->plain_length - source->plain_offset;
    if (available == 0U) {
      int rc;

      if (source->done) {
        break;
      }
      rc = lc_pouch_crypto_read_frame(source, NULL, NULL, error);
      if (rc != LC_OK) {
        return copied;
      }
      available = source->plain_length - source->plain_offset;
      if (available == 0U && source->done) {
        break;
      }
    }
    take = count - copied;
    if (take > available) {
      take = available;
    }
    memcpy((unsigned char *)buffer + copied,
           source->plain + source->plain_offset, take);
    source->plain_offset += take;
    copied += take;
  }
  return copied;
}

static size_t lc_pouch_crypto_reader_chunk_read(lc_source *source, void *buffer,
                                                size_t capacity,
                                                lc_error *error) {
  lc_pouch_crypto_reader *reader = source->impl;
  size_t got = 0U;
  if (buffer == NULL) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch chunk read requires source and buffer", NULL,
                       NULL, "pouch");
    return 0U;
  }
  if (capacity == 0U)
    return 0U;
  if (reader->decryptor->plain_offset == reader->decryptor->plain_length &&
      capacity >= LC_POUCH_FRAME_PLAINTEXT_BYTES) {
    if (!reader->decryptor->done &&
        lc_pouch_crypto_read_frame(reader->decryptor, buffer, &got, error) !=
            LC_OK)
      return 0U;
    return got;
  }
  return source->read(source, buffer, capacity, error);
}

lc_pouch_crypto_read_fn
lc_pouch_crypto_reader_select_read(lc_pouch_crypto_reader *reader,
                                   lc_source *source) {
  if (reader != NULL && source == &reader->pub && reader->decryptor != NULL &&
      reader->active == &reader->decryptor->pub)
    return lc_pouch_crypto_reader_chunk_read;
  return source->read;
}

static int lc_pouch_crypto_source_reset(lc_source *self, lc_error *error) {
  lc_pouch_crypto_source *source;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto source reset requires source", NULL, NULL,
                        "pouch");
  }
  source = (lc_pouch_crypto_source *)self->impl;
  source->cipher_begin = source->cipher_end = 0U;
  if (source->input && source->input->reset) {
    int rc = source->input->reset(source->input, error);
    if (rc != LC_OK)
      return rc;
  }
  if (source->bounded) {
    source->read_offset = source->span_offset;
    source->span_remaining = source->span_length;
    source->plain_offset = 0U;
    source->plain_length = 0U;
    source->counter = 0UL;
    source->done = 0;
    return LC_OK;
  }
  source->read_offset = 0UL;
  source->plain_offset = 0U;
  source->plain_length = 0U;
  source->counter = 0UL;
  source->done = 0;
  return LC_OK;
}

static void lc_pouch_crypto_source_close(lc_source *self) {
  lc_pouch_crypto_source *source;

  if (self == NULL) {
    return;
  }
  source = (lc_pouch_crypto_source *)self->impl;
  if (source->fd >= 0) {
    close(source->fd);
  }
  EVP_CIPHER_CTX_free(source->ctx);
  OPENSSL_cleanse(source->key, sizeof(source->key));
  OPENSSL_cleanse(source->plain, sizeof(source->plain));
  lc_free_with_allocator(&source->allocator, source->context);
  lc_free_with_allocator(&source->allocator, source);
}

static size_t lc_pouch_plain_span_source_read(lc_source *self, void *buffer,
                                              size_t count, lc_error *error) {
  lc_pouch_plain_span_source *source;
  uint64_t remaining;
  uint64_t position;
  off_t target;
  ssize_t nread;
  size_t want;

  if (self == NULL || buffer == NULL || count == 0U) {
    return 0U;
  }
  source = (lc_pouch_plain_span_source *)self->impl;
  if (source == NULL || source->read_bytes >= source->length) {
    return 0U;
  }
  remaining = source->length - source->read_bytes;
  want = (uint64_t)count < remaining ? count : (size_t)remaining;
  if (source->offset > LC_U64_MAX - source->read_bytes) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch payload offset exceeds platform limit", NULL,
                       NULL, "pouch");
    return 0U;
  }
  position = source->offset + source->read_bytes;
  target = (off_t)position;
  if (target < 0 || (uint64_t)target != position) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch payload offset exceeds platform limit", NULL,
                       NULL, "pouch");
    return 0U;
  }
  do {
    nread = pread(source->fd, buffer, want, target);
  } while (nread < 0 && errno == EINTR);
  if (nread < 0) {
    (void)lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                       "failed to read pouch payload span", strerror(errno),
                       NULL, "pouch");
    return 0U;
  }
  if (nread == 0) {
    (void)lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                       "pouch payload span is truncated", NULL, NULL, "pouch");
    return 0U;
  }
  source->read_bytes += (uint64_t)nread;
  return (size_t)nread;
}

static int lc_pouch_plain_span_source_reset(lc_source *self, lc_error *error) {
  lc_pouch_plain_span_source *source;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch payload span reset requires source", NULL, NULL,
                        "pouch");
  }
  source = (lc_pouch_plain_span_source *)self->impl;
  if (source == NULL || source->fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to reset pouch payload span", strerror(errno),
                        NULL, "pouch");
  }
  source->read_bytes = 0UL;
  return LC_OK;
}
