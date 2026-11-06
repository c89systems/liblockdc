#include "lc_pouch_path.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static int lc_pouch_path_char_safe(unsigned char value) {
  return (value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z') ||
         (value >= '0' && value <= '9') || value == '-' || value == '_' ||
         value == '.';
}

static int lc_pouch_path_dot_only(const char *name) {
  const char *cursor;

  if (name == NULL || name[0] == '\0') {
    return 0;
  }
  for (cursor = name; *cursor != '\0'; ++cursor) {
    if (*cursor != '.') {
      return 0;
    }
  }
  return 1;
}

static int lc_pouch_path_hex_value(unsigned char value) {
  if (value >= '0' && value <= '9') {
    return (int)(value - '0');
  }
  if (value >= 'a' && value <= 'f') {
    return (int)(value - 'a' + 10);
  }
  if (value >= 'A' && value <= 'F') {
    return (int)(value - 'A' + 10);
  }
  return -1;
}

char *lc_pouch_path_join(const lc_allocator *allocator, const char *root,
                         const char *leaf) {
  char *path;
  size_t root_len;
  size_t leaf_len;
  int has_slash;

  if (root == NULL || leaf == NULL) {
    return NULL;
  }
  root_len = strlen(root);
  leaf_len = strlen(leaf);
  has_slash = root_len > 0U && root[root_len - 1U] == '/';
  path = (char *)lc_alloc_with_allocator(
      allocator, root_len + (has_slash ? 0U : 1U) + leaf_len + 1U);
  if (path == NULL) {
    return NULL;
  }
  memcpy(path, root, root_len);
  if (!has_slash) {
    path[root_len++] = '/';
  }
  memcpy(path + root_len, leaf, leaf_len + 1U);
  return path;
}

char *lc_pouch_path_escape_name(const lc_allocator *allocator,
                                const char *name) {
  static const char hex[] = "0123456789abcdef";
  const unsigned char *cursor;
  char *escaped;
  char *dst;
  size_t length;
  int dot_only;

  if (name == NULL || name[0] == '\0') {
    return NULL;
  }
  dot_only = lc_pouch_path_dot_only(name);
  length = 0U;
  cursor = (const unsigned char *)name;
  while (*cursor != '\0') {
    length += !dot_only && lc_pouch_path_char_safe(*cursor) ? 1U : 3U;
    ++cursor;
  }
  escaped = (char *)lc_alloc_with_allocator(allocator, length + 1U);
  if (escaped == NULL) {
    return NULL;
  }
  dst = escaped;
  cursor = (const unsigned char *)name;
  while (*cursor != '\0') {
    if (!dot_only && lc_pouch_path_char_safe(*cursor)) {
      *dst++ = (char)*cursor;
    } else {
      *dst++ = '%';
      *dst++ = hex[*cursor >> 4];
      *dst++ = hex[*cursor & 0x0fU];
    }
    ++cursor;
  }
  *dst = '\0';
  return escaped;
}

char *lc_pouch_path_unescape_name(const lc_allocator *allocator,
                                  const char *name) {
  const char *cursor;
  char *unescaped;
  char *dst;
  size_t length;

  if (name == NULL || name[0] == '\0') {
    return NULL;
  }
  length = strlen(name);
  unescaped = (char *)lc_alloc_with_allocator(allocator, length + 1U);
  if (unescaped == NULL) {
    return NULL;
  }
  cursor = name;
  dst = unescaped;
  while (*cursor != '\0') {
    if (*cursor == '%') {
      int high;
      int low;

      if (cursor[1] == '\0' || cursor[2] == '\0') {
        lc_free_with_allocator(allocator, unescaped);
        return NULL;
      }
      high = lc_pouch_path_hex_value((unsigned char)cursor[1]);
      low = lc_pouch_path_hex_value((unsigned char)cursor[2]);
      if (high < 0 || low < 0) {
        lc_free_with_allocator(allocator, unescaped);
        return NULL;
      }
      *dst++ = (char)(((unsigned int)high << 4U) | (unsigned int)low);
      cursor += 3;
    } else {
      *dst++ = *cursor++;
    }
  }
  *dst = '\0';
  if (unescaped[0] == '\0') {
    lc_free_with_allocator(allocator, unescaped);
    return NULL;
  }
  return unescaped;
}

int lc_pouch_path_ensure_directory(const char *path, const char *message,
                                   lc_error *error) {
  struct stat st;

  if (path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
  }
  if (mkdir(path, 0777) != 0 && errno != EEXIST) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L, message, strerror(errno),
                        NULL, NULL);
  }
  if (stat(path, &st) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L, message, strerror(errno),
                        NULL, NULL);
  }
  if (!S_ISDIR(st.st_mode)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, message,
                        "path exists but is not a directory", NULL, NULL);
  }
  return LC_OK;
}

int lc_pouch_path_fsync_directory(const char *path, const char *message,
                                  lc_error *error) {
  int fd;
  int flags;
  int rc;

  if (path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
  }
  flags = O_RDONLY;
#ifdef O_DIRECTORY
  flags |= O_DIRECTORY;
#endif
  fd = open(path, flags);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L, message, strerror(errno),
                        NULL, NULL);
  }
  rc = LC_OK;
  if (fsync(fd) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L, message, strerror(errno),
                      NULL, NULL);
  }
  if (close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L, message, strerror(errno),
                      NULL, NULL);
  }
  return rc;
}

static int lc_pouch_path_write_all(int fd, const char *text, size_t length) {
  size_t offset;

  offset = 0U;
  while (offset < length) {
    ssize_t written;

    written = write(fd, text + offset, length - offset);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return 0;
    }
    if (written == 0) {
      return 0;
    }
    offset += (size_t)written;
  }
  return 1;
}

static char *lc_pouch_path_dirname(const char *path) {
  const char *slash;
  char *out;
  size_t len;

  if (path == NULL) {
    return NULL;
  }
  slash = strrchr(path, '/');
  if (slash == NULL) {
    return lc_strdup_with_allocator(NULL, ".");
  }
  if (slash == path) {
    return lc_strdup_with_allocator(NULL, "/");
  }
  len = (size_t)(slash - path);
  out = (char *)lc_alloc_with_allocator(NULL, len + 1U);
  if (out == NULL) {
    return NULL;
  }
  memcpy(out, path, len);
  out[len] = '\0';
  return out;
}

static char *lc_pouch_path_temp_path(const char *path, unsigned int attempt) {
  char suffix[64];
  char *out;
  size_t path_len;
  size_t suffix_len;

  snprintf(suffix, sizeof(suffix), ".tmp.%ld.%u", (long)getpid(), attempt);
  path_len = strlen(path);
  suffix_len = strlen(suffix);
  out = (char *)lc_alloc_with_allocator(NULL, path_len + suffix_len + 1U);
  if (out == NULL) {
    return NULL;
  }
  memcpy(out, path, path_len);
  memcpy(out + path_len, suffix, suffix_len + 1U);
  return out;
}

static int lc_pouch_path_write_bytes_file_impl(const char *path,
                                               const char *bytes, size_t length,
                                               int sync_file,
                                               int sync_directory, int mode,
                                               lc_error *error) {
  char *dir;
  char *tmp_path;
  unsigned int attempt;
  int fd;
  int rc;

  if (path == NULL || (bytes == NULL && length > 0U)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch file write requires path and bytes", NULL, NULL,
                        NULL);
  }
  dir = lc_pouch_path_dirname(path);
  if (dir == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch file directory", NULL, NULL,
                        NULL);
  }
  tmp_path = NULL;
  fd = -1;
  for (attempt = 0U; attempt < 100U; ++attempt) {
    tmp_path = lc_pouch_path_temp_path(path, attempt);
    if (tmp_path == NULL) {
      lc_free_with_allocator(NULL, dir);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch temp file path", NULL, NULL,
                          NULL);
    }
    fd = open(tmp_path, O_WRONLY | O_CREAT | O_EXCL, mode < 0 ? 0666 : 0600);
    if (fd >= 0 || errno != EEXIST) {
      break;
    }
    lc_free_with_allocator(NULL, tmp_path);
    tmp_path = NULL;
  }
  if (fd < 0) {
    lc_free_with_allocator(NULL, tmp_path);
    lc_free_with_allocator(NULL, dir);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch file", strerror(errno), NULL,
                        NULL);
  }
  rc = LC_OK;
  if (!lc_pouch_path_write_all(fd, bytes != NULL ? bytes : "", length)) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L, "failed to write pouch file",
                      strerror(errno), NULL, NULL);
  } else if (mode >= 0 && fchmod(fd, (mode_t)mode) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to preserve pouch file permissions",
                      strerror(errno), NULL, NULL);
  } else if (sync_file && fsync(fd) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L, "failed to fsync pouch file",
                      strerror(errno), NULL, NULL);
  }
  if (close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L, "failed to close pouch file",
                      strerror(errno), NULL, NULL);
  }
  if (rc == LC_OK && rename(tmp_path, path) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to replace pouch file", strerror(errno), NULL,
                      NULL);
  }
  if (rc == LC_OK && sync_directory) {
    rc = lc_pouch_path_fsync_directory(dir, "failed to fsync pouch directory",
                                       error);
  }
  if (rc != LC_OK) {
    unlink(tmp_path);
  }
  lc_free_with_allocator(NULL, tmp_path);
  lc_free_with_allocator(NULL, dir);
  return rc;
}

int lc_pouch_path_write_text_file(const char *path, const char *text,
                                  lc_error *error) {
  return lc_pouch_path_write_bytes_file_impl(
      path, text, text != NULL ? strlen(text) : 0U, 1, 1, -1, error);
}

int lc_pouch_path_write_bytes_file(const char *path, const void *bytes,
                                   size_t length, lc_error *error) {
  return lc_pouch_path_write_bytes_file_impl(path, (const char *)bytes, length,
                                             1, 1, -1, error);
}

int lc_pouch_path_write_bytes_file_mode(const char *path, const void *bytes,
                                        size_t length, unsigned int mode,
                                        lc_error *error) {
  return lc_pouch_path_write_bytes_file_impl(path, (const char *)bytes, length,
                                             1, 1, (int)(mode & 0777U), error);
}

int lc_pouch_path_write_text_file_relaxed(const char *path, const char *text,
                                          lc_error *error) {
  return lc_pouch_path_write_bytes_file_impl(
      path, text, text != NULL ? strlen(text) : 0U, 0, 0, -1, error);
}

int lc_pouch_path_write_bytes_file_relaxed(const char *path, const char *bytes,
                                           size_t length, lc_error *error) {
  return lc_pouch_path_write_bytes_file_impl(path, bytes, length, 0, 0, -1,
                                             error);
}
