#ifndef LC_POUCH_PATH_H
#define LC_POUCH_PATH_H

#include "lc_api_internal.h"

char *lc_pouch_path_join(const lc_allocator *allocator, const char *root,
                         const char *leaf);
char *lc_pouch_path_escape_name(const lc_allocator *allocator,
                                const char *name);
char *lc_pouch_path_unescape_name(const lc_allocator *allocator,
                                  const char *name);
int lc_pouch_path_ensure_directory(const char *path, const char *message,
                                   lc_error *error);
int lc_pouch_path_fsync_directory(const char *path, const char *message,
                                  lc_error *error);
int lc_pouch_path_write_text_file(const char *path, const char *text,
                                  lc_error *error);
int lc_pouch_path_write_bytes_file(const char *path, const void *bytes,
                                   size_t length, lc_error *error);
/* Atomic durable replacement, retaining existing metadata permissions. The
 * temporary file starts private, before its final mode is restored. */
int lc_pouch_path_write_bytes_file_mode(const char *path, const void *bytes,
                                        size_t length, unsigned int mode,
                                        lc_error *error);
int lc_pouch_path_write_text_file_relaxed(const char *path, const char *text,
                                          lc_error *error);
int lc_pouch_path_write_bytes_file_relaxed(const char *path, const char *bytes,
                                           size_t length, lc_error *error);

#endif
