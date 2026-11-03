#include "lc_pouch_namespace.h"

#include "lc_api_internal.h"
#include "lc_intcompat.h"
#include "lc_pouch_format.h"
#include "lc_pouch_path.h"

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define LC_POUCH_SEGMENT_PREFIX "seg-"
#define LC_POUCH_SEGMENT_SUFFIX ".log"
#define LC_POUCH_SNAPSHOT_PREFIX "snapshot-"
#define LC_POUCH_SNAPSHOT_SUFFIX ".log"

typedef lc_pouch_namespace_marker_peer_stat lc_pouch_marker_entry;

static int lc_pouch_namespace_parse_u64(const char *text, size_t length,
                                        uint64_t *out_value) {
  lc_u64 value;

  if (out_value == NULL ||
      !lc_parse_u64_base10_range_checked(text, length, &value)) {
    return 0;
  }
  *out_value = (uint64_t)value;
  return 1;
}

char *lc_pouch_namespace_path(const lc_allocator *allocator,
                              const char *root_path, const char *ns) {
  char *namespaces_path;
  char *escaped;
  char *path;

  namespaces_path = lc_pouch_path_join(allocator, root_path, "namespaces");
  escaped = lc_pouch_path_escape_name(allocator, ns);
  path = namespaces_path != NULL && escaped != NULL
             ? lc_pouch_path_join(allocator, namespaces_path, escaped)
             : NULL;
  lc_free_with_allocator(allocator, namespaces_path);
  lc_free_with_allocator(allocator, escaped);
  return path;
}

char *lc_pouch_namespace_segment_leaf(const lc_allocator *allocator,
                                      uint64_t segment_id) {
  char stack[96];
  char id[32];

  if (lc_u64_format_base10_padded((lc_u64)segment_id, 20U, id, sizeof(id)) <
          0 ||
      snprintf(stack, sizeof(stack), "%s%s%s", LC_POUCH_SEGMENT_PREFIX, id,
               LC_POUCH_SEGMENT_SUFFIX) < 0) {
    return NULL;
  }
  return lc_strdup_with_allocator(allocator, stack);
}

char *lc_pouch_namespace_snapshot_leaf(const lc_allocator *allocator,
                                       uint64_t segment_id) {
  char stack[96];
  char id[32];

  if (lc_u64_format_base10_padded((lc_u64)segment_id, 20U, id, sizeof(id)) <
          0 ||
      snprintf(stack, sizeof(stack), "%s%s%s", LC_POUCH_SNAPSHOT_PREFIX, id,
               LC_POUCH_SNAPSHOT_SUFFIX) < 0) {
    return NULL;
  }
  return lc_strdup_with_allocator(allocator, stack);
}

static int lc_pouch_namespace_ensure_child(const lc_allocator *allocator,
                                           const char *namespace_path,
                                           const char *child,
                                           const char *message,
                                           lc_error *error) {
  char *path;
  int rc;

  path = lc_pouch_path_join(allocator, namespace_path, child);
  if (path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace path", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_path_ensure_directory(path, message, error);
  lc_free_with_allocator(allocator, path);
  return rc;
}

static int lc_pouch_namespace_parse_segment_id(const char *leaf,
                                               uint64_t *segment_id) {
  const char *digits;
  uint64_t value;
  size_t prefix_len;
  size_t suffix_len;
  size_t leaf_len;
  size_t digits_len;
  size_t i;

  if (leaf == NULL || segment_id == NULL) {
    return 0;
  }
  prefix_len = strlen(LC_POUCH_SEGMENT_PREFIX);
  suffix_len = strlen(LC_POUCH_SEGMENT_SUFFIX);
  leaf_len = strlen(leaf);
  if (leaf_len <= prefix_len + suffix_len ||
      strncmp(leaf, LC_POUCH_SEGMENT_PREFIX, prefix_len) != 0 ||
      strcmp(leaf + leaf_len - suffix_len, LC_POUCH_SEGMENT_SUFFIX) != 0) {
    return 0;
  }
  digits = leaf + prefix_len;
  digits_len = leaf_len - prefix_len - suffix_len;
  for (i = 0U; i < digits_len; ++i) {
    if (digits[i] < '0' || digits[i] > '9') {
      return 0;
    }
  }
  if (!lc_pouch_namespace_parse_u64(digits, digits_len, &value) ||
      value == 0U) {
    return 0;
  }
  *segment_id = value;
  return 1;
}

int lc_pouch_namespace_parse_segment_leaf(const char *leaf,
                                          uint64_t *segment_id) {
  return lc_pouch_namespace_parse_segment_id(leaf, segment_id);
}

static int lc_pouch_namespace_parse_snapshot_id(const char *leaf,
                                                uint64_t *snapshot_id) {
  const char *digits;
  uint64_t value;
  size_t prefix_len;
  size_t suffix_len;
  size_t leaf_len;
  size_t digits_len;
  size_t i;

  if (leaf == NULL || snapshot_id == NULL) {
    return 0;
  }
  prefix_len = strlen(LC_POUCH_SNAPSHOT_PREFIX);
  suffix_len = strlen(LC_POUCH_SNAPSHOT_SUFFIX);
  leaf_len = strlen(leaf);
  if (leaf_len <= prefix_len + suffix_len ||
      strncmp(leaf, LC_POUCH_SNAPSHOT_PREFIX, prefix_len) != 0 ||
      strcmp(leaf + leaf_len - suffix_len, LC_POUCH_SNAPSHOT_SUFFIX) != 0) {
    return 0;
  }
  digits = leaf + prefix_len;
  digits_len = leaf_len - prefix_len - suffix_len;
  for (i = 0U; i < digits_len; ++i) {
    if (digits[i] < '0' || digits[i] > '9') {
      return 0;
    }
  }
  if (!lc_pouch_namespace_parse_u64(digits, digits_len, &value) ||
      value == 0U) {
    return 0;
  }
  *snapshot_id = value;
  return 1;
}

static int lc_pouch_namespace_manifest_list_append(
    const lc_allocator *allocator, char ***items, uint64_t **marked_at,
    unsigned long *count, const char *leaf, uint64_t marked_at_unix,
    lc_error *error) {
  char **grown;
  uint64_t *grown_marked_at;
  char *copy;
  unsigned long i;

  if (items == NULL || marked_at == NULL || count == NULL || leaf == NULL ||
      leaf[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch manifest obsolete entry requires a leaf", NULL,
                        NULL, NULL);
  }
  for (i = 0UL; i < *count; ++i) {
    if (strcmp((*items)[i], leaf) == 0) {
      return LC_OK;
    }
  }
  if (*count == (unsigned long)(((size_t)-1) / sizeof(**items))) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch manifest obsolete entries",
                        NULL, NULL, NULL);
  }
  grown = (char **)lc_realloc_with_allocator(
      allocator, *items, ((size_t)(*count) + 1U) * sizeof(**items));
  if (grown == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch manifest obsolete entries",
                        NULL, NULL, NULL);
  }
  *items = grown;
  grown_marked_at = (uint64_t *)lc_realloc_with_allocator(
      allocator, *marked_at, ((size_t)(*count) + 1U) * sizeof(**marked_at));
  if (grown_marked_at == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch manifest obsolete times",
                        NULL, NULL, NULL);
  }
  *marked_at = grown_marked_at;
  copy = lc_strdup_with_allocator(allocator, leaf);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch manifest obsolete entry",
                        NULL, NULL, NULL);
  }
  grown[*count] = copy;
  grown_marked_at[*count] = marked_at_unix;
  ++*count;
  return LC_OK;
}

static int lc_pouch_namespace_manifest_list_contains(char **items,
                                                     unsigned long count,
                                                     const char *leaf) {
  unsigned long i;

  if (leaf == NULL) {
    return 0;
  }
  for (i = 0UL; i < count; ++i) {
    if (strcmp(items[i], leaf) == 0) {
      return 1;
    }
  }
  return 0;
}

static void
lc_pouch_namespace_manifest_list_cleanup(const lc_allocator *allocator,
                                         char **items, uint64_t *marked_at,
                                         unsigned long count) {
  unsigned long i;

  if (items == NULL) {
    return;
  }
  for (i = 0UL; i < count; ++i) {
    lc_free_with_allocator(allocator, items[i]);
  }
  lc_free_with_allocator(allocator, items);
  lc_free_with_allocator(allocator, marked_at);
}

static int lc_pouch_namespace_segment_leaf_compare(const void *left,
                                                   const void *right) {
  const char *const *a;
  const char *const *b;

  a = (const char *const *)left;
  b = (const char *const *)right;
  return strcmp(*a, *b);
}

static int lc_pouch_namespace_scan_segments(const lc_allocator *allocator,
                                            const char *namespace_path,
                                            uint64_t *max_segment_id,
                                            char ***segment_leaves,
                                            unsigned long *segment_count,
                                            lc_error *error) {
  char *segments_path;
  DIR *dir;
  struct dirent *entry;
  char **leaves;
  unsigned long count;
  unsigned long capacity;

  *max_segment_id = 0UL;
  *segment_leaves = NULL;
  *segment_count = 0UL;
  leaves = NULL;
  count = 0UL;
  capacity = 0UL;
  segments_path = lc_pouch_path_join(allocator, namespace_path, "segments");
  if (segments_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch segments path", NULL, NULL,
                        NULL);
  }
  dir = opendir(segments_path);
  lc_free_with_allocator(allocator, segments_path);
  if (dir == NULL) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch namespace segments", NULL, NULL,
                        NULL);
  }
  while ((entry = readdir(dir)) != NULL) {
    uint64_t segment_id;
    int valid;

    valid = lc_pouch_namespace_parse_segment_id(entry->d_name, &segment_id);
    if (!valid) {
      continue;
    }
    if (lc_pouch_namespace_parse_segment_id(entry->d_name, &segment_id) &&
        segment_id > *max_segment_id) {
      *max_segment_id = segment_id;
    }
    if (count == capacity) {
      char **grown;
      unsigned long next_capacity = capacity == 0UL ? 8UL : capacity * 2UL;

      if (next_capacity <= capacity ||
          next_capacity > (unsigned long)(((size_t)-1) / sizeof(*leaves))) {
        closedir(dir);
        lc_pouch_namespace_manifest_list_cleanup(allocator, leaves, NULL,
                                                 count);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "pouch segment list exceeds local limit", NULL,
                            NULL, NULL);
      }
      grown = (char **)lc_realloc_with_allocator(
          allocator, leaves, (size_t)next_capacity * sizeof(*leaves));
      if (grown == NULL) {
        closedir(dir);
        lc_pouch_namespace_manifest_list_cleanup(allocator, leaves, NULL,
                                                 count);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch segment list", NULL, NULL,
                            NULL);
      }
      leaves = grown;
      capacity = next_capacity;
    }
    leaves[count] = lc_strdup_with_allocator(allocator, entry->d_name);
    if (leaves[count] == NULL) {
      closedir(dir);
      lc_pouch_namespace_manifest_list_cleanup(allocator, leaves, NULL, count);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy pouch segment name", NULL, NULL,
                          NULL);
    }
    ++count;
  }
  closedir(dir);
  if (count > 1UL) {
    qsort(leaves, (size_t)count, sizeof(*leaves),
          lc_pouch_namespace_segment_leaf_compare);
  }
  *segment_leaves = leaves;
  *segment_count = count;
  return LC_OK;
}

static int lc_pouch_namespace_scan_snapshots(const lc_allocator *allocator,
                                             const char *namespace_path,
                                             uint64_t *max_snapshot_id,
                                             char **latest_snapshot,
                                             lc_error *error) {
  char *snapshots_path;
  DIR *dir;
  struct dirent *entry;

  *max_snapshot_id = 0UL;
  *latest_snapshot = NULL;
  snapshots_path = lc_pouch_path_join(allocator, namespace_path, "snapshots");
  if (snapshots_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch snapshots path", NULL, NULL,
                        NULL);
  }
  dir = opendir(snapshots_path);
  lc_free_with_allocator(allocator, snapshots_path);
  if (dir == NULL) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch namespace snapshots", NULL, NULL,
                        NULL);
  }
  while ((entry = readdir(dir)) != NULL) {
    uint64_t snapshot_id;

    if (lc_pouch_namespace_parse_snapshot_id(entry->d_name, &snapshot_id) &&
        snapshot_id > *max_snapshot_id) {
      char *copy;

      copy = lc_strdup_with_allocator(allocator, entry->d_name);
      if (copy == NULL) {
        closedir(dir);
        lc_free_with_allocator(allocator, *latest_snapshot);
        *latest_snapshot = NULL;
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch snapshot name", NULL,
                            NULL, NULL);
      }
      lc_free_with_allocator(allocator, *latest_snapshot);
      *latest_snapshot = copy;
      *max_snapshot_id = snapshot_id;
    }
  }
  closedir(dir);
  return LC_OK;
}

static int lc_pouch_namespace_manifest_read(
    const lc_allocator *allocator, const char *manifest_path,
    uint64_t *active_segment_id, uint64_t *snapshot_segment_id,
    lc_pouch_generation *state_max_version, char **latest_snapshot,
    char ***obsolete_segments, uint64_t **obsolete_segment_marked_at,
    unsigned long *obsolete_segment_count, char ***obsolete_snapshots,
    uint64_t **obsolete_snapshot_marked_at,
    unsigned long *obsolete_snapshot_count) {
  char line[256];
  FILE *fp;
  int ok;

  *active_segment_id = 0UL;
  *snapshot_segment_id = 0UL;
  *state_max_version = 0UL;
  *latest_snapshot = NULL;
  *obsolete_segments = NULL;
  *obsolete_segment_marked_at = NULL;
  *obsolete_segment_count = 0UL;
  *obsolete_snapshots = NULL;
  *obsolete_snapshot_marked_at = NULL;
  *obsolete_snapshot_count = 0UL;
  fp = fopen(manifest_path, "rb");
  if (fp == NULL) {
    return 0;
  }
  ok = 1;
  while (fgets(line, sizeof(line), fp) != NULL) {
    char *value;
    size_t len;

    len = strlen(line);
    if (len > 0U && line[len - 1U] == '\n') {
      line[len - 1U] = '\0';
    }
    value = strchr(line, '=');
    if (value == NULL) {
      continue;
    }
    *value++ = '\0';
    if (strcmp(line, "active_segment") == 0) {
      uint64_t parsed;

      if (lc_pouch_namespace_parse_segment_id(value, &parsed)) {
        *active_segment_id = parsed;
      }
    } else if (strcmp(line, "snapshot") == 0) {
      uint64_t parsed;

      if (lc_pouch_namespace_parse_snapshot_id(value, &parsed)) {
        char *copy;

        copy = lc_strdup_with_allocator(allocator, value);
        if (copy == NULL) {
          fclose(fp);
          lc_free_with_allocator(allocator, *latest_snapshot);
          *latest_snapshot = NULL;
          *snapshot_segment_id = 0UL;
          return 0;
        }
        lc_free_with_allocator(allocator, *latest_snapshot);
        *latest_snapshot = copy;
        *snapshot_segment_id = parsed;
      }
    } else if (strcmp(line, "state_max_version") == 0) {
      uint64_t parsed;

      if (lc_pouch_namespace_parse_u64(value, strlen(value), &parsed)) {
        *state_max_version = parsed;
      }
    } else if (strcmp(line, "obsolete_segment") == 0) {
      uint64_t parsed;
      char *timestamp;
      uint64_t marked_at_unix;

      timestamp = strchr(value, '\t');
      if (timestamp == NULL) {
        ok = 0;
        break;
      }
      *timestamp++ = '\0';
      if (!lc_pouch_namespace_parse_u64(timestamp, strlen(timestamp),
                                        &marked_at_unix) ||
          marked_at_unix == 0U ||
          !lc_pouch_namespace_parse_segment_id(value, &parsed) ||
          lc_pouch_namespace_manifest_list_append(
              allocator, obsolete_segments, obsolete_segment_marked_at,
              obsolete_segment_count, value, marked_at_unix, NULL) != LC_OK) {
        ok = 0;
        break;
      }
    } else if (strcmp(line, "obsolete_snapshot") == 0) {
      uint64_t parsed;
      char *timestamp;
      uint64_t marked_at_unix;

      timestamp = strchr(value, '\t');
      if (timestamp == NULL) {
        ok = 0;
        break;
      }
      *timestamp++ = '\0';
      if (!lc_pouch_namespace_parse_u64(timestamp, strlen(timestamp),
                                        &marked_at_unix) ||
          marked_at_unix == 0U ||
          !lc_pouch_namespace_parse_snapshot_id(value, &parsed) ||
          lc_pouch_namespace_manifest_list_append(
              allocator, obsolete_snapshots, obsolete_snapshot_marked_at,
              obsolete_snapshot_count, value, marked_at_unix, NULL) != LC_OK) {
        ok = 0;
        break;
      }
    }
  }
  fclose(fp);
  if (!ok) {
    lc_free_with_allocator(allocator, *latest_snapshot);
    *latest_snapshot = NULL;
    lc_pouch_namespace_manifest_list_cleanup(allocator, *obsolete_segments,
                                             *obsolete_segment_marked_at,
                                             *obsolete_segment_count);
    *obsolete_segments = NULL;
    *obsolete_segment_marked_at = NULL;
    *obsolete_segment_count = 0UL;
    lc_pouch_namespace_manifest_list_cleanup(allocator, *obsolete_snapshots,
                                             *obsolete_snapshot_marked_at,
                                             *obsolete_snapshot_count);
    *obsolete_snapshots = NULL;
    *obsolete_snapshot_marked_at = NULL;
    *obsolete_snapshot_count = 0UL;
    *snapshot_segment_id = 0UL;
    return 0;
  }
  return *active_segment_id != 0UL;
}

static int lc_pouch_namespace_manifest_text_append(
    const lc_allocator *allocator, char **text, size_t *length,
    size_t *capacity, const char *line, lc_error *error) {
  char *grown;
  size_t line_len;
  size_t required;
  size_t next_capacity;

  line_len = strlen(line);
  required = *length + line_len + 1U;
  if (required < *length) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace manifest", NULL,
                        NULL, NULL);
  }
  if (required > *capacity) {
    next_capacity = *capacity == 0U ? 1024U : *capacity;
    while (next_capacity < required) {
      if (next_capacity > ((size_t)-1) / 2U) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch namespace manifest", NULL,
                            NULL, NULL);
      }
      next_capacity *= 2U;
    }
    grown = (char *)lc_realloc_with_allocator(allocator, *text, next_capacity);
    if (grown == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch namespace manifest", NULL,
                          NULL, NULL);
    }
    *text = grown;
    *capacity = next_capacity;
  }
  memcpy(*text + *length, line, line_len);
  *length += line_len;
  (*text)[*length] = '\0';
  return LC_OK;
}

static int
lc_pouch_namespace_manifest_write(const lc_allocator *allocator, const char *ns,
                                  lc_pouch_namespace_manifest *manifest,
                                  lc_error *error) {
  char line[512];
  char max_segment_id[32];
  char state_max_version[32];
  char marked_at_unix[32];
  char *text;
  char *manifest_path;
  size_t length;
  size_t capacity;
  unsigned long i;
  int written;
  int rc;

  manifest_path =
      lc_pouch_path_join(allocator, manifest->namespace_path, "manifest");
  if (manifest_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace manifest path",
                        NULL, NULL, NULL);
  }
  text = NULL;
  length = 0U;
  capacity = 0U;
  if (lc_u64_format_base10((lc_u64)manifest->max_segment_id, max_segment_id,
                           sizeof(max_segment_id)) < 0 ||
      lc_u64_format_base10((lc_u64)manifest->state_max_version,
                           state_max_version, sizeof(state_max_version)) < 0) {
    lc_free_with_allocator(allocator, manifest_path);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "failed to format pouch namespace manifest", NULL, NULL,
                        NULL);
  }
  written =
      snprintf(line, sizeof(line),
               "layout=%s\nversion=%lu\nnamespace=%s\n"
               "active_segment=%s\nmax_segment_id=%s\n"
               "state_max_version=%s\n",
               LC_POUCH_LAYOUT_NAME, LC_POUCH_LAYOUT_VERSION, ns,
               manifest->active_segment, max_segment_id, state_max_version);
  if (written < 0 || (size_t)written >= sizeof(line)) {
    lc_free_with_allocator(allocator, manifest_path);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to format pouch namespace manifest", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_namespace_manifest_text_append(allocator, &text, &length,
                                               &capacity, line, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(allocator, manifest_path);
    return rc;
  }
  if (manifest->latest_snapshot != NULL) {
    written = snprintf(line, sizeof(line), "snapshot=%s\n",
                       manifest->latest_snapshot);
    if (written < 0 || (size_t)written >= sizeof(line)) {
      lc_free_with_allocator(allocator, manifest_path);
      lc_free_with_allocator(allocator, text);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to format pouch namespace manifest", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_namespace_manifest_text_append(allocator, &text, &length,
                                                 &capacity, line, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(allocator, manifest_path);
      lc_free_with_allocator(allocator, text);
      return rc;
    }
  }
  for (i = 0UL; i < manifest->obsolete_segment_count; ++i) {
    if (lc_u64_format_base10((lc_u64)manifest->obsolete_segment_marked_at[i],
                             marked_at_unix, sizeof(marked_at_unix)) < 0) {
      lc_free_with_allocator(allocator, manifest_path);
      lc_free_with_allocator(allocator, text);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "failed to format pouch namespace manifest", NULL,
                          NULL, NULL);
    }
    written = snprintf(line, sizeof(line), "obsolete_segment=%s\t%s\n",
                       manifest->obsolete_segments[i], marked_at_unix);
    if (written < 0 || (size_t)written >= sizeof(line)) {
      lc_free_with_allocator(allocator, manifest_path);
      lc_free_with_allocator(allocator, text);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to format pouch namespace manifest", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_namespace_manifest_text_append(allocator, &text, &length,
                                                 &capacity, line, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(allocator, manifest_path);
      lc_free_with_allocator(allocator, text);
      return rc;
    }
  }
  for (i = 0UL; i < manifest->obsolete_snapshot_count; ++i) {
    if (lc_u64_format_base10((lc_u64)manifest->obsolete_snapshot_marked_at[i],
                             marked_at_unix, sizeof(marked_at_unix)) < 0) {
      lc_free_with_allocator(allocator, manifest_path);
      lc_free_with_allocator(allocator, text);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "failed to format pouch namespace manifest", NULL,
                          NULL, NULL);
    }
    written = snprintf(line, sizeof(line), "obsolete_snapshot=%s\t%s\n",
                       manifest->obsolete_snapshots[i], marked_at_unix);
    if (written < 0 || (size_t)written >= sizeof(line)) {
      lc_free_with_allocator(allocator, manifest_path);
      lc_free_with_allocator(allocator, text);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to format pouch namespace manifest", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_namespace_manifest_text_append(allocator, &text, &length,
                                                 &capacity, line, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(allocator, manifest_path);
      lc_free_with_allocator(allocator, text);
      return rc;
    }
  }
  rc = lc_pouch_path_write_text_file(manifest_path, text, error);
  lc_free_with_allocator(allocator, text);
  lc_free_with_allocator(allocator, manifest_path);
  return rc;
}

static int lc_pouch_marker_entry_compare(const void *left, const void *right) {
  const lc_pouch_marker_entry *a;
  const lc_pouch_marker_entry *b;

  a = (const lc_pouch_marker_entry *)left;
  b = (const lc_pouch_marker_entry *)right;
  return strcmp(a->name, b->name);
}

static void lc_pouch_marker_entries_cleanup(const lc_allocator *allocator,
                                            lc_pouch_marker_entry *entries,
                                            size_t count) {
  size_t i;

  if (entries == NULL) {
    return;
  }
  for (i = 0U; i < count; ++i) {
    lc_free_with_allocator(allocator, entries[i].name);
  }
  lc_free_with_allocator(allocator, entries);
}

static void lc_pouch_marker_refresh_peer_stats_cleanup(
    const lc_allocator *allocator,
    lc_pouch_namespace_marker_refresh_state *state) {
  if (state == NULL) {
    return;
  }
  lc_pouch_marker_entries_cleanup(allocator, state->peer_stats,
                                  (size_t)state->peer_stat_count);
  state->peer_stats = NULL;
  state->peer_stat_count = 0UL;
}

static int lc_pouch_marker_entries_append(const lc_allocator *allocator,
                                          lc_pouch_marker_entry **entries,
                                          size_t *count, size_t *capacity,
                                          const char *name, uint64_t size,
                                          long mtime, lc_error *error) {
  lc_pouch_marker_entry *grown;
  char *name_copy;
  size_t next_capacity;

  if (*count == *capacity) {
    next_capacity = *capacity == 0U ? 8U : *capacity * 2U;
    if (next_capacity < *capacity ||
        next_capacity > ((size_t)-1) / sizeof(**entries)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch marker snapshot", NULL,
                          NULL, NULL);
    }
    grown = (lc_pouch_marker_entry *)lc_realloc_with_allocator(
        allocator, *entries, next_capacity * sizeof(**entries));
    if (grown == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch marker snapshot", NULL,
                          NULL, NULL);
    }
    memset(grown + *capacity, 0,
           (next_capacity - *capacity) * sizeof(**entries));
    *entries = grown;
    *capacity = next_capacity;
  }
  name_copy = lc_strdup_with_allocator(allocator, name);
  if (name_copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch marker name", NULL, NULL,
                        NULL);
  }
  (*entries)[*count].name = name_copy;
  (*entries)[*count].size = size;
  (*entries)[*count].mtime = mtime;
  ++*count;
  return LC_OK;
}

static int lc_pouch_marker_fingerprint_append(const lc_allocator *allocator,
                                              char **fingerprint,
                                              size_t *length, size_t *capacity,
                                              const char *text,
                                              lc_error *error) {
  char *grown;
  size_t text_len;
  size_t required;
  size_t next_capacity;

  text_len = strlen(text);
  required = *length + text_len + 1U;
  if (required < *length) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch marker fingerprint", NULL,
                        NULL, NULL);
  }
  if (required > *capacity) {
    next_capacity = *capacity == 0U ? 128U : *capacity;
    while (next_capacity < required) {
      if (next_capacity > ((size_t)-1) / 2U) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch marker fingerprint", NULL,
                            NULL, NULL);
      }
      next_capacity *= 2U;
    }
    grown = (char *)lc_realloc_with_allocator(allocator, *fingerprint,
                                              next_capacity);
    if (grown == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch marker fingerprint", NULL,
                          NULL, NULL);
    }
    *fingerprint = grown;
    *capacity = next_capacity;
  }
  memcpy(*fingerprint + *length, text, text_len);
  *length += text_len;
  (*fingerprint)[*length] = '\0';
  return LC_OK;
}

static int lc_pouch_marker_snapshot_build_fingerprint(
    const lc_allocator *allocator, lc_pouch_marker_entry *entries, size_t count,
    char **out, lc_error *error) {
  char *fingerprint;
  size_t length;
  size_t capacity;
  size_t i;
  int rc;

  fingerprint = NULL;
  length = 0U;
  capacity = 0U;
  rc = lc_pouch_marker_fingerprint_append(allocator, &fingerprint, &length,
                                          &capacity, "", error);
  if (rc != LC_OK) {
    return rc;
  }
  for (i = 0U; i < count; ++i) {
    char line[512];
    char size_text[32];
    int written;

    if (lc_u64_format_base10((lc_u64)entries[i].size, size_text,
                             sizeof(size_text)) < 0) {
      lc_free_with_allocator(allocator, fingerprint);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "failed to format pouch marker fingerprint", NULL,
                          NULL, NULL);
    }
    written = snprintf(line, sizeof(line), "%s size=%s mtime=%ld\n",
                       entries[i].name, size_text, entries[i].mtime);
    if (written < 0 || (size_t)written >= sizeof(line)) {
      lc_free_with_allocator(allocator, fingerprint);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to format pouch marker fingerprint", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_marker_fingerprint_append(allocator, &fingerprint, &length,
                                            &capacity, line, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(allocator, fingerprint);
      return rc;
    }
  }
  *out = fingerprint;
  return LC_OK;
}

static int lc_pouch_marker_entries_read(const lc_allocator *allocator,
                                        const char *namespace_path,
                                        const char *self_marker_leaf,
                                        lc_pouch_marker_entry **entries_out,
                                        size_t *count_out, lc_error *error) {
  lc_pouch_marker_entry *entries;
  char *markers_path;
  DIR *dir;
  struct dirent *entry;
  size_t count;
  size_t capacity;
  int rc;

  *entries_out = NULL;
  *count_out = 0U;
  entries = NULL;
  markers_path = lc_pouch_path_join(allocator, namespace_path, "markers");
  if (markers_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch markers path", NULL, NULL,
                        NULL);
  }
  dir = opendir(markers_path);
  if (dir == NULL) {
    lc_free_with_allocator(allocator, markers_path);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch marker directory", NULL, NULL,
                        NULL);
  }
  count = 0U;
  capacity = 0U;
  rc = LC_OK;
  while ((entry = readdir(dir)) != NULL) {
    char *marker_path;
    struct stat st;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0 ||
        (self_marker_leaf != NULL &&
         strcmp(entry->d_name, self_marker_leaf) == 0)) {
      continue;
    }
    marker_path = lc_pouch_path_join(allocator, markers_path, entry->d_name);
    if (marker_path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch marker path", NULL, NULL,
                        NULL);
      break;
    }
    if (stat(marker_path, &st) == 0 && S_ISREG(st.st_mode)) {
      if (st.st_size < 0) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch marker file has invalid size", NULL, NULL,
                          "pouch");
        lc_free_with_allocator(allocator, marker_path);
        break;
      }
      rc = lc_pouch_marker_entries_append(
          allocator, &entries, &count, &capacity, entry->d_name,
          (uint64_t)st.st_size, (long)st.st_mtime, error);
      if (rc != LC_OK) {
        lc_free_with_allocator(allocator, marker_path);
        break;
      }
    }
    lc_free_with_allocator(allocator, marker_path);
  }
  closedir(dir);
  lc_free_with_allocator(allocator, markers_path);
  if (rc != LC_OK) {
    lc_pouch_marker_entries_cleanup(allocator, entries, count);
    return rc;
  }
  if (count > 1U) {
    qsort(entries, count, sizeof(*entries), lc_pouch_marker_entry_compare);
  }
  *entries_out = entries;
  *count_out = count;
  return LC_OK;
}

int lc_pouch_namespace_manifest_open(const lc_allocator *allocator,
                                     const char *root_path, const char *ns,
                                     lc_pouch_namespace_manifest *out,
                                     unsigned long *cleanup_deleted_count,
                                     unsigned long *cleanup_pending_count,
                                     lc_error *error) {
  char *manifest_path;
  char *manifest_snapshot;
  char *scanned_snapshot;
  uint64_t active_segment_id;
  uint64_t max_segment_id;
  uint64_t manifest_snapshot_id;
  uint64_t max_snapshot_id;
  lc_pouch_generation state_max_version;
  int manifest_valid;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace manifest open requires out", NULL,
                        NULL, NULL);
  }
  if (cleanup_deleted_count != NULL) {
    *cleanup_deleted_count = 0UL;
  }
  if (cleanup_pending_count != NULL) {
    *cleanup_pending_count = 0UL;
  }
  memset(out, 0, sizeof(*out));
  out->namespace_path = lc_pouch_namespace_path(allocator, root_path, ns);
  if (out->namespace_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace path", NULL, NULL,
                        NULL);
  }
  manifest_snapshot = NULL;
  scanned_snapshot = NULL;
  state_max_version = 0UL;
  rc = lc_pouch_namespace_scan_segments(allocator, out->namespace_path,
                                        &max_segment_id, &out->segment_leaves,
                                        &out->segment_count, error);
  if (rc != LC_OK) {
    lc_pouch_namespace_manifest_cleanup(allocator, out);
    return rc;
  }
  rc = lc_pouch_namespace_scan_snapshots(allocator, out->namespace_path,
                                         &max_snapshot_id, &scanned_snapshot,
                                         error);
  if (rc != LC_OK) {
    lc_pouch_namespace_manifest_cleanup(allocator, out);
    return rc;
  }
  manifest_path =
      lc_pouch_path_join(allocator, out->namespace_path, "manifest");
  if (manifest_path == NULL) {
    lc_free_with_allocator(allocator, scanned_snapshot);
    lc_pouch_namespace_manifest_cleanup(allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace manifest path",
                        NULL, NULL, NULL);
  }
  manifest_valid = lc_pouch_namespace_manifest_read(
      allocator, manifest_path, &active_segment_id, &manifest_snapshot_id,
      &state_max_version, &manifest_snapshot, &out->obsolete_segments,
      &out->obsolete_segment_marked_at, &out->obsolete_segment_count,
      &out->obsolete_snapshots, &out->obsolete_snapshot_marked_at,
      &out->obsolete_snapshot_count);
  lc_free_with_allocator(allocator, manifest_path);
  if (!manifest_valid && max_snapshot_id > manifest_snapshot_id &&
      !lc_pouch_namespace_manifest_list_contains(out->obsolete_snapshots,
                                                 out->obsolete_snapshot_count,
                                                 scanned_snapshot)) {
    lc_free_with_allocator(allocator, manifest_snapshot);
    manifest_snapshot = scanned_snapshot;
    scanned_snapshot = NULL;
    manifest_snapshot_id = max_snapshot_id;
    out->repaired = 1;
  }
  if (!manifest_valid || active_segment_id < max_segment_id) {
    active_segment_id = max_segment_id != 0UL ? max_segment_id : 1UL;
    if (!manifest_valid && active_segment_id <= manifest_snapshot_id) {
      active_segment_id = manifest_snapshot_id + 1UL;
    }
    out->repaired = 1;
  }
  if (active_segment_id == 0UL) {
    active_segment_id = 1UL;
    out->repaired = 1;
  }
  out->active_segment_id = active_segment_id;
  out->latest_snapshot = manifest_snapshot;
  manifest_snapshot = NULL;
  out->latest_snapshot_segment_id = manifest_snapshot_id;
  out->state_max_version = state_max_version;
  out->max_segment_id =
      max_segment_id > active_segment_id ? max_segment_id : active_segment_id;
  out->active_segment =
      lc_pouch_namespace_segment_leaf(allocator, out->active_segment_id);
  if (out->active_segment == NULL) {
    lc_pouch_namespace_manifest_cleanup(allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch active segment name", NULL,
                        NULL, NULL);
  }
  if (out->repaired) {
    rc = lc_pouch_namespace_manifest_write(allocator, ns, out, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(allocator, scanned_snapshot);
      lc_pouch_namespace_manifest_cleanup(allocator, out);
      return rc;
    }
  }
  lc_free_with_allocator(allocator, manifest_snapshot);
  lc_free_with_allocator(allocator, scanned_snapshot);
  return LC_OK;
}

int lc_pouch_namespace_manifest_rotate(const lc_allocator *allocator,
                                       const char *ns,
                                       lc_pouch_namespace_manifest *manifest,
                                       uint64_t segment_id, lc_error *error) {
  char *active_segment;
  int rc;

  if (manifest == NULL || segment_id == 0UL || manifest->borrowed) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace manifest rotate requires manifest "
                        "ownership and segment id",
                        NULL, NULL, NULL);
  }
  active_segment = lc_pouch_namespace_segment_leaf(allocator, segment_id);
  if (active_segment == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch active segment name", NULL,
                        NULL, NULL);
  }
  lc_free_with_allocator(allocator, manifest->active_segment);
  manifest->active_segment = active_segment;
  manifest->active_segment_id = segment_id;
  if (segment_id > manifest->max_segment_id) {
    manifest->max_segment_id = segment_id;
  }
  rc = lc_pouch_namespace_manifest_write(allocator, ns, manifest, error);
  if (rc == LC_OK) {
    manifest->repaired = 0;
  }
  return rc;
}

int lc_pouch_namespace_manifest_install_snapshot(
    const lc_allocator *allocator, const char *ns,
    lc_pouch_namespace_manifest *manifest, const char *snapshot_leaf,
    uint64_t snapshot_segment_id, lc_error *error) {
  char *snapshot_copy;
  int rc;

  if (manifest == NULL || manifest->borrowed || snapshot_leaf == NULL ||
      snapshot_leaf[0] == '\0' || snapshot_segment_id == 0UL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch snapshot install requires owned manifest and "
                        "snapshot",
                        NULL, NULL, NULL);
  }
  snapshot_copy = lc_strdup_with_allocator(allocator, snapshot_leaf);
  if (snapshot_copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch snapshot name", NULL, NULL,
                        NULL);
  }
  lc_free_with_allocator(allocator, manifest->latest_snapshot);
  manifest->latest_snapshot = snapshot_copy;
  manifest->latest_snapshot_segment_id = snapshot_segment_id;
  rc = lc_pouch_namespace_manifest_write(allocator, ns, manifest, error);
  if (rc == LC_OK) {
    manifest->repaired = 0;
  }
  return rc;
}

int lc_pouch_namespace_manifest_mark_obsolete_segment(
    const lc_allocator *allocator, lc_pouch_namespace_manifest *manifest,
    const char *segment_leaf, uint64_t marked_at_unix, lc_error *error) {
  uint64_t parsed;

  if (manifest == NULL || manifest->borrowed || marked_at_unix == 0U ||
      !lc_pouch_namespace_parse_segment_id(segment_leaf, &parsed)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch obsolete segment requires owned manifest and "
                        "segment leaf",
                        NULL, NULL, NULL);
  }
  if (manifest->active_segment != NULL &&
      strcmp(manifest->active_segment, segment_leaf) == 0) {
    return LC_OK;
  }
  return lc_pouch_namespace_manifest_list_append(
      allocator, &manifest->obsolete_segments,
      &manifest->obsolete_segment_marked_at, &manifest->obsolete_segment_count,
      segment_leaf, marked_at_unix, error);
}

int lc_pouch_namespace_manifest_mark_obsolete_snapshot(
    const lc_allocator *allocator, lc_pouch_namespace_manifest *manifest,
    const char *snapshot_leaf, uint64_t marked_at_unix, lc_error *error) {
  uint64_t parsed;

  if (manifest == NULL || manifest->borrowed || marked_at_unix == 0U ||
      !lc_pouch_namespace_parse_snapshot_id(snapshot_leaf, &parsed)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch obsolete snapshot requires owned manifest and "
                        "snapshot leaf",
                        NULL, NULL, NULL);
  }
  if (manifest->latest_snapshot != NULL &&
      strcmp(manifest->latest_snapshot, snapshot_leaf) == 0) {
    return LC_OK;
  }
  return lc_pouch_namespace_manifest_list_append(
      allocator, &manifest->obsolete_snapshots,
      &manifest->obsolete_snapshot_marked_at,
      &manifest->obsolete_snapshot_count, snapshot_leaf, marked_at_unix, error);
}

int lc_pouch_namespace_manifest_save(const lc_allocator *allocator,
                                     const char *ns,
                                     lc_pouch_namespace_manifest *manifest,
                                     lc_error *error) {
  if (manifest == NULL || manifest->borrowed || ns == NULL || ns[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace manifest save requires owned manifest "
                        "and namespace",
                        NULL, NULL, NULL);
  }
  return lc_pouch_namespace_manifest_write(allocator, ns, manifest, error);
}

static int lc_pouch_namespace_obsolete_due(const char *leaf, uint64_t marked_at,
                                           const char *current_leaf,
                                           uint64_t now_unix,
                                           uint64_t grace_seconds) {
  return leaf != NULL && marked_at != 0U &&
         (current_leaf == NULL || strcmp(leaf, current_leaf) != 0) &&
         now_unix >= marked_at && now_unix - marked_at >= grace_seconds;
}

static int lc_pouch_namespace_manifest_prune_obsolete_list(
    const lc_allocator *allocator, char ***items, uint64_t **marked_at,
    unsigned long *count, const char *namespace_path, const char *directory,
    const char *current_leaf, lc_pouch_namespace_manifest *segments,
    uint64_t now_unix, uint64_t delete_grace_seconds, int *changed,
    unsigned long *deleted_count, unsigned long *pending_count,
    lc_error *error) {
  unsigned long index;
  char *directory_path = NULL;
  int rc = LC_OK;

  if (items == NULL || marked_at == NULL || count == NULL ||
      namespace_path == NULL || directory == NULL || changed == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch obsolete cleanup requires list context", NULL,
                        NULL, "pouch");
  }
  /* Keep retirement markers until all unlinks in this directory are durable.
   * On partial failure a retry may see ENOENT, but must never resurrect an
   * already deleted leaf as a candidate in this same manifest view. */
  for (index = 0; index < *count; ++index) {
    char *path;
    if (!lc_pouch_namespace_obsolete_due((*items)[index], (*marked_at)[index],
                                         current_leaf, now_unix,
                                         delete_grace_seconds)) {
      if (pending_count != NULL)
        ++*pending_count;
      continue;
    }
    if (directory_path == NULL)
      directory_path = lc_pouch_path_join(allocator, namespace_path, directory);
    path = directory_path != NULL
               ? lc_pouch_path_join(allocator, directory_path, (*items)[index])
               : NULL;
    if (path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch obsolete path", NULL, NULL,
                        NULL);
      goto done;
    }
    if (unlink(path) != 0 && errno != ENOENT) {
      int saved_errno = errno;
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to delete obsolete pouch log file",
                        strerror(saved_errno), NULL, "pouch");
      lc_free_with_allocator(allocator, path);
      goto done;
    }
    lc_free_with_allocator(allocator, path);
    if (segments != NULL && segments->segment_count != 0UL) {
      char **found = bsearch(&(*items)[index], segments->segment_leaves,
                             segments->segment_count, sizeof(char *),
                             lc_pouch_namespace_segment_leaf_compare);
      if (found != NULL) {
        size_t position = (size_t)(found - segments->segment_leaves);
        lc_free_with_allocator(allocator, *found);
        memmove(found, found + 1,
                (segments->segment_count - position - 1) * sizeof(*found));
        --segments->segment_count;
      }
    }
  }
  if (directory_path == NULL)
    goto done;
  rc = lc_pouch_path_fsync_directory(
      directory_path, "failed to sync obsolete pouch log deletion", error);
  if (rc != LC_OK)
    goto done;
  index = 0;
  while (index < *count) {
    if (!lc_pouch_namespace_obsolete_due((*items)[index], (*marked_at)[index],
                                         current_leaf, now_unix,
                                         delete_grace_seconds)) {
      ++index;
      continue;
    }
    lc_free_with_allocator(allocator, (*items)[index]);
    if (index + 1UL < *count) {
      memmove(*items + index, *items + index + 1UL,
              ((size_t)(*count - index - 1UL)) * sizeof(**items));
      memmove(*marked_at + index, *marked_at + index + 1UL,
              ((size_t)(*count - index - 1UL)) * sizeof(**marked_at));
    }
    --*count;
    *changed = 1;
    if (deleted_count != NULL)
      ++*deleted_count;
  }
done:
  lc_free_with_allocator(allocator, directory_path);
  return rc;
}

int lc_pouch_namespace_manifest_cleanup_obsolete(
    const lc_allocator *allocator, const char *ns,
    lc_pouch_namespace_manifest *manifest, uint64_t now_unix,
    uint64_t delete_grace_seconds, unsigned long *deleted_count,
    unsigned long *pending_count, lc_error *error) {
  int changed;
  int rc;

  if (manifest == NULL || manifest->borrowed ||
      manifest->namespace_path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch obsolete cleanup requires owned manifest", NULL,
                        NULL, NULL);
  }
  if (deleted_count != NULL) {
    *deleted_count = 0UL;
  }
  if (pending_count != NULL) {
    *pending_count = 0UL;
  }
  changed = 0;
  rc = lc_pouch_namespace_manifest_prune_obsolete_list(
      allocator, &manifest->obsolete_segments,
      &manifest->obsolete_segment_marked_at, &manifest->obsolete_segment_count,
      manifest->namespace_path, "segments", manifest->active_segment, manifest,
      now_unix, delete_grace_seconds, &changed, deleted_count, pending_count,
      error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_namespace_manifest_prune_obsolete_list(
      allocator, &manifest->obsolete_snapshots,
      &manifest->obsolete_snapshot_marked_at,
      &manifest->obsolete_snapshot_count, manifest->namespace_path, "snapshots",
      manifest->latest_snapshot, NULL, now_unix, delete_grace_seconds, &changed,
      deleted_count, pending_count, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (changed) {
    return lc_pouch_namespace_manifest_write(allocator, ns, manifest, error);
  }
  return LC_OK;
}

int lc_pouch_namespace_touch_marker(const lc_allocator *allocator,
                                    const char *namespace_path,
                                    const char *writer_marker_leaf,
                                    uint64_t sequence, lc_error *error) {
  char text[256];
  char sequence_text[32];
  char *markers_path;
  char *marker_path;
  FILE *fp;

  if (namespace_path == NULL || writer_marker_leaf == NULL ||
      writer_marker_leaf[0] == '\0' || sequence == (uint64_t)0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch marker touch requires namespace path and "
                        "writer marker leaf and non-zero sequence",
                        NULL, NULL, NULL);
  }
  if (lc_u64_format_base10_padded((lc_u64)sequence, 20U, sequence_text,
                                  sizeof(sequence_text)) < 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "failed to format pouch marker sequence", NULL, NULL,
                        NULL);
  }
  snprintf(text, sizeof(text),
           "writer_pid=%ld\nwriter_marker=%s\nsequence=%s\n%s", (long)getpid(),
           writer_marker_leaf, sequence_text,
           (sequence % (uint64_t)2U) == (uint64_t)0U ? "pad=x\n" : "");
  markers_path = lc_pouch_path_join(allocator, namespace_path, "markers");
  marker_path =
      markers_path != NULL
          ? lc_pouch_path_join(allocator, markers_path, writer_marker_leaf)
          : NULL;
  lc_free_with_allocator(allocator, markers_path);
  if (marker_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch marker path", NULL, NULL,
                        NULL);
  }
  /* Markers are advisory peer-refresh hints; the append log carries durability.
   */
  fp = fopen(marker_path, "wb");
  if (fp == NULL) {
    int saved_errno = errno;

    lc_free_with_allocator(allocator, marker_path);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to update pouch marker", strerror(saved_errno),
                        NULL, "pouch");
  }
  if (fputs(text, fp) == EOF) {
    int saved_errno = errno;

    (void)fclose(fp);
    lc_free_with_allocator(allocator, marker_path);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to update pouch marker", strerror(saved_errno),
                        NULL, "pouch");
  }
  if (fclose(fp) != 0) {
    int saved_errno = errno;

    lc_free_with_allocator(allocator, marker_path);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to update pouch marker", strerror(saved_errno),
                        NULL, "pouch");
  }
  lc_free_with_allocator(allocator, marker_path);
  return LC_OK;
}

int lc_pouch_namespace_marker_snapshot_read(
    const lc_allocator *allocator, const char *namespace_path,
    const char *self_marker_leaf, lc_pouch_namespace_marker_snapshot *out,
    lc_error *error) {
  lc_pouch_marker_entry *entries;
  size_t count;
  int rc;

  if (namespace_path == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch marker snapshot requires namespace path and "
                        "out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  entries = NULL;
  rc = lc_pouch_marker_entries_read(allocator, namespace_path, self_marker_leaf,
                                    &entries, &count, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_marker_snapshot_build_fingerprint(allocator, entries, count,
                                                  &out->fingerprint, error);
  if (rc != LC_OK) {
    lc_pouch_marker_entries_cleanup(allocator, entries, count);
    return rc;
  }
  out->marker_count = (unsigned long)count;
  lc_pouch_marker_entries_cleanup(allocator, entries, count);
  return LC_OK;
}

int lc_pouch_namespace_marker_snapshot_changed(
    const lc_pouch_namespace_marker_snapshot *before,
    const lc_pouch_namespace_marker_snapshot *after) {
  const char *before_fingerprint;
  const char *after_fingerprint;

  if (before == NULL || after == NULL) {
    return 1;
  }
  if (before->marker_count != after->marker_count) {
    return 1;
  }
  before_fingerprint = before->fingerprint != NULL ? before->fingerprint : "";
  after_fingerprint = after->fingerprint != NULL ? after->fingerprint : "";
  return strcmp(before_fingerprint, after_fingerprint) != 0;
}

int lc_pouch_namespace_marker_directory_snapshot_read(
    const lc_allocator *allocator, const char *namespace_path,
    lc_pouch_namespace_marker_directory_snapshot *out, lc_error *error) {
  char *markers_path;
  struct stat st;

  if (namespace_path == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch marker directory snapshot requires namespace "
                        "path and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  markers_path = lc_pouch_path_join(allocator, namespace_path, "markers");
  if (markers_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch markers path", NULL, NULL,
                        NULL);
  }
  if (stat(markers_path, &st) != 0) {
    lc_free_with_allocator(allocator, markers_path);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to stat pouch marker directory",
                        strerror(errno), NULL, NULL);
  }
  lc_free_with_allocator(allocator, markers_path);
  if (!S_ISDIR(st.st_mode)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch marker path is not a directory", NULL, NULL,
                        NULL);
  }
  if (st.st_size < 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch marker directory has invalid size", NULL, NULL,
                        "pouch");
  }
  out->size = (uint64_t)st.st_size;
  out->mtime = (long)st.st_mtime;
  return LC_OK;
}

int lc_pouch_namespace_marker_directory_snapshot_changed(
    const lc_pouch_namespace_marker_directory_snapshot *before,
    const lc_pouch_namespace_marker_directory_snapshot *after) {
  if (before == NULL || after == NULL) {
    return 1;
  }
  return before->size != after->size || before->mtime != after->mtime;
}

static int lc_pouch_marker_refresh_cached_peer_stats_changed(
    const lc_allocator *allocator, const char *namespace_path,
    const lc_pouch_namespace_marker_refresh_state *state, int *changed,
    lc_error *error) {
  char *markers_path;
  unsigned long i;
  int rc;

  *changed = 0;
  markers_path = lc_pouch_path_join(allocator, namespace_path, "markers");
  if (markers_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch markers path", NULL, NULL,
                        NULL);
  }
  rc = LC_OK;
  for (i = 0UL; i < state->peer_stat_count; ++i) {
    char *marker_path;
    struct stat st;

    marker_path =
        lc_pouch_path_join(allocator, markers_path, state->peer_stats[i].name);
    if (marker_path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch marker path", NULL, NULL,
                        NULL);
      break;
    }
    if (stat(marker_path, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 0 ||
        state->peer_stats[i].size != (uint64_t)st.st_size ||
        state->peer_stats[i].mtime != (long)st.st_mtime) {
      *changed = 1;
      lc_free_with_allocator(allocator, marker_path);
      break;
    }
    lc_free_with_allocator(allocator, marker_path);
  }
  lc_free_with_allocator(allocator, markers_path);
  return rc;
}

static int lc_pouch_marker_refresh_full(
    const lc_allocator *allocator, const char *namespace_path,
    const char *self_marker_leaf,
    lc_pouch_namespace_marker_refresh_state *state,
    const lc_pouch_namespace_marker_directory_snapshot *directory, int forced,
    int *should_scan, lc_error *error) {
  lc_pouch_marker_entry *entries;
  lc_pouch_namespace_marker_snapshot peers;
  size_t count;
  int peers_changed;
  int rc;

  memset(&peers, 0, sizeof(peers));
  entries = NULL;
  rc = lc_pouch_marker_entries_read(allocator, namespace_path, self_marker_leaf,
                                    &entries, &count, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_marker_snapshot_build_fingerprint(allocator, entries, count,
                                                  &peers.fingerprint, error);
  if (rc != LC_OK) {
    lc_pouch_marker_entries_cleanup(allocator, entries, count);
    return rc;
  }
  peers.marker_count = (unsigned long)count;
  peers_changed =
      !state->initialized ||
      lc_pouch_namespace_marker_snapshot_changed(&state->peers, &peers);
  lc_pouch_namespace_marker_snapshot_cleanup(allocator, &state->peers);
  lc_pouch_marker_refresh_peer_stats_cleanup(allocator, state);
  state->peers = peers;
  state->peer_stats = entries;
  state->peer_stat_count = (unsigned long)count;
  memset(&peers, 0, sizeof(peers));
  entries = NULL;
  state->directory = *directory;
  state->initialized = 1;
  state->skipped_refreshes = 0UL;
  *should_scan = forced || peers_changed;
  return LC_OK;
}

int lc_pouch_namespace_marker_refresh_should_scan(
    const lc_allocator *allocator, const char *namespace_path,
    const char *self_marker_leaf,
    lc_pouch_namespace_marker_refresh_state *state,
    unsigned long force_after_skips, int *should_scan, lc_error *error) {
  lc_pouch_namespace_marker_directory_snapshot directory;
  int cached_peers_changed;
  int directory_changed;
  int forced;
  int rc;

  if (state == NULL || should_scan == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch marker refresh requires state and scan output",
                        NULL, NULL, NULL);
  }
  memset(&directory, 0, sizeof(directory));
  rc = lc_pouch_namespace_marker_directory_snapshot_read(
      allocator, namespace_path, &directory, error);
  if (rc != LC_OK) {
    return rc;
  }
  directory_changed = !state->initialized ||
                      lc_pouch_namespace_marker_directory_snapshot_changed(
                          &state->directory, &directory);
  forced = state->initialized && force_after_skips > 0UL &&
           state->skipped_refreshes >= force_after_skips;
  if (!directory_changed && !forced) {
    rc = lc_pouch_marker_refresh_cached_peer_stats_changed(
        allocator, namespace_path, state, &cached_peers_changed, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (cached_peers_changed) {
      return lc_pouch_marker_refresh_full(allocator, namespace_path,
                                          self_marker_leaf, state, &directory,
                                          0, should_scan, error);
    }
    ++state->skipped_refreshes;
    *should_scan = 0;
    return LC_OK;
  }
  return lc_pouch_marker_refresh_full(allocator, namespace_path,
                                      self_marker_leaf, state, &directory,
                                      forced, should_scan, error);
}

void lc_pouch_namespace_marker_snapshot_cleanup(
    const lc_allocator *allocator,
    lc_pouch_namespace_marker_snapshot *snapshot) {
  if (snapshot == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, snapshot->fingerprint);
  memset(snapshot, 0, sizeof(*snapshot));
}

void lc_pouch_namespace_marker_refresh_state_cleanup(
    const lc_allocator *allocator,
    lc_pouch_namespace_marker_refresh_state *state) {
  if (state == NULL) {
    return;
  }
  lc_pouch_namespace_marker_snapshot_cleanup(allocator, &state->peers);
  lc_pouch_marker_refresh_peer_stats_cleanup(allocator, state);
  memset(state, 0, sizeof(*state));
}

void lc_pouch_namespace_manifest_cleanup(
    const lc_allocator *allocator, lc_pouch_namespace_manifest *manifest) {
  if (manifest == NULL) {
    return;
  }
  if (manifest->borrowed) {
    memset(manifest, 0, sizeof(*manifest));
    return;
  }
  lc_free_with_allocator(allocator, manifest->namespace_path);
  lc_free_with_allocator(allocator, manifest->active_segment);
  lc_free_with_allocator(allocator, manifest->latest_snapshot);
  lc_pouch_namespace_manifest_list_cleanup(allocator, manifest->segment_leaves,
                                           NULL, manifest->segment_count);
  lc_pouch_namespace_manifest_list_cleanup(
      allocator, manifest->obsolete_segments,
      manifest->obsolete_segment_marked_at, manifest->obsolete_segment_count);
  lc_pouch_namespace_manifest_list_cleanup(
      allocator, manifest->obsolete_snapshots,
      manifest->obsolete_snapshot_marked_at, manifest->obsolete_snapshot_count);
  memset(manifest, 0, sizeof(*manifest));
}

int lc_pouch_namespace_ensure_layout(const lc_allocator *allocator,
                                     const char *root_path, const char *ns,
                                     lc_error *error) {
  char *namespace_path;
  int rc;

  if (ns == NULL || ns[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace name is required", NULL, NULL, NULL);
  }
  namespace_path = lc_pouch_namespace_path(allocator, root_path, ns);
  if (namespace_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace path", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_path_ensure_directory(
      namespace_path, "failed to create pouch namespace directory", error);
  if (rc == LC_OK) {
    rc = lc_pouch_namespace_ensure_child(
        allocator, namespace_path, "segments",
        "failed to create pouch namespace segments directory", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_namespace_ensure_child(
        allocator, namespace_path, "snapshots",
        "failed to create pouch namespace snapshots directory", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_namespace_ensure_child(
        allocator, namespace_path, "markers",
        "failed to create pouch namespace markers directory", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_namespace_ensure_child(
        allocator, namespace_path, "index",
        "failed to create pouch namespace index directory", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_namespace_ensure_child(
        allocator, namespace_path, "queue-notify",
        "failed to create pouch namespace queue notification directory", error);
  }
  lc_free_with_allocator(allocator, namespace_path);
  return rc;
}

int lc_pouch_namespace_ensure(const lc_allocator *allocator,
                              const char *root_path, const char *ns,
                              lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  int rc;

  rc = lc_pouch_namespace_ensure_layout(allocator, root_path, ns, error);
  if (rc == LC_OK) {
    memset(&manifest, 0, sizeof(manifest));
    rc = lc_pouch_namespace_manifest_open(allocator, root_path, ns, &manifest,
                                          NULL, NULL, error);
    lc_pouch_namespace_manifest_cleanup(allocator, &manifest);
  }
  return rc;
}
