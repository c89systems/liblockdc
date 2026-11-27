#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_outbox_store.h"
#include "lc_pouch_path.h"

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define LC_POUCH_HISTORY_MAGIC "LCH1"
#define LC_POUCH_HISTORY_BYTES 64U
#define LC_POUCH_HISTORY_CONSUMER_PREFIX "consumer:"

struct lc_history_consumer_handle {
  lc_history_consumer pub;
  lc_client_handle *client;
  char *ns;
  char *consumer_id;
  int registered;
};

typedef enum lc_pouch_history_operation {
  LC_POUCH_HISTORY_REGISTER = 1,
  LC_POUCH_HISTORY_POSITION = 2,
  LC_POUCH_HISTORY_ADVANCE = 3,
  LC_POUCH_HISTORY_UNREGISTER = 4
} lc_pouch_history_operation;

typedef struct lc_pouch_history_operation_context {
  lc_history_consumer_handle *consumer;
  lc_pouch_history_operation operation;
  lc_index_seq requested_acknowledged_index_seq;
  lc_history_consumer_position *out;
} lc_pouch_history_operation_context;

static int lc_pouch_history_directory(lc_pouch *pouch, const char *ns,
                                      int create, char **out, lc_error *error) {
  char *control_directory;
  char *history_directory;
  char *namespace_leaf;
  char *namespace_directory;
  int rc;

  *out = NULL;
  control_directory =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, ".lockd");
  history_directory =
      control_directory != NULL
          ? lc_pouch_path_join(&pouch->allocator, control_directory,
                               "history-consumers")
          : NULL;
  namespace_leaf = lc_pouch_path_escape_name(&pouch->allocator, ns);
  namespace_directory =
      history_directory != NULL && namespace_leaf != NULL
          ? lc_pouch_path_join(&pouch->allocator, history_directory,
                               namespace_leaf)
          : NULL;
  lc_free_with_allocator(&pouch->allocator, namespace_leaf);
  if (control_directory == NULL || history_directory == NULL ||
      namespace_directory == NULL) {
    lc_free_with_allocator(&pouch->allocator, control_directory);
    lc_free_with_allocator(&pouch->allocator, history_directory);
    lc_free_with_allocator(&pouch->allocator, namespace_directory);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch history consumer path", NULL,
                        NULL, "pouch");
  }
  rc = LC_OK;
  if (create) {
    rc = lc_pouch_path_ensure_directory(
        control_directory, "failed to create pouch control directory", error);
    if (rc == LC_OK) {
      rc = lc_pouch_path_ensure_directory(
          history_directory, "failed to create pouch history directory", error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_path_ensure_directory(
          namespace_directory,
          "failed to create pouch namespace history directory", error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_path_fsync_directory(
          pouch->root_path, "failed to sync pouch root history directory",
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_path_fsync_directory(
          control_directory, "failed to sync pouch control history directory",
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_path_fsync_directory(
          history_directory, "failed to sync pouch history directory", error);
    }
  }
  lc_free_with_allocator(&pouch->allocator, control_directory);
  lc_free_with_allocator(&pouch->allocator, history_directory);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, namespace_directory);
    return rc;
  }
  *out = namespace_directory;
  return LC_OK;
}

static int lc_pouch_history_staging_directory(lc_pouch *pouch,
                                              const char *directory, int create,
                                              char **out, lc_error *error) {
  char *staging_directory;
  int rc;

  *out = NULL;
  staging_directory =
      lc_pouch_path_join(&pouch->allocator, directory, ".staging");
  if (staging_directory == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch history staging directory",
                        NULL, directory, "pouch");
  }
  if (!create) {
    *out = staging_directory;
    return LC_OK;
  }
  rc = lc_pouch_path_ensure_directory(
      staging_directory, "failed to create pouch history staging directory",
      error);
  if (rc == LC_OK) {
    rc = lc_pouch_path_fsync_directory(
        directory, "failed to sync pouch history consumer directory", error);
  }
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, staging_directory);
    return rc;
  }
  *out = staging_directory;
  return LC_OK;
}

static int lc_pouch_history_staging_cleanup(lc_pouch *pouch,
                                            const char *directory,
                                            lc_error *error) {
  char *staging_directory;
  DIR *dir;
  struct dirent *entry;
  int removed;
  int rc;

  staging_directory = NULL;
  rc = lc_pouch_history_staging_directory(pouch, directory, 0,
                                          &staging_directory, error);
  if (rc != LC_OK) {
    return rc;
  }
  dir = opendir(staging_directory);
  if (dir == NULL) {
    if (errno == ENOENT) {
      lc_free_with_allocator(&pouch->allocator, staging_directory);
      return LC_OK;
    }
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to open pouch history staging directory",
                      strerror(errno), staging_directory, "pouch");
    lc_free_with_allocator(&pouch->allocator, staging_directory);
    return rc;
  }
  removed = 0;
  rc = LC_OK;
  while (rc == LC_OK && (entry = readdir(dir)) != NULL) {
    char *path;
    struct stat st;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    path =
        lc_pouch_path_join(&pouch->allocator, staging_directory, entry->d_name);
    if (path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch history staging record", NULL,
                        staging_directory, "pouch");
    } else if (lstat(path, &st) != 0 || !S_ISREG(st.st_mode)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch history staging entry is not a regular file",
                        NULL, path, "pouch");
    } else if (unlink(path) != 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to remove abandoned pouch history staging "
                        "record",
                        strerror(errno), path, "pouch");
    } else {
      removed = 1;
    }
    lc_free_with_allocator(&pouch->allocator, path);
  }
  if (closedir(dir) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch history staging directory",
                      strerror(errno), staging_directory, "pouch");
  }
  if (rc == LC_OK && removed) {
    rc = lc_pouch_path_fsync_directory(
        staging_directory, "failed to sync pouch history staging directory",
        error);
  }
  lc_free_with_allocator(&pouch->allocator, staging_directory);
  return rc;
}

static int lc_pouch_history_parse_ack(char *line, lc_index_seq *out,
                                      lc_error *error) {
  lc_u64 value;
  size_t length;

  if (line == NULL || out == NULL || strncmp(line, "ack=", 4U) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch history consumer acknowledgement is invalid",
                        NULL, NULL, "pouch");
  }
  length = strlen(line);
  if (length < 6U || line[length - 1U] != '\n') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch history consumer acknowledgement is truncated",
                        NULL, NULL, "pouch");
  }
  line[length - 1U] = '\0';
  if (!lc_u64_parse_base10(line + 4U, &value)) {
    line[length - 1U] = '\n';
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch history consumer acknowledgement is invalid",
                        NULL, NULL, "pouch");
  }
  line[length - 1U] = '\n';
  *out = (lc_index_seq)value;
  return LC_OK;
}

/* History controls are durable Pouch metadata, not caller-owned inputs.  Read
 * only regular files so a damaged control directory cannot redirect a cursor
 * operation through a symlink or special file. */
static int lc_pouch_history_validate_file(const char *path, int *exists,
                                          lc_error *error) {
  struct stat st;

  *exists = 0;
  if (lstat(path, &st) != 0) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to inspect pouch history consumer record",
                        strerror(errno), path, "pouch");
  }
  if (!S_ISREG(st.st_mode)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch history consumer entry is not a regular file",
                        NULL, path, "pouch");
  }
  *exists = 1;
  return LC_OK;
}

static int lc_pouch_history_read(const char *path, int *found,
                                 lc_index_seq *acknowledged_index_seq,
                                 lc_error *error) {
  char magic[16];
  char ack[64];
  char extra[2];
  FILE *fp;
  int exists;
  int rc;

  *found = 0;
  *acknowledged_index_seq = 0U;
  exists = 0;
  rc = lc_pouch_history_validate_file(path, &exists, error);
  if (rc != LC_OK || !exists) {
    return rc;
  }
  fp = fopen(path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch history consumer record",
                        strerror(errno), path, "pouch");
  }
  rc = LC_OK;
  if (fgets(magic, sizeof(magic), fp) == NULL ||
      strcmp(magic, LC_POUCH_HISTORY_MAGIC "\n") != 0 ||
      fgets(ack, sizeof(ack), fp) == NULL) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch history consumer record is malformed", NULL, path,
                      "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_history_parse_ack(ack, acknowledged_index_seq, error);
  }
  if (rc == LC_OK && fgets(extra, sizeof(extra), fp) != NULL) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch history consumer record has trailing bytes", NULL,
                      path, "pouch");
  }
  if (ferror(fp) && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to read pouch history consumer record",
                      strerror(errno), path, "pouch");
  }
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch history consumer record",
                      strerror(errno), path, "pouch");
  }
  if (rc == LC_OK) {
    *found = 1;
  }
  return rc;
}

static int lc_pouch_history_encode(lc_index_seq seq,
                                   char text[LC_POUCH_HISTORY_BYTES],
                                   lc_error *error) {
  char acknowledged[32];
  int length;

  if (lc_u64_format_base10((lc_u64)seq, acknowledged, sizeof(acknowledged)) < 0)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "failed to format pouch history consumer record", NULL,
                        NULL, "pouch");
  length = snprintf(text, LC_POUCH_HISTORY_BYTES,
                    LC_POUCH_HISTORY_MAGIC "\nack=%s\n", acknowledged);
  if (length < 0 || (size_t)length >= LC_POUCH_HISTORY_BYTES)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch history consumer record exceeds limit", NULL,
                        NULL, "pouch");
  return LC_OK;
}

#ifdef LC_POUCH_MULTIFILE_RUNTIME
#include "lc_pouch_multifile_history.h"
#endif

static int lc_pouch_history_current_locked(lc_pouch *pouch, const char *ns,
                                           lc_index_seq *out, lc_error *error) {
  lc_pouch_generation current_index_seq;
  int rc;

  *out = 0U;
  current_index_seq = 0U;
  rc = lc_pouch_state_index_seq(pouch, ns, &current_index_seq, error);
  if (rc == LC_OK) {
    *out = (lc_index_seq)current_index_seq;
  }
  return rc;
}

typedef int (*lc_pouch_history_visit_fn)(void *, const char *, lc_index_seq,
                                         lc_error *);

static int history_store_open(lc_pouch *pouch, const char *ns, int create,
                              lc_pouch_outbox_store *store, lc_error *error) {
  if (!pouch->storage.ops->history_markers_open)
    return lc_error_set(error, LC_ERR_UNSUPPORTED, 0L,
                        "pouch storage owner has no history consumer store",
                        NULL, NULL, "pouch");
  return pouch->storage.ops->history_markers_open(pouch->storage.context, ns,
                                                  create, store, error);
}

static int history_store_read(lc_pouch *pouch, lc_pouch_outbox_store *store,
                              const char *id, int *found, lc_index_seq *seq,
                              lc_error *error) {
  static const char magic[] = LC_POUCH_HISTORY_MAGIC "\n";
  unsigned char *bytes = NULL;
  char line[LC_POUCH_HISTORY_BYTES];
  size_t length = 0, prefix = sizeof(magic) - 1U;
  int rc;

  *found = 0;
  *seq = 0;
  rc = store->ops->read(store->context, id, &pouch->allocator, prefix + 6U,
                        sizeof(line) - 1U, &bytes, &length, error);
  if (rc == LC_POUCH_OUTBOX_MARKER_MISSING)
    return LC_OK;
  if (rc == LC_OK && (memcmp(bytes, magic, prefix) || memchr(bytes, 0, length)))
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "pouch history consumer record is malformed", NULL, id,
                      "pouch");
  if (rc == LC_OK) {
    memcpy(line, bytes + prefix, length - prefix);
    line[length - prefix] = '\0';
    rc = lc_pouch_history_parse_ack(line, seq, error);
  }
  lc_free_with_allocator(&pouch->allocator, bytes);
  if (rc == LC_OK)
    *found = 1;
  return rc;
}

static int history_store_write(lc_pouch_outbox_store *store, const char *id,
                               lc_index_seq seq, lc_error *error) {
  char text[LC_POUCH_HISTORY_BYTES];
  int rc = lc_pouch_history_encode(seq, text, error);
  if (rc == LC_OK)
    rc = store->ops->write(store->context, id, (const unsigned char *)text,
                           strlen(text), error);
  return rc;
}

static int history_visit_legacy(lc_pouch *pouch, const char *ns,
                                lc_pouch_history_visit_fn visit, void *context,
                                lc_error *error) {
  char *directory;
  DIR *dir;
  struct dirent *entry;
  int rc;

  directory = NULL;
  rc = lc_pouch_history_directory(pouch, ns, 0, &directory, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_history_staging_cleanup(pouch, directory, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, directory);
    return rc;
  }
  dir = opendir(directory);
  if (dir == NULL) {
    if (errno == ENOENT) {
      lc_free_with_allocator(&pouch->allocator, directory);
      return LC_OK;
    }
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to open pouch history consumer directory",
                      strerror(errno), directory, "pouch");
    lc_free_with_allocator(&pouch->allocator, directory);
    return rc;
  }
  rc = LC_OK;
  while (rc == LC_OK && (entry = readdir(dir)) != NULL) {
    char *decoded;
    char *path;
    lc_index_seq acknowledged_index_seq;
    int found;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    if (strcmp(entry->d_name, ".staging") == 0) {
      continue;
    }
    decoded = lc_pouch_path_unescape_name(&pouch->allocator, entry->d_name);
    path = lc_pouch_path_join(&pouch->allocator, directory, entry->d_name);
    if (decoded == NULL || path == NULL) {
      lc_free_with_allocator(&pouch->allocator, decoded);
      lc_free_with_allocator(&pouch->allocator, path);
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch history consumer directory contains an invalid "
                        "entry",
                        NULL, directory, "pouch");
      break;
    }
    found = 0;
    acknowledged_index_seq = 0U;
    rc = lc_pouch_history_read(path, &found, &acknowledged_index_seq, error);
    if (rc == LC_OK && found)
      rc = visit(context, decoded, acknowledged_index_seq, error);
    lc_free_with_allocator(&pouch->allocator, decoded);
    lc_free_with_allocator(&pouch->allocator, path);
  }
  if (closedir(dir) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch history consumer directory",
                      strerror(errno), directory, "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, directory);
  return rc;
}

static int history_visit_store(lc_pouch *pouch, const char *ns,
                               lc_pouch_history_visit_fn visit, void *context,
                               lc_error *error) {
  lc_pouch_outbox_store store = {0};
  const char *name = NULL;
  int rc = history_store_open(pouch, ns, 0, &store, error);
  while (rc == LC_OK &&
         (rc = store.ops->next(store.context, &name, error)) == LC_OK && name) {
    lc_index_seq seq;
    int found;
    rc = history_store_read(pouch, &store, name, &found, &seq, error);
    if (rc == LC_OK && found)
      rc = visit(context, name, seq, error);
  }
  return lc_pouch_outbox_store_close(&store, rc, error);
}

typedef struct history_oldest {
  lc_index_seq seq;
  int found;
} history_oldest;

static int history_minimum(void *context, const char *id, lc_index_seq seq,
                           lc_error *error) {
  history_oldest *oldest = context;
  (void)id;
  (void)error;
  if (!oldest->found || seq < oldest->seq)
    oldest->seq = seq;
  oldest->found = 1;
  return LC_OK;
}

int lc_pouch_history_oldest_acknowledged(lc_pouch *pouch, const char *ns,
                                         int *has_consumers,
                                         lc_pouch_generation *out,
                                         lc_error *error) {
  history_oldest oldest = {0};
  int rc;
  if (!pouch || !ns || !*ns || !has_consumers || !out)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch history acknowledgement requires namespace and "
                        "outputs",
                        NULL, NULL, "pouch");
  rc = pouch->storage.ops
           ? history_visit_store(pouch, ns, history_minimum, &oldest, error)
           : history_visit_legacy(pouch, ns, history_minimum, &oldest, error);
  *has_consumers = rc == LC_OK && oldest.found;
  *out = rc == LC_OK ? oldest.seq : 0;
  return rc;
}

static int history_import_one(void *context, const char *id, lc_index_seq seq,
                              lc_error *error) {
  size_t prefix = sizeof(LC_POUCH_HISTORY_CONSUMER_PREFIX) - 1U;
  if (strncmp(id, LC_POUCH_HISTORY_CONSUMER_PREFIX, prefix) || !id[prefix])
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "legacy history consumer name is invalid", NULL, id,
                        "pouch");
  return history_store_write(context, id + prefix, seq, error);
}

int lc_pouch_history_import(lc_pouch *source, lc_pouch *target, const char *ns,
                            lc_error *error) {
  lc_pouch_outbox_store store = {0};
  int rc = history_store_open(target, ns, 0, &store, error);
  if (rc == LC_OK)
    rc = history_visit_legacy(source, ns, history_import_one, &store, error);
  return lc_pouch_outbox_store_close(&store, rc, error);
}

static int history_position_write(lc_pouch *pouch, const char *directory,
                                  const char *path,
                                  lc_pouch_outbox_store *store, const char *id,
                                  lc_index_seq seq, lc_error *error) {
  if (store->ops)
    return history_store_write(store, id, seq, error);
#ifdef LC_POUCH_MULTIFILE_RUNTIME
  return lc_pouch_history_write(pouch, directory, path, seq, error);
#else
  (void)pouch;
  (void)directory;
  (void)path;
  return lc_error_set(error, LC_ERR_UNSUPPORTED, 0L,
                      "legacy history positions are read-only", NULL, NULL,
                      "pouch");
#endif
}

static int history_position_remove(const char *directory, const char *path,
                                   lc_pouch_outbox_store *store, const char *id,
                                   lc_error *error) {
  if (store->ops)
    return store->ops->remove(store->context, id, error);
#ifdef LC_POUCH_MULTIFILE_RUNTIME
  return lc_pouch_history_multifile_remove(directory, path, error);
#else
  (void)directory;
  (void)path;
  return lc_error_set(error, LC_ERR_UNSUPPORTED, 0L,
                      "legacy history positions are read-only", NULL, NULL,
                      "pouch");
#endif
}

static int lc_pouch_history_operation_locked(void *opaque, lc_error *error) {
  lc_pouch_history_operation_context *context;
  lc_history_consumer_handle *consumer;
  lc_pouch *pouch;
  lc_pouch_outbox_store store = {0};
  char *directory;
  char *path;
  lc_index_seq current_index_seq;
  lc_index_seq acknowledged_index_seq;
  int found;
  int rc;

  context = (lc_pouch_history_operation_context *)opaque;
  if (context == NULL || context->consumer == NULL ||
      context->consumer->client == NULL ||
      context->consumer->client->pouch == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch history consumer is unavailable", NULL, NULL,
                        "pouch");
  }
  consumer = context->consumer;
  pouch = consumer->client->pouch;
  directory = NULL;
  path = NULL;
  current_index_seq = 0U;
  acknowledged_index_seq = 0U;
  found = 0;
  rc = lc_pouch_history_current_locked(pouch, consumer->ns, &current_index_seq,
                                       error);
  if (rc == LC_OK && pouch->storage.ops) {
    rc = history_store_open(pouch, consumer->ns,
                            context->operation == LC_POUCH_HISTORY_REGISTER,
                            &store, error);
    if (rc == LC_OK)
      rc = history_store_read(pouch, &store, consumer->consumer_id, &found,
                              &acknowledged_index_seq, error);
  } else if (rc == LC_OK) {
#ifdef LC_POUCH_MULTIFILE_RUNTIME
    rc = lc_pouch_history_file_path(pouch, consumer->ns, consumer->consumer_id,
                                    context->operation ==
                                        LC_POUCH_HISTORY_REGISTER,
                                    &directory, &path, error);
    if (rc == LC_OK)
      rc = lc_pouch_history_staging_cleanup(pouch, directory, error);
    if (rc == LC_OK)
      rc = lc_pouch_history_read(path, &found, &acknowledged_index_seq, error);
#else
    rc = lc_error_set(error, LC_ERR_UNSUPPORTED, 0L,
                      "legacy history positions are read-only", NULL, NULL,
                      "pouch");
#endif
  }
  if (rc == LC_OK && context->operation == LC_POUCH_HISTORY_REGISTER &&
      !found) {
    acknowledged_index_seq = context->requested_acknowledged_index_seq ==
                                     LC_HISTORY_CONSUMER_START_AT_CURRENT
                                 ? current_index_seq
                                 : context->requested_acknowledged_index_seq;
    if (acknowledged_index_seq > current_index_seq) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "history consumer acknowledgement exceeds current "
                        "namespace sequence",
                        NULL, NULL, "pouch");
    } else {
      rc = history_position_write(pouch, directory, path, &store,
                                  consumer->consumer_id, acknowledged_index_seq,
                                  error);
      if (rc == LC_OK) {
        found = 1;
      }
    }
  }
  if (rc == LC_OK && context->operation == LC_POUCH_HISTORY_ADVANCE) {
    if (!found) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch history consumer is no longer registered", NULL,
                        NULL, "pouch");
    } else if (context->requested_acknowledged_index_seq <
               acknowledged_index_seq) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "history consumer acknowledgement cannot move backward",
                        NULL, NULL, "pouch");
    } else if (context->requested_acknowledged_index_seq > current_index_seq) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "history consumer acknowledgement exceeds current "
                        "namespace sequence",
                        NULL, NULL, "pouch");
    } else if (context->requested_acknowledged_index_seq >
               acknowledged_index_seq) {
      rc = lc_pouch_compaction_note_history_advanced(consumer->client->pouch,
                                                     consumer->ns, error);
      if (rc == LC_OK) {
        acknowledged_index_seq = context->requested_acknowledged_index_seq;
        rc = history_position_write(pouch, directory, path, &store,
                                    consumer->consumer_id,
                                    acknowledged_index_seq, error);
      }
    }
  }
  if (rc == LC_OK && context->operation == LC_POUCH_HISTORY_POSITION &&
      !found) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch history consumer is no longer registered", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK && context->operation == LC_POUCH_HISTORY_UNREGISTER) {
    if (found) {
      rc = lc_pouch_compaction_note_history_advanced(consumer->client->pouch,
                                                     consumer->ns, error);
      if (rc == LC_OK)
        rc = history_position_remove(directory, path, &store,
                                     consumer->consumer_id, error);
    }
    if (rc == LC_OK) {
      consumer->registered = 0;
    }
  }
  if (rc == LC_OK && context->operation != LC_POUCH_HISTORY_UNREGISTER &&
      context->out != NULL) {
    context->out->acknowledged_index_seq = acknowledged_index_seq;
    context->out->current_index_seq = current_index_seq;
  }
  lc_free_with_allocator(&consumer->client->allocator, directory);
  lc_free_with_allocator(&consumer->client->allocator, path);
  return lc_pouch_outbox_store_close(&store, rc, error);
}

static int lc_history_consumer_run(lc_history_consumer_handle *consumer,
                                   lc_pouch_history_operation operation,
                                   lc_index_seq acknowledged_index_seq,
                                   lc_history_consumer_position *out,
                                   lc_error *error) {
  lc_pouch_history_operation_context context;

  if (consumer == NULL || consumer->client == NULL ||
      consumer->client->pouch == NULL || !consumer->registered) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "history consumer is closed or unregistered", NULL,
                        NULL, "pouch");
  }
  if (out != NULL) {
    memset(out, 0, sizeof(*out));
  }
  memset(&context, 0, sizeof(context));
  context.consumer = consumer;
  context.operation = operation;
  context.requested_acknowledged_index_seq = acknowledged_index_seq;
  context.out = out;
  return lc_pouch_state_with_namespace_lock(
      consumer->client->pouch, consumer->ns, lc_pouch_history_operation_locked,
      &context, error);
}

static int
lc_history_consumer_position_method(lc_history_consumer *self,
                                    lc_history_consumer_position *out,
                                    lc_error *error) {
  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "history consumer position requires output", NULL, NULL,
                        NULL);
  }
  return lc_history_consumer_run((lc_history_consumer_handle *)self,
                                 LC_POUCH_HISTORY_POSITION, 0U, out, error);
}

static int lc_history_consumer_advance_method(
    lc_history_consumer *self, lc_index_seq acknowledged_index_seq,
    lc_history_consumer_position *out, lc_error *error) {
  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "history consumer advance requires output", NULL, NULL,
                        NULL);
  }
  return lc_history_consumer_run((lc_history_consumer_handle *)self,
                                 LC_POUCH_HISTORY_ADVANCE,
                                 acknowledged_index_seq, out, error);
}

static int lc_history_consumer_unregister_method(lc_history_consumer *self,
                                                 lc_error *error) {
  return lc_history_consumer_run((lc_history_consumer_handle *)self,
                                 LC_POUCH_HISTORY_UNREGISTER, 0U, NULL, error);
}

static void lc_history_consumer_close_method(lc_history_consumer *self) {
  lc_history_consumer_handle *consumer;
  lc_client_handle *client;
  lc_allocator allocator;

  if (self == NULL) {
    return;
  }
  consumer = (lc_history_consumer_handle *)self;
  client = consumer->client;
  memset(&allocator, 0, sizeof(allocator));
  if (client != NULL) {
    allocator = client->allocator;
  }
  lc_free_with_allocator(&allocator, consumer->ns);
  lc_free_with_allocator(&allocator, consumer->consumer_id);
  lc_free_with_allocator(&allocator, consumer);
  if (client != NULL) {
    lc_client_handle_release(client);
  }
}

int lc_client_new_history_consumer_method(
    lc_client *self, const lc_history_consumer_config *config,
    lc_history_consumer **out, lc_error *error) {
  lc_client_handle *client;

  if (self == NULL || config == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "new_history_consumer requires client, config, and "
                        "output",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  client = (lc_client_handle *)self;
  if (client->is_pouch) {
    return lc_pouch_client_new_history_consumer_method(self, config, out,
                                                       error);
  }
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "durable history consumers are currently supported only "
                      "by Pouch",
                      NULL, NULL, NULL);
}

int lc_pouch_client_new_history_consumer_method(
    lc_client *self, const lc_history_consumer_config *config,
    lc_history_consumer **out, lc_error *error) {
  lc_client_handle *client;
  lc_history_consumer_handle *consumer;
  lc_history_consumer_position position;
  int rc;

  if (self == NULL || config == NULL || out == NULL || config->ns == NULL ||
      config->ns[0] == '\0' || config->consumer_id == NULL ||
      config->consumer_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "new_history_consumer requires namespace and consumer "
                        "identity",
                        NULL, NULL, "pouch");
  }
  *out = NULL;
  client = (lc_client_handle *)self;
  if (!client->is_pouch || client->pouch == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "history consumer requires an open Pouch client", NULL,
                        NULL, "pouch");
  }
  consumer = (lc_history_consumer_handle *)lc_client_calloc(client, 1U,
                                                            sizeof(*consumer));
  if (consumer == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate history consumer", NULL, NULL,
                        NULL);
  }
  consumer->client = client;
  consumer->ns = lc_client_strdup(client, config->ns);
  consumer->consumer_id = lc_client_strdup(client, config->consumer_id);
  if (consumer->ns == NULL || consumer->consumer_id == NULL) {
    lc_client_free(client, consumer->ns);
    lc_client_free(client, consumer->consumer_id);
    lc_client_free(client, consumer);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy history consumer identity", NULL, NULL,
                        NULL);
  }
  consumer->registered = 1;
  consumer->pub.position = lc_history_consumer_position_method;
  consumer->pub.advance = lc_history_consumer_advance_method;
  consumer->pub.unregister = lc_history_consumer_unregister_method;
  consumer->pub.close = lc_history_consumer_close_method;
  consumer->pub.impl = consumer;
  lc_client_handle_retain(client);
  memset(&position, 0, sizeof(position));
  rc = lc_history_consumer_run(consumer, LC_POUCH_HISTORY_REGISTER,
                               config->initial_acknowledged_index_seq,
                               &position, error);
  if (rc != LC_OK) {
    lc_history_consumer_close_method(&consumer->pub);
    return rc;
  }
  *out = &consumer->pub;
  return LC_OK;
}
