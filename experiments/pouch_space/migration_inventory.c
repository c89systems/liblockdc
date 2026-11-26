#include "lc_api_internal.h"
#include "lc_pouch_path.h"
#include "migration.h"
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <string.h>
#include <sys/stat.h>

enum inventory_scope {
  ROOT,
  NAMESPACES,
  NAMESPACE,
  SEGMENTS,
  SNAPSHOTS,
  WRITERS,
  MARKERS,
  WATCHES,
  INDEX,
  COLLECTIONS,
  COLLECTION_NAMES,
  COLLECTION_DATA,
  COLLECTION,
  COLLECTION_QUERY,
  COLLECTION_PAGES,
  CONTROL,
  TERMINAL_MARKERS,
  RETENTION_NAMESPACES,
  RETENTION_WORK,
  OUTBOX_NAMESPACES,
  OUTBOX_MARKERS,
  HISTORY_NAMESPACES,
  HISTORY_MARKERS,
  HISTORY_STAGING,
  COMPRESSION_WORK,
  FILE_ENTRY,
  UNKNOWN_ENTRY
};

static int prefix(const char *name, const char *part) {
  return strncmp(name, part, strlen(part)) == 0;
}

static int suffix(const char *name, const char *part) {
  size_t length = strlen(name), end = strlen(part);
  return length >= end && !strcmp(name + length - end, part);
}

static int escaped(const char *name) {
  const char *digits = "0123456789ABCDEFabcdef";
  if (!*name || !strncmp(name, "%00", 3))
    return 0;
  while (*name) {
    if (*name == '%') {
      if (!name[1] || !name[2] || !strchr(digits, name[1]) ||
          !strchr(digits, name[2]))
        return 0;
      name += 3;
    } else if (!strchr("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
                       "0123456789._-",
                       *name++))
      return 0;
  }
  return 1;
}

static int numbered_log(const char *name, const char *part) {
  size_t length = strlen(part);
  return prefix(name, part) && strlen(name) == length + 24U &&
         strspn(name + length, "0123456789") == 20U && suffix(name, ".log");
}

static int one_of(const char *name, const char *const *allowed) {
  while (*allowed && strcmp(name, *allowed))
    ++allowed;
  return *allowed != NULL;
}

static enum inventory_scope file_entry(int matches) {
  return matches ? FILE_ENTRY : UNKNOWN_ENTRY;
}

static enum inventory_scope classify(enum inventory_scope scope,
                                     const char *name, int directory) {
  static const char *const root_files[] = {"manifest",
                                           "manifest.lock",
                                           "writer-mode.lock",
                                           "pouch.mutation.lock",
                                           ".lockdc-control-migration-v1",
                                           ".compression-ready",
                                           PS_MIGRATION_RECEIPT,
                                           NULL};
  static const char *const namespace_files[] = {
      "manifest",   "sequence",    "sequence.lock", "sequence.clean",
      "write.lock", "append.lock", "compact.lock",  NULL};
  static const char *const collection_files[] = {"identity",
                                                 "head",
                                                 "clean",
                                                 "order",
                                                 "ids",
                                                 ".session.lock",
                                                 ".join.lock",
                                                 ".append.lock",
                                                 ".metadata.lock",
                                                 ".streams.lock",
                                                 ".compaction.lock",
                                                 ".commit.lock",
                                                 ".head.pending",
                                                 NULL};
  static const char *const outbox_directories[] = {
      "outbox-state-reclaim",     "outbox-dead-letter-reclaim",
      "outbox-completed-reclaim", "outbox-inbox-reclaim",
      "outbox-command-reclaim",   NULL};
  static const char *const compression_files[] = {"journal.tmp", "payload",
                                                  "plan", "reachable", NULL};
  static const char *const compression_receipts[] = {
      ".head", ".map", ".query-id", ".reach", ".refs", NULL};
  size_t ordinal;
  switch (scope) {
  case ROOT:
    if (directory) {
      if (!strcmp(name, "namespaces"))
        return NAMESPACES;
      if (!strcmp(name, ".compression-new") ||
          !strcmp(name, ".compression-old"))
        return NAMESPACES;
      if (!strcmp(name, ".compression-work"))
        return COMPRESSION_WORK;
      if (!strcmp(name, ".lockd"))
        return CONTROL;
      if (!strcmp(name, "exclusive-writers"))
        return WRITERS;
      if (!strcmp(name, "collection-work"))
        return TERMINAL_MARKERS;
      return UNKNOWN_ENTRY;
    }
    return file_entry(
        one_of(name, root_files) ||
        (prefix(name, "pouch.implicit-txn-") &&
         strlen(name) == sizeof("pouch.implicit-txn-00.lock") - 1 &&
         suffix(name, ".lock")));
  case NAMESPACES:
    return directory && escaped(name) ? NAMESPACE : UNKNOWN_ENTRY;
  case NAMESPACE:
    if (!directory)
      return file_entry(one_of(name, namespace_files));
    if (!strcmp(name, "segments"))
      return SEGMENTS;
    if (!strcmp(name, "snapshots"))
      return SNAPSHOTS;
    if (!strcmp(name, "markers"))
      return MARKERS;
    if (!strcmp(name, "queue-notify"))
      return WATCHES;
    if (!strcmp(name, "index"))
      return INDEX;
    if (!strcmp(name, "collections"))
      return COLLECTIONS;
    return UNKNOWN_ENTRY;
  case SEGMENTS:
    return file_entry(!directory && numbered_log(name, "seg-"));
  case SNAPSHOTS:
    return file_entry(!directory && numbered_log(name, "snapshot-"));
  case WRITERS:
    return file_entry(!directory && prefix(name, "writer-") &&
                      suffix(name, ".presence"));
  case MARKERS:
    return file_entry(!directory && prefix(name, "writer-") &&
                      suffix(name, ".marker"));
  case WATCHES:
    return file_entry(!directory && escaped(name));
  case INDEX:
    return file_entry(!directory &&
                      (prefix(name, "query.") || prefix(name, ".query.")));
  case COLLECTIONS:
    if (directory && !strcmp(name, "names"))
      return COLLECTION_NAMES;
    if (directory && !strcmp(name, "data"))
      return COLLECTION_DATA;
    return file_entry(!directory && !strcmp(name, ".catalog.lock"));
  case COLLECTION_NAMES:
    return file_entry(!directory && strlen(name) == 64U &&
                      strspn(name, "0123456789abcdef") == 64U);
  case COLLECTION_DATA:
    return directory && lc_xid_is_valid(name) ? COLLECTION : UNKNOWN_ENTRY;
  case COLLECTION:
    if (directory)
      return !strcmp(name, "query") ? COLLECTION_QUERY : UNKNOWN_ENTRY;
    return file_entry(
        one_of(name, collection_files) || numbered_log(name, "seg-") ||
        numbered_log(name, "snapshot-") ||
        (prefix(name, ".readers-") && suffix(name, ".lock")) ||
        (prefix(name, "pending-") && suffix(name, ".log")) ||
        prefix(name, "order.rebuild-") || prefix(name, "ids.rebuild-") ||
        prefix(name, "order.compact-") || prefix(name, "ids.compact-") ||
        prefix(name, ".obsolete-"));
  case COLLECTION_QUERY:
    if (directory)
      return lc_xid_is_valid(name) ? COLLECTION_PAGES : UNKNOWN_ENTRY;
    return file_entry(prefix(name, "query.") || prefix(name, ".query."));
  case COLLECTION_PAGES:
    return file_entry(!directory &&
                      (prefix(name, "page-") ||
                       (prefix(name, ".page-") && suffix(name, ".tmp"))));
  case CONTROL:
    if (!directory)
      return UNKNOWN_ENTRY;
    if (!strcmp(name, "terminal-reclaim"))
      return TERMINAL_MARKERS;
    if (!strcmp(name, "state-retention"))
      return RETENTION_NAMESPACES;
    if (!strcmp(name, "history-consumers"))
      return HISTORY_NAMESPACES;
    return one_of(name, outbox_directories) ? OUTBOX_NAMESPACES : UNKNOWN_ENTRY;
  case TERMINAL_MARKERS:
  case OUTBOX_MARKERS:
    return file_entry(!directory && escaped(name));
  case RETENTION_NAMESPACES:
    return directory && escaped(name) ? RETENTION_WORK : UNKNOWN_ENTRY;
  case RETENTION_WORK:
    return file_entry(!directory && !strcmp(name, "work"));
  case OUTBOX_NAMESPACES:
    return directory && escaped(name) ? OUTBOX_MARKERS : UNKNOWN_ENTRY;
  case HISTORY_NAMESPACES:
    return directory && escaped(name) ? HISTORY_MARKERS : UNKNOWN_ENTRY;
  case HISTORY_MARKERS:
    if (directory && !strcmp(name, ".staging"))
      return HISTORY_STAGING;
    return file_entry(!directory && prefix(name, "consumer%3a") &&
                      escaped(name));
  case HISTORY_STAGING:
    return file_entry(!directory && prefix(name, "consumer%3a"));
  case COMPRESSION_WORK:
    ordinal = strspn(name, "0123456789");
    return file_entry(
        !directory &&
        (one_of(name, compression_files) ||
         (ordinal && one_of(name + ordinal, compression_receipts))));
  default:
    return UNKNOWN_ENTRY;
  }
}

static int inventory_error(lc_error *error, int rc, const char *message,
                           const char *path) {
  return lc_error_set(error, rc, 0L, message, path, "pouch_migration_layout",
                      "pouch");
}

static enum inventory_scope classify_entry(enum inventory_scope scope,
                                           const char *name, int directory) {
  const char *temporary, *pid, *attempt;
  char base[NAME_MAX + 1U];
  size_t length;
  enum inventory_scope result = classify(scope, name, directory);
  if (result != UNKNOWN_ENTRY || directory ||
      !(temporary = strstr(name, ".tmp.")))
    return result;
  pid = temporary + sizeof(".tmp.") - 1U;
  attempt = pid + strspn(pid, "0123456789");
  if (attempt == pid || *attempt++ != '.' || !*attempt ||
      attempt[strspn(attempt, "0123456789")] != 0)
    return result;
  length = (size_t)(temporary - name);
  if (!length || length >= sizeof(base))
    return result;
  memcpy(base, name, length);
  base[length] = 0;
  return classify(scope, base, 0);
}

static int walk(const char *path, const lc_allocator *allocator,
                enum inventory_scope scope, dev_t device, lc_error *error) {
  DIR *dir;
  struct dirent *entry;
  int rc = LC_OK;
  dir = opendir(path);
  if (!dir)
    return inventory_error(error, LC_ERR_TRANSPORT,
                           "cannot open legacy pouch directory", path);
  while (rc == LC_OK) {
    enum inventory_scope child_scope;
    struct stat info;
    char *child;

    errno = 0;
    entry = readdir(dir);
    if (!entry) {
      if (errno)
        rc = inventory_error(error, LC_ERR_TRANSPORT,
                             "cannot enumerate legacy pouch directory", path);
      break;
    }
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
      continue;
    child = lc_pouch_path_join(allocator, path, entry->d_name);
    if (!child) {
      rc = inventory_error(error, LC_ERR_NOMEM,
                           "cannot allocate legacy pouch entry path", path);
      break;
    }
    if (lstat(child, &info) != 0)
      rc = inventory_error(error, LC_ERR_TRANSPORT,
                           "cannot identify legacy pouch entry", child);
    else if (info.st_dev != device ||
             (!S_ISDIR(info.st_mode) &&
              (!S_ISREG(info.st_mode) || info.st_nlink != 1)))
      rc = inventory_error(error, LC_ERR_INVALID,
                           "legacy pouch contains an external entry", child);
    else {
      child_scope = classify_entry(scope, entry->d_name, S_ISDIR(info.st_mode));
      if (child_scope == UNKNOWN_ENTRY)
        rc = inventory_error(error, LC_ERR_INVALID,
                             "legacy pouch contains an unrecognized entry",
                             child);
      else if (child_scope != FILE_ENTRY)
        rc = walk(child, allocator, child_scope, device, error);
    }
    lc_free_with_allocator(allocator, child);
  }
  if (closedir(dir) != 0 && rc == LC_OK)
    rc = inventory_error(error, LC_ERR_TRANSPORT,
                         "cannot close legacy pouch directory", path);
  return rc;
}

int ps_pouch_migration_inventory(const char *path,
                                 const lc_allocator *allocator,
                                 lc_error *error) {
  struct stat info;
  if (lstat(path, &info) != 0 || !S_ISDIR(info.st_mode))
    return inventory_error(error, LC_ERR_INVALID,
                           "legacy pouch requires a directory", path);
  return walk(path, allocator, ROOT, info.st_dev, error);
}
