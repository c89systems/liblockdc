#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#ifdef __linux__
#include <poll.h>
#include <sys/inotify.h>
#endif

int lc_pouch_multifile_queue_watch_wait(lc_pouch *pouch, const char *ns,
                                        const char *queue,
                                        uint64_t timeout_ms) {
#if defined(__linux__)
  char *namespace_path;
  char *notify_dir;
  char *escaped_queue;
  char *notify_leaf;
  struct pollfd pollfd;
  union {
    unsigned char bytes[4096];
    struct inotify_event alignment;
  } events;
  size_t leaf_len;
  size_t offset;
  ssize_t bytes_read;
  int fd;
  int watch;
  int timeout;
  int ready;
  int result;
  uint64_t timeout_ns;
  uint64_t elapsed_ns;
  uint64_t remaining_ns;
  struct timespec started;
  struct timespec now;
  int clock_started;

  if (pouch == NULL || !pouch->queue_watch_enabled || pouch->aborted ||
      ns == NULL || ns[0] == '\0' || queue == NULL || queue[0] == '\0') {
    return -1;
  }
  namespace_path =
      lc_pouch_namespace_path(&pouch->allocator, pouch->root_path, ns);
  notify_dir = namespace_path != NULL
                   ? lc_pouch_path_join(&pouch->allocator, namespace_path,
                                        "queue-notify")
                   : NULL;
  escaped_queue = lc_pouch_path_escape_name(&pouch->allocator, queue);
  notify_leaf = NULL;
  result = -1;
  if (notify_dir == NULL || escaped_queue == NULL ||
      strlen(escaped_queue) > SIZE_MAX - strlen(".notify") - 1U) {
    goto cleanup;
  }
  leaf_len = strlen(escaped_queue) + strlen(".notify") + 1U;
  notify_leaf = (char *)lc_alloc_with_allocator(&pouch->allocator, leaf_len);
  if (notify_leaf == NULL) {
    goto cleanup;
  }
  snprintf(notify_leaf, leaf_len, "%s.notify", escaped_queue);
  fd = inotify_init();
  if (fd < 0) {
    goto cleanup;
  }
  watch = inotify_add_watch(fd, notify_dir,
                            IN_MOVED_TO | IN_CREATE | IN_CLOSE_WRITE |
                                IN_ATTRIB | IN_DELETE_SELF | IN_MOVE_SELF);
  if (watch < 0) {
    (void)close(fd);
    goto cleanup;
  }
  result = 0;
  timeout_ns = timeout_ms > LC_U64_MAX / (uint64_t)1000000UL
                   ? LC_U64_MAX
                   : timeout_ms * (uint64_t)1000000UL;
  clock_started = clock_gettime(CLOCK_MONOTONIC, &started) == 0;
  memset(&pollfd, 0, sizeof(pollfd));
  pollfd.fd = fd;
  pollfd.events = POLLIN;
  for (;;) {
    if (clock_started && clock_gettime(CLOCK_MONOTONIC, &now) == 0) {
      elapsed_ns = lc_pouch_elapsed_ns(&started, &now);
      if (elapsed_ns >= timeout_ns) {
        break;
      }
      remaining_ns = timeout_ns - elapsed_ns;
      timeout = remaining_ns / (uint64_t)1000000UL >= (uint64_t)INT_MAX
                    ? INT_MAX
                    : (int)((remaining_ns + (uint64_t)999999UL) /
                            (uint64_t)1000000UL);
    } else {
      timeout = timeout_ms > (uint64_t)INT_MAX ? INT_MAX : (int)timeout_ms;
    }
    pollfd.revents = 0;
    ready = poll(&pollfd, 1U, timeout);
    if (ready == 0) {
      break;
    }
    if (ready < 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    if ((pollfd.revents & POLLIN) == 0) {
      break;
    }
    bytes_read = read(fd, events.bytes, sizeof(events.bytes));
    if (bytes_read < 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    offset = 0U;
    while (offset + sizeof(struct inotify_event) <= (size_t)bytes_read) {
      const struct inotify_event *event;
      size_t event_size;

      event = (const struct inotify_event *)(events.bytes + offset);
      event_size = sizeof(*event) + event->len;
      if (event_size > (size_t)bytes_read - offset) {
        break;
      }
      if ((event->mask & (IN_Q_OVERFLOW | IN_DELETE_SELF | IN_MOVE_SELF)) !=
              0U ||
          (event->len > 0U && strcmp(event->name, notify_leaf) == 0)) {
        result = 1;
        break;
      }
      offset += event_size;
    }
    if (result != 0) {
      break;
    }
  }
  (void)inotify_rm_watch(fd, watch);
  (void)close(fd);

cleanup:
  lc_free_with_allocator(&pouch->allocator, notify_leaf);
  lc_free_with_allocator(&pouch->allocator, escaped_queue);
  lc_free_with_allocator(&pouch->allocator, notify_dir);
  lc_free_with_allocator(&pouch->allocator, namespace_path);
  return result;
#else
  (void)pouch;
  (void)ns;
  (void)queue;
  (void)timeout_ms;
  return -1;
#endif
}

void lc_pouch_multifile_queue_notify(lc_client_handle *client, const char *ns,
                                     const char *queue) {
  lc_error ignored;
  char text[256];
  char sequence_text[32];
  char *namespace_path;
  char *notify_dir;
  char *escaped_queue;
  char *notify_leaf;
  char *notify_path;
  size_t leaf_len;
  uint64_t sequence;

  lc_error_init(&ignored);
  namespace_path = NULL;
  notify_dir = NULL;
  escaped_queue = NULL;
  notify_leaf = NULL;
  notify_path = NULL;
  if (lc_pouch_namespace_ensure(&client->allocator, client->pouch->root_path,
                                ns, &ignored) != LC_OK) {
    goto cleanup;
  }
  namespace_path =
      lc_pouch_namespace_path(&client->allocator, client->pouch->root_path, ns);
  notify_dir = namespace_path != NULL
                   ? lc_pouch_path_join(&client->allocator, namespace_path,
                                        "queue-notify")
                   : NULL;
  escaped_queue = lc_pouch_path_escape_name(&client->allocator, queue);
  if (notify_dir == NULL || escaped_queue == NULL) {
    goto cleanup;
  }
  leaf_len = strlen(escaped_queue) + strlen(".notify") + 1U;
  notify_leaf = (char *)lc_alloc_with_allocator(&client->allocator, leaf_len);
  if (notify_leaf == NULL) {
    goto cleanup;
  }
  snprintf(notify_leaf, leaf_len, "%s.notify", escaped_queue);
  notify_path = lc_pouch_path_join(&client->allocator, notify_dir, notify_leaf);
  if (notify_path == NULL) {
    goto cleanup;
  }
  sequence = ++client->pouch->marker_sequence;
  if (lc_u64_format_base10_padded((lc_u64)sequence, 20U, sequence_text,
                                  sizeof(sequence_text)) < 0) {
    goto cleanup;
  }
  snprintf(text, sizeof(text), "queue=%s\nsequence=%s\n%s", escaped_queue,
           sequence_text,
           (sequence % (uint64_t)2U) == (uint64_t)0U ? "pad=x\n" : "");
  (void)lc_pouch_path_write_text_file(notify_path, text, NULL);

cleanup:
  lc_free_with_allocator(&client->allocator, notify_path);
  lc_free_with_allocator(&client->allocator, notify_leaf);
  lc_free_with_allocator(&client->allocator, escaped_queue);
  lc_free_with_allocator(&client->allocator, notify_dir);
  lc_free_with_allocator(&client->allocator, namespace_path);
  lc_error_cleanup(&ignored);
}
