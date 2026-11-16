#ifndef POUCH_SPACE_ROOT_INTERNAL_H
#define POUCH_SPACE_ROOT_INTERNAL_H
#include "root.h"
#include <pthread.h>

struct ps_namespace_data;
struct ps_collection_owner;
struct ps_namespace {
  ps_root *root;
  char *name;
  uint64_t hash;
  size_t pins, users;
  int loading, ready, data_loading;
  ps_namespace_info identity;
  struct ps_namespace_data *data;
  struct ps_collection_owner *collections;
  pthread_mutex_t collection_catalog;
  pthread_mutex_t maintenance;
  unsigned int work_loaded, work_present;
  struct ps_namespace *next, *hash_next;
  struct ps_namespace **link;
};
struct ps_root {
  ps_space *space;
  lc_pouch_crypto *crypto;
  lc_allocator crypto_allocator;
  pthread_mutex_t mutex;
  pthread_cond_t changed;
  ps_namespace *namespaces, **buckets;
  size_t capacity, count, calls, pins;
};
/* Root close has already excluded namespace pins/calls. No payload I/O. */
void ps_namespace_data_destroy(struct ps_namespace_data *);
void ps_namespace_retain(ps_namespace *);
#endif
