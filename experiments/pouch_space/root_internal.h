#ifndef POUCH_SPACE_ROOT_INTERNAL_H
#define POUCH_SPACE_ROOT_INTERNAL_H
#include "coordination.h"
#include "root.h"
#include <pthread.h>

struct ps_namespace_data;
struct ps_collection_owner;
struct ps_outbox_marker_gate;
struct ps_query_index_generation;
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
  pthread_mutex_t query;
  pthread_mutex_t outbox;
  pthread_cond_t outbox_changed;
  struct ps_outbox_marker_gate *outbox_writers;
  struct ps_query_index_generation *query_readers;
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
/* No calls/pins remain; close can count the budgets its caches will release. */
size_t ps_namespace_data_budgets(const struct ps_namespace_data *);
void ps_namespace_retain(ps_namespace *);
uint64_t ps_namespace_name_hash(const char *);
int ps_namespace_guard(ps_namespace *, ps_coordination_guard **, lc_error *);
int ps_namespace_unguard(ps_namespace *, ps_coordination_guard *, int,
                         lc_error *);
/* Exclusive open-time conversion, before exposing any root capability. */
int ps_root_config_write(ps_root *, lc_pouch_crypto *, lc_error *);
int ps_root_conversion_run(ps_root *, const char *, const ps_root_options *,
                           lc_error *);
#ifdef LOCKDC_TEST_BUILD
extern int (*ps_root_conversion_test_hook)(void *, const char *, lc_error *);
extern void *ps_root_conversion_test_context;
#endif
#endif
