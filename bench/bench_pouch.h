#ifndef BENCH_POUCH_H
#define BENCH_POUCH_H

#include "lc/lc.h"

/* Experimental runs can put both stores at the same child path of a
 * benchmark-owned temporary directory. Standalone probes pass a fresh path. */
int bench_open_pouch(const lc_client_config *config, int create,
                     int owned_directory, lc_client **out, lc_error *error);

int bench_pouch_root_path(char *buffer, size_t buffer_size, const char *suffix);
void bench_pouch_cleanup_root(const char *root);

int bench_pouch_lease(long iterations);
int bench_pouch_state_api(long iterations);
int bench_pouch_attachment(long iterations);
int bench_pouch_queue_delivery(long iterations);
int bench_pouch_queue_watch(long iterations);
int bench_pouch_transaction(long iterations);
int bench_collection_control(long iterations);

#endif
