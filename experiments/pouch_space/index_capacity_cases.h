#ifndef POUCH_SPACE_INDEX_CAPACITY_CASES_H
#define POUCH_SPACE_INDEX_CAPACITY_CASES_H
#include "index_cases.h"
static void index_capacity(const char *directory) {
  lc_error e;
  lc_error_init(&e);
  size_t denied = 0U;
  char path[1400];
  REQUIRE(snprintf(path, sizeof(path), "%s/index-capacity", directory) <
          (int)sizeof(path));
  for (int sync = 0; sync < 2; ++sync)
    for (int action = 0; action < 4; ++action) {
      lc_client *client = index_client(path, sync, 1, 1, NULL, &e);
      index_seed(client, action, &e);
      if (action == 3) {
        lc_client_close(client);
        client = index_client(path, sync, 0, 0, NULL, &e);
      }
      ps_space *space = ps_root_space(ps_pouch_state_root(index_pouch(client)));
      ok(ps_checkpoint(space, &e), &e);
      if (action)
        index_change(client, &e);
      ps_budget *peer = retention_peer(space, &e);
      int rc = index_action(client, action, &e);
      if (rc != LC_OK) {
        REQUIRE(rc == LC_ERR_INVALID || rc == LC_ERR_NOMEM);
        ++denied;
      }
      clear(&e);
      ok(ps_check(space, &e), &e);
      ok(ps_budget_close(peer, &e), &e);
      ok(index_action(client, action, &e), &e);
      lc_client_close(client);
      REQUIRE(!live);
      client = index_client(path, sync, 0, 1, NULL, &e);
      index_verify(client, action, 1, &e);
      lc_client_close(client);
      REQUIRE(!live);
      struct stat st;
      REQUIRE(!stat(path, &st) && st.st_size == PS_DEFAULT_CAPACITY);
      one_file(path);
      REQUIRE(!unlink(path));
    }
  REQUIRE(denied);
  printf("index capacity denials=%zu; retry and 10 MiB capacity verified\n",
         denied);
  lc_error_cleanup(&e);
}
#endif
