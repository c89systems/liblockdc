#ifndef POUCH_SPACE_XA_CAPACITY_CASES_H
#define POUCH_SPACE_XA_CAPACITY_CASES_H
#include "xa_managed_cases.h"
static void xa_capacity(const char *directory) {
  lc_error e;
  char path[1400];
  size_t denied = 0U;
  lc_error_init(&e);
  for (int sync = 0; sync < 2; ++sync)
    for (int managed = 0; managed < 2; ++managed)
      for (int action = 0; action < (managed ? 8 : 11); ++action) {
        REQUIRE(snprintf(path, sizeof(path), "%s/xa-capacity", directory) <
                (int)sizeof(path));
        lc_client *client = xa_client(path, sync, 1, NULL, &e);
        lc_xa_store *journal = NULL, *other = NULL;
        if (managed) {
          ok(lc_xa_store_open_owned(client, xa_owner_ids[0], &journal, &e), &e);
          ok(lc_xa_store_open_owned(client, xa_owner_ids[1], &other, &e), &e);
          xa_managed_seed(client, journal, action, &e);
        } else
          xa_seed(client, action, &e);
        lc_pouch *p = ((lc_client_handle *)client)->pouch;
        ps_space *space = ps_pouch_state_root(p)->space;
        ok(ps_checkpoint(space, &e), &e);
        if (!managed && action == 3)
          xa_stage(client, "{\"value\":84}", &e);
        ps_budget *peer = retention_peer(space, &e);
        int rc = managed ? xa_managed_action(client, journal, other, action, &e)
                         : xa_action(client, action, &e);
        int complete = rc == LC_OK ||
                       (!managed && action == 3 && rc == LC_ERR_PENDING &&
                        e.server_code && !strcmp(e.server_code, "xa_pending"));
        if (!complete) {
          REQUIRE(rc == LC_ERR_INVALID || rc == LC_ERR_NOMEM);
          ++denied;
        }
        clear(&e);
        ok(ps_check(space, &e), &e);
        ok(ps_budget_close(peer, &e), &e);
        if (managed)
          xa_managed_verify(client, journal, other, action, &e);
        else
          xa_verify(client, action, complete, &e);
        lc_xa_store_close(other);
        lc_xa_store_close(journal);
        lc_client_close(client);
        REQUIRE(!live);
        one_file(path);
        struct stat status;
        REQUIRE(!stat(path, &status) && status.st_size == PS_DEFAULT_CAPACITY);
        REQUIRE(!unlink(path));
      }
  REQUIRE(denied);
  lc_error_cleanup(&e);
  printf("XA capacity denied=%zu; retry and 10 MiB size verified\n", denied);
}
#endif
