#ifndef POUCH_SPACE_ABANDONED_POUCH_CASES_H
#define POUCH_SPACE_ABANDONED_POUCH_CASES_H
static void maintenance_abandoned_failure(const char *path,
                                          const char *directory) {
  for (int crypto = 0; crypto < 4; ++crypto)
    for (int sync = 0; sync < 2; ++sync)
      for (int reopened = 0; reopened < 2; ++reopened) {
        lc_error e;
        lc_error_init(&e);
        ps_pouch_state_options o = options(crypto, sync);
        o.maximum_capacity = PS_DEFAULT_CAPACITY;
        lc_pouch *p = NULL;
        lc_pouch_state_write_result written = {0};
        lc_pouch_maintenance_options request = {0};
        lc_pouch_maintenance_result result = {0};
        ps_namespace_status status;
        ps_stats before, after;
        void *ns = NULL;
        input in = source(PS_MIB, 71);
        in.fail_after = 2 * PS_CHUNK;
        ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
        REQUIRE(put(p, "abandoned", "hot", &in, NULL, &written, &e) ==
                LC_ERR_TRANSPORT);
        REQUIRE(in.offset >= in.fail_after && in.offset < in.length);
        clear(&e);
        lc_pouch_state_write_result_cleanup(&allocator, &written);
        if (reopened) {
          lc_pouch_close(p);
          ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
        }
        ok(p->storage.ops->namespace_open(p->storage.context, "abandoned", 0,
                                          &ns, &e),
           &e);
        ok(ps_namespace_status_get(ns, &status, &e), &e);
        REQUIRE(!status.active_segment && status.bound_segments == 1);
        ok(ps_status(ps_root_space(ps_pouch_state_root(p)), &before, &e), &e);
        request.ns = "abandoned";
        request.force = 1;
        ok(lc_pouch_maintenance_run(p, &request, &result, &e), &e);
        REQUIRE(result.cleanup_deleted_count && !result.cleanup_pending_count);
        lc_pouch_maintenance_result_cleanup(&allocator, &result);
        ok(ps_namespace_status_get(ns, &status, &e), &e);
        REQUIRE(!status.active_segment && !status.bound_segments &&
                !status.runs && !status.tail);
        ok(ps_status(ps_root_space(ps_pouch_state_root(p)), &after, &e), &e);
        REQUIRE(after.capacity == PS_DEFAULT_CAPACITY &&
                after.allocated_bytes + PS_PAGE <= before.allocated_bytes);
        in = source(65569, 19);
        ok(put(p, "abandoned", "hot", &in, NULL, &written, &e), &e);
        uint64_t version = written.version;
        REQUIRE(version && in.offset == in.length);
        lc_pouch_state_write_result_cleanup(&allocator, &written);
        read_value(p, "abandoned", "hot", 65569, 19, version, &e);
        p->storage.ops->namespace_close(p->storage.context, ns);
        lc_pouch_close(p);
        ok(ps_pouch_state_open(path, 0, &allocator, &o, &p, &e), &e);
        read_value(p, "abandoned", "hot", 65569, 19, version, &e);
        lc_pouch_close(p);
        one_file(directory);
        REQUIRE(!unlink(path));
        lc_error_cleanup(&e);
      }
}
static void maintenance_abandoned_cycles(const char *path,
                                         const char *directory) {
  for (int crypto = 0; crypto < 4; ++crypto)
    for (int sync = 0; sync < 2; ++sync)
      for (int selected = 0; selected < 2; ++selected) {
        lc_error e;
        lc_error_init(&e);
        ps_pouch_state_options o = options(crypto, sync);
        o.maximum_capacity = PS_DEFAULT_CAPACITY;
        lc_pouch *p = NULL;
        lc_pouch_maintenance_options request = {0};
        lc_pouch_maintenance_result result = {0};
        ps_namespace_status status;
        uint64_t entries = 0;
        void *ns = NULL;
        ok(ps_pouch_state_open(path, 1, &allocator, &o, &p, &e), &e);
        ok(p->storage.ops->namespace_open(p->storage.context, "abandoned", 1,
                                          &ns, &e),
           &e);
        if (selected)
          ok(ps_namespace_rotate(ns, 0, &e), &e);
        request.ns = "abandoned";
        request.force = 1;
        for (unsigned int cycle = 0; cycle < 4; ++cycle) {
          lc_pouch_storage_append upload = {0};
          ok(p->storage.ops->append_begin(p->storage.context, ns, 0, PS_MIB / 4,
                                          LC_POUCH_STORAGE_APPEND_PREFIX, NULL,
                                          &upload, &e),
             &e);
          /* A live producer retains its grants, including an unselected
           * first source. Maintenance reports pending without waiting. */
          ok(lc_pouch_maintenance_run(p, &request, &result, &e), &e);
          REQUIRE(result.cleanup_pending_count);
          lc_pouch_maintenance_result_cleanup(&allocator, &result);
          p->storage.ops->append_close(p->storage.context, &upload);
          for (unsigned int pass = 0; pass < 4; ++pass) {
            ok(lc_pouch_maintenance_run(p, &request, &result, &e), &e);
            int pending = result.cleanup_pending_count != 0;
            lc_pouch_maintenance_result_cleanup(&allocator, &result);
            if (!pending)
              break;
          }
          ok(ps_namespace_status_get(ns, &status, &e), &e);
          REQUIRE(status.bound_segments == (uint64_t)selected &&
                  status.active_segment ==
                      (uint64_t)(selected ? cycle + 2 : 0) &&
                  !status.runs && !status.tail && !status.pending_keys &&
                  !status.checkpoint_keys);
          ps_stats stats;
          ok(ps_status(ps_root_space(ps_pouch_state_root(p)), &stats, &e), &e);
          if (!cycle)
            entries = stats.entries;
          REQUIRE(stats.entries == entries &&
                  stats.capacity == PS_DEFAULT_CAPACITY && !stats.budgets);
        }
        p->storage.ops->namespace_close(p->storage.context, ns);
        lc_pouch_close(p);
        one_file(directory);
        REQUIRE(!unlink(path));
        lc_error_cleanup(&e);
      }
}
static void maintenance_abandoned(const char *path, const char *directory) {
  maintenance_abandoned_failure(path, directory);
  maintenance_abandoned_cycles(path, directory);
}
#endif
