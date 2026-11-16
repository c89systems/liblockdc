# Native-only experimental target, never installed or linked into the product.
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" OR
   NOT CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux" OR
   NOT CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64)$" OR
   NOT LOCKDC_TARGET_ARCH STREQUAL "x86_64" OR NOT TARGET lc_static)
  return()
endif()

set(pouch_space_sources space.c budget.c directory.c stream.c catalog.c record_io.c frame.c control.c collection_segment.c collection_reclaim.c collections.c root.c namespace.c namespace_index.c namespace_snapshot.c namespace_retention.c namespace_work.c outbox_markers.c pouch.c maintenance.c)
add_library(pouch_space_experiment STATIC ${pouch_space_sources})
target_include_directories(pouch_space_experiment PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
  PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(pouch_space_experiment PUBLIC lc_static)
lc_configure_c_target(pouch_space_experiment)

if(LOCKDC_BUILD_BENCHMARKS)
  add_executable(pouch_space_control_compare control_compare.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_control_compare PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_control_compare PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_control_compare PRIVATE
    "LINKER:--wrap=fdatasync" "LINKER:--wrap=fsync")
  lc_configure_c_target(pouch_space_control_compare)
  if(LOCKDC_BUILD_TESTS)
    foreach(mode file space)
      add_test(NAME pouch_space_control_compare_${mode}
        COMMAND pouch_space_control_compare ${mode} ${CMAKE_CURRENT_BINARY_DIR} 16)
      set_tests_properties(pouch_space_control_compare_${mode} PROPERTIES TIMEOUT 60
        LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
    endforeach()
  endif()
  add_executable(pouch_space_compare compare.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_compare PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_compare PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_compare PRIVATE
    "LINKER:--wrap=fdatasync" "LINKER:--wrap=fsync")
  lc_configure_c_target(pouch_space_compare)
  add_executable(pouch_space_queue_outbox_compare queue_outbox_compare.c
    ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_queue_outbox_compare PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_queue_outbox_compare PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_queue_outbox_compare PRIVATE
    "LINKER:--wrap=fdatasync" "LINKER:--wrap=fsync")
  lc_configure_c_test_target(pouch_space_queue_outbox_compare)
  if(LOCKDC_BUILD_TESTS)
    foreach(mode pouch-queue runtime-queue pouch-outbox runtime-outbox)
      foreach(sync 0 1)
        foreach(reopen 0 1)
          set(reopen_args)
          if(reopen)
            set(reopen_args --reopen)
          endif()
          add_test(NAME pouch_space_compare_${mode}_${sync}_${reopen}
            COMMAND pouch_space_queue_outbox_compare ${mode}
              ${CMAKE_CURRENT_BINARY_DIR} 4 4096 ${sync} 2 ${reopen_args})
          set_tests_properties(pouch_space_compare_${mode}_${sync}_${reopen}
            PROPERTIES TIMEOUT 120 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
        endforeach()
      endforeach()
    endforeach()
  endif()
  if(LOCKDC_BUILD_TESTS)
    foreach(mode space-state stream-state runtime-state pouch-state runtime-metadata pouch-metadata runtime-delete pouch-delete runtime-compact pouch-compact runtime-scope pouch-scope runtime-staged pouch-staged runtime-sdk-read-points pouch-sdk-read-points runtime-sdk-scan-metadata pouch-sdk-scan-metadata runtime-read-many pouch-read-many runtime-read-points pouch-read-points runtime-scan-bodies pouch-scan-bodies runtime-scan-metadata pouch-scan-metadata space-collection stream-collection pouch-collection runtime-collection pouch-collection-compact runtime-collection-compact pouch-collection-maintain runtime-collection-maintain)
      add_test(NAME pouch_space_compare_${mode}
        COMMAND pouch_space_compare ${mode} ${CMAKE_CURRENT_BINARY_DIR} 12 4096 0 2 4096)
      set_tests_properties(pouch_space_compare_${mode} PROPERTIES TIMEOUT 60
        LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
      add_test(NAME pouch_space_compare_${mode}_durable
        COMMAND pouch_space_compare ${mode} ${CMAKE_CURRENT_BINARY_DIR} 8 65536 1 2 4096)
      set_tests_properties(pouch_space_compare_${mode}_durable PROPERTIES TIMEOUT 60
        LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
      add_test(NAME pouch_space_compare_${mode}_reopened
        COMMAND pouch_space_compare ${mode} ${CMAKE_CURRENT_BINARY_DIR} 12 4096 0 2 4096 --reopen)
      set_tests_properties(pouch_space_compare_${mode}_reopened PROPERTIES TIMEOUT 60
        LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
      add_test(NAME pouch_space_compare_${mode}_reopened_durable
        COMMAND pouch_space_compare ${mode} ${CMAKE_CURRENT_BINARY_DIR} 8 65536 1 2 4096 --reopen)
      set_tests_properties(pouch_space_compare_${mode}_reopened_durable PROPERTIES TIMEOUT 60
        LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
    endforeach()
  endif()
endif()

if(LOCKDC_BUILD_TESTS)
  add_executable(pouch_space_collection_test collection_test.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_collection_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_collection_test PRIVATE pouch_space_experiment)
  lc_configure_c_test_target(pouch_space_collection_test)
  add_test(NAME pouch_space_collection_storage COMMAND pouch_space_collection_test ${CMAKE_CURRENT_BINARY_DIR})
  set_tests_properties(pouch_space_collection_storage PROPERTIES TIMEOUT 120 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  # Same owner sources against the existing logical-engine test library, so
  # logical expiry clocks never alter filesystem writer-heartbeat clocks.
  add_library(pouch_space_test_experiment STATIC ${pouch_space_sources})
  target_include_directories(pouch_space_test_experiment PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
    PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_compile_definitions(pouch_space_test_experiment PRIVATE LOCKDC_TEST_BUILD=1)
  target_link_libraries(pouch_space_test_experiment PUBLIC lc_test_static lc::zlib_static)
  lc_configure_c_target(pouch_space_test_experiment)
  foreach(surface queue outbox)
    if(surface STREQUAL "queue")
      set(behavior_source ${CMAKE_SOURCE_DIR}/tests/unit/test_lc_pouch.c)
      set(behavior_cases
        test_client_queue_enqueue_dequeue_ack_and_nack
        test_txn_queue_decision_rejects_newer_delivery_lease
        test_client_queue_dequeue_honors_wait_seconds
        test_client_queue_redelivers_abandoned_inflight_after_visibility_timeout
        test_client_queue_large_payload_stats_dequeue_and_ack
        test_txn_decisions_apply_queue_side_effects
        test_txn_recovery_applies_queue_side_effects
        test_client_queue_dequeue_batch_returns_page
        test_client_queue_dequeue_batch_rolls_back_unreturned_messages
        test_client_queue_dequeue_batch_preserves_fifo_payload_order
        test_client_queue_dequeue_cursor_resumes_after_consumed_message
        test_client_queue_dequeue_with_state_uses_pouch_lease
        test_client_queue_dequeue_with_state_rolls_back_partial_delivery
        test_client_queue_state_lease_transaction_update_uses_distinct_lease_key
        test_client_queue_dequeue_with_state_nack_releases_state_lease
        test_client_queue_ttl_and_retry_terminal_states
        test_client_queue_max_durations_obey_timestamp_range
        test_client_queue_subscribe_polling_paths
        test_client_queue_watch_polling_detects_change
        test_client_queue_watch_detects_transaction_ack_commit)
    else()
      set(behavior_source ${CMAKE_SOURCE_DIR}/tests/unit/test_lc_outbox.c)
      set(behavior_cases
        test_pouch_outbox_duplicate_rejects_immutable_envelope_conflicts
        test_pouch_outbox_stateful_delivery_lifecycle
        test_pouch_dispatcher_wait_includes_worker_release
        test_pouch_outbox_stateful_renewal_terminal_versions
        test_pouch_outbox_transaction_and_duplicate
        test_pouch_outbox_stateful_handoff_failure_rolls_back
        test_pouch_duplicate_inbox_handles_json_runtime_failure
        test_pouch_participant_cleanup_after_transaction_close
        test_pouch_participant_allocation_failure_rolls_back_enrollment
        test_pouch_command_receipt_allocation_failure_rolls_back_enrollment
        test_pouch_command_terminal_load_failure_cleans_record
        test_pouch_outbox_allocation_failures_roll_back_enrollment
        test_pouch_command_receipt_commits_with_outbox_and_result
        test_pouch_generated_command_key_waits_by_id
        test_pouch_command_attachment_failure_aborts_transaction
        test_pouch_dead_letter_operations
        test_pouch_terminal_retention_public_transitions
        test_pouch_terminal_retention_count_optout_and_time
        test_pouch_terminal_retention_provisional_inbox
        test_pouch_terminal_retention_generation_busy_and_failure
        test_pouch_dead_letter_retention_is_bounded_and_generation_safe
        test_pouch_dispatcher_scan_recovery_when_indexing_is_disabled)
    endif()
    add_executable(pouch_space_${surface}_sdk_test ${behavior_source}
      queue_outbox_sdk_adapter.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
    target_include_directories(pouch_space_${surface}_sdk_test PRIVATE
      ${CMAKE_SOURCE_DIR}/src)
    target_compile_definitions(pouch_space_${surface}_sdk_test PRIVATE LOCKDC_TEST_BUILD=1)
    target_link_libraries(pouch_space_${surface}_sdk_test PRIVATE
      pouch_space_test_experiment lc::cmocka)
    target_link_options(pouch_space_${surface}_sdk_test PRIVATE
      "LINKER:--wrap=lc_client_open" "LINKER:--wrap=lc_pouch_open")
    if(surface STREQUAL "queue")
      target_link_options(pouch_space_${surface}_sdk_test PRIVATE "LINKER:--wrap=time")
    endif()
    lc_configure_c_test_target(pouch_space_${surface}_sdk_test)
    if(surface STREQUAL "queue")
      add_test(NAME pouch_space_native_queue_sdk
        COMMAND $<TARGET_FILE:lc_unit_pouch> ${behavior_cases})
      set_tests_properties(pouch_space_native_queue_sdk PROPERTIES TIMEOUT 180
        LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
    endif()
    foreach(codec RANGE 0 3)
      foreach(sync RANGE 0 1)
        add_test(NAME pouch_space_${surface}_sdk_${codec}_${sync}
          COMMAND ${CMAKE_COMMAND} -E env POUCH_SPACE_TEST_CODEC=${codec}
            POUCH_SPACE_TEST_SYNC=${sync} $<TARGET_FILE:pouch_space_${surface}_sdk_test>
            ${behavior_cases})
        set_tests_properties(pouch_space_${surface}_sdk_${codec}_${sync}
          PROPERTIES TIMEOUT 180 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
        if(surface STREQUAL "queue")
          add_test(NAME pouch_space_queue_notifications_${codec}_${sync}
            COMMAND ${CMAKE_COMMAND} -E env POUCH_SPACE_TEST_CODEC=${codec}
              POUCH_SPACE_TEST_SYNC=${sync} POUCH_SPACE_TEST_WATCH=1
              $<TARGET_FILE:pouch_space_queue_sdk_test>
              test_client_queue_watch_polling_detects_change
              test_client_queue_watch_detects_transaction_ack_commit)
          set_tests_properties(pouch_space_queue_notifications_${codec}_${sync}
            PROPERTIES TIMEOUT 60 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
        endif()
      endforeach()
    endforeach()
  endforeach()
  add_executable(pouch_space_collection_sdk_test collection_sdk_test.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_collection_sdk_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_compile_definitions(pouch_space_collection_sdk_test PRIVATE LOCKDC_TEST_BUILD=1)
  target_link_libraries(pouch_space_collection_sdk_test PRIVATE pouch_space_test_experiment)
  target_link_options(pouch_space_collection_sdk_test PRIVATE
    "LINKER:--wrap=lc_alloc_with_allocator" "LINKER:--wrap=lc_calloc_with_allocator"
    "LINKER:--wrap=lc_realloc_with_allocator" "LINKER:--wrap=lc_strdup_with_allocator")
  lc_configure_c_test_target(pouch_space_collection_sdk_test)
  add_test(NAME pouch_space_collection_sdk COMMAND pouch_space_collection_sdk_test ${CMAKE_CURRENT_BINARY_DIR})
  set_tests_properties(pouch_space_collection_sdk PROPERTIES TIMEOUT 240 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  foreach(codec RANGE 0 3)
    foreach(sync RANGE 0 1)
      foreach(mode maintenance maintenance-io maintenance-allocation background background-io background-allocation)
        add_test(NAME pouch_space_collection_${mode}_${codec}_${sync}
          COMMAND pouch_space_collection_sdk_test ${CMAKE_CURRENT_BINARY_DIR} ${mode} ${codec} ${sync})
        set_tests_properties(pouch_space_collection_${mode}_${codec}_${sync} PROPERTIES
          TIMEOUT 1200 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
      endforeach()
      add_test(NAME pouch_space_collection_io_${codec}_${sync}
        COMMAND pouch_space_collection_sdk_test ${CMAKE_CURRENT_BINARY_DIR} io ${codec} ${sync})
      set_tests_properties(pouch_space_collection_io_${codec}_${sync} PROPERTIES
        TIMEOUT 1200 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
      add_test(NAME pouch_space_collection_allocation_${codec}_${sync}
        COMMAND pouch_space_collection_sdk_test ${CMAKE_CURRENT_BINARY_DIR} allocation ${codec} ${sync})
      set_tests_properties(pouch_space_collection_allocation_${codec}_${sync} PROPERTIES
        TIMEOUT 1200 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
    endforeach()
  endforeach()
  add_executable(pouch_space_lease_test lease_test.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_lease_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_compile_definitions(pouch_space_lease_test PRIVATE LOCKDC_TEST_BUILD=1)
  target_link_libraries(pouch_space_lease_test PRIVATE pouch_space_test_experiment)
  target_link_options(pouch_space_lease_test PRIVATE
    "LINKER:--wrap=fdatasync" "LINKER:--wrap=fsync"
    "LINKER:--wrap=lc_alloc_with_allocator" "LINKER:--wrap=lc_calloc_with_allocator"
    "LINKER:--wrap=lc_realloc_with_allocator" "LINKER:--wrap=lc_strdup_with_allocator")
  lc_configure_c_test_target(pouch_space_lease_test)
  foreach(mode lifecycle phases policy protection fairness concurrency coordinated)
    add_test(NAME pouch_space_lease_${mode} COMMAND pouch_space_lease_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_lease_${mode} PROPERTIES TIMEOUT 240 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  if(LOCKDC_BUILD_BENCHMARKS)
    add_test(NAME pouch_space_lease_compare COMMAND pouch_space_lease_test ${CMAKE_CURRENT_BINARY_DIR} compare)
    set_tests_properties(pouch_space_lease_compare PROPERTIES TIMEOUT 240 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endif()
  foreach(crypto 0 1 2 3)
    add_test(NAME pouch_space_lease_process-${crypto} COMMAND pouch_space_lease_test ${CMAKE_CURRENT_BINARY_DIR} process-${crypto})
    set_tests_properties(pouch_space_lease_process-${crypto} PROPERTIES TIMEOUT 240 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
    foreach(native 0 1)
      foreach(sync 0 1)
        foreach(cold 0 1)
          foreach(no_error 0 1)
            add_test(NAME pouch_space_lease_allocation-${crypto}-${native}-${sync}-${cold}-${no_error}
              COMMAND pouch_space_lease_test ${CMAKE_CURRENT_BINARY_DIR} allocation-${crypto} ${native} ${sync} ${cold} ${no_error})
            set_tests_properties(pouch_space_lease_allocation-${crypto}-${native}-${sync}-${cold}-${no_error}
              PROPERTIES TIMEOUT 600 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
          endforeach()
        endforeach()
      endforeach()
    endforeach()
    foreach(sync 0 1)
      add_test(NAME pouch_space_lease_io-${crypto}-${sync}
        COMMAND pouch_space_lease_test ${CMAKE_CURRENT_BINARY_DIR} io-${crypto}-${sync})
      set_tests_properties(pouch_space_lease_io-${crypto}-${sync}
        PROPERTIES TIMEOUT 600 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
    endforeach()
  endforeach()
  add_executable(pouch_space_client_test client_test.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_client_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_client_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_client_test PRIVATE
    "LINKER:--wrap=pthread_mutex_init"
    "LINKER:--wrap=lc_alloc_with_allocator" "LINKER:--wrap=lc_calloc_with_allocator"
    "LINKER:--wrap=lc_realloc_with_allocator" "LINKER:--wrap=lc_strdup_with_allocator")
  lc_configure_c_test_target(pouch_space_client_test)
  foreach(mode lifecycle validation faults io-faults namespaces namespace-concurrency namespace-compare retention-codec retention retention-faults retention-io-faults retention-capacity retention-capacity-faults retention-concurrency retention-compare retention-discovery-compare guards guard-concurrency guard-faults guard-public guard-compare outbox-markers outbox-notifications outbox-marker-io-faults outbox-marker-allocation-faults outbox-marker-crash outbox-marker-concurrency)
    add_test(NAME pouch_space_client_${mode} COMMAND pouch_space_client_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_client_${mode} PROPERTIES TIMEOUT 120 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  set_tests_properties(pouch_space_client_outbox-marker-crash PROPERTIES TIMEOUT 300)
  set_tests_properties(pouch_space_client_faults pouch_space_client_io-faults PROPERTIES TIMEOUT 600)
  set_tests_properties(pouch_space_client_retention-faults pouch_space_client_retention-io-faults PROPERTIES TIMEOUT 600)
  add_executable(pouch_space_frame_test frame_test.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_frame_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_frame_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_frame_test PRIVATE "LINKER:--wrap=lc_alloc_with_allocator")
  lc_configure_c_test_target(pouch_space_frame_test)
  foreach(mode codec writer recovery)
    add_test(NAME pouch_space_frames_${mode} COMMAND pouch_space_frame_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_frames_${mode} PROPERTIES TIMEOUT 120 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  add_executable(pouch_space_pouch_test pouch_test.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_pouch_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_pouch_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_pouch_test PRIVATE
    "LINKER:--wrap=OPENSSL_init_crypto"
    "LINKER:--wrap=clock_gettime"
    "LINKER:--wrap=lc_alloc_with_allocator" "LINKER:--wrap=lc_calloc_with_allocator"
    "LINKER:--wrap=lc_realloc_with_allocator" "LINKER:--wrap=lc_strdup_with_allocator")
  lc_configure_c_test_target(pouch_space_pouch_test)
  foreach(mode lifecycle faults concurrency io-faults crypto-init durable-scope durable-scope-faults durable-scope-crash validation mutations mutation-faults mutation-io-faults retention-hint mutation-concurrency mutation-capacity key-bounds reads read-faults read-io-faults read-concurrency snapshots projection projection-faults projection-concurrency sequence-recovery lifetimes lifetime-faults staging staging-faults staging-recovery staging-recovery-faults staging-concurrency staging-crash maintenance maintenance-faults maintenance-io-faults maintenance-concurrency maintenance-checksums)
    add_test(NAME pouch_space_pouch_${mode} COMMAND pouch_space_pouch_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_pouch_${mode} PROPERTIES TIMEOUT 240
      LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  # Thousands of independent cold/warm allocation failures also run under TSAN.
  set_tests_properties(pouch_space_pouch_read-faults PROPERTIES TIMEOUT 600)
  set_tests_properties(pouch_space_pouch_lifetime-faults PROPERTIES TIMEOUT 600)
  set_tests_properties(pouch_space_pouch_staging-faults PROPERTIES TIMEOUT 600)
  foreach(mode staging-active-faults staging-active-recovery-faults)
    add_test(NAME pouch_space_pouch_${mode} COMMAND pouch_space_pouch_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_pouch_${mode} PROPERTIES TIMEOUT 600 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  foreach(crypto 0 1 2 3)
    foreach(sync 0 1)
      foreach(rotation 0 1)
        add_test(NAME pouch_space_pouch_staging-crash-${crypto}-${sync}-${rotation}
          COMMAND pouch_space_pouch_test ${CMAKE_CURRENT_BINARY_DIR} staging-crash-matrix ${crypto} ${sync} ${rotation})
        set_tests_properties(pouch_space_pouch_staging-crash-${crypto}-${sync}-${rotation}
          PROPERTIES TIMEOUT 240 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
      endforeach()
    endforeach()
  endforeach()
  set_tests_properties(pouch_space_pouch_maintenance-faults pouch_space_pouch_maintenance-io-faults PROPERTIES TIMEOUT 600)
  add_executable(pouch_space_namespace_test namespace_test.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_namespace_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_namespace_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_namespace_test PRIVATE
    "LINKER:--wrap=lc_alloc_with_allocator" "LINKER:--wrap=lc_calloc_with_allocator"
    "LINKER:--wrap=lc_realloc_with_allocator" "LINKER:--wrap=lc_strdup_with_allocator")
  lc_configure_c_test_target(pouch_space_namespace_test)
  foreach(mode lifecycle faults concurrency validation reservation publication snapshots snapshot-faults snapshot-concurrency snapshot-churn bodies body-budget body-budget-faults append-budget append-budget-faults snapshot-budget snapshot-budget-faults)
    add_test(NAME pouch_space_namespace_${mode}
      COMMAND pouch_space_namespace_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_namespace_${mode} PROPERTIES TIMEOUT 120
      LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  set_tests_properties(pouch_space_namespace_body-budget-faults pouch_space_namespace_append-budget-faults pouch_space_namespace_snapshot-budget-faults PROPERTIES TIMEOUT 600)
  add_executable(pouch_space_state_record_test state_record_test.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_state_record_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_state_record_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_state_record_test PRIVATE
    "LINKER:--wrap=lc_alloc_with_allocator" "LINKER:--wrap=lc_calloc_with_allocator"
    "LINKER:--wrap=lc_realloc_with_allocator" "LINKER:--wrap=lc_strdup_with_allocator")
  lc_configure_c_test_target(pouch_space_state_record_test)
  foreach(mode lifecycle faults concurrency snapshots snapshot-faults)
    add_test(NAME pouch_space_state_records_${mode}
      COMMAND pouch_space_state_record_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_state_records_${mode} PROPERTIES TIMEOUT 120
      LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  add_executable(pouch_space_root_test root_test.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_root_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_root_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_root_test PRIVATE
    "LINKER:--wrap=lc_alloc_with_allocator"
    "LINKER:--wrap=lc_calloc_with_allocator"
    "LINKER:--wrap=lc_realloc_with_allocator"
    "LINKER:--wrap=lc_strdup_with_allocator")
  lc_configure_c_test_target(pouch_space_root_test)
  foreach(mode lifecycle faults concurrency)
    add_test(NAME pouch_space_root_${mode}
      COMMAND pouch_space_root_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_root_${mode} PROPERTIES TIMEOUT 120
      LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  add_executable(pouch_space_control_test control_test.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_control_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_control_test PRIVATE pouch_space_experiment)
  lc_configure_c_test_target(pouch_space_control_test)
  add_test(NAME pouch_space_controls COMMAND pouch_space_control_test ${CMAKE_CURRENT_BINARY_DIR})
  set_tests_properties(pouch_space_controls PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  add_executable(pouch_space_record_test record_test.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_record_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_record_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_record_test PRIVATE "LINKER:--wrap=writev" "LINKER:--wrap=write")
  if(LOCKDC_TARGET_LIBC STREQUAL "gnu")
    target_compile_definitions(pouch_space_record_test PRIVATE RECORD_PREADV64=1)
    target_link_options(pouch_space_record_test PRIVATE "LINKER:--wrap=preadv64" "LINKER:--wrap=pread64")
  else()
    target_link_options(pouch_space_record_test PRIVATE "LINKER:--wrap=preadv" "LINKER:--wrap=pread")
  endif()
  lc_configure_c_test_target(pouch_space_record_test)
  add_test(NAME pouch_space_records COMMAND pouch_space_record_test ${CMAKE_CURRENT_BINARY_DIR})
  set_tests_properties(pouch_space_records PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  add_executable(pouch_space_directory_test directory_test.c)
  target_link_libraries(pouch_space_directory_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_directory_test PRIVATE
    "LINKER:--wrap=lc_alloc_with_allocator"
    "LINKER:--wrap=lc_calloc_with_allocator")
  lc_configure_c_test_target(pouch_space_directory_test)
  add_test(NAME pouch_space_directory COMMAND pouch_space_directory_test)
  set_tests_properties(pouch_space_directory PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  add_executable(pouch_space_test space_test.c)
  target_link_libraries(pouch_space_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_test PRIVATE
    "LINKER:--wrap=lc_alloc_with_allocator"
    "LINKER:--wrap=lc_calloc_with_allocator"
    "LINKER:--wrap=lc_realloc_with_allocator"
    "LINKER:--wrap=lc_strdup_with_allocator")
  target_include_directories(pouch_space_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  lc_configure_c_test_target(pouch_space_test)
  add_test(NAME pouch_space_foundation COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR})
  add_test(NAME pouch_space_commit COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} commit)
  add_test(NAME pouch_space_budget COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} budget)
  add_test(NAME pouch_space_budget-capacity COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} budget-capacity)
  foreach(mode value-budget value-budget-capacity value-budget-faults value-budget-concurrency value-budget-process)
    add_test(NAME pouch_space_${mode} COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_${mode} PROPERTIES TIMEOUT 240
      LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  add_test(NAME pouch_space_grant-budget COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} grant-budget)
  add_test(NAME pouch_space_append-budget COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} append-budget)
  set_tests_properties(pouch_space_append-budget PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  add_test(NAME pouch_space_grant-budget-faults COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} grant-budget-faults)
  set_tests_properties(pouch_space_grant-budget pouch_space_grant-budget-faults PROPERTIES TIMEOUT 600
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_budget pouch_space_budget-capacity PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  add_test(NAME pouch_space_reopen COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} reopen)
  add_test(NAME pouch_space_reopen-sanitize COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} reopen-sanitize)
  set_tests_properties(pouch_space_reopen-sanitize PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  add_test(NAME pouch_space_reopen-ordering COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} reopen-ordering)
  set_tests_properties(pouch_space_reopen-ordering PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  add_test(NAME pouch_space_metadata COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} metadata)
  add_test(NAME pouch_space_metadata-capacity COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} metadata-capacity)
  add_test(NAME pouch_space_metadata-capacity-faults COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} metadata-capacity-faults)
  add_test(NAME pouch_space_metadata-repack COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} metadata-repack)
  add_test(NAME pouch_space_metadata-repack-faults COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} metadata-repack-faults)
  add_test(NAME pouch_space_metadata-growth COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} metadata-growth)
  add_test(NAME pouch_space_metadata-growth-faults COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} metadata-growth-faults)
  add_test(NAME pouch_space_packing COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} packing)
  add_test(NAME pouch_space_streams COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} streams)
  add_test(NAME pouch_space_stream-recovery COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} stream-recovery)
  add_test(NAME pouch_space_lifecycle COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} lifecycle)
  add_test(NAME pouch_space_catalog COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} catalog)
  set_tests_properties(pouch_space_catalog PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_lifecycle PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_streams PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_stream-recovery PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_packing PROPERTIES TIMEOUT 60
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_metadata PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_metadata-capacity PROPERTIES TIMEOUT 240
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_metadata-capacity-faults PROPERTIES TIMEOUT 600
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_metadata-repack pouch_space_metadata-repack-faults pouch_space_metadata-growth PROPERTIES TIMEOUT 240
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_metadata-growth-faults PROPERTIES TIMEOUT 600
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_reopen PROPERTIES TIMEOUT 30
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_commit PROPERTIES TIMEOUT 30
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_foundation PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  lc_apply_test_defaults()
endif()
