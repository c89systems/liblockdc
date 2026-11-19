# Native-only experimental target, never installed or linked into the product.
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" OR
   NOT CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux" OR
   NOT CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64)$" OR
   NOT LOCKDC_TARGET_ARCH STREQUAL "x86_64" OR NOT TARGET lc_static)
  return()
endif()

set(pouch_space_sources space.c budget.c directory.c stream.c catalog.c record_io.c frame.c control.c collection_segment.c collection_reclaim.c collection_conversion.c collection_walk.c collection_transform.c collections.c root.c root_conversion.c namespace.c namespace_index.c namespace_metadata.c namespace_reclaim.c namespace_recode.c namespace_conversion.c namespace_repack.c namespace_retention.c namespace_work.c outbox_markers.c query_index.c query_revision.c checkpoint_record.c checkpoint.c pouch.c maintenance.c)
list(APPEND pouch_space_sources coordination.c readiness.c)
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
  add_executable(pouch_space_lifecycle_compare lifecycle_compare.c
    ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_lifecycle_compare PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_lifecycle_compare PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_lifecycle_compare PRIVATE
    "LINKER:--wrap=fdatasync" "LINKER:--wrap=fsync")
  lc_configure_c_test_target(pouch_space_lifecycle_compare)
  if(LOCKDC_BUILD_TESTS)
    foreach(mode pouch-index runtime-index)
      foreach(sync 0 1)
        add_test(NAME pouch_space_compare_${mode}_churn_${sync}
          COMMAND pouch_space_lifecycle_compare ${mode}
            ${CMAKE_CURRENT_BINARY_DIR} 8 65536 ${sync} 4 --reopen --rounds 4)
        set_tests_properties(pouch_space_compare_${mode}_churn_${sync}
          PROPERTIES TIMEOUT 120 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
        add_test(NAME pouch_space_compare_${mode}_stable_${sync}
          COMMAND pouch_space_lifecycle_compare ${mode}
            ${CMAKE_CURRENT_BINARY_DIR} 8 65536 ${sync} 4 --reopen --rounds 4
            --stable-index)
        set_tests_properties(pouch_space_compare_${mode}_stable_${sync}
          PROPERTIES TIMEOUT 120 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
      endforeach()
    endforeach()
  endif()
  if(LOCKDC_BUILD_TESTS)
    foreach(mode pouch-queue runtime-queue pouch-outbox runtime-outbox pouch-xa runtime-xa pouch-index runtime-index)
      foreach(sync 0 1)
        foreach(reopen 0 1)
          set(reopen_args)
          if(reopen)
            set(reopen_args --reopen)
          endif()
          add_test(NAME pouch_space_compare_${mode}_${sync}_${reopen}
            COMMAND pouch_space_lifecycle_compare ${mode}
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
  add_executable(pouch_space_live_allocator_test live_allocator_test.c)
  target_include_directories(pouch_space_live_allocator_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_live_allocator_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_live_allocator_test PRIVATE
    "LINKER:--wrap=ps_coordination_open"
    "LINKER:--wrap=mmap" "LINKER:--wrap=mmap64"
    "LINKER:--wrap=lc_alloc_with_allocator" "LINKER:--wrap=lc_calloc_with_allocator"
    "LINKER:--wrap=lc_realloc_with_allocator" "LINKER:--wrap=lc_strdup_with_allocator")
  lc_configure_c_test_target(pouch_space_live_allocator_test)
  foreach(mode threads faults fence processes serialized packed history
      captures capture-faults capture-owned capture-race
      projection projection-faults projection-recovery projection-threads
      projection-boundaries projection-pressure projection-tail
      shared-durable readiness-cohort
      readiness-faults journal-replay journal-replay-faults
      journal-replay-structural journal-replay-process
      owned-stream-readiness owned-stream-cold owned-stream-faults)
    add_test(NAME pouch_space_live-allocator_${mode}
      COMMAND pouch_space_live_allocator_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_live-allocator_${mode} PROPERTIES TIMEOUT 120
      LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  add_executable(pouch_space_physical_claim_test physical_claim_test.c)
  target_include_directories(pouch_space_physical_claim_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_physical_claim_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_physical_claim_test PRIVATE
    "LINKER:--wrap=ps_coordination_open"
    "LINKER:--wrap=lc_alloc_with_allocator" "LINKER:--wrap=lc_calloc_with_allocator"
    "LINKER:--wrap=lc_realloc_with_allocator" "LINKER:--wrap=lc_strdup_with_allocator")
  lc_configure_c_test_target(pouch_space_physical_claim_test)
  foreach(mode lifecycle faults producers processes metadata-lifecycle metadata-faults metadata-processes metadata-producers)
    add_test(NAME pouch_space_physical-claims_${mode}
      COMMAND pouch_space_physical_claim_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_physical-claims_${mode} PROPERTIES TIMEOUT 120
      LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  add_executable(pouch_space_reader_pin_test reader_pin_test.c)
  target_include_directories(pouch_space_reader_pin_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_reader_pin_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_reader_pin_test PRIVATE
    "LINKER:--wrap=lc_alloc_with_allocator" "LINKER:--wrap=lc_calloc_with_allocator"
    "LINKER:--wrap=lc_realloc_with_allocator" "LINKER:--wrap=lc_strdup_with_allocator")
  if(LOCKDC_TARGET_LIBC STREQUAL "gnu")
    target_compile_definitions(pouch_space_reader_pin_test PRIVATE PIN_LIBC64=1)
    target_link_options(pouch_space_reader_pin_test PRIVATE
      "LINKER:--wrap=fcntl64" "LINKER:--wrap=pread64")
  else()
    target_link_options(pouch_space_reader_pin_test PRIVATE
      "LINKER:--wrap=fcntl" "LINKER:--wrap=pread")
  endif()
  lc_configure_c_test_target(pouch_space_reader_pin_test)
  foreach(mode lifecycle pressure malformed faults admission threads writer-death fragmented)
    add_test(NAME pouch_space_reader-pins_${mode}
      COMMAND pouch_space_reader_pin_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_reader-pins_${mode} PROPERTIES TIMEOUT 240
      LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  add_executable(pouch_space_coordination_test coordination_test.c)
  target_include_directories(pouch_space_coordination_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_coordination_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_coordination_test PRIVATE
    "LINKER:--wrap=pthread_mutex_init"
    "LINKER:--wrap=lc_alloc_with_allocator"
    "LINKER:--wrap=lc_calloc_with_allocator"
    "LINKER:--wrap=lc_realloc_with_allocator"
    "LINKER:--wrap=lc_strdup_with_allocator")
  lc_configure_c_test_target(pouch_space_coordination_test)
  foreach(mode lifecycle root-lifetime threads faults processes)
    add_test(NAME pouch_space_coordination_${mode}
      COMMAND pouch_space_coordination_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_coordination_${mode} PROPERTIES TIMEOUT 120
      LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  add_executable(pouch_space_collection_test collection_test.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_collection_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_collection_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_collection_test PRIVATE
    "LINKER:--wrap=lc_alloc_with_allocator" "LINKER:--wrap=lc_calloc_with_allocator"
    "LINKER:--wrap=lc_realloc_with_allocator" "LINKER:--wrap=lc_strdup_with_allocator")
  lc_configure_c_test_target(pouch_space_collection_test)
  add_test(NAME pouch_space_collection_storage COMMAND pouch_space_collection_test ${CMAKE_CURRENT_BINARY_DIR})
  set_tests_properties(pouch_space_collection_storage PROPERTIES TIMEOUT 120 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  foreach(mode units unit-faults unit-concurrency records record-faults workspace workspace-faults walk walk-faults transform transform-faults transform-prepare-faults transform-concurrency)
    add_test(NAME pouch_space_collection_${mode}
      COMMAND pouch_space_collection_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_collection_${mode} PROPERTIES TIMEOUT 600
      LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  set_tests_properties(pouch_space_collection_transform-faults
    pouch_space_collection_transform-prepare-faults PROPERTIES TIMEOUT 1800)
  # Same owner sources against the existing logical-engine test library, so
  # logical expiry clocks never alter filesystem writer-heartbeat clocks.
  add_library(pouch_space_test_experiment STATIC ${pouch_space_sources})
  target_include_directories(pouch_space_test_experiment PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
    PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_compile_definitions(pouch_space_test_experiment PRIVATE LOCKDC_TEST_BUILD=1)
  target_link_libraries(pouch_space_test_experiment PUBLIC lc_test_static lc::zlib_static)
  lc_configure_c_target(pouch_space_test_experiment)
  foreach(surface queue outbox xa query collection-query)
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
    elseif(surface STREQUAL "outbox")
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
    elseif(surface STREQUAL "xa")
      set(behavior_source ${CMAKE_SOURCE_DIR}/tests/unit/test_lc_pouch_xa.c)
      # Shared-root client cases remain native controls until shared physical
      # ownership is integrated. Every exclusive journal/coordinator case runs.
      set(behavior_cases
        test_history_default_age_and_idempotent_completion
        test_history_live_owner_and_unfinished_work_are_protected
        test_history_count_cap_retires_oldest_on_release
        test_history_invalid_policy_and_corrupt_completion_fail_closed
        test_history_unfinished_commit_preserves_branch_until_application
        test_history_interrupted_retirement_resumes_after_reopen
        test_xa_outbox_detached_owner_closes_application_views
        test_xa_outbox_sql_first_binding_and_receipts
        test_xa_outbox_first_adoption_and_rollback
        test_xa_outbox_close_during_pending_commit_keeps_owner
        test_coordinator_owner_checks_and_deferred_transaction_close
        test_coordinator_state_only_commit_and_retained_receivers
        test_coordinator_cancelled_begin_and_active_owner_recovery
        test_coordinator_raw_terminal_replay_preserves_owner_epoch
        test_coordinator_rejected_same_owner_recovery_preserves_live_claim
        test_coordinator_begin_journal_failure_preserves_id
        test_managed_pouch_vote_and_independent_transactions
        test_managed_owner_exclusion_and_commit_takeover
        test_managed_owner_begin_failure_retains_recoverable_identity
        test_managed_owner_publication_failure_keeps_terminal_decision
        test_tc_expired_owner_cannot_reuse_an_old_epoch
        test_tc_release_preserves_epoch_across_reopen
        test_driver_contract_pins_closed_owners_and_preallocates_branch
        test_driver_contract_pins_transferred_branch_until_operation_close
        test_driver_contract_reclaims_unclaimed_and_cancelled_branches
        test_native_prepare_observation_is_durable_without_yes
        test_public_store_rejects_completion_contrary_to_decision
        test_public_store_pins_client_and_preserves_resource_identity
        test_pending_commit_survives_reopen
        test_all_external_branches_complete_before_application
        test_rollback_waits_for_uncertain_prepare_cleanup
        test_enrollment_seals_and_opposite_resolution_is_rejected
        test_branch_identity_is_immutable_and_namespace_private
        test_missing_descriptor_blocks_entire_application
        test_invalid_native_identity_and_phase_are_rejected
        test_prepare_intent_is_not_a_yes_vote_after_reopen
        test_expired_prepare_preserves_abort_until_external_cleanup
        test_corrupt_descriptor_never_opens_application_gate
        test_native_lease_vote_waits_for_external_completion)
    elseif(surface STREQUAL "collection-query")
      set(behavior_source ${CMAKE_SOURCE_DIR}/tests/unit/test_lc_collections.c)
      # Physical path corruption/cleanup controls stay in the native suite.
      # These existing cases specify public selection and page semantics.
      set(behavior_cases
        test_query_build_consumes_large_states_incrementally
        test_indexed_filters_agree_with_scan_and_cover_unindexed_tail
        test_query_index_does_not_retry_valid_out_of_range_numbers
        test_query_pruning_preserves_an_active_visibility_view
        test_indexed_resume_retains_horizon_and_accepted_nonmatches
        test_late_partial_query_page_cannot_replace_a_complete_page
        test_selection_first_n_complete_windows_and_checkpoints
        test_selection_first_n_resume_budget_binding_and_accepted_cursor
        test_selection_first_n_cancellation_preserves_unconsumed_budget
        test_selection_index_scan_parity_across_pages
        test_selection_survives_transforms_compaction_and_restart
        test_selection_first_n_lf_failure_preserves_entry_budget
        test_selection_captures_scalar_options_before_callbacks
        test_selection_filters_after_input_count
        test_selection_filtered_progress_rejection_preserves_input_budget
        test_selection_coalesced_nonmatches_preserve_input_budget)
    else()
      set(behavior_source ${CMAKE_SOURCE_DIR}/tests/unit/test_lc_pouch.c)
      # Keep shared-client/shared-compaction and native transformed-body-cache
      # allocation controls in the native suite. These are exclusive API cases.
      set(behavior_cases
        test_query_doc_table_cache_borrow_survives_publication
        test_query_generation_cache_survives_parallel_eviction
        test_query_doc_table_cache_rows_survive_concurrent_eviction
        test_query_doc_table_cache_serializes_decoded_key_population
        test_parallel_state_writes_keep_query_projection_consistent
        test_parallel_query_flush_preserves_later_write
        test_query_index_staleness_is_namespace_scoped
        test_pouch_indexer_deadline_clamps_u64_interval
        test_exclusive_indexer_publishes_at_document_threshold
        test_exclusive_indexer_publishes_delete_batch_at_document_threshold
        test_exclusive_indexer_waits_for_all_active_operations
        test_exclusive_indexer_operation_guards_are_lease_scoped
        test_exclusive_indexer_retires_expired_operation_guard
        test_exclusive_indexer_cancels_guard_after_metadata_copy_failure
        test_exclusive_indexer_idle_flush_waits_for_active_operation
        test_exclusive_indexer_idle_flushes_direct_state_mutations
        test_exclusive_indexer_idle_flush_waits_for_incomplete_active_operation
        test_exclusive_indexer_publishes_transaction_decision_at_threshold
        test_exclusive_indexer_clears_guards_after_failed_transaction_decision
        test_exclusive_indexer_defers_lease_metadata_until_release
        test_query_index_sequence_ignores_lease_metadata
        test_incremental_query_index_ignores_lease_only_key
        test_query_index_ignores_internal_objects
        test_staged_object_delete_does_not_queue_query_index
        test_query_index_threshold_handles_repeated_lease_updates
        test_query_index_threshold_handles_deferred_first_lease_update
        test_pouch_endpoint_query_engine_routes_implicit_queries
        test_query_indexing_opt_out_invalidates_before_compaction
        test_query_watermark_waits_for_exclusive_namespace_mutation
        test_transaction_bound_lease_commit_makes_first_body_queryable
        test_lease_metadata_persists_query_hidden
        test_query_keys_scan_uses_liblql_and_query_hidden
        test_query_keys_callback_can_reenter_pouch_public_api
        test_query_keys_enforces_lockd_limit_contract
        test_query_index_sync_flush_uses_durable_state
        test_query_keys_index_summary_preserves_callback_failure
        test_query_keys_index_all_text_preserves_complete_matches
        test_query_keys_index_all_text_compacts_candidate_matches
        test_query_keys_index_all_text_token_postings_preserve_substrings
        test_query_keys_index_preserves_strict_json_pointer_paths
        test_query_keys_index_root_or_uses_scalar_union
        test_query_keys_index_long_strings_use_validated_candidates
        test_query_keys_index_large_strings_use_fallback_candidates
        test_query_keys_index_date_lql_filters_temporal_candidates
        test_query_keys_index_recursive_exists_uses_container_presence
        test_query_keys_index_rejects_wildcard_exists_selectors
        test_query_documents_scan_matches_escaped_pointer_paths
        test_query_documents_scan_streams_rows
        test_query_documents_index_uses_scalar_postings
        test_exclusive_indexer_retires_expired_acquire_for_update_guard
        test_acquire_for_update_first_body_is_queryable)
    endif()
    add_executable(pouch_space_${surface}_sdk_test ${behavior_source}
      sdk_test_adapter.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
    target_include_directories(pouch_space_${surface}_sdk_test PRIVATE
      ${CMAKE_SOURCE_DIR}/src)
    target_compile_definitions(pouch_space_${surface}_sdk_test PRIVATE LOCKDC_TEST_BUILD=1)
    if(surface STREQUAL "collection-query")
      target_compile_definitions(pouch_space_${surface}_sdk_test PRIVATE
        LC_COLLECTION_TMP_PARENT="${CMAKE_CURRENT_BINARY_DIR}")
    endif()
    target_link_libraries(pouch_space_${surface}_sdk_test PRIVATE
      pouch_space_test_experiment lc::cmocka)
    target_link_options(pouch_space_${surface}_sdk_test PRIVATE
      "LINKER:--wrap=lc_client_open" "LINKER:--wrap=lc_pouch_open")
    if(surface STREQUAL "queue" OR surface STREQUAL "query")
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
        set(index_environment)
        if(surface STREQUAL "query" OR surface STREQUAL "collection-query")
          set(index_environment POUCH_SPACE_TEST_INDEX=1)
        endif()
        add_test(NAME pouch_space_${surface}_sdk_${codec}_${sync}
          COMMAND ${CMAKE_COMMAND} -E env POUCH_SPACE_TEST_CODEC=${codec}
            POUCH_SPACE_TEST_SYNC=${sync} ${index_environment} $<TARGET_FILE:pouch_space_${surface}_sdk_test>
            ${behavior_cases})
        set_tests_properties(pouch_space_${surface}_sdk_${codec}_${sync}
          PROPERTIES TIMEOUT 180 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
        set(conversion_cases)
        if(surface STREQUAL "queue")
          set(conversion_cases test_txn_recovery_applies_queue_side_effects
            test_txn_queue_decision_rejects_newer_delivery_lease)
        elseif(surface STREQUAL "outbox")
          set(conversion_cases test_pouch_dispatcher_scan_recovery_when_indexing_is_disabled)
        elseif(surface STREQUAL "xa")
          set(conversion_cases test_pending_commit_survives_reopen
            test_history_unfinished_commit_preserves_branch_until_application)
        endif()
        if(conversion_cases)
          add_test(NAME pouch_space_root_conversion_${surface}_${codec}_${sync}
            COMMAND ${CMAKE_COMMAND} -E env POUCH_SPACE_TEST_CODEC=${codec}
              POUCH_SPACE_TEST_SYNC=${sync} POUCH_SPACE_TEST_CONVERT=1
              $<TARGET_FILE:pouch_space_${surface}_sdk_test> ${conversion_cases})
          set_tests_properties(pouch_space_root_conversion_${surface}_${codec}_${sync}
            PROPERTIES TIMEOUT 180 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
        endif()
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
  add_test(NAME pouch_space_root_conversion COMMAND pouch_space_collection_sdk_test ${CMAKE_CURRENT_BINARY_DIR} root-conversion)
  set_tests_properties(pouch_space_root_conversion PROPERTIES TIMEOUT 240 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  add_test(NAME pouch_space_root_conversion-growth COMMAND pouch_space_collection_sdk_test ${CMAKE_CURRENT_BINARY_DIR} root-conversion-growth)
  set_tests_properties(pouch_space_root_conversion-growth PROPERTIES TIMEOUT 240 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  foreach(codec RANGE 0 3)
    foreach(sync RANGE 0 1)
      add_test(NAME pouch_space_root_conversion-faults_${codec}_${sync}
        COMMAND pouch_space_collection_sdk_test ${CMAKE_CURRENT_BINARY_DIR} root-conversion-faults ${codec} ${sync})
      set_tests_properties(pouch_space_root_conversion-faults_${codec}_${sync} PROPERTIES TIMEOUT 1200 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
      if(codec EQUAL 0 OR codec EQUAL 3)
        add_test(NAME pouch_space_root_conversion-crash_${codec}_${sync}
          COMMAND pouch_space_collection_sdk_test ${CMAKE_CURRENT_BINARY_DIR} root-conversion-crash ${codec} ${sync})
        set_tests_properties(pouch_space_root_conversion-crash_${codec}_${sync} PROPERTIES TIMEOUT 240 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
        add_test(NAME pouch_space_root_conversion-controls_${codec}_${sync}
          COMMAND pouch_space_collection_sdk_test ${CMAKE_CURRENT_BINARY_DIR} root-conversion-controls ${codec} ${sync})
        set_tests_properties(pouch_space_root_conversion-controls_${codec}_${sync} PROPERTIES TIMEOUT 1200 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
      endif()
      foreach(mode maintenance maintenance-io maintenance-allocation background background-io background-allocation)
        add_test(NAME pouch_space_collection_${mode}_${codec}_${sync}
          COMMAND pouch_space_collection_sdk_test ${CMAKE_CURRENT_BINARY_DIR} ${mode} ${codec} ${sync})
        set_tests_properties(pouch_space_collection_${mode}_${codec}_${sync} PROPERTIES
          TIMEOUT 1200 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
      endforeach()
      foreach(mode query query-io query-allocation)
        if((codec EQUAL 1 OR codec EQUAL 2) AND NOT mode STREQUAL "query")
          continue()
        endif()
        add_test(NAME pouch_space_collection_${mode}_${codec}_${sync}
          COMMAND pouch_space_collection_sdk_test ${CMAKE_CURRENT_BINARY_DIR}
            ${mode} ${codec} ${sync})
        set_tests_properties(pouch_space_collection_${mode}_${codec}_${sync}
          PROPERTIES TIMEOUT 1200 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
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
  foreach(mode lifecycle phases policy protection fairness concurrency coordinated capacity-completion capacity-required attachment-windows)
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
  foreach(mode lifecycle validation faults io-faults namespaces namespace-concurrency namespace-compare retention-codec retention retention-faults retention-io-faults retention-capacity retention-capacity-faults retention-progress retention-concurrency retention-compare retention-discovery-compare guards guard-concurrency guard-faults guard-public guard-compare outbox-markers outbox-notifications outbox-marker-io-faults outbox-marker-allocation-faults outbox-marker-crash outbox-marker-concurrency)
    add_test(NAME pouch_space_client_${mode} COMMAND pouch_space_client_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_client_${mode} PROPERTIES TIMEOUT 120 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  set_tests_properties(pouch_space_client_outbox-marker-crash PROPERTIES TIMEOUT 300)
  foreach(codec RANGE 0 3)
    foreach(mode index-artifacts index-open-faults index-durability index-recovery index-io-faults index-allocation-faults index-capacity index-concurrency index-crash)
      if((codec EQUAL 1 OR codec EQUAL 2) AND mode MATCHES "(faults|crash)$")
        continue()
      endif()
      add_test(NAME pouch_space_client_${mode}_${codec}
        COMMAND ${CMAKE_COMMAND} -E env POUCH_INDEX_CODEC=${codec}
          $<TARGET_FILE:pouch_space_client_test> ${CMAKE_CURRENT_BINARY_DIR} ${mode})
      set_tests_properties(pouch_space_client_${mode}_${codec} PROPERTIES
        TIMEOUT 900 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
    endforeach()
    foreach(mode xa-durability xa-io-faults xa-allocation-faults
        xa-managed-durability xa-managed-io-faults xa-managed-allocation-faults
        xa-concurrency xa-scope xa-capacity xa-crash)
      # The canonical codec owners already have their own four-codec fault
      # matrices. Exercise this handoff protocol's full failure sweeps on plain
      # and combined encryption/compression, and its behavior on every codec.
      if((codec EQUAL 1 OR codec EQUAL 2) AND
         mode MATCHES "(io-faults|allocation-faults|crash)$")
        continue()
      endif()
      add_test(NAME pouch_space_client_${mode}_${codec}
        COMMAND ${CMAKE_COMMAND} -E env POUCH_XA_CODEC=${codec}
          $<TARGET_FILE:pouch_space_client_test> ${CMAKE_CURRENT_BINARY_DIR} ${mode})
      set_tests_properties(pouch_space_client_${mode}_${codec} PROPERTIES
        TIMEOUT 900 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
    endforeach()
  endforeach()
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
  foreach(mode lifecycle body-select faults concurrency io-faults crypto-init durable-scope durable-scope-faults durable-scope-crash validation mutations mutation-faults mutation-io-faults retention-hint mutation-concurrency mutation-capacity key-bounds reads ordered-reads read-faults read-io-faults read-concurrency snapshots projection projection-faults projection-concurrency sequence-recovery lifetimes lifetime-faults staging staging-faults staging-recovery staging-recovery-faults staging-concurrency staging-crash maintenance maintenance-abandoned maintenance-faults maintenance-io-faults maintenance-concurrency maintenance-piece-concurrency maintenance-tail maintenance-checksums)
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
  foreach(mode lifecycle faults concurrency validation reservation publication bodies recode recode-faults recode-capacity body-select body-select-faults body-select-concurrency compound-recode compound-recode-faults compound-recode-concurrency conversion conversion-faults conversion-guards body-budget body-budget-faults append-budget append-budget-faults batch-admission batch-faults budget-transfer)
    add_test(NAME pouch_space_namespace_${mode}
      COMMAND pouch_space_namespace_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_namespace_${mode} PROPERTIES TIMEOUT 120
      LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  set_tests_properties(pouch_space_namespace_conversion-faults pouch_space_namespace_compound-recode-faults pouch_space_namespace_body-budget-faults pouch_space_namespace_append-budget-faults pouch_space_namespace_batch-faults PROPERTIES TIMEOUT 600)
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
  add_test(NAME pouch_space_checkpoint-idempotence
    COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} checkpoint-idempotence)
  set_tests_properties(pouch_space_checkpoint-idempotence PROPERTIES TIMEOUT 60
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  foreach(mode native-metadata native-metadata-faults native-metadata-concurrency)
    add_test(NAME pouch_space_${mode} COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_${mode} PROPERTIES TIMEOUT 240 LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  foreach(mode revision revision-faults revision-concurrency revision-process revision-values checkpoint-records checkpoint-owner checkpoint-ranges checkpoint-namespace checkpoint-faults checkpoint-large-group checkpoint-history stream-pieces stream-piece-repair owned-repack stream-repack owned-bitmap owned-page-packing namespace-repack namespace-repack-concurrency namespace-repack-churn namespace-tail namespace-tail-faults namespace-tail-concurrency namespace-abandoned namespace-abandoned-empty namespace-abandoned-faults namespace-abandoned-concurrency standing-prefix standing-owners standing-capacity standing-concurrency standing-deletion root-obligations root-obligation-faults owned-repack-faults)
    add_test(NAME pouch_space_${mode} COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_${mode} PROPERTIES TIMEOUT 240
      LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  set_tests_properties(pouch_space_checkpoint-faults PROPERTIES TIMEOUT 600)
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
