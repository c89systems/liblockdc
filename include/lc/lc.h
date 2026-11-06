#ifndef LC_LOCKDC_H
#define LC_LOCKDC_H

/**
 * @file lc/lc.h
 * @brief Public C API for remote lockd and local Pouch clients.
 *
 * The API is C89-compatible, supports C++98-or-later consumers through C
 * linkage, and uses receiver-style handles for normal operations. Initialize
 * transparent request/config structs with their
 * matching `*_init()` function, then call methods such as
 * `client->acquire(client, ...)` and `lease->update(lease, ...)`.
 *
 * Unless a field says otherwise, input strings, arrays, sources, sinks, and
 * callback contexts are borrowed for the duration of the call. Successful
 * operations that return handles or heap-backed response fields transfer
 * ownership to the caller. Close handles with their receiver `close()` method
 * or the matching `lc_*_close()` wrapper, and release response fields with the
 * matching `*_cleanup()` helper. The free-function close wrappers and cleanup
 * helpers accept `NULL`.
 *
 * Fallible functions return `LC_OK` on success and an `LC_ERR_*` status on
 * failure. When supplied, `lc_error` receives actionable diagnostics and must
 * eventually be passed to `lc_error_cleanup()`. Unless documented as a
 * terminal operation, an existing handle remains owned by the caller after a
 * failed method call. `lc_error_set()` copies diagnostic strings supplied by
 * public callbacks into an error they return to the library.
 *
 * APIs described as streaming move data through bounded buffers without
 * materializing the complete value. In-memory sources/sinks and ordinary
 * mapped lonejson fields are materialized by design; lonejson source-backed
 * and spool-backed mappings retain their documented streaming/file-backed
 * behavior.
 */

#include <lc/version.h>
#include <lonejson.h>
#include <pslog.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * `lc_shared` is compiled with hidden visibility so implementation helpers do
 * not become an accidental dynamic ABI.  Keep declarations in this installed
 * header explicitly default-visible; their matching definitions retain that
 * visibility on GCC- and Clang-family C89 toolchains.  Restore the consumer's
 * visibility state at the end of the header.
 */
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(default)
#endif

/** Default maximum bytes accepted while parsing a typed JSON HTTP response. */
#define LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT (100UL * 1024UL * 1024UL)

/** Maximum combined byte length of durable outbox envelope fields. Payloads are
 * streamed attachments and are not included in this bound. */
#define LC_OUTBOX_MAX_ENVELOPE_BYTES (1024UL * 1024UL)

/** Maximum serialized byte length reserved for a durable command or inbox
 * receipt. Input identity fields are conservatively bounded below this value
 * to account for JSON escaping and fixed record fields; payload and result
 * bodies are streamed attachments and are not included. */
#define LC_OUTBOX_MAX_RECEIPT_BYTES (1024UL * 1024UL)

/** Opaque client handle. */
typedef struct lc_client lc_client;
/** Opaque lease/state handle returned from acquire and message state flows. */
typedef struct lc_lease lc_lease;
/** Opaque queue message handle returned from dequeue and subscription flows. */
typedef struct lc_message lc_message;
/** Opaque managed queue consumer service. */
typedef struct lc_consumer_service lc_consumer_service;
/** Opaque durable Pouch history-retention consumer. */
typedef struct lc_history_consumer lc_history_consumer;
/** Ordered owner of immutable JSON state objects. */
typedef struct lc_collection lc_collection;
/** Owned incremental JSON append; commit/abort do not replace the required
 * close.
 */
typedef struct lc_collection_writer lc_collection_writer;
/** Opaque transactional outbox handle. */
typedef struct lc_outbox lc_outbox;
/** Opaque process-local outbox dispatcher. */
typedef struct lc_outbox_dispatcher lc_outbox_dispatcher;
/** Opaque outbox transaction handle. */
typedef struct lc_outbox_transaction lc_outbox_transaction;
/** Restricted non-terminal lease view owned by an outbox transaction. */
typedef struct lc_outbox_participant lc_outbox_participant;
/** Owned, claimed outbox delivery returned from a dispatcher. */
typedef struct lc_outbox_job lc_outbox_job;
/** Opaque byte source used for uploads and streamed request bodies. */
typedef struct lc_source lc_source;
/** Opaque byte sink used for downloads and streamed response bodies. */
typedef struct lc_sink lc_sink;

/** API-visible monotonic object version. Mirrors lockd's signed int64 value. */
typedef int64_t lc_version;
/**
 * Unix timestamp in seconds. Mirrors lockd's signed int64 value.
 *
 * Pouch and transport response paths preserve this range, including streamed
 * attachment metadata headers.
 */
typedef int64_t lc_unix_seconds;
/** Query/index sequence. Mirrors lockd's unsigned uint64 value. */
typedef uint64_t lc_index_seq;
/**
 * Selects the namespace's durable current sequence when creating a history
 * consumer. This value is only meaningful in
 * `lc_history_consumer_config.initial_acknowledged_index_seq`.
 */
#define LC_HISTORY_CONSUMER_START_AT_CURRENT ((lc_index_seq) - 1)
/** Transaction-coordinator leader term. Mirrors lockd's unsigned uint64 value.
 */
typedef uint64_t lc_tc_term;

/** Backend support advertised by this state observation. UNKNOWN is not proof
 * that a requested policy was applied: older remote servers may ignore it.
 * A known expiry value of zero confirms no expiry; a positive duration staged
 * in a transaction has no final deadline until commit. */
typedef enum lc_state_retention_support {
  /** No authoritative support observation; a server may have ignored the
   * requested policy.
   */
  LC_STATE_RETENTION_UNKNOWN = 0,
  /** Backend explicitly supports state-retention metadata. */
  LC_STATE_RETENTION_SUPPORTED = 1,
  /** Backend explicitly reports that state retention is unsupported. */
  LC_STATE_RETENTION_UNSUPPORTED = 2
} lc_state_retention_support;

/** Private snapshot fully captured in a temporary file before the handler.
 * The reader is file-backed, rather than a live producer-to-handler stream.
 * Its fields and reader are borrowed only for the handler invocation. */
typedef struct lc_state_snapshot {
  /** File-backed private state reader. NULL when has_state is zero. */
  lc_source *reader;
  /** Non-zero when the lease's private view contains a state body. */
  int has_state;
  /** State content type returned by lockd. */
  const char *content_type;
  /** State entity tag returned by lockd. */
  const char *etag;
  /** State version returned by lockd. */
  lc_version version;
  /** Current fencing token returned by lockd. */
  long fencing_token;
  /** Correlation id for the snapshot read. */
  const char *correlation_id;
  /** Backend support for per-object state retention. */
  lc_state_retention_support retention_support;
  /** Non-zero when the returned body has a known retention deadline. */
  int has_retention_expires_at_unix;
  /** Expiry eligibility boundary; zero confirms no expiry. Deletion is
   * deferred. */
  lc_unix_seconds retention_expires_at_unix;
} lc_state_snapshot;

/** Context passed to `lc_acquire_for_update_handler_fn`. */
typedef struct lc_acquire_for_update_context {
  /**
   * Active lease handle. Borrowed for the duration of the callback; do not
   * close or release it from the handler because the helper releases it.
   */
  lc_lease *lease;
  /** Private state snapshot fetched before the callback. */
  lc_state_snapshot state;
} lc_acquire_for_update_context;

/**
 * Structured error returned by all public operations.
 *
 * The library allocates any string members it populates. Release them with
 * `lc_error_cleanup()` when the error is no longer needed.
 */
typedef struct lc_error {
  /** Public `LC_*` status code for the failed operation. */
  int code;
  /** HTTP status from the remote server, or zero for local/Pouch failures. */
  long http_status;
  /** Human-readable summary. Owned by the error object. */
  char *message;
  /** Optional longer diagnostic detail. Owned by the error object. */
  char *detail;
  /** Optional stable backend reason code, including local Pouch reasons.
   * Owned by the error object. */
  char *server_code;
  /** Optional request correlation id. Owned by the error object. */
  char *correlation_id;
} lc_error;

/** Exact printable size of an rs/xid-compatible identifier, excluding NUL. */
#define LC_XID_STRING_LENGTH 20U
/** Buffer size required by `lc_xid_new()`, including the terminating NUL. */
#define LC_XID_STRING_SIZE (LC_XID_STRING_LENGTH + 1U)

/**
 * Callback invoked while an acquire-for-update lease is held.
 *
 * `update` is borrowed and valid only for this invocation. Return `LC_OK` to
 * commit staged work or a non-`LC_OK` status to request rollback. Populate
 * `error` with the failure cause when returning an error status.
 */
typedef int (*lc_acquire_for_update_handler_fn)(
    void *context, lc_acquire_for_update_context *update, lc_error *error);

/**
 * Callback used by `lc_source_from_callbacks()` to fill `buffer`.
 *
 * Write at most `count` bytes and return the number written. Short reads are
 * allowed. Return zero only for end-of-stream or failure; distinguish failure
 * by setting `error->code` to a non-`LC_OK` value. `buffer` is borrowed for the
 * call and must not be retained.
 */
typedef size_t (*lc_source_read_fn)(void *context, void *buffer, size_t count,
                                    lc_error *error);
/**
 * Optional callback used by `lc_source_from_callbacks()` to rewind a source.
 *
 * Return `LC_OK` after resetting the next read to the beginning, or a
 * non-`LC_OK` status with `error` populated. Omitting this callback makes the
 * source single-pass.
 */
typedef int (*lc_source_reset_fn)(void *context, lc_error *error);
/**
 * Optional callback invoked exactly once when a callback source is closed.
 *
 * Use it to release resources owned by `context`; it cannot report an error.
 */
typedef void (*lc_source_close_fn)(void *context);

/** Custom allocation hook for malloc-style allocation. */
typedef void *(*lc_malloc_fn)(void *context, size_t size);
/** Custom allocation hook for realloc-style allocation. */
typedef void *(*lc_realloc_fn)(void *context, void *ptr, size_t size);
/** Custom allocation hook for freeing memory returned by the allocator hooks.
 */
typedef void (*lc_free_fn)(void *context, void *ptr);

/**
 * Allocator override used by the client and all derived handles.
 *
 * Leave zeroed to use the process-wide built-in allocator. Custom malloc/free
 * hooks must be provided together; realloc is optional.
 * Custom hooks bypass the optional built-in allocation tracker. The caller
 * remains responsible for diagnostics and the lifetime of its allocator
 * context.
 */
typedef struct lc_allocator {
  /** Allocates `size` bytes, or returns `NULL` on failure. */
  lc_malloc_fn malloc_fn;
  /** Resizes `ptr` to `size` bytes using the same allocation domain. */
  lc_realloc_fn realloc_fn;
  /** Releases memory returned by `malloc_fn` or `realloc_fn`. */
  lc_free_fn free_fn;
  /** Opaque caller context passed to every allocator hook. */
  void *context;
} lc_allocator;

/** One query option used to construct a Pouch endpoint. */
typedef struct lc_pouch_endpoint_option {
  /** Decoded option name. It must be non-empty. */
  const char *name;
  /** Decoded option value, or `NULL` to emit a bare `?name` option. */
  const char *value;
} lc_pouch_endpoint_option;

/** Bit in `lc_pouch_settings.set_mask` selecting `single_writer`. */
#define LC_POUCH_SETTING_SINGLE_WRITER ((uint64_t)1)
/** Bit in `lc_pouch_settings.set_mask` selecting `durable_sync`. */
#define LC_POUCH_SETTING_DURABLE_SYNC ((uint64_t)1 << 1)
/** Bit in `lc_pouch_settings.set_mask` selecting `fsync_batch_max_ops`. */
#define LC_POUCH_SETTING_FSYNC_BATCH_MAX_OPS ((uint64_t)1 << 2)
/** Bit in `lc_pouch_settings.set_mask` selecting `segment_target_bytes`. */
#define LC_POUCH_SETTING_SEGMENT_TARGET_BYTES ((uint64_t)1 << 3)
/** Bit in `lc_pouch_settings.set_mask` selecting `indexer_flush_docs`. */
#define LC_POUCH_SETTING_INDEXER_FLUSH_DOCS ((uint64_t)1 << 4)
/** Bit selecting `indexer_flush_interval_seconds`. */
#define LC_POUCH_SETTING_INDEXER_FLUSH_INTERVAL_SECONDS ((uint64_t)1 << 5)
/** Bit selecting `background_compaction_enabled`. */
#define LC_POUCH_SETTING_BACKGROUND_COMPACTION ((uint64_t)1 << 6)
/** Bit selecting `compaction_throttling_disabled`. */
#define LC_POUCH_SETTING_DISABLE_COMPACTION_THROTTLING ((uint64_t)1 << 7)
/** Bit selecting `terminal_reclaim_min_bytes`. */
#define LC_POUCH_SETTING_TERMINAL_RECLAIM_MIN_BYTES ((uint64_t)1 << 8)
/** Bit selecting `queue_watch`. */
#define LC_POUCH_SETTING_QUEUE_WATCH ((uint64_t)1 << 9)
/** Bit selecting `query_engine`. */
#define LC_POUCH_SETTING_QUERY_ENGINE ((uint64_t)1 << 10)
/** Bit selecting `query_fallback_engine`. */
#define LC_POUCH_SETTING_QUERY_FALLBACK_ENGINE ((uint64_t)1 << 11)
/** Bit selecting `query_indexing_enabled`. */
#define LC_POUCH_SETTING_QUERY_INDEXING ((uint64_t)1 << 12)
/** Bit selecting `crypto_key`. */
#define LC_POUCH_SETTING_CRYPTO_KEY ((uint64_t)1 << 13)
/** Bit selecting `crypto_key_file`. */
#define LC_POUCH_SETTING_CRYPTO_KEY_FILE ((uint64_t)1 << 14)
/** Bit selecting `crypto_generate_key_file`. */
#define LC_POUCH_SETTING_CRYPTO_GENERATE_KEY_FILE ((uint64_t)1 << 15)
/** Bit selecting `compression`. */
#define LC_POUCH_SETTING_COMPRESSION ((uint64_t)1 << 16)

/**
 * Optional typed local-Pouch root-open settings.
 *
 * Initialize with `lc_pouch_settings_init()`, set a field's matching
 * `LC_POUCH_SETTING_*` bit, then attach the object to
 * `lc_client_config.pouch_settings`. The object and its strings are borrowed
 * only for `lc_client_open()`. A nonzero mask requires exactly one local
 * `pouch://` endpoint; remote and Unix-socket clients reject it rather than
 * silently ignoring root-local policy. String fields selected by the mask
 * must be non-NULL and non-empty. Boolean fields must be zero or one.
 *
 * A selected field overrides the corresponding legacy endpoint query option.
 * Existing dedicated `lc_client_config` crypto/compression fields remain
 * supported and take precedence over endpoint options, while this object has
 * final precedence. These settings are distinct from per-query engine hints
 * and durable namespace configuration.
 */
typedef struct lc_pouch_settings {
  /** Bitwise OR of the `LC_POUCH_SETTING_*` fields selected below. */
  uint64_t set_mask;
  /** Exclusive root writer by default; zero selects shared-root writers. */
  int single_writer;
  /** Nonzero waits for root group-sync at public mutation boundaries.
   * Default zero is NoSync. Recovery/retirement barriers can still force sync.
   */
  int durable_sync;
  /** Maximum durable group-commit requests; zero leaves it unbounded.
   * Ready requests dispatch without a fill delay. The root queue is shared
   * among handles in this process; separate processes have separate queues. */
  uint64_t fsync_batch_max_ops;
  /** Target immutable segment size; zero uses the Pouch default. */
  uint64_t segment_target_bytes;
  /** Pending index documents before an index flush; zero uses the default. */
  uint64_t indexer_flush_docs;
  /** Index flush interval in seconds; zero uses the default. */
  uint64_t indexer_flush_interval_seconds;
  /** Default enabled: root compaction, state/collection expiry and partition
   * cleanup. Outbox receipt/dead-letter reclamation uses its dispatcher. */
  int background_compaction_enabled;
  /** Disables the background compaction I/O throttle. */
  int compaction_throttling_disabled;
  /** Active-segment threshold for terminal reclaim; zero uses the default. */
  uint64_t terminal_reclaim_min_bytes;
  /** Requests filesystem-backed queue notifications where supported. */
  int queue_watch;
  /** Default query engine: `"index"` or `"scan"`. */
  const char *query_engine;
  /** Fallback query engine: `"index"` or `"scan"`. */
  const char *query_fallback_engine;
  /** Enables or disables local derived query-index maintenance. */
  int query_indexing_enabled;
  /** Pouch root encryption key string. Treat it as secret material. */
  const char *crypto_key;
  /** Pouch root encryption key-file path. */
  const char *crypto_key_file;
  /** Creates a missing selected Pouch key file. */
  int crypto_generate_key_file;
  /** At-rest compression: `"none"` or `"zlib"`. Omission inherits an existing
   * root's mode; new roots default to none. A differing explicit mode converts
   * the entire root synchronously with exclusive ownership. If other writers
   * prevent conversion, open warns and uses the stored mode. Conversion errors
   * fail the open. Conversion replaces one canonical file at a time, with
   * temporary payload space for that file rather than a second root. Any
   * interrupted conversion resumes on the next open, even with omission. */
  const char *compression;
} lc_pouch_settings;

/** Zeros a typed local-Pouch root-open settings object. */
void lc_pouch_settings_init(lc_pouch_settings *settings);

/**
 * Client construction settings.
 *
 * Use `lc_client_config_init()` before overriding individual fields.
 * Set either `endpoints` for TCP/TLS operation, one `pouch://` endpoint for
 * local Pouch storage, or `unix_socket_path` for Unix-domain-socket transport.
 *
 * Pouch endpoints use an absolute filesystem root path:
 * `pouch:///var/lib/app/lockd-root`. Local Pouch storage defaults to exclusive
 * single-writer mode. A second default writer for the same root fails at open;
 * callers that intentionally need multiple active local writers must opt in
 * with `LC_POUCH_SETTING_SINGLE_WRITER` set to zero in `pouch_settings`, or
 * through the legacy `?single_writer=false` endpoint option.
 *
 * Endpoint query options are copied at open and use the same C-native Pouch
 * storage engine as direct Pouch callers. Supported options include
 * `compression`, `crypto_key`, `crypto_key_file`, `crypto_generate_key_file`,
 * `durable_sync`, `fsync_batch_max_ops`, `segment_target_bytes`,
 * `indexer_flush_docs`, `indexer_flush_interval_seconds`,
 * `background_compaction`, `disable_compaction_throttling`,
 * `terminal_reclaim_min_bytes`, `queue_watch`,
 * `query_engine`, `query_fallback_engine`, and `query_indexing`.
 * Set `query_indexing=false` for roots that never execute indexed queries;
 * ordinary queries then use scans and explicit indexed queries are rejected.
 * New code should prefer the typed `pouch_settings` field below. Endpoint
 * options remain a compatibility input and are overridden per field by typed
 * settings.
 */
typedef struct lc_client_config {
  /**
   * Endpoint URLs tried in order for TCP/TLS transport.
   *
   * For Pouch, provide exactly one `pouch://` endpoint. Pouch does not fail
   * over across multiple roots through this field.
   */
  const char *const *endpoints;
  /** Number of entries in `endpoints`. */
  size_t endpoint_count;
  /** Unix-domain socket path used instead of `endpoints` when set. */
  const char *unix_socket_path;
  /** Streamed combined CA/client-cert/private-key PEM bundle.
   *
   * Preferred over `client_bundle_path`. The source is borrowed and consumed
   * during `lc_client_open()`; callers remain responsible for closing it.
   */
  lc_source *client_bundle_source;
  /** Deprecated compatibility path to the combined PEM bundle. */
  const char *client_bundle_path;
  /** Namespace used when a request leaves `ns` unset. */
  const char *default_namespace;
  /**
   * Whole-request timeout in milliseconds.
   *
   * Remote requests use this as the transport timeout. Local Pouch operations
   * remain local filesystem calls; blocking lease/dequeue behavior is
   * controlled by the corresponding request fields.
   */
  long timeout_ms;
  /** Disables mTLS client authentication when the server allows it. */
  int disable_mtls;
  /** Skips peer verification. Intended for local dev and test only. */
  int insecure_skip_verify;
  /** Prefers HTTP/2 when the endpoint and libcurl build support it. */
  int prefer_http_2;
  /** Maximum typed JSON response bytes parsed through lonejson for operations
   * issued through this client. Zero uses
   * `LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT`. A remote outbox owns a separate
   * dispatcher client with the standard bounded limit and rejects envelopes
   * above `LC_OUTBOX_MAX_ENVELOPE_BYTES`, so a lower application-facing
   * limit cannot strand its library-owned durable envelopes.
   */
  size_t http_json_response_limit_bytes;
  /**
   * Borrowed client logger used for SDK diagnostics. `NULL` selects a no-op
   * logger. The caller owns the base logger and its output context; keep them
   * alive until the client and all derived handles, consumer services, and
   * dispatchers have closed. SDK worker threads may emit concurrently, so
   * custom output callbacks must be thread-safe and callable from native
   * threads.
   */
  pslog_logger *logger;
  /** Disables the automatic `sys=client.lockd` logger field derived by the
   * SDK when non-zero. */
  int disable_logger_sys_field;
  /** Custom allocator hooks inherited by all derived handles and buffers. */
  lc_allocator allocator;
  /**
   * Pouch root key string for encrypted local storage.
   *
   * Prefer key files for long-lived process config. This string is copied as
   * secret material and wiped before the client releases it. It may also be
   * supplied as the `crypto_key` endpoint option.
   */
  const char *pouch_crypto_key;
  /**
   * Pouch root key file path for encrypted local storage.
   *
   * The file stores an `lc-pouch-key-v1:<base64url>` root key. It may also be
   * supplied as the `crypto_key_file` endpoint option.
   */
  const char *pouch_crypto_key_file;
  /**
   * Creates `pouch_crypto_key_file` with a new root key when it is missing.
   *
   * Generation writes a mode-0600 file below a current-user-owned mode-0700
   * parent directory, rejecting unsafe existing parent directories.
   */
  int pouch_crypto_generate_key_file;
  /** Local Pouch at-rest compression: NULL inherits an existing root's mode
   * (none for new roots). Explicit "none"/"zlib" requests whole-root conversion
   * if different, before open returns. Other writers prevent conversion and
   * the open warns and adopts the stored mode. Conversion errors fail the open.
   * Conversion uses one canonical-file replacement plus recovery metadata;
   * interrupted work resumes before clients are admitted, even with NULL.
   * Consumer service workers retain the mode actually opened.
   */
  const char *pouch_compression;
  /** Marks `pouch_crypto_generate_key_file` as explicitly configured.
   *
   * Leave zero for legacy endpoint-option precedence. Set non-zero when an
   * explicit false value must override `crypto_generate_key_file=true`
   * in a Pouch endpoint.
   */
  int pouch_crypto_generate_key_file_set;
  /**
   * Optional typed local-Pouch root-open policy.
   *
   * This object is borrowed for `lc_client_open()` and may be released when
   * that call returns. A nonzero `set_mask` is valid only for exactly one
   * `pouch://` endpoint. It overrides legacy Pouch endpoint options and the
   * dedicated compatibility crypto/compression fields above.
   */
  const lc_pouch_settings *pouch_settings;
} lc_client_config;

/** Public status codes returned by all API entry points. */
enum {
  /** Operation completed successfully. */
  LC_OK = 0,
  /** A required argument, value, state, or precondition is invalid. */
  LC_ERR_INVALID = 1,
  /** Allocation failed. */
  LC_ERR_NOMEM = 2,
  /** Transport, filesystem, or other I/O failed. */
  LC_ERR_TRANSPORT = 3,
  /** A remote response or durable local value violated its protocol. */
  LC_ERR_PROTOCOL = 4,
  /** The backend rejected the request, including local Pouch conflicts or
   * missing records. Inspect http_status and server_code for the reason. */
  LC_ERR_SERVER = 5,
  /** An operation or local wait reached its deadline before completion. */
  LC_ERR_TIMEOUT = 6,
  /** Work is incomplete or temporarily blocked. Inspect operation done/wait
   * information for asynchronous progress. For a durable transaction decision,
   * preserve identity/decision and recover rather than replay application work.
   */
  LC_ERR_PENDING = 7,
  /** The backend or selected execution mode does not support this feature. */
  LC_ERR_UNSUPPORTED = 8
};

/**
 * Input stream used for uploads and generic byte transport. A source may be
 * single-pass; in that case `reset()` returns `LC_ERR_INVALID`.
 *
 * The source owns `impl` and releases it from `close()`.
 */
struct lc_source {
  /** Reads up to `count` bytes into `buffer`; returns zero at EOF. */
  size_t (*read)(lc_source *self, void *buffer, size_t count, lc_error *error);
  /** Rewinds the source when supported; otherwise returns `LC_ERR_INVALID`. */
  int (*reset)(lc_source *self, lc_error *error);
  /** Releases the source and any owned backing resources. */
  void (*close)(lc_source *self);
  /** Private implementation pointer owned by the source. */
  void *impl;
};

/**
 * Output stream used for downloads and generic byte transport.
 *
 * The sink owns `impl` and releases it from `close()`.
 */
struct lc_sink {
  /**
   * Writes exactly `count` bytes.
   *
   * Returns non-zero on success or zero with `error` populated on failure.
   * `bytes` is borrowed for the call and must not be retained.
   */
  int (*write)(lc_sink *self, const void *bytes, size_t count, lc_error *error);
  /** Releases the sink and any owned backing resources. */
  void (*close)(lc_sink *self);
  /** Private implementation pointer owned by the sink. */
  void *impl;
};

/** Collection-wide ownership for many independently committed appends. owner
 * is a nonempty UTF-8 label of at most 128 bytes, not shared credentials. TTL
 * must be positive; acquisition never waits. private_read defaults to zero:
 * committed entries remain public. Selecting it hides payload reads from other
 * receivers until release/expiry. */
typedef struct lc_collection_lease_req {
  /** Diagnostic label, never an acquisition or sharing credential. */
  const char *owner;
  /** Positive lifetime measured from acquisition/renewal time. */
  long ttl_seconds;
  /** Nonzero admits payload reads only by the live owner. */
  int private_read;
} lc_collection_lease_req;

/** Bounded ownership observation, copied by value. active is evaluated at the
 * observation time. An expired record retains its fencing high-water mark.
 * Observing these fields does not credential a receiver. */
typedef struct lc_collection_lease_info {
  /** Nonzero only while the observed lease has not expired. */
  int active;
  /** Recorded application owner label; observation alone grants no authority.
   */
  char owner[129];
  /** Observation/precondition for takeover, not a receiver credential. */
  char lease_id[LC_XID_STRING_SIZE];
  /** Monotonic ownership epoch; retained after release and never reused. */
  uint64_t fencing_token;
  /** Absolute Unix seconds; zero means no recorded lease. */
  lc_unix_seconds expires_at_unix;
  /** Nonzero when the recorded owner requested private payload reads. */
  int private_read;
} lc_collection_lease_info;

/** Collection creation/open options. Names are nonempty UTF-8, at most 255
 * bytes. An absent namespace selects the client's default namespace.
 * Collection expiry drops the owner and all remaining entries; it does not
 * assign a maximum age to each entry. New collections default to no expiry. */
typedef struct lc_collection_config {
  /** Namespace; NULL selects the client's default on input. */
  const char *ns;
  /** Collection name within its namespace. */
  const char *name;
  /** Creation-only retention metadata. Omission creates without expiry.
   * Opening with this flag selected is invalid. */
  int has_retention_seconds;
  /** Seconds from creation commit; zero disables expiry. */
  long retention_seconds;
  /** Creation-only live entry limit. Omission/zero is unlimited. Opening with
   * this flag selected is invalid. Maintenance lazily removes oldest entries;
   * appends may temporarily exceed the limit. */
  int has_max_entries;
  /** Creation-only live-entry limit; zero is unlimited. */
  uint64_t max_entries;
  /** Optional creation-only lease, established before publishing the name.
   * The returned receiver is credentialed; borrowed only during creation. */
  const lc_collection_lease_req *lease;
} lc_collection_config;

/** Collection identity and current counts. Strings are borrowed from the
 * collection receiver, or for the duration of a list callback. Counts describe
 * one committed observation and can change after the call returns. */
typedef struct lc_collection_info {
  /** Namespace; NULL selects the client's default on input. */
  const char *ns;
  /** Collection name within its namespace. */
  const char *name;
  /** Immutable collection-incarnation identifier. */
  const char *collection_id;
  /** Permanent position high-water mark, including gaps and deleted entries. */
  uint64_t upper_position;
  /** Live-entry count at the captured observation, before deferred
   * maintenance.
   */
  uint64_t live_count;
  /** Nonzero when payload query indexing is enabled for this collection. */
  int query_indexing;
  /** Backend support for per-object retention metadata. */
  lc_state_retention_support retention_support;
  /** Nonzero when the expiry observation is known, including a zero deadline.
   */
  int has_retention_expires_at_unix;
  /** Collection's own expiry; lazy removal drops all its entries as well. */
  lc_unix_seconds retention_expires_at_unix;
  /** Configured live entry limit, zero for unlimited. This is not a disk cap.
   */
  uint64_t max_entries;
  /** Copied ownership observation; does not credential the observing
   * receiver.
   */
  lc_collection_lease_info lease;
} lc_collection_info;

/** Endpoint-local capabilities, queried without a network request. Pouch
 * supports LQL, both cursors, ranges, first-N, count retention, append writers,
 * explicit staged pull, atomic parent renewal, collection leases and atomic
 * edits. Last-N and position selection are currently false. Remote clients
 * currently return all fields as zero. */
typedef struct lc_collection_capabilities {
  /** Supports collection lifecycle and independently atomic append/read
   * operations.
   */
  int collections;
  /** Supports LQL predicates on ordered collection reads. */
  int lql_filter;
  /** Supports resuming an incomplete read from an accepted boundary. */
  int resume;
  /** Supports starting a new read window from a completed checkpoint. */
  int checkpoint;
  /** Supports after-exclusive and through-inclusive position bounds. */
  int ranges;
  /** Supports a live-input limit applied before LQL filtering. */
  int first_n;
  /** Reserved capability; last-N selection is currently unsupported. */
  int last_n;
  /** Reserved capability; explicit position sets are currently unsupported. */
  int position_selection;
  /** Supports per-collection max_entries and lazy oldest-first trimming. */
  int count_retention;
  /** Supports owned push-fed append writers with bounded canonical chunks. */
  int append_writer;
  /** Supports explicit single-pass sources through that same staged writer. */
  int append_staged;
  /** Supports atomic parent retention directives on all append forms. */
  int append_collection_retention;
  /** Supports optional collection-wide writer ownership. */
  int leases;
  /** Supports conditional replacement of the observed live owner. */
  int lease_takeover;
  /** Supports private payload visibility during ownership. */
  int private_reads;
  /** Atomic range edits, retry receipts, recovery heads and read preconditions.
   */
  int atomic_edit;
} lc_collection_capabilities;

/** Required retry identity, scoped to a collection incarnation. Supply 1..128
 * ASCII letters, digits, '-', '_', '.', or '~'; retain it before uploading.
 * Append accepts exactly one JSON object and compacts structural whitespace
 * incrementally. Retry identity compares the compact bytes, not JSON meaning.
 * Appends commit independently and do not participate in implicit/managed XA.
 */
typedef struct lc_collection_append_req {
  /** Caller-supplied retry identity, scoped to this collection incarnation. */
  const char *append_id;
  /** Nonzero selects explicit retention for this new entry. */
  int has_retention_seconds;
  /** Entry retention from commit. Omission/zero means no expiry for a new
   * entry. An identical retry preserves the existing deadline. */
  long retention_seconds;
  /** Optional parent directive, applied atomically only for a new append.
   * Omission preserves, zero clears, positive seconds reset from this commit.
   * Duplicates and aborted/failed-before-publication appends never renew it.
   * A commit error after publication is uncertain; reconcile through status. */
  int has_collection_retention_seconds;
  /** Parent lifetime from the new append's commit; zero clears parent expiry.
   */
  long collection_retention_seconds;
} lc_collection_append_req;

/** Retention metadata. Omission preserves, zero clears, positive
 * seconds replace the deadline from this metadata commit. Expired objects
 * remain ordinary objects until maintenance deletes them. max_entries applies
 * only to the collection owner; selecting it for entry_metadata is invalid. */
typedef struct lc_collection_metadata_req {
  /** Nonzero selects the retention_seconds metadata update. */
  int has_retention_seconds;
  /** Zero clears expiry; positive seconds reset it from this metadata commit.
   */
  long retention_seconds;
  /** Nonzero selects max_entries; invalid for entry-only metadata updates. */
  int has_max_entries;
  /** Zero disables count retention; positive values retain newest live entries
   * by permanent position, independently of age retention and LQL filtering.
   * Updates apply to existing and future entries; deletion is eventual. */
  uint64_t max_entries;
} lc_collection_metadata_req;

/** One append outcome. Fixed-size metadata needs no cleanup. etag is the
 * lowercase SHA-256 of the compact stored JSON, excluding the output LF.
 * A deleted duplicate returns its original identity without recreating it. */
typedef struct lc_collection_append_res {
  /** Permanent collection position; deletion never renumbers other entries. */
  uint64_t position;
  /** Compact plaintext JSON byte count, excluding the output LF. */
  uint64_t bytes;
  /** Fingerprint of the compact stored JSON bytes. */
  char etag[65];
  /** Nonzero when returning the original outcome of an identical committed
   * retry.
   */
  int duplicate;
  /** Nonzero when the original published entry has since been deleted. */
  int deleted;
  /** Backend support for per-object retention metadata. */
  lc_state_retention_support retention_support;
  /** A deleted binding has no current state deadline. */
  int has_retention_expires_at_unix;
  /** Unix-second expiry eligibility boundary; zero means no expiry. Cleanup
   * is deferred.
   */
  lc_unix_seconds retention_expires_at_unix;
  /** Original append's selected parent deadline, also retained for retries.
   * It is not the parent's current deadline; use collection.describe for that.
   * Absent when the original append selected no parent directive. */
  int has_collection_retention_expires_at_unix;
  /** Observed parent deadline; zero means no parent expiry when the
   * corresponding flag is set.
   */
  lc_unix_seconds collection_retention_expires_at_unix;
} lc_collection_append_res;

/** Published append-ID status; absence does not cancel an in-flight producer.
 */
typedef enum lc_collection_append_state {
  /** No committed identity was observed; an upload may still be in flight. */
  LC_COLLECTION_APPEND_ABSENT = 0,
  /** The append identity has an authoritative committed entry. */
  LC_COLLECTION_APPEND_COMMITTED = 1,
  /** The original entry was deleted; its retry identity remains reserved. */
  LC_COLLECTION_APPEND_DELETED = 2
} lc_collection_append_state;

/** An absent observation neither cancels nor resolves an upload still running.
 * entry facts are zero when state is ABSENT. */
typedef struct lc_collection_append_status_res {
  /** Absent, committed or deleted observation for the requested append ID. */
  lc_collection_append_state state;
  /** Original entry facts; zeroed when state is LC_COLLECTION_APPEND_ABSENT. */
  lc_collection_append_res entry;
} lc_collection_append_status_res;

/** Maximum disjoint position intervals accepted by one atomic collection edit.
 */
#define LC_COLLECTION_EDIT_MAX_RANGES 64U
/** Maximum UTF-8 recovery descriptor bytes, excluding the terminating NUL. */
#define LC_COLLECTION_RECOVERY_MAX_BYTES 1024U

/** One finite, after-exclusive / through-inclusive pre-edit interval. */
typedef struct lc_collection_edit_range {
  /** Exclusive lower permanent position in the pre-edit view. */
  uint64_t after;
  /** Inclusive upper permanent position; must exceed after. */
  uint64_t through;
} lc_collection_edit_range;

/** Coherent payload-protected observation. Fixed storage needs no cleanup.
 * etag is opaque, incarnation-scoped and may change on canonical recovery.
 * Zero anchor means no required entry and reports available. An unavailable
 * nonzero anchor does not pin or resurrect the referenced entry. */
typedef struct lc_collection_head_res {
  /** Immutable collection-incarnation identifier. */
  char collection_id[LC_XID_STRING_SIZE];
  /** Opaque incarnation-scoped head token for edit/read preconditions. */
  char etag[65];
  /** Permanent position high-water mark, including gaps and deleted entries. */
  uint64_t upper_position;
  /** Live-entry count at the captured observation, before deferred
   * maintenance.
   */
  uint64_t live_count;
  /** Revision of the currently published application recovery descriptor. */
  uint64_t recovery_revision;
  /** Referenced entry position, or zero when no entry is required. */
  uint64_t recovery_anchor;
  /** Nonzero when the anchor is zero or its entry is still live. */
  int recovery_anchor_available;
  /** NUL-terminated opaque UTF-8 application recovery descriptor. */
  char recovery[LC_COLLECTION_RECOVERY_MAX_BYTES + 1U];
} lc_collection_head_res;

/** Bounded options and metadata are copied before input callbacks. Keep
 * original retry metadata separate from result storage. Ranges must be
 * ordered/disjoint. A source requires replacement and vice versa; a new
 * replacement ID must be unused. Omitted recovery preserves; a selected empty
 * string clears it. A selected anchor of UINT64_MAX refers to this edit's
 * replacement. Other nonzero anchors must survive the cut. Recovery is bounded
 * opaque UTF-8 text. Identical committed retries return their original outcome,
 * including after later mutations. Retries revalidate writer ownership after
 * input; only the stale head precondition is bypassed. Errors after publication
 * can be uncertain (use status). */
typedef struct lc_collection_edit_req {
  /** Required durable retry identity; retain the original request until
   * reconciled.
   */
  const char *operation_id;
  /** Required opaque head token captured before preparing this edit. */
  const char *expected_head;
  /** Borrowed ordered, disjoint cuts in the pre-edit position space. */
  const lc_collection_edit_range *ranges;
  /** Number of ranges, at most LC_COLLECTION_EDIT_MAX_RANGES; zero permits
   * append-only edits.
   */
  size_t range_count;
  /** Optional append request paired with the supplied replacement source. */
  const lc_collection_append_req *replacement;
  /** Nonzero replaces the recovery descriptor and anchor; zero preserves
   * both.
   */
  int has_recovery;
  /** Borrowed UTF-8 descriptor, at most LC_COLLECTION_RECOVERY_MAX_BYTES;
   * empty clears it.
   */
  const char *recovery;
  /** Zero requires no entry; UINT64_MAX selects this edit's replacement
   * position.
   */
  uint64_t recovery_anchor;
} lc_collection_edit_req;

/** Original edit facts; never refreshed on retry. No owned allocations. */
typedef struct lc_collection_edit_res {
  /** Original resulting head token, or the observed current token on
   * collection_changed.
   */
  char head_etag[65];
  /** Permanent position high-water mark, including gaps and deleted entries. */
  uint64_t upper_position;
  /** Live-entry count at the captured observation, before deferred
   * maintenance.
   */
  uint64_t live_count;
  /** Number of live entries newly removed by this edit. */
  uint64_t deleted_count;
  /** Nonzero when this edit appended a replacement entry. */
  int has_replacement;
  /** Original replacement outcome; meaningful only when has_replacement is
   * nonzero.
   */
  lc_collection_append_res replacement;
  /** Nonzero when returning the original outcome of an identical committed
   * retry.
   */
  int duplicate;
  /** A receipt was published or an identical committed retry passed validation.
   * If the call
   * fails with this set, reconcile the outcome/durability through status or
   * an identical retry. A conflict observation has published == 0. */
  int published;
} lc_collection_edit_res;

/** found == 0 observes absence only; it does not cancel a preparing call. */
typedef struct lc_collection_edit_status_res {
  /** Nonzero when an authoritative committed receipt exists for the operation
   * ID.
   */
  int found;
  /** Original committed edit outcome; zeroed when found is zero. */
  lc_collection_edit_res result;
} lc_collection_edit_status_res;

/** Position-preserving deletion outcome, copied by value without owned
 * allocations.
 */
typedef struct lc_collection_delete_res {
  /** Permanent collection position; deletion never renumbers other entries. */
  uint64_t position;
  /** Nonzero when this call deleted a live entry. */
  int deleted;
  /** Nonzero when the position was already deleted; its identity remains
   * reserved.
   */
  int already_deleted;
} lc_collection_delete_res;

/** Distinguishes delivered-entry metadata from examined-input progress. */
typedef enum lc_collection_read_metadata_kind {
  /** One complete JSON object and LF have reached the sink. */
  LC_COLLECTION_READ_METADATA_ENTRY = 1,
  /** Examined nonmatches advanced the proposed input boundary without payload
   * output.
   */
  LC_COLLECTION_READ_METADATA_PROGRESS = 2
} lc_collection_read_metadata_kind;

/** Borrowed record/progress metadata, never injected into the JSON sink.
 * ENTRY arrives after the stored JSON bytes and LF reached the sink. PROGRESS
 * has no payload and only last_examined_position/resume_cursor are meaningful.
 * Copy strings before retaining them beyond the callback. */
typedef struct lc_collection_read_metadata {
  /** ENTRY acknowledges a delivered object; PROGRESS acknowledges examined
   * nonmatches.
   */
  lc_collection_read_metadata_kind kind;
  /** Delivered entry position; meaningful only for ENTRY metadata. */
  uint64_t position;
  /** Caller-supplied retry identity, scoped to this collection incarnation. */
  const char *append_id;
  /** Fingerprint of the compact stored JSON bytes. */
  const char *etag;
  /** Compact plaintext JSON byte count, excluding the output LF. */
  uint64_t bytes;
  /** Last examined input boundary proposed for acknowledgement. */
  uint64_t last_examined_position;
  /** Borrowed opaque cursor for this boundary; copy before returning from the
   * callback.
   */
  const char *resume_cursor;
  /** Backend support for per-object retention metadata. */
  lc_state_retention_support retention_support;
  /** Nonzero when the expiry observation is known, including a zero deadline.
   */
  int has_retention_expires_at_unix;
  /** Unix-second expiry eligibility boundary; zero means no expiry. Cleanup
   * is deferred.
   */
  lc_unix_seconds retention_expires_at_unix;
} lc_collection_read_metadata;

/** Returns LC_OK to accept this boundary or an error to stop. An unsuccessful
 * callback leaves the previous cursor accepted even if payload bytes arrived.
 * Do not re-enter the same collection receiver. Deferred close is permitted. */
typedef int (*lc_collection_read_metadata_fn)(
    void *context, const lc_collection_read_metadata *metadata,
    lc_error *error);
/** Synchronous metadata visitor. Return LC_OK to continue or an error to
 * stop; copy borrowed fields before retaining them.
 */
typedef int (*lc_collection_visit_fn)(void *context,
                                      const lc_collection_info *collection,
                                      lc_error *error);

/** Position bounds and an optional first-N window, followed by synchronous
 * raw JSONL delivery. Repeat selection fields with either cursor purpose.
 * Scalar/callback fields are copied at entry; strings and contexts remain
 * borrowed for the call.
 * No projection, reverse output, last-N or explicit position lists. */
typedef struct lc_collection_read_opts {
  /** Optional opaque head precondition, checked before delivering any bytes. */
  const char *expected_head;
  /** Optional borrowed LQL predicate, applied after position and input-count
   * selection.
   */
  const char *selector_lql;
  /** Optional borrowed resume or checkpoint token; repeat the original
   * selection fields.
   */
  const char *cursor;
  /** Optional synchronous boundary acknowledgement callback, separate from
   * the byte sink.
   */
  lc_collection_read_metadata_fn on_metadata;
  /** Borrowed context passed to on_metadata during this call. */
  void *metadata_context;
  /** Nonzero requests cooperative cancellation, checked between bounded chunks.
   */
  int (*cancelled)(void *context);
  /** Borrowed context passed to the cooperative cancellation callback. */
  void *cancel_context;
  /** Selects after; zero preserves the default start at the beginning. */
  int has_after;
  /** Exclusive permanent position. A start beyond current EOD is empty. */
  uint64_t after;
  /** Selects through; omission uses the captured committed EOD. */
  int has_through;
  /** Inclusive permanent position; must not precede selected after. */
  uint64_t through;
  /** Selects first_n; omission is unlimited, explicit zero is empty. */
  int has_first_n;
  /** Maximum live input entries in this logical window, across resumes.
   * Counts before LQL filtering; nonmatches consume this budget, deleted
   * entries do not. Output remains in ascending position order. */
  uint64_t first_n;
} lc_collection_read_opts;

/** Reason ordered delivery stopped; completion and accepted cursors are
 * reported separately.
 */
typedef enum lc_collection_read_stop_reason {
  /** The selected input window has been fully examined. */
  LC_COLLECTION_READ_FINISHED = 0,
  /** Cooperative cancellation stopped before completing the selected window. */
  LC_COLLECTION_READ_CANCELLED = 1,
  /** Input, validation, sink or metadata acknowledgement failed. */
  LC_COLLECTION_READ_ERROR = 2
} lc_collection_read_stop_reason;

/** Owned read progress, also populated after an error once a view exists.
 * Initialize before use and cleanup before reuse. resume_cursor is retained on
 * incomplete reads; checkpoint_cursor starts a fresh read after completing the
 * selected window.
 * A first-N checkpoint starts the next window after its last safely examined
 * input, including filtered-out inputs. Its resume cursor preserves the
 * unconsumed input budget. Store cursors only after durably accepting the
 * referenced output. Resume captures a new visibility view, so intervening
 * deletes/expiry can remove entries; an individual deletion does not invalidate
 * the cursor. A partial final object must be discarded before resuming into
 * the same sink. */
typedef struct lc_collection_read_res {
  /** Captured EOD capped by through. A first-N window may complete earlier;
   * an empty future range can start beyond this position. */
  uint64_t upper_position;
  /** Entries whose bytes, LF and optional metadata acknowledgement were
   * accepted.
   */
  uint64_t completed_records;
  /** Last safely acknowledged input, including accepted nonmatch progress. */
  uint64_t last_examined_position;
  /** Nonzero when the selected input window is fully examined. */
  int complete;
  /** Finished, cooperatively cancelled or failed outcome. */
  lc_collection_read_stop_reason stop_reason;
  /** Owned continuation for incomplete reads; release with
   * lc_collection_read_res_cleanup().
   */
  char *resume_cursor;
  /** Owned next-window token after completion; release with
   * lc_collection_read_res_cleanup().
   */
  char *checkpoint_cursor;
} lc_collection_read_res;

/** Bound collection incarnation. Entry bodies and positions are immutable;
 * retention metadata may change and entries may be deleted. All streams are
 * borrowed. Closing a client or this receiver from a callback defers
 * reclamation until the active call unwinds. Each returned collection must
 * still be closed separately. */
struct lc_collection {
  /** Observes counts, head token and bounded recovery metadata atomically. */
  int (*head)(lc_collection *, lc_collection_head_res *, lc_error *);
  /** One atomic cut/replacement/marker, preserving existing lease ownership. */
  int (*edit)(lc_collection *, const lc_collection_edit_req *, lc_source *,
              lc_collection_edit_res *, lc_error *);
  /** Observes a committed receipt without opening replacement payloads. */
  int (*edit_status)(lc_collection *, const char *,
                     lc_collection_edit_status_res *, lc_error *);
  /** Returns another owned receiver carrying fresh credentials. */
  int (*acquire)(lc_collection *self, const lc_collection_lease_req *req,
                 lc_collection **out, lc_error *error);
  /** Replaces exactly the observed live lease, never unconditionally. */
  int (*takeover)(lc_collection *self, const lc_collection_lease_req *req,
                  const char *observed_lease_id, lc_collection **out,
                  lc_error *error);
  /** Renews only ownership; it does not renew retention or commit entries. */
  int (*keepalive)(lc_collection *self, long ttl_seconds,
                   lc_collection_lease_info *out, lc_error *error);
  /** Releases and closes on success. Failed release leaves the receiver open.
   * Local close never releases durable ownership. */
  int (*release)(lc_collection *self, lc_error *error);
  /** Observes identity, counts, retention and ownership without reading bodies.
   */
  int (*describe)(lc_collection *self, lc_collection_info *out,
                  lc_error *error);
  /** Changes collection expiry/count policy; cleanup is deferred to
   * maintenance. */
  int (*metadata)(lc_collection *self, const lc_collection_metadata_req *req,
                  lc_collection_info *out, lc_error *error);
  /** Borrows a single-pass source; direct pull retains append authority while
   * reading it. Use staged pull for sources that may wait externally. */
  int (*append)(lc_collection *self, const lc_collection_append_req *req,
                lc_source *source, lc_collection_append_res *out,
                lc_error *error);
  /** Borrows a single-pass source through the writer's canonical streaming
   * path; source reads hold no publication locks. Never resets/closes source.
   */
  int (*append_staged)(lc_collection *self, const lc_collection_append_req *req,
                       lc_source *source, lc_collection_append_res *out,
                       lc_error *error);
  /** Copies the request into an owned writer without publishing an entry.
   * Close the writer on success or failure, independently of this receiver. */
  int (*append_begin)(lc_collection *self, const lc_collection_append_req *req,
                      lc_collection_writer **out, lc_error *error);
  /** Observes a retry binding without a payload; absence reserves nothing. */
  int (*append_status)(lc_collection *self, const char *append_id,
                       lc_collection_append_status_res *out, lc_error *error);
  /** Changes only a live entry's retention; reads/writes no payload bytes. */
  int (*entry_metadata)(lc_collection *self, uint64_t position,
                        const lc_collection_metadata_req *req,
                        lc_collection_append_res *out, lc_error *error);
  /** Streams original compact JSON + LF, with separate per-object cursor
   * metadata. A captured committed EOD bounds this invocation. Sink writes are
   * byte fragments whose sizes and boundaries can vary independently of
   * entries.
   */
  int (*read)(lc_collection *self, const lc_collection_read_opts *opts,
              lc_sink *sink, lc_collection_read_res *out, lc_error *error);
  /** Deletes one permanent position without renumbering or resetting append
   * identities.
   */
  int (*delete_entry)(lc_collection *self, uint64_t position,
                      lc_collection_delete_res *out, lc_error *error);
  /** Logically removes this incarnation; deferred reclamation respects active
   * readers.
   */
  int (*drop)(lc_collection *self, lc_error *error);
  /** Releases this local receiver; never releases a durable collection lease.
   */
  void (*close)(lc_collection *self);
  /** Borrowed namespace name, valid until receiver close. */
  const char *ns;
  /** Borrowed collection name, valid until receiver close. */
  const char *name;
  /** Borrowed incarnation ID, valid until receiver close; distinct from its
   * reusable name.
   */
  const char *collection_id;
  /** Private SDK implementation; applications must not inspect or replace it.
   */
  void *impl;
};

/** Owned incremental append. Writes borrow bytes for the call and return
 * LC_OK/status, not the lc_sink boolean convention. Zero bytes are not EOF.
 * Commit supplies EOF and is terminal; pre-operation argument rejection need
 * not consume the writer. Errors may have an uncertain published
 * outcome. Abort/close never publish or undo a committed append. Close every
 * returned writer, including after success/error. No publication locks are held
 * between calls. Sequential thread transfer is allowed; overlapping operations
 * fail. Closing an active writer defers destruction until its call unwinds.
 * The writer retains its collection/client even after their public close.
 * Pouch validates/encodes once into bounded chunks in shared canonical
 * segments. Commit publishes metadata without reading back or copying the
 * payload. Scratch is bounded independently of entry size; no whole-entry spool
 * is used. A live writer defers compaction capture until commit, abort or
 * close. */
struct lc_collection_writer {
  /** Accepts raw JSON fragments synchronously, using bounded validation and
   * codec scratch.
   */
  int (*write)(lc_collection_writer *self, const void *bytes, size_t count,
               lc_error *error);
  /** Finishes validation and publishes one entry; reconcile uncertain errors
   * by append ID.
   */
  int (*commit)(lc_collection_writer *self, lc_collection_append_res *out,
                lc_error *error);
  /** Abandons unpublished work; cannot undo an already published entry. */
  int (*abort)(lc_collection_writer *self, lc_error *error);
  /** Releases the owned writer, abandoning unfinished canonical extents for
   * maintenance.
   */
  void (*close)(lc_collection_writer *self);
  /** Private SDK implementation; applications must not inspect or replace it.
   */
  void *impl;
};

/** Initializes a creation/open config with no selected retention policy. */
void lc_collection_config_init(lc_collection_config *config);
/** Zero-initialize edit options; operation ID and expected head are required.
 */
void lc_collection_edit_req_init(lc_collection_edit_req *req);
/** Captures a coherent head without traversing entry payloads. */
int lc_collection_head(lc_collection *collection, lc_collection_head_res *out,
                       lc_error *error);
/** Publishes one collection-local edit. On collection_changed, out contains
 * the observed current token/counts, with no mutation. On publication errors,
 * reconcile the original operation ID before issuing another operation. */
int lc_collection_edit(lc_collection *collection,
                       const lc_collection_edit_req *req, lc_source *source,
                       lc_collection_edit_res *out, lc_error *error);
/** Finds the original operation outcome; absence reserves nothing. */
int lc_collection_edit_status(lc_collection *collection,
                              const char *operation_id,
                              lc_collection_edit_status_res *out,
                              lc_error *error);
/** Initializes a request with a 30-second TTL, public reads and no owner. */
void lc_collection_lease_req_init(lc_collection_lease_req *req);
/** Acquires collection-wide ownership. Unleased collections remain multiwriter.
 * Stale credentialed receivers never fall back to unleased mutation. The
 * original receiver is unchanged; close every returned owned receiver. */
int lc_collection_acquire(lc_collection *collection,
                          const lc_collection_lease_req *req,
                          lc_collection **out, lc_error *error);
/** Conditional takeover requires the observed current live lease ID. It fences
 * publication but does not interrupt a source callback. Direct pull retains
 * append authority while consuming its source; other threads can renew/take
 * over and peer readers can capture the committed view. Use staged pull or push
 * for input that may wait externally. Source callback reentry rules still
 * apply.
 */
int lc_collection_takeover(lc_collection *collection,
                           const lc_collection_lease_req *req,
                           const char *observed_lease_id, lc_collection **out,
                           lc_error *error);
/** Renews the live owned collection lease; ttl_seconds is positive and does
 * not alter retention.
 */
int lc_collection_keepalive(lc_collection *collection, long ttl_seconds,
                            lc_collection_lease_info *out, lc_error *error);
/** Releases ownership and closes the receiver on success, without rolling
 * back any committed entry. On failure it remains open for retry/close. */
int lc_collection_release(lc_collection *collection, lc_error *error);
/** Zeroes append options; the caller must supply append_id before use. */
void lc_collection_append_req_init(lc_collection_append_req *req);
/** Zeroes metadata options so omitted retention/count fields preserve
 * existing policy.
 */
void lc_collection_metadata_req_init(lc_collection_metadata_req *req);
/** Zeroes selection options for an unfiltered, unlimited read from the
 * beginning.
 */
void lc_collection_read_opts_init(lc_collection_read_opts *opts);
/** Initializes empty owned cursor storage; cleanup before reinitializing a
 * populated result.
 */
void lc_collection_read_res_init(lc_collection_read_res *result);
/** Releases owned cursors and clears the result; accepts NULL. */
void lc_collection_read_res_cleanup(lc_collection_read_res *result);
/** Creates a new collection incarnation; an existing name is a conflict.
 * The returned receiver is owned and must be closed separately from client. */
int lc_client_create_collection(lc_client *client,
                                const lc_collection_config *config,
                                lc_collection **out, lc_error *error);
/** Opens an existing incarnation without creating or renewing it. Selecting
 * creation expiry or count retention, even zero, is invalid. Returns an owned
 * receiver. */
int lc_client_open_collection(lc_client *client,
                              const lc_collection_config *config,
                              lc_collection **out, lc_error *error);
/** Lists one namespace in name order, without state payloads. The visitor is
 * synchronous and runs outside mutation locks. NULL ns selects the default. */
int lc_client_list_collections(lc_client *client, const char *ns,
                               lc_collection_visit_fn visitor, void *context,
                               lc_error *error);
/** Returns local capability facts even for a remote client with no collection
 * support. Other remote collection operations return LC_ERR_UNSUPPORTED. */
int lc_client_collection_capabilities(lc_client *client,
                                      lc_collection_capabilities *out,
                                      lc_error *error);
/** Observes copied identity, counts, retention and ownership without reading
 * payload bytes.
 */
int lc_collection_describe(lc_collection *collection, lc_collection_info *out,
                           lc_error *error);
/** Updates collection expiry and/or max_entries. Omitted fields preserve their
 * values. Expiry does not assign a per-entry age limit. Increasing/disabling
 * the count limit never restores deleted entries or their bodies.
 */
int lc_collection_metadata(lc_collection *collection,
                           const lc_collection_metadata_req *req,
                           lc_collection_info *out, lc_error *error);
/** Changes a live entry's expiry without payload I/O or changes to its body,
 * ETag, append ID or permanent position. A deleted entry cannot be revived. */
int lc_collection_entry_metadata(lc_collection *collection, uint64_t position,
                                 const lc_collection_metadata_req *req,
                                 lc_collection_append_res *out,
                                 lc_error *error);
/** Direct pull consumes a borrowed source once under append authority; it never
 * resets/closes the source or automatically replays an upload. Slow source
 * reads delay other appends to this collection; staged pull releases that
 * authority while consuming input. Same-receiver callback operations and peer
 * mutations of this collection fail; peer reads and independent collections may
 * progress. A post-publication error may be ambiguous: use append_status or
 * explicitly retry the same ID with a fresh source. An identical retry
 * preserves expiry.
 */
int lc_collection_append(lc_collection *collection,
                         const lc_collection_append_req *req, lc_source *source,
                         lc_collection_append_res *out, lc_error *error);
/** Explicit staged pull for slow or concurrent producers. Borrows and consumes
 * source once, never resets or closes it, and never retries automatically.
 * Uses the same validation, bounded canonical chunks, fencing and commit
 * protocol as append_begin/write/commit. No publication locks are held while
 * reading source. Source callbacks retain append's reentry rules: same-receiver
 * operations and peer mutations of this collection fail; peer reads and other
 * collections may progress. Closing public owners does not invalidate the call.
 * Source/validation failures publish nothing; commit errors can be uncertain.
 * Direct append can be cheaper for fast inputs that do not wait externally. */
int lc_collection_append_staged(lc_collection *collection,
                                const lc_collection_append_req *req,
                                lc_source *source,
                                lc_collection_append_res *out, lc_error *error);
/** Starts an owned push writer, copying request strings before return. It keeps
 * its owners alive but holds no publication locks between fragment calls.
 * Returns NULL output on failure; close a returned writer on every outcome. */
int lc_collection_append_begin(lc_collection *collection,
                               const lc_collection_append_req *req,
                               lc_collection_writer **out, lc_error *error);
/** Borrows one raw JSON fragment for the call. Returns LC_OK/status, not a sink
 * boolean; zero bytes are not EOF. Validation/encoding failures are terminal.
 */
int lc_collection_writer_write(lc_collection_writer *writer, const void *bytes,
                               size_t count, lc_error *error);
/** Finishes validation and publishes one entry; output is required. After a
 * publication error, use append_status with the original append ID. */
int lc_collection_writer_commit(lc_collection_writer *writer,
                                lc_collection_append_res *out, lc_error *error);
/** Idempotently discards an unfinished append; safe after write/commit failure.
 */
int lc_collection_writer_abort(lc_collection_writer *writer, lc_error *error);
/** Releases the owned writer, abandoning unfinished chunks for maintenance.
 * Never commits or undoes publication. The C pointer is invalid after this
 * call. */
void lc_collection_writer_close(lc_collection_writer *writer);
/** Observes ABSENT/COMMITTED/DELETED without a body. ABSENT returns LC_OK and
 * does not cancel an upload in progress. Deleted retry identities are retained.
 */
int lc_collection_append_status(lc_collection *collection,
                                const char *append_id,
                                lc_collection_append_status_res *out,
                                lc_error *error);
/** Streams compact JSON objects plus LF into a required borrowed sink; metadata
 * uses opts->on_metadata instead of the payload. NULL opts selects a full read.
 * Bounds and first_n select inputs before LQL. Initialize out before calling
 * and clean it up even on failure; it can contain accepted partial progress.
 */
int lc_collection_read(lc_collection *collection,
                       const lc_collection_read_opts *opts, lc_sink *sink,
                       lc_collection_read_res *out, lc_error *error);
/** Deletes one entry while preserving its position gap and append retry
 * identity. Repeating a known deletion reports already_deleted. */
int lc_collection_delete_entry(lc_collection *collection, uint64_t position,
                               lc_collection_delete_res *out, lc_error *error);
/** Removes the incarnation logically. Physical reclamation is deferred;
 * old handles/cursors cannot address a replacement with the same name. */
int lc_collection_drop(lc_collection *collection, lc_error *error);
/** Releases this receiver; active callback close defers reclamation. */
void lc_collection_close(lc_collection *collection);

/**
 * Callback used to resolve a file-backed local-mutate input into a new source.
 *
 * `resolved_path` is borrowed for the call. Return `LC_OK` and transfer a new
 * source through `out`, or return a non-`LC_OK` status with `error` populated.
 * The local mutate operation closes a successfully returned source.
 */
typedef int (*lc_file_value_open_fn)(void *context, const char *resolved_path,
                                     lc_source **out, lc_error *error);

/** Optional override used by local mutate to open file-backed values. */
typedef struct lc_file_value_resolver {
  /** Opens `resolved_path` and transfers the returned source to the caller. */
  lc_file_value_open_fn open;
  /** Opaque borrowed context passed to `open`. */
  void *context;
} lc_file_value_resolver;

/** Request used to acquire a new lease. */
typedef struct lc_acquire_req {
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *ns;
  /** Lease key to acquire. */
  const char *key;
  /** Non-empty logical owner identifier required for lease acquisition. */
  const char *owner;
  /** Requested lease TTL in seconds. */
  long ttl_seconds;
  /** Optional server-side blocking acquire wait in seconds. */
  long block_seconds;
  /** When non-zero, acquisition succeeds only if the key does not yet exist. */
  int if_not_exists;
  /** Optional transaction identifier to bind the acquire into. */
  const char *txn_id;
} lc_acquire_req;

/** Result metadata returned by a successful lease acquisition. */
typedef struct lc_acquire_res {
  /** State version visible to the acquired lease. */
  lc_version version;
  /** Private state entity tag for the acquired lease. */
  char *state_etag;
  /** Fencing token assigned to this lease acquisition. */
  long fencing_token;
  /** Server correlation id for the acquisition. */
  char *correlation_id;
} lc_acquire_res;

/** Stable lease identity used by client-level operations on an existing lease.
 * Mutating operations require a non-empty `lease_id` and its matching fencing
 * token; an empty reference is never an unfenced write request. */
typedef struct lc_lease_ref {
  /** Namespace of the existing lease. */
  const char *ns;
  /** Key of the existing lease. */
  const char *key;
  /** Server-issued lease identifier, required for every mutation. */
  const char *lease_id;
  /** Transaction identifier associated with the lease, if any. */
  const char *txn_id;
  /** Current fencing token for optimistic concurrency. */
  long fencing_token;
} lc_lease_ref;

/** Request used to describe a lease or state object. */
typedef struct lc_describe_req {
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *ns;
  /** Key to describe. */
  const char *key;
} lc_describe_req;

/** Lease description returned by describe operations. */
typedef struct lc_describe_res {
  /** Namespace of the described key. */
  char *ns;
  /** Described key. */
  char *key;
  /** Current lease owner, when a lease is active. */
  char *owner;
  /** Current state version. */
  lc_version version;
  /** Current lease identifier, when a lease is active. */
  char *lease_id;
  /** Current lease expiry as a Unix timestamp, or zero when not leased. */
  lc_unix_seconds lease_expires_at_unix;
  /** Current fencing token, or zero when not leased. */
  long fencing_token;
  /** Transaction identifier associated with the lease, if any. */
  char *txn_id;
  /** Private state etag. */
  char *state_etag;
  /** Public committed state etag. */
  char *public_state_etag;
  /** Non-zero when `query_hidden` was explicitly set. */
  int has_query_hidden;
  /** Whether the public state is hidden from query results. */
  int query_hidden;
  /** Server correlation id for the describe response. */
  char *correlation_id;
  /** Backend support for per-object state retention. */
  lc_state_retention_support retention_support;
  /** Non-zero when the returned body has a known retention deadline. */
  int has_retention_expires_at_unix;
  /** Expiry eligibility boundary; zero confirms no expiry. Deletion is
   * deferred. */
  lc_unix_seconds retention_expires_at_unix;
} lc_describe_res;

/** Optional flags that control state reads. */
typedef struct lc_get_opts {
  /** Reads the public view instead of the private lease-bound state. */
  int public_read;
} lc_get_opts;

/** Metadata returned by streamed or buffered state reads. */
typedef struct lc_get_res {
  /** Non-zero when the key has no readable state body. */
  int no_content;
  /** Content type recorded for the returned state body. */
  char *content_type;
  /** Entity tag for the returned state body. */
  char *etag;
  /** Version of the returned state body. */
  lc_version version;
  /** Fencing token associated with the lease-bound read, if any. */
  long fencing_token;
  /** Server correlation id for the read response. */
  char *correlation_id;
  /** Backend support for per-object state retention. */
  lc_state_retention_support retention_support;
  /** Non-zero when the returned body has a known retention deadline. */
  int has_retention_expires_at_unix;
  /** Expiry eligibility boundary; zero confirms no expiry. Deletion is
   * deferred. */
  lc_unix_seconds retention_expires_at_unix;
} lc_get_res;

/** Optional controls for streamed JSON state updates. */
typedef struct lc_update_opts {
  /** Optional state etag precondition for optimistic concurrency. */
  const char *if_state_etag;
  /** Optional version precondition for optimistic concurrency. */
  lc_version if_version;
  /** Enables `if_version` when non-zero so version `0` stays representable. */
  int has_if_version;
  /** Content type sent with the JSON state body. */
  const char *content_type;
  /** Selects retention_seconds; omission preserves the existing policy. */
  int has_retention_seconds;
  /** Seconds from commit; zero clears expiry. Must be nonnegative if selected.
   */
  long retention_seconds;
} lc_update_opts;

/** Client-level update operation for an existing, credentialed lease reference.
 */
typedef struct lc_update_req {
  /** Existing lease identity to update. */
  lc_lease_ref lease;
  /** Optional state etag precondition for optimistic concurrency. */
  const char *if_state_etag;
  /** Optional version precondition for optimistic concurrency. */
  lc_version if_version;
  /** Enables `if_version` when non-zero so version `0` stays representable. */
  int has_if_version;
  /** Content type sent with the JSON state body. */
  const char *content_type;
  /** Selects retention_seconds; omission preserves the existing policy. */
  int has_retention_seconds;
  /** Seconds from commit; zero clears expiry. Must be nonnegative if selected.
   */
  long retention_seconds;
} lc_update_req;

/** Metadata returned by a successful state update. */
typedef struct lc_update_res {
  /** State version after the update. */
  lc_version new_version;
  /** Owned entity tag for the updated private state. */
  char *new_state_etag;
  /** State bytes, representable by `long` on the calling architecture. */
  long bytes;
  /** Owned server correlation id for the update response. */
  char *correlation_id;
  /** Backend support for per-object state retention. */
  lc_state_retention_support retention_support;
  /** Non-zero when the returned body has a known retention deadline. */
  int has_retention_expires_at_unix;
  /** Expiry eligibility boundary; zero confirms no expiry. Deletion is
   * deferred. */
  lc_unix_seconds retention_expires_at_unix;
} lc_update_res;

/** Request that applies one or more server-side state mutations. */
typedef struct lc_mutate_req {
  /** JSON mutation expressions evaluated in order by the server. */
  const char *const *mutations;
  /** Number of entries in `mutations`. */
  size_t mutation_count;
  /** Optional state etag precondition for optimistic concurrency. */
  const char *if_state_etag;
  /** Optional version precondition for optimistic concurrency. */
  lc_version if_version;
  /** Enables `if_version` when non-zero so version `0` stays representable. */
  int has_if_version;
  /** Selects retention_seconds; omission preserves the existing policy. */
  int has_retention_seconds;
  /** Seconds from commit; zero clears expiry. Must be nonnegative if selected.
   */
  long retention_seconds;
} lc_mutate_req;

/**
 * Request used by `mutate_local()` to stream a state document through the
 * local mutate engine and then upload the staged JSON result back to lockd.
 */
typedef struct lc_mutate_local_req {
  /** JSON mutation expressions evaluated in order by the local mutate engine.
   */
  const char *const *mutations;
  /** Number of entries in `mutations`. */
  size_t mutation_count;
  /** When non-zero, do not default `if_state_etag`/`if_version` from `get()`.
   */
  int disable_fetched_cas;
  /** Base directory used to resolve relative `file:`/`textfile:` paths. */
  const char *file_value_base_dir;
  /** Optional file opener override for file-backed mutators. */
  const lc_file_value_resolver *file_value_resolver;
  /** Optional explicit update preconditions and content type overrides. */
  lc_update_opts update;
} lc_mutate_local_req;

/** Client-level mutate operation for an existing, credentialed lease reference.
 */
typedef struct lc_mutate_op {
  /** Existing lease identity to mutate. */
  lc_lease_ref lease;
  /** JSON mutation expressions evaluated in order by the server. */
  const char *const *mutations;
  /** Number of entries in `mutations`. */
  size_t mutation_count;
  /** Optional state etag precondition for optimistic concurrency. */
  const char *if_state_etag;
  /** Optional version precondition for optimistic concurrency. */
  lc_version if_version;
  /** Enables `if_version` when non-zero so version `0` stays representable. */
  int has_if_version;
  /** Selects retention_seconds; omission preserves the existing policy. */
  int has_retention_seconds;
  /** Seconds from commit; zero clears expiry. Must be nonnegative if selected.
   */
  long retention_seconds;
} lc_mutate_op;

/** Metadata returned by a successful mutate operation. */
typedef struct lc_mutate_res {
  /** State version after applying all mutations. */
  lc_version new_version;
  /** Owned entity tag for the mutated private state. */
  char *new_state_etag;
  /** State bytes, representable by `long` on the calling architecture. */
  long bytes;
  /** Owned server correlation id for the mutate response. */
  char *correlation_id;
  /** Backend support for per-object state retention. */
  lc_state_retention_support retention_support;
  /** Non-zero when the returned body has a known retention deadline. */
  int has_retention_expires_at_unix;
  /** Expiry eligibility boundary; zero confirms no expiry. Deletion is
   * deferred. */
  lc_unix_seconds retention_expires_at_unix;
} lc_mutate_res;

/** Request used to update lease metadata without replacing state bytes. */
typedef struct lc_metadata_req {
  /** Set to non-zero when `query_hidden` should be updated. */
  int has_query_hidden;
  /** Whether the state should be hidden from normal query results. */
  int query_hidden;
  /** Optional version precondition for optimistic concurrency. */
  lc_version if_version;
  /** Enables `if_version` when non-zero so version `0` stays representable. */
  int has_if_version;
  /** Selects retention_seconds; omission preserves the existing policy. */
  int has_retention_seconds;
  /** Seconds from commit; zero clears expiry. Must be nonnegative if selected.
   */
  long retention_seconds;
} lc_metadata_req;

/** Client-level metadata update operation for an existing, credentialed lease
 * reference. */
typedef struct lc_metadata_op {
  /** Existing lease identity to update. */
  lc_lease_ref lease;
  /** Set to non-zero when `query_hidden` should be updated. */
  int has_query_hidden;
  /** Whether the state should be hidden from normal query results. */
  int query_hidden;
  /** Optional version precondition for optimistic concurrency. */
  lc_version if_version;
  /** Enables `if_version` when non-zero so version `0` stays representable. */
  int has_if_version;
  /** Selects retention_seconds; omission preserves the existing policy. */
  int has_retention_seconds;
  /** Seconds from commit; zero clears expiry. Must be nonnegative if selected.
   */
  long retention_seconds;
} lc_metadata_op;

/** Metadata returned by a successful metadata update. */
typedef struct lc_metadata_res {
  /** Namespace of the updated key. */
  char *ns;
  /** Updated key. */
  char *key;
  /** State version after the metadata update. */
  lc_version version;
  /** Non-zero when `query_hidden` is set. */
  int has_query_hidden;
  /** Current query visibility flag. */
  int query_hidden;
  /** Server correlation id for the metadata response. */
  char *correlation_id;
  /** Backend support for per-object state retention. */
  lc_state_retention_support retention_support;
  /** Non-zero when the returned body has a known retention deadline. */
  int has_retention_expires_at_unix;
  /** Expiry eligibility boundary; zero confirms no expiry. Deletion is
   * deferred. */
  lc_unix_seconds retention_expires_at_unix;
} lc_metadata_res;

/** Request used to remove state bytes while keeping the lease. */
typedef struct lc_remove_req {
  /** Optional state etag precondition for optimistic concurrency. */
  const char *if_state_etag;
  /** Optional version precondition for optimistic concurrency. */
  lc_version if_version;
  /** Enables `if_version` when non-zero so version `0` stays representable. */
  int has_if_version;
} lc_remove_req;

/** Client-level remove operation for an existing, credentialed lease reference.
 */
typedef struct lc_remove_op {
  /** Existing lease identity to remove state from. */
  lc_lease_ref lease;
  /** Optional state etag precondition for optimistic concurrency. */
  const char *if_state_etag;
  /** Optional version precondition for optimistic concurrency. */
  lc_version if_version;
  /** Enables `if_version` when non-zero so version `0` stays representable. */
  int has_if_version;
} lc_remove_op;

/** Result returned by a remove operation. */
typedef struct lc_remove_res {
  /** Non-zero when state bytes were removed. */
  int removed;
  /** State version after removal. */
  lc_version new_version;
  /** Owned server correlation id for the remove response. */
  char *correlation_id;
} lc_remove_res;

/** Request used to extend an existing lease. */
typedef struct lc_keepalive_req {
  /** New TTL in seconds for the renewed lease. */
  long ttl_seconds;
} lc_keepalive_req;

/** Client-level keepalive operation for an existing lease reference. */
typedef struct lc_keepalive_op {
  /** Existing lease identity to renew. */
  lc_lease_ref lease;
  /** New TTL in seconds for the renewed lease. */
  long ttl_seconds;
} lc_keepalive_op;

/** Result returned by a successful keepalive operation. */
typedef struct lc_keepalive_res {
  /** New lease expiry as a Unix timestamp. */
  lc_unix_seconds lease_expires_at_unix;
  /** State version visible after renewal. */
  lc_version version;
  /** Private state etag visible after renewal. */
  char *state_etag;
  /** Server correlation id for the keepalive response. */
  char *correlation_id;
} lc_keepalive_res;

/** Request used to release a lease, optionally rolling back transactional
 * state. */
typedef struct lc_release_req {
  /** Rolls transactional state back before releasing when non-zero. */
  int rollback;
} lc_release_req;

/** Client-level release operation for an existing lease reference. */
typedef struct lc_release_op {
  /** Existing lease identity to release. */
  lc_lease_ref lease;
  /** Rolls transactional state back before releasing when non-zero. */
  int rollback;
} lc_release_op;

/** Result returned by a release operation. */
typedef struct lc_release_res {
  /** Non-zero when the durable lease was released. */
  int released;
  /** Owned server correlation id for the release response. */
  char *correlation_id;
} lc_release_res;

/** Streamed query request. Query result rows are written into a sink. */
typedef struct lc_query_req {
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *ns;
  /** JSON selector expression used by the query engine. */
  const char *selector_json;
  /** Maximum number of rows to return in this page. */
  long limit;
  /** Cursor returned by a previous query page, or `NULL` for a fresh query. */
  const char *cursor;
  /** Optional JSON field-selection expression. */
  const char *fields_json;
  /** Server-specific query return mode. */
  const char *return_mode;
  /** Optional query engine hint, for example `index` or `scan`. */
  const char *engine;
  /** Optional query refresh mode, for example `wait_for`. */
  const char *refresh;
  /**
   * Full-form LQL selector expression used by the query engine. Mutually
   * exclusive with `selector_json`.
   */
  const char *selector_lql;
} lc_query_req;

/** Query metadata returned alongside streamed query results. */
typedef struct lc_query_res {
  /** Cursor for the next result page, or `NULL` when there is no next page. */
  char *cursor;
  /** Return mode selected by the engine for this response. */
  char *return_mode;
  /** Query/index freshness sequence observed by this response. */
  lc_index_seq index_seq;
  /** Server correlation id for the query response. */
  char *correlation_id;
  /** Raw JSON metadata emitted by query responses, when present. */
  char *metadata_json;
} lc_query_res;

/** Chunked callback handler for `lc_query_keys()`. */
typedef struct lc_query_key_handler {
  /**
   * Called when a decoded key string begins.
   *
   * Return non-zero to continue, or zero to stop. Populate `error` when
   * stopping due to a failure.
   */
  int (*begin)(void *context, lc_error *error);
  /**
   * Called with decoded UTF-8 key bytes.
   *
   * Bytes are borrowed and valid only for the call. A key may arrive in
   * multiple chunks, including chunks that split a UTF-8 sequence.
   * Return non-zero to continue, or zero to stop. Populate `error` when
   * stopping due to a failure.
   */
  int (*chunk)(void *context, const char *bytes, size_t len, lc_error *error);
  /**
   * Called after the key and following array delimiter are validated.
   *
   * Return non-zero to continue with the next key, or zero to stop. Populate
   * `error` when stopping due to a failure.
   */
  int (*end)(void *context, lc_error *error);
} lc_query_key_handler;

/**
 * Creates or reopens one durable history-retention consumer.
 *
 * The `(ns, consumer_id)` pair is durable identity. A consumer
 * pins Pouch log history until it advances its acknowledged sequence or is
 * explicitly unregistered. It does not itself deliver history records.
 */
typedef struct lc_history_consumer_config {
  /** Namespace whose durable history this consumer retains. */
  const char *ns;
  /** Stable, nonempty application-defined consumer identity. */
  const char *consumer_id;
  /**
   * Initial retained boundary for a newly created consumer. Existing durable
   * consumers retain their recorded boundary and ignore this field. Set
   * `LC_HISTORY_CONSUMER_START_AT_CURRENT` to retain only later records.
   */
  lc_index_seq initial_acknowledged_index_seq;
} lc_history_consumer_config;

/** Durable and current positions reported by a history consumer. */
typedef struct lc_history_consumer_position {
  /** Highest history sequence durably acknowledged by this consumer. */
  lc_index_seq acknowledged_index_seq;
  /** Current durable namespace sequence observed by this operation. */
  lc_index_seq current_index_seq;
} lc_history_consumer_position;

/** Generic owned string list used by several management responses. */
typedef struct lc_string_list {
  /** Owned array of owned strings. */
  char **items;
  /** Number of entries in `items`. */
  size_t count;
} lc_string_list;

/** Request used to read or update namespace configuration. */
typedef struct lc_namespace_config_req {
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *ns;
  /** Preferred query engine, for example `index` or `scan`. */
  const char *preferred_engine;
  /** Fallback query engine, for example `scan`. */
  const char *fallback_engine;
  /** Optional etag precondition for updates. */
  const char *if_etag;
} lc_namespace_config_req;

/** Namespace configuration returned by get or update operations. */
typedef struct lc_namespace_config_res {
  /** Namespace whose config was returned. */
  char *ns;
  /** Preferred query engine. */
  char *preferred_engine;
  /** Fallback query engine. */
  char *fallback_engine;
  /** Namespace config etag. */
  char *etag;
  /** Server correlation id for the namespace config response. */
  char *correlation_id;
} lc_namespace_config_res;

/** Request used to flush an index for a namespace. */
typedef struct lc_index_flush_req {
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *ns;
  /** Flush mode, for example `wait` for synchronous freshness. */
  const char *mode;
} lc_index_flush_req;

/** Result returned by an index flush request. */
typedef struct lc_index_flush_res {
  /** Namespace whose index was flushed. */
  char *ns;
  /** Flush mode applied by the engine. */
  char *mode;
  /** Engine-generated flush identifier, when present. */
  char *flush_id;
  /** Non-zero when an asynchronous flush was accepted. */
  int accepted;
  /** Non-zero when the index was flushed before returning. */
  int flushed;
  /** Non-zero when index work remains pending. */
  int pending;
  /** Index sequence visible after the flush request. */
  lc_index_seq index_seq;
  /** Server correlation id for the flush response. */
  char *correlation_id;
} lc_index_flush_res;

/** Transaction participant identity used by TC prepare/commit/rollback
 * operations. A nonempty backend hash targets that exact resource manager;
 * an empty hash is a compatibility wildcard. Outer whitespace is normalized;
 * whitespace-only nonempty values are invalid. */
typedef struct lc_txn_participant {
  /** Participant namespace. */
  const char *ns;
  /** Participant key. */
  const char *key;
  /** Target backend hash or compatibility wildcard. */
  const char *backend_hash;
} lc_txn_participant;

/** Request used to replay transaction coordinator state for a transaction id.
 */
typedef struct lc_txn_replay_req {
  /** Transaction identifier to replay. */
  const char *txn_id;
} lc_txn_replay_req;

/** Result returned by transaction replay. Pouch also populates this result on
 * LC_ERR_PENDING: state is the durable decision and participant application
 * remains incomplete. Release owned fields with the normal cleanup function. */
typedef struct lc_txn_replay_res {
  /** Replayed transaction identifier. */
  char *txn_id;
  /** Durable decision state after replay. */
  char *state;
  /** Server correlation id for the replay response. */
  char *correlation_id;
} lc_txn_replay_res;

/** Request used for TC prepare, commit, and rollback decisions.
 * A nonempty target_backend_hash scopes the decision to one receiving resource
 * manager. It is normalized for outer whitespace and must match that resource
 * manager's backend identity. */
typedef struct lc_txn_decision_req {
  /** Transaction identifier to decide. */
  const char *txn_id;
  /** Participant set covered by this decision. */
  const lc_txn_participant *participants;
  /** Number of entries in `participants`. */
  size_t participant_count;
  /** Expiry for a prepared decision as a Unix timestamp. */
  lc_unix_seconds expires_at_unix;
  /** Transaction-coordinator term for this decision. */
  lc_tc_term tc_term;
  /** Optional exact backend target for this decision. */
  const char *target_backend_hash;
} lc_txn_decision_req;

/** Result returned by TC prepare, commit, or rollback. Pouch also populates
 * this result on LC_ERR_PENDING after recording a durable decision. The
 * decision must not be reversed because participant application is pending.
 * Release owned fields with lc_txn_decision_res_cleanup(), including on errors.
 */
typedef struct lc_txn_decision_res {
  /** Decided transaction identifier. */
  char *txn_id;
  /** Durable decision state after the operation. */
  char *state;
  /** Server correlation id for the decision response. */
  char *correlation_id;
} lc_txn_decision_res;

/** Durable external branch identity and progress. No SQL or credentials belong
 * in this record. Native XIDs, system identifiers and OIDs are exact decimal
 * text; do not narrow them.
 * .txns owns the decision; this descriptor only records participant progress.
 */
typedef struct lc_xa_branch_record {
  /** Global transaction XID, distinct from a native database transaction ID. */
  char txn_id[LC_XID_STRING_SIZE];
  /** Stable registered resource XID used during enrollment and recovery. */
  char resource_id[LC_XID_STRING_SIZE];
  /** Stable enrolled branch XID within the global transaction. */
  char branch_id[LC_XID_STRING_SIZE];
  /** Exact native prepared-transaction identifier, NUL-terminated. */
  char gid[200];
  /** Exact epoch-qualified native transaction ID as decimal text. */
  char full_xid[21];
  /** Exact database-cluster system identifier as decimal text. */
  char system_id[21];
  /** Exact database OID as decimal text, NUL-terminated. */
  char database_oid[11];
  /** Exact role OID as decimal text, NUL-terminated. */
  char role_oid[11];
  /** Durable native prepare acknowledgement/inventory observation. This is
   * distinct from a coordinator YES vote, including recovery after ABORT. */
  unsigned char prepared_observed;
  unsigned char phase;    /**< LC_XA_ENROLLED through LC_XA_ABORTED. */
  unsigned char evidence; /**< LC_XA_EVIDENCE_* completion source. */
  lc_unix_seconds
      completed_at_unix; /**< Zero until proven branch completion. */
} lc_xa_branch_record;

enum {
  /** Exact branch identity is durably enrolled; native prepare is not yet
   * acknowledged.
   */
  LC_XA_ENROLLED = 0,
  /** Prepare transmission intent is durable; native outcome may need
   * reconciliation.
   */
  LC_XA_PREPARE_SENT = 1,
  /** Native preparation was observed; coordinator votes remain separate. */
  LC_XA_PREPARED = 2,
  /** Commit transmission intent matches the durable global COMMIT decision. */
  LC_XA_COMMIT_SENT = 3,
  /** Rollback transmission intent matches the durable global ABORT decision. */
  LC_XA_ROLLBACK_SENT = 4,
  /** Native branch commit has been proven. */
  LC_XA_COMMITTED = 5,
  /** Native branch rollback/abort has been proven. */
  LC_XA_ABORTED = 6
};
enum {
  /** No branch-completion evidence has been recorded. */
  LC_XA_EVIDENCE_NONE = 0,
  /** Completion was acknowledged by the native operation. */
  LC_XA_EVIDENCE_ACK = 1,
  /** Completion was established from native transaction status. */
  LC_XA_EVIDENCE_NATIVE = 2,
  /** Completion was established from durable prepare/resolution protocol
   * evidence, without replaying application SQL. */
  LC_XA_EVIDENCE_RECONCILED = 3
};

/** Retained Pouch coordinator journal, independent of native client libraries.
 * Calls are explicitly blocking and serialized by the coordinator. Asynchronous
 * providers call them from a durability worker, never their socket owner loop.
 * Close only after all worker calls finish; the journal pins its root client.
 */
typedef struct lc_xa_store lc_xa_store;
/** Opens a local Pouch journal and retains its client. Clears *out on error. */
int lc_xa_store_open(lc_client *client, lc_xa_store **out, lc_error *error);
/** Retain a journal for an explicitly linked provider. Close releases a
 * reference after its worker has stopped using the journal. */
int lc_xa_store_retain(lc_xa_store *store, lc_error *error);
/** Consumes one journal reference; wait for its worker calls before release. */
void lc_xa_store_close(lc_xa_store *store);
/** Register stable resource/backend identity in .xa. Re-registration must
 * match. Only resource_id, system_id, database_oid and role_oid are used. */
int lc_xa_store_register_blocking(lc_xa_store *store,
                                  const lc_xa_branch_record *identity,
                                  lc_error *error);
/** Durably enrolls an exact branch identity before native prepare. Enrollment
 * must precede sealing; an existing branch cannot change native identity. */
int lc_xa_store_enroll_blocking(lc_xa_store *store,
                                const lc_xa_branch_record *branch,
                                lc_error *error);
/** Validates phase-two direction against the immutable global decision before
 * recording transmission intent. A provider must not send before this succeeds.
 */
int lc_xa_store_progress_blocking(lc_xa_store *store,
                                  const lc_xa_branch_record *branch,
                                  lc_error *error);
/** Copies the descriptor and, when non-NULL, its authoritative coordinator YES
 * vote into prepared. prepared_observed is separate native preparation
 * evidence; an observed prepare after ABORT does not create a YES vote. */
int lc_xa_store_read_blocking(lc_xa_store *store, const char *txn_id,
                              const char *branch_id, lc_xa_branch_record *out,
                              int *prepared, lc_error *error);

/** Public adapter view. Initialize before use; size/version are checked before
 * a provider callback. The native connection is borrowed, never owned. Retain
 * the borrow while a cursor/result/callback uses this view and validate again
 * before native access. owner_thread is an opaque identity, not pthread data.
 */
typedef struct lc_xa_connection_view {
  /** Structure byte size used to validate the adapter boundary. */
  size_t size;
  /** Independently versioned adapter ABI; initialize with the matching
   * LC_XA_*_ABI constant.
   */
  unsigned long abi_version;
  /** Native provider kind, such as LC_XA_RESOURCE_POSTGRES. */
  int resource_kind;
  /** Stable registered resource XID used during enrollment and recovery. */
  char resource_id[LC_XID_STRING_SIZE];
  /** Global transaction XID, distinct from a native database transaction ID. */
  char txn_id[LC_XID_STRING_SIZE];
  /** Stable enrolled branch XID within the global transaction. */
  char branch_id[LC_XID_STRING_SIZE];
  /** Borrowed provider connection; retain/validate its scope before every
   * native use.
   */
  void *native_connection;
  /** Process identity owning this native connection scope. */
  uint64_t owner_process;
  /** Opaque owning-thread identity; not a portable pthread_t representation. */
  const void *owner_thread;
  /** Validity epoch used to reject stale connection views. */
  uint64_t generation;
} lc_xa_connection_view;

/** Native connection-view ABI, independent of the library release version. */
#define LC_XA_CONNECTION_VIEW_ABI 1UL
enum {
  /** Explicitly linked PostgreSQL participant provider. */
  LC_XA_RESOURCE_POSTGRES = 1
};
/** Retained owning-process/thread validity token for a borrowed native
 * connection.
 */
typedef struct lc_xa_borrow lc_xa_borrow;
/** Provider callbacks stay linked while any borrow reference lives. view must
 * validate its retained native token before returning a pointer. release
 * reclaims context, including on protocol error; it never transfers native
 * ownership to an adapter. No callback is invoked from a foreign thread/fork.
 */
typedef struct lc_xa_borrow_callbacks {
  /** Validates the live scope and copies a borrowed view; transfers no
   * connection ownership.
   */
  int (*view)(void *context, lc_xa_connection_view *out, lc_error *error);
  /** Consumes the callback context on final release, including when protocol
   * cleanup fails.
   */
  int (*release)(void *context, lc_error *error);
} lc_xa_borrow_callbacks;
/** Zeroes the view and sets the supported size/ABI before validation. */
void lc_xa_connection_view_init(lc_xa_connection_view *view);
/** Context ownership transfers only on success. */
int lc_xa_borrow_open(const lc_xa_borrow_callbacks *callbacks, void *context,
                      lc_xa_borrow **out, lc_error *error);
/** Adds one owning-process/thread reference for a cursor/result/callback. */
int lc_xa_borrow_retain(lc_xa_borrow *borrow, lc_error *error);
/** Validates the current native scope and copies its borrowed connection view.
 * A retained token remains releasable after native access has been invalidated.
 */
int lc_xa_borrow_view(lc_xa_borrow *borrow, lc_xa_connection_view *out,
                      lc_error *error);
/** Consumes one reference on the owning process/thread. The final release
 * destroys the token even if provider cleanup reports an error; never retry
 * that reference. Release results/cursors first: outstanding protocol fails the
 * branch and preserves durable cleanup. Foreign-owner rejection releases none.
 */
int lc_xa_borrow_release(lc_xa_borrow *borrow, lc_error *error);

/** Explicitly linked XA participant contract. No native client types or loader
 * dependencies enter core. Providers copy configuration at open and keep their
 * callbacks linked until the last resource/branch/operation/borrow is released.
 */
#define LC_XA_DRIVER_ABI 1UL
/** Nonterminal operation readiness, copied without transferring descriptor
 * ownership.
 */
typedef struct lc_xa_wait {
  /** Borrowed readiness descriptor; do not close it. */
  int fd;
  unsigned int events; /**< READ=1, WRITE=2; zero when terminal. */
  int64_t
      deadline_ms; /**< Absolute CLOCK_MONOTONIC milliseconds, -1 infinity. */
} lc_xa_wait;
enum {
  /** Native outcome is unavailable or ambiguous; preserve recovery intent. */
  LC_XA_OUTCOME_UNKNOWN = 0,
  /** Native transaction status proves commit. */
  LC_XA_OUTCOME_COMMITTED = 1,
  /** Native transaction status proves abort. */
  LC_XA_OUTCOME_ABORTED = 2,
  /** Native transaction remains active or prepared. */
  LC_XA_OUTCOME_IN_PROGRESS = 3,
  /** Native status history has expired; this observation alone proves no
   * terminal outcome. */
  LC_XA_OUTCOME_DISCARDED = 4
};
/** One diagnostic native prepared-transaction observation; not authority to
 * resolve it.
 */
typedef struct lc_xa_inventory_row {
  /** Exact native prepared-transaction identifier, NUL-terminated. */
  char gid[200];
  /** Exact native 32-bit XID as decimal text; not a global or full epoch-
   * qualified ID.
   */
  char xid[11];
  /** Exact database OID as decimal text, NUL-terminated. */
  char database_oid[11];
  /** Exact role OID as decimal text, NUL-terminated. */
  char role_oid[11];
} lc_xa_inventory_row;
/** Terminal operation snapshot. Diagnostics contain SQLSTATE only, never SQL,
 * parameters, credentials or raw native diagnostic text. */
typedef struct lc_xa_result {
  /** Copied durable identity/progress of the observed branch. */
  lc_xa_branch_record record;
  /** LC_XA_OUTCOME_* observation; not a coordinator decision or publication
   * authority.
   */
  int native_outcome;
  /** Number of populated inventory rows, at most 64. */
  size_t row_count;
  /** Bounded inventory page; only row_count entries are populated. */
  lc_xa_inventory_row rows[64];
  /** Five-character SQLSTATE plus NUL, or empty when unavailable; never raw
   * SQL diagnostics.
   */
  char sqlstate[6];
} lc_xa_result;
/** Explicit provider callbacks. Size and ABI are checked; retain linked
 * callbacks until every derived handle/borrow is released.
 */
typedef struct lc_xa_driver {
  /** Structure byte size used to validate the adapter boundary. */
  size_t size;
  /** Independently versioned adapter ABI; initialize with the matching
   * LC_XA_*_ABI constant.
   */
  unsigned long abi_version;
  /** Native provider kind, such as LC_XA_RESOURCE_POSTGRES. */
  int resource_kind;
  /** Copies provider configuration and returns an owned resource retaining
   * the supplied journal.
   */
  int (*open)(lc_xa_store *store, const void *configuration, void **resource,
              lc_error *error);
  /** Consumes a provider resource after draining its durability cleanup. */
  void (*close_blocking)(void *resource);
  /** Returns the resource's borrowed durable backend identity. */
  const lc_xa_branch_record *(*identity)(void *resource);
  /** Starts backend validation and durable resource identity registration. */
  int (*register_start)(void *resource, int64_t deadline_ms, void **operation,
                        lc_error *error);
  /** Starts and enrolls a branch under the supplied global transaction ID. */
  int (*branch_start)(void *resource, const char *txn_id, int64_t deadline_ms,
                      void **operation, lc_error *error);
  /** Starts durable prepare intent and native prepare after application
   * access ends.
   */
  int (*prepare_start)(void *branch, int64_t deadline_ms, void **operation,
                       lc_error *error);
  /** Starts commit or rollback matching the authoritative global decision. */
  int (*resolve_start)(void *branch, int commit, int64_t deadline_ms,
                       void **operation, lc_error *error);
  /** Starts exact-identity reconciliation/resolution of an enrolled branch. */
  int (*recover_start)(void *resource, const char *txn_id,
                       const char *branch_id, int commit, int64_t deadline_ms,
                       void **operation, lc_error *error);
  /** Observes the exact enrolled native outcome without choosing or applying
   * a decision.
   */
  int (*status_start)(void *resource, const char *txn_id, const char *branch_id,
                      int64_t deadline_ms, void **operation, lc_error *error);
  /** Starts a bounded diagnostic inventory; prefix membership is never
   * resolution authority.
   */
  int (*inventory_start)(void *resource, const char *after_gid, size_t limit,
                         int64_t deadline_ms, void **operation,
                         lc_error *error);
  /** Performs bounded nonblocking progress; LC_ERR_PENDING may also describe
   * a terminal result.
   */
  int (*advance)(void *operation, lc_error *error);
  /** Copies readiness and an absolute monotonic deadline for a nonterminal
   * operation.
   */
  int (*wait)(void *operation, lc_xa_wait *out, lc_error *error);
  /** Returns zero until terminal; otherwise copies the result. A successful
   * start/recover yields one owned branch. Other operations yield none. */
  int (*result)(void *operation, lc_xa_result *out, void **branch);
  /** Stops native work while preserving possibly prepared branches and
   * durable recovery intent.
   */
  int (*cancel)(void *operation, lc_error *error);
  /** Consumes an operation after in-flight journal cleanup completes. */
  void (*operation_close_blocking)(void *operation);
  /** Returns an owned validity token for the branch's exact native
   * connection.
   */
  int (*borrow)(void *branch, lc_xa_borrow **out, lc_error *error);
  /** Returns the branch's borrowed durable identity/progress snapshot. */
  const lc_xa_branch_record *(*branch_record)(void *branch);
  /** Releases provider branch state; prepared work remains durable for
   * recovery.
   */
  void (*branch_close)(void *branch);
} lc_xa_driver;
/** Owned explicitly registered participant resource, independent of native
 * client types in core.
 */
typedef struct lc_xa_resource lc_xa_resource;
/** Owned enrolled native branch; close its active operation before closing
 * the branch.
 */
typedef struct lc_xa_branch lc_xa_branch;
/** Owned participant operation; advance readiness until terminal, then close
 * explicitly.
 */
typedef struct lc_xa_operation lc_xa_operation;
/** The resource copies the function table and pins the journal. Its owning
 * process/thread drives native I/O. Zero deadlines select the provider's 30s
 * default; -1 selects infinity. Durability runs on the provider worker. */
int lc_xa_resource_open(lc_xa_store *store, const lc_xa_driver *driver,
                        const void *configuration, lc_xa_resource **out,
                        lc_error *error);
/** Borrowed identity while this live receiver remains retained. Backend fields
 * become authoritative after successful registration; copy before later work.
 */
const lc_xa_branch_record *lc_xa_resource_identity(lc_xa_resource *resource);
/** Starts provider capability/backend validation and durable registration. */
int lc_xa_resource_register_start(lc_xa_resource *resource, int64_t deadline_ms,
                                  lc_xa_operation **out, lc_error *error);
/** Starts/enrolls a branch under the caller's valid global ID. Successful
 * completion makes one branch available through lc_xa_operation_take_branch().
 */
int lc_xa_resource_branch_start(lc_xa_resource *resource, const char *txn_id,
                                int64_t deadline_ms, lc_xa_operation **out,
                                lc_error *error);
/** Recovers exact journal/backend identity without SQL replay. commit is zero
 * or one and must match the durable decision; success yields one owned branch.
 */
int lc_xa_resource_recover_start(lc_xa_resource *resource, const char *txn_id,
                                 const char *branch_id, int commit,
                                 int64_t deadline_ms, lc_xa_operation **out,
                                 lc_error *error);
/** Observes an enrolled native outcome without resolving or publishing work. */
int lc_xa_resource_status_start(lc_xa_resource *resource, const char *txn_id,
                                const char *branch_id, int64_t deadline_ms,
                                lc_xa_operation **out, lc_error *error);
/** Starts bounded diagnostic inventory (at most 64 rows). PostgreSQL uses GID
 * order, NULL for the first cursor and zero for its default limit of 64. */
int lc_xa_resource_inventory_start(lc_xa_resource *resource,
                                   const char *after_gid, size_t limit,
                                   int64_t deadline_ms, lc_xa_operation **out,
                                   lc_error *error);
/** Consumes the receiver and prevents new starts/borrows, including on retained
 * branches. Existing operations finish; retained branches remain inspectable
 * and closeable. Final release may wait for journal fsync, never network I/O.
 */
int lc_xa_resource_close_blocking(lc_xa_resource *resource, lc_error *error);
/** Starts durable prepare/YES voting. Drain protocol and release SQL
 * borrows/results first; enroll every member before the first prepare. */
int lc_xa_branch_prepare_start(lc_xa_branch *branch, int64_t deadline_ms,
                               lc_xa_operation **out, lc_error *error);
/** Resolves in the immutable decision's direction; commit must be zero or one.
 * Timeout/cancellation cannot reverse an already durable COMMIT. */
int lc_xa_branch_resolve_start(lc_xa_branch *branch, int commit,
                               int64_t deadline_ms, lc_xa_operation **out,
                               lc_error *error);
/** Opens an exact-connection borrow for application work on an open branch. */
int lc_xa_branch_borrow(lc_xa_branch *branch, lc_xa_borrow **out,
                        lc_error *error);
/** Borrowed branch record while this receiver is live; copy before later work.
 */
const lc_xa_branch_record *lc_xa_branch_record_get(lc_xa_branch *branch);
/** Consumes the caller reference. Native destruction waits for every pin,
 * including the creating operation after lc_xa_operation_take_branch(), to be
 * released. */
int lc_xa_branch_close(lc_xa_branch *branch, lc_error *error);
/** Advances bounded native work. LC_ERR_PENDING with a NULL result needs wait/
 * advance; a non-NULL result is terminal, including pending recovery. */
int lc_xa_operation_advance(lc_xa_operation *operation, lc_error *error);
/** Copies current readiness after nonterminal LC_ERR_PENDING. Honor the
 * absolute deadline; an already due deadline requires immediate advancement. */
int lc_xa_operation_wait(lc_xa_operation *operation, lc_xa_wait *out,
                         lc_error *error);
/** NULL until terminal (or for a foreign owner). The snapshot is borrowed until
 * operation close; a result alone does not prove a successful return status. */
const lc_xa_result *lc_xa_operation_result(lc_xa_operation *operation);
/** Transfer the successful start/recover branch exactly once. Unclaimed
 * branches are closed by operation_close_blocking; result never owns a shell.
 */
int lc_xa_operation_take_branch(lc_xa_operation *operation, lc_xa_branch **out,
                                lc_error *error);
/** Stops owned transmissions, preserving potentially prepared work for
 * recovery. Pending journal cleanup is completed by the blocking close. */
int lc_xa_operation_cancel(lc_xa_operation *operation, lc_error *error);
/** Consumes the operation, closes unclaimed branches and releases retained
 * branch/resource pins. May wait for durability cleanup, never native I/O. */
int lc_xa_operation_close_blocking(lc_xa_operation *operation, lc_error *error);

/** Managed participation in the existing Pouch transaction coordinator. Native
 * providers are linked explicitly and registered through lc_xa_driver. */
typedef struct lc_xa_coordinator lc_xa_coordinator;
/** Owned global transaction whose decision is persisted by the Pouch
 * coordinator.
 */
typedef struct lc_xa_transaction lc_xa_transaction;
/** Owned coordinator operation with nonblocking progress and explicit
 * durability cleanup.
 */
typedef struct lc_xa_coordinator_operation lc_xa_coordinator_operation;
/** Copied owner-lifetime and completed-history policy; zero fields select
 * their documented defaults.
 */
typedef struct lc_xa_coordinator_config {
  /** Zero selects 30 seconds. Renew during long application work; operations
   * renew automatically while advanced at their advertised readiness/deadline.
   */
  long owner_ttl_seconds;
  /** Completed coordinator history age. Zero selects 30 days; -1 disables
   * age retirement. Unfinished work and live owners are never retired. */
  long completed_retention_seconds;
  /** Optional completed-history count cap; zero means unlimited. Oldest
   * eligible records retire first, independently of outbox/receipt policies. */
  size_t completed_max_records;
} lc_xa_coordinator_config;
enum {
  /** No immutable global terminal decision has been recorded. */
  LC_XA_DECISION_UNDECIDED = 0,
  /** Global COMMIT is durable and must be completed, never reversed. */
  LC_XA_DECISION_COMMIT = 1,
  /** Global ABORT is durable and must be completed, never reversed. */
  LC_XA_DECISION_ABORT = 2
};
/** Borrowed operation snapshot. IDs exist before durable work starts and remain
 * available after errors/cancellation. decision is observed from .txns; applied
 * is true only after this operation proves every participant's application. */
typedef struct lc_xa_coordinator_result {
  /** Global transaction XID, distinct from a native database transaction ID. */
  char txn_id[LC_XID_STRING_SIZE];
  /** Stable registered resource XID used during enrollment and recovery. */
  char resource_id[LC_XID_STRING_SIZE];
  /** Nonzero when this operation is terminal; inspect its status and decision
   * independently.
   */
  int done;
  /** LC_XA_DECISION_* observation from the authoritative coordinator journal.
   */
  int decision;
  /** Nonzero only after every participant's application is proven by this
   * operation.
   */
  int applied;
  /** Number of complete histories removed by a retirement operation. */
  size_t retired_count;
  /** Five-character SQLSTATE plus NUL, or empty when unavailable; never raw
   * SQL diagnostics.
   */
  char sqlstate[6];
} lc_xa_coordinator_result;
/** Opens the local Pouch coordinator with zero/default policy when config is
 * NULL. Retains the client; native operations use the owning process/thread. */
int lc_xa_coordinator_open(lc_client *client,
                           const lc_xa_coordinator_config *config,
                           lc_xa_coordinator **out, lc_error *error);
/** Runs completed-history maintenance on the serialized journal worker. The
 * same policy runs automatically at open and once per minute while idle;
 * enabled count caps also run on final transaction release. Drive the
 * operation to observe cleanup errors. */
int lc_xa_coordinator_retire_start(lc_xa_coordinator *coordinator,
                                   int64_t deadline_ms,
                                   lc_xa_coordinator_operation **out,
                                   lc_error *error);
/** Adds an owning-process/thread reference. Child operations and transactions
 * also retain this receiver independently of the caller's reference. */
int lc_xa_coordinator_retain(lc_xa_coordinator *coordinator, lc_error *error);
/** Consumes one reference. Final release stops the journal worker and native
 * resources; may wait for journal fsync, never native network responses. */
int lc_xa_coordinator_close_blocking(lc_xa_coordinator *coordinator,
                                     lc_error *error);
/** Copies linked driver/configuration and starts backend registration. Close
 * each operation before starting another on this coordinator. */
int lc_xa_coordinator_register_start(lc_xa_coordinator *coordinator,
                                     const lc_xa_driver *driver,
                                     const void *configuration,
                                     int64_t deadline_ms,
                                     lc_xa_coordinator_operation **out,
                                     lc_error *error);
/** Diagnostic/manual resource receiver, borrowed while coordinator is retained.
 * The coordinator owns it: do not close it or overlap its operations with
 * managed operations. No driver configuration/credentials enter the journal. */
lc_xa_resource *lc_xa_coordinator_resource(lc_xa_coordinator *coordinator,
                                           const char *resource_id);
/** Creates a global ID before durable work. Successful completion transfers an
 * independently owned transaction through
 * lc_xa_coordinator_operation_take_transaction(). */
int lc_xa_coordinator_begin_start(lc_xa_coordinator *coordinator,
                                  int64_t deadline_ms,
                                  lc_xa_coordinator_operation **out,
                                  lc_error *error);
/** Adopts an existing local, unvoted Pouch transaction (for outbox-first use),
 * or reserves its known ID before its first participant. Existing decisions,
 * external branches and active ownership cannot be adopted. */
int lc_xa_coordinator_adopt_start(lc_xa_coordinator *coordinator,
                                  const char *txn_id, int64_t deadline_ms,
                                  lc_xa_coordinator_operation **out,
                                  lc_error *error);
/** Recovery never replays SQL. An active other owner yields LC_ERR_PENDING;
 * fenced abandoned undecided work selects ABORT. Missing registered resources
 * remain pending and cannot publish Pouch state/effects. */
int lc_xa_coordinator_recover_start(lc_xa_coordinator *coordinator,
                                    const char *txn_id, int64_t deadline_ms,
                                    lc_xa_coordinator_operation **out,
                                    lc_error *error);
/** Borrowed stable global ID while this transaction is retained; NULL for a
 * foreign owner. Copy the ID before releasing the final reference. */
const char *lc_xa_transaction_id(lc_xa_transaction *transaction);
/** Adds one owning-process/thread transaction reference. */
int lc_xa_transaction_retain(lc_xa_transaction *transaction, lc_error *error);
/** Releases a reference. Final close invalidates branch access and relinquishes
 * its owner lease, retaining durable identity/decision for recovery. */
int lc_xa_transaction_close_blocking(lc_xa_transaction *transaction,
                                     lc_error *error);
/** Enrolls a registered native resource under the same global ID. Enroll every
 * participant before commit seals the set; success does not vote YES. */
int lc_xa_transaction_enroll_start(lc_xa_transaction *transaction,
                                   const char *resource_id, int64_t deadline_ms,
                                   lc_xa_coordinator_operation **out,
                                   lc_error *error);
/** Pouch acquire on the journal worker, bound automatically to the global ID.
 * No blocking acquire wait is accepted. Configuration strings are copied. */
int lc_xa_transaction_acquire_start(lc_xa_transaction *transaction,
                                    const lc_acquire_req *request,
                                    int64_t deadline_ms,
                                    lc_xa_coordinator_operation **out,
                                    lc_error *error);
/** Opens a checked connection scope for an already enrolled native resource. */
int lc_xa_transaction_borrow(lc_xa_transaction *transaction,
                             const char *resource_id, lc_xa_borrow **out,
                             lc_error *error);
/** Refreshes the fenced owner lease during long application work. Drive this
 * operation before expiry; renewal does not prepare or resolve participants. */
int lc_xa_transaction_renew_start(lc_xa_transaction *transaction,
                                  int64_t deadline_ms,
                                  lc_xa_coordinator_operation **out,
                                  lc_error *error);
/** Seal enrollment, prepare all members, persist the immutable decision,
 * resolve native participants, then apply Pouch. A failed prepare selects
 * ABORT; errors after durable COMMIT retain COMMIT. Release every SQL
 * borrow/result first. */
int lc_xa_transaction_commit_start(lc_xa_transaction *transaction,
                                   int64_t deadline_ms,
                                   lc_xa_coordinator_operation **out,
                                   lc_error *error);
/** Selects ABORT for undecided work and resolves/applies in the durable
 * direction. An existing COMMIT is preserved and can never become ABORT. */
int lc_xa_transaction_rollback_start(lc_xa_transaction *transaction,
                                     int64_t deadline_ms,
                                     lc_xa_coordinator_operation **out,
                                     lc_error *error);
/** Advances bounded work. Inspect result->done on LC_ERR_PENDING: zero needs
 * wait/advance; nonzero is terminal and may still need subsequent recovery. */
int lc_xa_coordinator_operation_advance(lc_xa_coordinator_operation *operation,
                                        lc_error *error);
/** Wait after LC_ERR_PENDING unless result.done is true. A negative fd with an
 * already due deadline means advance immediately; do not add a polling delay.
 */
int lc_xa_coordinator_operation_wait(lc_xa_coordinator_operation *operation,
                                     lc_xa_wait *out, lc_error *error);
/** Borrowed snapshot available before first advancement, including the global
 * ID for begin/adopt/recover. Inspect done and the advance status together;
 * copy fields before closing the operation. NULL for a foreign owner. */
const lc_xa_coordinator_result *
lc_xa_coordinator_operation_result(lc_xa_coordinator_operation *operation);
/** Successful begin/adopt/recovery transfers its preallocated transaction once.
 */
int lc_xa_coordinator_operation_take_transaction(
    lc_xa_coordinator_operation *operation, lc_xa_transaction **out,
    lc_error *error);
/** Successful acquire transfers its independently owned lease once. */
int lc_xa_coordinator_operation_take_lease(
    lc_xa_coordinator_operation *operation, lc_lease **out, lc_error *error);
/** Stops further native transmission. In-flight journal work is observed before
 * reclamation; cancellation never changes durable COMMIT or invents rollback.
 */
int lc_xa_coordinator_operation_cancel(lc_xa_coordinator_operation *operation,
                                       lc_error *error);
/** Consumes the operation and releases any unclaimed transaction/lease. Stops
 * unfinished native work and waits for journal cleanup, preserving the durable
 * decision and recoverable identity. */
int lc_xa_coordinator_operation_close_blocking(
    lc_xa_coordinator_operation *operation, lc_error *error);

/** Request used to acquire the TC leader lease. */
typedef struct lc_tc_lease_acquire_req {
  /** Non-empty identity of the leadership candidate. */
  const char *candidate_id;
  /** Endpoint advertised for the candidate. */
  const char *candidate_endpoint;
  /** Candidate term; stale terms are rejected. Pouch requires a newer term
   * after expiry/release; only its live current owner can reuse the same term.
   */
  lc_tc_term term;
  /** Requested leader-lease TTL in milliseconds. */
  long ttl_ms;
} lc_tc_lease_acquire_req;

/** Result returned by a TC leader lease acquisition attempt. */
typedef struct lc_tc_lease_acquire_res {
  /** Non-zero when the candidate acquired leadership. */
  int granted;
  /** Owned identity of the leader observed after the request. */
  char *leader_id;
  /** Owned endpoint of the leader observed after the request. */
  char *leader_endpoint;
  /** Current transaction-coordinator term. */
  lc_tc_term term;
  /** Leader-lease expiry as a Unix timestamp. */
  lc_unix_seconds expires_at_unix;
  /** Owned server correlation id for the acquisition response. */
  char *correlation_id;
} lc_tc_lease_acquire_res;

/** Request used to renew the TC leader lease. */
typedef struct lc_tc_lease_renew_req {
  /** Identity of the current leader. */
  const char *leader_id;
  /** Term held by the current leader. */
  lc_tc_term term;
  /** Requested renewed TTL in milliseconds. */
  long ttl_ms;
} lc_tc_lease_renew_req;

/** Result returned by TC leader lease renewal. */
typedef struct lc_tc_lease_renew_res {
  /** Non-zero when the leader lease was renewed. */
  int renewed;
  /** Owned identity of the leader observed after the request. */
  char *leader_id;
  /** Owned endpoint of the leader observed after the request. */
  char *leader_endpoint;
  /** Current transaction-coordinator term. */
  lc_tc_term term;
  /** Renewed leader-lease expiry as a Unix timestamp. */
  lc_unix_seconds expires_at_unix;
  /** Owned server correlation id for the renewal response. */
  char *correlation_id;
} lc_tc_lease_renew_res;

/** Request used to release the TC leader lease. */
typedef struct lc_tc_lease_release_req {
  /** Identity of the leader releasing the lease. */
  const char *leader_id;
  /** Term held by the releasing leader. */
  lc_tc_term term;
} lc_tc_lease_release_req;

/** Result returned by TC leader lease release. */
typedef struct lc_tc_lease_release_res {
  /** Non-zero when the leader lease was released. */
  int released;
  /** Owned server correlation id for the release response. */
  char *correlation_id;
} lc_tc_lease_release_res;

/** Snapshot of the current transaction coordinator leader. */
typedef struct lc_tc_leader_res {
  /** Owned current leader identity; `NULL` or empty when no leader is active.
   * Pouch returns an owned empty string. */
  char *leader_id;
  /** Owned current leader endpoint; `NULL` or empty when no leader is active.
   * Pouch returns an owned empty string. */
  char *leader_endpoint;
  /** Current transaction-coordinator term. Pouch preserves the last term even
   * with no active leader; advance it for a new leadership epoch. */
  lc_tc_term term;
  /** Current leader-lease expiry as a Unix timestamp. */
  lc_unix_seconds expires_at_unix;
  /** Owned server correlation id for the leader response. */
  char *correlation_id;
} lc_tc_leader_res;

/** Request used to announce the current node into the TC cluster. */
typedef struct lc_tc_cluster_announce_req {
  /** Endpoint advertised by this node. */
  const char *self_endpoint;
} lc_tc_cluster_announce_req;

/** Cluster membership state returned by TC cluster operations. */
typedef struct lc_tc_cluster_res {
  /** Owned list of currently registered cluster endpoints. */
  lc_string_list endpoints;
  /** Last membership update as a Unix timestamp. */
  lc_unix_seconds updated_at_unix;
  /** Membership expiry as a Unix timestamp, or zero when not exposed. */
  lc_unix_seconds expires_at_unix;
  /** Owned server correlation id for the cluster response. */
  char *correlation_id;
} lc_tc_cluster_res;

/** Request used to register a resource manager endpoint for a backend hash. */
typedef struct lc_tc_rm_register_req {
  /** Non-empty stable backend identity hash. */
  const char *backend_hash;
  /** Resource-manager endpoint to register. */
  const char *endpoint;
} lc_tc_rm_register_req;

/** Request used to unregister a resource manager endpoint for a backend hash.
 */
typedef struct lc_tc_rm_unregister_req {
  /** Stable backend identity hash. */
  const char *backend_hash;
  /** Resource-manager endpoint to unregister. */
  const char *endpoint;
} lc_tc_rm_unregister_req;

/** Result returned by TC RM register and unregister operations. */
typedef struct lc_tc_rm_res {
  /** Owned backend identity hash affected by the operation. */
  char *backend_hash;
  /** Owned current endpoint list for that backend. */
  lc_string_list endpoints;
  /** Last registry update as a Unix timestamp. */
  lc_unix_seconds updated_at_unix;
  /** Owned server correlation id for the registry response. */
  char *correlation_id;
} lc_tc_rm_res;

/** Single TC RM backend entry returned by list operations. */
typedef struct lc_tc_rm_backend {
  /** Owned backend identity hash. */
  char *backend_hash;
  /** Owned endpoints currently registered for the backend. */
  lc_string_list endpoints;
  /** Last update for this backend as a Unix timestamp. */
  lc_unix_seconds updated_at_unix;
} lc_tc_rm_backend;

/** Full TC RM backend listing. */
typedef struct lc_tc_rm_list_res {
  /** Owned array of backend registry entries. */
  lc_tc_rm_backend *backends;
  /** Number of entries in `backends`. */
  size_t backend_count;
  /** Last update for the returned registry view. */
  lc_unix_seconds updated_at_unix;
  /** Owned server correlation id for the listing response. */
  char *correlation_id;
} lc_tc_rm_list_res;

/** Request used to enqueue a streamed queue payload. */
typedef struct lc_enqueue_req {
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *ns;
  /** Queue name. */
  const char *queue;
  /** Initial delivery delay in seconds. The resulting Unix timestamp must fit
   * in `lc_unix_seconds`. */
  long delay_seconds;
  /** Visibility timeout granted to the consumer on dequeue. The resulting
   * Unix timestamp must fit in `lc_unix_seconds`. */
  long visibility_timeout_seconds;
  /** Message TTL in seconds. The resulting Unix timestamp must fit in
   * `lc_unix_seconds`. */
  long ttl_seconds;
  /** Maximum delivery attempts before the server gives up. */
  int max_attempts;
  /** Payload content type recorded with the message. */
  const char *content_type;
} lc_enqueue_req;

/** Result returned by a successful queue enqueue operation. */
typedef struct lc_enqueue_res {
  /** Owned namespace containing the message. */
  char *ns;
  /** Owned queue name. */
  char *queue;
  /** Owned server-issued message identifier. */
  char *message_id;
  /** Initial delivery-attempt count. */
  int attempts;
  /** Maximum delivery attempts configured for the message. */
  int max_attempts;
  /** Initial processing-failure attempt count. */
  int failure_attempts;
  /** Earliest delivery time as a Unix timestamp. */
  lc_unix_seconds not_visible_until_unix;
  /** Returned visibility timeout. Values outside `long` are rejected. */
  long visibility_timeout_seconds;
  /** Returned payload byte count. Values outside `long` are rejected. */
  long payload_bytes;
  /** Owned server correlation id for the enqueue response. */
  char *correlation_id;
} lc_enqueue_res;

/** Request used to dequeue queue messages. */
typedef struct lc_dequeue_req {
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *ns;
  /** Queue name. */
  const char *queue;
  /**
   * Non-empty logical consumer/owner identifier required for direct delivery.
   * `lc_consumer_service` may synthesize this value from its configuration.
   */
  const char *owner;
  /** Optional transaction identifier to bind the dequeue into. */
  const char *txn_id;
  /** Visibility timeout granted when a message is delivered. The resulting
   * Unix timestamp must fit in `lc_unix_seconds`. */
  long visibility_timeout_seconds;
  /** Long-poll wait in seconds before returning no message. */
  long wait_seconds;
  /** Maximum number of messages requested in one server page. */
  int page_size;
  /** Queue cursor used to continue from a previous position. */
  const char *start_after;
} lc_dequeue_req;

/** Request used to fetch queue-level statistics. */
typedef struct lc_queue_stats_req {
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *ns;
  /** Queue name. */
  const char *queue;
} lc_queue_stats_req;

/** Queue-level metrics and head-of-line metadata. */
typedef struct lc_queue_stats_res {
  /** Namespace containing the queue. */
  char *ns;
  /** Queue name. */
  char *queue;
  /** Number of consumers currently waiting. */
  int waiting_consumers;
  /** Number of pending delivery candidates observed by the backend. */
  int pending_candidates;
  /** Total registered consumers observed by the backend. */
  int total_consumers;
  /** Non-zero when a queue watcher is active. */
  int has_active_watcher;
  /** Non-zero when a message is immediately available. */
  int available;
  /** Message id at the head of the queue, when present. */
  char *head_message_id;
  /** Enqueue timestamp for the head message. */
  lc_unix_seconds head_enqueued_at_unix;
  /** Visibility deadline for the head message. */
  lc_unix_seconds head_not_visible_until_unix;
  /** Age of the head message in seconds. */
  long head_age_seconds;
  /** Server correlation id for the stats response. */
  char *correlation_id;
} lc_queue_stats_res;

/**
 * Batch result returned by `dequeue_batch()`.
 *
 * The result owns every message until `lc_dequeue_batch_cleanup()` is called.
 * A terminal `ack()` or `nack()` on a batch message does not close that handle;
 * the batch cleanup releases it.
 */
typedef struct lc_dequeue_batch_res {
  /** Delivered messages in dequeue order. */
  lc_message **messages;
  /** Number of entries in `messages`. */
  size_t count;
} lc_dequeue_batch_res;

/** Stable queue message identity used by client-level queue operations. */
typedef struct lc_message_ref {
  /** Namespace of the message. */
  const char *ns;
  /** Queue containing the message. */
  const char *queue;
  /** Server-issued message identifier. */
  const char *message_id;
  /** Delivery lease identifier for the queue message. */
  const char *lease_id;
  /** Transaction identifier associated with the message, if any. */
  const char *txn_id;
  /** Current fencing token for the message delivery lease. */
  long fencing_token;
  /** Current queue metadata etag. */
  const char *meta_etag;
  /** Current state etag when the message carries state. */
  const char *state_etag;
  /** Associated state lease identifier when the message carries state. */
  const char *state_lease_id;
  /** Fencing token for the associated state lease. */
  long state_fencing_token;
} lc_message_ref;

/** Result returned by queue ack operations. */
typedef struct lc_ack_res {
  /** Non-zero when the message was acknowledged. */
  int acked;
  /** Server correlation id for the ack response. */
  char *correlation_id;
} lc_ack_res;

/** Client-level ack operation on an existing queue message reference. */
typedef struct lc_ack_op {
  /** Queue message identity to acknowledge. */
  lc_message_ref message;
} lc_ack_op;

/**
 * Public nack intent for queue redelivery semantics.
 *
 * This is intentionally an enum in the SDK surface so callers cannot send
 * arbitrary transport strings by mistake. The transport still serializes the
 * server contract values (`failure` or `defer`) internally.
 */
typedef enum lc_nack_intent {
  /**
   * Default nack behavior.
   *
   * This maps to the server's `failure` intent and counts as a processing
   * failure for queue retry budgeting.
   */
  LC_NACK_INTENT_UNSPECIFIED = 0,
  /**
   * Processing failure.
   *
   * Use this when the handler attempted to process the message and failed.
   * This maps to the server's `failure` transport value and consumes the
   * message's failure budget.
   */
  LC_NACK_INTENT_FAILURE = 1,
  /**
   * Intentional deferral.
   *
   * Use this when processing should be retried later without counting as a
   * handler failure. This maps to the server's `defer` transport value and
   * does not consume the message's failure budget.
   */
  LC_NACK_INTENT_DEFER = 2
} lc_nack_intent;

/** Request used to negatively acknowledge and optionally requeue a message. */
typedef struct lc_nack_req {
  /** Delay in seconds before the message becomes visible again. The resulting
   * Unix timestamp must fit in `lc_unix_seconds`. */
  long delay_seconds;
  /**
   * Redelivery intent for the nack.
   *
   * `LC_NACK_INTENT_UNSPECIFIED` defaults to `LC_NACK_INTENT_FAILURE`.
   * Use `LC_NACK_INTENT_DEFER` for deliberate retry-later behavior that should
   * not count against failure-based retry limits.
   */
  lc_nack_intent intent;
  /**
   * Optional JSON object describing the last processing error.
   *
   * This is meaningful for `LC_NACK_INTENT_FAILURE`. Leave it `NULL` for
   * `LC_NACK_INTENT_DEFER` unless you intentionally want to record failure
   * detail alongside the nack.
   */
  const char *last_error_json;
} lc_nack_req;

/** Client-level nack operation on an existing queue message reference. */
typedef struct lc_nack_op {
  /** Queue message identity to negatively acknowledge. */
  lc_message_ref message;
  /** Delay in seconds before the message becomes visible again. The resulting
   * Unix timestamp must fit in `lc_unix_seconds`. */
  long delay_seconds;
  /**
   * Redelivery intent for the nack.
   *
   * `LC_NACK_INTENT_UNSPECIFIED` defaults to `LC_NACK_INTENT_FAILURE`.
   * Use `LC_NACK_INTENT_DEFER` to requeue intentionally without consuming the
   * message's failure budget.
   */
  lc_nack_intent intent;
  /**
   * Optional JSON object describing the last processing error.
   *
   * This is typically set for `LC_NACK_INTENT_FAILURE` and omitted for
   * `LC_NACK_INTENT_DEFER`.
   */
  const char *last_error_json;
} lc_nack_op;

/** Result returned by queue nack operations. */
typedef struct lc_nack_res {
  /** Non-zero when the message was returned to the delivery lifecycle. */
  int requeued;
  /** Owned updated queue metadata etag. */
  char *meta_etag;
  /** Owned server correlation id for the nack response. */
  char *correlation_id;
} lc_nack_res;

/** Request used to extend queue message visibility. */
typedef struct lc_extend_req {
  /** Additional visibility time in seconds. The resulting Unix timestamp must
   * fit in `lc_unix_seconds`. */
  long extend_by_seconds;
} lc_extend_req;

/** Client-level extend operation on an existing queue message reference. */
typedef struct lc_extend_op {
  /** Queue message identity to extend. */
  lc_message_ref message;
  /** Additional visibility time in seconds. The resulting Unix timestamp must
   * fit in `lc_unix_seconds`. */
  long extend_by_seconds;
} lc_extend_op;

/** Result returned by queue visibility extension operations. */
typedef struct lc_extend_res {
  /** New delivery lease expiry as a Unix timestamp. */
  lc_unix_seconds lease_expires_at_unix;
  /** Effective visibility timeout in seconds. */
  long visibility_timeout_seconds;
  /** Updated queue metadata etag. */
  char *meta_etag;
  /** New associated state-lease expiry when state is attached. */
  lc_unix_seconds state_lease_expires_at_unix;
  /** Server correlation id for the extend response. */
  char *correlation_id;
} lc_extend_res;

/** Request used to watch queue availability changes. */
typedef struct lc_watch_queue_req {
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *ns;
  /** Queue name. */
  const char *queue;
} lc_watch_queue_req;

/** Event delivered to queue watch handlers. */
typedef struct lc_watch_event {
  /** Namespace containing the watched queue. */
  char *ns;
  /** Queue name. */
  char *queue;
  /** Non-zero when a message is available. */
  int available;
  /** Message id at the queue head, when present. */
  char *head_message_id;
  /** Timestamp when the availability state changed. */
  lc_unix_seconds changed_at_unix;
  /** Server correlation id for the watch event. */
  char *correlation_id;
} lc_watch_event;

/**
 * Callback invoked for each queue watch event.
 *
 * `event` and all of its fields are borrowed for the invocation. Return
 * non-zero to continue watching, or zero to stop. Populate `error` when
 * stopping due to a failure.
 */
typedef int (*lc_watch_handler_fn)(void *context, const lc_watch_event *event,
                                   lc_error *error);

/** Queue watch callback registration. */
typedef struct lc_watch_handler {
  /** Required event callback; returning zero stops the watch. */
  lc_watch_handler_fn handle;
  /** Opaque borrowed context passed to `handle`. */
  void *context;
} lc_watch_handler;

/**
 * Queue consumer callback registration used by subscribe flows.
 *
 * The message is borrowed for the callback. Before returning `LC_OK`, the
 * callback must terminalize it with `message->ack()` or `message->nack()`.
 * Returning an error while the message remains open causes the subscribe flow
 * to attempt a failure nack before closing the local handle.
 */
typedef struct lc_consumer {
  /** Required delivery callback; return `LC_OK` only after ack or nack. */
  int (*handle)(void *context, lc_message *message, lc_error *error);
  /** Opaque borrowed context passed to `handle`. */
  void *context;
} lc_consumer;

/**
 * Restart policy for one managed queue consumer loop.
 *
 * Defaults match the Go SDK `StartConsumer` behavior: three immediate retries,
 * then exponential backoff starting at 250ms with multiplier 2.0 and a
 * 5-minute cap.
 */
typedef struct lc_consumer_restart_policy {
  /** Number of consecutive failures retried without delay. */
  int immediate_retries;
  /** Initial delayed retry interval in milliseconds. */
  long base_delay_ms;
  /** Maximum retry delay in milliseconds. */
  long max_delay_ms;
  /** Exponential growth multiplier applied after immediate retries. */
  double multiplier;
  /** Jitter applied as +/- `jitter_ms` around the computed delay. */
  long jitter_ms;
  /** Maximum consecutive failures before the consumer stops. Zero retries
   * forever. */
  int max_failures;
} lc_consumer_restart_policy;

/**
 * Delivery object passed to managed consumer callbacks.
 *
 * All pointers are borrowed for the duration of the callback only. The handler
 * may leave the delivery open and return `LC_OK`; the managed consumer will
 * then acknowledge it automatically on success. If the handler returns a
 * non-`LC_OK` error, the managed consumer treats it as a failure, negatively
 * acknowledges the open delivery after the callback unwinds, reports the
 * error, and resumes consuming without counting that delivery against the
 * service restart budget.
 * Explicit `message->ack()`/`message->nack()` remain available for handlers
 * that want to terminalize the delivery themselves. When `state` is non-NULL,
 * it is the lease handle associated with the delivery and is owned by
 * `message`.
 */
typedef struct lc_consumer_message {
  /** Active SDK client for this consumer loop. Safe to reuse inside the
   * callback. */
  lc_client *client;
  /** Borrowed SDK logger for this callback. Always non-NULL; do not destroy it.
   * Its base logger follows `lc_client_config.logger` ownership rules. */
  pslog_logger *logger;
  /** Resolved consumer name. Defaults to the queue name when omitted. */
  const char *name;
  /** Queue currently being consumed. */
  const char *queue;
  /** Non-zero when this delivery includes an attached state lease. */
  int with_state;
  /** Delivered queue message handle. */
  lc_message *message;
  /** Attached state lease, or `NULL` for stateless consumers. */
  lc_lease *state;
} lc_consumer_message;

/**
 * Recoverable consumer service error reported to `on_error`.
 *
 * The `cause` object is borrowed for the duration of the callback only.
 */
typedef struct lc_consumer_error {
  /** Resolved consumer name. */
  const char *name;
  /** Queue whose loop failed. */
  const char *queue;
  /** Non-zero when the failing loop used `dequeue_with_state()`. */
  int with_state;
  /** Current consecutive loop failure count. Zero means the error was scoped to
   * one delivery and did not consume the service restart budget. */
  int attempt;
  /** Delay before the next retry, in milliseconds. Zero means immediate retry
   * or a delivery-level error with no service-level backoff. */
  long restart_in_ms;
  /** Error that caused the failure. */
  const lc_error *cause;
} lc_consumer_error;

/**
 * Lifecycle transition reported for one managed consumer loop attempt.
 *
 * The optional `error` pointer is borrowed for the duration of the callback.
 */
typedef struct lc_consumer_lifecycle_event {
  /** Resolved consumer name. */
  const char *name;
  /** Queue whose loop transitioned. */
  const char *queue;
  /** Non-zero when the loop uses `dequeue_with_state()`. */
  int with_state;
  /** One-based attempt sequence number for this consumer loop. */
  int attempt;
  /** Terminal error for the attempt, or `NULL` on clean start/stop. */
  const lc_error *error;
} lc_consumer_lifecycle_event;

/**
 * One managed consumer loop configuration used by `lc_consumer_service`.
 *
 * The service will default `request.owner` when it is empty, default
 * `request.ns` from the client when it is empty, and then run a
 * long-lived streaming subscribe loop that calls `handle()` once per
 * delivery.
 */
typedef struct lc_consumer_config {
  /** Logical consumer name used in events and owner defaulting. */
  const char *name;
  /** Dequeue options for this consumer. `request.queue` is required. */
  lc_dequeue_req request;
  /**
   * Number of parallel subscribe workers for this consumer config.
   *
   * Each worker runs its own subscribe loop and invokes `handle()`
   * sequentially for the deliveries it receives. Zero defaults to `1`.
   */
  size_t worker_count;
  /** Non-zero to use `dequeue_with_state()` and populate
   * `lc_consumer_message.state`. */
  int with_state;
  /**
   * Handles one delivery.
   *
   * Return `LC_OK` after successfully processing the delivery. If the handler
   * leaves the delivery open, the managed consumer acknowledges it
   * automatically on success and negatively acknowledges it automatically on
   * failure before reporting the delivery error and resuming consumption.
   * Explicit
   * `message->nack()`/`message->ack()` are still available when a handler wants
   * to terminalize the delivery itself before returning.
   */
  int (*handle)(void *context, lc_consumer_message *message, lc_error *error);
  /**
   * Observes a failed delivery or failed loop.
   *
   * Delivery-level errors are reported with `attempt == 0` and do not consume
   * the service restart budget. Loop/runtime failures are reported with a
   * positive `attempt` and may back off according to `restart_policy`.
   * Return `LC_OK` to continue. Returning a non-`LC_OK` code stops the service
   * and returns that error from `run()`/`wait()`.
   */
  int (*on_error)(void *context, const lc_consumer_error *event,
                  lc_error *error);
  /** Called when one consumer attempt starts. */
  void (*on_start)(void *context, const lc_consumer_lifecycle_event *event);
  /** Called when one consumer attempt stops. */
  void (*on_stop)(void *context, const lc_consumer_lifecycle_event *event);
  /** Opaque user context passed to all callbacks for this consumer. */
  void *context;
  /** Restart behavior after loop/runtime transport failures. */
  lc_consumer_restart_policy restart_policy;
} lc_consumer_config;

/**
 * Settings used to create a managed queue consumer service.
 *
 * The service owns a deep copy of these configs after successful creation.
 */
typedef struct lc_consumer_service_config {
  /** Array of consumer loop definitions. */
  const lc_consumer_config *consumers;
  /** Number of entries in `consumers`. */
  size_t consumer_count;
} lc_consumer_service_config;

/** Settings for one inbox/outbox namespace.
 *
 * `ns` is required. All zero duration/count fields use the
 * documented defaults; negative values other than documented retention
 * opt-outs, invalid retry ranges, and claim TTLs that cannot produce an
 * `lc_unix_seconds` deadline are rejected during
 * `lc_client_new_outbox()`. Dispatcher resources are created only by
 * `lc_outbox_dispatcher_get_or_start()`.
 * Automatic retention requires a live dispatcher. All producers and
 * dispatchers for one namespace must use the same settings. Explicit
 * retention settings are rejected for remote lockd.
 * Retention is asynchronous: a positive count cap can reclaim before expiry.
 * Reclamation ends duplicate/idempotency protection and command status lookup.
 * Time-policy changes affect future terminal transitions only; existing marker
 * deadlines remain unchanged. Records written before retention, or while a
 * type is disabled, are not backfilled. See docs/pouch-terminal-retention.md.
 */
typedef struct lc_outbox_config {
  /** Required namespace that contains both inbox and outbox records. */
  const char *ns;
  /** Optional lease owner for component-owned records and claims. Defaults to
   * `"lockdc-outbox"`. */
  const char *owner;
  /** TTL for outbox transaction participants; zero defaults to 30 seconds. */
  long transaction_ttl_seconds;
  /** TTL for an outbox claim; zero defaults to five minutes. */
  long claim_ttl_seconds;
  /** Maximum delivery attempts including the first; zero defaults to 100. */
  int max_attempts;
  /** Bounded direct-key notification capacity; zero defaults to 1024. */
  size_t notification_capacity;
  /** Initial automatic retry cap; zero defaults to one second. */
  long retry_initial_delay_seconds;
  /** Maximum automatic retry cap; zero defaults to fifteen minutes. */
  long retry_max_delay_seconds;
  /** Maximum host-requested retry delay; zero defaults to one hour. */
  long host_retry_delay_max_seconds;
  /**
   * Infrequent durable-recovery cadence. Zero defaults to five minutes for a
   * remote endpoint and disables routine scans for local Pouch. Startup
   * recovery is requested by the first blocking consumer demand or explicit
   * reconcile call; overflow reconciliation is always scheduled. Set a
   * positive value for shared-root Pouch dispatch. A positive interval must
   * produce a supported Unix
   * timestamp from the outbox creation time.
   */
  long recovery_interval_seconds;
  /**
   * Maximum duration of one remote dispatcher request; zero inherits an
   * explicit root-client timeout or defaults to 30 seconds. Pouch dispatch
   * uses its retained local session.
   */
  long shutdown_timeout_ms;
  /** When non-zero, replay durable dead letters through startup reconciliation.
   */
  int replay_dead_letters_on_startup;
  /**
   * Pouch-only automatic dead-letter retention. Zero defaults to 30 days;
   * `-1` disables automatic retention explicitly; a positive value is the
   * retention period in seconds. Other negative values are invalid. An
   * explicit value is rejected for remote lockd rather than ignored.
   */
  long dead_letter_retention_seconds;
  /**
   * Pouch-only retained dead-letter envelope cap. Zero defaults to 1,024;
   * `SIZE_MAX` disables this capacity bound. An explicit value is rejected for
   * remote lockd rather than ignored.
   */
  size_t dead_letter_max_count;
  /**
   * Pouch-only logical retained-byte cap, including envelope payload and a
   * stateful checkpoint when present. Zero defaults to 64 MiB;
   * `~(uint64_t)0U` disables this capacity bound. An explicit value is rejected
   * for remote lockd rather than ignored.
   */
  uint64_t dead_letter_max_bytes;
  /**
   * Pouch-only completed outbox envelope retention, measured from completion.
   * Zero selects 30 days; positive values select seconds; `-1` disables
   * reclamation for this type, including its count cap. Other negatives are
   * invalid. Reclamation ends duplicate protection for the effect identity.
   */
  long completed_retention_seconds;
  /**
   * Optional Pouch-only completed envelope cap per namespace. Zero or
   * `SIZE_MAX` leaves the count unbounded. A positive cap permits reclamation
   * before the time window expires, ending duplicate protection early.
   */
  size_t completed_max_count;
  /**
   * Pouch-only accepted inbox receipt retention. Zero selects 30 days;
   * positive values select seconds; `-1` disables reclamation and its count
   * cap. Reclamation permits a redelivered message identity to execute again.
   */
  long inbox_retention_seconds;
  /** Optional Pouch-only inbox receipt cap per namespace; zero or `SIZE_MAX`
   * is unbounded. A positive cap permits early reclamation and ends duplicate
   * protection early. */
  size_t inbox_max_count;
  /**
   * Pouch-only completed/failed command receipt retention from its terminal
   * timestamp. Zero selects 30 days; positive values select seconds; `-1`
   * disables reclamation and its count cap. Reclamation also removes owned
   * result attachments and ends idempotency and status lookup for the identity.
   */
  long command_retention_seconds;
  /** Optional Pouch-only terminal command cap per namespace; zero or `SIZE_MAX`
   * is unbounded. A positive cap permits early reclamation and ends idempotency
   * protection early. */
  size_t command_max_count;
} lc_outbox_config;

/** Streaming representation requested from `export_dead_letters()`. */
enum {
  /** Write one JSON array containing every exported dead-letter envelope. */
  LC_DEAD_LETTER_EXPORT_JSON = 1,
  /** Write one complete JSON envelope followed by a newline per record. */
  LC_DEAD_LETTER_EXPORT_JSONL = 2
};

/** Bounded export settings for outbox dead-letter envelopes. */
typedef struct lc_dead_letter_export_opts {
  /** `LC_DEAD_LETTER_EXPORT_JSON` or `LC_DEAD_LETTER_EXPORT_JSONL`. */
  int format;
  /** Maximum records to export; zero uses the outbox notification capacity.
   * Values larger than `LONG_MAX` are rejected with `LC_ERR_INVALID`.
   */
  size_t limit;
} lc_dead_letter_export_opts;

/** Result of a streamed dead-letter export. */
typedef struct lc_dead_letter_export_res {
  /** Number of complete envelope documents written to the caller's sink. */
  size_t exported;
} lc_dead_letter_export_res;

/** Cheap process-local outbox observability snapshot. */
typedef struct lc_outbox_stats {
  /** Non-zero while this dispatcher may accept demand for new work. */
  int running;
  /** Current bounded direct-key candidate backlog. */
  size_t pending_candidates;
  /** Current bounded delayed-retry wake backlog. */
  size_t delayed_wakes;
  /** Number of raw consumers currently waiting in `next()`. */
  size_t waiting_consumers;
  /** Monotonic successful key notifications in this outbox process. */
  uint64_t direct_notifications;
  /** Monotonic notification overflows repaired through reconciliation. */
  uint64_t notification_overflows;
  /** Monotonic indexed reconciliation sweeps started by this process. */
  uint64_t recovery_queries;
  /** Monotonic outbox candidates rediscovered by reconciliation. */
  uint64_t recovered_claims;
  /** Monotonic claims that could not become a host job. */
  uint64_t claim_losses;
  /** Monotonic payload streaming failures observed from claimed jobs. */
  uint64_t payload_open_failures;
  /** Monotonic Pouch retention markers safely consumed by maintenance. */
  uint64_t dead_letter_reclaims;
  /** Monotonic Pouch retention maintenance failures retained for retry. */
  uint64_t dead_letter_reclaim_failures;
  /** Monotonic completed/inbox/command retention markers safely retired,
   * including stale markers whose parent no longer matches. */
  uint64_t terminal_reclaims;
  /** Monotonic terminal retention failures preserved for retry. */
  uint64_t terminal_reclaim_failures;
  /** Owned latest dispatcher error text, or `NULL`. */
  char *last_error;
} lc_outbox_stats;

/** Immutable envelope and routing data for one durable outbox effect. The
 * combined byte length of its string fields must not exceed
 * `LC_OUTBOX_MAX_ENVELOPE_BYTES`. */
typedef struct lc_outbox_entry {
  /** Required stable business-operation identity. */
  const char *operation_id;
  /** Required stable identity of this effect within `operation_id`. */
  const char *effect_id;
  /** Required idempotency key sent unchanged to the foreign effect system. */
  const char *effect_key;
  /**
   * Required immutable caller-generated digest that binds this entry to its
   * exact payload bytes. The format is application-defined. Retries with the
   * same outbox identity must provide the same digest.
   */
  const char *payload_digest;
  /** Optional immediate durable cause (command or source message identity). */
  const char *causation_id;
  /** Required caller-defined routing kind. */
  const char *kind;
  /** Optional application message-schema version. */
  const char *schema_version;
  /** Required caller-defined transport destination. */
  const char *destination;
  /** Optional payload media type; defaults to `application/octet-stream`. */
  const char *content_type;
  /** Optional immutable JSON object string containing envelope headers. */
  const char *headers_json;
  /** Optional immutable trace-propagation value. */
  const char *trace_context;
} lc_outbox_entry;

/** Immutable identity for one received source message. */
typedef struct lc_inbox_message {
  /** Required stable identifier for this local consumer. */
  const char *consumer_id;
  /** Required stable source technology or event-kind identifier. */
  const char *source_kind;
  /** Required stable identity of the source stream or producer. */
  const char *source_id;
  /** Required source-assigned message identity. */
  const char *message_id;
  /** Optional immutable digest used to reject conflicting redeliveries. */
  const char *payload_digest;
  /** Optional cross-service business-operation identity. */
  const char *operation_id;
} lc_inbox_message;

/** Immutable identity selecting one durable API-command receipt. */
typedef struct lc_command_identity {
  /** Required trusted service, tenant, or authorization scope. */
  const char *scope;
  /** Required stable operation name. */
  const char *command_type;
  /** Required caller retry identity within `scope` and `command_type`. */
  const char *idempotency_key;
} lc_command_identity;

/** Command identity plus immutable binding data accepted by the outbox. */
typedef struct lc_command_request {
  /** Required receipt identity. */
  lc_command_identity identity;
  /**
   * Generates an XID-backed idempotency key when non-zero. In this explicit
   * mode `identity.idempotency_key` must be `NULL` or empty; the generated key
   * is returned through the owned command receipt.
   */
  int generate_idempotency_key;
  /** Required opaque semantic request binding; never interpreted by liblockdc.
   */
  const char *request_digest;
  /** Optional durable business-correlation identity. */
  const char *operation_id;
} lc_command_request;

enum {
  /** Command acceptance is durable but no terminal command result is
   * published.
   */
  LC_COMMAND_PENDING = 1,
  /** Command completed successfully with its terminal result metadata. */
  LC_COMMAND_COMPLETED = 2,
  /** Command completed with a durable failure result. */
  LC_COMMAND_FAILED = 3
};

/** One terminal outcome staged through an owned outbox transaction. */
typedef struct lc_command_result {
  /** Required safe application result class for a completed command. */
  const char *result_code;
  /** Optional safe resource, outbox, or status reference. */
  const char *result_reference;
  /** Required only when `body` is non-NULL. */
  const char *content_type;
  /** Optional result bytes, streamed into the durable `result` attachment. */
  lc_source *body;
  /** Required safe application failure class for a failed command. */
  const char *failure_code;
  /** Optional bounded safe failure message. */
  const char *failure_message;
} lc_command_result;

/**
 * Owned durable command status and safe terminal outcome.
 *
 * Release every string with `lc_command_receipt_cleanup()`.
 */
typedef struct lc_command_receipt {
  /** `LC_COMMAND_PENDING`, `LC_COMMAND_COMPLETED`, or `LC_COMMAND_FAILED`. */
  int state;
  /** Non-zero when acceptance returned an existing matching receipt. */
  int duplicate;
  /** Owned component-generated stable command identity. */
  char *command_id;
  /** Owned authenticated scope used to select this receipt. */
  char *scope;
  /** Owned stable operation name used to select this receipt. */
  char *command_type;
  /** Owned caller retry identity used to select this receipt. */
  char *idempotency_key;
  /** Owned optional durable business-correlation identity. */
  char *operation_id;
  /** Owned safe completed result class, or `NULL`. */
  char *result_code;
  /** Owned safe completed resource, outbox, or status reference, or `NULL`.
   */
  char *result_reference;
  /** Owned safe terminal failure class, or `NULL`. */
  char *failure_code;
  /** Owned safe terminal failure message, or `NULL`. */
  char *failure_message;
  /** Non-zero when a completed receipt owns a streamable `result` attachment.
   */
  int has_result_body;
} lc_command_receipt;

/** Optional durable evidence retained when an outbox delivery completes. */
typedef struct lc_outbox_completion {
  /**
   * Optional provider or broker delivery reference. At most
   * `LC_OUTBOX_MAX_COMPLETION_EVIDENCE_BYTES` bytes are accepted.
   */
  const char *delivery_reference;
  /**
   * Optional opaque digest of the provider response or acknowledgement. At
   * most `LC_OUTBOX_MAX_COMPLETION_EVIDENCE_BYTES` bytes are accepted.
   */
  const char *response_digest;
} lc_outbox_completion;

/**
 * Result of a durable outbox append.
 *
 * Release strings with `lc_outbox_receipt_cleanup()`.
 */
typedef struct lc_outbox_receipt {
  /** Owned durable outbox key; pass it to dead-letter controls or logs. */
  char *outbox_key;
  /** Owned copy of the immutable foreign-system idempotency key. */
  char *effect_key;
  /** Non-zero when the append matched an already committed outbox record. */
  int duplicate;
} lc_outbox_receipt;

/** Receipts published by one successful outbox transaction commit.
 *
 * `outbox_receipts` is owned by this result and contains only effects made
 * durable by this commit. Initialize before use and clean it up after use. */
typedef struct lc_outbox_commit_result {
  /** Owned receipts for newly committed outbox effects. */
  lc_outbox_receipt *outbox_receipts;
  /** Number of entries in `outbox_receipts`. */
  size_t outbox_receipt_count;
} lc_outbox_commit_result;

/** Result of durable inbox acceptance. */
typedef struct lc_inbox_accept_result {
  /** Non-zero when a new inbox record and transaction were created. */
  int accepted;
  /** Non-zero when an existing matching inbox record was found. */
  int duplicate;
} lc_inbox_accept_result;

/** Maximum retained byte length of an outbox failure diagnostic. */
#define LC_OUTBOX_MAX_DIAGNOSTIC_BYTES 4096U

/**
 * Maximum retained byte length of either optional provider completion-evidence
 * field. Both fields are durable outbox metadata, not payload attachments.
 */
#define LC_OUTBOX_MAX_COMPLETION_EVIDENCE_BYTES 4096U

/** Durable outcome requested for a claimed outbox job. */
typedef struct lc_outbox_retry {
  /** Delay before the next eligible delivery, in seconds. Zero uses policy. */
  long delay_seconds;
  /**
   * Optional failure diagnostic retained with the record. At most
   * `LC_OUTBOX_MAX_DIAGNOSTIC_BYTES` bytes are accepted.
   */
  const char *diagnostic;
} lc_outbox_retry;

/** Domain-lease acquisition request within an outbox transaction. */
typedef struct lc_outbox_participant_request {
  /**
   * Lease request for the domain key. `key` is required and `txn_id` must be
   * `NULL`; namespace, owner, and TTL default from the parent outbox.
   */
  lc_acquire_req acquire;
} lc_outbox_participant_request;

/** Attachment selector by id or name. */
typedef struct lc_attachment_selector {
  /** Attachment content/hash identifier. Mutually exclusive with `name`. */
  const char *id;
  /** Attachment name. Mutually exclusive with `id`. */
  const char *name;
} lc_attachment_selector;

/** Attachment metadata returned by list, fetch, and attach operations. */
typedef struct lc_attachment_info {
  /** Attachment content/hash identifier. */
  char *id;
  /** Attachment name. */
  char *name;
  /** Attachment byte count. Values outside `long` are rejected. */
  long size;
  /** Hex SHA-256 digest of the plaintext attachment bytes. */
  char *plaintext_sha256;
  /** Content type recorded for the attachment. */
  char *content_type;
  /** Unix timestamp when this attachment was first created. */
  lc_unix_seconds created_at_unix;
  /** Unix timestamp of the attachment revision returned by this operation. */
  lc_unix_seconds updated_at_unix;
} lc_attachment_info;

/** Request used to attach streamed content to a lease. */
typedef struct lc_attach_req {
  /** Attachment name recorded by the server. */
  const char *name;
  /** Content type recorded with the attachment. */
  const char *content_type;
  /** Maximum accepted upload size when `has_max_bytes` is non-zero. */
  long max_bytes;
  /** Enables enforcement of `max_bytes`. */
  int has_max_bytes;
  /** Rejects overwriting an existing attachment with the same name. */
  int prevent_overwrite;
} lc_attach_req;

/** Client-level attachment upload operation for an existing, credentialed
 * lease reference. */
typedef struct lc_attach_op {
  /** Existing lease identity to attach to. */
  lc_lease_ref lease;
  /** Attachment name recorded by the server. */
  const char *name;
  /** Content type recorded with the attachment. */
  const char *content_type;
  /** Maximum accepted upload size when `has_max_bytes` is non-zero. */
  long max_bytes;
  /** Enables enforcement of `max_bytes`. */
  int has_max_bytes;
  /** Rejects overwriting an existing attachment with the same name. */
  int prevent_overwrite;
} lc_attach_op;

/** Result returned by an attachment upload. */
typedef struct lc_attach_res {
  /** Metadata for the written attachment. */
  lc_attachment_info attachment;
  /** Non-zero when the request was a no-op. */
  int noop;
  /** State version after the attachment update. */
  lc_version version;
  /** Server correlation id for the attach response. */
  char *correlation_id;
} lc_attach_res;

/** Attachment listing returned by list operations. */
typedef struct lc_attachment_list {
  /** Owned array of attachment metadata entries. */
  lc_attachment_info *items;
  /** Number of entries in `items`. */
  size_t count;
  /** Server correlation id for the list response. */
  char *correlation_id;
} lc_attachment_list;

/** Request used to fetch an attachment stream. */
typedef struct lc_attachment_get_req {
  /** Attachment id or name selector. */
  lc_attachment_selector selector;
  /** Reads the committed public view when nonzero. Private reads require an
   * active lease and, when transaction-bound, include that transaction's
   * staged attachment changes. */
  int public_read;
} lc_attachment_get_req;

/** Client-level request used to list lease attachments. */
typedef struct lc_attachment_list_req {
  /** Existing lease identity to list through. */
  lc_lease_ref lease;
  /** Reads the committed public view when nonzero. Private reads require an
   * active lease and, when transaction-bound, include that transaction's
   * staged attachment changes. */
  int public_read;
} lc_attachment_list_req;

/** Client-level attachment fetch operation for an existing lease reference. */
typedef struct lc_attachment_get_op {
  /** Existing lease identity to read through when `public_read` is zero. */
  lc_lease_ref lease;
  /** Attachment id or name selector. */
  lc_attachment_selector selector;
  /** Reads the committed public view when nonzero. Private reads require an
   * active lease and, when transaction-bound, include that transaction's
   * staged attachment changes. */
  int public_read;
} lc_attachment_get_op;

/** Client-level attachment delete operation for an existing, credentialed
 * lease reference. */
typedef struct lc_attachment_delete_op {
  /** Existing lease identity to delete through. */
  lc_lease_ref lease;
  /** Attachment id or name selector. */
  lc_attachment_selector selector;
} lc_attachment_delete_op;

/** Client-level operation used to delete all attachments through an existing,
 * credentialed lease reference. */
typedef struct lc_attachment_delete_all_op {
  /** Existing lease identity whose attachments should be deleted. */
  lc_lease_ref lease;
} lc_attachment_delete_all_op;

/** Result metadata returned by attachment fetch operations. */
typedef struct lc_attachment_get_res {
  /** Metadata for the fetched attachment. */
  lc_attachment_info attachment;
  /** Server correlation id for the fetch response. */
  char *correlation_id;
} lc_attachment_get_res;

/**
 * Lease handle with direct methods and read-only identity metadata.
 *
 * This is the normal handle returned from `acquire()` and from queue flows that
 * carry state. Use it for the common lease lifecycle:
 *
 * `describe()` or read the published fields, `get()`/`load()` the state,
 * `update()`/`save()`/`mutate()` it, `keepalive()` while the lease is held, and
 * finally `release()` to release the server-side lease and close the handle.
 *
 * If you want to abandon the local handle without a server-side release, call
 * `close()`.
 */
struct lc_lease {
  /** Refreshes the published lease fields on `self` from the server. */
  int (*describe)(lc_lease *self, lc_error *error);
  /**
   * Streams the current state document into `dst`.
   *
   * Use this for large documents. The caller owns `dst` and closes it.
   * `opts` may be `NULL` for the default read behavior.
   */
  int (*get)(lc_lease *self, lc_sink *dst, const lc_get_opts *opts,
             lc_get_res *out, lc_error *error);
  /**
   * Parses the current state document into `dst` through a lonejson map.
   *
   * Callers define their struct and map with `LONEJSON_FIELD_*` and
   * `LONEJSON_MAP_DEFINE(...)`, then release lonejson-owned field storage with
   * a lonejson runtime cleanup call when finished. For very large string or
   * byte values, use lonejson spool-backed field mappings so load can spill
   * those fields instead of forcing them to stay resident in memory.
   */
  int (*load)(lc_lease *self, const lonejson_map *map, void *dst,
              const lc_get_opts *opts, lc_get_res *out, lc_error *error);
  /**
   * Convenience mapped-struct variant of `update()`.
   *
   * The caller supplies the same lonejson map and struct shape used for
   * `load()`. lonejson serializes the mapped struct to JSON, and liblockdc
   * streams that JSON into the bound lease update. For very large outbound text
   * or byte values, use lonejson source-backed fields so serialization can read
   * from files or file descriptors without first materializing those bytes in
   * memory.
   * `opts` uses the same retention and precondition controls as `update()`;
   * `NULL` preserves retention and uses the normal fetched-version
   * precondition.
   */
  int (*save)(lc_lease *self, const lonejson_map *map, const void *src,
              const lc_update_opts *opts, lc_error *error);
  /**
   * Replaces the state document from a streamed JSON source.
   *
   * Use this for large JSON payloads. `opts` may be `NULL` for default update
   * behavior. On success, `self->version` and `self->state_etag` are refreshed.
   */
  int (*update)(lc_lease *self, lc_source *src, const lc_update_opts *opts,
                lc_error *error);
  /**
   * Applies one or more server-side mutations to the current state.
   *
   * On success, the published version and etag fields on `self` are refreshed.
   */
  int (*mutate)(lc_lease *self, const lc_mutate_req *req, lc_error *error);
  /**
   * Streams the current state through the local mutate engine and uploads the
   * staged JSON result back to lockd.
   *
   * This is the bounded-memory path for `file:`, `textfile:`, and
   * `base64file:` mutators. On success, `self->version` and
   * `self->state_etag` are refreshed from the update response.
   */
  int (*mutate_local)(lc_lease *self, const lc_mutate_local_req *req,
                      lc_error *error);
  /**
   * Updates lease metadata without replacing the state bytes.
   *
   * On success, the published query-hidden fields on `self` are refreshed.
   */
  int (*metadata)(lc_lease *self, const lc_metadata_req *req, lc_error *error);
  /**
   * Removes the current state bytes while keeping the lease itself alive.
   *
   * Use this when the lease should continue to exist but the state document
   * should not.
   */
  int (*remove)(lc_lease *self, const lc_remove_req *req, lc_error *error);
  /**
   * Renews the lease TTL and refreshes the expiry-related fields on `self`.
   *
   * Call this while the lease is still in use.
   */
  int (*keepalive)(lc_lease *self, const lc_keepalive_req *req,
                   lc_error *error);
  /**
   * Releases the lease on the server and closes the handle on success.
   *
   * This is the normal terminal step for an acquired lease. If it fails, the
   * handle remains valid and may be retried or closed locally with `close()`.
   */
  int (*release)(lc_lease *self, const lc_release_req *req, lc_error *error);
  /**
   * Streams an attachment upload from `src`.
   *
   * Use this for large binary payloads that must not be buffered in memory.
   */
  int (*attach)(lc_lease *self, const lc_attach_req *req, lc_source *src,
                lc_attach_res *out, lc_error *error);
  /** Lists the attachments currently associated with the lease. */
  int (*list_attachments)(lc_lease *self, lc_attachment_list *out,
                          lc_error *error);
  /**
   * Streams an attachment download into `dst`.
   *
   * Use this for large attachments. The caller owns `dst` and closes it.
   */
  int (*get_attachment)(lc_lease *self, const lc_attachment_get_req *req,
                        lc_sink *dst, lc_attachment_get_res *out,
                        lc_error *error);
  /** Deletes a single attachment selected by name or digest. */
  int (*delete_attachment)(lc_lease *self,
                           const lc_attachment_selector *selector, int *deleted,
                           lc_error *error);
  /** Deletes every attachment currently associated with the lease. */
  int (*delete_all_attachments)(lc_lease *self, int *deleted_count,
                                lc_error *error);
  /**
   * Closes the local handle without issuing a server-side release.
   *
   * Use this only when abandoning the handle or after a failed `release()`.
   */
  void (*close)(lc_lease *self);

  /** Namespace of the bound lease. */
  const char *ns;
  /** Key of the bound lease. */
  const char *key;
  /** Current recorded owner of the lease. */
  const char *owner;
  /** Server-issued lease identifier. */
  const char *lease_id;
  /** Transaction identifier associated with the lease, if any. */
  const char *txn_id;
  /** Current fencing token for optimistic concurrency. */
  long fencing_token;
  /** Current state version published by the server. */
  lc_version version;
  /** Current lease expiry as a Unix timestamp. */
  lc_unix_seconds lease_expires_at_unix;
  /** Current private state etag. */
  const char *state_etag;
  /** Non-zero when `query_hidden` was explicitly set by the server. */
  int has_query_hidden;
  /** Whether the state is hidden from normal query results. */
  int query_hidden;

  /** Retention from the latest private observation; unknown after uncertain
   * writes. */
  lc_state_retention_support retention_support;
  /** Non-zero when retention_expires_at_unix describes the observed body. */
  int has_retention_expires_at_unix;
  /** Expiry eligibility boundary; zero confirms no expiry. */
  lc_unix_seconds retention_expires_at_unix;

  /** Private implementation pointer; callers must not inspect or modify it. */
  void *impl;
};

/**
 * Queue message handle with direct lifecycle and payload access methods.
 *
 * This is the normal handle returned from `dequeue()` and subscription flows.
 * Read or stream the payload, optionally inspect `state()`, then terminate the
 * delivery with `ack()` or `nack()`. If you need to abandon the local handle
 * without acknowledging it, call `close()`.
 */
struct lc_message {
  /**
   * Acknowledges the message and closes a non-batch handle on success.
   *
   * Batch messages remain owned by their `lc_dequeue_batch_res` and are closed
   * by `lc_dequeue_batch_cleanup()`.
   *
   * If this call fails, the handle remains valid and may be retried or closed.
   */
  int (*ack)(lc_message *self, lc_error *error);
  /**
   * Negatively acknowledges the message and closes a non-batch handle on
   * success. Batch messages remain owned by their `lc_dequeue_batch_res` and
   * are closed by `lc_dequeue_batch_cleanup()`.
   *
   * `req->intent` selects whether the nack is treated as a processing
   * `failure` or an intentional `defer`. If this call fails, the handle
   * remains valid and may be retried or closed.
   */
  int (*nack)(lc_message *self, const lc_nack_req *req, lc_error *error);
  /**
   * Extends message visibility and refreshes the expiry fields on `self`.
   *
   * This is non-terminal: the message remains open after success.
   */
  int (*extend)(lc_message *self, const lc_extend_req *req, lc_error *error);
  /**
   * Returns the associated lease when the dequeue variant includes state.
   *
   * The returned handle is owned by the message and is closed with the message.
   */
  lc_lease *(*state)(lc_message *self);
  /**
   * Returns the payload reader owned by the message handle.
   *
   * Do not close the returned source directly; close the message instead. Some
   * live subscribe deliveries are single-pass streams and may not support
   * `reset()`.
   */
  lc_source *(*payload_reader)(lc_message *self);
  /** Rewinds the payload stream when the underlying source supports it. */
  int (*rewind_payload)(lc_message *self, lc_error *error);
  /**
   * Copies the payload stream into `dst`.
   *
   * Resettable payloads are rewound before copying. Single-pass payloads are
   * copied from their current position.
   */
  int (*write_payload)(lc_message *self, lc_sink *dst, size_t *written,
                       lc_error *error);
  /**
   * Closes the local message handle without acknowledging it.
   *
   * Use this only when abandoning the handle or after a failed terminal call.
   */
  void (*close)(lc_message *self);

  /** Namespace containing the queue message. */
  const char *ns;
  /** Queue name. */
  const char *queue;
  /** Server-issued message identifier. */
  const char *message_id;
  /** Number of delivery attempts so far. */
  int attempts;
  /** Maximum delivery attempts configured for the message. */
  int max_attempts;
  /** Number of failed delivery attempts recorded by the server. */
  int failure_attempts;
  /** Unix timestamp until which the message is hidden from other consumers. */
  lc_unix_seconds not_visible_until_unix;
  /** Current visibility timeout in seconds. */
  long visibility_timeout_seconds;
  /** Payload content type recorded with the message. */
  const char *payload_content_type;
  /** Server correlation identifier for the dequeue response. */
  const char *correlation_id;
  /** Delivery lease identifier for the message. */
  const char *lease_id;
  /** Current delivery lease expiry as a Unix timestamp. */
  lc_unix_seconds lease_expires_at_unix;
  /** Current fencing token for the delivery lease. */
  long fencing_token;
  /** Transaction identifier associated with the message, if any. */
  const char *txn_id;
  /** Current queue metadata etag. */
  const char *meta_etag;
  /** Cursor for the next queue page when the server returned one. */
  const char *next_cursor;
  /** Internal payload reader handle owned by the message. */
  lc_source *payload;

  /** Private implementation pointer; callers must not inspect or modify it. */
  void *impl;
};

/**
 * Managed queue consumer service.
 *
 * Create this from `client->new_consumer_service()`, then call `run()` for the
 * blocking Go-`StartConsumer` style outbox or `start()`/`stop()`/`wait()` for
 * explicit daemon-style integration.
 */
struct lc_consumer_service {
  /**
   * Starts all configured consumer loops and blocks until they stop.
   *
   * This is the closest equivalent to the Go SDK `StartConsumer` call.
   * Returning `LC_OK` means the service stopped cleanly, either because
   * `stop()` was requested or because all loops exited without a fatal error.
   */
  int (*run)(lc_consumer_service *self, lc_error *error);
  /**
   * Starts all configured consumer loops asynchronously.
   *
   * Pair this with `stop()` and `wait()` when embedding the service into a
   * larger daemon that owns its own main loop.
   */
  int (*start)(lc_consumer_service *self, lc_error *error);
  /**
   * Requests shutdown of all consumer loops.
   *
   * This does not free the service object. Call `wait()` to join all worker
   * threads, then `close()` when the service is no longer needed.
   */
  int (*stop)(lc_consumer_service *self);
  /**
   * Blocks until all consumer loops exit and returns the terminal service
   * result.
   *
   * `LC_OK` means clean shutdown. Any non-`LC_OK` result is the first fatal
   * service error captured during execution.
   */
  int (*wait)(lc_consumer_service *self, lc_error *error);
  /**
   * Closes the local service object and releases all associated resources.
   *
   * If the service is still running, `close()` first requests shutdown and
   * waits for all loops to exit.
   */
  void (*close)(lc_consumer_service *self);

  /** Private implementation pointer; callers must not inspect or modify it. */
  void *impl;
};

/**
 * One owned outbox transaction. Its stable xid exists before the first
 * participant; later acquires automatically carry that xid. A bound managed XA
 * transaction chooses the terminal decision and gates all publication.
 * If an operation fails after enrolling a later participant, implicit XA has
 * rolled the xid back; close this transaction and begin a new one to retry.
 */
struct lc_outbox_transaction {
  /** Stable global ID, borrowed until close. Minting it performs no storage
   * I/O. */
  const char *(*id)(lc_outbox_transaction *self);
  /** Bind to an open managed transaction on the same local Pouch root. Before
   * the first member, this selects the managed ID. After members exist, IDs
   * must match (adopt this outbox's ID first). The coordinator pins this facade
   * across operations; no SQL/provider types enter the outbox interface. */
  int (*bind_xa)(lc_outbox_transaction *self, lc_xa_transaction *transaction,
                 lc_error *error);
  /** Starts phase-two commit through the bound coordinator; drive/close the
   * returned operation.
   */
  int (*commit_start)(lc_outbox_transaction *self, int64_t deadline_ms,
                      lc_xa_coordinator_operation **out, lc_error *error);
  /** Starts phase-two rollback through the bound coordinator; does not
   * discard recovery intent.
   */
  int (*rollback_start)(lc_outbox_transaction *self, int64_t deadline_ms,
                        lc_xa_coordinator_operation **out, lc_error *error);
  /**
   * Accepts one durable command receipt in this transaction.
   *
   * A transaction may own at most one command. A matching existing receipt is
   * returned with `receipt->duplicate` set and does not repeat the command.
   */
  int (*accept_command)(lc_outbox_transaction *self,
                        const lc_command_request *request,
                        lc_command_receipt *receipt, lc_error *error);
  /**
   * Accepts one durable inbox receipt in this transaction.
   *
   * A matching existing receipt is returned with `result->duplicate` set and
   * does not repeat the inbox transition. A fresh receipt becomes durable only
   * with this transaction's terminal decision.
   */
  int (*accept_inbox)(lc_outbox_transaction *self,
                      const lc_inbox_message *message,
                      lc_inbox_accept_result *result, lc_error *error);
  /**
   * Acquires a non-terminal domain participant under this transaction's xid.
   *
   * The request must not supply its own transaction id. Close the returned
   * participant after use; only this transaction can commit or roll it back.
   */
  int (*acquire)(lc_outbox_transaction *self,
                 const lc_outbox_participant_request *request,
                 lc_outbox_participant **out, lc_error *error);
  /**
   * Stages an immutable outbox effect and its streamed payload.
   *
   * A fresh effect leaves `out` empty until commit; an existing committed
   * duplicate is returned immediately. `entry->payload_digest` binds duplicate
   * identity without pre-reading the payload source. The payload callback may
   * close this transaction safely; in that case staging returns
   * `LC_ERR_INVALID` because no transaction remains for a later commit.
   */
  int (*append)(lc_outbox_transaction *self, const lc_outbox_entry *entry,
                lc_source *payload, lc_outbox_receipt *out, lc_error *error);
  /**
   * Completes this transaction's owned pending command with a safe result.
   * A streamed `result->body` may close this transaction safely; completion
   * then returns `LC_ERR_INVALID` rather than reporting a usable decision.
   */
  int (*complete_command)(lc_outbox_transaction *self,
                          const lc_command_result *result, lc_error *error);
  /**
   * Fails this transaction's owned pending command with a safe result.
   * A streamed `result->body` may close this transaction safely; failure then
   * returns `LC_ERR_INVALID` rather than reporting a usable decision.
   */
  int (*fail_command)(lc_outbox_transaction *self,
                      const lc_command_result *result, lc_error *error);
  /**
   * Commits every enrolled participant and consumes their lease handles.
   *
   * `out` is populated only after the durable commit and contains fresh
   * wakeable outbox receipts. Returns an error when the endpoint durably rolls
   * the transaction back or cannot confirm its terminal decision.
   */
  int (*commit)(lc_outbox_transaction *self, lc_outbox_commit_result *out,
                lc_error *error);
  /** Rolls back every enrolled participant and consumes their lease handles. */
  int (*rollback)(lc_outbox_transaction *self, lc_error *error);
  /**
   * Releases the local transaction wrapper.
   *
   * For an undecided transaction, this makes a best-effort rollback before
   * invalidating every participant view.
   */
  void (*close)(lc_outbox_transaction *self);
  /** Private implementation pointer; callers must not inspect or modify it. */
  void *impl;
};

/**
 * Restricted non-terminal view over an outbox-owned lease. A terminal
 * transaction decision or transaction close invalidates the view; subsequent
 * methods fail with `LC_ERR_INVALID`, but `close` remains required and safe.
 */
struct lc_outbox_participant {
  /** Refreshes the public lease snapshot fields from the staged view. */
  int (*describe)(lc_outbox_participant *self, lc_error *error);
  /**
   * Streams the participant's staged state into `dst`.
   * The sink may close this view safely; it becomes unusable after the call.
   */
  int (*get)(lc_outbox_participant *self, lc_sink *dst, const lc_get_opts *opts,
             lc_get_res *out, lc_error *error);
  /**
   * Streams and stages a state replacement. The source may close this view
   * safely; it becomes unusable after the call.
   */
  int (*update)(lc_outbox_participant *self, lc_source *src,
                const lc_update_opts *opts, lc_error *error);
  /** Applies a staged server-side mutation. */
  int (*mutate)(lc_outbox_participant *self, const lc_mutate_req *req,
                lc_error *error);
  /** Applies a staged client-side mutation. */
  int (*mutate_local)(lc_outbox_participant *self,
                      const lc_mutate_local_req *req, lc_error *error);
  /** Stages metadata changes without replacing the state body. */
  int (*metadata)(lc_outbox_participant *self, const lc_metadata_req *req,
                  lc_error *error);
  /** Stages removal of this participant's state. */
  int (*remove)(lc_outbox_participant *self, const lc_remove_req *req,
                lc_error *error);
  /** Extends the underlying lease while leaving the transaction open. */
  int (*keepalive)(lc_outbox_participant *self, const lc_keepalive_req *req,
                   lc_error *error);
  /**
   * Streams and stages one attachment upload. The source may close this view
   * safely; it becomes unusable after the call.
   */
  int (*attach)(lc_outbox_participant *self, const lc_attach_req *req,
                lc_source *src, lc_attach_res *out, lc_error *error);
  /** Lists attachments visible through the participant's staged view. */
  int (*list_attachments)(lc_outbox_participant *self, lc_attachment_list *out,
                          lc_error *error);
  /**
   * Streams an attachment visible through the participant's staged view. The
   * sink may close this view safely; it becomes unusable after the call.
   */
  int (*get_attachment)(lc_outbox_participant *self,
                        const lc_attachment_get_req *req, lc_sink *dst,
                        lc_attachment_get_res *out, lc_error *error);
  /** Stages deletion of one attachment. */
  int (*delete_attachment)(lc_outbox_participant *self,
                           const lc_attachment_selector *selector, int *deleted,
                           lc_error *error);
  /** Stages deletion of every attachment. */
  int (*delete_all_attachments)(lc_outbox_participant *self, int *deleted_count,
                                lc_error *error);
  /** Releases this view; it cannot make a terminal transaction decision. */
  void (*close)(lc_outbox_participant *self);
  /** Borrowed namespace name; valid until this view is closed. */
  const char *ns;
  /** Borrowed state key; valid until this view is closed. */
  const char *key;
  /** Borrowed transaction id; valid until this view is closed. */
  const char *txn_id;
  /** Current fencing token from the staged lease snapshot. */
  long fencing_token;
  /** Current staged state version. */
  lc_version version;
  /** Borrowed staged state ETag, or `NULL`; valid until the next operation. */
  const char *state_etag;
  /** Retention from the latest private state observation. */
  lc_state_retention_support retention_support;
  /** Non-zero when retention_expires_at_unix describes an existing body. */
  int has_retention_expires_at_unix;
  /** Expiry eligibility boundary; zero confirms no expiry. */
  lc_unix_seconds retention_expires_at_unix;
  /** Private implementation pointer; callers must not inspect or modify it. */
  void *impl;
};

/**
 * One claimed outbox effect, owned by the host after `dispatcher->next()`.
 *
 * Terminal methods never invoke host callbacks. `write_payload()` streams the
 * immutable payload attachment into the caller's sink without materializing it
 * in liblockdc; that sink may close the job safely, and the job becomes
 * unusable after the call. A terminal method consumes the job on success. All
 * string fields are borrowed and valid until `close()` or a successful
 * terminal method.
 */
struct lc_outbox_job {
  /**
   * Streams the immutable payload attachment into `dst`; `dst` may close this
   * job safely.
   */
  int (*write_payload)(lc_outbox_job *self, lc_sink *dst, size_t *written,
                       lc_error *error);
  /**
   * Returns the optional state lease associated with this stateful delivery.
   *
   * The returned handle is borrowed from the job and is invalidated by a
   * successful terminal operation or `close()`. It is `NULL` for a job
   * obtained through the stateless dispatcher operation. Successful state and
   * attachment mutations are immediately durable under this lease's fencing
   * precondition, so a later expiry claimant can observe a host checkpoint.
   */
  lc_lease *(*state)(lc_outbox_job *self);
  /** Extends the claim before a longer foreign operation. */
  int (*renew)(lc_outbox_job *self, long ttl_seconds, lc_error *error);
  /** Marks delivery complete and optionally retains delivery evidence. */
  int (*complete)(lc_outbox_job *self, const lc_outbox_completion *completion,
                  lc_error *error);
  /** Returns the job to `retry_wait` after an optional delay and diagnostic. */
  int (*retry)(lc_outbox_job *self, const lc_outbox_retry *request,
               lc_error *error);
  /** Makes the job terminal dead-lettered with an optional diagnostic. */
  int (*dead_letter)(lc_outbox_job *self, const char *diagnostic,
                     lc_error *error);
  /** Abandons the local job handle without a terminal transition. */
  void (*close)(lc_outbox_job *self);
  /** Opaque durable record key; useful for logs and diagnostics. */
  const char *outbox_key;
  /** Borrowed stable business-operation identity. */
  const char *operation_id;
  /** Borrowed stable identity of this effect within the operation. */
  const char *effect_id;
  /** Borrowed idempotency key to pass to the foreign effect system. */
  const char *effect_key;
  /** Component-generated stable transport message identity. */
  const char *message_id;
  /**
   * Optional component-generated command identity that owns this effect.
   * It is set for an effect appended through a command-owning transaction and
   * is independent of the caller-defined `causation_id`.
   */
  const char *command_id;
  /** Optional immediate durable command or source-message cause. */
  const char *causation_id;
  /** Borrowed caller-defined routing kind. */
  const char *kind;
  /** Optional caller-defined message schema version. */
  const char *schema_version;
  /** Borrowed caller-defined transport destination. */
  const char *destination;
  /** Borrowed payload media type. */
  const char *content_type;
  /** Borrowed JSON object string containing immutable envelope headers. */
  const char *headers_json;
  /** Borrowed immutable trace-propagation value. */
  const char *trace_context;
  /** Delivery number, starting at one. */
  int attempt;
  /** Maximum permitted attempts including the first. */
  int max_attempts;
  /** Current claim expiry; refreshed by `renew()`. */
  lc_unix_seconds lease_expires_at_unix;
  /** Private implementation pointer; callers must not inspect or modify it. */
  void *impl;
  /** Reserved ABI-4 extension slots. Always `NULL`. */
  void *reserved_extension_slots[8];
};

/** Threadless outbox producer and transactional receipt façade. */
struct lc_outbox {
  /**
   * Creates a lazy transaction with no durable marker or xid until its first
   * participant is staged. An empty transaction cannot commit.
   */
  int (*begin)(lc_outbox *self, lc_outbox_transaction **out, lc_error *error);
  /**
   * Accepts an idempotent command.
   *
   * A fresh command returns an owned transaction in `out_txn`. A matching
   * receipt returns `*out_txn == NULL` with `receipt->duplicate` set.
   */
  int (*accept_command)(lc_outbox *self, const lc_command_request *request,
                        lc_outbox_transaction **out_txn,
                        lc_command_receipt *receipt, lc_error *error);
  /** Reads one durable command receipt without acquiring outbox work. */
  int (*get_command_receipt)(lc_outbox *self,
                             const lc_command_identity *identity,
                             lc_command_receipt *out, lc_error *error);
  /** Reads a durable command receipt by its component-generated command id. */
  int (*get_command_receipt_by_id)(lc_outbox *self, const char *command_id,
                                   lc_command_receipt *out, lc_error *error);
  /**
   * Waits for a command receipt to become terminal without claiming work or
   * invoking host code. `timeout_ms` is zero for a one-shot read, `-1` to wait
   * without a deadline, or positive for a monotonic deadline that also bounds
   * each remote receipt read. A timeout returns `LC_ERR_TIMEOUT` and the
   * latest pending receipt in `out`.
   */
  int (*wait_command)(lc_outbox *self, const char *command_id, long timeout_ms,
                      lc_command_receipt *out, lc_error *error);
  /**
   * Streams a completed command's result attachment into `dst`, if present.
   * The sink may close this outbox safely; it becomes unusable after the call.
   */
  int (*write_command_result)(lc_outbox *self,
                              const lc_command_identity *identity, lc_sink *dst,
                              size_t *written, lc_error *error);
  /**
   * Resumes a pending command in a new transaction.
   *
   * A terminal receipt returns `*out_txn == NULL`; it is never reopened.
   */
  int (*resume_command)(lc_outbox *self, const lc_command_identity *identity,
                        lc_outbox_transaction **out_txn,
                        lc_command_receipt *receipt, lc_error *error);
  /**
   * Resumes a pending command by its component-generated command id. This is
   * intended for a claimed command-bound outbox job's supervisor. A terminal
   * command returns `*out_txn == NULL` and its current receipt. With valid
   * output arguments, failure also clears `*out_txn`.
   */
  int (*resume_command_by_id)(lc_outbox *self, const char *command_id,
                              lc_outbox_transaction **out_txn,
                              lc_command_receipt *receipt, lc_error *error);
  /**
   * Stages one durable outbox effect and starts its lazy transaction.
   *
   * A fresh effect leaves `receipt` empty; its wakeable receipt appears only
   * in `transaction->commit()`. A matching committed effect returns no
   * transaction and sets `receipt->duplicate`. The payload source may close
   * this outbox safely; the append then returns `LC_ERR_INVALID` and no
   * transaction.
   */
  int (*append)(lc_outbox *self, const lc_outbox_entry *entry,
                lc_source *payload, lc_outbox_transaction **out_txn,
                lc_outbox_receipt *receipt, lc_error *error);
  /**
   * Creates one durable inbox receipt and starts its transaction.
   *
   * A matching source message returns no transaction and sets
   * `result->duplicate`.
   */
  int (*accept_inbox)(lc_outbox *self, const lc_inbox_message *message,
                      lc_outbox_transaction **out_txn,
                      lc_inbox_accept_result *result, lc_error *error);
  /** Obtains the one compatible local dispatcher for this client and config. */
  int (*get_or_start_dispatcher)(lc_outbox *self, lc_outbox_dispatcher **out,
                                 lc_error *error);
  /** Releases this threadless producer. It never stops an attached dispatcher.
   */
  void (*close)(lc_outbox *self);
  /** Private implementation pointer; callers must not inspect or modify it. */
  void *impl;
  /** Reserved ABI-4 extension slots. Always `NULL`. */
  void *reserved_extension_slots[8];
};

/**
 * Explicit process-local outbox consumer. Its private thread queues bounded
 * candidates and recovery intent only. Acquisition itself performs no recovery
 * I/O; a blocking `next()` or explicit `reconcile()` requests it. A
 * zero-timeout `next()` is an in-memory non-query probe. `next()` claims work
 * on caller demand.
 */
struct lc_outbox_dispatcher {
  /**
   * Claims one owned job on consumer demand; failure clears `*out`.
   * Zero probes only in-memory candidates without starting recovery; `-1`
   * waits indefinitely, positive milliseconds bound the pull, and other
   * negatives are invalid. Foreground pulls never run receipt retention.
   */
  int (*next)(lc_outbox_dispatcher *self, long timeout_ms, lc_outbox_job **out,
              lc_error *error);
  /** Copies one committed durable outbox key into the bounded wake queue. */
  int (*notify_outbox_key)(lc_outbox_dispatcher *self, const char *outbox_key,
                           lc_error *error);
  /** Copies process-local counters without querying storage. `out` must be
   * initialized; release its owned error text with `lc_outbox_stats_cleanup()`.
   */
  int (*get_stats)(lc_outbox_dispatcher *self, lc_outbox_stats *out,
                   lc_error *error);
  /** Requests asynchronous bounded durable reconciliation. */
  int (*reconcile)(lc_outbox_dispatcher *self, lc_error *error);
  /** Returns one dead-lettered effect to pending and signals dispatch. */
  int (*replay_dead_letter)(lc_outbox_dispatcher *self, const char *outbox_key,
                            lc_error *error);
  /** Permanently deletes one dead-lettered effect and its payload attachment.
   */
  int (*delete_dead_letter)(lc_outbox_dispatcher *self, const char *outbox_key,
                            lc_error *error);
  /** Streams selected dead-letter envelopes as JSON or JSONL; never payloads.
   */
  int (*export_dead_letters)(lc_outbox_dispatcher *self,
                             const lc_dead_letter_export_opts *options,
                             lc_sink *dst, lc_dead_letter_export_res *out,
                             lc_error *error);
  /**
   * Requests stop and waits for private infrastructure and caller-owned jobs.
   * Zero polls; `-1` waits indefinitely; positive milliseconds bound the wait.
   * A timeout leaves the dispatcher stopping and jobs usable. Finish or close
   * handed-out jobs before an indefinite wait; lease expiry alone does not
   * release their local handles. This operation is idempotent.
   */
  int (*stop)(lc_outbox_dispatcher *self, long deadline_ms, lc_error *error);
  /** Observes an already requested stop with the same deadlines as `stop()`;
   * returns `LC_ERR_INVALID` if stop has not been requested. */
  int (*wait)(lc_outbox_dispatcher *self, long deadline_ms, lc_error *error);
  /** Releases one caller reference; it does not stop the dispatcher. */
  void (*close)(lc_outbox_dispatcher *self);
  /** Private implementation pointer; callers must not inspect or modify it. */
  void *impl;
  /**
   * Waits for and claims one outbox job with its paired durable state lease.
   *
   * On failure no job escapes. The returned job owns the state lease exposed
   * by `lc_outbox_job_state()` until terminalization or close; callers must not
   * close that borrowed lease. Requires Pouch and uses `next()` timeouts.
   * Before its probe, a bounded exact-key checkpoint cleanup may run; receipt
   * retention remains private dispatcher work. This consumes
   * one formerly reserved ABI-4 extension slot.
   */
  int (*next_with_state)(lc_outbox_dispatcher *self, long timeout_ms,
                         lc_outbox_job **out, lc_error *error);
  /** Reserved ABI-4 extension slots. Always `NULL`. */
  void *reserved_extension_slots[7];
};

/**
 * Durable cursor which prevents Pouch compaction from reclaiming unacknowledged
 * namespace history.
 *
 * Closing this local handle does not unregister the durable consumer. Call
 * `unregister()` only after the consumer no longer needs retained history.
 */
struct lc_history_consumer {
  /** Reads this consumer's durable acknowledgement and current sequence. */
  int (*position)(lc_history_consumer *self, lc_history_consumer_position *out,
                  lc_error *error);
  /**
   * Durably advances the acknowledgement. The value must be monotonic and no
   * greater than the current namespace sequence.
   */
  int (*advance)(lc_history_consumer *self, lc_index_seq acknowledged_index_seq,
                 lc_history_consumer_position *out, lc_error *error);
  /** Removes the durable retention pin. The local handle remains closable. */
  int (*unregister)(lc_history_consumer *self, lc_error *error);
  /** Releases the local handle without changing its durable registration. */
  void (*close)(lc_history_consumer *self);
  /** Private implementation pointer; callers must not inspect or modify it. */
  void *impl;
};

/**
 * Root client handle.
 *
 * This is the root object for the SDK. Open it once with `lc_client_open()`,
 * then use it to acquire leases, query state, enqueue or dequeue messages, and
 * call the admin surfaces.
 *
 * For the common path, `acquire()` returns an `lc_lease` handle and
 * `dequeue()` returns an `lc_message` handle. Those returned handles own their
 * ongoing lifecycle and should normally be finished with `release()`/`ack()`
 * rather than jumping back to client-level calls.
 *
 * A concrete client retains itself for the duration of any application
 * callback invoked by one of its methods, including source and sink callbacks,
 * query handlers, queue consumers, queue watches, and
 * `acquire_for_update()` handlers. Such a callback may close the root client;
 * the active operation then unwinds safely, but the callback must not make a
 * further root-client call after closing it.
 */
struct lc_client {
  /**
   * Acquires a new lease and returns an `lc_lease` handle on success.
   *
   * The returned handle is the normal object for follow-up calls such as
   * `lease->update()`, `lease->get()`, `lease->keepalive()`, and
   * `lease->release()`.
   */
  int (*acquire)(lc_client *self, const lc_acquire_req *req, lc_lease **out,
                 lc_error *error);
  /** Describes the current state and lease metadata for `req->key`. */
  int (*describe)(lc_client *self, const lc_describe_req *req,
                  lc_describe_res *out, lc_error *error);
  /** Streams the current state document for `key` into `dst`. */
  int (*get)(lc_client *self, const char *key, const lc_get_opts *opts,
             lc_sink *dst, lc_get_res *out, lc_error *error);
  /**
   * Convenience variant of `get()` that parses the current state into `dst`
   * through a lonejson map.
   *
   * Callers define their struct and map with `LONEJSON_FIELD_*` and
   * `LONEJSON_MAP_DEFINE(...)`, then release lonejson-owned field storage with
   * a lonejson runtime cleanup call when finished.
   */
  int (*load)(lc_client *self, const char *key, const lonejson_map *map,
              void *dst, const lc_get_opts *opts, lc_get_res *out,
              lc_error *error);
  /**
   * Convenience variant of `load()` for a component record in an explicit
   * namespace rather than this client's default namespace.
   *
   * The namespace must be nonempty. Implementations that expose an
   * `lc_client` receiver shell must provide this method when they support
   * `lc_load_in_namespace()`.
   */
  int (*load_in_namespace)(lc_client *self, const char *ns, const char *key,
                           const lonejson_map *map, void *dst,
                           const lc_get_opts *opts, lc_get_res *out,
                           lc_error *error);
  /** Updates an existing, credentialed lease reference from a streamed JSON
   * source. */
  int (*update)(lc_client *self, const lc_update_req *req, lc_source *src,
                lc_update_res *out, lc_error *error);
  /** Applies one or more server-side mutations to an existing, credentialed
   * lease reference. */
  int (*mutate)(lc_client *self, const lc_mutate_op *req, lc_mutate_res *out,
                lc_error *error);
  /** Updates metadata fields on an existing, credentialed lease reference. */
  int (*metadata)(lc_client *self, const lc_metadata_op *req,
                  lc_metadata_res *out, lc_error *error);
  /** Removes state bytes for an existing, credentialed lease reference. */
  int (*remove)(lc_client *self, const lc_remove_op *req, lc_remove_res *out,
                lc_error *error);
  /** Renews an existing lease reference without using a bound `lc_lease`. */
  int (*keepalive)(lc_client *self, const lc_keepalive_op *req,
                   lc_keepalive_res *out, lc_error *error);
  /** Releases an existing lease reference without using a bound `lc_lease`. */
  int (*release)(lc_client *self, const lc_release_op *req, lc_release_res *out,
                 lc_error *error);
  /** Streams an attachment upload for an existing, credentialed lease
   * reference. */
  int (*attach)(lc_client *self, const lc_attach_op *req, lc_source *src,
                lc_attach_res *out, lc_error *error);
  /** Lists the attachments associated with an existing lease reference. */
  int (*list_attachments)(lc_client *self, const lc_attachment_list_req *req,
                          lc_attachment_list *out, lc_error *error);
  /** Streams an attachment download for an existing lease reference. */
  int (*get_attachment)(lc_client *self, const lc_attachment_get_op *req,
                        lc_sink *dst, lc_attachment_get_res *out,
                        lc_error *error);
  /** Deletes one attachment through an existing, credentialed lease
   * reference. */
  int (*delete_attachment)(lc_client *self, const lc_attachment_delete_op *req,
                           int *deleted, lc_error *error);
  /** Deletes all attachments through an existing, credentialed lease
   * reference. */
  int (*delete_all_attachments)(lc_client *self,
                                const lc_attachment_delete_all_op *req,
                                int *deleted_count, lc_error *error);
  /** Returns queue depth and delivery metadata for a queue. */
  int (*queue_stats)(lc_client *self, const lc_queue_stats_req *req,
                     lc_queue_stats_res *out, lc_error *error);
  /** Acknowledges a queue message from client-level identifiers. */
  int (*queue_ack)(lc_client *self, const lc_ack_op *req, lc_ack_res *out,
                   lc_error *error);
  /** Negatively acknowledges a queue message from client-level identifiers. */
  int (*queue_nack)(lc_client *self, const lc_nack_op *req, lc_nack_res *out,
                    lc_error *error);
  /** Extends visibility for a queue message from client-level identifiers. */
  int (*queue_extend)(lc_client *self, const lc_extend_op *req,
                      lc_extend_res *out, lc_error *error);
  /** Streams query results into `dst`. */
  int (*query)(lc_client *self, const lc_query_req *req, lc_sink *dst,
               lc_query_res *out, lc_error *error);
  /** Reads namespace-level engine configuration. */
  int (*get_namespace_config)(lc_client *self,
                              const lc_namespace_config_req *req,
                              lc_namespace_config_res *out, lc_error *error);
  /** Updates namespace-level engine configuration. */
  int (*update_namespace_config)(lc_client *self,
                                 const lc_namespace_config_req *req,
                                 lc_namespace_config_res *out, lc_error *error);
  /** Triggers an index flush for the selected namespace. */
  int (*flush_index)(lc_client *self, const lc_index_flush_req *req,
                     lc_index_flush_res *out, lc_error *error);
  /** Replays a durable decision. Expired raw prepare becomes rollback;
   * managed undecided work requires fenced coordinator recovery. On
   * LC_ERR_PENDING, out retains the decision while application is incomplete.
   */
  int (*txn_replay)(lc_client *self, const lc_txn_replay_req *req,
                    lc_txn_replay_res *out, lc_error *error);
  /** Prepares a transaction decision. */
  int (*txn_prepare)(lc_client *self, const lc_txn_decision_req *req,
                     lc_txn_decision_res *out, lc_error *error);
  /** Records COMMIT and applies/releases matching participants. Pouch returns
   * LC_ERR_PENDING with out populated while external completion is unproven. */
  int (*txn_commit)(lc_client *self, const lc_txn_decision_req *req,
                    lc_txn_decision_res *out, lc_error *error);
  /** Records/applies ABORT without reversing COMMIT. Pouch may return
   * LC_ERR_PENDING with out populated while external cleanup is incomplete. */
  int (*txn_rollback)(lc_client *self, const lc_txn_decision_req *req,
                      lc_txn_decision_res *out, lc_error *error);
  /** Acquires a TC lease. */
  int (*tc_lease_acquire)(lc_client *self, const lc_tc_lease_acquire_req *req,
                          lc_tc_lease_acquire_res *out, lc_error *error);
  /** Renews a TC lease. */
  int (*tc_lease_renew)(lc_client *self, const lc_tc_lease_renew_req *req,
                        lc_tc_lease_renew_res *out, lc_error *error);
  /** Releases a TC lease. */
  int (*tc_lease_release)(lc_client *self, const lc_tc_lease_release_req *req,
                          lc_tc_lease_release_res *out, lc_error *error);
  /** Returns current TC leader information. */
  int (*tc_leader)(lc_client *self, lc_tc_leader_res *out, lc_error *error);
  /** Announces the current node into the TC cluster set. */
  int (*tc_cluster_announce)(lc_client *self,
                             const lc_tc_cluster_announce_req *req,
                             lc_tc_cluster_res *out, lc_error *error);
  /** Removes the current node from the TC cluster set. */
  int (*tc_cluster_leave)(lc_client *self, lc_tc_cluster_res *out,
                          lc_error *error);
  /** Lists the current TC cluster set. */
  int (*tc_cluster_list)(lc_client *self, lc_tc_cluster_res *out,
                         lc_error *error);
  /** Registers a TC resource manager. */
  int (*tc_rm_register)(lc_client *self, const lc_tc_rm_register_req *req,
                        lc_tc_rm_res *out, lc_error *error);
  /** Unregisters a TC resource manager. */
  int (*tc_rm_unregister)(lc_client *self, const lc_tc_rm_unregister_req *req,
                          lc_tc_rm_res *out, lc_error *error);
  /** Lists registered TC resource managers. */
  int (*tc_rm_list)(lc_client *self, lc_tc_rm_list_res *out, lc_error *error);
  /** Streams a queue payload upload and enqueues it. */
  int (*enqueue)(lc_client *self, const lc_enqueue_req *req, lc_source *src,
                 lc_enqueue_res *out, lc_error *error);
  /**
   * Dequeues a queue message without an associated state lease.
   * On error, no delivery lease is retained by the client.
   */
  int (*dequeue)(lc_client *self, const lc_dequeue_req *req, lc_message **out,
                 lc_error *error);
  /**
   * Dequeues up to `req->page_size` messages and returns them as a batch.
   * On error, no delivery lease is retained by the client.
   */
  int (*dequeue_batch)(lc_client *self, const lc_dequeue_req *req,
                       lc_dequeue_batch_res *out, lc_error *error);
  /**
   * Dequeues a queue message and its associated state lease handle atomically.
   * On error, neither lease is retained by the client.
   */
  int (*dequeue_with_state)(lc_client *self, const lc_dequeue_req *req,
                            lc_message **out, lc_error *error);
  /** Consumes queue messages with a streaming callback. */
  int (*subscribe)(lc_client *self, const lc_dequeue_req *req,
                   const lc_consumer *consumer, lc_error *error);
  /** Consumes queue messages with attached state leases. */
  int (*subscribe_with_state)(lc_client *self, const lc_dequeue_req *req,
                              const lc_consumer *consumer, lc_error *error);
  /**
   * Creates a managed consumer service that mirrors the Go SDK
   * `StartConsumer` outbox.
   *
   * The returned service owns deep copies of the consumer configs and may
   * outlive the root client. Remote workers use cloned client instances. Pouch
   * workers retain and share the source client's local session so the default
   * exclusive writer remains a single root owner; closing the source client
   * after service creation does not release that session until the service is
   * closed.
   */
  int (*new_consumer_service)(lc_client *self,
                              const lc_consumer_service_config *config,
                              lc_consumer_service **out, lc_error *error);
  /** Creates a threadless inbox/outbox producer owned by this client.
   * Outbox workflows currently require Pouch; remote lockd is unsupported.
   */
  int (*new_outbox)(lc_client *self, const lc_outbox_config *config,
                    lc_outbox **out, lc_error *error);
  /**
   * Creates a threadless outbox and attaches an already-compatible explicit
   * dispatcher. The dispatcher receives post-commit local wake hints only.
   */
  int (*new_outbox_with_dispatcher)(lc_client *self,
                                    const lc_outbox_config *config,
                                    lc_outbox_dispatcher *dispatcher,
                                    lc_outbox **out, lc_error *error);
  /** Watches queue depth changes with a streaming watch callback. */
  int (*watch_queue)(lc_client *self, const lc_watch_queue_req *req,
                     const lc_watch_handler *handler, lc_error *error);
  /**
   * Closes this root client receiver.
   *
   * Do not make further calls through `self` after this returns. Independently
   * returned lease, message, outbox, and service handles retain the ownership
   * they need to reach their own documented terminal or close boundary; callers
   * must still close or terminalize each of them. Calling this from an
   * application callback is safe: the active operation retains the concrete
   * client until it has fully unwound.
   */
  void (*close)(lc_client *self);

  /** Default namespace applied when request structs leave `ns`
   * unset. */
  const char *default_namespace;
  /** Private implementation pointer; callers must not inspect or modify it. */
  void *impl;
  /**
   * Acquires a lease, spools the complete private state snapshot to a temporary
   * file, invokes `handler`, closes the snapshot, and releases the lease before
   * returning.
   *
   * The `lc_acquire_for_update_context`, its `lease`, and its `state.reader`
   * are borrowed and valid only for the duration of the handler call. The
   * helper owns the final release, so handlers must not close or release the
   * borrowed lease. Handler success commits staged changes; handler failure
   * releases with rollback.
   *
   * Extension slots are appended to keep method offsets stable within the
   * current shared-library ABI line.
   */
  int (*acquire_for_update)(lc_client *self, const lc_acquire_req *req,
                            lc_acquire_for_update_handler_fn handler,
                            void *handler_context, lc_error *error);
  /**
   * Streams key query result strings through chunked callbacks.
   * An empty response may omit `keys`; it succeeds without invoking handlers.
   *
   * Extension slots are appended to keep method offsets stable within the
   * current shared-library ABI line.
   */
  int (*query_keys)(lc_client *self, const lc_query_req *req,
                    const lc_query_key_handler *handler, void *context,
                    lc_query_res *out, lc_error *error);
  /**
   * Opens a durable Pouch history-retention consumer.
   *
   * Remote lockd clients currently reject this local-storage operation.
   */
  int (*new_history_consumer)(lc_client *self,
                              const lc_history_consumer_config *config,
                              lc_history_consumer **out, lc_error *error);
  /**
   * Streams a state document from an explicit namespace into `dst`.
   *
   * This is the namespace-aware counterpart to `get()`. It is useful for
   * component records that deliberately live outside the client's default
   * namespace. The namespace must be nonempty.
   */
  int (*get_in_namespace)(lc_client *self, const char *ns, const char *key,
                          const lc_get_opts *opts, lc_sink *dst,
                          lc_get_res *out, lc_error *error);
  /**
   * Reserved ABI-4 extension slots. They are always `NULL`; callers must not
   * inspect or modify them.
   */
  void *reserved_extension_slots[7];
  /** Creates an independently atomic journal collection. Pouch is supported;
   * remote lockd returns LC_ERR_UNSUPPORTED until it implements collections. */
  int (*create_collection)(lc_client *self, const lc_collection_config *config,
                           lc_collection **out, lc_error *error);
  /** Opens an existing collection incarnation; never creates one or renews
   * retention.
   */
  int (*open_collection)(lc_client *self, const lc_collection_config *config,
                         lc_collection **out, lc_error *error);
  /** Visits namespace collection metadata in name order, without reading
   * entry bodies.
   */
  int (*list_collections)(lc_client *self, const char *ns,
                          lc_collection_visit_fn visitor, void *context,
                          lc_error *error);
  /** Observes endpoint-local support; remote collection operations remain
   * unsupported.
   */
  int (*collection_capabilities)(lc_client *self,
                                 lc_collection_capabilities *out,
                                 lc_error *error);
};

/**
 * Returns the semantic version string compiled into this build.
 *
 * The returned process-lifetime string is borrowed and must not be freed.
 */
const char *lc_version_string(void);

/**
 * Resets an `lc_error` to a known empty state.
 *
 * Accepts `NULL`. Cleanup an error that already owns fields before
 * reinitializing it.
 */
void lc_error_init(lc_error *error);
/**
 * Releases all heap-owned fields inside an `lc_error` and zeroes it.
 *
 * Accepts `NULL` and an already-zeroed error.
 */
void lc_error_cleanup(lc_error *error);
/**
 * Replaces `error` with an owned diagnostic and returns `code`.
 *
 * All supplied strings are copied, so callback implementations may pass
 * transient storage. `error` may be `NULL` when only the status is needed.
 */
int lc_error_set(lc_error *error, int code, long http_status,
                 const char *message, const char *detail,
                 const char *server_code, const char *correlation_id);
/**
 * Mints a sortable, rs/xid-compatible identifier into `out`.
 *
 * `out` must provide at least `LC_XID_STRING_SIZE` bytes. The result is the
 * canonical 20-character lowercase base32-hex form (`[0-9a-v]{20}`), accepted
 * by lockd wherever it accepts an XID transaction id. The helper returns an
 * actionable error if its process-wide XID generator cannot initialize.
 */
int lc_xid_new(char out[LC_XID_STRING_SIZE], lc_error *error);
/** Reports whether text is a canonical rs/xid identifier. */
int lc_xid_is_valid(const char *text);
/**
 * Initializes an allocator override to use the default allocator.
 *
 * Accepts `NULL`. All-zero hooks select the process-wide built-in allocator.
 */
void lc_allocator_init(lc_allocator *allocator);
/**
 * Selects allocation tracking for the process-wide built-in allocator.
 *
 * Tracking defaults to 0 (off); pass 1 to enable exact live allocation counts
 * and requested payload bytes at ordinary exit or shared-library unload, after
 * library-owned global resources have been released. A
 * clean tracked shutdown is silent. Custom allocator hooks, raw facade result
 * metadata, and allocations made by the host or dependencies are excluded.
 *
 * Call once at process startup, before any SDK allocation or concurrent SDK
 * use. This precondition also applies when an earlier SDK call used the
 * implicit default. The allocator cannot be switched while objects may still be
 * alive. Omitting this call leaves tracking off. Invalid values and repeated
 * successful configuration return LC_ERR_INVALID without changing the selected
 * allocator. Allocations dispatch directly to the selected callbacks, with no
 * per-allocation tracking-flag test. Abrupt termination does not run the
 * diagnostic.
 */
int lc_allocator_tracking_configure(int enabled, lc_error *error);
/**
 * Initializes client configuration with public defaults.
 *
 * Accepts `NULL`. The initialized config has a 30-second timeout, prefers
 * HTTP/2, uses the default 100 MiB typed-JSON limit, enables normal mTLS peer
 * verification, and leaves endpoint, namespace, logger, allocator, and Pouch
 * options unset.
 */
void lc_client_config_init(lc_client_config *config);

/**
 * @name Request and callback initializers
 *
 * Every initializer in this group accepts `NULL` and otherwise resets the
 * entire object. Request initializers produce the documented zero/default
 * behavior; they do not release fields previously owned by the object.
 * @{ */
/** Initializes a lease reference to all-zero/empty values. */
void lc_lease_ref_init(lc_lease_ref *lease);
/** Initializes an acquire request to all-zero/empty values. */
void lc_acquire_req_init(lc_acquire_req *request);
/** Initializes a describe request to all-zero/empty values. */
void lc_describe_req_init(lc_describe_req *request);
/** Initializes get options to all-zero/empty values. */
void lc_get_opts_init(lc_get_opts *options);
/** Initializes update options to all-zero/empty values. */
void lc_update_opts_init(lc_update_opts *options);
/** Initializes a client-level update request to all-zero/empty values. */
void lc_update_req_init(lc_update_req *request);
/** Initializes a mutate request to all-zero/empty values. */
void lc_mutate_req_init(lc_mutate_req *request);
/** Initializes a local mutate request to all-zero/empty values. */
void lc_mutate_local_req_init(lc_mutate_local_req *request);
/** Initializes a client-level mutate operation to all-zero/empty values. */
void lc_mutate_op_init(lc_mutate_op *request);
/** Initializes a metadata request to all-zero/empty values. */
void lc_metadata_req_init(lc_metadata_req *request);
/** Initializes a client-level metadata operation to all-zero/empty values. */
void lc_metadata_op_init(lc_metadata_op *request);
/** Initializes a remove request to all-zero/empty values. */
void lc_remove_req_init(lc_remove_req *request);
/** Initializes a client-level remove operation to all-zero/empty values. */
void lc_remove_op_init(lc_remove_op *request);
/** Initializes a keepalive request to all-zero/empty values. */
void lc_keepalive_req_init(lc_keepalive_req *request);
/** Initializes a client-level keepalive operation to all-zero/empty values. */
void lc_keepalive_op_init(lc_keepalive_op *request);
/** Initializes a release request to all-zero/empty values. */
void lc_release_req_init(lc_release_req *request);
/** Initializes a client-level release operation to all-zero/empty values. */
void lc_release_op_init(lc_release_op *request);
/** Initializes a query request to all-zero/empty values. */
void lc_query_req_init(lc_query_req *request);
/** Initializes a namespace configuration request to all-zero/empty values. */
void lc_namespace_config_req_init(lc_namespace_config_req *request);
/** Initializes an index flush request to all-zero/empty values. */
void lc_index_flush_req_init(lc_index_flush_req *request);
/** Initializes a transaction replay request to all-zero/empty values. */
void lc_txn_replay_req_init(lc_txn_replay_req *request);
/** Initializes a transaction decision request to all-zero/empty values. */
void lc_txn_decision_req_init(lc_txn_decision_req *request);
/** Initializes a TC lease acquire request to all-zero/empty values. */
void lc_tc_lease_acquire_req_init(lc_tc_lease_acquire_req *request);
/** Initializes a TC lease renew request to all-zero/empty values. */
void lc_tc_lease_renew_req_init(lc_tc_lease_renew_req *request);
/** Initializes a TC lease release request to all-zero/empty values. */
void lc_tc_lease_release_req_init(lc_tc_lease_release_req *request);
/** Initializes a TC cluster announce request to all-zero/empty values. */
void lc_tc_cluster_announce_req_init(lc_tc_cluster_announce_req *request);
/** Initializes a TC RM register request to all-zero/empty values. */
void lc_tc_rm_register_req_init(lc_tc_rm_register_req *request);
/** Initializes a TC RM unregister request to all-zero/empty values. */
void lc_tc_rm_unregister_req_init(lc_tc_rm_unregister_req *request);
/** Initializes an enqueue request to all-zero/empty values. */
void lc_enqueue_req_init(lc_enqueue_req *request);
/** Initializes a dequeue request to all-zero/empty values. */
void lc_dequeue_req_init(lc_dequeue_req *request);
/** Initializes a queue stats request to all-zero/empty values. */
void lc_queue_stats_req_init(lc_queue_stats_req *request);
/** Initializes a message reference to all-zero/empty values. */
void lc_message_ref_init(lc_message_ref *message);
/** Initializes a nack request to all-zero/empty values. */
void lc_nack_req_init(lc_nack_req *request);
/** Initializes a client-level nack operation to all-zero/empty values. */
void lc_nack_op_init(lc_nack_op *request);
/** Initializes an extend request to all-zero/empty values. */
void lc_extend_req_init(lc_extend_req *request);
/** Initializes a client-level extend operation to all-zero/empty values. */
void lc_extend_op_init(lc_extend_op *request);
/** Initializes a queue watch request to all-zero/empty values. */
void lc_watch_queue_req_init(lc_watch_queue_req *request);
/** Initializes a watch handler registration to all-zero/empty values. */
void lc_watch_handler_init(lc_watch_handler *handler);
/** Initializes a consumer registration to all-zero/empty values. */
void lc_consumer_init(lc_consumer *consumer);
/** Initializes a consumer restart policy to Go-SDK-compatible defaults. */
void lc_consumer_restart_policy_init(lc_consumer_restart_policy *policy);
/**
 * Initializes one managed consumer config with one worker and the default
 * restart policy; all other fields are zero/empty.
 */
void lc_consumer_config_init(lc_consumer_config *config);
/** Initializes a consumer service config to all-zero/empty values. */
void lc_consumer_service_config_init(lc_consumer_service_config *config);
/** Initializes a history-consumer config with a zero acknowledgement. */
void lc_history_consumer_config_init(lc_history_consumer_config *config);
/** Initializes an inbox/outbox config to all-zero/empty values. */
void lc_outbox_config_init(lc_outbox_config *config);
/** Clears an outbox commit result without releasing any prior contents. */
void lc_outbox_commit_result_init(lc_outbox_commit_result *result);
/** Releases every receipt owned by an outbox commit result. */
void lc_outbox_commit_result_cleanup(lc_outbox_commit_result *result);
/** Initializes a dead-letter export request to JSON with the default bound. */
void lc_dead_letter_export_opts_init(lc_dead_letter_export_opts *options);
/** Clears a dead-letter export result to zero records. */
void lc_dead_letter_export_res_init(lc_dead_letter_export_res *result);
/** Clears an outbox observability snapshot to zero/empty values. */
void lc_outbox_stats_init(lc_outbox_stats *stats);
/** Releases strings owned by an outbox observability snapshot. */
void lc_outbox_stats_cleanup(lc_outbox_stats *stats);
/** Initializes an outbox envelope request to all-zero/empty values. */
void lc_outbox_entry_init(lc_outbox_entry *entry);
/** Initializes an inbox identity request to all-zero/empty values. */
void lc_inbox_message_init(lc_inbox_message *message);
/** Initializes a command identity to all-zero/empty values. */
void lc_command_identity_init(lc_command_identity *identity);
/** Initializes a command acceptance request to all-zero/empty values. */
void lc_command_request_init(lc_command_request *request);
/** Initializes a terminal command result to all-zero/empty values. */
void lc_command_result_init(lc_command_result *result);
/** Initializes an owned command receipt to all-zero/empty values. */
void lc_command_receipt_init(lc_command_receipt *receipt);
/** Releases strings owned by a command receipt. */
void lc_command_receipt_cleanup(lc_command_receipt *receipt);
/** Initializes optional outbox completion evidence to all-zero/empty values. */
void lc_outbox_completion_init(lc_outbox_completion *completion);
/** Clears a retry request so policy chooses its delay and no diagnostic. */
void lc_outbox_retry_init(lc_outbox_retry *request);
/** Initializes an outbox participant request to all-zero/empty values. */
void lc_outbox_participant_request_init(lc_outbox_participant_request *request);
/** Initializes an outbox receipt to all-zero/empty values. */
void lc_outbox_receipt_init(lc_outbox_receipt *receipt);
/** Releases strings owned by an outbox receipt. */
void lc_outbox_receipt_cleanup(lc_outbox_receipt *receipt);
/** Initializes an attachment selector to all-zero/empty values. */
void lc_attachment_selector_init(lc_attachment_selector *selector);
/** Initializes an attachment upload request to all-zero/empty values. */
void lc_attach_req_init(lc_attach_req *request);
/** Initializes a client-level attachment upload operation to all-zero/empty
 * values. */
void lc_attach_op_init(lc_attach_op *request);
/** Initializes an attachment get request to all-zero/empty values. */
void lc_attachment_get_req_init(lc_attachment_get_req *request);
/** Initializes an attachment list request to all-zero/empty values. */
void lc_attachment_list_req_init(lc_attachment_list_req *request);
/** Initializes a client-level attachment get operation to all-zero/empty
 * values. */
void lc_attachment_get_op_init(lc_attachment_get_op *request);
/** Initializes a client-level attachment delete operation to all-zero/empty
 * values. */
void lc_attachment_delete_op_init(lc_attachment_delete_op *request);
/** Initializes a client-level delete-all-attachments operation to
 * all-zero/empty values. */
void lc_attachment_delete_all_op_init(lc_attachment_delete_all_op *request);
/** @} */

/**
 * Opens a new client handle.
 *
 * Pouch open automatically upgrades released collection heads under exclusive
 * root access, preserving entry files. Close all other clients for first
 * upgrade; contention returns LC_ERR_PENDING/collection_upgrade_busy. Later
 * opens skip migration. Older binaries cannot use the upgraded collections.
 *
 * @param config Client construction settings.
 * @param out Receives the newly allocated client on success.
 * @param error Optional structured error output.
 * @return `LC_OK` on success, otherwise an error status code.
 */
int lc_client_open(const lc_client_config *config, lc_client **out,
                   lc_error *error);

/**
 * Creates a rewindable source backed by caller-provided memory.
 *
 * The bytes are borrowed; they must remain valid and unchanged until the
 * source is closed. The caller owns the returned source.
 */
int lc_source_from_memory(const void *bytes, size_t length, lc_source **out,
                          lc_error *error);
/**
 * Opens a rewindable source backed by a file path.
 *
 * The returned source owns its file descriptor and closes it from
 * `lc_source_close()`. The caller owns the returned source.
 */
int lc_source_from_file(const char *path, lc_source **out, lc_error *error);
/**
 * Wraps a borrowed file descriptor as a single-pass source.
 *
 * Closing the source does not close `fd`; the caller retains descriptor
 * ownership. The source reads from and advances the descriptor's current file
 * offset. Pass a duplicate when the source needs an independent offset or
 * lifetime.
 */
int lc_source_from_fd(int fd, lc_source **out, lc_error *error);
/**
 * Wraps caller-provided callbacks as a source.
 *
 * `read` is required. `reset` may be `NULL` for single-pass streams, in which
 * case APIs that require a rewindable source return `LC_ERR_INVALID`. `close`
 * may be `NULL`; when present it is called from `lc_source_close()`. The
 * caller owns the returned source.
 */
int lc_source_from_callbacks(lc_source_read_fn read, lc_source_reset_fn reset,
                             lc_source_close_fn close, void *context,
                             lc_source **out, lc_error *error);
/**
 * Creates a sink that truncates and writes a file path.
 *
 * The sink owns the opened descriptor and closes it with `lc_sink_close()`.
 */
int lc_sink_to_file(const char *path, lc_sink **out, lc_error *error);
/**
 * Creates a sink that writes bytes to a borrowed file descriptor.
 *
 * Closing the sink does not close `fd`; the caller retains descriptor
 * ownership. Pass a duplicate when the sink needs an independent lifetime.
 */
int lc_sink_to_fd(int fd, lc_sink **out, lc_error *error);
/** Creates a caller-owned sink that accepts and discards all bytes. */
int lc_sink_to_discard(lc_sink **out, lc_error *error);
/** Returns non-zero when a sink was created by `lc_sink_to_discard()`. */
int lc_sink_is_discard(const lc_sink *sink);
/**
 * Creates a caller-owned in-memory sink.
 *
 * The sink grows as bytes are written and releases its accumulated buffer when
 * closed.
 */
int lc_sink_to_memory(lc_sink **out, lc_error *error);
/**
 * Returns the bytes accumulated by an in-memory sink.
 *
 * The returned buffer is borrowed from `sink` and remains valid until the sink
 * is written again or closed.
 */
int lc_sink_memory_bytes(lc_sink *sink, const void **bytes, size_t *length,
                         lc_error *error);
/**
 * Copies all bytes from a source into a sink.
 *
 * The copy is streaming and starts with a small bounded chunk. Longer copies
 * use a bounded heap buffer from the built-in allocator; allocation failure
 * returns `LC_ERR_NOMEM`. Errors can leave either stream partially advanced.
 * It resets `error`
 * for this operation before reading the source. It advances `src`, does not
 * reset it, and leaves both handles open. `written` may be `NULL`; on success
 * it receives the total bytes copied.
 */
int lc_copy(lc_source *src, lc_sink *dst, size_t *written, lc_error *error);

/**
 * Constructs a canonical `pouch://` endpoint from an absolute root and query
 * options.
 *
 * The root, names, and values are decoded filesystem/query strings. The
 * library percent-encodes every reserved byte, preserving `/` in the absolute
 * root; callers must not compose or escape endpoint syntax themselves. A
 * `NULL` option value emits a bare option, while an empty string emits
 * `name=`. The returned endpoint is owned by the caller and must be released
 * with `lc_pouch_endpoint_free()`.
 */
int lc_pouch_endpoint_build(const char *root_path,
                            const lc_pouch_endpoint_option *options,
                            size_t option_count, char **out, lc_error *error);

/**
 * Reports whether a decoded query option name is present in a Pouch endpoint.
 *
 * The inspection follows the same percent-decoding and fragment handling as
 * Pouch endpoint opening. It never exposes option values. On success `present`
 * is zero or one; duplicate and bare options count as present.
 */
int lc_pouch_endpoint_has_option(const char *endpoint, const char *name,
                                 int *present, lc_error *error);

/** Releases an endpoint returned by `lc_pouch_endpoint_build()`. Accepts
 * `NULL`.
 */
void lc_pouch_endpoint_free(char *endpoint);

/** Generates a new `lc-pouch-key-v1:<base64url>` root key string.
 *
 * On success `out` receives a liblockdc-owned allocation transferred to the
 * caller. Release it with `lc_pouch_crypto_key_string_free()`.
 */
int lc_pouch_crypto_generate_key_string(char **out, lc_error *error);

/**
 * Releases a key string or path returned by Pouch crypto helpers.
 *
 * Accepts `NULL`. Do not use this helper for caller-owned key strings.
 */
void lc_pouch_crypto_key_string_free(char *key_string);

/** Returns the default Pouch crypto key-file path.
 *
 * The path is `$XDG_CONFIG_HOME/liblockdc/pouch.key`, or
 * `$HOME/.config/liblockdc/pouch.key` when `XDG_CONFIG_HOME` is unset. The
 * returned string must be released with
 * `lc_pouch_crypto_key_string_free()`.
 */
int lc_pouch_crypto_default_key_file(char **out, lc_error *error);

/** Generates and writes a Pouch root key file with mode `0600`.
 *
 * Parent directories are created with mode `0700`; the key file's parent must
 * be owned by the current user and have mode `0700`. When `overwrite` is
 * zero, an existing key file is rejected. When `key_string_out` is non-NULL,
 * the generated key string is returned and must be released with
 * `lc_pouch_crypto_key_string_free()`.
 */
int lc_pouch_crypto_generate_key_file(const char *path, int overwrite,
                                      char **key_string_out, lc_error *error);

/** Closes and frees a client handle. Accepts `NULL`. */
void lc_client_close(lc_client *client);
/** Closes and frees a lease handle. Accepts `NULL`. */
void lc_lease_close(lc_lease *lease);
/**
 * Returns an independently closable copy of a non-staged lease, or `NULL`.
 *
 * A `NULL` source, an exhausted allocator, or a transaction-staged lease
 * returns `NULL`; staged leases remain exclusively owned by their outbox
 * transaction and cannot be cloned.
 */
lc_lease *lc_lease_clone(const lc_lease *lease);
/** Closes and frees a message handle. Accepts `NULL`. */
void lc_message_close(lc_message *message);
/** Closes and frees a source. Accepts `NULL`. */
void lc_source_close(lc_source *source);
/** Closes and frees a sink. Accepts `NULL`. */
void lc_sink_close(lc_sink *sink);

/**
 * @name Owned response cleanup
 *
 * These helpers release all nested strings, arrays, handles, and metadata
 * owned by their response, then zero the object. Every helper accepts `NULL`
 * and an already-zeroed object.
 * @{ */
/** Releases fields owned by an `lc_describe_res`. */
void lc_describe_res_cleanup(lc_describe_res *response);
/** Releases fields owned by an `lc_get_res`. */
void lc_get_res_cleanup(lc_get_res *response);
/** Releases fields owned by an `lc_update_res`. */
void lc_update_res_cleanup(lc_update_res *response);
/** Releases fields owned by an `lc_mutate_res`. */
void lc_mutate_res_cleanup(lc_mutate_res *response);
/** Releases fields owned by an `lc_metadata_res`. */
void lc_metadata_res_cleanup(lc_metadata_res *response);
/** Releases fields owned by an `lc_remove_res`. */
void lc_remove_res_cleanup(lc_remove_res *response);
/** Releases fields owned by an `lc_keepalive_res`. */
void lc_keepalive_res_cleanup(lc_keepalive_res *response);
/** Releases fields owned by an `lc_release_res`. */
void lc_release_res_cleanup(lc_release_res *response);
/** Releases fields owned by an `lc_query_res`. */
void lc_query_res_cleanup(lc_query_res *response);
/** Releases every string and the array owned by an `lc_string_list`. */
void lc_string_list_cleanup(lc_string_list *response);
/** Releases fields owned by an `lc_namespace_config_res`. */
void lc_namespace_config_res_cleanup(lc_namespace_config_res *response);
/** Releases fields owned by an `lc_index_flush_res`. */
void lc_index_flush_res_cleanup(lc_index_flush_res *response);
/** Releases fields owned by an `lc_txn_replay_res`. */
void lc_txn_replay_res_cleanup(lc_txn_replay_res *response);
/** Releases fields owned by an `lc_txn_decision_res`. */
void lc_txn_decision_res_cleanup(lc_txn_decision_res *response);
/** Releases fields owned by an `lc_tc_lease_acquire_res`. */
void lc_tc_lease_acquire_res_cleanup(lc_tc_lease_acquire_res *response);
/** Releases fields owned by an `lc_tc_lease_renew_res`. */
void lc_tc_lease_renew_res_cleanup(lc_tc_lease_renew_res *response);
/** Releases fields owned by an `lc_tc_lease_release_res`. */
void lc_tc_lease_release_res_cleanup(lc_tc_lease_release_res *response);
/** Releases fields owned by an `lc_tc_leader_res`. */
void lc_tc_leader_res_cleanup(lc_tc_leader_res *response);
/** Releases fields owned by an `lc_tc_cluster_res`. */
void lc_tc_cluster_res_cleanup(lc_tc_cluster_res *response);
/** Releases fields owned by an `lc_tc_rm_res`. */
void lc_tc_rm_res_cleanup(lc_tc_rm_res *response);
/** Releases every backend entry owned by an `lc_tc_rm_list_res`. */
void lc_tc_rm_list_res_cleanup(lc_tc_rm_list_res *response);
/** Releases fields owned by an `lc_enqueue_res`. */
void lc_enqueue_res_cleanup(lc_enqueue_res *response);
/** Releases fields owned by an `lc_queue_stats_res`. */
void lc_queue_stats_res_cleanup(lc_queue_stats_res *response);
/** Releases fields owned by an `lc_ack_res`. */
void lc_ack_res_cleanup(lc_ack_res *response);
/** Releases fields owned by an `lc_nack_res`. */
void lc_nack_res_cleanup(lc_nack_res *response);
/** Releases fields owned by an `lc_extend_res`. */
void lc_extend_res_cleanup(lc_extend_res *response);
/** Closes every message still owned by a dequeue batch and clears it. */
void lc_dequeue_batch_cleanup(lc_dequeue_batch_res *response);
/** Releases fields owned by an `lc_watch_event`. */
void lc_watch_event_cleanup(lc_watch_event *event);
/** Releases fields owned by an `lc_attachment_info`. */
void lc_attachment_info_cleanup(lc_attachment_info *info);
/** Releases fields owned by an `lc_attach_res`. */
void lc_attach_res_cleanup(lc_attach_res *response);
/** Releases every attachment owned by an `lc_attachment_list`. */
void lc_attachment_list_cleanup(lc_attachment_list *response);
/** Releases fields owned by an `lc_attachment_get_res`. */
void lc_attachment_get_res_cleanup(lc_attachment_get_res *response);
/** @} */

/** Acquires a lease and returns a bound `lc_lease` handle for follow-up work.
 */
int lc_acquire(lc_client *client, const lc_acquire_req *req, lc_lease **out,
               lc_error *error);
/**
 * Acquires a lease, spools the complete private snapshot to a temporary file,
 * runs `handler`, and always attempts to release the lease before returning.
 * The handler receives a borrowed lease and must not close or release it.
 * Handler success commits staged changes; handler failure releases with
 * rollback. Use lc_acquire() and lc_lease_get() with a sink for transfer
 * without snapshot staging.
 */
int lc_acquire_for_update(lc_client *client, const lc_acquire_req *req,
                          lc_acquire_for_update_handler_fn handler,
                          void *handler_context, lc_error *error);
/** Describes the current lease and state metadata for a key. */
int lc_describe(lc_client *client, const lc_describe_req *req,
                lc_describe_res *out, lc_error *error);
/** Streams a state document into `dst`. Prefer the lease method once acquired.
 */
int lc_get(lc_client *client, const char *key, const lc_get_opts *opts,
           lc_sink *dst, lc_get_res *out, lc_error *error);
/**
 * Streams a state document from an explicit namespace into `dst`.
 *
 * This is the namespace-aware counterpart to `lc_get()`. It is useful for
 * component records that deliberately live outside the client's default
 * namespace. `ns` must be nonempty.
 */
int lc_get_in_namespace(lc_client *client, const char *ns, const char *key,
                        const lc_get_opts *opts, lc_sink *dst, lc_get_res *out,
                        lc_error *error);
/**
 * Parses state into a caller-provided struct through a lonejson map.
 *
 * Ordinary mapped fields are materialized. Spool-backed lonejson fields may
 * remain file-backed; use `lc_get()` with a sink for fully streamed transfer.
 */
int lc_load(lc_client *client, const char *key, const lonejson_map *map,
            void *dst, const lc_get_opts *opts, lc_get_res *out,
            lc_error *error);
/**
 * Parses state from an explicit namespace into `dst` through a lonejson map.
 *
 * This is the namespace-aware counterpart to `lc_load()`. It is useful for
 * component records that deliberately live outside the client's default
 * namespace.
 */
int lc_load_in_namespace(lc_client *client, const char *ns, const char *key,
                         const lonejson_map *map, void *dst,
                         const lc_get_opts *opts, lc_get_res *out,
                         lc_error *error);
/** Updates an existing, credentialed lease reference from a streamed source. */
int lc_update(lc_client *client, const lc_update_req *req, lc_source *src,
              lc_update_res *out, lc_error *error);
/** Applies one or more server-side mutations to a credentialed lease
 * reference. */
int lc_mutate(lc_client *client, const lc_mutate_op *req, lc_mutate_res *out,
              lc_error *error);
/** Updates metadata fields on a credentialed lease reference. */
int lc_metadata(lc_client *client, const lc_metadata_op *req,
                lc_metadata_res *out, lc_error *error);
/** Removes state bytes for a credentialed lease reference while keeping the
 * lease alive. */
int lc_remove(lc_client *client, const lc_remove_op *req, lc_remove_res *out,
              lc_error *error);
/** Renews an existing lease reference without a bound `lc_lease` handle. */
int lc_keepalive(lc_client *client, const lc_keepalive_op *req,
                 lc_keepalive_res *out, lc_error *error);
/** Releases an existing lease reference without a bound `lc_lease` handle. */
int lc_release(lc_client *client, const lc_release_op *req, lc_release_res *out,
               lc_error *error);
/** Streams an attachment upload for a credentialed lease reference. */
int lc_attach(lc_client *client, const lc_attach_op *req, lc_source *src,
              lc_attach_res *out, lc_error *error);
/** Lists attachments associated with a lease reference. */
int lc_list_attachments(lc_client *client, const lc_attachment_list_req *req,
                        lc_attachment_list *out, lc_error *error);
/** Streams an attachment download into `dst`. */
int lc_get_attachment(lc_client *client, const lc_attachment_get_op *req,
                      lc_sink *dst, lc_attachment_get_res *out,
                      lc_error *error);
/** Deletes one attachment through a credentialed lease reference. */
int lc_delete_attachment(lc_client *client, const lc_attachment_delete_op *req,
                         int *deleted, lc_error *error);
/** Deletes all attachments through a credentialed lease reference. */
int lc_delete_all_attachments(lc_client *client,
                              const lc_attachment_delete_all_op *req,
                              int *deleted_count, lc_error *error);
/** Returns queue depth and delivery metadata. */
int lc_queue_stats(lc_client *client, const lc_queue_stats_req *req,
                   lc_queue_stats_res *out, lc_error *error);
/** Acknowledges a queue message from explicit client-level identifiers. */
int lc_queue_ack(lc_client *client, const lc_ack_op *req, lc_ack_res *out,
                 lc_error *error);
/**
 * Negatively acknowledges a queue message from explicit identifiers.
 *
 * `req->intent` controls whether the server records a processing failure or an
 * intentional deferral.
 */
int lc_queue_nack(lc_client *client, const lc_nack_op *req, lc_nack_res *out,
                  lc_error *error);
/** Extends queue message visibility from explicit client-level identifiers. */
int lc_queue_extend(lc_client *client, const lc_extend_op *req,
                    lc_extend_res *out, lc_error *error);
/** Streams query results into `dst`. */
int lc_query(lc_client *client, const lc_query_req *req, lc_sink *dst,
             lc_query_res *out, lc_error *error);
/** Streams key query result strings through chunked callbacks.
 * An empty response may omit `keys`; it succeeds without invoking handlers.
 */
int lc_query_keys(lc_client *client, const lc_query_req *req,
                  const lc_query_key_handler *handler, void *context,
                  lc_query_res *out, lc_error *error);
/** Reads namespace-level engine configuration. */
int lc_get_namespace_config(lc_client *client,
                            const lc_namespace_config_req *req,
                            lc_namespace_config_res *out, lc_error *error);
/** Updates namespace-level engine configuration. */
int lc_update_namespace_config(lc_client *client,
                               const lc_namespace_config_req *req,
                               lc_namespace_config_res *out, lc_error *error);
/** Triggers an index flush for the selected namespace. */
int lc_flush_index(lc_client *client, const lc_index_flush_req *req,
                   lc_index_flush_res *out, lc_error *error);
/** Replays a durable decision. Expired raw prepare becomes rollback; managed
 * undecided work requires fenced coordinator recovery. On LC_ERR_PENDING,
 * out retains the decision while participant application remains incomplete. */
int lc_txn_replay(lc_client *client, const lc_txn_replay_req *req,
                  lc_txn_replay_res *out, lc_error *error);
/** Prepares a transaction decision. */
int lc_txn_prepare(lc_client *client, const lc_txn_decision_req *req,
                   lc_txn_decision_res *out, lc_error *error);
/** Records COMMIT and applies/releases matching participants. Pouch returns
 * LC_ERR_PENDING with out populated while external completion is unproven. */
int lc_txn_commit(lc_client *client, const lc_txn_decision_req *req,
                  lc_txn_decision_res *out, lc_error *error);
/** Records/applies ABORT without reversing COMMIT. Pouch may return
 * LC_ERR_PENDING with out populated while external cleanup is incomplete. */
int lc_txn_rollback(lc_client *client, const lc_txn_decision_req *req,
                    lc_txn_decision_res *out, lc_error *error);
/** Acquires a TC lease. */
int lc_tc_lease_acquire(lc_client *client, const lc_tc_lease_acquire_req *req,
                        lc_tc_lease_acquire_res *out, lc_error *error);
/** Renews a TC lease. */
int lc_tc_lease_renew(lc_client *client, const lc_tc_lease_renew_req *req,
                      lc_tc_lease_renew_res *out, lc_error *error);
/** Releases a TC lease. */
int lc_tc_lease_release(lc_client *client, const lc_tc_lease_release_req *req,
                        lc_tc_lease_release_res *out, lc_error *error);
/** Returns current TC leader information. */
int lc_tc_leader(lc_client *client, lc_tc_leader_res *out, lc_error *error);
/** Announces the current node into the TC cluster set. */
int lc_tc_cluster_announce(lc_client *client,
                           const lc_tc_cluster_announce_req *req,
                           lc_tc_cluster_res *out, lc_error *error);
/** Removes the current node from the TC cluster set. */
int lc_tc_cluster_leave(lc_client *client, lc_tc_cluster_res *out,
                        lc_error *error);
/** Lists the current TC cluster set. */
int lc_tc_cluster_list(lc_client *client, lc_tc_cluster_res *out,
                       lc_error *error);
/** Registers a TC resource manager. */
int lc_tc_rm_register(lc_client *client, const lc_tc_rm_register_req *req,
                      lc_tc_rm_res *out, lc_error *error);
/** Unregisters a TC resource manager. */
int lc_tc_rm_unregister(lc_client *client, const lc_tc_rm_unregister_req *req,
                        lc_tc_rm_res *out, lc_error *error);
/** Lists registered TC resource managers. */
int lc_tc_rm_list(lc_client *client, lc_tc_rm_list_res *out, lc_error *error);
/** Streams a queue payload upload and enqueues it. */
int lc_enqueue(lc_client *client, const lc_enqueue_req *req, lc_source *src,
               lc_enqueue_res *out, lc_error *error);
/**
 * Dequeues a message and returns an `lc_message` handle for follow-up calls.
 * On error, no delivery lease is retained by the client.
 */
int lc_dequeue(lc_client *client, const lc_dequeue_req *req, lc_message **out,
               lc_error *error);
/**
 * Dequeues up to `req->page_size` messages and returns them as a batch.
 * On error, no delivery lease is retained by the client.
 */
int lc_dequeue_batch(lc_client *client, const lc_dequeue_req *req,
                     lc_dequeue_batch_res *out, lc_error *error);
/**
 * Dequeues a message and its associated state lease handle atomically.
 * On error, neither lease is retained by the client.
 */
int lc_dequeue_with_state(lc_client *client, const lc_dequeue_req *req,
                          lc_message **out, lc_error *error);
/** Consumes queue messages with a streaming callback. */
int lc_subscribe(lc_client *client, const lc_dequeue_req *req,
                 const lc_consumer *consumer, lc_error *error);
/** Consumes queue messages with attached state leases. */
int lc_subscribe_with_state(lc_client *client, const lc_dequeue_req *req,
                            const lc_consumer *consumer, lc_error *error);
/**
 * Creates a managed consumer service.
 *
 * Use the returned handle for the long-running worker-service model. `run()`
 * blocks until the service stops; `start()`/`stop()`/`wait()` provide explicit
 * control when embedding into an existing daemon.
 */
int lc_client_new_consumer_service(lc_client *client,
                                   const lc_consumer_service_config *config,
                                   lc_consumer_service **out, lc_error *error);
/** Creates a threadless inbox/outbox producer; clears `*out` on error.
 * Outbox workflows currently require Pouch; remote lockd is unsupported.
 */
int lc_client_new_outbox(lc_client *client, const lc_outbox_config *config,
                         lc_outbox **out, lc_error *error);
/**
 * Creates a threadless outbox attached to a compatible explicit dispatcher.
 * Clears `*out` on error.
 */
int lc_client_new_outbox_with_dispatcher(lc_client *client,
                                         const lc_outbox_config *config,
                                         lc_outbox_dispatcher *dispatcher,
                                         lc_outbox **out, lc_error *error);
/** Opens a durable Pouch history-retention consumer. */
int lc_client_new_history_consumer(lc_client *client,
                                   const lc_history_consumer_config *config,
                                   lc_history_consumer **out, lc_error *error);
/** Watches queue depth changes with a streaming watch callback. */
int lc_watch_queue(lc_client *client, const lc_watch_queue_req *req,
                   const lc_watch_handler *handler, lc_error *error);

/** Refreshes the published fields on a bound lease handle. */
int lc_lease_describe(lc_lease *lease, lc_error *error);
/** Streams the state document for a bound lease into `dst`. */
int lc_lease_get(lc_lease *lease, lc_sink *dst, const lc_get_opts *opts,
                 lc_get_res *out, lc_error *error);
/**
 * Parses bound-lease state into a caller-provided struct through lonejson.
 *
 * Ordinary mapped fields are materialized. Spool-backed lonejson fields may
 * remain file-backed; use `lc_lease_get()` for fully streamed transfer.
 */
int lc_lease_load(lc_lease *lease, const lonejson_map *map, void *dst,
                  const lc_get_opts *opts, lc_get_res *out, lc_error *error);
/** Convenience mapped-struct `update()` variant for lonejson-compatible data.
 * `opts` may be `NULL`; selected retention is applied in the same streamed
 * write.
 */
int lc_lease_save(lc_lease *lease, const lonejson_map *map, const void *src,
                  const lc_update_opts *opts, lc_error *error);
/** Streams a replacement state document into a bound lease. */
int lc_lease_update(lc_lease *lease, lc_source *src, const lc_update_opts *opts,
                    lc_error *error);
/** Applies server-side mutations to the current state of a bound lease. */
int lc_lease_mutate(lc_lease *lease, const lc_mutate_req *req, lc_error *error);
/** Streams the current state through the local mutate engine and updates it. */
int lc_lease_mutate_local(lc_lease *lease, const lc_mutate_local_req *req,
                          lc_error *error);
/** Updates metadata fields on a bound lease. */
int lc_lease_metadata(lc_lease *lease, const lc_metadata_req *req,
                      lc_error *error);
/** Removes the current state bytes while keeping the bound lease alive. */
int lc_lease_remove(lc_lease *lease, const lc_remove_req *req, lc_error *error);
/** Renews a bound lease and refreshes its expiry fields. */
int lc_lease_keepalive(lc_lease *lease, const lc_keepalive_req *req,
                       lc_error *error);
/** Releases a bound lease and closes the handle on success. */
int lc_lease_release(lc_lease *lease, const lc_release_req *req,
                     lc_error *error);
/** Streams an attachment upload onto a bound lease. */
int lc_lease_attach(lc_lease *lease, const lc_attach_req *req, lc_source *src,
                    lc_attach_res *out, lc_error *error);
/** Lists the attachments currently associated with a bound lease. */
int lc_lease_list_attachments(lc_lease *lease, lc_attachment_list *out,
                              lc_error *error);
/** Streams an attachment download from a bound lease into `dst`. */
int lc_lease_get_attachment(lc_lease *lease, const lc_attachment_get_req *req,
                            lc_sink *dst, lc_attachment_get_res *out,
                            lc_error *error);
/** Deletes one attachment associated with a bound lease. */
int lc_lease_delete_attachment(lc_lease *lease,
                               const lc_attachment_selector *selector,
                               int *deleted, lc_error *error);
/** Deletes all attachments associated with a bound lease. */
int lc_lease_delete_all_attachments(lc_lease *lease, int *deleted_count,
                                    lc_error *error);

/** Acknowledges a bound message and closes the handle on success. */
int lc_message_ack(lc_message *message, lc_error *error);
/**
 * Negatively acknowledges a bound message and closes the handle on success.
 *
 * `req->intent` controls whether the server records a processing failure or an
 * intentional deferral.
 */
int lc_message_nack(lc_message *message, const lc_nack_req *req,
                    lc_error *error);
/** Extends visibility for a bound message without closing it. */
int lc_message_extend(lc_message *message, const lc_extend_req *req,
                      lc_error *error);
/**
 * Returns the state lease associated with a bound message, if any.
 *
 * The returned lease is borrowed from `message` and is invalid after a
 * successful message terminal operation or `lc_message_close()`. Use
 * `lc_lease_clone()` before retaining a non-staged view independently.
 */
lc_lease *lc_message_state(lc_message *message);
/**
 * Returns the payload reader owned by the message handle.
 *
 * The borrowed reader may be single-pass for live subscription deliveries;
 * do not close it independently of the message.
 */
lc_source *lc_message_payload(lc_message *message);
/** Rewinds a bound message payload so it can be consumed again. */
int lc_message_rewind_payload(lc_message *message, lc_error *error);
/**
 * Copies a bound message payload into `dst`.
 *
 * Resettable payloads are rewound before copying. Single-pass payloads are
 * copied from their current position.
 */
int lc_message_write_payload(lc_message *message, lc_sink *dst, size_t *written,
                             lc_error *error);
/** Starts all managed consumer loops and blocks until they stop. */
int lc_consumer_service_run(lc_consumer_service *service, lc_error *error);
/** Starts all managed consumer loops asynchronously. */
int lc_consumer_service_start(lc_consumer_service *service, lc_error *error);
/** Requests shutdown of a running managed consumer service. */
int lc_consumer_service_stop(lc_consumer_service *service);
/** Waits for a managed consumer service to stop. */
int lc_consumer_service_wait(lc_consumer_service *service, lc_error *error);
/** Closes and frees a managed consumer service. */
void lc_consumer_service_close(lc_consumer_service *service);

/**
 * Creates one durable outbox effect and its transaction.
 *
 * On a matching existing effect, including `entry->payload_digest`, succeeds
 * with `*out_txn == NULL` and `receipt->duplicate` set. The caller owns and
 * must close a returned transaction. The payload source may close `outbox`
 * safely; this call then returns `LC_ERR_INVALID` with no transaction.
 */
int lc_outbox_append(lc_outbox *outbox, const lc_outbox_entry *entry,
                     lc_source *payload, lc_outbox_transaction **out_txn,
                     lc_outbox_receipt *receipt, lc_error *error);
/**
 * Creates one durable inbox receipt and its transaction.
 *
 * On a matching existing message, succeeds with `*out_txn == NULL` and
 * `result->duplicate` set.
 */
int lc_outbox_accept_inbox(lc_outbox *outbox, const lc_inbox_message *message,
                           lc_outbox_transaction **out_txn,
                           lc_inbox_accept_result *result, lc_error *error);
/**
 * Accepts an idempotent command and starts a transaction for a fresh receipt.
 *
 * A matching receipt succeeds with `*out_txn == NULL` and
 * `receipt->duplicate` set.
 */
int lc_outbox_accept_command(lc_outbox *outbox,
                             const lc_command_request *request,
                             lc_outbox_transaction **out_txn,
                             lc_command_receipt *receipt, lc_error *error);
/** Reads a durable command receipt without claiming or resuming work. */
int lc_outbox_get_command_receipt(lc_outbox *outbox,
                                  const lc_command_identity *identity,
                                  lc_command_receipt *out, lc_error *error);
/** Reads a durable command receipt by its component-generated command id. */
int lc_outbox_get_command_receipt_by_id(lc_outbox *outbox,
                                        const char *command_id,
                                        lc_command_receipt *out,
                                        lc_error *error);
/**
 * Waits for a command receipt to become terminal without performing dispatch.
 * `timeout_ms` is zero for one read, `-1` for no deadline, or positive for a
 * monotonic deadline that also bounds each remote receipt read. A timeout
 * returns `LC_ERR_TIMEOUT` and the latest pending receipt in `out`.
 */
int lc_outbox_wait_command(lc_outbox *outbox, const char *command_id,
                           long timeout_ms, lc_command_receipt *out,
                           lc_error *error);
/**
 * Streams a completed command result attachment into `dst`, if it exists.
 * The sink may close `outbox` safely; it becomes unusable after the call.
 */
int lc_outbox_write_command_result(lc_outbox *outbox,
                                   const lc_command_identity *identity,
                                   lc_sink *dst, size_t *written,
                                   lc_error *error);
/**
 * Starts a transaction for a pending command receipt.
 *
 * A terminal receipt succeeds with `*out_txn == NULL` and is never reopened.
 */
int lc_outbox_resume_command(lc_outbox *outbox,
                             const lc_command_identity *identity,
                             lc_outbox_transaction **out_txn,
                             lc_command_receipt *receipt, lc_error *error);
/** Resumes a pending command by component-generated command id. With valid
 * output arguments, failure clears `*out_txn`; a terminal command succeeds
 * with `*out_txn == NULL` and its current receipt. */
int lc_outbox_resume_command_by_id(lc_outbox *outbox, const char *command_id,
                                   lc_outbox_transaction **out_txn,
                                   lc_command_receipt *receipt,
                                   lc_error *error);
/** Creates a lazy outbox transaction with no durable marker until use. */
int lc_outbox_begin(lc_outbox *outbox, lc_outbox_transaction **out,
                    lc_error *error);
/** Obtains the one compatible explicit dispatcher and clears `*out` on error.
 */
int lc_outbox_dispatcher_get_or_start(lc_outbox *outbox,
                                      lc_outbox_dispatcher **out,
                                      lc_error *error);
/**
 * Releases a threadless outbox producer and its dispatcher reference.
 * It never stops that dispatcher.
 */
void lc_outbox_close(lc_outbox *outbox);
/**
 * Waits for, claims, and returns one job on explicit consumer demand.
 *
 * `timeout_ms == 0` only checks immediately available in-memory work and never
 * starts a recovery query. `-1` waits indefinitely; other negative values are
 * invalid.
 */
int lc_outbox_dispatcher_next(lc_outbox_dispatcher *dispatcher, long timeout_ms,
                              lc_outbox_job **out, lc_error *error);
/**
 * Waits for and claims one stateful outbox job on explicit consumer demand.
 *
 * `timeout_ms == 0` only checks immediately available in-memory work and never
 * starts recovery. A Pouch stateful pull may first reclaim one durably named
 * completed checkpoint; that bounded operation never scans or queries a
 * namespace. `-1` waits indefinitely; other negative values are invalid.
 * On failure `*out` is cleared and no partial claim is handed to the caller.
 * Stateful delivery currently requires a Pouch client; unsupported backends
 * return `LC_ERR_INVALID` rather than silently returning a stateless job.
 */
int lc_outbox_dispatcher_next_with_state(lc_outbox_dispatcher *dispatcher,
                                         long timeout_ms, lc_outbox_job **out,
                                         lc_error *error);
/** Copies a committed outbox key into the dispatcher's bounded wake queue. */
int lc_outbox_dispatcher_notify_outbox_key(lc_outbox_dispatcher *dispatcher,
                                           const char *outbox_key,
                                           lc_error *error);
/** Returns a cheap dispatcher-local observability snapshot. */
int lc_outbox_dispatcher_get_stats(lc_outbox_dispatcher *dispatcher,
                                   lc_outbox_stats *out, lc_error *error);
/** Requests asynchronous bounded durable reconciliation. */
int lc_outbox_dispatcher_reconcile(lc_outbox_dispatcher *dispatcher,
                                   lc_error *error);
/** Returns one dead-lettered effect to pending and signals dispatch. */
int lc_outbox_dispatcher_replay_dead_letter(lc_outbox_dispatcher *dispatcher,
                                            const char *outbox_key,
                                            lc_error *error);
/** Permanently deletes one dead-lettered effect and its payload attachment. */
int lc_outbox_dispatcher_delete_dead_letter(lc_outbox_dispatcher *dispatcher,
                                            const char *outbox_key,
                                            lc_error *error);
/** Streams selected dead-letter envelopes as JSON or JSONL; never payloads. */
int lc_outbox_dispatcher_export_dead_letters(
    lc_outbox_dispatcher *dispatcher, const lc_dead_letter_export_opts *options,
    lc_sink *dst, lc_dead_letter_export_res *out, lc_error *error);
/** Requests stop and waits for private infrastructure and caller-owned jobs.
 * Zero polls; `-1` waits indefinitely; positive milliseconds bound the wait.
 * Timeout leaves the dispatcher stopping and handed-out jobs usable. Finish
 * or close those jobs before an indefinite wait; expiry alone does not release
 * their local handles. Repeated stop calls are safe. */
int lc_outbox_dispatcher_stop(lc_outbox_dispatcher *dispatcher,
                              long deadline_ms, lc_error *error);
/** Observes an already requested stop with the same deadlines as `stop()`.
 * Returns `LC_ERR_INVALID` if stop has not been requested. */
int lc_outbox_dispatcher_wait(lc_outbox_dispatcher *dispatcher,
                              long deadline_ms, lc_error *error);
/** Releases one dispatcher handle reference without stopping it. */
void lc_outbox_dispatcher_close(lc_outbox_dispatcher *dispatcher);
/**
 * Streams the immutable payload of a claimed outbox job into `dst`.
 * The sink may close `job` safely; it becomes unusable after the call.
 */
int lc_outbox_job_write_payload(lc_outbox_job *job, lc_sink *dst,
                                size_t *written, lc_error *error);
/**
 * Returns the borrowed Pouch state lease for a stateful outbox job, or `NULL`.
 * The job owns this handle; do not close it independently. Successful state
 * and attachment mutations are immediately durable under the paired lease's
 * fencing precondition, including when the job later expires.
 */
lc_lease *lc_outbox_job_state(lc_outbox_job *job);
/** Extends a claimed outbox job before a longer foreign operation. */
int lc_outbox_job_renew(lc_outbox_job *job, long ttl_seconds, lc_error *error);
/**
 * Marks a claimed outbox job complete and consumes it on success.
 * On error the job remains active and may be retried with the same terminal
 * operation until its claim expires.
 */
int lc_outbox_job_complete(lc_outbox_job *job,
                           const lc_outbox_completion *completion,
                           lc_error *error);
/**
 * Returns a claimed outbox job to `retry_wait` and consumes it on success.
 * On error the job remains active and may be retried with the same terminal
 * operation until its claim expires.
 */
int lc_outbox_job_retry(lc_outbox_job *job, const lc_outbox_retry *request,
                        lc_error *error);
/**
 * Marks a claimed outbox job dead-lettered and consumes it on success.
 * On error the job remains active and may be retried with the same terminal
 * operation until its claim expires.
 */
int lc_outbox_job_dead_letter(lc_outbox_job *job, const char *diagnostic,
                              lc_error *error);
/** Releases a local outbox-job handle without a terminal transition. */
void lc_outbox_job_close(lc_outbox_job *job);
/** Acquires a non-terminal domain participant under the transaction's xid. */
int lc_outbox_transaction_acquire(lc_outbox_transaction *transaction,
                                  const lc_outbox_participant_request *request,
                                  lc_outbox_participant **out, lc_error *error);
/** Returns the borrowed global XID while this transaction remains open. */
const char *lc_outbox_transaction_id(lc_outbox_transaction *transaction);
/** Binds to a managed XA owner. After staging participants, xa must already
 * carry this outbox's global ID; participants cannot move to another ID.
 */
int lc_outbox_transaction_bind_xa(lc_outbox_transaction *transaction,
                                  lc_xa_transaction *xa, lc_error *error);
/** Delegates terminal work to the bound coordinator. Drive/close the returned
 * operation, then collect preallocated receipts through commit().
 * Unbound outboxes continue to use their ordinary synchronous terminal methods.
 */
int lc_outbox_transaction_commit_start(lc_outbox_transaction *transaction,
                                       int64_t deadline_ms,
                                       lc_xa_coordinator_operation **out,
                                       lc_error *error);
/** Starts rollback through the bound coordinator; drive and close the
 * returned operation before releasing transaction state.
 */
int lc_outbox_transaction_rollback_start(lc_outbox_transaction *transaction,
                                         int64_t deadline_ms,
                                         lc_xa_coordinator_operation **out,
                                         lc_error *error);
/**
 * Stages one immutable outbox effect and its payload in a transaction.
 * `entry->payload_digest` must bind the exact payload bytes without requiring
 * liblockdc to pre-read the source.
 */
int lc_outbox_transaction_append(lc_outbox_transaction *transaction,
                                 const lc_outbox_entry *entry,
                                 lc_source *payload, lc_outbox_receipt *out,
                                 lc_error *error);
/** Accepts the transaction's one durable command receipt. */
int lc_outbox_transaction_accept_command(lc_outbox_transaction *transaction,
                                         const lc_command_request *request,
                                         lc_command_receipt *receipt,
                                         lc_error *error);
/** Accepts one durable inbox receipt through an open outbox transaction. */
int lc_outbox_transaction_accept_inbox(lc_outbox_transaction *transaction,
                                       const lc_inbox_message *message,
                                       lc_inbox_accept_result *result,
                                       lc_error *error);
/** Completes the transaction's owned pending command. */
int lc_outbox_transaction_complete_command(lc_outbox_transaction *transaction,
                                           const lc_command_result *result,
                                           lc_error *error);
/** Fails the transaction's owned pending command. */
int lc_outbox_transaction_fail_command(lc_outbox_transaction *transaction,
                                       const lc_command_result *result,
                                       lc_error *error);
/** Commits all enrolled participants and consumes their lease handles.
 * Returns an error when the endpoint durably rolls the transaction back. */
int lc_outbox_transaction_commit(lc_outbox_transaction *transaction,
                                 lc_outbox_commit_result *out, lc_error *error);
/** Rolls back all enrolled participants and consumes their lease handles. */
int lc_outbox_transaction_rollback(lc_outbox_transaction *transaction,
                                   lc_error *error);
/** Best-effort rolls back an undecided transaction, then releases its wrapper.
 */
void lc_outbox_transaction_close(lc_outbox_transaction *transaction);
/** Refreshes the staged lease snapshot exposed by an outbox participant. */
int lc_outbox_participant_describe(lc_outbox_participant *participant,
                                   lc_error *error);
/** Streams the participant's staged state into `dst`. */
int lc_outbox_participant_get(lc_outbox_participant *participant, lc_sink *dst,
                              const lc_get_opts *opts, lc_get_res *out,
                              lc_error *error);
/** Streams and stages a replacement state for an outbox participant. */
int lc_outbox_participant_update(lc_outbox_participant *participant,
                                 lc_source *src, const lc_update_opts *opts,
                                 lc_error *error);
/** Applies a staged server-side mutation for an outbox participant. */
int lc_outbox_participant_mutate(lc_outbox_participant *participant,
                                 const lc_mutate_req *req, lc_error *error);
/** Applies a staged client-side mutation for an outbox participant. */
int lc_outbox_participant_mutate_local(lc_outbox_participant *participant,
                                       const lc_mutate_local_req *req,
                                       lc_error *error);
/** Stages metadata changes for an outbox participant. */
int lc_outbox_participant_metadata(lc_outbox_participant *participant,
                                   const lc_metadata_req *req, lc_error *error);
/** Stages removal of an outbox participant's state. */
int lc_outbox_participant_remove(lc_outbox_participant *participant,
                                 const lc_remove_req *req, lc_error *error);
/** Extends the underlying lease while retaining the open transaction. */
int lc_outbox_participant_keepalive(lc_outbox_participant *participant,
                                    const lc_keepalive_req *req,
                                    lc_error *error);
/** Streams and stages one outbox-participant attachment upload. */
int lc_outbox_participant_attach(lc_outbox_participant *participant,
                                 const lc_attach_req *req, lc_source *src,
                                 lc_attach_res *out, lc_error *error);
/** Lists attachments visible through an outbox participant's staged view. */
int lc_outbox_participant_list_attachments(lc_outbox_participant *participant,
                                           lc_attachment_list *out,
                                           lc_error *error);
/** Streams an attachment visible through an outbox participant's staged view.
 */
int lc_outbox_participant_get_attachment(lc_outbox_participant *participant,
                                         const lc_attachment_get_req *req,
                                         lc_sink *dst,
                                         lc_attachment_get_res *out,
                                         lc_error *error);
/** Stages deletion of one attachment from an outbox participant. */
int lc_outbox_participant_delete_attachment(
    lc_outbox_participant *participant, const lc_attachment_selector *selector,
    int *deleted, lc_error *error);
/** Stages deletion of every attachment from an outbox participant. */
int lc_outbox_participant_delete_all_attachments(
    lc_outbox_participant *participant, int *deleted_count, lc_error *error);
/** Releases an outbox participant view; it cannot make a terminal decision. */
void lc_outbox_participant_close(lc_outbox_participant *participant);
/** Reads a history consumer's durable acknowledgement and current sequence. */
int lc_history_consumer_get_position(lc_history_consumer *consumer,
                                     lc_history_consumer_position *out,
                                     lc_error *error);
/** Durably advances a history consumer's monotonic acknowledgement. */
int lc_history_consumer_advance(lc_history_consumer *consumer,
                                lc_index_seq acknowledged_index_seq,
                                lc_history_consumer_position *out,
                                lc_error *error);
/** Removes a history consumer's durable retention pin. */
int lc_history_consumer_unregister(lc_history_consumer *consumer,
                                   lc_error *error);
/** Releases a history-consumer handle without unregistering it. */
void lc_history_consumer_close(lc_history_consumer *consumer);

#ifdef __cplusplus
}
#endif

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif
