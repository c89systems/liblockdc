#ifndef LC_POUCH_INDEX_STORE_H
#define LC_POUCH_INDEX_STORE_H
#include "lc_pouch.h"
#include "lc_pouch_record_io.h"

enum lc_pouch_index_artifact_kind {
  LC_POUCH_INDEX_MANIFEST = 0,
  LC_POUCH_INDEX_HEADER,
  LC_POUCH_INDEX_PACKED,
  LC_POUCH_INDEX_DOCUMENTS,
  LC_POUCH_INDEX_EXACT,
  LC_POUCH_INDEX_PRESENCE,
  LC_POUCH_INDEX_RANGE,
  LC_POUCH_INDEX_TEXT,
  LC_POUCH_INDEX_TRIGRAM,
  LC_POUCH_INDEX_TEMPORAL,
  LC_POUCH_INDEX_DELETES,
  LC_POUCH_INDEX_ARTIFACT_KINDS
};
typedef struct lc_pouch_index_artifact_id {
  uint64_t generation;
  unsigned int kind;
  int segmented;
} lc_pouch_index_artifact_id;
/* Opaque immutable-artifact identity. The names retain the existing manifest
 * wire fields; native files use stat facts, container owners use incarnation
 * facts. No clock or filesystem interpretation belongs to the query engine. */
typedef struct lc_pouch_index_signature {
  uint64_t size;
  unsigned long mtime, mtime_nsec, ctime, ctime_nsec, inode;
  int present;
} lc_pouch_index_signature;
typedef struct lc_pouch_index_input {
  lc_pouch_record_io io;
  lc_pouch_index_signature signature;
  void *handle;
  uint64_t bytes;
} lc_pouch_index_input;
typedef int (*lc_pouch_index_produce_fn)(void *, lc_sink *, lc_error *);
/* A logical owner revalidates its captured binding under domain authority,
 * then invokes commit while retaining that authority. A stale build may be
 * skipped with LC_OK. Producers and codecs run before this callback. */
typedef int (*lc_pouch_index_commit_fn)(void *, lc_error *);
typedef int (*lc_pouch_index_select_fn)(void *, lc_pouch_index_commit_fn,
                                        void *, lc_error *);
/* Shared bounded derived-metadata codec. Zero limits retain the namespace
 * codec's existing unbounded-metadata policy. Collection pages supply their
 * explicit stored, decoded and descriptor ceilings. Inputs remain borrowed. */
int lc_pouch_index_metadata_bound(lc_pouch *, size_t, int encrypted, uint64_t *,
                                  lc_error *);
int lc_pouch_index_metadata_encode(lc_pouch *, const char *, const char *,
                                   size_t, int encrypted, lc_sink *,
                                   lc_error *);
int lc_pouch_index_metadata_decode(lc_pouch *, const lc_pouch_index_input *,
                                   const char *, int encrypted, uint64_t,
                                   size_t, size_t, char **, size_t *,
                                   lc_error *);
/* The logical engine retains publication authority and immutable generation
 * selection. These capabilities own only derived index artifacts, never a
 * directory, application value, SQL execution or another decision log.
 * Open captures immutable encoded bytes and survives replace/remove. Close
 * releases its pin. Replace admits the complete conservative encoded bound
 * before invoking the bounded producer; failure cannot expose partial bytes.
 * unpublished permits a native direct build only when no manifest names it.
 * durable forces invalidation before canonical topology retirement. Prune
 * preserves the manifest and named generations; interrupted unbound builds
 * remain the physical owner's responsibility. Root/allocator metadata locks
 * never span producers. Publication authority belongs to the owner's domain. */
typedef struct lc_pouch_index_store_ops {
  int (*open)(void *, const char *, const lc_pouch_index_artifact_id *,
              lc_pouch_index_input *, lc_error *);
  void (*close)(void *, lc_pouch_index_input *);
  int (*replace)(void *, const char *, const lc_pouch_index_artifact_id *,
                 uint64_t, lc_pouch_index_produce_fn, void *, int unpublished,
                 int durable, lc_error *);
  int (*remove)(void *, const char *, const lc_pouch_index_artifact_id *,
                lc_error *);
  int (*signature)(void *, const char *, const lc_pouch_index_artifact_id *,
                   lc_pouch_index_signature *, lc_error *);
  int (*prune)(void *, const char *, const uint64_t *, size_t, lc_error *);
  /* Prepare private generation bytes and their stable signature, admitting
   * both the generation and a conservative manifest bound before production.
   * Publish selects them as one logical revision; dispose always releases the
   * build, after success or failure. The native reference retains its normal
   * temporary-file/manifest protocol through these same capabilities. */
  int (*prepare)(void *, const char *, const lc_pouch_index_artifact_id *,
                 uint64_t, lc_pouch_index_produce_fn, void *, int,
                 uint64_t manifest_bound, void **, lc_pouch_index_signature *,
                 lc_error *);
  int (*publish)(void *, void *, const char *,
                 const lc_pouch_index_artifact_id *, uint64_t,
                 lc_pouch_index_produce_fn, void *, lc_error *);
  void (*dispose)(void *, void *);
  int (*acquire)(void *, const char *, void **, lc_error *);
  void (*release)(void *, void *);
} lc_pouch_index_store_ops;

/* One owned logical reference, allocated in the Pouch allocator. Free with
 * that allocator. ns/leaf/key point into this allocation; key is an opaque
 * namespace-qualified cache identity, never a physical pathname. */
typedef struct lc_pouch_index_artifact {
  lc_pouch *pouch;
  lc_pouch_index_artifact_id id;
  const char *ns, *leaf, *key;
} lc_pouch_index_artifact;
int lc_pouch_index_artifact_leaf(const lc_pouch_index_artifact_id *, char *,
                                 size_t, lc_error *);
int lc_pouch_index_artifact_kind(const char *, unsigned int *);
lc_pouch_index_artifact *
lc_pouch_index_artifact_new(lc_pouch *, const char *,
                            const lc_pouch_index_artifact_id *, lc_error *);
int lc_pouch_index_artifact_signature(const lc_pouch_index_artifact *,
                                      lc_pouch_index_signature *, lc_error *);
/* Materialized derived index metadata, matching the existing generation and
 * manifest codecs. Payload extraction remains producer-to-consumer streaming.
 * Ciphertext is decoded from the captured bounded input without a value spool.
 * A non-strict invalid artifact returns present=1/valid=0 for canonical
 * rebuild; allocation and transport errors remain errors. Output uses Pouch
 * allocator. */
int lc_pouch_index_artifact_load(const lc_pouch_index_artifact *, int strict,
                                 char **, size_t *, int *present, int *valid,
                                 lc_error *);
int lc_pouch_index_artifact_write(const lc_pouch_index_artifact *, const char *,
                                  size_t, int unpublished, int durable,
                                  lc_error *);
int lc_pouch_index_artifact_remove(const lc_pouch_index_artifact *, lc_error *);
int lc_pouch_index_artifacts_prune(lc_pouch *, const char *, const uint64_t *,
                                   size_t, lc_error *);
typedef struct lc_pouch_index_revision lc_pouch_index_revision;
int lc_pouch_index_revision_prepare(const lc_pouch_index_artifact *,
                                    const char *, size_t, int unpublished,
                                    uint64_t manifest_bound,
                                    lc_pouch_index_revision **,
                                    lc_pouch_index_signature *, lc_error *);
int lc_pouch_index_revision_publish(lc_pouch_index_revision *,
                                    const lc_pouch_index_artifact *,
                                    const char *, size_t, lc_error *);
void lc_pouch_index_revision_dispose(lc_pouch_index_revision *);
int lc_pouch_index_publication_acquire(lc_pouch *, const char *, void **,
                                       lc_error *);
void lc_pouch_index_publication_release(lc_pouch *, void *);
extern const lc_pouch_index_store_ops lc_pouch_index_file_ops;
#endif
