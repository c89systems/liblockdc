#ifndef POUCH_SPACE_NAMESPACE_METADATA_H
#define POUCH_SPACE_NAMESPACE_METADATA_H
#include "checkpoint.h"
typedef struct ps_namespace_metadata ps_namespace_metadata;
int ps_namespace_metadata_begin(ps_namespace *, int,
                                const lc_pouch_storage_batch *, uint64_t,
                                ps_namespace_metadata **, lc_error *);
int ps_namespace_metadata_commit(ps_namespace_metadata *,
                                 const lc_pouch_storage_batch *,
                                 enum ps_commit_mode, lc_error *);
void ps_namespace_metadata_close(ps_namespace_metadata *);
#endif
