#ifndef LC_COLLECTION_CATALOG_RECORD_H
#define LC_COLLECTION_CATALOG_RECORD_H

#include "lc/lc.h"

#define LC_COLLECTION_CATALOG_BYTES 320U
#define LC_COLLECTION_NAME_BYTES 256U

/* Immutable collection identity. The exact UTF-8 name and endpoint-minted XID
 * are authoritative; a native filename digest is only a lookup accelerator.
 * Shared by persistent owners. Decode leaves outputs untouched on failure. */
int lc_collection_catalog_name_valid(const char *);
int lc_collection_catalog_encode(const char *, const char *,
                                 unsigned char[LC_COLLECTION_CATALOG_BYTES],
                                 lc_error *);
int lc_collection_catalog_decode(
    const unsigned char[LC_COLLECTION_CATALOG_BYTES], char[LC_XID_STRING_SIZE],
    char[LC_COLLECTION_NAME_BYTES], lc_error *);

#endif
