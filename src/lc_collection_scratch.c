#include "lc_collection_scratch.h"
#include "lc_api_internal.h"
#include <pthread.h>
#include <string.h>

#define SCRATCH_PAGE_BYTES 4096U

struct lc_collection_scratch {
  lc_allocator allocator;
  pthread_mutex_t mutex;
  unsigned char **pages;
  size_t slots, populated, references;
  lc_u64 length;
};

static int scratch_error(lc_error *error, int code, const char *message) {
  return lc_error_set(error, code, 0L, message, NULL, NULL, "pouch");
}

int lc_collection_scratch_open(const lc_allocator *allocator,
                               lc_collection_scratch **out, lc_error *error) {
  lc_collection_scratch *scratch;
  *out = NULL;
  scratch = lc_calloc_with_allocator(allocator, 1, sizeof(*scratch));
  if (!scratch)
    return scratch_error(error, LC_ERR_NOMEM,
                         "cannot allocate collection metadata scratch");
  if (allocator)
    scratch->allocator = *allocator;
  else
    lc_allocator_init(&scratch->allocator);
  if (pthread_mutex_init(&scratch->mutex, NULL) != 0) {
    lc_free_with_allocator(&scratch->allocator, scratch);
    return scratch_error(error, LC_ERR_TRANSPORT,
                         "cannot initialize collection metadata scratch lock");
  }
  scratch->references = 1U;
  *out = scratch;
  return LC_OK;
}

void lc_collection_scratch_retain(lc_collection_scratch *scratch) {
  pthread_mutex_lock(&scratch->mutex);
  ++scratch->references;
  pthread_mutex_unlock(&scratch->mutex);
}

void lc_collection_scratch_close(lc_collection_scratch *scratch) {
  size_t i, remaining;
  lc_allocator allocator;
  if (!scratch)
    return;
  pthread_mutex_lock(&scratch->mutex);
  remaining = --scratch->references;
  pthread_mutex_unlock(&scratch->mutex);
  if (remaining)
    return;
  allocator = scratch->allocator;
  for (i = 0; i < scratch->slots; ++i)
    lc_free_with_allocator(&allocator, scratch->pages[i]);
  lc_free_with_allocator(&allocator, scratch->pages);
  pthread_mutex_destroy(&scratch->mutex);
  lc_free_with_allocator(&allocator, scratch);
}

static int reserve_pages(lc_collection_scratch *scratch, size_t first,
                         size_t last, lc_error *error) {
  const size_t maximum = (size_t)-1 / sizeof(*scratch->pages);
  unsigned char **pages;
  size_t slots, i;
  if (last >= maximum)
    return scratch_error(error, LC_ERR_INVALID,
                         "collection metadata scratch exceeds address space");
  if (last >= scratch->slots) {
    slots = scratch->slots ? scratch->slots : 1U;
    while (slots <= last)
      slots = slots <= maximum / 2U ? slots * 2U : maximum;
    pages = lc_realloc_with_allocator(&scratch->allocator, scratch->pages,
                                      slots * sizeof(*pages));
    if (!pages)
      return scratch_error(error, LC_ERR_NOMEM,
                           "cannot grow collection metadata page table");
    memset(pages + scratch->slots, 0,
           (slots - scratch->slots) * sizeof(*pages));
    scratch->pages = pages;
    scratch->slots = slots;
  }
  for (i = first; i <= last; ++i) {
    if (scratch->pages[i])
      continue;
    scratch->pages[i] =
        lc_calloc_with_allocator(&scratch->allocator, 1, SCRATCH_PAGE_BYTES);
    if (!scratch->pages[i])
      return scratch_error(error, LC_ERR_NOMEM,
                           "cannot allocate collection metadata page");
    ++scratch->populated;
  }
  return LC_OK;
}

int lc_collection_scratch_io(lc_collection_scratch *scratch, lc_u64 offset,
                             unsigned char *bytes, size_t length, int writing,
                             lc_error *error) {
  lc_u64 end, page;
  size_t first, last, displacement, chunk;
  unsigned char *stored;
  int rc = LC_OK;
  if (!length)
    return LC_OK;
  if (offset > (lc_u64)LC_I64_MAX ||
      (lc_u64)length > (lc_u64)LC_I64_MAX - offset)
    return scratch_error(error, LC_ERR_INVALID,
                         "collection metadata scratch offset exceeds range");
  end = offset + length;
  pthread_mutex_lock(&scratch->mutex);
  if (writing) {
    rc = lc_u64_to_size_checked(offset / SCRATCH_PAGE_BYTES, &first) &&
                 lc_u64_to_size_checked((end - 1U) / SCRATCH_PAGE_BYTES, &last)
             ? reserve_pages(scratch, first, last, error)
             : scratch_error(
                   error, LC_ERR_INVALID,
                   "collection metadata scratch exceeds address space");
  } else if (end > scratch->length)
    rc = scratch_error(error, LC_ERR_PROTOCOL,
                       "collection metadata scratch is truncated");
  while (rc == LC_OK && length) {
    page = offset / SCRATCH_PAGE_BYTES;
    displacement = (size_t)(offset % SCRATCH_PAGE_BYTES);
    chunk = SCRATCH_PAGE_BYTES - displacement;
    if (chunk > length)
      chunk = length;
    stored = page < scratch->slots ? scratch->pages[(size_t)page] : NULL;
    if (writing)
      memcpy(stored + displacement, bytes, chunk);
    else if (stored)
      memcpy(bytes, stored + displacement, chunk);
    else
      memset(bytes, 0, chunk);
    bytes += chunk;
    offset += chunk;
    length -= chunk;
  }
  if (rc == LC_OK && writing && end > scratch->length)
    scratch->length = end;
  pthread_mutex_unlock(&scratch->mutex);
  return rc;
}

void lc_collection_scratch_extend(lc_collection_scratch *scratch,
                                  lc_u64 bytes) {
  pthread_mutex_lock(&scratch->mutex);
  if (bytes > scratch->length)
    scratch->length = bytes;
  pthread_mutex_unlock(&scratch->mutex);
}

void lc_collection_scratch_size(lc_collection_scratch *scratch, lc_u64 *length,
                                lc_u64 *allocated) {
  pthread_mutex_lock(&scratch->mutex);
  *length = scratch->length;
  if (allocated)
    *allocated = sizeof(*scratch) +
                 (lc_u64)scratch->slots * sizeof(*scratch->pages) +
                 (lc_u64)scratch->populated * SCRATCH_PAGE_BYTES;
  pthread_mutex_unlock(&scratch->mutex);
}
