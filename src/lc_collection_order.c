#include "lc_collection_order.h"
#include "lc_api_internal.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>

#define LC_CO_ROOT_BYTES LC_COLLECTION_ORDER_ROOT_BYTES
#define LC_CO_ROW_BYTES LC_COLLECTION_ORDER_ROW_BYTES
#define LC_CO_PAGE_BYTES 4096U
#define LC_CO_PAGE_HEADER_BYTES 64U
#define LC_CO_ROOT_MAGIC 0x4c434f49UL
#define LC_CO_PAGE_MAGIC 0x4c434f50UL

typedef struct lc_co_page {
  lc_u64 count;
  lc_u64 first;
  lc_u64 last;
  lc_u64 live;
  lc_u64 deleted_through;
} lc_co_page;

static int lc_co_error(lc_error *error, int code, const char *message) {
  (void)lc_error_set(error, code, 0L, message,
                     code == LC_ERR_TRANSPORT ? strerror(errno) : NULL,
                     code == LC_ERR_PROTOCOL ? "collection_corrupt" : NULL,
                     "pouch");
  return code;
}

static void lc_co_put64(unsigned char *bytes, lc_u64 value) {
  unsigned int i;
  for (i = 0U; i < 8U; ++i) {
    bytes[i] = (unsigned char)(value & 255U);
    value >>= 8U;
  }
}

static lc_u64 lc_co_get64(const unsigned char *bytes) {
  lc_u64 value;
  unsigned int i;
  value = 0U;
  for (i = 8U; i > 0U; --i) {
    value = (value << 8U) | bytes[i - 1U];
  }
  return value;
}

static void lc_co_seal(unsigned char *bytes, size_t size) {
  unsigned long crc;
  crc = (unsigned long)crc32(0L, bytes, (uInt)(size - 4U));
  bytes[size - 4U] = (unsigned char)crc;
  bytes[size - 3U] = (unsigned char)(crc >> 8U);
  bytes[size - 2U] = (unsigned char)(crc >> 16U);
  bytes[size - 1U] = (unsigned char)(crc >> 24U);
}

static int lc_co_valid(const unsigned char *bytes, size_t size) {
  unsigned long crc;
  unsigned long stored;
  crc = (unsigned long)crc32(0L, bytes, (uInt)(size - 4U));
  stored = (unsigned long)bytes[size - 4U] |
           ((unsigned long)bytes[size - 3U] << 8U) |
           ((unsigned long)bytes[size - 2U] << 16U) |
           ((unsigned long)bytes[size - 1U] << 24U);
  return crc == stored;
}

static int lc_co_io(int fd, lc_u64 offset, unsigned char *bytes, size_t length,
                    int write_bytes, lc_error *error) {
  ssize_t n;
  size_t done;
  if (offset > (lc_u64)LC_I64_MAX ||
      (lc_u64)length > (lc_u64)LC_I64_MAX - offset) {
    return lc_co_error(error, LC_ERR_INVALID,
                       "collection index offset exceeds file range");
  }
  done = 0U;
  while (done < length) {
    if (write_bytes) {
      n = pwrite(fd, bytes + done, length - done, (off_t)(offset + done));
    } else {
      n = pread(fd, bytes + done, length - done, (off_t)(offset + done));
    }
    if (n < 0 && errno == EINTR) {
      continue;
    }
    if (n <= 0) {
      return lc_co_error(
          error, n == 0 && !write_bytes ? LC_ERR_PROTOCOL : LC_ERR_TRANSPORT,
          "failed to access collection ordered index");
    }
    done += (size_t)n;
  }
  return LC_OK;
}

static int lc_co_page_offset(lc_u64 page, lc_u64 *offset, lc_error *error) {
  if (page > ((lc_u64)LC_I64_MAX - LC_CO_ROOT_BYTES - LC_CO_PAGE_BYTES) /
                 LC_CO_PAGE_BYTES) {
    return lc_co_error(error, LC_ERR_INVALID,
                       "collection index page exceeds file range");
  }
  *offset = LC_CO_ROOT_BYTES + page * LC_CO_PAGE_BYTES;
  return LC_OK;
}

static int lc_co_row_offset(lc_u64 ordinal, lc_u64 *offset, lc_error *error) {
  int rc;
  rc =
      lc_co_page_offset(ordinal / LC_COLLECTION_ORDER_PAGE_ROWS, offset, error);
  if (rc == LC_OK) {
    *offset += LC_CO_PAGE_HEADER_BYTES +
               (ordinal % LC_COLLECTION_ORDER_PAGE_ROWS) * LC_CO_ROW_BYTES;
  }
  return rc;
}

static int lc_co_root_write(lc_collection_order *order, lc_error *error) {
  unsigned char bytes[LC_CO_ROOT_BYTES];
  memset(bytes, 0, sizeof(bytes));
  lc_co_put64(bytes, LC_CO_ROOT_MAGIC);
  lc_co_put64(bytes + 8U, order->count);
  lc_co_put64(bytes + 16U, order->generation);
  lc_co_put64(bytes + 24U, order->upper_position);
  lc_co_put64(bytes + 32U, order->replay_segment);
  lc_co_put64(bytes + 40U, order->replay_offset);
  lc_co_put64(bytes + 48U, order->live_count);
  lc_co_put64(bytes + 56U, order->snapshot);
  lc_co_put64(bytes + 64U, order->reclaimable_bytes);
  memcpy(bytes + 72U, order->query_id, LC_XID_STRING_SIZE - 1U);
  lc_co_put64(bytes + 96U, order->metadata_generation);
  lc_co_put64(bytes + 104U, order->retention_present ? 1U : 0U);
  lc_co_put64(bytes + 112U, order->first_live);
  lc_co_put64(bytes + 120U, order->collection_retention_generation);
  lc_co_put64(bytes + 128U, (lc_u64)order->collection_retention);
  lc_co_put64(bytes + 136U, order->edit_count);
  lc_co_put64(bytes + 144U, order->edit_in_progress ? 1U : 0U);
  bytes[152] = LC_CO_ROW_BYTES;
  lc_co_seal(bytes, sizeof(bytes));
  return lc_co_io(order->fd, 0U, bytes, sizeof(bytes), 1, error);
}

int lc_collection_order_refresh(lc_collection_order *order, lc_error *error) {
  unsigned char bytes[LC_CO_ROOT_BYTES];
  size_t i;
  struct stat st;
  lc_u64 end;
  int rc;
  if (order == NULL || order->fd < 0) {
    return lc_co_error(error, LC_ERR_INVALID, "collection index is closed");
  }
  rc = lc_co_io(order->fd, 0U, bytes, sizeof(bytes), 0, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!lc_co_valid(bytes, sizeof(bytes)) ||
      lc_co_get64(bytes) != LC_CO_ROOT_MAGIC || bytes[152] != LC_CO_ROW_BYTES) {
    return lc_co_error(error, LC_ERR_PROTOCOL,
                       "collection index header is invalid");
  }
  for (i = 92U; i < sizeof(bytes) - 4U; ++i)
    if ((i < 96U || i >= 153U) && bytes[i] != 0U)
      return lc_co_error(error, LC_ERR_PROTOCOL,
                         "collection index header has unknown fields");
  order->edit_count = lc_co_get64(bytes + 136U);
  order->edit_in_progress = lc_co_get64(bytes + 144U) != 0U;
  if (lc_co_get64(bytes + 144U) > 1U)
    return lc_co_error(error, LC_ERR_PROTOCOL, "invalid edit repair boundary");
  order->count = lc_co_get64(bytes + 8U);
  order->generation = lc_co_get64(bytes + 16U);
  order->upper_position = lc_co_get64(bytes + 24U);
  order->replay_segment = lc_co_get64(bytes + 32U);
  order->replay_offset = lc_co_get64(bytes + 40U);
  order->live_count = lc_co_get64(bytes + 48U);
  order->first_live = lc_co_get64(bytes + 112U);
  order->collection_retention_generation = lc_co_get64(bytes + 120U);
  if (lc_co_get64(bytes + 128U) > (lc_u64)LC_I64_MAX)
    return lc_co_error(error, LC_ERR_PROTOCOL,
                       "collection renewal deadline is invalid");
  order->collection_retention = (lc_unix_seconds)lc_co_get64(bytes + 128U);
  order->snapshot = lc_co_get64(bytes + 56U);
  order->reclaimable_bytes = lc_co_get64(bytes + 64U);
  order->metadata_generation = lc_co_get64(bytes + 96U);
  if (lc_co_get64(bytes + 104U) > 1U)
    return lc_co_error(error, LC_ERR_PROTOCOL,
                       "collection retention work hint is invalid");
  order->retention_present = lc_co_get64(bytes + 104U) != 0U;
  if (order->metadata_generation > order->generation ||
      order->collection_retention_generation > order->generation)
    return lc_co_error(error, LC_ERR_PROTOCOL,
                       "collection metadata generation exceeds its watermark");
  memcpy(order->query_id, bytes + 72U, LC_XID_STRING_SIZE - 1U);
  order->query_id[LC_XID_STRING_SIZE - 1U] = '\0';
  if (!lc_xid_is_valid(order->query_id))
    return lc_co_error(error, LC_ERR_PROTOCOL,
                       "collection query projection identity is invalid");
  if ((order->count == 0U && order->upper_position != 0U) ||
      order->upper_position < order->count ||
      order->generation < order->count || order->live_count > order->count ||
      order->first_live > order->count ||
      (order->live_count != 0U && order->first_live == order->count)) {
    return lc_co_error(error, LC_ERR_PROTOCOL,
                       "collection index header bounds are inconsistent");
  }
  if (fstat(order->fd, &st) != 0) {
    return lc_co_error(error, LC_ERR_TRANSPORT,
                       "failed to stat collection ordered index");
  }
  end = LC_CO_ROOT_BYTES;
  if (order->count > 0U) {
    rc = lc_co_row_offset(order->count - 1U, &end, error);
    if (rc != LC_OK) {
      return rc;
    }
    end += LC_CO_ROW_BYTES;
  }
  if (!S_ISREG(st.st_mode) || st.st_size < 0 || (lc_u64)st.st_size < end) {
    return lc_co_error(error, LC_ERR_PROTOCOL,
                       "collection ordered index is truncated");
  }
  return LC_OK;
}

int lc_collection_order_open(const char *path, int create,
                             lc_collection_order *out, lc_error *error) {
  struct stat st;
  int rc;
  if (out == NULL) {
    return lc_co_error(error, LC_ERR_INVALID,
                       "collection ordered index requires output");
  }
  memset(out, 0, sizeof(*out));
  out->fd = -1;
  if (path == NULL || path[0] == '\0') {
    return lc_co_error(error, LC_ERR_INVALID,
                       "collection ordered index requires path");
  }
  out->fd = open(path, O_RDWR | (create ? O_CREAT : 0), 0600);
  if (out->fd < 0) {
    return lc_co_error(error, LC_ERR_TRANSPORT,
                       "failed to open collection ordered index");
  }
  rc = LC_OK;
  if (fstat(out->fd, &st) != 0) {
    rc = lc_co_error(error, LC_ERR_TRANSPORT,
                     "failed to stat collection ordered index");
  } else if (st.st_size == 0 && create) {
    rc = lc_xid_new(out->query_id, error);
    if (rc == LC_OK)
      rc = lc_co_root_write(out, error);
  }
  if (rc == LC_OK) {
    rc = lc_collection_order_refresh(out, error);
  }
  if (rc != LC_OK) {
    lc_collection_order_close(out);
  }
  return rc;
}

void lc_collection_order_close(lc_collection_order *order) {
  if (order != NULL) {
    if (order->fd >= 0) {
      (void)close(order->fd);
    }
    memset(order, 0, sizeof(*order));
    order->fd = -1;
  }
}

int lc_collection_order_checkpoint(lc_collection_order *order,
                                   lc_u64 replay_segment, lc_u64 replay_offset,
                                   lc_error *error) {
  if (order == NULL || order->fd < 0 || replay_segment == 0U) {
    return lc_co_error(error, LC_ERR_INVALID,
                       "collection ordered checkpoint requires context");
  }
  order->replay_segment = replay_segment;
  order->replay_offset = replay_offset;
  return lc_co_root_write(order, error);
}

static int lc_co_row_decode(lc_collection_order *order,
                            const unsigned char bytes[LC_CO_ROW_BYTES],
                            lc_collection_order_row *row, lc_error *error) {
  static const unsigned char empty[32] = {0};
  lc_u64 packed, delta;
  if (!lc_co_valid(bytes, LC_CO_ROW_BYTES) || bytes[96] != 0U ||
      bytes[97] != 0U || bytes[98] != 0U || bytes[99] != 0U) {
    return lc_co_error(error, LC_ERR_PROTOCOL,
                       "collection ordered row checksum is invalid");
  }
  row->position = lc_co_get64(bytes);
  row->published_at = lc_co_get64(bytes + 8U);
  row->deleted_at = lc_co_get64(bytes + 16U);
  row->segment = lc_co_get64(bytes + 24U);
  row->record_offset = lc_co_get64(bytes + 32U);
  row->bytes = lc_co_get64(bytes + 40U);
  row->stored_bytes = lc_co_get64(bytes + 48U);
  packed = lc_co_get64(bytes + 56U);
  memcpy(row->proof, bytes + 64U, sizeof(row->proof));
  row->unit_segment = row->unit_offset = 0;
  if (packed) {
    if (!(packed >> 32U))
      return lc_co_error(error, LC_ERR_PROTOCOL,
                         "collection unit locator is invalid");
    delta = (packed >> 32U) - 1U;
    if (delta >= (row->segment & (lc_u64)LC_I64_MAX))
      return lc_co_error(error, LC_ERR_PROTOCOL,
                         "collection unit locator precedes history");
    row->unit_segment = row->segment - delta;
    row->unit_offset = packed & (lc_u64)0xffffffffUL;
  } else if (memcmp(row->proof, empty, sizeof(empty)))
    return lc_co_error(error, LC_ERR_PROTOCOL,
                       "collection unit evidence has no location");
  if (row->position == 0U || row->position > order->upper_position ||
      row->published_at == 0U || row->published_at > order->generation ||
      (row->deleted_at != 0U && (row->deleted_at <= row->published_at ||
                                 row->deleted_at > order->generation)) ||
      row->segment == 0U || row->unit_segment > row->segment ||
      (!row->unit_segment && row->unit_offset)) {
    return lc_co_error(error, LC_ERR_PROTOCOL,
                       "collection ordered row bounds are inconsistent");
  }
  return LC_OK;
}

static int lc_co_row_read(lc_collection_order *order, lc_u64 ordinal,
                          lc_collection_order_row *row, lc_error *error) {
  unsigned char bytes[LC_CO_ROW_BYTES];
  lc_u64 offset;
  int rc;
  if (ordinal >= order->count) {
    return lc_co_error(error, LC_ERR_INVALID,
                       "collection index ordinal is out of bounds");
  }
  rc = lc_co_row_offset(ordinal, &offset, error);
  if (rc == LC_OK) {
    rc = lc_co_io(order->fd, offset, bytes, sizeof(bytes), 0, error);
  }
  return rc == LC_OK ? lc_co_row_decode(order, bytes, row, error) : rc;
}

static int lc_co_row_write(lc_collection_order *order, lc_u64 ordinal,
                           const lc_collection_order_row *row,
                           lc_error *error) {
  unsigned char bytes[LC_CO_ROW_BYTES];
  lc_u64 offset, packed = 0;
  int rc;
  if (row->unit_segment) {
    if (row->unit_segment > row->segment ||
        ((row->unit_segment ^ row->segment) & ~(lc_u64)LC_I64_MAX) ||
        row->unit_offset > (lc_u64)0xffffffffUL ||
        row->segment - row->unit_segment >= (lc_u64)0xffffffffUL)
      return lc_co_error(error, LC_ERR_PROTOCOL,
                         "collection unit locator exceeds its hint bounds");
    packed =
        ((row->segment - row->unit_segment + 1U) << 32U) | row->unit_offset;
  }
  memset(bytes, 0, sizeof(bytes));
  lc_co_put64(bytes, row->position);
  lc_co_put64(bytes + 8U, row->published_at);
  lc_co_put64(bytes + 16U, row->deleted_at);
  lc_co_put64(bytes + 24U, row->segment);
  lc_co_put64(bytes + 32U, row->record_offset);
  lc_co_put64(bytes + 40U, row->bytes);
  lc_co_put64(bytes + 48U, row->stored_bytes);
  lc_co_put64(bytes + 56U, packed);
  memcpy(bytes + 64U, row->proof, sizeof(row->proof));
  lc_co_seal(bytes, sizeof(bytes));
  rc = lc_co_row_offset(ordinal, &offset, error);
  if (rc == LC_OK) {
    rc = lc_co_io(order->fd, offset, bytes, sizeof(bytes), 1, error);
  }
  return rc;
}

static int lc_co_page_decode(lc_collection_order *order, lc_u64 number,
                             const unsigned char bytes[64], lc_co_page *page,
                             lc_error *error) {
  lc_u64 expected;
  if (order->count == 0U ||
      number > (order->count - 1U) / LC_COLLECTION_ORDER_PAGE_ROWS) {
    return lc_co_error(error, LC_ERR_INVALID,
                       "collection ordered page is out of bounds");
  }
  if (!lc_co_valid(bytes, LC_CO_PAGE_HEADER_BYTES) ||
      lc_co_get64(bytes) != LC_CO_PAGE_MAGIC ||
      lc_co_get64(bytes + 48U) != 0U || bytes[56] != 0U || bytes[57] != 0U ||
      bytes[58] != 0U || bytes[59] != 0U) {
    return lc_co_error(error, LC_ERR_PROTOCOL,
                       "collection ordered page checksum is invalid");
  }
  page->count = lc_co_get64(bytes + 8U);
  page->first = lc_co_get64(bytes + 16U);
  page->last = lc_co_get64(bytes + 24U);
  page->live = lc_co_get64(bytes + 32U);
  page->deleted_through = lc_co_get64(bytes + 40U);
  expected = order->count - number * LC_COLLECTION_ORDER_PAGE_ROWS;
  if (expected > LC_COLLECTION_ORDER_PAGE_ROWS) {
    expected = LC_COLLECTION_ORDER_PAGE_ROWS;
  }
  if (page->count != expected || page->first == 0U ||
      page->last < page->first || page->last > order->upper_position ||
      page->live > page->count ||
      (page->live == 0U && page->deleted_through == 0U) ||
      (page->live != 0U && page->deleted_through != 0U) ||
      page->deleted_through > order->generation) {
    return lc_co_error(error, LC_ERR_PROTOCOL,
                       "collection ordered page bounds are inconsistent");
  }
  return LC_OK;
}

static int lc_co_page_read(lc_collection_order *order, lc_u64 number,
                           lc_co_page *page, lc_error *error) {
  unsigned char bytes[64];
  lc_u64 offset;
  int rc;
  rc = lc_co_page_offset(number, &offset, error);
  if (rc == LC_OK) {
    rc = lc_co_io(order->fd, offset, bytes, sizeof(bytes), 0, error);
  }
  return rc == LC_OK ? lc_co_page_decode(order, number, bytes, page, error)
                     : rc;
}

static int lc_co_page_write(lc_collection_order *order, lc_u64 number,
                            const lc_co_page *page, lc_error *error) {
  unsigned char bytes[64];
  lc_u64 offset;
  int rc;
  memset(bytes, 0, sizeof(bytes));
  lc_co_put64(bytes, LC_CO_PAGE_MAGIC);
  lc_co_put64(bytes + 8U, page->count);
  lc_co_put64(bytes + 16U, page->first);
  lc_co_put64(bytes + 24U, page->last);
  lc_co_put64(bytes + 32U, page->live);
  lc_co_put64(bytes + 40U, page->deleted_through);
  lc_co_seal(bytes, sizeof(bytes));
  rc = lc_co_page_offset(number, &offset, error);
  if (rc == LC_OK) {
    rc = lc_co_io(order->fd, offset, bytes, sizeof(bytes), 1, error);
  }
  return rc;
}

static int lc_co_append(lc_collection_order *order,
                        const lc_collection_order_row *row,
                        lc_u64 replay_segment, lc_u64 replay_offset,
                        int recovered, lc_error *error) {
  lc_co_page page;
  lc_collection_order_row previous;
  lc_u64 number;
  int rc;
  if (order == NULL || order->fd < 0 || row == NULL ||
      row->position <= order->upper_position || row->published_at == 0U ||
      (!recovered && ((row->published_at <= order->generation &&
                       !(order->edit_in_progress &&
                         row->published_at == order->generation)) ||
                      row->deleted_at != 0U)) ||
      (row->deleted_at != 0U && row->deleted_at <= row->published_at) ||
      row->segment == 0U || replay_segment == 0U ||
      replay_segment < order->replay_segment ||
      (replay_segment == order->replay_segment &&
       replay_offset <= order->replay_offset)) {
    return lc_co_error(error, LC_ERR_INVALID,
                       "collection ordered append is not increasing");
  }
  if (recovered && order->count != 0U) {
    rc = lc_co_row_read(order, order->count - 1U, &previous, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (row->published_at <= previous.published_at) {
      return lc_co_error(error, LC_ERR_PROTOCOL,
                         "recovered collection publication order is invalid");
    }
  }
  memset(&page, 0, sizeof(page));
  number = order->count / LC_COLLECTION_ORDER_PAGE_ROWS;
  if (order->count % LC_COLLECTION_ORDER_PAGE_ROWS != 0U) {
    rc = lc_co_page_read(order, number, &page, error);
    if (rc != LC_OK) {
      return rc;
    }
  } else {
    page.first = row->position;
  }
  rc = lc_co_row_write(order, order->count, row, error);
  if (rc != LC_OK) {
    return rc;
  }
  ++page.count;
  page.live += row->deleted_at == 0U ? 1U : 0U;
  page.last = row->position;
  if (page.live != 0U) {
    page.deleted_through = 0U;
  } else if (row->deleted_at > page.deleted_through) {
    page.deleted_through = row->deleted_at;
  }
  rc = lc_co_page_write(order, number, &page, error);
  if (rc == LC_OK) {
    if (order->live_count == 0U)
      order->first_live = order->count + (row->deleted_at != 0U ? 1U : 0U);
    ++order->count;
    order->live_count += row->deleted_at == 0U ? 1U : 0U;
    if (row->published_at > order->generation) {
      order->generation = row->published_at;
    }
    if (row->deleted_at > order->generation) {
      order->generation = row->deleted_at;
    }
    order->upper_position = row->position;
    order->replay_segment = replay_segment;
    order->replay_offset = replay_offset;
    rc = lc_co_root_write(order, error);
  }
  return rc;
}

int lc_collection_order_append(lc_collection_order *order,
                               const lc_collection_order_row *row,
                               lc_u64 replay_segment, lc_u64 replay_offset,
                               lc_error *error) {
  return lc_co_append(order, row, replay_segment, replay_offset, 0, error);
}

int lc_collection_order_append_recovered(lc_collection_order *order,
                                         const lc_collection_order_row *row,
                                         lc_u64 replay_segment,
                                         lc_u64 replay_offset,
                                         lc_error *error) {
  return lc_co_append(order, row, replay_segment, replay_offset, 1, error);
}

/* Returns the ordinal of the first position strictly greater than after.
 * Positions are keys, including values above the signed integer range. */
static int lc_co_upper_bound(lc_collection_order *order, lc_u64 after,
                             lc_u64 *ordinal, lc_error *error) {
  lc_u64 lo;
  lc_u64 hi;
  lc_u64 mid;
  lc_collection_order_row row;
  int rc;
  /* Strictly increasing positive positions ending at count are necessarily
   * dense, even with tombstones. Validate the selected row as usual; sparse
   * recovered indexes retain the general unsigned-key binary search below. */
  if (order->upper_position == order->count) {
    *ordinal = after < order->count ? after : order->count;
    if (*ordinal == order->count)
      return LC_OK;
    rc = lc_co_row_read(order, *ordinal, &row, error);
    if (rc == LC_OK && row.position != *ordinal + 1U)
      rc = lc_co_error(error, LC_ERR_PROTOCOL,
                       "dense collection position contradicts its row");
    return rc;
  }
  lo = 0U;
  hi = order->count;
  while (lo < hi) {
    mid = lo + (hi - lo) / 2U;
    rc = lc_co_row_read(order, mid, &row, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (row.position <= after) {
      lo = mid + 1U;
    } else {
      hi = mid;
    }
  }
  *ordinal = lo;
  return LC_OK;
}

int lc_collection_order_find(lc_collection_order *order, lc_u64 position,
                             lc_collection_order_row *row, int *found,
                             lc_error *error) {
  lc_u64 ordinal;
  int rc;
  if (found != NULL) {
    *found = 0;
  }
  if (order == NULL || order->fd < 0 || position == 0U || row == NULL ||
      found == NULL) {
    return lc_co_error(error, LC_ERR_INVALID,
                       "collection ordered lookup arguments are invalid");
  }
  rc = lc_co_upper_bound(order, position - 1U, &ordinal, error);
  if (rc == LC_OK && ordinal < order->count) {
    rc = lc_co_row_read(order, ordinal, row, error);
    *found = rc == LC_OK && row->position == position;
  }
  return rc;
}

int lc_collection_order_metadata(lc_collection_order *order,
                                 const lc_collection_order_row *row,
                                 lc_u64 generation, lc_u64 replay_segment,
                                 lc_u64 replay_offset, lc_error *error) {
  lc_collection_order_row current;
  lc_u64 ordinal;
  int rc;
  if (order == NULL || order->fd < 0 || row == NULL || row->position == 0U ||
      row->deleted_at != 0U || row->segment == 0U ||
      generation < row->published_at || replay_segment == 0U ||
      replay_segment < order->replay_segment ||
      (replay_segment == order->replay_segment &&
       replay_offset < order->replay_offset))
    return lc_co_error(error, LC_ERR_INVALID,
                       "collection ordered metadata arguments are invalid");
  rc = lc_co_upper_bound(order, row->position - 1U, &ordinal, error);
  if (rc == LC_OK && ordinal == order->count)
    return lc_co_error(error, LC_ERR_PROTOCOL,
                       "collection metadata has no committed append");
  if (rc == LC_OK)
    rc = lc_co_row_read(order, ordinal, &current, error);
  if (rc == LC_OK &&
      (current.position != row->position || current.deleted_at != 0U ||
       current.published_at != row->published_at ||
       current.bytes != row->bytes ||
       current.stored_bytes != row->stored_bytes))
    return lc_co_error(error, LC_ERR_PROTOCOL,
                       "collection metadata changed its immutable state");
  if (rc == LC_OK)
    rc = lc_co_row_write(order, ordinal, row, error);
  if (rc == LC_OK) {
    if (generation > order->generation)
      order->generation = generation;
    if (generation > order->metadata_generation)
      order->metadata_generation = generation;
    order->replay_segment = replay_segment;
    order->replay_offset = replay_offset;
    rc = lc_co_root_write(order, error);
  }
  return rc;
}

int lc_collection_order_delete(lc_collection_order *order, lc_u64 position,
                               lc_u64 generation, lc_u64 replay_segment,
                               lc_u64 replay_offset, lc_error *error) {
  lc_collection_order_row row;
  lc_co_page page;
  lc_u64 ordinal;
  lc_u64 number;
  lc_u64 i;
  lc_u64 deleted_through;
  int rc;
  if (order == NULL || order->fd < 0 || position == 0U ||
      generation <= order->generation || replay_segment == 0U ||
      replay_segment < order->replay_segment ||
      (replay_segment == order->replay_segment &&
       replay_offset <= order->replay_offset)) {
    return lc_co_error(error, LC_ERR_INVALID,
                       "collection ordered deletion arguments are invalid");
  }
  rc = lc_co_upper_bound(order, position - 1U, &ordinal, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (ordinal == order->count) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection position was never assigned", NULL,
                        "entry_not_found", "pouch");
  }
  rc = lc_co_row_read(order, ordinal, &row, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (row.position != position) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection position was never assigned", NULL,
                        "entry_not_found", "pouch");
  }
  if (row.deleted_at != 0U) {
    return LC_OK;
  }
  number = ordinal / LC_COLLECTION_ORDER_PAGE_ROWS;
  rc = lc_co_page_read(order, number, &page, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (page.live == 0U) {
    return lc_co_error(error, LC_ERR_PROTOCOL,
                       "collection deletion summary contradicts its row");
  }
  deleted_through = generation;
  if (page.live == 1U) {
    for (i = 0U; i < page.count; ++i) {
      lc_collection_order_row peer;
      rc = lc_co_row_read(order, number * LC_COLLECTION_ORDER_PAGE_ROWS + i,
                          &peer, error);
      if (rc != LC_OK) {
        return rc;
      }
      if (peer.deleted_at > deleted_through) {
        deleted_through = peer.deleted_at;
      }
    }
  }
  row.deleted_at = generation;
  rc = lc_co_row_write(order, ordinal, &row, error);
  if (rc == LC_OK) {
    --page.live;
    page.deleted_through = page.live == 0U ? deleted_through : 0U;
    rc = lc_co_page_write(order, number, &page, error);
  }
  if (rc == LC_OK) {
    order->generation = generation;
    --order->live_count;
    if (ordinal == order->first_live)
      order->first_live = ordinal + 1U;
    if (order->live_count == 0U)
      order->first_live = order->count;
    order->reclaimable_bytes =
        row.stored_bytes > LC_U64_MAX - order->reclaimable_bytes
            ? LC_U64_MAX
            : order->reclaimable_bytes + row.stored_bytes;
    order->replay_segment = replay_segment;
    order->replay_offset = replay_offset;
    rc = lc_co_root_write(order, error);
  }
  return rc;
}

int lc_collection_order_oldest(lc_collection_order *order,
                               lc_collection_order_row *row, int *found,
                               lc_error *error) {
  lc_collection_order_iter iter;
  lc_u64 examined;
  size_t count;
  int rc;
  if (order == NULL || order->fd < 0 || row == NULL || found == NULL)
    return lc_co_error(error, LC_ERR_INVALID,
                       "oldest collection entry requires order and output");
  *found = 0;
  if (order->live_count == 0U)
    return LC_OK;
  memset(&iter, 0, sizeof(iter));
  iter.next = order->first_live;
  iter.count = order->count;
  iter.generation = order->generation;
  iter.through = order->upper_position;
  do {
    rc = lc_collection_order_batch(order, &iter, row, 1U, &count, &examined,
                                   error);
    if (rc != LC_OK)
      return rc;
    if (count != 0U) {
      order->first_live = iter.next - 1U;
      *found = 1;
      return LC_OK;
    }
  } while (!iter.done);
  return lc_co_error(error, LC_ERR_PROTOCOL,
                     "collection live count contradicts ordered rows");
}

int lc_collection_order_seek(lc_collection_order *order, lc_u64 after,
                             lc_u64 through, lc_u64 generation, int reverse,
                             lc_collection_order_iter *iter, lc_error *error) {
  int rc;
  if (order == NULL || order->fd < 0 || iter == NULL ||
      generation > order->generation) {
    return lc_co_error(error, LC_ERR_INVALID,
                       "collection ordered seek arguments are invalid");
  }
  memset(iter, 0, sizeof(*iter));
  iter->count = order->count;
  iter->after = after;
  iter->through = through;
  iter->generation = generation;
  iter->reverse = reverse != 0;
  if (after >= through || order->count == 0U) {
    iter->done = 1;
    return LC_OK;
  }
  rc = lc_co_upper_bound(order, reverse ? through : after, &iter->next, error);
  if (reverse) {
    if (iter->next == 0U) {
      iter->done = 1;
    } else {
      --iter->next;
    }
  } else if (iter->next == iter->count) {
    iter->done = 1;
  }
  return rc;
}

int lc_collection_order_batch(lc_collection_order *order,
                              lc_collection_order_iter *iter,
                              lc_collection_order_row *rows, size_t capacity,
                              size_t *count, lc_u64 *examined,
                              lc_error *error) {
  lc_co_page page;
  lc_collection_order_row row;
  lc_u64 ordinal;
  lc_u64 number;
  lc_u64 offset;
  lc_u64 page_rows;
  size_t length;
  size_t slot;
  int rc;
  if (count != NULL)
    *count = 0U;
  if (examined != NULL)
    *examined = 0U;
  if (order == NULL || order->fd < 0 || iter == NULL || rows == NULL ||
      capacity == 0U || count == NULL || examined == NULL ||
      iter->count > order->count) {
    return lc_co_error(error, LC_ERR_INVALID,
                       "collection ordered iteration arguments are invalid");
  }
  if (!iter->done) {
    ordinal = iter->next;
    if (ordinal >= iter->count) {
      return lc_co_error(error, LC_ERR_INVALID,
                         "collection ordered iterator is out of bounds");
    }
    number = ordinal / LC_COLLECTION_ORDER_PAGE_ROWS;
    if (!iter->page_loaded || iter->page_number != number) {
      rc = lc_co_page_offset(number, &offset, error);
      if (rc != LC_OK) {
        return rc;
      }
      page_rows = order->count - number * LC_COLLECTION_ORDER_PAGE_ROWS;
      if (page_rows > LC_COLLECTION_ORDER_PAGE_ROWS) {
        page_rows = LC_COLLECTION_ORDER_PAGE_ROWS;
      }
      length = LC_CO_PAGE_HEADER_BYTES + (size_t)page_rows * LC_CO_ROW_BYTES;
      rc = lc_co_io(order->fd, offset, iter->page, length, 0, error);
      if (rc == LC_OK) {
        rc = lc_co_page_decode(order, number, iter->page, &page, error);
      }
      if (rc != LC_OK) {
        return rc;
      }
      iter->page_number = number;
      iter->page_loaded = 1;
    } else {
      /* This page belongs to the read's existing visibility view. Later
       * appends/deletes need not make us reread it or change that view. */
      page.live = lc_co_get64(iter->page + 32U);
      page.deleted_through = lc_co_get64(iter->page + 40U);
      page.first = lc_co_get64(iter->page + 16U);
      page.last = lc_co_get64(iter->page + 24U);
    }
    if (page.live == 0U && page.deleted_through <= iter->generation) {
      if (iter->reverse) {
        *examined = page.first < iter->after ? iter->after : page.first;
        if (number == 0U) {
          iter->done = 1;
        } else {
          iter->next = number * LC_COLLECTION_ORDER_PAGE_ROWS - 1U;
        }
        if (page.first <= iter->after)
          iter->done = 1;
      } else {
        *examined = page.last > iter->through ? iter->through : page.last;
        iter->next = (number + 1U) * LC_COLLECTION_ORDER_PAGE_ROWS;
        if (iter->next > iter->count)
          iter->next = iter->count;
        iter->done = iter->next >= iter->count;
        if (page.last >= iter->through)
          iter->done = 1;
      }
      return LC_OK;
    }
    while (!iter->done && *count < capacity &&
           iter->next / LC_COLLECTION_ORDER_PAGE_ROWS == number) {
      ordinal = iter->next;
      slot = (size_t)(ordinal % LC_COLLECTION_ORDER_PAGE_ROWS);
      rc = lc_co_row_decode(
          order, iter->page + LC_CO_PAGE_HEADER_BYTES + slot * LC_CO_ROW_BYTES,
          &row, error);
      if (rc != LC_OK)
        return rc;
      if (iter->reverse) {
        iter->done = ordinal == 0U;
        iter->next = ordinal == 0U ? 0U : ordinal - 1U;
      } else {
        iter->next = ordinal + 1U;
        iter->done = iter->next >= iter->count;
      }
      if (row.position <= iter->after || row.position > iter->through) {
        *examined = iter->reverse ? iter->after : iter->through;
        iter->done = 1;
        break;
      }
      *examined = row.position;
      if (row.published_at <= iter->generation &&
          (row.deleted_at == 0U || row.deleted_at > iter->generation))
        rows[(*count)++] = row;
    }
  }
  return LC_OK;
}

int lc_collection_order_next(lc_collection_order *order,
                             lc_collection_order_iter *iter,
                             lc_collection_order_row *row, int *found,
                             lc_error *error) {
  lc_u64 examined;
  size_t count;
  int rc;
  if (found == NULL)
    return lc_co_error(error, LC_ERR_INVALID,
                       "collection iterator requires found output");
  *found = 0;
  do {
    rc = lc_collection_order_batch(order, iter, row, 1U, &count, &examined,
                                   error);
    if (rc != LC_OK)
      return rc;
  } while (count == 0U && !iter->done);
  *found = count != 0U;
  return LC_OK;
}

int lc_collection_order_rows(lc_collection_order *order, lc_u64 first,
                             lc_collection_order_row *rows, size_t capacity,
                             size_t *count, lc_error *error) {
  unsigned char bytes[LC_CO_PAGE_BYTES];
  lc_co_page page;
  lc_u64 number;
  lc_u64 offset;
  size_t slot;
  size_t size;
  size_t i;
  int rc;
  if (order == NULL || order->fd < 0 || rows == NULL || capacity == 0U ||
      count == NULL)
    return lc_co_error(error, LC_ERR_INVALID,
                       "collection order batch requires output");
  *count = 0U;
  if (first >= order->count)
    return LC_OK;
  number = first / LC_COLLECTION_ORDER_PAGE_ROWS;
  slot = (size_t)(first % LC_COLLECTION_ORDER_PAGE_ROWS);
  rc = lc_co_page_read(order, number, &page, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_co_page_offset(number, &offset, error);
  if (rc == LC_OK)
    rc =
        lc_co_io(order->fd, offset, bytes,
                 LC_CO_PAGE_HEADER_BYTES + (size_t)page.count * LC_CO_ROW_BYTES,
                 0, error);
  if (rc != LC_OK)
    return rc;
  size = (size_t)page.count - slot;
  if (size > capacity)
    size = capacity;
  for (i = 0U; i < size; ++i) {
    rc = lc_co_row_decode(
        order, bytes + LC_CO_PAGE_HEADER_BYTES + (slot + i) * LC_CO_ROW_BYTES,
        rows + i, error);
    if (rc != LC_OK)
      return rc;
    if (i != 0U && rows[i].position <= rows[i - 1U].position)
      return lc_co_error(error, LC_ERR_PROTOCOL,
                         "collection order batch is not increasing");
  }
  *count = size;
  return LC_OK;
}

int lc_collection_order_edit_begin(lc_collection_order *order,
                                   lc_u64 generation, lc_error *error) {
  if (generation <= order->generation)
    return lc_co_error(error, LC_ERR_PROTOCOL,
                       "invalid edit projection boundary");
  order->edit_in_progress = 1;
  order->generation = order->metadata_generation = generation;
  return lc_co_root_write(order, error);
}

static int lc_co_ranges(lc_collection_order *order,
                        const lc_allocator *allocator,
                        const lc_collection_edit_range *ranges, size_t count,
                        lc_u64 generation, lc_u64 *removed, lc_error *error) {
  unsigned char *bytes;
  lc_collection_order_row row;
  lc_co_page page;
  lc_u64 ordinal, number, offset, through, stamp, rows;
  size_t r, q, i, length;
  int rc = LC_OK, changed;
  *removed = 0;
  if (!count)
    return LC_OK;
  bytes = lc_alloc_with_allocator(allocator, LC_CO_PAGE_BYTES);
  if (bytes == NULL)
    return lc_co_error(error, LC_ERR_NOMEM,
                       "failed to allocate edit metadata page");
  r = 0;
  ordinal = order->count;
  rc = lc_co_upper_bound(order, ranges[0].after, &ordinal, error);
  while (rc == LC_OK && r < count && ordinal < order->count) {
    number = ordinal / LC_COLLECTION_ORDER_PAGE_ROWS;
    rc = lc_co_page_offset(number, &offset, error);
    rows = order->count - number * LC_COLLECTION_ORDER_PAGE_ROWS;
    if (rows > LC_COLLECTION_ORDER_PAGE_ROWS)
      rows = LC_COLLECTION_ORDER_PAGE_ROWS;
    length = LC_CO_PAGE_HEADER_BYTES + (size_t)rows * LC_CO_ROW_BYTES;
    if (rc == LC_OK)
      rc = lc_co_io(order->fd, offset, bytes, length, 0, error);
    if (rc == LC_OK)
      rc = lc_co_page_decode(order, number, bytes, &page, error);
    if (rc != LC_OK)
      break;
    if (page.first > ranges[r].through) {
      ++r;
      if (r < count)
        rc = lc_co_upper_bound(order, ranges[r].after, &ordinal, error);
      continue;
    }
    changed = 0;
    through = 0;
    q = r;
    /* Zero generation selects a read-only count under current metadata
     * authority. Mutation still stamps rows to preserve older reader views. */
    if (!generation && page.first > ranges[r].after &&
        page.last <= ranges[r].through) {
      *removed += page.live;
      i = (size_t)page.count;
    } else
      i = generation ? 0 : (size_t)(ordinal % LC_COLLECTION_ORDER_PAGE_ROWS);
    for (; rc == LC_OK && i < (size_t)page.count; ++i) {
      unsigned char *encoded =
          bytes + LC_CO_PAGE_HEADER_BYTES + i * LC_CO_ROW_BYTES;
      rc = lc_co_row_decode(order, encoded, &row, error);
      if (rc != LC_OK)
        break;
      while (q < count && row.position > ranges[q].through)
        ++q;
      /* Counting does not need mutation's whole-page deletion watermark.
       * Do not decode the rest of a page after a tiny selected interval. */
      if (!generation && (q == count || ranges[q].after >= page.last))
        break;
      if (q < count && row.position > ranges[q].after && row.deleted_at == 0) {
        ++*removed;
        if (!generation)
          continue;
        lc_co_put64(encoded + 16U, generation);
        lc_co_seal(encoded, LC_CO_ROW_BYTES);
        --page.live;
        --order->live_count;
        stamp = row.stored_bytes;
        order->reclaimable_bytes = stamp > LC_U64_MAX - order->reclaimable_bytes
                                       ? LC_U64_MAX
                                       : order->reclaimable_bytes + stamp;
        row.deleted_at = generation;
        changed = 1;
      }
      if (row.deleted_at > through)
        through = row.deleted_at;
    }
    if (rc == LC_OK && changed) {
      lc_co_put64(bytes + 32U, page.live);
      lc_co_put64(bytes + 40U, page.live ? 0 : through);
      lc_co_seal(bytes, LC_CO_PAGE_HEADER_BYTES);
      rc = lc_co_io(order->fd, offset, bytes, length, 1, error);
#ifdef LOCKDC_TEST_BUILD
      if (rc == LC_OK)
        rc = lc_collection_edit_page_test(error);
#endif
    }
    if (rc != LC_OK)
      break;
    while (r < count && ranges[r].through <= page.last)
      ++r;
    if (r < count && ranges[r].after >= page.last)
      rc = lc_co_upper_bound(order, ranges[r].after, &ordinal, error);
    else
      ordinal = (number + 1U) * LC_COLLECTION_ORDER_PAGE_ROWS;
  }
  if (generation && order->live_count == 0)
    order->first_live = order->count;
  lc_free_with_allocator(allocator, bytes);
  return rc;
}

int lc_collection_order_count_ranges(lc_collection_order *order,
                                     const lc_allocator *allocator,
                                     const lc_collection_edit_range *ranges,
                                     size_t count, lc_u64 *out,
                                     lc_error *error) {
  return lc_co_ranges(order, allocator, ranges, count, 0, out, error);
}

int lc_collection_order_cut(lc_collection_order *order,
                            const lc_allocator *allocator,
                            const lc_collection_edit_range *ranges,
                            size_t count, lc_u64 generation, lc_u64 *removed,
                            lc_error *error) {
  return lc_co_ranges(order, allocator, ranges, count, generation, removed,
                      error);
}
