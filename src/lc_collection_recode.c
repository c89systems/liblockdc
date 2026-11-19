#include "lc_collection_recode.h"
#include "lc_collection_chunks.h"
#include "lc_collection_record_recode.h"
#include "lc_collection_unit_recode.h"
#include "lc_pouch_path.h"
#include "lc_pouch_record.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>

typedef struct cr_visit {
  lc_pouch_recode *work;
  const char *scope;
} cr_visit;

typedef struct cr_output {
  lc_pouch_recode *work;
  lc_pouch_recode_file *file;
  lc_collection_unit_recode *codec;
  unsigned char *buffer;
  lc_u64 first, current, parts, bytes;
  unsigned int mode;
  int source_fd, fd, hold;
  char leaf[64];
} cr_output;

static lc_u64 cr_get(const unsigned char *p) {
  lc_u64 n = 0;
  size_t i;
  for (i = 0; i < 8U; ++i)
    n |= (lc_u64)p[i] << (8U * i);
  return n;
}
static void cr_put(unsigned char *p, lc_u64 n) {
  size_t i;
  for (i = 0; i < 8U; ++i)
    p[i] = (unsigned char)(n >> (8U * i));
}
static int cr_error(lc_error *e, const char *message) {
  return lc_collection_failure(e, LC_ERR_TRANSPORT, NULL, message);
}
static int cr_corrupt(lc_error *e, const char *message) {
  return lc_collection_failure(e, LC_ERR_PROTOCOL, "collection_corrupt",
                               message);
}
static int cr_read(int fd, void *bytes, size_t count, lc_u64 offset,
                   lc_error *e) {
  unsigned char *p = bytes;
  ssize_t n;
  while (count) {
    n = pread(fd, p, count, (off_t)offset);
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      return n < 0 ? cr_error(e, "failed to read collection conversion source")
                   : cr_corrupt(e, "collection conversion source is truncated");
    p += (size_t)n;
    offset += (size_t)n;
    count -= (size_t)n;
  }
  return LC_OK;
}
static lc_pouch_recode_file *cr_file(lc_pouch_recode *work, const char *scope,
                                     lc_u64 segment, lc_error *e) {
  char leaf[64];
  size_t first = 0, last = work->count, mid;
  int order;
  if (lc_collection_record_leaf(segment, leaf, e) != LC_OK)
    return NULL;
  while (first < last) {
    mid = first + (last - first) / 2U;
    order = strcmp(work->files[mid].scope, scope);
    if (!order)
      order = strcmp(work->files[mid].leaf, leaf);
    if (order < 0)
      first = mid + 1U;
    else
      last = mid;
  }
  if (first < work->count && !strcmp(work->files[first].scope, scope) &&
      !strcmp(work->files[first].leaf, leaf))
    return &work->files[first];
  cr_corrupt(e, "collection index refers outside retained canonical history");
  return NULL;
}
static int cr_mark(void *context, const lc_collection_chunk_ref *ref,
                   unsigned int height, lc_error *e) {
  cr_visit *visit = context;
  lc_pouch_recode_file *file =
      cr_file(visit->work, visit->scope, ref->segment, e);
  lc_pouch_recode_reference *grown, *entry;
  size_t capacity;
  if (!file)
    return e && e->code != LC_OK ? e->code : LC_ERR_PROTOCOL;
  if (file->reference_count == file->reference_capacity) {
    capacity = file->reference_capacity ? file->reference_capacity * 2U : 64U;
    if (capacity < file->reference_capacity ||
        capacity > (size_t)-1 / sizeof(*grown))
      return lc_collection_failure(e, LC_ERR_NOMEM, NULL,
                                   "collection relocation table is too large");
    grown =
        lc_realloc_with_allocator(&visit->work->pouch->allocator,
                                  file->references, capacity * sizeof(*grown));
    if (!grown)
      return lc_collection_failure(
          e, LC_ERR_NOMEM, NULL,
          "failed to allocate collection relocation metadata");
    file->references = grown;
    file->reference_capacity = capacity;
  }
  entry = &file->references[file->reference_count++];
  entry->offset = ref->offset;
  entry->bytes = ref->bytes;
  entry->height = height;
  return LC_OK;
}
static int cr_reference_compare(const void *a, const void *b) {
  const lc_pouch_recode_reference *x = a, *y = b;
  return x->offset < y->offset ? -1 : x->offset != y->offset;
}
static int cr_reference_sort(lc_pouch_recode_file *file, lc_error *e) {
  size_t i, used = 0;
  if (file->reference_count > 1U)
    qsort(file->references, file->reference_count, sizeof(*file->references),
          cr_reference_compare);
  for (i = 0; i < file->reference_count; ++i) {
    if (used &&
        file->references[used - 1U].offset == file->references[i].offset) {
      if (file->references[used - 1U].bytes != file->references[i].bytes ||
          file->references[used - 1U].height != file->references[i].height)
        return cr_corrupt(
            e, "collection index gives inconsistent physical references");
    } else
      file->references[used++] = file->references[i];
  }
  file->reference_count = used;
  return LC_OK;
}
static int cr_alias(lc_pouch_recode *work, lc_pouch_recode_file *file,
                    const lc_pouch_collection_record *record, lc_error *e) {
  lc_pouch_collection_record body;
  char *path = lc_pouch_path_join(&work->pouch->allocator, file->scope,
                                  record->payload_container);
  struct stat st;
  int fd = path ? open(path, O_RDONLY | O_CLOEXEC) : -1;
  int rc = path ? LC_OK : LC_ERR_NOMEM;
  if (rc == LC_OK && (fd < 0 || fstat(fd, &st) != 0 || st.st_size < 0))
    rc = cr_error(e, "failed to inspect collection conversion reference");
  if (rc == LC_OK)
    rc = lc_pouch_collection_record_read(
        lc_pouch_record_file_io(fd), record->payload_record_offset,
        (lc_u64)st.st_size, record->payload_container, &body, e);
  if (rc == LC_OK)
    rc = lc_pouch_collection_record_check_body(record, &body, e);
  if (fd >= 0)
    (void)close(fd);
  lc_free_with_allocator(&work->pouch->allocator, path);
  return rc;
}

/* Scan only outer canonical metadata. Private allocation bytes need not be
 * parseable unless a published root actually references them. */
static int cr_scan(lc_pouch_recode *work, lc_pouch_recode_file *file,
                   int collect, lc_error *e) {
  lc_pouch_collection_record record;
  lc_collection_chunk_reader reader = {0};
  cr_visit visit;
  struct stat st;
  lc_u64 at = 0, limit, bound = 1, end, previous_end;
  size_t next = 0;
  int fd = open(file->source, O_RDONLY | O_CLOEXEC), rc = LC_OK;
  if (fd < 0 || fstat(fd, &st) != 0 || st.st_size < 0)
    rc = cr_error(e, "failed to inspect collection conversion source");
  limit = rc == LC_OK ? (lc_u64)st.st_size : 0;
  visit.work = work;
  visit.scope = file->scope;
  while (rc == LC_OK && at < limit) {
    if (limit - at < LC_POUCH_RECORD_HEADER_BYTES && file->active)
      break;
    rc = lc_pouch_collection_record_read(lc_pouch_record_file_io(fd), at, limit,
                                         file->leaf, &record, e);
    if (rc == LC_ERR_PENDING && file->active) {
      lc_error_cleanup(e);
      lc_error_init(e);
      rc = LC_OK;
      break;
    }
    if (rc != LC_OK)
      break;
    if (collect) {
      if (record.kind == LC_POUCH_COLLECTION_STREAM) {
        rc = lc_collection_chunks_open(
            &reader, &work->pouch->allocator, work->pouch->crypto,
            file->collection_id,
            lc_collection_chunks_file_access(&reader, file->scope),
            lc_pouch_record_file_io(fd), &record, e);
        if (rc == LC_OK)
          rc = lc_collection_chunks_visit(&reader, cr_mark, &visit, e);
      } else if (record.metadata_only)
        rc = cr_alias(work, file, &record, e);
      if (bound == LC_U64_MAX)
        rc = cr_corrupt(e, "collection conversion record count overflows");
      else
        ++bound;
    } else {
      previous_end = at;
      while (next < file->reference_count &&
             file->references[next].offset < record.next_offset) {
        lc_pouch_recode_reference *ref = &file->references[next++];
        lc_u64 prefix = ref->height ? 40U : LC_COLLECTION_UNIT_HEADER;
        if (ref->offset < previous_end ||
            ref->offset > (lc_u64)LC_I64_MAX - prefix - ref->bytes) {
          rc = cr_corrupt(e, "collection unit references overlap");
          break;
        }
        end = ref->offset + prefix + ref->bytes;
        if (record.kind == LC_POUCH_COLLECTION_REGION) {
          if (ref->height || ref->offset < at + 32U || end > record.next_offset)
            rc = cr_corrupt(e, "collection unit escapes its canonical range");
        } else if (ref->offset != at || end != record.next_offset ||
                   record.kind != (ref->height ? LC_POUCH_COLLECTION_NODE
                                               : LC_POUCH_COLLECTION_CHUNK))
          rc = cr_corrupt(e, "collection index refers inside another record");
        previous_end = end;
        if (rc != LC_OK)
          break;
      }
    }
    at = record.next_offset;
  }
  if (rc == LC_OK && !collect && next != file->reference_count)
    rc = cr_corrupt(e, "collection index refers beyond committed history");
  /* A durable reachability table may not depend on volatile NoSync source
   * bytes. Seal the observed history before its table can outlive a restart. */
  if (rc == LC_OK && !collect && fsync(fd) != 0)
    rc = cr_error(e, "failed to seal collection conversion source");
  if (collect)
    file->output_parts = bound;
  lc_collection_chunks_reader_close(&reader);
  if (fd >= 0)
    (void)close(fd);
  return rc;
}

static int cr_reach_table(lc_pouch_recode *work, lc_pouch_recode_file *file,
                          size_t index, const char *directory, int load,
                          lc_error *e) {
  char *path = lc_pouch_recode_progress(work, directory, index, "reach");
  char *tmp =
      lc_pouch_path_join(&work->pouch->allocator, directory, "journal.tmp");
  FILE *fp = path && tmp
                 ? (load ? fopen(path, "rb") : lc_pouch_recode_create(tmp))
                 : NULL;
  struct stat st;
  lc_u64 count = file->reference_count;
  size_t i;
  int valid, rc = path && tmp ? LC_OK : LC_ERR_NOMEM;
  if (!fp && rc == LC_OK)
    rc = cr_error(e, "failed to open collection reachability table");
  if (rc != LC_OK)
    goto done;
  if (load) {
    valid = lc_pouch_recode_get64(fp, &count) && fstat(fileno(fp), &st) == 0 &&
            st.st_size >= 8 && count == (lc_u64)(st.st_size - 8) / 24U &&
            (st.st_size - 8) % 24U == 0 &&
            count <= (size_t)-1 / sizeof(*file->references);
    if (valid && count) {
      file->references = lc_alloc_with_allocator(
          &work->pouch->allocator, (size_t)count * sizeof(*file->references));
      if (!file->references) {
        rc = LC_ERR_NOMEM;
        valid = 0;
      }
    }
    for (i = 0; valid && i < count; ++i) {
      lc_pouch_recode_reference *ref = &file->references[i];
      valid = lc_pouch_recode_get64(fp, &ref->offset) &&
              lc_pouch_recode_get64(fp, &ref->bytes) &&
              lc_pouch_recode_get64(fp, &ref->height) &&
              ref->height < LC_COLLECTION_CHUNK_LEVELS && ref->bytes &&
              ref->bytes <= (ref->height ? LC_COLLECTION_CHUNK_PAGE
                                         : LC_COLLECTION_CHUNK_MAX) &&
              (!i || ref->offset > file->references[i - 1U].offset);
    }
    if (!valid || fgetc(fp) != EOF || ferror(fp))
      rc = rc != LC_OK ? rc : lc_pouch_recode_journal_error(e);
    if (rc == LC_OK)
      file->reference_count = file->reference_capacity = (size_t)count;
    (void)fclose(fp);
  } else {
    valid = lc_pouch_recode_put64(fp, count);
    for (i = 0; valid && i < count; ++i)
      valid = lc_pouch_recode_put64(fp, file->references[i].offset) &&
              lc_pouch_recode_put64(fp, file->references[i].bytes) &&
              lc_pouch_recode_put64(fp, file->references[i].height);
    rc = lc_pouch_recode_commit(fp, tmp, path, directory, valid, e);
  }
done:
  lc_free_with_allocator(&work->pouch->allocator, path);
  lc_free_with_allocator(&work->pouch->allocator, tmp);
  return rc;
}

int lc_collection_recode_references(lc_pouch_recode *work,
                                    const char *directory, lc_error *e) {
  char *path =
      lc_pouch_path_join(&work->pouch->allocator, directory, "reachable");
  char *tmp =
      lc_pouch_path_join(&work->pouch->allocator, directory, "journal.tmp");
  FILE *fp;
  lc_u64 count, bound = 0;
  size_t i;
  int ready = 0, rc = path && tmp ? LC_OK : LC_ERR_NOMEM;
  if (rc != LC_OK)
    goto done;
  fp = fopen(path, "rb");
  if (fp) {
    ready = lc_pouch_recode_get64(fp, &count) && count == work->count &&
            fgetc(fp) == EOF && !ferror(fp);
    (void)fclose(fp);
    if (!ready)
      rc = lc_pouch_recode_journal_error(e);
  } else if (errno != ENOENT)
    rc = cr_error(e, "failed to read collection reachability boundary");
  for (i = 0; rc == LC_OK && !ready && i < work->count; ++i)
    if (work->files[i].collection_id[0])
      rc = cr_scan(work, &work->files[i], 1, e);
  for (i = 0; rc == LC_OK && i < work->count; ++i) {
    lc_pouch_recode_file *file = &work->files[i];
    if (!file->collection_id[0])
      continue;
    if (!ready) {
      if (!i || strcmp(file->scope, work->files[i - 1U].scope) ||
          strncmp(file->leaf, work->files[i - 1U].leaf, 4))
        bound = 0;
      rc = cr_reference_sort(file, e);
      if (rc == LC_OK)
        rc = cr_scan(work, file, 0, e);
      if (rc == LC_OK) {
        lc_u64 available = LC_COLLECTION_SNAPSHOT_BIT - 1U - file->output_floor;
        if (file->output_parts > available ||
            file->reference_count > available - file->output_parts ||
            bound > available - file->output_parts - file->reference_count)
          rc = lc_collection_failure(
              e, LC_ERR_INVALID, "sequence_exhausted",
              "collection has insufficient identities for conversion");
        else
          bound += file->output_parts + file->reference_count;
      }
    }
    if (rc == LC_OK)
      rc = cr_reach_table(work, file, i, directory, ready, e);
    file->output_parts = 0;
  }
  if (rc == LC_OK && !ready) {
    fp = lc_pouch_recode_create(tmp);
    if (!fp)
      rc = cr_error(e, "failed to write collection reachability boundary");
    else
      rc = lc_pouch_recode_commit(fp, tmp, path, directory,
                                  lc_pouch_recode_put64(fp, work->count), e);
  }
done:
  lc_free_with_allocator(&work->pouch->allocator, path);
  lc_free_with_allocator(&work->pouch->allocator, tmp);
  return rc;
}

static lc_u64 cr_class(const lc_pouch_recode_file *file) {
  return !strncmp(file->leaf, "snapshot-", 9) ? LC_COLLECTION_SNAPSHOT_BIT : 0;
}
static int cr_open_part(cr_output *out, lc_error *e) {
  char *path;
  int rc;
  if (out->fd >= 0 && (out->current & (lc_u64)LC_I64_MAX) == (lc_u64)LC_I64_MAX)
    return lc_collection_failure(
        e, LC_ERR_INVALID, "sequence_exhausted",
        "collection conversion identities are exhausted");
  if (out->fd >= 0) {
    if (fchmod(out->fd, (mode_t)out->mode) != 0)
      return cr_error(e, "failed to preserve converted collection mode");
    if (fsync(out->fd) != 0)
      return cr_error(e, "failed to sync converted collection part");
    if (close(out->fd) != 0) {
      out->fd = -1;
      return cr_error(e, "failed to close converted collection part");
    }
    out->fd = -1;
    ++out->current;
  }
  rc = lc_collection_record_leaf(out->current, out->leaf, e);
  path = rc == LC_OK ? lc_pouch_path_join(&out->work->pouch->allocator,
                                          out->file->scope, out->leaf)
                     : NULL;
  if (rc == LC_OK && !path)
    rc = LC_ERR_NOMEM;
  if (rc == LC_OK) {
    out->fd = open(path, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (out->fd < 0)
      rc = cr_error(e, "failed to create converted collection part");
  }
  if (rc == LC_OK)
    ++out->parts;
  lc_free_with_allocator(&out->work->pouch->allocator, path);
  return rc;
}
static int cr_place(cr_output *out, lc_u64 *offset, lc_error *e) {
  off_t end;
  int rc = LC_OK;
  end = lseek(out->fd, 0, SEEK_END);
  if (end < 0)
    return cr_error(e, "failed to seek converted collection part");
  /* One indivisible codec unit may cross the soft target. Testing the actual
   * end, rather than its plaintext bound, keeps compressed units packed. */
  if (!out->hold && (lc_u64)end >= out->work->pouch->segment_target_bytes) {
    rc = cr_open_part(out, e);
    end = 0;
  }
  *offset = (lc_u64)end;
  return rc;
}
static int cr_unit(cr_output *out, const lc_pouch_recode_reference *ref,
                   lc_pouch_recode_offset *map, lc_error *e) {
  lc_collection_unit unit;
  lc_u64 offset;
  int rc = LC_OK;
  if (!out->codec)
    rc = lc_collection_unit_recode_open(
        &out->work->pouch->allocator, out->work->pouch->crypto, &out->codec, e);
  if (rc == LC_OK)
    rc = lc_collection_unit_recode_run(
        out->codec, lc_collection_unit_file_io(out->source_fd), ref->offset,
        ref->bytes, lc_collection_unit_file_io(out->fd), &offset, &unit, e);
  if (rc == LC_OK && offset != map->after)
    rc = cr_corrupt(e, "converted collection unit moved outside its placement");
  if (rc == LC_OK) {
    map->stored = unit.stored;
    map->units = 1;
    map->first_segment = map->segment;
    map->first_offset = map->after;
  }
  return rc;
}

static int cr_node(cr_output *out, const lc_pouch_recode_reference *ref,
                   lc_pouch_recode_offset *map, lc_error *e) {
  lc_collection_chunk_ref result;
  lc_pouch_record_header header;
  unsigned char prefix[40];
  lc_pouch_recode_offset *child;
  lc_pouch_recode_file *owner;
  size_t at;
  char leaf[64];
  int rc;
  if (!ref->bytes || ref->bytes > LC_COLLECTION_CHUNK_PAGE ||
      ref->bytes % LC_COLLECTION_CHUNK_TUPLE)
    return cr_corrupt(e,
                      "collection conversion index exceeds its bounded page");
  if (!out->buffer)
    out->buffer = lc_alloc_with_allocator(&out->work->pouch->allocator,
                                          LC_COLLECTION_CHUNK_PAGE);
  if (!out->buffer)
    return lc_collection_failure(e, LC_ERR_NOMEM, NULL,
                                 "cannot allocate collection index scratch");
  rc = cr_read(out->source_fd, prefix, sizeof(prefix), ref->offset, e);
  if (rc == LC_OK)
    rc = lc_pouch_record_header_decode(prefix, &header, e);
  if (rc == LC_OK)
    rc = cr_read(out->source_fd, out->buffer, (size_t)ref->bytes,
                 ref->offset + 40U, e);
  if (rc == LC_OK &&
      (header.type != LC_POUCH_COLLECTION_NODE || header.flags ||
       header.key_len || header.meta_len != 8U ||
       header.payload_len != ref->bytes ||
       cr_get(prefix + 32U) != ref->height - 1U ||
       header.payload_crc !=
           (unsigned long)crc32(0L, out->buffer, (uInt)ref->bytes)))
    rc = cr_corrupt(e, "collection conversion index checksum is invalid");
  for (at = 0; rc == LC_OK && at < ref->bytes; at += 24U) {
    unsigned char *tuple = out->buffer + at;
    rc = lc_collection_record_leaf(cr_get(tuple), leaf, e);
    if (rc == LC_OK)
      rc = lc_pouch_recode_lookup(out->work, out->file->scope, leaf,
                                  cr_get(tuple + 8U), &owner, &child, e);
    if (rc != LC_OK)
      break;
    if (!child->segment || !child->units || !child->first_segment ||
        child->stored > LC_U64_MAX - map->stored ||
        child->units > LC_U64_MAX - map->units)
      return cr_corrupt(e, "converted collection node has an invalid child");
    if (!at) {
      map->first_segment = child->first_segment;
      map->first_offset = child->first_offset;
    }
    map->stored += child->stored;
    map->units += child->units;
    cr_put(tuple, child->segment);
    cr_put(tuple + 8U, child->after);
    cr_put(tuple + 16U,
           ref->height == 1U ? child->stored : cr_get(tuple + 16U));
  }
  if (rc == LC_OK)
    rc = lc_collection_chunks_write_index(
        lc_collection_unit_file_io(out->fd), map->segment,
        (unsigned int)ref->height - 1U, out->buffer, (size_t)ref->bytes,
        &result, e);
  return rc;
}
static int cr_extent(cr_output *out, const lc_pouch_recode_reference *ref,
                     lc_error *e) {
  lc_u64 after;
  lc_pouch_recode_offset *map;
  int rc = cr_place(out, &after, e);
  if (rc == LC_OK)
    rc =
        lc_pouch_recode_offset_add(out->work, out->file, ref->offset, after, e);
  if (rc != LC_OK)
    return rc;
  map = &out->file->offsets[out->file->count - 1U];
  map->segment = out->current;
  return ref->height ? cr_node(out, ref, map, e) : cr_unit(out, ref, map, e);
}

static int cr_record(lc_pouch_recode *work, lc_pouch_recode_file *file,
                     int source_fd, lc_u64 offset, lc_u64 limit, int fd,
                     const char *destination_leaf, lc_error *error) {
  lc_pouch_collection_record current, body, converted;
  lc_collection_recode_stream stream;
  lc_collection_recode_links links;
  lc_pouch_recode_file *owner;
  lc_pouch_recode_offset *location, *root;
  lc_pouch_record_header header;
  unsigned char prefix[32];
  struct iovec part;
  struct stat st;
  char leaf[64], previous_leaf[64], *path = NULL;
  int body_fd = -1, rc;
  memset(&stream, 0, sizeof(stream));
  memset(&links, 0, sizeof(links));
  rc = lc_pouch_collection_record_read(lc_pouch_record_file_io(source_fd),
                                       offset, limit, file->leaf, &current,
                                       error);
  if (rc != LC_OK)
    return rc;
  if (current.metadata_only) {
    rc = lc_pouch_recode_lookup(work, file->scope, current.payload_container,
                                current.payload_record_offset, &owner,
                                &location, error);
    if (rc == LC_OK)
      path =
          lc_pouch_recode_location(work, owner, location->segment, leaf, error);
    if (rc == LC_OK && !path)
      rc = LC_ERR_NOMEM;
    if (rc == LC_OK) {
      body_fd = open(path, O_RDONLY | O_CLOEXEC);
      if (body_fd < 0 || fstat(body_fd, &st) != 0 || st.st_size < 0)
        rc = cr_error(error, "failed to inspect converted collection body");
    }
    if (rc == LC_OK)
      rc = lc_pouch_collection_record_read(lc_pouch_record_file_io(body_fd),
                                           location->after, (lc_u64)st.st_size,
                                           leaf, &body, error);
    if (rc == LC_OK)
      links.body = &body;
    if (rc == LC_OK && current.previous_segment) {
      rc = lc_collection_record_leaf(current.previous_segment, previous_leaf,
                                     error);
      if (rc == LC_OK)
        rc = lc_pouch_recode_lookup(work, file->scope, previous_leaf,
                                    current.previous_offset, &owner, &location,
                                    error);
      if (rc == LC_OK) {
        links.previous_segment = location->segment;
        links.previous_offset = location->after;
      }
    }
  } else if (current.stream_extents) {
    rc = lc_collection_record_leaf(current.stream_segment, leaf, error);
    if (rc == LC_OK)
      rc = lc_pouch_recode_lookup(work, file->scope, leaf,
                                  current.stream_offset, &owner, &root, error);
    if (rc == LC_OK && (root->units != current.stream_extents ||
                        !root->first_segment || !root->stored))
      rc = cr_corrupt(error,
                      "converted collection index changed its unit count");
    if (rc == LC_OK)
      path = lc_pouch_recode_location(work, owner, root->first_segment, leaf,
                                      error);
    if (rc == LC_OK && !path)
      rc = LC_ERR_NOMEM;
    if (rc == LC_OK) {
      body_fd = open(path, O_RDONLY | O_CLOEXEC);
      if (body_fd < 0)
        rc = cr_error(error,
                      "failed to open converted collection manifest unit");
    }
    part.iov_base = prefix;
    part.iov_len = sizeof(prefix);
    if (rc == LC_OK)
      rc = lc_pouch_record_io_read(lc_collection_unit_file_io(body_fd),
                                   root->first_offset, &part, 1, error);
    if (rc == LC_OK)
      rc = lc_pouch_record_header_decode(prefix, &header, error);
    if (rc == LC_OK)
      rc = lc_collection_unit_read(lc_collection_unit_file_io(body_fd),
                                   root->first_offset, header.payload_len,
                                   &stream.first, error);
    if (rc == LC_OK) {
      stream.root.segment = root->segment;
      stream.root.offset = root->after;
      stream.root.bytes =
          current.stream_height ? current.stream_root_bytes : root->stored;
      stream.units = root->units;
      stream.stored = root->stored;
      stream.height = current.stream_height;
      links.stream = &stream;
    }
  }
  if (rc == LC_OK)
    rc = lc_pouch_collection_record_recode(
        work->pouch, work->pouch->crypto, file->collection_id,
        lc_pouch_record_file_io(source_fd), file->leaf, offset, limit, &links,
        lc_pouch_record_file_io(fd), destination_leaf, &converted, error);
  if (body_fd >= 0)
    (void)close(body_fd);
  lc_free_with_allocator(&work->pouch->allocator, path);
  return rc;
}

static int cr_convert(cr_output *out, lc_error *e) {
  lc_pouch_collection_record record;
  struct stat st;
  lc_u64 at = 0, after = 0, limit;
  lc_pouch_recode_offset *map;
  size_t next = 0;
  int rc;
  out->source_fd = open(out->file->source, O_RDONLY | O_CLOEXEC);
  if (out->source_fd < 0 || fstat(out->source_fd, &st) != 0 || st.st_size < 0)
    return cr_error(e, "failed to inspect original collection segment");
  limit = (lc_u64)st.st_size;
  out->mode = (unsigned int)(st.st_mode & 0777);
  rc = cr_open_part(out, e);
  while (rc == LC_OK && at < limit) {
    if (limit - at < 32U && out->file->active)
      break;
    rc =
        lc_pouch_collection_record_read(lc_pouch_record_file_io(out->source_fd),
                                        at, limit, out->file->leaf, &record, e);
    if (rc == LC_ERR_PENDING && out->file->active) {
      lc_error_cleanup(e);
      lc_error_init(e);
      rc = LC_OK;
      break;
    }
    if (rc != LC_OK)
      break;
    if (next < out->file->reference_count &&
        out->file->references[next].offset == at &&
        (record.kind == LC_POUCH_COLLECTION_CHUNK ||
         record.kind == LC_POUCH_COLLECTION_NODE)) {
      rc = cr_extent(out, &out->file->references[next++], e);
    } else {
      rc = cr_place(out, &after, e);
      if (rc == LC_OK)
        rc = lc_pouch_recode_offset_add(out->work, out->file, at, after, e);
      map = rc == LC_OK ? &out->file->offsets[out->file->count - 1U] : NULL;
      if (map)
        map->segment = out->current;
      if (rc == LC_OK && record.kind == LC_POUCH_COLLECTION_REGION) {
        while (rc == LC_OK && next < out->file->reference_count &&
               out->file->references[next].offset < record.next_offset)
          rc = cr_extent(out, &out->file->references[next++], e);
      } else if (rc == LC_OK && record.kind != LC_POUCH_COLLECTION_CHUNK &&
                 record.kind != LC_POUCH_COLLECTION_NODE) {
        rc = cr_record(out->work, out->file, out->source_fd, at, limit, out->fd,
                       out->leaf, e);
        /* A provisional edit body and its control are one publication. Keep
         * them adjacent even when this source pair crosses the soft target. */
        out->hold = record.kind == LC_POUCH_COLLECTION_EDIT_BODY;
      }
    }
    at = record.next_offset;
  }
  if (rc == LC_OK && next != out->file->reference_count)
    rc = cr_corrupt(e, "converted collection missed a published unit");
  if (rc == LC_OK) {
    off_t end = lseek(out->fd, 0, SEEK_END);
    if (end < 0)
      rc = cr_error(e, "failed to locate converted collection end");
    else
      after = (lc_u64)end;
  }
  if (rc == LC_OK && (!out->file->count ||
                      out->file->offsets[out->file->count - 1U].before < at)) {
    rc = lc_pouch_recode_offset_add(out->work, out->file, at, after, e);
    if (rc == LC_OK)
      out->file->offsets[out->file->count - 1U].segment = out->current;
  }
  if (rc == LC_OK && at != limit) {
    rc = lc_pouch_recode_offset_add(out->work, out->file, limit, after, e);
    if (rc == LC_OK)
      out->file->offsets[out->file->count - 1U].segment = out->current;
  }
  if (rc == LC_OK && fchmod(out->fd, (mode_t)out->mode) != 0)
    rc = cr_error(e, "failed to preserve final collection part mode");
  if (rc == LC_OK && fsync(out->fd) != 0)
    rc = cr_error(e, "failed to sync final collection conversion part");
  return rc;
}

static int cr_remove_outputs(lc_pouch_recode *work, lc_pouch_recode_file *file,
                             lc_u64 first, lc_error *e) {
  DIR *dir = opendir(file->scope);
  struct dirent *entry;
  char *path;
  lc_u64 id, kind = cr_class(file);
  size_t prefix = kind ? 9U : 4U;
  int rc = dir ? LC_OK
               : cr_error(e, "failed to inspect unfinished collection outputs");
  while (rc == LC_OK) {
    errno = 0;
    entry = readdir(dir);
    if (!entry) {
      if (errno)
        rc = cr_error(e, "failed to enumerate unfinished collection outputs");
      break;
    }
    if (strlen(entry->d_name) != prefix + 24U ||
        strncmp(entry->d_name, kind ? "snapshot-" : "seg-", prefix) ||
        strcmp(entry->d_name + prefix + 20U, ".log") ||
        !lc_parse_u64_base10_range_checked(entry->d_name + prefix, 20U, &id) ||
        !id || id >= LC_COLLECTION_SNAPSHOT_BIT || (id | kind) < first)
      continue;
    path =
        lc_pouch_path_join(&work->pouch->allocator, file->scope, entry->d_name);
    if (!path)
      rc = LC_ERR_NOMEM;
    else if (unlink(path) != 0)
      rc = cr_error(e, "failed to discard unfinished collection output");
    lc_free_with_allocator(&work->pouch->allocator, path);
  }
  if (dir)
    (void)closedir(dir);
  if (rc == LC_OK)
    rc = lc_pouch_path_fsync_directory(
        file->scope, "failed to sync discarded collection output", e);
  return rc;
}

static int cr_map(lc_pouch_recode *work, lc_pouch_recode_file *file,
                  size_t index, const char *directory, lc_u64 before,
                  lc_u64 input, lc_u64 output, int load, lc_error *e) {
  char *path = lc_pouch_recode_progress(work, directory, index, "map");
  char *tmp =
      lc_pouch_path_join(&work->pouch->allocator, directory, "journal.tmp");
  FILE *fp = path && tmp
                 ? (load ? fopen(path, "rb") : lc_pouch_recode_create(tmp))
                 : NULL;
  struct stat st;
  lc_u64 count = file->count, first = file->output_first,
         parts = file->output_parts;
  size_t i;
  int valid, rc = path && tmp ? LC_OK : LC_ERR_NOMEM;
  if (!fp && rc == LC_OK)
    rc = cr_error(e, "failed to open collection conversion map");
  if (rc != LC_OK)
    goto done;
  if (load) {
    valid = lc_pouch_recode_get64(fp, &before) &&
            lc_pouch_recode_get64(fp, &input) &&
            lc_pouch_recode_get64(fp, &output) &&
            lc_pouch_recode_get64(fp, &first) &&
            lc_pouch_recode_get64(fp, &parts) && parts &&
            (first & (lc_u64)LC_I64_MAX) > file->output_floor &&
            (first & LC_COLLECTION_SNAPSHOT_BIT) == cr_class(file) &&
            parts - 1U <= (lc_u64)LC_I64_MAX - (first & (lc_u64)LC_I64_MAX) &&
            lc_pouch_recode_get64(fp, &count) && fstat(fileno(fp), &st) == 0 &&
            st.st_size >= 48 && count == (lc_u64)(st.st_size - 48) / 56U &&
            (st.st_size - 48) % 56U == 0 &&
            count <= (size_t)-1 / sizeof(*file->offsets);
    for (i = 0; valid && i < count; ++i) {
      lc_u64 from, to;
      valid =
          lc_pouch_recode_get64(fp, &from) && lc_pouch_recode_get64(fp, &to);
      if (valid) {
        rc = lc_pouch_recode_offset_add(work, file, from, to, e);
        valid = rc == LC_OK;
      }
      if (valid) {
        lc_pouch_recode_offset *row = &file->offsets[file->count - 1U];
        valid = lc_pouch_recode_get64(fp, &row->segment) &&
                lc_pouch_recode_get64(fp, &row->stored) &&
                lc_pouch_recode_get64(fp, &row->units) &&
                lc_pouch_recode_get64(fp, &row->first_segment) &&
                lc_pouch_recode_get64(fp, &row->first_offset) &&
                row->segment >= first && row->segment - first < parts &&
                (!i || row->before > file->offsets[file->count - 2U].before);
      }
    }
    if (!valid || fgetc(fp) != EOF || ferror(fp))
      rc = rc != LC_OK ? rc : lc_pouch_recode_journal_error(e);
    (void)fclose(fp);
    if (rc == LC_OK) {
      file->output_first = first;
      file->output_parts = parts;
      work->input_bytes += input;
      work->output_bytes += output;
      if (stat(file->source, &st) == 0) {
        if ((lc_u64)st.st_ino != before)
          rc = lc_pouch_recode_journal_error(e);
      } else if (errno != ENOENT)
        rc = cr_error(e, "failed to inspect retired collection source");
    }
  } else {
    valid =
        lc_pouch_recode_put64(fp, before) && lc_pouch_recode_put64(fp, input) &&
        lc_pouch_recode_put64(fp, output) && lc_pouch_recode_put64(fp, first) &&
        lc_pouch_recode_put64(fp, parts) && lc_pouch_recode_put64(fp, count);
    for (i = 0; valid && i < count; ++i) {
      lc_pouch_recode_offset *row = &file->offsets[i];
      valid = lc_pouch_recode_put64(fp, row->before) &&
              lc_pouch_recode_put64(fp, row->after) &&
              lc_pouch_recode_put64(fp, row->segment) &&
              lc_pouch_recode_put64(fp, row->stored) &&
              lc_pouch_recode_put64(fp, row->units) &&
              lc_pouch_recode_put64(fp, row->first_segment) &&
              lc_pouch_recode_put64(fp, row->first_offset);
    }
    rc = lc_pouch_recode_commit(fp, tmp, path, directory, valid, e);
  }
done:
  lc_free_with_allocator(&work->pouch->allocator, path);
  lc_free_with_allocator(&work->pouch->allocator, tmp);
  return rc;
}

static int cr_seal_parts(lc_pouch_recode *work, lc_pouch_recode_file *file,
                         lc_u64 *bytes, lc_error *e) {
  char leaf[64], *path;
  lc_u64 part;
  struct stat st;
  int rc = LC_OK;
  *bytes = 0;
  for (part = 0; rc == LC_OK && part < file->output_parts; ++part) {
    path = lc_pouch_recode_location(work, file, file->output_first + part, leaf,
                                    e);
    if (!path)
      rc = LC_ERR_NOMEM;
    else if (stat(path, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 0)
      rc = cr_error(e, "converted collection output is missing");
    else if ((lc_u64)st.st_size > LC_U64_MAX - *bytes)
      rc = cr_corrupt(e, "converted collection byte count overflows");
    else
      *bytes += (lc_u64)st.st_size;
    lc_free_with_allocator(&work->pouch->allocator, path);
  }
  /* Each part already restored its mode and synced exactly once at close.
   * This directory barrier must precede the source-retirement receipt. */
  if (rc == LC_OK)
    rc = lc_pouch_path_fsync_directory(
        file->scope, "failed to sync converted collection names", e);
  return rc;
}

int lc_collection_recode_segment(lc_pouch_recode *work, size_t index,
                                 const char *directory, lc_error *e) {
  lc_pouch_recode_file *file = &work->files[index], *prior;
  char *map_path = lc_pouch_recode_progress(work, directory, index, "map");
  cr_output out;
  struct stat st, journal;
  lc_u64 first = file->output_floor + 1U, output = 0;
  size_t i;
  int rc = map_path ? LC_OK : LC_ERR_NOMEM;
  memset(&out, 0, sizeof(out));
  out.fd = out.source_fd = -1;
  if (rc != LC_OK)
    goto done;
  for (i = 0; i < index; ++i) {
    prior = &work->files[i];
    if (prior->collection_id[0] && !strcmp(prior->scope, file->scope) &&
        cr_class(prior) == cr_class(file) && prior->output_parts)
      first = (prior->output_first & (lc_u64)LC_I64_MAX) + prior->output_parts;
  }
  first |= cr_class(file);
  if (stat(map_path, &journal) == 0) {
    rc = cr_map(work, file, index, directory, 0, 0, 0, 1, e);
    if (rc == LC_OK && file->output_first != first)
      rc = lc_pouch_recode_journal_error(e);
    if (rc == LC_OK)
      rc = cr_seal_parts(work, file, &output, e);
  } else if (errno != ENOENT)
    rc = cr_error(e, "failed to inspect collection conversion progress");
  else {
    rc = cr_remove_outputs(work, file, first, e);
    if (rc == LC_OK && (stat(file->source, &st) != 0 || st.st_size < 0))
      rc = cr_error(e, "original collection conversion source is missing");
    out.work = work;
    out.file = file;
    out.current = out.first = first;
    if (rc == LC_OK)
      rc = cr_convert(&out, e);
    if (rc == LC_OK) {
      file->output_first = first;
      file->output_parts = out.parts;
      rc = cr_seal_parts(work, file, &output, e);
    }
    if (rc == LC_OK)
      rc = cr_map(work, file, index, directory, (lc_u64)st.st_ino,
                  (lc_u64)st.st_size, output, 0, e);
    if (rc == LC_OK) {
      work->input_bytes += (lc_u64)st.st_size;
      work->output_bytes += output;
      rc = lc_pouch_recode_phase(LC_POUCH_RECODE_SEGMENT_READY, e);
    }
  }
  if (rc == LC_OK && unlink(file->source) != 0 && errno != ENOENT)
    rc = cr_error(e, "failed to retire converted collection source");
  if (rc == LC_OK)
    rc = lc_pouch_recode_phase(LC_POUCH_RECODE_SEGMENT_RENAMED, e);
  if (rc == LC_OK)
    rc = lc_pouch_path_fsync_directory(
        file->scope, "failed to sync collection source retirement", e);
  if (rc == LC_OK)
    rc = lc_pouch_recode_phase(LC_POUCH_RECODE_SEGMENT_INSTALLED, e);
done:
  if (out.fd >= 0)
    (void)close(out.fd);
  if (out.source_fd >= 0)
    (void)close(out.source_fd);
  lc_collection_unit_recode_close(out.codec);
  lc_free_with_allocator(&work->pouch->allocator, out.buffer);
  lc_free_with_allocator(&work->pouch->allocator, map_path);
  return rc;
}
