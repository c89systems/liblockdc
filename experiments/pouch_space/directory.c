#include "directory.h"
#include "catalog.h"
#include "crc.h"
#include "lc_api_internal.h"
#include "lc_intcompat.h"
#include "owned.h"
#include "wire.h"

#include <string.h>
#include <zlib.h>

#define DIRECTORY_MAGIC UINT32_C(0x31524944)
#define HEADER 64U
#define SLOT 64U
#define LIMIT ((PS_PAGE - HEADER) / SLOT)
#define MINIMUM (LIMIT / 2U)
#define MAX_LEVEL 15U
#define SCAN_LEAF_PAGES 8U

uint64_t ps_directory_page_bound(uint64_t entries) {
  uint64_t pages = 0;
  uint64_t nodes = entries / MINIMUM + (entries % MINIMUM != 0);
  while (nodes) {
    pages += nodes;
    nodes = nodes == 1 ? 0 : nodes / MINIMUM + (nodes % MINIMUM != 0);
  }
  return pages;
}
static uint64_t packed_groups(uint64_t count) {
  return count / LIMIT + (count % LIMIT != 0);
}
uint64_t ps_directory_packed_pages(uint64_t entries) {
  uint64_t pages = 0, nodes = packed_groups(entries);
  while (nodes) {
    pages += nodes;
    nodes = nodes == 1 ? 0 : packed_groups(nodes);
  }
  return pages;
}

typedef struct directory_node directory_node;
typedef struct directory_item {
  ps_key key;
  union {
    ps_directory_value value;
    struct {
      ps_directory_root ref;
      directory_node *node;
    } child;
  } binding;
} directory_item;
struct directory_node {
  ps_directory_root original, written;
  unsigned int level, count;
  int dirty;
  directory_item items[LIMIT + 1U];
  directory_node *next;
};
struct ps_directory {
  ps_directory_io io;
  ps_directory_root original;
  directory_node *root, *nodes;
  ps_directory_root *allocated, *retired;
  size_t node_count, allocated_count, retired_count;
  int failed, flushed, root_loaded;
};

static int directory_error(lc_error *e, int rc, const char *message) {
  return lc_error_set(e, rc, 0, message, NULL, NULL, "pouch-directory");
}
static uint32_t page_crc(const unsigned char *p) {
  return (uint32_t)ps_crc32(ps_crc32(0, p, 12), p + 16, PS_PAGE - 16);
}
int ps_directory_key_compare(ps_key a, ps_key b) {
  if (a.kind != b.kind)
    return a.kind < b.kind ? -1 : 1;
  if (a.owner != b.owner)
    return a.owner < b.owner ? -1 : 1;
  return a.key == b.key ? 0 : a.key < b.key ? -1 : 1;
}
static int valid_root(ps_directory_root root) {
  return (!root.offset && !root.generation) ||
         (root.offset && root.offset % PS_PAGE == 0 &&
          root.offset <= (uint64_t)LC_I64_MAX - PS_PAGE && root.generation);
}
static int valid_value(ps_key key, const ps_directory_value *value) {
  const ps_directory_extent *extent = &value->extent;
  const ps_descriptor_ref *ref = &extent->ref;
  if (value->tag.storage == PS_DIRECTORY_NAMESPACE)
    return key.kind == PS_CATALOG && !key.owner && key.key &&
           value->name.length <= PS_DIRECTORY_NAMESPACE_MAX;
  if (value->tag.storage == PS_DIRECTORY_CHECKPOINT)
    return key.kind == PS_NAMESPACE_CHECKPOINT &&
           value->checkpoint.incarnation &&
           value->checkpoint.length <= PS_DIRECTORY_CHECKPOINT_MAX;
  if (value->tag.storage == PS_DIRECTORY_STREAM ||
      value->tag.storage == PS_DIRECTORY_SEALED_STREAM) {
    const ps_directory_stream *stream = &value->stream;
    return key.kind == PS_STREAM && stream->incarnation &&
           stream->tail <= stream->capacity && stream->capacity <= LC_I64_MAX &&
           (value->tag.storage == PS_DIRECTORY_STREAM || !stream->admitted_at);
  }
  if (!ref->offset || !ref->bytes || !ref->incarnation ||
      ref->offset > LC_I64_MAX ||
      ref->bytes > (uint64_t)LC_I64_MAX - ref->offset)
    return 0;
  if (value->tag.storage == PS_DIRECTORY_DESCRIPTOR)
    return !extent->crc && !extent->deletion_bytes;
  if (value->tag.storage == PS_DIRECTORY_PACKED_VALUE)
    return ref->bytes < PS_PAGE && ps_internal_immutable_key(key) &&
           key.kind != PS_NAMESPACE_CHECKPOINT &&
           (key.kind == PS_KEY_METADATA || !extent->deletion_bytes);
  return value->tag.storage == PS_DIRECTORY_OWNED_RUN &&
         key.kind == PS_OWNED_RUN && !extent->deletion_bytes &&
         ref->offset % PS_PAGE == 0 && ref->bytes % PS_PAGE == 0 &&
         ref->bytes <= PS_MIB;
}
static int same_value(const ps_directory_value *a,
                      const ps_directory_value *b) {
  const ps_directory_extent *x = &a->extent, *y = &b->extent;
  if (a->tag.storage != b->tag.storage)
    return 0;
  if (a->tag.storage == PS_DIRECTORY_NAMESPACE) {
    const ps_directory_name *name = &a->name, *other = &b->name;
    return name->length == other->length &&
           !memcmp(name->bytes, other->bytes, name->length);
  }
  if (a->tag.storage == PS_DIRECTORY_CHECKPOINT) {
    const ps_directory_checkpoint *x = &a->checkpoint, *y = &b->checkpoint;
    return x->incarnation == y->incarnation && x->length == y->length &&
           !memcmp(x->bytes, y->bytes, x->length);
  }
  if (a->tag.storage == PS_DIRECTORY_STREAM ||
      a->tag.storage == PS_DIRECTORY_SEALED_STREAM) {
    const ps_directory_stream *head = &a->stream;
    const ps_directory_stream *other = &b->stream;
    return head->tail == other->tail && head->capacity == other->capacity &&
           head->incarnation == other->incarnation &&
           head->runs == other->runs && head->admitted_at == other->admitted_at;
  }
  return x->ref.offset == y->ref.offset && x->ref.bytes == y->ref.bytes &&
         x->ref.incarnation == y->ref.incarnation &&
         x->deletion_bytes == y->deletion_bytes && x->crc == y->crc;
}
static int available(ps_directory *d, lc_error *error) {
  return d && !d->failed && !d->flushed
             ? LC_OK
             : directory_error(error, LC_ERR_INVALID,
                               "checkpoint directory is failed or finalized");
}
static directory_node *new_node(ps_directory *d, unsigned int level,
                                lc_error *error) {
  directory_node *node = lc_calloc_with_allocator(NULL, 1, sizeof(*node));
  if (!node) {
    directory_error(error, LC_ERR_NOMEM, "cannot allocate directory page");
    return NULL;
  }
  node->level = level;
  node->next = d->nodes;
  d->nodes = node;
  ++d->node_count;
  return node;
}
static int check_page(ps_directory *d, ps_directory_root ref,
                      const unsigned char *page, lc_error *error) {
  if (ps_wire_get32(page) != DIRECTORY_MAGIC ||
      ps_wire_get32(page + 4) > MAX_LEVEL || !ps_wire_get32(page + 8) ||
      ps_wire_get32(page + 8) > LIMIT ||
      ps_wire_get32(page + 12) != page_crc(page) ||
      ps_wire_get64(page + 16) != ref.generation ||
      ps_wire_get64(page + 24) != ref.offset ||
      memcmp(page + 32, d->io.identity, 16) || ps_wire_get64(page + 48) ||
      ps_wire_get64(page + 56))
    return directory_error(error, LC_ERR_PROTOCOL,
                           "invalid checkpoint directory page");
  return LC_OK;
}
static int read_page(ps_directory *d, ps_directory_root ref,
                     unsigned char *page, lc_error *error) {
  int rc = d->io.read(d->io.context, ref.offset, page, PS_PAGE, error);
  return rc == LC_OK ? check_page(d, ref, page, error) : rc;
}
static ps_key slot_key(const unsigned char *slot) {
  ps_key key;
  key.owner = ps_wire_get64(slot + 8);
  key.key = ps_wire_get64(slot + 16);
  key.kind = ps_wire_get32(slot);
  return key;
}
static int slot_child(const unsigned char *slot, ps_directory_root parent,
                      ps_directory_root *child) {
  child->offset = ps_wire_get64(slot + 24);
  child->generation = ps_wire_get64(slot + 32);
  return child->offset && valid_root(*child) &&
         child->generation <= parent.generation && !ps_wire_get32(slot + 4) &&
         !ps_wire_get64(slot + 40) && !ps_wire_get64(slot + 48) &&
         !ps_wire_get64(slot + 56);
}
static int slot_value(const unsigned char *slot, ps_key key,
                      ps_directory_value *value) {
  size_t i;
  uint32_t storage = ps_wire_get32(slot + 4);
  ps_directory_extent *extent = &value->extent;
  if (storage == PS_DIRECTORY_NAMESPACE) {
    if (slot[24] > PS_DIRECTORY_NAMESPACE_MAX)
      return 0;
    value->name.storage = storage;
    value->name.length = slot[24];
    for (i = 25 + value->name.length; i < SLOT; ++i)
      if (slot[i])
        return 0;
    memcpy(value->name.bytes, slot + 25, PS_DIRECTORY_NAMESPACE_MAX);
  } else if (storage == PS_DIRECTORY_CHECKPOINT) {
    ps_directory_checkpoint *checkpoint = &value->checkpoint;
    checkpoint->storage = storage;
    checkpoint->incarnation = ps_wire_get64(slot + 24);
    checkpoint->length = slot[32];
    if (checkpoint->length > PS_DIRECTORY_CHECKPOINT_MAX)
      return 0;
    for (i = 33 + checkpoint->length; i < SLOT; ++i)
      if (slot[i])
        return 0;
    memcpy(checkpoint->bytes, slot + 33, PS_DIRECTORY_CHECKPOINT_MAX);
  } else if (storage == PS_DIRECTORY_STREAM ||
             storage == PS_DIRECTORY_SEALED_STREAM) {
    ps_directory_stream *stream = &value->stream;
    if (ps_wire_get32(slot + 52))
      return 0;
    stream->storage = storage;
    stream->tail = ps_wire_get64(slot + 24);
    stream->capacity = ps_wire_get64(slot + 32);
    stream->incarnation = ps_wire_get64(slot + 40);
    stream->runs = ps_wire_get32(slot + 48);
    stream->admitted_at = ps_wire_get64(slot + 56);
  } else {
    if (ps_wire_get32(slot + 52))
      return 0;
    extent->storage = storage;
    extent->ref.offset = ps_wire_get64(slot + 24);
    extent->ref.bytes = ps_wire_get64(slot + 32);
    extent->ref.incarnation = ps_wire_get64(slot + 40);
    extent->crc = ps_wire_get32(slot + 48);
    extent->deletion_bytes = ps_wire_get64(slot + 56);
  }
  return valid_value(key, value);
}
static int read_node(ps_directory *d, ps_directory_root ref,
                     directory_node **out, lc_error *error) {
  unsigned char *page;
  directory_node *node;
  directory_item *item;
  const unsigned char *slot;
  unsigned int i;
  int rc;
  page = lc_alloc_with_allocator(NULL, PS_PAGE);
  if (!page)
    return directory_error(error, LC_ERR_NOMEM,
                           "cannot allocate directory read buffer");
  rc = read_page(d, ref, page, error);
  if (rc != LC_OK)
    goto done;
  node = new_node(d, ps_wire_get32(page + 4), error);
  if (!node) {
    rc = LC_ERR_NOMEM;
    goto done;
  }
  node->original = ref;
  node->count = ps_wire_get32(page + 8);
  for (i = 0; i < node->count; ++i) {
    item = &node->items[i];
    slot = page + HEADER + SLOT * i;
    item->key = slot_key(slot);
    if (!item->key.kind ||
        (i && ps_directory_key_compare(node->items[i - 1].key, item->key) >= 0))
      goto invalid_slot;
    if (node->level) {
      if (!slot_child(slot, ref, &item->binding.child.ref))
        goto invalid_slot;
    } else if (!slot_value(slot, item->key, &item->binding.value))
      goto invalid_slot;
  }
  *out = node;
  goto done;
invalid_slot:
  rc = directory_error(error, LC_ERR_PROTOCOL,
                       "invalid checkpoint directory slot");
done:
  lc_free_with_allocator(NULL, page);
  return rc;
}
static int root_node(ps_directory *d, lc_error *error) {
  int rc;
  if (d->root_loaded)
    return LC_OK;
  rc = read_node(d, d->original, &d->root, error);
  if (rc == LC_OK && d->root->level && d->root->count < 2)
    rc = directory_error(error, LC_ERR_PROTOCOL,
                         "checkpoint directory root was not collapsed");
  if (rc == LC_OK)
    d->root_loaded = 1;
  return rc;
}
static int child_node(ps_directory *d, directory_node *parent,
                      unsigned int index, directory_node **out,
                      lc_error *error) {
  directory_item *item = &parent->items[index];
  int rc;
  if (!item->binding.child.node) {
    rc =
        read_node(d, item->binding.child.ref, &item->binding.child.node, error);
    if (rc != LC_OK)
      return rc;
  }
  *out = item->binding.child.node;
  if ((!(*out)->dirty && (*out)->count < MINIMUM) ||
      (*out)->level + 1 != parent->level ||
      ps_directory_key_compare((*out)->items[0].key, item->key) ||
      (index + 1 < parent->count &&
       ps_directory_key_compare((*out)->items[(*out)->count - 1].key,
                                parent->items[index + 1].key) >= 0))
    return directory_error(error, LC_ERR_PROTOCOL,
                           "checkpoint directory child binding mismatch");
  return LC_OK;
}
/* Lower-bound search: index pages contain each child's minimum key. */
static unsigned int position(directory_node *node, ps_key key) {
  unsigned int low = 0, high = node->count, middle;
  while (low < high) {
    middle = low + (high - low) / 2;
    if (ps_directory_key_compare(node->items[middle].key, key) < 0)
      low = middle + 1;
    else
      high = middle;
  }
  return low;
}
static unsigned int branch_position(directory_node *node, ps_key key) {
  unsigned int index = position(node, key);
  if (index == node->count ||
      ps_directory_key_compare(node->items[index].key, key) > 0)
    return index ? index - 1 : 0;
  return index;
}
static int finish_operation(ps_directory *d, int rc) {
  if (d && rc != LC_OK)
    d->failed = 1;
  return rc;
}
int ps_directory_open(const ps_directory_io *io, ps_directory_root root,
                      ps_directory **out, lc_error *error) {
  ps_directory *d;
  if (out)
    *out = NULL;
  if (!out || !io || !io->read || !io->write || !io->allocate ||
      !valid_root(root))
    return directory_error(error, LC_ERR_INVALID,
                           "directory requires page I/O and a valid root");
  d = lc_calloc_with_allocator(NULL, 1, sizeof(*d));
  if (!d)
    return directory_error(error, LC_ERR_NOMEM,
                           "cannot allocate checkpoint directory");
  d->io = *io;
  d->original = root;
  d->root_loaded = !root.offset;
  *out = d;
  return LC_OK;
}
typedef struct packed_level {
  uint64_t remaining, pages;
} packed_level;
typedef struct packed_directory {
  packed_level levels[MAX_LEVEL + 1];
  ps_directory_next_fn next;
  void *context;
  ps_key previous;
} packed_directory;
static int build_node(ps_directory *d, packed_directory *plan,
                      unsigned int level, directory_node **out,
                      lc_error *error) {
  packed_level *steps = &plan->levels[level];
  directory_node *node = new_node(d, level, error);
  directory_item *item;
  unsigned int i;
  int rc = LC_OK;
  if (!node)
    return LC_ERR_NOMEM;
  /* Balance the final groups too. Splitting after a maximal prefix could
   * leave an underfull last page that the ordinary reader cannot adopt. */
  node->count = (unsigned int)(steps->remaining / steps->pages +
                               (steps->remaining % steps->pages != 0));
  steps->remaining -= node->count;
  --steps->pages;
  node->dirty = 1;
  for (i = 0; rc == LC_OK && i < node->count; ++i) {
    item = &node->items[i];
    if (level) {
      rc = build_node(d, plan, level - 1, &item->binding.child.node, error);
      if (rc == LC_OK)
        item->key = item->binding.child.node->items[0].key;
    } else {
      rc = plan->next(plan->context, &item->key, &item->binding.value, error);
      if (rc == LC_OK &&
          (!item->key.kind || !valid_value(item->key, &item->binding.value) ||
           ps_directory_key_compare(plan->previous, item->key) >= 0))
        rc = directory_error(error, LC_ERR_PROTOCOL,
                             "packed directory input is not ordered");
      if (rc == LC_OK)
        plan->previous = item->key;
    }
  }
  if (rc == LC_OK)
    *out = node;
  return rc;
}
int ps_directory_build(ps_directory *d, uint64_t count,
                       ps_directory_next_fn next, void *context,
                       lc_error *error) {
  packed_directory plan;
  unsigned int level = 0;
  int rc = available(d, error);
  if (rc == LC_OK && (!next || d->original.offset || d->node_count))
    rc = directory_error(error, LC_ERR_INVALID,
                         "packed directory requires an empty tree and input");
  if (rc != LC_OK || !count)
    return finish_operation(d, rc);
  memset(&plan, 0, sizeof(plan));
  plan.next = next;
  plan.context = context;
  for (;;) {
    plan.levels[level].remaining = count;
    plan.levels[level].pages = count = packed_groups(count);
    if (count == 1)
      break;
    if (level == MAX_LEVEL)
      return finish_operation(d, directory_error(error, LC_ERR_INVALID,
                                                 "packed directory too deep"));
    ++level;
  }
  return finish_operation(d, build_node(d, &plan, level, &d->root, error));
}
int ps_directory_get(ps_directory *d, ps_key key, ps_directory_value *out,
                     int *found, lc_error *error) {
  directory_node *node;
  unsigned int index;
  int rc = available(d, error);
  if (found)
    *found = 0;
  if (rc == LC_OK && (!key.kind || !out || !found))
    rc = directory_error(error, LC_ERR_INVALID,
                         "directory lookup requires typed key and outputs");
  if (rc == LC_OK)
    rc = root_node(d, error);
  node = d ? d->root : NULL;
  while (rc == LC_OK && node && node->level)
    rc = child_node(d, node, branch_position(node, key), &node, error);
  if (rc == LC_OK && node) {
    index = position(node, key);
    if (index < node->count &&
        !ps_directory_key_compare(node->items[index].key, key)) {
      *out = node->items[index].binding.value;
      *found = 1;
    }
  }
  return finish_operation(d, rc);
}
static void insert_item(directory_node *node, unsigned int index,
                        const directory_item *item) {
  memmove(node->items + index + 1, node->items + index,
          (node->count - index) * sizeof(*node->items));
  node->items[index] = *item;
  ++node->count;
  node->dirty = 1;
}
static int put_node(ps_directory *d, directory_node *node, ps_key key,
                    const ps_directory_value *value, directory_node **split,
                    lc_error *error) {
  directory_node *child, *right;
  directory_item item;
  unsigned int index, half;
  int rc;
  *split = NULL;
  index = node->level ? branch_position(node, key) : position(node, key);
  if (!node->level) {
    if (index < node->count &&
        !ps_directory_key_compare(node->items[index].key, key)) {
      if (!same_value(&node->items[index].binding.value, value)) {
        node->items[index].binding.value = *value;
        node->dirty = 1;
      }
    } else {
      memset(&item, 0, sizeof(item));
      item.key = key;
      item.binding.value = *value;
      insert_item(node, index, &item);
    }
  } else {
    rc = child_node(d, node, index, &child, error);
    if (rc != LC_OK)
      return rc;
    rc = put_node(d, child, key, value, &right, error);
    if (rc != LC_OK)
      return rc;
    if (child->dirty) {
      node->items[index].key = child->items[0].key;
      node->dirty = 1;
    }
    if (right) {
      memset(&item, 0, sizeof(item));
      item.key = right->items[0].key;
      item.binding.child.node = right;
      insert_item(node, index + 1, &item);
    }
  }
  if (node->count > LIMIT) {
    right = new_node(d, node->level, error);
    if (!right)
      return LC_ERR_NOMEM;
    half = node->count / 2;
    right->count = node->count - half;
    memcpy(right->items, node->items + half,
           right->count * sizeof(*right->items));
    memset(node->items + half, 0, right->count * sizeof(*node->items));
    node->count = half;
    node->dirty = right->dirty = 1;
    *split = right;
  }
  return LC_OK;
}
int ps_directory_put(ps_directory *d, ps_key key, ps_directory_value value,
                     lc_error *error) {
  directory_node *right, *root;
  int rc = available(d, error);
  if (rc == LC_OK && (!key.kind || !valid_value(key, &value)))
    rc = directory_error(error, LC_ERR_INVALID,
                         "directory requires typed key and checked value span");
  if (rc == LC_OK)
    rc = root_node(d, error);
  if (rc == LC_OK && !d->root) {
    d->root = new_node(d, 0, error);
    if (!d->root)
      rc = LC_ERR_NOMEM;
  }
  if (rc == LC_OK) {
    rc = put_node(d, d->root, key, &value, &right, error);
    if (rc == LC_OK && right) {
      if (d->root->level == MAX_LEVEL)
        rc = directory_error(error, LC_ERR_INVALID,
                             "checkpoint directory height exhausted");
      else {
        root = new_node(d, d->root->level + 1, error);
        if (!root)
          rc = LC_ERR_NOMEM;
        else {
          root->count = 2;
          root->dirty = 1;
          root->items[0].key = d->root->items[0].key;
          root->items[0].binding.child.node = d->root;
          root->items[1].key = right->items[0].key;
          root->items[1].binding.child.node = right;
          d->root = root;
        }
      }
    }
  }
  return finish_operation(d, rc);
}
static void remove_item(directory_node *node, unsigned int index) {
  --node->count;
  memmove(node->items + index, node->items + index + 1,
          (node->count - index) * sizeof(*node->items));
  memset(node->items + node->count, 0, sizeof(*node->items));
  node->dirty = 1;
}
static int rebalance(ps_directory *d, directory_node *parent,
                     unsigned int index, lc_error *error) {
  directory_node *left, *right;
  unsigned int first = index ? index - 1 : 0, total, half, move;
  int rc;
  if (parent->count < 2)
    return LC_OK;
  rc = child_node(d, parent, first, &left, error);
  if (rc == LC_OK)
    rc = child_node(d, parent, first + 1, &right, error);
  if (rc != LC_OK)
    return rc;
  total = left->count + right->count;
  left->dirty = right->dirty = parent->dirty = 1;
  if (total <= LIMIT) {
    memcpy(left->items + left->count, right->items,
           right->count * sizeof(*right->items));
    left->count = total;
    parent->items[first].key = left->items[0].key;
    remove_item(parent, first + 1);
  } else {
    half = total / 2;
    if (left->count < half) {
      move = half - left->count;
      memcpy(left->items + left->count, right->items,
             move * sizeof(*left->items));
      memmove(right->items, right->items + move,
              (right->count - move) * sizeof(*right->items));
      right->count -= move;
    } else {
      move = left->count - half;
      memmove(right->items + move, right->items,
              right->count * sizeof(*right->items));
      memcpy(right->items, left->items + half, move * sizeof(*right->items));
      right->count += move;
    }
    left->count = half;
    memset(left->items + left->count, 0,
           (LIMIT + 1 - left->count) * sizeof(*left->items));
    memset(right->items + right->count, 0,
           (LIMIT + 1 - right->count) * sizeof(*right->items));
    parent->items[first].key = left->items[0].key;
    parent->items[first + 1].key = right->items[0].key;
  }
  return LC_OK;
}
static int remove_node(ps_directory *d, directory_node *node, ps_key key,
                       int *removed, lc_error *error) {
  directory_node *child;
  unsigned int index;
  int rc;
  index = node->level ? branch_position(node, key) : position(node, key);
  if (!node->level) {
    if (index < node->count &&
        !ps_directory_key_compare(node->items[index].key, key)) {
      remove_item(node, index);
      *removed = 1;
    }
    return LC_OK;
  }
  rc = child_node(d, node, index, &child, error);
  if (rc == LC_OK)
    rc = remove_node(d, child, key, removed, error);
  if (rc != LC_OK || !*removed)
    return rc;
  node->dirty = 1;
  if (child->count)
    node->items[index].key = child->items[0].key;
  if (child->count < MINIMUM)
    return rebalance(d, node, index, error);
  return LC_OK;
}
int ps_directory_remove(ps_directory *d, ps_key key, int *removed,
                        lc_error *error) {
  directory_node *child;
  int rc = available(d, error);
  if (removed)
    *removed = 0;
  if (rc == LC_OK && (!key.kind || !removed))
    rc = directory_error(error, LC_ERR_INVALID,
                         "directory removal requires typed key and output");
  if (rc == LC_OK)
    rc = root_node(d, error);
  if (rc == LC_OK && d->root)
    rc = remove_node(d, d->root, key, removed, error);
  while (rc == LC_OK && d->root && d->root->level && d->root->count == 1) {
    rc = child_node(d, d->root, 0, &child, error);
    if (rc == LC_OK) {
      d->root->dirty = 1;
      d->root = child;
    }
  }
  if (rc == LC_OK && d->root && !d->root->count) {
    d->root->dirty = 1;
    d->root = NULL;
  }
  return finish_operation(d, rc);
}
typedef struct visit_state {
  ps_directory_visit_fn visit;
  ps_directory_page_fn page;
  void *context;
  ps_key previous;
  int found;
} visit_state;
static int visit_binding(visit_state *state, ps_key key,
                         const ps_directory_value *value, lc_error *error) {
  if (state->found && ps_directory_key_compare(state->previous, key) >= 0)
    return directory_error(error, LC_ERR_PROTOCOL,
                           "checkpoint directory key ranges overlap");
  state->previous = key;
  state->found = 1;
  return state->visit ? state->visit(state->context, key, value, error) : LC_OK;
}
static int visit_node(ps_directory *d, directory_node *node, visit_state *state,
                      lc_error *error) {
  directory_node *child;
  unsigned int i;
  int rc;
  if (state->page) {
    rc = state->page(state->context, node->original, error);
    if (rc != LC_OK)
      return rc;
  }
  for (i = 0; i < node->count; ++i) {
    if (node->level) {
      rc = child_node(d, node, i, &child, error);
      if (rc == LC_OK)
        rc = visit_node(d, child, state, error);
    } else
      rc = visit_binding(state, node->items[i].key,
                         &node->items[i].binding.value, error);
    if (rc != LC_OK)
      return rc;
  }
  return LC_OK;
}
static int scan_page(ps_directory *d, ps_directory_root ref,
                     const unsigned char *page, unsigned char *work,
                     unsigned int parent_level, ps_key lower, ps_key upper,
                     visit_state *state, lc_error *error);
static int scan_children(ps_directory *d, ps_directory_root ref,
                         const unsigned char *page, unsigned char *work,
                         unsigned int level, unsigned int count, ps_key upper,
                         visit_state *state, lc_error *error) {
  ps_directory_root children[SCAN_LEAF_PAGES];
  ps_key previous = {0}, key, next;
  const unsigned char *slot;
  unsigned int first, i, batch;
  int rc;
  for (first = 0; first < count; first += batch) {
    /* Read only siblings explicitly referenced by the parent. Keep branch
     * storage to one page per level and reserve the batch window for leaves. */
    batch = 1;
    slot = page + HEADER + first * SLOT;
    if (level == 1) {
      uint64_t offset = ps_wire_get64(slot + 24);
      while (batch < SCAN_LEAF_PAGES && batch < count - first &&
             offset <= (uint64_t)LC_I64_MAX - batch * PS_PAGE &&
             ps_wire_get64(slot + batch * SLOT + 24) ==
                 offset + batch * PS_PAGE)
        ++batch;
    }
    for (i = 0; i < batch; ++i, slot += SLOT) {
      key = slot_key(slot);
      if (!key.kind || ps_directory_key_compare(previous, key) >= 0 ||
          !slot_child(slot, ref, &children[i]))
        return directory_error(error, LC_ERR_PROTOCOL,
                               "invalid checkpoint directory slot");
      previous = key;
    }
    rc = d->io.read(d->io.context, children[0].offset, work, batch * PS_PAGE,
                    error);
    for (i = 0; rc == LC_OK && i < batch; ++i) {
      const unsigned char *child = work + i * PS_PAGE;
      slot = page + HEADER + (first + i) * SLOT;
      next = first + i + 1 < count ? slot_key(slot + SLOT) : upper;
      rc = check_page(d, children[i], child, error);
      if (rc == LC_OK)
        rc =
            scan_page(d, children[i], child, level == 1 ? NULL : work + PS_PAGE,
                      level, slot_key(slot), next, state, error);
    }
    if (rc != LC_OK)
      return rc;
  }
  return LC_OK;
}
/* Branch pages stay resident only while walking their children. Each leaf
 * still supplies independent framing, checksum and ordered key checks. */
static int scan_page(ps_directory *d, ps_directory_root ref,
                     const unsigned char *page, unsigned char *work,
                     unsigned int parent_level, ps_key lower, ps_key upper,
                     visit_state *state, lc_error *error) {
  ps_directory_value value;
  ps_key key;
  const unsigned char *slot;
  unsigned int level = ps_wire_get32(page + 4);
  unsigned int count = ps_wire_get32(page + 8), i;
  int rc;
  if (parent_level) {
    if (count < MINIMUM || level + 1 != parent_level ||
        ps_directory_key_compare(slot_key(page + HEADER), lower) ||
        (upper.kind &&
         ps_directory_key_compare(slot_key(page + HEADER + (count - 1) * SLOT),
                                  upper) >= 0))
      return directory_error(error, LC_ERR_PROTOCOL,
                             "checkpoint directory child binding mismatch");
  } else if (level && count < 2)
    return directory_error(error, LC_ERR_PROTOCOL,
                           "checkpoint directory root was not collapsed");
  if (state->page) {
    rc = state->page(state->context, ref, error);
    if (rc != LC_OK)
      return rc;
  }
  if (level)
    return scan_children(d, ref, page, work, level, count, upper, state, error);
  for (i = 0; i < count; ++i) {
    slot = page + HEADER + i * SLOT;
    key = slot_key(slot);
    if (!key.kind || !slot_value(slot, key, &value))
      break;
    rc = visit_binding(state, key, &value, error);
    if (rc != LC_OK)
      return rc;
  }
  return i == count ? LC_OK
                    : directory_error(error, LC_ERR_PROTOCOL,
                                      "invalid checkpoint directory slot");
}
static int scan_directory(ps_directory *d, visit_state *state,
                          lc_error *error) {
  unsigned char root[PS_PAGE], *work = NULL;
  ps_key unbounded = {0};
  unsigned int level;
  int rc = read_page(d, d->original, root, error);
  if (rc != LC_OK)
    return rc;
  level = ps_wire_get32(root + 4);
  if (level) {
    work =
        lc_alloc_with_allocator(NULL, (level - 1 + SCAN_LEAF_PAGES) * PS_PAGE);
    if (!work)
      return directory_error(error, LC_ERR_NOMEM,
                             "cannot allocate directory scan pages");
  }
  rc = scan_page(d, d->original, root, work, 0, unbounded, unbounded, state,
                 error);
  lc_free_with_allocator(NULL, work);
  return rc;
}
int ps_directory_visit(ps_directory *d, ps_directory_visit_fn visit,
                       ps_directory_page_fn page, void *context,
                       lc_error *error) {
  visit_state state;
  directory_node *node;
  int rc = available(d, error);
  if (rc == LC_OK && !visit && !page)
    rc = directory_error(error, LC_ERR_INVALID, "directory visitor required");
  if (rc == LC_OK && page)
    for (node = d->nodes; node; node = node->next)
      if (node->dirty) {
        rc = directory_error(
            error, LC_ERR_INVALID,
            "page ownership walk requires unmodified directory");
        break;
      }
  if (rc == LC_OK) {
    memset(&state, 0, sizeof(state));
    state.visit = visit;
    state.page = page;
    state.context = context;
    if (!d->root_loaded)
      rc = scan_directory(d, &state, error);
    else if (d->root)
      rc = visit_node(d, d->root, &state, error);
  }
  return finish_operation(d, rc);
}
static int write_node(ps_directory *d, directory_node *node,
                      uint64_t generation, unsigned char *page,
                      lc_error *error) {
  directory_item *item;
  const ps_directory_value *value;
  directory_node *child;
  unsigned char *slot;
  unsigned int i;
  uint64_t offset;
  int rc;
  if (!node->dirty) {
    node->written = node->original;
    return LC_OK;
  }
  if (node->level)
    for (i = 0; i < node->count; ++i) {
      item = &node->items[i];
      child = item->binding.child.node;
      if (child) {
        rc = write_node(d, child, generation, page, error);
        if (rc != LC_OK)
          return rc;
        item->binding.child.ref = child->written;
      }
    }
  rc = d->io.allocate(d->io.context, &offset, error);
  if (rc != LC_OK)
    return rc;
  node->written.offset = offset;
  node->written.generation = generation;
  d->allocated[d->allocated_count++] = node->written;
  if (!offset || offset % PS_PAGE || offset > (uint64_t)LC_I64_MAX - PS_PAGE)
    return directory_error(error, LC_ERR_PROTOCOL,
                           "directory allocator returned an unaligned page");
  memset(page, 0, PS_PAGE);
  ps_wire_put32(page, DIRECTORY_MAGIC);
  ps_wire_put32(page + 4, node->level);
  ps_wire_put32(page + 8, node->count);
  ps_wire_put64(page + 16, generation);
  ps_wire_put64(page + 24, offset);
  memcpy(page + 32, d->io.identity, 16);
  for (i = 0; i < node->count; ++i) {
    item = &node->items[i];
    slot = page + HEADER + SLOT * i;
    ps_wire_put32(slot, item->key.kind);
    ps_wire_put64(slot + 8, item->key.owner);
    ps_wire_put64(slot + 16, item->key.key);
    if (node->level) {
      ps_wire_put64(slot + 24, item->binding.child.ref.offset);
      ps_wire_put64(slot + 32, item->binding.child.ref.generation);
    } else {
      value = &item->binding.value;
      ps_wire_put32(slot + 4, value->tag.storage);
      if (value->tag.storage == PS_DIRECTORY_NAMESPACE) {
        const ps_directory_name *name = &value->name;
        slot[24] = name->length;
        memcpy(slot + 25, name->bytes, name->length);
      } else if (value->tag.storage == PS_DIRECTORY_CHECKPOINT) {
        const ps_directory_checkpoint *checkpoint = &value->checkpoint;
        ps_wire_put64(slot + 24, checkpoint->incarnation);
        slot[32] = (unsigned char)checkpoint->length;
        memcpy(slot + 33, checkpoint->bytes, checkpoint->length);
      } else if (value->tag.storage == PS_DIRECTORY_STREAM ||
                 value->tag.storage == PS_DIRECTORY_SEALED_STREAM) {
        const ps_directory_stream *stream = &value->stream;
        ps_wire_put64(slot + 24, stream->tail);
        ps_wire_put64(slot + 32, stream->capacity);
        ps_wire_put64(slot + 40, stream->incarnation);
        ps_wire_put32(slot + 48, stream->runs);
        ps_wire_put64(slot + 56, stream->admitted_at);
      } else {
        const ps_directory_extent *extent = &value->extent;
        ps_wire_put64(slot + 24, extent->ref.offset);
        ps_wire_put64(slot + 32, extent->ref.bytes);
        ps_wire_put64(slot + 40, extent->ref.incarnation);
        ps_wire_put32(slot + 48, extent->crc);
        ps_wire_put64(slot + 56, extent->deletion_bytes);
      }
    }
  }
  ps_wire_put32(page + 12, page_crc(page));
  return d->io.write(d->io.context, offset, page, error);
}
int ps_directory_flush(ps_directory *d, uint64_t generation,
                       ps_directory_root *out, lc_error *error) {
  directory_node *node;
  unsigned char *page = NULL;
  int rc = available(d, error);
  if (out)
    memset(out, 0, sizeof(*out));
  if (rc == LC_OK &&
      (!out || !generation || generation <= d->original.generation))
    rc = directory_error(error, LC_ERR_INVALID,
                         "directory flush requires a newer generation");
  if (rc == LC_OK)
    rc = root_node(d, error);
  if (rc == LC_OK && d->node_count) {
    d->allocated =
        lc_calloc_with_allocator(NULL, d->node_count, sizeof(*d->allocated));
    d->retired =
        lc_calloc_with_allocator(NULL, d->node_count, sizeof(*d->retired));
    page = lc_alloc_with_allocator(NULL, PS_PAGE);
    if (!d->allocated || !d->retired || !page)
      rc = directory_error(error, LC_ERR_NOMEM,
                           "cannot allocate directory flush ownership");
  }
  if (rc == LC_OK) {
    for (node = d->nodes; node; node = node->next)
      if (node->dirty && node->original.offset)
        d->retired[d->retired_count++] = node->original;
    if (d->root) {
      rc = write_node(d, d->root, generation, page, error);
      if (rc == LC_OK)
        *out = d->root->written;
    }
  }
  lc_free_with_allocator(NULL, page);
  if (rc == LC_OK)
    d->flushed = 1;
  return finish_operation(d, rc);
}
void ps_directory_changed_pages(const ps_directory *d,
                                ps_directory_pages *out) {
  if (!out)
    return;
  memset(out, 0, sizeof(*out));
  if (d) {
    out->allocated = d->allocated;
    out->allocated_count = d->allocated_count;
    out->retired = d->retired;
    out->retired_count = d->retired_count;
  }
}
void ps_directory_close(ps_directory *d) {
  directory_node *node;
  if (!d)
    return;
  while (d->nodes) {
    node = d->nodes;
    d->nodes = node->next;
    lc_free_with_allocator(NULL, node);
  }
  lc_free_with_allocator(NULL, d->allocated);
  lc_free_with_allocator(NULL, d->retired);
  lc_free_with_allocator(NULL, d);
}
