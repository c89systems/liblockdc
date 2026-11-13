#include "directory.h"
#include "lc_api_internal.h"

#include <string.h>
#include <zlib.h>

#define DIRECTORY_MAGIC UINT32_C(0x31524944)
#define HEADER 64U
#define SLOT 64U
#define LIMIT ((PS_PAGE - HEADER) / SLOT)
#define MINIMUM (LIMIT / 2U)
#define MAX_LEVEL 15U

uint64_t ps_directory_page_bound(uint64_t entries) {
  uint64_t pages = 0;
  uint64_t nodes = entries / MINIMUM + (entries % MINIMUM != 0);
  while (nodes) {
    pages += nodes;
    nodes = nodes == 1 ? 0 : nodes / MINIMUM + (nodes % MINIMUM != 0);
  }
  return pages;
}

typedef struct directory_node directory_node;
typedef struct directory_item {
  ps_key key;
  ps_directory_value value;
  directory_node *child;
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
static void store64(unsigned char *p, uint64_t value) {
  unsigned int i;
  for (i = 0; i < 8; ++i)
    p[i] = (unsigned char)(value >> (8U * i));
}
static uint64_t load64(const unsigned char *p) {
  uint64_t value = 0;
  unsigned int i;
  for (i = 0; i < 8; ++i)
    value |= (uint64_t)p[i] << (8U * i);
  return value;
}
static void store32(unsigned char *p, uint32_t value) {
  unsigned int i;
  for (i = 0; i < 4; ++i)
    p[i] = (unsigned char)(value >> (8U * i));
}
static uint32_t load32(const unsigned char *p) {
  return (uint32_t)p[0] | (uint32_t)p[1] << 8U | (uint32_t)p[2] << 16U |
         (uint32_t)p[3] << 24U;
}
static uint32_t page_crc(const unsigned char *p) {
  return (uint32_t)crc32(crc32(0, p, 12), p + 16, PS_PAGE - 16);
}
static int key_compare(ps_key a, ps_key b) {
  if (a.kind != b.kind)
    return a.kind < b.kind ? -1 : 1;
  if (a.owner != b.owner)
    return a.owner < b.owner ? -1 : 1;
  return a.key == b.key ? 0 : a.key < b.key ? -1 : 1;
}
static int valid_root(ps_directory_root root) {
  return (!root.offset && !root.generation) ||
         (root.offset && root.offset % PS_PAGE == 0 &&
          root.offset <= (uint64_t)INT64_MAX - PS_PAGE && root.generation);
}
static int valid_value(ps_directory_value value) {
  return value.offset && value.bytes && value.incarnation &&
         value.offset <= INT64_MAX &&
         value.bytes <= (uint64_t)INT64_MAX - value.offset;
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
  rc = d->io.read(d->io.context, ref.offset, page, error);
  if (rc != LC_OK)
    goto done;
  if (load32(page) != DIRECTORY_MAGIC || load32(page + 4) > MAX_LEVEL ||
      !load32(page + 8) || load32(page + 8) > LIMIT ||
      load32(page + 12) != page_crc(page) ||
      load64(page + 16) != ref.generation || load64(page + 24) != ref.offset ||
      memcmp(page + 32, d->io.identity, 16) || load64(page + 48) ||
      load64(page + 56)) {
    rc = directory_error(error, LC_ERR_PROTOCOL,
                         "invalid checkpoint directory page");
    goto done;
  }
  node = new_node(d, load32(page + 4), error);
  if (!node) {
    rc = LC_ERR_NOMEM;
    goto done;
  }
  node->original = ref;
  node->count = load32(page + 8);
  for (i = 0; i < node->count; ++i) {
    item = &node->items[i];
    slot = page + HEADER + SLOT * i;
    item->key.kind = load32(slot);
    item->key.owner = load64(slot + 8);
    item->key.key = load64(slot + 16);
    item->value.offset = load64(slot + 24);
    item->value.bytes = load64(slot + 32);
    item->value.incarnation = load64(slot + 40);
    if (!item->key.kind || load32(slot + 4) || load64(slot + 48) ||
        load64(slot + 56) ||
        (i && key_compare(node->items[i - 1].key, item->key) >= 0) ||
        (node->level
             ? (!item->value.offset || item->value.offset % PS_PAGE ||
                item->value.offset > (uint64_t)INT64_MAX - PS_PAGE ||
                !item->value.bytes || item->value.bytes > ref.generation ||
                item->value.incarnation)
             : !valid_value(item->value))) {
      rc = directory_error(error, LC_ERR_PROTOCOL,
                           "invalid checkpoint directory slot");
      goto done;
    }
  }
  *out = node;
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
  ps_directory_root ref;
  int rc;
  if (!item->child) {
    ref.offset = item->value.offset;
    ref.generation = item->value.bytes;
    rc = read_node(d, ref, &item->child, error);
    if (rc != LC_OK)
      return rc;
  }
  *out = item->child;
  if ((!(*out)->dirty && (*out)->count < MINIMUM) ||
      (*out)->level + 1 != parent->level ||
      key_compare((*out)->items[0].key, item->key) ||
      (index + 1 < parent->count &&
       key_compare((*out)->items[(*out)->count - 1].key,
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
    if (key_compare(node->items[middle].key, key) < 0)
      low = middle + 1;
    else
      high = middle;
  }
  return low;
}
static unsigned int branch_position(directory_node *node, ps_key key) {
  unsigned int index = position(node, key);
  if (index == node->count || key_compare(node->items[index].key, key) > 0)
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
    if (index < node->count && !key_compare(node->items[index].key, key)) {
      *out = node->items[index].value;
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
                    ps_directory_value value, directory_node **split,
                    lc_error *error) {
  directory_node *child, *right;
  directory_item item;
  unsigned int index, half;
  int rc;
  *split = NULL;
  index = node->level ? branch_position(node, key) : position(node, key);
  if (!node->level) {
    if (index < node->count && !key_compare(node->items[index].key, key)) {
      if (memcmp(&node->items[index].value, &value, sizeof(value))) {
        node->items[index].value = value;
        node->dirty = 1;
      }
    } else {
      memset(&item, 0, sizeof(item));
      item.key = key;
      item.value = value;
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
      item.child = right;
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
  if (rc == LC_OK && (!key.kind || !valid_value(value)))
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
    rc = put_node(d, d->root, key, value, &right, error);
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
          root->items[0].child = d->root;
          root->items[1].key = right->items[0].key;
          root->items[1].child = right;
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
    if (index < node->count && !key_compare(node->items[index].key, key)) {
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
    } else {
      if (state->found && key_compare(state->previous, node->items[i].key) >= 0)
        return directory_error(error, LC_ERR_PROTOCOL,
                               "checkpoint directory key ranges overlap");
      state->previous = node->items[i].key;
      state->found = 1;
      rc = state->visit ? state->visit(state->context, node->items[i].key,
                                       node->items[i].value, error)
                        : LC_OK;
    }
    if (rc != LC_OK)
      return rc;
  }
  return LC_OK;
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
  if (rc == LC_OK)
    rc = root_node(d, error);
  if (rc == LC_OK && d->root) {
    memset(&state, 0, sizeof(state));
    state.visit = visit;
    state.page = page;
    state.context = context;
    rc = visit_node(d, d->root, &state, error);
  }
  return finish_operation(d, rc);
}
static int write_node(ps_directory *d, directory_node *node,
                      uint64_t generation, unsigned char *page,
                      lc_error *error) {
  directory_item *item;
  unsigned char *slot;
  unsigned int i;
  uint64_t offset;
  int rc;
  if (!node->dirty) {
    node->written = node->original;
    return LC_OK;
  }
  for (i = 0; i < node->count; ++i) {
    item = &node->items[i];
    if (node->level && item->child) {
      rc = write_node(d, item->child, generation, page, error);
      if (rc != LC_OK)
        return rc;
      item->value.offset = item->child->written.offset;
      item->value.bytes = item->child->written.generation;
    }
  }
  rc = d->io.allocate(d->io.context, &offset, error);
  if (rc != LC_OK)
    return rc;
  node->written.offset = offset;
  node->written.generation = generation;
  d->allocated[d->allocated_count++] = node->written;
  if (!offset || offset % PS_PAGE || offset > (uint64_t)INT64_MAX - PS_PAGE)
    return directory_error(error, LC_ERR_PROTOCOL,
                           "directory allocator returned an unaligned page");
  memset(page, 0, PS_PAGE);
  store32(page, DIRECTORY_MAGIC);
  store32(page + 4, node->level);
  store32(page + 8, node->count);
  store64(page + 16, generation);
  store64(page + 24, offset);
  memcpy(page + 32, d->io.identity, 16);
  for (i = 0; i < node->count; ++i) {
    item = &node->items[i];
    slot = page + HEADER + SLOT * i;
    store32(slot, item->key.kind);
    store64(slot + 8, item->key.owner);
    store64(slot + 16, item->key.key);
    store64(slot + 24, item->value.offset);
    store64(slot + 32, item->value.bytes);
    store64(slot + 40, item->value.incarnation);
  }
  store32(page + 12, page_crc(page));
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
