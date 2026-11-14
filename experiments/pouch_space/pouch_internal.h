#ifndef POUCH_SPACE_POUCH_INTERNAL_H
#define POUCH_SPACE_POUCH_INTERNAL_H
#include "pouch.h"
#include "root.h"
/* Borrowed for an active harness call; NULL for another storage owner. */
ps_root *ps_pouch_state_root(lc_pouch *);
#endif
