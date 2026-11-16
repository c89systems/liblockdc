#ifndef POUCH_SPACE_POUCH_INTERNAL_H
#define POUCH_SPACE_POUCH_INTERNAL_H
#include "pouch.h"
#include "root.h"
/* Borrowed for an active harness call; NULL for another storage owner. */
ps_root *ps_pouch_state_root(lc_pouch *);
int ps_pouch_state_maintain(lc_pouch *, const lc_pouch_maintenance_options *,
                            lc_pouch_maintenance_result *, lc_error *);
#endif
