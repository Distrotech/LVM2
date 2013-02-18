#ifndef _LVM_LOCKCACHE_H
#define _LVM_LOCKCACHE_H

#include "locking.h"

int lockcache_vgname_is_locked(const char *vgname);
void lockcache_lock_vgname(const char *vgname, int read_only);
void lockcache_unlock_vgname(const char *vgname);
int lockcache_vgs_locked(void);
int lockcache_verify_lock_order(const char *vgname);
void lockcache_destroy(void);

#endif
