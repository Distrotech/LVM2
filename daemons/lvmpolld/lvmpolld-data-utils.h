/*
 * Copyright (C) 2014 Red Hat, Inc.
 *
 * This file is part of LVM2.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License v.2.1.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

#ifndef _LVM_LVMPOLLD_DATA_UTILS_H
#define _LVM_LVMPOLLD_DATA_UTILS_H

#include <pthread.h>

struct lvmpolld_state;

enum poll_type {
	PVMOVE,
	CONVERT
};

typedef struct {
	enum poll_type type; /* PVMOVE, CONVERT */
	char *vgid;

	pthread_t tid;

	pthread_mutex_t lock; /* accesed from client threads and monitoring threads */

	struct lvmpolld_state *ds;

	unsigned int use_count;
} lvmpolld_vg_t;

/* pdvg structure has use_count == 1 after create */
lvmpolld_vg_t *pdvg_create(struct lvmpolld_state *ls, const char *vgid, const enum poll_type type);

/* use count must not reach 0 when structure is inside hash table */
void pdvg_put(lvmpolld_vg_t *pdvg);
void pdvg_get(lvmpolld_vg_t *pdvg);

#endif /* _LVM_LVMPOLLD_DATA_UTILS_H */
