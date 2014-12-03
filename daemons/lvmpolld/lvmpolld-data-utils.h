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
	PVMOVE = 0,
	CONVERT,
	MERGE,
	MERGE_THIN,

	POLL_TYPE_MAX	/* keep it at tail */
};

typedef struct {
	int ret_code;
	int signal;
} lvmpolld_cmd_stat_t;

typedef struct {
	struct lvmpolld_state *ls;

	enum poll_type type;
	/* full lvid vguuid+lvuuid. may be also vguuid+zeroes -> PVMOVE */
	const char *lvid;
	/* either fullname vg/lv or vgname only */
	const char *name;

	/* pid of polled lvm command */
	pid_t lvmcmd;
	pthread_t tid;
	/* accesed from client threads and polling thread */
	pthread_mutex_t lock;
	/* cond singnaling update of: cmd state, percent or pollin_finished */
	pthread_cond_t cond_update;

	lvmpolld_cmd_stat_t cmd_state;
	dm_percent_t percent;
	unsigned polling_finished:1;

	unsigned interval;
	const char *sinterval;
	unsigned int use_count;

	/* FIXME: read from lvmpolld configuration */
	unsigned debug:1;
	unsigned verbose:1;

	/* unrecoverable error in fork_and_poll code */
	unsigned internal_error:1;
} lvmpolld_lv_t;

/* pdlv structure has use_count == 1 after create */
lvmpolld_lv_t *pdlv_create(struct lvmpolld_state *ls, const char *lvid, const enum poll_type type, const char *sinterval);

/* use count must not reach 0 when structure is inside hash table */
void pdlv_put(lvmpolld_lv_t *pdlv);
void pdlv_get(lvmpolld_lv_t *pdlv);

void pdlv_set_percents(lvmpolld_lv_t *pdlv, dm_percent_t percent);

void pdlv_set_cmd_state(lvmpolld_lv_t *pdlv, lvmpolld_cmd_stat_t *cmd_state);

dm_percent_t pdlv_get_percents(lvmpolld_lv_t *pdlv);

void pdlv_set_internal_error(lvmpolld_lv_t *pdlv, unsigned error);

#endif /* _LVM_LVMPOLLD_DATA_UTILS_H */
