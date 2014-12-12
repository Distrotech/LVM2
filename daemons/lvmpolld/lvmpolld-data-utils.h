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

typedef struct lvmpolld_state lvmpolld_state_t;

/* replace with configuration option */
#define PDTIMEOUT_DEF 60

enum poll_type {
	PVMOVE = 0,
	CONVERT,
	MERGE,
	MERGE_THIN,
	POLL_TYPE_MAX
};

typedef struct lvmpolld_lv lvmpolld_lv_t;

typedef struct {
	pthread_mutex_t lock;
	void *store;
} lvmpolld_store_t;

typedef struct {
	int ret_code;
	int signal;
} lvmpolld_cmd_stat_t;

void pdst_init(lvmpolld_store_t *pdst);
void pdst_destroy(lvmpolld_store_t *pdst);

typedef void (*lvmpolld_parse_output_fn_t) (lvmpolld_lv_t *pdlv, const char *line);

typedef struct lvmpolld_lv {
	/*
	 * accessing following vars doesn't
	 * require lvmpolld_lv_t lock
	 */
	struct lvmpolld_state *const ls;
	const enum poll_type type;
	/* full lvid vguuid+lvuuid. may also be vguuid+zeroes -> PVMOVE */
	const char *const lvid;
	/* either fullname vg/lv or vgname only */
	/* const char *name; */
	const unsigned pdtimeout; /* in seconds */
	const char *const sinterval;
	lvmpolld_store_t *const pdst;
	lvmpolld_parse_output_fn_t parse_output_fn;

	/* pid of polled lvm command */
	pid_t lvmcmd;
	pthread_t tid;

	/*
	 * cond_update singnals update of one
	 * or more variable in following block
	 */
	pthread_mutex_t lock;
	pthread_cond_t cond_update;

	/* block of shared variables */
	unsigned int use_count;
	lvmpolld_cmd_stat_t cmd_state;
	dm_percent_t percent;
	unsigned polling_finished:1; /* no more update */
	unsigned internal_error:1; /* unrecoverable error in lvmpolld */

	/* TODO: read from lvmpolld configuration and change to const */
	unsigned debug:1; /* may be read w/o lock */
	unsigned verbose:1; /* may be read w/o lock */
	const unsigned abort:1; /* may be read w/o lock */
} lvmpolld_lv_t;

static inline void pdst_lock(lvmpolld_store_t *pdst)
{
	pthread_mutex_lock(&pdst->lock);
}

static inline void pdst_unlock(lvmpolld_store_t *pdst)
{
	pthread_mutex_unlock(&pdst->lock);
}

static inline void pdlv_lock(lvmpolld_lv_t *pdlv)
{
	pthread_mutex_lock(&pdlv->lock);
}

static inline void pdlv_unlock(lvmpolld_lv_t *pdlv)
{
	pthread_mutex_unlock(&pdlv->lock);
}

static inline int pdlv_locked_await_update(lvmpolld_lv_t *pdlv)
{
	return !pthread_cond_wait(&pdlv->cond_update, &pdlv->lock);
}

static inline int pdlv_is_type(const lvmpolld_lv_t *pdlv, enum poll_type type)
{
	return pdlv->type == type;
}

static inline void pdlv_set_debug(lvmpolld_lv_t *pdlv, unsigned debug)
{
	pdlv->debug = debug;
}

/* FIXME: --verbose is countable parameter in LVM2 */
static inline void pdlv_set_verbose(lvmpolld_lv_t *pdlv, unsigned verbose)
{
	pdlv->verbose = verbose;
}

static inline int pdlv_locked_polling_finished(const lvmpolld_lv_t *pdlv)
{
	return pdlv->polling_finished;
}

static inline dm_percent_t pdlv_locked_get_percent(const lvmpolld_lv_t *pdlv)
{
	return pdlv->percent;
}

static inline lvmpolld_cmd_stat_t pdlv_locked_get_cmd_state(const lvmpolld_lv_t *pdlv)
{
	return pdlv->cmd_state;
}

static inline unsigned pdlv_locked_get_internal_error(const lvmpolld_lv_t *pdlv)
{
	return pdlv->internal_error;
}

static inline void pdlv_set_cmd_pid(lvmpolld_lv_t *pdlv, pid_t pid)
{
	pdlv->lvmcmd = pid;
}

static inline pid_t pdlv_get_cmd_pid(const lvmpolld_lv_t *pdlv)
{
	return pdlv->lvmcmd;
}

static inline unsigned pdlv_get_timeout(const lvmpolld_lv_t *pdlv)
{
	return pdlv->pdtimeout;
}

static inline lvmpolld_lv_t *pdst_lookup(lvmpolld_store_t *pdst, const char *key)
{
	return dm_hash_lookup(pdst->store, key);
}

static inline void pdst_remove(lvmpolld_store_t *pdst, const char *key)
{
	dm_hash_remove(pdst->store, key);
}

static inline int pdst_insert(lvmpolld_store_t *pdst, const char *key, lvmpolld_lv_t *pdlv)
{
	return dm_hash_insert(pdst->store, key, pdlv);
}

/* pdlv structure has use_count == 1 after create */
lvmpolld_lv_t *pdlv_create(struct lvmpolld_state *ls, const char *lvid,
			   const enum poll_type type, const char *sinterval,
			   unsigned pdtimeout, unsigned abort,
			   lvmpolld_store_t *pdst,
			   lvmpolld_parse_output_fn_t parse_fn);

/* use count must not reach 0 when structure is in lvmpolld_store_t */
void pdlv_put(lvmpolld_lv_t *pdlv);
void pdlv_get(lvmpolld_lv_t *pdlv);

void pdlv_set_percents(lvmpolld_lv_t *pdlv, dm_percent_t percent);

void pdlv_set_cmd_state(lvmpolld_lv_t *pdlv, const lvmpolld_cmd_stat_t *cmd_state);

dm_percent_t pdlv_get_percents(lvmpolld_lv_t *pdlv);

void pdlv_set_internal_error(lvmpolld_lv_t *pdlv, unsigned error);

#endif /* _LVM_LVMPOLLD_DATA_UTILS_H */
