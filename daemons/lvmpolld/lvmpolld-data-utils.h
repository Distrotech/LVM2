/*
 * Copyright (C) 2015 Red Hat, Inc.
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

typedef struct lvmpolld_lv lvmpolld_lv_t;
typedef struct lvmpolld_state lvmpolld_state_t;

typedef void (*lvmpolld_parse_output_fn_t) (lvmpolld_lv_t *pdlv, const char *line);

/* TODO: replace with configuration option */
#define PDTIMEOUT_DEF 60

enum poll_type {
	PVMOVE = 0,
	CONVERT,
	MERGE,
	MERGE_THIN,
	POLL_TYPE_MAX
};

typedef struct {
	pthread_mutex_t lock;
	void *store;
	const char *name;
	unsigned active_polling_count;
} lvmpolld_store_t;

typedef struct {
	int retcode;
	int signal;
} lvmpolld_cmd_stat_t;

typedef struct {
	unsigned internal_error:1;
	unsigned polling_finished:1;
	lvmpolld_cmd_stat_t cmd_state;
	dm_percent_t percent;
} lvmpolld_lv_state_t;

typedef struct lvmpolld_lv {
	/*
	 * accessing following vars doesn't
	 * require lvmpolld_lv_t lock
	 */
	struct lvmpolld_state *const ls;
	const enum poll_type type;
	const char *const lvid;
	const char *const lvname; /* full vg/lv name */
	const unsigned pdtimeout; /* in seconds */
	const char *const sinterval;
	lvmpolld_store_t *const pdst;
	lvmpolld_parse_output_fn_t parse_output_fn;
	const char *const *cmdargv;

	/* only used by write */
	pid_t cmd_pid;
	pthread_t tid;

	pthread_mutex_t lock;

	/* block of shared variables protected by lock */
	lvmpolld_cmd_stat_t cmd_state;
	dm_percent_t percent;
	unsigned polling_finished:1; /* no more updates */
	unsigned internal_error:1; /* unrecoverable error occured in lvmpolld */
} lvmpolld_lv_t;

/* LVMPOLLD_LV_T section */

/* only call with appropriate lvmpolld_store_t lock held */
lvmpolld_lv_t *pdlv_create(struct lvmpolld_state *ls, const char *lvid,
			   const char *lvname, const enum poll_type type,
			   const char *sinterval, unsigned pdtimeout,
			   lvmpolld_store_t *pdst,
			   lvmpolld_parse_output_fn_t parse_fn);

/* only call with appropriate lvmpolld_store_t lock held */
void pdlv_destroy(lvmpolld_lv_t *pdlv);

static inline void pdlv_lock(lvmpolld_lv_t *pdlv)
{
	pthread_mutex_lock(&pdlv->lock);
}

static inline void pdlv_unlock(lvmpolld_lv_t *pdlv)
{
	pthread_mutex_unlock(&pdlv->lock);
}

/*
 * no lvmpolld_lv_t lock required section
 */
static inline int pdlv_is_type(const lvmpolld_lv_t *pdlv, enum poll_type type)
{
	return pdlv->type == type;
}

static inline unsigned pdlv_get_timeout(const lvmpolld_lv_t *pdlv)
{
	return pdlv->pdtimeout;
}

unsigned pdlv_get_polling_finished(lvmpolld_lv_t *pdlv);
lvmpolld_lv_state_t pdlv_get_status(lvmpolld_lv_t *pdlv);
void pdlv_set_cmd_state(lvmpolld_lv_t *pdlv, const lvmpolld_cmd_stat_t *cmd_state);
void pdlv_set_internal_error(lvmpolld_lv_t *pdlv, unsigned error);
void pdlv_set_percents(lvmpolld_lv_t *pdlv, dm_percent_t percent);
void pdlv_set_polling_finished(lvmpolld_lv_t *pdlv, unsigned finished);

/*
 * lvmpolld_lv_t lock required section
 */
static inline lvmpolld_cmd_stat_t pdlv_locked_cmd_state(const lvmpolld_lv_t *pdlv)
{
	return pdlv->cmd_state;
}

static inline int pdlv_locked_polling_finished(const lvmpolld_lv_t *pdlv)
{
	return pdlv->polling_finished;
}

static inline unsigned pdlv_locked_internal_error(const lvmpolld_lv_t *pdlv)
{
	return pdlv->internal_error;
}

static inline dm_percent_t pdlv_locked_percent(const lvmpolld_lv_t *pdlv)
{
	return pdlv->percent;
}

/* LVMPOLLD_STORE_T manipulation routines */

void pdst_init(lvmpolld_store_t *pdst, const char *name);
void pdst_destroy(lvmpolld_store_t *pdst);

static inline void pdst_lock(lvmpolld_store_t *pdst)
{
	pthread_mutex_lock(&pdst->lock);
}

static inline void pdst_unlock(lvmpolld_store_t *pdst)
{
	pthread_mutex_unlock(&pdst->lock);
}

static inline void pdst_locked_inc(lvmpolld_store_t *pdst)
{
	pdst->active_polling_count++;
}

static inline void pdst_locked_dec(lvmpolld_store_t *pdst)
{
	pdst->active_polling_count--;
}

static inline unsigned pdst_locked_get_active_polling_count(const lvmpolld_store_t *pdst)
{
	return pdst->active_polling_count;
}

static inline int pdst_locked_insert(lvmpolld_store_t *pdst, const char *key, lvmpolld_lv_t *pdlv)
{
	return dm_hash_insert(pdst->store, key, pdlv);
}

static inline lvmpolld_lv_t *pdst_locked_lookup(lvmpolld_store_t *pdst, const char *key)
{
	return dm_hash_lookup(pdst->store, key);
}

static inline void pdst_locked_remove(lvmpolld_store_t *pdst, const char *key)
{
	dm_hash_remove(pdst->store, key);
}

#endif /* _LVM_LVMPOLLD_DATA_UTILS_H */
