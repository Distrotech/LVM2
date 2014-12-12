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

#include "libdevmapper.h"
#include "lvmpolld-data-utils.h"

static void pdlv_destroy(lvmpolld_lv_t *pdlv)
{
	dm_free((void *)pdlv->lvid);
	dm_free((void *)pdlv->sinterval);

	pthread_mutex_destroy(&pdlv->lock);
	pthread_cond_destroy(&pdlv->cond_update);

	dm_free((void *)pdlv);
}

lvmpolld_lv_t *pdlv_create(lvmpolld_state_t *ls, const char *lvid,
			   enum poll_type type, const char *sinterval,
			   unsigned pdtimeout, unsigned abort,
			   lvmpolld_store_t *pdst,
			   lvmpolld_parse_output_fn_t parse_fn)
{
	lvmpolld_lv_t tmp = {
		.ls = ls,
		.type = type,
		.lvid = dm_strdup(lvid),
		.sinterval = sinterval ? dm_strdup(sinterval) : NULL,
		.pdtimeout = pdtimeout ?: PDTIMEOUT_DEF,
		.percent = DM_PERCENT_0,
		.cmd_state = { .ret_code = -1, .signal = 0 },
		.use_count = 1,
		.pdst = pdst,
		.parse_output_fn = parse_fn,
		.abort = abort
	}, *pdlv = (lvmpolld_lv_t *) dm_malloc(sizeof(lvmpolld_lv_t));

	if (!pdlv) {
		dm_free((void *)tmp.lvid);
		dm_free((void *)tmp.sinterval);
		return NULL;
	}

	memcpy(pdlv, &tmp, sizeof(*pdlv));

	if (!pdlv->lvid)
		goto lvid_err;

	if (sinterval && !pdlv->sinterval)
		goto sint_err;

	if (pthread_mutex_init(&pdlv->lock, NULL))
		goto mutex_err;

	if (pthread_cond_init(&pdlv->cond_update, NULL))
		goto cond_err;

	return pdlv;
cond_err:
	pthread_mutex_destroy(&pdlv->lock);
mutex_err:
	dm_free((void *)pdlv->sinterval);
sint_err:
	dm_free((void *)pdlv->lvid);
lvid_err:
	dm_free((void *)pdlv);

	return NULL;
}

/*
lvmpolld_lv_t *pdlv_create(struct lvmpolld_state *ls, const char *lvid,
			   enum poll_type type, const char *sinterval)
{
	lvmpolld_lv_t *pdlv = (lvmpolld_lv_t *) dm_malloc(sizeof(lvmpolld_lv_t));
	if (!pdlv)
		return NULL;

	pdlv->lvid = dm_strdup(lvid);
	if (!pdlv->lvid)
		goto lvid_err;

	pdlv->sinterval = NULL;
	if (sinterval && !(pdlv->sinterval = dm_strdup(sinterval)))
		goto sint_err;

	if (pthread_mutex_init(&pdlv->lock, NULL))
		goto mutex_err;

	if (pthread_cond_init(&pdlv->cond_update, NULL))
		goto cond_err;

	pdlv->lvmcmd = 0;
	pdlv->type = type;
	pdlv->cmd_state.ret_code = 1;
	pdlv->cmd_state.signal = 0;
	pdlv->percent = DM_PERCENT_0;
	pdlv->use_count = 1;
	pdlv->ls = ls;
	pdlv->polling_finished = 0;
	pdlv->internal_error = 0;

	return pdlv;
cond_err:
	pthread_mutex_destroy(&pdlv->lock);
mutex_err:
	dm_free((void *)pdlv->sinterval);
sint_err:
	dm_free((void *)pdlv->lvid);
lvid_err:
	dm_free((void *)pdlv);

	return NULL;
}
*/

/* with lvid_to_pdlv lock held only */
void pdlv_get(lvmpolld_lv_t *pdlv)
{
	pdlv_lock(pdlv);
	pdlv->use_count++;
	pdlv_unlock(pdlv);
}

/*
 * polling thread should first detach the data structure 
 * from lvmpolld_state_t and put it afterwards.
 *
 *  Otherwise we have race use_count == 0 vs use_count == 1
 */
void pdlv_put(lvmpolld_lv_t *pdlv)
{
	unsigned int r;

	pdlv_lock(pdlv);
	r = --pdlv->use_count;
	pdlv_unlock(pdlv);

	if (!r)
		pdlv_destroy(pdlv);
}

void pdlv_set_percents(lvmpolld_lv_t *pdlv, dm_percent_t percent)
{
	pdlv_lock(pdlv);

	pdlv->percent = percent;

	pthread_cond_broadcast(&pdlv->cond_update);

	pdlv_unlock(pdlv);
}

void pdlv_set_cmd_state(lvmpolld_lv_t *pdlv, const lvmpolld_cmd_stat_t *cmd_state)
{
	pdlv_lock(pdlv);

	pdlv->cmd_state = *cmd_state;
	pdlv->polling_finished = 1;

	pthread_cond_broadcast(&pdlv->cond_update);

	pdlv_unlock(pdlv);
}

dm_percent_t pdlv_get_percents(lvmpolld_lv_t *pdlv)
{
	dm_percent_t p;

	pdlv_lock(pdlv);
	p = pdlv_locked_get_percent(pdlv);
	pdlv_unlock(pdlv);

	return p;
}

void pdlv_set_internal_error(lvmpolld_lv_t *pdlv, unsigned error)
{
	pdlv_lock(pdlv);

	pdlv->internal_error = error;
	pdlv->polling_finished = 1;

	pthread_cond_broadcast(&pdlv->cond_update);

	pdlv_unlock(pdlv);
}

void pdst_init(lvmpolld_store_t *pdst)
{
	pdst->store = dm_hash_create(32);
	pthread_mutex_init(&pdst->lock, NULL);
}

void pdst_destroy(lvmpolld_store_t *pdst)
{
	dm_hash_destroy(pdst->store);
	pthread_mutex_destroy(&pdst->lock);
}
