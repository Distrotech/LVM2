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
	if (pdlv->lvid)
		dm_free((void *)pdlv->lvid);

	pthread_mutex_destroy(&pdlv->lock);

	pthread_cond_destroy(&pdlv->cond_update);

	dm_free((void *)pdlv);
}

lvmpolld_lv_t *pdlv_create(struct lvmpolld_state *ls, const char *lvid, enum poll_type type)
{
	lvmpolld_lv_t *pdlv = (lvmpolld_lv_t *) dm_malloc(sizeof(lvmpolld_lv_t));
	if (!pdlv)
		return NULL;

	pdlv->lvid = dm_strdup(lvid);
	if (!pdlv->lvid)
		goto lvid_err;

	if (pthread_mutex_init(&pdlv->lock, NULL))
		goto mutex_err;

	if (pthread_cond_init(&pdlv->cond_update, NULL))
		goto cond_err;

	pdlv->type = type;
	pdlv->cmd_state.ret_code = 1;
	pdlv->cmd_state.signal = 0;
	pdlv->percent = DM_PERCENT_0; /* ??? */
	pdlv->use_count = 1;
	pdlv->ls = ls;
	pdlv->polling_finished = 0;

	return pdlv;
cond_err:
	pthread_mutex_destroy(&pdlv->lock);
mutex_err:
	dm_free((void *)pdlv->lvid);
lvid_err:
	dm_free((void *)pdlv);

	return NULL;
}

/* with lvid_to_pdlv lock held only */
void pdlv_get(lvmpolld_lv_t *pdlv)
{
	pthread_mutex_lock(&pdlv->lock);
	pdlv->use_count++;
	pthread_mutex_unlock(&pdlv->lock);
}

/*
 * polling thread should first detach the data structure 
 * from lvmpolld_state_t and put it afterwards.
 *
 *  Otherwise we have race use_count == 0 vs use_count == 1
 */
void pdlv_put(lvmpolld_lv_t *pdlv)
{
	pthread_mutex_lock(&pdlv->lock);

	//DEBUGLOG(pdlv->ls, "%s: %s %d", "LVMPOLLD","going to decrease use_count with value", pdlv->use_count);
	//pdlv->use_count--;
	//DEBUGLOG(pdlv->ls, "%s: %s %d", "LVMPOLLD","new value", pdlv->use_count);

	if (!--pdlv->use_count) {
		pthread_mutex_unlock(&pdlv->lock);
		//DEBUGLOG(pdlv->ls, "%s: %s", "LVMPOLLD","Going to destroy the pdlv");
		pdlv_destroy(pdlv);
		return;
	}

	pthread_mutex_unlock(&pdlv->lock);
}

void pdlv_set_percents(lvmpolld_lv_t *pdlv, dm_percent_t percent)
{
	pthread_mutex_lock(&pdlv->lock);

	pdlv->percent = percent;

	pthread_cond_broadcast(&pdlv->cond_update);

	pthread_mutex_unlock(&pdlv->lock);
}

void pdlv_set_cmd_state(lvmpolld_lv_t *pdlv, lvmpolld_cmd_stat_t *cmd_state)
{
	pthread_mutex_lock(&pdlv->lock);

	pdlv->cmd_state = *cmd_state;

	pdlv->polling_finished = 1;

	pthread_cond_broadcast(&pdlv->cond_update);

	pthread_mutex_unlock(&pdlv->lock);
}

dm_percent_t pdlv_get_percents(lvmpolld_lv_t *pdlv)
{
	dm_percent_t p;

	pthread_mutex_lock(&pdlv->lock);
	p = pdlv->percent;
	pthread_mutex_unlock(&pdlv->lock);

	return p;
}
