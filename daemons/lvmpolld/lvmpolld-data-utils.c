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

static void pdvg_destroy(lvmpolld_vg_t *pdvg)
{
	if (pdvg->vgid)
		dm_free((void *)pdvg->vgid);

	pthread_mutex_destroy(&pdvg->lock);

	dm_free((void *)pdvg);
}

lvmpolld_vg_t *pdvg_create(struct lvmpolld_state *ls, const char *vgid, const enum poll_type type)
{
	lvmpolld_vg_t *pdvg = (lvmpolld_vg_t *) dm_malloc(sizeof(lvmpolld_vg_t));
	if (!pdvg)
		return NULL;

	pdvg->use_count = 1;

	pdvg->type = type;

	pdvg->ds = ls;

	if (!(pdvg->vgid = dm_strdup(vgid)))
		goto err;

	if (pthread_mutex_init(&pdvg->lock, NULL))
		goto err;

	return pdvg;
err:
	pdvg_destroy(pdvg);
	return NULL;
}

/* with vgid_to_pdvg lock held only */
void pdvg_get(lvmpolld_vg_t *pdvg)
{
	pthread_mutex_lock(&pdvg->lock);
	pdvg->use_count++;
	pthread_mutex_unlock(&pdvg->lock);
}

/*
 * polling thread should first detach the data structure 
 * from lvmpolld_state_t and put it afterwards.
 *
 *  Otherwise we have race use_count == 0 vs use_count == 1
 */
void pdvg_put(lvmpolld_vg_t *pdvg)
{
	pthread_mutex_lock(&pdvg->lock);

	/* pdvg->use_count--; */

	if (!--pdvg->use_count) {
		pthread_mutex_unlock(&pdvg->lock);
		pdvg_destroy(pdvg);
		return;
	}

	pthread_mutex_unlock(&pdvg->lock);
}
