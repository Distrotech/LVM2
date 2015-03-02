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

#include "libdevmapper.h"
#include "lvmpolld-data-utils.h"

static char *_construct_full_lvname(const char *vgname, const char *lvname)
{
	char *name;
	size_t l;

	l = strlen(vgname) + strlen(lvname) + 2; /* vg/lv and \0 */
	name = (char *) dm_malloc(l * sizeof(char));
	if (!name)
		return NULL;

	if (dm_snprintf(name, l, "%s/%s", vgname, lvname) < 0) {
		dm_free(name);
		name = NULL;
	}

	return name;
}

static char *_construct_lvm_system_dir_env(const char *sysdir)
{
	/*
	 *  Store either "LVM_SYSTEM_DIR=/path/to..."
	 *		    - or -
	 *  just single char to store NULL byte
	 */
	size_t l = sysdir ? strlen(sysdir) + 16 : 1;
	char *env = (char *) dm_malloc(l * sizeof(char));

	if (!env)
		return NULL;

	*env = '\0';

	if (sysdir && dm_snprintf(env, l, "LVM_SYSTEM_DIR=%s", sysdir) < 0) {
		dm_free(env);
		env = NULL;
	}

	return env;
}

static inline const char *_get_lvid(const char *lvmpolld_id, const char *sysdir)
{
	return lvmpolld_id ? (lvmpolld_id + (sysdir ? strlen(sysdir) : 0)) : NULL;
}

char *construct_id(const char *sysdir, const char *uuid)
{
	char *id;
	int r;
	size_t l;

	l = strlen(uuid) + (sysdir ? strlen(sysdir) : 0) + 1;
	id = (char *) dm_malloc(l * sizeof(char));
	if (!id)
		return NULL;

	r = sysdir ? dm_snprintf(id, l, "%s%s", sysdir, uuid) :
		     dm_snprintf(id, l, "%s", uuid);

	if (r < 0) {
		dm_free(id);
		id = NULL;
	}

	return id;
}

lvmpolld_lv_t *pdlv_create(lvmpolld_state_t *ls, const char *id,
			   const char *vgname, const char *lvname,
			   const char *sysdir, enum poll_type type,
			   const char *sinterval, unsigned pdtimeout,
			   lvmpolld_store_t *pdst)
{
	char *lvmpolld_id = dm_strdup(id), /* copy */
	     *full_lvname = _construct_full_lvname(vgname, lvname), /* copy */
	     *lvm_system_dir_env = _construct_lvm_system_dir_env(sysdir); /* copy */

	lvmpolld_lv_t tmp = {
		.ls = ls,
		.type = type,
		.lvmpolld_id = lvmpolld_id,
		.lvid = _get_lvid(lvmpolld_id, sysdir),
		.lvname = full_lvname,
		.lvm_system_dir_env = lvm_system_dir_env,
		.sinterval = dm_strdup(sinterval), /* copy */
		.pdtimeout = pdtimeout ?: PDTIMEOUT_DEF,
		.cmd_state = { .retcode = -1, .signal = 0 },
		.pdst = pdst
	}, *pdlv = (lvmpolld_lv_t *) dm_malloc(sizeof(lvmpolld_lv_t));

	if (!pdlv || !tmp.lvid || !tmp.lvname || !tmp.lvm_system_dir_env || !tmp.sinterval)
		goto err;

	memcpy(pdlv, &tmp, sizeof(*pdlv));

	if (pthread_mutex_init(&pdlv->lock, NULL))
		goto err;

	return pdlv;

err:
	dm_free((void *)lvmpolld_id);
	dm_free((void *)full_lvname);
	dm_free((void *)lvm_system_dir_env);
	dm_free((void *)tmp.sinterval);
	dm_free((void *)pdlv);

	return NULL;
}

void pdlv_destroy(lvmpolld_lv_t *pdlv)
{
	dm_free((void *)pdlv->lvmpolld_id);
	dm_free((void *)pdlv->lvname);
	dm_free((void *)pdlv->sinterval);
	dm_free((void *)pdlv->lvm_system_dir_env);
	dm_free((void *)pdlv->cmdargv);
	dm_free((void *)pdlv->cmdenvp);

	pthread_mutex_destroy(&pdlv->lock);

	dm_free((void *)pdlv);
}

unsigned pdlv_get_polling_finished(lvmpolld_lv_t *pdlv)
{
	unsigned ret;

	pdlv_lock(pdlv);
	ret = pdlv->polling_finished;
	pdlv_unlock(pdlv);

	return ret;
}

lvmpolld_lv_state_t pdlv_get_status(lvmpolld_lv_t *pdlv)
{
	lvmpolld_lv_state_t r;

	pdlv_lock(pdlv);
	r.internal_error = pdlv_locked_internal_error(pdlv);
	r.polling_finished = pdlv_locked_polling_finished(pdlv);
	r.cmd_state = pdlv_locked_cmd_state(pdlv);
	pdlv_unlock(pdlv);

	return r;
}

void pdlv_set_cmd_state(lvmpolld_lv_t *pdlv, const lvmpolld_cmd_stat_t *cmd_state)
{
	pdlv_lock(pdlv);
	pdlv->cmd_state = *cmd_state;
	pdlv_unlock(pdlv);
}

void pdlv_set_internal_error(lvmpolld_lv_t *pdlv, unsigned error)
{
	pdlv_lock(pdlv);
	pdlv->internal_error = error;
	pdlv->polling_finished = 1;
	pdlv_unlock(pdlv);
}

void pdlv_set_polling_finished(lvmpolld_lv_t *pdlv, unsigned finished)
{
	pdlv_lock(pdlv);
	pdlv->polling_finished = finished;
	pdlv_unlock(pdlv);
}

void pdst_init(lvmpolld_store_t *pdst, const char *name)
{
	pthread_mutex_init(&pdst->lock, NULL);
	pdst->store = dm_hash_create(32);
	pdst->name = name;
}

void pdst_destroy(lvmpolld_store_t *pdst)
{
	dm_hash_destroy(pdst->store);
	pthread_mutex_destroy(&pdst->lock);
}
