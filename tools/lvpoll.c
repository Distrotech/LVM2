/*
 * Copyright (C) 2014 Red Hat, Inc. All rights reserved.
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

#include "tools.h"
#include "polldaemon.h"
#include "pvmove.h"
#include "lvconvert.h"
#include "polling_ops.h"

typedef struct lvpoll_parms {
	union lvid lvid;
	struct daemon_parms *parms;
} lvpoll_parms_t;

static struct poll_functions _pvmove_fns = {
	.poll_progress = poll_mirror_progress,
	.update_metadata = pvmove_update_metadata,
	.finish_copy = finish_pvmove
};

static struct poll_functions _convert_fns = {
	.poll_progress = poll_mirror_progress,
	.finish_copy = finish_lvconvert_mirror
};

static struct poll_functions _merge_fns = {
	.poll_progress = poll_merge_progress,
	.finish_copy = finish_lvconvert_merge
};

static struct poll_functions _thin_merge_fns = {
	.poll_progress = poll_thin_merge_progress,
	.finish_copy = finish_lvconvert_merge
};

static int poll_lv_by_lvid(struct cmd_context *cmd,
			   const char *vg_name __attribute__((unused)),
			   struct volume_group *vg, void *handle)
{
	int finished;
	struct logical_volume *lv;
	struct lv_list *lvl;
	lvpoll_parms_t *lvp = (lvpoll_parms_t *)handle;

	if (id_equal(lvp->lvid.id, &vg->id)) {
		log_verbose("Found requested VG");
		/* return process_each_lv_in_vg(cmd, vg, NULL, NULL, 1,
					     lvp_parms->parms, get_pvmove_lv);*/
		dm_list_iterate_items(lvl, &vg->lvs) {
			lv = lvl->lv;
			/* PVMOVE LVs have lv_uuid zeroed, any operation initiated by lvconvert has lv_type == 0 */
			if ((lvp->parms->lv_type && !(lv->status & lvp->parms->lv_type)) ||
			    (*lvp->lvid.id[1].uuid &&
			     !id_equal(&lvp->lvid.id[1], &lv->lvid.id[1])))
				continue;

			log_verbose("Found requested LV");

			/* I don't care about device name */
			if (!check_lv_status(cmd, vg, lv, "none", lvp->parms, &finished))
				return ECMD_FAILED;

			if (!finished)
				lvp->parms->outstanding_count++;
		}
	}

	return ECMD_PROCESSED;
}

static int set_daemon_parms(const char* poll_type, struct daemon_parms *parms)
{
	if (!strcmp(poll_type, PVMOVE_POLL)) {
		parms->progress_title = "Moved";
		parms->lv_type = PVMOVE;
		parms->poll_fns = &_pvmove_fns;
	} else if (!strcmp(poll_type, CONVERT_POLL)) {
		parms->progress_title = "Converted";
		parms->poll_fns = &_convert_fns;
	} else if (!strcmp(poll_type, MERGE_POLL)) {
		parms->progress_title = "Merged";
		parms->poll_fns = &_merge_fns;
	} else if (!strcmp(poll_type, MERGE_THIN_POLL)) {
		parms->progress_title = "Merged";
		parms->poll_fns = &_thin_merge_fns;
	} else {
		log_error("Unknown polling type %s", poll_type);
		return 0;
	}

	return 1;
}

static int poll_vg(struct cmd_context *cmd, const char *poll_type,
		   const char *uuid, unsigned abort, unsigned int interval)
{
	int ret;
	lvpoll_parms_t lvp;

	int wait_before_testing = (arg_sign_value(cmd, interval_ARG, SIGN_NONE) == SIGN_PLUS);
	struct daemon_parms parms = { 0 };

	parms.interval = interval;
	parms.aborting = abort;
	parms.progress_display = 1;

	if (!set_daemon_parms(poll_type, &parms))
		return EINVALID_CMD_LINE;

	lvp.parms = &parms;

	strncpy(lvp.lvid.s, uuid, sizeof(lvp.lvid));
	lvp.lvid.s[sizeof(lvp.lvid.s) - 1] = '\0';

	log_verbose("uuid: %s", uuid);
	log_verbose("lvid: %s", lvp.lvid.s);

	if (!id_valid(lvp.lvid.id)) {
		log_error("Invalid VG UUID format");
		return EINVALID_CMD_LINE;
	}

	if (lvp.lvid.s[ID_LEN] && !id_valid(&lvp.lvid.id[1])) {
		log_error("Invalid LV UUID format");
		return EINVALID_CMD_LINE;
	}

	while (1) {
		if (wait_before_testing)
			sleep_and_rescan_devices(lvp.parms);

		parms.outstanding_count = 0;
		/* replace with lookup by vgname ? */
		ret = process_each_vg(cmd, 0, NULL, READ_FOR_UPDATE, &lvp, poll_lv_by_lvid);
		log_verbose("finished process_each_vg(): %d", ret);

		assert(!(lvp.parms->outstanding_count > 1));

		if (!lvp.parms->outstanding_count || ret != ECMD_PROCESSED)
			break;

		if (!wait_before_testing)
			sleep_and_rescan_devices(lvp.parms);
	};

	return ret;
}

int lvpoll(struct cmd_context *cmd, int argc, char **argv)
{
	if (argc < 2) {
		log_error("polling type and uuid parameters are mandatory");
		return EINVALID_CMD_LINE;
	}

	if (arg_sign_value(cmd, interval_ARG, SIGN_NONE) == SIGN_MINUS) {
		log_error("Argument to --interval cannot be negative");
		return EINVALID_CMD_LINE;
	}

	log_print_unless_silent("LVM_SYSTEM_DIR=%s", getenv("LVM_SYSTEM_DIR") ?: "<not set>");

	return poll_vg(cmd, argv[0], argv[1], arg_is_set(cmd, abort_ARG),
		       arg_uint_value(cmd, interval_ARG, find_config_tree_int(cmd, activation_polling_interval_CFG, NULL)));
}
