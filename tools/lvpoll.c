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
			   struct volume_group *vg,
			   struct daemon_parms *parms,
			   union lvid *lvid)
{
	int finished;
	struct logical_volume *lv;
	struct lv_list *lvl;

	dm_list_iterate_items(lvl, &vg->lvs) {
		lv = lvl->lv;
		/* PVMOVE LVs have lv_uuid zeroed, any operation initiated by lvconvert has lv_type == 0 */
		if ((parms->lv_type && !(lv->status & parms->lv_type)) ||
		    (*lvid->id[1].uuid &&
		     !id_equal(&lvid->id[1], &lv->lvid.id[1])))
			continue;

		log_verbose("Found requested LV");

		/* I don't care about device name */
		if (!check_lv_status(cmd, vg, lv, "none", parms, &finished))
			return 0;

		if (!finished)
			parms->outstanding_count++;
	}

	return 1;
}

static int set_daemon_parms(struct cmd_context *cmd, struct daemon_parms *parms)
{
	const char *poll_oper = arg_str_value(cmd, polloperation_ARG, "");

	parms->interval = arg_uint_value(cmd, interval_ARG, 0);
	parms->aborting = arg_count(cmd, abort_ARG);
	parms->progress_display = 1;
	parms->wait_before_testing = (arg_sign_value(cmd, interval_ARG, SIGN_NONE) == SIGN_PLUS);

	if (!strcmp(poll_oper, PVMOVE_POLL)) {
		parms->progress_title = "Moved";
		parms->lv_type = PVMOVE;
		parms->poll_fns = &_pvmove_fns;
	} else if (!strcmp(poll_oper, CONVERT_POLL)) {
		parms->progress_title = "Converted";
		parms->poll_fns = &_convert_fns;
	} else if (!strcmp(poll_oper, MERGE_POLL)) {
		parms->progress_title = "Merged";
		parms->poll_fns = &_merge_fns;
	} else if (!strcmp(poll_oper, MERGE_THIN_POLL)) {
		parms->progress_title = "Merged";
		parms->poll_fns = &_thin_merge_fns;
	} else {
		log_error("Unknown polling operation %s", poll_oper);
		return 0;
	}

	cmd->handles_missing_pvs = arg_count(cmd, handlemissingpvs_ARG);

	return 1;
}

static int poll_vg(struct cmd_context *cmd, const char *vgname)
{
	int finished = 0, r;
	struct volume_group *vg;
	union lvid lvid;
	struct daemon_parms parms = { 0 };

	if (!set_daemon_parms(cmd, &parms))
		return EINVALID_CMD_LINE;

	strncpy(lvid.s, arg_str_value(cmd, uuidstr_ARG, ""), sizeof(union lvid));
	lvid.s[sizeof(lvid.s) - 1] = '\0';

	if (!id_valid(lvid.id)) {
		log_error("Invalid VG UUID format");
		return EINVALID_CMD_LINE;
	}

	if (lvid.s[ID_LEN] && !id_valid(&lvid.id[1])) {
		log_error("Invalid LV UUID format");
		return EINVALID_CMD_LINE;
	}

	while (!finished) {
		parms.outstanding_count = 0;

		if (parms.wait_before_testing)
			sleep_and_rescan_devices(&parms);

		dev_close_all();

		vg = vg_read_for_update(cmd, vgname, NULL, 0);
		if (vg_read_error(vg)) {
			release_vg(vg);
			log_error("ABORTING: Can't reread VG %s", vgname);
			return ECMD_FAILED;
		}

		/* TODO: eventually replace with process_each_lv_in_vg */
		r = poll_lv_by_lvid(cmd, vg, &parms, &lvid);

		unlock_and_release_vg(cmd, vg, vg->name);

		if (!r)
			return ECMD_FAILED;

		/* TODO: remove as it's perhaps useless now */
		assert(!(parms.outstanding_count > 1));

		finished = !parms.outstanding_count;

		if (!parms.wait_before_testing && !finished)
			sleep_and_rescan_devices(&parms);
	}

	return ECMD_PROCESSED;
}

int lvpoll(struct cmd_context *cmd, int argc, char **argv)
{
	if (!arg_count(cmd, uuidstr_ARG)) {
		log_error("--uuid parameter is mandatory");
		return EINVALID_CMD_LINE;
	}

	if (!arg_count(cmd, polloperation_ARG)) {
		log_error("--poll-operation parameter is mandatory");
		return EINVALID_CMD_LINE;
	}

	if (arg_sign_value(cmd, interval_ARG, SIGN_NONE) == SIGN_MINUS) {
		log_error("Argument to --interval cannot be negative");
		return EINVALID_CMD_LINE;
	}

	if (!argc) {
		log_error("Provide vgname");
		return EINVALID_CMD_LINE;
	}

	log_print_unless_silent("LVM_SYSTEM_DIR=%s", getenv("LVM_SYSTEM_DIR") ?: "<not set>");
	log_verbose("cmdline: %s", cmd->cmd_line);

	return poll_vg(cmd, argv[0]);
}
