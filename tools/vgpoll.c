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

typedef struct vgpoll_parms {
	struct id id;
	struct daemon_parms *parms;
} vgpoll_parms_t;

static struct poll_functions _pvmove_fns = {
	/* Can be removed later */
	.get_copy_name_from_lv = NULL,
	.get_copy_vg = NULL,
	.get_copy_lv = NULL,
	/* in polldaemon so far vvvv */
	.poll_progress = poll_mirror_progress,
	.update_metadata = pvmove_update_metadata,
	.finish_copy = finish_pvmove
};

static int get_pvmove_lv(struct cmd_context *cmd, struct logical_volume *lv, void *handle)
{
	const char *devname;
	int finished;
	struct daemon_parms *parms = (struct daemon_parms *)handle;

	if (!(lv->status & parms->lv_type))
		return ECMD_PROCESSED;

	log_verbose("Found requested lv");

	/* may skip? */
	devname = get_pvmove_pvname_from_lv_mirr(lv);
	if (!lv_is_active(lv)) {
		log_print_unless_silent("%s: Skipping inactive LV. Try lvchange or vgchange.", devname);
		return ECMD_PROCESSED;
	}

	/* this will produce progress log, rerout it to socket? */
	if(!check_lv_status(cmd, lv->vg, lv, devname, parms, &finished))
		return ECMD_FAILED;

	if (!finished)
		parms->outstanding_count++;

	return ECMD_PROCESSED;
}

static int poll_vg_by_vgid(struct cmd_context *cmd,
			   const char *vg_name __attribute__((unused)),
			   struct volume_group *vg, void *handle)
{
	vgpoll_parms_t *vgp_parms = (vgpoll_parms_t *)handle;

	/* need to care about duplicite vgid? KISS for the time... */
	if (id_equal(&vgp_parms->id, &vg->id)) {
		log_verbose("Found requested VG");
		return process_each_lv_in_vg(cmd, vg, NULL, NULL, 1,
					     vgp_parms->parms, get_pvmove_lv);
	}

	return ECMD_PROCESSED;
}

/* or pvid, vgname ? */
static int poll_vg(struct cmd_context * cmd, const char *vgid, unsigned abort, unsigned int interval)
{
	int ret;

	int wait_before_testing = (arg_sign_value(cmd, interval_ARG, SIGN_NONE) == SIGN_PLUS);

	struct daemon_parms parms = {
	/* FIXME: vvvv, also consider moving to heap */
		.interval = interval,
		.aborting = abort,
		.outstanding_count = 0,
		.progress_display = 1,
		.progress_title = "Moved",
		.lv_type = PVMOVE,
		.poll_fns = &_pvmove_fns
	};

	vgpoll_parms_t vgpoll_parms = {
		.parms = &parms
	};

	if (!id_read_format(&vgpoll_parms.id, vgid)) {
		log_error("Invalid UUID format");
		return EINVALID_CMD_LINE;
	}

	while (1) {
		if (wait_before_testing)
			sleep_and_rescan_devices(vgpoll_parms.parms);

		ret = process_each_vg(cmd, 0, NULL, READ_FOR_UPDATE, (void *)&vgpoll_parms, poll_vg_by_vgid);
		log_verbose("finished process_each_vg(): %d", ret);

		assert(!(vgpoll_parms.parms->outstanding_count > 1));

		if (!vgpoll_parms.parms->outstanding_count || ret != ECMD_PROCESSED)
			break;

		if (!wait_before_testing)
			sleep_and_rescan_devices(vgpoll_parms.parms);
	};

	return ret;
}

int vgpoll(struct cmd_context *cmd, int argc, char **argv)
{
	if (argc < 1) {
		log_error("VG uuid parameter is required");
		return EINVALID_CMD_LINE;
	}

	if (arg_sign_value(cmd, interval_ARG, SIGN_NONE) == SIGN_MINUS) {
		log_error("Argument to --interval cannot be negative");
		return EINVALID_CMD_LINE;
	}

	arg_count_increment(cmd, all_ARG);

	/* process other params here as well */

	return poll_vg(cmd, argv[0], arg_is_set(cmd, abort_ARG), arg_uint_value(cmd, interval_ARG,
					   find_config_tree_int(cmd, activation_polling_interval_CFG, NULL)));
}
