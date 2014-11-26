/*
 * Copyright (C) 2001-2004 Sistina Software, Inc. All rights reserved.
 * Copyright (C) 2004-2007 Red Hat, Inc. All rights reserved.
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

struct pvchange_params {
	unsigned done;
	unsigned total;
};

static int _pvchange_single(struct cmd_context *cmd, struct volume_group *vg,
			    struct physical_volume *pv,
			    void *handle __attribute__((unused)))
{
	struct pvchange_params *params = handle;
	const char *pv_name = pv_dev_name(pv);
	char uuid[64] __attribute__((aligned(8)));

	int allocatable = arg_int_value(cmd, allocatable_ARG, 0);
	int mda_ignore = arg_int_value(cmd, metadataignore_ARG, 0);
	int tagargs = arg_count(cmd, addtag_ARG) + arg_count(cmd, deltag_ARG);

	params->total++;

	/* If in a VG, must change using volume group. */
	if (!is_orphan(pv)) {
		if (tagargs && !(vg->fid->fmt->features & FMT_TAGS)) {
			log_error("Volume group containing %s does not "
				  "support tags", pv_name);
			return ECMD_FAILED;
		}
		if (arg_count(cmd, uuid_ARG) && lvs_in_vg_activated(vg)) {
			log_error("Volume group containing %s has active "
				  "logical volumes", pv_name);
			return ECMD_FAILED;
		}
		if (!archive(vg))
			return ECMD_FAILED;
	} else {
		if (tagargs) {
			log_error("Can't change tag on Physical Volume %s not "
				  "in volume group", pv_name);
			return ECMD_FAILED;
		}
	}

	if (arg_count(cmd, allocatable_ARG)) {
		if (is_orphan(pv) &&
		    !(pv->fmt->features & FMT_ORPHAN_ALLOCATABLE)) {
			log_error("Allocatability not supported by orphan "
				  "%s format PV %s", pv->fmt->name, pv_name);
			return ECMD_FAILED;
		}

		/* change allocatability for a PV */
		if (allocatable && (pv_status(pv) & ALLOCATABLE_PV)) {
			log_warn("Physical volume \"%s\" is already "
				 "allocatable.", pv_name);
			params->done++;
			return ECMD_PROCESSED;
		}

		if (!allocatable && !(pv_status(pv) & ALLOCATABLE_PV)) {
			log_warn("Physical volume \"%s\" is already "
				 "unallocatable.", pv_name);
			params->done++;
			return ECMD_PROCESSED;
		}

		if (allocatable) {
			log_verbose("Setting physical volume \"%s\" "
				    "allocatable", pv_name);
			pv->status |= ALLOCATABLE_PV;
		} else {
			log_verbose("Setting physical volume \"%s\" NOT "
				    "allocatable", pv_name);
			pv->status &= ~ALLOCATABLE_PV;
		}
	}

	/* Convert sh to ex.  gl only needed for orphans. */
	if (is_orphan(pv) && !lockd_gl(cmd, "ex", 0))
		return_ECMD_FAILED;

	if (tagargs) {
		/* tag or deltag */
		if (arg_count(cmd, addtag_ARG) && !change_tag(cmd, NULL, NULL, pv, addtag_ARG))
			return_ECMD_FAILED;

		if (arg_count(cmd, deltag_ARG) && !change_tag(cmd, NULL, NULL, pv, deltag_ARG))
			return_ECMD_FAILED;
 
	}

	if (arg_count(cmd, metadataignore_ARG)) {
		if ((vg_mda_copies(vg) != VGMETADATACOPIES_UNMANAGED) &&
		    (arg_count(cmd, force_ARG) == PROMPT) &&
		    yes_no_prompt("Override preferred number of copies "
				  "of VG %s metadata? [y/n]: ",
				  pv_vg_name(pv)) == 'n') {
			log_error("Physical volume %s not changed", pv_name);
			return ECMD_FAILED;
		}
		if (!pv_change_metadataignore(pv, mda_ignore))
			return_ECMD_FAILED;
	} 

	if (arg_count(cmd, uuid_ARG)) {
		/* --uuid: Change PV ID randomly */
		memcpy(&pv->old_id, &pv->id, sizeof(pv->id));
		if (!id_create(&pv->id)) {
			log_error("Failed to generate new random UUID for %s.",
				  pv_name);
			return ECMD_FAILED;
		}
		if (!id_write_format(&pv->id, uuid, sizeof(uuid)))
			return ECMD_FAILED;
		log_verbose("Changing uuid of %s to %s.", pv_name, uuid);
		if (!is_orphan(pv) && (!pv_write(cmd, pv, 1))) {
			log_error("pv_write with new uuid failed "
				  "for %s.", pv_name);
			return ECMD_FAILED;
		}
	}

	log_verbose("Updating physical volume \"%s\"", pv_name);
	if (!is_orphan(pv)) {
		if (!vg_write(vg) || !vg_commit(vg)) {
			log_error("Failed to store physical volume \"%s\" in "
				  "volume group \"%s\"", pv_name, vg->name);
			return ECMD_FAILED;
		}
		backup(vg);
	} else if (!(pv_write(cmd, pv, 0))) {
		log_error("Failed to store physical volume \"%s\"",
			  pv_name);
		return ECMD_FAILED;
	}

	log_print_unless_silent("Physical volume \"%s\" changed", pv_name);

	params->done++;
	return ECMD_PROCESSED;
}

int pvchange(struct cmd_context *cmd, int argc, char **argv)
{
	struct pvchange_params params;
	const char *done_s;
	const char *undone_s;
	int ret;

	if (!(arg_count(cmd, allocatable_ARG) + arg_is_set(cmd, addtag_ARG) +
	    arg_is_set(cmd, deltag_ARG) + arg_count(cmd, uuid_ARG) +
	    arg_count(cmd, metadataignore_ARG))) {
		log_error("Please give one or more of -x, -uuid, "
			  "--addtag, --deltag or --metadataignore");
		return EINVALID_CMD_LINE;
	}

	if (!(arg_count(cmd, all_ARG)) && !argc) {
		log_error("Please give a physical volume path");
		return EINVALID_CMD_LINE;
	}

	if (arg_count(cmd, all_ARG) && argc) {
		log_error("Option --all and PhysicalVolumePath are exclusive.");
		return EINVALID_CMD_LINE;
	}

	if (!lockd_gl(cmd, "sh", 0))
		return_ECMD_FAILED;

	params.done = 0;
	params.total = 0;

	if (!argc) {
		/*
		 * Take the global lock here so the lvmcache remains
		 * consistent across orphan/non-orphan vg locks.  If we don't
		 * take the lock here, pvs with 0 mdas in a non-orphan VG will
		 * be processed twice.
		 */
		if (!lock_vol(cmd, VG_GLOBAL, LCK_VG_WRITE, NULL)) {
			log_error("Unable to obtain global lock.");
			return ECMD_FAILED;
		}
	}

	ret = process_each_pv(cmd, argc, argv, NULL, READ_FOR_UPDATE, &params,
			      _pvchange_single);

	done_s = params.done == 1 ? "" : "s";
	undone_s = (params.total - params.done) == 1 ? "" : "s";

	log_print_unless_silent("%d physical volume%s changed / %d physical volume%s not changed",
				params.done, done_s,
				params.total - params.done, undone_s);

	return ret;
}
