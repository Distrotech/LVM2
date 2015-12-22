/*
 * Copyright (C) 2001-2004 Sistina Software, Inc. All rights reserved.
 * Copyright (C) 2004-2009 Red Hat, Inc. All rights reserved.
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

void pvcreate_each_params_set_defaults(struct pvcreate_each_params *pp)
{
	pp->zero = 1;
	pp->size = 0;
	pp->data_alignment = UINT64_C(0);
	pp->data_alignment_offset = UINT64_C(0);
	pp->pvmetadatacopies = DEFAULT_PVMETADATACOPIES;
	pp->pvmetadatasize = DEFAULT_PVMETADATASIZE;
	pp->labelsector = DEFAULT_LABELSECTOR;
	pp->force = PROMPT;
	pp->yes = 0;
	pp->metadataignore = DEFAULT_PVMETADATAIGNORE;
	pp->restorefile = NULL;
	pp->uuid_str = NULL;
	pp->ba_start = 0;
	pp->ba_size = 0;
	pp->pe_start = PV_PE_START_CALC;
	pp->extent_count = 0;
	pp->extent_size = 0;

	dm_list_init(&pp->prompts);
	dm_list_init(&pp->arg_devices);
	dm_list_init(&pp->arg_create);
	dm_list_init(&pp->arg_fail);
	dm_list_init(&pp->pvs);
}

int pvcreate_each_params_from_args(struct cmd_context *cmd, struct pvcreate_each_params *pp)
{
	pp->yes = arg_count(cmd, yes_ARG);
	pp->force = (force_t) arg_count(cmd, force_ARG);

	if (arg_int_value(cmd, labelsector_ARG, 0) >= LABEL_SCAN_SECTORS) {
		log_error("labelsector must be less than %lu.",
			  LABEL_SCAN_SECTORS);
		return 0;
	} else {
		pp->labelsector = arg_int64_value(cmd, labelsector_ARG,
						  DEFAULT_LABELSECTOR);
	}

	if (!(cmd->fmt->features & FMT_MDAS) &&
	    (arg_count(cmd, pvmetadatacopies_ARG) ||
	     arg_count(cmd, metadatasize_ARG)   ||
	     arg_count(cmd, dataalignment_ARG)  ||
	     arg_count(cmd, dataalignmentoffset_ARG))) {
		log_error("Metadata and data alignment parameters only "
			  "apply to text format.");
		return 0;
	}

	if (!(cmd->fmt->features & FMT_BAS) &&
	    arg_count(cmd, bootloaderareasize_ARG)) {
		log_error("Bootloader area parameters only "
			  "apply to text format.");
		return 0;
	}

	if (arg_count(cmd, metadataignore_ARG))
		pp->metadataignore = arg_int_value(cmd, metadataignore_ARG,
						   DEFAULT_PVMETADATAIGNORE);
	else
		pp->metadataignore = find_config_tree_bool(cmd, metadata_pvmetadataignore_CFG, NULL);

	if (arg_count(cmd, pvmetadatacopies_ARG) &&
	    !arg_int_value(cmd, pvmetadatacopies_ARG, -1) &&
	    pp->metadataignore) {
		log_error("metadataignore only applies to metadatacopies > 0");
		return 0;
	}

	pp->zero = arg_int_value(cmd, zero_ARG, 1);

	if (arg_sign_value(cmd, dataalignment_ARG, SIGN_NONE) == SIGN_MINUS) {
		log_error("Physical volume data alignment may not be negative.");
		return 0;
	}
	pp->data_alignment = arg_uint64_value(cmd, dataalignment_ARG, UINT64_C(0));

	if (pp->data_alignment > UINT32_MAX) {
		log_error("Physical volume data alignment is too big.");
		return 0;
	}

	if (arg_sign_value(cmd, dataalignmentoffset_ARG, SIGN_NONE) == SIGN_MINUS) {
		log_error("Physical volume data alignment offset may not be negative");
		return 0;
	}
	pp->data_alignment_offset = arg_uint64_value(cmd, dataalignmentoffset_ARG, UINT64_C(0));

	if (pp->data_alignment_offset > UINT32_MAX) {
		log_error("Physical volume data alignment offset is too big.");
		return 0;
	}

	if ((pp->data_alignment + pp->data_alignment_offset) &&
	    (pp->pe_start != PV_PE_START_CALC)) {
		if ((pp->data_alignment ? pp->pe_start % pp->data_alignment : pp->pe_start) != pp->data_alignment_offset) {
			log_warn("WARNING: Ignoring data alignment %s"
				 " incompatible with restored pe_start value %s)",
				 display_size(cmd, pp->data_alignment + pp->data_alignment_offset),
				 display_size(cmd, pp->pe_start));
			pp->data_alignment = 0;
			pp->data_alignment_offset = 0;
		}
	}

	if (arg_sign_value(cmd, metadatasize_ARG, SIGN_NONE) == SIGN_MINUS) {
		log_error("Metadata size may not be negative.");
		return 0;
	}

	if (arg_sign_value(cmd, bootloaderareasize_ARG, SIGN_NONE) == SIGN_MINUS) {
		log_error("Bootloader area size may not be negative.");
		return 0;
	}

	pp->pvmetadatasize = arg_uint64_value(cmd, metadatasize_ARG, UINT64_C(0));
	if (!pp->pvmetadatasize)
		pp->pvmetadatasize = find_config_tree_int(cmd, metadata_pvmetadatasize_CFG, NULL);

	pp->pvmetadatacopies = arg_int_value(cmd, pvmetadatacopies_ARG, -1);
	if (pp->pvmetadatacopies < 0)
		pp->pvmetadatacopies = find_config_tree_int(cmd, metadata_pvmetadatacopies_CFG, NULL);

	if (pp->pvmetadatacopies > 2) {
		log_error("Metadatacopies may only be 0, 1 or 2");
		return 0;
	}

	pp->ba_size = arg_uint64_value(cmd, bootloaderareasize_ARG, pp->ba_size);

	return 1;
}

/*
 * Intial sanity checking of recovery-related command-line arguments.
 * These args are: --restorefile, --uuid, and --physicalvolumesize
 *
 * Output arguments:
 * pp: structure allocated by caller, fields written / validated here
 */
static int pvcreate_each_restore_params_from_args(struct cmd_context *cmd, int argc,
					          struct pvcreate_each_params *pp)
{
	const char *uuid = NULL;

	pp->restorefile = arg_str_value(cmd, restorefile_ARG, NULL);

	if (arg_count(cmd, restorefile_ARG) && !arg_count(cmd, uuidstr_ARG)) {
		log_error("--uuid is required with --restorefile");
		return 0;
	}

	if (!arg_count(cmd, restorefile_ARG) && arg_count(cmd, uuidstr_ARG)) {
		if (!arg_count(cmd, norestorefile_ARG) &&
		    find_config_tree_bool(cmd, devices_require_restorefile_with_uuid_CFG, NULL)) {
			log_error("--restorefile is required with --uuid");
			return 0;
		}
	}

	if (arg_count(cmd, uuidstr_ARG) && argc != 1) {
		log_error("Can only set uuid on one volume at once");
		return 0;
	}

	if (arg_count(cmd, uuidstr_ARG)) {
		pp->uuid_str = arg_str_value(cmd, uuidstr_ARG, "");
		if (!id_read_format(&pp->id, uuid))
			return 0;
	}

	if (arg_sign_value(cmd, physicalvolumesize_ARG, SIGN_NONE) == SIGN_MINUS) {
		log_error("Physical volume size may not be negative");
		return 0;
	}
	pp->size = arg_uint64_value(cmd, physicalvolumesize_ARG, UINT64_C(0));

	if (arg_count(cmd, restorefile_ARG) || arg_count(cmd, uuidstr_ARG))
		pp->zero = 0;
	return 1;
}

static int pvcreate_each_restore_params_from_backup(struct cmd_context *cmd,
					            struct pvcreate_each_params *pp)
{
	struct volume_group *vg;
	struct pv_list *existing_pvl;
	const char *uuid;

	/*
	 * When restoring a PV, params need to be read from a backup file.
	 */
	if (!pp->restorefile)
		return 1;

	uuid = arg_str_value(cmd, uuidstr_ARG, "");

	if (!(vg = backup_read_vg(cmd, NULL, pp->restorefile))) {
		log_error("Unable to read volume group from %s", pp->restorefile);
		return 0;
	}

	if (!(existing_pvl = find_pv_in_vg_by_uuid(vg, &pp->id))) {
		release_vg(vg);
		log_error("Can't find uuid %s in backup file %s",
			  uuid, pp->restorefile);
		return 0;
	}

	pp->ba_start = pv_ba_start(existing_pvl->pv);
	pp->ba_size = pv_ba_size(existing_pvl->pv);
	pp->pe_start = pv_pe_start(existing_pvl->pv);
	pp->extent_size = pv_pe_size(existing_pvl->pv);
	pp->extent_count = pv_pe_count(existing_pvl->pv);

	release_vg(vg);
	return 1;
}

int pvcreate(struct cmd_context *cmd, int argc, char **argv)
{
	struct pvcreate_each_params pp = { 0 };
	int ret;

	if (!argc) {
		log_error("Please enter a physical volume path.");
		return 0;
	}

	/*
	 * Five kinds of pvcreate param values:
	 * 1. defaults
	 * 2. normal command line args
	 * 3. recovery-related command line args
	 * 4. recovery-related args from backup file
	 * 5. argc/argv free args specifying devices
	 */

	pvcreate_each_params_set_defaults(&pp);

	if (!pvcreate_each_params_from_args(cmd, &pp))
		return EINVALID_CMD_LINE;

	if (!pvcreate_each_restore_params_from_args(cmd, argc, &pp))
		return EINVALID_CMD_LINE;

	if (!pvcreate_each_restore_params_from_backup(cmd, &pp))
		return EINVALID_CMD_LINE;

	pp.pv_count = argc;
	pp.pv_names = argv;

	/*
	 * Needed to change the set of orphan PVs.
	 * (disable afterward to prevent process_each_pv from doing
	 * a shared global lock since it's already acquired it ex.)
	 */
	if (!lockd_gl(cmd, "ex", 0))
		return_ECMD_FAILED;
	cmd->lockd_gl_disable = 1;

	ret = pvcreate_each_device(cmd, &pp);

	return ret;
}
