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

struct vgextend_params {
	struct pvcreate_params pp;
	int pv_count;
	const char *const *pv_names;
};

static int _restore_pv(struct volume_group *vg, const char *pv_name)
{
	struct pv_list *pvl = NULL;
	pvl = find_pv_in_vg(vg, pv_name);
	if (!pvl) {
		log_warn("WARNING: PV %s not found in VG %s", pv_name, vg->name);
		return 0;
	}

	if (!(pvl->pv->status & MISSING_PV)) {
		log_warn("WARNING: PV %s was not missing in VG %s", pv_name, vg->name);
		return 0;
	}

	if (!pvl->pv->dev) {
		log_warn("WARNING: The PV %s is still missing.", pv_name);
		return 0;
	}

	pvl->pv->status &= ~MISSING_PV;
	return 1;
}

static int vgextend_restore(struct cmd_context *cmd __attribute__((unused)), const char *vg_name,
			    struct volume_group *vg, void *handle)
{
	struct vgextend_params *vp = handle;
	int fixed = 0;
	int i;

	for (i = 0; i < vp->pv_count; i++) {
		if (_restore_pv(vg, vp->pv_names[i]))
			fixed++;
	}

	if (!fixed) {
		log_error("No PV has been restored.");
		return ECMD_FAILED;
	}

	if (!vg_write(vg) || !vg_commit(vg))
		return ECMD_FAILED;

	backup(vg);

	log_print_unless_silent("Volume group \"%s\" successfully extended", vg_name);

	return ECMD_PROCESSED;
}

static int vgextend_single(struct cmd_context *cmd, const char *vg_name,
			   struct volume_group *vg, void *handle)
{
	struct vgextend_params *vp = handle;
	struct pvcreate_params *pp = &vp->pp;
	uint32_t mda_copies;
	uint32_t mda_used;
	int ret = ECMD_FAILED;

	if (arg_count(cmd, metadataignore_ARG) &&
	    (pp->force == PROMPT) && !pp->yes &&
	    (vg_mda_copies(vg) != VGMETADATACOPIES_UNMANAGED) &&
	    (yes_no_prompt("Override preferred number of copies of VG %s metadata? [y/n]: ", vg_name) == 'n')) {
		log_error("Volume group %s not changed", vg_name);
		return ECMD_FAILED;
	}

	if (!lock_vol(cmd, VG_ORPHANS, LCK_VG_WRITE, NULL)) {
		log_error("Can't get lock for orphan PVs");
		return ECMD_FAILED;
	}

	if (!vg_extend(vg, vp->pv_count, vp->pv_names, pp))
		goto done;

	if (arg_count(cmd, metadataignore_ARG)) {
		mda_copies = vg_mda_copies(vg);
		mda_used = vg_mda_used_count(vg);

		if ((mda_copies != VGMETADATACOPIES_UNMANAGED) &&
		    (mda_copies != mda_used)) {
			log_warn("WARNING: Changing preferred number of copies of VG %s metadata from %"PRIu32" to %"PRIu32,
				 vg_name, mda_copies, mda_used);
			vg_set_mda_copies(vg, mda_used);
		}

	}

	log_verbose("Volume group \"%s\" will be extended by %d new physical volumes", vg_name, vp->pv_count);

	if (!vg_write(vg) || !vg_commit(vg))
		goto done;

	backup(vg);

	log_print_unless_silent("Volume group \"%s\" successfully extended", vg_name);
	ret = ECMD_PROCESSED;
done:
	unlock_vg(cmd, VG_ORPHANS);
	return ret;

}

int vgextend(struct cmd_context *cmd, int argc, char **argv)
{
	struct vgextend_params vp;
	int restore = arg_is_set(cmd, restoremissing_ARG);

	if (!argc) {
		log_error("Please enter volume group name and "
			  "physical volume(s)");
		return EINVALID_CMD_LINE;
	}

	if (arg_count(cmd, metadatacopies_ARG)) {
		log_error("Invalid option --metadatacopies, "
			  "use --pvmetadatacopies instead.");
		return EINVALID_CMD_LINE;
	}

	pvcreate_params_set_defaults(&vp.pp);
	vp.pv_count = argc - 1;
	vp.pv_names = (const char* const*)(argv + 1);

	if (!pvcreate_params_validate(cmd, vp.pv_count, &vp.pp))
		return EINVALID_CMD_LINE;

	/*
	 * It is always ok to add new PVs to a VG - even if there are
	 * missing PVs.  No LVs are affected by this operation, but
	 * repair processes - particularly for RAID segtypes - can
	 * be facilitated.
	 */
	cmd->handles_missing_pvs = 1;

	return process_each_vg(cmd, argc, argv,
			       READ_FOR_UPDATE | ONLY_FIRST_NAME, &vp,
			       restore ? &vgextend_restore : &vgextend_single);
}
