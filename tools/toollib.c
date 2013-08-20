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
#include <sys/stat.h>

const char *command_name(struct cmd_context *cmd)
{
	return cmd->command->name;
}

/*
 * Strip dev_dir if present
 */
const char *skip_dev_dir(struct cmd_context *cmd, const char *vg_name,
		   unsigned *dev_dir_found)
{
	const char *dmdir = dm_dir();
	size_t dmdir_len = strlen(dmdir), vglv_sz;
	char *vgname, *lvname, *layer, *vglv;

	/* FIXME Do this properly */
	if (*vg_name == '/') {
		while (*vg_name == '/')
			vg_name++;
		vg_name--;
	}

	/* Reformat string if /dev/mapper found */
	if (!strncmp(vg_name, dmdir, dmdir_len) && vg_name[dmdir_len] == '/') {
		if (dev_dir_found)
			*dev_dir_found = 1;
		vg_name += dmdir_len;
		while (*vg_name == '/')
			vg_name++;

		if (!dm_split_lvm_name(cmd->mem, vg_name, &vgname, &lvname, &layer) ||
		    *layer) {
			log_error("skip_dev_dir: Couldn't split up device name %s",
				  vg_name);
			return vg_name;
		}
		vglv_sz = strlen(vgname) + strlen(lvname) + 2;
		if (!(vglv = dm_pool_alloc(cmd->mem, vglv_sz)) ||
		    dm_snprintf(vglv, vglv_sz, "%s%s%s", vgname,
				 *lvname ? "/" : "",
				 lvname) < 0) {
			log_error("vg/lv string alloc failed");
			return vg_name;
		}
		return vglv;
	}

	if (!strncmp(vg_name, cmd->dev_dir, strlen(cmd->dev_dir))) {
		if (dev_dir_found)
			*dev_dir_found = 1;
		vg_name += strlen(cmd->dev_dir);
		while (*vg_name == '/')
			vg_name++;
	} else if (dev_dir_found)
		*dev_dir_found = 0;

	return vg_name;
}

/*
 * Metadata iteration functions
 */
int process_each_lv_in_vg(struct cmd_context *cmd,
			  struct volume_group *vg,
			  const struct dm_list *arg_lvnames,
			  const struct dm_list *tags,
			  void *handle,
			  process_single_lv_fn_t process_single_lv)
{
	int ret_max = ECMD_PROCESSED;
	int ret = 0;
	unsigned process_all = 0;
	unsigned process_lv = 0;
	unsigned tags_supplied = 0;
	unsigned lvargs_supplied = 0;
	unsigned lvargs_matched = 0;
	struct lv_list *lvl;

	if (!vg_check_status(vg, EXPORTED_VG))
		return_ECMD_FAILED;

	if (tags && !dm_list_empty(tags))
		tags_supplied = 1;

	if (arg_lvnames && !dm_list_empty(arg_lvnames))
		lvargs_supplied = 1;

	/* Process all LVs in this VG if no restrictions given */
	if (!tags_supplied && !lvargs_supplied)
		process_all = 1;

	/* Or if VG tags match */
	if (!process_lv && tags_supplied &&
	    str_list_match_list(tags, &vg->tags, NULL)) {
		process_all = 1;
	}

	/*
	 * FIXME: In case of remove it goes through deleted entries,
	 * but it works since entries are allocated from vg mem pool.
	 */
	dm_list_iterate_items(lvl, &vg->lvs) {
		if (lvl->lv->status & SNAPSHOT)
			continue;

		/* Skip availability change for non-virt snaps when processing all LVs */
		/* FIXME: pass process_all to process_single_lv() */
		if (process_all && arg_count(cmd, activate_ARG) &&
		    lv_is_cow(lvl->lv) && !lv_is_virtual_origin(origin_from_cow(lvl->lv)))
			continue;

		if (lv_is_virtual_origin(lvl->lv) && !arg_count(cmd, all_ARG))
			continue;

		/*
		 * Only let hidden LVs through it --all was used or the LVs 
		 * were specifically named on the command line.
		 */
		if (!lvargs_supplied && !lv_is_visible(lvl->lv) && !arg_count(cmd, all_ARG))
			continue;

		/* Should we process this LV? */
		if (process_all)
			process_lv = 1;
		else
			process_lv = 0;

		/* LV tag match? */
		if (!process_lv && tags_supplied &&
		    str_list_match_list(tags, &lvl->lv->tags, NULL)) {
			process_lv = 1;
		}

		/* LV name match? */
		if (lvargs_supplied &&
		    str_list_match_item(arg_lvnames, lvl->lv->name)) {
			process_lv = 1;
			lvargs_matched++;
		}

		if (!process_lv)
			continue;

		if (sigint_caught())
			return_ECMD_FAILED;

		ret = process_single_lv(cmd, lvl->lv, handle);

		if (ret > ret_max)
			ret_max = ret;
	}

	if (lvargs_supplied && lvargs_matched != dm_list_size(arg_lvnames)) {
		/*
		 * FIXME: lvm supports removal of LV with all its dependencies
		 * this leads to miscalculation that depends on the order of args.
		 */
		log_error("One or more specified logical volume(s) not found.");
		if (ret_max < ECMD_FAILED)
			ret_max = ECMD_FAILED;
	}

	return ret_max;
}

int process_each_segment_in_pv(struct cmd_context *cmd,
			       struct volume_group *vg,
			       struct physical_volume *pv,
			       void *handle,
			       process_single_pvseg_fn_t process_single_pvseg)
{
	struct pv_segment *pvseg;
	struct pv_list *pvl;
	const char *vg_name = NULL;
	int ret_max = ECMD_PROCESSED;
	int ret;
	struct volume_group *old_vg = vg;
	struct pv_segment _free_pv_segment = { .pv = pv };

	if (is_pv(pv) && !vg && !is_orphan(pv)) {
		vg_name = pv_vg_name(pv);

		vg = vg_read(cmd, vg_name, NULL, 0);
		if (vg_read_error(vg)) {
			release_vg(vg);
			log_error("Skipping volume group %s", vg_name);
			return ECMD_FAILED;
		}

		/*
		 * Replace possibly incomplete PV structure with new one
		 * allocated in vg_read_internal() path.
		 */
		if (!(pvl = find_pv_in_vg(vg, pv_dev_name(pv)))) {
			 log_error("Unable to find %s in volume group %s",
				   pv_dev_name(pv), vg_name);
			 unlock_and_release_vg(cmd, vg, vg_name);
			 return ECMD_FAILED;
		}

		pv = pvl->pv;
	}

	if (dm_list_empty(&pv->segments)) {
		ret = process_single_pvseg(cmd, NULL, &_free_pv_segment, handle);
		if (ret > ret_max)
			ret_max = ret;
	} else
		dm_list_iterate_items(pvseg, &pv->segments) {
			if (sigint_caught()) {
				ret_max = ECMD_FAILED;
				stack;
				break;
			}
			ret = process_single_pvseg(cmd, vg, pvseg, handle);
			if (ret > ret_max)
				ret_max = ret;
		}

	if (vg_name)
		unlock_vg(cmd, vg_name);
	if (!old_vg)
		release_vg(vg);

	return ret_max;
}

int process_each_segment_in_lv(struct cmd_context *cmd,
			       struct logical_volume *lv,
			       void *handle,
			       process_single_seg_fn_t process_single_seg)
{
	struct lv_segment *seg;
	int ret_max = ECMD_PROCESSED;
	int ret;

	dm_list_iterate_items(seg, &lv->segments) {
		if (sigint_caught())
			return_ECMD_FAILED;
		ret = process_single_seg(cmd, seg, handle);
		if (ret > ret_max)
			ret_max = ret;
	}

	return ret_max;
}

int process_each_pv_in_vg(struct cmd_context *cmd, struct volume_group *vg,
			  void *handle, process_single_pv_fn_t process_single_pv)
{
	int ret_max = ECMD_PROCESSED;
	int ret = 0;
	struct pv_list *pvl;

	dm_list_iterate_items(pvl, &vg->pvs) {
		if (sigint_caught())
			return_ECMD_FAILED;
		ret = process_single_pv(cmd, vg, pvl->pv, handle);
		if (ret > ret_max)
			ret_max = ret;
		if (sigint_caught())
			break;
	}

	return ret_max;
}

/*
 * Determine volume group name from a logical volume name
 */
const char *extract_vgname(struct cmd_context *cmd, const char *lv_name)
{
	const char *vg_name = lv_name;
	char *st;
	char *dev_dir = cmd->dev_dir;

	/* Path supplied? */
	if (vg_name && strchr(vg_name, '/')) {
		/* Strip dev_dir (optional) */
		if (*vg_name == '/') {
			while (*vg_name == '/')
				vg_name++;
			vg_name--;
		}
		if (!strncmp(vg_name, dev_dir, strlen(dev_dir))) {
			vg_name += strlen(dev_dir);
			while (*vg_name == '/')
				vg_name++;
		}
		if (*vg_name == '/') {
			log_error("\"%s\": Invalid path for Logical "
				  "Volume", lv_name);
			return 0;
		}

		/* Require exactly one set of consecutive slashes */
		if ((st = strchr(vg_name, '/')))
			while (*st == '/')
				st++;

		if (!st || strchr(st, '/')) {
			log_error("\"%s\": Invalid path for Logical Volume",
				  lv_name);
			return 0;
		}

		vg_name = dm_pool_strdup(cmd->mem, vg_name);
		if (!vg_name) {
			log_error("Allocation of vg_name failed");
			return 0;
		}

		*strchr(vg_name, '/') = '\0';
		return vg_name;
	}

	if (!(vg_name = default_vgname(cmd))) {
		if (lv_name)
			log_error("Path required for Logical Volume \"%s\"",
				  lv_name);
		return 0;
	}

	return vg_name;
}

/*
 * Extract default volume group name from environment
 */
char *default_vgname(struct cmd_context *cmd)
{
	const char *vg_path;

	/* Take default VG from environment? */
	vg_path = getenv("LVM_VG_NAME");
	if (!vg_path)
		return 0;

	vg_path = skip_dev_dir(cmd, vg_path, NULL);

	if (strchr(vg_path, '/')) {
		log_error("Environment Volume Group in LVM_VG_NAME invalid: "
			  "\"%s\"", vg_path);
		return 0;
	}

	return dm_pool_strdup(cmd->mem, vg_path);
}

/*
 * Process physical extent range specifiers
 */
static int _add_pe_range(struct dm_pool *mem, const char *pvname,
			 struct dm_list *pe_ranges, uint32_t start, uint32_t count)
{
	struct pe_range *per;

	log_debug("Adding PE range: start PE %" PRIu32 " length %" PRIu32
		  " on %s", start, count, pvname);

	/* Ensure no overlap with existing areas */
	dm_list_iterate_items(per, pe_ranges) {
		if (((start < per->start) && (start + count - 1 >= per->start))
		    || ((start >= per->start) &&
			(per->start + per->count - 1) >= start)) {
			log_error("Overlapping PE ranges specified (%" PRIu32
				  "-%" PRIu32 ", %" PRIu32 "-%" PRIu32 ")"
				  " on %s",
				  start, start + count - 1, per->start,
				  per->start + per->count - 1, pvname);
			return 0;
		}
	}

	if (!(per = dm_pool_alloc(mem, sizeof(*per)))) {
		log_error("Allocation of list failed");
		return 0;
	}

	per->start = start;
	per->count = count;
	dm_list_add(pe_ranges, &per->list);

	return 1;
}

static int xstrtouint32(const char *s, char **p, int base, uint32_t *result)
{
	unsigned long ul;

	errno = 0;
	ul = strtoul(s, p, base);
	if (errno || *p == s || ul > UINT32_MAX)
		return 0;
	*result = ul;

	return 1;
}

static int _parse_pes(struct dm_pool *mem, char *c, struct dm_list *pe_ranges,
		      const char *pvname, uint32_t size)
{
	char *endptr;
	uint32_t start, end;

	/* Default to whole PV */
	if (!c) {
		if (!_add_pe_range(mem, pvname, pe_ranges, UINT32_C(0), size))
			return_0;
		return 1;
	}

	while (*c) {
		if (*c != ':')
			goto error;

		c++;

		/* Disallow :: and :\0 */
		if (*c == ':' || !*c)
			goto error;

		/* Default to whole range */
		start = UINT32_C(0);
		end = size - 1;

		/* Start extent given? */
		if (isdigit(*c)) {
			if (!xstrtouint32(c, &endptr, 10, &start))
				goto error;
			c = endptr;
			/* Just one number given? */
			if (!*c || *c == ':')
				end = start;
		}
		/* Range? */
		if (*c == '-') {
			c++;
			if (isdigit(*c)) {
				if (!xstrtouint32(c, &endptr, 10, &end))
					goto error;
				c = endptr;
			}
		}
		if (*c && *c != ':')
			goto error;

		if ((start > end) || (end > size - 1)) {
			log_error("PE range error: start extent %" PRIu32 " to "
				  "end extent %" PRIu32, start, end);
			return 0;
		}

		if (!_add_pe_range(mem, pvname, pe_ranges, start, end - start + 1))
			return_0;

	}

	return 1;

      error:
	log_error("Physical extent parsing error at %s", c);
	return 0;
}

static int _create_pv_entry(struct dm_pool *mem, struct pv_list *pvl,
			     char *colon, int allocatable_only, struct dm_list *r)
{
	const char *pvname;
	struct pv_list *new_pvl = NULL, *pvl2;
	struct dm_list *pe_ranges;

	pvname = pv_dev_name(pvl->pv);
	if (allocatable_only && !(pvl->pv->status & ALLOCATABLE_PV)) {
		log_warn("Physical volume %s not allocatable.", pvname);
		return 1;
	}

	if (allocatable_only && is_missing_pv(pvl->pv)) {
		log_warn("Physical volume %s is missing.", pvname);
		return 1;
	}

	if (allocatable_only &&
	    (pvl->pv->pe_count == pvl->pv->pe_alloc_count)) {
		log_warn("No free extents on physical volume \"%s\".", pvname);
		return 1;
	}

	dm_list_iterate_items(pvl2, r)
		if (pvl->pv->dev == pvl2->pv->dev) {
			new_pvl = pvl2;
			break;
		}

	if (!new_pvl) {
		if (!(new_pvl = dm_pool_alloc(mem, sizeof(*new_pvl)))) {
			log_error("Unable to allocate physical volume list.");
			return 0;
		}

		memcpy(new_pvl, pvl, sizeof(*new_pvl));

		if (!(pe_ranges = dm_pool_alloc(mem, sizeof(*pe_ranges)))) {
			log_error("Allocation of pe_ranges list failed");
			return 0;
		}
		dm_list_init(pe_ranges);
		new_pvl->pe_ranges = pe_ranges;
		dm_list_add(r, &new_pvl->list);
	}

	/* Determine selected physical extents */
	if (!_parse_pes(mem, colon, new_pvl->pe_ranges, pv_dev_name(pvl->pv),
			pvl->pv->pe_count))
		return_0;

	return 1;
}

struct dm_list *create_pv_list(struct dm_pool *mem, struct volume_group *vg, int argc,
			    char **argv, int allocatable_only)
{
	struct dm_list *r;
	struct pv_list *pvl;
	struct dm_list tags, arg_pvnames;
	char *pvname = NULL;
	char *colon, *at_sign, *tagname;
	int i;

	/* Build up list of PVs */
	if (!(r = dm_pool_alloc(mem, sizeof(*r)))) {
		log_error("Allocation of list failed");
		return NULL;
	}
	dm_list_init(r);

	dm_list_init(&tags);
	dm_list_init(&arg_pvnames);

	for (i = 0; i < argc; i++) {
		dm_unescape_colons_and_at_signs(argv[i], &colon, &at_sign);

		if (at_sign && (at_sign == argv[i])) {
			tagname = at_sign + 1;
			if (!validate_tag(tagname)) {
				log_error("Skipping invalid tag %s", tagname);
				continue;
			}
			dm_list_iterate_items(pvl, &vg->pvs) {
				if (str_list_match_item(&pvl->pv->tags,
							tagname)) {
					if (!_create_pv_entry(mem, pvl, NULL,
							      allocatable_only,
							      r))
						return_NULL;
				}
			}
			continue;
		}

		pvname = argv[i];

		if (colon && !(pvname = dm_pool_strndup(mem, pvname,
					(unsigned) (colon - pvname)))) {
			log_error("Failed to clone PV name");
			return NULL;
		}

		if (!(pvl = find_pv_in_vg(vg, pvname))) {
			log_error("Physical Volume \"%s\" not found in "
				  "Volume Group \"%s\"", pvname, vg->name);
			return NULL;
		}
		if (!_create_pv_entry(mem, pvl, colon, allocatable_only, r))
			return_NULL;
	}

	if (dm_list_empty(r))
		log_error("No specified PVs have space available");

	return dm_list_empty(r) ? NULL : r;
}

struct dm_list *clone_pv_list(struct dm_pool *mem, struct dm_list *pvsl)
{
	struct dm_list *r;
	struct pv_list *pvl, *new_pvl;

	/* Build up list of PVs */
	if (!(r = dm_pool_alloc(mem, sizeof(*r)))) {
		log_error("Allocation of list failed");
		return NULL;
	}
	dm_list_init(r);

	dm_list_iterate_items(pvl, pvsl) {
		if (!(new_pvl = dm_pool_zalloc(mem, sizeof(*new_pvl)))) {
			log_error("Unable to allocate physical volume list.");
			return NULL;
		}

		memcpy(new_pvl, pvl, sizeof(*new_pvl));
		dm_list_add(r, &new_pvl->list);
	}

	return r;
}

void vgcreate_params_set_defaults(struct vgcreate_params *vp_def,
				  struct volume_group *vg)
{
	if (vg) {
		vp_def->vg_name = NULL;
		vp_def->extent_size = vg->extent_size;
		vp_def->max_pv = vg->max_pv;
		vp_def->max_lv = vg->max_lv;
		vp_def->alloc = vg->alloc;
		vp_def->clustered = vg_is_clustered(vg);
		vp_def->vgmetadatacopies = vg->mda_copies;
	} else {
		vp_def->vg_name = NULL;
		vp_def->extent_size = DEFAULT_EXTENT_SIZE * 2;
		vp_def->max_pv = DEFAULT_MAX_PV;
		vp_def->max_lv = DEFAULT_MAX_LV;
		vp_def->alloc = DEFAULT_ALLOC_POLICY;
		vp_def->clustered = DEFAULT_CLUSTERED;
		vp_def->vgmetadatacopies = DEFAULT_VGMETADATACOPIES;
	}
}

/*
 * Set members of struct vgcreate_params from cmdline arguments.
 * Do preliminary validation with arg_*() interface.
 * Further, more generic validation is done in validate_vgcreate_params().
 * This function is to remain in tools directory.
 */
int vgcreate_params_set_from_args(struct cmd_context *cmd,
				  struct vgcreate_params *vp_new,
				  struct vgcreate_params *vp_def)
{
	vp_new->vg_name = skip_dev_dir(cmd, vp_def->vg_name, NULL);
	vp_new->max_lv = arg_uint_value(cmd, maxlogicalvolumes_ARG,
					vp_def->max_lv);
	vp_new->max_pv = arg_uint_value(cmd, maxphysicalvolumes_ARG,
					vp_def->max_pv);
	vp_new->alloc = (alloc_policy_t) arg_uint_value(cmd, alloc_ARG, vp_def->alloc);

	/* Units of 512-byte sectors */
	vp_new->extent_size =
	    arg_uint_value(cmd, physicalextentsize_ARG, vp_def->extent_size);

	if (arg_count(cmd, clustered_ARG))
		vp_new->clustered =
			!strcmp(arg_str_value(cmd, clustered_ARG,
					      vp_def->clustered ? "y":"n"), "y");
	else
		/* Default depends on current locking type */
		vp_new->clustered = locking_is_clustered();

	if (arg_sign_value(cmd, physicalextentsize_ARG, SIGN_NONE) == SIGN_MINUS) {
		log_error("Physical extent size may not be negative");
		return 0;
	}

	if (arg_uint64_value(cmd, physicalextentsize_ARG, 0) > MAX_EXTENT_SIZE) {
		log_error("Physical extent size cannot be larger than %s",
				  display_size(cmd, (uint64_t) MAX_EXTENT_SIZE));
		return 0;
	}

	if (arg_sign_value(cmd, maxlogicalvolumes_ARG, SIGN_NONE) == SIGN_MINUS) {
		log_error("Max Logical Volumes may not be negative");
		return 0;
	}

	if (arg_sign_value(cmd, maxphysicalvolumes_ARG, SIGN_NONE) == SIGN_MINUS) {
		log_error("Max Physical Volumes may not be negative");
		return 0;
	}

	if (arg_count(cmd, metadatacopies_ARG)) {
		vp_new->vgmetadatacopies = arg_int_value(cmd, metadatacopies_ARG,
							DEFAULT_VGMETADATACOPIES);
	} else if (arg_count(cmd, vgmetadatacopies_ARG)) {
		vp_new->vgmetadatacopies = arg_int_value(cmd, vgmetadatacopies_ARG,
							DEFAULT_VGMETADATACOPIES);
	} else {
		vp_new->vgmetadatacopies = find_config_tree_int(cmd, metadata_vgmetadatacopies_CFG, NULL);
	}

	return 1;
}

/* Shared code for changing activation state for vgchange/lvchange */
int lv_change_activate(struct cmd_context *cmd, struct logical_volume *lv,
		       activation_change_t activate)
{
	if (!lv_active_change(cmd, lv, activate))
		return_0;

	if (background_polling() &&
	    (activate != CHANGE_AN) &&
	    (activate != CHANGE_ALN) &&
	    (lv->status & (PVMOVE|CONVERTING|MERGING)))
		lv_spawn_background_polling(cmd, lv);

	return 1;
}

int lv_refresh(struct cmd_context *cmd, struct logical_volume *lv)
{
	int r = 0;

	if (!cmd->partial_activation && (lv->status & PARTIAL_LV)) {
		log_error("Refusing refresh of partial LV %s. Use --partial to override.",
			  lv->name);
		goto out;
	}

	r = suspend_lv(cmd, lv);
	if (!r)
		goto_out;

	r = resume_lv(cmd, lv);
	if (!r)
		goto_out;

	/*
	 * check if snapshot merge should be polled
	 * - unfortunately: even though the dev_manager will clear
	 *   the lv's merge attributes if a merge is not possible;
	 *   it is clearing a different instance of the lv (as
	 *   retrieved with lv_from_lvid)
	 * - fortunately: polldaemon will immediately shutdown if the
	 *   origin doesn't have a status with a snapshot percentage
	 */
	if (background_polling() && lv_is_origin(lv) && lv_is_merging_origin(lv))
		lv_spawn_background_polling(cmd, lv);

out:
	return r;
}

int vg_refresh_visible(struct cmd_context *cmd, struct volume_group *vg)
{
	struct lv_list *lvl;
	int r = 1;

	sigint_allow();
	dm_list_iterate_items(lvl, &vg->lvs) {
		if (sigint_caught()) {
			r = 0;
			stack;
			break;
		}

		if (lv_is_visible(lvl->lv) && !lv_refresh(cmd, lvl->lv)) {
			r = 0;
			stack;
		}
	}

	sigint_restore();

	return r;
}

void lv_spawn_background_polling(struct cmd_context *cmd,
				 struct logical_volume *lv)
{
	const char *pvname;

	if ((lv->status & PVMOVE) &&
	    (pvname = get_pvmove_pvname_from_lv_mirr(lv))) {
		log_verbose("Spawning background pvmove process for %s",
			    pvname);
		pvmove_poll(cmd, pvname, 1);
	} else if ((lv->status & LOCKED) &&
	    (pvname = get_pvmove_pvname_from_lv(lv))) {
		log_verbose("Spawning background pvmove process for %s",
			    pvname);
		pvmove_poll(cmd, pvname, 1);
	}

	if (lv->status & (CONVERTING|MERGING)) {
		log_verbose("Spawning background lvconvert process for %s",
			lv->name);
		lvconvert_poll(cmd, lv, 1);
	}
}

/*
 * Intial sanity checking of non-recovery related command-line arguments.
 *
 * Output arguments:
 * pp: structure allocated by caller, fields written / validated here
 */
int pvcreate_params_validate(struct cmd_context *cmd,
			     int argc, char **argv,
			     struct pvcreate_params *pp)
{
	if (!argc) {
		log_error("Please enter a physical volume path");
		return 0;
	}

	pp->yes = arg_count(cmd, yes_ARG);
	pp->force = (force_t) arg_count(cmd, force_ARG);

	if (arg_int_value(cmd, labelsector_ARG, 0) >= LABEL_SCAN_SECTORS) {
		log_error("labelsector must be less than %lu",
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

	if (arg_count(cmd, pvmetadatacopies_ARG) &&
	    arg_int_value(cmd, pvmetadatacopies_ARG, -1) > 2) {
		log_error("Metadatacopies may only be 0, 1 or 2");
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

	if (arg_count(cmd, zero_ARG))
		pp->zero = strcmp(arg_str_value(cmd, zero_ARG, "y"), "n");

	if (arg_sign_value(cmd, dataalignment_ARG, SIGN_NONE) == SIGN_MINUS) {
		log_error("Physical volume data alignment may not be negative");
		return 0;
	}
	pp->data_alignment = arg_uint64_value(cmd, dataalignment_ARG, UINT64_C(0));

	if (pp->data_alignment > UINT32_MAX) {
		log_error("Physical volume data alignment is too big.");
		return 0;
	}

	if (pp->data_alignment && pp->rp.pe_start != PV_PE_START_CALC) {
		if (pp->rp.pe_start % pp->data_alignment)
			log_warn("WARNING: Ignoring data alignment %" PRIu64
				 " incompatible with --restorefile value (%"
				 PRIu64").", pp->data_alignment, pp->rp.pe_start);
		pp->data_alignment = 0;
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

	if (pp->data_alignment_offset && pp->rp.pe_start != PV_PE_START_CALC) {
		log_warn("WARNING: Ignoring data alignment offset %" PRIu64
			 " incompatible with --restorefile value (%"
			 PRIu64").", pp->data_alignment_offset, pp->rp.pe_start);
		pp->data_alignment_offset = 0;
	}

	if (arg_sign_value(cmd, metadatasize_ARG, SIGN_NONE) == SIGN_MINUS) {
		log_error("Metadata size may not be negative");
		return 0;
	}

	if (arg_sign_value(cmd, bootloaderareasize_ARG, SIGN_NONE) == SIGN_MINUS) {
		log_error("Bootloader area size may not be negative");
		return 0;
	}

	pp->pvmetadatasize = arg_uint64_value(cmd, metadatasize_ARG, UINT64_C(0));
	if (!pp->pvmetadatasize)
		pp->pvmetadatasize = find_config_tree_int(cmd, metadata_pvmetadatasize_CFG, NULL);

	pp->pvmetadatacopies = arg_int_value(cmd, pvmetadatacopies_ARG, -1);
	if (pp->pvmetadatacopies < 0)
		pp->pvmetadatacopies = find_config_tree_int(cmd, metadata_pvmetadatacopies_CFG, NULL);

	pp->rp.ba_size = arg_uint64_value(cmd, bootloaderareasize_ARG, pp->rp.ba_size);

	return 1;
}

int get_activation_monitoring_mode(struct cmd_context *cmd,
				   int *monitoring_mode)
{
	*monitoring_mode = DEFAULT_DMEVENTD_MONITOR;

	if (arg_count(cmd, monitor_ARG) &&
	    (arg_count(cmd, ignoremonitoring_ARG) ||
	     arg_count(cmd, sysinit_ARG))) {
		log_error("--ignoremonitoring or --sysinit option not allowed with --monitor option");
		return 0;
	}

	if (arg_count(cmd, monitor_ARG))
		*monitoring_mode = arg_int_value(cmd, monitor_ARG,
						 DEFAULT_DMEVENTD_MONITOR);
	else if (is_static() || arg_count(cmd, ignoremonitoring_ARG) ||
		 arg_count(cmd, sysinit_ARG) ||
		 !find_config_tree_bool(cmd, activation_monitoring_CFG, NULL))
		*monitoring_mode = DMEVENTD_MONITOR_IGNORE;

	return 1;
}

int get_pool_params(struct cmd_context *cmd,
		    struct profile *profile,
		    int *passed_args,
		    uint32_t *chunk_size,
		    thin_discards_t *discards,
		    uint64_t *pool_metadata_size,
		    int *zero)
{
	const char *dstr;

	*passed_args = 0;
	if (arg_count(cmd, zero_ARG)) {
		*passed_args |= PASS_ARG_ZERO;
		*zero = strcmp(arg_str_value(cmd, zero_ARG, "y"), "n");
		log_very_verbose("Setting pool zeroing: %u", *zero);
	} else
		*zero = find_config_tree_bool(cmd, allocation_thin_pool_zero_CFG, profile);

	if (arg_count(cmd, discards_ARG)) {
		*passed_args |= PASS_ARG_DISCARDS;
		*discards = (thin_discards_t) arg_uint_value(cmd, discards_ARG, 0);
		log_very_verbose("Setting pool discards: %s",
				 get_pool_discards_name(*discards));
	} else {
		dstr = find_config_tree_str(cmd, allocation_thin_pool_discards_CFG, profile);
		if (!get_pool_discards(dstr, discards))
			return_0;
	}

	if (arg_count(cmd, chunksize_ARG)) {
		if (arg_sign_value(cmd, chunksize_ARG, SIGN_NONE) == SIGN_MINUS) {
			log_error("Negative chunk size is invalid.");
			return 0;
		}
		*passed_args |= PASS_ARG_CHUNK_SIZE;
		*chunk_size = arg_uint_value(cmd, chunksize_ARG,
					     DM_THIN_MIN_DATA_BLOCK_SIZE);
		log_very_verbose("Setting pool chunk size: %s",
				 display_size(cmd, *chunk_size));
	} else
		*chunk_size = find_config_tree_int(cmd, allocation_thin_pool_chunk_size_CFG, profile) * 2;

	if ((*chunk_size < DM_THIN_MIN_DATA_BLOCK_SIZE) ||
	    (*chunk_size > DM_THIN_MAX_DATA_BLOCK_SIZE)) {
		log_error("Chunk size must be in the range %s to %s.",
			  display_size(cmd, DM_THIN_MIN_DATA_BLOCK_SIZE),
			  display_size(cmd, DM_THIN_MAX_DATA_BLOCK_SIZE));
		return 0;
	}

	if (arg_count(cmd, poolmetadatasize_ARG)) {
		if (arg_sign_value(cmd, poolmetadatasize_ARG, SIGN_NONE) == SIGN_MINUS) {
			log_error("Negative pool metadata size is invalid.");
			return 0;
		}
		*passed_args |= PASS_ARG_POOL_METADATA_SIZE;
	}
	*pool_metadata_size = arg_uint64_value(cmd, poolmetadatasize_ARG, UINT64_C(0));

	return 1;
}

/*
 * Generic stripe parameter checks.
 */
static int _validate_stripe_params(struct cmd_context *cmd, uint32_t *stripes,
				   uint32_t *stripe_size)
{
	if (*stripes == 1 && *stripe_size) {
		log_print_unless_silent("Ignoring stripesize argument with single stripe");
		*stripe_size = 0;
	}

	if (*stripes > 1 && !*stripe_size) {
		*stripe_size = find_config_tree_int(cmd, metadata_stripesize_CFG, NULL) * 2;
		log_print_unless_silent("Using default stripesize %s",
			  display_size(cmd, (uint64_t) *stripe_size));
	}

	if (*stripes < 1 || *stripes > MAX_STRIPES) {
		log_error("Number of stripes (%d) must be between %d and %d",
			  *stripes, 1, MAX_STRIPES);
		return 0;
	}

	if (*stripes > 1 && (*stripe_size < STRIPE_SIZE_MIN ||
			     *stripe_size & (*stripe_size - 1))) {
		log_error("Invalid stripe size %s",
			  display_size(cmd, (uint64_t) *stripe_size));
		return 0;
	}

	return 1;
}

/*
 * The stripe size is limited by the size of a uint32_t, but since the
 * value given by the user is doubled, and the final result must be a
 * power of 2, we must divide UINT_MAX by four and add 1 (to round it
 * up to the power of 2)
 */
int get_stripe_params(struct cmd_context *cmd, uint32_t *stripes, uint32_t *stripe_size)
{
	/* stripes_long_ARG takes precedence (for lvconvert) */
	*stripes = arg_uint_value(cmd, arg_count(cmd, stripes_long_ARG) ? stripes_long_ARG : stripes_ARG, 1);

	*stripe_size = arg_uint_value(cmd, stripesize_ARG, 0);
	if (*stripe_size) {
		if (arg_sign_value(cmd, stripesize_ARG, SIGN_NONE) == SIGN_MINUS) {
			log_error("Negative stripesize is invalid");
			return 0;
		}

		if(arg_uint64_value(cmd, stripesize_ARG, 0) > STRIPE_SIZE_LIMIT * 2) {
			log_error("Stripe size cannot be larger than %s",
				  display_size(cmd, (uint64_t) STRIPE_SIZE_LIMIT));
			return 0;
		}
	}

	return _validate_stripe_params(cmd, stripes, stripe_size);
}

/* FIXME move to lib */
static int _pv_change_tag(struct physical_volume *pv, const char *tag, int addtag)
{
	if (addtag) {
		if (!str_list_add(pv->fmt->cmd->mem, &pv->tags, tag)) {
			log_error("Failed to add tag %s to physical volume %s",
				  tag, pv_dev_name(pv));
			return 0;
		}
	} else
		str_list_del(&pv->tags, tag);

	return 1;
}

/* Set exactly one of VG, LV or PV */
int change_tag(struct cmd_context *cmd, struct volume_group *vg,
	       struct logical_volume *lv, struct physical_volume *pv, int arg)
{
	const char *tag;
	struct arg_value_group_list *current_group;

	dm_list_iterate_items(current_group, &cmd->arg_value_groups) {
		if (!grouped_arg_is_set(current_group->arg_values, arg))
			continue;

		if (!(tag = grouped_arg_str_value(current_group->arg_values, arg, NULL))) {
			log_error("Failed to get tag");
			return 0;
		}

		if (vg && !vg_change_tag(vg, tag, arg == addtag_ARG))
			return_0;
		else if (lv && !lv_change_tag(lv, tag, arg == addtag_ARG))
			return_0;
		else if (pv && !_pv_change_tag(pv, tag, arg == addtag_ARG))
			return_0;
	}

	return 1;
}

int process_each_label(struct cmd_context *cmd, int argc, char **argv, void *handle,
		       process_single_label_fn_t process_single_label)
{
	struct label *label;
	struct dev_iter *iter;
	struct device *dev;

	int ret_max = ECMD_PROCESSED;
	int ret = 0;
	int opt = 0;

	if (argc) {
		for (; opt < argc; opt++) {
			if (!(dev = dev_cache_get(argv[opt], cmd->filter))) {
				log_error("Failed to find device "
					  "\"%s\"", argv[opt]);
				ret_max = ECMD_FAILED;
				continue;
			}

			if (!label_read(dev, &label, 0)) {
				log_error("No physical volume label read from %s",
					  argv[opt]);
				ret_max = ECMD_FAILED;
				continue;
			}

			ret = process_single_label(cmd, label, handle);

			if (ret > ret_max)
				ret_max = ret;

			if (sigint_caught())
				break;
		}

		return ret_max;
	}

	if (!(iter = dev_iter_create(cmd->filter, 1))) {
		log_error("dev_iter creation failed");
		return ECMD_FAILED;
	}

	while ((dev = dev_iter_get(iter)))
	{
		if (!label_read(dev, &label, 0))
			continue;

		ret = process_single_label(cmd, label, handle);

		if (ret > ret_max)
			ret_max = ret;

		if (sigint_caught())
			break;
	}

	dev_iter_destroy(iter);

	return ret_max;
}

/*
 * If arg is tag, add it to arg_tags.
 * Else the arg is vgname, add the vgname
 * from each arg to arg_vgnames.
 */

static int get_arg_vgnames(struct cmd_context *cmd,
			   int argc, char **argv,
			   struct dm_list *arg_vgnames,
			   struct dm_list *arg_tags)
{
	int opt = 0;
	int ret_max = ECMD_PROCESSED;
	const char *vg_name;

	log_verbose("Using volume group(s) on command line");

	for (; opt < argc; opt++) {
		vg_name = argv[opt];
		if (*vg_name == '@') {
			if (!validate_tag(vg_name + 1)) {
				log_error("Skipping invalid tag %s", vg_name);
				if (ret_max < EINVALID_CMD_LINE)
					ret_max = EINVALID_CMD_LINE;
				continue;
			}
			if (!str_list_add(cmd->mem, arg_tags,
					  dm_pool_strdup(cmd->mem, vg_name + 1))) {
				log_error("strlist allocation failed");
				return ECMD_FAILED;
			}
			continue;
		}

		vg_name = skip_dev_dir(cmd, vg_name, NULL);
		if (strchr(vg_name, '/')) {
			log_error("Invalid volume group name: %s", vg_name);
			if (ret_max < EINVALID_CMD_LINE)
				ret_max = EINVALID_CMD_LINE;
			continue;
		}
		if (!str_list_add_order(cmd->mem, arg_vgnames,
					dm_pool_strdup(cmd->mem, vg_name))) {
			log_error("strlist allocation failed");
			return ECMD_FAILED;
		}
	}

	return ret_max;
}

static int get_all_vgnames(struct cmd_context *cmd, struct dm_list *all_vgnames,
			   int include_orphan)
{
	struct dm_list orphan_vgnames;
	struct dm_list *vgnames;
	struct dm_list *tmp, *safe;
	struct str_list *sl;
	const char *vg_name;
	const char *dup_str;
	int ret_max = ECMD_PROCESSED;
	int rv;

	dm_list_init(&orphan_vgnames);

	log_verbose("Finding all volume groups");

	if (!lvmetad_vg_list_to_lvmcache(cmd))
		stack;

	if (!(vgnames = get_vgnames(cmd, include_orphan)) || dm_list_empty(vgnames))
		goto out;

	dm_list_iterate_items(sl, vgnames) {
		vg_name = sl->str;
		if (!vg_name)
			continue;

		dup_str = dm_pool_strdup(cmd->mem, vg_name);
		if (!dup_str) {
			log_error("strdup allocation failed");
			return ECMD_FAILED;
		}

		if (include_orphan && is_orphan_vg(vg_name))
			rv = str_list_add(cmd->mem, &orphan_vgnames, dup_str);
		else
			rv = str_list_add_order(cmd->mem, all_vgnames, dup_str);

		if (!rv) {
			log_error("strlist allocation failed");
			return ECMD_FAILED;
		}
	}

	/*
	 * vg list processing/locking requires the orphan vgs to be at the end,
	 * so we save all the orphan vgs on the tmp list and then add them to
	 * to the end of the real list here.
	 */
	dm_list_iterate_safe(tmp, safe, &orphan_vgnames) {
		dm_list_del(tmp);
		dm_list_add(all_vgnames, tmp);
	}

out:
	return ret_max;
}

static struct vg_list *find_vgl(struct dm_list *vgl_list, const char *vg_name)
{
	struct vg_list *vgl;

	dm_list_iterate_items(vgl, vgl_list) {
		if (vgl->vg && !strcmp(vg_name, vgl->vg->name))
			return vgl;
	}
	return NULL;
}

static int release_vgl(struct cmd_context *cmd, struct vg_list *vgl)
{
	if (!vgl->vg)
		return 0;

	int r = 0;

	if (is_orphan_vg(vgl->vg->name)) {
		log_debug("release_vgl orphan %s", vgl->vg->name);
		dev_close_vg(vgl->vg->name);
		release_vg(vgl->vg);
		r = 1;
	} else if (vg_read_error(vgl->vg)) {
		log_debug("release_vgl error %s", vgl->vg->name);
		dev_close_vg(vgl->vg->name);
		release_vg(vgl->vg);
	} else {
		log_debug("release_vgl %s", vgl->vg->name);
		dev_close_vg(vgl->vg->name);
		unlock_vg(cmd, vgl->vg->name);
		release_vg(vgl->vg);
	}

	vgl->vg = NULL;
	return r;
}

/*
* Special treatment is needed for unlocking orphan vgs. There are three orphan
* vgs, and all three share one orphan clvm lock. We return 1 if the orphan VG
* was in the list to be released.
*/
static int release_vg_list(struct cmd_context *cmd, struct dm_list *vgl_list)
{
	struct vg_list *vgl;

	int r = 0;

	dm_list_iterate_items(vgl, vgl_list)
		r += release_vgl(cmd, vgl);

	return r;
}

static int read_vg_name_list(struct cmd_context *cmd, uint32_t flags,
			     struct dm_list *vg_name_list,
			     struct dm_list *vgl_list)
{
	struct volume_group *vg;
	struct vg_list *vgl;
	struct str_list *sl;
	const char *vg_name;
	int ret_max = ECMD_PROCESSED;

	dm_list_iterate_items(sl, vg_name_list) {
		vg_name = sl->str;

		if (!(vgl = dm_pool_alloc(cmd->mem, sizeof(*vgl)))) {
			log_error("vg_list alloc failed");
			release_vg_list(cmd, vgl_list);
			return ECMD_FAILED;
		}

		log_debug("read_vg_name_list %s", vg_name);

		vg = vg_read(cmd, vg_name, NULL, flags);
		if (vg_read_error(vg)) {
			if (!((flags & READ_ALLOW_INCONSISTENT) &&
			    (vg_read_error(vg) == FAILED_INCONSISTENT))) {
				ret_max = ECMD_FAILED;
				release_vg(vg);
				stack;
				continue;
			}
		}

		vgl->vg = vg;
		dm_list_add(vgl_list, &vgl->list);

		if (sigint_caught())
			break;
	}

	return ret_max;
}

static int process_vg_name_list(struct cmd_context *cmd, uint32_t flags,
				struct dm_list *vg_name_list,
				struct dm_list *arg_vgnames,
				struct dm_list *arg_tags,
				struct dm_list *vgl_list,
				void *handle,
				process_single_vg_fn_t process_single_vg)
{
	struct volume_group *vg;
	struct vg_list *vgl;
	struct str_list *sl;
	const char *vgname;
	int ret_max = ECMD_PROCESSED;
	int ret = 0;
	int process_all = 0;
	int process_vg;

	if (dm_list_empty(arg_vgnames) && dm_list_empty(arg_tags))
		process_all = 1;

	dm_list_iterate_items(sl, vg_name_list) {
		vgname = sl->str;

		vgl = find_vgl(vgl_list, vgname);
		if (!vgl)
			continue;

		vg = vgl->vg;

		process_vg = 0;

		if (process_all)
			process_vg = 1;

		if (!process_vg && !dm_list_empty(arg_vgnames) &&
		    str_list_match_item(arg_vgnames, vgname))
			process_vg = 1;

		if (!process_vg && !dm_list_empty(arg_tags) &&
		    str_list_match_list(arg_tags, &vg->tags, NULL))
			process_vg = 1;

		if (process_vg)
			ret = process_single_vg(cmd, vgname, vg, handle);

		/*
		 * Close devices after each because open devices
		 * from one may cause the next to fail.
		 */
		release_vgl(cmd, vgl);

		if (ret > ret_max)
			ret_max = ret;
		if (sigint_caught())
			break;
	}

	return ret_max;
}

int process_each_vg(struct cmd_context *cmd,
		    int argc, char **argv, uint32_t flags,
		    void *handle,
		    process_single_vg_fn_t process_single_vg)
{
	struct dm_list all_vgnames;
	struct dm_list arg_vgnames;
	struct dm_list arg_tags;
	struct dm_list vgl_list;
	struct dm_list *vg_name_list;
	int ret_max = ECMD_PROCESSED;
	int ret;

	dm_list_init(&all_vgnames);
	dm_list_init(&arg_vgnames);
	dm_list_init(&arg_tags);
	dm_list_init(&vgl_list);

	ret = get_arg_vgnames(cmd, argc, argv, &arg_vgnames, &arg_tags);
	if (ret != ECMD_PROCESSED)
		return ret;

	if ((dm_list_empty(&arg_vgnames) && (flags & ENABLE_ALL_VGNAMES)) ||
	    !dm_list_empty(&arg_tags)) {
		ret = get_all_vgnames(cmd, &all_vgnames, 0);
		if (ret != ECMD_PROCESSED)
			return ret;
	}

	if (dm_list_empty(&arg_vgnames) && dm_list_empty(&all_vgnames)) {
		log_error("No volume groups found");
		return ECMD_PROCESSED;
	}

	if (!dm_list_empty(&all_vgnames))
		vg_name_list = &all_vgnames;
	else
		vg_name_list = &arg_vgnames;

	ret = read_vg_name_list(cmd, flags, vg_name_list, &vgl_list);

	if (ret > ret_max)
		ret_max = ret;
	if (sigint_caught())
		goto out;

	if (dm_list_empty(&vgl_list)) {
		stack;
		goto out;
	}

	ret = process_vg_name_list(cmd, flags, vg_name_list,
			           &arg_vgnames, &arg_tags,
			           &vgl_list,
			           handle, process_single_vg);
	if (ret > ret_max)
		ret_max = ret;
out:
	release_vg_list(cmd, &vgl_list);
	return ret_max;
}

/*
 * If arg is tag, add it to arg_tags
 * else the arg is either vgname or vgname/lvname:
 * - add the vgname of each arg to arg_vgnames
 * - if arg has no lvname, add just vgname arg_lvnames,
 *   it represents all lvs in the vg
 * - if arg has lvname, add vgname/lvname to arg_lvnames
 */

static int get_arg_lvnames(struct cmd_context *cmd,
			   int argc, char **argv,
			   struct dm_list *arg_vgnames,
			   struct dm_list *arg_lvnames,
			   struct dm_list *arg_tags)
{
	int opt = 0;
	int ret_max = ECMD_PROCESSED;
	char *vglv;
	size_t vglv_sz;
	const char *vgname;

	log_verbose("Using logical volume(s) on command line");

	for (; opt < argc; opt++) {
		const char *lv_name = argv[opt];
		const char *tmp_lv_name;
		char *vgname_def;
		unsigned dev_dir_found = 0;

		/* Do we have a tag or vgname or lvname? */
		vgname = lv_name;

		if (*vgname == '@') {
			if (!validate_tag(vgname + 1)) {
				log_error("Skipping invalid tag %s", vgname);
				continue;
			}
			if (!str_list_add(cmd->mem, arg_tags,
					  dm_pool_strdup(cmd->mem, vgname + 1))) {
				log_error("strlist allocation failed");
				return ECMD_FAILED;
			}
			continue;
		}

		/* FIXME Jumbled parsing */
		vgname = skip_dev_dir(cmd, vgname, &dev_dir_found);

		if (*vgname == '/') {
			log_error("\"%s\": Invalid path for Logical Volume",
				  argv[opt]);
			if (ret_max < ECMD_FAILED)
				ret_max = ECMD_FAILED;
			continue;
		}
		lv_name = vgname;
		if ((tmp_lv_name = strchr(vgname, '/'))) {
			/* Must be an LV */
			lv_name = tmp_lv_name;
			while (*lv_name == '/')
				lv_name++;
			if (!(vgname = extract_vgname(cmd, vgname))) {
				if (ret_max < ECMD_FAILED) {
					stack;
					ret_max = ECMD_FAILED;
				}
				continue;
			}
		} else if (!dev_dir_found &&
			   (vgname_def = default_vgname(cmd))) {
			vgname = vgname_def;
		} else
			lv_name = NULL;

		if (!str_list_add_order(cmd->mem, arg_vgnames,
					dm_pool_strdup(cmd->mem, vgname))) {
			log_error("strlist allocation failed");
			return ECMD_FAILED;
		}

		if (!lv_name) {
			if (!str_list_add(cmd->mem, arg_lvnames,
					  dm_pool_strdup(cmd->mem, vgname))) {
				log_error("strlist allocation failed");
				return ECMD_FAILED;
			}
		} else {
			vglv_sz = strlen(vgname) + strlen(lv_name) + 2;
			if (!(vglv = dm_pool_alloc(cmd->mem, vglv_sz)) ||
			    dm_snprintf(vglv, vglv_sz, "%s/%s", vgname, lv_name) < 0) {
				log_error("vg/lv string alloc failed");
				return ECMD_FAILED;
			}
			if (!str_list_add(cmd->mem, arg_lvnames, vglv)) {
				log_error("strlist allocation failed");
				return ECMD_FAILED;
			}
		}
	}

	return ret_max;
}

static int process_lv_vg_name_list(struct cmd_context *cmd, uint32_t flags,
				   struct dm_list *vg_name_list,
				   struct dm_list *arg_vgnames,
				   struct dm_list *arg_lvnames,
				   struct dm_list *arg_tags,
				   struct dm_list *vgl_list,
				   void *handle,
				   process_single_lv_fn_t process_single_lv)
{
	struct volume_group *vg;
	struct vg_list *vgl;
	struct str_list *sl, *sll;
	struct dm_list *tags_arg;
	struct dm_list lvnames;
	const char *vgname;
	int ret_max = ECMD_PROCESSED;
	int ret = 0;

	dm_list_iterate_items(sl, vg_name_list) {
		vgname = sl->str;

		vgl = find_vgl(vgl_list, vgname);
		if (!vgl)
			continue;

		vg = vgl->vg;

		/*
		 * arg_lvnames contains some elements that are just "vgname"
		 * which means process all lvs in the vg.  Other elements
		 * are "vgname/lvname" which means process only the select
		 * lvs in the vg.
		 */

		tags_arg = arg_tags;
		dm_list_init(&lvnames);	/* LVs to be processed in this VG */

		dm_list_iterate_items(sll, arg_lvnames) {
			const char *vg_name = sll->str;
			const char *lv_name = strchr(vg_name, '/');

			if (!lv_name && !strcmp(vg_name, vgname)) {
				/* Process all LVs in this VG */
				tags_arg = NULL;
				dm_list_init(&lvnames);
				break;
			}
			
			if (lv_name && !strncmp(vg_name, vgname, strlen(vgname)) &&
			    strlen(vgname) == (size_t) (lv_name - vg_name)) {
				if (!str_list_add(cmd->mem, &lvnames,
						  dm_pool_strdup(cmd->mem, lv_name + 1))) {
					log_error("strlist allocation failed");
					return ECMD_FAILED;
				}
			}
		}

		ret = process_each_lv_in_vg(cmd, vg, &lvnames, tags_arg,
					    handle, process_single_lv);

		release_vgl(cmd, vgl);

		if (ret > ret_max)
			ret_max = ret;

		if (sigint_caught())
			break;
	}

	return ret_max;
}

int process_each_lv(struct cmd_context *cmd,
		    int argc, char **argv, uint32_t flags,
		    void *handle,
		    process_single_lv_fn_t process_single_lv)
{
	struct dm_list all_vgnames;
	struct dm_list arg_vgnames;
	struct dm_list arg_lvnames;
	struct dm_list arg_tags;
	struct dm_list vgl_list;
	struct dm_list *vg_name_list;
	int ret_max = ECMD_PROCESSED;
	int ret;

	dm_list_init(&all_vgnames);
	dm_list_init(&arg_vgnames);
	dm_list_init(&arg_lvnames);
	dm_list_init(&arg_tags);
	dm_list_init(&vgl_list);

	ret = get_arg_lvnames(cmd, argc, argv,
			      &arg_vgnames, &arg_lvnames, &arg_tags);
	if (ret != ECMD_PROCESSED)
		return ret;

	if ((dm_list_empty(&arg_vgnames) && (flags & ENABLE_ALL_VGNAMES)) ||
	    !dm_list_empty(&arg_tags)) {
		ret = get_all_vgnames(cmd, &all_vgnames, 0);
		if (ret != ECMD_PROCESSED)
			return ret;
	}

	if (dm_list_empty(&arg_vgnames) && dm_list_empty(&all_vgnames)) {
		log_error("No volume groups found");
		return ECMD_PROCESSED;
	}

	if (!dm_list_empty(&all_vgnames))
		vg_name_list = &all_vgnames;
	else
		vg_name_list = &arg_vgnames;

	ret = read_vg_name_list(cmd, flags, vg_name_list, &vgl_list);

	if (ret > ret_max)
		ret_max = ret;
	if (sigint_caught())
		goto out;

	if (dm_list_empty(&vgl_list)) {
		stack;
		goto out;
	}

	ret = process_lv_vg_name_list(cmd, flags, vg_name_list,
				      &arg_vgnames, &arg_lvnames, &arg_tags,
				      &vgl_list,
				      handle, process_single_lv);
	if (ret > ret_max)
		ret_max = ret;
out:
	release_vg_list(cmd, &vgl_list);

	return ret_max;
}

static int get_arg_pvnames(struct cmd_context *cmd,
			   int argc, char **argv,
			   struct dm_list *arg_pvnames,
			   struct dm_list *arg_tags)
{
	int opt = 0;
	char *at_sign, *tagname;
	char *arg_name;
	int ret_max = ECMD_PROCESSED;

	log_verbose("Using physical volume(s) on command line");

	for (; opt < argc; opt++) {
		arg_name = argv[opt];

		dm_unescape_colons_and_at_signs(arg_name, NULL, &at_sign);
		if (at_sign && (at_sign == arg_name)) {
			tagname = at_sign + 1;

			if (!validate_tag(tagname)) {
				log_error("Skipping invalid tag %s", tagname);
				if (ret_max < EINVALID_CMD_LINE)
					ret_max = EINVALID_CMD_LINE;
				continue;
			}
			if (!str_list_add(cmd->mem, arg_tags,
					  dm_pool_strdup(cmd->mem, tagname))) {
				log_error("strlist allocation failed");
				return ECMD_FAILED;
			}
			continue;
		}

		if (!str_list_add(cmd->mem, arg_pvnames,
				  dm_pool_strdup(cmd->mem, arg_name))) {
			log_error("strlist allocation failed");
			return ECMD_FAILED;
		}
	}

	return ret_max;
}

/*
 * Go through all devs and process those not found in all_vgs->pvs.
 */

static int process_all_other_devs(struct cmd_context *cmd,
				  struct dm_list *all_vgs,
				  void *handle,
				  process_single_pv_fn_t process_single_pv)
{
	struct physical_volume *pv;
	struct physical_volume pv_dummy;
	struct dev_iter *iter;
	struct device *dev;
	struct vg_list *vgl;
	struct pv_list *pvl;
	int ret_max = ECMD_PROCESSED;
	int ret = 0;
	int found;

	log_debug("process_all_other_devs");

	lvmcache_seed_infos_from_lvmetad(cmd);

	if (!(iter = dev_iter_create(cmd->filter, 1))) {
		log_error("dev_iter creation failed");
		return ECMD_FAILED;
	}

	while ((dev = dev_iter_get(iter))) {

		/* TODO use a device-indexed hash here */

		found = 0;

		dm_list_iterate_items(vgl, all_vgs) {
			dm_list_iterate_items(pvl, &vgl->vg->pvs) {
				if (pvl->pv->dev != dev)
					continue;
				found = 1;
				break;
			}
			if (found)
				break;
		}
		if (found)
			continue;

		memset(&pv_dummy, 0, sizeof(pv_dummy));
		dm_list_init(&pv_dummy.tags);
		dm_list_init(&pv_dummy.segments);
		pv_dummy.dev = dev;
		pv = &pv_dummy;

		ret = process_single_pv(cmd, NULL, pv, handle);

		if (ret > ret_max)
			ret_max = ret;
		if (sigint_caught())
			break;
	}

	dev_iter_destroy(iter);

	return ECMD_PROCESSED;
}

/*
 * If arg_pvnames or arg_tags matches a pv in a vg,
 * move that vgl from all_vgs to use_vgs, and
 * the vg name sl from all_vgnames to use_vgnames.
 */

static int select_vgs_with_pvs(struct cmd_context *cmd, uint32_t flags,
			       struct dm_list *arg_pvnames,
			       struct dm_list *arg_tags,
			       struct dm_list *all_vgnames,
			       struct dm_list *use_vgnames,
			       struct dm_list *all_vgs,
			       struct dm_list *use_vgs)
{
	struct volume_group *vg;
	struct physical_volume *pv;
	struct vg_list *vgl, *safe;
	struct pv_list *pvl;
	const char *vg_name;
	const char *pv_name;
	struct str_list *sl;
	int process_pv;
	int ret_max = ECMD_PROCESSED;

	dm_list_iterate_items_safe(vgl, safe, all_vgs) {
		vg = vgl->vg;
		vg_name = vg->name;

		process_pv = 0;

		dm_list_iterate_items(pvl, &vg->pvs) {
			pv = pvl->pv;
			pv_name = pv_dev_name(pv);

			process_pv = 0;

			if (!process_pv && !dm_list_empty(arg_pvnames) &&
			    str_list_match_item(arg_pvnames, pv_name))
				process_pv = 1;

			if (!process_pv && !dm_list_empty(arg_tags) &&
			    str_list_match_list(arg_tags, &pv->tags, NULL))
				process_pv = 1;

			if (!process_pv)
				continue;

			log_debug("select_vgs_with_pvs vg %s for pv %s",
				  vg_name, pv_name);

			dm_list_del(&vgl->list);
			dm_list_add(use_vgs, &vgl->list);
			break;
		}

		if (!process_pv)
			continue;

		dm_list_iterate_items(sl, all_vgnames) {
			if (strcmp(sl->str, vg_name))
				continue;

			dm_list_del(&sl->list);
			dm_list_add(use_vgnames, &sl->list);
			break;
		}
	}

	return ret_max;
}

/*
 * Iterate through all pvs in each listed vg.  Process a pv if
 * the name or tag matches arg_pvnames or arg_tags.  If both
 * arg_pvnames and arg_tags are empty, then process all pvs.
 */

static int process_pvs_in_vgs(struct cmd_context *cmd, uint32_t flags,
			      struct dm_list *use_vgs,
			      struct dm_list *arg_pvnames,
			      struct dm_list *arg_tags,
			      void *handle,
			      process_single_pv_fn_t process_single_pv)
{
	struct volume_group *vg;
	struct physical_volume *pv;
	struct vg_list *vgl;
	struct pv_list *pvl;
	const char *pv_name;
	int process_all = 0;
	int process_pv;
	int matched_pvnames = 0;
	int ret_max = ECMD_PROCESSED;
	int ret = 0;

	if (dm_list_empty(arg_pvnames) && dm_list_empty(arg_tags))
		process_all = 1;

	log_debug("process_pvs_in_vgs all %d", process_all);

	dm_list_iterate_items(vgl, use_vgs) {
		vg = vgl->vg;

		dm_list_iterate_items(pvl, &vg->pvs) {
			pv = pvl->pv;
			pv_name = pv_dev_name(pv);

			process_pv = 0;

			if (process_all)
				process_pv = 1;

			if (!process_pv && !dm_list_empty(arg_pvnames) &&
			    str_list_match_item(arg_pvnames, pv_name)) {
				process_pv = 1;
				matched_pvnames++;
			}

			if (!process_pv && !dm_list_empty(arg_tags) &&
			    str_list_match_list(arg_tags, &pv->tags, NULL)) {
				process_pv = 1;
			}

			if (process_pv) {
				log_debug("process_pvs_in_vgs vg %s pv %s",
					  vg->name, pv_name);
				ret = process_single_pv(cmd, vg, pv, handle);
			}

			if (ret > ret_max)
				ret_max = ret;
			if (sigint_caught())
				break;
		}
	}

	/* command should return an error if a pvname arg was not a pv */

	if (dm_list_size(arg_pvnames) != matched_pvnames)
		ret_max = ECMD_FAILED;

	return ret_max;
}

int process_each_pv(struct cmd_context *cmd,
		    int argc, char **argv,
		    struct volume_group *vg,
		    uint32_t flags,
		    void *handle,
		    process_single_pv_fn_t process_single_pv)
{
	struct dm_list arg_tags;        /* named in argv */
	struct dm_list arg_pvnames;     /* named in argv */
	struct dm_list all_vgnames;
	struct dm_list use_vgnames;
	struct dm_list all_vgs;
	struct dm_list use_vgs;
	struct vg_list *vgl;
	int include_other_devs = 0;
	int ret_max = ECMD_PROCESSED;
	int ret;
	int unlock_orphans = 0;

	/*
	 * arg_tags, arg_pvnames are names specified in argv.
	 * They are used to select a subset of all pvs.
	 *
	 * all_vgnames is every vg name, and includes orphan vgs here.
	 * use_vgnames is a subset of all_vgnames, selected by arg_tags/arg_pvnames.
	 * The selected entries from all_vgnames are moved to use_vgnames.
	 * use_ will be all of of all_ when both arg_ lists are empty.
	 *
	 * all_vgs are vg structs that have been read for each all_vgnames entry.
	 * all_vgs entries are moved to use_vgs in parallel with moving
	 * all_vgnames to use_vgnames.
	 */

	dm_list_init(&arg_tags);
	dm_list_init(&arg_pvnames);

	dm_list_init(&all_vgnames);
	dm_list_init(&use_vgnames);

	dm_list_init(&all_vgs);
	dm_list_init(&use_vgs);

	/*
	 * Create two lists from argv:
	 * arg_pvnames: pvs explicitly named in argv
	 * arg_tags: tags explicitly named in argv
	 */

	ret = get_arg_pvnames(cmd, argc, argv, &arg_pvnames, &arg_tags);
	if (ret != ECMD_PROCESSED)
		return ret;

	/*
	 * Caller has already selected, locked, and read one vg in which to
	 * process pvs. Empty arg_pvnames and empty arg_tags means all pvs in
	 * this vg, empty arg_pvnames and non-empty arg_tags means all pvs in
	 * this vg with a matching tag.
	 */

	if (vg) {
		if (!(vgl = dm_pool_alloc(cmd->mem, sizeof(*vgl)))) {
			log_error("vg_list alloc failed");
			return ECMD_FAILED;
		}
		vgl->vg = vg;
		dm_list_add(&use_vgs, &vgl->list);
		goto process;
	}

	/* Populate all_vgnames and all_vgs. */

	ret = get_all_vgnames(cmd, &all_vgnames, 1);
	if (ret != ECMD_PROCESSED)
		return ret;

	ret = read_vg_name_list(cmd, flags, &all_vgnames, &all_vgs);
	if (ret != ECMD_PROCESSED)
		return ret;

	/*
	 * Check if the caller wants to process all devs (not only pvs).
	 * If so, we first process all pvs by way of all_vgs, then at the
	 * end interate through all devs (including non-pvs), and process
	 * any that were not already done.
	 */

	if (dm_list_empty(&arg_pvnames) && dm_list_empty(&arg_tags) &&
	    (flags & ENABLE_ALL_DEVS)) {
		dm_list_splice(&use_vgs, &all_vgs);
		dm_list_splice(&use_vgnames, &all_vgnames);
		include_other_devs = 1;
		goto process;
	}

	/*
	 * Select which vgs from all_vgs we need to process, and move them
	 * to use_vgs (and the corresponding all_vgnames to use_vgnames).
	 * The vgl's and vg_name's that are not needed are left on
	 * all_vgs/all_vgnames and can be released.
	 */

	if (!dm_list_empty(&arg_pvnames) || !dm_list_empty(&arg_tags)) {
		/*
		 * Select vgs that include the pvs named in arg_pvnames, or
		 * vgs that include pvs with tags matching arg_tags.
		 */
		ret = select_vgs_with_pvs(cmd, flags,
					  &arg_pvnames, &arg_tags,
					  &all_vgnames, &use_vgnames,
					  &all_vgs, &use_vgs);
		if (ret > ret_max)
			ret_max = ret;

		if (release_vg_list(cmd, &all_vgs))
			unlock_orphans = 1;

	} else if (flags & ENABLE_ALL_VGNAMES) {
		/*
		 * In thise case, arg_pvnames and arg_tags are both empty,
		 * which means process all pvs.
		 */
		dm_list_splice(&use_vgs, &all_vgs);
		dm_list_splice(&use_vgnames, &all_vgnames);
	} else {
		release_vg_list(cmd, &all_vgs);
		goto out;
	}

process:
	ret = process_pvs_in_vgs(cmd, flags, &use_vgs,
				 &arg_pvnames, &arg_tags,
				 handle, process_single_pv);
	if (ret > ret_max)
		ret_max = ret;
	if (sigint_caught())
		goto out;

	if (include_other_devs) {
		ret = process_all_other_devs(cmd, &use_vgs,
					     handle, process_single_pv);
		if (ret > ret_max)
			ret_max = ret;
	}
out:
	if (!vg && release_vg_list(cmd, &use_vgs) || unlock_orphans)
		unlock_vg(cmd, VG_ORPHANS);

	return ret_max;
}

