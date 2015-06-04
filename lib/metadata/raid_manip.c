/*
 * Copyright (C) 2011-2015 Red Hat, Inc. All rights reserved.
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

#include "lib.h"
#include "archiver.h"
#include "metadata.h"
#include "toolcontext.h"
#include "segtype.h"
#include "display.h"
#include "activate.h"
#include "lv_alloc.h"
#include "lvm-string.h"
#include "lvm-signal.h"

/* HM FIXME: REMOVEME: devel output */
#if 0
#include "dump.h"
#endif

/* HM FIXME: REMOVEME: devel output */
#ifdef USE_PFL
#define PFL() printf("%s %u\n", __func__, __LINE__);
#define PFLA(format, arg...) printf("%s %u " format "\n", __func__, __LINE__, arg);
#else
#define PFL()
#define PFLA(format, arg...)
#endif

#define	ARRAY_SIZE(a) (sizeof(a) / sizeof(*a))

/* Ensure minimum region size on @lv */
static void _ensure_min_region_size(struct logical_volume *lv)
{
	struct lv_segment *seg = first_seg(lv);
	/* MD's bitmap is limited to tracking 2^21 regions */
	uint32_t min_region_size = lv->size / (1 << 21);
	uint32_t region_size = seg->region_size;

	while (seg->region_size < min_region_size)
		seg->region_size *= 2;

	if (seg->region_size != region_size)
		log_very_verbose("Setting region_size to %u", seg->region_size);
}

/* HM Return "linear" for striped @segtype instead of "striped" */
static const char *_get_segtype_name(const struct segment_type *segtype, unsigned new_image_count)
{
	return (segtype_is_striped(segtype) && new_image_count == 1) ? "linear" : segtype->name;
}

/*
 * HM
 *
 * Default region_size on @lv unless already set i
 */
static void _check_and_init_region_size(struct logical_volume *lv)
{
	struct lv_segment *seg = first_seg(lv);

	seg->region_size = seg->region_size ?: get_default_region_size(lv->vg->cmd);
	_ensure_min_region_size(lv);
}

/* Return data images count for @total_rimages depending on @seg's type */
static uint32_t _data_rimages_count(struct lv_segment *seg, uint32_t total_rimages)
{
	return total_rimages - seg->segtype->parity_devs;
}

/*
 * HM
 *
 * Compare the raid levels in segtype @t1 and @t2
 *
 * Return 0 if same, else != 0
 */
static int _cmp_level(const struct segment_type *t1, const struct segment_type *t2)
{
	return strncmp(t1->name, t2->name, 5);
}

/*
 * HM
 *
 * Check for same raid levels in segtype @t1 and @t2
 *
 * Return 1 if same, else != 1
 */
static int is_same_level(const struct segment_type *t1, const struct segment_type *t2)
{
#if 0
	static uint64_t level_flags[] = {
		SEG_RAID0|SEG_RAID0_META,
		SEG_RAID5_LS|SEG_RAID5_LA|SEG_RAID5_RS|SEG_RAID5_RA|SEG_RAID5_N,
		SEG_RAID6_ZR|SEG_RAID6_NC|SEG_RAID6_NR| \
		SEG_RAID6_LS_6|SEG_RAID6_LA_6|SEG_RAID6_RS_6|SEG_RAID6_RA_6|SEG_RAID6_N_6
	};
	unsigned i = ARRAY_SIZE(level_flags);

	if (t1->flag & t2->flag)
		return 1;

	while (i--)
		if ((t1->flag & level_flags[i]) && (t2->flag & level_flags[i]))
			return 1;

	return 0;
#else
	return !_cmp_level(t1, t2);
#endif
}

#if 0
/*
 * HM
 *
 * Check for raid level by segtype going up from @t1 to @t2
 *
 * Return 1 if same, else != 1
 */
static int is_level_up(const struct segment_type *t1, const struct segment_type *t2)
{
	if (segtype_is_raid(t1) && segtype_is_striped(t2))
		return 0;

	if (segtype_is_striped(t1) && segtype_is_raid(t2))
		return 1;

	return _cmp_level(t1, t2) < 0;
}
#endif

static int _lv_is_raid_with_tracking(const struct logical_volume *lv,
				     struct logical_volume **tracking)
{
	uint32_t s;
	const struct lv_segment *seg = first_seg(lv);

	*tracking = NULL;

	if (!lv_is_raid(lv))
		return 0;

	for (s = 0; s < seg->area_count; s++)
		if (lv_is_visible(seg_lv(seg, s)) &&
		    !(seg_lv(seg, s)->status & LVM_WRITE)) {
			*tracking = seg_lv(seg, s);
			return 1;
		}

	return 0;
}

int lv_is_raid_with_tracking(const struct logical_volume *lv)
{
	struct logical_volume *tracking;

	return _lv_is_raid_with_tracking(lv, &tracking);
}

uint32_t lv_raid_image_count(const struct logical_volume *lv)
{
	struct lv_segment *seg = first_seg(lv);

	return seg_is_raid(seg) ? seg->area_count : 1;
}

static int _activate_sublv_preserving_excl(struct logical_volume *top_lv,
					   struct logical_volume *sub_lv)
{
	struct cmd_context *cmd = top_lv->vg->cmd;

	/* If top RAID was EX, use EX */
	if (lv_is_active_exclusive_locally(top_lv)) {
		if (!activate_lv_excl_local(cmd, sub_lv))
			return_0;
	} else if (!activate_lv(cmd, sub_lv))
			return_0;

	return 1;
}

/*
 * HM
 *
 * Deactivate and remove the LVs on @removal_lvs list from @vg
 *
 * Returns 1 on success or 0 on failure
 */
static int _deactivate_and_remove_lvs(struct volume_group *vg, struct dm_list *removal_lvs)
{
	struct lv_list *lvl;

	dm_list_iterate_items(lvl, removal_lvs) {
PFLA("lvl->lv->name=%s", lvl->lv->name);
		if (!deactivate_lv(vg->cmd, lvl->lv))
			return_0;

PFLA("lvl->lv->name=%s", lvl->lv->name);
		if (!lv_remove(lvl->lv))
			return_0;
	}

	return 1;
}

/*
 * _raid_in_sync
 * @lv
 *
 * _raid_in_sync works for all types of RAID segtypes, as well
 * as 'mirror' segtype.  (This is because 'lv_raid_percent' is
 * simply a wrapper around 'lv_mirror_percent'.
 *
 * Returns: 1 if in-sync, 0 otherwise.
 */
static int _raid_in_sync(struct logical_volume *lv)
{
	dm_percent_t sync_percent;
	struct lv_segment *seg = first_seg(lv);

	if (seg_is_striped(seg) || seg_is_any_raid0(seg))
		return 1;

	if (!lv_raid_percent(lv, &sync_percent)) {
		log_error("Unable to determine sync status of %s.", display_lvname(lv));
		return 0;
	}

	return (sync_percent == DM_PERCENT_100) ? 1 : 0;
}

/*
 * HM
 *
 * Remove seg from segments using @lv and set one segment mapped to error target on @lv
 *
 * Returns 1 on success or 0 on failure
 */
static int _remove_and_set_error_target(struct logical_volume *lv, struct lv_segment *seg)
{
	lv_set_visible(lv);

	if (!remove_seg_from_segs_using_this_lv(lv, seg))
		return_0;

	return replace_lv_with_error_segment(lv);
}

/*
 * _convert_raid_to_linear
 * @lv
 * @removal_lvs
 *
 * Remove top layer of RAID LV in order to convert to linear.
 * This function makes no on-disk changes.  The residual LVs
 * returned in 'removal_lvs' must be freed by the caller.
 *
 * Returns: 1 on succes, 0 on failure
 */
static int _extract_image_component_list(struct lv_segment *seg,
					 uint64_t type, uint32_t idx,
					 struct dm_list *removal_lvs);
static int _convert_raid_to_linear(struct logical_volume *lv,
				   struct dm_list *removal_lvs)
{
	struct lv_list *lvl;
	struct logical_volume *lv_tmp;
	struct lv_segment *seg = first_seg(lv);

	if (!seg_is_any_raid0(seg) &&
	    !seg_is_mirrored(seg) && !seg_is_raid1(seg) &&
	    !seg_is_raid4(seg) && !seg_is_any_raid5(seg)) {
		log_error(INTERNAL_ERROR
				"Unable to remove RAID layer from segment type %s",
				lvseg_name(seg));
		return 0;
	}

	/* Only one area may result from the check! */
	if (seg->area_count != 1) {
		log_error(INTERNAL_ERROR
				"Unable to remove RAID layer when there"
				" is more than one sub-lv");
		return 0;
	}

	if (!(lvl = dm_pool_alloc(lv->vg->vgmem, sizeof(*lvl))))
		return_0;

	if (seg->meta_areas &&
	    !_extract_image_component_list(seg, RAID_META, 0, removal_lvs))
		return 0;

	/* Add remaining last image lv to removal_lvs */
	lv_tmp = seg_lv(seg, 0);
	lv_tmp->status &= ~RAID_IMAGE;
	lv_set_visible(lv_tmp);
	lvl->lv = lv_tmp;
	dm_list_add(removal_lvs, &lvl->list);

	if (!remove_layer_from_lv(lv, lv_tmp))
		return_0;

	if (!(first_seg(lv)->segtype = get_segtype_from_string(lv->vg->cmd, SEG_TYPE_NAME_STRIPED)))
		return_0;

	lv->status &= ~(MIRRORED | RAID);

	return 1;
}

/*
 * _clear_lv
 * @lv
 *
 * If LV is active:
 *        clear first block of device
 * otherwise:
 *        activate, clear, deactivate
 *
 * Returns: 1 on success, 0 on failure
 */
static int _clear_lv(struct logical_volume *lv)
{
	int was_active = lv_is_active_locally(lv);

	if (test_mode())
		return 1;

	lv->status |= LV_TEMPORARY;
	if (!was_active && !activate_lv_local(lv->vg->cmd, lv)) {
		log_error("Failed to activate localy %s for clearing",
				lv->name);
		return 0;
	}
	lv->status &= ~LV_TEMPORARY;

	PFLA("Clearing metadata area of %s", display_lvname(lv));
	log_verbose("Clearing metadata area of %s", display_lvname(lv));
	/*
	 * Rather than wiping lv->size, we can simply
	 * wipe the first sector to remove the superblock of any previous
	 * RAID devices.  It is much quicker.
	 */
	if (!wipe_lv(lv, (struct wipe_params) { .do_zero = 1, .zero_sectors = 1 })) {
		log_error("Failed to zero %s", lv->name);
		return 0;
	}

	if (!was_active && !deactivate_lv(lv->vg->cmd, lv)) {
		log_error("Failed to deactivate %s", lv->name);
		return 0;
	}

	return 1;
}

/*
 * HM
 *
 * Wipe all LVs on @lv_list
 *
 * Makes on-disk metadata changes!
 *
 * Returns 1 on success or 0 on failure
 */
static int _clear_lvs(struct dm_list *lv_list)
{
	struct lv_list *lvl;
	struct volume_group *vg = NULL;

	if (dm_list_empty(lv_list)) {
		log_debug_metadata(INTERNAL_ERROR "Empty list of LVs given for clearing");
		return 1;
	}

	PFL();
	dm_list_iterate_items(lvl, lv_list) {
		if (!lv_is_visible(lvl->lv)) {
			log_error(INTERNAL_ERROR
					"LVs must be set visible before clearing");
			return 0;
		}

		vg = lvl->lv->vg;
	}

	/*
	 * FIXME: only vg_[write|commit] if LVs are not already written
	 * as visible in the LVM metadata (which is never the case yet).
	 */
	PFL();
	PFLA("vg_validate(vg)=%d", vg_validate(vg));
	PFL();
	if (!vg || !vg_write(vg) || !vg_commit(vg))
		return_0;
	PFL();
	dm_list_iterate_items(lvl, lv_list)
		if (!_clear_lv(lvl->lv))
			return 0;

	return 1;
}

/*
 * HM
 *
 * Check for maximum supported raid devices imposed by the kernel MD
 * maximum device limits _and_ dm-raid superblock bitfield constraints
 *
 * Returns 1 on success or 0 on failure
 */
static int _check_max_raid_devices(uint32_t image_count)
{
	if (image_count > DEFAULT_RAID_MAX_IMAGES) {
		log_error("Unable to handle arrays with more than %u devices",
		          DEFAULT_RAID_MAX_IMAGES);
		return 0;
	}

	return 1;
}

/*
 * HM
 *
 * Check for maximum supported mirror devices imposed
 * by the kernel mirror target maximum device
 *
 * Returns 1 on success or 0 on failure
 */
static int _check_max_mirror_devices(uint32_t image_count)
{
	if (image_count > DEFAULT_MIRROR_MAX_IMAGES) {
		log_error("Unable to handle mirrors with more than %u devices",
		          DEFAULT_MIRROR_MAX_IMAGES);
		return 0;
	}

	return 1;
}

/* Replace @lv with error segment */
static int _replace_lv_with_error_segment(struct logical_volume *lv)
{
	if (lv && (lv->status & PARTIAL_LV)) {
		log_debug("Replacing %s segments with error target", lv->name);

		if (!replace_lv_with_error_segment(lv)) {
			log_error("Failed to replace %s's extents with error target.", display_lvname(lv));
			return 0;
		}
	}

	return 1;
}

/* Retrieve index from @*lv_name and add it to @prefix; set the result in @*lv_name */
static int _lv_name_add_string_index(struct cmd_context *cmd, const char **lv_name, const char *prefix)
{
	size_t len;
	char *b, *e, *newname, *tmpname;

	if (!(tmpname = dm_pool_strdup(cmd->mem, *lv_name)))
		return 0;

	if (!(e = strrchr(tmpname, '_')))
		return 0;

	*e = '\0';
	if (!(b = strrchr(tmpname, '_')))
		return 0;

	len = strlen(prefix) + (e - b) + 1;
	if (!(newname = dm_pool_zalloc(cmd->mem, len))) {
		log_error("Failed to allocate new LV name");
		return 0;
	}

	strcpy(newname, prefix);
	strcat(newname, b);
	*lv_name = newname;
	return 1;
}

/*
 * Shift image @*name (suffix) from @s to (@s - @missing)
 *
 * E.g. s=5, missing=2 -> change "*_r(image,meta)_5" to "*_r(image,meta)_3" 
 */
static int _shift_image_name(struct lv_segment *seg, char **name, unsigned s, unsigned missing)
{
	unsigned num;
	size_t len;
	char *numptr, *shift_name;

	if (!(shift_name = dm_pool_strdup(seg_lv(seg, s - missing)->vg->cmd->mem, *name))) {
		log_error("Memory allocation failed.");
		return 0;
	}

	if (!(numptr = strrchr(shift_name, '_')) ||
			(num = atoi(numptr + 1)) != s) {
		log_error("Malformatted image name");
		return 0;
	}

	*name = shift_name;
	len = strlen(++numptr) + 1;

	return dm_snprintf(numptr, len, "%u", num - missing) < 0 ? 0 : 1;
}

/*
 * _shift_image_components
 * @seg: Top-level RAID segment
 *
 * Shift all higher indexed segment areas down to fill in gaps where
 * there are 'AREA_UNASSIGNED' areas.
 *
 * We don't need to bother with name reallocation,
 * because the name lenght wirl be less or equal
 * when shifting down as opposed to shifting up.
 *
 * Returns: 1 on success, 0 on failure
 */
static int _shift_image_components(struct lv_segment *seg)
{
	uint32_t s, missing;

	if (!seg_is_raid(seg))
		return_0;

	/* Should not be possible here, but... */
	if (!_check_max_raid_devices(seg->area_count))
		return 0;

	log_very_verbose("Shifting images in %s", lvseg_name(seg));

	for (s = missing = 0; s < seg->area_count; s++) {
		if (seg_type(seg, s) == AREA_UNASSIGNED) {
			if (seg_metatype(seg, s) != AREA_UNASSIGNED) {
				log_error(INTERNAL_ERROR "Metadata segment area"
						" #%d should be AREA_UNASSIGNED", s);
				return 0;
			}

			missing++;
			continue;
		}

		if (missing) {
			log_very_verbose("Shifting %s and %s by %u",
					seg_metalv(seg, s)->name,
					seg_lv(seg, s)->name, missing);
			seg->areas[s - missing] = seg->areas[s];
			if (!_shift_image_name(seg, (char **) &seg_lv(seg, s - missing)->name, s, missing))
				return 0;

			if (seg->meta_areas) {
				seg->meta_areas[s - missing] = seg->meta_areas[s];
				if (!_shift_image_name(seg, (char **) &seg_metalv(seg, s - missing)->name, s, missing))
					return 0;
			}
		}

	}

	seg->area_count -= missing;
	return 1;
}

/* Generate raid subvolume name and validate it */
static char *_generate_raid_name(struct logical_volume *lv,
		const char *suffix, int count)
{
	const char *format = (count < 0) ? "%s_%s" : "%s_%s_%u";
	size_t len = strlen(lv->name) + strlen(suffix) + ((count < 0) ? 2 : 5);
	char *name;

	if (!(name = dm_pool_alloc(lv->vg->vgmem, len))) {
		log_error("Failed to allocate new name.");
		return NULL;
	}

	if (dm_snprintf(name, len, format, lv->name, suffix, count) < 0)
		return_NULL;

	PFLA("name=%s", name);
	if (!validate_name(name)) {
		log_error("New logical volume name \"%s\" is not valid.", name);
		return NULL;
	}

	if (find_lv_in_vg(lv->vg, name)) {
		log_error("Logical volume %s already exists in volume group %s.",
				name, lv->vg->name);
		return NULL;
	}

	return name;
}

/*
 * Eliminate the extracted LVs on @removal_lvs from @vg incl. vg write, commit and backup 
 */
static int _eliminate_extracted_lvs(struct volume_group *vg, struct dm_list *removal_lvs)
{
	if (!removal_lvs || dm_list_empty(removal_lvs))
		return 1;

	sync_local_dev_names(vg->cmd);

	PFL();
	if (!dm_list_empty(removal_lvs)) {
		if (!_deactivate_and_remove_lvs(vg, removal_lvs))
			return 0;

		if (!vg_write(vg) || !vg_commit(vg))
			return_0;

		if (!backup(vg))
			log_error("Backup of VG %s failed after removal of image component LVs", vg->name);
	}
	PFL();

	return 1;
}

/*
 * Reallocate segment areas given by @seg_areas (i.e eith data or metadata areas)
 * in segment @seg to amount in @areas copying the minimum of common areas across
 */
static int _realloc_seg_areas(struct logical_volume *lv, struct lv_segment *seg,
		uint32_t areas, struct lv_segment_area **seg_areas)
{
	struct lv_segment_area *new_areas;

	if (!(new_areas = dm_pool_zalloc(lv->vg->vgmem, areas * sizeof(*new_areas)))) {
		log_error("Allocation of new areas array failed.");
		return 0;
	}

	if (*seg_areas)
		memcpy(new_areas, *seg_areas, min(areas, seg->area_count) * sizeof(*new_areas));

	*seg_areas = new_areas;
	return 1;
}

/*
 * HM
 *
 * Reallocate both data and metadata areas of segmen @seg to new amount in @ares
 */
static int _realloc_meta_and_data_seg_areas(struct logical_volume *lv, struct lv_segment *seg,
		uint32_t areas)
{
	return (_realloc_seg_areas(lv, seg, areas, &seg->meta_areas) &&
			_realloc_seg_areas(lv, seg, areas, &seg->areas)) ? 1 : 0;
}

#if 0
/*
 * HM
 *
 * Move the end of a partial segment area from @seg_from to @seg_to
 */
static int _raid_move_partial_lv_segment_area(struct lv_segment *seg_to, uint32_t area_to,
		struct lv_segment *seg_from, uint32_t area_from, uint32_t area_reduction)
{
	uint32_t pe;
	struct physical_volume *pv;

	if (seg_type(seg_from, area_from) != AREA_PV)
		return 0;

	pv = seg_pv(seg_from, area_from);
	pe = seg_pe(seg_from, area_from) + seg_from->area_len - area_reduction;;

	if (!release_lv_segment_area(seg_from, area_from, area_reduction))
		return_0;

	if (!release_lv_segment_area(seg_to, area_to, area_reduction))
		return_0;

	if (!set_lv_segment_area_pv(seg_to, area_to, pv, pe))
		return_0;

	seg_from->area_len -= area_reduction;

	return 1;
}
#endif

/*
 * _extract_image_component
 * @seg
 * @idx:  The index in the areas array to remove
 * @data: != 0 to extract data dev / 0 extract metadata_dev
 * @extracted_lv:  The displaced metadata/data LV
 */
static int _extract_image_component(struct lv_segment *seg,
		uint64_t type, uint32_t idx,
		struct logical_volume **extracted_lv)
{
	struct logical_volume *lv;

	switch (type) {
		case RAID_META:
			lv = seg_metalv(seg, idx);
			seg_metalv(seg, idx) = NULL;
			seg_metatype(seg, idx) = AREA_UNASSIGNED;
			break;
		case RAID_IMAGE:
			lv = seg_lv(seg, idx);
			seg_lv(seg, idx) = NULL;
			seg_type(seg, idx) = AREA_UNASSIGNED;
			break;
		default:
			log_error(INTERNAL_ERROR "Bad type provided to %s.", __func__);
			return 0;
	}

	if (!lv)
		return 0;

	log_very_verbose("Extracting image component %s from %s", lv->name, lvseg_name(seg));
	lv->status &= ~(type | RAID);
	lv_set_visible(lv);

	/* release lv areas */
	if (!remove_seg_from_segs_using_this_lv(lv, seg))
		return_0;

	if (!(lv->name = _generate_raid_name(lv, "extracted", -1)))
		return_0;

	if (!replace_lv_with_error_segment(lv))
		return_0;

	*extracted_lv = lv;

	return 1;
}

/*
 * @seg
 * @idx:  The index in the areas array to remove
 * @lvl_array: The displaced metadata + data LVs
 *
 * These functions extracts _one_  image component pair - setting the respective
 * 'lvl_array' pointers.  It appends '_extracted' to the LVs' names, so that
 * there are not future conflicts.  It does /not/ commit the results.
 * (IOW, erroring-out requires no unwinding of operations.)
 *
 * This function does /not/ attempt to:
 *    1) shift the 'areas' or 'meta_areas' arrays.
 *       The '[meta_]areas' are left as AREA_UNASSIGNED.
 *    2) Adjust the seg->area_count
 *    3) Name the extracted LVs appropriately (appends '_extracted' to names)
 * These actions must be performed by the caller.
 *
 * Returns: 1 on success, 0 on failure
 */
static int _extract_image_component_pair(struct lv_segment *seg, uint32_t idx,
					 struct lv_list *lvl_array)
{
	if (idx >= seg->area_count) {
		log_error(INTERNAL_ERROR "area index too large for segment");
		return 0;
	}

	/* Don't change extraction sequence; callers are relying on it */
	if (!_extract_image_component(seg, RAID_META,  idx, &lvl_array[0].lv) ||
			!_extract_image_component(seg, RAID_IMAGE, idx, &lvl_array[1].lv))
		return_0;

	return 1;
}

/* Remove sublvs fo @type from @lv starting at @idx and put them on @removal_lvs */
static int _extract_image_component_list(struct lv_segment *seg,
					 uint64_t type, uint32_t idx,
					 struct dm_list *removal_lvs)
{
	uint32_t s;
	unsigned i;
	struct lv_list *lvl_array;

	if (idx >= seg->area_count) {
		log_error(INTERNAL_ERROR "area index too large for segment");
		return 0;
	}

	if (!(lvl_array = dm_pool_alloc(seg_lv(seg, 0)->vg->vgmem, sizeof(*lvl_array) * (seg->area_count - idx))))
		return_0;

	for (i = 0, s = idx; s < seg->area_count; s++) {
		if (!_extract_image_component(seg, type, s, &lvl_array[i].lv))
			return 0;

		dm_list_add(removal_lvs, &lvl_array[i].list);
		i++;
	}

	if (type == RAID_IMAGE)
		seg->areas = NULL;
	else
		seg->meta_areas = NULL;

	return 1;
}

/* Add new @lvs to @lv at @area_offset */
static int _add_image_component_list(struct lv_segment *seg, int delete_from_list,
				     uint64_t lv_flags, struct dm_list *lvs, uint32_t area_offset)
{
	uint32_t s = area_offset;
	struct lv_list *lvl, *tmp;

	dm_list_iterate_items_safe(lvl, tmp, lvs) {
		if (delete_from_list)
			dm_list_del(&lvl->list);

		if (lv_flags & VISIBLE_LV)
			lv_set_visible(lvl->lv);
		else
			lv_set_hidden(lvl->lv);

		if (lv_flags & LV_REBUILD)
			lvl->lv->status |= LV_REBUILD;
		else
			lvl->lv->status &= ~LV_REBUILD;

		if (!set_lv_segment_area_lv(seg, s++, lvl->lv, 0 /* le */,
					lvl->lv->status)) {
			log_error("Failed to add sublv %s", lvl->lv->name);
			return 0;
		}
	}

	return 1;
}

/*
 * Create an LV of specified type.  Set visible after creation.
 * This function does not make metadata changes.
 */
static struct logical_volume *_alloc_image_component(struct logical_volume *lv, const char *alt_base_name,
		struct alloc_handle *ah, uint32_t first_area,
		uint64_t type)
{
	uint64_t status = RAID | LVM_READ | LVM_WRITE | type;
	char img_name[NAME_LEN];
	const char *type_suffix;
	struct logical_volume *tmp_lv;
	const struct segment_type *segtype;

	switch (type) {
		case RAID_META:
			type_suffix = "rmeta";
			break;
		case RAID_IMAGE:
			type_suffix = "rimage";
			status |= LV_REBUILD;
			break;
		default:
			log_error(INTERNAL_ERROR "Bad type provided to %s.", __func__);
			return 0;
	}

	if (dm_snprintf(img_name, sizeof(img_name), "%s_%s_%%d",
				alt_base_name ?: lv->name, type_suffix) < 0)
		return_0;


	if (!(tmp_lv = lv_create_empty(img_name, NULL, status, ALLOC_INHERIT, lv->vg))) {
		log_error("Failed to allocate new raid component, %s.", img_name);
		return 0;
	}

	/* If no allocation requested, leave it to the empty LV (needed for striped -> raid0 takeover) */
	if (ah) {
		if (!(segtype = get_segtype_from_string(lv->vg->cmd, SEG_TYPE_NAME_STRIPED)))
			return_0;

		if (!lv_add_segment(ah, first_area, 1, tmp_lv, segtype, 0, status, 0)) {
			log_error("Failed to add segment to LV, %s", img_name);
			return 0;
		}

		first_seg(tmp_lv)->status |= SEG_RAID;
	}

	lv_set_visible(tmp_lv);

	return tmp_lv;
}

/* Calculate absolute amount of metadata device extens based on @rimage_extents, @region_size and @extens_size */
static uint32_t _raid_rmeta_extents(struct cmd_context *cmd,
		uint32_t rimage_extents, uint32_t region_size, uint32_t extent_size)
{
	uint64_t bytes, regions, sectors;

	region_size = region_size ?: get_default_region_size(cmd);
	regions = rimage_extents * extent_size / region_size;

	/* raid and bitmap superblocks + region bytes */
	bytes = 2 * 4096 + dm_div_up(regions, 8);
	sectors = dm_div_up(bytes, 512);

	return dm_div_up(sectors, extent_size);
}

/*
 * Returns raid metadata device size _change_ in extents, algorithm from dm-raid ("raid" target) kernel code.
 */
uint32_t raid_rmeta_extents_delta(struct cmd_context *cmd,
		uint32_t rimage_extents_cur, uint32_t rimage_extents_new,
		uint32_t region_size, uint32_t extent_size)
{
	uint32_t rmeta_extents_cur = _raid_rmeta_extents(cmd, rimage_extents_cur, region_size, extent_size);
	uint32_t rmeta_extents_new = _raid_rmeta_extents(cmd, rimage_extents_new, region_size, extent_size);
	PFLA("rimage_extents_cur=%u rmeta_extents_cur=%u rimage_extents_new=%u rmeta_extents_new=%u region_size=%u extent_size=%u", rimage_extents_cur, rmeta_extents_cur,  rimage_extents_new, rmeta_extents_new, region_size, extent_size);
	/* Need minimum size on LV creation */
	if (!rimage_extents_cur)
		return rmeta_extents_new;

	/* Need current size on LV deletion */
	if (!rimage_extents_new)
		return rmeta_extents_cur;

	if (rmeta_extents_new == rmeta_extents_cur)
		return 0;

	/* Extending/reducing... */
	return rmeta_extents_new > rmeta_extents_cur ?
		rmeta_extents_new - rmeta_extents_cur :
		rmeta_extents_cur - rmeta_extents_new;
}

/*
 * _alloc_rmeta_for_lv
 * @lv
 *
 * Allocate  RAID metadata device for the given LV (which is or will
 * be the associated RAID data device).  The new metadata device must
 * be allocated from the same PV(s) as the data device.
 */
static int _alloc_rmeta_for_lv(struct logical_volume *data_lv,
		struct logical_volume **meta_lv)
{
	int r = 1;
	char *p;
	struct dm_list allocatable_pvs;
	struct alloc_handle *ah;
	struct lv_segment *seg = first_seg(data_lv);

	dm_list_init(&allocatable_pvs);

	if (!seg_is_linear(seg)) {
		log_error(INTERNAL_ERROR "Unable to allocate RAID metadata "
				"area for non-linear LV, %s", data_lv->name);
		return 0;
	}

	_check_and_init_region_size(data_lv);

	if ((p = strstr(data_lv->name, "_mimage_")) ||
			(p = strstr(data_lv->name, "_rimage_")))
		*p = '\0';

	if (!get_pv_list_for_lv(data_lv->vg->cmd->mem,
				data_lv, &allocatable_pvs)) {
		log_error("Failed to build list of PVs for %s", display_lvname(data_lv));
		return 0;
	}

	if (!(ah = allocate_extents(data_lv->vg, NULL, seg->segtype,
					0, 1, 0,
					seg->region_size,
					_raid_rmeta_extents(data_lv->vg->cmd, data_lv->le_count,
						seg->region_size, data_lv->vg->extent_size),
					&allocatable_pvs, data_lv->alloc, 0, NULL)))
		return_0;

	if (!(*meta_lv = _alloc_image_component(data_lv, data_lv->name, ah, 0, RAID_META)))
		r = 0;

	if (p)
		*p = '_';

	alloc_destroy(ah);

	return r;
}

/*
 * HM Allocate metadata devs for all @new_data_devs and link them to list @new_meta_lvs
 */
static int _alloc_rmeta_devs_for_rimage_devs(struct logical_volume *lv,
		struct dm_list *new_data_lvs,
		struct dm_list *new_meta_lvs)
{
	uint32_t a = 0, raid_devs = 0;
	struct dm_list *l;
	struct lv_list *lvl, *lvl_array;

	dm_list_iterate(l, new_data_lvs)
		raid_devs++;

	if (!raid_devs)
		return 0;

	if (!(lvl_array = dm_pool_zalloc(lv->vg->vgmem, raid_devs * sizeof(*lvl_array))))
		return 0;

	dm_list_iterate_items(lvl, new_data_lvs) {
		log_debug_metadata("Allocating new metadata LV for %s",
				lvl->lv->name);
		if (!_alloc_rmeta_for_lv(lvl->lv, &lvl_array[a].lv)) {
			log_error("Failed to allocate metadata LV for %s in %s",
					lvl->lv->name, lv->vg->name);
			return 0;
		}

		dm_list_add(new_meta_lvs, &lvl_array[a].list);
		a++;
	}

	return 1;
}

/*
 * HM
 *
 * Allocate metadata devs for all data devs of an LV
 A*/
static int _alloc_rmeta_devs_for_lv(struct logical_volume *lv, struct dm_list *meta_lvs)
{
	uint32_t s;
	struct lv_list *lvl_array;
	struct dm_list data_lvs;
	struct lv_segment *seg = first_seg(lv);

	dm_list_init(&data_lvs);

	if (seg->meta_areas) {
		log_error(INTERNAL_ERROR "Metadata LVs exists in %s", display_lvname(lv));
		return 0;
	}

	if (!(seg->meta_areas = dm_pool_zalloc(lv->vg->vgmem,
					seg->area_count * sizeof(*seg->meta_areas))))
		return 0;

	if (!(lvl_array = dm_pool_alloc(lv->vg->vgmem, seg->area_count * sizeof(*lvl_array))))
		return_0;

	for (s = 0; s < seg->area_count; s++) {
		lvl_array[s].lv = seg_lv(seg, s);
		dm_list_add(&data_lvs, &lvl_array[s].list);
	}

	if (!_alloc_rmeta_devs_for_rimage_devs(lv, &data_lvs, meta_lvs)) {
		log_error("Failed to allocate metadata LVs for %s", lv->name);
		return 0;
	}

	return 1;
}

/*
 * Create @count new image component pairs for @lv and return them in
 * @new_meta_lvs and @new_data_lvs allocating space if @allocate is set.
 *
 * Use @pvs list for allocation if set.
 */
static int _alloc_image_components(struct logical_volume *lv, int allocate,
		struct dm_list *pvs, uint32_t count,
		struct dm_list *meta_lvs,
		struct dm_list *data_lvs)
{
	uint32_t s, extents;
	struct lv_segment *seg = first_seg(lv);
	const struct segment_type *segtype;
	struct alloc_handle *ah = NULL;
	struct dm_list *parallel_areas;
	struct lv_list *lvl_array;

	if (!meta_lvs && !data_lvs)
		return 0;

	if (!(lvl_array = dm_pool_alloc(lv->vg->vgmem, 2 * count * sizeof(*lvl_array))))
		return_0;

	PFL();
	if (!(parallel_areas = build_parallel_areas_from_lv(lv, 0, 1)))
		return_0;
	PFL();

	_check_and_init_region_size(lv);

	if (seg_is_raid(seg))
		segtype = seg->segtype;
	else if (!(segtype = get_segtype_from_flag(lv->vg->cmd, SEG_RAID1)))
		return_0;
	PFL();

	/*
	 * The number of extents is based on the RAID type.  For RAID1/10,
	 * each of the rimages is the same size - 'le_count'.  However
	 * for RAID 0/4/5/6, the stripes add together (NOT including the parity
	 * devices) to equal 'le_count'.  Thus, when we are allocating
	 * individual devices, we must specify how large the individual device
	 * is along with the number we want ('count').
	 */
	if (allocate) {
		if (meta_lvs || data_lvs) {
			uint32_t stripes, mirrors, metadata_area_count = count;

			/* Amount of extents for the rimage device(s) */
			if (segtype_is_striped_raid(seg->segtype)) {
				stripes = count;
				mirrors = 1;
				extents = count * (lv->le_count / _data_rimages_count(seg, seg->area_count));

				PFLA("stripes=%u lv->le_count=%u data_rimages_count=%u", stripes, lv->le_count, _data_rimages_count(seg, seg->area_count));
			} else {
				stripes = 1;
				mirrors = count;
				extents = lv->le_count;
			}

			if (!(ah = allocate_extents(lv->vg, NULL, segtype,
							stripes, mirrors, metadata_area_count,
							seg->region_size, extents,
							pvs, lv->alloc, 0, parallel_areas)))
				return_0;
		}
	}
	PFLA("count=%u extents=%u", count, extents);

	for (s = 0; s < count; s++) {
		/*
		 * The allocation areas are grouped together.  First
		 * come the rimage allocated areas, then come the metadata
		 * allocated areas.  Thus, the metadata areas are pulled
		 * from 's + count'.
		 */

		/*
		 * If the segtype is raid0, we may avoid allocating metadata LVs
		 * to accompany the data LVs by not passing in @meta_lvs
		 */
		if (meta_lvs) {
			if (!(lvl_array[s + count].lv = _alloc_image_component(lv, NULL, ah, s + count, RAID_META)))
				goto err;

			dm_list_add(meta_lvs, &(lvl_array[s + count].list));
		}

		if (data_lvs) {
			if (!(lvl_array[s].lv = _alloc_image_component(lv, NULL, ah, s, RAID_IMAGE)))
				goto err;

			dm_list_add(data_lvs, &(lvl_array[s].list));
		}
	}

	PFL();
	if (ah)
		alloc_destroy(ah);
	PFL();

	return 1;
err:
	alloc_destroy(ah);
	return 0;
}

/* Return reshape LEs per device for @seg */
static uint32_t _reshape_les_per_dev(struct lv_segment *seg)
{
	return seg->reshape_len / _data_rimages_count(seg, seg->area_count);
}

/*
 * Relocate @out_of_place_les_per_disk from @lv's data images  begin <-> end depending on @to_end
 *
 * to_end != 0 -> begin -> end
 * to_end == 0 -> end -> begin
 */
static int _relocate_reshape_space(struct logical_volume *lv, int to_end)
{
	uint32_t le, end, s, len_per_dlv;
	struct logical_volume *dlv;
	struct lv_segment *seg = first_seg(lv);
	struct lv_segment *data_seg;
	struct dm_list *where;

	if (!seg->reshape_len ||
			!(len_per_dlv = _reshape_les_per_dev(seg))) {
		log_error(INTERNAL_ERROR "No reshape space to relocate");
		return 0;
	}

	PFLA("seg->area_count=%u", seg->area_count);
	/*
	 * Move the reshape LEs of each stripe (i.e. the data image sub lv)
	 * in the first/last segments across to new segments of just use
	 * them in case size fits
	 */
	for (s = 0; s < seg->area_count; s++) {
		dlv = seg_lv(seg, s);

		/* Move to the end -> start from 0 and end with reshape LEs */
		if (to_end) {
			le = 0;
			end = len_per_dlv;

			/* Move to the beginning -> from "end - reshape LEs" to end  */
		} else {
			le = dlv->le_count - len_per_dlv;
			end = dlv->le_count;
		}

		PFLA("len_per_dlv=%u le=%u end=%u", len_per_dlv, le, end);
		dm_list_iterate_items(data_seg, &dlv->segments)
			PFLA("1. dlv=%s data_seg->le=%u data_seg->len=%u pe=%u", dlv->name, data_seg->le, data_seg->len, seg_pe(data_seg, 0));


		/* Ensure segment boundary at begin/end of reshape space */
		if (!lv_split_segment(dlv, to_end ? end : le))
			return_0;

		dm_list_iterate_items(data_seg, &dlv->segments)
			PFLA("2. dlv=%s data_seg->le=%u data_seg->len=%u pe=%u", dlv->name, data_seg->le, data_seg->len, seg_pe(data_seg, 0));

		/* Find start segment */
		data_seg = find_seg_by_le(dlv, le);
		while (le < end) {
			struct lv_segment *n = dm_list_item(data_seg->list.n, struct lv_segment);

			le += data_seg->len;
			/* select destination to move to (begin/end) */
			where = to_end ? &dlv->segments : dlv->segments.n;
			dm_list_move(where, &data_seg->list);
			data_seg = n;
		}

		dm_list_iterate_items(data_seg, &dlv->segments)
			PFLA("3. dlv=%s data_seg->le=%u data_seg->len=%u pe=%u", dlv->name, data_seg->le, data_seg->len, seg_pe(data_seg, 0));

		/* Adjust starting LEs of data lv segments after move */;
		le = 0;
		dm_list_iterate_items(data_seg, &dlv->segments) {
			data_seg->le = le;
			le += data_seg->len;
		}
		dm_list_iterate_items(data_seg, &dlv->segments)
			PFLA("4. dlv=%s data_seg->le=%u data_seg->len=%u pe=%u", dlv->name, data_seg->le, data_seg->len, seg_pe(data_seg, 0));

	}

	return 1;
}

/*
 * Check if we've got out of space reshape
 * capacity in @lv and allocate if necessary.
 *
 * We inquire the targets status interface to retrieve
 * the current data_offset and the device size and
 * compare that to the size of the component image LV
 * to tell if an extension of the LV is needed or
 * existing space can just be used,
 *
 * Three different scenarios need to be covered:
 *
 *  - we have to reshape forwards
 *    (true for adding disks to a raid set) ->
 *    add extent to each component image upfront
 *    or move an exisiting one at the end across;
 *    kernel will set component devs data_offset to
 *    the passed in one and new_data_offset to 0,
 *    i.e. the data starts at offset 0 after the reshape
 *
 *  - we have to reshape backwards
 *    (true for removing disks form a raid set) ->
 *    add extent to each component image by the end
 *    or use already exisiting one from a previous reshape;
 *    kernel will leave the data_offset of each component dev
 *    at 0 and set new_data_offset to the passed in one,
 *    i.e. the data will be at offset new_data_offset != 0
 *    after the reshape
 *
 *  - we are free to reshape either way
 *    (true for layout changes keeping number of disks) ->
 *    let the kernel identify free out of place reshape space
 *    and select the appropriate data_offset and 
 *    reshape direction
 *
 * Kernel will always be told to put data offset
 * on an extent boundary.
 * When we convert to mappings outside MD ones such as linear,
 * striped and mirror _and_ data_offset != 0, split the first segment
 * and adjust the rest to remove the reshape space.
 * If it's at the end, just lv_reduce() and set seg->reshape_len to 0.
 *
 * Does not write metadata!
 */
enum alloc_where { alloc_begin, alloc_end, alloc_anywhere };
static int _lv_alloc_reshape_space(struct logical_volume *lv,
		enum alloc_where where,
		struct dm_list *allocate_pvs)
{
	/* Reshape LEs per disk minimum one MiB for now... */
	uint32_t out_of_place_les_per_disk = max(2048ULL / (unsigned long long) lv->vg->extent_size, 1ULL);
	uint64_t data_offset, dev_sectors;
	struct lv_segment *seg = first_seg(lv);

	/* Get data_offset and dev_sectors from the kernel */
	if (!lv_raid_offset_and_sectors(lv, &data_offset, &dev_sectors)) {
		log_error("Can't get data offset and dev size for %s from kernel",
				display_lvname(lv));
		return 0;
	}

	PFLA("data_offset=%llu dev_sectors=%llu seg->reshape_len=%u out_of_place_les_per_disk=%u lv->le_count=%u", (unsigned long long) data_offset, (unsigned long long) dev_sectors, seg->reshape_len, out_of_place_les_per_disk, lv->le_count);

	/*
	 * Check if we have reshape space allocated or extend the LV to have it
	 *
	 * first_seg(lv)->reshape_len (only segment of top level raid LV)
	 * is accounting for the data rimages so that unchanged
	 * lv_extend()/lv_reduce() can be used to allocate/free
	 */
	if (!seg->reshape_len) {
		uint32_t reshape_len = out_of_place_les_per_disk * _data_rimages_count(seg, seg->area_count);

		if (!lv_extend(lv, seg->segtype,
					_data_rimages_count(seg, seg->area_count),
					seg->stripe_size,
					1, seg->region_size,
					reshape_len /* # of reshape LEs to add */,
					allocate_pvs, lv->alloc, 0))
			return 0;

		/* Store the allocated reshape length per LV in the only segment of the top-level RAID LV */
		seg->reshape_len = reshape_len;
	}

	/* Don't set any offset in case we fail relocating reshape space */
	seg->data_offset = 0;

	/*
	 * Handle reshape space relocation
	 */
	switch (where) {
		case alloc_begin:
			/* Kernel says we have it at the end -> relocate it to the begin */
			if (!data_offset &&
					!_relocate_reshape_space(lv, 0))
				return_0;
			break;

		case alloc_end:
			/* Kernel says we have it at the beginning -> relocate it to the end */
			if (data_offset &&
					!_relocate_reshape_space(lv, 1))
				return_0;
			break;

		case alloc_anywhere:
			/* We don't care were the space is */
			break;

		default:
			log_error(INTERNAL_ERROR "Bogus reshape space allocation request");
			return 0;
	}

	/* Inform kernel about the reshape length in sectors */
	seg->data_offset = _reshape_les_per_dev(seg) * lv->vg->extent_size;

	/* At least try merging segments */
	return lv_merge_segments(lv);
}

/* Remove any reshape space from the data lvs of @lv */
static int _lv_free_reshape_space(struct logical_volume *lv)
{
	struct lv_segment *seg = first_seg(lv);

	if (seg->reshape_len) {
		/*
		 * Got reshape space on request to free it ->
		 * if at the beginning of the data LVs remap it
		 * to the end in order to lvreduce it
		 */
		if (!_lv_alloc_reshape_space(lv, alloc_end, NULL))
			return_0;

		if (!lv_reduce(lv, seg->reshape_len))
			return_0;

		seg->reshape_len = 0;
	}

	return 1;
}

/*
 * Convert linear @lv to raid by making the linear lv
 * the one data sub lv of a new top-level lv
 */
static struct lv_segment *_convert_linear_to_raid1(struct logical_volume *lv)
{
	struct lv_segment *seg = first_seg(lv);
	uint64_t flags = RAID | LVM_READ | LVM_WRITE;

	if (!seg_is_linear(seg)) {
		log_error(INTERNAL_ERROR " Called with non-linear lv %s", display_lvname(lv));
		return NULL;
	}

	if (!insert_layer_for_lv(lv->vg->cmd, lv, flags, "_rimage_0"))
		return NULL;

	seg_lv(first_seg(lv), 0)->status |= RAID_IMAGE | flags;

	/* First segment has changed because of layer insertion */
	seg = first_seg(lv);
	seg_lv(seg, 0)->status |= RAID_IMAGE | LVM_READ | LVM_WRITE;

	/* Set raid1 segtype, so that the following image allocation works */
	if (!(seg->segtype = get_segtype_from_flag(lv->vg->cmd, SEG_RAID1)))
		return NULL;

	_check_and_init_region_size(lv);

	return first_seg(lv);
}

/* Reset any rebuild or reshape disk flags on @lv, first segment already passed to the kernel */
static int _reset_flags_passed_to_kernel(struct logical_volume *lv, int *flag_cleared)
{
	uint32_t s;
	struct lv_segment *seg = first_seg(lv);
	uint64_t reset_flags = LV_REBUILD | LV_RESHAPE_DELTA_DISKS_PLUS | LV_RESHAPE_DELTA_DISKS_MINUS;

	*flag_cleared = 0;
	for (s = 0; s < seg->area_count; s++)
		if (seg_lv(seg, s)->status & reset_flags) {
			seg_lv(seg, s)->status &= ~reset_flags;
			*flag_cleared = 1;
		}

	if (seg->data_offset) {
		seg->data_offset = 0;
		*flag_cleared = 1;
	}

	if (*flag_cleared) {
		if (!vg_write(lv->vg) || !vg_commit(lv->vg)) {
			log_error("Failed to clear flags for %s components",
					display_lvname(lv));
			return 0;
		}

		backup(lv->vg);
	}

	return 1;
}

/* Area reorder helper: swap 2 LV segment areas @a1 and @a2 */
static void _swap_areas(struct lv_segment_area *a1, struct lv_segment_area *a2)
{
	struct lv_segment_area tmp = *a1;
	char *tmp_name;

	*a1 = *a2;
	*a2 = tmp;
#if 0
	/* Rename LVs ? */
	tmp_name = a1->u.lv.lv->name;
	a1->u.lv.lv->name = a2->u.lv.lv->name;
	a2->u.lv.lv->name = tmp_name;
#endif
}

/*
 * Reorder the areas in the first segment of @seg to suit raid10/raid0 layout
 *
 * Examples with 6 disks indexed 0..5:
 *
 * raid0  (012345) -> raid10 (031425) order
 * idx     024135
 * raid10 (012345) -> raid0  (024135/135024) order depending on mirror selection
 * idx     031425
 * _or_ (variations possible)
 * idx     304152
 *
 */
static void _raid10_reorder_seg_areas(struct lv_segment *seg, int to_raid10)
{
	unsigned s, ss, xchg;
	uint32_t half_areas = seg->area_count / 2;
	short unsigned idx[seg->area_count];
	unsigned i = 0;

	/* Set up positional index array */
	if (to_raid10)
		for (s = 0; s < seg->area_count; s++)
			idx[s] = s < half_areas ? s * 2 : (s - half_areas) * 2 + 1;
	else
#if 1
		for (s = 0; s < seg->area_count; s++)
			idx[s < half_areas ? s * 2 : (s - half_areas) * 2 + 1] = s;
#else
	/* This selection casues image name suffixes to start > 0 and needs names shifting! */
	for (s = 0; s < seg->area_count; s++)
		idx[s < half_areas ? s * 2 + 1 : (s - half_areas) * 2] = s;
#endif
	/* Sort areas */
	do {
		xchg = seg->area_count;

		for (s = 0; s < seg->area_count ; s++)
			if (idx[s] == s)
				xchg--;

			else {
				_swap_areas(seg->areas + s, seg->areas + idx[s]);
				_swap_areas(seg->meta_areas + s, seg->meta_areas + idx[s]);
				ss = idx[idx[s]];
				idx[idx[s]] = idx[s];
				idx[s] = ss;
			}
		i++;
	} while (xchg);

	PFLA("%d iterations", i);
	for (s = 0; s < seg->area_count; s++)
		PFLA("seg_lv(seg, %u)->name=%s", s, seg_lv(seg, s)->name);
}

/* Write vg of @lv, suspend @lv and commit the vg */
static int _vg_write_lv_suspend_vg_commit(struct logical_volume *lv)
{
	if (!vg_write(lv->vg)) {
		log_error("Failed to write changes to %s in %s",
				lv->name, lv->vg->name);
		return 0;
	}

	if (!suspend_lv(lv->vg->cmd, lv)) {
		log_error("Failed to suspend %s before committing changes",
				display_lvname(lv));
		vg_revert(lv->vg);
		return 0;
	}

	if (!vg_commit(lv->vg)) {
		log_error("Failed to commit changes to %s in %s",
				lv->name, lv->vg->name);
		return 0;
	}

	return 1;
}

/*
 * _raid_extract_images
 * @lv
 * @new_count:  The absolute count of images (e.g. '2' for a 2-way mirror)
 * @target_pvs:  The list of PVs that are candidates for removal
 * @shift:  If set, use _shift_image_components().
 *          Otherwise, leave the [meta_]areas as AREA_UNASSIGNED and
 *          seg->area_count unchanged.
 * @extracted_[meta|data]_lvs:  The LVs removed from the array.  If 'shift'
 *                              is set, then there will likely be name conflicts.
 *
 * This function extracts _both_ portions of the indexed image.  It
 * does /not/ commit the results.  (IOW, erroring-out requires no unwinding
 * of operations.)
 *
 * Returns: 1 on success, 0 on failure
 */
static int _raid_extract_images(struct logical_volume *lv, uint32_t new_count,
		struct dm_list *target_pvs, int shift,
		struct dm_list *extracted_meta_lvs,
		struct dm_list *extracted_data_lvs)
{
	int ss;
	unsigned s, extract, i, lvl_idx = 0;
	struct lv_list *lvl_array;
	struct lv_segment *seg = first_seg(lv);
	struct segment_type *error_segtype;

	extract = seg->area_count - new_count;
	log_verbose("Extracting %u image%s from %s", extract,
			(extract > 1) ? "s" : "", display_lvname(lv));

	if ((s = dm_list_size(target_pvs)) < extract) {
		log_error("Unable to remove %d images:  Only %d device%s given.",
				extract, s, (s == 1) ? "" : "s");
		return 0;
	}

	if (!(lvl_array = dm_pool_alloc(lv->vg->vgmem, 2 * extract * sizeof(*lvl_array))))
		return_0;

	if (!(error_segtype = get_segtype_from_string(lv->vg->cmd, "error")))
		return_0;

	/*
	 * We make two passes over the devices.
	 * - The first pass we look for error LVs
	 * - The second pass we look for PVs that match target_pvs
	 */
	for (ss = (seg->area_count * 2) - 1; (ss >= 0) && extract; ss--) {
		s = ss % seg->area_count;

		if (ss / seg->area_count) {
			/* Conditions for first pass */
			if ((first_seg(seg_lv(seg, s))->segtype != error_segtype) &&
					(first_seg(seg_metalv(seg, s))->segtype != error_segtype))
				continue;

			if (!dm_list_empty(target_pvs) &&
					(target_pvs != &lv->vg->pvs)) {
				/*
				 * User has supplied a list of PVs, but we
				 * cannot honor that list because error LVs
				 * must come first.
				 */
				log_error("%s has components with error targets"
						" that must be removed first: %s.",
						display_lvname(lv), display_lvname(seg_lv(seg, s)));

				log_error("Try removing the PV list and rerun"
						" the command.");
				return 0;
			}

			log_debug("LVs with error segments to be removed: %s %s",
					display_lvname(seg_metalv(seg, s)),
					display_lvname(seg_lv(seg, s)));

		} else {
			/* Conditions for second pass */
			if (!target_pvs ||
					!lv_is_on_pvs(seg_lv(seg, s), target_pvs) ||
					!lv_is_on_pvs(seg_metalv(seg, s), target_pvs))
				continue;

			if (!_raid_in_sync(lv) &&
					(!seg_is_mirrored(seg) || !s)) {
				log_error("Unable to extract %sRAID image"
						" while RAID array is not in-sync",
						seg_is_mirrored(seg) ? "primary " : "");
				return 0;
			}
		}

		PFLA("seg_lv(seg, %u)=%s", s, seg_lv(seg,s)->name);
		if (!_extract_image_component_pair(seg, s, lvl_array + lvl_idx)) {
			log_error("Failed to extract %s from %s", seg_lv(seg, s)->name, lv->name);
			return 0;
		}

		lvl_idx += 2;
		extract--;
	}

	if (shift && !_shift_image_components(seg)) {
		log_error("Failed to shift and rename image components");
		return 0;
	}

	for (i = 0; i < lvl_idx; i += 2) {
		dm_list_add(extracted_meta_lvs, &lvl_array[i].list);
		dm_list_add(extracted_data_lvs, &lvl_array[i + 1].list);
	}

	if (extract) {
		log_error("Unable to extract enough images to satisfy request");
		return 0;
	}

	return 1;
}


/****************************************************************************/
/*
 * HM
 *
 * Add/remove metadata areas to/from raid0
 *
 * Update metadata and reload mappings if @update_and_reload
 */
static int _alloc_and_add_rmeta_devs_for_lv(struct logical_volume *lv)
{
	struct lv_segment *seg = first_seg(lv);
	struct dm_list meta_lvs;

	dm_list_init(&meta_lvs);

	log_debug_metadata("Allocating metadata LVs for %s", display_lvname(lv));
	if (!_alloc_rmeta_devs_for_lv(lv, &meta_lvs)) {
		log_error("Failed to allocate metadata LVs for %s", display_lvname(lv));
		return_0;
	}

	/* Metadata LVs must be cleared before being added to the array */
	log_debug_metadata("Clearing newly allocated metadata LVs for %s", display_lvname(lv));
	if (!_clear_lvs(&meta_lvs)) {
		log_error("Failed to initialize metadata LVs for %s", display_lvname(lv));
		return_0;
	}

	/* Set segment areas for metadata sub_lvs */
	log_debug_metadata("Adding newly allocated metadata LVs to %s", display_lvname(lv));
	if (!_add_image_component_list(seg, 1, 0, &meta_lvs, 0)) {
		log_error("Failed to add newly allocated metadata LVs to %s", display_lvname(lv));
		return_0;
	}

	return 1;
}

/*
 * HM
 *
 * Add/remove metadata areas to/from raid0
 *
 * Update metadata and reload mappings if @update_and_reload
 */
static int _raid0_add_or_remove_metadata_lvs(struct logical_volume *lv,
					     int update_and_reload,
					     struct dm_list *removal_lvs)
{
	uint64_t raid_type_flag;
	struct lv_segment *seg = first_seg(lv);

	if (seg->meta_areas) {
		log_debug_metadata("Extracting metadata LVs");
		if (!removal_lvs) {
			log_error(INTERNAL_ERROR "Called with NULL removal LVs list");
			return 0;
		}

		if (!_extract_image_component_list(seg, RAID_META, 0, removal_lvs)) {
			log_error(INTERNAL_ERROR "Failed to extract metadata LVs");
			return 0;
		}
		PFL();
		seg->meta_areas = NULL;
		raid_type_flag = SEG_RAID0;
		PFL();
	} else {
		if (!_alloc_and_add_rmeta_devs_for_lv(lv))
			return 0;

		raid_type_flag = SEG_RAID0_META;
	}

	if (!(seg->segtype = get_segtype_from_flag(lv->vg->cmd, raid_type_flag)))
		return_0;

	if (update_and_reload) {
		if (!lv_update_and_reload_origin(lv))
			return_0;

		/* If any residual LVs, eliminate them, write VG, commit it and take a backup */
		return _eliminate_extracted_lvs(lv->vg, removal_lvs);
	}

	return 1;
}

/*
 * Split off raid1 images of @lv, prefix with @split_name,
 * leave @new_count in the raid1 set and find them on @splittable_pvs
 */
int lv_raid_split(struct logical_volume *lv, const char *split_name,
		  uint32_t new_count, struct dm_list *splittable_pvs)
{
	struct lv_list *lvl;
	struct dm_list removal_lvs, data_list;
	struct cmd_context *cmd = lv->vg->cmd;
	struct logical_volume *tracking;
	struct dm_list tracking_pvs;

	dm_list_init(&removal_lvs);
	dm_list_init(&data_list);

	if (!new_count) {
		log_error("Unable to split all images from %s",
				display_lvname(lv));
		return 0;
	}

	if (!seg_is_mirrored(first_seg(lv)) ||
			segtype_is_raid10(first_seg(lv)->segtype)) {
		log_error("Unable to split logical volume of segment type, %s",
				lvseg_name(first_seg(lv)));
		return 0;
	}

	if (vg_is_clustered(lv->vg) && !lv_is_active_exclusive_locally(lv)) {
		log_error("%s must be active exclusive locally to"
				" perform this operation.", display_lvname(lv));
		return 0;
	}

	if (find_lv_in_vg(lv->vg, split_name)) {
		log_error("Logical Volume \"%s\" already exists in %s",
				split_name, lv->vg->name);
		return 0;
	}

	if (!_raid_in_sync(lv)) {
		log_error("Unable to split %s while it is not in-sync.",
				display_lvname(lv));
		return 0;
	}

	/*
	 * We only allow a split while there is tracking if it is to
	 * complete the split of the tracking sub-LV
	 */
	if (_lv_is_raid_with_tracking(lv, &tracking)) {
		if (!lv_is_on_pvs(tracking, splittable_pvs)) {
			log_error("Unable to split additional image from %s "
					"while tracking changes for %s",
					lv->name, tracking->name);
			return 0;
		}

		/* Ensure we only split the tracking image */
		dm_list_init(&tracking_pvs);
		splittable_pvs = &tracking_pvs;
		if (!get_pv_list_for_lv(tracking->vg->cmd->mem,
					tracking, splittable_pvs))
			return_0;
	}

	if (!_raid_extract_images(lv, new_count, splittable_pvs, 1,
				&removal_lvs, &data_list)) {
		log_error("Failed to extract images from %s",
				display_lvname(lv));
		return 0;
	}


	/* Convert to linear? */
	if (new_count == 1 &&
	    !_convert_raid_to_linear(lv, &removal_lvs)) {
		log_error("Failed to remove RAID layer after linear conversion");
		return 0;
	}

	/* Rename all extracted rimages */
	dm_list_iterate_items(lvl, &data_list)
		if (!_lv_name_add_string_index(cmd, &lvl->lv->name, split_name))
			return 0;

	if (!_vg_write_lv_suspend_vg_commit(lv))
		return 0;

	dm_list_iterate_items(lvl, &data_list)
		if (!activate_lv_excl_local(cmd, lvl->lv))
			return_0;

	dm_list_iterate_items(lvl, &removal_lvs)
		if (!activate_lv_excl_local(cmd, lvl->lv))
			return_0;

	if (!resume_lv(lv->vg->cmd, lv_lock_holder(lv))) {
		log_error("Failed to resume %s after committing changes",
			  display_lvname(lv));
		return 0;
	}

	return _eliminate_extracted_lvs(lv->vg, &removal_lvs);
}


/*
 * lv_raid_split_and_track
 * @lv
 * @splittable_pvs
 *
 * Only allows a single image to be split while tracking.  The image
 * never actually leaves the mirror.  It is simply made visible.  This
 * action triggers two things: 1) users are able to access the (data) image
 * and 2) lower layers replace images marked with a visible flag with
 * error targets.
 *
 * Returns: 1 on success, 0 on error
 */
int lv_raid_split_and_track(struct logical_volume *lv,
			    struct dm_list *splittable_pvs)
{
	int s;
	struct lv_segment *seg = first_seg(lv);

	if (!seg_is_mirrored(seg)) {
		log_error("Unable to split images from non-mirrored RAID");
		return 0;
	}

	if (!_raid_in_sync(lv)) {
		log_error("Unable to split image from %s while not in-sync",
			  display_lvname(lv));
		return 0;
	}

	/* Cannot track two split images at once */
	if (lv_is_raid_with_tracking(lv)) {
		log_error("Cannot track more than one split image at a time");
		return 0;
	}

	for (s = seg->area_count - 1; s >= 0; --s) {
		if (!lv_is_on_pvs(seg_lv(seg, s), splittable_pvs))
			continue;
		lv_set_visible(seg_lv(seg, s));
		seg_lv(seg, s)->status &= ~LVM_WRITE;
		break;
	}

	if (s < 0) {
		log_error("Unable to find image to satisfy request");
		return 0;
	}

	if (!lv_update_and_reload(lv))
		return_0;

	log_print_unless_silent("%s split from %s for read-only purposes.",
				seg_lv(seg, s)->name, lv->name);

	/* Activate the split (and tracking) LV */
	if (!_activate_sublv_preserving_excl(lv, seg_lv(seg, s)))
		return_0;

	log_print_unless_silent("Use 'lvconvert --merge %s' to merge back into %s",
				display_lvname(seg_lv(seg, s)), lv->name);
	return 1;
}

int lv_raid_merge(struct logical_volume *image_lv)
{
	uint32_t s;
	char *p, *lv_name;
	struct lv_list *lvl;
	struct logical_volume *lv;
	struct logical_volume *meta_lv = NULL;
	struct lv_segment *seg;
	struct volume_group *vg = image_lv->vg;
	struct logical_volume *tracking;

	if (!(lv_name = dm_pool_strdup(vg->vgmem, image_lv->name)))
		return_0;

	if (!(p = strstr(lv_name, "_rimage_"))) {
		log_error("Unable to merge non-mirror image %s.",
			  display_lvname(image_lv));
		return 0;
	}
	*p = '\0'; /* lv_name is now that of top-level RAID */

	if (!(lvl = find_lv_in_vg(vg, lv_name))) {
		log_error("Unable to find containing RAID array for %s.",
			  display_lvname(image_lv));
		return 0;
	}

	lv = lvl->lv;
	seg = first_seg(lv);

	if (!seg_is_raid1(seg)) {
		log_error("%s is no RAID1 array - refusing to merge.",
			  display_lvname(lv));
		return 0;
	}

	if (!_lv_is_raid_with_tracking(lv, &tracking)) {
		log_error("%s is not a tracking LV.",
			  display_lvname(lv));
		return 0;
	}

	if (tracking != image_lv) {
		log_error("%s is not the tracking LV of %s but %s is.",
			  display_lvname(image_lv), display_lvname(lv), display_lvname(tracking));
		return 0;
	}

	if (image_lv->status & LVM_WRITE) {
		log_error("%s is not read-only - refusing to merge.",
			  display_lvname(image_lv));
	}

	for (s = 0; s < seg->area_count; ++s)
		if (seg_lv(seg, s) == image_lv)
			meta_lv = seg_metalv(seg, s);

	if (!meta_lv) {
		log_error("Failed to find meta for %s in RAID array %s.",
			  display_lvname(image_lv),
			  display_lvname(lv));
		return 0;
	}

	if (!deactivate_lv(vg->cmd, meta_lv)) {
		log_error("Failed to deactivate %s before merging.",
			  display_lvname(meta_lv));
		return 0;
	}

	if (!deactivate_lv(vg->cmd, image_lv)) {
		log_error("Failed to deactivate %s before merging.",
			  display_lvname(image_lv));
		return 0;
	}

	lv_set_hidden(image_lv);
	image_lv->status |= (lv->status & LVM_WRITE);
	image_lv->status |= RAID_IMAGE;

	if (!lv_update_and_reload(lv))
		return_0;

	log_print_unless_silent("%s successfully merged back into %s",
				display_lvname(image_lv), display_lvname(lv));
	return 1;
}

/*
 * Rename all data sub LVs of @lv to mirror
 * or raid name depending on @direction
 */
enum mirror_raid_conv { mirror_to_raid1 = 0, raid1_to_mirror };
static int _rename_data_lvs(struct logical_volume *lv, enum mirror_raid_conv direction)
{
	uint32_t s;
	char *p;
	struct lv_segment *seg = first_seg(lv);
	static struct {
		char type_char;
		uint64_t set_flag;
		uint64_t reset_flag;
	} conv[] = {
		{ 'r', RAID_IMAGE  , MIRROR_IMAGE },
		{ 'm', MIRROR_IMAGE, RAID_IMAGE }
	};

	for (s = 0; s < seg->area_count; ++s) {
		struct logical_volume *dlv = seg_lv(seg, s);

		if (!((p = strstr(dlv->name, "_mimage_")) ||
		      (p = strstr(dlv->name, "_rimage_")))) {
			log_error(INTERNAL_ERROR "name lags image part");
			return 0;
		}

		*(p+1) = conv[direction].type_char;
		log_debug_metadata("data lv renamed to %s", dlv->name);

		dlv->status &= ~conv[direction].reset_flag;
		dlv->status |= conv[direction].set_flag;
	}

	return 1;
}

/*
 * Convert @lv with "mirror" mapping to "raid1".
 *
 * Returns: 1 on success, 0 on failure
 */
static int _convert_mirror_to_raid(struct logical_volume *lv,
				   const struct segment_type *new_segtype,
				   int update_and_reload)
{
	struct lv_segment *seg = first_seg(lv);

	if (!seg_is_mirrored(seg)) {
		log_error(INTERNAL_ERROR "mirror conversion supported only");
		return 0;
	}

	/* Remove any mirror log */
	if (seg->log_lv) {
		log_debug_metadata("Removing mirror log, %s", seg->log_lv->name);
		if (!remove_mirror_log(lv->vg->cmd, lv, NULL, 0)) {
			log_error("Failed to remove mirror log");
			return 0;
		}
	}

	/* Allocate metadata devs for all mimage ones (writes+commits metadata) */
	if (!_alloc_and_add_rmeta_devs_for_lv(lv))
		return 0;

	/* Rename all data sub lvs for "*_rimage_*" to "*_mimage_*" */
	if (!_rename_data_lvs(lv, mirror_to_raid1))
		return 0;

	init_mirror_in_sync(1);

	log_debug_metadata("Setting new segtype and status of %s", display_lvname(lv));
	seg->segtype = new_segtype;
	lv->status &= ~(MIRROR | MIRRORED);
	lv->status |= RAID;
	seg->status |= RAID;

	return update_and_reload ? lv_update_and_reload(lv) : 1;
}

/*
 * Convert @lv with "raid1" mapping to "mirror".
 *
 * Returns: 1 on success, 0 on failure
 */
static int _convert_raid1_to_mirror(struct logical_volume *lv,
				    const struct segment_type *new_segtype,
				    struct dm_list *allocatable_pvs,
				    int update_and_reload,
				    struct dm_list *removal_lvs)
{
	uint32_t image_count = lv_raid_image_count(lv);
	struct lv_segment *seg = first_seg(lv);

	if (!seg_is_raid1(seg)) {
		log_error(INTERNAL_ERROR "raid1 conversion supported only");
		return 0;
	}

	if (!_check_max_mirror_devices(image_count)) {
		log_error("Unable to convert %s LV %s with %u images to %s",
			  SEG_TYPE_NAME_RAID1, display_lvname(lv), image_count, SEG_TYPE_NAME_MIRROR);
		log_error("Please at least reduce to the maximum of %u images with \"lvconvert -m%u %s\"",
			  DEFAULT_MIRROR_MAX_IMAGES, DEFAULT_MIRROR_MAX_IMAGES - 1, display_lvname(lv));
		return 0;
	}

	/* Remove rmeta LVs */
	log_debug_metadata("Extracting and renaming metadata LVs");
	if (!_extract_image_component_list(seg, RAID_META, 0, removal_lvs))
		return 0;

	seg->meta_areas = NULL;

	/* Rename all data sub lvs for "*_rimage_*" to "*_mimage_*" */
	if (!_rename_data_lvs(lv, raid1_to_mirror))
		return 0;

	log_debug_metadata("Setting new segtype %s for %s", new_segtype->name, lv->name);
	seg->segtype = new_segtype;
	lv->status &= ~RAID;
	seg->status &= ~RAID;
	lv->status |= (MIRROR | MIRRORED);

	/* Add mirrored mirror_log LVs */
	if (!add_mirror_log(lv->vg->cmd, lv, 1, seg->region_size, allocatable_pvs, lv->vg->alloc)) {
		log_error("Unable to add mirror log to %s", display_lvname(lv));
		return 0;
	}

	if (update_and_reload) {
		if (!lv_update_and_reload(lv))
			return_0;

		/* Eliminate the residual LVs, write VG, commit it and take a backup */
		return _eliminate_extracted_lvs(lv->vg, removal_lvs);
	}

	return 1;
}

/* BEGIN: striped -> raid0 conversion */
/*
 * HM
 *
 * Helper convert striped to raid0
 *
 * For @lv, empty hidden LVs in @new_data_lvs have been created by the caller.
 *
 * All areas from @lv segments are being moved to new
 * segments allocated with area_count=1 for @new_data_lvs.
 *
 * Returns: 1 on success, 0 on failure
 */
static int _striped_to_raid0_move_segs_to_raid0_lvs(struct logical_volume *lv,
						    struct dm_list *new_data_lvs)
{
	uint32_t area_idx = 0, le;
	struct logical_volume *dlv;
	struct lv_segment *seg_from, *seg_new, *tmp;
	struct dm_list *l;
	struct segment_type *segtype;

	if (!(segtype = get_segtype_from_string(lv->vg->cmd, SEG_TYPE_NAME_STRIPED)))
		return_0;

	dm_list_iterate(l, new_data_lvs) {
		dlv = (dm_list_item(l, struct lv_list))->lv;

		le = 0;
		dm_list_iterate_items(seg_from, &lv->segments) {
			uint64_t status = RAID | SEG_RAID | (seg_from->status & (LVM_READ | LVM_WRITE));

			/* Allocate a segment with one area for each segment in the striped LV */
			if (!(seg_new = alloc_lv_segment(segtype, dlv,
							 le, seg_from->area_len - seg_from->reshape_len,
							 seg_from->reshape_len, status,
							 seg_from->stripe_size, NULL, 1 /* area_count */,
							 seg_from->area_len, seg_from->chunk_size,
							 0 /* region_size */, 0, NULL)))
				return_0;

			seg_type(seg_new, 0) = AREA_UNASSIGNED;
			dm_list_add(&dlv->segments, &seg_new->list);
			le += seg_from->area_len;

			/* Move the respective area across to our new segment */
			if (!move_lv_segment_area(seg_new, 0, seg_from, area_idx))
				return_0;
		}

		/* Adjust le count and lv size */
		dlv->le_count = le;
		dlv->size = (uint64_t) le * lv->vg->extent_size;
		area_idx++;
	}

	/* Remove the empty segments from the striped LV */
	dm_list_iterate_items_safe(seg_from, tmp, &lv->segments)
		dm_list_del(&seg_from->list);

	return 1;
}

/*
 * Helper convert striped to raid0
 *
 * Add list of data device in @new_data_devs to @lv
 *
 * Returns: 1 on success, 0 on failure
 */
static int _striped_to_raid0_alloc_raid0_segment(struct logical_volume *lv,
						 uint32_t area_count,
						 struct lv_segment *seg)
{
	struct lv_segment *seg_new;
	struct segment_type *segtype = get_segtype_from_flag(lv->vg->cmd, SEG_RAID0);

PFLA("seg->stripe_size=%u seg->chunk_size=%u", seg->stripe_size, seg->chunk_size);

	/* Allocate single segment to hold the image component areas */
	if (!(seg_new = alloc_lv_segment(segtype, lv,
					 0 /* le */, lv->le_count /* len */,
					 0 /* reshape_len */,
					 seg->status,
					 seg->stripe_size, NULL /* log_lv */,
					 area_count, lv->le_count, seg->chunk_size,
					 0 /* seg->region_size */, 0u /* extents_copied */ ,
					 NULL /* pvmove_source_seg */)))
		return_0;

PFLA("seg_new->stripe_size=%u seg_new->chunk_size=%u", seg_new->stripe_size, seg_new->chunk_size);
	/* Add new segment to LV */
	dm_list_add(&lv->segments, &seg_new->list);

	return 1;
}

/* Check that @lv has one stripe one, i.e. same stripe count in all of its segements */
static int _lv_has_one_stripe_zone(struct logical_volume *lv)
{
	struct lv_segment *seg;
	unsigned area_count = first_seg(lv)->area_count;

	dm_list_iterate_items(seg, &lv->segments)
		if (seg->area_count != area_count)
			return 0;

	return 1;
}

/*
 * HM
 *
 * Helper: convert striped to raid0
 *
 * Inserts hidden LVs for all segments and the parallel areas in @lv and moves 
 * given segments and areas across.
 *
 * Optionally allocates metadata devs if @alloc_metadata_devs
 * Optionally updates metadata and reloads mappings if @update_and_reload
 *
 * Returns: 1 on success, 0 on failure
 */
static struct lv_segment *_convert_striped_to_raid0(struct logical_volume *lv,
						    int alloc_metadata_devs,
						    int update_and_reload)
{
	struct lv_segment *data_lv_seg, *seg = first_seg(lv);
	struct dm_list data_lvs;
	struct dm_list *l;
	unsigned area_count = seg->area_count;

	if (!seg_is_striped(seg)) {
		log_error("Cannot convert non-striped LV %s to raid0", display_lvname(lv));
		return NULL;
	}

	/* Check for not yet supported varying area_count on multi-segment striped LVs */
	if (!_lv_has_one_stripe_zone(lv)) {
		log_error("Cannot convert striped LV %s with varying stripe count to raid0",
			  display_lvname(lv));
		return NULL;
	}

	dm_list_init(&data_lvs);

	/* FIXME: insert_layer_for_lv() not suitable */
	/* Allocate empty rimage components in order to be able to support multi-segment "striped" LVs */
	if (!_alloc_image_components(lv, 0, NULL, area_count, NULL, &data_lvs)) {
		log_error("Failed to allocate empty image components for raid0 LV %s.", display_lvname(lv));
		return_NULL;
	}

	/* Image components are being allocated with LV_REBUILD preset and raid0 does not need it */
	dm_list_iterate(l, &data_lvs)
		(dm_list_item(l, struct lv_list))->lv->status &= ~LV_REBUILD;

	/* Move the AREA_PV areas across to the new rimage components */
	if (!_striped_to_raid0_move_segs_to_raid0_lvs(lv, &data_lvs)) {
		log_error("Failed to insert linear LVs underneath %s.", display_lvname(lv));
		return_NULL;
	}

	/* Allocate new top-level LV segment using credentials of first ne data lv for stripe_size... */
	data_lv_seg = first_seg(dm_list_item(dm_list_first(&data_lvs), struct lv_list)->lv);
	data_lv_seg->stripe_size = seg->stripe_size;
	if (!_striped_to_raid0_alloc_raid0_segment(lv, area_count, data_lv_seg)) {
		log_error("Failed to allocate new raid0 segement for LV %s.", display_lvname(lv));
		return_NULL;
	}

	/* Get reference to new allocated raid0 segment _before_ adding the data lvs */
	seg = first_seg(lv);

	/* Add data lvs to the top-level lvs segment */
	if (!_add_image_component_list(seg, 1, 0, &data_lvs, 0))
		return NULL;

	/* Adjust the segment type to raid0 */
	if (!(seg->segtype = get_segtype_from_flag(lv->vg->cmd, SEG_RAID0)))
		return NULL;

	lv->status |= RAID;

	/* Allocate metadata lvs if requested */
	if (alloc_metadata_devs &&
	    !_raid0_add_or_remove_metadata_lvs(lv, update_and_reload, NULL))
		return NULL;

	if (update_and_reload &&
	    !lv_update_and_reload(lv))
		return NULL;

	return first_seg(lv);
}
/* END: striped -> raid0 conversion */


/* BEGIN: raid0 -> striped conversion */

/* HM Helper: walk the segment lvs of a segment @seg and find smallest area at offset @area_le */
static uint32_t _smallest_segment_lvs_area(struct lv_segment *seg, uint32_t area_le)
{
	uint32_t r = ~0, s;

	/* Find smallest segment of each of the data image lvs at offset area_le */
	for (s = 0; s < seg->area_count; s++) {
		struct lv_segment *seg1 = find_seg_by_le(seg_lv(seg, s), area_le);

		r = min(r, seg1->le + seg1->len - area_le);
	}

	return r;
}

/* HM Helper: Split segments in segment LVs in all areas of @seg at offset @area_le) */
static int _split_area_lvs_segments(struct lv_segment *seg, uint32_t area_le)
{
	uint32_t s;

	/* Make sure that there's segments starting at area_le all data LVs */
	if (area_le < seg_lv(seg, 0)->le_count)
		for (s = 0; s < seg->area_count; s++)
			if (!lv_split_segment(seg_lv(seg, s), area_le)) {
				log_error(INTERNAL_ERROR "splitting data lv segment");
				return_0;
			}

	return 1;
}

/* HM Helper: allocate a new striped segment and add it to list @new_segments */
static int _alloc_new_striped_segment(struct logical_volume *lv,
				      uint32_t le, uint32_t area_len,
				      struct dm_list *new_segments)
{
	struct lv_segment *seg = first_seg(lv), *new_seg;
	struct segment_type *striped_segtype;

	if (!(striped_segtype = get_segtype_from_string(lv->vg->cmd, SEG_TYPE_NAME_STRIPED)))
		return_0;

	/* Allocate a segment with seg->area_count areas */
	if (!(new_seg = alloc_lv_segment(striped_segtype, lv, le, area_len * seg->area_count,
					 0 /* seg->reshape_len */, seg->status & ~RAID,
					 seg->stripe_size, NULL, seg->area_count,
					 area_len, seg->chunk_size, 0, 0, NULL)))
		return_0;

	dm_list_add(new_segments, &new_seg->list);

	return 1;
}

/*
 * HM
 *
 * All areas from @lv image component LV's segments are
 * being split at "striped" compatible boundaries and
 * moved to @new_segments allocated.
 *
 * The metadata+data component LVs are being mapped to an
 * error target and linked to @removal_lvs for callers
 * disposal.
 *
 * Returns: 1 on success, 0 on failure
 */
static int _raid0_to_striped_retrieve_segments_and_lvs(struct logical_volume *lv,
						       struct dm_list *removal_lvs)
{
	uint32_t s, area_le, area_len, le;
	struct lv_segment *seg = first_seg(lv), *seg_to;
	struct dm_list new_segments;
	struct lv_list *lvl_array, *lvl;

	if (!(lvl_array = dm_pool_alloc(lv->vg->vgmem, 2 * seg->area_count * sizeof(*lvl_array))))
		return_0;

	dm_list_init(&new_segments);

	/*
	 * Walk all segments of all data LVs splitting them up at proper boundaries
	 * and create the number of new striped segments we need to move them across
	 */
	area_le = le = 0;
	while (le < lv->le_count) {
		area_len = _smallest_segment_lvs_area(seg, area_le);
		area_le += area_len;

		if (!_split_area_lvs_segments(seg, area_le))
			return 0;
		if (!_alloc_new_striped_segment(lv, le, area_len, &new_segments))
			return 0;

		le += area_len * seg->area_count;
	}

	/* Now move the prepared split areas across to the new segments */
	area_le = 0;
	dm_list_iterate_items(seg_to, &new_segments) {
		struct lv_segment *data_seg;

		for (s = 0; s < seg->area_count; s++) {
			data_seg = find_seg_by_le(seg_lv(seg, s), area_le);

			/* Move the respective area across to our new segments area */
			if (!move_lv_segment_area(seg_to, s, data_seg, 0))
				return_0;
		}

		/* Presumes all data LVs have equal size */
		area_le += data_seg->len;
	}

	/* Loop the areas and set any metadata LVs and all data LVs to error segments and remove them */
	for (s = 0; s < seg->area_count; s++) {
		/* If any metadata lvs -> remove them */
		lvl = &lvl_array[seg->area_count + s];
		if (seg->meta_areas &&
		    (lvl->lv = seg_metalv(seg, s))) {
			dm_list_add(removal_lvs, &lvl->list);
			if (!_remove_and_set_error_target(lvl->lv, seg))
				return_0;
		}


		lvl = &lvl_array[s];
		lvl->lv = seg_lv(seg, s);
		dm_list_add(removal_lvs, &lvl->list);
		if (!_remove_and_set_error_target(lvl->lv, seg))
			return_0;
	}

	/*
	 * Remove the one segment holding the image component areas
	 * from the top-level LV and add the new segments to it
	 */
	dm_list_del(&seg->list);
	dm_list_splice(&lv->segments, &new_segments);

	lv->status &= ~RAID;
	lv->status |= LVM_READ | LVM_WRITE;
	lv_set_visible(lv);

	return 1;
}

/*
 * HM
 *
 * Helper convert raid0 to striped
 *
 * Convert a RAID0 set to striped
 *
 * Returns: 1 on success, 0 on failure
 *
 * HM FIXME: check last_seg(lv)->reshape_len and reduce LV aprropriately
 */
static int _convert_raid0_to_striped(struct logical_volume *lv,
				     int update_and_reload,
				     struct dm_list *removal_lvs)
{
	struct lv_segment *seg = first_seg(lv);

PFLA("seg->segtype=%s", lvseg_name(seg));

	/* Caller should ensure, but... */
	if (!seg_is_any_raid0(seg)) {
		log_error(INTERNAL_ERROR "Can't cope with %s", lvseg_name(seg));
		return 0;
	}
PFL();
	/* Move the AREA_PV areas across to new top-level segments of type "striped" */
	if (!_raid0_to_striped_retrieve_segments_and_lvs(lv, removal_lvs)) {
		log_error("Failed to retrieve raid0 segments from %s.", lv->name);
		return_0;
	}

	if (!(first_seg(lv)->segtype = get_segtype_from_string(lv->vg->cmd, SEG_TYPE_NAME_STRIPED)))
		return_0;
PFL();
	if (update_and_reload) {
		if (!lv_update_and_reload(lv))
			return_0;
PFL();
		/* Eliminate the residual LVs, write VG, commit it and take a backup */
		return _eliminate_extracted_lvs(lv->vg, removal_lvs);
	}

	return 1;
}
/* END: raid0 -> striped conversion */

/* BEGIN: raid <-> raid conversion */
/*
 *
 * HM
 *
 * Compares current raid disk count of active RAID set @lv to
 * requested @dev_count returning number of disks as of healths
 * string in @devs_health and synced disks in @devs_in_sync
 *
 * Returns:
 *
 * 	0: error
 * 	1: kernel dev count = @dev_count
 * 	2: kernel dev count < @dev_count
 * 	3: kernel dev count > @dev_count
 *
 */
static int _reshaped_state(struct logical_volume *lv, const unsigned dev_count,
			   unsigned *devs_health, unsigned *devs_in_sync)
{
	unsigned d;
	uint32_t kernel_devs;
	char *raid_health;

	*devs_health = *devs_in_sync = 0;

	if (!lv_raid_dev_count(lv, &kernel_devs)) {
		log_error("Failed to get device count");
		return_0;
	}

	if (!lv_raid_dev_health(lv, &raid_health)) {
		log_error("Failed to get device health");
		return_0;
	}

	d = (unsigned) strlen(raid_health);
	while (d--) { 
		(*devs_health)++;
		if (raid_health[d] == 'A')
			(*devs_in_sync)++;
	}

	if (kernel_devs == dev_count)
		return 1;

	return kernel_devs < dev_count ? 2 : 3;
}

static int _lv_change_image_count(struct logical_volume *lv, const struct segment_type *new_segtype,
				  uint32_t new_image_count,
				  struct dm_list *allocate_pvs, struct dm_list *removal_lvs);
/*
 * Reshape logical volume @lv by adding/removing stripes
 * (absolute new stripes given in @new_stripes), changing
 * layout (e.g. raid5_ls -> raid5_ra) or changing
 * stripe size to @new_stripe_size.
 *
 * In case of disk addition, any PVs listed in
 * @allocate_pvs will be used for allocation of
 * new stripes.
 */
static int _lv_update_and_reload_origin_eliminate_lvs(struct logical_volume *lv,
						      struct dm_list *removal_lvs);
static int _raid_reshape(struct logical_volume *lv,
			 const struct segment_type *new_segtype,
			 int yes, int force,
			 const unsigned new_stripes,
			 const unsigned new_stripe_size,
		 	 struct dm_list *allocate_pvs)
{
	int r;
	int too_few = 0;
	uint32_t new_len, s;
	struct lv_segment *seg = first_seg(lv);
	unsigned old_dev_count = seg->area_count;
	unsigned new_dev_count = new_stripes + seg->segtype->parity_devs;
	unsigned devs_health, devs_in_sync;
	struct dm_list removal_lvs;
	struct lvinfo info = { 0 };

	dm_list_init(&removal_lvs);

PFLA("old_dev_count=%u new_dev_count=%u", old_dev_count, new_dev_count);
	if (seg->segtype == new_segtype &&
	    old_dev_count == new_dev_count &&
	    seg->stripe_size == new_stripe_size) {
		log_error("Nothing to do");
		return 0;
	}

	if (!seg_is_raid4(seg) && !seg_is_any_raid5(seg) && !seg_is_any_raid6(seg) &&
	    (old_dev_count != new_dev_count || seg->stripe_size != new_stripe_size)) {
		log_error("Can't reshape raid0/striped LV %s", display_lvname(lv));
		log_error("You may want to convert to raid4/5/6 first");
		return 0;
	}

	/* raid4/5 with N image component pairs (i.e. N-1 stripes): allow for raid4/5 reshape to 2 devices, i.e. raid1 layout */
	if (seg_is_raid4(seg) || seg_is_any_raid5(seg)) {
		if (new_stripes < 1)
			too_few = 1;

	/* raid6 device count: check for 2 stripes minimum */
	} else if (new_stripes < 2)
		too_few = 1;

	if (too_few) {
		log_error("Too few stripes requested");
		return 0;
	}

	seg->stripe_size = new_stripe_size;
	switch ((r = _reshaped_state(lv, old_dev_count, &devs_health, &devs_in_sync))) {
	case 1:
		/*
		 * old_dev_count == kernel_dev_count
		 *
		 * Check for device health
		 */
		if (devs_in_sync < devs_health) {
			log_error("Can't reshape out of sync LV %s", display_lvname(lv));
			return 0;
		}
PFL()
		/* device count and health are good -> ready to add disks */
		break;

	case 2:
		if (devs_in_sync == new_dev_count)
			break;

		/* Possible after a shrinking reshape and forgotten device removal */
		log_error("Device count is incorrect. "
			  "Forgotten \"lvconvert --stripes %d %s\" to remove %u images after reshape?",
			  devs_in_sync - seg->segtype->parity_devs, display_lvname(lv),
			  old_dev_count - devs_in_sync);
		return 0;

	default:
		log_error(INTERNAL_ERROR "Bad return=%d provided to %s.", r, __func__);
		return 0;
	}

	/* Handle disk addition reshaping */
	if (old_dev_count < new_dev_count) {
PFL();
		/* Conversion to raid1 */
		if (old_dev_count == 2)
			new_segtype = seg->segtype;

		if (!lv_info(lv->vg->cmd, lv, 0, &info, 1, 0) && driver_version(NULL, 0)) {
			log_error("lv_info failed: aborting");
			return 0;
		}

		new_len = _data_rimages_count(seg, new_dev_count) * (seg->len / _data_rimages_count(seg, old_dev_count));
		log_warn("WARNING: Adding stripes to active%s logical volume %s will grow "
			 "it from %u to %u extents!\n"
			 "You may want to run \"lvresize -l%u %s\" to shrink it after\n"
			 "the conversion has finished or make use of the gained capacity",
			 info.open_count ? " and open" : "",
			 display_lvname(lv), seg->len, new_len,
			 new_len, display_lvname(lv));
		if (!yes && yes_no_prompt("WARNING: Do you really want to add %u stripes to %s extending it? [y/n]: ",
					  new_dev_count - old_dev_count,  display_lvname(lv)) == 'n') {
			log_error("Logical volume %s NOT converted to extend", display_lvname(lv));
			return 0;
		}
		if (sigint_caught())
			return_0;

		/*
		 * Allocate free forward out of place reshape space at the beginning of all data image LVs
		 */
		if (!_lv_alloc_reshape_space(lv, alloc_begin, allocate_pvs))
			return 0;

		if (!_lv_change_image_count(lv, new_segtype, new_dev_count, allocate_pvs, NULL))
			return 0;

		if (seg->segtype != new_segtype)
			log_warn("Ignoring layout change on device adding reshape");

	/*
 	 * HM FIXME: I don't like the flow doing this here and in _raid_add_images on addition
	 */

	/* Handle disk removal reshaping */
	} else if (old_dev_count > new_dev_count) {
		switch (_reshaped_state(lv, new_dev_count, &devs_health, &devs_in_sync)) {
		case 3:
			/*
			 * Disk removal reshape step 1:
			 *
			 * we got more disks active than requested via @new_stripes
			 *
			 * -> flag the ones to remove
			 *
			 */
PFL();
			if (!lv_info(lv->vg->cmd, lv, 0, &info, 1, 0) && driver_version(NULL, 0)) {
				log_error("lv_info failed: aborting");
				return 0;
			}

			new_len = _data_rimages_count(seg, new_dev_count) *
				  (seg->len / _data_rimages_count(seg, old_dev_count));
PFLA("new_dev_count=%u _data_rimages_count(seg, new_dev_count)=%u new_len=%u", new_dev_count, _data_rimages_count(seg, new_dev_count), new_len);
			log_warn("WARNING: Removing stripes from active%s logical volume %s will shrink "
				 "it from %u to %u extents!\n"
				 "THIS MAY DESTROY (PARTS OF) YOUR DATA!\n"
				 "You may want to run \"lvresize -y -l%u %s\" _before_ the conversion starts!\n"
				 "If that leaves the logical volume larger than %u extents, grow the filesystem etc. as well",
				 info.open_count ? " and open" : "",
				 display_lvname(lv), seg->len, new_len,
				 seg->len * _data_rimages_count(seg, old_dev_count) / _data_rimages_count(seg, new_dev_count),
				 display_lvname(lv), new_len);
			log_warn("WARNING: You have to run \"lvconvert --stripes %u %s\" again after the reshapa has finished",
				 new_stripes, display_lvname(lv));

			if (!yes && yes_no_prompt("Do you really want to remove %u stripes from %s? [y/n]: ",
						  old_dev_count - new_dev_count,  display_lvname(lv)) == 'n') {
				log_error("Logical volume %s NOT converted to reduce", display_lvname(lv));
				return 0;
			}
			if (sigint_caught())
				return_0;

#if 0
			if (!force) {
				log_warn("WARNING: Can't remove stripes without --force option");
				return 0;
			}
#endif

			/*
			 * Allocate free backward out of place reshape space at the end of all data image LVs
			 */
			if (!_lv_alloc_reshape_space(lv, alloc_end, allocate_pvs))
				return 0;

			for (s = new_dev_count; s < old_dev_count; s++)
				seg_lv(seg, s)->status |= LV_RESHAPE_DELTA_DISKS_MINUS;

			if (seg->segtype != new_segtype)
				log_warn("Ignoring layout change on device removing reshape");

			break;

		case 1:
			/*
		 	* Disk removal reshape step 2:
		 	*
		 	* we got the proper (smaller) amount of devices active
		 	* for a previously finished disk removal reshape
		 	*
		 	* -> remove the freed up images
		 	*
		 	*/
PFL();
			if (!_lv_change_image_count(lv, new_segtype, new_dev_count, allocate_pvs, &removal_lvs))
				return 0;

			break;

		default:
PFL();
			log_error(INTERNAL_ERROR "Bad return provided to %s.", __func__);
			return 0;
		}

	/* Handle raid set layout reshaping (e.g. raid5_ls -> raid5_n) */
	} else {
		/*
		 * Allocate free data image LVs space for out-of-place reshape anywhere
		 */
		if (!_lv_alloc_reshape_space(lv, alloc_anywhere, allocate_pvs))
			return 0;

		seg->segtype = new_segtype;
	}

PFLA("new_segtype=%s seg->area_count=%u", new_segtype->name, seg->area_count);

	return _lv_update_and_reload_origin_eliminate_lvs(lv, &removal_lvs);
}

/*
 * HM
 *
 * TAKEOVER: copes with all raid level switches aka takeover of @lv
 *
 * Overwrites the users "--type level_algorithm" request (e.g. --type raid6_zr)
 * with the appropriate, constrained one to allow for takeover.
 *
 * raid0 can take over:
 *  raid4 - if all data disks are active.
 *  raid5 - providing it is Raid4 layout and one disk is faulty
 *  raid10 - assuming we have all necessary active disks
 *  raid1 - with (N -1) mirror drives faulty
 *
 * raid1 can take over:
 *  raid5 with 2 devices, any layout or chunk size
 *
 * raid10 can take over:
 *  raid0 - with any number of drives
 *
 * raid4 can take over:
 *  raid0 - if there is only one strip zone
 *  raid5 - if layout is right (parity on last disk)
 *
 * raid5 can take over:
 *  raid0 - if there is only one strip zone - make it a raid4 layout
 *  raid1 - if there are two drives.  We need to know the chunk size
 *  raid4 - trivial - just use a raid4 layout.
 *  raid6 - Providing it is a *_6 layout
 *
 * raid6 currently can only take over a (raid4/)raid5.  We map the
 * personality to an equivalent raid6 personality
 * with the Q block at the end.
 */
struct possible_type {
	const uint64_t current_types;
	const uint64_t possible_types;
};
/*
 * Return any segtype flags it is possible to convert to from @seg 
 *
 * HM FIXME: complete?
 */
static uint64_t _get_conversion_to_segtypes(const struct lv_segment *seg)
{
	unsigned cn;
	struct possible_type pt[] = {
		{ .current_types  = SEG_AREAS_STRIPED, /* linear, i.e. seg->area_count = 1 */
		  .possible_types = SEG_RAID1|SEG_RAID10|SEG_RAID4|SEG_RAID5_LS|SEG_RAID5_LA| \
				    SEG_RAID5_RS|SEG_RAID5_RA|SEG_RAID5_N },
		{ .current_types  = SEG_AREAS_STRIPED, /* striped, i.e. seg->area_count > 1 */
		  .possible_types = SEG_RAID0|SEG_RAID0_META|SEG_RAID10|SEG_RAID4|SEG_RAID5_N|SEG_RAID6_N_6 },
		{ .current_types  = SEG_RAID0|SEG_RAID0_META, /* seg->area_count = 1 */
		  .possible_types = SEG_RAID1|SEG_RAID10|SEG_RAID4|SEG_RAID5_LS|SEG_RAID5_LA| \
				    SEG_RAID5_RS|SEG_RAID5_RA|SEG_RAID5_N },
		{ .current_types  = SEG_RAID0|SEG_RAID0_META, /* raid0 striped, i.e. seg->area_count > 1 */
		  .possible_types = SEG_AREAS_STRIPED|SEG_RAID10|SEG_RAID4|SEG_RAID5_N|SEG_RAID6_N_6 },
		{ .current_types  = SEG_RAID1, /* only if seg->area_count = 2 */
		  .possible_types = SEG_RAID10|SEG_RAID4|SEG_RAID5_LS|SEG_RAID5_LA|SEG_RAID5_RS|SEG_RAID5_RA|SEG_RAID5_N },
		{ .current_types  = SEG_RAID4,
		  .possible_types = SEG_AREAS_STRIPED|SEG_RAID0|SEG_RAID0_META|SEG_RAID5_N|SEG_RAID6_N_6 },
		{ .current_types  = SEG_RAID5_LS,
		  .possible_types = SEG_RAID5_N|SEG_RAID5_LA|SEG_RAID5_RS|SEG_RAID5_RA|SEG_RAID6_LS_6 },
		{ .current_types  = SEG_RAID5_LA,
		  .possible_types = SEG_RAID5|SEG_RAID5_N|SEG_RAID5_LS|SEG_RAID5_RS|SEG_RAID5_RA|SEG_RAID6_LA_6 },
		{ .current_types  = SEG_RAID5_RS,
		  .possible_types = SEG_RAID5|SEG_RAID5_N|SEG_RAID5_LS|SEG_RAID5_LA| SEG_RAID5_RA|SEG_RAID6_RS_6 },
		{ .current_types  = SEG_RAID5_RA,
		  .possible_types = SEG_RAID5|SEG_RAID5_N|SEG_RAID5_LS|SEG_RAID5_RS|SEG_RAID5_LA|SEG_RAID6_RA_6 },
		{ .current_types  = SEG_RAID5_N,
		  .possible_types = SEG_AREAS_STRIPED|SEG_RAID0|SEG_RAID0_META|SEG_RAID4| \
				    SEG_RAID5_LA|SEG_RAID5_LS|SEG_RAID5_RS|SEG_RAID5_RA|SEG_RAID6_N_6 },
		{ .current_types  = SEG_RAID6_NC|SEG_RAID6_NR|SEG_RAID6_ZR,
		  .possible_types = SEG_RAID6_NC|SEG_RAID6_NR|SEG_RAID6_ZR|SEG_RAID6_N_6 },
		{ .current_types  = SEG_RAID6_LS_6,
		  .possible_types = SEG_RAID6_LA_6|SEG_RAID6_RS_6|SEG_RAID6_RA_6| \
				    SEG_RAID6_NC|SEG_RAID6_NR|SEG_RAID6_ZR|SEG_RAID6_N_6|SEG_RAID5_LS },
		{ .current_types  = SEG_RAID6_RS_6,
		  .possible_types = SEG_RAID6_LS_6|SEG_RAID6_LA_6|SEG_RAID6_RA_6| \
				    SEG_RAID6_NC|SEG_RAID6_NR|SEG_RAID6_ZR|SEG_RAID6_N_6|SEG_RAID5_RS },
		{ .current_types  = SEG_RAID6_LA_6,
		  .possible_types = SEG_RAID6_LS_6|SEG_RAID6_RS_6|SEG_RAID6_RA_6| \
				    SEG_RAID6_NC|SEG_RAID6_NR|SEG_RAID6_ZR|SEG_RAID6_N_6|SEG_RAID5_LA },
		{ .current_types  = SEG_RAID6_RA_6,
		  .possible_types = SEG_RAID6_LS_6|SEG_RAID6_LA_6|SEG_RAID6_RS_6| \
				    SEG_RAID6_NC|SEG_RAID6_NR|SEG_RAID6_ZR|SEG_RAID6_N_6|SEG_RAID5_RA },
		{ .current_types  = SEG_RAID6_N_6,
		  .possible_types = SEG_RAID6_LS_6|SEG_RAID6_LA_6|SEG_RAID6_RS_6|SEG_RAID6_RA_6| \
				    SEG_RAID6_NR|SEG_RAID6_NC|SEG_RAID6_ZR| \
				    SEG_RAID5_N|SEG_RAID4|SEG_RAID0|SEG_RAID0_META|SEG_AREAS_STRIPED },
		{ .current_types  = SEG_RAID10,
		  .possible_types = SEG_AREAS_STRIPED|SEG_RAID0|SEG_RAID0_META }
	};

	for (cn = 0; cn < ARRAY_SIZE(pt); cn++)
		if (seg->segtype->flags & pt[cn].current_types) {
			/* Skip to striped (raid0), i.e. seg->rea_count > 1 */
			if ((seg_is_striped(seg) || seg_is_any_raid0(seg)) &&
			    seg->area_count > 1 && (SEG_RAID1 & pt[cn].possible_types))
				continue;

			return pt[cn].possible_types;
		}

	return 0;
}

/*
 * Log any possible conversions for @lv
 */
/* HM FIXEM: use log_info? */
static void _log_possible_conversion_types(struct logical_volume *lv)
{
	unsigned i;
	uint64_t t, possible_types;
	const char *alias = "";
	const struct lv_segment *seg = first_seg(lv);
	const struct segment_type *segtype;

	if (!(possible_types = _get_conversion_to_segtypes(seg)))
		return;

	if (seg_is_raid1(seg) && seg->area_count != 2)
		log_warn("Conversions on raid1 LV %s only possible after \"lvconvert -m1 %s\"",
			 display_lvname(lv), display_lvname(lv));

	if (seg_is_raid5_ls(seg))
		alias = SEG_TYPE_NAME_RAID5_LS;
	else if (seg_is_raid6_zr(seg))
		alias = SEG_TYPE_NAME_RAID6_ZR;

	log_warn("Converting %s from %s%s%s%c is possible to:",
		 display_lvname(lv), _get_segtype_name(seg->segtype, seg->area_count),
		 *alias ? " (same as " : "", alias, *alias ? ')' : 0);

	/* Print any possible segment types to convert to */
	for (i = 0; i < 64; i++) {
		t = 1ULL << i;
		if ((t & possible_types) &&
		    ((segtype = get_segtype_from_flag(lv->vg->cmd, t))))
			log_warn("%s", segtype->name);
	}
}

/*
 * Find takeover raid flag for segment type flag of @seg
 */
struct _raid_flag_to_new_flag {
	uint64_t flag;
	uint64_t new_flag;
};

static uint64_t _seg_flag_to_new_flag(uint64_t flags, const struct _raid_flag_to_new_flag *rftnf, unsigned elems)
{
	while (elems--)
		if (flags & rftnf[elems].flag)
			return rftnf[elems].new_flag;

	return 0;
}

static uint64_t _raid_seg_flag_5_to_6(const struct lv_segment *seg)
{
	static const struct _raid_flag_to_new_flag r5_to_r6[] = {
		{ SEG_RAID5_LS, SEG_RAID6_LS_6 },
		{ SEG_RAID5_LA, SEG_RAID6_LA_6 },
		{ SEG_RAID5_RS, SEG_RAID6_RS_6 },
		{ SEG_RAID5_RA, SEG_RAID6_RA_6 },
		{ SEG_RAID5_N,  SEG_RAID6_N_6 },
	};

	return _seg_flag_to_new_flag(seg->segtype->flags, r5_to_r6, ARRAY_SIZE(r5_to_r6));
}

static uint64_t _raid_seg_flag_6_to_5(const struct lv_segment *seg)
{
	static const struct _raid_flag_to_new_flag r6_to_r5[] = {
		{ SEG_RAID6_LS_6, SEG_RAID5_LS },
		{ SEG_RAID6_LA_6, SEG_RAID5_LA },
		{ SEG_RAID6_RS_6, SEG_RAID5_RS },
		{ SEG_RAID6_RA_6, SEG_RAID5_RA },
		{ SEG_RAID6_N_6,  SEG_RAID5_N }
	};

	return _seg_flag_to_new_flag(seg->segtype->flags, r6_to_r5, ARRAY_SIZE(r6_to_r5));
}
/******* END: raid <-> raid conversion *******/


/*
 * Report current number of redundant disks for @total_images and @segtype 
 */
static void _seg_get_redundancy(const struct segment_type *segtype, unsigned total_images, unsigned *nr)
{
	if (segtype_is_raid10(segtype))
		*nr = total_images / 2; /* Only if one in each stripe failing */

	else if (segtype_is_raid1(segtype))
		*nr = total_images - 1;

	else if (segtype_is_raid4(segtype) ||
		 segtype_is_any_raid5(segtype) ||
		 segtype_is_any_raid6(segtype))
		*nr = segtype->parity_devs;

	else
		*nr = 0;
}


/****************************************************************************/
/****************************************************************************/
/****************************************************************************/
/* Construction site of takeover handler function jump table solution */

/*
 * Change the image count of the raid @lv to @new_image_count
 * allocating from list @allocate_pvs and putting any removed
 * LVs on the @removal_lvs list
 */
static int _lv_change_image_count(struct logical_volume *lv, const struct segment_type *new_segtype,
				  uint32_t new_image_count,
				  struct dm_list *allocate_pvs, struct dm_list *removal_lvs)
{
	struct lv_segment *seg = first_seg(lv);
	struct dm_list meta_lvs, data_lvs;
	int reshape_disks = seg_is_striped_raid(seg) && !seg_is_any_raid0(seg) &&
			    is_same_level(seg->segtype, new_segtype);
	uint32_t old_count = seg->area_count, s;

	if (old_count == new_image_count) {
		log_warn("%s already has image count of %d.",
			 display_lvname(lv), new_image_count);
		return 1;
	}

	/* Check for maximum supported raid devices */
	if (!_check_max_raid_devices(new_image_count))
		return 0;

PFLA("reshape_disks=%u", reshape_disks);
	dm_list_init(&meta_lvs);
	dm_list_init(&data_lvs);

	if (old_count == new_image_count) {
		log_warn("raid1^logical volume %s already has requested number of mirrors", display_lvname(lv));
		return 0;

	} else if (old_count < new_image_count) {
		/* Allocate an additional meta and data LVs pair */
		log_debug_metadata("Allocating additional data and metadata LV pair for %s", display_lvname(lv));
		if (!_alloc_image_components(lv, 1, allocate_pvs, new_image_count - old_count,
					     &meta_lvs, &data_lvs)) {
			log_error("Failed to allocate additional data and metadata LV pair for %s", display_lvname(lv));
			return_0;
		}

		log_debug_metadata("Clearing newly allocated metadata LVs of %s", display_lvname(lv));
		if (!_clear_lvs(&meta_lvs)) {
			log_error("Failed to clear newly allocated metadata LVs of %s", display_lvname(lv));
			return_0;
		}

		/* Grow areas arrays for metadata and data devs  */
		log_debug_metadata("Realocating areas arrays of %s", display_lvname(lv));
		if (!_realloc_meta_and_data_seg_areas(lv, seg, new_image_count)) {
			log_error("Relocation of areas arrays for %s failed", display_lvname(lv));
			return_0;
		}

		log_debug_metadata("Adding new data and metadata LVs to %s", display_lvname(lv));
		if (!_add_image_component_list(seg, 1, 0, &meta_lvs, old_count) ||
		    !_add_image_component_list(seg, 1, LV_REBUILD, &data_lvs, old_count)) {
			log_error("Failed to add new data and metadata LVs to %s", display_lvname(lv));
			return_0;
		} 

		if (reshape_disks) {
			uint32_t plus_extents = (new_image_count - old_count) *
						(lv->le_count / _data_rimages_count(seg, old_count));
PFL();
			/*
		 	* Reshape adding image component pairs:
		 	*
		 	* - reset rebuild flag on new image LVs
		 	* - set delta disks plus flag on new image LVs
		 	*/
			for (s = old_count; s < new_image_count; s++) {
PFL();
				seg_lv(seg, s)->status &= ~LV_REBUILD;
				seg_lv(seg, s)->status |= LV_RESHAPE_DELTA_DISKS_PLUS;
			}
PFL();
			/* Reshape adding image component pairs -> change sizes accordingly */
PFLA("lv->le_count=%u data_rimages=%u plus_extents=%u", lv->le_count, _data_rimages_count(seg, old_count), plus_extents);
			lv->le_count += plus_extents;
			lv->size = lv->le_count * lv->vg->extent_size;
			seg->len += plus_extents;
			seg->area_len += plus_extents;
			seg->reshape_len = seg->reshape_len / _data_rimages_count(seg, old_count) *
						      _data_rimages_count(seg, new_image_count);
			if (old_count == 2 && !seg->stripe_size)
				seg->stripe_size = DEFAULT_STRIPESIZE;
PFLA("lv->le_count=%u", lv->le_count);
		}

	} else {
		if (!removal_lvs) {
			log_error(INTERNAL_ERROR "Called with NULL removal LVs list");
			return 0;
		}

		/* Extract all image and any metadata lvs past new_image_count */
		log_debug_metadata("Extracting data and metadata LVs from %s", display_lvname(lv));
		if (!_raid_extract_images(lv, new_image_count, allocate_pvs,
					  0 /* shift */, removal_lvs, removal_lvs)) {
			log_error("Failed to extract data and metadata LVs from %s", display_lvname(lv));
			return 0;
		}

		/* Shrink areas arrays for metadata and data devs  */
		log_debug_metadata("Reallocating data and metadata areas arrays of %s", display_lvname(lv));
		if (!_realloc_meta_and_data_seg_areas(lv, seg, new_image_count)) {
			log_error("Failed to reallocate data and metadata areas arrays of %s", display_lvname(lv));
			return 0;
		}

		/* Reshape removing image component pairs -> change sizes accordingly */
		if (reshape_disks) {
			uint32_t minus_extents = (old_count - new_image_count) *
						 (lv->le_count / _data_rimages_count(seg, old_count));

PFLA("lv->le_count=%u data_rimages=%u minus_extents=%u", lv->le_count, _data_rimages_count(seg, old_count), minus_extents);
			lv->le_count -= minus_extents;
			lv->size = lv->le_count * lv->vg->extent_size;
			seg->len -= minus_extents;
			seg->area_len -= minus_extents;
			seg->reshape_len = seg->reshape_len / _data_rimages_count(seg, old_count) *
						      	_data_rimages_count(seg, new_image_count);
PFLA("lv->le_count=%u", lv->le_count);
		}
	}

	seg->area_count = new_image_count;

	return 1;
}

/*
 * Update metadata, reload origin @lv, eliminate any LVs listed on @remova_lvs
 * and then clear lags passed to the kernel (if any) in the metadata
 */
static int _lv_update_and_reload_origin_eliminate_lvs(struct logical_volume *lv,
						      struct dm_list *removal_lvs)
{
	int flag_cleared;

	if (!lv_update_and_reload_origin(lv))
		return_0;

	/* Eliminate any residual LV, write VG, commit it and take a backup */
	if (!_eliminate_extracted_lvs(lv->vg, removal_lvs))
		return_0;

	/*
	 * Now that any 'REBUILD' or 'RESHAPE_DELTA_DISKS' etc. has/have made
	 * its/their way to the kernel, we must remove the flag(s) so that the
	 * individual devices are not rebuilt/reshaped upon every activation.
	 */
	if (!_reset_flags_passed_to_kernel(lv, &flag_cleared))
		return 0;

	return flag_cleared ? lv_update_and_reload_origin(lv) : 1;
}

/* Display error message and return 0 if @lv is not synced, else 1 */
static int _lv_is_synced(struct logical_volume *lv)
{
	if (lv->status & LV_NOTSYNCED) {
		log_error("Can't convert out-of-sync LV %s"
			  " use 'lvchange --resync %s' first",
			  display_lvname(lv), display_lvname(lv));
		return 0;
	}

	return 1;
}

/* Begin: various conversions between layers (aka MD takeover) */
/*
 * takeover function argument list definition
 *
 * All takeover functions and helper functions
 * to support them have this list of arguments
 */
#define TAKEOVER_FN_ARGUMENTS					\
	struct logical_volume *lv,				\
	int yes, int force,					\
	const struct segment_type *new_segtype,			\
	unsigned new_image_count,				\
	const unsigned new_stripes,				\
	unsigned new_stripe_size, struct dm_list *allocate_pvs

typedef int (*takeover_fn_t)(TAKEOVER_FN_ARGUMENTS);

/* Return takeover function table index for @segtype */
static unsigned _takeover_fn_idx(const struct segment_type *segtype, uint32_t area_count)
{
	static uint64_t _segtype_to_idx[] = {
		0, /* linear, seg->area_count = 1 */
		SEG_AREAS_STRIPED,
		SEG_MIRROR,
		SEG_RAID0,
		SEG_RAID0_META,
		SEG_RAID1,
		SEG_RAID4|SEG_RAID5_LS|SEG_RAID5_LA|SEG_RAID5_LS|SEG_RAID5_RS|SEG_RAID5_RA|SEG_RAID5_N,
		SEG_RAID6_LS_6|SEG_RAID6_LA_6|SEG_RAID6_RS_6|SEG_RAID6_RA_6| \
		SEG_RAID6_NC|SEG_RAID6_NR|SEG_RAID6_ZR|SEG_RAID6_N_6,
		SEG_RAID10
	};
	unsigned r = ARRAY_SIZE(_segtype_to_idx);

PFLA("segtype=%s area_count=%u", segtype->name, area_count);
	/* Linear special case */
	if (segtype_is_striped(segtype) && area_count == 1)
		return 0;

	while (r-- > 0)
		if (segtype->flags & _segtype_to_idx[r])
			return r;

	return 0;
}

/* Macro to define raid takeover helper function header */
#define TAKEOVER_FN(function_name) \
static int function_name(TAKEOVER_FN_ARGUMENTS)

/* Macro to spot takeover helper functions easily */
#define TAKEOVER_HELPER_FN(function_name) TAKEOVER_FN(function_name)

/*
 * noop and error takoover handler functions
 * to allow for logging that an lv already
 * has the requested type or that the requested
 * conversion is not possible
 */
/* Noop takeover handler for @lv: logs that LV already is of the requested type */
TAKEOVER_FN(_noop)
{
	log_warn("Logical volume %s already is of requested type %s",
		 display_lvname(lv), lvseg_name(first_seg(lv)));

	return 1;
}

/* Error takeover handler for @lv: logs what's (im)possible to convert to (and mabye added later) */
TAKEOVER_FN(_error)
{
	struct lv_segment *seg = first_seg(lv);

	/* FIXME: enhance message */
	log_error("Converting the segment type for %s (directly) from %s to %s"
		  " is not supported.", display_lvname(lv),
		  lvseg_name(seg), new_segtype->name);

	return 0;
}

/*
 * Begin takeover helper funtions
 */
/* Helper: linear -> raid0* */
TAKEOVER_HELPER_FN(_linear_raid0)
{
	struct lv_segment *seg = first_seg(lv);
	struct dm_list meta_lvs;

	dm_list_init(&meta_lvs);

	if ((!seg_is_linear(seg) && !seg_is_any_raid0(seg)) || new_image_count != 1) {
		log_error(INTERNAL_ERROR "Can't convert non-(linear|raid0) lv or to image count != 1");
		return 0;
	}

	/* Convert any linear segment to raid1 by inserting a layer and presetting segtype as raid1 */
	if (seg_is_linear(seg) &&
	    !(seg = _convert_linear_to_raid1(lv)))
		return 0;

	/* raid0_meta: need to add an rmeta device to pair it with the given linear device as well */
	if (segtype_is_raid0_meta(new_segtype) &&
	    !_alloc_and_add_rmeta_devs_for_lv(lv))
		return 0;

	/* HM FIXME: overloading force argument here! */
	/* We may be called to convert to !raid0*, i.e. an interim conversion on the way to radi4/5/6 */
	if (force)
		return 1;

	seg->segtype = new_segtype;
	seg->region_size = 0;

	return lv_update_and_reload_origin(lv);
}

/* Helper: linear/raid0 with 1 image <-> raid1/4/5 takeover handler for @lv */
TAKEOVER_HELPER_FN(_linear_raid14510)
{
	struct lv_segment *seg = first_seg(lv);
	struct dm_list data_lvs, meta_lvs;
	struct segment_type *segtype;

	dm_list_init(&data_lvs);
	dm_list_init(&meta_lvs);

	if ((segtype_is_raid4(new_segtype) || segtype_is_any_raid5(new_segtype)) &&
	    new_image_count != 2) {
		log_error("Can't convert %s from %s to %s != 2 images",
			  display_lvname(lv), SEG_TYPE_NAME_LINEAR, new_segtype->name);
		return 0;
	}

	/* HM FIXME: elaborate this raid4 restriction more! */
	if (segtype_is_raid4(new_segtype)) {
		log_error("Can't convert %s from %s to %s, please use %s",
			  display_lvname(lv), SEG_TYPE_NAME_LINEAR,
			  SEG_TYPE_NAME_RAID4, SEG_TYPE_NAME_RAID5);
		return 0;
	}

	new_image_count = new_image_count > 1 ? new_image_count : 2;

	/* HM FIXME: overloading force argument to avoid metadata update in _linear_raid0() */
	/* Use helper _linear_raid0() to create the initial raid0_meta with one image pair up */
	if (!(segtype = get_segtype_from_flag(lv->vg->cmd, SEG_RAID0_META)) ||
	    !_linear_raid0(lv, 0, 1, segtype, 1, 0, 0, allocate_pvs))
		return 0;

	/* Allocate the additional meta and data lvs requested */
	if (!_lv_change_image_count(lv, new_segtype, new_image_count, allocate_pvs, NULL))
		return 0;

	seg = first_seg(lv);
	seg->segtype = new_segtype;

	return _lv_update_and_reload_origin_eliminate_lvs(lv, NULL);
}

/* Helper: striped/raid0* -> raid4/5/6/10 */
TAKEOVER_HELPER_FN(_striped_raid0_raid45610)
{
	struct lv_segment *seg = first_seg(lv);

	/* This helper can be used to convert from raid0* -> raid10 too */
	if (seg_is_striped(seg) &&
	    !(seg = _convert_striped_to_raid0(lv, 0 /* alloc_metadata_devs */, 0 /* update_and_reload */)))
		return 0;

	if (seg_is_raid0(seg) &&
	    !_raid0_add_or_remove_metadata_lvs(lv, 0 /* !update_and_reload */, NULL))
		return 0;

	if (!_lv_change_image_count(lv, new_segtype, new_image_count, allocate_pvs, NULL))
		return 0;

	/* If this is any raid5 conversion request -> enforce raid5_n, because we convert from striped */
	if (segtype_is_any_raid5(new_segtype)) {
	    	if (!(new_segtype = get_segtype_from_flag(lv->vg->cmd, SEG_RAID5_N)))
			return 0;

	/* If this is any raid6 conversion request -> enforce raid6_n_6, because we convert from striped */
	} else if (segtype_is_any_raid6(new_segtype) &&
		   !(new_segtype = get_segtype_from_flag(lv->vg->cmd, SEG_RAID6_N_6))) {
		return 0;

	/* If this is a raid10 conversion request -> reorder the areas to suit raid19 */
	} else if (segtype_is_raid10(new_segtype)) {
		log_debug_metadata("Reordering areas for raid0 -> raid10 takeover");
		_raid10_reorder_seg_areas(seg, 1);
	}

	seg->segtype = new_segtype;

	return _lv_update_and_reload_origin_eliminate_lvs(lv, NULL);
}

/* raid0 -> linear */
TAKEOVER_HELPER_FN(_raid0_linear)
{
	struct dm_list removal_lvs;

	dm_list_init(&removal_lvs);

	if (!_convert_raid_to_linear(lv, &removal_lvs))
		return_0;

	if (!(first_seg(lv)->segtype = get_segtype_from_string(lv->vg->cmd, SEG_TYPE_NAME_STRIPED)))
		return_0;

	/* HM FIXME: overloading force argument here! */
	if (force)
		return 1;

	return _lv_update_and_reload_origin_eliminate_lvs(lv, &removal_lvs);
}

/* Helper: raid0* with one image -> mirror */
TAKEOVER_HELPER_FN(_raid0_mirror)
{
	struct segment_type *segtype;

	if (first_seg(lv)->area_count != 1)
		return _error(lv, 0, 0, new_segtype, 0, 0, 0, NULL);

	new_image_count = new_image_count > 1 ? new_image_count : 2;

	if (!_check_max_mirror_devices(new_image_count))
		return 0;

	if (seg_is_raid0(first_seg(lv)) &&
	    !_alloc_and_add_rmeta_devs_for_lv(lv))
		return 0;
	/* First convert to raid1... */
	if (!(segtype = get_segtype_from_flag(lv->vg->cmd, SEG_RAID1)) ||
	    !_linear_raid14510(lv, 0, 0, segtype, new_image_count, 0 /* new_stripes */,
			       new_stripe_size, allocate_pvs))
		return 0;

	/* ...second convert to mirror */
	return _convert_raid1_to_mirror(lv, new_segtype, allocate_pvs,
					1 /* !update_and_reload */, NULL);
}

/* raid0 with one image -> raid1 */
TAKEOVER_HELPER_FN(_raid0_raid1)
{
	struct lv_segment *seg = first_seg(lv);

	if (seg->area_count != 1) {
		log_error(INTERNAL_ERROR "Can't convert non-mirrored segment in lv %s",
			  display_lvname(lv));
		return 0;
	}

	new_image_count = new_image_count > 1 ? new_image_count : 2;

	if (seg_is_raid0(seg) &&
	    !_alloc_and_add_rmeta_devs_for_lv(lv))
		return 0;

	seg->segtype = new_segtype;
	if (!_lv_change_image_count(lv, new_segtype, new_image_count, allocate_pvs, NULL))
		return 0;

	/* Master leg is the first sub lv */
	seg_lv(seg, 0)->status &= ~LV_REBUILD;

	return _lv_update_and_reload_origin_eliminate_lvs(lv, NULL);
}

/* Helper: mirror -> raid0* */
TAKEOVER_HELPER_FN(_mirror_raid0)
{
	struct lv_segment *seg = first_seg(lv);
	struct dm_list removal_lvs;

	dm_list_init(&removal_lvs);

	if (!seg_is_mirrored(seg)) {
		log_error(INTERNAL_ERROR "Can't convert non-mirrored segment of lv %s",
			  display_lvname(lv));
		return 0;
	}

	if (!_lv_is_synced(lv))
		return 0;

	new_image_count = new_image_count > 1 ? new_image_count : 2;

	log_debug_metadata("Converting mirror lv %s to raid", display_lvname(lv));
	if (!_convert_mirror_to_raid(lv, new_segtype, 0 /* update_and_reaload */))
		return 0;

	/* Remove the last half of the meta and data image pairs */
	log_debug_metadata("Reducing lv %s to 1 image", display_lvname(lv));
	if (!_lv_change_image_count(lv, new_segtype, 1, allocate_pvs, &removal_lvs))
		return 0;

	if (seg_is_raid0(seg)) {
		/* Remove rmeta LVs */
		log_debug_metadata("Extracting and renaming metadata LVs from lv %s",
				   display_lvname(lv));
		if (!_extract_image_component_list(seg, RAID_META, 0, &removal_lvs))
			return 0;
	}

	seg->segtype = new_segtype;

	return _lv_update_and_reload_origin_eliminate_lvs(lv, &removal_lvs);
}

/* Helper: convert mirror with 2 images <-> raid4/5 */
TAKEOVER_HELPER_FN(_mirror_r45)
{
	struct lv_segment *seg = first_seg(lv);
	struct dm_list removal_lvs;

	dm_list_init(&removal_lvs);

	if (seg->area_count != 2) {
		log_error("Can't convert %s between %s and %s/%s with != 2 images",
			  display_lvname(lv), SEG_TYPE_NAME_MIRROR,
			  SEG_TYPE_NAME_RAID4, SEG_TYPE_NAME_RAID5);
		return 0;
	}

	if (!_lv_is_synced(lv))
		return 0;

	if (segtype_is_mirror(new_segtype)) {
		if (!_lv_free_reshape_space(lv)) {
			log_error(INTERNAL_ERROR "Failed to free reshape space");
			return 0;
		}

		if (!(seg->segtype = get_segtype_from_flag(lv->vg->cmd, SEG_RAID1)) ||
		    !_convert_raid1_to_mirror(lv, new_segtype, allocate_pvs,
					      0 /* !update_and_reload */, &removal_lvs))
			return 0;

	} else if (!_convert_mirror_to_raid(lv, new_segtype, 0 /* update_and_reaload */))
		return 0;

	return _lv_update_and_reload_origin_eliminate_lvs(lv, &removal_lvs);
}

/* Helper: raid1 -> raid0* */
TAKEOVER_HELPER_FN(_raid1_raid0)
{
	struct lv_segment *seg = first_seg(lv);
	struct dm_list removal_lvs;

	dm_list_init(&removal_lvs);

	if (!seg_is_raid1(seg)) {
		log_error(INTERNAL_ERROR "Can't convert non-raid1 segment in lv %s",
			  display_lvname(lv));
		return 0;
	}

	if (!_lv_is_synced(lv))
		return 0;

	seg->segtype = new_segtype;
	if (!_lv_change_image_count(lv, new_segtype, 1, allocate_pvs, &removal_lvs))
		return 0;

	/* Remove rmeta last LV if raid0  */
	if (segtype_is_raid0(new_segtype)) {
		log_debug_metadata("Extracting and renaming metadata LVs frim lv %s",
				   display_lvname(lv));
		if (!_extract_image_component_list(seg, RAID_META, 0, &removal_lvs))
			return 0;
	}

	return _lv_update_and_reload_origin_eliminate_lvs(lv, &removal_lvs);
}

/* helper raid1 with N images or raid4/5* with 2 images <-> linear */
TAKEOVER_HELPER_FN(_raid14510_linear)
{
	struct dm_list removal_lvs;
	struct lv_segment *seg = first_seg(lv);

	dm_list_init(&removal_lvs);

	if (seg->area_count > 2 && !seg_is_raid1(seg)) {
		log_error("Can't convert %s with!%u images",
			  display_lvname(lv), seg->area_count);
		return 0;
	}

	if (!_lv_is_synced(lv))
		return 0;

	/*
	 * Have to remove any reshape space which my be a the beginning of
	 * the component data images or linear ain't happy about data content
	 */
	if (!_lv_free_reshape_space(lv)) {
		log_error(INTERNAL_ERROR "Failed to free reshape space");
		return 0;
	}

	/* Reduce image count to one */
	if (!(seg->segtype = get_segtype_from_flag(lv->vg->cmd, SEG_RAID1)) ||
	    !_lv_change_image_count(lv, new_segtype, 1, allocate_pvs, &removal_lvs))
		return 0;

	if (!_convert_raid_to_linear(lv, &removal_lvs))
		return_0;

	return _lv_update_and_reload_origin_eliminate_lvs(lv, &removal_lvs);
}

/* Helper: raid1 with N images to N images (N != M) and raid4/5 to raid6* */
TAKEOVER_HELPER_FN(_raid145_raid1_raid6) 
{
	struct dm_list removal_lvs;

	dm_list_init(&removal_lvs);

	if (!_lv_is_synced(lv))
		return 0;

	if (!_lv_change_image_count(lv, new_segtype, new_image_count, allocate_pvs, &removal_lvs))
		return 0;

	first_seg(lv)->segtype = new_segtype;

	return _lv_update_and_reload_origin_eliminate_lvs(lv, &removal_lvs);
}

/* Helper: raid1 with 2 images <-> raid4/5/10 */
TAKEOVER_HELPER_FN(_raid145_raid4510)
{
	struct lv_segment *seg = first_seg(lv);

PFL();
	if (seg_is_raid1(seg) && seg->area_count != 2) {
		log_error("Can't convert %s between %s and %s/%s with != 2 images",
			  display_lvname(lv), SEG_TYPE_NAME_RAID1,
			  SEG_TYPE_NAME_RAID4, SEG_TYPE_NAME_RAID5);
		return 0;
	}

	if ((!seg_is_raid1(seg) && !seg_is_raid5_n(seg)) && segtype_is_raid4(new_segtype)) {
		log_error("Can't convert %s between %s and %s",
			  display_lvname(lv), lvseg_name(seg), SEG_TYPE_NAME_RAID4);
		log_error("Convert %s to %s first",
			  display_lvname(lv), SEG_TYPE_NAME_RAID5_N);
		return 0;
	}

	if (!_lv_is_synced(lv))
		return 0;

	if (new_image_count && new_image_count != 2)
		log_error("Ignoring new image count for %s", display_lvname(lv));

	/*
	 * Have to remove any reshape space which my be a the beginning of
	 * the component data images or linear ain't happy about data content
	 */
	if (segtype_is_raid1(new_segtype) &&
	    !_lv_free_reshape_space(lv)) {
		log_error(INTERNAL_ERROR "Failed to free reshape space");
		return 0;
	}

	if ((!seg_is_raid1(seg) && !seg_is_any_raid5(seg)) && segtype_is_any_raid5(new_segtype)) {
		if (!(seg->segtype = get_segtype_from_flag(lv->vg->cmd, SEG_RAID5_N)))
			return_0;
	} else
		seg->segtype = new_segtype;

	return lv_update_and_reload_origin(lv);
}

/* Helper: raid10 with 2 images <-> raid1/raid4/raid5* */
TAKEOVER_HELPER_FN(_raid10_raid145)
{
	struct lv_segment *seg = first_seg(lv);

	if (seg->area_count != 2) {
		log_error("Can't convert %s from %s to %s with != 2 images",
			  display_lvname(lv), SEG_TYPE_NAME_RAID10, new_segtype->name);
		return 0;
	}

	if (!_lv_is_synced(lv))
		return 0;

	seg->segtype = new_segtype;

	return lv_update_and_reload_origin(lv);
}

/* Helper: raid10 -> striped/raid0* */
TAKEOVER_HELPER_FN(_raid10_striped_r0)
{
	struct lv_segment *seg = first_seg(lv);
	struct dm_list removal_lvs;

	dm_list_init(&removal_lvs);

	if (!_lv_is_synced(lv))
		return 0;

	log_debug_metadata("Reordering areas for raid10 -> raid0 takeover");
	_raid10_reorder_seg_areas(seg, 0);

	/* Remove the last half of the meta and data image pairs */
	if (!_lv_change_image_count(lv, new_segtype, seg->area_count / 2, allocate_pvs, &removal_lvs))
		return 0;

	if (!segtype_is_any_raid0(new_segtype)) {
		if (!(seg->segtype = get_segtype_from_flag(lv->vg->cmd, SEG_RAID0_META)))
			return_0;

		if (!_convert_raid0_to_striped(lv, 0, &removal_lvs))
			return 0;

		seg = first_seg(lv);
	} else if (segtype_is_raid0(new_segtype) &&
		   !_raid0_add_or_remove_metadata_lvs(lv, 0 /* update_and_reload */, &removal_lvs))
		return 0;

	seg->segtype = new_segtype;

	return _lv_update_and_reload_origin_eliminate_lvs(lv, &removal_lvs);
}
/* End takeover helper funtions */


/*
 * Begin all takeover functions addressed via the _takeover_fn_table[][]
 */
/* Linear -> raid0 */
TAKEOVER_FN(_l_r0)
{
	return _linear_raid0(lv, 0, 0, new_segtype, 1, 0, 0, allocate_pvs);
}

/* Linear -> raid1 */
TAKEOVER_FN(_l_r1)
{
	if (first_seg(lv)->area_count != 1)
		return 0;

	return _linear_raid14510(lv, 0, 0, new_segtype,
				 new_image_count, 0 /* new_stripes */,
				 new_stripe_size, allocate_pvs);
}

/* Linear -> raid4/5 */
TAKEOVER_FN(_l_r45)
{
	if (first_seg(lv)->area_count != 1)
		return 0;

	return _linear_raid14510(lv, 0, 0, new_segtype,
				 2 /* new_image_count */, 0 /* new_stripes */,
				 new_stripe_size, allocate_pvs);
}

/* Linear -> raid10 */
TAKEOVER_FN(_l_r10)
{
	return _linear_raid14510(lv, 0, 0, new_segtype,
				 2 /* new_image_count */ , 0 /* new_stripes */,
				 new_stripe_size, allocate_pvs);
}

/* Striped -> raid0 */
TAKEOVER_FN(_s_r0)
{
	return _convert_striped_to_raid0(lv, 0 /* !alloc_metadata_devs */, 1 /* update_and_reload */) ? 1 : 0;
}

/* Striped -> raid0_meta */
TAKEOVER_FN(_s_r0m)
{
	return _convert_striped_to_raid0(lv, 1 /* alloc_metadata_devs */, 1 /* update_and_reload */) ? 1 : 0;
} 

/* Striped -> raid4/5 */
TAKEOVER_FN(_s_r45)
{
	return _striped_raid0_raid45610(lv, 0, 0, new_segtype, first_seg(lv)->area_count + 1, 0, 0, allocate_pvs);
}

/* Striped -> raid6 */
TAKEOVER_FN(_s_r6)
{
	return _striped_raid0_raid45610(lv, 0, 0, new_segtype, first_seg(lv)->area_count + 2, 0, 0, allocate_pvs);
}

TAKEOVER_FN(_s_r10)
{
	return _striped_raid0_raid45610(lv, 0, 0, new_segtype, first_seg(lv)->area_count * 2, 0, 0, allocate_pvs);
}

/* mirror -> raid0 */
TAKEOVER_FN(_m_r0)
{
	return _mirror_raid0(lv, 0, 0, new_segtype, 0, 0, 0, allocate_pvs);
}

/* mirror -> raid0_meta */
TAKEOVER_FN(_m_r0m)
{
	return _mirror_raid0(lv, 0, 0, new_segtype, 0, 0, 0, allocate_pvs);
}

/* Mirror -> raid1 */
TAKEOVER_FN(_m_r1)
{
	return _convert_mirror_to_raid(lv, new_segtype, 1 /* update_and_reaload */);
}

/* Mirror with 2 images -> raid4/5 */
TAKEOVER_FN(_m_r45)
{
	return _mirror_r45(lv, 0, 0, new_segtype, 0, 0, 0, allocate_pvs);
}

/* Mirror with 2 images -> raid10 */
TAKEOVER_FN(_m_r10)
{
	struct lv_segment *seg = first_seg(lv);

	if (seg->area_count != 2) {
		log_error("Can't convert %s from %s to %s with != 2 images",
			  display_lvname(lv), SEG_TYPE_NAME_MIRROR, new_segtype->name);
		return 0;
	}

	if (!_lv_is_synced(lv))
		return 0;

	if (!_convert_mirror_to_raid(lv, new_segtype, 0 /* update_and_reaload */))
		return 0;

	seg->segtype = new_segtype;

	return lv_update_and_reload(lv);;
}


/* raid0 -> linear */
TAKEOVER_FN(_r0_l)
{
	return _raid0_linear(lv, 0, 0, new_segtype, 0, 0, 0, allocate_pvs);
}

/* raid0 with one image -> mirror */
TAKEOVER_FN(_r0_m)
{
	return _raid0_mirror(lv, 0, 0, new_segtype, 0, 0, 0, allocate_pvs);
}

/* raid0 -> raid0_meta */
TAKEOVER_FN(_r0_r0m)
{
	return _raid0_add_or_remove_metadata_lvs(lv, 1, NULL);
}

/* raid0 -> striped */
TAKEOVER_FN(_r0_s)
{
	struct dm_list removal_lvs;

	dm_list_init(&removal_lvs);

	return _convert_raid0_to_striped(lv, 1, &removal_lvs);
}

/* raid0 with one image -> raid1 */
TAKEOVER_FN(_r0_r1)
{
	return _raid0_raid1(lv, 0, 0, new_segtype, new_image_count, 0, 0, allocate_pvs);
}

/* raid0 -> raid4/5_n */
TAKEOVER_FN(_r0_r45)
{
	return _striped_raid0_raid45610(lv, 0, 0, new_segtype, first_seg(lv)->area_count + 1, 0, 0, allocate_pvs);
}

/* raid0 -> raid6_n_6 */
TAKEOVER_FN(_r0_r6)
{
	return _striped_raid0_raid45610(lv, 0, 0, new_segtype, first_seg(lv)->area_count + 2, 0, 0, allocate_pvs);
}

/* raid0 with N images (N > 1) -> raid10 */
TAKEOVER_FN(_r0_r10)
{
	return _striped_raid0_raid45610(lv, 0, 0, new_segtype, first_seg(lv)->area_count * 2, 0, 0, allocate_pvs);
}

/* raid0_meta -> */
TAKEOVER_FN(_r0m_l)
{
	return _raid0_linear(lv, 0, 0, new_segtype, 0, 0, 0, allocate_pvs);
}

/* raid0_meta -> mirror */
TAKEOVER_FN(_r0m_m)
{
	return _raid0_mirror(lv, 0, 0, new_segtype, 0, 0, 0, allocate_pvs);
}

/* raid0_meta -> raid0 */
TAKEOVER_FN(_r0m_r0)
{
	struct dm_list removal_lvs;

	dm_list_init(&removal_lvs);

	return _raid0_add_or_remove_metadata_lvs(lv, 1, &removal_lvs);
}

/* raid0_meta -> striped */
TAKEOVER_FN(_r0m_s)
{
	struct dm_list removal_lvs;

	dm_list_init(&removal_lvs);

	return _convert_raid0_to_striped(lv, 1, &removal_lvs);
}


/* raid0_meta wih 1 image -> raid1 */
TAKEOVER_FN(_r0m_r1)
{
	return _raid0_raid1(lv, 0, 0, new_segtype, new_image_count, 0, 0, allocate_pvs);
}


/* raid0_meta -> raid4/5_n */
TAKEOVER_FN(_r0m_r45)
{
	return _striped_raid0_raid45610(lv, 0, 0, new_segtype, first_seg(lv)->area_count + 1, 0, 0, allocate_pvs);
}

/* raid0_meta -> raid6_n_6 */
TAKEOVER_FN(_r0m_r6)
{
	return _striped_raid0_raid45610(lv, 0, 0, new_segtype, first_seg(lv)->area_count + 2, 0, 0, allocate_pvs);
}


/* raid0_meta wih 1 image -> raid10 */
TAKEOVER_FN(_r0m_r10)
{
	return _striped_raid0_raid45610(lv, 0, 0, new_segtype, first_seg(lv)->area_count * 2, 0, 0, allocate_pvs);
}


/* raid1 with N images -> linear */
TAKEOVER_FN(_r1_l)
{
	return _raid14510_linear(lv, 0, 0, NULL, 0, 0, 0, allocate_pvs);
}

/* raid1 -> mirror */
TAKEOVER_FN(_r1_m)
{
	struct dm_list removal_lvs;

	dm_list_init(&removal_lvs);

	if (!_lv_is_synced(lv))
		return 0;

	/*
	 * FIXME: support this conversion or don't invite users to switch back to "mirror"?
	 *        I find this at least valuable in case of an erroneous conversion to raid1
	 */
	if (!yes && yes_no_prompt("WARNING: Do you really want to convert %s to "
				  "non-recommended \"%s\" type? [y/n]: ",
			  display_lvname(lv), SEG_TYPE_NAME_MIRROR) == 'n') {
		log_warn("Logical volume %s NOT converted to \"%s\"",
			  display_lvname(lv), SEG_TYPE_NAME_MIRROR);
		return 0;
	}
	if (sigint_caught())
		return_0;

	return _convert_raid1_to_mirror(lv, new_segtype, allocate_pvs, 1, &removal_lvs);
}


/* raid1 -> raid0 */
TAKEOVER_FN(_r1_r0)
{
	return _raid1_raid0(lv, 0, 0, new_segtype, 0, 0, 0, allocate_pvs);
}

/* raid1 -> raid0_meta */
TAKEOVER_FN(_r1_r0m)
{
	return _raid1_raid0(lv, 0, 0, new_segtype, 0, 0, 0, allocate_pvs);
}

TAKEOVER_FN(_r1_r1) 
{
	return _raid145_raid1_raid6(lv, 0, 0, new_segtype, new_image_count, 0, 0, allocate_pvs);
}

/* raid1 with 2 legs -> raid10 */
TAKEOVER_FN(_r1_r45)
{
	return _raid145_raid4510(lv, 0, 0, new_segtype, new_image_count, 0, 0, allocate_pvs);
}


/****************************************************************************/
/*
 * Experimental conversion via creation of a new LV to top-level raid1 the content to
 */
/* Create a new LV with type @segtype */
static struct logical_volume *_create_lv(struct volume_group *vg, const char *lv_name,
					 const struct segment_type *segtype,
					 uint32_t mirrors, uint32_t stripes,
					 uint32_t region_size, uint32_t stripe_size,
					 uint32_t extents, struct dm_list *pvs)
{
	uint64_t flags = RAID | LVM_READ | LVM_WRITE;
	struct logical_volume *r;
	
	if (!(r = lv_create_empty(lv_name, NULL, flags, ALLOC_INHERIT, vg))) {
		log_error("Failed to allocate new LV %s/%s.", vg->name, lv_name);
		return_NULL;
	}

	/* Hide the lv, it's going to be a raid1 slave leg */
	lv_set_hidden(r);

	if (!lv_extend(r, segtype, stripes - segtype->parity_devs, stripe_size,
		       mirrors, region_size, extents, pvs, vg->alloc, 0)) {
		log_error("Failed to extend new LV %s", display_lvname(r));
		return_NULL;
	}

	return r;
}

/*
 * Helper: raid to raid conversion by duplication
 *
 * Inserts a layer on top of the given @lv,
 * creates a destination lv of the same size with
 * the requested @new_segtype
 */
TAKEOVER_HELPER_FN(_raid_conv_duplicate_raid)
{
	char *lv_name, *src_lv_name;
	size_t sz;
	const char *src_fix = "_csrc", *dst_fix = "_cdst";
	struct logical_volume *src_lv, *dst_lv, *top_lv;
	struct lv_segment *seg = first_seg(lv);;

	/* Memorize source lv name */
	if (!(src_lv_name = dm_pool_strdup(lv->vg->vgmem, lv->name)))
		return 0;

	/* Create and allocate new destination lv with the size of the source lv */
	sz = strlen(src_lv_name) + strlen(dst_fix) + 1;
	if (!(lv_name = dm_pool_zalloc(lv->vg->vgmem, sz)) ||
	    (dm_snprintf(lv_name, sz, "%s%s", lv->name, dst_fix) < 0) ||
	    !(dst_lv =_create_lv(lv->vg, lv_name, new_segtype, 1, new_image_count,
				 1024 /* region size */, new_stripe_size, lv->le_count, allocate_pvs))) {
		log_error("Failed to create destination lv %s/%s", lv->vg->name, lv_name);
		return 0;
	}

return 0;

	/* Insert layer on top of the source LV to use as a new top-levels raid1 master leg */
	if (!(src_lv = insert_layer_for_lv(lv->vg->cmd, lv, lv->status, src_fix)))
		return 0;

PFL();
	/* Create new top-level raid1 LV and attach source and destination LVs */
	if (!(top_lv =_create_lv(lv->vg, src_lv_name, seg->segtype, 2, 0,
				 seg->region_size, 0, 0, allocate_pvs)))
		return 0;

	/* Refer to its only segment */
	seg = first_seg(top_lv);

	/* Set source and destination LVs as the 2 legs of the new top-level raid1 LV */
	if (!set_lv_segment_area_lv(seg, 0, src_lv, src_lv->le_count, src_lv->status) ||
	    !set_lv_segment_area_lv(seg, 1, dst_lv, dst_lv->le_count, dst_lv->status)) {
		log_error("Failed to add sublv %s or %s", display_lvname(lv), display_lvname(dst_lv));
		return 0;
	}

	seg_lv(seg, 0)->status &= ~LV_REBUILD;
	seg_lv(seg, 1)->status |= LV_REBUILD;

	/* Set lenght/sizes of top-level lv */
	seg->len = seg->area_len = lv->le_count;
	top_lv->le_count = lv->le_count;
	top_lv->size = lv->le_count * lv->vg->extent_size;

	/* Suspend the source lv */
	if (!_vg_write_lv_suspend_vg_commit(lv))
		return 0;

	return resume_lv(top_lv->vg->cmd, lv_lock_holder(top_lv));
}

TAKEOVER_FN(_r_dup_r6)
{
	return _raid_conv_duplicate_raid(lv, 0, 0, new_segtype, 4, 64 * 2, 0, allocate_pvs);
}
/****************************************************************************/

/* raid1 with 2 legs -> raid10 */
TAKEOVER_FN(_r1_r10)
{
	return _raid145_raid4510(lv, 0, 0, new_segtype, new_image_count, 0, 0, allocate_pvs);
}

/* raid45 with 2 images -> linear */
TAKEOVER_FN(_r45_l)
{
	if (first_seg(lv)->area_count != 2) {
		log_error("Can't convert %s from %s/%s to %s with != 2 images",
			  display_lvname(lv), SEG_TYPE_NAME_RAID4,
			  SEG_TYPE_NAME_RAID5, SEG_TYPE_NAME_LINEAR);
		return 0;
	}

	return _raid14510_linear(lv, 0, 0, NULL, 0, 0, 0, allocate_pvs);
}

/* raid45 -> striped */
TAKEOVER_FN(_r456_r0_striped)
{
	struct lv_segment *seg = first_seg(lv);
	struct dm_list removal_lvs;

	if (!_lv_is_synced(lv))
		return 0;

	if (!seg_is_raid4(seg) && !seg_is_raid5_n(seg) && !seg_is_raid6_n_6(seg)) {
		log_error("LV %s has to be of type raid4/raid5_n/raid6_n_6 to allow for this conversion",
			  display_lvname(lv));
		return 0;
	}

	dm_list_init(&removal_lvs);

	if (!_lv_free_reshape_space(lv)) {
		log_error(INTERNAL_ERROR "Failed to free reshape space");
		return 0;
	}

	/* Remove meta and data lvs requested */
	if (!_lv_change_image_count(lv, new_segtype, new_image_count, allocate_pvs, &removal_lvs))
		return 0;

	if (!(seg->segtype = get_segtype_from_flag(lv->vg->cmd, SEG_RAID0_META)))
		return_0;

	if (segtype_is_striped(new_segtype)) {
		if (!_convert_raid0_to_striped(lv, 0, &removal_lvs))
			return_0;

	} else if (segtype_is_raid0(new_segtype) &&
		   !_raid0_add_or_remove_metadata_lvs(lv, 0 /* update_and_reload */, &removal_lvs))
		return_0;

	return _lv_update_and_reload_origin_eliminate_lvs(lv, &removal_lvs);
}

/* raid4/5 -> striped */
TAKEOVER_FN(_r45_s)
{
	return _r456_r0_striped(lv, 0, 0, new_segtype, first_seg(lv)->area_count - 1, 0, 0, allocate_pvs);
}

/* raid4/5 with 2 images -> mirror */
TAKEOVER_FN(_r45_m)
{
	return _mirror_r45(lv, 0, 0, new_segtype, 0, 0, 0, allocate_pvs);
}

/* raid4/5 -> raid0 */
TAKEOVER_FN(_r45_r0)
{
	return _r456_r0_striped(lv, 0, 0, new_segtype, first_seg(lv)->area_count - 1, 0, 0, allocate_pvs);
}

/* raid4/5 -> raid0_meta */
TAKEOVER_FN(_r45_r0m)
{
	return _r456_r0_striped(lv, 0, 0, new_segtype, first_seg(lv)->area_count - 1, 0, 0, allocate_pvs);
}

/* raid4/5_n with 2 images -> raid1 */
TAKEOVER_FN(_r45_r1)
{
	return _raid145_raid4510(lv, 0, 0, new_segtype, 0, 0, 0, allocate_pvs);
}

/* raid4/5* <-> raid4/5* */
TAKEOVER_FN(_r45_r45)
{
	return _raid145_raid4510(lv, 0, 0, new_segtype, 0, 0, 0, allocate_pvs);
}

/* raid4/5* <-> raid6* */
TAKEOVER_FN(_r45_r6)
{
	struct lv_segment *seg = first_seg(lv);

	if (seg_is_raid4(seg)) {
		log_error("Please convert %s from %s to %s first",
			  display_lvname(lv), SEG_TYPE_NAME_RAID4,  SEG_TYPE_NAME_RAID5_N);
		return 0;
	}

	if (seg->area_count < 3) {
		log_error("Please convert %s from 1 stripe to at least 2 first for this conversion",
			  display_lvname(lv));
		return 0;
	}

	if (segtype_is_any_raid6(new_segtype) &&
	    !(new_segtype = get_segtype_from_flag(lv->vg->cmd, _raid_seg_flag_5_to_6(seg)))) {
		log_error(INTERNAL_ERROR "Failed to get raid5 -> raid6 conversion type");
		return_0;
	}

	return _raid145_raid1_raid6(lv, 0, 0, new_segtype, seg->area_count + 1, 0, 0, allocate_pvs);
}

/* raid4/5* with 2 images -> raid10 */
TAKEOVER_FN(_r45_r10)
{
	return _raid10_raid145(lv, 0, 0, new_segtype,0, 0, 0, allocate_pvs);
}


/* raid6 -> striped */
TAKEOVER_FN(_r6_s)
{
	return _r456_r0_striped(lv, 0, 0, new_segtype, first_seg(lv)->area_count - 2, 0, 0, allocate_pvs);
}

/* raid6 -> raid0 */
TAKEOVER_FN(_r6_r0)
{
	return _r456_r0_striped(lv, 0, 0, new_segtype, first_seg(lv)->area_count - 2, 0, 0, allocate_pvs);
}

/* raid6 -> raid0_meta */
TAKEOVER_FN(_r6_r0m)
{
	return _r456_r0_striped(lv, 0, 0, new_segtype, first_seg(lv)->area_count - 2, 0, 0, allocate_pvs);
}

/* raid6* -> raid4/5* */
TAKEOVER_FN(_r6_r45)
{
	struct lv_segment *seg = first_seg(lv);
	struct dm_list removal_lvs;

	if (!_lv_is_synced(lv))
		return 0;

	if (segtype_is_raid4(new_segtype) &&
	    !seg_is_raid6_n_6(seg)) {
		log_error("LV %s has to be of type %s to allow for this conversion",
			  display_lvname(lv), SEG_TYPE_NAME_RAID6_N_6);
		return 0;
	}
	if ((seg_is_raid6_zr(seg) ||
	     seg_is_raid6_nc(seg) ||
	     seg_is_raid6_nr(seg)) &&
	    !seg_is_raid6_n_6(seg)) {
		log_error("LV %s has to be of type %s,%s,%s,%s or %s to allow for this conversion",
			  display_lvname(lv),
			  SEG_TYPE_NAME_RAID6_LS_6, SEG_TYPE_NAME_RAID6_LA_6,
			  SEG_TYPE_NAME_RAID6_RS_6, SEG_TYPE_NAME_RAID6_RA_6,
			  SEG_TYPE_NAME_RAID6_N_6);
		return 0;
	}

	if (!_lv_is_synced(lv))
		return 0;

	new_image_count = seg->area_count - 1;
	dm_list_init(&removal_lvs);

	/* Remove meta and data lvs requested */
	if (!_lv_change_image_count(lv, new_segtype, new_image_count, allocate_pvs, &removal_lvs))
		return 0;

	if (!(seg->segtype = get_segtype_from_flag(lv->vg->cmd, _raid_seg_flag_6_to_5(seg)))) {
		log_error(INTERNAL_ERROR "Failed to get raid6 -> raid5 conversion type");
		return_0;
	}

	return _lv_update_and_reload_origin_eliminate_lvs(lv, &removal_lvs);
}


/* raid6* <-> raid6* */
TAKEOVER_FN(_r6_r6)
{
	struct lv_segment *seg = first_seg(lv);

	if (seg->segtype == new_segtype) {
		_noop(lv, 0, 0, NULL, 0, 0, 0, allocate_pvs);
		return 0;
	}

	if (!_lv_is_synced(lv))
		return 0;

	seg->segtype = new_segtype;

	return lv_update_and_reload_origin(lv);
}

/* raid10 with 2 images -> linear */
TAKEOVER_FN(_r10_l)
{
	if (first_seg(lv)->area_count != 2) {
		log_error("Can't convert %s from %s to %s with != 2 images",
			  display_lvname(lv), SEG_TYPE_NAME_RAID10, SEG_TYPE_NAME_LINEAR);
		return 0;
	}

	return _raid14510_linear(lv, 0, 0, NULL, 0, 0, 0, allocate_pvs);
}

/* raid10 -> raid0* */
TAKEOVER_FN(_r10_s)
{
	return _raid10_striped_r0(lv, 0, 0, new_segtype, 0, 0, 0, allocate_pvs);
}

/* raid10 with 2 images -> mirror */
TAKEOVER_FN(_r10_m)
{
	struct lv_segment *seg = first_seg(lv);
	struct dm_list removal_lvs;

	dm_list_init(&removal_lvs);

	if (seg->area_count != 2) {
		log_error("Can't convert %s from %s to %s with != 2 images",
			  display_lvname(lv), SEG_TYPE_NAME_RAID10, SEG_TYPE_NAME_MIRROR);
		return 0;
	}

	if (!_lv_is_synced(lv))
		return 0;

	if (!(seg->segtype = get_segtype_from_flag(lv->vg->cmd, SEG_RAID1)) ||
	    !_convert_raid1_to_mirror(lv, new_segtype, allocate_pvs, 0, &removal_lvs))
		return 0;

	return _lv_update_and_reload_origin_eliminate_lvs(lv, &removal_lvs);
}

/* raid10 -> raid0 */
TAKEOVER_FN(_r10_r0)
{
	return _raid10_striped_r0(lv, 0, 0, new_segtype, 0, 0, 0, allocate_pvs);
}

/* raid10 -> raid0_meta */
TAKEOVER_FN(_r10_r0m)
{
	return _raid10_striped_r0(lv, 0, 0, new_segtype, 0, 0, 0, allocate_pvs);
}

/* raid10 with 2 images -> raid1 */
TAKEOVER_FN(_r10_r1)
{
	return _raid10_raid145(lv, 0, 0, new_segtype,0, 0, 0, allocate_pvs);
}

/* raid10 with 2 images -> raid4/5* */
TAKEOVER_FN(_r10_r45)
{
	return _raid10_raid145(lv, 0, 0, new_segtype,0, 0, 0, allocate_pvs);
}

/*
 * 2-dimensional takeover function array defining the
 * FSM of possible/impossible or noop (i.e. requested
 * conversion already given on the lv) conversions
 *
 * Rows define from segtype and columns to segtype
 */
static takeover_fn_t _takeover_fn_table[9][9] = {
	/* from |, to ->    linear  striped   mirror   raid0    raid0_meta  raid1    raid4/5     raid6  raid10 */
	/*      v */
	/* linear      */ { _noop,   _error,  _error,  _l_r0,   _l_r0,      _l_r1,   _l_r45,    _error,  _l_r10   },
	/* striped     */ { _error,  _noop,   _error,  _s_r0,   _s_r0m,     _l_r1,   _s_r45,    _s_r6,   _s_r10   },
	/* mirror      */ { _error,  _error,  _noop,   _m_r0,   _m_r0m,     _m_r1,   _m_r45,    _error,  _m_r10   },
	/* raid0       */ { _r0_l,   _r0_s,   _r0_m,   _noop,   _r0_r0m,    _r0_r1,  _r0_r45,   _r0_r6,  _r0_r10  },
	/* raid0_meta  */ { _r0m_l,  _r0m_s,  _r0m_m,  _r0m_r0, _noop,      _r0m_r1, _r0m_r45,  _r0m_r6, _r0m_r10 },
	/* raid1       */ { _r1_l,   _r1_l,   _r1_m,   _r1_r0,  _r1_r0m,    _r1_r1,  _r1_r45,   _r_dup_r6, _r1_r10  },
	/* raid4/5     */ { _r45_l,  _r45_s,  _r45_m,  _r45_r0, _r45_r0m,   _r45_r1, _r45_r45,  _r45_r6, _r45_r10 },
	/* raid6       */ { _error,  _r6_s,   _error,  _r6_r0,  _r6_r0m,    _error,  _r6_r45,   _r6_r6,  _error   },
	/* raid10      */ { _r10_l,  _r10_s,  _r10_m,  _r10_r0, _r10_r0m,   _r10_r1, _r10_r45,  _error,  _error   },
};

/* End: various conversions between layers (aka MD takeover) */
/****************************************************************************/

/*
 * lv_raid_convert
 * @lv
 * @new_segtype
 *
 * Convert @lv from one RAID type (or 'mirror' segtype) to @new_segtype,
 * change RAID algorithm (e.g. left symmetric to right asymmetric),
 * add/remove LVs to/from a RAID LV or change stripe sectors
 *
 * Non dm-raid changes are factored in e.g. "mirror" and "striped" related
 * fucntions called from here.
 * All the rest of the raid <-> raid conversions go into a function
 * _convert_raid_to_raid() of their own called from here.
 *
 * Returns: 1 on success, 0 on failure
 */
/*
 * [18:42] <lvmguy> agk: what has to be changed when getting "Performing unsafe table load while..."
 * [18:50] <agk> Ah, that depends on the context
 * [18:51] <agk> as you're doing something new, we need to look at the trace and work out what to do
 * [18:51] <agk> What it means is:
 * [18:52] <agk>    if a device is suspended, i/o might get blocked and you might be unable to allocate memory
 * [18:52] <agk>    doing a table load needs memory
 * [18:52] <agk> So if you have suspend + load, then you could get deadlock
 * [18:52] <agk> and it's warning about that 
 * [18:52] <agk> but not every situation is like that - there are false positives
 * [18:53] <agk> So get the -vvvv trace from the command, then grep out the ioctls
 * [18:53] <agk> and look at the sequence and see what is supended at the time of the load
 * [18:54] <agk> IOW a suspend can cause a later table load to block - and it won't clear until you get a resume - but that resume depends on the load completing, which isn't going to happen
 * [18:54] <lvmguy> I thought it was trying to prevent OOM. need analyze the details...
 * [18:54] <agk> so the code normally does:   load, suspend, resume   in that order
 * [18:54] <agk> never suspend, load, resume
 * [18:55] <agk> but when you get complex operations all that dependency tree code tries to deal with this
 * [18:56] <lvmguy> yep, the sequences I have to do look like they fall into this latter realm ;)
 * [18:56] <agk> - it tries to sort all the operations on the various devices into a safe order in which to perform them
 * [18:58] <agk> So normally, (1) get the actual list of operations it's performing.  (2) work out if there is an easy fix by performing them in a different order - if so, we work out how to change the code to do that (often needs hacks)
 * [18:59] <agk>   - if not, then we look for an alternative strategy, usually by splitting operations into more than one step which can be done within the dependency rules
 * [19:02] <lvmguy> let me figure out dependency details then we can discuss
 * [19:03] <agk> - kabi is *very* familiar with fixing these sorts of problems:)
 * [19:04] <agk>   - we had to go through it all for thin and cache
 * [19:04] <agk> But so far, we've not yet hit a situation we couldn't solve
 * [19:04] <lvmguy> k
 * */
/*
 * TODO:
 *  - review size calculations in raid1 <-> raid4/5
 *  - review stripe size usage on conversion from/to striped/nonstriped segment types
 *  - review reshape space alloc/free
 *  - conversion raid0 -> raid10 only mentions redundancy = 1 instead of 1..#stripes maximum
 *  - false --striped user entry shows wrong message
 *  - keep ti->len small on initial disk adding reshape and grow after it has finished
 *    in order to avoid bio_endio in the targets map method?
 */
int lv_raid_convert(struct logical_volume *lv,
		    const struct segment_type *new_segtype,
		    int yes, int force,
		    unsigned new_image_count,
		    const unsigned new_stripes,
		    unsigned new_stripe_size,
		    struct dm_list *allocate_pvs)
{
	int r, segtype_change, stripe_size_change, y;
	uint32_t stripes = new_stripes;
	unsigned cur_redundancy, new_redundancy;
	struct lv_segment *seg = first_seg(lv);
	struct segment_type *new_segtype_tmp = (struct segment_type *) new_segtype;
	struct segment_type *striped_segtype;
	struct lvinfo info = { 0 };
	struct dm_list removal_lvs;

	dm_list_init(&removal_lvs);

	if (!new_segtype) {
		log_error(INTERNAL_ERROR "New segtype not specified");
		return 0;
	}

	if (!(striped_segtype = get_segtype_from_string(lv->vg->cmd, SEG_TYPE_NAME_STRIPED)))
		return_0;

	/* Given segtype of @lv */
	if (!seg_is_striped(seg) && /* Catches linear = "overloaded striped with one area" as well */
	    !seg_is_mirror(seg) &&
	    !seg_is_raid(seg))
		goto err;

	/* Requested segtype */
	if (!segtype_is_linear(new_segtype) &&
	    !segtype_is_striped(new_segtype) &&
	    !segtype_is_mirror(new_segtype) &&
	    !segtype_is_raid(new_segtype))
		goto err;

	/* Define new image count if not passed in */
	new_image_count = new_image_count ?: seg->area_count;
PFLA("new_segtype=%s new_image_count=%u segtype=%s, seg->area_count=%u", new_segtype->name, new_image_count, seg->segtype->name, seg->area_count);

	if (!_check_max_raid_devices(new_image_count))
		return 0;

	/* Converting raid1 -> linear given "lvconvert -m0 ..." w/o "--type ..." */
	if (new_image_count == 1 &&
	    seg->segtype == new_segtype)
		new_segtype = striped_segtype;

	/* Converting linear to raid1 given "lvconvert -mN ..." (N > 0)  w/o "--type ..." */
	if (seg_is_linear(seg) &&
	    seg->segtype == new_segtype &&
	    new_image_count > 1 &&
	    !(new_segtype = get_segtype_from_flag(lv->vg->cmd, SEG_RAID1)))
		return_0;
PFLA("new_segtype=%s new_image_count=%u segtype=%s, seg->area_count=%u", new_segtype->name, new_image_count, seg->segtype->name, seg->area_count);

	/* Define new stripe size if not passed in */
	new_stripe_size = new_stripe_size ?: seg->stripe_size;

	segtype_change = (seg->segtype != new_segtype);
	stripe_size_change = !seg->stripe_size && seg->stripe_size != new_stripe_size;
	if (segtype_change && stripe_size_change) {
		log_error("Can't change raid type and stripe size at once on %s",
			  display_lvname(lv));
		return 0;
	}

	/* @lv has to be active to perform raid conversion operatons */
	if (!lv_is_active(lv)) {
		log_error("%s must be active to perform this operation.",
			  display_lvname(lv));
		return 0;
	}

	/* If clustered VG, @lv has to be active locally */
	if (vg_is_clustered(lv->vg) && !lv_is_active_exclusive_locally(lv)) {
		log_error("%s must be active exclusive locally to"
			  " perform this operation.", display_lvname(lv));
		return 0;
	}

	/* Can'nt perfom any raid conversions on out of sync LVs */
	if (!_raid_in_sync(lv)) {
		log_error("Unable to convert %s while it is not in-sync",
			  display_lvname(lv));
		return 0;
	}

	if (!lv_info(lv->vg->cmd, lv, 0, &info, 1, 0) && driver_version(NULL, 0)) {
		log_error("Unable to retrieve logical volume information: aborting");
		return 0;
	}

	/* Get number of redundant disk for current and new segtype */
	_seg_get_redundancy(seg->segtype, seg->area_count, &cur_redundancy);
	_seg_get_redundancy(new_segtype, new_image_count = new_image_count ?: lv_raid_image_count(lv), &new_redundancy);

	/*
	 * In case of any resilience related conversion -> ask the user unless "-y/--yes" on command line
	 */
	/* HM FIXME: need to reorder redundany and conversion checks to avoid bogus user messages */
PFLA("cur_redundancy=%u new_redundancy=%u", cur_redundancy, new_redundancy);
	y = yes;
	if (new_redundancy == cur_redundancy) {
		if (!new_stripes)
			log_info("INFO: Converting active%s %s %s%s%s%s will keep "
				 "resilience of %u disk failure%s",
				 info.open_count ? " and open" : "", display_lvname(lv),
				 seg->segtype != new_segtype_tmp ? "from " : "",
				 seg->segtype != new_segtype_tmp ? _get_segtype_name(seg->segtype, seg->area_count) : "",
				 seg->segtype != new_segtype_tmp ? " to " : "",
				 seg->segtype != new_segtype_tmp ? _get_segtype_name(new_segtype_tmp, new_image_count) : "",
				 cur_redundancy,
				 (!cur_redundancy || cur_redundancy > 1) ? "s" : "");

		else
			y = 1;

	} else if (new_redundancy > cur_redundancy)
		log_info("INFO: Converting active%s %s %s%s%s%s will extend "
			 "resilience from %u disk failure%s to %u",
			 info.open_count ? " and open" : "", display_lvname(lv),
			 seg->segtype != new_segtype_tmp ? "from " : "",
			 seg->segtype != new_segtype_tmp ? _get_segtype_name(seg->segtype, seg->area_count) : "",
			 seg->segtype != new_segtype_tmp ? " to " : "",
			 seg->segtype != new_segtype_tmp ? _get_segtype_name(new_segtype_tmp, new_image_count) : "",
			 cur_redundancy,
			 (!cur_redundancy || cur_redundancy > 1) ? "s" : "",
			 new_redundancy);

	else if (new_redundancy &&
		 new_redundancy < cur_redundancy)
		log_warn("WARNING: Converting active%s %s %s%s%s%s will reduce "
			 "resilience from %u disk failures to just %u",
			 info.open_count ? " and open" : "", display_lvname(lv),
			 seg->segtype != new_segtype_tmp ? "from " : "",
			 seg->segtype != new_segtype_tmp ? _get_segtype_name(seg->segtype, seg->area_count) : "",
			 seg->segtype != new_segtype_tmp ? " to " : "",
			 seg->segtype != new_segtype_tmp ? _get_segtype_name(new_segtype_tmp, new_image_count) : "",
			 cur_redundancy, new_redundancy);

	else if (!new_redundancy && cur_redundancy)
		log_warn("WARNING: Converting active%s %s from %s to %s will loose "
			 "all resilience to %u disk failure%s",
			 info.open_count ? " and open" : "", display_lvname(lv),
			 _get_segtype_name(seg->segtype, seg->area_count),
			 _get_segtype_name(new_segtype_tmp, new_image_count),
			 cur_redundancy, cur_redundancy > 1 ? "s" : "");

	else
		y = 1;


	/****************************************************************************/
	/* No --type arg */
	/* Linear/raid0 with 1 image to raid1 via "-mN" option */
	if (seg->segtype == new_segtype_tmp &&
	    (seg_is_linear(seg) || (seg_is_any_raid0(seg) && seg->area_count == 1)) &&
    	    new_image_count > 1 &&
	    !(new_segtype_tmp = get_segtype_from_flag(lv->vg->cmd, SEG_RAID1)))
		return_0;

	if (!y) {
		if (segtype_change &&
		    yes_no_prompt("Do you really want to convert %s with type %s to %s? [y/n]: ",
				display_lvname(lv),
				_get_segtype_name(seg->segtype, seg->area_count),
				_get_segtype_name(new_segtype_tmp, new_image_count)) == 'n') {
			log_error("Logical volume %s NOT converted", display_lvname(lv));
			return 0;
		}

		if (stripe_size_change &&
		    yes_no_prompt("Do you really want to convert %s from stripesize %d to stripesize %d? [y/n]: ",
				display_lvname(lv),
				seg->stripe_size, new_stripe_size) == 'n') {
			log_error("Logical volume %s NOT converted", display_lvname(lv));
			return 0;
		}
	}
	if (sigint_caught())
		return_0;

	/* Now archive metadata after the user has confirmed */
	if (!archive(lv->vg))
		return_0;
	
	/*
	 * Staying on the same level -> reshape required to change:
	 *
	 * - #stripes (i.e. # of disks)
	 * - stripe size
	 * - layout (e.g. raid6_zr -> raid6_ls_6
	 */
	if (is_same_level(seg->segtype, new_segtype) &&
	    seg_is_striped_raid(seg) &&
	    !seg_is_any_raid0(seg) &&
	    ((stripes && _data_rimages_count(seg, seg->area_count) != stripes) ||
	     (new_stripe_size && new_stripe_size != seg->stripe_size))) {
		stripes = stripes ?: _data_rimages_count(seg, seg->area_count);

		return _raid_reshape(lv, new_segtype, yes, force, stripes, new_stripe_size, allocate_pvs);
	}

	/*
	 * Table driven takeover, i.e. conversions from one segment type to another
	 */
	r = _takeover_fn_table[_takeover_fn_idx(seg->segtype, seg->area_count)][_takeover_fn_idx(new_segtype, new_image_count)]
	     (lv, yes, force, new_segtype, new_image_count, new_stripes, new_stripe_size, allocate_pvs);
	if (!r)
		_log_possible_conversion_types(lv);

	return r;

err:
	/* FIXME: enhance message */
	log_error("Converting the segment type for %s (directly) from %s to %s"
		  " is not supported.", display_lvname(lv),
		  lvseg_name(seg), new_segtype_tmp->name);

	return 0;
}

static int _remove_partial_multi_segment_image(struct logical_volume *lv,
					       struct dm_list *remove_pvs)
{
	uint32_t s, extents_needed;
	struct lv_segment *rm_seg, *raid_seg = first_seg(lv);
	struct logical_volume *rm_image = NULL;
	struct physical_volume *pv;

	if (!(lv->status & PARTIAL_LV))
		return_0;

	for (s = 0; s < raid_seg->area_count; s++) {
		extents_needed = 0;
		if ((seg_lv(raid_seg, s)->status & PARTIAL_LV) &&
		    lv_is_on_pvs(seg_lv(raid_seg, s), remove_pvs) &&
		    (dm_list_size(&(seg_lv(raid_seg, s)->segments)) > 1)) {
			rm_image = seg_lv(raid_seg, s);

			/* First, how many damaged extents are there */
			if (seg_metalv(raid_seg, s)->status & PARTIAL_LV)
				extents_needed += seg_metalv(raid_seg, s)->le_count;
			dm_list_iterate_items(rm_seg, &rm_image->segments) {
				/*
				 * segment areas are for stripe, mirror, raid,
				 * etc.  We only need to check the first area
				 * if we are dealing with RAID image LVs.
				 */
				if (seg_type(rm_seg, 0) != AREA_PV)
					continue;
				pv = seg_pv(rm_seg, 0);
				if (pv->status & MISSING_PV)
					extents_needed += rm_seg->len;
			}
			log_debug("%u extents needed to repair %s",
				  extents_needed, rm_image->name);

			/* Second, do the other PVs have the space */
			dm_list_iterate_items(rm_seg, &rm_image->segments) {
				if (seg_type(rm_seg, 0) != AREA_PV)
					continue;
				pv = seg_pv(rm_seg, 0);
				if (pv->status & MISSING_PV)
					continue;

				if ((pv->pe_count - pv->pe_alloc_count) >
				    extents_needed) {
					log_debug("%s has enough space for %s",
						  pv_dev_name(pv),
						  rm_image->name);
					goto has_enough_space;
				}
				log_debug("Not enough space on %s for %s",
					  pv_dev_name(pv), rm_image->name);
			}
		}
	}

	/*
	 * This is likely to be the normal case - single
	 * segment images.
	 */
	return_0;

has_enough_space:
	/*
	 * Now we have a multi-segment, partial image that has enough
	 * space on just one of its PVs for the entire image to be
	 * replaced.  So, we replace the image's space with an error
	 * target so that the allocator can find that space (along with
	 * the remaining free space) in order to allocate the image
	 * anew.
	 */
	return _replace_lv_with_error_segment(rm_image);
}

/* HM */
static int _avoid_pvs_of_lv(struct logical_volume *lv, void *data)
{
	struct dm_list *allocate_pvs = (struct dm_list *) data;
	struct pv_list *pvl, *tmp;

	dm_list_iterate_items_safe(pvl, tmp, allocate_pvs)
		if (!(lv->status & PARTIAL_LV) &&
		    lv_is_on_pv(lv, pvl->pv))
			pvl->pv->status |= PV_ALLOCATION_PROHIBITED;

	return 1;
}

/*
 * Prevent any PVs holding other image components of @lv from being used for allocation,
 * I.e. remove respective PVs from @allocatable_pvs
 */
static int _avoid_pvs_with_other_images_of_lv(struct logical_volume *lv, struct dm_list *allocate_pvs)
{
	return for_each_sub_lv(lv, _avoid_pvs_of_lv, allocate_pvs);
}

/* HM Helper fn to generate LV names and set segment area lv */
static int _generate_name_and_set_segment(struct logical_volume *lv,
					  uint32_t s, uint32_t sd,
					  struct dm_list *lvs, char **tmp_names)
{
	struct lv_segment *raid_seg = first_seg(lv);
	struct lv_list *lvl = dm_list_item(dm_list_first(lvs), struct lv_list);

	dm_list_del(&lvl->list);
	if (!(tmp_names[sd] = _generate_raid_name(lv, s == sd ? "rmeta" : "rimage", s)))
		return_0;
	if (!set_lv_segment_area_lv(raid_seg, s, lvl->lv, 0, lvl->lv->status)) {
		log_error("Failed to add %s to %s", lvl->lv->name, lv->name);
		return 0;
	}

	lv_set_hidden(lvl->lv);
	return 1;
}

/*
 * lv_raid_replace
 * @lv
 * @remove_pvs
 * @allocate_pvs
 *
 * Replace the specified PVs.
 */
int lv_raid_replace(struct logical_volume *lv,
		    struct dm_list *remove_pvs,
		    struct dm_list *allocate_pvs)
{
	int partial_segment_removed = 0;
	uint32_t s, sd, match_count = 0;
	struct dm_list old_lvs;
	struct dm_list new_meta_lvs, new_data_lvs;
	struct lv_segment *raid_seg = first_seg(lv);
	struct lv_list *lvl;
	char *tmp_names[raid_seg->area_count * 2];

	dm_list_init(&old_lvs);
	dm_list_init(&new_meta_lvs);
	dm_list_init(&new_data_lvs);

	/* Replacement for raid0 would request data loss */
	if (seg_is_any_raid0(raid_seg)) {
		log_error("Replacement of devices in %s %s LV prohibited.",
			  display_lvname(lv), raid_seg->segtype->name);
		return 0;
	}

	if (lv->status & PARTIAL_LV)
		lv->vg->cmd->partial_activation = 1;

	if (!lv_is_active_exclusive_locally(lv_lock_holder(lv))) {
		log_error("%s must be active %sto perform this operation.",
			  display_lvname(lv),
			  vg_is_clustered(lv->vg) ? "exclusive locally " : "");
		return 0;
	}

	if (!_raid_in_sync(lv)) {
		log_error("Unable to replace devices in %s while it is"
			  " not in-sync.", display_lvname(lv));
		return 0;
	}

	if (!archive(lv->vg))
		return_0;

	/*
	 * How many sub-LVs are being removed?
	 */
	for (s = 0; s < raid_seg->area_count; s++) {
		if ((seg_type(raid_seg, s) == AREA_UNASSIGNED) ||
		    (seg_metatype(raid_seg, s) == AREA_UNASSIGNED)) {
			log_error("Unable to replace RAID images while the "
				  "array has unassigned areas");
			return 0;
		}

		if (lv_is_virtual(seg_lv(raid_seg, s)) ||
		    lv_is_virtual(seg_metalv(raid_seg, s)) ||
		    lv_is_on_pvs(seg_lv(raid_seg, s), remove_pvs) ||
		    lv_is_on_pvs(seg_metalv(raid_seg, s), remove_pvs))
			match_count++;
	}

	if (!match_count) {
		log_verbose("%s does not contain devices specified"
			    " for replacement", display_lvname(lv));
		return 1;
	} else if (match_count == raid_seg->area_count) {
		log_error("Unable to remove all PVs from %s at once.",
			  display_lvname(lv));
		return 0;
	} else if (raid_seg->segtype->parity_devs &&
		   (match_count > raid_seg->segtype->parity_devs)) {
		log_error("Unable to replace more than %u PVs from (%s) %s",
			  raid_seg->segtype->parity_devs,
			  lvseg_name(raid_seg),
			  display_lvname(lv));
		return 0;
	} else if (seg_is_raid10(raid_seg)) {
		uint32_t i, rebuilds_per_group = 0;
		/* FIXME: We only support 2-way mirrors in RAID10 currently */
		uint32_t copies = 2;

		for (i = 0; i < raid_seg->area_count * copies; i++) {
			s = i % raid_seg->area_count;
			if (!(i % copies))
				rebuilds_per_group = 0;
			if (lv_is_on_pvs(seg_lv(raid_seg, s), remove_pvs) ||
			    lv_is_on_pvs(seg_metalv(raid_seg, s), remove_pvs) ||
			    lv_is_virtual(seg_lv(raid_seg, s)) ||
			    lv_is_virtual(seg_metalv(raid_seg, s)))
				rebuilds_per_group++;
			if (rebuilds_per_group >= copies) {
				log_error("Unable to replace all the devices "
					  "in a RAID10 mirror group.");
				return 0;
			}
		}
	}

	/* Prevent any PVs holding image components from being used for allocation */
	if (!_avoid_pvs_with_other_images_of_lv(lv, allocate_pvs)) {
		log_error("Failed to prevent PVs holding image components "
			  "from being used for allocation.");
		return 0;
	}

	/*
	 * Allocate the new image components first
	 * - This makes it easy to avoid all currently used devs
	 * - We can immediately tell if there is enough space
	 *
	 * - We need to change the LV names when we insert them.
	 */
try_again:
	if (!_alloc_image_components(lv, 1, allocate_pvs, match_count,
				     &new_meta_lvs, &new_data_lvs)) {
		if (!(lv->status & PARTIAL_LV)) {
			log_error("LV %s is not partial.", display_lvname(lv));
			return 0;
		}

		/* This is a repair, so try to do better than all-or-nothing */
		match_count--;
		if (match_count > 0) {
			log_error("Failed to replace %u devices."
				  "  Attempting to replace %u instead.",
				  match_count, match_count+1);
			/*
			 * Since we are replacing some but not all of the bad
			 * devices, we must set partial_activation
			 */
			lv->vg->cmd->partial_activation = 1;
			goto try_again;
		} else if (!match_count && !partial_segment_removed) {
			/*
			 * We are down to the last straw.  We can only hope
			 * that a failed PV is just one of several PVs in
			 * the image; and if we extract the image, there may
			 * be enough room on the image's other PVs for a
			 * reallocation of the image.
			 */
			if (!_remove_partial_multi_segment_image(lv, remove_pvs))
				return_0;

			match_count = 1;
			partial_segment_removed = 1;
			lv->vg->cmd->partial_activation = 1;
			goto try_again;
		}
		log_error("Failed to allocate replacement images for %s",
			  display_lvname(lv));

		return 0;
	}

	/* The new metadata LV(s) must be cleared before being added to the array */
	log_debug_metadata("Clearing newly allocated replacement metadata LV");
	if (!_clear_lvs(&new_meta_lvs))
		return 0;

	/*
	 * Remove the old images
	 * - If we did this before the allocate, we wouldn't have to rename
	 *   the allocated images, but it'd be much harder to avoid the right
	 *   PVs during allocation.
	 *
	 * - If this is a repair and we were forced to call
	 *   _remove_partial_multi_segment_image, then the remove_pvs list
	 *   is no longer relevant - _raid_extract_images is forced to replace
	 *   the image with the error target.  Thus, the full set of PVs is
	 *   supplied - knowing that only the image with the error target
	 *   will be affected.
	 */
	if (!_raid_extract_images(lv, raid_seg->area_count - match_count,
				  partial_segment_removed ?
				  &lv->vg->pvs : remove_pvs, 0 /* Don't shift */,
				  &old_lvs, &old_lvs)) {
		log_error("Failed to remove the specified images from %s",
			  display_lvname(lv));
		return 0;
	}

	/*
	 * Now that they are extracted and visible, make the system aware
	 * of their new names.
	 */
	dm_list_iterate_items(lvl, &old_lvs)
		if (!activate_lv_excl_local(lv->vg->cmd, lvl->lv))
			return_0;

	/*
	 * Skip metadata operation normally done to clear the metadata sub-LVs.
	 *
	 * The LV_REBUILD flag is set on the new sub-LVs,
	 * so they will be rebuilt and we don't need to clear the metadata dev.
	 */

	for (s = 0; s < raid_seg->area_count; s++) {
		sd = s + raid_seg->area_count;

		if ((seg_type(raid_seg, s) == AREA_UNASSIGNED) &&
		    (seg_metatype(raid_seg, s) == AREA_UNASSIGNED)) {
			if (!_generate_name_and_set_segment(lv, s, s,  &new_meta_lvs, tmp_names) ||
			    !_generate_name_and_set_segment(lv, s, sd, &new_data_lvs, tmp_names))
				return 0;
		} else
			tmp_names[s] = tmp_names[sd] = NULL;
	}

	if (!lv_update_and_reload_origin(lv))
		return_0;

	if (!_deactivate_and_remove_lvs(lv->vg, &old_lvs))
		return 0;

	/* Update new sub-LVs to correct name and clear REBUILD flag */
	for (s = 0; s < raid_seg->area_count; s++) {
		sd = s + raid_seg->area_count;
		if (tmp_names[s] && tmp_names[sd]) {
			seg_metalv(raid_seg, s)->name = tmp_names[s];
			seg_lv(raid_seg, s)->name = tmp_names[sd];
			seg_metalv(raid_seg, s)->status &= ~LV_REBUILD;
			seg_lv(raid_seg, s)->status &= ~LV_REBUILD;
		}
	}

	/* FIXME: will this discontinue a running rebuild of the replaced legs? */
	/* HM: no, because md will restart based on the recovery_cp offset in the superblock */
	if (!lv_update_and_reload_origin(lv))
		return_0;

	return 1;
}

int lv_raid_remove_missing(struct logical_volume *lv)
{
	uint32_t s;
	struct lv_segment *seg = first_seg(lv);

	if (!(lv->status & PARTIAL_LV)) {
		log_error(INTERNAL_ERROR "%s is not a partial LV",
			  display_lvname(lv));
		return 0;
	}

	if (!archive(lv->vg))
		return_0;

	log_debug("Attempting to remove missing devices from %s LV, %s",
		  lvseg_name(seg), lv->name);

	/*
	 * FIXME: Make sure # of compromised components will not affect RAID
	 */
	for (s = 0; s < seg->area_count; s++)
		if (!_replace_lv_with_error_segment(seg_lv(seg, s)) ||
		    !_replace_lv_with_error_segment(seg_metalv(seg, s)))
			return 0;

	if (!lv_update_and_reload(lv))
		return_0;

	return 1;
}

/* Return 1 if a partial raid LV can be activated redundantly */
static int _partial_raid_lv_is_redundant(const struct logical_volume *lv)
{
	struct lv_segment *raid_seg = first_seg(lv);
	uint32_t copies;
	uint32_t i, s, rebuilds_per_group = 0;
	uint32_t failed_components = 0;

	if (seg_is_raid10(raid_seg)) {
		/* FIXME: We only support 2-way mirrors in RAID10 currently */
		copies = 2;
		for (i = 0; i < raid_seg->area_count * copies; i++) {
			s = i % raid_seg->area_count;

			if (!(i % copies))
				rebuilds_per_group = 0;

			if ((seg_lv(raid_seg, s)->status & PARTIAL_LV) ||
			    (seg_metalv(raid_seg, s)->status & PARTIAL_LV) ||
			    lv_is_virtual(seg_lv(raid_seg, s)) ||
			    lv_is_virtual(seg_metalv(raid_seg, s)))
				rebuilds_per_group++;

			if (rebuilds_per_group >= copies) {
				log_verbose("An entire mirror group has failed in %s.",
					    display_lvname(lv));
				return 0;	/* Insufficient redundancy to activate */
			}
		}

		return 1; /* Redundant */
	}

	for (s = 0; s < raid_seg->area_count; s++) {
		if ((seg_lv(raid_seg, s)->status & PARTIAL_LV) ||
		    (seg_metalv(raid_seg, s)->status & PARTIAL_LV) ||
		    lv_is_virtual(seg_lv(raid_seg, s)) ||
		    lv_is_virtual(seg_metalv(raid_seg, s)))
			failed_components++;
	}

	if (failed_components && seg_is_any_raid0(raid_seg)) {
		log_verbose("No components of raid LV %s may fail",
			    display_lvname(lv));
		return 0;

	} else if (failed_components == raid_seg->area_count) {
		log_verbose("All components of raid LV %s have failed.",
			    display_lvname(lv));
		return 0;	/* Insufficient redundancy to activate */
	} else if (raid_seg->segtype->parity_devs &&
		   (failed_components > raid_seg->segtype->parity_devs)) {
		log_verbose("More than %u components from %s %s have failed.",
			    raid_seg->segtype->parity_devs,
			    lvseg_name(raid_seg),
			    display_lvname(lv));
		return 0;	/* Insufficient redundancy to activate */
	}

	return 1;
}

/* Sets *data to 1 if the LV cannot be activated without data loss */
static int _lv_may_be_activated_in_degraded_mode(struct logical_volume *lv, void *data)
{
	int *not_capable = (int *)data;
	uint32_t s;
	struct lv_segment *seg;

	if (*not_capable)
		return 1;	/* No further checks needed */

	if (!(lv->status & PARTIAL_LV))
		return 1;

	if (lv_is_raid(lv)) {
		*not_capable = !_partial_raid_lv_is_redundant(lv);
		return 1;
	}

	/* Ignore RAID sub-LVs. */
	if (lv_is_raid_type(lv))
		return 1;

	dm_list_iterate_items(seg, &lv->segments)
		for (s = 0; s < seg->area_count; s++)
			if (seg_type(seg, s) != AREA_LV) {
				log_verbose("%s contains a segment incapable of degraded activation",
					    display_lvname(lv));
				*not_capable = 1;
			}

	return 1;
}

int partial_raid_lv_supports_degraded_activation(const struct logical_volume *clv)
{
	int not_capable = 0;
	struct logical_volume * lv = (struct logical_volume *)clv; /* drop const */

	if (!_lv_may_be_activated_in_degraded_mode(lv, &not_capable) || not_capable)
		return_0;

	if (!for_each_sub_lv(lv, _lv_may_be_activated_in_degraded_mode, &not_capable)) {
		log_error(INTERNAL_ERROR "for_each_sub_lv failure.");
		return 0;
	}

	return !not_capable;
}
