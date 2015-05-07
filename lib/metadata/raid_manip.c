/*
 * Copyright (C) 2011-2014 Red Hat, Inc. All rights reserved.
 *
 * This file is part of LVM2.
 a
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

#if 1
#include "dump.h"
#endif

#if 1
#define PFL() printf("%s %u\n", __func__, __LINE__);
#define PFLA(format, arg...) printf("%s %u " format "\n", __func__, __LINE__, arg);
#else
#define PFL()
#define PFLA(format, arg...)
#endif

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

/* Default region_size on @lv unless already set */
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

static int _cmp_level(const struct segment_type *t1, const struct segment_type *t2)
{
	return strncmp(t1->name, t2->name, 5);
}

static int is_same_level(const struct segment_type *t1, const struct segment_type *t2)
{
	return !_cmp_level(t1, t2);
}

static int is_level_up(const struct segment_type *t1, const struct segment_type *t2)
{
	if (segtype_is_raid(t1) && segtype_is_striped(t2))
		return 0;

	if (segtype_is_striped(t1) && segtype_is_raid(t2))
		return 1;

	return _cmp_level(t1, t2) < 0;
}

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

/* Deactivate and remove the LVs on @removal_lvs list */
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
		log_error("Unable to determine sync status of %s/%s.",
			  lv->vg->name, lv->name);
		return 0;
	}

	return (sync_percent == DM_PERCENT_100) ? 1 : 0;
}

/* Remove seg from segments using @lv and set one segment mapped to error target to @lv */
static int _remove_and_set_error_target(struct logical_volume *lv, struct lv_segment *seg)
{
	lv_set_visible(lv);

	if (!remove_seg_from_segs_using_this_lv(lv, seg))
		return_0;

	return replace_lv_with_error_segment(lv);
}

/*
 * _raid_remove_top_layer
 * @lv
 * @removal_list
 *
 * Remove top layer of RAID LV in order to convert to linear.
 * This function makes no on-disk changes.  The residual LVs
 * returned in 'removal_list' must be freed by the caller.
 *
 * Returns: 1 on succes, 0 on failure
 */
static int _raid_remove_top_layer(struct logical_volume *lv,
				  struct dm_list *removal_list)
{
	struct lv_list *lvl_array, *lvl;
	struct logical_volume *lv_tmp;
	struct lv_segment *seg = first_seg(lv);

	if (!seg_is_mirrored(seg)) {
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

	if (!(lvl_array = dm_pool_alloc(lv->vg->vgmem, 2 * sizeof(*lvl))))
		return_0;

	if (seg->meta_areas) {
		lv_tmp = seg_metalv(seg, 0);
		lv_tmp->status &= ~RAID_META;
		lv_set_visible(lv_tmp);
		lvl_array[0].lv = lv_tmp;
PFL();
		/* Remove reference from top-layer lv to the rmeta one. */
		if (!remove_seg_from_segs_using_this_lv(lv_tmp, seg))
			return_0;

		seg_metatype(seg, 0) = AREA_UNASSIGNED;
		dm_list_add(removal_list, &(lvl_array[0].list));
	}
PFL();
	/* Add remaining last image lv to removal_list */
	lv_tmp = seg_lv(seg, 0);
	lv_tmp->status &= ~RAID_IMAGE;
	lv_set_visible(lv_tmp);
	lvl_array[1].lv = lv_tmp;
	dm_list_add(removal_list, &(lvl_array[1].list));
PFL();
	if (!remove_layer_from_lv(lv, lv_tmp))
		return_0;
PFL();
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

PFLA("Clearing metadata area of %s/%s", lv->vg->name, lv->name);
	log_verbose("Clearing metadata area of %s/%s",
		    lv->vg->name, lv->name);
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

/* Makes on-disk metadata changes */
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
	if (!vg || !vg_write(vg) || !vg_commit(vg))
		return_0;

PFL();
	dm_list_iterate_items(lvl, lv_list)
		if (!_clear_lv(lvl->lv))
			return 0;

	return 1;
}

/* Check for maximum supported devices caused by the kernel superblock bitfield constraint */
static int _check_maximum_devices(uint32_t num_devices)
{
	if (num_devices > DEFAULT_RAID_MAX_IMAGES) {
		log_error("Unable to handle arrays with more than %u devices",
			  DEFAULT_RAID_MAX_IMAGES);
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
 * Returns: 1 on success, 0 on failure
 */
static int _shift_image_components(struct lv_segment *seg)
{
	uint32_t s, missing;

	if (!seg_is_raid(seg))
		return_0;

	/* Should not be possible here, but... */
	if (!_check_maximum_devices(seg->area_count))
		return 0;

	log_very_verbose("Shifting images in %s", seg->lv->name);

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
 * Eliminate the extracted LVs on @removal_list from @vg incl. vg write, commit and backup 
 */
static int _eliminate_extracted_lvs(struct volume_group *vg, struct dm_list *removal_list)
{
	sync_local_dev_names(vg->cmd);

PFL();
	if (!dm_list_empty(removal_list)) {
		if (!_deactivate_and_remove_lvs(vg, removal_list))
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
 * Reallocate both data and metadata areas of segmen @seg to new amount in @ares
 */
static int _realloc_meta_and_data_seg_areas(struct logical_volume *lv, struct lv_segment *seg,
					     uint32_t areas)
{
	return (_realloc_seg_areas(lv, seg, areas, &seg->meta_areas) &&
	        _realloc_seg_areas(lv, seg, areas, &seg->areas)) ? 1 : 0;
}

/* Add new @lvs to @lv at @area_offset */
static int _add_sublvs_to_lv(struct logical_volume *lv, int delete_from_list,
			      uint64_t lv_flags, struct dm_list *lvs, uint32_t area_offset)
{
	uint32_t s = area_offset;
	struct lv_segment *seg = first_seg(lv);
	struct lv_list *lvl, *tmp;

	dm_list_iterate_items_safe(lvl, tmp, lvs) {
		if (delete_from_list)
			dm_list_del(&lvl->list);

		if (!set_lv_segment_area_lv(seg, s++, lvl->lv, 0 /* le */,
					    lvl->lv->status)) {
			log_error("Failed to add %s to %s", lvl->lv->name, lv->name);
			return 0;
		}

		if (lv_flags & VISIBLE_LV)
			lv_set_visible(lvl->lv);
		else
			lv_set_hidden(lvl->lv);

		if (lv_flags & LV_REBUILD)
			lvl->lv->status |= LV_REBUILD;
		else
			lvl->lv->status &= ~LV_REBUILD;
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
		if (!(segtype = get_segtype_from_string(lv->vg->cmd, "striped")))
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
uint32_t r;

	region_size = region_size ?: get_default_region_size(cmd);
	regions = rimage_extents * extent_size / region_size;

	/* raid and bitmap superblocks + region bytes */
	bytes = 2 * 4096 + dm_div_up(regions, 8);
	sectors = dm_div_up(bytes, 512);

PFLA("sectors=%llu", (long long unsigned) sectors);
	r = dm_div_up(sectors, extent_size);
PFLA("regions=%llu r=%llu", (long long unsigned) regions, (long long unsigned) r);
return r;
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
 * Allocate a RAID metadata device for the given LV (which is or will
 * be the associated RAID data device).  The new metadata device must
 * be allocated from the same PV(s) as the data device.
 */
static int _alloc_rmeta_for_lv(struct logical_volume *data_lv,
		struct logical_volume **meta_lv)
{
	struct dm_list allocatable_pvs;
	struct alloc_handle *ah;
	struct lv_segment *seg = first_seg(data_lv);
	char *p, base_name[NAME_LEN];

	dm_list_init(&allocatable_pvs);

	if (!seg_is_linear(seg)) {
		log_error(INTERNAL_ERROR "Unable to allocate RAID metadata "
				"area for non-linear LV, %s", data_lv->name);
		return 0;
	}

	_check_and_init_region_size(data_lv);

	(void) dm_strncpy(base_name, data_lv->name, sizeof(base_name));
	if ((p = strstr(base_name, "_mimage_")) ||
			(p = strstr(base_name, "_rimage_")))
		*p = '\0';

	if (!get_pv_list_for_lv(data_lv->vg->cmd->mem,
				data_lv, &allocatable_pvs)) {
		log_error("Failed to build list of PVs for %s/%s",
				data_lv->vg->name, data_lv->name);
		return 0;
	}

	if (!(ah = allocate_extents(data_lv->vg, NULL, seg->segtype,
				    0, 1, 0,
				    seg->region_size,
				    _raid_rmeta_extents(data_lv->vg->cmd, data_lv->le_count,
							seg->region_size, data_lv->vg->extent_size),
				    &allocatable_pvs, data_lv->alloc, 0, NULL)))
		return_0;

	if (!(*meta_lv = _alloc_image_component(data_lv, base_name, ah, 0, RAID_META))) {
		alloc_destroy(ah);
		return_0;
	}

	alloc_destroy(ah);

	return 1;
}

/*
 * Allocate metadata devs for all @new_data_devs and link them to list @new_meta_lvs
 */
static int _alloc_rmeta_devs_for_rimage_devs(struct logical_volume *lv,
					     struct dm_list *new_data_lvs, struct dm_list *new_meta_lvs)
{
	uint32_t a = 0, raid_devs = 0;
	struct dm_list *l;
	struct lv_list *lvl, *lvl_array;

	dm_list_iterate(l, new_data_lvs)
		raid_devs++;

	if (!raid_devs)
		return 0;

	lvl_array = dm_pool_zalloc(lv->vg->vgmem, raid_devs * sizeof(*lvl_array));
	if (!lvl_array)
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
	uint32_t count = lv_raid_image_count(lv), s;
	struct lv_list *lvl_array;
	struct dm_list data_lvs;
	struct lv_segment *seg = first_seg(lv);

	dm_list_init(&data_lvs);

PFLA("seg->meta_areas=%p", seg->meta_areas);
	/*
	 * A complete resync will be done because of
	 * the new raid4/5/6 set, no need to mark each sub-lv
	 *
	 * -> reset rebuild flag
	 */
	// lv_flags = 0;

	if (!(lvl_array = dm_pool_alloc(lv->vg->vgmem, count * sizeof(*lvl_array))))
		return_0;

	for (s = 0; s < count; s++) {
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
				   struct dm_list *new_meta_lvs,
				   struct dm_list *new_data_lvs)
{
	uint32_t s;
	uint32_t extents;
	struct lv_segment *seg = first_seg(lv);
	const struct segment_type *segtype;
	struct alloc_handle *ah = NULL;
	struct dm_list *parallel_areas;
	struct lv_list *lvl_array;

	if (!new_meta_lvs && !new_data_lvs)
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
	else if (!(segtype = get_segtype_from_string(lv->vg->cmd, SEG_TYPE_NAME_RAID1)))
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
		if (new_meta_lvs || new_data_lvs) {
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
		 * to accompany the data LVs by not passing in @new_meta_lvs
		 */
		if (new_meta_lvs) {
			if (!(lvl_array[s + count].lv = _alloc_image_component(lv, NULL, ah, s + count, RAID_META)))
				goto err;

			dm_list_add(new_meta_lvs, &(lvl_array[s + count].list));
		}

		if (new_data_lvs) {
			if (!(lvl_array[s].lv = _alloc_image_component(lv, NULL, ah, s, RAID_IMAGE)))
				goto err;

			dm_list_add(new_data_lvs, &(lvl_array[s].list));
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

/* Cleanly remove newly-allocated LVs that failed insertion attempt */
static int _remove_lvs(struct dm_list *lvs)
{
	struct lv_list *lvl;

	dm_list_iterate_items(lvl, lvs)
		if (!lv_remove(lvl->lv))
			return_0;

	return 0;
}

/* Factored out function to allocate an rmeta dev for a linear one */
static int _alloc_rmeta_for_linear(struct logical_volume *lv, struct dm_list *meta_lvs)
{
	struct lv_list *lvl;

	if (!(lvl = dm_pool_alloc(lv->vg->vgmem, sizeof(*lvl)))) {
		log_error("Memory allocation failed");
		return 0;
	}

	if (!_alloc_rmeta_for_lv(lv, &lvl->lv))
		return_0;

	dm_list_add(meta_lvs, &lvl->list);

	return 1;
}

/* Correct LV names for @data_lvs in case of a linear @lv */
static int _correct_data_lv_names(struct logical_volume *lv, uint32_t count, struct dm_list *data_lvs)
{
	struct dm_list *l;
	struct lv_list *lvl, *lvl_n;

	dm_list_iterate(l, data_lvs) {
		lvl = dm_list_item(l, struct lv_list);

		if (l == dm_list_last(data_lvs)) {
			if (!(lvl->lv->name = _generate_raid_name(lv, "rimage", count)))
				return_0;
			continue;
		}

		lvl_n = dm_list_item(l->n, struct lv_list);
		lvl->lv->name = lvl_n->lv->name;
	}

	return 1;
}

/* Return length of unsigned @idx as a string */
static unsigned _unsigned_str_len(unsigned idx)
{
	unsigned r = 0;

	do  {
		r++;
	} while ((idx /= 10));

	return r;
}

/* Create an rimage string suffix with @idx appended */
static const char *_generate_rimage_suffix(struct logical_volume *lv, unsigned idx)
{
	const char *type = "_rimage";
	char *suffix;
	size_t len = strlen(type) + _unsigned_str_len(idx) + 1;

	if (!(suffix = dm_pool_alloc(lv->vg->vgmem, len))) {
		log_error("Failed to allocate name suffix.");
		return 0;
	}

	if (dm_snprintf(suffix, len, "%s%u", type, idx) < 0)
		return_0;

	return suffix;
}

/* Insert RAID layer on top of @lv with suffix counter @idx */
static int _insert_raid_layer_for_lv(struct logical_volume *lv, const char *sfx, unsigned idx)
{
	uint64_t flags = RAID | LVM_READ | LVM_WRITE;
	const char *suffix = sfx ?: _generate_rimage_suffix(lv, idx);

	if (!insert_layer_for_lv(lv->vg->cmd, lv, flags, suffix))
		return 0;

	seg_lv(first_seg(lv), 0)->status |= RAID_IMAGE | flags;

	return 1;
}

/* Convert linear @lv to raid1 */
static int _convert_linear_to_raid1(struct logical_volume *lv)
{
	struct lv_segment *seg = first_seg(lv);
	uint32_t region_size = seg->region_size;

	if (!_insert_raid_layer_for_lv(lv, "_rimage_0", 0))
		return 0;

	/* Segment has changed */
	seg = first_seg(lv);
	seg_lv(seg, 0)->status |= RAID_IMAGE | LVM_READ | LVM_WRITE;
	seg->region_size = region_size;
	_check_and_init_region_size(lv);

	if (!(seg->segtype = get_segtype_from_string(lv->vg->cmd, SEG_TYPE_NAME_RAID1)))
		return_0;

	return 1;
}

/* Reset any rebuild or reshape disk flags on @lv, first segment already passed to the kernel */
static int _reset_flags_passed_to_kernel(struct logical_volume *lv)
{
	int flag_cleared = 0;
	uint32_t s;
	struct lv_segment *seg = first_seg(lv);

	for (s = 0; s < seg->area_count; s++) {
		if ((seg_metalv(seg, s)->status & (LV_REBUILD|LV_RESHAPE_DELTA_DISKS_PLUS|LV_RESHAPE_DELTA_DISKS_MINUS)) ||
		    (seg_lv(seg, s)->status & (LV_REBUILD|LV_RESHAPE_DELTA_DISKS_PLUS|LV_RESHAPE_DELTA_DISKS_MINUS))) {
			seg_metalv(seg, s)->status &= ~(LV_REBUILD|LV_RESHAPE_DELTA_DISKS_PLUS|LV_RESHAPE_DELTA_DISKS_MINUS);
			seg_lv(seg, s)->status &= ~(LV_REBUILD|LV_RESHAPE_DELTA_DISKS_PLUS|LV_RESHAPE_DELTA_DISKS_MINUS);

			flag_cleared = 1;
		}
	}

	if (flag_cleared) {
		if (!vg_write(lv->vg) || !vg_commit(lv->vg)) {
			log_error("Failed to clear flags for %s/%s components",
				  lv->vg->name, lv->name);
			return 0;
		}

		backup(lv->vg);
	}

	return 1;
}

#if 0
/*
 * Reorder the areas in the first segment of @lv to suit raid10/raid0 layout
 *
 * raid0  (123456) -> raid10 (142536) order
 * raid10 (123456) -> raid0  (135246/246135 depending on mirror selection) order
 *
 */
/* HM FIXME: only need nr_copies / 2 in case of raid10 -> raid0 */
/* HM FIXME: find memory optimized sort */
static int _bad_raid10_reorder_seg_areas(struct logical_volume *lv, int to_raid10)
{
	struct lv_segment *seg = first_seg(lv);
	uint32_t src, dst;
	unsigned nr_copies = seg->area_count / 2 - 1;
	struct lv_segment_area *copies;
	
	if (!(copies = dm_pool_alloc(lv->vg->vgmem, 2 * nr_copies * sizeof(*copies))))
		return_0;

	/* Save n - 1 areas starting with offset 1 */
	src = 1;
	dst = 0;
	while (dst < nr_copies) {
		copies[dst] = seg->areas[src];
		copies[dst + nr_copies] = seg->meta_areas[src];
		src++;
		dst++;
	}

	if (to_raid10) {
		/* Reorder raid0 -> raid10 */
		src = seg->area_count / 2;
		dst = 1;
		while (dst < seg->area_count - 1) {
			seg->areas[dst] = seg->areas[src];
			seg->meta_areas[dst] = seg->meta_areas[src];
			src++;
			dst += 2;
		}
	
		src = 0;
		dst = 2;
		while (dst < seg->area_count - 1) {
			seg->areas[dst] = copies[src];
			seg->meta_areas[dst] = copies[src + nr_copies];
			src++;
			dst += 2;
		}
	} else {
		/* Reorder raid10 -> raid0 */
		src = 2;
		dst = 1;
		while (src < seg->area_count) {
			seg->areas[dst] = seg->areas[src];
			seg->meta_areas[dst] = seg->meta_areas[src];
			src += 2;
			dst++;
		}

		src = 0;
		dst = seg->area_count / 2 + 1;
		while (dst < seg->area_count) {
			seg->areas[dst] = copies[src];
			seg->meta_areas[dst] = copies[src + nr_copies];
			src += 2;
			dst += 2;
		}
	}

	dm_pool_free(lv->vg->vgmem, copies);

	return 1;
}
#endif

/* Helper: swap 2 LV segment areas @a1 and @a2 */
static void _swap_areas(struct lv_segment_area *a1, struct lv_segment_area *a2)
{
	struct lv_segment_area tmp = *a1;

	*a1 = *a2;
	*a2 = tmp;
}

/*
 * Reorder the areas in the first segment of @lv to suit raid10/raid0 layout
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
static void _lv_raid10_reorder_seg_areas(struct logical_volume *lv, int to_raid10)
{
	unsigned i;
	struct lv_segment *seg = first_seg(lv);
	uint32_t half_areas = seg->area_count / 2;
	short unsigned idx[seg->area_count];
	
	/* Set up index array */
	if (to_raid10)
		for (i = 0; i < seg->area_count; i++)
			idx[i] = i < half_areas ? i * 2 : (i - half_areas) * 2 + 1;
	else
#if 1
		for (i = 0; i < seg->area_count; i++)
			idx[i < half_areas ? i * 2 : (i - half_areas) * 2 + 1] = i;
#else
		/* This selection casues image name suffixes to start > 0 and needs names shifting! */
		for (i = 0; i < seg->area_count; i++)
			idx[i < half_areas ? i * 2 + 1 : (i - half_areas) * 2] = i;
#endif
for (i = 0; i < seg->area_count; i++)
PFLA("idx[%u]=%u", i, idx[i]);

	/* Sort ad swap */
	for (i = 0; i < seg->area_count - 1; i++) {
		unsigned j;
		unsigned short min;

		for (min = idx[i], j = i + 1; j < seg->area_count; j++)
			if (idx[j] < min)
				min = idx[j];

		_swap_areas(seg->areas + i, seg->areas + min);
		_swap_areas(seg->meta_areas + i, seg->meta_areas + min);
	}
}

/*
 * Add raid rmeta/rimage pair(s) to @lv to get to
 * absolute @new_count using @pvs to allocate from
 *
 */
static int _raid_add_images(struct logical_volume *lv,
			    const struct segment_type *segtype,
			    uint32_t new_count, struct dm_list *pvs)
{
	struct lv_segment *seg = first_seg(lv);
	int add_all_rmeta = 0, linear;
	int reshape_disks = (seg_is_striped_raid(seg) && segtype && is_same_level(seg->segtype, segtype));
	uint32_t s;
	uint32_t old_count = lv_raid_image_count(lv);
	uint32_t count = new_count - old_count;
	uint64_t lv_flags = LV_REBUILD;
	struct dm_list data_lvs, meta_lvs;

PFLA("seg->meta_areas=%p", seg->meta_areas);
	segtype = segtype ?: seg->segtype;
PFLA("segtype->name=%s seg->segtype->name=%s, seg->area_count=%u new_count=%u old_count=%u count=%u", segtype->name, seg->segtype->name, seg->area_count, new_count, old_count, count);

	if (!(linear = seg_is_linear(seg)) &&
	    !seg_is_raid(seg)) {
		log_error("Unable to add RAID images to %s of segment type %s",
			  lv->name, lvseg_name(seg));
		return 0;
	}

PFL();
	if (lv->status & LV_NOTSYNCED) {
		log_error("Can't add image to out-of-sync RAID LV:"
			  " use 'lvchange --resync' first.");
		return 0;
	}

	dm_list_init(&data_lvs); /* For data image additions */
	dm_list_init(&meta_lvs); /* For metadata image additions */

PFLA("seg->meta_areas=%p", seg->meta_areas);
	/*
	 * If the segtype is linear, then we must allocate a metadata
	 * LV to accompany it.
	 */
	if (linear) {
PFL();
		/*
		 * A complete resync will be done because of
		 * the new raid1 set, no need to mark each sub-lv
		 *
		 * -> reset rebuild flag
		 *
		 * Need to add an rmeta device to the
		 * given linear device as well
		 */
		lv_flags = 0;
		add_all_rmeta = 1;

		/* Allocate an rmeta device to pair with the linear image */
		if (!_alloc_rmeta_for_linear(lv, &meta_lvs))
			return 0;

	/*
	 * In case this is a conversion from raid0 to raid4/5/6,
	 * add the metadata image LVs for the raid0 rimage LVs
	 * presumably they don't exist already.
	 */
	} else if (!seg->meta_areas) {
		add_all_rmeta = 1;

		if (!_alloc_rmeta_devs_for_lv(lv, &meta_lvs))
			return 0;
	}


PFLA("seg->segtype->flags=%X lv_flags=%lX", seg->segtype->flags, lv_flags);
	/* Allocate the additional meta and data lvs requested */
	if (!_alloc_image_components(lv, 1, pvs, count, &meta_lvs, &data_lvs))
		return_0;
PFL();
	/*
	 * If linear, we must correct data LV names.  They are off-by-one
	 * because the linear volume hasn't taken its proper name of "_rimage_0"
	 * yet.  This action must be done before '_clear_lvs' because it
	 * commits the LVM metadata before clearing the LVs.
	 */
	if (linear) {
PFL();
		if (!_correct_data_lv_names(lv, count, &data_lvs))
			return 0;
	    	if (!_convert_linear_to_raid1(lv))
			return 0;
		seg = first_seg(lv);
	}
PFL();
	/* Metadata LVs must be cleared before being added to the array */
	log_debug_metadata("Clearing newly allocated metadata LVs");
	if (!_clear_lvs(&meta_lvs))
		goto fail;

	/*
	 * FIXME: It would be proper to activate the new LVs here, instead of having
	 * them activated by the suspend.  However, this causes residual device nodes
	 * to be left for these sub-lvs.
	 */

	/* Grow areas arrays for metadata and data devs  */
	log_debug_metadata("Reallocating areas arrays");
	if (!_realloc_meta_and_data_seg_areas(lv, seg, new_count)) {
		log_error("Relocation of areas arrays failed.");
		return 0;
	}

	seg->area_count = new_count;
PFL();
	/*
	 * Set segment areas for metadata sub_lvs adding
	 * an extra meta area when converting from linear
	 */
	log_debug_metadata("Adding new metadata LVs");
	if (!_add_sublvs_to_lv(lv, 0, 0, &meta_lvs, add_all_rmeta ? 0 : old_count))
		goto fail;

	/* Set segment areas for data sub_lvs */
	log_debug_metadata("Adding new data LVs");
	if (!_add_sublvs_to_lv(lv, 0, lv_flags, &data_lvs, old_count))
		goto fail;

	/* Reorder the areas in case this is a raid0 -> raid10 conversion */
	if (seg_is_any_raid0(seg) && segtype_is_raid10(segtype)) {
		log_debug_metadata("Redordering areas for raid0 -> raid10 takeover");
		_lv_raid10_reorder_seg_areas(lv, 1);
	}

	/*
	 * Reshape adding image component pairs:
	 *
	 * - reset rebuild flag on new image LVs
	 * - set delta disks plus flag on new image LVs
	 */
	if (reshape_disks) {
PFL();
		for (s = old_count; s < new_count; s++) {
PFL();
			seg_lv(seg, s)->status &= ~LV_REBUILD;
			seg_lv(seg, s)->status |= LV_RESHAPE_DELTA_DISKS_PLUS;
		}
	}
PFL();
	if (!linear)
		seg->segtype = segtype;

	if (!lv_update_and_reload_origin(lv))
		goto fail;

PFL();
	/* Reshape adding image component pairs -> change size accordingly */
	if (reshape_disks) {
		uint32_t plus_extents = count * (lv->le_count / _data_rimages_count(seg, old_count));

PFLA("lv->le_count=%u data_rimages=%u plus_extents=%u", lv->le_count, _data_rimages_count(seg, old_count), plus_extents);
		lv->le_count += plus_extents;
		lv->size = lv->le_count * lv->vg->extent_size;
		seg->len += plus_extents;
		seg->area_len += plus_extents;
PFLA("lv->le_count=%u", lv->le_count);
	}

PFL();
	/*
	 * Now that the 'REBUILD' or 'RESHAPE_DELTA_DISKS' has/have made its/their
	 * way to the kernel, we must remove the flag(s) so that the individual
	 * devices are not rebuilt/reshaped upon every activation.
	 */
	if (!_reset_flags_passed_to_kernel(lv))
		return 0;

	/* Reload striped raid again after removal of flags to change size */
	if (1) { // reshape_disks) {
		sleep(2); /* HM FIXME: REMOVEME: hack to allow for add/remove disk devel until out of place reshape is supported */

		if (!lv_update_and_reload_origin(lv))
			return_0;
	}
PFL();
	return 1;

fail:
PFL();
	/* Cleanly remove newly-allocated LVs that failed insertion attempt */
	if (!_remove_lvs(&meta_lvs) ||
	    !_remove_lvs(&data_lvs))
		return_0;

	return 0;
}

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
		lv->status &= ~RAID_META;
		break;
	case RAID_IMAGE:
		lv = seg_lv(seg, idx);
		lv->status &= ~RAID_IMAGE;
		break;
	default:
		log_error(INTERNAL_ERROR "Bad type provided to %s.", __func__);
		return 0;
	}

	lv->status &= ~RAID;

	log_very_verbose("Extracting image component %s from %s", lv->name, seg->lv->name);
	lv_set_visible(lv);

	/* release lv areas */
	if (!remove_seg_from_segs_using_this_lv(lv, seg))
		return_0;

	switch (type) {
	case RAID_META:
		seg_metatype(seg, idx) = AREA_UNASSIGNED;
		seg_metalv(seg, idx) = NULL;
		break;
	case RAID_IMAGE:
		seg_type(seg, idx) = AREA_UNASSIGNED;
		seg_lv(seg, idx) = NULL;
	}

	if (!(lv->name = _generate_raid_name(lv, "extracted", -1)))
		return_0;

	*extracted_lv = lv;

	return 1;
}

/*
 * _extract_image_components_to_*
 * @seg
 * @idx:  The index in the areas array to remove
 * @extracted_lvl_array / @extracted_lvs:  The displaced metadata + data LVs
 *
 * These functions extract the image components - setting the respective
 * 'extracted' pointers.  It appends '_extracted' to the LVs' names, so that
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
static int _extract_image_components(struct lv_segment *seg, uint32_t idx,
				     struct lv_list *lvl_array)
{
	/* Don't change extraction sequence; callers are relying on it */
	if (!_extract_image_component(seg, RAID_META, idx, &lvl_array[0].lv) ||
	    !_extract_image_component(seg, RAID_IMAGE, idx, &lvl_array[1].lv))
		return_0;

	return 1;
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
		log_error("Failed to suspend %s/%s before committing changes",
			  lv->vg->name, lv->name);
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
	log_verbose("Extracting %u image%s from %s/%s", extract,
		    (extract > 1) ? "s" : "", lv->vg->name, lv->name);

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
		if (!_extract_image_components(seg, s, lvl_array + lvl_idx)) {
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

/* Remove image component pairs from @lv defined by @new_count (< old_count) */
static int _raid_remove_images(struct logical_volume *lv,
			       const struct segment_type *segtype,
			       uint32_t new_count, struct dm_list *pvs)
{
	struct lv_segment *seg = first_seg(lv);
	int raid0 = segtype_is_raid0(segtype);
	int reshape_disks = (seg_is_striped_raid(seg) && segtype && is_same_level(seg->segtype, segtype));
	unsigned old_count = seg->area_count;
	struct dm_list removal_list;
	struct lv_list *lvl;

	/* HM FIXME: TESTME: allow to remove out-of-sync dedicated parity/Q syndrome devices */
	if (seg_is_striped_raid(seg) &&
	    (lv->status & LV_NOTSYNCED) &&
	    !((seg_is_raid5_n(seg) || seg_is_raid6_n_6(seg)) &&
	      old_count - new_count == 1)) {
		log_error("Can't remove image(s) from out-of-sync striped RAID LV:"
			  " use 'lvchange --resync' first.");
		return 0;
	}

PFLA("segtype=%s new_count=%u", segtype->name, new_count);
	dm_list_init(&removal_list);

	/* Reorder the areas in case this is a raid10 -> raid0 conversion */
	if (seg_is_raid10(seg) && segtype_is_any_raid0(segtype)) {
		log_debug_metadata("Reordering areas for raid0 -> raid10 takeover");
		_lv_raid10_reorder_seg_areas(lv, 0);
	}

	/* Extract all image and any metadata lvs past new_count */
	if (!_raid_extract_images(lv, new_count, pvs, 1,
				  &removal_list, &removal_list)) {
		log_error("Failed to extract images from %s/%s",
			  lv->vg->name, lv->name);
		return 0;
	}

	seg->area_count = new_count;

	/*
	 * In case this is a conversion to raid0,
	 * remove the metadata image LVs.
	 */
	if (raid0 && seg->meta_areas) {
		uint32_t s;
		struct lv_list *lvl_array;

		if (!(lvl_array = dm_pool_alloc(lv->vg->vgmem, sizeof(*lvl_array) * new_count)))
			return_0;

		for (s = 0; s < new_count; s++) {
			if (!_extract_image_component(seg, RAID_META, s, &lvl_array[s].lv))
				return 0;

			dm_list_add(&removal_list, &lvl_array[s].list);
		}

		seg->meta_areas = NULL;
	}

	if (segtype_is_any_raid0(segtype))
		seg->region_size = 0;

	/* Reshape adding image component pairs -> change size accordingly */
	if (reshape_disks) {
		uint32_t minus_extents = (old_count - new_count) * (lv->le_count / _data_rimages_count(seg, old_count));

PFLA("lv->le_count=%u data_rimages=%u minus_extents=%u", lv->le_count, _data_rimages_count(seg, old_count), minus_extents);
		lv->le_count -= minus_extents;
		lv->size = lv->le_count * lv->vg->extent_size;
		seg->len -= minus_extents;
		seg->area_len -= minus_extents;
PFLA("lv->le_count=%u", lv->le_count);
	}

	/* Convert to linear? */
	if (new_count == 1) {
		if (!_raid_remove_top_layer(lv, &removal_list)) {
			log_error("Failed to remove RAID layer"
			  	" after linear conversion");
			return 0;
		}

		lv->status &= ~(LV_NOTSYNCED | LV_WRITEMOSTLY);
		first_seg(lv)->writebehind = 0;
	}

	/* Shrink areas arrays for metadata and data devs  */
	if (!_realloc_meta_and_data_seg_areas(lv, seg, new_count)) {
		log_error("Relocation of areas arrays failed.");
		return 0;
	}

#if 0
dump_lv("", lv);
#endif
PFL();
	seg->segtype = segtype;
	if (!_vg_write_lv_suspend_vg_commit(lv))
		return 0;

	/*
	 * We activate the extracted sub-LVs first so they are
	 * renamed and won't conflict with the remaining sub-LVs.
	 */
PFL();
	dm_list_iterate_items(lvl, &removal_list) {
		if (!activate_lv_excl_local(lv->vg->cmd, lvl->lv)) {
			log_error("Failed to resume extracted LVs");
			return 0;
		}
	}

PFL();
	if (!resume_lv(lv->vg->cmd, lv)) {
		log_error("Failed to resume %s/%s after committing changes",
			  lv->vg->name, lv->name);
		return 0;
	}

PFL();
	/* Eliminate the residual LVs, write VG, commit it and take a backup */
	return _eliminate_extracted_lvs(lv->vg, &removal_list);
}

/*
 * HM
 *
 * Add/remove metadata areas to/from raid0
 *
 * Update metadata and reload mappings if @update_and_reload
 */
static int _raid0_add_or_remove_metadata_lvs(struct logical_volume *lv, int update_and_reload)
{
	uint32_t s;
	struct lv_segment *seg = first_seg(lv);
	struct dm_list removal_mlvs;

	dm_list_init(&removal_mlvs);

	if (seg->meta_areas) {
		struct lv_list *lvl_array;

		if (!(lvl_array = dm_pool_alloc(lv->vg->vgmem, seg->area_count * sizeof(*lvl_array))))
			return_0;

		log_debug_metadata("Extracting metadata LVs");
		for (s = 0; s < seg->area_count; s++) {
			if (!_extract_image_component(seg, RAID_META, s, &lvl_array[s].lv)) {
				log_error("Failed to extract metadata image %u from %s/%s",
					  s, lv->vg->name, lv->name);
				return 0;
			}

			dm_list_add(&removal_mlvs, &lvl_array[s].list);
		}

		seg->meta_areas = NULL;
		if (!(seg->segtype = get_segtype_from_string(lv->vg->cmd, "raid0")))
			return_0;
		
	} else {
		struct dm_list meta_lvs;

		dm_list_init(&meta_lvs);

		if (!(seg->meta_areas = dm_pool_zalloc(lv->vg->vgmem, seg->area_count * sizeof(*seg->meta_areas))))
			return_0;

		if (!_alloc_rmeta_devs_for_lv(lv, &meta_lvs))
			return 0;

		/* Metadata LVs must be cleared before being added to the array */
		log_debug_metadata("Clearing newly allocated metadata LVs");
		if (!_clear_lvs(&meta_lvs)) {
			log_error("Failed to initialize metadata LVs");
			return 0;
		}

		/* Set segment areas for metadata sub_lvs */
		if (!_add_sublvs_to_lv(lv, 1, 0, &meta_lvs, 0))
			return 0;

		if (!(seg->segtype = get_segtype_from_string(lv->vg->cmd, "raid0_meta")))
			return_0;
	}

	if (update_and_reload &&
	    !lv_update_and_reload(lv))
		return_0;

	/* If any residual LVs, eliminate them, write VG, commit it and take a backup */
	return dm_list_empty(&removal_mlvs) ? 1 : _eliminate_extracted_lvs(lv->vg, &removal_mlvs);
}

/*
 * lv_raid_change_image_count
 * @lv
 * @new_count: The absolute count of images (e.g. '2' for a 2-way mirror)
 * @pvs: The list of PVs that are candidates for removal (or empty list)
 *
 * RAID arrays have 'images' which are composed of two parts, they are:
 *    - 'rimage': The data/parity holding portion
 *    - 'rmeta' : The metadata holding portion (i.e. superblock/bitmap area)
 * This function adds or removes _both_ portions of the image and commits
 * the results.
 *
 * Returns: 1 on success, 0 on failure
 */
static int _lv_raid_change_image_count(struct logical_volume *lv,
				       const struct segment_type *segtype,
				       uint32_t new_count, struct dm_list *pvs)
{
	uint32_t old_count = lv_raid_image_count(lv);

	if (old_count == new_count) {
		log_warn("%s/%s already has image count of %d.",
			 lv->vg->name, lv->name, new_count);
		return 1;
	}

	segtype = segtype ?: first_seg(lv)->segtype;
PFLA("segtype=%s old_count=%u new_count=%u", segtype->name, old_count, new_count);

	/* Check for maximum supported raid devices */
	if (!_check_maximum_devices(new_count))
		return 0;
	/*
	 * LV must be either in-active or exclusively active
	 */
	if (lv_is_active(lv) &&
	    vg_is_clustered(lv->vg) &&
	    !lv_is_active_exclusive_locally(lv)) {
		log_error("%s/%s must be active exclusive locally to"
			  " perform this operation.", lv->vg->name, lv->name);
		return 0;
	}

	return (old_count > new_count ? _raid_remove_images : _raid_add_images)(lv, segtype, new_count, pvs);
}

int lv_raid_change_image_count(struct logical_volume *lv,
			       uint32_t new_count, struct dm_list *pvs)
{
	return _lv_raid_change_image_count(lv, NULL, new_count, pvs);
}

int lv_raid_split(struct logical_volume *lv, const char *split_name,
		  uint32_t new_count, struct dm_list *splittable_pvs)
{
	struct lv_list *lvl;
	struct dm_list removal_list, data_list;
	struct cmd_context *cmd = lv->vg->cmd;
	struct logical_volume *tracking;
	struct dm_list tracking_pvs;

	dm_list_init(&removal_list);
	dm_list_init(&data_list);

	if (!new_count) {
		log_error("Unable to split all images from %s/%s",
			  lv->vg->name, lv->name);
		return 0;
	}

	if (!seg_is_mirrored(first_seg(lv)) ||
	    segtype_is_raid10(first_seg(lv)->segtype)) {
		log_error("Unable to split logical volume of segment type, %s",
			  lvseg_name(first_seg(lv)));
		return 0;
	}

	if (vg_is_clustered(lv->vg) && !lv_is_active_exclusive_locally(lv)) {
		log_error("%s/%s must be active exclusive locally to"
			  " perform this operation.", lv->vg->name, lv->name);
		return 0;
	}

	if (find_lv_in_vg(lv->vg, split_name)) {
		log_error("Logical Volume \"%s\" already exists in %s",
			  split_name, lv->vg->name);
		return 0;
	}

	if (!_raid_in_sync(lv)) {
		log_error("Unable to split %s/%s while it is not in-sync.",
			  lv->vg->name, lv->name);
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
				  &removal_list, &data_list)) {
		log_error("Failed to extract images from %s/%s",
			  lv->vg->name, lv->name);
		return 0;
	}


	/* Convert to linear? */
	if (new_count == 1 && !_raid_remove_top_layer(lv, &removal_list)) {
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

	dm_list_iterate_items(lvl, &removal_list)
		if (!activate_lv_excl_local(cmd, lvl->lv))
			return_0;

	if (!resume_lv(lv->vg->cmd, lv_lock_holder(lv))) {
		log_error("Failed to resume %s/%s after committing changes",
			  lv->vg->name, lv->name);
		return 0;
	}

	return _eliminate_extracted_lvs(lv->vg, &removal_list);
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
		log_error("Unable to split image from %s/%s while not in-sync",
			  lv->vg->name, lv->name);
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

	log_print_unless_silent("Use 'lvconvert --merge %s/%s' to merge back into %s",
				lv->vg->name, seg_lv(seg, s)->name, lv->name);
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

	log_print_unless_silent("%s/%s successfully merged back into %s/%s",
				vg->name, image_lv->name, vg->name, lv->name);
	return 1;
}

/*
 * Convert @lv with "mirror" mapping to "raid1".
 *
 * Returns: 1 on success, 0 on failure
 */
static int _convert_mirror_to_raid1(struct logical_volume *lv,
				    const struct segment_type *new_segtype)
{
	uint32_t s;
	struct lv_segment *seg = first_seg(lv);
	struct lv_list lvl_array[seg->area_count], *lvl;
	struct dm_list meta_lvs;
	struct lv_segment_area *meta_areas;
	char *new_name;

	dm_list_init(&meta_lvs);

	if (!(meta_areas = dm_pool_zalloc(lv->vg->vgmem,
					  lv_mirror_count(lv) * sizeof(*meta_areas)))) {
		log_error("Failed to allocate meta areas memory.");
		return 0;
	}
#if 0
	/* HM FIXME: has been called in lvconvert already */
	if (!archive(lv->vg))
		return_0;
#endif
	for (s = 0; s < seg->area_count; s++) {
		log_debug_metadata("Allocating new metadata LV for %s",
				   seg_lv(seg, s)->name);
		if (!_alloc_rmeta_for_lv(seg_lv(seg, s), &(lvl_array[s].lv))) {
			log_error("Failed to allocate metadata LV for %s in %s",
				  seg_lv(seg, s)->name, lv->name);
			return 0;
		}

		dm_list_add(&meta_lvs, &(lvl_array[s].list));
	}

	log_debug_metadata("Clearing newly allocated metadata LVs");
	if (!_clear_lvs(&meta_lvs)) {
		log_error("Failed to initialize metadata LVs");
		return 0;
	}

	if (seg->log_lv) {
		log_debug_metadata("Removing mirror log, %s", seg->log_lv->name);
		if (!remove_mirror_log(lv->vg->cmd, lv, NULL, 0)) {
			log_error("Failed to remove mirror log");
			return 0;
		}
	}

	seg->meta_areas = meta_areas;
	s = 0;

	dm_list_iterate_items(lvl, &meta_lvs) {
		log_debug_metadata("Adding %s to %s", lvl->lv->name, lv->name);

		/* Images are known to be in-sync */
		lvl->lv->status &= ~LV_REBUILD;
		first_seg(lvl->lv)->status &= ~LV_REBUILD;
		lv_set_hidden(lvl->lv);

		if (!set_lv_segment_area_lv(seg, s, lvl->lv, 0,
					    lvl->lv->status)) {
			log_error("Failed to add %s to %s",
				  lvl->lv->name, lv->name);
			return 0;
		}

		s++;
	}

	for (s = 0; s < seg->area_count; ++s) {
		if (!(new_name = _generate_raid_name(lv, "rimage", s)))
			return_0;
		log_debug_metadata("Renaming %s to %s", seg_lv(seg, s)->name, new_name);
		seg_lv(seg, s)->name = new_name;
		seg_lv(seg, s)->status &= ~MIRROR_IMAGE;
		seg_lv(seg, s)->status |= RAID_IMAGE;
	}

	init_mirror_in_sync(1);

	log_debug_metadata("Setting new segtype for %s", lv->name);
	seg->segtype = new_segtype;
	lv->status &= ~MIRROR;
	lv->status &= ~MIRRORED;
	lv->status |= RAID;
	seg->status |= RAID;

	if (!lv_update_and_reload(lv))
		return_0;

	return 1;
}

/*
 * Convert @lv with "raid1" mapping to "mirror".
 *
 * Returns: 1 on success, 0 on failure
 */
static int _convert_raid1_to_mirror(struct logical_volume *lv,
				    const struct segment_type *new_segtype,
				    struct dm_list *allocatable_pvs)
{
	uint32_t s;
	uint32_t image_count = lv_raid_image_count(lv);
	char *new_name;
	struct lv_segment *seg = first_seg(lv);
	struct dm_list removal_mlvs;
	struct lv_list *lvl_array;

	if (image_count > DEFAULT_MIRROR_MAX_IMAGES) {
		log_error("Unable to convert mirror LV %s/%s with %u images",
			  lv->vg->name, lv->name, image_count);
		log_error("Please reduce to the maximum of %u images with \"lvconvert -m%u %s/%s\"",
			  DEFAULT_MIRROR_MAX_IMAGES, DEFAULT_MIRROR_MAX_IMAGES - 1, lv->vg->name, lv->name);
		return 0;
	}

	dm_list_init(&removal_mlvs);

	/* Allocate for number of metadata LVs */
	if (!(lvl_array = dm_pool_alloc(lv->vg->vgmem, seg->area_count * sizeof(*lvl_array))))
		return_0;

	/* Remove rmeta LVs */
	if (seg->meta_areas) {
		for (s = 0; s < seg->area_count; s++) {
			struct logical_volume *mlv = lvl_array[s].lv = seg_metalv(seg, s);

			dm_list_add(&removal_mlvs, &lvl_array[s].list);
			if (!(new_name = _generate_raid_name(mlv, "extracted", -1)))
				return_0;
			log_debug_metadata("Extracting and Renaming %s to %s", mlv->name, new_name);
			if (!_remove_and_set_error_target(mlv, seg))
				return 0;

			mlv->name = new_name;
		}

		seg->meta_areas = NULL;
	}

	/* Add mirrored mirror_log LVs */
	if (!add_mirror_log(lv->vg->cmd, lv, 1, seg->region_size, allocatable_pvs, lv->vg->alloc)) {
		log_error("Unable to add mirror log to %s/%s", lv->vg->name, lv->name);
		return 0;
	}

	for (s = 0; s < seg->area_count; ++s) {
		struct logical_volume *dlv = seg_lv(seg, s);

		if (!(new_name = _generate_raid_name(lv, "mimage", s)))
			return_0;
		log_debug_metadata("Renaming %s to %s", dlv->name, new_name);
		dlv->name = new_name;
		dlv->status &= ~RAID_IMAGE;
		dlv->status |= MIRROR_IMAGE;
	}


	log_debug_metadata("Setting new segtype %s for %s", new_segtype->name, lv->name);
	seg->segtype = new_segtype;
	lv->status |= (MIRROR | MIRRORED);
	lv->status &= ~RAID;
	seg->status &= ~RAID;

	if (!lv_update_and_reload(lv))
		return_0;

	/* Eliminate the residual LVs, write VG, commit it and take a backup */
	return _eliminate_extracted_lvs(lv->vg, &removal_mlvs);
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

	if (!(segtype = get_segtype_from_string(lv->vg->cmd, "striped")))
		return_0;

	dm_list_iterate(l, new_data_lvs) {
		dlv = (dm_list_item(l, struct lv_list))->lv;

		le = 0;
		dm_list_iterate_items(seg_from, &lv->segments) {
			uint64_t status = RAID | SEG_RAID | (seg_from->status & (LVM_READ | LVM_WRITE));

			/* Allocate a segment with one area for each segment in the striped LV */
			if (!(seg_new = alloc_lv_segment(segtype, dlv,
							 le, seg_from->area_len, status,
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
	struct segment_type *segtype = get_segtype_from_string(lv->vg->cmd, SEG_TYPE_NAME_RAID0);

PFLA("seg->stripe_size=%u seg->chunk_size=%u", seg->stripe_size, seg->chunk_size);

	/* Allocate single segment to hold the image component areas */
	if (!(seg_new = alloc_lv_segment(segtype, lv,
					 0 /* le */, lv->le_count /* len */,
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
static int _convert_striped_to_raid0(struct logical_volume *lv,
				     int alloc_metadata_devs,
				     int update_and_reload)
{
	struct lv_segment *data_lv_seg, *seg = first_seg(lv);
	struct dm_list new_meta_lvs;
	struct dm_list new_data_lvs;
	struct dm_list *l;
	unsigned area_count = seg->area_count;

	if (!seg_is_striped(seg)) {
		log_error("Cannot convert non-striped LV %s/%s to raid0", lv->vg->name, lv->name);
		return 0;
	}

	/* Check for not yet supported varying area_count on multi-segment striped LVs */
	if (!_lv_has_one_stripe_zone(lv)) {
		log_error("Cannot convert striped LV %s/%s with varying stripe count to raid0",
			  lv->vg->name, lv->name);
		return 0;
	}

	dm_list_init(&new_meta_lvs);
	dm_list_init(&new_data_lvs);

	/* FIXME: insert_layer_for_lv() not suitable */
	/* Allocate empty rimage components in order to be able to support multi-segment "striped" LVs */
	if (!_alloc_image_components(lv, 0, NULL, area_count, NULL, &new_data_lvs)) {
		log_error("Failed to allocate empty image components for raid0 LV %s/%s.", lv->vg->name, lv->name);
		return_0;
	}

	/* Image components are being allocated with LV_REBUILD preset and raid0 does not need it */
	dm_list_iterate(l, &new_data_lvs)
		(dm_list_item(l, struct lv_list))->lv->status &= ~LV_REBUILD;

	/* Move the AREA_PV areas across to the new rimage components */
	if (!_striped_to_raid0_move_segs_to_raid0_lvs(lv, &new_data_lvs)) {
		log_error("Failed to insert linear LVs underneath %s/%s.", lv->vg->name, lv->name);
		return_0;
	}

	/* Allocate new top-level LV segment using credentials of first ne data lv for stripe_size... */
	data_lv_seg = first_seg(dm_list_item(dm_list_first(&new_data_lvs), struct lv_list)->lv);
	if (!_striped_to_raid0_alloc_raid0_segment(lv, area_count, data_lv_seg)) {
		log_error("Failed to allocate new raid0 segement for LV %s/%s.", lv->vg->name, lv->name);
		return_0;
	}

	/* Add data lvs to the top-level lvs segment */
	if (!_add_sublvs_to_lv(lv, 1, 0, &new_data_lvs, 0))
		return 0;


	/* Get reference to new allocated raid0 segment */
	seg = first_seg(lv);
	seg->segtype = get_segtype_from_string(lv->vg->cmd, "raid0");
	lv->status |= RAID;

	/* Allocate metadata lvs if requested */
	if (alloc_metadata_devs) {
		if (!_raid0_add_or_remove_metadata_lvs(lv, update_and_reload))
			return 0;

	} else if (update_and_reload &&
		   !lv_update_and_reload(lv))
			return 0;

	return 1;
}
/* END: striped -> raid0 conversion */


/* BEGIN: raid0 -> striped conversion */

/*
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

/* Return segment of @lv for logical extent @le */
static struct lv_segment *_seg_by_le(struct logical_volume *lv, unsigned le)
{
	struct lv_segment *seg;

	dm_list_iterate_items(seg, &lv->segments)
		if (le >= seg->le && le < seg->le + seg->len)
			return seg;

	return NULL;
}

/*
 * HM
 *
 * All areas from @lv image component LV's segments are
 * being moved to @new_segments allocated.
 * The metadata+data component LVs are being mapped to an
 * error target and linked to @removal_lvs
 *
 * Returns: 1 on success, 0 on failure
 */
static int _raid0_to_striped_retrieve_segments_and_lvs(struct logical_volume *lv,
						       struct dm_list *removal_lvs)
{
	uint32_t s, area_le = 0, area_len, l, le = 0;
	struct lv_segment *seg = first_seg(lv), *seg_from, *seg_to;
	struct logical_volume *mlv, *dlv;
	struct dm_list new_segments;
	struct lv_list *lvl_array;
	struct segment_type *striped_segtype;

	if (!(striped_segtype = get_segtype_from_string(lv->vg->cmd, "striped")))
		return_0;

	dm_list_init(&new_segments);

	if (!(lvl_array = dm_pool_alloc(lv->vg->vgmem, 2 * seg->area_count * sizeof(*lvl_array))))
		return_0;
	/*
	 * Walk all segments of all data LVs to create the number
	 * of segments we need and move mappings across,
	 */
	while (le < seg->len) {
		area_len = ~0;

		/* Find shortest length of the segments of each of the data image lvs */
		for (s = 0; s < seg->area_count; s++) {
			dlv = seg_lv(seg, s);
			seg_from = _seg_by_le(dlv, area_le);

			l = seg_from->len - (area_le - seg_from->le);
			if (l < area_len)
				area_len = l;
		}

		/* Allocate a segment with area_count areas */
		if (!(seg_to = alloc_lv_segment(striped_segtype, lv, le, area_len * seg->area_count,
						seg->status & ~RAID,
						seg->stripe_size, NULL, seg->area_count,
						area_len, seg->chunk_size,
						0, 0, NULL)))
			return_0;

		dm_list_add(&new_segments, &seg_to->list);

		area_le += area_len;
		le += area_len * seg->area_count;
	}

	dm_list_iterate_back_items(seg_to, &new_segments) {
		area_le -= seg_to->area_len;

		for (s = 0; s < seg->area_count; s++) {
			dlv = seg_lv(seg, s);
			seg_from = _seg_by_le(dlv, area_le);

			if (!_raid_move_partial_lv_segment_area(seg_to, s, seg_from, 0, seg_to->area_len))
				return 0;
		}
	}

	/* Loop the areas listing the image LVs and move all areas across from them to @new_segments */
	for (s = 0; s < seg->area_count; s++) {
		/* If any metadata lvs -> remove them */
		if (seg->meta_areas &&
		    (mlv = lvl_array[seg->area_count + s].lv = seg_metalv(seg, s))) {
			dm_list_add(removal_lvs, &lvl_array[seg->area_count + s].list);
			if (!_remove_and_set_error_target(mlv, seg))
				return 0;
		}

		/* Walk the segment list and move the respective area across to the corresponding new segment */
		dlv = lvl_array[s].lv = seg_lv(seg, s);
		dm_list_add(removal_lvs, &lvl_array[s].list);

		if (!_remove_and_set_error_target(dlv, seg))
			return 0;
	}

	/*
	 * Remove the one segment holding the image component areas
	 * from the top-level LV and add the new segments to it
	 */
	dm_list_del(&seg->list);
	dm_list_splice(&lv->segments, &new_segments);

	lv->status &= RAID;
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
 */
static int _convert_raid0_to_striped(struct logical_volume *lv,
				     const struct segment_type *new_segtype)
{
	struct lv_segment *seg = first_seg(lv);
	struct dm_list removal_lvs;

	dm_list_init(&removal_lvs);

	/* Caller should ensure, but... */
	if (!seg_is_any_raid0(seg) ||
	    !segtype_is_striped(new_segtype))
		return 0;

	/* Move the AREA_PV areas across to new top-level segments */
	if (!_raid0_to_striped_retrieve_segments_and_lvs(lv, &removal_lvs)) {
		log_error("Failed to retrieve raid0 segments from %s.", lv->name);
		return_0;
	}

	if (!lv_update_and_reload(lv))
		return_0;

	/* Eliminate the residual LVs, write VG, commit it and take a backup */
	return _eliminate_extracted_lvs(lv->vg, &removal_lvs);
}
/* END: raid0 -> striped conversion */

/* BEGIN: raid <-> raid conversion */
/*
 * Reshape logical volume @lv by adding/removing stripes
 * (absolute new stripes given in @new_stripes), changing
 * stripe size set in @new_stripe_size.
 * Any PVs listed in @allocate_pvs will be tried for
 * allocation of new stripes.
 */
/* HM FIXME: CODEME TESTME */

/*
 * Compares current raid disk count of active RAID set to requested @dev_count
 *
 * Returns:
 *
 * 	0: error
 * 	1: active dev count = @dev_count
 * 	2: active dev count < @dev_count
 * 	3: active dev count > @dev_count
 *
 */
static int _reshaped_state(struct logical_volume *lv, const unsigned dev_count, unsigned *devs_synced)
{
	char *raid_health;
	unsigned d, devs;

	if (!lv_raid_dev_health(lv, &raid_health))
		return_0;

	devs = (unsigned) strlen(raid_health);
	*devs_synced = 0;

	for (d = 0; d < devs; d++)
		if (raid_health[d] == 'A')
			(*devs_synced)++;

	if (*devs_synced == dev_count)
		return 1;

	return *devs_synced < dev_count ? 2 : 3;
}

static int _lv_optionally_extend_reshape_space(struct logical_volume *lv,
					       int extend_upfront, struct dm_list *allocate_pvs)
{
	struct lv_segment *seg = first_seg(lv);

#if 1
	return 1;
#endif

	if (!lv_extend(lv, seg->segtype,
			_data_rimages_count(seg, seg->area_count),
			seg->stripe_size,
			1, seg->region_size,
			seg->area_count /* # of reshape LEs to add */,
			extend_upfront /* 0 = normal extend at end / 1 = extend at the beginning */,
			allocate_pvs, lv->alloc, 0))
		return 0;

	return 1;
}

static int _convert_reshape(struct logical_volume *lv,
			    const struct segment_type *new_segtype,
			    int yes, int force,
			    const unsigned new_stripes,
			    const unsigned new_stripe_size,
		 	    struct dm_list *allocate_pvs)
{
	int update_and_reload = 1;
	int reset_flags = 0;
	int too_few = 0;
	uint32_t new_len;
	struct lv_segment *seg = first_seg(lv);
	unsigned old_dev_count = seg->area_count;
	unsigned new_dev_count = new_stripes + seg->segtype->parity_devs;
	unsigned devs_synced;
	struct lvinfo info = { 0 };

PFLA("old_dev_count=%u new_dev_count=%u", old_dev_count, new_dev_count);
	if (seg->segtype == new_segtype &&
	    old_dev_count == new_dev_count &&
	    seg->stripe_size == new_stripe_size) {
		log_error("Nothing to do");
		return 0;
	}

	if (segtype_is_any_raid0(new_segtype) &&
	    (old_dev_count != new_dev_count || seg->stripe_size != new_stripe_size)) {
		log_error("Can't reshape raid0");
		log_error("You may want to convert to raid4/5/6 first");
		return 0;
	}

	/* raid5 with 3 image component pairs (i.e. 2 stripes): allow for raid5 reshape to 2 devices, i.e. raid1 layout */
	if (seg_is_raid4(seg) || seg_is_any_raid5(seg)) {
		if (new_stripes < 1)
			too_few = 1;

	/* any other raid4/5/6 device count: check for 2 stripes minimum */
	} else if (new_stripes < 2)
		too_few = 1;

	if (too_few) {
		log_error("Too few stripes requested");
		return 0;
	}

	seg->stripe_size = new_stripe_size;

	/* Handle disk addition reshaping */
	if (old_dev_count < new_dev_count) {
PFL();
		switch (_reshaped_state(lv, old_dev_count, &devs_synced)) {
		case 0:
PFL();
			/* Status retrieve error (e.g. raid set not activated) -> can't proceed */
			return 0;
		case 1:
			/* device count is good -> ready to add disks */
			break;
		case 2:
			log_error("Device count is incorrrect. "
				  "Forgotten \"lvconvert --stripes %d %s/%s\" to remove %u images after reshape?",
				  devs_synced - seg->segtype->parity_devs, lv->vg->name, lv->name,
				  old_dev_count - devs_synced);
			return 0;

		case 3:
			return 0;

		default:
			log_error(INTERNAL_ERROR "Bad return provided to %s.", __func__);
			return 0;
		}

		if (old_dev_count == 2)
			new_segtype = seg->segtype;

		if (!lv_info(lv->vg->cmd, lv, 0, &info, 1, 0) && driver_version(NULL, 0)) {
			log_error("lv_info failed: aborting");
			return 0;
		}

		new_len = _data_rimages_count(seg, new_dev_count) * (seg->len / _data_rimages_count(seg, seg->area_count));
		log_warn("WARNING: Adding stripes to active%s logical volume %s/%s will grow "
			 "it from %u to %u extents!\n"
			 "You may want to run \"lvresize -y -l%u %s/%s\" after the conversion has finished\n"
			 "or make use of the gained capacity\n",
			 info.open_count ? " and open" : "",
			 lv->vg->name, lv->name, seg->len, new_len,
			 new_len, lv->vg->name, lv->name);
		if (!yes && yes_no_prompt("WARNING: Do you really want to add %u stripes to %s/%s extending it? [y/n]: ",
					  new_dev_count - old_dev_count,  lv->vg->name, lv->name) == 'n') {
			log_error("Logical volume %s/%s NOT converted to extend", lv->vg->name, lv->name);
			return 0;
		}
		if (sigint_caught())
			return_0;

		/*
		 * HM FIXME: check if there's enough free space for forward
		 * 	     out-of-place reshape
		 *
		 * If none, add an extent per image at the beginning and pass
		 * data_offset = extent_size to the kernel
		 */
		/* Do it here! */
		if (!_lv_optionally_extend_reshape_space(lv, 1 /* extend at BEGINNING */, allocate_pvs))
			return 0;


		if (!_lv_raid_change_image_count(lv, new_segtype, new_dev_count, allocate_pvs))
			return 0;

		update_and_reload = 0;

		if (seg->segtype != new_segtype)
			log_warn("Ignoring layout change on device adding reshape");

	/*
 	 * HM FIXME: I don't like the flow doing this here and in _raid_add_images on addition
	 */

	/* Handle disk removal reshaping */
	} else if (old_dev_count > new_dev_count) {
		uint32_t s;

		switch (_reshaped_state(lv, new_dev_count, &devs_synced)) {
		case 0:
PFL();
			/* Status retrieve error (e.g. raid set not activated) -> can't proceed */
			return 0;

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
			if (_reshaped_state(lv, old_dev_count, &devs_synced) == 2) {
				log_error("Device count is incorrrect. "
					  "Forgotten \"lvconvert --stripes %d %s/%s\" to remove %u images after reshape?",
					  devs_synced - seg->segtype->parity_devs, lv->vg->name, lv->name,
					  old_dev_count - devs_synced);
				return 0;
			}

			if (!lv_info(lv->vg->cmd, lv, 0, &info, 1, 0) && driver_version(NULL, 0)) {
				log_error("lv_info failed: aborting");
				return 0;
			}

			new_len = _data_rimages_count(seg, new_dev_count) *
				  (seg->len / _data_rimages_count(seg, seg->area_count));
			log_warn("WARNING: Removing stripes from active%s logical volume %s/%s will shrink "
				 "it from %u to %u extents!\n"
				 "THIS MAY DESTROY (PARTS OF YOUR DATA!\n"
				 "You may want to run \"lvresize -y -l+%u %s/%s\" _before_ the conversion starts!\n"
				 "If that leaves the logical volume larger than %u extents, grow the filesystem etc. as well\n",
				 info.open_count ? " and open" : "",
				 lv->vg->name, lv->name, seg->len, new_len,
				 seg->len - (new_dev_count == 2 ? 0 : new_len), lv->vg->name, lv->name, new_len);
			if (!yes && yes_no_prompt("Do you really want to remove %u stripes from %s/%s? [y/n]: ",
						  old_dev_count - new_dev_count,  lv->vg->name, lv->name) == 'n') {
				log_error("Logical volume %s/%s NOT converted to reduce", lv->vg->name, lv->name);
				return 0;
			}
			if (sigint_caught())
				return_0;

			/*
			 * HM FIXME: check if there's enough free space for backward
			 * 	     out-of-place reshape
			 *
			 * If none, add an extent per image to the end of each data image 
			 */
			/* Do it here! */
			if (!_lv_optionally_extend_reshape_space(lv, 0 /* extend at end */, allocate_pvs))
				return 0;


			for (s = new_dev_count; s < old_dev_count; s++)
				seg_lv(seg, s)->status |= LV_RESHAPE_DELTA_DISKS_MINUS;

			update_and_reload = 1;
			reset_flags = 1;

			if (seg->segtype != new_segtype)
				log_warn("Ignoring layout change on reshape");

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
			if (!_lv_raid_change_image_count(lv, new_segtype, new_dev_count, allocate_pvs))
				return 0;

			if (!vg_write(lv->vg) || !vg_commit(lv->vg)) {
				log_error("Failed to write reshaped %s/%s", lv->vg->name, lv->name);
				return 0;
			}

			backup(lv->vg);

			update_and_reload = 0;
			reset_flags = 0;
			break;

		default:
PFL();
			log_error(INTERNAL_ERROR "Bad return provided to %s.", __func__);
			return 0;
		}

	} else {
		if ((seg_is_raid5_n(seg) && segtype_is_any_raid5(new_segtype)) ||
		    (seg_is_raid6_n_6(seg) && segtype_is_any_raid6(new_segtype))) {
			if (!yes && yes_no_prompt("Do you really want to convert %s/%s from %s to %s? [y/n]: ",
						  lv->vg->name, lv->name, seg->segtype->name, new_segtype->name) == 'n') {
				log_error("Logical volume %s/%s NOT converted", lv->vg->name, lv->name);
				return 0;
			}
			if (sigint_caught())
				return_0;
		}

		seg->segtype = new_segtype;
	}

PFLA("new_segtype=%s seg->area_count=%u", new_segtype->name, seg->area_count);

	if (update_and_reload) {
		if (!lv_update_and_reload(lv))
			return_0;
PFL();
		/* HM FIXME: i don't like the flow doing this here and in _raid_add_images on addition of component images */
		/*
	 	 * Now that the 'RESHAPE_DELTA_DISKS_MINUS' has made its way to
	 	 * the kernel, we must remove the flag so that the individual devices
	 	 * are not reshaped upon every activation.
	 	 */
		if (reset_flags && !_reset_flags_passed_to_kernel(lv))
			return 0;
PFL();
	}

	return 1;
}

/* Process one level up takeover on @lv to @segtype allocating fron @allocate_pvs */
static int _raid_takeover(struct logical_volume *lv,
			  int up,
			  const struct segment_type *segtype,
			  struct dm_list *allocate_pvs,
			  const char *error_msg)
{
	struct lv_segment *seg = first_seg(lv);
	uint32_t new_count = seg->area_count + segtype->parity_devs - seg->segtype->parity_devs;

	/* Make sure to set default region size on takeover from raid0 */
	_check_and_init_region_size(lv);

PFLA("segtype=%s old_count=%u new_count=%u", segtype->name, seg->area_count, new_count);
	/* Takeover raid4* <-> raid5* */
	if (new_count == seg->area_count) {
PFL();
		if ((segtype_is_raid5_n(seg->segtype) && segtype_is_raid4(segtype)) ||
		    (segtype_is_raid4(seg->segtype)   && segtype_is_raid5_n(segtype))) {
			seg->segtype = segtype;

			if (!lv_update_and_reload(lv))
				return_0;

			return 1;
		}

		return 0;
	}

	/*
	 * In case of raid1 -> raid5, takeover will run a degraded 2 disk raid5 set with the same content
	 * in each leg which will get an additional disk allocated afterwards and reloaded starting
	 * resynchronization to reach full redundance.
	 *
	 * FIXME: 2 step process to a) take over a 2 legged raid1 mapping to raid5 and b) reshape it to add at least one disk
	 */
	if ((seg_is_raid1(seg) && segtype_is_any_raid5(segtype)) ||
            (seg_is_any_raid5(seg) && segtype_is_raid1(segtype))) {
PFL();
		if (seg->area_count == 2) {
PFL();
			seg->segtype = segtype;
			if (!seg->stripe_size)
				/* raid1 does not preset stripe size */
				seg->stripe_size = 64 * 2;
PFL();
			if (!lv_update_and_reload_origin(lv))
				return_0;

PFL();
			return 1;
		}
PFL();
		log_error(error_msg, lv->vg->name, lv->name);
		return 0;
	}

	/*
	 * The top-level LV is being reloaded and the VG
	 * written and committed in the course of this call
	 */
	if (!_lv_raid_change_image_count(lv, segtype, new_count, allocate_pvs))
		return_0;

	return 1;
}

static int _raid_level_up(struct logical_volume *lv,
			  const struct segment_type *segtype,
			  struct dm_list *allocate_pvs)
{
	return _raid_takeover(lv, 1, segtype, allocate_pvs,
			      "raid1 set %s/%s has to have 2 operational disks.");
}

	/* Process one level down takeover on @lv to @segtype */
static int _raid_level_down(struct logical_volume *lv,
			    const struct segment_type *segtype,
			    struct dm_list *allocate_pvs)
{
	return _raid_takeover(lv, 0, segtype, allocate_pvs,
			      "raid4/5 set %s/%s has to have 1 stripe. Use \"lvconvert --stripes 1 ...\"");
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
 *  raid0 - providing it has only two drives
 *
 * raid4 can take over:
 *  raid0 - if there is only one strip zone
 *  raid5 - if layout is right
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

#define	ARRAY_SIZE(a) (sizeof(a) / sizeof(*a))
struct possible_type {
	const char *current_type;
	const char *possible_types[13];
};
static const struct segment_type *_adjust_segtype(struct logical_volume *lv,
						  const struct segment_type *segtype,
						  const struct segment_type *new_segtype)
{
	unsigned cn, pn;
	struct possible_type pt[] = {
		{ .current_type = SEG_TYPE_NAME_LINEAR,
		  .possible_types = { SEG_TYPE_NAME_RAID1,
				      SEG_TYPE_NAME_RAID4,
				      SEG_TYPE_NAME_RAID5_N, NULL } },
		{ .current_type = SEG_TYPE_NAME_STRIPED,
		  .possible_types = { SEG_TYPE_NAME_RAID0, SEG_TYPE_NAME_RAID0_META,
				      SEG_TYPE_NAME_RAID4,
				      SEG_TYPE_NAME_RAID5_N, SEG_TYPE_NAME_RAID6_N_6, NULL } },
		{ .current_type = SEG_TYPE_NAME_RAID0,
		  .possible_types = { SEG_TYPE_NAME_STRIPED,
				      SEG_TYPE_NAME_RAID4,
				      SEG_TYPE_NAME_RAID5_N, SEG_TYPE_NAME_RAID6_N_6,
				      SEG_TYPE_NAME_RAID10, NULL } },
		{ .current_type = SEG_TYPE_NAME_RAID0_META,
		  .possible_types = { SEG_TYPE_NAME_STRIPED,
				      SEG_TYPE_NAME_RAID4,
				      SEG_TYPE_NAME_RAID5_N, SEG_TYPE_NAME_RAID6_N_6,
				      SEG_TYPE_NAME_RAID10, NULL } },
		{ .current_type = SEG_TYPE_NAME_RAID1,
		  .possible_types = { SEG_TYPE_NAME_RAID5_N, NULL } },
		{ .current_type = SEG_TYPE_NAME_RAID4,
		  .possible_types = { SEG_TYPE_NAME_STRIPED,
				      SEG_TYPE_NAME_RAID0, SEG_TYPE_NAME_RAID0_META,
				      SEG_TYPE_NAME_RAID1,
				      SEG_TYPE_NAME_RAID5_N,
				      SEG_TYPE_NAME_RAID6_N_6,  NULL } },
		{ .current_type = SEG_TYPE_NAME_RAID5,
		  .possible_types = { SEG_TYPE_NAME_RAID1,
				      SEG_TYPE_NAME_RAID5_0,  SEG_TYPE_NAME_RAID5_N,
				      SEG_TYPE_NAME_RAID5_LS, SEG_TYPE_NAME_RAID5_RS,
				      SEG_TYPE_NAME_RAID5_LA, SEG_TYPE_NAME_RAID5_RA,
				      SEG_TYPE_NAME_RAID6_LS_6, NULL } },
		{ .current_type = SEG_TYPE_NAME_RAID5_LS,
		  .possible_types = { SEG_TYPE_NAME_RAID1,
				      SEG_TYPE_NAME_RAID5,    SEG_TYPE_NAME_RAID5_0,  SEG_TYPE_NAME_RAID5_N,
							      SEG_TYPE_NAME_RAID5_RS,
				      SEG_TYPE_NAME_RAID5_LA, SEG_TYPE_NAME_RAID5_RA,
		                      SEG_TYPE_NAME_RAID6_LS_6, NULL } },
		{ .current_type = SEG_TYPE_NAME_RAID5_RS,
		  .possible_types = { SEG_TYPE_NAME_RAID1,
		  		      SEG_TYPE_NAME_RAID5,     SEG_TYPE_NAME_RAID5_0,  SEG_TYPE_NAME_RAID5_N,
				      SEG_TYPE_NAME_RAID5_LS, 
				      SEG_TYPE_NAME_RAID5_LA,  SEG_TYPE_NAME_RAID5_RA,
		                      SEG_TYPE_NAME_RAID6_RS_6, NULL } },
		{ .current_type = SEG_TYPE_NAME_RAID5_LA,
		  .possible_types = { SEG_TYPE_NAME_RAID1,
				      SEG_TYPE_NAME_RAID5,    SEG_TYPE_NAME_RAID5_0,  SEG_TYPE_NAME_RAID5_N,
				      SEG_TYPE_NAME_RAID5_LS, SEG_TYPE_NAME_RAID5_RS,
							      SEG_TYPE_NAME_RAID5_RA,
		                      SEG_TYPE_NAME_RAID6_LA_6, NULL } },
		{ .current_type = SEG_TYPE_NAME_RAID5_RA,
		  .possible_types = { SEG_TYPE_NAME_RAID1,
				      SEG_TYPE_NAME_RAID5,    SEG_TYPE_NAME_RAID5_0,  SEG_TYPE_NAME_RAID5_N,
				      SEG_TYPE_NAME_RAID5_LS, SEG_TYPE_NAME_RAID5_RS,
				      SEG_TYPE_NAME_RAID5_LA,
		                      SEG_TYPE_NAME_RAID6_RA_6, NULL } },
		{ .current_type = SEG_TYPE_NAME_RAID5_0,
		  .possible_types = { SEG_TYPE_NAME_RAID1,
				      SEG_TYPE_NAME_RAID4,
				      SEG_TYPE_NAME_RAID5,     SEG_TYPE_NAME_RAID5_N,
				      SEG_TYPE_NAME_RAID5_LS,  SEG_TYPE_NAME_RAID5_RS,
				      SEG_TYPE_NAME_RAID5_LA,  SEG_TYPE_NAME_RAID5_RA,
				      SEG_TYPE_NAME_RAID6_0_6, NULL } },
		{ .current_type = SEG_TYPE_NAME_RAID5_N,
		  .possible_types = { SEG_TYPE_NAME_STRIPED,
				      SEG_TYPE_NAME_RAID0,     SEG_TYPE_NAME_RAID0_META,
				      SEG_TYPE_NAME_RAID1,
				      SEG_TYPE_NAME_RAID4,
				      SEG_TYPE_NAME_RAID5,     SEG_TYPE_NAME_RAID5_0,
				      SEG_TYPE_NAME_RAID5_LS,  SEG_TYPE_NAME_RAID5_RS,
				      SEG_TYPE_NAME_RAID5_LA,  SEG_TYPE_NAME_RAID5_RA,
				      SEG_TYPE_NAME_RAID6_N_6, NULL } },
		{ .current_type = SEG_TYPE_NAME_RAID6_ZR,
		  .possible_types = { SEG_TYPE_NAME_RAID6_NC, SEG_TYPE_NAME_RAID6_NR,
				      SEG_TYPE_NAME_RAID6_N_6, NULL } },
		{ .current_type = SEG_TYPE_NAME_RAID6_NC,
		  .possible_types = { SEG_TYPE_NAME_RAID6_ZR, SEG_TYPE_NAME_RAID6_NR,
				      SEG_TYPE_NAME_RAID6_N_6, NULL } },
		{ .current_type = SEG_TYPE_NAME_RAID6_NR,
		  .possible_types = { SEG_TYPE_NAME_RAID6_ZR, SEG_TYPE_NAME_RAID6_NC,
				      SEG_TYPE_NAME_RAID6_N_6, NULL } },
		{ .current_type = SEG_TYPE_NAME_RAID6_N_6,
		  .possible_types = { SEG_TYPE_NAME_RAID6_ZR, SEG_TYPE_NAME_RAID6_NR,
				      SEG_TYPE_NAME_RAID6_NC, SEG_TYPE_NAME_RAID5_N,
				      SEG_TYPE_NAME_RAID0,    SEG_TYPE_NAME_RAID0_META,
				      SEG_TYPE_NAME_RAID4,
				      SEG_TYPE_NAME_STRIPED, NULL } },
		{ .current_type = SEG_TYPE_NAME_RAID6_LS_6,
		  .possible_types = { SEG_TYPE_NAME_RAID6_ZR, SEG_TYPE_NAME_RAID6_NR,
				      SEG_TYPE_NAME_RAID6_NC, SEG_TYPE_NAME_RAID5_LS, NULL } },
		{ .current_type = SEG_TYPE_NAME_RAID6_RS_6,
		  .possible_types = { SEG_TYPE_NAME_RAID6_ZR, SEG_TYPE_NAME_RAID6_NR,
				      SEG_TYPE_NAME_RAID6_NC, SEG_TYPE_NAME_RAID5_RS, NULL } },
		{ .current_type = SEG_TYPE_NAME_RAID6_LA_6,
		  .possible_types = { SEG_TYPE_NAME_RAID6_ZR, SEG_TYPE_NAME_RAID6_NR,
				      SEG_TYPE_NAME_RAID6_NC, SEG_TYPE_NAME_RAID5_LA, NULL } },
		{ .current_type = SEG_TYPE_NAME_RAID6_RA_6,
		  .possible_types = { SEG_TYPE_NAME_RAID6_ZR, SEG_TYPE_NAME_RAID6_NR,
				      SEG_TYPE_NAME_RAID6_NC, SEG_TYPE_NAME_RAID5_RA, NULL } },
		{ .current_type = SEG_TYPE_NAME_RAID10,
		  .possible_types = { SEG_TYPE_NAME_RAID0, SEG_TYPE_NAME_RAID0_META, NULL } },
	};

	for (cn = 0; cn < ARRAY_SIZE(pt); cn++) {
		if (!strcmp(segtype->name, pt[cn].current_type)) {
			for (pn = 0; pt[cn].possible_types[pn]; pn++)
				if (!strcmp(new_segtype->name, pt[cn].possible_types[pn]))
					return get_segtype_from_string(lv->vg->cmd, pt[cn].possible_types[pn]);

			for (pn = 0; pt[cn].possible_types[pn]; pn++)
				if (!strncmp(new_segtype->name, pt[cn].possible_types[pn], 5))
					return get_segtype_from_string(lv->vg->cmd, pt[cn].possible_types[pn]);
		}
	}

	return NULL;
}

/*
 * Convert a RAID set in @lv to another RAID level and algorithm defined
 * by @requested_segtype, stripe size set by @new_stripe_size or number
 * of RAID devices requested by @new_stripes.
 *
 * Returns: 1 on success, 0 on failure
 */
static int _convert_raid_to_raid(struct logical_volume *lv,
				 const struct segment_type *new_segtype,
				 const struct segment_type *final_segtype,
				 int yes, int force,
				 const unsigned new_stripes,
				 const unsigned new_stripe_size,
				 struct dm_list *allocate_pvs)
{
	struct lv_segment *seg = first_seg(lv);
	unsigned stripes = new_stripes ?: _data_rimages_count(seg, seg->area_count);
	unsigned stripe_size = new_stripe_size ?: seg->stripe_size;
	const struct segment_type *new_segtype_sav = new_segtype;

PFLA("seg->segtype=%s new_segtype->name=%s stripes=%u new_stripes=%u", seg->segtype->name, new_segtype->name, stripes, new_stripes);
	if (new_segtype == seg->segtype &&
	    stripes == _data_rimages_count(seg, seg->area_count) &&
	    stripe_size == seg->stripe_size) {
PFLA("stripes=%u stripe_size=%u seg->stripe_size=%u", stripes, stripe_size, seg->stripe_size);
		log_error("Nothing to do");
		return 0;
	}

	/* Check + apply stripe size change */
	if (stripe_size &&
	    (stripe_size & (stripe_size - 1) ||
	     stripe_size < 8)) {
		log_error("Invalid stripe size on %s", lv->name);
		return_0;
	}

	if (seg->stripe_size != stripe_size) {
		if (seg_is_striped(seg) || seg_is_any_raid0(seg)) {
			log_error("Cannot change stripe size on \"%s\"", lv->name);
			return_0;
		}

		if (stripe_size > lv->vg->extent_size) {
			log_error("Stripe size for %s too large for volume group extent size", lv->name);
			return_0;
		}

		if (stripe_size > seg->region_size) {
			log_error("New stripe size for %s is larger than region size", lv->name);
			return_0;
		}
	}

	/*
	 * Special case raid0 <-> raid0_meta adding metadata image
	 * devices on converting from raid0 -> raid0_meta or removing
	 * them going the other way.
	 */
	if ((seg_is_raid0(seg) && segtype_is_raid0_meta(new_segtype)) ||
	    (seg_is_raid0_meta(seg) && segtype_is_raid0(new_segtype)))
		return _raid0_add_or_remove_metadata_lvs(lv, 1);

	/*
	 * Staying on the same level -> reshape required to change
	 * stripes (i.e. # of disks), stripe size or algorithm
	 */
	if (is_same_level(seg->segtype, new_segtype)) {
PFLA("stripes=%u stripe_size=%u seg->stripe_size=%u", stripes, stripe_size, seg->stripe_size);
		return _convert_reshape(lv, new_segtype, yes, force, stripes, stripe_size, allocate_pvs);
	}

	/*
	 * HM
	 *
	 * Up/down takeover of raid levels
	 *
	 * In order to takeover the raid set level N to M (M > N) in @lv, all existing
	 * rimages in that set need to be paired with rmeta devs (if not yet present)
	 * to store superblocks and bitmaps of the to be taken over raid0/raid1/raid4/raid5/raid6
	 * set plus another rimage/rmeta pair has to be allocated for dedicated xor/q.
	 *
	 * In order to postprocess the takeover of a raid set from level M to M (M > N)
	 * in @lv, the last rimage/rmeta devs pair need to be droped in the metadata.
	 */
PFLA("seg->segtype=%s new_segtype->name=%s", seg->segtype->name, new_segtype->name);
	if (!(new_segtype = _adjust_segtype(lv, seg->segtype, new_segtype))) {
		const char *interim_type = "?";

		if (seg_is_any_raid6(seg)) {
			if (segtype_is_any_raid5(new_segtype_sav))
				interim_type = "raid6_ls_6, raid6_la_6, raid6_rs_6 or raid6_ra_6";
			else
				interim_type = "raid6_n_6";

		} else if (seg_is_any_raid5(seg)) {
			if (seg_is_raid5_n(seg))
				interim_type = "raid1";
			else
				interim_type = "raid5_n";

		} else if (seg_is_striped(seg))
			interim_type = "raid5";
			
		log_error("Can't takeover %s to %s", seg->segtype->name, new_segtype_sav->name);
		log_error("Convert to %s first!", interim_type);
		return 0;
	}

PFLA("seg->segtype=%s new_segtype->name=%s", seg->segtype->name, new_segtype->name);

	if (!(is_level_up(seg->segtype, new_segtype) ?
	      _raid_level_up : _raid_level_down)(lv, new_segtype, allocate_pvs))
		return 0;
PFLA("seg->segtype=%s new_segtype->name=%s", seg->segtype->name, new_segtype->name);

	return 1;
}
/******* END: raid <-> raid conversion *******/

/* Return "linear" for striped @segtype instead of "striped" */
static const char *_get_segtype_name(const struct segment_type *segtype, unsigned new_image_count)
{
	return (segtype_is_striped(segtype) && new_image_count == 1) ? "linear" : segtype->name;
}

/*
 * Report current number of redundant disks for @total_images and @segtype 
 */
static void _seg_get_redundancy(struct segment_type *segtype, unsigned total_images, unsigned *nr)
{
	if (segtype_is_raid10(segtype))
		*nr = 1;
	else if (segtype_is_raid1(segtype))
		*nr = total_images - 1;
	else if (segtype_is_any_raid5(segtype))
		*nr = 1;
	else if (segtype_is_any_raid6(segtype))
		*nr = 2;
	else
		*nr = 0;
}

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
int lv_raid_convert(struct logical_volume *lv,
		    const struct segment_type *new_segtype,
		    int yes, int force,
		    unsigned new_image_count,
		    const unsigned new_stripes,
		    const unsigned new_stripe_size,
		    struct dm_list *allocate_pvs)
{
	int r, y;
	unsigned cur_redundancy, new_redundancy;
	struct lv_segment *seg = first_seg(lv);
	const struct segment_type *final_segtype = NULL;
	const struct segment_type *new_segtype_tmp = new_segtype;
	const struct segment_type *striped_segtype;
	struct segment_type *raid0_segtype = get_segtype_from_string(lv->vg->cmd, "raid0");
	struct lvinfo info = { 0 };

	if (!(striped_segtype = get_segtype_from_string(lv->vg->cmd, "striped")))
		return_0;

	if (!new_segtype) {
		log_error(INTERNAL_ERROR "New segtype not specified");
		return 0;
	}

	/* Given segtype of @lv */
	if (!seg_is_striped(seg) &&
	    !seg_is_mirror(seg) &&
	    !seg_is_raid(seg))
		goto err;

	/* Requested segtype */
	if (!segtype_is_linear(new_segtype) &&
	    !segtype_is_striped(new_segtype) &&
	    !segtype_is_mirror(new_segtype) &&
	    !segtype_is_raid(new_segtype))
		goto err;

PFLA("new_image_count=%u new_stripes=%u", new_image_count, new_stripes);
	/* @lv has to be active locally */
	if (vg_is_clustered(lv->vg) && !lv_is_active_exclusive_locally(lv)) {
		log_error("%s/%s must be active exclusive locally to"
			  " perform this operation.", lv->vg->name, lv->name);
		return 0;
	}

	if (!_raid_in_sync(lv)) {
		log_error("Unable to convert %s/%s while it is not in-sync",
			  lv->vg->name, lv->name);
		return 0;
	}

	if (!lv_info(lv->vg->cmd, lv, 0, &info, 1, 0) && driver_version(NULL, 0)) {
		log_error("lv_info failed: aborting");
		return 0;
	}

	/* Get number of redundant disk for current and new segtype */
	_seg_get_redundancy(seg->segtype, seg->area_count, &cur_redundancy);
	_seg_get_redundancy(new_segtype, new_image_count, &new_redundancy);


	if (seg_is_raid1(seg) && new_image_count == 1)
	    	new_segtype_tmp = striped_segtype;

PFLA("cur_redundancy=%u new_redundancy=%u", cur_redundancy, new_redundancy);
	y = yes;
	if (new_redundancy > cur_redundancy)
		log_warn("INFO: Converting active%s %s/%s %s%s%s%s will extend "
			 "resiliency from %u disk failure%s to %u\n",
			 info.open_count ? " and open" : "", lv->vg->name, lv->name,
			 seg->segtype != new_segtype_tmp ? "from " : "",
			 seg->segtype != new_segtype_tmp ? _get_segtype_name(seg->segtype, new_image_count) : "",
			 seg->segtype != new_segtype_tmp ? " to " : "",
			 seg->segtype != new_segtype_tmp ? _get_segtype_name(new_segtype_tmp, new_image_count) : "",
			 cur_redundancy,
			 (!cur_redundancy || cur_redundancy > 1) ? "s" : "",
			 new_redundancy);

	else if (!new_redundancy && cur_redundancy)
		log_warn("WARNING: Converting active%s %s/%s from %s to %s will loose "
			 "all resiliency to %u disk failure%s\n",
			 info.open_count ? " and open" : "", lv->vg->name, lv->name,
			 _get_segtype_name(seg->segtype, new_image_count),
			 _get_segtype_name(new_segtype_tmp, new_image_count),
			 cur_redundancy, cur_redundancy > 1 ? "s" : "");

	else if (new_redundancy &&
		 new_redundancy < cur_redundancy)
		log_warn("WARNING: Converting active%s %s/%s %s%s%s%s will reduce "
			 "resiliency from %u disk failures to just %u\n",
			 info.open_count ? " and open" : "", lv->vg->name, lv->name,
			 seg->segtype != new_segtype_tmp ? "from " : "",
			 seg->segtype != new_segtype_tmp ? _get_segtype_name(seg->segtype, new_image_count) : "",
			 seg->segtype != new_segtype_tmp ? " to " : "",
			 seg->segtype != new_segtype_tmp ? _get_segtype_name(new_segtype_tmp, new_image_count) : "",
			 cur_redundancy, new_redundancy);

	else
		y = 1;

	if (!y && yes_no_prompt("Do you really want to convert %s/%s with type %s to %s [y/n]: ",
				lv->vg->name, lv->name,
				_get_segtype_name(seg->segtype, new_image_count),
				_get_segtype_name(new_segtype_tmp, new_image_count)) == 'n') {
		log_error("Logical volume %s/%s NOT converted", lv->vg->name, lv->name);
		return 0;
	}
	if (sigint_caught())
		return_0;

	/* HM FIXME: archive only when user requests via -y or yes to prompt further downcall */
	if (!archive(lv->vg))
		return_0;

	/*
	 * Linear <-> RAID1 conversion _or_ change image count of RAID1
	 */
	if ((seg_is_linear(seg) && new_image_count > 1) ||
	    (seg_is_linear(seg) && segtype_is_raid1(new_segtype)) ||
	    (seg_is_raid1(seg) && segtype_is_linear(new_segtype)) ||
	    (seg_is_raid1(seg) && segtype_is_raid1(new_segtype))) {
		if (seg_is_linear(seg) && segtype_is_raid1(new_segtype)) {
			if (new_image_count < 2)
				new_image_count = 2;

		} else if (new_image_count < 1)
				new_image_count = 1;

		return _lv_raid_change_image_count(lv, new_segtype, new_image_count, allocate_pvs);
	}

	/*
	 * Mirror -> RAID1 conversion
	 */
	if (seg_is_mirror(seg) && segtype_is_raid1(new_segtype))
		return _convert_mirror_to_raid1(lv, new_segtype);

	/*
	 * RAID1 -> Mirror conversion
	 */
	/*
	 * FIXME: support this conversion or don't invite users to switch back to "mirror"?
	 *        I find this at least valuable in case of an erroneous conversion to raid1
	 */
	if (seg_is_raid1(seg) && segtype_is_mirror(new_segtype)) {
		if (!yes && yes_no_prompt("WARNING: Do you really want to convert %s/%s to "
					  "non-recommended \"mirror\" type? [y/n]: ",
				  lv->vg->name, lv->name) == 'n') {
			log_error("Logical volume %s/%s NOT converted to \"mirror\"", lv->vg->name, lv->name);
			return 0;
		}
		if (sigint_caught())
			return_0;

		return _convert_raid1_to_mirror(lv, new_segtype, allocate_pvs);
	}

	/*
	 * RAID0 <-> RAID10 comversion
	 *
	 * MD RAID10 is a stripe on top of stripes number of 2-way mirrors
	 */
	/* HM FIXME: adjust_segtype() needed at all? */
	if (seg_is_any_raid0(seg) && segtype_is_raid10(new_segtype))
		return _lv_raid_change_image_count(lv, new_segtype, lv_raid_image_count(lv) * 2, allocate_pvs);

	if (seg_is_raid10(seg) && segtype_is_any_raid0(new_segtype))
		return _lv_raid_change_image_count(lv, new_segtype, lv_raid_image_count(lv) / 2, allocate_pvs);

PFLA("segtype_is_linear(new_segtype)=%d", segtype_is_linear(new_segtype));
	/* Striped -> RAID0 conversion */
	if (seg_is_striped(seg) && segtype_is_striped_raid(new_segtype)) {
		/* Only allow _convert_striped_to_raid0() to update and reload metadata if the final level is raid0* */
		int update_and_reload = segtype_is_any_raid0(new_segtype);

PFLA("update_and_reload=%u", update_and_reload);
		r = _convert_striped_to_raid0(lv,
					      !segtype_is_raid0(new_segtype) /* -> alloc_metadata_devs */,
					      update_and_reload);
PFLA("r=%d", r);
		/* If error or final type was raid0 -> already finished with remapping in _covert_striped_to_raid9(). */
		if (!r || update_and_reload)
			return r;


	/* RAID0 <-> striped conversion */
	} else if (segtype_is_linear(new_segtype) ||
		   segtype_is_striped(new_segtype)) {
		if (seg_is_any_raid0(seg))
			return _convert_raid0_to_striped(lv, striped_segtype);

		/* Memorize the final "striped" segment type */
		final_segtype = new_segtype;

		/* Let _convert_raid_to_raid() go to "raid0", thus droping metadata images */
		new_segtype = raid0_segtype;
	}

	/* All the rest of the raid conversions... */
	r = _convert_raid_to_raid(lv, new_segtype, final_segtype, yes, force, new_stripes, new_stripe_size, allocate_pvs);

	/* Do the final step to convert from "raid0" to "striped" here if requested */
	/* HM FIXME: avoid update and reload in _convert_raid_to_raid! */
	if (r && final_segtype)
		r = _convert_raid0_to_striped(lv, final_segtype);

	return r;

err:
	/* FIXME: enhance message */
	log_error("Converting the segment type for %s/%s from %s to %s"
		  " is not supported.", lv->vg->name, lv->name,
		  lvseg_name(seg), new_segtype->name);
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
	if (!_replace_lv_with_error_segment(rm_image))
		return 0;

	return 1;
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

/* Helper fn to generate LV names and set segment area lv */
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
		log_error("Replacement of devices in %s/%s %s LV prohibited.",
			  lv->vg->name, lv->name, raid_seg->segtype->name);
		return 0;
	}

	if (lv->status & PARTIAL_LV)
		lv->vg->cmd->partial_activation = 1;

	if (!lv_is_active_exclusive_locally(lv_lock_holder(lv))) {
		log_error("%s/%s must be active %sto perform this operation.",
			  lv->vg->name, lv->name,
			  vg_is_clustered(lv->vg) ? "exclusive locally " : "");
		return 0;
	}

	if (!_raid_in_sync(lv)) {
		log_error("Unable to replace devices in %s/%s while it is"
			  " not in-sync.", lv->vg->name, lv->name);
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
		log_verbose("%s/%s does not contain devices specified"
			    " for replacement", lv->vg->name, lv->name);
		return 1;
	} else if (match_count == raid_seg->area_count) {
		log_error("Unable to remove all PVs from %s/%s at once.",
			  lv->vg->name, lv->name);
		return 0;
	} else if (raid_seg->segtype->parity_devs &&
		   (match_count > raid_seg->segtype->parity_devs)) {
		log_error("Unable to replace more than %u PVs from (%s) %s/%s",
			  raid_seg->segtype->parity_devs,
			  lvseg_name(raid_seg),
			  lv->vg->name, lv->name);
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
		log_error("Failed to allocate replacement images for %s/%s",
			  lv->vg->name, lv->name);

		return 0;
	}

	/* HM FIXME: TESTME */
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
		log_error("Failed to remove the specified images from %s/%s",
			  lv->vg->name, lv->name);
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

#if 0
dump_lv("", lv);
#endif
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

#if 0
dump_lv("", lv);
#endif
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
		log_error(INTERNAL_ERROR "%s/%s is not a partial LV",
			  lv->vg->name, lv->name);
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
