/*
 * Copyright (C) 2011-2014 Red Hat, Inc. All rights reserved.
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

#if 1
#include "dump.h"
#endif

/* HM FIXME: TESTME: Ensure minimum region size because of respective MD limit */
static void _ensure_min_region_size(struct logical_volume *lv)
{
	/* MD's bitmap is limited to tracking 2^21 regions */
	uint32_t min_region_size = lv->size / (1 << 21);
	int changed = 0;
	struct lv_segment *seg = first_seg(lv);

	while (seg->region_size < min_region_size) {
		seg->region_size *= 2;
		changed = 1;
	}

	if (changed)
		log_very_verbose("Setting RAID1 region_size to %uS",
				 seg->region_size);
}

/* Default region_size on @lv unless already set */
static void __init_region_size(struct logical_volume *lv)
{
	struct lv_segment *seg = first_seg(lv);

	seg->region_size = seg->region_size ? : get_default_region_size(lv->vg->cmd);
	_ensure_min_region_size(lv);
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
static int __deactivate_and_remove_lvs(struct volume_group *vg, struct dm_list *removal_lvs)
{
	struct lv_list *lvl;

	dm_list_iterate_items(lvl, removal_lvs) {
		if (!deactivate_lv(vg->cmd, lvl->lv))
			return_0;

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

	if (seg_is_striped(seg) || seg_is_raid0(seg))
		return 1;

	if (!lv_raid_percent(lv, &sync_percent)) {
		log_error("Unable to determine sync status of %s/%s.",
			  lv->vg->name, lv->name);
		return 0;
	}

	return (sync_percent == DM_PERCENT_100) ? 1 : 0;
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

	if (seg->area_count != 1) {
		log_error(INTERNAL_ERROR
			  "Unable to remove RAID layer when there"
			  " is more than one sub-lv");
		return 0;
	}

	if (!(lvl_array = dm_pool_alloc(lv->vg->vgmem, 2 * sizeof(*lvl))))
		return_0;

	/* Add remaining last metadata area to removal_list */
	lv_tmp = seg_metalv(seg, 0);
	lv_set_visible(lv_tmp);
	lvl_array[0].lv = lv_tmp;

	if (!remove_seg_from_segs_using_this_lv(lv_tmp, seg))
		return_0;

	seg_metatype(seg, 0) = AREA_UNASSIGNED;
	dm_list_add(removal_list, &(lvl_array[0].list));

	/* Remove RAID layer and add residual LV to removal_list*/
	lv_tmp = seg_lv(seg, 0);
	lv_tmp->status &= ~RAID_IMAGE;
	lv_set_visible(lv_tmp);
	lvl_array[1].lv = lv_tmp;

	dm_list_add(removal_list, &(lvl_array[1].list));

	if (!remove_layer_from_lv(lv, lv_tmp))
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
	if (!vg || !vg_write(vg) || !vg_commit(vg))
		return_0;

	dm_list_iterate_items(lvl, lv_list) {
		if (!_clear_lv(lvl->lv))
			return 0;
	}

	return 1;
}

/* Check for maximum supported devices caused by the kernel superblock bitfield constraint */
static int __check_maximum_devices(unsigned num_devices)
{
	if (num_devices > DEFAULT_RAID_MAX_IMAGES) {
		log_error("Unable to handle arrays with more than %u devices",
			  DEFAULT_RAID_MAX_IMAGES);
		return 0;
	}

	return 1;
}

/* Retrieve index from @*lv_name and add it to @prefix; set the result in @*lv_name */
static int __lv_name_add_string_index(struct cmd_context *cmd, const char **lv_name, const char *prefix)
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
	if (!__check_maximum_devices(seg->area_count))
		return 0;

	log_very_verbose("Shifting images in %s", seg->lv->name);

	for (s = 0, missing = 0; s < seg->area_count; s++) {
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
			seg->meta_areas[s - missing] = seg->meta_areas[s];
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

	if (!dm_list_empty(removal_list)) {
		if (!__deactivate_and_remove_lvs(vg, removal_list))
			return 0;

		if (!vg_write(vg) || !vg_commit(vg))
			return_0;

		if (!backup(vg))
			log_error("Backup of VG %s failed after removal of image component LVs", vg->name);
	}

	return 1;
}

/*
 * Reallocate segment areas given by @seg_areas (i.e eith data or metadata areas)
 * in segment @seg to amount in @areas copying the minimum of common areas across
 */
static int __realloc_seg_areas(struct logical_volume *lv, struct lv_segment *seg,
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
static int __realloc_meta_and_data_seg_areas(struct logical_volume *lv, struct lv_segment *seg,
					     uint32_t areas)
{
	return (__realloc_seg_areas(lv, seg, areas, &seg->meta_areas) &&
	        __realloc_seg_areas(lv, seg, areas, &seg->areas)) ? 1 : 0;
}

/* Add new @lvs to @lv at @area_offset */
static int __add_sublvs_to_lv(struct logical_volume *lv, int delete_from_list,
			      uint64_t lv_flags, struct dm_list *lvs, uint32_t area_offset)
{
	unsigned a = area_offset;
	struct lv_segment *seg = first_seg(lv);
	struct lv_list *lvl, *tmp;

	dm_list_iterate_items_safe(lvl, tmp, lvs) {
		if (delete_from_list)
			dm_list_del(&lvl->list);

		if (!set_lv_segment_area_lv(seg, a, lvl->lv, 0,
					    lvl->lv->status)) {
			log_error("Failed to add %s to %s",
				  lvl->lv->name, lv->name);
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

		a++;
	}

	return 1;
}

/*
 * Create an LV of specified type.  Set visible after creation.
 * This function does not make metadata changes.
 */
static struct logical_volume *_alloc_image_component(struct logical_volume *lv,
						     const char *alt_base_name,
						     struct alloc_handle *ah, uint32_t first_area,
						     uint64_t type)
{
	uint64_t status = LVM_READ | LVM_WRITE | type;
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
			alt_base_name ? : lv->name, type_suffix) < 0)
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
	}

	lv_set_visible(tmp_lv);

	return tmp_lv;
}

static uint32_t __calc_rmeta_extents(struct logical_volume *lv)
{
printf("seg->region_size=%u\n", first_seg(lv)->region_size);
	return raid_rmeta_extents(lv->vg->cmd, lv->size / lv->vg->extent_size,
				  first_seg(lv)->region_size, lv->vg->extent_size);
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

	__init_region_size(data_lv);

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

printf("%s %u\n", __func__, __LINE__);
	if (!(ah = allocate_extents(data_lv->vg, NULL, seg->segtype, 0, 1, 0,
				    seg->region_size,
				    __calc_rmeta_extents(data_lv),
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
static int __alloc_rmeta_devs_for_rimage_devs(struct logical_volume *lv,
					      struct dm_list *new_data_lvs, struct dm_list *new_meta_lvs)
{
	unsigned a = 0, raid_devs = 0;
	struct lv_list *lvl, *lvl_array;

	dm_list_iterate_items(lvl, new_data_lvs)
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


static void __alloc_destroy(struct alloc_handle *ah_metadata, struct alloc_handle *ah_data)
{
	if (ah_metadata)
		alloc_destroy(ah_metadata);

	if (ah_data)
		alloc_destroy(ah_data);
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
	struct alloc_handle *ah_metadata = NULL, *ah_data = NULL;
	struct dm_list *parallel_areas;
	struct lv_list *lvl_array;

	if (!new_meta_lvs && !new_data_lvs)
		return 0;

	if (!(lvl_array = dm_pool_alloc(lv->vg->vgmem, sizeof(*lvl_array) * count * 2)))
		return_0;

	if (!(parallel_areas = build_parallel_areas_from_lv(lv, 0, 1)))
		return_0;

	__init_region_size(lv);

	if (seg_is_raid(seg))
		segtype = seg->segtype;
	/* HM FIXME: still needed? */
	else if (!(segtype = get_segtype_from_string(lv->vg->cmd, SEG_TYPE_NAME_RAID1)))
		return_0;

printf("%s %u segtype=%s seg->segtype=%s\n", __func__, __LINE__, segtype->name, seg->segtype->name);
	/*
	 * The number of extents is based on the RAID type.  For RAID1/10,
	 * each of the rimages is the same size - 'le_count'.  However
	 * for RAID 0/4/5/6, the stripes add together (NOT including the parity
	 * devices) to equal 'le_count'.  Thus, when we are allocating
	 * individual devies, we must specify how large the individual device
	 * is along with the number we want ('count').
	 */
	if (allocate) {
		if (new_meta_lvs) {
			/* Allocate one extent for the rmeta device(s) */
			if (!(ah_metadata = allocate_extents(lv->vg, NULL, segtype, 0, count, count,
							     seg->region_size, __calc_rmeta_extents(lv), pvs,
							     lv->alloc, 0, parallel_areas)))
				return_0;
		}

		if (new_data_lvs) {
			/* And the appropriate amount of extents for the rimage device(s) */
			extents = (segtype_is_raid0(segtype) || segtype->parity_devs) ?
				  lv->le_count / (seg->area_count - segtype->parity_devs) : lv->le_count;

			if (!(ah_data = allocate_extents(lv->vg, NULL, segtype, 0, count, count,
							 seg->region_size, extents, pvs,
							 lv->alloc, 0, parallel_areas))) {
				__alloc_destroy(ah_metadata, ah_data);
				return_0;
			}
		}
	}

	for (s = 0; s < count; ++s) {
		/*
		 * The allocation areas are grouped together.  First
		 * come the rimage allocated areas, then come the metadata
		 * allocated areas.  Thus, the metadata areas are pulled
		 * from 's + count'.
		 */

		/*
		 * If the segtype is raid0, we may avoid allocating metadata
		 * LV to accompany the data LV by not passing in @new_meta_lvs
		 */
		if (new_meta_lvs) {
			if (!(lvl_array[s + count].lv =
			      _alloc_image_component(lv, NULL, ah_metadata, s + count, RAID_META))) {
				__alloc_destroy(ah_metadata, ah_data);
				return_0;
			}

			dm_list_add(new_meta_lvs, &(lvl_array[s + count].list));
		}

		if (new_data_lvs) {
			if (!(lvl_array[s].lv = _alloc_image_component(lv, NULL, ah_data, s, RAID_IMAGE))) {
				__alloc_destroy(ah_data, ah_data);
				return_0;
			}

			dm_list_add(new_data_lvs, &(lvl_array[s].list));
		}
	}

	__alloc_destroy(ah_metadata, ah_data);

	return 1;
}

/* Cleanly remove newly-allocated LVs that failed insertion attempt */
static int __remove_lvs(struct dm_list *lvs)
{
	struct lv_list *lvl;

	dm_list_iterate_items(lvl, lvs)
		if (!lv_remove(lvl->lv))
			return_0;

	return 0;
}

/* Factored out function to allocate an rmeta dev for a linear one */
static int __alloc_rmeta_for_linear(struct logical_volume *lv, struct dm_list *meta_lvs)
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
static int __correct_data_lv_names(struct logical_volume *lv, uint32_t count, struct dm_list *data_lvs)
{
	struct dm_list *l;
	struct lv_list *lvl, *lvl_n;

printf("%s %u\n", __func__, __LINE__);
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

/* Convert linear @lv to raid1 */
static int __convert_linear_to_raid1(struct logical_volume *lv)
{
	struct lv_segment *seg = first_seg(lv);;
	uint32_t region_size = seg->region_size;

printf("%s %u\n", __func__, __LINE__);
	seg->status |= RAID_IMAGE;
	if (!insert_layer_for_lv(lv->vg->cmd, lv,
				 RAID | LVM_READ | LVM_WRITE,
				 "_rimage_0"))
		return_0;

	lv->status |= RAID;
	seg = first_seg(lv);
	seg_lv(seg, 0)->status |= RAID_IMAGE | LVM_READ | LVM_WRITE;
	seg->region_size = region_size;
	__init_region_size(lv);

	if (!(seg->segtype = get_segtype_from_string(lv->vg->cmd, SEG_TYPE_NAME_RAID1)))
		return_0;

	return 1;
}

/* Reset any rebuild or reshape flags on @seg already passed to the kernel */
static int __reset_flags_passed_to_kernel(struct lv_segment *seg)
{
	unsigned s;
	int flag_cleared = 0;

	for (s = 0; s < seg->area_count; s++) {
		if ((seg_metalv(seg, s)->status & LV_REBUILD) ||
		    (seg_lv(seg, s)->status & LV_REBUILD)) {
			seg_metalv(seg, s)->status &= ~LV_REBUILD;
			seg_lv(seg, s)->status &= ~LV_REBUILD;
			flag_cleared = 1;
		}
		
		if ((seg_lv(seg, s)->status & LV_RESHAPE_DELTA_DISKS_PLUS)) {
			seg_lv(seg, s)->status &= ~LV_RESHAPE_DELTA_DISKS_PLUS;
			flag_cleared = 1;
		}

		if ((seg_lv(seg, s)->status & LV_RESHAPE_DELTA_DISKS_MINUS)) {
			seg_lv(seg, s)->status &= ~LV_RESHAPE_DELTA_DISKS_MINUS;
			flag_cleared = 1;
		}
	}

	return flag_cleared;
}

/*
 * Add raid rmeta/rimage pair(s) to @lv to get to
 * absolute @new_count using @pvs to allocate from
 *
 */
static int _raid_add_images(struct logical_volume *lv, struct segment_type *segtype,
			    uint32_t new_count, struct dm_list *pvs)
{
	int linear;
	uint32_t s;
	uint32_t old_count = lv_raid_image_count(lv);
	uint32_t count = new_count - old_count;
	uint64_t lv_flags = LV_REBUILD;
	struct lv_segment *seg = first_seg(lv);
	struct dm_list meta_lvs, data_lvs;

	if (!(linear = seg_is_linear(seg)) &&
	    !seg_is_raid(seg)) {
		log_error("Unable to add RAID images to %s of segment type %s",
			  lv->name, lvseg_name(seg));
		return 0;
	}

	if (lv->status & LV_NOTSYNCED) {
		log_error("Can't add image to out-of-sync RAID LV:"
			  " use 'lvchange --resync' first.");
		return 0;
	}

	if (!_raid_in_sync(lv)) {
		log_error("Can't add image to RAID LV that is still initializing.");
		return 0;
	}

	if (!archive(lv->vg))
		return_0;

	dm_list_init(&meta_lvs); /* For metadata image additions */
	dm_list_init(&data_lvs); /* For data image additions */

	/*
	 * If the segtype is linear, then we must allocate a metadata
	 * LV to accompany it.
	 */
	if (linear) {
		/*
		 * A complete resync will be done because of
		 * the new raid1 set, no need to mark each sub-lv
		 */
		lv_flags = 0; /* Resets LV_REBUILD in __add_sublvs_to_lv() */

		/* Allocate an rmeta device to pair with the linear image */
		if (!__alloc_rmeta_for_linear(lv, &meta_lvs))
			return 0;
	}

	/* Allocate the additional meta and data lvs requested */
	if (!_alloc_image_components(lv, 1, pvs, count, &meta_lvs, &data_lvs))
		return_0;

	/*
	 * If linear, we must correct data LV names.  They are off-by-one
	 * because the linear volume hasn't taken its proper name of "_rimage_0"
	 * yet.  This action must be done before '_clear_lvs' because it
	 * commits the LVM metadata before clearing the LVs.
	 */
	if (linear && !__correct_data_lv_names(lv, count, &data_lvs))
		return 0;

	/* Metadata LVs must be cleared before being added to the array */
	if (!_clear_lvs(&meta_lvs))
		goto fail;

	/* We are converting from linear to raid1 */
	if (linear) {
		if (!__convert_linear_to_raid1(lv))
			return 0;

		seg = first_seg(lv);
	}

#if 0
	/*
	 * FIXME: It would be proper to activate the new LVs here, instead of having
	 * them activated by the suspend.  However, this causes residual device nodes
	 * to be left for these sub-lvs.
	 */
	dm_list_iterate_items(lvl, &meta_lvs)
		if (!do_correct_activate(lv, lvl->lv))
			return_0;
	dm_list_iterate_items(lvl, &data_lvs)
		if (!do_correct_activate(lv, lvl->lv))
			return_0;
#endif

	/* Expand areas arrays for metadata and data devs  */
	if (!__realloc_meta_and_data_seg_areas(lv, seg, new_count)) {
		log_error("Relocation of areas arrays failed.");
		return 0;
	}

	seg->area_count = new_count;

	/* Set segment areas for metadata sub_lvs */
	/* Add extra meta area when converting from linear */
	if (!__add_sublvs_to_lv(lv, 0, 0, &meta_lvs, linear ? 0 : old_count))
		goto fail;

	/* Set segment areas for data sub_lvs */
	if (!__add_sublvs_to_lv(lv, 0, lv_flags, &data_lvs, old_count))
		goto fail;

	if (!seg_is_raid1(seg) && seg->segtype == segtype) {
		unsigned les = lv->le_count / (old_count - seg->segtype->parity_devs);

printf("%s %d le_count=%u old_count=%u new_count=%u les=%u\n", __func__, __LINE__, lv->le_count, old_count, new_count, les);
		for (s = old_count; s < new_count; s++) {
printf("%s %d %u\n", __func__, __LINE__, s);
			seg_lv(seg, s)->status &= ~LV_REBUILD;
			seg_lv(seg, s)->status |= LV_RESHAPE_DELTA_DISKS_PLUS;

			lv->le_count += les;
		}

		seg->len = lv->le_count;
		seg->area_len = les;
		// seg->len = seg->area_len = lv->le_count;
printf("%s %d le_count=%u old_count=%u les=%u\n", __func__, __LINE__, lv->le_count, old_count, les);
	}

	/* HM FIXME: really needed? */
	if (!linear && segtype)
		seg->segtype = segtype;

#if 0
	/* HM FIXME: change LV size for delta_disks plus/minus */

	/* HM FIXME: TESTME for raid1 -> raid5 */
	if (count == 1 && old_count == 2 && segtype_is_any_raid5(segtype)) {
		lv->le_count *= 2;
		seg->len *= 2;
	}
#endif

printf("%s %d\n", __func__, __LINE__);
	if (!lv_update_and_reload_origin(lv))
		return_0;
printf("%s %d\n", __func__, __LINE__);

	/*
	 * Now that the 'REBUILD'/'RESHAPE_DELTA_DISKS' has made its way to
	 * the kernel, we must remove the flag so that the individual devices
	 * are not rebuilt upon every activation.
	 */
	if (__reset_flags_passed_to_kernel(first_seg(lv))) {
		if (!vg_write(lv->vg) || !vg_commit(lv->vg)) {
			log_error("Failed to clear REBUILD flag for %s/%s components",
				  lv->vg->name, lv->name);
			return 0;
		}

		backup(lv->vg);
	}

printf("%s %d\n", __func__, __LINE__);
	return 1;

fail:
	/* Cleanly remove newly-allocated LVs that failed insertion attempt */
	if (!__remove_lvs(&meta_lvs) ||
	    !__remove_lvs(&data_lvs))
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
				    uint64_t type, unsigned idx,
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
		break;
	case RAID_IMAGE:
		seg_type(seg, idx) = AREA_UNASSIGNED;
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
static int _extract_image_components_to_lv_list(struct lv_segment *seg, uint32_t idx,
						struct lv_list *lvl_array)
{
	/* Don't change extraction sequence; callers are relying on it */
	if (!_extract_image_component(seg, RAID_META, idx, &lvl_array[0].lv) ||
	    !_extract_image_component(seg, RAID_IMAGE, idx, &lvl_array[1].lv))
		return_0;

	return 1;
}

/* Write vg of @lv, suspend @lv and commit the vg */
static int __vg_write_lv_suspend_vg_commit(struct logical_volume *lv)
{
	if (!vg_write(lv->vg)) {
		log_error("Failed to write changes to %s in %s",
			  lv->name, lv->vg->name);
		return 0;
	}

	if (!suspend_lv(lv->vg->cmd, lv_lock_holder(lv))) {
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

		if (!_extract_image_components_to_lv_list(seg, s, lvl_array + lvl_idx)) {
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

static int _raid_remove_images(struct logical_volume *lv,
			       uint32_t new_count, struct dm_list *pvs)
{
	struct dm_list removal_list;
	struct lv_list *lvl;
#if 0
	unsigned s;
	struct lv_segment *seg = first_seg(lv);
#endif

	if (!archive(lv->vg))
		return_0;

	dm_list_init(&removal_list);

#if 0
	/* HM FIXME: pass delta_disks to the kernel first, _then_ extract images and commit */
	for (s = new_count, s < old_count; s++) {
printf("%s %d %u\n", __func__, __LINE__, s);
		seg_lv(seg, s)->status &= ~LV_REBUILD;
		seg_lv(seg, s)->status |= LV_RESHAPE_DELTA_DISKS_MINUS;
	}
#endif

	if (!_raid_extract_images(lv, new_count, pvs, 1,
				 &removal_list, &removal_list)) {
		log_error("Failed to extract images from %s/%s",
			  lv->vg->name, lv->name);
		return 0;
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

printf("%s %d\n", __func__, __LINE__);
#if 0
dump_lv("", lv);
#endif

	if (!__vg_write_lv_suspend_vg_commit(lv))
		return 0;

printf("%s %d\n", __func__, __LINE__);

	/*
	 * We activate the extracted sub-LVs first so they are
	 * renamed and won't conflict with the remaining sub-LVs.
	 */
	dm_list_iterate_items(lvl, &removal_list) {
		if (!activate_lv_excl_local(lv->vg->cmd, lvl->lv)) {
			log_error("Failed to resume extracted LVs");
			return 0;
		}
	}
printf("%s %d\n", __func__, __LINE__);

	if (!resume_lv(lv->vg->cmd, lv)) {
		log_error("Failed to resume %s/%s after committing changes",
			  lv->vg->name, lv->name);
		return 0;
	}
printf("%s %d\n", __func__, __LINE__);

	/* Eliminate the residual LVs, write VG, commit it and take a backup */
	return _eliminate_extracted_lvs(lv->vg, &removal_list);
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
static int __lv_raid_change_image_count(struct logical_volume *lv, struct segment_type *segtype,
					uint32_t new_count, struct dm_list *pvs)
{
	uint32_t old_count = lv_raid_image_count(lv);

printf("%s %d\n", __func__, __LINE__);
	if (old_count == new_count) {
		log_warn("%s/%s already has image count of %d.",
			 lv->vg->name, lv->name, new_count);
		return 1;
	}

	/* Check for maximum supported raid devices */
	if (!__check_maximum_devices(new_count))
		return 0;

	/*
	 * LV must be either in-active or exclusively active
	 */
	if (lv_is_active(lv) && vg_is_clustered(lv->vg) &&
	    !lv_is_active_exclusive_locally(lv)) {
		log_error("%s/%s must be active exclusive locally to"
			  " perform this operation.", lv->vg->name, lv->name);
		return 0;
	}

	if (old_count > new_count)
		return _raid_remove_images(lv, new_count, pvs);

	return _raid_add_images(lv, segtype, new_count, pvs);
}

int lv_raid_change_image_count(struct logical_volume *lv,
			       uint32_t new_count, struct dm_list *pvs)
{
	return __lv_raid_change_image_count(lv, NULL, new_count, pvs);
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
		if (!__lv_name_add_string_index(cmd, &lvl->lv->name, split_name))
			return 0;

	if (!__vg_write_lv_suspend_vg_commit(lv))
		return 0;

	if (!resume_lv(lv->vg->cmd, lv_lock_holder(lv))) {
		log_error("Failed to resume %s/%s after committing changes",
			  lv->vg->name, lv->name);
		return 0;
	}

	/*
	 * First activate the newly split LV and LVs on the removal list.
	 * This is necessary so that there are no name collisions due to
	 * the original RAID LV having possibly had sub-LVs that have been
	 * shifted and renamed.
	 */
	dm_list_iterate_items(lvl, &data_list)
		if (!activate_lv_excl_local(cmd, lvl->lv))
			return_0;

	dm_list_iterate_items(lvl, &removal_list)
		if (!activate_lv_excl_local(cmd, lvl->lv))
			return_0;

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

	if (!_raid_in_sync(lv)) {
		log_error("Unable to convert %s/%s while it is not in-sync",
			  lv->vg->name, lv->name);
		return 0;
	}

	if (!(meta_areas = dm_pool_zalloc(lv->vg->vgmem,
					  lv_mirror_count(lv) * sizeof(*meta_areas)))) {
		log_error("Failed to allocate meta areas memory.");
		return 0;
	}

	if (!archive(lv->vg))
		return_0;

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
		if (!(new_name = _generate_raid_name(seg_lv(seg, s), "rimage", s)))
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

/* BEGIN: striped -> raid0 conversion */
/*
 * HM
 *
 * Helper convert striped to raid0
 *
 * For @lv, empty hidden LVs im @new_data_lvs have been created by the caller.
 *
 * All areas from @lv segments are being moved
 * to new segments allocated for @new_data_lvs.
 *
 * Returns: 1 on success, 0 on failure
 */
static int __striped_to_raid0_move_segs_to_raid0_components(struct logical_volume *lv,
							   struct dm_list *new_data_lvs)
{
	unsigned area_idx = 0, le;
	struct logical_volume *new_data_lv;
	struct lv_segment *seg_from, *seg_new, *tmp;
	struct dm_list *l;
	struct segment_type *segtype = get_segtype_from_string(lv->vg->cmd, "striped");

	dm_list_iterate(l, new_data_lvs) {
		new_data_lv = (dm_list_item(l, struct lv_list))->lv;

		le = 0;
		dm_list_iterate_items(seg_from, &lv->segments) {
			uint64_t status = RAID | (seg_from->status & (LVM_READ | LVM_WRITE));

			/* Allocate a segment with one area for each segment in the striped LV */
			seg_new = alloc_lv_segment(segtype, new_data_lv,
						   le, seg_from->area_len, status,
						   seg_from->stripe_size, NULL, 1 /* area_count */,
						   seg_from->area_len, seg_from->chunk_size,
						   seg_from->region_size, 0, NULL);
			if (!seg_new)
				return_0;

			seg_type(seg_new, 0) = AREA_UNASSIGNED;
			dm_list_add(&new_data_lv->segments, &seg_new->list);
			le += seg_from->area_len;

			/* Move the respective area across to our new segment */
			if (!move_lv_segment_area(seg_new, 0, seg_from, area_idx))
				return_0;

			/* Adjust le count and lv size */
			new_data_lv->le_count += seg_from->area_len;
			new_data_lv->size += (uint64_t) seg_from->area_len * lv->vg->extent_size;
		}

		area_idx++;
	}

	/* Remove the empty segments of the striped LV */
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
static int __striped_to_raid0_alloc_raid0_segment(struct logical_volume *lv,
						  uint32_t area_count,
						  struct lv_segment *seg)
{
	struct lv_segment *seg_new;
	struct segment_type *segtype = get_segtype_from_string(lv->vg->cmd, SEG_TYPE_NAME_RAID0);

	/* Allocate single segment to hold the image component areas */
	seg_new = alloc_lv_segment(segtype, lv,
				   0, lv->le_count,
				   seg->status,
				   seg->stripe_size, NULL, area_count,
				   lv->le_count, seg->chunk_size,
				   seg->region_size, 0u, NULL);
	if (!seg_new)
		return_0;

	lv->status |= RAID;
	seg_new->status |= RAID;
	dm_list_add(&lv->segments, &seg_new->list);

	return 1;
}

/*
 * HM
 *
 * Helper convert striped to raid0
 *
 * Inserts hidden LVs for all segments and the parallel areas in @lv and moves 
 * given segments and areas across.
 *
 * Optionally allocates metadata devs if @alloc_metadata_devs
 *
 * Returns: 1 on success, 0 on failure
 */
static int _convert_striped_to_raid0(struct logical_volume *lv,
				     const struct segment_type *new_segtype,
				     int alloc_metadata_devs)
{
	unsigned a;
	struct lv_segment *seg = first_seg(lv), *seg1;
	struct dm_list new_meta_lvs;
	struct dm_list new_data_lvs;
	unsigned area_count = seg->area_count;

	dm_list_init(&new_meta_lvs);
	dm_list_init(&new_data_lvs);

	if (!seg_is_striped(seg) ||
	     seg->area_count < 2)
		return 0;

	/* Check for non-supported varying area_count on multi-segment striped LVs */
	dm_list_iterate_items(seg1, &lv->segments) {
		if (seg1->area_count != area_count) {
			log_error("Cannot convert striped LV %s with varying stripe numbers to raid0", lv->name);
			return 0;
		}
	}

	if (!archive(lv->vg))
		return_0;

	/* Allocate rimage components in order to be able to support multi-segment "striped" LVs */
	if (!_alloc_image_components(lv, 0, NULL, area_count, NULL, &new_data_lvs)) {
		log_error("Failed to allocate image components for raid0 LV %s.", lv->name);
		return_0;
	}

	/* Image components are being allocated with LV_REBUILD preset and we don't need it for 'striped' */
	for (a = 0; a < area_count; a++)
		seg_lv(seg, a)->status &= LV_REBUILD;

	/* Move the AREA_PV areas across to the new rimage components */
	if (!__striped_to_raid0_move_segs_to_raid0_components(lv, &new_data_lvs)) {
		log_error("Failed to insert linear LVs underneath, %s.", lv->name);
		return_0;
	}

	/* Allocate new top-level LV segment */
	seg = first_seg(dm_list_item(dm_list_first(&new_data_lvs), struct lv_list)->lv);
	if (!__striped_to_raid0_alloc_raid0_segment(lv, area_count, seg)) {
		log_error("Failed to allocate new raid0 segement for LV %s.", lv->name);
		return_0;
	}

	if (alloc_metadata_devs) {
		seg = first_seg(lv);

		/* Allocate a new metadata device for each of the raid0 stripe LVs */
		if (!__alloc_rmeta_devs_for_rimage_devs(lv, &new_data_lvs, &new_meta_lvs))
			return 0;

		/* Now that we allocated the rmeta_devs based on the new_data_lvs list , add to the top-level LV */
		if (!__add_sublvs_to_lv(lv, 1, 0, &new_data_lvs, 0))
			return 0;

		/* Metadata LVs must be cleared before being added to the array */
		log_debug_metadata("Clearing newly allocated metadata LVs");
		if (!_clear_lvs(&new_meta_lvs)) {
			log_error("Failed to initialize metadata LVs");
			return 0;
		}

		if (!__realloc_seg_areas(lv, seg, area_count, &seg->meta_areas))
			return 0;

		seg->area_count = area_count;

		if (!__add_sublvs_to_lv(lv, 1, 0, &new_meta_lvs, 0))
			return_0;

	} else if (!__add_sublvs_to_lv(lv, 1, 0, &new_data_lvs, 0))
		return 0;

	if (!lv_update_and_reload(lv))
		return_0;

	return 1;
}
/* END: striped -> raid0 conversion */

/* BEGIN: raid0 -> striped conversion */
/*
 * HM
 *
 * All areas from @lv image component LV's segments are
 * being moved to @new_segments allocated.
 * The metadata+data component LVs are being linked to @removal_lvs
 *
 * Returns: 1 on success, 0 on failure
 */
static int __raid0_to_striped_retrieve_segments_and_lvs(struct logical_volume *lv,
							struct dm_list *removal_lvs)
{
	unsigned a = 0, le = 0;
	struct lv_segment *seg = first_seg(lv), *seg_from, *seg_to;
	struct logical_volume *mlv, *dlv;
	struct dm_list new_segments;
	struct lv_list *lvl_array;
	struct segment_type *striped_segtype = get_segtype_from_string(lv->vg->cmd, "striped");

	dm_list_init(&new_segments);

	if (!(lvl_array = dm_pool_alloc(lv->vg->vgmem, 2 * sizeof(*lvl_array) * seg->area_count)))
		return_0;

	/* Allocate number of striped segments needed. */
	dlv = seg_lv(seg, 0);
	dm_list_iterate_items(seg_from, &dlv->segments) {
		uint64_t status = (seg_from->status & ~RAID);

		/* Allocate a segment with #stripes areas for each segment in the image LV */
		if (!(seg_to = alloc_lv_segment(striped_segtype, lv,
						le, lv->le_count, status,
						seg->stripe_size, NULL, seg->area_count,
						seg_from->area_len, seg->chunk_size,
						seg->region_size, 0, NULL)))
			return_0;

		dm_list_add(&new_segments, &seg_to->list);
		le += seg_from->area_len;
	}

	/* Loop the areas listing the image LVs and move all areas across from them to @new_segments */
	for (a = 0; a < seg->area_count; a++) {
		unsigned len = 0;

		dlv = seg_lv(seg, a);
		dlv->status &= ~RAID;
		lvl_array[a].lv = dlv;
		dm_list_add(removal_lvs, &lvl_array[a].list);

		if (seg->meta_areas) {
			if ((mlv = seg_metalv(seg, a))) {
				mlv->status &= ~RAID;
				lvl_array[seg->area_count + a].lv = mlv;
				dm_list_add(removal_lvs, &lvl_array[seg->area_count + a].list);

				if (!remove_seg_from_segs_using_this_lv(mlv, seg))
					return_0;

				lv_set_visible(mlv);

				if (!replace_lv_with_error_segment(mlv))
					return_0;
			}
		}

		seg_from = first_seg(dlv);
		dm_list_iterate_items(seg_to, &new_segments) {

			/* Move the respective area across to our new segment */
			seg_type(seg_to, a) = AREA_UNASSIGNED;
			len += seg_from->area_len;

			if (!move_lv_segment_area(seg_to, a, seg_from, 0))
				return_0;

			seg_from = dm_list_item(dm_list_next(&dlv->segments, &seg_from->list), struct lv_segment);
		}

		if (!remove_seg_from_segs_using_this_lv(dlv, seg))
			return_0;

		lv_set_visible(dlv);

		/* Set component lv to error target */
		dlv->le_count = len;
		if (!replace_lv_with_error_segment(dlv))
			return_0;
	}

	/*
	 * Remove the one segment holding the image component areas
	 * from the top-level LV and add the new segments to it
	 */
	dm_list_del(&seg->list);
	dm_list_splice(&lv->segments, &new_segments);

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
	if (!seg_is_raid0(seg) ||
	    !segtype_is_striped(new_segtype))
		return 0;

	if (!archive(lv->vg))
		return_0;

	/* Move the AREA_PV areas across to new top-level segments */
	if (!__raid0_to_striped_retrieve_segments_and_lvs(lv, &removal_lvs)) {
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
static int __cmp_level(const struct segment_type *t1, const struct segment_type *t2)
{
	return strncmp(t1->name, t2->name, 5);
}

static int is_same_level(const struct segment_type *t1, const struct segment_type *t2)
{
	return !__cmp_level(t1, t2);
}

static int is_level_up(const struct segment_type *t1, const struct segment_type *t2)
{
	return __cmp_level(t1, t2) < 0;
}

/*
 * HM
 *
 * TAKEOVER: copes with all raid level switches aka takeover of @lv
 *
 * Overwrites the users "--type level_algorithm" request (e.g. --type raid6_zr)
 * with the appropriate, constraint one to allow for takeover.
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

/* Adjust @*segtype to the correct one for takeover */
static int __adjust_segtype_for_takeover(struct logical_volume *lv, struct segment_type **new_segtype)
{
	struct lv_segment *seg = first_seg(lv);
	struct segment_type *requested_segtype = *new_segtype;
	struct segment_type *segtype = *new_segtype;
	struct cmd_context *cmd = lv->vg->cmd;
	const char *segtype_name = NULL;

	/* Should be ensured by caller not to, but... */
	if (is_same_level(seg->segtype, segtype)) {
		log_error("No takeover requested");
		return 0;
	}

	/* Level up adjustments */
	if (is_level_up(seg->segtype, segtype)){
		/* To raid1 */
		if (segtype_is_raid1(segtype)) {
			/* From linear/raid0 */
			if (!seg_is_raid0(seg))
				return 0;
printf("%s %u segname=%s\n", __func__, __LINE__, segtype->name);

		/* To raid10 */
		} else if (segtype_is_raid10(segtype)) {
			/* From raid0 */
			if (!seg_is_raid0(seg))
				return 0;

		/* To raid4 */
		} else if (segtype_is_any_raid4(segtype)) {
			/* From raid0 */
			if (seg_is_raid0(seg))
				segtype_name = SEG_TYPE_NAME_RAID4_N;
			else
				return 0;

		/* To raid5 */
		} else if (segtype_is_any_raid5(segtype)) {
			/* From raid0/1/4 */
			/* HM FIXME: is raid4 supported by the raid5 personality? */
			if (seg_is_raid0(seg) ||
			    seg_is_raid4(seg))
				segtype_name = SEG_TYPE_NAME_RAID5_N;
			else if (seg_is_raid1(seg)) {
				if (seg->area_count != 2) {
					log_error("raid1 LV %s/%s has to have 2 devices for conversion; use \"lvconvert -m1 %s/%s\"",
						  lv->vg->name, lv->name, lv->vg->name, lv->name);
					return 0;
				}

				segtype_name = SEG_TYPE_NAME_RAID5_LS;
			} else
				return 0;

		/* To raid6 */
		} else if (segtype_is_any_raid6(segtype)) {
			/* From raid4/5_* ; raid4_n is not supported */
			if (seg_is_raid4(seg) ||
			    seg_is_raid5_0(seg))
				segtype_name = SEG_TYPE_NAME_RAID6_0_6;
			else if (seg_is_raid5_n(seg))
				segtype_name = SEG_TYPE_NAME_RAID6_N_6;
			else if (seg_is_raid5_ls(seg))
				segtype_name = SEG_TYPE_NAME_RAID6_LS_6;
			else if (seg_is_raid5_rs(seg))
				segtype_name = SEG_TYPE_NAME_RAID6_RS_6;
			else if (seg_is_raid5_la(seg))
				segtype_name = SEG_TYPE_NAME_RAID6_LA_6;
			else if (seg_is_raid5_ra(seg))
				segtype_name = SEG_TYPE_NAME_RAID6_RA_6;
			else
				return 0;
		} else
			return 0;

	/* Level down adjustments */
	} else {
		/* To raid0 */
		if (segtype_is_raid0(segtype)) {
			/* From raid 1, 10, 4_n, 5_n */
			if (!(seg_is_raid1(seg) ||
			      seg_is_raid10(seg) ||
			      seg_is_raid4_n(seg) ||
			      seg_is_raid5_n(seg)))
				return 0;

		/* To raid1 */
		} else if (segtype_is_raid1(segtype)) {
			/* From raid5* */
			if (!seg_is_any_raid5(seg) ||
			    seg->area_count != 3)
				return 0;

			// lv->le_count /= 2;

		/* To raid4 */
		} else if (segtype_is_any_raid4(segtype)) {
			/* From raid6_0_6 */
			if (seg_is_raid6_0_6(seg))
				segtype_name = SEG_TYPE_NAME_RAID4;
			/* From raid6_n_6 */
			else if (seg_is_raid6_n_6(seg))
				segtype_name = SEG_TYPE_NAME_RAID4_N;
			else
				return 0;

		/* To raid5* */
		} else if (segtype_is_any_raid5(segtype)) {
			/* From raid6_{ls,rs,la,ra}_6 */
			if (seg_is_raid6_ls_6(seg))
				segtype_name = SEG_TYPE_NAME_RAID5_LS;
			else if (seg_is_raid6_rs_6(seg))
				segtype_name = SEG_TYPE_NAME_RAID5_RS;
			else if (seg_is_raid6_la_6(seg))
				segtype_name = SEG_TYPE_NAME_RAID5_LA;
			else if (seg_is_raid6_ra_6(seg))
				segtype_name = SEG_TYPE_NAME_RAID5_RA;
			else if (seg_is_raid6_0_6(seg))
				segtype_name = SEG_TYPE_NAME_RAID5_0;
			else if (seg_is_raid6_n_6(seg))
				segtype_name = SEG_TYPE_NAME_RAID5_N;
			else
				return 0;

		} else
			return 0;

	}

	if (segtype_name)
		segtype = get_segtype_from_string(cmd, segtype_name);

	if (segtype_is_unknown(segtype))
		return 0;

	if (segtype != requested_segtype)
		log_warn("Adjusting segment type to %s on %s/%s.",
			 (segtype)->name, lv->vg->name, lv->name);

	*new_segtype = segtype;
	return 1;
}

/*
 * Reshape logical volume @lv by adding/removing stripes
 * (absolute new stripes given in @new_stripes), changing
 * stripe size set in @new_stripe_size.
 * Any PVs listed in @allocate_pvs will be tried for
 * allocation of new stripes.
 */
/* HM FIXME: CODEME TESTME */
static int __convert_reshape(struct logical_volume *lv,
			     struct segment_type *new_segtype,
			     const unsigned new_stripes,
			     const unsigned new_stripe_size,
		 	     struct dm_list *allocate_pvs)
{
	struct lv_segment *seg = first_seg(lv);
	unsigned old_stripes = seg->area_count - seg->segtype->parity_devs;

printf("segtype=%s new_segtype=%s\n", seg->segtype->name, new_segtype->name);
printf("stripes=%d new_stripes=%d\n", seg->area_count - seg->segtype->parity_devs, new_stripes);
printf("stripe_size=%d new_stripe_size=%d\n", seg->chunk_size, new_stripe_size);
	if (seg->segtype == new_segtype &&
	    seg->area_count - seg->segtype->parity_devs == new_stripes &&
	    seg->stripe_size == new_stripe_size) {
		log_error("Nothing to do");
		return 0;
	}

	if (!archive(lv->vg))
		return_0;

	if (old_stripes < new_stripes) {
printf("%s %u\n", __func__, __LINE__);
		if (!__lv_raid_change_image_count(lv, new_segtype, new_stripes + seg->segtype->parity_devs, allocate_pvs))
				return 0;
	}

	seg->segtype = new_segtype;

	return 1;
}

/*
 * Convert a RAID set to another RAID alogoritm or stripe size
 *
 * Returns: 1 on success, 0 on failure
 */
static int _convert_raid_to_raid(struct logical_volume *lv,
				 const struct segment_type *requested_segtype,
				 const unsigned new_stripes,
				 const unsigned new_stripe_size,
				 struct dm_list *allocate_pvs)
{
	int new_count;
	struct lv_segment *seg = first_seg(lv);
	struct segment_type *new_segtype = (struct segment_type *) requested_segtype;

#if 1
	/* HM FIXME: REMOVEME once supported */
	/* No stripes reduction so far */
	if (seg->area_count - seg->segtype->parity_devs > new_stripes) {
		log_error("No stripes reduction on %s supported so far", lv->name);
		return_0;
	}
#endif

	/* Check + apply stripe size change */
	if (new_stripe_size && !(new_stripe_size & (new_stripe_size - 1)) &&
	    seg->stripe_size != new_stripe_size) {
		if (seg_is_striped(seg) || seg_is_raid0(seg)) {
			log_error("Cannot change stripe size on \"%s\"", lv->name);
			return_0;
		}

		if (new_stripe_size > seg->region_size) {
			log_error("New stripe size for %s larger than region size", lv->name);
			return_0;
		}

		seg->stripe_size = new_stripe_size;
		log_debug_metadata("Setting new stripe size for %s", lv->name);
	}

	/* Staying on the same level -> reshape required to change stripes, stripe size or algorithm */
	if (is_same_level(seg->segtype, new_segtype)) {
		if (!__convert_reshape(lv, new_segtype, new_stripes, new_stripe_size, allocate_pvs))
			return 0;

		if (!lv_update_and_reload(lv))
			return_0;

		return 1;

	}

	/* Takeover (i.e. level switch) requested */
	if (!__adjust_segtype_for_takeover(lv, &new_segtype))
		return 0;

	/*
	 * HM
	 *
	 * Up takeover of raid levels
	 *
	 * In order to takeover the raid set level N to M (M > N) in @lv, all existing
	 * rimages in that set need to be paired with rmeta devs (if not yet present)
	 * to store superblocks* and bitmaps of the to be taken over raid4/raid5/raid6
	 * set plus another rimage/rmeta pair has to be allocated for dedicated xor/q.
	 */
	if (is_level_up(seg->segtype, new_segtype)) {
		new_count = seg->area_count + 1;

		/* Make sure to set default region size on takeover from raid0 */
		__init_region_size(lv);

		/*
		 * In case of raid1 -> raid5, takeover will run a degraded 2 disk raid5 set
		 * which will get an additional disk allocated afterwards and reloaded starting
		 * resynchronization to reach full redundance.
		 *
		 * FIXME: fully redundant raid5_ls set does not double-fold capacity after takeover from raid1 yet!!!
		 */
printf("%s %u\n", __func__, __LINE__);
		if (seg_is_raid1(seg)) {
			seg->segtype = new_segtype;
			seg->stripe_size = 64*2;

printf("%s %u\n", __func__, __LINE__);
			/* This causes the raid1 -> raid5 (2 disks) takeover */
			if (!lv_update_and_reload_origin(lv))
				return_0;

		}

		/*
		 * The top-level LV is being reloaded and the VG
		 * written and committed in the course of this call
		 */
		if (!__lv_raid_change_image_count(lv, new_segtype, new_count, allocate_pvs))
			return 0;


	/*
	 * HM
	 *
	 * Down takeover of raid levels
	 *
	 * In order to postprocess the takeover of a raid set from level M to M (M > N)
	 * in @lv, the last rimage/rmeta devs pair need to be droped in the metadata.
	 */
	} else {
		new_count = seg->area_count - 1;

		if (segtype_is_raid1(new_segtype)) {
			/* FIXME: delta_disks = -1 mandatory! */
			/* Reduce image count to 2 first */
			if (!__lv_raid_change_image_count(lv, NULL, new_count, allocate_pvs))
				return 0;

			seg->segtype = new_segtype;

			/* This causes the raid5 -> raid1 (2 disks) takeover */
			if (!lv_update_and_reload_origin(lv))
				return_0;

			return 1;
		}

		seg->segtype = new_segtype;

		/* This causes any !raid1 -> raid takeover */
		if (!lv_update_and_reload(lv))
			return_0;

		if (!__lv_raid_change_image_count(lv, new_segtype, new_count, allocate_pvs))
			return 0;
	} 

	return 1;
}
/******* END: raid <-> raid conversion *******/



/*
 * lv_raid_reshape
 * @lv
 * @new_segtype
 *
 * Convert an LV from one RAID type (or 'mirror' segtype) to another,
 * add/remove LVs from a RAID LV or change stripe sectors
 *
 * Returns: 1 on success, 0 on failure
 */
int lv_raid_reshape(struct logical_volume *lv,
		    const struct segment_type *new_segtype,
		    const unsigned new_stripes,
		    const unsigned new_stripe_size,
		    struct dm_list *allocate_pvs)
{
	struct lv_segment *seg = first_seg(lv);

printf("%s %u\n", __func__, __LINE__);
	if (!new_segtype) {
		log_error(INTERNAL_ERROR "New segtype not specified");
		return 0;
	}
printf("%s %u new_segtype=%s segtype=%s\n", __func__, __LINE__, new_segtype->name, seg->segtype->name);

	if (new_segtype == seg->segtype &&
	    new_stripes == seg->area_count - seg->segtype->parity_devs &&
	    new_stripe_size == seg->stripe_size) {
		log_error("Nothing to do");
		return 0;
	}

printf("%s %u\n", __func__, __LINE__);
	/* Given segtype of @lv */
	if (!seg_is_striped(seg) &&
	    !seg_is_mirror(seg) &&
	    !seg_is_raid(seg))
		goto err;
printf("%s %u\n", __func__, __LINE__);

	/* Requested segtype */
	if (!segtype_is_striped(new_segtype) &&
	    !segtype_is_raid(new_segtype))
		goto err;

	if (!_raid_in_sync(lv)) {
		log_error("Unable to convert %s/%s while it is not in-sync",
			  lv->vg->name, lv->name);
		return 0;
	}

	/* @lv has to be active locally */
	if (vg_is_clustered(lv->vg) && !lv_is_active_exclusive_locally(lv)) {
		log_error("%s/%s must be active exclusive locally to"
			  " perform this operation.", lv->vg->name, lv->name);
		return 0;
	}

	/* Mirror -> RAID1 conversion */
	if (seg_is_mirror(seg) && segtype_is_raid1(new_segtype))
		return _convert_mirror_to_raid1(lv, new_segtype);

	/* FIXME: support Mirror -> RAID1 conversion? */
	if (seg_is_raid1(seg) && segtype_is_mirror(new_segtype))
		return 0;

	/* Striped -> RAID0 conversion */
	if (seg_is_striped(seg) && segtype_is_raid0(new_segtype))
		return _convert_striped_to_raid0(lv, new_segtype, 1 /* -> alloc_metadata_devs */);
		// return _convert_striped_to_raid0(lv, new_segtype, segtype_is_raid0_with_rmeta(new_segtype));

	/* RAID0 <-> striped conversion */
	if (seg_is_raid0(seg) && segtype_is_striped(new_segtype))
		return _convert_raid0_to_striped(lv, new_segtype);

	/* All the rest of the raid conversions... */
	if ((seg_is_linear (seg) || seg_is_raid(seg)) && segtype_is_raid(new_segtype) &&
	    _convert_raid_to_raid(lv, new_segtype, new_stripes, new_stripe_size, allocate_pvs))
		return 1;

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
	if (!replace_lv_with_error_segment(rm_image))
		return_0;

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
	if (seg_is_raid0(raid_seg)) {
		log_error("Replacement of devices in %s/%s raid0 LV prohibited.",
			  lv->vg->name, lv->name);
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
			/* Adjust the new metadata LV name */
			lvl = dm_list_item(dm_list_first(&new_meta_lvs),
					   struct lv_list);
			dm_list_del(&lvl->list);
			if (!(tmp_names[s] = _generate_raid_name(lv, "rmeta", s)))
				return_0;
			if (!set_lv_segment_area_lv(raid_seg, s, lvl->lv, 0,
						    lvl->lv->status)) {
				log_error("Failed to add %s to %s",
					  lvl->lv->name, lv->name);
				return 0;
			}
			lv_set_hidden(lvl->lv);

			/* Adjust the new data LV name */
			lvl = dm_list_item(dm_list_first(&new_data_lvs),
					   struct lv_list);
			dm_list_del(&lvl->list);
			if (!(tmp_names[sd] = _generate_raid_name(lv, "rimage", s)))
				return_0;
			if (!set_lv_segment_area_lv(raid_seg, s, lvl->lv, 0,
						    lvl->lv->status)) {
				log_error("Failed to add %s to %s",
					  lvl->lv->name, lv->name);
				return 0;
			}
			lv_set_hidden(lvl->lv);
		} else
			tmp_names[s] = tmp_names[sd] = NULL;
	}

	if (!lv_update_and_reload_origin(lv))
		return_0;

	if (!__deactivate_and_remove_lvs(lv->vg, &old_lvs))
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
	for (s = 0; s < seg->area_count; s++) {
		if (!(seg_lv(seg, s)->status & PARTIAL_LV) &&
		    !(seg_metalv(seg, s)->status & PARTIAL_LV))
			continue;

		log_debug("Replacing %s and %s segments with error target",
			  seg_lv(seg, s)->name, seg_metalv(seg, s)->name);
		if (!replace_lv_with_error_segment(seg_lv(seg, s))) {
			log_error("Failed to replace %s's extents with error target.",
				  display_lvname(seg_lv(seg, s)));
			return 0;
		}
		if (!replace_lv_with_error_segment(seg_metalv(seg, s))) {
			log_error("Failed to replace %s's extents with error target.",
				  display_lvname(seg_metalv(seg, s)));
			return 0;
		}
	}

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

	if (failed_components && seg_is_raid0(raid_seg)) {
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
