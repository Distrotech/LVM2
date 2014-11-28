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

int swap_lv_identifiers(struct cmd_context *cmd, struct logical_volume *a,
			struct logical_volume *b);

int finish_thin_merge(struct cmd_context *cmd, struct logical_volume *merge_lv,
		      struct logical_volume *lv);

int finish_lvconvert_mirror(struct cmd_context *cmd, struct volume_group *vg,
			    struct logical_volume *lv,
			    struct dm_list *lvs_changed __attribute__((unused)));

int finish_lvconvert_merge(struct cmd_context *cmd, struct volume_group *vg,
			   struct logical_volume *lv,
			   struct dm_list *lvs_changed __attribute__((unused)));

progress_t poll_merge_progress(struct cmd_context *cmd,	struct logical_volume *lv,
			       const char *name __attribute__((unused)),
			       struct daemon_parms *parms);

progress_t poll_thin_merge_progress(struct cmd_context *cmd,
				     struct logical_volume *lv,
				     const char *name __attribute__((unused)),
				     struct daemon_parms *parms);
