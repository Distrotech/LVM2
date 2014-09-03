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

#ifndef _LVM_DEV_AUX_STATUS_H
#define _LVM_DEV_AUX_STATUS_H

typedef enum {
	DEV_AUX_STATUS_SRC_NATIVE,
	DEV_AUX_STATUS_SRC_UDEV
} dev_aux_status_source_t;

struct dev_aux_status {
	dev_aux_status_source_t source;
	void *handle;
};

const char *dev_aux_status_source_name(dev_aux_status_source_t);
const char *dev_aux_status_source_name_used(struct dev_aux_status *status);
int dev_aux_status_use_native(struct dev_aux_status *status, const char *dev_name);

#endif
