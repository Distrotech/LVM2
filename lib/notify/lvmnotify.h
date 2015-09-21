/*
 * Copyright (C) 2015 Red Hat, Inc.
 *
 * This file is part of LVM2.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License v.2.1.
 */

#ifndef _LVMNOTIFY_H
#define _LVMNOTIFY_H

void lvmnotify_init(struct cmd_context *cmd);
void lvmnotify_exit(void);

void notify_vg_update(struct volume_group *vg);

#endif

