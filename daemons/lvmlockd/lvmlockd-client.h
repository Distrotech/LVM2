/*
 * Copyright (C) 2014 Red Hat, Inc.
 *
 * This file is part of LVM2.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License v.2.1.
 */

#ifndef _LVM_LVMLOCKD_CLIENT_H
#define _LVM_LVMLOCKD_CLIENT_H

#include "daemon-client.h"

/* Wrappers to open/close connection */

static inline daemon_handle lvmlockd_open(const char *socket)
{
	daemon_info lvmlockd_info = {
		.path = "lvmlockd",
		.socket = socket ?: DEFAULT_RUN_DIR "/lvmlockd.socket",
		.protocol = "lvmlockd",
		.protocol_version = 1,
		.autostart = 0
	};

	return daemon_open(lvmlockd_info);
}

static inline void lvmlockd_close(daemon_handle h)
{
	return daemon_close(h);
}

/*
 * Also see lvmlockd-sanlock GL_LOCK_BEGIN, VG_LOCK_BEGIN, LV_LOCK_BEGIN.
 * gl lock at sanlock lease area 65
 * vg lock at sanlock lease area 66
 * lv locks begin at sanlock lease area 67
 *
 * LV_LOCK_BEGIN + MAX_LVS_IN_VG = sanlock lease areas required
 * with 512 byte sectors, each lease area is 1MB
 * with 4k byte sectors, each lease area is 8MB (use this for sizing)
 *
 * 66+190 = 256 sanlock lease areas,
 * so we need 256 * 8MB = 2GB lock lv size to hold 190 lv leases.
 */
#define LVMLOCKD_SANLOCK_MAX_LVS_IN_VG 190
#define LVMLOCKD_SANLOCK_LV_SIZE       2147483648 /* 2GB */

#endif
