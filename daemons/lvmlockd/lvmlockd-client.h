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

#endif
