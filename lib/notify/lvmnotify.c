/*
 * Copyright (C) 2015 Red Hat, Inc.
 *
 * This file is part of LVM2.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License v.2.1.
 */

#include "lib.h"
#include "toolcontext.h"
#include "metadata.h"
#include "lvmnotify.h"

#ifdef LVMNOTIFY_SUPPORT

#include <dbus/dbus.h>

#define NOTIFY_DBUS_PATH  "com.lvm"
#define NOTIFY_DBUS_IFACE "com/lvm"

static DBusConnection *_dbus_con = NULL;

void lvmnotify_init(struct cmd_context *cmd)
{
	if (!(_dbus_con = dbus_bus_get_private(DBUS_BUS_SYSTEM, NULL)))
		log_debug("Failed to connect to dbus");
}

void lvmnotify_exit(void)
{
	if (_dbus_con) {
		dbus_connection_close(_dbus_con);
		dbus_connection_unref(_dbus_con);
	}
	_dbus_con = NULL;
}

void notify_vg_update(struct volume_group *vg)
{
	DBusMessage *msg = NULL;

	if (_dbus_con && !dbus_connection_read_write(_dbus_con, 1)) {
		log_debug("Disconnected from dbus");
		notify_exit();
	}

	if (!_dbus_con)
		return;

	if (!(msg = dbus_message_new_signal(NOTIFY_DBUS_PATH,
					    NOTIFY_DBUS_IFACE,
					    "vg_update"))) {
		log_error("Failed to create dbus signal");
		goto out;
	}

	if (!dbus_message_append_args(msg,
				      DBUS_TYPE_STRING, &vg->name,
				      DBUS_TYPE_INT32, &vg->seqno,
				      DBUS_TYPE_INVALID)) {
		log_error("Failed to append args to dbus signal");
		goto out;
	}

	dbus_connection_send(_dbus_con, msg, NULL);
	dbus_connection_flush(_dbus_con);

out:
	if (msg)
		dbus_message_unref(msg);
}

#else

void lvmnotify_init(struct cmd_context *cmd)
{
}

void lvmnotify_exit(void)
{
}

void notify_vg_update(struct volume_group *vg)
{
}

#endif

