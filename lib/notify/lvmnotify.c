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

#ifdef NOTIFYDBUS_SUPPORT
#include <gio/gio.h>

static GDBusProxy *_dbus_con = NULL;

int lvmnotify_init(struct cmd_context *cmd)
{
	GError *error = NULL;

	_dbus_con = g_dbus_proxy_new_for_bus_sync(G_BUS_TYPE_SYSTEM,
						  G_DBUS_PROXY_FLAGS_NONE,
						  NULL,
						  "com.lvm1",
						  "/com/lvm1/Manager",
						  "com.lvm1.Manager",
						  NULL,
						  &error);
	if (!_dbus_con && error) {
		log_debug("Failed to connect to dbus %d %s",
			  error->code, error->message);
		g_error_free(error);
		return 0;
	}
	return 1;
}

void lvmnotify_exit(void)
{
	if (_dbus_con) {
		g_object_unref(_dbus_con);
		_dbus_con = NULL;
	}
}

void notify_vg_update(struct volume_group *vg)
{
	char uuid[64] __attribute__((aligned(8)));
	GError *error = NULL;
	GVariant *rc;
	int result = 0;

	if (!_dbus_con)
		return;

	if (!id_write_format(&vg->id, uuid, sizeof(uuid)))
		return;

	rc = g_dbus_proxy_call_sync(_dbus_con,
				    "ExternalEvent",
				    g_variant_new("(sssu)",
						  "vg_update",
						  vg->name,
						  uuid,
						  vg->seqno),
				    G_DBUS_CALL_FLAGS_NONE,
				    -1, NULL, &error);

	if (rc) {
		g_variant_get(rc, "(i)", &result);
		if (result)
			log_debug("Error from sending dbus notification %d", result);
		g_variant_unref(rc);

	} else if (error) {
		if (error->code != 2)
			log_debug("Failed to send dbus notification %d %s", error->code, error->message);
		g_error_free(error);

	} else {
		log_debug("Undefined dbus result");
	}
}

#else

int lvmnotify_init(struct cmd_context *cmd)
{
}

void lvmnotify_exit(void)
{
}

void notify_vg_update(struct volume_group *vg)
{
}

#endif

