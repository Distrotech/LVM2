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

void lvmnotify_send(struct cmd_context *cmd)
{
	GDBusProxy *con = NULL;
	GError *error = NULL;
	const char *vg_msg;
	const char *pv_msg;
	const char *cmd_name;
	GVariant *rc;
	int result = 0;

	if (!cmd->vg_notify && !cmd->pv_notify)
		return;

	con = g_dbus_proxy_new_for_bus_sync(G_BUS_TYPE_SYSTEM,
						  G_DBUS_PROXY_FLAGS_NONE,
						  NULL,
						  "com.redhat.lvmdbus1.Manager",
						  "/com/redhat/lvmdbus1/Manager",
						  "com.redhat.lvmdbus1.Manager",
						  NULL,
						  &error);
	if (!con && error) {
		log_debug("Failed to connect to dbus %d %s",
			  error->code, error->message);
		g_error_free(error);
		return;
	}

	cmd_name = get_cmd_name();
	vg_msg = cmd->vg_notify ? "vg_update" : "vg_none";
	pv_msg = cmd->pv_notify ? "pv_update" : "pv_none";

	rc = g_dbus_proxy_call_sync(con,
				    "ExternalEvent",
				    g_variant_new("(sss)", cmd_name, vg_msg, pv_msg),
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


	g_object_unref(con);
}

void set_vg_notify(struct cmd_context *cmd)
{
	cmd->vg_notify = 1;
}

void set_pv_notify(struct cmd_context *cmd)
{
	cmd->pv_notify = 1;
}

#else

void lvmnotify_send(struct cmd_context *cmd)
{
}

void set_vg_notify(struct cmd_context *cmd)
{
}

void set_pv_notify(struct cmd_context *cmd)
{
}

#endif

