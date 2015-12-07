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
#include <systemd/sd-bus.h>

void lvmnotify_send(struct cmd_context *cmd)
{
	sd_bus *bus = NULL;
	sd_bus_message *m = NULL;
	sd_bus_error error = SD_BUS_ERROR_NULL;
	const char *path;
	const char *cmd_name;
	int ret;

	if (!cmd->vg_notify && !cmd->lv_notify && !cmd->pv_notify)
		return;

	cmd_name = get_cmd_name();

	ret = sd_bus_open_system(&bus);
	if (ret < 0) {
		log_debug("Failed to connect to dbus: %d", ret);
		return;
	}

	ret = sd_bus_call_method(bus,
				 "com.redhat.lvmdbus1.Manager",
				 "/com/redhat/lvmdbus1/Manager",
				 "com.redhat.lvmdbus1.Manager",
				 "ExternalEvent",
				 &error,
				 NULL,
				 "s",
				 cmd_name);

	if (ret < 0) {
		log_debug("Failed to issue dbus method call: %s", error.message);
		goto out;
	}

	ret = sd_bus_message_read(m, "o", &path);
	if (ret < 0)
		log_debug("Failed to parse dbus response message: %d", ret);

	sd_bus_error_free(&error);
	sd_bus_message_unref(m);
	sd_bus_unref(bus);
}

void set_vg_notify(struct cmd_context *cmd)
{
	cmd->vg_notify = 1;
}

void set_lv_notify(struct cmd_context *cmd)
{
	cmd->lv_notify = 1;
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

void set_lv_notify(struct cmd_context *cmd)
{
}

void set_pv_notify(struct cmd_context *cmd)
{
}

#endif

