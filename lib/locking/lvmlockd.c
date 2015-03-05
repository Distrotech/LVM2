/*
 * Copyright (C) 2014 Red Hat, Inc.
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
#include "segtype.h"
#include "lvmetad.h"
#include "lvmlockd.h"
#include "lvmcache.h"
#include "lvmlockd-client.h"

static daemon_handle _lvmlockd;
static int _lvmlockd_active;
static int _lvmlockd_connected;

static const char *_lvmlockd_socket = NULL;
static struct cmd_context *_lvmlockd_cmd = NULL;

void lvmlockd_disconnect(void)
{
	if (_lvmlockd_connected)
		daemon_close(_lvmlockd);
	_lvmlockd_connected = 0;
	_lvmlockd_cmd = NULL;
}

void lvmlockd_init(struct cmd_context *cmd)
{
	if (!_lvmlockd_active && !access(LVMLOCKD_PIDFILE, F_OK))
		log_warn("lvmlockd is not running.");
	if (!_lvmlockd_active)
		return;
	_lvmlockd_cmd = cmd;
}

static void _lvmlockd_connect(void)
{
	if (!_lvmlockd_active || !_lvmlockd_socket || _lvmlockd_connected)
		return;

	_lvmlockd = lvmlockd_open(_lvmlockd_socket);

	if (_lvmlockd.socket_fd >= 0 && !_lvmlockd.error) {
		log_debug("Successfully connected to lvmlockd on fd %d.",
			  _lvmlockd.socket_fd);
		_lvmlockd_connected = 1;
	}
}

void lvmlockd_connect_or_warn(void)
{
	if (!_lvmlockd_active || _lvmlockd_connected)
		return;

	_lvmlockd_connect();

	if (!_lvmlockd_connected) {
		log_warn("Failed to connect to lvmlockd: %s.",
			 strerror(_lvmlockd.error));
	}
}

/*
 * in command setup:
 *
 * 1. if use_lvmlockd is set in config,
 *    lvmlockd_set_active() sets _lvmlockd_active = 1
 *
 * 2. lvmlockd_init() sees _lvmlockd_active, and sets _lvmlockd_cmd
 *
 * 3. lvmlockd_connect_or_warn()/_lvmlockd_connect() see _lvmlockd_active,
 *    create connection and if successful set _lvmlockd_connected = 1
 *
 * in command processing:
 *
 * 1. lock function calls lvmlockd_connected() which returns
 *    _lvmlockd_connected
 *
 * 2. if lvmlockd_connected() returns 0, lock function fails
 */

int lvmlockd_connected(void)
{
	if (_lvmlockd_connected)
		return 1;

	return 0;
}

void lvmlockd_set_active(int active)
{
	_lvmlockd_active = active;
}

void lvmlockd_set_socket(const char *sock)
{
	_lvmlockd_socket = sock;
}

