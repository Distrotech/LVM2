/*
 * Copyright (C) 2014 Red Hat, Inc.
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

#ifndef _LVM_LVMPOLLD_CLIENT_H
#define _LVM_LVMPOLLD_CLIENT_H

#include "daemon-client.h"

struct cmd_context;

#define LVMPOLLD_SOCKET DEFAULT_RUN_DIR "/lvmpolld.socket"

/* daemon_handle lvmpolld_connect(const char *socket);*/
void lvmpolld_disconnect(void);

int lvmpolld_poll_init(const struct cmd_context *cmd, const char *vgname,
		       const char *uuid, unsigned background, uint64_t lv_type,
		       unsigned interval, unsigned abort);

int lvmpolld_request_info(const char *uuid, const char *name,
			  const char *progress_title, unsigned abort,
			  uint64_t lv_type, unsigned *finished);

int lvmpolld_use(void);

void lvmpolld_set_active(int active);

void lvmpolld_set_socket(const char *socket);

#endif /* _LVM_LVMPOLLD_CLIENT_H */
