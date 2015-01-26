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

#include <assert.h>

#include "lib.h"
#include "lvmpolld-client.h"
#include "daemon-io.h"
#include "metadata-exported.h"
#include "polling_ops.h"
#include "toolcontext.h"

struct progress_info {
	unsigned error:1;
	unsigned finished:1;
	int cmd_signal;
	int cmd_retcode;
	dm_percent_t percents;
};

static int _lvmpolld_use;
static int _lvmpolld_connected;
static const char* _lvmpolld_socket;

static daemon_handle _lvmpolld = { .error = 0 };

static daemon_handle _lvmpolld_connect(const char *socket)
{
	daemon_info lvmpolld_info = {
		.path = "lvmpolld",
		.socket = socket ?: LVMPOLLD_SOCKET,
		.protocol = "lvmpolld",
		.protocol_version = 1
	};

	return daemon_open(lvmpolld_info);
}

void lvmpolld_set_active(int active)
{
	_lvmpolld_use = active;
}

void lvmpolld_set_socket(const char *socket)
{
	_lvmpolld_socket = socket;
}

int lvmpolld_use(void)
{
	if (!_lvmpolld_use)
		return 0;

	if (!_lvmpolld_connected && !_lvmpolld.error) {
		_lvmpolld = _lvmpolld_connect(_lvmpolld_socket);
		_lvmpolld_connected = _lvmpolld.socket_fd >= 0;
	}

	return _lvmpolld_connected;
}

void lvmpolld_disconnect(void)
{
	if (_lvmpolld_connected) {
		daemon_close(_lvmpolld);
		_lvmpolld_connected = 0;
	}
}

static struct progress_info _request_progress_info(const char *uuid, unsigned abort)
{
	daemon_reply repl;
	struct progress_info ret = { .error = 1, .finished = 1 };
	daemon_request req = daemon_request_make("progress_info");

	if (!daemon_request_extend(req, "lvid = %s", uuid,
					"abort = %d", abort,
					NULL)) {
		log_error("failed to create 'progress_info' request");
		goto out_req;
	}

	repl = daemon_send(_lvmpolld, req);
	if (repl.error) {
		log_error("I/O error while communicating with lvmpolld");
		goto out_repl;
	}

	if (!strcmp(daemon_reply_str(repl, "response", ""), "inprogress")) {
		ret.percents = (dm_percent_t) daemon_reply_int(repl, "data", 0);
		ret.finished = 0;
		ret.error = 0;
	} else if (!strcmp(daemon_reply_str(repl, "response", ""), "finished")) {
		ret.percents = (dm_percent_t) daemon_reply_int(repl, "data", 0);
		if (!strcmp(daemon_reply_str(repl, "reason", ""), "signal"))
			ret.cmd_signal = daemon_reply_int(repl, "value", 0);
		else
			ret.cmd_retcode = daemon_reply_int(repl, "value", -1);
		ret.error = 0;
	} else if (!strcmp(daemon_reply_str(repl, "response", ""), "not_found")) {
		log_verbose("lvmpolld: no polling operation in progress regarding LV %s", uuid);
		ret.error = 0;
	} else {
		if (!strcmp(daemon_reply_str(repl, "response", ""), "failed"))
			log_verbose("lvmpolld: internal error occured. See lvmpolld log file");
		log_error("failed to receive progress data: The reason: %s",
			  daemon_reply_str(repl, "reason", "<empty>"));
	}
out_repl:
	daemon_reply_destroy(repl);
out_req:
	daemon_request_destroy(req);

	return ret;
}

/*
 * interval in seconds long
 * enough for more than a year
 * of waiting
 */
#define INTERV_SIZE 10

static int _process_poll_init(const char *cmd_line, const char *poll_type,
			      const char *vgname, const char *uuid,
			      unsigned background, unsigned interval,
			      unsigned abort)
{
	char *str;
	daemon_reply rep;
	daemon_request req;
	int r = 0; 

	str = dm_malloc(INTERV_SIZE * sizeof(char));
	if (!str)
		return r;

	if (snprintf(str, INTERV_SIZE, "%u", interval) >= INTERV_SIZE) {
		log_warn("interval string conversion got truncated");
		str[INTERV_SIZE - 1] = '\0';
	}

	req = daemon_request_make(poll_type);
	if (!daemon_request_extend(req, "lvid = %s", uuid,
					"vgname = %s", vgname,
					"interval = %s", str,
					"background = %d", background,
					"abort = %d", abort,
					"cmdline = %s", cmd_line,
					NULL)) {
		log_error("failed to create %s request", poll_type);
		goto out_req;
	}

	rep = daemon_send(_lvmpolld, req);

	if (!strcmp(daemon_reply_str(rep, "response", ""), "OK"))
		/* OK */
		r = 1;
	else {
		if (rep.error)
			log_error("failed to process request with error %s (errno: %d)",
				  strerror(rep.error), rep.error);
		else
			log_error("failed to initialise lvmpolld operation: %s. The reason: %s",
				  poll_type, daemon_reply_str(rep, "reason", ""));
	}

	daemon_reply_destroy(rep);
out_req:
	daemon_request_destroy(req);
	dm_free(str);

	return r;
}

int lvmpolld_poll_init(const char *cmd_line, const char *vgname, const char *uuid,
		       unsigned background, uint64_t lv_type, unsigned interval,
		       unsigned abort)
{
	int r = 0;

	if (!uuid) {
		log_error(INTERNAL_ERROR "use of lvmpolld requires uuid set");
		return 0;
	}

	if (!vgname) {
		log_error(INTERNAL_ERROR "use of lvmpolld requires vgname set");
		return 0;
	}

	if (lv_type & PVMOVE) {
		log_verbose("lvmpolld: pvmove%s", abort ? "--abort" : "");
		r =  _process_poll_init(cmd_line, PVMOVE_POLL, vgname, uuid, background, interval, abort);
	} else if (lv_type & CONVERTING) {
		log_verbose("lvmpolld: convert mirror");
		r =  _process_poll_init(cmd_line, CONVERT_POLL, vgname, uuid, background, interval, 0);
	} else if (lv_type & MERGING) {
		if (lv_type & SNAPSHOT) {
		log_verbose("lvmpolld: Merge snapshot");
			r =  _process_poll_init(cmd_line, MERGE_POLL, vgname, uuid, background, interval, 0);
		}
		else if (lv_type & THIN_VOLUME) {
			log_verbose("lvmpolld: Merge thin snapshot");
			r = _process_poll_init(cmd_line, MERGE_THIN_POLL, vgname, uuid, background, interval, 0);
		}
		else {
			log_error(INTERNAL_ERROR "Unsupported poll operation");
		}
	} else
		log_error(INTERNAL_ERROR "Unsupported poll operation");

	return r;
}

int lvmpolld_request_info(const char *uuid, const char *name,
			  const char *progress_title, unsigned abort,
			  uint64_t lv_type, unsigned *finished)
{
	struct progress_info info;
	int ret = 0;

	*finished = 1;

	if (!uuid) {
		log_error(INTERNAL_ERROR "use of lvmpolld requires uuid being set");
		return 0;
	}

	info = _request_progress_info(uuid, abort);
	*finished = info.finished;

	if (info.error)
		return 0;

	/* 
	 * not interested in progress info with pvmove --abort or
	 * while converting thin snapshot
	 */ 
	if (!abort && !(lv_type & THIN_VOLUME))
		log_print_unless_silent("%s: %s: %.1f%%", name,
						progress_title, dm_percent_to_float(info.percents));

	if (info.finished) {
		if (info.cmd_signal)
			log_error("lvmpolld: polling command got terminated by signal (%d)",
				  info.cmd_signal);
		else if (info.cmd_retcode)
			log_error("lvmpolld: polling command exited with return code: %d",
				  info.cmd_retcode);
		else  {
			log_verbose("lvmpolld: polling finished successfully");
			ret = 1;
		}
	} else
		ret = 1;

	return ret;
}
