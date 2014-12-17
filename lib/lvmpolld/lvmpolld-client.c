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

/* TODO: if we find this reasonable, let's move the code to libdaemon instead */
static daemon_reply _read_single_response(daemon_request rq)
{
	daemon_reply repl = { 0 };
	buffer_init(&repl.buffer);

	if (!dm_config_write_node(rq.cft->root, buffer_line, &repl.buffer)) {
		repl.error = ENOMEM;
		return repl;
	}

	assert(repl.buffer.mem);
	if (buffer_read(_lvmpolld.socket_fd, &repl.buffer)) {
		repl.cft = dm_config_from_string(repl.buffer.mem);
		if (!repl.cft)
			repl.error = EPROTO;
	} else
		repl.error = errno;

	return repl;
}

static daemon_reply _receive_data_stream(daemon_request req, const char *name, const char *progress_title)
{
	daemon_reply repl = daemon_send(_lvmpolld, req);

	while (!strcmp(daemon_reply_str(repl, "response", ""), "progress_data"))
	{
		log_print_unless_silent("%s: %s: %.1f%%", name, progress_title, dm_percent_to_float((dm_percent_t)daemon_reply_int(repl, "data", 0)));

		daemon_reply_destroy(repl);

		repl = _read_single_response(req);
	}

	return repl;
}

static int _receive_progress_data(const char *name, const char *uuid, const char *progress_title)
{
	daemon_reply repl;
	int r = 0;
	daemon_request req = daemon_request_make("progress_data_single");

	if (!daemon_request_extend(req, "lvid = %s", uuid,
					NULL)) {
		log_error("failed to create 'progress_data_single' request");
		goto out_req;
	}

	repl = daemon_send(_lvmpolld, req);

	if (repl.error)
		log_error("I/O error while communicating with lvmpolld");
	else if (!strcmp(daemon_reply_str(repl, "response", ""), "progress_data")) {
		/* this is only response type meaning the polling is still acitve */
		log_print_unless_silent("%s: %s: %.1f%%", name, progress_title,
					dm_percent_to_float((dm_percent_t)daemon_reply_int(repl, "data", 0)));
		r = 1;
	} else if (!strcmp(daemon_reply_str(repl, "response", ""), "failed") &&
		 !strcmp(daemon_reply_str(repl, "reason", ""), "not found")) {
		/* the polled LV is just gone. be silent? */
		log_verbose("Polling already finished");
	} else
		log_error("Unexpected reply, reason: %s",
			  daemon_reply_str(repl, "reason", ""));

	daemon_reply_destroy(repl);
out_req:
	daemon_request_destroy(req);

	return r;
}

/*
 * interval in seconds long
 * enough for year of waiting
 */
#define INTERV_SIZE 10

static int _process_request(const struct cmd_context *cmd, const char *poll_type, const char *name,
			    const char *uuid, unsigned stream_data,
			    const char *progress_title, unsigned interval,
			    unsigned abort)
{
	char *str;
	daemon_reply rep;
	daemon_request req;
	int r = 0; 

	str = dm_malloc(INTERV_SIZE * sizeof(char));
	if (!str)
		goto out;

	if (snprintf(str, INTERV_SIZE, "%u", interval) >= INTERV_SIZE) {
		log_warn("interval string conversion got truncated");
		str[INTERV_SIZE - 1] = '\0';
	}

	req = daemon_request_make(poll_type);
	if (!daemon_request_extend(req, "lvid = %s", uuid,
					"interval = %s", str,
					"stream_data = %d", stream_data,
					"abort = %d", abort,
					"cmdline = %s", cmd->cmd_line,
					NULL)) {
		log_error("failed to create %s request", poll_type);
		goto out_req;
	}

	if (stream_data)
		rep = _receive_data_stream(req, name, progress_title);
	else
		rep = daemon_send(_lvmpolld, req);

	/* TODO: audit me */
	if (strcmp(daemon_reply_str(rep, "response", ""), "OK")) {
		if (rep.error)
			log_error("failed to process request with error %s (errno: %d)",
				  strerror(rep.error), rep.error);
		else {
			if (!strcmp(daemon_reply_str(rep, "reason", ""), "ret_code"))
				log_error("lvmpolld: polling command '%s' exited with ret_code: %" PRIu64,
					  poll_type, daemon_reply_int(rep, "value", 103));
			else if (!strcmp(daemon_reply_str(rep, "reason", ""), "signal"))
				log_error("lvmpolld: polling command '%s' got terminated by signal (%" PRIu64 ")",
					  poll_type, daemon_reply_int(rep, "value", 0));
			else
				log_error("failed to %s. The reason: %s",
					  stream_data ? "receive progress data stream" : "initiate operation polling",
					  daemon_reply_str(rep, "reason", ""));
		}
		goto out_rep;
	}

	r = 1;

out_rep:
	daemon_reply_destroy(rep);
out_req:
	daemon_request_destroy(req);
	dm_free(str);
out:

	return r;
}

int lvmpolld(struct cmd_context *cmd, const char *name, const char *uuid, unsigned background,
	     uint64_t lv_type, const char *progress_title, unsigned stream_data,
	     unsigned interval, unsigned abort)
{
	int r = 0;

	if (!uuid) {
		log_error(INTERNAL_ERROR "use of lvmpolld requires uuid being set");
		return 0;
	}

	if (abort) {
		log_warn("Enforcing background due to requested abort");
		background = 1;
	}

	if (background && stream_data) {
		stream_data = 0;
		log_warn("Disabling progress data stream due to background mode already set");
	}

	if (lv_type & PVMOVE)
		r =  _process_request(cmd, PVMOVE_POLL, name, uuid, stream_data, progress_title, interval, abort);
	else if (lv_type & CONVERTING)
		r =  _process_request(cmd, CONVERT_POLL, name, uuid, stream_data, progress_title, interval, 0);
	else if (lv_type & MERGING) {
		if (lv_type & SNAPSHOT) {
		log_verbose("Merging snapshot");
			r =  _process_request(cmd, MERGE_POLL, name, uuid, stream_data, progress_title, interval, 0);
		}
		else if (lv_type & THIN_VOLUME) {
			log_verbose("Merging thin snapshot");
			r = _process_request(cmd, MERGE_THIN_POLL, name, uuid, stream_data, progress_title, interval, 0);
		}
		else {
			log_error(INTERNAL_ERROR "Unsupported poll operation");
		}
	} else
		log_error(INTERNAL_ERROR "Unsupported poll operation");

	/* successful init required */
	if (r && !background && !stream_data)
		r = _receive_progress_data(name, uuid, progress_title);

	return r;
}
