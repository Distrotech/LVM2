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
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <poll.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <wait.h>
#include <sys/types.h>

/*
 * FIXME: should be removed together with code supposed
 * to become general (single_read/single_write). or shouldn't?
 */
#include "daemon-io.h"
/* */
#include "configure.h"
#include "daemon-server.h"
#include "daemon-log.h"
#include "config-util.h"
#include "lvmpolld-data-utils.h"
#include "polling_ops.h"
#include "lvm-version.h"

#define LVMPOLLD_SOCKET DEFAULT_RUN_DIR "/lvmpolld.socket"

/* 
 * FIXME: I don't follow the logic behind prefix variables in lvm2
 * configure script
 */
#define CMD_PATH "/home/okozina/testik/usr/sbin"
#define VGPOLL_CMD_BIN CMD_PATH  "/vgpoll"
/* extract this info from autoconf/automake files */

static const char *const const polling_ops[] = { [PVMOVE] = PVMOVE_POLL,
						 [CONVERT] = CONVERT_POLL,
						 [MERGE] = MERGE_POLL,
						 [MERGE_THIN] = MERGE_THIN_POLL };

typedef struct lvmpolld_state {
	log_state *log;
	const char *log_config;

	/*
	 * maps lvid to internal lvmpolld LV representation
	 *
	 * only thread responsible for polling of lvm command
	 * should remove the pdlv from hash_table
	 *
	 * progress threads should only manipulate use_count of
	 * pdlv structure
	 */
	struct dm_hash_table *lvid_to_pdlv;

	struct {
		pthread_mutex_t lvid_to_pdlv;
	} lock;
} lvmpolld_state_t;

static void usage(const char *prog, FILE *file)
{
	fprintf(file, "Usage:\n"
		"%s [-V] [-h] [-f] [-l {all|wire|debug}] [-s path]\n\n"
		"   -V       Show version info\n"
		"   -h       Show this help information\n"
		"   -f       Don't fork, run in the foreground\n"
		"   -l       Logging message level (-l {all|wire|debug})\n"
		"   -p       Set path to the pidfile\n"
		"   -s       Set path to the socket to listen on\n\n", prog);
}

static int init(struct daemon_state *s)
{
	lvmpolld_state_t *ls = s->private;
	ls->log = s->log;

	if (!daemon_log_parse(ls->log, DAEMON_LOG_OUTLET_STDERR, ls->log_config, 1))
		return 0;

	ls->lvid_to_pdlv = dm_hash_create(32);

	pthread_mutex_init(&ls->lock.lvid_to_pdlv, NULL);

	return 1;
}

static int fini(struct daemon_state *s)
{
	lvmpolld_state_t *ls = s->private;

	pthread_mutex_destroy(&ls->lock.lvid_to_pdlv);

	dm_hash_destroy(ls->lvid_to_pdlv);

	return 1;
}

static void lock_lvid_to_pdlv(lvmpolld_state_t *ls)
{
	pthread_mutex_lock(&ls->lock.lvid_to_pdlv);
}

static void unlock_lvid_to_pdlv(lvmpolld_state_t *ls)
{
	pthread_mutex_unlock(&ls->lock.lvid_to_pdlv);
}

static response reply_fail(const char *reason)
{
	return daemon_reply_simple("failed", "reason = %s", reason, NULL);
}

static int read_single_line(char **line, size_t *lsize, FILE *file)
{
	ssize_t r = getline(line, lsize, file);

	if (r > 0 && *(*line + r - 1) == '\n')
		*(*line + r - 1) = '\0';

	return (r > 0);
}

static inline const char *get_keyword(const enum poll_type type)
{
	switch (type) {
	case PVMOVE:
		return "Moved";
	case CONVERT:
		return "Converted";
	case MERGE:
	case MERGE_THIN:
		return "Merged";
	default:
		return NULL;
	}
}

static dm_percent_t parse_line_for_percents(lvmpolld_lv_t *pdlv, const char *line)
{
	char *endptr, *keyw, *nr;
	double d;

	if (!(keyw = strstr(line, get_keyword(pdlv->type))) || keyw == line
	    || !strchr(keyw, DM_PERCENT_CHAR))
		return DM_PERCENT_FAILED;

	nr = strpbrk(keyw, "+-0123456789");
	if (!nr)
		return DM_PERCENT_FAILED;

	d = strtod(nr, &endptr);
	if (nr == endptr)
		return DM_PERCENT_FAILED;
	if (d > 100.0)
		return DM_PERCENT_INVALID;

	return dm_make_percent((uint64_t)(d * DM_PERCENT_1), DM_PERCENT_100);
}

static void poll_for_output(lvmpolld_lv_t *pdlv, pid_t child, int outfd, int errfd)
{
	FILE *fout = NULL, *ferr = NULL;

	dm_percent_t prcs;
	char *line = NULL;
	int ch_stat, r, wait4, fds_count;
	lvmpolld_cmd_stat_t cmd_state = { .ret_code = -1, .signal = 0 };
	size_t lsize;
	struct pollfd fds[] = { { .fd = outfd, .events = POLLIN },
				{ .fd = errfd, .events = POLLIN } };

	if ((fout = fdopen(outfd, "r")) == NULL)
			goto out;
	if ((ferr = fdopen(errfd, "r")) == NULL)
			goto out;

	fds_count = 2;
	while (1) {
		/* TODO: timeout should be multiple of interval */
		r = poll(fds, 2, 10000);
		DEBUGLOG(pdlv->ls, "%s: %s %d", "LVMPOLLD", "poll() returned", r);
		if (!r) {
			/* TODO: perhaps let it timeout 3 times */
			WARN(pdlv->ls, "%s: %s (PID %d) %s", "LVMPOLLD",
			     "polling for output of lvm cmd", child, "timed out");
			goto out;
		} else if (r < 0) {
			if (errno == EINTR) /* TODO: interrupt handler? */
				continue;

			ERROR(pdlv->ls, "%s: %s (PID %d) %s (%d): %s",
			      "LVMPOLLD", "poll() for LVM2 cmd", child,
			      "ended with error",  errno, "(strerror())");
			break;
			/* FIXME: handle interrupts */
		}

		/* handle fds structures */
		if (fds[0].revents & POLLIN) {
			DEBUGLOG(pdlv->ls, "%s: %s", "LVMPOLLD", "caught input data in STDOUT");
			if (read_single_line(&line, &lsize, fout)) {
				/* TODO: parse progress data */
				INFO(pdlv->ls, "%s: PID %d: %s: '%s'", "LVM2CMD", child, "STDOUT", line);
				prcs = parse_line_for_percents(pdlv, line);
				if (prcs < DM_PERCENT_0)
					WARN(pdlv->ls, "%s: %s", "LVMPOLLD", "parsing percentage from lvm2 command failed");
				else {
					DEBUGLOG(pdlv->ls, "%s: %s (%f)", "LVMPOLLD", "parsed this", dm_percent_to_float(prcs));
					pdlv_set_percents(pdlv, prcs);
				}
			}
			else {
				/* TODO: remove this section. May not happen */
				DEBUGLOG(pdlv->ls, "%s: %s", "LVMPOLLD", "got EOF while reading STDOUT");
				fds[0].fd = -1;
				fds_count--;
			}
		} else if (fds[0].revents & POLLHUP) {
			DEBUGLOG(pdlv->ls, "%s: %s", "LVMPOLLD", "caught POLLHUP");
			fds[0].fd = -1;
			fds_count--;
		} else if (fds[0].revents) {
			WARN(pdlv->ls, "%s: %s", "LVMPOLLD", "poll for command's STDOUT failed");
			fds[0].fd = -1;
			fds_count--;
		}
		/* TODO: why do I keep getting POLLHUP event for STDERR? */
		/* ANSWER: because you had a stupid bug there:) */
		if (fds[1].revents & POLLIN) {
			DEBUGLOG(pdlv->ls, "%s: %s", "LVMPOLLD", "caught input data in STDERR");
			if (read_single_line(&line, &lsize, ferr))
				INFO(pdlv->ls, "%s: PID %d: %s: '%s'", "LVM2CMD", child, "STDERR", line);
			else {
				/* TODO: remove this section. May not happen */
				DEBUGLOG(pdlv->ls, "%s: %s", "LVMPOLLD", "got EOF while reading STDERR");
				fds[1].fd = -1;
				fds_count--;
			}
		} else if (fds[1].revents & POLLHUP) {
			DEBUGLOG(pdlv->ls, "%s: %s", "LVMPOLLD", "caught err POLLHUP");
			fds[1].fd = -1;
			fds_count--;
		} else if (fds[1].revents) {
			WARN(pdlv->ls, "%s: %s", "LVMPOLLD", "poll for command's STDOUT failed");
			fds[1].fd = -1;
			fds_count--;
		}

		/* fds_count == 0 means polling reached EOF or error on both descriptors */
		wait4 = waitpid(child, &ch_stat, fds_count ? WNOHANG : 0);
		if (wait4) {
			if (wait4 < 0) {
				ERROR(pdlv->ls, "%s: %s (PID %d) %s", "LVMPOLLD",
				      "waitpid() for lvm2 cmd", child,
				      "resulted in error");
				/* TODO: what else to (not) do? */
				if (kill(child, SIGTERM))
					WARN(pdlv->ls, "%s: %s (PID %d)",
					     "LVMPOLLD",
					     "...and failed to SIGTERM the lvm2 cmd as well",
					     child);
				goto out;
			}

			DEBUGLOG(pdlv->ls, "%s: %s", "LVMPOLLD", "child exited");
			break;
		}
	}

	if (fds_count) { /* read all remaining bits in pipes */
		DEBUGLOG(pdlv->ls, "%s: %s", "LVMPOLLD", "about collect remaining lines");
		if (fds[0].fd >= 0)
			while (read_single_line(&line, &lsize, fout)) {
				INFO(pdlv->ls, "%s: PID %d: %s: %s", "LVM2CMD", child, "STDOUT", line);
				prcs = parse_line_for_percents(pdlv, line);
				if (prcs == DM_PERCENT_FAILED || prcs == DM_PERCENT_INVALID)
					WARN(pdlv->ls, "%s: %s", "LVMPOLLD", "parsing percentage from lvm2 command failed");
				else {
					DEBUGLOG(pdlv->ls, "%s: %s (%f)", "LVMPOLLD", "parsed this", dm_percent_to_float(prcs));
					pdlv_set_percents(pdlv, prcs);
				}
			}
		if (fds[1].fd >= 0)
			while (read_single_line(&line, &lsize, ferr))
				INFO(pdlv->ls, "%s: PID %d: %s: %s", "LVM2CMD", child, "STDERR", line);
	}

	if (WIFEXITED(ch_stat)) {
		INFO(pdlv->ls, "%s: %s (PID %d) %s (%d)", "LVMPOLLD",
		     "lvm2 cmd", child, "exited with", WEXITSTATUS(ch_stat));
		cmd_state.ret_code = WEXITSTATUS(ch_stat);
	} else if (WIFSIGNALED(ch_stat)) {
		WARN(pdlv->ls, "%s: %s (PID %d) %s (%d)", "LVMPOLLD",
		     "lvm2 cmd", child, "got terminated by signal",
		     WTERMSIG(ch_stat));
		cmd_state.signal = WTERMSIG(ch_stat);
	}

out:
	/* FIXME: double check it's on proper place */
	/* lock_lvid_to_pdlv(pdlv->ls);
	dm_hash_remove(pdlv->ls->lvid_to_pdlv, pdlv->lvid);
	unlock_lvid_to_pdlv(pdlv->ls); */

	pdlv_set_cmd_state(pdlv, &cmd_state);

	if (fout && fclose(fout))
		WARN(pdlv->ls, "%s: %s", "LVMPOLLD", "failed to close stdout file");
	if (ferr && fclose(ferr))
		WARN(pdlv->ls, "%s: %s", "LVMPOLLD", "failed to close stderr file");
	if (line)
		dm_free(line);
}

static void *fork_and_poll(void *args)
{
	pid_t r;

	lvmpolld_lv_t *pdlv = (lvmpolld_lv_t *) args;

	int outpipe[2] = { -1, -1 }, errpipe[2] = { -1, -1 };
	const char *tmplvid = dm_strdup(pdlv->lvid);
	const char *cmdargv[] = { VGPOLL_CMD_BIN,
				  "--config",
				  "devices { filter = [ \"a/.*/\" ] }",
				  "--debug",
				  "--verbose",
				  polling_ops[pdlv->type],
				  tmplvid,
				  NULL };

	if (!tmplvid) {
		ERROR(pdlv->ls, "%s: %s", "LVMPOLLD", "not enough memory");
		return NULL;
	}

	if (pipe(outpipe) || pipe(errpipe)) {
		ERROR(pdlv->ls, "%s: %s", "LVMPOLLD", "failed to create pipe");
		goto err;
	}

	/* don't duplicate read end of the pipe */
	if (fcntl(outpipe[0], F_SETFD, FD_CLOEXEC))
		WARN(pdlv->ls, "%s: %s", "LVMPOLLD", "failed to set FD_CLOEXEC on read end of pipe");
	if (fcntl(outpipe[1], F_SETFD, FD_CLOEXEC))
		WARN(pdlv->ls, "%s: %s", "LVMPOLLD", "failed to set FD_CLOEXEC on write end of pipe");
	if (fcntl(errpipe[0], F_SETFD, FD_CLOEXEC))
		WARN(pdlv->ls, "%s: %s", "LVMPOLLD", "failed to set FD_CLOEXEC on read end of err pipe");
	if (fcntl(errpipe[1], F_SETFD, FD_CLOEXEC))
		WARN(pdlv->ls, "%s: %s", "LVMPOLLD", "failed to set FD_CLOEXEC on write end of err pipe");

	r = fork();
	if (!r) {
		/* child */
		/* !!! Do not touch any shared variables belonging to polldaemon !!! */

		if ((dup2(outpipe[1], STDOUT_FILENO ) != STDOUT_FILENO) ||
		    (dup2(errpipe[1], STDERR_FILENO ) != STDERR_FILENO))
			_exit(100);

		execve(VGPOLL_CMD_BIN, (char *const *)cmdargv, NULL);

		_exit(101);
	} else {
		/* parent */
		if (r == -1) {
			ERROR(pdlv->ls, "%s: %s", "LVMPOLLD", "fork failed");
			/* FIXME: report failures to progress threads */
			goto err;
		}

		INFO(pdlv->ls, "%s: LVM2 cmd \"%s\" (PID: %d)", "LVMPOLLD", VGPOLL_CMD_BIN, r);

		dm_free((void *)tmplvid);
		if (close(outpipe[1]))
			WARN(pdlv->ls, "%s: %s", "LVMPOLLD", "failed to close write end of pipe");
		if (close(errpipe[1]))
			WARN(pdlv->ls, "%s: %s", "LVMPOLLD", "failed to close write end of err pipe");


		poll_for_output(pdlv, r, *outpipe, *errpipe);
		DEBUGLOG(pdlv->ls, "%s: %s", "LVMPOLLD", "polling command finished");

		lock_lvid_to_pdlv(pdlv->ls);
		dm_hash_remove(pdlv->ls->lvid_to_pdlv, pdlv->lvid);
		unlock_lvid_to_pdlv(pdlv->ls);

		DEBUGLOG(pdlv->ls, "%s: %s", "LVMPOLLD", "entering pdlv_put");
		pdlv_put(pdlv);
	}

	return NULL;
err:
	lock_lvid_to_pdlv(pdlv->ls);
	dm_hash_remove(pdlv->ls->lvid_to_pdlv, pdlv->lvid);
	unlock_lvid_to_pdlv(pdlv->ls);

	pdlv_put(pdlv);

	dm_free((void *)tmplvid);

	if (outpipe[0] != -1)
		close(outpipe[0]);
	if (outpipe[1] != -1)
		close(outpipe[1]);
	if (errpipe[0] != -1)
		close(errpipe[0]);
	if (errpipe[1] != -1)
		close(errpipe[1]);

	lock_lvid_to_pdlv(pdlv->ls);
	dm_hash_remove(pdlv->ls->lvid_to_pdlv, pdlv->lvid);
	unlock_lvid_to_pdlv(pdlv->ls);

	pdlv_put(pdlv);

	return NULL;
}

static int send_single_response(client_handle h, response resp)
{
	int r = 0;

	if (!resp.buffer.mem) {
		if (!dm_config_write_node(resp.cft->root, buffer_line, &resp.buffer))
			goto fail;
		if (!buffer_append(&resp.buffer, "\n\n"))
			goto fail;
		dm_config_destroy(resp.cft);
	}

	r = buffer_write(h.socket_fd, &resp.buffer);
fail:
	buffer_destroy(&resp.buffer);

	return r;
}

static response stream_progress_data(client_handle h, lvmpolld_lv_t *pdlv)
{
	response resp;
	dm_percent_t perc = DM_PERCENT_INVALID;
	lvmpolld_cmd_stat_t cmd_state;

	/* DEBUGLOG(pdlv->ls, "%s: %s", "LVMPOLLD-tpd()", "waiting for lock"); */
	pthread_mutex_lock(&pdlv->lock);
	/* DEBUGLOG(pdlv->ls, "%s: %s", "LVMPOLLD-tpd()", "acquired lock"); */
	/* LOCKED */

	while (!pdlv->polling_finished) {
		if (perc != pdlv->percent) {
			perc = pdlv->percent;

			pthread_mutex_unlock(&pdlv->lock);
			/* UNLOCKED */
			/* DEBUGLOG(pdlv->ls, "%s: %s", "LVMPOLLD-tpd()", "released lock"); */

			resp = daemon_reply_simple("progress_data", "data = %d", perc, NULL);
			/* may block */
			if (!send_single_response(h, resp)) {
				ERROR(pdlv->ls, "%s: %s", "LVMPOLLD", "Failed to send progress data");
				goto fail;
			}

			/* DEBUGLOG(pdlv->ls, "%s: %s", "LVMPOLLD-tpd()", "waiting for lock"); */
			pthread_mutex_lock(&pdlv->lock);
			/* DEBUGLOG(pdlv->ls, "%s: %s", "LVMPOLLD-tpd()", "acquired lock"); */
			/* LOCKED */
		} else {
			/* LOCKED */
			/* wait for next modification of pdlv->cmd_state or pdlv->percent */
			/* DEBUGLOG(pdlv->ls, "%s: %s", "LVMPOLLD-tpd()", "going to wait on cond_update"); */
			if (pthread_cond_wait(&pdlv->cond_update, &pdlv->lock)) {
				ERROR(pdlv->ls, "%s: %s", "LVMPOLLD", "pthread_cond_wait() failed terribly");
				pthread_mutex_unlock(&pdlv->lock);
				/* UNLOCKED */
				goto fail;
			}
			/* DEBUGLOG(pdlv->ls, "%s: %s", "LVMPOLLD-tpd()", "woke up from sleep on cond_update"); */
		}
		/* LOCKED */
	}
	/* LOCKED */

	cmd_state = pdlv->cmd_state;
	if (perc != pdlv->percent)
		perc = pdlv->percent;
	else
		perc = DM_PERCENT_INVALID;

	pthread_mutex_unlock(&pdlv->lock);
	/* DEBUGLOG(pdlv->ls, "%s: %s", "LVMPOLLD-tpd()", "released lock"); */
	/* UNLOCKED */

	if (perc != DM_PERCENT_INVALID) {
		resp = daemon_reply_simple("progress_data", "data = %d", perc, NULL);
		/* may block */
		if (!send_single_response(h, resp)) {
			ERROR(pdlv->ls, "%s: %s", "LVMPOLLD", "Failed to send progress data");
			goto fail;
		}
	}

	if (cmd_state.signal)
		resp = daemon_reply_simple("failed",
					   "reason = %s",
					   "signal",
					   "value = %d", cmd_state.signal, 
					   NULL);
	else if (cmd_state.ret_code)
		resp = daemon_reply_simple("failed",
					   "reason = %s",
					   "ret_code",
					   "value = %d", cmd_state.ret_code, 
					   NULL);
	else
		resp = daemon_reply_simple("OK", NULL);

	return resp;
fail:
	/* UNLOCKED */
	return reply_fail("unrecoverable error occured");
}

/*
 * initialise internal lvmpolld structures related to PVMOVE polling
 *
 * it doesn't care if the real lvm command fails later
 *
 * @returns
 * 	LVMPOLD_OK 		if initiated sucesfuly or already being polled
 * 	LVMPOLD_FAIL		if vg is missing, or pvmove is not running
 *				or anything else went wrong
 */
static response poll_init(client_handle h, lvmpolld_state_t *ls, request req, enum poll_type type)
{
	lvmpolld_lv_t *pdlv;
	response r = reply_fail("not enough memory");
	const char *lvid = daemon_request_str(req, "lvid", NULL);
	unsigned stream_data = daemon_request_int(req, "stream_data", 0);

	assert(type < POLL_TYPE_MAX);

	if (!lvid)
		return reply_fail("requires LV UUID");

	lock_lvid_to_pdlv(ls);

	/*
	 * lookup already monitored VG object or create new one
	 */
	pdlv = dm_hash_lookup(ls->lvid_to_pdlv, lvid);
	if (!pdlv) {
		/* pdlv->use_count == 1 after create */
		if ((pdlv = pdlv_create(ls, lvid, type)) == NULL) {
			unlock_lvid_to_pdlv(ls);
			ERROR(ls, "%s: %s", "LVMPOLLD", "pdlv_create failed");
			return r;
		}
		if (!dm_hash_insert(ls->lvid_to_pdlv, lvid, pdlv)) {
			unlock_lvid_to_pdlv(ls);
			pdlv_put(pdlv);
			ERROR(ls, "%s: %s", "LVMPOLLD", "dm_hash_insert(pdlv) failed");
			return r;
		}
		if (pthread_create(&pdlv->tid, NULL, fork_and_poll, (void *)pdlv)) {
			dm_hash_remove(ls->lvid_to_pdlv, lvid);
			unlock_lvid_to_pdlv(ls);
			pdlv_put(pdlv);
			ERROR(ls, "%s: %s", "LVMPOLLD", "pthread_create failed");
			return r;
		}
	}

	if (pdlv->type != type) {
		unlock_lvid_to_pdlv(ls);
		return reply_fail("VG is already beying polled with different operation in place");
	}

	/* increase use count for streaming thread */
	if (stream_data)
		pdlv_get(pdlv);

	unlock_lvid_to_pdlv(ls);

	if (stream_data) {
		r = stream_progress_data(h, pdlv);
		pdlv_put(pdlv);
		return r;
	}
	else
		return daemon_reply_simple("OK", NULL);
}

static response progress_stream(client_handle h, lvmpolld_state_t *ls, request req)
{
	lvmpolld_lv_t *pdlv;
	response r;
	const char *lvid = daemon_request_str(req, "lvid", NULL);

	if (!lvid)
		return reply_fail("requires UUID");

	lock_lvid_to_pdlv(ls);

	pdlv = dm_hash_lookup(ls->lvid_to_pdlv, lvid);
	if (!pdlv) {
		unlock_lvid_to_pdlv(ls);
		r = reply_fail("not found");
		goto out;
	}
	else
		pdlv_get(pdlv);

	unlock_lvid_to_pdlv(ls);

	r = stream_progress_data(h, pdlv);

	pdlv_put(pdlv);
out:
	return r;
}

static response progress_data_single(client_handle h, lvmpolld_state_t *ls, request req)
{
	lvmpolld_lv_t *pdlv;
	response r;
	const char *lvid = daemon_request_str(req, "lvid", NULL);

	if (!lvid)
		return reply_fail("requires UUID");

	lock_lvid_to_pdlv(ls);

	pdlv = dm_hash_lookup(ls->lvid_to_pdlv, lvid);
	if (!pdlv) {
		unlock_lvid_to_pdlv(ls);
		r = reply_fail("not found");
		goto out;
	}
	else
		pdlv_get(pdlv);

	unlock_lvid_to_pdlv(ls);

	r = daemon_reply_simple("progress_data", "data = %d", pdlv_get_percents(pdlv), NULL);

	pdlv_put(pdlv);
out:
	return r;
}

static response handler(struct daemon_state s, client_handle h, request r)
{
	lvmpolld_state_t *ls = s.private;
	const char *rq = daemon_request_str(r, "request", "NONE");

	if (!strcmp(rq, PVMOVE_POLL))
		return poll_init(h, ls, r, PVMOVE);
	if (!strcmp(rq, CONVERT_POLL))
		return poll_init(h, ls, r, CONVERT);
	if (!strcmp(rq, MERGE_POLL))
		return poll_init(h, ls, r, MERGE);
	if (!strcmp(rq, MERGE_THIN_POLL))
		return poll_init(h, ls, r, MERGE_THIN);

	if (!strcmp(rq, "progress_stream"))
		return progress_stream(h, ls, r);
	if (!strcmp(rq, "progress_data_single"))
		return progress_data_single(h, ls, r);

	return reply_fail("request not implemented");
}

int main(int argc, char *argv[])
{
	signed char opt;
	lvmpolld_state_t ls = { .log_config = "" };
	daemon_state s = {
		.daemon_fini = fini,
		.daemon_init = init,
		.handler = handler,
		.name = "lvmpolld",
		.pidfile = getenv("LVM_LVMPOLLD_PIDFILE") ? : LVMPOLLD_PIDFILE,
		.private = &ls,
		.protocol = "lvmpolld",
		.protocol_version = 1,
		.socket_path = getenv("LVM_LVMPOLLD_SOCKET") ? : LVMPOLLD_SOCKET,
	};

	// use getopt_long
	while ((opt = getopt(argc, argv, "?fhVl:p:s:")) != EOF) {
		switch (opt) {
		case 'h':
			usage(argv[0], stdout);
			exit(0);
		case '?':
			usage(argv[0], stderr);
			exit(0);
		case 'f':
			s.foreground = 1;
			break;
		case 'l':
			ls.log_config = optarg;
			break;
		case 'p':
			s.pidfile = optarg;
			break;
		case 's': // --socket
			s.socket_path = optarg;
			break;
		case 'V':
			printf("lvmpolld version: " LVM_VERSION "\n");
			exit(1);
		}
	}

	daemon_start(s);

	return 0;
}
