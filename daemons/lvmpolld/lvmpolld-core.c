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
#include "lvm-version.h"  /* ??? */

#define LVMPOLLD_SOCKET DEFAULT_RUN_DIR "/lvmpolld.socket"

#define PD_LOG_PREFIX "LVMPOLLD"
#define LVM2_LOG_PREFIX "LVPOLL"

/* 
 * FIXME: I don't follow the logic behind prefix variables in lvm2
 * configure script
 */

#define LVPOLL_CMD "lvpoll"
/* extract this info from autoconf/automake files */

typedef struct lvmpolld_state {
	log_state *log;
	const char *log_config;

	lvmpolld_store_t lvid_to_pdlv_abort;
	lvmpolld_store_t lvid_to_pdlv_poll;

	const char *prefix_dir;
	const char *cmd;
} lvmpolld_state_t;

static const char *const const polling_ops[] = { [PVMOVE] = PVMOVE_POLL,
						 [CONVERT] = CONVERT_POLL,
						 [MERGE] = MERGE_POLL,
						 [MERGE_THIN] = MERGE_THIN_POLL };


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

#define LVMPOLLD_SBIN_DIR "/usr/sbin/"

static int init(struct daemon_state *s)
{
	char *tmp;
	size_t n;
	lvmpolld_state_t *ls = s->private;
	ls->log = s->log;

	if (!daemon_log_parse(ls->log, DAEMON_LOG_OUTLET_STDERR, ls->log_config, 1))
		return 0;

	pdst_init(&ls->lvid_to_pdlv_poll, "polling");
	pdst_init(&ls->lvid_to_pdlv_abort, "abort");

	DEBUGLOG(ls, "%s: LVM_SYSTEM_DIR=%s", PD_LOG_PREFIX, getenv("LVM_SYSTEM_DIR") ?: "<not set>");

	/* TODO: this HAS TO be removed. I feel sick of it */
	ls->prefix_dir = ls->prefix_dir ?: LVMPOLLD_SBIN_DIR;
	n = strlen(ls->prefix_dir) + strlen(LVPOLL_CMD) + 2; /* PREFIX_DIR + / + LVPOLL_CMD + \0 */
	tmp = dm_malloc(n * sizeof(char));
	memcpy(tmp, ls->prefix_dir, strlen(ls->prefix_dir));
	*(tmp + strlen(ls->prefix_dir)) = '/';
	memcpy(tmp + strlen(ls->prefix_dir) + 1, LVPOLL_CMD, strlen(LVPOLL_CMD));
	*(tmp + n - 1) = '\0';

	ls->cmd = tmp;

	return 1;
}

static int fini(struct daemon_state *s)
{
	lvmpolld_state_t *ls = s->private;

	pdst_destroy(&ls->lvid_to_pdlv_poll);
	pdst_destroy(&ls->lvid_to_pdlv_abort);

	dm_free((void *)ls->cmd);

	return 1;
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

static void parse_line_for_percents(lvmpolld_lv_t *pdlv, const char *line)
{
	char *endptr, *keyw, *nr;
	dm_percent_t perc;
	double d;

	if (!(keyw = strstr(line, get_keyword(pdlv->type))) || keyw == line
	    || !strchr(keyw, DM_PERCENT_CHAR)) {
		INFO(pdlv->ls, "%s: %s", PD_LOG_PREFIX,
		     "parsing percentage from lvm2 command failed");
		return;
	}

	nr = strpbrk(keyw, "+-0123456789");
	if (!nr) {
		INFO(pdlv->ls, "%s: %s", PD_LOG_PREFIX,
		     "parsing percentage from lvm2 command failed");
		return;
	}

	d = strtod(nr, &endptr);
	if (nr == endptr) {
		INFO(pdlv->ls, "%s: %s", PD_LOG_PREFIX,
		     "parsing percentage from lvm2 command failed");
		return;
	} else if (d > 100.0) {
		WARN(pdlv->ls, "%s: %s", PD_LOG_PREFIX,
		     "parsing percentage from lvm2 command returned invalid value");
		return;
	}

	perc = dm_make_percent((uint64_t)(d * DM_PERCENT_1), DM_PERCENT_100);

	DEBUGLOG(pdlv->ls, "%s: %s %.1f%%", PD_LOG_PREFIX,
		 "parsed", dm_percent_to_float(perc));

	pdlv_set_percents(pdlv, perc);
}

/* make this configurable */
#define MAX_TIMEOUT 0

static int poll_for_output(lvmpolld_lv_t *pdlv, int outfd, int errfd)
{
	size_t lsize;
	int ch_stat, r, wait4, err = 1, fds_count = 2, timeout = 0;

	FILE *fout = NULL, *ferr = NULL;

	char *line = NULL;
	lvmpolld_cmd_stat_t cmd_state = { .retcode = -1, .signal = 0 };
	struct pollfd fds[] = { { .fd = outfd, .events = POLLIN },
				{ .fd = errfd, .events = POLLIN } };

	if (!(fout = fdopen(outfd, "r")) || !(ferr = fdopen(errfd, "r"))) {
		ERROR(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "failed to open file stream");
		goto out;
	}

	while (1) {
		do {
			r = poll(fds, 2, pdlv_get_timeout(pdlv) * 1000);
		} while (r < 0 && errno == EINTR);

		DEBUGLOG(pdlv->ls, "%s: %s %d", PD_LOG_PREFIX, "poll() returned", r);
		if (r < 0) {
			/* likely ENOMEM happened */
			ERROR(pdlv->ls, "%s: %s (PID %d) %s (%d): %s",
			      PD_LOG_PREFIX, "poll() for LVM2 cmd", pdlv->cmd_pid,
			      "ended with error", errno, "(strerror())");
			goto out;
		} else if (!r) {
			timeout++;

			WARN(pdlv->ls, "%s: %s (PID %d) %s", PD_LOG_PREFIX,
			     "polling for output of lvm cmd", pdlv->cmd_pid, "has timed out");

			if (timeout > MAX_TIMEOUT) {
				ERROR(pdlv->ls, "%s: %s (PID %d)", PD_LOG_PREFIX,
				      "Exceeded maximum number of allowed timeouts for lvm cmd",
				      pdlv->cmd_pid);
				goto out;
			}

			continue; /* while(1) */
		}

		timeout = 0;

		/* handle the command's STDOUT */
		if (fds[0].revents & POLLIN) {
			DEBUGLOG(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "caught input data in STDOUT");

			assert(read_single_line(&line, &lsize, fout)); /* may block indef. anyway */
			INFO(pdlv->ls, "%s: PID %d: %s: '%s'", LVM2_LOG_PREFIX,
			     pdlv->cmd_pid, "STDOUT", line);

			if (pdlv->parse_output_fn)
				pdlv->parse_output_fn(pdlv, line);
		} else if (fds[0].revents) {
			if (fds[0].revents & POLLHUP)
				DEBUGLOG(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "caught POLLHUP");
			else
				WARN(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "poll for command's STDOUT failed");

			fds[0].fd = -1;
			fds_count--;
		}

		/* handle the command's STDERR */
		if (fds[1].revents & POLLIN) {
			DEBUGLOG(pdlv->ls, "%s: %s", PD_LOG_PREFIX,
				 "caught input data in STDERR");

			assert(read_single_line(&line, &lsize, ferr)); /* may block indef. anyway */
			INFO(pdlv->ls, "%s: PID %d: %s: '%s'", LVM2_LOG_PREFIX,
			     pdlv->cmd_pid, "STDERR", line);
		} else if (fds[1].revents) {
			if (fds[1].revents & POLLHUP)
				DEBUGLOG(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "caught err POLLHUP");
			else
				WARN(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "poll for command's STDOUT failed");

			fds[1].fd = -1;
			fds_count--;
		}

		do {
			/*
			 * fds_count == 0 means polling reached EOF
			 * or received error on both descriptors.
			 * In such case, just wait for command to finish
			 */
			wait4 = waitpid(pdlv->cmd_pid, &ch_stat, fds_count ? WNOHANG : 0);
		} while (wait4 < 0 && errno == EINTR);

		if (wait4) {
			if (wait4 < 0) {
				ERROR(pdlv->ls, "%s: %s (PID %d) %s", PD_LOG_PREFIX,
				      "waitpid() for lvm2 cmd", pdlv->cmd_pid,
				      "resulted in error");
				goto out;
			}
			DEBUGLOG(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "child exited");
			break;
		}
	} /* while(1) */

	DEBUGLOG(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "about to collect remaining lines");
	if (fds[0].fd >= 0)
		while (read_single_line(&line, &lsize, fout)) {
			assert(r > 0);
			INFO(pdlv->ls, "%s: PID %d: %s: %s", LVM2_LOG_PREFIX, pdlv->cmd_pid, "STDOUT", line);
			if (pdlv->parse_output_fn)
				pdlv->parse_output_fn(pdlv, line);
		}
	if (fds[1].fd >= 0)
		while (read_single_line(&line, &lsize, ferr)) {
			assert(r > 0);
			INFO(pdlv->ls, "%s: PID %d: %s: %s", LVM2_LOG_PREFIX, pdlv->cmd_pid, "STDERR", line);
		}

	if (WIFEXITED(ch_stat)) {
		INFO(pdlv->ls, "%s: %s (PID %d) %s (%d)", PD_LOG_PREFIX,
		     "lvm2 cmd", pdlv->cmd_pid, "exited with", WEXITSTATUS(ch_stat));
		cmd_state.retcode = WEXITSTATUS(ch_stat);
	} else if (WIFSIGNALED(ch_stat)) {
		WARN(pdlv->ls, "%s: %s (PID %d) %s (%d)", PD_LOG_PREFIX,
		     "lvm2 cmd", pdlv->cmd_pid, "got terminated by signal",
		     WTERMSIG(ch_stat));
		cmd_state.signal = WTERMSIG(ch_stat);
	}

	err = 0;
out:
	if (!err)
		pdlv_set_cmd_state(pdlv, &cmd_state);

	if (fout && fclose(fout))
		WARN(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "failed to close stdout file");
	if (ferr && fclose(ferr))
		WARN(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "failed to close stderr file");
	dm_free(line);

	return err;
}

#define MIN_SIZE 16

static int add_to_cmdargv(const char ***cmdargv, const char *str, int *index, int renameme)
{
	const char **newargv = *cmdargv;

	if (*index && !(*index % renameme)) {
		newargv = dm_realloc(*cmdargv, (*index / renameme + 1) * renameme * sizeof(char *));
		if (!newargv)
			return 0;
		*cmdargv = newargv;
	}

	*(*cmdargv + (*index)++) = str;

	return 1;
}

static const char **cmdargv_ctr(lvmpolld_lv_t *pdlv, unsigned abort)
{
	int i = 0;
	const char **cmd_argv = dm_malloc(MIN_SIZE * sizeof(char *));

	if (!cmd_argv) {
		ERROR(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "construct_cmdargv: malloc failed");
		return NULL;
	}

	if (!add_to_cmdargv(&cmd_argv, pdlv->ls->cmd, &i, MIN_SIZE))
		goto err;

	/* by default run under global filter */
	if (!add_to_cmdargv(&cmd_argv, "--config", &i, MIN_SIZE) ||
	    !add_to_cmdargv(&cmd_argv, "devices { filter = [ \"a/.*/\" ] }", &i, MIN_SIZE))
		goto err;

	/* transfer internal polling interval */
	if (pdlv->sinterval &&
	    (!add_to_cmdargv(&cmd_argv, "--interval", &i, MIN_SIZE) ||
	     !add_to_cmdargv(&cmd_argv, pdlv->sinterval, &i, MIN_SIZE)))
		goto err;

	/* pass abort param */
	if (abort &&
		!add_to_cmdargv(&cmd_argv, "--abort", &i, MIN_SIZE))
		goto err;

	/* one of: "convert", "pvmove", "merge", "merge_thin" */
	if (!add_to_cmdargv(&cmd_argv, "--poll-operation", &i, MIN_SIZE) ||
	    !add_to_cmdargv(&cmd_argv, polling_ops[pdlv->type], &i, MIN_SIZE))
		goto err;

	/* either vg_uuid+\0\0\0\0... or vg_uuid+lv_uuid */
	if (!add_to_cmdargv(&cmd_argv, "--uuid", &i, MIN_SIZE) ||
	    !add_to_cmdargv(&cmd_argv, pdlv->lvid, &i, MIN_SIZE))
		goto err;

	/* vgname */
	if (!add_to_cmdargv(&cmd_argv, pdlv->vgname, &i, MIN_SIZE))
		goto err;

	/* terminating NULL */
	if (!add_to_cmdargv(&cmd_argv, NULL, &i, MIN_SIZE))
		goto err;

	return cmd_argv;
err:
	ERROR(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "construct_cmdargv: realloc failed");
	dm_free(cmd_argv);
	return NULL;
}

static void *fork_and_poll(void *args)
{
	lvmpolld_store_t *pdst;
	pid_t r;
	int error = 1;

	lvmpolld_lv_t *pdlv = (lvmpolld_lv_t *) args;

	int outpipe[2] = { -1, -1 }, errpipe[2] = { -1, -1 };

	if (pipe(outpipe) || pipe(errpipe)) {
		ERROR(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "failed to create pipe");
		goto err;
	}

	/* FIXME: failure to set O_CLOEXEC will perhaps result in broken polling anyway */
	/* don't duplicate read end of the pipe */
	if (fcntl(outpipe[0], F_SETFD, FD_CLOEXEC))
		WARN(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "failed to set FD_CLOEXEC on read end of pipe");
	if (fcntl(outpipe[1], F_SETFD, FD_CLOEXEC))
		WARN(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "failed to set FD_CLOEXEC on write end of pipe");
	if (fcntl(errpipe[0], F_SETFD, FD_CLOEXEC))
		WARN(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "failed to set FD_CLOEXEC on read end of err pipe");
	if (fcntl(errpipe[1], F_SETFD, FD_CLOEXEC))
		WARN(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "failed to set FD_CLOEXEC on write end of err pipe");

	r = fork();
	if (!r) {
		/* child */
		/* !!! Do not touch any posix thread primitive !!! */

		if ((dup2(outpipe[1], STDOUT_FILENO ) != STDOUT_FILENO) ||
		    (dup2(errpipe[1], STDERR_FILENO ) != STDERR_FILENO))
			_exit(100);

		execv(*(pdlv->cmdargv), (char *const *)pdlv->cmdargv);

		_exit(101);
	} else {
		/* parent */
		if (r == -1) {
			ERROR(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "fork failed");
			goto err;
		}

		INFO(pdlv->ls, "%s: LVM2 cmd \"%s\" (PID: %d)", PD_LOG_PREFIX, *(pdlv->cmdargv), r);

		pdlv->cmd_pid = r;

		/* failure to close write end of any pipe will result in broken polling */
		if (close(outpipe[1])) {
			ERROR(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "failed to close write end of pipe");
			goto err;
		}
		if (close(errpipe[1])) {
			ERROR(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "failed to close write end of err pipe");
			goto err;
		}

		outpipe[1] = errpipe[1] = -1;

		error = poll_for_output(pdlv, *outpipe, *errpipe);
		DEBUGLOG(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "polling command finished");
	}

err:
	r = 0;
	pdst = pdlv->pdst;

	pdst_lock(pdst);

	if (pdlv_get_background(pdlv)) {
		/* holding store lock provides there's no single reader */
		pdst_remove(pdst, pdlv->lvid);
		pdlv_destroy(pdlv);
	} else if (error) {
		/* last reader is responsible for pdlv cleanup */
		r = pdlv->cmd_pid;
		pdlv_set_internal_error(pdlv, 1);
	} else
		pdlv_set_polling_finished(pdlv, 1);

	pdst_unlock(pdst);
	/*
	 * after the store get unlocked pdlv
	 * may be already removed by some reader
	 */

	if (outpipe[0] != -1)
		close(outpipe[0]);
	if (outpipe[1] != -1)
		close(outpipe[1]);
	if (errpipe[0] != -1)
		close(errpipe[0]);
	if (errpipe[1] != -1)
		close(errpipe[1]);

	/*
	 * This is unfortunate case where we
	 * know nothing about state of lvm cmd and
	 * (eventually) ongoing progress.
	 *
	 * harvest zombies
	 */
	if (r)
		while(waitpid(r, NULL, 0) < 0 && errno == EINTR);

	return NULL;
}

static response progress_info(client_handle h, lvmpolld_state_t *ls, request req)
{
	lvmpolld_lv_t *pdlv;
	lvmpolld_store_t *pdst;
	lvmpolld_lv_state_t st;
	response r;
	const char *lvid = daemon_request_str(req, "lvid", NULL);
	unsigned abort = daemon_request_int(req, "abort", 0);

	if (!lvid)
		return reply_fail("requires UUID");

	pdst = abort ? &ls->lvid_to_pdlv_abort : &ls->lvid_to_pdlv_poll;

	pdst_lock(pdst);
	/* store locked */

	pdlv = pdst_lookup(pdst, lvid);
	if (pdlv) {
		if (pdlv_get_and_unset_background(pdlv))
			WARN(ls, "%s: %s", PD_LOG_PREFIX,
			     "Client asked for progress info on LV with background flag set");
		/*
		 * with store lock held, I'm the only reader accessing the pdlv
		 */
		st = pdlv_get_status(pdlv);

		if (st.internal_error || st.polling_finished) {
			INFO(ls, "%s: %s %s", PD_LOG_PREFIX,
			     "Polling finished. Removing related data structure for LV",
			     lvid);
			pdst_remove(pdst, lvid);
			pdlv_destroy(pdlv);
		}
	}
	/* pdlv must not be dereferenced from now on */

	pdst_unlock(pdst);
	/* store unlocked */

	if (pdlv) {
		if (st.internal_error)
			return reply_fail("lvmpolld internal error occured");

		if (st.polling_finished)
			r = daemon_reply_simple("finished",
						"data = %d", st.percent,
						"reason = %s", st.cmd_state.signal ? "signal" : "retcode",
						"value = %d", st.cmd_state.signal ?: st.cmd_state.retcode,
						NULL);
		else
			r = daemon_reply_simple("inprogress",
						"data = %d", st.percent,
						NULL);
	}
	else
		r = daemon_reply_simple("not_found", NULL);

	return r;
}

static lvmpolld_lv_t *construct_pdlv(request req, lvmpolld_state_t *ls,
				     const char *lvid, const char *vgname,
				     enum poll_type type, unsigned abort,
				     lvmpolld_store_t *pdst, unsigned background)
{
	const char **cmdargv;
	lvmpolld_lv_t *pdlv;
	unsigned interval;
	const char *sinterval = daemon_request_str(req, "interval", NULL);

	if (!sinterval || strpbrk(sinterval, "-") || sscanf(sinterval, "%u", &interval) != 1) {
		ERROR(ls, "%s: %s: %s", PD_LOG_PREFIX, "Illegal mandatory interval parameter",
		      sinterval ?: "<empty>");
		return NULL;
	}

	pdlv = pdlv_create(ls, lvid, vgname, type, sinterval, 2 * interval, pdst,
			   (abort ? NULL : parse_line_for_percents), background);

	if (!pdlv) {
		ERROR(ls, "%s: %s", PD_LOG_PREFIX, "Failed to create pdlv");
		return NULL;
	}

	cmdargv = cmdargv_ctr(pdlv, abort);
	if (!cmdargv) {
		pdlv_destroy(pdlv);
		ERROR(pdlv->ls, "%s: %s", PD_LOG_PREFIX, "failed to construct cmd arguments for lvpoll command");
		return NULL;
	}

	pdlv->cmdargv = cmdargv;

	return pdlv;
}

static int spawn_detached_thread(lvmpolld_lv_t *pdlv)
{
	int r;
	pthread_attr_t attr;

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	r = pthread_create(&pdlv->tid, &attr, fork_and_poll, (void *)pdlv);

	pthread_attr_destroy(&attr);

	return !r;
}

static response poll_init(client_handle h, lvmpolld_state_t *ls, request req, enum poll_type type)
{
	lvmpolld_lv_t *pdlv;
	lvmpolld_store_t *pdst;

	const char *lvid = daemon_request_str(req, "lvid", NULL);
	const char *vgname = daemon_request_str(req, "vgname", NULL);
	unsigned abort = daemon_request_int(req, "abort", 0);
	/* make background default on purpose */
	unsigned background = daemon_request_int(req, "background", 1);

	if (!lvid)
		/* TODO: "response" == "fail" is used with internall errors only */
		/* perhaps add new type of response */
		return reply_fail("lvid required");

	if (!vgname)
		/* TODO: "response" == "fail" is used with internall errors only */
		/* perhaps add new type of response */
		return reply_fail("vgname required");

	background = background > 1 ? 1 : 0;

	assert(type < POLL_TYPE_MAX);

	if (abort && type != PVMOVE)
		/* TODO: "response" == "fail" is used with internall errors only */
		/* perhaps add new type of response */
		return reply_fail("abort is not supported with requested poll operation");

	pdst = abort ? &ls->lvid_to_pdlv_abort : &ls->lvid_to_pdlv_poll;

	pdst_lock(pdst);

	pdlv = pdst_lookup(pdst, lvid);
	if (pdlv && pdlv_get_polling_finished(pdlv)) {
		WARN(ls, "%s: %s %s", PD_LOG_PREFIX, "Removing uncollected info for LV",
			 lvid);
		/* 
		 * lvmpolld has to remove uncollected results in this case.
		 * otherwise it would have to refuse request for new polling
		 * lv with same id.
		 */
		pdst_remove(pdst, lvid);
		pdlv_destroy(pdlv);
		pdlv = NULL;
	}

	if (pdlv) {
		if (!pdlv_is_type(pdlv, type)) {
			/* TODO: "response" == "fail" is used with internall errors only */
			/* perhaps add new type of response */
			pdst_unlock(pdst);
			return reply_fail("different operation with same lvid already running");
		}
		/* notify lvmpolld about future requests for info */
		if (!background)
			pdlv_set_background(pdlv, background);
	} else {
		pdlv = construct_pdlv(req, ls, lvid, vgname, type, abort, pdst,
				      background);
		if (!pdlv) {
			pdst_unlock(pdst);
			return reply_fail("lvmpolld internal error");
		}
		if (!pdst_insert(pdst, lvid, pdlv)) {
			pdlv_destroy(pdlv);
			pdst_unlock(pdst);
			return reply_fail("lvmpolld internal error");
		}
		if (!spawn_detached_thread(pdlv)) {
			ERROR(ls, "%s: %s", PD_LOG_PREFIX, "failed to spawn detached thread");
			pdst_remove(pdst, lvid);
			pdlv_destroy(pdlv);
			pdst_unlock(pdst);
			return reply_fail("lvmpolld internal error");
		}
	}

	pdst_unlock(pdst);

	/* TODO: "response" == "fail" is used with internall errors only */
	/* perhaps add new type of response */
	return daemon_reply_simple("OK", NULL);
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
	if (!strcmp(rq, "progress_info"))
		return progress_info(h, ls, r);

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
		.pidfile = getenv("LVM_LVMPOLLD_PIDFILE") ?: LVMPOLLD_PIDFILE,
		.private = &ls,
		.protocol = "lvmpolld",
		.protocol_version = 1,
		.socket_path = getenv("LVM_LVMPOLLD_SOCKET") ?: LVMPOLLD_SOCKET,
	};

	// use getopt_long
	while ((opt = getopt(argc, argv, "?fhVl:p:s:U:")) != EOF) {
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
		case 'U': /* --prefix */
			ls.prefix_dir = optarg;
			break;
		case 'V':
			printf("lvmpolld version: " LVM_VERSION "\n");
			exit(1);
		}
	}

	daemon_start(s);

	return 0;
}
