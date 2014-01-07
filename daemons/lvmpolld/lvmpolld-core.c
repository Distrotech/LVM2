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

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <poll.h>
#include <pthread.h>
#include <unistd.h>
#include <wait.h>
#include <sys/types.h>

#include "configure.h"
#include "daemon-server.h"
#include "daemon-log.h"
#include "config-util.h"
#include "lvmpolld-data-utils.h"

/* remove this */
// #define LVM_LVMPOLLD_PIDFILE "/run/testd.pid"
// #define LVM_LVMPOLLD_SOCKET "/run/lvm/testd.socket"

/* remove this */
//#define LVMPOLLD_PIDFILE LVM_LVMPOLLD_PIDFILE
#define LVMPOLLD_SOCKET DEFAULT_RUN_DIR "/lvmpolld.socket"

/* extract this info from autoconf/automake files */
#define VGPOLL_CMD_BIN "/sbin/vgpoll"

typedef struct lvmpolld_state {
	log_state *log;
	const char *log_config;

	/*
	 * maps vgid to internal lvmpolld VG representation
	 *
	 * only thread responsible for polling of lvm command
	 * should remove the pdvg from hash_table
	 *
	 * progress threads should only manipulate use_count of
	 * pdvg structure
	 */
	struct dm_hash_table *vgid_to_pdvg;

	struct {
		pthread_mutex_t vgid_to_pdvg;
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

	ls->vgid_to_pdvg = dm_hash_create(32);

	pthread_mutex_init(&ls->lock.vgid_to_pdvg, NULL);

	return 1;
}

static int fini(struct daemon_state *s)
{
	lvmpolld_state_t *ls = s->private;

	pthread_mutex_destroy(&ls->lock.vgid_to_pdvg);

	dm_hash_destroy(ls->vgid_to_pdvg);

	return 1;
}

static void lock_vgid_to_pdvg(lvmpolld_state_t *ls)
{
	pthread_mutex_lock(&ls->lock.vgid_to_pdvg);
}

static void unlock_vgid_to_pdvg(lvmpolld_state_t *ls)
{
	pthread_mutex_unlock(&ls->lock.vgid_to_pdvg);
}

static response reply_fail(const char *reason)
{
	return daemon_reply_simple("failed", "reason = %s", reason, NULL);
}

static int read_single_line(char **line, size_t *lsize, FILE *file)
{
	ssize_t r = getline(line, lsize, file);

	if (r > 0 && *line[r] == '\n') {
		*line[r] = '\0';
		return 1;
	}

	return (r > 0);
}

static void poll_for_output(lvmpolld_vg_t *pdvg,
			   pid_t child,
			   int outfd,
			   int errfd,
			   FILE *fout,
			   FILE *ferr)
{
	char *line = NULL;
	size_t lsize;
	int ch_stat, r, wait4;
	struct pollfd fds[] = { { .fd = outfd, .events = POLLIN },
				{ .fd = errfd, .events = POLLIN } };

	while (1) {
		r = poll(fds, 2, 10000);
		if (!r) {
			/* perhaps let it timeout 3 times */
			WARN(pdvg->ds, "%s: %s (PID %d) %s", "LVMPOLLD",
			     "polling for output of lvm cmd", child, "timed out");
			goto out;
		} else if (r < 0) {
			if (errno == EINTR) /* FIXME: interrupt handler */
				continue;

			ERROR(pdvg->ds, "%s: %s (PID %d) %s (%d): %s",
			      "LVMPOLLD", "poll() for LVM2 cmd", child,
			      "ended with error",  errno, "(strerror())");
			break;
			/* FIXME: handle interrupts */
		}

		/* handle fds structures */
		if (fds[0].revents & POLLIN)
			if (read_single_line(&line, &lsize, fout))
				INFO(pdvg->ds, "%s: PID %d: %s: %s", "LVM2CMD", child, "STDOUT", line);
			else
				fds[0].fd = -1;
		if (fds[1].revents & POLLIN)
			if (read_single_line(&line, &lsize, ferr))
				INFO(pdvg->ds, "%s: PID %d: %s: %s", "LVM2CMD", child, "STDERR", line);
			else
				fds[1].fd = -1;

		wait4 = waitpid(child, &ch_stat, WNOHANG);
		if (wait4) {
			if (wait4 < 0) {
				ERROR(pdvg->ds, "%s: %s (PID %d) %s", "LVMPOLLD",
				      "waitpid() for lvm2 cmd", child,
				      "resulted in error");
				goto out;
			}
			else {
				/* be more verbose about child exit status */
				break;
			}
		}
	}

	if (r > 0) { /* read all remaining bits in pipes */
		while (read_single_line(&line, &lsize, fout))
			INFO(pdvg->ds, "%s: PID %d: %s: %s", "LVM2CMD", child, "STDOUT", line);
		while (read_single_line(&line, &lsize, ferr))
			INFO(pdvg->ds, "%s: PID %d: %s: %s", "LVM2CMD", child, "STDERR", line);
	}

	if (WIFEXITED(ch_stat))
		INFO(pdvg->ds, "%s: %s (PID %d) %s (%d)", "LVMPOLLD",
		     "lvm2 cmd", child, "exited with", WEXITSTATUS(ch_stat));

	if (WIFSIGNALED(ch_stat))
		WARN(pdvg->ds, "%s: %s (PID %d) %s (%d)", "LVMPOLLD",
		     "lvm2 cmd", child, "got terminated by signal",
		     WTERMSIG(ch_stat));
out:
	if (line)
		dm_free(line);
}

static void *pvmove_fork_and_poll(void *args)
{
	pid_t r;

	lvmpolld_vg_t *pdvg = (lvmpolld_vg_t *) args;

	FILE *fout = NULL, *ferr = NULL;
	char *tmpvgid = dm_strdup(pdvg->vgid);
	int outpipe[2] = { -1, -1 }, errpipe[2] = { -1, -1 };
	char *cmdargv[] = { VGPOLL_CMD_BIN, tmpvgid, NULL };

	if (!tmpvgid) {
		ERROR(pdvg->ds, "%s: %s", "LVMPOLLD", "couldn't duplicate vgid string");
		return NULL;
	}

	if (pipe(outpipe)) {
		ERROR(pdvg->ds, "%s: %s", "LVMPOLLD", "can't create pipe");
		goto err;
	}

	if (pipe(errpipe)) {
		ERROR(pdvg->ds, "%s: %s", "LVMPOLLD", "can't create err pipe");
		goto err;
	}

	/* don't duplicate read end of the pipe */
	if (fcntl(outpipe[0], F_SETFD, FD_CLOEXEC))
		WARN(pdvg->ds, "%s: %s", "LVMPOLLD", "failed to set FD_CLOEXEC on read end of pipe");

	if (fcntl(errpipe[0], F_SETFD, FD_CLOEXEC))
		WARN(pdvg->ds, "%s: %s", "LVMPOLLD", "failed to set FD_CLOEXEC on read end of err pipe");

	r = fork();
	if (!r) {
		/* child */
		/* !!! Do not touch any shared variables belonging to polldaemon !!! */

		if ((dup2(outpipe[1], STDOUT_FILENO ) != STDOUT_FILENO) ||
		    close(outpipe[1]) || 
		    (dup2(errpipe[1], STDERR_FILENO ) != STDERR_FILENO) ||
		    close(errpipe[1]))
			_exit(4);

		/* FIXME: environment variables! */
		execve(VGPOLL_CMD_BIN, cmdargv, NULL);

		/* since we forked from threaded app, die immediatly */
		_exit(5);
	} else {
		/* parent */
		if (r == -1) {
			ERROR(pdvg->ds, "%s: %s", "LVMPOLLD", "fork failed");
			/* FIXME: report failures to progress threads */
			goto err;
		}

		INFO(pdvg->ds, "%s: LVM2 cmd \"%s\" (PID: %d)", "LVMPOLLD", VGPOLL_CMD_BIN, r);

		dm_free(tmpvgid);
		if (close(outpipe[1]))
			WARN(pdvg->ds, "%s: %s", "LVMPOLLD", "failed to close write end of pipe");
		if (close(errpipe[1]))
			WARN(pdvg->ds, "%s: %s", "LVMPOLLD", "failed to close write end of err pipe");

		if ((fout = fdopen(outpipe[0], "r")) == NULL)
			goto err;

		if ((ferr = fdopen(errpipe[0], "r")) == NULL)
			goto err;

		poll_for_output(pdvg, r, *errpipe, *outpipe, fout, ferr);
	}

	if (fclose(fout))
		WARN(pdvg->ds, "%s: %s", "LVMPOLLD", "failed to close out file");
	if (fclose(ferr))
		WARN(pdvg->ds, "%s: %s", "LVMPOLLD", "failed to close err file");
	if (close(outpipe[0]))
		WARN(pdvg->ds, "%s: %s", "LVMPOLLD", "failed to close read end of pipe");
	if (close(errpipe[0]))
		WARN(pdvg->ds, "%s: %s", "LVMPOLLD", "failed to close read end of err pipe");

	return NULL;
err:
	dm_free(tmpvgid);

	if (fout)
		fclose(fout);
	if (ferr)
		fclose(ferr);
	if (outpipe[0] != -1)
		close(outpipe[0]);
	if (outpipe[1] != -1)
		close(outpipe[1]);
	if (errpipe[0] != -1)
		close(errpipe[0]);
	if (errpipe[1] != -1)
		close(errpipe[1]);

	return NULL;
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
static response pvmove_poll_init(lvmpolld_state_t *ls, request req)
{
	int background = 1;
	lvmpolld_vg_t *pdvg;
	response r = reply_fail("not enough memory");
	const char *vgid = daemon_request_str(req, "vgid", NULL);

	if (!vgid)
		return reply_fail("requires VG UUID");

	lock_vgid_to_pdvg(ls);

	/*
	 * lookup already monitored VG object or create new one
	 */
	pdvg = dm_hash_lookup(ls->vgid_to_pdvg, vgid);
	if (!pdvg) {
		/* pdvg->use_count == 1 after create */
		if ((pdvg = pdvg_create(ls, vgid, PVMOVE)) == NULL) {
			unlock_vgid_to_pdvg(ls);
			ERROR(ls, "%s: %s", "LVMPOLLD", "pdvg_create failed");
			return r;
		}
		if (!dm_hash_insert(ls->vgid_to_pdvg, vgid, pdvg)) {
			unlock_vgid_to_pdvg(ls);
			pdvg_put(pdvg);
			ERROR(ls, "%s: %s", "LVMPOLLD", "dm_hash_insert(pdvg) failed");
			return r;
		}
		if (pthread_create(&pdvg->tid, NULL, pvmove_fork_and_poll, (void *)pdvg)) {
			dm_hash_remove(ls->vgid_to_pdvg, vgid);
			unlock_vgid_to_pdvg(ls);
			pdvg_put(pdvg);
			ERROR(ls, "%s: %s", "LVMPOLLD", "pthread_create failed");
			return r;
		}
	} else if (pdvg->type != PVMOVE) {
		unlock_vgid_to_pdvg(ls);
		return reply_fail("VG is already beying polled with different operation in place");
	}

	/* increase use count for progress thread */
	/* if (!background)
		pdvg_get(pdvg);*/

	unlock_vgid_to_pdvg(ls);

	/*
	 * FIXME: transfer progress info if !background
	 */

	return daemon_reply_simple("OK", NULL);
}

static response handler(struct daemon_state s, client_handle h, request r)
{
	lvmpolld_state_t *ls = s.private;
	const char *rq = daemon_request_str(r, "request", "NONE");

	/* init pvmove only */
	if (!strcmp(rq, "pvmove_poll"))
		return pvmove_poll_init(ls, r);

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
		}
	}

	daemon_start(s);

	return 0;
}
