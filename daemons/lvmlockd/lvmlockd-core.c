/*
 * Copyright (C) 2014 Red Hat, Inc.
 *
 * This file is part of LVM2.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License v.2.1.
 */

#define _XOPEN_SOURCE 500  /* pthread */
#define _ISOC99_SOURCE
#define _GNU_SOURCE

#include "configure.h"
#include "daemon-io.h"
#include "daemon-server.h"
#include "daemon-log.h"
#include "config-util.h"
#include "lvm-version.h"
#include "lvmetad-client.h"
#include "lvmlockd-client.h"

#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <unistd.h>
#include <poll.h>
#include <errno.h>
#include <signal.h>
#include <getopt.h>
#include <syslog.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/utsname.h>
#include <sys/un.h>

#define EXTERN
#include "lvmlockd-internal.h"

/*
 * Basic operation of lvmlockd
 *
 * lvmlockd main process runs main_loop() which uses poll().
 * poll listens for new connections from lvm commands and for
 * messages from existing connected lvm commands.
 *
 * lvm command starts and connects to lvmlockd.
 *
 * lvmlockd receives a connection request from command and adds a
 * 'struct client' to keep track of the connection to the command.
 * The client's fd is added to the set of fd's in poll().
 *
 * lvm command sends a lock request to lvmlockd.  The lock request
 * can be for the global lock, a vg lock, or an lv lock.
 *
 * lvmlockd main_loop/poll sees a message from an existing client.
 * It sets client.recv = 1, then wakes up client_thread_main.
 *
 * client_thread_main iterates through client structs (cl), looking
 * for any that need processing, finds the one with cl->recv set,
 * and calls client_recv_action(cl).
 *
 * client_recv_action(cl) reads the message/request from the client,
 * allocates a new 'struct action' (act) to represent the request,
 * sets the act with what is found in the request, then looks at
 * the specific operation in act->op (LD_OP_FOO) to decide what to
 * do with the action:
 *
 * . If the action is to start a lockspace, create a new thread
 *   to manage that lockspace: add_lockspace(act).
 *
 * . If the action is a lock request, pass the act to the thread
 *   that is managing that lockspace: add_lock_action(act).
 *
 * . If the action is to add/remove a local VG to the list of
 *   local VGs, do that directly: add_local_vg()/rem_local_vg().
 *
 * . Other misc actions are are passed to the worker_thread:
 *   add_work_action(act).
 *
 * Onec the client_thread has passed the action off to another
 * thread to process, it goes back to waiting for more client
 * handling work to do.
 *
 * The thread that was given the action by the client_thread
 * now processes that action according to the operation, act->op.
 * This is either a lockspace_thread (for lock ops or ops that
 * add/rem a lockspace), or the worker_thread.  See below for
 * how these ops are processed by these threads.  When the
 * given thread is done processing the action, the result is
 * set in act->result, and the act struct for the completed action
 * is passed back to the client_thread (client_results list).
 *
 * The client_thread takes completed actions (from client_results
 * list), and sends the result back to the client that sent the
 * request represented by the action.  The act struct is then freed.
 *
 * This completes the cycle of work between lvm commands (clients)
 * and lvmlockd.  In summary:
 *
 * - main process polls for new client connections and new requests
 *   from lvm commands
 * - client_thread reads requests from clients
 * - client_thread creates an action struct for each request
 * - client_thread passes the act to another thread for processing
 * - other threads pass completed act structs back to client_thread
 * - client_thread sends the act result back to the client and frees the act
 *
 *
 * Lockspace threads:
 * Each lockd VG has its own lockspace that contains locks for that VG.
 * Each 'struct lockspace' is managed by a separate lockspace_thread.
 * When the lockspace_thread is first created, the first thing it does
 * is join the lockspace in the lock manager.  This can take a long time.
 * If the join fails, the thread exits.  After the join, the thread
 * enters a loop waiting for lock actions to perform in the lockspace.
 *
 * The request to remove/leave a lockspace causes a flag to be set in
 * the lockspace struct.  When the lockspace_thread sees this flag
 * set, it leaves the lockspace, and exits.
 *
 * When the client_thread passes a new action to a lockspace_thread,
 * i.e. a new lock request, the lockspace_thread identifies which resource
 * is being locked (GL, VG, LV), and gets the 'struct resource' (r) for it.
 * r->type will be LD_RT_GL, LD_RT_VG, or LD_RT_LV.  r->name is the
 * resource name, and is fixed for GL and VG resources, but is based on
 * the LV name for LV resources.  The act is added to the resource's
 * list of actions: r->actions, i.e. outstanding lock requests on the
 * resource.
 *
 * The lockspace thread then iterates through each resource in the
 * lockspace, processing any outstanding actions on each: res_process(ls, r).
 *
 * res_process() compares the outstanding actions/requests in r->actions
 * against any existing locks on the resource in r->locks.  If the
 * action is blocked by existing locks, it's left on r->actions.  If not,
 * the action/request is passed to the lock manager.  If the result from
 * the lock manager is success, a new 'struct lock' is created for the
 * action and saved on r->locks.  The result is set in act->result and
 * the act is passed back to the client_thread to be returned to the client.
 */

static const char *lvmlockd_protocol = "lvmlockd";
static const int lvmlockd_protocol_version = 1;
static int daemon_quit;
static char *our_system_id;
static int adopt_opt;

static daemon_handle lvmetad_handle;
static pthread_mutex_t lvmetad_mutex;
static int lvmetad_connected;

/*
 * We use a separate socket for dumping daemon info.
 * This will not interfere with normal operations, and allows
 * free-form debug data to be dumped instead of the libdaemon
 * protocol that wants all data in the cft format.
 * 1MB should fit all the info we need to dump.
 */
#define DUMP_SOCKET_NAME "lvmlockd-dump.sock"
#define DUMP_BUF_SIZE (1024 * 1024)
static char dump_buf[DUMP_BUF_SIZE];
static struct sockaddr_un dump_addr;
static socklen_t dump_addrlen;

/*
 * Main program polls client connections, adds new clients,
 * adds work for client thread.
 *
 * pollfd_mutex is used for adding vs removing entries,
 * and for resume vs realloc.
 */
#define POLL_FD_UNUSED -1		/* slot if free */
#define POLL_FD_IGNORE -2		/* slot is used but ignore in poll */
#define ADD_POLL_SIZE 16		/* increment slots by this amount */

static pthread_mutex_t pollfd_mutex;
static struct pollfd *pollfd;
static int pollfd_size;
static int pollfd_maxi;
static int listen_pi;
static int listen_fd;
static int restart_pi;
static int restart_fds[2];

/*
 * Each lockspace has its own thread to do locking.
 * The lockspace thread makes synchronous lock requests to dlm/sanlock.
 * Every vg with a lockd type, i.e. "dlm", "sanlock", should be on this list.
 *
 * lockspaces_inactive holds old ls structs for vgs that have been
 * stopped, or for vgs that failed to start.  The old ls structs
 * are removed from the inactive list and freed when a new ls with
 * the same name is started and added to the standard lockspaces list.
 * Keeping this bit of "history" for the ls allows us to return a
 * more informative error message if a vg lock request is made for
 * an ls that has been stopped or failed to start.
 */
static pthread_mutex_t lockspaces_mutex;
static struct list_head lockspaces;
static struct list_head lockspaces_inactive;

/*
 * This flag is set to 1 if we see multiple vgs with the global
 * lock enabled.  While this is set, we return a special flag
 * with the vg lock result indicating to the lvm command that
 * there is a duplicate gl in the vg which should be resolved.
 * While this is set, find_lockspace_name has the side job of
 * counting the number of lockspaces with enabled gl's so that
 * this can be set back to zero when the duplicates are disabled.
 */
static int sanlock_gl_dup;

/*
 * VG's that do not have a lockd type are on the local_vgs list.
 * Every vg on the system should be in either the lockspaces
 * list or the local_vgs list.
 *
 * lvm commands send lock requests to lvmlockd for local vgs
 * because at the point locks are acquired in the command,
 * the vg has not been read, so the command does not know if
 * the vg's lock_type is local and the locks can be skipped.
 * So lvmlockd keeps track of which vg's are local so it can
 * quickly check if a vg lock request can be skipped.  (Rather
 * than having to look up the lock_type in lvmetad for every
 * operation on a local vg.)
 *
 * When local_thread_also is set, lvmlockd's local_thread is
 * used to manage locks for local pids on vgs from local_vgs.
 * (In addition to standard locking for lockd type vgs.)
 *
 * When local_thread_only is set, lvmlockd is only used to
 * manage locks for local pids on vgs from local_vgs, and
 * not to manage lockd type vgs.
 *
 * local locking:
 *
 * lock_gl: only do local_thread locking for gl when local_thread_only
 * is set.  local_thread_only means that no standard lockd lockspaces
 * are being used, and lvmlockd is used only for inter-pid locking.
 * When local_thread_only is not set (meaning both local and shared vgs
 * are expected), then the standard gl lockspace works for both local
 * (between local pids) and remote (between pids on different nodes).
 *
 * lock_vg: only do local_thread locking for local, non-lockd, vgs in
 * the local_vgs list.  When the vg is a lockd-type, then the standard
 * lockspace thread works for locking between pids also.
 *
 * local_thread_only=1 local_thread_also=1
 * Use lvmlockd for locking only between local pids, both gl and vg locks.
 * No shared disks or lockd type vgs should exist.
 *
 * local_thread_only=0 local_thread_also=1
 * Use lvmlockd for locking between local pids for local vgs,
 * and use lvmlockd for distributed locking for lockd-type vgs.
 * Use global lock from a lockd-type vgs.  A local-only gl does
 * not make sense here.
 *
 * local_thread_only=0 local_thread_also=0
 * Do not use lvmlockd for locking between local pids.
 * No shared disks or lockd type vgs should exist.
 * (lvmlockd should probably not be run at all in this case.)
 *
 * local_thread_only=1 local_thread_also=0
 * Not allowed.
 */
static pthread_t local_thread;
static pthread_mutex_t local_thread_mutex;
static pthread_cond_t local_thread_cond;
static struct list_head local_thread_actions;
static struct list_head local_vgs;
static struct lockspace *local_thread_gls;
static int local_thread_also;
static int local_thread_only;
static int local_thread_stop;
static int local_thread_work;

/*
 * Client thread reads client requests and writes client results.
 */
static pthread_t client_thread;
static pthread_mutex_t client_mutex;
static pthread_cond_t client_cond;
static struct list_head client_list;    /* connected clients */
static struct list_head client_results; /* actions to send back to clients */
static uint32_t client_ids;             /* 0 and ADOPT_CLIENT_ID are skipped */
static int client_stop;                 /* stop the thread */
static int client_work;                 /* a client on client_list has work to do */

#define ADOPT_CLIENT_ID 0xFFFFFFFF      /* special client_id for adopt actions */
static struct list_head adopt_results;  /* special start actions from adopt_locks() */

/*
 * Worker thread performs misc non-locking actions, e.g. init/free.
 */
static pthread_t worker_thread;
static pthread_mutex_t worker_mutex;
static pthread_cond_t worker_cond;
static struct list_head worker_list;    /* actions for worker_thread */
static int worker_stop;                 /* stop the thread */
static int worker_wake;                 /* wake the thread without adding work */

/*
 * The content of every log_foo() statement is saved in the
 * circular buffer, which can be dumped to a client and printed.
 */
#define LOG_LINE_SIZE 256
#define LOG_DUMP_SIZE DUMP_BUF_SIZE
static char log_dump[LOG_DUMP_SIZE];
static unsigned int log_point;
static unsigned int log_wrap;
static pthread_mutex_t log_mutex;
static int syslog_priority = LOG_WARNING;

/*
 * Structure pools to avoid repeated malloc/free.
 */
#define MAX_UNUSED_ACTION 64
#define MAX_UNUSED_CLIENT 64
#define MAX_UNUSED_RESOURCE 64
#define MAX_UNUSED_LOCK 64
static pthread_mutex_t unused_struct_mutex;
static struct list_head unused_action;
static struct list_head unused_client;
static struct list_head unused_resource;
static struct list_head unused_lock;
static int unused_action_count;
static int unused_client_count;
static int unused_resource_count;
static int unused_lock_count;
static int resource_lm_data_size; /* max size of lm_data from sanlock|dlm */

#define DO_STOP 1
#define NO_STOP 0
#define DO_FREE 1
#define NO_FREE 0
#define DO_FORCE 1
#define NO_FORCE 0

static int add_lock_action(struct action *act);
static int str_to_lm(const char *str);
static void clear_lockspace_inactive(char *name);

static uint64_t monotime(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts.tv_sec;
}

static void log_save_line(int len, char *line,
			  char *log_buf, unsigned int *point, unsigned int *wrap)
{
	unsigned int p = *point;
	unsigned int w = *wrap;
	int i;

	if (len < LOG_DUMP_SIZE - p) {
		memcpy(log_buf + p, line, len);
		p += len;

		if (p == LOG_DUMP_SIZE) {
			p = 0;
			w = 1;
		}
		goto out;
	}

	for (i = 0; i < len; i++) {
		log_buf[p++] = line[i];

		if (p == LOG_DUMP_SIZE) {
			p = 0;
			w = 1;
		}
	}
 out:
	*point = p;
	*wrap = w;
}

void log_level(int level, const char *fmt, ...)
{
	char line[LOG_LINE_SIZE];
	va_list ap;
	int len = LOG_LINE_SIZE - 1;
	int ret, pos = 0;

	memset(line, 0, sizeof(line));

	ret = snprintf(line, len, "%llu ", (unsigned long long)time(NULL));
	pos += ret;

	va_start(ap, fmt);
	ret = vsnprintf(line + pos, len - pos, fmt, ap);
	va_end(ap);

	if (ret >= len - pos)
		pos = len - 1;
	else
		pos += ret;

	line[pos++] = '\n';
	line[pos++] = '\0';

	pthread_mutex_lock(&log_mutex);
	log_save_line(pos - 1, line, log_dump, &log_point, &log_wrap);
	pthread_mutex_unlock(&log_mutex);

	if (level <= syslog_priority)
		syslog(level, "%s", line);

	if (daemon_debug)
		fprintf(stderr, "%s", line);
}

static int dump_log(int *dump_len)
{
	int tail_len;

	pthread_mutex_lock(&log_mutex);

	if (!log_wrap && !log_point) {
		*dump_len = 0;
	} else if (log_wrap) {
		tail_len = LOG_DUMP_SIZE - log_point;
		memcpy(dump_buf, log_dump+log_point, tail_len);
		if (log_point)
			memcpy(dump_buf+tail_len, log_dump, log_point);
		*dump_len = LOG_DUMP_SIZE;
	} else {
		memcpy(dump_buf, log_dump, log_point-1);
		*dump_len = log_point-1;
	}
	pthread_mutex_unlock(&log_mutex);

	return 0;
}

struct lockspace *alloc_lockspace(void)
{
	struct lockspace *ls;

	if (!(ls = malloc(sizeof(struct lockspace)))) {
		log_error("out of memory for lockspace");
		return NULL;
	}

	memset(ls, 0, sizeof(struct lockspace));
	INIT_LIST_HEAD(&ls->actions);
	INIT_LIST_HEAD(&ls->resources);
	pthread_mutex_init(&ls->mutex, NULL);
	pthread_cond_init(&ls->cond, NULL);
	return ls;
}

static struct action *alloc_action(void)
{
	struct action *act;

	pthread_mutex_lock(&unused_struct_mutex);
	if (!unused_action_count) {
		act = malloc(sizeof(struct action));
	} else {
		act = list_first_entry(&unused_action, struct action, list);
		list_del(&act->list);
		unused_action_count--;
	}
	pthread_mutex_unlock(&unused_struct_mutex);
	if (act)
		memset(act, 0, sizeof(struct action));
	else
		log_error("out of memory for action");
	return act;
}

static struct client *alloc_client(void)
{
	struct client *cl;

	pthread_mutex_lock(&unused_struct_mutex);
	if (!unused_client_count) {
		cl = malloc(sizeof(struct client));
	} else {
		cl = list_first_entry(&unused_client, struct client, list);
		list_del(&cl->list);
		unused_client_count--;
	}
	pthread_mutex_unlock(&unused_struct_mutex);
	if (cl)
		memset(cl, 0, sizeof(struct client));
	else
		log_error("out of memory for client");
	return cl;
}

static struct resource *alloc_resource(void)
{
	struct resource *r;

	pthread_mutex_lock(&unused_struct_mutex);
	if (!unused_resource_count) {
		r = malloc(sizeof(struct resource) + resource_lm_data_size);
	} else {
		r = list_first_entry(&unused_resource, struct resource, list);
		list_del(&r->list);
		unused_resource_count--;
	}
	pthread_mutex_unlock(&unused_struct_mutex);
	if (r) {
		memset(r, 0, sizeof(struct resource) + resource_lm_data_size);
		INIT_LIST_HEAD(&r->locks);
		INIT_LIST_HEAD(&r->actions);
	} else {
		log_error("out of memory for resource");
	}
	return r;
}

static struct lock *alloc_lock(void)
{
	struct lock *lk;

	pthread_mutex_lock(&unused_struct_mutex);
	if (!unused_lock_count) {
		lk = malloc(sizeof(struct lock));
	} else {
		lk = list_first_entry(&unused_lock, struct lock, list);
		list_del(&lk->list);
		unused_lock_count--;
	}
	pthread_mutex_unlock(&unused_struct_mutex);
	if (lk)
		memset(lk, 0, sizeof(struct lock));
	else
		log_error("out of memory for lock");
	return lk;
}

static void free_action(struct action *act)
{
	pthread_mutex_lock(&unused_struct_mutex);
	if (unused_action_count >= MAX_UNUSED_ACTION) {
		free(act);
	} else {
		list_add_tail(&act->list, &unused_action);
		unused_action_count++;
	}
	pthread_mutex_unlock(&unused_struct_mutex);
}

static void free_client(struct client *cl)
{
	pthread_mutex_lock(&unused_struct_mutex);
	if (unused_client_count >= MAX_UNUSED_CLIENT) {
		free(cl);
	} else {
		list_add_tail(&cl->list, &unused_client);
		unused_client_count++;
	}
	pthread_mutex_unlock(&unused_struct_mutex);
}

static void free_resource(struct resource *r)
{
	pthread_mutex_lock(&unused_struct_mutex);
	if (unused_resource_count >= MAX_UNUSED_RESOURCE) {
		free(r);
	} else {
		list_add_tail(&r->list, &unused_resource);
		unused_resource_count++;
	}
	pthread_mutex_unlock(&unused_struct_mutex);
}

static void free_lock(struct lock *lk)
{
	pthread_mutex_lock(&unused_struct_mutex);
	if (unused_lock_count >= MAX_UNUSED_LOCK) {
		free(lk);
	} else {
		list_add_tail(&lk->list, &unused_lock);
		unused_lock_count++;
	}
	pthread_mutex_unlock(&unused_struct_mutex);
}

static int setup_structs(void)
{
	struct action *act;
	struct client *cl;
	struct resource *r;
	struct lock *lk;
	int data_san = lm_data_size_sanlock();
	int data_dlm = lm_data_size_dlm();
	int i;

	resource_lm_data_size = data_san > data_dlm ? data_san : data_dlm;

	pthread_mutex_init(&unused_struct_mutex, NULL);
	INIT_LIST_HEAD(&unused_action);
	INIT_LIST_HEAD(&unused_client);
	INIT_LIST_HEAD(&unused_resource);
	INIT_LIST_HEAD(&unused_lock);

	for (i = 0; i < MAX_UNUSED_ACTION/2; i++) {
		if (!(act = alloc_action()))
			goto fail;
		free_action(act);
	}

	for (i = 0; i < MAX_UNUSED_CLIENT/2; i++) {
		if (!(cl = alloc_client()))
			goto fail;
		free_client(cl);
	}

	for (i = 0; i < MAX_UNUSED_RESOURCE/2; i++) {
		if (!(r = alloc_resource()))
			goto fail;
		free_resource(r);
	}

	for (i = 0; i < MAX_UNUSED_LOCK/2; i++) {
		if (!(lk = alloc_lock()))
			goto fail;
		free_lock(lk);
	}
	return 0;
fail:
	return -ENOMEM;
}

static int add_pollfd(int fd)
{
	int i, new_size;

	pthread_mutex_lock(&pollfd_mutex);
	for (i = 0; i < pollfd_size; i++) {
		if (pollfd[i].fd != POLL_FD_UNUSED)
			continue;

		pollfd[i].fd = fd;
		pollfd[i].events = POLLIN;
		pollfd[i].revents = 0;

		if (i > pollfd_maxi)
			pollfd_maxi = i;

		pthread_mutex_unlock(&pollfd_mutex);
		return i;
	}

	new_size = pollfd_size + ADD_POLL_SIZE;

	pollfd = realloc(pollfd, new_size * sizeof(struct pollfd));
	if (!pollfd) {
		log_error("can't alloc new size %d for pollfd", new_size);
		return -ENOMEM;
	}

	for (i = pollfd_size; i < new_size; i++) {
		pollfd[i].fd = POLL_FD_UNUSED;
		pollfd[i].events = 0;
		pollfd[i].revents = 0;
	}

	i = pollfd_size;
	pollfd[i].fd = fd;
	pollfd[i].events = POLLIN;
	pollfd[i].revents = 0;
	pollfd_maxi = i;

	pollfd_size = new_size;

	pthread_mutex_unlock(&pollfd_mutex);
	return i;
}

static void rem_pollfd(int pi)
{
	if (pi < 0) {
		log_error("rem_pollfd %d", pi);
		return;
	}
	pthread_mutex_lock(&pollfd_mutex);
	pollfd[pi].fd = POLL_FD_UNUSED;
	pollfd[pi].events = 0;
	pollfd[pi].revents = 0;
	pthread_mutex_unlock(&pollfd_mutex);
}

static const char *lm_str(int x)
{
	switch (x) {
	case LD_LM_NONE:
		return "none";
	case LD_LM_DLM:
		return "dlm";
	case LD_LM_SANLOCK:
		return "sanlock";
	default:
		return "lm_unknown";
	}
}

static const char *rt_str(int x)
{
	switch (x) {
	case LD_RT_GL:
		return "gl";
	case LD_RT_VG:
		return "vg";
	case LD_RT_LV:
		return "lv";
	default:
		return ".";
	};
}

static const char *op_str(int x)
{
	switch (x) {
	case LD_OP_INIT:
		return "init";
	case LD_OP_FREE:
		return "free";
	case LD_OP_START:
		return "start";
	case LD_OP_STOP:
		return "stop";
	case LD_OP_LOCK:
		return "lock";
	case LD_OP_UPDATE:
		return "update";
	case LD_OP_CLOSE:
		return "close";
	case LD_OP_ENABLE:
		return "enable";
	case LD_OP_DISABLE:
		return "disable";
	case LD_OP_ADD_LOCAL:
		return "add_local";
	case LD_OP_REM_LOCAL:
		return "rem_local";
	case LD_OP_UPDATE_LOCAL:
		return "update_local";
	case LD_OP_START_WAIT:
		return "start_wait";
	case LD_OP_STOP_ALL:
		return "stop_all";
	default:
		return "op_unknown";
	};
}

static const char *mode_str(int x)
{
	switch (x) {
	case LD_LK_IV:
		return "iv";
	case LD_LK_UN:
		return "un";
	case LD_LK_NL:
		return "nl";
	case LD_LK_SH:
		return "sh";
	case LD_LK_EX:
		return "ex";
	default:
		return ".";
	};
}

int last_string_from_args(char *args_in, char *last)
{
	const char *args = args_in;
	const char *colon, *str = NULL;

	while (1) {
		if (!args || (*args == '\0'))
			break;
		colon = strstr(args, ":");
		if (!colon)
			break;
		str = colon;
		args = colon + 1;
        }

	if (str) {
		snprintf(last, MAX_ARGS, "%s", str + 1);
		return 0;
	}
	return -1;
}

int version_from_args(char *args, unsigned int *major, unsigned int *minor, unsigned int *patch)
{
	char version[MAX_ARGS];
	char *major_str, *minor_str, *patch_str;
	char *n, *d1, *d2;

	strncpy(version, args, MAX_ARGS);

	n = strstr(version, ":");
	if (n)
		*n = '\0';

	d1 = strstr(version, ".");
	if (!d1)
		return -1;

	d2 = strstr(d1 + 1, ".");
	if (!d2)
		return -1;

	major_str = version;
	minor_str = d1 + 1;
	patch_str = d2 + 1;

	*d1 = '\0';
	*d2 = '\0';

	if (major)
		*major = atoi(major_str);
	if (minor)
		*minor = atoi(minor_str);
	if (patch)
		*patch = atoi(patch_str);

	return 0;
}

/*
 * These are few enough that arrays of function pointers can
 * be avoided.
 */

static int lm_add_lockspace(struct lockspace *ls, struct action *act, int adopt)
{
	int rv;

	if (ls->lm_type == LD_LM_DLM)
		rv = lm_add_lockspace_dlm(ls, adopt);
	else if (ls->lm_type == LD_LM_SANLOCK)
		rv = lm_add_lockspace_sanlock(ls, adopt);
	else
		return -1;

	if (act)
		act->lm_rv = rv;
	return rv;
}

static int lm_rem_lockspace(struct lockspace *ls, struct action *act, int free_vg)
{
	int rv;

	if (ls->lm_type == LD_LM_DLM)
		rv = lm_rem_lockspace_dlm(ls, free_vg);
	else if (ls->lm_type == LD_LM_SANLOCK)
		rv = lm_rem_lockspace_sanlock(ls, free_vg);
	else
		return -1;

	if (act)
		act->lm_rv = rv;
	return rv;
}

static int lm_lock(struct lockspace *ls, struct resource *r, int mode, struct action *act,
		   uint32_t *r_version, uint32_t *n_version, int *retry, int adopt)
{
	int rv;

	if (ls->lm_type == LD_LM_DLM)
		rv = lm_lock_dlm(ls, r, mode, r_version, n_version, adopt);
	else if (ls->lm_type == LD_LM_SANLOCK)
		rv = lm_lock_sanlock(ls, r, mode, r_version, n_version, retry, adopt);
	else
		return -1;

	if (act)
		act->lm_rv = rv;
	return rv;
}

static int lm_convert(struct lockspace *ls, struct resource *r,
		      int mode, struct action *act, uint32_t r_version)
{
	int rv;

	if (ls->lm_type == LD_LM_DLM)
		rv = lm_convert_dlm(ls, r, mode, r_version);
	else if (ls->lm_type == LD_LM_SANLOCK)
		rv = lm_convert_sanlock(ls, r, mode, r_version);
	else
		return -1;

	if (act)
		act->lm_rv = rv;
	return rv;
}

static int lm_unlock(struct lockspace *ls, struct resource *r, struct action *act,
		     uint32_t r_version, uint32_t n_version, uint32_t lmu_flags)
{
	int rv;

	if (ls->lm_type == LD_LM_DLM)
		return lm_unlock_dlm(ls, r, r_version, n_version, lmu_flags);
	else if (ls->lm_type == LD_LM_SANLOCK)
		return lm_unlock_sanlock(ls, r, r_version, n_version, lmu_flags);
	else
		return -1;

	if (act)
		act->lm_rv = rv;
	return rv;
}

static int lm_hosts(struct lockspace *ls, int notify)
{
	if (ls->lm_type == LD_LM_DLM)
		return 0;
	else if (ls->lm_type == LD_LM_SANLOCK)
		return lm_hosts_sanlock(ls, notify);
	return -1;
}

static void lm_rem_resource(struct lockspace *ls, struct resource *r)
{
	if (ls->lm_type == LD_LM_DLM)
		lm_rem_resource_dlm(ls, r);
	else if (ls->lm_type == LD_LM_SANLOCK)
		lm_rem_resource_sanlock(ls, r);
}

/*
 * While adopting locks, actions originate from the adopt_locks()
 * function, not from a client.  So, these actions (flagged ADOPT),
 * should be passed back to the adopt_locks() function through the
 * adopt_results list, and not be sent back to a client via the
 * client_list/client_thread.
 */

static void add_client_result(struct action *act)
{
	pthread_mutex_lock(&client_mutex);
	if (act->flags & LD_AF_ADOPT)
		list_add_tail(&act->list, &adopt_results);
	else
		list_add_tail(&act->list, &client_results);
	pthread_cond_signal(&client_cond);
	pthread_mutex_unlock(&client_mutex);
}

static struct lock *find_lock_client(struct resource *r, uint32_t client_id)
{
	struct lock *lk;

	list_for_each_entry(lk, &r->locks, list) {
		if (lk->client_id == client_id)
			return lk;
	}
	return NULL;
}

static struct lock *find_lock_persistent(struct resource *r)
{
	struct lock *lk;

	list_for_each_entry(lk, &r->locks, list) {
		if (lk->flags & LD_LF_PERSISTENT)
			return lk;
	}
	return NULL;
}

static struct action *find_action_client(struct resource *r, uint32_t client_id)
{
	struct action *act;

	list_for_each_entry(act, &r->actions, list) {
		if (act->client_id != client_id)
			continue;
		return act;
	}
	return NULL;
}

static void add_work_action(struct action *act)
{
	pthread_mutex_lock(&worker_mutex);
	if (!worker_stop) {
		list_add_tail(&act->list, &worker_list);
		pthread_cond_signal(&worker_cond);
	}
	pthread_mutex_unlock(&worker_mutex);
}

static void create_work_action(int op)
{
	struct action *act;

	if (!(act = alloc_action()))
		return;
	act->op = op;
	add_work_action(act);
}

static int res_lock(struct lockspace *ls, struct resource *r, struct action *act, int *retry)
{
	struct lock *lk;
	uint32_t r_version = 0;
	uint32_t n_version = 0;
	int rv;

	log_debug("S %s R %s res_lock mode %s", ls->name, r->name, mode_str(act->mode));

	if (r->mode == LD_LK_SH && act->mode == LD_LK_SH)
		goto add_lk;

	if (r->type == LD_RT_LV && act->lv_args[0])
		memcpy(r->lv_args, act->lv_args, MAX_ARGS);

	rv = lm_lock(ls, r, act->mode, act, &r_version, &n_version, retry, act->flags & LD_AF_ADOPT);
	if (rv == -EAGAIN)
		return rv;
	if (rv < 0) {
		log_error("S %s R %s res_lock lm error %d", ls->name, r->name, rv);
		return rv;
	}

	log_debug("S %s R %s res_lock lm done r_version %u n_version %u",
		  ls->name, r->name, r_version, n_version);

	/* lm_lock() reads new r_version and n_version */

	if (r_version > r->version) {
		/*
		 * New r_version of the lock: means that another
		 * host has changed data protected by this lock
		 * since the last time we acquired it.  We
		 * should invalidate any local cache of the data
		 * protected by this lock and reread it from disk.
		 */
		r->version = r_version;

		/*
		 * r is vglk: tell lvmetad to set the vg invalid
		 * flag, and provide the new r_version.  If lvmetad finds
		 * that its cached vg has seqno less than the value
		 * we send here, it will set the vg invalid flag.
		 * lvm commands that read the vg from lvmetad, will
		 * see the invalid flag returned, will reread the
		 * vg from disk, update the lvmetad copy, and go on.
		 *
		 * r is global: tell lvmetad to set the global invalid
		 * flag.  When commands see this flag returned from lvmetad,
		 * they will reread metadata from disk, update the lvmetad
		 * caches, and tell lvmetad to set global invalid to 0.
		 */

		if ((r->type == LD_RT_VG) && lvmetad_connected) {
			daemon_reply reply;
			char *uuid;

			log_debug("S %s R %s res_lock set lvmetad vg version %u",
				  ls->name, r->name, r_version);
	
			if (!ls->vg_uuid[0] || !strcmp(ls->vg_uuid, "none"))
				uuid = ls->name;
			else
				uuid = ls->vg_uuid;

			pthread_mutex_lock(&lvmetad_mutex);
			reply = daemon_send_simple(lvmetad_handle, "set_vg_info",
						   "token = %s", "skip",
						   "uuid = %s", uuid,
						   "version = %d", (int)r_version,
						   NULL);
			pthread_mutex_unlock(&lvmetad_mutex);
			/* TODO: check reply? */
			daemon_reply_destroy(reply);
		}

		if ((r->type == LD_RT_GL) && lvmetad_connected) {
			daemon_reply reply;

			log_debug("S %s R %s res_lock set lvmetad global invalid",
				  ls->name, r->name);

			pthread_mutex_lock(&lvmetad_mutex);
			reply = daemon_send_simple(lvmetad_handle, "set_global_info",
						   "token = %s", "skip",
						   "global_invalid = %d", 1,
						   NULL);
			pthread_mutex_unlock(&lvmetad_mutex);
			/* TODO: check reply? */
			daemon_reply_destroy(reply);
		}
	}

	if ((r->type == LD_RT_GL) && (n_version > ls->names_version)) {
		/*
		 * Set a flag that will cause update_local_vgs to be run
		 * when the gl is unlocked (by queueing an UPDATE_LOCK action).
		 * It needs to happen on unlock because lvmetad needs to be updated
		 * by the command before there is an updated vg list to be read.
		 */
		log_debug("S %s gl res_lock set update_local_vgs", ls->name);
		ls->update_local_vgs = 1;
		ls->names_version = n_version;
	}

	if ((r->type == LD_RT_GL) && (act->flags & LD_AF_UPDATE_NAMES_VERSION)) {
		/*
		 * Set a flag that will cause the ls->names_version to be
		 * incremented and written to the gl lvb n_version when
		 * the gl is unlocked.
		 * Other hosts will eventually take the gl lock, see the new
		 * n_version and run update_local_vgs.
		 */
		log_debug("S %s gl res_lock set update_names_version", ls->name);
		ls->update_names_version = 1;
	}

	r->mode = act->mode;

add_lk:
	if (r->mode == LD_LK_SH)
		r->sh_count++;

	if (!(lk = alloc_lock()))
		return -ENOMEM;

	lk->client_id = act->client_id;
	lk->mode = act->mode;

	if (act->flags & LD_AF_PERSISTENT) {
		lk->flags |= LD_LF_PERSISTENT;
		lk->client_id = 0;
	}

	list_add_tail(&lk->list, &r->locks);

	return 0;
}

static int res_convert(struct lockspace *ls, struct resource *r,
		       struct lock *lk, struct action *act)
{
	uint32_t r_version;
	int rv;

	log_debug("S %s R %s res_convert mode %d", ls->name, r->name, act->mode);

	if (act->mode == LD_LK_EX && lk->mode == LD_LK_SH && r->sh_count > 1)
		return -EAGAIN;

	/*
	 * lm_convert() writes new version (from ex)
	 * Same as lm_unlock()
	 */

        if ((r->type == LD_RT_GL) && (r->mode == LD_LK_EX)) {
		r->version++;
		lk->version = r->version;
		r_version = r->version;
		log_debug("S %s R %s res_convert r_version inc %u",
			  ls->name, r->name, r_version);

	} else if ((r->type == LD_RT_VG) && (r->mode == LD_LK_EX) && (lk->version > r->version)) {
		r->version = lk->version;
		r_version = r->version;
		log_debug("S %s R %s res_convert r_version new %u", ls->name, r->name, r_version);
	} else {
		r_version = 0;
	}

	rv = lm_convert(ls, r, act->mode, act, r_version);
	if (rv < 0) {
		log_error("S %s R %s res_convert lm error %d", ls->name, r->name, rv);
		return rv;
	}

	log_debug("S %s R %s res_convert lm done", ls->name, r->name);

	if (lk->mode == LD_LK_EX && act->mode == LD_LK_SH) {
		r->sh_count = 1;
	} else if (lk->mode == LD_LK_SH && act->mode == LD_LK_EX) {
		r->sh_count = 0;
	} else {
		/* should not be possible */
		log_error("S %s R %s res_convert invalid modes %d %d",
			  ls->name, r->name, lk->mode, act->mode);
		return -1;
	}

	r->mode = act->mode;
	lk->mode = act->mode;

	return 0;
}

static int res_cancel(struct lockspace *ls, struct resource *r,
		      struct action *act)
{
	struct action *cact;

	/*
	 * a client can cancel its own non-persistent lock requests,
	 * when could this happen?
	 *
	 * a client can cancel other client's persistent lock requests,
	 * when could this happen?
	 */

	if (act->flags & LD_AF_PERSISTENT) {
		list_for_each_entry(cact, &r->actions, list) {
			if (!(cact->flags & LD_AF_PERSISTENT))
				continue;
			goto do_cancel;
		}
	} else {
		cact = find_action_client(r, act->client_id);
		if (cact)
			goto do_cancel;
	}

	return -ENOENT;

do_cancel:
	log_debug("S %s R %s res_cancel client %d", ls->name, r->name, cact->client_id);
	cact->result = -ECANCELED;
	list_del(&cact->list);
	add_client_result(cact);

	return -ECANCELED;
}

/*
 * lm_unlock() writes new a r_version (from ex)
 *
 * The r_version of the vg resource is incremented if
 * an "update" was received for the vg lock.  The update
 * contains the new vg seqno from the vg metadata which is
 * used as the r_version.
 *
 * The r_version of the global resource is automatically
 * incremented when it is unlocked from ex mode.
 *
 * For the global resource, n_version is used in addition
 * to r_version:
 *
 * r_version is incremented every time a command releases
 * the global lock from ex.
 *
 * n_version is incremented every time a command that
 * changes the list of vg names releases the global lock from ex.
 *
 * Changes to n_version are used by hosts to detect that other
 * hosts have added/removed/renamed local (non-lockd) vgs which
 * can be seen by multiple hosts, so the local_vgs list probably
 * needs to be updated.  lvmlockd knows about changes to lockd-type
 * vgs through their locks, but local vgs do not have locks,
 * so the n_version change is the only way to know that the
 * local_vgs list should be updated.
 */

/*
 * persistent locks will not be unlocked for OP_CLOSE/act_close
 * because act_close->flags does not have the PERSISTENT flag
 * set, and a persistent lk->client_id is zero, which will not
 * match the client in act_close->client_id.
 */

static int res_unlock(struct lockspace *ls, struct resource *r,
		      struct action *act)
{
	struct lock *lk;
	uint32_t r_version;
	uint32_t n_version = 0;
	int rv;

	if (act->flags & LD_AF_PERSISTENT) {
		lk = find_lock_persistent(r);
		if (lk)
			goto do_unlock;
	} else {
		lk = find_lock_client(r, act->client_id);
		if (lk)
			goto do_unlock;
	}

	if (act->op != LD_OP_CLOSE)
		log_error("S %s R %s res_unlock no locks", ls->name, r->name);
	return -ENOENT;

do_unlock:
	log_debug("S %s R %s res_unlock %s", ls->name, r->name,
		  (act->op == LD_OP_CLOSE) ? "from close" : "");

	/* send unlock to lm when last sh lock is unlocked */
	if (lk->mode == LD_LK_SH) {
		r->sh_count--;
		if (r->sh_count > 0)
			goto rem_lk;
	}

	if ((r->type == LD_RT_GL) && (r->mode == LD_LK_EX)) {
		r->version++;
		lk->version = r->version;
		r_version = r->version;

		log_debug("S %s R %s res_unlock r_version inc %u", ls->name, r->name, r_version);

		if (ls->update_names_version) {
			ls->names_version++;
			n_version = ls->names_version;
			log_debug("S %s gl res_unlock got update_names_version %u",
				  ls->name, n_version);
		}

	} else if ((r->type == LD_RT_VG) && (r->mode == LD_LK_EX) && (lk->version > r->version)) {
		r->version = lk->version;
		r_version = r->version;

		log_debug("S %s R %s res_unlock r_version new %u",
			  ls->name, r->name, r_version);
	} else {
		r_version = 0;
	}

	rv = lm_unlock(ls, r, act, r_version, n_version, 0);
	if (rv < 0) {
		/* should never happen, retry? */
		log_error("S %s R %s res_unlock lm error %d", ls->name, r->name, rv);
		return rv;
	}

	log_debug("S %s R %s res_unlock lm done", ls->name, r->name);

	if ((r->type == LD_RT_GL) && (ls->update_local_vgs || ls->update_names_version)) {
		log_debug("S %s gl res_unlock got update_local_vgs %d update_names_version %d",
			  ls->name, ls->update_local_vgs, ls->update_names_version);
		ls->update_local_vgs = 0;
		ls->update_names_version = 0;
		create_work_action(LD_OP_UPDATE_LOCAL);
	}

rem_lk:
	list_del(&lk->list);
	free_lock(lk);

	/*
	 * TODO: if unlock isn't synchronous, and next lock runs into
	 * it, what will the effect be?
	 */

	if (list_empty(&r->locks))
		r->mode = LD_LK_UN;

	return 0;
}

static int res_update(struct lockspace *ls, struct resource *r,
		      struct action *act)
{
	struct lock *lk;

	lk = find_lock_client(r, act->client_id);
	if (!lk) {
		log_error("S %s R %s res_update client %u lock not found",
			  ls->name, r->name, act->client_id);
		return -ENOENT;
	}

	if (r->mode != LD_LK_EX) {
		log_error("S %s R %s res_update version on non-ex lock",
			  ls->name, r->name);
		return -EINVAL;
	}

	/* lk version will be written to lm by unlock */

	/* TODO: try to write it to lm here in some cases?
	 * when a SYNC flag is set for update? */

	if (act->flags & LD_AF_NEXT_VERSION)
		lk->version = r->version + 1;
	else
		lk->version = act->version;

	log_debug("S %s R %s res_update lk version to %u", ls->name, r->name, lk->version);

	return 0;
}

static int free_lv(struct lockspace *ls, struct resource *r)
{
	if (ls->lm_type == LD_LM_SANLOCK)
		return lm_free_lv_sanlock(ls, r);
	else if (ls->lm_type == LD_LM_DLM)
		return 0;
	else
		return -EINVAL;
}

/*
 * NB. we can't do this if sanlock is holding any locks on
 * the resource; we'd be rewriting the resource from under
 * sanlock and would confuse or break it badly.  We don't
 * know what another host is doing, so these must be used
 * very carefully.
 */

static int res_able(struct lockspace *ls, struct resource *r,
		    struct action *act)
{
	int rv;

	if (ls->lm_type != LD_LM_SANLOCK) {
		log_error("enable/disable only applies to sanlock");
		return -EINVAL;
	}

	if (r->type != LD_RT_GL) {
		log_error("enable/disable only applies to global lock");
		return -EINVAL;
	}

	if (r->mode != LD_LK_UN) {
		log_error("enable/disable only allowed on unlocked resource");
		return -EINVAL;
	}

	if (act->op == LD_OP_ENABLE && gl_lsname_sanlock[0]) {
		log_error("disable global lock in %s before enable in %s",
			  gl_lsname_sanlock, ls->name);
		return -EINVAL;
	}

	if ((act->op == LD_OP_DISABLE) && (act->flags & LD_AF_EX_DISABLE)) {
		rv = lm_ex_disable_gl_sanlock(ls);
		goto out;
	}

	rv = lm_able_gl_sanlock(ls, act->op == LD_OP_ENABLE);
out:
	return rv;
}

/*
 * Go through queued actions, and make lock/unlock calls on the resource
 * based on the actions and the existing lock state.
 *
 * All lock operations sent to the lock manager are non-blocking.
 * This is because sanlock does not support lock queueing.
 * Eventually we could enhance this to take advantage of lock
 * queueing when available (i.e. for the dlm).
 *
 * act_close_list: list of CLOSE actions, identifying clients that have
 * closed/terminated their lvmlockd connection, and whose locks should
 * be released.  Do not remove these actions from act_close_list.
 *
 * retry_out: set to 1 if the lock manager said we should retry,
 * meaning we should call res_process() again in a short while to retry.
 */

static void res_process(struct lockspace *ls, struct resource *r,
			struct list_head *act_close_list, int *retry_out)
{
	struct action *act, *safe, *act_close;
	struct lock *lk;
	int lm_retry;
	int rv;

	/*
	 * handle version updates for ex locks
	 * (new version will be written by unlock)
	 */

	list_for_each_entry_safe(act, safe, &r->actions, list) {
		if (act->op == LD_OP_UPDATE) {
			rv = res_update(ls, r, act);
			act->result = rv;
			list_del(&act->list);
			add_client_result(act);
		}
	}

	/*
	 * handle explicit unlock actions
	 */

	list_for_each_entry_safe(act, safe, &r->actions, list) {
		if ((act->op == LD_OP_LOCK) &&
		    (act->mode == LD_LK_IV || act->mode == LD_LK_NL)) {
			act->result = -EINVAL;
			list_del(&act->list);
			add_client_result(act);
		}

		if (act->op == LD_OP_LOCK && act->mode == LD_LK_UN) {
			rv = res_unlock(ls, r, act);

			if (rv == -ENOENT && (act->flags & LD_AF_UNLOCK_CANCEL))
				rv = res_cancel(ls, r, act);

			/*
			 * possible unlock results:
			 * 0: unlock succeeded
			 * -ECANCELED: cancel succeeded
			 * -ENOENT: nothing to unlock or cancel
			 */

			act->result = rv;
			list_del(&act->list);
			add_client_result(act);
		}
	}

	/*
	 * handle implicit unlocks due to client exit,
	 * also clear any outstanding actions for the client
	 */

	list_for_each_entry(act_close, act_close_list, list) {
		res_unlock(ls, r, act_close);
		res_cancel(ls, r, act_close);
	}

	/*
	 * handle freeing a lock for an lv that has been removed
	 */

	list_for_each_entry_safe(act, safe, &r->actions, list) {
		if (act->op == LD_OP_FREE && act->rt == LD_RT_LV) {
			log_debug("S %s R %s free_lv", ls->name, r->name);
			rv = free_lv(ls, r);
			act->result = rv;
			list_del(&act->list);
			add_client_result(act);
			goto r_free;

		}
	}

	/*
	 * handle enable/disable
	 */

	list_for_each_entry_safe(act, safe, &r->actions, list) {
		if (act->op == LD_OP_ENABLE || act->op == LD_OP_DISABLE) {
			rv = res_able(ls, r, act);
			act->result = rv;
			list_del(&act->list);
			add_client_result(act);
		}

		if (!rv && act->op == LD_OP_DISABLE) {
			log_debug("S %s R %s free disabled", ls->name, r->name);
			goto r_free;
		}
	}

	/*
	 * transient requests on existing transient locks
	 */

	list_for_each_entry_safe(act, safe, &r->actions, list) {
		if (act->flags & LD_AF_PERSISTENT)
			continue;

		lk = find_lock_client(r, act->client_id);
		if (!lk)
			continue;

		if (lk->mode != act->mode) {
			/* convert below */
			/*
			act->result = -EEXIST;
			list_del(&act->list);
			add_client_result(act);
			*/
			continue;
		} else {
			/* success */
			act->result = -EALREADY;
			list_del(&act->list);
			add_client_result(act);
		}
	}

	/*
	 * persistent requests on existing persistent locks
	 *
	 * persistent locks are not owned by a client, so any
	 * existing with matching mode satisfies a request.
	 * only one persistent lock is kept on a resource.
	 * a single "unowned" persistent lock satisfies
	 * any/multiple client requests for a persistent lock.
	 */

	list_for_each_entry_safe(act, safe, &r->actions, list) {
		if (!(act->flags & LD_AF_PERSISTENT))
			continue;

		lk = find_lock_persistent(r);
		if (!lk)
			continue;

		if (lk->mode != act->mode) {
			/* convert below */
			/*
			act->result = -EEXIST;
			list_del(&act->list);
			add_client_result(act);
			*/
			continue;
		} else {
			/* success */
			act->result = -EALREADY;
			list_del(&act->list);
			add_client_result(act);
		}
	}

	/*
	 * transient requests with existing persistent locks
	 *
	 * Just grant the transient request and do not
	 * keep a record of it.  Assume that the persistent
	 * lock will not go away while the transient lock
	 * is needed.
	 *
	 * TODO: define exactly when this can be used,
	 * because there are a number of cases where it
	 * will not work: updating version number (lv
	 * locks have none), ex locks from multiple
	 * clients will not conflict, explicit un of the
	 * transient lock will fail.
	 *
	 * This would be used when an ex, persistent lv lock
	 * exists from activation, and then something like
	 * lvextend asks for ex lock to change the lv.  The
	 * lv could not be unlocked by deactivation while
	 * the lvextend was running.
	 */

	list_for_each_entry_safe(act, safe, &r->actions, list) {
		if (act->flags & LD_AF_PERSISTENT)
			continue;

		lk = find_lock_persistent(r);
		if (!lk)
			continue;

		if ((lk->mode == LD_LK_EX) ||
		    (lk->mode == LD_LK_SH && act->mode == LD_LK_SH)) {
			act->result = 0;
			list_del(&act->list);
			add_client_result(act);
		} else {
			/* persistent lock is sh, transient request is ex */
			/* is this case needed? do a convert here? */
			log_debug("res_process %s existing persistent lock new transient", r->name);
			act->result = -EEXIST;
			list_del(&act->list);
			add_client_result(act);
		}
	}

	/*
	 * persistent requests with existing transient locks
	 *
	 * If a client requests a P lock for a T lock it already
	 * holds, we can just change T to P.  Fail if the same
	 * happens for locks from different clients.  Changing
	 * another client's lock from T to P may cause problems
	 * if that client tries to unlock or update version.
	 *
	 * This would be used in a case like vgchange --lock-vg ex vgname
	 * where a transient vg lock was acquired to read the vg,
	 * then the command wants to acquire a persistent lock.
	 * The command could instead unlock, then relock in the mode
	 * it wants, so this case may not be necessary.  Or, lvmlockd
	 * could itself attempt a lock conversion by unlock+relock.
	 */

	list_for_each_entry_safe(act, safe, &r->actions, list) {
		if (!(act->flags & LD_AF_PERSISTENT))
			continue;

		lk = find_lock_client(r, act->client_id);
		if (!lk)
			continue;

		if (lk->mode != act->mode) {
			/* TODO: convert and change to persistent? */
			log_debug("res_process %s existing transient lock new persistent", r->name);
			act->result = -EEXIST;
			list_del(&act->list);
			add_client_result(act);
		} else {
			lk->flags |= LD_LF_PERSISTENT;
			lk->client_id = 0;
			act->result = 0;
			list_del(&act->list);
			add_client_result(act);
		}
	}

	/*
	 * convert mode of existing locks
	 */

	list_for_each_entry_safe(act, safe, &r->actions, list) {
		if (act->flags & LD_AF_PERSISTENT)
			lk = find_lock_persistent(r);
		else
			lk = find_lock_client(r, act->client_id);
		if (!lk)
			continue;

		if (lk->mode == act->mode) {
			/* should never happen, should be found above */
			log_error("convert same mode");
			continue;
		}

		/* convert fails immediately, no EAGAIN retry */
		rv = res_convert(ls, r, lk, act);
		act->result = rv;
		list_del(&act->list);
		add_client_result(act);
	}

	/*
	 * Cases above are all requests addressed by existing locks.
	 * Below handles the rest.  Transient and persistent are
	 * handled the same, except
	 * - if mode of existing lock is incompat with requested,
	 *   leave the act on r->actions
	 * - if r mode is EX, any lock action is blocked, just quit
	 *
	 * Retry a lock request that fails due to a lock conflict (-EAGAIN):
	 * if we have not exceeded max retries and lm sets lm_retry (sanlock
	 * transient conflicts from shared lock implementation), or r type
	 * is gl or vg (transient real conflicts we want to hide from command).
	 * lv lock conflicts won't be transient so don't retry them.
	 */


	if (r->mode == LD_LK_EX)
		return;

	/*
	 * r mode is SH or UN, pass lock-sh actions to lm
	 */

	list_for_each_entry_safe(act, safe, &r->actions, list) {
		/* grant in order, so break here */
		if (act->op == LD_OP_LOCK && act->mode == LD_LK_EX)
			break;

		if (act->op == LD_OP_LOCK && act->mode == LD_LK_SH) {
			lm_retry = 0;

			rv = res_lock(ls, r, act, &lm_retry);
			if ((rv == -EAGAIN) &&
			    (act->retries <= act->max_retries) &&
			    (lm_retry || (r->type != LD_RT_LV))) {
				/* leave act on list */
				log_debug("S %s R %s res_lock EAGAIN retry", ls->name, r->name);
				act->retries++;
				*retry_out = 1;
			} else {
				act->result = rv;
				list_del(&act->list);
				add_client_result(act);
			}
			if (rv == -EUNATCH)
				goto r_free;
		}
	}

	/*
	 * r mode is SH, any ex lock action is blocked, just quit
	 */

	if (r->mode == LD_LK_SH)
		return;

	/*
	 * r mode is UN, pass lock-ex action to lm
	 */

	list_for_each_entry_safe(act, safe, &r->actions, list) {
		if (act->op == LD_OP_LOCK && act->mode == LD_LK_EX) {
			lm_retry = 0;

			rv = res_lock(ls, r, act, &lm_retry);
			if ((rv == -EAGAIN) &&
			    (act->retries <= act->max_retries) &&
			    (lm_retry || (r->type != LD_RT_LV))) {
				/* leave act on list */
				log_debug("S %s R %s res_lock EAGAIN retry", ls->name, r->name);
				act->retries++;
				*retry_out = 1;
			} else {
				act->result = rv;
				list_del(&act->list);
				add_client_result(act);
			}
			if (rv == -EUNATCH)
				goto r_free;
			break;
		}
	}

	return;

r_free:
	/* For the EUNATCH case it may be possible there are queued actions? */
	list_for_each_entry_safe(act, safe, &r->actions, list) {
		log_error("S %s R %s res_process r_free cancel %s client %d",
			  ls->name, r->name, op_str(act->op), act->client_id);
		act->result = -ECANCELED;
		list_del(&act->list);
		add_client_result(act);
	}
	log_debug("S %s R %s res_process free", ls->name, r->name);
	lm_rem_resource(ls, r);
	list_del(&r->list);
	free_resource(r);
}

#define LOCKS_EXIST_ANY 1
#define LOCKS_EXIST_GL  2
#define LOCKS_EXIST_VG  3
#define LOCKS_EXIST_LV  4

static int for_each_lock(struct lockspace *ls, int locks_do)
{
	struct resource *r;
	struct lock *lk;

	list_for_each_entry(r, &ls->resources, list) {
		list_for_each_entry(lk, &r->locks, list) {
			if (locks_do == LOCKS_EXIST_ANY)
				return 1;

			if (locks_do == LOCKS_EXIST_GL && r->type == LD_RT_GL)
				return 1;

			if (locks_do == LOCKS_EXIST_VG && r->type == LD_RT_VG)
				return 1;

			if (locks_do == LOCKS_EXIST_LV && r->type == LD_RT_LV)
				return 1;
		}
	}

	return 0;
}

static int clear_locks(struct lockspace *ls, int free_vg)
{
	struct resource *r, *r_safe;
	struct lock *lk, *lk_safe;
	struct action *act, *act_safe;
	uint32_t lk_version;
	uint32_t r_version;
	int lk_count = 0;
	int rv;

	list_for_each_entry_safe(r, r_safe, &ls->resources, list) {
		lk_version = 0;

		list_for_each_entry_safe(lk, lk_safe, &r->locks, list) {
			lk_count++;

			if (lk->flags & LD_LF_PERSISTENT)
				log_error("S %s R %s clear lock persistent", ls->name, r->name);
			else
				log_error("S %s R %s clear lock client %d", ls->name, r->name, lk->client_id);

			if (lk->version > lk_version)
				lk_version = lk->version;

			list_del(&lk->list);
			free_lock(lk);
		}

		if (r->mode == LD_LK_UN)
			goto r_free;

		if ((r->type == LD_RT_GL) && (r->mode == LD_LK_EX)) {
			r->version++;
			r_version = r->version;
			log_debug("S %s R %s clear_locks r_version inc %u",
				  ls->name, r->name, r_version);

		} else if ((r->type == LD_RT_VG) && (r->mode == LD_LK_EX) && (lk_version > r->version)) {
			r->version = lk_version;
			r_version = r->version;
			log_debug("S %s R %s clear_locks r_version new %u",
				  ls->name, r->name, r_version);

		} else {
			r_version = 0;
		}

		rv = lm_unlock(ls, r, NULL, r_version, 0, free_vg ? LMUF_FREE_VG : 0);
		if (rv < 0) {
			/* should never happen */
			log_error("S %s R %s clear_locks free %d lm unlock error %d",
				  ls->name, r->name, free_vg, rv);
		}

		list_for_each_entry_safe(act, act_safe, &r->actions, list) {
			log_error("S %s R %s clear_locks cancel %s client %d",
				  ls->name, r->name, op_str(act->op), act->client_id);
			act->result = -ECANCELED;
			list_del(&act->list);
			add_client_result(act);
		}
 r_free:
		log_debug("S %s R %s free", ls->name, r->name);
		lm_rem_resource(ls, r);
		list_del(&r->list);
		free_resource(r);
	}

	return lk_count;
}

/*
 * find and return the resource that is referenced by the action
 * - there is a single gl resource per lockspace
 * - there is a single vg resource per lockspace
 * - there can be many lv resources per lockspace, compare names
 */

static struct resource *find_resource_act(struct lockspace *ls,
					  struct action *act,
					  int nocreate)
{
	struct resource *r;

	list_for_each_entry(r, &ls->resources, list) {
		if (r->type != act->rt)
			continue;

		if (r->type == LD_RT_GL && act->rt == LD_RT_GL)
			return r;

		if (r->type == LD_RT_VG && act->rt == LD_RT_VG)
			return r;

		if (r->type == LD_RT_LV && act->rt == LD_RT_LV &&
		    !strcmp(r->name, act->lv_name))
			return r;
	}

	if (nocreate)
		return NULL;

	if (!(r = alloc_resource()))
		return NULL;

	r->type = act->rt;

	r->mode = LD_LK_UN;

	if (r->type == LD_RT_GL)
		strncpy(r->name, R_NAME_GL, MAX_NAME);
	else if (r->type == LD_RT_VG)
		strncpy(r->name, R_NAME_VG, MAX_NAME);
	else if (r->type == LD_RT_LV)
		strncpy(r->name, act->lv_name, MAX_NAME);

	list_add_tail(&r->list, &ls->resources);

	return r;
}

static void free_ls_resources(struct lockspace *ls)
{
	struct resource *r, *r_safe;

	list_for_each_entry_safe(r, r_safe, &ls->resources, list) {
		lm_rem_resource(ls, r);
		list_del(&r->list);
		free_resource(r);
	}
}

/*
 * Process actions queued for this lockspace by
 * client_recv_action / add_lock_action.
 *
 * The lockspace_thread can touch its own ls struct without holding
 * lockspaces_mutex until it sets ls->thread_done, after which it
 * cannot touch ls without holding lockspaces_mutex.
 */

#define LOCK_RETRY_MS 1000 /* milliseconds to delay between retry */

static void *lockspace_thread_main(void *arg_in)
{
	struct lockspace *ls = arg_in;
	struct resource *r, *r2;
	struct action *add_act, *act, *safe;
	struct list_head tmp_act;
	struct list_head act_close;
	int free_vg = 0;
	int error = 0;
	int adopt_flag = 0;
	int wait_flag = 0;
	int retry;
	int rv;

	INIT_LIST_HEAD(&act_close);

	/* first action may be client add */
	pthread_mutex_lock(&ls->mutex);
	act = NULL;
	add_act = NULL;
	if (!list_empty(&ls->actions)) {
		act = list_first_entry(&ls->actions, struct action, list);
		if (act->op == LD_OP_START) {
			add_act = act;
			list_del(&add_act->list);

			if (add_act->flags & LD_AF_WAIT)
				wait_flag = 1;
			if (add_act->flags & LD_AF_ADOPT)
				adopt_flag = 1;
		}
	}
	pthread_mutex_unlock(&ls->mutex);

	log_debug("S %s lm_add_lockspace %s wait %d adopt %d",
		  ls->name, lm_str(ls->lm_type), wait_flag, adopt_flag);

	if (add_act && !wait_flag) {
		/* send partial join result back to client */
		add_act->result = 0;
		add_client_result(add_act);
		add_act = NULL;
	}

	/* the lm join can take a while */

	error = lm_add_lockspace(ls, add_act, adopt_flag);

	log_debug("S %s lm_add_lockspace done %d", ls->name, error);

	if (ls->sanlock_gl_enabled && gl_lsname_sanlock[0] &&
	    strcmp(ls->name, gl_lsname_sanlock))
		sanlock_gl_dup = 1;

	if (add_act) {
		/* send final join result back to client */
		add_act->result = error;
		add_client_result(add_act);
	}

	pthread_mutex_lock(&ls->mutex);
	if (error) {
		ls->thread_stop = 1;
		ls->create_fail = 1;
	} else {
		ls->create_done = 1;
	}
	pthread_mutex_unlock(&ls->mutex);

	if (error)
		goto out_act;

	while (1) {
		pthread_mutex_lock(&ls->mutex);
		while (!ls->thread_work) {
			if (ls->thread_stop) {
				pthread_mutex_unlock(&ls->mutex);
				goto out_rem;
			}
			pthread_cond_wait(&ls->cond, &ls->mutex);
		}

		/* client thread queues actions on ls->actions, we move
		   ls->actions to r->actions, then process the resources */

		while (1) {
			if (list_empty(&ls->actions)) {
				ls->thread_work = 0;
				break;
			}

			act = list_first_entry(&ls->actions, struct action, list);

			if (sanlock_gl_dup && ls->sanlock_gl_enabled)
				act->flags |= LD_AF_DUP_GL_LS;

			if (act->op == LD_OP_STOP) {
				ls->thread_work = 0;
				break;
			}

			if (act->op == LD_OP_FREE && act->rt == LD_RT_VG) {
				/* vgremove */
				log_debug("S %s checking for lockspace hosts", ls->name);
				rv = lm_hosts(ls, 1);
				if (rv) {
					/*
					 * Checking for hosts here in addition to after the
					 * main loop allows vgremove to fail and be rerun
					 * after the ls is stopped on other hosts.
					 */
					log_error("S %s lockspace hosts %d", ls->name, rv);
					list_del(&act->list);
					act->result = -EBUSY;
					add_client_result(act);
					continue;
				}
				ls->thread_work = 0;
				ls->thread_stop = 1;
				free_vg = 1;
				break;
			}

			list_del(&act->list);

			/* applies to all resources */
			if (act->op == LD_OP_CLOSE) {
				list_add(&act->list, &act_close);
				continue;
			}

			/*
			 * Find the specific resource this action refers to;
			 * creates resource if not found.
			 */

			r = find_resource_act(ls, act, (act->op == LD_OP_FREE) ? 1 : 0);
			if (!r) {
				act->result = (act->op == LD_OP_FREE) ? -ENOENT : -ENOMEM;
				add_client_result(act);
				continue;
			}

			list_add_tail(&act->list, &r->actions);

			log_debug("S %s R %s action %s %s", ls->name, r->name,
				  op_str(act->op), mode_str(act->mode));
		}
		pthread_mutex_unlock(&ls->mutex);

		retry = 0;

		list_for_each_entry_safe(r, r2, &ls->resources, list)
			res_process(ls, r, &act_close, &retry);

		list_for_each_entry_safe(act, safe, &act_close, list) {
			list_del(&act->list);
			free_action(act);
		}

		if (retry) {
			ls->thread_work = 1;
			usleep(LOCK_RETRY_MS * 1000);
		}
	}

out_rem:
	log_debug("S %s stopping", ls->name);

	/*
	 * For sanlock, we need to unlock any existing locks
	 * before removing the lockspace, otherwise the sanlock
	 * daemon will kill us when the lockspace goes away.
	 * For dlm, we leave with force, so all locks will
	 * automatically be dropped when we leave the lockspace,
	 * so unlocking all before leaving could be skipped.
	 *
	 * Blindly dropping all existing locks must only be
	 * allowed in emergency/force situations, otherwise it's
	 * obviously dangerous, since the lock holders are still
	 * operating under the assumption that they hold the lock.
	 *
	 * For vgremove of a sanlock vg, the vg lock will be held,
	 * and possibly the gl lock if this vg holds the gl.
	 * sanlock vgremove wants to unlock-rename these locks.
	 */

	log_debug("S %s clearing locks", ls->name);

	rv = clear_locks(ls, free_vg);

	/*
	 * Tell any other hosts in the lockspace to leave it
	 * before we remove it (for vgremove).  We do this
	 * before leaving the lockspace ourself because we
	 * need to be in the lockspace to see others.
	 */

	if (free_vg) {
		log_debug("S %s checking for lockspace hosts", ls->name);
		rv = lm_hosts(ls, 1);
		if (rv)
			log_error("S %s other lockspace hosts %d", ls->name, rv);
	}

	/*
	 * Leave the lockspace.
	 */

	rv = lm_rem_lockspace(ls, NULL, free_vg);

	log_debug("S %s rem_lockspace done %d", ls->name, rv);

out_act:
	/*
	 * Move remaining actions to results; this will usually (always?)
	 * be only the stop action.
	 */
	INIT_LIST_HEAD(&tmp_act);

	pthread_mutex_lock(&ls->mutex);
	list_for_each_entry_safe(act, safe, &ls->actions, list) {
		if (act->op == LD_OP_FREE)
			act->result = 0;
		else if (act->op == LD_OP_STOP)
			act->result = 0;
		else
			act->result = -ENOLS;
		list_del(&act->list);
		list_add_tail(&act->list, &tmp_act);
	}
	pthread_mutex_unlock(&ls->mutex);

	pthread_mutex_lock(&client_mutex);
	list_for_each_entry_safe(act, safe, &tmp_act, list) {
		list_del(&act->list);
		list_add_tail(&act->list, &client_results);
	}
	pthread_cond_signal(&client_cond);
	pthread_mutex_unlock(&client_mutex);

	pthread_mutex_lock(&lockspaces_mutex);
	ls->thread_done = 1;
	pthread_mutex_unlock(&lockspaces_mutex);

	/*
	 * worker_thread will join this thread, and move the
	 * ls struct from lockspaces list to lockspaces_inactive.
	 */
	pthread_mutex_lock(&worker_mutex);
	worker_wake = 1;
	pthread_cond_signal(&worker_cond);
	pthread_mutex_unlock(&worker_mutex);

	return NULL;
}

static void process_local_ls(struct lockspace *ls)
{
	struct resource *r = list_first_entry(&ls->resources, struct resource, list);
	struct action *act, *act_safe;
	struct lock *lk;
	int prev_mode;
	int result;

	list_for_each_entry_safe(act, act_safe, &ls->actions, list) {
		if (act->op != LD_OP_LOCK)
			continue;
		if (act->mode != LD_LK_UN)
			continue;

		result = -ENOENT;

		list_for_each_entry(lk, &r->locks, list) {
			if (lk->client_id != act->client_id)
				continue;
			list_del(&lk->list);
			free_lock(lk);
			result = 0;
			break;
		}

		act->result = result;
		list_del(&act->list);
		add_client_result(act);
	}

	prev_mode = LD_LK_UN;

	if (!list_empty(&r->locks)) {
		lk = list_first_entry(&r->locks, struct lock, list);
		if (lk->mode == LD_LK_EX)
			return;

		/* sanity check */
		if (lk->mode != LD_LK_SH) {
			log_error("process_local_ls bad lk mode %d", lk->mode);
			return;
		}

		prev_mode = LD_LK_SH;
	}

	/* grant lock requests until we reach one that's one not compat with prev_mode */

	list_for_each_entry_safe(act, act_safe, &ls->actions, list) {

		if (act->mode == LD_LK_EX && prev_mode == LD_LK_UN) {
			/* grant it and return because no more can be granted */

			if (!(lk = alloc_lock()))
				return;

			lk->client_id = act->client_id;
			lk->mode = LD_LK_EX;
			list_add(&lk->list, &r->locks);

			act->result = 0;
			list_del(&act->list);
			add_client_result(act);
			return;

		} else if (act->mode == LD_LK_EX && prev_mode == LD_LK_SH) {

			/* we'll process this act and try to grant it the
			   next we come through here. */

			return;

		} else if (act->mode == LD_LK_SH) {
			prev_mode = LD_LK_SH;

			/* grant it and continue */

			if (!(lk = alloc_lock()))
				return;

			lk->client_id = act->client_id;
			lk->mode = LD_LK_SH;
			list_add_tail(&lk->list, &r->locks);

			act->result = 0;
			list_del(&act->list);
			add_client_result(act);
		}
	}
}

static void purge_local_client(uint32_t client_id)
{
	struct lockspace *ls;
	struct resource *r;
	struct lock *lk, *lk_safe;
	struct action *act, *act_safe;

	list_for_each_entry(ls, &local_vgs, list) {
		r = list_first_entry(&ls->resources, struct resource, list);

		list_for_each_entry_safe(lk, lk_safe, &r->locks, list) {
			if (lk->client_id != client_id)
				continue;
			list_del(&lk->list);
			free_lock(lk);
		}

		list_for_each_entry_safe(act, act_safe, &ls->actions, list) {
			if (act->client_id != client_id)
				continue;
			list_del(&act->list);
			free_action(act);
		}
	}
}

static void *local_thread_main(void *arg_in)
{
	struct lockspace *ls;
	struct action *act, *act_safe;

	while (1) {
		pthread_mutex_lock(&local_thread_mutex);
		while (!local_thread_work) {
			if (local_thread_stop) {
				pthread_mutex_unlock(&local_thread_mutex);
				goto out;
			}
			pthread_cond_wait(&local_thread_cond, &local_thread_mutex);
		}

		/* close actions: clear all locks and actions in all lockspaces for client */
		list_for_each_entry_safe(act, act_safe, &local_thread_actions, list) {
			if (act->op != LD_OP_CLOSE)
				continue;
			purge_local_client(act->client_id);
			list_del(&act->list);
			free_action(act);
		}

		list_for_each_entry(ls, &local_vgs, list) {
			if (list_empty(&ls->actions))
				continue;
			process_local_ls(ls);
		}

		local_thread_work = 0;
		pthread_mutex_unlock(&local_thread_mutex);
	}
out:
	return NULL;
}

int lockspaces_empty(void)
{
	int rv;
	pthread_mutex_lock(&lockspaces_mutex);
	rv = list_empty(&lockspaces);
	pthread_mutex_unlock(&lockspaces_mutex);
	return rv;
}

/*
 * lockspaces_mutex is locked
 *
 * When duplicate sanlock global locks have been seen,
 * this function has a secondary job of counting the
 * number of lockspaces that exist with the gl enabled,
 * with the side effect of setting sanlock_gl_dup back to
 * zero when the duplicates have been removed/disabled.
 */

static struct lockspace *find_lockspace_name(char *ls_name)
{
	struct lockspace *ls_found = NULL;
	struct lockspace *ls;
	int gl_count = 0;

	list_for_each_entry(ls, &lockspaces, list) {
		if (!strcmp(ls->name, ls_name))
			ls_found = ls;

		if (!sanlock_gl_dup && ls_found)
			return ls_found;

		if (sanlock_gl_dup && ls->sanlock_gl_enabled)
			gl_count++;
	}

	/* this is the side effect we want from this function */
	if (sanlock_gl_dup && gl_count < 2)
		sanlock_gl_dup = 0;

	return ls_found;
}

/* local_thread_mutex is locked */
static struct lockspace *find_local_vg(const char *name, const char *uuid)
{
	struct lockspace *ls;

	list_for_each_entry(ls, &local_vgs, list) {
		if (name && name[0] && !strcmp(ls->vg_name, name))
			return ls;
		if (uuid && uuid[0] && !strcmp(ls->vg_uuid, uuid))
			return ls;
	}
	return NULL;
}

/*
 * vgcreate/vgremove of local vgs do add_local/rem_local which
 * updates local_vgs on the local host.  Other hosts' local_vgs
 * are updated with these changes asynchronously when they see
 * the n_version change in the global lock lvb, and do
 * update_local_vgs.
 *
 * So, the global lock n_version and update_local_vgs is about
 * asyncronous propagation of add_local/rem_local to other hosts.
 * Because these are local vgs, they are not used concurrently
 * by multiple hosts, but will be used only by the host in the
 * vg's system_id, which is doing the add_local/rem_local.
 *
 * A local vg created on host1 does not need to be immediately
 * usable on host2, and is not locked between hosts anyway.
 * So, returning a not found error on host2 for a while will
 * be ok.  Once node2 asynchronously updates local_vgs, it
 * would know about a new local vg created on host1.  Then
 * lockd_vg on this vg would change from "not found" ENOLS
 * (as above) to -EOTHERVG (or ELOCALVG if no sysid is set,
 * but hosts shouldn't be actively sharing a vg with no
 * lock_type, so an async delay in this case is not a problem.)
 */

/* local_thread_mutex is locked */
static void add_local_vg(const char *vg_name, const char *vg_uuid, const char *vg_sysid)
{
	struct lockspace *ls;
	struct resource *r;

	/* not really a lockspace, we're just reusing the struct */

	if (!vg_name || !vg_uuid || !vg_name[0] || !vg_uuid[0]) {
		log_error("add_local_vg incomplete %s %s",
			  vg_name ? vg_name : "no-name",
			  vg_uuid ? vg_uuid : "no-uuid");
			  
		return;
	}

	if ((ls = find_local_vg(vg_name, vg_uuid))) {
		if (vg_sysid && ls->vg_sysid[0] && !strcmp(vg_sysid, "none")) {
			log_debug("add_local_vg %s %s clear sysid", vg_name, vg_uuid);
			memset(&ls->vg_sysid, 0, MAX_NAME);
		} else if (vg_sysid && strcmp(ls->vg_sysid, vg_sysid)) {
			log_debug("add_local_vg %s %s update %s", vg_name, vg_uuid, vg_sysid);
			strncpy(ls->vg_sysid, vg_sysid, MAX_NAME);
		}
		return;
	}

	if (!(ls = alloc_lockspace()))
		return;

	if (!(r = alloc_resource())) {
		free(ls);
		return;
	}

	strncpy(ls->vg_name, vg_name, MAX_NAME);
	strncpy(ls->vg_uuid, vg_uuid, 64);
	strncpy(ls->vg_sysid, vg_sysid, MAX_NAME);

	r->type = LD_RT_VG;
	r->mode = LD_LK_UN;
	strncpy(r->name, R_NAME_VG, MAX_NAME);
	list_add_tail(&r->list, &ls->resources);

	list_add(&ls->list, &local_vgs);

	log_debug("add_local_vg %s %s %s", vg_name, vg_uuid, vg_sysid ?: "");
}

/* local_thread_mutex is locked */
static void rem_local_vg(const char *vg_name, const char *vg_uuid)
{
	struct lockspace *ls;
	struct resource *r;
	struct lock *lk, *lk_safe;
	struct action *act, *act_safe;

	log_debug("rem_local_vg %s %s", vg_name, vg_uuid);

	if (!(ls = find_local_vg(vg_name, vg_uuid)))
		return;

	r = list_first_entry(&ls->resources, struct resource, list);

	list_for_each_entry_safe(lk, lk_safe, &r->locks, list) {
		list_del(&lk->list);
		free_lock(lk);
	}

	list_del(&r->list);
	free_resource(r);

	list_for_each_entry_safe(act, act_safe, &ls->actions, list) {
		list_del(&act->list);
		free_action(act);
	}

	list_del(&ls->list);
	free(ls);
}

static struct lockspace *find_update_vg(struct list_head *head, const char *name, const char *uuid)
{
	struct lockspace *ls;

	list_for_each_entry(ls, head, list) {
		if (!strcmp(ls->vg_name, name) && !strcmp(ls->vg_uuid, uuid))
			return ls;
	}
	return NULL;
}

/*
 * called by worker_thread. the work action is queued when we see that another
 * host has changed the global lock n_version, which means they have changed the
 * global vg name list, so our local_vgs list may need updating.
 *
 * Handle the issue where a lot of devices all appear together,
 * pvscan is run for each of them to populate lvmetad, each pvscan
 * triggers an update_local, and we end up calling this function many
 * times in a row.  We only really need/want one update_local when all
 * the pvscans are done, and this is a rough approximation of that.
 * If we're asked to do update_local within one second of the previous run,
 * then push it off to the delayed work list, so it will be called in a
 * couple seconds.  Ignore more update_local actions while a delayed
 * update_local action exists.  IOW, if we see two quick back to back
 * update_local actions, delay the second one for a couple seconds in
 * an attempt to buffer more of them which can be eliminated.
 */

static uint64_t last_update_local;

static int work_update_local_vgs(void)
{
	struct list_head update_vgs;
	daemon_reply reply;
	struct dm_config_node *cn;
	struct dm_config_node *metadata;
	struct lockspace *lls, *uls, *safe;
	const char *vg_name;
	const char *vg_uuid;
	const char *lock_type;
	const char *system_id;
	int mutex_unlocked = 0;

	INIT_LIST_HEAD(&update_vgs);

	if (monotime() - last_update_local <= 1)
		return -EAGAIN;

	last_update_local = monotime();

	/* get a list of all vg uuids from lvmetad */

	pthread_mutex_lock(&lvmetad_mutex);
	reply = daemon_send_simple(lvmetad_handle, "vg_list",
				   "token = %s", "skip",
				   NULL);

	if (!(cn = dm_config_find_node(reply.cft->root, "volume_groups"))) {
		log_error("work_update_local no vgs");
		goto out;
	}

	/* create an update_vgs list of all vg uuids */

	for (cn = cn->child; cn; cn = cn->sib) {
		vg_uuid = cn->key;

		if (!(uls = alloc_lockspace()))
			goto out;

		strncpy(uls->vg_uuid, vg_uuid, 64);
		list_add_tail(&uls->list, &update_vgs);
		log_debug("work_update_local %s", vg_uuid);
	}

	daemon_reply_destroy(reply);

	/* get vg_name and system_id for each vg uuid entry in update_vgs */

	list_for_each_entry(uls, &update_vgs, list) {
		reply = daemon_send_simple(lvmetad_handle, "vg_lookup",
					   "token = %s", "skip",
					   "uuid = %s", uls->vg_uuid,
					   NULL);

		vg_name = daemon_reply_str(reply, "name", NULL);
		if (!vg_name) {
			log_error("work_update_local %s no name", uls->vg_uuid);
			goto next;
		}

		strncpy(uls->vg_name, vg_name, MAX_NAME);

		metadata = dm_config_find_node(reply.cft->root, "metadata");
		if (!metadata) {
			log_error("work_update_local %s name %s no metadata",
				  uls->vg_uuid, uls->vg_name);
			goto next;
		}

		lock_type = dm_config_find_str(metadata, "metadata/lock_type", NULL);
		uls->lm_type = str_to_lm(lock_type);

		system_id = dm_config_find_str(metadata, "metadata/system_id", NULL);
		if (system_id)
			strncpy(uls->vg_sysid, system_id, MAX_NAME);
next:
		daemon_reply_destroy(reply);

		log_debug("work_update_local %s lock_type %s %d sysid %s %s",
			  uls->vg_name, lock_type ?: "NULL", uls->lm_type, uls->vg_sysid, uls->vg_uuid);

		if (!vg_name || !metadata)
			goto out;
	}
	pthread_mutex_unlock(&lvmetad_mutex);
	mutex_unlocked = 1;

	/* remove local_vgs entries that no longer exist in update_vgs */

	pthread_mutex_lock(&local_thread_mutex);

	list_for_each_entry_safe(lls, safe, &local_vgs, list) {
		uls = find_update_vg(&update_vgs, lls->vg_name, lls->vg_uuid);
		if (!uls) {
			log_debug("work_update_local remove local_vg %s %s",
				  lls->vg_name, lls->vg_uuid);
			list_del(&lls->list);
			free(lls);

		} else if (uls->lm_type != LD_LM_NONE) {
			log_debug("work_update_local remove local_vg %s %s new lm_type %d",
				  lls->vg_name, lls->vg_uuid, uls->lm_type);
			list_del(&lls->list);
			free(lls);
		}
	}

	/* add local_vgs entries for any new non-lockd entries in update_vgs */

	list_for_each_entry_safe(uls, safe, &update_vgs, list) {
		if (uls->lm_type != LD_LM_NONE)
			continue;
		/* add_local_vg doesn't add any that already exist, it may update sysid */
		add_local_vg(uls->vg_name, uls->vg_uuid, uls->vg_sysid);
	}
	pthread_mutex_unlock(&local_thread_mutex);
out:
	list_for_each_entry_safe(uls, safe, &update_vgs, list) {
		list_del(&uls->list);
		free(uls);
	}

	if (!mutex_unlocked)
		pthread_mutex_unlock(&lvmetad_mutex);

	return 0;
}

/*
 * TODO: we don't use the reply here, so it would be more
 * efficient to send without waiting for a reply.
 */

static void invalidate_lvmetad_vg(struct lockspace *ls)
{
	daemon_reply reply;

	pthread_mutex_lock(&lvmetad_mutex);
	reply = daemon_send_simple(lvmetad_handle, "set_vg_info",
				   "token = %s", "skip",
				   "uuid = %s", ls->vg_uuid,
				   "version = %d", 0,
				   NULL);
	pthread_mutex_unlock(&lvmetad_mutex);
	daemon_reply_destroy(reply);
}

/* TODO: handle lvm_<vg_name> longer than max lockspace name?  Use vg uuid? */

static int vg_ls_name(const char *vg_name, char *ls_name)
{
	if (strlen(vg_name) + 4 > MAX_NAME) {
		log_error("vg name too long %s", vg_name);
		return -1;
	}

	snprintf(ls_name, MAX_NAME, "%s%s", LVM_LS_PREFIX, vg_name);
	return 0;
}

/* TODO: add mutex for gl_lsname_ ? */

static int gl_ls_name(char *ls_name)
{
	if (gl_use_dlm)
		memcpy(ls_name, gl_lsname_dlm, MAX_NAME);
	else if (gl_use_sanlock)
		memcpy(ls_name, gl_lsname_sanlock, MAX_NAME);
	else {
		log_error("gl_ls_name: global lockspace type unknown");
		return -1;
	}
	return 0;
}

/*
 * When this function returns an error, the caller needs to deal
 * with act (in the cases where act exists).
 */

static int add_lockspace_thread(const char *ls_name,
				const char *vg_name,
				const char *vg_uuid,
				int lm_type, const char *vg_args,
				struct action *act)
{
	struct lockspace *ls, *ls2;
	struct resource *r;
	uint32_t version = 0;
	int rv;

	if (act)
		version = act->version;

	log_debug("add_lockspace_thread %s %s version %u",
		  lm_str(lm_type), ls_name, version);

	if (!(ls = alloc_lockspace()))
		return -ENOMEM;

	strncpy(ls->name, ls_name, MAX_NAME);
	ls->lm_type = lm_type;

	if (act)
		ls->start_client_id = act->client_id;

	if (vg_uuid)
		strncpy(ls->vg_uuid, vg_uuid, 64);

	if (vg_name)
		strncpy(ls->vg_name, vg_name, MAX_NAME);

	if (vg_args)
		strncpy(ls->vg_args, vg_args, MAX_ARGS);

	if (act)
		ls->host_id = act->host_id;

	if (!(r = alloc_resource())) {
		free(ls);
		return -ENOMEM;
	}

	r->type = LD_RT_VG;
	r->mode = LD_LK_UN;
	r->version = version;
	strncpy(r->name, R_NAME_VG, MAX_NAME);
	list_add_tail(&r->list, &ls->resources);

	pthread_mutex_lock(&lockspaces_mutex);
	ls2 = find_lockspace_name(ls->name);
	if (ls2) {
		if (ls2->thread_stop)
			rv = -EAGAIN;
		else
			rv = -EEXIST;
		pthread_mutex_unlock(&lockspaces_mutex);
		free_resource(r);
		free(ls);
		return rv;
	}

	/*
	 * act will be null when this lockspace is added automatically/internally
	 * and not by an explicit client action that wants a result.
	 */
	if (act)
		list_add(&act->list, &ls->actions);

	clear_lockspace_inactive(ls->name);

	list_add_tail(&ls->list, &lockspaces);
	pthread_mutex_unlock(&lockspaces_mutex);

	rv = pthread_create(&ls->thread, NULL, lockspace_thread_main, ls);
	if (rv < 0) {
		pthread_mutex_lock(&lockspaces_mutex);
		list_del(&ls->list);
		pthread_mutex_unlock(&lockspaces_mutex);
		free_resource(r);
		free(ls);
		return rv;
	}

	return 0;
}

/*
 * There is no add_sanlock_global_lockspace or
 * rem_sanlock_global_lockspace because with sanlock,
 * the global lockspace is one of the vg lockspaces.
 */

static int add_dlm_global_lockspace(struct action *act)
{
	int rv;

	if (gl_running_dlm)
		return -EEXIST;

	gl_running_dlm = 1;

	/* Keep track of whether we automatically added
	   the global ls, so we know to automatically
	   remove it. */

	if (act)
		gl_auto_dlm = 0;
	else
		gl_auto_dlm = 1;

	/*
	 * There's a short period after which a previous gl lockspace thread
	 * has set gl_running_dlm = 0, but before its ls struct has been
	 * deleted, during which this add_lockspace_thread() can fail with
	 * -EAGAIN.
	 */

	rv = add_lockspace_thread(gl_lsname_dlm, NULL, NULL, LD_LM_DLM, NULL, act);

	if (rv < 0) {
		log_error("add_dlm_global_lockspace add_lockspace_thread %d", rv);
		gl_running_dlm = 0;
		gl_auto_dlm = 0;
	}

	return rv;
}

/*
 * If dlm gl lockspace is the only one left, then stop it.
 * This is not used for an explicit rem_lockspace action from
 * the client, only for auto remove.
 */

static int rem_dlm_global_lockspace(void)
{
	struct lockspace *ls, *ls_gl = NULL;
	int others = 0;
	int rv = 0;

	pthread_mutex_lock(&lockspaces_mutex);
	list_for_each_entry(ls, &lockspaces, list) {
		if (!strcmp(ls->name, gl_lsname_dlm)) {
			ls_gl = ls;
			continue;
		}
		if (ls->thread_stop)
			continue;
		others++;
		break;
	}

	if (others) {
		rv = -EAGAIN;
		goto out;
	}

	if (!ls_gl) {
		rv = -ENOENT;
		goto out;
	}

	ls = ls_gl;
	pthread_mutex_lock(&ls->mutex);
	ls->thread_stop = 1;
	ls->thread_work = 1;
	pthread_cond_signal(&ls->cond);
	pthread_mutex_unlock(&ls->mutex);
	rv = 0;
out:
	pthread_mutex_unlock(&lockspaces_mutex);
	return rv;
}

/*
 * When the first dlm lockspace is added for a vg,
 * automatically add a separate dlm lockspace for the
 * global lock if it hasn't been done explicitly.
 * This is to make the dlm global lockspace work similarly to
 * the sanlock global lockspace, which is "automatic" by
 * nature of being one of the vg lockspaces.
 *
 * For sanlock, a separate lockspace is not used for
 * the global lock, but the gl lock lives in a vg
 * lockspace, (although it's recommended to create a
 * special vg dedicated to holding the gl).
 *
 * N.B. for dlm, if this is an add+WAIT action for a vg
 * lockspace, and this triggered the automatic addition
 * of the global lockspace, then the action may complete
 * for the vg ls add, while the gl ls add is still in
 * progress.  If the caller wants to ensure that the
 * gl ls add is complete, they should explicitly add+WAIT
 * the gl ls.
 *
 * If this function returns and error, the caller
 * will queue the act with that error for the client.
 */

static int add_lockspace(struct action *act)
{
	struct lockspace *ls;
	char ls_name[MAX_NAME+1];
	int rv;

	if (local_thread_only) {
		log_error("add_lockspace not allowed local_thread_only");
		return -EINVAL;
	}

	/*
	 * This should not generally happen, but does happen when a vg
	 * lock_type is changed from none to sanlock.
	 */
	pthread_mutex_lock(&local_thread_mutex);
	ls = find_local_vg(act->vg_name, NULL);
	if (ls) {
		log_error("add_lockspace vg %s remove matching local_vg", act->vg_name);
		list_del(&ls->list);
		free_ls_resources(ls);
		free(ls);
	}
	pthread_mutex_unlock(&local_thread_mutex);

	memset(ls_name, 0, sizeof(ls_name));

	if (act->rt == LD_RT_GL) {
		if (gl_use_dlm) {
			rv = add_dlm_global_lockspace(act);
			return rv;
		} else {
			return -EINVAL;
		}
	}

	if (act->rt == LD_RT_VG) {
		if (gl_use_dlm) {
			rv = add_dlm_global_lockspace(NULL);
			if (rv < 0 && rv != -EEXIST)
				return rv;
		}

		vg_ls_name(act->vg_name, ls_name);

		rv = add_lockspace_thread(ls_name, act->vg_name, act->vg_uuid,
					  act->lm_type, act->vg_args,
					  act);

		if (rv)
			log_error("add_lockspace %s add_lockspace_thread %d", ls_name, rv);
		return rv;
	}

	log_error("add_lockspace bad type %d", act->rt);
	return -1;
}

/*
 * vgchange --lock-stop vgname will lock the vg ex, then send a stop,
 * so we exect to find the ex vg lock held here, and will automatically
 * unlock it when stopping.
 *
 * TODO: if the vg contains the gl lock, should we also automatically
 * unlock that when other lockspaces exist?  Or return an error about
 * stopping the vg with the gl lock while other lockspaces are running,
 * and require a force to do that?
 */

static int rem_lockspace(struct action *act)
{
	struct lockspace *ls;
	char ls_name[MAX_NAME+1];
	int force = act->flags & LD_AF_FORCE;
	int rt = act->rt;

	if (act->rt == LD_RT_GL && act->lm_type != LD_LM_DLM)
		return -EINVAL;

	memset(ls_name, 0, sizeof(ls_name));

	if (act->rt == LD_RT_GL)
		gl_ls_name(ls_name);
	else
		vg_ls_name(act->vg_name, ls_name);

	pthread_mutex_lock(&lockspaces_mutex);
	ls = find_lockspace_name(ls_name);
	if (!ls) {
		pthread_mutex_unlock(&lockspaces_mutex);
		return -ENOLS;
	}

	pthread_mutex_lock(&ls->mutex);
	if (ls->thread_stop) {
		pthread_mutex_unlock(&ls->mutex);
		pthread_mutex_unlock(&lockspaces_mutex);
		return -ESTALE;
	}

	if (!force && for_each_lock(ls, LOCKS_EXIST_LV)) {
		pthread_mutex_unlock(&ls->mutex);
		pthread_mutex_unlock(&lockspaces_mutex);
		return -EBUSY;
	}
	ls->thread_work = 1;
	ls->thread_stop = 1;
	if (act)
		list_add_tail(&act->list, &ls->actions);
	pthread_cond_signal(&ls->cond);
	pthread_mutex_unlock(&ls->mutex);
	pthread_mutex_unlock(&lockspaces_mutex);

	/*
	 * If the dlm global lockspace was automatically added when
	 * the first dlm vg lockspace was added, then reverse that
	 * by automatically removing the dlm global lockspace when
	 * the last dlm vg lockspace is removed.
	 */

	if (rt == LD_RT_VG && gl_use_dlm && gl_auto_dlm)
		rem_dlm_global_lockspace();

	return 0;
}

/*
 * count how many lockspaces started by this client are still starting;
 * the client will use this to wait for all its start operations to finish
 * (START_WAIT).
 */

static int count_lockspace_starting(uint32_t client_id)
{
	struct lockspace *ls;
	int count = 0;
	int done = 0;
	int fail = 0;

	pthread_mutex_lock(&lockspaces_mutex);
	list_for_each_entry(ls, &lockspaces, list) {
		if (ls->start_client_id != client_id)
			continue;

		if (!ls->create_done && !ls->create_fail) {
			count++;
			continue;
		}

		if (ls->create_done)
			done++;
		if (ls->create_fail)
			fail++;
	}
	pthread_mutex_unlock(&lockspaces_mutex);

	log_debug("count_lockspace_starting client %u count %d done %d fail %d",
		  client_id, count, done, fail);

	return count;
}

/* lockspaces_mutex is held */
static struct lockspace *find_lockspace_inactive(char *ls_name)
{
	struct lockspace *ls;

	list_for_each_entry(ls, &lockspaces_inactive, list) {
		if (!strcmp(ls->name, ls_name))
			return ls;
	}

	return NULL;
}

/* lockspaces_mutex is held */
static void clear_lockspace_inactive(char *ls_name)
{
	struct lockspace *ls;

	ls = find_lockspace_inactive(ls_name);
	if (ls) {
		list_del(&ls->list);
		free(ls);
	}
}

static void free_lockspaces_inactive(void)
{
	struct lockspace *ls, *safe;

	pthread_mutex_lock(&lockspaces_mutex);
	list_for_each_entry_safe(ls, safe, &lockspaces_inactive, list) {
		list_del(&ls->list);
		free(ls);
	}
	pthread_mutex_unlock(&lockspaces_mutex);
}

/*
 * Loop through all lockspaces, and:
 * - if do_stop is set, stop any that are not stopped
 * - if do_free is set, join any that are done stopping (and free ls)
 *
 * do_stop will not stop an ls with lv locks unless force is set.
 *
 * This function does not block or wait for anything.
 *
 * do_stop (no do_free):
 * returns count of lockspaces that need stop (have locks and no force)
 *
 * do_free (no do_stop):
 * returns count of lockspaces that are stopped and need freeing
 *
 * do_stop and do_free:
 * returns sum of the previous two
 */

static int for_each_lockspace(int do_stop, int do_free, int do_force)
{
	struct lockspace *ls, *safe;
	int need_stop = 0;
	int need_free = 0;
	int stop_count = 0;
	int free_count = 0;
	int done;
	int stop;

	pthread_mutex_lock(&lockspaces_mutex);

	if (do_stop) {
		list_for_each_entry(ls, &lockspaces, list) {

			pthread_mutex_lock(&ls->mutex);
			if (ls->thread_stop) {
				pthread_mutex_unlock(&ls->mutex);
				continue;
			}

			if (!do_force && for_each_lock(ls, LOCKS_EXIST_ANY)) {
				need_stop++;
			} else {
				ls->thread_work = 1;
				ls->thread_stop = 1;
				pthread_cond_signal(&ls->cond);
				stop_count++;
			}
			pthread_mutex_unlock(&ls->mutex);
		}
	}

	if (do_free) {
		list_for_each_entry_safe(ls, safe, &lockspaces, list) {

			pthread_mutex_lock(&ls->mutex);
			done = ls->thread_done;
			stop = ls->thread_stop;
			pthread_mutex_unlock(&ls->mutex);

			/* This ls has locks and force is not set. */
			if (!stop)
				continue;

			/*
			 * Once thread_done is set, we know that the lockspace_thread
			 * will not be using/touching the ls struct.  Any other
			 * thread touches the ls struct under lockspaces_mutex.
			 */
			if (done) {
				pthread_join(ls->thread, NULL);
				list_del(&ls->list);

				/* TODO: remove this if unneeded */
				if (!list_empty(&ls->actions))
					log_error("TODO: free ls actions");

				free_ls_resources(ls);
				list_add(&ls->list, &lockspaces_inactive);
				free_count++;
			} else {
				need_free++;
			}
		}
	}

	if (list_empty(&lockspaces)) {
		if (!gl_type_static) {
			gl_use_dlm = 0;
			gl_use_sanlock = 0;
		}
	}
	pthread_mutex_unlock(&lockspaces_mutex);

	if (stop_count || free_count || need_stop || need_free) {
		log_debug("for_each_lockspace do_stop %d do_free %d "
			  "stop_count %d free_count %d need_stop %d need_free %d",
			  do_stop, do_free, stop_count, free_count, need_stop, need_free);
	}

	return need_stop + need_free;
}

/*
 * This is only called when the daemon is exiting so the sleep/retry
 * loop doesn't have any adverse impact.
 */

static void for_each_lockspace_retry(int do_stop, int do_free, int do_force)
{
	int count;

	while (1) {
		count = for_each_lockspace(do_stop, do_free, do_force);
		if (!count)
			break;

		log_debug("for_each_lockspace_retry remaining %d", count);
		sleep(1);
	}
}

static int work_init_vg(struct action *act)
{
	char ls_name[MAX_NAME+1];
	int rv = 0;

	memset(ls_name, 0, sizeof(ls_name));

	vg_ls_name(act->vg_name, ls_name);

	if (act->lm_type == LD_LM_SANLOCK)
		rv = lm_init_vg_sanlock(ls_name, act->vg_name, act->flags, act->vg_args);
	else if (act->lm_type == LD_LM_DLM)
		rv = lm_init_vg_dlm(ls_name, act->vg_name, act->flags, act->vg_args);
	else
		rv = -EINVAL;

	return rv;
}

static void work_test_gl(void)
{
	struct lockspace *ls;
	int is_enabled = 0;

	pthread_mutex_lock(&lockspaces_mutex);
	list_for_each_entry(ls, &lockspaces, list) {
		if (ls->lm_type != LD_LM_SANLOCK)
			continue;

		pthread_mutex_lock(&ls->mutex);
		if (ls->create_done && !ls->thread_stop) {
			is_enabled = lm_gl_is_enabled(ls);
			if (is_enabled) {
				log_debug("S %s worker found gl_is_enabled", ls->name);
				strncpy(gl_lsname_sanlock, ls->name, MAX_NAME);
			}
		}
		pthread_mutex_unlock(&ls->mutex);

		if (is_enabled)
			break;
	}

	if (!is_enabled)
		log_debug("worker found no gl_is_enabled");
	pthread_mutex_unlock(&lockspaces_mutex);
}

static int work_init_lv(struct action *act)
{
	struct lockspace *ls;
	char ls_name[MAX_NAME+1];
	char vg_args[MAX_ARGS];
	char lv_args[MAX_ARGS];
	int lm_type = 0;
	int rv = 0;

	memset(ls_name, 0, sizeof(ls_name));
	memset(vg_args, 0, MAX_ARGS);
	memset(lv_args, 0, MAX_ARGS);

	vg_ls_name(act->vg_name, ls_name);

	pthread_mutex_lock(&lockspaces_mutex);
	ls = find_lockspace_name(ls_name);
	if (ls) {
		lm_type = ls->lm_type;
		memcpy(vg_args, ls->vg_args, MAX_ARGS);
	}
	pthread_mutex_unlock(&lockspaces_mutex);

	if (!ls) {
		lm_type = act->lm_type;
		memcpy(vg_args, act->vg_args, MAX_ARGS);
	}

	if (act->lm_type != lm_type) {
		log_error("init_lv ls_name %s wrong lm_type %d %d",
			  ls_name, act->lm_type, lm_type);
		return -EINVAL;
	}

	if (lm_type == LD_LM_SANLOCK) {
		rv = lm_init_lv_sanlock(ls_name, act->vg_name, act->lv_name,
					vg_args, lv_args);

		memcpy(act->lv_args, lv_args, MAX_ARGS);
		return rv;

	} else if (act->lm_type == LD_LM_DLM) {
		return 0;
	} else {
		log_error("init_lv ls_name %s bad lm_type %d", ls_name, act->lm_type);
		return -EINVAL;
	}
}

/*
 * When an action is queued for the worker_thread, it is processed right away.
 * After processing, some actions need to be retried again in a short while.
 * These actions are put on the delayed_list, and the worker_thread will
 * process these delayed actions again in SHORT_DELAY_PERIOD.
 */

#define SHORT_DELAY_PERIOD 2
#define LONG_DELAY_PERIOD 60

static void *worker_thread_main(void *arg_in)
{
	struct list_head delayed_list;
	struct timespec ts;
	struct action *act, *safe;
	uint64_t last_delayed_time = 0;
	int delayed_update_local = 0;
	int delay_sec = LONG_DELAY_PERIOD;
	int rv;

	INIT_LIST_HEAD(&delayed_list);

	while (1) {
		pthread_mutex_lock(&worker_mutex);
		clock_gettime(CLOCK_REALTIME, &ts);
		ts.tv_sec += delay_sec;
		rv = 0;
		act = NULL;

		while (list_empty(&worker_list) && !worker_stop && !worker_wake && !rv) {
			rv = pthread_cond_timedwait(&worker_cond, &worker_mutex, &ts);
		}
		worker_wake = 0;

		if (worker_stop) {
			pthread_mutex_unlock(&worker_mutex);
			goto out;
		}

		if (!list_empty(&worker_list)) {
			act = list_first_entry(&worker_list, struct action, list);
			list_del(&act->list);
		}
		pthread_mutex_unlock(&worker_mutex);

		/*
		 * Do new work actions before processing delayed work actions.
		 */

		if (!act)
			goto delayed_work;

		if ((act->op == LD_OP_LOCK) && (act->flags & LD_AF_SEARCH_LS)) {
			/*
			 * worker_thread used as a helper to search existing
			 * sanlock vgs for an enabled gl.
			 */
			log_debug("work search for gl");
			work_test_gl();

			/* try again to find a gl lockspace for this act */
			rv = add_lock_action(act);
			if (rv < 0) {
				act->result = rv;
				add_client_result(act);
			}

		} else if ((act->op == LD_OP_INIT) && (act->rt == LD_RT_VG)) {
			log_debug("work init_vg %s", act->vg_name);
			act->result = work_init_vg(act);
			add_client_result(act);

		} else if ((act->op == LD_OP_INIT) && (act->rt == LD_RT_LV)) {
			log_debug("work init_lv %s/%s", act->vg_name, act->lv_name);
			act->result = work_init_lv(act);
			add_client_result(act);

		} else if (act->op == LD_OP_UPDATE_LOCAL) {
			if (delayed_update_local) {
				log_debug("work update_local ignore repeat");
				free_action(act);
			} else {
				log_debug("work update_local");
				rv = work_update_local_vgs();
				if (rv == -EAGAIN) {
					delayed_update_local = 1;
					list_add(&act->list, &delayed_list);
				} else {
					free_action(act);
				}
			}

		} else if (act->op == LD_OP_START_WAIT) {
			act->result = count_lockspace_starting(act->client_id);
			if (!act->result)
				add_client_result(act);
			else
				list_add(&act->list, &delayed_list);

		} else if (act->op == LD_OP_STOP_ALL) {
			act->result = for_each_lockspace(DO_STOP, DO_FREE, (act->flags & LD_AF_FORCE) ? DO_FORCE : NO_FORCE);
			if (!act->result || !(act->flags & LD_AF_WAIT))
				add_client_result(act);
			else
				list_add(&act->list, &delayed_list);

		} else {
			log_error("work unknown op %d", act->op);
			act->result = -EINVAL;
			add_client_result(act);
		}

 delayed_work:
		/*
		 * We may want to track retry times per action so that
		 * we can delay different actions by different amounts.
		 */

		if (monotime() - last_delayed_time < SHORT_DELAY_PERIOD) {
			delay_sec = 1;
			continue;
		}
		last_delayed_time = monotime();

		list_for_each_entry_safe(act, safe, &delayed_list, list) {
			if (act->op == LD_OP_START_WAIT) {
				log_debug("work delayed start_wait for client %u", act->client_id);
				act->result = count_lockspace_starting(act->client_id);
				if (!act->result) {
					list_del(&act->list);
					add_client_result(act);
				}

			} else if (act->op == LD_OP_UPDATE_LOCAL) {
				log_debug("work delayed update_local");
				rv = work_update_local_vgs();
				if (rv == -EAGAIN)
					continue;
				list_del(&act->list);
				free_action(act);
				delayed_update_local = 0;

			} else if (act->op == LD_OP_STOP_ALL) {
				log_debug("work delayed stop_all");
				act->result = for_each_lockspace(DO_STOP, DO_FREE, (act->flags & LD_AF_FORCE) ? DO_FORCE : NO_FORCE);
				if (!act->result) {
					list_del(&act->list);
					act->result = 0;
					add_client_result(act);
				}
			}
		}

		/*
		 * This is not explicitly queued work, and not delayed work,
		 * but lockspace thread cleanup that's needed when a
		 * lockspace has been stopped/removed or failed to start.
		 */

		for_each_lockspace(NO_STOP, DO_FREE, NO_FORCE);

		if (list_empty(&delayed_list))
			delay_sec = LONG_DELAY_PERIOD;
		else
			delay_sec = 1;
	}
out:
	list_for_each_entry_safe(act, safe, &delayed_list, list) {
		list_del(&act->list);
		free_action(act);
	}

	pthread_mutex_lock(&worker_mutex);
	list_for_each_entry_safe(act, safe, &worker_list, list) {
		list_del(&act->list);
		free_action(act);
	}
	pthread_mutex_unlock(&worker_mutex);
	return NULL;
}

static int setup_worker_thread(void)
{
	int rv;

	INIT_LIST_HEAD(&worker_list);

	pthread_mutex_init(&worker_mutex, NULL);
	pthread_cond_init(&worker_cond, NULL);

	rv = pthread_create(&worker_thread, NULL, worker_thread_main, NULL);
	if (rv)
		return -1;
	return 0;
}

static void close_worker_thread(void)
{
	pthread_mutex_lock(&worker_mutex);
	worker_stop = 1;
	pthread_cond_signal(&worker_cond);
	pthread_mutex_unlock(&worker_mutex);
	pthread_join(worker_thread, NULL);
}

/* client_mutex is locked */
static struct client *find_client_work(void)
{
	struct client *cl;

	list_for_each_entry(cl, &client_list, list) {
		if (cl->recv || cl->dead)
			return cl;
	}
	return NULL;
}

/* client_mutex is locked */
static struct client *find_client_id(uint32_t id)
{
	struct client *cl;

	list_for_each_entry(cl, &client_list, list) {
		if (cl->id == id)
			return cl;
	}
	return NULL;
}

/* client_mutex is locked */
static struct client *find_client_pi(int pi)
{
	struct client *cl;

	list_for_each_entry(cl, &client_list, list) {
		if (cl->pi == pi)
			return cl;
	}
	return NULL;
}

/*
 * wake up poll() because we have added an fd
 * back into pollfd and poll() needs to be restarted
 * to recognize it.
 */
static void restart_poll(void)
{
	write(restart_fds[1], "w", 1);
}

/* poll will take requests from client again, cl->mutex must be held */
static void client_resume(struct client *cl)
{
	if (cl->dead)
		return;

	if (!cl->poll_ignore || cl->fd == -1 || cl->pi == -1) {
		/* shouldn't happen */
		log_error("client_resume %d bad state ig %d fd %d pi %d",
			  cl->id, cl->poll_ignore, cl->fd, cl->pi);
		return;
	}

	pthread_mutex_lock(&pollfd_mutex);
	if (pollfd[cl->pi].fd != POLL_FD_IGNORE) {
		log_error("client_resume %d pi %d fd %d not IGNORE",
			  cl->id, cl->pi, cl->fd);
	}
	pollfd[cl->pi].fd = cl->fd;
	pollfd[cl->pi].events = POLLIN;
	pthread_mutex_unlock(&pollfd_mutex);

	restart_poll();
}

/* called from client_thread, cl->mutex is held */
static void client_send_result(struct client *cl, struct action *act)
{
	response res;
	char result_flags[128];

	if (cl->dead) {
		log_debug("client send %d skip dead", cl->id);
		return;
	}

	memset(result_flags, 0, sizeof(result_flags));

	buffer_init(&res.buffer);

	/*
	 * EUNATCH is returned when the global lock existed,
	 * but had been disabled when we tried to lock it,
	 * so we removed it, and no longer have a gl to lock.
	 */

	if (act->result == -EUNATCH)
		act->result = -ENOLS;

	/*
	 * init_vg with dlm|sanlock returns vg_args
	 * init_lv with sanlock returns lv_args
	 */

	if (act->result == -ENOLS) {
		/*
		 * The lockspace could not be found, in which case
		 * the caller may want to know if any lockspaces exist
		 * or if lockspaces exist, but not one with the global lock.
		 * Given this detail, it may be able to procede without
		 * the lock.
		 */
		pthread_mutex_lock(&lockspaces_mutex);
		if (list_empty(&lockspaces))
			strcat(result_flags, "NO_LOCKSPACES,");
		pthread_mutex_unlock(&lockspaces_mutex);

		if (gl_use_sanlock && !gl_lsname_sanlock[0])
			strcat(result_flags, "NO_GL_LS,");
		else if (gl_use_dlm && !gl_lsname_dlm[0])
			strcat(result_flags, "NO_GL_LS,");
		else
			strcat(result_flags, "NO_GL_LS,");
	}

	if (act->flags & LD_AF_LOCAL_LS)
		strcat(result_flags, "LOCAL_LS,");

	if (act->flags & LD_AF_DUP_GL_LS)
		strcat(result_flags, "DUP_GL_LS,");

	if (act->flags & LD_AF_INACTIVE_LS)
		strcat(result_flags, "INACTIVE_LS,");

	if (act->flags & LD_AF_ADD_LS_ERROR)
		strcat(result_flags, "ADD_LS_ERROR,");
	
	if (act->op == LD_OP_INIT) {
		/*
		 * init is a special case where lock args need
		 * to be passed back to the client.
		 */
		const char *vg_args = "none";
		const char *lv_args = "none";

		if (act->vg_args[0])
			vg_args = act->vg_args;

		if (act->lv_args[0])
			lv_args = act->lv_args;

		log_debug("send %s[%d.%u] %s %s rv %d vg_args %s lv_args %s",
			  cl->name[0] ? cl->name : "client", cl->pid, cl->id,
			  op_str(act->op), rt_str(act->rt),
			  act->result, vg_args ? vg_args : "", lv_args ? lv_args : "");

		res = daemon_reply_simple("OK",
					  "op = %d", act->op,
					  "op_result = %d", act->result,
					  "lm_result = %d", act->lm_rv,
					  "vg_lock_args = %s", vg_args,
					  "lv_lock_args = %s", lv_args,
					  "result_flags = %s", result_flags[0] ? result_flags : "none",
					  NULL);
	} else {
		/*
		 * A normal reply.
		 */

		log_debug("send %s[%d.%u] %s %s rv %d %s %s",
			  cl->name[0] ? cl->name : "client", cl->pid, cl->id,
			  op_str(act->op), rt_str(act->rt),
			  act->result, (act->result == -ENOLS) ? "ENOLS" : "", result_flags);

		res = daemon_reply_simple("OK",
					  "op = %d", act->op,
					  "lock_type = %s", lm_str(act->lm_type),
					  "op_result = %d", act->result,
					  "lm_result = %d", act->lm_rv,
					  "result_flags = %s", result_flags[0] ? result_flags : "none",
					  NULL);
	}

	buffer_write(cl->fd, &res.buffer);
	buffer_destroy(&res.buffer);

	client_resume(cl);
}

/*
 * TODO: optimize common case where a client has only locked
 * a couple of lockspaces.  Keep two lockspace ids in cl->ls_ids
 * and queue OP_CLOSE in only these lockspaces.  If more than two
 * are used, then clear ls_ids and queue for all.
 */

/* called from client_thread */
static void client_purge(struct client *cl)
{
	struct lockspace *ls;
	struct action *act;

	pthread_mutex_lock(&lockspaces_mutex);
	list_for_each_entry(ls, &lockspaces, list) {
		if (!(act = alloc_action()))
			continue;

		act->op = LD_OP_CLOSE;
		act->client_id = cl->id;
		act->flags |= LD_AF_CLIENT_DEAD;

		pthread_mutex_lock(&ls->mutex);
		if (!ls->thread_stop) {
			list_add_tail(&act->list, &ls->actions);
			ls->thread_work = 1;
			pthread_cond_signal(&ls->cond);
		} else {
			free_action(act);
		}
		pthread_mutex_unlock(&ls->mutex);
	}
	pthread_mutex_unlock(&lockspaces_mutex);

	if (local_thread_also) {
		if (!(act = alloc_action()))
			return;

		act->op = LD_OP_CLOSE;
		act->client_id = cl->id;
		act->flags |= LD_AF_CLIENT_DEAD;

		pthread_mutex_lock(&local_thread_mutex);
		list_add_tail(&act->list, &local_thread_actions);
		local_thread_work = 1;
		pthread_cond_signal(&local_thread_cond);
		pthread_mutex_unlock(&local_thread_mutex);
	}
}

static int add_lock_action(struct action *act)
{
	struct lockspace *ls = NULL;
	char ls_name[MAX_NAME+1];

	memset(ls_name, 0, sizeof(ls_name));

	/* Determine which lockspace this action is for, and set ls_name. */

	if (act->rt == LD_RT_GL && gl_use_sanlock &&
	    (act->op == LD_OP_ENABLE || act->op == LD_OP_DISABLE))
		vg_ls_name(act->vg_name, ls_name);
	else if (act->rt == LD_RT_GL)
		gl_ls_name(ls_name);
	else
		vg_ls_name(act->vg_name, ls_name);

 retry:
	pthread_mutex_lock(&lockspaces_mutex);
	if (ls_name[0])
		ls = find_lockspace_name(ls_name);
	if (!ls) {
		int ls_inactive = 0;
		int ls_create_fail = 0;

		ls = find_lockspace_inactive(ls_name);
		if (ls) {
			ls_inactive = 1;
			ls_create_fail = ls->create_fail;
			ls = NULL;
		}
		pthread_mutex_unlock(&lockspaces_mutex);

		if (act->op == LD_OP_UPDATE && act->rt == LD_RT_VG) {
			log_debug("lockspace not found ignored for vg update");
			return -ENOLS;

		} else if (act->flags & LD_AF_SEARCH_LS) {
			/* fail if we've already tried searching for the ls */
			log_error("lockspace search repeated %s", ls_name);
			return -ENOLS;

		} else if (act->op == LD_OP_LOCK && act->rt == LD_RT_GL && gl_use_sanlock) {
			/* gl may have been enabled in an existing vg */
			log_debug("gl lockspace not found check sanlock vgs");
			act->flags |= LD_AF_SEARCH_LS;
			add_work_action(act);
			return 0;

		} else if (act->op == LD_OP_LOCK && act->rt == LD_RT_GL && gl_use_dlm) {
			log_debug("gl lockspace not found add dlm global");
			act->flags |= LD_AF_SEARCH_LS;
			act->flags |= LD_AF_WAIT_STARTING;
			add_dlm_global_lockspace(NULL);
			gl_ls_name(ls_name);
			goto retry;

		} else if (act->op == LD_OP_LOCK && act->mode == LD_LK_UN) {
			log_debug("lockspace not found ignored for unlock");
			return -ENOLS;

		} else if (act->op == LD_OP_LOCK && act->rt == LD_RT_VG && ls_inactive) {
			/* ls has been stopped or previously failed to start */
			log_debug("lockspace inactive create_fail %d %s",
				  ls_create_fail, ls_name);
			act->flags |= LD_AF_INACTIVE_LS;
			if (ls_create_fail)
				act->flags |= LD_AF_ADD_LS_ERROR;
			return -ENOLS;

		} else {
			log_error("lockspace not found %s", ls_name);
			return -ENOLS;
		}
	}

	if (act->lm_type == LD_LM_NONE) {
		/* return to the command the type we are using */
		act->lm_type = ls->lm_type;
	} else if (act->lm_type != ls->lm_type) {
		/* should not happen */
		log_error("S %s add_lock_action bad lm_type %d ls %d",
			  ls_name, act->lm_type, ls->lm_type);
		return -EINVAL;
	}

	pthread_mutex_lock(&ls->mutex);
	if (ls->thread_stop) {
		pthread_mutex_unlock(&ls->mutex);
		pthread_mutex_unlock(&lockspaces_mutex);
		log_error("lockspace is stopping %s", ls_name);
		return -ESTALE;
	}

	if (!ls->create_fail && !ls->create_done && !(act->flags & LD_AF_WAIT_STARTING)) {
		pthread_mutex_unlock(&ls->mutex);
		pthread_mutex_unlock(&lockspaces_mutex);
		log_debug("lockspace is starting %s", ls_name);
		return -ESTARTING;
	}

	list_add_tail(&act->list, &ls->actions);
	ls->thread_work = 1;
	pthread_cond_signal(&ls->cond);
	pthread_mutex_unlock(&ls->mutex);
	pthread_mutex_unlock(&lockspaces_mutex);

	/* lockspace_thread_main / res_process take it from here */

	return 0;
}

static int add_local_lock_action(struct lockspace *ls, struct action *act)
{
	act->flags |= LD_AF_LOCAL_LS;
	pthread_mutex_lock(&local_thread_mutex);
	if (!ls && local_thread_only)
		list_add_tail(&act->list, &local_thread_gls->actions);
	else if (ls)
		list_add_tail(&act->list, &ls->actions);
	local_thread_work = 1;
	pthread_cond_signal(&local_thread_cond);
	pthread_mutex_unlock(&local_thread_mutex);
	return 0;
}

static int str_to_op_rt(const char *req_name, int *op, int *rt)
{
	if (!req_name)
		goto out;

	if (!strcmp(req_name, "hello")) {
		*op = LD_OP_HELLO;
		*rt = 0;
		return 0;
	}
	if (!strcmp(req_name, "quit")) {
		*op = LD_OP_QUIT;
		*rt = 0;
		return 0;
	}
	if (!strcmp(req_name, "info")) {
		*op = LD_OP_DUMP_INFO;
		*rt = 0;
		return 0;
	}
	if (!strcmp(req_name, "dump")) {
		*op = LD_OP_DUMP_LOG;
		*rt = 0;
		return 0;
	}
	if (!strcmp(req_name, "init_vg")) {
		*op = LD_OP_INIT;
		*rt = LD_RT_VG;
		return 0;
	}
	if (!strcmp(req_name, "init_lv")) {
		*op = LD_OP_INIT;
		*rt = LD_RT_LV;
		return 0;
	}
	if (!strcmp(req_name, "free_vg")) {
		*op = LD_OP_FREE;
		*rt = LD_RT_VG;
		return 0;
	}
	if (!strcmp(req_name, "free_lv")) {
		*op = LD_OP_FREE;
		*rt = LD_RT_LV;
		return 0;
	}
	if (!strcmp(req_name, "start_vg")) {
		*op = LD_OP_START;
		*rt = LD_RT_VG;
		return 0;
	}
	if (!strcmp(req_name, "stop_vg")) {
		*op = LD_OP_STOP;
		*rt = LD_RT_VG;
		return 0;
	}
	if (!strcmp(req_name, "start_wait")) {
		*op = LD_OP_START_WAIT;
		*rt = 0;
		return 0;
	}
	if (!strcmp(req_name, "stop_all")) {
		*op = LD_OP_STOP_ALL;
		*rt = 0;
		return 0;
	}
	if (!strcmp(req_name, "lock_gl")) {
		*op = LD_OP_LOCK;
		*rt = LD_RT_GL;
		return 0;
	}
	if (!strcmp(req_name, "lock_vg")) {
		*op = LD_OP_LOCK;
		*rt = LD_RT_VG;
		return 0;
	}
	if (!strcmp(req_name, "lock_lv")) {
		*op = LD_OP_LOCK;
		*rt = LD_RT_LV;
		return 0;
	}
	if (!strcmp(req_name, "vg_update")) {
		*op = LD_OP_UPDATE;
		*rt = LD_RT_VG;
		return 0;
	}
	if (!strcmp(req_name, "enable_gl")) {
		*op = LD_OP_ENABLE;
		*rt = LD_RT_GL;
		return 0;
	}
	if (!strcmp(req_name, "disable_gl")) {
		*op = LD_OP_DISABLE;
		*rt = LD_RT_GL;
		return 0;
	}
	if (!strcmp(req_name, "add_local")) {
		*op = LD_OP_ADD_LOCAL;
		return 0;
	}
	if (!strcmp(req_name, "rem_local")) {
		*op = LD_OP_REM_LOCAL;
		return 0;
	}
	if (!strcmp(req_name, "update_local")) {
		*op = LD_OP_UPDATE_LOCAL;
		return 0;
	}
out:
	return -1;
}

static int str_to_mode(const char *str)
{
	if (!str)
		goto out;
	if (!strcmp(str, "un"))
		return LD_LK_UN;
	if (!strcmp(str, "nl"))
		return LD_LK_NL;
	if (!strcmp(str, "sh"))
		return LD_LK_SH;
	if (!strcmp(str, "ex"))
		return LD_LK_EX;
out:
	return LD_LK_IV;
}

static int str_to_lm(const char *str)
{
	if (!str || !strcmp(str, "none"))
		return LD_LM_NONE;
	if (!strcmp(str, "sanlock"))
		return LD_LM_SANLOCK;
	if (!strcmp(str, "dlm"))
		return LD_LM_DLM;
	return -2; 
}

static uint32_t str_to_opts(const char *str)
{
	uint32_t flags = 0;

	if (!str)
		goto out;
	if (strstr(str, "persistent"))
		flags |= LD_AF_PERSISTENT;
	if (strstr(str, "unlock_cancel"))
		flags |= LD_AF_UNLOCK_CANCEL;
	if (strstr(str, "next_version"))
		flags |= LD_AF_NEXT_VERSION;
	if (strstr(str, "wait"))
		flags |= LD_AF_WAIT;
	if (strstr(str, "force"))
		flags |= LD_AF_FORCE;
	if (strstr(str, "ex_disable"))
		flags |= LD_AF_EX_DISABLE;
	if (strstr(str, "enable"))
		flags |= LD_AF_ENABLE;
	if (strstr(str, "disable"))
		flags |= LD_AF_DISABLE;
	if (strstr(str, "update_names"))
		flags |= LD_AF_UPDATE_NAMES_VERSION;
out:
	return flags;
}

static int is_other_sysid(struct lockspace *lls)
{
	if (!our_system_id || !lls->vg_sysid[0])
		return 0;
	if (!strcmp(lls->vg_sysid, our_system_id))
		return 0;
	return 1;
}


/*
 * dump info
 * client_list: each client struct
 * local_vgs: each lockspace struct (representing a local vg)
 * lockspaces: each lockspace struct
 * lockspace actions: each action struct
 * lockspace resources: each resource struct
 * lockspace resource actions: each action struct
 * lockspace resource locks: each lock struct
 */

static int setup_dump_socket(void)
{
	int s;

	s = socket(AF_LOCAL, SOCK_DGRAM, 0);
	if (s < 0)
		return s;

	memset(&dump_addr, 0, sizeof(dump_addr));
	dump_addr.sun_family = AF_LOCAL;
	strcpy(&dump_addr.sun_path[1], DUMP_SOCKET_NAME);
	dump_addrlen = sizeof(sa_family_t) + strlen(dump_addr.sun_path+1) + 1;

	return s;
}

static int send_dump_buf(int fd, int dump_len)
{
	int pos = 0;
	int ret;

retry:
	ret = sendto(fd, dump_buf + pos, dump_len - pos, MSG_DONTWAIT | MSG_NOSIGNAL,
		     (struct sockaddr *)&dump_addr, dump_addrlen);
	if (ret <= 0)
		return ret;

	pos += ret;

	if (pos < dump_len)
		goto retry;

	return 0;
}

static int print_client(struct client *cl, const char *prefix, int pos, int len)
{
	return snprintf(dump_buf + pos, len - pos,
			"info=%s "
			"pid=%d "
			"fd=%d "
			"pi=%d "
			"id=%u "
			"name=%s\n",
			prefix,
			cl->pid,
			cl->fd,
			cl->pi,
			cl->id,
			cl->name[0] ? cl->name : ".");
}

static int print_local_vg(struct lockspace *ls, const char *prefix, int pos, int len)
{
	return snprintf(dump_buf + pos, len - pos,
			"info=%s "
			"vg_name=%s "
			"vg_uuid=%s "
			"vg_sysid=%s\n",
			prefix,
			ls->vg_name,
			ls->vg_uuid,
			ls->vg_sysid[0] ? ls->vg_sysid : ".");
}

static int print_lockspace(struct lockspace *ls, const char *prefix, int pos, int len)
{
	return snprintf(dump_buf + pos, len - pos,
			"info=%s "
			"ls_name=%s "
			"vg_name=%s "
			"vg_uuid=%s "
			"vg_sysid=%s "
			"vg_args=%s "
			"lm_type=%s "
			"host_id=%llu "
			"names_version=%u "
			"create_fail=%d "
			"create_done=%d "
			"thread_work=%d "
			"thread_stop=%d "
			"thread_done=%d "
			"update_local_vgs=%d "
			"update_names_version=%d "
			"sanlock_gl_enabled=%d "
			"sanlock_gl_dup=%d\n",
			prefix,
			ls->name,
			ls->vg_name,
			ls->vg_uuid,
			ls->vg_sysid[0] ? ls->vg_sysid : ".",
			ls->vg_args,
			lm_str(ls->lm_type),
			(unsigned long long)ls->host_id,
			ls->names_version,
			ls->create_fail ? 1 : 0,
			ls->create_done ? 1 : 0,
			ls->thread_work ? 1 : 0,
			ls->thread_stop ? 1 : 0,
			ls->thread_done ? 1 : 0,
			ls->update_local_vgs ? 1 : 0,
			ls->update_names_version ? 1 : 0,
			ls->sanlock_gl_enabled ? 1 : 0,
			ls->sanlock_gl_dup ? 1 : 0);
}

static int print_action(struct action *act, const char *prefix, int pos, int len)
{
	return snprintf(dump_buf + pos, len - pos,
			"info=%s "
			"client_id=%u "
			"flags=0x%x "
			"version=%u "
			"op=%s "
			"rt=%s "
			"mode=%s "
			"lm_type=%s "
			"result=%d "
			"lm_rv=%d\n",
			prefix,
			act->client_id,
			act->flags,
			act->version,
			op_str(act->op),
			rt_str(act->rt),
			mode_str(act->mode),
			lm_str(act->lm_type),
			act->result,
			act->lm_rv);
}

static int print_resource(struct resource *r, const char *prefix, int pos, int len)
{
	return snprintf(dump_buf + pos, len - pos,
			"info=%s "
			"name=%s "
			"type=%s "
			"mode=%s "
			"sh_count=%d "
			"version=%u\n",
			prefix,
			r->name,
			rt_str(r->type),
			mode_str(r->mode),
			r->sh_count,
			r->version);
}

static int print_lock(struct lock *lk, const char *prefix, int pos, int len)
{
	return snprintf(dump_buf + pos, len - pos,
			"info=%s "
			"mode=%s "
			"version=%u "
			"flags=0x%x "
			"client_id=%u\n",
			prefix,
			mode_str(lk->mode),
			lk->version,
			lk->flags,
			lk->client_id);
}

static int dump_info(int *dump_len)
{
	struct client *cl;
	struct lockspace *ls;
	struct resource *r;
	struct lock *lk;
	struct action *act;
	int len, pos, ret;
	int rv = 0;

	memset(dump_buf, 0, sizeof(dump_buf));
	len = sizeof(dump_buf);
	pos = 0;

	/*
	 * clients
	 */

	pthread_mutex_lock(&client_mutex);
	list_for_each_entry(cl, &client_list, list) {
		ret = print_client(cl, "client", pos, len);
		if (ret >= len - pos) {
			 rv = -ENOSPC;
			 break;
		}
		pos += ret;
	}
	pthread_mutex_unlock(&client_mutex);

	if (rv < 0)
		return rv;

	/*
	 * local vgs
	 */

	pthread_mutex_lock(&lockspaces_mutex);
	list_for_each_entry(ls, &local_vgs, list) {
		ret = print_local_vg(ls, "local_vg", pos, len);
		if (ret >= len - pos) {
			 rv = -ENOSPC;
			 break;
		}
		pos += ret;
	}
	pthread_mutex_unlock(&lockspaces_mutex);

	if (rv < 0)
		return rv;

	/*
	 * lockspaces with their action/resource/lock info
	 */

	pthread_mutex_lock(&lockspaces_mutex);
	list_for_each_entry(ls, &lockspaces, list) {

		ret = print_lockspace(ls, "ls", pos, len);
		if (ret >= len - pos) {
			 rv = -ENOSPC;
			 goto out;
		}
		pos += ret;

		list_for_each_entry(act, &ls->actions, list) {
			ret = print_action(act, "ls_action", pos, len);
			if (ret >= len - pos) {
				rv = -ENOSPC;
				goto out;
			}
			pos += ret;
		}

		list_for_each_entry(r, &ls->resources, list) {
			ret = print_resource(r, "r", pos, len);
			if (ret >= len - pos) {
				rv = -ENOSPC;
				goto out;
			}
			pos += ret;

			list_for_each_entry(lk, &r->locks, list) {
				ret = print_lock(lk, "lk", pos, len);
				if (ret >= len - pos) {
					rv = -ENOSPC;
					goto out;
				}
				pos += ret;
			}

			list_for_each_entry(act, &r->actions, list) {
				ret = print_action(act, "r_action", pos, len);
				if (ret >= len - pos) {
					rv = -ENOSPC;
					goto out;
				}
				pos += ret;
			}
		}
	}
out:
	pthread_mutex_unlock(&lockspaces_mutex);

	*dump_len = pos;

	return rv;
}

/* called from client_thread, cl->mutex is held */
static void client_recv_action(struct client *cl)
{
	request req;
	response res;
	struct lockspace *lls = NULL;
	struct action *act;
	const char *cl_name;
	const char *vg_name;
	const char *vg_uuid;
	const char *vg_sysid;
	const char *str;
	int64_t val;
	uint32_t opts = 0;
	int result = 0;
	int cl_pid;
	int op, rt, lm, mode;
	int rv;

	buffer_init(&req.buffer);

	rv = buffer_read(cl->fd, &req.buffer);
	if (!rv) {
		if (errno == ECONNRESET) {
			log_debug("client recv %d ECONNRESET", cl->id);
			cl->dead = 1;
		} else {
			log_error("client recv %d buffer_read error %d", cl->id, errno);
		}
		buffer_destroy(&req.buffer);
		client_resume(cl);
		return;
	}

	req.cft = dm_config_from_string(req.buffer.mem);
	if (!req.cft) {
		log_error("client recv %d config_from_string error", cl->id);
		buffer_destroy(&req.buffer);
		client_resume(cl);
		return;
	}

	str = daemon_request_str(req, "request", NULL);
	rv = str_to_op_rt(str, &op, &rt);
	if (rv < 0) {
		log_error("client recv %d bad request name \"%s\"", cl->id, str ? str : "");
		dm_config_destroy(req.cft);
		buffer_destroy(&req.buffer);
		client_resume(cl);
		return;
	}

	if (op == LD_OP_HELLO || op == LD_OP_QUIT ||
	    op == LD_OP_DUMP_INFO || op == LD_OP_DUMP_LOG) {

		/*
		 * TODO: add the client command name to the hello messages
		 * so it can be saved in cl->name here.
		 */

		result = 0;

		if (op == LD_OP_QUIT) {
			log_debug("op quit");
			pthread_mutex_lock(&lockspaces_mutex);
		       	if (list_empty(&lockspaces)) {
				daemon_quit = 1;
			} else {
				result = -EBUSY;
			}
			pthread_mutex_unlock(&lockspaces_mutex);
		}

		buffer_init(&res.buffer);

		if (op == LD_OP_DUMP_INFO || op == LD_OP_DUMP_LOG) {
			int dump_len = 0;
			int fd;

			fd = setup_dump_socket();
			if (fd < 0)
				result = fd;
			else if (op == LD_OP_DUMP_INFO)
				result = dump_info(&dump_len);
			else if (op == LD_OP_DUMP_LOG)
				result = dump_log(&dump_len);
			else
				result = -EINVAL;

			res = daemon_reply_simple("OK",
					  "result = %d", result,
					  "dump_len = %d", dump_len,
					  NULL);
			if (fd >= 0) {
				send_dump_buf(fd, dump_len);
				close(fd);
			}

		} else {
			res = daemon_reply_simple("OK",
					  "result = %d", result,
					  "protocol = %s", lvmlockd_protocol,
					  "version = %d", lvmlockd_protocol_version,
					  NULL);
		}

		buffer_write(cl->fd, &res.buffer);
		buffer_destroy(&res.buffer);
		dm_config_destroy(req.cft);
		buffer_destroy(&req.buffer);
		client_resume(cl);
		return;
	}

	cl_name = daemon_request_str(req, "cmd", NULL);
	cl_pid = daemon_request_int(req, "pid", 0);
	vg_name = daemon_request_str(req, "vg_name", NULL);
	vg_uuid = daemon_request_str(req, "vg_uuid", NULL);
	vg_sysid = daemon_request_str(req, "vg_sysid", NULL);
	str = daemon_request_str(req, "mode", NULL);
	mode = str_to_mode(str);
	str = daemon_request_str(req, "opts", NULL);
	opts = str_to_opts(str);
	str = daemon_request_str(req, "vg_lock_type", NULL);
	lm = str_to_lm(str);

	if (cl_pid && cl_pid != cl->pid)
		log_error("client recv bad message pid %d client %d", cl_pid, cl->pid);

	/* TODO: do this in hello message instead */
	if (!cl->name[0] && cl_name)
		strncpy(cl->name, cl_name, MAX_NAME-1);

	if (!our_system_id) {
		str = daemon_request_str(req, "our_system_id", NULL);
		if (str && strcmp(str, "none"))
			our_system_id = strdup(str);
	}

	/*
	 * Detect the common case of a lock op on a local vg and queue
	 * a reply immediately without going through a thread.
	 */

	if (rt == LD_RT_VG && op == LD_OP_LOCK) {
		pthread_mutex_lock(&local_thread_mutex);
		lls = find_local_vg(vg_name, vg_uuid);
		pthread_mutex_unlock(&local_thread_mutex);
		if (lls)
			result = is_other_sysid(lls) ? -EOTHERVG : -ELOCALVG;
	}

	/*
	 * A local vg with no sysid, accessible from multiple hosts, can be
	 * modified without coordination if a user is not careful.  The best we
	 * can do is disable the lvmetad cache for these vgs so any problems are
	 * detected earlier, and not masked by lvmetad caching.
	 */

	if (lls && (result == -ELOCALVG) && !lls->vg_sysid[0])
		invalidate_lvmetad_vg(lls);

	if ((result == -EOTHERVG) || (result == -ELOCALVG && !local_thread_also)) {
		const char *sysid = lls->vg_sysid[0] ? lls->vg_sysid : "none";

		log_debug("local vg %s result %d %s sysid %s", vg_name, result,
			  (result == -EOTHERVG) ? "other" : "local", sysid);

		buffer_init(&res.buffer);
		res = daemon_reply_simple("OK",
					  "op_result = %d", result,
					  "vg_sysid = %s", sysid,
					  "lock_type = %s", "none",
					  "result_flags = %s", "LOCAL_LS",
					  NULL);
		buffer_write(cl->fd, &res.buffer);
		buffer_destroy(&res.buffer);
		dm_config_destroy(req.cft);
		buffer_destroy(&req.buffer);
		client_resume(cl);
		return;
	}

	if (!gl_use_dlm && !gl_use_sanlock && (lm > 0)) {
		if (lm == LD_LM_DLM)
			gl_use_dlm = 1;
		else if (lm == LD_LM_SANLOCK)
			gl_use_sanlock = 1;

		log_debug("set gl_use_%s", lm_str(lm));
	}

	if (!(act = alloc_action())) {
		/* TODO: return an error to the client */
		dm_config_destroy(req.cft);
		buffer_destroy(&req.buffer);
		client_resume(cl);
		return;
	}

	act->client_id = cl->id;
	act->op = op;
	act->rt = rt;
	act->mode = mode;
	act->flags = opts;
	act->lm_type = lm;

	if (vg_name && strcmp(vg_name, "none"))
		strncpy(act->vg_name, vg_name, MAX_NAME);

	if (vg_uuid && strcmp(vg_uuid, "none"))
		strncpy(act->vg_uuid, vg_uuid, 64);

	if (vg_sysid && strcmp(vg_sysid, "none"))
		strncpy(act->vg_sysid, vg_sysid, MAX_NAME);

	str = daemon_request_str(req, "lv_name", NULL);
	if (str && strcmp(str, "none"))
		strncpy(act->lv_name, str, MAX_NAME);

	val = daemon_request_int(req, "version", 0);
	if (val)
		act->version = (uint32_t)val;

	str = daemon_request_str(req, "vg_lock_args", NULL);
	if (str && strcmp(str, "none"))
		strncpy(act->vg_args, str, MAX_ARGS);

	str = daemon_request_str(req, "lv_lock_args", NULL);
	if (str && strcmp(str, "none"))
		strncpy(act->lv_args, str, MAX_ARGS);

	/* start_vg will include lvmlocal.conf local/host_id here */
	val = daemon_request_int(req, "host_id", 0);
	if (val)
		act->host_id = val;

	act->max_retries = daemon_request_int(req, "max_retries", DEFAULT_MAX_RETRIES);

	dm_config_destroy(req.cft);
	buffer_destroy(&req.buffer);

	log_debug("recv %s[%d.%u] %s %s \"%s\" mode %s flags %x",
		  cl->name[0] ? cl->name : "client", cl->pid, cl->id,
		  op_str(act->op), rt_str(act->rt), act->vg_name, mode_str(act->mode), opts);

	/*
	 * local lock on local vg (lls) is done when local locking is enabled.
	 * local lock on gl is done when local locking is enabled and lockd is not.
	 */
	if ((local_thread_also && lls) ||
	    (local_thread_only && rt == LD_RT_GL && op == LD_OP_LOCK)) {
		add_local_lock_action(lls, act);
		return;
	}

	switch (act->op) {
	case LD_OP_START:
		rv = add_lockspace(act);
		break;
	case LD_OP_STOP:
		rv = rem_lockspace(act);
		break;
	case LD_OP_INIT:
	case LD_OP_UPDATE_LOCAL:
	case LD_OP_START_WAIT:
	case LD_OP_STOP_ALL:
		add_work_action(act);
		rv = 0;
		break;
	case LD_OP_LOCK:
	case LD_OP_UPDATE:
	case LD_OP_ENABLE:
	case LD_OP_DISABLE:
	case LD_OP_FREE:
		rv = add_lock_action(act);
		break;
	case LD_OP_ADD_LOCAL:
		pthread_mutex_lock(&local_thread_mutex);
		add_local_vg(act->vg_name, act->vg_uuid, act->vg_sysid);
		pthread_mutex_unlock(&local_thread_mutex);
		act->result = 0;
		add_client_result(act);
		rv = 0;
		break;
	case LD_OP_REM_LOCAL:
		pthread_mutex_lock(&local_thread_mutex);
		rem_local_vg(act->vg_name, act->vg_uuid);
		pthread_mutex_unlock(&local_thread_mutex);
		act->result = 0;
		add_client_result(act);
		rv = 0;
		break;
	default:
		rv = -EINVAL;
	};

	if (rv < 0) {
		act->result = rv;
		add_client_result(act);
	}
}

static void *client_thread_main(void *arg_in)
{
	struct client *cl;
	struct action *act;

	while (1) {
		pthread_mutex_lock(&client_mutex);
		while (!client_work && list_empty(&client_results)) {
			if (client_stop) {
				pthread_mutex_unlock(&client_mutex);
				goto out;
			}
			pthread_cond_wait(&client_cond, &client_mutex);
		}

		/*
		 * Send outgoing results back to clients
		 */

		if (!list_empty(&client_results)) {
			act = list_first_entry(&client_results, struct action, list);
			list_del(&act->list);
			cl = find_client_id(act->client_id);
			pthread_mutex_unlock(&client_mutex);

			if (cl) {
				pthread_mutex_lock(&cl->mutex);
				client_send_result(cl, act);
				pthread_mutex_unlock(&cl->mutex);
			} else {
				log_debug("no client for result");
			}
			free_action(act);
			continue;
		}

		/*
		 * Queue incoming actions for lockspace threads
		 */

		if (client_work) {
			cl = find_client_work();
			if (!cl)
				client_work = 0;
			pthread_mutex_unlock(&client_mutex);

			if (!cl)
				continue;

			pthread_mutex_lock(&cl->mutex);

			if (cl->recv) {
				cl->recv = 0;
				client_recv_action(cl);
			}

			if (cl->dead) {
				/*
				log_debug("client rem %d pi %d fd %d ig %d",
					  cl->id, cl->pi, cl->fd, cl->poll_ignore);
				*/
				/*
				 * If cl->dead was set in main_loop, then the
				 * fd has already been closed and the pollfd
				 * entry is already unused.
				 * main_loop set dead=1, ignore=0, pi=-1, fd=-1
				 *
				 * if cl->dead was not set in main_loop, but
				 * set in client_recv_action, then the main_loop
				 * should be ignoring this client fd.
				 * main_loop set ignore=1
				 */

				if (cl->poll_ignore) {
					log_debug("client close %d pi %d fd %d",
						  cl->id, cl->pi, cl->fd);
					/* assert cl->pi != -1 */
					/* assert pollfd[pi].fd == FD_IGNORE */
					close(cl->fd);
					rem_pollfd(cl->pi);
					cl->pi = -1;
					cl->fd = -1;
					cl->poll_ignore = 0;
				} else {
					/* main thread should have closed */
					if (cl->pi != -1 || cl->fd != -1) {
						log_error("client %d bad state pi %d fd %d",
							  cl->id, cl->pi, cl->fd);
					}
				}
				pthread_mutex_unlock(&cl->mutex);

				pthread_mutex_lock(&client_mutex);
				list_del(&cl->list);
				pthread_mutex_unlock(&client_mutex);

				client_purge(cl);

				free_client(cl);
			} else {
				pthread_mutex_unlock(&cl->mutex);
			}
		}
		pthread_mutex_unlock(&client_mutex);
	}
out:
	return NULL;
}

static int setup_client_thread(void)
{
	int rv;

	INIT_LIST_HEAD(&client_list);
	INIT_LIST_HEAD(&client_results);

	pthread_mutex_init(&client_mutex, NULL);
	pthread_cond_init(&client_cond, NULL);

	rv = pthread_create(&client_thread, NULL, client_thread_main, NULL);
	if (rv)
		return -1;
	return 0;
}

static void close_client_thread(void)
{
	pthread_mutex_lock(&client_mutex);
	client_stop = 1;
	pthread_cond_signal(&client_cond);
	pthread_mutex_unlock(&client_mutex);
	pthread_join(client_thread, NULL);
}

static int setup_local_thread(void)
{
	struct lockspace *ls;
	struct resource *r;
	int rv;

	if (!local_thread_also)
		return 0;

	if (local_thread_only) {
		if (!(ls = alloc_lockspace()))
			return -ENOMEM;

		if (!(r = alloc_resource())) {
			free(ls);
			return -ENOMEM;
		}

		strcpy(ls->name, "local_thread_gls");

		r->type = LD_RT_GL;
		r->mode = LD_LK_UN;
		strncpy(r->name, R_NAME_GL, MAX_NAME);
		list_add_tail(&r->list, &ls->resources);

		list_add(&ls->list, &local_vgs);
		local_thread_gls = ls;
	}

	rv = pthread_create(&local_thread, NULL, local_thread_main, NULL);
	if (rv)
		return -1;

	return 0;
}

static void close_local_thread(void)
{
	if (!local_thread_also)
		return;

	pthread_mutex_lock(&local_thread_mutex);
	local_thread_stop = 1;
	pthread_cond_signal(&local_thread_cond);
	pthread_mutex_unlock(&local_thread_mutex);
	pthread_join(local_thread, NULL);
}

#if 0
static void setup_listener(void)
{
	struct sockaddr_un addr;
	int rv, fd, ci;

	rv = lvmlockd_socket_address(&addr);
	if (rv < 0)
		return rv;

	fd = socket(AF_LOCAL, SOCK_STREAM, 0);
	if (fd < 0)
		return fd;

	unlink(addr.sun_path);
	rv = bind(fd, (struct sockaddr *) &addr, sizeof(struct sockaddr_un));
	if (rv < 0)
		goto exit_fail;

	rv = chmod(addr.sun_path, DEFAULT_SOCKET_MODE);
	if (rv < 0)
		goto exit_fail;

	rv = chown(addr.sun_path, com.uid, com.gid);
	if (rv < 0) {
		log_error("could not set socket %s permissions: %s",
			  addr.sun_path, strerror(errno));
		goto exit_fail;
	}

	rv = listen(fd, 5);
	if (rv < 0)
		goto exit_fail;

	fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);

	listen_pi = add_pollfd(fd);

	return 0;

exit_fail:
	close(fd);
	return -1;
}
#endif

/*
 * Get a list of all VGs with a lockd type (sanlock|dlm) from lvmetad.
 * We'll match this list against a list of existing lockspaces that are
 * found in the lock manager.
 *
 * For each of these VGs, also create a struct resource on ls->resources to
 * represent each LV in the VG that uses a lock.  For each of these LVs
 * that are active, we'll attempt to adopt a lock.
 */

static int get_lockd_vgs(struct list_head *vg_lockd)
{
	struct list_head update_vgs;
	daemon_reply reply;
	struct dm_config_node *cn;
	struct dm_config_node *metadata;
	struct dm_config_node *md_cn;
	struct dm_config_node *lv_cn;
	struct lockspace *ls, *safe;
	struct resource *r;
	const char *vg_name;
	const char *vg_uuid;
	const char *lock_type;
	const char *lock_args;
	const char *system_id;
	char lv_lock_path[PATH_MAX];
	int mutex_unlocked = 0;
	int rv = 0;

	INIT_LIST_HEAD(&update_vgs);

	pthread_mutex_lock(&lvmetad_mutex);
	reply = daemon_send_simple(lvmetad_handle, "vg_list",
				   "token = %s", "skip",
				   NULL);

	if (!(cn = dm_config_find_node(reply.cft->root, "volume_groups"))) {
		log_error("get_lockd_vgs no vgs");
		rv = -EINVAL;
		goto out;
	}

	/* create an update_vgs list of all vg uuids */

	for (cn = cn->child; cn; cn = cn->sib) {
		vg_uuid = cn->key;

		if (!(ls = alloc_lockspace())) {
			rv = -ENOMEM;
			break;
		}

		strncpy(ls->vg_uuid, vg_uuid, 64);
		list_add_tail(&ls->list, &update_vgs);
		log_debug("get_lockd_vgs %s", vg_uuid);
	}

	daemon_reply_destroy(reply);

	if (rv < 0)
		goto out;

	/* get vg_name and lock_type for each vg uuid entry in update_vgs */

	list_for_each_entry(ls, &update_vgs, list) {
		reply = daemon_send_simple(lvmetad_handle, "vg_lookup",
					   "token = %s", "skip",
					   "uuid = %s", ls->vg_uuid,
					   NULL);

		vg_name = daemon_reply_str(reply, "name", NULL);
		if (!vg_name) {
			log_error("get_lockd_vgs %s no name", ls->vg_uuid);
			rv = -EINVAL;
			goto next;
		}

		strncpy(ls->vg_name, vg_name, MAX_NAME);

		metadata = dm_config_find_node(reply.cft->root, "metadata");
		if (!metadata) {
			log_error("get_lockd_vgs %s name %s no metadata",
				  ls->vg_uuid, ls->vg_name);
			rv = -EINVAL;
			goto next;
		}

		lock_type = dm_config_find_str(metadata, "metadata/lock_type", NULL);
		ls->lm_type = str_to_lm(lock_type);

		if ((ls->lm_type != LD_LM_SANLOCK) && (ls->lm_type != LD_LM_DLM))
			continue;

		lock_args = dm_config_find_str(metadata, "metadata/lock_args", NULL);
		if (lock_args)
			strncpy(ls->vg_args, lock_args, MAX_ARGS);

		system_id = dm_config_find_str(metadata, "metadata/system_id", NULL);
		if (system_id)
			strncpy(ls->vg_sysid, system_id, MAX_NAME);

		/*
		 * Make a record (struct resource) of each lv that uses a lock.
		 * For any lv that uses a lock, we'll check if the lv is active
		 * and if so try to adopt a lock for it.
		 */

		for (md_cn = metadata->child; md_cn; md_cn = md_cn->sib) {
			if (strcmp(md_cn->key, "logical_volumes"))
				continue;

			for (lv_cn = md_cn->child; lv_cn; lv_cn = lv_cn->sib) {
				snprintf(lv_lock_path, PATH_MAX, "%s/lock_type", lv_cn->key);
				lock_type = dm_config_find_str(lv_cn, lv_lock_path, NULL);

				if (!lock_type)
					continue;

				snprintf(lv_lock_path, PATH_MAX, "%s/lock_args", lv_cn->key);
				lock_args = dm_config_find_str(lv_cn, lv_lock_path, NULL);

				if (!(r = alloc_resource())) {
					rv = -ENOMEM;
					goto next;
				}

				r->type = LD_RT_LV;
				strncpy(r->name, lv_cn->key, MAX_NAME);
				strncpy(r->lv_args, lock_args, MAX_ARGS);
				list_add_tail(&r->list, &ls->resources);
			}
		}
next:
		daemon_reply_destroy(reply);

		log_debug("get_lockd_vgs %s lock_type %s lock_args %s",
			  ls->vg_name, lock_type, lock_args);
	}
	pthread_mutex_unlock(&lvmetad_mutex);
	mutex_unlocked = 1;
out:
	/* Return lockd VG's on the vg_lockd list. */

	list_for_each_entry_safe(ls, safe, &update_vgs, list) {
		list_del(&ls->list);

		if ((ls->lm_type == LD_LM_SANLOCK) || (ls->lm_type == LD_LM_DLM))
			list_add_tail(&ls->list, vg_lockd);
		else
			free(ls);
	}

	if (!mutex_unlocked)
		pthread_mutex_unlock(&lvmetad_mutex);

	return rv;
}

/*
 * For each lockd VG found in lvmetad, check if any
 * of it's LV's are active.  If there's a device node
 * for the LV in /dev/<vg>/ then we conclude it's active
 * and will go on to adopt a lock for it.
 * If not, then we don't need to adopt an LV lock for it
 * and remove the struct resource that represents the LV.
 */

static int get_active_lvs(struct list_head *vg_lockd)
{
	struct lockspace *ls;
	struct resource *r, *rsafe;
	struct list_head tmp_resources;
	char vg_dir_path[PATH_MAX];
	struct dirent *de;
	DIR *vg_dir;

	INIT_LIST_HEAD(&tmp_resources);

	/* Get the subset of lockd vgs with active lvs. */

	list_for_each_entry(ls, vg_lockd, list) {

		/* Add these resources back as LV device nodes are found for them. */
		list_for_each_entry_safe(r, rsafe, &ls->resources, list) {
			list_del(&r->list);
			list_add(&r->list, &tmp_resources);
		}

		snprintf(vg_dir_path, PATH_MAX-1, "/dev/%s/", ls->vg_name);

		if ((vg_dir = opendir(vg_dir_path))) {
			while ((de = readdir(vg_dir))) {
				if (de->d_name[0] == '.')
					continue;

				/* put the struct resource back on the ls */
				list_for_each_entry(r, &tmp_resources, list) {
					if (strncmp(r->name, de->d_name, MAX_NAME))
						continue;

					log_debug("lockd vg %s has active lv %s", ls->vg_name, r->name);
					list_del(&r->list);
					list_add_tail(&r->list, &ls->resources);
					break;
				}
			}
			closedir(vg_dir);
		}

		/* Remove remaining tmp_resources for LVs that are not active. */
		list_for_each_entry_safe(r, rsafe, &tmp_resources, list) {
			log_debug("lockd vg %s ignore inactive lv %s", ls->vg_name, r->name);
			list_del(&r->list);
			free_resource(r);
		}
	}

	return 0;
}

static void adopt_locks(void)
{
	struct list_head ls_found;
	struct list_head vg_lockd;
	struct list_head to_unlock;
	struct lockspace *ls, *lsafe;
	struct lockspace *ls1, *l1safe;
	struct lockspace *ls2, *l2safe;
	struct resource *r, *rsafe;
	struct action *act, *asafe;
	int count_start = 0, count_start_done = 0, count_start_fail = 0;
	int count_adopt = 0, count_adopt_done = 0, count_adopt_fail = 0;
	int found, rv;

	INIT_LIST_HEAD(&adopt_results);

	INIT_LIST_HEAD(&ls_found);
	INIT_LIST_HEAD(&vg_lockd);
	INIT_LIST_HEAD(&to_unlock);

	/*
	 * Get list of lockspaces from lock managers.
	 * Get list of VGs from lvmetad with a lockd type.
	 * Get list of active lockd type LVs from /dev.
	 *
	 * ECONNREFUSED means the lock manager is not running.
	 * This is expected for at least one of them.
	 */

	rv = lm_get_lockspaces_dlm(&ls_found);
	if ((rv < 0) && (rv != -ECONNREFUSED))
		goto fail;

	rv = lm_get_lockspaces_sanlock(&ls_found);
	if ((rv < 0) && (rv != -ECONNREFUSED))
		goto fail;

	if (list_empty(&ls_found)) {
		log_debug("No lockspaces found to adopt");
		return;
	}

	rv = get_lockd_vgs(&vg_lockd);
	if (rv < 0)
		goto fail;

	rv = get_active_lvs(&vg_lockd);
	if (rv < 0)
		goto fail;

	list_for_each_entry(ls, &ls_found, list) {
		if (ls->lm_type == LD_LM_DLM)
			gl_use_dlm = 1;

		log_debug("adopt %s lockspace %s vg %s",
			  lm_str(ls->lm_type), ls->name, ls->vg_name);
	}

	if (!gl_use_dlm)
		gl_use_sanlock = 1;

	list_for_each_entry(ls, &vg_lockd, list) {
		log_debug("adopt lvmetad vg %s lock_type %s lock_args %s",
			  ls->vg_name, lm_str(ls->lm_type), ls->vg_args);

		list_for_each_entry(r, &ls->resources, list)
			log_debug("adopt device lv %s/%s", ls->vg_name, r->name);
	}

	/*
	 * Compare and merge the list of lockspaces in ls_found
	 * and the list of lockd VGs in vg_lockd.
	 *
	 * An ls from ls_found may not have had any active lvs when
	 * previous lvmlockd died, but the ls should still be joined,
	 * and checked for GL/VG locks.
	 *
	 * An ls from vg_lockd with active lvs should be in ls_found.
	 * If it's not then we might want to join the ls and acquire locks
	 * for the active lvs (as opposed to adopting orphans for them.)
	 * The orphan lock in the ls should have prevented the ls in
	 * the lock manager from going away.
	 *
	 * If an ls in vg_lockd has no active lvs and does not have
	 * a matching entry in ls_found, then skip it.
	 * 
	 * An ls in ls_found should always have a matching ls in
	 * vg_lockd.  If it doesn't, then maybe the vg has been
	 * removed even though the lockspace for the vg is still
	 * in the lock manager.  Just leave the ls in the lm
	 * alone, and skip the ls_found entry.
	 */

	list_for_each_entry_safe(ls1, l1safe, &ls_found, list) {

		/* The dlm global lockspace is special and doesn't match a VG. */
		if (!strcmp(ls1->name, gl_lsname_dlm)) {
			list_del(&ls1->list);
			free(ls1);
		}

		found = 0;

		list_for_each_entry_safe(ls2, l2safe, &vg_lockd, list) {
			if (strcmp(ls1->vg_name, ls2->vg_name))
				continue;

			/*
		 	 * LS in both ls_found and vg_lockd.
		 	 */
			log_debug("ls %s matches vg %s", ls1->name, ls2->vg_name);
			memcpy(ls1->vg_uuid, ls2->vg_uuid, 64);
			memcpy(ls1->vg_args, ls2->vg_args, MAX_ARGS);
			list_for_each_entry_safe(r, rsafe, &ls2->resources, list) {
				list_del(&r->list);
				list_add(&r->list, &ls1->resources);
			}
			list_del(&ls2->list);
			free(ls2);
			found = 1;
			break;
		}

		/*
		 * LS in ls_found, not in vg_lockd.
		 * An lvm lockspace found in the lock manager has no
		 * corresponding VG in lvmetad.  This shouldn't usually
		 * happen, but it's possible the VG could have been removed
		 * while the orphaned lockspace from it was still around.
		 * Report an error and leave the ls in the lm alone.
		 */
		if (!found) {
			log_error("No VG %s found for lockspace %s %s",
				  ls1->vg_name, ls1->name, lm_str(ls1->lm_type));
			list_del(&ls1->list);
			free(ls1);
		}
	}

	/*
	 * LS in vg_lockd, not in ls_found.
	 * lockd vgs from lvmetad that do not have an existing lockspace.
	 * This wouldn't be unusual; we just skip the vg.
	 * But, if the vg has active lvs, then it should have had locks
	 * and a lockspace.  Should we attempt to join the lockspace and
	 * acquire (not adopt) locks for these LVs?
	 */

	list_for_each_entry_safe(ls, lsafe, &vg_lockd, list) {
		if (!list_empty(&ls->resources)) {
			/* We should have found a lockspace. */
			/* TODO: add this ls and acquire locks for ls->resources? */
			log_error("No lockspace %s %s found for VG %s with active LVs",
				  ls->name, lm_str(ls->lm_type), ls->vg_name);
		} else {
			/* The VG wasn't started in the previous lvmlockd. */
			log_debug("No ls found for vg %s", ls->vg_name);
		}
		
		list_del(&ls->list);
		free(ls);
	}

	/*
	 * Create and queue start actions to add lockspaces.
	 */

	if (gl_use_dlm) {
		if (!(act = alloc_action()))
			goto fail;
		log_debug("adopt add dlm global lockspace");
		act->op = LD_OP_START;
		act->flags = (LD_AF_ADOPT | LD_AF_WAIT);
		act->rt = LD_RT_GL;
		act->lm_type = LD_LM_DLM;
		act->client_id = ADOPT_CLIENT_ID;
		add_dlm_global_lockspace(act);
		count_start++;
	}

	list_for_each_entry_safe(ls, lsafe, &ls_found, list) {
		if (!(act = alloc_action()))
			goto fail;
		act->op = LD_OP_START;
		act->flags = (LD_AF_ADOPT | LD_AF_WAIT);
		act->rt = LD_RT_VG;
		act->lm_type = ls->lm_type;
		act->client_id = ADOPT_CLIENT_ID;
		strncpy(act->vg_name, ls->vg_name, MAX_NAME);
		memcpy(act->vg_uuid, ls->vg_uuid, 64);
		memcpy(act->vg_args, ls->vg_args, MAX_ARGS);
		act->host_id = ls->host_id;

		/* set act->version from lvmetad data? */

		log_debug("adopt add %s vg lockspace %s", lm_str(act->lm_type), act->vg_name);

		rv = add_lockspace_thread(ls->name, act->vg_name, act->vg_uuid,
					  act->lm_type, act->vg_args, act);
		if (rv < 0) {
			log_error("Failed to create lockspace thread for VG %s", ls->vg_name);
			list_del(&ls->list);
			free(ls);
			free_action(act);
			count_start_fail++;
			continue;
		}

		/*
		 * When the lockspace_thread is done with the start act,
		 * it will see the act ADOPT flag and move the act onto
		 * the adopt_results list for us to collect below.
		 */
		count_start++;
	}

	log_debug("adopt starting %d lockspaces", count_start);

	/*
	 * Wait for all start/rejoin actions to complete.  Each start action
	 * queued above will appear on the adopt_results list when finished.
	 */

	while (count_start_done < count_start) {
		sleep(1);
		act = NULL;

		pthread_mutex_lock(&client_mutex);
		if (!list_empty(&adopt_results)) {
			act = list_first_entry(&adopt_results, struct action, list);
			list_del(&act->list);
		}
		pthread_mutex_unlock(&client_mutex);

		if (!act)
			continue;

		if (act->result < 0) {
			log_error("adopt add lockspace failed vg %s %d", act->vg_name, act->result);
			count_start_fail++;
		}

		free_action(act);
		count_start_done++;
	}

	log_debug("adopt started %d lockspaces done %d fail %d",
		  count_start, count_start_done, count_start_fail);

	/*
	 * Create lock-adopt actions for active LVs (ls->resources),
	 * and GL/VG locks (we don't know if these locks were held
	 * and orphaned by the last lvmlockd, so try to adopt them
	 * to see.)
	 *
	 * A proper struct lockspace now exists on the lockspaces list
	 * for each ls in ls_found.  Lock ops for one of those
	 * lockspaces can be done as OP_LOCK actions queued using
	 * add_lock_action();
	 *
	 * Start by attempting to adopt the lock in the most likely
	 * mode it was left in (ex for lvs, sh for vg/gl).  If
	 * the mode is wrong, the lm will return an error and we
	 * try again with the other mode.
	 */

	list_for_each_entry(ls, &ls_found, list) {

		/*
		 * Adopt orphan LV locks.
		 */

		list_for_each_entry(r, &ls->resources, list) {
			if (!(act = alloc_action()))
				goto fail;
			act->op = LD_OP_LOCK;
			act->rt = LD_RT_LV;
			act->mode = LD_LK_EX;
			act->flags = (LD_AF_ADOPT | LD_AF_PERSISTENT);
			act->client_id = ADOPT_CLIENT_ID;
			act->lm_type = ls->lm_type;
			strncpy(act->vg_name, ls->vg_name, MAX_NAME);
			strncpy(act->lv_name, r->name, MAX_NAME);
			strncpy(act->lv_args, r->lv_args, MAX_ARGS);

			log_debug("adopt lock for lv %s/%s", act->vg_name, act->lv_name);

			rv = add_lock_action(act);
			if (rv < 0) {
				log_error("adopt add_lock_action lv %s/%s error %d", act->vg_name, act->lv_name, rv);
				count_adopt_fail++;
				free_action(act);
			} else {
				count_adopt++;
			}
		}

		/*
		 * Adopt orphan VG lock.
		 */

		if (!(act = alloc_action()))
			goto fail;
		act->op = LD_OP_LOCK;
		act->rt = LD_RT_VG;
		act->mode = LD_LK_SH;
		act->flags = LD_AF_ADOPT;
		act->client_id = ADOPT_CLIENT_ID;
		act->lm_type = ls->lm_type;
		strncpy(act->vg_name, ls->vg_name, MAX_NAME);

		log_debug("adopt lock for vg %s", act->vg_name);

		rv = add_lock_action(act);
		if (rv < 0) {
			log_error("adopt add_lock_action vg %s error %d", act->vg_name, rv);
			count_adopt_fail++;
			free_action(act);
		} else {
			count_adopt++;
		}
	}

	/*
	 * Adopt orphan GL lock.
	 */

	if (!(act = alloc_action()))
		goto fail;
	act->op = LD_OP_LOCK;
	act->rt = LD_RT_GL;
	act->mode = LD_LK_SH;
	act->flags = LD_AF_ADOPT;
	act->client_id = ADOPT_CLIENT_ID;
	act->lm_type = (gl_use_sanlock ? LD_LM_SANLOCK : LD_LM_DLM);

	log_debug("adopt lock for gl");

	rv = add_lock_action(act);
	if (rv < 0) {
		log_error("adopt add_lock_action gl %s error %d", act->vg_name, rv);
		count_adopt_fail++;
		free_action(act);
	} else {
		count_adopt++;
	}

	/*
	 * Wait for lock-adopt actions to complete.  The completed
	 * actions are passed back here via the adopt_results list.
	 */

	while (count_adopt_done < count_adopt) {
		sleep(1);
		act = NULL;

		pthread_mutex_lock(&client_mutex);
		if (!list_empty(&adopt_results)) {
			act = list_first_entry(&adopt_results, struct action, list);
			list_del(&act->list);
		}
		pthread_mutex_unlock(&client_mutex);

		if (!act)
			continue;

		/*
		 * lock adopt results 
		 */

		if (act->result == -EUCLEAN) {
			/*
			 * Adopt failed because the orphan has a different mode
			 * than initially requested.  Repeat the lock-adopt operation
			 * with the other mode.  N.B. this logic depends on first
			 * trying sh then ex for GL/VG locks, and ex then sh for
			 * LV locks.
			 */

			if ((act->rt != LD_RT_LV) && (act->mode == LD_LK_SH)) {
				/* GL/VG locks: attempt to adopt ex after sh failed. */
				act->mode = LD_LK_EX;
				rv = add_lock_action(act);

			} else if ((act->rt == LD_RT_LV) && (act->mode == LD_LK_EX)) {
				/* LV locks: attempt to adopt sh after ex failed. */
				act->mode = LD_LK_SH;
				rv = add_lock_action(act);

			} else {
				log_error("Failed to adopt %s lock in vg %s error %d",
					  rt_str(act->rt), act->vg_name, act->result);
				count_adopt_fail++;
				count_adopt_done++;
				free_action(act);
				rv = 0;
			}

			if (rv < 0) {
				log_error("adopt add_lock_action again %s", act->vg_name);
				count_adopt_fail++;
				count_adopt_done++;
				free_action(act);
			}

		} else if (act->result == -ENOENT) {
			/*
			 * No orphan lock exists.  This is common for GL/VG locks
			 * because they may not have been held when lvmlockd exited.
			 * It's also expected for LV types that do not use a lock.
			 */

			if (act->rt == LD_RT_LV) {
				/* Unexpected, we should have found an orphan. */
				log_error("Failed to adopt LV lock for %s/%s error %d",
					  act->vg_name, act->lv_name, act->result);
				count_adopt_fail++;
			} else {
				/* Normal, no GL/VG lock was orphaned. */
				log_debug("Did not adopt %s lock in vg %s error %d",
					   rt_str(act->rt), act->vg_name, act->result);
			}

			count_adopt_done++;
			free_action(act);

		} else if (act->result < 0) {
			/*
			 * Some unexpected error.
			 */

			log_error("adopt lock rt %s vg %s lv %s error %d",
				  rt_str(act->rt), act->vg_name, act->lv_name, act->result);
			count_adopt_fail++;
			count_adopt_done++;
			free_action(act);

		} else {
			/*
			 * Adopt success.
			 */

			if (act->rt == LD_RT_LV) {
				log_debug("adopt success lv %s/%s %s", act->vg_name, act->lv_name, mode_str(act->mode));
				free_action(act);
			} else if (act->rt == LD_RT_VG) {
				log_debug("adopt success vg %s %s", act->vg_name, mode_str(act->mode));
				list_add_tail(&act->list, &to_unlock);
			} else if (act->rt == LD_RT_GL) {
				log_debug("adopt success gl %s %s", act->vg_name, mode_str(act->mode));
				list_add_tail(&act->list, &to_unlock);
			}
			count_adopt_done++;
		}
	}

	/*
	 * Release adopted GL/VG locks.
	 * The to_unlock actions were the ones used to lock-adopt the GL/VG locks;
	 * now use them to do the unlocks.  These actions will again be placed
	 * on adopt_results for us to collect because they have the ADOPT flag set.
	 */

	count_adopt = 0;
	count_adopt_done = 0;

	list_for_each_entry_safe(act, asafe, &to_unlock, list) {
		list_del(&act->list);

		if (act->mode == LD_LK_EX) {
			/*
			 * TODO: we probably want to check somehow that
			 * there's no lvm command still running that's
			 * using this ex lock and changing things.
			 */
			log_warn("adopt releasing ex %s lock %s",
				 rt_str(act->rt), act->vg_name);
		}

		act->mode = LD_LK_UN;

		log_debug("adopt unlock for %s %s", rt_str(act->rt), act->vg_name);

		rv = add_lock_action(act);
		if (rv < 0) {
			log_error("adopt unlock add_lock_action error %d", rv);
			free_action(act);
		} else {
			count_adopt++;
		}
	}

	/* Wait for the unlocks to complete. */

	while (count_adopt_done < count_adopt) {
		sleep(1);
		act = NULL;

		pthread_mutex_lock(&client_mutex);
		if (!list_empty(&adopt_results)) {
			act = list_first_entry(&adopt_results, struct action, list);
			list_del(&act->list);
		}
		pthread_mutex_unlock(&client_mutex);

		if (!act)
			continue;

		if (act->result < 0)
			log_error("adopt unlock error %d", act->result);

		count_adopt_done++;
		free_action(act);
	}


	/* TODO: purge any remaining orphan locks in each rejoined ls? */

	if (count_start_fail || count_adopt_fail)
		goto fail;

	log_debug("adopt_locks done");
	return;

fail:
	log_error("adopt_locks failed, reset host");
}

static int get_peer_pid(int fd)
{
	struct ucred cred;
	unsigned int len = sizeof(cred);

	if (getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &cred, &len) != 0)
		return -1;

	return cred.pid;
}

static void process_listener(int poll_fd)
{
	struct client *cl;
	int fd, pi;

	/* assert poll_fd == listen_fd */

	fd = accept(listen_fd, NULL, NULL);
	if (fd < 0)
		return;

	if (!(cl = alloc_client()))
		return;

	pi = add_pollfd(fd);
	if (pi < 0) {
		log_error("process_listener add_pollfd error %d", pi);
		free_client(cl);
		return;
	}

	cl->pi = pi;
	cl->fd = fd;
	cl->pid = get_peer_pid(fd);

	pthread_mutex_init(&cl->mutex, NULL);

	pthread_mutex_lock(&client_mutex);
	if (client_ids == ADOPT_CLIENT_ID)
		client_ids += 2;
	if (!client_ids)
		client_ids += 1;
	cl->id = client_ids;
	list_add_tail(&cl->list, &client_list);
	pthread_mutex_unlock(&client_mutex);

	/* log_debug("client add %d pi %d fd %d", cl->id, cl->pi, cl->fd); */
}

/*
 * main loop polls on pipe[0] so that a thread can
 * restart the poll by writing to pipe[1].
 */
static int setup_restart(void)
{
	if (pipe(restart_fds)) {
		log_error("setup_restart pipe error %d", errno);
		return -1;
	}

	restart_pi = add_pollfd(restart_fds[0]);
	if (restart_pi < 0)
		return restart_pi;

	return 0;
}

/*
 * thread wrote 'w' to restart_fds[1] to restart poll()
 * after adding an fd back into pollfd.
 */
static void process_restart(int fd)
{
	char wake[1];
	/* assert fd == restart_fds[0] */
	read(restart_fds[0], wake, 1);
}

static void sigterm_handler(int sig __attribute__((unused)))
{
	daemon_quit = 1;
}

static int main_loop(daemon_state *ds_arg)
{
	struct client *cl;
	int i, rv, is_recv, is_dead;

	signal(SIGTERM, &sigterm_handler);

	rv = setup_structs();
	if (rv < 0) {
		log_error("Can't allocate memory");
		return rv;
	}

	/* TODO: avoid possible vg name collision */
	strcpy(gl_lsname_dlm, S_NAME_GL_DLM);

	INIT_LIST_HEAD(&local_vgs);
	INIT_LIST_HEAD(&local_thread_actions);
	pthread_mutex_init(&local_thread_mutex, NULL);
	pthread_cond_init(&local_thread_cond, NULL);
	INIT_LIST_HEAD(&lockspaces);
	INIT_LIST_HEAD(&lockspaces_inactive);
	pthread_mutex_init(&lockspaces_mutex, NULL);
	pthread_mutex_init(&pollfd_mutex, NULL);
	pthread_mutex_init(&log_mutex, NULL);

	openlog("lvmlockd", LOG_CONS | LOG_PID, LOG_DAEMON);
	log_warn("lvmlockd started");

	listen_fd = ds_arg->socket_fd;
	listen_pi = add_pollfd(listen_fd);

	setup_client_thread();
	setup_worker_thread();
	setup_local_thread();
	setup_restart();

	pthread_mutex_init(&lvmetad_mutex, NULL);
	lvmetad_handle = lvmetad_open(NULL);
	if (lvmetad_handle.error || lvmetad_handle.socket_fd < 0)
		log_error("lvmetad_open error %d", lvmetad_handle.error);
	else
		lvmetad_connected = 1;

	/* add entries to local_vgs */
	create_work_action(LD_OP_UPDATE_LOCAL);

	/*
	 * Attempt to rejoin lockspaces and adopt locks from a previous
	 * instance of lvmlockd that left behind lockspaces/locks.
	 */
	if (adopt_opt)
		adopt_locks();

	while (1) {
		rv = poll(pollfd, pollfd_maxi + 1, -1);
		if (rv == -1 && errno == EINTR) {
			if (daemon_quit) {
				int count;
				/* first sigterm would trigger stops, and
				   second sigterm may finish the joins. */
				count = for_each_lockspace(DO_STOP, DO_FREE, NO_FORCE);
				if (!count)
					break;
				log_debug("ignore shutdown for %d lockspaces", count);
				daemon_quit = 0;
			}
			continue;
		}
		if (rv < 0) {
			log_error("poll errno %d", errno);
			break;
		}

		for (i = 0; i <= pollfd_maxi; i++) {
			if (pollfd[i].fd < 0)
				continue;

			is_recv = 0;
			is_dead = 0;

			if (pollfd[i].revents & POLLIN)
				is_recv = 1;
			if (pollfd[i].revents & (POLLERR | POLLHUP | POLLNVAL))
				is_dead = 1;

			if (!is_recv && !is_dead)
				continue;

			if (i == listen_pi) {
				process_listener(pollfd[i].fd);
				continue;
			}

			if (i == restart_pi) {
				process_restart(pollfd[i].fd);
				continue;
			}

			/*
			log_debug("poll pi %d fd %d revents %x",
				  i, pollfd[i].fd, pollfd[i].revents);
			*/

			pthread_mutex_lock(&client_mutex);
			cl = find_client_pi(i);
			if (cl) {
				pthread_mutex_lock(&cl->mutex);

				if (cl->recv) {
					/* should not happen */
					log_error("main client %d already recv", cl->id);

				} else if (cl->dead) {
					/* should not happen */
					log_error("main client %d already dead", cl->id);

				} else if (is_dead) {
					log_debug("close %s[%d.%u] fd %d",
						  cl->name[0] ? cl->name : "client",
						  cl->pid, cl->id, cl->fd);
					cl->dead = 1;
					cl->pi = -1;
					cl->fd = -1;
					cl->poll_ignore = 0;
					close(pollfd[i].fd);
					pollfd[i].fd = POLL_FD_UNUSED;
					pollfd[i].events = 0;
					pollfd[i].revents = 0;

				} else if (is_recv) {
					cl->recv = 1;
					cl->poll_ignore = 1;
					pollfd[i].fd = POLL_FD_IGNORE;
					pollfd[i].events = 0;
					pollfd[i].revents = 0;
				}

				pthread_mutex_unlock(&cl->mutex);

				client_work = 1;
				pthread_cond_signal(&client_cond);

				/* client_thread will pick up and work on any
				   client with cl->recv or cl->dead set */

			} else {
				/* don't think this can happen */
				log_error("no client for index %d fd %d",
					  i, pollfd[i].fd);
				close(pollfd[i].fd);
				pollfd[i].fd = POLL_FD_UNUSED;
				pollfd[i].events = 0;
				pollfd[i].revents = 0;
			}
			pthread_mutex_unlock(&client_mutex);

			/* TODO?: after set_dead, scan pollfd for last unused
			   slot and reduce pollfd_maxi */
		}
	}

	for_each_lockspace_retry(DO_STOP, DO_FREE, DO_FORCE);
	free_lockspaces_inactive();
	close_worker_thread();
	close_client_thread();
	close_local_thread();
	closelog();
	daemon_close(lvmetad_handle);
	return 0;
}

static void usage(char *prog, FILE *file)
{
	fprintf(file, "Usage:\n");
	fprintf(file, "%s [options]\n\n", prog);
	fprintf(file, "  --help | -h\n");
	fprintf(file, "        Show this help information.\n");
	fprintf(file, "  --version | -V\n");
	fprintf(file, "        Show version of lvmlockd.\n");
	fprintf(file, "  --test | -T\n");
	fprintf(file, "        Test mode, do not call lock manager.\n");
	fprintf(file, "  --foreground | -f\n");
	fprintf(file, "        Don't fork.\n");
	fprintf(file, "  --daemon-debug | -D\n");
	fprintf(file, "        Don't fork and print debugging to stdout.\n");
	fprintf(file, "  --pid-file | -p <path>\n");
	fprintf(file, "        Set path to the pid file. [%s]\n", LVMLOCKD_PIDFILE);
	fprintf(file, "  --socket-path | -s <path>\n");
	fprintf(file, "        Set path to the socket to listen on. [%s]\n", LVMLOCKD_SOCKET);
	fprintf(file, "  --log-config | -l <str>\n");
	fprintf(file, "        Set log config.\n");
	fprintf(file, "  --local-also | -a\n");
	fprintf(file, "        Manage locks between pids for local vgs.\n");
	fprintf(file, "  --local-only | -o\n");
	fprintf(file, "        Only manage locks for local vgs, not dlm|sanlock vgs.\n");
	fprintf(file, "  --gl-type | -g <str>\n");
	fprintf(file, "        Set global lock type to be dlm|sanlock.\n");
	fprintf(file, "  --system-id | -y <str>\n");
	fprintf(file, "        Set the local system id.\n");
	fprintf(file, "  --host-id | -i <num>\n");
	fprintf(file, "        Set the local sanlock host id.\n");
	fprintf(file, "  --host-id-file | -F <path>\n");
	fprintf(file, "        A file containing the local sanlock host_id.\n");
	fprintf(file, "  --adopt | -A 0|1\n");
	fprintf(file, "        Adopt locks from a previous instance of lvmlockd.\n");
}

int main(int argc, char *argv[])
{
	daemon_state ds;

	ds.daemon_main = main_loop;
	ds.daemon_init = NULL;
	ds.daemon_fini = NULL;
	ds.pidfile = getenv("LVM_LVMLOCKD_PIDFILE");
	ds.socket_path = getenv("LVM_LVMLOCKD_SOCKET");
	ds.protocol = lvmlockd_protocol;
	ds.protocol_version = lvmlockd_protocol_version;
	ds.name = "lvmlockd";

	static struct option long_options[] = {
		{"help",        no_argument,       0, 'h' },
		{"version",     no_argument,       0, 'V' },
		{"test",        no_argument,       0, 'T' },
		{"foreground",  no_argument,       0, 'f' },
		{"daemon-debug",no_argument,       0, 'D' },
		{"pid-file",    required_argument, 0, 'p' },
		{"socket-path", required_argument, 0, 's' },
		{"local-also",  no_argument,       0, 'a' },
		{"local-only",  no_argument,       0, 'o' },
		{"gl-type",     required_argument, 0, 'g' },
		{"system-id",   required_argument, 0, 'y' },
		{"host-id",     required_argument, 0, 'i' },
		{"host-id-file",required_argument, 0, 'F' },
		{"adopt",       required_argument, 0, 'A' },
		{0, 0, 0, 0 }
	};

	while (1) {
		int c;
		int lm;
		int option_index = 0;

		c = getopt_long(argc, argv, "hVTfDp:s:l:aog:S:I:A:",
				long_options, &option_index);
		if (c == -1)
			break;

		switch (c) {
		case '0':
			break;
		case 'h':
			usage(argv[0], stdout);
			exit(EXIT_SUCCESS);
		case 'V':
			printf("lvmlockd version: " LVM_VERSION "\n");
			exit(EXIT_SUCCESS);
		case 'T':
			daemon_test = 1;
			break;
		case 'f':
			ds.foreground = 1;
			break;
		case 'D':
			ds.foreground = 1;
			daemon_debug = 1;
			break;
		case 'p':
			ds.pidfile = strdup(optarg);
			break;
		case 's':
			ds.socket_path = strdup(optarg);
			break;
		case 'a':
			local_thread_also = 1;
			break;
		case 'o':
			local_thread_also = 1;
			local_thread_only = 1;
			break;
		case 'g':
			lm = str_to_lm(optarg);
			if (lm == LD_LM_DLM)
				gl_use_dlm = 1;
			else if (lm == LD_LM_SANLOCK)
				gl_use_sanlock = 1;
			else {
				fprintf(stderr, "invalid gl-type option");
				exit(EXIT_FAILURE);
			}
			break;
		case 'y':
			our_system_id = strdup(optarg);
			break;
		case 'i':
			daemon_host_id = atoi(optarg);
			break;
		case 'F':
			daemon_host_id_file = strdup(optarg);
			break;
		case 'A':
			adopt_opt = atoi(optarg);
			break;
		case '?':
		default:
			usage(argv[0], stdout);
			exit(EXIT_FAILURE);
		}
	}

	if (!ds.pidfile)
		ds.pidfile = LVMLOCKD_PIDFILE;

	if (!ds.socket_path)
		ds.socket_path = LVMLOCKD_SOCKET;

	/* runs daemon_main/main_loop */
	daemon_start(ds);

	return 0;
}
