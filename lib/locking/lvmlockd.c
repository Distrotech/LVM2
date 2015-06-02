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
#include "activate.h"
#include "lvmetad.h"
#include "lvmlockd.h"
#include "lvmcache.h"
#include "lvmlockd-client.h"

static daemon_handle _lvmlockd;
static const char *_lvmlockd_socket = NULL;
static struct cmd_context *_lvmlockd_cmd = NULL;
static int _use_lvmlockd;         /* is 1 if command is configured to use lvmlockd */
static int _lvmlockd_connected;   /* is 1 if command is connected to lvmlockd */
static int _lvmlockd_init_failed; /* used to suppress further warnings */

void lvmlockd_set_socket(const char *sock)
{
	_lvmlockd_socket = sock;
}

/*
 * Set directly from global/use_lvmlockd
 */
void lvmlockd_set_use(int use)
{
	_use_lvmlockd = use;
}

/*
 * Returns the value of global/use_lvmlockd being used by the command.
 */
int lvmlockd_use(void)
{
	return _use_lvmlockd;
}

/*
 * The command continues even if init and/or connect fail,
 * because the command is allowed to use local VGs without lvmlockd,
 * and is allowed to read lockd VGs without locks from lvmlockd.
 */
void lvmlockd_init(struct cmd_context *cmd)
{
	if (!_use_lvmlockd) {
		/* Should never happen, don't call init when not using lvmlockd. */
		log_error("Should not initialize lvmlockd with use_lvmlockd=0.");
	}

	if (!_lvmlockd_socket) {
		log_warn("WARNING: lvmlockd socket location is not configured.");
		_lvmlockd_init_failed = 1;
	}

	if (!!access(LVMLOCKD_PIDFILE, F_OK)) {
		log_warn("WARNING: lvmlockd process is not running.");
		_lvmlockd_init_failed = 1;
	} else {
		_lvmlockd_init_failed = 0;
	}

	_lvmlockd_cmd = cmd;
}

void lvmlockd_connect(void)
{
	if (!_use_lvmlockd) {
		/* Should never happen, don't call connect when not using lvmlockd. */
		log_error("Should not connect to lvmlockd with use_lvmlockd=0.");
	}

	if (_lvmlockd_connected) {
		/* Should never happen, only call connect once. */
		log_error("lvmlockd is already connected.");
	}

	if (_lvmlockd_init_failed)
		return;

	_lvmlockd = lvmlockd_open(_lvmlockd_socket);

	if (_lvmlockd.socket_fd >= 0 && !_lvmlockd.error) {
		log_debug("Successfully connected to lvmlockd on fd %d.", _lvmlockd.socket_fd);
		_lvmlockd_connected = 1;
	} else {
		log_warn("WARNING: lvmlockd connect failed.");
	}
}

void lvmlockd_disconnect(void)
{
	if (_lvmlockd_connected)
		daemon_close(_lvmlockd);
	_lvmlockd_connected = 0;
	_lvmlockd_cmd = NULL;
}

/* Translate the result strings from lvmlockd to bit flags. */
static void _flags_str_to_lockd_flags(const char *flags_str, uint32_t *lockd_flags)
{
	if (strstr(flags_str, "NO_LOCKSPACES"))
		*lockd_flags |= LD_RF_NO_LOCKSPACES;

	if (strstr(flags_str, "NO_GL_LS"))
		*lockd_flags |= LD_RF_NO_GL_LS;

	if (strstr(flags_str, "LOCAL_LS"))
		*lockd_flags |= LD_RF_LOCAL_LS;

	if (strstr(flags_str, "DUP_GL_LS"))
		*lockd_flags |= LD_RF_DUP_GL_LS;

	if (strstr(flags_str, "INACTIVE_LS"))
		*lockd_flags |= LD_RF_INACTIVE_LS;

	if (strstr(flags_str, "ADD_LS_ERROR"))
		*lockd_flags |= LD_RF_ADD_LS_ERROR;
}

/*
 * evaluate the reply from lvmlockd, check for errors, extract
 * the result and lockd_flags returned by lvmlockd.
 * 0 failure (no result/lockd_flags set)
 * 1 success (result/lockd_flags set)
 */

/*
 * This is an arbitrary number that we know lvmlockd
 * will not return.  daemon_reply_int reverts to this
 * value if it finds no result value.
 */
#define NO_LOCKD_RESULT -1000

static int _lockd_result(daemon_reply reply, int *result, uint32_t *lockd_flags)
{
	int reply_result;
	const char *flags_str = NULL;
	const char *lock_type = NULL;

	if (reply.error) {
		log_error("lockd_result reply error %d", reply.error);
		return 0;
	}

	if (strcmp(daemon_reply_str(reply, "response", ""), "OK")) {
		log_error("lockd_result bad response");
		return 0;
	}

	reply_result = daemon_reply_int(reply, "op_result", NO_LOCKD_RESULT);
	if (reply_result == NO_LOCKD_RESULT) {
		log_error("lockd_result no op_result");
		return 0;
	}

	/* The lock_type that lvmlockd used for locking. */
	lock_type = daemon_reply_str(reply, "lock_type", "none");

	*result = reply_result;

	if (lockd_flags) {
		if ((flags_str = daemon_reply_str(reply, "result_flags", NULL)))
			_flags_str_to_lockd_flags(flags_str, lockd_flags);
	}

	log_debug("lockd_result %d flags %s lm %s", reply_result,
		  flags_str ? flags_str : "none", lock_type);
	return 1;
}

static daemon_reply _lockd_send(const char *req_name, ...)
{
	va_list ap;
	daemon_reply repl;
	daemon_request req;

	req = daemon_request_make(req_name);

	va_start(ap, req_name);
	daemon_request_extend_v(req, ap);
	va_end(ap);

	repl = daemon_send(_lvmlockd, req);

	daemon_request_destroy(req);

	return repl;
}

/*
 * result/lockd_flags are values returned from lvmlockd.
 *
 * return 0 (failure)
 * return 1 (result/lockd_flags indicate success/failure)
 *
 * return 1 result 0   (success)
 * return 1 result < 0 (failure)
 *
 * caller may ignore result < 0 failure depending on
 * lockd_flags and the specific command/mode.
 *
 * When this function returns 0 (failure), no result/lockd_flags
 * were obtained from lvmlockd.
 *
 * When this function returns 1 (success), result/lockd_flags may
 * have been obtained from lvmlockd.  This lvmlockd result may
 * indicate a locking failure.
 */

static int _lockd_request(struct cmd_context *cmd,
		          const char *req_name,
		          const char *vg_name,
		          const char *vg_lock_type,
		          const char *vg_lock_args,
		          const char *lv_name,
		          const char *lv_uuid,
		          const char *lv_lock_args,
		          const char *mode,
		          const char *opts,
		          int *result,
		          uint32_t *lockd_flags)
{
	const char *cmd_name = get_cmd_name();
	daemon_reply reply;
	int pid = getpid();

	*result = 0;
	*lockd_flags = 0;

	if (!strcmp(mode, "na"))
		return 1;

	if (!_use_lvmlockd)
		return 1;
	if (!_lvmlockd_connected)
		return 0;

	/* cmd and pid are passed for informational and debugging purposes */

	if (!cmd_name || !cmd_name[0])
		cmd_name = "none";

	if (vg_name && lv_name) {
		reply = _lockd_send(req_name,
					"cmd = %s", cmd_name,
					"pid = %d", pid,
					"mode = %s", mode,
					"opts = %s", opts ?: "none",
					"vg_name = %s", vg_name,
					"lv_name = %s", lv_name,
					"lv_uuid = %s", lv_uuid,
					"vg_lock_type = %s", vg_lock_type ?: "none",
					"vg_lock_args = %s", vg_lock_args ?: "none",
					"lv_lock_args = %s", lv_lock_args ?: "none",
					NULL);

		if (!_lockd_result(reply, result, lockd_flags))
			goto fail;

		log_debug("lvmlockd %s %s vg %s lv %s result %d %x",
			  req_name, mode, vg_name, lv_name, *result, *lockd_flags);

	} else if (vg_name) {
		reply = _lockd_send(req_name,
					"cmd = %s", cmd_name,
					"pid = %d", pid,
					"mode = %s", mode,
					"opts = %s", opts ?: "none",
					"vg_name = %s", vg_name,
					"vg_lock_type = %s", vg_lock_type ?: "none",
					"vg_lock_args = %s", vg_lock_args ?: "none",
					NULL);

		if (!_lockd_result(reply, result, lockd_flags))
			goto fail;

		log_debug("lvmlockd %s %s vg %s result %d %x",
			  req_name, mode, vg_name, *result, *lockd_flags);

	} else {
		reply = _lockd_send(req_name,
					"cmd = %s", cmd_name,
					"pid = %d", pid,
					"mode = %s", mode,
					"opts = %s", opts ?: "none",
					"vg_lock_type = %s", vg_lock_type ?: "none",
					NULL);

		if (!_lockd_result(reply, result, lockd_flags))
			goto fail;

		log_debug("lvmlockd %s %s result %d %x",
			  req_name, mode, *result, *lockd_flags);
	}

	daemon_reply_destroy(reply);

	/* result/lockd_flags have lvmlockd result */
	return 1;

 fail:
	/* no result was obtained from lvmlockd */

	log_error("lvmlockd %s %s failed no result", req_name, mode);

	daemon_reply_destroy(reply);
	return 0;
}

/*
 * The name of the internal lv created to hold sanlock locks.
 */
#define LVMLOCKD_SANLOCK_LV_NAME "lvmlock"

/*
 * The internal sanlock lv starts at 512MB and is increased by that amount
 * whenever it runs out of space.
 */

#define LVMLOCKD_SANLOCK_LV_EXTEND (512 * 1024 * 1024)

static struct logical_volume *_find_sanlock_lv(struct volume_group *vg,
					       const char *lock_lv_name)
{
	struct lv_list *lvl;

	dm_list_iterate_items(lvl, &vg->lvs) {
		if (!strcmp(lvl->lv->name, lock_lv_name))
			return lvl->lv;
	}
	return NULL;
}

/*
 * Eventually add an option to specify which pv the lvmlock lv should be placed on.
 */

static int _create_sanlock_lv(struct cmd_context *cmd, struct volume_group *vg,
			      const char *lock_lv_name)
{
	struct logical_volume *lv;
	struct lvcreate_params lp = {
		.activate = CHANGE_ALY,
		.alloc = ALLOC_INHERIT,
		.extents = LVMLOCKD_SANLOCK_LV_EXTEND / (vg->extent_size * SECTOR_SIZE),
		.major = -1,
		.minor = -1,
		.permission = LVM_READ | LVM_WRITE,
		.pvh = &vg->pvs,
		.read_ahead = DM_READ_AHEAD_NONE,
		.stripes = 1,
		.vg_name = vg->name,
		.lv_name = dm_pool_strdup(cmd->mem, lock_lv_name),
		.zero = 1,
	};

	dm_list_init(&lp.tags);

	if (!(lp.segtype = get_segtype_from_string(vg->cmd, "striped")))
		return_0;

	lv = lv_create_single(vg, &lp);
	if (!lv) {
		log_error("Failed to create sanlock lv %s in vg %s", lock_lv_name, vg->name);
		return 0;
	}

	lv_set_hidden(lv);

	return 1;
}

static int _remove_sanlock_lv(struct cmd_context *cmd, struct volume_group *vg,
			      const char *lock_lv_name)
{
	struct logical_volume *lv;

	if (!(lv = _find_sanlock_lv(vg, lock_lv_name))) {
		log_error("Failed to find sanlock LV %s in VG %s", lock_lv_name, vg->name);
		return 0;
	}

	if (!lv_remove(lv)) {
		log_error("Failed to remove sanlock LV %s/%s", vg->name, lock_lv_name);
		return 0;
	}

	return 1;
}

static int _extend_sanlock_lv(struct cmd_context *cmd, struct volume_group *vg)
{
	const char *lock_lv_name = LVMLOCKD_SANLOCK_LV_NAME;
	struct logical_volume *lv;
	struct lvresize_params lp = {
		.lv_name = lock_lv_name,
		.sign = SIGN_NONE,
		.percent = PERCENT_NONE,
		.resize = LV_EXTEND,
		.ac_force = 1,
		.sizeargs = 1,
	};

	if (!(lv = _find_sanlock_lv(vg, lock_lv_name))) {
		log_error("Extend failed to find sanlock LV %s in VG %s", lock_lv_name, vg->name);
		return 0;
	}

	lp.size = lv->size + (LVMLOCKD_SANLOCK_LV_EXTEND / SECTOR_SIZE);

	if (!lv_resize_prepare(cmd, lv, &lp, &vg->pvs) ||
	    !lv_resize(cmd, lv, &lp, &vg->pvs)) {
		log_error("Extend LV %s/%s to size %llu failed.",
			  vg->name, lv->name, (unsigned long long)lp.size);
		return 0;
	}

	return 1;
}

/* When one host does _extend_sanlock_lv, the others need to refresh the size. */

static int _refresh_sanlock_lv(struct cmd_context *cmd, struct volume_group *vg)
{
	struct logical_volume *lv;
	const char *lock_lv_name = LVMLOCKD_SANLOCK_LV_NAME;

	if (!(lv = _find_sanlock_lv(vg, lock_lv_name))) {
		log_error("Refresh failed to find sanlock lv %s in vg %s", lock_lv_name, vg->name);
		return 0;
	}

	if (!lv_refresh_suspend_resume(cmd, lv)) {
		log_error("Failed to refresh %s.", lv->name);
		return 0;
	}

	return 1;
}

static int _activate_sanlock_lv(struct cmd_context *cmd, struct volume_group *vg)
{
	struct logical_volume *lv;
	const char *lock_lv_name = LVMLOCKD_SANLOCK_LV_NAME;

	if (!(lv = _find_sanlock_lv(vg, lock_lv_name))) {
		log_error("Failed to find sanlock lv %s in vg %s", lock_lv_name, vg->name);
		return 0;
	}

	if (!activate_lv(cmd, lv)) {
		log_error("Failed to activate sanlock lv %s/%s", vg->name, lock_lv_name);
		return 0;
	}

	return 1;
}

static int _deactivate_sanlock_lv(struct cmd_context *cmd, struct volume_group *vg)
{
	struct logical_volume *lv;
	const char *lock_lv_name = LVMLOCKD_SANLOCK_LV_NAME;

	if (!(lv = _find_sanlock_lv(vg, lock_lv_name))) {
		log_error("Failed to find sanlock lv %s in vg %s", lock_lv_name, vg->name);
		return 0;
	}

	if (!deactivate_lv(cmd, lv)) {
		log_error("Failed to deactivate sanlock lv %s/%s", vg->name, lock_lv_name);
		return 0;
	}

	return 1;
}

static int _init_vg_dlm(struct cmd_context *cmd, struct volume_group *vg)
{
	daemon_reply reply;
	const char *reply_str;
	const char *vg_lock_args = NULL;
	int result;
	int ret;

	if (!_use_lvmlockd)
		return 1;
	if (!_lvmlockd_connected)
		return 0;

	reply = _lockd_send("init_vg",
				"pid = %d", getpid(),
				"vg_name = %s", vg->name,
				"vg_lock_type = %s", "dlm",
				NULL);

	if (!_lockd_result(reply, &result, NULL)) {
		ret = 0;
		result = -ELOCKD;
	} else {
		ret = (result < 0) ? 0 : 1;
	}

	switch (result) {
	case 0:
		log_print_unless_silent("VG %s initialized %s lockspace", vg->name, vg->lock_type);
		break;
	case -ELOCKD:
		log_error("VG %s init failed: lvmlockd not available", vg->name);
		break;
	case -EARGS:
		log_error("VG %s init failed: invalid parameters for %s", vg->name, vg->lock_type);
		break;
	case -EMANAGER:
		log_error("VG %s init failed: lock manager %s is not running", vg->name, vg->lock_type);
		break;
	default:
		log_error("VG %s init failed: %d", vg->name, result);
	}

	if (!ret)
		goto out;

	if (!(reply_str = daemon_reply_str(reply, "vg_lock_args", NULL))) {
		log_error("VG %s init failed: lock_args not returned", vg->name);
		ret = 0;
		goto out;
	}

	if (!(vg_lock_args = dm_pool_strdup(cmd->mem, reply_str))) {
		log_error("VG %s init failed: lock_args alloc failed", vg->name);
		ret = 0;
		goto out;
	}

	vg->lock_args = vg_lock_args;

	if (!vg_write(vg) || !vg_commit(vg)) {
		log_error("VG %s init failed: vg_write vg_commit", vg->name);
		ret = 0;
		goto out;
	}

	ret = 1;
out:
	daemon_reply_destroy(reply);
	return ret;
}

static int _init_vg_sanlock(struct cmd_context *cmd, struct volume_group *vg)
{
	daemon_reply reply;
	const char *reply_str;
	const char *vg_lock_args = NULL;
	const char *lock_lv_name = LVMLOCKD_SANLOCK_LV_NAME;
	const char *opts = NULL;
	int result;
	int ret;

	if (!_use_lvmlockd)
		return 1;
	if (!_lvmlockd_connected)
		return 0;

	if (!_create_sanlock_lv(cmd, vg, lock_lv_name)) {
		log_error("Failed to create internal lv.");
		return 0;
	}

	/*
	 * N.B. this passes the lock_lv_name as vg_lock_args
	 * even though it is only part of the final args string
	 * which will be returned from lvmlockd.
	 */

	reply = _lockd_send("init_vg",
				"pid = %d", getpid(),
				"vg_name = %s", vg->name,
				"vg_lock_type = %s", "sanlock",
				"vg_lock_args = %s", lock_lv_name,
				"opts = %s", opts ?: "none",
				NULL);

	if (!_lockd_result(reply, &result, NULL)) {
		ret = 0;
		result = -ELOCKD;
	} else {
		ret = (result < 0) ? 0 : 1;
	}

	switch (result) {
	case 0:
		log_print_unless_silent("VG %s initialized %s lockspace", vg->name, vg->lock_type);
		break;
	case -ELOCKD:
		log_error("VG %s init failed: lvmlockd not available", vg->name);
		break;
	case -EARGS:
		log_error("VG %s init failed: invalid parameters for %s", vg->name, vg->lock_type);
		break;
	case -EMANAGER:
		log_error("VG %s init failed: lock manager %s is not running", vg->name, vg->lock_type);
		break;
	case -EMSGSIZE:
		log_error("VG %s init failed: no disk space for leases", vg->name);
		break;
	default:
		log_error("VG %s init failed: %d", vg->name, result);
	}

	if (!ret)
		goto out;

	if (!(reply_str = daemon_reply_str(reply, "vg_lock_args", NULL))) {
		log_error("VG %s init failed: lock_args not returned", vg->name);
		ret = 0;
		goto out;
	}

	if (!(vg_lock_args = dm_pool_strdup(cmd->mem, reply_str))) {
		log_error("VG %s init failed: lock_args alloc failed", vg->name);
		ret = 0;
		goto out;
	}

	vg->lock_args = vg_lock_args;

	if (!vg_write(vg) || !vg_commit(vg)) {
		log_error("VG %s init failed: vg_write vg_commit", vg->name);
		ret = 0;
		goto out;
	}

	ret = 1;
out:
	if (!ret) {
		/*
		 * The usleep delay gives sanlock time to close the lock lv,
		 * and usually avoids having an annoying error printed.
		 */
		usleep(1000000);
		_deactivate_sanlock_lv(cmd, vg);
		_remove_sanlock_lv(cmd, vg, lock_lv_name);
		if (!vg_write(vg) || !vg_commit(vg))
			stack;
	}

	daemon_reply_destroy(reply);
	return ret;
}

/* called after vg_remove on disk */

static int _free_vg_dlm(struct cmd_context *cmd, struct volume_group *vg)
{
	uint32_t lockd_flags;
	int result;
	int ret;

	/*
	 * Unlocking the vg lock here preempts the lvmlockd unlock in
	 * toollib.c which happens too late since the lockspace is
	 * left here.
	 */

	/* Equivalent to a standard unlock. */
	ret = _lockd_request(cmd, "lock_vg",
			     vg->name, NULL, NULL, NULL, NULL, NULL, "un", NULL,
			     &result, &lockd_flags);

	if (!ret || result < 0) {
		log_error("_free_vg_dlm lvmlockd result %d", result);
		return 0;
	}

	/* Leave the dlm lockspace. */
	lockd_stop_vg(cmd, vg);

	return 1;
}

/* called before vg_remove on disk */

static int _free_vg_sanlock(struct cmd_context *cmd, struct volume_group *vg)
{
	daemon_reply reply;
	const char *lock_lv_name = LVMLOCKD_SANLOCK_LV_NAME;
	int result;
	int ret;

	if (!_use_lvmlockd)
		return 1;
	if (!_lvmlockd_connected)
		return 0;

	if (!vg->lock_args || !strlen(vg->lock_args)) {
		/* Shouldn't happen in general, but maybe in some error cases? */
		log_debug("_free_vg_sanlock %s no lock_args", vg->name);
		return 1;
	}

	reply = _lockd_send("free_vg",
				"pid = %d", getpid(),
				"vg_name = %s", vg->name,
				"vg_lock_type = %s", vg->lock_type,
				"vg_lock_args = %s", vg->lock_args,
				NULL);

	if (!_lockd_result(reply, &result, NULL)) {
		ret = 0;
	} else {
		ret = (result < 0) ? 0 : 1;
	}

	/*
	 * Other hosts could still be joined to the lockspace, which means they
	 * are using the internal sanlock LV, which means we cannot remove the
	 * VG.  Once other hosts stop using the VG it can be removed.
	 */
	if (result == -EBUSY) {
		log_error("Lockspace for \"%s\" not stopped on other hosts", vg->name);
		goto out;
	}

	if (!ret) {
		log_error("_free_vg_sanlock lvmlockd result %d", result);
		goto out;
	}

	/*
	 * The usleep delay gives sanlock time to close the lock lv,
	 * and usually avoids having an annoying error printed.
	 */
	usleep(1000000);

	_deactivate_sanlock_lv(cmd, vg);
	_remove_sanlock_lv(cmd, vg, lock_lv_name);
 out:
	daemon_reply_destroy(reply);

	return ret;
}

/* vgcreate */

int lockd_init_vg(struct cmd_context *cmd, struct volume_group *vg)
{       
	switch (lock_type_to_num(vg->lock_type)) {
	case LOCK_TYPE_NONE:
	case LOCK_TYPE_CLVM:
		return 1;
	case LOCK_TYPE_DLM:
		return _init_vg_dlm(cmd, vg);
	case LOCK_TYPE_SANLOCK:
		return _init_vg_sanlock(cmd, vg);
	default:
		log_error("Unknown lock_type.");
		return 0;
	}
}

/* vgremove before the vg is removed */

int lockd_free_vg_before(struct cmd_context *cmd, struct volume_group *vg)
{
	if (cmd->lock_vg_mode && !strcmp(cmd->lock_vg_mode, "na"))
		return 1;

	switch (lock_type_to_num(vg->lock_type)) {
	case LOCK_TYPE_NONE:
	case LOCK_TYPE_CLVM:
	case LOCK_TYPE_DLM:
		return 1;
	case LOCK_TYPE_SANLOCK:
		/* returning an error will prevent vg_remove() */
		return _free_vg_sanlock(cmd, vg);
	default:
		log_error("Unknown lock_type.");
		return 0;
	}
}

/* vgremove after the vg is removed */

void lockd_free_vg_final(struct cmd_context *cmd, struct volume_group *vg)
{
	if (cmd->lock_vg_mode && !strcmp(cmd->lock_vg_mode, "na"))
		return;

	switch (lock_type_to_num(vg->lock_type)) {
	case LOCK_TYPE_NONE:
	case LOCK_TYPE_CLVM:
	case LOCK_TYPE_SANLOCK:
		break;
	case LOCK_TYPE_DLM:
		_free_vg_dlm(cmd, vg);
		break;
	default:
		log_error("Unknown lock_type.");
	}

	/* The vg lock no longer exists, so don't bother trying to unlock. */
	cmd->lockd_vg_disable = 1;
}

/*
 * Starting a vg involves:
 * 1. reading the vg without a lock
 * 2. getting the lock_type/lock_args from the vg metadata
 * 3. doing start_vg in lvmlockd for the lock_type;
 *    this means joining the lockspace
 *
 * The vg read in step 1 should not be used for anything
 * other than getting the lock_type/lock_args/uuid necessary
 * for starting the lockspace.  To use the vg after starting
 * the lockspace, follow the standard method which is:
 * lock the vg, read/use/write the vg, unlock the vg.
 */

int lockd_start_vg(struct cmd_context *cmd, struct volume_group *vg)
{
	char uuid[64] __attribute__((aligned(8)));
	daemon_reply reply;
	int host_id = 0;
	int result;
	int ret;

	memset(uuid, 0, sizeof(uuid));

	if (!_use_lvmlockd)
		return 1;
	if (!_lvmlockd_connected)
		return 0;

	/* Skip starting the vg lockspace when the vg lock is skipped. */

	if (cmd->lock_vg_mode && !strcmp(cmd->lock_vg_mode, "na"))
		return 1;

	if (!is_lockd_type(vg->lock_type))
		return 1;

	log_debug("lockd_start_vg %s lock_type %s", vg->name,
		  vg->lock_type ? vg->lock_type : "empty");

	if (vg->lock_type && !strcmp(vg->lock_type, "sanlock")) {
		/*
		 * This is the big difference between starting
		 * sanlock vgs vs starting dlm vgs: the internal
		 * sanlock lv needs to be activated before lvmlockd
		 * does the start because sanlock needs to use the lv
		 * to access locks.
		 */
		if (!_activate_sanlock_lv(cmd, vg))
			return 0;

		host_id = find_config_tree_int(cmd, local_host_id_CFG, NULL);
	}

	id_write_format(&vg->id, uuid, sizeof(uuid));

	reply = _lockd_send("start_vg",
				"pid = %d", getpid(),
				"vg_name = %s", vg->name,
				"vg_lock_type = %s", vg->lock_type,
				"vg_lock_args = %s", vg->lock_args ?: "none",
				"vg_uuid = %s", uuid[0] ? uuid : "none",
				"version = %d", (int64_t)vg->seqno,
				"host_id = %d", host_id,
				NULL);

	if (!_lockd_result(reply, &result, NULL)) {
		ret = 0;
		result = -ELOCKD;
	} else {
		ret = (result < 0) ? 0 : 1;
	}

	switch (result) {
	case 0:
		log_print_unless_silent("VG %s starting %s lockspace", vg->name, vg->lock_type);
		break;
	case -ELOCKD:
		log_error("VG %s start failed: lvmlockd not available", vg->name);
		break;
	case -EEXIST:
		log_debug("VG %s start error: already started", vg->name);
		ret = 1;
		break;
	case -EARGS:
		log_error("VG %s start failed: invalid parameters for %s", vg->name, vg->lock_type);
		break;
	case -EHOSTID:
		log_error("VG %s start failed: invalid sanlock host_id, set in lvmlocal.conf", vg->name);
		break;
	case -EMANAGER:
		log_error("VG %s start failed: lock manager %s is not running", vg->name, vg->lock_type);
		break;
	default:
		log_error("VG %s start failed: %d", vg->name, result);
	}

	daemon_reply_destroy(reply);

	return ret;
}

int lockd_stop_vg(struct cmd_context *cmd, struct volume_group *vg)
{
	daemon_reply reply;
	int result;
	int ret;

	if (!is_lockd_type(vg->lock_type))
		return 1;

	if (!_use_lvmlockd)
		return 1;
	if (!_lvmlockd_connected)
		return 0;

	log_debug("lockd_stop_vg %s lock_type %s", vg->name,
		  vg->lock_type ? vg->lock_type : "empty");

	reply = _lockd_send("stop_vg",
			"pid = %d", getpid(),
			"vg_name = %s", vg->name,
			NULL);

	if (!_lockd_result(reply, &result, NULL)) {
		ret = 0;
	} else {
		ret = (result < 0) ? 0 : 1;
	}

	if (result == -EBUSY) {
		log_error("VG %s stop failed: LVs must first be deactivated", vg->name);
		goto out;
	}

	if (!ret) {
		log_error("VG %s stop failed: %d", vg->name, result);
		goto out;
	}

	if (!strcmp(vg->lock_type, "sanlock")) {
		log_debug("lockd_stop_vg deactivate sanlock lv");
		_deactivate_sanlock_lv(cmd, vg);
	}
out:
	daemon_reply_destroy(reply);

	return ret;
}

int lockd_start_wait(struct cmd_context *cmd)
{
	daemon_reply reply;
	int result;
	int ret;

	if (!_use_lvmlockd)
		return 1;
	if (!_lvmlockd_connected)
		return 0;

	reply = _lockd_send("start_wait",
			"pid = %d", getpid(),
			NULL);

	if (!_lockd_result(reply, &result, NULL)) {
		ret = 0;
	} else {
		ret = (result < 0) ? 0 : 1;
	}

	if (!ret)
		log_error("Lock start failed");

	/*
	 * Get a list of vgs that started so we can
	 * better report what worked and what didn't?
	 */

	daemon_reply_destroy(reply);

	return ret;
}

static int _mode_num(const char *mode)
{
	if (!strcmp(mode, "na"))
		return -2;
	if (!strcmp(mode, "un"))
		return -1;
	if (!strcmp(mode, "nl"))
		return 0;
	if (!strcmp(mode, "sh"))
		return 1;
	if (!strcmp(mode, "ex"))
		return 2;
	return -3;
}

/* same rules as strcmp */
static int _mode_compare(const char *m1, const char *m2)
{
	int n1 = _mode_num(m1);
	int n2 = _mode_num(m2);

	if (n1 < n2)
		return -1;
	if (n1 == n2)
		return 0;
	if (n1 > n2)
		return 1;
	return -2;
}

/*
 * Mode is selected by:
 * 1. mode from command line option (only taken if allow_override is set)
 * 2. the function arg passed by the calling command (def_mode)
 * 3. look up a default mode for the command
 *    (cases where the caller doesn't know a default)
 *
 * MODE_NOARG: don't use mode from command line option
 */

/*
 * lockd_gl_create() is a variation of lockd_gl() used only by vgcreate.
 * It handles the case that when using sanlock, the global lock does
 * not exist until after the first vgcreate is complete, since the global
 * lock exists on storage within an actual VG.  So, the first vgcreate
 * needs special logic to detect this bootstrap case.
 *
 * When the vgcreate is not creating the first VG, then lockd_gl_create()
 * behaves the same as lockd_gl().
 *
 * vgcreate will have a lock_type for the new VG which lockd_gl_create()
 * can provide in the lock-gl call.
 *
 * lockd_gl() and lockd_gl_create() differ in the specific cases where
 * ENOLS (no lockspace found) is overriden.  In the vgcreate case, the
 * override cases are related to sanlock bootstrap, and the lock_type of
 * the vg being created is needed.
 *
 * 1. vgcreate of the first lockd-type vg calls lockd_gl_create()
 *    to acquire the global lock.
 *
 * 2. vgcreate/lockd_gl_create passes gl lock request to lvmlockd,
 *    along with lock_type of the new vg.
 *
 * 3. lvmlockd finds no global lockspace/lock.
 *
 * 4. dlm:
 *    If the lock_type from vgcreate is dlm, lvmlockd creates the
 *    dlm global lockspace, and queues the global lock request
 *    for vgcreate.  lockd_gl_create returns sucess with the gl held.
 *
 *    sanlock:
 *    If the lock_type from vgcreate is sanlock, lvmlockd returns -ENOLS
 *    with the NO_GL_LS flag.  lvmlockd cannot create or acquire a sanlock
 *    global lock until the VG exists on disk (the locks live within the VG).
 *
 *    lockd_gl_create sees sanlock/ENOLS/NO_GL_LS (and optionally the
 *    "enable" lock-gl arg), determines that this is the sanlock
 *    bootstrap special case, and returns success without the global lock.
 *   
 *    vgcreate creates the VG on disk, and calls lockd_init_vg() which
 *    initializes/enables a global lock on the new VG's internal sanlock lv.
 *    Future lockd_gl/lockd_gl_create calls will acquire the existing gl.
 */

int lockd_gl_create(struct cmd_context *cmd, const char *def_mode, const char *vg_lock_type)
{
	const char *mode = NULL;
	uint32_t lockd_flags;
	int retries = 0;
	int result;

	/* A specific lock mode was given on the command line. */
	if (cmd->lock_gl_mode) {
		mode = cmd->lock_gl_mode;
		if (mode && def_mode && strcmp(mode, "enable") && (_mode_compare(mode, def_mode) < 0)) {
			if (!find_config_tree_bool(cmd, global_allow_override_lock_modes_CFG, NULL)) {
				log_error("Disallowed lock-gl mode \"%s\"", mode);
				return 0;
			} else {
				log_warn("WARNING: overriding default global lock mode.");
			}
		}
	}

	if (!mode)
		mode = def_mode;
	if (!mode) {
		log_error("Unknown lock-gl mode");
		return 0;
	}

	if (!strcmp(mode, "ex") && find_config_tree_bool(cmd, global_read_only_lock_modes_CFG, NULL)) {
		log_error("Disallow lock-gl ex with read_only_lock_modes");
		return 0;
	}

 req:
	if (!_lockd_request(cmd, "lock_gl",
			      NULL, vg_lock_type, NULL, NULL, NULL, NULL, mode, NULL,
			      &result, &lockd_flags)) {
		/* No result from lvmlockd, it is probably not running. */
		log_error("Locking failed for global lock");
		return 0;
	}

	if (result == -EAGAIN) {
		if (retries < find_config_tree_int(cmd, global_lock_retries_CFG, NULL)) {
			log_warn("Retrying %s global lock", mode);
			sleep(1);
			retries++;
			goto req;
		}
	}

	/*
	 * result and lockd_flags were returned from lvmlockd.
	 *
	 * ENOLS: no lockspace was found with a global lock.
	 * It may not exist (perhaps this command is creating the first),
	 * or it may not be visible or started on the system yet.
	 */

	if (result == -ENOLS) {
		if (!strcmp(mode, "un"))
			return 1;

		/*
		 * This is the explicit sanlock bootstrap condition for
		 * proceding without the global lock: a chicken/egg case
		 * for the first sanlock VG that is created.
		 *
		 * When creating the first sanlock VG, there is no global
		 * lock to acquire because the gl will exist in the VG
		 * being created.  The "enable" option makes explicit that
		 * this is expected:
		 *
		 * vgcreate --lock-type sanlock --lock-gl enable
		 *
		 * There are three indications that this is the unique
		 * first-sanlock-vg bootstrap case:
		 *
		 * - result from lvmlockd is -ENOLS because lvmlockd found
		 *   no lockspace for this VG; expected because it's being
		 *   created here.
		 *
		 * - result flag LD_RF_NO_GL_LS from lvmlockd means that
		 *   lvmlockd has seen no other lockspace with a global lock.
		 *   This implies that this is probably the first sanlock vg
		 *   to be created.  If other sanlock vgs exist, the global
		 *   lock should be available from one of them.
		 *
		 * - command line lock-gl arg is "enable" which means the
		 *   user expects this to be the first sanlock vg, and the
		 *   global lock should be enabled in it.
		 */

		if ((lockd_flags & LD_RF_NO_GL_LS) &&
		    !strcmp(vg_lock_type, "sanlock") &&
		    !strcmp(mode, "enable")) {
			log_debug("Enabling sanlock global lock");
			lvmetad_validate_global_cache(cmd, 1);
			return 1;
		}

		/*
		 * This is an implicit sanlock bootstrap condition for
		 * proceeding without the global lock.  The command line does
		 * not indicate explicitly that this is a bootstrap situation
		 * (via "enable"), but it seems likely to be because lvmlockd
		 * has seen no lockd-type vgs.  It is possible that a global
		 * lock does exist in a vg that has not yet been seen.  If that
		 * vg appears after this creates a new vg with a new enabled
		 * gl, then there will be two enabled global locks, and one
		 * will need to be disabled.  (We could instead return an error
		 * here and insist with an error message that the --lock-gl
		 * enable option be used to exercise the explicit case above.)
		 */

		if ((lockd_flags & LD_RF_NO_GL_LS) &&
		    (lockd_flags & LD_RF_NO_LOCKSPACES) &&
		    !strcmp(vg_lock_type, "sanlock")) {
			log_print_unless_silent("Enabling sanlock global lock");
			lvmetad_validate_global_cache(cmd, 1);
			return 1;
		}

		log_error("Global lock %s error %d", mode, result);
		return 0;
	}

	if (result < 0) {
		if (result == -ESTARTING)
			log_error("Global lock %s error: lockspace is starting", mode);
		else
			log_error("Global lock %s error %d", mode, result);
		return 0;
	}

	lvmetad_validate_global_cache(cmd, 1);

	return 1;
}

/*
 * The global lock protects:
 *
 * - The global VG namespace.  Two VGs cannot have the same name.
 *   Used by any command that creates or removes a VG name,
 *   e.g. vgcreate, vgremove, vgrename, vgsplit, vgmerge.
 *
 * - The set of orphan PVs.
 *   Used by any command that changes a non-PV device into an orphan PV,
 *   an orphan PV into a device, a non-orphan PV (in a VG) into an orphan PV
 *   (not in a VG), or an orphan PV into a non-orphan PV,
 *   e.g. pvcreate, pvremove, vgcreate, vgremove, vgextend, vgreduce.
 *
 * - The properties of orphan PVs.  It is possible to make changes to the
 *   properties of an orphan PV, e.g. pvresize, pvchange.
 *
 * These are things that cannot be protected by a VG lock alone, since
 * orphan PVs do not belong to a real VG (an artificial VG does not
 * apply since a sanlock lock only exists on real storage.)
 *
 * If a command will change any of the things above, it must first acquire
 * the global lock in exclusive mode.
 *
 * If command is reading any of the things above, it must acquire the global
 * lock in shared mode.  A number of commands read the things above, including:
 *
 * - Reporting/display commands which show all VGs.  Any command that
 *   will iterate through the entire VG namespace must first acquire the
 *   global lock shared so that it has an accurate view of the namespace.
 *
 * - A command where a tag name is used to identify what to process.
 *   A tag requires reading all VGs to check if they match the tag.
 *
 * In these cases, the global lock must be acquired before the list of
 * all VGs is created.
 *
 * The global lock is not generally unlocked explicitly in the code.
 * When the command disconnects from lvmlockd, lvmlockd automatically
 * releases the locks held by the command.  The exception is if a command
 * will continue running for a long time while not needing the global lock,
 * e.g. commands that poll to report progress.
 *
 * Acquiring the global lock also updates the local lvmetad cache if
 * necessary.  lockd_gl() first acquires the lock via lvmlockd, then
 * before returning to the caller, it checks that the global information
 * (e.g. VG namespace, set of orphans) is up to date in lvmetad.  If
 * not, it scans disks and updates the lvmetad cache before returning
 * to the caller.  It does this checking using a version number associated
 * with the global lock.  The version number is incremented each time
 * a change is made to the state associated with the global lock, and
 * if the local version number is lower than the version number in the
 * lock, then the local lvmetad state must be updated.
 */

int lockd_gl(struct cmd_context *cmd, const char *def_mode, uint32_t flags)
{
	const char *mode = NULL;
	const char *opts = NULL;
	uint32_t lockd_flags;
	int retries = 0;
	int result;

	/* A specific lock mode was given on the command line. */
	if (!(flags & LDGL_MODE_NOARG) && cmd->lock_gl_mode) {
		mode = cmd->lock_gl_mode;
		if (mode && def_mode && (_mode_compare(mode, def_mode) < 0)) {
			if (!find_config_tree_bool(cmd, global_allow_override_lock_modes_CFG, NULL)) {
				log_error("Disallowed lock-gl mode \"%s\"", mode);
				return 0;
			} else {
				log_warn("WARNING: overriding default global lock mode.");
			}
		}
	}

	if (!mode)
		mode = def_mode;
	if (!mode) {
		log_error("Unknown lock-gl mode");
		return 0;
	}

	if (!strcmp(mode, "ex") && find_config_tree_bool(cmd, global_read_only_lock_modes_CFG, NULL)) {
		log_error("Disallow lock-gl ex with read_only_lock_modes");
		return 0;
	}

 req:
	if (!_lockd_request(cmd, "lock_gl",
			    NULL, NULL, NULL, NULL, NULL, NULL, mode, opts,
			    &result, &lockd_flags)) {
		/* No result from lvmlockd, it is probably not running. */

		/*
		 * We don't care if an unlock operation fails in this case, and
		 * we can allow a shared lock request to succeed without any
		 * serious harm.  To disallow basic reading/reporting when
		 * lvmlockd is stopped is too strict, unnecessary, and
		 * inconvenient.  We force a global cache validation in this
		 * case.
		 */

		if (!strcmp(mode, "un"))
			return 1;

		if (!strcmp(mode, "sh")) {
			log_warn("Reading without shared global lock.");
			lvmetad_validate_global_cache(cmd, 1);
			return 1;
		}

		log_error("Locking failed for global lock");
		return 0;
	}

	if (result == -EAGAIN) {
		if (retries < find_config_tree_int(cmd, global_lock_retries_CFG, NULL)) {
			log_warn("Retrying %s global lock", mode);
			sleep(1);
			retries++;
			goto req;
		}
	}

	/*
	 * result and lockd_flags were returned from lvmlockd.
	 *
	 * ENOLS: no lockspace was found with a global lock.
	 * The VG with the global lock may not be visible or started yet,
	 * this should be a temporary condition.
	 *
	 * ESTARTING: the lockspace with the gl is starting.
	 * The VG with the global lock is starting and should finish shortly.
	 */

	if (result == -ENOLS || result == -ESTARTING) {
		if (!strcmp(mode, "un"))
			return 1;

		/*
		 * This is a general condition for allowing the command to
		 * proceed without a shared global lock when the global lock is
		 * not found or ready.  This should not be a persistent
		 * condition.  The VG containing the global lock should appear
		 * on the system, or the global lock should be enabled in
		 * another VG, or the the lockspace with the gl should finish
		 * starting.
		 *
		 * Same reasons as above for allowing the command to proceed
		 * with the shared gl.  We force a global cache validation and
		 * print a warning.
		 */

		if (strcmp(mode, "sh")) {
			if (result == -ESTARTING)
				log_error("Global lock %s error: lockspace is starting", mode);
			else
				log_error("Global lock %s error %d", mode, result);
			return 0;
		}

		if (result == -ESTARTING) {
			log_warn("Skipping global lock: lockspace is starting");
			lvmetad_validate_global_cache(cmd, 1);
			return 1;
		}

		if ((lockd_flags & LD_RF_NO_GL_LS) ||
		    (lockd_flags & LD_RF_NO_LOCKSPACES)) {
			log_warn("Skipping global lock: not found");
			lvmetad_validate_global_cache(cmd, 1);
			return 1;
		}

		log_error("Global lock %s error %d", mode, result);
		return 0;
	}

	if ((lockd_flags & LD_RF_DUP_GL_LS) && strcmp(mode, "un"))
		log_warn("Duplicate sanlock global locks should be corrected");

	if (result < 0) {
		if (ignorelockingfailure()) {
			log_debug("Ignore failed locking for global lock");
			lvmetad_validate_global_cache(cmd, 1);
			return 1;
		} else {
			log_error("Global lock %s error %d", mode, result);
			return 0;
		}
	}

	if (!(flags & LDGL_SKIP_CACHE_VALIDATE))
		lvmetad_validate_global_cache(cmd, 0);

	return 1;
}

/*
 * VG lock
 *
 * Return 1: continue, lockd_state may still indicate an error
 * Return 0: failure, do not continue
 *
 * lvmlockd could also return the lock_type that it used for the VG,
 * and we could encode that in lockd_state, and verify later that it
 * matches vg->lock_type.
 *
 * The result of the VG lock operation needs to be saved in lockd_state
 * because the result needs to be passed into vg_read so it can be
 * assessed in combination with vg->lock_state.
 *
 * The VG lock protects the VG metadata on disk from concurrent access
 * among hosts.  The VG lock also ensures that the local lvmetad cache
 * contains the latest version of the VG metadata from disk.  (Since
 * another host may have changed the VG since it was last read.)
 *
 * The VG lock must be acquired before the VG is read, i.e. before vg_read().
 * The result from lockd_vg() is saved in the "lockd_state" variable, and
 * this result is passed into vg_read().  After vg_read() reads the VG,
 * it checks if the VG lock_type (sanlock or dlm) requires a lock to be
 * held, and if so, it verifies that the lock was correctly acquired by
 * looking at lockd_state.  If vg_read() sees that the VG is a local VG,
 * i.e. lock_type is not sanlock or dlm, then no lock is required, and it
 * ignores lockd_state (which would indicate no lock was found.)
 *
 * When acquiring the VG lock, lvmlockd checks if the local cached copy
 * of the VG metadata in lvmetad is up to date.  If not, it invalidates
 * the VG cached in lvmetad.  This would happen if another host changed
 * the VG since it was last read.  When lvm commands read the VG from
 * lvmetad, they will check if the metadata is invalid, and if so they
 * will reread it from disk, and update the copy in lvmetad.
 */

int lockd_vg(struct cmd_context *cmd, const char *vg_name, const char *def_mode,
	     uint32_t flags, uint32_t *lockd_state)
{
	const char *mode = NULL;
	uint32_t lockd_flags;
	int retries = 0;
	int result;
	int ret;

	if (!is_real_vg(vg_name))
		return 1;

	/*
	 * Some special cases need to disable the vg lock.
	 */
	if (cmd->lockd_vg_disable)
		return 1;

	/*
	 * An unlock is simply sent or skipped without any need
	 * for the mode checking for sh/ex.
	 *
	 * Look at lockd_state from the sh/ex lock, and if it failed,
	 * don't bother sending the unlock to lvmlockd.  The main
	 * purpose of this is to avoid sending an unnecessary unlock
	 * for local VGs (the lockd_state from sh/ex on the local VG
	 * will be failed.)  This implies that the lockd_state value
	 * should be preserved from the sh/ex lockd_vg() call and
	 * passed back to lockd_vg() for the corresponding unlock.
	 */
	if (def_mode && !strcmp(def_mode, "un")) {
		if (cmd->lock_vg_mode && !strcmp(cmd->lock_vg_mode, "na"))
			return 1;

		if (*lockd_state & LDST_FAIL) {
			log_debug("VG %s unlock skipped: lockd_state is failed", vg_name);
			return 1;
		}

		mode = "un";
		goto req;
	}

	*lockd_state = 0;

	/*
	 * A specific lock mode was given on the command line.
	 * LDVG_MODE_NOARG disables getting the mode from --lock-vg arg.
	 */
	if (!(flags & LDVG_MODE_NOARG) && cmd->lock_vg_mode) {
		mode = cmd->lock_vg_mode;
		if (mode && def_mode && (_mode_compare(mode, def_mode) < 0)) {
			if (!find_config_tree_bool(cmd, global_allow_override_lock_modes_CFG, NULL)) {
				log_error("Disallowed lock-vg mode \"%s\"", mode);
				return 0;
			} else {
				log_warn("WARNING: overriding default VG lock mode.");
			}
		}
	}

	/*
	 * The default mode may not have been provided in the
	 * function args.  This happens when lockd_vg is called
	 * from a process_each function that handles different
	 * commands.  Commands that only read/check/report/display
	 * the vg have LOCKD_VG_SH set in commands.h, which is
	 * copied to lockd_vg_default_sh.  Commands without this
	 * set modify the vg and need ex.
	 */
	if (!mode)
		mode = def_mode;
	if (!mode)
		mode = cmd->lockd_vg_default_sh ? "sh" : "ex";

	if (!strcmp(mode, "ex") && find_config_tree_bool(cmd, global_read_only_lock_modes_CFG, NULL)) {
		log_error("Disallow VG ex lock with read_only_lock_modes");
		return 0;
	}

	if (!strcmp(mode, "ex"))
		*lockd_state |= LDST_EX;
	else if (!strcmp(mode, "sh"))
		*lockd_state |= LDST_SH;

 req:
	if (!_lockd_request(cmd, "lock_vg",
			      vg_name, NULL, NULL, NULL, NULL, NULL, mode, NULL,
			      &result, &lockd_flags)) {
		/*
		 * No result from lvmlockd, it is probably not running.
		 * Decide if it is ok to continue without a lock after
		 * reading the VG and looking at the lock_type.
		 */
		*lockd_state |= LDST_FAIL_REQUEST;
		return 1;
	}

	if (result == -EAGAIN) {
		if (retries < find_config_tree_int(cmd, global_lock_retries_CFG, NULL)) {
			log_warn("Retrying %s lock on VG %s", mode, vg_name);
			sleep(1);
			retries++;
			goto req;
		}
	}

	switch (result) {
	case 0:
		/* success */
		break;
	case -ENOLS:
		*lockd_state |= LDST_FAIL_NOLS;
		break;
	case -ESTARTING:
		*lockd_state |= LDST_FAIL_STARTING;
		break;
	default:
		*lockd_state |= LDST_FAIL_OTHER;
	}

	/*
	 * Normal success.
	 */
	if (!result) {
		ret = 1;
		goto out;
	}

	/*
	 * The lockspace for the VG is starting (the VG must not
	 * be local), and is not yet ready to do locking.  Allow
	 * reading without a sh lock during this period.
	 */
	if (result == -ESTARTING) {
		if (!strcmp(mode, "un")) {
			ret = 1;
			goto out;
		} else if (!strcmp(mode, "sh")) {
			log_warn("VG %s lock skipped: lock start in progress", vg_name);
			ret = 1;
			goto out;
		} else {
			log_error("VG %s lock failed: lock start in progress", vg_name);
			ret = 0;
			goto out;
		}
	}

	/*
	 * An unused/previous lockspace for the VG was found.
	 * This means it must be a lockd VG, not local.  The
	 * lockspace needs to be started to be used.
	 */
	if ((result == -ENOLS) && (lockd_flags & LD_RF_INACTIVE_LS)) {
		if (!strcmp(mode, "un")) {
			ret = 1;
			goto out;
		} else if (!strcmp(mode, "sh")) {
			log_warn("VG %s lock skipped: lockspace is inactive", vg_name);
			ret = 1;
			goto out;
		} else {
			log_error("VG %s lock failed: lockspace is inactive", vg_name);
			ret = 0;
			goto out;
		}
	}

	/*
	 * An unused lockspace for the VG was found.  The previous
	 * start of the lockspace failed, so we can print a more useful
	 * error message.
	 */
	if ((result == -ENOLS) && (lockd_flags & LD_RF_ADD_LS_ERROR)) {
		if (!strcmp(mode, "un")) {
			ret = 1;
			goto out;
		} else if (!strcmp(mode, "sh")) {
			log_warn("VG %s lock skipped: lockspace start error", vg_name);
			ret = 1;
			goto out;
		} else {
			log_error("VG %s lock failed: lockspace start error", vg_name);
			ret = 0;
			goto out;
		}
	}

	/*
	 * No lockspace for the VG was found.  It may be a local
	 * VG that lvmlockd doesn't keep track of, or it may be
	 * a lockd VG that lvmlockd doesn't yet know about (it hasn't
	 * been started yet.)  Decide what to do after the VG is
	 * read and we can see the lock_type.
	 */
	if (result == -ENOLS) {
		ret = 1;
		goto out;
	}

	/*
	 * Unknown error.
	 */
	if (result) {
		if (!strcmp(mode, "un")) {
			ret = 1;
			goto out;
		} else if (!strcmp(mode, "sh")) {
			log_warn("VG %s lock skipped: error %d", vg_name, result);
			ret = 1;
			goto out;
		} else {
			log_error("VG %s lock failed: error %d", vg_name, result);
			ret = 0;
			goto out;
		}
	}

out:
	/*
	 * A notice from lvmlockd that duplicate gl locks have been found.
	 * It would be good for the user to disable one of them.
	 */
	if ((lockd_flags & LD_RF_DUP_GL_LS) && strcmp(mode, "un"))
		log_warn("Duplicate sanlock global lock in VG %s", vg_name);
 
	if (!ret && ignorelockingfailure()) {
		log_debug("Ignore failed locking for VG %s", vg_name);
		return 1;
	}
 
	return ret;
}

/*
 * This must be called before a new version of the VG metadata is
 * written to disk.  For local VGs, this is a no-op, but for lockd
 * VGs, this notifies lvmlockd of the new VG seqno.  lvmlockd must
 * know the latest VG seqno so that it can save it within the lock's
 * LVB.  The VG seqno in the VG lock's LVB is used by other hosts to
 * detect when their cached copy of the VG metadata is stale, i.e.
 * the cached VG metadata has a lower seqno than the seqno seen in
 * the VG lock.
 */

int lockd_vg_update(struct volume_group *vg)
{
	daemon_reply reply;
	int result;
	int ret;

	if (!is_lockd_type(vg->lock_type))
		return 1;

	if (!_use_lvmlockd)
		return 1;
	if (!_lvmlockd_connected)
		return 0;

	reply = _lockd_send("vg_update",
				"pid = %d", getpid(),
				"vg_name = %s", vg->name,
				"version = %d", (int64_t)vg->seqno,
				NULL);

	if (!_lockd_result(reply, &result, NULL)) {
		ret = 0;
	} else {
		ret = (result < 0) ? 0 : 1;
	}

	daemon_reply_destroy(reply);
	return ret;
}

/*
 * When this is called directly (as opposed to being called from
 * lockd_lv), the caller knows that the LV has a lock.
 */

int lockd_lv_name(struct cmd_context *cmd, struct volume_group *vg,
		  const char *lv_name, struct id *lv_id,
		  const char *lock_args, const char *def_mode, uint32_t flags)
{
	char lv_uuid[64] __attribute__((aligned(8)));
	const char *mode = NULL;
	const char *opts = NULL;
	uint32_t lockd_flags;
	int refreshed = 0;
	int result;

	if (cmd->lockd_lv_disable)
		return 1;

	id_write_format(lv_id, lv_uuid, sizeof(lv_uuid));

	/*
	 * For lvchange/vgchange activation, def_mode is "sh" or "ex"
	 * according to the specific -a{e,s}y mode designation.
	 * No e,s designation gives NULL def_mode.
	 *
	 * The --lock-lv option is saved in cmd->lock_lv_mode.
	 */

	if (cmd->lock_lv_mode && def_mode && strcmp(cmd->lock_lv_mode, "na") &&
	    strcmp(cmd->lock_lv_mode, def_mode)) {
		log_error("Different LV lock modes from activation %s and lock-lv %s",
			  def_mode, cmd->lock_lv_mode);
		return 0;
	}

	/* A specific lock mode was given on the command line. */
	if (cmd->lock_lv_mode && (_mode_compare(cmd->lock_lv_mode, "sh") < 0)) {
		if (!find_config_tree_bool(cmd, global_allow_override_lock_modes_CFG, NULL)) {
			log_error("Disallowed lock-lv mode \"%s\"", cmd->lock_lv_mode);
			return 0;
		} else {
			log_warn("WARNING: overriding default LV lock mode.");
		}
	}

	if (cmd->lock_lv_mode)
		mode = cmd->lock_lv_mode;
	else if (def_mode)
		mode = def_mode;

	if (mode && !strcmp(mode, "sh") && (flags & LDLV_MODE_NO_SH)) {
		log_error("Shared activation not compatible with LV type: %s/%s",
			  vg->name, lv_name);
		return 0;
	}

	if (!mode)
		mode = "ex";

	if (flags & LDLV_PERSISTENT)
		opts = "persistent";

 retry:
	if (!_lockd_request(cmd, "lock_lv",
			       vg->name, vg->lock_type, vg->lock_args,
			       lv_name, lv_uuid, lock_args, mode, opts,
			       &result, &lockd_flags)) {
		/* No result from lvmlockd, it is probably not running. */
		log_error("Locking failed for LV %s/%s", vg->name, lv_name);
		return 0;
	}

	/* The lv was not active/locked. */
	if (result == -ENOENT && !strcmp(mode, "un"))
		return 1;

	if (result == -EALREADY)
		return 1;

	if (result == -EAGAIN) {
		log_error("LV locked by other host: %s/%s", vg->name, lv_name);
		return 0;
	}

	if (result == -EMSGSIZE) {
		/* Another host probably extended lvmlock. */
		if (!refreshed++) {
			log_debug("Refresh lvmlock");
		       	_refresh_sanlock_lv(cmd, vg);
			goto retry;
		}
	}

	if (result < 0) {
		log_error("LV lock %s error %d: %s/%s", mode, result, vg->name, lv_name);
		return 0;
	}

	return 1;
}

/*
 * Direct the lock request to the pool LV.
 * For a thin pool and all its thin volumes, one ex lock is used.
 * It is the one specified in metadata of the pool data lv.
 */

static int _lockd_lv_thin(struct cmd_context *cmd, struct logical_volume *lv,
			  const char *def_mode, uint32_t flags)
{
	struct logical_volume *pool_lv;

	if (lv_is_thin_volume(lv)) {
		struct lv_segment *pool_seg = first_seg(lv);
		pool_lv = pool_seg ? pool_seg->pool_lv : NULL;

	} else if (lv_is_thin_pool(lv)) {
		pool_lv = lv;

	} else {
		/* This should not happen AFAIK. */
		log_error("Lock on incorrect thin lv type %s/%s",
			  lv->vg->name, lv->name);
		return 0;
	}

	if (!pool_lv) {
		/* This should not happen. */
		log_error("Cannot find thin pool for %s/%s",
			  lv->vg->name, lv->name);
		return 0;
	}

	/*
	 * Locking a locked lv (pool in this case) is a no-op.
	 * Unlock when the pool is no longer active.
	 */

	if (def_mode && !strcmp(def_mode, "un") && pool_is_active(pool_lv))
		return 1;

	flags |= LDLV_MODE_NO_SH;

	return lockd_lv_name(cmd, pool_lv->vg, pool_lv->name, &pool_lv->lvid.id[1],
			     pool_lv->lock_args, def_mode, flags);
}

/*
 * If the VG has no lock_type, then this function can return immediately.
 * The LV itself may have no lock (NULL lv->lock_type), but the lock request
 * may be directed to another lock, e.g. the pool LV lock in _lockd_lv_thin.
 * If the lock request is not directed to another LV, and the LV has no
 * lock_type set, it means that the LV has no lock, and no locking is done
 * for it.
 *
 * An LV lock is acquired before the LV is activated, and released
 * after the LV is deactivated.  If the LV lock cannot be acquired,
 * it means that the LV is active on another host and the activation
 * fails.  Commands that modify an inactive LV also acquire the LV lock.
 *
 * In non-lockd VGs, this is a no-op.
 *
 * In lockd VGs, normal LVs each have their own lock, but other
 * LVs do not have their own lock, e.g. the lock for a thin LV is
 * acquired on the thin pool LV, and a thin LV does not have a lock
 * of its own.  A cache pool LV does not have a lock of its own.
 * When the cache pool LV is linked to an origin LV, the lock of
 * the orgin LV protects the combined origin + cache pool.
 */

int lockd_lv(struct cmd_context *cmd, struct logical_volume *lv,
	     const char *def_mode, uint32_t flags)
{
	if (!is_lockd_type(lv->vg->lock_type))
		return 1;

	if (lv_is_thin_type(lv))
		return _lockd_lv_thin(cmd, lv, def_mode, flags);

	if (!is_lockd_type(lv->lock_type))
		return 1;

	/*
	 * LV type cannot be active concurrently on multiple hosts,
	 * so shared mode activation is not allowed.
	 */
	if (lv_is_external_origin(lv) ||
	    lv_is_thin_type(lv) ||
	    lv_is_mirror_type(lv) ||
	    lv_is_raid_type(lv) ||
	    lv_is_cache_type(lv)) {
		flags |= LDLV_MODE_NO_SH;
	}

	return lockd_lv_name(cmd, lv->vg, lv->name, &lv->lvid.id[1],
			     lv->lock_args, def_mode, flags);
}

static int _init_lv_sanlock(struct cmd_context *cmd, struct volume_group *vg,
			    const char *lv_name, struct id *lv_id,
			    const char **lock_args_ret)
{
	char lv_uuid[64] __attribute__((aligned(8)));
	daemon_reply reply;
	const char *reply_str;
	const char *lv_lock_args = NULL;
	int refreshed = 0;
	int extended = 0;
	int result;
	int ret;

	if (!_use_lvmlockd)
		return 1;
	if (!_lvmlockd_connected)
		return 0;

	id_write_format(lv_id, lv_uuid, sizeof(lv_uuid));
 retry:
	reply = _lockd_send("init_lv",
				"pid = %d", getpid(),
				"vg_name = %s", vg->name,
				"lv_name = %s", lv_name,
				"lv_uuid = %s", lv_uuid,
				"vg_lock_type = %s", "sanlock",
				"vg_lock_args = %s", vg->lock_args,
				NULL);

	if (!_lockd_result(reply, &result, NULL)) {
		ret = 0;
	} else {
		ret = (result < 0) ? 0 : 1;
	}

	if (result == -EEXIST) {
		log_error("Lock already exists for LV %s/%s", vg->name, lv_name);
		goto out;
	}

	if (result == -EMSGSIZE) {
		/*
		 * No space on the lvmlock lv for a new lease.
		 * Check if another host has extended lvmlock,
		 * and extend lvmlock if needed.
		 */
		if (!refreshed++) {
			log_debug("Refresh lvmlock");
			_refresh_sanlock_lv(cmd, vg);
			goto retry;
		}
		if (!extended++) {
			log_debug("Extend lvmlock");
			_extend_sanlock_lv(cmd, vg);
			goto retry;
		}
		goto out;
	}

	if (!ret) {
		log_error("_init_lv_sanlock lvmlockd result %d", result);
		goto out;
	}

	reply_str = daemon_reply_str(reply, "lv_lock_args", NULL);
	if (!reply_str) {
		log_error("lv_lock_args not returned");
		ret = 0;
		goto out;
	}

	lv_lock_args = dm_pool_strdup(cmd->mem, reply_str);
	if (!lv_lock_args) {
		log_error("lv_lock_args allocation failed");
		ret = 0;
	}
out:
	daemon_reply_destroy(reply);

	*lock_args_ret = lv_lock_args;
	return ret;
}

static int _free_lv_sanlock(struct cmd_context *cmd, struct volume_group *vg,
			    const char *lv_name, struct id *lv_id, const char *lock_args)
{
	char lv_uuid[64] __attribute__((aligned(8)));
	daemon_reply reply;
	int result;
	int ret;

	if (!_use_lvmlockd)
		return 1;
	if (!_lvmlockd_connected)
		return 0;

	id_write_format(lv_id, lv_uuid, sizeof(lv_uuid));

	reply = _lockd_send("free_lv",
				"pid = %d", getpid(),
				"vg_name = %s", vg->name,
				"lv_name = %s", lv_name,
				"lv_uuid = %s", lv_uuid,
				"vg_lock_type = %s", "sanlock",
				"vg_lock_args = %s", vg->lock_args,
				"lv_lock_args = %s", lock_args ?: "none",
				NULL);

	if (!_lockd_result(reply, &result, NULL)) {
		ret = 0;
	} else {
		ret = (result < 0) ? 0 : 1;
	}

	if (!ret) {
		log_error("_free_lv_sanlock lvmlockd result %d", result);
	}

	daemon_reply_destroy(reply);

	return ret;
}

int lockd_init_lv_args(struct cmd_context *cmd, struct volume_group *vg,
		       const char *lv_name, struct id *lv_id,
		       const char *lock_type, const char **lock_args)
{
	/* sanlock is the only lock type that sets per-LV lock_args. */
	if (!strcmp(lock_type, "sanlock"))
		return _init_lv_sanlock(cmd, vg, lv_name, lv_id, lock_args);
	return 1;
}

/*
 * lvcreate
 *
 * lvcreate sets lp lock_type to the vg lock_type, so any lv
 * created in a lockd vg will inherit the lock_type of the vg.
 * In some cases, e.g. thin lvs, this function may decide
 * that the lv should not be given a lock, in which case it
 * sets lp lock_type to NULL, which will cause the lv to not
 * have a lock_type set in its metadata.  A lockd_lv() request
 * on an lv with no lock_type will do nothing (unless the lv
 * type causes the lock request to be directed to another lv
 * with a lock, e.g. to the thin pool LV for thin LVs.)
 *
 * Current limitations:
 * - cache-type LV's in a lockd VG must be created with lvconvert.
 * - creating a thin pool and thin lv in one command is not allowed.
 */

int lockd_init_lv(struct cmd_context *cmd, struct volume_group *vg,
		  const char *lv_name, struct id *lv_id,
		  struct lvcreate_params *lp)
{
	const char *lv_name_lock;
	int lock_type_num = lock_type_to_num(lp->lock_type);

	if (cmd->lock_lv_mode && !strcmp(cmd->lock_lv_mode, "na"))
		return 1;

	switch (lock_type_num) {
	case LOCK_TYPE_NONE:
	case LOCK_TYPE_CLVM:
		return 1;
	case LOCK_TYPE_SANLOCK:
	case LOCK_TYPE_DLM:
		break;
	default:
		log_error("lockd_init_lv: unknown lock_type.");
		return 0;
	}

	if (seg_is_cache(lp) || seg_is_cache_pool(lp)) {
		log_error("Use lvconvert for cache with lock type %s", lp->lock_type);
		return 0;

	} else if (!seg_is_thin_volume(lp) && lp->snapshot) {
		struct logical_volume *origin_lv;

		/*
		 * COW snapshots are associated with their origin LV,
		 * and only the origin LV needs its own lock, which
		 * represents itself and all associated cow snapshots.
		 */

		if (!(origin_lv = find_lv(vg, lp->origin_name))) {
			log_error("Failed to find origin LV %s/%s", vg->name, lp->origin_name);
			return 0;
		}
		if (!lockd_lv(cmd, origin_lv, "ex", LDLV_PERSISTENT)) {
			log_error("Failed to lock origin LV %s/%s", vg->name, lp->origin_name);
			return 0;
		}
		lp->lock_type = NULL;
		return 1;

	} else if (seg_is_thin(lp)) {
		if ((seg_is_thin_volume(lp) && !lp->create_pool) ||
		    (!seg_is_thin_volume(lp) && lp->snapshot)) {
			struct lv_list *lvl;

			/*
			 * Creating a new thin lv or snapshot.  These lvs do not get
			 * their own lock but use the pool lock.  If an lv does not
			 * use its own lock, its lock_type is set to NULL.
			 */

			if (!(lvl = find_lv_in_vg(vg, lp->pool_name))) {
				log_error("Failed to find thin pool %s/%s", vg->name, lp->pool_name);
				return 0;
			}
			if (!lockd_lv(cmd, lvl->lv, "ex", LDLV_PERSISTENT)) {
				log_error("Failed to lock thin pool %s/%s", vg->name, lp->pool_name);
				return 0;
			}
			lp->lock_type = NULL;
			return 1;

		} else if (seg_is_thin_volume(lp) && lp->create_pool) {
			/*
			 * Creating a thin pool and a thin lv in it.  We could
			 * probably make this work by setting lp->lock_type and
			 * lp->lock_args to NULL in lv_create_single after
			 * creating the pool lv.  Then we would just set
			 * lv_name = lp->pool_name here.  Stop it at least for now
			 * to try to slow down some of the unnecessary complexity.
			 */
			log_error("Create thin pool and thin lv separately with lock type %s",
				  lp->lock_type);
			return 0;

		} else if (!seg_is_thin_volume(lp) && lp->create_pool) {
			/* Creating a thin pool only. */
			lv_name_lock = lp->pool_name;

		} else {
			log_error("Unknown thin options for lock init.");
			return 0;
		}

	} else {
		/* Creating a normal lv. */
		lv_name_lock = lv_name;
	}

	return lockd_init_lv_args(cmd, vg, lv_name_lock, lv_id,
				  lp->lock_type, &lp->lock_args);
}

/* lvremove */

int lockd_free_lv(struct cmd_context *cmd, struct volume_group *vg,
		  const char *lv_name, struct id *lv_id, const char *lock_args)
{
	if (cmd->lock_lv_mode && !strcmp(cmd->lock_lv_mode, "na"))
		return 1;

	switch (lock_type_to_num(vg->lock_type)) {
	case LOCK_TYPE_NONE:
	case LOCK_TYPE_CLVM:
	case LOCK_TYPE_DLM:
		return 1;
	case LOCK_TYPE_SANLOCK:
		return _free_lv_sanlock(cmd, vg, lv_name, lv_id, lock_args);
	default:
		log_error("lockd_free_lv: unknown lock_type.");
		return 0;
	}
}

int lockd_rename_vg_before(struct cmd_context *cmd, struct volume_group *vg)
{
	struct lv_list *lvl;
	daemon_reply reply;
	int result;
	int ret;

	if (!is_lockd_type(vg->lock_type))
		return 1;

	if (lvs_in_vg_activated(vg)) {
		log_error("LVs must be inactive before vgrename.");
		return 0;
	}

	/* Check that no LVs are active on other hosts. */

	dm_list_iterate_items(lvl, &vg->lvs) {
		if (!lockd_lv(cmd, lvl->lv, "ex", 0)) {
			log_error("LV %s/%s must be inactive on all hosts before vgrename.",
				  vg->name, lvl->lv->name);
			return 0;
		}

		if (!lockd_lv(cmd, lvl->lv, "un", 0)) {
			log_error("Failed to unlock LV %s/%s.", vg->name, lvl->lv->name);
			return 0;
		}
	}

	/*
	 * lvmlockd:
	 * checks for other hosts in lockspace
	 * leaves the lockspace
	 */

	reply = _lockd_send("rename_vg_before",
			"pid = %d", getpid(),
			"vg_name = %s", vg->name,
			"vg_lock_type = %s", vg->lock_type,
			"vg_lock_args = %s", vg->lock_args,
			NULL);

	if (!_lockd_result(reply, &result, NULL)) {
		ret = 0;
	} else {
		ret = (result < 0) ? 0 : 1;
	}

	daemon_reply_destroy(reply);
	
	if (!ret) {
		log_error("lockd_rename_vg_before lvmlockd result %d", result);
		return 0;
	}

	if (!strcmp(vg->lock_type, "sanlock")) {
		log_debug("lockd_rename_vg_before deactivate sanlock lv");
		_deactivate_sanlock_lv(cmd, vg);
	}

	return 1;
}

int lockd_rename_vg_final(struct cmd_context *cmd, struct volume_group *vg, int success)
{
	daemon_reply reply;
	int result;
	int ret;

	if (!is_lockd_type(vg->lock_type))
		return 1;

	if (!success) {
		/*
		 * Depending on the problem that caused the rename to
		 * fail, it may make sense to not restart the VG here.
		 */
		if (!lockd_start_vg(cmd, vg))
			log_error("Failed to restart VG %s lockspace.", vg->name);
		return 1;
	}

	if (!strcmp(vg->lock_type, "sanlock")) {
		if (!_activate_sanlock_lv(cmd, vg))
			return 0;

		/*
		 * lvmlockd needs to rewrite the leases on disk
		 * with the new VG (lockspace) name.
		 */
		reply = _lockd_send("rename_vg_final",
				"pid = %d", getpid(),
				"vg_name = %s", vg->name,
				"vg_lock_type = %s", vg->lock_type,
				"vg_lock_args = %s", vg->lock_args,
				NULL);

		if (!_lockd_result(reply, &result, NULL)) {
			ret = 0;
		} else {
			ret = (result < 0) ? 0 : 1;
		}
	
		daemon_reply_destroy(reply);

		if (!ret) {
			/*
			 * The VG has been renamed on disk, but renaming the
			 * sanlock leases failed.  Cleaning this up can
			 * probably be done by converting the VG to lock_type
			 * none, then converting back to sanlock.
			 */
			log_error("lockd_rename_vg_final lvmlockd result %d", result);
			return 0;
		}
	}

	if (!lockd_start_vg(cmd, vg))
		log_error("Failed to start VG %s lockspace.", vg->name);

	return 1;
}

const char *lockd_running_lock_type(struct cmd_context *cmd)
{
	daemon_reply reply;
	const char *lock_type = NULL;
	int result;

	if (!_use_lvmlockd)
		return NULL;
	if (!_lvmlockd_connected)
		return NULL;

	reply = _lockd_send("running_lm",
			"pid = %d", getpid(),
			NULL);

	if (!_lockd_result(reply, &result, NULL)) {
		log_error("Failed to get result from lvmlockd");
		goto out;
	}

	switch (result) {
	case -EXFULL:
		log_error("lvmlockd found multiple lock managers, use --lock-type to select one.");
		break;
	case -ENOLCK:
		log_error("lvmlockd found no lock manager running.");
		break;
	case LOCK_TYPE_SANLOCK:
		log_debug("lvmlockd found sanlock");
		lock_type = "sanlock";
		break;
	case LOCK_TYPE_DLM:
		log_debug("lvmlockd found dlm");
		lock_type = "dlm";
		break;
	default:
		log_error("Failed to find a running lock manager.");
		break;
	}
out:
	daemon_reply_destroy(reply);

	return lock_type;
}

