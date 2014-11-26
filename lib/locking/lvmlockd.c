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

/* Translate the result strings from lvmlockd to bit flags. */
static void _result_str_to_flags(const char *str, uint32_t *flags)
{
	if (strstr(str, "NO_LOCKSPACES"))
		*flags |= LD_RF_NO_LOCKSPACES;

	if (strstr(str, "NO_GL_LS"))
		*flags |= LD_RF_NO_GL_LS;

	if (strstr(str, "LOCAL_LS"))
		*flags |= LD_RF_LOCAL_LS;

	if (strstr(str, "DUP_GL_LS"))
		*flags |= LD_RF_DUP_GL_LS;

	if (strstr(str, "INACTIVE_LS"))
		*flags |= LD_RF_INACTIVE_LS;

	if (strstr(str, "ADD_LS_ERROR"))
		*flags |= LD_RF_ADD_LS_ERROR;
}

/*
 * evaluate the reply from lvmlockd, check for errors, extract
 * the result and result_flags returned by lvmlockd.
 * 0 failure (no result/result_flags set)
 * 1 success (result/result_flags set)
 */

static int _lockd_result(daemon_reply reply, int *result, uint32_t *result_flags)
{
	int reply_result;
	const char *reply_flags;
	const char *lock_type;

	if (reply.error) {
		log_error("lockd_result reply error %d", reply.error);
		return 0;
	}

	if (strcmp(daemon_reply_str(reply, "response", ""), "OK")) {
		log_error("lockd_result bad response");
		return 0;
	}

	/* -1000 is a random number that we know is not returned. */

	reply_result = daemon_reply_int(reply, "op_result", -1000);
	if (reply_result == -1000) {
		log_error("lockd_result no op_result");
		return 0;
	}

	/* The lock_type that lvmlockd used for locking. */
	lock_type = daemon_reply_str(reply, "lock_type", "none");

	*result = reply_result;

	if (!result_flags)
		goto out;

	reply_flags = daemon_reply_str(reply, "result_flags", NULL);
	if (reply_flags)
		_result_str_to_flags(reply_flags, result_flags);

 out:
	log_debug("lockd_result %d %s lm %s", reply_result, reply_flags, lock_type);
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
 * result/result_flags are values returned from lvmlockd.
 *
 * return 0 (failure)
 * return 1 (result/result_flags indicate success/failure)
 *
 * return 1 result 0   (success)
 * return 1 result < 0 (failure)
 *
 * caller may ignore result < 0 failure depending on
 * result_flags and the specific command/mode.
 *
 * When this function returns 0 (failure), no result/result_flags
 * were obtained from lvmlockd.
 *
 * When this function returns 1 (success), result/result_flags may
 * have been obtained from lvmlockd.  This lvmlockd result may
 * indicate a locking failure.
 */

static int _lockd_request(struct cmd_context *cmd,
		          const char *req_name,
		          const char *vg_name,
		          const char *vg_lock_type,
		          const char *vg_lock_args,
		          const char *lv_name,
		          const char *lv_lock_args,
		          const char *mode,
		          const char *opts,
		          int *result,
		          uint32_t *result_flags)
{
	const char *cmd_name = "unknown"; /* FIXME: setting this would help debugging */
	daemon_reply reply;
	int pid = getpid();

	*result = 0;
	*result_flags = 0;

	if (!strcmp(mode, "na"))
		return 1;

	if (!_lvmlockd_active)
		return 1;
	if (!lvmlockd_connected())
		return 0;

	/* cmd and pid are passed for informational and debugging purposes */

	if (vg_name && lv_name) {
		reply = _lockd_send(req_name,
					"cmd = %s", cmd_name,
					"pid = %d", pid,
					"mode = %s", mode,
					"opts = %s", opts ?: "none",
					"vg_name = %s", vg_name,
					"lv_name = %s", lv_name,
					"vg_lock_type = %s", vg_lock_type ?: "none",
					"vg_lock_args = %s", vg_lock_args ?: "none",
					"lv_lock_args = %s", lv_lock_args ?: "none",
					NULL);

		if (!_lockd_result(reply, result, result_flags))
			goto fail;

		log_debug("lvmlockd %s %s vg %s lv %s result %d %x",
			  req_name, mode, vg_name, lv_name, *result, *result_flags);

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

		if (!_lockd_result(reply, result, result_flags))
			goto fail;

		log_debug("lvmlockd %s %s vg %s result %d %x",
			  req_name, mode, vg_name, *result, *result_flags);

	} else {
		reply = _lockd_send(req_name,
					"cmd = %s", cmd_name,
					"pid = %d", pid,
					"mode = %s", mode,
					"opts = %s", opts ?: "none",
					"vg_lock_type = %s", vg_lock_type ?: "none",
					NULL);

		if (!_lockd_result(reply, result, result_flags))
			goto fail;

		log_debug("lvmlockd %s %s result %d %x",
			  req_name, mode, *result, *result_flags);
	}

	daemon_reply_destroy(reply);

	/* result/result_flags have lvmlockd result */
	return 1;

 fail:
	/* no result was obtained from lvmlockd */

	log_error("lvmlockd %s %s failed no result", req_name, mode);

	daemon_reply_destroy(reply);
	return 0;
}

/* The name of the internal lv created to hold sanlock locks. */
#define LVMLOCKD_SANLOCK_LV_NAME "lvmlock"

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
		.extents = LVMLOCKD_SANLOCK_LV_SIZE / (vg->extent_size * SECTOR_SIZE),
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

	lv = _find_sanlock_lv(vg, lock_lv_name);
	if (!lv) {
		log_error("Failed to find sanlock LV %s in VG %s", lock_lv_name, vg->name);
		return 0;
	}

	if (!lv_remove(lv)) {
		log_error("Failed to remove sanlock LV %s/%s", vg->name, lock_lv_name);
		return 0;
	}

	return 1;
}

static int _activate_sanlock_lv(struct cmd_context *cmd, struct volume_group *vg)
{
	struct logical_volume *lv;
	const char *lock_lv_name = LVMLOCKD_SANLOCK_LV_NAME;

	lv = _find_sanlock_lv(vg, lock_lv_name);
	if (!lv) {
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

	lv = _find_sanlock_lv(vg, lock_lv_name);
	if (!lv) {
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

	if (!_lvmlockd_active)
		return 1;
	if (!lvmlockd_connected())
		return 0;

	reply = _lockd_send("init_vg",
				"pid = %d", getpid(),
				"vg_name = %s", vg->name,
				"vg_lock_type = %s", "dlm",
				NULL);

	if (!_lockd_result(reply, &result, NULL)) {
		ret = 0;
	} else {
		ret = (result < 0) ? 0 : 1;
	}

	if (!ret) {
		log_error("_init_vg_dlm lvmlockd result %d", result);
		goto out;
	}

	reply_str = daemon_reply_str(reply, "vg_lock_args", NULL);
	if (!reply_str) {
		log_error("vg_lock_args not returned");
		ret = 0;
		goto out;
	}

	vg_lock_args = dm_pool_strdup(cmd->mem, reply_str);
	if (!vg_lock_args) {
		log_error("vg_lock_args allocation failed");
		ret = 0;
	}
out:
	daemon_reply_destroy(reply);

	vg->lock_args = vg_lock_args;
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

	if (!_lvmlockd_active)
		return 1;
	if (!lvmlockd_connected())
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
	} else {
		ret = (result < 0) ? 0 : 1;
	}

	if (!ret) {
		log_error("_init_vg_sanlock lvmlockd result %d", result);
		_remove_sanlock_lv(cmd, vg, lock_lv_name);
		goto out;
	}

	reply_str = daemon_reply_str(reply, "vg_lock_args", NULL);
	if (!reply_str) {
		log_error("vg_lock_args not returned");
		ret = 0;
		goto out;
	}

	vg_lock_args = dm_pool_strdup(cmd->mem, reply_str);
	if (!vg_lock_args) {
		log_error("vg_lock_args allocation failed");
		ret = 0;
	}
out:
	daemon_reply_destroy(reply);

	vg->lock_args = vg_lock_args;
	return ret;
}

/* called after vg_remove on disk */

static int _free_vg_dlm(struct cmd_context *cmd, struct volume_group *vg)
{
	uint32_t result_flags;
	int result;
	int ret;

	/*
	 * Unlocking the vg lock here preempts the lvmlockd unlock in
	 * toollib.c which happens too late since the lockspace is
	 * left here.
	 */

	/* Equivalent to a standard unlock. */
	ret = _lockd_request(cmd, "lock_vg",
			     vg->name, NULL, NULL, NULL, NULL, "un", NULL,
			     &result, &result_flags);

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

	if (!_lvmlockd_active)
		return 1;
	if (!lvmlockd_connected())
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

	_deactivate_sanlock_lv(cmd, vg);

	_remove_sanlock_lv(cmd, vg, lock_lv_name);
 out:
	daemon_reply_destroy(reply);

	return ret;
}

/*
 * Called to remove lvmlockd's record of the local vg which it caches as an
 * optimization.
 */

static int _free_vg_local(struct cmd_context *cmd, struct volume_group *vg)
{
	daemon_reply reply;
	char uuid[64] __attribute__((aligned(8)));
	int result;
	int ret;

	memset(uuid, 0, sizeof(uuid));
	id_write_format(&vg->id, uuid, sizeof(uuid));

	reply = _lockd_send("rem_local",
				"pid = %d", getpid(),
				"vg_name = %s", vg->name,
				"vg_uuid = %s", uuid[0] ? uuid : "none",
				"vg_lock_type = %s", "none",
				NULL);

	if (!_lockd_result(reply, &result, NULL)) {
		ret = 0;
	} else {
		ret = (result < 0) ? 0 : 1;
	}
	
	if (!ret) {
		log_error("_free_vg_local lvmlockd result %d", result);
	}

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
		_free_vg_local(cmd, vg);
		break;
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
	const char *lock_type;
	int host_id = 0;
	int result;
	int ret;

	memset(uuid, 0, sizeof(uuid));

	/*
	 * We do not skip non-lockd vg's here (see add_local below).
	 * We use this to ensure lvmlockd has seen the local vg.
	 * It is an optimization in case lvmlockd has not seen the
	 * local vg yet.
	 */

	if (!_lvmlockd_active)
		return 1;
	if (!lvmlockd_connected())
		return 0;

	/* Skip starting the vg lockspace when the vg lock is skipped. */

	if (cmd->lock_vg_mode && !strcmp(cmd->lock_vg_mode, "na"))
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

	if (!is_lockd_type(vg->lock_type)) {
		char *sysid = NULL;

		if (vg->system_id && (strlen(vg->system_id) > 0))
			sysid = vg->system_id;

		reply = _lockd_send("add_local",
				"pid = %d", getpid(),
				"vg_name = %s", vg->name,
				"vg_uuid = %s", uuid[0] ? uuid : "none",
				"vg_sysid = %s", sysid ?: "none",
				"our_system_id = %s", cmd->system_id ?: "none",
				NULL);

		lock_type = "local";
	} else {
		reply = _lockd_send("start_vg",
				"pid = %d", getpid(),
				"vg_name = %s", vg->name,
				"vg_lock_type = %s", vg->lock_type,
				"vg_lock_args = %s", vg->lock_args,
				"vg_uuid = %s", uuid[0] ? uuid : "none",
				"version = %d", (int64_t)vg->seqno,
				"host_id = %d", host_id,
				NULL);

		lock_type = vg->lock_type;
	}

	if (!_lockd_result(reply, &result, NULL)) {
		result = -1;
		ret = 0;
	} else {
		ret = (result < 0) ? 0 : 1;
	}

	if (result == -EEXIST) {
		ret = 1;
		goto out;
	}

	if (!ret)
		log_error("Locking start %s VG %s %d", lock_type, vg->name, result);
	else
		log_debug("lockd_start_vg %s done", vg->name);

out:
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

	if (!_lvmlockd_active)
		return 1;
	if (!lvmlockd_connected())
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
		log_error("Cannot stop locking in busy VG %s", vg->name);
		goto out;
	}

	if (!ret) {
		log_error("Locking stop %s VG %s %d", vg->lock_type, vg->name, result);
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
 * lockd_gl_create() is used by vgcreate to acquire and/or create the
 * global lock.  vgcreate will have a lock_type for the new vg which
 * lockd_gl_create() can provide in the lock-gl call.
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
	uint32_t result_flags;
	int result;

	if (cmd->lock_gl_mode) {
		mode = cmd->lock_gl_mode;
		if (mode && def_mode && strcmp(mode, "enable") &&
		    (_mode_compare(mode, def_mode) < 0) &&
		    !find_config_tree_bool(cmd, global_allow_override_lock_modes_CFG, NULL)) {
			log_error("Disallowed lock-gl mode \"%s\"", mode);
			return 0;
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

	if (!_lockd_request(cmd, "lock_gl",
			      NULL, vg_lock_type, NULL, NULL, NULL, mode, "update_names",
			      &result, &result_flags)) {
		/* No result from lvmlockd, it is probably not running. */
		log_error("Locking failed for global lock");
		return 0;
	}

	/*
	 * result and result_flags were returned from lvmlockd.
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

		if ((result_flags & LD_RF_NO_GL_LS) &&
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

		if ((result_flags & LD_RF_NO_GL_LS) &&
		    (result_flags & LD_RF_NO_LOCKSPACES) &&
		    !strcmp(vg_lock_type, "sanlock")) {
			log_print_unless_silent("Enabling sanlock global lock");
			lvmetad_validate_global_cache(cmd, 1);
			return 1;
		}

		/*
		 * Allow non-lockd-type vgs to be created even when the global
		 * lock is not available.  Once created, these vgs will only be
		 * accessible to the local system_id, and not protected by
		 * locks, so allowing the creation without a lock is a very
		 * minor exception to normal locking.
		 */

		if ((result_flags & LD_RF_NO_GL_LS) &&
		    (!strcmp(vg_lock_type, "none"))) {
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

int lockd_gl(struct cmd_context *cmd, const char *def_mode, uint32_t flags)
{
	const char *mode = NULL;
	const char *opts = NULL;
	uint32_t result_flags;
	int result;

	if (!(flags & LDGL_MODE_NOARG) && cmd->lock_gl_mode) {
		mode = cmd->lock_gl_mode;
		if (mode && def_mode &&
		    (_mode_compare(mode, def_mode) < 0) &&
		    !find_config_tree_bool(cmd, global_allow_override_lock_modes_CFG, NULL)) {
			log_error("Disallowed lock-gl mode \"%s\"", mode);
			return 0;
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

	/*
	 * The lockd_gl() caller uses this flag when it is going to change the
	 * VG namesapce.  lvmlockd uses this to encode extra information in the
	 * global lock data (a separate version number in the lvb) about what
	 * was changed.  Other hosts will see this extra information in the gl
	 * data and know that the VG namespace changed, which determines the
	 * kind of cache refresh they need to do.
	 */
	if (flags & LDGL_UPDATE_NAMES)
		opts = "update_names";

	if (!_lockd_request(cmd, "lock_gl",
			    NULL, NULL, NULL, NULL, NULL, mode, opts,
			    &result, &result_flags)) {
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

	/*
	 * result and result_flags were returned from lvmlockd.
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

		if ((result_flags & LD_RF_NO_GL_LS) ||
		    (result_flags & LD_RF_NO_LOCKSPACES)) {
			log_warn("Skipping global lock: not found");
			lvmetad_validate_global_cache(cmd, 1);
			return 1;
		}

		log_error("Global lock %s error %d", mode, result);
		return 0;
	}

	if ((result_flags & LD_RF_DUP_GL_LS) && strcmp(mode, "un"))
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

int lockd_vg(struct cmd_context *cmd, const char *vg_name, const char *def_mode, uint32_t flags)
{
	const char *mode = NULL;
	uint32_t result_flags;
	int result;

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
	 */
	if (def_mode && !strcmp(def_mode, "un")) {
		if (cmd->lock_vg_mode && !strcmp(cmd->lock_vg_mode, "na"))
			return 1;
		mode = "un";
		goto req;
	}

	/*
	 * LDVG_MODE_NOARG disables getting the mode from --lock-vg arg.
	 */
	if (!(flags & LDVG_MODE_NOARG) && cmd->lock_vg_mode) {
		mode = cmd->lock_vg_mode;
		if (mode && def_mode &&
		    (_mode_compare(mode, def_mode) < 0) &&
		    !find_config_tree_bool(cmd, global_allow_override_lock_modes_CFG, NULL)) {
			log_error("Disallowed lock-vg mode \"%s\"", mode);
			return 0;
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
		log_error("Disallow lock-vg ex with read_only_lock_modes");
		return 0;
	}
 req:
	if (!_lockd_request(cmd, "lock_vg",
			      vg_name, NULL, NULL, NULL, NULL, mode, NULL,
			      &result, &result_flags)) {
		/* No result from lvmlockd, it is probably not running. */

		/*
		 * See comment in lockd_gl() about these cases
		 * where we keep going if the mode is "un" or "sh".
		 */

		if (!strcmp(mode, "un"))
			return 1;

		if (!strcmp(mode, "sh")) {
			log_warn("Reading VG %s without shared lock.", vg_name);
			return 1;
		}

		log_error("Locking failed for VG %s", vg_name);
		return 0;
	}

	/*
	 * result and result_flags were returned from lvmlockd.
	 *
	 * ENOLS: no lockspace for the VG was found, the VG may not
	 * be started yet.  The VG should be started manually or by system,
	 * e.g. vgchange --lock-start
	 *
	 * ESTARTING: the lockspace for the VG is starting and should
	 * finish shortly.
	 *
	 * ELOCALVG: the VG is local and does not need locking.
	 *
	 * EOTHERVG:
	 *
	 * The VG is local and its system_id does not match
	 * the local system_id saved in lvmlockd (lvmlockd
	 * caches the names/system_ids of local VGs so it
	 * can quickly avoid any distributed locking on them.)
	 *
	 * In many cases we could simply return 0 (a failure)
	 * here, causing this VG to be skipped by the command
	 * before it's even read.  But there are a couple of
	 * reasons why we want to return 1 (success) here and
	 * proceed through vg_read and get to access_vg_systemid:
	 *
	 * . The command may allow reading foreign VG's,
	 *   i.e. vgs --foreign, which would not work if we
	 *   skipped the VG here.
	 *
	 * . The local host may accept multiple system_ids,
	 *   i.e. allow_system_id(), and lvmlockd does not
	 *   know about all the allowed system_ids.
	 *
	 * If neither of these are true, then access_vg_systemid
	 * will deny access, and the VG will be skipped at that
	 * point.  So, to allow the two exceptions above, we
	 * return success here and allow VG access to be
	 * decided later in access_vg_systemid.
	 */
 
	if (result == -ELOCALVG || result == -EOTHERVG)
		return 1;
 
	if (result == -ENOLS || result == -ESTARTING) {
		if (!strcmp(mode, "un"))
			return 1;
 
		if (strcmp(mode, "sh")) {
			/*
			 * An ex lock request always fails here.  Based on the
			 * result number and result flags we can often print a
			 * reason for the failure.
			 *
			 * (In the future we might want to continue through
			 * vg_read without the lock, and add something after
			 * vg_read to check if the lock request failed, and
			 * fail at that point.  Going through the vg_read
			 * before failing may provide more information about
			 * the failure and the VG.) 
			 */
			if ((result == -ENOLS) && (result_flags & LD_RF_INACTIVE_LS)) {
				if (result_flags & LD_RF_ADD_LS_ERROR)
					log_error("VG %s lock failed: lock start error", vg_name);
				else
					log_error("VG %s lock failed: locking stopped", vg_name);
 
			} else if (result == -ENOLS) {
				log_error("VG %s lock failed: lock start required", vg_name);
 
			} else if (result == -ESTARTING) {
				log_error("VG %s lock failed: lock start in progress", vg_name);
 
			} else {
				log_error("VG %s lock failed: %d", vg_name, result);
			}
			return 0;
		}
 
		/*
		 * When a sh lock failed we will allow the command to proceed
		 * because there's little harm that it could do.  See the
		 * reasoning above for proceeding after a failed gl sh lock.
		 * Do we want to invalidate the cached VG in these cases to
		 * force rereading from disk?
		 */
 
		if (result == -ESTARTING) {
			log_warn("Skipping lock for VG %s: lock start in progress", vg_name);
			return 1;
		}
 
		if ((result == -ENOLS) && (result_flags & LD_RF_ADD_LS_ERROR)) {
			log_warn("Skipping lock for VG %s: lock start error", vg_name);
			return 1;
		}
 
		if ((result == -ENOLS) && (result_flags & LD_RF_INACTIVE_LS)) {
			log_warn("Skipping lock for VG %s: locking stopped", vg_name);
			return 1;
		}
 
		if (result == -ENOLS) {
			log_warn("Skipping lock for VG %s: lock start required", vg_name);
			return 1;
		}
 
		log_error("VG %s shared lock error %d", vg_name, result);
		return 0;
	}
 
	/*
	 * A notice from lvmlockd that duplicate gl locks have been found.
	 * It would be good for the user to disable one of them.
	 */
	if ((result_flags & LD_RF_DUP_GL_LS) && strcmp(mode, "un"))
		log_warn("Duplicate sanlock global lock in VG %s", vg_name);
 
	if (result < 0) {
		if (ignorelockingfailure()) {
			log_debug("Ignore failed locking for VG %s", vg_name);
			return 1;
		} else {
			log_error("VG %s lock error: %d", vg_name, result);
			return 0;
		}
	}
 
	return 1;
}

/* A shortcut for back to back lockd_gl() + lockd_vg() */

int lockd_gl_vg(struct cmd_context *cmd, const char *vg_name,
		const char *def_gl_mode, const char *def_vg_mode,
		uint32_t flags)
{
	if (!lockd_gl(cmd, def_gl_mode, flags))
		return 0;

	if (!lockd_vg(cmd, vg_name, def_vg_mode, flags)) {
		lockd_gl(cmd, "un", LDGL_MODE_NOARG);
		return 0;
	}

	return 1;
}

int lockd_vg_update(struct volume_group *vg)
{
	daemon_reply reply;
	int result;
	int ret;

	if (!is_lockd_type(vg->lock_type))
		return 1;

	if (!_lvmlockd_active)
		return 1;
	if (!lvmlockd_connected())
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
		  const char *lv_name, const char *lock_args,
		  const char *def_mode, uint32_t flags)
{
	const char *mode = NULL;
	const char *opts = NULL;
	uint32_t result_flags;
	int result;

	if (cmd->lockd_lv_disable)
		return 1;

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

	if (cmd->lock_lv_mode && (_mode_compare(cmd->lock_lv_mode, "sh") < 0) &&
	    !find_config_tree_bool(cmd, global_allow_override_lock_modes_CFG, NULL)) {
		log_error("Disallowed lock-lv mode \"%s\"", cmd->lock_lv_mode);
		return 0;
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

	if (!_lockd_request(cmd, "lock_lv",
			       vg->name, vg->lock_type, vg->lock_args,
			       lv_name, lock_args, mode, opts,
			       &result, &result_flags)) {
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

	return lockd_lv_name(cmd, pool_lv->vg, pool_lv->name, pool_lv->lock_args,
			     def_mode, flags);
}

/*
 * If the VG has no lock_type, then this function can return immediately.
 * The LV itself may have no lock (NULL lv->lock_type), but the lock request
 * may be directed to another lock, e.g. the pool LV lock in _lockd_lv_thin.
 * If the lock request is not directed to another LV, and the LV has no
 * lock_type set, it means that the LV has no lock, and no locking is done
 * for it.
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

	return lockd_lv_name(cmd, lv->vg, lv->name, lv->lock_args, def_mode, flags);
}

static int _init_lv_sanlock(struct cmd_context *cmd, struct volume_group *vg,
			    const char *lv_name, const char **lock_args_ret)
{
	daemon_reply reply;
	const char *reply_str;
	const char *lv_lock_args = NULL;
	int result;
	int ret;

	if (!_lvmlockd_active)
		return 1;
	if (!lvmlockd_connected())
		return 0;

	reply = _lockd_send("init_lv",
				"pid = %d", getpid(),
				"vg_name = %s", vg->name,
				"lv_name = %s", lv_name,
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
			    const char *lv_name, const char *lock_args)
{
	daemon_reply reply;
	int result;
	int ret;

	if (!_lvmlockd_active)
		return 1;
	if (!lvmlockd_connected())
		return 0;

	reply = _lockd_send("free_lv",
				"pid = %d", getpid(),
				"vg_name = %s", vg->name,
				"lv_name = %s", lv_name,
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
		     struct lvcreate_params *lp)
{
	const char *lv_name;
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
			lv_name = lp->pool_name;

		} else {
			log_error("Unknown thin options for lock init.");
			return 0;
		}

	} else {
		/* Creating a normal lv. */
		lv_name = lp->lv_name;
	}

	if (lock_type_num == LOCK_TYPE_SANLOCK)
		return _init_lv_sanlock(cmd, vg, lv_name, &lp->lock_args);

	return 1;
}

/* lvremove */

int lockd_free_lv(struct cmd_context *cmd, struct volume_group *vg,
		  const char *lv_name, const char *lock_args)
{
	if (cmd->lock_lv_mode && !strcmp(cmd->lock_lv_mode, "na"))
		return 1;

	switch (lock_type_to_num(vg->lock_type)) {
	case LOCK_TYPE_NONE:
	case LOCK_TYPE_CLVM:
	case LOCK_TYPE_DLM:
		return 1;
	case LOCK_TYPE_SANLOCK:
		return _free_lv_sanlock(cmd, vg, lv_name, lock_args);
	default:
		log_error("lockd_free_lv: unknown lock_type.");
		return 0;
	}
}

