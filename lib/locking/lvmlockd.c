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

static void _result_str_to_flags(const char *str, uint32_t *flags)
{
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
		          const char *cmd_name,
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
	ret = _lockd_request(cmd, "vgremove", "lock_vg", vg->name,
			     NULL, NULL, NULL, NULL, "un", NULL,
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

	/* Added in a later patch. */
	/*
	if (cmd_mode && !strcmp(cmd_mode, "na"))
		return 1;
	*/

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

