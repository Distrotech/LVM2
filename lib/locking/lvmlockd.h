/*
 * Copyright (C) 2014 Red Hat, Inc.
 *
 * This file is part of LVM2.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License v.2.1.
 */

#ifndef _LVMLOCKD_H
#define _LVMLOCKD_H

#include "config-util.h"
#include "daemon-client.h"

#define LOCK_TYPE_NONE    0
#define LOCK_TYPE_CLVM    1
#define LOCK_TYPE_DLM     2
#define LOCK_TYPE_SANLOCK 3

/* lockd_gl flags */
#define LDGL_MODE_NOARG           0x00000001
#define LDGL_SKIP_CACHE_VALIDATE  0x00000002
#define LDGL_UPDATE_NAMES         0x00000004

/* lockd_vg flags */
#define LDVG_MODE_NOARG           0x00000001

/* lockd_lv flags */
#define LDLV_MODE_NOARG           0x00000001
#define LDLV_MODE_NO_SH           0x00000002
#define LDLV_PERSISTENT           0x00000004

/* lvmlockd result flags */
#define LD_RF_NO_LOCKSPACES     0x00000001
#define LD_RF_NO_GL_LS          0x00000002
#define LD_RF_LOCAL_LS          0x00000004
#define LD_RF_DUP_GL_LS         0x00000008
#define LD_RF_INACTIVE_LS       0x00000010
#define LD_RF_ADD_LS_ERROR      0x00000020

/* lockd_state flags */
#define LDST_EX			0x00000001
#define LDST_SH			0x00000002
#define LDST_FAIL_REQUEST	0x00000004
#define LDST_FAIL_NOLS		0x00000008
#define LDST_FAIL_STARTING	0x00000010
#define LDST_FAIL_OTHER		0x00000020
#define LDST_FAIL		(LDST_FAIL_REQUEST | LDST_FAIL_NOLS | LDST_FAIL_STARTING | LDST_FAIL_OTHER)

/*
 * lock_type    lock_type_num
 * "none"    -> LOCK_TYPE_NONE
 * "clvm"    -> LOCK_TYPE_CLVM
 * "dlm      -> LOCK_TYPE_DLM
 * "sanlock" -> LOCK_TYPE_SANLOCK
 */

static inline int lock_type_to_num(const char *lock_type)
{
	if (!lock_type)
		return LOCK_TYPE_NONE;
	if (!strcmp(lock_type, "none"))
		return LOCK_TYPE_NONE;
	if (!strcmp(lock_type, "clvm"))
		return LOCK_TYPE_CLVM;
	if (!strcmp(lock_type, "dlm"))
		return LOCK_TYPE_DLM;
	if (!strcmp(lock_type, "sanlock"))
		return LOCK_TYPE_SANLOCK;
	return -1;
}

/*
 * Check if a lock_type uses lvmlockd.
 * If not (none, clvm), return 0.
 * If so (dlm, sanlock), return > 0 (LOCK_TYPE_)
 */

static inline int is_lockd_type(const char *lock_type)
{
	if (!lock_type)
		return 0;

	if (!strcmp(lock_type, "dlm"))
		return LOCK_TYPE_DLM;
	if (!strcmp(lock_type, "sanlock"))
		return LOCK_TYPE_SANLOCK;

	return 0;
}

#ifdef LVMLOCKD_SUPPORT

/* lvmlockd connection and communication */

void lvmlockd_init(struct cmd_context *);
void lvmlockd_set_active(int);
void lvmlockd_set_socket(const char *);
void lvmlockd_disconnect(void);
void lvmlockd_connect_or_warn(void);
int lvmlockd_connected(void);
int lvmlockd_active(void);

/* vgcreate/vgremove use init/free */

int lockd_init_vg(struct cmd_context *cmd, struct volume_group *vg);
int lockd_free_vg_before(struct cmd_context *cmd, struct volume_group *vg);
void lockd_free_vg_final(struct cmd_context *cmd, struct volume_group *vg);

/* vgrename */

int lockd_rename_vg_before(struct cmd_context *cmd, struct volume_group *vg);
int lockd_rename_vg_final(struct cmd_context *cmd, struct volume_group *vg, int success);

/* start and stop the lockspace for a vg */

int lockd_start_vg(struct cmd_context *cmd, struct volume_group *vg);
int lockd_stop_vg(struct cmd_context *cmd, struct volume_group *vg);
int lockd_start_wait(struct cmd_context *cmd);

/* locking */

int lockd_gl_create(struct cmd_context *cmd, const char *def_mode, const char *vg_lock_type);
int lockd_gl(struct cmd_context *cmd, const char *def_mode, uint32_t flags);
int lockd_vg(struct cmd_context *cmd, const char *vg_name, const char *def_mode,
	     uint32_t flags, uint32_t *lockd_state);
int lockd_vg_update(struct volume_group *vg);

int lockd_lv_name(struct cmd_context *cmd, struct volume_group *vg,
		  const char *lv_name, const char *lock_args,
		  const char *def_mode, uint32_t flags);
int lockd_lv(struct cmd_context *cmd, struct logical_volume *lv,
	     const char *def_mode, uint32_t flags);

/* lvcreate/lvremove use init/free */

int lockd_init_lv(struct cmd_context *cmd, struct volume_group *vg,
		  const char *lv_name, struct lvcreate_params *lp);
int lockd_free_lv(struct cmd_context *cmd, struct volume_group *vg,
		  const char *lv_name, const char *lock_args);

int lockd_init_lv_args(struct cmd_context *cmd, struct volume_group *vg,
		       const char *lv_name, const char *lock_type, const char **lock_args);

const char *lockd_running_lock_type(struct cmd_context *cmd);

#else /* LVMLOCKD_SUPPORT */

#define lvmlockd_init(cmd)          do { } while (0)
#define lvmlockd_set_active(int)    do { } while (0)
#define lvmlockd_set_socket(str)    do { } while (0)
#define lvmlockd_disconnect()       do { } while (0)
#define lvmlockd_connect_or_warn()  do { } while (0)

static inline int lvmlockd_connected(void)
{
	return 0;
}

static inline int lvmlockd_active(void)
{
	return 0;
}

static inline int lockd_init_vg(struct cmd_context *cmd, struct volume_group *vg)
{
	return 1;
}

static inline int lockd_free_vg_before(struct cmd_context *cmd, struct volume_group *vg)
{
	return 1;
}

static inline void lockd_free_vg_final(struct cmd_context *cmd, struct volume_group *vg)
{
	return;
}

static inline int lockd_rename_vg_before(struct cmd_context *cmd, struct volume_group *vg)
{
	return 1;
}

static inline int lockd_rename_vg_final(struct cmd_context *cmd, struct volume_group *vg, int success)
{
	return 1;
}

static inline int lockd_start_vg(struct cmd_context *cmd, struct volume_group *vg)
{
	return 0;
}

static inline int lockd_stop_vg(struct cmd_context *cmd, struct volume_group *vg)
{
	return 0;
}

static inline int lockd_start_wait(struct cmd_context *cmd)
{
	return 0;
}

static inline int lockd_gl_create(struct cmd_context *cmd, const char *def_mode, const char *vg_lock_type)
{
	return 1;
}

static inline int lockd_gl(struct cmd_context *cmd, const char *def_mode, uint32_t flags)
{
	return 1;
}

static inline int lockd_vg(struct cmd_context *cmd, const char *vg_name, const char *def_mode,
	     uint32_t flags, uint32_t *lockd_state)
{
	return 1;
}

static inline int lockd_vg_update(struct volume_group *vg)
{
	return 1;
}

static inline int lockd_lv_name(struct cmd_context *cmd, struct volume_group *vg,
		  const char *lv_name, const char *lock_args,
		  const char *def_mode, uint32_t flags)
{
	return 1;
}

static inline int lockd_lv(struct cmd_context *cmd, struct logical_volume *lv,
	     const char *def_mode, uint32_t flags)
{
	return 1;
}

static inline int lockd_init_lv(struct cmd_context *cmd, struct volume_group *vg,
		  const char *lv_name, struct lvcreate_params *lp)
{
	return 0;
}

static inline int lockd_free_lv(struct cmd_context *cmd, struct volume_group *vg,
		  const char *lv_name, const char *lock_args)
{
	return 0;
}

static inline int lockd_init_lv_args(struct cmd_context *cmd, struct volume_group *vg,
		       const char *lv_name, const char *lock_type, const char **lock_args)
{
	return 0;
}

static inline const char *lockd_running_lock_type(struct cmd_context *cmd)
{
	return NULL;
}

#endif /* LVMLOCKD_SUPPORT */

#endif

