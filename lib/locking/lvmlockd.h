/*
 * Copyright (C) 2013 Red Hat, Inc.
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

/* daemon management */

void lvmlockd_init(struct cmd_context *);
void lvmlockd_set_active(int);
void lvmlockd_set_socket(const char *);
void lvmlockd_disconnect(void);
void lvmlockd_connect_or_warn(void);
int lvmlockd_connected(void);

#else /* LVMLOCKD_SUPPORT */

#define lvmlockd_init(cmd)          do { } while (0)
#define lvmlockd_set_active(int)    do { } while (0)
#define lvmlockd_set_socket(str)    do { } while (0)
#define lvmlockd_disconnect()       do { } while (0)
#define lvmlockd_connect_or_warn()  do { } while (0)
#define lvmlockd_connected          (0)

#endif /* LVMLOCKD_SUPPORT */

#endif

