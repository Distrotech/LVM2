/*
 * Copyright (C) 2015 Red Hat, Inc.
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

#include <unistd.h>

#include "libdevmapper.h"
#include "lvmpolld-cmd-utils.h"
#include "lvmpolld-protocol.h"

/* extract this info from autoconf/automake files */
#define LVPOLL_CMD "lvpoll"

extern char **environ;

static const char *const const polling_ops[] = { [PVMOVE] = LVMPD_REQ_PVMOVE,
						 [CONVERT] = LVMPD_REQ_CONVERT,
						 [MERGE] = LVMPD_REQ_MERGE,
						 [MERGE_THIN] = LVMPD_REQ_MERGE_THIN };

static int add_to_cmd_arr(const char ***cmdargv, const char *str, unsigned *index, unsigned renameme)
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

const char **cmdargv_ctr(const lvmpolld_lv_t *pdlv, const char *lvm_binary, unsigned abort, unsigned handle_missing_pvs)
{
	unsigned i = 0;
	const char **cmd_argv = dm_malloc(MIN_ARGV_SIZE * sizeof(char *));

	if (!cmd_argv)
		return NULL;

	/* path to lvm2 binary */
	if (!add_to_cmd_arr(&cmd_argv, lvm_binary, &i, MIN_ARGV_SIZE))
		goto err;

	/* cmd to execute */
	if (!add_to_cmd_arr(&cmd_argv, LVPOLL_CMD, &i, MIN_ARGV_SIZE))
		goto err;

	/* transfer internal polling interval */
	if (pdlv->sinterval &&
	    (!add_to_cmd_arr(&cmd_argv, "--interval", &i, MIN_ARGV_SIZE) ||
	     !add_to_cmd_arr(&cmd_argv, pdlv->sinterval, &i, MIN_ARGV_SIZE)))
		goto err;

	/* pass abort param */
	if (abort &&
	    !add_to_cmd_arr(&cmd_argv, "--abort", &i, MIN_ARGV_SIZE))
		goto err;

	/* pass handle-missing-pvs. used by mirror polling operation */
	if (handle_missing_pvs &&
	    !add_to_cmd_arr(&cmd_argv, "--handle-missing-pvs", &i, MIN_ARGV_SIZE))
		goto err;

	/* one of: "convert", "pvmove", "merge", "merge_thin" */
	if (!add_to_cmd_arr(&cmd_argv, "--poll-operation", &i, MIN_ARGV_SIZE) ||
	    !add_to_cmd_arr(&cmd_argv, polling_ops[pdlv->type], &i, MIN_ARGV_SIZE))
		goto err;

	/* vg/lv name */
	if (!add_to_cmd_arr(&cmd_argv, pdlv->lvname, &i, MIN_ARGV_SIZE))
		goto err;

	/* terminating NULL */
	if (!add_to_cmd_arr(&cmd_argv, NULL, &i, MIN_ARGV_SIZE))
		goto err;

	return cmd_argv;
err:
	dm_free(cmd_argv);
	return NULL;
}

/* FIXME: in fact exclude should be va list */
static int copy_env(const char ***cmd_envp, unsigned *i, unsigned renameme, const char *exclude)
{
	const char * const* tmp = environ;

	if (!tmp)
		return 0;

	while (*tmp) {
		if (strncmp(*tmp, exclude, strlen(exclude)) && !add_to_cmd_arr(cmd_envp, *tmp, i, renameme))
			return 0;
		tmp++;
	}

	return 1;
}

const char **cmdenvp_ctr(const lvmpolld_lv_t *pdlv)
{
	unsigned i = 0;
	const char **cmd_envp = dm_malloc(MIN_ARGV_SIZE * sizeof(char *));

	if (!cmd_envp)
		return NULL;

	/* copy whole environment from lvmpolld, exclude LVM_SYSTEM_DIR if set */
	if (!copy_env(&cmd_envp, &i, MIN_ARGV_SIZE, "LVM_SYSTEM_DIR="))
		goto err;

	/* Add per client LVM_SYSTEM_DIR variable if set */
	if (*pdlv->lvm_system_dir_env && !add_to_cmd_arr(&cmd_envp, pdlv->lvm_system_dir_env, &i, MIN_ARGV_SIZE))
		goto err;

	/* terminating NULL */
	if (!add_to_cmd_arr(&cmd_envp, NULL, &i, MIN_ARGV_SIZE))
		goto err;

	return cmd_envp;
err:
	dm_free(cmd_envp);
	return NULL;
}
