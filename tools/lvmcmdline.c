/*
 * Copyright (C) 2001-2004 Sistina Software, Inc. All rights reserved.   
 * Copyright (C) 2004 Red Hat, Inc. All rights reserved.
 *
 * This file is part of LVM2.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU General Public License v.2.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

#include "tools.h"
#include "lvm2cmdline.h"
#include "label.h"
#include "version.h"

#include "stub.h"
#include "lvm2cmd.h"

#include <signal.h>
#include <syslog.h>
#include <libgen.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/resource.h>

#ifdef HAVE_GETOPTLONG
#  include <getopt.h>
#  define GETOPTLONG_FN(a, b, c, d, e) getopt_long((a), (b), (c), (d), (e))
#  define OPTIND_INIT 0
#else
struct option {
};
extern int optind;
extern char *optarg;
#  define GETOPTLONG_FN(a, b, c, d, e) getopt((a), (b), (c))
#  define OPTIND_INIT 1
#endif

#ifdef READLINE_SUPPORT
#  include <readline/readline.h>
#  include <readline/history.h>
#  ifndef HAVE_RL_COMPLETION_MATCHES
#    define rl_completion_matches(a, b) completion_matches((char *)a, b)
#  endif
#endif

/*
 * Exported table of valid switches
 */
struct arg the_args[ARG_COUNT + 1] = {

#define arg(a, b, c, d) {b, "", "--" c, d, 0, NULL, 0, 0, INT64_C(0), UINT64_C(0), 0, NULL},
#include "args.h"
#undef arg

};

static int _array_size;
static int _num_commands;
static struct command *_commands;

static int _interactive;

int yes_no_arg(struct cmd_context *cmd __attribute((unused)), struct arg *a)
{
	a->sign = SIGN_NONE;

	if (!strcmp(a->value, "y")) {
		a->i_value = 1;
		a->ui_value = 1;
	}

	else if (!strcmp(a->value, "n")) {
		a->i_value = 0;
		a->ui_value = 0;
	}

	else
		return 0;

	return 1;
}

int yes_no_excl_arg(struct cmd_context *cmd __attribute((unused)),
		    struct arg *a)
{
	a->sign = SIGN_NONE;

	if (!strcmp(a->value, "e") || !strcmp(a->value, "ey") ||
	    !strcmp(a->value, "ye")) {
		a->i_value = CHANGE_AE;
		a->ui_value = CHANGE_AE;
	}

	else if (!strcmp(a->value, "y")) {
		a->i_value = CHANGE_AY;
		a->ui_value = CHANGE_AY;
	}

	else if (!strcmp(a->value, "n") || !strcmp(a->value, "en") ||
		 !strcmp(a->value, "ne")) {
		a->i_value = CHANGE_AN;
		a->ui_value = CHANGE_AN;
	}

	else if (!strcmp(a->value, "ln") || !strcmp(a->value, "nl")) {
		a->i_value = CHANGE_ALN;
		a->ui_value = CHANGE_ALN;
	}

	else if (!strcmp(a->value, "ly") || !strcmp(a->value, "yl")) {
		a->i_value = CHANGE_ALY;
		a->ui_value = CHANGE_ALY;
	}

	else
		return 0;

	return 1;
}

int metadatatype_arg(struct cmd_context *cmd, struct arg *a)
{
	struct format_type *fmt;
	char *format;

	format = a->value;

	list_iterate_items(fmt, &cmd->formats) {
		if (!strcasecmp(fmt->name, format) ||
		    !strcasecmp(fmt->name + 3, format) ||
		    (fmt->alias && !strcasecmp(fmt->alias, format))) {
			a->ptr = fmt;
			return 1;
		}
	}

	return 0;
}

static int _get_int_arg(struct arg *a, char **ptr)
{
	char *val;
	long v;

	val = a->value;
	switch (*val) {
	case '+':
		a->sign = SIGN_PLUS;
		val++;
		break;
	case '-':
		a->sign = SIGN_MINUS;
		val++;
		break;
	default:
		a->sign = SIGN_NONE;
	}

	if (!isdigit(*val))
		return 0;

	v = strtol(val, ptr, 10);

	if (*ptr == val)
		return 0;

	a->i_value = (int32_t) v;
	a->ui_value = (uint32_t) v;
	a->i64_value = (int64_t) v;
	a->ui64_value = (uint64_t) v;

	return 1;
}

static int _size_arg(struct cmd_context *cmd __attribute((unused)), struct arg *a, int factor)
{
	char *ptr;
	int i;
	static const char *suffixes = "kmgt";
	char *val;
	double v;

	val = a->value;
	switch (*val) {
	case '+':
		a->sign = SIGN_PLUS;
		val++;
		break;
	case '-':
		a->sign = SIGN_MINUS;
		val++;
		break;
	default:
		a->sign = SIGN_NONE;
	}

	if (!isdigit(*val))
		return 0;

	v = strtod(val, &ptr);

	if (ptr == val)
		return 0;

	if (*ptr) {
		for (i = strlen(suffixes) - 1; i >= 0; i--)
			if (suffixes[i] == tolower((int) *ptr))
				break;

		if (i < 0)
			return 0;

		while (i-- > 0)
			v *= 1024;
	} else
		v *= factor;

	a->i_value = (int32_t) v;
	a->ui_value = (uint32_t) v;
	a->i64_value = (int64_t) v;
	a->ui64_value = (uint64_t) v;

	return 1;
}

int size_kb_arg(struct cmd_context *cmd, struct arg *a)
{
	return _size_arg(cmd, a, 1);
}

int size_mb_arg(struct cmd_context *cmd, struct arg *a)
{
	return _size_arg(cmd, a, 1024);
}

int int_arg(struct cmd_context *cmd __attribute((unused)), struct arg *a)
{
	char *ptr;

	if (!_get_int_arg(a, &ptr) || (*ptr) || (a->sign == SIGN_MINUS))
		return 0;

	return 1;
}

int int_arg_with_sign(struct cmd_context *cmd __attribute((unused)), struct arg *a)
{
	char *ptr;

	if (!_get_int_arg(a, &ptr) || (*ptr))
		return 0;

	return 1;
}

int minor_arg(struct cmd_context *cmd __attribute((unused)), struct arg *a)
{
	char *ptr;

	if (!_get_int_arg(a, &ptr) || (*ptr) || (a->sign == SIGN_MINUS))
		return 0;

	if (a->i_value > 255) {
		log_error("Minor number outside range 0-255");
		return 0;
	}

	return 1;
}

int major_arg(struct cmd_context *cmd __attribute((unused)), struct arg *a)
{
	char *ptr;

	if (!_get_int_arg(a, &ptr) || (*ptr) || (a->sign == SIGN_MINUS))
		return 0;

	if (a->i_value > 255) {
		log_error("Major number outside range 0-255");
		return 0;
	}

	/* FIXME Also Check against /proc/devices */

	return 1;
}

int string_arg(struct cmd_context *cmd __attribute((unused)),
	       struct arg *a __attribute((unused)))
{
	return 1;
}

int tag_arg(struct cmd_context *cmd __attribute((unused)), struct arg *a)
{
	char *pos = a->value;

	if (*pos == '@')
		pos++;

	if (!validate_name(pos))
		return 0;

	return 1;
}

int permission_arg(struct cmd_context *cmd __attribute((unused)), struct arg *a)
{
	a->sign = SIGN_NONE;

	if ((!strcmp(a->value, "rw")) || (!strcmp(a->value, "wr")))
		a->ui_value = LVM_READ | LVM_WRITE;

	else if (!strcmp(a->value, "r"))
		a->ui_value = LVM_READ;

	else
		return 0;

	return 1;
}

int alloc_arg(struct cmd_context *cmd __attribute((unused)), struct arg *a)
{
	alloc_policy_t alloc;

	a->sign = SIGN_NONE;

	alloc = get_alloc_from_string(a->value);
	if (alloc == ALLOC_INVALID)
		return 0;

	a->ui_value = (uint32_t) alloc;

	return 1;
}

int segtype_arg(struct cmd_context *cmd, struct arg *a)
{
	if (!(a->ptr = (void *) get_segtype_from_string(cmd, a->value)))
		return 0;

	return 1;
}

char yes_no_prompt(const char *prompt, ...)
{
	int c = 0, ret = 0;
	va_list ap;

	do {
		if (c == '\n' || !c) {
			va_start(ap, prompt);
			vprintf(prompt, ap);
			va_end(ap);
		}

		if ((c = getchar()) == EOF) {
			ret = 'n';
			break;
		}

		c = tolower(c);
		if ((c == 'y') || (c == 'n'))
			ret = c;
	} while (!ret || c != '\n');

	if (c != '\n')
		printf("\n");

	return ret;
}

static void __alloc(int size)
{
	if (!(_commands = dm_realloc(_commands, sizeof(*_commands) * size))) {
		log_fatal("Couldn't allocate memory.");
		exit(ECMD_FAILED);
	}

	_array_size = size;
}

static void _alloc_command(void)
{
	if (!_array_size)
		__alloc(32);

	if (_array_size <= _num_commands)
		__alloc(2 * _array_size);
}

static void _create_new_command(const char *name, command_fn command,
				const char *desc, const char *usagestr,
				int nargs, int *args)
{
	struct command *nc;

	_alloc_command();

	nc = _commands + _num_commands++;

	nc->name = name;
	nc->desc = desc;
	nc->usage = usagestr;
	nc->fn = command;
	nc->num_args = nargs;
	nc->valid_args = args;
}

static void _register_command(const char *name, command_fn fn,
			      const char *desc, const char *usagestr, ...)
{
	int nargs = 0, i;
	int *args;
	va_list ap;

	/* count how many arguments we have */
	va_start(ap, usagestr);
	while (va_arg(ap, int) >= 0)
		 nargs++;
	va_end(ap);

	/* allocate space for them */
	if (!(args = dm_malloc(sizeof(*args) * nargs))) {
		log_fatal("Out of memory.");
		exit(ECMD_FAILED);
	}

	/* fill them in */
	va_start(ap, usagestr);
	for (i = 0; i < nargs; i++)
		args[i] = va_arg(ap, int);
	va_end(ap);

	/* enter the command in the register */
	_create_new_command(name, fn, desc, usagestr, nargs, args);
}

static void _register_commands()
{
#define xx(a, b, c...) _register_command(# a, a, b, ## c, \
					driverloaded_ARG, \
					debug_ARG, help_ARG, help2_ARG, \
					version_ARG, verbose_ARG, \
					quiet_ARG, config_ARG, -1);
#include "commands.h"
#undef xx
}

static struct command *_find_command(const char *name)
{
	int i;
	char *namebase, *base;

	namebase = strdup(name);
	base = basename(namebase);

	for (i = 0; i < _num_commands; i++) {
		if (!strcmp(base, _commands[i].name))
			break;
	}

	free(namebase);

	if (i >= _num_commands)
		return 0;

	return _commands + i;
}

static void _usage(const char *name)
{
	struct command *com = _find_command(name);

	if (!com)
		return;

	log_error("%s: %s\n\n%s", com->name, com->desc, com->usage);
}

/*
 * Sets up the short and long argument.  If there
 * is no short argument then the index of the
 * argument in the the_args array is set as the
 * long opt value.  Yuck.  Of course this means we
 * can't have more than 'a' long arguments.
 */
static void _add_getopt_arg(int arg, char **ptr, struct option **o)
{
	struct arg *a = the_args + arg;

	if (a->short_arg) {
		*(*ptr)++ = a->short_arg;

		if (a->fn)
			*(*ptr)++ = ':';
	}
#ifdef HAVE_GETOPTLONG
	if (*(a->long_arg + 2)) {
		(*o)->name = a->long_arg + 2;
		(*o)->has_arg = a->fn ? 1 : 0;
		(*o)->flag = NULL;
		if (a->short_arg)
			(*o)->val = a->short_arg;
		else
			(*o)->val = arg;
		(*o)++;
	}
#endif
}

static struct arg *_find_arg(struct command *com, int opt)
{
	struct arg *a;
	int i, arg;

	for (i = 0; i < com->num_args; i++) {
		arg = com->valid_args[i];
		a = the_args + arg;

		/*
		 * opt should equal either the
		 * short arg, or the index into
		 * 'the_args'.
		 */
		if ((a->short_arg && (opt == a->short_arg)) ||
		    (!a->short_arg && (opt == arg)))
			return a;
	}

	return 0;
}

static int _process_command_line(struct cmd_context *cmd, int *argc,
				 char ***argv)
{
	int i, opt;
	char str[((ARG_COUNT + 1) * 2) + 1], *ptr = str;
	struct option opts[ARG_COUNT + 1], *o = opts;
	struct arg *a;

	for (i = 0; i < ARG_COUNT; i++) {
		a = the_args + i;

		/* zero the count and arg */
		a->count = 0;
		a->value = 0;
		a->i_value = 0;
		a->ui_value = 0;
		a->i64_value = 0;
		a->ui64_value = 0;
	}

	/* fill in the short and long opts */
	for (i = 0; i < cmd->command->num_args; i++)
		_add_getopt_arg(cmd->command->valid_args[i], &ptr, &o);

	*ptr = '\0';
	memset(o, 0, sizeof(*o));

	/* initialise getopt_long & scan for command line switches */
	optarg = 0;
	optind = OPTIND_INIT;
	while ((opt = GETOPTLONG_FN(*argc, *argv, str, opts, NULL)) >= 0) {

		if (opt == '?')
			return 0;

		a = _find_arg(cmd->command, opt);

		if (!a) {
			log_fatal("Unrecognised option.");
			return 0;
		}

		if (a->fn) {
			if (a->count) {
				log_error("Option%s%c%s%s may not be repeated",
					  a->short_arg ? " -" : "",
					  a->short_arg ? : ' ',
					  (a->short_arg && a->long_arg) ?
					  "/" : "", a->long_arg ? : "");
				return 0;
			}

			if (!optarg) {
				log_error("Option requires argument.");
				return 0;
			}

			a->value = optarg;

			if (!a->fn(cmd, a)) {
				log_error("Invalid argument %s", optarg);
				return 0;
			}
		}

		a->count++;
	}

	*argc -= optind;
	*argv += optind;
	return 1;
}

static int _merge_synonym(struct cmd_context *cmd, int oldarg, int newarg)
{
	const struct arg *old;
	struct arg *new;

	if (arg_count(cmd, oldarg) && arg_count(cmd, newarg)) {
		log_error("%s and %s are synonyms.  Please only supply one.",
			  the_args[oldarg].long_arg, the_args[newarg].long_arg);
		return 0;
	}

	if (!arg_count(cmd, oldarg))
		return 1;

	old = the_args + oldarg;
	new = the_args + newarg;

	new->count = old->count;
	new->value = old->value;
	new->i_value = old->i_value;
	new->ui_value = old->ui_value;
	new->i64_value = old->i64_value;
	new->ui64_value = old->ui64_value;
	new->sign = old->sign;

	return 1;
}

int version(struct cmd_context *cmd __attribute((unused)),
	    int argc __attribute((unused)),
	    char **argv __attribute((unused)))
{
	char vsn[80];

	log_print("LVM version:     %s", LVM_VERSION);
	if (library_version(vsn, sizeof(vsn)))
		log_print("Library version: %s", vsn);
	if (driver_version(vsn, sizeof(vsn)))
		log_print("Driver version:  %s", vsn);

	return ECMD_PROCESSED;
}

static int _get_settings(struct cmd_context *cmd)
{
	cmd->current_settings = cmd->default_settings;

	if (arg_count(cmd, debug_ARG))
		cmd->current_settings.debug = _LOG_FATAL +
		    (arg_count(cmd, debug_ARG) - 1);

	if (arg_count(cmd, verbose_ARG))
		cmd->current_settings.verbose = arg_count(cmd, verbose_ARG);

	if (arg_count(cmd, quiet_ARG)) {
		cmd->current_settings.debug = 0;
		cmd->current_settings.verbose = 0;
	}

	if (arg_count(cmd, test_ARG))
		cmd->current_settings.test = arg_count(cmd, test_ARG);

	if (arg_count(cmd, driverloaded_ARG)) {
		cmd->current_settings.activation =
		    arg_int_value(cmd, driverloaded_ARG,
				  cmd->default_settings.activation);
	}

	if (arg_count(cmd, autobackup_ARG)) {
		cmd->current_settings.archive = 1;
		cmd->current_settings.backup = 1;
	}

	if (arg_count(cmd, partial_ARG)) {
		init_partial(1);
		log_print("Partial mode. Incomplete volume groups will "
			  "be activated read-only.");
	} else
		init_partial(0);

	if (arg_count(cmd, ignorelockingfailure_ARG))
		init_ignorelockingfailure(1);
	else
		init_ignorelockingfailure(0);

	if (arg_count(cmd, nosuffix_ARG))
		cmd->current_settings.suffix = 0;

	if (arg_count(cmd, units_ARG))
		if (!(cmd->current_settings.unit_factor =
		      units_to_bytes(arg_str_value(cmd, units_ARG, ""),
				     &cmd->current_settings.unit_type))) {
			log_error("Invalid units specification");
			return EINVALID_CMD_LINE;
		}

	/* Handle synonyms */
	if (!_merge_synonym(cmd, resizable_ARG, resizeable_ARG) ||
	    !_merge_synonym(cmd, allocation_ARG, allocatable_ARG) ||
	    !_merge_synonym(cmd, allocation_ARG, resizeable_ARG))
		return EINVALID_CMD_LINE;

	/* Zero indicates success */
	return 0;
}

static int _process_common_commands(struct cmd_context *cmd)
{
	if (arg_count(cmd, help_ARG) || arg_count(cmd, help2_ARG)) {
		_usage(cmd->command->name);
		return ECMD_PROCESSED;
	}

	if (arg_count(cmd, version_ARG)) {
		return version(cmd, 0, (char **) NULL);
	}

	/* Zero indicates it's OK to continue processing this command */
	return 0;
}

static void _display_help(void)
{
	int i;

	log_error("Available lvm commands:");
	log_error("Use 'lvm help <command>' for more information");
	log_error(" ");

	for (i = 0; i < _num_commands; i++) {
		struct command *com = _commands + i;

		log_error("%-16.16s%s", com->name, com->desc);
	}
}

int help(struct cmd_context *cmd __attribute((unused)), int argc, char **argv)
{
	if (!argc)
		_display_help();
	else {
		int i;
		for (i = 0; i < argc; i++)
			_usage(argv[i]);
	}

	return 0;
}

static int _override_settings(struct cmd_context *cmd)
{
	if (!(cmd->cft_override = create_config_tree_from_string(cmd, arg_str_value(cmd, config_ARG, "")))) {
		log_error("Failed to set overridden configuration entries.");
		return EINVALID_CMD_LINE;
	}

	return 0;
}

static void _apply_settings(struct cmd_context *cmd)
{
	init_debug(cmd->current_settings.debug);
	init_verbose(cmd->current_settings.verbose + VERBOSE_BASE_LEVEL);
	init_test(cmd->current_settings.test);
	init_full_scan_done(0);
	init_mirror_in_sync(0);

	init_msg_prefix(cmd->default_settings.msg_prefix);
	init_cmd_name(cmd->default_settings.cmd_name);

	archive_enable(cmd, cmd->current_settings.archive);
	backup_enable(cmd, cmd->current_settings.backup);

	set_activation(cmd->current_settings.activation);

	cmd->fmt = arg_ptr_value(cmd, metadatatype_ARG,
				 cmd->current_settings.fmt);
}

static char *_copy_command_line(struct cmd_context *cmd, int argc, char **argv)
{
	int i, space;

	/*
	 * Build up the complete command line, used as a
	 * description for backups.
	 */
	if (!dm_pool_begin_object(cmd->mem, 128))
		goto bad;

	for (i = 0; i < argc; i++) {
		space = strchr(argv[i], ' ') ? 1 : 0;

		if (space && !dm_pool_grow_object(cmd->mem, "'", 1))
			goto bad;

		if (!dm_pool_grow_object(cmd->mem, argv[i], strlen(argv[i])))
			goto bad;

		if (space && !dm_pool_grow_object(cmd->mem, "'", 1))
			goto bad;

		if (i < (argc - 1))
			if (!dm_pool_grow_object(cmd->mem, " ", 1))
				goto bad;
	}

	/*
	 * Terminate.
	 */
	if (!dm_pool_grow_object(cmd->mem, "\0", 1))
		goto bad;

	return dm_pool_end_object(cmd->mem);

      bad:
	log_err("Couldn't copy command line.");
	dm_pool_abandon_object(cmd->mem);
	return NULL;
}

static int _run_command(struct cmd_context *cmd, int argc, char **argv)
{
	int ret = 0;
	int locking_type;

	if (!(cmd->cmd_line = _copy_command_line(cmd, argc, argv)))
		return ECMD_FAILED;

	log_debug("Parsing: %s", cmd->cmd_line);

	if (!(cmd->command = _find_command(argv[0])))
		return ENO_SUCH_CMD;

	if (!_process_command_line(cmd, &argc, &argv)) {
		log_error("Error during parsing of command line.");
		return EINVALID_CMD_LINE;
	}

	set_cmd_name(cmd->command->name);

	if (arg_count(cmd, config_ARG))
		if ((ret = _override_settings(cmd)))
			goto_out;

	if (arg_count(cmd, config_ARG) || !cmd->config_valid || config_files_changed(cmd)) {
		/* Reinitialise various settings inc. logging, filters */
		if (!refresh_toolcontext(cmd)) {
			log_error("Updated config file invalid. Aborting.");
			return ECMD_FAILED;
		}
	}

	if ((ret = _get_settings(cmd)))
		goto_out;
	_apply_settings(cmd);

	log_debug("Processing: %s", cmd->cmd_line);

#ifdef O_DIRECT_SUPPORT
	log_debug("O_DIRECT will be used");
#endif

	if ((ret = _process_common_commands(cmd)))
		goto_out;

	if (arg_count(cmd, nolocking_ARG))
		locking_type = 0;
	else
		locking_type = find_config_tree_int(cmd,
					       "global/locking_type", 1);

	if (!init_locking(locking_type, cmd)) {
		log_error("Locking type %d initialisation failed.",
			  locking_type);
		ret = ECMD_FAILED;
		goto out;
	}

	ret = cmd->command->fn(cmd, argc, argv);

	fin_locking();

      out:
	if (test_mode()) {
		log_verbose("Test mode: Wiping internal cache");
		lvmcache_destroy();
	}

	if (cmd->cft_override) {
		destroy_config_tree(cmd->cft_override);
		cmd->cft_override = NULL;
		/* Move this? */
		if (!refresh_toolcontext(cmd))
			stack;
	}
 
	/* FIXME Move this? */
	cmd->current_settings = cmd->default_settings;
	_apply_settings(cmd);

	/*
	 * free off any memory the command used.
	 */
	dm_pool_empty(cmd->mem);

	if (ret == EINVALID_CMD_LINE && !_interactive)
		_usage(cmd->command->name);

	log_debug("Completed: %s", cmd->cmd_line);

	return ret;
}

static int _split(char *str, int *argc, char **argv, int max)
{
	char *b = str, *e;
	*argc = 0;

	while (*b) {
		while (*b && isspace(*b))
			b++;

		if ((!*b) || (*b == '#'))
			break;

		e = b;
		while (*e && !isspace(*e))
			e++;

		argv[(*argc)++] = b;
		if (!*e)
			break;
		*e++ = '\0';
		b = e;
		if (*argc == max)
			break;
	}

	return *argc;
}

static void _init_rand(void)
{
	srand((unsigned) time(NULL) + (unsigned) getpid());
}

static void _close_stray_fds(void)
{
	struct rlimit rlim;
	int fd;
	int suppress_warnings = 0;

	if (getrlimit(RLIMIT_NOFILE, &rlim) < 0) {
		fprintf(stderr, "getrlimit(RLIMIT_NOFILE) failed: %s\n",
			strerror(errno));
		return;
	}

	if (getenv("LVM_SUPPRESS_FD_WARNINGS"))
		suppress_warnings = 1;

	for (fd = 3; fd < rlim.rlim_cur; fd++) {
		if (suppress_warnings)
			close(fd);
		else if (!close(fd))
			fprintf(stderr, "File descriptor %d left open\n", fd);
		else if (errno != EBADF)
			fprintf(stderr, "Close failed on stray file "
				"descriptor %d: %s\n", fd, strerror(errno));
	}
}

static struct cmd_context *_init_lvm(void)
{
	struct cmd_context *cmd;

	if (!(cmd = create_toolcontext(&the_args[0]))) {
		stack;
		return NULL;
	}

	_init_rand();

	_apply_settings(cmd);

	return cmd;
}

static void _fin_commands(void)
{
	int i;

	for (i = 0; i < _num_commands; i++)
		dm_free(_commands[i].valid_args);

	dm_free(_commands);
}

static void _fin(struct cmd_context *cmd)
{
	_fin_commands();
	destroy_toolcontext(cmd);
}

static int _run_script(struct cmd_context *cmd, int argc, char **argv)
{
	FILE *script;

	char buffer[CMD_LEN];
	int ret = 0;
	int magic_number = 0;

	if ((script = fopen(argv[0], "r")) == NULL)
		return ENO_SUCH_CMD;

	while (fgets(buffer, sizeof(buffer), script) != NULL) {
		if (!magic_number) {
			if (buffer[0] == '#' && buffer[1] == '!')
				magic_number = 1;
			else {
				ret = ENO_SUCH_CMD;
				break;
			}
		}
		if ((strlen(buffer) == sizeof(buffer) - 1)
		    && (buffer[sizeof(buffer) - 1] - 2 != '\n')) {
			buffer[50] = '\0';
			log_error("Line too long (max 255) beginning: %s",
				  buffer);
			ret = EINVALID_CMD_LINE;
			break;
		}
		if (_split(buffer, &argc, argv, MAX_ARGS) == MAX_ARGS) {
			buffer[50] = '\0';
			log_error("Too many arguments: %s", buffer);
			ret = EINVALID_CMD_LINE;
			break;
		}
		if (!argc)
			continue;
		if (!strcmp(argv[0], "quit") || !strcmp(argv[0], "exit"))
			break;
		_run_command(cmd, argc, argv);
	}

	fclose(script);
	return ret;
}

#ifdef READLINE_SUPPORT
/* List matching commands */
static char *_list_cmds(const char *text, int state)
{
	static int i = 0;
	static size_t len = 0;

	/* Initialise if this is a new completion attempt */
	if (!state) {
		i = 0;
		len = strlen(text);
	}

	while (i < _num_commands)
		if (!strncmp(text, _commands[i++].name, len))
			return strdup(_commands[i - 1].name);

	return NULL;
}

/* List matching arguments */
static char *_list_args(const char *text, int state)
{
	static int match_no = 0;
	static size_t len = 0;
	static struct command *com;

	/* Initialise if this is a new completion attempt */
	if (!state) {
		char *s = rl_line_buffer;
		int j = 0;

		match_no = 0;
		com = NULL;
		len = strlen(text);

		/* Find start of first word in line buffer */
		while (isspace(*s))
			s++;

		/* Look for word in list of commands */
		for (j = 0; j < _num_commands; j++) {
			const char *p;
			char *q = s;

			p = _commands[j].name;
			while (*p == *q) {
				p++;
				q++;
			}
			if ((!*p) && *q == ' ') {
				com = _commands + j;
				break;
			}
		}

		if (!com)
			return NULL;
	}

	/* Short form arguments */
	if (len < 3) {
		while (match_no < com->num_args) {
			char s[3];
			char c;
			if (!(c = (the_args +
				   com->valid_args[match_no++])->short_arg))
				continue;

			sprintf(s, "-%c", c);
			if (!strncmp(text, s, len))
				return strdup(s);
		}
	}

	/* Long form arguments */
	if (match_no < com->num_args)
		match_no = com->num_args;

	while (match_no - com->num_args < com->num_args) {
		const char *l;
		l = (the_args +
		     com->valid_args[match_no++ - com->num_args])->long_arg;
		if (*(l + 2) && !strncmp(text, l, len))
			return strdup(l);
	}

	return NULL;
}

/* Custom completion function */
static char **_completion(const char *text, int start_pos, int end_pos)
{
	char **match_list = NULL;
	int p = 0;

	while (isspace((int) *(rl_line_buffer + p)))
		p++;

	/* First word should be one of our commands */
	if (start_pos == p)
		match_list = rl_completion_matches(text, _list_cmds);

	else if (*text == '-')
		match_list = rl_completion_matches(text, _list_args);
	/* else other args */

	/* No further completion */
	rl_attempted_completion_over = 1;
	return match_list;
}

static int _hist_file(char *buffer, size_t size)
{
	char *e = getenv("HOME");

	if (lvm_snprintf(buffer, size, "%s/.lvm_history", e) < 0) {
		log_error("$HOME/.lvm_history: path too long");
		return 0;
	}

	return 1;
}

static void _read_history(struct cmd_context *cmd)
{
	char hist_file[PATH_MAX];

	if (!_hist_file(hist_file, sizeof(hist_file)))
		return;

	if (read_history(hist_file))
		log_very_verbose("Couldn't read history from %s.", hist_file);

	stifle_history(find_config_tree_int(cmd, "shell/history_size",
				       DEFAULT_MAX_HISTORY));

}

static void _write_history(void)
{
	char hist_file[PATH_MAX];

	if (!_hist_file(hist_file, sizeof(hist_file)))
		return;

	if (write_history(hist_file))
		log_very_verbose("Couldn't write history to %s.", hist_file);
}

static int _shell(struct cmd_context *cmd)
{
	int argc, ret;
	char *input = NULL, *args[MAX_ARGS], **argv;

	rl_readline_name = "lvm";
	rl_attempted_completion_function = (CPPFunction *) _completion;

	_read_history(cmd);

	_interactive = 1;
	while (1) {
		free(input);
		input = readline("lvm> ");

		/* EOF */
		if (!input) {
			printf("\n");
			break;
		}

		/* empty line */
		if (!*input)
			continue;

		add_history(input);

		argv = args;

		if (_split(input, &argc, argv, MAX_ARGS) == MAX_ARGS) {
			log_error("Too many arguments, sorry.");
			continue;
		}

		if (!strcmp(argv[0], "lvm")) {
			argv++;
			argc--;
		}

		if (!argc)
			continue;

		if (!strcmp(argv[0], "quit") || !strcmp(argv[0], "exit")) {
			remove_history(history_length - 1);
			log_error("Exiting.");
			break;
		}

		ret = _run_command(cmd, argc, argv);
		if (ret == ENO_SUCH_CMD)
			log_error("No such command '%s'.  Try 'help'.",
				  argv[0]);

		_write_history();
	}

	free(input);
	return 0;
}

#endif

#ifdef CMDLIB

void *lvm2_init(void)
{
	struct cmd_context *cmd;

	_register_commands();

	if (!(cmd = _init_lvm()))
		return NULL;

	return (void *) cmd;
}

int lvm2_run(void *handle, const char *cmdline)
{
	int argc, ret, oneoff = 0;
	char *args[MAX_ARGS], **argv, *cmdcopy = NULL;
	struct cmd_context *cmd;

	argv = args;

	if (!handle) {
		oneoff = 1;
		if (!(handle = lvm2_init())) {
			log_error("Handle initialisation failed.");
			return ECMD_FAILED;
		}
	}

	cmd = (struct cmd_context *) handle;

	cmd->argv = argv;

	if (!(cmdcopy = dm_strdup(cmdline))) {
		log_error("Cmdline copy failed.");
		ret = ECMD_FAILED;
		goto out;
	}

	if (_split(cmdcopy, &argc, argv, MAX_ARGS) == MAX_ARGS) {
		log_error("Too many arguments.  Limit is %d.", MAX_ARGS);
		ret = EINVALID_CMD_LINE;
		goto out;
	}

	if (!argc) {
		log_error("No command supplied");
		ret = EINVALID_CMD_LINE;
		goto out;
	}

	ret = _run_command(cmd, argc, argv);

      out:
	dm_free(cmdcopy);

	if (oneoff)
		lvm2_exit(handle);

	return ret;
}

void lvm2_log_level(void *handle, int level)
{
	struct cmd_context *cmd = (struct cmd_context *) handle;

	cmd->default_settings.verbose = level - VERBOSE_BASE_LEVEL;

	return;
}

void lvm2_log_fn(lvm2_log_fn_t log_fn)
{
	init_log_fn(log_fn);
}

void lvm2_exit(void *handle)
{
	struct cmd_context *cmd = (struct cmd_context *) handle;

	_fin(cmd);
}

#endif

/*
 * Determine whether we should fall back and exec the equivalent LVM1 tool
 */
static int _lvm1_fallback(struct cmd_context *cmd)
{
	char vsn[80];
	int dm_present;

	if (!find_config_tree_int(cmd, "global/fallback_to_lvm1",
			     DEFAULT_FALLBACK_TO_LVM1) ||
	    strncmp(cmd->kernel_vsn, "2.4.", 4))
		return 0;

	log_suppress(1);
	dm_present = driver_version(vsn, sizeof(vsn));
	log_suppress(0);

	if (dm_present || !lvm1_present(cmd))
		return 0;

	return 1;
}

static void _exec_lvm1_command(char **argv)
{
	char path[PATH_MAX];

	if (lvm_snprintf(path, sizeof(path), "%s.lvm1", argv[0]) < 0) {
		log_error("Failed to create LVM1 tool pathname");
		return;
	}

	execvp(path, argv);
	log_sys_error("execvp", path);
}

int lvm2_main(int argc, char **argv, int is_static)
{
	char *namebase, *base;
	int ret, alias = 0;
	struct cmd_context *cmd;

	_close_stray_fds();

	namebase = strdup(argv[0]);
	base = basename(namebase);
	while (*base == '/')
		base++;
	if (strcmp(base, "lvm") && strcmp(base, "lvm.static") &&
	    strcmp(base, "initrd-lvm"))
		alias = 1;

	if (is_static && strcmp(base, "lvm.static") && 
	    path_exists(LVM_SHARED_PATH) &&
	    !getenv("LVM_DID_EXEC")) {
		setenv("LVM_DID_EXEC", base, 1);
		execvp(LVM_SHARED_PATH, argv);
		unsetenv("LVM_DID_EXEC");
	}

	free(namebase);

	if (!(cmd = _init_lvm()))
		return -1;

	cmd->argv = argv;
	_register_commands();

	if (_lvm1_fallback(cmd)) {
		/* Attempt to run equivalent LVM1 tool instead */
		if (!alias) {
			argv++;
			argc--;
			alias = 0;
		}
		if (!argc) {
			log_error("Falling back to LVM1 tools, but no "
				  "command specified.");
			return ECMD_FAILED;
		}
		_exec_lvm1_command(argv);
		return ECMD_FAILED;
	}
#ifdef READLINE_SUPPORT
	if (!alias && argc == 1) {
		ret = _shell(cmd);
		goto out;
	}
#endif

	if (!alias) {
		if (argc < 2) {
			log_fatal("Please supply an LVM command.");
			_display_help();
			ret = EINVALID_CMD_LINE;
			goto out;
		}

		argc--;
		argv++;
	}

	ret = _run_command(cmd, argc, argv);
	if ((ret == ENO_SUCH_CMD) && (!alias))
		ret = _run_script(cmd, argc, argv);
	if (ret == ENO_SUCH_CMD)
		log_error("No such command.  Try 'help'.");

      out:
	_fin(cmd);
	if (ret == ECMD_PROCESSED)
		ret = 0;
	return ret;
}
