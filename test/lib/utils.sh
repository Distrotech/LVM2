#!/bin/bash
# Copyright (C) 2011 Red Hat, Inc. All rights reserved.
#
# This copyrighted material is made available to anyone wishing to use,
# modify, copy, or redistribute it subject to the terms and conditions
# of the GNU General Public License v.2.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

set -e
MAX_TRIES=4

die() {
	echo "$@" >&2
	return 1
}

rand_bytes() {
	n=$1

	chars="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	dev_rand="/dev/urandom"
	if test -r "$dev_rand"; then
		# Note: 256-length($chars) == 194; 3 copies of $chars is 186 + 8 = 194.
		head -c"$n" "$dev_rand" | tr -c "$chars" "01234567$chars$chars$chars"
		return
	fi

	cmds='date; date +%N; free; who -a; w; ps auxww; ps ef; netstat -n'
	data=$( (eval "$cmds") 2>&1 | gzip )

	n_plus_50=$(expr $n + 50)

	# Ensure that $data has length at least 50+$n
	while :; do
		len=$(echo "$data" | wc -c)
		test $n_plus_50 -le $len && break;
		data=$( (echo "$data"; eval "$cmds") 2>&1 | gzip )
	done

	echo "$data" | dd bs=1 skip=50 count=$n 2>/dev/null \
		| tr -c "$chars" "01234567$chars$chars$chars"
}

mkdtemp() {
	case $# in
		2) ;;
		*) die "Usage: mkdtemp DIR TEMPLATE";;
	esac

	destdir=$1
	template=$2

	case "$template" in
		*XXXX) ;;
		*) die "Invalid template: $template (must have a suffix of at least 4 X's)";;
	esac

	fail=0

	# First, try to use mktemp.
	d=$(env -u TMPDIR mktemp -d -t -p "$destdir" "$template" 2>/dev/null) \
	  || fail=1

	# The resulting name must be in the specified directory.
	case "$d" in "${destdir}"*);; *) fail=1;; esac

	# It must have created the directory.
	test -d "$d" || fail=1

	# It must have 0700 permissions.
	perms=$(ls -dgo "$d" 2>/dev/null) || fail=1
	case "$perms" in drwx------*) ;; *) fail=1;; esac

	test $fail = 0 && { echo "$d"; return; }

	# If we reach this point, we'll have to create a directory manually.

	# Get a copy of the template without its suffix of X's.
	base_template=$(echo "$template" | sed 's/XX*$//')

	# Calculate how many X's we've just removed.
	nx=$(expr length "$template" - length "$base_template")

	err=
	i=1
	while :; do
		X=$(rand_bytes "$nx")
		candidate_dir="$destdir/$base_template$X"
		err=$(mkdir -m 0700 "$candidate_dir" 2>&1) && \
			{ echo "$candidate_dir"; return; }
		test $MAX_TRIES -le $i && break;
		i=$(expr $i + 1)
	done
	die "$err"
}

STACKTRACE() {
	trap - ERR
	local i=0

	echo "## - $0:${BASH_LINENO[0]}"
	while FUNC=${FUNCNAME[$i]}; test "$FUNC" != "main"; do
		echo "## $i ${FUNC}() called from ${BASH_SOURCE[$i]}:${BASH_LINENO[$i]}"
		i=$(($i + 1))
	done

	test -n "$RUNNING_DMEVENTD" -o -f LOCAL_DMEVENTD || {
		pgrep dmeventd &>/dev/null && \
			die "** During test dmeventd has been started!"
	}

	# Get backtraces from coredumps
	if which gdb &>/dev/null; then
		echo bt full > gdb_commands.txt
		echo l >> gdb_commands.txt
		echo quit >> gdb_commands.txt
		for core in $(ls core* 2>/dev/null); do
			bin=$(gdb -batch -c "$core" 2>&1 | grep "generated by" | \
			sed -e "s,.*generated by \`\([^ ']*\).*,\1,")
			gdb -batch -c "$core" -x gdb_commands.txt $(which "$bin")
		done
	fi

	test -z "$LVM_TEST_NODEBUG" -a -f debug.log && {
		sed -e "s,^,## DEBUG: ,;s,$top_srcdir/\?,," < debug.log
	}

	test -f SKIP_THIS_TEST && exit 200
}

init_udev_transaction() {
	if test "$DM_UDEV_SYNCHRONISATION" = 1; then
		local cookie=$(dmsetup udevcreatecookie)
		# Cookie is not generated if udev is not running!
		test -z "$cookie" || export DM_UDEV_COOKIE=$cookie
	fi
}

finish_udev_transaction() {
	if test "$DM_UDEV_SYNCHRONISATION" = 1 -a -n "$DM_UDEV_COOKIE"; then
		dmsetup udevreleasecookie
		unset DM_UDEV_COOKIE
	fi
}

teardown_udev_cookies() {
	if test "$DM_UDEV_SYNCHRONISATION" = 1; then
		# Delete any cookies created more than 10 minutes ago
		# and not used in the last 10 minutes.
		# Log only non-zero semaphores count
		(dmsetup udevcomplete_all -y 10 | grep -v "^0 ") || true
	fi
}

skip() {
	touch SKIP_THIS_TEST
	exit 200
}

kernel_at_least() {
	local major=$(uname -r | cut -d. -f1)
	local minor=$(uname -r | cut -d. -f2 | cut -d- -f1)

	test $major -gt $1 && return 0
	test $major -eq $1 || return 1
	test $minor -gt $2 && return 0
	test $minor -eq $2 || return 1
	test -z "$3" && return 0

	local minor2=$(uname -r | cut -d. -f3 | cut -d- -f1)
	test -z "$minor2" -a $3 -ne 0 && return 1
	test $minor2 -ge $3 2>/dev/null || return 1
}

prepare_test_vars() {
	vg="${PREFIX}vg"
	lv=LV

	for i in $(seq 1 16); do
		name="${PREFIX}pv$i"
		dev="$DM_DEV_DIR/mapper/$name"
		eval "dev$i=\"$dev\""
		eval "lv$i=LV$i"
		eval "vg$i=${PREFIX}vg$i"
	done
}

. lib/paths || die "you must run make first"

PATH="$abs_top_builddir/test/lib":$PATH
for d in daemons/dmeventd/plugins/mirror daemons/dmeventd/plugins/snapshot \
	daemons/dmeventd/plugins/lvm2 daemons/dmeventd liblvm tools libdm; do
	LD_LIBRARY_PATH="$abs_top_builddir/$d":$LD_LIBRARY_PATH
done
export PATH LD_LIBRARY_PATH

test -z "$PREFIX" || prepare_test_vars
