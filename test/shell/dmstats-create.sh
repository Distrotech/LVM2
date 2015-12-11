#!/bin/sh
# Copyright (C) 2009-2011 Red Hat, Inc. All rights reserved.
#
# This copyrighted material is made available to anyone wishing to use,
# modify, copy, or redistribute it subject to the terms and conditions
# of the GNU General Public License v.2.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

. lib/inittest

# Don't attempt to test stats with driver < 4.33.00
aux driver_at_least 4 33 || skip

# ensure we can create devices (uses dmsetup, etc)
aux prepare_devs 4

# basic dmstats create commands

dmstats create "$dev1"
dmstats create --start 0 --len 1 "$dev2"
dmstats create --segments "$dev3"
dmstats create --precise "$dev1"
dmstats create --bounds 10ms,20ms,30ms "$dev1"

