#!/bin/sh
# Copyright (C) 2012 Red Hat, Inc. All rights reserved.
#
# This copyrighted material is made available to anyone wishing to use,
# modify, copy, or redistribute it subject to the terms and conditions
# of the GNU General Public License v.2.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

. lib/inittest

aux prepare_devs 2
pvcreate $dev1 $dev2
vgcreate $vg $dev1 $dev2

lvcreate -n test -l 1 -ay $vg
check active $vg test
lvchange -an $vg
not check active $vg test
lvchange -ay $vg --config "activation { volume_list = [] }"
not check active $vg test
lvchange -ay $vg --config "activation { volume_list = [ \"$vg\" ] }" \
	         --config "activation { volume_list = [] }"
lvchange -ay $vg --config "activation/volume_list = [ \"$vg\" ]" \
	         --config "activation/volume_list = []"
not check active $vg test
lvchange -ay $vg --config "activation { volume_list = [] }" \
	         --config "activation { volume_list = [ \"$vg\" ] }"
check active $vg test
