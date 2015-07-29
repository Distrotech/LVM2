#!/bin/sh
# Copyright (C) 2015 Red Hat, Inc. All rights reserved.
#
# This copyrighted material is made available to anyone wishing to use,
# modify, copy, or redistribute it subject to the terms and conditions
# of the GNU General Public License v.2.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

. lib/inittest

########################################################
# MAIN
########################################################
aux have_raid 1 3 0 || skip

aux prepare_pvs 6 20  # 6 devices for striped test
vgcreate -s 128k $vg $(cat DEVICES)

############################################
# Create striped LV, convert to raid0* tests
############################################
# Create striped 6-way and cycle conversions
lvcreate -y -i 6 -l 50%FREE -n $lv1 $vg
lvconvert --type raid0 $vg/$lv1
lvconvert --type raid0_meta $vg/$lv1
lvconvert --type striped $vg/$lv1
lvremove -ff $vg

# Create raid0 5-way and cycle conversions
lvcreate -y --type raid0 -i 5 -l 50%FREE -n $lv1 $vg
lvconvert --type raid0_meta $vg/$lv1
lvconvert --type striped $vg/$lv1
lvconvert --type raid0 $vg/$lv1
lvremove -ff $vg

# Create raid0_meta 4-way and cycle conversions
lvcreate -y --type raid0_meta -i 4 -l 50%FREE -n $lv1 $vg
lvconvert --type raid0 $vg/$lv1
lvconvert --type striped $vg/$lv1
lvconvert --type raid0_meta $vg/$lv1
lvremove -ff $vg

# Create striped 3-way cosuming all vg space
lvcreate -y -i 3 -l 100%FREE -n $lv1 $vg
lvconvert --type raid0 $vg/$lv1
not lvconvert --type raid0_meta $vg/$lv1
lvconvert --type striped $vg/$lv1
lvremove -ff $vg

# Not enough drives
not lvcreate -y -i3 -l1 $vg "$dev1" "$dev2"
not lvcreate -y --type raid0 -i3 -l1 $vg "$dev1" "$dev2"
not lvcreate -y --type raid0_meta -i4 -l1 $vg "$dev1" "$dev2" "$dev3"

# Create 2..6-way raid0 LV and cycle conversions
for s in $(seq 2..6)
do
	lvcreate -y --type raid0 -l 95%FREE -i $s -n $lv1 $vg
	lvconvert --type raid0_meta $vg/$lv1
	lvconvert --type raid0 $vg/$lv1
	lvconvert --type striped $vg/$lv1
	lvconvert --type raid0 $vg/$lv1
	lvconvert --type raid0_meta $vg/$lv1
	lvremove -ff $vg
done

# Not enough drives for 7-way
not lvcreate -y --type raid0 -l 7 -i 7 -n $lv1 $vg

vgremove -ff $vg
