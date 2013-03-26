#!/bin/sh
# Copyright (C) 2008-2013 Red Hat, Inc. All rights reserved.
#
# This copyrighted material is made available to anyone wishing to use,
# modify, copy, or redistribute it subject to the terms and conditions
# of the GNU General Public License v.2.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

test_description='Exercise toollib process_each_pv'

. lib/test

aux prepare_devs 14

#
# process_each_pv is used by a number of pv commands:
# pvdisplay
# pvresize
# pvs
# vgreduce
#


#
# set up
#
# use use dev10 instead of dev1 because simple grep for
# dev1 matchines dev10,dev11,etc
#

vgcreate $vg1 "$dev10"
vgcreate $vg2 "$dev2" "$dev3" "$dev4" "$dev5"
vgcreate $vg3 "$dev6" "$dev7" "$dev8" "$dev9"

pvchange --addtag V2D3 "$dev3"
pvchange --addtag V2D4 "$dev4"
pvchange --addtag V2D45 "$dev4"
pvchange --addtag V2D5 "$dev5"
pvchange --addtag V2D45 "$dev5"

pvchange --addtag V3 "$dev6" "$dev7" "$dev8" "$dev9"
pvchange --addtag V3D9 "$dev9"

# orphan
pvcreate "$dev11"

# dev (a non-pv device)
pvcreate "$dev12"
pvremove "$dev12"

# dev13 is intentionally untouched so we can
# test that it is handled appropriately as a non-pv

# orphan
pvcreate "$dev14"


#
# test pvdisplay
#

# pv in vg
pvdisplay -s $dev10 >err
grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# pv not in vg (one orphan)
pvdisplay -s $dev11 >err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# dev is not a pv
not pvdisplay -s $dev12 >err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# two pvs in different vgs
pvdisplay -s $dev10 $dev2 >err
grep $dev10 err
grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# -a is invalid when used alone
not pvdisplay -a >err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# one pv and one orphan
pvdisplay -s $dev10 $dev11 >err
grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# one pv and one dev (dev refers to a non-pv device)
not pvdisplay -s $dev10 $dev12 >err
grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# one orphan and one dev
not pvdisplay -s $dev11 $dev12 >err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# all pvs (pvs in vgs and orphan pvs)
pvdisplay -s >err
grep $dev10 err
grep $dev2 err
grep $dev3 err
grep $dev4 err
grep $dev5 err
grep $dev6 err
grep $dev7 err
grep $dev8 err
grep $dev9 err
grep $dev11 err
not grep $dev12 err
not grep $dev13 err
grep $dev14 err

# all devs (pvs in vgs, orphan pvs, and devs)
pvdisplay -a -C >err
grep $dev10 err
grep $dev2 err
grep $dev3 err
grep $dev4 err
grep $dev5 err
grep $dev6 err
grep $dev7 err
grep $dev8 err
grep $dev9 err
grep $dev11 err
grep $dev12 err
grep $dev13 err
grep $dev14 err

# pv and orphan and dev
not pvdisplay -s $dev9 $dev11 $dev12 > err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
grep $dev9 err
grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# -s option not allowed with -a -C
not pvdisplay -s -a -C > err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# pv and all (all ignored)
pvdisplay -a -C $dev9 > err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# orphan and all (all ignored)
pvdisplay -a -C $dev11 > err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# one tag
pvdisplay -s @V2D3 > err
not grep $dev10 err
not grep $dev2 err
grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# two tags
pvdisplay -s @V2D3 @V2D45 > err
not grep $dev10 err
not grep $dev2 err
grep $dev3 err
grep $dev4 err
grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# tag and pv
pvdisplay -s @V2D3 $dev4 > err
not grep $dev10 err
not grep $dev2 err
grep $dev3 err
grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# tag and orphan
pvdisplay -s @V2D3 $dev11 > err
not grep $dev10 err
not grep $dev2 err
grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# tag and dev
not pvdisplay -s @V2D3 $dev12 > err
not grep $dev10 err
not grep $dev2 err
grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# tag and all (all ignored)
pvdisplay @V2D3 -a -C > err
not grep $dev10 err
not grep $dev2 err
grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# tag and pv redundant
pvdisplay -s @V2D3 $dev3 > err
not grep $dev10 err
not grep $dev2 err
grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err


#
# test pvs
#

# pv in vg
pvs $dev10 >err
grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# pv not in vg (one orphan)
pvs $dev11 >err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# dev is not a pv
not pvs $dev12 >err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# two pvs in different vgs
pvs $dev10 $dev2 >err
grep $dev10 err
grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# one pv and one orphan
pvs $dev10 $dev11 >err
grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# one pv and one dev
not pvs $dev10 $dev12 >err
grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# one orphan and one dev
not pvs $dev11 $dev12 >err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# all pvs (pvs in vgs and orphan pvs)
pvs >err
grep $dev10 err
grep $dev2 err
grep $dev3 err
grep $dev4 err
grep $dev5 err
grep $dev6 err
grep $dev7 err
grep $dev8 err
grep $dev9 err
grep $dev11 err
not grep $dev12 err
not grep $dev13 err
grep $dev14 err

# all devs (pvs in vgs, orphan pvs, and devs)
pvs -a >err
grep $dev10 err
grep $dev2 err
grep $dev3 err
grep $dev4 err
grep $dev5 err
grep $dev6 err
grep $dev7 err
grep $dev8 err
grep $dev9 err
grep $dev11 err
grep $dev12 err
grep $dev13 err
grep $dev14 err

# pv and orphan and dev
not pvs $dev9 $dev11 $dev12 > err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
grep $dev9 err
grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# pv and all (all ignored)
pvs -a $dev9 > err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# orphan and all (all ignored)
pvs -a $dev11 > err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# one tag
pvs @V2D3 > err
not grep $dev10 err
not grep $dev2 err
grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# two tags
pvs @V2D3 @V2D45 > err
not grep $dev10 err
not grep $dev2 err
grep $dev3 err
grep $dev4 err
grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# tag and pv
pvs @V2D3 $dev4 > err
not grep $dev10 err
not grep $dev2 err
grep $dev3 err
grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# tag and orphan
pvs @V2D3 $dev11 > err
not grep $dev10 err
not grep $dev2 err
grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# tag and dev
not pvs @V2D3 $dev12 > err
not grep $dev10 err
not grep $dev2 err
grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# tag and all (all ignored)
pvs @V2D3 -a > err
not grep $dev10 err
not grep $dev2 err
grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# tag and pv redundant
pvs @V2D3 $dev3 > err
not grep $dev10 err
not grep $dev2 err
grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err


#
# test vgreduce
#

# fail without dev
not vgreduce $vg2


# fail with dev and -a
not vgreduce $vg2 $dev2 -a
check pv_field $dev2 vg_name $vg2
check pv_field $dev3 vg_name $vg2
check pv_field $dev4 vg_name $vg2
check pv_field $dev5 vg_name $vg2
check pv_field $dev6 vg_name $vg3
check pv_field $dev7 vg_name $vg3
check pv_field $dev8 vg_name $vg3
check pv_field $dev9 vg_name $vg3


# remove one pv
vgreduce $vg2 $dev2
not check pv_field $dev2 vg_name $vg2
check pv_field $dev3 vg_name $vg2
check pv_field $dev4 vg_name $vg2
check pv_field $dev5 vg_name $vg2
check pv_field $dev6 vg_name $vg3
check pv_field $dev7 vg_name $vg3
check pv_field $dev8 vg_name $vg3
check pv_field $dev9 vg_name $vg3
# reset
vgextend $vg2 $dev2


# remove two pvs
vgreduce $vg2 $dev2 $dev3
not check pv_field $dev2 vg_name $vg2
not check pv_field $dev3 vg_name $vg2
check pv_field $dev4 vg_name $vg2
check pv_field $dev5 vg_name $vg2
check pv_field $dev6 vg_name $vg3
check pv_field $dev7 vg_name $vg3
check pv_field $dev8 vg_name $vg3
check pv_field $dev9 vg_name $vg3
# reset
vgextend $vg2 $dev2 $dev3
pvchange --addtag V2D3 "$dev3"


# remove one pv with tag
vgreduce $vg2 @V2D3
check pv_field $dev2 vg_name $vg2
not check pv_field $dev3 vg_name $vg2
check pv_field $dev4 vg_name $vg2
check pv_field $dev5 vg_name $vg2
check pv_field $dev6 vg_name $vg3
check pv_field $dev7 vg_name $vg3
check pv_field $dev8 vg_name $vg3
check pv_field $dev9 vg_name $vg3
# reset
vgextend $vg2 $dev3
pvchange --addtag V2D3 "$dev3"


# remove two pvs, each with different tag
vgreduce $vg2 @V2D3 @V2D4
check pv_field $dev2 vg_name $vg2
not check pv_field $dev3 vg_name $vg2
not check pv_field $dev4 vg_name $vg2
check pv_field $dev5 vg_name $vg2
check pv_field $dev6 vg_name $vg3
check pv_field $dev7 vg_name $vg3
check pv_field $dev8 vg_name $vg3
check pv_field $dev9 vg_name $vg3
# reset
vgextend $vg2 $dev3 $dev4
pvchange --addtag V2D3 "$dev3"
pvchange --addtag V2D4 "$dev4"
pvchange --addtag V2D45 "$dev4"


# remove two pvs, both with same tag
vgreduce $vg2 @V2D45
check pv_field $dev2 vg_name $vg2
check pv_field $dev3 vg_name $vg2
not check pv_field $dev4 vg_name $vg2
not check pv_field $dev5 vg_name $vg2
check pv_field $dev6 vg_name $vg3
check pv_field $dev7 vg_name $vg3
check pv_field $dev8 vg_name $vg3
check pv_field $dev9 vg_name $vg3
# reset
vgextend $vg2 $dev4 $dev5
pvchange --addtag V2D4 "$dev4"
pvchange --addtag V2D45 "$dev4"
pvchange --addtag V2D5 "$dev5"
pvchange --addtag V2D45 "$dev5"


# remove two pvs, one by name, one by tag
vgreduce $vg2 $dev2 @V2D3
not check pv_field $dev2 vg_name $vg2
not check pv_field $dev3 vg_name $vg2
check pv_field $dev4 vg_name $vg2
check pv_field $dev5 vg_name $vg2
check pv_field $dev6 vg_name $vg3
check pv_field $dev7 vg_name $vg3
check pv_field $dev8 vg_name $vg3
check pv_field $dev9 vg_name $vg3
# reset
vgextend $vg2 $dev2 $dev3
pvchange --addtag V2D3 "$dev3"


# remove one pv by tag, where another vg has a pv with same tag
pvchange --addtag V2D5V3D9 "$dev5"
pvchange --addtag V2D5V3D9 "$dev9"
vgreduce $vg2 @V2D5V3D9
check pv_field $dev2 vg_name $vg2
check pv_field $dev3 vg_name $vg2
check pv_field $dev4 vg_name $vg2
not check pv_field $dev5 vg_name $vg2
check pv_field $dev6 vg_name $vg3
check pv_field $dev7 vg_name $vg3
check pv_field $dev8 vg_name $vg3
check pv_field $dev9 vg_name $vg3
# reset
vgextend $vg2 $dev5
pvchange --addtag V2D5 "$dev5"
pvchange --addtag V2D45 "$dev5"


# fail to remove last pv (don't know which will be last)
not vgreduce -a $vg2
# reset
vgremove $vg2
vgcreate $vg2 "$dev2" "$dev3" "$dev4" "$dev5"
pvchange --addtag V2D3 "$dev3"
pvchange --addtag V2D4 "$dev4"
pvchange --addtag V2D45 "$dev4"
pvchange --addtag V2D5 "$dev5"
pvchange --addtag V2D45 "$dev5"


# lvcreate on one pv to make it used
# remove all unused pvs
lvcreate -n $lv1 -l 2 $vg2 $dev2
not vgreduce -a $vg2
check pv_field $dev2 vg_name $vg2
not check pv_field $dev3 vg_name $vg2
not check pv_field $dev4 vg_name $vg2
not check pv_field $dev5 vg_name $vg2
check pv_field $dev6 vg_name $vg3
check pv_field $dev7 vg_name $vg3
check pv_field $dev8 vg_name $vg3
check pv_field $dev9 vg_name $vg3
# reset
vgextend $vg2 $dev3 $dev4 $dev5
pvchange --addtag V2D3 "$dev3"
pvchange --addtag V2D4 "$dev4"
pvchange --addtag V2D45 "$dev4"
pvchange --addtag V2D5 "$dev5"
pvchange --addtag V2D45 "$dev5"
lvchange -an $vg2/$lv1
lvremove $vg2/$lv1


#
# test pvresize without orphans and and without non-pv devs
#

old="30.00m"
new="20.00m"

# use a common "old" size instead of getting the original size
# because orphan and non-orphan pv's may have different pv_size
# even if the devices are the same dev_size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev9 pv_size $old

# one pv
pvresize --setphysicalvolumesize $new $dev10
#check pv_field $dev10 pv_size $new
# unchanged
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev9 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9

# two pvs in separate vgs
pvresize --setphysicalvolumesize $new $dev2 $dev6
#check pv_field $dev2 pv_size $new
#check pv_field $dev6 pv_size $new
# unchanged
#check pv_field $dev10 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev9 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9

# one tag on one pv
pvresize --setphysicalvolumesize $new @V2D4
#check pv_field $dev4 pv_size $new
# unchanged
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev9 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9

# one tag on all pvs in one vg
pvresize --setphysicalvolumesize $new @V3
#check pv_field $dev6 pv_size $new
#check pv_field $dev7 pv_size $new
#check pv_field $dev8 pv_size $new
#check pv_field $dev9 pv_size $new
# unchanged
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9

# one tag on some pvs in one vg
pvresize --setphysicalvolumesize $new @V2D45
#check pv_field $dev4 pv_size $new
#check pv_field $dev5 pv_size $new
# unchanged
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev9 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9

# one tag on multiple pvs in separate vgs
pvchange --addtag V12 "$dev10" "$dev2" "$dev3" "$dev4" "$dev5"
pvresize --setphysicalvolumesize $new @V12
#check pv_field $dev10 pv_size $new
#check pv_field $dev2 pv_size $new
#check pv_field $dev3 pv_size $new
#check pv_field $dev4 pv_size $new
#check pv_field $dev5 pv_size $new
# unchanged
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev9 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9

# one pv and one tag on different pv
pvresize --setphysicalvolumesize $new $dev10 @V3D9
#check pv_field $dev10 pv_size $new
#check pv_field $dev9 pv_size $new
# unchanged
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9

# redundant pv and tag
pvresize --setphysicalvolumesize $new $dev9 @V3D9
#check pv_field $dev9 pv_size $new
# unchanged
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9

# two tags on pvs in separate vgs
pvresize --setphysicalvolumesize $new @V3D9 @V2D3
#check pv_field $dev9 pv_size $new
#check pv_field $dev3 pv_size $new
# unchanged
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9


#
# test pvresize with orphans
#

old="30.00m"
new="20.00m"

# use a common "old" size instead of getting the original size
# because orphan and non-orphan pv's may have different pv_size
# even if the devices are the same dev_size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9
pvresize --setphysicalvolumesize $old $dev11 $dev14
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev9 pv_size $old
#check pv_field $dev11 pv_size $old
#check pv_field $dev14 pv_size $old

# one orphan
pvresize --setphysicalvolumesize $new $dev11
#check pv_field $dev11 pv_size $new
# unchanged
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev9 pv_size $old
#check pv_field $dev14 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9
pvresize --setphysicalvolumesize $old $dev11 $dev14

# two orphans
pvresize --setphysicalvolumesize $new $dev11 $dev14
#check pv_field $dev11 pv_size $new
#check pv_field $dev14 pv_size $new
# unchanged
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev9 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9
pvresize --setphysicalvolumesize $old $dev11 $dev14

# one orphan, one tag
pvresize --setphysicalvolumesize $new @V3D9 $dev14
#check pv_field $dev9 pv_size $new
#check pv_field $dev14 pv_size $new
# unchanged
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev11 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9
pvresize --setphysicalvolumesize $old $dev11 $dev14

# one pv, one orphan, one tag
pvresize --setphysicalvolumesize $new @V3D9 $dev14 $dev10
#check pv_field $dev9 pv_size $new
#check pv_field $dev10 pv_size $new
#check pv_field $dev14 pv_size $new
# unchanged
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev11 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9
pvresize --setphysicalvolumesize $old $dev11 $dev14


#
# test pvresize with non-pv devs
#

# one dev (non-pv)
not pvresize --setphysicalvolumesize $new $dev13
# unchanged
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev9 pv_size $old
#check pv_field $dev11 pv_size $old
#check pv_field $dev14 pv_size $old

# one orphan and one dev (non-pv)
not pvresize --setphysicalvolumesize $new $dev14 $dev13
#check pv_field $dev14 pv_size $new
# unchanged
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev9 pv_size $old
#check pv_field $dev11 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9
pvresize --setphysicalvolumesize $old $dev11 $dev14

# one pv and one dev (non-pv)
not pvresize --setphysicalvolumesize $new $dev9 $dev13
#check pv_field $dev9 pv_size $new
# unchanged
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev11 pv_size $old
#check pv_field $dev14 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9
pvresize --setphysicalvolumesize $old $dev11 $dev14

# one tag and one dev (non-pv)
not pvresize --setphysicalvolumesize $new @V3D9 $dev13
#check pv_field $dev9 pv_size $new
# unchanged
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev11 pv_size $old
#check pv_field $dev14 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9
pvresize --setphysicalvolumesize $old $dev11 $dev14

# one pv, one orphan, one tag, one dev
not pvresize --setphysicalvolumesize $new @V3D9 $dev13 $dev14 $dev10
#check pv_field $dev9 pv_size $new
#check pv_field $dev10 pv_size $new
#check pv_field $dev14 pv_size $new
# unchanged
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev11 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9
pvresize --setphysicalvolumesize $old $dev11 $dev14



#
# tests including pvs without mdas
#

# remove old config
vgremove $vg1
vgremove $vg2
vgremove $vg3
pvremove $dev11
pvremove $dev14

# new config with some pvs that have zero mdas

# for vg1
pvcreate $dev10

# for vg2
pvcreate $dev2 --metadatacopies 0
pvcreate $dev3
pvcreate $dev4
pvcreate $dev5

# for vg3
pvcreate $dev6 --metadatacopies 0
pvcreate $dev7 --metadatacopies 0
pvcreate $dev8 --metadatacopies 0
pvcreate $dev9

# orphan with mda
pvcreate "$dev11"
# orphan without mda
pvcreate "$dev14" --metadatacopies 0

# non-pv devs
# dev12
# dev13

vgcreate $vg1 "$dev10"
vgcreate $vg2 "$dev2" "$dev3" "$dev4" "$dev5"
vgcreate $vg3 "$dev6" "$dev7" "$dev8" "$dev9"

pvchange --addtag V2D3 "$dev3"
pvchange --addtag V2D4 "$dev4"
pvchange --addtag V2D45 "$dev4"
pvchange --addtag V2D5 "$dev5"
pvchange --addtag V2D45 "$dev5"

pvchange --addtag V3 "$dev6" "$dev7" "$dev8" "$dev9"
pvchange --addtag V3D8 "$dev8"
pvchange --addtag V3D9 "$dev9"


#
# pvdisplay including pvs without mdas
#

# pv with mda
pvdisplay -s $dev10 >err
grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# pv without mda
pvdisplay -s $dev2 >err
not grep $dev10 err
grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# orphan with mda
pvdisplay -s $dev11 >err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# orphan without mda
pvdisplay -s $dev14 >err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
grep $dev14 err

# pv with mda, pv without mda, orphan with mda, orphan without mda
pvdisplay -s $dev10 $dev2 $dev11 $dev14 >err
grep $dev10 err
grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
grep $dev11 err
not grep $dev12 err
not grep $dev13 err
grep $dev14 err

# tag refering to pv with mda and pv without mda
pvdisplay -s @V3 >err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
grep $dev6 err
grep $dev7 err
grep $dev8 err
grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# tag refering to one pv without mda
pvdisplay -s @V3D8 >err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# all pvs (pvs in vgs and orphan pvs)
pvdisplay -s >err
grep $dev10 err
grep $dev2 err
grep $dev3 err
grep $dev4 err
grep $dev5 err
grep $dev6 err
grep $dev7 err
grep $dev8 err
grep $dev9 err
grep $dev11 err
not grep $dev12 err
not grep $dev13 err
grep $dev14 err

# all devs (pvs in vgs, orphan pvs, and devs)
pvdisplay -a -C >err
grep $dev10 err
grep $dev2 err
grep $dev3 err
grep $dev4 err
grep $dev5 err
grep $dev6 err
grep $dev7 err
grep $dev8 err
grep $dev9 err
grep $dev11 err
grep $dev12 err
grep $dev13 err
grep $dev14 err

#
# pvs including pvs without mdas
#

# pv with mda
pvs $dev10 >err
grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# pv without mda
pvs $dev2 >err
not grep $dev10 err
grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# orphan with mda
pvs $dev11 >err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# orphan without mda
pvs $dev14 >err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
grep $dev14 err

# pv with mda, pv without mda, orphan with mda, orphan without mda
pvs $dev10 $dev2 $dev11 $dev14 >err
grep $dev10 err
grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
not grep $dev8 err
not grep $dev9 err
grep $dev11 err
not grep $dev12 err
not grep $dev13 err
grep $dev14 err

# tag refering to pv with mda and pv without mda
pvs @V3 >err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
grep $dev6 err
grep $dev7 err
grep $dev8 err
grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# tag refering to one pv without mda
pvs @V3D8 >err
not grep $dev10 err
not grep $dev2 err
not grep $dev3 err
not grep $dev4 err
not grep $dev5 err
not grep $dev6 err
not grep $dev7 err
grep $dev8 err
not grep $dev9 err
not grep $dev11 err
not grep $dev12 err
not grep $dev13 err
not grep $dev14 err

# all pvs (pvs in vgs and orphan pvs)
pvs >err
grep $dev10 err
grep $dev2 err
grep $dev3 err
grep $dev4 err
grep $dev5 err
grep $dev6 err
grep $dev7 err
grep $dev8 err
grep $dev9 err
grep $dev11 err
not grep $dev12 err
not grep $dev13 err
grep $dev14 err

# all devs (pvs in vgs, orphan pvs, and devs)
pvs -a >err
grep $dev10 err
grep $dev2 err
grep $dev3 err
grep $dev4 err
grep $dev5 err
grep $dev6 err
grep $dev7 err
grep $dev8 err
grep $dev9 err
grep $dev11 err
grep $dev12 err
grep $dev13 err
grep $dev14 err


#
# vgreduce including pvs without mdas
#

# remove pv without mda
vgreduce $vg2 $dev2
not check pv_field $dev2 vg_name $vg2
check pv_field $dev3 vg_name $vg2
check pv_field $dev4 vg_name $vg2
check pv_field $dev5 vg_name $vg2
check pv_field $dev6 vg_name $vg3
check pv_field $dev7 vg_name $vg3
check pv_field $dev8 vg_name $vg3
check pv_field $dev9 vg_name $vg3
# reset
vgextend $vg2 $dev2

# remove pv with mda and pv without mda
vgreduce $vg2 $dev2 $dev3
not check pv_field $dev2 vg_name $vg2
not check pv_field $dev3 vg_name $vg2
check pv_field $dev4 vg_name $vg2
check pv_field $dev5 vg_name $vg2
check pv_field $dev6 vg_name $vg3
check pv_field $dev7 vg_name $vg3
check pv_field $dev8 vg_name $vg3
check pv_field $dev9 vg_name $vg3
# reset
vgextend $vg2 $dev2
vgextend $vg2 $dev3

# fail to remove only pv with mda
not vgreduce $vg3 $dev9
check pv_field $dev6 vg_name $vg3
check pv_field $dev7 vg_name $vg3
check pv_field $dev8 vg_name $vg3
check pv_field $dev9 vg_name $vg3
check pv_field $dev2 vg_name $vg2
check pv_field $dev3 vg_name $vg2
check pv_field $dev4 vg_name $vg2
check pv_field $dev5 vg_name $vg2

# remove by tag a pv without mda
vgreduce $vg3 @V3D8
check pv_field $dev6 vg_name $vg3
check pv_field $dev7 vg_name $vg3
not check pv_field $dev8 vg_name $vg3
check pv_field $dev9 vg_name $vg3
check pv_field $dev2 vg_name $vg2
check pv_field $dev3 vg_name $vg2
check pv_field $dev4 vg_name $vg2
check pv_field $dev5 vg_name $vg2
# reset
vgextend $vg3 $dev8


#
# pvresize including pvs without mdas
#


old="30.00m"
new="20.00m"

# use a common "old" size instead of getting the original size
# because orphan and non-orphan pv's may have different pv_size
# even if the devices are the same dev_size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9
pvresize --setphysicalvolumesize $old $dev11 $dev14
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev9 pv_size $old
#check pv_field $dev11 pv_size $old
#check pv_field $dev14 pv_size $old

# one pv without mda
pvresize --setphysicalvolumesize $new $dev2
#check pv_field $dev2 pv_size $new
# unchanged
#check pv_field $dev10 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev9 pv_size $old
#check pv_field $dev11 pv_size $old
#check pv_field $dev14 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9
pvresize --setphysicalvolumesize $old $dev11 $dev14

# two pvs without mdas
pvresize --setphysicalvolumesize $new $dev6 $dev7
#check pv_field $dev6 pv_size $new
#check pv_field $dev7 pv_size $new
# unchanged
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev9 pv_size $old
#check pv_field $dev11 pv_size $old
#check pv_field $dev14 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9
pvresize --setphysicalvolumesize $old $dev11 $dev14

# one pv with mda and one pv without mda
pvresize --setphysicalvolumesize $new $dev8 $dev9
#check pv_field $dev8 pv_size $new
#check pv_field $dev9 pv_size $new
# unchanged
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev11 pv_size $old
#check pv_field $dev14 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9
pvresize --setphysicalvolumesize $old $dev11 $dev14

# one orphan with mda
pvresize --setphysicalvolumesize $new $dev11
#check pv_field $dev11 pv_size $new
# unchanged
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev9 pv_size $old
#check pv_field $dev14 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9
pvresize --setphysicalvolumesize $old $dev11 $dev14

# one orphan without mda
pvresize --setphysicalvolumesize $new $dev14
#check pv_field $dev14 pv_size $new
# unchanged
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev9 pv_size $old
#check pv_field $dev11 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9
pvresize --setphysicalvolumesize $old $dev11 $dev14

# one orphan with mda and one orphan without mda
pvresize --setphysicalvolumesize $new $dev14 $dev11
#check pv_field $dev11 pv_size $new
#check pv_field $dev14 pv_size $new
# unchanged
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
#check pv_field $dev8 pv_size $old
#check pv_field $dev9 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9
pvresize --setphysicalvolumesize $old $dev11 $dev14

# one pv with mda and one pv without mda, and
# one orphan with mda and one orphan without mda
pvresize --setphysicalvolumesize $new $dev8 $dev9 $dev14 $dev11
#check pv_field $dev8 pv_size $new
#check pv_field $dev9 pv_size $new
#check pv_field $dev11 pv_size $new
#check pv_field $dev14 pv_size $new
# unchanged
#check pv_field $dev10 pv_size $old
#check pv_field $dev2 pv_size $old
#check pv_field $dev3 pv_size $old
#check pv_field $dev4 pv_size $old
#check pv_field $dev5 pv_size $old
#check pv_field $dev6 pv_size $old
#check pv_field $dev7 pv_size $old
# reset back to old size
pvresize --setphysicalvolumesize $old $dev10 $dev2 $dev3 $dev4 $dev5 $dev6 $dev7 $dev8 $dev9
pvresize --setphysicalvolumesize $old $dev11 $dev14


