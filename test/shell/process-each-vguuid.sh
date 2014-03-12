#!/bin/sh
# Copyright (C) 2008-2013 Red Hat, Inc. All rights reserved.
#
# This copyrighted material is made available to anyone wishing to use,
# modify, copy, or redistribute it subject to the terms and conditions
# of the GNU General Public License v.2.

test_description='Test vg uuids as command args to process_each_vg commands'

# TODO: test uuid args with other process_each_vg commands beyond vgs

. lib/test

aux prepare_devs 7

pvcreate $dev1
pvcreate $dev2
pvcreate $dev3

vgcreate $vg1 $dev1
UUID1=$(vgs --noheading -o vg_uuid $vg1 | sed s/' '//g)

vgcreate $vg2 $dev2
UUID2=$(vgs --noheading -o vg_uuid $vg2 | sed s/' '//g)

vgcreate $vg3 $dev3
UUID3=$(vgs --noheading -o vg_uuid $vg3 | sed s/' '//g)

vgs -o+vg_uuid >err
cat err
grep $vg1 err
grep $vg2 err
grep $vg3 err
grep $UUID1 err
grep $UUID2 err
grep $UUID3 err

vgs -o+vg_uuid $vg1 >err
grep $vg1 err
grep $UUID1 err
not grep $vg2 err
not grep $UUID2 err
not grep $vg3 err
not grep $UUID3 err

vgs -o+vg_uuid $UUID1 >err
grep $vg1 err
grep $UUID1 err
not grep $vg2 err
not grep $UUID2 err
not grep $vg3 err
not grep $UUID3 err

vgs -o+vg_uuid $vg2 >err
grep $vg2 err
grep $UUID2 err
not grep $vg1 err
not grep $UUID1 err
not grep $vg3 err
not grep $UUID3 err

vgs -o+vg_uuid $UUID2 >err
grep $vg2 err
grep $UUID2 err
not grep $vg1 err
not grep $UUID1 err
not grep $vg3 err
not grep $UUID3 err

vgs -o+vg_uuid $UUID1 $UUID2 >err
grep $vg1 err
grep $vg2 err
grep $UUID1 err
grep $UUID2 err
not grep $vg3 err
not grep $UUID3 err

UUID1B=$(echo $UUID1 | sed s/-//g)
vgs -o+vg_uuid $UUID1B >err
grep $vg1 err
grep $UUID1 err
not grep $UUID1B err
not grep $vg2 err
not grep $UUID2 err
not grep $vg3 err
not grep $UUID3 err


# create a vg with a uuid for a name

NAME4=`uuidgen`
vgcreate $NAME4 $dev4
UUID4=$(vgs --noheading -o vg_uuid $NAME4 | sed s/' '//g)
UUID4B=$(echo $UUID4 | sed s/-//g)

vgs -o+vg_uuid >err
grep $vg1 err
grep $vg2 err
grep $vg3 err
grep $NAME4 err
grep $UUID1 err
grep $UUID2 err
grep $UUID3 err
grep $UUID4 err

vgs -o+vg_uuid $NAME4 >err
cat err
not grep $vg1 err
not grep $vg2 err
not grep $vg3 err
grep $NAME4 err
not grep $UUID1 err
not grep $UUID2 err
not grep $UUID3 err
grep $UUID4 err

vgs -o+vg_uuid $UUID4 >err
cat err
not grep $vg1 err
not grep $vg2 err
not grep $vg3 err
grep $NAME4 err
not grep $UUID1 err
not grep $UUID2 err
not grep $UUID3 err
grep $UUID4 err

# after the vg is selected with NAME4, no vg is found to match UUID4
not vgs -o name,vg_uuid --separator ',' $NAME4 $UUID4 | sed s/' '//g >err
cat err
not grep $vg1 err
not grep $vg2 err
not grep $vg3 err
grep $NAME4,$UUID4 err
not grep $UUID1 err
not grep $UUID2 err
not grep $UUID3 err

vgs -o+vg_uuid $UUID4B >err
cat err
not grep $vg1 err
not grep $vg2 err
not grep $vg3 err
grep $NAME4 err
not grep $UUID1 err
not grep $UUID2 err
not grep $UUID3 err
grep $UUID4 err
not grep $UUID4B err

vgs -o+vg_uuid $vg3 $UUID4 >err
cat err
not grep $vg1 err
not grep $vg2 err
grep $vg3 err
grep $NAME4 err
not grep $UUID1 err
not grep $UUID2 err
grep $UUID3 err
grep $UUID4 err

vgs -o+vg_uuid $UUID3 $UUID4 >err
cat err
not grep $vg1 err
not grep $vg2 err
grep $vg3 err
grep $NAME4 err
not grep $UUID1 err
not grep $UUID2 err
grep $UUID3 err
grep $UUID4 err


# create a vg with a name from the uuid of an existing vg

NAME5=$vg5

vgcreate $NAME5 $dev5
UUID5=$(vgs --noheading -o vg_uuid $NAME5 | sed s/' '//g)

NAME6=$UUID5
vgcreate $NAME6 $dev6

# get UUID6 for NAME6 while avoiding confusion with NAME5/UUID5
vgs --noheading -o name,vg_uuid > lines.0
cat lines.0
grep -v $vg1 lines.0 > lines.1
grep -v $vg2 lines.1 > lines.2
grep -v $vg3 lines.2 > lines.3
grep -v $NAME4 lines.3 > lines.4
grep -v $NAME5 lines.4 > lines.5
cat lines.5
UUID6=$(cat lines.5 | awk '{print $2}')

echo NAME4 is $NAME4
echo UUID4 is $UUID4
echo NAME5 is $NAME5
echo UUID5 is $UUID5
echo NAME6 is $NAME6
echo UUID6 is $UUID6

vgs >err
cat err
grep $vg1 err
grep $vg2 err
grep $vg3 err
grep $NAME4 err
grep $NAME5 err
grep $NAME6 err

vgs -o vg_uuid >err
cat err
grep $UUID1 err
grep $UUID2 err
grep $UUID3 err
grep $UUID4 err
grep $UUID5 err
grep $UUID6 err

vgs -o name,vg_uuid --separator ',' | sed s/' '//g >err
cat err
grep $vg1,$UUID1 err
grep $vg2,$UUID2 err
grep $vg3,$UUID3 err
grep $NAME4,$UUID4 err
grep $NAME5,$UUID5 err
grep $NAME6,$UUID6 err

# If a uuid string matches one vg's name and
# another vg's uuid, then select the vg with
# the matching name.
#
# name,uuid
# X,Y
# Y,Z
# vgs Y shows Y,Z
#
# NAME5=X
# UUID5=Y
# NAME6=Y
# UUID6=Z

vgs -o name,vg_uuid --separator ',' $NAME5 | sed s/' '//g >err
cat err
not grep $vg1,$UUID1 err
not grep $vg2,$UUID2 err
not grep $vg3,$UUID3 err
not grep $NAME4,$UUID4 err
grep $NAME5,$UUID5 err
not grep $NAME6,$UUID6 err

vgs -o name,vg_uuid --separator ',' $NAME6 | sed s/' '//g >err
cat err
not grep $vg1,$UUID1 err
not grep $vg2,$UUID2 err
not grep $vg3,$UUID3 err
not grep $NAME4,$UUID4 err
not grep $NAME5,$UUID5 err
grep $NAME6,$UUID6 err

vgs -o name,vg_uuid --separator ',' $UUID5 | sed s/' '//g >err
cat err
not grep $vg1,$UUID1 err
not grep $vg2,$UUID2 err
not grep $vg3,$UUID3 err
not grep $NAME4,$UUID4 err
not grep $NAME5,$UUID5 err
grep $NAME6,$UUID6 err

vgs -o name,vg_uuid --separator ',' $UUID6 | sed s/' '//g >err
cat err
not grep $vg1,$UUID1 err
not grep $vg2,$UUID2 err
not grep $vg3,$UUID3 err
not grep $NAME4,$UUID4 err
not grep $NAME5,$UUID5 err
grep $NAME6,$UUID6 err

vgs -o name --separator ',' $NAME5 $NAME6 | sed s/' '//g >err
cat err
not grep $vg1 err
not grep $vg2 err
not grep $vg3 err
not grep $NAME4 err
grep $NAME5 err
grep $NAME6 err

# UUID5 and NAME6 are the same, and the command line processing
# of vg names removes duplicate names, so only one is processed.
# NAME6 is selected because uuids match vg names before vg uuids.
vgs -o name --separator ',' $UUID5 $NAME6 | sed s/' '//g >err
cat err
not grep $vg1 err
not grep $vg2 err
not grep $vg3 err
not grep $NAME4 err
not grep $NAME5 err
grep $NAME6 err

# UUID5 selects the vg with NAME6, then UUID6 matches no vgs
not vgs -o name --separator ',' $UUID5 $UUID6 | sed s/' '//g >err
cat err
not grep $vg1 err
not grep $vg2 err
not grep $vg3 err
not grep $NAME4 err
not grep $NAME5 err
grep $NAME6 err


# vg name is a uuid without dashes

NAME7=`uuidgen`
NAME7B=$(echo $NAME7 | sed s/-//g)
vgcreate $NAME7B $dev7
UUID7=$(vgs --noheading -o vg_uuid $NAME7B | sed s/' '//g)
UUID7B=$(echo $UUID7 | sed s/-//g)

vgs -o name $NAME7 >err
cat err
grep $NAME7B err
not grep $NAME7 err

vgs -o name $NAME7B >err
cat err
grep $NAME7B err
not grep $NAME7 err

vgs -o vg_uuid $UUID7 >err
cat err
grep $UUID7 err
not grep $UUID7B err

vgs -o vg_uuid $UUID7B >err
cat err
grep $UUID7 err
not grep $UUID7B err


# reporting unknown uuid should produce an error
#
# uuidgen and vgs put dashes in difference
# locations in the uuid string, so we can't
# grep for the full not found error string.

UUIDBAD=`uuidgen`
not vgs $UUIDBAD 2>err
cat err
grep "not found" err

