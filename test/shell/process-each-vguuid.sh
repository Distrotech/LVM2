#!/bin/sh
# Copyright (C) 2008-2013 Red Hat, Inc. All rights reserved.
#
# This copyrighted material is made available to anyone wishing to use,
# modify, copy, or redistribute it subject to the terms and conditions
# of the GNU General Public License v.2.

test_description='Test vg uuids as command args to process_each_vg commands'

. lib/test

aux prepare_devs 3

pvcreate $dev1
pvcreate $dev2
pvcreate $dev3

vgcreate $vg1 $dev1
UUID1=$(vgs --noheading -o vg_uuid $vg1)

vgcreate $vg2 $dev2
UUID2=$(vgs --noheading -o vg_uuid $vg2)

vgcreate $vg3 $dev3
UUID3=$(vgs --noheading -o vg_uuid $vg3)

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

