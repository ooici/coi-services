#!/bin/bash
#
# This script removes old capability container lockfiles
# 
# Author: Ian Katz
#

# go to root of repository
THISDIR=$(git rev-parse --show-toplevel)
cd $THISDIR

echo "\nRemoving old pyc files in $THISDIR"

#get all pyc in the root
for PYC in $(find ion | grep \.pyc$)
do
    echo "\t$PYC"
    rm $PYC
done
