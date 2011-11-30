#!/bin/bash
#
# This script attempts to completely update and initialize
#  COI-Services and its dependencies
# 
# Author: Ian Katz
#

# go to root of repository
THISDIR=$(git rev-parse --show-toplevel)
cd $THISDIR

echo "\n\n=== UPDATING PYON ===\n"
cd ../pyon
git pull --rebase

echo "\n\n=== UPDATING COI-SERVICES ===\n"
cd $THISDIR
git pull --rebase

echo "\n\n=== UPDATING COI-SERVICES SUBMODULE(S) ===\n"
cd extern/ion-definitions
git checkout master
if [ $? -ne 0 ]; then
    echo "\n$(basename $0) aborting due to inability to switch branches"
    exit 1
fi
git pull --rebase origin master
cd $THISDIR
git submodule update

echo "\n\n=== CLEANING UP ===\n"
ant clean
sh scripts/cc-cleanup.sh

echo "\n\n=== BOOTSTRAPPING ===\n"
python bootstrap.py

echo "\n\n=== BUILDING OUT ===\n"
bin/buildout

echo "\n\n=== GENERATING INTERFACES ===\n"
bin/generate_interfaces --force
