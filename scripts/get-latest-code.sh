#!/bin/bash
#
# This script attempts to completely update and initialize
#  COI-Services and its dependencies
# 
# Author: Ian Katz
#

# go to root of repository
THISDIR=$(git rev-parse --show-toplevel)
THISSCRIPT=$(basename $0)

get_submodule ()
{
    echo -e "\n\n=== UPDATING COI-SERVICES SUBMODULE ($1) ===\n"
 
    cd "$1"

    git checkout master
    if [ $? -ne 0 ]; then
        git status
        echo -e "\n$THISSCRIPT aborting due to inability to switch branches"
        exit 1
    fi
    git pull --rebase origin master

    cd $THISDIR
}


cd $THISDIR

if [ -r "scripts/get-latest-code-local-hooks.sh" ]; then
    echo -e "\n\n=== EXECUTING LOCAL HOOK ===\n"
    sh scripts/get-latest-code-local-hooks.sh
else
    echo -e "\n\nLooked for a script named scripts/get-latest-code-local-hooks.sh"
    echo -e "  (containing your local pre-update commands) but it didn't exist."
fi

#echo -e "\n\n=== UPDATING COI-SERVICES SUBMODULE(S) ===\n"
get_submodule extern/pyon
get_submodule extern/ion-definitions
get_submodule extern/marine-integrations
get_submodule extern/coverage-model

echo -e "\n\n=== UPDATING COI-SERVICES ===\n"
git pull --rebase
if [ $? -ne 0 ]; then
    git status
    echo -e "\n$(basename $0) aborting because pull failed (probably have unstashed changes)"
    exit 1
fi
git submodule update

echo -e "\n\n=== CLEANING UP ===\n"
ant clean
sh scripts/cc-cleanup.sh

echo -e "\n\n=== BOOTSTRAPPING ===\n"
python bootstrap.py

echo -e "\n\n=== BUILDING OUT ===\n"
bin/buildout

echo -e "\n\n=== GENERATING INTERFACES ===\n"
bin/generate_interfaces --force


echo -e "\n\n=== DONE  Printing git stashes: ===\n"
git stash list
