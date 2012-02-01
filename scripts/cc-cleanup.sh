#!/bin/bash
#
# This script removes old capability container lockfiles
# 
# Author: Ian Katz
#

# go to root of repository
THISDIR=$(git rev-parse --show-toplevel)
cd $THISDIR

echo -e "\nRemoving old CC lockfiles in $THISDIR"

#get all lockfiles in the root
for CL in $(ls cc-pid-* 2>/dev/null)
do
    #extract the PID
    PID=$(echo $CL | awk -F"-" '{print $3}')
    PROCDIR="/proc/$PID/status"

    #check whether proc is currently running (and owned by us)
    if [ -O $PROCDIR ];
    then    
        echo "! $CL -- Skipping; process appears to be running"
    else
        echo "- $CL"
        rm $CL
    fi
        
done
