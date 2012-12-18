#!/bin/bash
#
# This script is a shortcut for clearing the couch db
#  COI-Services and its dependencies
# 
# Author: Ian Katz
#

# go to root of repository
THISDIR=$(git rev-parse --show-toplevel)
THISSCRIPT=$(basename $0)

cd $THISDIR

echo -e "Force-cleaning CouchDB"
bin/pycc -X -fc

