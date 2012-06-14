#!/bin/bash

# Author: Luke Campbell <LCampbell@ASAScience.com>

VERSION="0.19.4"
ES="elasticsearch-$VERSION"
ES_GZ="$ES.tar.gz"
URL="https://github.com/downloads/elasticsearch/elasticsearch/$ES_GZ"
if [[ ! ( -n $INSTALL_DIR && -d $INSTALL_DIR ) ]]; then INSTALL_DIR="/usr/local"; fi
ES_ROOT=$INSTALL_DIR/$ES

JS_LANG_PLUGIN="elasticsearch/elasticsearch-lang-javascript/1.1.0"
COUCHDB_RIVER_PLUGIN="elasticsearch/elasticsearch-river-couchdb/1.1.0"
HEAD_PLUGIN="mobz/elasticsearch-head"
BIGDESK_PLUGIN="lukas-vlcek/bigdesk"

USAGE=" bash install_es.sh [-h] [-b] [-u] \
    -h Installs Head plugin which provides a visual representation of the ElasticSearch engine\
    -b Installs BigDesk plugin which provides real-time monitoring of the cluster\
    -u Uninstalls ElasticSearch"
    

args=`getopt hbu $*`
if [[ $? -ne 0 ]]; then
    echo $USAGE
fi
set -- $args
for i
do
    case "$i"
    in
        -h)
            INSTALL_HEAD=1
            ;;
        -b)
            INSTALL_BIGDESK=1
            ;;
        -u)
            UNINSTALL=1
            ;;
    esac
done


if [[ -n $UNINSTALL ]]; then
    if [[ -d $ES_ROOT ]]; then
        rm -rf $ES_ROOT && echo "ElasticSearch uninstalled"
        exit 0
    else
        echo "No installation of ElasticSearch found"
        exit 1
    fi
fi

#--------------------------------------------------------------------------------
# If ES is not installed to ES_ROOT, download it
# Determine which utility to download with
#--------------------------------------------------------------------------------
if [[ ! -d $ES_ROOT ]]; then
    #----------------------------------------------------------------------------
    # If the tarball doesn't exist download it
    #----------------------------------------------------------------------------
    if [[ ! -e $ES_GZ ]]; then
        WGET=`which wget`
        if [[ $? -ne 0 ]]; then
            unset WGET
        fi

        # If there's no wget try curl
        CURL=`which curl`
        if [[ $? -ne 0 ]]; then
            echo "No utility to download elasticsearch.  Please install either wget or curl"
            exit 1
        fi


        if [[ -n $WGET ]]; then
            $WGET $URL
        else
            $CURL -XGET -L $URL -o $ES_GZ
        fi
    fi
#--------------------------------------------------------------------------------
# Install ElasticSearch and plugins
#--------------------------------------------------------------------------------

    tar -zxvf $ES_GZ -C $INSTALL_DIR
    rm -rf $ES_GZ
fi
$ES_ROOT/bin/plugin -install $JS_LANG_PLUGIN
$ES_ROOT/bin/plugin -install $COUCHDB_RIVER_PLUGIN

if [[ -n $INSTALL_HEAD ]]; then
    $ES_ROOT/bin/plugin -install $HEAD_PLUGIN
fi

if [[ -n $INSTALL_BIGDESK ]]; then
    $ES_ROOT/bin/plugin -install $BIGDESK_PLUGIN
fi
