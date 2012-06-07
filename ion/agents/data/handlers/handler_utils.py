#!/usr/bin/env python

"""
@package 
@file handler_utils
@author Christopher Mueller
@brief 
"""
from pyon.public import log
from pyon.util.containers import get_safe
import glob, os, re
import requests
from ftplib import FTP
from StringIO import StringIO

def _get_type(base):
    if base.startswith('http://'):
        type = 'http'
    elif base.startswith('ftp://'):
        type = 'ftp'
    else:
        type = 'fs'

    return type

def list_file_info(base, pattern, name_index=0, type=None):
    '''
    Constructs a list of tuples containing information about the files as indicated by the pattern.
    The name_index should correspond to the index in the resulting tuple that contains the name of the file, default is 0
    '''
    
    # If type isn't specified, attempt to determine based on base
    type = type or _get_type(base)

    # Switch on type
    if type is 'http':
        lst = list_file_info_http(base, pattern, name_index)
    elif type is 'ftp':
        lst = list_file_info_ftp(base, pattern)
    elif type is 'fs':
        lst = list_file_info_fs(base, pattern)
    else:
        log.warn('Unknown type specified: {0}'.format(type))
        lst = []

    return lst

def list_file_info_http(base, pattern, name_index=0):
    response = requests.get(base)
    base_url = response.url
    flst = re.findall(pattern, response.content)
    olst = []
    for f in flst:
        if not isinstance(f, tuple):
            f = (f,)
        lst = []
        for i in xrange(len(f)):
            if i is name_index:
                lst.append(base_url+f[name_index])
            else:
                lst.append(f[i])

        olst.append(tuple(lst))

    return olst

def list_file_info_ftp(base, pattern):
    raise NotImplementedError

def list_file_info_fs(base, pattern):
    if not os.path.exists(base):
        raise StandardError('base \'{0}\' does not exist')
    if not os.path.isdir(base):
        raise StandardError('base \'{0}\' is not a directory'.format(base))
    flst = glob.glob(base + '/' + pattern)
    olst = []
    for f in flst:
        olst.append((f,os.path.getmtime(f),os.path.getsize(f)))

    return olst

#TODO:  IMPROVE - Function similar to above that reads a file to a StringIO from http, ftp, or fs
#VERY BASIC
def get_sbuffer(url, type=None):
    # If type isn't specified, attempt to determine based on url
    type = type or _get_type(url)

    # Switch on type
    if type is 'http':
        response = requests.get(url)
        buf = StringIO(response.content)
    elif type is 'ftp':
        raise NotImplementedError
    elif type is 'fs':
        buf = StringIO(open(url).read())
    else:
        log.warn('Unknown type specified: {0}'.format(type))
        buf = None

    return buf


"""
from ion.agents.data.handlers.handler_utils import *

# A couple of http tries
# full information
full_info=list_file_info('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/','<a href="([^"]*\.ruv)">.*(\d{2}-[a-zA-Z]{3}-\d{4} \d{2}:\d{2})\s*(\d{3,5}\w)')

# Just the names
names_only = list_file_info('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/',pattern='<a href="([^"]*\.ruv)">')


# A couple of filesystem tries (always full info for filesystem)
fs1 = list_file_info('test_data','*.nc')
fs2 = list_file_info('test_data/slocum',pattern='ru05-*-sbd.dat')


# SITE test - for DataSourceHandler!!!!
site_info = list_file_info('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/',pattern='<a href="([^"]*.*)">.*(\d{2}-[a-zA-Z]{3}-\d{4} \d{2}:\d{2})\s*(-)')

"""