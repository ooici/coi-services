#!/usr/bin/env python

__author__ = 'Bill Bollenbacher'
__license__ = 'Apache 2.0'

from pyon.util.log import log

class DirectAccessServer(object):
    """
    Class for direct access server that interfaces to an IA ResourceAgent
    """

    def on_start(self):
        log.debug("DirectAccessServer: on_start()")
