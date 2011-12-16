#!/usr/bin/env python

__author__ = 'Ian Katz'
__license__ = 'Apache 2.0'


class DirectAccess(object):
    """
    Interface class for direct access code 
    """

    def request(self, request_params={}):
        pass

    def stop(self, stop_params={}):
        pass

