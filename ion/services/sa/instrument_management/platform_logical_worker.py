#!/usr/bin/env python

__author__ = 'Ian Katz'
__license__ = 'Apache 2.0'

#from pyon.core.exception import BadRequest, NotFound

from ion.services.sa.instrument_management.ims_simple import IMSsimple

class PlatformLogicalWorker(IMSsimple):

    def _primary_object_name(self):
        return "PlatformLogical"

    def _primary_object_label(self):
        return "platform_logical"

