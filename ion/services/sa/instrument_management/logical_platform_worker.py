#!/usr/bin/env python

__author__ = 'Ian Katz'
__license__ = 'Apache 2.0'

#from pyon.core.exception import BadRequest, NotFound

from ion.services.sa.instrument_management.ims_simple import IMSsimple

class LogicalPlatformWorker(IMSsimple):

    def _primary_object_name(self):
        return "LogicalPlatform"

    def _primary_object_label(self):
        return "logical_platform"

    def link_agent(self, logical_platform_id='', platform_agent_id=''):
        raise NotImplementedError()

    def unlink_agent(self, logical_platform_id='', platform_agent_id=''):
        raise NotImplementedError()

    def link_instrument(self, logical_platform_id='', logical_instrument_id=''):
        raise NotImplementedError()

    def unlink_instrument(self, logical_platform_id='', logical_instrument_id=''):
        raise NotImplementedError()

    def link_platform(self, logical_platform_id='', logical_platform_child_id=''):
        raise NotImplementedError()

    def unlink_platform(self, logical_platform_id='', logical_platform_child_id=''):
        raise NotImplementedError()

