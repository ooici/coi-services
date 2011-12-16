#!/usr/bin/env python

__author__ = 'Ian Katz'
__license__ = 'Apache 2.0'

#from pyon.core.exception import BadRequest, NotFound

from ion.services.sa.instrument_management.ims_simple import IMSsimple

class PlatformAgentWorker(IMSsimple):

    def _primary_object_name(self):
        return "PlatformAgent"

    def _primary_object_label(self):
        return "platform_agent"

    def assign(self, platform_agent_id='', platform_device_id='', platform_agent_instance=''):
        raise NotImplementedError()

    def unassign(self, platform_agent_id='', platform_device_id='', platform_agent_instance=''):
        raise NotImplementedError()
