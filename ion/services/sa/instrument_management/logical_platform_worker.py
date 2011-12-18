#!/usr/bin/env python

__author__ = 'Ian Katz'
__license__ = 'Apache 2.0'

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import AT, RT

from ion.services.sa.instrument_management.ims_simple import IMSsimple

class LogicalPlatformWorker(IMSsimple):

    def _primary_object_name(self):
        return RT.LogicalPlatform

    def _primary_object_label(self):
        return "logical_platform"

    def link_agent(self, logical_platform_id='', platform_agent_id=''):
        return self.link_resources(logical_platform_id, AT.hasAgent, platform_agent_id)

    def unlink_agent(self, logical_platform_id='', platform_agent_id=''):
        return self.unlink_resources(logical_platform_id, AT.hasAgent, platform_agent_id)

    def link_instrument(self, logical_platform_id='', logical_instrument_id=''):
        return self.link_resources(logical_platform_id, AT.hasInstrument, logical_instrument_id)

    def unlink_instrument(self, logical_platform_id='', logical_instrument_id=''):
        return self.unlink_resources(logical_platform_id, AT.hasInstrument, logical_instrument_id)

    def link_platform(self, logical_platform_id='', logical_platform_child_id=''):
        return self.link_resources(logical_platform_id, AT.hasPlatform, logical_platform_child_id)

    def unlink_platform(self, logical_platform_id='', logical_platform_child_id=''):
        return self.unlink_resources(logical_platform_id, AT.hasPlatform, logical_platform_child_id)

