#!/usr/bin/env python

__author__ = 'Ian Katz'
__license__ = 'Apache 2.0'

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import AT, RT

from ion.services.sa.instrument_management.ims_simple import IMSsimple

class PlatformAgentWorker(IMSsimple):

    def _primary_object_name(self):
        return RT.PlatformAgent

    def _primary_object_label(self):
        return "platform_agent"

    def link_instance(self, platform_agent_id='', platform_agent_instance_id=''):
        return self.link_resources(platform_agent_id, AT.hasInstance, platform_agent_instance_id)

    def unlink_instance(self, platform_agent_id='', platform_agent_instance_id=''):
        return self.unlink_resources(platform_agent_id, AT.hasInstance, platform_agent_instance_id)

    def link_model(self, platform_agent_id='', platform_model_id=''):
        return self.link_resources(platform_agent_id, AT.hasModel, platform_model_id)

    def unlink_model(self, platform_agent_id='', platform_model_id=''):
        return self.unlink_resources(platform_agent_id, AT.hasModel, platform_model_id)
