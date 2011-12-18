#!/usr/bin/env python

__author__ = 'Ian Katz'
__license__ = 'Apache 2.0'

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import AT, RT

from ion.services.sa.instrument_management.ims_worker import IMSworker

class PlatformDeviceWorker(IMSworker):

    def _primary_object_name(self):
        return RT.PlatformDevice

    def _primary_object_label(self):
        return "platform_device"

    def link_agent_instance(self, platform_device_id='', platform_agent_instance_id=''):
        return self.link_resources(platform_device_id, AT.hasAgentInstance, platform_agent_instance_id)

    def unlink_agent_instance(self, platform_device_id='', platform_agent_instance_id=''):
        return self.unlink_resources(platform_device_id, AT.hasAgentInstance, platform_agent_instance_id)

    def link_assignment(self, platform_device_id='', logical_platform_id=''):
        return self.link_resources(platform_device_id, AT.hasAssignment, logical_platform_id)

    def unlink_assignment(self, platform_device_id='', logical_platform_id=''):
        return self.unlink_resources(platform_device_id, AT.hasAssignment, logical_platform_id)

    def link_model(self, platform_device_id='', platform_model_id=''):
        return self.link_resources(platform_device_id, AT.hasModel, platform_model_id)

    def unlink_model(self, platform_device_id='', platform_model_id=''):
        return self.unlink_resources(platform_device_id, AT.hasModel, platform_model_id)

    def link_instrument(self, platform_device_id='', instrument_device_id=''):
        return self.link_resources(platform_device_id, AT.hasInstrument, instrument_device_id)

    def unlink_instrument(self, platform_device_id='', instrument_device_id=''):
        return self.unlink_resources(platform_device_id, AT.hasInstrument, instrument_device_id)


