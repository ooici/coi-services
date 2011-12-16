#!/usr/bin/env python

__author__ = 'Ian Katz'
__license__ = 'Apache 2.0'

#from pyon.core.exception import BadRequest, NotFound

from ion.services.sa.instrument_management.ims_worker import IMSworker

class PlatformDeviceWorker(IMSworker):

    def _primary_object_name(self):
        return "PlatformDevice"

    def _primary_object_label(self):
        return "platform_device"

    def link_agent_instance(self, platform_device_id='', platform_agent_instance_id=''):
        raise NotImplementedError()

    def unlink_agent_instance(self, platform_device_id='', platform_agent_instance_id=''):
        raise NotImplementedError()

    def link_assignment(self, platform_device_id='', logical_platform_id=''):
        raise NotImplementedError()

    def unlink_assignment(self, platform_device_id='', logical_platform_id=''):
        raise NotImplementedError()

    def link_model(self, platform_device_id='', platform_model_id=''):
        raise NotImplementedError()

    def unlink_model(self, platform_device_id='', platform_model_id=''):
        raise NotImplementedError()

    def link_instrument(self, platform_device_id='', instrument_device_id=''):
        raise NotImplementedError()

    def unlink_instrument(self, platform_device_id='', instrument_device_id=''):
        raise NotImplementedError()


