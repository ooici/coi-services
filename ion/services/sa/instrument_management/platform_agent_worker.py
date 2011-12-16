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

    def link_instance(self, platform_agent_id='', platform_agent_instance=''):
        raise NotImplementedError()

    def unlink_instance(self, platform_agent_id='', platform_agent_instance=''):
        raise NotImplementedError()

    def link_model(self, platform_agent_id='', platform_model_id=''):
        raise NotImplementedError()

    def unlink_model(self, platform_agent_id='', platform_model_id=''):
        raise NotImplementedError()

