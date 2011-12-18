#!/usr/bin/env python

__author__ = 'Ian Katz'
__license__ = 'Apache 2.0'

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import RT

from ion.services.sa.instrument_management.ims_simple import IMSsimple

class PlatformAgentInstanceWorker(IMSsimple):

    def _primary_object_name(self):
        return RT.PlatformAgentInstance

    def _primary_object_label(self):
        return "platform_agent_instance"
