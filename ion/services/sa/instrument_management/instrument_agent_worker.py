#!/usr/bin/env python

__author__ = 'Ian Katz'
__license__ = 'Apache 2.0'

#from pyon.core.exception import BadRequest, NotFound

from ion.services.sa.instrument_management.ims_simple import IMSsimple

class InstrumentAgentWorker(IMSsimple):

    def _primary_object_name(self):
        return "InstrumentAgent"

    def _primary_object_label(self):
        return "instrument_agent"

    def assign(self, instrument_agent_id='', instrument_device_id='', instrument_agent_instance=''):
        raise NotImplementedError()

    def unassign(self, instrument_agent_id='', instrument_device_id='', instrument_agent_instance=''):
        raise NotImplementedError()
