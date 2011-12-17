#!/usr/bin/env python

__author__ = 'Ian Katz'
__license__ = 'Apache 2.0'

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import AT


from ion.services.sa.instrument_management.ims_simple import IMSsimple

class InstrumentAgentWorker(IMSsimple):

    def _primary_object_name(self):
        return "InstrumentAgent"

    def _primary_object_label(self):
        return "instrument_agent"

    def link_instance(self, instrument_agent_id='', instrument_agent_instance_id=''):
        return self.link_resources(instrument_agent_id, AT.hasInstance, instrument_agent_instance_id)

    def unlink_instance(self, instrument_agent_id='', instrument_agent_instance_id=''):
        return self.unlink_resources(instrument_agent_id, AT.hasInstance, instrument_agent_instance_id)

    def link_model(self, instrument_agent_id='', instrument_model_id=''):
        return self.link_resources(instrument_agent_id, AT.hasInstance, instrument_model_id)

    def unlink_model(self, instrument_agent_id='', instrument_model_id=''):
        return self.unlink_resources(instrument_agent_id, AT.hasInstance, instrument_model_id)

