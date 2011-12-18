#!/usr/bin/env python

__author__ = 'Ian Katz'
__license__ = 'Apache 2.0'

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import AT, RT

from ion.services.sa.instrument_management.ims_simple import IMSsimple

class LogicalInstrumentWorker(IMSsimple):

    def _primary_object_name(self):
        return RT.LogicalInstrument

    def _primary_object_label(self):
        return "logical_instrument"

    def link_agent(self, logical_instrument_id='', instrument_agent_id=''):
        return self.link_resources(logical_instrument_id, AT.hasAgent, instrument_agent_id)

    def unlink_agent(self, logical_instrument_id='', instrument_agent_id=''):
        return self.unlink_resources(logical_instrument_id, AT.hasAgent, instrument_agent_id)

    def find_having_agent(self, instrument_agent_id):
        return self._find_having(AT.hasAgent, instrument_agent_id)

    def find_stemming_agent(self, logical_instrument_id):
        return self._find_stemming(logical_instrument_id, AT.hasAgent, RT.InstrumentAgent)
