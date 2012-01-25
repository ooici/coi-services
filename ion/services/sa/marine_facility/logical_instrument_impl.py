#!/usr/bin/env python

"""
@package  ion.services.sa.marine_facility_management.logical_instrument_impl
@author   Ian Katz
"""


#from pyon.core.exception import BadRequest, NotFound
from pyon.public import PRED, RT

from ion.services.sa.marine_facility.mfms_simple import MFMSsimple

class LogicalInstrumentImpl(MFMSsimple):
    """
    @brief resource management for LogicalInstrument resources
    """

    def _primary_object_name(self):
        return RT.LogicalInstrument

    def _primary_object_label(self):
        return "logical_instrument"

    def link_agent(self, logical_instrument_id='', instrument_agent_id=''):
        return self._link_resources(logical_instrument_id, PRED.hasAgent, instrument_agent_id)

    def unlink_agent(self, logical_instrument_id='', instrument_agent_id=''):
        return self._unlink_resources(logical_instrument_id, PRED.hasAgent, instrument_agent_id)

    def find_having_agent(self, instrument_agent_id):
        return self._find_having(PRED.hasAgent, instrument_agent_id)

    def find_stemming_agent(self, logical_instrument_id):
        return self._find_stemming(logical_instrument_id, PRED.hasAgent, RT.InstrumentAgent)
