#!/usr/bin/env python

"""
@package  ion.services.sa.instrument.instrument_agent_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import PRED, RT


from ion.services.sa.instrument.ims_simple import IMSsimple

class InstrumentAgentImpl(IMSsimple):
    """
    @brief Resource management for InstrumentAgent resources
    """

    def _primary_object_name(self):
        return RT.InstrumentAgent

    def _primary_object_label(self):
        return "instrument_agent"

    def link_instance(self, instrument_agent_id='', instrument_agent_instance_id=''):
        return self._link_resources(instrument_agent_id, PRED.hasInstance, instrument_agent_instance_id)

    def unlink_instance(self, instrument_agent_id='', instrument_agent_instance_id=''):
        return self._unlink_resources(instrument_agent_id, PRED.hasInstance, instrument_agent_instance_id)

    def link_model(self, instrument_agent_id='', instrument_model_id=''):
        return self._link_resources(instrument_agent_id, PRED.hasModel, instrument_model_id)

    def unlink_model(self, instrument_agent_id='', instrument_model_id=''):
        return self._unlink_resources(instrument_agent_id, PRED.hasModel, instrument_model_id)

    def find_having_instance(self, instrument_agent_instance_id):
        return self._find_having(PRED.hasInstance, instrument_agent_instance_id)

    def find_stemming_instance(self, instrument_agent_id):
        return self._find_stemming(instrument_agent_id, PRED.hasInstance, RT.InstrumentAgentInstance)

    def find_having_model(self, instrument_model_id):
        return self._find_having(PRED.hasModel, instrument_model_id)

    def find_stemming_model(self, instrument_agent_id):
        return self._find_stemming(instrument_agent_id, PRED.hasModel, RT.InstrumentModel)
