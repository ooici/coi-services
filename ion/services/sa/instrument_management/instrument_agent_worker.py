#!/usr/bin/env python

"""
@package  ion.services.sa.instrument_management.instrument_agent_worker
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import AT, RT


from ion.services.sa.instrument_management.ims_simple import IMSsimple

class InstrumentAgentWorker(IMSsimple):
    """
    @brief Resource management for InstrumentAgent resources
    """

    def _primary_object_name(self):
        return RT.InstrumentAgent

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

    def find_having_instance(self, instrument_agent_instance_id):
        return self._find_having(AT.hasInstance, instrument_agent_instance_id)

    def find_stemming_instance(self, instrument_agent_id):
        return self._find_stemming(instrument_agent_id, AT.hasInstance, RT.InstrumentAgentInstance)

    def find_having_model(self, instrument_model_id):
        return self._find_having(AT.hasModel, instrument_model_id)

    def find_stemming_model(self, instrument_agent_id):
        return self._find_stemming(instrument_agent_id, AT.hasModel, RT.InstrumentModel)
