#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.instrument_agent_instance_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import PRED, RT

from ion.services.sa.resource_impl.resource_simple_impl import ResourceSimpleImpl

class InstrumentAgentInstanceImpl(ResourceSimpleImpl):
    """
    @brief Resource management for InstrumentAgentInstance
    """

    def _primary_object_name(self):
        return RT.InstrumentAgentInstance

    def _primary_object_label(self):
        return "instrument_agent_instance"

    def link_agent_definition(self, instrument_agent_instance_id='', instrument_agent_id=''):
        return self._link_resources_single_object(instrument_agent_instance_id, PRED.hasAgentDefinition, instrument_agent_id)

    def unlink_agent_definition(self, instrument_agent_instance_id='', instrument_agent_id=''):
        return self._unlink_resources(instrument_agent_instance_id, PRED.hasAgentDefinition, instrument_agent_id)

    def find_having_agent_definition(self, instrument_agent_id):
        return self._find_having(PRED.hasAgentDefinition, instrument_agent_id)

    def find_stemming_agent_definition(self, instrument_agent_instance_id):
        return self._find_stemming(instrument_agent_instance_id, PRED.hasAgentDefinition, RT.InstrumentAgent)

