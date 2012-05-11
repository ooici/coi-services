#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.instrument_agent_instance_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import RT

from ion.services.sa.resource_impl.resource_simple_impl import ResourceSimpleImpl

class InstrumentAgentInstanceImpl(ResourceSimpleImpl):
    """
    @brief Resource management for InstrumentAgentInstance
    """

    def _primary_object_name(self):
        return RT.InstrumentAgentInstance

    def _primary_object_label(self):
        return "instrument_agent_instance"
