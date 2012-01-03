#!/usr/bin/env python

"""
@package  ion.services.sa.instrument_management.instrument_agent_instance_dryer
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import RT

from ion.services.sa.instrument_management.ims_simple import IMSsimple

class InstrumentAgentInstanceDryer(IMSsimple):
    """
    @brief Resource management for InstrumentAgentInstance
    """

    def _primary_object_name(self):
        return RT.InstrumentAgentInstance

    def _primary_object_label(self):
        return "instrument_agent_instance"
