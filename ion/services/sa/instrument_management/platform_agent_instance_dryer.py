#!/usr/bin/env python

"""
@package  ion.services.sa.instrument_management.platform_agent_instance_dryer
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import RT

from ion.services.sa.instrument_management.ims_simple import IMSsimple

class PlatformAgentInstanceDryer(IMSsimple):
    """
    @brief resource management for PlatformAgentInstance resources
    """

    def _primary_object_name(self):
        return RT.PlatformAgentInstance

    def _primary_object_label(self):
        return "platform_agent_instance"
