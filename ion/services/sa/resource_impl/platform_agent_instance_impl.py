#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.platform_agent_instance_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import RT

from ion.services.sa.resource_impl.resource_simple_impl import ResourceSimpleImpl

class PlatformAgentInstanceImpl(ResourceSimpleImpl):
    """
    @brief resource management for PlatformAgentInstance resources
    """

    def _primary_object_name(self):
        return RT.PlatformAgentInstance

    def _primary_object_label(self):
        return "platform_agent_instance"
