#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.platform_agent_instance_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import RT, PRED

from ion.services.sa.resource_impl.resource_simple_impl import ResourceSimpleImpl

class PlatformAgentInstanceImpl(ResourceSimpleImpl):
    """
    @brief resource management for PlatformAgentInstance resources
    """

    def _primary_object_name(self):
        return RT.PlatformAgentInstance

    def _primary_object_label(self):
        return "platform_agent_instance"

    def link_agent_definition(self, platform_agent_instance_id='', platform_agent_definition_id=''):
        return self._link_resources_single_object(platform_agent_instance_id, PRED.hasAgentDefinition, platform_agent_definition_id)

    def unlink_agent_definition(self, platform_agent_instance_id='', platform_agent_definition_id=''):
        return self._unlink_resources(platform_agent_instance_id, PRED.hasAgentDefinition, platform_agent_definition_id)

    def find_having_agent_definition(self, platform_agent_definition_id):
        return self._find_having(PRED.hasAgentDefinition, platform_agent_definition_id)

    def find_stemming_agent_definition(self, platform_agent_instance_id):
        return self._find_stemming(platform_agent_instance_id, PRED.hasAgentDefinition, RT.PlatformAgent)

