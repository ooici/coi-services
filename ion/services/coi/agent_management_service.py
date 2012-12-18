#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from interface.services.coi.iagent_management_service import BaseAgentManagementService


class AgentManagementService(BaseAgentManagementService):
    """The Agent Management Service is the service that manages Agent Definitions, Agent Instance
    configurations and running Agents in the system.

    @see https://confluence.oceanobservatories.org/display/syseng/CIAD+COI+OV+Agent+Management+Service
    """

    def create_agent_definition(self, agent_definition=None):
        """Creates an Agent Definition resource from the parameter AgentDefinition object.

        @param agent_definition    AgentDefinition
        @retval agent_definition_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        ad_id, version = self.clients.resource_registry.create(agent_definition)
        return ad_id

    def update_agent_definition(self, agent_definition=None):
        """Updates an existing Agent Definition resource.

        @param agent_definition    AgentDefinition
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        self.clients.resource_registry.update(agent_definition)

    def read_agent_definition(self, agent_definition_id=''):
        """Returns an existing Agent Definition resource.

        @param agent_definition_id    str
        @retval agent_definition    AgentDefinition
        @throws NotFound    object with specified id does not exist
        """
        adef = self.clients.resource_registry.read(agent_definition_id)
        return adef

    def delete_agent_definition(self, agent_definition_id=''):
        """Deletes an existing Agent Definition resource.

        @param agent_definition_id    str
        @throws NotFound    object with specified id does not exist
        """
        self.clients.resource_registry.delete(agent_definition_id)


    def create_agent_instance(self, agent_instance=None):
        """Creates an Agent Instance resource from the parameter AgentInstance object.

        @param agent_instance    AgentInstance
        @retval agent_instance_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        ai_id, version = self.clients.resource_registry.create(agent_instance)
        return ai_id

    def update_agent_instance(self, agent_instance=None):
        """Updates an existing Agent Instance resource.

        @param agent_instance    AgentInstance
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        self.clients.resource_registry.update(agent_instance)

    def read_agent_instance(self, agent_instance_id=''):
        """Returns an existing Agent Instance resource.

        @param agent_instance_id    str
        @retval agent_instance    AgentInstance
        @throws NotFound    object with specified id does not exist
        """
        ainst = self.clients.resource_registry.read(agent_instance_id)
        return ainst

    def delete_agent_instance(self, agent_instance_id=''):
        """Deletes an existing Agent Instance resource.

        @param agent_instance_id    str
        @throws NotFound    object with specified id does not exist
        """
        self.clients.resource_registry.delete(agent_instance_id)

