#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from interface.services.coi.iagent_management_service import BaseAgentManagementService

class AgentManagementService(BaseAgentManagementService):

    def create_agent_definition(self, agent_definition={}):
        """ Should receive an AgentDefinition object
        """
        # Return Value
        # ------------
        # {agent_definition_id: ''}
        #
        pass

    def update_agent_definition(self, agent_definition={}):
        """ Should receive an AgentDefinition object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_agent_definition(self, agent_definition_id=''):
        """Should return an AgentDefinition object
        """
        # Return Value
        # ------------
        # agent_definition: {}
        #
        pass

    def delete_agent_definition(self, agent_definition_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def register_agent(self, agent_definition_id='', agent_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def unassign_instrument_agent(self, agent_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass
