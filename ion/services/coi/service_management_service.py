#!/usr/bin/env python


__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'


from interface.services.coi.iservice_management_service import BaseServiceManagementService
from pyon.util.containers import is_basic_identifier
from pyon.core.exception import NotFound, BadRequest

class ServiceManagementService(BaseServiceManagementService):

    """
	The Service Management Service is the service that manages the service definitions for all of the services running in the system
	"""
    def create_service_definition(self, service_definition=None):
        """ Should receive a ServiceDefinition object
        """
        # Return Value
        # ------------
        # {service_definition_id: ''}
        #
        if not is_basic_identifier(service_definition.name):
            raise BadRequest("Invalid service_definition.name: " % service_definition.name)
        service_definition_id, version = self.clients.resource_registry.create(service_definition)
        return service_definition_id

    def update_service_definition(self, service_definition=None):
        """ Should receive a ServiceDefinition object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        if not is_basic_identifier(service_definition.name):
            raise BadRequest("Invalid service_definition name: " % service_definition.name)
        return self.clients.resource_registry.update(service_definition)

    def read_service_definition(self, service_definition_id=''):
        """ Should return a ServiceDefinition object
        """
        # Return Value
        # ------------
        # service_definition: {}
        #
        if not service_definition_id:
            raise BadRequest("The service_definition_id parameter is missing")
        service_definition = self.clients.resource_registry.read(service_definition_id)
        if not service_definition:
            raise NotFound("Service_definition %s does not exist" % service_definition_id)
        return service_definition

    def delete_service_definition(self, service_definition_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        if not service_definition_id:
            raise BadRequest("The service_definition_id parameter is missing")
        service_definition = self.clients.resource_registry.read(service_definition_id)
        if not service_definition:
            raise NotFound("Service_definition %s does not exist" % service_definition_id)
        return self.clients.resource_registry.delete(service_definition_id)

