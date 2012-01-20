#!/usr/bin/env python


__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'


from interface.services.coi.iservice_management_service import BaseServiceManagementService

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
        pass

    def update_service_definition(self, service_definition=None):
        """ Should receive a ServiceDefinition object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_service_definition(self, service_definition_id=''):
        """ Should return a ServiceDefinition object
        """
        # Return Value
        # ------------
        # service_definition: {}
        #
        pass

    def delete_service_definition(self, service_definition_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_services(self, filters=None):
        """ Should receive a ResourceFilter object
        """
        # Return Value
        # ------------
        # service_definition_list: []
        #
        pass


