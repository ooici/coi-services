#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from interface.services.coi.iresource_management_service import BaseResourceManagementService

class ResourceManagementService(BaseResourceManagementService):

    def create_resource_type(self, resource_type={}):
        """ Should receive a ResourceType object
        """
        # Return Value
        # ------------
        # {resource_type_id: ''}
        #
        pass

    def update_resource_type(self, resource_type={}):
        """ Should receive a ResourceType object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_resource_type(self, resource_type_id=''):
        """ Should return a ResourceType object
        """
        # Return Value
        # ------------
        # resource_type: {}
        #
        pass

    def delete_resource_type(self, resource_type_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_resource_types(self, filters={}):
        """ Should receive a ResourceFilter object
        """
        # Return Value
        # ------------
        # resource_type_list: []
        #
        pass

    def create_resource_lifecycle(self, resource_lifecycle={}):
        """ Should receive a ResourceLifeCycle object
        """
        # Return Value
        # ------------
        # {resource_lifecycle_id: ''}
        #
        pass

    def update_resource_lifecycle(self, resource_lifecycle={}):
        """ Should receive a ResourceLifeCycle object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_resource_lifecycle(self, resource_lifecycle_id=''):
        """ Should return a ResourceLifeCycle object
        """
        # Return Value
        # ------------
        # resource_lifecycle: {}
        #
        pass

    def delete_resource_lifecycle(self, resource_lifecycle_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

