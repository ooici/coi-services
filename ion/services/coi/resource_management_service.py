#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from interface.services.coi.iresource_management_service import BaseResourceManagementService
from pyon.util.containers import is_basic_identifier
from pyon.core.exception import NotFound, BadRequest
from pyon.public import PRED
from exceptions import NotImplementedError

class ResourceManagementService(BaseResourceManagementService):

    """
    The Resource Management Service is the service that manages the Resource Types and Lifecycles associated with all Resources
    """
    
    def create_resource_type(self, resource_type=None, object_id=""):
        """ Should receive a ResourceType object
        """
        # Return Value
        # ------------
        # {resource_type_id: ''}
        #
        if not is_basic_identifier(resource_type.name):
            raise BadRequest("Invalid resource name: %s " % resource_type.name)
        if not object_id:
            raise BadRequest("Object_id is missing")

        object_type= self.clients.resource_registry.read(object_id)
        if resource_type.name != object_type.name:
            raise BadRequest("Resource and object name don't match: %s - %s" (resource_type.name,object_type.name))
        resource_id, version = self.clients.resource_registry.create(resource_type)
        self.clients.resource_registry.create_association(resource_id, PRED.hasObjectType, object_id)
        return resource_id

    def update_resource_type(self, resource_type=None):
        """ Should receive a ResourceType object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        raise NotImplementedError("Currently, updating ResourceType is not supported")

    def read_resource_type(self, resource_type_id=''):
        """ Should return a ResourceType object
        """
        # Return Value
        # ------------
        # resource_type: {}
        #
        if not resource_type_id:
            raise BadRequest("The resource_type_id parameter is missing")
        return self.clients.resource_registry.read(resource_type_id)

    def delete_resource_type(self, resource_type_id='', object_type_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        if not resource_type_id:
            raise BadRequest("The resource_type_id parameter is missing")
        if not object_type_id:
            raise BadRequest("The object_type_id parameter is missing")
        association_id = self.clients.resource_registry.get_association(resource_type_id, PRED.hasObjectType, object_type_id)
        self.clients.resource_registry.delete_association(association_id)
        return self.clients.resource_registry.delete(resource_type_id)

    def create_resource_lifecycle(self, resource_lifecycle=None):
        """ Should receive a ResourceLifeCycle object
        """
        # Return Value
        # ------------
        # {resource_lifecycle_id: ''}
        #
        raise NotImplementedError("Currently not supported")

    def update_resource_lifecycle(self, resource_lifecycle=None):
        """ Should receive a ResourceLifeCycle object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        raise NotImplementedError("Currently not supported")

    def read_resource_lifecycle(self, resource_lifecycle_id=''):
        """ Should return a ResourceLifeCycle object
        """
        # Return Value
        # ------------
        # resource_lifecycle: {}
        #
        raise NotImplementedError("Currently not supported")

    def delete_resource_lifecycle(self, resource_lifecycle_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        raise NotImplementedError("Currently not supported")

