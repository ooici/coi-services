#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from interface.services.coi.iobject_management_service import BaseObjectManagementService
from pyon.util.containers import is_basic_identifier


class ObjectManagementService(BaseObjectManagementService):


    """
    A service for defining and managing object types used as resource, messages, etc.
    """
    
    def create_object_type(self, object_type=None):
        """ Should receive an ObjectType object
        """
        # Return Value
        # ------------
        # {object_type_id: ''}
        #
        if not is_basic_identifier(object_type.name):
            raise BadRequest("Invalid object_type name: " % object_type.name)
        object_type_id, version = self.clients.resource_registry.create(object_type)
        return object_type_id

    def update_object_type(self, object_type=None):
        """ Should receive an ObjectType object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        if not is_basic_identifier(object_type.name):
            raise BadRequest("Invalid object_type name: " % object_type.name)
        return self.clients.resource_registry.update(object_type)

    def read_object_type(self, object_type_id=''):
        """  Should return an ObjectType object
        """
        # Return Value
        # ------------
        # object_type: {}
        #
        if not object_type_id:
            raise BadRequest("The resource_type_id parameter is missing")
        object_type = self.clients.resource_registry.read(object_type_id)
        if not object_type:
            raise NotFound("Object type %s does not exist" % object_type_id)
        return object_type

    def delete_object_type(self, object_type_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        if not object_type_id:
            raise BadRequest("The object_type_id parameter is missing")
        object_type = self.clients.resource_registry.read(object_type_id)
        if not object_type:
            raise NotFound("Object type type %s does not exist" % object_type_id)
        return self.clients.resource_registry.delete(object_type_id)

