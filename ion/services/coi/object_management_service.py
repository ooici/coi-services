#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from interface.services.coi.iobject_management_service import BaseObjectManagementService


class ObjectManagementService(BaseObjectManagementService):


    def create_object_type(self, object_type={}):
        """ Should receive an ObjectType object
        """
        # Return Value
        # ------------
        # {object_type_id: ''}
        #
        pass

    def update_object_type(self, object_type={}):
        """ Should receive an ObjectType object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_object_type(self, object_type_id=''):
        """  Should return an ObjectType object
        """
        # Return Value
        # ------------
        # object_type: {}
        #
        pass

    def delete_object_type(self, object_type_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_object_types(self, filters={}):
        """ Should receive a ResourceFilter object
        """
        # Return Value
        # ------------
        # object_type_list: []
        #
        pass
