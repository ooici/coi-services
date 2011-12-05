#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.dm.iindex_management_service import BaseIndexManagementService


class IndexManagementService(BaseIndexManagementService):

    """
    class docstring
    """

    def create_index(self, index={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {index_id: ''}
        #
        pass

    def update_index(self, index={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_index(self, index_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # index: {}
        #
        pass

    def delete_index(self, index_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def list_indexes(self):
        """
        method docstring
        """
        # Return Value
        # ------------
        # list: []
        #
        pass

    def find_indexes(self, filters={}):
        """method docstring
        """
        # Return Value
        # ------------
        # index_list: []
        #
        pass