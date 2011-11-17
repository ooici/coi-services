#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.dm.ipersistent_archive_management_service import BasePersistentArchiveManagementService

class PersistentArchiveManagementService(BasePersistentArchiveManagementService):

    """
    class docstring
    """

    def create_archive(self, archive={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {view_id: ''}
        #
        pass

    def update_archive(self, archive={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_archive(self, archive_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # archive: {}
        #
        pass

    def delete_archive(self, archive_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass


  