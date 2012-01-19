#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.dm.ipreservation_management_service import BasePreservationManagementService


class PreservationManagementService(BasePreservationManagementService):

    def create_datastore(self, datastore=None):
        """method docstring
        """
        # Return Value
        # ------------
        # {datastore_id: ''}
        #
        pass

    def update_datastore(self, datastore=None):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_datastore(self, datastore_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # datastore: {}
        #
        pass

    def delete_datastore(self, datastore_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def create_persistent_archive(self, persistent_archive=None):
        """method docstring
        """
        # Return Value
        # ------------
        # {persistent_archive_id: ''}
        #
        pass

    def update_persistent_archive(self, persistent_archive=None):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_persistent_archive(self, persistent_archive_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # persistent_archive: {}
        #
        pass

    def delete_persistent_archive(self, persistent_archive_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

