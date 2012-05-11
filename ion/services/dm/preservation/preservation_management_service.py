#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.dm.ipreservation_management_service import BasePreservationManagementService


class PreservationManagementService(BasePreservationManagementService):

    def create_persistence_system(self, persistence_system=None):
        """Create an PersistenceSystem resource describing a persistence system, such as a database cluster.

        @param persistence_system    PersistenceSystem
        @retval persistence_system_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        pass

    def update_persistence_system(self, persistence_system=None):
        """@todo document this interface!!!

        @param persistence_system    PersistenceSystem
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        pass

    def read_persistence_system(self, persistence_system_id=''):
        """@todo document this interface!!!

        @param persistence_system_id    str
        @retval persistence_system    PersistenceSystem
        @throws NotFound    object with specified id does not exist
        """
        pass

    def delete_persistence_system(self, persistence_system_id=''):
        """@todo document this interface!!!

        @param persistence_system_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        pass

    def create_datastore(self, datastore=None):
        """Create an DataStore resource, describing a separate namespace in a PersistenceSystem

        @param datastore    DataStore
        @retval datastore_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        pass

    def update_datastore(self, datastore=None):
        """@todo document this interface!!!

        @param datastore    DataStore
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        pass

    def read_datastore(self, datastore_id=''):
        """@todo document this interface!!!

        @param datastore_id    str
        @retval datastore    DataStore
        @throws NotFound    object with specified id does not exist
        """
        pass

    def delete_datastore(self, datastore_id=''):
        """@todo document this interface!!!

        @param datastore_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        pass
