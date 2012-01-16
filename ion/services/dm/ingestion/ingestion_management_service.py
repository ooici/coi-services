#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.dm.iingestion_management_service import BaseIngestionManagementService
from pyon.core.exception import NotFound
from pyon.public import RT, AT, log, IonObject
from pyon.public import CFG
from ion.services.dm.ingestion.ingestion import Ingestion
from ion.services.dm.transformation.transform_management_service import TransformManagementService

class IngestionManagementService(BaseIngestionManagementService):
    """
    class docstring
    """

    def create_ingestion_configuration(self, exchange_point_id, couch_storage, filesystem, root_path, hdf_storage,\
                                    server, database, number_of_workers = 0, default_policy = StreamIngestionPolicy):
        """
        Setup ingestion workers to ingest all the data from a single exchange point.
        """
        # create an ingestion_configuration instance and update the registry
        ingestion_configuration = IngestionConfiguration(exchange_point_id, couch_storage, filesystem, root_path, \
            hdf_storage, server, database, number_of_workers, default_policy = StreamIngestionPolicy)

        id, rev = self.clients.resource_registry.create(ingestion_configuration)

        # update the id attribute of ingestion_configuration
        ingestion_configuration._id = id
        ingestion_configuration._rev = rev

        return ingestion_configuration._id

    def update_ingestion_configuration(self, ingestion_configuration):
        """
        Change the number of workers or the default policy for ingesting data on each stream
        """
        log.debug("Updating ingestion configuration: %s" % ingestion_configuration.id)
        id, rev = self.clients.resource_registry.update(ingestion_configuration)

        # update the ingestion_configuration with its new id
        ingestion_configuration._id = id
        ingestion_configuration._rev = rev

        return ingestion_configuration._id

    def read_ingestion_configuration(self, ingestion_configuration_id):
        """
        Get an existing ingestion configuration object.
        """
        log.debug("Reading ingestion configuration object id: %s", ingestion_configuration_id)
        ingestion_configuration = self.clients.resource_registry.read(ingestion_configuration_id)
        if ingestion_configuration is None:
            raise NotFound("Ingestion configuration %s does not exist" % ingestion_configuration_id)
        return ingestion_configuration

    def delete_ingestion_configuration(self, ingestion_configuration_id):
        """
        Delete an existing ingestion configuration object.
        """
        log.debug("Deleting ingestion configuration: %s", ingestion_configuration_id)
        ingestion_configuration = self.read_ingestion_configuration(ingestion_configuration_id)
        if ingestion_configuration is None:
            raise NotFound("Ingestion configuration %d does not exist" % ingestion_configuration_id)

        self.clients.resource_registry.delete(ingestion_configuration)
        return True

    def create_stream_policy(self, stream_id, archive_data, archive_metadata):
        """
        Create a policy for a particular stream and associate it to the ingestion configuration for the
        exchange point the stream is on. (After LCA)
        """

#        return ingestion_policy_id

    def update_stream_policy(self, stream_policy):
        """
        Change the number of workers or the default policy for ingesting data on each stream (After LCA)
        """


    def read_stream_policy(self, stream_policy_id):
        """
        Get an existing stream policy object. (After LCA)
        """

#        return ingestion_configuration

    def delete_stream_policy(self, ingestion_configuration_id):
        """
        Delete an existing stream policy object. (After LCA)
        """

