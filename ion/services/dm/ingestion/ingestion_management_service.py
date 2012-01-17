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

    def create_ingestion_configuration(self, exchange_point_id='', couch_storage={}, hfd_storage={}, \
                                       number_of_workers=0, default_policy={}):
        """Setup ingestion workers to ingest all the data from a single exchange point.

        @param exchange_point_id    str
        @param couch_storage    Unknown
        @param hfd_storage    Unknown
        @param number_of_workers    int
        @param default_policy    Unknown
        @retval ingestion_configuration_id    str
        """
        # create an ingestion_configuration instance and update the registry
        ingestion_configuration = IonObject(RT.IngestionConfiguration,exchange_point_id = exchange_point_id, couch_storage, hfd_storage, \
            number_of_workers, default_policy)

        id, rev = self.clients.resource_registry.create(ingestion_configuration)

        # update the id attribute of ingestion_configuration
        ingestion_configuration._id = id
        ingestion_configuration._rev = rev

        return ingestion_configuration._id

    def update_ingestion_configuration(self, ingestion_configuration={}):
        """Change the number of workers or the default policy for ingesting data on each stream

        @param ingestion_configuration    IngestionConfiguration
        """
        log.debug("Updating ingestion configuration at exchange point: %s" % ingestion_configuration['exchange_point_id'])
        id, rev = self.clients.resource_registry.update(ingestion_configuration)

    def read_ingestion_configuration(self, ingestion_configuration_id=''):
        """Get an existing ingestion configuration object.

        @param ingestion_configuration_id    str
        @retval ingestion_configuration    IngestionConfiguration
        @throws NotFound    if ingestion configuration did not exist
        """
        log.debug("Reading ingestion configuration object id: %s", ingestion_configuration_id)
        ingestion_configuration = self.clients.resource_registry.read(ingestion_configuration_id)
        if ingestion_configuration is None:
            raise NotFound("Ingestion configuration %s does not exist" % ingestion_configuration_id)
        return ingestion_configuration

    def delete_ingestion_configuration(self, ingestion_configuration_id=''):
        """Delete an existing ingestion configuration object.

        @param ingestion_configuration_id    str
        @throws NotFound    if ingestion configuration did not exist
        """
        log.debug("Deleting ingestion configuration: %s", ingestion_configuration_id)
        ingestion_configuration = self.read_ingestion_configuration(ingestion_configuration_id)
        if ingestion_configuration is None:
            raise NotFound("Ingestion configuration %d does not exist" % ingestion_configuration_id)

        self.clients.resource_registry.delete(ingestion_configuration)


    def create_stream_policy(self, stream_id='', archive_data='', archive_metadata=''):
        """Create a policy for a particular stream and associate it to the ingestion configuration for the exchange point the stream is on. (After LCA)

        @param stream_id    str
        @param archive_data    str
        @param archive_metadata    str
        @retval ingestion_policy_id    str
        """

#        return ingestion_policy_id

    def update_stream_policy(self, stream_policy={}):
        """Change the number of workers or the default policy for ingesting data on each stream (After LCA)

        @param stream_policy    Unknown
        @throws NotFound    if ingestion configuration did not exist
        """


    def read_stream_policy(self, stream_policy_id=''):
        """Get an existing stream policy object. (After LCA)

        @param stream_policy_id    str
        @retval ingestion_configuration    IngestionConfiguration
        @throws NotFound    if ingestion configuration did not exist
        """

#        return ingestion_configuration

    def delete_stream_policy(self, ingestion_configuration_id=''):
        """Delete an existing stream policy object. (After LCA)

        @param ingestion_configuration_id    str
        @throws NotFound    if ingestion configuration did not exist
        """

