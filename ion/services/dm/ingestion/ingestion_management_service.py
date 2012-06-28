#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ingestion_management_service_a.py
@date 06/21/12 17:43
@description DESCRIPTION
'''
from pyon.public import PRED
from pyon.util.arg_check import validate_is_instance, validate_true

from interface.services.dm.iingestion_management_service import BaseIngestionManagementService
from interface.objects import IngestionConfigurationA, IngestionQueue, StreamQuery


class IngestionManagementService(BaseIngestionManagementService):

    def create_ingestion_configuration(self,name='', exchange_point_id='', queues=None):
        validate_is_instance(queues,list,'The queues parameter is not a proper list.')
        validate_true(len(queues)>0, 'Ingestion needs at least one queue to ingest from')
        for queue in queues:
            validate_is_instance(queue, IngestionQueue)

        ingestion_config = IngestionConfigurationA()

        ingestion_config.name = name
        ingestion_config.exchange_point = exchange_point_id
        ingestion_config.queues = queues

        config_id, rev = self.clients.resource_registry.create(ingestion_config)

        return config_id

    def read_ingestion_configuration(self, ingestion_configuration_id=''):
        return self.clients.resource_registry.read(ingestion_configuration_id)

    def update_ingestion_configuration(self, ingestion_configuration=None):
        return self.clients.resource_registry.update(ingestion_configuration)

    def delete_ingestion_configuration(self, ingestion_configuration_id=''):
        assocs = self.clients.resource_registry.find_associations(subject=ingestion_configuration_id, predicate=PRED.hasSubscription, id_only=True)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc)
            self.clients.pubsub_management.delete_subscription(assoc.o)
        return self.clients.resource_registry.delete(ingestion_configuration_id)

    # --

    def persist_data_stream(self, stream_id='', ingestion_configuration_id=''):
        # Figure out which MIME or xpath in the stream definition belongs where

        # Just going to use the first queue for now

        ingestion_config = self.read_ingestion_configuration(ingestion_configuration_id)

        ingestion_queue = self._determine_queue(stream_id, ingestion_config.queues)

        subscription_id = self.clients.pubsub_management.create_subscription(
            query=StreamQuery(stream_ids=[stream_id]),
            exchange_name=ingestion_queue.name
        )

        self.clients.pubsub_management.activate_subscription(subscription_id=subscription_id)

        self.clients.resource_registry.create_association(
            subject=ingestion_configuration_id,
            predicate=PRED.hasSubscription,
            object=subscription_id
        )

        # Create dataset stuff here
        dataset_id = self._new_dataset(stream_id)

        return ""

    def unpersist_data_stream(self, stream_id='', ingestion_configuration_id=''):
        subscriptions, assocs = self.clients.resource_registry.find_objects(subject=ingestion_configuration_id, predicate=PRED.hasSubscription, id_only=True)
        for i in xrange(len(subscriptions)):
            subscription = subscriptions[i]
            assoc = assocs[i]
            # Check if this subscription is the one with the stream_id

            if len(self.clients.resource_registry.find_associations(subject=subscription, object=stream_id))>0: # this subscription has this stream
                self.clients.pubsub_management.deactivate_subscription(subscription_id=subscription)
                self.clients.resource_registry.delete_association(assoc)
                self.clients.pubsub_management.delete_subscription(subscription)



    def _determine_queue(self,stream_id='', queues=[]):
        # For now just return the first queue until stream definition is defined
        return queues[0]

    def _new_dataset(self,stream_id=''):
        '''
        Handles stream definition inspection.
        Uses dataset management to create the dataset
        '''
        return ''


