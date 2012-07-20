#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ingestion_management_service_a.py
@date 06/21/12 17:43
@description DESCRIPTION
'''
from pyon.public import PRED, RT
from pyon.util.arg_check import validate_is_instance, validate_true
from interface.services.dm.iingestion_management_service import BaseIngestionManagementService
from interface.objects import IngestionConfiguration, IngestionQueue, StreamQuery


class IngestionManagementService(BaseIngestionManagementService):

    def create_ingestion_configuration(self,name='', exchange_point_id='', queues=None):
        validate_is_instance(queues,list,'The queues parameter is not a proper list.')
        validate_true(len(queues)>0, 'Ingestion needs at least one queue to ingest from')
        for queue in queues:
            validate_is_instance(queue, IngestionQueue)

        ingestion_config = IngestionConfiguration()

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
        assocs = self.clients.resource_registry.find_associations(subject=ingestion_configuration_id, predicate=PRED.hasSubscription, id_only=False)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc)
            self.clients.pubsub_management.delete_subscription(assoc.o)
        return self.clients.resource_registry.delete(ingestion_configuration_id)

    def list_ingestion_configurations(self, id_only=False):
        resources, _  = self.clients.resource_registry.find_resources(restype=RT.IngestionConfiguration,id_only=id_only)
        return resources


    # --- 

    def persist_data_stream(self, stream_id='', ingestion_configuration_id='', dataset_id=''):
        # Figure out which MIME or xpath in the stream definition belongs where

        # Just going to use the first queue for now

        validate_is_instance(stream_id,basestring, 'stream_id %s is not a valid string' % stream_id)

        ingestion_config = self.read_ingestion_configuration(ingestion_configuration_id)

        ingestion_queue = self._determine_queue(stream_id, ingestion_config.queues)

        subscription_id = self.clients.pubsub_management.create_subscription(
            query=StreamQuery(stream_ids=[stream_id]),
            exchange_name=ingestion_queue.name,
            exchange_point=ingestion_config.exchange_point
        )

        self.clients.pubsub_management.activate_subscription(subscription_id=subscription_id)

        self.clients.resource_registry.create_association(
            subject=ingestion_configuration_id,
            predicate=PRED.hasSubscription,
            object=subscription_id
        )
        if dataset_id:
            self._existing_dataset(stream_id,dataset_id)
        else:
            dataset_id = self._new_dataset(stream_id, ingestion_queue.datastore_name)

        return dataset_id

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

        datasets, _ = self.clients.resource_registry.find_subjects(subject_type=RT.DataSet,predicate=PRED.hasDataset,object=stream_id,id_only=True)
        for dataset_id in datasets:
            self.clients.dataset_management.remove_stream(stream_id)



    def _determine_queue(self,stream_id='', queues=[]):
        # For now just return the first queue until stream definition is defined
        return queues[0]

    def _new_dataset(self,stream_id='', datastore_name=''):
        '''
        Handles stream definition inspection.
        Uses dataset management to create the dataset
        '''
        dataset_id = self.clients.dataset_management.create_dataset(stream_id=stream_id,datastore_name=datastore_name)
        return dataset_id

    def _existing_dataset(self,stream_id='', dataset_id=''):
        self.clients.dataset_management.add_stream(dataset_id,stream_id)



