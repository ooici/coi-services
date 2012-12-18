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
from interface.objects import IngestionConfiguration, IngestionQueue
from pyon.core.exception import BadRequest
from pyon.util.log import log
from pyon.util.containers import DotDict


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
        #--------------------------------------------------------------------------------
        # Validate that the method call was indeed valid
        #--------------------------------------------------------------------------------
        validate_is_instance(stream_id,basestring, 'stream_id %s is not a valid string' % stream_id)
        validate_true(dataset_id,'Clients must specify the dataset to persist')
        log.info('Persisting stream %s to dataset %s.', stream_id, dataset_id)

        ingestion_config = self.read_ingestion_configuration(ingestion_configuration_id)
        if self.is_persisted(stream_id):
            raise BadRequest('This stream is already being persisted')
        #--------------------------------------------------------------------------------
        # Set up the stream subscriptions and associations for this stream and its ingestion_type
        #--------------------------------------------------------------------------------
        if self.setup_queues(ingestion_config, stream_id, dataset_id):
            self.clients.pubsub_management.persist_stream(stream_id)


        return dataset_id

    def setup_queues(self, ingestion_config, stream_id, dataset_id):
        #--------------------------------------------------------------------------------
        # Iterate through each queue, check to make sure it's a supported type
        # and it's the queue we're trying to set up
        #--------------------------------------------------------------------------------
        for queue in ingestion_config.queues:
            # Make the subscription from the stream to this queue
            queue_name = queue.name + '_' + dataset_id
            subscription_id = self.clients.pubsub_management.create_subscription(name=queue_name, stream_ids=[stream_id], exchange_name=queue_name)
            self.clients.pubsub_management.activate_subscription(subscription_id=subscription_id)
            
            # Associate the subscription with the ingestion config which ensures no dangling resources
            self.clients.resource_registry.create_association(
                subject=ingestion_config._id,
                predicate=PRED.hasSubscription,
                object=subscription_id
            )
            self._existing_dataset(stream_id, dataset_id)
            self.launch_worker(queue_name)

            return True

        return False


    def launch_worker(self, queue_name):
        config = DotDict()
        config.process.datastore_name = self.CFG.get_safe('service.ingestion_management.datastore_name', 'datasets')
        config.process.queue_name = queue_name
        config.process.buffer_limit = self.CFG.get_safe('service.ingestion_management.buffer_limit', 10)
        config.process.time_limit = self.CFG.get_safe('service.ingestion_management.time_limit', 10)

        process_definition_id, _  = self.clients.resource_registry.find_resources(restype=RT.ProcessDefinition, name='ingestion_worker_process', id_only=True)
        validate_true(len(process_definition_id), 'No process definition for ingestion workers could be found')
        process_definition_id = process_definition_id[0]

        process_id = self.clients.process_dispatcher.create_process(process_definition_id=process_definition_id)

        xn_ids, _ = self.clients.resource_registry.find_resources(restype=RT.ExchangeName, name=queue_name, id_only=True)
        for xn_id in xn_ids:
            self.clients.resource_registry.create_association(xn_id, PRED.hasIngestionWorker, process_id)

        self.clients.process_dispatcher.schedule_process(process_definition_id=process_definition_id, process_id=process_id, configuration=config)

    def kill_worker(self, subscription_id):
        for xn_obj in self.clients.resource_registry.find_subjects(object=subscription_id, predicate=PRED.hasSubscription, id_only=False)[0]:
            for process in self.clients.resource_registry.find_objects(subject=xn_obj, predicate=PRED.hasIngestionWorker, id_only=False)[0]:
                if process.process_id.startswith('ingestion_worker'):
                    self.clients.process_dispatcher.cancel_process(process._id)


    def unpersist_data_stream(self, stream_id='', ingestion_configuration_id=''):


        subscriptions, assocs = self.clients.resource_registry.find_objects(subject=ingestion_configuration_id, predicate=PRED.hasSubscription, id_only=True)

        self.clients.pubsub_management.unpersist_stream(stream_id)

        for i in xrange(len(subscriptions)):
            subscription = subscriptions[i]
            assoc = assocs[i]
            # Check if this subscription is the one with the stream_id

            if len(self.clients.resource_registry.find_associations(subject=subscription, object=stream_id))>0: # this subscription has this stream
                self.kill_worker(subscription)

                self.clients.pubsub_management.deactivate_subscription(subscription_id=subscription)
                self.clients.resource_registry.delete_association(assoc)
                self.clients.pubsub_management.delete_subscription(subscription)

        datasets, _ = self.clients.resource_registry.find_subjects(subject_type=RT.DataSet,predicate=PRED.hasStream,object=stream_id,id_only=True)
        for dataset_id in datasets:
            self.clients.dataset_management.remove_stream(dataset_id, stream_id)

    def is_persisted(self, stream_id=''):
        return self.clients.pubsub_management.is_persisted(stream_id)

    def _determine_queue(self,stream_id='', queues=[]):
        # For now just return the first queue until stream definition is defined
        return queues[0]

    def _existing_dataset(self,stream_id='', dataset_id=''):
        assocs = self.clients.resource_registry.find_associations(subject=dataset_id, predicate=PRED.hasStream)
        if not assocs:
            self.clients.dataset_management.add_stream(dataset_id,stream_id)



