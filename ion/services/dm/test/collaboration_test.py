#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file collaboration_test
@date 06/18/12 09:46
@description DESCRIPTION
'''
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase
from pyon.core.bootstrap import get_sys_name
from pyon.net.endpoint import Subscriber
from nose.plugins.attrib import attr
from gevent.event import AsyncResult
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessDefinition, IngestionQueue, Granule
import unittest
import os

import gevent
@attr('INT',group='dm')
class DMCollaborationIntTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        config = DotDict()
        self.container.start_rel_from_url('res/deploy/r2dm.yml', config)


        self.datastore_name       = 'datasets'
        self.pubsub_management    = PubsubManagementServiceClient()
        self.ingestion_management = IngestionManagementServiceClient()
        self.dataset_management   = DatasetManagementServiceClient()
        self.process_dispatcher   = ProcessDispatcherServiceClient()
        self.ingestion_management = IngestionManagementServiceClient()
        self.data_retriever       = DataRetrieverServiceClient()
        self.exchange_space       = 'science_granule_ingestion'
        self.exchange_point       = 'science_data'
        self.process_definitions  = {}

    def subscriber_action(self, msg, header):
        if msg == {}:
            return # End of replay
        self.assertTrue(isinstance(msg,Granule))

        if not hasattr(self,'received'):
            self.received = 0
        if not hasattr(self, 'async_done'):
            self.async_done = AsyncResult()
        self.received += 1
        if self.received >= 2:
            self.async_done.set(True)

    def create_process_definitions(self):
        producer_definition = ProcessDefinition(name='Example Data Producer')
        producer_definition.executable = {
            'module':'ion.processes.data.example_data_producer',
            'class' :'ExampleDataProducer'
        }

        process_definition_id = self.process_dispatcher.create_process_definition(process_definition=producer_definition)
        self.process_definitions['data_producer'] = process_definition_id

        ingestion_worker_definition = ProcessDefinition(name='ingestion worker')
        ingestion_worker_definition.executable = {
            'module':'ion.processes.data.ingestion.science_granule_ingestion_worker',
            'class' :'ScienceGranuleIngestionWorker'
        }
        process_definition_id = self.process_dispatcher.create_process_definition(process_definition=ingestion_worker_definition)
        self.process_definitions['ingestion_worker'] = process_definition_id


    def launch_processes(self, stream_id):
        # First launch the ingestors
        config = DotDict()
        config.process.datastore_name = 'datasets'
        config.process.queue_name = '%s.%s' %(self.exchange_point, self.exchange_space)

        self.process_dispatcher.schedule_process(self.process_definitions['ingestion_worker'],configuration=config)

        # Next launch the producer(s)
        config = DotDict()
        config.process.stream_id = stream_id

        self.process_dispatcher.schedule_process(self.process_definitions['data_producer'],configuration=config)

            
    def create_ingestion_config(self):
        ingest_queue = IngestionQueue(name=self.exchange_space, type='science_granule')
        config_id = self.ingestion_management.create_ingestion_configuration(name='standard_ingest', exchange_point_id=self.exchange_point, queues=[ingest_queue])
        return config_id


    #--------------------------------------------------------------------------------
    # I have to skip CEI mode in the interim because of the couch config madness, it's going away shortly.
    #--------------------------------------------------------------------------------

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_ingest_to_replay(self):

        self.async_done = AsyncResult()
        sysname = get_sys_name()

        #--------------------------------------------------------------------------------
        # Bootstrapping
        #--------------------------------------------------------------------------------
        # Set up the process definitions the services and process launchers will use
        self.create_process_definitions()

        # Make a transport stream
        stream_id = self.pubsub_management.create_stream()
        
        # Launch the processes
        self.launch_processes(stream_id)

        # Grab a handle on the datastore
        datastore = self.container.datastore_manager.get_datastore(self.datastore_name)


        #--------------------------------------------------------------------------------
        # Create the ingestion config for this exchange
        #--------------------------------------------------------------------------------
        
        ingestion_configuration_id = self.create_ingestion_config()

        #--------------------------------------------------------------------------------
        # Persist the data stream
        #--------------------------------------------------------------------------------

        dataset_id = self.ingestion_management.persist_data_stream(stream_id=stream_id, ingestion_configuration_id=ingestion_configuration_id)
        #--------------------------------------------------------------------------------
        # Define the replay
        #--------------------------------------------------------------------------------
        
        replay_id, replay_stream_id = self.data_retriever.define_replay(dataset_id = dataset_id)

        subscriber = Subscriber(name=('%s.science_data' % sysname, 'test_queue'), callback=self.subscriber_action, binding='%s.data' % replay_stream_id)
        gevent.spawn(subscriber.listen)

        done = False
        while not done:
            results = datastore.query_view('manifest/by_dataset')
            if len(results) >= 2:
                done = True

        self.data_retriever.start_replay(replay_id)

        try:
            self.async_done.get(timeout=10)
        except gevent.Timeout:
            self.assertTrue(False, 'Results not gathered')


