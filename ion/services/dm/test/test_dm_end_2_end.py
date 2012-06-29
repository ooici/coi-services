#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file test_dm_end_2_end
@date 06/29/12 13:58
@description DESCRIPTION
'''
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from pyon.core.exception import Timeout
from pyon.public import RT
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.objects import ProcessDefinition, Granule
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr

import time

@attr('INT',group='dm')
class TestDMEnd2End(IonIntegrationTestCase):
    def setUp(self): # Love the non pep-8 convention
        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.process_dispatcher   = ProcessDispatcherServiceClient()
        self.pubsub_management    = PubsubManagementServiceClient()
        self.resource_registry    = ResourceRegistryServiceClient()
        self.dataset_management   = DatasetManagementServiceClient()
        self.ingestion_management = IngestionManagementServiceClient()
        self.data_retriever       = DataRetrieverServiceClient()

    def launch_producer(self, stream_id=''):
        #--------------------------------------------------------------------------------
        # Create the process definition for the producer
        #--------------------------------------------------------------------------------
        producer_definition = ProcessDefinition(name='Example Data Producer')
        producer_definition.executable = {
            'module':'ion.processes.data.example_data_producer',
            'class' :'ExampleDataProducer'
        }

        process_definition_id = self.process_dispatcher.create_process_definition(process_definition=producer_definition)
        
        #--------------------------------------------------------------------------------
        # Launch the producer
        #--------------------------------------------------------------------------------

        config = DotDict()
        config.process.stream_id =  stream_id
        self.process_dispatcher.schedule_process(process_definition_id=process_definition_id, configuration=config)

    def get_ingestion_config(self):
        #--------------------------------------------------------------------------------
        # Grab the ingestion configuration from the resource registry
        #--------------------------------------------------------------------------------
        # The ingestion configuration should have been created by the bootstrap service 
        # which is configured through r2deploy.yml

        ingest_configs, _  = self.resource_registry.find_resources(restype=RT.IngestionConfiguration,id_only=True)
        return ingest_configs[0]
        
    def wait_until_we_have_enough_granules(self, dataset_id=''):
        dataset = self.dataset_management.read_dataset(dataset_id)
        datastore_name = dataset.datastore_name
        datastore = self.container.datastore_manager.get_datastore(datastore_name)

        now = time.time()
        timeout = now + 10
        done = False
        while not done:
            if now >= timeout:
                raise Timeout('Granules are not populating in time.')
            if len(datastore.query_view(dataset.view_name)) > 3:
                done = True


            now = time.time()

      

    def test_dm_end_2_end(self):
        #--------------------------------------------------------------------------------
        # Set up a stream and have a mock instrument (producer) send data
        #--------------------------------------------------------------------------------

        stream_id = self.pubsub_management.create_stream()

        self.launch_producer(stream_id)


        #--------------------------------------------------------------------------------
        # Start persisting the data on the stream 
        # - Get the ingestion configuration from the resource registry
        # - call persist_data_stream to setup the subscription for the ingestion workers
        #   on the stream that you specify which causes the data to be persisted
        #--------------------------------------------------------------------------------

        ingest_config_id = self.get_ingestion_config()

        dataset_id = self.ingestion_management.persist_data_stream(stream_id=stream_id, ingestion_configuration_id=ingest_config_id)

        #--------------------------------------------------------------------------------
        # Now the granules are ingesting and persisted
        #--------------------------------------------------------------------------------

        self.wait_until_we_have_enough_granules(dataset_id)
        
        #--------------------------------------------------------------------------------
        # Replay the data
        # - Define the replay using the dataset
        #--------------------------------------------------------------------------------

        replay_id, replay_stream_id = self.data_retriever.define_replay(dataset_id)

        #--------------------------------------------------------------------------------
        # Now get the data in one chunk using an RPC Call to start_retreive
        #--------------------------------------------------------------------------------
        
        replay_data = self.data_retriever.start_retrieve(replay_id)

        self.assertIsInstance(replay_data, Granule)

