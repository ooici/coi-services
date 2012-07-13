#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file test_dm_end_2_end
@date 06/29/12 13:58
@description DESCRIPTION
'''
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from pyon.core.exception import Timeout
from pyon.public import RT, log
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.datastore.datastore import DataStore
from interface.objects import ProcessDefinition, Granule
from pyon.util.containers import DotDict
from ion.services.dm.ingestion.test.ingestion_management_test import IngestionManagementIntTest
from pyon.util.int_test import IonIntegrationTestCase
from pyon.net.endpoint import Publisher
from pyon.ion.granule import RecordDictionaryTool, TaxyTool, build_granule
from pyon.core.bootstrap import get_sys_name
from nose.plugins.attrib import attr

import time
import numpy as np

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
        self.pids                 = []
        

    def tearDown(self):
        for pid in self.pids:
            self.process_dispatcher.cancel_process(pid)
        IngestionManagementIntTest.clean_subscriptions()
        

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
        pid = self.process_dispatcher.schedule_process(process_definition_id=process_definition_id, configuration=config)
        self.pids.append(pid)

    def get_ingestion_config(self):
        #--------------------------------------------------------------------------------
        # Grab the ingestion configuration from the resource registry
        #--------------------------------------------------------------------------------
        # The ingestion configuration should have been created by the bootstrap service 
        # which is configured through r2deploy.yml

        ingest_configs, _  = self.resource_registry.find_resources(restype=RT.IngestionConfiguration,id_only=True)
        return ingest_configs[0]

    def publish_fake_data(self,stream_id):

        pub = Publisher()
        tt = TaxyTool()
        tt.add_taxonomy_set('pres','long name for pres')
        tt.add_taxonomy_set('lat','long name for latitude')
        tt.add_taxonomy_set('lon','long name for longitude')
        tt.add_taxonomy_set('height','long name for height')
        tt.add_taxonomy_set('time','long name for time')
        tt.add_taxonomy_set('temp','long name for temp')
        tt.add_taxonomy_set('cond','long name for cond')

        rdt = RecordDictionaryTool(tt)

        rdt['pres'] = np.array([1,2,3,4,5])
        rdt['lat'] = np.array([1,2,3,4,5])
        rdt['lon'] = np.array([1,2,3,4,5])
        rdt['height'] = np.array([1,2,3,4,5])
        rdt['time'] = np.array([1,2,3,4,5])
        rdt['temp'] = np.array([1,2,3,4,5])
        rdt['cond'] = np.array([1,2,3,4,5])

        granule = build_granule('test',tt,rdt)

        xp = self.container.ex_manager.create_xp('science_data')
        xpr = xp.create_route('%s.data' % stream_id)

        pub.publish(granule,to_name=xpr)

        rdt = RecordDictionaryTool(tt)
        rdt['pres'] = np.array([1,2,3,4,5])
        rdt['lat'] = np.array([1,2,3,4,5])
        rdt['lon'] = np.array([1,2,3,4,5])
        rdt['height'] = np.array([1,2,3,4,5])
        rdt['time'] = np.array([6,7,8,9,10])
        rdt['temp'] = np.array([1,2,3,4,5])
        rdt['cond'] = np.array([1,2,3,4,5])


        granule = build_granule(data_producer_id='tool', taxonomy=tt, record_dictionary=rdt)

        pub.publish(granule,to_name=xpr)
        

    def get_datastore(self, dataset_id):
        dataset = self.dataset_management.read_dataset(dataset_id)
        datastore_name = dataset.datastore_name
        datastore = self.container.datastore_manager.get_datastore(datastore_name, DataStore.DS_PROFILE.SCIDATA)
        return datastore



        
    def wait_until_we_have_enough_granules(self, dataset_id='',granules=4):
        datastore = self.get_datastore(dataset_id)
        dataset = self.dataset_management.read_dataset(dataset_id)
        

        now = time.time()
        timeout = now + 10
        done = False
        while not done:
            if now >= timeout:
                raise Timeout('Granules are not populating in time.')
            if len(datastore.query_view(dataset.view_name)) >= granules:
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
        # Now get the data in one chunk using an RPC Call to start_retreive
        #--------------------------------------------------------------------------------
        
        replay_data = self.data_retriever.retrieve(dataset_id)

        self.assertIsInstance(replay_data, Granule)

    def test_replay_by_time(self):


        #--------------------------------------------------------------------------------
        # Create the datastore first,
        #--------------------------------------------------------------------------------
        stream_id = self.pubsub_management.create_stream()
        log.debug("BUGS! starting")

        config_id = self.get_ingestion_config()

        dataset_id = self.ingestion_management.persist_data_stream(stream_id=stream_id, ingestion_configuration_id=config_id)
        self.get_datastore(dataset_id)

        self.publish_fake_data(stream_id)

        self.wait_until_we_have_enough_granules(dataset_id,2) # I just need two

        replay_granule = self.data_retriever.retrieve(dataset_id,{'start_time':0,'end_time':2})

        rdt = RecordDictionaryTool.load_from_granule(replay_granule)

        comp = rdt['time'] == np.array([1,2,3,4,5])

        self.assertTrue(comp.all())

    def test_last_granule(self):
        pass

