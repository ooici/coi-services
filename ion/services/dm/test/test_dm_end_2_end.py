#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file test_dm_end_2_end
@date 06/29/12 13:58
@description DESCRIPTION
'''
from pyon.core.exception import Timeout
from pyon.public import RT, log
from pyon.ion.stream import StandaloneStreamSubscriber, StandaloneStreamPublisher
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.datastore.datastore import DataStore
from interface.objects import ProcessDefinition, Granule
from pyon.util.containers import DotDict
from pyon.event.event import EventSubscriber
from ion.services.dm.ingestion.test.ingestion_management_test import IngestionManagementIntTest
from pyon.util.int_test import IonIntegrationTestCase
from ion.services.dm.utility.granule_utils import RecordDictionaryTool, CoverageCraft
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.dm.ingestion.ingestion_management_service import IngestionManagementService
from gevent.event import Event
from nose.plugins.attrib import attr
from pyon.ion.exchange import ExchangeNameQueue
import unittest
import os

import gevent
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
        self.event                = Event()
        self.exchange_space_name  = 'test_granules'
        self.exchange_point_name  = 'science_data'       

        self.purge_queues()
        self.queue_buffer         = []

    def purge_queues(self):
        xn = self.container.ex_manager.create_xn_queue('science_granule_ingestion')
        xn.purge()
        

    def tearDown(self):
        self.purge_queues()
        for pid in self.pids:
            self.container.proc_manager.terminate_process(pid)
        IngestionManagementIntTest.clean_subscriptions()
        for queue in self.queue_buffer:
            if isinstance(queue, ExchangeNameQueue):
                queue.delete()
        

    def launch_producer(self, stream_id=''):
        #--------------------------------------------------------------------------------
        # Launch the producer
        #--------------------------------------------------------------------------------

        pid = self.container.spawn_process('better_data_producer', 'ion.processes.data.example_data_producer', 'BetterDataProducer', {'process':{'stream_id':stream_id}})

        self.pids.append(pid)

    def get_ingestion_config(self):
        #--------------------------------------------------------------------------------
        # Grab the ingestion configuration from the resource registry
        #--------------------------------------------------------------------------------
        # The ingestion configuration should have been created by the bootstrap service 
        # which is configured through r2deploy.yml

        ingest_configs, _  = self.resource_registry.find_resources(restype=RT.IngestionConfiguration,id_only=True)
        return ingest_configs[0]


    def publish_hifi(self,stream_id,stream_route,offset=0):
        pub = StandaloneStreamPublisher(stream_id, stream_route)

        black_box = CoverageCraft()
        black_box.rdt['time'] = np.arange(10) + (offset * 10)
        black_box.rdt['temp'] = (np.arange(10) + (offset * 10)) * 2
        granule = black_box.to_granule()
        pub.publish(granule)

    def publish_fake_data(self,stream_id, route):

        for i in xrange(4):
            self.publish_hifi(stream_id,route,i)
        

    def get_datastore(self, dataset_id):
        dataset = self.dataset_management.read_dataset(dataset_id)
        datastore_name = dataset.datastore_name
        datastore = self.container.datastore_manager.get_datastore(datastore_name, DataStore.DS_PROFILE.SCIDATA)
        return datastore

    def validate_granule_subscription(self, msg, route, stream_id):
        if msg == {}:
            return
        self.assertIsInstance(msg,Granule,'Message is improperly formatted. (%s)' % type(msg))
        self.event.set()

    def make_file_data(self):
        from interface.objects import File
        import uuid
        data = 'hello world\n'
        rand = str(uuid.uuid4())[:8]
        meta = File(name='/examples/' + rand + '.txt', group_id='example1')
        return {'body': data, 'meta':meta}

    def publish_file(self, stream_id, stream_route):
        publisher = StandaloneStreamPublisher(stream_id,stream_route)
        publisher.publish(self.make_file_data())
        
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


    def wait_until_we_have_enough_files(self):
        datastore = self.container.datastore_manager.get_datastore('filesystem', DataStore.DS_PROFILE.FILESYSTEM)

        now = time.time()
        timeout = now + 10
        done = False
        while not done:
            if now >= timeout:
                raise Timeout('Files are not populating in time.')
            if len(datastore.query_view('catalog/file_by_owner')) >= 1:
                done = True
            now = time.time()


    def create_dataset(self):
        craft = CoverageCraft
        sdom, tdom = craft.create_domains()
        sdom = sdom.dump()
        tdom = tdom.dump()
        pdict = craft.create_parameters()
        pdict = pdict.dump()

        dataset_id = self.dataset_management.create_dataset('test_dataset', parameter_dict=pdict, spatial_domain=sdom, temporal_domain=tdom)
        return dataset_id


    def test_coverage_ingest(self):
        stream_id, stream_route = self.pubsub_management.create_stream('test_coverage_ingest', exchange_point=self.exchange_point_name)
        dataset_id = self.create_dataset()
        # I freaking hate this bug
        self.get_datastore(dataset_id)
        ingestion_config_id = self.get_ingestion_config()
        self.ingestion_management.persist_data_stream(stream_id=stream_id, 
                    ingestion_configuration_id=ingestion_config_id,
                    dataset_id=dataset_id)

        black_box = CoverageCraft()
        black_box.rdt['time'] = np.arange(20)
        black_box.rdt['temp'] = np.random.random(20) * 10
        black_box.sync_with_granule()
        granule = black_box.to_granule()

        publisher = StandaloneStreamPublisher(stream_id, stream_route)
        publisher.publish(granule)

        self.wait_until_we_have_enough_granules(dataset_id,1)

        coverage = DatasetManagementService._get_coverage(dataset_id)

        black_box = CoverageCraft(coverage)
        black_box.sync_rdt_with_coverage()
        comp = black_box.rdt['time'] == np.arange(20)
        self.assertTrue(comp.all())

        black_box = CoverageCraft()
        black_box.rdt['time'] = np.arange(20) + 20
        black_box.rdt['temp'] = np.random.random(20) * 10
        black_box.sync_with_granule()
        granule = black_box.to_granule()

        publisher.publish(granule)


        self.wait_until_we_have_enough_granules(dataset_id,2)

        coverage = DatasetManagementService._get_coverage(dataset_id)

        black_box = CoverageCraft(coverage)
        black_box.sync_rdt_with_coverage()
        comp = black_box.rdt['time'] == np.arange(40)
        self.assertTrue(comp.all())


        granule = self.data_retriever.retrieve(dataset_id)

        black_box = CoverageCraft()
        black_box.sync_rdt_with_granule(granule)
        comp = black_box.rdt['time'] == np.arange(40)
        self.assertTrue(comp.all())
        




    @attr('SMOKE') 
    def test_dm_end_2_end(self):
        #--------------------------------------------------------------------------------
        # Set up a stream and have a mock instrument (producer) send data
        #--------------------------------------------------------------------------------

        stream_id, route = self.pubsub_management.create_stream('producer', exchange_point=self.exchange_point_name)
        self.launch_producer(stream_id)


        #--------------------------------------------------------------------------------
        # Start persisting the data on the stream 
        # - Get the ingestion configuration from the resource registry
        # - Create the dataset
        # - call persist_data_stream to setup the subscription for the ingestion workers
        #   on the stream that you specify which causes the data to be persisted
        #--------------------------------------------------------------------------------

        ingest_config_id = self.get_ingestion_config()
        dataset_id = self.create_dataset()
        self.ingestion_management.persist_data_stream(stream_id=stream_id, ingestion_configuration_id=ingest_config_id, dataset_id=dataset_id)

        #--------------------------------------------------------------------------------
        # Now the granules are ingesting and persisted
        #--------------------------------------------------------------------------------

        self.wait_until_we_have_enough_granules(dataset_id,4)
        
        #--------------------------------------------------------------------------------
        # Now get the data in one chunk using an RPC Call to start_retreive
        #--------------------------------------------------------------------------------
        
        replay_data = self.data_retriever.retrieve(dataset_id)
        self.assertIsInstance(replay_data, Granule)
        
        #--------------------------------------------------------------------------------
        # Now to try the streamed approach
        #--------------------------------------------------------------------------------

        replay_stream_id, replay_route = self.pubsub_management.create_stream('replay_out', exchange_point=self.exchange_point_name)
        self.replay_id = self.data_retriever.define_replay(dataset_id=dataset_id, stream_id=replay_stream_id)
        process_id = self.data_retriever.read_process_id(self.replay_id)

        self.launched_event = gevent.event.Event()
        
        def ugh(*args, **kwargs):
            self.launched_event.set()

        event_sub = EventSubscriber(event_type="ProcessLifecycleEvent", callback=ugh, origin=process_id, origin_type="DispatchedProcess")
        event_sub.start()
    
        #--------------------------------------------------------------------------------
        # Create the listening endpoint for the the retriever to talk to 
        #--------------------------------------------------------------------------------
        xp = self.container.ex_manager.create_xp(self.exchange_point_name)
        subscriber = StandaloneStreamSubscriber(self.exchange_space_name, self.validate_granule_subscription)
        self.queue_buffer.append(self.exchange_space_name)
        subscriber.start()
        subscriber.xn.bind(replay_route.routing_key, xp)

        if self.launched_event.wait(10):
            self.data_retriever.start_replay(self.replay_id)
        
        fail = False
        try:
            self.event.wait(10)
        except gevent.Timeout:
            fail = True

        self.assertTrue(self.launched_event.wait(10), 'Guess what, the process was never dispatched...')

        subscriber.stop()

        self.assertTrue(not fail, 'Failed to validate the data.')
        self.data_retriever.cancel_replay(self.replay_id)

        event_sub.stop()
        event_sub.close()


    def test_replay_by_time(self):
        #--------------------------------------------------------------------------------
        # Create the necessary configurations for the test
        #--------------------------------------------------------------------------------
        stream_id, route  = self.pubsub_management.create_stream('replay_by_time', exchange_point=self.exchange_point_name)
        config_id         = self.get_ingestion_config()
        dataset_id        = self.create_dataset()
        self.ingestion_management.persist_data_stream(stream_id=stream_id, ingestion_configuration_id=config_id, dataset_id=dataset_id)
        #--------------------------------------------------------------------------------
        # Create the datastore first,
        #--------------------------------------------------------------------------------
        # There is a race condition sometimes between the services and the process for
        # the creation of the datastore and it's instance, this ensures the datastore
        # exists before the process is even subscribing to data.
        self.get_datastore(dataset_id) 

        self.publish_fake_data(stream_id, route)
        self.wait_until_we_have_enough_granules(dataset_id,2) # I just need two

        replay_granule = self.data_retriever.retrieve(dataset_id,{'start_time':0,'end_time':6})

        rdt = RecordDictionaryTool.load_from_granule(replay_granule)

        comp = rdt['time'] == np.array([0,1,2,3,4,5])

        try:
            log.info('Compared granule: %s', replay_granule.__dict__)
            log.info('Granule tax: %s', replay_granule.taxonomy.__dict__)
        except:
            pass
        self.assertTrue(comp.all())

    def test_last_granule(self):
        #--------------------------------------------------------------------------------
        # Create the necessary configurations for the test
        #--------------------------------------------------------------------------------
        stream_id, route  = self.pubsub_management.create_stream('last_granule', exchange_point=self.exchange_point_name)
        config_id         = self.get_ingestion_config()
        dataset_id        = self.create_dataset()
        self.ingestion_management.persist_data_stream(stream_id=stream_id, ingestion_configuration_id=config_id, dataset_id=dataset_id)
        #--------------------------------------------------------------------------------
        # Create the datastore first,
        #--------------------------------------------------------------------------------
        self.get_datastore(dataset_id)

        self.publish_hifi(stream_id,route, 0)
        self.publish_hifi(stream_id,route, 1)
        

        self.wait_until_we_have_enough_granules(dataset_id,2) # I just need two

        replay_granule = self.data_retriever.retrieve_last_granule(dataset_id)

        rdt = RecordDictionaryTool.load_from_granule(replay_granule)

        comp = rdt['time'] == np.arange(10) + 10

        self.assertTrue(comp.all())


    def test_replay_with_parameters(self):
        #--------------------------------------------------------------------------------
        # Create the configurations and the dataset
        #--------------------------------------------------------------------------------
        stream_id, route  = self.pubsub_management.create_stream('replay_with_params', exchange_point=self.exchange_point_name)
        config_id  = self.get_ingestion_config()
        dataset_id = self.create_dataset()
        self.ingestion_management.persist_data_stream(stream_id=stream_id, ingestion_configuration_id=config_id, dataset_id=dataset_id)


        #--------------------------------------------------------------------------------
        # Coerce the datastore into existence (beats race condition)
        #--------------------------------------------------------------------------------
        self.get_datastore(dataset_id)

        self.launch_producer(stream_id)

        self.wait_until_we_have_enough_granules(dataset_id,4)

        query = {
            'start_time': 0,
            'end_time':   20,
            'stride_time' : 2,
            'parameters': ['time','temp']
        }
        retrieved_data = self.data_retriever.retrieve(dataset_id=dataset_id,query=query)

        rdt = RecordDictionaryTool.load_from_granule(retrieved_data)
        comp = np.arange(0,20,2) == rdt['time']
        self.assertTrue(comp.all(),'%s' % rdt.pretty_print())
        self.assertEquals(set(rdt.iterkeys()), set(['time','temp']))



    def test_repersist_data(self):
        stream_id, route = self.pubsub_management.create_stream(name='repersist', exchange_point=self.exchange_point_name)
        config_id = self.get_ingestion_config()
        dataset_id = self.create_dataset()
        self.ingestion_management.persist_data_stream(stream_id=stream_id, ingestion_configuration_id=config_id, dataset_id=dataset_id)
        self.get_datastore(dataset_id)
        self.publish_hifi(stream_id,route,0)
        self.publish_hifi(stream_id,route,1)
        self.wait_until_we_have_enough_granules(dataset_id,2)
        self.ingestion_management.unpersist_data_stream(stream_id=stream_id,ingestion_configuration_id=config_id)
        self.ingestion_management.persist_data_stream(stream_id=stream_id,ingestion_configuration_id=config_id,dataset_id=dataset_id)
        self.publish_hifi(stream_id,route,2)
        self.publish_hifi(stream_id,route,3)
        self.wait_until_we_have_enough_granules(dataset_id,4)
        retrieved_granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(retrieved_granule)
        comp = rdt['time'] == np.arange(0,40)
        self.assertTrue(comp.all(), 'Uh-oh: %s' % rdt['time'])

    def test_binary_ingestion(self):
        # Force the datastore to be created

        datastore = self.container.datastore_manager.get_datastore('filesystem', DataStore.DS_PROFILE.FILESYSTEM)
        #--------------------------------------------------------------------------------
        # Set up the ingestion subscriptions
        #--------------------------------------------------------------------------------
        success   = gevent.event.Event()
        stream_id, stream_route = self.pubsub_management.create_stream('test_stream',exchange_point=self.exchange_point_name)
        config_id = self.get_ingestion_config()
        self.ingestion_management.persist_data_stream(stream_id=stream_id, ingestion_configuration_id=config_id, dataset_id='', ingestion_type=IngestionManagementService.BINARY_INGESTION)


        #--------------------------------------------------------------------------------
        # Get the datastore to beat the race conditions
        #--------------------------------------------------------------------------------
        self.publish_file(stream_id, stream_route) 
        self.launched_event = gevent.event.Event()

        def launched(*args, **kwargs):
            self.launched_event.set()
       
        def notice(m,r,s):
            log.info( 'received: %s' , m)
            success.set()
        query =  {
           'file_stream_id': stream_id,
           'file_group_id':  'example1'
        }


        stream_id, route = self.pubsub_management.create_stream('replay_stream', exchange_point=self.exchange_point_name)
        replay_id = self.data_retriever.define_replay('', query, stream_id=stream_id, replay_type='BINARY')
        pid = self.data_retriever.read_process_id(replay_id)
        event_sub = EventSubscriber(event_type="ProcessLifecycleEvent", callback=launched, origin=pid, origin_type="DispatchedProcess")
        event_sub.start()


        sub = StandaloneStreamSubscriber('test_binary_ingestion', notice)
        self.queue_buffer.append(sub.xn)
        sub.start()

        xp = self.container.ex_manager.create_xp('science_data')
        sub.xn.bind(route.routing_key,xp)

        self.wait_until_we_have_enough_files()

        if self.launched_event.wait(10):
            self.data_retriever.start_replay(replay_id)
        self.assertTrue(success.wait(10))
        self.data_retriever.cancel_replay(replay_id)


        event_sub.stop()
        event_sub.close()
