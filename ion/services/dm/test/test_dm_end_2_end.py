#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file test_dm_end_2_end
@date 06/29/12 13:58
@description Complete DM End to End Integration Tests
'''
from pyon.core.exception import Timeout
from pyon.datastore.datastore import DataStore
from pyon.ion.exchange import ExchangeNameQueue
from pyon.ion.stream import StandaloneStreamSubscriber, StandaloneStreamPublisher
from pyon.public import RT, log
from pyon.util.poller import poll
from pyon.util.int_test import IonIntegrationTestCase

from ion.processes.data.replay.replay_client import ReplayClient
from ion.services.dm.ingestion.test.ingestion_management_test import IngestionManagementIntTest
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.dm.utility.granule_utils import RecordDictionaryTool, CoverageCraft, time_series_domain

from coverage_model.parameter import ParameterContext
from coverage_model.parameter_types import ArrayType, RecordType

from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.objects import Granule

from gevent.event import Event
from nose.plugins.attrib import attr

import gevent
import numpy as np
import os
import unittest
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
            elif isinstance(queue, str):
                xn = self.container.ex_manager.create_xn_queue(queue)
                xn.delete()

        

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

        stream_def = self.pubsub_management.read_stream_definition(stream_id=stream_id)
        stream_def_id = stream_def._id
        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = np.arange(10) + (offset * 10)
        rdt['temp'] = np.arange(10) + (offset * 10)
        pub.publish(rdt.to_granule())

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
        rdt = RecordDictionaryTool.load_from_granule(msg)
        log.info('%s', rdt.pretty_print())
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
        
        with gevent.timeout.Timeout(40):
            success = False
            while not success:
                success = len(datastore.query_view(dataset.view_name)) >= granules
                gevent.sleep(0.1)

        log.info(datastore.query_view(dataset.view_name))




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


    def create_dataset(self, parameter_dict_id=''):
        tdom, sdom = time_series_domain()
        sdom = sdom.dump()
        tdom = tdom.dump()
        if not parameter_dict_id:
            parameter_dict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)

        dataset_id = self.dataset_management.create_dataset('test_dataset', parameter_dictionary_id=parameter_dict_id, spatial_domain=sdom, temporal_domain=tdom)
        return dataset_id

    @unittest.skip('Doesnt work')
    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_replay_pause(self):
        # Get a precompiled parameter dictionary with basic ctd fields
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict',id_only=True)
        context_ids = self.dataset_management.read_parameter_contexts(pdict_id, id_only=True)

        # Add a field that supports binary data input.
        bin_context = ParameterContext('binary',  param_type=ArrayType())
        context_ids.append(self.dataset_management.create_parameter_context('binary', bin_context.dump()))
        # Add another field that supports dictionary elements.
        rec_context = ParameterContext('records', param_type=RecordType())
        context_ids.append(self.dataset_management.create_parameter_context('records', rec_context.dump()))

        pdict_id = self.dataset_management.create_parameter_dictionary('replay_pdict', parameter_context_ids=context_ids, temporal_context='time')
        

        stream_def_id = self.pubsub_management.create_stream_definition('replay_stream', parameter_dictionary_id=pdict_id)
        replay_stream, replay_route = self.pubsub_management.create_stream('replay', 'xp1', stream_definition_id=stream_def_id)
        dataset_id = self.create_dataset(pdict_id)
        scov = DatasetManagementService._get_coverage(dataset_id)

        bb = CoverageCraft(scov)
        bb.rdt['time'] = np.arange(100)
        bb.rdt['temp'] = np.random.random(100) + 30
        bb.sync_with_granule()

        DatasetManagementService._persist_coverage(dataset_id, bb.coverage) # This invalidates it for multi-host configurations
        # Set up the subscriber to verify the data
        subscriber = StandaloneStreamSubscriber(self.exchange_space_name, self.validate_granule_subscription)
        xp = self.container.ex_manager.create_xp('xp1')
        self.queue_buffer.append(self.exchange_space_name)
        subscriber.start()
        subscriber.xn.bind(replay_route.routing_key, xp)

        # Set up the replay agent and the client wrapper

        # 1) Define the Replay (dataset and stream to publish on)
        self.replay_id, process_id = self.data_retriever.define_replay(dataset_id=dataset_id, stream_id=replay_stream)
        # 2) Make a client to the interact with the process (optionall provide it a process to bind with)
        replay_client = ReplayClient(process_id)
        # 3) Start the agent (launch the process)
        self.data_retriever.start_replay_agent(self.replay_id)
        # 4) Start replaying...
        replay_client.start_replay()
        
        # Wait till we get some granules
        self.assertTrue(self.event.wait(5))
        
        # We got granules, pause the replay, clear the queue and allow the process to finish consuming
        replay_client.pause_replay()
        gevent.sleep(1)
        subscriber.xn.purge()
        self.event.clear()
        
        # Make sure there's no remaining messages being consumed
        self.assertFalse(self.event.wait(1))

        # Resume the replay and wait until we start getting granules again
        replay_client.resume_replay()
        self.assertTrue(self.event.wait(5))
    
        # Stop the replay, clear the queues
        replay_client.stop_replay()
        gevent.sleep(1)
        subscriber.xn.purge()
        self.event.clear()

        # Make sure that it did indeed stop
        self.assertFalse(self.event.wait(1))

        subscriber.stop()


    @attr('SMOKE') 
    def test_dm_end_2_end(self):
        #--------------------------------------------------------------------------------
        # Set up a stream and have a mock instrument (producer) send data
        #--------------------------------------------------------------------------------
        self.event.clear()

        # Get a precompiled parameter dictionary with basic ctd fields
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict',id_only=True)
        context_ids = self.dataset_management.read_parameter_contexts(pdict_id, id_only=True)

        # Add a field that supports binary data input.
        bin_context = ParameterContext('binary',  param_type=ArrayType())
        context_ids.append(self.dataset_management.create_parameter_context('binary', bin_context.dump()))
        # Add another field that supports dictionary elements.
        rec_context = ParameterContext('records', param_type=RecordType())
        context_ids.append(self.dataset_management.create_parameter_context('records', rec_context.dump()))

        pdict_id = self.dataset_management.create_parameter_dictionary('replay_pdict', parameter_context_ids=context_ids, temporal_context='time')
        
        stream_definition = self.pubsub_management.create_stream_definition('ctd data', parameter_dictionary_id=pdict_id)


        stream_id, route = self.pubsub_management.create_stream('producer', exchange_point=self.exchange_point_name, stream_definition_id=stream_definition)




        #--------------------------------------------------------------------------------
        # Start persisting the data on the stream 
        # - Get the ingestion configuration from the resource registry
        # - Create the dataset
        # - call persist_data_stream to setup the subscription for the ingestion workers
        #   on the stream that you specify which causes the data to be persisted
        #--------------------------------------------------------------------------------

        ingest_config_id = self.get_ingestion_config()
        dataset_id = self.create_dataset(pdict_id)
        self.ingestion_management.persist_data_stream(stream_id=stream_id, ingestion_configuration_id=ingest_config_id, dataset_id=dataset_id)

        #--------------------------------------------------------------------------------
        # Now the granules are ingesting and persisted
        #--------------------------------------------------------------------------------

        self.launch_producer(stream_id)
        self.wait_until_we_have_enough_granules(dataset_id,4)
        
        #--------------------------------------------------------------------------------
        # Now get the data in one chunk using an RPC Call to start_retreive
        #--------------------------------------------------------------------------------
        
        replay_data = self.data_retriever.retrieve(dataset_id)
        self.assertIsInstance(replay_data, Granule)
        rdt = RecordDictionaryTool.load_from_granule(replay_data)
        self.assertTrue((rdt['time'][:10] == np.arange(10)).all(),'%s' % rdt['time'][:])
        self.assertTrue((rdt['binary'][:10] == np.array(['hi']*10, dtype='object')).all())

        
        #--------------------------------------------------------------------------------
        # Now to try the streamed approach
        #--------------------------------------------------------------------------------
        replay_stream_id, replay_route = self.pubsub_management.create_stream('replay_out', exchange_point=self.exchange_point_name, stream_definition_id=stream_definition)
        self.replay_id, process_id =  self.data_retriever.define_replay(dataset_id=dataset_id, stream_id=replay_stream_id)
        log.info('Process ID: %s', process_id)

        replay_client = ReplayClient(process_id)

    
        #--------------------------------------------------------------------------------
        # Create the listening endpoint for the the retriever to talk to 
        #--------------------------------------------------------------------------------
        xp = self.container.ex_manager.create_xp(self.exchange_point_name)
        subscriber = StandaloneStreamSubscriber(self.exchange_space_name, self.validate_granule_subscription)
        self.queue_buffer.append(self.exchange_space_name)
        subscriber.start()
        subscriber.xn.bind(replay_route.routing_key, xp)

        self.data_retriever.start_replay_agent(self.replay_id)

        self.assertTrue(replay_client.await_agent_ready(5), 'The process never launched')
        replay_client.start_replay()
        
        self.assertTrue(self.event.wait(10))
        subscriber.stop()

        self.data_retriever.cancel_replay_agent(self.replay_id)


        #--------------------------------------------------------------------------------
        # Test the slicing capabilities
        #--------------------------------------------------------------------------------

        granule = self.data_retriever.retrieve(dataset_id=dataset_id, query={'tdoa':slice(0,5)})
        rdt = RecordDictionaryTool.load_from_granule(granule)
        b = rdt['time'] == np.arange(5)
        self.assertTrue(b.all() if not isinstance(b,bool) else b)



    def test_retrieve_and_transform(self):

        # Stream definition for the CTD data
        pdict_id             = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        stream_def_id        = self.pubsub_management.create_stream_definition('ctd data', parameter_dictionary_id=pdict_id)
        ctd_stream_id, route = self.pubsub_management.create_stream('ctd stream', 'xp1', stream_definition_id=stream_def_id)


        # Stream definition for the salinity data
        salinity_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        sal_stream_def_id = self.pubsub_management.create_stream_definition('sal data', parameter_dictionary_id=salinity_pdict_id)

        ingest_config_id = self.get_ingestion_config()
        dataset_id = self.create_dataset(pdict_id)
        #--------------------------------------------------------------------------------
        # Again with this ridiculous problem
        #--------------------------------------------------------------------------------
        self.get_datastore(dataset_id)
        self.ingestion_management.persist_data_stream(stream_id=ctd_stream_id, ingestion_configuration_id=ingest_config_id, dataset_id=dataset_id)

        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = np.arange(10)
        rdt['temp'] = np.random.randn(10) * 10 + 30
        rdt['conductivity'] = np.random.randn(10) * 2 + 10

        publisher = StandaloneStreamPublisher(ctd_stream_id, route)
        publisher.publish(rdt.to_granule())

        rdt['time'] = np.arange(10,20)

        publisher.publish(rdt.to_granule())


        self.wait_until_we_have_enough_granules(dataset_id, 2)

        granule = self.data_retriever.retrieve(dataset_id, 
                                             None,
                                             None, 
                                             'ion.processes.data.transforms.ctd.ctd_L2_salinity',
                                             'CTDL2SalinityTransformAlgorithm', 
                                             kwargs=dict(params=sal_stream_def_id))
        rdt = RecordDictionaryTool.load_from_granule(granule)
        for i in rdt['salinity']:
            self.assertNotEquals(i,0)



    def test_last_granule(self):
        #--------------------------------------------------------------------------------
        # Create the necessary configurations for the test
        #--------------------------------------------------------------------------------
        pdict_id          = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        stream_def_id     = self.pubsub_management.create_stream_definition('ctd parsed', parameter_dictionary_id=pdict_id)
        stream_id, route  = self.pubsub_management.create_stream('last_granule', exchange_point=self.exchange_point_name, stream_definition_id=stream_def_id)
        config_id         = self.get_ingestion_config()
        dataset_id        = self.create_dataset(pdict_id)

        self.ingestion_management.persist_data_stream(stream_id=stream_id, ingestion_configuration_id=config_id, dataset_id=dataset_id)
        #--------------------------------------------------------------------------------
        # Create the datastore first,
        #--------------------------------------------------------------------------------
        self.get_datastore(dataset_id)

        self.publish_hifi(stream_id,route, 0)
        self.publish_hifi(stream_id,route, 1)
        

        self.wait_until_we_have_enough_granules(dataset_id,2) # I just need two


        success = False
        def verifier():
                replay_granule = self.data_retriever.retrieve_last_granule(dataset_id)

                rdt = RecordDictionaryTool.load_from_granule(replay_granule)
                print rdt['time']

                comp = rdt['time'] == np.arange(10) + 10
                if not isinstance(comp,bool):
                    return comp.all()
                return False
        success = poll(verifier)

        self.assertTrue(success)

        success = False
        def verify_points():
                replay_granule = self.data_retriever.retrieve_last_data_points(dataset_id,5)

                rdt = RecordDictionaryTool.load_from_granule(replay_granule)

                comp = rdt['time'] == np.arange(15,20)
                if not isinstance(comp,bool):
                    return comp.all()
                return False
        success = poll(verify_points)

        self.assertTrue(success)



    def test_replay_with_parameters(self):
        #--------------------------------------------------------------------------------
        # Create the configurations and the dataset
        #--------------------------------------------------------------------------------
        # Get a precompiled parameter dictionary with basic ctd fields
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict',id_only=True)
        context_ids = self.dataset_management.read_parameter_contexts(pdict_id, id_only=True)

        # Add a field that supports binary data input.
        bin_context = ParameterContext('binary',  param_type=ArrayType())
        context_ids.append(self.dataset_management.create_parameter_context('binary', bin_context.dump()))
        # Add another field that supports dictionary elements.
        rec_context = ParameterContext('records', param_type=RecordType())
        context_ids.append(self.dataset_management.create_parameter_context('records', rec_context.dump()))

        pdict_id = self.dataset_management.create_parameter_dictionary('replay_pdict', parameter_context_ids=context_ids, temporal_context='time')
        

        stream_def_id = self.pubsub_management.create_stream_definition('replay_stream', parameter_dictionary_id=pdict_id)
        
        stream_id, route  = self.pubsub_management.create_stream('replay_with_params', exchange_point=self.exchange_point_name, stream_definition_id=stream_def_id)
        config_id  = self.get_ingestion_config()
        dataset_id = self.create_dataset(pdict_id)
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

        extents = self.dataset_management.dataset_extents(dataset_id=dataset_id, parameters=['time','temp'])
        self.assertTrue(extents['time']>=20)
        self.assertTrue(extents['temp']>=20)



    def test_repersist_data(self):
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict',id_only=True)
        stream_def_id = self.pubsub_management.create_stream_definition(name='ctd', parameter_dictionary_id=pdict_id)
        stream_id, route = self.pubsub_management.create_stream(name='repersist', exchange_point=self.exchange_point_name, stream_definition_id=stream_def_id)
        config_id = self.get_ingestion_config()
        dataset_id = self.create_dataset(pdict_id)
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
        success = False
        with gevent.timeout.Timeout(5):
            while not success:

                replay_granule = self.data_retriever.retrieve(dataset_id)

                rdt = RecordDictionaryTool.load_from_granule(replay_granule)

                comp = rdt['time'] == np.arange(0,40)
                if not isinstance(comp,bool):
                    success = comp.all()
                gevent.sleep(1)

        self.assertTrue(success)

