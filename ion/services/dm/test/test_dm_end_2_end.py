#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file test_dm_end_2_end
@date 06/29/12 13:58
@description Complete DM End to End Integration Tests
'''
from pyon.datastore.datastore import DataStore
from pyon.event.event import EventSubscriber
from pyon.ion.exchange import ExchangeNameQueue
from pyon.ion.stream import StandaloneStreamSubscriber, StandaloneStreamPublisher
from pyon.public import RT, log, OT, PRED, CFG
from pyon.util.containers import DotDict
from pyon.util.poller import poll
from pyon.util.int_test import IonIntegrationTestCase
from pyon.container.cc import Container

from ion.processes.data.replay.replay_client import ReplayClient
from ion.services.dm.ingestion.test.ingestion_management_test import IngestionManagementIntTest
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.dm.inventory.data_retriever_service import DataRetrieverService
from ion.services.dm.utility.granule_utils import RecordDictionaryTool, CoverageCraft, time_series_domain
from ion.services.dm.utility.test.parameter_helper import ParameterHelper
from ion.util.stored_values import StoredValueManager

from coverage_model.parameter import ParameterContext
from coverage_model.parameter_types import ArrayType, RecordType
from coverage_model import ParameterFunctionType, NumexprFunction, QuantityType, SparseConstantType, BooleanType, ConstantType, ViewCoverage, ComplexCoverage, SimplexCoverage

from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.objects import Granule, Dataset

from gevent.event import Event
from nose.plugins.attrib import attr

from uuid import uuid4

import gevent
import time
import numpy as np
import os
import unittest

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
        self.event                = Event()
        self.exchange_space_name  = 'test_granules'
        self.exchange_point_name  = 'science_data'       
        self.i                    = 0
        self.cci                  = 0

    #--------------------------------------------------------------------------------
    # Helper/Utility methods
    #--------------------------------------------------------------------------------
        
    def create_dataset(self, parameter_dict_id=''):
        '''
        Creates a time-series dataset
        '''
        if not parameter_dict_id:
            parameter_dict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)

        dataset = Dataset('test_dataset_%i'%self.i)
        dataset_id = self.dataset_management.create_dataset(dataset, parameter_dictionary_id=parameter_dict_id)
        self.addCleanup(self.dataset_management.delete_dataset, dataset_id)
        return dataset_id
    
    def get_datastore(self, dataset_id):
        '''
        Gets an instance of the datastore
            This method is primarily used to defeat a bug where integration tests in multiple containers may sometimes 
            delete a CouchDB datastore and the other containers are unaware of the new state of the datastore.
        '''
        dataset = self.dataset_management.read_dataset(dataset_id)
        datastore_name = dataset.datastore_name
        datastore = self.container.datastore_manager.get_datastore(datastore_name, DataStore.DS_PROFILE.SCIDATA)
        return datastore
    
    def get_ingestion_config(self):
        '''
        Grab the ingestion configuration from the resource registry
        '''
        # The ingestion configuration should have been created by the bootstrap service 
        # which is configured through r2deploy.yml

        ingest_configs, _  = self.resource_registry.find_resources(restype=RT.IngestionConfiguration,id_only=True)
        return ingest_configs[0]

    def launch_producer(self, stream_id=''):
        '''
        Launch the producer
        '''
        pid = self.container.spawn_process('better_data_producer', 'ion.processes.data.example_data_producer', 'BetterDataProducer', {'process':{'stream_id':stream_id}})
        self.addCleanup(self.container.terminate_process, pid)

    def make_simple_dataset(self):
        '''
        Makes a stream, a stream definition and a dataset, the essentials for most of these tests
        '''
        pdict_id             = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        stream_def_id        = self.pubsub_management.create_stream_definition('ctd data %i' % self.i, parameter_dictionary_id=pdict_id)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)
        stream_id, route     = self.pubsub_management.create_stream('ctd stream %i' % self.i, 'xp1', stream_definition_id=stream_def_id)
        self.addCleanup(self.pubsub_management.delete_stream, stream_id)

        dataset_id = self.create_dataset(pdict_id)

        # self.get_datastore(dataset_id)
        self.i += 1
        return stream_id, route, stream_def_id, dataset_id

    def publish_hifi(self,stream_id,stream_route,offset=0):
        '''
        Publish deterministic data
        '''

        pub = StandaloneStreamPublisher(stream_id, stream_route)

        stream_def = self.pubsub_management.read_stream_definition(stream_id=stream_id)
        stream_def_id = stream_def._id
        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = np.arange(10) + (offset * 10)
        rdt['temp'] = np.arange(10) + (offset * 10)
        pub.publish(rdt.to_granule())

    def publish_fake_data(self,stream_id, route):
        '''
        Make four granules
        '''
        for i in xrange(4):
            self.publish_hifi(stream_id,route,i)

    def start_ingestion(self, stream_id, dataset_id):
        '''
        Starts ingestion/persistence for a given dataset
        '''
        ingest_config_id = self.get_ingestion_config()
        self.ingestion_management.persist_data_stream(stream_id=stream_id, ingestion_configuration_id=ingest_config_id, dataset_id=dataset_id)
    
    def stop_ingestion(self, stream_id):
        ingest_config_id = self.get_ingestion_config()
        self.ingestion_management.unpersist_data_stream(stream_id=stream_id, ingestion_configuration_id=ingest_config_id)

    def validate_granule_subscription(self, msg, route, stream_id):
        '''
        Validation for granule format
        '''
        if msg == {}:
            return
        rdt = RecordDictionaryTool.load_from_granule(msg)
        log.info('%s', rdt.pretty_print())
        self.assertIsInstance(msg,Granule,'Message is improperly formatted. (%s)' % type(msg))
        self.event.set()

    def wait_until_we_have_enough_granules(self, dataset_id='',data_size=40):
        '''
        Loops until there is a sufficient amount of data in the dataset
        '''
        done = False
        with gevent.Timeout(40):
            while not done:
                extents = self.dataset_management.dataset_extents(dataset_id, 'time')
                granule = self.data_retriever.retrieve_last_data_points(dataset_id, 1)
                rdt     = RecordDictionaryTool.load_from_granule(granule)
                if rdt['time'] and rdt['time'][0] != rdt._pdict.get_context('time').fill_value and extents >= data_size:
                    done = True
                else:
                    gevent.sleep(0.2)


    #--------------------------------------------------------------------------------
    # Test Methods
    #--------------------------------------------------------------------------------

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
        self.addCleanup(self.stop_ingestion, stream_id)

        #--------------------------------------------------------------------------------
        # Now the granules are ingesting and persisted
        #--------------------------------------------------------------------------------

        self.launch_producer(stream_id)
        self.wait_until_we_have_enough_granules(dataset_id,40)
        
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
        sub_id = self.pubsub_management.create_subscription(self.exchange_space_name,stream_ids=[replay_stream_id])
        self.addCleanup(self.pubsub_management.delete_subscription, sub_id)
        self.pubsub_management.activate_subscription(sub_id)
        self.addCleanup(self.pubsub_management.deactivate_subscription, sub_id)
        subscriber = StandaloneStreamSubscriber(self.exchange_space_name, self.validate_granule_subscription)
        subscriber.start()
        self.addCleanup(subscriber.stop)

        self.data_retriever.start_replay_agent(self.replay_id)

        self.assertTrue(replay_client.await_agent_ready(5), 'The process never launched')
        replay_client.start_replay()
        
        self.assertTrue(self.event.wait(10))

        self.data_retriever.cancel_replay_agent(self.replay_id)


        #--------------------------------------------------------------------------------
        # Test the slicing capabilities
        #--------------------------------------------------------------------------------

        granule = self.data_retriever.retrieve(dataset_id=dataset_id, query={'tdoa':slice(0,5)})
        rdt = RecordDictionaryTool.load_from_granule(granule)
        b = rdt['time'] == np.arange(5)
        self.assertTrue(b.all() if not isinstance(b,bool) else b)


    def test_coverage_transform(self):
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        pdict_id = ph.create_parsed()
        stream_def_id = self.pubsub_management.create_stream_definition('ctd parsed', parameter_dictionary_id=pdict_id)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)

        stream_id, route = self.pubsub_management.create_stream('example', exchange_point=self.exchange_point_name, stream_definition_id=stream_def_id)
        self.addCleanup(self.pubsub_management.delete_stream, stream_id)

        ingestion_config_id = self.get_ingestion_config()
        dataset_id = self.create_dataset(pdict_id)

        self.ingestion_management.persist_data_stream(stream_id=stream_id, ingestion_configuration_id=ingestion_config_id, dataset_id=dataset_id)
        self.addCleanup(self.ingestion_management.unpersist_data_stream, stream_id, ingestion_config_id)
        publisher = StandaloneStreamPublisher(stream_id, route)
        
        rdt = ph.get_rdt(stream_def_id)
        ph.fill_parsed_rdt(rdt)

        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)

        publisher.publish(rdt.to_granule())
        self.assertTrue(dataset_monitor.wait())

        replay_granule = self.data_retriever.retrieve(dataset_id)
        rdt_out = RecordDictionaryTool.load_from_granule(replay_granule)

        np.testing.assert_array_almost_equal(rdt_out['time'], rdt['time'])
        np.testing.assert_array_almost_equal(rdt_out['temp'], rdt['temp'])

        np.testing.assert_array_almost_equal(rdt_out['conductivity_L1'], np.array([42.914]))
        np.testing.assert_array_almost_equal(rdt_out['temp_L1'], np.array([20.]))
        np.testing.assert_array_almost_equal(rdt_out['pressure_L1'], np.array([3.068]))
        np.testing.assert_array_almost_equal(rdt_out['density'], np.array([1021.7144739593881], dtype='float32'))
        np.testing.assert_array_almost_equal(rdt_out['salinity'], np.array([30.935132729668283], dtype='float32'))


    def test_ingestion_pause(self):
        ctd_stream_id, route, stream_def_id, dataset_id = self.make_simple_dataset()
        ingestion_config_id = self.get_ingestion_config()
        self.start_ingestion(ctd_stream_id, dataset_id)
        self.addCleanup(self.stop_ingestion, ctd_stream_id)

        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = np.arange(10)

        publisher = StandaloneStreamPublisher(ctd_stream_id, route)
        monitor = DatasetMonitor(dataset_id)
        self.addCleanup(monitor.stop)
        publisher.publish(rdt.to_granule())
        self.assertTrue(monitor.wait())
        granule = self.data_retriever.retrieve(dataset_id)


        self.ingestion_management.pause_data_stream(ctd_stream_id, ingestion_config_id)

        monitor.event.clear()
        rdt['time'] = np.arange(10,20)
        publisher.publish(rdt.to_granule())
        self.assertFalse(monitor.event.wait(1))

        self.ingestion_management.resume_data_stream(ctd_stream_id, ingestion_config_id)

        self.assertTrue(monitor.wait())

        granule = self.data_retriever.retrieve(dataset_id)
        rdt2 = RecordDictionaryTool.load_from_granule(granule)
        np.testing.assert_array_almost_equal(rdt2['time'], np.arange(20))

    def test_last_granule(self):
        stream_id, route, stream_def_id, dataset_id = self.make_simple_dataset()
        self.start_ingestion(stream_id, dataset_id)
        self.addCleanup(self.stop_ingestion, stream_id)

        self.publish_hifi(stream_id,route, 0)
        self.publish_hifi(stream_id,route, 1)
        

        self.wait_until_we_have_enough_granules(dataset_id,20) # I just need two


        success = False
        def verifier():
                replay_granule = self.data_retriever.retrieve_last_data_points(dataset_id, 10)

                rdt = RecordDictionaryTool.load_from_granule(replay_granule)

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
        self.addCleanup(self.stop_ingestion, stream_id)

        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)

        self.publish_fake_data(stream_id, route)

        self.assertTrue(dataset_monitor.wait())

        query = {
            'start_time': 0 - 2208988800,
            'end_time':   19 - 2208988800,
            'stride_time' : 2,
            'parameters': ['time','temp']
        }
        retrieved_data = self.data_retriever.retrieve(dataset_id=dataset_id,query=query)

        rdt = RecordDictionaryTool.load_from_granule(retrieved_data)
        np.testing.assert_array_equal(rdt['time'], np.arange(0,20,2))
        self.assertEquals(set(rdt.iterkeys()), set(['time','temp']))

        extents = self.dataset_management.dataset_extents(dataset_id=dataset_id, parameters=['time','temp'])
        self.assertTrue(extents['time']>=20)
        self.assertTrue(extents['temp']>=20)

    def test_repersist_data(self):
        stream_id, route, stream_def_id, dataset_id = self.make_simple_dataset()
        self.start_ingestion(stream_id, dataset_id)
        self.publish_hifi(stream_id,route,0)
        self.publish_hifi(stream_id,route,1)
        self.wait_until_we_have_enough_granules(dataset_id,20)
        config_id = self.get_ingestion_config()
        self.ingestion_management.unpersist_data_stream(stream_id=stream_id,ingestion_configuration_id=config_id)
        self.ingestion_management.persist_data_stream(stream_id=stream_id,ingestion_configuration_id=config_id,dataset_id=dataset_id)
        self.addCleanup(self.stop_ingestion, stream_id)
        self.publish_hifi(stream_id,route,2)
        self.publish_hifi(stream_id,route,3)
        self.wait_until_we_have_enough_granules(dataset_id,40)
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


    @unittest.skip('deprecated')
    def test_correct_time(self):

        # There are 2208988800 seconds between Jan 1 1900 and Jan 1 1970, i.e. 
        #  the conversion factor between unix and NTP time
        unix_now = np.floor(time.time())
        ntp_now  = unix_now + 2208988800 

        unix_ago = unix_now - 20
        ntp_ago  = unix_ago + 2208988800

        stream_id, route, stream_def_id, dataset_id = self.make_simple_dataset()
        coverage = DatasetManagementService._get_simplex_coverage(dataset_id, mode='a')
        coverage.insert_timesteps(20)
        coverage.set_parameter_values('time', np.arange(ntp_ago,ntp_now))
        
        temporal_bounds = self.dataset_management.dataset_temporal_bounds(dataset_id)

        self.assertTrue( np.abs(temporal_bounds[0] - unix_ago) < 2)
        self.assertTrue( np.abs(temporal_bounds[1] - unix_now) < 2)


    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Host requires file-system access to coverage files, CEI mode does not support.')
    def test_out_of_band_retrieve(self):
        # Setup the environemnt
        stream_id, route, stream_def_id, dataset_id = self.make_simple_dataset()
        self.start_ingestion(stream_id, dataset_id)
        
        # Fill the dataset
        self.publish_fake_data(stream_id, route)
        self.wait_until_we_have_enough_granules(dataset_id,40)

        # Retrieve the data
        granule = DataRetrieverService.retrieve_oob(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(granule)
        self.assertTrue((rdt['time'] == np.arange(40)).all())

    def publish_and_wait(self, dataset_id, granule):
        stream_ids, _ = self.resource_registry.find_objects(dataset_id, PRED.hasStream,id_only=True)
        stream_id=stream_ids[0]
        route = self.pubsub_management.read_stream_route(stream_id)
        publisher = StandaloneStreamPublisher(stream_id,route)
        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)
        publisher.publish(granule)
        self.assertTrue(dataset_monitor.wait())

    @unittest.skip('Gap support disabled')
    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Host requires file-system access to coverage files, CEI mode does not support.')
    def test_thorough_gap_analysis(self):
        dataset_id = self.test_ingestion_gap_analysis()
        vcov = DatasetManagementService._get_coverage(dataset_id)

        self.assertIsInstance(vcov,ViewCoverage)
        ccov = vcov.reference_coverage

        self.assertIsInstance(ccov, ComplexCoverage)
        self.assertEquals(len(ccov._reference_covs), 3)


    def test_ingestion_gap_analysis(self):
        stream_id, route, stream_def_id, dataset_id = self.make_simple_dataset()
        self.start_ingestion(stream_id, dataset_id)
        self.addCleanup(self.stop_ingestion, stream_id)

        connection1 = uuid4().hex
        connection2 = uuid4().hex

        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = [0]
        rdt['temp'] = [0]
        self.publish_and_wait(dataset_id, rdt.to_granule(connection_id=connection1,connection_index='0'))
        rdt['time'] = [1]
        rdt['temp'] = [1]
        self.publish_and_wait(dataset_id, rdt.to_granule(connection_id=connection1,connection_index='1'))
        rdt['time'] = [2]
        rdt['temp'] = [2]
        self.publish_and_wait(dataset_id, rdt.to_granule(connection_id=connection1,connection_index='3')) # Gap, missed message
        rdt['time'] = [3]
        rdt['temp'] = [3]
        self.publish_and_wait(dataset_id, rdt.to_granule(connection_id=connection2,connection_index='3')) # Gap, new connection
        rdt['time'] = [4]
        rdt['temp'] = [4]
        self.publish_and_wait(dataset_id, rdt.to_granule(connection_id=connection2,connection_index='4'))
        rdt['time'] = [5]
        rdt['temp'] = [5]
        self.publish_and_wait(dataset_id, rdt.to_granule(connection_id=connection2,connection_index='5'))

        granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(granule)
        np.testing.assert_array_equal(rdt['time'], np.arange(6))
        np.testing.assert_array_equal(rdt['temp'], np.arange(6))
        return dataset_id

    def test_sparse_values(self):
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        pdict_id = ph.create_sparse()
        stream_def_id = self.pubsub_management.create_stream_definition('sparse', parameter_dictionary_id=pdict_id)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)
        stream_id, route = self.pubsub_management.create_stream('example', exchange_point=self.exchange_point_name, stream_definition_id=stream_def_id)
        self.addCleanup(self.pubsub_management.delete_stream, stream_id)
        dataset_id = self.create_dataset(pdict_id)
        self.start_ingestion(stream_id,dataset_id)
        self.addCleanup(self.stop_ingestion, stream_id)

        # Publish initial granule
        # the first one has the sparse value set inside it, sets lat to 45 and lon to -71
        ntp_now = time.time() + 2208988800
        rdt = ph.get_rdt(stream_def_id)
        rdt['time'] = [ntp_now]
        rdt['internal_timestamp'] = [ntp_now]
        rdt['temp'] = [300000]
        rdt['preferred_timestamp'] = ['driver_timestamp']
        rdt['port_timestamp'] = [ntp_now]
        rdt['quality_flag'] = [None]
        rdt['lat'] = [45]
        rdt['conductivity'] = [4341400]
        rdt['driver_timestamp'] = [ntp_now]
        rdt['lon'] = [-71]
        rdt['pressure'] = [256.8]

        publisher = StandaloneStreamPublisher(stream_id, route)
        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)
        publisher.publish(rdt.to_granule())
        self.assertTrue(dataset_monitor.wait())
        dataset_monitor.reset()

        replay_granule = self.data_retriever.retrieve(dataset_id)
        rdt_out = RecordDictionaryTool.load_from_granule(replay_granule)

        # Check the values and make sure they're correct
        np.testing.assert_array_almost_equal(rdt_out['time'], rdt['time'])
        np.testing.assert_array_almost_equal(rdt_out['temp'], rdt['temp'])
        np.testing.assert_array_almost_equal(rdt_out['lat'], np.array([45]))
        np.testing.assert_array_almost_equal(rdt_out['lon'], np.array([-71]))

        np.testing.assert_array_almost_equal(rdt_out['conductivity_L1'], np.array([42.914]))
        np.testing.assert_array_almost_equal(rdt_out['temp_L1'], np.array([20.]))
        np.testing.assert_array_almost_equal(rdt_out['pressure_L1'], np.array([3.068]))
        np.testing.assert_array_almost_equal(rdt_out['density'], np.array([1021.7144739593881], dtype='float32'))
        np.testing.assert_array_almost_equal(rdt_out['salinity'], np.array([30.935132729668283], dtype='float32'))


        # We're going to change the lat/lon
        rdt = ph.get_rdt(stream_def_id)
        rdt['time'] = time.time() + 2208988800
        rdt['lat'] = [46]
        rdt['lon'] = [-73]
        
        publisher.publish(rdt.to_granule())
        self.assertTrue(dataset_monitor.wait())
        dataset_monitor.reset()


        replay_granule = self.data_retriever.retrieve(dataset_id)
        rdt_out = RecordDictionaryTool.load_from_granule(replay_granule)

        np.testing.assert_allclose(rdt_out['time'], rdt['time'])
        
        for i in xrange(9):
            ntp_now = time.time() + 2208988800
            rdt['time'] = [ntp_now]
            rdt['internal_timestamp'] = [ntp_now]
            rdt['temp'] = [300000]
            rdt['preferred_timestamp'] = ['driver_timestamp']
            rdt['port_timestamp'] = [ntp_now]
            rdt['quality_flag'] = [None]
            rdt['conductivity'] = [4341400]
            rdt['driver_timestamp'] = [ntp_now]
            rdt['pressure'] = [256.8]

            publisher.publish(rdt.to_granule())
            self.assertTrue(dataset_monitor.wait())
            dataset_monitor.reset()

        replay_granule = self.data_retriever.retrieve(dataset_id)
        rdt_out = RecordDictionaryTool.load_from_granule(replay_granule)

        np.testing.assert_allclose(rdt_out['pressure'], np.array([256.8] * 10))
        np.testing.assert_allclose(rdt_out['lat'], np.array([45] + [46] * 9))
        np.testing.assert_allclose(rdt_out['lon'], np.array([-71] + [-73] * 9))



class DatasetMonitor(object):
    def __init__(self, dataset_id=None, data_product_id=None):
        if data_product_id and not dataset_id:
            dataset_id = Container.instance.resource_registry.find_objects(data_product_id, PRED.hasDataset, id_only=True)[0][0]
        self.dataset_id = dataset_id
        self.event = Event()

        self.es = EventSubscriber(event_type=OT.DatasetModified, callback=self.cb, origin=self.dataset_id, auto_delete=True)
        self.es.start()
    
    def cb(self, *args, **kwargs):
        self.event.set()

    def stop(self):
        self.es.stop()

    def wait(self, timeout=None):
        if timeout is None:
            timeout = CFG.get_safe('endpoint.receive.timeout', 10)
        return self.event.wait(timeout)

    def reset(self):
        self.event.clear()


