#!/usr/bin/env python
'''
@file ion/services/sa/product/test/test_data_product_management_service_integration.py
@brief Data Product Management Service Integration Tests
'''
from mock import patch
from pyon.core.exception import NotFound
from pyon.public import  IonObject
from pyon.public import RT, PRED, CFG, OT
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.log import log
from pyon.util.context import LocalContextMixin
from pyon.util.containers import DotDict
from pyon.datastore.datastore import DataStore
from pyon.ion.stream import StandaloneStreamSubscriber, StandaloneStreamPublisher
from pyon.ion.exchange import ExchangeNameQueue
from pyon.ion.event import EventSubscriber

from ion.processes.data.last_update_cache import CACHE_DATASTORE_NAME
from ion.services.dm.utility.granule_utils import time_series_domain
from ion.services.dm.utility.granule import RecordDictionaryTool
from ion.util.parameter_yaml_IO import get_param_dict
from interface.services.dm.iuser_notification_service import UserNotificationServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.idata_product_management_service import  DataProductManagementServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient

from ion.util.stored_values import StoredValueManager
from interface.objects import LastUpdate, ComputedValueAvailability, Granule, DataProduct
from ion.services.dm.ingestion.test.ingestion_management_test import IngestionManagementIntTest

from nose.plugins.attrib import attr
from interface.objects import ProcessDefinition

from coverage_model.basic_types import AxisTypeEnum, MutabilityEnum
from coverage_model.coverage import CRS, GridDomain, GridShape
from coverage_model import ParameterContext, ParameterFunctionType, NumexprFunction, QuantityType

from gevent.event import Event

import unittest, gevent
import numpy as np

from ion.services.dm.utility.granule import RecordDictionaryTool
from pyon.ion.event import EventSubscriber
from gevent.event import Event
from pyon.public import OT
from pyon.ion.stream import StandaloneStreamPublisher

class FakeProcess(LocalContextMixin):
    name = ''


@attr('INT', group='sa')
#@unittest.skip('not working')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class TestDataProductManagementServiceIntegration(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        #print 'instantiating container'
        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.dpsc_cli           = DataProductManagementServiceClient()
        self.rrclient           = ResourceRegistryServiceClient()
        self.damsclient         = DataAcquisitionManagementServiceClient()
        self.pubsubcli          = PubsubManagementServiceClient()
        self.ingestclient       = IngestionManagementServiceClient()
        self.process_dispatcher = ProcessDispatcherServiceClient()
        self.dataset_management = DatasetManagementServiceClient()
        self.unsc               = UserNotificationServiceClient()
        self.data_retriever     = DataRetrieverServiceClient()

        #------------------------------------------
        # Create the environment
        #------------------------------------------

        datastore_name = CACHE_DATASTORE_NAME
        self.db = self.container.datastore_manager.get_datastore(datastore_name)
        self.stream_def_id = self.pubsubcli.create_stream_definition(name='SBE37_CDM')

        self.process_definitions  = {}
        ingestion_worker_definition = ProcessDefinition(name='ingestion worker')
        ingestion_worker_definition.executable = {
            'module':'ion.processes.data.ingestion.science_granule_ingestion_worker',
            'class' :'ScienceGranuleIngestionWorker'
        }
        process_definition_id = self.process_dispatcher.create_process_definition(process_definition=ingestion_worker_definition)
        self.process_definitions['ingestion_worker'] = process_definition_id

        self.pids = []
        self.exchange_points = []
        self.exchange_names = []

        #------------------------------------------------------------------------------------------------
        # First launch the ingestors
        #------------------------------------------------------------------------------------------------
        self.exchange_space       = 'science_granule_ingestion'
        self.exchange_point       = 'science_data'
        config = DotDict()
        config.process.datastore_name = 'datasets'
        config.process.queue_name = self.exchange_space

        self.exchange_names.append(self.exchange_space)
        self.exchange_points.append(self.exchange_point)

        pid = self.process_dispatcher.schedule_process(self.process_definitions['ingestion_worker'],configuration=config)
        log.debug("the ingestion worker process id: %s", pid)
        self.pids.append(pid)

        self.addCleanup(self.cleaning_up)

    def cleaning_up(self):
        for pid in self.pids:
            log.debug("number of pids to be terminated: %s", len(self.pids))
            try:
                self.process_dispatcher.cancel_process(pid)
                log.debug("Terminated the process: %s", pid)
            except:
                log.debug("could not terminate the process id: %s" % pid)
        IngestionManagementIntTest.clean_subscriptions()

        for xn in self.exchange_names:
            xni = self.container.ex_manager.create_xn_queue(xn)
            xni.delete()
        for xp in self.exchange_points:
            xpi = self.container.ex_manager.create_xp(xp)
            xpi.delete()

    def get_datastore(self, dataset_id):
        dataset = self.dataset_management.read_dataset(dataset_id)
        datastore_name = dataset.datastore_name
        datastore = self.container.datastore_manager.get_datastore(datastore_name, DataStore.DS_PROFILE.SCIDATA)
        return datastore


    def test_create_data_product(self):

        #------------------------------------------------------------------------------------------------
        # create a stream definition for the data from the ctd simulator
        #------------------------------------------------------------------------------------------------
        parameter_dictionary_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict')
        ctd_stream_def_id = self.pubsubcli.create_stream_definition(name='Simulated CTD data', parameter_dictionary_id=parameter_dictionary_id)
        log.debug("Created stream def id %s" % ctd_stream_def_id)

        #------------------------------------------------------------------------------------------------
        # test creating a new data product w/o a stream definition
        #------------------------------------------------------------------------------------------------

        # Generic time-series data domain creation
        tdom, sdom = time_series_domain()



        dp_obj = IonObject(RT.DataProduct,
            name='DP1',
            description='some new dp',
            temporal_domain = tdom.dump(), 
            spatial_domain = sdom.dump())

        dp_obj.geospatial_bounds.geospatial_latitude_limit_north = 10.0
        dp_obj.geospatial_bounds.geospatial_latitude_limit_south = -10.0
        dp_obj.geospatial_bounds.geospatial_longitude_limit_east = 10.0
        dp_obj.geospatial_bounds.geospatial_longitude_limit_west = -10.0

        #------------------------------------------------------------------------------------------------
        # Create a set of ParameterContext objects to define the parameters in the coverage, add each to the ParameterDictionary
        #------------------------------------------------------------------------------------------------

        dp_id = self.dpsc_cli.create_data_product( data_product= dp_obj,
                                            stream_definition_id=ctd_stream_def_id)
        self.dpsc_cli.activate_data_product_persistence(dp_id)

        dp_obj = self.dpsc_cli.read_data_product(dp_id)
        self.assertIsNotNone(dp_obj)
        self.assertEquals(dp_obj.geospatial_point_center.lat, 0.0)
        log.debug('Created data product %s', dp_obj)
        #------------------------------------------------------------------------------------------------
        # test creating a new data product with  a stream definition
        #------------------------------------------------------------------------------------------------
        log.debug('Creating new data product with a stream definition')
        dp_obj = IonObject(RT.DataProduct,
            name='DP2',
            description='some new dp',
            temporal_domain = tdom.dump(),
            spatial_domain = sdom.dump())

        dp_id2 = self.dpsc_cli.create_data_product(dp_obj, ctd_stream_def_id)
        self.dpsc_cli.activate_data_product_persistence(dp_id2)
        log.debug('new dp_id = %s' % dp_id2)

        #------------------------------------------------------------------------------------------------
        #make sure data product is associated with stream def
        #------------------------------------------------------------------------------------------------
        streamdefs = []
        streams, _ = self.rrclient.find_objects(dp_id2, PRED.hasStream, RT.Stream, True)
        for s in streams:
            log.debug("Checking stream %s" % s)
            sdefs, _ = self.rrclient.find_objects(s, PRED.hasStreamDefinition, RT.StreamDefinition, True)
            for sd in sdefs:
                log.debug("Checking streamdef %s" % sd)
                streamdefs.append(sd)
        self.assertIn(ctd_stream_def_id, streamdefs)


        # test reading a non-existent data product
        log.debug('reading non-existent data product')

        with self.assertRaises(NotFound):
            dp_obj = self.dpsc_cli.read_data_product('some_fake_id')

        # update a data product (tests read also)
        log.debug('Updating data product')
        # first get the existing dp object
        dp_obj = self.dpsc_cli.read_data_product(dp_id)

        # now tweak the object
        dp_obj.description = 'the very first dp'
        dp_obj.geospatial_bounds.geospatial_latitude_limit_north = 20.0
        dp_obj.geospatial_bounds.geospatial_latitude_limit_south = -20.0
        dp_obj.geospatial_bounds.geospatial_longitude_limit_east = 20.0
        dp_obj.geospatial_bounds.geospatial_longitude_limit_west = -20.0
        # now write the dp back to the registry
        update_result = self.dpsc_cli.update_data_product(dp_obj)


        # now get the dp back to see if it was updated
        dp_obj = self.dpsc_cli.read_data_product(dp_id)
        self.assertEquals(dp_obj.description,'the very first dp')
        self.assertEquals(dp_obj.geospatial_point_center.lat, 0.0)
        log.debug('Updated data product %s', dp_obj)

        #test extension
        extended_product = self.dpsc_cli.get_data_product_extension(dp_id)
        self.assertEqual(dp_id, extended_product._id)
        self.assertEqual(ComputedValueAvailability.PROVIDED,
                         extended_product.computed.product_download_size_estimated.status)
        self.assertEqual(0, extended_product.computed.product_download_size_estimated.value)

        self.assertEqual(ComputedValueAvailability.PROVIDED,
                         extended_product.computed.parameters.status)
        #log.debug("test_create_data_product: parameters %s" % extended_product.computed.parameters.value)

        # now 'delete' the data product
        log.debug("deleting data product: %s" % dp_id)
        self.dpsc_cli.delete_data_product(dp_id)
        self.dpsc_cli.force_delete_data_product(dp_id)

        # now try to get the deleted dp object
        with self.assertRaises(NotFound):
            dp_obj = self.dpsc_cli.read_data_product(dp_id)

        # Get the events corresponding to the data product
        ret = self.unsc.get_recent_events(resource_id=dp_id)
        events = ret.value

        for event in events:
            log.debug("event time: %s" % event.ts_created)

        self.assertTrue(len(events) > 0)

    def test_data_product_stream_def(self):
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        ctd_stream_def_id = self.pubsubcli.create_stream_definition(name='Simulated CTD data', parameter_dictionary_id=pdict_id)

        tdom, sdom = time_series_domain()

        sdom = sdom.dump()
        tdom = tdom.dump()



        dp_obj = IonObject(RT.DataProduct,
            name='DP1',
            description='some new dp',
            temporal_domain = tdom,
            spatial_domain = sdom)
        dp_id = self.dpsc_cli.create_data_product(data_product= dp_obj,
            stream_definition_id=ctd_stream_def_id)

        stream_def_id = self.dpsc_cli.get_data_product_stream_definition(dp_id)
        self.assertEquals(ctd_stream_def_id, stream_def_id)


    def test_derived_data_product(self):
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        ctd_stream_def_id = self.pubsubcli.create_stream_definition(name='ctd parsed', parameter_dictionary_id=pdict_id)
        self.addCleanup(self.pubsubcli.delete_stream_definition, ctd_stream_def_id)

        tdom, sdom = time_series_domain()

        dp = DataProduct(name='Instrument DP', temporal_domain=tdom.dump(), spatial_domain=sdom.dump())
        dp_id = self.dpsc_cli.create_data_product(dp, stream_definition_id=ctd_stream_def_id)
        self.addCleanup(self.dpsc_cli.force_delete_data_product, dp_id)

        self.dpsc_cli.activate_data_product_persistence(dp_id)
        self.addCleanup(self.dpsc_cli.suspend_data_product_persistence, dp_id)


        dataset_ids, _ = self.rrclient.find_objects(subject=dp_id, predicate=PRED.hasDataset, id_only=True)
        if not dataset_ids:
            raise NotFound("Data Product %s dataset  does not exist" % str(dp_id))
        dataset_id = dataset_ids[0]
        
        # Make the derived data product
        simple_stream_def_id = self.pubsubcli.create_stream_definition(name='TEMPWAT stream def', parameter_dictionary_id=pdict_id, available_fields=['time','temp'])
        tempwat_dp = DataProduct(name='TEMPWAT')
        tempwat_dp_id = self.dpsc_cli.create_data_product(tempwat_dp, stream_definition_id=simple_stream_def_id, dataset_id=dataset_id)
        self.addCleanup(self.dpsc_cli.delete_data_product, tempwat_dp_id)
        # Check that the streams associated with the data product are persisted with
        stream_ids, _ =  self.rrclient.find_objects(dp_id,PRED.hasStream,RT.Stream,True)
        for stream_id in stream_ids:
            self.assertTrue(self.ingestclient.is_persisted(stream_id))

        stream_id = stream_ids[0]
        route = self.pubsubcli.read_stream_route(stream_id=stream_id)

        rdt = RecordDictionaryTool(stream_definition_id=ctd_stream_def_id)
        rdt['time'] = np.arange(20)
        rdt['temp'] = np.arange(20)
        rdt['pressure'] = np.arange(20)

        publisher = StandaloneStreamPublisher(stream_id,route)
        
        dataset_modified = Event()
        def cb(*args, **kwargs):
            dataset_modified.set()
        es = EventSubscriber(event_type=OT.DatasetModified, callback=cb, origin=dataset_id, auto_delete=True)
        es.start()
        self.addCleanup(es.stop)

        publisher.publish(rdt.to_granule())

        self.assertTrue(dataset_modified.wait(30))

        tempwat_dataset_ids, _ = self.rrclient.find_objects(tempwat_dp_id, PRED.hasDataset, id_only=True)
        tempwat_dataset_id = tempwat_dataset_ids[0]
        granule = self.data_retriever.retrieve(tempwat_dataset_id, delivery_format=simple_stream_def_id)
        rdt = RecordDictionaryTool.load_from_granule(granule)
        np.testing.assert_array_equal(rdt['time'], np.arange(20))
        self.assertEquals(set(rdt.fields), set(['time','temp']))




    def test_activate_suspend_data_product(self):

        #------------------------------------------------------------------------------------------------
        # create a stream definition for the data from the ctd simulator
        #------------------------------------------------------------------------------------------------
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        ctd_stream_def_id = self.pubsubcli.create_stream_definition(name='Simulated CTD data', parameter_dictionary_id=pdict_id)
        log.debug("Created stream def id %s" % ctd_stream_def_id)

        #------------------------------------------------------------------------------------------------
        # test creating a new data product w/o a stream definition
        #------------------------------------------------------------------------------------------------
        # Construct temporal and spatial Coordinate Reference System objects
        tdom, sdom = time_series_domain()

        sdom = sdom.dump()
        tdom = tdom.dump()



        dp_obj = IonObject(RT.DataProduct,
            name='DP1',
            description='some new dp',
            temporal_domain = tdom,
            spatial_domain = sdom)

        log.debug("Created an IonObject for a data product: %s" % dp_obj)

        #------------------------------------------------------------------------------------------------
        # Create a set of ParameterContext objects to define the parameters in the coverage, add each to the ParameterDictionary
        #------------------------------------------------------------------------------------------------

        dp_id = self.dpsc_cli.create_data_product(data_product= dp_obj,
            stream_definition_id=ctd_stream_def_id)

        #------------------------------------------------------------------------------------------------
        # test activate and suspend data product persistence
        #------------------------------------------------------------------------------------------------
        self.dpsc_cli.activate_data_product_persistence(dp_id)
        
        dp_obj = self.dpsc_cli.read_data_product(dp_id)
        self.assertIsNotNone(dp_obj)

        dataset_ids, _ = self.rrclient.find_objects(subject=dp_id, predicate=PRED.hasDataset, id_only=True)
        if not dataset_ids:
            raise NotFound("Data Product %s dataset  does not exist" % str(dp_id))
        dataset_id = dataset_ids[0]


        # Check that the streams associated with the data product are persisted with
        stream_ids, _ =  self.rrclient.find_objects(dp_id,PRED.hasStream,RT.Stream,True)
        for stream_id in stream_ids:
            self.assertTrue(self.ingestclient.is_persisted(stream_id))

        stream_id = stream_ids[0]
        route = self.pubsubcli.read_stream_route(stream_id=stream_id)

        rdt = RecordDictionaryTool(stream_definition_id=ctd_stream_def_id)
        rdt['time'] = np.arange(20)
        rdt['temp'] = np.arange(20)

        publisher = StandaloneStreamPublisher(stream_id,route)
        
        dataset_modified = Event()
        def cb(*args, **kwargs):
            dataset_modified.set()
        es = EventSubscriber(event_type=OT.DatasetModified, callback=cb, origin=dataset_id, auto_delete=True)
        es.start()
        self.addCleanup(es.stop)

        publisher.publish(rdt.to_granule())

        self.assertTrue(dataset_modified.wait(30))

        #--------------------------------------------------------------------------------
        # Now get the data in one chunk using an RPC Call to start_retreive
        #--------------------------------------------------------------------------------

        replay_data = self.data_retriever.retrieve(dataset_ids[0])
        self.assertIsInstance(replay_data, Granule)

        log.debug("The data retriever was able to replay the dataset that was attached to the data product "
                  "we wanted to be persisted. Therefore the data product was indeed persisted with "
                  "otherwise we could not have retrieved its dataset using the data retriever. Therefore "
                  "this demonstration shows that L4-CI-SA-RQ-267 is satisfied: 'Data product management shall persist data products'")

        data_product_object = self.rrclient.read(dp_id)
        self.assertEquals(data_product_object.name,'DP1')
        self.assertEquals(data_product_object.description,'some new dp')

        log.debug("Towards L4-CI-SA-RQ-308: 'Data product management shall persist data product metadata'. "
                  " Attributes in create for the data product obj, name= '%s', description='%s', match those of object from the "
                  "resource registry, name='%s', desc='%s'" % (dp_obj.name, dp_obj.description,data_product_object.name,
                                                           data_product_object.description))

        #------------------------------------------------------------------------------------------------
        # test suspend data product persistence
        #------------------------------------------------------------------------------------------------
        self.dpsc_cli.suspend_data_product_persistence(dp_id)


        dataset_modified.clear()

        rdt['time'] = np.arange(20,40)

        publisher.publish(rdt.to_granule())
        self.assertFalse(dataset_modified.wait(2))

        self.dpsc_cli.activate_data_product_persistence(dp_id)
        dataset_modified.clear()

        publisher.publish(rdt.to_granule())
        self.assertTrue(dataset_modified.wait(30))

        granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(granule)
        np.testing.assert_array_almost_equal(rdt['time'], np.arange(40))


        dataset_ids, _ = self.rrclient.find_objects(dp_id, PRED.hasDataset, id_only=True)
        self.assertEquals(len(dataset_ids), 1)

        self.dpsc_cli.suspend_data_product_persistence(dp_id)
        self.dpsc_cli.force_delete_data_product(dp_id)
        # now try to get the deleted dp object

        with self.assertRaises(NotFound):
            dp_obj = self.rrclient.read(dp_id)

    def test_lookup_values(self):
        contexts = self.create_lookup_contexts()
        context_ids = [c_id for c,c_id in contexts.itervalues()]
        pdict_id = self.dataset_management.create_parameter_dictionary(name='lookup_pdict', parameter_context_ids=context_ids, temporal_context='time')
        self.addCleanup(self.dataset_management.delete_parameter_dictionary, pdict_id)
        stream_def_id = self.pubsubcli.create_stream_definition('lookup', parameter_dictionary_id=pdict_id)
        self.addCleanup(self.pubsubcli.delete_stream_definition, stream_def_id)

        data_product = DataProduct(name='lookup data product')
        tdom, sdom = time_series_domain()
        data_product.temporal_domain = tdom.dump()
        data_product.spatial_domain = sdom.dump()

        data_product_id = self.dpsc_cli.create_data_product(data_product, stream_definition_id=stream_def_id)
        self.addCleanup(self.dpsc_cli.delete_data_product, data_product_id)
        data_producer = DataProducer(name='producer')
        data_producer.producer_context = DataProcessProducerContext()
        data_producer.producer_context.configuration['qc_keys'] = ['offset_document']
        data_producer_id, _ = self.rrclient.create(data_producer)
        self.addCleanup(self.rrclient.delete, data_producer_id)
        assoc,_ = self.rrclient.create_association(subject=data_product_id, object=data_producer_id, predicate=PRED.hasDataProducer)
        self.addCleanup(self.rrclient.delete_association, assoc)

        document_keys = self.damsclient.list_qc_references(data_product_id)
            
        self.assertEquals(document_keys, ['offset_document'])
        svm = StoredValueManager(self.container)
        svm.stored_value_cas('offset_document', {'offset_a':2.0})
        self.dpsc_cli.activate_data_product_persistence(data_product_id)
        dataset_ids, _ = self.rrclient.find_objects(subject=data_product_id, predicate=PRED.hasDataset, id_only=True)
        dataset_id = dataset_ids[0]

        dataset_modified = Event()
        def cb(*args, **kwargs):
            dataset_modified.set()
        es = EventSubscriber(event_type=OT.DatasetModified, callback=cb, origin=dataset_id)
        es.start()
        self.addCleanup(es.stop)

        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = [0]
        rdt['temp'] = [20.]
        granule = rdt.to_granule()

        stream_ids, _ = self.rrclient.find_objects(subject=data_product_id, predicate=PRED.hasStream, id_only=True)
        stream_id = stream_ids[0]
        route = self.pubsubcli.read_stream_route(stream_id=stream_id)

        publisher = StandaloneStreamPublisher(stream_id, route)
        publisher.publish(granule)

        self.assertTrue(dataset_modified.wait(10))

        granule = self.data_retriever.retrieve(dataset_id)
        rdt2 = RecordDictionaryTool.load_from_granule(granule)
        np.testing.assert_array_equal(rdt['temp'], rdt2['temp'])
        np.testing.assert_array_almost_equal(rdt2['calibrated'], np.array([22.0]))


    def create_lookup_contexts(self):
        contexts = {}
        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.dtype('float64')))
        t_ctxt.uom = 'seconds since 01-01-1900'
        t_ctxt_id = self.dataset_management.create_parameter_context(name='time', parameter_context=t_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, t_ctxt_id)
        contexts['time'] = (t_ctxt, t_ctxt_id)
        
        temp_ctxt = ParameterContext('temp', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
        temp_ctxt.uom = 'deg_C'
        temp_ctxt_id = self.dataset_management.create_parameter_context(name='temp', parameter_context=temp_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, temp_ctxt_id)
        contexts['temp'] = temp_ctxt, temp_ctxt_id

        offset_ctxt = ParameterContext('offset_a', param_type=QuantityType(value_encoding='float32'), fill_value=-9999)
        offset_ctxt.lookup_value = True
        offset_ctxt_id = self.dataset_management.create_parameter_context(name='offset_a', parameter_context=offset_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, offset_ctxt_id)
        contexts['offset_a'] = offset_ctxt, offset_ctxt_id

        func = NumexprFunction('calibrated', 'temp + offset', ['temp','offset'], param_map={'temp':'temp', 'offset':'offset_a'})
        func.lookup_values = ['LV_offset']
        calibrated = ParameterContext('calibrated', param_type=ParameterFunctionType(func, value_encoding='float32'), fill_value=-9999)
        calibrated_id = self.dataset_management.create_parameter_context(name='calibrated', parameter_context=calibrated.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, calibrated_id)
        contexts['calibrated'] = calibrated, calibrated_id

        return contexts

