#!/usr/bin/env python
'''
@file ion/services/sa/product/test/test_data_product_management_service_integration.py
@brief Data Product Management Service Integration Tests
'''
from pyon.core.exception import NotFound
from pyon.public import  IonObject
from pyon.public import RT, PRED
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.log import log
from pyon.util.context import LocalContextMixin
from pyon.util.containers import DotDict
from pyon.datastore.datastore import DataStore
from pyon.ion.stream import StandaloneStreamSubscriber
from pyon.ion.exchange import ExchangeNameQueue

from ion.processes.data.last_update_cache import CACHE_DATASTORE_NAME
from ion.services.dm.utility.granule_utils import time_series_domain
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

from interface.objects import LastUpdate, ComputedValueAvailability, Granule
from ion.services.dm.ingestion.test.ingestion_management_test import IngestionManagementIntTest

from nose.plugins.attrib import attr
from interface.objects import ProcessDefinition

from coverage_model.basic_types import AxisTypeEnum, MutabilityEnum
from coverage_model.coverage import CRS, GridDomain, GridShape

import unittest, gevent

class FakeProcess(LocalContextMixin):
    name = ''


@attr('INT', group='sa')
#@unittest.skip('not working')
class TestDataProductManagementServiceIntegration(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        #print 'instantiating container'
        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.dpsc_cli = DataProductManagementServiceClient(node=self.container.node)
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubcli =  PubsubManagementServiceClient(node=self.container.node)
        self.ingestclient = IngestionManagementServiceClient(node=self.container.node)
        self.process_dispatcher   = ProcessDispatcherServiceClient()
        self.dataset_management = DatasetManagementServiceClient()
        self.unsc = UserNotificationServiceClient()
        self.data_retriever = DataRetrieverServiceClient()

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

        log.debug("Created an IonObject for a data product: %s" % dp_obj)

        #------------------------------------------------------------------------------------------------
        # Create a set of ParameterContext objects to define the parameters in the coverage, add each to the ParameterDictionary
        #------------------------------------------------------------------------------------------------

        dp_id = self.dpsc_cli.create_data_product( data_product= dp_obj,
                                            stream_definition_id=ctd_stream_def_id)
        self.dpsc_cli.activate_data_product_persistence(dp_id)

        dp_obj = self.dpsc_cli.read_data_product(dp_id)
        self.assertIsNotNone(dp_obj)

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
        # now write the dp back to the registry
        update_result = self.dpsc_cli.update_data_product(dp_obj)

        # now get the dp back to see if it was updated
        dp_obj = self.dpsc_cli.read_data_product(dp_id)
        self.assertEquals(dp_obj.description,'the very first dp')

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

#        self.assertTrue(len(events) > 0)

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
        self.get_datastore(dataset_ids[0])


        # Check that the streams associated with the data product are persisted with
        stream_ids, _ =  self.rrclient.find_objects(dp_id,PRED.hasStream,RT.Stream,True)
        for stream_id in stream_ids:
            self.assertTrue(self.ingestclient.is_persisted(stream_id))

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

        self.dpsc_cli.force_delete_data_product(dp_id)
        # now try to get the deleted dp object

        with self.assertRaises(NotFound):
            dp_obj = self.rrclient.read(dp_id)

