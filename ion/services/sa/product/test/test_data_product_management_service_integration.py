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
from interface.objects import LastUpdate, ComputedValueAvailability

from nose.plugins.attrib import attr
from interface.objects import ProcessDefinition

from coverage_model.basic_types import AxisTypeEnum, MutabilityEnum
from coverage_model.coverage import CRS, GridDomain, GridShape


import unittest

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


        #------------------------------------------------------------------------------------------------
        # First launch the ingestors
        #------------------------------------------------------------------------------------------------
        self.exchange_space       = 'science_granule_ingestion'
        self.exchange_point       = 'science_data'
        config = DotDict()
        config.process.datastore_name = 'datasets'
        config.process.queue_name = self.exchange_space

        self.process_dispatcher.schedule_process(self.process_definitions['ingestion_worker'],configuration=config)

    def get_datastore(self, dataset_id):
        dataset = self.dataset_management.read_dataset(dataset_id)
        datastore_name = dataset.datastore_name
        datastore = self.container.datastore_manager.get_datastore(datastore_name, DataStore.DS_PROFILE.SCIDATA)
        return datastore


    @unittest.skip('OBE')
    def test_get_last_update(self):



        # Construct temporal and spatial Coordinate Reference System objects
        tcrs = CRS([AxisTypeEnum.TIME])
        scrs = CRS([AxisTypeEnum.LON, AxisTypeEnum.LAT])

        # Construct temporal and spatial Domain objects
        tdom = GridDomain(GridShape('temporal', [0]), tcrs, MutabilityEnum.EXTENSIBLE) # 1d (timeline)
        sdom = GridDomain(GridShape('spatial', [0]), scrs, MutabilityEnum.IMMUTABLE) # 1d spatial topology (station/trajectory)

        sdom = sdom.dump()
        tdom = tdom.dump()

        #@TODO: DO NOT DO THIS, WHEN THIS TEST IS REWRITTEN GET RID OF THIS, IT WILL FAIL, thanks -Luke
        parameter_dictionary = get_param_dict('ctd_parsed_param_dict')
        parameter_dictionary = parameter_dictionary.dump()

        dp_obj = IonObject(RT.DataProduct,
            name='DP1',
            description='some new dp',
            temporal_domain = tdom,
            spatial_domain = sdom)

        data_product_id = self.dpsc_cli.create_data_product(data_product=dp_obj, stream_definition_id=self.stream_def_id, parameter_dictionary=parameter_dictionary)
        stream_ids, garbage = self.rrclient.find_objects(data_product_id, PRED.hasStream, id_only=True)
        stream_id = stream_ids[0]

        fake_lu = LastUpdate()
        fake_lu_doc = self.db._ion_object_to_persistence_dict(fake_lu)
        self.db.create_doc(fake_lu_doc, object_id=stream_id)

        #------------------------------------------
        # Now execute
        #------------------------------------------
        res = self.dpsc_cli.get_last_update(data_product_id=data_product_id)
        self.assertTrue(isinstance(res[stream_id], LastUpdate), 'retrieving documents failed')

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
        log.debug('test_createDataProduct: Creating new data product w/o a stream definition (L4-CI-SA-RQ-308)')

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

        dp_obj = self.dpsc_cli.read_data_product(dp_id)

        log.debug('new dp_id = %s' % dp_id)
        log.debug("test_createDataProduct: Data product info from registry %s (L4-CI-SA-RQ-308)", str(dp_obj))


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
        self.assertEqual(ComputedValueAvailability.NOTAVAILABLE,
                         extended_product.computed.product_download_size_estimated.status)
        self.assertEqual(0, extended_product.computed.product_download_size_estimated.value)

        # now 'delete' the data product
        log.debug("deleting data product: %s" % dp_id)
        self.dpsc_cli.delete_data_product(dp_id)


        # Get the events corresponding to the data product
        ret = self.unsc.get_recent_events(resource_id=dp_id)
        events = ret.value

        for event in events:
            log.debug("event time: %s" % event.ts_created)


        # now try to get the deleted dp object

        #todo: the RR should perhaps not return retired data products
    #            dp_obj = self.dpsc_cli.read_data_product(dp_id)

    # now try to delete the already deleted dp object
    #        log.debug( "deleting non-existing data product")
    #            self.dpsc_cli.delete_data_product(dp_id)

    # Shut down container
    #container.stop()


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
        log.debug('test_createDataProduct: Creating new data product w/o a stream definition (L4-CI-SA-RQ-308)')

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

        dp_obj = self.dpsc_cli.read_data_product(dp_id)

        log.debug('new dp_id = %s' % dp_id)
        log.debug("test_createDataProduct: Data product info from registry %s (L4-CI-SA-RQ-308)", str(dp_obj))

        dataset_ids, _ = self.rrclient.find_objects(subject=dp_id, predicate=PRED.hasDataset, id_only=True)
        if not dataset_ids:
            raise NotFound("Data Product %s dataset  does not exist" % str(dp_id))
        self.get_datastore(dataset_ids[0])

        #------------------------------------------------------------------------------------------------
        # test activate and suspend data product persistence
        #------------------------------------------------------------------------------------------------
        self.dpsc_cli.activate_data_product_persistence(dp_id)

        #------------------------------------------------------------------------------------------------
        # test suspend data product persistence
        #------------------------------------------------------------------------------------------------
        self.dpsc_cli.suspend_data_product_persistence(dp_id)

#        pid = self.container.spawn_process(name='dummy_process_for_test',
#            module='pyon.ion.process',
#            cls='SimpleProcess',
#            config={})
#        dummy_process = self.container.proc_manager.procs[pid]

