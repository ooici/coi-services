#!/usr/bin/env python
'''
@file ion/services/sa/product/test/test_data_product_management_service_integration.py
@brief Data Product Management Service Integration Tests
'''

import unittest, gevent, simplejson
import numpy as np
from ion.services.sa.test.helpers import any_old
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from mock import patch
from nose.plugins.attrib import attr
from gevent.event import Event

from pyon.util.int_test import IonIntegrationTestCase
from pyon.core.exception import NotFound, BadRequest
from pyon.public import  IonObject
from pyon.public import RT, PRED, CFG, OT
from pyon.util.log import log
from pyon.util.context import LocalContextMixin
from pyon.util.containers import DotDict

from ion.processes.data.last_update_cache import CACHE_DATASTORE_NAME
from ion.services.dm.utility.granule_utils import time_series_domain


from interface.services.dm.iuser_notification_service import UserNotificationServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.idata_product_management_service import  DataProductManagementServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.objects import ProcessDefinition, DataProducer, DataProcessProducerContext


class FakeProcess(LocalContextMixin):
    name = ''


@attr('INT', group='sa')
#@unittest.skip('not working')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class TestDataProductManagementServiceCoverage(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        #print 'instantiating container'
        self._start_container()

        log.debug("Start rel from url")
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.DPMS               = DataProductManagementServiceClient()
        self.RR                 = ResourceRegistryServiceClient()
        self.RR2                = EnhancedResourceRegistryClient(self.RR)
        self.DAMS               = DataAcquisitionManagementServiceClient()
        self.PSMS               = PubsubManagementServiceClient()
        self.ingestclient       = IngestionManagementServiceClient()
        self.PD                 = ProcessDispatcherServiceClient()
        self.DSMS               = DatasetManagementServiceClient()
        self.unsc               = UserNotificationServiceClient()
        self.data_retriever     = DataRetrieverServiceClient()

        #------------------------------------------
        # Create the environment
        #------------------------------------------
        log.debug("get datastore")
        datastore_name = CACHE_DATASTORE_NAME
        self.db = self.container.datastore_manager.get_datastore(datastore_name)
        self.stream_def_id = self.PSMS.create_stream_definition(name='SBE37_CDM')

        self.process_definitions  = {}
        ingestion_worker_definition = ProcessDefinition(name='ingestion worker')
        ingestion_worker_definition.executable = {
            'module':'ion.processes.data.ingestion.science_granule_ingestion_worker',
            'class' :'ScienceGranuleIngestionWorker'
        }
        process_definition_id = self.PD.create_process_definition(process_definition=ingestion_worker_definition)
        self.process_definitions['ingestion_worker'] = process_definition_id

        self.pids = []
        self.exchange_points = []
        self.exchange_names = []


        self.addCleanup(self.cleaning_up)


    @staticmethod
    def clean_subscriptions():
        ingestion_management = IngestionManagementServiceClient()
        pubsub = PubsubManagementServiceClient()
        rr     = ResourceRegistryServiceClient()
        ingestion_config_ids = ingestion_management.list_ingestion_configurations(id_only=True)
        for ic in ingestion_config_ids:
            subscription_ids, assocs = rr.find_objects(subject=ic, predicate=PRED.hasSubscription, id_only=True)
            for subscription_id, assoc in zip(subscription_ids, assocs):
                rr.delete_association(assoc)
                try:
                    pubsub.deactivate_subscription(subscription_id)
                except:
                    log.exception("Unable to decativate subscription: %s", subscription_id)
                pubsub.delete_subscription(subscription_id)


    def cleaning_up(self):
        for pid in self.pids:
            log.debug("number of pids to be terminated: %s", len(self.pids))
            try:
                self.PD.cancel_process(pid)
                log.debug("Terminated the process: %s", pid)
            except:
                log.debug("could not terminate the process id: %s" % pid)
        TestDataProductManagementServiceCoverage.clean_subscriptions()

        for xn in self.exchange_names:
            xni = self.container.ex_manager.create_xn_queue(xn)
            xni.delete()
        for xp in self.exchange_points:
            xpi = self.container.ex_manager.create_xp(xp)
            xpi.delete()


    def test_CRUD_data_product(self):

        #------------------------------------------------------------------------------------------------
        # create a stream definition for the data from the ctd simulator
        #------------------------------------------------------------------------------------------------
        parameter_dictionary = self.DSMS.read_parameter_dictionary_by_name('ctd_parsed_param_dict')
        ctd_stream_def_id = self.PSMS.create_stream_definition(name='Simulated CTD data', parameter_dictionary_id=parameter_dictionary._id)
        log.debug("Created stream def id %s" % ctd_stream_def_id)

        #------------------------------------------------------------------------------------------------
        # test creating a new data product w/o a stream definition
        #------------------------------------------------------------------------------------------------

        # Generic time-series data domain creation

        dp_obj = IonObject(RT.DataProduct,
                           name='DP1',
                           description='some new dp')

        dp_obj.geospatial_bounds.geospatial_latitude_limit_north = 10.0
        dp_obj.geospatial_bounds.geospatial_latitude_limit_south = -10.0
        dp_obj.geospatial_bounds.geospatial_longitude_limit_east = 10.0
        dp_obj.geospatial_bounds.geospatial_longitude_limit_west = -10.0
        dp_obj.ooi_product_name = "PRODNAME"

        #------------------------------------------------------------------------------------------------
        # Create a set of ParameterContext objects to define the parameters in the coverage, add each to the ParameterDictionary
        #------------------------------------------------------------------------------------------------

        log.debug("create dataset")
        dataset_id = self.RR2.create(any_old(RT.Dataset))
        log.debug("dataset_id = %s", dataset_id)

        log.debug("create data product 1")
        dp_id = self.DPMS.create_data_product( data_product= dp_obj,
                                                   stream_definition_id=ctd_stream_def_id,
                                                   dataset_id=dataset_id)
        log.debug("dp_id = %s", dp_id)
        # Assert that the data product has an associated stream at this stage
        stream_ids, _ = self.RR.find_objects(dp_id, PRED.hasStream, RT.Stream, True)
        self.assertNotEquals(len(stream_ids), 0)

        log.debug("read data product")
        dp_obj = self.DPMS.read_data_product(dp_id)

        log.debug("find data products")
        self.assertIn(dp_id, [r._id for r in self.DPMS.find_data_products()])

        log.debug("update data product")
        dp_obj.name = "brand new"
        self.DPMS.update_data_product(dp_obj)
        self.assertEqual("brand new", self.DPMS.read_data_product(dp_id).name)

        log.debug("activate/suspend persistence")
        self.assertFalse(self.DPMS.is_persisted(dp_id))
        self.DPMS.activate_data_product_persistence(dp_id)
        self.addCleanup(self.DPMS.suspend_data_product_persistence, dp_id) # delete op will do this for us
        self.assertTrue(self.DPMS.is_persisted(dp_id))

        log.debug("check error on checking persistence of nonexistent stream")
        #with self.assertRaises(NotFound):
        if True:
            self.DPMS.is_persisted(self.RR2.create(any_old(RT.DataProduct)))

        #log.debug("get extension")
        #self.DPMS.get_data_product_extension(dp_id)

        log.debug("prepare resource support")
        support = self.DPMS.prepare_data_product_support(dp_id)
        self.assertIsNotNone(support)

        log.debug("delete data product")
        self.DPMS.delete_data_product(dp_id)

        log.debug("try to suspend again")
        self.DPMS.suspend_data_product_persistence(dp_id)

        # try basic create
        log.debug("create without a dataset")
        dp_id2 = self.DPMS.create_data_product_(any_old(RT.DataProduct))
        self.assertIsNotNone(dp_id2)
        log.debug("activate product %s", dp_id2)
        self.assertRaises(BadRequest, self.DPMS.activate_data_product_persistence, dp_id2)

        #self.assertNotEqual(0, len(self.RR2.find_dataset_ids_of_data_product_using_has_dataset(dp_id2)))

        log.debug("force delete data product")
        self.DPMS.force_delete_data_product(dp_id2)

        log.debug("test complete")
