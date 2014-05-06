#!/usr/bin/env python

"""
@package coverage_model.test.test_coverage
@file coverage_model/test/test_coverage_recovery.py
@author James Case
@author Christopher Mueller
@brief Test cases for CoverageDoctor's Coverage recovery methods.
"""
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
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
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

from nose.plugins.attrib import attr
from interface.objects import ProcessDefinition, DataProducer, DataProcessProducerContext

from coverage_model.basic_types import AxisTypeEnum, MutabilityEnum
from coverage_model.coverage import CRS, GridDomain, GridShape
from coverage_model import ParameterContext, ParameterFunctionType, NumexprFunction, QuantityType
from coverage_model import SimplexCoverage, AbstractCoverage, ParameterDictionary, GridDomain

from coverage_model.recovery import CoverageDoctor

from gevent.event import Event

import unittest, gevent
import numpy as np
import os

from ion.services.dm.utility.granule import RecordDictionaryTool
from pyon.ion.event import EventSubscriber
from gevent.event import Event
from pyon.public import OT
from pyon.ion.stream import StandaloneStreamPublisher
# from ion.services.dm.utility.test.parameter_helper import ParameterHelper
# from ion.services.dm.test.test_dm_end_2_end import DatasetMonitor


from subprocess import call
import re
not_have_h5stat = call('which h5stat'.split(), stdout=open('/dev/null','w'))
if not not_have_h5stat:
    from subprocess import check_output
    from distutils.version import StrictVersion
    output = check_output('h5stat -V'.split())
    version_str = re.match(r'.*(\d+\.\d+\.\d+).*', output).groups()[0]
    h5stat_correct_version = StrictVersion(version_str) >= StrictVersion('1.8.9')

@attr('INT', group='dm')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class TestCoverageModelRecoveryInt(IonIntegrationTestCase):

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

    def get_datastore(self, dataset_id):
        dataset = self.dataset_management.read_dataset(dataset_id)
        datastore_name = dataset.datastore_name
        datastore = self.container.datastore_manager.get_datastore(datastore_name, DataStore.DS_PROFILE.SCIDATA)
        return datastore

    def load_data_product(self):
        dset_i = 0
        dataset_management      = DatasetManagementServiceClient()
        pubsub_management       = PubsubManagementServiceClient()
        data_product_management = DataProductManagementServiceClient()
        resource_registry       = self.container.instance.resource_registry
        dp_obj = DataProduct(
            name='instrument_data_product_%i' % dset_i,
            description='ctd stream test',
            processing_level_code='Parsed_Canonical')
        pdict_id = dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        stream_def_id = pubsub_management.create_stream_definition(name='parsed', parameter_dictionary_id=pdict_id)
        self.addCleanup(pubsub_management.delete_stream_definition, stream_def_id)
        data_product_id = data_product_management.create_data_product(data_product=dp_obj, stream_definition_id=stream_def_id)
        self.addCleanup(data_product_management.delete_data_product, data_product_id)
        data_product_management.activate_data_product_persistence(data_product_id)
        self.addCleanup(data_product_management.suspend_data_product_persistence, data_product_id)

        stream_ids, assocs = resource_registry.find_objects(subject=data_product_id, predicate='hasStream', id_only=True)
        stream_id = stream_ids[0]
        route = pubsub_management.read_stream_route(stream_id)

        dataset_ids, assocs = resource_registry.find_objects(subject=data_product_id, predicate='hasDataset', id_only=True)
        dataset_id = dataset_ids[0]

        return data_product_id, stream_id, route, stream_def_id, dataset_id

    def populate_dataset(self, dataset_id, hours):
        import time
        cov = DatasetManagementService._get_simplex_coverage(dataset_id=dataset_id, mode='w')
        # rcov = vcov.reference_coverage
        # cov = AbstractCoverage.load(rcov.persistence_dir, mode='a')
        dt = hours * 3600

        cov.insert_timesteps(dt)
        now = time.time()
        cov.set_parameter_values('time', np.arange(now - dt, now) + 2208988800)
        cov.set_parameter_values('temp', np.sin(np.arange(dt) * 2 * np.pi / 60))
        cov.set_parameter_values('lat', np.zeros(dt))
        cov.set_parameter_values('lon', np.zeros(dt))

        cov.close()
        gevent.sleep(1)

    @attr('LOCOINT')
    @unittest.skip('Coverage metadata is now stored in Postgres.  Recovery may no longer make sense.')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Host requires file-system access to coverage files, CEI mode does not support.')
    @unittest.skipIf(not_have_h5stat, 'h5stat is not accessible in current PATH')
    @unittest.skipIf(not not_have_h5stat and not h5stat_correct_version, 'HDF is the incorrect version: %s' % version_str)
    def test_coverage_recovery(self):
        # Create the coverage
        dp_id, stream_id, route, stream_def_id, dataset_id = self.load_data_product()
        self.populate_dataset(dataset_id, 36)
        dset = self.dataset_management.read_dataset(dataset_id)
        dprod = self.dpsc_cli.read_data_product(dp_id)
        cov = DatasetManagementService._get_simplex_coverage(dataset_id)
        cov_pth = cov.persistence_dir
        cov.close()

        num_params = len(cov.list_parameters())
        num_bricks = 8
        total = num_params + num_bricks + 1

        # Analyze the valid coverage
        dr = CoverageDoctor(cov_pth, dprod, dset)

        dr_result = dr.analyze()

        # TODO: Turn these into meaningful Asserts
        self.assertEqual(len(dr_result.get_brick_corruptions()), 0)
        self.assertEqual(len(dr_result.get_brick_size_ratios()), num_bricks)
        self.assertEqual(len(dr_result.get_corruptions()), 0)
        self.assertEqual(len(dr_result.get_master_corruption()), 0)
        self.assertEqual(len(dr_result.get_param_corruptions()), 0)
        self.assertEqual(len(dr_result.get_param_size_ratios()), num_params)
        self.assertEqual(len(dr_result.get_master_size_ratio()), 1)
        self.assertEqual(len(dr_result.get_size_ratios()), total)
        self.assertEqual(dr_result.master_status[1], 'NORMAL')

        self.assertFalse(dr_result.is_corrupt)
        self.assertEqual(dr_result.param_file_count, num_params)
        self.assertEqual(dr_result.brick_file_count, num_bricks)
        self.assertEqual(dr_result.total_file_count, total)

        # Get original values (mock)
        orig_cov = AbstractCoverage.load(cov_pth)
        time_vals_orig = orig_cov.get_time_values()
        orig_cov.close()

        # Corrupt the Master File
        fo = open(cov._persistence_layer.master_manager.file_path, "wb")
        fo.write('Junk')
        fo.close()
        # Corrupt the lon Parameter file
        fo = open(cov._persistence_layer.parameter_metadata['lon'].file_path, "wb")
        fo.write('Junk')
        fo.close()

        corrupt_res = dr.analyze(reanalyze=True)
        self.assertTrue(corrupt_res.is_corrupt)

        # Repair the metadata files
        dr.repair(reanalyze=True)

        fixed_res = dr.analyze(reanalyze=True)
        self.assertFalse(fixed_res.is_corrupt)

        fixed_cov = AbstractCoverage.load(cov_pth)
        self.assertIsInstance(fixed_cov, AbstractCoverage)

        time_vals_fixed = fixed_cov.get_time_values()
        fixed_cov.close()
        self.assertTrue(np.array_equiv(time_vals_orig, time_vals_fixed))
