#!/usr/bin/env python
'''
@author Luke Campbell
@file ion/services/dm/test/test_qc_functions.py
@description Integration Tests for QC Functions
'''

from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr

from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from ion.services.dm.utility.granule_utils import RecordDictionaryTool, time_series_domain
from ion.services.dm.utility.test.parameter_helper import ParameterHelper
from ion.util.parsers.global_range_test import grt_parser
from ion.util.parsers.spike_test import spike_parser
from ion.util.parsers.trend_test import trend_parser
from ion.util.stored_values import StoredValueManager
from ion.services.dm.test.test_dm_end_2_end import DatasetMonitor
from pyon.ion.event import EventSubscriber
from pyon.public import OT
from gevent.event import Event

from ion.services.dm.test.dm_test_case import DMTestCase
from ion_functions.qc.qc_functions import ntp_to_month

import numpy as np

@attr('INT', group='dm')
class TestQCFunctions(DMTestCase):
    def setUp(self):
        DMTestCase.setUp(self)
        self.ph = ParameterHelper(self.dataset_management, self.addCleanup)
        pdict_id = self.ph.create_simple_qc_pdict()

        self.stream_def_id = self.pubsub_management.create_stream_definition('global range', parameter_dictionary_id=pdict_id, stream_configuration={'reference_designator':'QCTEST'})
        self.addCleanup(self.pubsub_management.delete_stream_definition, self.stream_def_id)

        self.rdt = RecordDictionaryTool(stream_definition_id=self.stream_def_id)
        self.svm = StoredValueManager(self.container)

    def test_global_range_test(self):
        self.svm.stored_value_cas('grt_QCTEST_TEMPWAT', {'grt_min_value':10., 'grt_max_value':20.})

        self.rdt['time'] = np.arange(8)
        self.rdt['temp'] = [9, 10, 16, 17, 18, 19, 20, 25]
        self.rdt.fetch_lookup_values()
        np.testing.assert_array_almost_equal(self.rdt['tempwat_glblrng_qc'], [0, 1, 1, 1, 1, 1, 1, 0])

    def test_spike_test(self): # I know how redundant this sounds
        self.svm.stored_value_cas('spike_QCTEST_TEMPWAT', {'acc':0.1, 'spike_n':5., 'spike_l':5.})

        self.rdt['time'] = np.arange(8)
        self.rdt['temp'] = [-1, 3, 40, -1, 1, -6, -6, 1]
        self.rdt.fetch_lookup_values()

        np.testing.assert_array_almost_equal(self.rdt['tempwat_spketst_qc'], [1, 1, 0, 1, 1, 1, 1, 1])

    def test_stuck_value_test(self):
        self.svm.stored_value_cas('svt_QCTEST_TEMPWAT', {'svt_resolution':0.001, 'svt_n': 4.})

        self.rdt['time'] = np.arange(10)
        self.rdt['temp'] = [4.83, 1.40, 3.33, 3.33, 3.33, 3.33, 4.09, 2.97, 2.85, 3.67]
        self.rdt.fetch_lookup_values()

        np.testing.assert_array_almost_equal(self.rdt['tempwat_stuckvl_qc'], [1, 1, 0, 0, 0, 0, 1, 1, 1, 1])

    def test_trend_test(self):
        self.svm.stored_value_cas('trend_QCTEST_TEMPWAT', {'time_interval':0, 'polynomial_order': 1, 'standard_deviation': 3})
        self.rdt['time'] = np.arange(10)
        self.rdt['temp'] = [0.8147, 0.9058, 0.1270, 0.9134, 0.6324, 0.0975, 0.2785, 0.5469, 0.9575, 0.9649]

        self.rdt.fetch_lookup_values()

        np.testing.assert_array_equal(self.rdt['tempwat_trndtst_qc'], [1] * 10)


    def test_propagate_test(self):
        self.rdt['time'] = np.arange(8)
        self.rdt['temp'] = [9, 10, 16, 17, 18, 19, 20, 25]
        self.rdt['tempwat_glblrng_qc'] = [0, 1, 1, 1, 1, 1, 1, 0]
        self.rdt['tempwat_spketst_qc'] = [0, 1, 1, 1, 1, 1, 1, 0]
        self.rdt['tempwat_stuckvl_qc'] = [0, 1, 1, 1, 1, 1, 1, 0]
        self.rdt['tempwat_gradtst_qc'] = [0, 1, 1, 1, 1, 1, 1, 0]
        self.rdt['tempwat_trndtst_qc'] = [0, 1, 1, 1, 1, 1, 1, 0]
        self.rdt['tempwat_loclrng_qc'] = [0, 1, 1, 1, 1, 1, 1, 0]
        np.testing.assert_array_equal(self.rdt['cmbnflg_qc'], [0, 1, 1, 1, 1, 1, 1, 0])
    
    def test_gradient_test(self):
        self.svm.stored_value_cas('grad_QCTEST_TEMPWAT_time', {'d_dat_dx': 50, 'min_dx': 0, 'start_dat': 0, 'tol_dat': 5})
        self.rdt['time'] = np.arange(5)
        self.rdt['temp'] = [3, 5, 98, 99, 4]
        self.rdt.fetch_lookup_values()

        np.testing.assert_array_equal(self.rdt['tempwat_gradtst_qc'], [1, 1, 0, 0, 1])

    def test_localrange_test(self):
        t = np.array([3580144703.7555027, 3580144704.7555027, 3580144705.7555027, 3580144706.7555027, 3580144707.7555027, 3580144708.7555027, 3580144709.7555027, 3580144710.7555027, 3580144711.7555027, 3580144712.7555027])
        pressure = np.random.rand(10) * 2 + 33.0
        t_v = ntp_to_month(t)
        dat = t_v + pressure + np.arange(16,26)
        def lim1(p,m):
            return p+m+10
        def lim2(p,m):
            return p+m+20

        pressure_grid, month_grid = np.meshgrid(np.arange(0,150,10), np.arange(11))
        points = np.column_stack([pressure_grid.flatten(), month_grid.flatten()])
        datlim_0 = lim1(points[:,0], points[:,1])
        datlim_1 = lim2(points[:,0], points[:,1])
        datlim = np.column_stack([datlim_0, datlim_1])
        datlimz = points

        self.svm.stored_value_cas('lrt_QCTEST_TEMPWAT', {'datlim':datlim.tolist(), 'datlimz':datlimz.tolist()})
        self.rdt['time'] = t
        self.rdt['temp'] = dat
        self.rdt['pressure'] = pressure
        
        self.rdt.fetch_lookup_values()

        np.testing.assert_array_equal(self.rdt['tempwat_loclrng_qc'], [1 ,1 ,1 ,1 ,1 ,0 ,0 ,0 ,0 ,0])

    

@attr('INT', group='dm')
class TestCoverageQC(TestQCFunctions):
    def setUp(self):
        TestQCFunctions.setUp(self)
        self.dp_id = self.create_data_product(name='qc test', stream_def_id=self.stream_def_id)
        self.data_product_management.activate_data_product_persistence(self.dp_id)
        self.addCleanup(self.data_product_management.suspend_data_product_persistence, self.dp_id)
        self.dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(self.dp_id)
        self.dataset_monitor = DatasetMonitor(self.dataset_id)
        self.addCleanup(self.dataset_monitor.stop)

    def test_global_range_test(self):
        TestQCFunctions.test_global_range_test(self)

        flagged = Event()
        def cb(event, *args, **kwargs):
            times = event.temporal_values
            self.assertEquals(times,[0.0, 7.0])
            flagged.set()

        event_subscriber = EventSubscriber(event_type=OT.ParameterQCEvent,origin=self.dp_id, callback=cb, auto_delete=True)
        event_subscriber.start()
        self.addCleanup(event_subscriber.stop)

        self.ph.publish_rdt_to_data_product(self.dp_id, self.rdt)
        self.dataset_monitor.event.wait(10)

        rdt = RecordDictionaryTool.load_from_granule(self.data_retriever.retrieve(self.dataset_id))
        np.testing.assert_array_almost_equal(rdt['tempwat_glblrng_qc'], [0, 1, 1, 1, 1, 1, 1, 0])
        self.assertTrue(flagged.wait(10))

    def test_fill_value_qc(self):
        self.rdt['time'] = np.arange(5)
        self.rdt['temp'] = [12] * 5
        self.rdt.fetch_lookup_values()

        np.testing.assert_array_equal(self.rdt['tempwat_glblrng_qc'], [-99] * 5)
        np.testing.assert_array_equal(self.rdt['tempwat_spketst_qc'], [-99] * 5)
        np.testing.assert_array_equal(self.rdt['tempwat_stuckvl_qc'], [-99] * 5)
        np.testing.assert_array_equal(self.rdt['tempwat_trndtst_qc'], [-99] * 5)
        np.testing.assert_array_equal(self.rdt['tempwat_gradtst_qc'], [-99] * 5)
        self.ph.publish_rdt_to_data_product(self.dp_id, self.rdt)

        self.dataset_monitor.event.wait(10)
        rdt = RecordDictionaryTool.load_from_granule(self.data_retriever.retrieve(self.dataset_id))
        np.testing.assert_array_equal(rdt['tempwat_glblrng_qc'], [-99] * 5)
        np.testing.assert_array_equal(rdt['tempwat_spketst_qc'], [-99] * 5)
        np.testing.assert_array_equal(rdt['tempwat_stuckvl_qc'], [-99] * 5)
        np.testing.assert_array_equal(rdt['tempwat_trndtst_qc'], [-99] * 5)
        np.testing.assert_array_equal(rdt['tempwat_gradtst_qc'], [-99] * 5)

    def test_spike_test(self):
        TestQCFunctions.test_spike_test(self)
        self.ph.publish_rdt_to_data_product(self.dp_id, self.rdt)
        self.dataset_monitor.event.wait(10)

        rdt = RecordDictionaryTool.load_from_granule(self.data_retriever.retrieve(self.dataset_id))
        np.testing.assert_array_almost_equal(rdt['tempwat_spketst_qc'], [1, 1, 0, 1, 1, 1, 1, 1])


    def test_stuck_value_test(self):
        TestQCFunctions.test_stuck_value_test(self)
        self.ph.publish_rdt_to_data_product(self.dp_id, self.rdt)
        self.dataset_monitor.event.wait(10)

        rdt = RecordDictionaryTool.load_from_granule(self.data_retriever.retrieve(self.dataset_id))
        np.testing.assert_array_almost_equal(rdt['tempwat_stuckvl_qc'], [1, 1, 0, 0, 0, 0, 1, 1, 1, 1])
    
    def test_gradient_test(self):
        TestQCFunctions.test_gradient_test(self)
        self.ph.publish_rdt_to_data_product(self.dp_id, self.rdt)
        self.dataset_monitor.event.wait(10)

        rdt = RecordDictionaryTool.load_from_granule(self.data_retriever.retrieve(self.dataset_id))
        np.testing.assert_array_equal(rdt['tempwat_gradtst_qc'], [1, 1, 0, 0, 1])

    def test_trend_test(self):
        TestQCFunctions.test_trend_test(self)
        self.ph.publish_rdt_to_data_product(self.dp_id, self.rdt)
        self.dataset_monitor.event.wait(10)

        rdt = RecordDictionaryTool.load_from_granule(self.data_retriever.retrieve(self.dataset_id))
        np.testing.assert_array_almost_equal(rdt['tempwat_trndtst_qc'], [1] * 10)

