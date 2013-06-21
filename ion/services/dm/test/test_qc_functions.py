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
from pyon.util.containers import DotDict
from gevent.event import Event
from pyon.util.log import log

from ion.services.dm.test.dm_test_case import DMTestCase
from ion_functions.qc.qc_functions import ntp_to_month
from uuid import uuid4

import numpy as np

@attr('INT', group='dm')
class TestQCFunctions(DMTestCase):
    def setUp(self):
        DMTestCase.setUp(self)
        self.ph = ParameterHelper(self.dataset_management, self.addCleanup)
        self.pdict_id = self.ph.create_simple_qc_pdict()
        self.svm = StoredValueManager(self.container)

    def new_rdt(self,ref='QCTEST'):
        self.stream_def_id = self.create_stream_definition(uuid4().hex, parameter_dictionary_id=self.pdict_id, stream_configuration={'reference_designator':'QCTEST'})
        self.rdt = RecordDictionaryTool(stream_definition_id=self.stream_def_id)

    def test_qc_functions(self):
        self.check_global_range()
        self.check_spike()
        self.check_stuck_value()
        self.check_trend()
        self.check_gradient()
        self.check_localrange()
        self.check_propagate()


    def check_global_range(self):
        log.info('check_global_range')
        self.new_rdt()
        self.svm.stored_value_cas('grt_QCTEST_TEMPWAT', {'grt_min_value':10., 'grt_max_value':20.})

        self.rdt['time'] = np.arange(8)
        self.rdt['temp'] = [9, 10, 16, 17, 18, 19, 20, 25]
        self.rdt.fetch_lookup_values()
        np.testing.assert_array_almost_equal(self.rdt['tempwat_glblrng_qc'], [0, 1, 1, 1, 1, 1, 1, 0])

    def check_spike(self): 
        log.info('check_spike')
        self.new_rdt()
        self.svm.stored_value_cas('spike_QCTEST_TEMPWAT', {'acc':0.1, 'spike_n':5., 'spike_l':5.})

        self.rdt['time'] = np.arange(8)
        self.rdt['temp'] = [-1, 3, 40, -1, 1, -6, -6, 1]
        self.rdt.fetch_lookup_values()

        np.testing.assert_array_almost_equal(self.rdt['tempwat_spketst_qc'], [1, 1, 0, 1, 1, 1, 1, 1])

    def check_stuck_value(self):
        log.info('check_stuck_value')
        self.new_rdt()
        self.svm.stored_value_cas('svt_QCTEST_TEMPWAT', {'svt_resolution':0.001, 'svt_n': 4.})

        self.rdt['time'] = np.arange(10)
        self.rdt['temp'] = [4.83, 1.40, 3.33, 3.33, 3.33, 3.33, 4.09, 2.97, 2.85, 3.67]
        self.rdt.fetch_lookup_values()

        np.testing.assert_array_almost_equal(self.rdt['tempwat_stuckvl_qc'], [1, 1, 0, 0, 0, 0, 1, 1, 1, 1])

    def check_trend(self):
        log.info('check_trend')
        self.new_rdt()
        self.svm.stored_value_cas('trend_QCTEST_TEMPWAT', {'time_interval':0, 'polynomial_order': 1, 'standard_deviation': 3})
        self.rdt['time'] = np.arange(10)
        self.rdt['temp'] = [0.8147, 0.9058, 0.1270, 0.9134, 0.6324, 0.0975, 0.2785, 0.5469, 0.9575, 0.9649]

        self.rdt.fetch_lookup_values()

        np.testing.assert_array_equal(self.rdt['tempwat_trndtst_qc'], [1] * 10)


    def check_propagate(self):
        log.info('check_propagate')
        self.new_rdt()
        self.rdt['time'] = np.arange(8)
        self.rdt['temp'] = [9, 10, 16, 17, 18, 19, 20, 25]
        self.rdt['tempwat_glblrng_qc'] = [0, 1, 1, 1, 1, 1, 1, 0]
        self.rdt['tempwat_spketst_qc'] = [0, 1, 1, 1, 1, 1, 1, 0]
        self.rdt['tempwat_stuckvl_qc'] = [0, 1, 1, 1, 1, 1, 1, 0]
        self.rdt['tempwat_gradtst_qc'] = [0, 1, 1, 1, 1, 1, 1, 0]
        self.rdt['tempwat_trndtst_qc'] = [0, 1, 1, 1, 1, 1, 1, 0]
        self.rdt['tempwat_loclrng_qc'] = [0, 1, 1, 1, 1, 1, 1, 0]
        self.rdt['preswat_glblrng_qc'] = [0, 1, 1, 1, 1, 1, 1, 0]
        self.rdt['preswat_spketst_qc'] = [0, 1, 1, 1, 1, 1, 1, 0]
        self.rdt['preswat_stuckvl_qc'] = [0, 1, 1, 1, 1, 1, 1, 0]
        self.rdt['preswat_gradtst_qc'] = [0, 1, 1, 1, 1, 1, 1, 0]
        self.rdt['preswat_trndtst_qc'] = [0, 1, 1, 1, 1, 1, 1, 0]
        self.rdt['preswat_loclrng_qc'] = [0, 1, 1, 1, 1, 1, 1, 0]
        np.testing.assert_array_equal(self.rdt['cmbnflg_qc'], [0, 1, 1, 1, 1, 1, 1, 0])
    
    def check_gradient(self):
        log.info('check_gradient')
        self.new_rdt()
        self.svm.stored_value_cas('grad_QCTEST_TEMPWAT_time', {'d_dat_dx': 50, 'min_dx': 0, 'start_dat': 0, 'tol_dat': 5})
        self.rdt['time'] = np.arange(5)
        self.rdt['temp'] = [3, 5, 98, 99, 4]
        self.rdt.fetch_lookup_values()

        np.testing.assert_array_equal(self.rdt['tempwat_gradtst_qc'], [1, 1, 0, 0, 1])

    def check_localrange(self):
        log.info('check_localrange')
        self.new_rdt()
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

        self.svm.stored_value_cas('lrt_QCTEST_TEMPWAT', {'datlim':datlim.tolist(), 'datlimz':datlimz.tolist(), 'dims':['pressure', 'month']})
        self.rdt['time'] = t
        self.rdt['temp'] = dat
        self.rdt['pressure'] = pressure
        
        self.rdt.fetch_lookup_values()

        np.testing.assert_array_equal(self.rdt['tempwat_loclrng_qc'], [1 ,1 ,1 ,1 ,1 ,0 ,0 ,0 ,0 ,0])

    

@attr('INT', group='dm')
class TestCoverageQC(TestQCFunctions):

    def init_check(self):
        self.dp_id = self.create_data_product(name=uuid4().hex, stream_def_id=self.stream_def_id)
        self.data_product_management.activate_data_product_persistence(self.dp_id)
        self.addCleanup(self.data_product_management.suspend_data_product_persistence, self.dp_id)
        self.dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(self.dp_id)
        self.dataset_monitor = DatasetMonitor(self.dataset_id)
        self.addCleanup(self.dataset_monitor.stop)

    def test_qc_functions(self):
        self.check_fill_values()
        self.check_global_range()
        self.check_spike()
        self.check_stuck_value()
        self.check_gradient()
        self.check_trend()
        self.check_localrange()

    def check_global_range(self):
        TestQCFunctions.check_global_range(self)
        self.init_check()

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

    def check_fill_values(self):
        log.info('check_fill_values')
        self.new_rdt()
        self.init_check()
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

    def check_spike(self):
        log.info('check_spike')
        TestQCFunctions.check_spike(self)
        self.init_check()
        self.ph.publish_rdt_to_data_product(self.dp_id, self.rdt)
        self.dataset_monitor.event.wait(10)

        rdt = RecordDictionaryTool.load_from_granule(self.data_retriever.retrieve(self.dataset_id))
        np.testing.assert_array_almost_equal(rdt['tempwat_spketst_qc'], [1, 1, 0, 1, 1, 1, 1, 1])


    def check_stuck_value(self):
        log.info('check_stuck_value')
        TestQCFunctions.check_stuck_value(self)
        self.init_check()
        self.ph.publish_rdt_to_data_product(self.dp_id, self.rdt)
        self.dataset_monitor.event.wait(10)

        rdt = RecordDictionaryTool.load_from_granule(self.data_retriever.retrieve(self.dataset_id))
        np.testing.assert_array_almost_equal(rdt['tempwat_stuckvl_qc'], [1, 1, 0, 0, 0, 0, 1, 1, 1, 1])
    
    def check_gradient(self):
        log.info('check_gradient')
        TestQCFunctions.check_gradient(self)
        self.init_check()
        self.ph.publish_rdt_to_data_product(self.dp_id, self.rdt)
        self.dataset_monitor.event.wait(10)

        rdt = RecordDictionaryTool.load_from_granule(self.data_retriever.retrieve(self.dataset_id))
        np.testing.assert_array_equal(rdt['tempwat_gradtst_qc'], [1, 1, 0, 0, 1])

    def check_trend(self):
        log.info('check_trend')
        TestQCFunctions.check_trend(self)
        self.init_check()
        self.ph.publish_rdt_to_data_product(self.dp_id, self.rdt)
        self.dataset_monitor.event.wait(10)

        rdt = RecordDictionaryTool.load_from_granule(self.data_retriever.retrieve(self.dataset_id))
        np.testing.assert_array_almost_equal(rdt['tempwat_trndtst_qc'], [1] * 10)

    def check_localrange(self):
        log.info('check_localrange')
        TestQCFunctions.check_localrange(self)
        self.init_check()

        flagged = Event()
        def cb(event, *args, **kwargs):
            times = event.temporal_values
            if not event.qc_parameter == 'tempwat_loclrng_qc':
                return
            np.testing.assert_array_equal( times, np.array([ 3580144708.7555027, 3580144709.7555027, 3580144710.7555027, 3580144711.7555027, 3580144712.7555027]))
            flagged.set()

        event_subscriber = EventSubscriber(event_type = OT.ParameterQCEvent, origin=self.dp_id, callback=cb, auto_delete=True)
        event_subscriber.start()
        self.addCleanup(event_subscriber.stop)

        self.ph.publish_rdt_to_data_product(self.dp_id, self.rdt)
        self.dataset_monitor.event.wait(10)
        rdt = RecordDictionaryTool.load_from_granule(self.data_retriever.retrieve(self.dataset_id))
        np.testing.assert_array_almost_equal(rdt['tempwat_loclrng_qc'], [1 ,1 ,1 ,1 ,1 ,0 ,0 ,0 ,0 ,0])
        self.assertTrue(flagged.wait(10))


@attr('INT', group='dm')
class TestQCPreload(TestQCFunctions):
    def setUp(self):
        TestQCFunctions.setUp(self)
        self.preload()

    def preload(self):
        config = DotDict()
        config.op = 'load'
        config.scenario='BETA'
        config.categories='Parser,Reference'
        config.path='master'
        self.container.spawn_process('ion_loader', 'ion.processes.bootstrap.ion_loader','IONLoader',config)

    def loclrng_checks(self):
        ref = 'GP03FLMA-RI001-06-CTDMOG999'
        self.stream_def_id = self.create_stream_definition('local range check', parameter_dictionary_id=self.pdict_id, stream_configuration={'reference_designator':ref})
        self.rdt = RecordDictionaryTool(stream_definition_id=self.stream_def_id)
        self.rdt['time'] = np.arange(10)
        lon, lat, pressure = [-124.832179, 46.436926, 37.5]
        self.rdt['lon'] = lon
        self.rdt['lat'] = lat
        self.rdt['pressure'] = [37.5] * 10
        self.rdt['temp'] = [30]*10
        self.rdt.fetch_lookup_values()
        doc = self.svm.read_value('lrt_%s_TEMPWAT' % ref)
        self.assertEquals(doc['dims'], ['lon', 'lat', 'pressure'])
        doc = self.svm.read_value('lrt_%s_PRESWAT' % ref)
        self.assertEquals(doc['dims'], ['temp'])
        np.testing.assert_array_equal(self.rdt['tempwat_loclrng_qc'], [1]*10)
        self.rdt['pressure'] = [37.5] * 9 + [75.]
        self.rdt['temp'] = [30] * 9 + [18]
        np.testing.assert_array_equal(self.rdt['tempwat_loclrng_qc'], [1]*9 + [0])
        self.rdt['temp'] = [15] * 5 + [35] * 5
        self.rdt['pressure'] = [10] * 10
        np.testing.assert_array_equal(self.rdt['preswat_loclrng_qc'], [1]*5 + [0] * 5)

    def glblrng_checks(self):
        ref = 'CE01ISSM-MF005-01-CTDBPC999'
        self.stream_def_id = self.create_stream_definition('global range check', parameter_dictionary_id=self.pdict_id, stream_configuration={'reference_designator':ref})
        self.rdt = RecordDictionaryTool(stream_definition_id=self.stream_def_id)

        self.rdt['time'] = np.arange(10)
        self.rdt['temp'] = [-10] * 5 + [4] * 5
        self.rdt.fetch_lookup_values()

        np.testing.assert_array_equal(self.rdt['tempwat_glblrng_qc'], [0]*5 + [1]*5)

    def spketst_checks(self):
        ref = 'GP02HYPM-SP001-04-CTDPF0999'
        self.stream_def_id = self.create_stream_definition('spike test check', parameter_dictionary_id=self.pdict_id, stream_configuration={'reference_designator':ref})
        self.rdt = RecordDictionaryTool(stream_definition_id=self.stream_def_id)

        self.rdt['time'] = np.arange(20)
        self.rdt['temp'] = [13] * 9 + [100] + [13] * 10
        self.rdt.fetch_lookup_values()

        np.testing.assert_array_equal(self.rdt['tempwat_spketst_qc'], [1]*9 + [0] + [1]*10)

    def stuckvl_checks(self):
        ref = 'GP02HYPM-SP001-04-CTDPF0999'
        self.stream_def_id = self.create_stream_definition('stuck value checks', parameter_dictionary_id=self.pdict_id, stream_configuration={'reference_designator':ref})
        self.rdt = RecordDictionaryTool(stream_definition_id=self.stream_def_id)

        self.rdt['time'] = np.arange(50)
        self.rdt['temp'] = [20] * 30 + range(20)
        self.rdt.fetch_lookup_values()

        np.testing.assert_array_equal(self.rdt['tempwat_stuckvl_qc'], [0]*30 + [1]*20)

    def test_qc_functions(self):
        self.loclrng_checks()
        self.glblrng_checks()
        self.spketst_checks()
        self.stuckvl_checks()



