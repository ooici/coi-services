#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@date Tue Oct 16 09:14:37 EDT 2012
@file ion/services/dm/utility/test/test_granule.py
@brief Tests for granule
'''

from pyon.ion.stream import StandaloneStreamPublisher, StandaloneStreamSubscriber
from pyon.util.int_test import IonIntegrationTestCase

from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.dm.utility.granule import RecordDictionaryTool
from ion.services.dm.utility.test.parameter_helper import ParameterHelper

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient

from gevent.event import Event
from nose.plugins.attrib import attr
from coverage_model import ParameterContext, QuantityType, AxisTypeEnum, ConstantType, NumexprFunction, ParameterFunctionType, VariabilityEnum, PythonFunction

from ion.util.stored_values import StoredValueManager

import numpy as np

@attr('INT',group='dm')
class RecordDictionaryIntegrationTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.dataset_management = DatasetManagementServiceClient()
        self.pubsub_management  = PubsubManagementServiceClient()

        self.rdt                      = None
        self.data_producer_id         = None
        self.provider_metadata_update = None
        self.event                    = Event()

    def verify_incoming(self, m,r,s):
        rdt = RecordDictionaryTool.load_from_granule(m)
        self.assertEquals(rdt, self.rdt)
        self.assertEquals(m.data_producer_id, self.data_producer_id)
        self.assertEquals(m.provider_metadata_update, self.provider_metadata_update)
        self.assertNotEqual(m.creation_timestamp, None)
        self.event.set()


    def test_serialize_compatability(self):
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        pdict_id = ph.create_extended_parsed()

        stream_def_id = self.pubsub_management.create_stream_definition('ctd extended', parameter_dictionary_id=pdict_id)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)

        stream_id, route = self.pubsub_management.create_stream('ctd1', 'xp1', stream_definition_id=stream_def_id)
        self.addCleanup(self.pubsub_management.delete_stream, stream_id)

        sub_id = self.pubsub_management.create_subscription('sub1', stream_ids=[stream_id])
        self.addCleanup(self.pubsub_management.delete_subscription, sub_id)
        self.pubsub_management.activate_subscription(sub_id)
        self.addCleanup(self.pubsub_management.deactivate_subscription, sub_id)

        verified = Event()
        def verifier(msg, route, stream_id):
            for k,v in msg.record_dictionary.iteritems():
                if v is not None:
                    self.assertIsInstance(v, np.ndarray)
            rdt = RecordDictionaryTool.load_from_granule(msg)
            for field in rdt.fields:
                self.assertIsInstance(rdt[field], np.ndarray)
            verified.set()

        subscriber = StandaloneStreamSubscriber('sub1', callback=verifier)
        subscriber.start()
        self.addCleanup(subscriber.stop)

        publisher = StandaloneStreamPublisher(stream_id,route)
        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        ph.fill_rdt(rdt,10)
        publisher.publish(rdt.to_granule())
        self.assertTrue(verified.wait(10))


    def test_granule(self):
        
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        stream_def_id = self.pubsub_management.create_stream_definition('ctd', parameter_dictionary_id=pdict_id, stream_configuration={'reference_designator':"GA03FLMA-RI001-13-CTDMOG999"})
        pdict = DatasetManagementService.get_parameter_dictionary_by_name('ctd_parsed_param_dict')
        self.addCleanup(self.pubsub_management.delete_stream_definition,stream_def_id)

        stream_id, route = self.pubsub_management.create_stream('ctd_stream', 'xp1', stream_definition_id=stream_def_id)
        self.addCleanup(self.pubsub_management.delete_stream,stream_id)
        publisher = StandaloneStreamPublisher(stream_id, route)

        subscriber = StandaloneStreamSubscriber('sub', self.verify_incoming)
        subscriber.start()

        subscription_id = self.pubsub_management.create_subscription('sub', stream_ids=[stream_id])
        self.pubsub_management.activate_subscription(subscription_id)


        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = np.arange(10)
        rdt['temp'] = np.random.randn(10) * 10 + 30
        rdt['pressure'] = [20] * 10

        self.assertEquals(set(pdict.keys()), set(rdt.fields))
        self.assertEquals(pdict.temporal_parameter_name, rdt.temporal_parameter)

        self.assertEquals(rdt._stream_config['reference_designator'],"GA03FLMA-RI001-13-CTDMOG999")

        self.rdt = rdt
        self.data_producer_id = 'data_producer'
        self.provider_metadata_update = {1:1}

        publisher.publish(rdt.to_granule(data_producer_id='data_producer', provider_metadata_update={1:1}))

        self.assertTrue(self.event.wait(10))
        
        self.pubsub_management.deactivate_subscription(subscription_id)
        self.pubsub_management.delete_subscription(subscription_id)
        
        filtered_stream_def_id = self.pubsub_management.create_stream_definition('filtered', parameter_dictionary_id=pdict_id, available_fields=['time', 'temp'])
        self.addCleanup(self.pubsub_management.delete_stream_definition, filtered_stream_def_id)
        rdt = RecordDictionaryTool(stream_definition_id=filtered_stream_def_id)
        self.assertEquals(rdt._available_fields,['time','temp'])
        rdt['time'] = np.arange(20)
        rdt['temp'] = np.arange(20)
        with self.assertRaises(KeyError):
            rdt['pressure'] = np.arange(20)

        granule = rdt.to_granule(connection_id='c1', connection_index='0')
        rdt2 = RecordDictionaryTool.load_from_granule(granule)
        self.assertEquals(rdt._available_fields, rdt2._available_fields)
        self.assertEquals(rdt.fields, rdt2.fields)
        self.assertEquals(rdt2.connection_id,'c1')
        self.assertEquals(rdt2.connection_index,'0')
        for k,v in rdt.iteritems():
            self.assertTrue(np.array_equal(rdt[k], rdt2[k]))
        
        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = np.array([None,None,None])
        self.assertTrue(rdt['time'] is None)
        
        rdt['time'] = np.array([None, 1, 2])
        self.assertEquals(rdt['time'][0], rdt.fill_value('time'))


    def test_rdt_param_funcs(self):
        rdt = self.create_rdt()
        rdt['TIME'] = [0]
        rdt['TEMPWAT_L0'] = [280000]
        rdt['CONDWAT_L0'] = [100000]
        rdt['PRESWAT_L0'] = [2789]

        rdt['LAT'] = [45]
        rdt['LON'] = [-71]

        np.testing.assert_array_almost_equal(rdt['DENSITY'], np.array([1001.76506258]))

    def test_rdt_lookup(self):
        rdt = self.create_lookup_rdt()

        self.assertTrue('offset_a' in rdt.lookup_values())
        self.assertFalse('offset_b' in rdt.lookup_values())

        rdt['time'] = [0]
        rdt['temp'] = [10.0]
        rdt['offset_a'] = [2.0]
        self.assertEquals(rdt['offset_b'], None)
        self.assertEquals(rdt.lookup_values(), ['offset_a'])
        np.testing.assert_array_almost_equal(rdt['calibrated'], np.array([12.0]))

        svm = StoredValueManager(self.container)
        svm.stored_value_cas('coefficient_document', {'offset_b':2.0})
        svm.stored_value_cas("GA03FLMA-RI001-13-CTDMOG999_OFFSETC", {'offset_c':3.0})
        rdt.fetch_lookup_values()
        np.testing.assert_array_equal(rdt['offset_b'], np.array([2.0]))
        np.testing.assert_array_equal(rdt['calibrated_b'], np.array([14.0]))
        np.testing.assert_array_equal(rdt['offset_c'], np.array([3.0]))

    def test_global_range_lookup(self):
        reference_designator = "CE01ISSM-MF005-01-CTDBPC999"
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        pdict_id = ph.create_simple_qc_pdict()
        svm = StoredValueManager(self.container)
        doc_key = 'grt_%s_TEMPWAT' % reference_designator
        svm.stored_value_cas(doc_key, {'grt_min_value':-2, 'grt_max_value':40})
        
        stream_def_id = self.pubsub_management.create_stream_definition('qc parsed', parameter_dictionary_id=pdict_id, stream_configuration={'reference_designator':reference_designator})
        self.addCleanup(self.pubsub_management.delete_stream_definition,stream_def_id)
        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = [0]
        rdt['temp'] = [20]
        rdt.fetch_lookup_values()
        min_field = [i for i in rdt.fields if 'grt_min_value' in i][0]
        max_field = [i for i in rdt.fields if 'grt_max_value' in i][0]

        np.testing.assert_array_almost_equal(rdt[min_field], [-2.])
        np.testing.assert_array_almost_equal(rdt[max_field], [40.])

        np.testing.assert_array_almost_equal(rdt['temp_glblrng_qc'],[1])



    def create_rdt(self):
        contexts, pfuncs = self.create_pfuncs()
        context_ids = [_id for ct,_id in contexts.itervalues()]

        pdict_id = self.dataset_management.create_parameter_dictionary(name='functional_pdict', parameter_context_ids=context_ids, temporal_context='test_TIME')
        self.addCleanup(self.dataset_management.delete_parameter_dictionary, pdict_id)
        stream_def_id = self.pubsub_management.create_stream_definition('functional', parameter_dictionary_id=pdict_id)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)
        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        return rdt

    def create_lookup_rdt(self):
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        pdict_id = ph.create_lookups()

        stream_def_id = self.pubsub_management.create_stream_definition('lookup', parameter_dictionary_id=pdict_id, stream_configuration={'reference_designator':"GA03FLMA-RI001-13-CTDMOG999"})
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)
        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        return rdt


    def create_pfuncs(self):
        
        contexts = {}
        funcs = {}

        t_ctxt = ParameterContext('TIME', param_type=QuantityType(value_encoding=np.dtype('int64')))
        t_ctxt.uom = 'seconds since 1900-01-01'
        t_ctxt_id = self.dataset_management.create_parameter_context(name='test_TIME', parameter_context=t_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, t_ctxt_id)
        contexts['TIME'] = (t_ctxt, t_ctxt_id)

        lat_ctxt = ParameterContext('LAT', param_type=ConstantType(QuantityType(value_encoding=np.dtype('float32'))), fill_value=-9999)
        lat_ctxt.axis = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        lat_ctxt_id = self.dataset_management.create_parameter_context(name='test_LAT', parameter_context=lat_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, lat_ctxt_id)
        contexts['LAT'] = lat_ctxt, lat_ctxt_id

        lon_ctxt = ParameterContext('LON', param_type=ConstantType(QuantityType(value_encoding=np.dtype('float32'))), fill_value=-9999)
        lon_ctxt.axis = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        lon_ctxt_id = self.dataset_management.create_parameter_context(name='test_LON', parameter_context=lon_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, lon_ctxt_id)
        contexts['LON'] = lon_ctxt, lon_ctxt_id

        # Independent Parameters

        # Temperature - values expected to be the decimal results of conversion from hex
        temp_ctxt = ParameterContext('TEMPWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
        temp_ctxt.uom = 'deg_C'
        temp_ctxt_id = self.dataset_management.create_parameter_context(name='test_TEMPWAT_L0', parameter_context=temp_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, temp_ctxt_id)
        contexts['TEMPWAT_L0'] = temp_ctxt, temp_ctxt_id

        # Conductivity - values expected to be the decimal results of conversion from hex
        cond_ctxt = ParameterContext('CONDWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
        cond_ctxt.uom = 'S m-1'
        cond_ctxt_id = self.dataset_management.create_parameter_context(name='test_CONDWAT_L0', parameter_context=cond_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, cond_ctxt_id)
        contexts['CONDWAT_L0'] = cond_ctxt, cond_ctxt_id

        # Pressure - values expected to be the decimal results of conversion from hex
        press_ctxt = ParameterContext('PRESWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
        press_ctxt.uom = 'dbar'
        press_ctxt_id = self.dataset_management.create_parameter_context(name='test_PRESWAT_L0', parameter_context=press_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, press_ctxt_id)
        contexts['PRESWAT_L0'] = press_ctxt, press_ctxt_id


        # Dependent Parameters

        # TEMPWAT_L1 = (TEMPWAT_L0 / 10000) - 10
        tl1_func = '(T / 10000) - 10'
        expr = NumexprFunction('TEMPWAT_L1', tl1_func, ['T'])
        expr_id = self.dataset_management.create_parameter_function(name='test_TEMPWAT_L1', parameter_function=expr.dump())
        self.addCleanup(self.dataset_management.delete_parameter_function, expr_id)
        funcs['TEMPWAT_L1'] = expr, expr_id

        tl1_pmap = {'T': 'TEMPWAT_L0'}
        expr.param_map = tl1_pmap
        tempL1_ctxt = ParameterContext('TEMPWAT_L1', param_type=ParameterFunctionType(function=expr), variability=VariabilityEnum.TEMPORAL)
        tempL1_ctxt.uom = 'deg_C'
        tempL1_ctxt_id = self.dataset_management.create_parameter_context(name='test_TEMPWAT_L1', parameter_context=tempL1_ctxt.dump(), parameter_function_id=expr_id)
        self.addCleanup(self.dataset_management.delete_parameter_context, tempL1_ctxt_id)
        contexts['TEMPWAT_L1'] = tempL1_ctxt, tempL1_ctxt_id

        # CONDWAT_L1 = (CONDWAT_L0 / 100000) - 0.5
        cl1_func = '(C / 100000) - 0.5'
        expr = NumexprFunction('CONDWAT_L1', cl1_func, ['C'])
        expr_id = self.dataset_management.create_parameter_function(name='test_CONDWAT_L1', parameter_function=expr.dump())
        self.addCleanup(self.dataset_management.delete_parameter_function, expr_id)
        funcs['CONDWAT_L1'] = expr, expr_id

        cl1_pmap = {'C': 'CONDWAT_L0'}
        expr.param_map = cl1_pmap
        condL1_ctxt = ParameterContext('CONDWAT_L1', param_type=ParameterFunctionType(function=expr), variability=VariabilityEnum.TEMPORAL)
        condL1_ctxt.uom = 'S m-1'
        condL1_ctxt_id = self.dataset_management.create_parameter_context(name='test_CONDWAT_L1', parameter_context=condL1_ctxt.dump(), parameter_function_id=expr_id)
        self.addCleanup(self.dataset_management.delete_parameter_context, condL1_ctxt_id)
        contexts['CONDWAT_L1'] = condL1_ctxt, condL1_ctxt_id

        # Equation uses p_range, which is a calibration coefficient - Fixing to 679.34040721
        #   PRESWAT_L1 = (PRESWAT_L0 * p_range / (0.85 * 65536)) - (0.05 * p_range)
        pl1_func = '(P * p_range / (0.85 * 65536)) - (0.05 * p_range)'
        expr = NumexprFunction('PRESWAT_L1', pl1_func, ['P', 'p_range'])
        expr_id = self.dataset_management.create_parameter_function(name='test_PRESWAT_L1', parameter_function=expr.dump())
        self.addCleanup(self.dataset_management.delete_parameter_function, expr_id)
        funcs['PRESWAT_L1'] = expr, expr_id
        
        pl1_pmap = {'P': 'PRESWAT_L0', 'p_range': 679.34040721}
        expr.param_map = pl1_pmap
        presL1_ctxt = ParameterContext('PRESWAT_L1', param_type=ParameterFunctionType(function=expr), variability=VariabilityEnum.TEMPORAL)
        presL1_ctxt.uom = 'S m-1'
        presL1_ctxt_id = self.dataset_management.create_parameter_context(name='test_CONDWAT_L1', parameter_context=presL1_ctxt.dump(), parameter_function_id=expr_id)
        self.addCleanup(self.dataset_management.delete_parameter_context, presL1_ctxt_id)
        contexts['PRESWAT_L1'] = presL1_ctxt, presL1_ctxt_id

        # Density & practical salinity calucluated using the Gibbs Seawater library - available via python-gsw project:
        #       https://code.google.com/p/python-gsw/ & http://pypi.python.org/pypi/gsw/3.0.1

        # PRACSAL = gsw.SP_from_C((CONDWAT_L1 * 10), TEMPWAT_L1, PRESWAT_L1)
        owner = 'gsw'
        sal_func = 'SP_from_C'
        sal_arglist = ['C', 't', 'p']
        expr = PythonFunction('PRACSAL', owner, sal_func, sal_arglist)
        expr_id = self.dataset_management.create_parameter_function(name='test_PRACSAL', parameter_function=expr.dump())
        self.addCleanup(self.dataset_management.delete_parameter_function, expr_id)
        funcs['PRACSAL'] = expr, expr_id
        
        # A magic function that may or may not exist actually forms the line below at runtime.
        sal_pmap = {'C': NumexprFunction('CONDWAT_L1*10', 'C*10', ['C'], param_map={'C': 'CONDWAT_L1'}), 't': 'TEMPWAT_L1', 'p': 'PRESWAT_L1'}
        expr.param_map = sal_pmap
        sal_ctxt = ParameterContext('PRACSAL', param_type=ParameterFunctionType(expr), variability=VariabilityEnum.TEMPORAL)
        sal_ctxt.uom = 'g kg-1'
        sal_ctxt_id = self.dataset_management.create_parameter_context(name='test_PRACSAL', parameter_context=sal_ctxt.dump(), parameter_function_id=expr_id)
        self.addCleanup(self.dataset_management.delete_parameter_context, sal_ctxt_id)
        contexts['PRACSAL'] = sal_ctxt, sal_ctxt_id

        # absolute_salinity = gsw.SA_from_SP(PRACSAL, PRESWAT_L1, longitude, latitude)
        # conservative_temperature = gsw.CT_from_t(absolute_salinity, TEMPWAT_L1, PRESWAT_L1)
        # DENSITY = gsw.rho(absolute_salinity, conservative_temperature, PRESWAT_L1)
        owner = 'gsw'
        abs_sal_expr = PythonFunction('abs_sal', owner, 'SA_from_SP', ['PRACSAL', 'PRESWAT_L1', 'LON','LAT'])
        cons_temp_expr = PythonFunction('cons_temp', owner, 'CT_from_t', [abs_sal_expr, 'TEMPWAT_L1', 'PRESWAT_L1'])
        dens_expr = PythonFunction('DENSITY', owner, 'rho', [abs_sal_expr, cons_temp_expr, 'PRESWAT_L1'])
        dens_ctxt = ParameterContext('DENSITY', param_type=ParameterFunctionType(dens_expr), variability=VariabilityEnum.TEMPORAL)
        dens_ctxt.uom = 'kg m-3'
        dens_ctxt_id = self.dataset_management.create_parameter_context(name='test_DENSITY', parameter_context=dens_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, dens_ctxt_id)
        contexts['DENSITY'] = dens_ctxt, dens_ctxt_id
        return contexts, funcs

    


        
