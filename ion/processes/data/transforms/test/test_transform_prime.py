#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell at ASAScience dot com>
@date Tue Feb 12 09:54:27 EST 2013
@file ion/processes/data/transforms/test/test_transform_prime.py
'''

from ion.services.dm.utility.granule import RecordDictionaryTool
from pyon.ion.stream import StandaloneStreamPublisher,StandaloneStreamSubscriber, StreamPublisher
from pyon.util.int_test import IonIntegrationTestCase
from coverage_model import ParameterContext, AxisTypeEnum, QuantityType, ConstantType, NumexprFunction, ParameterFunctionType, VariabilityEnum 
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from pyon.util.containers import DotDict
import gevent
from nose.plugins.attrib import attr
import numpy as np
import unittest
import os
from gevent.event import Event
from ion.processes.data.transforms.transform_prime import TransformPrime


@attr('INT',group='dm')
class TestTransformPrime(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2deploy.yml') # Because hey why not?!

        self.dataset_management      = DatasetManagementServiceClient()
        self.data_process_management = DataProcessManagementServiceClient()
        self.pubsub_management       = PubsubManagementServiceClient()
        self.data_product_management = DataProductManagementServiceClient()


    def _L0_pdict(self):
        contexts = {}

        t_ctxt = ParameterContext('TIME', param_type=QuantityType(value_encoding=np.dtype('int64')))
        t_ctxt.uom = 'seconds since 01-01-1900'
        t_ctxt_id = self.dataset_management.create_parameter_context(name='test_TIME', parameter_context=t_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, t_ctxt_id)
        contexts['TIME'] = t_ctxt_id

        lat_ctxt = ParameterContext('LAT', param_type=ConstantType(QuantityType(value_encoding=np.dtype('float32'))), fill_value=-9999)
        lat_ctxt.axis = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        lat_ctxt_id = self.dataset_management.create_parameter_context(name='test_LAT', parameter_context=lat_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, lat_ctxt_id)
        contexts['LAT'] = lat_ctxt_id

        lon_ctxt = ParameterContext('LON', param_type=ConstantType(QuantityType(value_encoding=np.dtype('float32'))), fill_value=-9999)
        lon_ctxt.axis = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        lon_ctxt_id = self.dataset_management.create_parameter_context(name='test_LON', parameter_context=lon_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, lon_ctxt_id)
        contexts['LON'] = lon_ctxt_id

        # Independent Parameters

        # Temperature - values expected to be the decimal results of conversion from hex
        temp_ctxt = ParameterContext('TEMPWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
        temp_ctxt.uom = 'deg_C'
        temp_ctxt_id = self.dataset_management.create_parameter_context(name='test_TEMPWAT_L0', parameter_context=temp_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, temp_ctxt_id)
        contexts['TEMPWAT_L0'] = temp_ctxt_id

        # Conductivity - values expected to be the decimal results of conversion from hex
        cond_ctxt = ParameterContext('CONDWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
        cond_ctxt.uom = 'S m-1'
        cond_ctxt_id = self.dataset_management.create_parameter_context(name='test_CONDWAT_L0', parameter_context=cond_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, cond_ctxt_id)
        contexts['CONDWAT_L0'] = cond_ctxt_id

        # Pressure - values expected to be the decimal results of conversion from hex
        press_ctxt = ParameterContext('PRESWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
        press_ctxt.uom = 'dbar'
        press_ctxt_id = self.dataset_management.create_parameter_context(name='test_PRESWAT_L0', parameter_context=press_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, press_ctxt_id)
        contexts['PRESWAT_L0'] = press_ctxt_id

        context_ids = contexts.values()

        pdict_id = self.dataset_management.create_parameter_dictionary('L0 SBE37', parameter_context_ids=context_ids, temporal_context='TIME')
        self.addCleanup(self.dataset_management.delete_parameter_dictionary, pdict_id)

        return pdict_id


    def _L1_pdict(self):
        contexts = {}
        funcs = {}
        # Dependent Parameters
        t_ctxt = ParameterContext('TIME', param_type=QuantityType(value_encoding=np.dtype('int64')))
        t_ctxt.uom = 'seconds since 01-01-1900'
        t_ctxt_id = self.dataset_management.create_parameter_context(name='test_TIME', parameter_context=t_ctxt.dump())
        contexts['TIME'] = t_ctxt_id

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
        tempL1_ctxt_id = self.dataset_management.create_parameter_context(name='test_TEMPWAT_L1', parameter_context=tempL1_ctxt.dump(), parameter_function_ids=[expr_id])
        self.addCleanup(self.dataset_management.delete_parameter_context, tempL1_ctxt_id)
        contexts['TEMPWAT_L1'] = tempL1_ctxt_id

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
        condL1_ctxt_id = self.dataset_management.create_parameter_context(name='test_CONDWAT_L1', parameter_context=condL1_ctxt.dump(), parameter_function_ids=[expr_id])
        self.addCleanup(self.dataset_management.delete_parameter_context, condL1_ctxt_id)
        contexts['CONDWAT_L1'] = condL1_ctxt_id

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
        presL1_ctxt_id = self.dataset_management.create_parameter_context(name='test_CONDWAT_L1', parameter_context=presL1_ctxt.dump(), parameter_function_ids=[expr_id])
        self.addCleanup(self.dataset_management.delete_parameter_context, presL1_ctxt_id)
        contexts['PRESWAT_L1'] = presL1_ctxt_id
        context_ids = contexts.values()

        pdict_id = self.dataset_management.create_parameter_dictionary('L1 SBE37', parameter_context_ids=context_ids, temporal_context='TIME')
        self.addCleanup(self.dataset_management.delete_parameter_dictionary, pdict_id)

        return pdict_id

    
    def setup_streams(self):
        in_pdict_id = self._L0_pdict()
        out_pdict_id = self._L1_pdict()

        in_stream_def_id = self.pubsub_management.create_stream_definition('L0 SBE37', parameter_dictionary_id=in_pdict_id)
        self.addCleanup(self.pubsub_management.delete_stream_definition, in_stream_def_id)
        out_stream_def_id = self.pubsub_management.create_stream_definition('L1 SBE37', parameter_dictionary_id=out_pdict_id)
        self.addCleanup(self.pubsub_management.delete_stream_definition, out_stream_def_id)

        in_stream_id, in_route = self.pubsub_management.create_stream('L0 input', stream_definition_id=in_stream_def_id, exchange_point='test')
        self.addCleanup(self.pubsub_management.delete_stream, in_stream_id)
        out_stream_id, out_route = self.pubsub_management.create_stream('L0 output', stream_definition_id=out_stream_def_id, exchange_point='test')
        self.addCleanup(self.pubsub_management.delete_stream, out_stream_id)

        return [(in_stream_id, in_stream_def_id), (out_stream_id, out_stream_def_id)]


    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_execute_transform(self):
        queue_name = 'transform_prime'

        tp = TransformPrime()
        stream_info = self.setup_streams()
        in_stream_id, in_stream_def_id = stream_info[0]
        out_stream_id, out_stream_def_id = stream_info[1]

        routes = {}
        routes[(in_stream_id, out_stream_id)]= None

        config = DotDict()

        config.process.queue_name = queue_name
        config.process.routes = routes
        config.process.publish_streams = {out_stream_id:out_stream_id}

        sub_id = self.pubsub_management.create_subscription(queue_name, stream_ids=[in_stream_id])
        self.addCleanup(self.pubsub_management.delete_subscription, sub_id)
        self.pubsub_management.activate_subscription(sub_id)
        self.addCleanup(self.pubsub_management.deactivate_subscription, sub_id)

        pid = self.container.spawn_process('transform_prime', 'ion.processes.data.transforms.transform_prime','TransformPrime', config)

        listen_sub_id = self.pubsub_management.create_subscription('listener', stream_ids=[out_stream_id])
        self.addCleanup(self.pubsub_management.delete_subscription, listen_sub_id)

        self.pubsub_management.activate_subscription(listen_sub_id)
        self.addCleanup(self.pubsub_management.deactivate_subscription, listen_sub_id)


        validation_event = Event()
        def validator(msg, route, stream_id):
            rdt = RecordDictionaryTool.load_from_granule(msg)
            if not np.allclose(rdt['TEMPWAT_L1'], np.array([18.])):
                return
            if not np.allclose(rdt['CONDWAT_L1'], np.array([0.5])):
                return
            if not np.allclose(rdt['PRESWAT_L1'], np.array([0.04536611])):
                return
            validation_event.set()

        listener = StandaloneStreamSubscriber('listener', validator)
        listener.start()
        self.addCleanup(listener.stop)

        in_route = self.pubsub_management.read_stream_route(in_stream_id)
        publisher = StandaloneStreamPublisher(in_stream_id, in_route)

        outbound_rdt = RecordDictionaryTool(stream_definition_id=in_stream_def_id)
        outbound_rdt['TIME'] = [0]
        outbound_rdt['TEMPWAT_L0'] = [280000]
        outbound_rdt['CONDWAT_L0'] = [100000]
        outbound_rdt['PRESWAT_L0'] = [2789]

        outbound_rdt['LAT'] = [45]
        outbound_rdt['LON'] = [-71]

        outbound_granule = outbound_rdt.to_granule()

        publisher.publish(outbound_granule)

        self.assertTrue(validation_event.wait(2))






    
