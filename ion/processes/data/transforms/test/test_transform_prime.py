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


    def setup_streams(self):
        in_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('sbe37_L0_test', id_only=True)
        out_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('sbe37_L1_test', id_only=True)

        in_stream_def_id = self.pubsub_management.create_stream_definition('L0 SBE37', parameter_dictionary_id=in_pdict_id)
        self.addCleanup(self.pubsub_management.delete_stream_definition, in_stream_def_id)
        out_stream_def_id = self.pubsub_management.create_stream_definition('L1 SBE37', parameter_dictionary_id=out_pdict_id)
        self.addCleanup(self.pubsub_management.delete_stream_definition, out_stream_def_id)

        in_stream_id, in_route = self.pubsub_management.create_stream('L0 input', stream_definition_id=in_stream_def_id, exchange_point='test')
        self.addCleanup(self.pubsub_management.delete_stream, in_stream_id)
        out_stream_id, out_route = self.pubsub_management.create_stream('L0 output', stream_definition_id=out_stream_def_id, exchange_point='test')
        self.addCleanup(self.pubsub_management.delete_stream, out_stream_id)

        return [(in_stream_id, in_stream_def_id), (out_stream_id, out_stream_def_id)]

    def setup_advanced_streams(self):
        in_pdict_id = out_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('sbe37_LC_TEST', id_only=True)
        in_stream_def_id = self.pubsub_management.create_stream_definition('sbe37_instrument', parameter_dictionary_id=in_pdict_id, available_fields=['time', 'TEMPWAT_L0', 'CONDWAT_L0', 'PRESWAT_L0', 'lat', 'lon'])
        self.addCleanup(self.pubsub_management.delete_stream_definition, in_stream_def_id)

        out_stream_def_id = self.pubsub_management.create_stream_definition('sbe37_l2', parameter_dictionary_id=out_pdict_id, available_fields=['time', 'rho','PRACSAL_L2'])
        self.addCleanup(self.pubsub_management.delete_stream_definition, out_stream_def_id)

        in_stream_id, in_route = self.pubsub_management.create_stream('instrument stream', stream_definition_id=in_stream_def_id, exchange_point='test')
        self.addCleanup(self.pubsub_management.delete_stream, in_stream_id)

        out_stream_id, out_route = self.pubsub_management.create_stream('data product stream', stream_definition_id=out_stream_def_id, exchange_point='test')
        self.addCleanup(self.pubsub_management.delete_stream, out_stream_id)

        return [(in_stream_id, in_stream_def_id), (out_stream_id, out_stream_def_id)]


    def preload(self):
        config = DotDict()
        config.op = 'load'
        config.scenario = 'BASE,LC_TEST'
        config.categories = 'ParameterFunctions,ParameterDefs,ParameterDictionary'
        config.path = 'res/preload/r2_ioc'
        
        self.container.spawn_process('preload','ion.processes.bootstrap.ion_loader','IONLoader', config)

    def setup_advanced_transform(self):
        self.preload()
        queue_name = 'transform_prime'

        stream_info = self.setup_advanced_streams()
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

        self.container.spawn_process('transform_prime', 'ion.processes.data.transforms.transform_prime','TransformPrime', config)

        listen_sub_id = self.pubsub_management.create_subscription('listener', stream_ids=[out_stream_id])
        self.addCleanup(self.pubsub_management.delete_subscription, listen_sub_id)

        self.pubsub_management.activate_subscription(listen_sub_id)
        self.addCleanup(self.pubsub_management.deactivate_subscription, listen_sub_id)
        return [(in_stream_id, in_stream_def_id), (out_stream_id, out_stream_def_id)]


    def setup_transform(self):
        self.preload()
        queue_name = 'transform_prime'

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

        self.container.spawn_process('transform_prime', 'ion.processes.data.transforms.transform_prime','TransformPrime', config)

        listen_sub_id = self.pubsub_management.create_subscription('listener', stream_ids=[out_stream_id])
        self.addCleanup(self.pubsub_management.delete_subscription, listen_sub_id)

        self.pubsub_management.activate_subscription(listen_sub_id)
        self.addCleanup(self.pubsub_management.deactivate_subscription, listen_sub_id)
        return [(in_stream_id, in_stream_def_id), (out_stream_id, out_stream_def_id)]

    def setup_validator(self, validator):
        listener = StandaloneStreamSubscriber('listener', validator)
        listener.start()
        self.addCleanup(listener.stop)

    
    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_execute_advanced_transform(self):
        # Runs a transform across L0-L2 with stream definitions including available fields
        streams = self.setup_advanced_transform()
        in_stream_id, in_stream_def_id = streams[0]
        out_stream_id, out_stream_defs_id = streams[1]

        validation_event = Event()
        def validator(msg, route, stream_id):
            rdt = RecordDictionaryTool.load_from_granule(msg)
            if not np.allclose(rdt['rho'], np.array([1001.0055034])):
                return
            validation_event.set()

        self.setup_validator(validator)

        in_route = self.pubsub_management.read_stream_route(in_stream_id)
        publisher = StandaloneStreamPublisher(in_stream_id, in_route)

        outbound_rdt = RecordDictionaryTool(stream_definition_id=in_stream_def_id)
        outbound_rdt['time'] = [0]
        outbound_rdt['TEMPWAT_L0'] = [280000]
        outbound_rdt['CONDWAT_L0'] = [100000]
        outbound_rdt['PRESWAT_L0'] = [2789]

        outbound_rdt['lat'] = [45]
        outbound_rdt['lon'] = [-71]

        outbound_granule = outbound_rdt.to_granule()

        publisher.publish(outbound_granule)

        self.assertTrue(validation_event.wait(2))


    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_execute_transform(self):
        streams = self.setup_transform()
        in_stream_id, in_stream_def_id = streams[0]
        out_stream_id, out_stream_def_id = streams[1]


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

        self.setup_validator(validator)

        in_route = self.pubsub_management.read_stream_route(in_stream_id)
        publisher = StandaloneStreamPublisher(in_stream_id, in_route)

        outbound_rdt = RecordDictionaryTool(stream_definition_id=in_stream_def_id)
        outbound_rdt['time'] = [0]
        outbound_rdt['TEMPWAT_L0'] = [280000]
        outbound_rdt['CONDWAT_L0'] = [100000]
        outbound_rdt['PRESWAT_L0'] = [2789]

        outbound_rdt['lat'] = [45]
        outbound_rdt['lon'] = [-71]

        outbound_granule = outbound_rdt.to_granule()

        publisher.publish(outbound_granule)

        self.assertTrue(validation_event.wait(2))






    
