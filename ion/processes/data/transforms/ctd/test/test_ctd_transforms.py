#!/usr/bin/env python

'''
@brief Test to check CTD
@author Michael Meisinger
'''



from pyon.ion.stream import StreamPublisher, StreamSubscriber, StandaloneStreamPublisher
from pyon.public import log
from pyon.util.containers import DotDict
from pyon.util.file_sys import FileSystem
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import IonUnitTestCase
from nose.plugins.attrib import attr

from mock import Mock, sentinel, patch, mocksignature
from collections import defaultdict
from interface.objects import ProcessDefinition
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.objects import StreamRoute
from ion.processes.data.ctd_stream_publisher import SimpleCtdPublisher
from ion.processes.data.transforms.ctd.ctd_L0_all import ctd_L0_all
from ion.processes.data.transforms.ctd.ctd_L1_conductivity import CTDL1ConductivityTransform
from ion.processes.data.transforms.ctd.ctd_L1_pressure import CTDL1PressureTransform
from ion.processes.data.transforms.ctd.ctd_L1_temperature import CTDL1TemperatureTransform
from ion.processes.data.transforms.ctd.ctd_L2_salinity import SalinityTransform
from ion.processes.data.transforms.ctd.ctd_L2_density import DensityTransform
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from ion.util.parameter_yaml_IO import get_param_dict
import unittest, os

@attr('UNIT', group='ctd')
@unittest.skip('Not working')
class TestCtdTransforms(IonUnitTestCase):

    def setUp(self):
        # This test does not start a container so we have to hack creating a FileSystem singleton instance
#        FileSystem(DotDict())

        self.px_ctd = SimpleCtdPublisher()
        self.px_ctd.last_time = 0

        self.tx_L0 = ctd_L0_all()
        self.tx_L0.streams = defaultdict(Mock)
        self.tx_L0.cond_publisher = Mock()
        self.tx_L0.temp_publisher = Mock()
        self.tx_L0.pres_publisher = Mock()

        self.tx_L1_C = CTDL1ConductivityTransform()
        self.tx_L1_C.streams = defaultdict(Mock)

        self.tx_L1_T = CTDL1TemperatureTransform()
        self.tx_L1_T.streams = defaultdict(Mock)

        self.tx_L1_P = CTDL1PressureTransform()
        self.tx_L1_P.streams = defaultdict(Mock)

        self.tx_L2_S = SalinityTransform()
        self.tx_L2_S.streams = defaultdict(Mock)

        self.tx_L2_D = DensityTransform()
        self.tx_L2_D.streams = defaultdict(Mock)

    def test_transforms(self):

        length = 1

        packet = self.px_ctd._get_new_ctd_packet("STR_ID", length)

        log.info("Packet: %s" % packet)

        self.tx_L0.process(packet)

        self.tx_L0.cond_publisher.publish = mocksignature(self.tx_L0.cond_publisher.publish)
        self.tx_L0.cond_publisher.publish.return_value = ''

        self.tx_L0.temp_publisher.publish = mocksignature(self.tx_L0.cond_publisher.publish)
        self.tx_L0.temp_publisher.publish.return_value = ''

        self.tx_L0.pres_publisher.publish = mocksignature(self.tx_L0.cond_publisher.publish)
        self.tx_L0.pres_publisher.publish.return_value = ''

        L0_cond = self.tx_L0.cond_publisher.publish.call_args[0][0]
        L0_temp = self.tx_L0.temp_publisher.publish.call_args[0][0]
        L0_pres = self.tx_L0.pres_publisher.publish.call_args[0][0]

        log.info("L0 cond: %s" % L0_cond)
        log.info("L0 temp: %s" % L0_temp)
        log.info("L0 pres: %s" % L0_pres)

        L1_cond = self.tx_L1_C.execute(L0_cond)
        log.info("L1 cond: %s" % L1_cond)

        L1_temp = self.tx_L1_T.execute(L0_temp)
        log.info("L1 temp: %s" % L1_temp)

        L1_pres = self.tx_L1_P.execute(L0_pres)
        log.info("L1 pres: %s" % L1_pres)

        L2_sal = self.tx_L2_S.execute(packet)
        log.info("L2 sal: %s" % L2_sal)

        L2_dens = self.tx_L2_D.execute(packet)
        log.info("L2 dens: %s" % L2_dens)

@attr('INT', group='dm')
class CtdTransformsIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(CtdTransformsIntTest, self).setUp()

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.pubsub_management = PubsubManagementServiceClient()
        self.process_dispatcher = ProcessDispatcherServiceClient()



#    @unittest.skip('This version of L0 Transforms are deprecated this test needs to be rewritten')
    def test_process(self):
        '''
        Test that packets are processed by the ctd_L0_all transform
        '''


        #---------------------------------------------------------------------------------------------
        # Launch a ctd transform
        #---------------------------------------------------------------------------------------------
        process_definition = ProcessDefinition(
            name='ctd_L0_all',
            description='For testing ctd_L0_all')
        process_definition.executable['module']= 'ion.processes.data.transforms.ctd.ctd_L0_all'
        process_definition.executable['class'] = 'ctd_L0_all'
        ctd_publisher_proc_def_id = self.process_dispatcher.create_process_definition(process_definition=process_definition)

        log.debug("ctd_publisher_proc_def_id: %s" % ctd_publisher_proc_def_id)

        self.exchange_name = 'ctd_L0_all_queue'
        self.exchange_point = 'test_exchange'

        config = DotDict()
        config.process.queue_name = self.exchange_name
        config.process.exchange_point = self.exchange_point

        config.process.interval = 1.0
        config.process.publish_streams.conductivity, _ = self.pubsub_management.create_stream('test_conductivity',
            exchange_point='science_data')
        config.process.publish_streams.temperature, _ = self.pubsub_management.create_stream('test_temperature',
            exchange_point='science_data')
        config.process.publish_streams.pressure, _ = self.pubsub_management.create_stream('test_pressure',
            exchange_point='science_data')

        log.debug("test_ctd: config: %s" % config)

        # schedule the process
        self.process_dispatcher.schedule_process(process_definition_id=ctd_publisher_proc_def_id, configuration=config)

        #-------------------------------------------------------------------------------------
        # Publish packet
        #-------------------------------------------------------------------------------------

#        exchange_point = 'test_exchange'
        routing_key = 'stream_id.stream'
        stream_route = StreamRoute(self.exchange_point, routing_key)

        xn = self.container.ex_manager.create_xn_queue(self.exchange_name)
        xp = self.container.ex_manager.create_xp(self.exchange_point)
        xn.bind('stream_id.stream', xp)

        pub = StandaloneStreamPublisher('stream_id', stream_route)


        self.px_ctd = SimpleCtdPublisher()
        self.px_ctd.last_time = 0
        packet = self.px_ctd._get_new_ctd_packet(length = 5)
        log.info("Packet: %s" % packet)

        pub.publish(packet)

#        #---------------------------------------------------------------------
#        # Set up a publisher to publish a packet to the ctd transform
#        #---------------------------------------------------------------------
#        pdict = get_param_dict('ctd_parsed_param_dict')
#        stream_def_id = self.pubsub_management.create_stream_definition('ctd data', parameter_dictionary=pdict.dump())
#        input_stream_id, route = self.pubsub_management.create_stream('input_stream', exchange_point='science_data', stream_definition_id=stream_def_id)
#
#        #---------------------------------------------------------------------------------------------
#        # Launch a SimpleCtdPublisher object that publishes to the transform
#        #---------------------------------------------------------------------------------------------
#
#        # create the process definition
#        process_definition = ProcessDefinition(
#            name='SimpleCtdPublisher',
#            description='Publisher')
#        process_definition.executable['module']= 'ion.processes.data.ctd_stream_publisher'
#        process_definition.executable['class'] = 'SimpleCtdPublisher'
#        proc_def_id = self.process_dispatcher.create_process_definition(process_definition=process_definition)
#
#        log.debug("proc_def_id: %s" % proc_def_id)

#        config = DotDict()
#        config.process.stream_id, _ = self.pubsub_management.create_stream('test_ctd',exchange_point='science_data')
#
#        log.debug("test_ctd: config: %s" % config)
#
#        # schedule the process
#        self.process_dispatcher.schedule_process(process_definition_id=proc_def_id)



    @unittest.skip('write it later')
    def test_execute(self):
        '''
        Test that the other transforms (temperature, press, density) execute correctly
        '''

        pass
