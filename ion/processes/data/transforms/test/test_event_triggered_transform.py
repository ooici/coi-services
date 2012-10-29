#!/usr/bin/env python

'''
@brief Test to check CTD
@author Michael Meisinger
'''

from pyon.ion.stream import  StandaloneStreamPublisher
from pyon.public import log
from pyon.util.containers import DotDict
from pyon.util.file_sys import FileSystem
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import IonUnitTestCase
from pyon.util.containers import get_safe
from pyon.ion.stream import StandaloneStreamSubscriber
from pyon.event.event import EventPublisher
from nose.plugins.attrib import attr
import unittest
from mock import Mock, sentinel, patch, mocksignature
from interface.objects import ProcessDefinition
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.objects import StreamRoute, Granule
from ion.processes.data.ctd_stream_publisher import SimpleCtdPublisher
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
import gevent, os
import numpy, random
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient


class EventTriggeredTransformIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(EventTriggeredTransformIntTest, self).setUp()

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.queue_cleanup = []
        self.exchange_cleanup = []

        self.pubsub = PubsubManagementServiceClient()
        self.process_dispatcher = ProcessDispatcherServiceClient()

        self.exchange_name = 'test_queue'
        self.exchange_point = 'test_exchange'

        self.dataset_management = DatasetManagementServiceClient()

    def tearDown(self):
        for queue in self.queue_cleanup:
            xn = self.container.ex_manager.create_xn_queue(queue)
            xn.delete()
        for exchange in self.exchange_cleanup:
            xp = self.container.ex_manager.create_xp(exchange)
            xp.delete()

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_event_triggered_transform_A(self):
        '''
        Test that packets are processed by the event triggered transform
        '''

        #---------------------------------------------------------------------------------------------
        # Launch a ctd transform
        #---------------------------------------------------------------------------------------------
        # Create the process definition
        process_definition = ProcessDefinition(
            name='EventTriggeredTransform_A',
            description='For testing EventTriggeredTransform_A')
        process_definition.executable['module']= 'ion.processes.data.transforms.event_triggered_transform'
        process_definition.executable['class'] = 'EventTriggeredTransform_A'
        event_transform_proc_def_id = self.process_dispatcher.create_process_definition(process_definition=process_definition)

        # Build the config
        config = DotDict()
        config.process.queue_name = self.exchange_name
        config.process.exchange_point = self.exchange_point

        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)

        stream_def_id =  self.pubsub.create_stream_definition('cond_stream_def', parameter_dictionary_id=pdict_id)
        cond_stream_id, _ = self.pubsub.create_stream('test_conductivity',
            exchange_point='science_data',
            stream_definition_id=stream_def_id)

        config.process.publish_streams.conductivity = cond_stream_id
        config.process.event_type = 'ResourceLifecycleEvent'

        # Schedule the process
        self.process_dispatcher.schedule_process(process_definition_id=event_transform_proc_def_id, configuration=config)

        #---------------------------------------------------------------------------------------------
        # Publish an event to wake up the event triggered transform
        #---------------------------------------------------------------------------------------------

        event_publisher = EventPublisher("ResourceLifecycleEvent")
        event_publisher.publish_event(origin = 'fake_origin')

        #---------------------------------------------------------------------------------------------
        # Create subscribers that will receive the conductivity, temperature and pressure granules from
        # the ctd transform
        #---------------------------------------------------------------------------------------------
        ar_cond = gevent.event.AsyncResult()
        def subscriber1(m, r, s):
            ar_cond.set(m)
        sub_event_transform = StandaloneStreamSubscriber('sub_event_transform', subscriber1)
        self.addCleanup(sub_event_transform.stop)

        sub_event_transform_id = self.pubsub.create_subscription('subscription_cond',
            stream_ids=[cond_stream_id],
            exchange_name='sub_event_transform')

        self.pubsub.activate_subscription(sub_event_transform_id)

        self.queue_cleanup.append(sub_event_transform.xn.queue)

        sub_event_transform.start()

        #------------------------------------------------------------------------------------------------------
        # Use a StandaloneStreamPublisher to publish a packet that can be then picked up by a ctd transform
        #------------------------------------------------------------------------------------------------------

        # Do all the routing stuff for the publishing
        routing_key = 'stream_id.stream'
        stream_route = StreamRoute(self.exchange_point, routing_key)

        xn = self.container.ex_manager.create_xn_queue(self.exchange_name)
        xp = self.container.ex_manager.create_xp(self.exchange_point)
        xn.bind('stream_id.stream', xp)

        pub = StandaloneStreamPublisher('stream_id', stream_route)

        # Build a packet that can be published
        self.px_ctd = SimpleCtdPublisher()
        publish_granule = self._get_new_ctd_packet(stream_definition_id=stream_def_id, length = 5)

        # Publish the packet
        pub.publish(publish_granule)

        #------------------------------------------------------------------------------------------------------
        # Make assertions about whether the ctd transform executed its algorithm and published the correct
        # granules
        #------------------------------------------------------------------------------------------------------

        # Get the granule that is published by the ctd transform post processing
        result_cond = ar_cond.get(timeout=10)
        self.assertTrue(isinstance(result_cond, Granule))

        rdt = RecordDictionaryTool.load_from_granule(result_cond)
        self.assertTrue(rdt.__contains__('conductivity'))

        self.check_cond_algorithm_execution(publish_granule, result_cond)

    def check_cond_algorithm_execution(self, publish_granule, granule_from_transform):

        input_rdt_to_transform = RecordDictionaryTool.load_from_granule(publish_granule)
        output_rdt_transform = RecordDictionaryTool.load_from_granule(granule_from_transform)

        output_data = output_rdt_transform['conductivity']
        input_data = input_rdt_to_transform['conductivity']

        self.assertTrue(((input_data / 100000.0) - 0.5).all() == output_data.all())


    def _get_new_ctd_packet(self, stream_definition_id, length):

        rdt = RecordDictionaryTool(stream_definition_id=stream_definition_id)

        for field in rdt:
            rdt[field] = numpy.array([random.uniform(0.0,75.0)  for i in xrange(length)])

        g = rdt.to_granule()

        return g

    def test_event_triggered_transform_B(self):
        '''
        Test that packets are processed by the event triggered transform
        '''

        #---------------------------------------------------------------------------------------------
        # Launch a ctd transform
        #---------------------------------------------------------------------------------------------
        # Create the process definition
        process_definition = ProcessDefinition(
            name='EventTriggeredTransform_B',
            description='For testing EventTriggeredTransform_B')
        process_definition.executable['module']= 'ion.processes.data.transforms.event_triggered_transform'
        process_definition.executable['class'] = 'EventTriggeredTransform_B'
        event_transform_proc_def_id = self.process_dispatcher.create_process_definition(process_definition=process_definition)



        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)

        stream_def_id =  self.pubsub.create_stream_definition('stream_def', parameter_dictionary_id=pdict_id)
        stream_id, _ = self.pubsub.create_stream('test_stream',
            exchange_point='science_data',
            stream_definition_id=stream_def_id)

        # Build the config
        config = DotDict()
        config.process.queue_name = self.exchange_name
        config.process.exchange_point = self.exchange_point
        config.process.publish_streams.output = stream_id
        config.process.event_type = 'ResourceLifecycleEvent'
        config.process.stream_id = stream_id

        # Schedule the process
        self.process_dispatcher.schedule_process(process_definition_id=event_transform_proc_def_id, configuration=config)

        #---------------------------------------------------------------------------------------------
        # Publish an event to wake up the event triggered transform
        #---------------------------------------------------------------------------------------------

        event_publisher = EventPublisher("ResourceLifecycleEvent")
        event_publisher.publish_event(origin = 'fake_origin')

#        #---------------------------------------------------------------------------------------------
#        # Create subscribers that will receive the conductivity, temperature and pressure granules from
#        # the ctd transform
#        #---------------------------------------------------------------------------------------------
#        ar_cond = gevent.event.AsyncResult()
#        def subscriber1(m, r, s):
#            ar_cond.set(m)
#        sub_event_transform = StandaloneStreamSubscriber('sub_event_transform', subscriber1)
#        self.addCleanup(sub_event_transform.stop)
#
#        sub_event_transform_id = self.pubsub.create_subscription('subscription',
#            stream_ids=[stream_id],
#            exchange_name='sub_event_transform')
#
#        self.pubsub.activate_subscription(sub_event_transform_id)
#
#        self.queue_cleanup.append(sub_event_transform.xn.queue)
#
#        sub_event_transform.start()

#        #------------------------------------------------------------------------------------------------------
#        # Make assertions about whether the ctd transform executed its algorithm and published the correct
#        # granules
#        #------------------------------------------------------------------------------------------------------
#
#        # Get the granule that is published by the ctd transform post processing
#        result_cond = ar_cond.get(timeout=10)
#        self.assertTrue(isinstance(result_cond, Granule))


