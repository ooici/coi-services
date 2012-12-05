#!/usr/bin/env python

'''
@brief Test the new transform prototype against streams and events
@author Swarbhanu Chatterjee
'''

from pyon.public import log
from pyon.ion.stream import StandaloneStreamPublisher
from pyon.util.int_test import IonIntegrationTestCase
from pyon.event.event import EventPublisher, EventSubscriber
from nose.plugins.attrib import attr
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.objects import ProcessDefinition
import gevent, unittest, os
import datetime, time
import random, numpy
from datetime import timedelta
from interface.objects import StreamRoute, DeviceStatusType, DeviceCommsType
from interface.services.cei.ischeduler_service import SchedulerServiceClient

@attr('INT', group='dm')
class TransformPrototypeIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(TransformPrototypeIntTest, self).setUp()

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.rrc = ResourceRegistryServiceClient()
        self.dataset_management = DatasetManagementServiceClient()
        self.pubsub_management = PubsubManagementServiceClient()
        self.ssclient = SchedulerServiceClient()
        self.event_publisher = EventPublisher()

        self.exchange_names = []
        self.exchange_points = []

    def tearDown(self):

        for xn in self.exchange_names:
            xni = self.container.ex_manager.create_xn_queue(xn)
            xni.delete()
        for xp in self.exchange_points:
            xpi = self.container.ex_manager.create_xp(xp)
            xpi.delete()

    def now_utc(self):
        return time.mktime(datetime.datetime.utcnow().timetuple())

    def _create_interval_timer_with_end_time(self,timer_interval= None, end_time = None ):
        '''
        A convenience method to set up an interval timer with an end time
        '''
        self.timer_received_time = 0
        self.timer_interval = timer_interval

        start_time = self.now_utc()
        if not end_time:
            end_time = start_time + 2 * timer_interval + 1

        log.debug("got the end time here!! %s" % end_time)

        # Set up the interval timer. The scheduler will publish event with origin set as "Interval Timer"
        sid = self.ssclient.create_interval_timer(start_time="now" ,
            interval=self.timer_interval,
            end_time=end_time,
            event_origin="Interval Timer",
            event_subtype="")

        def cleanup_timer(scheduler, schedule_id):
            """
            Do a friendly cancel of the scheduled event.
            If it fails, it's ok.
            """
            try:
                scheduler.cancel_timer(schedule_id)
            except:
                log.warn("Couldn't cancel")

        self.addCleanup(cleanup_timer, self.ssclient, sid)

        return sid

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_event_processing(self):
        '''
        Test that events are processed by the transforms according to a provided algorithm
        '''


        #-------------------------------------------------------------------------------------
        # Set up the scheduler for an interval timer with an end time
        #-------------------------------------------------------------------------------------
        id = self._create_interval_timer_with_end_time(timer_interval=2)
        self.assertIsNotNone(id)

        #-------------------------------------------------------------------------------------
        # Create an event alert transform....
        # The configuration for the Event Alert Transform... set up the event types to listen to
        #-------------------------------------------------------------------------------------
        configuration = {
            'process':{
                'event_type': 'ResourceEvent',
                'timer_origin': 'Interval Timer',
                'instrument_origin': 'My_favorite_instrument'
            }
        }

        #-------------------------------------------------------------------------------------
        # Create the process
        #-------------------------------------------------------------------------------------
        pid = TransformPrototypeIntTest.create_process(  name= 'event_alert_transform',
            module='ion.processes.data.transforms.event_alert_transform',
            class_name='EventAlertTransform',
            configuration= configuration)

        self.assertIsNotNone(pid)

        #-------------------------------------------------------------------------------------
        # Publish events and make assertions about alerts
        #-------------------------------------------------------------------------------------

        queue = gevent.queue.Queue()

        def event_received(message, headers):
            queue.put(message)

        event_subscriber = EventSubscriber( origin="EventAlertTransform",
            event_type="DeviceEvent",
            callback=event_received)

        event_subscriber.start()
        self.addCleanup(event_subscriber.stop)

        # publish event twice

        for i in xrange(5):
            self.event_publisher.publish_event(    event_type = 'ExampleDetectableEvent',
                origin = "My_favorite_instrument",
                voltage = 5,
                telemetry = 10,
                temperature = 20)
            gevent.sleep(0.1)
            self.assertTrue(queue.empty())



        #publish event the third time but after a time interval larger than 2 seconds
        gevent.sleep(5)

        #-------------------------------------------------------------------------------------
        # Make assertions about the alert event published by the EventAlertTransform
        #-------------------------------------------------------------------------------------

        event = queue.get(timeout=10)

        log.debug("Alarm event received from the EventAertTransform %s" % event)

        self.assertEquals(event.type_, "DeviceEvent")
        self.assertEquals(event.origin, "EventAlertTransform")

        #------------------------------------------------------------------------------------------------
        # Now clear the event queue being populated by alarm events and publish normally once again
        #------------------------------------------------------------------------------------------------

        queue.queue.clear()

        for i in xrange(5):
            self.event_publisher.publish_event(    event_type = 'ExampleDetectableEvent',
                origin = "My_favorite_instrument",
                voltage = 5,
                telemetry = 10,
                temperature = 20)
            gevent.sleep(0.1)
            self.assertTrue(queue.empty())

        log.debug("This completes the requirement that the EventAlertTransform publishes \
                    an alarm event when it does not hear from the instrument for some time.")


    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_stream_processing(self):
        #--------------------------------------------------------------------------------
        #Test that streams are processed by the transforms according to a provided algorithm
        #--------------------------------------------------------------------------------

        #todo: In this simple implementation, we are checking if the stream has the word, PUBLISH,
        #todo(contd) and if the word VALUE=<number> exists and that number is less than something

        #todo later on we are going to use complex algorithms to make this prototype powerful

        #-------------------------------------------------------------------------------------
        # Start a subscriber to listen for an alert event from the Stream Alert Transform
        #-------------------------------------------------------------------------------------

        queue = gevent.queue.Queue()

        def event_received(message, headers):
            queue.put(message)

        event_subscriber = EventSubscriber( origin="StreamAlertTransform",
            event_type="DeviceEvent",
            callback=event_received)

        event_subscriber.start()
        self.addCleanup(event_subscriber.stop)

        #-------------------------------------------------------------------------------------
        # The configuration for the Stream Alert Transform... set up the event types to listen to
        #-------------------------------------------------------------------------------------
        config = {
            'process':{
                'queue_name': 'a_queue',
                'value': 10,
                'event_type':'DeviceEvent'
            }
        }

        #-------------------------------------------------------------------------------------
        # Create the process
        #-------------------------------------------------------------------------------------
        pid = TransformPrototypeIntTest.create_process( name= 'transform_data_process',
            module='ion.processes.data.transforms.event_alert_transform',
            class_name='StreamAlertTransform',
            configuration= config)

        self.assertIsNotNone(pid)

        #-------------------------------------------------------------------------------------
        # Publish streams and make assertions about alerts
        #-------------------------------------------------------------------------------------
        exchange_name = 'a_queue'
        exchange_point = 'test_exchange'
        routing_key = 'stream_id.stream'
        stream_route = StreamRoute(exchange_point, routing_key)

        xn = self.container.ex_manager.create_xn_queue(exchange_name)
        xp = self.container.ex_manager.create_xp(exchange_point)
        xn.bind('stream_id.stream', xp)

        pub = StandaloneStreamPublisher('stream_id', stream_route)

        message = "A dummy example message containing the word PUBLISH, and with VALUE = 5 . This message" +\
                  " will trigger an alert event from the StreamAlertTransform because the value provided is "\
                  "less than 10 that was passed in through the config."

        pub.publish(message)

        event = queue.get(timeout=10)
        self.assertEquals(event.type_, "DeviceEvent")
        self.assertEquals(event.origin, "StreamAlertTransform")

    #        self.purge_queues(exchange_name)

    #    def purge_queues(self, exchange_name):
    #        xn = self.container.ex_manager.create_xn_queue(exchange_name)
    #        xn.purge()

    @staticmethod
    def create_process(name= '', module = '', class_name = '', configuration = None):
        '''
        A helper method to create a process
        '''

        producer_definition = ProcessDefinition(name=name)
        producer_definition.executable = {
            'module':module,
            'class': class_name
        }

        process_dispatcher = ProcessDispatcherServiceClient()

        procdef_id = process_dispatcher.create_process_definition(process_definition=producer_definition)
        pid = process_dispatcher.schedule_process(process_definition_id= procdef_id, configuration=configuration)

        return pid

    def test_demo_stream_granules_processing(self):
        """
        Test that the Demo Stream Alert Transform is functioning. The transform coordinates with the scheduler.
        It is configured to listen to a source that publishes granules. It publishes a DeviceStatusEvent if it
        receives a granule with bad data or a DeviceCommsEvent if no granule has arrived between two timer events.

        The transform is configured at launch using a config dictionary.
        """
        #-------------------------------------------------------------------------------------
        # Start a subscriber to listen for an alert event from the Stream Alert Transform
        #-------------------------------------------------------------------------------------

        queue_bad_data = gevent.queue.Queue()
        queue_no_data = gevent.queue.Queue()

        def bad_data(message, headers):
            log.debug("Got a BAD data event: %s" % message)
            if message.type_ == "DeviceStatusEvent":
                queue_bad_data.put(message)

        def no_data(message, headers):
            log.debug("Got a NO data event: %s" % message)
            queue_no_data.put(message)

        event_subscriber_bad_data = EventSubscriber( origin="instrument_1",
            event_type="DeviceStatusEvent",
            callback=bad_data)

        event_subscriber_no_data = EventSubscriber( origin="instrument_1",
            event_type="DeviceCommsEvent",
            callback=no_data)

        event_subscriber_bad_data.start()
        event_subscriber_no_data.start()

        self.addCleanup(event_subscriber_bad_data.stop)
        self.addCleanup(event_subscriber_no_data.stop)

        #-------------------------------------------------------------------------------------
        # The configuration for the Stream Alert Transform... set up the event types to listen to
        #-------------------------------------------------------------------------------------
        self.valid_values = [-100, 100]
        self.timer_interval = 5
        self.queue_name = 'a_queue'

        config = {
            'process':{
                'timer_interval': self.timer_interval,
                'queue_name': self.queue_name,
                'variable_name': 'input_voltage',
                'time_field_name': 'preferred_timestamp',
                'valid_values': self.valid_values,
                'timer_origin': 'Interval Timer',
                'event_origin': 'instrument_1'
            }
        }

        #-------------------------------------------------------------------------------------
        # Create the process
        #-------------------------------------------------------------------------------------
        pid = TransformPrototypeIntTest.create_process( name= 'DemoStreamAlertTransform',
            module='ion.processes.data.transforms.event_alert_transform',
            class_name='DemoStreamAlertTransform',
            configuration= config)

        self.assertIsNotNone(pid)

        #-------------------------------------------------------------------------------------
        # Publish streams and make assertions about alerts
        #-------------------------------------------------------------------------------------

        pdict_id = self.dataset_management.read_parameter_dictionary_by_name(name= 'platform_eng_parsed', id_only=True)

        stream_def_id = self.pubsub_management.create_stream_definition('demo_stream', parameter_dictionary_id=pdict_id)
        stream_id, stream_route = self.pubsub_management.create_stream( name='test_demo_alert',
            exchange_point='exch_point_1',
            stream_definition_id=stream_def_id)

        sub_1 = self.pubsub_management.create_subscription(name='sub_1', stream_ids=[stream_id], exchange_points=['exch_point_1'], exchange_name=self.queue_name)
        self.pubsub_management.activate_subscription(sub_1)
        self.exchange_names.append('sub_1')
        self.exchange_points.append('exch_point_1')

        #-------------------------------------------------------------------------------------
        # publish a *GOOD* granule
        #-------------------------------------------------------------------------------------
        self.length = 2
        val = numpy.array([random.uniform(0,50)  for l in xrange(self.length)])
        self._publish_granules(stream_id= stream_id, stream_route= stream_route, number=1, values=val)

        self.assertTrue(queue_bad_data.empty())

        #-------------------------------------------------------------------------------------
        # publish a few *BAD* granules
        #-------------------------------------------------------------------------------------
        self.number = 2
        val = numpy.array([(110 + l)  for l in xrange(self.length)])
        self._publish_granules(stream_id= stream_id, stream_route= stream_route, number= self.number, values=val)

        for number in xrange(self.number):
            event = queue_bad_data.get(timeout=40)
            self.assertEquals(event.type_, "DeviceStatusEvent")
            self.assertEquals(event.origin, "instrument_1")
            self.assertEquals(event.state, DeviceStatusType.OUT_OF_RANGE)
            self.assertEquals(event.valid_values, self.valid_values)
            self.assertEquals(event.sub_type, 'input_voltage')
            self.assertTrue(set(event.values) ==  set(val))

            s = set(event.time_stamps)
            cond = s in [set(numpy.array([1  for l in xrange(self.length)]).tolist()), set(numpy.array([2  for l in xrange(self.length)]).tolist())]
            self.assertTrue(cond)

        # To ensure that only the bad values generated the alert events. Queue should be empty now
        self.assertEquals(queue_bad_data.qsize(), 0)

        #-------------------------------------------------------------------------------------
        # Do not publish any granules for some time. This should generate a DeviceCommsEvent for the communication status
        #-------------------------------------------------------------------------------------
        event = queue_no_data.get(timeout=15)

        self.assertEquals(event.type_, "DeviceCommsEvent")
        self.assertEquals(event.origin, "instrument_1")
        self.assertEquals(event.origin_type, "PlatformDevice")
        self.assertEquals(event.state, DeviceCommsType.DATA_DELIVERY_INTERRUPTION)
        self.assertEquals(event.sub_type, 'input_voltage')

        #-------------------------------------------------------------------------------------
        # Empty the queues and repeat tests
        #-------------------------------------------------------------------------------------
        queue_bad_data.queue.clear()
        queue_no_data.queue.clear()

        #-------------------------------------------------------------------------------------
        # publish a *GOOD* granule again
        #-------------------------------------------------------------------------------------
        val = numpy.array([(l + 20)  for l in xrange(self.length)])
        self._publish_granules(stream_id= stream_id, stream_route= stream_route, number=1, values=val)

        self.assertTrue(queue_bad_data.empty())

        #-------------------------------------------------------------------------------------
        # Again do not publish any granules for some time. This should generate a DeviceCommsEvent for the communication status
        #-------------------------------------------------------------------------------------

        event = queue_no_data.get(timeout=20)

        self.assertEquals(event.type_, "DeviceCommsEvent")
        self.assertEquals(event.origin, "instrument_1")
        self.assertEquals(event.origin_type, "PlatformDevice")
        self.assertEquals(event.state, DeviceCommsType.DATA_DELIVERY_INTERRUPTION)
        self.assertEquals(event.sub_type, 'input_voltage')

    def _publish_granules(self, stream_id=None, stream_route=None, values = None,number=None):

        pub = StandaloneStreamPublisher(stream_id, stream_route)

        stream_def = self.pubsub_management.read_stream_definition(stream_id=stream_id)
        stream_def_id = stream_def._id
        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)

        times = numpy.array([number  for l in xrange(self.length)])

        for i in xrange(number):
            rdt['input_voltage'] = values
            rdt['preferred_timestamp'] = ['time' for l in xrange(len(times))]
            rdt['time'] = times

            g = rdt.to_granule()
            g.data_producer_id = 'instrument_1'

            log.debug("granule #%s published by instrument:: %s" % ( number,g))

            pub.publish(g)
