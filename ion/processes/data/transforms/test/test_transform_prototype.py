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
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessDefinition
import gevent, unittest, os
import datetime, time
from datetime import timedelta
from interface.objects import StreamRoute
from interface.services.cei.ischeduler_service import SchedulerServiceClient

@attr('INT', group='dm')
class TransformPrototypeIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(TransformPrototypeIntTest, self).setUp()

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.rrc = ResourceRegistryServiceClient()
        self.ssclient = SchedulerServiceClient()
        self.event_publisher = EventPublisher()

    def now_utc(self):
        return time.mktime(datetime.datetime.utcnow().timetuple())

    def _create_interval_timer_with_end_time(self,interval_timer_interval= None ):
        '''
        A convenience method to set up an interval timer with an end time
        '''
        self.interval_timer_count = 0
        self.interval_timer_received_time = 0
        self.interval_timer_interval = interval_timer_interval

        start_time = self.now_utc()
        self.interval_timer_end_time = start_time + 5

        # Set up the interval timer. The scheduler will publish event with origin set as "Interval Timer"
        sid = self.ssclient.create_interval_timer(start_time="now" , interval=self.interval_timer_interval,
            end_time=self.interval_timer_end_time,
            event_origin="Interval Timer", event_subtype="")

        self.interval_timer_sent_time = datetime.datetime.utcnow()

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

        log.debug("Got the id here!! %s" % sid)

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
        id = self._create_interval_timer_with_end_time(interval_timer_interval=2)
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

        #-------------------------------------------------------------------------------------
        # The configuration for the Stream Alert Transform... set up the event types to listen to
        #-------------------------------------------------------------------------------------
        config = {
            'process':{
                'queue_name': 'a_queue',
                'value': 10
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

        message = "A dummy example message containing the word PUBLISH, and with VALUE = 5 . This message" + \
                    " will trigger an alert event from the StreamAlertTransform"

        pub.publish(message)

        event = queue.get(timeout=10)
        self.assertEquals(event.type_, "DeviceEvent")
        self.assertEquals(event.origin, "StreamAlertTransform")

        self.purge_queues(exchange_name)

    def purge_queues(self, exchange_name):
        xn = self.container.ex_manager.create_xn_queue(exchange_name)
        xn.purge()

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
