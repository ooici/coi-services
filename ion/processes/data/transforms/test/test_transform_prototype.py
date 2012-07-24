#!/usr/bin/env python

'''
@brief Test the new transform prototype against streams and events
@author Swarbhanu Chatterjee
'''

from pyon.ion.transforma import TransformEventListener, TransformEventPublisher, TransformAlgorithm
from pyon.public import log
from pyon.ion.stream import SimpleStreamPublisher
from pyon.util.containers import DotDict
from pyon.util.file_sys import FileSystem
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import IonUnitTestCase
from pyon.event.event import EventPublisher, EventSubscriber
from nose.plugins.attrib import attr
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.objects import ProcessDefinition, ExchangeQuery, StreamQuery


from mock import Mock, sentinel, patch
import gevent

from ion.processes.data.ctd_stream_publisher import SimpleCtdPublisher
from ion.processes.data.transforms.ctd.ctd_L0_all import ctd_L0_all
from ion.processes.data.transforms.ctd.ctd_L1_conductivity import CTDL1ConductivityTransform
from ion.processes.data.transforms.ctd.ctd_L1_pressure import CTDL1PressureTransform
from ion.processes.data.transforms.ctd.ctd_L1_temperature import CTDL1TemperatureTransform
from ion.processes.data.transforms.ctd.ctd_L2_salinity import SalinityTransform
from ion.processes.data.transforms.ctd.ctd_L2_density import DensityTransform

@attr('INT', group='dm')
class TransformPrototypeIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(TransformPrototypeIntTest, self).setUp()

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.rrc = ResourceRegistryServiceClient()
        self.event_publisher = EventPublisher()


    def test_event_processing(self):
        '''
        Test that events are processed by the transforms according to a provided algorithm
        '''

        #-------------------------------------------------------------------------------------
        # Create an event alert transform
        #-------------------------------------------------------------------------------------

        #-------------------------------------------------------------------------------------
        # The configuration for the Event Alert Transform... set up the event types to listen to
        #-------------------------------------------------------------------------------------
        configuration = {
                            'process':{
                                'event_type': 'ExampleDetectableEvent',
                                'max_count': 3,
                                'time_window': 5,
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

        proc = self.container.proc_manager.procs_by_name[pid]

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

        for i in xrange(2):
            self.event_publisher.publish_event(     event_type='ExampleDetectableEvent',
                                                    origin = "instrument_A",
                                                    ts_created = i,
                                                    voltage = 5,
                                                    telemetry = 10,
                                                    temperature = 20)
            gevent.sleep(4)
            self.assertTrue(queue.empty())


        #publish event the third time

        self.event_publisher.publish_event(     event_type='ExampleDetectableEvent',
                                                origin = "instrument_A",
                                                ts_created = 4,
                                                voltage = 5,
                                                telemetry = 10,
                                                temperature = 20)
        gevent.sleep(4)

        #-------------------------------------------------------------------------------------
        # Make assertions about the alert event published by the EventAlertTransform
        #-------------------------------------------------------------------------------------

        event = queue.get(timeout=10)

        self.assertEquals(event.type_, "DeviceEvent")
        self.assertEquals(event.origin, "EventAlertTransform")

        log.debug("This completes the requirement that the EventAlertTransform publishes \
         an event after a fixed number of events of a particular type are received from some instrument.")


    def test_stream_processing(self):
        '''
        Test that streams are processed by the transforms according to a provided algorithm
        '''

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

        xn = self.container.ex_manager.create_xn_queue(exchange_name)
        xp = self.container.ex_manager.create_xp(exchange_point)
        xn.bind('stream_id.data', xp)

        pub = SimpleStreamPublisher.new_publisher(self.container, exchange_point,'stream_id')

        message = "A dummy example message containing the word PUBLISH, and with VALUE = 5 . This message" + \
                    " will trigger an alert event from the StreamAlertTransform"

        pub.publish(message)

        gevent.sleep(4)

        event = queue.get()
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
