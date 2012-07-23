#!/usr/bin/env python

'''
@brief Test the new transform prototype against streams and events
@author Swarbhanu Chatterjee
'''

from pyon.ion.transforma import TransformEventListener, TransformEventPublisher, TransformAlgorithm
from pyon.public import log
from pyon.util.containers import DotDict
from pyon.util.file_sys import FileSystem
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import IonUnitTestCase
from pyon.event.event import EventPublisher, EventSubscriber
from nose.plugins.attrib import attr
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessDefinition, Algorithm

from ion.processes.data.transforms.event_alert_transform import TransformAlgorithmExample


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
        # Create an algorithm object
        query_statement = "SEARCH 'voltage' VALUES FROM 0 TO 10 FROM 'dummy_index'"
        field_names = ['voltage', 'telemetry', 'temperature']

        algorithm = Algorithm(query_statement=query_statement, field_names = field_names, operator_ = '+', operator_list_ = None)

        alg_id, _ = self.rrc.create(algorithm)

        #-------------------------------------------------------------------------------------
        # The configuration for the Event Alert Transform... set up the event types to listen to
        #-------------------------------------------------------------------------------------
        configuration = {
                            'process':{
                                'algorithm_id': alg_id,
                                'event_type': 'type_1',
                                'event_count': 3
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

        def got_event(msg, headers):
            queue.put(msg)

        proc.process_event = got_event

        # publish event twice

        for i in xrange(2):
            self.event_publisher.publish_event(     event_type='ExampleDetectableEvent',
                                                    origin = "instrument_A",
                                                    voltage = 5,
                                                    telemetry = 10,
                                                    temperature = 20)
            self.assertTrue(queue.empty())

        #publish event the third time

        self.event_publisher.publish_event(     event_type='ExampleDetectableEvent',
                                                origin = "instrument_A",
                                                voltage = 5,
                                                telemetry = 10,
                                                temperature = 20)

        # expect an alert event to be published by the EventAlertTransform
        self.assertFalse(queue.empty())
#        event = queue.get(timeout=10)

#        self.assertEquals(event.type_, "DeviceEvent")
#        self.assertEquals(event.origin, "EventAlertTransform")


    def test_stream_processing(self):
        '''
        Test that streams are processed by the transforms according to a provided algorithm
        '''


        #-------------------------------------------------------------------------------------
        # Create an algorithm object
        #-------------------------------------------------------------------------------------
        query_statement = ''
        field_names = []

        algorithm = Algorithm(query_statement=query_statement, field_names = field_names, operator_ = '+', operator_list_ = None)
        alg_id, _ = self.rrc.create(algorithm)

        #-------------------------------------------------------------------------------------
        # The configuration for the Event Alert Transform... set up the event types to listen to
        #-------------------------------------------------------------------------------------
        configuration = {
            'process':{
                'algorithm_id': alg_id
            }
        }

        #-------------------------------------------------------------------------------------
        # Create the process
        #-------------------------------------------------------------------------------------
        pid = TransformPrototypeIntTest.create_process(   name= 'transform_data_process',
                                module='ion.processes.data.transforms.event_alert_transform',
                                class_name='StreamAlertTransform',
                                configuration= configuration)

        self.assertIsNotNone(pid)

        #-------------------------------------------------------------------------------------
        # Publish streams and make assertions about alerts
        #-------------------------------------------------------------------------------------
        #todo


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
