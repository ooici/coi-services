#!/usr/bin/env python

'''
@brief Test the new transform prototype against streams and events
@author Swarbhanu Chatterjee
'''

from mock import Mock, sentinel, patch
from collections import defaultdict

from pyon.public import log
from pyon.util.containers import DotDict
from pyon.util.file_sys import FileSystem
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import IonUnitTestCase
from nose.plugins.attrib import attr

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessDefinition
from ion.processes.data.transforms.transform import TransformEventListener, TransformEventPublisher, TransformAlgorithm
from ion.processes.data.transforms.event_alert_transform import EventAlertTransform, AlgorithmA

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
        self.process_dispatcher = ProcessDispatcherServiceClient()


    def test_event_processing(self):
        '''
        Test that events are processed by the transforms according to a provided algorithm
        '''

        #-------------------------------------------------------------------------------------
        # Create an event alert transform
        #-------------------------------------------------------------------------------------

        # Create an algorithm object
        query_statement = ''
        algorithm_1 = AlgorithmA(statement=query_statement, fields = [1,4,20], _operator = '+', _operator_list = None)
        algorithm_2 = AlgorithmA(statement=query_statement, fields = [1,4,20], _operator = '+', _operator_list = ['+','-'])


        # construct event types etc to listen to for this test
        event_type = 'type_1'
        event_origin = 'origin_1'
        event_origin_type = 'origin_type_1'
        event_subtype = 'subtype_1'

        # The configuration for the listener... set up the event types to listen to
        configuration = {
                            'process':{
                                'algorithm': algorithm,
                                'event_type': event_type,
                                'event_origin': event_origin,
                                'event_origin_type': event_origin_type,
                                'event_subtype': event_subtype
                            }
                        }

        # Create the process
        pid = self.create_process(  name= 'event_alert_transform',
                                    module='ion.processes.data.transforms.event_alert_transform',
                                    class_name='EventAlertTransform',
                                    configuration= configuration)

        self.assertIsNotNone(pid)


    def test_stream_processing(self):
        '''
        Test that streams are processed by the transforms according to a provided algorithm
        '''



        pass

    def create_process(self, name= '', module = '', class_name = '', configuration = None):
        '''
        A helper method to create a process
        '''

        producer_definition = ProcessDefinition(name=name)
        producer_definition.executable = {
            'module':module,
            'class': class_name
        }

        procdef_id = self.process_dispatcher.create_process_definition(process_definition=producer_definition)
        pid = self.process_dispatcher.schedule_process(process_definition_id= procdef_id, configuration=configuration)

        return pid
