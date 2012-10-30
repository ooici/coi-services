"""
@author Swarbhanu Chatterjee
@file ion/services/dm/presentation/test/event_management_test.py
@description Unit and Integration test implementations for the event management service class.
"""

from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from pyon.util.containers import DotDict
from pyon.public import IonObject, RT, OT, PRED, Container, CFG
from pyon.core.exception import NotFound, BadRequest

from ion.services.dm.presentation.event_management_service import EventManagementService
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.ievent_management_service import EventManagementServiceClient
from interface.objects import PlatformEvent, EventType, ProcessDefinition
from mock import Mock, mocksignature
from nose.plugins.attrib import attr
import unittest

#@unittest.skip('Skipping before tests get completed')
@attr('UNIT',group='dm')
class EventManagementTest(PyonTestCase):
    def setUp(self):


        mock_clients = self._create_service_mock('event_management')
        self.event_management = EventManagementService()
        self.event_management.clients = mock_clients
        self.mock_rr_client = self.event_management.clients.resource_registry
        self.mock_pd_client = self.event_management.clients.process_dispatcher
        self.mock_pubsub_client = self.event_management.clients.pubsub_management

    def test_create_event_type(self):
        """
        Test creating an event
        """
        event_type = EventType(name="Test event type")
        self.mock_rr_client.create = mocksignature(self.mock_rr_client.create)
        self.mock_rr_client.create.return_value = ('event_type_id','rev_1')

        event_type_id = self.event_management.create_event_type(event_type)

        self.assertEquals(event_type_id, 'event_type_id')
        self.mock_rr_client.create.assert_called_once_with(event_type)

    def test_update_event_type(self):
        """
        Test updating an event
        """
        event_type = EventType(name="Test event type")
        self.mock_rr_client.update = mocksignature(self.mock_rr_client.update)
#        self.mock_rr_client.update.return_value = ('event_type_id','rev_1')

        self.event_management.update_event_type(event_type)
        self.mock_rr_client.update.assert_called_once_with(event_type)

    def test_read_event_type(self):
        """
        Test reading an event
        """
        event_type = Mock()
        self.mock_rr_client.read = mocksignature(self.mock_rr_client.read)
        self.mock_rr_client.read.return_value = event_type

        result = self.event_management.read_event_type(event_type_id='event_type_id')
        self.assertEquals(result, event_type)
        self.mock_rr_client.read.assert_called_once_with('event_type_id', '')


    def test_delete_event_type(self):
        """
        Test updating an event
        """
        self.mock_rr_client.delete = mocksignature(self.mock_rr_client.delete)

        self.event_management.delete_event_type(event_type_id='event_type_id')
        self.mock_rr_client.delete.assert_called_once_with('event_type_id')

    def test_create_event_process_definition(self):
        """
        Test creating an event process definition
        """
        process_definition = Mock()

        self.mock_pd_client.create_process_definition=mocksignature(self.mock_pd_client.create_process_definition)
        self.mock_pd_client.create_process_definition.return_value='procdef_id'

        self.mock_pd_client.schedule_process=mocksignature(self.mock_pd_client.schedule_process)
        self.mock_pd_client.schedule_process.return_value='pid'

        pid = self.event_management.create_event_process_definition(version='version',
                                                                    module ='module',
                                                                    class_name='class_name',
                                                                    uri='uri',
                                                                    arguments='arguments')

        self.assertEquals(pid, 'pid')
        self.mock_pd_client.schedule_process.assert_called_once_with('procdef_id', None, {}, '', '')

    def test_update_event_process_definition(self):
        """
        Test updating an event process definition
        """
        process_definition_id = 'an id'
        process_def = ProcessDefinition()

        self.mock_rr_client.read = mocksignature(self.mock_rr_client.read)
        self.mock_rr_client.read.return_value = process_def

        self.mock_rr_client.update = mocksignature(self.mock_rr_client.update)

        self.event_management.update_event_process_definition(process_definition_id)

        self.mock_rr_client.update.assert_called_once_with(process_def)

    def test_read_event_process_definition(self):
        """
        Test reading an event process definition
        """
        event_process_def = Mock()
        self.mock_rr_client.read = mocksignature(self.mock_rr_client.read)
        self.mock_rr_client.read.return_value = event_process_def

        result = self.event_management.read_event_process_definition()
        self.assertEquals(result, event_process_def)

    def test_delete_event_process_definition(self):
        """
        Test deleting an event process definition
        """
        self.mock_rr_client.delete = mocksignature(self.mock_rr_client.delete)

        self.event_management.delete_event_process_definition('an id')
        self.mock_rr_client.delete.assert_called_once_with('an id')

    def test_create_event_process(self):
        """
        Test creating an event process
        """
        self.mock_pd_client.schedule_process=mocksignature(self.mock_pd_client.schedule_process)
        self.mock_pd_client.schedule_process.return_value = 'pid'

        pid = self.event_management.create_event_process()

        self.assertEquals(pid, 'pid')

    @unittest.skip("The method for the test has not yet been implemented")
    def test_update_event_process(self):
        """
        Test updating an event process
        """
        #todo What the update_event_process() should do has not yet been decided
        pass

    def test_read_event_process(self):
        """
        Test reading an event process
        """
        event_process = Mock()
        self.mock_rr_client.read = mocksignature(self.mock_rr_client.read)
        self.mock_rr_client.read.return_value = event_process

        result = self.event_management.read_event_process('event_process_id')
        self.assertEquals(result, event_process)

    def test_delete_event_process(self):
        """
        Test deleting an event process
        """
        self.mock_rr_client.delete = mocksignature(self.mock_rr_client.delete)
        self.event_management.delete_event_process('event_process_id')

        self.mock_rr_client.delete.assert_called_once_with('event_process_id')

    def test_activate_event_process(self):
        """
        Test activating an event process
        """
        pass

    def test_deactivate_event_process(self):
        """
        Test deactivating an event process
        """
        pass

    def update_event_process_inputs(self):
        """
        Test updating event process inputs
        """
        pass


@attr('INT', group='dm')
class EventManagementIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(EventManagementIntTest, self).setUp()

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.event_management = EventManagementServiceClient()
        self.rrc = ResourceRegistryServiceClient()


    def test_create_read_update_delete_event_type(self):
        """
        Test that the CRUD method for event types work correctly
        """

        pass

    def test_create_read_update_delete_event_process_definition(self):
        """
        Test that the CRUD methods for the event process definitions work correctly
        """

        pass


    def test_create_read_update_delete_event_process(self):
        """
        Test that the CRUD methods for the event processes work correctly
        """

        pass
