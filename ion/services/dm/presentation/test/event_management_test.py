'''
@author Swarbhanu Chatterjee
@file ion/services/dm/presentation/test/event_management_test.py
@description Unit and Integration test implementations for the event management service class.
'''

from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from pyon.util.containers import DotDict
from pyon.public import IonObject, RT, OT, PRED, Container, CFG
from pyon.core.exception import NotFound, BadRequest

from ion.services.dm.presentation.event_management_service import EventManagementService
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.ievent_management_service import EventManagementServiceClient
from interface.objects import PlatformEvent
from mock import Mock, mocksignature
from nose.plugins.attrib import attr
import unittest

@unittest.skip('Skipping before tests get completed')
@attr('UNIT',group='dm')
class EventManagementTest(PyonTestCase):
    def setUp(self):


        mock_clients = self._create_service_mock('event_management')
        self.event_management = EventManagementService()
        self.event_management.clients = mock_clients
        self.event_management.container = DotDict()
        self.event_management.container.node = Mock()

        self.event_management.container['spawn_process'] = Mock()
        self.event_management.container['id'] = 'mock_container_id'
        self.event_management.container['proc_manager'] = DotDict()
        self.event_management.container.proc_manager['terminate_process'] = Mock()
        self.event_management.container.proc_manager['procs'] = {}

        self.mock_cc_spawn = self.event_management.container.spawn_process
        self.mock_cc_terminate = self.event_management.container.proc_manager.terminate_process
        self.mock_cc_procs = self.event_management.container.proc_manager.procs

        self.mock_rr_client = self.event_management.clients.resource_registry


    def test_create_event(self):
        '''
        Test creating an event
        '''
        pass

    def test_update_event(self):
        '''
        Test updating an event
        '''
        pass

    def test_read_event(self):
        '''
        Test reading an event
        '''
        pass
    def test_update_event(self):
        '''
        Test updating an event
        '''
        pass

@attr('INT', group='dm')
class EventManagementIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(EventManagementIntTest, self).setUp()

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.event_management = EventManagementServiceClient()
        self.rrc = ResourceRegistryServiceClient()


    def test_create_read_event(self):
        '''
        Test that the publishing of reload user info event occurs every time a create, update
        or delete notification occurs.
        '''

        event_in = PlatformEvent(origin='inst_1',origin_type='inst_type',sub_type='inst_subtype', ts_created='1234' )

        event_id = self.event_management.create_event(event_in)

        self.assertIsNotNone(event_id)

        event_out = self.event_management.read_event(event_id)

        self.assertEquals(event_out.type_, event_in.type_)
        self.assertEquals(event_out.origin, event_in.origin)
        self.assertEquals(event_out.origin_type, event_in.origin_type)
        self.assertEquals(event_out.sub_type, event_in.sub_type)
        self.assertEquals(event_out.ts_created, event_in.ts_created)

    def test_update_event(self):
        '''
        Test that the publishing of reload user info event occurs every time a create, update
        or delete notification occurs.
        '''

        pass

    def test_delete_event(self):
        '''
        Test that the publishing of reload user info event occurs every time a create, update
        or delete notification occurs.
        '''

        pass

