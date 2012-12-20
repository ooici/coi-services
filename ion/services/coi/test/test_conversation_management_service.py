#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

import unittest
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
from pyon.public import PRED, RT, IonObject, OT
from ion.services.coi.conversation_management_service import ConversationManagementService
from interface.services.coi.iconversation_management_service import ConversationManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

@attr('UNIT', group='coi')
class TestPolicyManagementService(PyonTestCase):

    def setUp(self):
        mock_clients = self._create_service_mock('conversation_management')

        # Rename to save some typing
        self.mock_create = mock_clients.resource_registry.create
        self.mock_read = mock_clients.resource_registry.read
        self.mock_update = mock_clients.resource_registry.update
        self.mock_delete = mock_clients.resource_registry.delete
        self.mock_create_association = mock_clients.resource_registry.create_association
        self.mock_delete_association = mock_clients.resource_registry.delete_association
        self.mock_find_objects = mock_clients.resource_registry.find_objects
        self.mock_find_resources = mock_clients.resource_registry.find_resources
        self.mock_find_subjects = mock_clients.resource_registry.find_subjects

        self.convo_management_service = ConversationManagementService()
        self.convo_management_service.clients = mock_clients

        # ConversationType
        self.convo_type = Mock()
        self.convo_type.name = 'Generic_RPC'

        # ConversationRole
        self.convo_role = Mock()
        self.convo_role.name = 'provider'

    def test_create_conversation_type(self):
        self.mock_create.return_value = ['123', 1]

        convo_type_id = self.convo_management_service.create_conversation_type(self.convo_type)

        assert convo_type_id == '123'
        self.mock_create.assert_called_once_with(self.convo_type)

    def test_read_and_update_conversation_type(self):
        self.mock_read.return_value = self.convo_type

        convo_type = self.convo_management_service.read_conversation_type('123')

        assert convo_type is self.mock_read.return_value
        self.mock_read.assert_called_once_with('123', '')

        convo_type.name = 'New_test_Type'

        self.mock_update.return_value = ['123', 2]

        self.convo_management_service.update_conversation_type(convo_type)

        self.mock_update.assert_called_once_with(convo_type)

    def test_delete_conversation_type(self):
        self.mock_read.return_value = self.convo_type
        self.mock_find_objects.return_value = ([], [])

        self.convo_management_service.delete_conversation_type('123')

        self.mock_delete.assert_called_once_with('123')

    def test_read_conversation_type_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.convo_management_service.read_conversation_type('bad type')

        ex = cm.exception
        self.assertEqual(ex.message, "Conversation Type 'bad type' does not exist")
        self.mock_read.assert_called_once_with('bad type', '')

    def test_delete_conversation_type_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.convo_management_service.delete_conversation_type('bad type')

        ex = cm.exception
        self.assertEqual(ex.message, "Conversation Type 'bad type' does not exist")
        self.mock_read.assert_called_once_with('bad type', '')


    def test_create_conversation_role(self):
        self.mock_create.return_value = ['123', 1]

        convo_role_id = self.convo_management_service.create_conversation_role(self.convo_role)

        assert convo_role_id == '123'
        self.mock_create.assert_called_once_with(self.convo_role)

    def test_read_and_update_conversation_role(self):
        self.mock_read.return_value = self.convo_role

        convo_role = self.convo_management_service.read_conversation_role('123')

        assert convo_role is self.mock_read.return_value
        self.mock_read.assert_called_once_with('123', '')

        convo_role.name = 'requester'

        self.mock_update.return_value = ['123', 2]

        self.convo_management_service.update_conversation_role(convo_role)

        self.mock_update.assert_called_once_with(convo_role)

    def test_delete_conversation_role(self):
        self.mock_read.return_value = self.convo_role
        self.mock_find_subjects.return_value = ([], [])

        self.convo_management_service.delete_conversation_role('123')

        self.mock_delete.assert_called_once_with('123')

    def test_read_conversation_role_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.convo_management_service.read_conversation_role('bad role')

        ex = cm.exception
        self.assertEqual(ex.message, "Conversation Role 'bad role' does not exist")
        self.mock_read.assert_called_once_with('bad role', '')

    def test_delete_conversation_role_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.convo_management_service.delete_conversation_role('bad role')

        ex = cm.exception
        self.assertEqual(ex.message, "Conversation Role 'bad role' does not exist")
        self.mock_read.assert_called_once_with('bad role', '')

    def test_not_implemented_methods(self):
        self.convo_management_service.create_conversation()
        self.convo_management_service.update_conversation()
        self.convo_management_service.read_conversation()
        self.convo_management_service.delete_conversation()


@attr('INT', group='coi')
class TestPolicyManagementServiceInt(IonIntegrationTestCase):

    def setUp(self):

        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2coi.yml')

        self.convo_management_service = ConversationManagementServiceClient(node=self.container.node)

        self.resource_registry = ResourceRegistryServiceClient(node=self.container.node)

        self.rpc_scribble_protocol = \
            '''global protocol RPC (role provider, role requester)
            {
                request from requester to provider;
            choice at provider {
                accept from provider to requester;
            choice at provider {
                (inform) from provider to requester;
            } or {
                (failure) from provider to requester;}
            } or {
                reject from provider to requester;}}'''


    def test_conversation_type_crud(self):
        bad_obj = IonObject('ConversationType', name='bad name', definition=self.rpc_scribble_protocol)
        with self.assertRaises(BadRequest):
            self.convo_management_service.create_conversation_type(bad_obj)

        convo_type_obj = IonObject('ConversationType', name='RPC', definition=self.rpc_scribble_protocol)
        convo_type_id = self.convo_management_service.create_conversation_type(convo_type_obj)
        self.assertNotEqual(convo_type_id, None)

        with self.assertRaises(BadRequest):
            self.convo_management_service.read_conversation_type()

        convo_type = None
        convo_type = self.convo_management_service.read_conversation_type(convo_type_id)
        self.assertNotEqual(convo_type, None)

        convo_type.name = 'RPC_2'
        self.convo_management_service.update_conversation_type(convo_type)

        convo_type = None
        convo_type = self.convo_management_service.read_conversation_type(convo_type_id)
        self.assertNotEqual(convo_type, None)
        self.assertEqual(convo_type.name, 'RPC_2')

        self.convo_management_service.delete_conversation_type(convo_type_id)

        with self.assertRaises(NotFound) as cm:
            self.convo_management_service.read_conversation_type(convo_type_id)
        self.assertIn("does not exist", cm.exception.message)

        with self.assertRaises(NotFound) as cm:
            self.convo_management_service.delete_conversation_type(convo_type_id)
        self.assertIn("does not exist", cm.exception.message)
        with self.assertRaises(BadRequest):
            self.convo_management_service.delete_conversation_type()

    def test_conversation_role_crud(self):
        bad_obj = IonObject("ConversationRole", {"name": "bad name"})
        with self.assertRaises(BadRequest):
            self.convo_management_service.create_conversation_role(bad_obj)
        convo_role_obj = IonObject("ConversationRole", {"name": "provider"})
        convo_role_id = self.convo_management_service.create_conversation_role(convo_role_obj)
        self.assertNotEqual(convo_role_id, None)

        with self.assertRaises(BadRequest):
            self.convo_management_service.read_conversation_role()
        convo_role = None
        convo_role = self.convo_management_service.read_conversation_role(convo_role_id)
        self.assertNotEqual(convo_role, None)

        with self.assertRaises(BadRequest):
            self.convo_management_service.update_conversation_type(bad_obj)
        convo_role.name = 'requester'
        self.convo_management_service.update_conversation_type(convo_role)

        convo_role = None
        convo_role = self.convo_management_service.read_conversation_role(convo_role_id)
        self.assertNotEqual(convo_role, None)
        self.assertEqual(convo_role.name, 'requester')

        with self.assertRaises(BadRequest):
            self.convo_management_service.delete_conversation_role()
        self.convo_management_service.delete_conversation_role(convo_role_id)

        with self.assertRaises(NotFound) as cm:
            self.convo_management_service.read_conversation_role(convo_role_id)
        self.assertIn("does not exist", cm.exception.message)

        with self.assertRaises(NotFound) as cm:
            self.convo_management_service.delete_conversation_role(convo_role_id)
        self.assertIn("does not exist", cm.exception.message)

    def test_conversation_type_role_binding(self):

        convo_type_obj = IonObject('ConversationType', name='RPC', definition=self.rpc_scribble_protocol)
        convo_type_id = self.convo_management_service.create_conversation_type(convo_type_obj)
        self.assertNotEqual(convo_type_id, None)

        convo_role_obj = IonObject("ConversationRole", {"name": "provider"})
        convo_role_id = self.convo_management_service.create_conversation_role(convo_role_obj)
        self.assertNotEqual(convo_role_id, None)

        self.convo_management_service.bind_conversation_type_to_role(convo_type_id, convo_role_id)

        convo_role_obj = IonObject("ConversationRole", {"name": "requester"})
        convo_role_id = self.convo_management_service.create_conversation_role(convo_role_obj)
        self.assertNotEqual(convo_role_id, None)

        with self.assertRaises(BadRequest):
            self.convo_management_service.bind_conversation_type_to_role()
        self.convo_management_service.bind_conversation_type_to_role(convo_type_id, convo_role_id)

        assoc,_ = self.resource_registry.find_objects(convo_type_id, PRED.hasRole, RT.ConversationRole)
        self.assertEqual(len(assoc),2)

        with self.assertRaises(BadRequest):
            self.convo_management_service.unbind_conversation_type_to_role()
        self.convo_management_service.unbind_conversation_type_to_role(convo_type_id, convo_role_id)

        assoc,_ = self.resource_registry.find_objects(convo_type_id, PRED.hasRole, RT.ConversationRole)
        self.assertEqual(len(assoc),1)
