#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

import unittest
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
from pyon.public import AT, RT
from ion.services.coi.policy_management_service import PolicyManagementService
from interface.services.coi.iresource_registry_service import BaseResourceRegistryService

@attr('UNIT', group='coi')
class TestPolicyManagementService(PyonTestCase):

    def setUp(self):
        mock_clients = self._create_service_mock('policy_management')

        self.policy_management_service = PolicyManagementService()
        self.policy_management_service.clients = mock_clients

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

        # Policy
        self.policy = Mock()
        self.policy.name = "Foo"


        # UserRole
        self.user_role = Mock()
        self.user_role.name = 'COI_Test_Administrator'


    def test_create_policy(self):
        self.mock_create.return_value = ['111', 1]

        policy_id = self.policy_management_service.create_policy(self.policy)

        assert policy_id == '111'
        self.mock_create.assert_called_once_with(self.policy)

    def test_read_and_update_policy(self):
        self.mock_read.return_value = self.policy

        policy = self.policy_management_service.read_policy('111')

        assert policy is self.mock_read.return_value
        self.mock_read.assert_called_once_with('111', '')

        policy.name = 'Bar'

        self.mock_update.return_value = ['111', 2]

        self.policy_management_service.update_policy(policy)

        self.mock_update.assert_called_once_with(policy)

    def test_delete_policy(self):
        self.mock_read.return_value = self.policy

        self.policy_management_service.delete_policy('111')

        self.mock_read.assert_called_once_with('111', '')
        self.mock_delete.assert_called_once_with(self.policy)

    def test_read_policy_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.policy_management_service.read_policy('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Policy bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')

    def test_delete_policy_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.policy_management_service.delete_policy('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Policy bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')


    def test_create_user_role(self):
        self.mock_create.return_value = ['123', 1]

        role_id = self.policy_management_service.create_role(self.user_role)

        assert role_id == '123'
        self.mock_create.assert_called_once_with(self.user_role)

    def test_read_and_update_user_role(self):
        self.mock_read.return_value = self.user_role

        user_role = self.policy_management_service.read_role('123')

        assert user_role is self.mock_read.return_value
        self.mock_read.assert_called_once_with('123', '')

        user_role.name = 'New_test_Admin'

        self.mock_update.return_value = ['123', 2]

        self.policy_management_service.update_role(user_role)

        self.mock_update.assert_called_once_with(user_role)

    def test_delete_user_role(self):
        self.mock_read.return_value = self.user_role

        self.policy_management_service.delete_role('123')

        self.mock_read.assert_called_once_with('123', '')
        self.mock_delete.assert_called_once_with(self.user_role)

    def test_read_user_role_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.policy_management_service.read_role('bad role')

        ex = cm.exception
        self.assertEqual(ex.message, 'Role bad role does not exist')
        self.mock_read.assert_called_once_with('bad role', '')

    def test_delete_user_role_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.policy_management_service.delete_role('bad role')

        ex = cm.exception
        self.assertEqual(ex.message, 'Role bad role does not exist')
        self.mock_read.assert_called_once_with('bad role', '')
