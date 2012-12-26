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
from ion.services.coi.policy_management_service import PolicyManagementService
from interface.services.coi.ipolicy_management_service import PolicyManagementServiceClient

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
        self.policy.description ="This is a test policy"
        self.policy.policy_type.policy_rule = '<Rule id="%s"> <description>%s</description></Rule>'

        # UserRole
        self.user_role = Mock()
        self.user_role.name = 'COI_Test_Administrator'

        # Resource
        self.resource = Mock()
        self.resource._id = '123'
        self.resource.name = "Foo"

        # Policy to Resource association
        self.policy_to_resource_association = Mock()
        self.policy_to_resource_association._id = '555'
        self.policy_to_resource_association.s = "123"
        self.policy_to_resource_association.st = RT.Resource
        self.policy_to_resource_association.p = PRED.hasPolicy
        self.policy_to_resource_association.o = "111"
        self.policy_to_resource_association.ot = RT.Policy

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

        self.mock_find_subjects.return_value = ([self.resource], [self.policy_to_resource_association])

        self.policy_management_service.delete_policy('111')

        self.mock_delete.assert_called_once_with('111')

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
        self.mock_find_subjects.return_value = ([], [])

        self.policy_management_service.delete_role('123')

        self.mock_delete.assert_called_once_with('123')

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


@attr('INT', group='coi')
class TestPolicyManagementServiceInt(IonIntegrationTestCase):

    def setUp(self):

        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2coi.yml')

        self.policy_management_service = PolicyManagementServiceClient(node=self.container.node)

    def test_policy_crud(self):

        res_policy_obj = IonObject(OT.ResourceAccessPolicy, policy_rule='<Rule id="%s"> <description>%s</description></Rule>')

        policy_obj = IonObject(RT.Policy, name='Test_Policy',
            description='This is a test policy',
            policy_type=res_policy_obj)

        policy_obj.name = ' '
        with self.assertRaises(BadRequest):
            self.policy_management_service.create_policy(policy_obj)

        policy_obj.name = 'Test_Policy'
        policy_id = self.policy_management_service.create_policy(policy_obj)
        self.assertNotEqual(policy_id, None)

        with self.assertRaises(BadRequest):
            self.policy_management_service.read_policy()
        policy = None
        policy = self.policy_management_service.read_policy(policy_id)
        self.assertNotEqual(policy, None)

        policy.name = ' '
        with self.assertRaises(BadRequest):
            self.policy_management_service.update_policy(policy)
        policy.name = 'Updated_Test_Policy'
        self.policy_management_service.update_policy(policy)

        policy = None
        policy = self.policy_management_service.read_policy(policy_id)
        self.assertNotEqual(policy, None)
        self.assertEqual(policy.name, 'Updated_Test_Policy')

        with self.assertRaises(BadRequest):
            self.policy_management_service.create_resource_access_policy()
        with self.assertRaises(BadRequest):
            self.policy_management_service.create_resource_access_policy(policy_id)
        with self.assertRaises(BadRequest):
            self.policy_management_service.create_resource_access_policy(policy_id, policy.name)
        with self.assertRaises(BadRequest):
            self.policy_management_service.create_resource_access_policy(policy_id, policy.name, "description")
        #p_id =  self.policy_management_service.create_resource_access_policy(policy_id, "Resource_access_name", "Policy Description", "Test_Rule")
        #self.assertNotEqual(p_id, None)

        with self.assertRaises(BadRequest):
            self.policy_management_service.create_service_access_policy()
        with self.assertRaises(BadRequest):
            self.policy_management_service.create_service_access_policy(service_name="service_name")
        with self.assertRaises(BadRequest):
            self.policy_management_service.create_service_access_policy(service_name="service_name", policy_name="policy_name")
        with self.assertRaises(BadRequest):
            self.policy_management_service.create_service_access_policy(service_name="service_name", policy_name="policy_name", description="description")
        #p_obj = self.policy_management_service.create_service_access_policy("service_name", "policy_name", "description", "policy_rule")
        #self.assertNotEqual(p_obj, None)

        with self.assertRaises(BadRequest):
            self.policy_management_service.create_common_service_access_policy()
        with self.assertRaises(BadRequest):
            self.policy_management_service.create_common_service_access_policy(policy_name="policy_name")
        with self.assertRaises(BadRequest):
            self.policy_management_service.create_common_service_access_policy(policy_name="policy_name",description="description")
        #p_id = self.policy_management_service.create_common_service_access_policy(policy_name="policy_name",description="description", policy_rule="test_rule")
        #self.assertNotEqual(p_id, None)

        with self.assertRaises(BadRequest):
            self.policy_management_service.add_process_operation_precondition_policy()
        with self.assertRaises(BadRequest):
            self.policy_management_service.add_process_operation_precondition_policy(process_name="process_name")
        with self.assertRaises(BadRequest):
            self.policy_management_service.add_process_operation_precondition_policy(process_name="process_name", op="op")

        self.policy_management_service.enable_policy(policy_id)
        self.policy_management_service.enable_policy(policy_id)
        with self.assertRaises(BadRequest):
            self.policy_management_service.delete_policy()
        self.policy_management_service.delete_policy(policy_id)

        with self.assertRaises(NotFound) as cm:
            self.policy_management_service.read_policy(policy_id)
        self.assertIn("does not exist", cm.exception.message)

        with self.assertRaises(NotFound) as cm:
            self.policy_management_service.delete_policy(policy_id)
        self.assertIn("does not exist", cm.exception.message)


    def test_role_crud(self):
        user_role_obj = IonObject("UserRole", {"name": "Test_User_Role"})
        user_role_id = self.policy_management_service.create_role(user_role_obj)
        self.assertNotEqual(user_role_id, None)

        user_role = None
        user_role = self.policy_management_service.read_role(user_role_id)
        self.assertNotEqual(user_role, None)

        user_role.name = 'Test_User_Role_2'
        self.policy_management_service.update_role(user_role)

        user_role = None
        user_role = self.policy_management_service.read_role(user_role_id)
        self.assertNotEqual(user_role, None)
        self.assertEqual(user_role.name, 'Test_User_Role_2')

        self.policy_management_service.delete_role(user_role_id)

        with self.assertRaises(NotFound) as cm:
            self.policy_management_service.read_role(user_role_id)
        self.assertIn("does not exist", cm.exception.message)

        with self.assertRaises(NotFound) as cm:
            self.policy_management_service.delete_role(user_role_id)
        self.assertIn("does not exist", cm.exception.message)

