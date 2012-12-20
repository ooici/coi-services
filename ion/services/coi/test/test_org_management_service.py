#!/usr/bin/env python


__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

import unittest
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
from pyon.public import PRED, RT, IonObject
from ion.services.coi.org_management_service import OrgManagementService
from interface.services.coi.iorg_management_service import OrgManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.core.governance.governance_controller import ORG_MANAGER_ROLE



@attr('UNIT', group='coi')
class TestOrgManagementService(PyonTestCase):

    def setUp(self):
        mock_clients = self._create_service_mock('org_management')

        self.org_management_service = OrgManagementService()
        self.org_management_service.clients = mock_clients

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

        # Exchange Space
        self.org = Mock()
        self.org.name = "Foo"

        self.user_role = Mock()
        self.user_role2 = Mock()

    @patch('pyon.ion.directory.Directory.__init__', Mock(return_value=None))
    def test_create_org(self):

        self.mock_find_objects.return_value = ([self.user_role], [self.user_role2])

        self.mock_create.return_value = ['111', 1]

        org_id = self.org_management_service.create_org(self.org)

        assert org_id == '111'
        self.mock_create.assert_called_once_with(self.org)

    def test_read_and_update_org(self):
        self.mock_read.return_value = self.org

        org = self.org_management_service.read_org('111')

        assert org is self.mock_read.return_value
        self.mock_read.assert_called_once_with('111', '')

        org.name = 'Bar'

        self.mock_update.return_value = ['111', 2]

        self.org_management_service.update_org(org)

        self.mock_update.assert_called_once_with(org)

    def test_delete_org(self):
        self.org_management_service.delete_org('111')

        self.mock_delete.assert_called_once_with('111')

    def test_read_org_not_found(self):
        self.mock_read.side_effect = NotFound('Org bad does not exist')

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.org_management_service.read_org('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Org bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')

    def test_delete_org_not_found(self):
        self.mock_delete.side_effect = NotFound('Org bad does not exist')

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.org_management_service.delete_org('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Org bad does not exist')
        self.mock_delete.assert_called_once_with('bad')


@attr('INT', group='coi')
class TestOrgManagementServiceInt(IonIntegrationTestCase):

    def setUp(self):

        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2coi.yml')

        self.resource_registry = ResourceRegistryServiceClient(node=self.container.node)
        self.org_management_service = OrgManagementServiceClient(node=self.container.node)


    def test_org_crud(self):

        with self.assertRaises(BadRequest) as br:
            self.org_management_service.create_org(IonObject("Org", {"name": "Test Facility"}))
        self.assertTrue("can only contain alphanumeric and underscore characters" in br.exception.message)

        with self.assertRaises(BadRequest):
            self.org_management_service.create_org()

        org_obj = IonObject("Org", {"name": "TestFacility"})
        org_id = self.org_management_service.create_org(org_obj)
        self.assertNotEqual(org_id, None)

        org = None
        org = self.org_management_service.read_org(org_id)
        self.assertNotEqual(org, None)

        #Check that the roles got associated to them
        role_list = self.org_management_service.find_org_roles(org_id)
        self.assertEqual(len(role_list),2 )

        with self.assertRaises(BadRequest):
            self.org_management_service.update_org()
        org.name = 'Updated_TestFacility'
        self.org_management_service.update_org(org)

        org = None
        org = self.org_management_service.read_org(org_id)
        self.assertNotEqual(org, None)
        self.assertEqual(org.name, 'Updated_TestFacility')

        user_role = self.org_management_service.find_org_role_by_name(org_id, ORG_MANAGER_ROLE)
        self.assertNotEqual(user_role, None)

        #find_org = self.org_management_service.remove_user_role(org_id, ORG_MANAGER_ROLE)
        #self.assertEqual(find_org, True)

        with self.assertRaises(BadRequest):
            self.org_management_service.delete_org()
        self.org_management_service.delete_org(org_id)

        with self.assertRaises(NotFound) as cm:
            self.org_management_service.read_org(org_id)
        self.assertIn("does not exist", cm.exception.message)

        with self.assertRaises(NotFound) as cm:
            self.org_management_service.delete_org(org_id)
        self.assertIn("does not exist", cm.exception.message)


    def test_org_affiliation(self):

        root_org = None
        root_org = self.org_management_service.find_org()
        self.assertNotEqual(root_org, None)

        org_obj = IonObject("Org", {"name": "TestFacility"})
        org_id = self.org_management_service.create_org(org_obj)
        self.assertNotEqual(org_id, None)

        ret = self.org_management_service.affiliate_org(root_org._id, org_id)
        self.assertTrue(ret)

        ret = self.org_management_service.unaffiliate_org(root_org._id, org_id)
        self.assertTrue(ret)

    def test_find_org_containers(self):

        root_org = None
        root_org = self.org_management_service.find_org()
        self.assertNotEqual(root_org, None)

        containers = self.org_management_service.find_org_containers(root_org._id)

        all_containers,_ = self.resource_registry.find_resources(restype=RT.CapabilityContainer, id_only=True)

        self.assertEqual(len(containers),len(all_containers))