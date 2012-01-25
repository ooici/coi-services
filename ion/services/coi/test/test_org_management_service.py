#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

import unittest
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
from pyon.public import PRED, RT
from ion.services.coi.org_management_service import OrgManagementService


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


    def test_create_org(self):
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


