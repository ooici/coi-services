#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

import unittest
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
from pyon.public import AT, RT
from ion.services.coi.exchange_management_service import ExchangeManagementService


@attr('UNIT', group='coi')
class TestExchangeManagementService(PyonTestCase):

    def setUp(self):
        mock_clients = self._create_service_mock('exchange_management')

        self.exchange_management_service = ExchangeManagementService()
        self.exchange_management_service.clients = mock_clients

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
        self.exchange_space = Mock()
        self.exchange_space.name = "Foo"


    def test_create_exchange_space(self):
        self.mock_create.return_value = ['111', 1]

        #TODO - Need to mock an org to pass in an valid Org_id

        exchange_space_id = self.exchange_management_service.create_exchange_space(self.exchange_space, "1233")

        assert exchange_space_id == '111'
        self.mock_create.assert_called_once_with(self.exchange_space)

    def test_read_and_update_exchange_space(self):
        self.mock_read.return_value = self.exchange_space

        exchange_space = self.exchange_management_service.read_exchange_space('111')

        assert exchange_space is self.mock_read.return_value
        self.mock_read.assert_called_once_with('111', '')

        exchange_space.name = 'Bar'

        self.mock_update.return_value = ['111', 2]

        self.exchange_management_service.update_exchange_space(exchange_space)

        self.mock_update.assert_called_once_with(exchange_space)

    def test_delete_exchange_space(self):
        self.mock_read.return_value = self.exchange_space

        self.exchange_management_service.delete_exchange_space('111')

        self.mock_read.assert_called_once_with('111', '')
        self.mock_delete.assert_called_once_with(self.exchange_space)

    def test_read_exchange_space_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.exchange_management_service.read_exchange_space('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Exchange Space bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')

    def test_delete_exchange_space_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.exchange_management_service.delete_exchange_space('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Exchange Space bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')


