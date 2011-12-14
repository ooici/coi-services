#!/usr/bin/env python

'''
@file ion/services/sa/data_acquisition_management/test/test_data_acquisition_management_service.py
@author Maurice Manning
@test ion.services.sa.data_acquisition_management.data_acquisition_management_service Unit test suite to cover all service code
'''

from mock import Mock, sentinel, patch
from pyon.util.unit_test import PyonTestCase
from ion.services.sa.data_acquisition_management.data_acquisition_management_service import DataAcquisitionManagementService
from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
import unittest

@attr('UNIT', group='mmm')
class TestDataAcquisitionManagement(PyonTestCase):

    def setUp(self):
        self.mock_ionobj = self._create_IonObject_mock('ion.services.sa.data_acquisition_management.data_acquisition_management_service.IonObject')
        mock_clients = self._create_service_mock('data_acquisition_management')

        self.data_acquisition_mgmt_service = DataAcquisitionManagementService()
        self.data_acquisition_mgmt_service.clients = mock_clients

        # save some typing
        self.mock_create = mock_clients.resource_registry.create
        self.mock_update = mock_clients.resource_registry.update
        self.mock_delete = mock_clients.resource_registry.delete
        self.mock_read = mock_clients.resource_registry.read

        self.data_source = Mock()
        self.data_source.name = 'dsname'
        self.data_source.type = 'foo'
        self.data_source.connection_params = {'param1':'111'}

        self.data_source = {"type": "foo", "connection_params": {}}

    def test_create_data_source(self):
        self.mock_create.return_value = ('111', 'bla')

        data_source_id = self.data_acquisition_mgmt_service.create_data_source(self.data_source)

        #self.mock_ionobj.assert_called_once_with('DataSource', self.ds)
        self.mock_create.assert_called_once_with(self.data_source)
        self.assertEqual(data_source_id, '111')



    def test_read_and_update_data_source(self):
        self.mock_read.return_value = self.data_source

        dsrc = self.data_acquisition_mgmt_service.read_data_source('111')

        assert dsrc is self.mock_read.return_value
        self.mock_read.assert_called_once_with('111', '')

        #dsrc.type = 'Bar'

        #self.mock_update.return_value = ['111', 2]

        #self.data_acquisition_mgmt_service.update_data_source(dsrc)

        #self.mock_update.assert_called_once_with(dsrc)
