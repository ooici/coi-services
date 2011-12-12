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
from pyon.core.exception import NotFound
import unittest

@attr('UNIT', group='mmm')
class TestDataAcquisitionManagement(PyonTestCase):

    def setUp(self):
        self.mock_ionobj = self._create_IonObject_mock('ion.services.sa.data_acquisition_management.data_acquisition_management_service.IonObject')
        self._create_service_mock('data_acquisition_management')

        self.data_acquisition_mgmt_service = DataAcquisitionManagementService()
        self.data_acquisition_mgmt_service.clients = self.clients

        # save some typing
        self.mock_create = self.resource_registry.create
        self.mock_update = self.resource_registry.update
        self.mock_delete = self.resource_registry.delete

    def test_create_data_source(self):
        self.mock_create.return_value = ('id_2', 'I do not care')
        source = {"type": "", "connection_params":{'p1':'one'}}

        stream_id = self.data_acquisition_mgmt_service.create_data_source(source)

        self.mock_ionobj.assert_called_once_with('DataSource', source)
        self.mock_create.assert_called_once_with(self.mock_ionobj.return_value)
        self.assertEqual(stream_id, 'id_2')
  