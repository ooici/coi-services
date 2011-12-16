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
from pyon.public import AT, RT

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
        self.mock_create_association = mock_clients.resource_registry.create_association

        self.data_source = Mock()
        self.data_source.name = 'dsname'
        self.data_source.type = 'foo'
        self.data_source.description = 'data source desc'
        self.data_source.connection_params = {'param1':'111'}

        self.data_producer = Mock()
        self.data_producer.name = 'dproducer'
        self.data_producer.type = 'instrument'
        self.data_producer.stream_id = '123'

        self.data_process = Mock()
        self.data_process.name = 'dprocess'
        self.data_process.description = 'process desc'

        self.instrument = Mock()
        self.instrument.name = 'inst'
        self.instrument.description = 'inst desc'

    ##############################################################################################
    #
    #  DataSource
    #
    ##############################################################################################


    def test_create_data_source(self):
        self.mock_create.return_value = ('111', 'bla')

        data_source_id = self.data_acquisition_mgmt_service.create_data_source(self.data_source)

        self.mock_create.assert_called_once_with(self.data_source)
        self.assertEqual(data_source_id, '111')

    def test_read_and_update_data_source(self):
        self.mock_read.return_value = self.data_source

        dsrc = self.data_acquisition_mgmt_service.read_data_source('111')

        assert dsrc is self.mock_read.return_value
        self.mock_read.assert_called_once_with('111', '')

        dsrc.type = 'Bar'
        self.mock_update.return_value = ['111', 2]

        self.data_acquisition_mgmt_service.update_data_source(dsrc)
        self.mock_update.assert_called_once_with(dsrc)


    def test_delete_data_source(self):
        self.mock_read.return_value = self.data_source

        self.data_acquisition_mgmt_service.delete_data_source('111')

        self.mock_read.assert_called_once_with('111', '')
        self.mock_delete.assert_called_once_with(self.data_source)

    def test_read_data_source_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.data_acquisition_mgmt_service.read_data_source('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'DataSource bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')

    def test_delete_data_source_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.data_acquisition_mgmt_service.delete_data_source('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'DataSource bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')



  ##############################################################################################
    #
    #  DataProducer
    #
    ##############################################################################################


    def test_create_data_producer(self):
        self.mock_create.return_value = ('111', 'bla')

        data_prod_id = self.data_acquisition_mgmt_service.create_data_producer(self.data_producer)

        self.mock_create.assert_called_once_with(self.data_producer)
        self.assertEqual(data_prod_id, '111')

    def test_read_and_update_data_producer(self):
        self.mock_read.return_value = self.data_producer

        dp = self.data_acquisition_mgmt_service.read_data_producer('111')

        assert dp is self.mock_read.return_value
        self.mock_read.assert_called_once_with('111', '')

        dp.type = 'transform'
        self.mock_update.return_value = ['111', 2]

        self.data_acquisition_mgmt_service.update_data_producer(dp)
        self.mock_update.assert_called_once_with(dp)


    def test_delete_data_producer(self):
        self.mock_read.return_value = self.data_producer

        self.data_acquisition_mgmt_service.delete_data_producer('111')

        self.mock_read.assert_called_once_with('111', '')
        self.mock_delete.assert_called_once_with(self.data_producer)

    def test_read_data_producer_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.data_acquisition_mgmt_service.read_data_producer('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Data producer bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')

    def test_delete_data_producer_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.data_acquisition_mgmt_service.delete_data_producer('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Data producer bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')


    def test_register_process(self):
        self.mock_read.return_value = self.data_process
        self.mock_create.return_value = ('111', 'bla')
        self.mock_create.return_value = ('222', 'bla')
        self.mock_create_association.return_value = ['333', 1]

        self.data_acquisition_mgmt_service.register_process('111')

        self.mock_read.assert_called_once_with('111', '')
        self.mock_create_association.assert_called_once_with('111', AT.hasDataProducer, '222', None)

    def test_register_process_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.data_acquisition_mgmt_service.register_process('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Data Process bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')

    def test_register_data_source(self):
        self.mock_read.return_value = self.data_source
        self.mock_create.return_value = ('111', 'bla')
        self.mock_create.return_value = ('222', 'bla')
        self.mock_create_association.return_value = ['333', 1]

        self.data_acquisition_mgmt_service.register_data_source('111')

        self.mock_read.assert_called_once_with('111', '')
        self.mock_create_association.assert_called_once_with('111', AT.hasDataProducer, '222', None)

    def test_register_data_source_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.data_acquisition_mgmt_service.register_data_source('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Data Source bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')

    def test_register_instrument(self):
        self.mock_read.return_value = self.instrument
        self.mock_create.return_value = ('111', 'bla')
        self.mock_create.return_value = ('222', 'bla')
        self.mock_create_association.return_value = ['333', 1]

        self.data_acquisition_mgmt_service.register_instrument('111')

        self.mock_read.assert_called_once_with('111', '')
        self.mock_create_association.assert_called_once_with('111', AT.hasDataProducer, '222', None)


    def test_register_instrument_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.data_acquisition_mgmt_service.register_instrument('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Instrument bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')