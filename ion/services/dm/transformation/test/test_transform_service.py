'''
@author Luke Campbell
@file ion/services/dm/transformation/test_transform_service.py
@description Unit Test for Transform Management Service
'''
from pyon.core.exception import NotFound

from pyon.util.unit_test import PyonTestCase
from mock import Mock
from nose.plugins.attrib import attr
from ion.services.dm.transformation.transform_management_service import TransformManagementService
import unittest

@attr('UNIT',group='dm')
class TransformManagementServiceTest(PyonTestCase):
    """Unit test for TransformManagementService

    """
    def setUp(self):
        mock_clients = self._create_service_mock('transform_management_service')
        self.transform_service = TransformManagementService()
        self.transform_service.clients = mock_clients
        # CRUD Shortcuts
        self.mock_create = self.transform_service.clients.resource_registry.create
        self.mock_read = self.transform_service.clients.resource_registry.read
        self.mock_update = self.transform_service.clients.resource_registry.update
        self.mock_delete = self.transform_service.clients.resource_registry.delete

    def test_create_transform(self):
        self.mock_create.return_value = ['transform_id',1]

        transform_id = self.transform_service.create_transform(transform={})
        self.mock_create.assert_called_with({})
        self.assertEquals(transform_id,'transform_id','create_transform does not pass the id correctly to return.')


    def test_update_transform(self):
        self.mock_update.return_value = ['transform_id',2]

        transform_id = self.transform_service.update_transform(transform={})
        self.mock_update.assert_called_with({})
        self.assertEquals(transform_id,'transform_id','update_transform does not correctly update the object.')

    def test_read_transform(self):
        self.mock_read.return_value = 'TransformObject'

        ret = self.transform_service.read_transform(transform_id='transform_id')
        self.mock_read.assert_called_with('transform_id','')
        self.assertEquals('TransformObject',ret)

        def side_effect(*args, **kwargs):
           raise NotFound

        self.mock_read.side_effect = side_effect

        # Ensure that read_transform throws NotFound when the transform doesn't exist

        with self.assertRaises(NotFound):
           self.transform_service.read_transform(transform_id='not_here')


    def test_delete_transform(self):
        self.mock_read.return_value = 'transform_object'
        self.transform_service.delete_transform(transform_id="transform_id")
        self.mock_delete.assert_called_with('transform_object')
        def side_effect(*args,**kwargs):
           raise NotFound
        self.mock_read.side_effect = side_effect
        with self.assertRaises(NotFound):
           self.transform_service.delete_transform(transform_id='blank')
            
    @unittest.skip('bind_transform not implemented yet')
    def test_bind_transform(self):
        pass

    @unittest.skip('schedule_transform not implemented yet')
    def test_schedule_transform(self):
        pass
            
