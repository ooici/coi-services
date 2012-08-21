#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

import unittest
from mock import Mock
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from pyon.core.exception import BadRequest,  NotFound
from pyon.public import RT, IonObject
from interface.services.coi.iservice_management_service import ServiceManagementServiceClient
from ion.services.coi.service_management_service import ServiceManagementService

@attr('UNIT', group='coi')
class TestServiceManagementServiceUnit(PyonTestCase):

    def setUp(self):
        self.mock_clients = self._create_service_mock('service_management')
        self.sms = ServiceManagementService()
        self.sms.clients = self.mock_clients
        self.yaml_definition = '''
name: datastore_testing
docstring:  Service used to create, read, update and delete persistent Objects
dependencies: []
methods:
  create_datastore:
    docstring: Create a new datastore namespace.
    in:
      datastore_name: ""
    out:
      success: True
'''
        self.bad_yaml ='''
methods:
  create_datastore:
    docstring: Create a new datastore namespace.
   in:
      datastore_name: ""
    out:
      success: True

    def test_create_resource(self):

'''
        service_definition = Mock()

        # Test bad YAML definition
        service_definition.name = "name"
        service_definition.definition = self.bad_yaml
        with self.assertRaises(BadRequest):
            self.sms.create_service_definition(service_definition)

        # Test bad name
        service_definition.definition = self.yaml_definition
        service_definition.name = "name name"
        with self.assertRaises(BadRequest):
            self.sms.create_service_definition(service_definition)

        service_definition.name = "name"
        self.mock_clients.resource_registry.create.return_value = ['123', 1]
        service_id = self.sms.create_service_definition(service_definition)
        self.assertEqual(service_id, '123')
        self.mock_clients.resource_registry.create.assert_called_once_with(service_definition)

    def test_read_and_update_resource(self):
        with self.assertRaises(BadRequest):
            self.sms.read_service_definition()

        service_definition = Mock()
        service_definition.name = "name"

        service_definition.definition = self.yaml_definition
        service_definition.description = "This is just a test, don't panic"
        self.mock_clients.resource_registry.read.return_value = service_definition

        sd = self.sms.read_service_definition("123")
        self.assertTrue(sd is service_definition)
        self.mock_clients.resource_registry.read.assert_called_once_with('123', '')

        sd.name = "new    name"
        with self.assertRaises(BadRequest):
            self.sms.update_service_definition(sd)

        sd.definition = self.bad_yaml
        with self.assertRaises(BadRequest):
            self.sms.update_service_definition(service_definition)

        sd.name = "new_name"
        sd.definition = self.yaml_definition
        self.mock_clients.resource_registry.update.return_value = ['123', 2]
        sd_id = self.sms.update_service_definition(sd)
        self.assertEqual(sd_id, '123')
        self.mock_clients.resource_registry.update.assert_called_once_with(sd)

    def test_read_not_found(self):
        self.mock_clients.resource_registry.read.side_effect = NotFound
        with self.assertRaises(NotFound):
            self.sms.read_service_definition("123")
        self.mock_clients.resource_registry.read.assert_called_once_with("123", '')

    def test_delete_resource(self):
        with self.assertRaises(BadRequest):
            self.sms.delete_service_definition()

        self.mock_clients.resource_registry.delete.return_value = True
        status = self.sms.delete_service_definition("123")
        self.assertEqual(status, True)
        self.mock_clients.resource_registry.delete.assert_called_once_with("123")

    def test_delete_not_found(self):
        self.mock_clients.resource_registry.delete.side_effect = NotFound
        with self.assertRaises(NotFound):
            self.sms.delete_service_definition("123")
        self.mock_clients.resource_registry.delete.assert_called_once_with("123")


@attr('INT', group='coi')
class TestServiceManagementService(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.sms = ServiceManagementServiceClient()

    def test_create_and_delete_service(self):
        service_definition = '''
name: datastore_testing
docstring:  Service used to create, read, update and delete persistent Objects
dependencies: []
methods:
  create_datastore:
    docstring: Create a new datastore namespace.
    in:
      datastore_name: ""
    out:
      success: True
'''
        # Create ServiceDefinition
        sd = IonObject(RT.ServiceDefinition, {"definition": service_definition})
        service_id = self.sms.create_service_definition(sd)
        self.assertTrue(type(service_id) == str)

        # Delete
        self.sms.delete_service_definition(service_id)

        # Validate it has been deleted
        with self.assertRaises(NotFound):
            self.sms.delete_service_definition(service_id)
        with self.assertRaises(NotFound):
            self.sms.read_service_definition(service_id)


    def test_read_and_update_service(self):
        service_definition = '''
name: datastore_testing2
docstring:  Service used to create, read, update and delete persistent Objects
dependencies: []
methods:
  create_datastore:
    docstring: Create a new datastore namespace.
    in:
      datastore_name: ""
    out:
      success: True
'''
        # Create ServiceDefinition
        sd = IonObject(RT.ServiceDefinition, {"definition": service_definition})
        service_id = self.sms.create_service_definition(sd)
        self.assertTrue(type(service_id) == str)

        # Read ServiceDefinition and validate
        service = self.sms.read_service_definition(service_id)
        self.assertEqual(service.definition, service_definition)

        #Update ServiceDefinition
        service_definition2 = '''
name: datastore_testing2
docstring:  Service used to create, read, update and delete persistent Objects
dependencies: []
methods:
  create_datastore:
    docstring: Create a new datastore namespace.
    in:
      datastore_id: 0
    out:
      success: True
'''
        service.definition = service_definition2
        self.sms.update_service_definition(service)

        # Read back and validate the update
        service2 = self.sms.read_service_definition(service_id)
        self.assertEqual(service2.definition, service_definition2)

        # Cleanup
        self.sms.delete_service_definition(service_id)

    def test_read_service_not_found(self):
        service_id = "ddsxxd3_deee"
        with self.assertRaises(NotFound):
            self.sms.read_service_definition(service_id)

    def test_delete_service_not_found(self):
        service_id = "ddsdxxd3_deee"
        with self.assertRaises(NotFound):
            self.sms.delete_service_definition(service_id)

