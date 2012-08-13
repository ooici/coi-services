#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from pyon.core.exception import BadRequest,  NotFound
from pyon.public import RT, IonObject
from interface.services.coi.iservice_management_service import ServiceManagementServiceClient
from mock import Mock, patch

@attr('semantest', group='coi')
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

