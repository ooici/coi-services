#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
from pyon.public import PRED, RT, IonObject, OT
from interface.services.coi.iresource_management_service import ResourceManagementServiceClient
from interface.services.coi.iobject_management_service import ObjectManagementServiceClient
from mock import Mock, patch

@attr('INT', group='coi')
class TestResourceManagementService(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.rms = ResourceManagementServiceClient()
        self.oms = ObjectManagementServiceClient()

    def test_create_and_delete_resource(self):
        object_definition = '''
Policy2: !Extends_InformationResource
  enabled: True
  definition: {}
'''
        # Create ObjectType
        ot = IonObject(RT.ObjectType, {"definition": object_definition})
        object_id = self.oms.create_object_type(ot)
        self.assertTrue(type(object_id) == str)

        # Create ResourceType and create association
        rt = IonObject(RT.ResourceType)
        with self.assertRaises(BadRequest):
            self.rms.create_resource_type(rt,'')
        resource_id = self.rms.create_resource_type(rt, object_id)
        self.assertTrue(type(resource_id) == str)

        # Cleanup by deleting the ObjectType and ResourceType
        self.oms.delete_object_type(object_id)
        self.rms.delete_resource_type(resource_id)

    def test_read_resource(self):
        object_definition = '''
Policy3: !Extends_InformationResource
  enabled: True
  definition: {}
'''
        # Create ObjectType
        ot = IonObject(RT.ObjectType, {"definition": object_definition})
        object_id = self.oms.create_object_type(ot)
        self.assertTrue(type(object_id) == str)

        # Create ResourceType and create association
        rt = IonObject(RT.ResourceType)
        resource_id = self.rms.create_resource_type(rt, object_id)
        self.assertTrue(type(resource_id) == str)

        # Read resource
        resource_type = self.rms.read_resource_type(resource_id)
        self.assertTrue(resource_type)
        with self.assertRaises(BadRequest):
            self.rms.read_resource_type("")

        # Cleanup by deleting the ObjectType and the ResourceType
        self.oms.delete_object_type(object_id)
        self.rms.delete_resource_type(resource_id)

    def test_read_resource_not_found(self):
        object_definition = '''
Policy4: !Extends_InformationResource
  enabled: True
  definition: {}
'''
        # Create ObjectType
        ot = IonObject(RT.ObjectType, {"definition": object_definition})
        object_id = self.oms.create_object_type(ot)
        self.assertTrue(type(object_id) == str)

        # Create ResourceType and create association
        rt = IonObject(RT.ResourceType)
        resource_id = self.rms.create_resource_type(rt, object_id)
        self.assertTrue(type(resource_id) == str)

        # Read resource
        resource_type = self.rms.read_resource_type(resource_id)
        self.assertTrue(resource_type)

        # Delete
        self.oms.delete_object_type(object_id)
        self.rms.delete_resource_type(resource_id)

        # Read ResourceType that has been deleted
        with self.assertRaises(NotFound):
            self.rms.read_resource_type(resource_id)

    def test_delete_resource_not_found(self):
        object_definition = '''
Policy5: !Extends_InformationResource
  enabled: True
  definition: {}
'''
        # Create ObjectType
        ot = IonObject(RT.ObjectType, {"definition": object_definition})
        object_id = self.oms.create_object_type(ot)
        self.assertTrue(type(object_id) == str)

        # Create ResourceType and create association
        rt = IonObject(RT.ResourceType)
        resource_id = self.rms.create_resource_type(rt, object_id)
        self.assertTrue(type(resource_id) == str)

        # Read resource
        resource_type = self.rms.read_resource_type(resource_id)
        self.assertTrue(resource_type)

        # Delete
        self.oms.delete_object_type(object_id)
        self.rms.delete_resource_type(resource_id)

        # Delete a ResourceType that has already been deleted
        with self.assertRaises(NotFound):
            self.rms.delete_resource_type(resource_id)

