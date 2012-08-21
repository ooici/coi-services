#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

import unittest
from mock import Mock
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from pyon.core.exception import BadRequest,  NotFound
from pyon.public import  RT, IonObject
from interface.services.coi.iobject_management_service import ObjectManagementServiceClient
from ion.services.coi.object_management_service import ObjectManagementService


@attr('UNIT', group='coi')
class TestObjectManagementServiceUnit(PyonTestCase):

    def setUp(self):
        self.mock_clients = self._create_service_mock('object_management')
        self.oms = ObjectManagementService()
        self.oms.clients = self.mock_clients
        self.yaml_definition = '''
TimerSchedulerEntry2: !Extends_AbstractSchedulerEntry
  # String to put in origin of TimerEvent
  event_origin: ""
  # String to put in subtype field of TimerEvent
  event_subtype: ""
'''
        self.bad_yaml ='''
TimerSchedulerEntry2: !Extends_AbstractSchedulerEntry
  # String to put in origin of TimerEvent
  event_origin ""
  # String to put in subtype field of TimerEvent
  event_subtype: ""
'''

    def rr_return_value(self):
        return ['123',1]

    def test_create_object(self):
        ot = Mock()
        ot.definition = self.bad_yaml
        ot.name = "name"
        with self.assertRaises(BadRequest):
            self.oms.create_object_type(ot)

        ot.name = "bad  name"
        with self.assertRaises(BadRequest):
            self.oms.create_object_type(ot)

        ot.name = "name"
        ot.definition = self.yaml_definition
        self.oms.clients.resource_registry.create.return_value = self.rr_return_value()
        object_id = self.oms.create_object_type(ot)
        self.assertEqual(object_id, '123')
        self.oms.clients.resource_registry.create.assert_called_once_with(ot)

    def test_read_and_update_object(self):
        with self.assertRaises(BadRequest):
            self.oms.read_object_type(None)

        ot = Mock()
        ot.definition = self.yaml_definition
        ot.name = "name"
        ot.description = "This is just a test, don't panic"
        self.oms.clients.resource_registry.read.return_value = ot

        ot_return = self.oms.read_object_type("123")
        self.assertTrue(ot_return is ot)
        self.oms.clients.resource_registry.read.assert_called_once_with('123','')

        ot_return.name = "new    name"
        with self.assertRaises(BadRequest):
            self.oms.update_object_type(ot_return)

        ot_return.name = "new_name"
        ot_return.definition = self.bad_yaml
        with self.assertRaises(BadRequest):
            self.oms.update_object_type(ot_return)

        ot.definition = self.yaml_definition
        self.oms.clients.resource_registry.update.return_value = ['123', 2]
        ot_id = self.oms.update_object_type(ot_return)
        self.assertEqual(ot_id, '123')
        self.oms.clients.resource_registry.update.assert_called_once_with(ot_return)

    def test_read_not_found(self):
        self.oms.clients.resource_registry.read.side_effect = NotFound
        with self.assertRaises(NotFound):
            self.oms.read_object_type("0xBADC0FFEE")
        self.oms.clients.resource_registry.read.assert_called_once_with('0xBADC0FFEE','')

    def test_delete_object(self):
        with self.assertRaises(BadRequest):
            self.oms.delete_object_type(None)

        self.oms.clients.resource_registry.delete.return_value = True
        status = self.oms.delete_object_type("123")
        self.assertEqual(status, True)
        self.oms.clients.resource_registry.delete.assert_called_once_with("123")

    def test_delete_not_found(self):
        self.oms.clients.resource_registry.delete.side_effect = NotFound
        with self.assertRaises(NotFound):
            self.oms.delete_object_type("0xBADC0FFEE")
        self.oms.clients.resource_registry.delete.assert_called_once_with('0xBADC0FFEE')


@attr('INT', group='coi')
class TestObjectManagementService(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.oms = ObjectManagementServiceClient()

    def test_create_object(self):
        yaml_str = '''
TimerSchedulerEntry2: !Extends_AbstractSchedulerEntry
  # String to put in origin of TimerEvent
  event_origin: ""
  # String to put in subtype field of TimerEvent
  event_subtype: ""
'''
        ot = IonObject(RT.ObjectType, {"definition": yaml_str})
        object_type_id = self.oms.create_object_type(ot)
        self.assertTrue(type(object_type_id) == str)
        self.oms.delete_object_type(object_type_id)


    def test_read_and_update_object(self):
        # Create object type
        # Read object type and validate
        # Update object type
        # Read back the object type and validate
        # Delete the object type
        object_definition = '''
TimerSchedulerEntry3: !Extends_AbstractSchedulerEntry
  # String to put in origin of TimerEvent
  event_origin: ""
  # String to put in subtype field of TimerEvent
  event_subtype: ""
'''
        ot = IonObject(RT.ObjectType, {"definition": object_definition})
        object_type_id = self.oms.create_object_type(ot)
        object_type = self.oms.read_object_type(object_type_id)
        self.assertEqual(object_definition,object_type.definition)
        object_definition2 = '''
TimerSchedulerEntry3: !Extends_AbstractSchedulerEntry
  # String to put in origin of TimerEvent
  event_origin: ""
  # String to put in subtype field of TimerEvent
  event_subtype: ""
 '''
        object_type.definition = object_definition2
        self.oms.update_object_type(object_type)
        object_type = self.oms.read_object_type(object_type_id)
        self.assertEqual(object_definition2, object_type.definition)
        self.oms.delete_object_type(object_type_id)

    def test_read_object_not_found(self):
        object_type_id = "0xbadc0ffee"
        with self.assertRaises(NotFound):
            self.oms.read_object_type(object_type_id)

    def test_delete_object_not_found(self):
        object_type_id = "0xbadc0ffee"
        with self.assertRaises(NotFound):
            self.oms.delete_object_type(object_type_id)


