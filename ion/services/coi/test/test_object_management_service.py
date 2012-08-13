#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
from pyon.public import PRED, RT, IonObject, OT
from mock import Mock
from interface.services.coi.iobject_management_service import ObjectManagementServiceClient

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
        object_type_id = "xx225566666d33"
        with self.assertRaises(NotFound):
            self.oms.read_object_type(object_type_id)

    def test_delete_object_not_found(self):
        object_type_id = "xx225566666d333"
        with self.assertRaises(NotFound):
            self.oms.delete_object_type(object_type_id)


