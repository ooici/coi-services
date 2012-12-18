#!/usr/bin/env python

__author__ = 'Stephen P. Henrie, Michael Meisinger'
__license__ = 'Apache 2.0'

from mock import Mock, patch
from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
from pyon.public import PRED, RT, IonObject, OT, log
from pyon.util.context import LocalContextMixin
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase

from ion.services.coi.resource_management_service import ResourceManagementService

from interface.services.coi.iresource_management_service import ResourceManagementServiceClient, ResourceManagementServiceProcessClient
from interface.services.coi.iobject_management_service import ObjectManagementServiceClient


@attr('UNIT', group='coi')
class TestResourceManagementServiceUnit(PyonTestCase):

    def setUp(self):
        mock_clients = self._create_service_mock('resource_management')
        self.rms = ResourceManagementService()
        self.rms.clients = mock_clients
        self.object_definition = '''
Policy9: !Extends_InformationResource
  enabled: True
  definition: {}
'''

    def test_create_resource(self):
        # Create ResourceType
        rt = Mock()
        rt.name = "bad name"
        with self.assertRaises(BadRequest):
            self.rms.create_resource_type(rt, "123")

        rt.name = "good_name"
        bad_object_id = None
        with self.assertRaises(BadRequest):
            self.rms.create_resource_type(rt, bad_object_id)

        # Create ObjectType
        ot = Mock()
        ot.definition = self.object_definition
        ot.name = "good_name"
        ot.description = "This is just a test. No need to panic"

        self.rms.clients.resource_registry.read.return_value = ot
        resource_id_return_value = '123'
        version_return_value = 1
        self.rms.clients.resource_registry.create.return_value = [resource_id_return_value, version_return_value]
        self.rms.clients.resource_registry.create_association.return_value = '999'

        object_id = "444"
        resource_id = self.rms.create_resource_type(rt, object_id)
        self.assertEqual(resource_id, '123')

        self.rms.clients.resource_registry.read.assert_called_once_with(object_id, '')
        self.rms.clients.resource_registry.create.assert_called_once_with(rt)
        self.rms.clients.resource_registry.create_association.assert_called_once_with(resource_id_return_value, PRED.hasObjectType, object_id, 'H2H')

    def test_read_resource(self):
        with self.assertRaises(BadRequest):
            self.rms.read_resource_type(None)
        # Create ResourceType
        rt = Mock()
        rt.name = "good_name"
        self.rms.clients.resource_registry.read.return_value = rt

        rt_read = self.rms.read_resource_type("123")
        self.assertTrue(rt_read is rt)
        self.rms.clients.resource_registry.read.assert_called_once_with('123','')

    def test_read_not_found(self):
        self.rms.clients.resource_registry.read.side_effect = NotFound
        with self.assertRaises(NotFound):
            self.rms.read_resource_type("0xBadC0ffee")
        self.rms.clients.resource_registry.read.assert_called_once_with("0xBadC0ffee", '')

    def test_delete_resource(self):
        with self.assertRaises(BadRequest):
            self.rms.delete_resource_type(None, "123")

        with self.assertRaises(BadRequest):
            self.rms.delete_resource_type("123", None)

        self.rms.clients.resource_registry.delete.return_value = True
        association_id = '999'
        self.rms.clients.resource_registry.get_association.return_value = association_id
        self.rms.clients.resource_registry.delete_association.return_value = True
        resource_id = '123'
        object_id = '456'
        status = self.rms.delete_resource_type(resource_id, object_id)
        self.assertTrue(status, True)
        self.rms.clients.resource_registry.delete.assert_called_once_with(resource_id)
        self.rms.clients.resource_registry.get_association.assert_called_once_with(resource_id, PRED.hasObjectType, object_id, None, False)
        self.rms.clients.resource_registry.delete_association.assert_called_once_with(association_id)

    def test_delete_not_found(self):
        self.rms.clients.resource_registry.get_association.side_effect = NotFound
        resource_id = "123"
        object_id = '456'
        with self.assertRaises(NotFound):
            self.rms.delete_resource_type(resource_id, object_id)
        self.rms.clients.resource_registry.get_association.assert_called_once_with(resource_id, PRED.hasObjectType, object_id, None, False)

        association_id = '999'
        self.rms.clients.resource_registry.delete.side_effect = NotFound
        self.rms.clients.resource_registry.get_association.side_effect = None
        self.rms.clients.resource_registry.get_association.return_value = association_id
        self.rms.clients.resource_registry.delete_association.return_value = True
        with self.assertRaises(NotFound):
            self.rms.delete_resource_type(resource_id, object_id)
        self.rms.clients.resource_registry.get_association.assert_called_with(resource_id, PRED.hasObjectType, object_id, None, False)
        self.rms.clients.resource_registry.delete_association.assert_called_with(association_id)
        self.rms.clients.resource_registry.delete.assert_called_with(resource_id)


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
        self.rms.delete_resource_type(resource_id, object_id)
        self.oms.delete_object_type(object_id)

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
        self.rms.delete_resource_type(resource_id, object_id)
        self.oms.delete_object_type(object_id)

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
        self.rms.delete_resource_type(resource_id, object_id)
        self.oms.delete_object_type(object_id)

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
        self.rms.delete_resource_type(resource_id, object_id)
        self.oms.delete_object_type(object_id)

        # Delete a ResourceType that has already been deleted
        with self.assertRaises(NotFound):
            self.rms.delete_resource_type(resource_id, object_id)


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''

@attr('INT', group='rms')
class TestResourceManagementServiceInterface(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Now create client to service
        self.rr = self.container.resource_registry
        fp = FakeProcess()

        self.rms = ResourceManagementServiceProcessClient(process=fp)

    def test_resource_interface(self):
        rid1,_ = self.rr.create(IonObject('Resource', name='res1'))

        cap_list = self.rms.get_capabilities(rid1)
        log.warn("Capabilities: %s", cap_list)
        self.assertTrue(type(cap_list) is list)

        get_res = self.rms.get_resource(rid1, params=['object_size'])
        log.warn("Get result: %s", get_res)

        self.rms.set_resource(rid1, params={'description': 'NEWDESC'})
        res_obj = self.rr.read(rid1)
        self.assertEquals(res_obj.description, 'NEWDESC')

        self.rr.delete(rid1)

        # Test CRUD
        rid2 = self.rms.create_resource(IonObject('Resource', name='res2'))
        res_obj = self.rr.read(rid2)
        self.assertEquals(res_obj.name, 'res2')

        res_obj.description = 'DESC2'
        self.rms.update_resource(res_obj)
        res_obj = self.rr.read(rid2)
        self.assertEquals(res_obj.description, 'DESC2')

        res_obj2 = self.rms.read_resource(rid2)
        self.assertEquals(res_obj.description, res_obj2.description)

        self.rms.delete_resource(rid2)

        rid3,_ = self.rr.create(IonObject('InstrumentDevice', name='res3'))
        rid3_r = self.rr.read(rid3)
        self.assertEquals(rid3_r.lcstate, "DRAFT_PRIVATE")

        self.rms.execute_lifecycle_transition(rid3, "plan")
        rid3_r = self.rr.read(rid3)
        self.assertEquals(rid3_r.lcstate, "PLANNED_PRIVATE")


