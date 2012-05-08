#!/usr/bin/env python

__author__ = 'Thomas Lennan'

from nose.plugins.attrib import attr

from pyon.core.bootstrap import get_sys_name
from pyon.core.exception import BadRequest
from pyon.datastore.datastore import DataStore
from pyon.public import IonObject, RT
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.idatastore_service import DatastoreServiceClient, DatastoreServiceProcessClient

@attr('INT', group='coi')
class TestDatastore(IonIntegrationTestCase):
    
    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2coi.yml')

        # Now create client to bank service
        self.datastore_service = DatastoreServiceClient(node=self.container.node)

    def test_manage_datastore(self):
        db_name_prefix = get_sys_name().lower()
        self.datastore_service.delete_datastore(db_name_prefix + "_foo")

        self.datastore_service.create_datastore(db_name_prefix + "_foo")

        with self.assertRaises(BadRequest) as cm:
            self.datastore_service.create_datastore(db_name_prefix + "_foo")
        self.assertTrue(cm.exception.message.startswith("Data store with name"))

        ds_list = self.datastore_service.list_datastores()
        self.assertTrue(db_name_prefix + "_foo" in ds_list)

        info = self.datastore_service.info_datastore(db_name_prefix + "_foo")

        self.assertTrue(self.datastore_service.datastore_exists(db_name_prefix + "_foo"))
        self.assertFalse(self.datastore_service.datastore_exists(db_name_prefix + "_bar"))

    def test_create_delete(self):
        # Persist IonObject
        actor_identity_obj = IonObject(RT.ActorIdentity, name="John Smith")
        actor_identity_obj_id, actor_identity_obj_rev = self.datastore_service.create(actor_identity_obj)

        # Make sure attempt to create object with same id fails
        with self.assertRaises(BadRequest) as cm:
            self.datastore_service.create(actor_identity_obj, actor_identity_obj_id)
        self.assertTrue(cm.exception.message.startswith("Object with id"))

        # Persist raw doc
        actor_identity_doc = {"name": "John Smith"}
        actor_identity_doc_id, actor_identity_doc_rev = self.datastore_service.create_doc(actor_identity_doc)

        # Make sure attempt to send object with _id to create fails
        bad_actor_identity_doc = {"name": "John Smith", "_id": "foo"}
        with self.assertRaises(BadRequest) as cm:
            self.datastore_service.create_doc(bad_actor_identity_doc)
        self.assertTrue(cm.exception.message.startswith("Doc must not have '_id'"))

        bad_actor_identity_doc = {"name": "John Smith", "_rev": "1"}
        with self.assertRaises(BadRequest) as cm:
            self.datastore_service.create_doc(bad_actor_identity_doc)
        self.assertTrue(cm.exception.message.startswith("Doc must not have '_rev'"))

        # Read IonObject
        read_actor_identity_obj = self.datastore_service.read(actor_identity_obj_id)

        # Pass something other than str id to read
        with self.assertRaises(BadRequest) as cm:
            self.datastore_service.read(123)
        self.assertTrue(cm.exception.message == "Object id param is not string")

        # Read raw dict
        read_actor_identity_doc = self.datastore_service.read_doc(actor_identity_doc_id)

        # Update IonObject and raw doc
        read_actor_identity_obj.name = "Jane Doe"
        read_actor_identity_doc["name"] = "Jane Doe"

        # Update IonObject
        updated_actor_identity_obj_id, updated_actor_identity_obj_rev = self.datastore_service.update(read_actor_identity_obj)

        # Try to pass non-IonObject to update
        with self.assertRaises(BadRequest) as cm:
            self.datastore_service.update(read_actor_identity_doc)
        self.assertTrue(cm.exception.message == "Obj param is not instance of IonObjectBase")

        # Update raw doc
        updated_actor_identity_doc_id, updated_actor_identity_doc_rev = self.datastore_service.update_doc(read_actor_identity_doc)

        # Delete IonObject
        self.datastore_service.delete(actor_identity_obj_id)

        # Re-read raw doc, try to pass non-IonObject to delete
        re_read_actor_identity_doc = self.datastore_service.read_doc(actor_identity_doc_id)
        with self.assertRaises(BadRequest) as cm:
            self.datastore_service.delete(re_read_actor_identity_doc)
        self.assertTrue(cm.exception.message == "Obj param is not instance of IonObjectBase or string id")

        # Delete raw doc
        self.datastore_service.delete_doc(actor_identity_doc_id)

