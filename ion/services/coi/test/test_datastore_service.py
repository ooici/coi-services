#!/usr/bin/env python

__author__ = 'Thomas Lennan'

from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest
from pyon.datastore.datastore import DataStore
from pyon.public import IonObject, RT
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.coi.idatastore_service import DatastoreServiceClient, DatastoreServiceProcessClient

@attr('INT', group='datastore')
class TestDatastore(IonIntegrationTestCase):
    
    def setUp(self):
        # Start container
        self._start_container()

        # Establish endpoint with container
        container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)
        container_client.start_rel_from_url('res/deploy/r2coi.yml')

        # Now create client to bank service
        self.datastore_service = DatastoreServiceClient(node=self.container.node)

    def tearDown(self):
        self._stop_container()

    def test_manage_datastore(self):
        self.datastore_service.delete_datastore("foo")

        self.datastore_service.create_datastore("foo")

        create_failed = False
        try:
            self.datastore_service.create_datastore("foo")
        except BadRequest as ex:
            self.assertTrue(ex.message == "Data store with name foo already exists")
            create_failed = True
        self.assertTrue(create_failed)

        ds_list = self.datastore_service.list_datastores()
        self.assertTrue("foo" in ds_list)

        info = self.datastore_service.info_datastore("foo")

        self.assertTrue(self.datastore_service.datastore_exists("foo"))
        self.assertFalse(self.datastore_service.datastore_exists("bar"))

        objs = self.datastore_service.list_objects("foo")
        self.assertTrue(len(objs) > 0)  # There are build in types

        revs = self.datastore_service.list_object_revisions(objs[0], "foo")
        self.assertTrue(len(revs) > 0)

    def test_create_delete(self):
        # Persist IonObject
        user_info_obj = IonObject(RT.UserInfo, name="John Smith")
        user_info_obj_id, user_info_obj_rev = self.datastore_service.create(user_info_obj)

        # Make sure attempt to create object with same id fails
        create_failed = False
        try:
            self.datastore_service.create(user_info_obj, user_info_obj_id)
        except BadRequest as ex:
            self.assertTrue(ex.message.startswith("Object with id"))
            create_failed = True
        self.assertTrue(create_failed)

        # Persist raw doc
        user_info_doc = {"name": "John Smith"}
        user_info_doc_id, user_info_doc_rev = self.datastore_service.create_doc(user_info_doc)

        # Make sure attempt to send object with _id to create fails
        bad_user_info_doc = {"name": "John Smith", "_id": "foo"}
        create_failed = False
        try:
            self.datastore_service.create_doc(bad_user_info_doc)
        except BadRequest as ex:
            self.assertTrue(ex.message.startswith("Doc must not have '_id'"))
            create_failed = True
        self.assertTrue(create_failed)

        bad_user_info_doc = {"name": "John Smith", "_rev": "1"}
        create_failed = False
        try:
            self.datastore_service.create_doc(bad_user_info_doc)
        except BadRequest as ex:
            self.assertTrue(ex.message.startswith("Doc must not have '_rev'"))
            create_failed = True
        self.assertTrue(create_failed)

        # Read IonObject
        read_user_info_obj = self.datastore_service.read(user_info_obj_id)

        # Pass something other than str id to read
        read_failed = False
        try:
            self.datastore_service.read(123)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Object id param is not string")
            read_failed = True
        self.assertTrue(read_failed)

        # Read raw dict
        read_user_info_doc = self.datastore_service.read_doc(user_info_doc_id)

        # Update IonObject and raw doc
        read_user_info_obj.name = "Jane Doe"
        read_user_info_doc["name"] = "Jane Doe"

        # Update IonObject
        updated_user_info_obj_id, updated_user_info_obj_rev = self.datastore_service.update(read_user_info_obj)

        # Try to pass non-IonObject to update
        update_failed = False
        try:
            self.datastore_service.update(read_user_info_doc)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Obj param is not instance of IonObjectBase")
            update_failed = True
        self.assertTrue(update_failed)

        # Update raw doc
        updated_user_info_doc_id, updated_user_info_doc_rev = self.datastore_service.update_doc(read_user_info_doc)

        # Delete IonObject
        self.datastore_service.delete(user_info_obj_id)

        # Re-read raw doc, try to pass non-IonObject to delete
        re_read_user_info_doc = self.datastore_service.read_doc(user_info_doc_id)
        delete_failed = False
        try:
            self.datastore_service.delete(re_read_user_info_doc)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Obj param is not instance of IonObjectBase or string id")
            delete_failed = True
        self.assertTrue(delete_failed)

        # Delete raw doc
        self.datastore_service.delete_doc(user_info_doc_id)

    def test_find(self):
        # Persist IonObject
        sample_obj1 = IonObject("SampleObject", name="John Smith", an_int=123)
        sampe_obj_id1, sample_obj_rev1 = self.datastore_service.create(sample_obj1)

        sample_obj2 = IonObject("SampleObject", name="Jane Smith", an_int=123)
        sampe_obj_id2, sample_obj_rev2 = self.datastore_service.create(sample_obj2)
        
        find_failed = False
        try:
            self.datastore_service.find([["an_int", DataStore.EQUAL]])
        except BadRequest as ex:
            self.assertTrue(ex.message == "Insufficient criterion values specified.  Much match [<field>, <logical constant>, <value>]")
            find_failed = True
        self.assertTrue(find_failed)
        
        find_failed = False
        try:
            self.datastore_service.find([["an_int", DataStore.EQUAL, 123]])
        except BadRequest as ex:
            self.assertTrue(ex.message == "All criterion values must be strings")
            find_failed = True
        self.assertTrue(find_failed)
        
        res = self.datastore_service.find([["an_int", DataStore.EQUAL, "123"]])
        self.assertTrue(len(res) == 2)

        res = self.datastore_service.find([["an_int", DataStore.EQUAL, "123"], DataStore.AND, ["name", DataStore.EQUAL, "John Smith"]])
        self.assertTrue(len(res) == 1)

