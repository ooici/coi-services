#!/usr/bin/env python

__author__ = 'Michael Meisinger, Thomas Lennan'

from unittest import SkipTest
from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest, Conflict, NotFound, Inconsistent
from pyon.public import IonObject, PRED, RT, LCS, LCE, iex
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient, ResourceRegistryServiceProcessClient

@attr('INT', group='resource')
class TestResourceRegistry(IonIntegrationTestCase):
    
#    service_dependencies = [('resource_registry', {'resource_registry': {'persistent': True, 'force_clean': True}})]

    def setUp(self):
        # Start container
        self._start_container()

        # Establish endpoint with container
        container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)
        container_client.start_rel_from_url('res/deploy/r2coi.yml')

        # Now create client to bank service
        self.resource_registry_service = ResourceRegistryServiceClient(node=self.container.node)

    def tearDown(self):
        self._stop_container()

    def test_crud(self):
        # Some quick registry tests
        # Can't call new with fields that aren't defined in the object's schema
        new_failed = False
        try:
            obj = IonObject("UserInfo", name="name", foo="bar")
        except TypeError as ex:
            self.assertTrue(ex.message == "__init__() got an unexpected keyword argument 'foo'")
            new_failed = True
        self.assertTrue(new_failed)

        # Can't call new with fields that aren't defined in the object's schema
        try:
            obj = IonObject("UserInfo", {"name": "name", "foo": "bar"})
        except TypeError as ex:
            self.assertTrue(ex.message == "__init__() got an unexpected keyword argument 'foo'")
            new_failed = True
        self.assertTrue(new_failed)

        # Can't call new with fields that aren't defined in the object's schema
        try:
            obj = IonObject("UserInfo", {"name": "name"}, foo="bar")
        except TypeError as ex:
            self.assertTrue(ex.message == "__init__() got an unexpected keyword argument 'foo'")
            new_failed = True
        self.assertTrue(new_failed)

        # Instantiate an object
        obj = IonObject("UserInfo", name="name")
        
        # Can set attributes that aren't in the object's schema
        set_failed = False
        try:
            setattr(obj, "foo", "bar")
        except AttributeError as ex:
            self.assertTrue(ex.message == "'UserInfo' object has no attribute 'foo'")
            set_failed = True
        self.assertTrue(set_failed)

        # Cam't call update with object that hasn't been persisted
        update_failed = False
        try:
            self.resource_registry_service.update(obj)
        except BadRequest as ex:
            self.assertTrue(ex.message.startswith("Object does not have required '_id' or '_rev' attribute"))
            update_failed = True
        self.assertTrue(update_failed)

        # Persist object and read it back
        obj_id, obj_rev = self.resource_registry_service.create(obj)
        read_obj = self.resource_registry_service.read(obj_id)

        # Cannot create object with _id and _rev fields pre-set        
        create_failed = False
        try:
            self.resource_registry_service.create(read_obj)
        except BadRequest as ex:
            self.assertTrue(ex.message.startswith("Doc must not have '_id'"))
            create_failed = True
        self.assertTrue(create_failed)

        # Update object
        read_obj.name = "John Doe"
        self.resource_registry_service.update(read_obj)

        # Update should fail with revision mismatch
        update_failed = False
        try:
            self.resource_registry_service.update(read_obj)
        except Conflict as ex:
            self.assertTrue(ex.message.startswith("Object not based on most current version"))
            update_failed = True
        self.assertTrue(update_failed)

        # Re-read and update object
        read_obj = self.resource_registry_service.read(obj_id)
        self.resource_registry_service.update(read_obj)
        
        # Delete object
        self.resource_registry_service.delete(obj_id)

        # Make sure read, update and delete report error
        read_failed = False
        try:
            self.resource_registry_service.read(obj_id)
        except NotFound as ex:
            self.assertTrue(ex.message.startswith("Object with id"))
            read_failed = True
        self.assertTrue(read_failed)

        update_failed = False
        try:
            self.resource_registry_service.update(read_obj)
        except Conflict as ex:
            self.assertTrue(ex.message.startswith("Object not based on most current version"))
            update_failed = True
        self.assertTrue(update_failed)

        delete_failed = False
        try:
            self.resource_registry_service.delete(obj_id)
        except NotFound as ex:
            self.assertTrue(ex.message.startswith("Object with id"))
            delete_failed = True
        self.assertTrue(delete_failed)
           
    def test_lifecycle(self):
        att = IonObject("Attachment", name='mine', description='desc')

        rid,rev = self.resource_registry_service.create(att)

        att1 = self.resource_registry_service.read(rid)
        self.assertEquals(att1.name, att.name)
        self.assertEquals(att1.lcstate, LCS.DRAFT)

        new_state = self.resource_registry_service.execute_lifecycle_transition(rid, LCE.register)
        self.assertEquals(new_state, LCS.PLANNED)

        att2 = self.resource_registry_service.read(rid)
        self.assertEquals(att2.lcstate, LCS.PLANNED)

        try:
            self.resource_registry_service.execute_lifecycle_transition(rid, LCE.register)
        except Inconsistent as ex:
            self.assertTrue("type=Attachment, lcstate=PLANNED has no transition for event register" in ex.message)

        new_state = self.resource_registry_service.execute_lifecycle_transition(rid, LCE.develop, LCS.PLANNED)
        self.assertEquals(new_state, LCS.DEVELOPED)

        self.assertRaises(iex.Inconsistent, self.resource_registry_service.execute_lifecycle_transition,
                                            resource_id=rid, transition_event=LCE.develop, current_lcstate=LCS.PLANNED)

    def test_association(self):
        # Instantiate UserIdentity object
        user_identity_obj = IonObject("UserIdentity", name="name")
        user_identity_obj_id, user_identity_obj_rev = self.resource_registry_service.create(user_identity_obj)
        read_user_identity_obj = self.resource_registry_service.read(user_identity_obj_id)

        # Instantiate UserInfo object
        user_info_obj = IonObject("UserInfo", name="name")
        user_info_obj_id, user_info_obj_rev = self.resource_registry_service.create(user_info_obj)
        read_user_info_obj = self.resource_registry_service.read(user_info_obj_id)

        # Test create failures
        create_failed = False
        try:
            self.resource_registry_service.create_association(user_identity_obj_id, PRED.bogus, user_info_obj_id)
        except AttributeError as ex:
            self.assertTrue(ex.message == "bogus")
            create_failed = True
        self.assertTrue(create_failed)

        # Predicate not provided
        create_failed = False
        try:
            self.resource_registry_service.create_association(user_identity_obj_id, None, user_info_obj_id)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Association must have all elements set")
            create_failed = True
        self.assertTrue(create_failed)

        # Bad association type
        create_failed = False
        try:
            self.resource_registry_service.create_association(user_identity_obj_id, PRED.hasInfo, user_info_obj_id, 'bogustype')
        except BadRequest as ex:
            self.assertTrue(ex.message == "Unsupported assoc_type: bogustype")
            create_failed = True
        self.assertTrue(create_failed)

        # Subject id or object not provided
        create_failed = False
        try:
            self.resource_registry_service.create_association(None, PRED.hasInfo, user_info_obj_id)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Association must have all elements set")
            create_failed = True
        self.assertTrue(create_failed)

        # Object id or object not provided
        create_failed = False
        try:
            self.resource_registry_service.create_association(user_identity_obj_id, PRED.hasInfo, None)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Association must have all elements set")
            create_failed = True
        self.assertTrue(create_failed)

        # Bad subject id
        create_failed = False
        try:
            self.resource_registry_service.create_association("bogus", PRED.hasInfo, user_info_obj_id)
        except NotFound as ex:
            self.assertTrue(ex.message == "Object with id bogus does not exist.")
            create_failed = True
        self.assertTrue(create_failed)

        # Bad object id
        create_failed = False
        try:
            self.resource_registry_service.create_association(user_identity_obj_id, PRED.hasInfo, "bogus")
        except NotFound as ex:
            self.assertTrue(ex.message == "Object with id bogus does not exist.")
            create_failed = True
        self.assertTrue(create_failed)

        # _id missing from subject
        create_failed = False
        try:
            self.resource_registry_service.create_association(user_identity_obj, PRED.hasInfo, user_info_obj_id)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Subject id or rev not available")
            create_failed = True
        self.assertTrue(create_failed)

        # _id missing from object
        create_failed = False
        try:
            self.resource_registry_service.create_association(user_identity_obj_id, PRED.hasInfo, user_info_obj)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Object id or rev not available")
            create_failed = True
        self.assertTrue(create_failed)

        # Wrong subject type
        create_failed = False
        try:
            self.resource_registry_service.create_association(user_info_obj_id, PRED.hasInfo, user_info_obj_id)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Illegal subject type UserInfo for predicate hasInfo")
            create_failed = True
        self.assertTrue(create_failed)

        # Wrong object type
        create_failed = False
        try:
            self.resource_registry_service.create_association(user_identity_obj_id, PRED.hasInfo, user_identity_obj_id)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Illegal object type UserIdentity for predicate hasInfo")
            create_failed = True
        self.assertTrue(create_failed)

        # Create duplicate associations
        assoc_id1, assoc_rev1 = self.resource_registry_service.create_association(user_identity_obj_id, PRED.hasInfo, user_info_obj_id)
        assoc_id2, assoc_rev2 = self.resource_registry_service.create_association(user_identity_obj_id, PRED.hasInfo, user_info_obj_id, "H2R")

        # Search for associations (good cases)
        ret1 = self.resource_registry_service.find_associations(user_identity_obj_id, PRED.hasInfo, user_info_obj_id)
        ret2 = self.resource_registry_service.find_associations(user_identity_obj_id, PRED.hasInfo)
        ret3 = self.resource_registry_service.find_associations(None, PRED.hasInfo)
        self.assertTrue(len(ret1) == len(ret2) == len(ret3))
        self.assertTrue(ret1[0]._id == ret2[0]._id == ret3[0]._id)

        ret1 = self.resource_registry_service.find_associations(user_identity_obj_id, PRED.hasInfo, user_info_obj_id, True)
        ret2 = self.resource_registry_service.find_associations(user_identity_obj_id, PRED.hasInfo, id_only=True)
        ret3 = self.resource_registry_service.find_associations(predicate=PRED.hasInfo, id_only=True)
        self.assertTrue(ret1 == ret2 == ret3)

        # Search for associations (good cases)
        ret1 = self.resource_registry_service.find_associations(read_user_identity_obj, PRED.hasInfo, read_user_info_obj)
        ret2 = self.resource_registry_service.find_associations(read_user_identity_obj, PRED.hasInfo)
        ret3 = self.resource_registry_service.find_associations(None, PRED.hasInfo)
        self.assertTrue(len(ret1) == len(ret2) == len(ret3))
        self.assertTrue(ret1[0]._id == ret2[0]._id == ret3[0]._id)

        ret1 = self.resource_registry_service.find_associations(user_identity_obj_id, PRED.hasInfo, read_user_info_obj, True)
        ret2 = self.resource_registry_service.find_associations(user_identity_obj_id, PRED.hasInfo, id_only=True)
        ret3 = self.resource_registry_service.find_associations(predicate=PRED.hasInfo, id_only=True)
        self.assertTrue(ret1 == ret2 == ret3)

        # Search for associations (bad cases)
        find_failed = False
        try:
            self.resource_registry_service.find_associations(None, None, None)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Illegal parameters")
            find_failed = True
        self.assertTrue(find_failed)

        find_failed = False
        try:
            self.resource_registry_service.find_associations(user_identity_obj_id, None, None)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Illegal parameters")
            find_failed = True
        self.assertTrue(find_failed)

        find_failed = False
        try:
            self.resource_registry_service.find_associations(None, None, user_info_obj_id)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Illegal parameters")
            find_failed = True
        self.assertTrue(find_failed)

        find_failed = False
        try:
            self.resource_registry_service.find_associations(user_identity_obj, None, user_info_obj_id)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Object id not available in subject")
            find_failed = True
        self.assertTrue(find_failed)

        find_failed = False
        try:
            self.resource_registry_service.find_associations(user_identity_obj_id, None, user_info_obj)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Object id not available in object")
            find_failed = True
        self.assertTrue(find_failed)

        # Find subjects (good cases)
        subj_ret1 = self.resource_registry_service.find_subjects(RT.UserIdentity, PRED.hasInfo, user_info_obj_id, True)
        subj_ret2 = self.resource_registry_service.find_subjects(RT.UserIdentity, PRED.hasInfo, read_user_info_obj, True)
        self.assertTrue(len(subj_ret1) == len(subj_ret2))
        self.assertTrue(subj_ret1[0] == subj_ret2[0])
        self.assertTrue(subj_ret1[1][0]._id == subj_ret2[1][0]._id)

        subj_ret3 = self.resource_registry_service.find_subjects(None, PRED.hasInfo, user_info_obj_id, True)
        subj_ret4 = self.resource_registry_service.find_subjects(None, None, read_user_info_obj, True)
        self.assertTrue(len(subj_ret3) == len(subj_ret4))
        self.assertTrue(subj_ret3[0] == subj_ret4[0])
        self.assertTrue(subj_ret3[1][0]._id == subj_ret4[1][0]._id)

        subj_ret5 = self.resource_registry_service.find_subjects(None, PRED.hasInfo, user_info_obj_id, False)
        subj_ret6 = self.resource_registry_service.find_subjects(None, None, read_user_info_obj, False)
        self.assertTrue(len(subj_ret5) == len(subj_ret6))
        self.assertTrue(subj_ret5[0][0]._id == subj_ret6[0][0]._id)
        self.assertTrue(subj_ret5[1][0]._id == subj_ret6[1][0]._id)

        # Find subjects (bad cases)
        find_failed = False
        try:
            self.resource_registry_service.find_subjects(None, None, None)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Must provide object")
            find_failed = True
        self.assertTrue(find_failed)

        find_failed = False
        try:
            self.resource_registry_service.find_subjects(RT.UserCredentials, PRED.bogus, user_info_obj_id, True)
        except AttributeError as ex:
            self.assertTrue(ex.message == "bogus")
            find_failed = True
        self.assertTrue(find_failed)

        ret = self.resource_registry_service.find_subjects(RT.UserInfo, PRED.hasCredentials, user_info_obj_id, True)
        self.assertTrue(len(ret[0]) == 0)

        ret = self.resource_registry_service.find_subjects(RT.UserCredentials, PRED.hasInfo, user_info_obj_id, True)
        self.assertTrue(len(ret[0]) == 0)

        find_failed = False
        try:
            self.resource_registry_service.find_subjects(RT.UserCredentials, PRED.hasInfo, user_info_obj, True)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Object id not available in object")
            find_failed = True
        self.assertTrue(find_failed)

        # Find objects (good cases)
        subj_ret1 = self.resource_registry_service.find_objects(user_identity_obj_id, PRED.hasInfo, RT.UserInfo, True)
        subj_ret2 = self.resource_registry_service.find_objects(read_user_identity_obj, PRED.hasInfo, RT.UserInfo, True)
        self.assertTrue(len(subj_ret1) == len(subj_ret2))
        self.assertTrue(subj_ret1[0] == subj_ret2[0])
        self.assertTrue(subj_ret1[1][0]._id == subj_ret2[1][0]._id)

        subj_ret3 = self.resource_registry_service.find_objects(user_identity_obj_id, PRED.hasInfo, None, True)
        subj_ret4 = self.resource_registry_service.find_objects(user_identity_obj_id, None, None, True)
        self.assertTrue(len(subj_ret3) == len(subj_ret4))
        self.assertTrue(subj_ret3[0] == subj_ret4[0])
        self.assertTrue(subj_ret3[1][0]._id == subj_ret4[1][0]._id)

        subj_ret5 = self.resource_registry_service.find_objects(user_identity_obj_id, PRED.hasInfo, None, False)
        subj_ret6 = self.resource_registry_service.find_objects(read_user_identity_obj, None, None, False)
        self.assertTrue(len(subj_ret5) == len(subj_ret6))
        self.assertTrue(subj_ret5[0][0]._id == subj_ret6[0][0]._id)
        self.assertTrue(subj_ret5[1][0]._id == subj_ret6[1][0]._id)

        # Find objects (bad cases)
        find_failed = False
        try:
            self.resource_registry_service.find_objects(None, None, None)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Must provide subject")
            find_failed = True
        self.assertTrue(find_failed)

        find_failed = False
        try:
            self.resource_registry_service.find_objects(user_identity_obj_id, PRED.bogus, RT.UserCredentials, True)
        except AttributeError as ex:
            self.assertTrue(ex.message == "bogus")
            find_failed = True
        self.assertTrue(find_failed)

        ret = self.resource_registry_service.find_objects(user_identity_obj_id, PRED.hasCredentials, RT.UserIdentity, True)
        self.assertTrue(len(ret[0]) == 0)

        ret = self.resource_registry_service.find_objects(user_identity_obj_id, PRED.hasInfo, RT.UserCredentials, True)
        self.assertTrue(len(ret[0]) == 0)

        find_failed = False
        try:
            self.resource_registry_service.find_objects(user_identity_obj, PRED.hasInfo, RT.UserInfo, True)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Object id not available in subject")
            find_failed = True
        self.assertTrue(find_failed)

        # Get association (bad cases)
        get_failed = False
        try:
            self.resource_registry_service.get_association(None, None, None)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Illegal parameters")
            get_failed = True
        self.assertTrue(get_failed)

        get_failed = False
        try:
            self.resource_registry_service.get_association(user_identity_obj_id, None, None)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Illegal parameters")
            get_failed = True
        self.assertTrue(get_failed)

        get_failed = False
        try:
            self.resource_registry_service.get_association(None, None, user_info_obj_id)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Illegal parameters")
            get_failed = True
        self.assertTrue(get_failed)

        get_failed = False
        try:
            self.resource_registry_service.get_association(user_identity_obj, None, user_info_obj_id)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Object id not available in subject")
            get_failed = True
        self.assertTrue(get_failed)

        get_failed = False
        try:
            self.resource_registry_service.get_association(user_identity_obj_id, None, user_info_obj)
        except BadRequest as ex:
            self.assertTrue(ex.message == "Object id not available in object")
            get_failed = True
        self.assertTrue(get_failed)

        get_failed = False
        try:
            self.resource_registry_service.get_association(user_identity_obj_id, PRED.hasInfo, user_info_obj_id)
        except Inconsistent as ex:
            self.assertTrue(ex.message.startswith("Duplicate associations found for subject/predicate/object"))
            get_failed = True
        self.assertTrue(get_failed)

        # Delete one of the associations
        self.resource_registry_service.delete_association(assoc_id1)

        assoc = self.resource_registry_service.get_association(user_identity_obj_id, PRED.hasInfo, user_info_obj_id)
        self.assertTrue(assoc._id == assoc_id2)

        # Delete (bad cases)
        delete_failed = False
        try:
            self.resource_registry_service.delete_association("bogus")
        except NotFound as ex:
            self.assertTrue(ex.message == "Object with id bogus does not exist.")
            delete_failed = True
        self.assertTrue(delete_failed)

        # Delete other association
        self.resource_registry_service.delete_association(assoc_id2)

        # Delete resources
        self.resource_registry_service.delete(user_identity_obj_id)
        self.resource_registry_service.delete(user_info_obj_id)

    def test_find_resources(self):
        find_failed = False
        try:
            ret = self.resource_registry_service.find_resources(RT.UserInfo, LCS.DRAFT, "name", False)
        except BadRequest as ex:
            self.assertTrue(ex.message == "find by name does not support lcstate")
            find_failed = True
        self.assertTrue(find_failed)
        
        ret = self.resource_registry_service.find_resources(RT.UserInfo, None, "name", False)
        self.assertTrue(len(ret[0]) == 0)

        # Instantiate an object
        obj = IonObject("UserInfo", name="name")
        
        # Persist object and read it back
        obj_id, obj_rev = self.resource_registry_service.create(obj)
        read_obj = self.resource_registry_service.read(obj_id)

        ret = self.resource_registry_service.find_resources(RT.UserInfo, None, "name", False)
        self.assertTrue(len(ret[0]) == 1)
        self.assertTrue(ret[0][0]._id == read_obj._id)

        ret = self.resource_registry_service.find_resources(RT.UserInfo, LCS.DRAFT, None, False)
        self.assertTrue(len(ret[0]) == 1)
        self.assertTrue(ret[0][0]._id == read_obj._id)

#    def test_service(self):
#        res = self.clients.resource_registry.find_resources(RT.Org, None, None, True)
