#!/usr/bin/env python

__author__ = 'Michael Meisinger, Thomas Lennan'

from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest, Conflict, NotFound, Inconsistent
from pyon.public import IonObject, PRED, RT, LCS, LCE, iex, log
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient, ResourceRegistryServiceProcessClient

@attr('INT', group='coi')
class TestResourceRegistry(IonIntegrationTestCase):
    
#    service_dependencies = [('resource_registry', {'resource_registry': {'persistent': True, 'force_clean': True}})]

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2coi.yml')

        # Now create client to bank service
        self.resource_registry_service = ResourceRegistryServiceClient(node=self.container.node)

    def test_crud(self):
        # Some quick registry tests
        # Can't call new with fields that aren't defined in the object's schema
        with self.assertRaises(TypeError) as cm:
            IonObject("UserInfo", name="name", foo="bar")
        self.assertTrue(cm.exception.message == "__init__() got an unexpected keyword argument 'foo'")

        # Can't call new with fields that aren't defined in the object's schema
        with self.assertRaises(TypeError) as cm:
            IonObject("UserInfo", {"name": "name", "foo": "bar"})
        self.assertTrue(cm.exception.message == "__init__() got an unexpected keyword argument 'foo'")

        # Can't call new with fields that aren't defined in the object's schema
        with self.assertRaises(TypeError) as cm:
            IonObject("UserInfo", {"name": "name"}, foo="bar")
        self.assertTrue(cm.exception.message == "__init__() got an unexpected keyword argument 'foo'")

        # Instantiate an object
        obj = IonObject("UserInfo", name="name")
        
        # Can set attributes that aren't in the object's schema
        with self.assertRaises(AttributeError) as cm:
            setattr(obj, "foo", "bar")
        self.assertTrue(cm.exception.message == "'UserInfo' object has no attribute 'foo'")

        # Cam't call update with object that hasn't been persisted
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.update(obj)
        self.assertTrue(cm.exception.message.startswith("Object does not have required '_id' or '_rev' attribute"))

        # Persist object and read it back
        obj_id, obj_rev = self.resource_registry_service.create(obj)
        read_obj = self.resource_registry_service.read(obj_id)

        # Cannot create object with _id and _rev fields pre-set        
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.create(read_obj)
        self.assertTrue(cm.exception.message.startswith("Doc must not have '_id'"))

        # Update object
        read_obj.name = "John Doe"
        self.resource_registry_service.update(read_obj)

        # Update should fail with revision mismatch
        with self.assertRaises(Conflict) as cm:
            self.resource_registry_service.update(read_obj)
        self.assertTrue(cm.exception.message.startswith("Object not based on most current version"))

        # Re-read and update object
        read_obj = self.resource_registry_service.read(obj_id)
        self.resource_registry_service.update(read_obj)
        
        # Delete object
        self.resource_registry_service.delete(obj_id)

        # Make sure read, update and delete report error
        with self.assertRaises(NotFound) as cm:
            self.resource_registry_service.read(obj_id)
        self.assertTrue(cm.exception.message.startswith("Object with id"))

        with self.assertRaises(NotFound) as cm:
            self.resource_registry_service.update(read_obj)
        self.assertTrue(cm.exception.message.startswith("Object with id"))

        with self.assertRaises(NotFound) as cm:
            self.resource_registry_service.delete(obj_id)
        self.assertTrue(cm.exception.message.startswith("Object with id"))

        # Owner creation tests
        user = IonObject("UserIdentity", name='user')
        uid,_ = self.resource_registry_service.create(user)

        inst = IonObject("InstrumentDevice", name='instrument')
        iid,_ = self.resource_registry_service.create(inst, headers={'ion-actor-id':str(uid)})

        ids = self.resource_registry_service.find_associations(iid, PRED.hasOwner)
        self.assertEquals(len(ids), 1)

    def test_lifecycle(self):
        att = IonObject("InstrumentDevice", name='mine', description='desc')

        rid,rev = self.resource_registry_service.create(att)

        att1 = self.resource_registry_service.read(rid)
        self.assertEquals(att1.name, att.name)
        self.assertEquals(att1.lcstate, LCS.DRAFT_PRIVATE)

        new_state = self.resource_registry_service.execute_lifecycle_transition(rid, LCE.PLAN)
        self.assertEquals(new_state, LCS.PLANNED_PRIVATE)

        att2 = self.resource_registry_service.read(rid)
        self.assertEquals(att2.lcstate, LCS.PLANNED_PRIVATE)

        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.execute_lifecycle_transition(rid, LCE.UNANNOUNCE)
        self.assertTrue("type=InstrumentDevice, lcstate=PLANNED_PRIVATE has no transition for event unannounce" in cm.exception.message)

        new_state = self.resource_registry_service.execute_lifecycle_transition(rid, LCE.DEVELOP)
        self.assertEquals(new_state, LCS.DEVELOPED_PRIVATE)

        self.assertRaises(iex.BadRequest, self.resource_registry_service.execute_lifecycle_transition,
                                            resource_id=rid, transition_event='NONE##')

        self.resource_registry_service.set_lifecycle_state(rid, LCS.INTEGRATED_PRIVATE)
        att1 = self.resource_registry_service.read(rid)
        self.assertEquals(att1.lcstate, LCS.INTEGRATED_PRIVATE)

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
        with self.assertRaises(AttributeError) as cm:
            self.resource_registry_service.create_association(user_identity_obj_id, PRED.bogus, user_info_obj_id)
        self.assertTrue(cm.exception.message == "bogus")

        # Predicate not provided
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.create_association(user_identity_obj_id, None, user_info_obj_id)
        self.assertTrue(cm.exception.message == "Association must have all elements set")

        # Bad association type
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.create_association(user_identity_obj_id, PRED.hasInfo, user_info_obj_id, 'bogustype')
        self.assertTrue(cm.exception.message == "Unsupported assoc_type: bogustype")

        # Subject id or object not provided
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.create_association(None, PRED.hasInfo, user_info_obj_id)
        self.assertTrue(cm.exception.message == "Association must have all elements set")

        # Object id or object not provided
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.create_association(user_identity_obj_id, PRED.hasInfo, None)
        self.assertTrue(cm.exception.message == "Association must have all elements set")

        # Bad subject id
        with self.assertRaises(NotFound) as cm:
            self.resource_registry_service.create_association("bogus", PRED.hasInfo, user_info_obj_id)
        self.assertTrue(cm.exception.message == "Object with id bogus does not exist.")

        # Bad object id
        with self.assertRaises(NotFound) as cm:
            self.resource_registry_service.create_association(user_identity_obj_id, PRED.hasInfo, "bogus")
        self.assertTrue(cm.exception.message == "Object with id bogus does not exist.")

        # _id missing from subject
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.create_association(user_identity_obj, PRED.hasInfo, user_info_obj_id)
        self.assertTrue(cm.exception.message == "Subject id or rev not available")

        # _id missing from object
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.create_association(user_identity_obj_id, PRED.hasInfo, user_info_obj)
        self.assertTrue(cm.exception.message == "Object id or rev not available")

        # Wrong subject type
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.create_association(user_info_obj_id, PRED.hasInfo, user_info_obj_id)
        self.assertTrue(cm.exception.message == "Illegal subject type UserInfo for predicate hasInfo")

        # Wrong object type
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.create_association(user_identity_obj_id, PRED.hasInfo, user_identity_obj_id)
        self.assertTrue(cm.exception.message == "Illegal object type UserIdentity for predicate hasInfo")

        # Create two different association types between the same subject and predicate
        assoc_id1, assoc_rev1 = self.resource_registry_service.create_association(user_identity_obj_id, PRED.hasInfo, user_info_obj_id)
        assoc_id2, assoc_rev2 = self.resource_registry_service.create_association(user_identity_obj_id, PRED.hasInfo, user_info_obj_id, "H2R")

        # Search for associations (good cases)
        ret1 = self.resource_registry_service.find_associations(user_identity_obj_id, PRED.hasInfo, user_info_obj_id)
        ret2 = self.resource_registry_service.find_associations(user_identity_obj_id, PRED.hasInfo)
        ret3 = self.resource_registry_service.find_associations(None, PRED.hasInfo)
        self.assertTrue(len(ret1) == len(ret2) == len(ret3))
        self.assertTrue(ret1[0]._id == ret2[0]._id == ret3[0]._id)

        ret1 = self.resource_registry_service.find_associations(user_identity_obj_id, PRED.hasInfo, user_info_obj_id, None, False)
        ret2 = self.resource_registry_service.find_associations(user_identity_obj_id, PRED.hasInfo, id_only=False)
        ret3 = self.resource_registry_service.find_associations(predicate=PRED.hasInfo, id_only=False)
        self.assertTrue(ret1 == ret2 == ret3)

        # Search for associations (good cases)
        ret1 = self.resource_registry_service.find_associations(read_user_identity_obj, PRED.hasInfo, read_user_info_obj)
        ret2 = self.resource_registry_service.find_associations(read_user_identity_obj, PRED.hasInfo)
        ret3 = self.resource_registry_service.find_associations(None, PRED.hasInfo)
        self.assertTrue(len(ret1) == len(ret2) == len(ret3))
        self.assertTrue(ret1[0]._id == ret2[0]._id == ret3[0]._id)

        ret1 = self.resource_registry_service.find_associations(user_identity_obj_id, PRED.hasInfo, read_user_info_obj, None, True)
        ret2 = self.resource_registry_service.find_associations(user_identity_obj_id, PRED.hasInfo, id_only=True)
        ret3 = self.resource_registry_service.find_associations(predicate=PRED.hasInfo, id_only=True)
        self.assertTrue(ret1 == ret2 == ret3)

        # Search for associations (bad cases)
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.find_associations(None, None, None)
        self.assertTrue(cm.exception.message == "Illegal parameters")

        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.find_associations(user_identity_obj_id, None, None)
        self.assertTrue(cm.exception.message == "Illegal parameters")

        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.find_associations(None, None, user_info_obj_id)
        self.assertTrue(cm.exception.message == "Illegal parameters")

        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.find_associations(user_identity_obj, None, user_info_obj_id)
        self.assertTrue(cm.exception.message == "Object id not available in subject")

        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.find_associations(user_identity_obj_id, None, user_info_obj)
        self.assertTrue(cm.exception.message == "Object id not available in object")

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
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.find_subjects(None, None, None)
        self.assertTrue(cm.exception.message == "Must provide object")

        with self.assertRaises(AttributeError) as cm:
            self.resource_registry_service.find_subjects(RT.UserCredentials, PRED.bogus, user_info_obj_id, True)
        self.assertTrue(cm.exception.message == "bogus")

        ret = self.resource_registry_service.find_subjects(RT.UserInfo, PRED.hasCredentials, user_info_obj_id, True)
        self.assertTrue(len(ret[0]) == 0)

        ret = self.resource_registry_service.find_subjects(RT.UserCredentials, PRED.hasInfo, user_info_obj_id, True)
        self.assertTrue(len(ret[0]) == 0)

        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.find_subjects(RT.UserCredentials, PRED.hasInfo, user_info_obj, True)
        self.assertTrue(cm.exception.message == "Object id not available in object")

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
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.find_objects(None, None, None)
        self.assertTrue(cm.exception.message == "Must provide subject")

        with self.assertRaises(AttributeError) as cm:
            self.resource_registry_service.find_objects(user_identity_obj_id, PRED.bogus, RT.UserCredentials, True)
        self.assertTrue(cm.exception.message == "bogus")

        ret = self.resource_registry_service.find_objects(user_identity_obj_id, PRED.hasCredentials, RT.UserIdentity, True)
        self.assertTrue(len(ret[0]) == 0)

        ret = self.resource_registry_service.find_objects(user_identity_obj_id, PRED.hasInfo, RT.UserCredentials, True)
        self.assertTrue(len(ret[0]) == 0)

        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.find_objects(user_identity_obj, PRED.hasInfo, RT.UserInfo, True)
        self.assertTrue(cm.exception.message == "Object id not available in subject")

        # Get association (bad cases)
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.get_association(None, None, None)
        self.assertTrue(cm.exception.message == "Illegal parameters")

        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.get_association(user_identity_obj_id, None, None)
        self.assertTrue(cm.exception.message == "Illegal parameters")

        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.get_association(None, None, user_info_obj_id)
        self.assertTrue(cm.exception.message == "Illegal parameters")

        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.get_association(user_identity_obj, None, user_info_obj_id)
        self.assertTrue(cm.exception.message == "Object id not available in subject")

        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.get_association(user_identity_obj_id, None, user_info_obj)
        self.assertTrue(cm.exception.message == "Object id not available in object")

        # Delete one of the associations
        self.resource_registry_service.delete_association(assoc_id2)

        assoc = self.resource_registry_service.get_association(user_identity_obj_id, PRED.hasInfo, user_info_obj_id)
        self.assertTrue(assoc._id == assoc_id1)

        # Delete (bad cases)
        with self.assertRaises(NotFound) as cm:
            self.resource_registry_service.delete_association("bogus")
        self.assertTrue(cm.exception.message == "Object with id bogus does not exist.")

        # Delete other association
        self.resource_registry_service.delete_association(assoc_id1)

        # Delete resources
        self.resource_registry_service.delete(user_identity_obj_id)
        self.resource_registry_service.delete(user_info_obj_id)

    def test_find_resources(self):
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.find_resources(RT.UserInfo, LCS.DRAFT, "name", False)
        self.assertTrue(cm.exception.message == "find by name does not support lcstate")
        
        ret = self.resource_registry_service.find_resources(RT.UserInfo, None, "name", False)
        self.assertTrue(len(ret[0]) == 0)

        # Instantiate an object
        obj = IonObject("InstrumentDevice", name="name")
        
        # Persist object and read it back
        obj_id, obj_rev = self.resource_registry_service.create(obj)
        read_obj = self.resource_registry_service.read(obj_id)

        ret = self.resource_registry_service.find_resources(RT.InstrumentDevice, None, "name", False)
        self.assertTrue(len(ret[0]) == 1)
        self.assertTrue(ret[0][0]._id == read_obj._id)

        ret = self.resource_registry_service.find_resources(RT.InstrumentDevice, LCS.DRAFT, None, False)
        self.assertTrue(len(ret[0]) == 1)
        self.assertTrue(ret[0][0]._id == read_obj._id)
