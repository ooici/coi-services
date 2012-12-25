#!/usr/bin/env python

__author__ = 'Michael Meisinger, Thomas Lennan, Stephen Henrie'

import unittest
from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest, Conflict, NotFound, Inconsistent
from pyon.public import IonObject, PRED, RT, LCS, LCE, iex, log
from pyon.util.int_test import IonIntegrationTestCase

from interface.objects import Attachment, AttachmentType, Resource, DataProcess, Transform, ProcessDefinition
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

@attr('INT', group='rr1')
class TestResourceRegistry(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2coi.yml')

        # Now create client to bank service
        self.resource_registry_service = ResourceRegistryServiceClient(node=self.container.node)

    @unittest.skip('Represents a bug in storage/retrieval')
    def test_tuple_in_dict(self):
        # create a resource with a tuple saved in a dict
        transform_obj = IonObject(RT.Transform)
        transform_obj.configuration = {}
        transform_obj.configuration["tuple"] = ('STRING',)
        transform_id, _ = self.resource_registry_service.create(transform_obj)

        # read the resource back
        returned_transform_obj = self.resource_registry_service.read(transform_id)

        self.assertEqual(transform_obj.configuration["tuple"], returned_transform_obj.configuration["tuple"])

    #@unittest.skip('this test just for debugging setup')
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
        user = IonObject("ActorIdentity", name='user')
        uid,_ = self.resource_registry_service.create(user)

        inst = IonObject("InstrumentDevice", name='instrument')
        iid,_ = self.resource_registry_service.create(inst, headers={'ion-actor-id':str(uid)})

        ids,_ = self.resource_registry_service.find_objects(iid, PRED.hasOwner, RT.ActorIdentity, id_only=True)
        self.assertEquals(len(ids), 1)

        assoc = self.resource_registry_service.read(ids[0])
        self.resource_registry_service.delete(iid)

        with self.assertRaises(NotFound) as ex:
            assoc = self.resource_registry_service.read(iid)


    #@unittest.skip('this test just for debugging setup')
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

    #@unittest.skip('this test just for debugging setup')
    def test_association(self):
        # Instantiate ActorIdentity object
        actor_identity_obj = IonObject("ActorIdentity", name="name")
        actor_identity_obj_id, actor_identity_obj_rev = self.resource_registry_service.create(actor_identity_obj)
        read_actor_identity_obj = self.resource_registry_service.read(actor_identity_obj_id)

        # Instantiate UserInfo object
        user_info_obj = IonObject("UserInfo", name="name")
        user_info_obj_id, user_info_obj_rev = self.resource_registry_service.create(user_info_obj)
        read_user_info_obj = self.resource_registry_service.read(user_info_obj_id)

        # Test create failures
        with self.assertRaises(AttributeError) as cm:
            self.resource_registry_service.create_association(actor_identity_obj_id, PRED.bogus, user_info_obj_id)
        self.assertTrue(cm.exception.message == "bogus")

        # Predicate not provided
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.create_association(actor_identity_obj_id, None, user_info_obj_id)
        self.assertTrue(cm.exception.message == "Association must have all elements set")

        # Subject id or object not provided
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.create_association(None, PRED.hasInfo, user_info_obj_id)
        self.assertTrue(cm.exception.message == "Association must have all elements set")

        # Object id or object not provided
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.create_association(actor_identity_obj_id, PRED.hasInfo, None)
        self.assertTrue(cm.exception.message == "Association must have all elements set")

        # Bad subject id
        with self.assertRaises(NotFound) as cm:
            self.resource_registry_service.create_association("bogus", PRED.hasInfo, user_info_obj_id)
        self.assertTrue(cm.exception.message == "Object with id bogus does not exist.")

        # Bad object id
        with self.assertRaises(NotFound) as cm:
            self.resource_registry_service.create_association(actor_identity_obj_id, PRED.hasInfo, "bogus")
        self.assertTrue(cm.exception.message == "Object with id bogus does not exist.")

        # _id missing from subject
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.create_association(actor_identity_obj, PRED.hasInfo, user_info_obj_id)
        self.assertTrue(cm.exception.message == "Subject id or rev not available")

        # _id missing from object
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.create_association(actor_identity_obj_id, PRED.hasInfo, user_info_obj)
        self.assertTrue(cm.exception.message == "Object id or rev not available")

        # Wrong subject type
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.create_association(user_info_obj_id, PRED.hasInfo, user_info_obj_id)
        self.assertTrue(cm.exception.message == "Illegal subject type UserInfo for predicate hasInfo")

        # Wrong object type
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.create_association(actor_identity_obj_id, PRED.hasInfo, actor_identity_obj_id)
        self.assertTrue(cm.exception.message == "Illegal object type ActorIdentity for predicate hasInfo")

        # Create two different association types between the same subject and predicate
        assoc_id1, assoc_rev1 = self.resource_registry_service.create_association(actor_identity_obj_id, PRED.hasInfo, user_info_obj_id)

        # Read object, subject
        res_obj1 = self.resource_registry_service.read_object(actor_identity_obj_id, PRED.hasInfo, RT.UserInfo)
        self.assertEquals(res_obj1._id, user_info_obj_id)
        res_obj1 = self.resource_registry_service.read_object(actor_identity_obj_id, PRED.hasInfo, RT.UserInfo, id_only=True)
        self.assertEquals(res_obj1, user_info_obj_id)
        res_obj2 = self.resource_registry_service.read_subject(RT.ActorIdentity, PRED.hasInfo, user_info_obj_id)
        self.assertEquals(res_obj2._id, actor_identity_obj_id)
        res_obj2 = self.resource_registry_service.read_subject(RT.ActorIdentity, PRED.hasInfo, user_info_obj_id, id_only=True)
        self.assertEquals(res_obj2, actor_identity_obj_id)

        # Search for associations (good cases)
        ret1 = self.resource_registry_service.find_associations(actor_identity_obj_id, PRED.hasInfo, user_info_obj_id)
        ret2 = self.resource_registry_service.find_associations(actor_identity_obj_id, PRED.hasInfo)
        ret3 = self.resource_registry_service.find_associations(None, PRED.hasInfo)
        self.assertTrue(len(ret1) == len(ret2) == len(ret3))
        self.assertTrue(ret1[0]._id == ret2[0]._id == ret3[0]._id)

        ret1 = self.resource_registry_service.find_associations(actor_identity_obj_id, PRED.hasInfo, user_info_obj_id, None, False)
        ret2 = self.resource_registry_service.find_associations(actor_identity_obj_id, PRED.hasInfo, id_only=False)
        ret3 = self.resource_registry_service.find_associations(predicate=PRED.hasInfo, id_only=False)
        self.assertTrue(ret1 == ret2 == ret3)

        # Search for associations (good cases)
        ret1 = self.resource_registry_service.find_associations(read_actor_identity_obj, PRED.hasInfo, read_user_info_obj)
        ret2 = self.resource_registry_service.find_associations(read_actor_identity_obj, PRED.hasInfo)
        ret3 = self.resource_registry_service.find_associations(None, PRED.hasInfo)
        self.assertTrue(len(ret1) == len(ret2) == len(ret3))
        self.assertTrue(ret1[0]._id == ret2[0]._id == ret3[0]._id)

        ret1 = self.resource_registry_service.find_associations(actor_identity_obj_id, PRED.hasInfo, read_user_info_obj, None, True)
        ret2 = self.resource_registry_service.find_associations(actor_identity_obj_id, PRED.hasInfo, id_only=True)
        ret3 = self.resource_registry_service.find_associations(predicate=PRED.hasInfo, id_only=True)
        self.assertTrue(ret1 == ret2 == ret3)

        # Search for associations (bad cases)
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.find_associations(None, None, None)
        self.assertIn("Illegal parameters", cm.exception.message)

        # Find subjects (good cases)
        subj_ret1 = self.resource_registry_service.find_subjects(RT.ActorIdentity, PRED.hasInfo, user_info_obj_id, True)
        subj_ret2 = self.resource_registry_service.find_subjects(RT.ActorIdentity, PRED.hasInfo, read_user_info_obj, True)
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
        subj_ret1 = self.resource_registry_service.find_objects(actor_identity_obj_id, PRED.hasInfo, RT.UserInfo, True)
        subj_ret2 = self.resource_registry_service.find_objects(read_actor_identity_obj, PRED.hasInfo, RT.UserInfo, True)
        self.assertTrue(len(subj_ret1) == len(subj_ret2))
        self.assertTrue(subj_ret1[0] == subj_ret2[0])
        self.assertTrue(subj_ret1[1][0]._id == subj_ret2[1][0]._id)

        subj_ret3 = self.resource_registry_service.find_objects(actor_identity_obj_id, PRED.hasInfo, None, True)
        subj_ret4 = self.resource_registry_service.find_objects(actor_identity_obj_id, None, None, True)
        self.assertTrue(len(subj_ret3) == len(subj_ret4))
        self.assertTrue(subj_ret3[0] == subj_ret4[0])
        self.assertTrue(subj_ret3[1][0]._id == subj_ret4[1][0]._id)

        subj_ret5 = self.resource_registry_service.find_objects(actor_identity_obj_id, PRED.hasInfo, None, False)
        subj_ret6 = self.resource_registry_service.find_objects(read_actor_identity_obj, None, None, False)
        self.assertTrue(len(subj_ret5) == len(subj_ret6))
        self.assertTrue(subj_ret5[0][0]._id == subj_ret6[0][0]._id)
        self.assertTrue(subj_ret5[1][0]._id == subj_ret6[1][0]._id)

        # Find objects (bad cases)
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.find_objects(None, None, None)
        self.assertTrue(cm.exception.message == "Must provide subject")

        with self.assertRaises(AttributeError) as cm:
            self.resource_registry_service.find_objects(actor_identity_obj_id, PRED.bogus, RT.UserCredentials, True)
        self.assertTrue(cm.exception.message == "bogus")

        ret = self.resource_registry_service.find_objects(actor_identity_obj_id, PRED.hasCredentials, RT.ActorIdentity, True)
        self.assertTrue(len(ret[0]) == 0)

        ret = self.resource_registry_service.find_objects(actor_identity_obj_id, PRED.hasInfo, RT.UserCredentials, True)
        self.assertTrue(len(ret[0]) == 0)

        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.find_objects(actor_identity_obj, PRED.hasInfo, RT.UserInfo, True)
        self.assertTrue(cm.exception.message == "Object id not available in subject")

        # Get association (bad cases)
        with self.assertRaises(BadRequest) as cm:
            self.resource_registry_service.get_association(None, None, None)
        self.assertIn("Illegal parameters", cm.exception.message)


        assoc = self.resource_registry_service.get_association(actor_identity_obj_id, PRED.hasInfo, user_info_obj_id)
        self.assertTrue(assoc._id == assoc_id1)

        # Delete (bad cases)
        with self.assertRaises(NotFound) as cm:
            self.resource_registry_service.delete_association("bogus")
        self.assertTrue(cm.exception.message == "Object with id bogus does not exist.")

        # Delete other association
        self.resource_registry_service.delete_association(assoc_id1)

        # Delete resources
        self.resource_registry_service.delete(actor_identity_obj_id)
        self.resource_registry_service.delete(user_info_obj_id)

    #@unittest.skip('this test just for debugging setup')
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

    #@unittest.skip('this test just for debugging setup')
    def test_attach(self):

        binary = "\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x10\x00\x00\x00\x10\x08\x03\x00\x00\x00(-\x0fS\x00\x00\x00\x03sBIT\x08\x08\x08\xdb\xe1O\xe0\x00\x00\x00~PLTEf3\x00\xfc\xf7\xe0\xee\xcc\x00\xd3\xa0\x00\xcc\x99\x00\xec\xcdc\x9fl\x00\xdd\xb2\x00\xff\xff\xff|I\x00\xf9\xdb\x00\xdd\xb5\x19\xd9\xad\x10\xb6\x83\x00\xf8\xd6\x00\xf2\xc5\x00\xd8\xab\x00n;\x00\xff\xcc\x00\xd6\xa4\t\xeb\xb8\x00\x83Q\x00\xadz\x00\xff\xde\x00\xff\xd6\x00\xd6\xa3\x00\xdf\xaf\x00\xde\xad\x10\xbc\x8e\x00\xec\xbe\x00\xec\xd4d\xff\xe3\x00tA\x00\xf6\xc4\x00\xf6\xce\x00\xa5u\x00\xde\xa5\x00\xf7\xbd\x00\xd6\xad\x08\xdd\xaf\x19\x8cR\x00\xea\xb7\x00\xee\xe9\xdf\xc5\x00\x00\x00\tpHYs\x00\x00\n\xf0\x00\x00\n\xf0\x01B\xac4\x98\x00\x00\x00\x1ctEXtSoftware\x00Adobe Fireworks CS4\x06\xb2\xd3\xa0\x00\x00\x00\x15tEXtCreation Time\x0029/4/09Oq\xfdE\x00\x00\x00\xadIDAT\x18\x95M\x8f\x8d\x0e\x820\x0c\x84;ZdC~f\x07\xb2\x11D\x86\x89\xe8\xfb\xbf\xa0+h\xe2\x97\\\xd2^\x93\xb6\x07:1\x9f)q\x9e\xa5\x06\xad\xd5\x13\x8b\xac,\xb3\x02\x9d\x12C\xa1-\xef;M\x08*\x19\xce\x0e?\x1a\xeb4\xcc\xd4\x0c\x831\x87V\xca\xa1\x1a\xd3\x08@\xe4\xbd\xb7\x15P;\xc8\xd4{\x91\xbf\x11\x90\xffg\xdd\x8di\xfa\xb6\x0bs2Z\xff\xe8yg2\xdc\x11T\x96\xc7\x05\xa5\xef\x96+\xa7\xa59E\xae\xe1\x84cm^1\xa6\xb3\xda\x85\xc8\xd8/\x17se\x0eN^'\x8c\xc7\x8e\x88\xa8\xf6p\x8e\xc2;\xc6.\xd0\x11.\x91o\x12\x7f\xcb\xa5\xfe\x00\x89]\x10:\xf5\x00\x0e\xbf\x00\x00\x00\x00IEND\xaeB`\x82"

        # Owner creation tests
        instrument = IonObject("InstrumentDevice", name='instrument')
        iid,_ = self.resource_registry_service.create(instrument)

        att = Attachment(content=binary, attachment_type=AttachmentType.BLOB)
        aid1 = self.resource_registry_service.create_attachment(iid, att)

        att1 = self.resource_registry_service.read_attachment(aid1, include_content=True)
        self.assertEquals(binary, att1.content)

        import base64
        att = Attachment(content=base64.encodestring(binary), attachment_type=AttachmentType.ASCII)
        aid2 = self.resource_registry_service.create_attachment(iid, att)

        att1 = self.resource_registry_service.read_attachment(aid2, include_content=True)
        self.assertEquals(binary, base64.decodestring(att1.content))

        att_ids = self.resource_registry_service.find_attachments(iid, id_only=True)
        self.assertEquals(att_ids, [aid1, aid2])

        att_ids = self.resource_registry_service.find_attachments(iid, id_only=True, descending=True)
        self.assertEquals(att_ids, [aid2, aid1])

        att_ids = self.resource_registry_service.find_attachments(iid, id_only=True, descending=True, limit=1)
        self.assertEquals(att_ids, [aid2])

        atts = self.resource_registry_service.find_attachments(iid, id_only=False, include_content=True, limit=1)
        self.assertEquals(atts[0].content, binary)

        self.resource_registry_service.delete_attachment(aid1)

        att_ids = self.resource_registry_service.find_attachments(iid, id_only=True)
        self.assertEquals(att_ids, [aid2])

        self.resource_registry_service.delete_attachment(aid2)

        att_ids = self.resource_registry_service.find_attachments(iid, id_only=True)
        self.assertEquals(att_ids, [])

    #@unittest.skip('this test just for debugging setup')
    def test_read_mult(self):
        test_resource1_id,_  = self.resource_registry_service.create(Resource(name='test1'))
        test_resource2_id,_  = self.resource_registry_service.create(Resource(name='test2'))

        res_list = [test_resource1_id, test_resource2_id]

        objects = self.resource_registry_service.read_mult(res_list)

        for o in objects:
            self.assertIsInstance(o,Resource)
            self.assertTrue(o._id in res_list)

    #@unittest.skip('this test just for debugging setup')
    def test_find_objects_mult(self):
        dp = DataProcess()
        transform = Transform()
        pd = ProcessDefinition()

        dp_id, _ = self.resource_registry_service.create(dp)
        transform_id, _ = self.resource_registry_service.create(transform)
        pd_id, _ = self.resource_registry_service.create(pd)

        self.resource_registry_service.create_association(subject=dp_id, object=transform_id, predicate=PRED.hasTransform)
        self.resource_registry_service.create_association(subject=transform_id, object=pd_id, predicate=PRED.hasProcessDefinition)

        results, _  = self.resource_registry_service.find_objects_mult(subjects=[dp_id],id_only=True)
        self.assertTrue(results == [transform_id])

        results, _  = self.resource_registry_service.find_objects_mult(subjects=[dp_id, transform_id], id_only=True)
        results.sort()
        correct = [transform_id, pd_id]
        correct.sort()
        self.assertTrue(results == correct)

    def test_get_resource_extension(self):


        #Testing multiple instrument owners
        subject1 = "/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A254"

        actor_identity_obj1 = IonObject("ActorIdentity", {"name": subject1})
        user_id1,_ = self.resource_registry_service.create(actor_identity_obj1)

        user_info_obj1 = IonObject("UserInfo", {"name": "Foo"})
        user_info_id1,_ = self.resource_registry_service.create(user_info_obj1)
        self.resource_registry_service.create_association(user_id1, PRED.hasInfo, user_info_id1)

        subject2 = "/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Bob Cumbers A256"

        actor_identity_obj2 = IonObject("ActorIdentity", {"name": subject2})
        user_id2,_ = self.resource_registry_service.create(actor_identity_obj2)

        user_info_obj2 = IonObject("UserInfo", {"name": "Foo2"})
        user_info_id2,_ = self.resource_registry_service.create(user_info_obj2)
        self.resource_registry_service.create_association(user_id2, PRED.hasInfo, user_info_id2)

        test_obj = IonObject('InformationResource',  {"name": "TestResource"})
        test_obj_id,_ = self.resource_registry_service.create(test_obj)
        self.resource_registry_service.create_association(test_obj_id, PRED.hasOwner, user_id1)
        self.resource_registry_service.create_association(test_obj_id, PRED.hasOwner, user_id2)

        extended_resource = self.resource_registry_service.get_resource_extension(test_obj_id, 'ExtendedInformationResource')

        self.assertEqual(test_obj_id,extended_resource._id)
        self.assertEqual(len(extended_resource.owners),2)

        extended_resource_list = self.resource_registry_service.get_resource_extension(str([user_info_id1,user_info_id2]), 'ExtendedInformationResource')
        self.assertEqual(len(extended_resource_list), 2)
