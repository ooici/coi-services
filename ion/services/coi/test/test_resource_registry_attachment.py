from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.objects import AttachmentType
from pyon.public import RT, PRED, IonObject
import unittest
from nose.plugins.attrib import attr


@attr('INT', group='coi')
class TestResourceRegistryAttachments(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        #print 'instantiating container'
        self._start_container()
        #container = Container()
        #print 'starting container'
        #container.start()
        #print 'started container'

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.RR  = ResourceRegistryServiceClient(node=self.container.node)

        print 'started services'

    def test_resource_registry_blob_sanity(self):
        resource_id, _ = self.RR.create(IonObject(RT.Resource, name="foo"))

        MY_CONTENT = "the quick brown fox etc etc etc"

        #save
        att_id = self.RR.create_attachment(resource_id,  IonObject(RT.Attachment,
                                                          name="test.txt",
                                                          content=MY_CONTENT,
                                                          content_type="text/plain",
                                                          keywords=["test1", "test2"],
                                                          attachment_type=AttachmentType.BLOB))
        
        #load
        attachment = self.RR.read_attachment(att_id, include_content=True)
        self.assertEqual("test.txt", attachment.name)
        self.assertEqual("text/plain", attachment.content_type)
        self.assertIn("test1", attachment.keywords)
        self.assertIn("test2", attachment.keywords)

        #content has changed; it's base64-encoded from what we put in
        self.assertEqual(MY_CONTENT, attachment.content)

        obj = self.RR.read(resource_id)
        self.assertEqual(obj.name, "foo")
        obj.name = "TheDudeAbides"
        obj = self.RR.update(obj)
        obj = self.RR.read(resource_id)
        self.assertEqual(obj.name, "TheDudeAbides")

        att = self.RR.find_attachments(resource_id)
        self.assertNotEqual(att, None)


        actor_identity_obj = IonObject("ActorIdentity", name="name")
        actor_identity_obj_id, actor_identity_obj_rev = self.RR.create(actor_identity_obj)
        user_info_obj = IonObject("UserInfo", name="name")
        user_info_obj_id, user_info_obj_rev = self.RR.create(user_info_obj)
        assoc_id, assoc_rev = self.RR.create_association(actor_identity_obj_id, PRED.hasInfo, user_info_obj_id)
        self.assertNotEqual(assoc_id, None)

        find_assoc = self.RR.find_associations(actor_identity_obj_id, PRED.hasInfo, user_info_obj_id)
        self.assertTrue(find_assoc[0]._id == assoc_id)
        subj = self.RR.find_subjects(RT.ActorIdentity, PRED.hasInfo, user_info_obj_id, True)

        res_obj1 = self.RR.read_object(actor_identity_obj_id, PRED.hasInfo, RT.UserInfo)
        self.assertEquals(res_obj1._id, user_info_obj_id)

        self.RR.delete_association(assoc_id)
        self.RR.delete_attachment(att_id)
        self.RR.delete(resource_id)
