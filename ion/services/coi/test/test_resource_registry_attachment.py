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

        self.container.start_rel_from_url('res/deploy/r2deploy_no_bootstrap.yml')
        self.RR  = ResourceRegistryServiceClient(node=self.container.node)

        print 'started services'

    @unittest.skip("demonstration of RR error")
    def test_resource_registry_blob_sanity(self):
        resource_id, _ = self.RR.create(IonObject(RT.Resource, name="foo"))

        MY_CONTENT = "the quick brown fox etc etc etc"

        #save
        self.RR.create_attachment(resource_id,  IonObject(RT.Attachment,
                                                          name="test.txt",
                                                          content=MY_CONTENT,
                                                          content_type="text/plain",
                                                          keywords=["test1", "test2"],
                                                          attachment_type=AttachmentType.BLOB))
        
        #load
        attachments, _ = self.RR.find_objects(resource_id, PRED.hasAttachment, RT.Attachment, False)
        self.assertEqual(1, len(attachments))
        att = attachments[0]
        self.assertEqual("test.txt", att.name)
        self.assertEqual("text/plain", att.content_type)
        self.assertIn("test1", att.keywords)
        self.assertIn("test2", att.keywords)

        #content has changed; it's base64-encoded from what we put in
        self.assertEqual(MY_CONTENT, att.content)

