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
        attachment = self.RR.read_attachment(att_id)
        self.assertEqual("test.txt", attachment.name)
        self.assertEqual("text/plain", attachment.content_type)
        self.assertIn("test1", attachment.keywords)
        self.assertIn("test2", attachment.keywords)

        #content has changed; it's base64-encoded from what we put in
        self.assertEqual(MY_CONTENT, attachment.content)

