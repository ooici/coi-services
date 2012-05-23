#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.ion.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.services.sa.instrument.instrument_management_service import InstrumentManagementService
from interface.services.sa.iinstrument_management_service import IInstrumentManagementService, InstrumentManagementServiceClient
from interface.objects import AttachmentType

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, PRED, LCS
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
from pyon.util.log import log

import string
import base64

from ion.services.sa.test.helpers import any_old

BASE64_ZIPFILE = """
UEsDBAoAAgAAAFiItkCVXfh3BAAAAAQAAAAFABwAYS50eHRVVAkAA/f+u09Q/7tPdXgLAAEE6AMA
AAToAwAAYWFhClBLAwQKAAIAAABaiLZA4R8mTAQAAAAEAAAABQAcAGIudHh0VVQJAAP7/rtPUP+7
T3V4CwABBOgDAAAE6AMAAGJiYgpQSwMECgACAAAAW4i2QPIjQ+wEAAAABAAAAAUAHABjLnR4dFVU
CQAD/v67T1D/u091eAsAAQToAwAABOgDAABjY2MKUEsDBBQAAgAIAGSltkC2NAzcWgAAAIQAAAAM
ABwATUFOSUZFU1QuY3N2VVQJAAOcMrxPnDK8T3V4CwABBOgDAAAE6AMAAGXKuQ2AMBAEwNxlOF5B
DUAhaG2OR/iTOfF0DznJRJMYBT4nlaSjPkWwy3PlOh2Gjd4KlVvbErglWKIDQ1n5OdMa9xsOPZwo
4SrPbI3/DY8BC2Mk/MoaNrHmBVBLAQIeAwoAAgAAAFiItkCVXfh3BAAAAAQAAAAFABgAAAAAAAEA
AACkgQAAAABhLnR4dFVUBQAD9/67T3V4CwABBOgDAAAE6AMAAFBLAQIeAwoAAgAAAFqItkDhHyZM
BAAAAAQAAAAFABgAAAAAAAEAAACkgUMAAABiLnR4dFVUBQAD+/67T3V4CwABBOgDAAAE6AMAAFBL
AQIeAwoAAgAAAFuItkDyI0PsBAAAAAQAAAAFABgAAAAAAAEAAACkgYYAAABjLnR4dFVUBQAD/v67
T3V4CwABBOgDAAAE6AMAAFBLAQIeAxQAAgAIAGSltkC2NAzcWgAAAIQAAAAMABgAAAAAAAEAAACk
gckAAABNQU5JRkVTVC5jc3ZVVAUAA5wyvE91eAsAAQToAwAABOgDAABQSwUGAAAAAAQABAAzAQAA
aQEAAAAA
"""



BASE64_EGG = "ZWdn"

@attr('INT', group='sa')
class TestInstrumentManagementServiceAgents(IonIntegrationTestCase):

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
        self.IMS = InstrumentManagementServiceClient(node=self.container.node)

        print 'started services'

    @unittest.skip('this test just for debugging setup')
    def test_just_the_setup(self):
        return

    def test_register_instrument_agent(self):

        inst_agent_id = self.IMS.create_instrument_agent(any_old(RT.InstrumentAgent))

        self.IMS.register_instrument_agent(inst_agent_id, BASE64_EGG, BASE64_ZIPFILE)

        attachments, _ = self.RR.find_objects(inst_agent_id, PRED.hasAttachment, RT.Attachment, False)

        self.assertEqual(len(attachments), 3)

        for a in attachments:
            parts = string.split(a.name, ".")

            self.assertEqual("txt", parts[1])
            self.assertEqual("text/plain", a.content_type)
            
            self.assertIn(parts[0], a.keywords)

            self.assertEqual(base64.decodestring(a.content), parts[0] * 3)

            

        return
