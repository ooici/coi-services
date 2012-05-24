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
CQAD/v67T1D/u091eAsAAQToAwAABOgDAABjY2MKUEsDBBQAAgAIADc5uEAlKiTAiAAAAAIBAAAM
ABwATUFOSUZFU1QuY3N2VVQJAAP5Fr5P+Ra+T3V4CwABBOgDAAAE6AMAAI3MMQ6DMAxG4T2niDL/
aoeeoO1BKseYEjUkkTEFbl/UjY3lLU/6+pSl0Cj4p5OJNTVLtYBrMSn2sq0JPrItVbvJ0cVWA8+q
+8ubn8s8SYebr723QXwWM1FPMFnt2jKlgkC4g3IbaG9PwcVzSDwgEQ9EMUJU+tbg+BzCB4TxxJvG
kcADaU4S3A9QSwECHgMKAAIAAABYiLZAlV34dwQAAAAEAAAABQAYAAAAAAABAAAApIEAAAAAYS50
eHRVVAUAA/f+u091eAsAAQToAwAABOgDAABQSwECHgMKAAIAAABaiLZA4R8mTAQAAAAEAAAABQAY
AAAAAAABAAAApIFDAAAAYi50eHRVVAUAA/v+u091eAsAAQToAwAABOgDAABQSwECHgMKAAIAAABb
iLZA8iND7AQAAAAEAAAABQAYAAAAAAABAAAApIGGAAAAYy50eHRVVAUAA/7+u091eAsAAQToAwAA
BOgDAABQSwECHgMUAAIACAA3ObhAJSokwIgAAAACAQAADAAYAAAAAAABAAAApIHJAAAATUFOSUZF
U1QuY3N2VVQFAAP5Fr5PdXgLAAEE6AMAAAToAwAAUEsFBgAAAAAEAAQAMwEAAJcBAAAAAA==
"""


BASE64_EGG = """
UEsDBBQAAAAIAPtOuEASJdDPBQEAAHMBAAARABwARUdHLUlORk8vUEtHLUlORk9VVAkAA+o9vk8f
tL5PdXgLAAEE6AMAAAToAwAAXZDBbsIwDIbveQo/AGlhHJByGlORkIBuGmM7u4nXRmriKkmH+vYL
Zdthlk/27++3faKEBhPKdwrRslewKlaiRkcKImFjg5GxofUmukYyW82BxJ92mbXn0TkMk4LzXQ7r
zfn0BFWwXxTEnh3JAduM61IaoirL1qZubArNrrwBbZnHrSdpfaI2YMrkKLZj6jgo2JkrZuZ+zM3w
U5Xk0PYKyHRz+XHU0RRkRnG0mnzMXtsBdUfwUCxFxVffMxp5eT3el8g7zMaFp1QG6gkjRVFR1MEO
aT7sN946GyEnQs++BQH/QhxounIwUcGMXOTp24sWw8R+odmKlx7TJwen4FIf6uePWnwDUEsBAh4D
FAAAAAgA+064QBIl0M8FAQAAcwEAABEAGAAAAAAAAQAAAKSBAAAAAEVHRy1JTkZPL1BLRy1JTkZP
VVQFAAPqPb5PdXgLAAEE6AMAAAToAwAAUEsFBgAAAAABAAEAVwAAAFABAAAAAA==
"""

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

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.RR  = ResourceRegistryServiceClient(node=self.container.node)
        self.IMS = InstrumentManagementServiceClient(node=self.container.node)

        print 'started services'

    @unittest.skip('this test just for debugging setup')
    def test_just_the_setup(self):
        return

    def test_register_instrument_agent(self):

        inst_agent_id = self.IMS.create_instrument_agent(any_old(RT.InstrumentAgent))

        self.IMS.register_instrument_agent(inst_agent_id, BASE64_EGG, BASE64_ZIPFILE)

        attachments, _ = self.RR.find_objects(inst_agent_id, PRED.hasAttachment, RT.Attachment, True)

        self.assertEqual(len(attachments), 3)

        for a_id in attachments:

            a = self.RR.read_attachment(a_id)

            parts = string.split(a.name, ".")

            self.assertEqual("txt", parts[1])
            self.assertEqual("text/plain", a.content_type)
            
            self.assertIn(parts[0], a.keywords)

            self.assertEqual(a.content, (parts[0] * 3) + "\n")

            

        return
