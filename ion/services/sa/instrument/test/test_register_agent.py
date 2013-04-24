#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.ion.endpoint import ProcessRPCClient
import tempfile
from urllib2 import urlopen
from ion.util.module_uploader import RegisterModulePreparerEgg
from pyon.ion.resource import LCE
from pyon.public import Container, IonObject
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.services.sa.instrument.instrument_management_service import InstrumentManagementService
from interface.services.sa.iinstrument_management_service import IInstrumentManagementService, InstrumentManagementServiceClient
from interface.objects import AttachmentType

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, PRED, LCS
from pyon.public import CFG
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
from ooi.logging import log

import string
import subprocess
import os
import pwd

from ion.services.sa.test.helpers import any_old


"""
@qa_documents a base64-encoded zip file containing a MANIFEST.csv file


zip contents:
 a.txt
 b.txt
 c.txt
 MANIFEST.csv

MANIFEST.csv fields:
- filename
- name
- description
- content_type

text file contents: aaa, bbb, and ccc

MANIFEST.csv contents:

filename,name,description,content_type,keywords
a.txt,currently unused,3 of the letter a,text/plain,"a,A,alpha,alfa"
b.txt,currently unused,3 of the letter b,text/plain,"b,B,beta,bravo"
c.txt,currently unused,3 of the letter c,text/plain,"c,C,gamma,charlie"

"""
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

"""
@agent_egg a base64-encoded egg file

this contains only EGG-INFO/PKG-INFO, and in that file only the name and version fields matter
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



@attr('UNIT', group='sa')
class TestIMSRegisterAgent(PyonTestCase):

    def setUp(self):
        self.mock_ionobj = self._create_IonObject_mock('ion.services.sa.instrument.instrument_management_service.IonObject')

        #self.mock_ionobj = IonObject
        self.mock_clients = self._create_service_mock('instrument_management')

        self.RR = self.mock_clients.resource_registry
        self.IMS = InstrumentManagementService()
        self.IMS.clients = self.mock_clients


        # must call this manually
        self.IMS.on_init()

        self.IMS.module_uploader = RegisterModulePreparerEgg(dest_user="my_user",
                                                             dest_host="my_host",
                                                             dest_path="/my/remote/wwwroot/my/path",
                                                             dest_wwwprefix="http://my_host/my/path")

        self.addCleanup(delattr, self, "IMS")
        self.addCleanup(delattr, self, "mock_ionobj")
        #self.resource_impl_cleanup()

        #def resource_impl_cleanup(self):
        #pass


    def _mock_uploader_subprocess(self, emulate_success):
        self.mock_dict = {}

        popen_mock = Mock()
        popen_mock.communicate.return_value = ("scp_out dump", "scp_err dump")

        if emulate_success:
            popen_mock.returncode = 0
        else:
            popen_mock.returncode = 1

        subprocess_mock = Mock()
        subprocess_mock.Popen.return_value = popen_mock
        subprocess_mock.PIPE = "MYPIPE"

        tempfile_mock = Mock()
        tempfile_mock.mkstemp.return_value = ("my_handle", "my_tempfile_name")


        self.mock_dict["tempfile"]   = tempfile_mock
        self.mock_dict["subprocess"] = subprocess_mock
        self.mock_dict["os"]         = Mock()

        self.IMS.module_uploader.modules = self.mock_dict


    def test_register_instrument_agent_unit(self):
        # setup

        inst_agent_id = "iaid1"

        self._mock_uploader_subprocess(False)
        self.RR.read.return_value = any_old(RT.InstrumentAgent)
        self.assertRaises(BadRequest, self.IMS.register_instrument_agent, inst_agent_id, BASE64_EGG, BASE64_ZIPFILE)
        self.RR.read.assert_called_once_with(inst_agent_id, '')

        self._mock_uploader_subprocess(True)
        self.IMS.register_instrument_agent(inst_agent_id, BASE64_EGG, BASE64_ZIPFILE)
        self.RR.execute_lifecycle_transition.assert_called_once_with(inst_agent_id, LCE.INTEGRATE)

        remote_cred = "my_user@my_host"
        remote_path = "/my/remote/wwwroot/my/path/seabird_sbe37smb_ooicore-0.1-py2.7.egg"
        scp_dest = "%s:%s" % (remote_cred, remote_path)
        self.mock_dict["subprocess"].Popen.assert_called_any(["scp", "-v", "-o", "PasswordAuthentication=no",
                                                              "-o", 'StrictHostKeyChecking=no',
                                                              'my_tempfile_name',
                                                              scp_dest],
                                                               stdout=self.mock_dict["subprocess"].PIPE,
                                                               stderr=self.mock_dict["subprocess"].PIPE)

        self.mock_dict["subprocess"].Popen.assert_called_any(["ssh", remote_cred, "chmod", "664", remote_path],
                                                               stdout=self.mock_dict["subprocess"].PIPE,
                                                               stderr=self.mock_dict["subprocess"].PIPE)

        self.assertEqual(4, self.RR.create_attachment.call_count)

        url_content = "[InternetShortcut]\nURL=http://my_host/my/path/seabird_sbe37smb_ooicore-0.1-py2.7.egg"
        found = False
        for acall in self.mock_ionobj.call_args_list:
            if ("Attachment",) == acall[0]:
                self.assertEqual(url_content, acall[1]["content"])
                found = True
        self.assertTrue(found, "URL attachment not found")





@attr('INT', group='sa')
class TestIMSRegisterAgentIntegration(IonIntegrationTestCase):

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

#    @unittest.skip('this test just for debugging setup')
#    def test_just_the_setup(self):
#        return

    def test_register_instrument_agent_int(self):

        #test ssh-ability
        cfg_host = CFG.service.instrument_management.driver_release_host #'amoeaba.ucsd.edu'
        cfg_user = pwd.getpwuid(os.getuid())[0]

        if "driver_release_user" in CFG.service.instrument_management:
            cfg_user = CFG.service.instrument_management.driver_release_user

        remotehost = "%s@%s" % (cfg_user, cfg_host)

        ssh_retval = subprocess.call(["ssh", "-o", "PasswordAuthentication=no",
                                      "-o", "StrictHostKeyChecking=no",
                                      remotehost, "-f", "true"],
                                    stdout=open(os.devnull),
                                    stderr=open(os.devnull))
        
        if 0 != ssh_retval:
            raise unittest.SkipTest("SSH/SCP credentials to %s didn't work" % remotehost)



        inst_agent_id = self.IMS.create_instrument_agent(any_old(RT.InstrumentAgent))
        inst_model_id = self.IMS.create_instrument_model(any_old(RT.InstrumentModel))

        self.IMS.assign_instrument_model_to_instrument_agent(inst_model_id, inst_agent_id)

        self.IMS.register_instrument_agent(inst_agent_id, BASE64_EGG, BASE64_ZIPFILE)

        attachments, _ = self.RR.find_objects(inst_agent_id, PRED.hasAttachment, RT.Attachment, True)

        self.assertEqual(len(attachments), 4)

        for a_id in attachments:

            a = self.RR.read_attachment(a_id, include_content=True)

            parts = string.split(a.name, ".")

            # we expect a.txt to contain "aaa" and have a keyword listed called "a"
            if "txt" == parts[1]:
                self.assertEqual("text/plain", a.content_type)
                self.assertIn(parts[0], a.keywords)
                self.assertEqual(a.content, str(parts[0] * 3) + "\n")

            elif "text/url" == a.content_type:
                remote_url = a.content.split("URL=")[1]

                failmsg = ""
                try:
                    code = urlopen(remote_url).code
                    if 400 <= code:
                        failmsg = "HTTP code %s" % code
                except Exception as e:
                    failmsg = str(e)

                if failmsg:
                    self.fail(("Uploaded succeeded, but fetching '%s' failed with '%s'. ") %
                              (remote_url, failmsg))


        log.info("L4-CI-SA-RQ-148")
        log.info("L4-CI-SA-RQ-148: The test services shall ensure that test results are incorporated into physical resource metadata. ")
        log.info("L4-CI-SA-RQ-336: Instrument activation shall support the registration of Instrument Agents")

        # cleanup
        self.IMS.force_delete_instrument_agent(inst_agent_id)
        self.IMS.force_delete_instrument_model(inst_model_id)

        return
