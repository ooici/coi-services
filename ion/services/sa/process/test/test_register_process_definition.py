#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.ion.endpoint import ProcessRPCClient

from ion.util.module_uploader import RegisterModulePreparerPy
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.services.sa.process.data_process_management_service import DataProcessManagementService
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient

from pyon.core.exception import BadRequest
#from pyon.public import RT, PRED, LCS
from pyon.public import CFG
from mock import Mock
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest

import subprocess
import os
import pwd
from urllib2 import urlopen, urlparse

"""
contents of the file encoded below:

class Fake(object):
    def __init__(self):
        print "hello world"

"""

BASE64_PYFILE = """
WTJ4aGMzTWdSbUZyWlNodlltcGxZM1FwT2dvZ0lDQWdaR1ZtSUY5ZmFXNXBkRjlmS0hObGJH
WXBPZ29nSUNBZ0lDQWdJSEJ5DQphVzUwSUNKb1pXeHNieUIzYjNKc1pDSUsNCg==
"""

"""
contents fo the file encoded below:

class class
"""

BASE64_BADPYFILE = """
2xhc3MgY2xhc3MK
"""

@attr('UNIT', group='sa')
class TestRegisterProcessDefinition(PyonTestCase):

    def setUp(self):
        self.mock_ionobj = self._create_IonObject_mock('ion.services.sa.process.data_process_management_service.IonObject')

        #self.mock_ionobj = IonObject
        self.mock_clients = self._create_service_mock('data_process_management')

        self.RR = self.mock_clients.resource_registry
        self.DPMS = DataProcessManagementService()
        self.DPMS.clients = self.mock_clients


        # must call this manually
        self.DPMS.on_init()

        self.DPMS.module_uploader = RegisterModulePreparerPy(dest_user="my_user",
                                                             dest_host="my_host",
                                                             dest_path="/my/remote/wwwroot/my/path",
                                                             dest_wwwprefix="http://my_host/my/path")

        self.addCleanup(delattr, self, "DPMS")
        self.addCleanup(delattr, self, "mock_ionobj")
        #self.resource_impl_cleanup()

        #def resource_impl_cleanup(self):
        #pass


    def _mock_uploader_modules(self, emulate_success):
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

        self.DPMS.module_uploader.modules = self.mock_dict


    def test_register_data_process_definition_unit(self):
        # setup

        self.DPMS.get_unique_id = (lambda: "my_uuid")

        print "Willing mock with bad py file"
        self._mock_uploader_modules(True)
        self.assertRaises(BadRequest, self.DPMS.register_data_process_definition, BASE64_BADPYFILE)

        print "Unwilling mock with good py file"
        self._mock_uploader_modules(False)
        self.assertRaises(BadRequest, self.DPMS.register_data_process_definition, BASE64_PYFILE)

        print "Willing mock with good py file"
        self._mock_uploader_modules(True)
        uri = self.DPMS.register_data_process_definition(BASE64_PYFILE)

        scp_dest = "my_user@my_host:/my/remote/wwwroot/my/path/process_code_my_uuid.py"
        self.mock_dict["subprocess"].Popen.assert_any_call(["scp", "-v", "-o", "PasswordAuthentication=no",
                                                            "-o", 'StrictHostKeyChecking=no',
                                                            'my_tempfile_name',
                                                            scp_dest],
                                                           stdout=self.mock_dict["subprocess"].PIPE,
                                                           stderr=self.mock_dict["subprocess"].PIPE)


        self.assertEqual("http://my_host/my/path/process_code_my_uuid.py", uri)





@attr('INT', group='sa')
class TestRegisterProcessDefinitionIntegration(IonIntegrationTestCase):

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
        self.DPMS = DataProcessManagementServiceClient(node=self.container.node)

        print 'started services'

#    @unittest.skip('this test just for debugging setup')
#    def test_just_the_setup(self):
#        return

    def test_register_data_process_definition_int(self):

        self.assertRaises(BadRequest, self.DPMS.register_data_process_definition, BASE64_BADPYFILE)

        #test ssh-ability
        cfg_host = CFG.service.data_process_management.process_release_host #'amoeaba.ucsd.edu'
        cfg_user = pwd.getpwuid(os.getuid())[0]

        if "driver_release_user" in CFG.service.data_process_management:
            cfg_user = CFG.service.data_process_management.process_release_user

        cfg_remotepath = CFG.service.data_process_management.process_release_directory

        remotehost = "%s@%s" % (cfg_user, cfg_host)

        ssh_retval = subprocess.call(["ssh", "-o", "PasswordAuthentication=no",
                                      "-o", "StrictHostKeyChecking=no",
                                      remotehost, "-f", "true"],
                                    stdout=open(os.devnull),
                                    stderr=open(os.devnull))
        
        if 0 != ssh_retval:
            raise unittest.SkipTest("SSH/SCP credentials to %s didn't work" % remotehost)


        remote_url = self.DPMS.register_data_process_definition(BASE64_PYFILE)

        def delete_remote_file():
            remote_filename = os.path.basename(urlparse.urlsplit(remote_url)[2])
            remote_file = os.path.join(cfg_remotepath, remote_filename)

            ssh_retval = subprocess.call(["ssh", "-o", "PasswordAuthentication=no",
                                          "-o", "StrictHostKeyChecking=no",
                                          remotehost, "-f", "rm", "-f", remote_file],
                                          stdout=open(os.devnull),
                                          stderr=open(os.devnull))
            if 0 != ssh_retval:
                print "Failed to delete remote file"

        self.addCleanup(delete_remote_file)

        failmsg = ""
        try:
            code = urlopen(remote_url).code
            if 400 <= code:
                failmsg = "HTTP code %s" % code
        except Exception as e:
            failmsg = str(e)

        if failmsg:
            self.fail(("Uploaded succeeded, but fetching '%s' failed with '%s'.") %
                      (remote_url, failmsg))

        return
