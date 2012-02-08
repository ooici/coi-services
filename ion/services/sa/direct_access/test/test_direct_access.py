from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container, IonObject
from pyon.util.int_test import IonIntegrationTestCase
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, LCS, PRED
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
import sys, pprint, time, types, select
from pyon.util.log import log

from ion.services.sa.direct_access.direct_access_server import DirectAccessServer, DirectAccessTypes


@attr('INT', group='sa')
@unittest.skip("not working; container doesn't start properly")
class Test_DirectAccessServer_Integration(IonIntegrationTestCase):

    def setUp(self):

        # Start container
        #print 'starting container'
        self._start_container()
        #print 'started container'

        # Establish endpoint with container
        container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)
        #print 'got CC client'
        container_client.start_rel_from_url('res/deploy/examples/ia_mock.yml')
        print 'started services'

        self.container_client = container_client

    def test_just_the_setup(self):
        """
        does nothing on its own, but will implicitly call setUp and 
           trigger any errors that might be in there
        """
        pass

    @unittest.skip('not completed')
    def test_direct_access_server(self):
        #time.sleep(1)
        print("quitting test")

