from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, LCS, PRED
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
from ion.services.sa.direct_access.direct_access_server import DirectAccessServer


@attr('INT', group='sa')
#@unittest.skip('not working')
class Test_DirectAccessServer_Integration(IonIntegrationTestCase):

    def test_directAccessServer(self):
        # Start container
        #print 'starting container'
        self._start_container()
        #print 'started container'

        # Establish endpoint with container
        container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)
        #print 'got CC client'
        container_client.start_rel_from_url('res/deploy/r2sa.yml')
        print 'started services'

        container_client.spawn_process('DA_Server', 'ion.services.sa.direct_access.direct_access_server', 'DA_SERVER', process_type='immediate')
