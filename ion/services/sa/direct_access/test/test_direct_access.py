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
import sys, pprint, time
from pyon.util.log import log

from ion.services.sa.direct_access.direct_access_server import DirectAccessServer, directAccessTypes


@attr('INT', group='sa')
#@unittest.skip('not working')
class Test_DirectAccessServer_Integration(IonIntegrationTestCase):

    def test_directAccessServer(self):
        """
        # Start container
        #print 'starting container'
        self._start_container()
        #print 'started container'

        # Establish endpoint with container
        container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)
        #print 'got CC client'
        container_client.start_rel_from_url('res/deploy/r2sa.yml')
        print 'started services'
        """
        pprint.pprint(sys.path)

        
if __name__ == '__main__':

    "For command line testing - Accept a single connection"
    daServer = None
                
    def inputProcessor(data):
        print ("inputProcessor: data = " + str(data))
        print ("len of data = " + str(len(data)))
        daServer.write("Test.inputProcessor() rcvd: " + data + chr(10))
        
    print ("instatiating a DA Server with username=admin & password=123")

    daServer = DirectAccessServer(directAccessTypes.telnet, inputProcessor)
    print ("started the DA Server")

    connectionInfo = daServer.getConnectionInfo()
    print ("connection info = " + str(connectionInfo))
    time.sleep(300)
    