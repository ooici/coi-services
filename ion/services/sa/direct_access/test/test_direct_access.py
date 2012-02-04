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
    # For command line testing of telnet DA Server 
    # Accepts a single telnet connection at a time

    daServer = None
    lostConnection = False
    inputData = ''
    gotData = False
                
    def isThereUserInput():
        return select.select([sys.stdin], [], [], 0) == ([sys.stdin], [], [])
    
    def telnetInputProcessor(data):
        global lostConnection, inputData, gotData
        
        print ("telnetInputProcessor: data = " + str(data))
        if isinstance(data, int):
            if data == -1:
                print ("connection lost")
                lostConnection = True
            else:
                print ("got unexpected integer " + str(data))
            return
        print ("len of data = " + str(len(data)))
        inputData = data
        gotData = True
        
    while True:
        while True:
            print ("'start' to start a telnet DA Server, 'exit' to end the test")
            line = sys.stdin.readline()
            if line == 'exit'+chr(10):
                print ("DA Server test: exiting")
                exit(0)
            if line == 'start'+chr(10):
                break
        
        print ("DA Server test: instantiating a telnet DA Server")   
        daServer = DirectAccessServer(directAccessTypes.telnet, telnetInputProcessor)
        print ("DA Server test: started the telnet DA Server")
    
        connectionInfo = daServer.getConnectionInfo()
        print ("DA Server test: telnet DA Server connection info = " + str(connectionInfo))
        
        print ("'stop' to stop the telnet DA Server")
        while True:
            if isThereUserInput():
                line = sys.stdin.readline()
                if line == 'stop'+chr(10):
                    daServer.stop()
                    del daServer
                    lostConnection = False
                    gotData = False
                    print ("DA Server test: telnet DA Server stopped")
                    break
            if gotData:
                daServer.write("DA Server test.telnetInputProcessor() rcvd: " + inputData + chr(10))
                gotData = False
            if lostConnection:
                daServer.stop()
                del daServer
                break
            time.sleep(.5)
    
    