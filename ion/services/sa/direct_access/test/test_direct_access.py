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
        # Start container
        #print 'starting container'
        self._start_container()
        #print 'started container'

        # Establish endpoint with container
        container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)
        #print 'got CC client'
        container_client.start_rel_from_url('res/deploy/examples/ia_mock.yml')
        print 'started services'
        #time.sleep(1)
        print("quitting test")

        
if __name__ == '__main__':
    # For command line testing of telnet DA Server 
    # Accepts a single telnet connection at a time

    daServer = None
    lostConnection = False
    inputData = ''
    gotData = False
                
    def isThereUserInput():
        # check for input on stdin
        return select.select([sys.stdin], [], [], 0) == ([sys.stdin], [], [])
    
    def telnetInputProcessor(data):
        # callback passed to DA Server for receiving input server
        global lostConnection, inputData, gotData
        
        if isinstance(data, int):
            # not character data, so check for lost connection
            if data == -1:
                print ("telnetInputProcessor: connection lost")
                lostConnection = True
            else:
                print ("telnetInputProcessor: got unexpected integer " + str(data))
            return
        print ("telnetInputProcessor: data = " + str(data))
        #print ("len of data = " + str(len(data)))
        inputData = data
        gotData = True
        
    while True:
        # prompt for starting server or exiting test
        while True:
            print (chr(10) + "'start' to start a telnet DA Server, 'exit' to end the test")
            line = sys.stdin.readline()
            if line == 'exit'+chr(10):
                print ("DA Server test: exiting")
                exit(0)
            if line == 'start'+chr(10):
                break
        
        #print ("DA Server test: instantiating a telnet DA Server")
        # start the DA Server using telnet protocol and pass in the callback routine   
        daServer = DirectAccessServer(directAccessTypes.telnet, telnetInputProcessor)
        #print ("DA Server test: started the telnet DA Server")
    
        # get the connection info from the server
        connectionInfo = daServer.getConnectionInfo()
        print ("DA Server test: telnet DA Server connection info = " + str(connectionInfo))
        
        print (chr(10) + "'stop' to stop the telnet DA Server")
        # loop until user enters 'stop' or the connection is lost
        while True:
            # check for user input
            if isThereUserInput():
                line = sys.stdin.readline()
                if line == 'stop'+chr(10):
                    # user entered 'stop', so stop the server
                    daServer.stop()
                    del daServer
                    lostConnection = False
                    gotData = False
                    print ("DA Server test: telnet DA Server stopped")
                    break
            # check for input from server
            if gotData:
                # echo received data back to server and onto the telnet client
                daServer.write("DA Server test.telnetInputProcessor() rcvd: " + inputData + chr(10))
                gotData = False
            # check for lost connection
            if lostConnection:
                daServer.stop()
                del daServer
                lostConnection = False
                break
            time.sleep(.5)   # let other code run
    
    