#!/usr/bin/env python

"""IA mock for DA demo"""

__author__ = 'Bill Bollenbacher'
__license__ = 'Apache 2.0'

from pyon.agent.agent import ResourceAgent, UserAgent
from ion.services.sa.direct_access.direct_access_server import DirectAccessServer, directAccessTypes
import gevent

daServer = None
lostConnection = False
    
def telnetInputProcessor(data):
    # callback passed to DA Server for receiving input server
    global daServer, lostConnection
    
    if isinstance(data, int):
        # not character data, so check for lost connection
        if data == -1:
            print ("InstAgentMock.telnetInputProcessor: connection lost")
            lostConnection = True
        else:
            print ("InstAgentMock.telnetInputProcessor: got unexpected integer " + str(data))
        return
    #print ("InstAgentMock.telnetInputProcessor: data = " + str(data))
    daServer.send("InstAgentMock.telnetInputProcessor() rcvd: " + data + chr(10))

class InstAgentMock(UserAgent):
    
    def on_start(self):
        global daServer
        
        print ("InstAgentMock.on_start(): entering")
        daServer = DirectAccessServer(directAccessTypes.telnet, telnetInputProcessor)
        connectionInfo = daServer.getConnectionInfo()
        print ("InstAgentMock: telnet DA Server connection info = " + str(connectionInfo))
        while True:
            if lostConnection:
                break
            gevent.sleep(1)
        daServer.stop()
        del daServer
        print ("InstAgentMock.on_start(): leaving")

