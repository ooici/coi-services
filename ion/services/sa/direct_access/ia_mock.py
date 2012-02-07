#!/usr/bin/env python

"""IA mock for DA demo"""

__author__ = 'Bill Bollenbacher'
__license__ = 'Apache 2.0'

from pyon.agent.agent import ResourceAgent, UserAgent
from ion.services.sa.direct_access.direct_access_server import DirectAccessServer, directAccessTypes
import gevent

    
class InstAgentMock(UserAgent):
    
    daServer = None
    lostConnection = False
    quit = False
    
    def telnetInputProcessor(self, data):
        # callback passed to DA Server for receiving input server       
        if isinstance(data, int):
            # not character data, so check for lost connection
            if data == -1:
                print ("InstAgentMock.telnetInputProcessor: connection lost")
                self.lostConnection = True
            else:
                print ("InstAgentMock.telnetInputProcessor: got unexpected integer " + str(data))
            return
        print ("InstAgentMock.telnetInputProcessor: data = " + str(data) + " len=" + str(len(data)))
        self.daServer.send("InstAgentMock.telnetInputProcessor() rcvd: " + data + chr(10))
        if data == 'quit':
            self.quit = True
    
    def on_start(self):        
        print ("InstAgentMock.on_start(): entering")
        while True:
            self.daServer = DirectAccessServer(directAccessTypes.telnet, self.telnetInputProcessor)
            connectionInfo = self.daServer.getConnectionInfo()
            print ("InstAgentMock: telnet DA Server connection info = " + str(connectionInfo))
            while True:
                if self.lostConnection:
                    self.lostConnection = False
                    break
                gevent.sleep(1)
            self.daServer.stop()
            del self.daServer
            if self.quit == True:
                break
        print ("InstAgentMock.on_start(): leaving")

