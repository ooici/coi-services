#!/usr/bin/env python

"""IA mock for DA demo"""

__author__ = 'Bill Bollenbacher'
__license__ = 'Apache 2.0'

from pyon.agent.agent import ResourceAgent, UserAgent
from ion.services.sa.direct_access.direct_access_server import DirectAccessServer, DirectAccessTypes
import gevent

    
class InstAgentMock(UserAgent):
    
    da_server = None
    lost_connection = False
    quit = False
    
    def telnet_input_processor(self, data):
        # callback passed to DA Server for receiving input server       
        if isinstance(data, int):
            # not character data, so check for lost connection
            if data == -1:
                print ("InstAgentMock.telnetInputProcessor: connection lost")
                self.lost_connection = True
            else:
                print ("InstAgentMock.telnetInputProcessor: got unexpected integer " + str(data))
            return
        print ("InstAgentMock.telnetInputProcessor: data = " + str(data) + " len=" + str(len(data)))
        self.da_server.send("InstAgentMock.telnetInputProcessor() rcvd: " + data + chr(10))
        if data == 'quit':
            self.quit = True
            
    def server(self):
        print ("InstAgentMock.server(): entering")
        while True:
            self.da_server = DirectAccessServer(DirectAccessTypes.telnet, self.telnet_input_processor)
            connection_info = self.da_server.get_connection_info()
            print ("InstAgentMock: telnet DA Server connection info = " + str(connection_info))
            while True:
                if self.lost_connection:
                    self.lost_connection = False
                    break
                gevent.sleep(1)
            self.da_server.stop()
            del self.da_server
            if self.quit == True:
                break
        setattr(self.container, 'ia_mock_quit', True)
        print ("InstAgentMock.server(): leaving")
   
    def on_start(self):        
        print ("InstAgentMock.on_start(): entering")
        gevent.spawn(self.server)
        print ("InstAgentMock.on_start(): leaving")

