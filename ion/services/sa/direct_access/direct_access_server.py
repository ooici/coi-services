#!/usr/bin/env python

__author__ = 'Bill Bollenbacher'
__license__ = 'Apache 2.0'

from pyon.util.log import log
from pyon.core.exception import ServerError

from ion.services.sa.direct_access.telnet_server import TelnetServer

class directAccessTypes:
    (telnet, vsp, ssh) = range(1, 4)
    

class DirectAccessServer(object):
    """
    Class for direct access server that interfaces to an IA ResourceAgent
    """
    
    server = None
    
    def __init__(self, direct_access_type=None, inputCallback=None):
        log.debug("DirectAccessServer.__init__()")
        if not inputCallback:
            log.warning("DirectAccessServer.__init__(): callback not specified")
            raise ServerError("callback not specified")
        
        if not direct_access_type:
            log.warning("DirectAccessServer.__init__(): direct access type not specified")
            raise ServerError("direct access type not specified")
        
        # start the correct server based on direct_access_type
        if direct_access_type == directAccessTypes.telnet:
            self.server = TelnetServer(inputCallback)
        else:
            raise ServerError("Unsupported direct access type")
        
        
    def getConnectionInfo(self):
        return self.server.getConnectionInfo()
    
    def stop(self):
        log.debug("DirectAccessServer.stop()")
        self.server.stop()
        del self.server
        
    def write(self, data):
        log.debug("DirectAccessServer.write(): data = " + str(data))
        self.server.write(data)
        
        
        
