#!/usr/bin/env python

__author__ = 'Bill Bollenbacher'
__license__ = 'Apache 2.0'

from pyon.util.log import log
from pyon.core.exception import ServerError

from ion.services.sa.direct_access.telnet_server import TelnetServer

class directAccessTypes:
    (telnet, vsp, ssh) = range(0, 3)
    

class DirectAccessServer(object):
    """
    Class for direct access server that interfaces to an IA ResourceAgent
    """
    
    server = None
    parentInputCallback = None
    
    def __init__(self, direct_access_type=None, inputCallback=None):
        log.debug("DirectAccessServer.__init__()")
        if not inputCallback:
            log.warning("DirectAccessServer.__init__(): callback not specified")
            raise ServerError("callback not specified")
        self.parentInputCallback = inputCallback
        
        if not direct_access_type:
            log.warning("DirectAccessServer.__init__(): direct access type not specified")
            raise ServerError("direct access type not specified")
        if direct_access_type == directAccessTypes.telnet:
            self.server, ip_address, port = TelnetServer(self.parentInputCallback, 'admin', '123')
        return ip_address, port
    
    def stop(self):
        log.debug("DirectAccessServer.stop()")
        self.server.stop()
        
    def write(self, data):
        self.server.write(data)
        
        
        
