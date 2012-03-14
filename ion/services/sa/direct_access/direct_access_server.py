#!/usr/bin/env python

__author__ = 'Bill Bollenbacher'
__license__ = 'Apache 2.0'

from pyon.util.log import log
from pyon.core.exception import ServerError

from ion.services.sa.direct_access.ion_telnet_server import TelnetServer

class DirectAccessTypes:
    (telnet, vsp, ssh) = range(1, 4)
    

class DirectAccessServer(object):
    """
    Class for direct access server that interfaces to an IA ResourceAgent
    """
    
    server = None
    
    def __init__(self, direct_access_type=None, input_callback=None, ip_address=None):
        log.debug("DirectAccessServer.__init__()")

        if not direct_access_type:
            log.warning("DirectAccessServer.__init__(): direct access type not specified")
            raise ServerError("DirectAccessServer.__init__(): direct access type not specified")

        if not input_callback:
            log.warning("DirectAccessServer.__init__(): callback not specified")
            raise ServerError("DirectAccessServer.__init__(): callback not specified")
               
        if not ip_address:
            log.warning("DirectAccessServer.__init__(): IP address not specified")
            raise ServerError("DirectAccessServer.__init__(): IP address not specified")
               
        # start the correct server based on direct_access_type
        if direct_access_type == DirectAccessTypes.telnet:
            self.server = TelnetServer(input_callback, ip_address)
        else:
            raise ServerError("DirectAccessServer.__init__(): Unsupported direct access type")
        
        
    def get_connection_info(self):
        return self.server.get_connection_info()
    
    def stop(self):
        log.debug("DirectAccessServer.stop()")
        self.server.stop()
        del self.server
        
    def send(self, data):
        log.debug("DirectAccessServer.send(): data = " + str(data))
        self.server.send(data)
        
        
        
