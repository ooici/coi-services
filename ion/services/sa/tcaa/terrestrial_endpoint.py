#!/usr/bin/env python
"""
@package 
@file 
@author Edward Hunter
@brief 
"""

__author__ = 'Edward Hunter'

__license__ = 'Apache 2.0'

from interface.services.sa.iterrestrial_endpoint import BaseTerrestrialEndpoint
from interface.services.sa.iterrestrial_endpoint import TerrestrialEndpointProcessClient
from ion.services.sa.tcaa.r3pc import R3PCServer
from ion.services.sa.tcaa.r3pc import R3PCClient

class TerrestrialEndpoint(BaseTerrestrialEndpoint):
    """
    """
    def __init__(self):
        """
        """
        #self._server = R3PCServer(self._req_callback, self._server_close_callback)
        #self._client = R3PCClient(self._ack_callback, self._client_close_callback)
        #self._terrestrial_port = None

    def on_init(self):
        """
        """
        pass
    
    def on_start(self):
        """
        """
        # Set up a subscriber to link events.
        
        # Start server, including bind to random port.
        #self._terrestrial_port = self._server.start('*', 0)
        pass
    
    def on_stop(self):
        """
        """        
        # Close client and server.
        #self._server.stop()
        #self._client.stop()
        #self._terrestrial_port = None
        pass
    
    def _req_callback(self, request):
        """
        """
        pass
    
    def _server_close_callback(self):
        """
        """
        pass
    
    def _ack_callback(self):
        """
        """
        pass
    
    def _client_close_callback(self):
        """
        """
        pass
        
    def enqueue_command(self, command=None, link=False):
        """
        """
        print 'XXXXXXXX HELLO!'

    def get_queue(self, resource_id=''):
        """
        """
        pass

    def clear_queue(self, resource_id=''):
        """
        """
        pass

    def pop_queue(self, resource_id='', index=0):
        """
        """
        pass
    
    def get_port(self):
        """
        """
        #return self._terrestrial_port
        return 1234

class TerrestrialEndpointClient(TerrestrialEndpointProcessClient):
    """
    """
    pass
