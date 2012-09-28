#!/usr/bin/env python
"""
@package ion.services.sa.tcaa.remote_endpoint
@file ion/services/sa/tcaa/remote_endpoint.py
@author Edward Hunter
@brief 2CAA Remote endpoint.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Pyon log and config objects.
from pyon.public import log
from pyon.public import CFG

import uuid
import time

# Pyon exceptions.
from pyon.core.exception import BadRequest
from pyon.core.exception import Conflict

from pyon.event.event import EventPublisher, EventSubscriber
from interface.objects import TelemetryStatusType, RemoteCommand

from interface.services.sa.iremote_endpoint import BaseRemoteEndpoint
from interface.services.sa.iremote_endpoint import RemoteEndpointProcessClient
from ion.services.sa.tcaa.endpoint_mixin import EndpointMixin

class RemoteEndpoint(BaseRemoteEndpoint, EndpointMixin):
    """
    """
    def __init__(self, *args, **kwargs):
        """
        For framework level code only.
        """
        super(RemoteEndpoint, self).__init__(*args, **kwargs)
        
    ######################################################################    
    # Framework process lifecycle funcitons.
    ######################################################################    

    def on_init(self):
        """
        Application level initializer.
        Setup default internal values.
        """
        super(RemoteEndpoint, self).on_init()
        self.mixin_on_init()

    def on_start(self):
        """
        Process about to be started.
        """
        super(RemoteEndpoint, self).on_start()
        self.mixin_on_start()
        
    def on_stop(self):
        """
        Process about to be stopped.
        """
        self.mixin_on_stop()
        super(RemoteEndpoint, self).on_stop()
    
    def on_quit(self):
        """
        Process terminated following.
        """
        self.mixin_on_quit()
        super(RemoteEndpoint, self).on_quit()

    ######################################################################    
    # Callbacks.
    ######################################################################    

    def _req_callback(self, result):
        """
        """
        pass
    
    def _ack_callback(self, request):
        """
        """
        pass
    
    def _server_close_callback(self):
        """
        """
        pass
    
    def _client_close_callback(self):
        """
        """
        pass
    
    def _consume_telemetry_event(self, *args, **kwargs):
        """
        """
        log.debug('Telemetry event received by remote endpoint, args: %s, kwargs: %s',
                  str(args), str(kwargs))
        evt = args[0]
        self._link_status = evt.status
        if evt.status == TelemetryStatusType.AVAILABLE:
            log.debug('Remote endpoint telemetry available.')
            self._on_link_up()
            
        elif evt.status == TelemetryStatusType.UNAVAILABLE:
            log.debug('Remote endpoint telemetry not available.')
            self._on_link_down()
    
    ######################################################################    
    # Commands.
    ######################################################################    
    
    def get_port(self):
        """
        """
        return self._this_port
    
class RemoteEndpointClient(RemoteEndpointProcessClient):
    """
    Remote endpoint client.
    """
    pass

