#!/usr/bin/env python
"""
@package ion.services.sa.tcaa.endpoint_mixin
@file ion/services/sa/tcaa/endpoint_mixin.py
@author Edward Hunter
@brief 2CAA Terrestrial endpoint mixin.
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

from ion.services.sa.tcaa.r3pc import R3PCServer
from ion.services.sa.tcaa.r3pc import R3PCClient



class EndpointMixin(object):
    """
    """
    
    def __init__(self):
        """
        """
        pass
    
    def mixin_on_init(self):
        """
        """
        self._server = None
        self._client = None
        self._other_host = self.CFG.other_host
        self._other_port = self.CFG.other_port
        self._this_port = self.CFG.this_port
        self._platform_resource_id = self.CFG.platform_resource_id
        self._link_status = TelemetryStatusType.UNAVAILABLE
        self._event_subscriber = None
        self._server_greenlet = None
        self._publisher = EventPublisher()
    
    def mixin_on_start(self):
        """
        """
        self._server = R3PCServer(self._req_callback,
                                self._server_close_callback)
        self._client = R3PCClient(self._ack_callback,
                                self._client_close_callback)
        if self._this_port == 0:            
            self._this_port = self._server.start('*', self._this_port)
        else:
            self._server.start('*', self._this_port)
        log.debug('%s server binding to *:%i', self.__class__.__name__,
                  self._this_port)
    
        # Start the event subscriber.
        self._event_subscriber = EventSubscriber(
            event_type='PlatformTelemetryEvent',
            callback=self._consume_telemetry_event,
            origin=self._platform_resource_id)
        self._event_subscriber.start()
        self._event_subscriber._ready_event.wait(timeout=5)
    
    def mixin_on_quit(self):
        """
        """
        self._stop()
    
    def mixin_on_stop(self):
        """
        """
        self._stop()
    
    ######################################################################    
    # Helpers.
    ######################################################################    

    def _on_link_up(self):
        """
        Processing on link up event.
        Start client socket.
        ION link availability published when pending commands are transmitted.
        """
        log.debug('%s client connecting to %s:%i',
                    self.__class__.__name__,
                    self._other_host, self._other_port)
        self._client.start(self._other_host, self._other_port)
        self._publisher.publish_event(
                                event_type='PublicPlatformTelemetryEvent',
                                origin=self._platform_resource_id,
                                status=TelemetryStatusType.AVAILABLE)        

    def _on_link_down(self):
        """
        Processing on link down event.
        Stop client socket and publish ION link unavailability.
        """
        self._client.stop()
        self._publisher.publish_event(
                                event_type='PublicPlatformTelemetryEvent',
                                origin=self._platform_resource_id,
                                status=TelemetryStatusType.UNAVAILABLE)        
        
    def _stop(self):
        """
        Stop sockets and subscriber.
        """
        if self._event_subscriber:
            self._event_subscriber.stop()
            self._event_subscriber = None
        if self._server:
            self._server.stop()
            self._server = None
        if self._client:
            self._client.stop()
            self._client = None

