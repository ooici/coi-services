#!/usr/bin/env python
"""
@package 
@file 
@author Edward Hunter
@brief 
"""

__author__ = 'Edward Hunter'

__license__ = 'Apache 2.0'

# Pyon log and config objects.
from pyon.public import log
from pyon.public import CFG

import uuid
import time

from pyon.event.event import EventPublisher, EventSubscriber
from interface.objects import TelemetryStatusType, RemoteCommand

from interface.services.sa.iterrestrial_endpoint import BaseTerrestrialEndpoint
from interface.services.sa.iterrestrial_endpoint import TerrestrialEndpointProcessClient
from ion.services.sa.tcaa.r3pc import R3PCServer
from ion.services.sa.tcaa.r3pc import R3PCClient

class TerrestrialEndpoint(BaseTerrestrialEndpoint):
    """
    """
    def __init__(self, *args, **kwargs):
        """
        For framework level code only.
        """
        super(BaseTerrestrialEndpoint, self).__init__(*args, **kwargs)
        
    def on_init(self):
        """
        Application level initializer.
        """
        super(BaseTerrestrialEndpoint, self).on_init()
        self._server = None
        self._client = None
        self._remote_host = self.CFG.remote_host
        self._remote_port = self.CFG.remote_port
        self._terrestrial_port = self.CFG.terrestrial_port
        self._platform_resource_id = self.CFG.platform_resource_id
        self._link_status = TelemetryStatusType.UNAVAILABLE
        self._tx_queue = []
        self._tx_dict = {}
        self._event_subscriber = None
        self._server_greenlet = None
    
    def on_start(self):
        """
        Process about to be started.
        """
        super(BaseTerrestrialEndpoint, self).on_start()
        self._server = R3PCServer(self._req_callback,
                                self._server_close_callback)
        self._client = R3PCClient(self._ack_callback,
                                self._client_close_callback)
        if self._terrestrial_port == 0:            
            self._terrestrial_port = self._server.start('*', self._terrestrial_port)
        else:
            self._server.start('*', self._terrestrial_port)
        log.debug('Terrestrial server binding to *:%i', self._terrestrial_port)
    
        # Start the event subscriber.
        self._event_subscriber = EventSubscriber(
            event_type='PlatformTelemetryEvent',
            callback=self._consume_telemetry_event,
            origin=self._platform_resource_id)
        self._event_subscriber.start()
        self._event_subscriber._ready_event.wait(timeout=5)
        
    def on_stop(self):
        """
        Process about to be stopped. 
        """
        self._stop()
        super(BaseTerrestrialEndpoint, self).on_stop()
    
    def on_quit(self):
        """
        Process terminated following.
        """
        self._stop()
        super(BaseTerrestrialEndpoint, self).on_quit()

    def _stop(self):
        """
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

    def _consume_telemetry_event(self, *args, **kwargs):
        """
        """
        log.debug('Telemetry event received by terrestrial endpoint, args: %s, kwargs: %s',
                  str(args), str(kwargs))
        evt = args[0]
        self._link_status = evt.status
        if evt.status == TelemetryStatusType.AVAILABLE:
            log.debug('Telemetry available.')
            self._on_link_up()
            
        elif evt.status == TelemetryStatusType.UNAVAILABLE:
            log.debug('Telemetry not available.')
            self._on_link_down()
        
    def _on_link_up(self):
        """
        """
        log.debug('Terrestrial client connecting to %s:%i',
                  self._remote_host, self._remote_port)
        self._client.start(self._remote_host, self._remote_port)

    def _on_link_down(self):
        """
        """
        self._client.stop()
        
    def enqueue_command(self, command=None, link=False):
        """
        """
        if isinstance(command, RemoteCommand):
            command.time_queued = time.time()
            command.command_id = uuid.uuid4()
            print 'got command %s' % str(command)
            print str(type(command))

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
        return self._terrestrial_port

class TerrestrialEndpointClient(TerrestrialEndpointProcessClient):
    """
    """
    pass
