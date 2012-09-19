#!/usr/bin/env python
"""
@package ion.services.sa.tcaa.terrestrial_endpoint
@file ion/services/sa/tcaa/terrestrial_endpoint.py
@author Edward Hunter
@brief 2CAA Terrestrial endpoint.
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
    Terrestrial endpoint for two component agent architecture.
    This class provides a manipulable terrestrial command queue and fully
    asynchronous command and result transmission between shore and remote
    containers.
    """
    def __init__(self, *args, **kwargs):
        """
        For framework level code only.
        """
        super(BaseTerrestrialEndpoint, self).__init__(*args, **kwargs)
        
    def on_init(self):
        """
        Application level initializer.
        Setup default internal values.
        """
        super(BaseTerrestrialEndpoint, self).on_init()
        self._server = None
        self._client = None
        self._remote_host = self.CFG.remote_host
        self._remote_port = self.CFG.remote_port
        self._terrestrial_port = self.CFG.terrestrial_port
        self._platform_resource_id = self.CFG.platform_resource_id
        self._link_status = TelemetryStatusType.UNAVAILABLE
        self._tx_dict = {}
        self._event_subscriber = None
        self._server_greenlet = None
        self._publisher = EventPublisher()
    
    def on_start(self):
        """
        Process about to be started.
        Create client and server R3PC sockets.
        Start server.
        Start telemetry subscriber.
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
        Stop sockets and subscriber.
        """
        self._stop()
        super(BaseTerrestrialEndpoint, self).on_stop()
    
    def on_quit(self):
        """
        Process terminated following.
        Stop sockets and subscriber.
        """
        self._stop()
        super(BaseTerrestrialEndpoint, self).on_quit()

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

    def _req_callback(self, result):
        """
        Terrestrial server callback for result receipts.
        Pop pending command, append result and publish.
        """
        log.debug('Terrestrial server got result: %s', str(result))
        
        try:
            id = result['command_id']
            _result = result['result']
            cmd = self._tx_dict.pop(id)
            cmd.time_completed = time.time()
            cmd.result = _result
            self._publisher.publish_event(
                                    event_type='RemoteCommandResult',
                                    command=cmd,
                                    origin=cmd.resource_id)
            log.debug('Published remote result: %s.', str(result))
        except KeyError:
            log.warning('Error publishing remote result: %s.', str(result))

        # If the send queue is empty, publish availability.
        if len(self._client._queue) == 0:
            pass
    
    def _ack_callback(self, request):
        """
        Terrestrial client callback for command transmission acks.
        Insert command into pending command dictionary.
        """
        log.debug('Terrestrial client got ack for request: %s', str(request))
        self._tx_dict[request.command_id] = request
        self._publisher.publish_event(
                                event_type='RemoteCommandTransmittedEvent',
                                origin=self._platform_resource_id,
                                queue_size=len(self._client._queue))        

    def _server_close_callback(self):
        """
        Terrestrial server has closed.
        """
        log.debug('Terrestrial endpoint server closed.')    
    
    def _client_close_callback(self):
        """
        Terrestrial client has closed.
        """
        log.debug('Terrestrial endpoint client closed.')

    def _consume_telemetry_event(self, *args, **kwargs):
        """
        Telemetry event callback.
        Trigger link up or link down processing as needed.
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
        Processing on link up event.
        Start client socket.
        ION link availability published when pending commands are transmitted.
        """
        log.debug('Terrestrial client connecting to %s:%i',
                      self._remote_host, self._remote_port)
        self._client.start(self._remote_host, self._remote_port)
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
        
    def enqueue_command(self, command=None, link=False):
        """
        Enqueue command for remote processing.
        """
        if link and self._link_status != TelemetryStatusType.AVAILABLE:
            return
        
        if not isinstance(command, RemoteCommand):
            return
        
        command.time_queued = time.time()
        command.command_id = str(uuid.uuid4())
        self._client.enqueue(command)
        self._publisher.publish_event(
                                event_type='RemoteQueueModifiedEvent',
                                origin=self._platform_resource_id,
                                queue_size=len(self._client._queue))        
        return command
    
    def get_queue(self, resource_id=''):
        """
        Retrieve the command queue by resource id.
        """
        if resource_id == '':
            result = list(self._client._queue)
        else:
            [x for x in self._client._queue if x.resource_id == resource_id]

    def clear_queue(self, resource_id=''):
        """
        Clear the command queue by resource id.
        Only availabile in offline mode.
        """
        popped = []
        if self._link_status == TelemetryStatusType.UNAVAILABLE:        
            new_queue = [x for x in self._client._queue if x.resource_id != resource_id]
            popped = [x for x in self._client._queue if x.resource_id == resource_id]
            self._client._queue = new_queue
            self._publisher.publish_event(
                                event_type='RemoteQueueModifiedEvent',
                                origin=self._platform_resource_id,
                                queue_size=len(self._client._queue))        
            
        return popped

    def pop_queue(self, command_id=''):
        """
        Pop command queue by command id.
        Only available in offline mode.
        """
        poped = None
        if self._link_status == TelemetryStatusType.UNAVAILABLE:
            for x in self._client._queue:
                if x.command_id == command_id:
                    poped = self._client._queue.pop(x)
                    self._publisher.publish_event(
                                event_type='RemoteQueueModifiedEvent',
                                origin=self._platform_resource_id,
                                queue_size=len(self._client._queue))        
        return poped
    
    def get_pending(self, resource_id=''):
        """
        Retrieve pending commands by resource id.
        """
        if resource_id == '':
            pending = self._tx_dict.values()
        
        else:
            pending = []
            for (key,val) in self._tx_dict.iteritems():
                if val.resource_id == resource_id:
                    pending.append(val)
        return pending

    def get_port(self):
        """
        Retrieve the terrestrial server port.
        """
        return self._terrestrial_port


class TerrestrialEndpointClient(TerrestrialEndpointProcessClient):
    """
    Terrestrial endpoint client.
    """
    pass
