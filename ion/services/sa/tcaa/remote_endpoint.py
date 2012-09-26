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
from ion.services.sa.tcaa.r3pc import R3PCServer
from ion.services.sa.tcaa.r3pc import R3PCClient

class RemoteEndpoint(BaseRemoteEndpoint):
    """
    """
    def __init__(self, *args, **kwargs):
        """
        For framework level code only.
        """
        super(BaseRemoteEndpoint, self).__init__(*args, **kwargs)
        
    ######################################################################    
    # Framework process lifecycle funcitons.
    ######################################################################    

    def on_init(self):
        """
        Application level initializer.
        Setup default internal values.
        """
        super(BaseRemoteEndpoint, self).on_init()
        self._server = None
        self._client = None
        self._terrestrial_host = self.CFG.terrestrial_host
        self._terrestrial_port = self.CFG.terrestrial_port
        self._remote_port = 0
        self._platform_resource_id = self.CFG.platform_resource_id
        self._remote_port = self.CFG.remote_port
        self._link_status = TelemetryStatusType.UNAVAILABLE
        self._event_subscriber = None
        self._server_greenlet = None
        self._publisher = EventPublisher()

    def on_start(self):
        """
        Process about to be started.
        """
        super(BaseRemoteEndpoint, self).on_start()
        
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
        super(BaseRemoteEndpoint, self).on_stop()
    
    def on_quit(self):
        """
        Process terminated following.
        """
        self._stop()
        super(BaseRemoteEndpoint, self).on_quit()

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
        print '##########################################'
        print "GOT A TELEMETRY EVENT: args:%s  kwargs:%s" % (str(args), str(kwargs))

    ######################################################################    
    # Helpers.
    ######################################################################    

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
    
    ######################################################################    
    # Commands.
    ######################################################################    
    
    def get_port(self):
        """
        """
        return self._remote_port
    
class RemoteEndpointClient(RemoteEndpointProcessClient):
    """
    Remote endpoint client.
    """
    pass

"""
from pyon.core.bootstrap import get_service_registry
svc_client_cls = get_service_registry().get_service_by_name(svc_name).client    
""" 

"""
list procs from self.container.proc_manager.list_procs():
EventPersister(name=event_persister,id=Edwards-MacBook-Pro_local_12469.1,type=standalone)
ResourceRegistryService(name=resource_registry,id=Edwards-MacBook-Pro_local_12469.3,type=service)
IdentityManagementService(name=identity_management,id=Edwards-MacBook-Pro_local_12469.5,type=service)
DirectoryService(name=directory,id=Edwards-MacBook-Pro_local_12469.4,type=service)
ExchangeManagementService(name=exchange_management,id=Edwards-MacBook-Pro_local_12469.7,type=service)

procs by name (self.container.proc_manager.procs_by_name):
data_retriever == DataRetrieverService(name=data_retriever,id=Edwards-MacBook-Pro_local_12469.18,type=service)
agent_management == AgentManagementService(name=agent_management,id=Edwards-MacBook-Pro_local_12469.9,type=service)
event_persister == EventPersister(name=event_persister,id=Edwards-MacBook-Pro_local_12469.1,type=standalone)
identity_management == IdentityManagementService(name=identity_management,id=Edwards-MacBook-Pro_local_12469.5,type=service)
catalog_management == CatalogManagementService(name=catalog_management,id=Edwards-MacBook-Pro_local_12469.22,type=service)
pubsub_management == PubsubManagementService(name=pubsub_management,id=Edwards-MacBook-Pro_local_12469.15,type=service)
"""
    