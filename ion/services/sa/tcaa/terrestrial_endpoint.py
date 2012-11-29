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
import copy

# Pyon exceptions.
from pyon.core.exception import BadRequest
from pyon.core.exception import Conflict
from pyon.core.exception import ConfigNotFound

from pyon.event.event import EventPublisher, EventSubscriber
from interface.objects import TelemetryStatusType, RemoteCommand
from pyon.public import IonObject

from interface.services.sa.iterrestrial_endpoint import BaseTerrestrialEndpoint
from interface.services.sa.iterrestrial_endpoint import TerrestrialEndpointProcessClient
from ion.services.sa.tcaa.endpoint_mixin import EndpointMixin

# http://localhost:5984/_utils/index.html
# bin/pycc -fc

class TerrestrialEndpoint(BaseTerrestrialEndpoint, EndpointMixin):
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
        super(TerrestrialEndpoint, self).__init__(*args, **kwargs)

    ######################################################################    
    # Framework process lifecycle functions.
    ######################################################################    
    
    def on_init(self):
        """
        Application level initializer.
        Setup default internal values.
        """
        super(BaseTerrestrialEndpoint, self).on_init()
        self.mixin_on_init()
        self._tx_dict = {}
        if not self.CFG.xs_name:
            raise ConfigNotFound('Terrestrial endpoint missing required xs_name parameter.')
        self._xs_name = self.CFG.xs_name    
        
    def on_start(self):
        """
        Process about to be started.
        Create client and server R3PC sockets.
        Start server.
        Start telemetry subscriber.
        """
        super(BaseTerrestrialEndpoint, self).on_start()
        self.mixin_on_start()
        self._initialize_queue_resource()
        
    def on_stop(self):
        """
        Process about to be stopped.
        Stop sockets and subscriber.
        """
        self.mixin_on_stop()
        super(BaseTerrestrialEndpoint, self).on_stop()
    
    def on_quit(self):
        """
        Process terminated following.
        Stop sockets and subscriber.
        """
        self.mixin_on_quit()
        super(BaseTerrestrialEndpoint, self).on_quit()

    ######################################################################    
    # Callbacks.
    ######################################################################    

    """
    An error that could occur if dictionary is missing the command_id.
    test_repeated_clear_pop ... Traceback (most recent call last):
      File "/opt/cache/gevent-0.13.7-py2.7-linux-x86_64.egg/gevent/greenlet.py", line 390, in run
        result = self._run(*self.args, **self.kwargs)
      File "/var/buildbot-runner/bbot/slaves/centoslca6_py27/coi_pycc/build/ion/services/sa/tcaa/r3pc.py", line 130, in server_loop
        self._callback(request)
      File "/var/buildbot-runner/bbot/slaves/centoslca6_py27/coi_pycc/build/ion/services/sa/tcaa/terrestrial_endpoint.py", line 120, in _req_callback
        log.warning('Command: %s.', str(cmd))
    UnboundLocalError: local variable 'cmd' referenced before assignment
    <Greenlet at 0xfb93550: server_loop> failed with UnboundLocalError
    """
    
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
            if cmd.resource_id:
                origin = cmd.resource_id
            elif cmd.svc_name:
                origin = cmd.svc_name + self._xs_name
            else:
                raise KeyError
            
            self._publisher.publish_event(
                                    event_type='RemoteCommandResult',
                                    command=cmd,
                                    origin=origin)
            log.debug('Published remote result: %s to origin %s.', str(result),
                      str(origin))
        except KeyError:
            log.warning('Error publishing remote result: %s.', str(result))
            
    def _ack_callback(self, request):
        """
        Terrestrial client callback for command transmission acks.
        Insert command into pending command dictionary.
        """
        log.debug('Terrestrial client got ack for request: %s', str(request))
        #self._tx_dict[request.command_id] = request
        self._update_queue_resource()
        self._publisher.publish_event(
                                event_type='RemoteCommandTransmittedEvent',
                                origin=self._xs_name,
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
        log.debug('%s client connecting to %s:%i',
                    self.__class__.__name__,
                    self._other_host, self._other_port)
        self._client.start(self._other_host, self._other_port)
        self._publisher.publish_event(
                                event_type='PublicPlatformTelemetryEvent',
                                origin=self._xs_name,
                                status=TelemetryStatusType.AVAILABLE)        

    def _on_link_down(self):
        """
        Processing on link down event.
        Stop client socket and publish ION link unavailability.
        """
        self._client.stop()
        self._publisher.publish_event(
                                event_type='PublicPlatformTelemetryEvent',
                                origin=self._xs_name,
                                status=TelemetryStatusType.UNAVAILABLE)

    ######################################################################    
    # Queue persistence helpers.
    ######################################################################    
        
    def _initialize_queue_resource(self):
        """
        Retrieve the resource and restore the remote queue.
        If it does not exist, create a new one.
        """
        listen_name = self.CFG.process.listen_name
        objs, ids = self.clients.resource_registry.find_resources(name=listen_name)
        
        # If no persisted queue exists, create one.
        if len(objs) == 0:
            createtime = time.time()
            obj = IonObject('RemoteCommandQueue',
                        name=listen_name,
                        updated=createtime,
                        created=createtime)

            # Persist object and read it back.
            obj_id, obj_rev = self.clients.resource_registry.create(obj)
            obj = self.clients.resource_registry.read(obj_id)
        
            log.debug('Created persistent queue for name=%s', listen_name)
        
        # If one exists, restore it here.
        elif len(objs) == 1:
            obj = objs[0]
            obj_id = ids[0]
            for command in obj.queue:
                self._tx_dict[command.command_id] = command
                self._client.enqueue(command)            

            log.debug('Restored remote queue for name=%s: len=%i updated=%f.',
                  listen_name, len(obj.queue), obj.updated)
        
        # Error: multiple queues with same name.
        else:
            log.error('%i > 1 remote command queues found for name=%s',
                        len(objs), listen_name)
        
        
    def _update_queue_resource(self):
        """
        Retrieve and update the resource that persists the remote command
        queue.
        """
        
        listen_name = self.CFG.process.listen_name
        
        while True:        
            objs, ids = self.clients.resource_registry.find_resources(name=listen_name)
            if len(objs) != 1:
                log.error('Incorrect number of persistent queues for %s.',
                          listen_name)
                return
            obj = objs[0]
            obj_id = ids[0]
            obj.queue = copy.deepcopy(self._client._queue)
            obj.updated = time.time()
            try:
                self.clients.resource_registry.update(obj)
                log.debug('Updated queue for name=%s: len=%i updated=%f.',
                      listen_name, len(obj.queue), obj.updated)
                break
            except Conflict:
                log.debug('Conflict with queue resource version, rereading.')
        
    ######################################################################    
    # Commands.
    ######################################################################    

    def enqueue_command(self, command=None, link=False):
        """
        Enqueue command for remote processing.
        """
        if link and self._link_status != TelemetryStatusType.AVAILABLE:
            raise Conflict('Cannot forward while link is down.')
        
        if not isinstance(command, RemoteCommand):
            raise BadRequest('Invalid command parameter.')
        
        command.time_queued = time.time()
        if not command.command_id:
            command.command_id = str(uuid.uuid4())
        self._tx_dict[command.command_id] = command
        self._client.enqueue(command)
        self._update_queue_resource()
        self._publisher.publish_event(
                                event_type='RemoteQueueModifiedEvent',
                                origin=self._xs_name,
                                queue_size=len(self._client._queue))        
        return command
    
    def get_queue(self, resource_id='', svc_name=''):
        """
        Retrieve the command queue by resource id.
        """
        if resource_id == '' and svc_name == '':
            result = list(self._client._queue)
        elif resource_id:
            result = [x for x in self._client._queue if x.resource_id == resource_id]
        elif svc_name:
            result = [x for x in self._client._queue if x.svc_name == svc_name]

        return result

    def clear_queue(self, resource_id='', svc_name=''):
        """
        Clear the command queue by resource id.
        Only availabile in offline mode.
        """
        popped = []
        if self._link_status == TelemetryStatusType.UNAVAILABLE:
            if resource_id == '' and svc_name == '':
                new_queue = []
                popped = self._client._queue
            elif resource_id:
                new_queue = [x for x in self._client._queue if x.resource_id != resource_id]
                popped = [x for x in self._client._queue if x.resource_id == resource_id]
            else:
                new_queue = [x for x in self._client._queue if x.svc_name != svc_name]
                popped = [x for x in self._client._queue if x.svc_name == svc_name]
            
            for x in popped:
                if x.command_id in self._tx_dict:
                    self._tx_dict.pop(x.command_id)
                    
            self._client._queue = new_queue
            if len(popped)>0:
                self._update_queue_resource()                
                self._publisher.publish_event(
                                event_type='RemoteQueueModifiedEvent',
                                origin=self._xs_name,
                                queue_size=len(self._client._queue))        
            
        return popped
    
    def pop_queue(self, command_id=''):
        """
        Pop command queue by command id.
        Only available in offline mode.
        """
        poped = None
        if self._link_status == TelemetryStatusType.UNAVAILABLE:
            for x in range(len(self._client._queue)):
                if self._client._queue[x].command_id == command_id:
                    poped = self._client._queue.pop(x)
                    if poped.command_id in self._tx_dict:
                        self._tx_dict.pop(poped.command_id)
                        
                    self._publisher.publish_event(
                                event_type='RemoteQueueModifiedEvent',
                                origin=self._xs_name,
                                queue_size=len(self._client._queue))
                    break
                    
        if poped:
            self._update_queue_resource()                
            
        return poped
    
    def get_pending(self, resource_id='', svc_name=''):
        """
        Retrieve pending commands by resource id.
        """
        pending = []
        if resource_id == '' and svc_name == '':
            pending = self._tx_dict.values()
        
        elif resource_id:
            for (key,val) in self._tx_dict.iteritems():
                if val.resource_id == resource_id:
                    pending.append(val)        
        else:
            for (key,val) in self._tx_dict.iteritems():
                if val.svc_name == svc_name:
                    pending.append(val)
            
        return pending

    """
    We remove this command from the endpoint.
    Once transmitted, commands can't be cleared.
    (Complexity arount iterating over the dict while it is being
    modified makes this tricky. )
    def clear_pending(self, resource_id='', svc_name=''):
        #
        #Clear pending commands by resource id.
        #
        pending = []
        if resource_id == '' and svc_name == '':
            pending = self._tx_dict.values()
            self._tx_dict = {}
            
        elif resource_id:
            for (key,val) in self._tx_dict.iteritems():
                if val.resource_id == resource_id:
                    pending.append(val)
                    self._tx_dict.pop(key)
        
        else:
            for (key,val) in self._tx_dict.iteritems():
                if val.svc_name == svc_name:
                    pending.append(val)
                    self._tx_dict.pop(key)
            
        return pending
    """
    
    def get_port(self):
        """
        Retrieve the terrestrial server port.
        """
        return self._this_port

    def set_client_port(self, port):
        """
        Set the terrestrial client port.
        """
        self._other_port = port

    def get_client_port(self):
        """
        Get the terrestrial client port.
        """
        return self._other_port


class TerrestrialEndpointClient(TerrestrialEndpointProcessClient):
    """
    Terrestrial endpoint client.
    """
    pass
