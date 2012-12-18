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

# Standard imports.
import uuid
import time
import random

#3rd party imports.
import gevent

# Pyon exceptions.
from pyon.core.exception import IonException
from pyon.core.exception import BadRequest
from pyon.core.exception import ServerError
from pyon.core.exception import NotFound

from pyon.event.event import EventPublisher, EventSubscriber
from interface.objects import TelemetryStatusType, RemoteCommand
from pyon.core.bootstrap import get_service_registry
from pyon.agent.agent import ResourceAgentClient

from interface.services.sa.iremote_endpoint import BaseRemoteEndpoint
from interface.services.sa.iremote_endpoint import RemoteEndpointProcessClient
from ion.services.sa.tcaa.endpoint_mixin import EndpointMixin

class ServiceCommandQueue(object):
    """
    """
    def __init__(self, id, client, callback):
        """
        """
        self._id = id
        self._queue = []
        self._client = client
        self._callback = callback
        self._greenlet = None
        
    def start(self):
        
        def command_loop():
            while True:
                try:
                    cmd = self._queue.pop(0)
                    
                except IndexError:
                    # No command available, sleep for a while.
                    gevent.sleep(.1)
                    continue
                
                if self._id == 'fake_id':
                    log.debug('Processing fake command.')
                    worktime = cmd.kwargs.get('worktime', None)
                    if worktime:
                        worktime = random.uniform(0,worktime)
                        gevent.sleep(worktime)
                    payload = cmd.kwargs.get('payload', None)
                    result = payload or 'fake_result'
                else:
                    cmdstr = cmd.command
                    args = cmd.args
                    kwargs = cmd.kwargs
                                        
                    try:
                        log.debug('Remote endpoint attempting command: %s',
                                  cmdstr)
                        func = getattr(self._client, cmdstr)
                        result = func(*args, **kwargs)
                        log.debug('Remote endpoint command %s got result %s',
                                  cmdstr, str(result))

                    except AttributeError, TypeError:
                        # The command does not exist.
                        errstr = 'Unable to call remote command %s.' % cmdstr
                        log.error(errstr)
                        result = BadRequest(errstr)
                    
                    except IonException as ex:
                        # populate result with error.
                        log.error(str(ex))
                        result = ex
                        
                    except Exception as ex:
                        # populate result with error.
                        log.error(str(ex))
                        result = ServerError(str(ex))
                    
                cmd_result = {
                    'command_id' : cmd.command_id,
                    'result' : result
                }
                self._callback(cmd_result)
            
            
                    
        self._greenlet = gevent.spawn(command_loop)

    def stop(self):
        """
        """
        if self._greenlet:
            self._greenlet.kill()
            self._greenlet.join()
            self._greenlet = None
            
    def insert(self, cmd):
        """
        """
        self._queue.append(cmd)

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
        self._service_command_queues = {}

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
        self._stop_queues()
        super(RemoteEndpoint, self).on_stop()
    
    def on_quit(self):
        """
        Process terminated following.
        """
        self.mixin_on_quit()
        self._stop_queues()
        super(RemoteEndpoint, self).on_quit()

    ######################################################################    
    # Helpers.
    ######################################################################    

    def _stop_queues(self):
        """
        """
        for (id, queue) in self._service_command_queues.iteritems():
            print 'stopping queue %s' % str(id)
            queue.stop()
        self._service_command_queues = {}

    def _get_service_queue(self, request):
        """
        """
        id = request.resource_id
        svc_name = request.svc_name
        svc_queue = None
        if id:
            svc_queue = self._service_command_queues.get(id, None)
            if not svc_queue:
                if id == 'fake_id':
                    svc_queue = ServiceCommandQueue(id, None, self._result_complete)
                    svc_queue.start()
                    self._service_command_queues[id] = svc_queue
                    
                else:
                    try:
                        res_client = ResourceAgentClient(id, process=self)
                    except NotFound:
                        res_client = None
                        
                    else:
                        # Create and return a new queue with this client.
                        svc_queue = ServiceCommandQueue(id, res_client, self._result_complete)
                        svc_queue.start()
                        self._service_command_queues[id] = svc_queue
            
        elif svc_name:
            svc_queue = self._service_command_queues.get(svc_name, None)
            if not svc_queue:
                svc_cls = get_service_registry().get_service_by_name(svc_name)
                if svc_cls:
                    svc_client_cls = svc_cls.client
                    svc_client = svc_client_cls(process=self)
                    svc_queue = ServiceCommandQueue(svc_name, svc_client, self._result_complete)
                    svc_queue.start()
                    self._service_command_queues[svc_name] = svc_queue
                    
        return svc_queue

    ######################################################################    
    # Callbacks.
    ######################################################################    

    def _req_callback(self, request):
        """
        """
        log.debug('Remote endpoint got request: %s', str(request))
        svc_queue = self._get_service_queue(request)
        
        if svc_queue:
            # Add command the the service queue.
            svc_queue.insert(request)
        
        else:
            # No service or resource, return error.
            errstr = 'resource_id=%s or svc_name=%s not found.' % (request.resource_id, request.svc_name)
            log.error(errstr)
            result = {
                'command_id' : request.command_id,
                'result' : NotFound(errstr)
            }
            self._result_complete(result)
    
    def _ack_callback(self, result):
        """
        """
        log.debug('Remote endpoint got ack for result: %s', str(result))
    
    def _server_close_callback(self):
        """
        """
        log.debug('Remote endpoint server closed.')
    
    def _client_close_callback(self):
        """
        """
        log.debug('Remote endpoint client closed.')
    
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

    def _on_link_down(self):
        """
        Processing on link down event.
        Stop client socket and publish ION link unavailability.
        """
        self._client.stop()
    
    def _result_complete(self, result):
        """
        """
        if self._client:
            log.debug('Remote endpoint enqueuing result %s.', str(result))
            self._client.enqueue(result)
        else:
            log.warning('Received a result but no client available to transmit.')

    ######################################################################    
    # Commands.
    ######################################################################    
    
    def get_port(self):
        """
        Get the remote server port number.
        """
        return self._this_port
    
    def set_client_port(self, port):
        """
        Set the remote client port number.
        """
        self._other_port = port
    
    def get_client_port(self):
        """
        Get the remote client port number.
        """
        return self._other_port
    
    
class RemoteEndpointClient(RemoteEndpointProcessClient):
    """
    Remote endpoint client.
    """
    pass

