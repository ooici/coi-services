#!/usr/bin/env python
"""
@package ion.services.sa.tcaa.remote_proxy_client
@file ion/services/sa/tcaa/remote_proxy_client.py
@author Edward Hunter
@brief A generic proxy client to talk to a remote agent or service.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Pyon log and config objects.
from pyon.public import log
from pyon.public import CFG

# Standard imports.
import inspect
import copy
import uuid

#3rd party imports
import gevent

# Zope interfaces.
from zope.interface import Interface, implements, directlyProvides
from zope.interface.interface import InterfaceClass

# Pyon exceptions.
from pyon.core.exception import ConfigNotFound
from pyon.core.exception import Conflict
from pyon.core.exception import BadRequest
from pyon.core.exception import Timeout

# Ion imports.
from interface.services.sa.iterrestrial_endpoint import ITerrestrialEndpoint
from ion.services.sa.tcaa.terrestrial_endpoint import TerrestrialEndpointClient
from pyon.public import IonObject
from gevent.event import AsyncResult
from pyon.event.event import EventSubscriber

class RemoteClient(object):
    """
    A proxy client for any service or resource that forwards commands
    to the terrestrial endpoint for transmission.
    """
    implements(ITerrestrialEndpoint)
    
    def __init__(self, iface=None, xs_name=None, resource_id=None,
                 svc_name=None, process=None):
        """
        Construct remote proxy client.
        Verify required parameters.
        Construct remote endpoint client.
        Set internal variables.
        Construct and add service method attributes.
        """

        # Throw an exception if required interface arg not provided.        
        if not isinstance(iface, InterfaceClass):
            raise ConfigNotFound('Invalid interface parameter.')
        
        # Throw an exception if the xs_name is not provided.
        if not isinstance(xs_name, str) or xs_name == '':
            raise ConfigNotFound('Invalid exchange space name parameter.')
        
        # Create the endpoint client.
        # Throw exception if unsuccessful.
        to_name = 'terrestrial_endpoint' + xs_name
        self._te_client = TerrestrialEndpointClient(process=process,
                to_name=to_name)
        
        # Must define a resource id or service name.
        if not resource_id and not svc_name:
            raise ConfigNotFound('No resource or service specified.')

        # Can't specify both a resource and a service.
        if resource_id and svc_name:
            raise ConfigNotFound('Can\'t specify both a resource and a service.')
        
        self._resource_id = resource_id
        self._xs_name = xs_name
        self._svc_name = svc_name
        
        # Grab the service method names.
        methods = iface.names()

        # Generate the service interface.
        # Each will forward to the terrestrial endpoint passing
        # the function name, args and kwargs.
        for m in methods:
            setattr(self, m, self.generate_service_method(m))
        
        # Declare the dynamic interface.
        directlyProvides(self, iface)

        # Initialize the async results objects for blocking behavior.
        #self._async_result_evt = None

    def generate_service_method(self, name):
        """
        A closure that returns a function for forwarding service calls.
        The service call name is stored as a kwarg.
        """
        def func(*args, **kwargs):
            args = copy.deepcopy(args)
            kwargs = copy.deepcopy(kwargs)
            kwargs['func_name'] = name
            return self.forward(*args, **kwargs)
        return func

    def forward(self, *args, **kwargs):
        """
        Forward a service method to the terrestrial endpoint
        through the service interface.
        """
        func_name = kwargs.pop('func_name')
        try:
            link = kwargs.pop('link')
        except KeyError:
            link = True
        cid = ''
        try:
            remote_timeout = kwargs.pop('remote_timeout')
            if not isinstance(remote_timeout, int):
                remote_timeout = 0
            elif remote_timeout < 0:
                remote_timeout = 0
            elif remote_timeout == 0:
                pass
            else:
                cid = str(uuid.uuid4())
                
        except KeyError:
            remote_timeout = 0
                
        cmd = IonObject('RemoteCommand',
                             resource_id=self._resource_id,
                             svc_name=self._svc_name,
                             command=func_name,
                             command_id=cid,
                             args= args,
                             kwargs= kwargs)
        
        if remote_timeout == 0 :
            return self._te_client.enqueue_command(cmd, link)
            
        else:

            if self._resource_id:
                origin = self._resource_id
            elif self._svc_name:
                origin = self._svc_name + self._xs_name

            pending_cmd = cmd
            async_result_evt = AsyncResult()
            
            def result_callback(evt, *args, **kwargs):
                """
                Callback for subscriber retrive blocking results.
                """
                #global async_result_evt
                if evt.type_ == 'RemoteCommandResult':
                    cmd = evt.command
                    if cmd.command_id == pending_cmd.command_id:
                        async_result_evt.set(cmd)                

            sub = EventSubscriber(
                event_type='RemoteCommandResult',
                origin=origin,
                callback=result_callback)

            sub.start()
            #self._pending_cmd = cmd
            cmd = self._te_client.enqueue_command(cmd, link)
            try:
                result = async_result_evt.get(timeout=remote_timeout)
                #self._pending_cmd = None
                sub.stop()                
            except gevent.Timeout:
                #self._pending_cmd = None
                sub.stop()
                raise Timeout('Timed out waiting for remote result.')

            return result
    
    """
    def _result_callback(self, evt, *args, **kwargs):
        #Callback for subscriber retrive blocking results.
        if evt.type_ == 'RemoteCommandResult':
            cmd = evt.command
            if self._pending_cmd:
                if cmd.command_id == self._pending_cmd.command_id:
                    self._pending_cmd = None
                    if self._async_result_evt:
                        self._async_result_evt.set(cmd)
    """
    
    def enqueue_command(self, command=None, link=False):
        """
        Enqueue command with terrestrial endoint.
        """
        return self._te_client(command, link)

    def get_queue(self):
        """
        Get terrestrial endpoint queue for this resource/service.
        """
        if self._resource_id:
            return self._te_client.get_queue(resource_id=self._resource_id)
        elif self._svc_name:
            return self._te_client.get_queue(svc_name=self._svc_name)

    def clear_queue(self):
        """
        Clear terrestrial endpoint queue for this resource/service.
        """
        if self._resource_id:
            return self._te_client.clear_queue(resource_id=self._resource_id)
        elif self._svc_name:
            return self._te_client.clear_queue(svc_name=self._svc_name)

    def pop_queue(self, command_id=''):
        """
        Pop a command from the terrestrial endpoint queue.        
        """
        return self._te_client.pop_queue(command_id=command_id)

    def get_pending(self):
        """
        Get pending commands for this resource/service.
        """
        if self._resource_id:
            return self._te_client.get_pending(resource_id=self._resource_id)
        elif self._svc_name:
            return self._te_client.get_pending(svc_name=self._svc_name)

    def get_port(self):
        """
        Not supported for remote proxy clients.
        """
        raise BadRequest('get_port not available via remote client.')

    def set_client_port(self, port=0):
        """
        Not supported for remote proxy clients.
        """
        raise BadRequest('set_client_port not available via remote client.')

    def get_client_port(self):
        """
        Not supported for remote proxy clients.
        """
        raise BadRequest('get_client_port not available via remote client.')
    
    