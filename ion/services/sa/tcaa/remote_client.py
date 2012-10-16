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

#
import inspect
import copy

# Zope interfaces.
from zope.interface import Interface, implements, directlyProvides

"""
from interface.services.iresource_agent import IResourceAgent
from ion.services.sa.tcaa.remote_client import RemoteClient
rc = RemoteClient(IResourceAgent,None)
params = ['param1','params2']

"""

class RemoteClient(object):
    """
    """
    
    def __init__(self, iface, xs_name, process=None):
        """
        """

        # Throw an exception if required interface arg not provided.        
        if not isinstance(iface, Interface):
            pass
        
        # Throw an exception if the xs_name is not provided.
        if not isinstance(xs_name, str) or xs_name == '':
            pass
        
        # Create the endpoint client.
        # Throw exception if unsuccessful.
        to_name = 'terrestrial_endpoint' + xs_name
        self._te_client = TerrestrailEndpointClient(process=process,
                to_name=to_name)
        
        # Grab the service method names.
        methods = iface.names()

        # Generate the service interface.
        # Each will forward to the terrestrial endpoint passing
        # the function name, args and kwargs.
        for m in methods:
            setattr(self, m, self.generate_service_method(m))

    def generate_service_method(self, name):
        """
        A closure that returns a function for forwarding service calls.
        The service call name is stored as a kwarg.
        """
        def func(*args, **kwargs):
            args = copy.deepcopy(args)
            kwargs = copy.deepcopy(kwargs)
            kwargs['func_name'] = name
            self.forward(*args, **kwargs)
        return func

    def forward(self, *args, **kwargs):
        """
        """
        
        
        func_name = kwargs.pop('func_name')
        print 'forwarding %s args: %s kwargs: %s' % (func_name, str(args), str(kwargs))
        
        
