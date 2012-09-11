#!/usr/bin/env python

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Pyon log and config objects.
from pyon.public import log
from pyon.public import CFG

# Standard imports.
import time
import os
import signal
import time
import unittest
from datetime import datetime
import uuid
import socket
import re

# 3rd party imports.
import gevent
from gevent import spawn
from gevent.event import AsyncResult
from nose.plugins.attrib import attr
from mock import patch

# Pyon unittest support.
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase

from pyon.util.context import LocalContextMixin
from ion.services.sa.tcaa.terrestrial_endpoint import TerrestrialEndpoint
from ion.services.sa.tcaa.terrestrial_endpoint import TerrestrialEndpointClient
from interface.services.icontainer_agent import ContainerAgentClient
from ion.services.sa.tcaa.r3pc import R3PCServer
from ion.services.sa.tcaa.r3pc import R3PCClient

# bin/nosetests -s -v ion/services/sa/tcaa/test/test_terrestrial_endpoint.py
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_terrestrial_endpoint.py:TestTerrestrialEndpoint.test_something

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


class RemoteEndpoint(object):
    """
    """
    

@attr('INT', group='sa')
class TestTerrestrialEndpoint(IonIntegrationTestCase):
    """
    """
    def setUp(self):
        """
        """
        #self._remote_server = R3PCServer(self.consume_req, self.remote_server_close)
        #self._remote_client = R3PCClient(self.consume_ack, self.remote_client_close)
        #self.addCleanup(self._remote_server.stop)
        #self.addCleanup(self._remote_client.stop)
        self._remote_port = 0
        #self._remote_port = self._remote_server.start('*', 0)
        
        
        # Start container.
        log.debug('Staring capability container.')
        self._start_container()
        
        # Bring up services in a deploy file (no need to message)
        log.info('Staring deploy services.')
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        log.debug('Creating container client.')
        container_client = ContainerAgentClient(node=self.container.node,
            name=self.container.name)

        # Create agent config.
        endpoint_config = {
            'remote_host' : 'localhost',
            'remote_port' : self._remote_port,
            'terrestrial_port' : 0
        }

        """
        self._server = R3PCServer(self.consume_req, self.server_close)
        self.addCleanup(self._server.stop)        
        port = self._server.start('*', 0)
        """
        
        log.debug('Spawning terrestrial endpoint process.')
        te_pid = container_client.spawn_process(
            name='remote_endpoint_1',
            module='ion.services.sa.tcaa.terrestrial_endpoint',
            cls='TerrestrialEndpoint',
            config=endpoint_config)
        log.debug('Endpoint pid=%s.', str(te_pid))

        # On init or on start etc to start the endpoint server and port.

        """
        spawn_process(self, name=None, module=None, cls=None, config=None, process_id=None):
        client(self, process=None, to_name=None, name=None, node=None, **kwargs):
        """

        # Start a resource agent client to talk with the instrument agent.
        self.te_client = TerrestrialEndpointClient(
            process=FakeProcess(),
            to_name=te_pid)
        log.debug('Got te client %s.', str(self.te_client))
        
        #self._terrestrial_port = self.te_client.get_port()
        
    def on_link_up(self):
        """
        """
        #self._remote_client.start('localhost', self._terrestrial_port)
        pass
        # Publish a link up event to be caught by the endpoint.
    
    def on_link_down(self):
        """
        """
        #self._remote_client.stop()
        pass
        # Publish a link down event to be caught by the endpoint.

    def consume_req(self, request):
        """
        """
        # Spawn a greenlet to sleep briefly with each request and
        # then respond with a result through the remote client.
        pass

    def consume_ack(self):
        """
        """
        pass

    def remote_server_close(self):
        """
        """
        pass
    
    def remote_client_close(self):
        """
        """
        pass

    def test_something(self):
        """
        """
        #self.te_client.enqueue_command()
        #print 'The terrestrial port is %i' % self._terrestrial_port
        pass
        