#!/usr/bin/env python
"""
@package ion.services.sa.tcaa.test.test_terrestrial_endpoint
@file ion/services/sa/tcaa/test/test_terrestrial_endpoint.py
@author Edward Hunter
@brief Test cases for 2CAA terrestrial endpoint.
"""

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
import random

# 3rd party imports.
import gevent
from gevent import spawn
from gevent.event import AsyncResult
from nose.plugins.attrib import attr
from mock import patch

# Pyon unittest support.
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase

from pyon.public import IonObject
from pyon.event.event import EventPublisher, EventSubscriber
from pyon.util.context import LocalContextMixin
from ion.services.sa.tcaa.remote_endpoint import RemoteEndpoint
from ion.services.sa.tcaa.remote_endpoint import RemoteEndpointClient
from interface.services.icontainer_agent import ContainerAgentClient
from ion.services.sa.tcaa.r3pc import R3PCServer
from ion.services.sa.tcaa.r3pc import R3PCClient
from interface.objects import TelemetryStatusType

# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_endpoint.py:TestRemoteEndpoint
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_endpoint.py:TestRemoteEndpoint.test_xxx

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''
        
@attr('INT', group='sa')
class TestRemoteEndpoint(IonIntegrationTestCase):
    """
    Test cases for 2CAA terrestrial endpoint.
    """
    def setUp(self):
        """
        """
        
        self._terrestrial_server = R3PCServer(self.consume_req, self.terrestrial_server_close)
        self._terrestrial_client = R3PCClient(self.consume_ack, self.terrestrial_client_close)
        self.addCleanup(self._terrestrial_server.stop)
        self.addCleanup(self._terrestrial_client.stop)
        self._other_port = self._terrestrial_server.start('*', 0)
        log.debug('Terrestrial server binding to *:%i', self._other_port)
        
        self._other_host = 'localhost'
        self._platform_resource_id = 'abc123'
        
        # Start container.
        log.debug('Staring capability container.')
        self._start_container()
        
        # Bring up services in a deploy file (no need to message).
        log.info('Staring deploy services.')
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Create a container client.
        log.debug('Creating container client.')
        container_client = ContainerAgentClient(node=self.container.node,
            name=self.container.name)

        # Create agent config.
        endpoint_config = {
            'other_host' : self._other_host,
            'other_port' : self._other_port,
            'this_port' : 0,
            'platform_resource_id' : self._platform_resource_id
        }
        
        # Spawn the remote enpoint process.
        log.debug('Spawning remote endpoint process.')
        re_pid = container_client.spawn_process(
            name='remote_endpoint_1',
            module='ion.services.sa.tcaa.remote_endpoint',
            cls='RemoteEndpoint',
            config=endpoint_config)
        log.debug('Endpoint pid=%s.', str(re_pid))

        # Create an endpoint client.
        self.re_client = RemoteEndpointClient(
            process=FakeProcess(),
            to_name=re_pid)
        log.debug('Got re client %s.', str(self.re_client))
        
        # Remember the remote port.
        self._this_port = self.re_client.get_port()
        log.debug('The remote port is: %i.', self._this_port)
        
        # Start the event publisher.
        self._event_publisher = EventPublisher()
      
    def on_link_up(self):
        """
        Called by a test to simulate turning the link on.
        """
        log.debug('Terrestrial client connecting to localhost:%i.',
                 self._this_port)
        self._terrestrial_client.start('localhost', self._this_port)
        # Publish a link up event to be caught by the endpoint.
        log.debug('Publishing telemetry event.')
        self._event_publisher.publish_event(
                            event_type='PlatformTelemetryEvent',
                            origin=self._platform_resource_id,
                            status = TelemetryStatusType.AVAILABLE)
    
    def on_link_down(self):
        """
        Called by a test to simulate turning the link off.
        """
        self._terrestrial_client.stop()
        # Publish a link down event to be caught by the endpoint.
        log.debug('Publishing telemetry event.')
        self._event_publisher.publish_event(
                            event_type='PlatformTelemetryEvent',
                            origin=self._platform_resource_id,
                            status = TelemetryStatusType.UNAVAILABLE)    
    
    def consume_req(self):
        """
        """
        pass
    
    def consume_ack(self):
        """
        """
        pass
    
    def terrestrial_server_close(self):
        """
        """
        pass
    
    def terrestrial_client_close(self):
        """
        """
        pass
    
    def test_xxx(self):
        """
        """
        
        self.on_link_up()
        gevent.sleep(2)
        self.on_link_down()
        gevent.sleep(2)
