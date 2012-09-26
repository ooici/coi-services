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
        self._terrestrial_host = None
        self._terrestrial_port = None
        self._remote_port = None
        self._platform_resource_id = 'a_remote_platform'
        
        # Start container.
        log.debug('Staring capability container.')
        self._start_container()
        
        # Bring up services in a deploy file (no need to message)
        log.info('Staring deploy services.')
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Create a container client.
        log.debug('Creating container client.')
        container_client = ContainerAgentClient(node=self.container.node,
            name=self.container.name)

        # Create agent config.
        endpoint_config = {
            'terrestrial_host' : self._terrestrial_host,
            'terrestrial_port' : self._terrestrial_port,
            'remote_port' : 0,
            'platform_resource_id' : self._platform_resource_id
        }
        
        # Spawn the terrestrial enpoint process.
        log.debug('Spawning remote endpoint process.')
        te_pid = container_client.spawn_process(
            name='remote_endpoint_1',
            module='ion.services.sa.tcaa.remote_endpoint',
            cls='RemoteEndpoint',
            config=endpoint_config)
        log.debug('Endpoint pid=%s.', str(te_pid))

        # Create an endpoint client.
        self.re_client = RemoteEndpointClient(
            process=FakeProcess(),
            to_name=te_pid)
        log.debug('Got te client %s.', str(self.re_client))
        
        # Remember the terrestrial port.
        self._remote_port = self.re_client.get_port()
        log.debug('The remote port is: %i.', self._remote_port)
        
        # Start the event publisher.
        self._event_publisher = EventPublisher()
      
    def on_link_up(self):
        """
        Called by a test to simulate turning the link on.
        """
        log.debug('Remote client connecting to localhost:%i.',
                  self._terrestrial_port)
        self._remote_client.start('localhost', self._terrestrial_port)
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
        self._remote_client.stop()
        # Publish a link down event to be caught by the endpoint.
        log.debug('Publishing telemetry event.')
        self._event_publisher.publish_event(
                            event_type='PlatformTelemetryEvent',
                            origin=self._platform_resource_id,
                            status = TelemetryStatusType.UNAVAILABLE)    
    
    def test_xxx(self):
        """
        """
        pass

