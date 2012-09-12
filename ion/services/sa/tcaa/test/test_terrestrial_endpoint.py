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

from pyon.public import IonObject
from pyon.event.event import EventPublisher, EventSubscriber
from pyon.util.context import LocalContextMixin
from ion.services.sa.tcaa.terrestrial_endpoint import TerrestrialEndpoint
from ion.services.sa.tcaa.terrestrial_endpoint import TerrestrialEndpointClient
from interface.services.icontainer_agent import ContainerAgentClient
from interface.objects import TelemetryStatusType
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

@attr('INT', group='sa')
class TestTerrestrialEndpoint(IonIntegrationTestCase):
    """
    """
    def setUp(self):
        """
        """
        # Create fake remote client and server.
        # Add clean up to shut down properly.
        # Start remote server on a random port.
        self._remote_server = R3PCServer(self.consume_req, self.remote_server_close)
        self._remote_client = R3PCClient(self.consume_ack, self.remote_client_close)
        self.addCleanup(self._remote_server.stop)
        self.addCleanup(self._remote_client.stop)
        self._remote_port = self._remote_server.start('*', 0)
        log.debug('Remote server binding to *:%i', self._remote_port)
        self._remote_host = 'localhost'
        self._platform_resource_id = 'abc123'
        
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
            'remote_host' : self._remote_host,
            'remote_port' : self._remote_port,
            'terrestrial_port' : 0,
            'platform_resource_id' : self._platform_resource_id
        }
        
        # Spawn the terrestrial enpoint process.
        log.debug('Spawning terrestrial endpoint process.')
        te_pid = container_client.spawn_process(
            name='remote_endpoint_1',
            module='ion.services.sa.tcaa.terrestrial_endpoint',
            cls='TerrestrialEndpoint',
            config=endpoint_config)
        log.debug('Endpoint pid=%s.', str(te_pid))

        # Create an endpoint client.
        self.te_client = TerrestrialEndpointClient(
            process=FakeProcess(),
            to_name=te_pid)
        log.debug('Got te client %s.', str(self.te_client))
        
        # Remember the terrestrial port.
        self._terrestrial_port = self.te_client.get_port()
        
        # Start the event publisher.
        self._event_publisher = EventPublisher()
        
        # Start the test subscriber.
        self._event_subscriber = EventSubscriber(
            event_type='PlatformTelemetryEvent',
            callback=self.consume_event,
            origin=self._platform_resource_id)
        self._event_subscriber.start()
        self._event_subscriber._ready_event.wait(timeout=5)
        self.addCleanup(self._event_subscriber.stop)
        
        self._got_event = AsyncResult()    
    
    def consume_event(self, *args, **kwargs):
        """
        """
        log.debug('args: %s, kwargs: %s', str(args), str(kwargs))
        self._got_event.set()
        
    def on_link_up(self):
        """
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
        """
        self._remote_client.stop()
        # Publish a link down event to be caught by the endpoint.
        log.debug('Publishing telemetry event.')
        self._event_publisher.publish_event(
                            event_type='PlatformTelemetryEvent',
                            origin=self._platform_resource_id,
                            status = TelemetryStatusType.UNAVAILABLE)

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

    """
    RemoteCommand:
      resource_id: ''
      command: ''
      args: []
      kwargs: {}
      command_id: ''
      time_queued: 0
      time_completed: 0
      result: ''
    """
    
    def test_something(self):
        """
        """
        print 'The terrestrial port is %i' % self._terrestrial_port
        
        for i in range(10):
            cmd = 'fake_cmd_%i' % i
            rcmd = IonObject('RemoteCommand', resource_id='fake_id',
                             command=cmd, args=['arg1', 23],
                             kwargs={'kwargs1':'someval'})
            self.te_client.enqueue_command(rcmd)
        
