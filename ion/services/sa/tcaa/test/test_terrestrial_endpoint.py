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
from ion.services.sa.tcaa.terrestrial_endpoint import TerrestrialEndpoint
from ion.services.sa.tcaa.terrestrial_endpoint import TerrestrialEndpointClient
from interface.services.icontainer_agent import ContainerAgentClient
from interface.objects import TelemetryStatusType
from ion.services.sa.tcaa.r3pc import R3PCServer
from ion.services.sa.tcaa.r3pc import R3PCClient

# bin/nosetests -s -v ion/services/sa/tcaa/test/test_terrestrial_endpoint.py
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_terrestrial_endpoint.py:TestTerrestrialEndpoint.test_process_queued
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_terrestrial_endpoint.py:TestTerrestrialEndpoint.test_process_online
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_terrestrial_endpoint.py:TestTerrestrialEndpoint.test_remote_late

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
    Test cases for 2CAA terrestrial endpoint.
    """
    def setUp(self):
        """
        Setup fake remote components.
        Start remote server.
        Set internal configuration and test variables.
        Start container.
        Start services.
        Spawn endpoint.
        Create and start subscribers.
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
        
        # Set internal variables.
        self._remote_host = 'localhost'
        self._platform_resource_id = 'abc123'
        self._resource_id = 'fake_id'
        self._no_requests = 10
        self._requests_sent = {}
        self._results_recv = {}
        self._workers = []
        self._done_evt = AsyncResult()
        
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

        # Start the result subscriber.        
        self._result_subscriber = EventSubscriber(
            event_type='RemoteCommandResult', origin=self._resource_id,
                callback=self.recv_remote_result)
        self._result_subscriber.start()
        self._result_subscriber._ready_event.wait(timeout=5)
        self.addCleanup(self._result_subscriber.stop)

        self._got_event = AsyncResult()    
    
    def recv_remote_result(self, evt, *args, **kwargs):
        """
        Subscriber callback to receive remote results.
        """
        cmd = evt.command
        log.debug('Test got remote result: %s', str(cmd))
        self._results_recv[cmd.command_id] = cmd    
        if len(self._results_recv) == self._no_requests:
            self._done_evt.set()
    
    def consume_event(self, *args, **kwargs):
        """
        Test callback for events.
        """
        log.debug('args: %s, kwargs: %s', str(args), str(kwargs))
        self._got_event.set()
        
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

    def consume_req(self, request):
        """
        Remote request callback.
        Fire a greenlet to do some fake work before returning via
        the remote client to terrestrial endpoint.
        """
        # Spawn a greenlet to sleep briefly with each request and
        # then respond with a result through the remote client.
        log.debug('Remote endpoint got request: %s', str(request))
        greenlet = gevent.spawn(self.process_remote_request, request)
        self._workers.append(greenlet)

    def consume_ack(self, request):
        """
        Remote ack callback.
        """
        log.debug('Remote endpoint got ack: %s', str(request))    

    def process_remote_request(self, request):
        """
        Process remote request.
        Do random amount of fake work and enqueue result for return to
        terrestrial endpoint.
        """
        worktime = random.uniform(.1,3)
        gevent.sleep(worktime)
        result = {
            'command_id' : request.command_id,
            'result' : 'fake_result'
        }
        log.debug('Finished processing request: %s', str(request))
        self._remote_client.enqueue(result)

    def remote_server_close(self):
        """
        Remote server closed callback.
        """
        log.debug('The remote server closed.')
    
    def remote_client_close(self):
        """
        Remoe client closed callback.
        """
        log.debug('The remote client closed.')
    
    def make_fake_command(self, no):
        """
        Build a fake command for use in tests.
        """
            
        cmdstr = 'fake_cmd_%i' % no
        cmd = IonObject('RemoteCommand',
                             resource_id=self._resource_id,
                             command=cmdstr,
                             args=['arg1', 23],
                             kwargs={'kwargs1':'someval'})
        return cmd
    
    def test_process_queued(self):
        """
        Test forwarding of queued commands upon link up.
        """
        
        for i in range(self._no_requests):
            cmd = self.make_fake_command(i)
            cmd = self.te_client.enqueue_command(cmd)
            self._requests_sent[cmd.command_id] = cmd
        
        self.on_link_up()
        
        self._done_evt.get(timeout=10)
        
        self.on_link_down()

        self.assertItemsEqual(self._requests_sent.keys(),
                                  self._results_recv.keys())


    def test_process_online(self):
        """
        Test forwarding commands when the link is up.
        """
        
        self.on_link_up()
        
        for i in range(self._no_requests):
            cmd = self.make_fake_command(i)
            cmd = self.te_client.enqueue_command(cmd)
            self._requests_sent[cmd.command_id] = cmd
            gevent.sleep(.2)

        self._done_evt.get(timeout=10)
        
        self.on_link_down()

        self.assertItemsEqual(self._requests_sent.keys(),
                                  self._results_recv.keys())

    def test_remote_late(self):
        """
        Test simulates behavior when the remote side is initially unavailable.
        """
        
        self.on_link_up()

        self._remote_server.stop()
        self._remote_client.stop()

        for i in range(self._no_requests):
            cmd = self.make_fake_command(i)
            cmd = self.te_client.enqueue_command(cmd)
            self._requests_sent[cmd.command_id] = cmd
        

        gevent.sleep(3)
        
        self._remote_client.start('localhost', self._terrestrial_port)
        self._remote_server.start('*', self._remote_port)

        self._done_evt.get(timeout=10)
        
        self.on_link_down()

        self.assertItemsEqual(self._requests_sent.keys(),
                                  self._results_recv.keys())
