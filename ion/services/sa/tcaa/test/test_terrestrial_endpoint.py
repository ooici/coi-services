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

# bin/nosetests -s -v ion/services/sa/tcaa/test/test_terrestrial_endpoint.py:TestTerrestrialEndpoint
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_terrestrial_endpoint.py:TestTerrestrialEndpoint.test_process_queued
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_terrestrial_endpoint.py:TestTerrestrialEndpoint.test_process_online
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_terrestrial_endpoint.py:TestTerrestrialEndpoint.test_remote_late
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_terrestrial_endpoint.py:TestTerrestrialEndpoint.test_get_clear_queue
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_terrestrial_endpoint.py:TestTerrestrialEndpoint.test_pop_pending_queue
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_terrestrial_endpoint.py:TestTerrestrialEndpoint.test_repeated_clear_pop

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
        self._other_port = self._remote_server.start('*', 0)
        log.debug('Remote server binding to *:%i', self._other_port)
        
        # Set internal variables.
        self._other_host = 'localhost'
        self._platform_resource_id = 'abc123'
        self._resource_id = 'fake_id'
        self._no_requests = 10
        self._requests_sent = {}
        self._results_recv = {}
        self._workers = []
        self._done_evt = AsyncResult()
        self._queue_mod_evts = []
        self._cmd_tx_evts = []
        self._telem_evts = []
        self._no_telem_evts = 0
        self._no_queue_mod_evts = 0
        self._no_cmd_tx_evts = 0
        self._done_queue_mod_evts = AsyncResult()
        self._done_telem_evts = AsyncResult()
        self._done_cmd_tx_evts = AsyncResult()
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
            'other_host' : self._other_host,
            'other_port' : self._other_port,
            'this_port' : 0,
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
        self._this_port = self.te_client.get_port()
        
        # Start the event publisher.
        self._event_publisher = EventPublisher()
        
        # Start the event subscriber.
        self._event_subscriber = EventSubscriber(
            event_type='PlatformEvent',
            callback=self.consume_event,
            origin=self._platform_resource_id)
        self._event_subscriber.start()
        self._event_subscriber._ready_event.wait(timeout=5)
        self.addCleanup(self._event_subscriber.stop)

        # Start the result subscriber.        
        self._result_subscriber = EventSubscriber(
            event_type='RemoteCommandResult',
            origin=self._resource_id,
            callback=self.consume_event)
        self._result_subscriber.start()
        self._result_subscriber._ready_event.wait(timeout=5)
        self.addCleanup(self._result_subscriber.stop)
 
    def consume_event(self, evt, *args, **kwargs):
        """
        Test callback for events.
        """
        log.debug('Got event: %s, args: %s, kwargs: %s',
                  str(evt), str(args), str(kwargs))
        if evt.type_ == 'PublicPlatformTelemetryEvent':
            self._telem_evts.append(evt)
            if self._no_telem_evts > 0 and self._no_telem_evts == len(self._telem_evts):
                    self._done_telem_evts.set()
                    
        elif evt.type_ == 'RemoteQueueModifiedEvent':
            self._queue_mod_evts.append(evt)
            if self._no_queue_mod_evts > 0 and self._no_queue_mod_evts == len(self._queue_mod_evts):
                    self._done_queue_mod_evts.set()
            
        elif evt.type_ == 'RemoteCommandTransmittedEvent':
            self._cmd_tx_evts.append(evt)
            if self._no_cmd_tx_evts > 0 and self._no_cmd_tx_evts == len(self._cmd_tx_evts):
                    self._done_cmd_tx_evts.set()
        
        elif evt.type_ == 'RemoteCommandResult':
            cmd = evt.command
            self._results_recv[cmd.command_id] = cmd
            if len(self._results_recv) == self._no_requests:
                self._done_evt.set()
            
    def on_link_up(self):
        """
        Called by a test to simulate turning the link on.
        """
        log.debug('Remote client connecting to localhost:%i.',
                  self._this_port)
        self._remote_client.start('localhost', self._this_port)
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
        
        self._no_cmd_tx_evts = self._no_requests
        self._no_queue_mod_evts = self._no_requests
        self._no_telem_evts = 2
        
        for i in range(self._no_requests):
            cmd = self.make_fake_command(i)
            cmd = self.te_client.enqueue_command(cmd)
            self._requests_sent[cmd.command_id] = cmd
        
        self._done_queue_mod_evts.get(timeout=5)
        
        self.on_link_up()
        
        self._done_cmd_tx_evts.get(timeout=5)
                
        self._done_evt.get(timeout=10)

        pending = self.te_client.get_pending()
        self.assertEqual(len(pending), 0)
                
        self.on_link_down()

        self._done_telem_evts.get(timeout=5)

        self.assertItemsEqual(self._requests_sent.keys(),
                                  self._results_recv.keys())

    def test_process_online(self):
        """
        Test forwarding commands when the link is up.
        """

        self._no_cmd_tx_evts = self._no_requests
        self._no_queue_mod_evts = self._no_requests
        self._no_telem_evts = 2
        
        self.on_link_up()
        
        for i in range(self._no_requests):
            cmd = self.make_fake_command(i)
            cmd = self.te_client.enqueue_command(cmd)
            self._requests_sent[cmd.command_id] = cmd
            gevent.sleep(.2)

        self._done_queue_mod_evts.get(timeout=5)
        self._done_cmd_tx_evts.get(timeout=5)
        self._done_evt.get(timeout=10)
        
        pending = self.te_client.get_pending()
        self.assertEqual(len(pending), 0)
        
        self.on_link_down()

        self._done_telem_evts.get(timeout=5)

        self.assertItemsEqual(self._requests_sent.keys(),
                                  self._results_recv.keys())

    def test_remote_late(self):
        """
        Test simulates behavior when the remote side is initially unavailable.
        """
        
        self._no_cmd_tx_evts = self._no_requests
        self._no_queue_mod_evts = self._no_requests
        self._no_telem_evts = 2
        
        self.on_link_up()

        self._remote_server.stop()
        self._remote_client.stop()

        for i in range(self._no_requests):
            cmd = self.make_fake_command(i)
            cmd = self.te_client.enqueue_command(cmd)
            self._requests_sent[cmd.command_id] = cmd
        
        self._done_queue_mod_evts.get(timeout=5)

        gevent.sleep(3)
        
        self._remote_client.start('localhost', self._this_port)
        self._remote_server.start('*', self._other_port)

        self._done_cmd_tx_evts.get(timeout=5)
        self._done_evt.get(timeout=10)
        
        pending = self.te_client.get_pending()
        self.assertEqual(len(pending), 0)

        self.on_link_down()

        self._done_telem_evts.get(timeout=5)

        self.assertItemsEqual(self._requests_sent.keys(),
                                  self._results_recv.keys())

    def test_get_clear_queue(self):
        """
        Test endpoint queue get and clear manipulators.
        """
        
        # Set up for events expected.
        self._no_queue_mod_evts = self._no_requests

        # Queue commands.        
        for i in range(self._no_requests):
            cmd = self.make_fake_command(i)
            cmd = self.te_client.enqueue_command(cmd)
            self._requests_sent[cmd.command_id] = cmd
        
        # Confirm queue mod events.
        self._done_queue_mod_evts.get(timeout=5)
        
        # Confirm get queue with no id.
        queue = self.te_client.get_queue()
        self.assertEqual(len(queue), self._no_requests)

        # Confirm get queue with id.
        queue = self.te_client.get_queue(self._resource_id)
        self.assertEqual(len(queue), self._no_requests)

        # Confirm get queue with bogus id.
        queue = self.te_client.get_queue('bogus_id')        
        self.assertEqual(len(queue), 0)

        # Reset queue mod expected events.        
        self._queue_mod_evts = []
        self._no_queue_mod_evts = 1
        self._done_queue_mod_evts = AsyncResult()

        # Clear queue with no id.        
        poped = self.te_client.clear_queue()
    
        # Confirm queue mod event and mods.
        self._done_queue_mod_evts.get(timeout=5)
        queue = self.te_client.get_queue()
        self.assertEqual(len(poped), self._no_requests)
        self.assertEqual(len(queue), 0)

        # Queue new commands and confirm event.       
        self._queue_mod_evts = []
        self._no_queue_mod_evts = self._no_requests
        self._done_queue_mod_evts = AsyncResult()
        
        self._requests_sent = {}
        for i in range(self._no_requests):
            cmd = self.make_fake_command(i)
            cmd = self.te_client.enqueue_command(cmd)
            self._requests_sent[cmd.command_id] = cmd
        
        self._done_queue_mod_evts.get(timeout=5)

        # Reset queue mod expected events.
        self._queue_mod_evts = []
        self._no_queue_mod_evts = 1
        self._done_queue_mod_evts = AsyncResult()

        # Clear queue with id.
        poped = self.te_client.clear_queue(self._resource_id)
    
        # Confirm mods and mod events.
        self._done_queue_mod_evts.get(timeout=5)
        queue = self.te_client.get_queue()
        self.assertEqual(len(poped), self._no_requests)
        self.assertEqual(len(queue), 0)

        # Queue new commands and confirm events.        
        self._queue_mod_evts = []
        self._no_queue_mod_evts = self._no_requests
        self._done_queue_mod_evts = AsyncResult()
        
        self._requests_sent = {}
        for i in range(self._no_requests):
            cmd = self.make_fake_command(i)
            cmd = self.te_client.enqueue_command(cmd)
            self._requests_sent[cmd.command_id] = cmd
        
        self._done_queue_mod_evts.get(timeout=5)
        
        # Clear queue with bogus id.
        poped = self.te_client.clear_queue('bogus id')
        queue = self.te_client.get_queue()
        self.assertEqual(len(poped), 0)
        self.assertEqual(len(queue), self._no_requests)
        
        # Clear queue and confirm empty.
        self.te_client.clear_queue()
        queue = self.te_client.get_queue()
        self.assertEqual(len(queue), 0)
        
        # Turn on link and wait a few seconds.
        # Confirm no data or tx events arrive.
        self.on_link_up()

        gevent.sleep(2)
        self.assertEqual(len(self._cmd_tx_evts), 0)
        self.assertEqual(len(self._results_recv), 0)
        
        self._no_telem_evts = 2

        self.on_link_down()
        
        self._done_telem_evts.get(timeout=5)
                
        
    def test_pop_pending_queue(self):
        """
        Test endpoint queue pop manipulators.
        """
        
        # Set up for events expected.
        self._no_queue_mod_evts = self._no_requests

        # Queue commands.        
        for i in range(self._no_requests):
            cmd = self.make_fake_command(i)
            cmd = self.te_client.enqueue_command(cmd)
            self._requests_sent[cmd.command_id] = cmd
        
        # Confirm queue mod events.
        self._done_queue_mod_evts.get(timeout=5)
        queue = self.te_client.get_queue()
        self.assertEqual(len(queue), self._no_requests)

        # Pop a few commands from the queue, confirm events.
        self._queue_mod_evts = []
        self._no_queue_mod_evts = 3
        self._done_queue_mod_evts = AsyncResult()

        cmd_ids = self._requests_sent.keys()[:3]
        poped = []
        for x in cmd_ids:
            poped.append(self.te_client.pop_queue(x))
            self._requests_sent.pop(x)
            
        # Try poping with illegal args. This should have no effect
        poped.append(self.te_client.pop_queue())
        poped.append(self.te_client.pop_queue('bogus id'))
        poped = [x for x in poped if x != None]
        
        self._done_queue_mod_evts.get(timeout=5)
        queue = self.te_client.get_queue()
        self.assertEqual(len(poped), 3)
        self.assertEqual(len(queue), self._no_requests - 3)
        
        # Turn on the link and verify that only the remaining commands
        # get processed.
        
        self._no_telem_evts = 2
        self._no_requests = self._no_requests - 3
        self._no_cmd_tx_evts = self._no_requests
        
        self.on_link_up()
        self._done_cmd_tx_evts.get(timeout=5)
        
        self._done_evt.get(timeout=10)
        
        self.on_link_down()
        self._done_telem_evts.get(timeout=5)

        self.assertItemsEqual(self._requests_sent.keys(),
                                  self._results_recv.keys())
                
        pending = self.te_client.get_pending()
        self.assertEqual(len(pending), 0)        

    def test_repeated_clear_pop(self):
        """
        Test endpoint queue pop manipulators.
        """

        # Set up for events expected.
        self._no_queue_mod_evts = self._no_requests

        for i in range(3):
            
            self._queue_mod_evts = []
            self._no_queue_mod_evts = self._no_requests
            self._done_queue_mod_evts = AsyncResult()
            # Queue commands.
            self._requests_sent = {}
            for i in range(self._no_requests):
                cmd = self.make_fake_command(i)
                cmd = self.te_client.enqueue_command(cmd)
                self._requests_sent[cmd.command_id] = cmd
            
            # Confirm queue mod events.
            self._done_queue_mod_evts.get(timeout=5)
            
            # Confirm get queue with no id.
            queue = self.te_client.get_queue()
            self.assertEqual(len(queue), self._no_requests)
    
            # Reset queue mod expected events.        
            self._queue_mod_evts = []
            self._no_queue_mod_evts = 1
            self._done_queue_mod_evts = AsyncResult()
    
            # Clear queue with no id.        
            poped = self.te_client.clear_queue()
        
            # Confirm queue mod event and mods.
            self._done_queue_mod_evts.get(timeout=5)
            queue = self.te_client.get_queue()
            self.assertEqual(len(poped), self._no_requests)
            self.assertEqual(len(queue), 0)

        self._queue_mod_evts = []
        self._no_queue_mod_evts = self._no_requests
        self._done_queue_mod_evts = AsyncResult()
        # Queue commands.
        self._requests_sent = {}
        for i in range(self._no_requests):
            cmd = self.make_fake_command(i)
            cmd = self.te_client.enqueue_command(cmd)
            self._requests_sent[cmd.command_id] = cmd
        
        # Confirm queue mod events.
        self._done_queue_mod_evts.get(timeout=5)
        
        # Confirm get queue with no id.
        queue = self.te_client.get_queue()
        self.assertEqual(len(queue), self._no_requests)

        # Pop a few commands from the queue, confirm events.
        self._queue_mod_evts = []
        self._no_queue_mod_evts = 3
        self._done_queue_mod_evts = AsyncResult()

        cmd_ids = self._requests_sent.keys()[:3]
        poped = []
        for x in cmd_ids:
            poped.append(self.te_client.pop_queue(x))
            self._requests_sent.pop(x)
                    
        self._done_queue_mod_evts.get(timeout=5)
        queue = self.te_client.get_queue()
        self.assertEqual(len(poped), 3)
        self.assertEqual(len(queue), self._no_requests - 3)

        self._no_telem_evts = 2
        self._no_requests = self._no_requests - 3
        self._no_cmd_tx_evts = self._no_requests
        
        self.on_link_up()
        self._done_cmd_tx_evts.get(timeout=5)
        
        self._done_evt.get(timeout=10)
        
        self.on_link_down()
        self._done_telem_evts.get(timeout=5)

        self.assertItemsEqual(self._requests_sent.keys(),
                                  self._results_recv.keys())
                
        pending = self.te_client.get_pending()
        self.assertEqual(len(pending), 0)        


