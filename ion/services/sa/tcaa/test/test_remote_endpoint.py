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
from interface.objects import UserInfo


# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_endpoint.py:TestRemoteEndpoint
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_endpoint.py:TestRemoteEndpoint.test_process_queued
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_endpoint.py:TestRemoteEndpoint.test_process_online
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_endpoint.py:TestRemoteEndpoint.test_terrestrial_late
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_endpoint.py:TestRemoteEndpoint.test_service_commands
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_endpoint.py:TestRemoteEndpoint.test_xxx

"""
Example code to dynamically create client to container service.        
https://github.com/ooici/coi-services/blob/master/ion/services/coi/agent_management_service.py#L531        
"""

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
        self._resource_id = 'fake_id'
        self._no_requests = 10
        self._requests_sent = {}
        self._results_recv = {}
        self._no_telem_events = 0
        self._done_evt = AsyncResult()
        self._done_telem_evts = AsyncResult()
        self._cmd_tx_evt = AsyncResult()
        
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
      
    ######################################################################    
    # Helpers.
    ######################################################################    

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
    
    def consume_req(self, res):
        """
        """
        command_id = res['command_id']
        self._results_recv[command_id] = res
        if len(self._results_recv) == self._no_requests:
            self._done_evt.set()
    
    def consume_ack(self, cmd):
        """
        """
        self._requests_sent[cmd.command_id] = cmd
        if len(self._requests_sent) == self._no_requests:
            self._cmd_tx_evt.set()
        
    def terrestrial_server_close(self):
        """
        """
        pass
    
    def terrestrial_client_close(self):
        """
        """
        pass
    
    def make_fake_command(self, no):
        """
        Build a fake command for use in tests.
        """
            
        cmdstr = 'fake_cmd_%i' % no
        cmd = IonObject('RemoteCommand',
                             resource_id=self._resource_id,
                             command=cmdstr,
                             args=['arg1', 23],
                             kwargs={'kwargs1':'someval'},
                             command_id = str(uuid.uuid4()))
        return cmd

    ######################################################################    
    # Tests.
    ######################################################################    

    def test_process_queued(self):
        """
        """        
        
        # Create and enqueue some requests.
        for i in range(self._no_requests):
            cmd = self.make_fake_command(i)
            self._terrestrial_client.enqueue(cmd)

        # Publish a telemetry available event.
        # This will cause the endpoint clients to wake up and connect.
        self.on_link_up()

        # Wait for all the enqueued commands to be acked.
        # Wait for all the responses to arrive.
        self._cmd_tx_evt.get(timeout=self._no_requests*4)
        self._done_evt.get(timeout=self._no_requests*4)

        # Publish a telemetry unavailable event.
        # This will cause the endpoint clients to disconnect and go to sleep.
        self.on_link_down()

        # Confirm the results match the commands sent.
        self.assertItemsEqual(self._requests_sent.keys(),
                                  self._results_recv.keys())
    
    def test_process_online(self):
        """
        """        
        
        # Publish a telemetry available event.
        # This will cause the endpoint clients to wake up and connect.
        self.on_link_up()

        # Wait for the link to be up.
        # The remote side does not publish public telemetry events
        # so we can't wait for that.
        gevent.sleep(1)

        # Create and enqueue some requests.
        for i in range(self._no_requests):
            cmd = self.make_fake_command(i)
            self._terrestrial_client.enqueue(cmd)

        # Wait for all the enqueued commands to be acked.
        # Wait for all the responses to arrive.
        self._cmd_tx_evt.get(timeout=self._no_requests*4)
        self._done_evt.get(timeout=self._no_requests*4)

        # Publish a telemetry unavailable event.
        # This will cause the endpoint clients to disconnect and go to sleep.
        self.on_link_down()

        # Confirm the results match the commands sent.
        self.assertItemsEqual(self._requests_sent.keys(),
                                  self._results_recv.keys())

    def test_terrestrial_late(self):
        """
        """        
        
        # Publish a telemetry available event.
        # This will cause the endpoint clients to wake up and connect.
        self.on_link_up()

        # Wait for the link to be up.
        # The remote side does not publish public telemetry events
        # so we can't wait for that.
        gevent.sleep(1)

        # Manually stop the terrestrial endpoint.
        # This will cause it to be unavailable when commands are queued
        # to simulate stability during asynchronous wake ups.
        self._terrestrial_server.stop()
        self._terrestrial_client.stop()

        # Create and enqueue some requests.
        for i in range(self._no_requests):
            cmd = self.make_fake_command(i)
            self._terrestrial_client.enqueue(cmd)

        # Remote side awaits the terrestrial waking up.
        gevent.sleep(3)

        # Terrestrail endpoint eventually wakes up and starts transmitting.        
        self._terrestrial_client.start('localhost', self._this_port)
        self._terrestrial_server.start('*', self._other_port)
    
        # Wait for all the enqueued commands to be acked.
        # Wait for all the responses to arrive.
        self._cmd_tx_evt.get(timeout=self._no_requests*4)
        self._done_evt.get(timeout=self._no_requests*4)

        # Publish a telemetry unavailable event.
        # This will cause the endpoint clients to disconnect and go to sleep.
        self.on_link_down()

        # Confirm the results match the commands sent.
        self.assertItemsEqual(self._requests_sent.keys(),
                                  self._results_recv.keys())

    #@unittest.skip('Not ready.')
    def test_service_commands(self):
        """
        """
        
        # Publish a telemetry available event.
        # This will cause the endpoint clients to wake up and connect.
        self.on_link_up()

        # Send commands one at a time.
        # Reset queues and events.
        self._no_requests = 1
        self._done_evt = AsyncResult()
        self._cmd_tx_evt = AsyncResult()
        self._requests_sent = {}
        self._results_recv = {}
        
        # Create user object.
        obj = IonObject("UserInfo", name="some_name")
        cmd = IonObject('RemoteCommand',
                             resource_id='',
                             svc_name='resource_registry',
                             command='create',
                             args=[obj],
                             kwargs='',
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd)
        
        # Wait for command request to be acked.
        # Wait for response to arrive.
        self._cmd_tx_evt.get(timeout=5)
        self._done_evt.get(timeout=5)
        
        # Returns obj_id, obj_rev.
        obj_id, obj_rev = self._results_recv[cmd.command_id]['result']
        
        # Confirm the results are valid.
        """
        Result is a tuple of strings.
        {'result': ['ad183ff26bae4f329ddd85fd69d160a9',
        '1-00a308c45fff459c7cda1db9a7314de6'],
        'command_id': 'cc2ae00d-40b0-47d2-af61-8ffb87f1aca2'}
        """
        self.assertIsInstance(obj_id, str)
        self.assertNotEqual(obj_id, '')
        self.assertIsInstance(obj_rev, str)
        self.assertNotEqual(obj_rev, '')
        
        # Send commands one at a time.
        # Reset queues and events.
        self._no_requests = 1
        self._done_evt = AsyncResult()
        self._cmd_tx_evt = AsyncResult()
        self._requests_sent = {}
        self._results_recv = {}

        # Read user object.
        cmd = IonObject('RemoteCommand',
                             resource_id='',
                             svc_name='resource_registry',
                             command='read',
                             args=[obj_id],
                             kwargs='',
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd)

        # Wait for command request to be acked.
        # Wait for response to arrive.
        self._cmd_tx_evt.get(timeout=5)
        self._done_evt.get(timeout=5)

        # Returns read_obj.
        read_obj = self._results_recv[cmd.command_id]['result']
        
        # Confirm the results are valid.
        """
        Result is a user info object with the name set.
        {'lcstate': 'DEPLOYED_AVAILABLE',
        '_rev': '1-851f067bac3c34b2238c0188b3340d0f',
        'description': '',
        'ts_updated': '1349213207638',
        'type_': 'UserInfo',
        'contact': <interface.objects.ContactInformation object at 0x10d7df590>,
        '_id': '27832d93f4cd4535a75ac75c06e00a7e',
        'ts_created': '1349213207638',
        'variables': [{'name': '', 'value': ''}],
        'name': 'some_name'}
        """
        self.assertIsInstance(read_obj, UserInfo)
        self.assertEquals(read_obj.name, 'some_name')
        
        # Send commands one at a time.
        # Reset queues and events.
        self._no_requests = 1
        self._done_evt = AsyncResult()
        self._cmd_tx_evt = AsyncResult()
        self._requests_sent = {}
        self._results_recv = {}

        # Update user object.
        read_obj.name = 'some_other_name'
        cmd = IonObject('RemoteCommand',
                             resource_id='',
                             svc_name='resource_registry',
                             command='update',
                             args=[read_obj],
                             kwargs='',
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd)

        # Wait for command request to be acked.
        # Wait for response to arrive.
        self._cmd_tx_evt.get(timeout=5)
        self._done_evt.get(timeout=5)

        # Returns nothing.
        
        # Send commands one at a time.
        # Reset queues and events.
        self._no_requests = 1
        self._done_evt = AsyncResult()
        self._cmd_tx_evt = AsyncResult()
        self._requests_sent = {}
        self._results_recv = {}

        # Read user object.
        cmd = IonObject('RemoteCommand',
                             resource_id='',
                             svc_name='resource_registry',
                             command='read',
                             args=[obj_id],
                             kwargs='',
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd)        

        # Wait for command request to be acked.
        # Wait for response to arrive.
        self._cmd_tx_evt.get(timeout=5)
        self._done_evt.get(timeout=5)

        # Returns read_obj.
        read_obj = self._results_recv[cmd.command_id]['result']
        
        self.assertIsInstance(read_obj, UserInfo)
        self.assertEquals(read_obj.name, 'some_other_name')
        
        # Send commands one at a time.
        # Reset queues and events.
        self._no_requests = 1
        self._done_evt = AsyncResult()
        self._cmd_tx_evt = AsyncResult()
        self._requests_sent = {}
        self._results_recv = {}
        
        # Delete user object.
        cmd = IonObject('RemoteCommand',
                             resource_id='',
                             svc_name='resource_registry',
                             command='delete',
                             args=[obj_id],
                             kwargs='',
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd)        

        # Wait for command request to be acked.
        # Wait for response to arrive.
        self._cmd_tx_evt.get(timeout=5)
        self._done_evt.get(timeout=5)

        # Returns nothing.
            
        # Publish a telemetry unavailable event.
        # This will cause the endpoint clients to disconnect and go to sleep.
        self.on_link_down()

