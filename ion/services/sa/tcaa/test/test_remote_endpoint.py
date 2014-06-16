#!/usr/bin/env python
"""
@package ion.services.sa.tcaa.test.test_remote_endpoint
@file ion/services/sa/tcaa/test/test_remote_endpoint.py
@author Edward Hunter
@brief Test cases for 2CAA remote endpoint.
"""

__author__ = 'Edward Hunter'


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

# Pyon exceptions.
from pyon.core.exception import IonException
from pyon.core.exception import BadRequest
from pyon.core.exception import ServerError
from pyon.core.exception import NotFound

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
from pyon.agent.agent import ResourceAgentClient
from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent
from interface.objects import AgentCommand
from ion.agents.instrument.driver_int_test_support import DriverIntegrationTestSupport

DEV_ADDR = CFG.device.sbe37.host
DEV_PORT = CFG.device.sbe37.port
DATA_PORT = CFG.device.sbe37.port_agent_data_port
CMD_PORT = CFG.device.sbe37.port_agent_cmd_port
PA_BINARY = CFG.device.sbe37.port_agent_binary
DELIM = CFG.device.sbe37.delim
WORK_DIR = CFG.device.sbe37.workdir
DRV_URI = CFG.device.sbe37.dvr_egg

from ion.agents.instrument.test.agent_test_constants import IA_RESOURCE_ID
from ion.agents.instrument.test.agent_test_constants import IA_NAME
from ion.agents.instrument.test.agent_test_constants import IA_MOD
from ion.agents.instrument.test.agent_test_constants import IA_CLS
from ion.agents.instrument.test.load_test_driver_egg import load_egg
DVR_CONFIG = load_egg()
DRV_MOD = DVR_CONFIG['dvr_mod']
DRV_CLS = DVR_CONFIG['dvr_cls']

# This import will dynamically load the driver egg.  It is needed for the MI includes below
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent

# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_endpoint.py:TestRemoteEndpoint
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_endpoint.py:TestRemoteEndpoint.test_process_queued
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_endpoint.py:TestRemoteEndpoint.test_process_online
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_endpoint.py:TestRemoteEndpoint.test_terrestrial_late
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_endpoint.py:TestRemoteEndpoint.test_service_commands
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_endpoint.py:TestRemoteEndpoint.test_resource_commands
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_endpoint.py:TestRemoteEndpoint.test_bad_service_name_resource_id
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_endpoint.py:TestRemoteEndpoint.test_bad_commands
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_endpoint.py:TestRemoteEndpoint.test_resource_command_sequence

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
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
@unittest.skip('Skipping as this is now out of scope and takes a long time.')
class TestRemoteEndpoint(IonIntegrationTestCase):
    """
    Test cases for 2CAA terrestrial endpoint.
    """
    def setUp(self):
        """
        Start fake terrestrial components and add cleanup.
        Start terrestrial server and retrieve port.
        Set internal variables.
        Start container.
        Start deployment.
        Start container agent.
        Spawn remote endpoint process.
        Create remote endpoint client and retrieve remote server port.
        Create event publisher.
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
        Consume a terrestrial request setting async event when necessary.
        """
        command_id = res['command_id']
        self._results_recv[command_id] = res
        if len(self._results_recv) == self._no_requests:
            self._done_evt.set()
    
    def consume_ack(self, cmd):
        """
        Consume terrestrial ack setting async event when necessary.
        """
        self._requests_sent[cmd.command_id] = cmd
        if len(self._requests_sent) == self._no_requests:
            self._cmd_tx_evt.set()
        
    def terrestrial_server_close(self):
        """
        Callback when terrestrial server closes.
        """
        pass
    
    def terrestrial_client_close(self):
        """
        Callback when terrestrial client closes.
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
                             kwargs={'worktime':3},
                             command_id = str(uuid.uuid4()))
        return cmd

    def start_agent(self):
        """
        Start an instrument agent and client.
        """
        
        log.info('Creating driver integration test support:')
        log.info('driver module: %s', DRV_MOD)
        log.info('driver class: %s', DRV_CLS)
        log.info('device address: %s', DEV_ADDR)
        log.info('device port: %s', DEV_PORT)
        log.info('log delimiter: %s', DELIM)
        log.info('work dir: %s', WORK_DIR)        
        self._support = DriverIntegrationTestSupport(DRV_MOD,
                                                     DRV_CLS,
                                                     DEV_ADDR,
                                                     DEV_PORT,
                                                     DATA_PORT,
                                                     CMD_PORT,
                                                     PA_BINARY,
                                                     DELIM,
                                                     WORK_DIR)
        
        # Start port agent, add stop to cleanup.
        port = self._support.start_pagent()
        log.info('Port agent started at port %i',port)
        
        # Configure driver to use port agent port number.
        DVR_CONFIG['comms_config'] = {
            'addr' : 'localhost',
            'port' : port,
            'cmd_port' : CMD_PORT
        }
        self.addCleanup(self._support.stop_pagent)    
                        
        # Create agent config.
        agent_config = {
            'driver_config' : DVR_CONFIG,
            'stream_config' : {},
            'agent'         : {'resource_id': IA_RESOURCE_ID},
            'test_mode' : True
        }
    
        # Start instrument agent.
        log.debug("Starting IA.")
        container_client = ContainerAgentClient(node=self.container.node,
            name=self.container.name)
    
        ia_pid = container_client.spawn_process(name=IA_NAME,
            module=IA_MOD,
            cls=IA_CLS,
            config=agent_config)
    
        log.info('Agent pid=%s.', str(ia_pid))
    
        # Start a resource agent client to talk with the instrument agent.
    
        self._ia_client = ResourceAgentClient(IA_RESOURCE_ID, process=FakeProcess())
        log.info('Got ia client %s.', str(self._ia_client))                
                
    ######################################################################    
    # Tests.
    ######################################################################    

    def test_process_queued(self):
        """
        test_process_queued
        Test that queued commands are forwarded to and handled by
        remote endpoint when link comes up.
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
        self._cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_evt.get(timeout=CFG.endpoint.receive.timeout)

        # Publish a telemetry unavailable event.
        # This will cause the endpoint clients to disconnect and go to sleep.
        self.on_link_down()

        # Confirm the results match the commands sent.
        self.assertItemsEqual(self._requests_sent.keys(),
                                  self._results_recv.keys())
    
    def test_process_online(self):
        """
        test_process_online
        Test commands are forwarded and handled while link is up.
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
        self._cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_evt.get(timeout=CFG.endpoint.receive.timeout)

        # Publish a telemetry unavailable event.
        # This will cause the endpoint clients to disconnect and go to sleep.
        self.on_link_down()

        # Confirm the results match the commands sent.
        self.assertItemsEqual(self._requests_sent.keys(),
                                  self._results_recv.keys())

    def test_terrestrial_late(self):
        """
        test_terrestrial_late
        Test queued commands are forwarded and handled by remote endpoint
        when terrestrial side is late to come up.
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
        self._cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_evt.get(timeout=CFG.endpoint.receive.timeout)

        # Publish a telemetry unavailable event.
        # This will cause the endpoint clients to disconnect and go to sleep.
        self.on_link_down()

        # Confirm the results match the commands sent.
        self.assertItemsEqual(self._requests_sent.keys(),
                                  self._results_recv.keys())

    def test_service_commands(self):
        """
        test_service_commands
        Test that real service commands are handled by the remote endpoint.
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
        self._cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_evt.get(timeout=CFG.endpoint.receive.timeout)
        
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
        self._cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_evt.get(timeout=CFG.endpoint.receive.timeout)

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
        self._cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_evt.get(timeout=CFG.endpoint.receive.timeout)

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
        self._cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_evt.get(timeout=CFG.endpoint.receive.timeout)

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
        self._cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_evt.get(timeout=CFG.endpoint.receive.timeout)

        # Returns nothing.
            
        # Publish a telemetry unavailable event.
        # This will cause the endpoint clients to disconnect and go to sleep.
        self.on_link_down()

        gevent.sleep(1)
        
    def test_resource_commands(self):
        """
        test_resource_commands
        Test that real resource commands are handled by the remote endpoint.
        """
        
        # Start the IA and check it's out there and behaving.
        self.start_agent()
        
        state = self._ia_client.get_agent_state()
        log.debug('Agent state is: %s', state)
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        retval = self._ia_client.ping_agent()
        log.debug('Agent ping is: %s', str(retval))
        self.assertIn('ping from InstrumentAgent', retval)

        # Publish a telemetry available event.
        # This will cause the endpoint clients to wake up and connect.
        self.on_link_up()

        # Wait for the link to be up.
        # The remote side does not publish public telemetry events
        # so we can't wait for that.
        gevent.sleep(1)

        # Send commands one at a time.
        # Reset queues and events.
        self._no_requests = 1
        self._done_evt = AsyncResult()
        self._cmd_tx_evt = AsyncResult()
        self._requests_sent = {}
        self._results_recv = {}

        # Get agent state via remote endpoint.        
        cmd = IonObject('RemoteCommand',
                             resource_id=IA_RESOURCE_ID,
                             svc_name='',
                             command='get_agent_state',
                             args=[],
                             kwargs={},
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd)
        
        # Wait for command request to be acked.
        # Wait for response to arrive.
        self._cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_evt.get(timeout=CFG.endpoint.receive.timeout)
        
        # Returns agent state.
        state = self._results_recv[cmd.command_id]['result']
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        # Send commands one at a time.
        # Reset queues and events.
        self._no_requests = 1
        self._done_evt = AsyncResult()
        self._cmd_tx_evt = AsyncResult()
        self._requests_sent = {}
        self._results_recv = {}

        # Ping agent via remote endpoint. 
        cmd = IonObject('RemoteCommand',
                             resource_id=IA_RESOURCE_ID,
                             svc_name='',
                             command='ping_agent',
                             args=[],
                             kwargs={},
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd)
        
        # Wait for command request to be acked.
        # Wait for response to arrive.
        self._cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_evt.get(timeout=CFG.endpoint.receive.timeout)
        
        # Returns agent state.
        ping = self._results_recv[cmd.command_id]['result']
        self.assertIn('ping from InstrumentAgent', ping)
        
        # Publish a telemetry unavailable event.
        # This will cause the endpoint clients to disconnect and go to sleep.
        self.on_link_down()

        gevent.sleep(1)

    def test_bad_service_name_resource_id(self):
        """
        test_bad_service_name_resource_id
        Test for proper exception behavior when a bad service name or
        resource id is used in a command forwarded to the remote endpoint.
        """
        
        # Publish a telemetry available event.
        # This will cause the endpoint clients to wake up and connect.
        self.on_link_up()

        # Wait for the link to be up.
        # The remote side does not publish public telemetry events
        # so we can't wait for that.
        gevent.sleep(1)

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
                             svc_name='bogus_service',
                             command='create',
                             args=[obj],
                             kwargs='',
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd)
        
        # Wait for command request to be acked.
        # Wait for response to arrive.
        self._cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_evt.get(timeout=CFG.endpoint.receive.timeout)
        
        # Returns NotFound.
        result = self._results_recv[cmd.command_id]['result']
        self.assertIsInstance(result, NotFound)

        # Send commands one at a time.
        # Reset queues and events.
        self._no_requests = 1
        self._done_evt = AsyncResult()
        self._cmd_tx_evt = AsyncResult()
        self._requests_sent = {}
        self._results_recv = {}

        # Get agent state via remote endpoint.        
        cmd = IonObject('RemoteCommand',
                             resource_id='bogus_resource_id',
                             svc_name='',
                             command='get_agent_state',
                             args=[],
                             kwargs={},
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd)
        
        # Wait for command request to be acked.
        # Wait for response to arrive.
        self._cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_evt.get(timeout=CFG.endpoint.receive.timeout)

        # Returns NotFound.
        result = self._results_recv[cmd.command_id]['result']
        self.assertIsInstance(result, NotFound)

        # Publish a telemetry unavailable event.
        # This will cause the endpoint clients to disconnect and go to sleep.
        self.on_link_down()

        gevent.sleep(1)

    def test_bad_commands(self):
        """
        test_bad_commands
        Test for correct exception behavior if a bad command name is forwarded
        to a remote service or resource.
        """
        
        # Start the IA and check it's out there and behaving.
        self.start_agent()
        
        state = self._ia_client.get_agent_state()
        log.debug('Agent state is: %s', state)
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        retval = self._ia_client.ping_agent()
        log.debug('Agent ping is: %s', str(retval))
        self.assertIn('ping from InstrumentAgent', retval)
        
        # Publish a telemetry available event.
        # This will cause the endpoint clients to wake up and connect.
        self.on_link_up()

        # Wait for the link to be up.
        # The remote side does not publish public telemetry events
        # so we can't wait for that.
        gevent.sleep(1)

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
                             command='what_the_flunk',
                             args=[obj],
                             kwargs='',
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd)
        
        # Wait for command request to be acked.
        # Wait for response to arrive.
        self._cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_evt.get(timeout=CFG.endpoint.receive.timeout)
        
        # Returns BadRequest.
        result = self._results_recv[cmd.command_id]['result']
        self.assertIsInstance(result, BadRequest)

        # Send commands one at a time.
        # Reset queues and events.
        self._no_requests = 1
        self._done_evt = AsyncResult()
        self._cmd_tx_evt = AsyncResult()
        self._requests_sent = {}
        self._results_recv = {}

        # Get agent state via remote endpoint.        
        cmd = IonObject('RemoteCommand',
                             resource_id=IA_RESOURCE_ID,
                             svc_name='',
                             command='what_the_flunk',
                             args=[],
                             kwargs={},
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd)
        
        # Wait for command request to be acked.
        # Wait for response to arrive.
        self._cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_evt.get(timeout=CFG.endpoint.receive.timeout)

        # Returns NotFound.
        result = self._results_recv[cmd.command_id]['result']
        self.assertIsInstance(result, BadRequest)
        
        # Publish a telemetry unavailable event.
        # This will cause the endpoint clients to disconnect and go to sleep.
        self.on_link_down()

        gevent.sleep(1)

    def test_resource_command_sequence(self):
        """
        test_resource_command_sequence
        Test for successful completion of a properly ordered sequence of
        resource commands queued for forwarding to the remote endpoint.
        """
        # Start the IA and check it's out there and behaving.
        self.start_agent()
        
        state = self._ia_client.get_agent_state()
        log.debug('Agent state is: %s', state)
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        retval = self._ia_client.ping_agent()
        log.debug('Agent ping is: %s', str(retval))
        self.assertIn('ping from InstrumentAgent', retval)

        # We execute a sequence of twelve consecutive events.
        self._no_requests = 12

        # Get agent state.
        cmd1 = IonObject('RemoteCommand',
                             resource_id=IA_RESOURCE_ID,
                             svc_name='',
                             command='get_agent_state',
                             args=[],
                             kwargs={},
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd1)
        
        # Initialize agent.
        cmd2 = IonObject('RemoteCommand',
                             resource_id=IA_RESOURCE_ID,
                             svc_name='',
                             command='execute_agent',
                             args=[AgentCommand(command=ResourceAgentEvent.INITIALIZE)],
                             kwargs={},
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd2)
        
        # Get agent state.
        cmd3 = IonObject('RemoteCommand',
                             resource_id=IA_RESOURCE_ID,
                             svc_name='',
                             command='get_agent_state',
                             args=[],
                             kwargs={},
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd3)
        
        # Go active.
        cmd4 = IonObject('RemoteCommand',
                             resource_id=IA_RESOURCE_ID,
                             svc_name='',
                             command='execute_agent',
                             args=[AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)],
                             kwargs={},
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd4)
        
        # Get agent state.
        cmd5 = IonObject('RemoteCommand',
                             resource_id=IA_RESOURCE_ID,
                             svc_name='',
                             command='get_agent_state',
                             args=[],
                             kwargs={},
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd5)
        
        # Run.
        cmd6 = IonObject('RemoteCommand',
                             resource_id=IA_RESOURCE_ID,
                             svc_name='',
                             command='execute_agent',
                             args=[AgentCommand(command=ResourceAgentEvent.RUN)],
                             kwargs={},
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd6)
        
        # Get agent state.
        cmd7 = IonObject('RemoteCommand',
                             resource_id=IA_RESOURCE_ID,
                             svc_name='',
                             command='get_agent_state',
                             args=[],
                             kwargs={},
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd7)
        
        # Acquire sample.
        cmd8 = IonObject('RemoteCommand',
                             resource_id=IA_RESOURCE_ID,
                             svc_name='',
                             command='execute_resource',
                             args=[AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)],
                             kwargs={},
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd8)
        
        # Acquire sample
        cmd9 = IonObject('RemoteCommand',
                             resource_id=IA_RESOURCE_ID,
                             svc_name='',
                             command='execute_resource',
                             args=[AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)],
                             kwargs={},
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd9)
        
        # Acquire sample.
        cmd10 = IonObject('RemoteCommand',
                             resource_id=IA_RESOURCE_ID,
                             svc_name='',
                             command='execute_resource',
                             args=[AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)],
                             kwargs={},
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd10)
        
        # Reset.
        cmd11 = IonObject('RemoteCommand',
                             resource_id=IA_RESOURCE_ID,
                             svc_name='',
                             command='execute_agent',
                             args=[AgentCommand(command=ResourceAgentEvent.RESET)],
                             kwargs={},
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd11)
        
        # Get agent state.
        cmd12 = IonObject('RemoteCommand',
                             resource_id=IA_RESOURCE_ID,
                             svc_name='',
                             command='get_agent_state',
                             args=[],
                             kwargs={},
                             command_id = str(uuid.uuid4()))
        self._terrestrial_client.enqueue(cmd12)

        
        # Publish a telemetry available event.
        # This will cause the endpoint clients to wake up and connect.
        self.on_link_up()
        
        # Wait for command request to be acked.
        # Wait for response to arrive.
        self._cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_evt.get(timeout=CFG.endpoint.receive.timeout)

        # Check results of command sequence.
        """
        0ccf1e10-eeca-400d-aefe-f9d6888ec963   {'result': 'RESOURCE_AGENT_STATE_INACTIVE', 'command_id': '0ccf1e10-eeca-400d-aefe-f9d6888ec963'}
        92531bdf-c2c8-4aa8-817d-5107c7311b37   {'result': <interface.objects.AgentCommandResult object at 0x10d7f11d0>, 'command_id': '92531bdf-c2c8-4aa8-817d-5107c7311b37'}
        509934a1-5038-40d8-8014-591e2d8042b6   {'result': 'RESOURCE_AGENT_STATE_COMMAND', 'command_id': '509934a1-5038-40d8-8014-591e2d8042b6'}
        88bacbb7-5366-4d27-9ecf-fff2bec34b2c   {'result': <interface.objects.AgentCommandResult object at 0x10d389190>, 'command_id': '88bacbb7-5366-4d27-9ecf-fff2bec34b2c'}
        f8b4d3fa-a249-439b-8bd4-ac212b6100aa   {'result': <interface.objects.AgentCommandResult object at 0x10d3893d0>, 'command_id': 'f8b4d3fa-a249-439b-8bd4-ac212b6100aa'}
        8ae98e39-fdb3-4218-ad8f-584620397d9f   {'result': <interface.objects.AgentCommandResult object at 0x10d739990>, 'command_id': '8ae98e39-fdb3-4218-ad8f-584620397d9f'}
        746364a1-c4c7-400f-96d4-ee36df5dc1a4   {'result': BadRequest('Execute argument "command" not set.',), 'command_id': '746364a1-c4c7-400f-96d4-ee36df5dc1a4'}
        d516d3d9-e4f9-4ea5-80e0-34639a6377b5   {'result': <interface.objects.AgentCommandResult object at 0x10d3b2350>, 'command_id': 'd516d3d9-e4f9-4ea5-80e0-34639a6377b5'}
        c7da03f5-59bc-420a-9e10-0a7794266599   {'result': 'RESOURCE_AGENT_STATE_IDLE', 'command_id': 'c7da03f5-59bc-420a-9e10-0a7794266599'}
        678d870a-bf18-424a-afb0-f80ecf3277e2   {'result': <interface.objects.AgentCommandResult object at 0x10d739590>, 'command_id': '678d870a-bf18-424a-afb0-f80ecf3277e2'}
        750c6a30-56eb-4535-99c2-a81fefab1b1f   {'result': 'RESOURCE_AGENT_STATE_COMMAND', 'command_id': '750c6a30-56eb-4535-99c2-a81fefab1b1f'}
        c17bd658-3775-4aa3-8844-02df70a0e3c0   {'result': 'RESOURCE_AGENT_STATE_UNINITIALIZED', 'command_id': 'c17bd658-3775-4aa3-8844-02df70a0e3c0'}
        """        
        
        # First result is a state string.
        result1 = self._results_recv[cmd1.command_id]['result']
        self.assertEqual(result1, ResourceAgentState.UNINITIALIZED)
        
        # Second result is an empty AgentCommandResult.
        result2 = self._results_recv[cmd2.command_id]['result']

        # Third result is a state string.
        result3 = self._results_recv[cmd3.command_id]['result']
        self.assertEqual(result3, ResourceAgentState.INACTIVE)
        
        # Fourth result is an empty AgentCommandResult.
        result4 = self._results_recv[cmd4.command_id]['result']

        # Fifth result is a state string.
        result5 = self._results_recv[cmd5.command_id]['result']
        self.assertEqual(result5, ResourceAgentState.IDLE)

        # Sixth result is an empty AgentCommandResult.
        result6 = self._results_recv[cmd6.command_id]['result']

        # Seventh result is a state string.
        result7 = self._results_recv[cmd7.command_id]['result']
        self.assertEqual(result7, ResourceAgentState.COMMAND)
        
        """
        {'raw': {'quality_flag': 'ok', 'preferred_timestamp': 'driver_timestamp',
        'stream_name': 'raw', 'pkt_format_id': 'JSON_Data',
        'pkt_version': 1, '
        values': [{'binary': True, 'value_id': 'raw',
        'value': 'NzkuNDM3MywxNy4yMDU2NCwgNzYxLjg4NSwgICA2LjIxOTgsIDE1MDYuMzk3LCAwMSBGZWIgMjAwMSwgMDE6MDE6MDA='}],
        'driver_timestamp': 3558286748.8039923},
        'parsed': {'quality_flag': 'ok', 'preferred_timestamp': 'driver_timestamp',
        'stream_name': 'parsed', 'pkt_format_id': 'JSON_Data', 'pkt_version': 1,
        'values': [{'value_id': 'temp', 'value': 79.4373},
        {'value_id': 'conductivity', 'value': 17.20564},
        {'value_id': 'pressure', 'value': 761.885}],
        'driver_timestamp': 3558286748.8039923}}
        """
        
        # Eigth result is an AgentCommandResult containing a sample.
        result8 = self._results_recv[cmd8.command_id]['result']
        self.assertTrue('parsed',result8.result )
        
        # Ninth result is an AgentCommandResult containing a sample.
        result9 = self._results_recv[cmd9.command_id]['result']
        self.assertTrue('parsed',result9.result )

        # Tenth result is an AgentCommandResult containing a sample.
        result10 = self._results_recv[cmd10.command_id]['result']
        self.assertTrue('parsed',result10.result )

        # Eleventh result is an empty AgentCommandResult.
        result11 = self._results_recv[cmd11.command_id]['result']

        # Twelth result is a state string.
        result12 = self._results_recv[cmd12.command_id]['result']
        self.assertEqual(result1, ResourceAgentState.UNINITIALIZED)
        
        # Publish a telemetry unavailable event.
        # This will cause the endpoint clients to disconnect and go to sleep.
        self.on_link_down()

        gevent.sleep(1)
