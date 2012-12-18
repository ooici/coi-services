#!/usr/bin/env python
"""
@package ion.services.sa.tcaa.test.test_remote_client
@file ion/services/sa/tcaa/test/test_remote_client.py
@author Edward Hunter
@brief Test cases for 2CAA remote clients.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Pyon log and config objects.
from pyon.public import log
from pyon.public import CFG

# Standard imports.
import unittest

# 3rd party imports.
import gevent
from gevent import spawn
from gevent.event import AsyncResult
from nose.plugins.attrib import attr
from mock import patch

# Zope interfaces.
from zope.interface import providedBy

# Pyon unittest support.
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase

from pyon.core.exception import ConfigNotFound
from pyon.core.exception import Conflict
from pyon.core.exception import BadRequest

from pyon.public import IonObject
from pyon.util.context import LocalContextMixin
from pyon.event.event import EventPublisher, EventSubscriber
from ion.services.sa.tcaa.terrestrial_endpoint import TerrestrialEndpoint
from ion.services.sa.tcaa.terrestrial_endpoint import TerrestrialEndpointClient
from ion.services.sa.tcaa.remote_endpoint import RemoteEndpoint
from ion.services.sa.tcaa.remote_endpoint import RemoteEndpointClient
from interface.services.icontainer_agent import ContainerAgentClient
from ion.services.sa.tcaa.remote_client import RemoteClient
from interface.objects import TelemetryStatusType
from interface.services.iresource_agent import IResourceAgent
from interface.services.coi.iresource_registry_service import IResourceRegistryService
from interface.services.sa.iterrestrial_endpoint import ITerrestrialEndpoint
from interface.objects import UserInfo

# Agent imports.
from pyon.agent.agent import ResourceAgentClient
from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent
from interface.objects import AgentCommand
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent

# IA config imports.
from ion.agents.instrument.test.test_instrument_agent import DRV_MOD
from ion.agents.instrument.test.test_instrument_agent import DRV_CLS
from ion.agents.instrument.test.test_instrument_agent import DVR_CONFIG
from ion.agents.instrument.test.test_instrument_agent import DEV_ADDR
from ion.agents.instrument.test.test_instrument_agent import DEV_PORT
from ion.agents.instrument.test.test_instrument_agent import DATA_PORT
from ion.agents.instrument.test.test_instrument_agent import CMD_PORT
from ion.agents.instrument.test.test_instrument_agent import PA_BINARY
from ion.agents.instrument.test.test_instrument_agent import DELIM
from ion.agents.instrument.test.test_instrument_agent import WORK_DIR
from ion.agents.instrument.test.test_instrument_agent import IA_RESOURCE_ID
from ion.agents.instrument.test.test_instrument_agent import IA_NAME
from ion.agents.instrument.test.test_instrument_agent import IA_MOD
from ion.agents.instrument.test.test_instrument_agent import IA_CLS

# IA launch imports.
from ion.agents.instrument.driver_int_test_support import DriverIntegrationTestSupport

# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_client.py:TestRemoteClient
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_client.py:TestRemoteClient.test_resource_client_online
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_client.py:TestRemoteClient.test_resource_client_blocking
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_client.py:TestRemoteClient.test_service_client_blocking
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_client.py:TestRemoteClient.test_queue_manipulators
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_client.py:TestRemoteClient.test_errors
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_client.py:TestRemoteClient.test_interfaces



class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''
    
@attr('INT', group='sa')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class TestRemoteClient(IonIntegrationTestCase):
    """
    Test cases for 2CAA remote clients.
    """
    def setUp(self):
        """
        Setup the test parameters.
        Start the container and retrieve client.
        Start the endpoints and resource agent.
        Start publisher and subscribers.
        """
        ###################################################################
        # Internal parameters and container.
        ###################################################################
        
        # Internal parameters.        
        self._terrestrial_platform_id = 'terrestrial_id'
        self._remote_platform_id = 'remote_id'
        self._resource_id = 'fake_id'
        self._xs_name = 'remote1'
        self._terrestrial_svc_name = 'terrestrial_endpoint'
        self._terrestrial_listen_name = self._terrestrial_svc_name + self._xs_name
        self._remote_svc_name = 'remote_endpoint'
        self._remote_listen_name = self._remote_svc_name + self._xs_name
        self._remote_port = 0
        self._terrestrial_port = 0
        self._te_client = None
        self._re_client = None
        self._remote_pid = None
        self._terrestrial_pid = None

        # Async test results.
        self._no_requests = 10
        self._no_telem_evts = 2
        self._no_cmd_tx_evts = self._no_requests
        self._no_queue_mod_evts = self._no_requests
        self._telem_evts = []
        self._queue_mod_evts = []
        self._cmd_tx_evts = []
        self._results_recv = {}
        self._requests_sent = {}
        self._done_telem_evt = AsyncResult()
        self._done_queue_mod_evt = AsyncResult()
        self._done_cmd_tx_evt = AsyncResult()
        self._done_cmd_evt = AsyncResult()

        # Start container.
        log.debug('Staring capability container.')
        self._start_container()
        
        # Bring up services in a deploy file (no need to message).
        log.info('Staring deploy services.')
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Create a container client.
        log.debug('Creating container client.')
        self._container_client = ContainerAgentClient(node=self.container.node,
            name=self.container.name)
        
        ###################################################################
        # Start endpoints, agent.
        ###################################################################

        self._start_terrestrial()
        self._start_remote()
        self._start_agent()
                        
        ###################################################################
        # Assign client ports.
        # This is primarily for test purposes as the IP config in
        # deployment will be fixed in advance.
        ###################################################################
        
        self.te_client.set_client_port(self._remote_port)
        check_port = self.te_client.get_client_port()
        log.debug('Terrestrial client port is: %i', check_port)
    
        self.re_client.set_client_port(self._terrestrial_port)
        check_port = self.re_client.get_client_port()
        log.debug('Remote client port is: %i', check_port)
        
        ###################################################################
        # Start the event publisher and subscribers.
        # Used to send fake agent telemetry publications to the endpoints,
        # and to receive endpoint publications.
        ###################################################################
        self._event_publisher = EventPublisher()

        # Start the event subscriber for remote namespace platform events.
        # This event could be changed to RemoteNamespaceEvent.
        self._event_subscriber = EventSubscriber(
            event_type='PlatformEvent',
            callback=self.consume_event,
            origin=self._xs_name)
        self._event_subscriber.start()
        self._event_subscriber._ready_event.wait(timeout=CFG.endpoint.receive.timeout)
        self.addCleanup(self._event_subscriber.stop)

        # Start the result subscriber for remote resource events.
        self._resource_result_subscriber = EventSubscriber(
            event_type='RemoteCommandResult',
            origin=IA_RESOURCE_ID,
            callback=self.consume_event)
        self._resource_result_subscriber.start()
        self._resource_result_subscriber._ready_event.wait(timeout=CFG.endpoint.receive.timeout)
        self.addCleanup(self._resource_result_subscriber.stop)

        # Start the result subscriber for remote service events.
        self._service_result_subscriber = EventSubscriber(
            event_type='RemoteCommandResult',
            origin='resource_registry' + 'remote1',
            callback=self.consume_event)
        self._service_result_subscriber.start()
        self._service_result_subscriber._ready_event.wait(timeout=CFG.endpoint.receive.timeout)
        self.addCleanup(self._service_result_subscriber.stop)

        # Start the result subscriber for fake resource results.      
        self._fake_result_subscriber = EventSubscriber(
            event_type='RemoteCommandResult',
            origin=self._resource_id,
            callback=self.consume_event)
        self._fake_result_subscriber.start()
        self._fake_result_subscriber._ready_event.wait(timeout=CFG.endpoint.receive.timeout)
        self.addCleanup(self._fake_result_subscriber.stop)

    ###################################################################
    # Start/stop helpers.
    ###################################################################

    def _start_agent(self):
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
            'port' : port
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
        
    def _start_terrestrial(self):
        """
        Start up the terrestrial endpoint.
        """
        # Create terrestrial config.
        terrestrial_endpoint_config = {
            'other_host' : 'localhost',
            'other_port' : self._remote_port,
            'this_port' : self._terrestrial_port,
            'platform_resource_id' : self._terrestrial_platform_id,
            'xs_name' : self._xs_name,
            'process' : {
                'listen_name' : self._terrestrial_listen_name
            }
        }
        
        # Spawn the terrestrial enpoint process.
        log.debug('Spawning terrestrial endpoint process.')
        self._terrestrial_pid = self._container_client.spawn_process(
            name=self._terrestrial_listen_name,
            module='ion.services.sa.tcaa.terrestrial_endpoint',
            cls='TerrestrialEndpoint',
            config=terrestrial_endpoint_config)
        log.debug('Terrestrial endpoint pid=%s.', str(self._terrestrial_pid))

        # Create a terrestrial client.
        self.te_client = TerrestrialEndpointClient(
            process=FakeProcess(),
            to_name=self._terrestrial_listen_name)
        log.debug('Got te client %s.', str(self.te_client))
        self._terrestrial_port = self.te_client.get_port()
        log.debug('Terrestrial port is: %i', self._terrestrial_port)
        
    def _start_remote(self):
        """
        Start up the remote endpoint.
        """        
        # Create agent config.
        remote_endpoint_config = {
            'other_host' : 'localhost',
            'other_port' : self._terrestrial_port,
            'this_port' : self._remote_port,
            'platform_resource_id' : self._remote_platform_id,
            'xs_name' : self._xs_name,
            'process' : {
                'listen_name' : self._remote_listen_name
            }
        }
        
        # Spawn the remote enpoint process.
        log.debug('Spawning remote endpoint process.')
        self._remote_pid = self._container_client.spawn_process(
            name=self._remote_listen_name,
            module='ion.services.sa.tcaa.remote_endpoint',
            cls='RemoteEndpoint',
            config=remote_endpoint_config)
        log.debug('Remote endpoint pid=%s.', str(self._remote_pid))

        # Create an endpoint client.
        self.re_client = RemoteEndpointClient(
            process=FakeProcess(),
            to_name=self._remote_listen_name)
        log.debug('Got re client %s.', str(self.re_client))
        
        # Remember the remote port.
        self._remote_port = self.re_client.get_port()
        log.debug('The remote port is: %i.', self._remote_port)
        
        
    ###################################################################
    # Telemetry publications to start/top endpoint.
    # (Normally be published by appropriate platform agents.)
    ###################################################################

    def terrestrial_link_up(self):
        """
        Publish telemetry available to the terrestrial endpoint.
        """
        # Publish a link up event to be caught by the terrestrial endpoint.
        log.debug('Publishing terrestrial telemetry available event.')
        self._event_publisher.publish_event(
                            event_type='PlatformTelemetryEvent',
                            origin=self._terrestrial_platform_id,
                            status = TelemetryStatusType.AVAILABLE)
        
    def terrestrial_link_down(self):
        """
        Publish telemetry unavailable to the terrestrial endpoint.
        """
        # Publish a link up event to be caught by the terrestrial endpoint.
        log.debug('Publishing terrestrial telemetry unavailable event.')
        self._event_publisher.publish_event(
                            event_type='PlatformTelemetryEvent',
                            origin=self._terrestrial_platform_id,
                            status = TelemetryStatusType.UNAVAILABLE)
    
    def remote_link_up(self):
        """
        Publish telemetry available to the remote endpoint.
        """
        # Publish a link up event to be caught by the remote endpoint.
        log.debug('Publishing remote telemetry available event.')
        self._event_publisher.publish_event(
                            event_type='PlatformTelemetryEvent',
                            origin=self._remote_platform_id,
                            status = TelemetryStatusType.AVAILABLE)
    
    def remote_link_down(self):
        """
        Publish telemetry unavailable to the remote endpoint.
        """
        # Publish a link down event to be caught by the remote endpoint.
        log.debug('Publishing remote telemetry unavailable event.')
        self._event_publisher.publish_event(
                            event_type='PlatformTelemetryEvent',
                            origin=self._remote_platform_id,
                            status = TelemetryStatusType.UNAVAILABLE)
    
    def consume_event(self, evt, *args, **kwargs):
        """
        Test callback for events.
        """
        log.debug('Test got event: %s, args: %s, kwargs: %s',
                  str(evt), str(args), str(kwargs))
        
        if evt.type_ == 'PublicPlatformTelemetryEvent':
            self._telem_evts.append(evt)
            if self._no_telem_evts > 0 and self._no_telem_evts == len(self._telem_evts):
                    self._done_telem_evt.set()
                    
        elif evt.type_ == 'RemoteQueueModifiedEvent':
            self._queue_mod_evts.append(evt)
            if self._no_queue_mod_evts > 0 and self._no_queue_mod_evts == len(self._queue_mod_evts):
                    self._done_queue_mod_evt.set()
            
        elif evt.type_ == 'RemoteCommandTransmittedEvent':
            self._cmd_tx_evts.append(evt)
            if self._no_cmd_tx_evts > 0 and self._no_cmd_tx_evts == len(self._cmd_tx_evts):
                    self._done_cmd_tx_evt.set()
        
        elif evt.type_ == 'RemoteCommandResult':
            cmd = evt.command
            self._results_recv[cmd.command_id] = cmd
            if len(self._results_recv) == self._no_requests:
                self._done_cmd_evt.set()
        
    ###################################################################
    # Misc helpers.
    ###################################################################
        
    def make_fake_command(self, no):
        """
        Build a fake command for use in tests.
        """
            
        cmdstr = 'fake_cmd_%i' % no
        cmd = IonObject('RemoteCommand',
                             resource_id='fake_id',
                             command=cmdstr,
                             args=['arg1', 23],
                             kwargs={'kwargs1':'someval'})
        return cmd

    ###################################################################
    # Tests.
    ###################################################################

    def test_resource_client_online(self):
        """
        test_recourse_client_online
        Test the client transparently forwards commands to the remote
        resource while link is up.
        """

        # Publish link up events.
        self.terrestrial_link_up()
        self.remote_link_up()
        
        remote_client = RemoteClient(iface=IResourceAgent, xs_name=self._xs_name,
            resource_id='fake_id', process=FakeProcess())
        
        # Queue up a series of fake commands to be handled by the remote side.
        for i in range(self._no_requests):
            cmd = remote_client.ping_agent()
            self._requests_sent[cmd.command_id] = cmd
            
        # Block on queue mod events.
        self._done_queue_mod_evt.get(timeout=CFG.endpoint.receive.timeout)
        
        # Block on command transmissions and results.
        self._done_cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)                
        self._done_cmd_evt.get(timeout=CFG.endpoint.receive.timeout)
        
        # Publish link down events.
        self.terrestrial_link_down()
        self.remote_link_down()
        
        # Block on terrestrial public telemetry events.
        self._done_telem_evt.get(timeout=CFG.endpoint.receive.timeout)

        self.assertItemsEqual(self._requests_sent.keys(),
                                  self._results_recv.keys())
    
    #@unittest.skip('For some reason this bastard wont run on the builder.')
    def test_resource_client_blocking(self):
        """
        test_resource_client_blocking
        Test the client can block on remote resource command results.
        """

        # Publish link up events.
        self.terrestrial_link_up()
        self.remote_link_up()
        
        remote_client = RemoteClient(iface=IResourceAgent, xs_name=self._xs_name,
            resource_id=IA_RESOURCE_ID, process=FakeProcess())
        
        # Queue up a series of fake commands to be handled by the remote side.
        """
        {'time_completed': 1350421095.804607, 'resource_id': '123xyz',
        'time_queued': 1350421095.623531, 'args': [], 'type_': 'RemoteCommand',
        'command': 'ping_agent', 'result': 'ping from InstrumentAgent
        (name=Agent007,id=Edwards-MacBook-Pro_local_10126.35,type=agent),
        time: 1350421095757', 'kwargs': {}, 'svc_name': '',
        'command_id': '76be11b4-a22c-49de-89cd-4e019463d7c9'}
        """
        for i in range(self._no_requests):
            cmd = remote_client.ping_agent(remote_timeout=CFG.endpoint.receive.timeout)
            self.assertEqual(cmd.resource_id, IA_RESOURCE_ID)
            self.assertEqual(cmd.command, 'ping_agent')
            self.assertIn('ping from InstrumentAgent', cmd.result)
        
        # Publish link down events.
        self.terrestrial_link_down()
        self.remote_link_down()
        
        # Block on terrestrial public telemetry events.
        self._done_telem_evt.get(timeout=CFG.endpoint.receive.timeout)

    #@unittest.skip('For some reason this bastard wont run on the builder.')
    def test_service_client_blocking(self):
        """
        test_service_client_blocking
        Test the client can command remote services and block on their
        results.
        """

        # Publish link up events.
        self.terrestrial_link_up()
        self.remote_link_up()

        # Create remote client for the resource registry.
        remote_client = RemoteClient(iface=IResourceRegistryService,
            xs_name=self._xs_name, svc_name='resource_registry',
            process=FakeProcess())

        # Create user object.
        obj = IonObject("UserInfo", name="some_name")
        result = remote_client.create(obj,
            remote_timeout=CFG.endpoint.receive.timeout)

        # Returns obj_id, obj_rev.
        obj_id, obj_rev = result.result
        
        # Confirm the results are valid.
        
        #Result is a tuple of strings.
        #{'result': ['ad183ff26bae4f329ddd85fd69d160a9',
        #'1-00a308c45fff459c7cda1db9a7314de6'],
        #'command_id': 'cc2ae00d-40b0-47d2-af61-8ffb87f1aca2'}
        
        self.assertIsInstance(obj_id, str)
        self.assertNotEqual(obj_id, '')
        self.assertIsInstance(obj_rev, str)
        self.assertNotEqual(obj_rev, '')

        # Read user object.
        result = remote_client.read(obj_id,
            remote_timeout=CFG.endpoint.receive.timeout)

        # Returns read_obj.
        read_obj = result.result
        
        # Confirm the results are valid.
        
        #Result is a user info object with the name set.
        #{'lcstate': 'DEPLOYED_AVAILABLE',
        #'_rev': '1-851f067bac3c34b2238c0188b3340d0f',
        #'description': '',
        #'ts_updated': '1349213207638',
        #'type_': 'UserInfo',
        #'contact': <interface.objects.ContactInformation object at 0x10d7df590>,
        #'_id': '27832d93f4cd4535a75ac75c06e00a7e',
        #'ts_created': '1349213207638',
        #'variables': [{'name': '', 'value': ''}],
        #'name': 'some_name'}
        
        self.assertIsInstance(read_obj, UserInfo)
        self.assertEquals(read_obj.name, 'some_name')

        # Update user object.
        read_obj.name = 'some_other_name'
        result = remote_client.update(read_obj,
            remote_timeout=CFG.endpoint.receive.timeout)
        
        # Returns nothing.

        # Read user object.
        result = remote_client.read(obj_id,
            remote_timeout=CFG.endpoint.receive.timeout)

        # Returns read_obj.
        read_obj = result.result
        
        # Confirm results are valid.        
        self.assertIsInstance(read_obj, UserInfo)
        self.assertEquals(read_obj.name, 'some_other_name')

        # Delete user object.
        result = remote_client.delete(obj_id,
            remote_timeout=CFG.endpoint.receive.timeout)
        
        # Returns nothing.

        # Publish link down events.
        self.terrestrial_link_down()
        self.remote_link_down()
        
        # Block on terrestrial public telemetry events.
        self._done_telem_evt.get(timeout=CFG.endpoint.receive.timeout)

    def test_queue_manipulators(self):
        """
        test_queue_manipulators
        Test ability to instpect and manipulate the command queue corresponding
        to this resource or service.
        """

        remote_client = RemoteClient(iface=IResourceAgent, xs_name=self._xs_name,
            resource_id='fake_id', process=FakeProcess())
        
        # Queue up a series of fake commands to be handled by the remote side.
        for i in range(self._no_requests):
            cmd = remote_client.ping_agent(link=False)
            self._requests_sent[cmd.command_id] = cmd

        # Block on queue mod events.
        self._done_queue_mod_evt.get(timeout=CFG.endpoint.receive.timeout)

        queue = remote_client.get_queue()
        self.assertEqual(len(queue), self._no_requests)

        popped = remote_client.clear_queue()
        self.assertEqual(len(popped), self._no_requests)

        self._requests_sent = {}

        # Queue up a series of fake commands to be handled by the remote side.
        for i in range(self._no_requests):
            cmd = remote_client.ping_agent(link=False)
            self._requests_sent[cmd.command_id] = cmd

        # Pop the last three commands.
        cmd_ids = self._requests_sent.keys()[:3]
        poped = []
        for x in cmd_ids:
            poped.append(remote_client.pop_queue(x))
            self._requests_sent.pop(x)

        queue = remote_client.get_queue()
        self.assertEqual(len(queue), self._no_requests - 3)
        self.assertEqual(len(poped), 3)

        self._no_requests = self._no_requests - 3
        self._no_cmd_tx_evts = self._no_requests

        # Publish link up events.
        self.terrestrial_link_up()
        self.remote_link_up()            
        
        # Block on command transmissions and results.
        self._done_cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        pending = remote_client.get_pending()
        for x in pending:
            self.assertIn(x.command_id, self._requests_sent.keys())
        self._done_cmd_evt.get(timeout=CFG.endpoint.receive.timeout)
        
        # Publish link down events.
        self.terrestrial_link_down()
        self.remote_link_down()
        
        # Block on terrestrial public telemetry events.
        self._done_telem_evt.get(timeout=CFG.endpoint.receive.timeout)

        self.assertItemsEqual(self._requests_sent.keys(),
                                  self._results_recv.keys())

    def test_errors(self):
        """
        test_errors
        Test various error conditions.
        """
        
        # Constructed without a xs name.
        with self.assertRaises(ConfigNotFound):
            remote_client = RemoteClient(iface=IResourceAgent,
                    resource_id=IA_RESOURCE_ID, process=FakeProcess())
        
        # Constructed without an interface.
        with self.assertRaises(ConfigNotFound):
            remote_client = RemoteClient(xs_name=self._xs_name,
                    resource_id=IA_RESOURCE_ID, process=FakeProcess())

        # Construct with invalid interface.
        with self.assertRaises(ConfigNotFound):
            remote_client = RemoteClient('Bogus_interface', xs_name=self._xs_name,
            resource_id=IA_RESOURCE_ID, process=FakeProcess())
        
        # Construct with no resource or service specified.
        with self.assertRaises(ConfigNotFound):
            remote_client = RemoteClient(iface=IResourceAgent, xs_name=self._xs_name,
                    process=FakeProcess())

        # Construct with both resource and service specified.
        with self.assertRaises(ConfigNotFound):
            remote_client = RemoteClient(iface=IResourceAgent, xs_name=self._xs_name,
                    resource_id=IA_RESOURCE_ID, svc_name='resource_registry', process=FakeProcess())

        # Create a valid resource client.
        remote_client = RemoteClient(iface=IResourceAgent, xs_name=self._xs_name,
            resource_id=IA_RESOURCE_ID, process=FakeProcess())
        
        # Send a command while link is down.
        with self.assertRaises(Conflict):
            result = remote_client.ping_agent()

        # Test port manipulators refused by remote proxy client.
        with self.assertRaises(BadRequest):
            remote_client.get_port()
        with self.assertRaises(BadRequest):
            remote_client.set_client_port()
        with self.assertRaises(BadRequest):
            remote_client.get_client_port()

    def test_interfaces(self):
        """
        test_interfaces
        Test that the client declare the correct interfaces.
        """
        remote_client = RemoteClient(iface=IResourceAgent, xs_name=self._xs_name,
            resource_id='fake_id', process=FakeProcess())

        interfaces = providedBy(remote_client)
        self.assertIn(IResourceAgent, interfaces)
        self.assertIn(ITerrestrialEndpoint, interfaces)
        
