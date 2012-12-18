#!/usr/bin/env python
"""
@package ion.services.sa.tcaa.test.test_2caa
@file ion/services/sa/tcaa/test/test_2caa.py
@author Edward Hunter
@brief Test cases combined 2CAA endpoints.
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

# Pyon unittest support.
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from pyon.util.context import LocalContextMixin

# Pyon exceptions.
from pyon.core.exception import IonException
from pyon.core.exception import BadRequest
from pyon.core.exception import ServerError
from pyon.core.exception import NotFound

# Endpoints.
from ion.services.sa.tcaa.remote_endpoint import RemoteEndpoint
from ion.services.sa.tcaa.terrestrial_endpoint import TerrestrialEndpoint
from ion.services.sa.tcaa.remote_endpoint import RemoteEndpointClient
from ion.services.sa.tcaa.terrestrial_endpoint import TerrestrialEndpointClient
from interface.services.icontainer_agent import ContainerAgentClient

# Publishers, subscribers.
from pyon.event.event import EventPublisher, EventSubscriber
from interface.objects import TelemetryStatusType

# Objects.
from pyon.public import IonObject
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
from ion.agents.instrument.test.test_instrument_agent import start_instrument_agent_process
from ion.agents.instrument.driver_int_test_support import DriverIntegrationTestSupport

# bin/nosetests -s -v --nologcapture ion/services/sa/tcaa/test/test_2caa.py:Test2CAA
# bin/nosetests -s -v --nologcapture ion/services/sa/tcaa/test/test_2caa.py:Test2CAA.test_queued_fake
# bin/nosetests -s -v --nologcapture ion/services/sa/tcaa/test/test_2caa.py:Test2CAA.test_process_online
# bin/nosetests -s -v --nologcapture ion/services/sa/tcaa/test/test_2caa.py:Test2CAA.test_remote_late
# bin/nosetests -s -v --nologcapture ion/services/sa/tcaa/test/test_2caa.py:Test2CAA.test_resource_commands
# bin/nosetests -s -v --nologcapture ion/services/sa/tcaa/test/test_2caa.py:Test2CAA.test_service_command_sequence


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''
        
@attr('INT', group='sa')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class Test2CAA(IonIntegrationTestCase):
    """
    Test cases for 2CAA terrestrial endpoint.
    """
    def setUp(self):
        """
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
                             kwargs={'worktime':3})
        return cmd
        
    ###################################################################
    # Tests.
    ###################################################################

    def test_queued_fake(self):
        """
        test_queued_fake
        Test fake resource commands queued prior to linkup.
        """
                
        # Queue up a series of fake commands to be handled by the remote side.
        for i in range(self._no_requests):
            cmd = self.make_fake_command(i)
            cmd = self.te_client.enqueue_command(cmd)
            self._requests_sent[cmd.command_id] = cmd

        # Block on queue mod events.
        self._done_queue_mod_evt.get(timeout=CFG.endpoint.receive.timeout)

        # Publish link up events.
        self.terrestrial_link_up()
        self.remote_link_up()
        
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

    # The following error occurs when queue persistence is enabled.
    # Need to verify correct solution to enable persistent queues.
    """
    2012-11-06 14:27:13,517 INFO     pyon.container.procs ProcManager.terminate_process: org_management -> pid=Edwards-MacBook-Pro_local_8975.8
    Traceback (most recent call last):
      File "/Users/edward/Documents/Dev/code/coi-services/eggs/gevent-0.13.7-py2.7-macosx-10.5-intel.egg/gevent/greenlet.py", line 390, in run
        result = self._run(*self.args, **self.kwargs)
      File "/Users/edward/Documents/Dev/code/coi-services/ion/services/sa/tcaa/remote_endpoint.py", line 97, in command_loop
        self._callback(cmd_result)
      File "/Users/edward/Documents/Dev/code/coi-services/ion/services/sa/tcaa/remote_endpoint.py", line 284, in _result_complete
        self._client.enqueue(result)
    AttributeError: 'NoneType' object has no attribute 'enqueue'
    <Greenlet at 0x1059ca9b0: command_loop> failed with AttributeError
    
    2012-11-06 14:27:13,586 INFO     pyon.container.procs ProcManager.terminate_process: exchange_management -> pid=Edwards-MacBook-Pro_local_8975.7
    2012-11-06 14:27:13,665 INFO     pyon.container.procs ProcManager.terminate_process: policy_management -> pid=Edwards-MacBook-Pro_local_8975.6
    2012-11-06 14:27:13,739 INFO     pyon.container.procs ProcManager.terminate_process: identity_management -> pid=Edwards-MacBook-Pro_local_8975.5
    2012-11-06 14:27:13,807 INFO     pyon.container.procs ProcManager.terminate_process: directory -> pid=Edwards-MacBook-Pro_local_8975.4
    2012-11-06 14:27:13,874 INFO     pyon.container.procs ProcManager.terminate_process: resource_registry -> pid=Edwards-MacBook-Pro_local_8975.3
    2012-11-06 14:27:13,941 INFO     pyon.container.procs ProcManager.terminate_process: event_persister -> pid=Edwards-MacBook-Pro_local_8975.1
    2012-11-06 14:27:13,945 INFO     pyon.event.event EventSubscriber stopped. Event pattern=#
    2012-11-06 14:27:14,124 INFO     pyon.datastore.couchdb.couchdb_standalone Connecting to CouchDB server: http://localhost:5984
    2012-11-06 14:27:14,399 INFO     pyon.datastore.couchdb.couchdb_standalone Closing connection to CouchDB
    
    ======================================================================
    ERROR: test_process_online
    ----------------------------------------------------------------------
    Traceback (most recent call last):
      File "/Users/edward/Documents/Dev/code/coi-services/eggs/mock-0.8.0-py2.7.egg/mock.py", line 1605, in _inner
        return f(*args, **kw)
      File "/Users/edward/Documents/Dev/code/coi-services/ion/services/sa/tcaa/test/test_2caa.py", line 492, in test_process_online
        cmd = self.te_client.enqueue_command(cmd)
      File "/Users/edward/Documents/Dev/code/coi-services/interface/services/sa/iterrestrial_endpoint.py", line 188, in enqueue_command
        return self.request(IonObject('terrestrial_endpoint_enqueue_command_in', **{'command': command or None,'link': link}), op='enqueue_command', headers=headers, timeout=timeout)
      File "/Users/edward/Documents/Dev/code/coi-services/extern/pyon/pyon/net/endpoint.py", line 1012, in request
        return RequestResponseClient.request(self, msg, headers=headers, timeout=timeout)
      File "/Users/edward/Documents/Dev/code/coi-services/extern/pyon/pyon/net/endpoint.py", line 822, in request
        retval, headers = e.send(msg, headers=headers, timeout=timeout)
      File "/Users/edward/Documents/Dev/code/coi-services/extern/pyon/pyon/ion/conversation.py", line 310, in send
        result_data, result_headers = c.send(self.conv_type.server_role, msg, headers, **kwargs)
      File "/Users/edward/Documents/Dev/code/coi-services/extern/pyon/pyon/ion/conversation.py", line 126, in send
        return self._invite_and_send(to_role, msg, headers, **kwargs)
      File "/Users/edward/Documents/Dev/code/coi-services/extern/pyon/pyon/ion/conversation.py", line 145, in _invite_and_send
        return self._send(to_role, to_role_name, msg, header, **kwargs)
      File "/Users/edward/Documents/Dev/code/coi-services/extern/pyon/pyon/ion/conversation.py", line 169, in _send
        return self._end_point_unit._message_send(msg, headers, **kwargs)
      File "/Users/edward/Documents/Dev/code/coi-services/extern/pyon/pyon/ion/conversation.py", line 289, in _message_send
        return ProcessRPCRequestEndpointUnit.send(self, msg, headers,  **kwargs)
      File "/Users/edward/Documents/Dev/code/coi-services/extern/pyon/pyon/net/endpoint.py", line 134, in send
        return self._send(_msg, _header, **kwargs)
      File "/Users/edward/Documents/Dev/code/coi-services/extern/pyon/pyon/net/endpoint.py", line 880, in _send
        raise ex
    Conflict: 409 - Object not based on most current version
    
    """

    def test_process_online(self):
        """
        test_process_online
        Test fake resource commands queued while link is up.
        """
                
        # Publish link up events.
        self.terrestrial_link_up()
        self.remote_link_up()

        # Queue up a series of fake commands to be handled by the remote side.
        for i in range(self._no_requests):
            cmd = self.make_fake_command(i)
            cmd = self.te_client.enqueue_command(cmd)
            self._requests_sent[cmd.command_id] = cmd

        # Block on queue mods, command transmissions and results.
        self._done_queue_mod_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)                
        self._done_cmd_evt.get(timeout=CFG.endpoint.receive.timeout)
        
        # Publish link down events.
        self.terrestrial_link_down()
        self.remote_link_down()
        
        # Block on terrestrial public telemetry events.
        self._done_telem_evt.get(timeout=CFG.endpoint.receive.timeout)

        self.assertItemsEqual(self._requests_sent.keys(),
                                  self._results_recv.keys())

    def test_remote_late(self):
        """
        test_remote_late
        Test fake resource commands queued prior to linkup.
        Delay remote side linkup substantially to test terrestrial
        behavior when remote server not initially available.
        """
                
        for i in range(self._no_requests):
            cmd = self.make_fake_command(i)
            cmd = self.te_client.enqueue_command(cmd)
            self._requests_sent[cmd.command_id] = cmd

        # Block on queue mod events.
        self._done_queue_mod_evt.get(timeout=CFG.endpoint.receive.timeout)

        # Actually stop the remote process, since the server is
        # available even if it hasn't linked up yet and the test is running
        # in the same container. The test will remember the appropriate
        # remote port numbers.
        self._container_client.terminate_process(self._remote_pid)
        self.terrestrial_link_up()
        gevent.sleep(10)
        
        # Restart remote side and publish remote link up.
        self._start_remote()
        self.remote_link_up()
        
        # Block for transmission and result events.
        self._done_cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)                
        self._done_cmd_evt.get(timeout=CFG.endpoint.receive.timeout)
        
        # Publish link down events.
        self.terrestrial_link_down()
        self.remote_link_down()
        
        self._done_telem_evt.get(timeout=CFG.endpoint.receive.timeout)

        self.assertItemsEqual(self._requests_sent.keys(),
                                  self._results_recv.keys())
        
    def test_resource_commands(self):
        """
        test_resource_commands
        """

        # Set up to verify the two commands queued.        
        self._no_requests = 2
        self._no_telem_evts = 2
        self._no_cmd_tx_evts = self._no_requests
        self._no_queue_mod_evts = self._no_requests

        # Use IA client to verify IA.
        state = self._ia_client.get_agent_state()
        log.debug('Agent state is: %s', state)
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        retval = self._ia_client.ping_agent()
        log.debug('Agent ping is: %s', str(retval))
        self.assertIn('ping from InstrumentAgent', retval)

        # Create and enqueue commands.
        state_cmd = IonObject('RemoteCommand',
                             resource_id=IA_RESOURCE_ID,
                             svc_name='',
                             command='get_agent_state',
                             args=[],
                             kwargs={})
        state_cmd = self.te_client.enqueue_command(state_cmd)
        self._requests_sent[state_cmd.command_id] = state_cmd

        ping_cmd = IonObject('RemoteCommand',
                             resource_id=IA_RESOURCE_ID,
                             svc_name='',
                             command='ping_agent',
                             args=[],
                             kwargs={})
        ping_cmd = self.te_client.enqueue_command(ping_cmd)
        self._requests_sent[ping_cmd.command_id] = ping_cmd

        # Block on queue mod events.
        self._done_queue_mod_evt.get(timeout=CFG.endpoint.receive.timeout)

        # Publish link up events.
        self.terrestrial_link_up()
        self.remote_link_up()

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
        
        self.assertEqual(self._results_recv[state_cmd.command_id].result,
                         ResourceAgentState.UNINITIALIZED)
        self.assertIn('ping from InstrumentAgent',
                      self._results_recv[ping_cmd.command_id].result)
        
    def test_service_command_sequence(self):
        """
        test_service_commands
        """
        # Publish link up events.
        self.terrestrial_link_up()
        self.remote_link_up()

        # Send commands one at a time.
        # Reset queues and events.
        self._no_requests = 1
        self._no_cmd_tx_evts = self._no_requests
        self._no_queue_mod_evts = self._no_requests
        self._done_queue_mod_evt = AsyncResult()
        self._done_cmd_tx_evt = AsyncResult()
        self._done_cmd_evt = AsyncResult()
        self._queue_mod_evts = []        
        self._cmd_tx_evts = []
        self._requests_sent = {}
        self._results_recv = {}

        # Create user object.
        obj = IonObject("UserInfo", name="some_name")
        cmd = IonObject('RemoteCommand',
                             resource_id='',
                             svc_name='resource_registry',
                             command='create',
                             args=[obj],
                             kwargs='')
        cmd = self.te_client.enqueue_command(cmd)

        # Block for the queue to be modified, the command to be transmitted,
        # and the result to be received.
        self._done_queue_mod_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_cmd_evt.get(timeout=CFG.endpoint.receive.timeout)

        # Returns obj_id, obj_rev.
        obj_id, obj_rev = self._results_recv[cmd.command_id].result
        
        # Confirm the results are valid.
        
        #Result is a tuple of strings.
        #{'result': ['ad183ff26bae4f329ddd85fd69d160a9',
        #'1-00a308c45fff459c7cda1db9a7314de6'],
        #'command_id': 'cc2ae00d-40b0-47d2-af61-8ffb87f1aca2'}
        
        self.assertIsInstance(obj_id, str)
        self.assertNotEqual(obj_id, '')
        self.assertIsInstance(obj_rev, str)
        self.assertNotEqual(obj_rev, '')
        
        # Send commands one at a time.
        # Reset queues and events.
        self._no_requests = 1
        self._no_cmd_tx_evts = self._no_requests
        self._no_queue_mod_evts = self._no_requests
        self._done_queue_mod_evt = AsyncResult()
        self._done_cmd_tx_evt = AsyncResult()
        self._done_cmd_evt = AsyncResult()
        self._queue_mod_evts = []        
        self._cmd_tx_evts = []
        self._requests_sent = {}
        self._results_recv = {}

        # Create user object.
        cmd = IonObject('RemoteCommand',
                             resource_id='',
                             svc_name='resource_registry',
                             command='read',
                             args=[obj_id],
                             kwargs='')
        cmd = self.te_client.enqueue_command(cmd)

        # Block for the queue to be modified, the command to be transmitted,
        # and the result to be received.
        self._done_queue_mod_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_cmd_evt.get(timeout=CFG.endpoint.receive.timeout)        
                
        # Returns read_obj.
        read_obj = self._results_recv[cmd.command_id].result
        
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

        # Send commands one at a time.
        # Reset queues and events.
        self._no_requests = 1
        self._no_cmd_tx_evts = self._no_requests
        self._no_queue_mod_evts = self._no_requests
        self._done_queue_mod_evt = AsyncResult()
        self._done_cmd_tx_evt = AsyncResult()
        self._done_cmd_evt = AsyncResult()
        self._queue_mod_evts = []
        self._cmd_tx_evts = []        
        self._requests_sent = {}
        self._results_recv = {}

        # Create user object.
        read_obj.name = 'some_other_name'
        cmd = IonObject('RemoteCommand',
                             resource_id='',
                             svc_name='resource_registry',
                             command='update',
                             args=[read_obj],
                             kwargs='')
        cmd = self.te_client.enqueue_command(cmd)

        # Block for the queue to be modified, the command to be transmitted,
        # and the result to be received.
        self._done_queue_mod_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_cmd_evt.get(timeout=CFG.endpoint.receive.timeout)        

        # Returns nothing.

        # Send commands one at a time.
        # Reset queues and events.
        self._no_requests = 1
        self._no_cmd_tx_evts = self._no_requests
        self._no_queue_mod_evts = self._no_requests
        self._done_queue_mod_evt = AsyncResult()
        self._done_cmd_tx_evt = AsyncResult()
        self._done_cmd_evt = AsyncResult()
        self._queue_mod_evts = []
        self._cmd_tx_evts = []        
        self._requests_sent = {}
        self._results_recv = {}

        # Create user object.
        cmd = IonObject('RemoteCommand',
                             resource_id='',
                             svc_name='resource_registry',
                             command='read',
                             args=[obj_id],
                             kwargs='')
        cmd = self.te_client.enqueue_command(cmd)

        # Block for the queue to be modified, the command to be transmitted,
        # and the result to be received.
        self._done_queue_mod_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_cmd_evt.get(timeout=CFG.endpoint.receive.timeout)        
        
        # Returns read_obj.
        read_obj = self._results_recv[cmd.command_id].result
        
        self.assertIsInstance(read_obj, UserInfo)
        self.assertEquals(read_obj.name, 'some_other_name')
        
        # Send commands one at a time.
        # Reset queues and events.
        self._no_requests = 1
        self._no_cmd_tx_evts = self._no_requests
        self._no_queue_mod_evts = self._no_requests
        self._done_queue_mod_evt = AsyncResult()
        self._done_cmd_tx_evt = AsyncResult()
        self._done_cmd_evt = AsyncResult()
        self._queue_mod_evts = []
        self._cmd_tx_evts = []        
        self._requests_sent = {}
        self._results_recv = {}

        # Create user object.
        cmd = IonObject('RemoteCommand',
                             resource_id='',
                             svc_name='resource_registry',
                             command='delete',
                             args=[obj_id],
                             kwargs='')
        cmd = self.te_client.enqueue_command(cmd)

        # Block for the queue to be modified, the command to be transmitted,
        # and the result to be received.
        self._done_queue_mod_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_cmd_tx_evt.get(timeout=CFG.endpoint.receive.timeout)
        self._done_cmd_evt.get(timeout=CFG.endpoint.receive.timeout)        

        # Returns nothing.
        
        # Publish link down events.
        self.terrestrial_link_down()
        self.remote_link_down()
        
