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

# bin/nosetests -s -v ion/services/sa/tcaa/test/test_2caa.py:Test2CAA
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_2caa.py:Test2CAA.test_xxx


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
        self._remote_port = 0
        self._terrestrial_port = 0
        self._te_client = None
        self._re_client = None
        self._remote_pid = None
        self._terrestrial_pid = None

        self._done_telem_evt = AsyncResult()
        self._done_queue_mod_evt = AsyncResult()
        self._done_cmd_tx_evt = AsyncResult()
        self._done_cmd_evnt = AsyncResult()

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
        
        ###################################################################
        # Terrestrial endpoint.
        ###################################################################

        # Create the remote name.
        xs_name = 'remote1'
        terrestrial_svc_name = 'terrestrial_endpoint'
        terrestrial_listen_name = terrestrial_svc_name + xs_name

        # Create terrestrial config.
        terrestrial_endpoint_config = {
            'other_host' : 'localhost',
            'other_port' : self._remote_port,
            'this_port' : 0,
            'platform_resource_id' : self._terrestrial_platform_id,
            'process' : {
                'listen_name' : terrestrial_listen_name
            }
        }
        
        # Spawn the terrestrial enpoint process.
        log.debug('Spawning terrestrial endpoint process.')
        self._terrestrial_pid = container_client.spawn_process(
            name=terrestrial_listen_name,
            module='ion.services.sa.tcaa.terrestrial_endpoint',
            cls='TerrestrialEndpoint',
            config=terrestrial_endpoint_config)
        log.debug('Terrestrial endpoint pid=%s.', str(self._terrestrial_pid))

        # Create a terrestrial client.
        self.te_client = TerrestrialEndpointClient(
            process=FakeProcess(),
            to_name=terrestrial_listen_name)
        log.debug('Got te client %s.', str(self.te_client))
        self._terrestrial_port = self.te_client.get_port()
        log.debug('Terrestrial port is: %i', self._terrestrial_port)
    
        ###################################################################
        # Remote endpoint.
        ###################################################################
    
        remote_svc_name = 'remote_endpoint'
        remote_listen_name = remote_svc_name + xs_name
        
        # Create agent config.
        remote_endpoint_config = {
            'other_host' : 'localhost',
            'other_port' : self._remote_port,
            'this_port' : 0,
            'platform_resource_id' : self._remote_platform_id,
            'process' : {
                'listen_name' : remote_listen_name
            }
        }
        
        # Spawn the remote enpoint process.
        log.debug('Spawning remote endpoint process.')
        self._remote_pid = container_client.spawn_process(
            name=remote_listen_name,
            module='ion.services.sa.tcaa.remote_endpoint',
            cls='RemoteEndpoint',
            config=remote_endpoint_config)
        log.debug('Remote endpoint pid=%s.', str(self._remote_pid))

        # Create an endpoint client.
        self.re_client = RemoteEndpointClient(
            process=FakeProcess(),
            to_name=remote_listen_name)
        log.debug('Got re client %s.', str(self.re_client))
        
        # Remember the remote port.
        self._remote_port = self.re_client.get_port()
        log.debug('The remote port is: %i.', self._remote_port)
        
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

        # Start the event subscriber.
        self._event_subscriber = EventSubscriber(
            event_type='PlatformEvent',
            callback=self.consume_event,
            origin=self._terrestrial_platform_id)
        self._event_subscriber.start()
        self._event_subscriber._ready_event.wait(timeout=CFG.endpoint.receive.timeout)
        self.addCleanup(self._event_subscriber.stop)

        # Start the result subscriber.        
        self._result_subscriber = EventSubscriber(
            event_type='RemoteCommandResult',
            origin=self._resource_id,
            callback=self.consume_event)
        self._result_subscriber.start()
        self._result_subscriber._ready_event.wait(timeout=CFG.endpoint.receive.timeout)
        self.addCleanup(self._result_subscriber.stop)

    ###################################################################
    # Agent startup.
    ###################################################################

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
        
        """
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
        """
        
    def test_xxx(self):
        """
        """
        
        gevent.sleep(2)
        self.terrestrial_link_up()
        gevent.sleep(2)
        self.terrestrial_link_down()
        

