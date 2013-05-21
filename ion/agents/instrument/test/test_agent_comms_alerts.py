#!/usr/bin/env python

"""
@package ion.agents.instrument.test.test_agent_persistence
@file ion/agents.instrument/test_agent_persistence.py
@author Edward Hunter
@brief Test cases for R2 instrument agent state and config persistence between running instances.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Import pyon first for monkey patching.

# Pyon log and config objects.
from pyon.public import log
from pyon.public import CFG

# Standard imports.
import sys
import time
import socket
import re
import json
import unittest
import os

# 3rd party imports.
import gevent
from gevent.event import AsyncResult
from nose.plugins.attrib import attr
from mock import patch

# Pyon pubsub and event support.
from pyon.event.event import EventSubscriber, EventPublisher
from pyon.ion.stream import StandaloneStreamSubscriber
from ion.services.dm.utility.granule_utils import RecordDictionaryTool

# Pyon unittest support.
from pyon.util.int_test import IonIntegrationTestCase

# Pyon exceptions.
from pyon.core.exception import BadRequest, Conflict, Timeout, ResourceError

# Agent imports.
from pyon.util.context import LocalContextMixin
from pyon.agent.agent import ResourceAgentClient
from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent

# Driver imports.
from ion.agents.instrument.driver_int_test_support import DriverIntegrationTestSupport
from ion.agents.instrument.driver_process import DriverProcessType
from ion.agents.instrument.driver_process import ZMQEggDriverProcess

# Objects and clients.
from interface.objects import AgentCommand
from interface.objects import CapabilityType
from interface.objects import AgentCapability
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient

# Alerts.
from interface.objects import StreamAlertType, AggregateStatusType

"""
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_agent_comms_alerts.py:TestAgentCommsAlerts
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_agent_comms_alerts.py:TestAgentCommsAlerts.test_lost_connection_alert
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_agent_comms_alerts.py:TestAgentCommsAlerts.test_connect_failed_alert
"""

###############################################################################
# Global constants.
###############################################################################

DEV_ADDR = CFG.device.sbe37.host
DEV_PORT = CFG.device.sbe37.port
DATA_PORT = CFG.device.sbe37.port_agent_data_port
CMD_PORT = CFG.device.sbe37.port_agent_cmd_port
PA_BINARY = CFG.device.sbe37.port_agent_binary

# Work dir and logger delimiter.
WORK_DIR = '/tmp/'
DELIM = ['<<','>>']

from ion.agents.instrument.instrument_agent import InstrumentAgent
# Agent parameters.
IA_RESOURCE_ID = '123xyz'
IA_NAME = 'Agent007'
IA_MOD = 'ion.agents.instrument.instrument_agent'
IA_CLS = 'InstrumentAgent'

# A seabird driver.
DRV_URI = 'http://sddevrepo.oceanobservatories.org/releases/seabird_sbe37smb_ooicore-0.1.1-py2.7.egg'
DRV_MOD = 'mi.instrument.seabird.sbe37smb.ooicore.driver'
DRV_CLS = 'SBE37Driver'

# Driver config.
# DVR_CONFIG['comms_config']['port'] is set by the setup.
DVR_CONFIG = {
    'dvr_egg' : DRV_URI,
    'dvr_mod' : DRV_MOD,
    'dvr_cls' : DRV_CLS,
    'workdir' : WORK_DIR,
    'process_type' : (DriverProcessType.EGG,)
}

# Dynamically load the egg into the test path
launcher = ZMQEggDriverProcess(DVR_CONFIG)
egg = launcher._get_egg(DRV_URI)
if not egg in sys.path: sys.path.insert(0, egg)

# Load MI modules from the egg
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverConnectionState
from mi.core.exceptions import InstrumentParameterException
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Parameter

state_alert_def = {
    'name' : 'comms_state_warning',
    'description' : 'Detected comms failure.',
    'alert_type' : StreamAlertType.WARNING,
    'alert_states' : [
        ResourceAgentState.LOST_CONNECTION,
        ResourceAgentState.ACTIVE_UNKNOWN
                      ],
    'clear_states' : [
        ResourceAgentState.IDLE,
        ResourceAgentState.COMMAND,
        ResourceAgentState.STREAMING
        ],
    'alert_class' : 'StateAlert'
}

command_alert_def = {
    'name' : 'comms_command_warning',
    'description' : 'Detected comms failure while connecting.',
    'alert_type' : StreamAlertType.WARNING,
    'command' : ResourceAgentEvent.GO_ACTIVE,
    'clear_states' : [
        ResourceAgentState.IDLE,
        ResourceAgentState.COMMAND,
        ResourceAgentState.STREAMING
        ],
    'alert_class' : 'CommandErrorAlert'
}

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''

@attr('HARDWARE', group='sa')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 120}}})
class TestAgentCommsAlerts(IonIntegrationTestCase):
    """
    """
    
    ############################################################################
    # Setup, teardown.
    ############################################################################
        
    def setUp(self):
        """
        Set up driver integration support.
        Start port agent, add port agent cleanup.
        Start container.
        Start deploy services.
        Define agent config.
        """
        self._ia_client = None

        log.info('Creating driver integration test support:')
        log.info('driver uri: %s', DRV_URI)
        log.info('device address: %s', DEV_ADDR)
        log.info('device port: %s', DEV_PORT)
        log.info('log delimiter: %s', DELIM)
        log.info('work dir: %s', WORK_DIR)
        self._support = DriverIntegrationTestSupport(None,
                                                     None,
                                                     DEV_ADDR,
                                                     DEV_PORT,
                                                     DATA_PORT,
                                                     CMD_PORT,
                                                     PA_BINARY,
                                                     DELIM,
                                                     WORK_DIR)
                
        # Start port agent, add stop to cleanup.
        self._start_pagent()
        self.addCleanup(self._support.stop_pagent)    
        
        # Start container.
        log.info('Staring capability container.')
        self._start_container()
        
        # Bring up services in a deploy file (no need to message)
        log.info('Staring deploy services.')
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self._event_count = 0
        self._events_received = []
        self._async_event_result = AsyncResult()

        def consume_event(*args, **kwargs):
            log.debug('Test recieved ION event: args=%s, kwargs=%s, event=%s.',
                     str(args), str(kwargs), str(args[0]))
            self._events_received.append(args[0])
            if self._event_count > 0 and \
                self._event_count == len(self._events_received):
                self._async_event_result.set()
                        
        self._event_subscriber = EventSubscriber(
            event_type='DeviceStatusAlertEvent', callback=consume_event,
            origin=IA_RESOURCE_ID)
        
        self._event_subscriber.start()

        def stop_subscriber():
            self._event_subscriber.stop()
            self._event_subscriber = None
        self.addCleanup(stop_subscriber)

        log.info('building stream configuration')
        # Setup stream config.
        self._build_stream_config()

        # Create agent config.
        self._agent_config = {
            'driver_config' : DVR_CONFIG,
            'stream_config' : self._stream_config,
            'agent'         : {'resource_id': IA_RESOURCE_ID},
            'test_mode' : True,
            'forget_past' : True,
            'enable_persistence' : False,
            'aparam_alerts_config' : [state_alert_def, command_alert_def]
        }

        self._ia_client = None
        self._ia_pid = None
        
        self.addCleanup(self._verify_agent_reset)

    ###############################################################################
    # Port agent helpers.
    ###############################################################################
        
    def _start_pagent(self):
        """
        Construct and start the port agent.
        """

        port = self._support.start_pagent()
        log.info('Port agent started at port %i',port)
        
        # Configure driver to use port agent port number.
        DVR_CONFIG['comms_config'] = {
            'addr' : 'localhost',
            'port' : port,
            'cmd_port' : CMD_PORT
        }
                                    
    ###############################################################################
    # Data stream helpers.
    ###############################################################################

    def _build_stream_config(self):
        """
        """
        # Create a pubsub client to create streams.
        pubsub_client = PubsubManagementServiceClient(node=self.container.node)
        dataset_management = DatasetManagementServiceClient()
        
        # Create streams and subscriptions for each stream named in driver.
        self._stream_config = {}

        stream_name = 'parsed'
        param_dict_name = 'ctd_parsed_param_dict'
        pd_id = dataset_management.read_parameter_dictionary_by_name(param_dict_name, id_only=True)
        stream_def_id = pubsub_client.create_stream_definition(name=stream_name, parameter_dictionary_id=pd_id)
        pd = pubsub_client.read_stream_definition(stream_def_id).parameter_dictionary
        stream_id, stream_route = pubsub_client.create_stream(name=stream_name,
                                                exchange_point='science_data',
                                                stream_definition_id=stream_def_id)
        stream_config = dict(stream_route=stream_route,
                                 routing_key=stream_route.routing_key,
                                 exchange_point=stream_route.exchange_point,
                                 stream_id=stream_id,
                                 stream_definition_ref=stream_def_id,
                                 parameter_dictionary=pd)
        self._stream_config[stream_name] = stream_config

        stream_name = 'raw'
        param_dict_name = 'ctd_raw_param_dict'
        pd_id = dataset_management.read_parameter_dictionary_by_name(param_dict_name, id_only=True)
        stream_def_id = pubsub_client.create_stream_definition(name=stream_name, parameter_dictionary_id=pd_id)
        pd = pubsub_client.read_stream_definition(stream_def_id).parameter_dictionary
        stream_id, stream_route = pubsub_client.create_stream(name=stream_name,
                                                exchange_point='science_data',
                                                stream_definition_id=stream_def_id)
        stream_config = dict(stream_route=stream_route,
                                 routing_key=stream_route.routing_key,
                                 exchange_point=stream_route.exchange_point,
                                 stream_id=stream_id,
                                 stream_definition_ref=stream_def_id,
                                 parameter_dictionary=pd)
        self._stream_config[stream_name] = stream_config

    ###############################################################################
    # Agent start stop helpers.
    ###############################################################################

    def _start_agent(self):
        """
        """
        container_client = ContainerAgentClient(node=self.container.node,
            name=self.container.name)
        
        self._ia_pid = container_client.spawn_process(name=IA_NAME,
            module=IA_MOD,
            cls=IA_CLS,
            config=self._agent_config)
        log.info('Started instrument agent pid=%s.', str(self._ia_pid))
        
        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = None
        self._ia_client = ResourceAgentClient(IA_RESOURCE_ID, process=FakeProcess())
        log.info('Got instrument agent client %s.', str(self._ia_client))

    def _stop_agent(self):
        """
        """
        if self._ia_pid:
            container_client = ContainerAgentClient(node=self.container.node,
                name=self.container.name)
            container_client.terminate_process(self._ia_pid)
            self._ia_pid = None
            
        if self._ia_client:
            self._ia_client = None

    def _verify_agent_reset(self):
        """
        Check agent state and reset if necessary.
        This called if a test fails and reset hasn't occurred.
        """
        if self._ia_client is None:
            return

        state = self._ia_client.get_agent_state()
        if state != ResourceAgentState.UNINITIALIZED:
            cmd = AgentCommand(command=ResourceAgentEvent.RESET)
            retval = self._ia_client.execute_agent(cmd)
            self._ia_client = None

    ###############################################################################
    # Tests.
    ###############################################################################

    def test_lost_connection_alert(self):
        """
        test_lost_connection_alert
        Verify that agents detect lost connection state and issue alert.
        """

        self._event_count = 3

        self._start_agent()

        # We start in uninitialized state.
        # In this state there is no driver process.
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
        
        # Ping the agent.
        retval = self._ia_client.ping_agent()
        log.info(retval)

        # Initialize the agent.
        # The agent is spawned with a driver config, but you can pass one in
        # optinally with the initialize command. This validates the driver
        # config, launches a driver process and connects to it via messaging.
        # If successful, we switch to the inactive state.
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        # Ping the driver proc.
        retval = self._ia_client.ping_resource()
        log.info(retval)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        # Confirm the persisted parameters.
        retval = self._ia_client.get_agent(['alerts'])['alerts']
        """
        {'origin': '123xyz', 'status': 1, '_id': 'da03b90d2e064b25bf51ed90b729e82e',
        'description': 'The alert is cleared.', 'time_stamps': [],
        'type_': 'DeviceStatusAlertEvent', 'valid_values': [],
        'values': ['RESOURCE_AGENT_STATE_INACTIVE'], 'value_id': '',
        'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'],
        'stream_name': '', 'ts_created': '1366749987069', 'sub_type': 3,
        'origin_type': 'InstrumentDevice', 'name': 'comms_warning'}
        """

        # Acquire sample returns a string, not a particle.  The particle
        # is created by the data handler though.
        cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)
        retval = self._ia_client.execute_resource(cmd)

        # Blow the port agent out from under the agent.
        self._support.stop_pagent()

        # Wait for a while, the supervisor is restarting the port agent.
        gevent.sleep(5)
        self._support.start_pagent()

        timeout = gevent.Timeout(120)
        timeout.start()

        try:
            
            while True:
                state = self._ia_client.get_agent_state()
                if state == ResourceAgentState.COMMAND:
                    break
                else:
                    gevent.sleep(2)
            
        except Timeout as t:
            self.fail('Could not reconnect to device.')
        
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        # Reset the agent. This causes the driver messaging to be stopped,
        # the driver process to end and switches us back to uninitialized.
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        self._async_event_result.get(timeout=30) 
        """
        {'origin': '123xyz', 'status': 1, '_id': '9d7db919a0414741a2aa97f3dc310647', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': ['RESOURCE_AGENT_STATE_INACTIVE'], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': '', 'ts_created': '1367008029709', 'sub_type': 'ALL_CLEAR', 'origin_type': 'InstrumentDevice', 'name': 'comms_state_warning'}
        {'origin': '123xyz', 'status': 1, '_id': 'f746cec5e486445e856ef29af8d8d49a', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [None], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': '', 'ts_created': '1367008029717', 'sub_type': 'ALL_CLEAR', 'origin_type': 'InstrumentDevice', 'name': 'comms_command_warning'}
        {'origin': '123xyz', 'status': 1, '_id': '126b1e20f54f4bd2b84a93584831ea8d', 'description': 'Detected comms failure.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': ['RESOURCE_AGENT_STATE_LOST_CONNECTION'], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': '', 'ts_created': '1367008062061', 'sub_type': 'WARNING', 'origin_type': 'InstrumentDevice', 'name': 'comms_state_warning'}
        {'origin': '123xyz', 'status': 1, '_id': '90f5762112dd446b939c6675d34dd61d', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': ['RESOURCE_AGENT_STATE_COMMAND'], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': '', 'ts_created': '1367008098639', 'sub_type': 'ALL_CLEAR', 'origin_type': 'InstrumentDevice', 'name': 'comms_state_warning'}
        """
        
    def test_connect_failed_alert(self):
        """
        test_connect_failed_alert
        Verify that agents detect failed connections and issue alert.
        """

        self._event_count = 4

        # Remove the port agent.
        self._support.stop_pagent()

        self._start_agent()

        # We start in uninitialized state.
        # In this state there is no driver process.
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
        
        # Ping the agent.
        retval = self._ia_client.ping_agent()
        log.info(retval)

        # Initialize the agent.
        # The agent is spawned with a driver config, but you can pass one in
        # optinally with the initialize command. This validates the driver
        # config, launches a driver process and connects to it via messaging.
        # If successful, we switch to the inactive state.
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        # Ping the driver proc.
        retval = self._ia_client.ping_resource()
        log.info(retval)

        with self.assertRaises(Exception):
            cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
            retval = self._ia_client.execute_agent(cmd)

        # Now start up the port agent.
        self._support.start_pagent()
        gevent.sleep(5)

        # This time it will work.
        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        self._async_event_result.get(timeout=30) 
        """
        {'origin': '123xyz', 'status': 1, '_id': 'ac1ae3ec24e74f65bde24362d689346a', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': ['RESOURCE_AGENT_STATE_INACTIVE'], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': '', 'ts_created': '1367007751167', 'sub_type': 'ALL_CLEAR', 'origin_type': 'InstrumentDevice', 'name': 'comms_state_warning'}
        {'origin': '123xyz', 'status': 1, '_id': '6af6a6156172481ebc44baad2708ec5c', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [None], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': '', 'ts_created': '1367007751175', 'sub_type': 'ALL_CLEAR', 'origin_type': 'InstrumentDevice', 'name': 'comms_command_warning'}
        {'origin': '123xyz', 'status': 1, '_id': 'b493698b46fd4fc1b4c66c53b70ed043', 'description': 'Detected comms failure while connecting.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [None], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': '', 'ts_created': '1367007756771', 'sub_type': 'WARNING', 'origin_type': 'InstrumentDevice', 'name': 'comms_command_warning'}
        {'origin': '123xyz', 'status': 1, '_id': 'c01e911ecb1a47c5a04cbbcbfb348a42', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [None], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': '', 'ts_created': '1367007792905', 'sub_type': 'ALL_CLEAR', 'origin_type': 'InstrumentDevice', 'name': 'comms_command_warning'}
        """
        