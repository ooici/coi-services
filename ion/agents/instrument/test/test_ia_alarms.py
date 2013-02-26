#!/usr/bin/env python

"""
@package ion.agents.instrument.test.test_ia_alarms
@file ion/agents.instrument/test_ia_alarms.py
@author Edward Hunter
@brief Test cases for IA alarms, particularly preloaded configs.
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

# 3rd party imports.
import gevent
from gevent.event import AsyncResult
from nose.plugins.attrib import attr
from mock import patch

# Pyon pubsub and event support.
from pyon.event.event import EventSubscriber, EventPublisher
from pyon.ion.stream import StandaloneStreamSubscriber

# Pyon unittest support.
from pyon.util.int_test import IonIntegrationTestCase

# Pyon Object Serialization
from pyon.core.bootstrap import get_obj_registry
from pyon.core.object import IonObjectDeserializer

# Pyon exceptions.
from pyon.core.exception import BadRequest
from pyon.core.exception import Conflict

# Agent imports.
from pyon.util.context import LocalContextMixin
from pyon.agent.agent import ResourceAgentClient
from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent

# Driver imports.
from ion.agents.instrument.direct_access.direct_access_server import DirectAccessTypes
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

# Alarms.
from pyon.public import IonObject
from interface.objects import StreamAlarmType

"""
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_ia_alarms.py:TestIAAlarms
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_ia_alarms.py:TestIAAlarms.test_config
"""

###############################################################################
# Global constants.
###############################################################################

# Real and simulated devcies we test against.
DEV_ADDR = CFG.device.sbe37.host
DEV_PORT = CFG.device.sbe37.port
#DEV_ADDR = 'localhost'
#DEV_ADDR = '67.58.49.220' 
#DEV_ADDR = '137.110.112.119' # Moxa DHCP in Edward's office.
#DEV_ADDR = 'sbe37-simulator.oceanobservatories.org' # Simulator addr.
#DEV_PORT = 4001 # Moxa port or simulator random data.
#DEV_PORT = 4002 # Simulator sine data.

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
DRV_URI = 'http://sddevrepo.oceanobservatories.org/releases/seabird_sbe37smb_ooicore-0.0.4-py2.7.egg'
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


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''

#Refactored as stand alone method for starting an instrument agent for use in other tests, like governance
#to do policy testing for resource agents
#shenrie
def start_instrument_agent_process(container, stream_config={}, resource_id=IA_RESOURCE_ID, resource_name=IA_NAME, org_name=None, message_headers=None):
    log.info("foobar")

    # Create agent config.
    agent_config = {
        'driver_config' : DVR_CONFIG,
        'stream_config' : stream_config,
        'agent'         : {'resource_id': resource_id},
        'test_mode' : True
    }

    if org_name is not None:
        agent_config['org_name'] = org_name


    # Start instrument agent.

    log.info("TestInstrumentAgent.setup(): starting IA.")
    container_client = ContainerAgentClient(node=container.node,
        name=container.name)

    log.info("Agent setup")
    ia_pid = container_client.spawn_process(name=resource_name,
        module=IA_MOD,
        cls=IA_CLS,
        config=agent_config, headers=message_headers)

    log.info('Agent pid=%s.', str(ia_pid))

    # Start a resource agent client to talk with the instrument agent.

    ia_client = ResourceAgentClient(resource_id, process=FakeProcess())
    log.info('Got ia client %s.', str(ia_client))

    return ia_client

@attr('HARDWARE', group='mi')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 120}}})
class TestIAAlarms(IonIntegrationTestCase):
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
        Define agent config, start agent.
        Start agent client.
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

        log.info('building stream configuration')
        # Setup stream config.
        self._build_stream_config()

        # Start a resource agent client to talk with the instrument agent.
        log.info('starting IA process')
        self._ia_client = start_instrument_agent_process(self.container, self._stream_config)
        self.addCleanup(self._verify_agent_reset)
        log.info('test setup complete')


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
            
    ###############################################################################
    # Event helpers.
    ###############################################################################


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

        streams = {
            'parsed' : 'ctd_parsed_param_dict',
            'raw'    : 'ctd_raw_param_dict'
        }

        for (stream_name, param_dict_name) in streams.iteritems():
            pd_id = dataset_management.read_parameter_dictionary_by_name(param_dict_name, id_only=True)

            stream_def_id = pubsub_client.create_stream_definition(name=stream_name, parameter_dictionary_id=pd_id)
            pd            = pubsub_client.read_stream_definition(stream_def_id).parameter_dictionary

            stream_id, stream_route = pubsub_client.create_stream(name=stream_name,
                                                exchange_point='science_data',
                                                stream_definition_id=stream_def_id)

            stream_config = dict(stream_route=stream_route,
                                 routing_key=stream_route.routing_key,
                                 exchange_point=stream_route.exchange_point,
                                 stream_id=stream_id,
                                 stream_definition_ref=stream_def_id,
                                 parameter_dictionary=pd)
            
            if stream_name == 'parsed':
                
                type = 'IntervalAlarmDef'
                kwargs = {
                    'name' : 'test_sim_warning',
                    'stream_name' : 'parsed',
                    'value_id' : 'temp',
                    'message' : 'Temperature is above test range of 5.0.',
                    'type' : StreamAlarmType.WARNING,
                    'upper_bound' : 5.0,
                    'upper_rel_op' : '<'
                }
                alarm = {}
                alarm['type'] = type
                alarm['kwargs'] = kwargs
                alarms = [alarm]
                stream_config['alarms'] = alarms
            
            self._stream_config[stream_name] = stream_config

    def _start_data_subscribers(self, count):
        """
        """        
        # Create a pubsub client to create streams.
        pubsub_client = PubsubManagementServiceClient(node=self.container.node)
                
        # Create streams and subscriptions for each stream named in driver.
        self._data_subscribers = []
        self._samples_received = []
        self._async_sample_result = AsyncResult()

        # A callback for processing subscribed-to data.
        def recv_data(message, stream_route, stream_id):
            log.info('Received message on %s (%s,%s)', stream_id, stream_route.exchange_point, stream_route.routing_key)
            self._samples_received.append(message)
            if len(self._samples_received) == count:
                self._async_sample_result.set()

        for (stream_name, stream_config) in self._stream_config.iteritems():
            
            stream_id = stream_config['stream_id']
            
            # Create subscriptions for each stream.

            from pyon.util.containers import create_unique_identifier
            # exchange_name = '%s_queue' % stream_name
            exchange_name = create_unique_identifier("%s_queue" %
                    stream_name)
            self._purge_queue(exchange_name)
            sub = StandaloneStreamSubscriber(exchange_name, recv_data)
            sub.start()
            self._data_subscribers.append(sub)
            print 'stream_id: %s' % stream_id
            sub_id = pubsub_client.create_subscription(name=exchange_name, stream_ids=[stream_id])
            pubsub_client.activate_subscription(sub_id)
            sub.subscription_id = sub_id # Bind the subscription to the standalone subscriber (easier cleanup, not good in real practice)

    def _purge_queue(self, queue):
        xn = self.container.ex_manager.create_xn_queue(queue)
        xn.purge()
 
    def _stop_data_subscribers(self):
        for subscriber in self._data_subscribers:
            pubsub_client = PubsubManagementServiceClient()
            if hasattr(subscriber,'subscription_id'):
                try:
                    pubsub_client.deactivate_subscription(subscriber.subscription_id)
                except:
                    pass
                pubsub_client.delete_subscription(subscriber.subscription_id)
            subscriber.stop()

    ###############################################################################
    # Tests.
    ###############################################################################
    
    def test_config(self):
        """
        test_initialize
        Test agent initialize command. This causes creation of
        driver process and transition to inactive.
        """

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

        decoder = IonObjectDeserializer(obj_registry=get_obj_registry())

        # Grab the alarms defined in the config.
        retval = decoder.deserialize(self._ia_client.get_agent(['alarms'])['alarms'])

        """
        {'status': None, 'stream_name': 'parsed', 'name': 'test_sim_warning',
        'upper_bound': 5.0, 'expr': 'x<5.0', 'upper_rel_op': '<',
        'lower_rel_op': None, 'type_': 'IntervalAlarmDef', 'value_id': 'temp',
        'lower_bound': None, 'message': 'Temperature is above test range of 5.0.',
        'current_val': None, 'type': 1}
        """
        self.assertEqual(retval[0].type_, 'IntervalAlarmDef')
        self.assertEqual(retval[0].upper_bound, 5.0)
        self.assertEqual(retval[0].expr, 'x<5.0')
        
        # Reset the agent. This causes the driver messaging to be stopped,
        # the driver process to end and switches us back to uninitialized.
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)