#!/usr/bin/env python

"""
@package ion.agents.instrument.test.test_instrument_agent
@file ion/agents.instrument/test_instrument_agent.py
@author Edward Hunter
@brief Test cases for R2 instrument agent.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Import pyon first for monkey patching.

# Pyon log and config objects.
from pyon.public import log
from pyon.public import CFG

# Standard imports.
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
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverConnectionState

# Objects and clients.
from interface.objects import AgentCommand
from interface.objects import CapabilityType
from interface.objects import AgentCapability
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient

# MI imports.
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Parameter
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent
from mi.instrument.seabird.sbe37smb.ooicore.driver import PACKET_CONFIG

# TODO chagne the path following the refactor.
# bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_gateway_to_instrument_agent.py:TestInstrumentAgentViaGateway
# bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent
# bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_initialize
# bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_resource_states
# bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_states
# bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_get_set
# bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_get_set_errors
# bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_get_set_agent
# bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_poll
# bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_autosample
# bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_capabilities
# bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_command_errors
# bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_direct_access
# bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_test
# bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_states_special
# bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_data_buffering


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

# A seabird driver.
DRV_MOD = 'mi.instrument.seabird.sbe37smb.ooicore.driver'
DRV_CLS = 'SBE37Driver'

# Work dir and logger delimiter.
WORK_DIR = '/tmp/'
DELIM = ['<<','>>']

# Driver config.
# DVR_CONFIG['comms_config']['port'] is set by the setup.
DVR_CONFIG = {
    'dvr_mod' : DRV_MOD,
    'dvr_cls' : DRV_CLS,
    'workdir' : WORK_DIR,
    'process_type' : ('ZMQPyClassDriverLauncher',)
}

# Agent parameters.
IA_RESOURCE_ID = '123xyz'
IA_NAME = 'Agent007'
IA_MOD = 'ion.agents.instrument.instrument_agent'
IA_CLS = 'InstrumentAgent'

# Used to validate param config retrieved from driver.
PARAMS = {
    SBE37Parameter.OUTPUTSAL : bool,
    SBE37Parameter.OUTPUTSV : bool,
    SBE37Parameter.NAVG : int,
    SBE37Parameter.SAMPLENUM : int,
    SBE37Parameter.INTERVAL : int,
    SBE37Parameter.STORETIME : bool,
    SBE37Parameter.TXREALTIME : bool,
    SBE37Parameter.SYNCMODE : bool,
    SBE37Parameter.SYNCWAIT : int,
    SBE37Parameter.TCALDATE : tuple,
    SBE37Parameter.TA0 : float,
    SBE37Parameter.TA1 : float,
    SBE37Parameter.TA2 : float,
    SBE37Parameter.TA3 : float,
    SBE37Parameter.CCALDATE : tuple,
    SBE37Parameter.CG : float,
    SBE37Parameter.CH : float,
    SBE37Parameter.CI : float,
    SBE37Parameter.CJ : float,
    SBE37Parameter.WBOTC : float,
    SBE37Parameter.CTCOR : float,
    SBE37Parameter.CPCOR : float,
    SBE37Parameter.PCALDATE : tuple,
    SBE37Parameter.PA0 : float,
    SBE37Parameter.PA1 : float,
    SBE37Parameter.PA2 : float,
    SBE37Parameter.PTCA0 : float,
    SBE37Parameter.PTCA1 : float,
    SBE37Parameter.PTCA2 : float,
    SBE37Parameter.PTCB0 : float,
    SBE37Parameter.PTCB1 : float,
    SBE37Parameter.PTCB2 : float,
    SBE37Parameter.POFFSET : float,
    SBE37Parameter.RCALDATE : tuple,
    SBE37Parameter.RTCA0 : float,
    SBE37Parameter.RTCA1 : float,
    SBE37Parameter.RTCA2 : float
}


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

    log.debug("TestInstrumentAgent.setup(): starting IA.")
    container_client = ContainerAgentClient(node=container.node,
        name=container.name)

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
class TestInstrumentAgent(IonIntegrationTestCase):
    """
    Test cases for instrument agent class. Functions in this class provide
    instrument agent integration tests and provide a tutorial on use of
    the agent setup and interface.
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
        self._start_pagent()
        self.addCleanup(self._support.stop_pagent)    
        
        # Start container.
        log.info('Staring capability container.')
        self._start_container()
        
        # Bring up services in a deploy file (no need to message)
        log.info('Staring deploy services.')
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Setup stream config.
        self._build_stream_config()


        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = None
        self._ia_client = start_instrument_agent_process(self.container, self._stream_config)
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
            'port' : port
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

    def _start_event_subscriber(self, type='ResourceAgentEvent', count=0):
        """
        Start a subscriber to the instrument agent events.
        @param type The type of event to catch.
        @count Trigger the async event result when events received reaches this.
        """
        def consume_event(*args, **kwargs):
            log.info('Test recieved ION event: args=%s, kwargs=%s, event=%s.', 
                     str(args), str(kwargs), str(args[0]))
            self._events_received.append(args[0])
            if self._event_count > 0 and \
                self._event_count == len(self._events_received):
                self._async_event_result.set()
            
        # Event array and async event result.
        self._event_count = count
        self._events_received = []
        self._async_event_result = AsyncResult()
            
        self._event_subscriber = EventSubscriber(
            event_type=type, callback=consume_event,
            origin=IA_RESOURCE_ID)
        self._event_subscriber.start()
        self._event_subscriber._ready_event.wait(timeout=5)

    def _stop_event_subscriber(self):
        """
        Stop event subscribers on cleanup.
        """
        self._event_subscriber.stop()
        self._event_subscriber = None

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
            'raw' : 'ctd_raw_param_dict'
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
    # Socket listen.
    ###############################################################################

    def _socket_listen(self, s, prompt, timeout):

        buf = ''
        starttime = time.time()
        while True:
            try:
                buf += s.recv(1024)
                print '##### Listening, got: %s' % buf
                if prompt and buf.find(prompt) != -1:
                    break
            except:
                gevent.sleep(1)
            
            finally:
                delta = time.time() - starttime
                if delta > timeout:
                    break
        return buf            
                
    ###############################################################################
    # Assert helpers.
    ###############################################################################
        
    def assertSampleDict(self, val):
        """
        Verify the value is a sample dictionary for the sbe37.
        """
        # AgentCommandResult.result['parsed']
        """
        {'quality_flag': 'ok', 'preferred_timestamp': 'driver_timestamp',
        'stream_name': 'parsed', 'pkt_format_id': 'JSON_Data',
        'pkt_version': 1, 'values':
        [{'value_id': 'temp', 'value': 21.4894},
        {'value_id': 'conductivity', 'value': 13.22157},
        {'value_id': 'pressure', 'value': 146.186}],
        'driver_timestamp': 3556901018.170206}
        """
        
        self.assertTrue(isinstance(val, dict))
        self.assertTrue(val.has_key('values'))
        values_list = val['values']
        self.assertTrue(isinstance(values_list, list))
        self.assertTrue(len(values_list)==3)
        
        ids = ['temp', 'conductivity', 'pressure']
        ids_found = []

        for x in values_list:
            self.assertTrue(x.has_key('value_id'))
            self.assertTrue(x.has_key('value'))
            ids_found.append(x['value_id'])
            self.assertTrue(isinstance(x['value'], float))

        self.assertItemsEqual(ids, ids_found)

        self.assertTrue(val.has_key('driver_timestamp'))
        time = val['driver_timestamp']
        self.assertTrue(isinstance(time, float))
        
    def assertParamDict(self, pd, all_params=False):
        """
        Verify all device parameters exist and are correct type.
        """
        if all_params:
            self.assertEqual(set(pd.keys()), set(PARAMS.keys()))
            for (key, type_val) in PARAMS.iteritems():
                if type_val == list or type_val == tuple:
                    self.assertTrue(isinstance(pd[key], (list, tuple)))
                else:
                    self.assertTrue(isinstance(pd[key], type_val))

        else:
            for (key, val) in pd.iteritems():
                self.assertTrue(PARAMS.has_key(key))
                self.assertTrue(isinstance(val, PARAMS[key]))
        
    def assertParamVals(self, params, correct_params):
        """
        Verify parameters take the correct values.
        """
        self.assertEqual(set(params.keys()), set(correct_params.keys()))
        for (key, val) in params.iteritems():
            correct_val = correct_params[key]
            if isinstance(val, float):
                # Verify to 5% of the larger value.
                max_val = max(abs(val), abs(correct_val))
                self.assertAlmostEqual(val, correct_val, delta=max_val*.01)

            elif isinstance(val, (list, tuple)):
                # list of tuple.
                self.assertEqual(list(val), list(correct_val))
            
            else:
                # int, bool, str.
                self.assertEqual(val, correct_val)

    ###############################################################################
    # Tests.
    ###############################################################################

    def test_initialize(self):
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

        # Reset the agent. This causes the driver messaging to be stopped,
        # the driver process to end and switches us back to uninitialized.
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
        
    def test_resource_states(self):
        """
        test_resource_states
        Bring the agent up, through COMMAND state, and reset to UNINITIALIZED,
        verifying the resource state at each step. Verify
        ResourceAgentResourceStateEvents are published.
        """

        # Set up a subscriber to collect error events.
        self._start_event_subscriber('ResourceAgentResourceStateEvent', 6)
        self.addCleanup(self._stop_event_subscriber)    

        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
    
        with self.assertRaises(Conflict):
            res_state = self._ia_client.get_resource_state()
    
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)
        
        res_state = self._ia_client.get_resource_state()
        self.assertEqual(res_state, DriverConnectionState.UNCONFIGURED)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        res_state = self._ia_client.get_resource_state()
        self.assertEqual(res_state, DriverProtocolState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)        
        
        res_state = self._ia_client.get_resource_state()
        self.assertEqual(res_state, DriverProtocolState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
        
        with self.assertRaises(Conflict):
            res_state = self._ia_client.get_resource_state()
        
        self._async_event_result.get(timeout=CFG.endpoint.receive.timeout)
        self.assertGreaterEqual(len(self._events_received), 6)
        
    def test_states(self):
        """
        test_states
        Test agent state transitions through execute agent interface.
        Verify agent state status as we go. Verify ResourceAgentStateEvents
        are published. Verify agent and resource pings.
        """

        # Set up a subscriber to collect error events.
        self._start_event_subscriber('ResourceAgentStateEvent', 8)
        self.addCleanup(self._stop_event_subscriber)    

        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        # Ping the agent.
        retval = self._ia_client.ping_agent()
        log.info(retval)

        with self.assertRaises(Conflict):
            retval = self._ia_client.ping_resource()
    
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

        cmd = AgentCommand(command=ResourceAgentEvent.PAUSE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.STOPPED)

        cmd = AgentCommand(command=ResourceAgentEvent.RESUME)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.CLEAR)
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
            
        self._async_event_result.get(timeout=CFG.endpoint.receive.timeout)
        self.assertEquals(len(self._events_received), 8)
            
    def test_get_set(self):
        """
        test_get_set
        Test instrument driver get and set resource interface. Verify
        ResourceAgentResourceConfigEvents are published.
        """
                
        # Set up a subscriber to collect error events.
        self._start_event_subscriber('ResourceAgentResourceConfigEvent', 2)
        self.addCleanup(self._stop_event_subscriber)    
        
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
    
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        params = SBE37Parameter.ALL
        retval = self._ia_client.get_resource(params)
        self.assertParamDict(retval, True)
        orig_config = retval

        params = [
            SBE37Parameter.OUTPUTSV,
            SBE37Parameter.NAVG,
            SBE37Parameter.TA0
        ]
        retval = self._ia_client.get_resource(params)
        self.assertParamDict(retval)
        orig_params = retval

        new_params = {
            SBE37Parameter.OUTPUTSV : not orig_params[SBE37Parameter.OUTPUTSV],
            SBE37Parameter.NAVG : orig_params[SBE37Parameter.NAVG] + 1,
            SBE37Parameter.TA0 : orig_params[SBE37Parameter.TA0] * 2
        }

        self._ia_client.set_resource(new_params)
        retval = self._ia_client.get_resource(params)
        self.assertParamVals(retval, new_params)

        params = SBE37Parameter.ALL
        self._ia_client.set_resource(orig_config)
        retval = self._ia_client.get_resource(params)
        self.assertParamVals(retval, orig_config)        
        
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
    
        self._async_event_result.get(timeout=CFG.endpoint.receive.timeout)

        # ResourceAgentResourceConfigEvent
        """        
        {'origin': '123xyz', 'description': '', 'config': {'PA1': 0.4851819, 'WBOTC': 1.2024e-05, 'PCALDATE': [12, 8, 2005], 'STORETIME': False, 'CPCOR': 9.57e-08, 'PTCA2': 0.00575649, 'OUTPUTSV': False, 'SAMPLENUM': 0, 'TCALDATE': [8, 11, 2005], 'OUTPUTSAL': False, 'NAVG': 0, 'POFFSET': 0.0, 'INTERVAL': 10873, 'SYNCWAIT': 0, 'CJ': 3.339261e-05, 'CI': 0.0001334915, 'CH': 0.1417895, 'TA0': -0.0002572242, 'TA1': 0.0003138936, 'TA2': -9.717158e-06, 'TA3': 2.138735e-07, 'RCALDATE': [8, 11, 2005], 'CG': -0.987093, 'CTCOR': 3.25e-06, 'PTCB0': 24.6145, 'PTCB1': -0.0009, 'PTCB2': 0.0, 'CCALDATE': [8, 11, 2005], 'PA0': 5.916199, 'PTCA1': 0.6603433, 'PA2': 4.596432e-07, 'SYNCMODE': False, 'PTCA0': 276.2492, 'TXREALTIME': True, 'RTCA2': -3.022745e-08, 'RTCA1': 1.686132e-06, 'RTCA0': 0.9999862}, 'type_': 'ResourceAgentResourceConfigEvent', 'base_types': ['ResourceAgentEvent', 'Event'], 'ts_created': '1349373583593', 'sub_type': '', 'origin_type': ''}
        {'origin': '123xyz', 'description': '', 'config': {'PA1': 0.4851819, 'WBOTC': 1.2024e-05, 'PCALDATE': [12, 8, 2005], 'STORETIME': False, 'CPCOR': 9.57e-08, 'PTCA2': 0.00575649, 'OUTPUTSV': True, 'SAMPLENUM': 0, 'TCALDATE': [8, 11, 2005], 'OUTPUTSAL': False, 'NAVG': 1, 'POFFSET': 0.0, 'INTERVAL': 10873, 'SYNCWAIT': 0, 'CJ': 3.339261e-05, 'CI': 0.0001334915, 'CH': 0.1417895, 'TA0': -0.0005144484, 'TA1': 0.0003138936, 'TA2': -9.717158e-06, 'TA3': 2.138735e-07, 'RCALDATE': [8, 11, 2005], 'CG': -0.987093, 'CTCOR': 3.25e-06, 'PTCB0': 24.6145, 'PTCB1': -0.0009, 'PTCB2': 0.0, 'CCALDATE': [8, 11, 2005], 'PA0': 5.916199, 'PTCA1': 0.6603433, 'PA2': 4.596432e-07, 'SYNCMODE': False, 'PTCA0': 276.2492, 'TXREALTIME': True, 'RTCA2': -3.022745e-08, 'RTCA1': 1.686132e-06, 'RTCA0': 0.9999862}, 'type_': 'ResourceAgentResourceConfigEvent', 'base_types': ['ResourceAgentEvent', 'Event'], 'ts_created': '1349373591121', 'sub_type': '', 'origin_type': ''}
        {'origin': '123xyz', 'description': '', 'config': {'PA1': 0.4851819, 'WBOTC': 1.2024e-05, 'PCALDATE': [12, 8, 2005], 'STORETIME': False, 'CPCOR': 9.57e-08, 'PTCA2': 0.00575649, 'OUTPUTSV': False, 'SAMPLENUM': 0, 'TCALDATE': [8, 11, 2005], 'OUTPUTSAL': False, 'NAVG': 0, 'POFFSET': 0.0, 'INTERVAL': 10873, 'SYNCWAIT': 0, 'CJ': 3.339261e-05, 'CI': 0.0001334915, 'CH': 0.1417895, 'TA0': -0.0002572242, 'TA1': 0.0003138936, 'TA2': -9.717158e-06, 'TA3': 2.138735e-07, 'RCALDATE': [8, 11, 2005], 'CG': -0.987093, 'CTCOR': 3.25e-06, 'PTCB0': 24.6145, 'PTCB1': -0.0009, 'PTCB2': 0.0, 'CCALDATE': [8, 11, 2005], 'PA0': 5.916199, 'PTCA1': 0.6603433, 'PA2': 4.596432e-07, 'SYNCMODE': False, 'PTCA0': 276.2492, 'TXREALTIME': True, 'RTCA2': -3.022745e-08, 'RTCA1': 1.686132e-06, 'RTCA0': 0.9999862}, 'type_': 'ResourceAgentResourceConfigEvent', 'base_types': ['ResourceAgentEvent', 'Event'], 'ts_created': '1349373639231', 'sub_type': '', 'origin_type': ''}
        """
        
        log.warning('*******************CHECKING EVENTS RECEIVED in test_get_set:')
        for x in self._events_received:
            log.warning(str(x))
        self.assertGreaterEqual(len(self._events_received), 2)

    def test_get_set_errors(self):
        """
        test_get_set_errors
        Test instrument driver get and set resource errors.
        """
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        # Attempt to get in invalid state.
        params = SBE37Parameter.ALL
        with self.assertRaises(Conflict):
            self._ia_client.get_resource(params)
        
        # Attempt to set in invalid state.
        params = {
            SBE37Parameter.TA0 : -2.5e-04
        }
        with self.assertRaises(Conflict):
            self._ia_client.set_resource(params)
    
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        # Attempt to get in invalid state.
        params = SBE37Parameter.ALL
        with self.assertRaises(Conflict):
            self._ia_client.get_resource(params)
        
        # Attempt to set in invalid state.
        params = {
            SBE37Parameter.TA0 : -2.5e-04
        }
        with self.assertRaises(Conflict):
            self._ia_client.set_resource(params)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        # Attempt to get with no parameters.
        with self.assertRaises(BadRequest):
            self._ia_client.get_resource()
                
        # Attempt to get with bogus parameters.
        params = [
            'I am a bogus parameter name',
            SBE37Parameter.OUTPUTSV            
        ]
        with self.assertRaises(BadRequest):
            retval = self._ia_client.get_resource(params)
        
        # Attempt to set with no parameters.
        # Set without parameters.
        with self.assertRaises(BadRequest):
            retval = self._ia_client.set_resource()
        
        # Attempt to set with bogus parameters.
        params = {
            'I am a bogus parameter name' : 'bogus val',
            SBE37Parameter.OUTPUTSV : False
        }
        with self.assertRaises(BadRequest):
            self._ia_client.set_resource(params)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

    def test_get_set_agent(self):
        """
        test_get_set_agent
        Test instrument agent get and set interface, including errors.
        """
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        # Test with a bad parameter name.
        with self.assertRaises(BadRequest):
            retval = self._ia_client.get_agent(['a bad param name'])

        # Test with a bad parameter type.
        with self.assertRaises(BadRequest):
            retval = self._ia_client.get_agent([123])

        retval = self._ia_client.get_agent(['example'])

        with self.assertRaises(BadRequest):
            self._ia_client.set_agent({'a bad param name' : 'newvalue'})

        with self.assertRaises(BadRequest):
            self._ia_client.set_agent({123 : 'newvalue'})

        with self.assertRaises(BadRequest):
            self._ia_client.set_agent({'example' : 999})

        self._ia_client.set_agent({'example' : 'newvalue'})

        retval = self._ia_client.get_agent(['example'])

        self.assertEquals(retval['example'], 'newvalue')
        
        retval = self._ia_client.get_agent(['streams'])
        expected_streams_result = {'streams': {'raw': ['quality_flag', 'lon',
            'raw', 'lat'],'parsed': ['quality_flag', 'temp', 'density', 'lon',
            'salinity', 'pressure', 'lat', 'conductivity']}}
        self.assertEqual(retval, expected_streams_result)
        
        retval = self._ia_client.get_agent(['pubfreq'])
        expected_pubfreq_result = {'pubfreq': {'raw': 0, 'parsed': 0}}
        self.assertEqual(retval, expected_pubfreq_result)
        
        retval = self._ia_client.get_agent(['status'])
        expected_status_result = {'status': {
            'parsed_conductivity':'RESOURCE_AGENT_STREAM_STATUS_ALL_CLEAR',
            'parsed_lat': 'RESOURCE_AGENT_STREAM_STATUS_ALL_CLEAR',
            'parsed_pressure': 'RESOURCE_AGENT_STREAM_STATUS_ALL_CLEAR',
            'parsed_temp': 'RESOURCE_AGENT_STREAM_STATUS_ALL_CLEAR',
            'parsed_lon': 'RESOURCE_AGENT_STREAM_STATUS_ALL_CLEAR',
            'parsed_density': 'RESOURCE_AGENT_STREAM_STATUS_ALL_CLEAR',
            'raw_quality_flag': 'RESOURCE_AGENT_STREAM_STATUS_ALL_CLEAR',
            'raw_lon': 'RESOURCE_AGENT_STREAM_STATUS_ALL_CLEAR',
            'parsed_salinity': 'RESOURCE_AGENT_STREAM_STATUS_ALL_CLEAR',
            'parsed_quality_flag': 'RESOURCE_AGENT_STREAM_STATUS_ALL_CLEAR',
            'raw_raw': 'RESOURCE_AGENT_STREAM_STATUS_ALL_CLEAR',
            'raw_lat': 'RESOURCE_AGENT_STREAM_STATUS_ALL_CLEAR'}}
        self.assertEqual(retval, expected_status_result)

        retval = self._ia_client.get_agent(['alarms'])
        #{'alarms': {}}

    def test_poll(self):
        """
        test_poll
        """
        #--------------------------------------------------------------------------------
        # Test observatory polling function thorugh execute resource interface.
        # Verify ResourceAgentCommandEvents are published.
        #--------------------------------------------------------------------------------

        # Start data subscribers.
        self._start_data_subscribers(6)
        self.addCleanup(self._stop_data_subscribers)
        
        # Set up a subscriber to collect command events.
        self._start_event_subscriber('ResourceAgentCommandEvent', 7)
        self.addCleanup(self._stop_event_subscriber)    
        
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
    
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)
        retval = self._ia_client.execute_resource(cmd)
        self.assertSampleDict(retval.result['parsed'])
        retval = self._ia_client.execute_resource(cmd)
        self.assertSampleDict(retval.result['parsed'])
        retval = self._ia_client.execute_resource(cmd)
        self.assertSampleDict(retval.result['parsed'])

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
        
        self._async_event_result.get(timeout=CFG.endpoint.receive.timeout)        
        
        """
        {'origin': '123xyz', 'description': '', 'kwargs': {}, 'args': [], 'execute_command': 'RESOURCE_AGENT_EVENT_INITIALIZE', 'type_': 'ResourceAgentCommandEvent', 'command': 'execute_agent', 'result': None, 'base_types': ['ResourceAgentEvent', 'Event'], 'ts_created': '1349373063952', 'sub_type': '', 'origin_type': ''}
        {'origin': '123xyz', 'description': '', 'kwargs': {}, 'args': [], 'execute_command': 'RESOURCE_AGENT_EVENT_GO_ACTIVE', 'type_': 'ResourceAgentCommandEvent', 'command': 'execute_agent', 'result': None, 'base_types': ['ResourceAgentEvent', 'Event'], 'ts_created': '1349373069507', 'sub_type': '', 'origin_type': ''}
        {'origin': '123xyz', 'description': '', 'kwargs': {}, 'args': [], 'execute_command': 'RESOURCE_AGENT_EVENT_RUN', 'type_': 'ResourceAgentCommandEvent', 'command': 'execute_agent', 'result': None, 'base_types': ['ResourceAgentEvent', 'Event'], 'ts_created': '1349373069547', 'sub_type': '', 'origin_type': ''}
        {'origin': '123xyz', 'description': '', 'kwargs': {}, 'args': [], 'execute_command': 'DRIVER_EVENT_ACQUIRE_SAMPLE', 'type_': 'ResourceAgentCommandEvent', 'command': 'execute_resource', 'result': {'raw': {'quality_flag': 'ok', 'preferred_timestamp': 'driver_timestamp', 'stream_name': 'raw', 'pkt_format_id': 'JSON_Data', 'pkt_version': 1, 'values': [{'binary': True, 'value_id': 'raw', 'value': 'MzkuNzk5OSwyMS45NTM0MSwgNDMuOTIzLCAgIDE0LjMzMjcsIDE1MDYuMjAzLCAwMSBGZWIgMjAwMSwgMDE6MDE6MDA='}], 'driver_timestamp': 3558361870.788932}, 'parsed': {'quality_flag': 'ok', 'preferred_timestamp': 'driver_timestamp', 'stream_name': 'parsed', 'pkt_format_id': 'JSON_Data', 'pkt_version': 1, 'values': [{'value_id': 'temp', 'value': 39.7999}, {'value_id': 'conductivity', 'value': 21.95341}, {'value_id': 'pressure', 'value': 43.923}], 'driver_timestamp': 3558361870.788932}}, 'base_types': ['ResourceAgentEvent', 'Event'], 'ts_created': '1349373071084', 'sub_type': '', 'origin_type': ''}
        {'origin': '123xyz', 'description': '', 'kwargs': {}, 'args': [], 'execute_command': 'DRIVER_EVENT_ACQUIRE_SAMPLE', 'type_': 'ResourceAgentCommandEvent', 'command': 'execute_resource', 'result': {'raw': {'quality_flag': 'ok', 'preferred_timestamp': 'driver_timestamp', 'stream_name': 'raw', 'pkt_format_id': 'JSON_Data', 'pkt_version': 1, 'values': [{'binary': True, 'value_id': 'raw', 'value': 'NjIuNTQxNCw1MC4xNzI3MCwgMzA0LjcwNywgICA2LjE4MDksIDE1MDYuMTU1LCAwMSBGZWIgMjAwMSwgMDE6MDE6MDA='}], 'driver_timestamp': 3558361872.398573}, 'parsed': {'quality_flag': 'ok', 'preferred_timestamp': 'driver_timestamp', 'stream_name': 'parsed', 'pkt_format_id': 'JSON_Data', 'pkt_version': 1, 'values': [{'value_id': 'temp', 'value': 62.5414}, {'value_id': 'conductivity', 'value': 50.1727}, {'value_id': 'pressure', 'value': 304.707}], 'driver_timestamp': 3558361872.398573}}, 'base_types': ['ResourceAgentEvent', 'Event'], 'ts_created': '1349373072613', 'sub_type': '', 'origin_type': ''}
        {'origin': '123xyz', 'description': '', 'kwargs': {}, 'args': [], 'execute_command': 'DRIVER_EVENT_ACQUIRE_SAMPLE', 'type_': 'ResourceAgentCommandEvent', 'command': 'execute_resource', 'result': {'raw': {'quality_flag': 'ok', 'preferred_timestamp': 'driver_timestamp', 'stream_name': 'raw', 'pkt_format_id': 'JSON_Data', 'pkt_version': 1, 'values': [{'binary': True, 'value_id': 'raw', 'value': 'NDYuODk0Niw5MS4wNjkyNCwgMzQyLjkyMCwgICA3LjQyNzgsIDE1MDYuOTE2LCAwMSBGZWIgMjAwMSwgMDE6MDE6MDA='}], 'driver_timestamp': 3558361873.907537}, 'parsed': {'quality_flag': 'ok', 'preferred_timestamp': 'driver_timestamp', 'stream_name': 'parsed', 'pkt_format_id': 'JSON_Data', 'pkt_version': 1, 'values': [{'value_id': 'temp', 'value': 46.8946}, {'value_id': 'conductivity', 'value': 91.06924}, {'value_id': 'pressure', 'value': 342.92}], 'driver_timestamp': 3558361873.907537}}, 'base_types': ['ResourceAgentEvent', 'Event'], 'ts_created': '1349373074141', 'sub_type': '', 'origin_type': ''}
        {'origin': '123xyz', 'description': '', 'kwargs': {}, 'args': [], 'execute_command': 'RESOURCE_AGENT_EVENT_RESET', 'type_': 'ResourceAgentCommandEvent', 'command': 'execute_agent', 'result': None, 'base_types': ['ResourceAgentEvent', 'Event'], 'ts_created': '1349373076321', 'sub_type': '', 'origin_type': ''}        
        """
        
        log.warning('******************* Checking events in test_poll:')
        for x in self._events_received:
            log.warning(str(x))
        self.assertGreaterEqual(len(self._events_received), 7)
        
        self._async_sample_result.get(timeout=CFG.endpoint.receive.timeout)
        self.assertEquals(len(self._samples_received), 6)
        
    def test_autosample(self):
        """
        test_autosample
        Test instrument driver execute interface to start and stop streaming
        mode. Verify ResourceAgentResourceStateEvents are publsihed.
        """
        
        # Start data subscribers.
        self._start_data_subscribers(6)
        self.addCleanup(self._stop_data_subscribers)    
        
        # Set up a subscriber to collect error events.
        self._start_event_subscriber('ResourceAgentResourceStateEvent', 7)
        self.addCleanup(self._stop_event_subscriber)            
        
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
    
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=SBE37ProtocolEvent.START_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)
        
        gevent.sleep(15)
        
        cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)
 
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        self._async_event_result.get(timeout=CFG.endpoint.receive.timeout)
        self.assertGreaterEqual(len(self._events_received), 6)

        self._async_sample_result.get(timeout=CFG.endpoint.receive.timeout)
        self.assertGreaterEqual(len(self._samples_received), 6)

    def test_capabilities(self):
        """
        test_capabilities
        Test the ability to retrieve agent and resource parameter and command
        capabilities in various system states.
        """

        agt_cmds_all = [
            ResourceAgentEvent.INITIALIZE,
            ResourceAgentEvent.RESET,
            ResourceAgentEvent.GO_ACTIVE,
            ResourceAgentEvent.GO_INACTIVE,
            ResourceAgentEvent.RUN,
            ResourceAgentEvent.CLEAR,
            ResourceAgentEvent.PAUSE,
            ResourceAgentEvent.RESUME,
            ResourceAgentEvent.GO_COMMAND,
            ResourceAgentEvent.GO_DIRECT_ACCESS           
        ]
        
        agt_pars_all = ['example',
                        'alarms',
                        'streams',
                        'status',
                        'pubfreq'
                        ]
        
        res_cmds_all =[
            SBE37ProtocolEvent.TEST,
            SBE37ProtocolEvent.ACQUIRE_SAMPLE,
            SBE37ProtocolEvent.START_AUTOSAMPLE,
            SBE37ProtocolEvent.STOP_AUTOSAMPLE
        ]
        
        res_iface_all = [
            'get_resource',
            'set_resource',
            'execute_resource',
            'ping_resource',
            'get_resource_state'            
            ]
        
        res_cmds_iface_all = list(res_cmds_all)
        res_cmds_iface_all.extend(res_iface_all)
        
        res_pars_all = PARAMS.keys()
        
        
        def sort_caps(caps_list):
            agt_cmds = []
            agt_pars = []
            res_cmds = []
            res_iface = []
            res_pars = []
            
            if len(caps_list)>0 and isinstance(caps_list[0], AgentCapability):
                agt_cmds = [x.name for x in caps_list if x.cap_type==CapabilityType.AGT_CMD]
                agt_pars = [x.name for x in caps_list if x.cap_type==CapabilityType.AGT_PAR]
                res_cmds = [x.name for x in caps_list if x.cap_type==CapabilityType.RES_CMD]
                res_iface = [x.name for x in caps_list if x.cap_type==CapabilityType.RES_IFACE]
                res_pars = [x.name for x in caps_list if x.cap_type==CapabilityType.RES_PAR]
            
            elif len(caps_list)>0 and isinstance(caps_list[0], dict):
                agt_cmds = [x['name'] for x in caps_list if x['cap_type']==CapabilityType.AGT_CMD]
                agt_pars = [x['name'] for x in caps_list if x['cap_type']==CapabilityType.AGT_PAR]
                res_cmds = [x['name'] for x in caps_list if x['cap_type']==CapabilityType.RES_CMD]
                res_iface = [x['name'] for x in caps_list if x['cap_type']==CapabilityType.RES_IFACE]
                res_pars = [x['name'] for x in caps_list if x['cap_type']==CapabilityType.RES_PAR]

            return agt_cmds, agt_pars, res_cmds, res_iface, res_pars
             
        
        ##################################################################
        # UNINITIALIZED
        ##################################################################
        
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)        
        
        # Get exposed capabilities in current state.
        retval = self._ia_client.get_capabilities()
        
        # Validate capabilities for state UNINITIALIZED.
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)
        
        agt_cmds_uninitialized = [
            ResourceAgentEvent.INITIALIZE
        ]
                        
        self.assertItemsEqual(agt_cmds, agt_cmds_uninitialized)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_iface, [])
        self.assertItemsEqual(res_pars, [])
        
        # Get exposed capabilities in all states.
        retval = self._ia_client.get_capabilities(False)        

        # Validate all capabilities as read from state UNINITIALIZED.
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)
       
        res_cmds_iface_uninitialized_all = res_iface_all
       
        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_iface, res_iface_all)
        self.assertItemsEqual(res_pars, [])
                
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)
        
        ##################################################################
        # INACTIVE
        ##################################################################        

        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        # Get exposed capabilities in current state.
        retval = self._ia_client.get_capabilities()

        # Validate capabilities for state INACTIVE.
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)
                
        agt_cmds_inactive = [
            ResourceAgentEvent.GO_ACTIVE,
            ResourceAgentEvent.RESET
        ]
        
        res_iface_inactive = [
            'ping_resource',
            'get_resource_state'
        ]
        
            
        self.assertItemsEqual(agt_cmds, agt_cmds_inactive)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_iface, res_iface_inactive)
        self.assertItemsEqual(res_pars, [])
        
        # Get exposed capabilities in all states.
        retval = self._ia_client.get_capabilities(False)        
 
         # Validate all capabilities as read from state INACTIVE.
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)
 
        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_iface, res_iface_all)
        self.assertItemsEqual(res_pars, [])
        
        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = self._ia_client.execute_agent(cmd)
        
        ##################################################################
        # IDLE
        ##################################################################                
        
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        # Get exposed capabilities in current state.
        retval = self._ia_client.get_capabilities()

         # Validate capabilities for state IDLE.
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)

        agt_cmds_idle = [
            ResourceAgentEvent.GO_INACTIVE,
            ResourceAgentEvent.RESET,
            ResourceAgentEvent.RUN
        ]
        
        res_iface_idle = [
            'ping_resource',
            'get_resource_state'            
        ]
        
        self.assertItemsEqual(agt_cmds, agt_cmds_idle)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_iface, res_iface_idle)
        self.assertItemsEqual(res_pars, [])
        
        # Get exposed capabilities in all states as read from IDLE.
        retval = self._ia_client.get_capabilities(False)        
        
         # Validate all capabilities as read from state IDLE.
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)
        
        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_iface, res_iface_all)
        self.assertItemsEqual(res_pars, [])
                        
        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = self._ia_client.execute_agent(cmd)
        
        ##################################################################
        # COMMAND
        ##################################################################                
        
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        # Get exposed capabilities in current state.
        retval = self._ia_client.get_capabilities()

         # Validate capabilities of state COMMAND
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)

        agt_cmds_command = [
            ResourceAgentEvent.CLEAR,
            ResourceAgentEvent.RESET,
            ResourceAgentEvent.GO_DIRECT_ACCESS,
            ResourceAgentEvent.GO_INACTIVE,
            ResourceAgentEvent.PAUSE
        ]

        res_cmds_command = [
            SBE37ProtocolEvent.TEST,
            SBE37ProtocolEvent.ACQUIRE_SAMPLE,
            SBE37ProtocolEvent.START_AUTOSAMPLE
        ]
        
        self.assertItemsEqual(agt_cmds, agt_cmds_command)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_command)
        self.assertItemsEqual(res_iface, res_iface_all)
        self.assertItemsEqual(res_pars, res_pars_all)

        # Get exposed capabilities in all states as read from state COMMAND.
        retval = self._ia_client.get_capabilities(False)        
        
         # Validate all capabilities as read from state COMMAND
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_all)
        self.assertItemsEqual(res_iface, res_iface_all)
        self.assertItemsEqual(res_pars, res_pars_all)
        
        cmd = AgentCommand(command=SBE37ProtocolEvent.START_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)
    
        ##################################################################
        # STREAMING
        ##################################################################                        
        
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.STREAMING)

        # Get exposed capabilities in current state.
        retval = self._ia_client.get_capabilities()

         # Validate capabilities of state STREAMING
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)

 
        agt_cmds_streaming = [
            ResourceAgentEvent.RESET,
            ResourceAgentEvent.GO_INACTIVE
        ]

        res_cmds_streaming = [
            SBE37ProtocolEvent.STOP_AUTOSAMPLE,
        ]

        res_iface_streaming = [
            'get_resource',
            'execute_resource',
            'ping_resource',
            'get_resource_state'                                               
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_streaming)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_streaming)
        self.assertItemsEqual(res_iface, res_iface_streaming)
        self.assertItemsEqual(res_pars, res_pars_all)
        
        # Get exposed capabilities in all states as read from state STREAMING.
        retval = self._ia_client.get_capabilities(False)        
        
         # Validate all capabilities as read from state COMMAND
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_all)
        self.assertItemsEqual(res_iface, res_iface_all)
        self.assertItemsEqual(res_pars, res_pars_all)
        
        gevent.sleep(5)
        
        cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)
        
        ##################################################################
        # COMMAND
        ##################################################################                        
        
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        # Get exposed capabilities in current state.
        retval = self._ia_client.get_capabilities()

         # Validate capabilities of state COMMAND
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_command)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_command)
        self.assertItemsEqual(res_iface, res_iface_all)
        self.assertItemsEqual(res_pars, res_pars_all)        
        
        # Get exposed capabilities in all states as read from state STREAMING.
        retval = self._ia_client.get_capabilities(False)        
        
         # Validate all capabilities as read from state COMMAND
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_all)
        self.assertItemsEqual(res_iface, res_iface_all)
        self.assertItemsEqual(res_pars, res_pars_all)        
        
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        
        ##################################################################
        # UNINITIALIZED
        ##################################################################                        
        
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        # Get exposed capabilities in current state.
        retval = self._ia_client.get_capabilities()
        
        # Validate capabilities for state UNINITIALIZED.
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_uninitialized)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_iface, [])
        self.assertItemsEqual(res_pars, [])
        
        # Get exposed capabilities in all states.
        retval = self._ia_client.get_capabilities(False)        

        # Validate all capabilities as read from state UNINITIALIZED.
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)
       
        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_iface, res_iface_all)
        self.assertItemsEqual(res_pars, [])        
        
    def test_command_errors(self):
        """
        test_command_errors
        Test illegal behavior and replies. Verify ResourceAgentErrorEvents
        are published.
        """
        
        # Set up a subscriber to collect error events.
        self._start_event_subscriber('ResourceAgentErrorEvent', 6)
        self.addCleanup(self._stop_event_subscriber)    
        
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
    
        # Try to execute agent command with no command arg.
        with self.assertRaises(BadRequest):
            retval = self._ia_client.execute_agent()    

        # Try to execute agent command with bogus command.
        with self.assertRaises(BadRequest):
            cmd = AgentCommand(command='BOGUS_COMMAND')
            retval = self._ia_client.execute_agent()

        # Try to execute a valid command, wrong state.
        with self.assertRaises(Conflict):
            cmd = AgentCommand(command=ResourceAgentEvent.RUN)
            retval = self._ia_client.execute_agent(cmd)

        # Try to execute the resource, wrong state.
        with self.assertRaises(Conflict):
            cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)
            retval = self._ia_client.execute_resource(cmd)        

        # Try initializing with a bogus option driver config parameter.
        with self.assertRaises(BadRequest):
            bogus_config = {
                'no' : 'idea'
            }
            cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE,
                           args=[bogus_config])
            retval = self._ia_client.execute_agent()

        # Initialize the agent correctly.
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE,
                        args=[DVR_CONFIG])
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        # Issue a good resource command and verify result.
        cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)
        retval = self._ia_client.execute_resource(cmd)
        self.assertSampleDict(retval.result['parsed'])

        # Try to issue a wrong state resource command.
        with self.assertRaises(Conflict):
            cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
            retval = self._ia_client.execute_resource(cmd)

        # Reset and shutdown.
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        self._async_event_result.get(timeout=CFG.endpoint.receive.timeout)
        self.assertEquals(len(self._events_received), 6)
        
    def test_direct_access(self):
        """
        test_direct_access
        Test agent direct_access command. This causes creation of
        driver process and transition to direct access.
        """

        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
    
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_DIRECT_ACCESS,
                            #kwargs={'session_type': DirectAccessTypes.telnet,
                            kwargs={'session_type':DirectAccessTypes.vsp,
                            'session_timeout':600,
                            'inactivity_timeout':600})
        
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.DIRECT_ACCESS)
                
        log.info("GO_DIRECT_ACCESS retval=" + str(retval.result))

        # {'status': 0, 'type_': 'AgentCommandResult', 'command': 'RESOURCE_AGENT_EVENT_GO_DIRECT_ACCESS',
        # 'result': {'token': 'F2B6EED3-F926-4B3B-AE80-4F8DE79276F3', 'ip_address': 'Edwards-MacBook-Pro.local', 'port': 8000},
        # 'ts_execute': '1344889063861', 'command_id': ''}
        
        host = retval.result['ip_address']
        port = retval.result['port']
        token = retval.result['token']
        
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))
        s.settimeout(0.0)
        
        s.sendall('ts\r\n')
        buf = self._socket_listen(s, None, 3)

        # CORRECT PATTERN TO MATCH IN RESPONSE BUFFER:
        """
        ts
        
        -0.1964,85.12299, 697.168,   39.5241, 1506.965, 01 Feb 2001, 01:01:00
        """
        
        sample_pattern = r'^#? *(-?\d+\.\d+), *(-?\d+\.\d+), *(-?\d+\.\d+)'
        sample_pattern += r'(, *(-?\d+\.\d+))?(, *(-?\d+\.\d+))?'
        sample_pattern += r'(, *(\d+) +([a-zA-Z]+) +(\d+), *(\d+):(\d+):(\d+))?'
        sample_pattern += r'(, *(\d+)-(\d+)-(\d+), *(\d+):(\d+):(\d+))?'
        sample_regex = re.compile(sample_pattern)
        lines = buf.split('\r\n')
        
        sample_count = 0
        for x in lines:
            if sample_regex.match(x):
                sample_count += 1
        self.assertEqual(sample_count, 1)            

        s.sendall('ds\r\n')
        buf = self._socket_listen(s, None, 3)

        # CORRECT PATTERN TO MATCH IN RESPONSE BUFFER:
        """
        ds
        SBE37-SMP V 2.6 SERIAL NO. 2165   01 Feb 2001  01:01:00
        not logging: received stop command
        sample interval = 23195 seconds
        samplenumber = 0, free = 200000
        do not transmit real-time data
        do not output salinity with each sample
        do not output sound velocity with each sample
        do not store time with each sample
        number of samples to average = 0
        reference pressure = 0.0 db
        serial sync mode disabled
        wait time after serial sync sampling = 0 seconds
        internal pump is installed
        temperature = 7.54 deg C
        WARNING: LOW BATTERY VOLTAGE!!
        """

        self.assertNotEqual(buf.find('SBE37-SMP'), -1)
        self.assertNotEqual(buf.find('sample interval'), -1)
        self.assertNotEqual(buf.find('samplenumber'), -1)
        self.assertNotEqual(buf.find('number of samples to average'), -1)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_COMMAND)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)
        
        cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)
        retval = self._ia_client.execute_resource(cmd)
        self.assertSampleDict(retval.result['parsed'])

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

    @unittest.skip('Test very long and simulator not accurate here.')
    def test_test(self):
        """
        test_test
        """
        
        # Set up a subscriber to collect command events.
        self._start_event_subscriber('ResourceAgentAsyncResultEvent', 1)
        self.addCleanup(self._stop_event_subscriber)    
        
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
    
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=SBE37ProtocolEvent.TEST)
        retval = self._ia_client.execute_resource(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.TEST)

        start_time = time.time()
        while state != ResourceAgentState.COMMAND:
            gevent.sleep(1)
            elapsed_time = time.time() - start_time
            log.debug('Device testing: %i seconds elapsed', elapsed_time)
            state = self._ia_client.get_agent_state()

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
                
        # Verify we received the test result and it passed.
        # We need to verify that the simulator has the proper behavior.
        # I believe the SBE37 driver logic is accurate and correct for
        # the real hardware, but it gives Failures on the simulator test
        # output.
        """
        {'origin': '123xyz', 'description': '',
        'type_': 'ResourceAgentAsyncResultEvent',
        'command': 'DRIVER_EVENT_TEST',
        'result':
            {'pres_data': 't\x00p\x00\r\x00\n\x00
            -7.320\r\n
            -7.750\r\n
            -6.811\r\n
            ...
            -6.796\r\n\r\nS>',
            'success': 'Failed',
            'cond_data': 't\x00c\x00\r\x00\n\x00
            0.00705\r\n
            0.08241\r\n
            0.00563\r\n 
            ...
            0.03455\r\n\r\nS>',
            'cmd': 'DRIVER_EVENT_TEST',
            'temp_test': 'Failed',
            'pres_test': 'Failed',
            'cond_test': 'Failed',
            'temp_data': 't\x00t\x00\r\x00\n\x00
            19.7688\r\n
            18.4637\r\n
            15.2186\r\n
            ...
            16.1100\r\n\r\nS>',
            'desc': 'SBE37 self-test result'}
        """
        
        self._async_event_result.get(timeout=CFG.endpoint.receive.timeout)                        
        self.assertGreaterEqual(len(self._events_received), 1)        
        #self.assertEqual(test_results[0]['value']['success'], 'Passed')

    @unittest.skip('This test used to track down publisher threadsafety bug.')
    def test_states_special(self):
        """
        test_states_special
        """
        states = [
            ResourceAgentState.INACTIVE,
            ResourceAgentState.IDLE,
            ResourceAgentState.COMMAND,
            ResourceAgentState.STOPPED,
            ResourceAgentState.COMMAND,
            ResourceAgentState.IDLE,
            ResourceAgentState.COMMAND,
            ResourceAgentState.UNINITIALIZED                        
        ]
        states.reverse()

        publisher = EventPublisher()

        # Set up a subscriber to collect error events.
        self._start_event_subscriber('ResourceAgentStateEvent', 8)
        self.addCleanup(self._stop_event_subscriber)    

        gevent.sleep(1)

        def loop(publisher, ev):
            #pub = EventPublisher()
            pub = publisher
            while not ev.wait(timeout=0.1):
                event_data = {
                    'state': ResourceAgentState.INACTIVE
                }
                result = pub.publish_event(event_type='ResourceAgentStateEvent',
                              origin='xxxxzzzz', **event_data)

        gl = []
        for x in range(20):
            ev = gevent.event.Event()
            gl.append((gevent.spawn(loop, publisher, ev), ev))

        def cleanup_gl(gl_array):
            for g in gl_array:
                g[1].set()

            #gevent.killall(gl_array)
            gevent.joinall([g[0] for g in gl_array])
        self.addCleanup(cleanup_gl, gl)

        while len(states)>0:
            gevent.sleep(5)
            state = states.pop()
            event_data = {
                'state': state
            }
            result = publisher.publish_event(event_type='ResourceAgentStateEvent',
                              origin=IA_RESOURCE_ID, **event_data)
            log.info('Published event %s with result %s.',
                     state, str(result))
        self._async_event_result.get(timeout=CFG.endpoint.receive.timeout)
        self.assertEquals(len(self._events_received), 8)
        
    def test_data_buffering(self):
        """
        test_data_buffering
        """
        
        # Start data subscribers.
        self._start_data_subscribers(1)
        self.addCleanup(self._stop_data_subscribers)    
        
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
    
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        # Set buffering parameters.
        pubfreq = {
            'parsed':15,
            'raw':15
        }
        self._ia_client.set_agent({'pubfreq':pubfreq})
        retval = self._ia_client.get_agent(['pubfreq'])
        print '#############'
        print str(retval)
        
        cmd = AgentCommand(command=SBE37ProtocolEvent.START_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)
        
        gevent.sleep(60)
        
        cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)
 
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        self._async_sample_result.get(timeout=CFG.endpoint.receive.timeout)
        self.assertLessEqual(len(self._samples_received), 16)
