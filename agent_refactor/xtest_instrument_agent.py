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
import os
import signal
import time
import unittest
from datetime import datetime
import uuid

# 3rd party imports.
import gevent
from gevent import spawn
from gevent.event import AsyncResult
from nose.plugins.attrib import attr
from mock import patch

# Pyon pubsub and event support.
from pyon.public import StreamSubscriberRegistrar
from pyon.event.event import EventSubscriber, EventPublisher

# Pyon unittest support.
from pyon.util.int_test import IonIntegrationTestCase

# Pyon exceptions.
from pyon.core.exception import IonException
from pyon.core.exception import BadRequest
from pyon.core.exception import Conflict
from pyon.core.exception import Timeout
from pyon.core.exception import NotFound
from pyon.core.exception import ServerError
from pyon.core.exception import ResourceError

# Agent imports.
from pyon.util.context import LocalContextMixin
from pyon.agent.agent import ResourceAgentClient
from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent

# Driver imports.
from ion.agents.instrument.direct_access.direct_access_server import DirectAccessTypes
from ion.agents.instrument.driver_int_test_support import DriverIntegrationTestSupport
from ion.agents.port.logger_process import EthernetDeviceLogger
from ion.agents.instrument.driver_process import DriverProcessType
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverConnectionState

# Objects and clients.
from interface.objects import AgentCommand
from interface.objects import StreamQuery
from interface.objects import CapabilityType
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

# Stream defs.
from prototype.sci_data.stream_defs import ctd_stream_definition

# MI imports.
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Parameter
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent
from mi.instrument.seabird.sbe37smb.ooicore.driver import PACKET_CONFIG

# TODO chagne the path following the refactor.
# bin/nosetests -s -v ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_initialize
# bin/nosetests -s -v ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_resource_states
# bin/nosetests -s -v ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_states
# bin/nosetests -s -v ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_get_set
# bin/nosetests -s -v ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_get_set_errors
# bin/nosetests -s -v ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_poll
# bin/nosetests -s -v ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_autosample
# bin/nosetests -s -v ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_capabilities
# bin/nosetests -s -v ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_command_errors


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

@attr('HARDWARE', group='mi')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class TestInstrumentAgent(IonIntegrationTestCase):
    """
    Test cases for instrument agent class. Functions in this class provide
    instrument agent integration tests and provide a tutorial on use of
    the agent setup and interface.
    """
    
    ############################################################################
    # Setup, teardown, helpers.
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
                                                     DELIM,
                                                     WORK_DIR)
        
        # Start port agent, add stop to cleanup.
        self._pagent = None        
        self._start_pagent()
        self.addCleanup(self._support.stop_pagent)    
        
        # Start container.
        log.info('Staring capability container.')
        self._start_container()
        
        # Bring up services in a deploy file (no need to message)
        log.info('Staring deploy services.')
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Create agent config.
        agent_config = {
            'driver_config' : DVR_CONFIG,
            'stream_config' : None,
            'agent'         : {'resource_id': IA_RESOURCE_ID},
            'test_mode' : True
        }

        # Start instrument agent.
        self._ia_pid = None
        log.debug("TestInstrumentAgent.setup(): starting IA.")
        log.info('Agent config: %s', str(agent_config))
        container_client = ContainerAgentClient(node=self.container.node,
                                                name=self.container.name)
        self._ia_pid = container_client.spawn_process(name=IA_NAME,
                                                      module=IA_MOD, 
                                                      cls=IA_CLS, 
                                                      config=agent_config)      
        log.info('Agent pid=%s.', str(self._ia_pid))
        
        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = None
        self._ia_client = ResourceAgentClient(IA_RESOURCE_ID,
                                              process=FakeProcess())
        log.info('Got ia client %s.', str(self._ia_client))        
        
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
        
    def assertSampleDict(self, val):
        """
        Verify the value is a sample dictionary for the sbe37.
        """
        # AgentCommandResult.result['parsed']
        # {'p': [707.311], 'c': [69.03532], 'stream_name': 'parsed', 't': [85.9109], 'time': [1343258355.202828]}
        self.assertTrue(isinstance(val, dict))
        self.assertTrue(val.has_key('c'))
        self.assertTrue(val.has_key('t'))
        self.assertTrue(val.has_key('p'))
        self.assertTrue(val.has_key('time'))
        c = val['c'][0]
        t = val['t'][0]
        p = val['p'][0]
        time = val['time'][0]
    
        self.assertTrue(isinstance(c, float))
        self.assertTrue(isinstance(t, float))
        self.assertTrue(isinstance(p, float))
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
        Test agent initialize command. This causes creation of
        driver process and transition to inactive.
        """
        
        # We start in uninitialized state.
        # In this state there is no driver process.
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
        
        # Initialize the agent.
        # The agent is spawned with a driver config, but you can pass one in
        # optinally with the initialize command. This validates the driver
        # config, launches a driver process and connects to it via messaging.
        # If successful, we switch to the inactive state.
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        # Reset the agent. This causes the driver messaging to be stopped,
        # the driver process to end and switches us back to uninitialized.
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
        
    def test_resource_states(self):
        """
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
        
        self._async_event_result.get(timeout=2)
        self.assertGreaterEqual(len(self._events_received), 6)
        
    def test_states(self):
        """
        Test agent state transitions through execute agent interface.
        Verify agent state status as we go. Verify ResourceAgentStateEvents
        are published.
        """

        # Set up a subscriber to collect error events.
        self._start_event_subscriber('ResourceAgentStateEvent', 8)
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
            
        self._async_event_result.get(timeout=2)
        self.assertEquals(len(self._events_received), 8)
            
    def test_get_set(self):
        """
        Test instrument driver get and set resource interface. Verify
        ResourceAgentResourceConfigEvents are published.
        """
                
        # Set up a subscriber to collect error events.
        self._start_event_subscriber('ResourceAgentResourceConfigEvent', 3)
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
    
        self._async_event_result.get(timeout=2)
        self.assertEquals(len(self._events_received), 3)


    def test_get_set_errors(self):
        """
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

    def test_poll(self):
        """
        Test observatory polling function thorugh execute resource interface.
        Verify ResourceAgentCommandEvents are published.
        """
        
        # Set up a subscriber to collect error events.
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
               
        self._async_event_result.get(timeout=2)
        self.assertEquals(len(self._events_received), 7)
               
    def test_autosample(self):
        """
        Test instrument driver execute interface to start and stop streaming
        mode. Verify ResourceAgentResourceStateEvents are publsihed.
        """
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
        
        time.sleep(15)
        
        cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)
 
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        self._async_event_result.get(timeout=2)
        self.assertGreaterEqual(len(self._events_received), 8)

    def test_capabilities(self):
        """
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
        
        agt_pars_all = ['aparam1']
        
        res_cmds_all =[
            SBE37ProtocolEvent.TEST,
            SBE37ProtocolEvent.ACQUIRE_SAMPLE,
            SBE37ProtocolEvent.START_AUTOSAMPLE,
            SBE37ProtocolEvent.STOP_AUTOSAMPLE
        ]
                
        res_pars_all = PARAMS.keys()
        
        ##################################################################
        # UNINITIALIZED
        ##################################################################
        
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)        
        
        # Get exposed capabilities in current state.
        retval = self._ia_client.get_capabilities()
        
        # Validate capabilities for state UNINITIALIZED.
        agt_cmds = [x.name for x in retval if x.cap_type==CapabilityType.AGT_CMD]
        agt_pars = [x.name for x in retval if x.cap_type==CapabilityType.AGT_PAR]
        res_cmds = [x.name for x in retval if x.cap_type==CapabilityType.RES_CMD]
        res_pars = [x.name for x in retval if x.cap_type==CapabilityType.RES_PAR]
        
        agt_cmds_uninitialized = [
            ResourceAgentEvent.INITIALIZE
        ]
        self.assertItemsEqual(agt_cmds, agt_cmds_uninitialized)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])
        
        # Get exposed capabilities in all states.
        retval = self._ia_client.get_capabilities(False)        

        # Validate all capabilities as read from state UNINITIALIZED.
        agt_cmds = [x.name for x in retval if x.cap_type==CapabilityType.AGT_CMD]
        agt_pars = [x.name for x in retval if x.cap_type==CapabilityType.AGT_PAR]
        res_cmds = [x.name for x in retval if x.cap_type==CapabilityType.RES_CMD]
        res_pars = [x.name for x in retval if x.cap_type==CapabilityType.RES_PAR]
        
        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
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
        agt_cmds = [x.name for x in retval if x.cap_type==CapabilityType.AGT_CMD]
        agt_pars = [x.name for x in retval if x.cap_type==CapabilityType.AGT_PAR]
        res_cmds = [x.name for x in retval if x.cap_type==CapabilityType.RES_CMD]
        res_pars = [x.name for x in retval if x.cap_type==CapabilityType.RES_PAR]
                
        agt_cmds_inactive = [
            ResourceAgentEvent.GO_ACTIVE,
            ResourceAgentEvent.RESET
        ]
        
        self.assertItemsEqual(agt_cmds, agt_cmds_inactive)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])
        
        # Get exposed capabilities in all states.
        retval = self._ia_client.get_capabilities(False)        
 
         # Validate all capabilities as read from state INACTIVE.
        agt_cmds = [x.name for x in retval if x.cap_type==CapabilityType.AGT_CMD]
        agt_pars = [x.name for x in retval if x.cap_type==CapabilityType.AGT_PAR]
        res_cmds = [x.name for x in retval if x.cap_type==CapabilityType.RES_CMD]
        res_pars = [x.name for x in retval if x.cap_type==CapabilityType.RES_PAR]
 
        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
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
        agt_cmds = [x.name for x in retval if x.cap_type==CapabilityType.AGT_CMD]
        agt_pars = [x.name for x in retval if x.cap_type==CapabilityType.AGT_PAR]
        res_cmds = [x.name for x in retval if x.cap_type==CapabilityType.RES_CMD]
        res_pars = [x.name for x in retval if x.cap_type==CapabilityType.RES_PAR]

        agt_cmds_idle = [
            ResourceAgentEvent.GO_INACTIVE,
            ResourceAgentEvent.RESET,
            ResourceAgentEvent.RUN
        ]
        
        self.assertItemsEqual(agt_cmds, agt_cmds_idle)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])
        
        # Get exposed capabilities in all states as read from IDLE.
        retval = self._ia_client.get_capabilities(False)        
        
         # Validate all capabilities as read from state IDLE.
        agt_cmds = [x.name for x in retval if x.cap_type==CapabilityType.AGT_CMD]
        agt_pars = [x.name for x in retval if x.cap_type==CapabilityType.AGT_PAR]
        res_cmds = [x.name for x in retval if x.cap_type==CapabilityType.RES_CMD]
        res_pars = [x.name for x in retval if x.cap_type==CapabilityType.RES_PAR]
        
        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
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
        agt_cmds = [x.name for x in retval if x.cap_type==CapabilityType.AGT_CMD]
        agt_pars = [x.name for x in retval if x.cap_type==CapabilityType.AGT_PAR]
        res_cmds = [x.name for x in retval if x.cap_type==CapabilityType.RES_CMD]
        res_pars = [x.name for x in retval if x.cap_type==CapabilityType.RES_PAR]

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
        self.assertItemsEqual(res_pars, res_pars_all)
        
        # Get exposed capabilities in all states as read from state COMMAND.
        retval = self._ia_client.get_capabilities(False)        
        
         # Validate all capabilities as read from state COMMAND
        agt_cmds = [x.name for x in retval if x.cap_type==CapabilityType.AGT_CMD]
        agt_pars = [x.name for x in retval if x.cap_type==CapabilityType.AGT_PAR]
        res_cmds = [x.name for x in retval if x.cap_type==CapabilityType.RES_CMD]
        res_pars = [x.name for x in retval if x.cap_type==CapabilityType.RES_PAR]        
                
        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_all)
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
        agt_cmds = [x.name for x in retval if x.cap_type==CapabilityType.AGT_CMD]
        agt_pars = [x.name for x in retval if x.cap_type==CapabilityType.AGT_PAR]
        res_cmds = [x.name for x in retval if x.cap_type==CapabilityType.RES_CMD]
        res_pars = [x.name for x in retval if x.cap_type==CapabilityType.RES_PAR]

 
        agt_cmds_streaming = [
            ResourceAgentEvent.RESET,
            ResourceAgentEvent.GO_INACTIVE
        ]

        res_cmds_streaming = [
            SBE37ProtocolEvent.STOP_AUTOSAMPLE
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_streaming)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_streaming)
        self.assertItemsEqual(res_pars, res_pars_all)
        
        # Get exposed capabilities in all states as read from state STREAMING.
        retval = self._ia_client.get_capabilities(False)        
        
         # Validate all capabilities as read from state COMMAND
        agt_cmds = [x.name for x in retval if x.cap_type==CapabilityType.AGT_CMD]
        agt_pars = [x.name for x in retval if x.cap_type==CapabilityType.AGT_PAR]
        res_cmds = [x.name for x in retval if x.cap_type==CapabilityType.RES_CMD]
        res_pars = [x.name for x in retval if x.cap_type==CapabilityType.RES_PAR]        
        
        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_all)
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
        agt_cmds = [x.name for x in retval if x.cap_type==CapabilityType.AGT_CMD]
        agt_pars = [x.name for x in retval if x.cap_type==CapabilityType.AGT_PAR]
        res_cmds = [x.name for x in retval if x.cap_type==CapabilityType.RES_CMD]
        res_pars = [x.name for x in retval if x.cap_type==CapabilityType.RES_PAR]
        
        self.assertItemsEqual(agt_cmds, agt_cmds_command)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_command)
        self.assertItemsEqual(res_pars, res_pars_all)        
        
        # Get exposed capabilities in all states as read from state STREAMING.
        retval = self._ia_client.get_capabilities(False)        
        
         # Validate all capabilities as read from state COMMAND
        agt_cmds = [x.name for x in retval if x.cap_type==CapabilityType.AGT_CMD]
        agt_pars = [x.name for x in retval if x.cap_type==CapabilityType.AGT_PAR]
        res_cmds = [x.name for x in retval if x.cap_type==CapabilityType.RES_CMD]
        res_pars = [x.name for x in retval if x.cap_type==CapabilityType.RES_PAR]        
        
        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_all)
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
        agt_cmds = [x.name for x in retval if x.cap_type==CapabilityType.AGT_CMD]
        agt_pars = [x.name for x in retval if x.cap_type==CapabilityType.AGT_PAR]
        res_cmds = [x.name for x in retval if x.cap_type==CapabilityType.RES_CMD]
        res_pars = [x.name for x in retval if x.cap_type==CapabilityType.RES_PAR]
        
        self.assertItemsEqual(agt_cmds, agt_cmds_uninitialized)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])
        
        # Get exposed capabilities in all states.
        retval = self._ia_client.get_capabilities(False)        

        # Validate all capabilities as read from state UNINITIALIZED.
        agt_cmds = [x.name for x in retval if x.cap_type==CapabilityType.AGT_CMD]
        agt_pars = [x.name for x in retval if x.cap_type==CapabilityType.AGT_PAR]
        res_cmds = [x.name for x in retval if x.cap_type==CapabilityType.RES_CMD]
        res_pars = [x.name for x in retval if x.cap_type==CapabilityType.RES_PAR]
        
        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])        
                
    def test_command_errors(self):
        """
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

        self._async_event_result.get(timeout=2)
        self.assertEquals(len(self._events_received), 6)
        
    @unittest.skip('Direct access test to be finished by adding the telnet client, manual for now.')
    def test_direct_access(self):
        """
        Test agent direct_access command. This causes creation of
        driver process and transition to direct access.
        """
        pass


