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
from pyon.public import log
from pyon.public import CFG
from pyon.public import StreamSubscriberRegistrar
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from pyon.event.event import EventSubscriber, EventPublisher
from pyon.core.exception import InstParameterError

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

# Agent imports.
from pyon.agent.agent import ResourceAgentClient
from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent
from ion.agents.instrument.direct_access.direct_access_server import DirectAccessTypes
from ion.agents.instrument.driver_int_test_support import DriverIntegrationTestSupport
from ion.agents.port.logger_process import EthernetDeviceLogger
from ion.agents.instrument.driver_process import DriverProcessType

# Objects and clients.
from interface.objects import AgentCommand
from interface.objects import StreamQuery
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

# Stream defs.
from prototype.sci_data.stream_defs import ctd_stream_definition

# MI imports.
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Parameter
from mi.instrument.seabird.sbe37smb.ooicore.driver import PACKET_CONFIG

# TODO chagne the path following the refactor.
# bin/nosetests -s -v ion/agents/instrument/refactor/test_instrument_agent.py:TestInstrumentAgent.test_initialize
# bin/nosetests -s -v ion/agents/instrument/refactor/test_instrument_agent.py:TestInstrumentAgent.test_states
# bin/nosetests -s -v ion/agents/instrument/refactor/test_instrument_agent.py:TestInstrumentAgent.test_get_set
# bin/nosetests -s -v ion/agents/instrument/refactor/test_instrument_agent.py:TestInstrumentAgent.test_poll
# bin/nosetests -s -v ion/agents/instrument/refactor/test_instrument_agent.py:TestInstrumentAgent.test_autosample
# bin/nosetests -s -v ion/agents/instrument/refactor/test_instrument_agent.py:TestInstrumentAgent.test_capabilities


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
IA_MOD = 'ion.agents.instrument.refactor.instrument_agent'
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

CMDS = [
    'acquire_sample',
    'calibrate',
    'direct_access',
    'start_autosample',
    'start_direct_access',
    'stop_autosample',
    'stop_direct_access',
    'test'    
]

AGT_CMDS = [
    'clear',
    'end_transaction',
    'get_current_state',
    'go_active',
    'go_direct_access',
    'go_inactive',
    'go_layer_ping',
    'go_observatory',
    'go_streaming',
    'helo_agent',
    'helo_driver',
    'initialize',
    'pause',
    'power_down',
    'power_up',
    'reset',
    'resume',
    'run',
    'start_transaction'
]

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
    
    ###############################################################################
    # Setup, teardown, helpers.
    ###############################################################################
        
    def setUp(self):
        """
        Initialize test members.
        Start port agent.
        Start container and client.
        Start streams and subscribers.
        Start agent, client.
        """
                
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
        self._start_container()
        
        # Bring up services in a deploy file (no need to message)
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Start data suscribers, add stop to cleanup.
        # Define stream_config.
        self._no_samples = None
        self._async_data_result = AsyncResult()
        self._data_greenlets = []
        self._stream_config = {}
        self._samples_received = []
        self._data_subscribers = []
        self._start_data_subscribers()
        self.addCleanup(self._stop_data_subscribers)

        # Start event subscribers, add stop to cleanup.
        self._no_events = None
        self._async_event_result = AsyncResult()
        self._events_received = []
        self._event_subscribers = []
        self._start_event_subscribers()
        self.addCleanup(self._stop_event_subscribers)
                
        # Create agent config.
        agent_config = {
            'driver_config' : DVR_CONFIG,
            'stream_config' : self._stream_config,
            'agent'         : {'resource_id': IA_RESOURCE_ID},
            'test_mode' : True
        }
        
        # Start instrument agent.
        self._ia_pid = None
        log.debug("TestInstrumentAgent.setup(): starting IA.")
        container_client = ContainerAgentClient(node=self.container.node,
                                                name=self.container.name)
        self._ia_pid = container_client.spawn_process(name=IA_NAME,
                                                      module=IA_MOD, 
                                                      cls=IA_CLS, 
                                                      config=agent_config)      
        log.info('Agent pid=%s.', str(self._ia_pid))
        
        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = None
        self._ia_client = ResourceAgentClient(IA_RESOURCE_ID, process=FakeProcess())
        log.info('Got ia client %s.', str(self._ia_client))        
        
    def _start_pagent(self):
        """
        Construct and start the port agent.
        """
        port = self._support.start_pagent()
        
        # Configure driver to use port agent port number.
        DVR_CONFIG['comms_config'] = {
            'addr' : 'localhost',
            'port' : port
        }
        
    def _start_data_subscribers(self):
        """
        """
        # Create a pubsub client to create streams.
        pubsub_client = PubsubManagementServiceClient(node=self.container.node)

        # A callback for processing subscribed-to data.
        def consume_data(message, headers):
            log.info('Subscriber received data message: %s.', str(message))
            self._samples_received.append(message)
            if self._no_samples and self._no_samples == len(self._samples_received):
                self._async_data_result.set()
                
        # Create a stream subscriber registrar to create subscribers.
        subscriber_registrar = StreamSubscriberRegistrar(process=self.container,
                                                container=self.container)

        # Create streams and subscriptions for each stream named in driver.
        self._stream_config = {}
        self._data_subscribers = []
        for (stream_name, val) in PACKET_CONFIG.iteritems():
            stream_def = ctd_stream_definition(stream_id=None)
            stream_def_id = pubsub_client.create_stream_definition(
                                                    container=stream_def)        
            stream_id = pubsub_client.create_stream(
                        name=stream_name,
                        stream_definition_id=stream_def_id,
                        original=True,
                        encoding='ION R2')
            self._stream_config[stream_name] = stream_id
            
            # Create subscriptions for each stream.
            exchange_name = '%s_queue' % stream_name
            sub = subscriber_registrar.create_subscriber(exchange_name=exchange_name,
                                                         callback=consume_data)
            self._listen(sub)
            self._data_subscribers.append(sub)
            query = StreamQuery(stream_ids=[stream_id])
            sub_id = pubsub_client.create_subscription(\
                                query=query, exchange_name=exchange_name)
            pubsub_client.activate_subscription(sub_id)
            
    def _listen(self, sub):
        """
        Pass in a subscriber here, this will make it listen in a background greenlet.
        """
        gl = spawn(sub.listen)
        self._data_greenlets.append(gl)
        sub._ready_event.wait(timeout=5)
        return gl
                                 
    def _stop_data_subscribers(self):
        """
        Stop the data subscribers on cleanup.
        """
        for sub in self._data_subscribers:
            sub.stop()
        for gl in self._data_greenlets:
            gl.kill()
            
    def _start_event_subscribers(self):
        """
        Create subscribers for agent and driver events.
        """
        def consume_event(*args, **kwargs):
            log.info('Test recieved ION event: args=%s, kwargs=%s, event=%s.', 
                     str(args), str(kwargs), str(args[0]))
            self._events_received.append(args[0])
            if self._no_events and self._no_events == len(self._event_received):
                self._async_event_result.set()
                
        event_sub = EventSubscriber(event_type="DeviceEvent", callback=consume_event)
        event_sub.start()
        self._event_subscribers.append(event_sub)
        
    def _stop_event_subscribers(self):
        """
        Stop event subscribers on cleanup.
        """
        for sub in self._event_subscribers:
            sub.stop()
        
    def assertSampleDict(self, val):
        """
        Verify the value is a sample dictionary for the sbe37.
        """
        #{'p': [-6.945], 'c': [0.08707], 't': [20.002], 'time': [1333752198.450622]}        
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
        
        state = self._ia_client.get_agent_state()
        log.info('Agent in state: %s', state)

        
        retval = self._ia_client.get_agent()
        for item in retval:
            print str(item)
        #print str(retval)
        #cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        #retval = self._ia_client.execute_agent(cmd)
        
        
        """
        state = self._ia_client.get_agent_state()
        log.info('Agent in state: %s', state)

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)

        state = self._ia_client.get_agent_state()
        log.info('Agent in state: %s', state)
        """
        
    def test_states(self):
        """
        Test agent state transitions.
        """
        pass

    def test_get_set(self):
        """
        Test instrument driver get and set interface.
        """
        pass

    def test_poll(self):
        """
        Test observatory polling function.
        """
        pass        
        
    def test_autosample(self):
        """
        Test instrument driver execute interface to start and stop streaming
        mode.
        """
        pass

    def test_capabilities(self):
        """
        Test the ability to retrieve agent and resource parameter and command
        capabilities.
        """
        pass
    
    @unittest.skip('Never written')
    def test_errors(self):
        """
        Test illegal behavior and replies.
        """
        pass

        
    @unittest.skip('Direct access test to be finished by adding the telnet client, manual for now.')
    def test_direct_access(self):
        """
        Test agent direct_access command. This causes creation of
        driver process and transition to direct access.
        """
        pass


