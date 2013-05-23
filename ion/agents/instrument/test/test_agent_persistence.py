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
from pyon.public import get_obj_registry

# Standard imports.
import sys
import time
import socket
import re
import json
import unittest
import os
from copy import deepcopy

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


from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessDefinition, ProcessSchedule, ProcessTarget,\
    ProcessStateEnum, ProcessQueueingMode, ProcessRestartMode, ProcessDefinitionType

from pyon.core.object import IonObjectSerializer, IonObjectDeserializer
from pyon.core.bootstrap import IonObject

"""
--with-pycc
--with-queueblame
bin/nosetests -s -v --nologcapture --with-queueblame --with-pycc ion/agents/instrument/test/test_agent_persistence.py:TestAgentPersistence
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_agent_persistence.py:TestAgentPersistence
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_agent_persistence.py:TestAgentPersistence.test_agent_config_persistence
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_agent_persistence.py:TestAgentPersistence.test_agent_state_persistence
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_agent_persistence.py:TestAgentPersistence.test_agent_rparam_persistence
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_agent_persistence.py:TestAgentPersistence.test_xx
bin/nosetests -s -v --nologcapture --with-queueblame --with-pycc ion/agents/instrument/test/test_agent_persistence.py:TestAgentPersistence.test_xx
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

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''

@attr('HARDWARE', group='mi')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 360}}})
class TestAgentPersistence(IonIntegrationTestCase):
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

        log.info('building stream configuration')
        # Setup stream config.
        self._build_stream_config()

        # Create agent config.
        self._agent_config = {
            'driver_config' : DVR_CONFIG,
            'stream_config' : self._stream_config,
            'agent'         : {'resource_id': IA_RESOURCE_ID},
            'test_mode' : True,
            'forget_past' : False,
            'enable_persistence' : True
        }

        self._ia_client = None
        self._ia_pid = '1234'
        
        self.addCleanup(self._verify_agent_reset)
        self.addCleanup(self.container.state_repository.put_state,
                        self._ia_pid, {})

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
        stream_config = dict(routing_key=stream_route.routing_key,
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
        stream_config = dict(routing_key=stream_route.routing_key,
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
            config=self._agent_config,
            process_id=self._ia_pid)            
            
        
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

    #@unittest.skip('Fails on buildbot, reason unknown.')
    def test_agent_config_persistence(self):
        """
        test_agent_config_persistence
        Test that agent parameter configuration is persisted between running
        instances.
        """
        
        # Start the agent.
        self._start_agent()

        # We start in uninitialized state.
        # In this state there is no driver process.
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
        
        # Ping the agent.
        retval = self._ia_client.ping_agent()
        log.info(retval)

        # Confirm the default agent parameters.
        #{'streams': {'raw': ['quality_flag', 'ingestion_timestamp', 'port_timestamp', 'raw', 'lat', 'driver_timestamp', 'preferred_timestamp', 'lon', 'internal_timestamp', 'time'], 'parsed': ['quality_flag', 'ingestion_timestamp', 'port_timestamp', 'pressure', 'lat', 'driver_timestamp', 'conductivity', 'preferred_timestamp', 'temp', 'density', 'salinity', 'lon', 'internal_timestamp', 'time']}}
        retval = self._ia_client.get_agent(['streams'])['streams']
        self.assertIn('raw', retval.keys())
        self.assertIn('parsed', retval.keys())

        #{'pubrate': {'raw': 0, 'parsed': 0}}
        retval = self._ia_client.get_agent(['pubrate'])['pubrate']
        self.assertIn('raw', retval.keys())
        self.assertIn('parsed', retval.keys())
        self.assertEqual(retval['raw'], 0)
        self.assertEqual(retval['parsed'], 0)
        
        #{'alerts': []}
        retval = self._ia_client.get_agent(['alerts'])['alerts']
        self.assertEqual(retval, [])

        # Define a few new parameters and set them.
        # Confirm they are set.
        alert_def_1 = {
            'name' : 'current_warning_interval',
            'stream_name' : 'parsed',
            'description' : 'Current is below normal range.',
            'alert_type' : StreamAlertType.WARNING,
            'aggregate_type' : AggregateStatusType.AGGREGATE_DATA,
            'value_id' : 'temp',
            'lower_bound' : None,
            'lower_rel_op' : None,
            'upper_bound' : 10.0,
            'upper_rel_op' : '<',
            'alert_class' : 'IntervalAlert'
        }

        alert_def_2 = {
            'name' : 'temp_alarm_interval',
            'stream_name' : 'parsed',
            'description' : 'Temperatoure is critical.',
            'alert_type' : StreamAlertType.ALARM,
            'aggregate_type' : AggregateStatusType.AGGREGATE_DATA,
            'value_id' : 'temp',
            'lower_bound' : None,
            'lower_rel_op' : None,
            'upper_bound' : 20.0,
            'upper_rel_op' : '<',
            'alert_class' : 'IntervalAlert'
        }

        alert_def3 = {
            'name' : 'late_data_warning',
            'stream_name' : 'parsed',
            'description' : 'Expected data has not arrived.',
            'alert_type' : StreamAlertType.WARNING,
            'aggregate_type' : AggregateStatusType.AGGREGATE_COMMS,
            'time_delta' : 180,
            'alert_class' : 'LateDataAlert'
        }

        orig_alerts = [alert_def_1,alert_def_2, alert_def3]
        pubrate = {
            'parsed' : 10,
            'raw' : 20
        }
        params = {
            'alerts' : orig_alerts,
            'pubrate' : pubrate
        }
        
        # Set the new agent params and confirm.
        self._ia_client.set_agent(params)
        
        params = [
            'alerts',
            'pubrate'
        ]
        retval = self._ia_client.get_agent(params)
        pubrate = retval['pubrate']
        alerts = retval['alerts']
        self.assertIn('raw', pubrate.keys())
        self.assertIn('parsed', pubrate.keys())
        self.assertEqual(pubrate['parsed'], 10)
        self.assertEqual(pubrate['raw'], 20)
        count = 0
        for x in alerts:
            x.pop('status')
            x.pop('value')
            for y in orig_alerts:
                if x['name'] == y['name']:
                    count += 1
                    self.assertItemsEqual(x.keys(), y.keys())
        self.assertEqual(count, 3)

        # Now stop and restart the agent.
        self._stop_agent()
        gevent.sleep(15)
        self._start_agent()

        # We start in uninitialized state.
        # In this state there is no driver process.
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
        
        # Ping the agent.
        retval = self._ia_client.ping_agent()
        log.info(retval)

        # Confirm the persisted parameters.
        params = [
            'alerts',
            'pubrate'
        ]
        retval = self._ia_client.get_agent(params)
        
        pubrate = retval['pubrate']
        alerts = retval['alerts']
        self.assertIn('raw', pubrate.keys())
        self.assertIn('parsed', pubrate.keys())
        self.assertEqual(pubrate['parsed'], 10)
        self.assertEqual(pubrate['raw'], 20)
        count = 0
        for x in alerts:
            x.pop('status')
            x.pop('value')
            for y in orig_alerts:
                if x['name'] == y['name']:
                    count += 1
                    self.assertItemsEqual(x.keys(), y.keys())
        self.assertEqual(count, 3)
       
    #@unittest.skip('Fails on buildbot, reason unknown.')
    def test_agent_state_persistence(self):
        """
        test_agent_state_persistence
        Verify that agents can be restored to their prior running state.
        """

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

        # Acquire sample returns a string, not a particle.  The particle
        # is created by the data handler though.
        cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)
        retval = self._ia_client.execute_resource(cmd)

        # Now stop and restart the agent.
        self._stop_agent()
        gevent.sleep(15)
        self._start_agent()

        timeout = gevent.Timeout(240)
        timeout.start()
        try:
            while True:                
                state = self._ia_client.get_agent_state()
                if state == ResourceAgentState.COMMAND:
                    timeout.cancel()
                    break
                else:
                    gevent.sleep(1)
        except gevent.Timeout:
            fail("Could not restore agent state to COMMAND.")

        cmd = AgentCommand(command=ResourceAgentEvent.PAUSE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.STOPPED)

        # Now stop and restart the agent.
        self._stop_agent()
        gevent.sleep(15)
        self._start_agent()

        timeout = gevent.Timeout(240)
        timeout.start()
        try:
            while True:                
                state = self._ia_client.get_agent_state()
                if state == ResourceAgentState.STOPPED:
                    timeout.cancel()
                    break
                else:
                    gevent.sleep(1)
        except gevent.Timeout:
            fail("Could not restore agent state to STOPPED.")

        # Reset the agent. This causes the driver messaging to be stopped,
        # the driver process to end and switches us back to uninitialized.
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)


    #@unittest.skip('')
    def test_agent_rparam_persistence(self):
        """
        test_agent_rparam_persistence
        Verify ability to restore device configuration.
        ### Original values:
        {'TA0': -0.0002572242, 'OUTPUTSV': False, 'NAVG': 0}
        
        ### Values after set:
        {'TA0': -0.0005144484, 'OUTPUTSV': True, 'NAVG': 1}

        ### Restore config:
        {'PTCA1': 0.6603433, 'WBOTC': 1.2024e-05, 'PCALDATE': [12, 8, 2005],
        'STORETIME': False, 'CPCOR': 9.57e-08, 'PTCA2': 0.00575649,
        'OUTPUTSV': True, 'SAMPLENUM': 0, 'TCALDATE': [8, 11, 2005],
        'OUTPUTSAL': False, 'TA2': -9.717158e-06, 'POFFSET': 0.0,
        'INTERVAL': 19733, 'SYNCWAIT': 0, 'CJ': 3.339261e-05,
        'CI': 0.0001334915, 'CH': 0.1417895, 'TA0': -0.0005144484,
        'TA1': 0.0003138936, 'NAVG': 1, 'TA3': 2.138735e-07, '
        RCALDATE': [8, 11, 2005], 'CG': -0.987093, 'CTCOR': 3.25e-06, '
        PTCB0': 24.6145, 'PTCB1': -0.0009, 'PTCB2': 0.0,
        'CCALDATE': [8, 11, 2005], 'PA0': 5.916199, 'PA1': 0.4851819,
        'PA2': 4.596432e-07, 'SYNCMODE': False, 'PTCA0': 276.2492,
        'TXREALTIME': True, 'RTCA2': -3.022745e-08, 'RTCA1': 1.686132e-06,
        'RTCA0': 0.9999862}
        
        ### Of which we have:
        {'TA0': -0.0005144484, 'OUTPUTSV': True, 'NAVG': 1}        
        """

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

        params = [
            SBE37Parameter.OUTPUTSV,
            SBE37Parameter.NAVG,
            SBE37Parameter.TA0
        ]
        retval = self._ia_client.get_resource(params)
        orig_params = retval

        new_params = {
            SBE37Parameter.OUTPUTSV : not orig_params[SBE37Parameter.OUTPUTSV],
            SBE37Parameter.NAVG : orig_params[SBE37Parameter.NAVG] + 1,
            SBE37Parameter.TA0 : orig_params[SBE37Parameter.TA0] * 2
        }

        self._ia_client.set_resource(new_params)
        retval = self._ia_client.get_resource(params)
        
        self.assertEqual(retval[SBE37Parameter.OUTPUTSV],
                         new_params[SBE37Parameter.OUTPUTSV])
        self.assertEqual(retval[SBE37Parameter.NAVG],
                         new_params[SBE37Parameter.NAVG])
        delta = max(retval[SBE37Parameter.TA0],
                    new_params[SBE37Parameter.TA0])*.01
        self.assertAlmostEqual(retval[SBE37Parameter.TA0],
                               new_params[SBE37Parameter.TA0], delta=delta)

        # Now stop and restart the agent.
        self._stop_agent()
        gevent.sleep(15)
        self._start_agent()

        timeout = gevent.Timeout(240)
        timeout.start()
        try:
            while True:                
                state = self._ia_client.get_agent_state()
                if state == ResourceAgentState.COMMAND:
                    timeout.cancel()
                    break
                else:
                    gevent.sleep(3)
        except gevent.Timeout:
            fail("Could not restore agent state to COMMAND.")
        
        # Verify the parameters have been restored as needed.
        retval = self._ia_client.get_resource(params)

        self._ia_client.set_resource(new_params)
        retval = self._ia_client.get_resource(params)
        
        self.assertEqual(retval[SBE37Parameter.OUTPUTSV],
                         new_params[SBE37Parameter.OUTPUTSV])
        self.assertEqual(retval[SBE37Parameter.NAVG],
                         new_params[SBE37Parameter.NAVG])
        delta = max(retval[SBE37Parameter.TA0],
                    new_params[SBE37Parameter.TA0])*.01
        self.assertAlmostEqual(retval[SBE37Parameter.TA0],
                               new_params[SBE37Parameter.TA0], delta=delta)

        # Reset the agent. This causes the driver messaging to be stopped,
        # the driver process to end and switches us back to uninitialized.
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
        
    
    @unittest.skip('Not ready yet.')
    def test_xx(self):
        
        pdc = ProcessDispatcherServiceClient(node=self.container.node)
        p_def = ProcessDefinition(name='Agent007')
        p_def.executable = {
            'module' : 'ion.agents.instrument.instrument_agent',
            'class' : 'InstrumentAgent'
        }
        p_def_id = pdc.create_process_definition(p_def)
        
        
        def event_callback(event, *args, **kwargs):
            print '######### proc %s in state %s' % (event.origin, ProcessStateEnum._str_map[event.state])
            
        pid = pdc.create_process(p_def_id)
        
        sub = EventSubscriber(event_type='ProcessLifecycleEvent',
                              callback=event_callback,
                              origin=pid,
                              origin_type='DispatchedProcess')
        
        sub.start()
        
        pdc.schedule_process(p_def_id, process_id=pid,
                             configuration=self._agent_config)
        
        gevent.sleep(5)
        
        sub.stop()
    
    
        