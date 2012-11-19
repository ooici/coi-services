#!/usr/bin/env python

"""
@package 
@file
@author
@brief
"""

__author__ = 'Tom O\'Reilly'
__license__ = 'Apache 2.0'

# Import pyon first for monkey patching.

# Pyon log and config objects.
from pyon.public import log
from pyon.public import CFG

# Standard imports.
import unittest

# 3rd party imports.
from nose.plugins.attrib import attr
from mock import patch

# Pyon unittest support.
from pyon.util.int_test import IonIntegrationTestCase

# Agent imports.
from pyon.util.context import LocalContextMixin
from pyon.agent.agent import ResourceAgentClient
from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent

# Driver imports.
from ion.agents.instrument.driver_int_test_support import DriverIntegrationTestSupport

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

# TODO chagne the path following the refactor.
# bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_puck.py:TestPuck
# bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_puck.py:TestPuck.test_xxx

###############################################################################
# Global constants.
###############################################################################

# Real and simulated devcies we test against.
DEV_ADDR = CFG.device.sbe37.host
DEV_PORT = CFG.device.sbe37.port
DATA_PORT = CFG.device.sbe37.port_agent_data_port
CMD_PORT = CFG.device.sbe37.port_agent_cmd_port
PA_BINARY = CFG.device.sbe37.port_agent_binary

# A seabird driver. These get set by the puck reader.
DRV_MOD = '' #'mi.instrument.seabird.sbe37smb.ooicore.driver'
DRV_CLS = '' #'SBE37Driver'

# Work dir and logger delimiter.
WORK_DIR = '/tmp/'
DELIM = ['<<','>>']
PROCESS_TYPE = ('ZMQPyClassDriverLauncher',)

# Agent parameters.
IA_RESOURCE_ID = '123xyz'
IA_NAME = 'Agent007'
IA_MOD = 'ion.agents.instrument.instrument_agent'
IA_CLS = 'InstrumentAgent'

class FakePuckReader():
    """
    """
    def read_puck(self, host, port):
        """
        """
        puck_data = {
            'dvr_mod' : 'mi.instrument.seabird.sbe37smb.ooicore.driver',
            'dvr_cls' : 'SBE37Driver',
            'serial_no' : '12345',
            'manufacturer' : 'Seabird Electronics',
            'model' : 'SBE37-SMP'
        }
        return puck_data

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''

@attr('HARDWARE', group='mi')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 120}}})
class TestPuck(IonIntegrationTestCase):
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
        self._support.start_pagent()
        self.addCleanup(self._support.stop_pagent)    
        
        # Start container.
        log.info('Staring capability container.')
        self._start_container()
        
        # Bring up services in a deploy file (no need to message)
        log.info('Staring deploy services.')
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        log.debug("Starting container client.")
        self.container_client = ContainerAgentClient(node=self.container.node,
            name=self.container.name)

        self._build_stream_config()
        
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
                     
    ###############################################################################
    # Tests.
    ###############################################################################

    def test_xxx(self, host='localhost', port=DATA_PORT,
                 resource_id=IA_RESOURCE_ID, stream_config=None):
        """
        """
        
        if not stream_config:
            stream_config = self._stream_config
            
        puck_reader = FakePuckReader()
        puck_data = puck_reader.read_puck(host, port)

        driver_config = {
            'dvr_mod' : puck_data['dvr_mod'],
            'dvr_cls' : puck_data['dvr_cls'],
            'workdir' : WORK_DIR,
            'process_type' : PROCESS_TYPE,
            'comms_config' : {
                'addr' : host,
                'port' : port
            }
        }
        
        agent_config = {
            'driver_config' : driver_config,
            'stream_config' : stream_config,
            'agent'         : {'resource_id': resource_id},
            'test_mode'     : True            
        }

        log.debug("Starting instrument agent.")
        ia_pid = self.container_client.spawn_process(
            name=IA_NAME,
            module=IA_MOD,
            cls=IA_CLS,
            config=agent_config)
        #self.addCleanup(self._verify_agent_reset)

        ia_client = ResourceAgentClient(resource_id, process=FakeProcess())
        log.info('Got ia client %s.', str(ia_client))
    
        # We start in uninitialized state.
        # In this state there is no driver process.
        state = ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
        
        # Ping the agent.
        retval = ia_client.ping_agent()
        log.info(retval)
    
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = ia_client.execute_agent(cmd)
        state = ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = ia_client.execute_agent(cmd)
        state = ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = ia_client.execute_agent(cmd)
        state = ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)
        retval = ia_client.execute_resource(cmd)
        
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = ia_client.execute_agent(cmd)
        state = ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
        

