#!/usr/bin/env python

"""
@package ion.agents.instrument.agent_cmd_util
@file ion/agents.instrument/agent_cmd_util.py
@author Edward Hunter
@brief Command line utilities to test agent function interactively.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

"""
from ion.agents.instrument.agent_cmd_util import *
import ion.agents.instrument.agent_cmd_util as util

"""

from pyon.util.context import LocalContextMixin
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand
from pyon.agent.agent import ResourceAgentEvent

from ion.agents.instrument.driver_process import DriverProcessType
from ion.agents.instrument.driver_process import ZMQEggDriverProcess
from ion.agents.instrument.driver_int_test_support import DriverIntegrationTestSupport

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''

ACQUIRE_SAMPLE = 'DRIVER_EVENT_ACQUIRE_SAMPLE'
START_AUTOSAMPLE = 'DRIVER_EVENT_START_AUTOSAMPLE'
STOP_AUTOSAMPLE = 'DRIVER_EVENT_STOP_AUTOSAMPLE'

# Module globals.
CFG = None
cc = None
pagent = None
ia_client = None

# Work dir and logger delimiter.
WORK_DIR = '/tmp/'
DELIM = ['<<','>>']

# Agent default parameters.
IA_RESOURCE_ID = '123xyz'
IA_NAME = 'Agent007'
IA_MOD = 'ion.agents.instrument.instrument_agent'
IA_CLS = 'InstrumentAgent'

# Default driver config.
# DVR_CONFIG['comms_config']['port'] is set by the setup.
DVR_CONFIG = {
    'dvr_egg' : 'http://sddevrepo.oceanobservatories.org/releases/seabird_sbe37smb_ooicore-0.0.4-py2.7.egg',
    'dvr_mod' : 'mi.instrument.seabird.sbe37smb.ooicore.driver',
    'dvr_cls' : 'SBE37Driver',
    'workdir' : WORK_DIR,
    'process_type' : (DriverProcessType.EGG,)
}

# Default streams.
STREAMS = {
            'parsed' : 'ctd_parsed_param_dict',
            'raw'    : 'ctd_raw_param_dict'
        }

def init(_cfg, _cc):
    """
    """
    global CFG
    global cc
    CFG = _cfg
    cc = _cc
    # Start up all the deploy services.
    cc.start_rel_from_url('res/deploy/r2deploy.yml')

def start_port_agent(dev_addr=None,
                     dev_port=None,
                     data_port=None,
                     cmd_port=None,
                     pa_binary=None,
                     work_dir=WORK_DIR,
                     delim=DELIM
                     ):
    """
    """
        
    global pagent
    global CFG
    pagent = DriverIntegrationTestSupport(        
        None,
        None,
        dev_addr or CFG.device.sbe37.host,
        dev_port or CFG.device.sbe37.port,
        data_port or CFG.device.sbe37.port_agent_data_port,
        cmd_port or CFG.device.sbe37.port_agent_cmd_port,
        pa_binary or CFG.device.sbe37.port_agent_binary,
        delim,
        work_dir)
    
    pagent.start_pagent()
    
def stop_port_agent():
    """
    """
    global pagent
    if pagent:
        pagent.stop_pagent()
        pagent = None
    
def build_stream_config(streams):
    """
    """
    # Create a pubsub client to create streams.
    pubsub_client = PubsubManagementServiceClient(node=cc.node)
    dataset_management = DatasetManagementServiceClient() 
    
    # Create streams and subscriptions for each stream named in driver.
    agent_stream_config = {}

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
        agent_stream_config[stream_name] = stream_config

    return agent_stream_config

def start_agent(
    dvr_config=DVR_CONFIG,
    streams=STREAMS,
    resource_id=IA_RESOURCE_ID,
    ia_name=IA_NAME,
    ia_mod=IA_MOD,
    ia_cls=IA_CLS,
    test_mode=True):
    """
    """
    
    # Build the stream config.
    agent_stream_config = build_stream_config(streams)
    
    # Build full agent config.
    agent_config = {
        'driver_config' : dvr_config,
        'stream_config' : streams,
        'agent'         : {'resource_id': resource_id},
        'test_mode'     : test_mode
    }

    #if org_name is not None:
    #    agent_config['org_name'] = org_name

    # Launch agent.
    ia_pid = cc.spawn_process(name=ia_name,
        module=ia_mod,
        cls=ia_cls,
        config=agent_config)

    print 'agent process id: ' + str(ia_pid)

    # Start a resource agent client to talk with the instrument agent.
    global ia_client
    ia_client = ResourceAgentClient(resource_id, process=FakeProcess())
    print 'ia client: ' + str(ia_client)

def stop_agent():
    """
    """
    pass

def get_agent_state():
    if ia_client:
        return ia_client.get_agent_state()
        
def get_resource_state():
    if ia_client:
        return ia_client.get_resource_state()
        
def ping_agent():
    if ia_client:
        return ia_client.ping_agent()
        
def initialize():
    if ia_client:
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        return ia_client.execute_agent(cmd)
         
def go_active():
    if ia_client:
        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        return ia_client.execute_agent(cmd)

def run():
    if ia_client:
        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        return ia_client.execute_agent(cmd)

def go_inactive():
    if ia_client:
        cmd = AgentCommand(command=ResourceAgentEvent.GO_INACTIVE)
        return ia_client.execute_agent(cmd)

def reset():
    if ia_client:
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        return ia_client.execute_agent(cmd)

def sample():
    if ia_client:
        cmd = AgentCommand(command=ACQUIRE_SAMPLE)
        retval = self._ia_client.execute_resource(cmd)

def start_autosample():
    if ia_client:
        cmd = AgentCommand(command=START_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)
    
def stop_autosample():
    if ia_client:
        cmd = AgentCommand(command=STOP_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)    

def get_capabilities():
    if ia_client:
        return ia_client.get_capabilities()