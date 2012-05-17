#!/usr/bin/env python

"""
@package ion.services.mi.test.test_instrument_agent
@file ion/services/mi/test_instrument_agent.py
@author Edward Hunter
@brief Test cases for R2 instrument agent.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

from pyon.public import log
from nose.plugins.attrib import attr

from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest
from pyon.core.object import IonObjectSerializer

from interface.objects import StreamQuery
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from pyon.public import StreamSubscriberRegistrar
from prototype.sci_data.stream_defs import ctd_stream_definition
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
"""
from ion.services.mi.drivers.sbe37.sbe37_driver import SBE37Channel
from ion.services.mi.drivers.sbe37.sbe37_driver import SBE37Parameter
from ion.services.mi.drivers.sbe37.sbe37_driver import PACKET_CONFIG
"""
from pyon.public import CFG
from mock import patch

import time
import unittest
import simplejson, urllib
from ion.services.coi.service_gateway_service import GATEWAY_RESPONSE, GATEWAY_ERROR, GATEWAY_ERROR_MESSAGE

# bin/nosetests -s -v ion/services/mi/test/test_instrument_agent.py:TestInstrumentAgent.test_initialize
# bin/nosetests -s -v ion/services/mi/test/test_instrument_agent.py:TestInstrumentAgent.test_go_active
# bin/nosetests -s -v ion/services/mi/test/test_instrument_agent.py:TestInstrumentAgent.test_get_set
# bin/nosetests -s -v ion/services/mi/test/test_instrument_agent.py:TestInstrumentAgent.test_poll
# bin/nosetests -s -v ion/services/mi/test/test_instrument_agent.py:TestInstrumentAgent.test_autosample

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''
    
#@unittest.skip('Do not run hardware test.')
@unittest.skip('Need to align.')
@attr('HARDWARE', group='sa')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class TestInstrumentAgent(IonIntegrationTestCase):
    """
    Test cases for instrument agent class. Functions in this class provide
    instrument agent integration tests and provide a tutorial on use of
    the agent setup and interface.
    """
 
    def customCleanUp(self):
        log.info('CUSTOM CLEAN UP ******************************************************************************')

    def setUp(self):
        """
        Setup the test environment to exersice use of instrumet agent, including:
        * define driver_config parameters.
        * create container with required services and container client.
        * create publication stream ids for each driver data stream.
        * create stream_config parameters.
        * create and activate subscriptions for agent data streams.
        * spawn instrument agent process and create agent client.
        * add cleanup functions to cause subscribers to get stopped.
        """


 #       params = { ('CTD', 'TA2'): -1.9434316e-05,
 #       ('CTD', 'PTCA1'): 1.3206866,
 #       ('CTD', 'TCALDATE'): [8, 11, 2006] }

 #       for tup in params:
 #           print tup




        self.addCleanup(self.customCleanUp)
        # Names of agent data streams to be configured.
        parsed_stream_name = 'ctd_parsed'        
        raw_stream_name = 'ctd_raw'        

        # Driver configuration.
        #Simulator

        self.driver_config = {
            'svr_addr': 'localhost',
            'cmd_port': 5556,
            'evt_port': 5557,
            'dvr_mod': 'ion.services.mi.drivers.sbe37.sbe37_driver',
            'dvr_cls': 'SBE37Driver',
            'comms_config': {
                SBE37Channel.CTD: {
                    'method':'ethernet',
                    'device_addr': CFG.device.sbe37.host,
                    'device_port': CFG.device.sbe37.port,
                    'server_addr': 'localhost',
                    'server_port': 8888
                }                
            }
        }

        #Hardware

        '''
        self.driver_config = {
            'svr_addr': 'localhost',
            'cmd_port': 5556,
            'evt_port': 5557,
            'dvr_mod': 'ion.services.mi.drivers.sbe37.sbe37_driver',
            'dvr_cls': 'SBE37Driver',
            'comms_config': {
                SBE37Channel.CTD: {
                    'method':'ethernet',
                    'device_addr': '137.110.112.119',
                    'device_port': 4001,
                    'server_addr': 'localhost',
                    'server_port': 8888
                }
            }
        }
        '''

        # Start container.
        self._start_container()

        # Establish endpoint with container (used in tests below)
        self._container_client = ContainerAgentClient(node=self.container.node,
                                                      name=self.container.name)
        
        # Bring up services in a deploy file (no need to message)
        self.container.start_rel_from_url('res/deploy/r2dm.yml')

        # Create a pubsub client to create streams.
        self._pubsub_client = PubsubManagementServiceClient(
                                                    node=self.container.node)

        # A callback for processing subscribed-to data.
        def consume(message, headers):
            log.info('Subscriber received message: %s', str(message))

        # Create a stream subscriber registrar to create subscribers.
        subscriber_registrar = StreamSubscriberRegistrar(process=self.container,
                                                node=self.container.node)

        self.subs = []

        # Create streams for each stream named in driver.
        self.stream_config = {}
        for (stream_name, val) in PACKET_CONFIG.iteritems():
            stream_def = ctd_stream_definition(stream_id=None)
            stream_def_id = self._pubsub_client.create_stream_definition(
                                                    container=stream_def)        
            stream_id = self._pubsub_client.create_stream(
                        name=stream_name,
                        stream_definition_id=stream_def_id,
                        original=True,
                        encoding='ION R2')
            self.stream_config[stream_name] = stream_id
            
            # Create subscriptions for each stream.
            exchange_name = '%s_queue' % stream_name
            sub = subscriber_registrar.create_subscriber(exchange_name=exchange_name, callback=consume)
            sub.start()
            query = StreamQuery(stream_ids=[stream_id])
            sub_id = self._pubsub_client.create_subscription(\
                                query=query, exchange_name=exchange_name)
            self._pubsub_client.activate_subscription(sub_id)
            self.subs.append(sub)
            
        # Add cleanup function to stop subscribers.        
        def stop_subscriber(sub_list):
            for sub in sub_list:
                sub.stop()            
        self.addCleanup(stop_subscriber, self.subs)            
            

        # Create agent config.

        self.agent_resource_id = '123xyz'

        self.agent_config = {
            'driver_config' : self.driver_config,
            'stream_config' : self.stream_config,
            'agent'         : {'resource_id': self.agent_resource_id}
        }

        # Launch an instrument agent process.
        self._ia_name = 'agent007'
        self._ia_mod = 'ion.services.mi.instrument_agent'
        self._ia_class = 'InstrumentAgent'
        self._ia_pid = self._container_client.spawn_process(name=self._ia_name,
                                       module=self._ia_mod, cls=self._ia_class,
                                       config=self.agent_config)


        log.info('got pid=%s', str(self._ia_pid))


        self._ia_client = None
        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = ResourceAgentClient(self.agent_resource_id, process=FakeProcess())
        log.info('got ia client %s', str(self._ia_client))



    def test_initialize(self):
        """
        Test agent initialize command. This causes creation of
        driver process and transition to inactive.
        """


        cmd = AgentCommand(command='initialize')
        reply = gw_agent_execute_agent(self.agent_resource_id, cmd)
        log.info(reply)

        time.sleep(2)

        caps = gw_agent_get_capabilities(self.agent_resource_id)
        log.info('Capabilities: %s',str(caps))

        time.sleep(2)

        cmd = AgentCommand(command='reset')
        reply = gw_agent_execute_agent(self.agent_resource_id, cmd)
        log.info(reply)


    def test_direct_access(self):
        """
        Test agent direct_access command. This causes creation of
        driver process and transition to direct access.
        """
        print("test initing")
        cmd = AgentCommand(command='initialize')
        reply = gw_agent_execute_agent(self.agent_resource_id, cmd)
        time.sleep(2)

        print("test go_active")
        cmd = AgentCommand(command='go_active')
        reply = gw_agent_execute_agent(self.agent_resource_id, cmd)
        time.sleep(2)

        print("test run")
        cmd = AgentCommand(command='run')
        reply = gw_agent_execute_agent(self.agent_resource_id, cmd)
        time.sleep(2)

        print("test go_da")
        cmd = AgentCommand(command='go_direct_access')
        reply = gw_agent_execute_agent(self.agent_resource_id, cmd)
        print("reply=" + str(reply))
        time.sleep(2)

        print("test go_ob")
        cmd = AgentCommand(command='go_observatory')
        reply = gw_agent_execute_agent(self.agent_resource_id, cmd)
        time.sleep(2)

        print("test go_inactive")
        cmd = AgentCommand(command='go_inactive')
        reply = gw_agent_execute_agent(self.agent_resource_id, cmd)
        time.sleep(2)

        print("test reset")
        cmd = AgentCommand(command='reset')
        reply = gw_agent_execute_agent(self.agent_resource_id, cmd)
        time.sleep(2)

    def test_go_active(self):
        """
        Test agent go_active command. This causes a driver process to
        launch a connection broker, connect to device hardware, determine
        entry state of driver and initialize driver parameters.
        """
        cmd = AgentCommand(command='initialize')
        reply = gw_agent_execute_agent(self.agent_resource_id, cmd)
        time.sleep(2)

        cmd = AgentCommand(command='go_active')
        reply = gw_agent_execute_agent(self.agent_resource_id, cmd)
        time.sleep(2)

        cmd = AgentCommand(command='go_inactive')
        reply = gw_agent_execute_agent(self.agent_resource_id, cmd)
        time.sleep(2)

        cmd = AgentCommand(command='reset')
        reply = gw_agent_execute_agent(self.agent_resource_id, cmd)
        time.sleep(2)



def agent_gateway_request(uri, payload):

    server_hostname = 'localhost'
    server_port = 5000
    web_server_cfg = None
    try:
        web_server_cfg = CFG['container']['service_gateway']['web_server']
    except Exception, e:
        web_server_cfg = None

    if web_server_cfg is not None:
        if 'hostname' in web_server_cfg:
            server_hostname = web_server_cfg['hostname']
        if 'port' in web_server_cfg:
            server_port = web_server_cfg['port']



    SEARCH_BASE = 'http://' + server_hostname + ':' + str(server_port) + '/ion-agent/' + uri


    args = {}
    args.update({
        #'format': "unix",
        #'output': 'json'
    })
    url = SEARCH_BASE + '?' + urllib.urlencode(args)
    log.debug(url)
    log.debug(payload)

    result = simplejson.load(urllib.urlopen(url, 'payload=' + str(payload ) ))
    if not result.has_key('data'):
        log.error('Not a correct JSON response: %s' & result)

    return result

def process_gateway_request(resource_id, operation, json_request, requester):

    if requester is not None:
        agent_execute_request["agentRequest"]["requester"] = requester

    payload = simplejson.dumps(json_request)

    response = agent_gateway_request(resource_id + '/' + operation,   payload)


    if response['data'].has_key(GATEWAY_ERROR):
        log.error(response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])
        raise BadRequest(response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])

    if "type_" in response['data'][GATEWAY_RESPONSE]:
        del response['data'][GATEWAY_RESPONSE]["type_"]

    return response['data'][GATEWAY_RESPONSE]


def gw_agent_execute_agent(resource_id, cmd, requester=None):


    agent_cmd_params = IonObjectSerializer().serialize(cmd)

    agent_execute_request = {  "agentRequest": {
        "agentId": resource_id,
        "agentOp": "execute_agent",
        "expiry": 0,
        "params": {
            "command": agent_cmd_params
        }
        }
    }

    ret_values = process_gateway_request(resource_id, "execute_agent", agent_execute_request, requester)

    ret_obj = IonObject('AgentCommandResult',ret_values)
    return ret_obj

def gw_agent_get_capabilities(resource_id,  requester=None):


    agent_get_capabilities_request = {  "agentRequest": {
        "agentId": resource_id,
        "agentOp": "get_capabilities",
        "expiry": 0,
        "params": { }
    }
    }

    return process_gateway_request(resource_id, "get_capabilities", agent_get_capabilities_request, requester)


#TODO - The functions below must be able to handle sending tuples ad dict keys and also enhance the gateway to handle it, since JSON does not allow it.

def gw_agent_get_param(resource_id, params,  requester=None):

    agent_get_param_request = {  "agentRequest": {
        "agentId": resource_id,
        "agentOp": "get_param",
        "expiry": 0,
        "params": {
            "name" : params
        }
    }
    }

    return NotImplemented()

    #return process_gateway_request(resource_id, "get_param", agent_get_param_request, requester)

def gw_agent_set_param(resource_id,  params, requester=None):

    agent_set_param_request = {  "agentRequest": {
        "agentId": resource_id,
        "agentOp": "set_param",
        "expiry": 0,
        "params": {
            "name" : []
        }
    }
    }

    return NotImplemented()

    #return process_gateway_request(resource_id, "set_param", agent_set_param_request, requester)


