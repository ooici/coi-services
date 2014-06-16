#!/usr/bin/env python

"""
@package ion.agents.instrument.test.test_vel3d
@file ion/agents.instrument/test_vel3d.py
@author Edward Hunter
@brief Test cases for vel3d instrument agent.
"""

__author__ = 'Edward Hunter'


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
import signal
import subprocess

# 3rd party imports.
import gevent
from gevent.event import AsyncResult
from nose.plugins.attrib import attr
from mock import patch
import numpy

# Pyon pubsub and event support.
from pyon.event.event import EventSubscriber, EventPublisher
from pyon.ion.stream import StandaloneStreamSubscriber
from ion.services.dm.utility.granule_utils import RecordDictionaryTool

# Pyon unittest support.
from pyon.util.int_test import IonIntegrationTestCase

# Pyon Object Serialization
from pyon.core.object import IonObjectSerializer

# Pyon exceptions.
from pyon.core.exception import BadRequest, Conflict, Timeout, ResourceError
from pyon.core.exception import IonException

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


"""
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_vel3d.py:TestVel3d
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_vel3d.py:TestVel3d.test_initialize
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_vel3d.py:TestVel3d.test_xx
"""

###############################################################################
# Global constants.
###############################################################################


# Work dir and logger delimiter.
WORK_DIR = '/tmp/'

from ion.agents.instrument.instrument_agent import InstrumentAgent
# Agent parameters.
IA_RESOURCE_ID = '123xyz'
IA_NAME = 'Agent007'
IA_MOD = 'ion.agents.instrument.instrument_agent'
IA_CLS = 'InstrumentAgent'

# Vel3d URI, Mod, Class.
DRV_URI = 'http://sddevrepo.oceanobservatories.org/releases/nobska_mavs4_ooicore-0.0.8-py2.7.egg'
DRV_MOD = 'mi.instrument.nobska.mavs4.ooicore.driver'
DRV_CLS = 'mavs4InstrumentDriver'

# Driver config.
# DVR_CONFIG['comms_config']['port'] is set by the setup.
DVR_CONFIG = {
    'dvr_egg' : DRV_URI,
    'dvr_mod' : DRV_MOD,
    'dvr_cls' : DRV_CLS,
    'workdir' : WORK_DIR,
    'process_type' : (DriverProcessType.EGG,),
    'comms_config' : {
                      'addr' : 'rsn-port-agent-test.oceanobservatories.org',
                      'port' : 13015,
                      'cmd_port' : 12015
    }
}

class TcpClient():
    '''
    TCP client for testing DA.
    '''

    # 'will echo' command sequence to be sent from DA telnet server
    # see RFCs 854 & 857
    WILL_ECHO_CMD = '\xff\xfd\x03\xff\xfb\x03\xff\xfb\x01'
    # 'do echo' command sequence to be sent back from telnet client
    DO_ECHO_CMD   = '\xff\xfb\x03\xff\xfd\x03\xff\xfd\x01'
    BUFFER_SIZE = 1024

    buf = ""

    def __init__(self, host = None, port = None):
        '''
        Constructor - open/connect to the socket
        @param host: host address
        @param port: host port
        '''
        self.buf = ""
        s = None

        if(host and port):
            self._connect(host, port)

    def _connect(self, host, port):
        log.debug("TcpClient._connect: host = " + str(host) + " port = " + str(port))
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((host, port))
        self.s.settimeout(0.0)

    def disconnect(self):
        log.debug("TcpClient.disconnect:")
        if(self.s):
            self.s.close()
            self.s = None

    def do_telnet_handshake(self):
        if(self.expect(self.WILL_ECHO_CMD)):
            self.send_data(self.DO_ECHO_CMD)
            return True
        return False

    def start_telnet(self, token=None, handshake=True):
        
        def assert_success(function_to_call, *args):
            result = function_to_call(*args)
            log.debug("TcpClient.start_telnet.assert_success: result = %s" %result)
            if result == False:
                raise Exception("call [%s] failed" %str(function_to_call))
        
        gevent.sleep(1)   # give DA server a chance to start handler
        try:
            assert_success(self.expect, "Username: ")
            assert_success(self.send_data, "someone\r\n")
            if token:
                assert_success(self.expect, "token: ")
                assert_success(self.send_data, token + "\r\n")
            else:
                return True
            if handshake:
                assert_success(self.do_telnet_handshake)
                assert_success(self.expect, "connected\r\n")
        except Exception as ex:
            log.debug("TcpClient.start_telnet: exception caught [%s]" %str(ex))
            return False
        return True
    
    def send_data(self, data):
        if self.s == None:
            return False
        log.debug("TcpClient.send_data: data to send = [" + repr(data) + "]")
        try:
            self.s.sendall(data)
            return True
        except Exception as ex:
            log.debug("TcpClient.send_data: exception caught [%s] during sending" %ex)
            return False

    def expect(self, prompt, timeout=2):

        if self.s == None:
            return False
        self.buf = ''
        found = False
        starttime = time.time() 
        
        log.debug('TcpClient.expect: prompt = [%s]' %prompt)
        while True:
            try:
                self.buf += self.s.recv(self.BUFFER_SIZE)
                log.debug('TcpClient.expect: buf = [%s]' % self.buf)
                if prompt and self.buf.find(prompt) != -1:
                    found = True
                    break
            except:
                gevent.sleep(.1)            
            delta = time.time() - starttime
            if delta > timeout:
                break
        if prompt == None:
            return found, self.buf 
        else:
            return found           
                

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('HARDWARE', group='sa')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 600}}})
@unittest.skipIf((not os.getenv('PYCC_MODE', False)) and os.getenv('CEI_LAUNCH_TEST', False), 'Skip until tests support launch port agent configurations.')
class TestVel3d(IonIntegrationTestCase):
    """
    Test cases for instrument agent class. Functions in this class provide
    instrument agent integration tests and provide a tutorial on use of
    the agent setup and interface.
    """
    
    def setUp(self):

        """
        Set up driver integration support.
        Start port agent, add port agent cleanup.
        Start container.
        Start deploy services.
        Define agent config, start agent.
        Start agent client.
        """
        print '#####################'
        print 'IN SETUP'
        
        self._ia_client = None

        # Start container.
        log.info('Staring capability container.')
        self._start_container()

        # Bring up services in a deploy file (no need to message)
        log.info('Staring deploy services.')
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        log.info('building stream configuration')
        # Setup stream config.
        self._build_stream_config()

        #log.info('driver uri: %s', DRV_URI)
        #log.info('device address: %s', DEV_ADDR)
        #log.info('device port: %s', DEV_PORT)
        #log.info('work dir: %s', WORK_DIR)
        
        # Create agent config.
        agent_config = {
            'driver_config' : DVR_CONFIG,
            'stream_config' : self._stream_config,
            'agent'         : {'resource_id': IA_RESOURCE_ID},
            'test_mode' : True,
            'forget_past' : True,
            'enable_persistence' : False
        }
    
        #if org_governance_name is not None:
        #    agent_config['org_governance_name'] = org_governance_name
    
        # Start instrument agent.
    
        log.info("TestInstrumentAgent.setup(): starting IA.")
        container_client = ContainerAgentClient(node=self.container.node,
            name=self.container.name)
    
        log.info("Agent setup")
        ia_pid = container_client.spawn_process(name=IA_NAME,
            module=IA_MOD,
            cls=IA_CLS,
            config=agent_config)    
        log.info('Agent pid=%s.', str(ia_pid))
        #self.addCleanup(self._verify_agent_reset)
    
        # Start a resource agent client to talk with the instrument agent.
    
        self._ia_client = ResourceAgentClient(IA_RESOURCE_ID, process=FakeProcess())
        log.info('Got ia client %s.', str(self._ia_client))
    
        log.info('test setup complete')


    ###############################################################################
    # Port agent helpers.
    ###############################################################################
                                
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
        
        encoder = IonObjectSerializer()
        
        # Create streams and subscriptions for each stream named in driver.
        self._stream_config = {}

        stream_name = 'parsed'
        param_dict_name = 'ctd_parsed_param_dict'
        pd_id = dataset_management.read_parameter_dictionary_by_name(param_dict_name, id_only=True)
        stream_def_id = pubsub_client.create_stream_definition(name=stream_name, parameter_dictionary_id=pd_id)
        stream_def = pubsub_client.read_stream_definition(stream_def_id)
        stream_def_dict = encoder.serialize(stream_def)        
        pd = stream_def.parameter_dictionary
        stream_id, stream_route = pubsub_client.create_stream(name=stream_name,
                                                exchange_point='science_data',
                                                stream_definition_id=stream_def_id)
        stream_config = dict(routing_key=stream_route.routing_key,
                                 exchange_point=stream_route.exchange_point,
                                 stream_id=stream_id,
                                 parameter_dictionary=pd,
                                 stream_def_dict=stream_def_dict)
        self._stream_config[stream_name] = stream_config

        stream_name = 'raw'
        param_dict_name = 'ctd_raw_param_dict'
        pd_id = dataset_management.read_parameter_dictionary_by_name(param_dict_name, id_only=True)
        stream_def_id = pubsub_client.create_stream_definition(name=stream_name, parameter_dictionary_id=pd_id)
        stream_def = pubsub_client.read_stream_definition(stream_def_id)
        stream_def_dict = encoder.serialize(stream_def)
        pd = stream_def.parameter_dictionary
        stream_id, stream_route = pubsub_client.create_stream(name=stream_name,
                                                exchange_point='science_data',
                                                stream_definition_id=stream_def_id)
        stream_config = dict(routing_key=stream_route.routing_key,
                                 exchange_point=stream_route.exchange_point,
                                 stream_id=stream_id,
                                 parameter_dictionary=pd,
                                 stream_def_dict=stream_def_dict)
        self._stream_config[stream_name] = stream_config

    def _start_data_subscribers(self, count, raw_count):
        """
        """        
        # Create a pubsub client to create streams.
        pubsub_client = PubsubManagementServiceClient(node=self.container.node)
                
        # Create streams and subscriptions for each stream named in driver.
        self._data_subscribers = []
        self._samples_received = []
        self._raw_samples_received = []
        self._async_sample_result = AsyncResult()
        self._async_raw_sample_result = AsyncResult()

        # A callback for processing subscribed-to data.
        def recv_data(message, stream_route, stream_id):
            log.info('Received parsed data on %s (%s,%s)', stream_id, stream_route.exchange_point, stream_route.routing_key)
            self._samples_received.append(message)
            if len(self._samples_received) == count:
                self._async_sample_result.set()

        def recv_raw_data(message, stream_route, stream_id):
            log.info('Received raw data on %s (%s,%s)', stream_id, stream_route.exchange_point, stream_route.routing_key)
            self._raw_samples_received.append(message)
            if len(self._raw_samples_received) == raw_count:
                self._async_raw_sample_result.set()

        from pyon.util.containers import create_unique_identifier

        stream_name = 'parsed'
        parsed_config = self._stream_config[stream_name]
        stream_id = parsed_config['stream_id']
        exchange_name = create_unique_identifier("%s_queue" %
                    stream_name)
        self._purge_queue(exchange_name)
        sub = StandaloneStreamSubscriber(exchange_name, recv_data)
        sub.start()
        self._data_subscribers.append(sub)
        sub_id = pubsub_client.create_subscription(name=exchange_name, stream_ids=[stream_id])
        pubsub_client.activate_subscription(sub_id)
        sub.subscription_id = sub_id # Bind the subscription to the standalone subscriber (easier cleanup, not good in real practice)
        
        stream_name = 'raw'
        parsed_config = self._stream_config[stream_name]
        stream_id = parsed_config['stream_id']
        exchange_name = create_unique_identifier("%s_queue" %
                    stream_name)
        self._purge_queue(exchange_name)
        sub = StandaloneStreamSubscriber(exchange_name, recv_raw_data)
        sub.start()
        self._data_subscribers.append(sub)
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
    # tcp helpers.
    ###############################################################################        

    def _start_tcp_client(self, retval):
        host = retval.result['ip_address']
        port = retval.result['port']
        tcp_client = TcpClient(host, port)
        return tcp_client
        

    ###############################################################################
    # Tests.
    ###############################################################################

    @unittest.skip('Test should be run manually only.')        
    def test_initialize(self):
        """
        test_initialize
        Test agent initialize command. This causes creation of
        driver process and transition to inactive.
        """
        
        print '#### in test'

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
        
    @unittest.skip('Test should be run manually only.')        
    def test_xx(self):
        """
        """


        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
    
        with self.assertRaises(Conflict):
            res_state = self._ia_client.get_resource_state()
            
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)
        
        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()    
        print '################################# mavs4 came up in state: ' + state
        
        if state == ResourceAgentState.IDLE:
            cmd = AgentCommand(command=ResourceAgentEvent.RUN)
            retval = self._ia_client.execute_agent(cmd)
            state = self._ia_client.get_agent_state()
            self.assertEqual(state, ResourceAgentState.COMMAND)        
        
        elif state == ResourceAgentState.STREAMING:
            cmd = AgentCommand(command='DRIVER_EVENT_STOP_AUTOSAMPLE')
            retval = self._ia_client.execute_resource(cmd)
            state = self._ia_client.get_agent_state()
            self.assertEqual(state, ResourceAgentState.COMMAND)        
        
        state = self._ia_client.get_agent_state()    
        print '################################# mavs4 now in state: ' + state
        
        cmd = AgentCommand(command=ResourceAgentEvent.GO_DIRECT_ACCESS,
                           kwargs={'session_type': DirectAccessTypes.telnet,
                           'session_timeout':600,
                           'inactivity_timeout':600})
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.DIRECT_ACCESS)        
        
        state = self._ia_client.get_agent_state()    
        print '################################# mavs4 now in state: ' + state
        
        tcp_client = self._start_tcp_client(retval)
        self.assertTrue(tcp_client.start_telnet(token=retval.result['token']))
        
        self.assertTrue(tcp_client.send_data('\r\r'))        
        
        gevent.sleep(180)
        
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.DIRECT_ACCESS)
        
        cmd = AgentCommand(command=ResourceAgentEvent.GO_COMMAND)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)
        

        
