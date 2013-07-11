#!/usr/bin/env python

"""
@package ion.agents.instrument.test.test_direct_access
@file ion/agents.instrument/test/test_direct_access.py
@author Bill Bollenbacher
@brief Test cases for R2 instrument agent.
"""

"""
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
NOTE: MANY OF THESE TESTS WON"T PASS IF USED WITH THE OLDER DRIVER EGGS (THAT INCLUDE .PYC FILES) SINCE THE DRIVERS RUN
10 TIMES SLOWER THAN THEY SHOULD WITH THESE EGGS
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
"""

__author__ = 'Bill Bollenbacher'
__license__ = 'Apache 2.0'

# Import pyon first for monkey patching.

# Pyon log and config objects.
from pyon.public import log
from pyon.public import CFG

# Standard imports.
import time
import socket
import re
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

# Objects and clients.
from interface.objects import AgentCommand
from interface.objects import CapabilityType
from interface.objects import AgentCapability
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient

# Alarms.
from pyon.public import IonObject
from interface.objects import StreamAlertType, AggregateStatusType

from ooi.timer import Timer

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

#from ion.agents.instrument.instrument_agent import InstrumentAgent
# Agent parameters.
IA_RESOURCE_ID = '123xyz'
IA_NAME = 'Agent007'
IA_MOD = 'ion.agents.instrument.instrument_agent'
IA_CLS = 'InstrumentAgent'

# A seabird driver.
DRV_URI = 'http://sddevrepo.oceanobservatories.org/releases/seabird_sbe37smb_ooicore-0.1.1-py2.7.egg'
DRV_SHA = '28e1b59708d72e008b0aa68ea7392d3a2467f393'
#DRV_URI = 'http://sddevrepo.oceanobservatories.org/releases/seabird_sbe37smb_ooicore-0.1.4-py2.7.egg'
#DRV_SHA = '50de2e8383ebd801c3cd78c31f88983800e6bd0c'
DRV_MOD = 'mi.instrument.seabird.sbe37smb.ooicore.driver'
DRV_CLS = 'SBE37Driver'

# Driver config.
# DVR_CONFIG['comms_config']['port'] is set by the setup.
DVR_CONFIG = {
    'dvr_egg' : DRV_URI,
    'dvr_mod' : DRV_MOD,
    'dvr_cls' : DRV_CLS,
    'workdir' : WORK_DIR,
    'process_type' : None
}

# Launch from egg or a local MI repo.
LAUNCH_FROM_EGG=True
#LAUNCH_FROM_EGG=False

if LAUNCH_FROM_EGG:
    # tell driver process launcher to dynamically load the driver egg into the python path
    DVR_CONFIG['process_type'] = (DriverProcessType.EGG,)

else:
    # tell driver process launcher to load the driver module from 'mi_repo' 
    #mi_repo = os.getcwd() + os.sep + 'extern' + os.sep + 'mi_repo'
    mi_repo = os.getcwd() + os.sep + 'extern' + os.sep + 'egg_test'
    DVR_CONFIG['process_type'] = (DriverProcessType.PYTHON_MODULE,)
    DVR_CONFIG['mi_repo'] = mi_repo


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

    def start_telnet(self, token):
        
        def assert_success(function_to_call, *args):
            result = function_to_call(*args)
            log.debug("TcpClient.start_telnet.assert_success: result = %s" %result)
            if result == False:
                raise Exception("call [%s] failed" %str(function_to_call))
        
        gevent.sleep(1)   # give DA server a chance to start handler
        try:
            assert_success(self.expect, "Username: ")
            assert_success(self.send_data, "someone\r\n")
            assert_success(self.expect, "token: ")
            assert_success(self.send_data, token + "\r\n")
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
                gevent.sleep(1)            
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

#Refactored as stand alone method for starting an instrument agent for use in other tests, like governance
#to do policy testing for resource agents
#shenrie
def start_instrument_agent_process(container, stream_config={}, resource_id=IA_RESOURCE_ID, resource_name=IA_NAME, org_governance_name=None, message_headers=None):
    log.info("foobar")

    # Create agent config.
    agent_config = {
        'driver_config' : DVR_CONFIG,
        'stream_config' : stream_config,
        'agent'         : {'resource_id': resource_id},
        'test_mode' : True,
        'forget_past' : True,
        'enable_persistence' : False
    }

    if org_governance_name is not None:
        agent_config['org_governance_name'] = org_governance_name


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

class InstrumentAgentTestDA():
    """
    Test cases for instrument agent class. Functions in this class provide
    instrument agent integration tests for Direct Access mode and provide 
    a tutorial on use of the agent setup and interface.
    """
    
    def _setup(self):

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
        log.info('LAUNCH_FROM_EGG: %s', LAUNCH_FROM_EGG)
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
    # Assert helpers.
    ###############################################################################        

    def assertInstrumentAgentState(self, expected_state, timeout=0):
        end_time = time.time() + timeout
        
        while (True):
            state = self._ia_client.get_agent_state()
            log.debug("assertInstrumentAgentState: IA state = %s, expected state = %s" %(state, expected_state))
            if state == expected_state:
                return True
            if time.time() >= end_time:
                self.fail("assertInstrumentAgentState: IA failed to transition to %s state" %expected_state)
            gevent.sleep(1)
                
    
    def assertSetInstrumentState(self, command, new_state):
        if type(command) == str:
            log.debug("assertSetInstrumentState: building command for %s" %command)
            cmd = AgentCommand(command=command)
        else:
            cmd = command
        retval = self._ia_client.execute_agent(cmd)
        self.assertInstrumentAgentState(new_state)
        return retval
        

    ###############################################################################
    # Tests.
    ###############################################################################

    def test_direct_access_vsp_connection(self):
        """
        Test agent direct_access mode for virtual serial port. This causes creation of
        driver process and transition to direct access.
        """
        def start_and_stop_DA():
            retval = self.assertSetInstrumentState(cmd, ResourceAgentState.DIRECT_ACCESS)
            host = retval.result['ip_address']
            port = retval.result['port']
            tcp_client = TcpClient(host, port)
            self.assertTrue(tcp_client.send_data('ts\r\n'))
            self.assertSetInstrumentState(ResourceAgentEvent.GO_COMMAND, ResourceAgentState.COMMAND)

        self.assertInstrumentAgentState(ResourceAgentState.UNINITIALIZED)
    
        self.assertSetInstrumentState(ResourceAgentEvent.INITIALIZE, ResourceAgentState.INACTIVE)

        self.assertSetInstrumentState(ResourceAgentEvent.GO_ACTIVE, ResourceAgentState.IDLE)

        self.assertSetInstrumentState(ResourceAgentEvent.RUN, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_DIRECT_ACCESS,
                           kwargs={'session_type':DirectAccessTypes.vsp,
                           'session_timeout':600,
                           'inactivity_timeout':600})
        
        retval = self.assertSetInstrumentState(cmd, ResourceAgentState.DIRECT_ACCESS)

        log.info("GO_DIRECT_ACCESS retval=" + str(retval.result))

        # {'status': 0, 'type_': 'AgentCommandResult', 'command': 'RESOURCE_AGENT_EVENT_GO_DIRECT_ACCESS',
        # 'result': {'token': 'F2B6EED3-F926-4B3B-AE80-4F8DE79276F3', 'ip_address': 'Edwards-MacBook-Pro.local', 'port': 8000},
        # 'ts_execute': '1344889063861', 'command_id': ''}
        
        host = retval.result['ip_address']
        port = retval.result['port']
        
        tcp_client = TcpClient(host, port)
        
        self.assertTrue(tcp_client.send_data('ts\r\n'))
        _, response = tcp_client.expect(None, 3)

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
        lines = response.split('\r\n')
        
        sample_count = 0
        for x in lines:
            if sample_regex.match(x):
                sample_count += 1
        #self.assertEqual(sample_count, 1)

        self.assertTrue(tcp_client.send_data('ds\r\n'))
        _, response = tcp_client.expect(None, 3)

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

        self.assertNotEqual(response.find('SBE37-SMP'), -1)
        self.assertNotEqual(response.find('sample interval'), -1)
        self.assertNotEqual(response.find('samplenumber'), -1)
        self.assertNotEqual(response.find('number of samples to average'), -1)

        self.assertSetInstrumentState(ResourceAgentEvent.GO_COMMAND, ResourceAgentState.COMMAND)
        
        for i in range(0, 10):
            start_and_stop_DA()
        
        self.assertSetInstrumentState(ResourceAgentEvent.RESET, ResourceAgentState.UNINITIALIZED)
        
    def test_direct_access_telnet_connection(self):
        """
        Test agent direct_access mode for telnet connection. This causes creation of
        driver process and transition to direct access.
        """

        def start_and_stop_DA():
            retval = self.assertSetInstrumentState(cmd, ResourceAgentState.DIRECT_ACCESS)
            host = retval.result['ip_address']
            port = retval.result['port']
            token = retval.result['token']
            tcp_client = TcpClient(host, port)
            self.assertTrue(tcp_client.start_telnet(token))
            self.assertTrue(tcp_client.send_data('ts\r\n'))
            self.assertSetInstrumentState(ResourceAgentEvent.GO_COMMAND, ResourceAgentState.COMMAND)

        self.assertInstrumentAgentState(ResourceAgentState.UNINITIALIZED)
    
        self.assertSetInstrumentState(ResourceAgentEvent.INITIALIZE, ResourceAgentState.INACTIVE)

        self.assertSetInstrumentState(ResourceAgentEvent.GO_ACTIVE, ResourceAgentState.IDLE)

        self.assertSetInstrumentState(ResourceAgentEvent.RUN, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_DIRECT_ACCESS,
                           kwargs={'session_type': DirectAccessTypes.telnet,
                           'session_timeout':600,
                           'inactivity_timeout':600})
        
        retval = self.assertSetInstrumentState(cmd, ResourceAgentState.DIRECT_ACCESS)

        log.info("GO_DIRECT_ACCESS retval=" + str(retval.result))

        # {'status': 0, 'type_': 'AgentCommandResult', 'command': 'RESOURCE_AGENT_EVENT_GO_DIRECT_ACCESS',
        # 'result': {'token': 'F2B6EED3-F926-4B3B-AE80-4F8DE79276F3', 'ip_address': 'Edwards-MacBook-Pro.local', 'port': 8000},
        # 'ts_execute': '1344889063861', 'command_id': ''}
        
        host = retval.result['ip_address']
        port = retval.result['port']
        token = retval.result['token']
        
        tcp_client = TcpClient(host, port)
        
        self.assertTrue(tcp_client.start_telnet(token))
        
        self.assertTrue(tcp_client.send_data('ts\r\n'))
        _, response = tcp_client.expect(None, 3)

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
        lines = response.split('\r\n')
        
        sample_count = 0
        for x in lines:
            if sample_regex.match(x):
                sample_count += 1
        #self.assertEqual(sample_count, 1)

        self.assertTrue(tcp_client.send_data('ds\r\n'))
        _, response = tcp_client.expect(None, 3)

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

        self.assertNotEqual(response.find('SBE37-SMP'), -1)
        self.assertNotEqual(response.find('sample interval'), -1)
        self.assertNotEqual(response.find('samplenumber'), -1)
        self.assertNotEqual(response.find('number of samples to average'), -1)

        self.assertSetInstrumentState(ResourceAgentEvent.GO_COMMAND, ResourceAgentState.COMMAND)
        
        for i in range(0, 10):
            start_and_stop_DA()
        
        self.assertSetInstrumentState(ResourceAgentEvent.RESET, ResourceAgentState.UNINITIALIZED)
        
    def test_direct_access_vsp_client_disconnect(self):
        """
        Test agent direct_access mode for virtual serial port with client disconnect. This causes creation of
        driver process and transition to direct access.
        """

        self.assertInstrumentAgentState(ResourceAgentState.UNINITIALIZED)
    
        self.assertSetInstrumentState(ResourceAgentEvent.INITIALIZE, ResourceAgentState.INACTIVE)

        self.assertSetInstrumentState(ResourceAgentEvent.GO_ACTIVE, ResourceAgentState.IDLE)

        self.assertSetInstrumentState(ResourceAgentEvent.RUN, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_DIRECT_ACCESS,
                           kwargs={'session_type':DirectAccessTypes.vsp,
                           'session_timeout':600,
                           'inactivity_timeout':600})
        
        retval = self.assertSetInstrumentState(cmd, ResourceAgentState.DIRECT_ACCESS)

        log.info("GO_DIRECT_ACCESS retval=" + str(retval.result))

        # {'status': 0, 'type_': 'AgentCommandResult', 'command': 'RESOURCE_AGENT_EVENT_GO_DIRECT_ACCESS',
        # 'result': {'token': 'F2B6EED3-F926-4B3B-AE80-4F8DE79276F3', 'ip_address': 'Edwards-MacBook-Pro.local', 'port': 8000},
        # 'ts_execute': '1344889063861', 'command_id': ''}
        
        host = retval.result['ip_address']
        port = retval.result['port']
        
        tcp_client = TcpClient(host, port)
        
        self.assertTrue(tcp_client.send_data('ts\r\n'))
        
        tcp_client.disconnect()

        self.assertInstrumentAgentState(ResourceAgentState.COMMAND, timeout=20)        
                
        self.assertSetInstrumentState(ResourceAgentEvent.RESET, ResourceAgentState.UNINITIALIZED)
        
    def test_direct_access_telnet_client_disconnect(self):
        """
        Test agent direct_access mode for telnet client disconnect. This causes creation of
        driver process and transition to direct access.
        """

        self.assertInstrumentAgentState(ResourceAgentState.UNINITIALIZED)
    
        self.assertSetInstrumentState(ResourceAgentEvent.INITIALIZE, ResourceAgentState.INACTIVE)

        self.assertSetInstrumentState(ResourceAgentEvent.GO_ACTIVE, ResourceAgentState.IDLE)

        self.assertSetInstrumentState(ResourceAgentEvent.RUN, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_DIRECT_ACCESS,
                           kwargs={'session_type': DirectAccessTypes.telnet,
                           'session_timeout':600,
                           'inactivity_timeout':600})
        
        retval = self.assertSetInstrumentState(cmd, ResourceAgentState.DIRECT_ACCESS)

        log.info("GO_DIRECT_ACCESS retval=" + str(retval.result))

        # {'status': 0, 'type_': 'AgentCommandResult', 'command': 'RESOURCE_AGENT_EVENT_GO_DIRECT_ACCESS',
        # 'result': {'token': 'F2B6EED3-F926-4B3B-AE80-4F8DE79276F3', 'ip_address': 'Edwards-MacBook-Pro.local', 'port': 8000},
        # 'ts_execute': '1344889063861', 'command_id': ''}
        
        host = retval.result['ip_address']
        port = retval.result['port']
        token = retval.result['token']
        
        tcp_client = TcpClient(host, port)
        
        self.assertTrue(tcp_client.start_telnet(token))

        self.assertTrue(tcp_client.send_data('ts\r\n'))
        
        tcp_client.disconnect()

        self.assertInstrumentAgentState(ResourceAgentState.COMMAND, timeout=20)        
                
        self.assertSetInstrumentState(ResourceAgentEvent.RESET, ResourceAgentState.UNINITIALIZED)
        
    def test_direct_access_vsp_session_timeout(self):
        """
        Test agent session timeout for direct_access mode. This causes creation of
        driver process and transition to direct access.
        """

        self.assertInstrumentAgentState(ResourceAgentState.UNINITIALIZED)
    
        self.assertSetInstrumentState(ResourceAgentEvent.INITIALIZE, ResourceAgentState.INACTIVE)

        self.assertSetInstrumentState(ResourceAgentEvent.GO_ACTIVE, ResourceAgentState.IDLE)

        self.assertSetInstrumentState(ResourceAgentEvent.RUN, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_DIRECT_ACCESS,
                           kwargs={'session_type':DirectAccessTypes.vsp,
                           'session_timeout':10,
                           'inactivity_timeout':600})
        
        retval = self.assertSetInstrumentState(cmd, ResourceAgentState.DIRECT_ACCESS)

        log.info("GO_DIRECT_ACCESS retval=" + str(retval.result))

        # {'status': 0, 'type_': 'AgentCommandResult', 'command': 'RESOURCE_AGENT_EVENT_GO_DIRECT_ACCESS',
        # 'result': {'token': 'F2B6EED3-F926-4B3B-AE80-4F8DE79276F3', 'ip_address': 'Edwards-MacBook-Pro.local', 'port': 8000},
        # 'ts_execute': '1344889063861', 'command_id': ''}

        self.assertInstrumentAgentState(ResourceAgentState.COMMAND, timeout=30)        
        
        self.assertSetInstrumentState(ResourceAgentEvent.RESET, ResourceAgentState.UNINITIALIZED)
        
    def test_direct_access_telnet_session_timeout(self):
        """
        Test agent session timeout for direct_access mode. This causes creation of
        driver process and transition to direct access.
        """

        self.assertInstrumentAgentState(ResourceAgentState.UNINITIALIZED)
    
        self.assertSetInstrumentState(ResourceAgentEvent.INITIALIZE, ResourceAgentState.INACTIVE)

        self.assertSetInstrumentState(ResourceAgentEvent.GO_ACTIVE, ResourceAgentState.IDLE)

        self.assertSetInstrumentState(ResourceAgentEvent.RUN, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_DIRECT_ACCESS,
                           kwargs={'session_type': DirectAccessTypes.telnet,
                           'session_timeout':10,
                           'inactivity_timeout':600})
        
        retval = self.assertSetInstrumentState(cmd, ResourceAgentState.DIRECT_ACCESS)

        log.info("GO_DIRECT_ACCESS retval=" + str(retval.result))

        # {'status': 0, 'type_': 'AgentCommandResult', 'command': 'RESOURCE_AGENT_EVENT_GO_DIRECT_ACCESS',
        # 'result': {'token': 'F2B6EED3-F926-4B3B-AE80-4F8DE79276F3', 'ip_address': 'Edwards-MacBook-Pro.local', 'port': 8000},
        # 'ts_execute': '1344889063861', 'command_id': ''}

        host = retval.result['ip_address']
        port = retval.result['port']
        token = retval.result['token']
        
        tcp_client = TcpClient(host, port)
        
        self.assertTrue(tcp_client.start_telnet(token))

        self.assertInstrumentAgentState(ResourceAgentState.COMMAND, timeout=30)        
        
        self.assertSetInstrumentState(ResourceAgentEvent.RESET, ResourceAgentState.UNINITIALIZED)
        
    def test_direct_access_vsp_inactivity_timeout(self):
        """
        Test agent inactivity timeout for direct_access mode. This causes creation of
        driver process and transition to direct access.
        """

        self.assertInstrumentAgentState(ResourceAgentState.UNINITIALIZED)
    
        self.assertSetInstrumentState(ResourceAgentEvent.INITIALIZE, ResourceAgentState.INACTIVE)

        self.assertSetInstrumentState(ResourceAgentEvent.GO_ACTIVE, ResourceAgentState.IDLE)

        self.assertSetInstrumentState(ResourceAgentEvent.RUN, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_DIRECT_ACCESS,
                           kwargs={'session_type':DirectAccessTypes.vsp,
                           'session_timeout':600,
                           'inactivity_timeout':10})
        
        retval = self.assertSetInstrumentState(cmd, ResourceAgentState.DIRECT_ACCESS)

        log.info("GO_DIRECT_ACCESS retval=" + str(retval.result))

        # {'status': 0, 'type_': 'AgentCommandResult', 'command': 'RESOURCE_AGENT_EVENT_GO_DIRECT_ACCESS',
        # 'result': {'token': 'F2B6EED3-F926-4B3B-AE80-4F8DE79276F3', 'ip_address': 'Edwards-MacBook-Pro.local', 'port': 8000},
        # 'ts_execute': '1344889063861', 'command_id': ''}

        self.assertInstrumentAgentState(ResourceAgentState.COMMAND, timeout=30)        
        
        self.assertSetInstrumentState(ResourceAgentEvent.RESET, ResourceAgentState.UNINITIALIZED)
        
    def test_direct_access_telnet_inactivity_timeout(self):
        """
        Test agent inactivity timeout for direct_access mode. This causes creation of
        driver process and transition to direct access.
        """

        self.assertInstrumentAgentState(ResourceAgentState.UNINITIALIZED)
    
        self.assertSetInstrumentState(ResourceAgentEvent.INITIALIZE, ResourceAgentState.INACTIVE)

        self.assertSetInstrumentState(ResourceAgentEvent.GO_ACTIVE, ResourceAgentState.IDLE)

        self.assertSetInstrumentState(ResourceAgentEvent.RUN, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_DIRECT_ACCESS,
                           kwargs={'session_type': DirectAccessTypes.telnet,
                           'session_timeout':600,
                           'inactivity_timeout':10})
        
        retval = self.assertSetInstrumentState(cmd, ResourceAgentState.DIRECT_ACCESS)

        log.info("GO_DIRECT_ACCESS retval=" + str(retval.result))

        # {'status': 0, 'type_': 'AgentCommandResult', 'command': 'RESOURCE_AGENT_EVENT_GO_DIRECT_ACCESS',
        # 'result': {'token': 'F2B6EED3-F926-4B3B-AE80-4F8DE79276F3', 'ip_address': 'Edwards-MacBook-Pro.local', 'port': 8000},
        # 'ts_execute': '1344889063861', 'command_id': ''}

        host = retval.result['ip_address']
        port = retval.result['port']
        token = retval.result['token']
        
        tcp_client = TcpClient(host, port)
        
        self.assertTrue(tcp_client.start_telnet(token))

        self.assertInstrumentAgentState(ResourceAgentState.COMMAND, timeout=30)        
        
        self.assertSetInstrumentState(ResourceAgentEvent.RESET, ResourceAgentState.UNINITIALIZED)
        
    @unittest.skip('A manual test for timing purposes.')        
    def test_exit_da_timing(self):
        """
        test_exit_da_timing
        Test time it takes to leave direct access and return to command mode.
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
                           kwargs={'session_type':DirectAccessTypes.vsp,
                           'session_timeout':600,
                           'inactivity_timeout':600})
        
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.DIRECT_ACCESS)
                
        log.info("GO_DIRECT_ACCESS retval=" + str(retval.result))

        starttime = time.time()
        
        cmd = AgentCommand(command=ResourceAgentEvent.GO_COMMAND)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)
        
        delta = time.time() - starttime
        
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        print '######## exiting direct access takes: %f seconds' % delta


@attr('HARDWARE', group='sa')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 300}}})
@unittest.skipIf((not os.getenv('PYCC_MODE', False)) and os.getenv('CEI_LAUNCH_TEST', False), 'Skip until tests support launch port agent configurations.')
class TestInstrumentAgent(IonIntegrationTestCase, InstrumentAgentTestDA):

    ############################################################################
    # Setup, teardown.
    ############################################################################

    def setUp(self):
        self._setup()
