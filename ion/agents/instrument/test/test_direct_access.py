#!/usr/bin/env python

"""
@package ion.agents.instrument.test.test_direct_access
@file ion/agents.instrument/test/test_direct_access.py
@author Bill Bollenbacher
@brief Test cases for R2 instrument agent.
"""

__author__ = 'Bill Bollenbacher'
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
import os

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

# Alarms.
from pyon.public import IonObject
from interface.objects import StreamAlertType, AggregateStatusType

from ooi.timer import Timer

"""
--with-queueblame   report leftover queues
--with-pycc         run in seperate container
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_gateway_to_instrument_agent.py:TestInstrumentAgentViaGateway
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_initialize
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_resource_states
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_states
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_get_set
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_get_set_errors
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_get_set_agent
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_poll
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_autosample
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_capabilities
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_command_errors
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_direct_access
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_test
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_states_special
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_data_buffering
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_streaming_memuse
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_capabilities_new
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_exit_da_timing
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
DRV_URI = 'http://sddevrepo.oceanobservatories.org/releases/seabird_sbe37smb_ooicore-0.1.1-py2.7.egg'
DRV_SHA = '28e1b59708d72e008b0aa68ea7392d3a2467f393'
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

if LAUNCH_FROM_EGG:
    # Dynamically load the egg into the test path
    launcher = ZMQEggDriverProcess(DVR_CONFIG)
    egg = launcher._get_egg(DRV_URI)
    from hashlib import sha1
    with open(egg,'r') as f:
        doc = f.read()
        sha = sha1(doc).hexdigest()
        if sha != DRV_SHA:
            raise ImportError('Failed to load driver %s: incorrect checksum.  (%s!=%s)' % (DRV_URI, DRV_SHA, sha))
    if not egg in sys.path: sys.path.insert(0, egg)
    DVR_CONFIG['process_type'] = (DriverProcessType.EGG,)

else:
    mi_repo = os.getcwd() + os.sep + 'extern' + os.sep + 'mi_repo'
    if not mi_repo in sys.path: sys.path.insert(0, mi_repo)
    DVR_CONFIG['process_type'] = (DriverProcessType.PYTHON_MODULE,)
    DVR_CONFIG['mi_repo'] = mi_repo

# Load MI modules from the egg
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverConnectionState
from mi.core.exceptions import InstrumentParameterException
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Parameter


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


class TcpClient():
    '''
    TCP client for testing.
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
            assert_success(self.expect, "tOken: ")
            assert_success(self.send_data, token + "\r\n")
            assert_success(self.do_telnet_handshake)
            assert_success(self.expect, "connected\r\n")
        except Exception as ex:
            log.debug("TcpClient.start_telnet: exception caught [%s]" %str(ex))
            return False
        return True
    
    def send_data(self, data):
        log.debug("TcpClient.send_data: data to send = [" + repr(data) + "]")
        try:
            self.s.sendall(data)
            return True
        except Exception as ex:
            log.debug("TcpClient.send_data: exception caught [%s] during sending" %ex)
            return False

    def expect(self, prompt, timeout=1):

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

#@unittest.skipIf((not os.getenv('PYCC_MODE', False)) and os.getenv('CEI_LAUNCH_TEST', False), 'Skip until tests support launch port agent configurations.')
class InstrumentAgentTest():
    """
    Test cases for instrument agent class. Functions in this class provide
    instrument agent integration tests and provide a tutorial on use of
    the agent setup and interface.
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
        
        self.assertIsInstance(val, dict)
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

    def assertGranule(self, granule):
        rdt = RecordDictionaryTool.load_from_granule(granule)
        self.assertIsInstance(rdt['temp'][0], numpy.float32)
        self.assertIsInstance(rdt['conductivity'][0], numpy.float32)
        self.assertIsInstance(rdt['pressure'][0], numpy.float32)
        self.assertIsInstance(rdt['time'][0], numpy.float64)

    def assertRawGranule(self, granule):
        rdt = RecordDictionaryTool.load_from_granule(granule)
        self.assertIsInstance(rdt['raw'][0], str)            
        self.assertIsInstance(rdt['time'][0], numpy.float64)            

    def assertVectorGranules(self, granule_list, field):
        sizes = []
        for granule in granule_list:
            rdt = RecordDictionaryTool.load_from_granule(granule)
            sizes.append(rdt[field].size)
        self.assertTrue(any([x>1 for x in sizes]))
        
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
            #time.sleep(1)
                
    
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
        token = retval.result['token']
        
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
        
        self.assertSetInstrumentState(ResourceAgentEvent.RESET, ResourceAgentState.UNINITIALIZED)
        
    def test_direct_access_telnet_connection(self):
        """
        Test agent direct_access mode for telnet connection. This causes creation of
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
        
        self.assertSetInstrumentState(ResourceAgentEvent.RESET, ResourceAgentState.UNINITIALIZED)
        
    def test_direct_access_session_timeout(self):
        """
        Test agent session timeout for direct_access mode. This causes creation of
        driver process and transition to direct access.
        """

        self.assertInstrumentAgentState(ResourceAgentState.UNINITIALIZED)
    
        self.assertSetInstrumentState(ResourceAgentEvent.INITIALIZE, ResourceAgentState.INACTIVE)

        self.assertSetInstrumentState(ResourceAgentEvent.GO_ACTIVE, ResourceAgentState.IDLE)

        self.assertSetInstrumentState(ResourceAgentEvent.RUN, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_DIRECT_ACCESS,
                            #kwargs={'session_type': DirectAccessTypes.telnet,
                            kwargs={'session_type':DirectAccessTypes.vsp,
                            'session_timeout':10,
                            'inactivity_timeout':600})
        
        retval = self.assertSetInstrumentState(cmd, ResourceAgentState.DIRECT_ACCESS)

        log.info("GO_DIRECT_ACCESS retval=" + str(retval.result))

        # {'status': 0, 'type_': 'AgentCommandResult', 'command': 'RESOURCE_AGENT_EVENT_GO_DIRECT_ACCESS',
        # 'result': {'token': 'F2B6EED3-F926-4B3B-AE80-4F8DE79276F3', 'ip_address': 'Edwards-MacBook-Pro.local', 'port': 8000},
        # 'ts_execute': '1344889063861', 'command_id': ''}

        self.assertInstrumentAgentState(ResourceAgentState.COMMAND, timeout=120)        
        
        self.assertSetInstrumentState(ResourceAgentEvent.RESET, ResourceAgentState.UNINITIALIZED)
        
    def test_direct_access_inactivity_timeout(self):
        """
        Test agent inactivity timeout for direct_access mode. This causes creation of
        driver process and transition to direct access.
        """

        self.assertInstrumentAgentState(ResourceAgentState.UNINITIALIZED)
    
        self.assertSetInstrumentState(ResourceAgentEvent.INITIALIZE, ResourceAgentState.INACTIVE)

        self.assertSetInstrumentState(ResourceAgentEvent.GO_ACTIVE, ResourceAgentState.IDLE)

        self.assertSetInstrumentState(ResourceAgentEvent.RUN, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_DIRECT_ACCESS,
                            #kwargs={'session_type': DirectAccessTypes.telnet,
                            kwargs={'session_type':DirectAccessTypes.vsp,
                            'session_timeout':600,
                            'inactivity_timeout':10})
        
        retval = self.assertSetInstrumentState(cmd, ResourceAgentState.DIRECT_ACCESS)

        log.info("GO_DIRECT_ACCESS retval=" + str(retval.result))

        # {'status': 0, 'type_': 'AgentCommandResult', 'command': 'RESOURCE_AGENT_EVENT_GO_DIRECT_ACCESS',
        # 'result': {'token': 'F2B6EED3-F926-4B3B-AE80-4F8DE79276F3', 'ip_address': 'Edwards-MacBook-Pro.local', 'port': 8000},
        # 'ts_execute': '1344889063861', 'command_id': ''}

        self.assertInstrumentAgentState(ResourceAgentState.COMMAND, timeout=120)        
        
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
                            #kwargs={'session_type': DirectAccessTypes.telnet,
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
        
        cap_list = self._ia_client.get_capabilities()
        
        delta = time.time() - starttime
        
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        print '######## exiting direct access takes: %f seconds' % delta


@attr('HARDWARE', group='sa')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 300}}})
@unittest.skipIf((not os.getenv('PYCC_MODE', False)) and os.getenv('CEI_LAUNCH_TEST', False), 'Skip until tests support launch port agent configurations.')
class TestInstrumentAgent(IonIntegrationTestCase, InstrumentAgentTest):

    ############################################################################
    # Setup, teardown.
    ############################################################################

    def setUp(self):
        self._setup()
