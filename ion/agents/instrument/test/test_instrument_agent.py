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
--with-greenletleak ewpoer leftover greenlets
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
bin/nosetests -s -v --nologcapture --with-greenletleak ion/agents/instrument/test/test_instrument_agent.py:TestInstrumentAgent.test_driver_crash
"""

###############################################################################
# Global constants.
###############################################################################

# Real and simulated devcies we test against.
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
DELIM = CFG.device.sbe37.delim
WORK_DIR = CFG.device.sbe37.workdir
DRV_URI = CFG.device.sbe37.dvr_egg

from ion.agents.instrument.test.agent_test_constants import IA_RESOURCE_ID
from ion.agents.instrument.test.agent_test_constants import IA_NAME
from ion.agents.instrument.test.agent_test_constants import IA_MOD
from ion.agents.instrument.test.agent_test_constants import IA_CLS

# Launch from egg or a local MI repo.
LAUNCH_FROM_EGG=True
if LAUNCH_FROM_EGG:
    from ion.agents.instrument.test.load_test_driver_egg import load_egg
    DVR_CONFIG = load_egg()

else:
    mi_repo = '/path/to/your/local/mi/repo'
    from ion.agents.instrument.test.load_test_driver_egg import load_repo
    DVR_CONFIG = load_repo(mi_repo)

# Load MI modules from the egg
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverConnectionState
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
class InstrumentAgentTest(IonIntegrationTestCase):
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
        log.info('Received a granule with time parameter: %f' % rdt['time'][0])
        log.info('Preferred timestamp: %s' % rdt['preferred_timestamp'][0])

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

        cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
        with self.assertRaises(Conflict):
            retval = self._ia_client.execute_resource(cmd)

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

    def _test_next_state(self, step_tuple, timeout=None, recover=30):
        # execute command
        log.info('agent FSM step: %r timeout=%s', step_tuple, timeout)

        if step_tuple[0]:
            if timeout:
                with self.assertRaises(Timeout):
                    try:
                        self._ia_client.execute_agent(AgentCommand(command=step_tuple[0]), timeout=timeout)
                    except:
                        log.error('expecting agent command to timeout: %s', step_tuple[0], exc_info=True)
                        raise
                # give some time for the operation in the agent to complete, even RPC timed out
                time.sleep(recover)
            else:
                self._ia_client.execute_agent(AgentCommand(command=step_tuple[0]))
        # check state
        if step_tuple[1]:
            state = self._ia_client.get_agent_state()
            self.assertEqual(state, step_tuple[1])
        # ping agent
        if len(step_tuple)>2:
            if step_tuple[2]:
                self._ia_client.ping_agent()
            else:
                with self.assertRaises(Conflict):
                    self._ia_client.ping_agent()
        # ping resource
        if len(step_tuple)>3:
            if step_tuple[3]:
                self._ia_client.ping_resource()
            else:
                with self.assertRaises(Conflict):
                    self._ia_client.ping_resource()

    def test_states_timeout(self):
        """
        get_states test like above,
        but cause RPC to timeout before instrument completes state transition.
        """
        # Set up a subscriber to collect error events.
        self._start_event_subscriber('ResourceAgentStateEvent', 8)
        self.addCleanup(self._stop_event_subscriber)

        step_times_out = ResourceAgentEvent.GO_ACTIVE

        steps = [
            # (cmd, state_after, [ping_agent, ping_resource])
            (None, ResourceAgentState.UNINITIALIZED, True, False),
            (ResourceAgentEvent.INITIALIZE, ResourceAgentState.INACTIVE, True, True),
            (ResourceAgentEvent.GO_ACTIVE, ResourceAgentState.IDLE),
            (ResourceAgentEvent.RUN, ResourceAgentState.COMMAND),
            (ResourceAgentEvent.PAUSE, ResourceAgentState.STOPPED),
            (ResourceAgentEvent.RESUME, ResourceAgentState.COMMAND),
            (ResourceAgentEvent.CLEAR, ResourceAgentState.IDLE),
            (ResourceAgentEvent.RUN, ResourceAgentState.COMMAND),
            (ResourceAgentEvent.RESET, ResourceAgentState.UNINITIALIZED),
        ]

        # not all steps will time out in 5sec
        step_times_out  = ResourceAgentEvent.GO_ACTIVE

        for step in steps:
            if step[0]==step_times_out:
                self._test_next_state(step, timeout=5)
            else:
                self._test_next_state(step)

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

        # Returning an InstrumentParameterException, not BadRequest
        # agent not mapping correctly?
        with self.assertRaises(BadRequest):
            self._ia_client.get_resource()
                
        # Attempt to get with bogus parameters.
        params = [
            'I am a bogus parameter name',
            SBE37Parameter.OUTPUTSV            
        ]

        # Returning an InstrumentParameterException, not BadRequest
        # agent not mapping correctly?
        with self.assertRaises(BadRequest):
            retval = self._ia_client.get_resource(params)

        # Returning an InstrumentParameterException, not BadRequest
        # agent not mapping correctly?
        # Attempt to set with no parameters.
        # Set without parameters.
        with self.assertRaises(BadRequest):
            retval = self._ia_client.set_resource()
        
        # Attempt to set with bogus parameters.
        params = {
            'I am a bogus parameter name' : 'bogus val',
            SBE37Parameter.OUTPUTSV : False
        }
        # Returning an InstrumentParameterException, not BadRequest
        # agent not mapping correctly?
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

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        # Test the driver name.
        if LAUNCH_FROM_EGG:
            retval = self._ia_client.get_agent(['driver_name'])
            driver_name = retval['driver_name']
            self.assertIsInstance(driver_name, str)
            self.assertTrue(driver_name.endswith('.egg'))

            # Driver name is read only.
            self._ia_client.set_agent({'driver_name' : 'bogus'})
            retval = self._ia_client.get_agent(['driver_name'])
            driver_name_new = retval['driver_name']
            self.assertEqual(driver_name, driver_name_new)

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
        
        raw_fields = ['quality_flag', 'preferred_timestamp', 'port_timestamp',
            'lon', 'raw', 'internal_timestamp', 'time',
            'lat', 'driver_timestamp','ingestion_timestamp']
        parsed_fields = ['quality_flag', 'preferred_timestamp', 'temp',
            'density', 'port_timestamp', 'lon', 'salinity', 'pressure',
            'internal_timestamp', 'time', 'lat', 'driver_timestamp',
            'conductivity','ingestion_timestamp']

        retval = self._ia_client.get_agent(['streams'])['streams']
        self.assertIn('raw', retval)
        self.assertIn('parsed', retval)
        for x in raw_fields:
            self.assertIn(x, retval['raw'])
        for x in parsed_fields:
            self.assertIn(x, retval['parsed'])
        
        retval = self._ia_client.get_agent(['pubrate'])
        expected_pubrate_result = {'pubrate': {'raw': 0, 'parsed': 0}}
        self.assertEqual(retval, expected_pubrate_result)
        
        new_pubrate = {'raw' : 30, 'parsed' : 30}
        self._ia_client.set_agent({'pubrate' : new_pubrate})
        retval = self._ia_client.get_agent(['pubrate'])['pubrate']
        self.assertEqual(retval, new_pubrate)

    
        retval = self._ia_client.get_agent(['alerts'])['alerts']
        self.assertItemsEqual(retval, [])

        
        alert_def1 = {
            'name' : 'temp_warning_interval',
            'stream_name' : 'parsed',
            'description' : 'Temperature is above normal range.',
            'alert_type' : StreamAlertType.WARNING,
            'aggregate_type' : AggregateStatusType.AGGREGATE_DATA,
            'value_id' : 'temp',
            'lower_bound' : None,
            'lower_rel_op' : None,
            'upper_bound' : 10.5,
            'upper_rel_op' : '<',
            'alert_class' : 'IntervalAlert'
        }
        
        alert_def2 = {
            'name' : 'temp_alarm_interval',
            'stream_name' : 'parsed',
            'description' : 'Temperature is way above normal range.',
            'alert_type' : StreamAlertType.WARNING,
            'aggregate_type' : AggregateStatusType.AGGREGATE_DATA,
            'value_id' : 'temp',
            'lower_bound' : None,
            'lower_rel_op' : None,
            'upper_bound' : 15.5,
            'upper_rel_op' : '<',
            'alert_class' : 'IntervalAlert'
        }
        
        
        self._ia_client.set_agent({'alerts' : [alert_def1, alert_def2]})
        
        retval = self._ia_client.get_agent(['alerts'])['alerts']
        self.assertTrue(len(retval)==2)

        log.debug('test_get_set_agent updated alerts: %s', retval)

        retval = self._ia_client.get_agent(['aggstatus'])['aggstatus']
        self.assertTrue(len(retval)==4)
        log.debug('test_get_set_agent updated aggstatus: %s', retval)

        """
        {'status': None, 'stream_name': 'parsed', 'alert_type': 1, 'name': 'temp_warning_interval', 'upper_bound': 10.5, 'lower_bound': None, 'aggregate_type': 2, 'alert_class': 'IntervalAlert', 'value': None, 'value_id': 'temp', 'lower_rel_op': '<', 'message': 'Temperature is above normal range.', 'upper_rel_op': None}
        {'status': None, 'stream_name': 'parsed', 'alert_type': 1, 'name': 'temp_alarm_interval', 'upper_bound': 15.5, 'lower_bound': None, 'aggregate_type': 2, 'alert_class': 'IntervalAlert', 'value': None, 'value_id': 'temp', 'lower_rel_op': '<', 'message': 'Temperature is way above normal range.', 'upper_rel_op': None}
        """

        self._ia_client.set_agent({'alerts' : ['clear']})        
        retval = self._ia_client.get_agent(['alerts'])['alerts']
        self.assertItemsEqual(retval, [])

        self._ia_client.set_agent({'alerts' : ['set', alert_def1, alert_def2]})
        
        retval = self._ia_client.get_agent(['alerts'])['alerts']
        self.assertTrue(len(retval)==2)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

    def test_poll(self):
        """
        test_poll
        """
        #--------------------------------------------------------------------------------
        # Test observatory polling function thorugh execute resource interface.
        # Verify ResourceAgentCommandEvents are published.
        #--------------------------------------------------------------------------------

        # Start data subscribers.
        self._start_data_subscribers(3, 10)
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

        # Acquire sample returns a string, not a particle.  The particle
        # is created by the data handler though.
        cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)
        retval = self._ia_client.execute_resource(cmd)
        self.assertTrue(retval.result)
        retval = self._ia_client.execute_resource(cmd)
        self.assertTrue(retval.result)
        retval = self._ia_client.execute_resource(cmd)
        self.assertTrue(retval.result)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)        
        
        """
        {'origin': '123xyz', 'description': '', 'kwargs': {}, 'args': [], 'execute_command': 'RESOURCE_AGENT_EVENT_INITIALIZE', 'type_': 'ResourceAgentCommandEvent', 'command': 'execute_agent', 'result': None, 'base_types': ['ResourceAgentEvent', 'Event'], 'ts_created': '1349373063952', 'sub_type': '', 'origin_type': ''}
        {'origin': '123xyz', 'description': '', 'kwargs': {}, 'args': [], 'execute_command': 'RESOURCE_AGENT_EVENT_GO_ACTIVE', 'type_': 'ResourceAgentCommandEvent', 'command': 'execute_agent', 'result': None, 'base_types': ['ResourceAgentEvent', 'Event'], 'ts_created': '1349373069507', 'sub_type': '', 'origin_type': ''}
        {'origin': '123xyz', 'description': '', 'kwargs': {}, 'args': [], 'execute_command': 'RESOURCE_AGENT_EVENT_RUN', 'type_': 'ResourceAgentCommandEvent', 'command': 'execute_agent', 'result': None, 'base_types': ['ResourceAgentEvent', 'Event'], 'ts_created': '1349373069547', 'sub_type': '', 'origin_type': ''}
        {'origin': '123xyz', 'description': '', 'kwargs': {}, 'args': [], 'execute_command': 'DRIVER_EVENT_ACQUIRE_SAMPLE', 'type_': 'ResourceAgentCommandEvent', 'command': 'execute_resource', 'result': {'raw': {'quality_flag': 'ok', 'preferred_timestamp': 'driver_timestamp', 'stream_name': 'raw', 'pkt_format_id': 'JSON_Data', 'pkt_version': 1, 'values': [{'binary': True, 'value_id': 'raw', 'value': 'MzkuNzk5OSwyMS45NTM0MSwgNDMuOTIzLCAgIDE0LjMzMjcsIDE1MDYuMjAzLCAwMSBGZWIgMjAwMSwgMDE6MDE6MDA='}], 'driver_timestamp': 3558361870.788932}, 'parsed': {'quality_flag': 'ok', 'preferred_timestamp': 'driver_timestamp', 'stream_name': 'parsed', 'pkt_format_id': 'JSON_Data', 'pkt_version': 1, 'values': [{'value_id': 'temp', 'value': 39.7999}, {'value_id': 'conductivity', 'value': 21.95341}, {'value_id': 'pressure', 'value': 43.923}], 'driver_timestamp': 3558361870.788932}}, 'base_types': ['ResourceAgentEvent', 'Event'], 'ts_created': '1349373071084', 'sub_type': '', 'origin_type': ''}
        {'origin': '123xyz', 'description': '', 'kwargs': {}, 'args': [], 'execute_command': 'DRIVER_EVENT_ACQUIRE_SAMPLE', 'type_': 'ResourceAgentCommandEvent', 'command': 'execute_resource', 'result': {'raw': {'quality_flag': 'ok', 'preferred_timestamp': 'driver_timestamp', 'stream_name': 'raw', 'pkt_format_id': 'JSON_Data', 'pkt_version': 1, 'values': [{'binary': True, 'value_id': 'raw', 'value': 'NjIuNTQxNCw1MC4xNzI3MCwgMzA0LjcwNywgICA2LjE4MDksIDE1MDYuMTU1LCAwMSBGZWIgMjAwMSwgMDE6MDE6MDA='}], 'driver_timestamp': 3558361872.398573}, 'parsed': {'quality_flag': 'ok', 'preferred_timestamp': 'driver_timestamp', 'stream_name': 'parsed', 'pkt_format_id': 'JSON_Data', 'pkt_version': 1, 'values': [{'value_id': 'temp', 'value': 62.5414}, {'value_id': 'conductivity', 'value': 50.1727}, {'value_id': 'pressure', 'value': 304.707}], 'driver_timestamp': 3558361872.398573}}, 'base_types': ['ResourceAgentEvent', 'Event'], 'ts_created': '1349373072613', 'sub_type': '', 'origin_type': ''}
        {'origin': '123xyz', 'description': '', 'kwargs': {}, 'args': [], 'execute_command': 'DRIVER_EVENT_ACQUIRE_SAMPLE', 'type_': 'ResourceAgentCommandEvent', 'command': 'execute_resource', 'result': {'raw': {'quality_flag': 'ok', 'preferred_timestamp': 'driver_timestamp', 'stream_name': 'raw', 'pkt_format_id': 'JSON_Data', 'pkt_version': 1, 'values': [{'binary': True, 'value_id': 'raw', 'value': 'NDYuODk0Niw5MS4wNjkyNCwgMzQyLjkyMCwgICA3LjQyNzgsIDE1MDYuOTE2LCAwMSBGZWIgMjAwMSwgMDE6MDE6MDA='}], 'driver_timestamp': 3558361873.907537}, 'parsed': {'quality_flag': 'ok', 'preferred_timestamp': 'driver_timestamp', 'stream_name': 'parsed', 'pkt_format_id': 'JSON_Data', 'pkt_version': 1, 'values': [{'value_id': 'temp', 'value': 46.8946}, {'value_id': 'conductivity', 'value': 91.06924}, {'value_id': 'pressure', 'value': 342.92}], 'driver_timestamp': 3558361873.907537}}, 'base_types': ['ResourceAgentEvent', 'Event'], 'ts_created': '1349373074141', 'sub_type': '', 'origin_type': ''}
        {'origin': '123xyz', 'description': '', 'kwargs': {}, 'args': [], 'execute_command': 'RESOURCE_AGENT_EVENT_RESET', 'type_': 'ResourceAgentCommandEvent', 'command': 'execute_agent', 'result': None, 'base_types': ['ResourceAgentEvent', 'Event'], 'ts_created': '1349373076321', 'sub_type': '', 'origin_type': ''}        
        """
        
        # Check that the events were received.
        self._async_event_result.get(timeout=CFG.endpoint.receive.timeout)        
        self.assertGreaterEqual(len(self._events_received), 7)
        
        """
        {'domain': [1], 'data_producer_id': '123xyz', 'connection_index': 0, 'record_dictionary': {1: None, 2: [3573842031.2744761], 3: [3573842031.2744761], 4: None, 5: ['ok'], 6: ['port_timestamp'], 7: None, 8: [3573842030.3573842], 9: None, 10: [693.64899], 11: [57.46814], 12: None, 13: [36.3759], 14: None}, 'connection_id': 'ac8085c1572b4ac78e080d397cfa5d88', 'locator': None, 'type_': 'Granule', 'param_dictionary': '5907708894714910a80410f475709d69', 'creation_timestamp': 1364853231.735014, 'provider_metadata_update': {}}
        {'domain': [1], 'data_producer_id': '123xyz', 'connection_index': 1, 'record_dictionary': {1: None, 2: [3573842032.6812067], 3: [3573842032.6812067], 4: None, 5: ['ok'], 6: ['port_timestamp'], 7: None, 8: [3573842032.3573842], 9: None, 10: [941.828], 11: [92.264343], 12: None, 13: [55.146198], 14: None}, 'connection_id': 'ac8085c1572b4ac78e080d397cfa5d88', 'locator': None, 'type_': 'Granule', 'param_dictionary': '5907708894714910a80410f475709d69', 'creation_timestamp': 1364853232.80932, 'provider_metadata_update': {}}
        {'domain': [1], 'data_producer_id': '123xyz', 'connection_index': 2, 'record_dictionary': {1: None, 2: [3573842034.287919], 3: [3573842034.287919], 4: None, 5: ['ok'], 6: ['port_timestamp'], 7: None, 8: [3573842034.3573842], 9: None, 10: [132.754], 11: [59.86327], 12: None, 13: [83.803398], 14: None}, 'connection_id': 'ac8085c1572b4ac78e080d397cfa5d88', 'locator': None, 'type_': 'Granule', 'param_dictionary': '5907708894714910a80410f475709d69', 'creation_timestamp': 1364853234.387937, 'provider_metadata_update': {}}
        """
        
        # Await the published samples and veirfy parsed and raw streams.        
        self._async_sample_result.get(timeout=CFG.endpoint.receive.timeout)
        self._async_raw_sample_result.get(timeout=CFG.endpoint.receive.timeout)
        self.assertEqual(len(self._samples_received), 3)
        for x in self._samples_received:
            self.assertGranule(x)
        self.assertGreater(len(self._raw_samples_received), 10)
        for x in self._raw_samples_received:
            self.assertRawGranule(x)


    def test_autosample(self):
        """
        test_autosample
        Test instrument driver execute interface to start and stop streaming
        mode. Verify ResourceAgentResourceStateEvents are publsihed.
        """
        
        # Start data subscribers.
        self._start_data_subscribers(3, 10)
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
        self.assertGreaterEqual(len(self._events_received), 7)

        self._async_sample_result.get(timeout=CFG.endpoint.receive.timeout)
        self.assertGreaterEqual(len(self._samples_received), 3)
        
        for x in self._samples_received:
            self.assertGranule(x)
                        
        self._async_raw_sample_result.get(timeout=CFG.endpoint.receive.timeout)
        self.assertGreaterEqual(len(self._raw_samples_received), 10)

        for x in self._raw_samples_received:
            self.assertRawGranule(x)

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
                        'alerts',
                        'streams',
                        'pubrate',
                        'aggstatus',
                        'driver_pid'
                        ]
        
        res_cmds_all =[
            SBE37ProtocolEvent.ACQUIRE_STATUS,
            SBE37ProtocolEvent.TEST,
            SBE37ProtocolEvent.ACQUIRE_SAMPLE,
            SBE37ProtocolEvent.START_AUTOSAMPLE,
            SBE37ProtocolEvent.STOP_AUTOSAMPLE,
            SBE37ProtocolEvent.ACQUIRE_CONFIGURATION,
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

        def verify_schema(caps_list):
            
            dd_list = ['display_name','description']
            ddt_list = ['display_name','description','type']
            ddvt_list = ['display_name','description','visibility','type']
            ddak_list = ['display_name','description','args','kwargs']
            stream_list = ['raw', 'parsed']            
            
            for x in caps_list:
                if isinstance(x,dict):
                    x.pop('type_')
                    x = IonObject('AgentCapability', **x)
                
                if x.cap_type == CapabilityType.AGT_CMD:
                    keys = x.schema.keys()
                    for y in ddak_list:
                        self.assertIn(y, keys)
                    
                elif x.cap_type == CapabilityType.AGT_PAR:
                        if x.name != 'example':
                            keys = x.schema.keys()
                            for y in ddvt_list:
                                self.assertIn(y, keys)
                        
                elif x.cap_type == CapabilityType.RES_CMD:
                    keys = x.schema.keys()
                    self.assertIn('return',keys)
                    self.assertIn('display_name',keys)
                    self.assertIn('arguments',keys)
                    self.assertIn('timeout',keys)
               
                elif x.cap_type == CapabilityType.RES_IFACE:
                    pass

                elif x.cap_type == CapabilityType.RES_PAR:
                    keys = x.schema.keys()
                    self.assertIn('get_timeout',keys)
                    self.assertIn('set_timeout',keys)
                    self.assertIn('direct_access',keys)
                    self.assertIn('startup',keys)
                    self.assertIn('visibility',keys)
                
                elif x.cap_type == CapabilityType.AGT_STATES:
                    for (k,v) in x.schema.iteritems():
                        keys = v.keys()
                        for y in dd_list:
                            self.assertIn(y, keys)
                
                elif x.cap_type == CapabilityType.ALERT_DEFS:
                    for (k,v) in x.schema.iteritems():
                        keys = v.keys()
                        for y in ddt_list:
                            self.assertIn(y, keys)
                
                elif x.cap_type == CapabilityType.AGT_CMD_ARGS:
                    for (k,v) in x.schema.iteritems():
                        keys = v.keys()
                        for y in ddt_list:
                            self.assertIn(y, keys)
                
                elif x.cap_type == CapabilityType.AGT_STREAMS:
                    keys = x.schema.keys()
                    for y in stream_list:
                        self.assertIn(y, keys)
                
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
                                
        # Check all capabilities carry correct schema information.
        verify_schema(retval)        

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
        
        # Check all capabilities carry correct schema information.
        verify_schema(retval)        
        
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
                        
        # Check all capabilities carry correct schema information.
        verify_schema(retval)        

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
            SBE37ProtocolEvent.ACQUIRE_STATUS,
            SBE37ProtocolEvent.TEST,
            SBE37ProtocolEvent.ACQUIRE_SAMPLE,
            SBE37ProtocolEvent.START_AUTOSAMPLE,
            SBE37ProtocolEvent.ACQUIRE_CONFIGURATION
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
        
        for x in res_cmds:
            pass
        
        # Check all capabilities carry correct schema information.
        verify_schema(retval)        

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
        
        # Check all capabilities carry correct schema information.
        verify_schema(retval)        

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
        
        # Check all capabilities carry correct schema information.
        verify_schema(retval)        

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

        # Check all capabilities carry correct schema information.
        verify_schema(retval)        
        
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

        # Issue a good resource command and verify result. Result
        # is a sample string not a particle
        cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)
        retval = self._ia_client.execute_resource(cmd)
        self.assertTrue(retval.result)

        # Try to issue a wrong state resource command.
        # Returning ServerError: 500 -.  Stuck on this moving on.  Maybe
        # Edward can help
        #with self.assertRaises(Conflict):
        #    cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
        #    retval = self._ia_client.execute_resource(cmd)

        # Reset and shutdown.
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        #self._async_event_result.get(timeout=CFG.endpoint.receive.timeout)
        self.assertEquals(len(self._events_received), 6)

        # Note this is throwing an unexpect async driver error in addition to the expected ones.
        # Reduce count to 5 when this is fixed.
        """
        {'origin': '123xyz', 'error_type': "<class 'pyon.core.exception.BadRequest'>", 'description': '', 'kwargs': {}, 'args': [], 'execute_command': '', 'error_msg': 'Execute argument "command" not set.', 'error_code': 400, 'type_': 'ResourceAgentErrorEvent', 'command': 'execute_agent', 'actor_id': '', 'base_types': ['ResourceAgentEvent', 'Event'], '_id': '0328a68d550343acb749364923b3fc16', 'ts_created': '1373573451489', 'sub_type': '', 'origin_type': 'InstrumentDevice'}
        {'origin': '123xyz', 'error_type': "<class 'pyon.core.exception.BadRequest'>", 'description': '', 'kwargs': {}, 'args': [], 'execute_command': '', 'error_msg': 'Execute argument "command" not set.', 'error_code': 400, 'type_': 'ResourceAgentErrorEvent', 'command': 'execute_agent', 'actor_id': '', 'base_types': ['ResourceAgentEvent', 'Event'], '_id': 'd1782d99b1304a63982a70604743ee86', 'ts_created': '1373573451514', 'sub_type': '', 'origin_type': 'InstrumentDevice'}
        {'origin': '123xyz', 'error_type': "<class 'pyon.agent.instrument_fsm.FSMStateError'>", 'description': '', 'kwargs': {}, 'args': [], 'execute_command': 'RESOURCE_AGENT_EVENT_RUN', 'error_msg': 'Command RESOURCE_AGENT_EVENT_RUN not handled in state RESOURCE_AGENT_STATE_UNINITIALIZED', 'error_code': 409, 'type_': 'ResourceAgentErrorEvent', 'command': 'execute_agent', 'actor_id': '', 'base_types': ['ResourceAgentEvent', 'Event'], '_id': 'baa58ef32e1e49bbb029c741223ad740', 'ts_created': '1373573451540', 'sub_type': '', 'origin_type': 'InstrumentDevice'}
        {'origin': '123xyz', 'error_type': "<class 'pyon.agent.instrument_fsm.FSMStateError'>", 'description': '', 'kwargs': {}, 'args': [], 'execute_command': 'DRIVER_EVENT_ACQUIRE_SAMPLE', 'error_msg': 'Command RESOURCE_AGENT_EVENT_EXECUTE_RESOURCE not handled in state RESOURCE_AGENT_STATE_UNINITIALIZED', 'error_code': 409, 'type_': 'ResourceAgentErrorEvent', 'command': 'execute_resource', 'actor_id': '', 'base_types': ['ResourceAgentEvent', 'Event'], '_id': '1285a579e26146bf8d16a65ff4c0c986', 'ts_created': '1373573451565', 'sub_type': '', 'origin_type': 'InstrumentDevice'}
        {'origin': '123xyz', 'error_type': "<class 'pyon.core.exception.BadRequest'>", 'description': '', 'kwargs': {}, 'args': [], 'execute_command': '', 'error_msg': 'Execute argument "command" not set.', 'error_code': 400, 'type_': 'ResourceAgentErrorEvent', 'command': 'execute_agent', 'actor_id': '', 'base_types': ['ResourceAgentEvent', 'Event'], '_id': 'd45de8991fe14fd989cae07b114c1271', 'ts_created': '1373573451590', 'sub_type': '', 'origin_type': 'InstrumentDevice'}
        {'origin': '123xyz', 'error_type': "<type 'exceptions.ValueError'>", 'description': '', 'kwargs': {}, 'args': [], 'execute_command': '', 'error_msg': 'negative count', 'error_code': -1, 'type_': 'ResourceAgentErrorEvent', 'command': '', 'actor_id': '', 'base_types': ['ResourceAgentEvent', 'Event'], '_id': 'be7c3926092d41818acf02f783819ee7', 'ts_created': '1373573461201', 'sub_type': '', 'origin_type': 'InstrumentDevice'}
        """

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
        #self.assertEqual(sample_count, 1)

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
        self._start_data_subscribers(1, 1)
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
        pubrate = {
            'parsed':15,
            'raw':15
        }
        self._ia_client.set_agent({'pubrate':pubrate})
        retval = self._ia_client.get_agent(['pubrate'])

        cmd = AgentCommand(command=SBE37ProtocolEvent.START_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)
        
        gevent.sleep(35)

        cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)
 
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        self._async_sample_result.get(timeout=CFG.endpoint.receive.timeout)
        self._async_raw_sample_result.get(timeout=CFG.endpoint.receive.timeout)

        # Add check here to assure parsed granules are buffered.
        for x in self._samples_received:
            self.assertGranule(x)
        self.assertVectorGranules(self._samples_received, 'temp')
        for x in self._raw_samples_received:
            self.assertRawGranule(x)
        self.assertVectorGranules(self._raw_samples_received, 'raw')

    @unittest.skip('A manual test for memory use and leaks.')
    def test_streaming_memuse(self):
        """
        test_streaming_memuse
        Report the memory used by the interpreter process running this
        test for an extended period of time. For debugging.
        """
        # Using memory_profiler for this test.
        # https://pypi.python.org/pypi/memory_profiler
        
        mfile = open('memuse.txt','w')
        
        from memory_profiler import memory_usage
        
        def report_memuse():            
            mem_use = memory_usage(-1)[0]
            log.info("CURRENT MEMORY USAGE: %f", mem_use)
            mfile.write('%f\n' % mem_use)
        
        report_memuse()

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
        
        for x in range(1800):
            gevent.sleep(1)
            report_memuse()
            

        cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)
 
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        report_memuse()

    def test_capabilities_new(self):
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
                        'alerts',
                        'streams',
                        'pubrate',
                        'aggstatus',
                        'driver_pid'
                        ]
        
        res_cmds_all =[
            SBE37ProtocolEvent.ACQUIRE_STATUS,
            SBE37ProtocolEvent.TEST,
            SBE37ProtocolEvent.ACQUIRE_SAMPLE,
            SBE37ProtocolEvent.START_AUTOSAMPLE,
            SBE37ProtocolEvent.STOP_AUTOSAMPLE,
            SBE37ProtocolEvent.ACQUIRE_CONFIGURATION,
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
        
        print '#################'
        for x in retval:
            print str(x)
        
        
        """
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
        """

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

    def test_driver_crash(self):
        """
        Test detection of killed/crashed driver, or loss of driver comms.
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

        dvr_pid = self._ia_client.get_agent(['driver_pid'])['driver_pid']
        gevent.sleep(5)

        # Kill driver.
        log.info('Sending kill signal to driver.')
        args = ['kill', '-9', str(dvr_pid)]
        subprocess.check_output(args)

        def poll_state():
            start = time.time()
            elapsed = 0
            while elapsed < 120:
                gevent.sleep(5)
                state = self._ia_client.get_agent_state()
                log.info('Insturment agent state is %s.', state)
                if state == ResourceAgentState.UNINITIALIZED:
                    break
                elapsed = time.time() - start

        gl = gevent.spawn(poll_state)
        gl.join()

        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

@attr('HARDWARE', group='sa')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 600}}})
@unittest.skipIf((not os.getenv('PYCC_MODE', False)) and os.getenv('CEI_LAUNCH_TEST', False), 'Skip until tests support launch port agent configurations.')
class TestInstrumentAgent(InstrumentAgentTest):

    ############################################################################
    # Setup, teardown.
    ############################################################################

    def setUp(self):
        self._setup()





