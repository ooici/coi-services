#!/usr/bin/env python

"""
@package ion.agents.instrument.test.test_ia_alarms
@file ion/agents.instrument/test_ia_alarms.py
@author Edward Hunter
@brief Test cases for IA alarms, particularly preloaded configs.
"""

__author__ = 'Edward Hunter'
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
from pyon.core.bootstrap import get_obj_registry
from pyon.core.object import IonObjectDeserializer

# Pyon exceptions.
from pyon.core.exception import BadRequest
from pyon.core.exception import Conflict

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
from interface.objects import StreamAlarmType

"""
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_ia_alarms.py:TestIAAlarms
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_ia_alarms.py:TestIAAlarms.test_config
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_ia_alarms.py:TestIAAlarms.test_autosample
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
DRV_URI = 'http://sddevrepo.oceanobservatories.org/releases/seabird_sbe37smb_ooicore-0.0.4-py2.7.egg'
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

#Refactored as stand alone method for starting an instrument agent for use in other tests, like governance
#to do policy testing for resource agents
#shenrie
def start_instrument_agent_process(container, stream_config={}, resource_id=IA_RESOURCE_ID, resource_name=IA_NAME, org_name=None, message_headers=None):
    log.info("foobar")

    # Create agent config.
    agent_config = {
        'driver_config' : DVR_CONFIG,
        'stream_config' : stream_config,
        'agent'         : {'resource_id': resource_id},
        'test_mode' : True
    }

    if org_name is not None:
        agent_config['org_name'] = org_name


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

@attr('HARDWARE', group='mi')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 120}}})
class TestIAAlarms(IonIntegrationTestCase):
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

    def _start_event_subscriber(self, type='StreamAlarmEvent', count=0):
        """
        Start a subscriber to the instrument agent events.
        @param type The type of event to catch.
        @count Trigger the async event result when events received reaches this.
        """
        def consume_event(*args, **kwargs):
            print '#################'
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
        # Create streams and subscriptions for each stream named in driver.
        self._stream_config = {}

        streams = {
            'parsed' : 'ctd_parsed_param_dict',
            'raw'    : 'ctd_raw_param_dict'
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
            
            if stream_name == 'parsed':
                
                type = 'IntervalAlarmDef'
                kwargs = {
                    'name' : 'test_sim_warning',
                    'stream_name' : 'parsed',
                    'value_id' : 'temp',
                    'message' : 'Temperature is above test range of 5.0.',
                    'type' : StreamAlarmType.WARNING,
                    'upper_bound' : 5.0,
                    'upper_rel_op' : '<'
                }
                alarm = {}
                alarm['type'] = type
                alarm['kwargs'] = kwargs
                alarms = [alarm]
                stream_config['alarms'] = alarms
            
            self._stream_config[stream_name] = stream_config

    def _start_data_subscribers(self, count):
        """
        """        
        # Create a pubsub client to create streams.
        pubsub_client = PubsubManagementServiceClient(node=self.container.node)
                
        # Create streams and subscriptions for each stream named in driver.
        self._data_subscribers = []
        self._samples_received = []
        self._async_sample_result = AsyncResult()

        # A callback for processing subscribed-to data.
        def recv_data(message, stream_route, stream_id):
            log.info('Received message on %s (%s,%s)', stream_id, stream_route.exchange_point, stream_route.routing_key)
            #print '######################## stream message:'
            #print str(message)
            self._samples_received.append(message)
            if len(self._samples_received) == count:
                self._async_sample_result.set()

        for (stream_name, stream_config) in self._stream_config.iteritems():
            
            if stream_name == 'parsed':
                
                # Create subscription for parsed stream only.

                stream_id = stream_config['stream_id']
                
                from pyon.util.containers import create_unique_identifier
                # exchange_name = '%s_queue' % stream_name
                exchange_name = create_unique_identifier("%s_queue" %
                        stream_name)
                self._purge_queue(exchange_name)
                sub = StandaloneStreamSubscriber(exchange_name, recv_data)
                sub.start()
                self._data_subscribers.append(sub)
                print 'stream_id: %s' % stream_id
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
    # Tests.
    ###############################################################################
    
    def test_config(self):
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

        decoder = IonObjectDeserializer(obj_registry=get_obj_registry())

        # Grab the alarms defined in the config.
        retval = decoder.deserialize(self._ia_client.get_agent(['alarms'])['alarms'])

        """
        {'status': None, 'stream_name': 'parsed', 'name': 'test_sim_warning',
        'upper_bound': 5.0, 'expr': 'x<5.0', 'upper_rel_op': '<',
        'lower_rel_op': None, 'type_': 'IntervalAlarmDef', 'value_id': 'temp',
        'lower_bound': None, 'message': 'Temperature is above test range of 5.0.',
        'current_val': None, 'type': 1}
        """
        self.assertEqual(retval[0].type_, 'IntervalAlarmDef')
        self.assertEqual(retval[0].upper_bound, 5.0)
        self.assertEqual(retval[0].expr, 'x<5.0')
        
        # Reset the agent. This causes the driver messaging to be stopped,
        # the driver process to end and switches us back to uninitialized.
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
        
        
    def test_autosample(self):
        """
        test_autosample
        Test instrument driver execute interface to start and stop streaming
        mode. Verify ResourceAgentResourceStateEvents are publsihed.
        """
        
        # Start data subscribers.
        self._start_data_subscribers(5)
        self.addCleanup(self._stop_data_subscribers)    
        
        # Set up a subscriber to collect error events.
        self._start_event_subscriber()
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
        
        gevent.sleep(20)
        
        cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)
 
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        #self._async_event_result.get(timeout=CFG.endpoint.receive.timeout)
        #self.assertGreaterEqual(len(self._events_received), 6)

        self._async_sample_result.get(timeout=CFG.endpoint.receive.timeout)
        self.assertGreaterEqual(len(self._samples_received), 5)

        #for x in self._samples_received:
            

        gevent.sleep(5)
        
        """
        {'domain': [1], 'data_producer_id': '123xyz', 'record_dictionary': {1: array([  3.57142753e+09]), 2: array([  3.57142753e+09]), 3: array([port_timestamp], dtype=object), 4: 'ok', 5: None, 6: None, 7: array([ 83.66190338], dtype=float32), 8: array([ 27.09519958], dtype=float32), 9: array([ 495.5369873], dtype=float32), 10: None, 11: array([  3.57142753e+09]), 12: None, 13: None}, 'locator': None, 'type_': 'Granule', 'param_dictionary': '529b995c1aee4d149bca755aba7de687', 'creation_timestamp': 1362438731.584241, 'provider_metadata_update': {}}
        {'domain': [1], 'data_producer_id': '123xyz', 'record_dictionary': {1: array([  3.57142753e+09]), 2: array([  3.57142753e+09]), 3: array([port_timestamp], dtype=object), 4: 'ok', 5: None, 6: None, 7: array([ 50.97378922], dtype=float32), 8: array([ 17.82060051], dtype=float32), 9: array([ 280.375], dtype=float32), 10: None, 11: array([  3.57142753e+09]), 12: None, 13: None}, 'locator': None, 'type_': 'Granule', 'param_dictionary': '529b995c1aee4d149bca755aba7de687', 'creation_timestamp': 1362438734.120577, 'provider_metadata_update': {}}
        {'domain': [1], 'data_producer_id': '123xyz', 'record_dictionary': {1: array([  3.57142754e+09]), 2: array([  3.57142754e+09]), 3: array([port_timestamp], dtype=object), 4: 'ok', 5: None, 6: None, 7: array([ 79.61238098], dtype=float32), 8: array([ 99.90670013], dtype=float32), 9: array([ 830.60198975], dtype=float32), 10: None, 11: array([  3.57142754e+09]), 12: None, 13: None}, 'locator': None, 'type_': 'Granule', 'param_dictionary': '529b995c1aee4d149bca755aba7de687', 'creation_timestamp': 1362438737.154774, 'provider_metadata_update': {}}
        {'domain': [1], 'data_producer_id': '123xyz', 'record_dictionary': {1: array([  3.57142754e+09]), 2: array([  3.57142754e+09]), 3: array([port_timestamp], dtype=object), 4: 'ok', 5: None, 6: None, 7: array([ 81.31384277], dtype=float32), 8: array([ 89.92569733], dtype=float32), 9: array([ 575.98901367], dtype=float32), 10: None, 11: array([  3.57142754e+09]), 12: None, 13: None}, 'locator': None, 'type_': 'Granule', 'param_dictionary': '529b995c1aee4d149bca755aba7de687', 'creation_timestamp': 1362438739.68893, 'provider_metadata_update': {}}
        {'domain': [1], 'data_producer_id': '123xyz', 'record_dictionary': {1: array([  3.57142754e+09]), 2: array([  3.57142754e+09]), 3: array([port_timestamp], dtype=object), 4: 'ok', 5: None, 6: None, 7: array([ 80.85481262], dtype=float32), 8: array([ 86.46209717], dtype=float32), 9: array([ 563.9329834], dtype=float32), 10: None, 11: array([  3.57142754e+09]), 12: None, 13: None}, 'locator': None, 'type_': 'Granule', 'param_dictionary': '529b995c1aee4d149bca755aba7de687', 'creation_timestamp': 1362438742.722411, 'provider_metadata_update': {}}
        {'domain': [1], 'data_producer_id': '123xyz', 'record_dictionary': {1: array([  3.57142755e+09]), 2: array([  3.57142754e+09]), 3: array([port_timestamp], dtype=object), 4: 'ok', 5: None, 6: None, 7: array([ 42.43975067], dtype=float32), 8: array([ 13.87370014], dtype=float32), 9: array([ 910.49298096], dtype=float32), 10: None, 11: array([  3.57142755e+09]), 12: None, 13: None}, 'locator': None, 'type_': 'Granule', 'param_dictionary': '529b995c1aee4d149bca755aba7de687', 'creation_timestamp': 1362438745.256714, 'provider_metadata_update': {}}
        {'domain': [1], 'data_producer_id': '123xyz', 'record_dictionary': {1: array([  3.57142755e+09]), 2: array([  3.57142755e+09]), 3: array([port_timestamp], dtype=object), 4: 'ok', 5: None, 6: None, 7: array([ 76.90184021], dtype=float32), 8: array([ 84.85610199], dtype=float32), 9: array([ 742.34002686], dtype=float32), 10: None, 11: array([  3.57142755e+09]), 12: None, 13: None}, 'locator': None, 'type_': 'Granule', 'param_dictionary': '529b995c1aee4d149bca755aba7de687', 'creation_timestamp': 1362438748.29093, 'provider_metadata_update': {}}
        {'domain': [1], 'data_producer_id': '123xyz', 'record_dictionary': {1: array([  3.57142755e+09]), 2: array([  3.57142755e+09]), 3: array([port_timestamp], dtype=object), 4: 'ok', 5: None, 6: None, 7: array([ 92.28489685], dtype=float32), 8: array([ 19.77809906], dtype=float32), 9: array([ 272.94500732], dtype=float32), 10: None, 11: array([  3.57142755e+09]), 12: None, 13: None}, 'locator': None, 'type_': 'Granule', 'param_dictionary': '529b995c1aee4d149bca755aba7de687', 'creation_timestamp': 1362438750.841668, 'provider_metadata_update': {}}
        {'domain': [1], 'data_producer_id': '123xyz', 'record_dictionary': {1: array([  3.57142755e+09]), 2: array([  3.57142755e+09]), 3: array([port_timestamp], dtype=object), 4: 'ok', 5: None, 6: None, 7: array([ 60.02323151], dtype=float32), 8: array([-7.79680014], dtype=float32), 9: array([ 418.51400757], dtype=float32), 10: None, 11: array([  3.57142755e+09]), 12: None, 13: None}, 'locator': None, 'type_': 'Granule', 'param_dictionary': '529b995c1aee4d149bca755aba7de687', 'creation_timestamp': 1362438753.424074, 'provider_metadata_update': {}}
        {'domain': [1], 'data_producer_id': '123xyz', 'record_dictionary': {1: array([  3.57142756e+09]), 2: array([  3.57142756e+09]), 3: array([port_timestamp], dtype=object), 4: 'ok', 5: None, 6: None, 7: array([ 13.54555035], dtype=float32), 8: array([ 90.14240265], dtype=float32), 9: array([ 105.24099731], dtype=float32), 10: None, 11: array([  3.57142756e+09]), 12: None, 13: None}, 'locator': None, 'type_': 'Granule', 'param_dictionary': '529b995c1aee4d149bca755aba7de687', 'creation_timestamp': 1362438756.511647, 'provider_metadata_update': {}}
        #######################
        {'origin': '123xyz', 'stream_name': 'parsed', 'type': 'StreamAlaramType.WARNING', 'description': '', 'expr': 'x<5.0', 'value': 27.0952, 'type_': 'StreamWarningAlaramEvent', 'value_id': 'temp', 'base_types': ['StreamAlarmEvent', 'ResourceEvent', 'Event'], 'message': 'Temperature is above test range of 5.0.', '_id': '838337bd75ad4852afa15aac29c82a4c', 'ts_created': '1362438731572', 'sub_type': '', 'origin_type': 'InstrumentDevice', 'name': 'test_sim_warning'}
        {'origin': '123xyz', 'stream_name': 'parsed', 'type': 'StreamAlaramType.ALL_CLEAR', 'description': '', 'expr': 'x<5.0', 'value': -7.7968, 'type_': 'StreamAllClearAlarmEvent', 'value_id': 'temp', 'base_types': ['StreamAlarmEvent', 'ResourceEvent', 'Event'], 'message': 'Alarm is cleared.', '_id': '6a3dc641a3fd4092a203857cf6a70e0c', 'ts_created': '1362438753412', 'sub_type': '', 'origin_type': 'InstrumentDevice', 'name': 'test_sim_warning'}
        {'origin': '123xyz', 'stream_name': 'parsed', 'type': 'StreamAlaramType.WARNING', 'description': '', 'expr': 'x<5.0', 'value': 90.1424, 'type_': 'StreamWarningAlaramEvent', 'value_id': 'temp', 'base_types': ['StreamAlarmEvent', 'ResourceEvent', 'Event'], 'message': 'Temperature is above test range of 5.0.', '_id': 'd45d95a3d94345faa1cf82cb18398f3b', 'ts_created': '1362438756499', 'sub_type': '', 'origin_type': 'InstrumentDevice', 'name': 'test_sim_warning'}
        2013-03-04 15:13:00,746 INFO Dummy-1 pyon.event.event:259 EventSubscriber stopped. Event pattern=#.StreamAlarmEvent.#.*.#.*.123xyz
        """
        
        """
        {'domain': [1],
        'data_producer_id': '123xyz',
        'record_dictionary':
            {1: array([  3.57142756e+09]),
             2: array([  3.57142756e+09]),
             3: array([port_timestamp], dtype=object),
             4: 'ok',
             5: None,
             6: None,
             7: array([ 13.54555035], dtype=float32),
             8: array([ 90.14240265], dtype=float32),
             9: array([ 105.24099731], dtype=float32),
             10: None,
             11: array([  3.57142756e+09]),
             12: None, 13: None},
        'locator': None,
        'type_': 'Granule',
        'param_dictionary': '529b995c1aee4d149bca755aba7de687',
        'creation_timestamp': 1362438756.511647,
        'provider_metadata_update': {}}
        """
        
        print '#######################'
        print '#######################'
        print '#######################'
        for x in self._samples_received:
            #print str(x)
            print str(x.record_dictionary)
            #print str(type(x.record_dictionary))
            print str(x.param_dictionary)
            #print str(x['record_dictionary'][8])
            
        print '#######################'
        for x in self._events_received:
            print str(x)
            