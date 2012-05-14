#!/usr/bin/env python

"""
@package ion.agents.data.test.test_external_dataset_agent
@file ion/agents/data/test/test_external_dataset_agent.py
@author Tim Giguere
@author Christopher Mueller
@brief Test cases for R2 ExternalDatasetAgent

# bin/nosetests -s -v --nologcapture ion.agents.data.test.test_external_dataset_agent:TestExternalDatasetAgent.test_acquire_data
# bin/nosetests -s -v --nologcapture ion.agents.data.test.test_external_dataset_agent:TestExternalDatasetAgent.test_acquire_data_while_streaming
# bin/nosetests -s -v --nologcapture ion.agents.data.test.test_external_dataset_agent:TestExternalDatasetAgent.test_acquire_sample
# bin/nosetests -s -v --nologcapture ion.agents.data.test.test_external_dataset_agent:TestExternalDatasetAgent.test_streaming
# bin/nosetests -s -v --nologcapture ion.agents.data.test.test_external_dataset_agent:TestExternalDatasetAgent.test_observatory
# bin/nosetests -s -v --nologcapture ion.agents.data.test.test_external_dataset_agent:TestExternalDatasetAgent.test_get_set_param
# bin/nosetests -s -v --nologcapture ion.agents.data.test.test_external_dataset_agent:TestExternalDatasetAgent.test_initialize
# bin/nosetests -s -v --nologcapture ion.agents.data.test.test_external_dataset_agent:TestExternalDatasetAgent.test_states
# bin/nosetests -s -v --nologcapture ion.agents.data.test.test_external_dataset_agent:TestExternalDatasetAgent.test_capabilities
# bin/nosetests -s -v --nologcapture ion.agents.data.test.test_external_dataset_agent:TestExternalDatasetAgent.test_errors

"""

# Import pyon first for monkey patching.
from pyon.public import log, CFG
from pyon.core.exception import InstParameterError
# Standard imports.

# 3rd party imports.
from gevent import spawn
from gevent.event import AsyncResult
import gevent
from nose.plugins.attrib import attr
from mock import patch

# ION imports.
from interface.objects import StreamQuery
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from pyon.public import StreamSubscriberRegistrar
from prototype.sci_data.stream_defs import ctd_stream_definition
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand
from pyon.util.containers import get_safe
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from pyon.event.event import EventSubscriber

# MI imports
from ion.services.mi.instrument_agent import InstrumentAgentState

from ion.agents.data.handlers.base_data_handler import DataHandlerParameter

# todo: rethink this
from ion.agents.data.handlers.base_data_handler import PACKET_CONFIG

from pyon.ion.granule.taxonomy import TaxyTool

import unittest


#########################
# For Validation Purposes
#########################

# Used to validate param config retrieved from driver.
PARAMS = {
    'POLLING_INTERVAL':int,
}

# To validate the list of resource commands
CMDS = [
    'acquire_data',
    'acquire_sample',
    'start_autosample',
    'stop_autosample'
]

# To validate the list of agent commands
AGT_CMDS = [
    'clear',
    'end_transaction',
    'get_current_state',
    'go_active',
    'go_direct_access',
    'go_inactive',
    'go_observatory',
    'go_streaming',
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

@attr('INT', group='eoi')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class ExternalDatasetAgentTestBase(object):

    # Agent parameters.
    EDA_RESOURCE_ID = '123xyz'
    EDA_NAME = 'ExampleEDA'
    EDA_MOD = 'ion.agents.data.external_dataset_agent'
    EDA_CLS = 'ExternalDatasetAgent'


    """
    Test cases for instrument agent class. Functions in this class provide
    instrument agent integration tests and provide a tutorial on use of
    the agent setup and interface.
    """
    def setUp(self):
        """
        Initialize test members.
        Start port agent.
        Start container and client.
        Start streams and subscribers.
        Start agent, client.
        """

        # Start port agent, add stop to cleanup.
#        self._pagent = None
#        self._start_pagent()
#        self.addCleanup(self._stop_pagent)

#        log.warn('Starting the container')
        # Start container.
        self._start_container()

        # Bring up services in a deploy file (no need to message)
#        self.container.start_rel_from_url('res/deploy/r2dm.yml')
#        self.container.start_rel_from_url('res/deploy/r2eoi.yml')
#        log.warn('Starting the rel')
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Create a pubsub client to create streams.
#        log.warn('Init a pubsub client')
        self._pubsub_client = PubsubManagementServiceClient(node=self.container.node)
#        log.warn('Init a ContainerAgentClient')
        self._container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)

#        # Define stream_config.
#        self._stream_config = {}

        # Sample async and subscription
#        self._no_samples = None
#        self._async_data_result = AsyncResult()
#        self._data_greenlets = []
#        self._samples_received = []
#        self._data_subscribers = []
#        # This assigns self._stream_config based on the contents of PACKET_CONFIG and starts subscribers on those streams
#        self._start_data_subscribers()
#        self.addCleanup(self._stop_data_subscribers)

        # Event async and subscription
#        self._no_events = None
#        self._async_event_result = AsyncResult()
#        self._events_received = []
#        self._event_subscribers = []
#        self._start_event_subscribers()
#        self.addCleanup(self._stop_event_subscribers)

        # Data async and subscription  TODO: Replace with new subscriber
        self._finished_count = None
        #TODO: Switch to gevent.queue.Queue
        self._async_finished_result = AsyncResult()
        self._finished_events_received = []
        self._finished_event_subscriber = None
        log.warn('Init finished event subscriber')
        self._start_finished_event_subscriber()
        self.addCleanup(self._stop_finished_event_subscriber)

        # TODO: Finish dealing with the resources and whatnot
        # TODO: DVR_CONFIG and (potentially) stream_config could both be reconfigured in self._setup_resources()
        self._setup_resources()

        #TG: Setup/configure the granule logger to log granules as they're published

        # Create agent config.
        agent_config = {
            'driver_config' : self.DVR_CONFIG,
            'stream_config' : {},
            'agent'         : {'resource_id': self.EDA_RESOURCE_ID},
            'test_mode' : True
        }

        # Start instrument agent.
        self._ia_pid = None
        log.debug('TestInstrumentAgent.setup(): starting EDA.')
        self._ia_pid = self._container_client.spawn_process(
            name=self.EDA_NAME,
            module=self.EDA_MOD,
            cls=self.EDA_CLS,
            config=agent_config
        )
        log.info('Agent pid=%s.', str(self._ia_pid))

        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = None
        self._ia_client = ResourceAgentClient(self.EDA_RESOURCE_ID, process=FakeProcess())
        log.info('Got ia client %s.', str(self._ia_client))

    ########################################
    # Private "setup" functions
    ########################################

    def _setup_resources(self):
        raise NotImplementedError('_setup_resources must be implemented in the subclass')

    def create_stream_and_logger(self, name, stream_id=''):
        if not stream_id or stream_id is '':
            stream_id = self._pubsub_client.create_stream(name=name, encoding='ION R2')

        pid = self._container_client.spawn_process(
            name=name+'_logger',
            module='ion.processes.data.stream_granule_logger',
            cls='StreamGranuleLogger',
            config={'process':{'stream_id':stream_id}}
        )
        log.warn('Started StreamGranuleLogger \'{0}\' subscribed to stream_id={1}'.format(pid, stream_id))

        return stream_id

    def _start_data_subscribers(self):
        """
        """
        # A callback for processing subscribed-to data.
        def consume_data(message, headers):
            log.info('Subscriber received data message: %s.', str(message))
            self._samples_received.append(message)
            if self._no_samples and self._no_samples == len(self._samples_received):
                self._async_data_result.set()

        # Create a stream subscriber registrar to create subscribers.
        subscriber_registrar = StreamSubscriberRegistrar(process=self.container,
            node=self.container.node)

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
            log.info('Test received ION event: args=%s  kwargs=%s.', ''.join([str(x)+' ' for x in args]), str(kwargs))
            self._events_received.append(args[0])
            if self._no_events and self._no_events == len(self._event_received):
                self._async_event_result.set()

        event_sub = EventSubscriber(event_type="DeviceEvent", callback=consume_event)
        event_sub.activate()
        self._event_subscribers.append(event_sub)

    def _stop_event_subscribers(self):
        """
        Stop event subscribers on cleanup.
        """
        for sub in self._event_subscribers:
            sub.deactivate()

    def _start_finished_event_subscriber(self):

        def consume_event(*args,**kwargs):
            if args[0].description == 'TestingFinished':
                log.debug('TestingFinished event received')
                self._finished_events_received.append(args[0])
                if self._finished_count and self._finished_count == len(self._finished_events_received):
                    log.debug('Finishing test...')
                    self._async_finished_result.set()
                    log.debug('Called self._async_finished_result.set()')

        self._finished_event_subscriber = EventSubscriber(event_type='DeviceEvent', callback=consume_event)
        self._finished_event_subscriber.activate()

    def _stop_finished_event_subscriber(self):
        if self._finished_event_subscriber:
            self._finished_event_subscriber.deactivate()
            self._finished_event_subscriber = None


    ########################################
    # Custom assertion functions
    ########################################

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


    ########################################
    # Test functions
    ########################################

    def test_acquire_data(self):
        cmd=AgentCommand(command='initialize')
        _ = self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command='go_active')
        _ = self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command='run')
        _ = self._ia_client.execute_agent(cmd)

        self._finished_count = 3

        config = get_safe(self.DVR_CONFIG, 'dh_cfg', {})

        log.info('Send an unconstrained request for data (\'new data\')')
        cmd = AgentCommand(command='acquire_data')
        self._ia_client.execute(cmd)

        log.info('Send a second unconstrained request for data (\'new data\'), should be rejected')
        cmd = AgentCommand(command='acquire_data')
        self._ia_client.execute(cmd)

        log.info('Send a constrained request for data: constraints = HIST_CONSTRAINTS_1')
        config['stream_id'] = self.create_stream_and_logger(name='stream_id_for_historical_1')
        config['constraints']=self.HIST_CONSTRAINTS_1
        cmd = AgentCommand(command='acquire_data', args=[config])
        self._ia_client.execute(cmd)

        log.info('Send a second constrained request for data: constraints = HIST_CONSTRAINTS_2')
        config['stream_id'] = self.create_stream_and_logger(name='stream_id_for_historical_2')
        config['constraints']=self.HIST_CONSTRAINTS_2
#        config={'stream_id':'second_historical','TESTING':True, 'constraints':self.HIST_CONSTRAINTS_2}
        cmd = AgentCommand(command='acquire_data', args=[config])
        self._ia_client.execute(cmd)

        self._async_finished_result.get(timeout=10)
        self.assertEqual(len(self._finished_events_received),self._finished_count)

        cmd = AgentCommand(command='reset')
        _ = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

    def test_acquire_data_while_streaming(self):
        # Test instrument driver execute interface to start and stop streaming mode.
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        # Make sure the polling interval is appropriate for a test
        params = {
            'POLLING_INTERVAL':5
        }
        self._ia_client.set_param(params)

        self._finished_count = 2

        # Begin streaming.
        cmd = AgentCommand(command='go_streaming')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.STREAMING)

        config = get_safe(self.DVR_CONFIG, 'dh_cfg', {})

        log.info('Send a constrained request for data: constraints = HIST_CONSTRAINTS_1')
        config['stream_id'] = self.create_stream_and_logger(name='stream_id_for_historical_1')
        config['constraints']=self.HIST_CONSTRAINTS_1
        cmd = AgentCommand(command='acquire_data', args=[config])
        reply = self._ia_client.execute(cmd)
        self.assertNotEqual(reply.status, 660)

        gevent.sleep(12)

        # Halt streaming.
        cmd = AgentCommand(command='go_observatory')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        # Assert that data was received
        self._async_finished_result.get(timeout=10)
        self.assertTrue(len(self._finished_events_received) >= 3)

        cmd = AgentCommand(command='reset')
        _ = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

    @unittest.skip('Not used in DataHandler')
    def test_acquire_sample(self):
        # Test observatory polling function.

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        # Lets get 3 samples.
        self._no_samples = 3

        # Poll for a few samples.
        cmd = AgentCommand(command='acquire_sample')
        reply = self._ia_client.execute(cmd)
        self.assertSampleDict(reply.result)

        cmd = AgentCommand(command='acquire_sample')
        reply = self._ia_client.execute(cmd)
        self.assertSampleDict(reply.result)

        cmd = AgentCommand(command='acquire_sample')
        reply = self._ia_client.execute(cmd)
        self.assertSampleDict(reply.result)

        # Assert we got 3 samples.
        self._async_data_result.get(timeout=10)
        self.assertTrue(len(self._samples_received)==3)

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

    def test_streaming(self):
        # Test instrument driver execute interface to start and stop streaming mode.
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        # Make sure the polling interval is appropriate for a test
        params = {
            'POLLING_INTERVAL':5
        }
        self._ia_client.set_param(params)

        self._finished_count = 3

        # Begin streaming.
        cmd = AgentCommand(command='go_streaming')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.STREAMING)

        # Wait for some samples to roll in.
        gevent.sleep(12)

        # Halt streaming.
        cmd = AgentCommand(command='go_observatory')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        # Assert that data was received
        self._async_finished_result.get(timeout=10)
        self.assertTrue(len(self._finished_events_received) >= 3)

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

    def test_observatory(self):
        # Test instrument driver get and set interface.

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        # Retrieve all resource parameters.
        reply = self._ia_client.get_param('DRIVER_PARAMETER_ALL')
        self.assertParamDict(reply, True)
        orig_config = reply

        ## Retrieve a subset of resource parameters.
        params = [
            'POLLING_INTERVAL'
        ]
        reply = self._ia_client.get_param(params)
        self.assertParamDict(reply)
        orig_params = reply

        # Set a subset of resource parameters.
        new_params = {
            'POLLING_INTERVAL' : (orig_params['POLLING_INTERVAL'] * 2),
        }
        self._ia_client.set_param(new_params)
        check_new_params = self._ia_client.get_param(params)
        self.assertParamVals(check_new_params, new_params)

        #        # Reset the parameters back to their original values.
        #        self._ia_client.set_param(orig_params)
        #        reply = self._ia_client.get_param(DataHandlerParameter.POLLING_INTERVAL)
        #        reply.pop(DataHandlerParameter.POLLING_INTERVAL)
        #        orig_config.pop(DataHandlerParameter.POLLING_INTERVAL)
        #        self.assertParamVals(reply, orig_config)

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

    def test_get_set_param(self):
        cmd=AgentCommand(command='initialize')
        _ = self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command='go_active')
        _ = self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command='run')
        _ = self._ia_client.execute_agent(cmd)

        # Get a couple parameters, one that exists, one that doesn't
        self._ia_client.set_param({'POLLING_INTERVAL':3600})
        retval = self._ia_client.get_param(['POLLING_INTERVAL','BAD_PARAM'])
        self.assertTrue(isinstance(retval,dict))
        self.assertEqual(retval['POLLING_INTERVAL'],3600)
        self.assertEqual(retval['BAD_PARAM'],None)

        # Set the polling_interval to a new value, then get it to make sure it set properly
        self._ia_client.set_param({'POLLING_INTERVAL':10})
        retval = self._ia_client.get_param(['POLLING_INTERVAL'])
        self.assertTrue(isinstance(retval,dict))
        self.assertEqual(retval['POLLING_INTERVAL'],10)

        cmd = AgentCommand(command='reset')
        _ = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

    def test_initialize(self):
        # Test agent initialize command. This causes creation of driver process and transition to inactive.

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

    def test_states(self):
        # Test agent state transitions.

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        cmd = AgentCommand(command='pause')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.STOPPED)

        cmd = AgentCommand(command='resume')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        cmd = AgentCommand(command='clear')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        cmd = AgentCommand(command='pause')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.STOPPED)

        cmd = AgentCommand(command='clear')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        self._finished_count = 1

        cmd = AgentCommand(command='go_streaming')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.STREAMING)

        gevent.sleep(5)

        cmd = AgentCommand(command='go_observatory')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        self._async_finished_result.get(timeout=5)

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

    def test_capabilities(self):
        # Test the ability to retrieve agent and resource parameter and command capabilities.
        acmds = self._ia_client.get_capabilities(['AGT_CMD'])
        log.debug('Agent Commands: {0}'.format(acmds))
        acmds = [item[1] for item in acmds]
        self.assertEqual(acmds, AGT_CMDS)
        apars = self._ia_client.get_capabilities(['AGT_PAR'])
        log.debug('Agent Parameters: {0}'.format(apars))

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        rcmds = self._ia_client.get_capabilities(['RES_CMD'])
        log.debug('Resource Commands: {0}'.format(rcmds))
        rcmds = [item[1] for item in rcmds]
        self.assertEqual(rcmds, CMDS)

        rpars = self._ia_client.get_capabilities(['RES_PAR'])
        log.debug('Resource Parameters: {0}'.format(rpars))
        rpars = [item[1] for item in rpars]
        self.assertEqual(rpars, PARAMS.keys())

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

    def test_errors(self):
        # Test illegal behavior and replies.

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        # Can't go active in unitialized state.
        # Status 660 is state error.
        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        log.info('GO ACTIVE CMD %s',str(retval))
        self.assertEquals(retval.status, 660)

        # Can't command driver in this state.
        cmd = AgentCommand(command='acquire_sample')
        reply = self._ia_client.execute(cmd)
        self.assertEqual(reply.status, 660)

        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

#        # OK, I can do this now.
#        cmd = AgentCommand(command='acquire_sample')
#        reply = self._ia_client.execute(cmd)
#        self.assertSampleDict(reply.result)

        # 404 unknown agent command.
        cmd = AgentCommand(command='kiss_edward')
        retval = self._ia_client.execute_agent(cmd)
        self.assertEquals(retval.status, 404)

        # 670 unknown driver command.
        cmd = AgentCommand(command='acquire_sample_please')
        retval = self._ia_client.execute(cmd)
        self.assertEqual(retval.status, 670)

        # 630 Parameter error.
        with self.assertRaises(InstParameterError):
            reply = self._ia_client.get_param('bogus bogus')

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

@attr('INT', group='eoi')
class TestExternalDatasetAgent(ExternalDatasetAgentTestBase, IonIntegrationTestCase):
    # DataHandler config
    DVR_CONFIG = {
        'dvr_mod' : 'ion.agents.data.handlers.base_data_handler',
        'dvr_cls' : 'DummyDataHandler',
        }

    # Constraints dict
    HIST_CONSTRAINTS_1 = {
        'array_len':15,
        }
    HIST_CONSTRAINTS_2 = {
        'array_len':10,
        }

    def _setup_resources(self):
        stream_id = self.create_stream_and_logger(name='dummydata_stream')

        tx = TaxyTool()
        tx.add_taxonomy_set('data', 'external_data')
        self.DVR_CONFIG['dh_cfg'] = {
            'TESTING':True,
            'stream_id':stream_id,#TODO: This should probably be a 'stream_config' dict with stream_name:stream_id members
            'data_producer_id':'dummy_data_producer_id',
            'taxonomy':tx._t,
            'max_records':4,
            }

@attr('INT', group='eoi')
class TestExternalDatasetAgent_Fibonacci(ExternalDatasetAgentTestBase, IonIntegrationTestCase):
    DVR_CONFIG = {
        'dvr_mod' : 'ion.agents.data.handlers.base_data_handler',
        'dvr_cls' : 'FibonacciDataHandler',
    }

    HIST_CONSTRAINTS_1 = {
        'count':15,
    }

    HIST_CONSTRAINTS_2 = {
        'count':10,
    }

    def _setup_resources(self):
        stream_id = self.create_stream_and_logger(name='fibonacci_stream')

        tx = TaxyTool()
        tx.add_taxonomy_set('data', 'external_data')
        #TG: Build TaxonomyTool & add to dh_cfg.taxonomy
        self.DVR_CONFIG['dh_cfg'] = {
            'TESTING':True,
            'stream_id':stream_id,
            'data_producer_id':'fibonacci_data_producer_id',
            'taxonomy':tx._t,
            'max_records':4,
            }

