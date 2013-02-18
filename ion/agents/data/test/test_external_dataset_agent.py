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
from pyon.core.exception import Conflict, BadRequest, ServerError
# Standard imports.

# 3rd party imports.
from gevent.event import AsyncResult
from nose.plugins.attrib import attr
from mock import patch
import unittest
import numpy
import os

from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand, ExternalDatasetAgent, ExternalDatasetAgentInstance
from interface.objects import ExternalDataProvider, ExternalDataset, DataSource, DataSourceModel, DataProduct
from interface.objects import ContactInformation, UpdateDescription, DatasetDescription, Institution
from pyon.util.containers import get_safe
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from pyon.event.event import EventSubscriber
from pyon.ion.resource import PRED, RT

# MI imports
from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent
from ion.core.includes.mi import DriverEvent
from ion.services.dm.utility.granule_utils import time_series_domain

from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient

from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum

#########################
# For Validation Purposes
#########################

# Used to validate param config retrieved from driver.
PARAMS = {
    'POLLING_INTERVAL': int,
    'PATCHABLE_CONFIG_KEYS': list
}

# To validate the list of resource commands
CMDS = {
    'acquire_sample': str,
    'start_autosample': str,
    'stop_autosample': str
}

# To validate the list of agent commands
AGT_CMDS = {
    'clear': str,
    'end_transaction': str,
    'get_current_state': str,
    'go_active': str,
    'go_direct_access': str,
    'go_inactive': str,
    'go_observatory': str,
    'go_streaming': str,
    'initialize': str,
    'pause': str,
    'power_down': str,
    'power_up': str,
    'reset': str,
    'resume': str,
    'run': str,
    'start_transaction': str,
    }


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id = ''
    process_type = ''

@patch.dict(CFG, {'endpoint': {'receive': {'timeout': 60}}})
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
        """

        #log.warn('Starting the container')
        # Start container.
        self._start_container()

        # Bring up services in a deploy file
        #log.warn('Starting the rel')
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Create a pubsub client to create streams.
        #        log.warn('Init a pubsub client')
        self._pubsub_client = PubsubManagementServiceClient(node=self.container.node)
        #        log.warn('Init a ContainerAgentClient')
        self._container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)

        # Data async and subscription  TODO: Replace with new subscriber
        self._finished_count = None
        #TODO: Switch to gevent.queue.Queue
        self._async_finished_result = AsyncResult()
        self._finished_events_received = []
        self._finished_event_subscriber = None
        self._start_finished_event_subscriber()
        self.addCleanup(self._stop_finished_event_subscriber)

        # TODO: Finish dealing with the resources and whatnot
        # TODO: DVR_CONFIG and (potentially) stream_config could both be reconfigured in self._setup_resources()
        self._setup_resources()

        #TG: Setup/configure the granule logger to log granules as they're published

        # Create agent config.
        agent_config = {
            'driver_config': self.DVR_CONFIG,
            'stream_config': {},
            'agent': {'resource_id': self.EDA_RESOURCE_ID},
            'test_mode': True
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

    def create_stream_and_logger(self, name, stream_id='', pdict=None):

        stream_def_id = ''
        if not stream_id or stream_id is '':
            if pdict:
                stream_def_id = self._pubsub_client.create_stream_definition(parameter_dictionary=pdict.dump(), stream_type='stream')
            stream_id, route = self._pubsub_client.create_stream(name=name, exchange_point='science_data', stream_definition_id=stream_def_id)
        else:
            route = self._pubsub_client.read_stream_route(stream_id=stream_id)
            stream_def = self._pubsub_client.read_stream_definition(stream_id=stream_id)
            stream_def_id = stream_def._id

        pid = self._container_client.spawn_process(
            name=name + '_logger',
            module='ion.processes.data.stream_granule_logger',
            cls='StreamGranuleLogger',
            config={'process': {'stream_id': stream_id}}
        )
        log.info('Started StreamGranuleLogger \'{0}\' subscribed to stream_id={1}'.format(pid, stream_id))

        return stream_id, route, stream_def_id

    def _start_finished_event_subscriber(self):

        def consume_event(*args, **kwargs):
            if args[0].description == 'TestingFinished':
                log.debug('TestingFinished event received')
                self._finished_events_received.append(args[0])
                if self._finished_count and self._finished_count == len(self._finished_events_received):
                    log.debug('Finishing test...')
                    self._async_finished_result.set(len(self._finished_events_received))
                    log.debug('Called self._async_finished_result.set({0})'.format(len(self._finished_events_received)))

        self._finished_event_subscriber = EventSubscriber(event_type='DeviceEvent', callback=consume_event)
        self._finished_event_subscriber.start()

    def _stop_finished_event_subscriber(self):
        if self._finished_event_subscriber:
            self._finished_event_subscriber.stop()
            self._finished_event_subscriber = None

    ########################################
    # Custom assertion functions
    ########################################
    def assertListsEqual(self, lst1, lst2):
        lst1.sort()
        lst2.sort()
        return lst1 == lst2

    def assertSampleDict(self, val):
        """
        Verify the value is a sample dictionary for the sbe37.
        """
        #{'p': [-6.945], 'c': [0.08707], 't': [20.002], 'time': [1333752198.450622]}
        self.assertTrue(isinstance(val, dict))
        self.assertTrue('c' in val)
        self.assertTrue('t' in val)
        self.assertTrue('p' in val)
        self.assertTrue('time' in val)
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
                self.assertTrue(key in PARAMS)
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
                self.assertAlmostEqual(val, correct_val, delta=max_val * .01)

            elif isinstance(val, (list, tuple)):
                # list of tuple.
                self.assertEqual(list(val), list(correct_val))

            else:
                # int, bool, str.
                self.assertEqual(val, correct_val)

    ########################################
    # Test functions
    ########################################
    def test_acquire_data_while_streaming(self):
        # Test instrument driver execute interface to start and stop streaming mode.
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        params = {
            'POLLING_INTERVAL': 3
        }
        self._ia_client.set_resource(params)

        self._finished_count = 1

        cmd = AgentCommand(command=DriverEvent.START_AUTOSAMPLE)
        self._ia_client.execute_resource(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.STREAMING)

        config = get_safe(self.DVR_CONFIG, 'dh_cfg', {})
        log.info('Send a constrained request for data: constraints = HIST_CONSTRAINTS_1')
        config['stream_id'], config['stream_route'], _ = self.create_stream_and_logger(name='stream_id_for_historical_1')
        config['constraints'] = self.HIST_CONSTRAINTS_1
        cmd = AgentCommand(command=DriverEvent.ACQUIRE_SAMPLE, args=[config])
        self._ia_client.execute_resource(cmd)

        cmd = AgentCommand(command=DriverEvent.STOP_AUTOSAMPLE)
        self._ia_client.execute_resource(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        finished = self._async_finished_result.get(timeout=120)
        self.assertEqual(finished, self._finished_count)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

    def test_acquire_data(self):

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        log.warn('Send an unconstrained request for data (\'new data\')')
        cmd = AgentCommand(command=DriverEvent.ACQUIRE_SAMPLE)
        self._ia_client.execute_resource(command=cmd)
        state = self._ia_client.get_agent_state()
        log.info(state)
        self.assertEqual(state, ResourceAgentState.COMMAND)

        self._finished_count = 2

        config_mods = {}

        log.info('Send a constrained request for data: constraints = HIST_CONSTRAINTS_1')
        config_mods['stream_id'], config_mods['stream_route'], _ = self.create_stream_and_logger(name='stream_id_for_historical_1')
        config_mods['constraints'] = self.HIST_CONSTRAINTS_1
        cmd = AgentCommand(command=DriverEvent.ACQUIRE_SAMPLE, args=[config_mods])
        self._ia_client.execute_resource(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        log.info('Send a second constrained request for data: constraints = HIST_CONSTRAINTS_2')
        config_mods['stream_id'], config_mods['stream_route'], _ = self.create_stream_and_logger(name='stream_id_for_historical_2')
        config_mods['constraints'] = self.HIST_CONSTRAINTS_2
        cmd = AgentCommand(command=DriverEvent.ACQUIRE_SAMPLE, args=[config_mods])
        self._ia_client.execute_resource(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        finished = self._async_finished_result.get(timeout=120)
        self.assertEqual(finished, self._finished_count)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

    def test_streaming(self):
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        params = {
            'POLLING_INTERVAL': 3
        }
        self._ia_client.set_resource(params)

        self._finished_count = 3

        cmd = AgentCommand(command=DriverEvent.START_AUTOSAMPLE)
        self._ia_client.execute_resource(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.STREAMING)

        #Assert that data was received
        #        self._async_finished_result.get(timeout=600)
        #        self.assertTrue(len(self._finished_events_received) >= 3)

        cmd = AgentCommand(command=DriverEvent.STOP_AUTOSAMPLE)
        self._ia_client.execute_resource(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

    def test_command(self):
        # Test instrument driver get and set interface.

        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        # Retrieve all resource parameters.
        reply = self._ia_client.get_resource(params=['DRIVER_PARAMETER_ALL'])
        self.assertParamDict(reply, True)

        ## Retrieve a subset of resource parameters.
        params = [
            'POLLING_INTERVAL'
        ]
        reply = self._ia_client.get_resource(params=params)
        self.assertParamDict(reply)
        orig_params = reply

        # Set a subset of resource parameters.
        new_params = {
            'POLLING_INTERVAL': (orig_params['POLLING_INTERVAL'] * 2),
            }
        self._ia_client.set_resource(params=new_params)
        check_new_params = self._ia_client.get_resource(params)
        self.assertParamVals(check_new_params, new_params)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

    def test_get_set_resource(self):
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        self._ia_client.execute_agent(cmd)

        # Get a couple parameters
        retval = self._ia_client.get_resource(['POLLING_INTERVAL', 'PATCHABLE_CONFIG_KEYS'])
        log.debug('Retrieved parameters from agent: {0}'.format(retval))
        self.assertTrue(isinstance(retval, dict))
        self.assertEqual(type(retval['POLLING_INTERVAL']), int)
        self.assertEqual(type(retval['PATCHABLE_CONFIG_KEYS']), list)

        # Attempt to get a parameter that doesn't exist
        log.debug('Try getting a non-existent parameter \'BAD_PARAM\'')
        with self.assertRaises(ServerError):
            self._ia_client.get_resource(['BAD_PARAM'])

        # Set the polling_interval to a new value, then get it to make sure it set properly
        self._ia_client.set_resource({'POLLING_INTERVAL': 10})
        retval = self._ia_client.get_resource(['POLLING_INTERVAL'])
        log.debug('Retrieved parameters from agent: {0}'.format(retval))
        self.assertTrue(isinstance(retval, dict))
        self.assertEqual(retval['POLLING_INTERVAL'], 10)

        # Attempt to set a parameter that doesn't exist
        log.debug('Try setting a non-existent parameter \'BAD_PARAM\'')
        with self.assertRaises(ServerError):
            self._ia_client.set_resource({'BAD_PARAM': 'bad_val'})

        # Attempt to set one parameter that does exist, and one that doesn't
        with self.assertRaises(ServerError):
            self._ia_client.set_resource({'POLLING_INTERVAL': 20, 'BAD_PARAM': 'bad_val'})

        retval = self._ia_client.get_resource(['POLLING_INTERVAL'])
        log.debug('Retrieved parameters from agent: {0}'.format(retval))
        self.assertTrue(isinstance(retval, dict))
        self.assertEqual(retval['POLLING_INTERVAL'], 20)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

    def test_initialize(self):
        # Test agent initialize command. This causes creation of driver process and transition to inactive.

        # We start in uninitialized state.
        # In this state there is no driver process.
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        # Initialize the agent.
        # The agent is spawned with a driver config, but you can pass one in
        # optinally with the initialize command. This validates the driver
        # config, launches a driver process and connects to it via messaging.
        # If successful, we switch to the inactive state.
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        # Reset the agent. This causes the driver messaging to be stopped,
        # the driver process to end and switches us back to uninitialized.
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

    def test_states(self):
        # Test agent state transitions.

        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.PAUSE)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.STOPPED)

        cmd = AgentCommand(command=ResourceAgentEvent.RESUME)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.CLEAR)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.PAUSE)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.STOPPED)

        cmd = AgentCommand(command=ResourceAgentEvent.CLEAR)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=DriverEvent.START_AUTOSAMPLE)
        self._ia_client.execute_resource(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.STREAMING)

        cmd = AgentCommand(command=DriverEvent.STOP_AUTOSAMPLE)
        self._ia_client.execute_resource(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

    def test_capabilities(self):
        """
        Test the ability to retrieve agent and resource parameter and command
        capabilities in various system states.
        """

        # Test the ability to retrieve agent and resource parameter and command capabilities.
        acmds = self._ia_client.get_capabilities(['AGT_CMD'])
        log.debug('Agent Commands: {0}'.format(acmds))
        #        acmds = [item[1] for item in acmds]
        self.assertListsEqual(acmds, AGT_CMDS.keys())
        apars = self._ia_client.get_capabilities(['AGT_PAR'])
        log.debug('Agent Parameters: {0}'.format(apars))

        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        rcmds = self._ia_client.get_capabilities(['RES_CMD'])
        log.debug('Resource Commands: {0}'.format(rcmds))
        #        rcmds = [item[1] for item in rcmds]
        self.assertListsEqual(rcmds, CMDS.keys())

        rpars = self._ia_client.get_capabilities(['RES_PAR'])
        log.debug('Resource Parameters: {0}'.format(rpars))
        #        rpars = [item[1] for item in rpars]
        self.assertListsEqual(rpars, PARAMS.keys())

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

    def test_errors(self):
        # Test illegal behavior and replies.

        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        # Can't go active in unitialized state.
        # Status 660 is state error.
        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        with self.assertRaises(Conflict):
            self._ia_client.execute_agent(cmd)

        # Can't command driver in this state.
        cmd = AgentCommand(command=DriverEvent.ACQUIRE_SAMPLE)
        with self.assertRaises(Conflict):
            self._ia_client.execute_resource(cmd)
            #self.assertEqual(reply.status, 660)

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        # 404 unknown agent command.
        cmd = AgentCommand(command='kiss_edward')
        with self.assertRaises(BadRequest):
            self._ia_client.execute_agent(cmd)

        # 670 unknown driver command.
        cmd = AgentCommand(command='acquire_sample_please')
        with self.assertRaises(ServerError):
            self._ia_client.execute_resource(cmd)

        # 630 Parameter error.
        #self.assertRaises(InstParameterError, self._ia_client.get_param, 'bogus bogus')

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

#@attr('INT', group='eoi')
#class TestExternalDatasetAgent(ExternalDatasetAgentTestBase, IonIntegrationTestCase):
#    # DataHandler config
#    DVR_CONFIG = {
#        'dvr_mod' : 'ion.agents.data.handlers.base_data_handler',
#        'dvr_cls' : 'DummyDataHandler',
#        }
#
#    # Constraints dict
#    HIST_CONSTRAINTS_1 = {
#        'array_len':15,
#        }
#    HIST_CONSTRAINTS_2 = {
#        'array_len':10,
#        }
#
#    NDC = {
#    }
#
#    def _setup_resources(self):
#        stream_id = self.create_stream_and_logger(name='dummydata_stream')
#
#        tx = TaxyTool()
#        tx.add_taxonomy_set('dummy', 'external_data')
#        self.DVR_CONFIG['dh_cfg'] = {
#            'TESTING':True,
#            'stream_id':stream_id,#TODO: This should probably be a 'stream_config' dict with stream_name:stream_id members
#            'data_producer_id':'dummy_data_producer_id',
#            'taxonomy':tx.dump(),
#            'max_records':4,
#            }


@attr('INT', group='eoi')
class TestExternalDatasetAgent(ExternalDatasetAgentTestBase, IonIntegrationTestCase):
    DVR_CONFIG = {
        'dvr_mod': 'ion.agents.data.handlers.base_data_handler',
        'dvr_cls': 'FibonacciDataHandler',
        }

    HIST_CONSTRAINTS_1 = {
        'count': 15,
        }

    HIST_CONSTRAINTS_2 = {
        'count': 10,
        }

    NDC = {
    }

    def _setup_resources(self):
        pdict = ParameterDictionary()

        t_ctxt = ParameterContext('data', param_type=QuantityType(value_encoding=numpy.dtype('int64')))
        t_ctxt.axis = AxisTypeEnum.TIME
        t_ctxt.uom = 'seconds since 01-01-1970'
        pdict.add_context(t_ctxt)

        stream_id, stream_route, stream_def = self.create_stream_and_logger(name='fibonacci_stream', pdict=pdict)
        #        tx = TaxyTool()
        #        tx.add_taxonomy_set('data', 'external_data')

        self.DVR_CONFIG['dh_cfg'] = {
            'TESTING': True,
            'stream_id': stream_id,
            'stream_route': stream_route,
            'stream_def': stream_def,
            'data_producer_id': 'fibonacci_data_producer_id',
            'max_records': 4,
            }


@attr('INT_EXPERIMENTAL', group='eoi')
class TestExternalDatasetAgent_Dummy(ExternalDatasetAgentTestBase, IonIntegrationTestCase):
    # DataHandler config
    DVR_CONFIG = {
        'dvr_mod': 'ion.agents.data.handlers.base_data_handler',
        'dvr_cls': 'DummyDataHandler',
        }

    NDC = {
    }

    def _setup_resources(self):
        # TODO: some or all of this (or some variation) should move to DAMS'

        # Build the test resources for the dataset
        dms_cli = DatasetManagementServiceClient()
        dams_cli = DataAcquisitionManagementServiceClient()
        dpms_cli = DataProductManagementServiceClient()
        rr_cli = ResourceRegistryServiceClient()
        pubsub_cli = PubsubManagementServiceClient()

        eda = ExternalDatasetAgent(handler_module=self.DVR_CONFIG['dvr_mod'], handler_class=self.DVR_CONFIG['dvr_cls'])
        eda_id = dams_cli.create_external_dataset_agent(eda)

        eda_inst = ExternalDatasetAgentInstance()
        eda_inst_id = dams_cli.create_external_dataset_agent_instance(eda_inst, external_dataset_agent_id=eda_id)

        # Create and register the necessary resources/objects

        # Create DataProvider
        dprov = ExternalDataProvider(institution=Institution(), contact=ContactInformation())
        dprov.contact.individual_names_given = 'Christopher Mueller'
        dprov.contact.email = 'cmueller@asascience.com'

        # Create DataSource
        dsrc = DataSource(protocol_type='DAP', institution=Institution(), contact=ContactInformation())
        dsrc.connection_params['base_data_url'] = ''
        dsrc.contact.individual_names_given = 'Tim Giguere'
        dsrc.contact.email = 'tgiguere@asascience.com'

        # Create ExternalDataset
        ds_name = 'dummy_dataset'
        dset = ExternalDataset(name=ds_name, dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())

        # The usgs.nc test dataset is a download of the R1 dataset found here:
        # http://thredds-test.oceanobservatories.org/thredds/dodsC/ooiciData/E66B1A74-A684-454A-9ADE-8388C2C634E5.ncml
        dset.dataset_description.parameters['base_url'] = 'test_data/dummy'
        dset.dataset_description.parameters['list_pattern'] = 'test*.dum'
        dset.dataset_description.parameters['date_pattern'] = '%Y %m %d %H'
        dset.dataset_description.parameters['date_extraction_pattern'] = 'test([\d]{4})-([\d]{2})-([\d]{2})-([\d]{2}).dum'
        dset.dataset_description.parameters['temporal_dimension'] = 'time'
        dset.dataset_description.parameters['zonal_dimension'] = 'lon'
        dset.dataset_description.parameters['meridional_dimension'] = 'lat'
        dset.dataset_description.parameters['variables'] = [
            'dummy',
            ]

        # Create DataSourceModel
        dsrc_model = DataSourceModel(name='dap_model')
        #dsrc_model.model = 'DAP'
        dsrc_model.data_handler_module = 'N/A'
        dsrc_model.data_handler_class = 'N/A'

        ## Run everything through DAMS
        ds_id = dams_cli.create_external_dataset(external_dataset=dset)
        ext_dprov_id = dams_cli.create_external_data_provider(external_data_provider=dprov)
        ext_dsrc_id = dams_cli.create_data_source(data_source=dsrc)
        ext_dsrc_model_id = dams_cli.create_data_source_model(dsrc_model)

        # Register the ExternalDataset
        dproducer_id = dams_cli.register_external_data_set(external_dataset_id=ds_id)

        # Or using each method
        dams_cli.assign_data_source_to_external_data_provider(data_source_id=ext_dsrc_id, external_data_provider_id=ext_dprov_id)
        dams_cli.assign_data_source_to_data_model(data_source_id=ext_dsrc_id, data_source_model_id=ext_dsrc_model_id)
        dams_cli.assign_external_dataset_to_data_source(external_dataset_id=ds_id, data_source_id=ext_dsrc_id)
        dams_cli.assign_external_dataset_to_agent_instance(external_dataset_id=ds_id, agent_instance_id=eda_inst_id)

        #create temp streamdef so the data product can create the stream
        pc_list = []

        #Get 'time' parameter context
        pc_list.append(dms_cli.read_parameter_context_by_name('time', id_only=True))
        pc_list.append(dms_cli.read_parameter_context_by_name('dummy', id_only=True))

        for pc_k, pc in self._create_parameter_dictionary().iteritems():
            pc_list.append(dms_cli.create_parameter_context(pc_k, pc[1].dump()))

        pdict_id = dms_cli.create_parameter_dictionary('dummy_param_dict', pc_list)

        streamdef_id = pubsub_cli.create_stream_definition(name="dummy", parameter_dictionary_id=pdict_id, description="dummy")

        tdom, sdom = time_series_domain()
        tdom, sdom = tdom.dump(), sdom.dump()

        dprod = DataProduct(
            name='dummy_dataset',
            description='dummy data product',
            temporal_domain=tdom,
            spatial_domain=sdom)

        dproduct_id = dpms_cli.create_data_product(data_product=dprod,
            stream_definition_id=streamdef_id)

        dams_cli.assign_data_product(input_resource_id=ds_id, data_product_id=dproduct_id)  # , create_stream=True)

        stream_id, assn = rr_cli.find_objects(subject=dproduct_id, predicate=PRED.hasStream, object_type=RT.Stream, id_only=True)
        stream_id = stream_id[0]

        log.info('Created resources: {0}'.format({'ExternalDataset': ds_id, 'ExternalDataProvider': ext_dprov_id, 'DataSource': ext_dsrc_id, 'DataSourceModel': ext_dsrc_model_id, 'DataProducer': dproducer_id, 'DataProduct': dproduct_id, 'Stream': stream_id}))

        # Create the logger for receiving publications
        _, stream_route, stream_def = self.create_stream_and_logger(name='dummy', stream_id=stream_id)

        self.EDA_RESOURCE_ID = ds_id
        self.EDA_NAME = ds_name
        self.DVR_CONFIG['dh_cfg'] = {
            'TESTING': True,
            'stream_id': stream_id,
            'stream_route': stream_route,
            'stream_def': stream_def,
            'data_producer_id': dproducer_id,  # CBM: Should this be put in the main body of the config - with mod & cls?
            'max_records': 4,
            }

        folder = 'test_data/dummy'

        if os.path.isdir(folder):
            for the_file in os.listdir(folder):
                file_path = os.path.join(folder, the_file)
                try:
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                except Exception as e:
                    log.debug('_setup_resources error: {0}'.format(e))

        if not os.path.exists(folder):
            os.makedirs(folder)

        self.add_dummy_file('test_data/dummy/test2012-02-01-12.dum')
        self.add_dummy_file('test_data/dummy/test2012-02-01-13.dum')
        self.add_dummy_file('test_data/dummy/test2012-02-01-14.dum')
        self.add_dummy_file('test_data/dummy/test2012-02-01-15.dum')

    def _create_parameter_dictionary(self):
        pdict = ParameterDictionary()

        lat_ctxt = ParameterContext('lat', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        lat_ctxt.axis = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        pdict.add_context(lat_ctxt)

        lon_ctxt = ParameterContext('lon', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        lon_ctxt.axis = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        pdict.add_context(lon_ctxt)

        return pdict

    def add_dummy_file(self, file_name):
    #        with open(file_name, 'w') as f:
        f = open(file_name, 'wb')
        f.write(numpy.arange(100))

    def tearDown(self):
        folder = 'test_data/dummy'

        if os.path.isdir(folder):
            for the_file in os.listdir(folder):
                file_path = os.path.join(folder, the_file)
                try:
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                except Exception as e:
                    log.debug('_setup_resources error: {0}'.format(e))

    def clean_up(self, file_path):

        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            log.debug('_setup_resources error: {0}'.format(e))

    def test_new_data_available_at_end(self):
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        self._ia_client.execute_agent(cmd)

        self._finished_count = 1

        config_mods = {}

        log.info('Send a constrained request for data: constraints = HIST_CONSTRAINTS_1')
        config_mods['stream_id'], config_mods['stream_route'], _ = self.create_stream_and_logger(name='stream_id_for_historical_1')
        cmd = AgentCommand(command=DriverEvent.ACQUIRE_SAMPLE, args=[config_mods])
        self._ia_client.execute_resource(cmd)

        finished = self._async_finished_result.get(timeout=10)
        self.assertEqual(finished, self._finished_count)

        self.add_dummy_file('test_data/dummy/test2012-02-01-16.dum')
        self.add_dummy_file('test_data/dummy/test2012-02-01-17.dum')

        log.info('Send a constrained request for data: constraints = HIST_CONSTRAINTS_1')
        #config_mods['constraints']=self.HIST_CONSTRAINTS_1
        cmd = AgentCommand(command=DriverEvent.ACQUIRE_SAMPLE, args=[config_mods])
        self._ia_client.execute_resource(cmd)

        finished = self._async_finished_result.get(timeout=10)
        self.assertEqual(finished, self._finished_count)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        self.clean_up('test_data/dummy/test2012-02-01-16.dum')
        self.clean_up('test_data/dummy/test2012-02-01-17.dum')

    def test_new_data_available_at_beginning(self):
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        self._ia_client.execute_agent(cmd)

        self._finished_count = 1

        config_mods = {}

        log.info('Send a constrained request for data: constraints = HIST_CONSTRAINTS_1')
        config_mods['stream_id'], config_mods['stream_route'], _ = self.create_stream_and_logger(name='stream_id_for_historical_1')
        #config_mods['constraints']=self.HIST_CONSTRAINTS_1
        cmd = AgentCommand(command=DriverEvent.ACQUIRE_SAMPLE, args=[config_mods])
        self._ia_client.execute_resource(cmd)

        finished = self._async_finished_result.get(timeout=10)
        self.assertEqual(finished, self._finished_count)

        self.add_dummy_file('test_data/dummy/test2012-02-01-10.dum')
        self.add_dummy_file('test_data/dummy/test2012-02-01-11.dum')

        log.info('Send a constrained request for data: constraints = HIST_CONSTRAINTS_1')
        cmd = AgentCommand(command=DriverEvent.ACQUIRE_SAMPLE, args=[config_mods])
        self._ia_client.execute_resource(cmd)

        finished = self._async_finished_result.get(timeout=10)
        self.assertEqual(finished, self._finished_count)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        self.clean_up('test_data/dummy/test2012-02-01-10.dum')
        self.clean_up('test_data/dummy/test2012-02-01-11.dum')

    def test_new_data_available_at_beginning_and_end(self):
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        self._ia_client.execute_agent(cmd)

        self._finished_count = 1

        config_mods = {}

        log.info('Send a constrained request for data: constraints = HIST_CONSTRAINTS_1')
        config_mods['stream_id'], config_mods['stream_route'], _ = self.create_stream_and_logger(name='stream_id_for_historical_1')
        #config_mods['constraints']=self.HIST_CONSTRAINTS_1
        cmd = AgentCommand(command=DriverEvent.ACQUIRE_SAMPLE, args=[config_mods])
        self._ia_client.execute_resource(cmd)

        finished = self._async_finished_result.get(timeout=10)
        self.assertEqual(finished, self._finished_count)

        self.add_dummy_file('test_data/dummy/test2012-02-01-10.dum')
        self.add_dummy_file('test_data/dummy/test2012-02-01-11.dum')
        self.add_dummy_file('test_data/dummy/test2012-02-01-16.dum')
        self.add_dummy_file('test_data/dummy/test2012-02-01-17.dum')

        log.info('Send a constrained request for data: constraints = HIST_CONSTRAINTS_1')
        cmd = AgentCommand(command=DriverEvent.ACQUIRE_SAMPLE, args=[config_mods])
        self._ia_client.execute_resource(cmd)

        finished = self._async_finished_result.get(timeout=10)
        self.assertEqual(finished, self._finished_count)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        self.clean_up('test_data/dummy/test2012-02-01-10.dum')
        self.clean_up('test_data/dummy/test2012-02-01-11.dum')
        self.clean_up('test_data/dummy/test2012-02-01-16.dum')
        self.clean_up('test_data/dummy/test2012-02-01-17.dum')

    def test_data_removed_from_beginning(self):
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        self._ia_client.execute_agent(cmd)

        self._finished_count = 1

        config_mods = {}

        log.info('Send a constrained request for data: constraints = HIST_CONSTRAINTS_1')
        config_mods['stream_id'], config_mods['stream_route'], _ = self.create_stream_and_logger(name='stream_id_for_historical_1')
        #config_mods['constraints']=self.HIST_CONSTRAINTS_1
        cmd = AgentCommand(command=DriverEvent.ACQUIRE_SAMPLE, args=[config_mods])
        self._ia_client.execute_resource(cmd)

        finished = self._async_finished_result.get(timeout=10)
        self.assertEqual(finished, self._finished_count)

        os.remove('test_data/dummy/test2012-02-01-12.dum')
        os.remove('test_data/dummy/test2012-02-01-13.dum')

        log.info('Send a constrained request for data: constraints = HIST_CONSTRAINTS_1')
        cmd = AgentCommand(command=DriverEvent.ACQUIRE_SAMPLE, args=[config_mods])
        self._ia_client.execute_resource(cmd)

        finished = self._async_finished_result.get(timeout=10)
        self.assertEqual(finished, self._finished_count)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        self.clean_up('test_data/dummy/test2012-02-01-12.dum')
        self.clean_up('test_data/dummy/test2012-02-01-13.dum')

    def test_data_removed_from_end(self):
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        self._ia_client.execute_agent(cmd)

        self._finished_count = 1

        config_mods = {}

        log.info('Send a constrained request for data: constraints = HIST_CONSTRAINTS_1')
        config_mods['stream_id'], config_mods['stream_route'], _ = self.create_stream_and_logger(name='stream_id_for_historical_1')
        #config_mods['constraints']=self.HIST_CONSTRAINTS_1
        cmd = AgentCommand(command=DriverEvent.ACQUIRE_SAMPLE, args=[config_mods])
        self._ia_client.execute_resource(cmd)

        finished = self._async_finished_result.get(timeout=10)
        self.assertEqual(finished, self._finished_count)

        os.remove('test_data/dummy/test2012-02-01-14.dum')
        os.remove('test_data/dummy/test2012-02-01-15.dum')

        log.info('Send a constrained request for data: constraints = HIST_CONSTRAINTS_1')
        cmd = AgentCommand(command=DriverEvent.ACQUIRE_SAMPLE, args=[config_mods])
        self._ia_client.execute_resource(cmd)

        finished = self._async_finished_result.get(timeout=10)
        self.assertEqual(finished, self._finished_count)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        self.clean_up('test_data/dummy/test2012-02-01-14.dum')
        self.clean_up('test_data/dummy/test2012-02-01-15.dum')

    def test_acquire_data(self):
        pass

    def test_acquire_data_while_streaming(self):
        pass

    def test_streaming(self):
        pass

    def test_observatory(self):
        pass

    def test_get_set_param(self):
        pass

    def test_initialize(self):
        pass

    def test_states(self):
        pass

    def test_capabilities(self):
        pass

    def test_errors(self):
        pass
