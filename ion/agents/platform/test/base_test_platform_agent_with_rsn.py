#!/usr/bin/env python

"""
@package ion.agents.platform.test.base_test_platform_agent_with_rsn
@file    ion/agents/platform/test/base_test_platform_agent_with_rsn.py
@author  Carlos Rueda, Maurice Manning, Ian Katz
@brief   A base class for platform agent integration testing
"""

__author__ = 'Carlos Rueda, Maurice Manning, Ian Katz'
__license__ = 'Apache 2.0'

#
# The original test set-up in this file adapted pieces from various sources:
# - test_instrument_management_service_integration.py
# - test_driver_egg.py
# - test_oms_launch2.py  (now deprecated).
#
# The main class defined here serves as a base to specific platform agent
# integration tests:
# - ion/agents/platform/test/test_platform_agent_with_rsn.py
# - ion/services/sa/observatory/test/test_platform_launch.py
#
# NOTE: the platform IDs used here are organized as follows:
#   Node1D -> MJ01C -> LJ01D
#
# where -> goes from parent platform to child platform.
# This is a subset of the whole topology defined in the simulated platform
# network (network.yml), which in turn is used by the RSN OMS simulator.
#
# - 'LJ01D'  is the root platform used in test_single_platform
# - 'Node1D' is the root platform used in test_hierarchy
#
# In DEBUG logging level, the tests generate files like the following:
#   platform_agent_config_LJ01D.txt
#   platform_agent_config_Node1D_->_MJ01C.txt
#   platform_agent_config_Node1D_final.txt
# containing the corresponding platform agent configurations as they are
# constructed. In particular, the latter will be the whole configuration used
# for the hierarchy launch test.
#

from pyon.public import log
import logging
from pyon.public import CFG
from pyon.core.exception import ServerError

from pyon.util.int_test import IonIntegrationTestCase

from pyon.event.event import EventSubscriber

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient

from pyon.ion.stream import StandaloneStreamSubscriber

from pyon.util.context import LocalContextMixin
from pyon.public import RT, PRED

from nose.plugins.attrib import attr

from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand, ProcessStateEnum
from interface.objects import StreamConfiguration

from ion.agents.platform.platform_agent import PlatformAgentEvent

from ion.services.dm.utility.granule_utils import time_series_domain

from gevent.event import AsyncResult

from ion.agents.platform.test.helper import HelperTestMixin

from ion.agents.platform.rsn.oms_client_factory import CIOMSClientFactory
from ion.agents.platform.rsn.oms_util import RsnOmsUtil
from ion.agents.platform.util.network_util import NetworkUtil

from ion.agents.platform.platform_agent import PlatformAgentState

from ion.services.cei.process_dispatcher_service import ProcessStateGate
from mock import patch

import os
import time
import copy

from ion.services.sa.instrument.agent_configuration_builder import PlatformAgentConfigurationBuilder
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient

from pyon.util.containers import DotDict

from interface.services.coi.iidentity_management_service import IdentityManagementServiceClient

from ion.services.sa.test.helpers import any_old


# By default, test against "embedded" simulator. The OMS environment variable
# can be used to indicate a different RSN OMS server endpoint. Some aliases for
# the "oms_uri" parameter include "localsimulator" and "simulator".
# See CIOMSClientFactory.
DVR_CONFIG = {
    'oms_uri': os.getenv('OMS', 'embsimulator'),
}

DVR_MOD = 'ion.agents.platform.rsn.rsn_platform_driver'
DVR_CLS = 'RSNPlatformDriver'


# TIMEOUT: timeout for each execute_agent call.
TIMEOUT = 90

# DATA_TIMEOUT: timeout for reception of data sample
DATA_TIMEOUT = 25

# EVENT_TIMEOUT: timeout for reception of event
EVENT_TIMEOUT = 25


required_config_keys = [
    'org_name',
    'device_type',
    'agent',
    'driver_config',
    'stream_config',
    'startup_config',
    'alarm_defs',
    'children']


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('INT', group='sa')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 180}}})
class BaseIntTestPlatform(IonIntegrationTestCase, HelperTestMixin):
    """
    A base class with several conveniences supporting specific platform agent
    integration tests, see:
    - ion/agents/platform/test/test_platform_agent_with_rsn.py
    - ion/services/sa/observatory/test/test_platform_launch.py

    The platform IDs used here are organized as follows:
      Node1D -> MJ01C -> LJ01D

    where -> goes from parent platform to child platform.

    This is a subset of the whole topology defined in the simulated platform
    network (network.yml), which in turn is used by the RSN OMS simulator.

    - 'LJ01D'  is the root platform used in test_single_platform
    - 'Node1D' is the root platform used in test_hierarchy

    Methods are provided to construct specific platform topologies, but
    subclasses decide which to use.
    """

    @classmethod
    def setUpClass(cls):
        HelperTestMixin.setUpClass()

    def setUp(self):
        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.RR   = ResourceRegistryServiceClient(node=self.container.node)
        self.IMS  = InstrumentManagementServiceClient(node=self.container.node)
        self.DAMS = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.DP   = DataProductManagementServiceClient(node=self.container.node)
        self.PSC  = PubsubManagementServiceClient(node=self.container.node)
        self.PDC  = ProcessDispatcherServiceClient(node=self.container.node)
        self.DSC  = DatasetManagementServiceClient()
        self.IDS  = IdentityManagementServiceClient(node=self.container.node)
        self.RR2  = EnhancedResourceRegistryClient(self.RR)

        self.org_id = self.RR2.create(any_old(RT.Org))
        log.debug("Org created: %s", self.org_id)

        # Use the network definition provided by RSN OMS directly.
        rsn_oms = CIOMSClientFactory.create_instance(DVR_CONFIG['oms_uri'])
        self._network_definition = RsnOmsUtil.build_network_definition(rsn_oms)
        # get serialized version for the configuration:
        self._network_definition_ser = NetworkUtil.serialize_network_definition(self._network_definition)
        log.trace("NetworkDefinition serialization:\n%s", self._network_definition_ser)

        # set attributes for the platforms:
        self._platform_attributes = {}
        for platform_id in self._network_definition.pnodes:
            pnode = self._network_definition.pnodes[platform_id]
            dic = dict((attr.attr_id, attr.defn) for attr in pnode.attrs.itervalues())
            self._platform_attributes[platform_id] = dic
        log.trace("_platform_attributes: %s", self._platform_attributes)

        # set ports for the platforms:
        self._platform_ports = {}
        for platform_id in self._network_definition.pnodes:
            pnode = self._network_definition.pnodes[platform_id]
            dic = {}
            for port_id, port in pnode.ports.iteritems():
                dic[port_id] = dict(port_id=port_id,
                                    network=port.network)
            self._platform_ports[platform_id] = dic
        log.trace("_platform_ports: %s", self._platform_attributes)

        self._async_data_result = AsyncResult()
        self._data_subscribers = []
        self._samples_received = []
        self.addCleanup(self._stop_data_subscribers)

        self._async_event_result = AsyncResult()
        self._event_subscribers = []
        self._events_received = []
        self.addCleanup(self._stop_event_subscribers)
        self._start_event_subscriber()

    #################################################################
    # data subscribers handling
    #################################################################

    def _start_data_subscriber(self, stream_name, stream_id):
        """
        Starts data subscriber for the given stream_name and stream_config
        """

        def consume_data(message, stream_route, stream_id):
            # A callback for processing subscribed-to data.
            log.info('Subscriber received data message: %s. stream_name=%r stream_id=%r',
                     str(message), stream_name, stream_id)
            self._samples_received.append(message)
            self._async_data_result.set()

        log.info('_start_data_subscriber stream_name=%r stream_id=%r',
                 stream_name, stream_id)

        # Create subscription for the stream
        exchange_name = '%s_queue' % stream_name
        self.container.ex_manager.create_xn_queue(exchange_name).purge()
        sub = StandaloneStreamSubscriber(exchange_name, consume_data)
        sub.start()
        self._data_subscribers.append(sub)
        sub_id = self.PSC.create_subscription(name=exchange_name, stream_ids=[stream_id])
        self.PSC.activate_subscription(sub_id)
        sub.subscription_id = sub_id

    def _stop_data_subscribers(self):
        """
        Stop the data subscribers on cleanup.
        """
        try:
            for sub in self._data_subscribers:
                if hasattr(sub, 'subscription_id'):
                    try:
                        self.PSC.deactivate_subscription(sub.subscription_id)
                    except:
                        pass
                    self.PSC.delete_subscription(sub.subscription_id)
                sub.stop()
        finally:
            self._data_subscribers = []

    #################################################################
    # event subscribers handling
    #################################################################

    def _start_event_subscriber(self, event_type="DeviceEvent", sub_type="platform_event"):
        """
        Starts event subscriber for events of given event_type ("DeviceEvent"
        by default) and given sub_type ("platform_event" by default).
        """

        def consume_event(evt, *args, **kwargs):
            # A callback for consuming events.
            log.info('Event subscriber received evt: %s.', str(evt))
            self._events_received.append(evt)
            self._async_event_result.set(evt)

        sub = EventSubscriber(event_type=event_type,
            sub_type=sub_type,
            callback=consume_event)

        sub.start()
        log.info("registered event subscriber for event_type=%r, sub_type=%r",
            event_type, sub_type)

        self._event_subscribers.append(sub)
        sub._ready_event.wait(timeout=EVENT_TIMEOUT)

    def _stop_event_subscribers(self):
        """
        Stops the event subscribers on cleanup.
        """
        try:
            for sub in self._event_subscribers:
                if hasattr(sub, 'subscription_id'):
                    try:
                        self.PSC.deactivate_subscription(sub.subscription_id)
                    except:
                        pass
                    self.PSC.delete_subscription(sub.subscription_id)
                sub.stop()
        finally:
            self._event_subscribers = []

    #################################################################
    # config supporting methods
    #################################################################

    def _create_platform_config_builder(self):
        clients = DotDict()
        clients.resource_registry  = self.RR
        clients.pubsub_management  = self.PSC
        clients.dataset_management = self.DSC
        pconfig_builder = PlatformAgentConfigurationBuilder(clients)

        # can't do anything without an agent instance obj
        log.debug("Testing that preparing a launcher without agent instance raises an error")
        self.assertRaises(AssertionError, pconfig_builder.prepare, will_launch=False)

        return pconfig_builder

    def _generate_parent_with_child_config(self, p_parent, p_child):
        log.debug("Testing parent + child as parent config")
        pconfig_builder = self._create_platform_config_builder()
        pconfig_builder.set_agent_instance_object(p_parent.platform_agent_instance_obj)
        parent_config = pconfig_builder.prepare(will_launch=False)
        self._verify_parent_config(parent_config,
                                  p_parent.platform_device_id,
                                  p_child.platform_device_id)

        self._debug_config(parent_config,
                           "platform_agent_config_%s_->_%s.txt" % (
                           p_parent.platform_id, p_child.platform_id))

    def _generate_config(self, platform_agent_instance_obj, platform_id, suffix=''):
        pconfig_builder = self._create_platform_config_builder()
        pconfig_builder.set_agent_instance_object(platform_agent_instance_obj)
        config = pconfig_builder.prepare(will_launch=False)

        self._debug_config(config, "platform_agent_config_%s%s.txt" % (platform_id, suffix))

        return config

    def _get_platform_stream_configs(self):
        """
        This method is an adaptation of get_streamConfigs in
        test_driver_egg.py
        """
        return [
            StreamConfiguration(stream_name='parsed',
                                parameter_dictionary_name='platform_eng_parsed',
                                records_per_granule=2,
                                granule_publish_rate=5)

            # TODO include a "raw" stream?
        ]

    def _debug_config(self, config, outname):
        if log.isEnabledFor(logging.DEBUG):
            import pprint
            pprint.PrettyPrinter(stream=file(outname, "w")).pprint(config)
            log.debug("config pretty-printed to %s", outname)

    def _verify_child_config(self, config, device_id):
        for key in required_config_keys:
            self.assertIn(key, config)
        self.assertEqual(RT.PlatformDevice, config['device_type'])
        for key in DVR_CONFIG.iterkeys():
            self.assertIn(key, config['driver_config'])
        self.assertEqual({'resource_id': device_id}, config['agent'])
        self.assertIn('stream_config', config)

        for key in ['alarm_defs', 'children', 'startup_config']:
            self.assertEqual({}, config[key])

    def _verify_parent_config(self, config, parent_device_id, child_device_id):
        for key in required_config_keys:
            self.assertIn(key, config)
        self.assertEqual(RT.PlatformDevice, config['device_type'])
        for key in DVR_CONFIG.iterkeys():
            self.assertIn(key, config['driver_config'])
        self.assertEqual({'resource_id': parent_device_id}, config['agent'])
        self.assertIn('stream_config', config)
        for key in ['alarm_defs', 'startup_config']:
            self.assertEqual({}, config[key])

        self.assertIn(child_device_id, config['children'])
        self._verify_child_config(config['children'][child_device_id], child_device_id)

    def _create_platform_configuration(self, platform_id, parent_platform_id=None):
        """
        This method is an adaptation of test_agent_instance_config in
        test_instrument_management_service_integration.py

        @param platform_id
        @param parent_platform_id
        @return a DotDict with various of the constructed elements associated
                to the platform.
        """

        tdom, sdom = time_series_domain()
        sdom = sdom.dump()
        tdom = tdom.dump()

        #
        # TODO will each platform have its own param dictionary?
        #
        param_dict_name = 'platform_eng_parsed'
        parsed_rpdict_id = self.DSC.read_parameter_dictionary_by_name(
            param_dict_name,
            id_only=True)
        self.parsed_stream_def_id = self.PSC.create_stream_definition(
            name='parsed',
            parameter_dictionary_id=parsed_rpdict_id)

        def _make_platform_agent_structure(agent_config=None):
            if None is agent_config: agent_config = {}

            driver_config = copy.deepcopy(DVR_CONFIG)
            driver_config['attributes'] = self._platform_attributes[platform_id]
            driver_config['ports']      = self._platform_ports[platform_id]
            log.debug("driver_config: %s", driver_config)

            # instance creation
            platform_agent_instance_obj = any_old(RT.PlatformAgentInstance, {
                'driver_config': driver_config})
            platform_agent_instance_obj.agent_config = agent_config
            platform_agent_instance_id = self.IMS.create_platform_agent_instance(platform_agent_instance_obj)

            # agent creation
            platform_agent_obj = any_old(RT.PlatformAgent, {
                "stream_configurations": self._get_platform_stream_configs(),
                'driver_module':         DVR_MOD,
                'driver_class':          DVR_CLS})
            platform_agent_id = self.IMS.create_platform_agent(platform_agent_obj)

            # device creation
            platform_device_id = self.IMS.create_platform_device(any_old(RT.PlatformDevice))

            # data product creation
            dp_obj = any_old(RT.DataProduct, {"temporal_domain":tdom, "spatial_domain": sdom})
            dp_id = self.DP.create_data_product(data_product=dp_obj, stream_definition_id=self.parsed_stream_def_id)
            self.DAMS.assign_data_product(input_resource_id=platform_device_id, data_product_id=dp_id)
            self.DP.activate_data_product_persistence(data_product_id=dp_id)

            # assignments
            self.RR2.assign_platform_agent_instance_to_platform_device(platform_agent_instance_id, platform_device_id)
            self.RR2.assign_platform_agent_to_platform_agent_instance(platform_agent_id, platform_agent_instance_id)
            self.RR2.assign_platform_device_to_org_with_has_resource(platform_agent_instance_id, self.org_id)

            #######################################
            # dataset

            log.debug('data product = %s', dp_id)

            stream_ids, _ = self.RR.find_objects(dp_id, PRED.hasStream, None, True)
            log.debug('Data product stream_ids = %s', stream_ids)
            stream_id = stream_ids[0]

            # Retrieve the id of the OUTPUT stream from the out Data Product
            dataset_ids, _ = self.RR.find_objects(dp_id, PRED.hasDataset, RT.Dataset, True)
            log.debug('Data set for data_product_id1 = %s', dataset_ids[0])
            #######################################

            return platform_agent_instance_id, platform_agent_id, platform_device_id, stream_id

        log.debug("Making the structure for a platform agent")

        # TODO Note: the 'platform_config' entry is a mechanism that the
        # platform agent expects to know the platform_id and parent_platform_id.
        # Determine how to finally indicate this info.
        platform_config = {
            'platform_id':             platform_id,
            'parent_platform_id':      parent_platform_id,
        }

        child_agent_config = {
            'platform_config': platform_config
        }
        platform_agent_instance_child_id, _, platform_device_child_id, stream_id = \
            _make_platform_agent_structure(child_agent_config)

        platform_agent_instance_child_obj = self.RR2.read(platform_agent_instance_child_id)

        child_config = self._generate_config(platform_agent_instance_child_obj, platform_id)
        self._verify_child_config(child_config, platform_device_child_id)

        self.platform_device_parent_id = platform_device_child_id

        p_obj = DotDict()
        p_obj.platform_id = platform_id
        p_obj.parent_platform_id = parent_platform_id
        p_obj.agent_config = child_config
        p_obj.platform_agent_instance_obj = platform_agent_instance_child_obj
        p_obj.platform_device_id = platform_device_child_id
        p_obj.platform_agent_instance_id = platform_agent_instance_child_id
        p_obj.stream_id = stream_id
        return p_obj

    def _create_platform(self, platform_id, parent_platform_id=None):
        """
        The main method to create a platform configuration and do other
        preparations for a given platform.
        """
        p_obj = self._create_platform_configuration(platform_id, parent_platform_id)

        # start corresponding data subscriber:
        self._start_data_subscriber(p_obj.platform_agent_instance_id,
                                    p_obj.stream_id)

        return p_obj

    #################################################################
    # child-parent linking
    #################################################################

    def _assign_child_to_parent(self, p_child, p_parent):

        log.debug("assigning child platform %r to parent %r",
                  p_child.platform_id, p_parent.platform_id)

        self.RR2.assign_platform_device_to_platform_device(p_child.platform_device_id,
                                                           p_parent.platform_device_id)
        child_device_ids = self.RR2.find_platform_device_ids_of_device(p_parent.platform_device_id)
        self.assertNotEqual(0, len(child_device_ids))

        self._generate_parent_with_child_config(p_parent, p_child)

    #################################################################
    # some platform topologies
    #################################################################

    def _create_single_platform(self):
        """
        Creates and prepares a platform corresponding to the
        platform ID 'LJ01D', which is a leaf in the simulated network.
        """
        p_root = self._create_platform('LJ01D')
        return p_root

    def _create_small_hierarchy(self):
        """
        Creates a small platform network consisting of 3 platforms as follows:
          Node1D -> MJ01C -> LJ01D
        where -> goes from parent to child.
        """

        p_root       = self._create_platform('Node1D')
        p_child      = self._create_platform('MJ01C', parent_platform_id='Node1D')
        p_grandchild = self._create_platform('LJ01D', parent_platform_id='MJ01C')

        self._assign_child_to_parent(p_child, p_root)
        self._assign_child_to_parent(p_grandchild, p_child)

        self._generate_config(p_root.platform_agent_instance_obj, p_root.platform_id, "_final")

        return p_root

    #################################################################
    # start / stop platform
    #################################################################

    def _start_platform(self, agent_instance_id):
        log.debug("about to call start_platform_agent_instance with id=%s", agent_instance_id)
        pid = self.IMS.start_platform_agent_instance(platform_agent_instance_id=agent_instance_id)
        log.debug("start_platform_agent_instance returned pid=%s", pid)

        #wait for start
        agent_instance_obj = self.IMS.read_platform_agent_instance(agent_instance_id)
        gate = ProcessStateGate(self.PDC.read_process,
                                agent_instance_obj.agent_process_id,
                                ProcessStateEnum.RUNNING)
        self.assertTrue(gate.await(90), "The platform agent instance did not spawn in 90 seconds")

        # Start a resource agent client to talk with the agent.
        self._pa_client = ResourceAgentClient('paclient',
                                              name=agent_instance_obj.agent_process_id,
                                              process=FakeProcess())
        log.debug("got platform agent client %s", str(self._pa_client))

    def _stop_platform(self, agent_instance_id):
        self.IMS.stop_platform_agent_instance(platform_agent_instance_id=agent_instance_id)

    #################################################################
    # misc convenience methods
    #################################################################

    def _get_state(self):
        state = self._pa_client.get_agent_state()
        return state

    def _assert_state(self, state):
        self.assertEquals(self._get_state(), state)

    def _execute_agent(self, cmd):
        log.info("_execute_agent: cmd=%r kwargs=%r ...", cmd.command, cmd.kwargs)
        time_start = time.time()
        #retval = self._pa_client.execute_agent(cmd, timeout=timeout)
        retval = self._pa_client.execute_agent(cmd)
        elapsed_time = time.time() - time_start
        log.info("_execute_agent: cmd=%r elapsed_time=%s, retval = %s",
                 cmd.command, elapsed_time, str(retval))
        return retval

    #################################################################
    # commands that concrete tests can call
    #################################################################

    def _ping_agent(self):
        retval = self._pa_client.ping_agent()
        self.assertIsInstance(retval, str)

    def _ping_resource(self):
        cmd = AgentCommand(command=PlatformAgentEvent.PING_RESOURCE)
        if self._get_state() == PlatformAgentState.UNINITIALIZED:
            # should get ServerError: "Command not handled in current state"
            with self.assertRaises(ServerError):
                #self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
                self._pa_client.execute_agent(cmd)
        else:
            # In all other states the command should be accepted:
            retval = self._execute_agent(cmd)
            self.assertEquals("PONG", retval.result)

    def _get_metadata(self):
        cmd = AgentCommand(command=PlatformAgentEvent.GET_METADATA)
        retval = self._execute_agent(cmd)
        md = retval.result
        self.assertIsInstance(md, dict)
        # TODO verify possible subset of required entries in the dict.
        log.info("GET_METADATA = %s", md)

    def _get_ports(self):
        cmd = AgentCommand(command=PlatformAgentEvent.GET_PORTS)
        retval = self._execute_agent(cmd)
        md = retval.result
        self.assertIsInstance(md, dict)
        # TODO verify possible subset of required entries in the dict.
        log.info("GET_PORTS = %s", md)

    def _initialize(self):
        self._assert_state(PlatformAgentState.UNINITIALIZED)
        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE)
        retval = self._execute_agent(cmd)
        self._assert_state(PlatformAgentState.INACTIVE)

    def _go_active(self):
        cmd = AgentCommand(command=PlatformAgentEvent.GO_ACTIVE)
        retval = self._execute_agent(cmd)
        self._assert_state(PlatformAgentState.IDLE)

    def _run(self):
        cmd = AgentCommand(command=PlatformAgentEvent.RUN)
        retval = self._execute_agent(cmd)
        self._assert_state(PlatformAgentState.COMMAND)

    def _start_resource_monitoring(self):
        cmd = AgentCommand(command=PlatformAgentEvent.START_MONITORING)
        retval = self._execute_agent(cmd)
        self._assert_state(PlatformAgentState.MONITORING)

    def _wait_for_a_data_sample(self):
        log.info("waiting for reception of a data sample...")
        # just wait for at least one -- see consume_data
        self._async_data_result.get(timeout=DATA_TIMEOUT)
        self.assertTrue(len(self._samples_received) >= 1)
        log.info("Received samples: %s", len(self._samples_received))

    def _wait_for_external_event(self):
        log.info("waiting for reception of an external event...")
        # just wait for at least one -- see consume_event
        self._async_event_result.get(timeout=EVENT_TIMEOUT)
        self.assertTrue(len(self._events_received) >= 1)
        log.info("Received events: %s", len(self._events_received))

    def _stop_resource_monitoring(self):
        cmd = AgentCommand(command=PlatformAgentEvent.STOP_MONITORING)
        retval = self._execute_agent(cmd)
        self._assert_state(PlatformAgentState.COMMAND)

    def _pause(self):
        cmd = AgentCommand(command=PlatformAgentEvent.PAUSE)
        retval = self._execute_agent(cmd)
        self._assert_state(PlatformAgentState.STOPPED)

    def _resume(self):
        cmd = AgentCommand(command=PlatformAgentEvent.RESUME)
        retval = self._execute_agent(cmd)
        self._assert_state(PlatformAgentState.COMMAND)

    def _clear(self):
        cmd = AgentCommand(command=PlatformAgentEvent.CLEAR)
        retval = self._execute_agent(cmd)
        self._assert_state(PlatformAgentState.IDLE)

    def _go_inactive(self):
        cmd = AgentCommand(command=PlatformAgentEvent.GO_INACTIVE)
        retval = self._execute_agent(cmd)
        self._assert_state(PlatformAgentState.INACTIVE)

    def _reset(self):
        cmd = AgentCommand(command=PlatformAgentEvent.RESET)
        retval = self._execute_agent(cmd)
        self._assert_state(PlatformAgentState.UNINITIALIZED)

    def _check_sync(self):
        cmd = AgentCommand(command=PlatformAgentEvent.CHECK_SYNC)
        retval = self._execute_agent(cmd)
        log.info("CHECK_SYNC result: %s", retval.result)
        self.assertTrue(retval.result is not None)
        self.assertEquals(retval.result[0:3], "OK:")
        return retval.result

