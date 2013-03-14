#!/usr/bin/env python

"""
@package ion.services.sa.observatory.test.test_platform_launch.py
@file    ion/services/sa/observatory/test/test_platform_launch.py
@author  Carlos Rueda, Maurice Manning, Ian Katz
@brief   Test cases for launching platform agent network
"""

__author__ = 'Carlos Rueda, Maurice Manning, Ian Katz'
__license__ = 'Apache 2.0'

#
# The original set-up of this test suite adapted pieces from various sources:
# - test_instrument_management_service_integration.py
# - test_driver_egg.py
# - test_oms_launch2.py  (now deprecated).

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
# containing the corresponding platform agent configurations.


# developer conveniences:
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_single_platform
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_hierarchy

from pyon.public import log
import logging
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

from ion.agents.platform.rsn.oms_client_factory import CIOMSClientFactory
from ion.agents.platform.rsn.oms_util import RsnOmsUtil
from ion.agents.platform.util.network_util import NetworkUtil

from ion.services.cei.process_dispatcher_service import ProcessStateGate
import os

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
    'dvr_mod': 'ion.agents.platform.rsn.rsn_platform_driver',
    'dvr_cls': 'RSNPlatformDriver',
    'oms_uri': os.getenv('OMS', 'embsimulator'),
}

# TODO clean up the use of 'driver_config' entry in the platform_config,
# which is also set at the agent level (with a wrong definition BTW).

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
class TestPlatformLaunch(IonIntegrationTestCase):

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
        if log.isEnabledFor(logging.TRACE):
            log.trace("NetworkDefinition serialization:\n%s", self._network_definition_ser)


        self._async_data_result = AsyncResult()
        self._data_subscribers = []
        self._samples_received = []
        self.addCleanup(self._stop_data_subscribers)

        self._async_event_result = AsyncResult()
        self._event_subscribers = []
        self._events_received = []
        self.addCleanup(self._stop_event_subscribers)
        self._start_event_subscriber()

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
        self.assertEqual({'process_type': ('ZMQPyClassDriverLauncher',)}, config['driver_config'])
        self.assertEqual({'resource_id': device_id}, config['agent'])
        self.assertIn('stream_config', config)

        for key in ['alarm_defs', 'children', 'startup_config']:
            self.assertEqual({}, config[key])

    def _verify_parent_config(self, config, parent_device_id, child_device_id):
        for key in required_config_keys:
            self.assertIn(key, config)
        self.assertEqual(RT.PlatformDevice, config['device_type'])
        self.assertEqual({'process_type': ('ZMQPyClassDriverLauncher',)}, config['driver_config'])
        self.assertEqual({'resource_id': parent_device_id}, config['agent'])
        self.assertIn('stream_config', config)
        for key in ['alarm_defs', 'startup_config']:
            self.assertEqual({}, config[key])

        self.assertIn(child_device_id, config['children'])
        self._verify_child_config(config['children'][child_device_id], child_device_id)

    def _create_platform_configuration(self, platform_id):
        """
        This method is an adaptation of test_agent_instance_config in
        test_instrument_management_service_integration.py

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

            # instance creation
            platform_agent_instance_obj = any_old(RT.PlatformAgentInstance)
            platform_agent_instance_obj.agent_config = agent_config
            platform_agent_instance_id = self.IMS.create_platform_agent_instance(platform_agent_instance_obj)

            # agent creation
            platform_agent_obj = any_old(RT.PlatformAgent, {"stream_configurations":self._get_platform_stream_configs()})
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

        log.debug("Making the structure for a platform agent, which will be the child")
        child_agent_config = {
            'platform_config': {

                # TODO this id is temporary
                'platform_id':             platform_id,

                'driver_config':           DVR_CONFIG,
                'network_definition' :     self._network_definition_ser
            }
        }
        platform_agent_instance_child_id, _, platform_device_child_id, stream_id = \
            _make_platform_agent_structure(child_agent_config)

        platform_agent_instance_child_obj = self.RR2.read(platform_agent_instance_child_id)

        child_config = self._generate_config(platform_agent_instance_child_obj, platform_id)
        self._verify_child_config(child_config, platform_device_child_id)

        self.platform_device_parent_id = platform_device_child_id

        p_obj = DotDict()
        p_obj.platform_id = platform_id
        p_obj.agent_config = child_config
        p_obj.platform_agent_instance_obj = platform_agent_instance_child_obj
        p_obj.platform_device_id = platform_device_child_id
        p_obj.platform_agent_instance_id = platform_agent_instance_child_id
        p_obj.stream_id = stream_id
        return p_obj

    def _create_platform(self, platform_id):
        """
        The main method to create a platform configuration and do other
        preparations for a given platform.
        """
        p_obj = self._create_platform_configuration(platform_id)

        # start corresponding data subscriber:
        self._start_data_subscriber(p_obj.platform_agent_instance_id,
                                    p_obj.stream_id)

        return p_obj

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

    def _run_commands(self):

        # ping_agent can be issued before INITIALIZE
        retval = self._pa_client.ping_agent(timeout=TIMEOUT)
        log.debug('Base Platform ping_agent = %s', str(retval))

        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE)
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug('Base Platform INITIALIZE = %s', str(retval))

        # GO_ACTIVE
        cmd = AgentCommand(command=PlatformAgentEvent.GO_ACTIVE)
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug('Base Platform GO_ACTIVE = %s', str(retval))

        # RUN:
        cmd = AgentCommand(command=PlatformAgentEvent.RUN)
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug('Base Platform RUN = %s', str(retval))

        # START_MONITORING:
        cmd = AgentCommand(command=PlatformAgentEvent.START_MONITORING)
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug('Base Platform START_MONITORING = %s', str(retval))

        # wait for data sample
        # just wait for at least one -- see consume_data above
        log.info("waiting for reception of a data sample...")
        self._async_data_result.get(timeout=DATA_TIMEOUT)
        self.assertTrue(len(self._samples_received) >= 1)
        log.info("Received data samples: %s", len(self._samples_received))

        # wait for event
        # just wait for at least one event -- see consume_event above
        log.info("waiting for reception of an event...")
        self._async_event_result.get(timeout=EVENT_TIMEOUT)
        log.info("Received events: %s", len(self._events_received))

        # STOP_MONITORING:
        cmd = AgentCommand(command=PlatformAgentEvent.STOP_MONITORING)
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug('Base Platform STOP_MONITORING = %s', str(retval))

        # GO_INACTIVE
        cmd = AgentCommand(command=PlatformAgentEvent.GO_INACTIVE)
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug('Base Platform GO_INACTIVE = %s', str(retval))

        # RESET: Resets the base platform agent, which includes termination of
        # its sub-platforms processes:
        cmd = AgentCommand(command=PlatformAgentEvent.RESET)
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug('Base Platform RESET = %s', str(retval))

    def _stop_platform(self, agent_instance_id):
        self.IMS.stop_platform_agent_instance(platform_agent_instance_id=agent_instance_id)

    def test_single_platform(self):
        #
        # Tests the launch of a single platform, corresponding to the
        # platform ID 'LJ01D', which is a leaf in the simulated network.
        #

        p_root = self._create_platform('LJ01D')

        self._start_platform(p_root.platform_agent_instance_id)
        self._run_commands()
        self._stop_platform(p_root.platform_agent_instance_id)

    def _assign_child_to_parent(self, p_child, p_parent):

        log.debug("assigning child platform %r to parent %r",
                  p_child.platform_id, p_parent.platform_id)

        self.RR2.assign_platform_device_to_platform_device(p_child.platform_device_id,
                                                           p_parent.platform_device_id)
        child_device_ids = self.RR2.find_platform_device_ids_of_device(p_parent.platform_device_id)
        self.assertNotEqual(0, len(child_device_ids))

        self._generate_parent_with_child_config(p_parent, p_child)

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

    def test_hierarchy(self):
        #
        # Tests a small platform network consisting of 3 platforms as follows:
        #   Node1D -> MJ01C -> LJ01D
        # where -> goes from parent to child.

        p_root       = self._create_platform('Node1D')
        p_child      = self._create_platform('MJ01C')
        p_grandchild = self._create_platform('LJ01D')

        self._assign_child_to_parent(p_child, p_root)
        self._assign_child_to_parent(p_grandchild, p_child)

        self._generate_config(p_root.platform_agent_instance_obj, p_root.platform_id, "_final")

        self._start_platform(p_root.platform_agent_instance_id)
        self._run_commands()
        self._stop_platform(p_root.platform_agent_instance_id)
