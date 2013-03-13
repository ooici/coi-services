#!/usr/bin/env python

"""
@package ion.services.sa.observatory.test.test_platform_launch.py
@file    ion/services/sa/observatory/test/test_platform_launch.py
@author  Carlos Rueda, Maurice Manning
@brief   Test cases for launching platform agent network
"""

__author__ = 'Carlos Rueda, Maurice Manning'
__license__ = 'Apache 2.0'

#
# The set-up of this test suite adapts pieces from various sources, including:
# - test_instrument_management_service_integration.py
# - test_driver_egg.py
# - test_oms_launch2.py  (now deprecated).
#
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_single_platform
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_hierarchy
#

from pyon.public import log, IonObject
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
from gevent import sleep

from ion.agents.platform.rsn.oms_client_factory import CIOMSClientFactory
from ion.agents.platform.rsn.oms_util import RsnOmsUtil
from ion.agents.platform.util.network_util import NetworkUtil

from ion.services.cei.process_dispatcher_service import ProcessStateGate
from unittest import skip
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
# which also set at the agent level (with a wrong definition BTW).

# TIMEOUT: timeout for each execute_agent call.
TIMEOUT = 90

# DATA_TIMEOUT: timeout for reception of data sample
DATA_TIMEOUT = 25

# EVENT_TIMEOUT: timeout for reception of event
EVENT_TIMEOUT = 25


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


        self.parsed_stream_def_id = self._create_parsed_stream_definition()

        tdom, sdom = time_series_domain()
        self.sdom = sdom.dump()
        self.tdom = tdom.dump()

        self.org_id = self.RR2.create(any_old(RT.Org))

        self.inst_startup_config = {'startup': 'config'}

        self.required_config_keys = [
            'org_name',
            'device_type',
            'agent',
            'driver_config',
            'stream_config',
            'startup_config',
            'alarm_defs',
            'children']



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

    def _create_parsed_stream_definition(self):
        parsed_rpdict_id = self.DSC.read_parameter_dictionary_by_name(
            'platform_eng_parsed',
            id_only=True)

        return self.PSC.create_stream_definition(name='parsed',
                                                 parameter_dictionary_id=parsed_rpdict_id)

    def _start_data_subscriber(self, stream_name, stream_id):
        """
        Starts data subscriber for the given stream_name and stream_config
        """

        def consume_data(message, stream_route, stream_id):
            # A callback for processing subscribed-to data.
            log.info('Subscriber received data message: %s.', str(message))
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

    def _verify_child_config(self, config, device_id):
        for key in self.required_config_keys:
            self.assertIn(key, config)
        self.assertEqual('Org_1', config['org_name'])
        self.assertEqual(RT.PlatformDevice, config['device_type'])
        self.assertEqual({'process_type': ('ZMQPyClassDriverLauncher',)}, config['driver_config'])
        self.assertEqual({'resource_id': device_id}, config['agent'])
        self.assertIn('stream_config', config)

        for key in ['alarm_defs', 'children', 'startup_config']:
            self.assertEqual({}, config[key])

    def _verify_parent_config(self, config, parent_device_id, child_device_id):
        for key in self.required_config_keys:
            self.assertIn(key, config)
        self.assertEqual('Org_1', config['org_name'])
        self.assertEqual(RT.PlatformDevice, config['device_type'])
        self.assertEqual({'process_type': ('ZMQPyClassDriverLauncher',)}, config['driver_config'])
        self.assertEqual({'resource_id': parent_device_id}, config['agent'])
        self.assertIn('stream_config', config)
        for key in ['alarm_defs', 'startup_config']:
            self.assertEqual({}, config[key])

        self.assertIn(child_device_id, config['children'])
        self._verify_child_config(config['children'][child_device_id], child_device_id)

    def _create_platform(self, platform_id):
        agent_obj = any_old(RT.PlatformAgent, {
            #'name'       : '%s_PlatformAgent' % platform_id,
            'description': '%s_PlatformAgent platform agent' % platform_id,
            "stream_configurations": self._get_platform_stream_configs()
        })
        agent_id = self.IMS.create_platform_agent(agent_obj)
        return agent_obj, agent_id

    def _make_platform_agent_structure(self, platform_id, agent_config=None):
        if None is agent_config: agent_config = {}

        # instance creation
        platform_agent_instance_obj = any_old(RT.PlatformAgentInstance)
        platform_agent_instance_obj.agent_config = agent_config
        platform_agent_instance_id = self.IMS.create_platform_agent_instance(platform_agent_instance_obj)

        # agent creation
        platform_agent_obj, platform_agent_id = self._create_platform(platform_id)

        # device creation
        platform_device_id = self.IMS.create_platform_device(any_old(RT.PlatformDevice))

        # data product creation
        dp_obj = any_old(RT.DataProduct, {"temporal_domain":self.tdom,
                                          "spatial_domain": self.sdom})
        dp_id = self.DP.create_data_product(data_product=dp_obj, stream_definition_id=self.parsed_stream_def_id)
        self.DAMS.assign_data_product(input_resource_id=platform_device_id, data_product_id=dp_id)
        self.DP.activate_data_product_persistence(data_product_id=dp_id)

        # assignments
        self.RR2.assign_platform_agent_instance_to_platform_device(platform_agent_instance_id, platform_device_id)
        self.RR2.assign_platform_agent_to_platform_agent_instance(platform_agent_id, platform_agent_instance_id)
        self.RR2.assign_platform_device_to_org_with_has_resource(platform_agent_instance_id, self.org_id)

        return platform_agent_instance_id, platform_agent_id, platform_device_id

    def _create_platform_config_builder(self):
        clients = DotDict()
        clients.resource_registry  = self.RR
        clients.pubsub_management  = self.PSC
        clients.dataset_management = self.DSC
        pconfig_builder = PlatformAgentConfigurationBuilder(clients)
        return pconfig_builder

    def _create_single_platform_configuration(self, platform_id):
        pconfig_builder = self._create_platform_config_builder()

        # can't do anything without an agent instance obj
        log.debug("Testing that preparing a launcher without agent instance raises an error")
        self.assertRaises(AssertionError, pconfig_builder.prepare, will_launch=False)

        log.debug("Making the structure for a platform agent, which will be the child")
        child_agent_config = {
            'platform_id': platform_id,
            'platform_config': {

                # TODO this id is temporary
                'platform_id':             platform_id,

                'driver_config':           DVR_CONFIG,
                'network_definition' :     self._network_definition_ser
            }
        }
        platform_agent_instance_child_id, _, platform_device_child_id = \
            self._make_platform_agent_structure(platform_id, child_agent_config)
        platform_agent_instance_child_obj = self.RR2.read(platform_agent_instance_child_id)

        log.debug("Preparing a valid agent instance launch, for config only")
        pconfig_builder.set_agent_instance_object(platform_agent_instance_child_obj)
        child_config = pconfig_builder.prepare(will_launch=False)
        self._verify_child_config(child_config, platform_device_child_id)
        self._debug_config(child_config, "%s_config.txt" % platform_id)

        return child_config, platform_agent_instance_child_id, platform_device_child_id

    def _create_platform_configuration(self, platform_id):
        """
        Verify that agent configurations are being built properly
        """
        #
        # This method is an adaptation of test_agent_instance_config in
        # test_instrument_management_service_integration.py
        #

        clients = DotDict()
        clients.resource_registry  = self.RR
        clients.pubsub_management  = self.PSC
        clients.dataset_management = self.DSC
        pconfig_builder = PlatformAgentConfigurationBuilder(clients)

        rpdict_id = self.DSC.read_parameter_dictionary_by_name('ctd_raw_param_dict', id_only=True)
        self.raw_stream_def_id = self.PSC.create_stream_definition(name='raw', parameter_dictionary_id=rpdict_id)


        # can't do anything without an agent instance obj
        log.debug("Testing that preparing a launcher without agent instance raises an error")
        self.assertRaises(AssertionError, pconfig_builder.prepare, will_launch=False)

        log.debug("Making the structure for a platform agent, which will be the child")
        child_platform_id = "THE_CHILD_PLATFORM"
        child_agent_config = {
            'platform_id': child_platform_id,
            'platform_config': {

                # TODO this id is temporary
                'platform_id':             child_platform_id,

                'driver_config':           DVR_CONFIG,
                'network_definition' :     self._network_definition_ser
            }
        }
        platform_agent_instance_child_id, _, platform_device_child_id = \
            self._make_platform_agent_structure(child_platform_id, child_agent_config)
        platform_agent_instance_child_obj = self.RR2.read(platform_agent_instance_child_id)

        log.debug("Preparing a valid agent instance launch, for config only")
        pconfig_builder.set_agent_instance_object(platform_agent_instance_child_obj)
        child_config = pconfig_builder.prepare(will_launch=False)
        self._verify_child_config(child_config, platform_device_child_id)
        self._debug_config(child_config, "child_config.txt")

        log.debug("Making the structure for a platform agent, which will be the parent")
        parent_platform_id = "THE_MIDDLE_PLATFORM"
        parent_agent_config = {
            'platform_id': parent_platform_id,
            'platform_config': {

                # TODO this id is temporary
                'platform_id':             parent_platform_id,

                'driver_config':           DVR_CONFIG,
                'network_definition' :     self._network_definition_ser
            }
        }
        platform_agent_instance_parent_id, _, platform_device_parent_id = \
            self._make_platform_agent_structure(parent_platform_id, parent_agent_config)
        platform_agent_instance_parent_obj = self.RR2.read(platform_agent_instance_parent_id)

        log.debug("Testing child-less parent as a child config")
        pconfig_builder.set_agent_instance_object(platform_agent_instance_parent_obj)
        parent_config1 = pconfig_builder.prepare(will_launch=False)
        self._verify_child_config(parent_config1, platform_device_parent_id)
        self._debug_config(parent_config1, "parent_config1.txt")

        log.debug("assigning child platform to parent")
        self.RR2.assign_platform_device_to_platform_device(platform_device_child_id, platform_device_parent_id)
        self.platform_device_parent_id = platform_device_parent_id
        child_device_ids = self.RR2.find_platform_device_ids_of_device(platform_device_parent_id)
        self.assertNotEqual(0, len(child_device_ids))

        log.debug("Testing parent + child as parent config")
        platform_agent_instance_parent_obj = self.RR2.read(platform_agent_instance_parent_id)
        pconfig_builder.set_agent_instance_object(platform_agent_instance_parent_obj)
        parent_config2 = pconfig_builder.prepare(will_launch=False)
        self._verify_parent_config(parent_config2, platform_device_parent_id, platform_device_child_id)
        self._debug_config(parent_config2, "parent_config2.txt")

        log.debug("Testing entire config")
        platform_agent_instance_parent_obj = self.RR2.read(platform_agent_instance_parent_id)
        pconfig_builder.set_agent_instance_object(platform_agent_instance_parent_obj)
        agent_config = pconfig_builder.prepare(will_launch=False)
        self._verify_parent_config(agent_config, platform_device_parent_id, platform_device_child_id)

        # agent_config['platform_id'] = platform_id
        # agent_config['platform_config'] = {
        #     'platform_id':             platform_id,
        #     'driver_config':           DVR_CONFIG,
        #     'network_definition' :     self._network_definition_ser
        # }

        self._debug_config(agent_config, "complete_agent_config.txt")

        return agent_config

    def _debug_config(self, config, outname):  # pragma: no cover
        # if log.isEnabledFor(logging.DEBUG):
        import pprint
        pprint.PrettyPrinter(stream=file(outname, "w")).pprint(config)
        log.debug("config pretty-printed to %s", outname)

    def _get_platform_stream_configs(self):
        #
        # This method is an adaptation of get_streamConfigs in
        # test_driver_egg.py
        #
        return [
            StreamConfiguration(stream_name='parsed',
                                parameter_dictionary_name='platform_eng_parsed',
                                records_per_granule=2,
                                granule_publish_rate=5)
            # TODO "raw" stream also needed?
        ]

    def _create_and_assign_parsed_data_product_to_device(self, device_id):
        """
        @return stream_id
        """

        #######################################
        # data product  (adapted from test_instrument_management_service_integration)
        dp_obj = IonObject(RT.DataProduct,
            name='the parsed data',
            description='DataProduct test',
            processing_level_code='Parsed_Canonical',
            temporal_domain = self.tdom,
            spatial_domain = self.sdom)
        data_product_id1 = self.DP.create_data_product(data_product=dp_obj,
                                                       stream_definition_id=self.parsed_stream_def_id)
        log.debug('data_product_id1 = %s', data_product_id1)

        self.DAMS.assign_data_product(input_resource_id=device_id, data_product_id=data_product_id1)
        self.DP.activate_data_product_persistence(data_product_id=data_product_id1)

        #######################################
        # dataset

        stream_ids, _ = self.RR.find_objects(data_product_id1, PRED.hasStream, None, True)
        log.debug( 'Data product stream_ids = %s', stream_ids)
        stream_id = stream_ids[0]

        # Retrieve the id of the OUTPUT stream from the out Data Product
        dataset_ids, _ = self.RR.find_objects(data_product_id1, PRED.hasDataset, RT.Dataset, True)
        log.debug('Data set for data_product_id1 = %s', dataset_ids[0])
        self.parsed_dataset = dataset_ids[0]

        return stream_id

    def _start_platform(self, agent_instance_id):
        log.debug("about to call imsclient.start_platform_agent_instance with id=%s", agent_instance_id)
        pid = self.IMS.start_platform_agent_instance(platform_agent_instance_id=agent_instance_id)
        log.debug("start_platform_agent_instance returned pid=%s", pid)

        #wait for start
        instance_obj = self.IMS.read_platform_agent_instance(agent_instance_id)
        gate = ProcessStateGate(self.PDC.read_process,
                                instance_obj.agent_process_id,
                                ProcessStateEnum.RUNNING)
        self.assertTrue(gate.await(90), "The platform agent instance did not spawn in 90 seconds")

        agent_instance_obj= self.IMS.read_instrument_agent_instance(agent_instance_id)
        log.debug('Platform agent instance obj')

        # Start a resource agent client to talk with the instrument agent.
        self._pa_client = ResourceAgentClient('paclient',
                                              name=agent_instance_obj.agent_process_id,
                                              process=FakeProcess())
        log.debug("got platform agent client %s", str(self._pa_client))

    def _run_commands(self):
        # ping_agent can be issued before INITIALIZE
        retval = self._pa_client.ping_agent(timeout=TIMEOUT)
        log.debug('Base Platform ping_agent = %s', str(retval) )

        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE)
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug( 'Base Platform INITIALIZE = %s', str(retval) )


        # GO_ACTIVE
        cmd = AgentCommand(command=PlatformAgentEvent.GO_ACTIVE)
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug( 'Base Platform GO_ACTIVE = %s', str(retval) )

        # RUN:
        cmd = AgentCommand(command=PlatformAgentEvent.RUN)
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug( 'Base Platform RUN = %s', str(retval) )

        # START_MONITORING:
        cmd = AgentCommand(command=PlatformAgentEvent.START_MONITORING)
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug( 'Base Platform START_MONITORING = %s', str(retval) )

        # wait for data sample
        # just wait for at least one -- see consume_data above
        log.info("waiting for reception of a data sample...")
        self._async_data_result.get(timeout=DATA_TIMEOUT)
        self.assertTrue(len(self._samples_received) >= 1)

        log.info("waiting a bit more for reception of more data samples...")
        sleep(15)
        log.info("Got data samples: %d", len(self._samples_received))


        # wait for event
        # just wait for at least one event -- see consume_event above
        log.info("waiting for reception of an event...")
        self._async_event_result.get(timeout=EVENT_TIMEOUT)
        log.info("Received events: %s", len(self._events_received))

        #get the extended platfrom which wil include platform aggreate status fields
        # extended_platform = self.IMS.get_platform_device_extension(device_id)
        # log.debug( 'test_single_platform   extended_platform: %s', str(extended_platform) )
        # log.debug( 'test_single_platform   power_status_roll_up: %s', str(extended_platform.computed.power_status_roll_up.value) )
        # log.debug( 'test_single_platform   comms_status_roll_up: %s', str(extended_platform.computed.communications_status_roll_up.value) )

        # STOP_MONITORING:
        cmd = AgentCommand(command=PlatformAgentEvent.STOP_MONITORING)
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug( 'Base Platform STOP_MONITORING = %s', str(retval) )

        # GO_INACTIVE
        cmd = AgentCommand(command=PlatformAgentEvent.GO_INACTIVE)
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug( 'Base Platform GO_INACTIVE = %s', str(retval) )

        # RESET: Resets the base platform agent, which includes termination of
        # its sub-platforms processes:
        cmd = AgentCommand(command=PlatformAgentEvent.RESET)
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug( 'Base Platform RESET = %s', str(retval) )

    def _stop_platform(self, agent_instance_id):
        self.IMS.stop_platform_agent_instance(platform_agent_instance_id=agent_instance_id)

    def test_single_platform(self):

        platform_id = 'LJ01D'
        agent_config, agent_instance_id, device_id = self._create_single_platform_configuration(platform_id)

        stream_id = self._create_and_assign_parsed_data_product_to_device(device_id)

        self._start_data_subscriber(agent_instance_id, stream_id)

        self._start_platform(agent_instance_id)
        self._run_commands()
        self._stop_platform(agent_instance_id)


    @skip("Still needs alignment with new configuration structure")
    def test_hierarchy(self):

        platform_id = 'LJ01D'
        agent_config, platform_device_child_id = self._create_single_platform_configuration(platform_id)

        agent__obj, agent_id = self._create_platform(platform_id)

        device__obj = IonObject(RT.PlatformDevice,
            name='%s_PlatformDevice' % platform_id,
            description='%s_PlatformDevice platform device' % platform_id,
            #                        ports=port_objs,
            #                        platform_monitor_attributes = monitor_attribute_objs
        )

        device_id = self.IMS.create_platform_device(device__obj)


        #######################################
        # data product  (adapted from test_instrument_management_service_integration)
        dp_obj = IonObject(RT.DataProduct,
            name='the parsed data',
            description='DataProduct test',
            processing_level_code='Parsed_Canonical',
            temporal_domain = self.tdom,
            spatial_domain = self.sdom)
        data_product_id1 = self.DP.create_data_product(data_product=dp_obj, stream_definition_id=self.parsed_stream_def_id)
        log.debug('data_product_id1 = %s', data_product_id1)

        self.DAMS.assign_data_product(input_resource_id=device_id, data_product_id=data_product_id1)
        self.DP.activate_data_product_persistence(data_product_id=data_product_id1)
        #######################################

        #######################################
        # dataset

        stream_ids, _ = self.RR.find_objects(data_product_id1, PRED.hasStream, None, True)
        log.debug( 'Data product stream_ids = %s', stream_ids)
        stream_id = stream_ids[0]

        # Retrieve the id of the OUTPUT stream from the out Data Product
        dataset_ids, _ = self.RR.find_objects(data_product_id1, PRED.hasDataset, RT.Dataset, True)
        log.debug('Data set for data_product_id1 = %s', dataset_ids[0])
        self.parsed_dataset = dataset_ids[0]
        #######################################

        agent_instance_obj = IonObject(RT.PlatformAgentInstance,
            name='%s_PlatformAgentInstance' % platform_id,
            description="%s_PlatformAgentInstance" % platform_id,
            agent_config=agent_config)


        agent_instance_id = self.IMS.create_platform_agent_instance(
            platform_agent_instance = agent_instance_obj,
            platform_agent_id = agent_id,
            platform_device_id = device_id)


        self.RR2.assign_platform_device_to_platform_device(
            platform_device_child_id,
            device_id
        )
        child_device_ids = self.RR2.find_platform_device_ids_of_device(self.platform_device_parent_id)
        self.assertNotEqual(0, len(child_device_ids))

        self._start_data_subscriber(agent_instance_id, stream_id)

        log.debug("about to call imsclient.start_platform_agent_instance with id=%s", agent_instance_id)
        pid = self.IMS.start_platform_agent_instance(platform_agent_instance_id=agent_instance_id)
        log.debug("start_platform_agent_instance returned pid=%s", pid)

        #wait for start
        instance_obj = self.IMS.read_platform_agent_instance(agent_instance_id)
        gate = ProcessStateGate(self.PDC.read_process,
                                instance_obj.agent_process_id,
                                ProcessStateEnum.RUNNING)
        self.assertTrue(gate.await(90), "The platform agent instance did not spawn in 90 seconds")



        agent_instance_obj= self.IMS.read_instrument_agent_instance(agent_instance_id)
        log.debug('Platform agent instance obj')

        # Start a resource agent client to talk with the instrument agent.
        self._pa_client = ResourceAgentClient('paclient',
                                              name=agent_instance_obj.agent_process_id,
                                              process=FakeProcess())
        log.debug("got platform agent client %s", str(self._pa_client))


        self._run_commands(agent_instance_id)

        #-------------------------------
        # Stop Base Platform AgentInstance
        #-------------------------------
        self.IMS.stop_platform_agent_instance(platform_agent_instance_id=agent_instance_id)
