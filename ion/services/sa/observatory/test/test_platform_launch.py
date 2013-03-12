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
from interface.services.sa.iobservatory_management_service import  ObservatoryManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient

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

from ion.services.sa.instrument.agent_configuration_builder import PlatformAgentConfigurationBuilder, InstrumentAgentConfigurationBuilder
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

    def _create_platform_configuration(self):
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
        iconfig_builder = InstrumentAgentConfigurationBuilder(clients)


        tdom, sdom = time_series_domain()
        sdom = sdom.dump()
        tdom = tdom.dump()

        org_id = self.RR2.create(any_old(RT.Org))

        inst_startup_config = {'startup': 'config'}

        required_config_keys = [
            'org_name',
            'device_type',
            'agent',
            'driver_config',
            'stream_config',
            'startup_config',
            'alarm_defs',
            'children']



        def verify_instrument_config(config, device_id):
            for key in required_config_keys:
                self.assertIn(key, config)
            self.assertEqual('Org_1', config['org_name'])
            self.assertEqual(RT.InstrumentDevice, config['device_type'])
            self.assertIn('driver_config', config)
            driver_config = config['driver_config']
            expected_driver_fields = {'process_type': ('ZMQPyClassDriverLauncher',),
                                      }
            for k, v in expected_driver_fields.iteritems():
                self.assertIn(k, driver_config)
                self.assertEqual(v, driver_config[k])
            self.assertEqual

            self.assertEqual({'resource_id': device_id}, config['agent'])
            self.assertEqual(inst_startup_config, config['startup_config'])
            self.assertIn('stream_config', config)
            for key in ['alarm_defs', 'children']:
                self.assertEqual({}, config[key])


        def verify_child_config(config, device_id, inst_device_id=None):
            for key in required_config_keys:
                self.assertIn(key, config)
            self.assertEqual('Org_1', config['org_name'])
            self.assertEqual(RT.PlatformDevice, config['device_type'])
            self.assertEqual({'process_type': ('ZMQPyClassDriverLauncher',)}, config['driver_config'])
            self.assertEqual({'resource_id': device_id}, config['agent'])
            self.assertIn('stream_config', config)

            if None is inst_device_id:
                for key in ['alarm_defs', 'children', 'startup_config']:
                    self.assertEqual({}, config[key])
            else:
                for key in ['alarm_defs', 'startup_config']:
                    self.assertEqual({}, config[key])

                self.assertIn(inst_device_id, config['children'])
                verify_instrument_config(config['children'][inst_device_id], inst_device_id)


        def verify_parent_config(config, parent_device_id, child_device_id, inst_device_id=None):
            for key in required_config_keys:
                self.assertIn(key, config)
            self.assertEqual('Org_1', config['org_name'])
            self.assertEqual(RT.PlatformDevice, config['device_type'])
            self.assertEqual({'process_type': ('ZMQPyClassDriverLauncher',)}, config['driver_config'])
            self.assertEqual({'resource_id': parent_device_id}, config['agent'])
            self.assertIn('stream_config', config)
            for key in ['alarm_defs', 'startup_config']:
                self.assertEqual({}, config[key])

            self.assertIn(child_device_id, config['children'])
            verify_child_config(config['children'][child_device_id], child_device_id, inst_device_id)


        parsed_rpdict_id = self.DSC.read_parameter_dictionary_by_name(
            'platform_eng_parsed',
            id_only=True)
        self.parsed_stream_def_id = self.PSC.create_stream_definition(
            name='parsed',
            parameter_dictionary_id=parsed_rpdict_id)


        rpdict_id = self.DSC.read_parameter_dictionary_by_name('ctd_raw_param_dict', id_only=True)
        raw_stream_def_id = self.PSC.create_stream_definition(name='raw', parameter_dictionary_id=rpdict_id)
        #todo: create org and figure out which agent resource needs to get assigned to it

        def _make_platform_agent_structure(agent_config=None):
            if None is agent_config: agent_config = {}

            # instance creation
            platform_agent_instance_obj = any_old(RT.PlatformAgentInstance)
            platform_agent_instance_obj.agent_config = agent_config
            platform_agent_instance_id = self.IMS.create_platform_agent_instance(platform_agent_instance_obj)

            # agent creation
            raw_config = StreamConfiguration(stream_name='parsed',
                                             parameter_dictionary_name='platform_eng_parsed',
                                             records_per_granule=2,
                                             granule_publish_rate=5)
            platform_agent_obj = any_old(RT.PlatformAgent, {"stream_configurations":[raw_config]})
            platform_agent_id = self.IMS.create_platform_agent(platform_agent_obj)

            # device creation
            platform_device_id = self.IMS.create_platform_device(any_old(RT.PlatformDevice))

            # data product creation
            dp_obj = any_old(RT.DataProduct, {"temporal_domain":tdom, "spatial_domain": sdom})
            # dp_id = self.DP.create_data_product(data_product=dp_obj, stream_definition_id=raw_stream_def_id)
            dp_id = self.DP.create_data_product(data_product=dp_obj, stream_definition_id=self.parsed_stream_def_id)
            self.DAMS.assign_data_product(input_resource_id=platform_device_id, data_product_id=dp_id)
            self.DP.activate_data_product_persistence(data_product_id=dp_id)

            # assignments
            self.RR2.assign_platform_agent_instance_to_platform_device(platform_agent_instance_id, platform_device_id)
            self.RR2.assign_platform_agent_to_platform_agent_instance(platform_agent_id, platform_agent_instance_id)
            self.RR2.assign_platform_device_to_org_with_has_resource(platform_agent_instance_id, org_id)

            return platform_agent_instance_id, platform_agent_id, platform_device_id

        def _make_instrument_agent_structure(agent_config=None):
            if None is agent_config: agent_config = {}

            # instance creation
            instrument_agent_instance_obj = any_old(RT.InstrumentAgentInstance, {"startup_config": inst_startup_config})
            instrument_agent_instance_obj.agent_config = agent_config
            instrument_agent_instance_id = self.IMS.create_instrument_agent_instance(instrument_agent_instance_obj)

            # agent creation
            raw_config = StreamConfiguration(stream_name='raw', parameter_dictionary_name='ctd_raw_param_dict', records_per_granule=2, granule_publish_rate=5 )
            instrument_agent_obj = any_old(RT.InstrumentAgent, {"stream_configurations":[raw_config]})
            instrument_agent_id = self.IMS.create_instrument_agent(instrument_agent_obj)

            # device creation
            instrument_device_id = self.IMS.create_instrument_device(any_old(RT.InstrumentDevice))

            # data product creation
            dp_obj = any_old(RT.DataProduct, {"temporal_domain":tdom, "spatial_domain": sdom})
            dp_id = self.DP.create_data_product(data_product=dp_obj, stream_definition_id=raw_stream_def_id)
            self.DAMS.assign_data_product(input_resource_id=instrument_device_id, data_product_id=dp_id)
            self.DP.activate_data_product_persistence(data_product_id=dp_id)

            # assignments
            self.RR2.assign_instrument_agent_instance_to_instrument_device(instrument_agent_instance_id, instrument_device_id)
            self.RR2.assign_instrument_agent_to_instrument_agent_instance(instrument_agent_id, instrument_agent_instance_id)
            self.RR2.assign_instrument_device_to_org_with_has_resource(instrument_agent_instance_id, org_id)

            return instrument_agent_instance_id, instrument_agent_id, instrument_device_id


        # can't do anything without an agent instance obj
        log.debug("Testing that preparing a launcher without agent instance raises an error")
        self.assertRaises(AssertionError, pconfig_builder.prepare, will_launch=False)

        log.debug("Making the structure for a platform agent, which will be the child")
        platform_agent_instance_child_id, _, platform_device_child_id  = _make_platform_agent_structure()
        platform_agent_instance_child_obj = self.RR2.read(platform_agent_instance_child_id)

        log.debug("Preparing a valid agent instance launch, for config only")
        pconfig_builder.set_agent_instance_object(platform_agent_instance_child_obj)
        child_config = pconfig_builder.prepare(will_launch=False)
        verify_child_config(child_config, platform_device_child_id)

        log.debug("Making the structure for a platform agent, which will be the parent")
        platform_agent_instance_parent_id, _, platform_device_parent_id  = _make_platform_agent_structure()
        platform_agent_instance_parent_obj = self.RR2.read(platform_agent_instance_parent_id)

        log.debug("Testing child-less parent as a child config")
        pconfig_builder.set_agent_instance_object(platform_agent_instance_parent_obj)
        parent_config = pconfig_builder.prepare(will_launch=False)
        verify_child_config(parent_config, platform_device_parent_id)

        log.debug("assigning child platform to parent")
        self.RR2.assign_platform_device_to_platform_device(platform_device_child_id, platform_device_parent_id)
        child_device_ids = self.RR2.find_platform_device_ids_of_device(platform_device_parent_id)
        self.assertNotEqual(0, len(child_device_ids))

        log.debug("Testing parent + child as parent config")
        pconfig_builder.set_agent_instance_object(platform_agent_instance_parent_obj)
        parent_config = pconfig_builder.prepare(will_launch=False)
        verify_parent_config(parent_config, platform_device_parent_id, platform_device_child_id)

        log.debug("making the structure for an instrument agent")
        instrument_agent_instance_id, _, instrument_device_id = _make_instrument_agent_structure()
        instrument_agent_instance_obj = self.RR2.read(instrument_agent_instance_id)

        log.debug("Testing instrument config")
        iconfig_builder.set_agent_instance_object(instrument_agent_instance_obj)
        instrument_config = iconfig_builder.prepare(will_launch=False)
        verify_instrument_config(instrument_config, instrument_device_id)

        log.debug("assigning instrument to platform")
        self.RR2.assign_instrument_device_to_platform_device(instrument_device_id, platform_device_child_id)
        child_device_ids = self.RR2.find_instrument_device_ids_of_device(platform_device_child_id)
        self.assertNotEqual(0, len(child_device_ids))

        log.debug("Testing entire config")
        pconfig_builder.set_agent_instance_object(platform_agent_instance_parent_obj)
        full_config = pconfig_builder.prepare(will_launch=False)
        verify_parent_config(full_config, platform_device_parent_id, platform_device_child_id, instrument_device_id)

        if log.isEnabledFor(logging.TRACE):
            import pprint
            pp = pprint.PrettyPrinter()
            pp.pprint(full_config)
            log.trace("full_config = %s", pp.pformat(full_config))

        return full_config

    def get_streamConfigs(self):
        #
        # This method is an adaptation of get_streamConfigs in
        # test_driver_egg.py
        #
        return [
            StreamConfiguration(stream_name='parsed',
                                parameter_dictionary_name='platform_eng_parsed',
                                records_per_granule=2,
                                granule_publish_rate=5)

            # TODO enable something like the following when also
            # incorporating "raw" data:
            #,
            #StreamConfiguration(stream_name='raw',
            #                    parameter_dictionary_name='ctd_raw_param_dict',
            #                    records_per_granule=2,
            #                    granule_publish_rate=5)
        ]

    @skip("Still needs alignment with new configuration structure")
    def test_hierarchy(self):
        # TODO re-implement.
        pass

    def test_single_platform(self):
        full_config = self._create_platform_configuration()

        platform_id = 'LJ01D'


        stream_configurations = self.get_streamConfigs()
        agent__obj = IonObject(RT.PlatformAgent,
                               name='%s_PlatformAgent' % platform_id,
                               description='%s_PlatformAgent platform agent' % platform_id,
                               stream_configurations=stream_configurations)


        agent_id = self.IMS.create_platform_agent(agent__obj)


        device__obj = IonObject(RT.PlatformDevice,
            name='%s_PlatformDevice' % platform_id,
            description='%s_PlatformDevice platform device' % platform_id,
            #                        ports=port_objs,
            #                        platform_monitor_attributes = monitor_attribute_objs
        )

        self.device_id = self.IMS.create_platform_device(device__obj)


        #######################################
        # data product  (adapted from test_instrument_management_service_integration)
        tdom, sdom = time_series_domain()
        tdom = tdom.dump()
        sdom = sdom.dump()
        dp_obj = IonObject(RT.DataProduct,
            name='the parsed data',
            description='DataProduct test',
            processing_level_code='Parsed_Canonical',
            temporal_domain = tdom,
            spatial_domain = sdom)
        data_product_id1 = self.DP.create_data_product(data_product=dp_obj, stream_definition_id=self.parsed_stream_def_id)
        log.debug('data_product_id1 = %s', data_product_id1)

        self.DAMS.assign_data_product(input_resource_id=self.device_id, data_product_id=data_product_id1)
        self.DP.activate_data_product_persistence(data_product_id=data_product_id1)
        #######################################

        #######################################
        # dataset

        stream_ids, _ = self.RR.find_objects(data_product_id1, PRED.hasStream, None, True)
        log.debug( 'Data product streams1 = %s', stream_ids)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        dataset_ids, _ = self.RR.find_objects(data_product_id1, PRED.hasDataset, RT.Dataset, True)
        log.debug('Data set for data_product_id1 = %s', dataset_ids[0])
        self.parsed_dataset = dataset_ids[0]
        #######################################



        full_config['platform_config'] = {
            'platform_id':             platform_id,

            'driver_config':           DVR_CONFIG,

            'network_definition' :     self._network_definition_ser
        }

        agent_instance_obj = IonObject(RT.PlatformAgentInstance,
            name='%s_PlatformAgentInstance' % platform_id,
            description="%s_PlatformAgentInstance" % platform_id,
            agent_config=full_config)


        agent_instance_id = self.IMS.create_platform_agent_instance(
            platform_agent_instance = agent_instance_obj,
            platform_agent_id = agent_id,
            platform_device_id = self.device_id)


        stream_id = stream_ids[0]
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
        # extended_platform = self.IMS.get_platform_device_extension(self.device_id)
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

        #-------------------------------
        # Stop Base Platform AgentInstance
        #-------------------------------
        self.IMS.stop_platform_agent_instance(platform_agent_instance_id=agent_instance_id)
