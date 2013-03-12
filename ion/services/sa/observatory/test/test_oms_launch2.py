#!/usr/bin/env python

###############################################################################
# NOTE: Deprecated -- test_platform_launch.py is the new test file.
#
# TODO: To be removed once the new file fully re-enables the needed lauch tests
###############################################################################

"""
@package ion.services.sa.observatory.test.test_oms_launch2.py
@file    ion/services/sa/observatory/test/test_oms_launch2.py
@author  Maurice Manning, Carlos Rueda
@brief   Test cases for launching platform agent network
         using information from the RSN OMS interface.
"""

__author__ = 'Maurice Manning, Carlos Rueda'
__license__ = 'Apache 2.0'

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
from pyon.core.exception import BadRequest
from pyon.public import RT, PRED

from nose.plugins.attrib import attr
from pyon.public import OT


from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand, ProcessStateEnum

from ion.agents.platform.platform_agent import PlatformAgentEvent

from ion.services.dm.utility.granule_utils import time_series_domain
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService

from gevent.event import AsyncResult
from gevent import sleep

from ion.agents.platform.rsn.oms_client_factory import CIOMSClientFactory
from ion.agents.platform.rsn.oms_util import RsnOmsUtil
from ion.agents.platform.util.network_util import NetworkUtil

from ion.services.cei.process_dispatcher_service import ProcessStateGate
from unittest import skip
import os

# The ID of the base platform for this test .
# These Ids and names should correspond to corresponding entries in network.yml,
# which is used by the RSN OMS simulator.
# NOTE, previously using 'Node1A' (which has 19 descendents) but now using
# 'Node1D' which only has 2 descendents (1 direct child and 1 grand-child) so
# the test does not take too long.
BASE_PLATFORM_ID = 'Node1D'

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


@skip("Obsolete file to be removed once the launch tests are reenabled elsewhere")
@attr('INT', group='sa')
class TestOmsLaunch(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.omsclient = ObservatoryManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.dpclient = DataProductManagementServiceClient(node=self.container.node)
        self.pubsubcli = PubsubManagementServiceClient(node=self.container.node)
        self.processdispatchclient = ProcessDispatcherServiceClient(node=self.container.node)
        self.dataprocessclient = DataProcessManagementServiceClient(node=self.container.node)
        self.dataset_management = DatasetManagementServiceClient()


        # Use the network definition provided by RSN OMS directly.
        rsn_oms = CIOMSClientFactory.create_instance(DVR_CONFIG['oms_uri'])
        self._network_definition = RsnOmsUtil.build_network_definition(rsn_oms)
        # get serialized version for the configuration:
        self._network_definition_ser = NetworkUtil.serialize_network_definition(self._network_definition)
        if log.isEnabledFor(logging.DEBUG):
            log.debug("NetworkDefinition serialization:\n%s", self._network_definition_ser)



        self.platformModel_id = None

        self.all_platforms = {}
        self.agent_streamconfig_map = {}

        self._async_data_result = AsyncResult()
        self._data_subscribers = []
        self._samples_received = []
        self.addCleanup(self._stop_data_subscribers)

        self._async_event_result = AsyncResult()
        self._event_subscribers = []
        self._events_received = []
        self.addCleanup(self._stop_event_subscribers)
        self._start_event_subscriber()

        self._set_up_DataProduct_obj()
        self._set_up_PlatformModel_obj()

    def _set_up_DataProduct_obj(self):
        # Create data product object to be used for each of the platform log streams
        tdom, sdom = time_series_domain()
        sdom = sdom.dump()
        tdom = tdom.dump()

        self.pdict_id = self.dataset_management.read_parameter_dictionary_by_name('platform_eng_parsed', id_only=True)
        self.platform_eng_stream_def_id = self.pubsubcli.create_stream_definition(
            name='platform_eng', parameter_dictionary_id=self.pdict_id)
        self.dp_obj = IonObject(RT.DataProduct,
            name='platform_eng data',
            description='platform_eng test',
            temporal_domain = tdom,
            spatial_domain = sdom)

    def _set_up_PlatformModel_obj(self):
        # Create PlatformModel
        platformModel_obj = IonObject(RT.PlatformModel,
            name='RSNPlatformModel',
            description="RSNPlatformModel")
        try:
            self.platformModel_id = self.imsclient.create_platform_model(platformModel_obj)
        except BadRequest as ex:
            self.fail("failed to create new PLatformModel: %s" %ex)
        log.debug( 'new PlatformModel id = %s', self.platformModel_id)

    def _traverse(self, pnode, platform_id, parent_platform_objs=None):
        """
        Recursive routine that repeatedly calls _prepare_platform to build
        the object dictionary for each platform.

        @param pnode PlatformNode
        @param platform_id ID of the platform to be visited
        @param parent_platform_objs dict of objects associated to parent
                        platform, if any.

        @retval the dict returned by _prepare_platform at this level.
        """

        log.info("Starting _traverse for %r", platform_id)

        plat_objs = self._prepare_platform(pnode, platform_id, parent_platform_objs)

        self.all_platforms[platform_id] = plat_objs

        # now, traverse the children:
        for sub_pnode in pnode.subplatforms.itervalues():
            subplatform_id = sub_pnode.platform_id
            self._traverse(sub_pnode, subplatform_id, plat_objs)

        return plat_objs

    def _prepare_platform(self, pnode, platform_id, parent_platform_objs):
        """
        This routine generalizes the manual construction originally done in
        test_oms_launch.py. It is called by the recursive _traverse method so
        all platforms starting from a given base platform are prepared.

        Note: For simplicity in this test, sites are organized in the same
        hierarchical way as the platforms themselves.

        @param pnode PlatformNode
        @param platform_id ID of the platform to be visited
        @param parent_platform_objs dict of objects associated to parent
                        platform, if any.

        @retval a dict of associated objects similar to those in
                test_oms_launch
        """

        site__obj = IonObject(RT.PlatformSite,
            name='%s_PlatformSite' % platform_id,
            description='%s_PlatformSite platform site' % platform_id)

        site_id = self.omsclient.create_platform_site(site__obj)

        if parent_platform_objs:
            # establish hasSite association with the parent
            self.rrclient.create_association(
                subject=parent_platform_objs['site_id'],
                predicate=PRED.hasSite,
                object=site_id)

        # prepare platform attributes and ports:
        monitor_attribute_objs, monitor_attribute_dicts = self._prepare_platform_attributes(pnode, platform_id)

        port_objs, port_dicts = self._prepare_platform_ports(pnode, platform_id)

        device__obj = IonObject(RT.PlatformDevice,
            name='%s_PlatformDevice' % platform_id,
            description='%s_PlatformDevice platform device' % platform_id,
            #                        ports=port_objs,
            #                        platform_monitor_attributes = monitor_attribute_objs
        )

        device__dict = dict(ports=port_dicts,
            platform_monitor_attributes=monitor_attribute_dicts)

        self.device_id = self.imsclient.create_platform_device(device__obj)

        self.imsclient.assign_platform_model_to_platform_device(self.platformModel_id, self.device_id)
        self.rrclient.create_association(subject=site_id, predicate=PRED.hasDevice, object=self.device_id)
        self.damsclient.register_instrument(instrument_id=self.device_id)


        if parent_platform_objs:
            # establish hasDevice association with the parent
            self.rrclient.create_association(
                subject=parent_platform_objs['device_id'],
                predicate=PRED.hasDevice,
                object=self.device_id)

        agent__obj = IonObject(RT.PlatformAgent,
            name='%s_PlatformAgent' % platform_id,
            description='%s_PlatformAgent platform agent' % platform_id)

        agent_id = self.imsclient.create_platform_agent(agent__obj)

        if parent_platform_objs:
            # add this platform_id to parent's children:
            parent_platform_objs['children'].append(platform_id)


        self.imsclient.assign_platform_model_to_platform_agent(self.platformModel_id, agent_id)

        #        agent_instance_obj = IonObject(RT.PlatformAgentInstance,
        #                                name='%s_PlatformAgentInstance' % platform_id,
        #                                description="%s_PlatformAgentInstance" % platform_id)
        #
        #        agent_instance_id = self.imsclient.create_platform_agent_instance(
        #                            agent_instance_obj, agent_id, device_id)

        plat_objs = {
            'platform_id':        platform_id,
            'site__obj':          site__obj,
            'site_id':            site_id,
            'device__obj':        device__obj,
            'device_id':          self.device_id,
            'agent__obj':         agent__obj,
            'agent_id':           agent_id,
            #            'agent_instance_obj': agent_instance_obj,
            #            'agent_instance_id':  agent_instance_id,
            'children':           []
        }

        log.info("plat_objs for platform_id %r = %s", platform_id, str(plat_objs))

        stream_config = self._create_stream_config(plat_objs)
        self.agent_streamconfig_map[platform_id] = stream_config
        #        self.agent_streamconfig_map[platform_id] = None
        #        self._start_data_subscriber(agent_instance_id, stream_config)

        return plat_objs

    def _prepare_platform_attributes(self, pnode, platform_id):
        """
        Returns the list of PlatformMonitorAttributes objects corresponding to
        the attributes associated to the given platform.
        """
        # TODO complete the clean-up of this method
        ret_infos = dict((n, a.defn) for (n, a) in pnode.attrs.iteritems())

        monitor_attribute_objs = []
        monitor_attribute_dicts = []
        for attrName, attrDfn in ret_infos.iteritems():
            log.debug("platform_id=%r: preparing attribute=%r", platform_id, attrName)

            monitor_rate = attrDfn['monitorCycleSeconds']
            units =        attrDfn['units']

            plat_attr_obj = IonObject(OT.PlatformMonitorAttributes,
                id=attrName,
                monitor_rate=monitor_rate,
                units=units)

            plat_attr_dict = dict(id=attrName,
                monitor_rate=monitor_rate,
                units=units)

            monitor_attribute_objs.append(plat_attr_obj)
            monitor_attribute_dicts.append(plat_attr_dict)

        return monitor_attribute_objs, monitor_attribute_dicts

    def _prepare_platform_ports(self, pnode, platform_id):
        """
        Returns the list of PlatformPort objects corresponding to the ports
        associated to the given platform.
        """
        # TODO complete the clean-up of this method

        port_objs = []
        port_dicts = []
        for port_id, network in pnode.ports.iteritems():
            log.debug("platform_id=%r: preparing port=%r network=%s",
                      platform_id, port_id, network)

            #
            # Note: the name "IP" address has been changed to "network" address
            # in the CI-OMS interface spec.
            #
            plat_port_obj = IonObject(OT.PlatformPort,
                                      port_id=port_id,
                                      ip_address=network)

            plat_port_dict = dict(port_id=port_id,
                                  network=network)

            port_objs.append(plat_port_obj)

            port_dicts.append(plat_port_dict)

        return port_objs, port_dicts

    def _create_stream_config(self, plat_objs):

        platform_id = plat_objs['platform_id']
        device_id =   plat_objs['device_id']


        #create the log data product
        self.dp_obj.name = '%s platform_eng data' % platform_id
        self.data_product_id = self.dpclient.create_data_product(data_product=self.dp_obj, stream_definition_id=self.platform_eng_stream_def_id)
        self.damsclient.assign_data_product(input_resource_id=self.device_id, data_product_id=self.data_product_id)
        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(self.data_product_id, PRED.hasStream, None, True)

        stream_config = self._build_stream_config(stream_ids[0])
        return stream_config

    def _build_stream_config(self, stream_id=''):

        platform_eng_dictionary = DatasetManagementService.get_parameter_dictionary_by_name('platform_eng_parsed')

        #get the streamroute object from pubsub by passing the stream_id
        stream_def_ids, _ = self.rrclient.find_objects(stream_id,
            PRED.hasStreamDefinition,
            RT.StreamDefinition,
            True)


        stream_route = self.pubsubcli.read_stream_route(stream_id=stream_id)
        stream_config = {'routing_key' : stream_route.routing_key,
                         'stream_id' : stream_id,
                         'stream_definition_ref' : stream_def_ids[0],
                         'exchange_point' : stream_route.exchange_point,
                         'parameter_dictionary':platform_eng_dictionary.dump()}

        return stream_config

    def _set_platform_agent_instances(self):
        """
        Once most of the objs/defs associated with all platforms are in
        place, this method creates and associates the PlatformAgentInstance
        elements.
        """

        self.platform_configs = {}
        for platform_id, plat_objs in self.all_platforms.iteritems():

            PLATFORM_CONFIG  = {
                'platform_id':             platform_id,

                'agent_streamconfig_map':  None, #self.agent_streamconfig_map,

                'driver_config':           DVR_CONFIG,

                'network_definition' :     self._network_definition_ser
                }

            self.platform_configs[platform_id] = {
                'platform_id':             platform_id,

                'agent_streamconfig_map':  self.agent_streamconfig_map,

                'driver_config':           DVR_CONFIG,

                'network_definition' :     self._network_definition_ser
                }

            agent_config = {
                'platform_config': PLATFORM_CONFIG,
                }

            self.stream_id = self.agent_streamconfig_map[platform_id]['stream_id']

            #            import pprint
            #            print '============== platform id within unit test: %s ===========' % platform_id
            #            pprint.pprint(agent_config)
            #agent_config['platform_config']['agent_streamconfig_map'] = None

            agent_instance_obj = IonObject(RT.PlatformAgentInstance,
                name='%s_PlatformAgentInstance' % platform_id,
                description="%s_PlatformAgentInstance" % platform_id,
                agent_config=agent_config)

            agent_id = plat_objs['agent_id']
            device_id = plat_objs['device_id']
            agent_instance_id = self.imsclient.create_platform_agent_instance(
                agent_instance_obj, agent_id, self.device_id)

            plat_objs['agent_instance_obj'] = agent_instance_obj
            plat_objs['agent_instance_id']  = agent_instance_id


            stream_config = self.agent_streamconfig_map[platform_id]
            self._start_data_subscriber(agent_instance_id, stream_config)


    def _start_data_subscriber(self, stream_name, stream_config):
        """
        Starts data subscriber for the given stream_name and stream_config
        """

        def consume_data(message, stream_route, stream_id):
            # A callback for processing subscribed-to data.
            log.info('Subscriber received data message: %s.', str(message))
            self._samples_received.append(message)
            self._async_data_result.set()

        log.info('_start_data_subscriber stream_name=%r', stream_name)

        stream_id = self.stream_id #stream_config['stream_id']

        # Create subscription for the stream
        exchange_name = '%s_queue' % stream_name
        self.container.ex_manager.create_xn_queue(exchange_name).purge()
        sub = StandaloneStreamSubscriber(exchange_name, consume_data)
        sub.start()
        self._data_subscribers.append(sub)
        sub_id = self.pubsubcli.create_subscription(name=exchange_name, stream_ids=[stream_id])
        self.pubsubcli.activate_subscription(sub_id)
        sub.subscription_id = sub_id

    def _stop_data_subscribers(self):
        """
        Stop the data subscribers on cleanup.
        """
        try:
            for sub in self._data_subscribers:
                if hasattr(sub, 'subscription_id'):
                    try:
                        self.pubsubcli.deactivate_subscription(sub.subscription_id)
                    except:
                        pass
                    self.pubsubcli.delete_subscription(sub.subscription_id)
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
                        self.pubsubcli.deactivate_subscription(sub.subscription_id)
                    except:
                        pass
                    self.pubsubcli.delete_subscription(sub.subscription_id)
                sub.stop()
        finally:
            self._event_subscribers = []

    @skip("IMS does't net implement topology")
    def test_hierarchy(self):
        self._create_launch_verify(BASE_PLATFORM_ID)

    @skip("Needs alignment with recent IMS changes")
    def test_single_platform(self):
        self._create_launch_verify('LJ01D')

    def _create_launch_verify(self, base_platform_id):
        # and trigger the traversal of the branch rooted at that base platform
        # to create corresponding ION objects and configuration dictionaries:

        pnode = self._network_definition.pnodes[base_platform_id]
        base_platform_objs = self._traverse(pnode, base_platform_id)

        # now that most of the topology information is there, add the
        # PlatformAgentInstance elements
        self._set_platform_agent_instances()

        base_platform_config = self.platform_configs[base_platform_id]

        log.info("base_platform_id = %r", base_platform_id)


        #-------------------------------------------------------------------------------------
        # Create Data Process Definition and Data Process for the eng stream monitor process
        #-------------------------------------------------------------------------------------
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='DemoStreamAlertTransform',
            description='For testing EventTriggeredTransform_B',
            module='ion.processes.data.transforms.event_alert_transform',
            class_name='DemoStreamAlertTransform')
        self.platform_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)

        #THERE SHOULD BE NO STREAMDEF REQUIRED HERE.
        platform_streamdef_id = self.pubsubcli.create_stream_definition(name='platform_eng_parsed', parameter_dictionary_id=self.pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(platform_streamdef_id, self.platform_dprocdef_id, binding='output' )

        config = {
            'process':{
                'timer_interval': 5,
                'queue_name': 'a_queue',
                'variable_name': 'input_voltage',
                'time_field_name': 'preferred_timestamp',
                'valid_values': [-100, 100],
                'timer_origin': 'Interval Timer'
            }
        }


        platform_data_process_id = self.dataprocessclient.create_data_process(self.platform_dprocdef_id, [self.data_product_id], {}, config)
        self.dataprocessclient.activate_data_process(platform_data_process_id)
        self.addCleanup(self.dataprocessclient.delete_data_process, platform_data_process_id)

        #-------------------------------
        # Launch Base Platform AgentInstance, connect to the resource agent client
        #-------------------------------

        agent_instance_id = base_platform_objs['agent_instance_id']
        log.debug("about to call imsclient.start_platform_agent_instance with id=%s", agent_instance_id)
        pid = self.imsclient.start_platform_agent_instance(platform_agent_instance_id=agent_instance_id)
        log.debug("start_platform_agent_instance returned pid=%s", pid)

        #wait for start
        instance_obj = self.imsclient.read_platform_agent_instance(agent_instance_id)
        gate = ProcessStateGate(self.processdispatchclient.read_process,
            instance_obj.agent_process_id,
            ProcessStateEnum.RUNNING)
        self.assertTrue(gate.await(90), "The platform agent instance did not spawn in 90 seconds")

        agent_instance_obj= self.imsclient.read_instrument_agent_instance(agent_instance_id)
        log.debug('test_oms_create_and_launch: Platform agent instance obj: %s', str(agent_instance_obj))

        # Start a resource agent client to talk with the instrument agent.
        self._pa_client = ResourceAgentClient('paclient', name=agent_instance_obj.agent_process_id,  process=FakeProcess())
        log.debug(" test_oms_create_and_launch:: got pa client %s", str(self._pa_client))

        log.debug("base_platform_config =\n%s", base_platform_config)

        # ping_agent can be issued before INITIALIZE
        retval = self._pa_client.ping_agent(timeout=TIMEOUT)
        log.debug( 'Base Platform ping_agent = %s', str(retval) )

        # issue INITIALIZE command to the base platform, which will launch the
        # creation of the whole platform hierarchy rooted at base_platform_config['platform_id']
        #        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE, kwargs=dict(plat_config=base_platform_config))
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
        extended_platform = self.imsclient.get_platform_device_extension(self.device_id)
#        log.debug( 'test_single_platform   extended_platform: %s', str(extended_platform) )
#        log.debug( 'test_single_platform   power_status_roll_up: %s', str(extended_platform.computed.power_status_roll_up.value) )
#        log.debug( 'test_single_platform   comms_status_roll_up: %s', str(extended_platform.computed.communications_status_roll_up.value) )

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
        self.imsclient.stop_platform_agent_instance(platform_agent_instance_id=agent_instance_id)
