#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.ion.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.iobservatory_management_service import  ObservatoryManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

from pyon.ion.stream import StandaloneStreamSubscriber

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict, Inconsistent
from pyon.public import RT, PRED
#from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
from pyon.event.event import EventSubscriber
from pyon.public import OT

import unittest
from ooi.logging import log

import yaml

from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand, ProcessStateEnum

from ion.agents.platform.platform_agent import PlatformAgentState
from ion.agents.platform.platform_agent import PlatformAgentEvent

from ion.services.dm.utility.granule_utils import CoverageCraft
from ion.util.parameter_yaml_IO import get_param_dict

from coverage_model.parameter import ParameterDictionary

from gevent.event import AsyncResult
from gevent import sleep


from ion.services.cei.process_dispatcher_service import ProcessStateGate


# TIMEOUT: timeout for each execute_agent call.
# NOTE: the bigger the platform network size starting from the chosen
# PLATFORM_ID above, the more the time that should be given for commands to
# complete, in particular, for those with a cascading effect on all the
# descendents, eg, INITIALIZE.
# The following TIMEOUT value intends to be big enough for all typical cases.
TIMEOUT = 90


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


#@unittest.skip("Under reconstruction")
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

        self._no_samples = None
        self._async_data_result = AsyncResult()
        self._data_subscribers = []
        self._samples_received = []

        self.addCleanup(self._stop_data_subscribers)

    def consume_data(self, message, stream_route, stream_id):
        # A callback for processing subscribed-to data.
        log.info('Subscriber received data message: %s.', str(message))
        self._samples_received.append(message)
        if self._no_samples and self._no_samples == len(self._samples_received):
            self._async_data_result.set()

    def _start_data_subscriber(self, stream_name, stream_config):
        """
        Starts data subscriber for the given stream_name and stream_config
        """
        log.info('_start_data_subscriber stream_name=%r', stream_name)

        stream_id = stream_config['stream_id']

        # Create subscription for the stream
        exchange_name = '%s_queue' % stream_name
        self.container.ex_manager.create_xn_queue(exchange_name).purge()
        sub = StandaloneStreamSubscriber(exchange_name, self.consume_data)
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

    def _build_stream_config(self, stream_id=''):

        raw_parameter_dictionary = get_param_dict('simple_data_particle_raw_param_dict')

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
                         'parameter_dictionary':raw_parameter_dictionary.dump()}

        return stream_config


    def test_oms_create_and_launch(self):

        # Create PlatformModel
        platformModel_obj = IonObject(RT.PlatformModel, name='RSNPlatformModel', description="RSNPlatformModel", model="RSNPlatformModel" )
        try:
            platformModel_id = self.imsclient.create_platform_model(platformModel_obj)
        except BadRequest as ex:
            self.fail("failed to create new PLatformModel: %s" %ex)
        log.debug( 'new PlatformModel id = %s', platformModel_id)


        # Create data product object to be used for each of the platform log streams
        sdom, tdom = CoverageCraft.create_domains()
        sdom = sdom.dump()
        tdom = tdom.dump()

        raw_parameter_dictionary = get_param_dict('simple_data_particle_raw_param_dict')
        raw_stream_def_id = self.pubsubcli.create_stream_definition(name='raw', parameter_dictionary=raw_parameter_dictionary.dump())
        dp_obj = IonObject(RT.DataProduct,
            name='raw data',
            description='raw stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        #PlatformMonitorAttributes:
        #id: ""
        #name: ""
        ##monitor rate in seconds
        #monitor_rate: 0
        #units: ""


        # NOTE: the following dicts (which are passed to the main platform
        # agent via corresponding PLATFORM_CONFIG, see below), are indexed with
        # the IDs used in network.yml.
        agent_device_map = {}
        children_map = {}
        agent_streamconfig_map = {}

        #-------------------------------
        # Platform SS  (Shore Station)
        #-------------------------------

        platformSS_site__obj = IonObject(RT.PlatformSite,
            name='PlatformSSSite',
            description='PlatformSSSite platform site')
        platformSS_site_id = self.omsclient.create_platform_site(platformSS_site__obj)


        ports = []
        #create the port information for this device
        ports.append(  IonObject(OT.PlatformPort, port_id='ShoreStation_port_1', ip_address='ShoreStation_port_1_IP')  )
        ports.append(  IonObject(OT.PlatformPort, port_id='ShoreStation_port_2', ip_address='ShoreStation_port_2_IP')  )
        monitor_attributes = []
        #create the attributes that are specific to this model type
        monitor_attributes.append(  IonObject(OT.PlatformMonitorAttributes, id='ShoreStation_attr_1', monitor_rate=5, units='xyz')  )
        monitor_attributes.append(  IonObject(OT.PlatformMonitorAttributes, id='ShoreStation_attr_2', monitor_rate=5, units='xyz')  )

        platformSS_device__obj = IonObject(RT.PlatformDevice,
            name='ShoreStation',
            description='PlatformSSDevice platform device',
            ports = ports,
            platform_monitor_attributes = monitor_attributes)
        platformSS_device_id = self.imsclient.create_platform_device(platformSS_device__obj)
        self.imsclient.assign_platform_model_to_platform_device(platformModel_id, platformSS_device_id)
        self.rrclient.create_association(subject=platformSS_site_id, predicate=PRED.hasDevice, object=platformSS_device_id)
        self.damsclient.register_instrument(instrument_id=platformSS_device_id)

        platformSS_agent__obj = IonObject(RT.PlatformAgent,
            name='PlatformSSAgent',
            description='PlatformSSAgent platform agent')
        platformSS_agent_id = self.imsclient.create_platform_agent(platformSS_agent__obj)
        self.imsclient.assign_platform_model_to_platform_agent(platformModel_id, platformSS_agent_id)

        #create the log data product
        dp_obj.name = 'SS raw data'
        data_product_SS_id = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id=raw_stream_def_id, parameter_dictionary=raw_parameter_dictionary.dump())
        self.damsclient.assign_data_product(input_resource_id=platformSS_device_id, data_product_id=data_product_SS_id)
        self.dpclient.activate_data_product_persistence(data_product_id=data_product_SS_id)
        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_SS_id, PRED.hasStream, None, True)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        dataset_ids, _ = self.rrclient.find_objects(data_product_SS_id, PRED.hasDataset, RT.Dataset, True)


        platformSS_agent_instance_obj = IonObject(RT.PlatformAgentInstance, name='SSPlatformAgentInstance', description="SSPlatformAgentInstance",
            driver_module='ion.agents.platform.platform_agent', driver_class='PlatformAgent'   )
        platformSS_agent_instance_id = self.imsclient.create_platform_agent_instance(platformSS_agent_instance_obj, platformSS_agent_id, platformSS_device_id)

#        agent_device_map[platformSS_agent_instance_id] = platformSS_device__obj
        agent_device_map['ShoreStation'] = platformSS_device__obj
        stream_config = self._build_stream_config(stream_ids[0])
#        agent_streamconfig_map[platformSS_agent_instance_id] = stream_config
        agent_streamconfig_map['ShoreStation'] = stream_config
        self._start_data_subscriber(platformSS_agent_instance_id, stream_config)


        #-------------------------------
        # Platform Node1A
        #-------------------------------
        platform1A_site__obj = IonObject(RT.PlatformSite,
            name='Platform1ASite',
            description='Platform1ASite platform site')
        platform1A_site_id = self.omsclient.create_platform_site(platform1A_site__obj)
        self.rrclient.create_association(subject=platformSS_site_id, predicate=PRED.hasSite, object=platform1A_site_id)

        ports = []
        #create the port information for this device
        ports.append(  IonObject(OT.PlatformPort, port_id='Node1A_port_1', ip_address='Node1A_port_1_IP')  )
        ports.append(  IonObject(OT.PlatformPort, port_id='Node1A_port_2', ip_address='Node1A_port_2_IP')  )
        monitor_attributes = []
        #create the attributes that are specific to this model type
        monitor_attributes.append(  IonObject(OT.PlatformMonitorAttributes, id='Node1A_attr_1', monitor_rate=5, units='xyz')  )
        monitor_attributes.append(  IonObject(OT.PlatformMonitorAttributes, id='Node1A_attr_2', monitor_rate=5, units='xyz')  )

        platform1A_device__obj = IonObject(RT.PlatformDevice,
            name='Node1A',
            description='Platform1ADevice platform device',
            ports = ports,
            platform_monitor_attributes = monitor_attributes)
        platform1A_device_id = self.imsclient.create_platform_device(platform1A_device__obj)
        self.imsclient.assign_platform_model_to_platform_device(platformModel_id, platform1A_device_id)
        self.rrclient.create_association(subject=platform1A_site_id, predicate=PRED.hasDevice, object=platform1A_device_id)
        self.damsclient.register_instrument(instrument_id=platform1A_device_id)

        platform1A_agent__obj = IonObject(RT.PlatformAgent,
            name='Platform1AAgent',
            description='Platform1AAgent platform agent')
        platform1A_agent_id = self.imsclient.create_platform_agent(platform1A_agent__obj)
        self.imsclient.assign_platform_model_to_platform_agent(platformModel_id, platform1A_agent_id)

        #create the log data product
        dp_obj.name = '1A raw data'
        data_product_1A_id = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id=raw_stream_def_id, parameter_dictionary=raw_parameter_dictionary.dump())
        self.damsclient.assign_data_product(input_resource_id=platform1A_device_id, data_product_id=data_product_1A_id)
        self.dpclient.activate_data_product_persistence(data_product_id=data_product_1A_id)
        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_1A_id, PRED.hasStream, None, True)

        platform1A_agent_instance_obj = IonObject(RT.PlatformAgentInstance, name='Platform1AAgentInstance', description="Platform1AAgentInstance",
            driver_module='ion.agents.platform.platform_agent', driver_class='PlatformAgent'   )
        platform1A_agent_instance_id = self.imsclient.create_platform_agent_instance(platform1A_agent_instance_obj, platform1A_agent_id, platform1A_device_id)

#        agent_device_map[platform1A_agent_instance_id] = platform1A_device__obj
        agent_device_map['Node1A'] = platform1A_device__obj
#        children_map[platformSS_agent_instance_id] = [platform1A_agent_instance_id]
        children_map['ShoreStation'] = ['Node1A']
        stream_config = self._build_stream_config(stream_ids[0])
#        agent_streamconfig_map[platform1A_agent_instance_id] = stream_config
        agent_streamconfig_map['Node1A'] = stream_config
        self._start_data_subscriber(platform1A_agent_instance_id, stream_config)


        #-------------------------------
        # Platform Node1B
        #-------------------------------
        platform1B_site__obj = IonObject(RT.PlatformSite,
            name='Platform1BSite',
            description='Platform1BSite platform site')
        platform1B_site_id = self.omsclient.create_platform_site(platform1B_site__obj)
        self.rrclient.create_association(subject=platformSS_site_id, predicate=PRED.hasSite, object=platform1B_site_id)

        ports = []
        #create the port information for this device
        ports.append(  IonObject(OT.PlatformPort, port_id='Node1B_port_1', ip_address='Node1B_port_1_IP')  )
        ports.append(  IonObject(OT.PlatformPort, port_id='Node1B_port_2', ip_address='Node1B_port_2_IP')  )
        monitor_attributes = []
        #create the attributes that are specific to this model type
        monitor_attributes.append(  IonObject(OT.PlatformMonitorAttributes, id='Node1B_attr_1', monitor_rate=5, units='xyz')  )
        monitor_attributes.append(  IonObject(OT.PlatformMonitorAttributes, id='Node1B_attr_2', monitor_rate=5, units='xyz')  )

        platform1B_device__obj = IonObject(RT.PlatformDevice,
            name='Node1B',
            description='Platform1BDevice platform device',
            ports = ports,
            platform_monitor_attributes = monitor_attributes)
        platform1B_device_id = self.imsclient.create_platform_device(platform1B_device__obj)
        self.imsclient.assign_platform_model_to_platform_device(platformModel_id, platform1B_device_id)
        self.rrclient.create_association(subject=platform1B_site_id, predicate=PRED.hasDevice, object=platform1B_device_id)
        self.damsclient.register_instrument(instrument_id=platform1B_device_id)

        platform1B_agent__obj = IonObject(RT.PlatformAgent,
            name='Platform1BAgent',
            description='Platform1BAgent platform agent')
        platform1B_agent_id = self.imsclient.create_platform_agent(platform1B_agent__obj)
        self.imsclient.assign_platform_model_to_platform_agent(platformModel_id, platform1B_agent_id)

        #create the log data product
        dp_obj.name = '1B raw data'
        data_product_1B_id = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id=raw_stream_def_id, parameter_dictionary=raw_parameter_dictionary.dump())
        self.damsclient.assign_data_product(input_resource_id=platform1B_device_id, data_product_id=data_product_1B_id)
        self.dpclient.activate_data_product_persistence(data_product_id=data_product_1B_id)
        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_1B_id, PRED.hasStream, None, True)

        platform1B_agent_instance_obj = IonObject(RT.PlatformAgentInstance, name='Platform1BAgentInstance', description="Platform1BAgentInstance",
            driver_module='ion.agents.platform.platform_agent', driver_class='PlatformAgent'   )
        platform1B_agent_instance_id = self.imsclient.create_platform_agent_instance(platform1B_agent_instance_obj, platform1B_agent_id, platform1B_device_id)

#        agent_device_map[platform1B_agent_instance_id] = platform1B_device__obj
        agent_device_map['Node1B'] = platform1B_device__obj
#        children_map[platform1A_agent_instance_id] = [platform1B_agent_instance_id]
        children_map['Node1A'] = ['Node1B']
        stream_config = self._build_stream_config(stream_ids[0])
#        agent_streamconfig_map[platform1B_agent_instance_id] = stream_config
        agent_streamconfig_map['Node1B'] = stream_config
        self._start_data_subscriber(platform1B_agent_instance_id, stream_config)

        #-------------------------------
        # Platform Node1C
        #-------------------------------
        platform1C_site__obj = IonObject(RT.PlatformSite,
            name='Platform1CSite',
            description='Platform1CSite platform site')
        platform1C_site_id = self.omsclient.create_platform_site(platform1C_site__obj)
        self.rrclient.create_association(subject=platformSS_site_id, predicate=PRED.hasSite, object=platform1C_site_id)

        ports = []
        #create the port information for this device
        ports.append(  IonObject(OT.PlatformPort, port_id='Node1C_port_1', ip_address='Node1C_port_1_IP')  )
        ports.append(  IonObject(OT.PlatformPort, port_id='Node1C_port_2', ip_address='Node1C_port_2_IP')  )
        monitor_attributes = []
        #create the attributes that are specific to this model type
        monitor_attributes.append(  IonObject(OT.PlatformMonitorAttributes, id='Node1C_attr_1', monitor_rate=5, units='xyz')  )
        monitor_attributes.append(  IonObject(OT.PlatformMonitorAttributes, id='Node1C_attr_2', monitor_rate=5, units='xyz')  )


        platform1C_device__obj = IonObject(RT.PlatformDevice,
            name='Node1C',
            description='Platform1CDevice platform device',
            ports = ports,
            platform_monitor_attributes = monitor_attributes)

        platform1C_device_id = self.imsclient.create_platform_device(platform1C_device__obj)
        self.imsclient.assign_platform_model_to_platform_device(platformModel_id, platform1C_device_id)
        self.rrclient.create_association(subject=platform1C_site_id, predicate=PRED.hasDevice, object=platform1C_device_id)
        self.damsclient.register_instrument(instrument_id=platform1C_device_id)

        platform1C_agent__obj = IonObject(RT.PlatformAgent,
            name='Platform1CAgent',
            description='Platform1CAgent platform agent')
        platform1C_agent_id = self.imsclient.create_platform_agent(platform1C_agent__obj)
        self.imsclient.assign_platform_model_to_platform_agent(platformModel_id, platform1C_agent_id)

        #create the log data product
        dp_obj.name = '1C raw data'
        data_product_1C_id = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id=raw_stream_def_id, parameter_dictionary=raw_parameter_dictionary.dump())
        self.damsclient.assign_data_product(input_resource_id=platform1C_device_id, data_product_id=data_product_1C_id)
        self.dpclient.activate_data_product_persistence(data_product_id=data_product_1C_id)
        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_1C_id, PRED.hasStream, None, True)

        platform1C_agent_instance_obj = IonObject(RT.PlatformAgentInstance, name='Platform1CAgentInstance', description="Platform1CAgentInstance",
            driver_module='ion.agents.platform.platform_agent', driver_class='PlatformAgent'   )
        platform1C_agent_instance_id = self.imsclient.create_platform_agent_instance(platform1C_agent_instance_obj, platform1C_agent_id, platform1C_device_id)

#        agent_device_map[platform1C_agent_instance_id] = platform1C_device__obj
        agent_device_map['Node1C'] = platform1C_device__obj
        children_map[platform1B_agent_instance_id] = [platform1C_agent_instance_id]
        children_map['Node1B'] = ['Node1C']
        stream_config = self._build_stream_config(stream_ids[0])
#        agent_streamconfig_map[platform1C_agent_instance_id] = stream_config
        agent_streamconfig_map['Node1C'] = stream_config
        self._start_data_subscriber(platform1C_agent_instance_id, stream_config)



        #-------------------------------
        # Launch Platform SS AgentInstance, connect to the resource agent client
        #-------------------------------

        pid = self.imsclient.start_platform_agent_instance(platform_agent_instance_id=platformSS_agent_instance_id)
        log.debug("start_platform_agent_instance returned pid=%s", pid)

        #wait for start
        instance_obj = self.imsclient.read_platform_agent_instance(platformSS_agent_instance_id)
        gate = ProcessStateGate(self.processdispatchclient.read_process,
            instance_obj.agent_process_id,
            ProcessStateEnum.RUNNING)
        self.assertTrue(gate.await(30), "The platform agent instance did not spawn in 30 seconds")

        platformSS_agent_instance_obj= self.imsclient.read_instrument_agent_instance(platformSS_agent_instance_id)
        log.debug('test_oms_create_and_launch: Platform agent instance obj: %s', str(platformSS_agent_instance_obj))

        # Start a resource agent client to talk with the instrument agent.
        self._pa_client = ResourceAgentClient('paclient', name=platformSS_agent_instance_obj.agent_process_id,  process=FakeProcess())
        log.debug(" test_oms_create_and_launch:: got pa client %s", str(self._pa_client))

        DVR_CONFIG = {
            'dvr_mod': 'ion.agents.platform.oms.oms_platform_driver',
            'dvr_cls': 'OmsPlatformDriver',
            'oms_uri': 'embsimulator'
        }

        PLATFORM_CONFIG = {
            'platform_id':             'ShoreStation',
            'platform_topology' :      children_map,
            'agent_device_map':        agent_device_map,
            'agent_streamconfig_map':  agent_streamconfig_map,

            'driver_config': DVR_CONFIG,
            'container_name': self.container.name,

            }

        log.debug("Root PLATFORM_CONFIG =\n%s",
            yaml.dump(PLATFORM_CONFIG, default_flow_style=False))

        # ping_agent can be issued before INITIALIZE
        retval = self._pa_client.ping_agent(timeout=TIMEOUT)
        log.debug( 'ShoreSide Platform ping_agent = %s', str(retval) )

        # INITIALIZE should trigger the creation of the whole platform
        # hierarchy rooted at PLATFORM_CONFIG['platform_id']
        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE, kwargs=dict(plat_config=PLATFORM_CONFIG))
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug( 'ShoreSide Platform INITIALIZE = %s', str(retval) )


        # GO_ACTIVE
        cmd = AgentCommand(command=PlatformAgentEvent.GO_ACTIVE)
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug( 'ShoreSide Platform GO_ACTIVE = %s', str(retval) )

        # RUN
        cmd = AgentCommand(command=PlatformAgentEvent.RUN)
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug( 'ShoreSide Platform RUN = %s', str(retval) )

        log.info("sleeping to perhaps see some data publications...")
        sleep(15)



        #-------------------------------
        # Stop Platform SS AgentInstance,
        #-------------------------------
        self.imsclient.stop_platform_agent_instance(platform_agent_instance_id=platformSS_agent_instance_id)
