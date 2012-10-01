#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.ion.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.services.sa.observatory.observatory_management_service import ObservatoryManagementService
from interface.services.sa.iobservatory_management_service import IObservatoryManagementService, ObservatoryManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient

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

from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand, ProcessStateEnum

from ion.agents.platform.platform_agent import PlatformAgentState
from ion.agents.platform.platform_agent import PlatformAgentEvent



from gevent import queue


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


@unittest.skip("Under reconstruction")
@attr('INT', group='sa')
class TestOmsLaunch(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.omsclient = ObservatoryManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)

        self.processdispatchclient = ProcessDispatcherServiceClient(node=self.container.node)

    #@unittest.skip('targeting')
    def test_oms_create_and_launch(self):


        # Start data suscribers, add stop to cleanup.
        # Define stream_config.
        self._no_samples = None
        #self._async_data_result = AsyncResult()
        self._data_greenlets = []
        self._stream_config = {}
        self._samples_received = []
        self._data_subscribers = []


        # Create PlatformModel
        platformModel_obj = IonObject(RT.PlatformModel, name='RSNPlatformModel', description="RSNPlatformModel", model="RSNPlatformModel" )
        try:
            platformModel_id = self.imsclient.create_platform_model(platformModel_obj)
        except BadRequest as ex:
            self.fail("failed to create new PLatformModel: %s" %ex)
        log.debug( 'new PlatformModel id = %s' % platformModel_id)


        #PlatformMonitorAttributes:
        #id: ""
        #name: ""
        ##monitor rate in seconds
        #monitor_rate: 0
        #units: ""


        #maps an agent instance id to a device object with specifics about that device
        agent_device_map = {}
        parent_map = {}

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
        ports.append(  IonObject(OT.PlatformPort, port_id='ShoreStation_port_1', ip_address='ShoreStation_port_1_IP')  )
        monitor_attributes = []
        #create the attributes that are specific to this model type
        monitor_attributes.append(  IonObject(OT.PlatformMonitorAttributes, id='ShoreStation_attr_1', monitor_rate=5, units='xyz')  )
        monitor_attributes.append(  IonObject(OT.PlatformMonitorAttributes, id='ShoreStation_attr_2', monitor_rate=5, units='xyz')  )

        platformSS_device__obj = IonObject(RT.PlatformDevice,
                                        name='PlatformSSDevice',
                                        description='PlatformSSDevice platform device',
                                        ports = ports,
                                        platform_monitor_attributes = monitor_attributes)
        platformSS_device_id = self.imsclient.create_platform_device(platformSS_device__obj)
        self.imsclient.assign_platform_model_to_platform_device(platformModel_id, platformSS_device_id)
        self.rrclient.create_association(subject=platformSS_site_id, predicate=PRED.hasDevice, object=platformSS_device_id)

        platformSS_agent__obj = IonObject(RT.PlatformAgent,
                                        name='PlatformSSAgent',
                                        description='PlatformSSAgent platform agent')
        platformSS_agent_id = self.imsclient.create_platform_agent(platformSS_agent__obj)
        self.imsclient.assign_platform_model_to_platform_agent(platformModel_id, platformSS_agent_id)


        platformSS_agent_instance_obj = IonObject(RT.PlatformAgentInstance, name='SSPlatformAgentInstance', description="SSPlatformAgentInstance",
                                          driver_module='ion.agents.platform.platform_agent', driver_class='PlatformAgent'   )
        platformSS_agent_instance_id = self.imsclient.create_platform_agent_instance(platformSS_agent_instance_obj, platformSS_agent_id, platformSS_device_id)

        agent_device_map[platformSS_agent_instance_id] = platformSS_device__obj



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
                                        name='Platform1ADevice',
                                        description='Platform1ADevice platform device',
                                        ports = ports,
                                        platform_monitor_attributes = monitor_attributes)
        platform1A_device_id = self.imsclient.create_platform_device(platform1A_device__obj)
        self.imsclient.assign_platform_model_to_platform_device(platformModel_id, platform1A_device_id)
        self.rrclient.create_association(subject=platform1A_site_id, predicate=PRED.hasDevice, object=platform1A_device_id)

        platform1A_agent__obj = IonObject(RT.PlatformAgent,
                                        name='Platform1AAgent',
                                        description='Platform1AAgent platform agent')
        platform1A_agent_id = self.imsclient.create_platform_agent(platform1A_agent__obj)
        self.imsclient.assign_platform_model_to_platform_agent(platformModel_id, platform1A_agent_id)


        platform1A_agent_instance_obj = IonObject(RT.PlatformAgentInstance, name='Platform1AAgentInstance', description="Platform1AAgentInstance",
                                          driver_module='ion.agents.platform.platform_agent', driver_class='PlatformAgent'   )
        platform1A_agent_instance_id = self.imsclient.create_platform_agent_instance(platform1A_agent_instance_obj, platform1A_agent_id, platform1A_device_id)

        parent_map[platform1A_agent_instance_id] = [platformSS_agent_instance_id]
        agent_device_map[platform1A_agent_instance_id] = platform1A_device__obj


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
            name='Platform1BDevice',
            description='Platform1BDevice platform device',
            ports = ports,
            platform_monitor_attributes = monitor_attributes)
        platform1B_device_id = self.imsclient.create_platform_device(platform1B_device__obj)
        self.imsclient.assign_platform_model_to_platform_device(platformModel_id, platform1B_device_id)
        self.rrclient.create_association(subject=platform1B_site_id, predicate=PRED.hasDevice, object=platform1B_device_id)

        platform1B_agent__obj = IonObject(RT.PlatformAgent,
            name='Platform1BAgent',
            description='Platform1BAgent platform agent')
        platform1B_agent_id = self.imsclient.create_platform_agent(platform1B_agent__obj)
        self.imsclient.assign_platform_model_to_platform_agent(platformModel_id, platform1B_agent_id)


        platform1B_agent_instance_obj = IonObject(RT.PlatformAgentInstance, name='Platform1BAgentInstance', description="Platform1BAgentInstance",
            driver_module='ion.agents.platform.platform_agent', driver_class='PlatformAgent'   )
        platform1B_agent_instance_id = self.imsclient.create_platform_agent_instance(platform1B_agent_instance_obj, platform1B_agent_id, platform1B_device_id)

        parent_map[platform1B_agent_instance_id] = [platform1A_agent_instance_id]
        agent_device_map[platform1B_agent_instance_id] = platform1B_device__obj



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
            name='Platform1CDevice',
            description='Platform1CDevice platform device',
            ports = ports,
            platform_monitor_attributes = monitor_attributes)

        platform1C_device_id = self.imsclient.create_platform_device(platform1C_device__obj)
        self.imsclient.assign_platform_model_to_platform_device(platformModel_id, platform1C_device_id)
        self.rrclient.create_association(subject=platform1C_site_id, predicate=PRED.hasDevice, object=platform1C_device_id)

        platform1C_agent__obj = IonObject(RT.PlatformAgent,
            name='Platform1CAgent',
            description='Platform1CAgent platform agent')
        platform1C_agent_id = self.imsclient.create_platform_agent(platform1C_agent__obj)
        self.imsclient.assign_platform_model_to_platform_agent(platformModel_id, platform1C_agent_id)


        platform1C_agent_instance_obj = IonObject(RT.PlatformAgentInstance, name='Platform1CAgentInstance', description="Platform1CAgentInstance",
            driver_module='ion.agents.platform.platform_agent', driver_class='PlatformAgent'   )
        platform1C_agent_instance_id = self.imsclient.create_platform_agent_instance(platform1C_agent_instance_obj, platform1C_agent_id, platform1C_device_id)

        parent_map[platform1C_agent_instance_id] = [platform1B_agent_instance_id]
        agent_device_map[platform1C_agent_instance_id] = platform1C_device__obj



        #-------------------------------
        # Launch Platform SS AgentInstance, connect to the resource agent client
        #-------------------------------

        pid = self.imsclient.start_platform_agent_instance(platform_agent_instance_id=platformSS_agent_instance_id)
        log.debug("start_platform_agent_instance returned pid=%s", pid)

        #wait for start
        instance_obj = self.imsclient.read_platform_agent_instance(platformSS_agent_instance_id)
        gate = ProcessStateGate(self.processdispatchclient.read_process,
                                instance_obj.agent_process_id,
                                ProcessStateEnum.SPAWN)
        self.assertTrue(gate.await(30), "The platform agent instance did not spawn in 30 seconds")

        platformSS_agent_instance_obj= self.imsclient.read_instrument_agent_instance(platformSS_agent_instance_id)
        log.debug('test_oms_create_and_launch: Platform agent instance obj: %s', str(platformSS_agent_instance_obj))

        # Start a resource agent client to talk with the instrument agent.
        self._pa_client = ResourceAgentClient('paclient', name=platformSS_agent_instance_obj.agent_process_id,  process=FakeProcess())
        log.debug(" test_oms_create_and_launch:: got pa client %s" % str(self._pa_client))

        DVR_CONFIG = {
            'dvr_mod': 'ion.agents.platform.oms.oms_platform_driver',
            'dvr_cls': 'OmsPlatformDriver',
            'oms_uri': 'embsimulator'
        }

        PLATFORM_CONFIG = {
            'platform_id': platformSS_agent_id,
            'platform_topology' : parent_map,
            'driver_config': DVR_CONFIG,
            'container_name': self.container.name,
            'agent_device_map': agent_device_map

        }

        log.debug("Root PLATFORM_CONFIG = %s", PLATFORM_CONFIG)

        # PING_AGENT can be issued before INITIALIZE
        cmd = AgentCommand(command=PlatformAgentEvent.PING_AGENT)
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug( 'ShoreSide Platform PING_AGENT = %s' % str(retval) )

        # INITIALIZE should trigger the creation of the whole platform
        # hierarchy rooted at PLATFORM_CONFIG['platform_id']
        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE, kwargs=dict(plat_config=PLATFORM_CONFIG))
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug( 'ShoreSide Platform INITIALIZE = %s' % str(retval) )


        # GO_ACTIVE
        cmd = AgentCommand(command=PlatformAgentEvent.GO_ACTIVE)
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug( 'ShoreSide Platform GO_ACTIVE = %s' % str(retval) )

        # RUN
        cmd = AgentCommand(command=PlatformAgentEvent.RUN)
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug( 'ShoreSide Platform RUN = %s' % str(retval) )

        # TODO: here we could sleep for a little bit to let the resource
        # monitoring work for a while. But not done yet because the
        # definition of streams is not yet included. See
        # test_platform_agent_with_oms.py for a test that includes this.



        #-------------------------------
        # Stop Platform SS AgentInstance,
        #-------------------------------
        self.imsclient.stop_platform_agent_instance(platform_agent_instance_id=platformSS_agent_instance_id)
