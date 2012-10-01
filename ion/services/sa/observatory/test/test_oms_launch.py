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


        #CustomAttribute:
        #name: ""
        #type: ""
        #description: ""
        #default:

        custom_attributes = []
        #create the attributes that are specific to this model type
        custom_attr_obj = IonObject(OT.CustomAttribute, name='port', type='string', description='the port identifier (ip address) for this ')

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
        topology = {}

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
        ports.append(  IonObject(OT.PlatformMonitorAttributes, id='ShoreStation_attr_1', monitor_rate=5, units='xyz')  )
        ports.append(  IonObject(OT.PlatformMonitorAttributes, id='ShoreStation_attr_2', monitor_rate=5, units='xyz')  )

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
        ports.append(  IonObject(OT.PlatformMonitorAttributes, id='Node1A_attr_1', monitor_rate=5, units='xyz')  )
        ports.append(  IonObject(OT.PlatformMonitorAttributes, id='Node1A_attr_2', monitor_rate=5, units='xyz')  )

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

        topology[platform1A_agent_instance_id] = [platformSS_agent_instance_id]
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
        ports.append(  IonObject(OT.PlatformMonitorAttributes, id='Node1B_attr_1', monitor_rate=5, units='xyz')  )
        ports.append(  IonObject(OT.PlatformMonitorAttributes, id='Node1B_attr_2', monitor_rate=5, units='xyz')  )

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

        topology[platform1B_agent_instance_id] = [platform1A_agent_instance_id]
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
        ports.append(  IonObject(OT.PlatformMonitorAttributes, id='Node1C_attr_1', monitor_rate=5, units='xyz')  )
        ports.append(  IonObject(OT.PlatformMonitorAttributes, id='Node1C_attr_2', monitor_rate=5, units='xyz')  )


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

        topology[platform1C_agent_instance_id] = [platform1B_agent_instance_id]
        agent_device_map[platform1C_agent_instance_id] = platform1C_device__obj

#
#        #-------------------------------
#        # Platform A
#        #-------------------------------
#        platformA_site__obj = IonObject(RT.PlatformSite,
#            name='PlatformASite',
#            description='PlatformASite platform site')
#        platformA_site_id = self.omsclient.create_platform_site(platformA_site__obj)
#        self.rrclient.create_association(subject=platformSS_site_id, predicate=PRED.hasSite, object=platformA_site_id)
#
#
#        platformA_device__obj = IonObject(RT.PlatformDevice,
#            name='PlatformADevice',
#            description='PlatformADevice platform device')
#        platformA_device_id = self.imsclient.create_platform_device(platformA_device__obj)
#        self.imsclient.assign_platform_model_to_platform_device(platformModel_id, platformA_device_id)
#        self.rrclient.create_association(subject=platformSS_device_id, predicate=PRED.hasDevice, object=platformA_device_id)
#
#        platformA_agent__obj = IonObject(RT.PlatformAgent,
#            name='PlatformAAgent',
#            description='PlatformAAgent platform agent')
#        platformA_agent_id = self.imsclient.create_platform_agent(platformA_agent__obj)
#        self.imsclient.assign_platform_model_to_platform_agent(platformModel_id, platformA_agent_id)
#
#
#        platformA_agent_instance_obj = IonObject(RT.PlatformAgentInstance, name='PlatformAAgentInstance', description="PlatformAAgentInstance",
#            driver_module='ion.agents.platform.platform_agent', driver_class='PlatformAgent'   )
#        platformA_agent_instance_id = self.imsclient.create_platform_agent_instance(platformA_agent_instance_obj, platformA_agent_id, platformA_device_id)
#
#        #-------------------------------
#        # Platform A1
#        #-------------------------------
#        platformA1_site__obj = IonObject(RT.PlatformSite,
#                                        name='PlatformA1Site',
#                                        description='PlatformA1Site platform site')
#        platformA1_site_id = self.omsclient.create_platform_site(platformA1_site__obj)
#        self.rrclient.create_association(subject=platformA_site_id, predicate=PRED.hasSite, object=platformA1_site_id)
#
#
#        platformA1_device__obj = IonObject(RT.PlatformDevice,
#                                        name='PlatformA1Device',
#                                        description='PlatformA1Device platform device')
#        platformA1_device_id = self.imsclient.create_platform_device(platformA1_device__obj)
#        self.imsclient.assign_platform_model_to_platform_device(platformModel_id, platformA1_device_id)
#        self.rrclient.create_association(subject=platformA_device_id, predicate=PRED.hasDevice, object=platformA1_device_id)
#
#        platformA1_agent__obj = IonObject(RT.PlatformAgent,
#                                        name='PlatformA1Agent',
#                                        description='PlatformA1Agent platform agent')
#        platformA1_agent_id = self.imsclient.create_platform_agent(platformA1_agent__obj)
#        self.imsclient.assign_platform_model_to_platform_agent(platformModel_id, platformA1_agent_id)
#
#
#        platformA1_agent_instance_obj = IonObject(RT.PlatformAgentInstance, name='PlatformA1AgentInstance', description="PlatformA1AgentInstance",
#                                          driver_module='ion.agents.platform.platform_agent', driver_class='PlatformAgent'   )
#        platformA1_agent_instance_id = self.imsclient.create_platform_agent_instance(platformA1_agent_instance_obj, platformA1_agent_id, platformA1_device_id)
#
#
#
#        #-------------------------------
#        # Platform A1a
#        #-------------------------------
#        platformA1a_site__obj = IonObject(RT.PlatformSite,
#                                        name='PlatformA1aSite',
#                                        description='PlatformA1aSite platform site')
#        platformA1a_site_id = self.omsclient.create_platform_site(platformA1a_site__obj)
#        self.rrclient.create_association(subject=platformA1_site_id, predicate=PRED.hasSite, object=platformA1a_site_id)
#
#
#        platformA1a_device__obj = IonObject(RT.PlatformDevice,
#                                        name='PlatformA1aDevice',
#                                        description='PlatformA1aDevice platform device')
#        platformA1a_device_id = self.imsclient.create_platform_device(platformA1a_device__obj)
#        self.imsclient.assign_platform_model_to_platform_device(platformModel_id, platformA1a_device_id)
#        self.rrclient.create_association(subject=platformA1_device_id, predicate=PRED.hasDevice, object=platformA1a_device_id)
#
#        platformA1a_agent__obj = IonObject(RT.PlatformAgent,
#                                        name='PlatformA1aAgent',
#                                        description='PlatformA1aAgent platform agent')
#        platformA1a_agent_id = self.imsclient.create_platform_agent(platformA1a_agent__obj)
#        self.imsclient.assign_platform_model_to_platform_agent(platformModel_id, platformA1a_agent_id)
#
#
#        platformA1a_agent_instance_obj = IonObject(RT.PlatformAgentInstance, name='PlatformA1aAgentInstance', description="PlatformA1aAgentInstance",
#                                          driver_module='ion.agents.platform.platform_agent', driver_class='PlatformAgent'   )
#        platformA1a_agent_instance_id = self.imsclient.create_platform_agent_instance(platformA1a_agent_instance_obj, platformA1a_agent_id, platformA1a_device_id)
#
#
#        #-------------------------------
#        # Platform A1b
#        #-------------------------------
#        platformA1b_site__obj = IonObject(RT.PlatformSite,
#                                        name='PlatformA1bSite',
#                                        description='PlatformA1bSite platform site')
#        platformA1b_site_id = self.omsclient.create_platform_site(platformA1b_site__obj)
#        self.rrclient.create_association(subject=platformA1_site_id, predicate=PRED.hasSite, object=platformA1b_site_id)
#
#
#        platformA1b_device__obj = IonObject(RT.PlatformDevice,
#                                        name='PlatformA1bDevice',
#                                        description='PlatformA1bDevice platform device')
#        platformA1b_device_id = self.imsclient.create_platform_device(platformA1b_device__obj)
#        self.imsclient.assign_platform_model_to_platform_device(platformModel_id, platformA1b_device_id)
#        self.rrclient.create_association(subject=platformA1_device_id, predicate=PRED.hasDevice, object=platformA1b_device_id)
#
#        platformA1b_agent__obj = IonObject(RT.PlatformAgent,
#                                        name='PlatformA1bAgent',
#                                        description='PlatformA1bAgent platform agent')
#        platformA1b_agent_id = self.imsclient.create_platform_agent(platformA1b_agent__obj)
#        self.imsclient.assign_platform_model_to_platform_agent(platformModel_id, platformA1b_agent_id)
#
#
#        platformA1b_agent_instance_obj = IonObject(RT.PlatformAgentInstance, name='PlatformA1bAgentInstance', description="PlatformA1bAgentInstance",
#                                          driver_module='ion.agents.platform.platform_agent', driver_class='PlatformAgent'   )
#        platformA1b_agent_instance_id = self.imsclient.create_platform_agent_instance(platformA1b_agent_instance_obj, platformA1b_agent_id, platformA1b_device_id)
#
#
#        topology[platformA_agent_id] = [platformA1a_agent_id, platformA1b_agent_id]
#
#        #-------------------------------
#        # Platform A1b1
#        #-------------------------------
#        platformA1b1_site__obj = IonObject(RT.PlatformSite,
#                                        name='PlatformA1b1Site',
#                                        description='PlatformA1b1Site platform site')
#        platformA1b1_site_id = self.omsclient.create_platform_site(platformA1b1_site__obj)
#        self.rrclient.create_association(subject=platformA1b_site_id, predicate=PRED.hasSite, object=platformA1b1_site_id)
#
#
#        platformA1b1_device__obj = IonObject(RT.PlatformDevice,
#                                        name='PlatformA1b1Device',
#                                        description='PlatformA1b1Device platform device')
#        platformA1b1_device_id = self.imsclient.create_platform_device(platformA1b1_device__obj)
#        self.imsclient.assign_platform_model_to_platform_device(platformModel_id, platformA1b1_device_id)
#        self.rrclient.create_association(subject=platformA1b_device_id, predicate=PRED.hasDevice, object=platformA1b1_device_id)
#
#        platformA1b1_agent__obj = IonObject(RT.PlatformAgent,
#                                        name='PlatformA1b1Agent',
#                                        description='PlatformA1b1Agent platform agent')
#        platformA1b1_agent_id = self.imsclient.create_platform_agent(platformA1b1_agent__obj)
#        self.imsclient.assign_platform_model_to_platform_agent(platformModel_id, platformA1b1_agent_id)
#
#
#        platformA1b1_agent_instance_obj = IonObject(RT.PlatformAgentInstance, name='PlatformA1b1AgentInstance', description="PlatformA1b1AgentInstance",
#                                          driver_module='ion.agents.platform.platform_agent', driver_class='PlatformAgent'   )
#        platformA1b1_agent_instance_id = self.imsclient.create_platform_agent_instance(platformA1b1_agent_instance_obj, platformA1b1_agent_id, platformA1b1_device_id)
#
#
#        #-------------------------------
#        # Platform A1b2
#        #-------------------------------
#        platformA1b2_site__obj = IonObject(RT.PlatformSite,
#                                        name='PlatformA1b2Site',
#                                        description='PlatformA1b2Site platform site')
#        platformA1b2_site_id = self.omsclient.create_platform_site(platformA1b2_site__obj)
#        self.rrclient.create_association(subject=platformA1b_site_id, predicate=PRED.hasSite, object=platformA1b2_site_id)
#
#
#        platformA1b2_device__obj = IonObject(RT.PlatformDevice,
#                                        name='PlatformA1b2Device',
#                                        description='PlatformA1b2Device platform device')
#        platformA1b2_device_id = self.imsclient.create_platform_device(platformA1b2_device__obj)
#        self.imsclient.assign_platform_model_to_platform_device(platformModel_id, platformA1b2_device_id)
#        self.rrclient.create_association(subject=platformA1b_device_id, predicate=PRED.hasDevice, object=platformA1b2_device_id)
#
#        platformA1b2_agent__obj = IonObject(RT.PlatformAgent,
#                                        name='PlatformA1b2Agent',
#                                        description='PlatformA1b2Agent platform agent')
#        platformA1b2_agent_id = self.imsclient.create_platform_agent(platformA1b2_agent__obj)
#        self.imsclient.assign_platform_model_to_platform_agent(platformModel_id, platformA1b2_agent_id)
#
#
#        platformA1b2_agent_instance_obj = IonObject(RT.PlatformAgentInstance, name='PlatformA1b2AgentInstance', description="PlatformA1b2AgentInstance",
#                                          driver_module='ion.agents.platform.platform_agent', driver_class='PlatformAgent'   )
#        platformA1b2_agent_instance_id = self.imsclient.create_platform_agent_instance(platformA1b2_agent_instance_obj, platformA1b2_agent_id, platformA1b2_device_id)
#
#        topology[platformA1b_agent_id] = [platformA1b1_agent_id, platformA1b2_agent_id]
#
#        #-------------------------------
#        # Platform B
#        #-------------------------------
#        platformB_site__obj = IonObject(RT.PlatformSite,
#                                        name='PlatformBSite',
#                                        description='PlatformBSite platform site')
#        platformB_site_id = self.omsclient.create_platform_site(platformB_site__obj)
#        self.rrclient.create_association(subject=platformSS_site_id, predicate=PRED.hasSite, object=platformB_site_id)
#
#
#        platformB_device__obj = IonObject(RT.PlatformDevice,
#                                        name='PlatformBDevice',
#                                        description='PlatformBDevice platform device')
#        platformB_device_id = self.imsclient.create_platform_device(platformB_device__obj)
#        self.imsclient.assign_platform_model_to_platform_device(platformModel_id, platformB_device_id)
#        self.rrclient.create_association(subject=platformSS_device_id, predicate=PRED.hasDevice, object=platformB_device_id)
#
#        platformB_agent__obj = IonObject(RT.PlatformAgent,
#                                        name='PlatformBAgent',
#                                        description='PlatformBAgent platform agent')
#        platformB_agent_id = self.imsclient.create_platform_agent(platformB_agent__obj)
#        self.imsclient.assign_platform_model_to_platform_agent(platformModel_id, platformB_agent_id)
#
#
#        platformB_agent_instance_obj = IonObject(RT.PlatformAgentInstance, name='PlatformBAgentInstance', description="PlatformBAgentInstance",
#                                          driver_module='ion.agents.platform.platform_agent', driver_class='PlatformAgent'   )
#        platformB_agent_instance_id = self.imsclient.create_platform_agent_instance(platformB_agent_instance_obj, platformB_agent_id, platformB_device_id)
#
#        topology[platformSS_agent_id] = [platformA_agent_id, platformB_agent_id]
#
#        #-------------------------------
#        # Platform B1
#        #-------------------------------
#        platformB1_site__obj = IonObject(RT.PlatformSite,
#                                        name='PlatformB1Site',
#                                        description='PlatformB1Site platform site')
#        platformB1_site_id = self.omsclient.create_platform_site(platformB1_site__obj)
#        self.rrclient.create_association(subject=platformB_site_id, predicate=PRED.hasSite, object=platformB1_site_id)
#
#
#        platformB1_device__obj = IonObject(RT.PlatformDevice,
#                                        name='PlatformB1Device',
#                                        description='PlatformB1Device platform device')
#        platformB1_device_id = self.imsclient.create_platform_device(platformB1_device__obj)
#        self.imsclient.assign_platform_model_to_platform_device(platformModel_id, platformB1_device_id)
#        self.rrclient.create_association(subject=platformB_device_id, predicate=PRED.hasDevice, object=platformB1_device_id)
#
#        platformB1_agent__obj = IonObject(RT.PlatformAgent,
#                                        name='PlatformB1Agent',
#                                        description='PlatformB1Agent platform agent')
#        platformB1_agent_id = self.imsclient.create_platform_agent(platformB1_agent__obj)
#        self.imsclient.assign_platform_model_to_platform_agent(platformModel_id, platformB1_agent_id)
#
#
#        platformB1_agent_instance_obj = IonObject(RT.PlatformAgentInstance, name='PlatformB1AgentInstance', description="PlatformB1AgentInstance",
#                                          driver_module='ion.agents.platform.platform_agent', driver_class='PlatformAgent'   )
#        platformB1_agent_instance_id = self.imsclient.create_platform_agent_instance(platformB1_agent_instance_obj, platformB1_agent_id, platformB1_device_id)
#
#
#
#
#        #-------------------------------
#        # Platform B2
#        #-------------------------------
#        platformB2_site__obj = IonObject(RT.PlatformSite,
#                                        name='PlatformB2Site',
#                                        description='PlatformB2Site platform site')
#        platformB2_site_id = self.omsclient.create_platform_site(platformB2_site__obj)
#        self.rrclient.create_association(subject=platformB_site_id, predicate=PRED.hasSite, object=platformB2_site_id)
#
#
#        platformB2_device__obj = IonObject(RT.PlatformDevice,
#                                        name='PlatformB2Device',
#                                        description='PlatformB2Device platform device')
#        platformB2_device_id = self.imsclient.create_platform_device(platformB2_device__obj)
#        self.imsclient.assign_platform_model_to_platform_device(platformModel_id, platformB2_device_id)
#        self.rrclient.create_association(subject=platformB_device_id, predicate=PRED.hasDevice, object=platformB2_device_id)
#
#        platformB2_agent__obj = IonObject(RT.PlatformAgent,
#                                        name='PlatformB2Agent',
#                                        description='PlatformB2Agent platform agent')
#        platformB2_agent_id = self.imsclient.create_platform_agent(platformB2_agent__obj)
#        self.imsclient.assign_platform_model_to_platform_agent(platformModel_id, platformB2_agent_id)
#
#
#        platformB2_agent_instance_obj = IonObject(RT.PlatformAgentInstance, name='PlatformB2AgentInstance', description="PlatformB2AgentInstance",
#                                          driver_module='ion.agents.platform.platform_agent', driver_class='PlatformAgent'   )
#        platformB2_agent_instance_id = self.imsclient.create_platform_agent_instance(platformB2_agent_instance_obj, platformB2_agent_id, platformB2_device_id)
#
#        topology[platformB_agent_id] = [platformB1_agent_id, platformB2_agent_id]
#
#        #-------------------------------
#        # quick local test of retrieving associations:
#        objs, assocs = self.rrclient.find_objects(platformSS_device_id, PRED.hasDevice, RT.PlatformDevice, id_only=True)
#        log.debug('Found associated devices for platformSS_device_id=%r: objs=%s, assocs=%s' % (platformSS_device_id, objs, assocs))
#        for obj in objs: log.debug("Retrieved object=%s" % obj)
#        objs, assocs = self.rrclient.find_objects(platformA_device_id, PRED.hasDevice)
#        log.debug('Found associated devices for platformA_device_id=%r: objs=%s, assocs=%s' % (platformA_device_id, objs, assocs))
#        for obj in objs: log.debug("Retrieved object._id=%s" % obj._id)
#        #-------------------------------
#
#







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
            'platform_topology' : topology,
            'driver_config': DVR_CONFIG,
            'container_name': self.container.name,
            'agent_instance_object_list': agent_instance_list

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
