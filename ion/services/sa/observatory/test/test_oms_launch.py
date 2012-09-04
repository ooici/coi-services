#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.ion.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.services.sa.observatory.observatory_management_service import ObservatoryManagementService
from interface.services.sa.iobservatory_management_service import IObservatoryManagementService, ObservatoryManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict, Inconsistent
from pyon.public import RT, PRED
#from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
from pyon.util.log import log

from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand
from interface.objects import ProcessDefinition

from ion.agents.platform.platform_agent import PlatformAgentState
from ion.agents.platform.platform_agent import PlatformAgentEvent

import os

# The ID of the root platform for this test and the IDs of its sub-platforms.
# These Ids should correspond to corresponding entries in network.yml,
# which is used by the OMS simulator.
PLATFORM_ID = 'platA1'
SUBPLATFORM_IDS = ['platA1a', 'platA1b']

DVR_CONFIG = {
    'dvr_mod': 'ion.agents.platform.oms.oms_platform_driver',
    'dvr_cls': 'OmsPlatformDriver',
    'oms_uri':  'foo'               # os.getenv('OMS', 'embsimulator'),
}

PLATFORM_CONFIG = {
    'platform_id': PLATFORM_ID,
    'driver_config': DVR_CONFIG
}

# Agent parameters.
PA_RESOURCE_ID = 'oms_platform_agent_001'
PA_NAME = 'OmsPlatformAgent001'
PA_MOD = 'ion.agents.platform.platform_agent'
PA_CLS = 'PlatformAgent'


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('INT', group='sa')
class TestOmsLaunch(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        #print 'instantiating container'
        self._start_container()
        #container = Container()
        #print 'starting container'
        #container.start()
        #print 'started container'

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.omsclient = ObservatoryManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)




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


        agent_config = {
            'agent'         : {'resource_id': PA_RESOURCE_ID},
            'stream_config' : self._stream_config,
            'test_mode' : True
        }




        # Create PlatformModel
        platformModel_obj = IonObject(RT.PlatformModel, name='RSNPlatformModel', description="RSNPlatformModel", model="RSNPlatformModel" )
        try:
            platformModel_id = self.imsclient.create_platform_model(platformModel_obj)
        except BadRequest as ex:
            self.fail("failed to create new PLatformModel: %s" %ex)
        log.debug( 'new PLatformModel id = %s ', platformModel_id)


        #-------------------------------
        # Platform A
        #-------------------------------
        platformA_site__obj = IonObject(RT.PlatformSite,
                                        name='PlatformASite',
                                        description='PlatformASite platform site')
        platformA_site_id = self.omsclient.create_platform_site(platformA_site__obj)

        platformA_device__obj = IonObject(RT.PlatformDevice,
                                        name='PlatformADevice',
                                        description='PlatformADevice platform device')
        platformA_device_id = self.imsclient.create_platform_device(platformA_device__obj)
        self.imsclient.assign_platform_model_to_platform_device(platformModel_id, platformA_device_id)

        platformA_agent__obj = IonObject(RT.PlatformAgent,
                                        name='PlatformAAgent',
                                        description='PlatformAAgent platform agent')
        platformA_agent_id = self.imsclient.create_platform_agent(platformA_agent__obj)
        self.imsclient.assign_platform_model_to_platform_agent(platformModel_id, platformA_agent_id)


        platform_agent_instance_obj = IonObject(RT.PlatformAgentInstance, name='RSNPlatformAgentInstance', description="RSNPlatformAgentInstance",
                                          driver_module='ion.agents.platform.platform_agent', driver_class='PlatformAgent'   )
        platform_agent_instance_id = self.imsclient.create_platform_agent_instance(platform_agent_instance_obj, platformA_agent_id, platformA_device_id)



        #-------------------------------
        # Launch Platform A AgentInstance, connect to the resource agent client
        #-------------------------------
        self.imsclient.start_platform_agent_instance(platform_agent_instance_id=platform_agent_instance_id)

        platform_agent_instance_obj= self.imsclient.read_instrument_agent_instance(platform_agent_instance_id)
        print 'test_oms_create_and_launch: Platform agent instance obj: = ', platform_agent_instance_obj

        # Start a resource agent client to talk with the instrument agent.
        self._pa_client = ResourceAgentClient('paclient', name=platform_agent_instance_obj.agent_process_id,  process=FakeProcess())
        log.debug(" test_oms_create_and_launch:: got pa client %s", str(self._pa_client))

#        DVR_CONFIG = {
#            'dvr_mod': 'ion.agents.platform.oms.oms_platform_driver',
#            'dvr_cls': 'OmsPlatformDriver',
#            'oms_uri': 'embsimulator'
#        }
#
#        PLATFORM_CONFIG = {
#            'platform_id': platformA_device_id,
#            'driver_config': DVR_CONFIG
#        }
#
#        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE, kwargs=dict(plat_config=PLATFORM_CONFIG))
#        retval = self._pa_client.execute_agent(cmd)