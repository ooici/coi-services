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
import unittest
from ooi.logging import log

from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand
from interface.objects import ProcessDefinition

from ion.agents.platform.platform_agent import PlatformAgentState
from ion.agents.platform.platform_agent import PlatformAgentEvent

from ion.agents.platform.oms.oms_client_factory import OmsClientFactory

from pyon.event.event import EventSubscriber
from interface.objects import ProcessStateEnum

from gevent import queue


from ion.services.cei.process_dispatcher_service import ProcessStateGate


# The ID of the base platform for this test .
# These Ids and names should correspond to corresponding entries in network.yml,
# which is used by the OMS simulator.
PLATFORM_ID = 'Node1A'


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
        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.omsclient = ObservatoryManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)

        self.processdispatchclient = ProcessDispatcherServiceClient(node=self.container.node)


        self.platformModel_id = None

        # to retrieve network structure from RSN-OMS:
        self.rsn_oms = OmsClientFactory.create_instance()

        self.all_platforms = {}
        self.topology = {}

    def _prepare_platform(self, platform_id, parent_platform_objs):
        """
        This routine generalizes the manual construction currently done in
        test_oms_launch.py. It is called by the recursive _traverse method.

        Note: For simplicity in this test, sites are organized in the same
        hierarchical way as the platforms themselves.

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
            self.rrclient.create_association(
                subject=parent_platform_objs['site_id'],
                predicate=PRED.hasSite,
                object=site_id)

        device__obj = IonObject(RT.PlatformDevice,
                            name='%s_PlatformDevice' % platform_id,
                            description='%s_PlatformDevice platform device' % platform_id)

        device_id = self.imsclient.create_platform_device(device__obj)

        self.imsclient.assign_platform_model_to_platform_device(self.platformModel_id, device_id)

        if parent_platform_objs:
            self.rrclient.create_association(
                subject=parent_platform_objs['device_id'],
                predicate=PRED.hasDevice,
                object=device_id)

        agent__obj = IonObject(RT.PlatformAgent,
                            name='%s_PlatformAgent' % platform_id,
                            description='%s_PlatformAgent platform agent' % platform_id)

        agent_id = self.imsclient.create_platform_agent(agent__obj)

        if parent_platform_objs:
            # note, appending platform_id instead of agent_id as in test_oms_launch:
            parent_platform_objs['children'].append(platform_id)
#            parent_platform_objs['children'].append(agent_id)


        self.imsclient.assign_platform_model_to_platform_agent(self.platformModel_id, agent_id)

        agent_instance_obj = IonObject(RT.PlatformAgentInstance,
                                name='%s_PlatformAgentInstance' % platform_id,
                                description="%s_PlatformAgentInstance" % platform_id,
                                driver_module='ion.agents.platform.platform_agent',
                                driver_class='PlatformAgent')

        agent_instance_id = self.imsclient.create_platform_agent_instance(
                            agent_instance_obj, agent_id, device_id)

        plat_objs = {
            'platform_id':        platform_id,
            'site__obj':          site__obj,
            'site_id':            site_id,
            'device__obj':        device__obj,
            'device_id':          device_id,
            'agent__obj':         agent__obj,
            'agent_id':           agent_id,
            'agent_instance_obj': agent_instance_obj,
            'agent_instance_id':  agent_instance_id,
            'children':           []
        }
        #
        # Note: there're other pieces of information that can be retrieved
        # using self.rsn_oms (for example self.rsn_oms.getPlatformAttributes)
        # that could be added to the plat_objs dictionary. At the moment,
        # basically only the network topology is passed to the base platform
        # agent, which will then query the RSN-OMS interface for associated
        # attributes and other information. For this to happen, note that the
        # topology here is defined in terms of platform IDs (not agent IDs as
        # in test_oms_launch).
        # TODO need to determine the overall strategy.
        #

        log.info("plat_objs for platform_id %r = %s", platform_id, str(plat_objs))
        return plat_objs

    def _traverse(self, platform_id, parent_platform_objs=None):
        """
        Recursive routine that repeatedly calls _prepare_platform to build
        the object dictionary for each platform.

        @param platform_id ID of the platform to be visited
        @param parent_platform_objs dict of objects associated to parent
                        platform, if any.

        @retval the dict returned by _prepare_platform at this level.
        """

        log.info("Starting _traverse for %r", platform_id)

        plat_objs = self._prepare_platform(platform_id, parent_platform_objs)

        self.all_platforms[platform_id] = plat_objs

        # now, traverse the children:
        retval = self.rsn_oms.getSubplatformIDs(platform_id)
        subplatform_ids = retval[platform_id]
        for subplatform_id in subplatform_ids:
            self._traverse(subplatform_id, plat_objs)

        # note, topology in terms of platform_id instead of agent_id as in
        # test_oms_launch:
        self.topology[platform_id] = plat_objs['children']
#        self.topology[plat_objs['agent_id']] = plat_objs['children']

        return plat_objs


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
        platformModel_obj = IonObject(RT.PlatformModel,
                                      name='RSNPlatformModel',
                                      description="RSNPlatformModel",
                                      model="RSNPlatformModel")
        try:
            self.platformModel_id = self.imsclient.create_platform_model(platformModel_obj)
        except BadRequest as ex:
            self.fail("failed to create new PLatformModel: %s" %ex)
        log.debug( 'new PlatformModel id = %s' % self.platformModel_id)



        # trigger the traversal of the RSN-OMS reported network from some base
        # platform: if the root, we'd use self.rsn_oms.getRootPlatformID(),
        # but here we use another platform ID that exists in network.yml:
        base_platform_id = PLATFORM_ID
        base_platform_objs = self._traverse(base_platform_id)

        log.info("base_platform_id = %r", base_platform_id)
        log.info("topology = %s", str(self.topology))


        #-------------------------------
        # quick local test of retrieving associations:
        device_id = base_platform_objs['device_id']
        objs, assocs = self.rrclient.find_objects(device_id, PRED.hasDevice, RT.PlatformDevice, id_only=True)
        log.debug('Found associated devices for device_id=%r: objs=%s, assocs=%s' % (device_id, objs, assocs))
        for obj in objs: log.debug("Retrieved object=%s" % obj)
        #-------------------------------

        #-------------------------------
        # Launch Base Platform AgentInstance, connect to the resource agent client
        #-------------------------------

        agent_instance_id = base_platform_objs['agent_instance_id']
        pid = self.imsclient.start_platform_agent_instance(platform_agent_instance_id=agent_instance_id)
        log.debug("start_platform_agent_instance returned pid=%s", pid)

        #wait for start
        instance_obj = self.imsclient.read_platform_agent_instance(agent_instance_id)
        gate = ProcessStateGate(self.processdispatchclient.read_process,
                                instance_obj.agent_process_id,
                                ProcessStateEnum.SPAWN)
        self.assertTrue(gate.await(90), "The platform agent instance did not spawn in 90 seconds")

        agent_instance_obj= self.imsclient.read_instrument_agent_instance(agent_instance_id)
        log.debug('test_oms_create_and_launch: Platform agent instance obj: %s', str(agent_instance_obj))

        # Start a resource agent client to talk with the instrument agent.
        self._pa_client = ResourceAgentClient('paclient', name=agent_instance_obj.agent_process_id,  process=FakeProcess())
        log.debug(" test_oms_create_and_launch:: got pa client %s" % str(self._pa_client))

        DVR_CONFIG = {
            'dvr_mod': 'ion.agents.platform.oms.oms_platform_driver',
            'dvr_cls': 'OmsPlatformDriver',
            'oms_uri': 'embsimulator'
        }

        # note: ID value for 'platform_id' should be consistent with IDs in topology:
        PLATFORM_CONFIG = {
            'platform_id': base_platform_id,
#            'platform_id': base_platform_objs['agent_id'],
            'platform_topology' : self.topology,

            'driver_config': DVR_CONFIG,
            'container_name': self.container.name,
        }

        log.debug("Base PLATFORM_CONFIG = %s", PLATFORM_CONFIG)

        # PING_AGENT can be issued before INITIALIZE
        cmd = AgentCommand(command=PlatformAgentEvent.PING_AGENT)
        retval = self._pa_client.execute_agent(cmd)
        log.debug( 'Base Platform PING_AGENT = %s' % str(retval) )

        # INITIALIZE should trigger the creation of the whole platform
        # hierarchy rooted at PLATFORM_CONFIG['platform_id']
        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE, kwargs=dict(plat_config=PLATFORM_CONFIG))
        retval = self._pa_client.execute_agent(cmd)
        log.debug( 'Base Platform INITIALIZE = %s' % str(retval) )


        # GO_ACTIVE
        cmd = AgentCommand(command=PlatformAgentEvent.GO_ACTIVE)
        retval = self._pa_client.execute_agent(cmd)
        log.debug( 'Base Platform GO_ACTIVE = %s' % str(retval) )

        # RUN
        cmd = AgentCommand(command=PlatformAgentEvent.RUN)
        retval = self._pa_client.execute_agent(cmd)
        log.debug( 'Base Platform RUN = %s' % str(retval) )

        # TODO: here we could sleep for a little bit to let the resource
        # monitoring work for a while. But not done yet because the
        # definition of streams is not yet included. See
        # test_platform_agent_with_oms.py for a test that includes this.



        #-------------------------------
        # Stop Base Platform AgentInstance
        #-------------------------------
        self.imsclient.stop_platform_agent_instance(platform_agent_instance_id=agent_instance_id)
