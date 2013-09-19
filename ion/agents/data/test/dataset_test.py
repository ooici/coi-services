#!/usr/bin/env python

"""
@package ion.agents.data.test.dataset_test
@file ion/agents/data/dataset_test.py
@author Bill French
@brief Base class for dataset agent tests
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

# Pyon log and config objects.
from pyon.public import log
from pyon.public import CFG

# 3rd party imports.
import gevent
from nose.plugins.attrib import attr

# Pyon pubsub and event support.
from pyon.event.event import EventSubscriber, EventPublisher
from pyon.ion.stream import StandaloneStreamSubscriber
from ion.services.dm.utility.granule_utils import RecordDictionaryTool

# Pyon unittest support.
from pyon.util.int_test import IonIntegrationTestCase

# Pyon Object Serialization
from pyon.core.object import IonObjectSerializer

# Pyon exceptions.
from pyon.core.exception import BadRequest, Conflict, Timeout, ResourceError
from pyon.core.exception import IonException
from pyon.core.exception import ConfigNotFound

# Agent imports.
from pyon.util.context import LocalContextMixin
from pyon.agent.agent import ResourceAgentClient
from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent

# Driver imports.
from ion.agents.instrument.direct_access.direct_access_server import DirectAccessTypes
from ion.agents.instrument.driver_int_test_support import DriverIntegrationTestSupport

from pyon.public import RT, log, PRED

# Objects and clients.
from interface.objects import AgentCommand
from interface.objects import CapabilityType
from interface.objects import AgentCapability
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient

# Alarms.
from pyon.public import IonObject
from interface.objects import StreamAlertType, AggregateStatusType

from ooi.timer import Timer

###############################################################################
# Global constants.
###############################################################################
DEPLOY_FILE='res/deploy/r2deploy.yml'
#PRELOAD_CATEGORIES="CoordinateSystem,Constraint,Contact,StreamDefinition,StreamConfiguration,DataProduct,DataProductLink,InstrumentModel,InstrumentDevice,ParameterFunctions,ParameterDefs,ParameterDictionary,ExternalDatasetAgent,ExternalDatasetAgentInstance"
PRELOAD_SCENARIO="BETA"
PRELOAD_CATEGORIES = [
    'IDMap',                            # mapping of preload IDs
    'Constraint',                       # in memory only - all scenarios loaded
    'Contact',                          # in memory only - all scenarios loaded
    'User',
    'Org',
    #'Policy',
    'UserRole',                         # no resource - association only
    'CoordinateSystem',                 # in memory only - all scenarios loaded
    'ParameterFunctions',
    'ParameterDefs',
    'ParameterDictionary',
    'Alerts',                           # in memory only - all scenarios loaded
    'StreamConfiguration',              # in memory only - all scenarios loaded
    'PlatformModel',
    'InstrumentModel',
    'Observatory',
    'Subsite',
    'PlatformSite',
    'InstrumentSite',
    'StreamDefinition',
    'PlatformDevice',
    'PlatformAgent',
    'PlatformAgentInstance',
    'InstrumentAgent',
    'InstrumentDevice',
    'ExternalDataProvider',
    'ExternalDatasetModel',
    'ExternalDataset',
    'ExternalDatasetAgent',
    'ExternalDatasetAgentInstance',
    'InstrumentAgentInstance',
    'DataProduct',
    'TransformFunction',
    'DataProcessDefinition',
    'DataProcess',
    'Parser',
    'Attachment',
    'DataProductLink',                  # no resource but complex service call
    'WorkflowDefinition',
    'Workflow',
    'Deployment',
    'Scheduler',
    'Reference',                        # No resource
    ]
PRELOAD_CATEGORIES = None

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''

class DatasetAgentTestConfig(object):
    """
    test config object.
    """
    dsa_module  = None
    dsa_class   = None

    instrument_device_name = None

    data_dir    = "/tmp/dsatest"

    dataset_agent_resource_id = '123xyz'
    dataset_agent_name = 'Agent007'
    dataset_agent_module = 'mi.idk.instrument_agent'
    dataset_agent_class = 'InstrumentAgent'

    preload_scenario = None

    def initialize(self, *args, **kwargs):

        log.debug("initialize with %s", kwargs)

        self.dsa_module = kwargs.get('dsa_module', self.dsa_module)
        self.dsa_class  = kwargs.get('dsa_class', self.dsa_class)
        self.data_dir   = kwargs.get('data_dir', self.data_dir)

        self.dataset_agent_resource_id = kwargs.get('dataset_agent_resource_id', self.dataset_agent_resource_id)
        self.dataset_agent_name = kwargs.get('dataset_agent_name', self.dataset_agent_name)
        self.dataset_agent_module = kwargs.get('dataset_agent_module', self.dataset_agent_module)
        self.dataset_agent_class = kwargs.get('dataset_agent_class', self.dataset_agent_class)

        self.instrument_device_name = kwargs.get('instrument_device_name', self.instrument_device_name)

        self.preload_scenario = kwargs.get('preload_scenario', self.preload_scenario)

    def verify(self):
        """
        TODO: Add more config verification code
        """
        if not self.instrument_device_name:
            raise ConfigNotFound("missing instrument_device_name")


class DatasetAgentTestCase(IonIntegrationTestCase):
    """
    Base class for all coi dataset agent end to end tests
    """
    test_config = DatasetAgentTestConfig()

    def setUp(self, deploy_file=DEPLOY_FILE):

        """
        Start container.
        Start deploy services.
        Define agent config, start agent.
        Start agent client.
        """
        self._dsa_client = None

        # Ensure we have a good test configuration
        self.test_config.verify()

        # Start container.
        log.info('Staring capability container.')
        self._start_container()

        # Bring up services in a deploy file (no need to message)
        log.info('Staring deploy services. %s', deploy_file)
        self.container.start_rel_from_url(DEPLOY_FILE)

        # Load instrument specific parameters
        log.info('Loading additional scenarios')
        self._load_params()

        # Build agent and driver configuration
        self._build_config()

        #log.info('building stream configuration')
        # Setup stream config.
        self._build_stream_config()

        # Start a resource agent client to talk with the instrument agent.
        #log.info('starting DSA process')
        #self._ia_client = self._start_dataset_agent_process(self.container, self._stream_config)
        #self.addCleanup(self._verify_agent_reset)
        log.info('test setup complete')

    def _load_params(self):
        """
        Load specific instrument specific parameters from preload
        """
        scenario = None
        categories = None

        if PRELOAD_CATEGORIES:
            categories = ",".join(PRELOAD_CATEGORIES)

        # load_parameter_scenarios
        if PRELOAD_SCENARIO:
            scenario = PRELOAD_SCENARIO
        else:
            log.warn("No common preload defined.  Was this intentional?")

        if self.test_config.preload_scenario:
            if scenario:
                scenario = "%s,%s" % (scenario, self.test_config.preload_scenario)
            else:
                scenario = self.test_config.preload_scenario
        else:
            log.warn("No DSA specific preload defined.  Was this intentional?")

        log.debug("doing preload now: %s", scenario)
        if scenario:
            self.container.spawn_process("Loader", "ion.processes.bootstrap.ion_loader", "IONLoader", config=dict(
                op="load",
                scenario=scenario,
                path="master",
                categories=categories,
                clearcols="owner_id,org_ids",
                assets="res/preload/r2_ioc/ooi_assets",
                parseooi="True",
            ))

    def _build_config(self):
        """
        Build the agent and driver configuration based on preload values.
        """
        dsa_instance = self._get_dsa_instance()
        dsa = self._get_dsa(dsa_instance)
        #stream_configs = self._get_dsa(dsa)

    def _get_dsa_instance(self):
        """
        Get the dsa instance object from a dsa agent instance id
        @return: dsa instance resource object
        """
        name = self.test_config.instrument_device_name
        rr = self.container.resource_registry

        objects,_ = rr.find_resources(RT.InstrumentDevice)
        filtered_objs = [obj for obj in objects if obj.name == name]
        if (filtered_objs) == []:
            raise ConfigNotFound("No appropriate InstrumentDevice objects loaded")
        if len(filtered_objs) > 1:
            raise ResourceError("Multiple DSA instances found.")

        instrument_device = filtered_objs[0]
        log.trace("Found instrument device: %s", instrument_device)

        dasi = rr.read_object(subject=instrument_device._id,
                                     predicate=PRED.hasAgentInstance,
                                     object_type=RT.ExternalDatasetAgentInstance)

        log.debug("dataset instance object: %s", dasi)
        return dasi

    def _get_dsa(self, dsa_instance):
        """
        Get the dsa object from a dsa agent instance object
        @return: dsa resource object
        """
        dsa_id = dsa_instance.agent_id
        if dsa_id is None:
            raise ConfigNotFound("Missing dataset agent id")

    def _build_stream_config(self):
        """
        """
        # Create a pubsub client to create streams.
        pubsub_client = PubsubManagementServiceClient(node=self.container.node)
        dataset_management = DatasetManagementServiceClient()

        encoder = IonObjectSerializer()

        # Create streams and subscriptions for each stream named in driver.
        self._stream_config = {}

        stream_name = 'ctdpf_parsed'
        param_dict_name = 'ctdpf_parsed'
        pd_id = dataset_management.read_parameter_dictionary_by_name(param_dict_name, id_only=True)
        stream_def_id = pubsub_client.create_stream_definition(name=stream_name, parameter_dictionary_id=pd_id)
        stream_def = pubsub_client.read_stream_definition(stream_def_id)
        stream_def_dict = encoder.serialize(stream_def)
        pd = stream_def.parameter_dictionary
        stream_id, stream_route = pubsub_client.create_stream(name=stream_name,
                                                exchange_point='science_data',
                                                stream_definition_id=stream_def_id)
        stream_config = dict(routing_key=stream_route.routing_key,
                                 exchange_point=stream_route.exchange_point,
                                 stream_id=stream_id,
                                 parameter_dictionary=pd,
                                 stream_def_dict=stream_def_dict)
        self._stream_config[stream_name] = stream_config

    #def start_instrument_agent_process(container, stream_config={}, resource_id=IA_RESOURCE_ID, resource_name=IA_NAME, org_governance_name=None, message_headers=None):
    def _start_dataset_agent_process(self):
        # Create agent config.
        agent_config = {
            'driver_config' : DVR_CONFIG,
            'stream_config' : self._stream_config,
            'agent'         : {'resource_id': resource_id},
            'test_mode' : True,
            'forget_past' : True,
            'enable_persistence' : False
        }

        # Start instrument agent.
        log.info("TestInstrumentAgent.setup(): starting IA.")
        container_client = ContainerAgentClient(node=self.container.node,
            name=self.container.name)

        log.info("Agent setup")
        ia_pid = container_client.spawn_process(name=resource_name,
            module=IA_MOD,
            cls=IA_CLS,
            config=agent_config)

        log.info('Agent pid=%s.', str(ia_pid))

        # Start a resource agent client to talk with the instrument agent.
        self.dsa_client = ResourceAgentClient(resource_id, process=FakeProcess())
        log.info('Got ia client %s.', str(dsa_client))

    def _verify_agent_reset(self):
        """
        Check agent state and reset if necessary.
        This called if a test fails and reset hasn't occurred.
        """
        if self._dsa_client is None:
            return

        state = self._dsa_client.get_agent_state()
        if state != ResourceAgentState.UNINITIALIZED:
            cmd = AgentCommand(command=ResourceAgentEvent.RESET)
            retval = self._dsa_client.execute_agent(cmd)