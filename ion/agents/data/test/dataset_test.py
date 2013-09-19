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
from pyon.core.bootstrap import get_service_registry

# 3rd party imports.
import os
from gevent.event import AsyncResult
import gevent
import shutil
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

from ion.services.dm.test.dm_test_case import breakpoint
from ion.processes.data.import_dataset import ImportDataset
from pyon.public import RT, log, PRED

# Objects and clients.
from interface.objects import AgentCommand
from interface.objects import CapabilityType
from interface.objects import AgentCapability
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient

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
    #'IDMap',                            # mapping of preload IDs
    'Constraint',                       # in memory only - all scenarios loaded
    'Contact',                          # in memory only - all scenarios loaded
    #'User',
    #'Org',
    #'UserRole',                         # no resource - association only
    'CoordinateSystem',                 # in memory only - all scenarios loaded
    #'ParameterFunctions',
    'ParameterDefs',
    'ParameterDictionary',
    #'Alerts',                           # in memory only - all scenarios loaded
    'StreamConfiguration',              # in memory only - all scenarios loaded
    'PlatformModel',
    'InstrumentModel',
    'Observatory',
    'Subsite',
    'PlatformSite',
    'InstrumentSite',
    'StreamDefinition',
    'PlatformDevice',
    #'PlatformAgent',
    #'PlatformAgentInstance',
    #'InstrumentAgent',
    'InstrumentDevice',
    #'ExternalDataProvider',
    #'ExternalDatasetModel',
    'ExternalDataset',
    'ExternalDatasetAgent',
    'ExternalDatasetAgentInstance',
    #'InstrumentAgentInstance',
    'DataProduct',
    #'TransformFunction',
    #'DataProcessDefinition',
    #'DataProcess',
    #'Parser',
    #'Attachment',
    'DataProductLink',                  # no resource but complex service call
    #'WorkflowDefinition',
    #'Workflow',
    #'Deployment',
    #'Scheduler',
    #'Reference',                        # No resource
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
    test_resource_dir = None

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

        self.test_resource_dir = kwargs.get('test_resource_dir', os.path.join(self.test_base_dir(), 'resource'))

    def verify(self):
        """
        TODO: Add more config verification code
        """
        if not self.instrument_device_name:
            raise ConfigNotFound("missing instrument_device_name")

        if not self.test_resource_dir:
            raise ConfigNotFound("missing test_resource_dir")

    def test_base_dir(self):
        """
        Determine the base directory for the test, this is used to figure out
        where the test resource files are located.
        """
        return os.path.dirname(__file__)

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

        # Start data subscribers
        self._build_stream_config()
        self._start_data_subscribers()

        # Start a resource agent client to talk with the instrument agent.
        log.info('starting DSA process')
        self._dsa_client = self._start_dataset_agent_process()
        self.addCleanup(self.assert_reset)
        log.info('test setup complete')

    ###
    #   Test/Agent Startup Helpers
    ###
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

    def _start_dataset_agent_process(self):
        # Create agent config.
        name = self.test_config.instrument_device_name
        rr = self.container.resource_registry

        log.debug("Start dataset agent process for instrument device: %s", name)
        objects,_ = rr.find_resources(RT.InstrumentDevice)
        log.debug("Found Instrument Devices: %s", objects)

        filtered_objs = [obj for obj in objects if obj.name == name]
        if (filtered_objs) == []:
            raise ConfigNotFound("No appropriate InstrumentDevice objects loaded")

        instrument_device = filtered_objs[0]
        log.trace("Found instrument device: %s", instrument_device)

        dsa_instance = rr.read_object(subject=instrument_device._id,
                                     predicate=PRED.hasAgentInstance,
                                     object_type=RT.ExternalDatasetAgentInstance)

        log.debug("dsa_instance found: %s", dsa_instance)
        self._driver_config = dsa_instance.driver_config

        self.clear_sample_data()

        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        proc_id = self.damsclient.start_external_dataset_agent_instance(dsa_instance._id)
        client = ResourceAgentClient(instrument_device._id, process=FakeProcess())

        return client

    ###
    #   Data file helpers
    ###

    def _get_source_data_file(self, filename):
        """
        Search for a sample data file, first check the driver resource directory
        then just use the filename as a path.  If the file doesn't exists
        raise an exception
        @param filename name or path of the file to search for
        @return full path to the found data file
        @raise IonException if the file isn't found
        """
        resource_dir = self.test_config.test_resource_dir
        source_path = os.path.join(resource_dir, filename)

        log.debug("Search for resource file (%s) in %s", filename, resource_dir)
        if os.path.isfile(source_path):
            log.debug("Found %s in resource directory", filename)
            return source_path

        log.debug("Search for resource file (%s) in current directory", filename)
        if os.path.isfile(filename):
            log.debug("Found %s in the current directory", filename)
            return filename

        raise IonException("Data file %s does not exist", filename)

    def create_data_dir(self):
        """
        Verify the test data directory is created and exists.  Return the path to
        the directory.
        @return: path to data directory
        @raise: ConfigNotFound no harvester config
        @raise: IonException if data_dir exists, but not a directory
        """
        startup_config = self._driver_config.get('startup_config')
        if not startup_config:
            raise ConfigNotFound("Driver config missing 'startup_config'")

        harvester_config = startup_config.get('harvester')
        if not harvester_config:
            raise ConfigNotFound("Startup config missing 'harvester' config")

        data_dir = harvester_config.get("directory")
        if not data_dir:
            raise ConfigNotFound("Harvester config missing 'directory'")

        if not os.path.exists(data_dir):
            log.debug("Creating data dir: %s", data_dir)
            os.makedirs(data_dir)

        elif not os.path.isdir(data_dir):
            raise IonException("'data_dir' is not a directory")

        return data_dir

    def clear_sample_data(self):
        """
        Remove all files from the sample data directory
        """
        data_dir = self.create_data_dir()

        log.debug("Clean all data from %s", data_dir)
        self.remove_all_files(data_dir)

    def create_sample_data(self, filename, dest_filename=None):
        """
        Search for a data file in the driver resource directory and if the file
        is not found there then search using the filename directly.  Then copy
        the file to the test data directory.

        If a dest_filename is supplied it will be renamed in the destination
        directory.
        @param: filename - filename or path to a data file to copy
        @param: dest_filename - name of the file when copied. default to filename
        """
        data_dir = self.create_data_dir()
        source_path = self._get_source_data_file(filename)

        log.debug("DIR: %s", data_dir)
        if dest_filename is None:
            dest_path = os.path.join(data_dir, os.path.basename(source_path))
        else:
            dest_path = os.path.join(data_dir, dest_filename)

        log.debug("Creating data file src: %s, dest: %s", source_path, dest_path)
        shutil.copy2(source_path, dest_path)

    def remove_all_files(self, dir_name):
        """
        Remove all files from a directory.  Raise an exception if the directory contains something
        other than files.
        @param dir_name directory path to remove files.
        @raise RuntimeError if the directory contains anything except files.
        """
        for file_name in os.listdir(dir_name):
            file_path = os.path.join(dir_name, file_name)
            if not os.path.isfile(file_path):
                raise RuntimeError("%s is not a file", file_path)

        for file_name in os.listdir(dir_name):
            file_path = os.path.join(dir_name, file_name)
            os.unlink(file_path)

    ###############################################################################
    # Event helpers.
    ###############################################################################

    def _start_event_subscriber(self, type='ResourceAgentEvent', count=0):
        """
        Start a subscriber to the instrument agent events.
        @param type The type of event to catch.
        @count Trigger the async event result when events received reaches this.
        """
        def consume_event(*args, **kwargs):
            log.info('Test recieved ION event: args=%s, kwargs=%s, event=%s.',
                     str(args), str(kwargs), str(args[0]))
            self._events_received.append(args[0])
            if self._event_count > 0 and \
                self._event_count == len(self._events_received):
                self._async_event_result.set()

        # Event array and async event result.
        self._event_count = count
        self._events_received = []
        self._async_event_result = AsyncResult()

        self._event_subscriber = EventSubscriber(
            event_type=type, callback=consume_event,
            origin=IA_RESOURCE_ID)
        self._event_subscriber.start()
        self._event_subscriber._ready_event.wait(timeout=5)

    def _stop_event_subscriber(self):
        """
        Stop event subscribers on cleanup.
        """
        self._event_subscriber.stop()
        self._event_subscriber = None

    ###############################################################################
    # Data stream helpers.
    ###############################################################################

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

    def _start_data_subscribers(self):
        """
        """
        # Create a pubsub client to create streams.
        pubsub_client = PubsubManagementServiceClient(node=self.container.node)

        # Create streams and subscriptions for each stream named in driver.
        self._data_subscribers = []
        self._samples_received = []
        self._raw_samples_received = []
        self._async_sample_result = AsyncResult()
        self._async_raw_sample_result = AsyncResult()

        # A callback for processing subscribed-to data.
        def recv_data(message, stream_route, stream_id):
            log.info('Received parsed data on %s (%s,%s)', stream_id, stream_route.exchange_point, stream_route.routing_key)
            self._samples_received.append(message)

        from pyon.util.containers import create_unique_identifier

        stream_name = 'ctdpf_parsed'
        parsed_config = self._stream_config[stream_name]
        stream_id = parsed_config['stream_id']
        exchange_name = create_unique_identifier("%s_queue" %
                    stream_name)
        self._purge_queue(exchange_name)
        sub = StandaloneStreamSubscriber(exchange_name, recv_data)
        sub.start()
        self._data_subscribers.append(sub)
        sub_id = pubsub_client.create_subscription(name=exchange_name, stream_ids=[stream_id])
        pubsub_client.activate_subscription(sub_id)
        sub.subscription_id = sub_id # Bind the subscription to the standalone subscriber (easier cleanup, not good in real practice)

    def _purge_queue(self, queue):
        xn = self.container.ex_manager.create_xn_queue(queue)
        xn.purge()

    def _stop_data_subscribers(self):
        for subscriber in self._data_subscribers:
            pubsub_client = PubsubManagementServiceClient()
            if hasattr(subscriber,'subscription_id'):
                try:
                    pubsub_client.deactivate_subscription(subscriber.subscription_id)
                except:
                    pass
                pubsub_client.delete_subscription(subscriber.subscription_id)
            subscriber.stop()

    ###
    #   Common assert methods
    ###

    def assert_initialize(self, final_state = ResourceAgentState.STREAMING):
        '''
        Walk through DSA states to get to streaming mode from uninitialized
        '''
        state = self._dsa_client.get_agent_state()

        with self.assertRaises(Conflict):
            res_state = self._dsa_client.get_resource_state()

        log.debug("Initialize DataSet agent")
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._dsa_client.execute_agent(cmd)
        state = self._dsa_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)
        log.info("Sent INITIALIZE; DSA state = %s", state)

        log.debug("DataSet agent go active")
        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = self._dsa_client.execute_agent(cmd)
        state = self._dsa_client.get_agent_state()
        log.info("Sent GO_ACTIVE; DSA state = %s", state)
        self.assertEqual(state, ResourceAgentState.IDLE)

        log.debug("DataSet agent run")
        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = self._dsa_client.execute_agent(cmd)
        state = self._dsa_client.get_agent_state()
        log.info("Sent RUN; DSA state = %s", state)
        self.assertEqual(state, ResourceAgentState.COMMAND)

        if final_state == ResourceAgentState.STREAMING:
            self.assert_start_sampling()

    def assert_stop_sampling(self):
        '''
        transition to command.  Must be called from streaming
        '''
        state = self._dsa_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.STREAMING)

        log.debug("DataSet agent start sampling")
        cmd = AgentCommand(command='DRIVER_EVENT_STOP_AUTOSAMPLE')
        retval = self._dsa_client.execute_resource(cmd)
        state = self._dsa_client.get_agent_state()
        log.info("Sent START SAMPLING; DSA state = %s", state)
        self.assertEqual(state, ResourceAgentState.COMMAND)

    def assert_start_sampling(self):
        '''
        transition to sampling.  Must be called from command
        '''
        state = self._dsa_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        log.debug("DataSet agent start sampling")
        cmd = AgentCommand(command='DRIVER_EVENT_START_AUTOSAMPLE')
        retval = self._dsa_client.execute_resource(cmd)
        state = self._dsa_client.get_agent_state()
        log.info("Sent START SAMPLING; DSA state = %s", state)
        self.assertEqual(state, ResourceAgentState.STREAMING)

    def assert_reset(self):
        '''
        Put the instrument back in uninitialized
        '''
        if self._dsa_client is None:
            return

        state = self._dsa_client.get_agent_state()

        if state != ResourceAgentState.UNINITIALIZED:
            cmd = AgentCommand(command=ResourceAgentEvent.RESET)
            retval = self._dsa_client.execute_agent(cmd)
            state = self._dsa_client.get_agent_state()

        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)
