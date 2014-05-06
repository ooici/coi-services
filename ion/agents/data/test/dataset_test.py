#!/usr/bin/env python

"""
@package ion.agents.data.test.dataset_test
@file ion/agents/data/dataset_test.py
@author Bill French
@brief Base class for dataset agent tests
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import os
import sys
import pprint
import collections
import unittest
from gevent.event import AsyncResult
import gevent
import shutil
from nose.plugins.attrib import attr

# Pyon unittest support.
from ion.util.agent_launcher import AgentLauncher
from ion.agents.data.result_set import ResultSet
from ion.agents.instrument.common import BaseEnum
from pyon.util.int_test import IonIntegrationTestCase

# Pyon
from pyon.event.event import EventSubscriber, EventPublisher
from pyon.ion.stream import StandaloneStreamSubscriber
from pyon.core.exception import ConfigNotFound, ResourceError, ServerError
from pyon.core.object import IonObjectSerializer
from pyon.public import RT, log, PRED, BadRequest, Conflict, Timeout, IonException, NotFound, IonObject
from pyon.util.context import LocalContextMixin
from pyon.agent.agent import ResourceAgentClient, ResourceAgentState, ResourceAgentEvent

from ion.agents.data.dataset_agent import DSA_STATE_KEY

# ION services and utils
from ion.services.dm.test.dm_test_case import breakpoint
from ion.services.dm.utility.granule_utils import time_series_domain, RecordDictionaryTool
from ion.services.sa.instrument.agent_configuration_builder import ExternalDatasetAgentConfigurationBuilder

# Objects and clients.
from interface.objects import DataProduct, AgentCapability, CapabilityType, AgentCommand
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceDependentClients
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.sa.idata_acquisition_management_service import  DataAcquisitionManagementServiceClient

from ion.processes.bootstrap.ion_loader import TESTED_DOC

###############################################################################
# Global constants.
###############################################################################
DEPLOY_FILE='res/deploy/r2deploy.yml'
PRELOAD_SCENARIO="BETA"
PRELOAD_CATEGORIES = [
    #'IDMap',                            # mapping of preload IDs
    'Constraint',                       # in memory only - all scenarios loaded
    'Contact',                          # in memory only - all scenarios loaded
    #'User',
    #'Org',
    #'UserRole',                         # no resource - association only
    'CoordinateSystem',                 # in memory only - all scenarios loaded
    'ParameterFunctions',
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

# Uncomment to load all categories
#PRELOAD_CATEGORIES = None


class AgentCapabilityType(BaseEnum):
    AGENT_COMMAND = 'agent_command'
    AGENT_PARAMETER = 'agent_parameter'
    RESOURCE_COMMAND = 'resource_command'
    RESOURCE_INTERFACE = 'resource_interface'
    RESOURCE_PARAMETER = 'resource_parameter'


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
    instrument_device_name = None

    # If set load the driver from this repo instead of the egg
    mi_repo = None
    data_dir    = "/tmp/dsatest%s" % os.getpid()
    test_resource_dir = None
    preload_scenario = None
    stream_name = None
    exchange_name = 'science_data'


    def initialize(self, *args, **kwargs):

        log.debug("initialize with %s", kwargs)

        self.data_dir   = kwargs.get('data_dir', self.data_dir)

        self.instrument_device_name = kwargs.get('instrument_device_name', self.instrument_device_name)

        self.preload_scenario = kwargs.get('preload_scenario', self.preload_scenario)

        self.test_resource_dir = kwargs.get('test_resource_dir', os.path.join(self.test_base_dir(), 'resource'))

        self.mi_repo = kwargs.get('mi_repo', self.mi_repo)
        self.stream_name = kwargs.get('stream_name', self.stream_name)
        self.exchange_name = kwargs.get('exchange_name', self.exchange_name)


    def verify(self):
        """
        TODO: Add more config verification code
        """
        if not self.instrument_device_name:
            raise ConfigNotFound("missing instrument_device_name")

        if not self.test_resource_dir:
            raise ConfigNotFound("missing test_resource_dir")

        if not self.stream_name:
            raise ConfigNotFound("missing stream_name")


    def test_base_dir(self):
        """
        Determine the base directory for the test, this is used to figure out
        where the test resource files are located.
        """
        return os.path.dirname(__file__)

@unittest.skipIf(( (os.getenv('PYCC_MODE', False) is False) and (os.getenv('CEI_LAUNCH_TEST', False) is not False)), 'Skip tests during launch without NFS shared directories.')
class DatasetAgentTestCase(IonIntegrationTestCase):
    """
    Base class for all dataset agent end to end tests
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
        self.dams = DataAcquisitionManagementServiceClient()

        # Ensure we have a good test configuration
        self.test_config.verify()

        # Start container.
        log.info('Staring capability container.')
        self._start_container()
        self.rr = self.container.resource_registry

        # Bring up services in a deploy file (no need to message)
        log.info('Starting deploy services. %s', deploy_file)
        self.container.start_rel_from_url(DEPLOY_FILE)

        # Load instrument specific parameters
        log.info('Preload test scenarios')
        self._load_params()

        # Start a resource agent client to talk with the instrument agent.
        log.info('Starting DSA process')
        self._dsa_client = self._start_dataset_agent_process()
        log.debug("Client created: %s", type(self._dsa_client))
        self.addCleanup(self._stop_dataset_agent_process)
        log.info('test setup complete')

        # Start data subscribers
        self._start_data_subscribers()
        self.addCleanup(self._stop_data_subscribers)

    ###
    #   Test/Agent Startup Helpers
    ###
    def _load_params(self):
        """
        Do a second round of preload with instrument specific scenarios
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
            scenario = "%s,%s" % (scenario, self.test_config.preload_scenario) if scenario else self.test_config.preload_scenario
        else:
            log.warn("No DSA specific preload defined.  Was this intentional?")

        if scenario:
            preload_config=dict(
                op="load",
                scenario=scenario,
                #path="master",
                path=TESTED_DOC,
                categories=categories,
                clearcols="owner_id,org_ids",
                #assets="res/preload/r2_ioc/ooi_assets",
                #parseooi="True",
            )
            log.debug("Starting preload now: config=%s", preload_config)
            self.container.spawn_process("Loader", "ion.processes.bootstrap.ion_loader", "IONLoader", preload_config)

    def _start_dataset_agent_process(self):
        """
        Launch the agent process and store the configuration.  Tried
        to emulate the same process used by import_data.py
        """
        instrument_device, dsa_instance = self._get_dsa_instance()
        self._driver_config = dsa_instance.driver_config

        self._update_dsa_config(dsa_instance)
        self._update_harvester_config(dsa_instance)

        self._dsa_instance = dsa_instance
        self.clear_sample_data()

        # Return a resource agent client
        return self._get_dsa_client(instrument_device, dsa_instance)

    def _stop_dataset_agent_process(self):
        """
        Stop the dataset agent instance
        """
        self.assert_reset()
        self.dams.stop_external_dataset_agent_instance(self._dsa_instance._id)

    def _get_dsa_instance(self):
        """
        Find the dsa instance in preload and return an instance of that object
        """
        name = self.test_config.instrument_device_name

        log.debug("Start dataset agent process for instrument device: %s", name)
        objects,_ = self.rr.find_resources(RT.InstrumentDevice, name=name)
        log.debug("Found Instrument Devices: %s", objects)
        if not objects:
            raise ConfigNotFound("No appropriate InstrumentDevice objects loaded")

        instrument_device = objects[0]
        log.trace("Found instrument device: %s", instrument_device)

        dsa_instance = self.rr.read_object(subject=instrument_device._id,
                                     predicate=PRED.hasAgentInstance,
                                     object_type=RT.ExternalDatasetAgentInstance)

        log.info("dsa_instance found: %s", dsa_instance)

        return instrument_device, dsa_instance

    def _update_harvester_config(self, dsa_instance):
        """
        Update the harvester config such that we change the directory to something
        we have write permissions.
        """
        log.info("dsa agent instance: %s", dsa_instance)
        driver_config = dsa_instance.driver_config

        log.info("dsa agent driver config: %s", driver_config)
        driver_config['startup_config']['harvester']['directory'] = self.test_config.data_dir

        log.info("updated driver config: %s", driver_config)
        dsa_instance.driver_config = driver_config

        self.rr.update(dsa_instance)

    def _update_dsa_config(self, dsa_instance):
        """
        Update the dsa configuration prior to loading the agent.  This is where we can
        alter production configurations for use in a controlled test environment.
        """
        dsa_obj = self.rr.read_object(
            object_type=RT.ExternalDatasetAgent, predicate=PRED.hasAgentDefinition, subject=dsa_instance._id, id_only=False)

        log.info("dsa agent definition found: %s", dsa_obj)

        # If we don't want to load from an egg then we need to
        # alter the driver config read from preload
        if self.test_config.mi_repo is not None:
            dsa_obj.driver_uri = None
            # Strip the custom namespace
            dsa_obj.driver_module = ".".join(dsa_obj.driver_module.split('.')[1:])

            log.info("saving new dsa agent config: %s", dsa_obj)
            self.rr.update(dsa_obj)

            if not self.test_config.mi_repo in sys.path: sys.path.insert(0, self.test_config.mi_repo)

            log.debug("Driver module: %s", dsa_obj.driver_module)
            log.debug("MI Repo: %s", self.test_config.mi_repo)
            log.trace("Sys Path: %s", sys.path)

    def _get_dsa_client(self, instrument_device, dsa_instance):
        """
        Launch the agent and return a client
        """
        fake_process = FakeProcess()
        fake_process.container = self.container

        clients = DataAcquisitionManagementServiceDependentClients(fake_process)
        config_builder = ExternalDatasetAgentConfigurationBuilder(clients)

        try:
            config_builder.set_agent_instance_object(dsa_instance)
            self.agent_config = config_builder.prepare()
        except Exception as e:
            log.error('failed to launch: %s', e, exc_info=True)
            raise ServerError('failed to launch')

        self._dsa_pid = self.dams.start_external_dataset_agent_instance(dsa_instance._id)
        log.debug("_get_dsa_client CFG")
        return ResourceAgentClient(instrument_device._id, process=FakeProcess())

        dispatcher = ProcessDispatcherServiceClient()
        launcher = AgentLauncher(dispatcher)

        log.debug("Launching agent process!")

        self._dsa_pid = launcher.launch(self.agent_config, config_builder._get_process_definition()._id)
        if not self._dsa_pid:
            raise ServerError("Launched external dataset agent instance but no process_id")
        config_builder.record_launch_parameters(self.agent_config)

        launcher.await_launch(10.0)
        return ResourceAgentClient(instrument_device._id, process=FakeProcess())

    def _get_dsa_object_state(self):
        state, _id = self.container.state_repository.get_state(self._dsa_pid)
        log.debug("agent_state (%s): %s", self._dsa_pid, state)

        driver_state = state.get(DSA_STATE_KEY)
        log.debug("driver_state (%s): %s", self._dsa_pid, driver_state)

        return driver_state

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

        return dest_path

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
            origin="IA_RESOURCE_ID") #TODO
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
    def _start_data_subscribers(self):
        # A callback for processing subscribed-to data.
        def recv_data(message, stream_route, stream_id):
            if self._samples_received.get(stream_id) is None:
                self._samples_received[stream_id] = []

            log.info('Received parsed data on %s (%s,%s)', stream_id, stream_route.exchange_point, stream_route.routing_key)
            self._samples_received[stream_id].append(message)

        # Create streams and subscriptions for each stream named in driver.
        self._data_subscribers = []
        self._samples_received = {}
        self._stream_id_map = {}
        stream_config = self.agent_config['stream_config']

        log.info("starting data subscribers")

        for stream_name in stream_config.keys():
            log.debug("Starting data subscriber for stream '%s'", stream_name)
            stream_id = stream_config[stream_name]['stream_id']
            self._stream_id_map[stream_name] = stream_id
            self._start_data_subscriber(stream_config[stream_name], recv_data)

    def _start_data_subscriber(self, config, callback):
        """
        Setup and start a data subscriber
        """
        exchange_point = config['exchange_point']
        stream_id = config['stream_id']

        sub = StandaloneStreamSubscriber(exchange_point, callback)
        sub.start()
        self._data_subscribers.append(sub)

        pubsub_client = PubsubManagementServiceClient(node=self.container.node)
        sub_id = pubsub_client.create_subscription(name=exchange_point, stream_ids=[stream_id])
        pubsub_client.activate_subscription(sub_id)
        sub.subscription_id = sub_id # Bind the subscription to the standalone subscriber (easier cleanup, not good in real practice)

    def make_data_product(self, pdict_name, dp_name, available_fields=None):
        self.pubsub_management = PubsubManagementServiceClient()
        if available_fields is None: available_fields = []
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name(pdict_name, id_only=True)
        stream_def_id = self.pubsub_management.create_stream_definition('%s stream_def' % dp_name, parameter_dictionary_id=pdict_id, available_fields=available_fields or None)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)
        dp_obj = DataProduct(name=dp_name)
        data_product_id = self.data_product_management.create_data_product(dp_obj, stream_definition_id=stream_def_id)
        self.addCleanup(self.data_product_management.delete_data_product, data_product_id)
        return data_product_id

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

    def get_samples(self, stream_name, sample_count=1, timeout=30):
        """
        listen on a stream until 'sample_count' samples are read and return
        a list of all samples read.  If the required number of samples aren't
        read then throw an exception.

        Note that this method does not clear the sample queue for the stream.
        This should be done explicitly by the caller.  However, samples that
        are consumed by this method are removed.

        @raise SampleTimeout - if the required number of samples aren't read
        """
        to = gevent.Timeout(timeout)
        to.start()
        done = False
        result = []
        i = 0

        log.debug("Fetch %s sample(s) from stream '%s'" % (sample_count, stream_name))

        stream_id = self._stream_id_map.get(stream_name)
        log.debug("Stream ID Map: %s ", self._stream_id_map)
        self.assertIsNotNone(stream_id, msg="Unable to find stream name '%s'" % stream_name)

        try:
            while(not done):
                if (self._samples_received.has_key(stream_id) and
                   len(self._samples_received.get(stream_id))):
                    log.trace("get_samples() received sample #%d!", i)
                    result.append(self._samples_received[stream_id].pop(0))
                    i += 1

                    if i >= sample_count:
                        done = True

                else:
                    log.debug("No samples in %s. Sleep a bit to wait for the data queue to fill up.", stream_name)
                    gevent.sleep(1)

        except Timeout:
            log.error("Failed to get %d records from %s.  received: %d", sample_count, stream_name, i)
            self.fail("Failed to read samples from stream %s", stream_name)
        finally:
            to.cancel()
            return result

    def remove_sample_dir(self):
        """
        Remove the sample dir and all files
        """
        data_dir = self.create_data_dir()
        self.clear_sample_data()
        os.rmdir(data_dir)

    ###
    #   Common assert methods
    ###

    def assertDictEqual(self, d1, d2, msg=None): # assertEqual uses for dicts
        for k,v1 in d1.iteritems():
            self.assertIn(k, d2, msg)
            v2 = d2[k]
            if(isinstance(v1, collections.Iterable) and
               not isinstance(v1, basestring)):
                self.assertItemsEqual(v1, v2, msg)
            else:
                self.assertEqual(v1, v2, msg)
        return True

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

    def assert_data_values(self, granules, dataset_definition_file):
        """
        Verify granules match the granules defined in the definition file
        """
        rs_file = self._get_source_data_file(dataset_definition_file)
        rs = ResultSet(rs_file)

        self.assertTrue(rs.verify(granules), msg="Failed data validation.  See log for details")

    def assert_sample_queue_size(self, stream_name, size):
        """
        verify a sample queue is the size we expect it to be.
        """
        # Sleep a couple seconds to ensure the
        gevent.sleep(2)

        stream_id = self._stream_id_map.get(stream_name)
        length = 0
        if stream_id in self._samples_received:
            length = len(self._samples_received[stream_id])
        self.assertEqual(length, size, msg="Queue size != expected size (%d != %d)" % (length, size))

    def assert_set_pubrate(self, rate):
        """
        Set the pubrate for the parsed data stream.  Set to 0 for
        no buffering
        """
        self.assertIsInstance(rate, (int, float))
        self.assertGreaterEqual(rate, 0)

        expected_pubrate = {self.test_config.stream_name: rate}

        retval = self._dsa_client.set_agent({'pubrate': expected_pubrate})

        retval = self._dsa_client.get_agent(['pubrate'])
        expected_pubrate_result = {'pubrate': expected_pubrate}
        self.assertEqual(retval, expected_pubrate_result)

    def assert_agent_command(self, command, args=None, timeout=None):
        """
        Verify an agent command
        @param command: driver command to execute
        @param args: kwargs to pass to the agent command object
        """
        cmd = AgentCommand(command=command, kwargs=args)
        retval = self._dsa_client.execute_agent(cmd, timeout=timeout)

    def assert_resource_command(self, command, args=None, timeout=None):
        """
        Verify a resource command
        @param command: driver command to execute
        @param args: kwargs to pass to the agent command object
        """
        cmd = AgentCommand(command=command, kwargs=args)
        retval = self._dsa_client.execute_resource(cmd)

    def assert_schema(self, caps_list):

        dd_list = ['display_name','description']
        ddt_list = ['display_name','description','type']
        ddvt_list = ['display_name','description','visibility','type']
        ddak_list = ['display_name','description','args','kwargs']

        for x in caps_list:
            if isinstance(x,dict):
                x.pop('type_')
                x = IonObject('AgentCapability', **x)

            if x.cap_type == CapabilityType.AGT_CMD:
                keys = x.schema.keys()
                for y in ddak_list:
                    self.assertIn(y, keys)

            elif x.cap_type == CapabilityType.AGT_PAR:
                    if x.name != 'example':
                        keys = x.schema.keys()
                        for y in ddvt_list:
                            self.assertIn(y, keys)

            elif x.cap_type == CapabilityType.RES_CMD:
                keys = x.schema.keys()
                self.assertIn('return',keys)
                self.assertIn('display_name',keys)
                self.assertIn('arguments',keys)
                self.assertIn('timeout',keys)

            elif x.cap_type == CapabilityType.RES_IFACE:
                pass

            elif x.cap_type == CapabilityType.RES_PAR:
                keys = x.schema.keys()
                self.assertIn('get_timeout',keys)
                self.assertIn('set_timeout',keys)
                self.assertIn('direct_access',keys)
                self.assertIn('startup',keys)
                self.assertIn('visibility',keys)

            elif x.cap_type == CapabilityType.AGT_STATES:
                for (k,v) in x.schema.iteritems():
                    keys = v.keys()
                    for y in dd_list:
                        self.assertIn(y, keys)

            elif x.cap_type == CapabilityType.ALERT_DEFS:
                for (k,v) in x.schema.iteritems():
                    keys = v.keys()
                    for y in ddt_list:
                        self.assertIn(y, keys)

            elif x.cap_type == CapabilityType.AGT_CMD_ARGS:
                for (k,v) in x.schema.iteritems():
                    keys = v.keys()
                    for y in ddt_list:
                        self.assertIn(y, keys)

    def assert_agent_capabilities(self):
        """
        Verify capabilities throughout the agent lifecycle
        """
        capabilities = {
            AgentCapabilityType.AGENT_COMMAND: self._common_agent_commands(ResourceAgentState.UNINITIALIZED),
            AgentCapabilityType.AGENT_PARAMETER: self._common_agent_parameters(),
            AgentCapabilityType.RESOURCE_COMMAND: None,
            AgentCapabilityType.RESOURCE_INTERFACE: None,
            AgentCapabilityType.RESOURCE_PARAMETER: None,
        }

        ###
        # DSA State INACTIVE
        ###

        log.debug("Initialize DataSet agent")
        self.assert_agent_command(ResourceAgentEvent.INITIALIZE)
        self.assert_state_change(ResourceAgentState.INACTIVE)
        self.assert_capabilities(capabilities)

        ###
        # DSA State IDLE
        ###

        log.debug("DataSet agent go active")
        capabilities[AgentCapabilityType.AGENT_COMMAND] = self._common_agent_commands(ResourceAgentState.IDLE)
        self.assert_agent_command(ResourceAgentEvent.GO_ACTIVE)
        self.assert_state_change(ResourceAgentState.IDLE)
        self.assert_capabilities(capabilities)

        ###
        # DSA State COMMAND
        ###

        log.debug("DataSet agent run")
        capabilities[AgentCapabilityType.AGENT_COMMAND] = self._common_agent_commands(ResourceAgentState.COMMAND)
        capabilities[AgentCapabilityType.RESOURCE_COMMAND] = ['DRIVER_EVENT_START_AUTOSAMPLE']
        capabilities[AgentCapabilityType.RESOURCE_PARAMETER] = self._common_resource_parameters()
        self.assert_agent_command(ResourceAgentEvent.RUN)
        self.assert_state_change(ResourceAgentState.COMMAND)
        self.assert_capabilities(capabilities)


        ###
        # DSA State STREAMING
        ###
        capabilities[AgentCapabilityType.AGENT_COMMAND] = self._common_agent_commands(ResourceAgentState.STREAMING)
        capabilities[AgentCapabilityType.RESOURCE_COMMAND] = ['DRIVER_EVENT_STOP_AUTOSAMPLE']
        capabilities[AgentCapabilityType.RESOURCE_PARAMETER] = self._common_resource_parameters()
        self.assert_start_sampling()
        self.assert_capabilities(capabilities)


        ###
        # DSA State LOST_CONNECTION
        ###
        capabilities[AgentCapabilityType.AGENT_COMMAND] = self._common_agent_commands(ResourceAgentState.LOST_CONNECTION)
        capabilities[AgentCapabilityType.RESOURCE_COMMAND] = None
        capabilities[AgentCapabilityType.RESOURCE_PARAMETER] = None
        self.assert_agent_command(ResourceAgentEvent.RESET)
        self.assert_state_change(ResourceAgentState.UNINITIALIZED)

        self.remove_sample_dir()
        self.assert_initialize(final_state=ResourceAgentState.COMMAND)
        self.assert_resource_command('DRIVER_EVENT_START_AUTOSAMPLE')
        self.assert_state_change(ResourceAgentState.LOST_CONNECTION, 90)

    def assert_driver_state(self, expected_state=None):
        '''
        verify that expected persisted agent state matches was it actually stored
        @param expected_state dict expected
        '''
        state = self._get_dsa_object_state()

        if expected_state is None:
            self.assertIsNone(expected_state)
        else:
            self.assertEqual(expected_state, state)

    def assert_agent_state_after_restart(self):
        '''
        Restart the agent.  Verify that the agent PID changes. Then verify the new state
        matches the old state.
        '''
        old_pid = self._dsa_pid
        old_state = self._get_dsa_object_state()

        # Start a resource agent client to talk with the instrument agent.
        log.info('Restarting DSA process')
        self._stop_dataset_agent_process()
        self._dsa_client = self._start_dataset_agent_process()
        log.debug("Client created: %s", type(self._dsa_client))
        self.addCleanup(self.assert_reset)

        self.assert_initialize()

        self.assertNotEqual(old_pid, self._dsa_pid)
        self.assertEqual(old_state, self._get_dsa_object_state())

        # Kick it into autosample and give it time for samples to come in, there shouldn't be any
        gevent.sleep(5)

    def assert_capabilities(self, capabilities):
        '''
        Verify that all capabilities are available for a give state

        @todo: Currently resource interface not implemented because it requires
               a submodule update and some of the submodules are in release
               states.  So for now, no resource interfaces

        @param: dictionary of all the different capability types that are
        supposed to be there. i.e.
        {
          agent_command = ['DO_MY_COMMAND'],
          agent_parameter = ['foo'],
          resource_command = None,
          resource_interface = None,
          resource_parameter = None,
        }
        '''
        def sort_capabilities(caps_list):
            '''
            sort a return value into capability buckets.
            @retval agt_cmds, agt_pars, res_cmds, res_iface, res_pars
            '''
            agt_cmds = []
            agt_pars = []
            res_cmds = []
            res_iface = []
            res_pars = []

            if len(caps_list)>0 and isinstance(caps_list[0], AgentCapability):
                agt_cmds = [x.name for x in caps_list if x.cap_type==CapabilityType.AGT_CMD]
                agt_pars = [x.name for x in caps_list if x.cap_type==CapabilityType.AGT_PAR]
                res_cmds = [x.name for x in caps_list if x.cap_type==CapabilityType.RES_CMD]
                #res_iface = [x.name for x in caps_list if x.cap_type==CapabilityType.RES_IFACE]
                res_pars = [x.name for x in caps_list if x.cap_type==CapabilityType.RES_PAR]

            elif len(caps_list)>0 and isinstance(caps_list[0], dict):
                agt_cmds = [x['name'] for x in caps_list if x['cap_type']==CapabilityType.AGT_CMD]
                agt_pars = [x['name'] for x in caps_list if x['cap_type']==CapabilityType.AGT_PAR]
                res_cmds = [x['name'] for x in caps_list if x['cap_type']==CapabilityType.RES_CMD]
                #res_iface = [x['name'] for x in caps_list if x['cap_type']==CapabilityType.RES_IFACE]
                res_pars = [x['name'] for x in caps_list if x['cap_type']==CapabilityType.RES_PAR]

            agt_cmds.sort()
            agt_pars.sort()
            res_cmds.sort()
            res_iface.sort()
            res_pars.sort()

            return agt_cmds, agt_pars, res_cmds, res_iface, res_pars

        if not capabilities.get(AgentCapabilityType.AGENT_COMMAND):
            capabilities[AgentCapabilityType.AGENT_COMMAND] = []
        if not capabilities.get(AgentCapabilityType.AGENT_PARAMETER):
            capabilities[AgentCapabilityType.AGENT_PARAMETER] = []
        if not capabilities.get(AgentCapabilityType.RESOURCE_COMMAND):
            capabilities[AgentCapabilityType.RESOURCE_COMMAND] = []
        if not capabilities.get(AgentCapabilityType.RESOURCE_INTERFACE):
            capabilities[AgentCapabilityType.RESOURCE_INTERFACE] = []
        if not capabilities.get(AgentCapabilityType.RESOURCE_PARAMETER):
            capabilities[AgentCapabilityType.RESOURCE_PARAMETER] = []


        expected_agent_cmd = capabilities.get(AgentCapabilityType.AGENT_COMMAND)
        expected_agent_cmd.sort()
        expected_agent_param = self._common_agent_parameters()
        expected_agent_param.sort()
        expected_res_cmd = capabilities.get(AgentCapabilityType.RESOURCE_COMMAND)
        expected_res_cmd.sort()
        expected_res_param = capabilities.get(AgentCapabilityType.RESOURCE_PARAMETER)
        expected_res_param.sort()
        expected_res_int = capabilities.get(AgentCapabilityType.RESOURCE_INTERFACE)
        expected_res_int.sort()

        # go get the active capabilities
        retval = self._dsa_client.get_capabilities()
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_capabilities(retval)
        self.assert_schema(retval)

        log.debug("Agent Commands: %s ", str(agt_cmds))
        log.debug("Compared to: %s", expected_agent_cmd)
        log.debug("Agent Parameters: %s ", str(agt_pars))
        log.debug("Compared to: %s", expected_agent_param)
        log.debug("Resource Commands: %s ", str(res_cmds))
        log.debug("Compared to: %s", expected_res_cmd)
        log.debug("Resource Interface: %s ", str(res_iface))
        log.debug("Compared to: %s", expected_res_int)
        log.debug("Resource Parameter: %s ", str(res_pars))
        log.debug("Compared to: %s", expected_res_param)

        # Compare to what we are supposed to have
        self.assertEqual(expected_agent_cmd, agt_cmds)
        self.assertEqual(expected_agent_param, agt_pars)
        self.assertEqual(expected_res_cmd, res_cmds)
        self.assertEqual(expected_res_int, res_iface)
        self.assertEqual(expected_res_param, res_pars)

    def _common_resource_parameters(self):
        '''
        list of common resource parameters
        @return: list of resource parameters
        '''
        return ['batched_particle_count', 'publisher_polling_interval', 'records_per_second']

    def _common_agent_parameters(self):
        '''
        list of common agent parameters
        @return: list of agent parameters
        '''
        return ['aggstatus', 'alerts', 'driver_name', 'driver_pid', 'example', 'pubrate', 'streams']

    def _common_agent_commands(self, agent_state):
        '''
        list of common agent parameters for a agent state
        @return: list of agent parameters
        @raise: KeyError for undefined agent state
        '''
        capabilities = {
            ResourceAgentState.UNINITIALIZED: [
                ResourceAgentEvent.GO_ACTIVE,
                ResourceAgentEvent.RESET,
            ],
            ResourceAgentState.IDLE: [
                ResourceAgentEvent.GO_INACTIVE,
                ResourceAgentEvent.RESET,
                ResourceAgentEvent.RUN,
            ],
            ResourceAgentState.COMMAND: [
                ResourceAgentEvent.CLEAR,
                ResourceAgentEvent.RESET,
                ResourceAgentEvent.GO_INACTIVE,
                ResourceAgentEvent.PAUSE
            ],
            ResourceAgentState.STREAMING: [
                ResourceAgentEvent.RESET,
                ResourceAgentEvent.GO_INACTIVE
            ],

            ResourceAgentState.LOST_CONNECTION: [
                ResourceAgentEvent.RESET,
                ResourceAgentEvent.GO_INACTIVE
            ]
        }

        return capabilities[agent_state]

    def assert_state_change(self, target_agent_state, timeout=10):
        """
        Verify the agent and resource states change as expected within the timeout
        Fail if the state doesn't change to the expected state.
        @param target_agent_state: State we expect the agent to be in
        @param timeout: how long to wait for the driver to change states
        """
        to = gevent.Timeout(timeout)
        to.start()
        done = False
        agent_state = None

        try:
            while(not done):

                agent_state = self._dsa_client.get_agent_state()
                log.error("Current agent state: %s", agent_state)

                if(agent_state == target_agent_state):
                    log.debug("Current state match: %s", agent_state)
                    done = True

                if not done:
                    log.debug("state mismatch, waiting for state to transition.")
                    gevent.sleep(1)
        except Timeout:
            log.error("Failed to transition agent state to %s, current state: %s", target_agent_state, agent_state)
            self.fail("Failed to transition state.")
        finally:
            to.cancel()

