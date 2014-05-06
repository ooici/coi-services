#!/usr/bin/env python

"""
@package ion.agents.instrument.test.test_high_volume
@file ion/agents.instrument/test_high_volume.py
@author Bill French
@brief Test cases for high volume agents
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import simplejson, urllib, os, unittest, gevent
from mock import patch
import sys
import time
import re
from gevent import Timeout
import numpy as np

from gevent.event import AsyncResult

from pyon.public import log, CFG
from nose.plugins.attrib import attr

import pyon.core.exception as pyex
from pyon.public import RT, PRED
from pyon.core.bootstrap import IonObject
from pyon.core.object import IonObjectSerializer
from pyon.agent.agent import ResourceAgentClient
from pyon.ion.exchange import ExchangeManager
from ion.services.dm.utility.granule_utils import time_series_domain
from pyon.util.int_test import IonIntegrationTestCase
from ion.agents.instrument.test.test_instrument_agent import InstrumentAgentTestMixin
from ion.agents.instrument.test.test_instrument_agent import start_instrument_agent_process
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from ion.services.dm.utility.granule_utils import time_series_domain
from ion.services.dm.utility.granule_utils import RecordDictionaryTool
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import  DataProductManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

from interface.objects import AgentCommand
from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent


# bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_high_volume.py:TestInstrumentAgentHighVolume.test_autosample

DVR_CONFIG = {
    'dvr_egg' : 'http://sddevrepo.oceanobservatories.org/releases/ooici_mi_test_driver-0.0.1-py2.7.egg',
    'dvr_mod' : 'mi.instrument.ooici.mi.test_driver.driver',
    'dvr_cls' : 'InstrumentDriver',
    'workdir' : CFG.device.sbe37.workdir,
    'process_type' : None,

    # These values are ignored, but must be defined
    'comms_config' : {
            'addr' : 'localhost',
            'port' : 8080,
            'cmd_port' : 8181
    }
}

LAUNCH_FROM_EGG=True
if LAUNCH_FROM_EGG:
    from ion.agents.instrument.test.load_test_driver_egg import load_egg
    DVR_CONFIG = load_egg(DVR_CONFIG)

else:
    #mi_repo = '/path/to/your/local/mi/repo'
    from ion.agents.instrument.test.load_test_driver_egg import load_repo
    DVR_CONFIG = load_repo(mi_repo, DVR_CONFIG)
    log.info("adding repo to syspath: %s", sys.path)

# Load MI modules from the egg
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverConnectionState
#from mi.instrument.ooici.mi.test_driver.driver import ProtocolEvent
#from mi.instrument.ooici.mi.test_driver.driver import ParameterName

PAYLOAD_SIZE = 'PAYLOAD_SIZE'
SAMPLE_INTERVAL = 'SAMPLE_INTERVAL'

@attr('HARDWARE', group='mi')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 120}}})
class TestInstrumentAgentHighVolume(IonIntegrationTestCase, InstrumentAgentTestMixin):
    """
    Test cases for pumping high volume data through the agent and ingested.  Initially we are just testing raw data, but
    eventually we will update to publish data similar to the ORB
    """
    def setUp(self):
        """
        Set up driver integration support.
        Start container.
        Start deploy services.
        Define agent config, start agent.
        Start agent client.
        """

        log.info("DVR_CONFIG=%s", DVR_CONFIG)

        self._ia_client = None

        # Start container.
        log.info('Staring capability container.')
        self._start_container()

        # Bring up services in a deploy file (no need to message)
        log.info('Staring deploy services.')
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        log.info('building stream configuration')
        # Setup stream config.
        self._build_stream_config()

        # Start a resource agent client to talk with the instrument agent.
        log.info('starting IA process')
        self._ia_client = start_instrument_agent_process(self.container, self._stream_config, dvr_config=DVR_CONFIG)
        self.addCleanup(self._verify_agent_reset)
        log.info('test setup complete')

    def assert_initialize(self):
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

    def assert_start_autosample(self):
        cmd = AgentCommand(command=DriverEvent.START_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)

    def assert_stop_autosample(self):
        cmd = AgentCommand(command=DriverEvent.STOP_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)

    def assert_reset(self):
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)

    def assert_set_parameter(self, name, value, verify=True):
        '''
        verify that parameters are set correctly.  Assumes we are in command mode.
        '''
        setParams = { name : value }
        getParams = [ name ]

        self._ia_client.set_resource(setParams)

        if(verify):
            result = self._ia_client.get_resource(getParams)
            self.assertEqual(result[name], value)

    def _get_raw_queue(self):
        queue_names = self.container.ex_manager.list_queues(self._raw_exchange_name)
        log.info("Rabbit Queues: %s", queue_names)

        self.assertEqual(len(queue_names), 1)
        return queue_names[0]

    def _monitor_queue_size(self, queue_name, size):
        """
        Verify a queue doesn't exceed a specific backlog
        """
        while self._queue_size_monitor_stop == False:
            queue_info = self.container.ex_manager.get_queue_info(queue_name)
            log.info("Queue Info: %s", queue_info)
            gevent.sleep(1)

    def _start_queue_size_monitor(self, queue_name, size):
        log.debug("Start Queue Monitor: %s", queue_name)
        self._async_queue_size_monitor = AsyncResult()
        self._queue_size_monitor_thread = gevent.Greenlet(self._monitor_queue_size, queue_name, size)
        self._queue_size_monitor_stop = False
        self._queue_size_monitor_thread.start()

    def _stop_queue_size_monitor(self):
        if self._queue_size_monitor_thread:
            self._queue_size_monitor_stop = True
            self._queue_size_monitor_thread.join(timeout=60)

        self._queue_size_monitor_thread = False

    def _start_raw_ingestion(self):
        dpsc_cli = DataProductManagementServiceClient()
        rrclient = ResourceRegistryServiceClient()
        RR2 = EnhancedResourceRegistryClient(rrclient)


        dp_obj = IonObject(RT.DataProduct,
            name='DP1',
            description='some new dp')

        dp_obj.geospatial_bounds.geospatial_latitude_limit_north = 10.0
        dp_obj.geospatial_bounds.geospatial_latitude_limit_south = -10.0
        dp_obj.geospatial_bounds.geospatial_longitude_limit_east = 10.0
        dp_obj.geospatial_bounds.geospatial_longitude_limit_west = -10.0
        dp_obj.ooi_product_name = "PRODNAME"

        #------------------------------------------------------------------------------------------------
        # Create a set of ParameterContext objects to define the parameters in the coverage, add each to the ParameterDictionary
        #------------------------------------------------------------------------------------------------

        log.info("Create data product... raw stream id: %s", self._raw_stream_id)
        dp_id = dpsc_cli.create_data_product_(data_product= dp_obj)
        dataset_id = self.create_dataset(self._raw_stream_pdict_id)
        RR2.assign_stream_definition_to_data_product_with_has_stream_definition(self._raw_stream_def_id, dp_id)
        RR2.assign_stream_to_data_product_with_has_stream(self._raw_stream_id, dp_id)
        RR2.assign_dataset_to_data_product_with_has_dataset(dataset_id, dp_id)
        self._raw_dataset_id = dataset_id

        log.info("Create data product...Complete")

        # Assert that the data product has an associated stream at this stage
        stream_ids, _ = rrclient.find_objects(dp_id, PRED.hasStream, RT.Stream, True)
        self.assertNotEquals(len(stream_ids), 0)

        # Assert that the data product has an associated stream def at this stage
        stream_ids, _ = rrclient.find_objects(dp_id, PRED.hasStreamDefinition, RT.StreamDefinition, True)
        self.assertNotEquals(len(stream_ids), 0)

        log.info("Activate data product persistence")
        dpsc_cli.activate_data_product_persistence(dp_id)

        log.info("Read data product")
        dp_obj = dpsc_cli.read_data_product(dp_id)
        self.assertIsNotNone(dp_obj)
        self.assertEquals(dp_obj.geospatial_point_center.lat, 0.0)
        log.debug('Created data product %s', dp_obj)

    def create_dataset(self, parameter_dict_id=''):
        '''
        Creates a time-series dataset
        '''
        dataset_management = DatasetManagementServiceClient()
        if not parameter_dict_id:
            parameter_dict_id = dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)

        dataset_id = dataset_management.create_dataset('test_dataset_', parameter_dictionary_id=parameter_dict_id)
        self.addCleanup(dataset_management.delete_dataset, dataset_id)
        return dataset_id

    def assert_raw_granules_ingested(self, count, payload_size):
        #--------------------------------------------------------------------------------
        # Test the slicing capabilities
        #--------------------------------------------------------------------------------
        data_retriever = DataRetrieverServiceClient()

        for i in range(0, count-1):
            granule = data_retriever.retrieve(dataset_id=self._raw_dataset_id, query={'tdoa':slice(i,i+1)})
            rdt = RecordDictionaryTool.load_from_granule(granule)

            log.info("Granule index: %d, time: %s, size: %s", i, rdt['time'][0], len(rdt['raw'][0]))
            self.assertEqual(payload_size, len(rdt['raw'][0]))

    def test_autosample(self):
        """
        Start up the test instrument and sample at 1Hz.  Verify that we product n-1 packets in the duration specified
        and verify the payload is the size we expect.
        """
        duration = 100
        payload_size = 1024
        sample_rate = 1
        sample_count = duration / sample_rate
        timeout = duration * 5

        # Start data subscribers.
        self._start_data_subscribers(3, sample_count)
        self.addCleanup(self._stop_data_subscribers)

        self._start_raw_ingestion()

        # Get the raw queue name
        raw_queue_name = self._get_raw_queue()
        self.assertIsNotNone(raw_queue_name)

        # Start queue monitor.
        #self._start_queue_size_monitor(raw_queue_name, 3)
        #self.addCleanup(self._stop_queue_size_monitor)

        self.assert_initialize()

        self.assert_set_parameter(PAYLOAD_SIZE, payload_size)

        self.assert_start_autosample()

        gevent.sleep(duration)

        self.assert_stop_autosample()

        gevent.sleep(2)

        samples_rec = len(self._raw_samples_received)
        self.assertLessEqual(sample_count - samples_rec, 1)

        self.assert_raw_granules_ingested(len(self._raw_samples_received), payload_size)
        self.assert_reset()


