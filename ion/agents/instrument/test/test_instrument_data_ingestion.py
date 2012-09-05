#!/usr/bin/env python

"""
@package ion.agents.instrument.test.test_instrument_data_ingestion
@file    ion/agents/instrument/test/test_instrument_data_ingestion.py
@author  Carlos Rueda
@brief   Test cases for data ingestion from an instrument. It is basically the
         test_poll from test_instrument_agent (as of 7/23/12) with
         the additional preparation of the ingestion management service and the
         verification that the published granules are persisted.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from pyon.public import log
from pyon.public import RT
from pyon.public import PRED

# 3rd party imports.
from gevent import spawn
from gevent.event import AsyncResult
import gevent
from nose.plugins.attrib import attr
from mock import patch
import unittest

# ION imports.
from interface.objects import StreamQuery
from interface.objects import DataSet
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient

from pyon.public import StreamSubscriberRegistrar
from prototype.sci_data.stream_defs import ctd_stream_definition
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from pyon.public import CFG
from pyon.event.event import EventSubscriber, EventPublisher
from pyon.core.exception import InstParameterError

from ion.agents.instrument.taxy_factory import get_taxonomy
from ion.agents.instrument.driver_int_test_support import DriverIntegrationTestSupport
from ion.agents.instrument.instrument_agent import InstrumentAgentState

# MI imports.
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Parameter
from mi.instrument.seabird.sbe37smb.ooicore.driver import PACKET_CONFIG


DEV_ADDR = CFG.device.sbe37.host
DEV_PORT = CFG.device.sbe37.port

DRV_MOD = 'mi.instrument.seabird.sbe37smb.ooicore.driver'
DRV_CLS = 'SBE37Driver'

# Work dir and logger delimiter.
WORK_DIR = '/tmp/'
DELIM = ['<<','>>']

# Driver config.
# DVR_CONFIG['comms_config']['port'] is set by the setup.
DVR_CONFIG = {
    'dvr_mod' : DRV_MOD,
    'dvr_cls' : DRV_CLS,
    'workdir' : WORK_DIR,
    'process_type' : ('ZMQPyClassDriverLauncher',)
}

# Agent parameters.
IA_RESOURCE_ID = '123xyz'
IA_NAME = 'Agent007'
IA_MOD = 'ion.agents.instrument.instrument_agent'
IA_CLS = 'InstrumentAgent'

# Used to validate param config retrieved from driver.

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('HARDWARE', group='mi')
@unittest.skip("does not work")
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class TestInstrumentDataIngestion(IonIntegrationTestCase):
    """
    Tests for data ingestion from an instrument. It is basically the
    test_poll from test_instrument_agent (as of 7/23/12) with
    the additional preparation of the ingestion management service and the
    verification that the published granules are persisted.
    """

    def setUp(self):

        self.resource_registry = ResourceRegistryServiceClient()
        self.ingestion_management = IngestionManagementServiceClient()


        self._support = DriverIntegrationTestSupport(DRV_MOD,
                                                     DRV_CLS,
                                                     DEV_ADDR,
                                                     DEV_PORT,
                                                     DELIM,
                                                     WORK_DIR)
        # Start port agent, add stop to cleanup.
        self._pagent = None        
        self._start_pagent()
        self.addCleanup(self._support.stop_pagent)    
        
        # Start container.
        self._start_container()
        
        # Bring up services in a deploy file (no need to message)
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Start data suscribers, add stop to cleanup.
        # Define stream_config.
        self._no_samples = None
        self._async_data_result = AsyncResult()
        self._data_greenlets = []
        self._stream_config = {}
        self._samples_received = []
        self._data_subscribers = []
        self._start_data_subscribers()
        self.addCleanup(self._stop_data_subscribers)

        self.prepare_ingestion()

        # Start event subscribers, add stop to cleanup.
        self._no_events = None
        self._async_event_result = AsyncResult()
        self._events_received = []

        # Create agent config.
        agent_config = {
            'driver_config' : DVR_CONFIG,
            'stream_config' : self._stream_config,
            'agent'         : {'resource_id': IA_RESOURCE_ID},
            'test_mode' : True
        }
        
        # Start instrument agent.
        self._ia_pid = None
        log.debug("TestInstrumentDataIngestion.setup(): starting IA.")
        container_client = ContainerAgentClient(node=self.container.node,
                                                name=self.container.name)
        self._ia_pid = container_client.spawn_process(name=IA_NAME,
                                                      module=IA_MOD, 
                                                      cls=IA_CLS, 
                                                      config=agent_config)      
        log.info('Agent pid=%s.', str(self._ia_pid))
        
        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = None
        self._ia_client = ResourceAgentClient(IA_RESOURCE_ID, process=FakeProcess())
        log.info('Got ia client %s.', str(self._ia_client))        
        
    def _start_pagent(self):
        """
        Construct and start the port agent.
        """
        port = self._support.start_pagent()
        
        # Configure driver to use port agent port number.
        DVR_CONFIG['comms_config'] = {
            'addr' : 'localhost',
            'port' : port
        }
        
    def _start_data_subscribers(self):
        """
        """
        # Create a pubsub client to create streams.
        pubsub_client = PubsubManagementServiceClient(node=self.container.node)

        # A callback for processing subscribed-to data.
        def consume_data(message, headers):
            log.info('Subscriber received data message: type(message)=%s.', str(type(message)))
            log.info('Subscriber received data message: %s.', str(message))
            self._samples_received.append(message)
            if self._no_samples and self._no_samples == len(self._samples_received):
                self._async_data_result.set()
                
        # Create a stream subscriber registrar to create subscribers.
        subscriber_registrar = StreamSubscriberRegistrar(process=self.container,
                                                container=self.container)

        # Create streams and subscriptions for each stream named in driver.
        self._stream_config = {}
        self._data_subscribers = []
        for stream_name in PACKET_CONFIG:
            stream_id = pubsub_client.create_stream(name=stream_name, exchange_point='science_data')

            taxy = get_taxonomy(stream_name)
            stream_config = dict(
                id=stream_id,
                taxonomy=taxy.dump()
            )
            self._stream_config[stream_name] = stream_config

            # Create subscriptions for each stream.
            exchange_name = '%s_queue' % stream_name
            sub = subscriber_registrar.create_subscriber(exchange_name=exchange_name,
                                                         callback=consume_data)
            self._listen(sub)
            self._data_subscribers.append(sub)
            query = StreamQuery(stream_ids=[stream_id])
            sub_id = pubsub_client.create_subscription(
                                query=query, exchange_name=exchange_name, exchange_point='science_data')
            pubsub_client.activate_subscription(sub_id)
            
    def _listen(self, sub):
        """
        Pass in a subscriber here, this will make it listen in a background greenlet.
        """
        gl = spawn(sub.listen)
        self._data_greenlets.append(gl)
        sub._ready_event.wait(timeout=5)
        return gl
                                 
    def _stop_data_subscribers(self):
        """
        Stop the data subscribers on cleanup.
        """
        for sub in self._data_subscribers:
            sub.stop()
        for gl in self._data_greenlets:
            gl.kill()
            
    def assertRawSampleDict(self, val):
        """
        Verify the value is a raw sample dictionary for the sbe37.
        """
        #{'stream_name': 'raw', 'blob': ['20.8074,80.04590, 761.394,   33.5658, 1506.076, 01 Feb 2001, 01:01:00'], 'time': [1342392781.61211]}
        self.assertTrue(isinstance(val, dict))
        self.assertTrue(val.has_key('blob'))
        self.assertTrue(val.has_key('time'))
        blob = val['blob'][0]
        time = val['time'][0]
    
        self.assertTrue(isinstance(blob, str))
        self.assertTrue(isinstance(time, float))

    def assertParsedSampleDict(self, val):
        """
        Verify the value is a parsed sample dictionary for the sbe37.
        """
        #{'p': [-6.945], 'c': [0.08707], 't': [20.002], 'time': [1333752198.450622]}
        self.assertTrue(isinstance(val, dict))
        self.assertTrue(val.has_key('c'))
        self.assertTrue(val.has_key('t'))
        self.assertTrue(val.has_key('p'))
        self.assertTrue(val.has_key('time'))
        c = val['c'][0]
        t = val['t'][0]
        p = val['p'][0]
        time = val['time'][0]

        self.assertTrue(isinstance(c, float))
        self.assertTrue(isinstance(t, float))
        self.assertTrue(isinstance(p, float))
        self.assertTrue(isinstance(time, float))

    def assertSample(self, sample):
        """
        Verify a sample retrieved from the sbe37, which is expected to be a
        dict of dicts {'raw':raw_sample, 'parsed':parsed_sample}.
        """
        self.assertTrue(isinstance(sample, dict))
        raw_sample = sample['raw']
        parsed_sample = sample['parsed']
        self.assertParsedSampleDict(parsed_sample)
        self.assertRawSampleDict(raw_sample)

    def get_ingestion_config(self):
        #
        # From test_dm_end_2_end.py as of 7/23/12
        #

        #--------------------------------------------------------------------------------
        # Grab the ingestion configuration from the resource registry
        #--------------------------------------------------------------------------------
        # The ingestion configuration should have been created by the bootstrap service
        # which is configured through r2deploy.yml

        ingest_configs, _ = self.resource_registry.find_resources(
            restype=RT.IngestionConfiguration,id_only=True)
        return ingest_configs[0]

    def prepare_ingestion(self):
        #
        # Takes pieces from test_dm_end_2_end.py as of 7/23/12
        #

        # Get the ingestion configuration from the resource registry
        self.ingest_config_id = self.get_ingestion_config()

        # to keep the (stream_id, dataset_id) associated with each stream_name
        self.dataset_ids = {}

        for stream_name, stream_config in self._stream_config.iteritems():
            stream_id = stream_config['id']

            dataset_id = self.ingestion_management.persist_data_stream(
                stream_id=stream_id,
                ingestion_configuration_id=self.ingest_config_id)

            log.info("persisting stream_name=%s (stream_id=%s): dataset_id=%s" % (
                stream_name, stream_id, dataset_id))

            self.assertTrue(self.ingestion_management.is_persisted(stream_id))

            self.dataset_ids[stream_name] = (stream_id, dataset_id)

    def verify_granules_persisted(self):
        #
        # takes elements from ingestion_management_test.py as of 7/23/12
        #
        ingest_config_id = self.ingest_config_id
        for stream_name, (stream_id, dataset_id) in self.dataset_ids.iteritems():

            assoc = self.resource_registry.find_associations(
                subject=ingest_config_id, predicate=PRED.hasSubscription)

            sub = self.resource_registry.read(assoc[0].o)

            self.assertTrue(sub.is_active)

            dataset = self.resource_registry.read(dataset_id)
            self.assertIsInstance(dataset, DataSet)

            log.info("Data persisted for stream_name=%s (stream_id=%s, "
                     "dataset_id=%s) dataset=%s" % (stream_name, stream_id, dataset_id, dataset))

    @unittest.skip("does not work")
    def test_poll_and_verify_granules_persisted(self):
        #
        # As test_instrument_agent.py:TestInstrumentAgent.test_poll with
        # verification that data are persisted.
        #
        self._test_poll()
        self.verify_granules_persisted()

    def _test_poll(self):
        #
        # Same as test_instrument_agent.py:TestInstrumentAgent.test_poll as of 7/23/12
        #

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)
    
        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)
        
        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)
        
        # Lets call acquire_sample 3 times, so we should get 6 samples =
        # 3 raw samples + 3 parsed samples
        self._no_samples = 6
        
        # Poll for a few samples.
        cmd = AgentCommand(command='acquire_sample')
        reply = self._ia_client.execute(cmd)
        log.debug("acquire_sample reply.result=%s" % reply.result)
        self.assertSample(reply.result)

        cmd = AgentCommand(command='acquire_sample')
        reply = self._ia_client.execute(cmd)
        self.assertSample(reply.result)

        cmd = AgentCommand(command='acquire_sample')
        reply = self._ia_client.execute(cmd)
        self.assertSample(reply.result)

        # Assert we got 6 samples.
        self._async_data_result.get(timeout=10)
        self.assertEquals(len(self._samples_received), 6)

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)
