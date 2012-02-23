#!/usr/bin/env python

"""
@package ion.services.mi.test.test_instrument_agent
@file ion/services/mi/test_instrument_agent.py
@author Edward Hunter
@brief Test cases for R2 instrument agent.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

from pyon.public import log
from nose.plugins.attrib import attr

from interface.objects import CouchStorage, ProcessDefinition, StreamQuery, StreamPolicy
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from pyon.public import  StreamSubscriberRegistrar
from prototype.sci_data.ctd_stream import ctd_stream_definition
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from ion.services.mi.drivers.sbe37_driver import SBE37Channel
from ion.services.mi.drivers.sbe37_driver import SBE37Parameter

import time
import unittest

# bin/nosetests -s -v ion/services/mi/test/test_instrument_agent.py:TestInstrumentAgent.test_initialize
# bin/nosetests -s -v ion/services/mi/test/test_instrument_agent.py:TestInstrumentAgent.test_go_active
# bin/nosetests -s -v ion/services/mi/test/test_instrument_agent.py:TestInstrumentAgent.test_get_set
# bin/nosetests -s -v ion/services/mi/test/test_instrument_agent.py:TestInstrumentAgent.test_poll
# bin/nosetests -s -v ion/services/mi/test/test_instrument_agent.py:TestInstrumentAgent.test_autosample

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    
#@unittest.skip('Do not run hardware test.')
@attr('INT', group='mi')
class TestInstrumentAgent(IonIntegrationTestCase):
    """
    Test cases for instrument agent class. Functions in this class provide
    instrument agent integration tests and provide a tutorial on use of
    the agent setup and interface.
    """

    def setUp(self):
        """
        Setup the test environment to exersice use of instrumet agent, including:
        * define driver_config parameters.
        * create container with required services and container client.
        * create publication stream ids for each driver data stream.
        * create stream_config parameters.
        * create and activate subscriptions for agent data streams.
        * spawn instrument agent process and create agent client.
        * add cleanup functions to cause subscribers to get stopped.
        """
        
        # Names of agent data streams to be configured.
        parsed_stream_name = 'ctd_parsed'        
        raw_stream_name = 'ctd_raw'        

        # Driver configuration.
        self.driver_config = {
            'svr_addr': 'localhost',
            'cmd_port': 5556,
            'evt_port': 5557,
            'dvr_mod': 'ion.services.mi.drivers.sbe37_driver',
            'dvr_cls': 'SBE37Driver',
            'comms_config': {
                SBE37Channel.CTD: {
                    'method':'ethernet',
                    'device_addr': '137.110.112.119',
                    'device_port': 4001,
                    'server_addr': 'localhost',
                    'server_port': 8888
                }                
            },
            'packet_config' : {
                parsed_stream_name : ('prototype.sci_data.ctd_stream',
                                'ctd_stream_packet'),
                raw_stream_name : None
            }
        }

        # Start container.
        self._start_container()

        # Establish endpoint with container.
        self._container_client = ContainerAgentClient(node=self.container.node,
                                                      name=self.container.name)
        
        # Bring up services in a deploy file.        
        self._container_client.start_rel_from_url('res/deploy/r2dm.yml')

        # Create a pubsub client to create streams.
        self._pubsub_client = PubsubManagementServiceClient(
                                                    node=self.container.node)

        # Create parsed stream. The stream name must match one
        # used by the driver to label packet data.
        parsed_stream_def = ctd_stream_definition(stream_id=None)
        parsed_stream_id = self._pubsub_client.create_stream(
                        name=parsed_stream_name,
                        stream_definition=parsed_stream_def,
                        original=True,
                        encoding='ION R2')

        # Create raw stream. The stream name must match one used by the
        # driver to label packet data. This stream does not yet have a
        # packet definition so will not be published.
        raw_stream_def = ctd_stream_definition(stream_id=None)
        raw_stream_id = self._pubsub_client.create_stream(name=raw_stream_name,
                        stream_definition=raw_stream_def,
                        original=True,
                        encoding='ION R2')
        
        # Define stream configuration.
        self.stream_config = {
            parsed_stream_name : parsed_stream_id,
            raw_stream_name : raw_stream_id
        }

        # A callback for processing subscribed-to data.
        def consume(message, headers):
            log.info('Subscriber received message: %s', str(message))

        # Create a stream subscriber registrar to create subscribers.
        subscriber_registrar = StreamSubscriberRegistrar(process=self.container,
                                                node=self.container.node)

        # Create and activate parsed data subscription.
        parsed_sub = subscriber_registrar.create_subscriber(exchange_name=\
                                            'parsed_queue', callback=consume)
        parsed_sub.start()
        parsed_query = StreamQuery(stream_ids=[parsed_stream_id])
        parsed_sub_id = self._pubsub_client.create_subscription(\
                            query=parsed_query, exchange_name='parsed_queue')
        self._pubsub_client.activate_subscription(parsed_sub_id)

        # Create and activate raw data subscription.
        raw_sub = subscriber_registrar.create_subscriber(exchange_name=\
                                                'raw_queue', callback=consume)
        raw_sub.start()
        raw_query = StreamQuery(stream_ids=[raw_stream_id])
        raw_sub_id = self._pubsub_client.create_subscription(\
                            query=raw_query, exchange_name='raw_queue')
        self._pubsub_client.activate_subscription(raw_sub_id)

        # Create agent config.
        self.agent_config = {
            'driver_config' : self.driver_config,
            'stream_config' : self.stream_config
        }

        # Launch an instrument agent process.
        self._ia_name = 'agent007'
        self._ia_mod = 'ion.services.mi.instrument_agent'
        self._ia_class = 'InstrumentAgent'
        self._ia_pid = self._container_client.spawn_process(name=self._ia_name,
                                       module=self._ia_mod, cls=self._ia_class,
                                       config=self.agent_config)      
        log.info('got pid=%s', str(self._ia_pid))
        
        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = ResourceAgentClient('123xyz', name=self._ia_pid,
                                              process=FakeProcess())
        log.info('got ia client %s', str(self._ia_client))        
        
        # Add cleanup function to stop subscribers.        
        def stop_subscriber(sub_list):
            for sub in sub_list:
                sub.stop()            
        self.addCleanup(stop_subscriber, [parsed_sub, raw_sub])
                
    def test_initialize(self):
        """
        Test agent initialize command. This causes creation of
        driver process and transition to inactive.
        """
        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)        
        time.sleep(2)
        
        caps = self._ia_client.get_capabilities()
        log.info('Capabilities: %s',str(caps))
        
        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)

    def test_go_active(self):
        """
        Test agent go_active command. This causes a driver process to
        launch a connection broker, connect to device hardware, determine
        entry state of driver and intialize driver parameters.
        """
        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)        
        time.sleep(2)
        
        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        time.sleep(2)

        cmd = AgentCommand(command='go_inactive')
        retval = self._ia_client.execute_agent(cmd)
        time.sleep(2)

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)
        time.sleep(2)

    def test_get_set(self):
        """
        Test instrument driver resource get/set interface. This tests
        getting and setting driver reousrce paramters in various syntaxes and
        validates results including persistence on device hardware.
        """
        cmd = AgentCommand(command='initialize')
        reply = self._ia_client.execute_agent(cmd)        
        time.sleep(2)
        
        cmd = AgentCommand(command='go_active')
        reply = self._ia_client.execute_agent(cmd)
        time.sleep(2)

        cmd = AgentCommand(command='run')
        reply = self._ia_client.execute_agent(cmd)
        time.sleep(2)

        get_params = [
            (SBE37Channel.CTD, SBE37Parameter.ALL)            
        ]
        reply = self._ia_client.get_param(get_params)
        time.sleep(2)

        self.assertIsInstance(reply, dict)
        self.assertIsInstance(reply[(SBE37Channel.CTD, SBE37Parameter.TA2)], float)
        self.assertIsInstance(reply[(SBE37Channel.CTD, SBE37Parameter.PTCA1)], float)
        self.assertIsInstance(reply[(SBE37Channel.CTD, SBE37Parameter.TCALDATE)], (tuple, list))

        # Set up a param dict of the original values.
        old_ta2 = reply[(SBE37Channel.CTD, SBE37Parameter.TA2)]
        old_ptca1 = reply[(SBE37Channel.CTD, SBE37Parameter.PTCA1)]
        old_tcaldate = reply[(SBE37Channel.CTD, SBE37Parameter.TCALDATE)]

        orig_params = {
            (SBE37Channel.CTD, SBE37Parameter.TA2): old_ta2,
            (SBE37Channel.CTD, SBE37Parameter.PTCA1): old_ptca1,
            (SBE37Channel.CTD, SBE37Parameter.TCALDATE): old_tcaldate            
        }

        # Set up a param dict of new values.
        new_ta2 = old_ta2*2
        new_ptcal1 = old_ptca1*2
        new_tcaldate = list(old_tcaldate)
        new_tcaldate[2] = new_tcaldate[2] + 1
        
        new_params = {
            (SBE37Channel.CTD, SBE37Parameter.TA2): new_ta2,
            (SBE37Channel.CTD, SBE37Parameter.PTCA1): new_ptcal1,
            (SBE37Channel.CTD, SBE37Parameter.TCALDATE): new_tcaldate
        }

        # Set the params to their new values.
        reply = self._ia_client.set_param(new_params)
        time.sleep(2)

        # Check overall success and success of the individual paramters.
        self.assertIsInstance(reply, dict)
        
        # Get the same paramters back from the driver.
        get_params = [
            (SBE37Channel.CTD, SBE37Parameter.TA2),
            (SBE37Channel.CTD, SBE37Parameter.PTCA1),
            (SBE37Channel.CTD, SBE37Parameter.TCALDATE)
        ]
        reply = self._ia_client.get_param(get_params)
        time.sleep(2)

        # Check success, and check that the parameters were set to the
        # new values.
        self.assertIsInstance(reply, dict)
        self.assertIsInstance(reply[(SBE37Channel.CTD, SBE37Parameter.TA2)], float)
        self.assertIsInstance(reply[(SBE37Channel.CTD, SBE37Parameter.PTCA1)], float)
        self.assertIsInstance(reply[(SBE37Channel.CTD, SBE37Parameter.TCALDATE)], (tuple, list))
        self.assertAlmostEqual(reply[(SBE37Channel.CTD, SBE37Parameter.TA2)], new_ta2, delta=abs(0.01*new_ta2))
        self.assertAlmostEqual(reply[(SBE37Channel.CTD, SBE37Parameter.PTCA1)], new_ptcal1, delta=abs(0.01*new_ptcal1))
        self.assertEqual(reply[(SBE37Channel.CTD, SBE37Parameter.TCALDATE)], new_tcaldate)

        # Set the paramters back to their original values.        
        reply = self._ia_client.set_param(orig_params)
        self.assertIsInstance(reply, dict)

        # Get the parameters back from the driver.
        reply = self._ia_client.get_param(get_params)

        # Check overall and individual sucess, and that paramters were
        # returned to their original values.
        self.assertIsInstance(reply, dict)
        self.assertIsInstance(reply[(SBE37Channel.CTD, SBE37Parameter.TA2)], float)
        self.assertIsInstance(reply[(SBE37Channel.CTD, SBE37Parameter.PTCA1)], float)
        self.assertIsInstance(reply[(SBE37Channel.CTD, SBE37Parameter.TCALDATE)], (tuple, list))
        self.assertAlmostEqual(reply[(SBE37Channel.CTD, SBE37Parameter.TA2)], old_ta2, delta=abs(0.01*old_ta2))
        self.assertAlmostEqual(reply[(SBE37Channel.CTD, SBE37Parameter.PTCA1)], old_ptca1, delta=abs(0.01*old_ptca1))
        self.assertEqual(reply[(SBE37Channel.CTD, SBE37Parameter.TCALDATE)], old_tcaldate)

        time.sleep(2)

        cmd = AgentCommand(command='go_inactive')
        reply = self._ia_client.execute_agent(cmd)
        time.sleep(2)

        cmd = AgentCommand(command='reset')
        reply = self._ia_client.execute_agent(cmd)
        time.sleep(2)

    def test_poll(self):
        """
        Test instrument driver resource execute interface to do polled
        sampling.
        """
        cmd = AgentCommand(command='initialize')
        reply = self._ia_client.execute_agent(cmd)        
        time.sleep(2)
        
        cmd = AgentCommand(command='go_active')
        reply = self._ia_client.execute_agent(cmd)
        time.sleep(2)

        cmd = AgentCommand(command='run')
        reply = self._ia_client.execute_agent(cmd)
        time.sleep(2)

        cmd = AgentCommand(command='acquire_sample')
        reply = self._ia_client.execute(cmd)
        time.sleep(2)

        cmd = AgentCommand(command='acquire_sample')
        reply = self._ia_client.execute(cmd)
        time.sleep(2)

        cmd = AgentCommand(command='acquire_sample')
        reply = self._ia_client.execute(cmd)
        time.sleep(2)

        cmd = AgentCommand(command='go_inactive')
        reply = self._ia_client.execute_agent(cmd)
        time.sleep(2)

        cmd = AgentCommand(command='reset')
        reply = self._ia_client.execute_agent(cmd)
        time.sleep(2)


    def test_autosample(self):
        """
        Test instrument driver execute interface to start and stop streaming
        mode.
        """
        cmd = AgentCommand(command='initialize')
        reply = self._ia_client.execute_agent(cmd)        
        time.sleep(2)
        
        cmd = AgentCommand(command='go_active')
        reply = self._ia_client.execute_agent(cmd)
        time.sleep(2)

        cmd = AgentCommand(command='run')
        reply = self._ia_client.execute_agent(cmd)
        time.sleep(2)

        cmd = AgentCommand(command='go_streaming')
        reply = self._ia_client.execute_agent(cmd)
        time.sleep(30)
        
        cmd = AgentCommand(command='go_observatory')
        while True:
            reply = self._ia_client.execute_agent(cmd)
            result = reply.result
            if isinstance(result, dict):
                if all([val == None for val in result.values()]):
                    break
            time.sleep(2)
        time.sleep(2)
        
        cmd = AgentCommand(command='go_inactive')
        reply = self._ia_client.execute_agent(cmd)
        time.sleep(2)

        cmd = AgentCommand(command='reset')
        reply = self._ia_client.execute_agent(cmd)
        time.sleep(2)



