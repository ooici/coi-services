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
import os
import signal

from interface.objects import StreamQuery
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from pyon.public import StreamSubscriberRegistrar
from prototype.sci_data.stream_defs import ctd_stream_definition
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from ion.services.mi.drivers.sbe37_driver import SBE37Channel
from ion.services.mi.drivers.sbe37_driver import SBE37Parameter
from ion.services.mi.drivers.sbe37_driver import PACKET_CONFIG
from pyon.public import CFG
from mock import patch

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
    process_type = ''
    
#@unittest.skip('Do not run hardware test.')
@attr('HARDWARE', group='mi')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class TestInstrumentAgent(IonIntegrationTestCase):
    """
    Test cases for instrument agent class. Functions in this class provide
    instrument agent integration tests and provide a tutorial on use of
    the agent setup and interface.
    """
 
    def customCleanUp(self):
        log.info('CUSTOM CLEAN UP ******************************************************************************')

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
        
        self.addCleanup(self.customCleanUp)
        # Names of agent data streams to be configured.
        parsed_stream_name = 'ctd_parsed'        
        raw_stream_name = 'ctd_raw'        

        # Driver configuration.
        """
        self.driver_config = {
            'svr_addr': 'localhost',
            'cmd_port': 5556,
            'evt_port': 5557,
            'dvr_mod': 'ion.services.mi.drivers.sbe37_driver',
            'dvr_cls': 'SBE37Driver',
            'comms_config': {
                SBE37Channel.CTD: {
                    'method':'ethernet',
                    'device_addr': CFG.device.sbe37.host,
                    'device_port': CFG.device.sbe37.port,
                    'server_addr': 'localhost',
                    'server_port': 8888
                }                
            }
        }
        """
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
            }
        }
        
        # Start container.
        self._start_container()

        # Establish endpoint with container (used in tests below)
        self._container_client = ContainerAgentClient(node=self.container.node,
                                                      name=self.container.name)
        
        # Bring up services in a deploy file (no need to message)
        self.container.start_rel_from_url('res/deploy/r2dm.yml')

        # Create a pubsub client to create streams.
        self._pubsub_client = PubsubManagementServiceClient(
                                                    node=self.container.node)

        # A callback for processing subscribed-to data.
        def consume(message, headers):
            log.info('Subscriber received message: %s', str(message))

        # Create a stream subscriber registrar to create subscribers.
        subscriber_registrar = StreamSubscriberRegistrar(process=self.container,
                                                node=self.container.node)

        self.subs = []

        # Create streams for each stream named in driver.
        self.stream_config = {}
        for (stream_name, val) in PACKET_CONFIG.iteritems():
            stream_def = ctd_stream_definition(stream_id=None)
            stream_def_id = self._pubsub_client.create_stream_definition(
                                                    container=stream_def)        
            stream_id = self._pubsub_client.create_stream(
                        name=stream_name,
                        stream_definition_id=stream_def_id,
                        original=True,
                        encoding='ION R2')
            self.stream_config[stream_name] = stream_id
            
            # Create subscriptions for each stream.
            exchange_name = '%s_queue' % stream_name
            sub = subscriber_registrar.create_subscriber(exchange_name=exchange_name, callback=consume)
            sub.start()
            query = StreamQuery(stream_ids=[stream_id])
            sub_id = self._pubsub_client.create_subscription(\
                                query=query, exchange_name=exchange_name)
            self._pubsub_client.activate_subscription(sub_id)
            self.subs.append(sub)
            
        # Add cleanup function to stop subscribers.        
        def stop_subscriber(sub_list):
            for sub in sub_list:
                sub.stop()            
        self.addCleanup(stop_subscriber, self.subs)            

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
        
        self.dvr_proc_pid = None
        self.lgr_proc_pid = None
        self.lgr_pidfile_path = '/tmp/%s*.pid.txt' % str(self.driver_config['comms_config'][SBE37Channel.CTD]['device_addr'])

        def cleanup_procs(agttest):
            dvr_proc_pid = agttest.dvr_proc_pid
            lgr_proc_pid = agttest.lgr_proc_pid
            lgr_pidfile_path = agttest.lgr_pidfile_path
            if isinstance(dvr_proc_pid, int):
                log.info('CLEANING UP DVR PID %s', str(dvr_proc_pid))
                os.kill(dvr_proc_pid, signal.SIGTERM)
            if isinstance(lgr_proc_pid, int):
                log.info('CLEANING UP LGR PID %s', str(lgr_proc_pid))
                os.kill(lgr_proc_pid, signal.SIGTERM)
            if lgr_pidfile_path:
                log.info('REMOVING PIDFILE %s', lgr_pidfile_path)
                os.remove(lgr_pidfile_path)
            
        self.addCleanup(cleanup_procs, self)


    def test_direct_access(self):
        """
        Test agent direct_access command. This causes creation of
        driver process and transition to direct access.
        """
        print("test initing")
        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)        
        time.sleep(2)

        print("test go_active")
        cmd = AgentCommand(command='go_active')
        reply = self._ia_client.execute_agent(cmd)
        time.sleep(2)

        print("test run")
        cmd = AgentCommand(command='run')
        reply = self._ia_client.execute_agent(cmd)
        time.sleep(2)

        print("test go_da")
        cmd = AgentCommand(command='go_direct_access')
        retval = self._ia_client.execute_agent(cmd) 
        print("retval=" + str(retval))       
        time.sleep(2)

        print("test go_ob")
        cmd = AgentCommand(command='go_observatory')
        retval = self._ia_client.execute_agent(cmd)        
        time.sleep(2)

        print("test go_inactive")
        cmd = AgentCommand(command='go_inactive')
        reply = self._ia_client.execute_agent(cmd)
        time.sleep(2)

        print("test reset")
        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)
        time.sleep(2)

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
        log.info('initialize retval %s', str(retval))
        if isinstance(retval.result, int):             
            self.dvr_proc_pid = retval.result
            log.info('DRIVER PROCESS PID: %s', str(retval.result))
        time.sleep(2)
        
        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        if isinstance(retval.result['CHANNEL_CTD'], int):
            self.lgr_proc_pid = retval.result['CHANNEL_CTD']
            log.info('LOGGER PID: %s', str(retval.result))
            log.info('PIDFILE %s', self.lgr_pidfile_path)
            
        time.sleep(2)
        """
        log.info('TESTING CLEANUP>>>>>>>>')
        self.assertTrue(False)
        """
        
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



