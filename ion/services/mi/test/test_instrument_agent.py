#!/usr/bin/env python

"""
@package ion.services.mi.test.test_instrument_agent
@file ion/services/mi/test_instrument_agent.py
@author Edward Hunter
@brief Test cases for R2 instrument agent.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Import pyon first for monkey patching.
from pyon.public import log

# Standard imports.
import os
import signal
import time
import unittest
from datetime import datetime

# 3rd party imports.
from gevent import spawn
import gevent
from nose.plugins.attrib import attr
from mock import patch
import uuid

# ION imports.
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
from pyon.public import CFG
from pyon.event.event import EventSubscriber

# MI imports.
from ion.services.mi.logger_process import EthernetDeviceLogger

# bin/nosetests -s -v ion/services/mi/test/test_instrument_agent.py:TestInstrumentAgent.test_initialize
# bin/nosetests -s -v ion/services/mi/test/test_instrument_agent.py:TestInstrumentAgent.test_go_active
# bin/nosetests -s -v ion/services/mi/test/test_instrument_agent.py:TestInstrumentAgent.test_get_set
# bin/nosetests -s -v ion/services/mi/test/test_instrument_agent.py:TestInstrumentAgent.test_poll
# bin/nosetests -s -v ion/services/mi/test/test_instrument_agent.py:TestInstrumentAgent.test_autosample

# Device and port agent config.
#DEV_ADDR = '67.58.49.220' 
#DEV_ADDR = '137.110.112.119' # Moxa DHCP in Edward's office.
DEV_ADDR = 'sbe37-simulator.oceanobservatories.org' # Simulator addr.
DEV_PORT = 4001 # Moxa port or simulator random data.
#DEV_PORT = 4002 # Simulator sine data.

# Port agent config.
PAGENT_ADDR = 'localhost' # Run local.
PAGENT_PORT = 0 # Have the port agent select a server port.
WORK_DIR = '/tmp/' # Location of pid, status, port, log files.
DELIM = ['<<','>>'] # Log file delim.
SNIFFER_PORT = None # No sniffer capabilities yet.
TAG = str(uuid.uuid4()) # File tag.

# Driver comms config.

# Driver config.
from ion.services.mi.drivers.sbe37_driver import PACKET_CONFIG
DVR_CONFIG = {
    'svr_addr' : 'localhost',
    'cmd_port' : 5556,
    'evt_port' : 5557,
    'dvr_mod' : 'ion.services.mi.drivers.sbe37_driver',
    'dvr_cls' : 'SBE37Driver',
    'comms_config' : {
        'addr' : 'localhost',
        'port' : 0
    }
}

# Agent parameters.
IA_RESOURCE_ID = '123xyz'
IA_NAME = 'Agent007'
IA_MOD = 'ion.services.mi.instrument_agent'
IA_CLS = 'InstrumentAgent'

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''

@unittest.skip('In development.')    
@attr('HARDWARE', group='mi')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class TestInstrumentAgent(IonIntegrationTestCase):
    """
    Test cases for instrument agent class. Functions in this class provide
    instrument agent integration tests and provide a tutorial on use of
    the agent setup and interface.
    """ 
    def setUp(self):
        """
        """
        
        # Agent ion process id.
        self._ia_pid = None
        
        # Agent client.
        self._ia_client = None
        
        # Data subscriptions.
        self._subs = None
        
        # Container client.
        self._container_client = None
        
        # Pubsub client.
        self._pubsub_client = None
        
        # Port agent.
        self._pagent = None
        
        # Start the port agent.
        self._start_pagent()
        
        # Add cleanup to shut pagent down.
        self.addCleanup(self._stop_pagent)    
        
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

        # Create streams and subscriptions for each stream named in driver.
        stream_config = {}
        self._subs = []
        for (stream_name, val) in PACKET_CONFIG.iteritems():
            stream_def = ctd_stream_definition(stream_id=None)
            stream_def_id = self._pubsub_client.create_stream_definition(
                                                    container=stream_def)        
            stream_id = self._pubsub_client.create_stream(
                        name=stream_name,
                        stream_definition_id=stream_def_id,
                        original=True,
                        encoding='ION R2')
            stream_config[stream_name] = stream_id
            
            # Create subscriptions for each stream.
            exchange_name = '%s_queue' % stream_name
            sub = subscriber_registrar.create_subscriber(exchange_name=exchange_name, callback=consume)
            sub.start()
            query = StreamQuery(stream_ids=[stream_id])
            sub_id = self._pubsub_client.create_subscription(\
                                query=query, exchange_name=exchange_name)
            self._pubsub_client.activate_subscription(sub_id)
            self._subs.append(sub)
            
        # Add cleanup function to stop subscribers.        
        def stop_subscriber(sub_list):
            for sub in sub_list:
                sub.stop()            
        self.addCleanup(stop_subscriber, self._subs)

        """
        # Add subscription for events.
        def cb(*args, **kwargs):
            origin = args[0].origin
            event = str(args[0]._get_type())
            description = args[0].description
            time_stamp = str(datetime.fromtimestamp(time.mktime(time.gmtime(float(args[0].ts_created)/1000))))
            log.debug("got event: origin=%s, event=%s, description=%s, time stamp=%s"
                      %(origin, event, description, time_stamp))
            
            
        sub = EventSubscriber(event_type="ResourceEvent", callback=cb)
        self._listen(sub)
        """
        
        # Create agent config.
        agent_config = {
            'driver_config' : DVR_CONFIG,
            'stream_config' : stream_config,
            'agent'         : {'resource_id': IA_RESOURCE_ID},
            'test_mode' : True
        }
        
        log.debug("TestInstrumentAgent.setup(): starting IA")
        self._ia_pid = self._container_client.spawn_process(name=IA_NAME,
                                       module=IA_MOD, cls=IA_CLS,
                                       config=agent_config)      
        log.info('agent pid=%s', str(self._ia_pid))
        
        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = ResourceAgentClient(IA_RESOURCE_ID, process=FakeProcess())
        log.info('got ia client %s', str(self._ia_client))        
        
    def _start_pagent(self):
        """
        Construct and start the port agent.
        """
        
        # Create port agent object.
        this_pid = os.getpid()
        self._pagent = EthernetDeviceLogger(DEV_ADDR, DEV_PORT, PAGENT_PORT,
                        WORK_DIR, DELIM, SNIFFER_PORT, this_pid, TAG)
        log.info('Created port agent object for %s %d %d', DEV_ADDR,
                       DEV_PORT, PAGENT_PORT)

        # Stop the port agent if it is already running.
        # The port agent creates a pid file based on the config used to
        # construct it.
        self._stop_pagent()
        pid = None

        # Start the port agent.
        # Confirm it is started by getting pidfile.
        self._pagent.start_remote()
        pid = self._pagent.get_pid()
        while not pid:
            gevent.sleep(.1)
            pid = self._pagent.get_pid()
        port = self._pagent.get_port()
        while not port:
            gevent.sleep(.1)
            port = self._pagent.get_port()
            
        DVR_CONFIG['comms_config']['port'] = port
        log.info('Started port agent pid %d listening at port %d', pid, port)

    def _stop_pagent(self):
        """
        Stop the port agent.
        """
        if self._pagent:
            pid = self._pagent.get_pid()
            if pid:
                log.info('Stopping pagent pid %i', pid)
                self._pagent.stop()
            else:
                log.info('No port agent running.')
                
    def _listen(self, sub):
        """
        Pass in a subscriber here, this will make it listen in a background greenlet.
        """
        gl = spawn(sub.listen)
        sub._ready_event.wait(timeout=5)
        return gl

    def test_initialize(self):
        """
        Test agent initialize command. This causes creation of
        driver process and transition to inactive.
        """

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        log.info('IA state: %s', str(retval))
    
        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        log.info('initialize retval: %s', str(retval))

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        log.info('IA state: %s', str(retval))

        gevent.sleep(3)

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)
        log.info('reset retval: %s', str(retval))
                
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        log.info('IA state: %s', str(retval))
    
    def test_go_active(self):
        """
        Test agent go_active command. This causes a driver process to
        launch a connection broker, connect to device hardware, determine
        entry state of driver and intialize driver parameters.
        """
        pass
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
        """
        
    def test_get_set(self):
        """
        Test instrument driver resource get/set interface. This tests
        getting and setting driver reousrce paramters in various syntaxes and
        validates results including persistence on device hardware.
        """
        pass
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
        """
        
    def test_poll(self):
        """
        Test instrument driver resource execute interface to do polled
        sampling.
        """
        pass
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
        """

    def test_autosample(self):
        """
        Test instrument driver execute interface to start and stop streaming
        mode.
        """
        pass
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
        """
    def test_direct_access(self):
        """
        Test agent direct_access command. This causes creation of
        driver process and transition to direct access.
        """
        pass
    
        """
        print("test initing")
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
        """

