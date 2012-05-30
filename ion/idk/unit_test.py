#! /usr/bin/env python

"""
@file coi-services/ion/idk/unit_test.py
@author Bill French
@brief Base classes for instrument driver tests.  
"""

# Import pyon first for monkey patching.
from pyon.public import log

import re
import os
import signal

import gevent
from gevent import spawn
from gevent.event import AsyncResult

from ion.agents.instrument.zmq_driver_client import ZmqDriverClient
from ion.agents.instrument.zmq_driver_process import ZmqDriverProcess
from ion.agents.port.logger_process import EthernetDeviceLogger

from ion.idk.comm_config import CommConfig
from ion.idk.config import Config
from ion.idk.common import Singleton

from ion.idk.exceptions import TestNotInitialized
from ion.idk.exceptions import TestNoCommConfig
from ion.idk.exceptions import TestNoDeployFile

from pyon.util.log import log
from pyon.container.cc import Container
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin

from mi.core.logger import Log
from mi.core.exceptions import InstrumentException
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import DriverConnectionState

class InstrumentDriverTestConfig(Singleton):
    """
    Singleton driver test config object.
    """
    driver_module = None
    driver_class  = None
    working_dir   = "/tmp"
    delimeter     = ['<<','>>']
    
    instrument_agent_resource_id = None
    instrument_agent_name = None
    instrument_agent_module = 'ion.agents.instrument.instrument_agent'
    instrument_agent_class = 'InstrumentAgent'
    instrument_agent_packet_config = None
    instrument_agent_stream_encoding = 'ION R2'
    instrument_agent_stream_definition = None
    
    container_deploy_file = 'res/deploy/r2deploy.yml'
    
    initialized   = False
    
    def initialize(self, *args, **kwargs):
        self.driver_module = kwargs.get('driver_module')
        self.driver_class  = kwargs.get('driver_class')
        if kwargs.get('working_dir'):
            self.working_dir = kwargs.get('working_dir')
        if kwargs.get('delimeter'):
            self.delimeter = kwargs.get('delimeter')
        
        self.instrument_agent_resource_id = kwargs.get('instrument_agent_resource_id')
        self.instrument_agent_name = kwargs.get('instrument_agent_name')
        self.instrument_agent_packet_config = kwargs.get('instrument_agent_packet_config')
        self.instrument_agent_stream_definition = kwargs.get('instrument_agent_stream_definition')
        if kwargs.get('instrument_agent_module'):
            self.instrument_agent_module = kwargs.get('instrument_agent_module')
        if kwargs.get('instrument_agent_class'):
            self.instrument_agent_class = kwargs.get('instrument_agent_class')
        if kwargs.get('instrument_agent_stream_encoding'):
            self.instrument_agent_stream_encoding = kwargs.get('instrument_agent_stream_encoding')
        
        if kwargs.get('container_deploy_file'):
            self.container_deploy_file = kwargs.get('container_deploy_file')
        
        self.initialized = True
    
class InstrumentAgentPublishers(Singleton):
    """
    Singleton driver instrument agent publishers
    """
    no_samples = None
    async_data_result = AsyncResult()
    data_greenlets = []
    stream_config = {}
    samples_received = []
    data_subscribers = []

    no_events = None
    async_event_result = AsyncResult()
    events_received = []
    event_subscribers = []
    
    def initialize(self):
        no_samples = None
        async_data_result = AsyncResult()
        data_greenlets = []
        stream_config = {}
        samples_received = []
        data_subscribers = []

        no_events = None
        async_event_result = AsyncResult()
        events_received = []
        event_subscribers = []
        
    
        
class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''

class InstrumentDriverTestCase(IonIntegrationTestCase):
    """
    Base class for instrument driver tests
    """
    
    # configuration singleton
    _test_config = InstrumentDriverTestConfig()
    
    @classmethod
    def initialize(cls, *args, **kwargs):
        """
        Initialize the test_configuration singleton
        """
        cls._test_config.initialize(*args,**kwargs)
    
    # Port agent process object.
    port_agent = None
    
    def setUp(self):
        """
        @brief Setup test cases.
        """
        Log.debug("InstrumentDriverTestCase setUp")
        
        # Test to ensure we have initialized our test config
        if not self._test_config.initialized:
            return TestNotInitialized(msg="Tests non initialized. Missing InstrumentDriverTestCase.initalize(...)?")
            
        self.clear_events()
        self.init_comm_config()
        
    def tearDown(self):
        """
        @brief Test teardown
        """
        Log.debug("InstrumentDriverTestCase tearDown")
        
    def clear_events(self):
        """
        @brief Clear the event list.
        """
        self.events = []
        
    def event_received(self, evt):
        """
        @brief Simple callback to catch events from the driver for verification.
        """
        self.events.append(evt)
        
    def comm_config_file(self):
        """
        @brief Return the path the the driver comm config yaml file.
        @return if comm_config.yml exists return the full path
        """
        repo_dir = Config().get('working_repo')
        driver_path = self._test_config.driver_module
        p = re.compile('\.')
        driver_path = p.sub('/', driver_path)
        abs_path = "%s/%s/%s" % (repo_dir, os.path.dirname(driver_path), CommConfig.config_filename())
        
        Log.debug(abs_path)
        return abs_path
    
    def init_comm_config(self):
        """
        @brief Create the comm config object by reading the comm_config.yml file.
        """
        Log.info("Initialize comm config")
        config_file = self.comm_config_file()
        
        Log.debug( " -- reading comm config from: %s" % config_file )
        if not os.path.exists(config_file):
            raise TestNoCommConfig(msg="Missing comm config.  Try running start_driver or switch_driver")
        
        self.comm_config = CommConfig.get_config_from_file(config_file)
        
        
    def init_port_agent(self):
        """
        @brief Launch the driver process and driver client.  This is used in the
        integration and qualification tests.  The port agent abstracts the physical
        interface with the instrument.
        @retval return the pid to the logger process
        """
        Log.info("Startup Port Agent")
        # Create port agent object.
        this_pid = os.getpid()
        
        # Working dir and delim are hard coded here because this launch process
        # will change with the new port agent.  
        self.port_agent = EthernetDeviceLogger.launch_process(self.comm_config.device_addr,
                                                              self.comm_config.device_port,
                                                              self._test_config.working_dir,
                                                              self._test_config.delimeter,
                                                              this_pid)

        pid = self.port_agent.get_pid()
        while not pid:
            gevent.sleep(.1)
            pid = self.port_agent.get_pid()
        
        port = self.port_agent.get_port()
        
        while not port:
            gevent.sleep(.1)
            port = self.port_agent.get_port()

        Log.info('Started port agent pid %d listening at port %d' % (pid, port))
        return port
    
    def stop_port_agent(self):
        """
        Stop the port agent.
        """
        if self.port_agent:
            pid = self.port_agent.get_pid()
            if pid:
                Log.info('Stopping pagent pid %i' % pid)
                self.port_agent.stop()
            else:
                Log.info('No port agent running.')
    
    def init_driver_process_client(self):
        """
        @brief Launch the driver process and driver client
        @retval return driver process and driver client object
        """
        Log.info("Startup Driver Process")
        
        this_pid = os.getpid()
        (dvr_proc, cmd_port, evt_port) = ZmqDriverProcess.launch_process(self._test_config.driver_module,
                                                                         self._test_config.driver_class,
                                                                         self._test_config.working_dir,
                                                                         this_pid)
        self.driver_process = dvr_proc
        Log.info('Started driver process for %d %d %s %s' % 
                 (cmd_port, evt_port, self._test_config.driver_module, self._test_config.driver_class))
        Log.info('Driver process pid %d' % self.driver_process.pid)

        # Create driver client.
        self.driver_client = ZmqDriverClient('localhost', cmd_port, evt_port)
        Log.info('Created driver client for %d %d %s %s' % (cmd_port,
            evt_port, self._test_config.driver_module, self._test_config.driver_class))

        # Start client messaging.
        self.driver_client.start_messaging(self.event_received)
        Log.info('Driver messaging started.')
        gevent.sleep(.5)
    
    def stop_driver_process_client(self):
        """
        Stop the driver_process.
        """
        if self.driver_process:
            Log.info('Stopping driver process pid %d' % self.driver_process.pid)
            if self.driver_client:
                self.driver_client.done()
                self.driver_process.wait()
                self.driver_client = None

            else:
                try:
                    Log.info('Killing driver process.')
                    self.driver_process.kill()
                except OSError:
                    pass
            self.driver_process = None
    

    
    ###
    #   Private Methods
    ###
    def _kill_process(self):
        """
        @brief Ensure a driver process has been killed 
        """
        process = self._driver_process
        pid = process.pid
        
        Log.debug("Killing driver process. PID: %d" % pid)
        # For some reason process.kill and process.terminate didn't actually kill the process.
        # that's whay we had to use the os kill command.  We have to call process.wait so that
        # the returncode attribute is updated which could be blocking.  process.poll didn't
        # seem to work.
            
        for sig in [ signal.SIGTERM, signal.SIGKILL ]:
            if(process.returncode != None):
                break
            else:
                Log.debug("Sending signal %s to driver process" % sig)
                os.kill(pid, sig)
                process.wait()
            
        if(process.returncode == None):
            raise Exception("TODO: Better exception.  Failed to kill driver process. PID: %d" % self._driver_process.pid)
        
        
class InstrumentDriverUnitTestCase(InstrumentDriverTestCase):
    """
    Base class for instrument driver unit tests
    """
    def foo(): pass
    
class InstrumentDriverIntegrationTestCase(InstrumentDriverTestCase):   # Must inherit from here to get _start_container
    def setUp(self):
        """
        @brief Setup test cases.
        """
        InstrumentDriverTestCase.setUp(self)
        
        Log.debug("InstrumentDriverIntegrationTestCase setUp")
        self.init_port_agent()
        self.init_driver_process_client()
    
    def tearDown(self):
        """
        @brief Test teardown
        """
        InstrumentDriverTestCase.tearDown(self)
        
        Log.debug("InstrumentDriverIntegrationTestCase tearDown")
        self.stop_driver_process_client()
        self.stop_port_agent()
        
        ###
        #   Common Integration Tests
        ###
        
    def test_process(self):
        """
        Test for correct launch of driver process and communications, including
        asynchronous driver events.
        """

        Log.info("Common integration test: test_process")
        
        # Verify processes exist.
        self.assertNotEqual(self.driver_process, None)
        drv_pid = self.driver_process.pid
        self.assertTrue(isinstance(drv_pid, int))
        
        self.assertNotEqual(self.port_agent, None)
        pagent_pid = self.port_agent.get_pid()
        self.assertTrue(isinstance(pagent_pid, int))
        
        # Send a test message to the process interface, confirm result.
        msg = 'I am a ZMQ message going to the process.'
        reply = self.driver_client.cmd_dvr('process_echo', msg)
        self.assertEqual(reply,'process_echo: '+msg)
        
        # Test the driver is in state unconfigured.
        # TODO: Add this test back in after driver code is merged from coi-services
        #state = self.driver_client.cmd_dvr('get_current_state')
        #self.assertEqual(state, DriverConnectionState.UNCONFIGURED)
        
        # Send a test message to the driver interface, confirm result.
        msg = 'I am a ZMQ message going to the driver.'
        reply = self.driver_client.cmd_dvr('driver_echo', msg)
        self.assertEqual(reply, 'driver_echo: '+msg)
        
        # Test the event thread publishes and client side picks up events.
        events = [
            'I am important event #1!',
            'And I am important event #2!'
            ]
        reply = self.driver_client.cmd_dvr('test_events', events=events)
        gevent.sleep(1)
        
        # Confirm the events received are as expected.
        self.assertEqual(self.events, events)

        # Test the exception mechanism.
        with self.assertRaises(InstrumentException):
            exception_str = 'Oh no, something bad happened!'
            reply = self.driver_client.cmd_dvr('test_exceptions', exception_str)
        
        # Verify we received a driver error event.
        gevent.sleep(1)
        error_events = [evt for evt in self.events if isinstance(evt, dict) and evt['type']==DriverAsyncEvent.ERROR]
        self.assertTrue(len(error_events) == 1)
    
        
class InstrumentDriverQualificationTestCase(InstrumentDriverTestCase):
    # Publisher singleton.  Needed a singleton because the setupClass and
    # tearDownClass are class methods, not instances
    _publishers = InstrumentAgentPublishers()
        
    def setUp(self):
        """
        @brief Setup test cases.
        """
        Log.debug("InstrumentDriverQualificationTestCase setUp")
        
        self.container = Container.instance
        InstrumentDriverTestCase.setUp(self)
        
        #self.init_port_agent()
    
    def tearDown(self):
        """
        @brief Test teardown
        """
        Log.debug("InstrumentDriverQualificationTestCase tearDown")
        #self.stop_port_agent()
        #self.stop_data_subscribers()
        #self.stop_event_subscribers()
        
        InstrumentDriverTestCase.tearDown(self)
        
    @classmethod
    def setupClass(cls):
        """
        @brief One time setup class
        """
        Log.debug("InstrumentDriverQualificationTestCase setupClass")
        #cls.init_instrument_agent()
        
        
    @classmethod
    def tearDownClass(cls):
        """
        @brief One time teardown class
        """
        
        Log.debug("InstrumentDriverQualificationTestCase tear down class")
        #cls.stop_instrument_agent()
    
    @classmethod
    def init_instrument_agent(cls):
        """
        @brief Launch the instrument agent
        """
        Log.info("Startup Instrument Agent")
        
        if not os.path.exists(cls._test_config.container_deploy_file):
            raise TestNoDeployFile(cls._test_config.container_deploy_file)
            
        # Derive a special test case so we can instantiate a testcase object.
        # then we can run start_container which initiallized the capability container
        class _StartContainer(InstrumentDriverTestCase):
            def runTest(self): pass
            
        testcase = _StartContainer()
        
        # Start container.
        testcase._start_container()
        Log.debug( "Capability container id: %s" % testcase.container.id )
        
        # Bring up services in a deploy file (no need to message)
        testcase.container.start_rel_from_url(testcase._test_config.container_deploy_file)
        return
        
        ###
        #    Not starting up publishers and subscribers for now.  Waiting on DM
        #    for a new stream definition
        ###
        testcase._publishers.initialize()
        testcase.start_data_subscribers()
        testcase.start_event_subscribers()

        # Driver config
        driver_config = {
            dvr_mod : testcase._test_config.driver_module,
            dvr_cls : testcase._test_config.driver_class,
            workdir : testcase._test_config.working_dir
        }
        
        # Create agent config.
        agent_config = {
            'driver_config' : driver_config,
            'stream_config' : testcase._publishers.stream_config,
            'agent'         : {'resource_id': testcase._test_config.instrument_agent_resource_id},
            'test_mode' : True  ## Enable a poison pill. If the spawning process dies
                                ## shutdown the daemon process.
        }
        
        # Start instrument agent.
        testcase._instrument_agent_pid = None
        log.debug("TestInstrumentAgent.setup(): starting IA.")
        container_client = ContainerAgentClient(node=testcase.container.node,
                                                name=testcase.container.name)
        testcase._instrument_agent_pid = container_client.spawn_process(
                                                 name=testcase._test_config.instrument_agent_name,
                                                 module=testcase._test_config.instrument_agent_module, 
                                                 cls=testcase._test_config.instrument_agent_class, 
                                                 config=agent_config)      
        log.info('Agent pid=%s.', str(testcase._instrument_agent_pid))
        
        # Start a resource agent client to talk with the instrument agent.
        testcase._instrument_agent_client = None
        testcase._instrument_agent_client = ResourceAgentClient(
            testcase._test_config.instrument_agent_resource_id, process=FakeProcess())
        log.info('Got ia client %s.', str(testcase._instrument_agent_client))   
        
    @classmethod
    def stop_instrument_agent(cls):
        """
        @brief Stop the instrument agent
        """
        Log.info("Stop the instrument agent")
        
        # Derive a special test case so we can instantiate a testcase object.
        # then we can run start_container which initiallized the capability container
        class _StartContainer(InstrumentDriverTestCase):
            def runTest(self): pass
            
        testcase = _StartContainer()
        testcase.container = Container.instance
        testcase._stop_container()        
        
    def start_data_subscribers(self):
        """
        Data subscribers
        """
        # Create a pubsub client to create streams.
        pubsub_client = PubsubManagementServiceClient(node=self.container.node)

        # A callback for processing subscribed-to data.
        def consume_data(message, headers):
            log.info('Subscriber received data message: %s.', str(message))
            self._publishers.samples_received.append(message)
            if self._publishers.no_samples and self._publishers.no_samples == len(self._publishers.samples_received):
                self._publishers.async_data_result.set()
                
        # Create a stream subscriber registrar to create subscribers.
        subscriber_registrar = StreamSubscriberRegistrar(process=self.container,
                                                         node=self.container.node)

        # Create streams and subscriptions for each stream named in driver.
        self._publishers.stream_config = {}
        self._publishers.data_subscribers = []
        for (stream_name, val) in self._test_config.instrument_agent_packet_config.iteritems():
            stream_def_id = pubsub_client.create_stream_definition(
                                container=self._test_config.instrument_agent_stream_definition)        
            stream_id = pubsub_client.create_stream(
                        name=stream_name,
                        stream_definition_id=stream_def_id,
                        original=True,
                        encoding=self._test_config.instrument_agent_stream_encoding)
            self._publishers.stream_config[stream_name] = stream_id
            
            # Create subscriptions for each stream.
            exchange_name = '%s_queue' % stream_name
            sub = subscriber_registrar.create_subscriber(exchange_name=exchange_name,
                                                         callback=consume_data)
            self._listen(sub)
            self._publishers.data_subscribers.append(sub)
            query = StreamQuery(stream_ids=[stream_id])
            sub_id = pubsub_client.create_subscription(\
                                query=query, exchange_name=exchange_name)
            pubsub_client.activate_subscription(sub_id)
            
    def stop_data_subscribers(self):
        """
        Stop the data subscribers on cleanup.
        """
        for sub in self._publishers.data_subscribers:
            sub.stop()
        for gl in self._publishers.data_greenlets:
            gl.kill()
            
    def start_event_subscribers(self):
        """
        Create subscribers for agent and driver events.
        """
        def consume_event(*args, **kwargs):
            log.info('Test recieved ION event: args=%s, kwargs=%s, event=%s.', 
                     str(args), str(kwargs), str(args[0]))
            self._publishers.events_received.append(args[0])
            if self._publishers.no_events and self._publishers.no_events == len(self._publishers.event_received):
                self._publishers.async_event_result.set()
                
        event_sub = EventSubscriber(event_type="DeviceEvent", callback=consume_event)
        event_sub.activate()
        self._publishers.event_subscribers.append(event_sub)
        
    def stop_event_subscribers(self):
        """
        Stop event subscribers on cleanup.
        """
        for sub in self._publishers.event_subscribers:
            sub.deactivate()
            
    def _listen(self, sub):
        """
        Pass in a subscriber here, this will make it listen in a background greenlet.
        """
        gl = spawn(sub.listen)
        self._publishers.data_greenlets.append(gl)
        sub._publishers.ready_event.wait(timeout=5)
        return gl
                                 
    def test_common_qualification(self):
        self.assertTrue(1)
    
