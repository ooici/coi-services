#! /usr/bin/env python

"""
@file coi-services/ion/idk/unit_test.py
@author Bill French
@brief Base class for instrument driver tests.
"""

import os
import signal
import logging

from ion.agents.instrument.zmq_driver_client import ZmqDriverClient
from ion.agents.instrument.zmq_driver_process import ZmqDriverProcess
from pyon.util.unit_test import PyonTestCase

from pyon.util.int_test import IonIntegrationTestCase     # Must inherit from here to get _start_container

#    class InstrumentDriverTestCase(PyonTestCase):
class InstrumentDriverTestCase(IonIntegrationTestCase):   # Must inherit from here to get _start_container

    """Base class for instrument driver unit tests."""
    
    ###
    #   Private data
    ###
    _driver_process = None
    _driver_client = None
    
    
    ###
    #   Configuration
    ###
    """Overloadable configuration parameters for the driver process """
    @staticmethod
    def server_address(): return 'localhost'
    
    @staticmethod
    def server_command_port(): return 5556
    
    @staticmethod
    def server_event_port(): return 5557
    
    
    ###
    #   Class methods
    ###
    @classmethod
    def setUpClass(cls):
        """
        @brief Initialize comm configuration and logging
        """
        cls.init_comm()
        cls.init_log()
        
    @classmethod
    def init_comm(cls):
        """
        @brief Set self.comm_config to a dictionary containing driver comm settings.
        TODO: Update this method so it pulls simulator settings from pyon.  If there aren't
              any simulators available tests should be skipped if a device isn't configured.
        """
        raise Exception("Virtual method init_comm must be overloaded")
    
    @classmethod
    def init_log(cls):
        """
        @brief initialize logging for the driver
        """
        cls.log = logging.getLogger('mi_logger')
    
    
    ###
    #   Virtual Method
    ###
    def driver_module(self):
        """
        @brief Name of the driver module in python dot notation
        @retval driver module name as string object
        """
        raise Exception("Virtual method driver_module must be overloaded")
        
    def driver_class(self):
        """
        @brief Name of the driver class
        @retval driver class name as string object
        """
        raise Exception("Virtual method driver_class must be overloaded")
        
        
    ###
    #   Public Methods
    ###
    def setUp(self):
        """
        @brief Setup test cases.
        """
        self.events = None

    def tearDown(self):
        """
        @brief tear down the driver process if it has been configured and ensure that it is killed properly
        """
        #self.log.debug("Tear down test. Ensure driver process stopped")
        if(self._driver_client):
            self._driver_client.done()
            self._driver_process.wait()
            self._kill_process()
            
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
        
    
    def init_driver_process_client(self):
        """
        @brief Launch the driver process and driver client
        @retval return driver process and driver client object
        """
        self.log.info("Startup Driver Process")
        self.log.debug("Server Command Port: %d" % self.server_command_port())
        self.log.debug("Server Event Port: %d" % self.server_event_port())
        self.log.debug("Driver Module: %s" % self.driver_module())
        self.log.debug("Driver Class: %s" % self.driver_class())
        
        # Spawn the driver subprocess
        driver_process = ZmqDriverProcess.launch_process(self.server_command_port(), self.server_event_port(), self.driver_module(), self.driver_class())
        self._driver_process = driver_process
        
        self.log.info("Startup Driver Client")
        self.log.debug("Server Address: %s" % self.server_address())
        self.log.debug("Server Command Port: %d" % self.server_command_port())
        self.log.debug("Server Event Port: %d" % self.server_event_port())
        
        # Create client and start messaging.
        driver_client = ZmqDriverClient(self.server_address(), self.server_command_port(),
                                        self.server_event_port())
        self._driver_client = driver_client
        self.clear_events()
        
        driver_client.start_messaging(self.event_received)
        return driver_process, driver_client
    
    
    ###
    #   Private Methods
    ###
    def _kill_process(self):
        """
        @brief Ensure a driver process has been killed 
        """
        process = self._driver_process
        pid = process.pid
        
        self.log.debug("Killing driver process. PID: %d" % pid)
        # For some reason process.kill and process.terminate didn't actually kill the process.
        # that's whay we had to use the os kill command.  We have to call process.wait so that
        # the returncode attribute is updated which could be blocking.  process.poll didn't
        # seem to work.
            
        for sig in [ signal.SIGTERM, signal.SIGKILL ]:
            if(process.returncode != None):
                break
            else:
                self.log.debug("Sending signal %s to driver process" % sig)
                os.kill(pid, sig)
                process.wait()
            
        if(process.returncode == None):
            raise Exception("TODO: Better exception.  Failed to kill driver process. PID: %d" % self._driver_process.pid)
        
        
        
    
