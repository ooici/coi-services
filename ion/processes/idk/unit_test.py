#! /usr/bin/env python

"""Unit test base class for driver instruments"""

import logging

from ion.services.mi.zmq_driver_client import ZmqDriverClient
from ion.services.mi.zmq_driver_process import ZmqDriverProcess
from pyon.util.unit_test import PyonTestCase

class InstrumentDriverTestCase(PyonTestCase):
    def init_comm(self):
        """
        Create a comm config object for configuring the logger.  This must be overloaded
        """
        raise Exception("Vitual method must be overloaded")
    
    def init_log(self):
        """
        Create a comm log object
        """
        self.log = logging.getLogger('mi_logger')
    
    def setUp(self):
        """
        Setup test cases.
        """
        self.init_comm()
        self.init_log()
        self.events = None

    def server_address(self): return 'localhost'
    def server_command_port(self): return 5556
    def server_event_port(self): return 5557
    
    def driver_module(self):
        raise Exception("Virtual method must be overloaded")
        
    def driver_class(self):
        raise Exception("Virtual method must be overloaded")
        
    def clear_events(self):
        """
        Clear the event list.
        """
        self.events = []
        
    def event_received(self, evt):
        """
        Simple callback to catch events from the driver for verification.
        """
        self.events.append(evt)
        
    
    def _init_driver_process_client(self):
        # Launch driver process.
        
        self.log.info("Startup Driver Process")
        self.log.debug("Server Command Port: %d" % self.server_command_port())
        self.log.debug("Server Event Port: %d" % self.server_event_port())
        self.log.debug("Driver Module: %s" % self.driver_module())
        self.log.debug("Driver Class: %s" % self.driver_class())
        
        driver_process = ZmqDriverProcess.launch_process(self.server_command_port(), self.server_event_port(), self.driver_module(), self.driver_class())
        
        self.log.info("Startup Driver Client")
        self.log.debug("Server Address: %s" % self.server_address())
        self.log.debug("Server Command Port: %d" % self.server_command_port())
        self.log.debug("Server Event Port: %d" % self.server_event_port())
        
        # Create client and start messaging.
        driver_client = ZmqDriverClient(self.server_address(), self.server_command_port(),
                                        self.server_event_port())
        
        self.clear_events()
        
        driver_client.start_messaging(self.event_received)

        return driver_process, driver_client
    
    def _terminate_driver(self, driver_client, driver_process):
        # TODO: Should we update these two object so their dtor does this??
        driver_client.done()
        driver_process.wait()
        
        # Add test to verify process does not exist.