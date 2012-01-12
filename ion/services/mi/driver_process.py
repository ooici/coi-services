#!/usr/bin/env python

"""
@package ion.services.mi.driver_process
@file ion/services/mi/driver_process.py
@author Edward Hunter
@brief Messaing enabled driver processes.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

from multiprocessing import Process
import os
import time
import zmq
from pyon.core.exception import BadRequest, NotFound
from pyon.public import IonObject, log

class DriverProcess(Process):
    """
    class docstring
    """
    
    def __init__(self, driver_module, driver_class):
        """
        method docstring
        """
        Process.__init__(self)
        self.driver_module = driver_module
        self.driver_class = driver_class
        self.driver = None
       
    def start_messaging(self):
        """
        """
        pass

    def stop_messaging(self):
        """
        method docstring
        """
        pass

    def construct_driver(self):
        """
        method docstring
        """
        import_str = 'import %s as dvr_mod' % self.driver_module
        ctor_str = 'driver = dvr_mod.%s()' % self.driver_class
        
        try:
            log.info('importing driver: %s' % import_str)
            exec import_str
            log.info('constructing driver: %s' % ctor_str)
            exec ctor_str
        except ImportError, NameError:
            log.error('Could not import/construct driver, module %s, class %s' \
                      % (self.driver_module, self.driver_class))
        else:
            self.driver = driver
            self.driver.forward_event = self.forward_event
            log.info('driver process %i driver constructed' % self.pid)
            
    def shutdown(self):
        """
        method docstring
        """
        log.info('driver process %i shutting down' % self.pid)
        self.driver_module = None
        self.driver_class = None
        self.driver = None

    def process_cmd_message(self):
        """
        """
        pass

    def forward_event(self):
        """
        method docstring
        """
        pass
        
    def run(self):
        """
        method docstring
        """
        self.start_messaging()
        self.construct_driver()
        if self.driver:
            while True:
                done = self.process_cmd_message()
                if done == True:
                    break
        self.stop_messaging()
        self.shutdown()

class ZmqDriverProcess(DriverProcess):
    """
    class docstring
    """
    
    def __init__(self, cmd_port, event_host, event_port, driver_module, \
                 driver_class):
        """
        method docstring
        """
        DriverProcess.__init__(self, driver_module, driver_class)
        self.cmd_port = cmd_port
        self.cmd_host_string = 'tcp://*:%i' % self.cmd_port
        self.event_host = event_host
        self.event_port = event_port
        self.event_host_string = 'tcp://%s:%i' % (self.event_host, \
                                                  self.event_port)

    def start_messaging(self):
        """
        method docstring
        """
        self.zmq_context = zmq.Context()
        self.zmq_cmd_socket = self.zmq_context.socket(zmq.REP)
        log.info('driver process %i cmd socket binding to %s' % \
                 (self.pid,self.cmd_host_string))
        self.zmq_cmd_socket.bind(self.cmd_host_string)
        self.zmq_event_socket = self.zmq_context.socket(zmq.REQ)        
        log.info('driver process %i event socket connecting to %s' % \
                 (self.pid,self.event_host_string))
        self.zmq_event_socket.connect(self.event_host_string)
        log.info('driver process %i messaging started' % self.pid)
        
    def stop_messaging(self):
        """
        method docstring
        """
        self.zmq_cmd_socket.close()
        self.zmq_cmd_socket = None
        self.zmq_event_socket.close()
        self.zmq_event_socket = None
        self.zmq_context = None
        self.host_string = None
        log.info('driver process %i messaging stopped' % self.pid)
    
    def shutdown(self):
        """
        method docstring
        """
        DriverProcess.shutdown(self)
    
    def process_cmd_message(self):
        """
        method docstring
        """
        retval = False
        msg = self.zmq_cmd_socket.recv()
        if msg == "stop_driver_process":
            reply = 'driver_stopping'
            retval = True

        elif hasattr(self.driver, msg):
            cmd_func = getattr(self.driver, msg)
            result = cmd_func()
            reply = 'Command %s returned %s' % (msg, result)

        else:
            # This should be command not found error.
            reply = 'The driver process is sending it back to you: ' + msg

        self.zmq_cmd_socket.send(reply)
        return retval

    def forward_event(self, event):
        """
        method docstring
        """
        self.zmq_event_socket.send(event)
        reply = self.zmq_event_socket.recv()
    

