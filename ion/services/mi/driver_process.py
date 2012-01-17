#!/usr/bin/env python

"""
@package ion.services.mi.driver_process
@file ion/services/mi/driver_process.py
@author Edward Hunter
@brief Messaing enabled driver processes.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

#import pyon.core.exception as pe

from multiprocessing import Process
from threading import Thread
import os
import time
import logging
import sys

import zmq

"""
import ion.services.mi.mi_logger
import ion.services.mi.driver_process as dp
import ion.services.mi.driver_client as dc
p = dp.ZmqDriverProcess(5556, 5557, 'ion.services.mi.sbe37_driver', 'SBE37Driver')
c = dc.ZmqDriverClient('localhost', 5556, 5557)
"""

mi_logger = logging.getLogger('mi_logger')

class DriverProcess(Process):
    """
    Base class for messaging enabled OS-level driver processes. Provides
    run loop, dynamic driver import and construction and interface
    for messaging implementation subclasses.
    """
    
    def __init__(self, driver_module, driver_class):
        """
        @param driver_module The python module containing the driver code.
        @param driver_class The python driver class.
        """
        Process.__init__(self)
        self.driver_module = driver_module
        self.driver_class = driver_class
        self.driver = None
        self.events = []
        
    def construct_driver(self):
        """
        Attempt to import and construct the driver object based on
        configuration.
        @retval True if successful, False otherwise.
        """
        
        import_str = 'import %s as dvr_mod' % self.driver_module
        ctor_str = 'driver = dvr_mod.%s()' % self.driver_class
        
        try:
            exec import_str
            mi_logger.info('Imported driver module %s', self.driver_module)
            exec ctor_str
            mi_logger.info('Constructed driver %s', self.driver_class)
            
        except (ImportError, NameError, AttributeError):
            mi_logger.error('Could not import/construct driver module %s, class %s.',
                      self.driver_module, self.driver_class)
            return False

        else:
            self.driver = driver
            return True
            
    def start_messaging(self):
        """
        Initialize and start messaging resources for the driver, blocking
        until messaging terminates. Overridden in subclasses for
        specific messaging technologies. 
        """
        pass

    def stop_messaging(self):
        """
        Close messaging resource for the driver. Overridden in subclasses
        for specific messaging technologies.
        """
        pass

    def shutdown(self):
        """
        Shutdown function prior to process exit.
        """
        mi_logger.info('Driver process shutting down.')
        self.driver_module = None
        self.driver_class = None
        self.driver = None

    def cmd_driver(self, msg):
        """
        Process a command message against the driver. If the command
        exists as a driver attribute, call it passing supplied args and
        kwargs and returning the driver result. Special messages that are
        not forwarded to the driver are:
        'stop_driver_process' - signal to close messaging and terminate.
        'test_events' - populate event queue with test data.
        'process_echo' - echos the message back.
        If the command is not found in the driver, an echo message is
        replied to the client.
        @param msg A driver command message.
        @retval The driver command result.
        """
        cmd = msg.get('cmd', None)
        args = msg.get('args', None)
        kwargs = msg.get('kwargs', None)
        cmd_func = getattr(self.driver, cmd, None)
        if cmd == 'stop_driver_process':
            self.stop_messaging()
            return'stop_driver_process'
        elif cmd == 'test_events':
            self.events.append('I am event number 1!')
            self.events.append('And I am event number 2!')
            reply = 'test_events'
        elif cmd == 'process_echo':
            reply = msg
        elif cmd_func:
            reply = cmd_func(*args, **kwargs)
        else:
            reply = 'Unknown driver command'
        
        return reply        
            
    def run(self):
        """
        Process entry point. Construct driver and start messaging loops.
        Call shutdown when messaging terminates amd then end process.
        """
        mi_logger.info('Driver process started.')

        if self.construct_driver():
            self.start_messaging()

        self.shutdown()

class ZmqDriverProcess(DriverProcess):
    """
    A OS-level driver process that communicates with ZMQ sockets.
    Command-REP and event-PUB sockets monitor and react to comms
    needs in separate threads, which can be signaled to end
    by setting boolean flags stop_cmd_thread and stop_evt_thread.
    """
    
    def __init__(self, cmd_port, event_port, driver_module, driver_class):
        """
        @param cmd_port Int IP port of the command REP socket.
        @param event_port Int IP port of the event socket.
        @param driver_module The python module containing the driver code.
        @param driver_class The python driver class.
        """
        DriverProcess.__init__(self, driver_module, driver_class)
        self.cmd_port = cmd_port
        self.cmd_host_string = 'tcp://*:%i' % self.cmd_port
        self.event_port = event_port
        self.event_host_string = 'tcp://*:%i' % self.event_port
        self.evt_thread = None
        self.stop_evt_thread = True
        self.cmd_thread = None
        self.stop_cmd_thread = True

    def start_messaging(self):
        """
        Initialize and start messaging resources for the driver, blocking
        until messaging terminates. This ZMQ implementation starts and
        joins command and event threads, managing nonblocking send/recv calls
        on REP and PUB sockets, respectively. Terminate loops and close
        sockets when stop flag is set in driver process.
        """
        
        def recv_cmd_msg(zmq_driver_process):
            """
            Await commands on a ZMQ REP socket, forwaring them to the
            driver for processing and returning the result.
            """
            context = zmq.Context()
            sock = context.socket(zmq.REP)
            sock.bind(zmq_driver_process.cmd_host_string)
            mi_logger.info('Driver rpocess cmd socket bound to %s',
                           zmq_driver_process.cmd_host_string)
        
            zmq_driver_process.stop_cmd_thread = False
            while not zmq_driver_process.stop_cmd_thread:
                try:
                    msg = sock.recv_pyobj(flags=zmq.NOBLOCK)
                    mi_logger.debug('Processing message %s', str(msg))
                    reply = zmq_driver_process.cmd_driver(msg)
                    while reply:
                        try:
                            sock.send_pyobj(reply)
                            reply = None
                        except zmq.ZMQError:
                            time.sleep(0)
                except zmq.ZMQError:
                    time.sleep(0)
        
            sock.close()
            context.term()
            mi_logger.info('Driver process cmd socket closed.')
                           
        def send_evt_msg(zmq_driver_process):
            """
            Await events on the driver process event queue and publish them
            on a ZMQ PUB socket to the driver process client.
            """
            context = zmq.Context()
            sock = context.socket(zmq.PUB)
            sock.bind(zmq_driver_process.event_host_string)
            mi_logger.info('Driver process event socket bound to %s',
                           zmq_driver_process.event_host_string)

            zmq_driver_process.stop_evt_thread = False
            while not zmq_driver_process.stop_evt_thread:
                try:
                    evt = zmq_driver_process.events.pop(0)
                    mi_logger.debug('Event thread sending event %s', str(evt))
                    while evt:
                        try:
                            sock.send_pyobj(evt, flags=zmq.NOBLOCK)
                            evt = None
                            mi_logger.debug('Event sent!')
                        except zmq.ZMQError:
                            time.sleep(0)
                            
                except IndexError:
                    time.sleep(0)

            sock.close()
            context.term()
            mi_logger.info('Driver process event socket closed')

        self.cmd_thread = Thread(target=recv_cmd_msg, args=(self, ))
        self.evt_thread = Thread(target=send_evt_msg, args=(self, ))
        self.cmd_thread.start()        
        self.evt_thread.start()        
        self.cmd_thread.join()
        self.evt_thread.join()
    
    def stop_messaging(self):
        """
        Close messaging resource for the driver. Set flags to cause
        command and event threads to close sockets and conclude.
        """
        self.stop_cmd_thread = True
        self.stop_evt_thread = True
    
    def shutdown(self):
        """
        Shutdown function prior to process exit.
        """
        DriverProcess.shutdown(self)
    