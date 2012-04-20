#!/usr/bin/env python

"""
@package ion.services.mi.zmq_driver_process
@file ion/services/mi/zmq_driver_process.py
@author Edward Hunter
@brief Driver processes using ZMQ messaging.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

"""
To launch this object from class static constructor:
import ion.services.mi.zmq_driver_process as zdp
p = zdp.ZmqDriverProcess.launch_process(5556, 5557, 'ion.services.mi.drivers.sbe37.sbe37_driver', 'SBE37Driver')

"""

from threading import Thread
from subprocess import Popen
import os
import time
import logging
import sys

import zmq

import ion.services.mi.mi_logger
import ion.services.mi.driver_process as driver_process
from ion.services.mi.instrument_driver import DriverAsyncEvent

mi_logger = logging.getLogger('mi_logger')

class ZmqDriverProcess(driver_process.DriverProcess):
    """
    A OS-level driver process that communicates with ZMQ sockets.
    Command-REP and event-PUB sockets monitor and react to comms
    needs in separate threads, which can be signaled to end
    by setting boolean flags stop_cmd_thread and stop_evt_thread.
    """
    
    @classmethod
    def launch_process(cls, cmd_port, event_port, driver_module, driver_class, ppid):
        """
        Class method constructor to launch ZmqDriverProcess as a
        separate OS process. Creates command string for this
        class and pass to superclass static method. Method has the same
        function signature as the class constructor.
        @param cmd_port Int IP port of the command REP socket.
        @param event_port Int IP port of the event socket.
        @param driver_module The python module containing the driver code.
        @param driver_class The python driver class.
        @param ppid ID of the parent process, used to self destruct when
        parent dies in test cases.
        @retval a Popen object representing the ZMQ driver process.
        """
        
        # Construct the command string.
        cmd_str = 'from %s import %s; dp = %s(%i, %i, "%s", "%s", %s);dp.run()' \
                        % (__name__, cls.__name__, cls.__name__,
                cmd_port, event_port, driver_module, driver_class, str(ppid))
                
        # Call base class launch method.
        return driver_process.DriverProcess.launch_process(cmd_str)
        
    def __init__(self, cmd_port, event_port, driver_module, driver_class, ppid):
        """
        @param cmd_port Int IP port of the command REP socket.
        @param event_port Int IP port of the event socket.
        @param driver_module The python module containing the driver code.
        @param driver_class The python driver class.
        @param ppid ID of the parent process, used to self destruct when
        parent dies in test cases.        
        """
        driver_process.DriverProcess.__init__(self, driver_module, driver_class, ppid)
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
            mi_logger.info('Driver process cmd socket bound to %s',
                           zmq_driver_process.cmd_host_string)
        
            zmq_driver_process.stop_cmd_thread = False
            while not zmq_driver_process.stop_cmd_thread:
                try:
                    msg = sock.recv_pyobj(flags=zmq.NOBLOCK)
                    mi_logger.debug('Processing message %s', str(msg))
                    reply = zmq_driver_process.cmd_driver(msg)
                    while True:
                        try:
                            sock.send_pyobj(reply, flags=zmq.NOBLOCK)
                            break
                        except zmq.ZMQError:
                            time.sleep(.1)
                            if zmq_driver_process.stop_cmd_thread:
                                break
                except zmq.ZMQError:
                    time.sleep(.1)
                
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
                            time.sleep(.1)
                            if zmq_driver_process.stop_evt_thread:
                                break
                except IndexError:
                    time.sleep(.1)

            sock.close()
            context.term()
            mi_logger.info('Driver process event socket closed')

        self.cmd_thread = Thread(target=recv_cmd_msg, args=(self, ))
        self.evt_thread = Thread(target=send_evt_msg, args=(self, ))
        self.cmd_thread.start()        
        self.evt_thread.start()
        self.messaging_started = True
    
    def stop_messaging(self):
        """
        Close messaging resource for the driver. Set flags to cause
        command and event threads to close sockets and conclude.
        """
        self.stop_cmd_thread = True
        self.stop_evt_thread = True
        self.messaging_started = False
    
    def shutdown(self):
        """
        Shutdown function prior to process exit.
        """
        driver_process.DriverProcess.shutdown(self)

    
    
    
    