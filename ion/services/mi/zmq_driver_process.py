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
p = zdp.ZmqDriverProcess.launch_process(5556, 5557, 'ion.services.mi.drivers.sbe37_driver', 'SBE37Driver')
"""

from threading import Thread
from subprocess import Popen
import os
import time
import logging
import sys
import uuid

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
    def launch_process(cls, driver_module, driver_class, workdir='/tmp/', ppid=None):
        """
        Class method constructor to launch ZmqDriverProcess as a
        separate OS process. Creates command string for this
        class and pass to superclass static method. 
        @param driver_module The python module containing the driver code.
        @param driver_class The python driver class.
        @param workdir The work directory when temporary port files are written.
        @param ppid ID of the parent process, used to self destruct when
        parent dies in test cases.
        @retval Tuple containing (Popen object for the process, cmd port,
            evt_port)
        """
        
        # Construct the command string.
        tag = str(uuid.uuid4())
        cmd_port_fname = 'dvr_cmd_port_%s.txt' % tag
        cmd_port_fname = workdir + cmd_port_fname
        evt_port_fname = 'dvr_evt_port_%s.txt' % tag
        evt_port_fname = workdir + evt_port_fname
        cmd_str = 'from %s import %s; dp = %s("%s", "%s", "%s", "%s", %s);dp.run()' \
            % (__name__, cls.__name__, cls.__name__, driver_module,
               driver_class, cmd_port_fname, evt_port_fname, str(ppid))
                
        # Call base class launch method.
        dvr_proc = driver_process.DriverProcess.launch_process(cmd_str)
        while True:
            try:                
                cmd_port_file = file(cmd_port_fname, 'r')
                dvr_cmd_port = int(cmd_port_file.read().strip())
                cmd_port_file.close()
                os.remove(cmd_port_fname)
                break
            
            except IOError:
                time.sleep(.1)
        while True:
            try:                
                evt_port_file = file(evt_port_fname, 'r')
                dvr_evt_port = int(evt_port_file.read().strip())
                evt_port_file.close()
                os.remove(evt_port_fname)
                break
            
            except IOError:
                time.sleep(.1)

        return (dvr_proc, dvr_cmd_port, dvr_evt_port)
        
    def __init__(self, driver_module, driver_class, cmd_port_fname, evt_port_fname, ppid):
        """
        Zmq driver process constructor.
        @param driver_module The python module containing the driver code.
        @param driver_class The python driver class.
        @param cmd_port_fname Filename for temp cmd port file.
        @param evt_port_fname Filename for temp evt port file.
        @param ppid ID of the parent process, used to self destruct when
        parent dies in test cases.        
        """
        driver_process.DriverProcess.__init__(self, driver_module, driver_class, ppid)
        self.cmd_port = None
        self.cmd_port_fname = cmd_port_fname
        self.evt_port = None
        self.evt_port_fname = evt_port_fname
        self.cmd_host_string = 'tcp://*'
        self.event_host_string ='tcp://*'
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
            zmq_driver_process.cmd_port = sock.bind_to_random_port(zmq_driver_process.cmd_host_string)
            mi_logger.info('Driver process cmd socket bound to %i',
                           zmq_driver_process.cmd_port)
            file(zmq_driver_process.cmd_port_fname,'w+').write(str(zmq_driver_process.cmd_port)+'\n')

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
            zmq_driver_process.evt_port = sock.bind_to_random_port(zmq_driver_process.event_host_string)
            mi_logger.info('Driver process event socket bound to %i',
                           zmq_driver_process.evt_port)
            file(zmq_driver_process.evt_port_fname,'w+').write(str(zmq_driver_process.evt_port)+'\n')

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

    
    
    
    