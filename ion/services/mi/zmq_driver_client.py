#!/usr/bin/env python

"""
@package ion.services.mi.zmq_driver_client
@file ion/services/mi/zmq_driver_client.py
@author Edward Hunter
@brief Messaging client for ZMQ driver processes.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

"""
To create a client in the python interpreter:
import ion.services.mi.mi_logger
import ion.services.mi.zmq_driver_client as zdc
c = zdc.ZmqDriverClient('localhost', 5556, 5557)
"""

import thread
import logging
import time

# We import "regular" zmq, not the patched version because
# we handle the nonblocking sockets directly as they need to work
# with unpatched threads as well.
import zmq

from ion.services.mi.driver_client import DriverClient

mi_logger = logging.getLogger('mi_logger')


class ZmqDriverClient(DriverClient):
    """
    A class for communicating with a ZMQ-based driver process using python
    thread for catching asynchronous driver events.
    """
    
    def __init__(self, host, cmd_port, event_port):
        """
        Initialize members.
        @param host Host string address of the driver process.
        @param cmd_port Port number for the driver process command port.
        @param event_port Port number for the driver process event port.
        """
        DriverClient.__init__(self)
        self.host = host
        self.cmd_port = cmd_port
        self.event_port = event_port
        self.cmd_host_string = 'tcp://%s:%i' % (self.host, self.cmd_port)
        self.event_host_string = 'tcp://%s:%i' % (self.host, self.event_port)
        self.zmq_context = None
        self.zmq_cmd_socket = None
        self.event_thread = None
        self.stop_event_thread = True
        
    def start_messaging(self, evt_callback=None):
        """
        Initialize and start messaging resources for the driver process client.
        Initializes command socket for sending requests,
        and starts event thread that listens for events from the driver
        process independently of command request-reply.
        """
        self.zmq_context = zmq.Context()
        self.zmq_cmd_socket = self.zmq_context.socket(zmq.REQ)
        self.zmq_cmd_socket.connect(self.cmd_host_string)
        mi_logger.info('Driver client cmd socket connected to %s.',
                       self.cmd_host_string)        
        self.evt_callback = evt_callback
        
        def recv_evt_messages(driver_client):
            """
            A looping function that monitors a ZMQ SUB socket for asynchronous
            driver events. Can be run as a thread or greenlet.
            @param driver_client The client object that launches the thread.
            """
            context = zmq.Context()
            sock = context.socket(zmq.SUB)
            sock.connect(driver_client.event_host_string)
            sock.setsockopt(zmq.SUBSCRIBE, '')
            mi_logger.info('Driver client event thread connected to %s.',
                  driver_client.event_host_string)

            driver_client.stop_event_thread = False
            while not driver_client.stop_event_thread:
                try:
                    evt = sock.recv_pyobj(flags=zmq.NOBLOCK)
                    mi_logger.debug('got event: %s', str(evt))
                    if driver_client.evt_callback:
                        driver_client.evt_callback(evt)
                except zmq.ZMQError:
                    time.sleep(.5)

            sock.close()
            context.term()
            mi_logger.info('Client event socket closed.')
        self.event_thread = thread.start_new_thread(recv_evt_messages, (self,))
        mi_logger.info('Driver client messaging started.')
        
    def stop_messaging(self):
        """
        Close messaging resources for the driver process client. Close
        ZMQ command socket and terminate command context. Set flag to
        cause event thread to close event socket and context and terminate.
        Await event thread completion and return.
        """
        
        self.zmq_cmd_socket.close()
        self.zmq_cmd_socket = None
        self.zmq_context.term()
        self.zmq_context = None
        self.stop_event_thread = True                    
        #self.event_thread.join()
        self.event_thread = None
        self.evt_callback = None
        mi_logger.info('Driver client messaging closed.')        
    
    def cmd_dvr(self, cmd, *args, **kwargs):
        """
        Command a driver by request-reply messaging. Package command
        message and send on blocking command socket. Block on same socket
        to receive the reply. Return the driver reply.
        @param cmd The driver command identifier.
        @param args Positional arguments of the command.
        @param kwargs Keyword arguments of the command.
        @retval Command result.
        """
        # Package command dictionary.
        msg = {'cmd':cmd,'args':args,'kwargs':kwargs}
        
        mi_logger.debug('Sending command %s.', str(msg))
        while True:
            try:
                # Attempt command send. Retry if necessary.
                self.zmq_cmd_socket.send_pyobj(msg)
                if msg == 'stop_driver_process':
                    return 'driver stopping'

                # Command sent, break out and wait for reply.
                break    

            except zmq.ZMQError:
                # Socket not ready to accept send. Sleep and retry later.
                time.sleep(.5)
            
        mi_logger.debug('Awaiting reply.')
        while True:
            try:
                # Attempt reply recv. Retry if necessary.
                reply = self.zmq_cmd_socket.recv_pyobj(flags=zmq.NOBLOCK)

                # Reply recieved, break and return.
                break

            except zmq.ZMQError:
                # Socket not ready with the reply. Sleep and retry later.
                time.sleep(.5)
        mi_logger.debug('Reply: %s.', str(reply))
        
        if isinstance(reply, Exception):
            raise reply
        else:
            return reply
    