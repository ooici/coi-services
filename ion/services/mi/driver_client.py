#!/usr/bin/env python
"""
@package ion.services.mi.driver_client
@file ion/services/mi/driver_client.py
@author Edward Hunter
@brief Messaging client classes for driver processes.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

from threading import Thread

import zmq
import time

class DriverClient(object):
    """
    Base class for driver clients, subclassed for specific messaging
    implementations.
    """
    
    def __init__(self):
        """
        Initialize members.
        """
        self.events = []
    
    def start_messaging(self):
        """
        Initialize and start messaging resources for the driver process client.
        Overridden for specific messaging implementations.
        """
        pass
    
    def stop_messaging(self):
        """
        Close messaging resources for the driver process client. Overridden for
        specific messaging implementations.
        """
        pass

    def cmd_dvr(self):
        """
        Command a driver by request-reply messaging. Overridden for
        specific messaging implementations.
        """
        pass
    
    def done(self):
        """
        Conclude driver process and stop client messaging resources.
        """
        self.cmd_dvr('stop_driver_process')
        self.stop_messaging()

    def test(self):
        """
        Simple test script to verify command request-response and event
        messaging.
        """
        self.start_messaging()
        time.sleep(3)
        reply = self.cmd_dvr('process_echo', data='test 1 2 3')
        time.sleep(3)
        reply = self.cmd_dvr('process_echo', data='zoom zoom boom boom')
        time.sleep(5)
        reply = self.cmd_dvr('test_events')
        time.sleep(5)
        self.done()
    
    
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
        
    def start_messaging(self):
        """
        Initialize and start messaging resources for the driver process client.
        Initializes command socket for sending requests,
        and starts event thread that listens for events from the driver
        process independently of command request-reply.
        """
        self.zmq_context = zmq.Context()
        self.zmq_cmd_socket = self.zmq_context.socket(zmq.REQ)
        self.zmq_cmd_socket.connect(self.cmd_host_string)
        print('driver client cmd socket connected to %s' % self.cmd_host_string)        
        
        def recv_evt_messages(driver_client):
            """
            """
            context = zmq.Context()
            sock = context.socket(zmq.SUB)
            sock.connect(driver_client.event_host_string)
            sock.setsockopt(zmq.SUBSCRIBE, '')
            print('driver client event thread connected to %s'
                  % driver_client.event_host_string)

            driver_client.stop_event_thread = False
            while not driver_client.stop_event_thread:
                try:
                    evt = sock.recv_pyobj(flags=zmq.NOBLOCK)
                    print 'got event: %s\n' % str(evt)
                except zmq.ZMQError:
                    print 'event thread sleeping\n'
                    time.sleep(1)

            sock.close()
            context.term()
            print('client event socket closed')
        self.event_thread = Thread(target=recv_evt_messages, args=(self,))
        self.event_thread.start()
        print('driver client messaging started')
        
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
        self.event_thread.join()
        self.event_thread = None
        print('driver client messaging closed')        
    
    def cmd_dvr(self, cmd, *args, **kwargs):
        """
        Command a driver by request-reply messaging. Package command
        message and send on blocking command socket. Block on same socket
        to receive the reply. Return the driver reply.
        """
        msg = {'cmd':cmd,'args':args,'kwargs':kwargs}
        print 'sending command %s\n' % str(msg)
        self.zmq_cmd_socket.send_pyobj(msg)
        if msg == 'stop_driver_process':
            return 'driver stopping'
        print 'awaiting reply'
        reply = self.zmq_cmd_socket.recv_pyobj()
        print 'reply: %s\n' % str(reply)
        return reply
    
