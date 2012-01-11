#!/usr/bin/env python

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

import zmq

from pyon.public import IonObject, log


def event_listen(client, callback):
    """
    method docstring
    """
    while True:
        event = client.zmq_event_socket.recv()
        callback(event)
        client.zmq_event_socket.send('ack')

class DriverClient(object):
    """
    class docstring
    """
    
    def __init__(self):
        """
        method docstring
        """
        pass
    
    def start_messaging(self):
        """
        method docstring
        """
        pass
    
    def stop_messaging(self):
        """
        method docstring
        """
        pass

    def cmd_driver(self):
        """
        method docstring
        """
        pass
    
    def event_listen(self):
        """
        method docstring
        """
        pass    
    
class ZmqDriverClient(DriverClient):
    """
    class docstring
    """
    
    def __init__(self, cmd_host, cmd_port, event_port):
        """
        method docstring
        """
        DriverClient.__init__(self)
        self.cmd_host = cmd_host
        self.cmd_port = cmd_port
        self.event_port = event_port
        self.cmd_host_string = 'tcp://%s:%i' % (self.cmd_host, self.cmd_port)
        self.event_host_string = 'tcp://*:%i' % self.event_port
        self.zmq_context = None
        self.zmq_cmd_socket = None
        self.zmq_event_socket = None
        
    def start_messaging(self):
        """
        method docstring
        """
        self.zmq_context = zmq.Context()
        self.zmq_cmd_socket = self.zmq_context.socket(zmq.REQ)
        log.info('driver client cmd socket connecting to %s' % self.cmd_host_string)        
        self.zmq_cmd_socket.connect(self.cmd_host_string)
        self.zmq_event_socket = self.zmq_context.socket(zmq.REP)
        log.info('driver client event socket binding to %s' % self.event_host_string)        
        self.zmq_event_socket.bind(self.event_host_string)
    
    def stop_messaging(self):
        """
        method docstring
        """
        self.zmq_cmd_socket.close()
        self.zmq_cmd_socket = None
        self.zmq_event_socket.close()
        self.zmq_event_socket = None
        self.zmq_context = None    
    
    def cmd_driver(self, msg):
        """
        method docstring
        """
        self.zmq_cmd_socket.send(msg)
        if msg == 'stop_driver_process':
            return 'driver stopping'
        reply = self.zmq_cmd_socket.recv()
        return reply


