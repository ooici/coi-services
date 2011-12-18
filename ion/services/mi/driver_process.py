#!/usr/bin/env python

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'


from multiprocessing import Process
import os
import time
import zmq

class DriverProcess(Process):
    """
    class docstring
    """
    
    def __init__(self, port):
        """
        method docstring
        """
        Process.__init__(self)
        self.port = port
       
    def init_messaging(self):
        """
        """        
        self.zmq_context = zmq.Context()
        self.zmq_socket = self.zmq_context.socket(zmq.REP)
        host_string = 'tcp://*:'+str(self.port)
        print 'binding to: ' + host_string
        self.zmq_socket.bind(host_string)
        
    def run(self):
        """
        method docstring
        """
        self.init_messaging()
        while True:
            msg = self.zmq_socket.recv()
            reply = 'The driver process is sending it back to you: ' + msg
            self.zmq_socket.send(reply)
            if msg == 'done':
                break
        
        print 'driver process done' 
    
class DriverClient(object):
    """
    class docstring
    """
    
    def __init__(self, host, port):
        """
        method docstring
        """
        self.host = host
        self.port = port
        
    def init_messaging(self):
        """
        method docstring
        """
        self.zmq_context = zmq.Context()
        self.zmq_socket = self.zmq_context.socket(zmq.REQ)
        #host_string = 'tcp://'+self.host+':'+self.port
        host_string = 'tcp://localhost:5562'
        print 'connecting to: ' + host_string
        self.zmq_socket.connect(host_string)
    
    def send(self, msg):
        """
        method docstring
        """
        self.zmq_socket.send(msg)
        print 'sent'
        reply = self.zmq_socket.recv()
        print reply
    