#!/usr/bin/env python

"""
@package ion.services.sa.tcaa.r3pc
@file ion/services/sa/tcaa/r3pc.py
@author Edward Hunter
@brief Reliable, remote RPC sockets for 2CAA.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

from pyon.public import log
from pyon.public import CFG

import gevent
from gevent import monkey
from gevent.event import AsyncResult
monkey.patch_all()

from random import randint
import time
import zmq
from zmq import ZMQError
from zmq import NOBLOCK

class R3PCTestBehavior(object):
    """
    Simple object to hold test instrumentation.
    """
    delay = 0
    restart = 1
    stop = 2
    
    def __init__(self, type, delay=0):
        self.type = type
        self.delay = delay

class R3PCServer(object):
    """
    A remote, reliable 0MQ RPC server socket.
    """
    def __init__(self, callback=None, close_callback=None):
        """
        Initialize internal variables.
        """
        self._context = zmq.Context(1)
        self._greenlet = None
        self._server = None
        self._callback = callback or self._default_callback
        self._close_callback = close_callback or self._default_close_callback
        self.recv_count = 0
        self.test_behaviors = {}
        
    def start(self, host='*', port=7979, timeout=10, sleep=.1):
        """
        Start the server and processing greenlet.
        """
        self._host = host
        self._port = port
        self._endpoint = 'tcp://%s:%i' % (host, port)        
        self._sleep = sleep
        self._timeout = timeout

        def server_loop():
            """
            """
            log.debug('Server greenlet started.')
            
            while True:
                                                
                if not self._server:
                    self._start_sock()
                
                # Try to accept incomming message.
                # If nothing available, sleep the greenlet.
                # Never timeout.
                log.debug('Server awaiting request.')
                while True:
                    try:
                        request = self._server.recv_pyobj(NOBLOCK)
                        log.debug('Server got request %s.', str(request))
                        break
                    except ZMQError:
                        gevent.sleep(self._sleep)
    
                # Increment the receive count and retrieve any
                # instrumented test behaviors.
                self.recv_count += 1
                
                # Received a null request corresponding to a zmq interrupt.
                # Abandon request and restart server.
                if not request:
                    log.warning('Server got null request, abandoning request and restarting server socket.')
                    self._stop_sock()
                
                # Valid message received.
                # Try to send the message ack.
                # If socket not available, sleep and retry.
                # If timeout exceeded, abandon request and restart server.
                # If successful, send request to callback.
                else:
                    
                    # Grab the current test behavior if any.
                    test_behavior = self.test_behaviors.get(self.recv_count, None)
                    
                    # Run test behavior if any.
                    if test_behavior:

                        if test_behavior.type == R3PCTestBehavior.delay:
                            gevent.sleep(test_behavior.delay)
                                
                        if test_behavior.type == R3PCTestBehavior.stop:
                            if test_behavior.delay > 0:
                                gevent.sleep(test_behavior.delay)
                            self._stop_sock()
                            break
                        
                        if test_behavior.type == R3PCTestBehavior.restart:
                            if test_behavior.delay > 0:
                                gevent.sleep(test_behavior.delay)
                            self._stop_sock()
                        
                    sendtime = time.time()
                    while self._server:
                        try:
                            log.debug('Server sending ack.')
                            self._server.send_pyobj('OK', NOBLOCK)
                            log.debug('Server ack send, calling callback.')
                            self._callback(request)
                            break
                        except ZMQError:
                            gevent.sleep(self._sleep)
                            if time.time() - sendtime > self._timeout:
                                log.warning('Server ack failed, abandoning request and restarting server socket.')
                                self._stop_sock()
        
        log.debug('Spawning server greenlet.')
        self._start_evt = AsyncResult()
        self._greenlet = gevent.spawn(server_loop)
        self._start_evt.get()
        self._start_evt = None
        return self._port

    def _start_sock(self):
        """
        Create and bind the server socket.
        """
        log.debug('Constructing zmq rep server socket.')
        self._server = self._context.socket(zmq.REP)
        log.debug('Binding server socket to endpoint: %s', self._endpoint)
        if self._port == 0:
            addr = 'tcp://%s' % self._host
            port = self._server.bind_to_random_port(addr)
            self._port = port
            self._endpoint = 'tcp://%s:%i' % (self._host, port)
            log.debug('Server bound to random port: %s', self._endpoint)
        else:
            self._server.bind(self._endpoint)
        if self._start_evt:
            self._start_evt.set()
    
    def _stop_sock(self):
        """
        Close and destroy server socket.
        """
        if self._server:
            log.debug('Closing server.')
            self._server.setsockopt(zmq.LINGER, 0)
            self._server.close()
            self._server = None
            self._close_callback()
    
    def stop(self):
        """
        Stop server.
        Kill server greenlet and close and destroy server socket.
        """
        if self._greenlet:
            self._greenlet.kill()
            self._greenlet.join()
            self._greenlet = None
        self._stop_sock()
    
    def done(self):
        """
        Destroy 0MQ context.
        """
        self._context.term()
        self._context = None

    def _default_callback(self, request):
        """
        Default callback for server requests.
        """
        log.debug('Default callback on request: %s', request)

    def _default_close_callback(self, request):
        """
        Default callback for server close events.
        """
        log.debug('Server socket closed.')

class R3PCClient(object):
    """
    A remote, reliable 0MQ RPC client socket.
    """
    def __init__(self, callback=None, close_callback=None):
        """
        Initialize internal variables.
        """
        self._context = zmq.Context(1)
        self._greenlet = None
        self._queue = []
        self._callback = callback or self._default_callback
        self._close_callback = close_callback or self._default_close_callback
        self._client = None
        self.send_count = 0
        self.test_behaviors = {}

    def start(self, host='localhost', port=7979, timeout=3, sleep=.1):
        """
        Start client socket and processing greenlet.
        """
        self._host = host
        self._port = port
        self._endpoint = 'tcp://%s:%i' % (host, port)
        self._timeout = timeout
        self._sleep = sleep

        def client_loop():
            """
            """
            log.debug('Client greenlet started.')
            
            while True:

                # Start the client socket if necessary.
                # If retries exceeded, end loop.
                if not self._client:
                    self._start_sock()

                # Pop first message from the queue.
                # If empty sleep and retry.                    
                while True:
                    try:
                        request = self._queue[0]
                        break
                    except IndexError:
                        gevent.sleep(self._sleep)
                                                    
                # Send request to server.
                # If unable to send, sleep and retry.
                startsend = time.time()
                while True:
                    try:
                        log.debug('Client sending request %s.', request)                        
                        self._client.send_pyobj(request, NOBLOCK, 0)
                        self.send_count += 1
                        break
                    except ZMQError:
                        log.warning('Client socket not available for send.')
                        gevent.sleep(self._sleep)
                        if time.time() - startsend > self._timeout:
                            log.warning('Client timed out sending, restarting.')                            
                            self._stop_sock()
                            break

                # Grab the current test behavior if any.
                test_behavior = self.test_behaviors.get(self.send_count, None)

                # Run test behavior if any.
                if test_behavior:

                    if test_behavior.type == R3PCTestBehavior.delay:
                        gevent.sleep(test_behavior.delay)
                            
                    if test_behavior.type == R3PCTestBehavior.stop:
                        if test_behavior.delay > 0:
                            gevent.sleep(test_behavior.delay)
                        self._stop_sock()
                        break
                    
                    if test_behavior.type == R3PCTestBehavior.restart:
                        if test_behavior.delay > 0:
                            gevent.sleep(test_behavior.delay)
                        self._stop_sock()

                # Receive ack from server.
                # If unavailalbe, sleep and retry.
                # If timeout exceeded, increment retry count and restart client.
                # If ack received, zero retry could and pop message.
                startrecv = time.time()                
                while self._client:
                    try:
                        log.debug('Client awaiting ack.')
                        reply = self._client.recv_pyobj(NOBLOCK)
                        log.debug('Client received ack: %s for request: %s',
                            reply, request)
                        if not reply:
                            self._stop_sock()
                        else:
                            self._queue.pop(0)
                            self._callback(request)
                        break
                    except ZMQError:
                        log.debug('Client socket unavailable to receive ack.')
                        gevent.sleep(self._sleep)
                        #delta = time.time() - startrecv
                        #log.debug('delta=%f', delta)
                        if time.time() - startrecv > self._timeout:
                            log.warning('Client timed out awaiting ack, restarting.')                            
                            self._stop_sock()
                            break
        
        log.debug('Spawning client greenlet.')
        self._start_evt = AsyncResult()
        self._greenlet = gevent.spawn(client_loop)
        self._start_evt.get()
        self._start_evt = None
    
    def _start_sock(self):
        """
        Create and connect client socket.
        """
        log.debug('Constructing zmq req client.')
        self._client = self._context.socket(zmq.REQ)
        log.debug('Connecting client to endpoint %s.', self._endpoint)
        self._client.connect(self._endpoint)
        if self._start_evt:
            self._start_evt.set()
    
    def _stop_sock(self):
        """
        Close and destroy client socket.
        """
        if self._client:
            log.debug('Closing client.')
            self._client.setsockopt(zmq.LINGER, 0)
            self._client.close()
            self._client = None
            self._close_callback()
    
    def stop(self):
        """
        Kill client greenlet, close and destroy client socket.
        """
        if self._greenlet:
            self._greenlet.kill()
            self._greenlet.join()
            self._greenlet = None
        self._stop_sock()
    
    def enqueue(self, msg):
        """
        Enqueue message for transmission so server.
        """
        log.debug('Client enqueueing message: %s.', msg)
        self._queue.append(msg)
        return len(self._queue)
    
    def done(self):
        """
        Destroy 0MQ context.
        """
        log.debug('Terminating zmq context.')
        self._context.term()
        self._conext = None

    def _default_callback(self, request):
        """
        Default client ack callback.
        """
        log.debug('Client ack callback for request: %s', str(request))

    def _default_close_callback(self):
        """
        Default client close callback.
        """
        log.debug('Client socket closed.')