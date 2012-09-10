#!/usr/bin/env python

"""
@package 
@file 
@author Edward Hunter
@brief 
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


DELAY_AND_RESTART = 'DELAY_AND_RESTART'
STOP = 'STOP'
DELAY_ACK = 'DELAY_ACK'



class R3PCTestBehavior(object):
    delay = 0
    restart = 1
    stop = 2
    
    def __init__(self, type, delay=0):
        self.type = type
        self.delay = delay

"""
ok
test_client_restart (ion.services.sa.tcaa.test.test_r3pc.TestR3PCSocket) ... ok
test_delay_long (ion.services.sa.tcaa.test.test_r3pc.TestR3PCSocket) ... Traceback (most recent call last):
  File "/opt/cache/gevent-0.13.7-py2.7-linux-x86_64.egg/gevent/greenlet.py", line 390, in run
    result = self._run(*self.args, **self.kwargs)
  File "/home/buildbot-runner/bbot/slaves/centoslca6_py27/coi_pycc/build/ion/services/sa/tcaa/r3pc.py", line 79, in server_loop
    self._start_sock()
  File "/home/buildbot-runner/bbot/slaves/centoslca6_py27/coi_pycc/build/ion/services/sa/tcaa/r3pc.py", line 157, in _start_sock
    self._server.bind(self._endpoint)
  File "socket.pyx", line 390, in zmq.core.socket.Socket.bind (zmq/core/socket.c:3613)
ZMQError: Address already in use
<Greenlet at 0x9bac910: server_loop> failed with ZMQError
"""

class R3PCSocketError(Exception): pass

class R3PCServer(object):
    """
    """
    def __init__(self, callback=None, close_callback=None):
        """
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
                            self._close_callback()
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
        """
        if self._server:
            log.debug('Closing server.')
            self._server.setsockopt(zmq.LINGER, 0)
            self._server.close()
            self._server = None
    
    def stop(self):
        """
        """
        if self._greenlet:
            self._greenlet.kill()
            self._greenlet.join()
            self._greenlet = None
        self._stop_sock()
    
    def done(self):
        """
        """
        self._context.term()
        self._context = None

    def _default_callback(self, request):
        """
        """
        log.debug('Default callback on request: %s', request)

    def _default_close_callback(self, request):
        """
        """
        log.debug('Server socket closed.')

class R3PCClient(object):
    """
    """
    def __init__(self, callback=None, close_callback=None):
        """
        """
        self._context = zmq.Context(1)
        self._greenlet = None
        self._queue = []
        self._callback = callback or self._default_callback
        self._close_callback = close_callback or self._default_close_callback
        self._client = None
        self.send_count = 0
        self.test_behaviors = {}

    def start(self, host='localhost', port=7979, timeout=3, sleep=.1, retries=3):
        """
        """
        self._host = host
        self._port = port
        self._endpoint = 'tcp://%s:%i' % (host, port)
        self._timeout = timeout
        self._sleep = sleep
        self._retries = retries

        def client_loop():
            """
            """
            log.debug('Client greenlet started.')
            
            no_retries = 0            
            while True:

                # Start the client socket if necessary.
                # If retries exceeded, end loop.
                if not self._client:
                    if no_retries >= self._retries:
                        log.warning('Client exceeded maximum retries, closing.')
                        self._close_callback()                        
                        break
                    else:
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
                while True:
                    try:
                        log.debug('Client sending request %s.', request)                        
                        self._client.send_pyobj(request, NOBLOCK, 0)
                        break
                    except ZMQError:
                        log.warning('Client socket not available for send.')
                        gevent.sleep(self._sleep)
                                
                self.send_count += 1

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
                        self._close_callback()                                                
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
                        no_retries = 0
                        self._queue.pop(0)
                        self._callback()
                        break
                    except ZMQError:
                        log.debug('Client socket unavailable to receive ack.')
                        gevent.sleep(self._sleep)
                        #delta = time.time() - startrecv
                        #log.debug('delta=%f', delta)
                        if time.time() - startrecv > self._timeout:
                            log.warning('Client timed out awaiting ack, restarting.')                            
                            self._stop_sock()
                            no_retries += 1
                            break
        
        log.debug('Spawning client greenlet.')
        self._start_evt = AsyncResult()
        self._greenlet = gevent.spawn(client_loop)
        self._start_evt.get()
        self._start_evt = None
    
    def _start_sock(self):
        """
        """
        log.debug('Constructing zmq req client.')
        self._client = self._context.socket(zmq.REQ)
        log.debug('Connecting client to endpoint %s.', self._endpoint)
        self._client.connect(self._endpoint)
        if self._start_evt:
            self._start_evt.set()
    
    def _stop_sock(self):
        """
        """
        if self._client:
            log.debug('Closing client.')
            self._client.setsockopt(zmq.LINGER, 0)
            self._client.close()
            self._client = None
    
    def stop(self):
        """
        """
        if self._greenlet:
            self._greenlet.kill()
            self._greenlet.join()
            self._greenlet = None
        self._stop_sock()
    
    def enqueue(self, msg):
        """
        """
        log.debug('Client enqueueing message: %s.', msg)
        self._queue.append(msg)
        return len(self._queue)
    
    def done(self):
        """
        """
        log.debug('Terminating zmq context.')
        self._context.term()
        self._conext = None

    def _default_callback(self):
        """
        """
        log.debug('Client ack callback, queue size %i', len(self._queue))

    def _default_close_callback(self):
        """
        """
        log.debug('Client socket closed.')